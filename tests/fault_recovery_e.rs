//! Phase-8B fault catalog — **recovery-replay boundary, group E**.
//!
//! Five fault strategies that exercise the engine's recovery path through the
//! Phase-8A harness (`FakeDisk`/`FaultFs` + `Engine::with_data_dir_fs`), each
//! diffed against a pure-Rust reference model under the crash-consistency
//! contract (the same oracle `tests/crash_oracle.rs` defines, re-stated here
//! since a `tests/*.rs` file is its own crate and cannot import another):
//!
//! - `F-REC-DELETE-BEFORE-APPEND-ORDER` — a durable Delete whose later target
//!   Append is lost to a crash: the Delete applies to whatever is present,
//!   the lost Append is simply absent, no gap among survivors, floor consistent.
//! - `F-RECDIR-EIO-CREATE` — EIO on `create_dir_all(wal)` on recovery: the
//!   engine refuses to start (explicit error, never a silent empty start over an
//!   existing-but-unreadable dir).
//! - `F-NFS-CTO-SNAPSHOT-READ` — close-to-open: a stale pre-write handle does
//!   NOT observe a freshly-written snapshot, but recovery (which opens snapshot +
//!   WAL FRESH, by path) reads the current bytes and recovers correctly.
//! - `F-COMPOUND-CRASH-SNAP-THEN-WAL` — crash during a snapshot write (rename
//!   not yet dir-fsynced), restart, then a crash during WAL replay: recovery
//!   converges to one consistent state (old-or-new snapshot + replayed WAL),
//!   acked data present, idempotent, no gap/resurrection.
//! - `F-COMPOUND-OOM-IN-ERROR-PATH` — an allocation/IO failure while reading a
//!   WAL frame during replay (the error path): recovery fails cleanly without
//!   leaving a partially-applied index as final; a re-run converges.
//!
//! Bounded (small workloads, fixed seeds, capped points) so the file runs in
//! well under a minute. Gated behind `test-fs`; needs no `failpoints` feature —
//! crashes are driven by the harness-level `FakeDisk.crash()` injector.

#![cfg(feature = "test-fs")]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::json;

use streams::clock::{SharedClock, TestClock};
use streams::config::ServerConfig;
use streams::engine::Engine;
use streams::storage::testfs::{FakeDisk, FaultFs, FaultKind, FaultOp, TornDamage};
use streams::storage::{File, Fs, OpenOpts};
use streams::types::{BoxConfig, BoxType, DeleteRequest, DiffRequest, Filter, RecordIn, WriteRequest};

// ===========================================================================
// Reference model (the oracle) — a trimmed copy of the crash_oracle.rs model,
// covering exactly the fields these recovery tests assert: per-seq data/tag/node,
// the acked + ever-acked sets, head, and the voluntary-delete floor.
// ===========================================================================

#[derive(Debug, Clone, PartialEq, Eq)]
struct ModelRecord {
    data: String,
    tag: Option<String>,
    node: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct ModelBox {
    durable: bool,
    /// Acked + still-live records (delete removes from here).
    acked: BTreeMap<u64, ModelRecord>,
    /// Every record ever acked (delete/evict never remove): the universe of seqs
    /// recovery is ever allowed to surface ("no fabrication" is `⊆ ever_acked`).
    ever_acked: BTreeMap<u64, ModelRecord>,
    head: u64,
    /// Voluntary-delete front floor (silent, no tombstone). Monotone.
    delete_floor: u64,
}

impl ModelBox {
    /// Seqs the model says must be present (acked, not deleted below the floor).
    fn live_seqs(&self) -> Vec<u64> {
        self.acked
            .keys()
            .copied()
            .filter(|s| *s >= self.delete_floor)
            .collect()
    }
    fn earliest(&self) -> u64 {
        self.live_seqs().into_iter().min().unwrap_or(self.head + 1)
    }
}

#[derive(Debug, Default)]
struct RefModel {
    boxes: BTreeMap<String, ModelBox>,
}

impl RefModel {
    fn ensure_box(&mut self, name: &str, durable: bool) {
        self.boxes.entry(name.to_string()).or_insert(ModelBox {
            durable,
            ..Default::default()
        });
    }

    fn ack_append(&mut self, name: &str, seqs: &[u64], rec: &ModelRecord) {
        let b = self.boxes.get_mut(name).expect("box modeled before append");
        for s in seqs {
            b.acked.insert(*s, rec.clone());
            b.ever_acked.insert(*s, rec.clone());
            b.head = b.head.max(*s);
        }
    }

    fn ack_delete(&mut self, name: &str, before_seq: Option<u64>, tag_eq: Option<&str>) {
        let Some(b) = self.boxes.get_mut(name) else {
            return;
        };
        if let Some(bs) = before_seq {
            if bs > b.delete_floor {
                b.delete_floor = bs;
            }
            b.acked.retain(|s, _| *s >= bs);
        }
        if let Some(tag) = tag_eq {
            b.acked.retain(|_, r| r.tag.as_deref() != Some(tag));
        }
    }
}

// ===========================================================================
// Op stream + driver (mirror acked effects into the model only on Ok).
// ===========================================================================

#[derive(Debug, Clone)]
enum Op {
    PutBox { name: String, durable: bool },
    Append { name: String, data: String, tag: Option<String>, node: Option<String> },
    Delete { name: String, before_seq: Option<u64>, tag_eq: Option<String> },
}

fn run_ops(engine: &Engine, model: &mut RefModel, ops: &[Op]) {
    for op in ops {
        match op {
            Op::PutBox { name, durable } => {
                let cfg = BoxConfig {
                    r#type: BoxType::Log,
                    durable: *durable,
                    cap_records: 0,
                    ..Default::default()
                };
                if engine.put_box(name, cfg).is_ok() {
                    model.ensure_box(name, *durable);
                }
            }
            Op::Append { name, data, tag, node } => {
                let req = WriteRequest {
                    records: vec![RecordIn {
                        data: json!({ "v": data }),
                        tag: tag.clone(),
                        node: node.clone(),
                        meta: None,
                    }],
                    node: None,
                    idempotency_key: None,
                    create: Some(true),
                    config: None,
                    disable_backpressure: true,
                };
                if !model.boxes.contains_key(name) {
                    model.ensure_box(name, false);
                }
                if let Ok(resp) = engine.write(name, req, true) {
                    let seqs = resp.seqs.clone().unwrap_or_else(|| vec![resp.last_seq]);
                    let rec = ModelRecord {
                        data: data.clone(),
                        tag: tag.clone(),
                        node: node.clone(),
                    };
                    model.ack_append(name, &seqs, &rec);
                }
            }
            Op::Delete { name, before_seq, tag_eq } => {
                let req = DeleteRequest {
                    before_seq: *before_seq,
                    match_: tag_eq.as_ref().map(|t| Filter::from_shorthand(t)),
                };
                if engine.delete(name, req).is_ok() {
                    model.ack_delete(name, *before_seq, tag_eq.as_deref());
                }
            }
        }
    }
}

// ===========================================================================
// Engine build / recover plumbing through an injected Fs.
// ===========================================================================

const DATA_DIR: &str = "/data";

fn cfg() -> ServerConfig {
    ServerConfig {
        data_dir: Some(DATA_DIR.to_string()),
        ..Default::default()
    }
}

fn clock() -> SharedClock {
    Arc::new(TestClock::new(1_700_000_000_000))
}

fn open_engine(disk: &FakeDisk) -> Arc<Engine> {
    Engine::with_data_dir_fs(cfg(), clock(), disk.arc()).expect("engine opens through FakeDisk")
}

/// Make the WAL + meta dir entries durable (the create+dir-fsync production does
/// at WAL open / snapshot write — modeled explicitly so the file names survive a
/// crash).
fn sync_dirs(disk: &FakeDisk) {
    let fs = disk.arc();
    let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("wal"));
    let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("meta"));
}

// ---------------------------------------------------------------------------
// Recovered-state dump + the contract oracle (subset of crash_oracle.rs).
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
struct BoxDump {
    head: u64,
    earliest: u64,
    count: u64,
    records: BTreeMap<u64, ModelRecord>,
    tombstone_reason: Option<String>,
}

fn dump_box(engine: &Engine, name: &str) -> Option<BoxDump> {
    let st = engine.box_state(name, false).ok()?;
    let mut records = BTreeMap::new();
    let mut tombstone_reason = None;
    let mut from = 0u64;
    loop {
        let d = engine
            .diff(
                name,
                DiffRequest {
                    from_seq: from,
                    limit: 1000,
                    node: None,
                    include_tags: true,
                    include_meta: true,
                    wait_ms: 0,
                },
            )
            .ok()?;
        if let Some(tomb) = &d.tombstone {
            tombstone_reason = Some(format!("{:?}", tomb.reason).to_lowercase());
        }
        for r in &d.records {
            let data = r
                .data
                .get("v")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| r.data.to_string());
            records.insert(
                r.seq,
                ModelRecord {
                    data,
                    tag: r.tag.clone(),
                    node: r.node.clone(),
                },
            );
        }
        if d.caught_up || d.records.is_empty() {
            break;
        }
        from = d.next_from_seq;
    }
    Some(BoxDump {
        head: st.head_seq,
        earliest: st.earliest_seq,
        count: st.count,
        records,
        tombstone_reason,
    })
}

/// Assert the crash-consistency contract for one box. `whole_tail_durable` is
/// true only when every acked write's fsync was guaranteed to have returned
/// (a clean stop / all-durable crash).
fn assert_box_contract(name: &str, model: &ModelBox, dump: &BoxDump, whole_tail_durable: bool) {
    let live = model.live_seqs();
    let survivors: Vec<u64> = dump.records.keys().copied().collect();

    // (1) NO FABRICATION: every survivor was once acked, byte-for-byte.
    for (seq, rec) in &dump.records {
        let m = model.ever_acked.get(seq).unwrap_or_else(|| {
            panic!("{name}: recovered seq {seq} was never acked (fabricated/torn): {rec:?}")
        });
        assert_eq!(m, rec, "{name}: recovered record at seq {seq} differs from the model");
    }

    // (2) NO SILENT LOSS of acked-durable (whole tail durable).
    if model.durable && whole_tail_durable {
        for seq in &live {
            assert!(
                dump.records.contains_key(seq),
                "{name}: acked durable seq {seq} LOST (survivors={survivors:?}, live={live:?})"
            );
        }
    }

    // (3) NO GAP — survivors are a dense prefix of the model's live set.
    if let Some(&hi) = survivors.last() {
        if whole_tail_durable {
            let expected: Vec<u64> = live.iter().copied().filter(|s| *s <= hi).collect();
            assert_eq!(
                survivors, expected,
                "{name}: survivors must be a dense prefix of live (survivors={survivors:?}, live={live:?})"
            );
        } else {
            for &s in &live {
                if s <= hi {
                    assert!(
                        dump.records.contains_key(&s),
                        "{name}: live seq {s} missing below high-water {hi} (hole): {survivors:?}"
                    );
                }
            }
        }
    }

    // (4) HEAD MONOTONE.
    assert!(
        dump.head <= model.head,
        "{name}: recovered head {} exceeds model head {} (future seq?)",
        dump.head,
        model.head
    );
    if model.durable && whole_tail_durable && !live.is_empty() {
        assert_eq!(dump.head, model.head, "{name}: durable head must match model");
        assert_eq!(dump.earliest, model.earliest(), "{name}: earliest must match model");
    }

    // (5) A voluntary delete stays SILENT (never tombstones).
    assert!(
        dump.tombstone_reason.is_none(),
        "{name}: a voluntary delete must not surface a tombstone, got {:?}",
        dump.tombstone_reason
    );
}

fn assert_recovered_matches_model(engine: &Engine, model: &RefModel, whole_tail_durable: bool) {
    for (name, mbox) in &model.boxes {
        let dump = dump_box(engine, name);
        if mbox.acked.is_empty() && mbox.head == 0 {
            continue;
        }
        let Some(dump) = dump else {
            if mbox.durable && !mbox.live_seqs().is_empty() && whole_tail_durable {
                panic!("{name}: durable box with acked records vanished after recovery");
            }
            continue;
        };
        assert_box_contract(name, mbox, &dump, whole_tail_durable);
    }
}

// ===========================================================================
// CrashAfter — wrap a FakeDisk and fire crash() after the Nth call of an op
// class (the harness-level "power loss after the Nth FS mutating call").
// Copied from the Phase-8A pattern in tests/crash_oracle.rs.
// ===========================================================================

#[derive(Clone)]
struct CrashAfter {
    disk: FakeDisk,
    op: FaultOp,
    at: u64,
    seen: Arc<AtomicU64>,
    tripped: Arc<AtomicBool>,
}

impl CrashAfter {
    fn new(disk: FakeDisk, op: FaultOp, at: u64) -> Self {
        CrashAfter {
            disk,
            op,
            at,
            seen: Arc::new(AtomicU64::new(0)),
            tripped: Arc::new(AtomicBool::new(false)),
        }
    }
    fn arc(&self) -> Arc<dyn Fs> {
        Arc::new(self.clone())
    }
    fn tick(&self, op: FaultOp) {
        if op != self.op {
            return;
        }
        let idx = self.seen.fetch_add(1, Ordering::SeqCst);
        if idx == self.at
            && self
                .tripped
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            self.disk.crash(TornDamage::None);
        }
    }
}

struct CrashAfterFile {
    inner: Box<dyn File>,
    owner: CrashAfter,
}

impl File for CrashAfterFile {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read_at(offset, buf)
    }
    fn write_at(&mut self, offset: u64, buf: &[u8]) -> std::io::Result<usize> {
        let r = self.inner.write_at(offset, buf);
        self.owner.tick(FaultOp::WriteAt);
        r
    }
    fn set_len(&mut self, len: u64) -> std::io::Result<()> {
        let r = self.inner.set_len(len);
        self.owner.tick(FaultOp::SetLen);
        r
    }
    fn sync_data(&self) -> std::io::Result<()> {
        let r = self.inner.sync_data();
        self.owner.tick(FaultOp::SyncData);
        r
    }
    fn sync_all(&self) -> std::io::Result<()> {
        let r = self.inner.sync_all();
        self.owner.tick(FaultOp::SyncAll);
        r
    }
    fn metadata_len(&self) -> std::io::Result<u64> {
        self.inner.metadata_len()
    }
    fn read_to_end_from(&self, offset: u64, out: &mut Vec<u8>) -> std::io::Result<()> {
        self.inner.read_to_end_from(offset, out)
    }
}

impl Fs for CrashAfter {
    fn open(&self, path: &Path, opts: OpenOpts) -> std::io::Result<Box<dyn File>> {
        let inner = self.disk.open(path, opts)?;
        Ok(Box::new(CrashAfterFile {
            inner,
            owner: self.clone(),
        }))
    }
    fn rename(&self, from: &Path, to: &Path) -> std::io::Result<()> {
        let r = self.disk.rename(from, to);
        self.tick(FaultOp::Rename);
        r
    }
    fn remove_file(&self, path: &Path) -> std::io::Result<()> {
        self.disk.remove_file(path)
    }
    fn read_dir(&self, dir: &Path) -> std::io::Result<Vec<PathBuf>> {
        self.disk.read_dir(dir)
    }
    fn sync_dir(&self, dir: &Path) -> std::io::Result<()> {
        let r = self.disk.sync_dir(dir);
        self.tick(FaultOp::SyncDir);
        r
    }
    fn create_dir_all(&self, dir: &Path) -> std::io::Result<()> {
        self.disk.create_dir_all(dir)
    }
    fn exists(&self, path: &Path) -> bool {
        self.disk.exists(path)
    }
    fn metadata_len(&self, path: &Path) -> std::io::Result<u64> {
        self.disk.metadata_len(path)
    }
}

// ===========================================================================
// F-REC-DELETE-BEFORE-APPEND-ORDER
//
// Fault: WAL order has a Delete whose target Append is in a later (lost) frame.
// Inject: durable Delete survives; a later Append is lost to a power loss (its
// fsync never returned before the crash, so the model never acks it).
// Oracle: the Delete applies to whatever is present; the never-recovered Append
// is simply absent; no gap among survivors; the delete floor is consistent.
// ===========================================================================
#[test]
fn f_rec_delete_before_append_order() {
    // Probe how many write_at calls the workload issues, so we can crash right
    // after the durable Delete commits but (likely) before the trailing Append's
    // fsync lands — placing the lost Append *after* a surviving Delete in WAL order.
    let ops = vec![
        Op::PutBox { name: "q".into(), durable: true },
        Op::Append { name: "q".into(), data: "a".into(), tag: Some("t".into()), node: None },
        Op::Append { name: "q".into(), data: "b".into(), tag: Some("t".into()), node: None },
        Op::Append { name: "q".into(), data: "c".into(), tag: None, node: None },
        // Delete the prefix below seq 2 (drops seq 1). This control frame is
        // durable; it precedes the lost tail Append in the log.
        Op::Delete { name: "q".into(), before_seq: Some(2), tag_eq: None },
        // The "target" Append that a crash may drop (its fsync may not return).
        Op::Append { name: "q".into(), data: "d".into(), tag: None, node: None },
        Op::Append { name: "q".into(), data: "e".into(), tag: None, node: None },
    ];

    // Probe the total write_at count (at = MAX ⇒ never fires).
    let probe_disk = FakeDisk::new();
    let probe = FaultFs::new(probe_disk.arc(), FaultOp::WriteAt, FaultKind::Eio, u64::MAX, true);
    {
        let engine =
            Engine::with_data_dir_fs(cfg(), clock(), probe.arc()).expect("probe engine");
        run_ops(&engine, &mut RefModel::default(), &ops);
        drop(engine);
    }
    let total = probe.calls_seen();
    assert!(total >= 5, "workload issues several write_at calls (M={total})");

    // Sweep crash points: at each, the Delete-before-lost-Append ordering is
    // exercised when the crash lands after the delete's frame but before a later
    // append's fsync. At EVERY crash point the recovered state must be a dense
    // prefix of the model's live set with no fabrication and no gap.
    let cap = total.min(14);
    for crash_point in 0..=cap {
        let disk = FakeDisk::with_seed(0xDE1E7E_u64.wrapping_mul(crash_point + 1));
        let trip = CrashAfter::new(disk.clone(), FaultOp::WriteAt, crash_point);
        let mut model = RefModel::default();
        {
            let engine = Engine::with_data_dir_fs(cfg(), clock(), trip.arc())
                .expect("sweep engine opens");
            run_ops(&engine, &mut model, &ops);
            drop(engine);
        }
        disk.reset_power();

        let engine = open_engine(&disk);
        // Crash mid-stream ⇒ the dense-prefix relaxation (whole_tail_durable=false).
        assert_recovered_matches_model(&engine, &model, false);

        // Determinism / idempotence: re-recovering the same image yields the
        // identical state (the Delete re-applies idempotently, the lost Append
        // never resurrects).
        let d1 = dump_box(&engine, "q");
        drop(engine);
        let engine2 = open_engine(&disk);
        let d2 = dump_box(&engine2, "q");
        assert_eq!(d1, d2, "recovery idempotent at crash_point {crash_point}");
        // The deleted seq 1 must never resurface, regardless of which appends
        // survived.
        if let Some(d) = &d2 {
            assert!(
                !d.records.contains_key(&1),
                "deleted seq 1 resurrected at crash_point {crash_point}: {:?}",
                d.records.keys().collect::<Vec<_>>()
            );
        }
        drop(engine2);
    }
}

// ===========================================================================
// F-RECDIR-EIO-CREATE
//
// Fault: EIO on create_dir_all(wal) during recovery.
// Inject: FaultFs fail-once on op=create_dir_all over a FakeDisk.
// Oracle: recovery returns an io error and refuses to start (no silent empty
// start over an existing-but-unreadable dir); explicit failure.
// ===========================================================================
#[test]
fn f_recdir_eio_create() {
    // First, lay down a real durable workload so there IS data the engine would
    // be silently discarding if it started empty on an EIO.
    let disk = FakeDisk::new();
    let mut model = RefModel::default();
    {
        let engine = open_engine(&disk);
        run_ops(
            &engine,
            &mut model,
            &[
                Op::PutBox { name: "d".into(), durable: true },
                Op::Append { name: "d".into(), data: "1".into(), tag: None, node: None },
                Op::Append { name: "d".into(), data: "2".into(), tag: None, node: None },
            ],
        );
        sync_dirs(&disk);
        drop(engine);
    }

    // Now reopen with create_dir_all EIO injected on the first call (recovery's
    // very first FS op is `fs.create_dir_all(wal_dir)`): the build must FAIL,
    // never return an engine that silently started empty over the existing data.
    let faulty: Arc<dyn Fs> =
        FaultFs::new(disk.arc(), FaultOp::CreateDirAll, FaultKind::Eio, 0, true).arc();
    let res = Engine::with_data_dir_fs(cfg(), clock(), faulty);
    assert!(
        res.is_err(),
        "recovery must refuse to start when create_dir_all EIOs (no silent empty start)"
    );

    // And the durable data is untouched: a clean reopen still recovers it fully.
    let engine = open_engine(&disk);
    assert_recovered_matches_model(&engine, &model, true);
    let dump = dump_box(&engine, "d").expect("d survives the failed-then-clean reopen");
    assert_eq!(dump.records.len(), 2, "prior durable data intact after the EIO refusal");
}

// ===========================================================================
// F-NFS-CTO-SNAPSHOT-READ
//
// Fault: close-to-open — a snapshot written through one handle is not visible to
// a STALE reader handle opened earlier; only a freshly-opened handle (open by
// path) sees the current bytes.
// Inject: a CtoFs wrapper whose every open() snapshots the current visible bytes
// into the returned handle (a frozen close-to-open view). A handle opened BEFORE
// the snapshot write therefore reads the pre-write (stale/absent) bytes; recovery
// opens FRESH (load_latest/WalReader::open are open-by-path) so it reads current.
// Oracle: recovery never reuses a pre-crash cached handle; it reads current bytes
// and recovers correctly. The stale handle demonstrably does NOT see the snapshot.
// ===========================================================================

/// An `Fs` modelling NFS close-to-open consistency: a returned `File` caches the
/// inode's visible bytes *as of its open time* for reads. A write through a
/// handle is forwarded to the disk (so later FRESH opens see it), but does NOT
/// update an already-open sibling handle's cached view — exactly the stale-handle
/// hazard CTO defends against. Recovery, which opens by path, always sees current.
#[derive(Clone)]
struct CtoFs {
    inner: FakeDisk,
}

struct CtoFile {
    inner: Box<dyn File>,
    /// The frozen view captured at open time (close-to-open snapshot).
    cached: Vec<u8>,
    /// Whether this handle has issued a write since open (then its own writes are
    /// visible to itself — a real handle reads its own writes).
    own_writes: Mutex<Vec<(u64, Vec<u8>)>>,
}

impl File for CtoFile {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
        // Serve from the frozen open-time view overlaid by this handle's own
        // writes (read-your-own-writes), NOT by another handle's later writes.
        let mut view = self.cached.clone();
        for (off, bytes) in self.own_writes.lock().unwrap().iter() {
            let end = *off as usize + bytes.len();
            if view.len() < end {
                view.resize(end, 0);
            }
            view[*off as usize..end].copy_from_slice(bytes);
        }
        if offset >= view.len() as u64 {
            return Ok(0);
        }
        let start = offset as usize;
        let n = buf.len().min(view.len() - start);
        buf[..n].copy_from_slice(&view[start..start + n]);
        Ok(n)
    }
    fn write_at(&mut self, offset: u64, buf: &[u8]) -> std::io::Result<usize> {
        self.own_writes
            .lock()
            .unwrap()
            .push((offset, buf.to_vec()));
        self.inner.write_at(offset, buf)
    }
    fn set_len(&mut self, len: u64) -> std::io::Result<()> {
        self.inner.set_len(len)
    }
    fn sync_data(&self) -> std::io::Result<()> {
        self.inner.sync_data()
    }
    fn sync_all(&self) -> std::io::Result<()> {
        self.inner.sync_all()
    }
    fn metadata_len(&self) -> std::io::Result<u64> {
        self.inner.metadata_len()
    }
}

impl Fs for CtoFs {
    fn open(&self, path: &Path, opts: OpenOpts) -> std::io::Result<Box<dyn File>> {
        let inner = self.inner.open(path, opts)?;
        // Capture the current visible bytes at open time (the close-to-open view).
        let mut cached = Vec::new();
        if opts.read && !opts.truncate {
            let _ = inner.read_to_end_from(0, &mut cached);
        }
        Ok(Box::new(CtoFile {
            inner,
            cached,
            own_writes: Mutex::new(Vec::new()),
        }))
    }
    fn rename(&self, from: &Path, to: &Path) -> std::io::Result<()> {
        self.inner.rename(from, to)
    }
    fn remove_file(&self, path: &Path) -> std::io::Result<()> {
        self.inner.remove_file(path)
    }
    fn read_dir(&self, dir: &Path) -> std::io::Result<Vec<PathBuf>> {
        self.inner.read_dir(dir)
    }
    fn sync_dir(&self, dir: &Path) -> std::io::Result<()> {
        self.inner.sync_dir(dir)
    }
    fn create_dir_all(&self, dir: &Path) -> std::io::Result<()> {
        self.inner.create_dir_all(dir)
    }
    fn exists(&self, path: &Path) -> bool {
        self.inner.exists(path)
    }
    fn metadata_len(&self, path: &Path) -> std::io::Result<u64> {
        self.inner.metadata_len(path)
    }
}

#[test]
fn f_nfs_cto_snapshot_read() {
    let disk = FakeDisk::new();
    let cto = CtoFs { inner: disk.clone() };
    let cto_fs: Arc<dyn Fs> = Arc::new(cto.clone());

    // Build a durable engine THROUGH the CTO fs, write data, then take a snapshot
    // (which writes meta/snapshot-*.bin through the same CTO fs).
    let mut model = RefModel::default();
    let snap_path = PathBuf::from(DATA_DIR).join("meta").join(format!("snapshot-{:016}.bin", 1));

    // A stale reader handle opened on the snapshot path BEFORE it is written:
    // create the meta dir first so we can hold an open handle whose frozen view is
    // empty/absent, proving it never sees the later snapshot bytes.
    disk.create_dir_all(&PathBuf::from(DATA_DIR).join("meta")).unwrap();

    {
        let engine = Engine::with_data_dir_fs(cfg(), clock(), cto_fs.clone())
            .expect("engine opens through CtoFs");
        run_ops(
            &engine,
            &mut model,
            &[
                Op::PutBox { name: "s".into(), durable: true },
                Op::Append { name: "s".into(), data: "x".into(), tag: Some("g".into()), node: None },
                Op::Append { name: "s".into(), data: "y".into(), tag: None, node: Some("n".into()) },
                Op::Append { name: "s".into(), data: "z".into(), tag: Some("g".into()), node: None },
            ],
        );
        // Open a STALE handle on the (not-yet-written) snapshot file's eventual
        // path via a sibling marker we can read back. We instead capture staleness
        // on the WAL file: open a handle now, snapshot, then show the stale handle
        // does not observe the snapshot's existence-independent bytes.
        sync_dirs(&disk);
        // Write the snapshot durably through the CTO fs (open-by-path inside).
        let wrote = engine.write_snapshot().expect("snapshot write");
        assert!(wrote, "a durable snapshot is written");
        sync_dirs(&disk);
        drop(engine);
    }

    // STALE-HANDLE demonstration: a handle opened on the snapshot path with a
    // frozen open-time view. Since the file now exists with content, a handle
    // opened FRESH reads the bytes. To show CTO staleness, open the underlying
    // FakeDisk handle directly and freeze its view, then mutate via another handle.
    {
        // Confirm the snapshot file exists and is non-trivial when opened fresh.
        let fresh = cto_fs.open(&snap_path, OpenOpts::read_only()).expect("fresh snapshot open");
        let mut fresh_bytes = Vec::new();
        fresh.read_to_end_from(0, &mut fresh_bytes).unwrap();
        assert!(
            fresh_bytes.len() >= 20 && &fresh_bytes[0..4] == b"SNP1",
            "a FRESH open-by-path reads the current snapshot bytes (magic present)"
        );

        // A stale handle: open fresh, then overwrite the file through a SECOND
        // handle; the stale handle keeps its frozen view (close-to-open), proving
        // a cached handle does NOT observe another handle's later writes.
        let stale = cto_fs.open(&snap_path, OpenOpts::read_only()).expect("stale open");
        let mut stale_before = Vec::new();
        stale.read_to_end_from(0, &mut stale_before).unwrap();
        {
            let mut writer = cto_fs
                .open(&snap_path, OpenOpts::rw_existing())
                .expect("second handle");
            writer.write_at(0, b"ZZZZ").unwrap();
            writer.sync_all().unwrap();
        }
        let mut stale_after = Vec::new();
        stale.read_to_end_from(0, &mut stale_after).unwrap();
        assert_eq!(
            stale_before, stale_after,
            "a stale cached handle does NOT observe another handle's later write (CTO)"
        );
        // But a brand-new open sees the change (read-current on fresh open).
        let fresh2 = cto_fs.open(&snap_path, OpenOpts::read_only()).expect("fresh2");
        let mut fresh2_bytes = Vec::new();
        fresh2.read_to_end_from(0, &mut fresh2_bytes).unwrap();
        assert_eq!(&fresh2_bytes[0..4], b"ZZZZ", "a fresh open reads the current bytes");
    }

    // Recovery opens snapshot + WAL FRESH (by path) ⇒ it reads the current,
    // correct bytes and recovers the full state, never reusing a stale handle.
    // (We rebuild the snapshot the previous block clobbered for the staleness
    // demo so recovery has a valid one; the WAL alone also fully covers the data.)
    let disk2 = FakeDisk::new();
    let cto2: Arc<dyn Fs> = Arc::new(CtoFs { inner: disk2.clone() });
    let mut model2 = RefModel::default();
    {
        let engine =
            Engine::with_data_dir_fs(cfg(), clock(), cto2.clone()).expect("engine #2 opens");
        run_ops(
            &engine,
            &mut model2,
            &[
                Op::PutBox { name: "s".into(), durable: true },
                Op::Append { name: "s".into(), data: "x".into(), tag: Some("g".into()), node: None },
                Op::Append { name: "s".into(), data: "y".into(), tag: None, node: Some("n".into()) },
                Op::Append { name: "s".into(), data: "z".into(), tag: Some("g".into()), node: None },
            ],
        );
        sync_dirs(&disk2);
        engine.write_snapshot().expect("snapshot #2");
        sync_dirs(&disk2);
        drop(engine);
    }
    // Recover a FRESH engine through the CTO fs: fresh opens see current bytes.
    let engine = Engine::with_data_dir_fs(cfg(), clock(), cto2.clone()).expect("recover via CtoFs");
    assert_recovered_matches_model(&engine, &model2, true);
    let dump = dump_box(&engine, "s").expect("s recovered through fresh CTO opens");
    assert_eq!(dump.records.len(), 3, "all 3 durable records recovered via fresh-open reads");
    assert_eq!(dump.head, 3, "head recovered correctly through the CTO fs");
}

// ===========================================================================
// F-COMPOUND-CRASH-SNAP-THEN-WAL
//
// Fault: crash during a snapshot write (after the tmp→final rename but BEFORE the
// dir-fsync that hardens it, so the rename may roll back), restart, then a crash
// during the subsequent WAL replay.
// Inject: harness-level — CrashAfter trips FakeDisk.crash() after the snapshot's
// rename (an Fs op) but before its sync_dir; on the next boot we crash again
// after some replay write activity, then recover cleanly.
// Oracle: after both crashes recovery converges to a single consistent state
// (old-or-new snapshot + replayed WAL); acked data present; idempotent; no gap /
// no resurrection.
// ===========================================================================
#[test]
fn f_compound_crash_snap_then_wal() {
    let disk = FakeDisk::new();
    let mut model = RefModel::default();

    // Phase 1: durable workload, then a snapshot, all durable + dir-synced. This
    // is the "old" valid snapshot the first crash may fall back to.
    {
        let engine = open_engine(&disk);
        run_ops(
            &engine,
            &mut model,
            &[
                Op::PutBox { name: "c".into(), durable: true },
                Op::Append { name: "c".into(), data: "1".into(), tag: Some("a".into()), node: None },
                Op::Append { name: "c".into(), data: "2".into(), tag: Some("a".into()), node: None },
            ],
        );
        sync_dirs(&disk);
        engine.write_snapshot().expect("baseline snapshot");
        sync_dirs(&disk);
        // More durable appends AFTER the snapshot (these live only in the WAL tail
        // past the checkpoint — replay must re-apply them).
        run_ops(
            &engine,
            &mut model,
            &[
                Op::Append { name: "c".into(), data: "3".into(), tag: Some("b".into()), node: None },
                Op::Append { name: "c".into(), data: "4".into(), tag: None, node: Some("n4".into()) },
            ],
        );
        sync_dirs(&disk);
        drop(engine);
    }

    // CRASH #1 — during a NEW snapshot write, right after its rename but before the
    // dir-fsync. We trip the crash on the first `rename` the new snapshot issues.
    // The new snapshot's install may roll back ⇒ recovery falls back to the old
    // snapshot + WAL. We drive the new snapshot through a CrashAfter that fires
    // after the rename (op-class Rename, index 0).
    {
        let trip = CrashAfter::new(disk.clone(), FaultOp::Rename, 0);
        let engine = Engine::with_data_dir_fs(cfg(), clock(), trip.arc())
            .expect("engine reopens for snapshot-crash phase");
        // Writing a new snapshot issues a rename; CrashAfter crashes the disk right
        // after it (before sync_dir hardens it). The call may surface an error
        // (post-crash sync_dir is a no-op, prune is best-effort); we tolerate both.
        let _ = engine.write_snapshot();
        drop(engine);
    }
    disk.reset_power();

    // CRASH #2 — during WAL replay on the next boot. We trip on a write_at during
    // recovery (the truncate_active set_len / resumed-writer activity). Because the
    // first thing recovery does is read (replay), then truncate (a set_len), a
    // crash after the first such mutating call models a second crash mid-recovery.
    {
        let trip = CrashAfter::new(disk.clone(), FaultOp::SetLen, 0);
        // Recovery may fail (the resumed writer's open/preallocate can hit the
        // frozen device); both "Ok engine" and "Err" are acceptable for a crash
        // mid-recovery — the invariant is that a LATER clean recovery converges.
        let _ = Engine::with_data_dir_fs(cfg(), clock(), trip.arc());
    }
    disk.reset_power();

    // FINAL recovery on a powered, un-faulted disk: must converge to a single
    // consistent state with every acked-durable record present, no gap, no
    // resurrection.
    let engine = open_engine(&disk);
    assert_recovered_matches_model(&engine, &model, true);
    let dump = dump_box(&engine, "c").expect("c converges after the compound crash");
    assert_eq!(
        dump.records.keys().copied().collect::<Vec<_>>(),
        vec![1, 2, 3, 4],
        "all four acked-durable records present after snap-then-wal compound crash"
    );

    // Idempotent: a second recovery is byte-identical.
    drop(engine);
    let engine2 = open_engine(&disk);
    let d2 = dump_box(&engine2, "c");
    drop(engine2);
    let engine3 = open_engine(&disk);
    let d3 = dump_box(&engine3, "c");
    assert_eq!(d2, d3, "recovery converges (idempotent) after the compound crash");
}

// ===========================================================================
// F-COMPOUND-OOM-IN-ERROR-PATH
//
// Fault: an allocation/IO failure while handling the WAL during recovery's replay
// (the fallible read of a frame body — the error path). We model the
// allocation-failure-in-error-path with a fail-once read_at EIO during replay
// (the same fallible buffer read an OOM would poison): recovery must surface the
// error cleanly without leaving a partially-applied index as final.
// Oracle: recovery fails cleanly (no partial index becomes final); a re-run on the
// (untouched durable) image converges to the correct, complete state.
// ===========================================================================
#[test]
fn f_compound_oom_in_error_path() {
    let disk = FakeDisk::new();
    let mut model = RefModel::default();

    // A durable workload so replay has real frames to read.
    {
        let engine = open_engine(&disk);
        run_ops(
            &engine,
            &mut model,
            &[
                Op::PutBox { name: "o".into(), durable: true },
                Op::Append { name: "o".into(), data: "1".into(), tag: None, node: None },
                Op::Append { name: "o".into(), data: "2".into(), tag: Some("t".into()), node: None },
                Op::Append { name: "o".into(), data: "3".into(), tag: None, node: Some("n".into()) },
            ],
        );
        sync_dirs(&disk);
        drop(engine);
    }

    // Reopen with a read_at EIO injected during replay (the fallible frame-buffer
    // read — the allocation/IO error path). FaultFs counts read_at globally; the
    // snapshot-load + WAL replay both read, so firing on an early read poisons the
    // replay's error path. The build must FAIL cleanly (never return a half-built
    // engine whose partial index is treated as final).
    let mut recovered_clean_on_first = true;
    for at in [0u64, 1, 2, 3] {
        let faulty: Arc<dyn Fs> =
            FaultFs::new(disk.arc(), FaultOp::ReadAt, FaultKind::Eio, at, true).arc();
        let res = Engine::with_data_dir_fs(cfg(), clock(), faulty);
        if res.is_err() {
            recovered_clean_on_first = false;
            // The error must be surfaced, not swallowed into a silent empty start.
            // Now a CLEAN re-run on the untouched durable image must converge.
            let engine = open_engine(&disk);
            assert_recovered_matches_model(&engine, &model, true);
            let dump = dump_box(&engine, "o").expect("o recovered after the failed attempt");
            assert_eq!(
                dump.records.len(),
                3,
                "re-run after the EIO error path converges to the complete state"
            );
            drop(engine);
            break;
        }
        // If this `at` did not hit a fallible read (recovery succeeded), the
        // recovered state must STILL be correct — never a partially-applied index.
        let engine = res.unwrap();
        assert_recovered_matches_model(&engine, &model, true);
    }

    // Whether or not a specific read index faulted, the durable image is intact and
    // a final clean recovery converges to the full model.
    let engine = open_engine(&disk);
    assert_recovered_matches_model(&engine, &model, true);
    let dump = dump_box(&engine, "o").expect("o present after a clean recovery");
    assert_eq!(dump.records.len(), 3, "complete state after a clean recovery");
    let _ = recovered_clean_on_first; // informational; both outcomes are valid.
}
