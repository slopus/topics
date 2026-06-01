//! Phase-8B fault catalog — boundary **wal-fsync**, batch B.
//!
//! Five strategies, one test fn each, driven through the Phase-8A harness
//! ([`FakeDisk`] / [`FaultFs`] + [`Engine::with_data_dir_fs`], the [`RefModel`]
//! oracle copied from `tests/crash_oracle.rs`):
//!
//! - `F-WAL-CRASH-DURING-SHUTDOWN-DRAIN` — power loss during the Drop/shutdown
//!   `drain_and_commit_remaining` final forced fsync: acked frames survive, an
//!   unacked queued frame may vanish, no torn tail, recovery clean.
//! - `F-SNAP-CHECKPOINT-FLUSH-CRASH` — crash mid `write_snapshot()` (the durable
//!   checkpoint flush): no snapshot installed, recovery from prior snapshot+WAL is
//!   consistent, the unwritten snapshot simply doesn't exist.
//! - `F-NFS-FSYNC-LIES` — `sync_data` returns Ok without promoting (nobarrier /
//!   async-NFS commit): on crash acked-but-unflushed writes may be lost, but
//!   recovery is a contiguous prefix, no resurrection, seq monotone, never
//!   silent corruption.
//! - `F-NFS-DELAYED-DURABILITY` — fsync durable only after K further ops, crash
//!   inside the window: an in-window frame is the torn-tail/lost case (truncate),
//!   never corruption, survivors contiguous.
//! - `F-COMPOUND-FSYNC-FAIL-THEN-CRASH` — durable `sync_data` fails (batch rolled
//!   back, not acked), then a crash before the next batch: the failed batch is
//!   never acked nor published, recovery loses nothing acked and is contiguous.
//!
//! Bounded by design: small fixed workloads, fixed seeds, capped sweeps — the
//! whole file runs well under a minute.
//!
//! ```text
//! cargo test --features test-fs --test fault_wal_fsync_b
//! ```

#![cfg(feature = "test-fs")]
#![allow(
    clippy::ptr_arg,
    clippy::manual_clamp,
    clippy::unusual_byte_groupings,
    clippy::doc_lazy_continuation
)]

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::json;

use streams::clock::{SharedClock, TestClock};
use streams::config::ServerConfig;
use streams::engine::Engine;
use streams::storage::testfs::{FakeDisk, FaultFs, FaultKind, FaultOp, TornDamage};
use streams::storage::{File, Fs, OpenOpts};
use streams::types::{BoxConfig, BoxType, DiffRequest, RecordIn, WriteRequest};

// ===========================================================================
// Reference model (the oracle) — copied from tests/crash_oracle.rs, trimmed to
// the append/box surface these wal-fsync strategies exercise.
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
    /// Every acked record by seq (the must-survive set for a durable box).
    acked: BTreeMap<u64, ModelRecord>,
    /// Every record ever acked — the universe recovery may ever surface (no
    /// fabrication is `survivors ⊆ ever_acked`).
    ever_acked: BTreeMap<u64, ModelRecord>,
    head: u64,
}

impl ModelBox {
    fn live_seqs(&self) -> Vec<u64> {
        self.acked.keys().copied().collect()
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
}

// ===========================================================================
// Op stream
// ===========================================================================

#[derive(Debug, Clone)]
enum Op {
    PutBox {
        name: String,
        durable: bool,
    },
    Append {
        name: String,
        data: String,
        tag: Option<String>,
        node: Option<String>,
    },
}

/// Drive `ops` against a real engine, mirroring acked effects into `model`. A
/// durable op is mirrored only if the engine call returns `Ok` (its group fsync
/// returned ⇒ acked ⇒ must survive). A failed/aborted op is NOT mirrored.
fn run_ops(engine: &Engine, model: &mut RefModel, ops: &[Op]) {
    for op in ops {
        match op {
            Op::PutBox { name, durable } => {
                let cfg = BoxConfig {
                    r#type: BoxType::Log,
                    durable: *durable,
                    ..Default::default()
                };
                if engine.put_box(name, cfg).is_ok() {
                    model.ensure_box(name, *durable);
                }
            }
            Op::Append {
                name,
                data,
                tag,
                node,
            } => {
                if !model.boxes.contains_key(name) {
                    model.ensure_box(name, false);
                }
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
        }
    }
}

// ===========================================================================
// Engine build / recover plumbing through an injected Fs
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

fn open_engine_fs(fs: Arc<dyn Fs>) -> Arc<Engine> {
    Engine::with_data_dir_fs(cfg(), clock(), fs).expect("engine opens through injected fs")
}

/// Cleanly drop an engine (its WAL owner's Drop signals shutdown + joins the
/// writer, draining queued frames). To simulate a power loss instead, call
/// `disk.crash(..)` BEFORE dropping so the drain runs against a frozen device.
fn stop(engine: Arc<Engine>) {
    drop(engine);
}

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
                    max_batch_bytes: 0,
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

// ===========================================================================
// The oracle (crash-consistency contract) — from tests/crash_oracle.rs.
// ===========================================================================

/// Assert the contract for one box. `whole_tail_durable=true` ⇒ the survivor set
/// must equal the model's live set; `false` ⇒ a clean dense prefix is allowed
/// (an acked-but-unflushed/in-window tail may be missing).
fn assert_box_contract(name: &str, model: &ModelBox, dump: &BoxDump, whole_tail_durable: bool) {
    let live = model.live_seqs();
    let survivors: Vec<u64> = dump.records.keys().copied().collect();

    // (1) NO FABRICATION: survivors ⊆ ever_acked, byte-for-byte.
    for (seq, rec) in &dump.records {
        let m = model.ever_acked.get(seq).unwrap_or_else(|| {
            panic!("{name}: recovered seq {seq} was never acked (fabricated/torn): {rec:?}")
        });
        assert_eq!(
            m, rec,
            "{name}: recovered record at seq {seq} differs from the model"
        );
    }

    // (2) NO SILENT LOSS of acked-durable when the whole tail was durable.
    if model.durable && whole_tail_durable {
        for seq in &live {
            assert!(
                dump.records.contains_key(seq),
                "{name}: acked durable seq {seq} LOST (survivors={survivors:?}, live={live:?})"
            );
        }
    }

    // (3) NO GAP — survivors are a dense prefix of the model's live set below the
    //     surviving high-water (a crash drops a tail, never a middle live record).
    if let Some(&hi) = survivors.last() {
        if whole_tail_durable {
            let expected_prefix: Vec<u64> = live.iter().copied().filter(|s| *s <= hi).collect();
            assert_eq!(
                survivors, expected_prefix,
                "{name}: survivors must be a dense prefix (survivors={survivors:?}, live={live:?})"
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
            // Contiguity among survivors themselves: no gap between two surviving
            // seqs (the recovered set is a dense run up to `hi`).
            if let (Some(&lo), Some(&hi)) = (survivors.first(), survivors.last()) {
                for s in lo..=hi {
                    // Only those that are model-live below hi must be present; a
                    // tag/delete hole is not exercised here, so the run is dense.
                    assert!(
                        dump.records.contains_key(&s),
                        "{name}: gap at seq {s} in [{lo}..={hi}]: survivors={survivors:?}"
                    );
                }
            }
        }
    }

    // (4) HEAD MONOTONE / NO SEQ REUSE (R3). An `fsync` box (`model.durable`) has
    //     no head reservation: its head matches the acked head exactly (never a
    //     future seq). A `disk` box (acked before fsync) recovers at its durable
    //     head RESERVATION, so its head may sit ABOVE the acked head by up to
    //     `DISK_HEAD_RESERVE_AHEAD` (dropped un-fsynced seqs become silent gaps) and
    //     never regresses below an acked seq when the whole tail was durable.
    if model.durable {
        assert!(
            dump.head <= model.head,
            "{name}: fsync recovered head {} exceeds model head {} (future seq?)",
            dump.head,
            model.head
        );
        if whole_tail_durable && !live.is_empty() {
            assert_eq!(
                dump.head, model.head,
                "{name}: durable head must match model"
            );
        }
    } else {
        if whole_tail_durable && model.head > 0 {
            assert!(
                dump.head >= model.head,
                "{name}: disk recovered head {} REGRESSED below acked head {} (reuse!)",
                dump.head,
                model.head
            );
        }
        assert!(
            dump.head <= model.head + streams::config::DISK_HEAD_RESERVE_AHEAD,
            "{name}: disk recovered head {} exceeds reservation ceiling {}",
            dump.head,
            model.head + streams::config::DISK_HEAD_RESERVE_AHEAD
        );
    }

    // (5) EARLIEST: when the whole durable tail survived, earliest matches.
    if model.durable && whole_tail_durable && !live.is_empty() {
        assert_eq!(
            dump.earliest,
            model.earliest(),
            "{name}: earliest_seq must match model"
        );
    }

    // No involuntary loss ⇒ no tombstone should appear (these workloads have no cap).
    assert!(
        dump.tombstone_reason.is_none(),
        "{name}: unexpected tombstone {:?} (no cap/TTL in workload)",
        dump.tombstone_reason
    );
}

fn assert_recovered_matches_model(engine: &Engine, model: &RefModel, whole_tail_durable: bool) {
    for (name, mbox) in &model.boxes {
        if mbox.acked.is_empty() && mbox.head == 0 {
            continue;
        }
        let Some(dump) = dump_box(engine, name) else {
            if mbox.durable && !mbox.live_seqs().is_empty() && whole_tail_durable {
                panic!("{name}: durable box with acked records vanished after recovery");
            }
            continue;
        };
        assert_box_contract(name, mbox, &dump, whole_tail_durable);
    }
}

/// Make the WAL + meta dir names durable (models the create+dir-fsync prod does
/// at WAL open / snapshot install).
fn sync_dirs(fs: &Arc<dyn Fs>) {
    let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("wal"));
    let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("meta"));
}

// ===========================================================================
// DelayFs — promote a file's pending bytes only after K *further* fsyncs across
// the disk (NFS delayed-durability model). A `sync_data`/`sync_all` returns Ok
// but does NOT promote *this* call's pending bytes; instead it promotes the
// pending bytes that were outstanding K fsyncs ago. A `crash()` inside that
// window loses the still-un-promoted bytes — exactly "durable only after a delay".
// ===========================================================================

#[derive(Default)]
struct DelayState {
    /// FIFO of (inode-path, snapshot-of-pending-promotion) deferred fsyncs. We
    /// model the delay by counting fsyncs and only flushing the inner disk's
    /// real promotion once `k` later fsyncs have happened.
    queue: std::collections::VecDeque<PathBuf>,
}

/// An Fs wrapper that defers each file's durability by `k` fsync calls. It wraps a
/// [`FakeDisk`] whose own `sync_data` would promote immediately; we intercept and
/// hold the promotion back by re-routing through a "lying then later honest" plan:
/// the wrapped disk is kept in lying-fsync mode (never auto-promotes), and we
/// manually replay an honest fsync on a file only once `k` subsequent fsyncs have
/// elapsed.
#[derive(Clone)]
struct DelayFs {
    disk: FakeDisk,
    k: usize,
    state: Arc<Mutex<DelayState>>,
}

impl DelayFs {
    fn new(disk: FakeDisk, k: usize) -> Self {
        // Keep the underlying disk from promoting on its own fsync; we drive
        // promotion explicitly (deferred) via a dedicated honest handle.
        disk.set_lying_fsync(true);
        DelayFs {
            disk,
            k,
            state: Arc::new(Mutex::new(DelayState::default())),
        }
    }

    fn arc(&self) -> Arc<dyn Fs> {
        Arc::new(self.clone())
    }

    /// Record a logical fsync of `path` and, if `k` fsyncs have since elapsed,
    /// promote the file that is now due by issuing one honest (non-lying) fsync on
    /// a fresh handle to it.
    fn on_fsync(&self, path: &Path) {
        let due: Option<PathBuf> = {
            let mut st = self.state.lock().unwrap();
            st.queue.push_back(path.to_path_buf());
            if st.queue.len() > self.k {
                st.queue.pop_front()
            } else {
                None
            }
        };
        if let Some(p) = due {
            self.honest_promote(&p);
        }
    }

    /// Promote `p`'s pending bytes for real: flip the disk honest, fsync a fresh
    /// handle, flip it back to lying so future writes stay deferred.
    fn honest_promote(&self, p: &Path) {
        if let Ok(f) = self.disk.open(p, OpenOpts::rw_existing()) {
            self.disk.set_lying_fsync(false);
            let _ = f.sync_data();
            self.disk.set_lying_fsync(true);
        }
    }
}

struct DelayFile {
    inner: Box<dyn File>,
    owner: DelayFs,
    path: PathBuf,
}

impl File for DelayFile {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read_at(offset, buf)
    }
    fn write_at(&mut self, offset: u64, buf: &[u8]) -> io::Result<usize> {
        self.inner.write_at(offset, buf)
    }
    fn set_len(&mut self, len: u64) -> io::Result<()> {
        self.inner.set_len(len)
    }
    fn sync_data(&self) -> io::Result<()> {
        // Report success (the caller acks) but defer real durability.
        self.inner.sync_data()?;
        self.owner.on_fsync(&self.path);
        Ok(())
    }
    fn sync_all(&self) -> io::Result<()> {
        self.inner.sync_all()?;
        self.owner.on_fsync(&self.path);
        Ok(())
    }
    fn metadata_len(&self) -> io::Result<u64> {
        self.inner.metadata_len()
    }
    fn read_to_end_from(&self, offset: u64, out: &mut Vec<u8>) -> io::Result<()> {
        self.inner.read_to_end_from(offset, out)
    }
}

impl Fs for DelayFs {
    fn open(&self, path: &Path, opts: OpenOpts) -> io::Result<Box<dyn File>> {
        let inner = self.disk.open(path, opts)?;
        Ok(Box::new(DelayFile {
            inner,
            owner: self.clone(),
            path: path.to_path_buf(),
        }))
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.disk.rename(from, to)
    }
    fn remove_file(&self, path: &Path) -> io::Result<()> {
        self.disk.remove_file(path)
    }
    fn read_dir(&self, dir: &Path) -> io::Result<Vec<PathBuf>> {
        self.disk.read_dir(dir)
    }
    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        self.disk.sync_dir(dir)
    }
    fn create_dir_all(&self, dir: &Path) -> io::Result<()> {
        self.disk.create_dir_all(dir)
    }
    fn exists(&self, path: &Path) -> bool {
        self.disk.exists(path)
    }
    fn metadata_len(&self, path: &Path) -> io::Result<u64> {
        self.disk.metadata_len(path)
    }
}

// ===========================================================================
// CrashAfter — fire FakeDisk.crash() after the Nth call of a chosen op class.
// (Copied from tests/crash_oracle.rs; used by the snapshot-flush sweep.)
// ===========================================================================

#[derive(Clone)]
struct CrashAfter {
    disk: FakeDisk,
    op: FaultOp,
    at: u64,
    seen: Arc<AtomicU64>,
    tripped: Arc<std::sync::atomic::AtomicBool>,
}

impl CrashAfter {
    fn new(disk: FakeDisk, op: FaultOp, at: u64) -> Self {
        CrashAfter {
            disk,
            op,
            at,
            seen: Arc::new(AtomicU64::new(0)),
            tripped: Arc::new(std::sync::atomic::AtomicBool::new(false)),
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
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read_at(offset, buf)
    }
    fn write_at(&mut self, offset: u64, buf: &[u8]) -> io::Result<usize> {
        let r = self.inner.write_at(offset, buf);
        self.owner.tick(FaultOp::WriteAt);
        r
    }
    fn set_len(&mut self, len: u64) -> io::Result<()> {
        let r = self.inner.set_len(len);
        self.owner.tick(FaultOp::SetLen);
        r
    }
    fn sync_data(&self) -> io::Result<()> {
        let r = self.inner.sync_data();
        self.owner.tick(FaultOp::SyncData);
        r
    }
    fn sync_all(&self) -> io::Result<()> {
        let r = self.inner.sync_all();
        self.owner.tick(FaultOp::SyncAll);
        r
    }
    fn metadata_len(&self) -> io::Result<u64> {
        self.inner.metadata_len()
    }
    fn read_to_end_from(&self, offset: u64, out: &mut Vec<u8>) -> io::Result<()> {
        self.inner.read_to_end_from(offset, out)
    }
}

impl Fs for CrashAfter {
    fn open(&self, path: &Path, opts: OpenOpts) -> io::Result<Box<dyn File>> {
        let inner = self.disk.open(path, opts)?;
        Ok(Box::new(CrashAfterFile {
            inner,
            owner: self.clone(),
        }))
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        let r = self.disk.rename(from, to);
        self.tick(FaultOp::Rename);
        r
    }
    fn remove_file(&self, path: &Path) -> io::Result<()> {
        self.disk.remove_file(path)
    }
    fn read_dir(&self, dir: &Path) -> io::Result<Vec<PathBuf>> {
        self.disk.read_dir(dir)
    }
    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        let r = self.disk.sync_dir(dir);
        self.tick(FaultOp::SyncDir);
        r
    }
    fn create_dir_all(&self, dir: &Path) -> io::Result<()> {
        self.disk.create_dir_all(dir)
    }
    fn exists(&self, path: &Path) -> bool {
        self.disk.exists(path)
    }
    fn metadata_len(&self, path: &Path) -> io::Result<u64> {
        self.disk.metadata_len(path)
    }
}

// ===========================================================================
// F-WAL-CRASH-DURING-SHUTDOWN-DRAIN
// ===========================================================================

/// Power loss during the Drop/shutdown `drain_and_commit_remaining` final forced
/// fsync. The harness models the drain as the engine's `_wal_owner` Drop joining
/// the writer; freezing the device with `crash()` BEFORE the drop makes the final
/// drain a no-op on a powered-off device — exactly a crash *during* the drain.
///
/// ORACLE: frames whose group fsync returned before the crash (acked) survive; an
/// unacked queued frame may vanish; no torn tail; recovery clean (contiguous
/// prefix, no fabrication).
#[test]
fn f_wal_crash_during_shutdown_drain() {
    let disk = FakeDisk::new();
    let mut model = RefModel::default();

    // A durable acked prefix (group fsync returned ⇒ must survive), then a
    // non-durable burst that is only queued (its durability rides the shutdown
    // drain — which the crash interrupts).
    let mut ops = vec![
        Op::PutBox {
            name: "drain".into(),
            durable: true,
        },
        Op::Append {
            name: "drain".into(),
            data: "d1".into(),
            tag: Some("a".into()),
            node: None,
        },
        Op::Append {
            name: "drain".into(),
            data: "d2".into(),
            tag: Some("a".into()),
            node: Some("n".into()),
        },
        Op::Append {
            name: "drain".into(),
            data: "d3".into(),
            tag: Some("b".into()),
            node: None,
        },
    ];
    // A non-durable box whose tail is only flushed (if at all) by the drain.
    ops.push(Op::PutBox {
        name: "fast".into(),
        durable: false,
    });
    for i in 0..8 {
        ops.push(Op::Append {
            name: "fast".into(),
            data: format!("f{i}"),
            tag: None,
            node: None,
        });
    }

    {
        let engine = open_engine(&disk);
        run_ops(&engine, &mut model, &ops);
        sync_dirs(&disk.arc());
        // Power loss BEFORE the shutdown drain: freeze the device, then drop the
        // engine so its writer's `drain_and_commit_remaining` runs against a frozen
        // disk (hardens nothing new — the crash interrupts the drain).
        disk.crash(TornDamage::None);
        stop(engine);
    }
    disk.reset_power();

    let engine = open_engine(&disk);
    // The durable box's acked writes all survived the drain crash.
    let dump_d = dump_box(&engine, "drain").expect("durable box survives drain crash");
    assert_box_contract("drain", &model.boxes["drain"], &dump_d, true);
    assert_eq!(
        dump_d.records.len(),
        3,
        "all 3 acked durable frames survive the shutdown-drain crash"
    );

    // The non-durable box: whatever survived is a clean dense prefix — no torn
    // tail, no fabricated frame. (Its queued tail may legitimately be gone.)
    if let Some(dump_f) = dump_box(&engine, "fast") {
        assert_box_contract("fast", &model.boxes["fast"], &dump_f, false);
    }
    stop(engine);
}

// ===========================================================================
// F-SNAP-CHECKPOINT-FLUSH-CRASH
// ===========================================================================

/// Crash during `write_snapshot()`'s durable checkpoint flush (the snapshot is
/// written through the injected fs; a `crash()` after the i-th mutating FS call
/// of the snapshot path interrupts it). A bounded sweep over the snapshot's FS
/// calls: at every crash point either the new snapshot installed or it didn't,
/// and recovery from prior snapshot + WAL reproduces the model exactly.
///
/// ORACLE: no half-snapshot is ever loaded; the unwritten snapshot simply doesn't
/// exist; recovery is consistent (every acked durable frame present, contiguous).
#[test]
fn f_snap_checkpoint_flush_crash() {
    // The workload whose state the snapshot captures (all durable acked). Kept
    // small so the bounded crash-point sweep stays well under a minute (each
    // durable append blocks on a real group fsync).
    let ops = vec![
        Op::PutBox {
            name: "snap".into(),
            durable: true,
        },
        Op::Append {
            name: "snap".into(),
            data: "s1".into(),
            tag: Some("a".into()),
            node: None,
        },
        Op::Append {
            name: "snap".into(),
            data: "s2".into(),
            tag: Some("a".into()),
            node: Some("n".into()),
        },
        Op::Append {
            name: "snap".into(),
            data: "s3".into(),
            tag: Some("b".into()),
            node: None,
        },
    ];

    // Probe: how many write_at calls does the snapshot path itself issue? We only
    // need to crash across the snapshot's own writes, so reopen the laid-down
    // durable image and count just the `write_snapshot()` write_at span.
    let probe_disk = FakeDisk::new();
    {
        let mut throwaway = RefModel::default();
        let engine = open_engine(&probe_disk);
        run_ops(&engine, &mut throwaway, &ops);
        stop(engine);
    }
    let snap_writes = {
        let probe = FaultFs::new(
            probe_disk.arc(),
            FaultOp::WriteAt,
            FaultKind::Eio,
            u64::MAX,
            true,
        );
        let engine = open_engine_fs(probe.arc());
        let before = probe.calls_seen();
        engine.write_snapshot().expect("probe snapshot writes");
        let span = probe.calls_seen() - before;
        stop(engine);
        span
    };
    // Cap the sweep at the snapshot's own write span (bounded) so it covers every
    // interesting boundary of the snapshot flush while staying fast.
    let cap = snap_writes.max(1).min(8);

    // Tiered sweep (streams::testutil::crash_points): bounded deterministic sample
    // by default, full `0..=cap` under STREAMS_TEST_EXHAUSTIVE.
    for crash_point in streams::testutil::crash_points(cap) {
        let disk = FakeDisk::with_seed(0x5A0_0000 ^ crash_point);
        let mut model = RefModel::default();

        // Phase 1: lay down the durable workload cleanly (no crash yet).
        {
            let engine = open_engine(&disk);
            run_ops(&engine, &mut model, &ops);
            sync_dirs(&disk.arc());
            stop(engine);
        }

        // Phase 2: reopen and attempt a snapshot through a CrashAfter that fires a
        // power loss after the `crash_point`-th write_at of the snapshot path. The
        // snapshot may complete, partially write, or not install at all.
        {
            let trip = CrashAfter::new(disk.clone(), FaultOp::WriteAt, crash_point);
            let engine = open_engine_fs(trip.arc());
            // The snapshot may error (frozen mid-write) or succeed; both are fine —
            // we only require recovery consistency afterward.
            let _ = engine.write_snapshot();
            stop(engine);
        }
        disk.reset_power();

        // Recovery: prior-snapshot-or-none + WAL must reproduce the model exactly
        // (all acked durable frames present, contiguous, no half-snapshot loaded).
        let engine = open_engine(&disk);
        assert_recovered_matches_model(&engine, &model, true);
        let dump = dump_box(&engine, "snap").expect("box survives snapshot-flush crash");
        assert_eq!(
            dump.records.keys().copied().collect::<Vec<_>>(),
            vec![1, 2, 3],
            "all 3 acked durable frames present at crash_point {crash_point}"
        );
        // Idempotent recovery at a couple of points.
        if crash_point % 5 == 0 {
            let again = open_engine(&disk);
            let d2 = dump_box(&again, "snap");
            assert_eq!(
                Some(dump.clone()),
                d2,
                "recovery idempotent at crash_point {crash_point}"
            );
            stop(again);
        }
        stop(engine);
    }
}

// ===========================================================================
// F-NFS-FSYNC-LIES
// ===========================================================================

/// `sync_data`/`sync_all` return Ok WITHOUT promoting pending bytes (nobarrier /
/// async-NFS commit). A durable write is "acked" to the client but the bytes are
/// never durable; a `crash()` then loses them.
///
/// ORACLE: acked-but-actually-unflushed writes may be lost, but recovery still
/// yields a contiguous prefix, no resurrection, seq monotone among survivors;
/// loss is never silent corruption (no fabricated/torn frame). We assert the
/// always-true subset contract (survivors ⊆ ever_acked, dense, monotone) — the
/// must-survive guarantee is relaxed because the lying fsync broke acked⇒durable.
#[test]
fn f_nfs_fsync_lies() {
    let disk = FakeDisk::new();
    let mut model = RefModel::default();

    // Phase 1: a few HONEST durable writes (fsync promotes) so we have a guaranteed
    // surviving prefix — these are genuinely durable.
    {
        let engine = open_engine(&disk);
        run_ops(
            &engine,
            &mut model,
            &[
                Op::PutBox {
                    name: "nfs".into(),
                    durable: true,
                },
                Op::Append {
                    name: "nfs".into(),
                    data: "h1".into(),
                    tag: Some("a".into()),
                    node: None,
                },
                Op::Append {
                    name: "nfs".into(),
                    data: "h2".into(),
                    tag: Some("a".into()),
                    node: Some("n".into()),
                },
            ],
        );
        sync_dirs(&disk.arc());
        stop(engine);
    }
    let honest_prefix = model.boxes["nfs"].acked.len();
    assert_eq!(
        honest_prefix, 2,
        "two honest-durable frames form the guaranteed prefix"
    );

    // Phase 2: flip the disk to LYING fsync. Reopen, append more "durable" writes
    // that the engine acks but whose bytes never promote. Mirror them into the
    // model (the engine acked them) — recovery is allowed to drop this tail.
    disk.set_lying_fsync(true);
    {
        let engine = open_engine(&disk);
        run_ops(
            &engine,
            &mut model,
            &[
                Op::Append {
                    name: "nfs".into(),
                    data: "l3".into(),
                    tag: Some("b".into()),
                    node: None,
                },
                Op::Append {
                    name: "nfs".into(),
                    data: "l4".into(),
                    tag: None,
                    node: Some("n2".into()),
                },
                Op::Append {
                    name: "nfs".into(),
                    data: "l5".into(),
                    tag: Some("c".into()),
                    node: None,
                },
            ],
        );
        sync_dirs(&disk.arc());
        // Power loss: the lying-fsync'd bytes were never durable ⇒ dropped.
        disk.crash(TornDamage::None);
        stop(engine);
    }
    disk.reset_power();
    disk.set_lying_fsync(false);

    let engine = open_engine(&disk);
    let dump = dump_box(&engine, "nfs").expect("box survives with at least the honest prefix");

    // Contract (relaxed, whole_tail_durable=false): no fabrication, dense prefix,
    // monotone, no resurrection. The lie may have lost the l3..l5 tail.
    assert_box_contract("nfs", &model.boxes["nfs"], &dump, false);
    // The genuinely-durable prefix [1,2] MUST survive (those fsyncs were honest).
    assert!(
        dump.records.contains_key(&1) && dump.records.contains_key(&2),
        "the honest-durable prefix survives the lying-fsync crash: {:?}",
        dump.records.keys().collect::<Vec<_>>()
    );
    // Whatever survived is a contiguous prefix starting at 1 — never a gap.
    let surv: Vec<u64> = dump.records.keys().copied().collect();
    for (i, s) in surv.iter().enumerate() {
        assert_eq!(
            *s,
            i as u64 + 1,
            "survivors are a dense prefix, no gap: {surv:?}"
        );
    }
    stop(engine);
}

// ===========================================================================
// F-NFS-DELAYED-DURABILITY
// ===========================================================================

/// fsync is durable only after a delay (K further fsyncs); a crash inside the
/// window loses the not-yet-promoted frames. Modeled by [`DelayFs`] (k=2): each
/// file's promotion is deferred two fsyncs.
///
/// ORACLE: a frame acked but still inside the delay window is the torn-tail/lost
/// case (truncate), never corruption; survivors contiguous, no fabrication.
#[test]
fn f_nfs_delayed_durability() {
    let disk = FakeDisk::new();
    let delay = DelayFs::new(disk.clone(), 2);
    let mut model = RefModel::default();

    let ops = vec![
        Op::PutBox {
            name: "dly".into(),
            durable: true,
        },
        Op::Append {
            name: "dly".into(),
            data: "1".into(),
            tag: Some("a".into()),
            node: None,
        },
        Op::Append {
            name: "dly".into(),
            data: "2".into(),
            tag: Some("a".into()),
            node: Some("n".into()),
        },
        Op::Append {
            name: "dly".into(),
            data: "3".into(),
            tag: Some("b".into()),
            node: None,
        },
        Op::Append {
            name: "dly".into(),
            data: "4".into(),
            tag: None,
            node: None,
        },
        Op::Append {
            name: "dly".into(),
            data: "5".into(),
            tag: Some("c".into()),
            node: None,
        },
    ];

    {
        let engine = open_engine_fs(delay.arc());
        run_ops(&engine, &mut model, &ops);
        sync_dirs(&delay.arc());
        // Power loss INSIDE the durability window: the last ~k frames' promotions
        // are still deferred and are lost; earlier frames whose delay elapsed are
        // durable. (The underlying disk is in lying mode; crash drops un-promoted.)
        disk.crash(TornDamage::None);
        stop(engine);
    }
    disk.reset_power();
    disk.set_lying_fsync(false);

    let engine = open_engine(&disk);
    if let Some(dump) = dump_box(&engine, "dly") {
        // Relaxed contract: the in-window tail may be truncated, but survivors are
        // a dense prefix with no fabrication / no gap / monotone.
        assert_box_contract("dly", &model.boxes["dly"], &dump, false);
        let surv: Vec<u64> = dump.records.keys().copied().collect();
        for (i, s) in surv.iter().enumerate() {
            assert_eq!(
                *s,
                i as u64 + 1,
                "delayed-durability survivors are a dense prefix: {surv:?}"
            );
        }
        // The crash was inside the window, so a strict prefix (not the whole tail)
        // is the lost case — but a clean recovery (no torn frame) is the must-hold.
    }
    stop(engine);
}

// ===========================================================================
// F-COMPOUND-FSYNC-FAIL-THEN-CRASH
// ===========================================================================

/// A durable group-commit `sync_data` fails (EIO, fail-once) so the batch is
/// rolled back and the write returns Err (NOT acked, NOT published). Then a power
/// loss happens before any further batch.
///
/// ORACLE: the failed batch was never acked nor published; the box is left clean
/// by the rollback; the crash loses nothing acked; recovery is contiguous and
/// reproduces the model exactly (the failed frame leaves no trace).
#[test]
fn f_compound_fsync_fail_then_crash() {
    let disk = FakeDisk::new();
    let mut model = RefModel::default();

    // Phase 1: 3 honest durable acked writes on a clean disk.
    {
        let engine = open_engine(&disk);
        run_ops(
            &engine,
            &mut model,
            &[
                Op::PutBox {
                    name: "cmp".into(),
                    durable: true,
                },
                Op::Append {
                    name: "cmp".into(),
                    data: "1".into(),
                    tag: Some("a".into()),
                    node: None,
                },
                Op::Append {
                    name: "cmp".into(),
                    data: "2".into(),
                    tag: None,
                    node: Some("n".into()),
                },
                Op::Append {
                    name: "cmp".into(),
                    data: "3".into(),
                    tag: Some("b".into()),
                    node: None,
                },
            ],
        );
        sync_dirs(&disk.arc());
        stop(engine);
    }

    // Phase 2: wrap the disk so the next durable group-commit `sync_data` fails
    // once. The 4th durable append MUST return Err (batch rolled back, not acked),
    // so it is NOT mirrored into the model.
    {
        let faulty: Arc<dyn Fs> =
            FaultFs::new(disk.arc(), FaultOp::SyncData, FaultKind::Eio, 0, true).arc();
        let engine = open_engine_fs(faulty);
        let req = WriteRequest {
            records: vec![RecordIn {
                data: json!({ "v": "4" }),
                tag: None,
                node: None,
                meta: None,
            }],
            node: None,
            idempotency_key: None,
            create: Some(true),
            config: None,
            disable_backpressure: true,
        };
        let res = engine.write("cmp", req, true);
        assert!(
            res.is_err(),
            "durable append must fail when the group-commit fsync EIOs"
        );
        // NOT mirrored (unacked). Its bytes were buffered but the fsync errored, so
        // they are not durable. Power loss BEFORE the next batch: freeze, then drop.
        disk.crash(TornDamage::None);
        stop(engine);
    }
    disk.reset_power();

    // Recovery: exactly the 3 prior durable frames, matching the model — the failed
    // 4th batch left no trace, nothing acked was lost, survivors contiguous.
    let engine = open_engine(&disk);
    assert_recovered_matches_model(&engine, &model, true);
    let dump = dump_box(&engine, "cmp").expect("box survives the fsync-fail-then-crash");
    assert_eq!(
        dump.records.keys().copied().collect::<Vec<_>>(),
        vec![1, 2, 3],
        "failed batch left no trace; prior 3 acked frames intact and contiguous"
    );
    assert_eq!(
        dump.head, 3,
        "head did not advance for the unacked failed batch"
    );
    stop(engine);
}
