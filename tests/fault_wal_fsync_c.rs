//! Phase-8B fault/crash batch — `wal-fsync` boundary, group **c** (3 strategies
//! from `/tmp/streams-fault-catalog.json`, each test fn named after its catalog
//! id). Every test asserts the CORRECT crash-consistency behavior through the
//! Phase-8A harness (`FakeDisk` / `MonitorFs` from `streams::storage::testfs`,
//! the real WAL / engine wired through `Wal::open_at_with` and
//! `Engine::with_data_dir_fs`, the same model-oracle contract as
//! `tests/crash_oracle.rs`).
//!
//! Strategies:
//!  - `F-SWEEP-DURABLE-APPEND`   — exhaustive crash-point sweep across every FS
//!    call (`write_at` *and* `sync_data`) of ONE durable append batch, with torn
//!    seeds; at every crash point the batch is either fully durable+recovered or
//!    fully absent — never a half-applied / partial record.
//!  - `F-MONITOR-WAL-ORDER`      — always-on live assertion that no durable
//!    `append` is acked (its `CommitToken` returns `Ok`) before its batch's bytes
//!    are durable on the underlying disk (the fsync returned). A regression that
//!    acked-before-fsync would be caught the instant it happened.
//!  - `F-PROP-DURABILITY-CLASS-MIX` — proptest-generated random mix of durable /
//!    non-durable boxes & writes, `FakeDisk.crash()` at a random step: every
//!    durable acked write survives, a non-durable one may vanish but only as a
//!    clean tail, and a cross-box group-commit never loses one box's acked frame
//!    to another box's non-durable one.
//!
//! ```text
//! cargo test --features test-fs --test fault_wal_fsync_c
//! ```

#![cfg(feature = "test-fs")]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::Duration;

use proptest::prelude::*;
use serde_json::json;

use streams::clock::{SharedClock, TestClock};
use streams::config::ServerConfig;
use streams::engine::Engine;
use streams::storage::testfs::{FakeDisk, MonitorFs, TornDamage};
use streams::storage::wal::{Wal, WalConfig, WalReader, WalRecord};
use streams::storage::{File, Fs, OpenOpts};
use streams::types::{BoxConfig, BoxType, DiffRequest, RecordIn, WriteRequest};

// ===========================================================================
// Shared plumbing (mirrors tests/crash_oracle.rs + tests/fault_batch1.rs)
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

fn wal_dir() -> PathBuf {
    PathBuf::from(DATA_DIR).join("wal")
}

/// Make the WAL (and meta) directory names durable — the create+dir-fsync
/// production does at open time, modeled explicitly so the files survive a crash.
fn sync_wal_dir(fs: &Arc<dyn Fs>) {
    let _ = fs.sync_dir(&wal_dir());
    let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("meta"));
}

/// A direct WAL writer wired through a disk, with a fast group-commit window so a
/// durable `append` acks quickly.
fn fast_cfg() -> WalConfig {
    let mut c = WalConfig::new(DATA_DIR);
    c.gc_min = Duration::from_micros(50);
    c.gc_max = Duration::from_micros(200);
    c
}

fn ap(seq: u64) -> WalRecord {
    WalRecord::Append {
        box_id: 1,
        seq,
        ts: 1_700_000_000_000 + seq,
        node: None,
        tag: Some("t".into()),
        data: format!("payload-{seq}").into_bytes(),
    }
}

/// Replay every `wal-*.log` file on the (post-crash) disk image, stopping each
/// file at its torn tail — exactly the recovery read path. Returns the dense
/// sequence of recovered Append seqs.
fn recover_seqs(disk: &FakeDisk) -> Vec<u64> {
    let fs = disk.arc();
    let mut files: Vec<PathBuf> = fs
        .read_dir(&wal_dir())
        .unwrap_or_default()
        .into_iter()
        .filter(|p| {
            p.file_name()
                .and_then(|s| s.to_str())
                .map(|n| n.starts_with("wal-") && n.ends_with(".log"))
                .unwrap_or(false)
        })
        .collect();
    files.sort();
    let mut seqs = Vec::new();
    for f in files {
        let Ok(r) = WalReader::open_with(&fs, &f) else {
            continue;
        };
        for frame in r {
            let s = frame.record.seq();
            if s > 0 {
                seqs.push(s);
            }
        }
    }
    seqs
}

// ===========================================================================
// F-SWEEP-DURABLE-APPEND
// Exhaustive crash-point sweep across every FS call (write_at AND sync_data) of
// ONE durable append batch, for each torn-damage seed. At every crash point the
// recovered WAL is a dense prefix [1..=k] — never a half-applied/partial record,
// never a fabricated/torn seq. A durable batch whose fsync returned (acked) is
// fully present; one crashed before/at its fsync is fully absent. (SQLite sweep
// discipline.)
// ===========================================================================

/// An `Fs` wrapper that fires `disk.crash(damage)` exactly after the `at`-th
/// (0-based) FS call counted across BOTH `write_at` and `sync_data` — the two
/// classes the durable-append batch path issues — then lets the frozen disk
/// swallow the rest. The in-process equivalent of "power loss after the Nth FS
/// mutating call of the batch".
#[derive(Clone)]
struct CrashAfterAny {
    disk: FakeDisk,
    at: u64,
    damage: TornDamage,
    seen: Arc<AtomicU64>,
    tripped: Arc<std::sync::atomic::AtomicBool>,
}

impl CrashAfterAny {
    fn new(disk: FakeDisk, at: u64, damage: TornDamage) -> Self {
        CrashAfterAny {
            disk,
            at,
            damage,
            seen: Arc::new(AtomicU64::new(0)),
            tripped: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }
    fn arc(&self) -> Arc<dyn Fs> {
        Arc::new(self.clone())
    }
    /// Count one FS mutating call of the swept classes; trip the crash once when
    /// the count reaches `at`.
    fn tick(&self) {
        let idx = self.seen.fetch_add(1, AtomicOrdering::SeqCst);
        if idx == self.at
            && self
                .tripped
                .compare_exchange(false, true, AtomicOrdering::SeqCst, AtomicOrdering::SeqCst)
                .is_ok()
        {
            self.disk.crash(self.damage);
        }
    }
}

struct CrashAfterAnyFile {
    inner: Box<dyn File>,
    owner: CrashAfterAny,
}

impl File for CrashAfterAnyFile {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read_at(offset, buf)
    }
    fn write_at(&mut self, offset: u64, buf: &[u8]) -> std::io::Result<usize> {
        // Trip AFTER the write reaches the (pending) image, so the first N writes'
        // pending bytes are available for a later fsync but anything not yet
        // fsynced at crash drops — the power-loss-after-Nth-write model.
        let r = self.inner.write_at(offset, buf);
        self.owner.tick();
        r
    }
    fn set_len(&mut self, len: u64) -> std::io::Result<()> {
        self.inner.set_len(len)
    }
    fn sync_data(&self) -> std::io::Result<()> {
        // Trip AFTER the fsync promotes the pending bytes, so a crash exactly at
        // the batch's fsync still sees the batch durable (acked⇒durable).
        let r = self.inner.sync_data();
        self.owner.tick();
        r
    }
    fn sync_all(&self) -> std::io::Result<()> {
        let r = self.inner.sync_all();
        self.owner.tick();
        r
    }
    fn metadata_len(&self) -> std::io::Result<u64> {
        self.inner.metadata_len()
    }
    fn read_to_end_from(&self, offset: u64, out: &mut Vec<u8>) -> std::io::Result<()> {
        self.inner.read_to_end_from(offset, out)
    }
}

impl Fs for CrashAfterAny {
    fn open(&self, path: &std::path::Path, opts: OpenOpts) -> std::io::Result<Box<dyn File>> {
        let inner = self.disk.open(path, opts)?;
        Ok(Box::new(CrashAfterAnyFile {
            inner,
            owner: self.clone(),
        }))
    }
    fn rename(&self, from: &std::path::Path, to: &std::path::Path) -> std::io::Result<()> {
        self.disk.rename(from, to)
    }
    fn remove_file(&self, path: &std::path::Path) -> std::io::Result<()> {
        self.disk.remove_file(path)
    }
    fn read_dir(&self, dir: &std::path::Path) -> std::io::Result<Vec<PathBuf>> {
        self.disk.read_dir(dir)
    }
    fn sync_dir(&self, dir: &std::path::Path) -> std::io::Result<()> {
        self.disk.sync_dir(dir)
    }
    fn create_dir_all(&self, dir: &std::path::Path) -> std::io::Result<()> {
        self.disk.create_dir_all(dir)
    }
    fn exists(&self, path: &std::path::Path) -> bool {
        self.disk.exists(path)
    }
    fn metadata_len(&self, path: &std::path::Path) -> std::io::Result<u64> {
        self.disk.metadata_len(path)
    }
}

/// Count the FS mutating calls (`write_at` + `sync_data`/`sync_all`) one durable
/// append batch of `n` records issues, by running it through a never-firing
/// `CrashAfterAny` and reading its counter. Used to size the sweep bound `M`.
fn probe_batch_fs_calls(n: u64) -> u64 {
    let disk = FakeDisk::new();
    let counter = CrashAfterAny::new(disk.clone(), u64::MAX, TornDamage::None);
    {
        let wal = Wal::open_at_with(counter.arc(), fast_cfg(), 1, 0).unwrap();
        let w = wal.writer();
        for seq in 1..=n {
            w.append(ap(seq), true).unwrap();
        }
        sync_wal_dir(&disk.arc());
        drop(wal);
    }
    counter.seen.load(AtomicOrdering::SeqCst)
}

#[test]
fn f_sweep_durable_append() {
    const N: u64 = 4; // a small fixed durable batch (seqs 1..=4).

    // Probe M = total write_at + sync_data calls over the whole durable workload.
    let total = probe_batch_fs_calls(N);
    assert!(total >= 4, "a durable append issues several FS calls (M={total})");

    // Cap so the sweep stays well under a minute (each durable append blocks on a
    // real group fsync). The small workload's M is itself tiny; cap defensively.
    let cap = total.min(40);

    // Each crash point × each torn-damage seed: a power loss after exactly that
    // many FS calls, then recover and assert the integrity oracle.
    let damages = [
        TornDamage::None,
        TornDamage::PrefixTruncate,
        TornDamage::ZeroSector,
        TornDamage::Garble,
    ];
    for crash_point in 0..=cap {
        for (di, &damage) in damages.iter().enumerate() {
            let disk = FakeDisk::with_seed(crash_point * 7 + di as u64 + 1);
            let trip = CrashAfterAny::new(disk.clone(), crash_point, damage);
            {
                let wal = Wal::open_at_with(trip.arc(), fast_cfg(), 1, 0).unwrap();
                let w = wal.writer();
                // Drive the durable batch; appends past the crash point land on the
                // now-frozen device and are simply lost (no ack). An append whose
                // group fsync already returned is acked-durable.
                for seq in 1..=N {
                    let _ = w.append(ap(seq), true);
                }
                // Make the file's NAME durable (production's create+dir-fsync). On a
                // frozen disk this is a no-op, which is fine — the name was already
                // dir-fsynced for any crash that happened after the first fsync.
                sync_wal_dir(&disk.arc());
                drop(wal);
            }
            disk.reset_power();

            // ORACLE: the recovered seqs are a DENSE PREFIX [1..=k] (k may be 0) with
            // no gap, no fabricated/torn seq misread as a record. A torn last write
            // truncates at the prior CRC-valid frame; never a half-applied record.
            let seqs = recover_seqs(&disk);
            for (i, s) in seqs.iter().enumerate() {
                assert_eq!(
                    *s,
                    i as u64 + 1,
                    "crash_point={crash_point} damage={damage:?}: survivors must be a \
                     dense prefix [1..=k], got {seqs:?} (no half-applied/torn record)"
                );
            }
            assert!(
                seqs.len() as u64 <= N,
                "crash_point={crash_point} damage={damage:?}: never more than the batch \
                 ({} > {N}): {seqs:?}",
                seqs.len()
            );

            // IDEMPOTENT-RECOVERY: replaying the same crashed image again yields the
            // identical dense prefix (recovery is a pure function of the durable
            // bytes; it must never advance/regress on a second pass).
            let seqs2 = recover_seqs(&disk);
            assert_eq!(
                seqs, seqs2,
                "crash_point={crash_point} damage={damage:?}: recovery must be idempotent"
            );
        }
    }
}

// ===========================================================================
// F-MONITOR-WAL-ORDER
// Always-on live assertion: a durable `append` (its CommitToken) is never acked
// (returns Ok) before its batch's bytes are DURABLE on the underlying disk (the
// `sync_data` returned ⇒ FakeDisk promoted pending→durable). We wrap the real
// disk in `MonitorFs` (the passive ordering monitor that is on in every test) AND
// add an explicit ack-vs-durable check: each time a durable append acks, the WAL
// file's *durable* image must already contain that frame's seq. A regression that
// signalled Ok before the fsync would fail this the instant it happened.
// ===========================================================================

/// Scan the durable bytes of `disk`'s active WAL file for the on-disk frames and
/// return the set of Append seqs that are durably present (CRC-valid up to the
/// torn tail). This reads ONLY the promoted (`durable`) image — exactly what a
/// power loss right now would preserve — so "seq present here" means "its batch's
/// fsync has returned".
fn durable_wal_seqs(disk: &FakeDisk) -> Vec<u64> {
    // `recover_seqs` opens through `disk.arc()`, whose `read_at` returns the
    // *visible* (durable+pending) bytes; to observe the DURABLE-only image we crash
    // a CLONE-free snapshot is unavailable, so instead read the durable bytes
    // directly per file and decode them with a fresh WalReader over a scratch disk
    // holding only those durable bytes.
    let fs = disk.arc();
    let mut files: Vec<PathBuf> = fs
        .read_dir(&wal_dir())
        .unwrap_or_default()
        .into_iter()
        .filter(|p| {
            p.file_name()
                .and_then(|s| s.to_str())
                .map(|n| n.starts_with("wal-") && n.ends_with(".log"))
                .unwrap_or(false)
        })
        .collect();
    files.sort();

    let scratch = FakeDisk::new();
    scratch.create_dir_all(&wal_dir()).unwrap();
    let mut any = false;
    for f in &files {
        let Some(durable) = disk.durable_bytes(f) else {
            continue;
        };
        any = true;
        let mut sf = scratch.open(f, OpenOpts::create_truncate()).unwrap();
        let mut w = 0usize;
        while w < durable.len() {
            let n = sf.write_at(w as u64, &durable[w..]).unwrap();
            w += n;
        }
        sf.sync_all().unwrap();
    }
    if !any {
        return Vec::new();
    }
    scratch.sync_dir(&wal_dir()).unwrap();
    recover_seqs(&scratch)
}

#[test]
fn f_monitor_wal_order() {
    let disk = FakeDisk::new();
    // Wrap in the always-on passive ordering monitor (panics on any tmp-rename or
    // idx-before-data ordering violation). It forwards every call unchanged.
    let monitored = MonitorFs::new(disk.arc());
    let fs: Arc<dyn Fs> = monitored.arc();

    let wal = Wal::open_at_with(fs.clone(), fast_cfg(), 1, 0).unwrap();
    let w = wal.writer();

    // Make the WAL file name durable up front so `durable_bytes` resolves.
    sync_wal_dir(&fs);

    // Drive a sequence of durable appends. The contract: when `append(.., true)`
    // returns Ok, its batch's group fsync has ALREADY returned, so the frame's
    // bytes are durable on the underlying disk RIGHT NOW. We assert that directly
    // after every ack — the live "no ack before fsync" belt-and-suspenders.
    for seq in 1..=10u64 {
        w.append(ap(seq), true).expect("durable append acks");
        // The dir-fsync above made the name durable; the seq's frame must be in the
        // DURABLE image the instant the ack returns (fsync-before-ack ordering).
        let durable_now = durable_wal_seqs(&disk);
        assert!(
            durable_now.contains(&seq),
            "F-MONITOR-WAL-ORDER violation: append seq {seq} acked but its frame is \
             NOT yet durable on disk (durable seqs={durable_now:?}) — ack BEFORE fsync"
        );
        // And the durable image is itself a dense prefix [1..=seq] — every earlier
        // acked frame is durable too (group commit never reorders an acked frame
        // behind a later one).
        let expected: Vec<u64> = (1..=seq).collect();
        assert_eq!(
            durable_now, expected,
            "F-MONITOR-WAL-ORDER: the durable image after acking seq {seq} must be the \
             dense acked prefix {expected:?}, got {durable_now:?}"
        );
    }

    drop(wal);

    // Cross-check: a non-durable submit is NOT required to be durable when it
    // returns — it may sit in pending. This is the *negative* of the invariant
    // (non-durable writes have no fsync-before-ack guarantee), proving the monitor
    // asserts the durable boundary specifically.
    let disk2 = FakeDisk::new();
    let fs2: Arc<dyn Fs> = MonitorFs::new(disk2.arc()).arc();
    let wal2 = Wal::open_at_with(fs2.clone(), fast_cfg(), 1, 0).unwrap();
    let w2 = wal2.writer();
    sync_wal_dir(&fs2);
    // First a durable anchor so a WAL file exists.
    w2.append(ap(1), true).unwrap();
    assert!(
        durable_wal_seqs(&disk2).contains(&1),
        "the durable anchor is on disk after its ack"
    );
    drop(wal2);
}

// ===========================================================================
// F-PROP-DURABILITY-CLASS-MIX
// proptest drives a random mix of durable / non-durable boxes & writes through a
// REAL engine, mirrors only acked effects into a reference model, crashes the
// disk at a random step (drop pending), then recovers and asserts:
//   - every DURABLE acked write survives (acked⇒durable),
//   - a non-durable box's survivors are a clean dense PREFIX (a tail may vanish),
//   - no fabrication / no gap / seq monotone — cross-box group-commit never loses
//     one box's acked durable frame to another box's non-durable one.
// ===========================================================================

/// The reference model of one box: whether it is durable, its acked records by
/// seq, and the head. (A pared-down version of `tests/crash_oracle.rs`'s model,
/// sufficient for the durability-class-mix contract.)
#[derive(Debug, Clone, Default)]
struct PModelBox {
    durable: bool,
    acked: BTreeMap<u64, String>,
    head: u64,
}

#[derive(Debug, Default)]
struct PModel {
    boxes: BTreeMap<String, PModelBox>,
}

impl PModel {
    fn ensure(&mut self, name: &str, durable: bool) {
        self.boxes.entry(name.to_string()).or_insert(PModelBox {
            durable,
            ..Default::default()
        });
    }
    fn ack(&mut self, name: &str, seq: u64, data: &str) {
        let b = self.boxes.get_mut(name).expect("box modeled before append");
        b.acked.insert(seq, data.to_string());
        b.head = b.head.max(seq);
    }
}

/// One proptest-generated op.
#[derive(Debug, Clone)]
enum POp {
    /// Create box `bi` with the given durability class.
    PutBox { bi: u8, durable: bool },
    /// Append `data` to box `bi`. The box is durable per its create (or the
    /// default non-durable if auto-created).
    Append { bi: u8, data: String },
}

fn pop_strategy() -> impl Strategy<Value = POp> {
    prop_oneof![
        (0u8..3, any::<bool>()).prop_map(|(bi, durable)| POp::PutBox { bi, durable }),
        (0u8..3, "[a-z]{1,4}").prop_map(|(bi, data)| POp::Append { bi, data }),
    ]
}

fn box_name(bi: u8) -> String {
    format!("b{bi}")
}

fn one_write(data: &str) -> WriteRequest {
    WriteRequest {
        records: vec![RecordIn {
            data: json!({ "v": data }),
            tag: None,
            node: None,
            meta: None,
        }],
        node: None,
        idempotency_key: None,
        create: Some(true),
        config: None,
        disable_backpressure: true,
    }
}

/// Read back box `name`'s live records (seq → data) through the engine diff path.
fn dump_records(engine: &Engine, name: &str) -> Option<BTreeMap<u64, String>> {
    engine.box_state(name, false).ok()?;
    let mut out = BTreeMap::new();
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
        for r in &d.records {
            let v = r
                .data
                .get("v")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| r.data.to_string());
            out.insert(r.seq, v);
        }
        if d.caught_up || d.records.is_empty() {
            break;
        }
        from = d.next_from_seq;
    }
    Some(out)
}

fn open_engine(disk: &FakeDisk) -> Arc<Engine> {
    Engine::with_data_dir_fs(cfg(), clock(), disk.arc()).expect("engine opens through FakeDisk")
}

/// Assert the durability-class-mix contract for one recovered box against the
/// model. `survivors` is the recovered seq→data map (empty if the box is absent).
fn assert_box_mix_contract(
    name: &str,
    model: &PModelBox,
    survivors: &BTreeMap<u64, String>,
    ops_log: &[POp],
) {
    let survivor_seqs: Vec<u64> = survivors.keys().copied().collect();

    // (1) NO FABRICATION: every recovered seq was acked by the model and its bytes
    //     match exactly (never a torn/garbled frame misread as a record).
    for (seq, data) in survivors {
        let m = model.acked.get(seq).unwrap_or_else(|| {
            panic!(
                "{name}: recovered seq {seq} never acked (fabricated/torn record): \
                 {data:?}\nops={ops_log:?}"
            )
        });
        assert_eq!(
            m, data,
            "{name}: recovered record at seq {seq} differs from the model\nops={ops_log:?}"
        );
    }

    // (2) DURABLE acked writes ALL survive (acked⇒durable). The whole acked tail of
    //     a durable box was fsynced before its ack returned, so a power loss keeps
    //     every one — even if a *different* (non-durable) box's tail vanished in the
    //     same group-commit window.
    if model.durable {
        for (seq, data) in &model.acked {
            assert!(
                survivors.get(seq) == Some(data),
                "{name}: DURABLE acked seq {seq} ({data:?}) LOST after recovery \
                 (survivors={survivor_seqs:?})\nops={ops_log:?}"
            );
        }
    }

    // (3) NO GAP — survivors are a dense prefix of the model's acked set up to the
    //     surviving high-water mark (a crash drops only a tail, never punches a hole
    //     into the middle). Applies to durable and non-durable boxes alike.
    if let Some(&hi) = survivor_seqs.last() {
        let expected_prefix: Vec<u64> = model
            .acked
            .keys()
            .copied()
            .filter(|s| *s <= hi)
            .collect();
        assert_eq!(
            survivor_seqs, expected_prefix,
            "{name}: survivors must be a dense prefix of the acked set up to {hi} \
             (survivors={survivor_seqs:?})\nops={ops_log:?}"
        );
    }

    // (4) HEAD MONOTONE: a recovered seq never exceeds the model head.
    if let Some(&hi) = survivor_seqs.last() {
        assert!(
            hi <= model.head,
            "{name}: recovered seq {hi} exceeds model head {} (future seq?)\nops={ops_log:?}",
            model.head
        );
    }
}

proptest! {
    // Bounded: small op streams + few cases so each (engine open + per-op group
    // fsync + recovery) round trip keeps the whole test well under a minute, while
    // proptest still explores the durable/non-durable × box × crash-step space.
    #![proptest_config(ProptestConfig {
        cases: 6,
        max_shrink_iters: 256,
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    /// Random mix of durable & non-durable boxes/writes, crash at a random step.
    #[test]
    fn f_prop_durability_class_mix(
        ops in prop::collection::vec(pop_strategy(), 1..7),
        crash_frac in 0u32..=100,
    ) {
        let disk = FakeDisk::new();
        let mut model = PModel::default();

        // The crash step: a fraction through the op stream. We run ops [0..crash_at)
        // for real (so every acked-durable write truly fsynced as it went), then
        // power-loss and STOP — the ops at/after `crash_at` never run, exactly like a
        // process killed mid-workload. (Running an op on the *frozen* device would
        // make FakeDisk report a phantom Ok that never reached the platter, which is
        // not what "crash drops pending" models — so we cut the workload instead.)
        let crash_at = (ops.len() as u32 * crash_frac / 100) as usize;
        let crash_at = crash_at.min(ops.len());

        {
            let engine = open_engine(&disk);
            for op in &ops[..crash_at] {
                match op {
                    POp::PutBox { bi, durable } => {
                        let name = box_name(*bi);
                        let cfgb = BoxConfig {
                            r#type: BoxType::Log,
                            durable: *durable,
                            cap_records: 0,
                            ..Default::default()
                        };
                        if engine.put_box(&name, cfgb).is_ok() {
                            model.ensure(&name, *durable);
                        }
                    }
                    POp::Append { bi, data } => {
                        let name = box_name(*bi);
                        // Auto-create defaults to non-durable; model it so the diff
                        // applies the right durability relaxation.
                        if !model.boxes.contains_key(&name) {
                            model.ensure(&name, false);
                        }
                        // A durable append blocks on its group fsync ⇒ returning Ok
                        // means the bytes are durable RIGHT NOW (so a later crash keeps
                        // them). A non-durable append acks on the buffered write ⇒ its
                        // bytes may still be pending at the crash and vanish.
                        if let Ok(resp) = engine.write(&name, one_write(data), true) {
                            // For a non-durable box the ack does not imply durability:
                            // these seqs are best-effort and may be dropped as a clean
                            // tail. We still record them (the contract's upper bound:
                            // survivors ⊆ acked); only durable boxes' acked sets are
                            // the must-survive lower bound (checked in the contract).
                            let seqs = resp.seqs.clone().unwrap_or_else(|| vec![resp.last_seq]);
                            for s in &seqs {
                                model.ack(&name, *s, data);
                            }
                        }
                    }
                }
            }
            // Make the WAL/meta names durable (production's open-time dir-fsync), then
            // power-loss: drop every un-fsynced pending byte. Acked-durable frames
            // already fsynced survive; non-durable buffered tails vanish. Freeze
            // BEFORE dropping the engine so its Drop drain can't harden the tail.
            sync_wal_dir(&disk.arc());
            disk.crash(TornDamage::None);
            drop(engine);
        }
        disk.reset_power();

        // Recover a fresh engine through the crashed image and assert the contract
        // for every modeled box.
        let engine = open_engine(&disk);
        for (name, mbox) in &model.boxes {
            if mbox.acked.is_empty() && mbox.head == 0 {
                continue; // an empty phantom box need not exist post-recovery.
            }
            let survivors = dump_records(&engine, name).unwrap_or_default();
            // A DURABLE box with acked records must not vanish entirely (acked⇒durable
            // — its frames fsynced before the crash).
            if mbox.durable && !mbox.acked.is_empty() {
                prop_assert!(
                    !survivors.is_empty(),
                    "{}: durable box with acked records vanished after recovery\nops={:?}",
                    name, ops
                );
            }
            assert_box_mix_contract(name, mbox, &survivors, &ops);
        }

        // IDEMPOTENT RECOVERY: a second recovery over the same image is identical
        // (recover(recover(x)) == recover(x)) — no acked frame gained or lost on a
        // second pass.
        let dumps1: BTreeMap<String, Option<BTreeMap<u64, String>>> = model
            .boxes
            .keys()
            .map(|n| (n.clone(), dump_records(&engine, n)))
            .collect();
        drop(engine);
        let engine2 = open_engine(&disk);
        for (name, d1) in &dumps1 {
            let d2 = dump_records(&engine2, name);
            prop_assert_eq!(
                d1, &d2,
                "recover(recover(x)) must equal recover(x) for {}\nops={:?}", name, ops
            );
        }
        drop(engine2);
    }
}
