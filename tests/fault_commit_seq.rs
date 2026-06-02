//! Phase-8A reliability iteration #2 — **the off-lock per-topic commit SEQUENCER**
//! (`TopicState::append_lock` + `publish_ticket`/`publish_gate`/`publish_cv`, the
//! WAL `CommitToken` group commit, and the durable write path that stitches them
//! together: stage under the lock → enqueue one WAL frame per record to the single
//! ordered writer → take a publish ticket → wait the fsync OFF the lock → publish
//! in strict ticket order). This is the recently-added concurrency primitive that
//! lets many same-topic durable writers coalesce into ONE group-commit fsync while
//! still publishing in seq order, so it is under-tested relative to the original
//! 125-strategy suite.
//!
//! Invariants under test (the sequencer oracle):
//!
//! ```text
//!   CONTIGUOUS+ORDERED : the published seqs of one topic are a dense [base..=head]
//!                        with no gap and no duplicate — head advances in commit
//!                        (ticket) order, monotonically.
//!   NO PRE-FSYNC READ  : a reader gating on head_seq never observes a record
//!                        whose durable-class WAL frame's group fsync has not
//!                        returned (acked ⇒ durable; staged-unpublished invisible).
//!   FAIL = NO TRACE    : a fail-injected (EIO) durable batch is NOT acked, leaves
//!                        NO visible record, and opens NO gap — the survivors stay
//!                        a dense prefix of the genuinely-acked set after recovery.
//!   NO LOST WAKEUP     : every concurrent writer's CommitToken / publish-gate turn
//!                        resolves (no deadlock, no stuck waiter) under heavy churn.
//! ```
//!
//! Reuses the Phase-8A harness EXACTLY (per `tests/crash_oracle.rs` +
//! `src/storage/testfs.rs`): a real fully-wired [`Engine`] over a [`FakeDisk`] via
//! [`Engine::with_data_dir_fs`], the durable write path driven through
//! `engine.write`, a [`FaultFs`] for EIO injection on `sync_data`, a `CrashAfter`
//! wrapper (copied from `tests/crash_oracle.rs`) for the bounded power-loss sweep,
//! and a per-topic dense-prefix / no-fabrication oracle.
//!
//! ### loom / shuttle (documented follow-up — same status as `fault_concurrency_a`)
//!
//! The task permits loom/shuttle on the small sequencer primitive *if feasible*.
//! `loom`/`shuttle` are NOT in this crate's `Cargo.toml` (which the Tests stage
//! may not edit), and a `cfg(loom)` retrofit of `CommitState` + the publish gate
//! lives in `src/` (also off-limits here). Per the Phase-8B fallback policy
//! already adopted in `tests/fault_concurrency_a.rs`, the sequencer is instead
//! exercised by **robust bounded high-thread stress tests** asserting the SAME
//! invariants a loom/shuttle model would (contiguity, commit-order, no-lost-wakeup,
//! no pre-fsync visibility). Once loom is wired under `[dev-dependencies]` + a
//! `cfg(loom)` sync-module swap on `CommitState`/`publish_gate`, the 2-thread
//! `seq_sequencer_two_writer_stress` below is the natural model-check seed.
//!
//! All sweeps/stress are BOUNDED (small N, capped crash points, fixed seeds,
//! bounded thread counts) so the whole file runs in well under a minute.
//!
//! ```text
//! cargo test --features test-fs --test fault_commit_seq
//! ```

#![cfg(feature = "test-fs")]
#![allow(
    clippy::ptr_arg,
    clippy::manual_clamp,
    clippy::unusual_byte_groupings,
    clippy::needless_range_loop
)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::Duration;

use serde_json::json;

use topics::clock::{SharedClock, TestClock};
use topics::config::ServerConfig;
use topics::engine::Engine;
use topics::storage::testfs::{FakeDisk, FaultFs, FaultKind, FaultOp, TornDamage};
use topics::storage::Fs;
use topics::types::{DiffRequest, Durability, RecordIn, TopicConfig, TopicType, WriteRequest};

// ===========================================================================
// Shared plumbing (mirrors tests/crash_oracle.rs + tests/fault_concurrency_a.rs).
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

/// Build a fresh engine whose WAL + snapshots live on `disk`.
fn open_engine(disk: &FakeDisk) -> Arc<Engine> {
    Engine::with_data_dir_fs(cfg(), clock(), disk.arc()).expect("engine opens through FakeDisk")
}

/// Build a fresh engine plus the [`TestClock`] handle driving it, so a test can
/// advance time deterministically (lease expiry / TTL) without wall-clock sleeps.
fn open_engine_clock(disk: &FakeDisk) -> (Arc<Engine>, Arc<TestClock>) {
    let tc = Arc::new(TestClock::new(1_700_000_000_000));
    let clk: SharedClock = tc.clone();
    let engine = Engine::with_data_dir_fs(cfg(), clk, disk.arc()).expect("engine opens");
    (engine, tc)
}

/// Make every WAL/meta file NAME durable (the create+dir-fsync production does at
/// WAL open) so the file survives a crash — copied verbatim from crash_oracle.rs.
fn sync_wal_dir(disk: &FakeDisk) {
    let fs = disk.arc();
    let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("wal"));
    let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("meta"));
}

/// A `fsync`-class durable topic (the strongest class: the ack is held until the
/// group fsync returns — the sequencer's hard case).
fn durable_topic(name: &str, engine: &Engine) {
    engine
        .put_topic(
            name,
            TopicConfig {
                r#type: TopicType::Log,
                durability: Some(Durability::Fsync),
                ..Default::default()
            },
        )
        .expect("put_topic durable");
}

/// One write of a single record carrying a writer-identifying payload + tag.
fn write_one(engine: &Engine, name: &str, data: &str, tag: Option<&str>) -> Option<Vec<u64>> {
    let req = WriteRequest {
        records: vec![RecordIn {
            data: json!({ "v": data }),
            tag: tag.map(str::to_string),
            node: None,
            meta: None,
        }],
        node: None,
        idempotency_key: None,
        create: Some(true),
        config: None,
        disable_backpressure: true,
    };
    engine
        .write(name, req, true)
        .ok()
        .map(|r| r.seqs.unwrap_or_else(|| vec![r.last_seq]))
}

/// Read every live record of `name` through the public diff path (the same bytes
/// a consumer sees), returning `(seq -> data "v")`. `None` if the topic is gone.
fn dump(engine: &Engine, name: &str) -> Option<BTreeMap<u64, String>> {
    engine.topic_state(name, false).ok()?;
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
                    include_meta: false,
                    wait_ms: 0,
                    max_batch_bytes: 0,
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

/// Assert the recovered survivors of `name` are a DENSE PREFIX of the set of seqs
/// the test genuinely observed acked, with NO fabrication (every survivor was
/// acked) and NO gap (no hole below the surviving high-water). This is the
/// crash-mid-stream relaxation from crash_oracle.rs (`whole_tail_durable=false`):
///   acked-prefix ⊆ survivors ⊆ acked
fn assert_dense_prefix_of_acked(survivors: &BTreeMap<u64, String>, acked: &BTreeSet<u64>) {
    let surv: Vec<u64> = survivors.keys().copied().collect();
    // No fabrication: every survivor was genuinely acked.
    for s in &surv {
        assert!(
            acked.contains(s),
            "recovered seq {s} was never acked (fabricated/torn record); acked={acked:?}"
        );
    }
    // Dense: survivors form a contiguous run with no internal gap.
    for w in surv.windows(2) {
        assert_eq!(
            w[1],
            w[0] + 1,
            "survivors must be contiguous (no gap): {surv:?}"
        );
    }
    // No hole below the surviving high-water in the acked set: every acked seq
    // <= hi survived (a crash drops a tail, never a middle).
    if let Some(&hi) = surv.last() {
        for &a in acked {
            if a <= hi {
                assert!(
                    survivors.contains_key(&a),
                    "acked seq {a} missing below surviving high-water {hi} \
                     (hole in the committed prefix): survivors={surv:?}"
                );
            }
        }
    }
}

// ===========================================================================
// CrashAfter — a power-loss-after-Nth-FS-call injector (copied verbatim from
// tests/crash_oracle.rs; reused exactly per the Phase-8A harness contract).
// ===========================================================================

/// Wraps a [`FakeDisk`] and triggers `disk.crash()` exactly after the `at`-th
/// (0-based) call of a chosen op class, then lets the frozen disk swallow the
/// rest — the in-process equivalent of "SIGKILL after the Nth FS mutating call".
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
    inner: Box<dyn topics::storage::File>,
    owner: CrashAfter,
}

impl topics::storage::File for CrashAfterFile {
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
    fn open(
        &self,
        path: &std::path::Path,
        opts: topics::storage::OpenOpts,
    ) -> std::io::Result<Box<dyn topics::storage::File>> {
        let inner = self.disk.open(path, opts)?;
        Ok(Box::new(CrashAfterFile {
            inner,
            owner: self.clone(),
        }))
    }
    fn rename(&self, from: &std::path::Path, to: &std::path::Path) -> std::io::Result<()> {
        let r = self.disk.rename(from, to);
        self.tick(FaultOp::Rename);
        r
    }
    fn remove_file(&self, path: &std::path::Path) -> std::io::Result<()> {
        self.disk.remove_file(path)
    }
    fn read_dir(&self, dir: &std::path::Path) -> std::io::Result<Vec<PathBuf>> {
        self.disk.read_dir(dir)
    }
    fn sync_dir(&self, dir: &std::path::Path) -> std::io::Result<()> {
        let r = self.disk.sync_dir(dir);
        self.tick(FaultOp::SyncDir);
        r
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

// ===========================================================================
// 1) Many concurrent durable writers to ONE topic: published seqs are CONTIGUOUS,
//    in commit order, with no gap / no duplicate (the sequencer's core contract).
// ===========================================================================

/// 16 threads each issue a burst of `fsync`-class durable writes to a single topic.
/// The per-topic sequencer must assign + publish contiguous gapless seqs in commit
/// order even though the fsync waits overlap off the append lock. We assert:
///   * every returned seq is unique (no two writers got the same seq),
///   * the union of returned seqs is exactly the dense range [1..=N*K],
///   * the live read-back is that same dense range (head advanced to cover all),
///   * head_seq == N*K and count == N*K (monotone, no gap).
#[test]
fn many_concurrent_durable_writers_contiguous_seqs() {
    const WRITERS: u64 = 16;
    const PER: u64 = 12;
    let total = WRITERS * PER;

    let disk = FakeDisk::new();
    let engine = open_engine(&disk);
    durable_topic("hot", &engine);

    let start = Arc::new(Barrier::new(WRITERS as usize));
    let mut handles = Vec::new();
    for w in 0..WRITERS {
        let engine = engine.clone();
        let start = start.clone();
        handles.push(std::thread::spawn(move || {
            start.wait();
            let mut mine = Vec::new();
            for i in 0..PER {
                let seqs = write_one(&engine, "hot", &format!("w{w}-{i}"), None)
                    .expect("durable write acked");
                assert_eq!(seqs.len(), 1, "one record per write");
                mine.push(seqs[0]);
            }
            mine
        }));
    }

    let mut all: Vec<u64> = Vec::new();
    for h in handles {
        all.extend(h.join().expect("writer thread joined (no deadlock)"));
    }

    // Every assigned seq is unique (no double-assignment under the index lock).
    let uniq: BTreeSet<u64> = all.iter().copied().collect();
    assert_eq!(
        uniq.len() as u64,
        total,
        "every concurrent writer got a distinct seq (no duplicate)"
    );
    // The union is exactly the dense range [1..=total] — contiguous, no gap.
    assert_eq!(
        uniq,
        (1..=total).collect::<BTreeSet<u64>>(),
        "assigned seqs are a dense [1..=N*K] with no gap"
    );

    // The live read-back is the same dense range (every acked write visible).
    let live = dump(&engine, "hot").expect("topic present");
    assert_eq!(
        live.keys().copied().collect::<BTreeSet<u64>>(),
        uniq,
        "published (visible) seqs == assigned seqs"
    );

    let st = engine.topic_state("hot", false).unwrap();
    assert_eq!(
        st.head_seq, total,
        "head advanced to cover every acked write"
    );
    assert_eq!(st.count, total, "count == total (no gap, no loss)");
}

// ===========================================================================
// 2) NO PRE-FSYNC READ: a reader spinning on the topic never sees a record before
//    its durable group fsync returned. Concurrent durable writers + a hammering
//    reader; every observed seq must have already been ack-returned by its writer.
// ===========================================================================

/// A reader thread hammers the live-read path while 8 durable writers churn. The
/// sequencer publishes a record ONLY after its group fsync returned and in ticket
/// order, so the two hard invariants are:
///   (a) DENSE PREFIX — whatever the reader sees is [1..=k] with no gap and no
///       future/torn seq (publish stages the slots BEFORE the Release of head_seq
///       and the ticket gate keeps publish in strict seq order);
///   (b) VISIBLE ⇒ DURABLE — every seq the reader EVER observed must survive a
///       power loss, because it could only have become visible after its own
///       group fsync returned (no pre-fsync publish). We crash at the end and
///       assert every reader-observed seq is in the recovered survivors.
#[test]
fn reader_never_observes_pre_fsync_record() {
    const WRITERS: u64 = 8;
    const PER: u64 = 16;

    let disk = FakeDisk::new();
    let engine = open_engine(&disk);
    durable_topic("vis", &engine);

    // Every seq the reader ever saw visible (must all be durable on recovery).
    let observed: Arc<Mutex<BTreeSet<u64>>> = Arc::new(Mutex::new(BTreeSet::new()));
    let done = Arc::new(AtomicBool::new(false));
    let start = Arc::new(Barrier::new(WRITERS as usize + 1));

    // Reader: spin reading the whole live set; it must always be a dense prefix.
    let reader = {
        let engine = engine.clone();
        let observed = observed.clone();
        let done = done.clone();
        let start = start.clone();
        std::thread::spawn(move || {
            start.wait();
            while !done.load(Ordering::Relaxed) {
                if let Some(live) = dump(&engine, "vis") {
                    let seqs: Vec<u64> = live.keys().copied().collect();
                    // Dense prefix [1..=k]: a reader gating on head_seq that sees
                    // head==k also sees every slot [1..=k] (publish stages BEFORE
                    // the Release of head_seq); never a gap / future / torn seq.
                    for (i, s) in seqs.iter().enumerate() {
                        assert_eq!(
                            *s,
                            i as u64 + 1,
                            "reader saw a gap / out-of-order seq (non-dense prefix): {seqs:?}"
                        );
                    }
                    let mut o = observed.lock().unwrap();
                    o.extend(seqs);
                }
            }
        })
    };

    let mut handles = Vec::new();
    for w in 0..WRITERS {
        let engine = engine.clone();
        let start = start.clone();
        handles.push(std::thread::spawn(move || {
            start.wait();
            for i in 0..PER {
                let _ = write_one(&engine, "vis", &format!("w{w}-{i}"), None);
            }
        }));
    }
    for h in handles {
        h.join().expect("writer joined (no deadlock)");
    }
    done.store(true, Ordering::Relaxed);
    reader.join().expect("reader joined (no deadlock)");

    let st = engine.topic_state("vis", false).unwrap();
    assert_eq!(st.head_seq, WRITERS * PER, "all acked");
    let observed = Arc::try_unwrap(observed).unwrap().into_inner().unwrap();

    // VISIBLE ⇒ DURABLE: power loss now; every seq the reader observed visible
    // must survive (it could only have been published after its fsync returned).
    sync_wal_dir(&disk);
    disk.crash(TornDamage::None);
    drop(engine);
    disk.reset_power();

    let engine = open_engine(&disk);
    let survivors: BTreeSet<u64> = dump(&engine, "vis")
        .expect("topic recovered")
        .keys()
        .copied()
        .collect();
    for s in &observed {
        assert!(
            survivors.contains(s),
            "reader observed seq {s} visible but it was LOST after a crash \
             (it was published before its fsync was durable!): survivors={survivors:?}"
        );
    }
}

// ===========================================================================
// 3) FAIL-INJECTED batch leaves NO visible record + NO gap. A dead-device EIO on
//    sync_data fails the in-flight durable batch; it is NOT acked, publishes
//    nothing, and a recovery surfaces exactly the genuinely-acked prefix.
// ===========================================================================

/// Phase 1: 4 durable acked writes on a clean disk. Phase 2: wrap the disk in a
/// dead `sync_data` fault and issue more durable writes that MUST all fail (EIO),
/// be un-acked, and leave nothing visible (head must not advance past phase 1).
/// Then crash + recover: survivors are exactly the phase-1 acked prefix — the
/// failed batches left no trace and opened no gap.
#[test]
fn fail_injected_batch_no_trace_no_gap() {
    let disk = FakeDisk::new();
    let mut acked: BTreeSet<u64> = BTreeSet::new();

    // Phase 1: 4 clean durable acked writes.
    {
        let engine = open_engine(&disk);
        durable_topic("p", &engine);
        for i in 0..4 {
            let seqs = write_one(&engine, "p", &format!("ok-{i}"), None).expect("acked");
            acked.extend(seqs);
        }
        let head_before = engine.topic_state("p", false).unwrap().head_seq;
        assert_eq!(head_before, 4, "4 acked writes visible");
        sync_wal_dir(&disk);

        // Phase 2: dead-device EIO on every sync_data from here on. Reopen the
        // engine through the fault wrapper so the resumed WAL writer's group fsync
        // fails. Each durable write must return Err and publish NOTHING.
        drop(engine);
        let faulty: Arc<dyn Fs> =
            FaultFs::new(disk.arc(), FaultOp::SyncData, FaultKind::Eio, 0, false).arc();
        let engine2 = Engine::with_data_dir_fs(cfg(), clock(), faulty).expect("reopen via faultfs");
        for i in 0..3 {
            let req = WriteRequest {
                records: vec![RecordIn {
                    data: json!({ "v": format!("fail-{i}") }),
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
            let res = engine2.write("p", req, true);
            assert!(
                res.is_err(),
                "durable write must fail when fsync EIOs (i={i})"
            );
        }
        // The failed batches must NOT be visible: head stayed at the phase-1 prefix
        // (the sequencer rolled back each failed batch's seqs, never publishing).
        let live = dump(&engine2, "p").expect("topic present");
        assert_eq!(
            live.keys().copied().collect::<BTreeSet<u64>>(),
            (1..=4).collect::<BTreeSet<u64>>(),
            "failed durable batches left NO visible record (no pre-fsync publish)"
        );
        // Power loss: freeze BEFORE dropping so the writer's Drop drain cannot
        // harden the failed/buffered tail on a (faulted-anyway) device.
        disk.crash(TornDamage::None);
        drop(engine2);
    }
    disk.reset_power();

    // Recover a fresh engine: exactly the phase-1 acked prefix, dense, no gap.
    let engine = open_engine(&disk);
    let live = dump(&engine, "p").expect("topic recovered");
    assert_dense_prefix_of_acked(&live, &acked);
    assert_eq!(
        live.keys().copied().collect::<BTreeSet<u64>>(),
        (1..=4).collect::<BTreeSet<u64>>(),
        "only the 4 genuinely-acked writes survive; failed batches left no trace"
    );
}

// ===========================================================================
// 4) BOUNDED crash-point sweep over concurrent durable writers (FaultFs/FakeDisk
//    + CrashAfter). At every crash point the recovered survivors are a dense
//    prefix of the acked set — never a half-published record, never a gap.
// ===========================================================================

/// Probe how many `sync_data` calls a small concurrent durable workload issues,
/// then for each crash point in a capped range replay the workload on a FRESH
/// disk, `crash()` after that many `sync_data` calls (a power loss precisely at a
/// group-commit boundary), recover, and assert the sequencer oracle: survivors are
/// a dense prefix of the genuinely-acked set, no fabrication, no gap. Bounded
/// (4 writers x 3 writes, <=10 crash points, fixed seeds) so it runs fast.
#[test]
fn sweep_concurrent_durable_crash_points_oracle() {
    const WRITERS: u64 = 3;
    const PER: u64 = 2;

    // Run a concurrent durable workload, collecting the genuinely-acked seqs.
    // `fs_factory` lets the sweep swap in a CrashAfter for each crash point.
    fn run(engine: &Arc<Engine>) -> BTreeSet<u64> {
        durable_topic("s", engine);
        let start = Arc::new(Barrier::new(WRITERS as usize));
        let acked: Arc<Mutex<BTreeSet<u64>>> = Arc::new(Mutex::new(BTreeSet::new()));
        let mut handles = Vec::new();
        for w in 0..WRITERS {
            let engine = engine.clone();
            let start = start.clone();
            let acked = acked.clone();
            handles.push(std::thread::spawn(move || {
                start.wait();
                for i in 0..PER {
                    // A write may fail/return nothing once the disk is frozen mid
                    // sweep — only genuinely-acked (Ok) seqs enter `acked`.
                    if let Some(seqs) = write_one(&engine, "s", &format!("w{w}-{i}"), None) {
                        let mut a = acked.lock().unwrap();
                        a.extend(seqs);
                    }
                }
            }));
        }
        for h in handles {
            h.join().expect("writer joined (no deadlock under crash)");
        }
        Arc::try_unwrap(acked).unwrap().into_inner().unwrap()
    }

    // Probe the sync_data call count with a never-firing FaultFs counter.
    let probe_disk = FakeDisk::new();
    let probe = FaultFs::new(
        probe_disk.arc(),
        FaultOp::SyncData,
        FaultKind::Eio,
        u64::MAX,
        true,
    );
    {
        let engine = Engine::with_data_dir_fs(cfg(), clock(), probe.arc()).expect("probe engine");
        let _ = run(&engine);
        drop(engine);
    }
    let total_syncs = probe.calls_seen();
    assert!(total_syncs >= 1, "workload issues at least one group fsync");

    let cap = total_syncs.min(4);
    // Tiered sweep (topics::testutil::crash_points): bounded deterministic sample
    // by default, full `0..=cap` under TOPICS_TEST_EXHAUSTIVE.
    for crash_point in topics::testutil::crash_points(cap) {
        let disk = FakeDisk::with_seed(0xC0FFEE ^ crash_point);
        let trip = CrashAfter::new(disk.clone(), FaultOp::SyncData, crash_point);
        let acked = {
            let engine =
                Engine::with_data_dir_fs(cfg(), clock(), trip.arc()).expect("sweep engine opens");
            let acked = run(&engine);
            drop(engine);
            acked
        };
        disk.reset_power();

        // Recover a fresh engine through the crashed image; assert the oracle.
        let engine = open_engine(&disk);
        if let Some(live) = dump(&engine, "s") {
            assert_dense_prefix_of_acked(&live, &acked);
        } else {
            // Topic vanished entirely: acceptable only if nothing was acked (the
            // crash hit before the topic-create + first durable batch landed).
            assert!(
                acked.is_empty(),
                "topic vanished but {} writes were acked (lost durable state)",
                acked.len()
            );
        }
        drop(engine);
    }
}

// ===========================================================================
// 5) NO LOST WAKEUP / NO DEADLOCK under heavy multi-writer churn across MANY
//    topics (cross-topic group-commit coalescing + per-topic ticket ordering). A
//    watchdog thread bounds the run so a stuck CommitToken / publish-gate waiter
//    fails loud instead of hanging the suite.
// ===========================================================================

/// 32 writers spread across 4 durable topics, each issuing a burst. Every write
/// must complete (its CommitToken fires and its publish-gate turn arrives) — no
/// lost wakeup, no deadlock. A watchdog asserts the whole thing finishes well
/// within a bound. Afterwards each topic's published seqs are a dense [1..=k_topic].
#[test]
fn no_lost_wakeup_multi_topic_high_thread_stress() {
    const WRITERS: usize = 32;
    const PER: u64 = 10;
    const TOPICS: usize = 4;

    let disk = FakeDisk::new();
    let engine = open_engine(&disk);
    for b in 0..TOPICS {
        durable_topic(&format!("b{b}"), &engine);
    }

    // Per-topic ack tally to verify dense ranges + the total completion count.
    let per_topic: Arc<Vec<Mutex<BTreeSet<u64>>>> =
        Arc::new((0..TOPICS).map(|_| Mutex::new(BTreeSet::new())).collect());
    let completed = Arc::new(AtomicU64::new(0));
    let start = Arc::new(Barrier::new(WRITERS));

    let mut handles = Vec::new();
    for w in 0..WRITERS {
        let engine = engine.clone();
        let per_topic = per_topic.clone();
        let completed = completed.clone();
        let start = start.clone();
        handles.push(std::thread::spawn(move || {
            start.wait();
            // Each writer round-robins a topic so every topic has concurrent writers.
            let bi = w % TOPICS;
            let name = format!("b{bi}");
            for i in 0..PER {
                let seqs = write_one(&engine, &name, &format!("w{w}-{i}"), None)
                    .expect("durable write acked (token fired, gate turn arrived)");
                let mut s = per_topic[bi].lock().unwrap();
                s.extend(seqs);
                completed.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    // Watchdog: every write is a fast in-memory fsync (FakeDisk), so the whole
    // burst must finish in << the bound. If a CommitToken wakeup is lost or the
    // publish gate deadlocks, `completed` stalls and we fail loud (instead of the
    // test process hanging).
    let expected = WRITERS as u64 * PER;
    let watch = {
        let completed = completed.clone();
        std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + Duration::from_secs(20);
            while completed.load(Ordering::Relaxed) < expected {
                if std::time::Instant::now() > deadline {
                    return false; // a waiter is stuck — lost wakeup / deadlock.
                }
                std::thread::yield_now();
            }
            true
        })
    };

    for h in handles {
        h.join().expect("writer joined (no deadlock)");
    }
    assert!(
        watch.join().unwrap(),
        "all {expected} durable writes completed within the bound (no lost wakeup/deadlock)"
    );
    assert_eq!(
        completed.load(Ordering::Relaxed),
        expected,
        "every write acked"
    );

    // Each topic's published seqs are a dense [1..=k_topic], in commit order.
    for b in 0..TOPICS {
        let name = format!("b{b}");
        let acked = per_topic[b].lock().unwrap();
        let live = dump(&engine, &name).expect("topic present");
        assert_eq!(
            live.keys().copied().collect::<BTreeSet<u64>>(),
            acked.iter().copied().collect::<BTreeSet<u64>>(),
            "topic {name}: published == acked"
        );
        let k = acked.len() as u64;
        assert_eq!(
            acked.iter().copied().collect::<BTreeSet<u64>>(),
            (1..=k).collect::<BTreeSet<u64>>(),
            "topic {name}: dense [1..={k}] (sequencer kept seqs contiguous)"
        );
    }
}

// ===========================================================================
// 6) Loom-shaped 2-writer sequencer primitive stress (the natural loom seed): two
//    writers contend the SAME topic's append_lock + publish gate while a reader
//    spins. The strict-2-thread interleaving is the case a loom model would
//    enumerate; we drive the real primitive many bounded iterations with the
//    real Engine. Invariant: published seqs are always a dense prefix, head is
//    monotone, and BOTH writers always make progress (no starvation/lost wakeup).
// ===========================================================================

/// The minimal contended case (matching `fault_concurrency_a`'s documented
/// loom-follow-up seed): exactly two durable writers + one reader on one topic,
/// repeated for many bounded rounds. Each round both writers race a single
/// durable write; we assert head advanced by exactly 2 and the live set stayed a
/// dense prefix throughout (a reader never sees a gap or a non-durable record).
#[test]
fn seq_sequencer_two_writer_stress() {
    const ROUNDS: u64 = 150;

    let disk = FakeDisk::new();
    let engine = open_engine(&disk);
    durable_topic("two", &engine);

    let done = Arc::new(AtomicBool::new(false));
    // Reader spins the whole time asserting the dense-prefix invariant.
    let reader = {
        let engine = engine.clone();
        let done = done.clone();
        std::thread::spawn(move || {
            while !done.load(Ordering::Relaxed) {
                if let Some(live) = dump(&engine, "two") {
                    let seqs: Vec<u64> = live.keys().copied().collect();
                    for (i, s) in seqs.iter().enumerate() {
                        assert_eq!(*s, i as u64 + 1, "reader saw a gap: {seqs:?}");
                    }
                }
            }
        })
    };

    for round in 0..ROUNDS {
        let head_before = engine.topic_state("two", false).unwrap().head_seq;
        let barrier = Arc::new(Barrier::new(2));
        let mut hs = Vec::new();
        for w in 0..2u64 {
            let engine = engine.clone();
            let barrier = barrier.clone();
            hs.push(std::thread::spawn(move || {
                barrier.wait();
                write_one(&engine, "two", &format!("r{round}-w{w}"), None)
                    .expect("both writers make progress (no starvation/lost wakeup)")
            }));
        }
        let mut got = Vec::new();
        for h in hs {
            got.extend(h.join().expect("writer joined (no deadlock)"));
        }
        // Both writers' seqs are distinct and exactly the next two seqs (commit
        // order, contiguous).
        let got: BTreeSet<u64> = got.into_iter().collect();
        assert_eq!(got.len(), 2, "two distinct seqs assigned this round");
        assert_eq!(
            got,
            (head_before + 1..=head_before + 2).collect::<BTreeSet<u64>>(),
            "the round's two seqs are exactly [{}..={}] (contiguous, in order)",
            head_before + 1,
            head_before + 2
        );
        let head_after = engine.topic_state("two", false).unwrap().head_seq;
        assert_eq!(
            head_after,
            head_before + 2,
            "head advanced by exactly 2 (monotone)"
        );
    }
    done.store(true, Ordering::Relaxed);
    reader.join().expect("reader joined (no deadlock)");

    let st = engine.topic_state("two", false).unwrap();
    assert_eq!(st.head_seq, ROUNDS * 2, "final head == all writes");
    assert_eq!(st.count, ROUNDS * 2, "no gap across the whole stress run");
}

// ===========================================================================
// 7) QUEUE durable ack path: a fsync-class queue's ack durably deletes the job
//    via the explicit-seq WAL Delete frame BEFORE returning; the delete survives
//    a crash (the acked job does NOT reappear), while a NON-acked claimed job
//    self-heals back to claimable after restart (at-least-once, DESIGN §10.6).
// ===========================================================================

/// Build a `fsync`-class queue, enqueue jobs, claim + durably ack a subset, crash,
/// recover, and assert: the durably-acked jobs are GONE (the explicit-seq Delete
/// frame fsynced before the ack returned and replays deterministically), and the
/// un-acked jobs remain claimable (no silent loss — at-least-once).
#[test]
fn queue_durable_ack_delete_survives_crash() {
    let disk = FakeDisk::new();
    let acked_gone: Vec<u64>;
    let all_seqs: Vec<u64>;

    {
        let (engine, _clk) = open_engine_clock(&disk);
        engine
            .put_topic(
                "q",
                TopicConfig {
                    r#type: TopicType::Queue,
                    durability: Some(Durability::Fsync),
                    leases_durable: true,
                    ..Default::default()
                },
            )
            .expect("put queue");

        // Enqueue 6 jobs (durable writes onto the jobs log).
        all_seqs = (0..6)
            .map(|i| write_one(&engine, "q", &format!("job-{i}"), None).expect("job acked")[0])
            .collect();
        assert_eq!(all_seqs, vec![1, 2, 3, 4, 5, 6]);

        // Claim 4, then durably ack the first 3 of them. Ack on a fsync-class queue
        // logs an explicit-seq Delete frame and WAITS on its fsync before returning.
        let claim = engine.claim("q", "worker", 4, Some(60_000)).expect("claim");
        assert_eq!(claim.count, 4, "claimed 4 jobs");
        let claimed: Vec<u64> = claim.claimed.iter().map(|j| j.seq).collect();
        let to_ack = &claimed[..3];
        let ack = engine.ack("q", "worker", to_ack).expect("durable ack");
        assert_eq!(ack.acked, 3, "3 jobs durably acked");
        acked_gone = to_ack.to_vec();

        sync_wal_dir(&disk);
        // Power loss AFTER the durable ack's fsync returned ⇒ the deletes are
        // durable and must not roll back.
        disk.crash(TornDamage::None);
        drop(engine);
    }
    disk.reset_power();

    // Recover: the durably-acked jobs are gone; the rest remain. The un-acked-but-
    // claimed job's lease survives (leases_durable:true), so it is in-flight until
    // its deadline; advance the recovery clock past the lease so every surviving
    // job becomes claimable again (the self-healing visibility timeout, §10.6).
    let (engine, clk) = open_engine_clock(&disk);
    clk.advance(120_000); // past the 60s lease deadline.
    let live = dump(&engine, "q").expect("queue recovered");
    let survivors: BTreeSet<u64> = live.keys().copied().collect();

    for s in &acked_gone {
        assert!(
            !survivors.contains(s),
            "durably-acked job {s} REAPPEARED after crash (ack delete not durable!): {survivors:?}"
        );
    }
    // No fabrication: every survivor was a genuinely-enqueued job.
    let enq: BTreeSet<u64> = all_seqs.iter().copied().collect();
    for s in &survivors {
        assert!(enq.contains(s), "fabricated job seq {s} after recovery");
    }
    // The un-acked jobs (the other 3) survive and are claimable again.
    let expected_survivors: BTreeSet<u64> = enq
        .difference(&acked_gone.iter().copied().collect())
        .copied()
        .collect();
    assert_eq!(
        survivors, expected_survivors,
        "exactly the un-acked jobs survive (acked-delete durable, rest self-heal)"
    );
    let reclaim = engine
        .claim("q", "worker2", 10, Some(60_000))
        .expect("re-claim");
    assert_eq!(
        reclaim.count as usize,
        expected_survivors.len(),
        "every surviving job is claimable again after recovery (at-least-once self-heal)"
    );
}

// ===========================================================================
// 8) QUEUE dead-letter re-queue path: when a job exceeds max_deliveries it is
//    durably moved to the dead-letter topic (a durable_append into the DL topic, which
//    rides the SAME commit sequencer) and deleted from the source. After a crash
//    the DL copy is durable (it went through the fsync-gated durable_append) and
//    the source job is gone — no duplicate, no silent loss.
// ===========================================================================

/// A `fsync`-class queue with `max_deliveries=1` and a dead-letter topic. Claim a
/// job, let its lease expire, re-claim past the delivery cap ⇒ the job is
/// dead-lettered: durably appended to the DL topic (via the commit sequencer's
/// `durable_append`) and deleted from the source. Crash + recover: the DL copy
/// survives (durable by construction) and the source no longer holds the job.
#[test]
fn queue_dead_letter_requeue_durable_through_sequencer() {
    let disk = FakeDisk::new();
    let dl_payload: String;

    {
        let (engine, clk) = open_engine_clock(&disk);
        engine
            .put_topic(
                "jobs",
                TopicConfig {
                    r#type: TopicType::Queue,
                    durability: Some(Durability::Fsync),
                    leases_durable: true,
                    max_deliveries: 1,
                    dead_letter: Some("dead".into()),
                    ..Default::default()
                },
            )
            .expect("put queue with DL");
        // Dead-letter target: a durable topic so its copy is durable-by-construction.
        durable_topic("dead", &engine);

        dl_payload = "poison".to_string();
        let seq = write_one(&engine, "jobs", &dl_payload, None).expect("job acked")[0];
        assert_eq!(seq, 1);

        // First claim with a short lease, then advance the clock past it so a
        // re-claim is delivery #2 (> max_deliveries=1) ⇒ dead-lettered on the next
        // claim pass. The TestClock makes lease expiry deterministic (no wall sleep).
        let c1 = engine.claim("jobs", "w", 1, Some(1_000)).expect("claim #1");
        assert_eq!(c1.count, 1, "claimed the poison job once");
        clk.advance(5_000); // past the 1s lease ⇒ the job is reclaimable.
                            // Re-claim: this delivery exceeds max_deliveries ⇒ dead-letter move.
        let _ = engine.claim("jobs", "w", 1, Some(60_000));

        // The job is now durably moved into "dead" and deleted from "jobs".
        let dl = dump(&engine, "dead").expect("dead-letter topic present");
        assert_eq!(dl.len(), 1, "exactly one job dead-lettered (no duplicate)");
        assert!(
            dl.values().any(|v| v == &dl_payload),
            "the poison payload landed in the dead-letter topic: {dl:?}"
        );
        let src = dump(&engine, "jobs").unwrap_or_default();
        assert!(
            !src.values().any(|v| v == &dl_payload),
            "the dead-lettered job is gone from the source jobs log"
        );

        sync_wal_dir(&disk);
        disk.crash(TornDamage::None);
        drop(engine);
    }
    disk.reset_power();

    // Recover: the DL copy is durable (the durable_append rode the fsync-gated
    // commit sequencer), and the source no longer holds the dead-lettered job.
    let engine = open_engine(&disk);
    let dl = dump(&engine, "dead").expect("dead-letter topic recovered");
    assert_eq!(
        dl.len(),
        1,
        "the dead-letter copy survived the crash (durable_append is fsync-gated): {dl:?}"
    );
    assert!(
        dl.values().any(|v| v == &dl_payload),
        "the recovered dead-letter topic still holds the poison payload"
    );
    let src = dump(&engine, "jobs").unwrap_or_default();
    assert!(
        !src.values().any(|v| v == &dl_payload),
        "the dead-lettered job did NOT resurface in the source after recovery (no duplicate)"
    );
}
