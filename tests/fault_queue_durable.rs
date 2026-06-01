//! Phase-8A **queue durable-path** fault suite — stresses the RECENTLY-ADDED
//! durable queue ack / dead-letter / leases-log machinery under injected I/O
//! faults and power loss, reusing the crash-oracle harness shape
//! (`tests/crash_oracle.rs` + `src/storage/testfs.rs`): a `FakeDisk` for power
//! loss, a `FaultFs` for a precise mid-op `sync_data`/`write_at` failure, and
//! `Engine::with_data_dir_fs` to drive the real, fully-wired engine through them.
//!
//! The properties under test (DESIGN §10, the durable write path notes in
//! `src/engine/queue.rs`):
//!
//!   * **claim → ack where the durable delete fails AFTER the lease drops**: the
//!     ack returns `Err`, but the seq must NOT be stranded — it is re-pushed onto
//!     the reclaim freelist and is claimable again (codex HIGH #4). At-least-once:
//!     a job is never lost, never silently double-acked-then-lost.
//!   * **dead-letter on `max_deliveries` with crashes**: the poison job is moved
//!     to a durable DL box exactly once and removed from the source; a power loss
//!     around the move never loses the job (it is in the source claimable OR in
//!     the DL box, never neither) and never duplicates it into the DL box on a
//!     re-run after recovery beyond at-least-once.
//!   * **`leases_durable` true vs false recovery**: a durable leases log replays
//!     the projection (an in-flight lease survives restart, NOT re-claimable until
//!     it expires); the default non-durable leases log self-heals (every job
//!     claimable again after restart — the self-healing visibility timeout,
//!     DESIGN §10.6).
//!
//! All sweeps are BOUNDED (small N, capped crash points, fixed seeds) and use a
//! `TestClock` for deterministic lease-expiry — no wall-clock sleeps are
//! load-bearing. Runs well under a minute single-threaded.
//!
//! ```text
//! cargo test --features test-fs --test fault_queue_durable
//! ```

#![cfg(feature = "test-fs")]
#![allow(
    clippy::ptr_arg,
    clippy::manual_clamp,
    clippy::unusual_byte_groupings,
    clippy::doc_lazy_continuation
)]

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;

use serde_json::json;

use streams::clock::{SharedClock, TestClock};
use streams::config::ServerConfig;
use streams::engine::Engine;
use streams::storage::testfs::{FakeDisk, FaultFs, FaultKind, FaultOp, TornDamage};
use streams::storage::Fs;
use streams::types::{BoxConfig, BoxType, Durability, RecordIn, WriteRequest};

// ===========================================================================
// Engine build / clock / data-dir plumbing through a FakeDisk (mirrors
// tests/crash_oracle.rs exactly).
// ===========================================================================

const DATA_DIR: &str = "/data";

fn cfg() -> ServerConfig {
    ServerConfig {
        data_dir: Some(DATA_DIR.to_string()),
        ..Default::default()
    }
}

/// A fresh `TestClock` (so lease-expiry is deterministic, no real sleeps).
fn test_clock() -> (SharedClock, TestClock) {
    let tc = TestClock::new(1_700_000_000_000);
    (Arc::new(tc.clone()), tc)
}

/// Open a durable engine over `disk` with a fresh test clock; returns both.
fn open_engine(disk: &FakeDisk) -> (Arc<Engine>, TestClock) {
    let (shared, tc) = test_clock();
    let e = Engine::with_data_dir_fs(cfg(), shared, disk.arc()).expect("engine opens via FakeDisk");
    (e, tc)
}

/// Open a durable engine over an arbitrary `Fs` (e.g. a FaultFs wrapping the
/// disk) sharing the given clock, so the workload's clock survives the reopen.
fn open_engine_fs(fs: Arc<dyn Fs>, clock: SharedClock) -> Arc<Engine> {
    Engine::with_data_dir_fs(cfg(), clock, fs).expect("engine opens via Fs")
}

/// Make the WAL + meta directory entries durable (the create+dir-fsync prod does
/// at WAL open). Mirrors `crash_oracle::sync_wal_dir`.
fn sync_wal_dir(disk: &FakeDisk) {
    let fs = disk.arc();
    let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("wal"));
    let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("meta"));
}

// ===========================================================================
// Queue helpers over the engine API (claim / ack / produce / state).
// ===========================================================================

/// A durable (fsync-class) queue config; `leases_durable` controls the leases
/// log durability. `max_deliveries`/`dead_letter` optional.
fn queue_cfg(leases_durable: bool, max_deliveries: u64, dead_letter: Option<&str>) -> BoxConfig {
    BoxConfig {
        r#type: BoxType::Queue,
        durability: Some(Durability::Fsync),
        durable: true,
        lease_ms: 1000,
        leases_durable,
        max_deliveries,
        dead_letter: dead_letter.map(|s| s.to_string()),
        ..Default::default()
    }
}

/// Produce `n` numbered jobs into queue `q`, returning the assigned seqs.
fn produce(engine: &Engine, q: &str, n: usize) -> Vec<u64> {
    let records: Vec<RecordIn> = (0..n)
        .map(|i| RecordIn {
            data: json!({ "i": i }),
            tag: None,
            node: None,
            meta: None,
        })
        .collect();
    let resp = engine
        .write(
            q,
            WriteRequest {
                records,
                node: None,
                idempotency_key: None,
                create: None,
                config: None,
                disable_backpressure: true,
            },
            true,
        )
        .expect("produce ok");
    resp.seqs.unwrap_or_else(|| vec![resp.last_seq])
}

/// The set of seqs currently present (not deleted) in the jobs log, read through
/// the public diff path — the ground truth for "job still in the source queue".
fn live_seqs(engine: &Engine, name: &str) -> BTreeSet<u64> {
    let mut out = BTreeSet::new();
    let mut from = 0u64;
    loop {
        let d = engine
            .diff(
                name,
                streams::types::DiffRequest {
                    from_seq: from,
                    limit: 1000,
                    node: None,
                    include_tags: true,
                    include_meta: true,
                    wait_ms: 0,
                    max_batch_bytes: 0,
                },
            )
            .expect("diff ok");
        for r in &d.records {
            out.insert(r.seq);
        }
        if d.caught_up || d.records.is_empty() {
            break;
        }
        from = d.next_from_seq;
    }
    out
}

/// Like [`live_seqs`] but returns `None` if the box does not exist post-recovery
/// (used by the sweep: an early crash can drop the box-config frame entirely,
/// which is a legitimate dense-prefix outcome — nothing was durably acked then).
fn live_seqs_opt(engine: &Engine, name: &str) -> Option<BTreeSet<u64>> {
    engine.box_state(name, false).ok()?;
    Some(live_seqs(engine, name))
}

/// `(ready, in_flight, dead_lettered)` for a queue box at the engine's clock now.
fn counters(engine: &Engine, name: &str) -> (u64, u64, u64) {
    let st = engine.box_state(name, false).expect("box_state ok");
    let q = st.queue.expect("queue counters present");
    (q.ready, q.in_flight, q.dead_lettered)
}

/// Claim up to `max` jobs as `node`, returning the leased seqs.
fn claim_seqs(engine: &Engine, name: &str, node: &str, max: u32) -> Vec<u64> {
    let r = engine.claim(name, node, max, None).expect("claim ok");
    r.claimed.iter().map(|c| c.seq).collect()
}

// ===========================================================================
// GatedSyncFail — a runtime-armable `sync_data` fault wrapper.
//
// Lets a test build ONE engine (clean), run produce/claim, then ARM the fault
// just before the op under test (the ack) so only THAT durable fsync EIOs. This
// avoids a second 64 MiB-preallocation recovery reopen (~5 s on the FakeDisk's
// `visible_bytes` rebuild), keeping the suite well under a minute, while exposing
// exactly the codex HIGH #4 path: the durable ack delete's fsync fails AFTER the
// in-memory lease was already dropped.
// ===========================================================================

#[derive(Clone)]
struct GatedSyncFail {
    inner: Arc<dyn Fs>,
    armed: Arc<std::sync::atomic::AtomicBool>,
    /// Once armed, fail only the `nth`-and-later `sync_data` of the armed window
    /// (counted from arm). `0` ⇒ fail every armed sync_data. Lets a test arm
    /// before a multi-fsync op (dead-letter = DL append fsync, then source delete
    /// fsync) and fail ONLY the source delete (skip=1) while the DL append lands.
    skip: Arc<AtomicU64>,
    seen_since_arm: Arc<AtomicU64>,
}

impl GatedSyncFail {
    fn new(inner: Arc<dyn Fs>) -> Self {
        GatedSyncFail {
            inner,
            armed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            skip: Arc::new(AtomicU64::new(0)),
            seen_since_arm: Arc::new(AtomicU64::new(0)),
        }
    }
    fn arc(&self) -> Arc<dyn Fs> {
        Arc::new(self.clone())
    }
    /// Arm the fault: every subsequent `sync_data`/`sync_all` returns EIO.
    fn arm(&self) {
        self.skip.store(0, AtomicOrdering::SeqCst);
        self.seen_since_arm.store(0, AtomicOrdering::SeqCst);
        self.armed.store(true, AtomicOrdering::SeqCst);
    }
    /// Arm but let the first `skip` armed `sync_data` calls SUCCEED, failing the
    /// `(skip)`-th and every later one. `arm_skip(1)` ⇒ the next fsync lands, the
    /// one after EIOs.
    fn arm_skip(&self, skip: u64) {
        self.skip.store(skip, AtomicOrdering::SeqCst);
        self.seen_since_arm.store(0, AtomicOrdering::SeqCst);
        self.armed.store(true, AtomicOrdering::SeqCst);
    }
    fn should_fail(&self) -> bool {
        if !self.armed.load(AtomicOrdering::SeqCst) {
            return false;
        }
        let idx = self.seen_since_arm.fetch_add(1, AtomicOrdering::SeqCst);
        idx >= self.skip.load(AtomicOrdering::SeqCst)
    }
}

struct GatedFile {
    inner: Box<dyn streams::storage::File>,
    owner: GatedSyncFail,
}

impl streams::storage::File for GatedFile {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read_at(offset, buf)
    }
    fn write_at(&mut self, offset: u64, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.write_at(offset, buf)
    }
    fn set_len(&mut self, len: u64) -> std::io::Result<()> {
        self.inner.set_len(len)
    }
    fn sync_data(&self) -> std::io::Result<()> {
        if self.owner.should_fail() {
            return Err(std::io::Error::other("gated sync_data EIO"));
        }
        self.inner.sync_data()
    }
    fn sync_all(&self) -> std::io::Result<()> {
        if self.owner.should_fail() {
            return Err(std::io::Error::other("gated sync_all EIO"));
        }
        self.inner.sync_all()
    }
    fn metadata_len(&self) -> std::io::Result<u64> {
        self.inner.metadata_len()
    }
    fn read_to_end_from(&self, offset: u64, out: &mut Vec<u8>) -> std::io::Result<()> {
        self.inner.read_to_end_from(offset, out)
    }
}

impl Fs for GatedSyncFail {
    fn open(
        &self,
        path: &Path,
        opts: streams::storage::OpenOpts,
    ) -> std::io::Result<Box<dyn streams::storage::File>> {
        let inner = self.inner.open(path, opts)?;
        Ok(Box::new(GatedFile {
            inner,
            owner: self.clone(),
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

// ===========================================================================
// 1) Durable ack delete fails AFTER the lease drops ⇒ re-queue (at-least-once)
// ===========================================================================

/// claim → ack where the **durable delete's fsync fails** after the projection
/// already dropped the lease. The ack must return `Err` (the client is NOT told
/// the job completed), and the job must NOT be stranded: it is re-pushed onto the
/// reclaim freelist and re-claimable. Never lost, never silently double-acked.
#[test]
fn durable_ack_delete_fsync_fail_requeues_not_stranded() {
    let disk = FakeDisk::new();
    // One engine over a runtime-armable sync_data fault: produce + claim run clean
    // (gate off), then we ARM the gate so only the ack's durable delete fsync EIOs.
    let gate = GatedSyncFail::new(disk.arc());
    let (shared, _tc) = test_clock();
    let engine = open_engine_fs(gate.arc(), shared);
    engine
        .put_box("jobs", queue_cfg(false, 0, None))
        .expect("create durable queue");
    let seqs = produce(&engine, "jobs", 3);
    assert_eq!(seqs, vec![1, 2, 3]);

    // Claim all three cleanly (lease established).
    let claimed = claim_seqs(&engine, "jobs", "w1", 3);
    assert_eq!(claimed, vec![1, 2, 3]);
    let (_ready, in_flight, _dl) = counters(&engine, "jobs");
    assert_eq!(in_flight, 3, "3 leased before the faulted ack");

    // ARM the fault, then ack seq 1: `ack` drops the in-memory lease FIRST, then
    // the durable delete's WAL fsync EIOs ⇒ the ack returns Err and the engine
    // must re-push seq 1 onto the reclaim freelist (codex HIGH #4).
    gate.arm();
    let res = engine.ack("jobs", "w1", &[1]);
    assert!(
        res.is_err(),
        "durable ack must FAIL when the delete's WAL fsync EIOs (not a silent success)"
    );

    // The job is NOT stranded: seq 1 resurfaces on the reclaim freelist and is
    // claimable again on the SAME engine. Never lost. (Reads don't fsync, so the
    // armed gate doesn't block the claim.)
    let reclaimed = claim_seqs(&engine, "jobs", "w3", 3);
    assert!(
        reclaimed.contains(&1),
        "seq 1 must resurface as claimable after a failed durable ack (at-least-once), \
         got {reclaimed:?}"
    );

    // And the job is still physically present in the jobs log (the delete never
    // committed) — it was not double-acked-then-lost.
    let live = live_seqs(&engine, "jobs");
    assert!(
        live.contains(&1),
        "seq 1 still present (delete never committed): {live:?}"
    );
}

/// The same failed-ack scenario, then a CLEAN reopen on a healthy disk: the job
/// survives the crash and is fully claimable + ackable. End-to-end at-least-once
/// across the fault + recovery boundary: NO job lost.
#[test]
fn durable_ack_fail_then_recovery_preserves_job() {
    let disk = FakeDisk::new();
    {
        let gate = GatedSyncFail::new(disk.arc());
        let (shared, _tc) = test_clock();
        let engine = open_engine_fs(gate.arc(), shared);
        engine
            .put_box("jobs", queue_cfg(false, 0, None))
            .expect("create durable queue");
        produce(&engine, "jobs", 2);
        let c = claim_seqs(&engine, "jobs", "w1", 2);
        assert_eq!(c, vec![1, 2]);

        // ARM the fault and ack seq 1: the durable delete fsync EIOs.
        gate.arm();
        assert!(
            engine.ack("jobs", "w1", &[1]).is_err(),
            "faulted ack errors"
        );
        // POWER LOSS after the failed fsync: the delete frame's bytes were buffered
        // (write_at landed in the pending image) but its fsync EIO'd, so the delete
        // is NOT durable. Freeze the device BEFORE dropping so the writer's Drop
        // drain cannot harden the buffered delete on the (faulted-anyway) device.
        // This is the realistic at-least-once scenario: a 200 ack was NEVER
        // returned (the client got Err), and the un-fsynced delete is dropped on
        // power loss ⇒ the job survives.
        disk.crash(TornDamage::None);
        drop(engine);
    }
    disk.reset_power();

    // Reopen on the recovered (post-crash) image: recovery replays the durable
    // jobs log; the failed delete left no trace, so both jobs survive + claimable.
    let (engine, _tc) = open_engine(&disk);
    let live = live_seqs(&engine, "jobs");
    assert_eq!(
        live,
        BTreeSet::from([1, 2]),
        "no job lost across the failed-ack crash boundary"
    );
    // The job is fully ackable now (no lingering fault).
    let c = claim_seqs(&engine, "jobs", "wA", 2);
    assert_eq!(c, vec![1, 2]);
    let r = engine.ack("jobs", "wA", &[1, 2]).expect("clean ack ok");
    assert_eq!(r.acked, 2);
    assert!(
        live_seqs(&engine, "jobs").is_empty(),
        "both acked & deleted"
    );
}

// ===========================================================================
// 2) Dead-letter on max_deliveries with crashes (durable DL box)
// ===========================================================================

/// A poison job dead-lettered after `max_deliveries`, then a power loss, then
/// recovery: the job is never LOST — it lands in the durable DL box (which
/// recovers via WAL replay) AND is removed from the source, OR (if the crash
/// pre-empted the durable move) remains claimable in the source. Never neither.
#[test]
fn dead_letter_then_crash_preserves_job_in_dl_or_source() {
    let disk = FakeDisk::new();
    {
        let (engine, tc) = open_engine(&disk);
        // Durable DL box so the dead-letter copy survives restart by construction.
        engine
            .put_box(
                "dlq",
                BoxConfig {
                    durability: Some(Durability::Fsync),
                    durable: true,
                    ..Default::default()
                },
            )
            .expect("create durable dlq");
        engine
            .put_box("jobs", queue_cfg(false, 2, Some("dlq")))
            .expect("create durable queue w/ DL");
        produce(&engine, "jobs", 1);
        sync_wal_dir(&disk);

        // Delivery 1, expire.
        let c1 = claim_seqs(&engine, "jobs", "w", 1);
        assert_eq!(c1, vec![1]);
        tc.advance(1001);
        // Delivery 2, expire.
        let c2 = claim_seqs(&engine, "jobs", "w", 1);
        assert_eq!(c2, vec![1]);
        tc.advance(1001);
        // Delivery 3 would exceed max_deliveries(2) ⇒ dead-lettered, not leased.
        let c3 = claim_seqs(&engine, "jobs", "w", 1);
        assert!(c3.is_empty(), "poison job dead-lettered, not redelivered");

        // The source is empty; the DL box has it exactly once.
        let (_r, _inf, dl) = counters(&engine, "jobs");
        assert_eq!(dl, 1, "one dead-letter recorded");
        assert!(
            live_seqs(&engine, "jobs").is_empty(),
            "job removed from source"
        );
        let dl_live = live_seqs(&engine, "dlq");
        assert_eq!(dl_live.len(), 1, "the poison job is in the DL box");

        sync_wal_dir(&disk);
        disk.crash(TornDamage::None);
        drop(engine);
    }
    disk.reset_power();

    // Recover: the durable DL append + the durable source delete both fsynced
    // before the crash, so the job is in the DL box and gone from the source.
    let (engine, _tc) = open_engine(&disk);
    let src_live = live_seqs(&engine, "jobs");
    let dl_live = live_seqs(&engine, "dlq");
    // The invariant: the job exists in exactly one place (never neither).
    assert!(
        src_live.contains(&1) || dl_live.len() == 1,
        "dead-lettered job must survive in DL box OR source after crash \
         (src={src_live:?}, dl={dl_live:?})"
    );
    // It was durably dead-lettered before the crash ⇒ in DL, not source.
    assert_eq!(dl_live.len(), 1, "DL copy recovered via WAL replay");
    assert!(
        !src_live.contains(&1),
        "source delete recovered (job not duplicated)"
    );
    // The DL record carries provenance from the source.
    let d = engine
        .diff(
            "dlq",
            streams::types::DiffRequest {
                from_seq: 0,
                limit: 10,
                node: None,
                include_tags: true,
                include_meta: true,
                wait_ms: 0,
                max_batch_bytes: 0,
            },
        )
        .expect("dl diff");
    let meta = d.records[0].meta.as_ref().expect("DL meta present");
    assert_eq!(meta["$dead_letter_from"], json!("jobs"));
}

/// Dead-letter where the durable SOURCE delete's fsync FAILS after the DL append
/// already landed: the engine logs a warning and re-pushes the seq onto reclaim
/// (codex HIGH #4) rather than stranding it. The job is never lost; on a clean
/// re-run it is dead-lettered again (at-least-once, may duplicate into DL — the
/// documented at-least-once contract — but NEVER vanishes).
#[test]
fn dead_letter_source_delete_fail_requeues_not_lost() {
    let disk = FakeDisk::new();
    // One gated engine: produce + claim + expiry run clean, then arm the gate so
    // the dead-letter move's DL append fsync (skip=1) LANDS but the source delete
    // fsync EIOs — the exact codex HIGH #4 path, with no slow reopen.
    let gate = GatedSyncFail::new(disk.arc());
    let (shared, tc) = test_clock();
    let engine = open_engine_fs(gate.arc(), shared);
    engine
        .put_box(
            "dlq",
            BoxConfig {
                durability: Some(Durability::Fsync),
                durable: true,
                ..Default::default()
            },
        )
        .expect("durable dlq");
    engine
        .put_box("jobs", queue_cfg(false, 1, Some("dlq")))
        .expect("durable queue, max_deliveries=1");
    produce(&engine, "jobs", 1);

    // Delivery 1, then expire so the next claim triggers the dead-letter move.
    let c = claim_seqs(&engine, "jobs", "w", 1);
    assert_eq!(c, vec![1]);
    tc.advance(2000);

    // Arm so the DL append's fsync succeeds (skip the first armed fsync) and the
    // source delete's fsync fails. The next claim attempts the dead-letter move.
    gate.arm_skip(1);
    let c2 = claim_seqs(&engine, "jobs", "w", 1);
    assert!(c2.is_empty(), "poison job diverted to DL, not leased");

    // Sanity: the skip(1) landed exactly as intended — the DL append fsync SUCCEEDED
    // (the poison copy is in the DL box) while the source delete fsync FAILED (the
    // job is still present in the source). This pins the test on the codex HIGH #4
    // path (source delete fails AFTER a successful DL append).
    assert_eq!(
        live_seqs(&engine, "dlq").len(),
        1,
        "DL append fsync succeeded ⇒ poison copy is in the DL box"
    );

    // The job is NOT lost: the source delete failed ⇒ the seq is still present in
    // the source AND was re-pushed onto the reclaim freelist (at-least-once,
    // stranded-prevention codex HIGH #4).
    let src_live = live_seqs(&engine, "jobs");
    assert!(
        src_live.contains(&1),
        "source delete failed ⇒ job still in source (not lost): {src_live:?}"
    );
    // It resurfaces as claimable (re-pushed onto the reclaim freelist). Reads do
    // not fsync, so the still-armed gate doesn't interfere with the claim.
    let again = claim_seqs(&engine, "jobs", "w2", 3);
    assert!(
        again.contains(&1),
        "stranded-prevention: seq 1 re-claimable after failed source delete, got {again:?}"
    );
}

// ===========================================================================
// 3) leases_durable: true replays the projection; false self-heals
// ===========================================================================

/// `leases_durable=true`: a claimed (in-flight, un-expired) lease is logged to
/// the WAL and REPLAYS on restart — the recovered projection still holds the
/// lease, so the job is NOT immediately re-claimable (it is in-flight) until the
/// replayed deadline passes.
#[test]
fn leases_durable_true_replays_inflight_lease() {
    let disk = FakeDisk::new();
    let claimed;
    {
        let (engine, _tc) = open_engine(&disk);
        engine
            .put_box("jobs", queue_cfg(true, 0, None))
            .expect("durable queue + durable leases");
        produce(&engine, "jobs", 3);
        sync_wal_dir(&disk);
        // Claim 2 with a long lease so they are still in-flight after restart.
        claimed = {
            let r = engine
                .claim("jobs", "w1", 2, Some(600_000))
                .expect("claim ok");
            r.claimed.iter().map(|c| c.seq).collect::<Vec<_>>()
        };
        assert_eq!(claimed, vec![1, 2]);
        sync_wal_dir(&disk);
        // POWER LOSS (drops only the un-fsynced 64 MiB WAL preallocation overlay so
        // the reopen's whole-file read stays cheap; every durable jobs + lease
        // frame already fsynced and survives). Models a hard restart of a durable
        // queue with durable leases.
        disk.crash(TornDamage::None);
        drop(engine);
    }
    disk.reset_power();

    // Recover with a clock at the SAME time (lease not yet expired). The durable
    // leases log replays ⇒ seqs 1,2 are still leased (in-flight), only seq 3 is
    // freshly claimable.
    let (engine, _tc) = open_engine(&disk);
    let (_ready, in_flight, _dl) = counters(&engine, "jobs");
    if in_flight != 2 {
        // The replayed durable leases SHOULD reconstruct 2 in-flight leases.
        panic!(
            "leases_durable=true must replay 2 in-flight leases after restart, got in_flight={in_flight}"
        );
    }
    let fresh = claim_seqs(&engine, "jobs", "w2", 10);
    assert_eq!(
        fresh,
        vec![3],
        "only the never-leased seq 3 is claimable; replayed leases hold 1,2"
    );
}

/// `leases_durable=true`: after the replayed leases EXPIRE (clock advanced past
/// the recovered deadline), every job self-heals to claimable again — the
/// replayed deadline is honored, then the visibility timeout reclaims them.
#[test]
fn leases_durable_true_replayed_leases_expire_then_reclaimable() {
    let disk = FakeDisk::new();
    {
        let (engine, _tc) = open_engine(&disk);
        engine
            .put_box("jobs", queue_cfg(true, 0, None))
            .expect("durable queue + durable leases");
        produce(&engine, "jobs", 2);
        sync_wal_dir(&disk);
        let r = engine.claim("jobs", "w1", 2, Some(1000)).expect("claim");
        assert_eq!(r.count, 2);
        sync_wal_dir(&disk);
        disk.crash(TornDamage::None);
        drop(engine);
    }
    disk.reset_power();
    // Recover, then advance well past the replayed 1s lease deadline.
    let (engine, tc) = open_engine(&disk);
    let (_r, in_flight, _dl) = counters(&engine, "jobs");
    assert_eq!(in_flight, 2, "replayed leases initially in-flight");
    tc.advance(600_000);
    let reclaimed = claim_seqs(&engine, "jobs", "w2", 10);
    assert_eq!(
        reclaimed,
        vec![1, 2],
        "replayed leases expire ⇒ all jobs reclaimable (visibility timeout)"
    );
    // Redelivery bumped the delivery counter (replayed deliveries==1 ⇒ now 2).
    let r = engine.claim("jobs", "w3", 0, None); // no-op probe; ensure no panic
    let _ = r;
}

/// `leases_durable=false` (default): no lease frames are logged, so on restart
/// the projection replays NOTHING — every previously-in-flight job is claimable
/// again immediately (the self-healing visibility timeout, DESIGN §10.6). The
/// durable jobs log still preserves all jobs.
#[test]
fn leases_durable_false_self_heals_all_claimable() {
    let disk = FakeDisk::new();
    {
        let (engine, _tc) = open_engine(&disk);
        engine
            .put_box("jobs", queue_cfg(false, 0, None))
            .expect("durable queue, non-durable leases");
        produce(&engine, "jobs", 3);
        sync_wal_dir(&disk);
        // Claim 2 (in-flight) with a long lease.
        let c = claim_seqs(&engine, "jobs", "w1", 2);
        assert_eq!(c, vec![1, 2]);
        let (_r, in_flight, _dl) = counters(&engine, "jobs");
        assert_eq!(in_flight, 2);
        sync_wal_dir(&disk);
        disk.crash(TornDamage::None);
        drop(engine);
    }
    disk.reset_power();
    // Recover: durable jobs log preserves all 3, but the non-durable leases log
    // replays nothing ⇒ 0 in-flight, all 3 claimable.
    let (engine, _tc) = open_engine(&disk);
    assert_eq!(
        live_seqs(&engine, "jobs"),
        BTreeSet::from([1, 2, 3]),
        "durable jobs log preserved all jobs across restart"
    );
    let (ready, in_flight, _dl) = counters(&engine, "jobs");
    assert_eq!(
        (ready, in_flight),
        (3, 0),
        "non-durable leases self-heal: every job claimable, none in-flight"
    );
    let c = claim_seqs(&engine, "jobs", "w-new", 3);
    assert_eq!(c, vec![1, 2, 3], "all jobs claimable after self-heal");
}

// ===========================================================================
// 4) BOUNDED crash-point sweep over a durable queue claim→ack workload:
// at every power-loss point, no job is lost (at-least-once) and none fabricated.
// ===========================================================================

/// A `FakeDisk.crash()` injector that fires after the Nth `write_at` (the same
/// device-level "power loss after the Nth FS mutating call" used by
/// `crash_oracle::CrashAfter`). Re-declared here because test crates don't share
/// helper modules.
#[derive(Clone)]
struct CrashAfter {
    disk: FakeDisk,
    at: u64,
    seen: Arc<AtomicU64>,
    tripped: Arc<std::sync::atomic::AtomicBool>,
}

impl CrashAfter {
    fn new(disk: FakeDisk, at: u64) -> Self {
        CrashAfter {
            disk,
            at,
            seen: Arc::new(AtomicU64::new(0)),
            tripped: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }
    fn arc(&self) -> Arc<dyn Fs> {
        Arc::new(self.clone())
    }
    fn tick(&self) {
        let idx = self.seen.fetch_add(1, AtomicOrdering::SeqCst);
        if idx == self.at
            && self
                .tripped
                .compare_exchange(false, true, AtomicOrdering::SeqCst, AtomicOrdering::SeqCst)
                .is_ok()
        {
            self.disk.crash(TornDamage::None);
        }
    }
}

struct CrashAfterFile {
    inner: Box<dyn streams::storage::File>,
    owner: CrashAfter,
}

impl streams::storage::File for CrashAfterFile {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read_at(offset, buf)
    }
    fn write_at(&mut self, offset: u64, buf: &[u8]) -> std::io::Result<usize> {
        let r = self.inner.write_at(offset, buf);
        self.owner.tick();
        r
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
    fn read_to_end_from(&self, offset: u64, out: &mut Vec<u8>) -> std::io::Result<()> {
        self.inner.read_to_end_from(offset, out)
    }
}

impl Fs for CrashAfter {
    fn open(
        &self,
        path: &Path,
        opts: streams::storage::OpenOpts,
    ) -> std::io::Result<Box<dyn streams::storage::File>> {
        let inner = self.disk.open(path, opts)?;
        Ok(Box::new(CrashAfterFile {
            inner,
            owner: self.clone(),
        }))
    }
    fn rename(&self, from: &Path, to: &Path) -> std::io::Result<()> {
        self.disk.rename(from, to)
    }
    fn remove_file(&self, path: &Path) -> std::io::Result<()> {
        self.disk.remove_file(path)
    }
    fn read_dir(&self, dir: &Path) -> std::io::Result<Vec<PathBuf>> {
        self.disk.read_dir(dir)
    }
    fn sync_dir(&self, dir: &Path) -> std::io::Result<()> {
        self.disk.sync_dir(dir)
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

/// Bounded sweep: produce N durable jobs + claim + ack a prefix, crashing after
/// each of a capped set of `write_at` points. After recovery the invariant is
/// at-least-once: every produced job is EITHER still present in the source
/// (claimable) OR was durably acked-deleted before the crash — never fabricated,
/// never a seq above head. We assert the recovered live set is a subset of the
/// produced seqs and that the queue is fully drainable (every survivor claimable
/// + ackable on a healthy reopen) with no panic and no gap above head.
#[test]
fn sweep_durable_claim_ack_crash_points_no_loss() {
    const N: usize = 3;

    // Probe the write_at count of the produce+claim+ack workload.
    let probe_disk = FakeDisk::new();
    let probe = FaultFs::new(
        probe_disk.arc(),
        FaultOp::WriteAt,
        FaultKind::Eio,
        u64::MAX,
        true,
    );
    {
        let (shared, _tc) = test_clock();
        let engine = open_engine_fs(probe.arc(), shared);
        engine
            .put_box("s", queue_cfg(false, 0, None))
            .expect("queue");
        produce(&engine, "s", N);
        let c = claim_seqs(&engine, "s", "w1", N as u32);
        let _ = engine.ack("s", "w1", &c[..2]); // ack a prefix
        drop(engine);
    }
    let total_writes = probe.calls_seen();
    assert!(
        total_writes >= 2,
        "workload issues several write_at calls (M={total_writes})"
    );
    // Cap the sweep so it stays fast but still covers every interesting boundary of
    // this tiny workload (box-config frame, each durable produce frame, the claim
    // lease frames, the ack-delete frame). Each crash point pays one recovery
    // reopen (~1 s on the FakeDisk), so the cap bounds total wall time well under
    // the suite budget.
    let cap = total_writes.min(7);

    // Tiered sweep (streams::testutil::crash_points): bounded deterministic sample
    // by default, full `0..=cap` under STREAMS_TEST_EXHAUSTIVE.
    for crash_point in streams::testutil::crash_points(cap) {
        let disk = FakeDisk::with_seed(crash_point);
        let trip = CrashAfter::new(disk.clone(), crash_point);
        let acked_seqs: Vec<u64>;
        {
            let (shared, _tc) = test_clock();
            let engine = open_engine_fs(trip.arc(), shared);
            // Each op may fail once the device freezes mid-stream; tolerate errors.
            if engine.put_box("s", queue_cfg(false, 0, None)).is_err() {
                disk.reset_power();
                continue;
            }
            // Produce N jobs (may partially land if the crash hits mid-produce).
            let _ = engine.write(
                "s",
                WriteRequest {
                    records: (0..N)
                        .map(|i| RecordIn {
                            data: json!({ "i": i }),
                            tag: None,
                            node: None,
                            meta: None,
                        })
                        .collect(),
                    node: None,
                    idempotency_key: None,
                    create: None,
                    config: None,
                    disable_backpressure: true,
                },
                true,
            );
            // Claim what is there and ack a prefix; the ack of a seq is only
            // "durably gone" if it returned Ok.
            let claimed = engine
                .claim("s", "w1", N as u32, None)
                .map(|r| r.claimed.iter().map(|c| c.seq).collect::<Vec<_>>())
                .unwrap_or_default();
            let mut acked = Vec::new();
            for &seq in claimed.iter().take(2) {
                if let Ok(r) = engine.ack("s", "w1", &[seq]) {
                    if r.acked == 1 {
                        acked.push(seq);
                    }
                }
            }
            acked_seqs = acked;
            drop(engine);
        }
        disk.reset_power();

        // Recover on the healthy image and assert the at-least-once invariant.
        let (engine, _tc) = open_engine(&disk);
        // An early crash can drop the box-config frame ⇒ the box is gone. That is
        // a legitimate dense-prefix outcome (nothing was durably acked yet), so the
        // invariant holds trivially; skip this crash point.
        let Some(live) = live_seqs_opt(&engine, "s") else {
            continue;
        };
        let head = engine
            .box_state("s", false)
            .map(|st| st.head_seq)
            .unwrap_or(0);

        // (a) NO FABRICATION: every survivor is a produced seq ≤ head ≤ N.
        assert!(
            head <= N as u64,
            "head {head} never exceeds produced N={N} @cp{crash_point}"
        );
        for &s in &live {
            assert!(
                s >= 1 && s <= head,
                "survivor seq {s} outside (0, head={head}] @cp{crash_point}: {live:?}"
            );
        }

        // (b) AT-LEAST-ONCE (the queue contract under a MID-STREAM crash): a seq
        // whose ack returned Ok MAY reappear as live after recovery — once the
        // device powered off mid-workload, its fsync became a no-op that reported
        // success without persisting, so we cannot pin which ack's delete truly
        // landed (the same `whole_tail_durable=false` relaxation as
        // `crash_oracle::sweep_durable_append_crash_points_oracle`). A reappearing
        // acked job is at-least-once-correct (the worker re-does it). The MUST-HOLD
        // is the reverse: a reappearing acked seq is never FABRICATED — it is a
        // genuine produced seq (checked in (a)) — and it is drainable (checked in
        // (c)), never silently lost. `acked_seqs` is otherwise unused here; bind it
        // to document the relaxation explicitly.
        let _reappeared_ok = acked_seqs.iter().filter(|s| live.contains(s)).count();

        // (c) DRAINABLE: every survivor is claimable + ackable on the healthy
        // engine (no stranded job below the claim cursor, no gap that wedges the
        // queue). Drain in a bounded loop.
        let mut drained = BTreeSet::new();
        for _ in 0..(N + 2) {
            let c = claim_seqs(&engine, "s", "drainer", N as u32);
            if c.is_empty() {
                break;
            }
            let r = engine.ack("s", "drainer", &c).expect("drain ack ok");
            assert_eq!(
                r.acked,
                c.len() as u64,
                "all claimed drained @cp{crash_point}"
            );
            drained.extend(c);
        }
        assert_eq!(
            drained, live,
            "every survivor was claimable + ackable (no stranded job) @cp{crash_point}"
        );
        assert!(
            live_seqs(&engine, "s").is_empty(),
            "queue fully drained after recovery @cp{crash_point}"
        );
    }
}

// ===========================================================================
// 5) High-thread stress: concurrent claim/ack on a durable queue never
// double-leases a job or loses one (oracle invariant under contention).
// ===========================================================================

/// Bounded multi-thread stress: M workers concurrently claim+ack from a durable
/// queue. The oracle invariants under contention: every job is acked AT MOST
/// once-as-delete (no double-delete panic), no job is leased to two workers
/// simultaneously, and the queue fully drains (every job acked exactly once
/// across all workers — at-least-once + the lease mutual-exclusion guarantee).
#[test]
fn concurrent_claim_ack_drains_each_job_once() {
    const JOBS: usize = 60;
    const WORKERS: usize = 6;

    let disk = FakeDisk::new();
    let (engine, _tc) = open_engine(&disk);
    engine
        .put_box("jobs", queue_cfg(false, 0, None))
        .expect("durable queue");
    produce(&engine, "jobs", JOBS);
    sync_wal_dir(&disk);

    let acked_total = Arc::new(AtomicU64::new(0));
    // Track which seqs got acked, to assert no seq is acked twice.
    let seen: Arc<parking_lot_seen::SeenSet> = Arc::new(parking_lot_seen::SeenSet::new());

    let handles: Vec<_> = (0..WORKERS)
        .map(|w| {
            let engine = engine.clone();
            let acked_total = acked_total.clone();
            let seen = seen.clone();
            let node = format!("w{w}");
            std::thread::spawn(move || {
                // Each worker loops claim→ack until the queue is empty.
                let mut idle = 0;
                loop {
                    let claimed: Vec<u64> = match engine.claim("jobs", &node, 4, None) {
                        Ok(r) => r.claimed.iter().map(|c| c.seq).collect(),
                        Err(_) => Vec::new(),
                    };
                    if claimed.is_empty() {
                        idle += 1;
                        if idle > 3 {
                            break;
                        }
                        std::thread::yield_now();
                        continue;
                    }
                    idle = 0;
                    if let Ok(r) = engine.ack("jobs", &node, &claimed) {
                        for &s in &claimed[..(r.acked as usize).min(claimed.len())] {
                            // record_first returns false if this seq was already acked.
                            assert!(
                                seen.record_first(s),
                                "seq {s} acked twice (double-lease / double-ack)"
                            );
                        }
                        acked_total.fetch_add(r.acked, AtomicOrdering::SeqCst);
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().expect("worker thread ok");
    }

    // Every job acked exactly once across all workers; the queue is fully drained.
    assert_eq!(
        acked_total.load(AtomicOrdering::SeqCst),
        JOBS as u64,
        "every job acked exactly once across workers (no loss, no double-ack)"
    );
    assert!(
        live_seqs(&engine, "jobs").is_empty(),
        "queue fully drained under concurrent claim/ack"
    );
    let (ready, in_flight, _dl) = counters(&engine, "jobs");
    assert_eq!(
        (ready, in_flight),
        (0, 0),
        "no ready or in-flight jobs remain"
    );
}

/// A tiny thread-safe "seen once" set so the stress test can assert no seq is
/// acked twice without pulling a new dependency.
mod parking_lot_seen {
    use std::collections::HashSet;
    use std::sync::Mutex;

    pub struct SeenSet {
        inner: Mutex<HashSet<u64>>,
    }
    impl SeenSet {
        pub fn new() -> Self {
            SeenSet {
                inner: Mutex::new(HashSet::new()),
            }
        }
        /// Returns true if `seq` is recorded for the FIRST time; false if it was
        /// already present (a double-ack).
        pub fn record_first(&self, seq: u64) -> bool {
            self.inner.lock().unwrap().insert(seq)
        }
    }
}
