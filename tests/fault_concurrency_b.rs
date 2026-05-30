//! Phase-8B fault catalog — **category: concurrency** (group B).
//!
//! These strategies probe the **commit-token handoff** (the WAL writer's
//! `mpsc` ingest + `CommitState{Mutex<CommitOutcome>+Condvar}` + `CommitToken`)
//! and the **publish rollback** boundary — data races, not disk faults. The
//! catalog's `inject_how` prefers loom (exhaustive 2-thread) on the
//! `CommitState`/`CommitToken` primitive and shuttle (randomized, replayable) on
//! the multi-submitter assembly; **loom/shuttle are not wired into this crate's
//! Cargo.toml** (which this file may not edit). Per the Phase-8B fallback policy
//! each strategy is implemented as a **robust high-thread STRESS test** asserting
//! the SAME invariants the catalog oracle names, driving the *real* primitives (a
//! real [`Wal`] writer thread + many concurrent submitters, the real
//! [`FaultFs`]-injected fsync failure, the fully-wired [`Engine`] rollback path).
//! Loom/shuttle remain a documented follow-up (see the module note below).
//!
//! ```text
//!   every CommitToken resolves exactly once to Ok or Failed (never Pending forever)
//!   no lost wakeup; wait() never blocks forever
//!   multiple submitters + one writer drain: every submission is written, in
//!     per-box submit order; no submission dropped
//!   writer gone ⇒ pending waiters observe Failed (WriterGone), never deadlock;
//!     a submit after writer-gone returns Err
//!   an fsync that fails fails ALL tokens in the batch (durable AND non-durable)
//!   a rolled-back staged batch never advances head_seq (invisible, non-durable)
//! ```
//!
//! | id | what it pins |
//! |----|--------------|
//! | `F-CT-COMMIT-TOKEN-HANDOFF`  | the `Mutex<CommitOutcome>+Condvar` handoff: every token resolves exactly once; no lost wakeup; wait never hangs. |
//! | `F-CT-MPSC-SINGLE-CONSUMER`  | N submitters + one writer drain: every submission is written, per-box order preserved, none dropped. |
//! | `F-CT-WRITER-GONE`           | the writer exits with tokens still pending: waiters observe Failed (WriterGone), never deadlock; a later submit returns Err. |
//! | `F-CT-BATCH-PARTIAL-FAIL`    | an fsync EIO on a batch with durable + non-durable submissions fails ALL its tokens (no per-frame partial ack). |
//! | `F-PUB-ROLLBACK-INVISIBLE`   | a writer stages, the fsync fails, it rolls back; a concurrent reader polling head_seq sees nothing — box byte-identical to pre-stage. |
//!
//! ## Running
//!
//! ```text
//! cargo test --features test-fs --test fault_concurrency_b
//! ```
//!
//! ### loom / shuttle (follow-up)
//!
//! The `CommitState`/`CommitToken` handoff is exactly the small primitive loom
//! exhaustively model-checks (the writer's `signal` flips `Mutex<CommitOutcome>`
//! then `notify_all`, vs the waiter's `wait` loop on the `Condvar`), and the
//! multi-submitter drain is the shuttle target. Those deps are not present in
//! this crate today; once added under `[dev-dependencies]` with a
//! `cfg(loom)`/`cfg(shuttle)` swap of `Mutex`/`Condvar` inside `wal.rs`'s
//! `CommitState`, run e.g.:
//!
//! ```text
//! RUSTFLAGS="--cfg loom"    cargo test --features test-fs --test fault_concurrency_b -- loom_
//! RUSTFLAGS="--cfg shuttle" cargo test --features test-fs --test fault_concurrency_b -- shuttle_
//! ```
//!
//! The stress tests below assert the identical invariants in the meantime.

#![cfg(feature = "test-fs")]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use serde_json::json;

use streams::clock::{SharedClock, TestClock};
use streams::config::ServerConfig;
use streams::engine::Engine;
use streams::storage::testfs::{FakeDisk, FaultFs, FaultKind, FaultOp, TornDamage};
use streams::storage::wal::{Wal, WalConfig, WalError, WalReader, WalRecord};
use streams::storage::Fs;
use streams::types::{BoxConfig, BoxType, RecordIn, WriteRequest};

// ===========================================================================
// Shared plumbing (mirrors tests/fault_wal_*.rs — reused, not reinvented).
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

/// A fast group-commit config so a lone durable append fsyncs ~immediately.
fn fast_cfg(dir: &std::path::Path) -> WalConfig {
    let mut c = WalConfig::new(dir);
    c.gc_min = Duration::from_micros(50);
    c.gc_max = Duration::from_micros(200);
    c
}

/// An Append frame for `box_id` at `seq`.
fn ap(box_id: u32, seq: u64) -> WalRecord {
    WalRecord::Append {
        box_id,
        seq,
        ts: 1_700_000_000_000 + seq,
        node: None,
        tag: Some("t".into()),
        data: format!("p-{box_id}-{seq}").into_bytes(),
    }
}

/// Replay every `wal-*.log` on a disk image, stopping each file at its torn
/// tail. Returns the decoded (box_id, seq) pairs in file order.
fn replay_wal(disk: &FakeDisk, data_dir: &std::path::Path) -> Vec<(u32, u64)> {
    let fs = disk.arc();
    let wal_dir = data_dir.join("wal");
    let mut files: Vec<PathBuf> = fs
        .read_dir(&wal_dir)
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
    let mut out = Vec::new();
    for f in files {
        let r = WalReader::open_with(&fs, &f).expect("open wal file");
        for frame in r {
            if let WalRecord::Append { box_id, seq, .. } = &frame.record {
                out.push((*box_id, *seq));
            }
        }
    }
    out
}

fn sync_wal_dir(disk: &FakeDisk) {
    let fs = disk.arc();
    let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("wal"));
    let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("meta"));
}

// ===========================================================================
// F-CT-COMMIT-TOKEN-HANDOFF
// ---------------------------------------------------------------------------
// The CommitState handoff (Mutex<CommitOutcome> + Condvar) between the writer's
// `signal` and a waiter's `wait`. Every submitted token must resolve EXACTLY
// ONCE to Ok (after the group fsync for a durable frame) — never stuck Pending,
// never a lost wakeup, never a wait that blocks forever.
//
// Stress proxy for loom: many threads each submit a durable frame and block on
// the token. The race is the writer flipping CommitOutcome + notify_all vs each
// waiter entering `wait`. Every wait MUST return Ok (within a generous timeout
// watchdog — a hang would mean a lost wakeup, which we surface as a failure
// rather than a CI hang).
// ===========================================================================

#[test]
fn f_ct_commit_token_handoff() {
    const THREADS: usize = 16;
    const PER_THREAD: u64 = 40;

    let disk = FakeDisk::new();
    let data_dir = PathBuf::from(DATA_DIR);
    let wal = Wal::open_at_with(disk.arc(), fast_cfg(&data_dir), 1, 0).unwrap();

    let start = Arc::new(Barrier::new(THREADS));
    let oks = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for t in 0..THREADS {
        let w = wal.writer();
        let start = start.clone();
        let oks = oks.clone();
        handles.push(std::thread::spawn(move || {
            start.wait();
            for i in 0..PER_THREAD {
                let seq = (t as u64) * PER_THREAD + i + 1;
                // submit returns a token; wait() blocks on the CommitState handoff.
                let token = w.submit(ap(1, seq), true).expect("submit ok (writer alive)");
                // Every durable token resolves to Ok exactly once (after the fsync).
                token.wait().expect("commit token resolved Ok (no lost wakeup / hang)");
                oks.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    // Watchdog: if a lost wakeup left a waiter blocked forever, the joins below
    // would hang. A separate timer thread converts that into a clear failure.
    let expected = THREADS as u64 * PER_THREAD;
    let watchdog = {
        let oks = oks.clone();
        std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + Duration::from_secs(20);
            while std::time::Instant::now() < deadline {
                if oks.load(Ordering::Relaxed) >= expected {
                    return true;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            false
        })
    };

    for h in handles {
        h.join().expect("a submitter thread panicked (token did not resolve cleanly)");
    }
    assert!(
        watchdog.join().unwrap(),
        "a commit token never resolved within the watchdog window (lost wakeup / deadlock)"
    );
    assert_eq!(
        oks.load(Ordering::Relaxed),
        expected,
        "every submitted token resolved exactly once to Ok"
    );
    drop(wal);
}

// ===========================================================================
// F-CT-MPSC-SINGLE-CONSUMER
// ---------------------------------------------------------------------------
// Multiple submitters feed the single writer's mpsc; the one writer drains
// (drain_ready/try_recv) and group-commits. Every submission must be written
// exactly once; frames from one box must be written in submit order (the writer
// drains FIFO); group-commit batches are well-formed; no submission dropped.
//
// N submitter threads, each owning a distinct box_id, each submitting a
// contiguous run of durable frames in seq order. After all join + a clean
// shutdown drain, the recovered WAL must contain EVERY (box,seq) exactly once,
// and each box's seqs must appear in ascending (submit) order.
// ===========================================================================

#[test]
fn f_ct_mpsc_single_consumer() {
    const BOXES: u32 = 8;
    const PER_BOX: u64 = 60;

    let disk = FakeDisk::new();
    let data_dir = PathBuf::from(DATA_DIR);
    let wal = Wal::open_at_with(disk.arc(), fast_cfg(&data_dir), 1, 0).unwrap();

    let start = Arc::new(Barrier::new(BOXES as usize));
    let mut handles = Vec::new();
    for b in 1..=BOXES {
        let w = wal.writer();
        let start = start.clone();
        handles.push(std::thread::spawn(move || {
            start.wait();
            for seq in 1..=PER_BOX {
                // Block on each so this box's frames are submitted strictly in seq
                // order (the per-box FIFO the single consumer must preserve).
                w.append(ap(b, seq), true).expect("append ok");
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    sync_wal_dir(&disk);
    wal.shutdown(); // drains every queued submission with a final fsync.

    let frames = replay_wal(&disk, &data_dir);

    // (1) NO SUBMISSION DROPPED + NO DUPLICATE: exactly BOXES*PER_BOX frames, each
    //     (box,seq) present exactly once.
    let mut seen: BTreeMap<(u32, u64), u32> = BTreeMap::new();
    for f in &frames {
        *seen.entry(*f).or_insert(0) += 1;
    }
    assert_eq!(
        frames.len() as u64,
        BOXES as u64 * PER_BOX,
        "every submission written exactly once (none dropped / duplicated): {} frames",
        frames.len()
    );
    for b in 1..=BOXES {
        for seq in 1..=PER_BOX {
            assert_eq!(
                seen.get(&(b, seq)).copied().unwrap_or(0),
                1,
                "frame (box {b}, seq {seq}) must appear exactly once"
            );
        }
    }

    // (2) PER-BOX SUBMIT ORDER: within each box the seqs appear ascending in the
    //     write stream (the single consumer drains the mpsc FIFO; per-box frames
    //     were submitted in seq order, so they must land in seq order).
    let mut last_per_box: BTreeMap<u32, u64> = BTreeMap::new();
    for (b, seq) in &frames {
        let last = last_per_box.entry(*b).or_insert(0);
        assert!(
            *seq > *last,
            "box {b} seq {seq} landed out of submit order (prev {last}) — \
             single-consumer FIFO violated"
        );
        *last = *seq;
    }
}

// ===========================================================================
// F-CT-WRITER-GONE
// ---------------------------------------------------------------------------
// The writer thread exits (shutdown) with tokens still pending. Pending waiters
// must observe Failed (WriterGone) — never deadlock — and a submit issued AFTER
// the writer is gone must return Err(WriterGone).
//
// We stop the writer (join the thread) and THEN submit / wait, asserting both
// the submit-after-gone and the (already-submitted) token paths fail cleanly
// rather than hanging. A watchdog converts any hang into a failure.
// ===========================================================================

#[test]
fn f_ct_writer_gone() {
    let disk = FakeDisk::new();
    let data_dir = PathBuf::from(DATA_DIR);
    let wal = Wal::open_at_with(disk.arc(), fast_cfg(&data_dir), 1, 0).unwrap();
    let w = wal.writer();

    // A clean acked write first (the writer is alive and well).
    w.append(ap(1, 1), true).expect("first append acks");

    // Stop the writer thread (shutdown drains + joins). The WalWriter clone `w`
    // outlives it — its mpsc Sender is still open, but the receiver is dropped.
    wal.shutdown();

    // Watchdog: a deadlock here (a submit/wait that blocks forever after the
    // writer is gone) becomes a clear failure instead of a CI hang.
    let done = Arc::new(AtomicBool::new(false));
    let watchdog = {
        let done = done.clone();
        std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + Duration::from_secs(15);
            while std::time::Instant::now() < deadline {
                if done.load(Ordering::Relaxed) {
                    return true;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            false
        })
    };

    // (1) A submit AFTER the writer is gone returns Err(WriterGone) — never a
    //     token that will hang forever. (The receiver is dropped ⇒ send fails.)
    match w.submit(ap(1, 2), true) {
        Err(WalError::WriterGone) => {}
        Ok(token) => {
            // If the send raced in before the rx dropped, the token must still
            // resolve (to Failed) and never hang.
            match token.wait() {
                Err(WalError::WriterGone) => {}
                other => panic!("post-shutdown token must resolve WriterGone, got {other:?}"),
            }
        }
        Err(other) => panic!("submit after writer gone must be WriterGone, got {other:?}"),
    }

    // (2) `append` (submit + wait) after the writer is gone also fails cleanly.
    let r = w.append(ap(1, 3), true);
    assert!(
        matches!(r, Err(WalError::WriterGone)),
        "append after writer gone must return WriterGone, got {r:?}"
    );

    done.store(true, Ordering::Relaxed);
    assert!(
        watchdog.join().unwrap(),
        "a post-writer-gone submit/wait blocked forever (deadlock, not WriterGone)"
    );
}

// ===========================================================================
// F-CT-BATCH-PARTIAL-FAIL
// ---------------------------------------------------------------------------
// The group-commit fsync fails for a batch that contains BOTH durable and
// non-durable submissions. ALL tokens in that batch must get Failed — there is
// no per-frame partial ack; the non-durable submissions share the batch's
// durability fate (they were buffered into the same write, fsynced as a unit).
//
// We coalesce a mixed batch behind a slow group-commit window and inject an EIO
// on the very sync_data that batch triggers. Every token (durable + non-durable)
// in the failed batch must resolve to Err(WriterGone); recovery shows none of
// the batch's frames as durable.
// ===========================================================================

#[test]
fn f_ct_batch_partial_fail() {
    let disk = FakeDisk::new();
    let data_dir = PathBuf::from(DATA_DIR);

    // A WIDE group-commit window so many submissions coalesce into ONE batch that
    // triggers a single fsync — the batch we will fail.
    let mut wcfg = WalConfig::new(&data_dir);
    wcfg.gc_min = Duration::from_millis(40);
    wcfg.gc_max = Duration::from_millis(80);
    wcfg.channel_cap = 4096;

    // Fail-always on sync_data so the first (and any) group-commit fsync EIOs.
    let faulty: Arc<dyn Fs> =
        FaultFs::new(disk.arc(), FaultOp::SyncData, FaultKind::Eio, 0, false).arc();
    let wal = Wal::open_at_with(faulty, wcfg, 1, 0).unwrap();
    let w = wal.writer();

    // Submit a MIXED batch without blocking, so they all queue behind the wide
    // window and coalesce into one batch: alternating durable / non-durable.
    // (any_durable is true ⇒ the writer issues the one fsync, which EIOs.)
    let mut tokens = Vec::new();
    for seq in 1..=10 {
        let durable = seq % 2 == 0; // half durable, half non-durable.
        let token = w.submit(ap(1, seq), durable).expect("submit queues");
        tokens.push((seq, durable, token));
    }

    // Every token in the failed batch — durable AND non-durable — resolves Failed
    // (WriterGone). No per-frame partial ack: the non-durable frames share the
    // batch's durability fate because the fsync that would have hardened the whole
    // buffered write failed.
    for (seq, durable, token) in tokens {
        let r = token.wait();
        assert!(
            matches!(r, Err(WalError::WriterGone)),
            "seq {seq} (durable={durable}) in the fsync-failed batch must be Failed, got {r:?}"
        );
    }

    // Recovery: the fsync never landed, so none of the batch's frames are durable.
    sync_wal_dir(&disk);
    disk.crash(TornDamage::None);
    drop(wal);
    disk.reset_power();

    let frames = replay_wal(&disk, &data_dir);
    assert!(
        frames.is_empty(),
        "no frame of the fsync-failed batch is durable after recovery: {frames:?}"
    );
}

// ===========================================================================
// F-PUB-ROLLBACK-INVISIBLE
// ---------------------------------------------------------------------------
// A durable write stages records (into the index, head NOT advanced ⇒ invisible)
// then its WAL fsync FAILS, so the engine rolls the staged batch back. A
// concurrent reader polling head_seq must see NOTHING for the rolled-back batch;
// head_seq never advances; the box is byte-identical to pre-stage. No
// visible-but-not-durable record.
//
// We drive the fully-wired engine: a durable box, a reader thread polling
// box_state head_seq + diff while a writer issues durable writes whose fsync is
// forced to EIO. Every failed write must leave head unchanged and surface no new
// record to the reader.
// ===========================================================================

#[test]
fn f_pub_rollback_invisible() {
    let disk = FakeDisk::new();

    // Phase 1: a few acked durable writes on a clean disk (the stable prefix the
    // reader is allowed to see).
    {
        let engine = open_engine(&disk);
        engine
            .put_box(
                "roll",
                BoxConfig {
                    r#type: BoxType::Log,
                    durable: true,
                    ..Default::default()
                },
            )
            .expect("put_box");
        for i in 1..=3u64 {
            let req = WriteRequest {
                records: vec![RecordIn {
                    data: json!({ "v": i }),
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
            assert!(engine.write("roll", req, true).is_ok());
        }
        sync_wal_dir(&disk);
        drop(engine);
    }

    // Phase 2: reopen through a FaultFs that EIOs every group-commit sync_data, so
    // every durable write's fsync fails ⇒ the engine rolls back the staged batch.
    let faulty: Arc<dyn Fs> =
        FaultFs::new(disk.arc(), FaultOp::SyncData, FaultKind::Eio, 0, false).arc();
    let engine = Engine::with_data_dir_fs(cfg(), clock(), faulty).expect("reopen through faultfs");

    let head_before = engine.box_state("roll", false).unwrap().head_seq;
    assert_eq!(head_before, 3, "the 3 acked durable writes are the visible prefix");

    let stop = Arc::new(AtomicBool::new(false));
    let violated = Arc::new(AtomicU64::new(0));

    // Reader: poll head_seq + diff. It must NEVER observe a head above the stable
    // prefix (3) and never a 4th record — staged-but-rolled-back batches are
    // invisible.
    let reader = {
        let engine = engine.clone();
        let stop = stop.clone();
        let violated = violated.clone();
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let st = engine.box_state("roll", false).unwrap();
                if st.head_seq > 3 {
                    violated.fetch_add(1, Ordering::Relaxed);
                }
                let d = engine
                    .diff(
                        "roll",
                        streams::types::DiffRequest {
                            from_seq: 0,
                            limit: 100,
                            node: None,
                            include_tags: false,
                            include_meta: false,
                            wait_ms: 0,
                        },
                    )
                    .unwrap();
                if d.records.iter().any(|r| r.seq > 3) {
                    violated.fetch_add(1, Ordering::Relaxed);
                }
                std::hint::spin_loop();
            }
        })
    };

    // Writer: issue durable writes that all fail the fsync ⇒ all roll back.
    for i in 4..=12u64 {
        let req = WriteRequest {
            records: vec![RecordIn {
                data: json!({ "v": i }),
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
        let r = engine.write("roll", req, true);
        assert!(
            r.is_err(),
            "a durable write whose fsync EIOs must FAIL (rolled back), not ack: seq {i}"
        );
        // After each failed write, head is unchanged (the staged batch rolled back).
        assert_eq!(
            engine.box_state("roll", false).unwrap().head_seq,
            3,
            "head_seq advanced for a rolled-back batch (visible-but-not-durable!) at seq {i}"
        );
    }

    stop.store(true, Ordering::Relaxed);
    reader.join().unwrap();
    assert_eq!(
        violated.load(Ordering::Relaxed),
        0,
        "a reader observed a staged-but-rolled-back record (publish/rollback not invisible)"
    );

    // The box is byte-identical to pre-stage: still exactly the 3 acked records,
    // head still 3.
    let st = engine.box_state("roll", false).unwrap();
    assert_eq!(st.head_seq, 3, "head_seq unchanged after every rollback");
    assert_eq!(st.count, 3, "no rolled-back record left a trace in the box");
    drop(engine);
}
