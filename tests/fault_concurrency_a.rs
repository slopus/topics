//! Phase-8B fault catalog — **category: concurrency** (group A).
//!
//! These strategies probe **data races**, not disk faults: the publish/visibility
//! boundary (`head_seq` Acquire/Release vs a reader), the idempotency-key dedupe
//! lock, the segment-writer seal-vs-append mutex, a write racing a snapshot
//! capture, and the single-writer WAL-dir assumption. The catalog's
//! `inject_how` prefers loom (exhaustive) / shuttle (randomized) on the small
//! primitives, but **loom/shuttle are not wired into this crate's Cargo.toml**
//! (which this file may not edit). Per the Phase-8B fallback policy, each
//! strategy is therefore implemented as a **robust high-thread STRESS test**
//! (many concurrent actors, fixed iteration budgets) asserting the SAME
//! invariants the catalog oracle names, driving the *real* primitives
//! ([`TopicState::stage_append`]/`publish_staged`/`rollback_staged`, the real
//! [`SegmentWriter`] mutex, the fully-wired [`Engine`] dedupe path, a real
//! [`Wal`] over a [`FakeDisk`]). Loom/shuttle remain a documented follow-up
//! (see the module note below).
//!
//! ```text
//!   a reader seeing head==N also sees slots [base..=N]   (publish atomicity)
//!   a staged-but-unpublished batch is invisible
//!   at most one set of seqs is published per idempotency key in-window
//!   sealed segments are dense / gapless / contiguous (no record sealed twice)
//!   a racing write either replays from the WAL or is seq-skipped — never a gap
//! ```
//!
//! | id | what it pins |
//! |----|--------------|
//! | `F-WAL-DOUBLE-WRITER-FENCING`     | two writers on one WAL image: frames carry no cross-writer corruption; the single-writer assumption is a documented gap (no fencing token). |
//! | `F-SNAP-RACE-WRITE-DURING-CAPTURE`| a durable write commits during a snapshot capture: the racing write replays from the WAL (offset >= checkpoint) and is seq-skipped if already materialized — no acked write falls in the gap. |
//! | `F-SEG-SEAL-RACE-APPEND`          | concurrent appends race `seal_active` under the writer mutex: sealed segments stay dense/gapless/contiguous, no record sealed twice or skipped. |
//! | `F-IDEMP-CONCURRENT-RETRY`        | two concurrent writes with one idempotency key race the dedupe lock: exactly one set of seqs is published; the loser dedupes to the same seqs. |
//! | `F-PUB-READER-OBSERVES-PARTIAL`   | a reader gating on `head_seq` (Acquire) that observes head==N always finds slots [base..=N] populated; a staged-unpublished batch is invisible; never a torn/future seq. |
//!
//! ## Running
//!
//! ```text
//! cargo test --features test-fs --test fault_concurrency_a
//! ```
//!
//! ### loom / shuttle (follow-up)
//!
//! The `CommitState`/`head_seq` primitives are model-checkable with loom
//! (exhaustive 2-thread) and the multi-actor assembly with shuttle (randomized,
//! replayable). Those deps are not present in this crate today; once added under
//! a `[dev-dependencies]` + a `cfg(loom)`/`cfg(shuttle)` sync-module swap on the
//! `CommitState` + `head_seq` publish, run e.g.:
//!
//! ```text
//! RUSTFLAGS="--cfg loom"    cargo test --features test-fs --test fault_concurrency_a -- loom_
//! RUSTFLAGS="--cfg shuttle" cargo test --features test-fs --test fault_concurrency_a -- shuttle_
//! ```
//!
//! The stress tests below assert the identical invariants in the meantime.

#![cfg(feature = "test-fs")]
#![allow(
    clippy::ptr_arg,
    clippy::manual_clamp,
    clippy::unusual_byte_groupings,
    clippy::doc_lazy_continuation
)]

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::Duration;

use serde_json::json;

use topics::clock::{SharedClock, TestClock};
use topics::config::{SegmentConfig, ServerConfig};
use topics::engine::segwriter::SegmentWriter;
use topics::engine::topic_state::{PublishPermit, StoredRecord, TopicState};
use topics::engine::Engine;
use topics::storage::testfs::{FakeDisk, TornDamage};
use topics::storage::wal::{Wal, WalConfig, WalReader, WalRecord};
use topics::storage::{LocalSegmentStore, TopicTier};
use topics::types::{RecordIn, TopicConfig, TopicType, WriteRequest};

// ===========================================================================
// Shared plumbing (mirrors tests/crash_oracle.rs + tests/fault_wal_*.rs).
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

/// An Append frame for `topic_id` at `seq`.
fn ap(topic_id: u64, seq: u64) -> WalRecord {
    WalRecord::Append {
        topic_id,
        seq,
        ts: 1_700_000_000_000 + seq,
        node: None,
        tag: Some("t".into()),
        data: format!("payload-{seq}").into_bytes(),
    }
}

/// A minimal stored record carrying a tag (so staged/published records exercise
/// the tag index the rollback path must also unwind).
fn rec(data: &str, tag: Option<&str>) -> StoredRecord {
    StoredRecord {
        ts: 1_700_000_000_000,
        node: None,
        tag: tag.map(str::to_string),
        data: json!({ "v": data }),
        meta: None,
        bytes: data.len() as u64,
        deleted: false,
        payload_resident: true,
        hops: 0,
    }
}

/// Build a fresh standalone `TopicState` (durable=false; only the publish/stage
/// visibility primitive matters here, not the WAL).
fn fresh_topic(name: &str) -> Arc<TopicState> {
    Arc::new(TopicState::new(
        name.to_string(),
        1,
        TopicConfig {
            r#type: TopicType::Log,
            durable: false,
            ..Default::default()
        },
        1,
        1,
    ))
}

/// Build a real `SegmentWriter` over a temp-dir HOT store, sealing every
/// `max_events` records (so concurrent appends repeatedly cross a seal
/// boundary).
fn seg_writer(max_events: u64) -> (SegmentWriter, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let hot = Box::new(LocalSegmentStore::open(dir.path()).unwrap());
    let tier = Arc::new(TopicTier::new(hot, None));
    let cfg = SegmentConfig {
        max_events,
        max_bytes: 0,
        max_age_ms: 0,
        ..SegmentConfig::default()
    };
    (SegmentWriter::new(tier, cfg, clock()), dir)
}

// ===========================================================================
// F-PUB-READER-OBSERVES-PARTIAL
// ---------------------------------------------------------------------------
// A reader gating on head_seq (Acquire) that observes head==N MUST always find
// slots [base..=N] populated: publish stages the records into the index BEFORE
// the Release store of head_seq, so an Acquire reader that sees the higher head
// also sees every slot. A staged-but-unpublished batch is INVISIBLE (head not
// advanced). Never a torn / future seq.
//
// Stress proxy for loom: a publisher thread stages+publishes batches while a
// reader thread spins on head_seq(); every time it observes a head it asserts
// all slots up to that head are present and carry their genuine payload.
// ===========================================================================

#[test]
fn f_pub_reader_observes_partial() {
    const BATCHES: u64 = 400;
    const BATCH_SZ: u64 = 4;

    let b = fresh_topic("pub");
    let done = Arc::new(AtomicBool::new(false));
    let start = Arc::new(Barrier::new(2));

    // Reader: spin on head_seq (Acquire). Whenever it observes head==N, every
    // slot [base..=N] must be present and carry the genuine payload "payload-<seq>".
    let reader = {
        let b = b.clone();
        let done = done.clone();
        let start = start.clone();
        std::thread::spawn(move || {
            start.wait();
            let mut max_seen = 0u64;
            let mut observations = 0u64;
            while !done.load(Ordering::Relaxed) || b.head_seq() > max_seen {
                let head = b.head_seq(); // Acquire load.
                if head == 0 {
                    std::hint::spin_loop();
                    continue;
                }
                // Read the index AFTER the Acquire load: the Release store in
                // publish_staged happens-after the records were pushed, so an
                // Acquire reader seeing `head` must see every slot up to it.
                let index = b.index.read();
                let base = index.base_seq;
                for seq in base..=head {
                    let slot = index.get(seq).unwrap_or_else(|| {
                        panic!(
                            "reader observed head={head} but slot {seq} is MISSING \
                             (publish atomicity violated: head Released before slots)"
                        )
                    });
                    // No torn / future / garbage payload: it is exactly what the
                    // publisher staged for this seq.
                    let v = slot.data.get("v").and_then(|v| v.as_str()).unwrap_or("");
                    assert_eq!(
                        v,
                        format!("payload-{seq}"),
                        "reader saw a torn/foreign payload at seq {seq} (head={head})"
                    );
                }
                drop(index);
                if head > max_seen {
                    max_seen = head;
                }
                observations += 1;
                if observations > 50_000_000 {
                    break; // safety valve; never expected to trip.
                }
            }
            max_seen
        })
    };

    // Publisher: stage a contiguous batch (records land in the index, head NOT
    // advanced ⇒ invisible) then publish (Release head). The reader must never
    // see a partially-populated head.
    let publisher = {
        let b = b.clone();
        let done = done.clone();
        let start = start.clone();
        std::thread::spawn(move || {
            start.wait();
            for batch in 0..BATCHES {
                let first = batch * BATCH_SZ + 1;
                let recs: Vec<StoredRecord> = (0..BATCH_SZ)
                    .map(|i| {
                        let seq = first + i;
                        rec(&format!("payload-{seq}"), Some("t"))
                    })
                    .collect();
                let staged = b.stage_append(recs);
                // While staged-but-unpublished, head must not have advanced past the
                // previous batch end (the new records are invisible).
                assert!(
                    b.head_seq() < first,
                    "staged batch leaked visibility (head advanced before publish)"
                );
                b.publish_staged(staged, 1_700_000_000_000, PublishPermit::resident());
            }
            done.store(true, Ordering::Relaxed);
        })
    };

    publisher.join().unwrap();
    let max_seen = reader.join().unwrap();
    assert_eq!(b.head_seq(), BATCHES * BATCH_SZ, "all batches published");
    assert!(max_seen > 0, "reader observed progress");
}

// ===========================================================================
// F-IDEMP-CONCURRENT-RETRY
// ---------------------------------------------------------------------------
// Two concurrent durable writes carrying the SAME idempotency_key race the topic
// dedupe write lock. At most one set of seqs is assigned-and-published for the
// key in-window; the loser returns the SAME seqs (deduped:true). Never two
// distinct live batches for one key.
//
// Many concurrent threads per key, many keys, asserting per key: all winners +
// dedupers share one seq set, and the topic's head reflects exactly one append
// per key (no double-publish).
//
// BUG (real, repro below): the dedupe map is consulted under `b.dedupe.write()`
// but the lock is RELEASED before the WAL append, and the reservation
// (`dedupe.insert`) only happens AFTER publish (engine/mod.rs ~L1124 check vs
// ~L1265 insert). Two concurrent writes with one key both miss the entry, both
// append, both publish DISTINCT live batches with DISTINCT seqs — violating the
// F-IDEMP-CONCURRENT-RETRY oracle ("at most one set of seqs per key in-window;
// never two distinct live batches"). The check-then-act window is unsynchronized.
// Correct fix: reserve the key (a placeholder/pending entry) under the dedupe
// lock at check time, or hold a per-key gate across stage→publish, so the loser
// dedupes to the winner's seqs. Test asserts the CORRECT behavior; ignored until
// the Fix phase closes the race.
// ===========================================================================

#[test]
fn f_idemp_concurrent_retry() {
    const KEYS: usize = 40;
    const RACERS: usize = 6;

    let disk = FakeDisk::new();
    let engine = open_engine(&disk);
    // A durable topic so the write path takes the WAL-first reservation + dedupe.
    engine
        .put_topic(
            "idem",
            TopicConfig {
                r#type: TopicType::Log,
                durable: true,
                ..Default::default()
            },
        )
        .expect("put_topic");

    let start = Arc::new(Barrier::new(KEYS * RACERS));
    let mut handles = Vec::new();
    for k in 0..KEYS {
        for _ in 0..RACERS {
            let engine = engine.clone();
            let start = start.clone();
            handles.push(std::thread::spawn(move || {
                let key = format!("key-{k}");
                let req = WriteRequest {
                    records: vec![RecordIn {
                        data: json!({ "v": format!("v-{k}") }),
                        tag: None,
                        node: None,
                        meta: None,
                    }],
                    node: None,
                    idempotency_key: Some(key),
                    create: Some(true),
                    config: None,
                    disable_backpressure: true,
                };
                start.wait();
                let resp = engine.write("idem", req, true).expect("write ok");
                (
                    k,
                    resp.seqs.unwrap_or_else(|| vec![resp.last_seq]),
                    resp.deduped,
                )
            }));
        }
    }

    // Collect per-key seq sets across all racers.
    let mut per_key: std::collections::HashMap<usize, Vec<Vec<u64>>> = Default::default();
    for h in handles {
        let (k, seqs, _deduped) = h.join().unwrap();
        per_key.entry(k).or_default().push(seqs);
    }

    // INVARIANT: for each key, every racer (winner + losers) returns the SAME
    // single seq set — exactly one live batch per key in-window.
    let mut all_winning_seqs: BTreeSet<u64> = BTreeSet::new();
    for (k, sets) in &per_key {
        let first = &sets[0];
        for s in sets {
            assert_eq!(
                s, first,
                "key {k}: concurrent retries returned DIFFERENT seqs {s:?} vs {first:?} \
                 (two distinct live batches for one idempotency key)"
            );
        }
        assert_eq!(first.len(), 1, "key {k}: one-record write ⇒ one seq");
        // No seq is shared across two different keys (each key's batch is distinct,
        // contiguous, live).
        for &seq in first {
            assert!(
                all_winning_seqs.insert(seq),
                "seq {seq} assigned to two different idempotency keys (duplicate live seq)"
            );
        }
    }

    // The topic published exactly KEYS records (one per key), head == KEYS.
    let st = engine.topic_state("idem", false).unwrap();
    assert_eq!(
        st.head_seq, KEYS as u64,
        "exactly one append published per key (no double-publish under the race)"
    );
    assert_eq!(st.count, KEYS as u64, "no fabricated or lost record");
    drop(engine);
}

// ===========================================================================
// F-SEG-SEAL-RACE-APPEND
// ---------------------------------------------------------------------------
// Concurrent appends race seal_active under the SegmentWriter mutex (the real
// `Mutex<SegmentWriter>` the engine wraps each topic's writer in). Sealed segments
// must be dense/gapless/contiguous (the SegmentBuilder debug_assert holds in a
// debug test build); no record sealed twice or skipped; the sealed_seqs returned
// across all appends cover exactly the seqs the seal boundary crossed.
//
// The writer assigns no seqs itself (the topic does, in order, under append_lock),
// so we mirror production: a single monotonic seq source feeds N appender threads
// that each lock the writer to append. Interleaving seal + append under the lock
// must never break the contiguity invariant.
// ===========================================================================

#[test]
fn f_seg_seal_race_append() {
    const TOTAL: u64 = 600;
    const THREADS: usize = 6;
    const MAX_EVENTS: u64 = 5; // seal every 5 records ⇒ many boundaries crossed.

    let (writer, _dir) = seg_writer(MAX_EVENTS);
    let writer = Arc::new(Mutex::new(writer));
    // The single ordered seq source (production: assigned under the topic append
    // lock; here a fetch_add hands each append its contiguous seq).
    let next_seq = Arc::new(AtomicU64::new(1));
    // Every seq reported sealed by any thread, collected to prove no double/skip.
    let sealed_all = Arc::new(Mutex::new(Vec::<u64>::new()));
    let start = Arc::new(Barrier::new(THREADS));

    let mut handles = Vec::new();
    for _ in 0..THREADS {
        let writer = writer.clone();
        let next_seq = next_seq.clone();
        let sealed_all = sealed_all.clone();
        let start = start.clone();
        handles.push(std::thread::spawn(move || {
            start.wait();
            loop {
                // Production mirrors this exactly: the topic assigns the next
                // contiguous seq AND enqueues/materializes it under one critical
                // section (the `append_lock`), so a topic's segment appends arrive in
                // seq order. Here the writer lock IS that critical section: take it,
                // claim the next seq, append — so seq order == append order even
                // though many threads contend for the lock. A `debug_assert_eq!` in
                // SegmentBuilder::push fires on any non-contiguous seal/append.
                let (mut w, seq) = {
                    let w = writer.lock().unwrap();
                    let seq = next_seq.fetch_add(1, Ordering::SeqCst);
                    (w, seq)
                };
                if seq > TOTAL {
                    break;
                }
                let sealed = w.append_record(
                    seq,
                    1_700_000_000_000,
                    None,
                    Some("t"),
                    &json!({ "s": seq }),
                    &None,
                );
                drop(w);
                if !sealed.is_empty() {
                    sealed_all.lock().unwrap().extend(sealed);
                }
            }
        }));
    }
    for h in handles {
        h.join()
            .expect("no appender panicked (contiguity invariant held)");
    }

    // Flush the final active segment so every appended seq is materialized.
    let final_sealed = writer.lock().unwrap().flush();
    sealed_all.lock().unwrap().extend(final_sealed);

    // INVARIANT (no record sealed twice or skipped): the union of all sealed seqs
    // is exactly the dense set [1..=TOTAL], each seq exactly once.
    let mut sealed = sealed_all.lock().unwrap().clone();
    sealed.sort_unstable();
    let expected: Vec<u64> = (1..=TOTAL).collect();
    assert_eq!(
        sealed.len(),
        expected.len(),
        "a record was sealed twice or skipped under the seal/append race"
    );
    assert_eq!(
        sealed, expected,
        "sealed seqs must cover exactly [1..=TOTAL], no gap/dup"
    );

    // INVARIANT (dense/gapless/contiguous segments): the sealed segments' ranges
    // tile [1..=last] with no overlap and no gap (each segment is contiguous and
    // they abut). The active tail (if any) continues right after the last sealed.
    let w = writer.lock().unwrap();
    let mut segs = w.sealed_segments();
    segs.sort_by_key(|s| s.start_seq);
    let mut expect_next = 1u64;
    for s in &segs {
        assert_eq!(
            s.start_seq, expect_next,
            "sealed segment is not contiguous with the previous (gap/overlap): {segs:?}",
        );
        assert!(
            s.end_seq >= s.start_seq,
            "empty/inverted sealed segment range"
        );
        expect_next = s.end_seq + 1;
    }
    assert_eq!(
        expect_next,
        TOTAL + 1,
        "sealed segments tile exactly [1..=TOTAL]"
    );
}

// ===========================================================================
// F-SNAP-RACE-WRITE-DURING-CAPTURE
// ---------------------------------------------------------------------------
// A durable write commits BETWEEN a snapshot capture's position read and the topic
// materialization. The racing write has a WAL offset >= the recorded checkpoint
// ⇒ it is replayed on recovery; if it was also in the materialized snapshot set
// it is seq-skipped on replay (Append seq<=head). No acked write falls into the
// gap — recovery yields the full acked set, contiguous, no resurrection.
//
// We run a concurrent writer storm while repeatedly invoking the engine's real
// `write_snapshot()` (capture). After a crash + recovery, every acked durable
// write is present and the set is a dense prefix — proving capture racing live
// writes never loses or fabricates a record regardless of interleaving.
// ===========================================================================

#[test]
fn f_snap_race_write_during_capture() {
    const WRITERS: usize = 4;
    const PER_WRITER: u64 = 50;

    let disk = FakeDisk::new();
    let acked = Arc::new(Mutex::new(BTreeSet::<u64>::new()));
    {
        let engine = open_engine(&disk);
        engine
            .put_topic(
                "snaprace",
                TopicConfig {
                    r#type: TopicType::Log,
                    durable: true,
                    ..Default::default()
                },
            )
            .expect("put_topic");

        let start = Arc::new(Barrier::new(WRITERS + 1));
        let stop_cap = Arc::new(AtomicBool::new(false));

        // Capturer: hammer write_snapshot() concurrently with the write storm, so
        // a capture's position-read interleaves with in-flight durable commits.
        let capturer = {
            let engine = engine.clone();
            let start = start.clone();
            let stop_cap = stop_cap.clone();
            std::thread::spawn(move || {
                start.wait();
                let mut taken = 0u64;
                while !stop_cap.load(Ordering::Relaxed) {
                    let _ = engine.write_snapshot();
                    taken += 1;
                    std::thread::yield_now();
                }
                taken
            })
        };

        // Writer storm: each writer issues durable appends; an Ok means the fsync
        // returned ⇒ acked ⇒ must survive the crash regardless of any racing capture.
        let mut writers = Vec::new();
        for _ in 0..WRITERS {
            let engine = engine.clone();
            let start = start.clone();
            let acked = acked.clone();
            writers.push(std::thread::spawn(move || {
                start.wait();
                for _ in 0..PER_WRITER {
                    let req = WriteRequest {
                        records: vec![RecordIn {
                            data: json!({ "v": "x" }),
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
                    if let Ok(resp) = engine.write("snaprace", req, true) {
                        let seq = resp.last_seq;
                        acked.lock().unwrap().insert(seq);
                    }
                }
            }));
        }
        for w in writers {
            w.join().unwrap();
        }
        stop_cap.store(true, Ordering::Relaxed);
        capturer.join().unwrap();

        // Power loss: freeze BEFORE dropping so the writer's Drop drain cannot
        // harden anything new — only writes whose fsync already returned are acked.
        let fs = disk.arc();
        let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("wal"));
        let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("meta"));
        disk.crash(TornDamage::None);
        drop(engine);
    }
    disk.reset_power();

    // Recover and assert: every acked durable write survives (capture racing live
    // writes never dropped one), the survivor set is a dense prefix, no fabrication.
    let engine = open_engine(&disk);
    let st = engine.topic_state("snaprace", false).unwrap();
    let mut survivors = Vec::new();
    let mut from = 0u64;
    loop {
        let d = engine
            .diff(
                "snaprace",
                topics::types::DiffRequest {
                    from_seq: from,
                    limit: 1000,
                    node: None,
                    include_tags: false,
                    include_meta: false,
                    wait_ms: 0,
                    max_batch_bytes: 0,
                },
            )
            .unwrap();
        for r in &d.records {
            survivors.push(r.seq);
        }
        if d.caught_up || d.records.is_empty() {
            break;
        }
        from = d.next_from_seq;
    }
    let acked = acked.lock().unwrap();
    let max_acked = acked.iter().copied().max().unwrap_or(0);

    // Dense contiguous prefix [1..=k] — no gap, no fabrication.
    for (i, s) in survivors.iter().enumerate() {
        assert_eq!(
            *s,
            i as u64 + 1,
            "survivors must be a dense prefix: {survivors:?}"
        );
    }
    // Every acked durable write present (capture never lost an acked write to the
    // race): acked ⊆ survivors.
    let survivor_set: BTreeSet<u64> = survivors.iter().copied().collect();
    for &a in acked.iter() {
        assert!(
            survivor_set.contains(&a),
            "acked durable seq {a} LOST across a snapshot-capture race (survivors={survivors:?})"
        );
    }
    assert_eq!(
        st.head_seq, max_acked,
        "recovered head == highest acked seq"
    );
    drop(engine);
}

// ===========================================================================
// F-WAL-DOUBLE-WRITER-FENCING
// ---------------------------------------------------------------------------
// Two writers open the same WAL dir on one disk image (the single-writer
// assumption violated). The design assumes a single writer; there is NO fencing
// (epoch/lease) token today. This test DOCUMENTS that gap: it opens two
// independent `Wal` writers on the same FakeDisk image and asserts that every
// frame each writer lands is itself well-formed (CRC-valid, no cross-writer
// byte corruption WITHIN a frame) — i.e. a second writer cannot produce a
// half-frame that recovery misreads as a foreign record. Interleaved appends at
// the same tail offset CAN clobber each other's frames (the documented hazard);
// the invariant under test is only "no frame is a fabricated/garbled record" —
// recovery still stops at the first torn frame and never materializes garbage.
//
// NOTE: each writer keeps its OWN append offset, so writing both to one file at
// overlapping offsets is the realistic double-writer corruption. To keep the
// assertion crisp (and the known-gap documented, not papered over) we point the
// two writers at the SAME first index and assert recovery never yields a
// fabricated seq outside the union each writer attempted — it reads a clean
// prefix of *some* writer's frames and stops at the first inconsistency.
// ===========================================================================

#[test]
fn f_wal_double_writer_fencing() {
    let disk = FakeDisk::new();
    let data_dir = PathBuf::from(DATA_DIR);

    // Two writers on the SAME wal dir + same first index = the single-writer
    // assumption deliberately violated (no fencing token exists to prevent it).
    let wal_a = Wal::open_at_with(disk.arc(), fast_cfg(&data_dir), 1, 0).unwrap();
    let wal_b = Wal::open_at_with(disk.arc(), fast_cfg(&data_dir), 1, 0).unwrap();
    let wa = wal_a.writer();
    let wb = wal_b.writer();

    let start = Arc::new(Barrier::new(2));
    let ta = {
        let start = start.clone();
        std::thread::spawn(move || {
            start.wait();
            for seq in 1..=20 {
                let _ = wa.append(ap(1, seq), true);
            }
        })
    };
    let tb = {
        let start = start.clone();
        std::thread::spawn(move || {
            start.wait();
            for seq in 1..=20 {
                let _ = wb.append(ap(2, seq), true);
            }
        })
    };
    ta.join().unwrap();
    tb.join().unwrap();

    let fs = disk.arc();
    let _ = fs.sync_dir(&data_dir.join("wal"));
    drop(wal_a);
    drop(wal_b);

    // Recovery reads frames until the first torn/inconsistent one. The KNOWN GAP:
    // two writers can clobber each other so some frames are lost — that's allowed
    // and documented. The HARD invariant: every frame the reader DOES decode is a
    // genuine, CRC-valid Append frame for one of the two topics (no cross-writer
    // garble materializes a fabricated record), and the reader stops cleanly.
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
    let mut decoded = 0u64;
    for f in files {
        let r = WalReader::open_with(&fs, &f).expect("open wal file");
        for frame in r {
            decoded += 1;
            // Every decoded frame is a real Append for topic 1 or topic 2 with a seq in
            // the attempted range — never a fabricated/garbled record. (The reader
            // itself already validated the CRC; we assert the decoded fields are
            // ones a writer actually attempted.)
            if let WalRecord::Append {
                topic_id,
                seq,
                data,
                ..
            } = &frame.record
            {
                assert!(
                    *topic_id == 1 || *topic_id == 2,
                    "decoded a frame for an unattempted topic {topic_id} (cross-writer garble)"
                );
                assert!((1..=20).contains(seq), "decoded an out-of-range seq {seq}");
                assert_eq!(
                    data,
                    &format!("payload-{seq}").into_bytes(),
                    "decoded a frame whose payload was corrupted by the other writer"
                );
            } else {
                panic!("decoded a non-Append frame (only Appends were written)");
            }
        }
    }
    // We attempted 40 frames across two un-fenced writers; recovery decodes a
    // CRC-valid subset and never a fabricated record. (At least one frame lands.)
    assert!(decoded >= 1, "recovery decoded at least one valid frame");
    // DOCUMENTED GAP: without a fencing/epoch token, two writers on one dir is
    // unsafe (frames can be lost to mutual clobber). The engine NEVER opens two
    // writers on one data_dir (single owner), so production is unaffected; this
    // test pins that the *frame format* still fails safe (torn-truncate, never
    // misread) even under the adversarial double-open.
}
