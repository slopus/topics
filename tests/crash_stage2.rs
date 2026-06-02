//! Stage-2 DURABILITY/RECOVERY crash tests (single-node QUICK-WINS iteration):
//!
//! - **R3** — `disk`-class seq monotonicity across restart. A `disk` write is
//!   acked before its WAL frame is fsynced, so a power loss can drop the frame.
//!   A durable per-topic head RESERVATION (fsynced ahead of use) guarantees the
//!   recovered head never regresses and an already-acked `disk` seq is never
//!   re-handed: the lost records become silent deleted gaps, but the seq counter
//!   only advances.
//! - **R7** — durable evict watermark for TTL/byte-cap. A record evicted by TTL
//!   or a byte cap must NOT resurrect after a crash+restart: the involuntary
//!   floor is durably (fsync) logged, split cap-vs-ttl so the from-0 tombstone
//!   reason survives too.
//! - **R14** — RAII publish-ticket guard. A panic in a ticketed writer between
//!   taking the publish ticket and releasing it must NOT hang `quiesce_publishes`
//!   (snapshot capture) or strand every later writer behind the unreleased ticket.
//! - **put_topic CREATE WAL-first** — a fresh create whose `TopicConfig` WAL frame
//!   fails to commit must leave NO topic (no orphan) and return an error.
//!
//! ```text
//! cargo test --features test-fs --test crash_stage2
//! ```

#![cfg(feature = "test-fs")]

use std::sync::Arc;

use serde_json::json;

use topics::clock::{SharedClock, TestClock};
use topics::config::{self, ServerConfig};
use topics::engine::Engine;
use topics::storage::testfs::{FakeDisk, FaultFs, FaultKind, FaultOp};
use topics::storage::Fs;
use topics::types::{DiffRequest, Durability, RecordIn, TopicConfig, TopicType, WriteRequest};

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
    Engine::with_data_dir_fs(cfg(), clock(), fs).expect("engine opens through fs")
}

fn put_topic(engine: &Engine, name: &str, config: TopicConfig) {
    engine.put_topic(name, config).expect("put_topic");
}

/// Append one record; returns the assigned seq.
fn append(engine: &Engine, name: &str, data: &str) -> u64 {
    let req = WriteRequest {
        records: vec![RecordIn {
            data: json!({ "v": data }),
            tag: None,
            node: None,
            meta: None,
        }],
        node: None,
        idempotency_key: None,
        create: Some(false),
        config: None,
        disable_backpressure: true,
    };
    engine
        .write(name, req, true)
        .expect("append acked")
        .last_seq
}

/// The live seqs of `name` read back through `diff` (deleted holes are skipped).
fn live_seqs(engine: &Engine, name: &str) -> Vec<u64> {
    let mut out = Vec::new();
    let mut from = 0u64;
    loop {
        let d = engine
            .diff(
                name,
                DiffRequest {
                    from_seq: from,
                    limit: 1000,
                    node: None,
                    include_tags: false,
                    include_meta: false,
                    wait_ms: 0,
                    max_batch_bytes: 0,
                },
            )
            .expect("diff");
        for r in &d.records {
            out.push(r.seq);
        }
        if d.caught_up || d.records.is_empty() {
            break;
        }
        from = d.next_from_seq;
    }
    out
}

fn sync_dirs(disk: &FakeDisk) {
    let fs = disk.arc();
    let _ = fs.sync_dir(std::path::Path::new(DATA_DIR).join("wal").as_path());
    let _ = fs.sync_dir(std::path::Path::new(DATA_DIR).join("meta").as_path());
}

// ===========================================================================
// R3 — disk-class seq monotonicity / no reuse across a power loss
// ===========================================================================

/// A `disk` topic acks writes before their frames fsync. After a power loss that
/// drops the un-fsynced tail, recovery must NOT regress the head and re-hand an
/// already-acked seq: the durable head reservation advances the recovered head
/// past every acked seq (the lost records are silent gaps), so the next append
/// continues ABOVE the highest seq ever acked — never reusing one.
#[test]
fn disk_topic_does_not_reuse_acked_seq_after_crash() {
    let disk = FakeDisk::new();

    let highest_acked = {
        let engine = open_engine(&disk);
        put_topic(
            &engine,
            "d",
            TopicConfig {
                r#type: TopicType::Log,
                durability: Some(Durability::Disk),
                ..Default::default()
            },
        );
        // The create's TopicConfig frame fsynced. The FIRST disk write fsyncs a head
        // RESERVATION (a block ahead) before acking — that fsync also hardens this
        // record's frame. Subsequent writes within the reservation are acked from
        // the buffered write only (no fsync), so a crash drops their frames.
        let mut last = 0;
        for i in 0..8 {
            last = append(&engine, "d", &format!("r{i}"));
        }
        assert_eq!(last, 8, "8 disk writes acked at seqs 1..=8");
        // Crash BEFORE any further group fsync: seqs 2..=8 were acked but their
        // frames are not durable; only seq 1 (hardened by the reservation fsync)
        // and the reservation watermark survive.
        disk.crash(topics::storage::testfs::TornDamage::None);
        drop(engine);
        last
    };

    // Recover. The head must not regress below the highest acked seq (no reuse),
    // and it sits at the durable reservation ceiling (the lost seqs are gaps).
    let engine = open_engine(&disk);
    let st = engine.topic_state("d", false).expect("disk topic recovers");
    assert!(
        st.head_seq >= highest_acked,
        "recovered head {} regressed below the highest acked seq {highest_acked} (REUSE!)",
        st.head_seq
    );
    assert!(
        st.head_seq <= highest_acked + config::DISK_HEAD_RESERVE_AHEAD,
        "recovered head {} within the reservation block of {highest_acked}",
        st.head_seq
    );

    // A fresh post-recovery append continues STRICTLY ABOVE every previously-acked
    // seq — proving no acked seq is re-handed to a new, different record.
    let new_seq = append(&engine, "d", "after-crash");
    assert!(
        new_seq > highest_acked,
        "post-recovery append seq {new_seq} reused an already-acked seq (<= {highest_acked})"
    );
    // …and exactly head+1 (contiguous from the reserved head — no second gap).
    assert_eq!(
        new_seq,
        st.head_seq + 1,
        "append continues at the reserved head + 1"
    );

    // The new record is readable at its (non-reused) seq.
    let seqs = live_seqs(&engine, "d");
    assert!(
        seqs.contains(&new_seq),
        "the fresh record is live at {new_seq}"
    );
    assert!(
        !seqs
            .iter()
            .any(|&s| s > 1 && s < new_seq && s <= highest_acked && s != 1),
        "the lost un-fsynced seqs read as silent gaps (not live): {seqs:?}"
    );
}

/// A CLEANLY-stopped `disk` topic never loses an acked seq either: every acked
/// record survives, head never regresses, and a post-restart append never reuses
/// a seq.
#[test]
fn disk_topic_clean_restart_preserves_seqs_no_reuse() {
    let disk = FakeDisk::new();
    let highest = {
        let engine = open_engine(&disk);
        put_topic(
            &engine,
            "d",
            TopicConfig {
                r#type: TopicType::Log,
                durability: Some(Durability::Disk),
                ..Default::default()
            },
        );
        let mut last = 0;
        for i in 0..5 {
            last = append(&engine, "d", &format!("c{i}"));
        }
        sync_dirs(&disk);
        drop(engine); // clean drain + fsync.
        last
    };
    let engine = open_engine(&disk);
    let st = engine.topic_state("d", false).expect("disk topic recovers");
    // All 5 acked records survive a clean restart.
    let seqs = live_seqs(&engine, "d");
    assert_eq!(
        seqs,
        (1..=highest).collect::<Vec<_>>(),
        "all acked disk records survive clean restart"
    );
    assert!(st.head_seq >= highest, "head never below the acked head");
    let new_seq = append(&engine, "d", "after-restart");
    assert!(new_seq > highest, "no seq reuse after a clean restart");
}

// ===========================================================================
// R7 — a TTL / byte-cap evicted record does not resurrect after restart
// ===========================================================================

/// A record evicted by TTL is durably floored: after a crash+restart (and even
/// with the clock NOT advancing on recovery) it must NOT resurrect — the from-0
/// read still tombstones and the evicted seq is not live.
#[test]
fn ttl_evicted_record_does_not_resurrect_after_crash() {
    let disk = FakeDisk::new();
    // A fsync topic with a short TTL so the records are durably on disk AND expire.
    let ttl = 10_000i64;
    let clk = Arc::new(TestClock::new(1_700_000_000_000));
    {
        let engine = Engine::with_data_dir_fs(cfg(), clk.clone(), disk.arc()).expect("engine");
        put_topic(
            &engine,
            "t",
            TopicConfig {
                r#type: TopicType::Log,
                durable: true,
                ttl_ms: ttl as u64,
                ..Default::default()
            },
        );
        for i in 0..4 {
            append(&engine, "t", &format!("e{i}")); // seqs 1..=4, all durable.
        }
        // Advance the clock PAST the TTL and force a retention pass via a read:
        // seqs 1..=4 expire; the engine durably logs the TTL evict watermark (R7).
        clk.advance(ttl + 1);
        let seqs = live_seqs(&engine, "t");
        assert!(
            seqs.is_empty(),
            "all 4 records TTL-expired before the crash, got {seqs:?}"
        );
        // Crash: the durable TTL watermark + the appends are on disk.
        sync_dirs(&disk);
        disk.crash(topics::storage::testfs::TornDamage::None);
        drop(engine);
    }

    // Recover on a clock REWOUND to before the writes (the watermark, not the
    // clock, must keep the floor): the records must NOT resurrect.
    let rewound = Arc::new(TestClock::new(1_700_000_000_000));
    let engine = Engine::with_data_dir_fs(cfg(), rewound, disk.arc()).expect("recover engine");
    let st = engine.topic_state("t", false).expect("ttl topic recovers");
    let seqs = live_seqs(&engine, "t");
    assert!(
        seqs.is_empty(),
        "TTL-evicted records resurrected after a crash+restart (durable floor lost!): {seqs:?}"
    );
    assert_eq!(st.count, 0, "no evicted record is live after recovery");
    // The involuntary loss stays explicit: a from-0 read tombstones with a TTL/
    // mixed reason (never silent, never a fabricated record).
    let d = engine
        .diff(
            "t",
            DiffRequest {
                from_seq: 0,
                limit: 1000,
                node: None,
                include_tags: false,
                include_meta: false,
                wait_ms: 0,
                max_batch_bytes: 0,
            },
        )
        .expect("diff");
    let reason = d
        .tombstone
        .map(|t| format!("{:?}", t.reason).to_lowercase())
        .expect("a below-floor read tombstones (involuntary TTL loss is explicit)");
    assert!(
        reason == "ttl" || reason == "mixed",
        "the recovered tombstone reason reflects the TTL eviction, got {reason:?}"
    );
}

/// A record evicted by a byte cap does not resurrect after a crash+restart: the
/// byte-cap floor (not re-derivable from the head) is durably persisted.
#[test]
fn byte_cap_evicted_record_does_not_resurrect_after_crash() {
    let disk = FakeDisk::new();
    let highest;
    {
        let engine = open_engine(&disk);
        // A small byte cap so a few writes push the oldest out.
        put_topic(
            &engine,
            "b",
            TopicConfig {
                r#type: TopicType::Log,
                durable: true,
                cap_bytes: 200,
                ..Default::default()
            },
        );
        let mut last = 0;
        for i in 0..10 {
            // ~50-byte payloads so 200 bytes holds only the last few.
            last = append(&engine, "b", &format!("payload-number-{i:020}"));
        }
        highest = last;
        let before = live_seqs(&engine, "b");
        assert!(
            before.len() < 10,
            "byte cap evicted some records, live={before:?}"
        );
        assert!(
            *before.first().unwrap() > 1,
            "the oldest seq was evicted by the byte cap, live={before:?}"
        );
        sync_dirs(&disk);
        disk.crash(topics::storage::testfs::TornDamage::None);
        drop(engine);
    }

    let engine = open_engine(&disk);
    let after = live_seqs(&engine, "b");
    // No evicted-then-resurrected record: the live set's earliest never regresses
    // below the durable byte-cap floor (seq 1 must NOT come back).
    assert!(
        !after.contains(&1),
        "byte-cap-evicted seq 1 resurrected after recovery (durable floor lost!): {after:?}"
    );
    assert!(
        after.iter().all(|&s| s <= highest),
        "no fabricated future seq: {after:?}"
    );
    let d = engine
        .diff(
            "b",
            DiffRequest {
                from_seq: 0,
                limit: 1000,
                node: None,
                include_tags: false,
                include_meta: false,
                wait_ms: 0,
                max_batch_bytes: 0,
            },
        )
        .expect("diff");
    assert!(
        d.tombstone.is_some(),
        "an involuntary byte-cap eviction tombstones a from-0 read after recovery"
    );
}

// NOTE: R14 (the RAII publish-ticket guard releasing the gate on a panicking
// ticketed writer) is exercised by the focused unit test
// `topic_state::tests::publish_guard_releases_gate_on_panic`, which panics a guard
// holder directly at the `TopicState` gate (without the WAL writer-thread confound
// an engine-level failpoint would introduce) and asserts a second waiter +
// `quiesce_publishes` still make progress.

// ===========================================================================
// put_topic CREATE WAL-first — a create whose WAL frame fails leaves NO topic
// ===========================================================================

/// A fresh `put_topic` CREATE whose `TopicConfig` WAL frame fails to fsync must apply
/// NOTHING: the call returns an error AND no orphan topic is left in the registry
/// (the WAL-first path logs+fsyncs the create BEFORE inserting the topic). After
/// the error, the topic does not exist; a later successful create works normally.
#[test]
fn put_topic_create_wal_fail_leaves_no_orphan_topic() {
    let disk = FakeDisk::new();

    // Lay down one durable topic on a clean disk so the engine + WAL are healthy.
    {
        let engine = open_engine(&disk);
        put_topic(
            &engine,
            "ok",
            TopicConfig {
                durable: true,
                ..Default::default()
            },
        );
        sync_dirs(&disk);
        drop(engine);
    }

    // Reopen and arm a one-shot SyncData EIO at index 0. Engine open + recovery
    // only ever `sync_all`/`sync_dir` (never the group-commit `sync_data`), and we
    // issue NO durable write before the create — so the FIRST `sync_data` on the
    // device is the create's TopicConfig group-commit fsync. The CREATE must FAIL.
    let faulty = FaultFs::new(disk.arc(), FaultOp::SyncData, FaultKind::Eio, 0, true);
    let engine = open_engine_fs(faulty.arc());

    let res = engine.put_topic(
        "orphan",
        TopicConfig {
            durable: true,
            ..Default::default()
        },
    );
    assert!(
        res.is_err(),
        "a create whose WAL frame EIOs must return an error"
    );

    // NO ORPHAN: the topic must not exist in memory (the WAL-first path never
    // inserted it; the old apply-first path would have left a phantom topic).
    assert!(
        engine.topic_state("orphan", false).is_err(),
        "a create whose WAL frame failed must leave NO topic (orphan!)"
    );
    // The topic-count gauge is exact (the reservation was released): only the
    // pre-existing "ok" topic counts.
    assert_eq!(
        engine.topic_count(),
        1,
        "the failed create's reservation was released"
    );

    // A subsequent create (device healthy again) succeeds normally.
    let again = engine.put_topic(
        "orphan",
        TopicConfig {
            durable: true,
            ..Default::default()
        },
    );
    assert!(
        again.is_ok(),
        "a retry after the device recovers creates the topic"
    );
    assert!(
        engine.topic_state("orphan", false).is_ok(),
        "the topic now exists"
    );
    assert_eq!(
        engine.topic_count(),
        2,
        "topic count reflects the now-created topic"
    );
}

/// A successful CREATE is durable: it survives recovery (the WAL-first path logs +
/// fsyncs the create before returning, so a crash right after the ack recovers it).
#[test]
fn put_topic_create_success_is_durable() {
    let disk = FakeDisk::new();
    {
        let engine = open_engine(&disk);
        put_topic(
            &engine,
            "c",
            TopicConfig {
                r#type: TopicType::Log,
                durable: true,
                cap_records: 7,
                ..Default::default()
            },
        );
        sync_dirs(&disk);
        disk.crash(topics::storage::testfs::TornDamage::None);
        drop(engine);
    }
    let engine = open_engine(&disk);
    let st = engine
        .topic_state("c", false)
        .expect("created topic survives recovery");
    assert_eq!(
        st.config.cap_records, 7,
        "the created config survives recovery"
    );
}
