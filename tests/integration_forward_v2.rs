//! Async + derived router forwarding (`TOPICS_FORWARD_V2`) — Stage 2 properties.
//!
//! This binary runs with the async/derived forwarding path ENABLED for its whole
//! process (the flag is captured per-engine at construction from the env var; this
//! binary sets it once before building any engine). It asserts the four Stage-2
//! goals that the legacy synchronous `forward_from` path cannot meet:
//!
//!   1. NO AMPLIFICATION — one source append fanning to N dests does exactly ONE
//!      WAL append (the source's), not N. Forwarded dest records are derived off
//!      the source log + the per-router cursor, never separately WAL-logged.
//!   2. NO SILENT LOSS — a back-pressured forward (a full `discard:"reject"` dest)
//!      is NOT skipped: the router cursor stays below it and it is delivered once
//!      the dest drains. (The R2 bug advanced the cursor past it permanently.)
//!   3. DETERMINISTIC RE-MATERIALIZATION across a restart — a derived dest rebuilds
//!      with the SAME seqs after a crash/restart (a consumer cursor stays valid),
//!      from the source WAL + the durable (snapshotted) per-router cursor, with NO
//!      dest Append frames in the WAL.
//!   4. SOURCE-RETENTION BOUND — if the source trimmed records before a router
//!      forwarded them, the dest surfaces a `source_trim` TOMBSTONE (never a silent
//!      gap): the dest faithfully reflects the source's retention.
//!
//! All tests use a real durable engine (`Engine::with_data_dir`) so the WAL exists
//! and the frame counter is meaningful. The clock is a manual `TestClock` so the
//! TTL/source-trim case is deterministic.

use std::sync::Arc;

use serde_json::json;
use topics::clock::{SharedClock, TestClock};
use topics::config::ServerConfig;
use topics::engine::Engine;
use topics::types::{
    DiffRequest, Discard, RecordIn, RouterCreateRequest, TombstoneReason, TopicConfig, WriteRequest,
};

/// Enable the async/derived path for THIS test process. Idempotent; every test in
/// this binary wants it on, and the engine captures it at construction.
fn enable_v2() {
    std::env::set_var("TOPICS_FORWARD_V2", "1");
}

fn durable_engine(dir: &std::path::Path) -> (Arc<Engine>, TestClock) {
    let clock = TestClock::new(1_000_000);
    let shared: SharedClock = Arc::new(clock.clone());
    let cfg = ServerConfig {
        data_dir: Some(dir.to_string_lossy().to_string()),
        ..ServerConfig::default()
    };
    let engine = Engine::with_data_dir(cfg, shared).expect("durable engine");
    (engine, clock)
}

fn one(data: serde_json::Value, tag: Option<&str>, node: Option<&str>) -> RecordIn {
    RecordIn {
        data,
        tag: tag.map(str::to_string),
        node: node.map(str::to_string),
        meta: None,
    }
}

fn write_req(records: Vec<RecordIn>) -> WriteRequest {
    WriteRequest {
        records,
        node: None,
        idempotency_key: None,
        create: None,
        config: None,
        disable_backpressure: false,
    }
}

fn router_req(source: &str, dest: &str) -> RouterCreateRequest {
    RouterCreateRequest {
        source: source.to_string(),
        dest: dest.to_string(),
        preserve_node: true,
        preserve_tag: true,
        create_dest: true,
        filter: None,
        allow_cycle: false,
    }
}

fn diff_all(engine: &Engine, topic_name: &str) -> topics::types::DiffResponse {
    engine
        .diff(
            topic_name,
            DiffRequest {
                from_seq: 0,
                limit: 1000,
                include_tags: true,
                ..DiffRequest::default()
            },
        )
        .unwrap()
}

// ---------------------------------------------------------------------------
// 1. No amplification: one source append to N dests = ONE WAL append.
// ---------------------------------------------------------------------------

#[test]
fn one_source_append_to_n_dests_is_one_wal_append() {
    enable_v2();
    let dir = tempfile::tempdir().unwrap();
    let (engine, _clock) = durable_engine(dir.path());

    // Fan a single source out to FIVE durable dests. Use the fsync class for every
    // topic so a `disk` topic's R3 head-watermark reservation frame does not add to the
    // count — the only frames a write then produces are Append frames, so the frame
    // delta IS the append count.
    let n_dests = 5;
    let fsync_cfg = || TopicConfig {
        durable: true,
        ..TopicConfig::default()
    };
    engine.put_topic("src", fsync_cfg()).unwrap();
    for i in 0..n_dests {
        let dest = format!("dst{i}");
        engine.put_topic(&dest, fsync_cfg()).unwrap();
        engine
            .put_router(
                &format!("src->{dest}"),
                RouterCreateRequest {
                    create_dest: false,
                    ..router_req("src", &dest)
                },
            )
            .unwrap();
    }

    // Baseline WAL frame count AFTER all the control frames (topic + router creates).
    let frames_before = engine
        .wal_metrics()
        .unwrap()
        .frames
        .load(std::sync::atomic::Ordering::Relaxed);

    // ONE source write of one record fanning to 5 dests.
    engine
        .write(
            "src",
            write_req(vec![one(json!({"k": 1}), None, Some("o"))]),
            true,
        )
        .unwrap();

    // Read each dest, which catches up its feeding router (read-your-writes). The
    // forwarded copies are visible WITHOUT having logged a single dest Append frame.
    for i in 0..n_dests {
        let d = diff_all(&engine, &format!("dst{i}"));
        assert_eq!(d.records.len(), 1, "dst{i} got the forwarded copy");
        assert_eq!(d.records[0].data, json!({"k": 1}));
        assert_eq!(d.records[0].node.as_deref(), Some("o"), "$node preserved");
    }

    let frames_after = engine
        .wal_metrics()
        .unwrap()
        .frames
        .load(std::sync::atomic::Ordering::Relaxed);
    let appended = frames_after - frames_before;
    // EXACTLY ONE WAL frame for the whole fan-out (the source append). The legacy
    // path would have logged 1 (source) + 5 (dests) = 6.
    assert_eq!(
        appended, 1,
        "fanning one source write to {n_dests} dests must be ONE WAL append (no amplification); got {appended}"
    );

    // Measured WAL-writes-per-fanout (printed for the report): writes / fanout.
    println!(
        "WAL-writes-per-fanout: {appended} WAL append for a 1->{n_dests} fan-out (ratio {:.3})",
        appended as f64 / n_dests as f64
    );
}

// ---------------------------------------------------------------------------
// 2. No silent loss: a back-pressured forward is delivered once the dest drains.
// ---------------------------------------------------------------------------

#[test]
fn backpressured_forward_is_retried_not_dropped() {
    enable_v2();
    let dir = tempfile::tempdir().unwrap();
    let (engine, _clock) = durable_engine(dir.path());

    // A tiny reject-on-full dest (cap_records = 2).
    engine
        .put_topic(
            "dst",
            TopicConfig {
                cap_records: 2,
                discard: Discard::Reject,
                ..TopicConfig::default()
            },
        )
        .unwrap();
    engine
        .put_router("src->dst", router_req("src", "dst"))
        .unwrap();

    // Write FOUR records to the source in one shot. The dest can hold only two; the
    // forward of records 3 and 4 is back-pressured (deferred), NOT skipped.
    engine
        .write(
            "src",
            write_req(vec![
                one(json!({"i": 1}), None, None),
                one(json!({"i": 2}), None, None),
                one(json!({"i": 3}), None, None),
                one(json!({"i": 4}), None, None),
            ]),
            true,
        )
        .unwrap();

    // The catch-up on read forwards only the first two (the dest is then full).
    let d = diff_all(&engine, "dst");
    assert_eq!(d.records.len(), 2, "only two fit; the rest are deferred");
    let got: Vec<i64> = d
        .records
        .iter()
        .map(|r| r.data["i"].as_i64().unwrap())
        .collect();
    assert_eq!(got, vec![1, 2]);

    // The router cursor did NOT jump past records 3 and 4 (the R2 fix): forwarded
    // is exactly 2, so 3 and 4 are still pending below the cursor.
    let g = engine.get_router("src->dst").unwrap();
    assert_eq!(
        g.forwarded_total, 2,
        "cursor stopped at the first deferred record"
    );

    // Drain the dest (free capacity via a prefix delete of 1 and 2), then re-read:
    // the previously back-pressured records 3 and 4 are now delivered — no silent
    // loss.
    engine
        .delete(
            "dst",
            topics::types::DeleteRequest {
                before_seq: Some(3),
                match_: None,
            },
        )
        .unwrap();
    let d2 = diff_all(&engine, "dst");
    let got2: Vec<i64> = d2
        .records
        .iter()
        .map(|r| r.data["i"].as_i64().unwrap())
        .collect();
    assert_eq!(
        got2,
        vec![3, 4],
        "the deferred records are eventually delivered"
    );

    let g2 = engine.get_router("src->dst").unwrap();
    assert_eq!(g2.forwarded_total, 4, "all four eventually forwarded");
}

// ---------------------------------------------------------------------------
// 3. Deterministic re-materialization across a restart.
// ---------------------------------------------------------------------------

#[test]
fn dest_rematerializes_deterministically_across_restart() {
    enable_v2();
    let dir = tempfile::tempdir().unwrap();

    let pre_seqs: Vec<u64>;
    let pre_data: Vec<serde_json::Value>;
    {
        let (engine, _clock) = durable_engine(dir.path());
        engine.put_topic("src", TopicConfig::default()).unwrap();
        engine
            .put_router("src->dst", router_req("src", "dst"))
            .unwrap();

        // Three source writes; drive the derived dest.
        for i in 1..=3 {
            engine
                .write(
                    "src",
                    write_req(vec![one(json!({"i": i}), None, Some("o"))]),
                    true,
                )
                .unwrap();
        }
        let d = diff_all(&engine, "dst");
        assert_eq!(d.records.len(), 3);
        pre_seqs = d.records.iter().map(|r| r.seq).collect();
        pre_data = d.records.iter().map(|r| r.data.clone()).collect();
        assert_eq!(pre_seqs, vec![1, 2, 3]);

        // Snapshot so the per-router cursor is durable across the restart, then drop
        // the engine (flushes + joins the WAL writer).
        engine.write_snapshot().unwrap();
    }

    // Restart from the same data dir: the dest topic has NO Append frames in the WAL
    // (derived), so it is re-materialized by replaying forwarding from the
    // recovered cursor — with the SAME seqs.
    {
        let (engine, _clock) = durable_engine(dir.path());
        let d = diff_all(&engine, "dst");
        let post_seqs: Vec<u64> = d.records.iter().map(|r| r.seq).collect();
        let post_data: Vec<serde_json::Value> = d.records.iter().map(|r| r.data.clone()).collect();
        assert_eq!(
            post_seqs, pre_seqs,
            "dest re-materializes with identical seqs (consumer cursor stays valid)"
        );
        assert_eq!(
            post_data, pre_data,
            "dest re-materializes identical content"
        );
        for r in &d.records {
            assert_eq!(
                r.node.as_deref(),
                Some("o"),
                "$node preserved across restart"
            );
        }

        // Forwarding still works after restart for a fresh source write.
        engine
            .write(
                "src",
                write_req(vec![one(json!({"i": 4}), None, Some("o"))]),
                true,
            )
            .unwrap();
        let d2 = diff_all(&engine, "dst");
        assert_eq!(d2.records.len(), 4, "new write forwarded after restart");
        assert_eq!(
            d2.records[3].seq, 4,
            "next dest seq continues deterministically"
        );
    }
}

// ---------------------------------------------------------------------------
// 4. Source-retention bound: a trimmed source surfaces a source_trim tombstone.
// ---------------------------------------------------------------------------

#[test]
fn source_trim_surfaces_tombstone_not_silent_gap() {
    enable_v2();
    let dir = tempfile::tempdir().unwrap();
    let (engine, clock) = durable_engine(dir.path());

    // A source with a TTL so records age out, and a router that has NOT yet
    // forwarded them (we never read the dest until after the trim).
    engine
        .put_topic(
            "src",
            TopicConfig {
                ttl_ms: 1_000,
                ..TopicConfig::default()
            },
        )
        .unwrap();
    engine
        .put_router("src->dst", router_req("src", "dst"))
        .unwrap();

    // Two early source writes that we will let expire BEFORE forwarding.
    engine
        .write(
            "src",
            write_req(vec![one(json!({"i": 1}), None, None)]),
            true,
        )
        .unwrap();
    engine
        .write(
            "src",
            write_req(vec![one(json!({"i": 2}), None, None)]),
            true,
        )
        .unwrap();

    // Advance past the TTL so seqs 1 and 2 are involuntarily trimmed on the source,
    // then write a fresh record that is still live.
    clock.advance(2_000);
    // Force the source retention floor to advance (a state read enforces it).
    let _ = engine.topic_state("src", false).unwrap();
    engine
        .write(
            "src",
            write_req(vec![one(json!({"i": 3}), None, None)]),
            true,
        )
        .unwrap();

    // Now read the dest: the router catches up, finds the source trimmed below its
    // cursor, surfaces a source_trim tombstone, and forwards only the live record.
    let d = diff_all(&engine, "dst");

    // A reader starting from 0 crosses the source-trim gap ⇒ a tombstone (not a
    // silent skip), with the live record delivered after it.
    let tomb = d
        .tombstone
        .expect("a source_trim tombstone must surface across the trimmed prefix");
    assert_eq!(
        tomb.reason,
        TombstoneReason::SourceTrim,
        "the gap is attributed to the source's retention (source_trim)"
    );
    assert!(
        d.records.iter().any(|r| r.data == json!({"i": 3})),
        "the still-live source record is forwarded after the gap"
    );
    // The trimmed records were never silently dropped: the dest's earliest live seq
    // is above the gap.
    assert!(
        d.earliest_seq > 1,
        "dest earliest is above the trimmed prefix"
    );
}

// ---------------------------------------------------------------------------
// 5. No multi-source fan-in into a derived dest (codex P0 #2/#4): a SECOND router
//    with a DIFFERENT source into the same dest is rejected (its derived seq
//    stream must have a single owner for deterministic re-materialization). A topic
//    that ALSO takes direct writes / closes a cycle is still permitted (the /v0
//    `allow_cycle` contract requires it), so only genuine fan-in is refused.
// ---------------------------------------------------------------------------

#[test]
fn derived_dest_rejects_multi_source_fan_in() {
    enable_v2();
    let dir = tempfile::tempdir().unwrap();
    let (engine, _clock) = durable_engine(dir.path());

    engine.put_topic("src", TopicConfig::default()).unwrap();
    engine
        .put_router("src->dst", router_req("src", "dst"))
        .unwrap();

    // (a) A SECOND router with a DIFFERENT source into the same dest (multi-source
    //     fan-in) is rejected (409): two derived seq streams cannot share a dest.
    engine.put_topic("src2", TopicConfig::default()).unwrap();
    let fan_in = engine.put_router("src2->dst", router_req("src2", "dst"));
    assert!(
        fan_in.is_err(),
        "a second router with a different source into a derived dest must be rejected"
    );

    // (b) An idempotent re-PUT of the SAME router is allowed (single ownership kept).
    engine
        .put_router("src->dst", router_req("src", "dst"))
        .expect("re-PUT of the same router is allowed");

    // (c) A topic that ALSO takes direct writes can still become a router dest, and a
    //     direct write into a router dest still succeeds (the allow_cycle/`/v0`
    //     contract: a topic may be both a direct-write topic and a router dest).
    engine.put_topic("mixed", TopicConfig::default()).unwrap();
    engine
        .write(
            "mixed",
            write_req(vec![one(json!({"d": 1}), None, None)]),
            true,
        )
        .expect("direct write into a not-yet-router topic");
    engine
        .put_router(
            "src->mixed",
            RouterCreateRequest {
                create_dest: false,
                ..router_req("src", "mixed")
            },
        )
        .expect("a router onto a direct-write topic is allowed (mixed topic)");
    // A further direct write into the now-router-dest topic still succeeds.
    engine
        .write(
            "mixed",
            write_req(vec![one(json!({"d": 2}), None, None)]),
            true,
        )
        .expect("direct write into a router dest still succeeds");

    // Forwarding into the dedicated derived dest still works.
    engine
        .write(
            "src",
            write_req(vec![one(json!({"k": 9}), None, None)]),
            true,
        )
        .unwrap();
    let d = diff_all(&engine, "dst");
    assert_eq!(d.records.len(), 1);
    assert_eq!(d.records[0].data, json!({"k": 9}));
}

// ---------------------------------------------------------------------------
// 6. Durable cursor under recovery WITHOUT a snapshot (codex P0 #3): the
//    create-time cursor is logged in the RouterCreate frame, so a router created
//    AFTER source history does NOT backfill that history on a snapshot-less
//    restart (recovery replays the cursor from the WAL frame, not the live head).
// ---------------------------------------------------------------------------

#[test]
fn router_create_cursor_is_durable_without_snapshot() {
    enable_v2();
    let dir = tempfile::tempdir().unwrap();

    {
        let (engine, _clock) = durable_engine(dir.path());
        // Source history BEFORE the router exists.
        engine.put_topic("src", TopicConfig::default()).unwrap();
        for i in 1..=3 {
            engine
                .write(
                    "src",
                    write_req(vec![one(json!({"i": i}), None, None)]),
                    true,
                )
                .unwrap();
        }
        // Create the router AFTER the history: its cursor seeds at source head (3),
        // so it must NOT forward records 1..=3.
        engine
            .put_router("src->dst", router_req("src", "dst"))
            .unwrap();
        // One post-create write that SHOULD forward.
        engine
            .write(
                "src",
                write_req(vec![one(json!({"i": 4}), None, None)]),
                true,
            )
            .unwrap();
        let d = diff_all(&engine, "dst");
        assert_eq!(d.records.len(), 1, "only the post-create record forwards");
        assert_eq!(d.records[0].data, json!({"i": 4}));
        // Deliberately do NOT snapshot: force recovery to rebuild the cursor purely
        // from the RouterCreate WAL frame.
    }

    {
        let (engine, _clock) = durable_engine(dir.path());
        let d = diff_all(&engine, "dst");
        // The pre-create history (1..=3) must NOT be backfilled on recovery: the
        // logged create-time cursor pins the boundary, replay-order independent.
        assert_eq!(
            d.records.len(),
            1,
            "recovery must not backfill pre-create source history (durable cursor)"
        );
        assert_eq!(d.records[0].data, json!({"i": 4}));
    }
}

// ---------------------------------------------------------------------------
// 7. Snapshot ⇄ router-advance atomicity under concurrency (codex P0 #1): with a
//    background drain racing source writes and a mid-stream snapshot, recovery
//    re-materializes the derived dest with NEITHER duplication NOR loss (the
//    cursor + derived dest are one consistent checkpoint unit).
// ---------------------------------------------------------------------------

#[test]
fn snapshot_cursor_and_dest_stay_consistent_under_concurrency() {
    enable_v2();
    let dir = tempfile::tempdir().unwrap();
    let n: i64 = 200;

    {
        let (engine, _clock) = durable_engine(dir.path());
        engine.put_topic("src", TopicConfig::default()).unwrap();
        engine
            .put_router("src->dst", router_req("src", "dst"))
            .unwrap();

        // A background drainer racing the writer + a mid-stream snapshot.
        let e2 = engine.clone();
        let drainer = std::thread::spawn(move || {
            for _ in 0..2_000 {
                e2.drain_router_sources();
                std::thread::yield_now();
            }
        });
        let e3 = engine.clone();
        let snapper = std::thread::spawn(move || {
            for _ in 0..20 {
                let _ = e3.write_snapshot();
                std::thread::yield_now();
            }
        });
        for i in 1..=n {
            engine
                .write(
                    "src",
                    write_req(vec![one(json!({"i": i}), None, None)]),
                    true,
                )
                .unwrap();
        }
        drainer.join().unwrap();
        snapper.join().unwrap();

        // Catch up fully, then a final snapshot so the cursor is durable.
        let d = diff_all(&engine, "dst");
        assert_eq!(d.records.len(), n as usize, "all forwarded before restart");
        engine.write_snapshot().unwrap();
    }

    {
        let (engine, _clock) = durable_engine(dir.path());
        let d = diff_all(&engine, "dst");
        // Exactly n records, each i once, seqs 1..=n — no duplicate re-forward (old
        // cursor + new dest) and no silent loss (new cursor + old dest).
        assert_eq!(
            d.records.len(),
            n as usize,
            "derived dest re-materializes with exactly the source count (no dup/loss)"
        );
        let seqs: Vec<u64> = d.records.iter().map(|r| r.seq).collect();
        let expected: Vec<u64> = (1..=n as u64).collect();
        assert_eq!(
            seqs, expected,
            "dest seqs are contiguous 1..=n across restart"
        );
        let mut vals: Vec<i64> = d
            .records
            .iter()
            .map(|r| r.data["i"].as_i64().unwrap())
            .collect();
        vals.sort_unstable();
        assert_eq!(
            vals,
            (1..=n).collect::<Vec<_>>(),
            "each source record forwarded exactly once"
        );
    }
}
