//! Phase-4 Stage-2 durability tests: a durable write survives an engine
//! teardown + reopen (the "restart" surface for an in-process engine), config /
//! routers / deletes / eviction watermarks are restored, and a torn WAL tail is
//! truncated rather than misread as data.
//!
//! These drive the engine directly (no HTTP) with a unique `tempfile::tempdir`
//! per test, so each is isolated and leaves nothing behind. A separate
//! SIGKILL-against-the-binary test (`kill_during_durable_write_survives`)
//! exercises the real hard-kill path.

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::json;
use topics::clock::{SharedClock, SystemClock, TestClock};
use topics::config::{SegmentConfig, ServerConfig};
use topics::engine::Engine;
use topics::types::*;

/// A `ServerConfig` pointed at `dir` (durable mode).
fn config_at(dir: &std::path::Path) -> ServerConfig {
    ServerConfig {
        data_dir: Some(dir.to_string_lossy().into_owned()),
        ..ServerConfig::default()
    }
}

fn engine_at(dir: &std::path::Path) -> Arc<Engine> {
    let clock: SharedClock = Arc::new(SystemClock);
    Engine::with_data_dir(config_at(dir), clock).expect("open durable engine")
}

fn one(data: serde_json::Value, tag: Option<&str>) -> WriteRequest {
    WriteRequest {
        records: vec![RecordIn {
            data,
            tag: tag.map(str::to_string),
            node: None,
            meta: None,
        }],
        node: None,
        idempotency_key: None,
        create: None,
        config: None,
        disable_backpressure: false,
    }
}

fn durable_topic() -> TopicConfig {
    TopicConfig {
        durable: true,
        ..TopicConfig::default()
    }
}

fn diff_from(from_seq: u64) -> DiffRequest {
    DiffRequest {
        from_seq,
        include_tags: true,
        ..DiffRequest::default()
    }
}

/// The headline guarantee: durable writes are present after a restart.
#[test]
fn durable_writes_survive_restart() {
    let dir = tempfile::tempdir().unwrap();

    {
        let engine = engine_at(dir.path());
        engine.put_topic("jobs", durable_topic()).unwrap();
        for i in 1..=5 {
            // durable:true ⇒ fsync-gated; fsync_ms is populated and > 0.
            let resp = engine
                .write("jobs", one(json!({ "i": i }), Some("t")), true)
                .unwrap();
            assert!(
                resp.performance.fsync_ms.unwrap_or(0.0) > 0.0,
                "durable write must report a real fsync_ms"
            );
        }
        // Drop engine → WAL writer drains + fsyncs + joins (Drop path).
    }

    // Reopen the same data dir: state must be fully recovered.
    let engine = engine_at(dir.path());
    let st = engine.topic_state("jobs", false).unwrap();
    assert_eq!(st.head_seq, 5, "all 5 durable writes recovered");
    assert_eq!(st.earliest_seq, 1);
    assert_eq!(st.count, 5);
    assert!(st.config.durable, "config recovered");

    let d = engine.diff("jobs", diff_from(0)).unwrap();
    let seqs: Vec<u64> = d.records.iter().map(|r| r.seq).collect();
    assert_eq!(seqs, vec![1, 2, 3, 4, 5]);
    assert_eq!(d.records[0].data, json!({ "i": 1 }));
    assert_eq!(d.records[0].tag.as_deref(), Some("t"));
}

/// Regression test for a concurrent-durable-writer ordering bug: many threads
/// writing durable single-record appends to the SAME topic must all survive a
/// restart, with a contiguous `[1..=N]` seq set and no loss. The bug: seq
/// assignment (`TopicState::append`, under the index lock) and WAL enqueue were
/// not atomic, so two writers could assign seqs `A < B` yet enqueue `B`'s frame
/// before `A`'s; recovery (apply-in-WAL-order, skip `seq <= head`) then silently
/// dropped `A`. The per-topic `append_lock` makes assignment+enqueue atomic, so
/// WAL order matches seq order and every acked durable write is recovered.
#[test]
fn concurrent_durable_writers_no_loss_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    let writers = 8u64;
    let per_writer = 250u64;
    let total = writers * per_writer;
    {
        let engine = engine_at(dir.path());
        engine.put_topic("hot", durable_topic()).unwrap();
        let mut handles = Vec::new();
        for w in 0..writers {
            let engine = engine.clone();
            handles.push(std::thread::spawn(move || {
                for i in 0..per_writer {
                    // Each write is durable ⇒ the ack is fsync-gated; once it
                    // returns the frame is on disk and must survive a restart.
                    engine
                        .write("hot", one(json!({ "w": w, "i": i }), None), true)
                        .unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let st = engine.topic_state("hot", false).unwrap();
        assert_eq!(st.head_seq, total, "all concurrent durable writes acked");
        assert_eq!(st.count, total);
    }

    // Restart: every acked durable write is recovered as a contiguous prefix.
    let engine = engine_at(dir.path());
    let st = engine.topic_state("hot", false).unwrap();
    assert_eq!(
        st.head_seq, total,
        "no acked durable write lost across restart"
    );
    assert_eq!(st.earliest_seq, 1);
    assert_eq!(st.count, total);

    // The recovered seqs are exactly [1..=total] — dense, contiguous, no gaps.
    let mut seqs = Vec::new();
    let mut from = 0u64;
    loop {
        let d = engine
            .diff(
                "hot",
                DiffRequest {
                    from_seq: from,
                    limit: 1000,
                    ..DiffRequest::default()
                },
            )
            .unwrap();
        if d.records.is_empty() {
            break;
        }
        for r in &d.records {
            seqs.push(r.seq);
        }
        from = d.next_from_seq;
        if d.caught_up {
            break;
        }
    }
    assert_eq!(seqs, (1..=total).collect::<Vec<_>>(), "contiguous [1..=N]");
}

/// Non-durable writes are also replayed when they reached the WAL before a
/// clean teardown (the writer drains + fsyncs on drop). Power-loss can lose the
/// un-fsynced tail (the documented fast-path tradeoff), not a clean teardown.
#[test]
fn nondurable_writes_survive_clean_teardown() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = engine_at(dir.path());
        engine.put_topic("evts", TopicConfig::default()).unwrap(); // durable:false
        for i in 1..=3 {
            engine
                .write("evts", one(json!({ "i": i }), None), true)
                .unwrap();
        }
    }
    let engine = engine_at(dir.path());
    let st = engine.topic_state("evts", false).unwrap();
    // All 3 acked records survive a clean teardown (the writer drains + fsyncs on
    // drop). `count` is exact; for a `disk` topic `head_seq` may sit at the durable
    // head RESERVATION (R3 fsyncs a head ceiling ahead of use to prevent seq reuse
    // across a power loss), so it is `>= 3` and within the reservation block — the
    // reserved-but-unwritten seqs are silent gaps that don't affect the live count.
    assert_eq!(st.count, 3);
    assert!(
        st.head_seq >= 3,
        "head never below the acked head (no reuse), got {}",
        st.head_seq
    );
    assert!(
        st.head_seq <= 3 + topics::config::DISK_HEAD_RESERVE_AHEAD,
        "head within the reservation block, got {}",
        st.head_seq
    );
}

/// Deletes replay deterministically: a previously-deleted record stays gone,
/// the delete is still silent (no tombstone), and `count`/`earliest_seq` match.
#[test]
fn deletes_replay_and_stay_gone() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = engine_at(dir.path());
        engine.put_topic("d", durable_topic()).unwrap();
        for i in 1..=5 {
            engine
                .write("d", one(json!({ "i": i }), Some(&format!("tag{i}"))), true)
                .unwrap();
        }
        // Delete tag3 (a middle hole) and everything < seq 2 (a prefix).
        engine
            .delete(
                "d",
                DeleteRequest {
                    before_seq: Some(2),
                    match_: None,
                },
            )
            .unwrap();
        engine
            .delete(
                "d",
                DeleteRequest {
                    before_seq: None,
                    match_: Some(Filter::from_shorthand("tag3")),
                },
            )
            .unwrap();
    }
    let engine = engine_at(dir.path());
    let d = engine.diff("d", diff_from(0)).unwrap();
    let seqs: Vec<u64> = d.records.iter().map(|r| r.seq).collect();
    assert_eq!(
        seqs,
        vec![2, 4, 5],
        "seq 1 (prefix) and seq 3 (tag) stay deleted"
    );
    assert!(
        d.tombstone.is_none(),
        "deletion stays silent across restart"
    );
    assert_eq!(engine.topic_state("d", false).unwrap().count, 3);
}

/// Routers (and their auto-created topics) survive a restart and keep forwarding.
#[test]
fn routers_survive_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = engine_at(dir.path());
        engine
            .put_router(
                "src->dst",
                RouterCreateRequest {
                    source: "src".into(),
                    dest: "dst".into(),
                    preserve_node: true,
                    preserve_tag: true,
                    create_dest: true,
                    filter: None,
                    allow_cycle: false,
                    guarantee: Default::default(),
                },
            )
            .unwrap();
    }
    let engine = engine_at(dir.path());
    // The router and both topics are back.
    let g = engine.get_router("src->dst").unwrap();
    assert_eq!(g.source, "src");
    assert_eq!(g.dest, "dst");
    // Forwarding still works post-restart.
    engine
        .write("src", one(json!({ "x": 1 }), None), true)
        .unwrap();
    let d = engine.diff("dst", diff_from(0)).unwrap();
    assert_eq!(d.records.len(), 1);
    assert_eq!(d.records[0].data, json!({ "x": 1 }));
}

/// A recovered cap-eviction watermark still yields a tombstone after restart
/// (no silent involuntary loss across a restart).
#[test]
fn evict_floor_tombstone_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = engine_at(dir.path());
        let cfg = TopicConfig {
            cap_records: 3,
            durable: true,
            ..TopicConfig::default()
        };
        engine.put_topic("cap", cfg).unwrap();
        for i in 1..=6 {
            engine
                .write("cap", one(json!({ "i": i }), None), true)
                .unwrap();
        }
        // head=6, cap=3 ⇒ earliest=4, evict_floor=3.
        assert_eq!(engine.topic_state("cap", false).unwrap().earliest_seq, 4);
    }
    let engine = engine_at(dir.path());
    let st = engine.topic_state("cap", false).unwrap();
    assert_eq!(st.head_seq, 6);
    assert_eq!(st.earliest_seq, 4, "cap floor recovered");
    assert_eq!(st.count, 3);
    // A consumer at from_seq=0 fell below the recovered evict_floor ⇒ tombstone.
    let d = engine.diff("cap", diff_from(0)).unwrap();
    let tomb = d.tombstone.expect("cap tombstone after restart");
    assert_eq!(tomb.reason, TombstoneReason::Cap);
}

/// A torn tail frame (a partial/interrupted write at the end of the WAL) is
/// truncated on recovery, never interpreted as data — and earlier, complete
/// frames survive.
#[test]
fn torn_tail_is_truncated_not_read_as_data() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = engine_at(dir.path());
        engine.put_topic("t", durable_topic()).unwrap();
        for i in 1..=3 {
            engine
                .write("t", one(json!({ "i": i }), None), true)
                .unwrap();
        }
    }

    // Corrupt the tail of the active WAL file: append garbage bytes that look
    // like the start of a frame but cannot form a complete, CRC-valid one.
    let wal_dir = dir.path().join("wal");
    let mut files: Vec<_> = std::fs::read_dir(&wal_dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    files.sort();
    let active = files.last().unwrap().clone();

    // Find the true end-of-data via the WAL reader's own framing logic, then
    // write a half-written next frame right after it (an oversized frame_len with
    // only a few bytes following ⇒ length overrun / CRC failure ⇒ torn tail).
    use std::io::{Seek, SeekFrom, Write};
    let data_end = topics::storage::WalReader::open(&active)
        .unwrap()
        .count_valid_len();
    let mut f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&active)
        .unwrap();
    f.seek(SeekFrom::Start(data_end as u64)).unwrap();
    let mut junk = Vec::new();
    junk.extend_from_slice(&9999u32.to_le_bytes());
    junk.extend_from_slice(&[0xAB; 16]);
    f.write_all(&junk).unwrap();
    f.sync_all().unwrap();
    drop(f);

    // Recovery must truncate the torn tail and recover exactly the 3 good frames.
    let engine = engine_at(dir.path());
    let st = engine.topic_state("t", false).unwrap();
    assert_eq!(st.head_seq, 3, "good frames recovered; torn tail discarded");
    assert_eq!(st.count, 3);
    let d = engine.diff("t", diff_from(0)).unwrap();
    assert_eq!(
        d.records.iter().map(|r| r.seq).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );

    // And the truncated WAL is writable again: a new durable write appends cleanly
    // and survives a second restart (proves the tail was truncated, not appended
    // after garbage).
    engine
        .write("t", one(json!({ "i": 4 }), None), true)
        .unwrap();
    drop(engine);
    let engine = engine_at(dir.path());
    assert_eq!(engine.topic_state("t", false).unwrap().head_seq, 4);
}

// ===========================================================================
// Stage 4: restart-recovery correctness + the readiness gate.
// ===========================================================================

/// A durable engine is `ready` the instant `with_data_dir` returns (recovery is
/// synchronous and completes before serving), and an empty/missing data dir is a
/// clean fresh start (no error, no topics, ready).
#[test]
fn fresh_dir_is_clean_start_and_ready() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path());
    assert!(engine.is_ready(), "fresh engine is ready after recovery");
    assert!((engine.replay_progress() - 1.0).abs() < f64::EPSILON);
    assert_eq!(engine.topic_count(), 0, "empty data dir ⇒ no topics");
}

/// write → snapshot → more writes → simulate restart (drop + reopen the same
/// data dir) → the full materialized state matches the pre-crash committed
/// state: head/earliest/count/config/records, and the engine comes back ready.
#[test]
fn write_snapshot_more_writes_restart_matches() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = engine_at(dir.path());
        engine
            .put_topic(
                "jobs",
                TopicConfig {
                    durable: true,
                    ttl_ms: 0,
                    ..TopicConfig::default()
                },
            )
            .unwrap();
        for i in 1..=4 {
            engine
                .write("jobs", one(json!({ "i": i }), Some(&format!("t{i}"))), true)
                .unwrap();
        }
        // Snapshot after 4...
        assert!(engine.write_snapshot().unwrap());
        // ...then more writes land only in the active WAL tail.
        for i in 5..=8 {
            engine
                .write("jobs", one(json!({ "i": i }), Some(&format!("t{i}"))), true)
                .unwrap();
        }
    }

    // Simulate a restart: a brand-new engine over the same data dir.
    let engine = engine_at(dir.path());
    assert!(engine.is_ready(), "engine is ready after restart recovery");
    let st = engine.topic_state("jobs", false).unwrap();
    assert_eq!(st.head_seq, 8, "snapshotted prefix + replayed tail");
    assert_eq!(st.earliest_seq, 1);
    assert_eq!(st.count, 8);
    assert!(st.config.durable, "config restored");

    let d = engine.diff("jobs", diff_from(0)).unwrap();
    let seqs: Vec<u64> = d.records.iter().map(|r| r.seq).collect();
    assert_eq!(seqs, vec![1, 2, 3, 4, 5, 6, 7, 8]);
    assert_eq!(d.records[7].data, json!({ "i": 8 }));
    assert_eq!(d.records[7].tag.as_deref(), Some("t8"));
}

/// No silent loss across restart: a consumer whose cursor fell below the
/// recovered `earliest_seq` still receives a tombstone when the floor was driven
/// by cap eviction (involuntary), while a purely-deleted gap stays silent — both
/// behaviors survive a restart (DESIGN §5.4, ROADMAP Phase-4).
#[test]
fn tombstone_vs_silent_gap_survive_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = engine_at(dir.path());
        // Cap-eviction topic: head 5, cap 3 ⇒ evict_floor advances (involuntary).
        engine
            .put_topic(
                "capped",
                TopicConfig {
                    cap_records: 3,
                    durable: true,
                    ..TopicConfig::default()
                },
            )
            .unwrap();
        for i in 1..=5 {
            engine
                .write("capped", one(json!({ "i": i }), None), true)
                .unwrap();
        }
        // Deletion topic: delete a prefix (voluntary ⇒ silent, no evict_floor bump).
        engine.put_topic("pruned", durable_topic()).unwrap();
        for i in 1..=5 {
            engine
                .write("pruned", one(json!({ "i": i }), None), true)
                .unwrap();
        }
        engine
            .delete(
                "pruned",
                DeleteRequest {
                    before_seq: Some(3),
                    match_: None,
                },
            )
            .unwrap();
    }

    let engine = engine_at(dir.path());

    // Cap topic: a cursor below the recovered involuntary floor ⇒ tombstone.
    let cap = engine.diff("capped", diff_from(0)).unwrap();
    let tomb = cap
        .tombstone
        .expect("cap tombstone after restart (no silent loss)");
    assert_eq!(tomb.reason, TombstoneReason::Cap);
    assert!(cap.records.iter().all(|r| r.seq >= cap.earliest_seq));

    // Deletion topic: a cursor in the purely-deleted gap ⇒ NO tombstone, silent
    // advance past the deleted prefix.
    let pruned = engine.diff("pruned", diff_from(0)).unwrap();
    assert!(
        pruned.tombstone.is_none(),
        "voluntary delete stays silent across restart"
    );
    assert_eq!(
        pruned.records.iter().map(|r| r.seq).collect::<Vec<_>>(),
        vec![3, 4, 5],
        "deleted prefix gone; survivors remain"
    );
    assert_eq!(engine.topic_state("pruned", false).unwrap().earliest_seq, 3);
}

// --- The readiness gate, exercised through the real `/v0/ready` handler. ---

/// Build the real axum router over `engine` and run one blocking request on a
/// throwaway current-thread runtime; returns `(status, json-body)`.
fn ready_request(engine: Arc<Engine>) -> (u16, serde_json::Value) {
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt; // for `oneshot`

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        let app = topics::http::build_router(engine);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/v0/ready")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status().as_u16();
        let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        let body: serde_json::Value =
            serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
        (status, body)
    })
}

/// `/v0/ready` returns `503 not_ready` (with `error.detail.replay_progress`)
/// while WAL replay is in progress, and `200 ready` once recovery completes —
/// driven through the real handler by toggling the engine's ready gate.
#[test]
fn ready_gate_503_during_replay_then_200() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path());
    engine.put_topic("jobs", durable_topic()).unwrap();

    // Simulate "mid-replay": gate closed, 3 of 8 frames applied.
    engine.set_ready_for_test(false, 3, 8);
    let (status, body) = ready_request(engine.clone());
    assert_eq!(status, 503, "not ready ⇒ 503 during replay");
    assert_eq!(body["error"]["code"], json!("not_ready"));
    let progress = body["error"]["detail"]["replay_progress"].as_f64().unwrap();
    assert!((progress - 0.375).abs() < 1e-9, "replay_progress = 3/8");

    // Recovery completes: gate opens.
    engine.set_ready_for_test(true, 0, 0);
    let (status, body) = ready_request(engine.clone());
    assert_eq!(status, 200, "ready ⇒ 200");
    assert_eq!(body["status"], json!("ready"));
    assert_eq!(body["wal_replay_complete"], json!(true));
    assert_eq!(body["topics"], json!(1));
}

// ===========================================================================
// Stage 3: snapshot / checkpoint round-trips and recovery-via-snapshot.
// ===========================================================================

/// Count `wal-*.log` and `snapshot-*.bin` files under a data dir.
fn count_files(dir: &std::path::Path, sub: &str, prefix: &str, suffix: &str) -> usize {
    let d = dir.join(sub);
    std::fs::read_dir(&d)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .filter(|e| {
                    let name = e.file_name();
                    let n = name.to_string_lossy();
                    n.starts_with(prefix) && n.ends_with(suffix)
                })
                .count()
        })
        .unwrap_or(0)
}

/// A snapshot of a populated engine round-trips: after writing a snapshot and
/// reopening the data dir, the materialized state (head/earliest/count/records/
/// config) is identical — recovered from the snapshot, not a full WAL replay.
#[test]
fn snapshot_round_trips_materialized_state() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = engine_at(dir.path());
        engine.put_topic("jobs", durable_topic()).unwrap();
        for i in 1..=10 {
            engine
                .write("jobs", one(json!({ "i": i }), Some(&format!("t{i}"))), true)
                .unwrap();
        }
        // Delete a middle record and a prefix to exercise floors/holes.
        engine
            .delete(
                "jobs",
                DeleteRequest {
                    before_seq: Some(3),
                    match_: None,
                },
            )
            .unwrap();
        engine
            .delete(
                "jobs",
                DeleteRequest {
                    before_seq: None,
                    match_: Some(Filter::from_shorthand("t6")),
                },
            )
            .unwrap();

        // Capture the pre-snapshot materialized view.
        let pre = engine.topic_state("jobs", false).unwrap();

        // Write a snapshot; it must be durably present and the WAL prefix dropped.
        assert!(engine.write_snapshot().unwrap(), "snapshot written");
        assert_eq!(
            count_files(dir.path(), "meta", "snapshot-", ".bin"),
            1,
            "exactly one snapshot file kept"
        );

        // Re-read state from the SAME engine: snapshot must not perturb it.
        let post = engine.topic_state("jobs", false).unwrap();
        assert_eq!(post.head_seq, pre.head_seq);
        assert_eq!(post.earliest_seq, pre.earliest_seq);
        assert_eq!(post.count, pre.count);
    }

    // Reopen: state restored from the snapshot (+ replay of the tiny WAL tail).
    let engine = engine_at(dir.path());
    let st = engine.topic_state("jobs", false).unwrap();
    assert_eq!(st.head_seq, 10);
    // seqs 1,2 (prefix) and 6 (tag) deleted ⇒ earliest 3, count 7.
    assert_eq!(st.earliest_seq, 3);
    assert_eq!(st.count, 7);
    assert!(st.config.durable);

    let d = engine.diff("jobs", diff_from(0)).unwrap();
    let seqs: Vec<u64> = d.records.iter().map(|r| r.seq).collect();
    assert_eq!(seqs, vec![3, 4, 5, 7, 8, 9, 10], "deleted seqs stay gone");
    assert!(
        d.tombstone.is_none(),
        "deletes stay silent after snapshot recovery"
    );
    assert_eq!(d.records[0].data, json!({ "i": 3 }));
    assert_eq!(d.records[0].tag.as_deref(), Some("t3"));
}

/// After a snapshot, the WAL files fully absorbed by the checkpoint are dropped
/// and recovery resumes from the checkpoint — yet a write made AFTER the
/// snapshot (only in the active WAL tail) is still recovered.
#[test]
fn snapshot_drops_absorbed_wal_and_replays_tail() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = engine_at(dir.path());
        engine.put_topic("b", durable_topic()).unwrap();
        for i in 1..=4 {
            engine
                .write("b", one(json!({ "i": i }), None), true)
                .unwrap();
        }
        assert!(engine.write_snapshot().unwrap());
        // A write AFTER the snapshot lives only in the active WAL tail.
        engine
            .write("b", one(json!({ "i": 5 }), None), true)
            .unwrap();
    }
    let engine = engine_at(dir.path());
    let st = engine.topic_state("b", false).unwrap();
    assert_eq!(
        st.head_seq, 5,
        "snapshotted 4 + post-snapshot tail write recovered"
    );
    assert_eq!(st.count, 5);
    let d = engine.diff("b", diff_from(0)).unwrap();
    assert_eq!(
        d.records.iter().map(|r| r.seq).collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5]
    );
}

/// Routers + their auto-created topics survive a snapshot-based restart and keep
/// forwarding from the right cursor.
#[test]
fn routers_survive_snapshot_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = engine_at(dir.path());
        engine
            .put_router(
                "src->dst",
                RouterCreateRequest {
                    source: "src".into(),
                    dest: "dst".into(),
                    preserve_node: true,
                    preserve_tag: true,
                    create_dest: true,
                    filter: None,
                    allow_cycle: false,
                    guarantee: Default::default(),
                },
            )
            .unwrap();
        engine
            .write("src", one(json!({ "x": 1 }), None), true)
            .unwrap();
        assert!(engine.write_snapshot().unwrap());
    }
    let engine = engine_at(dir.path());
    let g = engine.get_router("src->dst").unwrap();
    assert_eq!(g.source, "src");
    assert_eq!(g.forwarded_total, 1, "forward total restored from snapshot");
    // Forwarding still works post-restart, and doesn't re-forward the old record.
    engine
        .write("src", one(json!({ "x": 2 }), None), true)
        .unwrap();
    let d = engine.diff("dst", diff_from(0)).unwrap();
    assert_eq!(d.records.len(), 2);
    assert_eq!(
        d.records.iter().map(|r| r.data.clone()).collect::<Vec<_>>(),
        vec![json!({ "x": 1 }), json!({ "x": 2 })]
    );
}

/// A cap-eviction watermark captured in a snapshot still yields a tombstone
/// after a snapshot-based restart (no silent involuntary loss across restart).
#[test]
fn evict_floor_survives_snapshot_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = engine_at(dir.path());
        let cfg = TopicConfig {
            cap_records: 3,
            durable: true,
            ..TopicConfig::default()
        };
        engine.put_topic("cap", cfg).unwrap();
        for i in 1..=6 {
            engine
                .write("cap", one(json!({ "i": i }), None), true)
                .unwrap();
        }
        assert!(engine.write_snapshot().unwrap());
    }
    let engine = engine_at(dir.path());
    let st = engine.topic_state("cap", false).unwrap();
    assert_eq!(st.head_seq, 6);
    assert_eq!(st.earliest_seq, 4, "cap floor recovered from snapshot");
    assert_eq!(st.count, 3);
    let tomb = engine
        .diff("cap", diff_from(0))
        .unwrap()
        .tombstone
        .expect("cap tombstone");
    assert_eq!(tomb.reason, TombstoneReason::Cap);
}

/// Repeated snapshots keep exactly one snapshot file (older ones pruned) and
/// each round-trips correctly.
#[test]
fn repeated_snapshots_keep_one_and_stay_consistent() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = engine_at(dir.path());
        engine.put_topic("b", durable_topic()).unwrap();
        for round in 0..3 {
            for i in 1..=3 {
                engine
                    .write("b", one(json!({ "r": round, "i": i }), None), true)
                    .unwrap();
            }
            assert!(engine.write_snapshot().unwrap());
            assert_eq!(count_files(dir.path(), "meta", "snapshot-", ".bin"), 1);
        }
    }
    let engine = engine_at(dir.path());
    assert_eq!(engine.topic_state("b", false).unwrap().head_seq, 9);
}

// ===========================================================================
// The real acceptance criterion: SIGKILL the actual `topics` binary mid-life
// and confirm an acked durable write is present after restart.
// ===========================================================================

/// A spawned server: the child process plus the `base_url` it actually bound.
struct Server {
    child: std::process::Child,
    base: String,
}

/// Spawn the `topics` binary on an EPHEMERAL port (`TOPICS_PORT=0`) with
/// `data_dir`, then read the OS-assigned `host:port` the child wrote to its
/// `TOPICS_PORT_FILE` and return it as the base URL. Robust under parallel
/// spawn: the child holds the bound socket continuously, so (unlike a
/// reserve-then-release scheme) nothing can steal the port between reservation
/// and bind.
fn spawn_server(data_dir: &std::path::Path, port_file: &std::path::Path) -> Server {
    let _ = std::fs::remove_file(port_file); // avoid reading a stale prior boot.
    let child = std::process::Command::new(env!("CARGO_BIN_EXE_topics"))
        .env("TOPICS_HOST", "127.0.0.1")
        .env("TOPICS_PORT", "0") // OS-assigned ephemeral port (no bind race).
        .env("TOPICS_PORT_FILE", port_file)
        .env("TOPICS_DATA_DIR", data_dir)
        // Pin a single WAL shard for a deterministic on-disk layout across the
        // restart (the default is num_cpus-based); these tests recover via the
        // shard-agnostic engine, so this only keeps the layout stable for any
        // direct file inspection.
        .env("TOPICS_WAL_SHARDS", "1")
        .env("RUST_LOG", "error")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn topics binary");
    let base = read_port_file(port_file, Duration::from_secs(10));
    Server { child, base }
}

/// Poll `port_file` until the child has written its resolved `host:port`, then
/// return the `http://host:port` base URL. Panics after `deadline`.
fn read_port_file(port_file: &std::path::Path, deadline: Duration) -> String {
    let start = Instant::now();
    loop {
        if let Ok(s) = std::fs::read_to_string(port_file) {
            let s = s.trim();
            if !s.is_empty() {
                return format!("http://{s}");
            }
        }
        if start.elapsed() > deadline {
            panic!("server did not report its bound port within {deadline:?}");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Block until `GET /v0/health` answers `200`, or panic after `deadline`.
fn wait_healthy(client: &reqwest::blocking::Client, base: &str, deadline: Duration) {
    let start = Instant::now();
    loop {
        if let Ok(r) = client.get(format!("{base}/v0/health")).send() {
            if r.status().is_success() {
                return;
            }
        }
        if start.elapsed() > deadline {
            panic!("server did not become healthy within {deadline:?}");
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

#[test]
fn kill_during_durable_write_survives_sigkill_restart() {
    let dir = tempfile::tempdir().unwrap();
    let pf = tempfile::NamedTempFile::new().unwrap();
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();

    // --- Boot #1, write a DURABLE record, get an ack, then SIGKILL. ---------
    let server = spawn_server(dir.path(), pf.path());
    let mut child = server.child;
    let base = server.base;
    wait_healthy(&client, &base, Duration::from_secs(10));

    let (status, _b) = {
        let r = client
            .put(format!("{base}/v0/topics/jobs"))
            .json(&json!({ "durable": true }))
            .send()
            .unwrap();
        (r.status(), r)
    };
    assert!(status.is_success(), "create durable topic");

    // The write response returns ONLY after the fsync (durable:true), so once we
    // hold a 2xx the record is on disk — a SIGKILL now must not lose it.
    let resp = client
        .post(format!("{base}/v0/topics/jobs"))
        .json(&json!({ "records": [{ "data": { "n": 42 }, "tag": "k" }] }))
        .send()
        .unwrap();
    assert!(resp.status().is_success(), "durable write acked");
    let body: serde_json::Value = resp.json().unwrap();
    assert_eq!(body["head_seq"], 1);
    let fsync_ms = body["performance"]["fsync_ms"].as_f64().unwrap_or(0.0);
    assert!(
        fsync_ms > 0.0,
        "durable write must be fsync-gated (fsync_ms>0)"
    );

    // Hard kill — no graceful shutdown, no drop handlers, nothing flushed beyond
    // what the WAL fsync already durably committed.
    let pid = child.id();
    unsafe {
        libc_kill(pid as i32);
    }
    let _ = child.wait();

    // --- Boot #2 on the SAME data dir: the acked durable write is present. The
    // ephemeral port differs across boots, so rebind to the new base URL. ---
    let server2 = spawn_server(dir.path(), pf.path());
    let mut child2 = server2.child;
    let base = server2.base;
    wait_healthy(&client, &base, Duration::from_secs(10));

    let st: serde_json::Value = client
        .get(format!("{base}/v0/topics/jobs"))
        .send()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(
        st["head_seq"], 1,
        "durable write survived SIGKILL + restart"
    );
    assert_eq!(st["count"], 1);

    let diff: serde_json::Value = client
        .post(format!("{base}/v0/topics/jobs/diff"))
        .json(&json!({ "from_seq": 0, "include_tags": true }))
        .send()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(diff["records"][0]["data"], json!({ "n": 42 }));
    assert_eq!(diff["records"][0]["$tag"], json!("k"));

    // Clean up the second process.
    let _ = child2.kill();
    let _ = child2.wait();
}

/// Minimal SIGKILL via the C `kill(2)` syscall (no extra crate). Unix-only; the
/// test is gated to unix below.
#[cfg(unix)]
unsafe fn libc_kill(pid: i32) {
    extern "C" {
        fn kill(pid: i32, sig: i32) -> i32;
    }
    const SIGKILL: i32 = 9;
    kill(pid, SIGKILL);
}

#[cfg(not(unix))]
unsafe fn libc_kill(_pid: i32) {}

/// Send SIGTERM (graceful shutdown) to `pid`.
#[cfg(unix)]
unsafe fn libc_term(pid: i32) {
    extern "C" {
        fn kill(pid: i32, sig: i32) -> i32;
    }
    const SIGTERM: i32 = 15;
    kill(pid, SIGTERM);
}

#[cfg(not(unix))]
unsafe fn libc_term(_pid: i32) {}

/// Graceful shutdown writes a snapshot: SIGTERM the real binary, wait for a
/// clean exit, then confirm a snapshot file is present and a fresh boot recovers
/// the data from it.
#[cfg(unix)]
#[test]
fn graceful_shutdown_writes_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let pf = tempfile::NamedTempFile::new().unwrap();
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();

    let server = spawn_server(dir.path(), pf.path());
    let mut child = server.child;
    let base = server.base;
    wait_healthy(&client, &base, Duration::from_secs(10));

    // A durable topic + a few writes.
    let r = client
        .put(format!("{base}/v0/topics/jobs"))
        .json(&json!({ "durable": true }))
        .send()
        .unwrap();
    assert!(r.status().is_success());
    for i in 1..=3 {
        let r = client
            .post(format!("{base}/v0/topics/jobs"))
            .json(&json!({ "records": [{ "data": { "n": i } }] }))
            .send()
            .unwrap();
        assert!(r.status().is_success());
    }

    // Graceful shutdown (SIGTERM) → the server writes a final snapshot on exit.
    let pid = child.id();
    unsafe { libc_term(pid as i32) };
    // Wait for a clean exit (bounded).
    let exited = {
        let start = Instant::now();
        loop {
            match child.try_wait().unwrap() {
                Some(_) => break true,
                None if start.elapsed() > Duration::from_secs(10) => break false,
                None => std::thread::sleep(Duration::from_millis(25)),
            }
        }
    };
    assert!(exited, "server exited gracefully on SIGTERM");

    // A snapshot file must have been written under <data_dir>/meta.
    let snaps = count_files(dir.path(), "meta", "snapshot-", ".bin");
    assert_eq!(snaps, 1, "graceful shutdown wrote exactly one snapshot");

    // Reboot recovers the data (from the snapshot). The ephemeral port differs
    // across boots, so rebind to the new base URL.
    let server2 = spawn_server(dir.path(), pf.path());
    let mut child2 = server2.child;
    let base = server2.base;
    wait_healthy(&client, &base, Duration::from_secs(10));
    let st: serde_json::Value = client
        .get(format!("{base}/v0/topics/jobs"))
        .send()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(
        st["head_seq"], 3,
        "data recovered after graceful-shutdown snapshot"
    );
    assert_eq!(st["count"], 3);

    let _ = child2.kill();
    let _ = child2.wait();
}

// ===========================================================================
// Phase-6 Stage-2: segment writer + bounded payload cache (tiered storage).
//
// These drive the REAL durable engine (segments are wired into the write/serve
// path) with a TestClock and a small SegmentConfig, so the seal triggers and
// the age trigger are deterministic (no wall-clock sleeps). They assert that
// (1) segments roll at the event/byte/age thresholds, (2) reads served out of a
// sealed segment match the original record, (3) sealing + payload eviction do
// not perturb count/bytes/earliest/head, and (4) the data survives a restart
// with sealing active.
// ===========================================================================

/// A durable `ServerConfig` at `dir` with a custom segment seal policy.
fn config_with_segment(dir: &std::path::Path, seg: SegmentConfig) -> ServerConfig {
    ServerConfig {
        data_dir: Some(dir.to_string_lossy().into_owned()),
        segment: seg,
        ..ServerConfig::default()
    }
}

/// A durable `ServerConfig` at `dir` with a HOT data dir, a COLD dir, and a
/// custom segment seal + hot-retention policy (tiering enabled).
fn config_with_cold(
    dir: &std::path::Path,
    cold: &std::path::Path,
    seg: SegmentConfig,
) -> ServerConfig {
    ServerConfig {
        data_dir: Some(dir.to_string_lossy().into_owned()),
        cold_dir: Some(cold.to_string_lossy().into_owned()),
        segment: seg,
        ..ServerConfig::default()
    }
}

fn engine_with_cold(
    dir: &std::path::Path,
    cold: &std::path::Path,
    seg: SegmentConfig,
    clock: SharedClock,
) -> Arc<Engine> {
    Engine::with_data_dir(config_with_cold(dir, cold, seg), clock).expect("open durable engine")
}

/// Count `seg-*.data` files under `<root>/topics` (across all topic dirs).
fn count_tier_segments(root: &std::path::Path) -> usize {
    let topics = root.join("topics");
    let mut n = 0usize;
    let Ok(rd) = std::fs::read_dir(&topics) else {
        return 0;
    };
    for topic_entry in rd.flatten() {
        if let Ok(inner) = std::fs::read_dir(topic_entry.path()) {
            for f in inner.flatten() {
                if let Some(name) = f.file_name().to_str() {
                    if name.starts_with("seg-") && name.ends_with(".data") {
                        n += 1;
                    }
                }
            }
        }
    }
    n
}

/// A durable engine at `dir` with a custom segment policy + a supplied clock.
fn engine_with_segment(
    dir: &std::path::Path,
    seg: SegmentConfig,
    clock: SharedClock,
) -> Arc<Engine> {
    Engine::with_data_dir(config_with_segment(dir, seg), clock).expect("open durable engine")
}

/// Count `seg-*.data` files under `<data_dir>/topics` (across all topic dirs) — the
/// number of sealed segments materialized to the HOT tier.
fn count_segment_files(data_dir: &std::path::Path) -> usize {
    let topics = data_dir.join("topics");
    let mut n = 0usize;
    let Ok(rd) = std::fs::read_dir(&topics) else {
        return 0;
    };
    for topic_entry in rd.flatten() {
        if let Ok(inner) = std::fs::read_dir(topic_entry.path()) {
            for f in inner.flatten() {
                if let Some(name) = f.file_name().to_str() {
                    if name.starts_with("seg-") && name.ends_with(".data") {
                        n += 1;
                    }
                }
            }
        }
    }
    n
}

fn seg_cfg(max_events: u64, max_bytes: u64, max_age_ms: u64) -> SegmentConfig {
    SegmentConfig {
        max_events,
        max_bytes,
        max_age_ms,
        ..SegmentConfig::default()
    }
}

/// Segments roll at the event threshold, and every record still reads back
/// correctly through the diff path (resident tail + sealed-segment resolution).
#[test]
fn segment_rolls_at_event_threshold_and_reads_match() {
    let dir = tempfile::tempdir().unwrap();
    let clock = TestClock::new(1_000);
    let shared: SharedClock = Arc::new(clock.clone());
    // Seal every 3 records; disable byte/age triggers.
    let engine = engine_with_segment(dir.path(), seg_cfg(3, 0, 0), shared);

    engine.put_topic("logs", durable_topic()).unwrap();
    for i in 1..=10u64 {
        engine
            .write("logs", one(json!({ "i": i }), Some("t")), true)
            .unwrap();
    }

    // 10 records, sealing every 3 → 3 sealed segments (seqs 1-3,4-6,7-9); seq 10
    // is still in the active (unsealed) segment.
    assert_eq!(
        count_segment_files(dir.path()),
        3,
        "10 records / 3-per-segment seals 3 segments (the 10th stays active)"
    );

    // All 10 read back in order with the right payloads, regardless of whether
    // the payload is resident (active tail) or resolved from a sealed segment.
    let d = engine.diff("logs", diff_from(0)).unwrap();
    let seqs: Vec<u64> = d.records.iter().map(|r| r.seq).collect();
    assert_eq!(seqs, (1..=10).collect::<Vec<_>>());
    for (k, r) in d.records.iter().enumerate() {
        assert_eq!(r.data, json!({ "i": (k as u64 + 1) }), "payload matches");
        assert_eq!(r.tag.as_deref(), Some("t"));
    }

    // Sealing must NOT perturb the topic's observable counters.
    let st = engine.topic_state("logs", false).unwrap();
    assert_eq!(st.head_seq, 10);
    assert_eq!(st.earliest_seq, 1);
    assert_eq!(st.count, 10, "all 10 still live (sealing is not eviction)");
    assert!(st.bytes > 0, "retained bytes unaffected by sealing");
}

/// The byte-cap seal trigger fires before the event cap on big payloads.
#[test]
fn segment_rolls_at_byte_threshold() {
    let dir = tempfile::tempdir().unwrap();
    let clock = TestClock::new(0);
    let shared: SharedClock = Arc::new(clock.clone());
    // Large event cap, tiny byte cap so each ~big record seals the prior segment.
    let engine = engine_with_segment(dir.path(), seg_cfg(1_000_000, 64, 0), shared);

    engine.put_topic("big", durable_topic()).unwrap();
    let payload = "x".repeat(80); // each record's frame well exceeds 64 bytes.
    for i in 1..=5u64 {
        engine
            .write("big", one(json!({ "i": i, "p": payload }), None), true)
            .unwrap();
    }

    // Each append after the first sees the active segment already over 64 bytes
    // and seals it → at least 4 sealed segments for 5 big records.
    assert!(
        count_segment_files(dir.path()) >= 4,
        "byte cap seals near-every big record (got {})",
        count_segment_files(dir.path())
    );
    // Reads still correct.
    let d = engine.diff("big", diff_from(0)).unwrap();
    assert_eq!(d.records.len(), 5);
    assert_eq!(d.records[0].data["i"], json!(1));
    assert_eq!(d.records[4].data["i"], json!(5));
}

/// The age seal trigger is driven by the TestClock (no wall-clock sleep): an
/// idle/partial segment seals once it crosses `max_age_ms`.
#[test]
fn segment_rolls_at_age_threshold_via_test_clock() {
    let dir = tempfile::tempdir().unwrap();
    let clock = TestClock::new(1_000);
    let shared: SharedClock = Arc::new(clock.clone());
    // Big event/byte caps so only the age trigger can seal.
    let engine = engine_with_segment(dir.path(), seg_cfg(1_000_000, 0, 5_000), shared);

    engine.put_topic("aged", durable_topic()).unwrap();
    engine
        .write("aged", one(json!({ "i": 1 }), None), true)
        .unwrap();
    // Still young → no seal.
    clock.advance(2_000);
    engine
        .write("aged", one(json!({ "i": 2 }), None), true)
        .unwrap();
    assert_eq!(count_segment_files(dir.path()), 0, "not yet aged out");

    // Cross the age cap: the next append seals the (now-old) active segment.
    clock.advance(6_000); // age since start (1000) = 8000 >= 5000.
    engine
        .write("aged", one(json!({ "i": 3 }), None), true)
        .unwrap();
    assert_eq!(
        count_segment_files(dir.path()),
        1,
        "age trigger sealed seqs 1,2"
    );

    // Data still reads correctly across the sealed/active boundary.
    let d = engine.diff("aged", diff_from(0)).unwrap();
    assert_eq!(
        d.records.iter().map(|r| r.seq).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(d.records[0].data["i"], json!(1));
    assert_eq!(d.records[2].data["i"], json!(3));
}

/// Sealing + payload eviction survives a restart: after reopening the data dir,
/// every record (now resident again from the snapshot/WAL replay) reads back.
#[test]
fn sealed_records_survive_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let clock: SharedClock = Arc::new(SystemClock);
        let engine = engine_with_segment(dir.path(), seg_cfg(2, 0, 0), clock);
        engine.put_topic("s", durable_topic()).unwrap();
        for i in 1..=7u64 {
            engine
                .write("s", one(json!({ "i": i }), Some("k")), true)
                .unwrap();
        }
        // 7 records / 2-per-segment → 3 sealed (1-2,3-4,5-6); seq 7 active.
        assert_eq!(count_segment_files(dir.path()), 3);
        let d = engine.diff("s", diff_from(0)).unwrap();
        assert_eq!(d.records.len(), 7);
        assert_eq!(d.records[0].data, json!({ "i": 1 }));
    }
    // Reopen: WAL replay restores every record; reads match.
    let clock: SharedClock = Arc::new(SystemClock);
    let engine = engine_with_segment(dir.path(), seg_cfg(2, 0, 0), clock);
    let st = engine.topic_state("s", false).unwrap();
    assert_eq!(st.head_seq, 7);
    assert_eq!(st.count, 7);
    let d = engine.diff("s", diff_from(0)).unwrap();
    assert_eq!(
        d.records.iter().map(|r| r.seq).collect::<Vec<_>>(),
        (1..=7).collect::<Vec<_>>()
    );
    assert_eq!(d.records[0].data, json!({ "i": 1 }));
    assert_eq!(d.records[6].data, json!({ "i": 7 }));
}

// ===========================================================================
// Phase-6 Stage-3: cold relocation + tiered reads (the HARD-INVARIANT stage),
// driven through the REAL durable engine with a configured COLD dir.
//
// `relocate_topic_cold` / `relocate_all_due` run the relocator state machine
// synchronously here (the production background task just calls them on a tick),
// so the tests are deterministic with no wall-clock sleeps: a relocated segment
// still reads correctly (data identical), an interrupted relocation recovers
// without loss, and the cold-read path surfaces a `cold_segments_read` hint.
// ===========================================================================

/// A segment policy that seals every `max_events` records and keeps only the
/// newest `hot_retain` sealed segments hot.
fn seg_cfg_retain(max_events: u64, hot_retain: u64) -> SegmentConfig {
    SegmentConfig {
        max_events,
        max_bytes: 0,
        max_age_ms: 0,
        hot_retain_segments: hot_retain,
        hot_retain_bytes: 0,
    }
}

/// A relocated COLD segment still reads back byte-identically through the diff
/// path, the hot copy is gone, and the response surfaces a `cold_segments_read`
/// hint for the records served from cold.
#[test]
fn relocated_segment_reads_identically_through_diff() {
    let dir = tempfile::tempdir().unwrap();
    let cold = tempfile::tempdir().unwrap();
    let clock: SharedClock = Arc::new(SystemClock);
    // Seal every 2 records; keep the newest 1 sealed segment hot.
    let engine = engine_with_cold(dir.path(), cold.path(), seg_cfg_retain(2, 1), clock);

    engine.put_topic("logs", durable_topic()).unwrap();
    for i in 1..=7u64 {
        engine
            .write("logs", one(json!({ "i": i }), Some("t")), true)
            .unwrap();
    }
    // 7 records / 2-per-segment → 3 sealed (1-2,3-4,5-6); seq 7 active. All hot.
    assert_eq!(
        count_tier_segments(dir.path()),
        3,
        "all sealed segments start hot"
    );
    assert_eq!(count_tier_segments(cold.path()), 0);

    // Relocate: keep the newest 1 sealed (5-6) hot; spill 1-2 and 3-4 to cold.
    let n = engine.relocate_topic_cold("logs");
    assert_eq!(n, 2, "two oldest sealed segments relocated");
    assert_eq!(
        count_tier_segments(dir.path()),
        1,
        "only the newest sealed kept hot"
    );
    assert_eq!(
        count_tier_segments(cold.path()),
        2,
        "two segments now in cold"
    );

    // Every record still reads back in order with the right payload — cold
    // portion (1-4) stitched transparently before the hot portion (5-7).
    let d = engine
        .diff(
            "logs",
            DiffRequest {
                from_seq: 0,
                limit: 1000,
                include_tags: true,
                ..DiffRequest::default()
            },
        )
        .unwrap();
    let seqs: Vec<u64> = d.records.iter().map(|r| r.seq).collect();
    assert_eq!(seqs, (1..=7).collect::<Vec<_>>());
    for (k, r) in d.records.iter().enumerate() {
        assert_eq!(
            r.data,
            json!({ "i": (k as u64 + 1) }),
            "data identical after relocation"
        );
        assert_eq!(r.tag.as_deref(), Some("t"));
    }
    // The records served from cold (seqs 1-4) are reported via the hint.
    assert_eq!(
        d.performance.cold_segments_read,
        Some(4),
        "four records served from the cold tier are surfaced as a perf hint"
    );

    // Counters are unperturbed by relocation (it is not eviction).
    let st = engine.topic_state("logs", false).unwrap();
    assert_eq!(st.head_seq, 7);
    assert_eq!(st.earliest_seq, 1);
    assert_eq!(st.count, 7, "all live; relocation moved bytes, not records");
}

/// An interrupted relocation (the cold copy landed but the hot copy was never
/// dropped — the crash window) recovers without loss: the segment is still
/// readable (HOT preferred), and re-running the relocator completes idempotently.
#[test]
fn interrupted_relocation_recovers_through_engine() {
    let dir = tempfile::tempdir().unwrap();
    let cold = tempfile::tempdir().unwrap();
    let clock: SharedClock = Arc::new(SystemClock);
    let engine = engine_with_cold(dir.path(), cold.path(), seg_cfg_retain(2, 0), clock);

    engine.put_topic("logs", durable_topic()).unwrap();
    for i in 1..=5u64 {
        engine
            .write("logs", one(json!({ "i": i }), None), true)
            .unwrap();
    }
    // 5 records / 2-per-segment → 2 sealed (1-2,3-4); seq 5 active.
    assert_eq!(count_tier_segments(dir.path()), 2);

    // Simulate the crash window: copy a segment's files into COLD *manually*
    // (mirroring the relocator's copy step) but DO NOT drop the hot copy — i.e. a
    // crash after the cold fsync, before the hot unlink. Both tiers now hold seg 1.
    let topic_dir_name = std::fs::read_dir(dir.path().join("topics"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .file_name();
    let hot_topic = dir.path().join("topics").join(&topic_dir_name);
    let cold_topic = cold.path().join("topics").join(&topic_dir_name);
    std::fs::create_dir_all(&cold_topic).unwrap();
    for ext in ["data", "idx"] {
        let name = format!("seg-{:016}.{}", 1u64, ext);
        std::fs::copy(hot_topic.join(&name), cold_topic.join(&name)).unwrap();
    }
    // Seg 1 is now in BOTH tiers, hot copy intact (the interrupted state).
    assert!(hot_topic.join(format!("seg-{:016}.data", 1u64)).exists());
    assert!(cold_topic.join(format!("seg-{:016}.data", 1u64)).exists());

    // The record is fully readable (no loss) despite the duplicate.
    let d = engine.diff("logs", diff_from(0)).unwrap();
    assert_eq!(
        d.records.iter().map(|r| r.seq).collect::<Vec<_>>(),
        (1..=5).collect::<Vec<_>>()
    );
    assert_eq!(d.records[0].data, json!({ "i": 1 }));

    // Re-running the relocator completes idempotently: the copy is a no-op (cold
    // exists), the flip+drop finishes, and reads still match.
    engine.relocate_topic_cold("logs");
    let d2 = engine.diff("logs", diff_from(0)).unwrap();
    assert_eq!(
        d2.records.iter().map(|r| r.seq).collect::<Vec<_>>(),
        (1..=5).collect::<Vec<_>>()
    );
    assert_eq!(
        d2.records[0].data,
        json!({ "i": 1 }),
        "no loss after recovery"
    );
}

/// Relocated COLD segments survive a restart: after reopening the same hot+cold
/// dirs, recovery re-derives each segment's tier (preferring any surviving copy)
/// and every record still reads back — no segment is ever lost across a restart.
#[test]
fn relocated_segments_survive_restart() {
    let dir = tempfile::tempdir().unwrap();
    let cold = tempfile::tempdir().unwrap();
    {
        let clock: SharedClock = Arc::new(SystemClock);
        let engine = engine_with_cold(dir.path(), cold.path(), seg_cfg_retain(2, 1), clock);
        engine.put_topic("logs", durable_topic()).unwrap();
        for i in 1..=7u64 {
            engine
                .write("logs", one(json!({ "i": i }), Some("t")), true)
                .unwrap();
        }
        let n = engine.relocate_topic_cold("logs");
        assert_eq!(n, 2);
        assert_eq!(
            count_tier_segments(cold.path()),
            2,
            "two segments relocated to cold"
        );
    }
    // Reopen the SAME hot+cold dirs: recovery rebuilds the index (payloads
    // resident via WAL replay) and the segments are still present across tiers.
    let clock: SharedClock = Arc::new(SystemClock);
    let engine = engine_with_cold(dir.path(), cold.path(), seg_cfg_retain(2, 1), clock);
    let st = engine.topic_state("logs", false).unwrap();
    assert_eq!(st.head_seq, 7);
    assert_eq!(st.count, 7, "no record lost across restart");
    let d = engine.diff("logs", diff_from(0)).unwrap();
    assert_eq!(
        d.records.iter().map(|r| r.seq).collect::<Vec<_>>(),
        (1..=7).collect::<Vec<_>>()
    );
    assert_eq!(d.records[0].data, json!({ "i": 1 }));
    assert_eq!(d.records[6].data, json!({ "i": 7 }));
    // The cold segments are still in the cold tier (not pulled back hot needlessly).
    assert!(
        count_tier_segments(cold.path()) >= 2,
        "relocated segments still present in cold"
    );
}

/// With NO cold dir configured (the default in every existing test), the
/// relocator is a no-op — nothing relocates and behavior is unchanged.
#[test]
fn no_cold_dir_means_no_relocation() {
    let dir = tempfile::tempdir().unwrap();
    let clock: SharedClock = Arc::new(SystemClock);
    let engine = engine_with_segment(dir.path(), seg_cfg(2, 0, 0), clock);
    engine.put_topic("logs", durable_topic()).unwrap();
    for i in 1..=7u64 {
        engine
            .write("logs", one(json!({ "i": i }), None), true)
            .unwrap();
    }
    assert_eq!(
        engine.relocate_all_due(),
        0,
        "no cold tier ⇒ nothing relocates"
    );
    assert_eq!(engine.relocate_topic_cold("logs"), 0);
    // All sealed segments stay hot; reads unchanged.
    let d = engine.diff("logs", diff_from(0)).unwrap();
    assert_eq!(d.records.len(), 7);
    assert_eq!(
        d.performance.cold_segments_read, None,
        "no cold reads when all hot"
    );
}

// ===========================================================================
// Phase-6 Stage-4: segment-aware recovery + segment-granular reclaim.
//
// Reclaim is segment-granular and lazy (ARCHITECTURE §3.3, §5.6): cap/TTL
// eviction and `before_seq`/`match` deletion drop WHOLE sealed segment files
// (HOT or COLD) once every record they cover is dead — never a per-record
// rewrite on the hot path. The dual-watermark semantics are preserved across
// segments + tiers: an involuntary cap/TTL floor still tombstones, a voluntary
// delete stays silent. Recovery rebuilds state across hot+cold segments + the
// WAL tail with no acked-write loss, and reclaim is idempotent across restart.
// ===========================================================================

/// Cap eviction drops whole sealed segment files once they fall below the live
/// floor, yet a consumer whose cursor fell below the (involuntary) `evict_floor`
/// still receives a `cap` tombstone — segment reclaim never turns involuntary
/// loss silent.
#[test]
fn cap_eviction_drops_whole_segments_and_still_tombstones() {
    let dir = tempfile::tempdir().unwrap();
    let clock: SharedClock = Arc::new(SystemClock);
    // Seal every 2 records; cap at 4 live records.
    let engine = engine_with_segment(dir.path(), seg_cfg(2, 0, 0), clock);
    engine
        .put_topic(
            "cap",
            TopicConfig {
                cap_records: 4,
                durable: true,
                ..TopicConfig::default()
            },
        )
        .unwrap();

    // Write 12 records → cap=4 keeps seqs 9..=12 live; 1..=8 evicted. Segments
    // seal every 2 (1-2,3-4,5-6,7-8,9-10), seq 11/12 active-ish. The segments
    // fully below earliest (9) — i.e. 1-2,3-4,5-6,7-8 — are dropped whole.
    for i in 1..=12u64 {
        engine
            .write("cap", one(json!({ "i": i }), None), true)
            .unwrap();
    }

    let st = engine.topic_state("cap", false).unwrap();
    assert_eq!(st.head_seq, 12);
    assert_eq!(st.earliest_seq, 9, "cap=4 keeps the newest 4 live");
    assert_eq!(st.count, 4);

    // The four sealed segments fully below earliest_seq=9 were physically
    // reclaimed; only the segment(s) overlapping the live set remain on disk.
    let remaining = count_segment_files(dir.path());
    assert!(
        remaining <= 2,
        "whole sealed segments below the live floor dropped (got {remaining} files)"
    );

    // A consumer at from_seq=0 fell below the involuntary floor ⇒ cap tombstone,
    // then live records resume at earliest_seq.
    let d = engine.diff("cap", diff_from(0)).unwrap();
    let tomb = d
        .tombstone
        .expect("cap eviction still tombstones after segment drop");
    assert_eq!(tomb.reason, TombstoneReason::Cap);
    assert_eq!(tomb.gap_from, 1);
    assert_eq!(tomb.gap_to, 8);
    let seqs: Vec<u64> = d.records.iter().map(|r| r.seq).collect();
    assert_eq!(seqs, vec![9, 10, 11, 12]);
    for r in &d.records {
        assert_eq!(r.data, json!({ "i": r.seq }), "surviving records read back");
    }
}

/// TTL expiry drops whole sealed segments once expired (the same segment-granular
/// reclaim as cap), and a cursor below the recovered TTL floor still tombstones
/// with `reason:"ttl"`. Driven by the TestClock (no wall-clock sleep).
#[test]
fn ttl_expiry_drops_whole_segments_and_tombstones() {
    let dir = tempfile::tempdir().unwrap();
    let clock = TestClock::new(1_000);
    let shared: SharedClock = Arc::new(clock.clone());
    let engine = engine_with_segment(dir.path(), seg_cfg(2, 0, 0), shared);
    engine
        .put_topic(
            "ttl",
            TopicConfig {
                ttl_ms: 1_000,
                durable: true,
                ..TopicConfig::default()
            },
        )
        .unwrap();

    // Write 6 at t≈1000 (seals 1-2,3-4; 5-6 maybe active), then advance past the
    // TTL and write more so the floor advances past the expired prefix.
    for i in 1..=6u64 {
        engine
            .write("ttl", one(json!({ "i": i }), None), true)
            .unwrap();
    }
    clock.advance(5_000); // now 6000: the first six (ts≈1000) are all expired.
    for i in 7..=8u64 {
        engine
            .write("ttl", one(json!({ "i": i }), None), true)
            .unwrap();
    }

    let st = engine.topic_state("ttl", false).unwrap();
    assert_eq!(st.head_seq, 8);
    assert_eq!(st.earliest_seq, 7, "seqs 1..=6 expired");
    assert_eq!(st.count, 2);

    // Sealed segments fully below earliest=7 (1-2,3-4,5-6) were dropped whole.
    assert!(
        count_segment_files(dir.path()) <= 2,
        "expired sealed segments reclaimed (got {})",
        count_segment_files(dir.path())
    );

    // A cursor below the TTL floor ⇒ ttl tombstone.
    let d = engine.diff("ttl", diff_from(0)).unwrap();
    let tomb = d.tombstone.expect("ttl tombstone after segment drop");
    assert_eq!(tomb.reason, TombstoneReason::Ttl);
    assert_eq!(
        d.records.iter().map(|r| r.seq).collect::<Vec<_>>(),
        vec![7, 8]
    );
}

/// A `before_seq` (prefix) delete that clears whole sealed segments reclaims them
/// silently — a voluntary delete advances `earliest_seq` but never `evict_floor`,
/// so reading across the gap returns `tombstone: null` even though segment files
/// were physically dropped.
#[test]
fn prefix_delete_reclaims_segments_silently() {
    let dir = tempfile::tempdir().unwrap();
    let clock: SharedClock = Arc::new(SystemClock);
    let engine = engine_with_segment(dir.path(), seg_cfg(2, 0, 0), clock);
    engine.put_topic("d", durable_topic()).unwrap();
    for i in 1..=8u64 {
        engine
            .write("d", one(json!({ "i": i }), None), true)
            .unwrap();
    }
    // 8 records / 2-per-segment → 3 sealed (1-2,3-4,5-6); seq 7,8 active-ish.
    let before = count_segment_files(dir.path());
    assert!(
        before >= 3,
        "several sealed segments before delete (got {before})"
    );

    // Delete everything below seq 7 (a prefix), clearing segments 1-2,3-4,5-6.
    let r = engine
        .delete(
            "d",
            DeleteRequest {
                before_seq: Some(7),
                match_: None,
            },
        )
        .unwrap();
    assert_eq!(r.deleted, 6);
    assert_eq!(r.earliest_seq, 7);

    // The fully-deleted sealed segments were physically reclaimed (silent).
    assert!(
        count_segment_files(dir.path()) < before,
        "prefix delete dropped whole sealed segments"
    );

    let d = engine.diff("d", diff_from(0)).unwrap();
    assert!(
        d.tombstone.is_none(),
        "voluntary delete stays silent despite segment drop"
    );
    assert_eq!(
        d.records.iter().map(|r| r.seq).collect::<Vec<_>>(),
        vec![7, 8]
    );
    assert_eq!(engine.topic_state("d", false).unwrap().count, 2);
}

/// A `match` delete that clears an **interior** sealed segment (every record in it
/// point-deleted) reclaims that whole segment silently, while earlier and later
/// live segments are untouched. Exercises `range_all_dead` interior reclaim.
#[test]
fn interior_match_delete_reclaims_its_segment_silently() {
    let dir = tempfile::tempdir().unwrap();
    let clock: SharedClock = Arc::new(SystemClock);
    let engine = engine_with_segment(dir.path(), seg_cfg(2, 0, 0), clock);
    engine.put_topic("m", durable_topic()).unwrap();
    // Tag seqs 3 and 4 (one whole sealed segment) "mid"; the rest "keep".
    for i in 1..=8u64 {
        let tag = if i == 3 || i == 4 { "mid" } else { "keep" };
        engine
            .write("m", one(json!({ "i": i }), Some(tag)), true)
            .unwrap();
    }
    let before = count_segment_files(dir.path());
    assert!(
        before >= 3,
        "segments sealed before the match delete (got {before})"
    );

    // Delete every "mid" record (seqs 3,4) — the whole interior segment 3-4.
    let r = engine
        .delete(
            "m",
            DeleteRequest {
                before_seq: None,
                match_: Some(Filter::from_shorthand("mid")),
            },
        )
        .unwrap();
    assert_eq!(r.deleted, 2);

    // The interior segment 3-4 was reclaimed; the front stays (1-2 still live), so
    // earliest_seq does NOT advance — the reclaim is purely the interior file drop.
    assert!(
        count_segment_files(dir.path()) < before,
        "interior fully-deleted segment dropped whole"
    );
    let st = engine.topic_state("m", false).unwrap();
    assert_eq!(st.earliest_seq, 1, "front still live; floor unchanged");
    assert_eq!(st.count, 6);

    let d = engine.diff("m", diff_from(0)).unwrap();
    assert!(d.tombstone.is_none(), "match delete stays silent");
    assert_eq!(
        d.records.iter().map(|r| r.seq).collect::<Vec<_>>(),
        vec![1, 2, 5, 6, 7, 8],
        "seqs 3,4 (interior segment) gone; the rest read back"
    );
    for r in &d.records {
        assert_eq!(r.data, json!({ "i": r.seq }));
    }
}

/// Segment reclaim survives a restart and is idempotent: after cap eviction drops
/// whole segments, a restart rebuilds the exact same head/earliest/count and does
/// not resurrect the reclaimed records, and the dropped segment files stay gone.
#[test]
fn reclaimed_segments_stay_gone_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let clock: SharedClock = Arc::new(SystemClock);
        let engine = engine_with_segment(dir.path(), seg_cfg(2, 0, 0), clock);
        engine
            .put_topic(
                "cap",
                TopicConfig {
                    cap_records: 4,
                    durable: true,
                    ..TopicConfig::default()
                },
            )
            .unwrap();
        for i in 1..=12u64 {
            engine
                .write("cap", one(json!({ "i": i }), None), true)
                .unwrap();
        }
        // Snapshot so the reclaimed prefix is also dropped from the WAL (the
        // checkpoint absorbs it), proving recovery does not resurrect it.
        assert!(engine.write_snapshot().unwrap());
    }
    // Reopen: state rebuilt across the surviving segments + WAL tail.
    let clock: SharedClock = Arc::new(SystemClock);
    let engine = engine_with_segment(dir.path(), seg_cfg(2, 0, 0), clock);
    let st = engine.topic_state("cap", false).unwrap();
    assert_eq!(st.head_seq, 12, "head recovered");
    assert_eq!(
        st.earliest_seq, 9,
        "cap floor recovered; reclaimed prefix not resurrected"
    );
    assert_eq!(st.count, 4);

    let d = engine.diff("cap", diff_from(0)).unwrap();
    let tomb = d
        .tombstone
        .expect("cap tombstone after restart (no silent loss)");
    assert_eq!(tomb.reason, TombstoneReason::Cap);
    assert_eq!(
        d.records.iter().map(|r| r.seq).collect::<Vec<_>>(),
        vec![9, 10, 11, 12]
    );
    assert_eq!(d.records[0].data, json!({ "i": 9 }));
}

/// Restart rebuilds the full state across HOT + COLD segments AND a reclaimed
/// prefix: relocate the oldest segments to cold, evict part of the live set via a
/// prefix delete, restart, and confirm head/earliest/count/config/records all
/// match — across tiers and after reclaim, with no acked-write loss.
#[test]
fn restart_rebuilds_across_hot_cold_and_reclaim() {
    let dir = tempfile::tempdir().unwrap();
    let cold = tempfile::tempdir().unwrap();
    {
        let clock: SharedClock = Arc::new(SystemClock);
        let engine = engine_with_cold(dir.path(), cold.path(), seg_cfg_retain(2, 1), clock);
        engine.put_topic("logs", durable_topic()).unwrap();
        for i in 1..=10u64 {
            engine
                .write("logs", one(json!({ "i": i }), Some(&format!("t{i}"))), true)
                .unwrap();
        }
        // Relocate older sealed segments to cold (keep newest 1 sealed hot).
        let n = engine.relocate_topic_cold("logs");
        assert!(n >= 1, "some segments relocated to cold");
        assert!(count_tier_segments(cold.path()) >= 1);
        // Voluntary prefix delete of seqs < 3 — drops a whole (cold) segment file.
        engine
            .delete(
                "logs",
                DeleteRequest {
                    before_seq: Some(3),
                    match_: None,
                },
            )
            .unwrap();
        assert!(engine.write_snapshot().unwrap());
    }
    // Reopen the SAME hot+cold dirs.
    let clock: SharedClock = Arc::new(SystemClock);
    let engine = engine_with_cold(dir.path(), cold.path(), seg_cfg_retain(2, 1), clock);
    let st = engine.topic_state("logs", false).unwrap();
    assert_eq!(st.head_seq, 10, "head recovered across tiers");
    assert_eq!(st.earliest_seq, 3, "prefix delete recovered (silent)");
    assert_eq!(st.count, 8);
    assert!(st.config.durable);

    let d = engine
        .diff(
            "logs",
            DiffRequest {
                from_seq: 0,
                limit: 1000,
                include_tags: true,
                ..DiffRequest::default()
            },
        )
        .unwrap();
    assert!(
        d.tombstone.is_none(),
        "deleted prefix stays silent after restart"
    );
    assert_eq!(
        d.records.iter().map(|r| r.seq).collect::<Vec<_>>(),
        (3..=10).collect::<Vec<_>>()
    );
    assert_eq!(d.records[0].data, json!({ "i": 3 }));
    assert_eq!(d.records[0].tag.as_deref(), Some("t3"));
    assert_eq!(d.records.last().unwrap().data, json!({ "i": 10 }));
}

/// Cap/TTL/delete reclaim drops a whole segment **in either tier** (ARCHITECTURE
/// §3.3): relocate the oldest segments to cold, then overflow the cap so those
/// cold segments fall below the live floor — the cold `.data`/`.idx` objects are
/// physically reclaimed, and a cursor below the floor still tombstones.
#[test]
fn cap_eviction_reclaims_a_relocated_cold_segment() {
    let dir = tempfile::tempdir().unwrap();
    let cold = tempfile::tempdir().unwrap();
    let clock: SharedClock = Arc::new(SystemClock);
    // Seal every 2 records; keep newest 1 sealed hot; cap at 6 live records (so
    // the first 6 writes don't evict — we relocate first, then overflow).
    let engine = engine_with_cold(dir.path(), cold.path(), seg_cfg_retain(2, 1), clock);
    engine
        .put_topic(
            "cap",
            TopicConfig {
                cap_records: 6,
                durable: true,
                ..TopicConfig::default()
            },
        )
        .unwrap();

    // Write 6 (all within cap=6) → seals 1-2,3-4 (5-6 active). Relocate the oldest.
    for i in 1..=6u64 {
        engine
            .write("cap", one(json!({ "i": i }), None), true)
            .unwrap();
    }
    let relocated = engine.relocate_topic_cold("cap");
    assert!(relocated >= 1, "at least one segment relocated to cold");
    let cold_before = count_tier_segments(cold.path());
    assert!(
        cold_before >= 1,
        "a segment is now in cold (got {cold_before})"
    );

    // Overflow the cap so the cold segments fall fully below the live floor.
    for i in 7..=14u64 {
        engine
            .write("cap", one(json!({ "i": i }), None), true)
            .unwrap();
    }
    let st = engine.topic_state("cap", false).unwrap();
    assert_eq!(st.head_seq, 14);
    assert_eq!(st.earliest_seq, 9, "cap=6 keeps the newest 6 live");

    // The relocated cold segments below the floor were physically reclaimed.
    assert!(
        count_tier_segments(cold.path()) < cold_before,
        "a cold segment object below the live floor was dropped"
    );

    // Still no silent involuntary loss: a cursor below evict_floor tombstones.
    let d = engine
        .diff(
            "cap",
            DiffRequest {
                from_seq: 0,
                limit: 1000,
                ..DiffRequest::default()
            },
        )
        .unwrap();
    let tomb = d
        .tombstone
        .expect("cap tombstone after cold-segment reclaim");
    assert_eq!(tomb.reason, TombstoneReason::Cap);
    assert_eq!(
        d.records.iter().map(|r| r.seq).collect::<Vec<_>>(),
        (9..=14).collect::<Vec<_>>()
    );
}

// ===========================================================================
// Phase-7 Stage-1: the three critical durability fixes.
// ===========================================================================

/// A forwarded copy into a destination topic survives a restart and is present
/// EXACTLY ONCE with $node/$tag fidelity. This is the observable recovery contract
/// the product guarantees. Recovery RE-DERIVES the copy from the still-retained
/// source record + the durable per-router cursor; forwarded copies are never
/// separately WAL-logged.
#[test]
fn forwarded_copy_into_durable_dest_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = engine_at(dir.path());
        // Both source and dest durable; preserve $node/$tag across the forward.
        engine.put_topic("src", durable_topic()).unwrap();
        engine.put_topic("dst", durable_topic()).unwrap();
        engine
            .put_router(
                "src->dst",
                RouterCreateRequest {
                    source: "src".into(),
                    dest: "dst".into(),
                    preserve_node: true,
                    preserve_tag: true,
                    create_dest: false,
                    filter: None,
                    allow_cycle: false,
                    guarantee: Default::default(),
                },
            )
            .unwrap();
        // A durable write to src is forwarded (durably) into dst.
        engine
            .write(
                "src",
                WriteRequest {
                    records: vec![RecordIn {
                        data: json!({ "fwd": 1 }),
                        tag: Some("ftag".into()),
                        node: Some("writerA".into()),
                        meta: None,
                    }],
                    node: None,
                    idempotency_key: None,
                    create: None,
                    config: None,
                    disable_backpressure: false,
                },
                true,
            )
            .unwrap();
        // The forwarded copy is visible in dst before restart.
        let pre = engine.diff("dst", diff_from(0)).unwrap();
        assert_eq!(pre.records.len(), 1, "forwarded copy present pre-restart");
        assert_eq!(pre.records[0].data, json!({ "fwd": 1 }));
        // Drop → WAL drains + fsyncs (the forwarded Append frame is on disk).
    }

    // Reopen: the forwarded copy is recovered from the WAL (durable by
    // construction), present exactly once with $node/$tag preserved.
    let engine = engine_at(dir.path());
    let st = engine.topic_state("dst", false).unwrap();
    assert_eq!(st.head_seq, 1, "forwarded durable copy recovered");
    assert_eq!(st.count, 1);
    let d = engine.diff("dst", diff_from(0)).unwrap();
    assert_eq!(
        d.records.len(),
        1,
        "forwarded copy present exactly once after restart"
    );
    assert_eq!(d.records[0].data, json!({ "fwd": 1 }));
    assert_eq!(d.records[0].tag.as_deref(), Some("ftag"), "$tag preserved");
    assert_eq!(
        d.records[0].node.as_deref(),
        Some("writerA"),
        "$node preserved"
    );

    // And forwarding still works for a NEW write after restart.
    engine
        .write("src", one(json!({ "fwd": 2 }), Some("ftag2")), true)
        .unwrap();
    let d2 = engine.diff("dst", diff_from(0)).unwrap();
    assert_eq!(
        d2.records
            .iter()
            .map(|r| r.data.clone())
            .collect::<Vec<_>>(),
        vec![json!({ "fwd": 1 }), json!({ "fwd": 2 })],
        "post-restart forward appends after the recovered copy (no duplicate)"
    );
}

/// Bug #2 (visible-before-durable): a durable write that the engine acknowledges
/// is always already on disk — the headline WAL-first guarantee, asserted as a
/// regression for the reservation reorder (stage → WAL → fsync → publish). A
/// reader never sees a record that has not become durable. (The fsync-failure
/// roll-back path is exercised implicitly; here we assert the happy-path
/// invariant that an acked durable write is recovered, with a real fsync_ms.)
#[test]
fn acked_durable_write_is_recovered_and_fsync_gated() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = engine_at(dir.path());
        engine.put_topic("d", durable_topic()).unwrap();
        let resp = engine
            .write("d", one(json!({ "n": 1 }), None), true)
            .unwrap();
        // The ack came back only after the fsync (WAL-first publish).
        assert!(resp.performance.fsync_ms.unwrap_or(0.0) > 0.0);
        // And it is immediately visible to a reader (published post-fsync).
        assert_eq!(engine.diff("d", diff_from(0)).unwrap().records.len(), 1);
    }
    let engine = engine_at(dir.path());
    assert_eq!(engine.topic_state("d", false).unwrap().head_seq, 1);
    assert_eq!(
        engine.diff("d", diff_from(0)).unwrap().records[0].data,
        json!({ "n": 1 })
    );
}

// ===========================================================================
// Durability commit classes (ephemeral / memory / disk / fsync) — Stage 2.
//
// A topic's `durability` selects where its records land and when an ack returns:
//   - ephemeral: resident-only records; config survives, records are queryable while
//     the process is running and intentionally lost on restart.
//   - memory: "disk-like but best-effort" — takes the SAME group-committed WAL
//     write + recovery path as disk (fully queryable), but with NO durability
//     GUARANTEE: records MAY survive a restart OR be lost; recovery is best-effort.
//   - disk:   group-committed WAL (today's durable:false); survives a clean
//     restart minus the un-fsynced tail.
//   - fsync:  fsync-gated ack (today's durable:true); survives any crash.
// ===========================================================================

/// A topic of the given durability class (queue/log defaults otherwise).
fn class_topic(class: Durability) -> TopicConfig {
    TopicConfig {
        durability: Some(class),
        ..TopicConfig::default()
    }
}

/// A `memory`-class topic is "disk-like but best-effort" (§0.10): it takes the same
/// group-committed WAL write + recovery path as `disk`, is fully queryable, and is
/// never fsync-gated (`fsync_ms == 0`). The WEAK contract: its records MAY survive
/// a restart OR be lost (no fabrication, seq monotone) — so this test asserts only
/// what is guaranteed (config survives; recovered records are a no-fabrication
/// subset of what was written; head never exceeds the acked head), NOT an exact
/// empty-on-restart nor an exact full-survival.
#[test]
fn memory_topic_best_effort_recovery_no_fabrication() {
    let dir = tempfile::tempdir().unwrap();
    let written: Vec<i64> = (1..=5).collect();
    {
        let engine = engine_at(dir.path());
        // A memory topic with a non-default field so we can prove the CONFIG persists.
        engine
            .put_topic(
                "cache",
                TopicConfig {
                    durability: Some(Durability::Memory),
                    cap_records: 99,
                    ..TopicConfig::default()
                },
            )
            .unwrap();
        for &i in &written {
            let resp = engine
                .write("cache", one(json!({ "i": i }), None), true)
                .unwrap();
            // Memory is never fsync-gated (it shares the disk-like path but with no
            // durability guarantee): zero fsync time.
            assert_eq!(
                resp.performance.fsync_ms,
                Some(0.0),
                "memory write is never fsync-gated"
            );
        }
        // Pre-restart the records are present and fully queryable.
        let st = engine.topic_state("cache", false).unwrap();
        assert_eq!(st.head_seq, 5);
        assert_eq!(st.count, 5);
        assert_eq!(
            engine.diff("cache", diff_from(0)).unwrap().records.len(),
            5,
            "memory topic is fully queryable pre-restart"
        );
        assert!(engine.write_snapshot().unwrap());
    }

    // Restart: the topic CONFIG always survives (a control-frame mutation). The
    // records MAY survive OR be lost (best-effort) — assert only the weak contract.
    let engine = engine_at(dir.path());
    let st = engine.topic_state("cache", false).unwrap();
    assert_eq!(
        st.config.durability,
        Some(Durability::Memory),
        "config (durability) survives"
    );
    assert_eq!(st.config.cap_records, 99, "config (cap_records) survives");
    assert!(
        !st.config.durable,
        "memory ⇒ durable:false (class != fsync)"
    );
    // head never exceeds the acked head (seq monotone, no future seq).
    assert!(
        st.head_seq <= 5,
        "recovered head {} must not exceed the acked head 5",
        st.head_seq
    );
    // NO FABRICATION: every recovered record is one that was actually written
    // (a `{"i": <1..=5>}` payload) — memory may lose records but never invents them.
    let d = engine.diff("cache", diff_from(0)).unwrap();
    for r in &d.records {
        let i = r.data.get("i").and_then(|v| v.as_i64());
        assert!(
            i.map(|v| written.contains(&v)).unwrap_or(false),
            "recovered record {:?} was never written (fabricated)",
            r.data
        );
    }

    // The topic is fully functional post-restart: a fresh write is accepted and
    // queryable (regardless of whether the prior records survived).
    let before = engine.topic_state("cache", false).unwrap().head_seq;
    engine
        .write("cache", one(json!({ "i": 100 }), None), true)
        .unwrap();
    assert_eq!(
        engine.topic_state("cache", false).unwrap().head_seq,
        before + 1,
        "a post-restart write advances head by 1"
    );
}

/// An `ephemeral` topic is the explicit RAM-only class: records are fully queryable
/// before restart, sequence numbers increase monotonically while live, and only the
/// config survives restart.
#[test]
fn ephemeral_topic_records_are_ram_only_and_live_seq_monotone() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = engine_at(dir.path());
        engine
            .put_topic(
                "scratch",
                TopicConfig {
                    durability: Some(Durability::Ephemeral),
                    cap_records: 99,
                    ..TopicConfig::default()
                },
            )
            .unwrap();
        for i in 1..=5 {
            let resp = engine
                .write("scratch", one(json!({ "i": i }), None), true)
                .unwrap();
            assert_eq!(
                resp.last_seq, i as u64,
                "ephemeral seqs are monotone while live"
            );
            assert_eq!(
                resp.performance.fsync_ms,
                Some(0.0),
                "ephemeral is never fsync-gated"
            );
        }
        let st = engine.topic_state("scratch", false).unwrap();
        assert_eq!(st.head_seq, 5);
        assert_eq!(st.count, 5);
        assert_eq!(
            engine.diff("scratch", diff_from(0)).unwrap().records.len(),
            5,
            "ephemeral topic is fully queryable pre-restart"
        );
        assert!(engine.write_snapshot().unwrap());
    }

    let engine = engine_at(dir.path());
    let st = engine.topic_state("scratch", false).unwrap();
    assert_eq!(st.config.durability, Some(Durability::Ephemeral));
    assert_eq!(st.config.cap_records, 99, "config survives");
    assert_eq!(
        st.head_seq, 5,
        "ephemeral snapshot preserves the head without recovering payloads"
    );
    assert_eq!(st.count, 0, "ephemeral records are intentionally lost");
    assert!(
        engine
            .diff("scratch", diff_from(0))
            .unwrap()
            .records
            .is_empty(),
        "ephemeral restart is an empty topic shell"
    );
    let resp = engine
        .write("scratch", one(json!({ "i": 100 }), None), true)
        .unwrap();
    assert_eq!(
        resp.last_seq, 6,
        "post-snapshot ephemeral seqs do not regress"
    );
}

/// A `disk`-class topic (durable:false) survives a CLEAN restart (the WAL
/// writer drains + fsyncs on a clean teardown). It reports `fsync_ms == 0` (no
/// per-write fsync), and resolves to `durable:false`.
#[test]
fn disk_topic_survives_clean_restart_and_is_not_fsync_gated() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = engine_at(dir.path());
        engine
            .put_topic("evts", class_topic(Durability::Disk))
            .unwrap();
        for i in 1..=4 {
            let resp = engine
                .write("evts", one(json!({ "i": i }), None), true)
                .unwrap();
            // Disk is group-committed: the ack is not fsync-gated.
            assert_eq!(
                resp.performance.fsync_ms,
                Some(0.0),
                "disk write is not fsync-gated"
            );
        }
        let st = engine.topic_state("evts", false).unwrap();
        assert_eq!(st.config.durability, Some(Durability::Disk));
        assert!(!st.config.durable, "disk ⇒ durable:false");
    }
    // Clean teardown drained + fsynced the WAL ⇒ the disk records are recovered.
    let engine = engine_at(dir.path());
    let st = engine.topic_state("evts", false).unwrap();
    // All 4 acked records survive (exact `count`). For a `disk` topic `head_seq` may
    // sit at the durable head RESERVATION (R3: a head ceiling fsynced ahead of use
    // to prevent seq reuse across a power loss) — `>= 4` and within the reservation
    // block; the reserved-but-unwritten seqs are silent gaps.
    assert_eq!(st.count, 4, "disk writes survive a clean restart");
    assert!(
        st.head_seq >= 4,
        "head never below the acked head, got {}",
        st.head_seq
    );
    assert!(
        st.head_seq <= 4 + topics::config::DISK_HEAD_RESERVE_AHEAD,
        "head within the reservation block, got {}",
        st.head_seq
    );
    assert_eq!(
        st.config.durability,
        Some(Durability::Disk),
        "class persisted"
    );
}

/// `durability_class` resolution (API §0.10): explicit `durability` wins; absent
/// it derives from `durable` (true⇒fsync, false⇒disk); and the response always
/// reports the resolved class with `durable == (class == fsync)`.
#[test]
fn durability_resolution_and_reporting() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path());

    // durable:true (no `durability`) ⇒ resolves to fsync.
    engine
        .put_topic(
            "a",
            TopicConfig {
                durable: true,
                ..TopicConfig::default()
            },
        )
        .unwrap();
    let a = engine.topic_state("a", false).unwrap().config;
    assert_eq!(a.durability, Some(Durability::Fsync));
    assert!(a.durable);

    // durable:false (no `durability`) ⇒ resolves to disk.
    engine.put_topic("b", TopicConfig::default()).unwrap();
    let b = engine.topic_state("b", false).unwrap().config;
    assert_eq!(b.durability, Some(Durability::Disk));
    assert!(!b.durable);

    // Explicit `durability` WINS over a conflicting `durable` bool.
    engine
        .put_topic(
            "c",
            TopicConfig {
                durable: true,
                durability: Some(Durability::Disk),
                ..TopicConfig::default()
            },
        )
        .unwrap();
    let c = engine.topic_state("c", false).unwrap().config;
    assert_eq!(
        c.durability,
        Some(Durability::Disk),
        "explicit durability wins"
    );
    assert!(!c.durable, "durable normalized to match the resolved class");

    // Explicit memory ⇒ durable:false; is_durable()==false.
    engine
        .put_topic("d", class_topic(Durability::Memory))
        .unwrap();
    let d = engine.topic_state("d", false).unwrap().config;
    assert_eq!(d.durability, Some(Durability::Memory));
    assert!(!d.durable);

    // Explicit ephemeral ⇒ durable:false; is_durable()==false.
    engine
        .put_topic("e", class_topic(Durability::Ephemeral))
        .unwrap();
    let e = engine.topic_state("e", false).unwrap().config;
    assert_eq!(e.durability, Some(Durability::Ephemeral));
    assert!(!e.durable);
}

/// A router forwarding into a `memory` destination takes the same disk-like
/// best-effort path (§0.10): the forwarded copy is live + queryable in the dest,
/// and the dest's config always survives a restart. Its records MAY survive OR be
/// lost (best-effort, no fabrication); the (durable) SOURCE always recovers fully.
#[test]
fn router_into_memory_dest_best_effort() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = engine_at(dir.path());
        // A durable source; a memory destination topic.
        engine.put_topic("src", durable_topic()).unwrap();
        engine
            .put_topic("mem_dst", class_topic(Durability::Memory))
            .unwrap();
        engine
            .put_router(
                "src->mem_dst",
                RouterCreateRequest {
                    source: "src".into(),
                    dest: "mem_dst".into(),
                    preserve_node: true,
                    preserve_tag: true,
                    create_dest: false, // dest already exists as a memory topic.
                    filter: None,
                    allow_cycle: false,
                    guarantee: Default::default(),
                },
            )
            .unwrap();
        engine
            .write("src", one(json!({ "x": 1 }), None), true)
            .unwrap();
        // The forward landed and is queryable: the memory dest has the copy now.
        assert_eq!(
            engine.diff("mem_dst", diff_from(0)).unwrap().records.len(),
            1,
            "forwarded copy is live + queryable in the memory dest"
        );
        assert!(engine.write_snapshot().unwrap());
    }
    // Restart: the durable source always recovers fully; the memory dest config
    // always survives. Its records are best-effort — assert only the weak contract.
    let engine = engine_at(dir.path());
    assert_eq!(
        engine.topic_state("src", false).unwrap().head_seq,
        1,
        "durable source survives"
    );
    let dst = engine.topic_state("mem_dst", false).unwrap();
    assert_eq!(
        dst.config.durability,
        Some(Durability::Memory),
        "memory dest config survives"
    );
    // head never exceeds the acked head; recovered records (if any) are not
    // fabricated — they are exactly the forwarded `{"x": 1}` copy.
    assert!(dst.head_seq <= 1, "memory dest head monotone");
    for r in &engine.diff("mem_dst", diff_from(0)).unwrap().records {
        assert_eq!(
            r.data.get("x").and_then(|v| v.as_i64()),
            Some(1),
            "recovered forwarded record is not fabricated"
        );
    }
}
