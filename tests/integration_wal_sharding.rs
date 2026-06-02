//! WAL sharding (TOPICS_WAL_SHARDS) integration tests: a multi-shard engine
//! round-trips writes and recovers them; a topic always lands in exactly ONE shard
//! within a run; recovery with a DIFFERENT shard count than was written still
//! recovers ALL data (the shard-count-agnostic-replay property); and a stalled /
//! failed shard is isolated from healthy ones.
//!
//! These drive the engine directly (no HTTP) with a unique `tempfile::tempdir`
//! per test. The shard count is set on the engine's `ServerConfig.wal_shards`,
//! since the env-var default is process-global; the field is the same knob
//! `TOPICS_WAL_SHARDS` populates.

use std::collections::HashSet;
use std::sync::Arc;

use serde_json::json;
use topics::clock::{SharedClock, SystemClock};
use topics::config::ServerConfig;
use topics::engine::Engine;
use topics::storage::{shard_for_topic, WalReader};
use topics::types::*;

/// A `ServerConfig` pointed at `dir` with `shards` WAL shards.
fn config_at(dir: &std::path::Path, shards: usize) -> ServerConfig {
    ServerConfig {
        data_dir: Some(dir.to_string_lossy().into_owned()),
        wal_shards: shards,
        ..ServerConfig::default()
    }
}

fn engine_at(dir: &std::path::Path, shards: usize) -> Arc<Engine> {
    let clock: SharedClock = Arc::new(SystemClock);
    Engine::with_data_dir(config_at(dir, shards), clock).expect("open durable engine")
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

/// Multi-shard round-trip + recovery: write to several topics spread across shards,
/// drop the engine (drains + fsyncs every shard), reopen with the SAME shard
/// count, and verify every topic's records survive.
#[test]
fn multi_shard_round_trips_and_recovers() {
    let dir = tempfile::tempdir().unwrap();
    let topics = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta"];

    {
        let engine = engine_at(dir.path(), 4);
        for b in topics {
            engine.put_topic(b, durable_topic()).unwrap();
            for i in 1..=10 {
                engine
                    .write(b, one(json!({ "b": b, "i": i }), Some("t")), true)
                    .unwrap();
            }
        }
        // Drop → every shard writer drains + fsyncs + joins.
    }

    let engine = engine_at(dir.path(), 4);
    for b in topics {
        let st = engine.topic_state(b, false).unwrap();
        assert_eq!(st.head_seq, 10, "topic {b}: all 10 writes recovered");
        assert_eq!(st.count, 10, "topic {b}: count recovered");
        let d = engine.diff(b, diff_from(0)).unwrap();
        let seqs: Vec<u64> = d.records.iter().map(|r| r.seq).collect();
        assert_eq!(
            seqs,
            (1..=10).collect::<Vec<_>>(),
            "topic {b}: contiguous seqs"
        );
        assert_eq!(d.records[0].data, json!({ "b": b, "i": 1 }));
    }
}

/// A topic's frames all land in ONE shard within a run: with N>1 shards, the
/// per-shard WAL subdirs that contain a given topic's `Append` frames number exactly
/// one. We write to several topics, then scan every shard's WAL files and confirm
/// each topic id appears in exactly one shard — and that shard matches the routing
/// hash `shard_for_topic(topic_id, N)`.
#[test]
fn a_topic_lands_in_exactly_one_shard() {
    let dir = tempfile::tempdir().unwrap();
    let n = 4usize;
    let names = [
        "one", "two", "three", "four", "five", "six", "seven", "eight",
    ];

    let mut topic_ids = std::collections::HashMap::new();
    {
        let engine = engine_at(dir.path(), n);
        for name in names {
            engine.put_topic(name, durable_topic()).unwrap();
            for i in 1..=5 {
                engine
                    .write(name, one(json!({ "i": i }), None), true)
                    .unwrap();
            }
            // Capture the interned topic id for the routing assertion.
            let id = engine.get_topic(name).unwrap().topic_id;
            topic_ids.insert(name.to_string(), id);
        }
    }

    // For each shard subdir, collect the set of topic ids that have an Append frame.
    let wal_dir = dir.path().join("wal");
    let mut shard_of_topic: std::collections::HashMap<u32, Vec<usize>> =
        std::collections::HashMap::new();
    for s in 0..n {
        let sub = wal_dir.join(format!("shard-{s:02}"));
        if !sub.exists() {
            continue;
        }
        let mut files: Vec<_> = std::fs::read_dir(&sub)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with("wal-") && n.ends_with(".log"))
                    .unwrap_or(false)
            })
            .collect();
        files.sort();
        let mut seen: HashSet<u32> = HashSet::new();
        for f in files {
            for frame in WalReader::open(&f).unwrap() {
                if let WalRecordKind::Append(topic_id) = classify(&frame) {
                    seen.insert(topic_id);
                }
            }
        }
        for id in seen {
            shard_of_topic.entry(id).or_default().push(s);
        }
    }

    // Every topic's Append frames appear in exactly ONE shard, and that shard is the
    // one the routing hash selects.
    for (name, id) in &topic_ids {
        let shards = shard_of_topic.get(id).cloned().unwrap_or_default();
        assert_eq!(
            shards.len(),
            1,
            "topic {name} (id {id}) must land in exactly one shard, got {shards:?}"
        );
        assert_eq!(
            shards[0],
            shard_for_topic(*id, n),
            "topic {name} (id {id}) lands in the hash-routed shard"
        );
    }
}

/// The shard-count-agnostic-replay property: a data dir WRITTEN with one shard
/// count recovers ALL data when REOPENED with a DIFFERENT shard count. We exercise
/// several reconfigurations (8→1, 1→8, 4→3, …) so a topic that routed to one shard
/// at write time is dispatched by topic_id on replay regardless of the new count.
#[test]
fn recovery_is_shard_count_agnostic() {
    for (write_shards, read_shards) in [(8usize, 1usize), (1, 8), (4, 3), (3, 7), (6, 2)] {
        let dir = tempfile::tempdir().unwrap();
        let topics = ["a", "bb", "ccc", "dddd", "eeeee"];

        {
            let engine = engine_at(dir.path(), write_shards);
            for b in topics {
                engine.put_topic(b, durable_topic()).unwrap();
                for i in 1..=7 {
                    engine
                        .write(b, one(json!({ "b": b, "i": i }), Some("tag")), true)
                        .unwrap();
                }
            }
        }

        // Reopen with a DIFFERENT shard count — all data must still recover.
        let engine = engine_at(dir.path(), read_shards);
        for b in topics {
            let st = engine
                .topic_state(b, false)
                .unwrap_or_else(|_| panic!("topic {b} recovered ({write_shards}->{read_shards})"));
            assert_eq!(
                st.head_seq, 7,
                "topic {b}: all writes recovered ({write_shards}->{read_shards})"
            );
            assert_eq!(
                st.count, 7,
                "topic {b}: count ({write_shards}->{read_shards})"
            );
            let d = engine.diff(b, diff_from(0)).unwrap();
            let seqs: Vec<u64> = d.records.iter().map(|r| r.seq).collect();
            assert_eq!(
                seqs,
                (1..=7).collect::<Vec<_>>(),
                "topic {b}: contiguous seqs ({write_shards}->{read_shards})"
            );
        }

        // And a further write after the reconfigured reopen lands cleanly + recovers
        // again (the resumed shard writers append after the recovered tails).
        engine
            .write("a", one(json!({ "post": true }), None), true)
            .unwrap();
        let st = engine.topic_state("a", false).unwrap();
        assert_eq!(
            st.head_seq, 8,
            "post-reopen write acked ({write_shards}->{read_shards})"
        );
        drop(engine);

        let engine = engine_at(dir.path(), read_shards);
        let st = engine.topic_state("a", false).unwrap();
        assert_eq!(
            st.head_seq, 8,
            "post-reopen write survives a second restart ({write_shards}->{read_shards})"
        );
    }
}

/// Per-shard failure isolation: a single WAL shard is exercised with many
/// concurrent durable writers across topics that spread over all shards; the writes
/// to healthy shards succeed and survive a restart even though topics are
/// independent per-shard streams (no shard blocks another). This is the positive
/// side of isolation — the negative (a stalled shard's `Full` does not stall
/// others) is covered by the unit test in `sharded_wal`.
#[test]
fn shards_are_independent_under_concurrent_load() {
    let dir = tempfile::tempdir().unwrap();
    let n = 4usize;
    // Enough distinct topics that, with 4 shards, every shard gets at least one topic.
    let names: Vec<String> = (0..16).map(|i| format!("topic-{i}")).collect();

    {
        let engine = engine_at(dir.path(), n);
        for name in &names {
            engine.put_topic(name, durable_topic()).unwrap();
        }
        let mut handles = Vec::new();
        for name in &names {
            let engine = engine.clone();
            let name = name.clone();
            handles.push(std::thread::spawn(move || {
                for i in 0..50u64 {
                    engine
                        .write(&name, one(json!({ "i": i }), None), true)
                        .unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        for name in &names {
            let st = engine.topic_state(name, false).unwrap();
            assert_eq!(st.head_seq, 50, "topic {name}: all writes acked");
        }
    }

    // Confirm every shard actually received at least one topic's writes (the load
    // really was spread, so isolation is meaningful) AND everything recovered.
    let used_shards: HashSet<usize> = (0..names.len())
        .map(|i| shard_for_topic((i as u32) + 1, n)) // topic ids are 1..=16 in order
        .collect();
    assert!(
        used_shards.len() >= 2,
        "the workload must span multiple shards to test isolation (spanned {used_shards:?})"
    );

    let engine = engine_at(dir.path(), n);
    for name in &names {
        let st = engine.topic_state(name, false).unwrap();
        assert_eq!(st.head_seq, 50, "topic {name}: all writes recovered");
        assert_eq!(st.count, 50);
    }
}

/// shards=1 is exactly the legacy flat layout: the WAL files live directly under
/// `<data_dir>/wal/` (no `shard-NN/` subdir), so a single-shard run is on-disk
/// back-compatible with the pre-sharding engine.
#[test]
fn single_shard_uses_flat_legacy_layout() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = engine_at(dir.path(), 1);
        engine.put_topic("jobs", durable_topic()).unwrap();
        for i in 1..=3 {
            engine
                .write("jobs", one(json!({ "i": i }), None), true)
                .unwrap();
        }
    }
    let wal_dir = dir.path().join("wal");
    // A flat `wal-<idx>.log` exists directly under wal/.
    let flat: Vec<_> = std::fs::read_dir(&wal_dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.starts_with("wal-") && n.ends_with(".log"))
                .unwrap_or(false)
        })
        .collect();
    assert!(
        !flat.is_empty(),
        "single shard writes the flat wal-<idx>.log layout"
    );
    // No `shard-NN/` subdir was created.
    let has_shard_subdir = std::fs::read_dir(&wal_dir).unwrap().any(|e| {
        e.unwrap()
            .path()
            .file_name()
            .and_then(|n| n.to_str())
            .map(|n| n.starts_with("shard-"))
            .unwrap_or(false)
    });
    assert!(
        !has_shard_subdir,
        "single shard must NOT create a shard-NN subdir"
    );
}

/// codex P0 #3: a topic-scoped CONTROL frame (config update / delete / head
/// watermark) written under the NEW layout must not be lost or regressed when the
/// topic's older-layout frames replay in a different group. We write under 3 shards
/// with NO snapshot (so the old create+appends stay in the WAL), reopen under 7
/// shards — a NON-multiple reconfigure where some topics route to a LOWER shard idx
/// (ids 1/5/11/13 go old-shard-2 → new-shard-0/1), so their NEW group sorts BEFORE
/// their OLD group — apply a config update + a delete + more appends, then restart
/// again WITHOUT a fresh snapshot so the new control frames AND the old create frame
/// both replay. Without epoch-ordered replay the old create frame would replay AFTER
/// the new config-update frame and overwrite it (config regression).
#[test]
fn control_frames_survive_shard_count_reconfigure() {
    let dir = tempfile::tempdir().unwrap();
    // Many topics so several route old-shard > new-shard (the failing order); ids
    // 1/5/11/13 are the 3->7 inversions verified by the routing hash.
    let topics: Vec<String> = (0..24).map(|i| format!("ctrl-{i}")).collect();

    {
        let engine = engine_at(dir.path(), 3);
        for b in &topics {
            engine.put_topic(b, durable_topic()).unwrap();
            for i in 1..=10 {
                engine
                    .write(b, one(json!({ "i": i }), Some("t")), true)
                    .unwrap();
            }
        }
        // NO snapshot: the create+appends stay in the 3-shard WAL groups, so on the
        // 7-shard reopen they replay alongside the new control frames.
        drop(engine);
    }

    {
        // Reopen with 7 shards; apply control ops that log NEW frames in (for the
        // inverted topics) a lower-sorting group than the topic's old create frame.
        let engine = engine_at(dir.path(), 7);
        for b in &topics {
            // Config update (logs a TopicConfig update frame under the new layout).
            let mut cfg = durable_topic();
            cfg.cap_records = 1000;
            engine.put_topic(b, cfg).unwrap();
            // Delete the first 3 records (logs a Delete frame; bound to point-in-time).
            engine
                .delete(
                    b,
                    DeleteRequest {
                        before_seq: Some(4),
                        match_: None,
                    },
                )
                .unwrap();
            // More appends (new seqs, durable ⇒ also a HeadWatermark may ride along).
            for i in 11..=13 {
                engine
                    .write(b, one(json!({ "i": i }), Some("t")), true)
                    .unwrap();
            }
        }
        // No snapshot here — the new control frames live only in the WAL.
        drop(engine);
    }

    // Restart again (still 7 shards): the new control frames must replay correctly
    // relative to the old create frame. Then one more restart with 1 shard to
    // exercise a 7 -> 1 shrink over the same un-snapshotted control frames.
    for read_shards in [7usize, 1usize] {
        let engine = engine_at(dir.path(), read_shards);
        for b in &topics {
            let st = engine
                .topic_state(b, false)
                .unwrap_or_else(|_| panic!("topic {b} recovered (->{read_shards})"));
            assert_eq!(
                st.head_seq, 13,
                "topic {b}: head after appends (->{read_shards})"
            );
            // First 3 deleted ⇒ live count 13 - 3 = 10.
            assert_eq!(st.count, 10, "topic {b}: delete survived (->{read_shards})");
            let cfg = engine.get_topic(b).unwrap().config.read().clone();
            assert_eq!(
                cfg.cap_records, 1000,
                "topic {b}: config update survived (->{read_shards})"
            );
            let d = engine.diff(b, diff_from(0)).unwrap();
            let seqs: Vec<u64> = d.records.iter().map(|r| r.seq).collect();
            assert_eq!(
                seqs,
                (4..=13).collect::<Vec<_>>(),
                "topic {b}: deleted prefix gone, rest contiguous (->{read_shards})"
            );
        }
        drop(engine);
    }
}

/// codex P0 #1: a multi-shard snapshot's `shard-00/` checkpoint offset must NOT be
/// applied to the FLAT `wal/` group after reopening with shards=1. We snapshot
/// under 4 shards (records a `shard-00` offset), reopen with 1 shard (writes land in
/// the flat `wal/` group), append MORE durable records, drop WITHOUT a new snapshot,
/// then restart with 1 shard. If the old `shard-00` offset were applied to the flat
/// group, the new flat frames whose end-offset is below it would be skipped.
#[test]
fn flat_group_not_skipped_by_stale_shard_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let b = "jobs";
    {
        let engine = engine_at(dir.path(), 4);
        engine.put_topic(b, durable_topic()).unwrap();
        for i in 1..=5 {
            engine.write(b, one(json!({ "i": i }), None), true).unwrap();
        }
        engine.write_snapshot().unwrap(); // records a shard-NN checkpoint.
    }
    {
        // Reopen flat (1 shard): new writes go to wal/wal-*.log, NOT shard-NN.
        let engine = engine_at(dir.path(), 1);
        for i in 6..=9 {
            engine.write(b, one(json!({ "i": i }), None), true).unwrap();
        }
        // Crash before a new snapshot: the flat frames are only in the WAL.
        drop(engine);
    }
    let engine = engine_at(dir.path(), 1);
    let st = engine.topic_state(b, false).unwrap();
    assert_eq!(
        st.head_seq, 9,
        "flat-group frames not skipped by stale shard offset"
    );
    assert_eq!(st.count, 9);
    let d = engine.diff(b, diff_from(0)).unwrap();
    let seqs: Vec<u64> = d.records.iter().map(|r| r.seq).collect();
    assert_eq!(seqs, (1..=9).collect::<Vec<_>>());
}

/// codex P0 #2: if orphan WAL files from an absorbed prior-layout group survive
/// (orphan removal failed/bypassed), recovery must NOT replay them from zero and
/// regress snapshotted state. We snapshot under 4 shards, COPY the shard subdirs
/// aside (to resurrect them), reopen+snapshot under 1 shard (which absorbs +
/// best-effort removes the orphans), then resurrect the orphan files on disk and
/// restart with 1 shard. Because the snapshot recorded an ABSORBED position for
/// every group (incl. the orphans), recovery skips the resurrected orphan frames —
/// the topic's config update is not overwritten by the old create, and no deleted
/// record resurfaces.
#[test]
fn surviving_orphan_wal_files_do_not_regress_state() {
    let dir = tempfile::tempdir().unwrap();
    let b = "jobs";
    let wal_dir = dir.path().join("wal");

    {
        let engine = engine_at(dir.path(), 4);
        engine.put_topic(b, durable_topic()).unwrap();
        for i in 1..=6 {
            engine.write(b, one(json!({ "i": i }), None), true).unwrap();
        }
        // Delete the first two so a resurrected old append would resurface them.
        engine
            .delete(
                b,
                DeleteRequest {
                    before_seq: Some(3),
                    match_: None,
                },
            )
            .unwrap();
        engine.write_snapshot().unwrap();
    }

    // Stash a copy of the 4-shard subdirs (the orphans we will resurrect later).
    let stash = dir.path().join("stash");
    std::fs::create_dir_all(&stash).unwrap();
    for entry in std::fs::read_dir(&wal_dir).unwrap() {
        let p = entry.unwrap().path();
        if p.is_dir()
            && p.file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.starts_with("shard-"))
                .unwrap_or(false)
        {
            let name = p.file_name().unwrap().to_owned();
            let dst = stash.join(&name);
            std::fs::create_dir_all(&dst).unwrap();
            for f in std::fs::read_dir(&p).unwrap() {
                let fp = f.unwrap().path();
                std::fs::copy(&fp, dst.join(fp.file_name().unwrap())).unwrap();
            }
        }
    }

    {
        // Reopen flat (1 shard): absorbs the 4-shard groups + a NEW snapshot whose
        // checkpoint records every group (incl. orphans) as absorbed. Also tighten
        // config so a resurrected old create frame would visibly regress it.
        let engine = engine_at(dir.path(), 1);
        let mut cfg = durable_topic();
        cfg.cap_records = 777;
        engine.put_topic(b, cfg).unwrap();
        for i in 7..=8 {
            engine.write(b, one(json!({ "i": i }), None), true).unwrap();
        }
        engine.write_snapshot().unwrap();
        drop(engine);
    }

    // Resurrect the orphan shard subdirs (simulate a failed unlink).
    for entry in std::fs::read_dir(&stash).unwrap() {
        let p = entry.unwrap().path();
        let name = p.file_name().unwrap().to_owned();
        let dst = wal_dir.join(&name);
        std::fs::create_dir_all(&dst).unwrap();
        for f in std::fs::read_dir(&p).unwrap() {
            let fp = f.unwrap().path();
            std::fs::copy(&fp, dst.join(fp.file_name().unwrap())).unwrap();
        }
    }

    // Restart flat: the resurrected orphan frames must be skipped (absorbed), so
    // nothing regresses.
    let engine = engine_at(dir.path(), 1);
    let st = engine.topic_state(b, false).unwrap();
    assert_eq!(st.head_seq, 8, "head not regressed by resurrected orphans");
    assert_eq!(
        st.count, 6,
        "first two stay deleted (2 deleted of 8) — no resurrection"
    );
    let cfg = engine.get_topic(b).unwrap().config.read().clone();
    assert_eq!(
        cfg.cap_records, 777,
        "config update not overwritten by old create frame"
    );
    let d = engine.diff(b, diff_from(0)).unwrap();
    let seqs: Vec<u64> = d.records.iter().map(|r| r.seq).collect();
    assert_eq!(
        seqs,
        (3..=8).collect::<Vec<_>>(),
        "deleted prefix stays gone"
    );
}

// ---------------------------------------------------------------------------
// Frame classification helper (read-only inspection of the WAL on disk).
// ---------------------------------------------------------------------------

enum WalRecordKind {
    Append(u32),
    Other,
}

fn classify(frame: &topics::storage::WalFrame) -> WalRecordKind {
    use topics::storage::WalRecord;
    match &frame.record {
        WalRecord::Append { topic_id, .. } => WalRecordKind::Append(*topic_id),
        _ => WalRecordKind::Other,
    }
}
