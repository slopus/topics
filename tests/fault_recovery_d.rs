//! Phase-8B fault catalog — boundary **recovery-replay**, group D.
//!
//! Five strategies that probe recovery's *idempotence and equivalence* rather
//! than a single injected I/O error: the recovery pipeline must converge to one
//! canonical state regardless of how it got there (replayed twice, rebuilt from
//! scratch, with or without a snapshot, with a checkpoint that overlaps the WAL,
//! or with a control frame missing for an append).
//!
//! | id | what it asserts |
//! |---|---|
//! | `F-REC-RUN-TWICE-IDENTICAL`      | `recover(recover(x)) == recover(x)` byte-for-byte |
//! | `F-REC-REBUILD-MATCHES-SCRATCH`  | snapshot+replay state == full from-frame-zero replay |
//! | `F-REC-NO-SNAPSHOT-FULL-REPLAY`  | snapshots gone/corrupt ⇒ rebuild equivalent state from WAL |
//! | `F-REC-PARTIAL-CHECKPOINT-OVERLAP` | checkpoint offset mid-snapshot-materialized frames ⇒ idempotent |
//! | `F-REC-TOPIC-MISSING-FOR-APPEND`   | Append whose TopicConfig was lost ⇒ lazily materialized, record kept, no panic |
//!
//! All five drive the *real, fully-wired* [`Engine`] through an in-memory
//! [`FakeDisk`] (the Phase-8A harness), then recover a fresh engine through the
//! same image and diff. Bounded workloads, fixed seeds, capped — runs in well
//! under a minute.
//!
//! ```text
//! cargo test --features test-fs --test fault_recovery_d
//! ```

#![cfg(feature = "test-fs")]
#![allow(
    clippy::ptr_arg,
    clippy::manual_clamp,
    clippy::unusual_byte_groupings,
    clippy::doc_lazy_continuation
)]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use serde_json::json;

use topics::clock::{SharedClock, TestClock};
use topics::config::ServerConfig;
use topics::engine::Engine;
use topics::storage::testfs::FakeDisk;
use topics::storage::wal::{encode_frame, WalRecord};
use topics::storage::OpenOpts;
use topics::types::{DiffRequest, RecordIn, TopicConfig, TopicType, WriteRequest};

// ===========================================================================
// Plumbing shared by every test (adapted from tests/crash_oracle.rs — the
// Phase-8A harness — so this file is self-contained per the suite rules).
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

/// Build / recover a durable engine whose WAL + snapshots live on `disk`.
fn open_engine(disk: &FakeDisk) -> Arc<Engine> {
    Engine::with_data_dir_fs(cfg(), clock(), disk.arc()).expect("engine opens through FakeDisk")
}

/// One recovered record, exactly the fields the durability contract preserves.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Rec {
    data: String,
    tag: Option<String>,
    node: Option<String>,
}

/// A flat, comparable dump of one recovered topic: head / earliest / count and the
/// live records read back through the real diff path (the same bytes a consumer
/// sees). This is the "byte-identical recovery" comparison key.
#[derive(Debug, Clone, PartialEq, Eq)]
struct TopicDump {
    head: u64,
    earliest: u64,
    count: u64,
    records: BTreeMap<u64, Rec>,
    tombstone_reason: Option<String>,
}

/// Read the full recovered state of `name` through the engine's public API.
fn dump_topic(engine: &Engine, name: &str) -> Option<TopicDump> {
    let st = engine.topic_state(name, false).ok()?;
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
                Rec {
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
    Some(TopicDump {
        head: st.head_seq,
        earliest: st.earliest_seq,
        count: st.count,
        records,
        tombstone_reason,
    })
}

/// Dump every topic the engine currently knows about, keyed by name, into one
/// comparable map (the whole-engine recovery fingerprint).
fn dump_all(engine: &Engine, names: &[&str]) -> BTreeMap<String, TopicDump> {
    let mut out = BTreeMap::new();
    for n in names {
        if let Some(d) = dump_topic(engine, n) {
            out.insert((*n).to_string(), d);
        }
    }
    out
}

/// Make every WAL/meta file's NAME durable (the create+dir-fsync production does
/// at WAL open) so files survive a power loss in the FakeDisk model.
fn sync_dirs(disk: &FakeDisk) {
    let fs = disk.arc();
    let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("wal"));
    let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("meta"));
}

/// Create a durable Log topic.
fn put_durable_topic(engine: &Engine, name: &str) {
    let cfg = TopicConfig {
        r#type: TopicType::Log,
        durable: true,
        cap_records: 0,
        ..Default::default()
    };
    engine.put_topic(name, cfg).expect("put_topic");
}

/// Append one durable record `(data, tag, node)`, blocking on its group fsync
/// (so it is acked ⇒ durable). Returns the assigned seq.
fn append(engine: &Engine, name: &str, data: &str, tag: Option<&str>, node: Option<&str>) -> u64 {
    let req = WriteRequest {
        records: vec![RecordIn {
            data: json!({ "v": data }),
            tag: tag.map(str::to_string),
            node: node.map(str::to_string),
            meta: None,
        }],
        node: None,
        idempotency_key: None,
        create: Some(true),
        config: None,
        disable_backpressure: true,
    };
    let resp = engine.write(name, req, true).expect("durable append acked");
    resp.last_seq
}

/// List the WAL `.log` files on the disk image, ascending.
fn wal_files(disk: &FakeDisk) -> Vec<PathBuf> {
    let fs = disk.arc();
    let mut v: Vec<PathBuf> = fs
        .read_dir(&PathBuf::from(DATA_DIR).join("wal"))
        .unwrap_or_default()
        .into_iter()
        .filter(|p| {
            p.file_name()
                .and_then(|s| s.to_str())
                .map(|n| n.starts_with("wal-") && n.ends_with(".log"))
                .unwrap_or(false)
        })
        .collect();
    v.sort();
    v
}

/// List the `snapshot-*.bin` files on the disk image, ascending.
fn snapshot_files(disk: &FakeDisk) -> Vec<PathBuf> {
    let fs = disk.arc();
    let mut v: Vec<PathBuf> = fs
        .read_dir(&PathBuf::from(DATA_DIR).join("meta"))
        .unwrap_or_default()
        .into_iter()
        .filter(|p| {
            p.file_name()
                .and_then(|s| s.to_str())
                .map(|n| n.starts_with("snapshot-") && n.ends_with(".bin"))
                .unwrap_or(false)
        })
        .collect();
    v.sort();
    v
}

/// Overwrite the durable bytes of `path` on the image with `bytes` and dir-fsync
/// so the change survives — used to corrupt a snapshot in place.
fn overwrite_durable(disk: &FakeDisk, path: &PathBuf, bytes: &[u8]) {
    let fs = disk.arc();
    {
        let mut f = fs
            .open(path, OpenOpts::create_truncate())
            .expect("open for overwrite");
        let mut off = 0usize;
        while off < bytes.len() {
            let n = f.write_at(off as u64, &bytes[off..]).expect("write");
            off += n;
        }
        f.sync_all().expect("fsync overwrite");
    }
    let parent = path.parent().unwrap().to_path_buf();
    fs.sync_dir(&parent).expect("dir fsync");
}

/// Encode a `{"d": {"v": <data>}}` payload exactly the way the engine's
/// `encode_record_payload` does, so a hand-crafted Append decodes back to a
/// record whose `data.v` the diff path surfaces.
fn payload(data: &str) -> Vec<u8> {
    serde_json::to_vec(&json!({ "d": { "v": data } })).unwrap()
}

/// Append a list of pre-built records as durable WAL frames to a hand-crafted WAL
/// file at `path` (creating/truncating it), then make it durable. The frames are
/// the genuine on-disk format (`encode_frame`), so recovery reads them exactly as
/// the writer-produced ones.
fn write_wal_file(disk: &FakeDisk, path: &PathBuf, records: &[WalRecord]) {
    let fs = disk.arc();
    let mut buf = Vec::new();
    let mut frame = Vec::new();
    for r in records {
        encode_frame(&mut frame, r, true);
        buf.extend_from_slice(&frame);
    }
    let mut f = fs
        .open(path, OpenOpts::create_truncate())
        .expect("open hand-crafted wal");
    let mut off = 0usize;
    while off < buf.len() {
        let n = f.write_at(off as u64, &buf[off..]).expect("write wal");
        off += n;
    }
    f.sync_all().expect("fsync wal");
    let parent = path.parent().unwrap().to_path_buf();
    fs.create_dir_all(&parent).ok();
    fs.sync_dir(&parent).expect("dir fsync wal");
}

// ===========================================================================
// F-REC-RUN-TWICE-IDENTICAL — replay(replay(x)) == replay(x), byte-for-byte.
// ===========================================================================

/// Drive a durable multi-topic workload (appends + a tag-delete + a cap eviction),
/// then recover THREE times in a row, each time snapshotting the full engine
/// state. All three fingerprints must be byte-identical: head / earliest / count
/// / per-seq data·tag·node / tombstone reason. This is the PG idempotent-replay
/// oracle (invariant 4).
#[test]
fn f_rec_run_twice_identical() {
    let disk = FakeDisk::new();
    let names = ["jobs", "events", "capped"];

    {
        let engine = open_engine(&disk);
        put_durable_topic(&engine, "jobs");
        put_durable_topic(&engine, "events");
        // A durable cap topic: writing past the cap advances the involuntary floor.
        let capcfg = TopicConfig {
            r#type: TopicType::Log,
            durable: true,
            cap_records: 3,
            ..Default::default()
        };
        engine.put_topic("capped", capcfg).unwrap();

        append(&engine, "jobs", "j1", Some("a"), Some("nA"));
        append(&engine, "jobs", "j2", Some("drop"), None);
        append(&engine, "jobs", "j3", Some("a"), Some("nB"));
        append(&engine, "events", "e1", None, Some("n1"));
        append(&engine, "events", "e2", Some("k"), None);
        for i in 1..=6 {
            append(&engine, "capped", &format!("c{i}"), None, None);
        }
        // A tag delete on jobs (the 'drop'-tagged j2 goes silently).
        engine
            .delete(
                "jobs",
                topics::types::DeleteRequest {
                    before_seq: None,
                    match_: Some(topics::types::Filter::from_shorthand("drop")),
                },
            )
            .unwrap();

        sync_dirs(&disk);
        drop(engine);
    }

    // recover #1, #2, #3 — each a fresh engine through the same durable image.
    let d1 = {
        let e = open_engine(&disk);
        let d = dump_all(&e, &names);
        drop(e);
        d
    };
    let d2 = {
        let e = open_engine(&disk);
        let d = dump_all(&e, &names);
        drop(e);
        d
    };
    let d3 = {
        let e = open_engine(&disk);
        let d = dump_all(&e, &names);
        drop(e);
        d
    };

    assert_eq!(d1, d2, "recover(recover(x)) must equal recover(x)");
    assert_eq!(d2, d3, "a third recovery is still identical (convergent)");

    // Spot-check the concrete survivors so the equality isn't trivially "all empty".
    let jobs = &d1["jobs"];
    assert_eq!(
        jobs.records.keys().copied().collect::<Vec<_>>(),
        vec![1, 3],
        "jobs: the 'drop'-tagged seq 2 is gone, 1 & 3 remain"
    );
    let capped = &d1["capped"];
    assert_eq!(
        capped.records.keys().copied().collect::<Vec<_>>(),
        vec![4, 5, 6],
        "capped: only the newest cap=3 survive"
    );
    assert_eq!(
        capped.tombstone_reason.as_deref(),
        Some("cap"),
        "cap eviction stays explicit across every recovery"
    );
}

// ===========================================================================
// F-REC-REBUILD-MATCHES-SCRATCH — snapshot+replay == from-frame-zero replay.
// ===========================================================================

/// Differential oracle (wal_consistency_checking analog): write a workload, force
/// a durable SNAPSHOT (so recovery #1 restores it and replays only the
/// post-checkpoint tail), capture that state; then DELETE every snapshot so
/// recovery #2 replays the entire WAL from frame zero, and capture that. The two
/// states must be identical — the snapshot is purely an optimization, never a
/// correctness input.
#[test]
fn f_rec_rebuild_matches_scratch() {
    let disk = FakeDisk::new();
    let names = ["a", "b"];

    {
        let engine = open_engine(&disk);
        put_durable_topic(&engine, "a");
        put_durable_topic(&engine, "b");
        // Pre-snapshot writes (these get materialized INTO the snapshot).
        append(&engine, "a", "a1", Some("t"), None);
        append(&engine, "a", "a2", None, Some("nA"));
        append(&engine, "b", "b1", None, None);

        // Force a durable checkpoint: 'a'/'b' state is materialized, the checkpoint
        // offset advances past these frames.
        assert!(engine.write_snapshot().expect("snapshot written"));

        // Post-snapshot writes (replayed from the WAL tail after the checkpoint).
        append(&engine, "a", "a3", Some("t"), None);
        append(&engine, "b", "b2", Some("k"), Some("nB"));
        engine
            .delete(
                "a",
                topics::types::DeleteRequest {
                    before_seq: Some(2),
                    match_: None,
                },
            )
            .unwrap();

        sync_dirs(&disk);
        drop(engine);
    }

    // Recovery #1: snapshot present ⇒ restore + replay the post-checkpoint tail.
    assert!(
        !snapshot_files(&disk).is_empty(),
        "a snapshot was durably written"
    );
    let with_snapshot = {
        let e = open_engine(&disk);
        let d = dump_all(&e, &names);
        drop(e);
        d
    };

    // Now remove every snapshot so recovery #2 must rebuild from WAL frame zero.
    let fs = disk.arc();
    for p in snapshot_files(&disk) {
        fs.remove_file(&p).expect("remove snapshot");
    }
    sync_dirs(&disk);
    assert!(
        snapshot_files(&disk).is_empty(),
        "all snapshots removed for the scratch rebuild"
    );

    let from_scratch = {
        let e = open_engine(&disk);
        let d = dump_all(&e, &names);
        drop(e);
        d
    };

    assert_eq!(
        with_snapshot, from_scratch,
        "snapshot+replay must equal a full from-frame-zero WAL replay"
    );

    // Concrete check: 'a' lost seq1 to the before_seq=2 delete; a2,a3 remain.
    let a = &from_scratch["a"];
    assert_eq!(a.records.keys().copied().collect::<Vec<_>>(), vec![2, 3]);
    assert_eq!(a.records[&3].data, "a3");
    let b = &from_scratch["b"];
    assert_eq!(b.records.keys().copied().collect::<Vec<_>>(), vec![1, 2]);
    assert_eq!(b.records[&2].node.as_deref(), Some("nB"));
}

// ===========================================================================
// F-REC-NO-SNAPSHOT-FULL-REPLAY — snapshots gone/corrupt ⇒ full WAL replay.
// ===========================================================================

/// Write a workload + a durable snapshot, then CORRUPT every snapshot body
/// (garble the magic + version + body) so `load_latest` skips them all and
/// recovery falls back to a full WAL replay from offset 0. The rebuilt state must
/// equal the clean (snapshot-intact) recovery — the snapshot is an optimization,
/// correctness comes from the WAL (invariant 6/7 fallback).
#[test]
fn f_rec_no_snapshot_full_replay() {
    let disk = FakeDisk::new();
    let names = ["log"];

    {
        let engine = open_engine(&disk);
        put_durable_topic(&engine, "log");
        for i in 1..=4 {
            append(&engine, "log", &format!("v{i}"), Some("t"), None);
        }
        assert!(engine.write_snapshot().expect("snapshot written"));
        for i in 5..=8 {
            append(&engine, "log", &format!("v{i}"), None, Some("n"));
        }
        sync_dirs(&disk);
        drop(engine);
    }

    // Baseline: recover with the snapshot intact.
    let intact = {
        let e = open_engine(&disk);
        let d = dump_all(&e, &names);
        drop(e);
        d
    };
    assert_eq!(
        intact["log"].records.len(),
        8,
        "all 8 durable records present with the snapshot"
    );

    // Corrupt EVERY snapshot file: overwrite the whole body with garbage so the
    // magic/version/CRC all fail ⇒ load_latest skips it ⇒ full WAL replay.
    let snaps = snapshot_files(&disk);
    assert!(!snaps.is_empty(), "snapshots exist to corrupt");
    for p in &snaps {
        overwrite_durable(&disk, p, &[0xAB; 64]);
    }

    let full_replay = {
        let e = open_engine(&disk);
        let d = dump_all(&e, &names);
        drop(e);
        d
    };

    assert_eq!(
        intact, full_replay,
        "a corrupt/skipped snapshot ⇒ full WAL replay rebuilds equivalent state"
    );
    assert_eq!(
        full_replay["log"].records.len(),
        8,
        "full replay from frame zero recovers all 8 records (no snapshot dependence)"
    );
    // And the WAL still holds frame zero (the pre-checkpoint file was kept because
    // nothing absorbed it durably here, or replay re-reads it from offset 0).
    assert!(!wal_files(&disk).is_empty(), "WAL is the source of truth");
}

// ===========================================================================
// F-REC-PARTIAL-CHECKPOINT-OVERLAP — checkpoint offset mid-snapshot frames.
// ===========================================================================

/// The checkpoint offset lands BEFORE frames that the snapshot already
/// materialized: a snapshot whose head covers seqs 1..=5 is paired with a
/// checkpoint OFFSET that points BEFORE those frames (rewound to 0), so the full
/// WAL — frames 1..=5 *and* the post-checkpoint 6,7 — is re-read. The offset-skip
/// + the `seq <= head` Append-skip must make the 1..=5 overlap a no-op (a stale
/// re-read control/append frame must not overwrite the snapshot's materialized
/// state), while 6,7 append exactly once at head+1: no double-insert, no gap
/// (invariant 4).
///
/// Construction: capture the REAL snapshot at head=5, append 6,7 (now genuinely
/// past the checkpoint position), then rewrite the snapshot with its checkpoint
/// `wal_offset` rewound to 0 so recovery replays the whole WAL against a head-5
/// snapshot — the exact "checkpoint mid snapshot-materialized frames" overlap.
#[test]
fn f_rec_partial_checkpoint_overlap() {
    use topics::engine::snapshot::capture as capture_snapshot;
    use topics::storage::{next_snapshot_id_with, write_snapshot_with};

    let disk = FakeDisk::new();
    let bcfg = TopicConfig {
        r#type: TopicType::Log,
        durable: true,
        cap_records: 0,
        ..Default::default()
    };

    {
        let engine = open_engine(&disk);
        engine.put_topic("ov", bcfg.clone()).unwrap();
        for i in 1..=5u64 {
            append(&engine, "ov", &format!("o{i}"), Some("t"), None);
        }

        // Capture the real snapshot (head=5, materializes seqs 1..=5) and record the
        // checkpoint position the writer is at right now.
        let fs = disk.arc();
        let snap_id = next_snapshot_id_with(&fs, &PathBuf::from(DATA_DIR));
        let mut snap = capture_snapshot(&engine, snap_id).expect("capture");
        assert_eq!(snap.checkpoint.last_checkpoint_seq, 5, "snapshot head is 5");

        // Now append 6,7 — these land in the WAL strictly AFTER the captured
        // checkpoint position, so they are the genuine post-checkpoint tail.
        append(&engine, "ov", "o6", None, None);
        append(&engine, "ov", "o7", None, None);

        // Rewind the checkpoint OFFSET to 0 (the partial-overlap injection): replay
        // will re-read the whole WAL (the TopicConfig create + Appends 1..=5 that the
        // snapshot already materialized, plus 6,7) against a head-5 snapshot.
        snap.checkpoint.wal_offset = 0;
        snap.checkpoint.wal_idx = 1;
        write_snapshot_with(&fs, &PathBuf::from(DATA_DIR), &snap).expect("write rewound snapshot");

        sync_dirs(&disk);
        drop(engine);
    }

    // Recover: restore snapshot (head 5, seqs 1..=5) then replay the WHOLE WAL from
    // offset 0. The overlapping TopicConfig is idempotent; Appends 1..=5 are
    // seq-skipped (<= head); 6,7 append at head+1. No double-insert, no gap.
    let e = open_engine(&disk);
    let ov = dump_topic(&e, "ov").expect("ov present after overlap recovery");
    drop(e);

    assert_eq!(
        ov.records.keys().copied().collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5, 6, 7],
        "overlap is idempotent: 1..=5 from snapshot (not double-applied), 6,7 appended once"
    );
    assert_eq!(
        ov.head, 7,
        "head advanced to the post-overlap tail exactly once"
    );
    // The overlapping seqs keep their SNAPSHOT-materialized payloads (the re-read
    // overlapping frames did not overwrite them).
    assert_eq!(ov.records[&1].data, "o1");
    assert_eq!(ov.records[&3].data, "o3");
    assert_eq!(ov.records[&6].data, "o6");
    assert_eq!(ov.records[&7].data, "o7");

    // Idempotent on a second recovery too.
    let e2 = open_engine(&disk);
    let ov2 = dump_topic(&e2, "ov").expect("ov present #2");
    drop(e2);
    assert_eq!(ov, ov2, "partial-overlap recovery is convergent");
}

// ===========================================================================
// F-REC-TOPIC-MISSING-FOR-APPEND — Append whose TopicConfig create frame was lost.
// ===========================================================================

/// A WAL where the topic's `TopicConfig` create frame is ABSENT (torn before it / lost
/// to a power loss) but a later `Append` for that `topic_id` survives. Recovery must
/// NOT panic and must NOT drop the record: `replay_frame` lazily materializes the
/// topic with default config under the synthetic name `topic-<topic_id>`
/// (`apply_put_topic_for_recovery`). The record is therefore preserved (no silent
/// loss) — but the contract note is that this never *silently changes a durable
/// topic's config*: the recovered topic carries DEFAULT (non-durable Log) config, not
/// the original durable config, which a from-scratch read surfaces under the
/// synthetic name. We assert the record survives, no panic, and the synthetic
/// materialization is observable (so the loss-of-config is explicit, not silent).
#[test]
fn f_rec_topic_missing_for_append() {
    let disk = FakeDisk::new();
    let fs = disk.arc();
    fs.create_dir_all(&PathBuf::from(DATA_DIR).join("wal"))
        .unwrap();

    // A topic_id that NO TopicConfig frame creates — only Appends reference it.
    let orphan_topic_id: u64 = 7;
    let frames = vec![
        WalRecord::Append {
            topic_id: orphan_topic_id,
            seq: 1,
            ts: 1_700_000_000_001,
            node: Some("nA".into()),
            tag: Some("t1".into()),
            data: payload("rec1"),
        },
        WalRecord::Append {
            topic_id: orphan_topic_id,
            seq: 2,
            ts: 1_700_000_000_002,
            node: None,
            tag: Some("t2".into()),
            data: payload("rec2"),
        },
    ];
    let wal_path = PathBuf::from(DATA_DIR)
        .join("wal")
        .join("wal-0000000000000001.log");
    write_wal_file(&disk, &wal_path, &frames);

    // Recovery must lazily materialize the topic and keep the records — no panic.
    let engine = open_engine(&disk);

    // The topic surfaces under the synthetic name `topic-<topic_id>` (default config).
    let synthetic = format!("topic-{orphan_topic_id}");
    let dump = dump_topic(&engine, &synthetic)
        .expect("Append with a missing TopicConfig is lazily materialized, record kept");
    assert_eq!(
        dump.records.keys().copied().collect::<Vec<_>>(),
        vec![1, 2],
        "both orphan-topic Appends are preserved (no silent loss)"
    );
    assert_eq!(dump.records[&1].data, "rec1");
    assert_eq!(dump.records[&1].tag.as_deref(), Some("t1"));
    assert_eq!(dump.records[&1].node.as_deref(), Some("nA"));
    assert_eq!(dump.records[&2].data, "rec2");
    assert_eq!(dump.head, 2, "head reflects the recovered appends");

    // Recovery is idempotent here too (the lazily-materialized topic is stable).
    drop(engine);
    let e2 = open_engine(&disk);
    let dump2 = dump_topic(&e2, &synthetic).expect("synthetic topic present #2");
    drop(e2);
    assert_eq!(dump, dump2, "lazy-materialization recovery is convergent");
}
