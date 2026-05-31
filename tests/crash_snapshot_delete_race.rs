//! Crash/race test for the SNAPSHOT ↔ WAL-FIRST-DELETE interaction (codex P0 #1).
//!
//! Stage 1 made the API `delete` WAL-first: it logs the `Delete` frame (and waits
//! for its fsync on a durable box) BEFORE applying the deletion in memory. That
//! opened a window the snapshot path could fall into:
//!
//!   1. delete logs its `Delete` frame at WAL offset X and its fsync returns,
//!   2. a checkpoint is taken — its position lands AFTER X (the frame is covered),
//!   3. the snapshot materializes the box's memory, which is STILL UNDELETED
//!      (the delete hasn't run its in-memory apply yet),
//!   4. crash. Recovery restores the snapshot (record present) and replays the WAL
//!      tail FROM the checkpoint offset — which is past frame X — so the `Delete`
//!      frame is NEVER replayed and the deleted record RESURRECTS.
//!
//! The fix threads the delete's in-memory apply through the SAME per-box publish
//! gate appends use: a ticket is reserved under `append_lock` (right after the
//! `Delete` frame is enqueued) and released only after the in-memory apply, so a
//! concurrent snapshot's `quiesce_publishes()` drains the in-flight delete before
//! capturing memory. A snapshot can then never persist pre-delete memory for a
//! delete whose frame the checkpoint already covers.
//!
//! Two tests:
//!   * a DETERMINISTIC failpoint test (`all(test-fs, failpoints)`) that pins a
//!     delete in the exact post-commit / pre-apply window and proves a concurrent
//!     snapshot does NOT capture the still-undeleted record, and
//!   * a `test-fs`-only concurrency smoke test that hammers deletes against a
//!     snapshot loop and asserts no acked-deleted seq ever resurrects.
//!
//! ```text
//! cargo test --features "test-fs,failpoints" --test crash_snapshot_delete_race -- --test-threads=1
//! cargo test --features test-fs            --test crash_snapshot_delete_race -- --test-threads=1
//! ```

#![cfg(feature = "test-fs")]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use serde_json::json;

use streams::clock::{SharedClock, TestClock};
use streams::config::ServerConfig;
use streams::engine::Engine;
use streams::storage::testfs::{FakeDisk, TornDamage};
use streams::types::{BoxConfig, BoxType, DeleteRequest, DiffRequest, Filter, RecordIn, WriteRequest};

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

fn sync_dirs(disk: &FakeDisk) {
    let fs = disk.arc();
    let _ = fs.sync_dir(std::path::Path::new(DATA_DIR).join("wal").as_path());
    let _ = fs.sync_dir(std::path::Path::new(DATA_DIR).join("meta").as_path());
}

/// Live records of `name` by seq → tag, read through the public diff path.
fn live_by_tag(engine: &Engine, name: &str) -> BTreeMap<u64, String> {
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
            .expect("diff");
        for r in &d.records {
            out.insert(r.seq, r.tag.clone().unwrap_or_default());
        }
        if d.caught_up || d.records.is_empty() {
            break;
        }
        from = d.next_from_seq;
    }
    out
}

fn put_durable_log(engine: &Engine, name: &str) {
    engine
        .put_box(
            name,
            BoxConfig {
                r#type: BoxType::Log,
                durable: true,
                ..Default::default()
            },
        )
        .expect("create durable box");
}

fn append(engine: &Engine, name: &str, tag: &str) -> u64 {
    let req = WriteRequest {
        records: vec![RecordIn {
            data: json!({ "v": tag }),
            tag: Some(tag.to_string()),
            node: None,
            meta: None,
        }],
        node: None,
        idempotency_key: None,
        create: Some(false),
        config: None,
        disable_backpressure: true,
    };
    engine.write(name, req, true).expect("durable append acked").last_seq
}

/// DETERMINISTIC (failpoints): pin a delete in its post-commit / pre-apply window
/// (the `Delete` frame is durable, the in-memory deletion has NOT run, and the
/// delete still holds its publish ticket). A snapshot taken in that window MUST
/// observe the deletion (the fix makes its `quiesce_publishes()` drain the
/// in-flight delete) — never the stale, undeleted record. After a crash +
/// recover-from-snapshot the deleted record must STAY gone.
#[cfg(feature = "failpoints")]
#[test]
fn snapshot_blocks_on_inflight_delete_no_resurrection() {
    let disk = FakeDisk::new();
    {
        let engine = open_engine(&disk);
        put_durable_log(&engine, "msgs");

        // Two records: tag "keep" (seq 1, survives) and tag "gone" (seq 2, the
        // delete target). A resurrection of seq 2 is then unambiguous.
        let s_keep = append(&engine, "msgs", "keep");
        let s_gone = append(&engine, "msgs", "gone");
        assert_eq!((s_keep, s_gone), (1, 2));

        // Pause every thread that reaches the post-commit / pre-apply seam.
        fail::cfg("delete::after_commit_before_apply", "pause").unwrap();

        // Worker: delete tag "gone". It logs + fsyncs the `Delete` frame (durable
        // box), takes its publish ticket, then PARKS at the failpoint holding the
        // ticket — frame durable, memory still undeleted.
        let worker = {
            let engine = engine.clone();
            thread::spawn(move || {
                engine
                    .delete(
                        "msgs",
                        DeleteRequest {
                            before_seq: None,
                            match_: Some(Filter::from_shorthand("gone")),
                        },
                    )
                    .expect("delete acked")
            })
        };

        // Let the worker reach the parked seam (its frame is now durable).
        thread::sleep(std::time::Duration::from_millis(150));

        // Take a snapshot on ANOTHER thread: with the fix it blocks at
        // `quiesce_publishes()` until the parked delete finishes (so it captures
        // the POST-delete state); without the fix it would race ahead and capture
        // the pre-delete record. Run it off-thread so we can release the seam and
        // let both complete without an ordering deadlock.
        let snapshot = {
            let engine = engine.clone();
            thread::spawn(move || {
                let _ = engine.write_snapshot();
            })
        };

        // Give the snapshot thread time to reach (and, under the fix, block on)
        // the per-box quiesce, then release the delete seam. The delete applies,
        // releases its ticket, and the snapshot — now unblocked — captures the
        // box WITH the deletion applied.
        thread::sleep(std::time::Duration::from_millis(150));
        fail::cfg("delete::after_commit_before_apply", "off").unwrap();

        let r = worker.join().unwrap();
        assert_eq!(r.deleted, 1, "the delete removed exactly tag=gone (seq 2)");
        snapshot.join().unwrap();
        fail::remove("delete::after_commit_before_apply");

        sync_dirs(&disk);
        disk.crash(TornDamage::None);
        drop(engine);
    }
    disk.reset_power();

    // Recover from the crashed image (latest snapshot + WAL tail from checkpoint).
    let engine = open_engine(&disk);
    let live = live_by_tag(&engine, "msgs");
    assert!(
        !live.values().any(|t| t == "gone"),
        "deleted tag=gone (seq 2) RESURRECTED after snapshot+recovery: live={live:?}"
    );
    assert!(
        live.values().any(|t| t == "keep"),
        "tag=keep (seq 1) must survive: live={live:?}"
    );
}

/// CONCURRENCY smoke (`test-fs`): several deleter threads race a snapshot loop;
/// after a crash + recover FROM THE SNAPSHOT, every record a delete confirmed
/// removed must STAY removed (no resurrection through the snapshot/checkpoint
/// gap). Probabilistic — the deterministic guarantee is proven above; this guards
/// against regressions under real contention.
#[test]
fn snapshot_during_inflight_delete_does_not_resurrect_deleted_records() {
    let disk = FakeDisk::new();

    // The set of seqs the engine confirmed DELETED (the delete returned Ok and
    // reported them gone). Each such delete's frame is fsynced before the call
    // returns, so a checkpoint afterwards covers it — the records must never come
    // back.
    let deleted_seqs: Arc<Mutex<BTreeSet<u64>>> = Arc::new(Mutex::new(BTreeSet::new()));

    {
        let engine = open_engine(&disk);
        put_durable_log(&engine, "msgs");

        const N: usize = 600;
        let mut tag_seq: Vec<(String, u64)> = Vec::with_capacity(N);
        for i in 0..N {
            let tag = format!("t{i}");
            let seq = append(&engine, "msgs", &tag);
            tag_seq.push((tag, seq));
        }

        let stop = Arc::new(AtomicBool::new(false));
        const DELETERS: usize = 4;
        let mut handles = Vec::new();
        for d in 0..DELETERS {
            let chunk: Vec<(String, u64)> =
                tag_seq.iter().skip(d).step_by(DELETERS).cloned().collect();
            let engine = engine.clone();
            let deleted_seqs = deleted_seqs.clone();
            let stop = stop.clone();
            handles.push(thread::spawn(move || {
                for (tag, seq) in chunk {
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    let r = engine
                        .delete(
                            "msgs",
                            DeleteRequest {
                                before_seq: None,
                                match_: Some(Filter::from_shorthand(&tag)),
                            },
                        )
                        .expect("delete acked");
                    if r.deleted >= 1 {
                        deleted_seqs.lock().unwrap().insert(seq);
                    }
                }
            }));
        }

        while !handles.iter().all(|h| h.is_finished()) {
            let _ = engine.write_snapshot();
        }
        for h in handles {
            h.join().unwrap();
        }
        let _ = engine.write_snapshot();

        sync_dirs(&disk);
        disk.crash(TornDamage::None);
        drop(engine);
    }
    disk.reset_power();

    let engine = open_engine(&disk);
    let live = live_by_tag(&engine, "msgs");
    let deleted = deleted_seqs.lock().unwrap();

    assert!(!deleted.is_empty(), "the workload acked at least one delete");
    for seq in deleted.iter() {
        assert!(
            !live.contains_key(seq),
            "deleted seq {seq} RESURRECTED after snapshot+recovery; \
             live={:?}",
            live.keys().collect::<Vec<_>>()
        );
    }
}
