//! Crash/race test: a SNAPSHOT taken WHILE many concurrent durable writers are
//! in-flight on the SAME box, then a power loss + recovery FROM THE SNAPSHOT
//! CHECKPOINT. Asserts the snapshot/checkpoint discipline (ARCHITECTURE §3): the
//! checkpoint position is recorded BEFORE state is materialized and each box is
//! captured under its `append_lock` after draining ticketed-but-unpublished
//! writes, so:
//!
//!   * NO ACKED WRITE IS MISSING after recovery (every write whose fsync returned
//!     is either in the snapshot or replayed from the WAL tail past the
//!     checkpoint offset — never excluded by a checkpoint that raced ahead of a
//!     staged frame), and
//!   * NO UNACKED WRITE IS UNEXPECTEDLY VISIBLE (a staged-but-unpublished tail is
//!     never persisted into the snapshot, and the recovered head never exceeds
//!     the highest acked seq).
//!
//! ```text
//! cargo test --features test-fs --test crash_snapshot_inflight -- --test-threads=1
//! ```

#![cfg(feature = "test-fs")]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use serde_json::json;

use streams::clock::{SharedClock, TestClock};
use streams::config::ServerConfig;
use streams::engine::Engine;
use streams::storage::testfs::{FakeDisk, TornDamage};
use streams::types::{BoxConfig, BoxType, DiffRequest, RecordIn, WriteRequest};

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

/// All live records of `name` by seq → value, read through the public diff path.
fn live_records(engine: &Engine, name: &str) -> BTreeMap<u64, String> {
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
                    include_tags: false,
                    include_meta: false,
                    wait_ms: 0,
                    max_batch_bytes: 0,
                },
            )
            .expect("diff");
        for r in &d.records {
            let v = r.data.get("v").and_then(|v| v.as_str()).unwrap_or_default();
            out.insert(r.seq, v.to_string());
        }
        if d.caught_up || d.records.is_empty() {
            break;
        }
        from = d.next_from_seq;
    }
    out
}

#[test]
fn snapshot_with_inflight_same_box_writers_loses_no_acked_write() {
    let disk = FakeDisk::new();

    // The set of (seq -> value) the engine ACKED (its durable fsync returned). This
    // is exactly the must-survive set: every acked durable write must recover.
    let acked: Arc<Mutex<BTreeMap<u64, String>>> = Arc::new(Mutex::new(BTreeMap::new()));

    {
        let engine = open_engine(&disk);
        engine
            .put_box(
                "hot",
                BoxConfig {
                    r#type: BoxType::Log,
                    durable: true,
                    ..Default::default()
                },
            )
            .expect("create durable box");

        let stop = Arc::new(AtomicBool::new(false));
        const N_WRITERS: usize = 6;
        const PER_WRITER: usize = 40;

        // Spawn many concurrent durable writers to the SAME box. Each blocks on its
        // group-commit fsync (durable box ⇒ `write` returns only after the fsync),
        // so an Ok response means the write is acked & durable.
        let mut handles = Vec::new();
        for wid in 0..N_WRITERS {
            let engine = engine.clone();
            let acked = acked.clone();
            let stop = stop.clone();
            handles.push(thread::spawn(move || {
                for i in 0..PER_WRITER {
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    let value = format!("w{wid}-{i}");
                    let req = WriteRequest {
                        records: vec![RecordIn {
                            data: json!({ "v": value }),
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
                    if let Ok(resp) = engine.write("hot", req, true) {
                        acked.lock().unwrap().insert(resp.last_seq, value);
                    }
                }
            }));
        }

        // Take snapshots WHILE the writers are in-flight — repeatedly, so a
        // snapshot's checkpoint-record / state-materialize window overlaps the
        // writers' fsync waits and ticketed-but-unpublished stages. Each snapshot
        // captures every box under its append_lock after draining in-flight writes.
        for _ in 0..8 {
            let _ = engine.write_snapshot();
            thread::yield_now();
        }

        for h in handles {
            h.join().unwrap();
        }
        // One more snapshot after the writers drained (covers the final tail).
        let _ = engine.write_snapshot();

        // Make all WAL + snapshot file names durable, then power-loss + drop.
        sync_dirs(&disk);
        disk.crash(TornDamage::None);
        drop(engine);
    }
    disk.reset_power();

    // Recover a FRESH engine through the crashed image. Recovery loads the latest
    // valid snapshot and replays the WAL tail from its checkpoint offset.
    let engine = open_engine(&disk);
    let survivors = live_records(&engine, "hot");
    let acked = acked.lock().unwrap();

    let max_acked = acked.keys().copied().max().unwrap_or(0);
    assert!(!acked.is_empty(), "the workload acked at least one durable write");

    // (1) NO ACKED WRITE MISSING: every acked durable write recovered, with its
    //     exact value (no checkpoint excluded a covered frame).
    for (seq, value) in acked.iter() {
        match survivors.get(seq) {
            Some(got) => assert_eq!(
                got, value,
                "recovered seq {seq} has the wrong value (snapshot/WAL disagree)"
            ),
            None => panic!(
                "acked durable seq {seq} ({value:?}) LOST after snapshot+recovery \
                 (survivors head={:?}, max_acked={max_acked})",
                survivors.keys().last()
            ),
        }
    }

    // (2) NO UNACKED WRITE UNEXPECTEDLY VISIBLE: every survivor is an acked write
    //     (no fabricated/torn record), and the recovered head never exceeds the
    //     highest acked seq (a staged-but-unpublished tail was never persisted).
    for (seq, value) in survivors.iter() {
        let a = acked.get(seq).unwrap_or_else(|| {
            panic!(
                "recovered seq {seq} ({value:?}) was never acked \
                 (unacked write became visible / fabricated record)"
            )
        });
        assert_eq!(a, value, "recovered seq {seq} value mismatch vs acked");
    }
    let st = engine.box_state("hot", false).unwrap();
    assert!(
        st.head_seq <= max_acked,
        "recovered head {} exceeds the highest acked seq {max_acked} (future/unacked seq)",
        st.head_seq
    );

    // (3) DENSE: the survivors are a gapless run [1..=head] (every assigned seq
    //     was acked here — no record was dropped mid-stream), proving the snapshot
    //     + WAL tail compose into a contiguous log.
    let keys: Vec<u64> = survivors.keys().copied().collect();
    if let (Some(&lo), Some(&hi)) = (keys.first(), keys.last()) {
        let expected: Vec<u64> = (lo..=hi).collect();
        assert_eq!(keys, expected, "survivors must be a dense contiguous run");
    }
}
