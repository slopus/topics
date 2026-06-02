//! Crash/fault test for the WAL-first `put_topic` config UPDATE (RELIABILITY fix
//! #2). The old path was APPLY-FIRST: an in-place config update swapped the
//! config in memory and only THEN logged the `TopicConfig` frame, so a WAL failure
//! left the in-memory config (a relaxed/tightened durability/cap/ttl) AHEAD of
//! the durable log — "applied but not committed", which a restart silently
//! reverts. The fix SNAPSHOTS the prior config and serializes the swap against
//! the topic's appends/deletes (under `append_lock`); on a WAL failure it RESTORES
//! the prior config and returns an error, so memory never diverges from the log.
//!
//! This test injects a WAL fsync failure on the TopicConfig update frame, asserts
//! the update returned an error AND the in-memory config rolled back, then
//! recovers a fresh engine and asserts the recovered config converges with the
//! (rolled-back) in-memory config — the mutation was fully rolled back, never
//! half-applied.
//!
//! ```text
//! cargo test --features test-fs --test crash_puttopic_update -- --test-threads=1
//! ```

#![cfg(feature = "test-fs")]

use std::collections::BTreeMap;
use std::sync::Arc;

use serde_json::json;

use topics::clock::{SharedClock, TestClock};
use topics::config::ServerConfig;
use topics::engine::Engine;
use topics::storage::testfs::{FakeDisk, FaultFs, FaultKind, FaultOp};
use topics::storage::Fs;
use topics::types::{DiffRequest, RecordIn, TopicConfig, TopicType, WriteRequest};

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

fn open_engine_fs(fs: Arc<dyn Fs>) -> Arc<Engine> {
    Engine::with_data_dir_fs(cfg(), clock(), fs).expect("engine opens")
}

fn open_engine(disk: &FakeDisk) -> Arc<Engine> {
    open_engine_fs(disk.arc())
}

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
        create: Some(true),
        config: None,
        disable_backpressure: true,
    };
    engine
        .write(name, req, true)
        .expect("append acked")
        .last_seq
}

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
                    include_tags: true,
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

/// The recovered config of `name`, by reading topic_state through a fresh engine.
fn recovered_config(disk: &FakeDisk, name: &str) -> TopicConfig {
    let engine = open_engine(disk);
    engine
        .topic_state(name, false)
        .expect("topic exists after recovery")
        .config
}

fn sync_dirs(disk: &FakeDisk) {
    let fs = disk.arc();
    let _ = fs.sync_dir(std::path::Path::new(DATA_DIR).join("wal").as_path());
    let _ = fs.sync_dir(std::path::Path::new(DATA_DIR).join("meta").as_path());
}

/// A `put_topic` UPDATE whose WAL frame fails to fsync must roll the in-memory
/// config back to the prior config, return an error, and leave NO durable trace —
/// so the recovered config converges with the rolled-back in-memory config.
#[test]
fn put_topic_update_wal_fail_rolls_back_and_converges() {
    let disk = FakeDisk::new();

    // --- Phase 1: create a durable topic with a relaxed config (cap=0, ttl=0) and
    //     write two acked records on a clean disk. ----------------------------
    {
        let engine = open_engine(&disk);
        engine
            .put_topic(
                "cfg",
                TopicConfig {
                    r#type: TopicType::Log,
                    durable: true,
                    cap_records: 0,
                    ttl_ms: 0,
                    ..Default::default()
                },
            )
            .expect("create durable topic");
        append(&engine, "cfg", "a");
        append(&engine, "cfg", "b");
        sync_dirs(&disk);
        drop(engine);
    }

    // --- Phase 2: reopen and arm a fail-once SyncData EIO at index 0. Engine
    //     open + WAL recovery only ever issue `sync_all`/`sync_dir` (never the
    //     group-commit `sync_data` / fdatasync), and we issue NO durable write
    //     after reopen before the update — so the FIRST `sync_data` on the device
    //     is the update's TopicConfig group-commit fsync. The update MUST fail and
    //     the in-memory config MUST roll back to the prior (cap=0, ttl=0). ------
    let faulty = FaultFs::new(
        disk.arc(),
        FaultOp::SyncData,
        FaultKind::Eio,
        0,
        true, // fail once (the TopicConfig fsync), then the device works again
    );
    {
        let engine = open_engine_fs(faulty.arc());

        // Pre-update in-memory config is the relaxed one.
        let before = engine.topic_state("cfg", false).unwrap().config;
        assert_eq!(before.cap_records, 0);
        assert_eq!(before.ttl_ms, 0);

        // The tightening update's WAL fsync EIOs ⇒ put_topic returns Err.
        let res = engine.put_topic(
            "cfg",
            TopicConfig {
                r#type: TopicType::Log,
                durable: true,
                cap_records: 1,
                ttl_ms: 60_000,
                ..Default::default()
            },
        );
        assert!(
            res.is_err(),
            "config update must fail when its WAL fsync EIOs"
        );

        // ROLLED BACK: the in-memory config is the prior relaxed one, NOT the
        // tightened one the client was told (via the error) did not take effect.
        let after = engine.topic_state("cfg", false).unwrap().config;
        assert_eq!(
            after.cap_records, 0,
            "cap_records rolled back to the prior config (mutation not applied)"
        );
        assert_eq!(after.ttl_ms, 0, "ttl_ms rolled back to the prior config");

        // The relaxed cap is still in force: a third write is NOT evicted by the
        // would-be cap=1 (proof the tighten did not silently apply in memory).
        append(&engine, "cfg", "c");
        let live = live_records(&engine, "cfg");
        assert_eq!(
            live.values().cloned().collect::<Vec<_>>(),
            vec!["a", "b", "c"],
            "the rolled-back cap=0 keeps all three records (the failed cap=1 never applied)"
        );

        // Capture the in-memory config to compare with the recovered config.
        let mem_cfg = engine.topic_state("cfg", false).unwrap().config;
        sync_dirs(&disk);
        drop(engine);

        // --- CONVERGENCE: recover a fresh engine and assert the recovered config
        //     equals the in-memory (rolled-back) config — the update left NO
        //     durable trace, so memory and disk agree. -------------------------
        let rec_cfg = recovered_config(&disk, "cfg");
        assert_eq!(
            rec_cfg.cap_records, mem_cfg.cap_records,
            "recovered cap_records converges with in-memory (rolled back)"
        );
        assert_eq!(
            rec_cfg.ttl_ms, mem_cfg.ttl_ms,
            "recovered ttl_ms converges with in-memory (rolled back)"
        );
        assert_eq!(
            rec_cfg.cap_records, 0,
            "recovered config is the prior relaxed cap"
        );
        assert_eq!(
            rec_cfg.ttl_ms, 0,
            "recovered config is the prior relaxed ttl"
        );
    }
}

/// A successful `put_topic` UPDATE (no fault) is fully durable: the tightened
/// config survives recovery (the WAL-first path logs before the response, so a
/// crash right after the ack still recovers the applied config).
#[test]
fn put_topic_update_success_is_durable() {
    let disk = FakeDisk::new();
    {
        let engine = open_engine(&disk);
        engine
            .put_topic(
                "cfg",
                TopicConfig {
                    r#type: TopicType::Log,
                    durable: true,
                    cap_records: 0,
                    ..Default::default()
                },
            )
            .unwrap();
        append(&engine, "cfg", "a");
        // Tighten the cap; this update fsyncs successfully.
        engine
            .put_topic(
                "cfg",
                TopicConfig {
                    r#type: TopicType::Log,
                    durable: true,
                    cap_records: 5,
                    ttl_ms: 123_000,
                    ..Default::default()
                },
            )
            .expect("update acked");
        sync_dirs(&disk);
        drop(engine);
    }

    let rec = recovered_config(&disk, "cfg");
    assert_eq!(rec.cap_records, 5, "the applied cap survives recovery");
    assert_eq!(rec.ttl_ms, 123_000, "the applied ttl survives recovery");
}
