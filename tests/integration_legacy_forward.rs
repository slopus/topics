//! Legacy synchronous in-line forwarding (`TOPICS_FORWARD_V2=0`) — the explicit
//! OPT-OUT path. The shipped default is the async + derived model
//! (`integration_forward_v2.rs`); these tests pin the legacy contract so the
//! opt-out keeps working and is not silently broken by the cutover (codex P1 #6).
//!
//! The legacy path is durable-BY-CONSTRUCTION (each forwarded dest record is its
//! own WAL append) but WAL-AMPLIFIED (N writes for an N-way fan-out, on the source
//! ack path) and PERMITS multi-source fan-in into a single dest (no single-owner
//! derived-dest rule).
//!
//! This is a dedicated test BINARY: it sets `TOPICS_FORWARD_V2=0` before any
//! engine is constructed, and the env capture happens at `Engine` construction, so
//! the whole binary runs legacy. The env set never races the v2-default tests (they
//! live in separate binaries with their own process env).

use std::sync::atomic::Ordering;
use std::sync::Arc;

use serde_json::json;
use topics::clock::{SharedClock, SystemClock};
use topics::config::ServerConfig;
use topics::engine::Engine;
use topics::types::{DiffRequest, RecordIn, RouterCreateRequest, TopicConfig, WriteRequest};

/// Force the legacy synchronous forwarding path for THIS test process. Idempotent;
/// every test in this binary wants it off, and the engine captures the flag at
/// construction (so this must run BEFORE `engine_at`).
fn force_legacy() {
    std::env::set_var("TOPICS_FORWARD_V2", "0");
}

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

fn durable_topic() -> TopicConfig {
    TopicConfig {
        durable: true,
        ..TopicConfig::default()
    }
}

fn router(source: &str, dest: &str) -> RouterCreateRequest {
    RouterCreateRequest {
        source: source.into(),
        dest: dest.into(),
        preserve_node: true,
        preserve_tag: true,
        create_dest: true,
        filter: None,
        allow_cycle: false,
    }
}

fn one(data: serde_json::Value, tag: Option<&str>, node: Option<&str>) -> WriteRequest {
    WriteRequest {
        records: vec![RecordIn {
            data,
            tag: tag.map(str::to_string),
            node: node.map(str::to_string),
            meta: None,
        }],
        node: None,
        idempotency_key: None,
        create: None,
        config: None,
        disable_backpressure: false,
    }
}

fn diff_from(from_seq: u64) -> DiffRequest {
    DiffRequest {
        from_seq,
        limit: 1000,
        include_tags: true,
        ..DiffRequest::default()
    }
}

/// Legacy forwarding is SYNCHRONOUS + in-line: the forwarded copy is visible in the
/// dest IMMEDIATELY after the source write returns, with no async worker / read-path
/// catch-up needed (contrast the v2 default, where the copy lands asynchronously).
#[test]
fn legacy_forward_is_synchronous_and_inline() {
    force_legacy();
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path());
    engine.put_topic("src", durable_topic()).unwrap();
    engine.put_topic("dst", durable_topic()).unwrap();
    engine.put_router("src->dst", router("src", "dst")).unwrap();

    engine
        .write("src", one(json!({ "x": 1 }), Some("t"), Some("n")), true)
        .unwrap();

    // No sleep, no catch-up call: the forwarded copy is already present (synchronous
    // in-line forwarding off the legacy path).
    let d = engine.diff("dst", diff_from(0)).unwrap();
    assert_eq!(d.records.len(), 1, "forwarded copy visible synchronously");
    assert_eq!(d.records[0].data, json!({ "x": 1 }));
    assert_eq!(d.records[0].tag.as_deref(), Some("t"), "$tag preserved");
    assert_eq!(d.records[0].node.as_deref(), Some("n"), "$node preserved");
}

/// Legacy forwarding is WAL-AMPLIFIED: a 1->N fan-out writes 1 (source) + N (dest)
/// WAL frames — the exact tradeoff the async/derived default removes (it writes ONE
/// frame regardless of fan-out, see `integration_forward_v2`).
#[test]
fn legacy_fan_out_is_wal_amplified() {
    force_legacy();
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path());

    let n_dests = 5u64;
    engine.put_topic("src", durable_topic()).unwrap();
    for i in 0..n_dests {
        let dest = format!("dst{i}");
        engine.put_topic(&dest, durable_topic()).unwrap();
        engine
            .put_router(
                &format!("src->{dest}"),
                RouterCreateRequest {
                    create_dest: false,
                    ..router("src", &dest)
                },
            )
            .unwrap();
    }

    let frames_before = engine.wal_metrics().unwrap().frames.load(Ordering::Relaxed);

    engine
        .write("src", one(json!({ "k": 1 }), None, Some("o")), true)
        .unwrap();

    let frames_after = engine.wal_metrics().unwrap().frames.load(Ordering::Relaxed);
    let appended = frames_after - frames_before;

    // 1 source append + N dest appends = N+1 WAL frames (amplification). All topics
    // are fsync-class so no `disk` head-watermark reservation frames are mixed in.
    assert_eq!(
        appended,
        n_dests + 1,
        "legacy 1->{n_dests} fan-out must write {} WAL frames (amplified); got {appended}",
        n_dests + 1
    );

    // Every dest synchronously got the copy.
    for i in 0..n_dests {
        let d = engine.diff(&format!("dst{i}"), diff_from(0)).unwrap();
        assert_eq!(d.records.len(), 1, "dst{i} got the forwarded copy");
    }
}

/// Legacy PERMITS multi-source fan-in: two routers with DIFFERENT sources into one
/// shared dest are both accepted (no `router_dest_fan_in` refusal — that is a
/// v2-only single-owner rule), and both sources' writes land in the dest.
#[test]
fn legacy_permits_multi_source_fan_in() {
    force_legacy();
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path());
    engine.put_topic("a", durable_topic()).unwrap();
    engine.put_topic("x", durable_topic()).unwrap();
    engine.put_topic("c", durable_topic()).unwrap();

    // a -> c and x -> c: under legacy BOTH succeed (v2 would refuse the second).
    engine.put_router("a->c", router("a", "c")).unwrap();
    engine
        .put_router("x->c", router("x", "c"))
        .expect("legacy permits a second router from a different source into the same dest");

    engine
        .write("a", one(json!({ "from": "a" }), None, None), true)
        .unwrap();
    engine
        .write("x", one(json!({ "from": "x" }), None, None), true)
        .unwrap();

    let d = engine.diff("c", diff_from(0)).unwrap();
    let datas: Vec<_> = d.records.iter().map(|r| r.data.clone()).collect();
    assert!(
        datas.contains(&json!({ "from": "a" })) && datas.contains(&json!({ "from": "x" })),
        "both fan-in sources land in the shared dest under legacy; got {datas:?}"
    );
}

/// Legacy recovery is durable-BY-CONSTRUCTION: the forwarded copy is recovered from
/// the DEST's OWN Append WAL frame (NOT re-derived from the source), so it survives
/// a restart even if the SOURCE has since been fully trimmed away. This is the
/// legacy-specific mechanism the v2 default replaces with source re-derivation.
#[test]
fn legacy_forwarded_copy_recovers_from_dest_wal_frame() {
    force_legacy();
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = engine_at(dir.path());
        engine.put_topic("src", durable_topic()).unwrap();
        engine.put_topic("dst", durable_topic()).unwrap();
        engine.put_router("src->dst", router("src", "dst")).unwrap();
        engine
            .write(
                "src",
                one(json!({ "fwd": 1 }), Some("ftag"), Some("wA")),
                true,
            )
            .unwrap();
        let pre = engine.diff("dst", diff_from(0)).unwrap();
        assert_eq!(pre.records.len(), 1, "forwarded copy present pre-restart");
        // Drop: the WAL drains + fsyncs, including the dest's OWN Append frame.
    }

    // Reopen: the dest copy is recovered from its own durable WAL frame, present
    // exactly once with fidelity — the legacy durable-by-construction contract.
    let engine = engine_at(dir.path());
    let st = engine.topic_state("dst", false).unwrap();
    assert_eq!(
        st.head_seq, 1,
        "forwarded durable copy recovered from dest WAL"
    );
    assert_eq!(st.count, 1);
    let d = engine.diff("dst", diff_from(0)).unwrap();
    assert_eq!(d.records.len(), 1, "present exactly once after restart");
    assert_eq!(d.records[0].data, json!({ "fwd": 1 }));
    assert_eq!(d.records[0].tag.as_deref(), Some("ftag"), "$tag preserved");
    assert_eq!(d.records[0].node.as_deref(), Some("wA"), "$node preserved");
}
