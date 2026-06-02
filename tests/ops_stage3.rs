//! Stage-3 OPS tests: the fleshed-out `/v0/metrics` exporter (M3) and the
//! bounded graceful-drain + SSE wind-down on shutdown (M11). These drive the
//! REAL dual-protocol serve loop + router (`common::Harness`), so the production
//! metrics and shutdown paths are exercised end-to-end.

mod common;

use common::Harness;
use reqwest::StatusCode;
use serde_json::json;
use std::io::Read;
use std::time::{Duration, Instant};

/// Find the value of a single (label-less) Prometheus gauge/counter line.
fn metric_value(body: &str, name: &str) -> Option<f64> {
    for line in body.lines() {
        if line.starts_with('#') {
            continue;
        }
        // Match `name <value>` with no labels (a `{` after the name means a
        // labelled series, handled separately).
        if let Some(rest) = line.strip_prefix(name) {
            let rest = rest.trim_start();
            if rest.starts_with('{') {
                continue;
            }
            return rest.split_whitespace().next().and_then(|v| v.parse().ok());
        }
    }
    None
}

#[test]
fn metrics_exposes_new_gauges() {
    let h = Harness::start();

    // Create a durable topic (drives the WAL) and a queue topic (drives lease gauges).
    let (s, _) = h.put(
        "/v0/topics/jobs",
        json!({ "durable": true, "cap_records": 1_000_000 }),
    );
    assert_eq!(s, StatusCode::CREATED);
    let (s, _) = h.put("/v0/topics/q", json!({ "durable": true, "type": "queue" }));
    assert_eq!(s, StatusCode::CREATED);

    // Two durable writes ⇒ WAL frames + at least one group-commit fsync.
    let (s, _) = h.post(
        "/v0/topics/jobs",
        json!({ "records": [{ "data": { "n": 1 } }, { "data": { "n": 2 } }] }),
    );
    assert_eq!(s, StatusCode::OK);

    // -- Prometheus text exposition -----------------------------------------
    let (s, body) = h.get_text("/v0/metrics", None);
    assert_eq!(s, StatusCode::OK, "metrics 200");

    // Engine gauges.
    assert_eq!(
        metric_value(&body, "topics_topics"),
        Some(2.0),
        "two topics:\n{body}"
    );
    assert_eq!(
        metric_value(&body, "topics_records_live"),
        Some(2.0),
        "two live records:\n{body}"
    );
    assert_eq!(
        metric_value(&body, "topics_queue_topics"),
        Some(1.0),
        "one queue topic:\n{body}"
    );
    assert_eq!(
        metric_value(&body, "topics_sse_connections"),
        Some(0.0),
        "no open SSE connections:\n{body}"
    );
    assert_eq!(
        metric_value(&body, "topics_ready"),
        Some(1.0),
        "ready after recovery:\n{body}"
    );
    assert_eq!(
        metric_value(&body, "topics_recovery_progress"),
        Some(1.0),
        "recovery complete:\n{body}"
    );

    // WAL counters: frames > 0 and at least one fsync from the durable writes.
    let frames = metric_value(&body, "topics_wal_frames_total").expect("wal frames present");
    assert!(frames > 0.0, "wal frames written: {frames}\n{body}");
    let fsyncs = metric_value(&body, "topics_wal_fsyncs_total").expect("wal fsyncs present");
    assert!(fsyncs > 0.0, "at least one durable fsync: {fsyncs}\n{body}");

    // Queue-depth gauge present (steady-state 0 once committed) + backpressure
    // counter present.
    assert_eq!(
        metric_value(&body, "topics_wal_queue_depth"),
        Some(0.0),
        "queue drained at rest:\n{body}"
    );
    assert!(
        body.contains("topics_wal_submit_full_total"),
        "backpressure counter present:\n{body}"
    );
    assert_eq!(
        metric_value(&body, "topics_wal_read_only"),
        Some(0.0),
        "wal healthy (not read-only):\n{body}"
    );

    // fsync-latency histogram: the `+Inf` bucket equals the observation count and
    // the `_count` series is present and > 0.
    assert!(
        body.contains("topics_wal_fsync_latency_us_bucket{le=\"+Inf\"}"),
        "fsync histogram +Inf bucket present:\n{body}"
    );
    let hist_count =
        metric_value(&body, "topics_wal_fsync_latency_us_count").expect("histogram count present");
    assert!(hist_count > 0.0, "fsync observed: {hist_count}\n{body}");
    assert_eq!(
        hist_count, fsyncs,
        "histogram count tracks the fsync counter"
    );

    // Topics-by-class labelled gauge present (durable ⇒ fsync class).
    assert!(
        body.contains("topics_topics_by_class{class=\"fsync\"} 2"),
        "two fsync-class topics:\n{body}"
    );

    // Per-topic labelled gauges (M3 / codex P2 #1): the `jobs` topic has 2 records and
    // a head at seq 2; the queue topic `q` carries ready/in-flight series.
    assert!(
        body.contains("topics_topic_head_seq{topic=\"jobs\"} 2"),
        "per-topic head_seq for jobs:\n{body}"
    );
    assert!(
        body.contains("topics_topic_records_live{topic=\"jobs\"} 2"),
        "per-topic records_live for jobs:\n{body}"
    );
    assert!(
        body.contains("topics_topic_bytes_live{topic=\"jobs\"}"),
        "per-topic bytes_live for jobs:\n{body}"
    );
    assert!(
        body.contains("topics_topic_queue_ready{topic=\"q\"}"),
        "per-queue-topic ready gauge for q:\n{body}"
    );
    assert!(
        body.contains("topics_topic_queue_in_flight{topic=\"q\"}"),
        "per-queue-topic in-flight gauge for q:\n{body}"
    );
    // The non-queue topic must NOT emit a queue series.
    assert!(
        !body.contains("topics_topic_queue_ready{topic=\"jobs\"}"),
        "no queue gauge for a non-queue topic:\n{body}"
    );
    assert_eq!(
        metric_value(&body, "topics_topic_metrics_truncated"),
        Some(0.0),
        "per-topic series not truncated (2 topics):\n{body}"
    );

    // -- JSON content negotiation still works -------------------------------
    // Hit the JSON branch explicitly via the `Accept: application/json` header.
    let resp = reqwest::blocking::Client::new()
        .get(format!("{}/v0/metrics", h.base_url()))
        .header(reqwest::header::ACCEPT, "application/json")
        .send()
        .expect("json metrics request");
    assert_eq!(resp.status(), StatusCode::OK);
    let v: serde_json::Value = resp.json().expect("metrics json");
    assert_eq!(v["topics"], 2, "json topics:\n{v}");
    assert_eq!(v["queue_topics"], 1, "json queue topics:\n{v}");
    assert!(
        v["wal"]["frames"].as_u64().unwrap() > 0,
        "json wal frames:\n{v}"
    );
    assert!(
        v["sse_connections"].is_number(),
        "json sse gauge present:\n{v}"
    );
}

#[test]
fn shutdown_drains_sse_within_timeout() {
    let mut h = Harness::start();

    // A topic to watch.
    let (s, _) = h.put("/v0/topics/events", json!({ "durable": true }));
    assert_eq!(s, StatusCode::CREATED);

    // Create a watch session and open the SSE stream on a background thread. The
    // stream would tail forever (no more appends), so absent the M11 wind-down the
    // graceful drain would block until the connection is force-closed.
    let (s, sess) = h.post(
        "/v0/watch",
        json!({ "topics": { "events": { "from": "tail" } } }),
    );
    assert_eq!(s, StatusCode::OK, "watch created: {sess}");
    let stream_url = format!("{}{}", h.base_url(), sess["stream_url"].as_str().unwrap());

    let reader = std::thread::spawn(move || {
        let mut resp = reqwest::blocking::Client::new()
            .get(&stream_url)
            .header(reqwest::header::ACCEPT, "text/event-stream")
            .timeout(Duration::from_secs(30))
            .send()
            .expect("open SSE stream");
        assert_eq!(resp.status(), StatusCode::OK);
        // Read to EOF; the server's wind-down close ends the stream. Return how
        // long until the body completed.
        let start = Instant::now();
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        let mut saw_shutdown = false;
        loop {
            match resp.read(&mut chunk) {
                Ok(0) => break, // server closed the stream.
                Ok(n) => {
                    buf.extend_from_slice(&chunk[..n]);
                    if String::from_utf8_lossy(&buf).contains("server_shutting_down") {
                        saw_shutdown = true;
                    }
                }
                Err(_) => break,
            }
        }
        (start.elapsed(), saw_shutdown)
    });

    // Give the stream a beat to establish + register its SSE connection.
    std::thread::sleep(Duration::from_millis(300));

    // Trigger shutdown and time the bounded drain (shutdown() sends the signal +
    // joins the server thread). It must complete WELL under the 10s drain budget;
    // the SSE stream is actively wound down rather than waited out.
    let drain_start = Instant::now();
    h.shutdown();
    let drain_elapsed = drain_start.elapsed();
    assert!(
        drain_elapsed < Duration::from_secs(5),
        "graceful drain must complete promptly via SSE wind-down, took {drain_elapsed:?}"
    );

    // The streaming client should have observed the wind-down close frame and the
    // stream ended quickly.
    let (stream_dur, saw_shutdown) = reader.join().expect("reader thread");
    assert!(
        saw_shutdown,
        "client must receive the server_shutting_down close frame"
    );
    assert!(
        stream_dur < Duration::from_secs(5),
        "SSE stream must close promptly on shutdown, ran {stream_dur:?}"
    );
}

/// codex P1 #3: many concurrent creates of the SAME new topic name must resolve to
/// exactly ONE topic, and after a restart (WAL replay) there must be NO orphan topic
/// (the losing creates must not have logged a create frame under their own
/// distinct topic id that replay would materialize as a second topic). The serialized
/// WAL-first create routes a duplicate through the normal update path under the
/// winner's topic id, so the durable log and the live registry always agree.
#[test]
fn concurrent_same_name_create_leaves_no_orphan_after_restart() {
    use std::sync::Arc;
    use topics::clock::SharedClock;
    use topics::config::ServerConfig;
    use topics::engine::Engine;
    use topics::types::TopicConfig;

    let data_dir = tempfile::tempdir().expect("temp data dir");
    let cfg = ServerConfig {
        data_dir: Some(data_dir.path().to_string_lossy().into_owned()),
        ..ServerConfig::default()
    };

    // First boot: 16 threads race to create the same durable topic name.
    let created_count = {
        let clock: SharedClock = Arc::new(topics::clock::SystemClock);
        let engine = Engine::with_data_dir(cfg.clone(), clock).expect("durable engine");
        let mut handles = Vec::new();
        for _ in 0..16 {
            let e = engine.clone();
            handles.push(std::thread::spawn(move || {
                let bc = TopicConfig {
                    durable: true,
                    ..TopicConfig::default()
                };
                let (created, _cfg) = e.put_topic("racy", bc).expect("put_topic ok");
                created
            }));
        }
        let created_count = handles
            .into_iter()
            .map(|h| h.join().expect("thread"))
            .filter(|c| *c)
            .count();
        // Exactly one create won; the rest resolved as updates.
        assert_eq!(created_count, 1, "exactly one create wins the name race");
        // Exactly one live topic.
        assert_eq!(engine.topic_count(), 1, "one live topic after the race");
        created_count
    };
    assert_eq!(created_count, 1);

    // Second boot: replay the WAL. There must be EXACTLY ONE topic and NO orphan
    // (a losing create logging under its own topic id would replay as a `topic-<id>`).
    {
        let clock: SharedClock = Arc::new(topics::clock::SystemClock);
        let engine = Engine::with_data_dir(cfg, clock).expect("reopen durable engine");
        assert_eq!(
            engine.topic_count(),
            1,
            "exactly one topic survives replay — no orphan from a losing create"
        );
        assert!(
            engine.get_topic("racy").is_some(),
            "the named topic survives replay"
        );
    }
}
