//! Forwarding fan-out micro-benchmark (Stage 4): measures the WAL-write
//! amplification and the write-ack latency for a single source write fanning to
//! 1 / 10 / 100 / 1000 router destinations, in the LEGACY synchronous path (v1,
//! `forward_from` on the write/ack path) vs. the ASYNC + derived path (v2,
//! `TOPICS_FORWARD_V2=1`: one WAL append per source write, forwarding off the ack
//! path).
//!
//! It is a deterministic, in-process measurement (the engine API directly, a real
//! durable WAL under a temp dir) so the WAL-frame delta is exact. The mode is
//! selected by the `TOPICS_FORWARD_V2` env var, which the engine captures at
//! construction — so the harness runs this binary TWICE (unset ⇒ v1, `=1` ⇒ v2)
//! and diffs the two reports.
//!
//! Run:
//! ```bash
//!   cargo run --release --example forward_fanout_bench            # v1 (sync)
//!   TOPICS_FORWARD_V2=1 cargo run --release --example forward_fanout_bench  # v2
//! ```

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use serde_json::json;
use topics::clock::{SharedClock, SystemClock};
use topics::config::ServerConfig;
use topics::engine::Engine;
use topics::types::{DiffRequest, RecordIn, RouterCreateRequest, TopicConfig, WriteRequest};

fn durable_engine(dir: &std::path::Path) -> Arc<Engine> {
    let clock: SharedClock = Arc::new(SystemClock);
    let cfg = ServerConfig {
        data_dir: Some(dir.to_string_lossy().to_string()),
        ..ServerConfig::default()
    };
    Engine::with_data_dir(cfg, clock).expect("durable engine")
}

fn write_one(engine: &Engine, topic_name: &str) -> std::time::Duration {
    let req = WriteRequest {
        records: vec![RecordIn {
            data: json!({"k": 1}),
            tag: None,
            node: Some("o".into()),
            meta: None,
        }],
        node: None,
        idempotency_key: None,
        create: None,
        config: None,
        disable_backpressure: false,
    };
    let t0 = Instant::now();
    engine.write(topic_name, req, false).expect("write");
    t0.elapsed()
}

/// Measure one fan-out degree: build a fresh durable engine with `fanout` dest
/// topics fed by `fanout` routers, then time a single source write and count the WAL
/// frames it produced. For v2 the forwarding is driven off the ack path; we then
/// fully drain it and time the end-to-end delivery so the async-forward latency is
/// reported too.
fn measure(fanout: usize, v2: bool) {
    let dir = tempfile::tempdir().unwrap();
    let engine = durable_engine(dir.path());

    // fsync class everywhere so the only frames a write produces are Appends (a
    // `disk` topic would add an R3 head-watermark frame and muddy the count).
    let fsync_cfg = || TopicConfig {
        durable: true,
        ..TopicConfig::default()
    };
    engine.put_topic("src", fsync_cfg()).unwrap();
    for i in 0..fanout {
        let dest = format!("d{i}");
        engine.put_topic(&dest, fsync_cfg()).unwrap();
        engine
            .put_router(
                &format!("src->{dest}"),
                RouterCreateRequest {
                    source: "src".into(),
                    dest: dest.clone(),
                    preserve_node: true,
                    preserve_tag: true,
                    create_dest: false,
                    filter: None,
                    allow_cycle: false,
                },
            )
            .unwrap();
    }

    let frames_before = engine.wal_metrics().unwrap().frames.load(Ordering::Relaxed);

    // The ACK latency: time of the source write call itself (returns immediately
    // under v2; pays the inline fan-out under v1). Average over a few writes after a
    // warm-up so the number is stable.
    let _ = write_one(&engine, "src"); // warm-up (first WAL extend etc.)
    let warm_frames = engine.wal_metrics().unwrap().frames.load(Ordering::Relaxed);

    let iters = 5u32;
    let mut ack_total = std::time::Duration::ZERO;
    for _ in 0..iters {
        ack_total += write_one(&engine, "src");
    }
    let ack_avg_us = ack_total.as_secs_f64() * 1e6 / iters as f64;

    // WAL frames produced by ONE source write (measured on the warm-up write so the
    // count excludes the cap/dir-extend one-time frames): (warm_frames - before).
    let frames_per_write = warm_frames - frames_before;

    // Async-forward delivery latency (v2): drive the background worker to drain all
    // routers and time it; for v1 forwarding already happened inline so this is ~0.
    let t0 = Instant::now();
    if v2 {
        // Drain until every router is caught up (read-path catch-up also works; use
        // the worker path to measure the async delivery cost).
        let mut guard = 0u32;
        loop {
            let n = engine.drain_router_sources();
            if n == 0 {
                break;
            }
            guard += 1;
            if guard > 1_000_000 {
                break;
            }
        }
    }
    let deliver_us = t0.elapsed().as_secs_f64() * 1e6;

    // Verify the fan-out actually landed (read one dest).
    let d = engine
        .diff(
            "d0",
            DiffRequest {
                from_seq: 0,
                limit: 10,
                ..DiffRequest::default()
            },
        )
        .unwrap();
    assert!(!d.records.is_empty(), "d0 received the forwarded copy");

    let mode = if v2 { "v2-async-derived" } else { "v1-sync" };
    println!(
        "{mode} fanout={fanout:>4}  wal_frames_per_source_write={frames_per_write:>5}  \
         writes_per_dest={:.4}  ack_avg_us={ack_avg_us:>10.3}  deliver_us={deliver_us:>10.3}",
        frames_per_write as f64 / fanout.max(1) as f64,
    );
}

fn main() {
    let v2 = topics::config::forward_v2_enabled();
    let mode = if v2 {
        "v2 (async + derived)"
    } else {
        "v1 (sync forward_from)"
    };
    println!("=== forwarding fan-out benchmark — mode: {mode} ===");
    println!(
        "(one source write fanning to N dests; wal_frames_per_source_write is the \
         amplification, ack_avg_us is the write-ack latency)"
    );
    for &fanout in &[1usize, 10, 100, 1000] {
        measure(fanout, v2);
    }
}
