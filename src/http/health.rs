//! Health / readiness / metrics endpoints (API §8). These do not require auth
//! by default; the auth middleware skips them unless `TOPICS_PROBE_AUTH`.

use super::AppState;
use crate::error::Error;
use crate::types::{ErrorCode, HealthResponse, ReadyResponse};
use axum::{
    extract::State,
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Json, Response},
};

/// Crate version, surfaced in `/v0/health`.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// `GET /v0/health` (alias `/healthz`) — liveness. Always `200`.
pub async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    let uptime_ms = state.engine.started_at.elapsed().as_millis() as i64;
    Json(HealthResponse {
        status: "ok".to_string(),
        version: VERSION.to_string(),
        uptime_ms,
    })
}

/// `GET /v0/ready` (alias `/readyz`) — readiness (API §8.2). `200 ready` once
/// restart recovery (snapshot load + WAL replay) has rebuilt the in-memory
/// state; `503 not_ready` while replay is in progress, carrying `Retry-After`
/// and `error.detail.replay_progress` (0.0–1.0). `/v0/health` stays `200`
/// throughout (liveness is independent of the ready gate).
pub async fn ready(State(state): State<AppState>) -> Response {
    if state.engine.is_ready() {
        return Json(ReadyResponse {
            status: "ready".to_string(),
            wal_replay_complete: true,
            topics: state.engine.topic_count(),
        })
        .into_response();
    }
    // Still replaying the WAL: `503 not_ready` with the canonical error envelope,
    // a `Retry-After`, and the replay progress so a probe/LB can back off.
    Error::new(ErrorCode::NotReady, "WAL replay in progress")
        .with_detail(serde_json::json!({
            "replay_progress": state.engine.replay_progress(),
        }))
        .with_retry_after(1)
        .into_response()
}

/// `GET /v0/metrics` — Prometheus text exposition by default; JSON snapshot
/// when `Accept: application/json`. Always `200`. Requires authentication (a
/// read-scoped key) when auth is enabled — it exposes operational state (topic
/// count), so it is not in the unauthenticated liveness/readiness probe set
/// (codex LOW #12).
pub async fn metrics(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let wants_json = headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .map(|a| a.contains("application/json"))
        .unwrap_or(false);

    if wants_json {
        (StatusCode::OK, Json(render_json(&state))).into_response()
    } else {
        let body = render_prometheus(&state);
        (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
            body,
        )
            .into_response()
    }
}

/// Cardinality cap on per-topic metric series (M3 / codex P2 #1): a deployment with
/// thousands of topics must not blow up a Prometheus scrape (and the engine's label
/// memory) with unbounded series. Beyond this the per-topic pass is truncated and a
/// `topics_topic_metrics_truncated` gauge flags it.
const MAX_PER_BOX_SERIES: usize = 1000;

/// Append a single Prometheus metric line with its `# HELP` / `# TYPE` header.
fn metric(out: &mut String, name: &str, help: &str, typ: &str, value: impl std::fmt::Display) {
    use std::fmt::Write;
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} {typ}");
    let _ = writeln!(out, "{name} {value}");
}

/// Escape a topic name for use as a Prometheus label value (backslash, double-quote,
/// newline) per the text exposition format, so an adversarial topic name cannot
/// inject extra series/lines.
fn escape_label(v: &str) -> String {
    let mut out = String::with_capacity(v.len());
    for c in v.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            _ => out.push(c),
        }
    }
    out
}

/// JSON snapshot mirror of the Prometheus exposition (`Accept: application/json`).
/// Carries the same gauges/counters in a single object so a tool that prefers
/// JSON over text parsing gets the full picture (M3).
fn render_json(state: &AppState) -> serde_json::Value {
    let eng = state.engine.metrics_snapshot();
    let mut snap = serde_json::json!({
        "topics": state.engine.topic_count(),
        "topics_memory": eng.topics_memory,
        "topics_disk": eng.topics_disk,
        "topics_fsync": eng.topics_fsync,
        "routers": state.engine.router_count(),
        "records_live": eng.records_live,
        "bytes_live": eng.bytes_live,
        "queue_topics": eng.queue_topics,
        "queue_leases_in_flight": eng.leases_in_flight,
        "sse_connections": state.live.sse_total(),
        "watch_sessions": state.sessions.len(),
        "ready": state.engine.is_ready(),
        "replay_progress": state.engine.replay_progress(),
        "uptime_ms": state.engine.started_at.elapsed().as_millis() as u64,
    });
    if let Some(w) = state.engine.wal_metrics() {
        use std::sync::atomic::Ordering::Relaxed;
        snap["wal"] = serde_json::json!({
            "fsyncs": w.fsyncs.load(Relaxed),
            "frames": w.frames.load(Relaxed),
            "batches": w.batches.load(Relaxed),
            "bytes_written": w.bytes_written.load(Relaxed),
            "rotations": w.rotations.load(Relaxed),
            "queue_depth": w.queued.load(Relaxed),
            "queue_depth_peak": w.queued_peak.load(Relaxed),
            "submit_full_total": w.submit_full.load(Relaxed),
            "read_only": w.read_only.load(Relaxed),
            "fsync_count": w.fsync_count.load(Relaxed),
            "fsync_micros_total": w.fsync_micros_total.load(Relaxed),
        });
    }
    snap
}

/// Render the Prometheus text exposition body (M3): engine topic/record/byte
/// gauges, WAL group-commit + fsync-latency histogram + queue-depth + rotation
/// counters, recovery progress, and queue-lease + SSE-connection gauges.
fn render_prometheus(state: &AppState) -> String {
    use std::fmt::Write;
    use std::sync::atomic::Ordering::Relaxed;

    let eng = state.engine.metrics_snapshot();
    let mut out = String::new();

    // --- Engine: topics / routers / records / bytes -------------------------
    metric(
        &mut out,
        "topics_topics",
        "Number of topics.",
        "gauge",
        state.engine.topic_count(),
    );
    // Topics broken down by durability class (single multi-series gauge).
    out.push_str("# HELP topics_topics_by_class Number of topics by durability class.\n");
    out.push_str("# TYPE topics_topics_by_class gauge\n");
    let _ = writeln!(
        out,
        "topics_topics_by_class{{class=\"memory\"}} {}",
        eng.topics_memory
    );
    let _ = writeln!(
        out,
        "topics_topics_by_class{{class=\"disk\"}} {}",
        eng.topics_disk
    );
    let _ = writeln!(
        out,
        "topics_topics_by_class{{class=\"fsync\"}} {}",
        eng.topics_fsync
    );
    metric(
        &mut out,
        "topics_routers",
        "Number of routers.",
        "gauge",
        state.engine.router_count(),
    );
    metric(
        &mut out,
        "topics_records_live",
        "Live (net-of-delete) records retained across all topics.",
        "gauge",
        eng.records_live,
    );
    metric(
        &mut out,
        "topics_bytes_live",
        "Retained payload bytes across all topics.",
        "gauge",
        eng.bytes_live,
    );

    // --- Per-topic gauges (M3 / codex P2 #1) ---------------------------------
    // Labeled by topic name, bounded to MAX_PER_BOX_SERIES to cap label cardinality.
    let (per_topic, total_topics) = state.engine.per_topic_metrics(MAX_PER_BOX_SERIES);
    out.push_str("# HELP topics_topic_head_seq Per-topic head seq (highest assigned).\n");
    out.push_str("# TYPE topics_topic_head_seq gauge\n");
    for m in &per_topic {
        let _ = writeln!(
            out,
            "topics_topic_head_seq{{topic=\"{}\"}} {}",
            escape_label(&m.name),
            m.head_seq
        );
    }
    out.push_str("# HELP topics_topic_earliest_seq Per-topic earliest retained seq.\n");
    out.push_str("# TYPE topics_topic_earliest_seq gauge\n");
    for m in &per_topic {
        let _ = writeln!(
            out,
            "topics_topic_earliest_seq{{topic=\"{}\"}} {}",
            escape_label(&m.name),
            m.earliest_seq
        );
    }
    out.push_str("# HELP topics_topic_records_live Per-topic live (net-of-delete) record count.\n");
    out.push_str("# TYPE topics_topic_records_live gauge\n");
    for m in &per_topic {
        let _ = writeln!(
            out,
            "topics_topic_records_live{{topic=\"{}\"}} {}",
            escape_label(&m.name),
            m.records_live
        );
    }
    out.push_str("# HELP topics_topic_bytes_live Per-topic retained payload bytes.\n");
    out.push_str("# TYPE topics_topic_bytes_live gauge\n");
    for m in &per_topic {
        let _ = writeln!(
            out,
            "topics_topic_bytes_live{{topic=\"{}\"}} {}",
            escape_label(&m.name),
            m.bytes_live
        );
    }
    // Queue ready / in-flight, only for queue topics (a label avoids emitting 0 for
    // every non-queue topic).
    out.push_str("# HELP topics_topic_queue_ready Per-queue-topic claimable jobs.\n");
    out.push_str("# TYPE topics_topic_queue_ready gauge\n");
    for m in &per_topic {
        if let Some(ready) = m.queue_ready {
            let _ = writeln!(
                out,
                "topics_topic_queue_ready{{topic=\"{}\"}} {}",
                escape_label(&m.name),
                ready
            );
        }
    }
    out.push_str("# HELP topics_topic_queue_in_flight Per-queue-topic leased (in-flight) jobs.\n");
    out.push_str("# TYPE topics_topic_queue_in_flight gauge\n");
    for m in &per_topic {
        if let Some(inflight) = m.queue_in_flight {
            let _ = writeln!(
                out,
                "topics_topic_queue_in_flight{{topic=\"{}\"}} {}",
                escape_label(&m.name),
                inflight
            );
        }
    }
    metric(
        &mut out,
        "topics_topic_metrics_truncated",
        "1 if the per-topic series were truncated at the cardinality cap, else 0.",
        "gauge",
        u8::from(total_topics > per_topic.len()),
    );

    // --- Queue (lease) gauges ----------------------------------------------
    metric(
        &mut out,
        "topics_queue_topics",
        "Number of queue topics (carry a lease projection).",
        "gauge",
        eng.queue_topics,
    );
    metric(
        &mut out,
        "topics_queue_leases_in_flight",
        "Jobs with an active (un-expired) lease across all queue topics.",
        "gauge",
        eng.leases_in_flight,
    );

    // --- SSE / watch-session gauges ----------------------------------------
    metric(
        &mut out,
        "topics_sse_connections",
        "Currently-open SSE (watch + work) connections.",
        "gauge",
        state.live.sse_total(),
    );
    metric(
        &mut out,
        "topics_watch_sessions",
        "Live watch sessions in the registry.",
        "gauge",
        state.sessions.len(),
    );

    // --- Recovery / readiness ----------------------------------------------
    metric(
        &mut out,
        "topics_ready",
        "1 once restart recovery has rebuilt in-memory state, else 0.",
        "gauge",
        u8::from(state.engine.is_ready()),
    );
    metric(
        &mut out,
        "topics_recovery_progress",
        "WAL-replay progress in [0,1]; 1.0 once ready.",
        "gauge",
        state.engine.replay_progress(),
    );
    metric(
        &mut out,
        "topics_uptime_ms",
        "Process uptime in milliseconds.",
        "counter",
        state.engine.started_at.elapsed().as_millis() as u64,
    );

    // --- WAL counters + fsync-latency histogram + queue depth --------------
    if let Some(w) = state.engine.wal_metrics() {
        metric(
            &mut out,
            "topics_wal_frames_total",
            "WAL frames written.",
            "counter",
            w.frames.load(Relaxed),
        );
        metric(
            &mut out,
            "topics_wal_batches_total",
            "WAL group-commit batches written.",
            "counter",
            w.batches.load(Relaxed),
        );
        metric(
            &mut out,
            "topics_wal_fsyncs_total",
            "WAL group-commit fsyncs.",
            "counter",
            w.fsyncs.load(Relaxed),
        );
        metric(
            &mut out,
            "topics_wal_bytes_written_total",
            "Bytes appended to the WAL.",
            "counter",
            w.bytes_written.load(Relaxed),
        );
        metric(
            &mut out,
            "topics_wal_rotations_total",
            "WAL file rotations.",
            "counter",
            w.rotations.load(Relaxed),
        );
        metric(
            &mut out,
            "topics_wal_queue_depth",
            "Submissions accepted into the bounded WAL ingest queue but not yet committed.",
            "gauge",
            w.queued.load(Relaxed),
        );
        metric(
            &mut out,
            "topics_wal_queue_depth_peak",
            "High-water mark of the WAL ingest queue depth.",
            "gauge",
            w.queued_peak.load(Relaxed),
        );
        metric(
            &mut out,
            "topics_wal_submit_full_total",
            "WAL submissions rejected because the bounded ingest queue was full (R5 backpressure).",
            "counter",
            w.submit_full.load(Relaxed),
        );
        metric(
            &mut out,
            "topics_wal_read_only",
            "1 if the WAL latched read-only after a rotation failure (R11), else 0.",
            "gauge",
            w.read_only.load(Relaxed),
        );

        // fsync-latency histogram (microseconds), cumulative `le` buckets.
        out.push_str(
            "# HELP topics_wal_fsync_latency_us WAL group-commit fsync latency (microseconds).\n",
        );
        out.push_str("# TYPE topics_wal_fsync_latency_us histogram\n");
        let count = w.fsync_count.load(Relaxed);
        for (i, le) in crate::storage::FSYNC_BUCKETS_US.iter().enumerate() {
            let _ = writeln!(
                out,
                "topics_wal_fsync_latency_us_bucket{{le=\"{le}\"}} {}",
                w.fsync_buckets[i].load(Relaxed)
            );
        }
        let _ = writeln!(
            out,
            "topics_wal_fsync_latency_us_bucket{{le=\"+Inf\"}} {count}"
        );
        // Histogram `_sum` is in the same unit as the buckets (microseconds).
        let _ = writeln!(
            out,
            "topics_wal_fsync_latency_us_sum {}",
            w.fsync_micros_total.load(Relaxed)
        );
        let _ = writeln!(out, "topics_wal_fsync_latency_us_count {count}");
    }

    out
}
