//! End-to-end smoke test exercising the documented README quickstart against
//! the real in-process axum app (the exact `http::build_router` the binary
//! serves), driven via `tower::ServiceExt::oneshot` — no sockets, no sleeps for
//! correctness; the one SSE read uses a bounded timeout and a fixed frame
//! budget so it can never hang.
//!
//! Flow (README §1–§6, ROADMAP Phase-2 acceptance):
//!   create topic -> write 2 records -> GET state -> diff (seqs/caught_up)
//!   -> tag-delete -> diff again (suppressed) -> router fan-out
//!   -> watch session + first SSE `record` + `caught-up` frames
//!   -> node loop-prevention (a node never reads its own records).

use std::sync::Arc;
use std::time::Duration;

use axum::{
    body::Body,
    http::{Request, StatusCode},
    Router,
};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tower::ServiceExt; // for `oneshot`

use topics::clock::{SharedClock, SystemClock};
use topics::config::ServerConfig;
use topics::engine::Engine;
use topics::http;

/// Build a fresh in-memory app (auth disabled, dev defaults).
fn app() -> Router {
    let clock: SharedClock = Arc::new(SystemClock);
    let engine = Engine::new(ServerConfig::default(), clock);
    http::build_router(engine)
}

/// Issue a JSON request and return `(status, parsed-body)`.
async fn req_json(
    app: &Router,
    method: &str,
    path: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(path);
    let body = match body {
        Some(v) => {
            builder = builder.header("content-type", "application/json");
            Body::from(serde_json::to_vec(&v).unwrap())
        }
        None => Body::empty(),
    };
    let request = builder.body(body).unwrap();
    let resp = app.clone().oneshot(request).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let value: Value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, value)
}

#[tokio::test]
async fn quickstart_end_to_end() {
    let app = app();

    // -- §1 Create a topic (explicit config) -----------------------------------
    let (status, body) = req_json(
        &app,
        "PUT",
        "/v0/topics/jobs",
        Some(json!({ "durable": true, "cap_records": 1_000_000, "ttl_ms": 0 })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "first PUT creates -> 201");
    assert_eq!(body["topic"], "jobs");
    assert_eq!(body["created"], true);
    // Documented config defaults must be echoed verbatim.
    let cfg = &body["config"];
    assert_eq!(cfg["ttl_ms"], 0);
    assert_eq!(cfg["cap_records"], 1_000_000);
    assert_eq!(cfg["cap_bytes"], 0);
    assert_eq!(cfg["discard"], "old");
    assert_eq!(cfg["durable"], true);
    assert_eq!(cfg["priority"], Value::Null);
    assert_eq!(cfg["auto_priority"], true);
    assert_eq!(cfg["auto_create"], true);
    assert_eq!(cfg["idempotency_window_ms"], 120_000);
    assert_eq!(cfg["dedupe_node"], true);
    assert!(body["performance"]["server_total_ms"].is_number());

    // A second PUT is idempotent -> 200, created:false.
    let (status, body) = req_json(&app, "PUT", "/v0/topics/jobs", Some(json!({}))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["created"], false);

    // -- §2 Write 2 records (server assigns seqs) ----------------------------
    let (status, body) = req_json(
        &app,
        "POST",
        "/v0/topics/jobs",
        Some(json!({
            "node": "worker-eu-1",
            "records": [
                { "data": { "url": "s3://b/a.png", "w": 256 }, "tag": "tenant42:job-9001" },
                { "data": { "url": "s3://b/b.png", "w": 512 }, "tag": "tenant42:job-9002" }
            ]
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "write to existing topic -> 200");
    assert_eq!(body["topic"], "jobs");
    assert_eq!(body["first_seq"], 1);
    assert_eq!(body["last_seq"], 2);
    assert_eq!(body["seqs"], json!([1, 2]));
    assert_eq!(body["head_seq"], 2);
    assert_eq!(body["count"], 2);
    assert_eq!(body["created"], false);
    assert_eq!(body["deduped"], false);
    // durable topic, phase 2: fsync is a no-op fast path -> 0.0.
    assert_eq!(body["performance"]["fsync_ms"], 0.0);

    // -- §3 GET state --------------------------------------------------------
    let (status, body) = req_json(&app, "GET", "/v0/topics/jobs", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["topic"], "jobs");
    assert_eq!(body["head_seq"], 2);
    assert_eq!(body["earliest_seq"], 1);
    assert_eq!(body["next_seq"], 3);
    assert_eq!(body["count"], 2);
    assert!(body["bytes"].as_u64().unwrap() > 0);

    // -- §4 diff from a cursor (a DIFFERENT node sees both records) -----------
    // `include_tags:true` so `$tag` is present (API §3: default is false).
    let (status, body) = req_json(
        &app,
        "POST",
        "/v0/topics/jobs/diff",
        Some(json!({ "from_seq": 0, "limit": 500, "node": "reader-x", "include_tags": true })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let records = body["records"].as_array().unwrap();
    assert_eq!(records.len(), 2, "reader-x is not the origin node");
    assert_eq!(records[0]["$seq"], 1);
    assert_eq!(records[0]["$tag"], "tenant42:job-9001");
    assert_eq!(records[1]["$seq"], 2);
    assert_eq!(body["next_from_seq"], 2);
    assert_eq!(body["head_seq"], 2);
    assert_eq!(body["earliest_seq"], 1);
    assert_eq!(body["caught_up"], true);
    assert_eq!(body["tombstone"], Value::Null);
    assert_eq!(body["lag"], 0);

    // -- node loop-prevention: the ORIGIN node sees none, cursor still advances
    let (status, body) = req_json(
        &app,
        "POST",
        "/v0/topics/jobs/diff",
        Some(json!({ "from_seq": 0, "limit": 500, "node": "worker-eu-1" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["records"].as_array().unwrap().len(),
        0,
        "loop prevention: origin node sees none of its own records"
    );
    assert_eq!(
        body["caught_up"], true,
        "cursor advanced to caught_up, not an empty loop"
    );
    assert_eq!(body["next_from_seq"], 2);

    // -- §6 Delete a job by exact tag, then a tenant by prefix glob ----------
    // Permanent, point-in-time, silent deletion (no persistent filter).
    let (status, body) = req_json(
        &app,
        "POST",
        "/v0/topics/jobs/delete",
        Some(json!({ "match": ["tag", "Eq", "tenant42:job-9001"] })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["topic"], "jobs");
    assert_eq!(body["deleted"], 1, "exactly one record removed");
    assert_eq!(
        body["earliest_seq"], 2,
        "deleting the FRONT record (seq 1) advances earliest to the next live seq (2)"
    );
    assert_eq!(body["count"], 1, "count drops, net of deletion");

    // diff again as a fresh reader: job-9001 is gone, NO tombstone (silent).
    let (status, body) = req_json(
        &app,
        "POST",
        "/v0/topics/jobs/diff",
        Some(json!({ "from_seq": 0, "limit": 500, "node": "reader-y" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let records = body["records"].as_array().unwrap();
    assert_eq!(records.len(), 1, "deleted record is removed from reads");
    assert_eq!(records[0]["$seq"], 2);
    assert_eq!(
        body["tombstone"],
        Value::Null,
        "permanent deletion never emits a tombstone (silent)"
    );
    assert_eq!(body["caught_up"], true);

    // prefix glob delete of the whole tenant -> the next reader sees nothing.
    let (status, body) = req_json(
        &app,
        "POST",
        "/v0/topics/jobs/delete",
        Some(json!({ "match": ["tag", "Glob", "tenant42:*"] })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["deleted"], 1, "remaining tenant42:* record removed");
    assert_eq!(body["count"], 0);
    let (_, body) = req_json(
        &app,
        "POST",
        "/v0/topics/jobs/diff",
        Some(json!({ "from_seq": 0, "limit": 500, "node": "reader-z" })),
    )
    .await;
    assert_eq!(
        body["records"].as_array().unwrap().len(),
        0,
        "prefix glob deletes the whole tenant"
    );
    assert_eq!(body["caught_up"], true);
    assert_eq!(body["tombstone"], Value::Null, "still silent");

    // -- §5-style router fan-out: src -> dst preserves $node ------------------
    // Fresh topics so tag-deletes above don't interfere.
    let (status, body) = req_json(
        &app,
        "PUT",
        "/v0/routers/feed-to-suba",
        Some(json!({ "source": "feed", "dest": "sub-a" })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["router"], "feed-to-suba");
    assert_eq!(body["created"], true);
    assert_eq!(body["source"], "feed");
    assert_eq!(body["dest"], "sub-a");

    // Write to the source; the record must appear in the dest with $node intact.
    // (`feed` was already lazily materialized when the router was created, so
    // the write is a 200 against an existing topic, not a 201 create.)
    let (status, wbody) = req_json(
        &app,
        "POST",
        "/v0/topics/feed",
        Some(json!({ "node": "origin-1", "records": [{ "data": { "v": 1 } }] })),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "router pre-created feed -> write is 200"
    );
    assert_eq!(wbody["created"], false);

    // Read the dest as an unrelated node so loop-prevention doesn't hide it.
    let (status, body) = req_json(
        &app,
        "POST",
        "/v0/topics/sub-a/diff",
        Some(json!({ "from_seq": 0, "limit": 100, "node": "consumer" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let records = body["records"].as_array().unwrap();
    assert_eq!(records.len(), 1, "router fanned the record into sub-a");
    assert_eq!(records[0]["data"]["v"], 1);
    assert_eq!(
        records[0]["$node"], "origin-1",
        "router preserves the origin $node"
    );

    // A cycle-creating router is rejected 409 router_cycle.
    let (status, body) = req_json(
        &app,
        "PUT",
        "/v0/routers/suba-to-feed",
        Some(json!({ "source": "sub-a", "dest": "feed" })),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "cycle -> 409");
    assert_eq!(body["error"]["code"], "router_cycle");

    // -- §5 Watch: create session + read first SSE record + caught-up --------
    let (status, body) = req_json(
        &app,
        "POST",
        "/v0/watch",
        Some(json!({ "topics": { "sub-a": { "from_seq": 0 } } })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let wid = body["wid"].as_str().unwrap().to_string();
    let stream_url = body["stream_url"].as_str().unwrap().to_string();
    assert_eq!(stream_url, format!("/v0/watch/{wid}"));
    assert!(wid.starts_with("wid_"));

    // Open the stream; read the first two named frames (the `record` then the
    // `caught-up` for the single backlog topic) with a hard timeout so the test
    // can never hang on the long-lived stream.
    let frames = read_sse_frames(&app, &stream_url, 2, Duration::from_secs(5)).await;
    let events: Vec<&str> = frames.iter().map(|f| f.event.as_str()).collect();
    assert!(
        events.contains(&"record"),
        "SSE must deliver a `record` frame; got {events:?}"
    );
    assert!(
        events.contains(&"caught-up"),
        "SSE must deliver a `caught-up` frame; got {events:?}"
    );

    // The record frame's payload matches the documented shape.
    let rec = frames.iter().find(|f| f.event == "record").unwrap();
    let data: Value = serde_json::from_str(&rec.data).unwrap();
    assert_eq!(data["topic"], "sub-a");
    assert_eq!(data["records"][0]["$seq"], 1);
    assert_eq!(data["records"][0]["data"]["v"], 1);
    assert!(
        !rec.id.is_empty(),
        "data-bearing frame carries a composite id"
    );

    let cu = frames.iter().find(|f| f.event == "caught-up").unwrap();
    let cu_data: Value = serde_json::from_str(&cu.data).unwrap();
    assert_eq!(cu_data["topic"], "sub-a");
    assert_eq!(cu_data["head_seq"], 1);
}

/// A parsed SSE frame (named event with `id`/`event`/`data`). Comments
/// (heartbeats, `retry:`) are skipped.
struct SseFrame {
    id: String,
    event: String,
    data: String,
}

/// Open the SSE stream and collect up to `max_frames` named-event frames,
/// abandoning the read after `deadline` so the test can never hang. The stream
/// is long-lived (it parks on `Notify`), so we stop once we have what we want.
async fn read_sse_frames(
    app: &Router,
    url: &str,
    max_frames: usize,
    deadline: Duration,
) -> Vec<SseFrame> {
    let request = Request::builder()
        .method("GET")
        .uri(url)
        .header("accept", "text/event-stream")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(request).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .starts_with("text/event-stream"));

    let mut body = resp.into_body();
    let mut buf: Vec<u8> = Vec::new();
    let mut frames: Vec<SseFrame> = Vec::new();

    let read = async {
        while frames.len() < max_frames {
            let Some(chunk) = body.frame().await else {
                break;
            };
            let chunk = chunk.unwrap();
            let Some(data) = chunk.data_ref() else {
                continue;
            };
            buf.extend_from_slice(data);
            drain_frames(&mut buf, &mut frames);
        }
    };

    // Bounded: if the producer parks with fewer than `max_frames`, time out and
    // return whatever we have (the record+caught-up arrive immediately).
    let _ = tokio::time::timeout(deadline, read).await;
    // Flush any trailing complete block accumulated before timeout.
    drain_frames(&mut buf, &mut frames);
    frames
}

/// Split the byte buffer on blank-line frame boundaries (`\n\n`) and parse each
/// complete frame, leaving any partial trailing block in `buf`.
fn drain_frames(buf: &mut Vec<u8>, out: &mut Vec<SseFrame>) {
    let text = String::from_utf8_lossy(buf).into_owned();
    let mut start = 0usize;
    let mut last_end = 0usize;
    let bytes = text.as_bytes();
    let mut i = 0usize;
    while i + 1 < bytes.len() {
        if bytes[i] == b'\n' && bytes[i + 1] == b'\n' {
            let end = i + 2;
            if let Some(frame) = parse_frame(&text[start..end]) {
                out.push(frame);
            }
            start = end;
            last_end = end;
            i += 2;
        } else {
            i += 1;
        }
    }
    if last_end > 0 {
        buf.drain(0..last_end.min(buf.len()));
    }
}

/// Parse a single SSE block into a named-event frame, or `None` for a comment
/// (`:` heartbeat) or a frame that carries only `retry:`.
fn parse_frame(raw: &str) -> Option<SseFrame> {
    let mut id = String::new();
    let mut event = String::new();
    let mut data = String::new();
    let mut has_event = false;
    for line in raw.lines() {
        if line.is_empty() || line.starts_with(':') {
            continue;
        }
        if let Some(v) = line.strip_prefix("id:") {
            id = v.trim().to_string();
        } else if let Some(v) = line.strip_prefix("event:") {
            event = v.trim().to_string();
            has_event = true;
        } else if let Some(v) = line.strip_prefix("data:") {
            if !data.is_empty() {
                data.push('\n');
            }
            data.push_str(v.strip_prefix(' ').unwrap_or(v));
        }
    }
    if has_event {
        Some(SseFrame { id, event, data })
    } else {
        None
    }
}
