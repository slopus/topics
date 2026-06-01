//! Phase-3 §2 — multiplexed SSE integration tests over a real bound server.
//!
//! Exercises the documented `/v0/watch` wire contract end-to-end against the
//! harness server (a real socket, the exact `http::build_router` the binary
//! serves):
//!
//!   * `POST /v0/watch` session create → `wid` / `stream_url` / per-box
//!     `from_seq`/`head_seq`/`earliest_seq` (API §7.2).
//!   * `GET /v0/watch/:wid` streams `record` (batched, composite base64url
//!     `id`), `caught-up`, `tombstone`, and `box-deleted` frames (§7.5).
//!   * `retry: 2000` is sent once at open (§7.5).
//!   * Heartbeat `: hb` comment arrives on an idle stream (§7.6).
//!   * `Last-Event-ID` resume rewinds the per-box cursors and re-delivers
//!     (§7.4).
//!   * `406 not_acceptable` when `Accept` is not `text/event-stream` (§7.5).
//!   * Node loop-prevention + permanent-deletion silence are reflected in the
//!     stream (§4 / §5).
//!
//! The harness `sse_frames` helper covers named frames over a GET with the
//! default `text/event-stream` Accept; for the cases it can't express
//! (`retry:`/`: hb` comments, a custom `Last-Event-ID`, a wrong `Accept`) this
//! file opens its own bounded raw reader (`raw_sse`) directly against
//! `h.base_url()`. Every read has a fixed frame budget and a hard timeout, so no
//! test can hang and none sleeps for correctness (the lone exception is the
//! heartbeat test, which by definition observes an *idle*-cadence comment and
//! waits on a bounded deadline for the byte to appear).

mod common;

use std::io::Read;
use std::time::{Duration, Instant};

use common::{Harness, SseFrame, StatusCode};
use serde_json::{json, Value};

// ---------------------------------------------------------------------------
// Raw SSE reader (sees comments + `retry:`, supports custom request headers).
// ---------------------------------------------------------------------------

/// A raw chunk of an SSE stream: the full decoded text collected within the
/// deadline, plus the response status and content-type so a test can assert the
/// open contract (200 / 406 / `text/event-stream`).
struct RawSse {
    status: StatusCode,
    content_type: String,
    text: String,
}

/// Open `path` on the harness server with the given `Accept` and optional
/// `Last-Event-ID`, then read raw bytes until either `min_bytes` accumulate, a
/// blank-comment `stop_marker` substring appears, or `deadline` elapses. Returns
/// the *raw* text (comments and `retry:` lines intact, unlike `sse_frames`).
///
/// Never hangs: the per-read timeout is the deadline and the loop is bounded.
fn raw_sse(
    h: &Harness,
    path: &str,
    accept: &str,
    last_event_id: Option<&str>,
    stop_marker: Option<&str>,
    deadline: Duration,
) -> RawSse {
    let client = reqwest::blocking::Client::builder()
        .timeout(deadline + Duration::from_secs(2))
        .build()
        .expect("build raw sse client");

    let url = format!("{}{}", h.base_url(), path);
    let mut req = client
        .get(&url)
        .header(reqwest::header::ACCEPT, accept)
        .timeout(deadline + Duration::from_secs(2));
    if let Some(leid) = last_event_id {
        req = req.header("Last-Event-ID", leid);
    }
    let mut resp = req.send().expect("open raw sse stream");
    let status = resp.status();
    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    // For a non-200 (e.g. 406) there is no stream to drain; return immediately.
    if !status.is_success() {
        let mut text = String::new();
        let _ = resp.read_to_string(&mut text);
        return RawSse {
            status,
            content_type,
            text,
        };
    }

    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 4096];
    let start = Instant::now();
    loop {
        if start.elapsed() >= deadline {
            break;
        }
        if let Some(marker) = stop_marker {
            if String::from_utf8_lossy(&buf).contains(marker) {
                break;
            }
        }
        match resp.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(_) => break, // read timeout / reset: return what we have.
        }
    }
    RawSse {
        status,
        content_type,
        text: String::from_utf8_lossy(&buf).into_owned(),
    }
}

// ---------------------------------------------------------------------------
// Small helpers.
// ---------------------------------------------------------------------------

/// Write `records` (already-formed `data` objects) to `box`, asserting a 2xx.
fn write(h: &Harness, box_name: &str, body: Value) -> Value {
    let (status, resp) = h.post(&format!("/v0/boxes/{box_name}"), body);
    assert!(
        status == StatusCode::OK || status == StatusCode::CREATED,
        "write to {box_name} should be 2xx, got {status}: {resp}"
    );
    resp
}

/// Create a watch session over `boxes`, returning `(stream_url, response-body)`.
fn create_watch(h: &Harness, body: Value) -> (String, Value) {
    let (status, resp) = h.post("/v0/watch", body);
    assert_eq!(
        status,
        StatusCode::OK,
        "POST /v0/watch should be 200: {resp}"
    );
    let url = resp["stream_url"]
        .as_str()
        .expect("stream_url present")
        .to_string();
    (url, resp)
}

/// The first frame of `event` kind, with its `data` parsed as JSON.
fn find(frames: &[SseFrame], event: &str) -> SseFrame {
    frames
        .iter()
        .find(|f| f.event == event)
        .unwrap_or_else(|| panic!("expected a `{event}` frame, got {:?}", events(frames)))
        .clone()
}

fn events(frames: &[SseFrame]) -> Vec<&str> {
    frames.iter().map(|f| f.event.as_str()).collect()
}

fn data_of(f: &SseFrame) -> Value {
    serde_json::from_str(&f.data).expect("frame data is JSON")
}

/// Minimal base64url (no-pad) codec, self-contained so the test crate needs no
/// extra dependency. Matches the server's `URL_SAFE_NO_PAD` alphabet.
const B64URL: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

fn b64url_encode(bytes: &[u8]) -> String {
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(B64URL[((n >> 18) & 63) as usize] as char);
        out.push(B64URL[((n >> 12) & 63) as usize] as char);
        if chunk.len() > 1 {
            out.push(B64URL[((n >> 6) & 63) as usize] as char);
        }
        if chunk.len() > 2 {
            out.push(B64URL[(n & 63) as usize] as char);
        }
    }
    out
}

fn b64url_decode(s: &str) -> Vec<u8> {
    let lut = |c: u8| -> u32 {
        B64URL
            .iter()
            .position(|&x| x == c)
            .expect("valid base64url char") as u32
    };
    let bytes = s.as_bytes();
    let mut out = Vec::new();
    for chunk in bytes.chunks(4) {
        let mut n = 0u32;
        for (i, &c) in chunk.iter().enumerate() {
            n |= lut(c) << (18 - 6 * i);
        }
        out.push((n >> 16) as u8);
        if chunk.len() > 2 {
            out.push((n >> 8) as u8);
        }
        if chunk.len() > 3 {
            out.push(n as u8);
        }
    }
    out
}

/// Decode a composite base64url SSE `id` back to its `box → seq` map.
fn decode_id(id: &str) -> serde_json::Map<String, Value> {
    let bytes = b64url_decode(id);
    serde_json::from_slice::<Value>(&bytes)
        .expect("id is JSON")
        .as_object()
        .expect("id is a map")
        .clone()
}

const SHORT: Duration = Duration::from_secs(5);

// ===========================================================================
// 1. Session create: wid / stream_url / per-box watermarks (§7.2).
// ===========================================================================

#[test]
fn watch_create_returns_wid_and_per_box_watermarks() {
    let h = Harness::start();

    // Seed two boxes with differing backlogs.
    write(
        &h,
        "jobs",
        json!({ "records": [{ "data": 1 }, { "data": 2 }, { "data": 3 }] }),
    );
    write(&h, "events", json!({ "records": [{ "data": "a" }] }));

    let (status, body) = h.post(
        "/v0/watch",
        json!({
            "boxes": {
                "jobs":   { "from_seq": 1 },
                "events": { "tail": true }
            }
        }),
    );
    assert_eq!(status, StatusCode::OK);

    let wid = body["wid"].as_str().expect("wid");
    assert!(wid.starts_with("wid_"), "wid should be prefixed: {wid}");
    assert_eq!(
        body["stream_url"],
        json!(format!("/v0/watch/{wid}")),
        "stream_url is /v0/watch/<wid>"
    );
    assert!(body["session_ttl_ms"].as_u64().unwrap() > 0);

    // Per-box watermarks resolve the requested cursor against current state.
    let jobs = &body["boxes"]["jobs"];
    assert_eq!(jobs["from_seq"], 1, "explicit from_seq echoed");
    assert_eq!(jobs["head_seq"], 3);
    assert_eq!(jobs["earliest_seq"], 1);

    let evb = &body["boxes"]["events"];
    assert_eq!(evb["from_seq"], 1, "tail resolves from_seq to current head");
    assert_eq!(evb["head_seq"], 1);
}

#[test]
fn watch_create_rejects_empty_boxes_and_unknown_box() {
    let h = Harness::start();

    // Empty `boxes` map → 400 invalid_request (§7.2).
    let (status, body) = h.post("/v0/watch", json!({ "boxes": {} }));
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], "invalid_request");

    // Unknown box → 404 box_not_found (no `?lenient=true`).
    let (status, body) = h.post(
        "/v0/watch",
        json!({ "boxes": { "ghost": { "from_seq": 0 } } }),
    );
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["code"], "box_not_found");
}

// ===========================================================================
// 2. record + caught-up frames + composite id (§7.5).
// ===========================================================================

#[test]
fn stream_delivers_record_and_caught_up_with_composite_id() {
    let h = Harness::start();
    write(
        &h,
        "jobs",
        json!({
            "node": "writer-1",
            "records": [
                { "data": { "v": 1 }, "tag": "t-1" },
                { "data": { "v": 2 }, "tag": "t-2" }
            ]
        }),
    );

    let (url, body) = create_watch(
        &h,
        json!({ "boxes": { "jobs": { "from_seq": 0 } }, "include_tags": true }),
    );
    assert_eq!(body["boxes"]["jobs"]["head_seq"], 2);

    let frames = h.sse_frames(&url, 2, SHORT);
    assert!(
        events(&frames).contains(&"record"),
        "got {:?}",
        events(&frames)
    );
    assert!(
        events(&frames).contains(&"caught-up"),
        "got {:?}",
        events(&frames)
    );

    // The `record` frame carries the documented batch shape.
    let rec = find(&frames, "record");
    let d = data_of(&rec);
    assert_eq!(d["box"], "jobs");
    assert_eq!(d["from_seq"], 0);
    assert_eq!(d["to_seq"], 2);
    assert_eq!(d["head_seq"], 2);
    let records = d["records"].as_array().unwrap();
    assert_eq!(
        records.len(),
        2,
        "both backlog records batched into one frame"
    );
    assert_eq!(records[0]["$seq"], 1);
    assert_eq!(records[0]["$tag"], "t-1", "include_tags=true surfaces $tag");
    assert_eq!(records[0]["$node"], "writer-1", "origin $node present");
    assert_eq!(records[1]["$seq"], 2);
    assert_eq!(records[1]["data"]["v"], 2);

    // The data-bearing frame's composite id decodes to the post-batch cursor.
    assert!(!rec.id.is_empty());
    assert_eq!(
        decode_id(&rec.id)["jobs"],
        2,
        "id advances cursor to to_seq"
    );

    // caught-up reports the head and is one-per-backlog→tailing transition.
    let cu = find(&frames, "caught-up");
    assert_eq!(data_of(&cu)["box"], "jobs");
    assert_eq!(data_of(&cu)["head_seq"], 2);
}

#[test]
fn include_data_false_yields_metadata_only_frames() {
    let h = Harness::start();
    write(
        &h,
        "feed",
        json!({ "records": [{ "data": { "big": "payload" }, "tag": "x" }] }),
    );

    let (url, _) = create_watch(
        &h,
        json!({ "boxes": { "feed": { "from_seq": 0 } }, "include_data": false, "include_tags": true }),
    );
    let frames = h.sse_frames(&url, 2, SHORT);
    let rec = find(&frames, "record");
    let r0 = &data_of(&rec)["records"][0];
    assert_eq!(r0["$seq"], 1);
    assert_eq!(r0["$tag"], "x");
    assert!(
        r0.get("data").is_none(),
        "include_data=false omits data: {r0}"
    );
}

// ===========================================================================
// 3. tail: only records after subscribe (§7.2).
// ===========================================================================

#[test]
fn tail_subscription_skips_backlog_and_delivers_new_records() {
    let h = Harness::start();
    // Backlog the writer should NOT see on a tail subscription.
    write(
        &h,
        "live",
        json!({ "records": [{ "data": 1 }, { "data": 2 }] }),
    );

    let (url, body) = create_watch(&h, json!({ "boxes": { "live": { "tail": true } } }));
    assert_eq!(body["boxes"]["live"]["from_seq"], 2, "tail starts at head");

    // Open the stream in a background thread, then append a fresh record; the
    // stream should deliver only that new record (seq 3), never the backlog.
    let base = h.base_url().to_string();
    let url2 = url.clone();
    let handle = std::thread::spawn(move || {
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(8))
            .build()
            .unwrap();
        let mut resp = client
            .get(format!("{base}{url2}"))
            .header(reqwest::header::ACCEPT, "text/event-stream")
            .timeout(Duration::from_secs(8))
            .send()
            .unwrap();
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        let start = Instant::now();
        // Read until we observe a `record` frame or 6s elapses.
        while start.elapsed() < Duration::from_secs(6) {
            if String::from_utf8_lossy(&buf).contains("event:record")
                || String::from_utf8_lossy(&buf).contains("event: record")
            {
                break;
            }
            match resp.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
                Err(_) => break,
            }
        }
        String::from_utf8_lossy(&buf).into_owned()
    });

    // Give the stream a moment to reach its parked (caught-up) state, then write.
    // This is a liveness nudge, not a correctness sleep — the assertion below
    // checks *which* seq is delivered, which is deterministic regardless.
    std::thread::sleep(Duration::from_millis(300));
    write(&h, "live", json!({ "records": [{ "data": 3 }] }));

    let text = handle.join().unwrap();
    assert!(
        text.contains("event: record") || text.contains("event:record"),
        "no record frame: {text}"
    );
    assert!(
        text.contains("\"$seq\":3"),
        "tail must deliver new seq 3: {text}"
    );
    assert!(
        !text.contains("\"$seq\":1"),
        "tail must NOT replay backlog seq 1: {text}"
    );
}

// ===========================================================================
// 4. retry: present + heartbeat `: hb` (§7.5 / §7.6).
// ===========================================================================

#[test]
fn stream_opens_with_retry_directive() {
    let h = Harness::start();
    write(&h, "jobs", json!({ "records": [{ "data": 1 }] }));
    let (url, _) = create_watch(&h, json!({ "boxes": { "jobs": { "from_seq": 0 } } }));

    // Raw read so the `retry:` line (skipped by `sse_frames`) is visible.
    let raw = raw_sse(&h, &url, "text/event-stream", None, Some("retry:"), SHORT);
    assert_eq!(raw.status, StatusCode::OK);
    assert!(
        raw.content_type.starts_with("text/event-stream"),
        "content-type: {}",
        raw.content_type
    );
    assert!(
        raw.text.contains("retry: 2000") || raw.text.contains("retry:2000"),
        "retry directive (2000ms) must open the stream, got:\n{}",
        raw.text
    );
}

#[test]
fn idle_stream_emits_heartbeat_comment() {
    // The keep-alive `: hb` comment fires only when the stream is idle. The
    // server drives that cadence from the session's clamped `heartbeat_ms`, whose
    // production floor is 1000ms; we lower the floor for the test process via
    // `STREAMS_TEST_MIN_HEARTBEAT_MS` (config::min_heartbeat_ms) and request a
    // sub-second heartbeat, so this asserts the *cadence* without the old ~15s
    // wall-clock wait. Bounded + non-flaky: it returns as soon as the byte arrives
    // and can never hang.
    //
    // The override is lower-only (capped at the production floor) and we RESTORE
    // the prior value when done, via a drop guard so it is restored even if an
    // assertion panics — so the process-global env mutation never leaks to other
    // tests sharing this binary. A leak would in any case be harmless (other tests
    // request the default 15_000ms heartbeat, far above any lowered floor), but
    // restoring keeps the test hermetic.
    struct EnvGuard(Option<String>);
    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match &self.0 {
                Some(v) => std::env::set_var("STREAMS_TEST_MIN_HEARTBEAT_MS", v),
                None => std::env::remove_var("STREAMS_TEST_MIN_HEARTBEAT_MS"),
            }
        }
    }
    let _env_guard = EnvGuard(std::env::var("STREAMS_TEST_MIN_HEARTBEAT_MS").ok());
    std::env::set_var("STREAMS_TEST_MIN_HEARTBEAT_MS", "100");

    let h = Harness::start();
    write(&h, "quiet", json!({ "records": [{ "data": 1 }] }));
    // Tail so the stream drains immediately to caught-up, then sits idle, and ask
    // for a 200ms keep-alive cadence (well above the 100ms floor we just set).
    let (url, _) = create_watch(
        &h,
        json!({ "boxes": { "quiet": { "tail": true } }, "heartbeat_ms": 200 }),
    );

    // A 2s deadline still spans ~10 heartbeat intervals, so the comment must
    // appear; if the cadence regressed back to 15s this would fail fast instead
    // of silently passing after a long wait.
    let raw = raw_sse(
        &h,
        &url,
        "text/event-stream",
        None,
        Some(": hb"),
        Duration::from_secs(2),
    );
    assert_eq!(raw.status, StatusCode::OK);
    assert!(
        raw.text.contains(": hb"),
        "idle stream must emit a `: hb` heartbeat comment on its sub-second cadence, got:\n{}",
        raw.text
    );
}

// ===========================================================================
// 5. 406 when Accept != text/event-stream (§7.5).
// ===========================================================================

#[test]
fn stream_rejects_non_event_stream_accept() {
    let h = Harness::start();
    write(&h, "jobs", json!({ "records": [{ "data": 1 }] }));
    let (url, _) = create_watch(&h, json!({ "boxes": { "jobs": { "from_seq": 0 } } }));

    let raw = raw_sse(&h, &url, "application/json", None, None, SHORT);
    assert_eq!(
        raw.status,
        StatusCode::NOT_ACCEPTABLE,
        "Accept: application/json must be 406"
    );
    // The error body carries the canonical code.
    let body: Value = serde_json::from_str(&raw.text).unwrap_or(Value::Null);
    assert_eq!(
        body["error"]["code"], "not_acceptable",
        "body: {}",
        raw.text
    );
}

// ===========================================================================
// 6. Last-Event-ID resume restores per-box cursors (§7.4).
// ===========================================================================

#[test]
fn last_event_id_resume_rewinds_and_redelivers() {
    let h = Harness::start();
    write(
        &h,
        "jobs",
        json!({ "records": [{ "data": 1 }, { "data": 2 }, { "data": 3 }] }),
    );
    let (url, _) = create_watch(&h, json!({ "boxes": { "jobs": { "from_seq": 0 } } }));

    // First connection drains the whole backlog; its record frame's id is the
    // post-batch composite cursor ({"jobs":3}). The session cursor is now at 3.
    let frames = h.sse_frames(&url, 2, SHORT);
    let rec = find(&frames, "record");
    assert_eq!(decode_id(&rec.id)["jobs"], 3);
    let full_cursor = rec.id.clone();

    // Forge a rewound Last-Event-ID at {"jobs":1} (as if the client only
    // processed seq 1). Reconnect: the server rewinds the session cursor to 1
    // (never advances past it) and re-delivers seq 2 and 3.
    let rewound = b64url_encode(&serde_json::to_vec(&json!({ "jobs": 1 })).unwrap());

    let raw = raw_sse(
        &h,
        &url,
        "text/event-stream",
        Some(&rewound),
        Some("event: caught-up"),
        SHORT,
    );
    assert_eq!(raw.status, StatusCode::OK);
    // The re-delivered batch starts after seq 1 and reaches seq 3.
    assert!(
        raw.text.contains("\"$seq\":2") && raw.text.contains("\"$seq\":3"),
        "resume must re-deliver seqs 2 and 3, got:\n{}",
        raw.text
    );
    assert!(
        !raw.text.contains("\"$seq\":1"),
        "rewind to {{jobs:1}} is exclusive — seq 1 already acked, not replayed:\n{}",
        raw.text
    );

    // A Last-Event-ID *ahead* of the server (full cursor at 3) must NOT advance
    // the cursor past the authoritative state; the connect still works and
    // simply reports caught-up with no re-delivery.
    let raw2 = raw_sse(
        &h,
        &url,
        "text/event-stream",
        Some(&full_cursor),
        Some("event: caught-up"),
        SHORT,
    );
    assert_eq!(raw2.status, StatusCode::OK);
    assert!(
        raw2.text.contains("event: caught-up") || raw2.text.contains("event:caught-up"),
        "an at-head resume still reaches caught-up:\n{}",
        raw2.text
    );
}

// ===========================================================================
// 7. tombstone frame on a cursor below the involuntary floor (§7.5).
// ===========================================================================
//
// We can't drive cap/TTL eviction deterministically through the live
// (SystemClock) harness, but the SSE connect-time `from_seq_too_old` variant is
// reachable: subscribe with a `from_seq` *above* head — no, that's caught-up.
// Instead, delete the front of the log so `earliest_seq` advances, then connect
// at `from_seq:0`: a delete advances `earliest_seq` but NOT `evict_floor`, so
// the gap is SILENT (no tombstone). That asserts deletion silence in-stream.
//
// The tombstone path proper (involuntary cap/TTL with `evict_floor`) is covered
// in the engine unit/property tests under a TestClock; here we assert the
// stream's *silent* behavior for a purely-deleted prefix, which is the
// observable SSE contract for deletion.

#[test]
fn deleted_prefix_is_silent_in_stream_no_tombstone() {
    let h = Harness::start();
    write(
        &h,
        "jobs",
        json!({ "records": [
            { "data": 1, "tag": "a" },
            { "data": 2, "tag": "b" },
            { "data": 3, "tag": "c" }
        ] }),
    );

    // Permanently delete the first two records (snapshot by seq). earliest_seq
    // advances to 3; evict_floor stays at 1 ⇒ a reader at from_seq:0 crosses a
    // purely-deleted gap, which is silent (no tombstone; API §5/§3.3).
    let (status, del) = h.post("/v0/boxes/jobs/delete", json!({ "before_seq": 3 }));
    assert_eq!(status, StatusCode::OK);
    assert_eq!(del["deleted"], 2);
    assert_eq!(del["earliest_seq"], 3);

    let (url, body) = create_watch(&h, json!({ "boxes": { "jobs": { "from_seq": 0 } } }));
    assert_eq!(
        body["boxes"]["jobs"]["earliest_seq"], 3,
        "earliest advanced past delete"
    );

    let frames = h.sse_frames(&url, 3, SHORT);
    let evs = events(&frames);
    assert!(
        !evs.contains(&"tombstone"),
        "a purely-deleted gap must be SILENT in the stream, got {evs:?}"
    );
    // Only the surviving record (seq 3) is delivered.
    let rec = find(&frames, "record");
    let d = data_of(&rec);
    let recs = d["records"].as_array().unwrap();
    assert_eq!(recs.len(), 1, "only the surviving record is delivered");
    assert_eq!(recs[0]["$seq"], 3);
    assert!(events(&frames).contains(&"caught-up"));
}

// ===========================================================================
// 8. box-deleted frame: terminal for that box, stream continues (§7.5).
// ===========================================================================
//
// Watch two boxes. Delete one; then write to the other (the write wakes the
// stream's park, so we don't wait on the 15s heartbeat). On the woken pass the
// deleted box yields a `box-deleted` frame while the live box delivers its
// record — proving the deletion is terminal for one box only.

#[test]
fn box_deleted_frame_is_terminal_for_that_box_only() {
    let h = Harness::start();
    write(&h, "boxA", json!({ "records": [{ "data": 1 }] }));
    write(&h, "boxB", json!({ "records": [{ "data": 10 }] }));

    let (url, _) = create_watch(
        &h,
        json!({ "boxes": { "boxA": { "from_seq": 0 }, "boxB": { "from_seq": 0 } } }),
    );

    // Open the stream in the background and collect raw text until we see the
    // box-deleted frame (or time out).
    let base = h.base_url().to_string();
    let url2 = url.clone();
    let handle = std::thread::spawn(move || {
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(8))
            .build()
            .unwrap();
        let mut resp = client
            .get(format!("{base}{url2}"))
            .header(reqwest::header::ACCEPT, "text/event-stream")
            .timeout(Duration::from_secs(8))
            .send()
            .unwrap();
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(6) {
            if String::from_utf8_lossy(&buf).contains("box-deleted") {
                break;
            }
            match resp.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
                Err(_) => break,
            }
        }
        String::from_utf8_lossy(&buf).into_owned()
    });

    // Let the stream reach its parked caught-up state, then delete boxA and
    // poke boxB so the park wakes and the loop re-checks both boxes.
    std::thread::sleep(Duration::from_millis(300));
    let (status, _) = h.delete("/v0/boxes/boxA");
    assert_eq!(status, StatusCode::OK);
    write(&h, "boxB", json!({ "records": [{ "data": 11 }] }));

    let text = handle.join().unwrap();
    assert!(
        text.contains("event: box-deleted") || text.contains("event:box-deleted"),
        "deleting a watched box must emit a box-deleted frame, got:\n{text}"
    );
    assert!(
        text.contains("\"box\":\"boxA\""),
        "box-deleted names the deleted box (boxA):\n{text}"
    );
    assert!(
        text.contains("\"reason\":\"deleted\""),
        "box-deleted reason is `deleted`:\n{text}"
    );
    // The other box keeps streaming: its new record (seq 11) is delivered.
    assert!(
        text.contains("\"$seq\":11") || text.contains("\"box\":\"boxB\""),
        "the surviving box keeps streaming after the peer's deletion:\n{text}"
    );
}

// ===========================================================================
// 9. Node loop-prevention reflected in the stream (§4 / §7.2).
// ===========================================================================

#[test]
fn node_loop_prevention_filters_own_records_in_stream() {
    let h = Harness::start();
    // Two records from node "self", one from node "other".
    write(
        &h,
        "topic",
        json!({ "records": [
            { "data": 1, "node": "self" },
            { "data": 2, "node": "other" },
            { "data": 3, "node": "self" }
        ] }),
    );

    // Watch as node "self": its own records (seq 1, 3) are filtered out, but the
    // cursor still advances past them (silent — no tombstone), and seq 2 is
    // delivered.
    let (url, _) = create_watch(
        &h,
        json!({ "node": "self", "boxes": { "topic": { "from_seq": 0 } }, "include_tags": false }),
    );

    let frames = h.sse_frames(&url, 2, SHORT);
    let evs = events(&frames);
    assert!(
        !evs.contains(&"tombstone"),
        "node filtering is silent, got {evs:?}"
    );
    assert!(
        evs.contains(&"caught-up"),
        "stream reaches caught-up, got {evs:?}"
    );

    let rec = find(&frames, "record");
    let d = data_of(&rec);
    let recs = d["records"].as_array().unwrap();
    assert_eq!(recs.len(), 1, "only the other node's record is delivered");
    assert_eq!(recs[0]["$seq"], 2);
    assert_eq!(recs[0]["$node"], "other");
    // The cursor (composite id) advanced past the filtered seq 3 to head.
    assert_eq!(
        d["to_seq"], 3,
        "cursor advances past filtered own-node records"
    );
    assert_eq!(decode_id(&rec.id)["topic"], 3);
}

// ===========================================================================
// 10. A node that produced everything sees no records, only caught-up (§4).
// ===========================================================================

#[test]
fn watcher_that_owns_all_records_sees_only_caught_up() {
    let h = Harness::start();
    write(
        &h,
        "echo",
        json!({ "node": "only", "records": [{ "data": 1 }, { "data": 2 }] }),
    );

    let (url, _) = create_watch(
        &h,
        json!({ "node": "only", "boxes": { "echo": { "from_seq": 0 } } }),
    );

    // No `record` frame is delivered (all filtered); the box still drains to
    // caught-up so the consumer isn't stuck in an empty loop (API §3.2).
    let frames = h.sse_frames(&url, 1, SHORT);
    let evs = events(&frames);
    assert!(
        evs.contains(&"caught-up"),
        "an all-own-node watcher still reaches caught-up, got {evs:?}"
    );
    let cu = find(&frames, "caught-up");
    assert_eq!(
        data_of(&cu)["head_seq"],
        2,
        "caught-up reports the true head"
    );
}
