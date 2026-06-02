//! Stage-2 security: resource / rate limits (DoS hardening). Every creation path
//! refuses past its configured cap with a clear `429 throttled` (+ `Retry-After`),
//! while an idempotent re-PUT of an existing resource still succeeds, and a `0`
//! cap means unlimited. Concurrency caps (SSE connections, per-key in-flight) free
//! their slot when the request/stream ends.
//!
//! Black-topic against the real bound server.

mod common;

use std::io::Read;
use std::time::Duration;

use common::{Harness, StatusCode};
use serde_json::{json, Value};
use topics::config::ServerConfig;
use topics::limits::Limits;

/// Boot a server with the given limits (auth disabled).
fn limited_harness(limits: Limits) -> Harness {
    Harness::start_with(ServerConfig {
        limits,
        ..Default::default()
    })
}

/// Boot an auth-enabled server with one full-access key + the given limits.
fn limited_auth_harness(limits: Limits, keys: &[&str]) -> Harness {
    Harness::start_with(ServerConfig {
        api_keys: keys.iter().map(|s| s.to_string()).collect(),
        limits,
        ..Default::default()
    })
}

fn code(body: &Value) -> &str {
    body["error"]["code"].as_str().unwrap_or("")
}

#[test]
fn max_topics_cap_via_put() {
    let h = limited_harness(Limits {
        max_topics: 2,
        ..Default::default()
    });
    // First two topics create (201).
    let (s, _) = h.put("/v0/topics/a", json!({}));
    assert_eq!(s, StatusCode::CREATED);
    let (s, _) = h.put("/v0/topics/b", json!({}));
    assert_eq!(s, StatusCode::CREATED);
    // Third NEW topic is refused with 429 throttled + Retry-After detail.
    let (s, body) = h.put("/v0/topics/c", json!({}));
    assert_eq!(
        s,
        StatusCode::TOO_MANY_REQUESTS,
        "3rd topic over cap: {body}"
    );
    assert_eq!(code(&body), "throttled");
    assert_eq!(body["error"]["detail"]["limit"], json!("max_topics"));
    assert_eq!(body["error"]["detail"]["max"], json!(2));

    // An idempotent re-PUT of an EXISTING topic is an update, not a create — it must
    // still succeed even at the cap (a saturated server can be reconfigured).
    let (s, _) = h.put("/v0/topics/a", json!({ "ttl_ms": 1000 }));
    assert_eq!(
        s,
        StatusCode::OK,
        "re-PUT of existing topic at cap must update"
    );
}

#[test]
fn max_topics_cap_via_auto_create_write() {
    let h = limited_harness(Limits {
        max_topics: 1,
        ..Default::default()
    });
    // First write auto-creates the topic (201).
    let (s, _) = h.post("/v0/topics/a", json!({ "records": [{ "data": 1 }] }));
    assert_eq!(s, StatusCode::CREATED);
    // A write that WOULD auto-create a second topic hits the cap → 429.
    let (s, body) = h.post("/v0/topics/b", json!({ "records": [{ "data": 1 }] }));
    assert_eq!(
        s,
        StatusCode::TOO_MANY_REQUESTS,
        "auto-create over cap: {body}"
    );
    assert_eq!(code(&body), "throttled");
    // But appending to the EXISTING topic still works (not a create).
    let (s, _) = h.post("/v0/topics/a", json!({ "records": [{ "data": 2 }] }));
    assert_eq!(s, StatusCode::OK);
}

#[test]
fn max_topics_zero_is_unlimited() {
    let h = limited_harness(Limits {
        max_topics: 0,
        ..Default::default()
    });
    for i in 0..50 {
        let (s, _) = h.put(&format!("/v0/topics/b{i}"), json!({}));
        assert_eq!(s, StatusCode::CREATED);
    }
}

#[test]
fn max_routers_cap() {
    let h = limited_harness(Limits {
        max_routers: 1,
        // Generous topic cap so router source/dest auto-create is unaffected.
        max_topics: 1000,
        ..Default::default()
    });
    let (s, _) = h.put("/v0/routers/r1", json!({ "source": "src", "dest": "dst1" }));
    assert_eq!(s, StatusCode::CREATED);
    // Second NEW router refused.
    let (s, body) = h.put("/v0/routers/r2", json!({ "source": "src", "dest": "dst2" }));
    assert_eq!(
        s,
        StatusCode::TOO_MANY_REQUESTS,
        "2nd router over cap: {body}"
    );
    assert_eq!(code(&body), "throttled");
    assert_eq!(body["error"]["detail"]["limit"], json!("max_routers"));

    // A refused router must not have auto-created its dest topic (checked before any
    // topic auto-create).
    let (s, _) = h.get("/v0/topics/dst2");
    assert_eq!(
        s,
        StatusCode::NOT_FOUND,
        "refused router left no phantom dest topic"
    );

    // Re-PUT of the existing router still updates at the cap.
    let (s, _) = h.put(
        "/v0/routers/r1",
        json!({ "source": "src", "dest": "dst1", "preserve_tag": false }),
    );
    assert_eq!(s, StatusCode::OK);
}

#[test]
fn max_watch_sessions_cap() {
    let h = limited_harness(Limits {
        max_watch_sessions: 2,
        ..Default::default()
    });
    h.put("/v0/topics/events", json!({}));
    let body = json!({ "topics": { "events": {} } });

    let (s, _) = h.post("/v0/watch", body.clone());
    assert_eq!(s, StatusCode::OK);
    let (s, _) = h.post("/v0/watch", body.clone());
    assert_eq!(s, StatusCode::OK);
    // Third session refused (no GET stream opened yet, so the registry holds 2).
    let (s, resp) = h.post("/v0/watch", body.clone());
    assert_eq!(
        s,
        StatusCode::TOO_MANY_REQUESTS,
        "3rd watch session over cap: {resp}"
    );
    assert_eq!(code(&resp), "throttled");
    assert_eq!(
        resp["error"]["detail"]["limit"],
        json!("max_watch_sessions")
    );
}

#[test]
fn max_sse_connections_global_cap_and_release() {
    // Cap concurrent SSE connections at 1 (global). The first open holds the slot;
    // the second is refused; after the first closes a new one is admitted.
    let h = limited_harness(Limits {
        max_sse_connections: 1,
        ..Default::default()
    });
    h.put("/v0/topics/events", json!({}));
    let (_s, w) = h.post("/v0/watch", json!({ "topics": { "events": {} } }));
    let wid = w["wid"].as_str().expect("wid").to_string();
    let stream_url = format!("{}/v0/watch/{}", h.base_url(), wid);

    // A second watch session for the parallel test.
    let (_s, w2) = h.post("/v0/watch", json!({ "topics": { "events": {} } }));
    let wid2 = w2["wid"].as_str().expect("wid2").to_string();
    let stream_url2 = format!("{}/v0/watch/{}", h.base_url(), wid2);

    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    // Open and HOLD the first stream (do not read to EOF).
    let mut held = client
        .get(&stream_url)
        .header(reqwest::header::ACCEPT, "text/event-stream")
        .send()
        .expect("open first stream");
    assert_eq!(held.status(), StatusCode::OK);
    // Read one chunk so the stream is fully established (the `retry:` frame).
    let mut chunk = [0u8; 256];
    let _ = held.read(&mut chunk);

    // The second concurrent stream must be refused with 429.
    let resp2 = client
        .get(&stream_url2)
        .header(reqwest::header::ACCEPT, "text/event-stream")
        .send()
        .expect("attempt second stream");
    assert_eq!(
        resp2.status(),
        StatusCode::TOO_MANY_REQUESTS,
        "2nd concurrent SSE conn over the global cap must be 429"
    );

    // Close the first stream; its connection slot frees.
    drop(held);
    // Give the server a moment to run the stream's drop (SseGuard release).
    std::thread::sleep(Duration::from_millis(200));

    // A fresh connection is now admitted.
    let resp3 = client
        .get(&stream_url2)
        .header(reqwest::header::ACCEPT, "text/event-stream")
        .send()
        .expect("third stream after free");
    assert_eq!(
        resp3.status(),
        StatusCode::OK,
        "after the held stream closes, a slot frees and a new SSE conn is admitted"
    );
    drop(resp3);
}

#[test]
fn max_inflight_per_key_does_not_break_normal_traffic() {
    // A generous per-key in-flight cap with auth enabled: ordinary serial requests
    // each acquire+release a slot, so normal traffic is unaffected. (The cap is a
    // concurrency guard; serial requests never accumulate.)
    let h = limited_auth_harness(
        Limits {
            max_inflight_per_key: 4,
            ..Default::default()
        },
        &["k"],
    );
    for i in 0..20 {
        let (s, _) = h.put_auth(&format!("/v0/topics/b{i}"), json!({}), "k");
        assert_eq!(
            s,
            StatusCode::CREATED,
            "serial request {i} should not be throttled"
        );
    }
}

#[test]
fn max_total_bytes_quota_refuses_oversize_write() {
    // codex HIGH #5: the global byte quota refuses a write that would push the
    // live total over the cap with 429 throttled. A small cap fills after a write.
    let h = limited_harness(Limits {
        max_total_bytes: 200,
        ..Default::default()
    });
    // A modest write fits under the 200-byte quota.
    let (s, _) = h.post("/v0/topics/a", json!({ "records": [{ "data": "x" }] }));
    assert_eq!(s, StatusCode::CREATED, "first small write fits");
    // A large write past the quota is refused.
    let big = "y".repeat(500);
    let (s, body) = h.post("/v0/topics/a", json!({ "records": [{ "data": big }] }));
    assert_eq!(s, StatusCode::TOO_MANY_REQUESTS, "over-quota write: {body}");
    assert_eq!(code(&body), "throttled");
    assert_eq!(body["error"]["detail"]["limit"], json!("max_total_bytes"));
}

#[test]
fn max_total_bytes_zero_is_unlimited() {
    // The default (0) imposes no global byte quota.
    let h = limited_harness(Limits {
        max_total_bytes: 0,
        ..Default::default()
    });
    let big = "z".repeat(100_000);
    let (s, _) = h.post("/v0/topics/a", json!({ "records": [{ "data": big }] }));
    assert_eq!(s, StatusCode::CREATED, "0 ⇒ unlimited, large write allowed");
}

#[test]
fn queue_ack_rejects_unbounded_seqs() {
    // codex MEDIUM #10: ack/nack/extend must reject a seqs array longer than
    // MAX_CLAIM (1000) with a clear 4xx rather than allocating/echoing it.
    let h = Harness::start();
    h.put("/v0/topics/q", json!({ "type": "queue" }));
    let seqs: Vec<u64> = (1..=1001).collect();
    let (s, body) = h.post("/v0/topics/q/ack", json!({ "node": "w1", "seqs": seqs }));
    assert_eq!(s, StatusCode::BAD_REQUEST, "1001 seqs rejected: {body}");
    assert_eq!(code(&body), "batch_too_large");
    // A bounded ack (no matching leases) is a normal 200 with 0 acked.
    let (s, body) = h.post(
        "/v0/topics/q/ack",
        json!({ "node": "w1", "seqs": [1, 2, 3] }),
    );
    assert_eq!(s, StatusCode::OK, "bounded ack ok: {body}");
}

#[test]
fn metrics_requires_auth_when_enabled() {
    // codex LOW #12: /v0/metrics is gated behind auth by default (it exposes the
    // topic count); health/ready stay open.
    let h = limited_auth_harness(Limits::default(), &["k"]);
    // No token ⇒ 401 on metrics.
    let (s, _) = h.get("/v0/metrics");
    assert_eq!(s, StatusCode::UNAUTHORIZED, "metrics requires auth");
    // With the key ⇒ 200.
    let (s, _) = h.get_auth("/v0/metrics", "k");
    assert_eq!(s, StatusCode::OK, "metrics open with a valid key");
    // Health stays open with no token.
    let (s, _) = h.get("/v0/health");
    assert_eq!(s, StatusCode::OK, "health stays unauthenticated");
}

#[test]
fn query_token_rejected_for_non_sse_routes() {
    // codex MEDIUM #8: the ?token= query fallback is accepted only for the SSE
    // stream GETs, never for ordinary data routes (it leaks via logs).
    let h = limited_auth_harness(Limits::default(), &["k"]);
    // A normal GET with ?token= (no header) is unauthorized.
    let (s, _) = h.get("/v0/topics?token=k");
    assert_eq!(
        s,
        StatusCode::UNAUTHORIZED,
        "?token= must not auth a data route"
    );
    // The header still works.
    let (s, _) = h.get_auth("/v0/topics", "k");
    assert_eq!(s, StatusCode::OK);
}

#[test]
fn limits_default_when_unconfigured_unchanged() {
    // The default limits are generous enough that a normal flow never trips them.
    let h = Harness::start();
    for i in 0..30 {
        let (s, _) = h.put(&format!("/v0/topics/b{i}"), json!({}));
        assert_eq!(s, StatusCode::CREATED);
    }
    let (s, _) = h.put("/v0/routers/r", json!({ "source": "b0", "dest": "b1" }));
    assert_eq!(s, StatusCode::CREATED);
    let (s, _) = h.post("/v0/watch", json!({ "topics": { "b0": {} } }));
    assert_eq!(s, StatusCode::OK);
}
