//! Phase-3 §2 — conventions + error model over real HTTP (API §0).
//!
//! Black-topic wire-contract coverage of the cross-cutting conventions every
//! endpoint shares:
//!
//!   * the canonical error envelope `{"error":{code,message,detail?}}` on a
//!     representative `400`/`404`/`405`/`409`/`413`/`415`/`422`,
//!   * success bodies carry **bare data** — never a `{"status":"ok"}`/`ok:true`
//!     envelope; the presence of a top-level `error` key is the only
//!     success/failure discriminator (API §0.5),
//!   * `415` on a missing/wrong `Content-Type`,
//!   * `413` on an over-limit body (pre-parse hard guard),
//!   * the `batch_too_large` / `record_too_large` write limits,
//!   * `404 topic_not_found` / `router_not_found`,
//!   * bearer auth on a *second* server booted **with** `TOPICS_API_KEYS`
//!     (the default elsewhere is the no-keys dev mode): `401` with no token and
//!     with a bad token, success with a good token,
//!   * the `performance` block is present on success responses.
//!
//! Everything here is wire-observable and clock-independent, so the
//! `SystemClock`-backed [`Harness`] is the right vehicle (no `TestClock`
//! needed — TTL/priority correctness lives in the engine unit/property tests).

mod common;

use common::{Harness, StatusCode};
use serde_json::{json, Value};

// ---------------------------------------------------------------------------
// Raw HTTP helpers
//
// The shared harness only exposes JSON helpers (which always set
// `application/json`) plus `post_empty` (no body / no content-type). To drive
// the content-type / raw-body edge cases (a *wrong* content-type, a
// syntactically broken body) we open our own blocking `reqwest` client against
// the harness's public `base_url()` — no harness changes needed.
// ---------------------------------------------------------------------------

/// `POST path` with a raw body and an optional explicit `Content-Type`.
/// `ct = None` sends no content-type header at all. Returns `(status, body)`.
fn post_raw(h: &Harness, path: &str, body: &[u8], ct: Option<&str>) -> (StatusCode, Value) {
    let client = reqwest::blocking::Client::new();
    let mut req = client
        .post(format!("{}{}", h.base_url(), path))
        .body(body.to_vec());
    if let Some(ct) = ct {
        req = req.header(reqwest::header::CONTENT_TYPE, ct);
    }
    let resp = req.send().expect("send raw request");
    let status = resp.status();
    let bytes = resp.bytes().expect("read body");
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, value)
}

/// `POST path` with a raw body and `Content-Type: application/json` (for the
/// malformed-JSON 400 path).
fn post_raw_json(h: &Harness, path: &str, body: &[u8]) -> (StatusCode, Value) {
    post_raw(h, path, body, Some("application/json"))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Assert a body is the canonical error envelope (API §0.5): a top-level
/// `error` object with a snake_case `code` string and a non-empty `message`
/// string. `detail` is optional. Returns the `error.code` for further checks.
fn assert_error_envelope(body: &Value) -> String {
    let err = body
        .get("error")
        .unwrap_or_else(|| panic!("error body must have a top-level `error` key, got {body}"));
    assert!(err.is_object(), "`error` must be an object, got {err}");
    let code = err["code"]
        .as_str()
        .unwrap_or_else(|| panic!("error.code must be a string, got {err}"));
    assert!(
        !code.is_empty() && code == code.to_lowercase(),
        "error.code must be non-empty snake_case, got {code:?}"
    );
    let msg = err["message"]
        .as_str()
        .unwrap_or_else(|| panic!("error.message must be a string, got {err}"));
    assert!(!msg.is_empty(), "error.message must be non-empty");
    // The only allowed keys are code/message/detail (no leakage / stray fields).
    for k in err.as_object().unwrap().keys() {
        assert!(
            matches!(k.as_str(), "code" | "message" | "detail"),
            "unexpected key {k:?} in error body {err}"
        );
    }
    // A success-style discriminator must NOT appear on an error.
    assert!(
        body.get("ok").is_none() && body.get("status").is_none(),
        "error body must not carry a success envelope: {body}"
    );
    code.to_string()
}

/// Assert a success body carries **bare data** (no `ok`/`status:"ok"` envelope)
/// and includes the `performance` block (API §0.5/§0.9).
fn assert_bare_success_with_perf(body: &Value) {
    assert!(
        body.get("error").is_none(),
        "success body must not carry an `error` key: {body}"
    );
    assert!(
        body.get("ok").is_none(),
        "success body must not wrap data in an `ok` envelope: {body}"
    );
    // `status` only ever appears on health/ready — never on data endpoints.
    assert!(
        body.get("status").is_none(),
        "success data body must not carry a `status` envelope: {body}"
    );
    assert!(
        body["performance"]["server_total_ms"].is_number(),
        "every data response must carry a performance block: {body}"
    );
}

// ---------------------------------------------------------------------------
// 400 invalid_request (+ bare-data success contrast)
// ---------------------------------------------------------------------------

#[test]
fn malformed_json_body_is_400_invalid_request() {
    let h = Harness::start();
    // Valid content-type but a syntactically broken body parses to 400.
    let (status, body) = post_raw_json(&h, "/v0/topics/jobs", b"{ not valid json ");
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(assert_error_envelope(&body), "invalid_request");
}

#[test]
fn empty_write_body_is_400_invalid_request() {
    let h = Harness::start();
    // `{}` is well-formed JSON but a write needs >=1 record.
    let (status, body) = h.post("/v0/topics/jobs", json!({}));
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(assert_error_envelope(&body), "invalid_request");

    // Empty `records` array — same code.
    let (status, body) = h.post("/v0/topics/jobs", json!({ "records": [] }));
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(assert_error_envelope(&body), "invalid_request");
}

#[test]
fn delete_without_selector_is_400_invalid_request() {
    let h = Harness::start();
    // Topic must exist first (delete on an absent topic is 404 — covered elsewhere).
    let (status, _) = h.post("/v0/topics/jobs", json!({ "records": [{ "data": 1 }] }));
    assert_eq!(status, StatusCode::CREATED);
    // Neither before_seq nor match -> 400.
    let (status, body) = h.post("/v0/topics/jobs/delete", json!({}));
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(assert_error_envelope(&body), "invalid_request");
}

#[test]
fn glob_without_trailing_star_is_400() {
    let h = Harness::start();
    let (status, _) = h.post("/v0/topics/jobs", json!({ "records": [{ "data": 1 }] }));
    assert_eq!(status, StatusCode::CREATED);
    // A Glob predicate must end with a single trailing `*` (API §5); reject otherwise.
    let (status, body) = h.post(
        "/v0/topics/jobs/delete",
        json!({ "match": ["tag", "Glob", "no-star"] }),
    );
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(assert_error_envelope(&body), "invalid_request");

    // A malformed predicate op is likewise a 400.
    let (status, body) = h.post(
        "/v0/topics/jobs/delete",
        json!({ "match": ["tag", "Regex", ".*"] }),
    );
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(assert_error_envelope(&body), "invalid_request");
}

#[test]
fn invalid_topic_name_on_create_is_400() {
    let h = Harness::start();
    // A name that does not start alphanumeric is invalid (API §1.1 charset).
    let (status, body) = h.put("/v0/topics/-bad", json!({}));
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(assert_error_envelope(&body), "invalid_request");
}

#[test]
fn router_missing_source_dest_is_400() {
    let h = Harness::start();
    // `source`/`dest` are required fields; omitting them is a parse-level 400.
    let (status, body) = h.put("/v0/routers/r1", json!({ "dest": "audit" }));
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(assert_error_envelope(&body), "invalid_request");
}

#[test]
fn router_source_equals_dest_is_400() {
    let h = Harness::start();
    let (status, body) = h.put("/v0/routers/r1", json!({ "source": "a", "dest": "a" }));
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(assert_error_envelope(&body), "invalid_request");
}

// ---------------------------------------------------------------------------
// 404 topic_not_found / router_not_found
// ---------------------------------------------------------------------------

#[test]
fn topic_state_on_absent_topic_is_404_topic_not_found() {
    let h = Harness::start();
    // State read never auto-creates (API §1.2).
    let (status, body) = h.get("/v0/topics/ghost");
    assert_eq!(status, StatusCode::NOT_FOUND);
    let code = assert_error_envelope(&body);
    assert_eq!(code, "topic_not_found");
    // The detail carries the offending topic name (API §0.5 example).
    assert_eq!(body["error"]["detail"]["topic"], "ghost");
}

#[test]
fn diff_on_absent_topic_is_404_topic_not_found() {
    let h = Harness::start();
    // Diff never auto-creates (API §3).
    let (status, body) = h.post("/v0/topics/ghost/diff", json!({ "from_seq": 0 }));
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(assert_error_envelope(&body), "topic_not_found");
}

#[test]
fn delete_records_on_absent_topic_is_404_topic_not_found() {
    let h = Harness::start();
    let (status, body) = h.post("/v0/topics/ghost/delete", json!({ "before_seq": 5 }));
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(assert_error_envelope(&body), "topic_not_found");
}

#[test]
fn write_with_create_false_on_absent_topic_is_404() {
    let h = Harness::start();
    // `create:false` refuses lazy create (Redis NOMKSTREAM, API §2).
    let (status, body) = h.post(
        "/v0/topics/ghost",
        json!({ "create": false, "records": [{ "data": 1 }] }),
    );
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(assert_error_envelope(&body), "topic_not_found");
}

#[test]
fn get_absent_router_is_404_router_not_found() {
    let h = Harness::start();
    let (status, body) = h.get("/v0/routers/nope");
    assert_eq!(status, StatusCode::NOT_FOUND);
    let code = assert_error_envelope(&body);
    assert_eq!(code, "router_not_found");
    assert_eq!(body["error"]["detail"]["router"], "nope");
}

#[test]
fn router_create_dest_false_missing_dest_is_404() {
    let h = Harness::start();
    // create_dest:false + an absent dest topic -> 404 topic_not_found (API §6.1).
    let (status, body) = h.put(
        "/v0/routers/r1",
        json!({ "source": "src", "dest": "missingdst", "create_dest": false }),
    );
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(assert_error_envelope(&body), "topic_not_found");
}

// ---------------------------------------------------------------------------
// 405 method_not_allowed
// ---------------------------------------------------------------------------

#[test]
fn wrong_method_is_405_method_not_allowed() {
    let h = Harness::start();
    // `/v0/topics/:topic/diff` is POST-only; a GET is the wrong method.
    let (status, body) = h.get("/v0/topics/jobs/diff");
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(assert_error_envelope(&body), "method_not_allowed");
}

#[test]
fn delete_on_diff_path_is_405() {
    let h = Harness::start();
    // `/v0/topics/:topic/delete` is POST-only; DELETE is the wrong method.
    let (status, body) = h.delete("/v0/topics/jobs/delete");
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(assert_error_envelope(&body), "method_not_allowed");
}

// ---------------------------------------------------------------------------
// 409 conflict (topic_not_empty, router_cycle)
// ---------------------------------------------------------------------------

#[test]
fn delete_non_empty_topic_if_empty_is_409_topic_not_empty() {
    let h = Harness::start();
    let (status, _) = h.post("/v0/topics/jobs", json!({ "records": [{ "data": 1 }] }));
    assert_eq!(status, StatusCode::CREATED);
    // `?if_empty=true` against a non-empty topic -> 409 topic_not_empty (API §1.4).
    let (status, body) = h.delete("/v0/topics/jobs?if_empty=true");
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(assert_error_envelope(&body), "topic_not_empty");
}

#[test]
fn router_cycle_is_409_router_cycle() {
    let h = Harness::start();
    // a -> b, then b -> a would close a directed cycle -> 409 (API §6.1).
    let (status, _) = h.put("/v0/routers/a-to-b", json!({ "source": "a", "dest": "b" }));
    assert_eq!(status, StatusCode::CREATED);
    let (status, body) = h.put("/v0/routers/b-to-a", json!({ "source": "b", "dest": "a" }));
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(assert_error_envelope(&body), "router_cycle");
}

// ---------------------------------------------------------------------------
// 413 payload_too_large (pre-parse body guard)
// ---------------------------------------------------------------------------

#[test]
fn oversized_body_is_413_payload_too_large() {
    use topics::config::ServerConfig;
    // Boot a server with a tiny hard body cap so we don't have to ship MiBs.
    let cfg = ServerConfig {
        max_body_bytes: 1024, // 1 KiB hard limit, pre-parse.
        ..Default::default()
    };
    let h = Harness::start_with(cfg);

    // A ~4 KiB JSON body exceeds the 1 KiB cap; rejected before parse.
    let big = "x".repeat(4096);
    let body = json!({ "records": [{ "data": big }] });
    let (status, env) = h.post("/v0/topics/jobs", body);
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(assert_error_envelope(&env), "payload_too_large");
}

// ---------------------------------------------------------------------------
// 415 unsupported_media_type (missing / wrong Content-Type)
// ---------------------------------------------------------------------------

#[test]
fn missing_content_type_is_415() {
    let h = Harness::start();
    // A POST with a body but no Content-Type header -> 415 (API §0.3).
    let (status, body) = post_raw(&h, "/v0/topics/jobs", b"{\"records\":[{\"data\":1}]}", None);
    assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
    assert_eq!(assert_error_envelope(&body), "unsupported_media_type");
}

#[test]
fn wrong_content_type_is_415() {
    let h = Harness::start();
    // A non-JSON content type on a body endpoint -> 415.
    let (status, body) = post_raw(&h, "/v0/topics/jobs", b"<xml/>", Some("application/xml"));
    assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
    assert_eq!(assert_error_envelope(&body), "unsupported_media_type");
}

#[test]
fn diff_wrong_content_type_is_415() {
    let h = Harness::start();
    // The 415 guard is shared by every body endpoint; verify on diff too.
    let (status, body) = post_raw(
        &h,
        "/v0/topics/jobs/diff",
        b"{\"from_seq\":0}",
        Some("text/plain"),
    );
    assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
    assert_eq!(assert_error_envelope(&body), "unsupported_media_type");
}

// ---------------------------------------------------------------------------
// 400 batch_too_large / record_too_large
// ---------------------------------------------------------------------------

#[test]
fn batch_over_limit_is_400_batch_too_large() {
    let h = Harness::start();
    // MAX_BATCH_RECORDS is 10_000; one more trips batch_too_large (API §2).
    let n = topics::config::MAX_BATCH_RECORDS + 1;
    let records: Vec<Value> = (0..n).map(|_| json!({ "data": 0 })).collect();
    let (status, body) = h.post("/v0/topics/jobs", json!({ "records": records }));
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(assert_error_envelope(&body), "batch_too_large");
}

#[test]
fn record_over_byte_limit_is_400_record_too_large() {
    let h = Harness::start();
    // A single record whose data exceeds MAX_RECORD_BYTES (1 MiB) is rejected
    // with record_too_large (distinct from a retryable 422 topic_full).
    let big = "y".repeat(topics::config::MAX_RECORD_BYTES + 1024);
    let (status, body) = h.post("/v0/topics/jobs", json!({ "records": [{ "data": big }] }));
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(assert_error_envelope(&body), "record_too_large");
}

// ---------------------------------------------------------------------------
// 422 topic_full (discard:"reject")
// ---------------------------------------------------------------------------

#[test]
fn full_reject_topic_is_422_topic_full() {
    let h = Harness::start();
    // A capacity-1 reject topic: the second record overflows -> 422 topic_full,
    // nothing appended (all-or-nothing). (API §2 / §0.10 discard:"reject".)
    let (status, _) = h.put(
        "/v0/topics/q",
        json!({ "cap_records": 1, "discard": "reject" }),
    );
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = h.post("/v0/topics/q", json!({ "records": [{ "data": 1 }] }));
    assert_eq!(status, StatusCode::OK);
    let (status, body) = h.post("/v0/topics/q", json!({ "records": [{ "data": 2 }] }));
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(assert_error_envelope(&body), "topic_full");

    // The rejected write appended nothing: head_seq unchanged at 1.
    let (status, st) = h.get("/v0/topics/q");
    assert_eq!(status, StatusCode::OK);
    assert_eq!(st["head_seq"], 1, "rejected write must not append");
    assert_eq!(st["count"], 1);
}

// ---------------------------------------------------------------------------
// Success bodies carry bare data + the performance block (no `ok` envelope)
// ---------------------------------------------------------------------------

#[test]
fn success_bodies_are_bare_data_with_performance() {
    let h = Harness::start();

    // PUT create (201).
    let (status, body) = h.put("/v0/topics/jobs", json!({ "durable": true }));
    assert_eq!(status, StatusCode::CREATED);
    assert_bare_success_with_perf(&body);
    assert_eq!(body["created"], true);

    // POST write (200, topic already exists).
    let (status, body) = h.post(
        "/v0/topics/jobs",
        json!({ "records": [{ "data": { "v": 1 } }, { "data": { "v": 2 } }] }),
    );
    assert_eq!(status, StatusCode::OK);
    assert_bare_success_with_perf(&body);
    assert_eq!(body["seqs"], json!([1, 2]));

    // GET state.
    let (status, body) = h.get("/v0/topics/jobs");
    assert_eq!(status, StatusCode::OK);
    assert_bare_success_with_perf(&body);

    // POST diff.
    let (status, body) = h.post("/v0/topics/jobs/diff", json!({ "from_seq": 0 }));
    assert_eq!(status, StatusCode::OK);
    assert_bare_success_with_perf(&body);
    assert_eq!(body["records"].as_array().unwrap().len(), 2);

    // GET list topics.
    let (status, body) = h.get("/v0/topics");
    assert_eq!(status, StatusCode::OK);
    assert_bare_success_with_perf(&body);

    // POST delete records.
    let (status, body) = h.post("/v0/topics/jobs/delete", json!({ "before_seq": 2 }));
    assert_eq!(status, StatusCode::OK);
    assert_bare_success_with_perf(&body);

    // DELETE topic.
    let (status, body) = h.delete("/v0/topics/jobs");
    assert_eq!(status, StatusCode::OK);
    assert_bare_success_with_perf(&body);
    assert_eq!(body["deleted"], true);
}

// ---------------------------------------------------------------------------
// Bearer auth: a SECOND server configured WITH TOPICS_API_KEYS.
// ---------------------------------------------------------------------------

/// Boot an auth-enabled server with a single known key.
fn auth_harness(key: &str) -> Harness {
    use topics::config::ServerConfig;
    let cfg = ServerConfig {
        api_keys: vec![key.to_string()],
        ..Default::default()
    };
    Harness::start_with(cfg)
}

#[test]
fn auth_required_no_token_is_401() {
    let h = auth_harness("s3cr3t");
    // No Authorization header on a data endpoint -> 401 unauthorized.
    let (status, body) = h.post("/v0/topics/jobs", json!({ "records": [{ "data": 1 }] }));
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(assert_error_envelope(&body), "unauthorized");

    // GET state likewise requires a key.
    let (status, body) = h.get("/v0/topics/jobs");
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(assert_error_envelope(&body), "unauthorized");
}

#[test]
fn auth_bad_token_is_401() {
    let h = auth_harness("s3cr3t");
    let (status, body) = h.post_auth(
        "/v0/topics/jobs",
        json!({ "records": [{ "data": 1 }] }),
        "wrong-key",
    );
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(assert_error_envelope(&body), "unauthorized");
}

#[test]
fn auth_good_token_succeeds() {
    let h = auth_harness("s3cr3t");
    // A valid bearer key grants full access: first write creates -> 201.
    let (status, body) = h.post_auth(
        "/v0/topics/jobs",
        json!({ "records": [{ "data": 1 }] }),
        "s3cr3t",
    );
    assert_eq!(status, StatusCode::CREATED);
    assert_bare_success_with_perf(&body);
    assert_eq!(body["created"], true);

    // A subsequent authed read works and shows the record landed.
    let (status, st) = h.get_auth("/v0/topics/jobs", "s3cr3t");
    assert_eq!(status, StatusCode::OK);
    assert_eq!(st["head_seq"], 1);
}

#[test]
fn auth_probe_endpoints_skip_auth_by_default() {
    let h = auth_harness("s3cr3t");
    // Health/ready (liveness/readiness) do not require auth unless
    // TOPICS_PROBE_AUTH is set (API §8): reachable with no token even on an
    // auth-enabled server. `/v0/metrics` is gated behind auth by default.
    let (status, body) = h.get("/v0/health");
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "ok");

    let (status, _) = h.get("/v0/ready");
    assert_eq!(status, StatusCode::OK);
}
