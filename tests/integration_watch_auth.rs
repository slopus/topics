//! Phase-7 security: the watch `wid` is an unguessable bearer capability, and the
//! SSE stream GET is authorized by *possessing* the wid (no api key in the URL),
//! bound — when auth is enabled — to the principal that created the session.
//!
//! These are black-topic tests against the real bound server (the exact
//! `http::build_router` the binary serves), exercising:
//!   * the `wid` shape is the random-capability form (`wid_` + base64url), not the
//!     old guessable monotonic counter;
//!   * `POST /v0/watch` still requires a valid bearer when auth is on;
//!   * `GET /v0/watch/:wid` when auth is on REQUIRES the creating key (header or
//!     the dev-only `?token=`); the wid alone is NOT sufficient — a leaked wid
//!     cannot be opened by a holder who lacks the key (codex HIGH #3);
//!   * `GET` with a WRONG bearer (header or `?token=`) is `401` (binding enforced);
//!   * `GET` with the CORRECT bearer (header or `?token=`) opens;
//!   * an unknown wid is `404` regardless of credentials.

mod common;

use std::io::Read;
use std::time::Duration;

use common::{Harness, StatusCode};
use serde_json::json;
use topics::config::ServerConfig;

/// Open `path` with optional `Authorization: Bearer <bearer>` and return just the
/// HTTP status (draining nothing for a non-200, reading a little for a 200 so the
/// server-side accept/auth path runs). Never hangs: a hard client timeout bounds it.
fn get_stream_status(h: &Harness, path: &str, bearer: Option<&str>) -> StatusCode {
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .expect("build client");
    let url = format!("{}{}", h.base_url(), path);
    let mut req = client
        .get(&url)
        .header(reqwest::header::ACCEPT, "text/event-stream")
        .timeout(Duration::from_secs(2));
    if let Some(b) = bearer {
        req = req.bearer_auth(b);
    }
    let mut resp = req.send().expect("open stream");
    let status = resp.status();
    // Read a bounded prefix so the open path fully executes, then drop.
    let mut chunk = [0u8; 1024];
    let _ = resp.read(&mut chunk);
    status
}

fn auth_harness(key: &str) -> Harness {
    let cfg = ServerConfig {
        api_keys: vec![key.to_string()],
        ..Default::default()
    };
    Harness::start_with(cfg)
}

#[test]
fn wid_is_unguessable_random_capability() {
    let h = Harness::start(); // dev mode (no auth)
    h.post("/v0/topics/jobs", json!({ "records": [{ "data": 1 }] }));

    let (status, body) = h.post(
        "/v0/watch",
        json!({ "topics": { "jobs": { "from_seq": 0 } } }),
    );
    assert_eq!(status, StatusCode::OK);
    let wid = body["wid"].as_str().expect("wid");

    // Keeps the documented prefix, but the suffix is 16 random bytes (128 bits)
    // base64url-encoded ⇒ 22 chars; total length 26. NOT a `wid_{n:010x}` counter.
    assert!(wid.starts_with("wid_"), "wid prefixed: {wid}");
    assert_eq!(wid.len(), 26, "wid carries >=128 bits of entropy: {wid}");
    let suffix = wid.strip_prefix("wid_").unwrap();
    assert!(
        suffix
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_'),
        "suffix is base64url: {suffix}"
    );

    // Two sessions get distinct, non-sequential wids.
    let (_, body2) = h.post(
        "/v0/watch",
        json!({ "topics": { "jobs": { "from_seq": 0 } } }),
    );
    let wid2 = body2["wid"].as_str().unwrap();
    assert_ne!(wid, wid2, "wids are random/unique, not monotonic");
}

#[test]
fn stream_requires_creating_key_not_just_wid_when_auth_on() {
    let h = auth_harness("s3cr3t");
    // Seed + create the session WITH a valid key (POST is authenticated).
    h.post_auth(
        "/v0/topics/jobs",
        json!({ "records": [{ "data": 1 }] }),
        "s3cr3t",
    );
    let (status, body) = h.post_auth(
        "/v0/watch",
        json!({ "topics": { "jobs": { "from_seq": 0 } } }),
        "s3cr3t",
    );
    assert_eq!(status, StatusCode::OK);
    let stream_url = body["stream_url"].as_str().unwrap();

    // When auth is enabled the wid alone is NOT sufficient: the GET must present
    // the creating key (header or the dev-only `?token=`). A leaked wid opened
    // with no credential is rejected (codex HIGH #3).
    assert_eq!(
        get_stream_status(&h, stream_url, None),
        StatusCode::UNAUTHORIZED,
        "wid alone (no bearer) must NOT open the stream when auth is on"
    );
    // With the creating key it opens.
    assert_eq!(
        get_stream_status(&h, stream_url, Some("s3cr3t")),
        StatusCode::OK,
        "the creating key opens the stream"
    );
}

#[test]
fn post_watch_still_requires_auth() {
    let h = auth_harness("s3cr3t");
    // Anonymous POST /v0/watch is rejected even though the GET is capability-gated.
    let (status, body) = h.post(
        "/v0/watch",
        json!({ "topics": { "jobs": { "from_seq": 0 } } }),
    );
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["error"]["code"], "unauthorized");
}

#[test]
fn stream_rejects_wrong_bearer_but_accepts_right_one() {
    let h = auth_harness("s3cr3t");
    h.post_auth(
        "/v0/topics/jobs",
        json!({ "records": [{ "data": 1 }] }),
        "s3cr3t",
    );
    let (_, body) = h.post_auth(
        "/v0/watch",
        json!({ "topics": { "jobs": { "from_seq": 0 } } }),
        "s3cr3t",
    );
    let stream_url = body["stream_url"].as_str().unwrap().to_string();

    // A wrong bearer presented on the GET is rejected (binding enforced) — a
    // stolen-but-mismatched key cannot ride a known wid.
    assert_eq!(
        get_stream_status(&h, &stream_url, Some("wrong-key")),
        StatusCode::UNAUTHORIZED,
        "mismatched bearer must be 401"
    );
    // The correct bearer is accepted.
    assert_eq!(
        get_stream_status(&h, &stream_url, Some("s3cr3t")),
        StatusCode::OK,
        "matching bearer must open"
    );
}

#[test]
fn stream_token_query_param_fallback_is_validated() {
    let h = auth_harness("s3cr3t");
    h.post_auth(
        "/v0/topics/jobs",
        json!({ "records": [{ "data": 1 }] }),
        "s3cr3t",
    );
    let (_, body) = h.post_auth(
        "/v0/watch",
        json!({ "topics": { "jobs": { "from_seq": 0 } } }),
        "s3cr3t",
    );
    let stream_url = body["stream_url"].as_str().unwrap().to_string();

    // The dev-only `?token=` fallback is validated against the binding: a wrong
    // token is 401, the right token opens.
    assert_eq!(
        get_stream_status(&h, &format!("{stream_url}?token=wrong"), None),
        StatusCode::UNAUTHORIZED,
        "wrong ?token= must be 401"
    );
    assert_eq!(
        get_stream_status(&h, &format!("{stream_url}?token=s3cr3t"), None),
        StatusCode::OK,
        "correct ?token= must open"
    );
}

#[test]
fn unknown_wid_is_404() {
    let h = auth_harness("s3cr3t");
    // Unknown wid is 404 whether or not a (valid) bearer is presented — the
    // capability simply does not exist.
    assert_eq!(
        get_stream_status(&h, "/v0/watch/wid_doesnotexist", Some("s3cr3t")),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        get_stream_status(&h, "/v0/watch/wid_doesnotexist", None),
        StatusCode::NOT_FOUND
    );
}
