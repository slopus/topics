//! Stage-1 security: optional API-key **scopes** (read/write/delete/admin) + a
//! topic-name **prefix allowlist**, with keys stored **hashed at rest** and
//! compared in constant time. All additive + back-compatible: a bare key with no
//! scopes/prefixes is full access (covered by `integration_errors.rs`).
//!
//! Black-topic against the real bound server. Scope→route mapping under test:
//! read = GET state/list + POST diff + POST /watch; write = POST records + queue
//! ack/nack/extend; r+w = queue claim; delete = DELETE topic + POST .../delete;
//! admin = PUT topic/router. Plus the prefix allowlist gating which topic names a key
//! may touch.

mod common;

use common::{Harness, StatusCode};
use serde_json::{json, Value};
use topics::config::ServerConfig;

/// Boot an auth-enabled server with the given `TOPICS_API_KEYS`-style entries
/// (each entry is `key` | `key:scopes` | `key:scopes:prefixes`).
fn scoped_harness(entries: &[&str]) -> Harness {
    let cfg = ServerConfig {
        api_keys: entries.iter().map(|s| s.to_string()).collect(),
        ..Default::default()
    };
    Harness::start_with(cfg)
}

/// `POST path` with a JSON body + bearer; returns `(status, body)`.
fn post(h: &Harness, path: &str, body: Value, key: &str) -> (StatusCode, Value) {
    h.post_auth(path, body, key)
}

fn code(body: &Value) -> &str {
    body["error"]["code"].as_str().unwrap_or("")
}

#[test]
fn full_access_bare_key_unchanged() {
    // Back-compat: a bare key (no scopes/prefixes) does everything.
    let h = scoped_harness(&["full"]);
    let (s, _) = post(
        &h,
        "/v0/topics/jobs",
        json!({ "records": [{ "data": 1 }] }),
        "full",
    );
    assert_eq!(s, StatusCode::CREATED);
    let (s, _) = h.get_auth("/v0/topics/jobs", "full");
    assert_eq!(s, StatusCode::OK);
    let (s, _) = post(&h, "/v0/topics/jobs/diff", json!({ "from_seq": 0 }), "full");
    assert_eq!(s, StatusCode::OK);
    let (s, _) = h.send_auth("DELETE", "/v0/topics/jobs", "full");
    assert_eq!(s, StatusCode::OK);
}

#[test]
fn read_only_key_can_read_not_write() {
    // Seed with an admin/full key, then probe a read-only key.
    let h = scoped_harness(&["full", "ro:read"]);
    post(
        &h,
        "/v0/topics/jobs",
        json!({ "records": [{ "data": 1 }] }),
        "full",
    );

    // Read paths: GET state, GET list, POST diff — all allowed.
    let (s, _) = h.get_auth("/v0/topics/jobs", "ro");
    assert_eq!(s, StatusCode::OK);
    let (s, _) = h.get_auth("/v0/topics", "ro");
    assert_eq!(s, StatusCode::OK);
    let (s, _) = post(&h, "/v0/topics/jobs/diff", json!({ "from_seq": 0 }), "ro");
    assert_eq!(s, StatusCode::OK);

    // Write path: POST records — forbidden (lacks write).
    let (s, b) = post(
        &h,
        "/v0/topics/jobs",
        json!({ "records": [{ "data": 2 }] }),
        "ro",
    );
    assert_eq!(s, StatusCode::FORBIDDEN);
    assert_eq!(code(&b), "forbidden");

    // Admin path: PUT topic — forbidden (lacks admin).
    let (s, b) = h.put_auth("/v0/topics/other", json!({}), "ro");
    assert_eq!(s, StatusCode::FORBIDDEN);
    assert_eq!(code(&b), "forbidden");

    // Delete path: DELETE topic — forbidden (lacks delete).
    let (s, b) = h.send_auth("DELETE", "/v0/topics/jobs", "ro");
    assert_eq!(s, StatusCode::FORBIDDEN);
    assert_eq!(code(&b), "forbidden");
}

#[test]
fn write_key_can_write_not_admin_or_delete() {
    let h = scoped_harness(&["full", "wo:read+write"]);
    // PUT (admin) is forbidden for a write key...
    let (s, _) = h.put_auth("/v0/topics/jobs", json!({}), "wo");
    assert_eq!(s, StatusCode::FORBIDDEN);
    // ...so create the topic with the full key.
    post(
        &h,
        "/v0/topics/jobs",
        json!({ "records": [{ "data": 1 }] }),
        "full",
    );
    // Write a record with the write key — allowed.
    let (s, _) = post(
        &h,
        "/v0/topics/jobs",
        json!({ "records": [{ "data": 2 }] }),
        "wo",
    );
    assert_eq!(s, StatusCode::OK);
    // Delete records — forbidden (lacks delete).
    let (s, b) = post(
        &h,
        "/v0/topics/jobs/delete",
        json!({ "before_seq": 2 }),
        "wo",
    );
    assert_eq!(s, StatusCode::FORBIDDEN);
    assert_eq!(code(&b), "forbidden");
}

#[test]
fn admin_scope_required_for_put_topic_and_router() {
    let h = scoped_harness(&["adm:admin+read+write"]);
    // PUT topic (admin) — allowed.
    let (s, _) = h.put_auth("/v0/topics/jobs", json!({}), "adm");
    assert!(s == StatusCode::CREATED || s == StatusCode::OK);
    // PUT router (admin) — allowed.
    let (s, _) = h.put_auth(
        "/v0/routers/jobs-%3Eaudit",
        json!({ "source": "jobs", "dest": "audit" }),
        "adm",
    );
    assert!(
        s == StatusCode::CREATED || s == StatusCode::OK,
        "router put status {s}"
    );
}

#[test]
fn delete_scope_gates_topic_and_record_delete() {
    let h = scoped_harness(&["full", "del:read+delete"]);
    post(
        &h,
        "/v0/topics/jobs",
        json!({ "records": [{ "data": 1 }, { "data": 2 }] }),
        "full",
    );
    // POST .../delete — allowed with delete scope.
    let (s, _) = post(
        &h,
        "/v0/topics/jobs/delete",
        json!({ "before_seq": 2 }),
        "del",
    );
    assert_eq!(s, StatusCode::OK);
    // DELETE topic — allowed with delete scope.
    let (s, _) = h.send_auth("DELETE", "/v0/topics/jobs", "del");
    assert_eq!(s, StatusCode::OK);
}

#[test]
fn queue_claim_requires_read_and_write() {
    // A read-only key must not be able to claim (claim leases ⇒ mutates).
    let h = scoped_harness(&["full", "ro:read", "rw:read+write"]);
    // Configure the topic as a queue (PUT) then enqueue one job (POST).
    h.put_auth("/v0/topics/q", json!({ "type": "queue" }), "full");
    post(
        &h,
        "/v0/topics/q",
        json!({ "records": [{ "data": 1 }] }),
        "full",
    );
    let (s, b) = post(
        &h,
        "/v0/topics/q/claim",
        json!({ "node": "w1", "max": 1 }),
        "ro",
    );
    assert_eq!(s, StatusCode::FORBIDDEN, "read-only cannot claim");
    assert_eq!(code(&b), "forbidden");
    // A read+write key can claim.
    let (s, _) = post(
        &h,
        "/v0/topics/q/claim",
        json!({ "node": "w1", "max": 1 }),
        "rw",
    );
    assert_eq!(s, StatusCode::OK);
}

#[test]
fn prefix_allowlist_gates_topic_names() {
    // A full-scope key restricted to the `tenant42:` prefix.
    let h = scoped_harness(&["full", "t42::tenant42:"]);
    // Allowed: a topic under the prefix.
    let (s, _) = h.put_auth("/v0/topics/tenant42:jobs", json!({}), "t42");
    assert!(s == StatusCode::CREATED || s == StatusCode::OK);
    let (s, _) = post(
        &h,
        "/v0/topics/tenant42:jobs",
        json!({ "records": [{ "data": 1 }] }),
        "t42",
    );
    assert_eq!(s, StatusCode::OK);
    // Forbidden: a topic OUTSIDE the prefix (even though scope is full).
    let (s, b) = h.put_auth("/v0/topics/tenant99:jobs", json!({}), "t42");
    assert_eq!(s, StatusCode::FORBIDDEN);
    assert_eq!(code(&b), "forbidden");
    let (s, b) = post(
        &h,
        "/v0/topics/tenant99:jobs",
        json!({ "records": [{ "data": 1 }] }),
        "t42",
    );
    assert_eq!(s, StatusCode::FORBIDDEN);
    assert_eq!(code(&b), "forbidden");
}

#[test]
fn watch_create_requires_read_scope() {
    let h = scoped_harness(&["full", "wo:write"]);
    post(
        &h,
        "/v0/topics/jobs",
        json!({ "records": [{ "data": 1 }] }),
        "full",
    );
    // A write-only key cannot create a watch (it is a read subscription).
    let (s, b) = post(
        &h,
        "/v0/watch",
        json!({ "topics": { "jobs": { "from_seq": 0 } } }),
        "wo",
    );
    assert_eq!(s, StatusCode::FORBIDDEN);
    assert_eq!(code(&b), "forbidden");
}

#[test]
fn invalid_scope_token_in_config_does_not_panic_router_build() {
    // A malformed scope token aborts router construction (fail-closed). The
    // checked builder surfaces the error rather than booting with bad auth.
    use std::sync::Arc;
    use topics::clock::SystemClock;
    use topics::engine::Engine;
    let cfg = ServerConfig {
        api_keys: vec!["k:bogusscope".to_string()],
        ..Default::default()
    };
    let engine = Engine::new(cfg, Arc::new(SystemClock));
    let r = topics::http::build_router_checked(engine);
    assert!(r.is_err(), "malformed scope must fail router build");
}

#[test]
fn prefix_key_cannot_watch_topic_outside_allowlist() {
    // codex HIGH #1: the prefix allowlist must gate the watch body's topic names,
    // not just the route. A read key limited to `tenant42:` cannot watch a topic
    // outside its prefix even though POST /v0/watch only needs `read`.
    let h = scoped_harness(&["full", "t42:read:tenant42:"]);
    post(
        &h,
        "/v0/topics/tenant42:jobs",
        json!({ "records": [{ "data": 1 }] }),
        "full",
    );
    post(
        &h,
        "/v0/topics/secret",
        json!({ "records": [{ "data": 1 }] }),
        "full",
    );

    // In-allowlist topic: allowed.
    let (s, _) = post(
        &h,
        "/v0/watch",
        json!({ "topics": { "tenant42:jobs": { "from_seq": 0 } } }),
        "t42",
    );
    assert_eq!(s, StatusCode::OK, "watch within prefix is allowed");

    // Out-of-allowlist topic: forbidden.
    let (s, b) = post(
        &h,
        "/v0/watch",
        json!({ "topics": { "secret": { "from_seq": 0 } } }),
        "t42",
    );
    assert_eq!(
        s,
        StatusCode::FORBIDDEN,
        "watch outside prefix is forbidden"
    );
    assert_eq!(code(&b), "forbidden");

    // A watch naming BOTH an allowed and a forbidden topic is rejected wholesale.
    let (s, _) = post(
        &h,
        "/v0/watch",
        json!({ "topics": { "tenant42:jobs": { "from_seq": 0 }, "secret": { "from_seq": 0 } } }),
        "t42",
    );
    assert_eq!(
        s,
        StatusCode::FORBIDDEN,
        "any forbidden topic rejects the watch"
    );
}

#[test]
fn prefix_key_cannot_route_to_topic_outside_allowlist() {
    // codex HIGH #2: a router's source/dest must be inside the key's allowlist,
    // not just the router-path name — else a scoped admin key could route data
    // into a forbidden topic (and auto-create it).
    let h = scoped_harness(&["adm:admin+read+write:tenant42:"]);

    // dest outside the allowlist ⇒ forbidden (would otherwise auto-create `audit`).
    let (s, b) = h.put_auth(
        "/v0/routers/tenant42:r1",
        json!({ "source": "tenant42:jobs", "dest": "audit" }),
        "adm",
    );
    assert_eq!(
        s,
        StatusCode::FORBIDDEN,
        "router dest outside prefix is forbidden"
    );
    assert_eq!(code(&b), "forbidden");

    // source outside the allowlist ⇒ forbidden.
    let (s, _) = h.put_auth(
        "/v0/routers/tenant42:r2",
        json!({ "source": "other:jobs", "dest": "tenant42:audit" }),
        "adm",
    );
    assert_eq!(
        s,
        StatusCode::FORBIDDEN,
        "router source outside prefix is forbidden"
    );

    // both in-allowlist ⇒ allowed.
    let (s, _) = h.put_auth(
        "/v0/routers/tenant42:r3",
        json!({ "source": "tenant42:jobs", "dest": "tenant42:audit" }),
        "adm",
    );
    assert!(
        s == StatusCode::CREATED || s == StatusCode::OK,
        "in-prefix router allowed: {s}"
    );
}

#[test]
fn prefix_key_list_is_filtered_to_allowlist() {
    // codex MEDIUM #7: a prefix-limited key must not enumerate cross-tenant topic or
    // router names in the list endpoints.
    let h = scoped_harness(&["full", "t42:read:tenant42:"]);
    post(
        &h,
        "/v0/topics/tenant42:a",
        json!({ "records": [{ "data": 1 }] }),
        "full",
    );
    post(
        &h,
        "/v0/topics/tenant42:b",
        json!({ "records": [{ "data": 1 }] }),
        "full",
    );
    post(
        &h,
        "/v0/topics/other:c",
        json!({ "records": [{ "data": 1 }] }),
        "full",
    );

    let (s, body) = h.get_auth("/v0/topics", "t42");
    assert_eq!(s, StatusCode::OK);
    let names: Vec<&str> = body["topics"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["topic"].as_str().unwrap())
        .collect();
    assert!(
        names.iter().all(|n| n.starts_with("tenant42:")),
        "only in-prefix topics listed: {names:?}"
    );
    assert!(names.contains(&"tenant42:a") && names.contains(&"tenant42:b"));
    assert!(
        !names.contains(&"other:c"),
        "cross-tenant topic must not be listed"
    );

    // The full key still sees everything.
    let (_, full_body) = h.get_auth("/v0/topics", "full");
    assert_eq!(full_body["topics"].as_array().unwrap().len(), 3);
}
