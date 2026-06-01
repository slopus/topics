//! Phase-3 §2 — in-process integration tests for the write + getDifference
//! (`POST /v0/boxes/:box` and `POST /v0/boxes/:box/diff`) wire contract, driven
//! over a *real bound server* via the shared `common::Harness`.
//!
//! These are black-box HTTP assertions of the documented `/v0` shapes (API §2,
//! §3, §3.2, §3.3, §0.8). The harness boots the real `http::build_router` with a
//! live `SystemClock`, so TTL-correctness here is exercised with a genuinely
//! short `ttl_ms` and a *bounded condition poll* (never a fixed sleep / exact
//! timing assert) to stay non-flaky. Watermark/tombstone correctness under a
//! manually-driven clock additionally lives in the engine unit tests with a
//! `TestClock`; this suite proves the HTTP surface matches.
//!
//! Coverage map:
//!   * append one / many: assigned seqs, first/last/head/count, 201->200.
//!   * idempotency_key dedupe: body field + `Idempotency-Key` header, original
//!     seqs, deduped:true, no second append.
//!   * diff from_seq / limit: default 256, clamp at max 1000, exact records.
//!   * cursor semantics: next_from_seq / caught_up / lag / earliest_seq.
//!   * node loop-prevention: origin sees none of its own; cursor still advances
//!     to caught_up (silent, no tombstone).
//!   * cap-eviction tombstone: reason=cap, gap_from/gap_to, missed_estimate.
//!   * TTL-expiry tombstone: reason=ttl, gap bounds (short real ttl).
//!   * wait_ms long-poll: returns promptly when a record arrives.

mod common;

use std::thread;
use std::time::{Duration, Instant};

use common::{Harness, StatusCode};
use serde_json::{json, Value};

// ---------------------------------------------------------------------------
// Append: one record / many records (API §2)
// ---------------------------------------------------------------------------

#[test]
fn write_single_record_auto_creates_and_assigns_seq() {
    let h = Harness::start();

    // First write to a not-yet-existent box auto-creates -> 201 Created.
    let (status, body) = h.post(
        "/v0/boxes/jobs",
        json!({ "records": [{ "data": { "url": "s3://b/a.png" } }] }),
    );
    assert_eq!(
        status,
        StatusCode::CREATED,
        "first write auto-creates -> 201"
    );
    assert_eq!(body["box"], "jobs");
    assert_eq!(body["first_seq"], 1, "seqs start at SEQ_BASE = 1");
    assert_eq!(body["last_seq"], 1);
    assert_eq!(body["seqs"], json!([1]));
    assert_eq!(body["head_seq"], 1);
    assert_eq!(body["count"], 1);
    assert_eq!(
        body["created"], true,
        "this write brought the box into existence"
    );
    assert_eq!(body["deduped"], false);
    assert!(body["performance"]["server_total_ms"].is_number());

    // A subsequent write to the now-existing box -> 200, created:false, seqs continue.
    let (status, body) = h.post(
        "/v0/boxes/jobs",
        json!({ "records": [{ "data": { "url": "s3://b/b.png" } }] }),
    );
    assert_eq!(status, StatusCode::OK, "write to existing box -> 200");
    assert_eq!(body["first_seq"], 2);
    assert_eq!(body["seqs"], json!([2]));
    assert_eq!(body["head_seq"], 2);
    assert_eq!(body["created"], false);
}

#[test]
fn write_many_records_assigns_contiguous_seqs() {
    let h = Harness::start();

    let (status, body) = h.post(
        "/v0/boxes/batch",
        json!({ "records": [
            { "data": 1 }, { "data": 2 }, { "data": 3 }, { "data": 4 }, { "data": 5 }
        ] }),
    );
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["first_seq"], 1);
    assert_eq!(body["last_seq"], 5);
    assert_eq!(
        body["seqs"],
        json!([1, 2, 3, 4, 5]),
        "contiguous, in array order"
    );
    assert_eq!(body["head_seq"], 5);
    assert_eq!(body["count"], 5);

    // GET state reflects head_seq / count / next_seq exactly.
    let (status, state) = h.get("/v0/boxes/batch");
    assert_eq!(status, StatusCode::OK);
    assert_eq!(state["head_seq"], 5);
    assert_eq!(state["earliest_seq"], 1);
    assert_eq!(state["next_seq"], 6);
    assert_eq!(state["count"], 5);
}

// ---------------------------------------------------------------------------
// Idempotency-key dedupe (API §0.8): body field + Idempotency-Key header.
// ---------------------------------------------------------------------------

#[test]
fn idempotency_key_body_dedupes_returns_original_seqs() {
    let h = Harness::start();

    let req = json!({
        "records": [{ "data": { "job": 1 } }, { "data": { "job": 2 } }],
        "idempotency_key": "client-batch-7f3a"
    });

    // First write appends seqs 1,2.
    let (status, first) = h.post("/v0/boxes/q", req.clone());
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(first["seqs"], json!([1, 2]));
    assert_eq!(first["deduped"], false);

    // Retry with the same in-window key -> ORIGINAL seqs, deduped:true, no append.
    let (status, second) = h.post("/v0/boxes/q", req);
    assert_eq!(status, StatusCode::OK, "dedupe hit is not a create -> 200");
    assert_eq!(second["deduped"], true);
    assert_eq!(second["seqs"], json!([1, 2]), "original seqs returned");
    assert_eq!(second["first_seq"], 1);
    assert_eq!(second["last_seq"], 2);
    assert_eq!(second["head_seq"], 2);

    // The box still has exactly the original two records (no second append).
    let (_, state) = h.get("/v0/boxes/q");
    assert_eq!(state["head_seq"], 2, "no new append happened");
    assert_eq!(state["count"], 2);
}

#[test]
fn idempotency_key_header_dedupes_when_body_omits_it() {
    let h = Harness::start();

    // First write carries the key only as the `Idempotency-Key` HTTP header.
    let body = json!({ "records": [{ "data": "x" }] });
    let (status, first) = post_with_idem_header(&h, "/v0/boxes/qh", body.clone(), "hdr-key-1");
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(first["seqs"], json!([1]));
    assert_eq!(first["deduped"], false);

    // A retry with the same header key dedupes to the original seq.
    let (status, second) = post_with_idem_header(&h, "/v0/boxes/qh", body, "hdr-key-1");
    assert_eq!(status, StatusCode::OK);
    assert_eq!(second["deduped"], true);
    assert_eq!(second["seqs"], json!([1]));

    let (_, state) = h.get("/v0/boxes/qh");
    assert_eq!(
        state["head_seq"], 1,
        "header-keyed retry did not append again"
    );
}

#[test]
fn idempotency_body_field_wins_over_header() {
    let h = Harness::start();

    // Seed key "A" via body.
    let (_, a) = h.post(
        "/v0/boxes/qw",
        json!({ "records": [{ "data": 1 }], "idempotency_key": "A" }),
    );
    assert_eq!(a["seqs"], json!([1]));

    // Now send a request whose BODY key is "A" but HEADER key is "B". Body wins,
    // so this dedupes to "A"'s original seqs (does NOT append a fresh record).
    let (status, body) = post_with_idem_header(
        &h,
        "/v0/boxes/qw",
        json!({ "records": [{ "data": 2 }], "idempotency_key": "A" }),
        "B",
    );
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["deduped"], true, "body field wins -> dedupe on A");
    assert_eq!(body["seqs"], json!([1]));

    let (_, state) = h.get("/v0/boxes/qw");
    assert_eq!(state["head_seq"], 1, "body key won, no append");
}

// ---------------------------------------------------------------------------
// diff: records, cursor semantics, defaults, limit + clamp (API §3)
// ---------------------------------------------------------------------------

#[test]
fn diff_returns_records_with_cursor_caughtup_and_lag() {
    let h = Harness::start();
    seed(&h, "d", 3);

    // include_tags omitted -> default false -> $tag absent; include_meta default true.
    let (status, body) = h.post("/v0/boxes/d/diff", json!({ "from_seq": 0 }));
    assert_eq!(status, StatusCode::OK);
    let recs = body["records"].as_array().unwrap();
    assert_eq!(recs.len(), 3);
    assert_eq!(recs[0]["$seq"], 1);
    assert_eq!(recs[2]["$seq"], 3);
    assert!(
        recs[0].get("$tag").is_none(),
        "include_tags defaults to false"
    );
    assert!(recs[0]["$ts"].is_number(), "$ts always present");
    assert_eq!(body["next_from_seq"], 3);
    assert_eq!(body["head_seq"], 3);
    assert_eq!(body["earliest_seq"], 1);
    assert_eq!(body["caught_up"], true);
    assert_eq!(body["tombstone"], Value::Null);
    assert_eq!(body["lag"], 0);
    assert!(body["performance"]["records_scanned"].is_number());

    // Partial read from a mid cursor: not caught up, lag reflects the remainder.
    let (_, body) = h.post("/v0/boxes/d/diff", json!({ "from_seq": 1, "limit": 1 }));
    let recs = body["records"].as_array().unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0]["$seq"], 2);
    assert_eq!(body["next_from_seq"], 2);
    assert_eq!(body["caught_up"], false);
    assert_eq!(body["lag"], 1, "head 3 - next_from_seq 2 = 1");

    // Tailing from head returns nothing new but stays caught up.
    let (_, body) = h.post("/v0/boxes/d/diff", json!({ "from_seq": 3 }));
    assert_eq!(body["records"].as_array().unwrap().len(), 0);
    assert_eq!(body["caught_up"], true);
    assert_eq!(body["next_from_seq"], 3);
    assert_eq!(body["lag"], 0);
}

#[test]
fn diff_include_tags_and_meta_flags() {
    let h = Harness::start();
    let (status, _) = h.post(
        "/v0/boxes/im",
        json!({ "records": [{ "data": 1, "tag": "t1", "meta": { "k": "v" } }] }),
    );
    assert_eq!(status, StatusCode::CREATED);

    // include_tags:true surfaces $tag; include_meta:true (default) keeps meta.
    let (_, body) = h.post(
        "/v0/boxes/im/diff",
        json!({ "from_seq": 0, "include_tags": true }),
    );
    let rec = &body["records"][0];
    assert_eq!(rec["$tag"], "t1");
    assert_eq!(rec["meta"], json!({ "k": "v" }));

    // include_meta:false drops meta; $tag also absent (include_tags default false).
    let (_, body) = h.post(
        "/v0/boxes/im/diff",
        json!({ "from_seq": 0, "include_meta": false }),
    );
    let rec = &body["records"][0];
    assert!(rec.get("meta").is_none(), "include_meta:false omits meta");
    assert!(rec.get("$tag").is_none());
}

#[test]
fn diff_default_limit_is_256() {
    let h = Harness::start();
    seed(&h, "lim", 300);

    // No limit field -> default 256 (API §3). 300 records, so first page = 256.
    let (status, body) = h.post("/v0/boxes/lim/diff", json!({ "from_seq": 0 }));
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["records"].as_array().unwrap().len(),
        256,
        "default limit 256"
    );
    assert_eq!(body["next_from_seq"], 256);
    assert_eq!(body["caught_up"], false);
    assert_eq!(body["lag"], 44, "head 300 - 256");

    // limit:0 is treated as the default too.
    let (_, body) = h.post("/v0/boxes/lim/diff", json!({ "from_seq": 0, "limit": 0 }));
    assert_eq!(
        body["records"].as_array().unwrap().len(),
        256,
        "limit:0 => default 256"
    );
}

#[test]
fn diff_limit_clamped_at_max_1000() {
    let h = Harness::start();
    seed(&h, "big", 1200);

    // limit far above MAX_LIMIT (1000) is clamped, never rejected (API §3).
    let (status, body) = h.post(
        "/v0/boxes/big/diff",
        json!({ "from_seq": 0, "limit": 5000 }),
    );
    assert_eq!(
        status,
        StatusCode::OK,
        "over-max limit is clamped, not a 400"
    );
    assert_eq!(
        body["records"].as_array().unwrap().len(),
        1000,
        "clamped to MAX_LIMIT 1000"
    );
    assert_eq!(body["next_from_seq"], 1000);
    assert_eq!(body["caught_up"], false);

    // A small explicit limit is honored exactly.
    let (_, body) = h.post("/v0/boxes/big/diff", json!({ "from_seq": 0, "limit": 10 }));
    assert_eq!(body["records"].as_array().unwrap().len(), 10);
    assert_eq!(body["next_from_seq"], 10);
}

#[test]
fn diff_on_missing_box_is_404_never_auto_creates() {
    let h = Harness::start();
    let (status, body) = h.post("/v0/boxes/ghost/diff", json!({ "from_seq": 0 }));
    assert_eq!(status, StatusCode::NOT_FOUND, "diff never auto-creates");
    assert_eq!(body["error"]["code"], "box_not_found");
    // The box must still not exist.
    let (status, _) = h.get("/v0/boxes/ghost");
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ---------------------------------------------------------------------------
// Node loop-prevention (API §4 / §3.2): origin gets none of its own records,
// but the cursor advances to caught_up (no infinite empty loop), silently.
// ---------------------------------------------------------------------------

#[test]
fn diff_node_loop_prevention_advances_cursor_silently() {
    let h = Harness::start();
    // Records 1,2 from node "self"; record 3 from "other".
    let (status, _) = h.post(
        "/v0/boxes/nb",
        json!({ "records": [
            { "data": 1, "node": "self" },
            { "data": 2, "node": "self" },
            { "data": 3, "node": "other" }
        ] }),
    );
    assert_eq!(status, StatusCode::CREATED);

    // Reader presenting "self" sees only the "other" record; cursor advances past
    // its own (skipped) records and reaches caught_up; no tombstone (silent).
    let (status, body) = h.post(
        "/v0/boxes/nb/diff",
        json!({ "from_seq": 0, "node": "self" }),
    );
    assert_eq!(status, StatusCode::OK);
    let recs = body["records"].as_array().unwrap();
    assert_eq!(recs.len(), 1, "only the other-node record is delivered");
    assert_eq!(recs[0]["$seq"], 3);
    assert_eq!(
        recs[0]["$node"], "other",
        "$node echoed for delivered records"
    );
    assert_eq!(body["next_from_seq"], 3, "cursor advanced past own records");
    assert_eq!(
        body["caught_up"], true,
        "reaches caught_up, not an empty loop"
    );
    assert_eq!(body["tombstone"], Value::Null, "node filtering is silent");

    // A reader of ONLY-own-node records: zero delivered yet caught_up reached and
    // the cursor advanced to head (the §3.2 reliable-no-more signal).
    let (status, _) = h.post(
        "/v0/boxes/own",
        json!({ "records": [{ "data": 1, "node": "me" }, { "data": 2, "node": "me" }] }),
    );
    assert_eq!(status, StatusCode::CREATED);
    let (_, body) = h.post("/v0/boxes/own/diff", json!({ "from_seq": 0, "node": "me" }));
    assert_eq!(body["records"].as_array().unwrap().len(), 0);
    assert_eq!(body["caught_up"], true);
    assert_eq!(body["next_from_seq"], 2);
    assert_eq!(body["lag"], 0);

    // A node-filter ARRAY ("node": ["a","b"]) drops several of one's identities.
    let (status, _) = h.post(
        "/v0/boxes/multi",
        json!({ "records": [
            { "data": 1, "node": "a" },
            { "data": 2, "node": "b" },
            { "data": 3, "node": "c" }
        ] }),
    );
    assert_eq!(status, StatusCode::CREATED);
    let (_, body) = h.post(
        "/v0/boxes/multi/diff",
        json!({ "from_seq": 0, "node": ["a", "b"] }),
    );
    let recs = body["records"].as_array().unwrap();
    assert_eq!(recs.len(), 1, "drop if $node in {{a,b}}");
    assert_eq!(recs[0]["$seq"], 3);
    assert_eq!(body["caught_up"], true);
    assert_eq!(body["next_from_seq"], 3);
}

// ---------------------------------------------------------------------------
// Cap-eviction tombstone (API §3.3): reason=cap, gap range, missed_estimate.
// Cap is count-based — deterministic under the harness's live clock.
// ---------------------------------------------------------------------------

#[test]
fn diff_cap_eviction_emits_cap_tombstone() {
    let h = Harness::start();
    // cap_records=3, discard:"old" (default). Pre-create with the cap.
    let (status, _) = h.put("/v0/boxes/cap", json!({ "cap_records": 3 }));
    assert_eq!(status, StatusCode::CREATED);

    // Write 5 -> seqs 1..=5; cap evicts 1,2 -> earliest_seq=3, evict_floor=2.
    for i in 1..=5 {
        let (status, _) = h.post("/v0/boxes/cap", json!({ "records": [{ "data": i }] }));
        assert_eq!(status, StatusCode::OK);
    }
    let (_, state) = h.get("/v0/boxes/cap");
    assert_eq!(state["head_seq"], 5);
    assert_eq!(state["earliest_seq"], 3);
    assert_eq!(state["count"], 3);

    // A consumer at from_seq=0 fell below the involuntary floor -> cap tombstone.
    let (status, body) = h.post("/v0/boxes/cap/diff", json!({ "from_seq": 0 }));
    assert_eq!(
        status,
        StatusCode::OK,
        "tombstone is an in-band 200, never an error"
    );
    let tomb = &body["tombstone"];
    assert!(tomb.is_object(), "expected a tombstone object, got {tomb}");
    assert_eq!(tomb["reason"], "cap");
    assert_eq!(tomb["gap_from"], 1, "first missing seq = from_seq + 1");
    assert_eq!(tomb["gap_to"], 2, "last missing seq = earliest_seq - 1");
    assert_eq!(tomb["missed_estimate"], 2, "[1,2] inclusive => 2");
    assert_eq!(tomb["earliest_seq"], 3);
    assert_eq!(tomb["head_seq"], 5);

    // Records resume at earliest_seq; cursor continues normally to caught_up.
    let recs = body["records"].as_array().unwrap();
    assert_eq!(
        recs.first().unwrap()["$seq"],
        3,
        "records begin at earliest_seq"
    );
    assert_eq!(recs.len(), 3);
    assert_eq!(body["earliest_seq"], 3);
    assert_eq!(body["caught_up"], true);

    // A consumer already at/above the floor gets NO tombstone (gap doesn't reach it).
    let (_, body) = h.post("/v0/boxes/cap/diff", json!({ "from_seq": 3 }));
    assert_eq!(body["tombstone"], Value::Null);
    assert_eq!(body["records"].as_array().unwrap().len(), 2, "seqs 4,5");
}

// ---------------------------------------------------------------------------
// TTL-expiry tombstone (API §3.3, reason=ttl) — driven by an injected TestClock.
//
// Correctness without wall-clock: boot the harness on a `TestClock`
// (`start_with_test_clock`) so the server's notion of "now" is advanced
// deterministically past the TTL window — no real sleeps, no polling, no
// flakiness. `enforce_retention` is lazy (runs on each write/state read), so
// after advancing the clock we append seq 4 to trigger it, then read the box
// state ONCE and assert the exact steady-state shape.
// ---------------------------------------------------------------------------

#[test]
fn diff_ttl_expiry_emits_ttl_tombstone() {
    let h = Harness::start_with_test_clock();
    let ttl_ms = 150u64;
    let (status, _) = h.put("/v0/boxes/ttl", json!({ "ttl_ms": ttl_ms }));
    assert_eq!(status, StatusCode::CREATED);

    // Write 3 records (stamped at the clock's current time) that will expire.
    for i in 1..=3 {
        let (status, _) = h.post("/v0/boxes/ttl", json!({ "records": [{ "data": i }] }));
        assert_eq!(status, StatusCode::OK);
    }

    // Advance the server's clock past the TTL window for those three records, then
    // append seq 4 (stamped at the new time) to trigger the lazy retention sweep.
    // Because the clock is injected, this is exact and instantaneous — the post
    // and the state read below observe the post-advance retention with no race.
    h.clock().advance((ttl_ms + 50) as i64);
    let (status, _) = h.post("/v0/boxes/ttl", json!({ "records": [{ "data": 4 }] }));
    assert_eq!(status, StatusCode::OK);

    let (_, state) = h.get("/v0/boxes/ttl");
    assert_eq!(
        state["earliest_seq"].as_u64().unwrap(),
        4,
        "the expired prefix [1,3] is reclaimed; earliest advances to 4"
    );
    assert_eq!(state["head_seq"], 4);
    assert_eq!(state["count"], 1, "only seq 4 remains live");

    // A consumer at from_seq=0 now crosses the TTL gap -> ttl tombstone.
    let (status, body) = h.post("/v0/boxes/ttl/diff", json!({ "from_seq": 0 }));
    assert_eq!(status, StatusCode::OK);
    let tomb = &body["tombstone"];
    assert!(tomb.is_object(), "expected a ttl tombstone, got {tomb}");
    assert_eq!(tomb["reason"], "ttl");
    assert_eq!(tomb["gap_from"], 1);
    assert_eq!(tomb["gap_to"], 3, "earliest_seq 4 - 1");
    assert_eq!(tomb["missed_estimate"], 3, "[1,3] inclusive");
    assert_eq!(tomb["earliest_seq"], 4);
    assert_eq!(tomb["head_seq"], 4);
    // The surviving record resumes the stream.
    let recs = body["records"].as_array().unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0]["$seq"], 4);
    assert_eq!(body["caught_up"], true);
}

// ---------------------------------------------------------------------------
// wait_ms long-poll (API §3): returns PROMPTLY when a record arrives. A
// background thread writes after a brief delay; the blocking diff must wake.
// ---------------------------------------------------------------------------

#[test]
fn diff_wait_ms_long_poll_wakes_on_append() {
    let h = Harness::start();
    // Seed one record, then tail from head: a plain diff is immediately caught up
    // with nothing to deliver, which is exactly when wait_ms parks.
    let (status, _) = h.post("/v0/boxes/lp", json!({ "records": [{ "data": 0 }] }));
    assert_eq!(status, StatusCode::CREATED);

    let base = h.base_url().to_string();

    // Background writer appends seq 2 after ~150 ms.
    let writer = thread::spawn(move || {
        thread::sleep(Duration::from_millis(150));
        let client = reqwest::blocking::Client::new();
        let _ = client
            .post(format!("{base}/v0/boxes/lp"))
            .json(&json!({ "records": [{ "data": 1 }] }))
            .send();
    });

    // Long-poll from head with a generous wait; it must return after the append,
    // well before the wait_ms ceiling.
    let started = Instant::now();
    let (status, body) = h.post(
        "/v0/boxes/lp/diff",
        json!({ "from_seq": 1, "wait_ms": 5000 }),
    );
    let elapsed = started.elapsed();
    writer.join().unwrap();

    assert_eq!(status, StatusCode::OK);
    let recs = body["records"].as_array().unwrap();
    assert_eq!(recs.len(), 1, "the newly-appended record is delivered");
    assert_eq!(recs[0]["$seq"], 2);
    assert_eq!(body["next_from_seq"], 2);
    assert_eq!(body["caught_up"], true);
    assert!(
        elapsed < Duration::from_secs(3),
        "long-poll returned promptly on append ({elapsed:?}), not after the full wait_ms"
    );
}

#[test]
fn diff_wait_ms_returns_at_deadline_when_idle() {
    let h = Harness::start();
    let (status, _) = h.post("/v0/boxes/idle", json!({ "records": [{ "data": 0 }] }));
    assert_eq!(status, StatusCode::CREATED);

    // Nothing else writes; the long-poll parks then returns caught-up at the
    // (short) deadline. We only assert it returns the correct shape, not exact ms.
    let started = Instant::now();
    let (status, body) = h.post(
        "/v0/boxes/idle/diff",
        json!({ "from_seq": 1, "wait_ms": 200 }),
    );
    let elapsed = started.elapsed();
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["records"].as_array().unwrap().len(), 0);
    assert_eq!(body["caught_up"], true);
    assert_eq!(body["next_from_seq"], 1);
    assert!(
        elapsed >= Duration::from_millis(150),
        "idle long-poll waited roughly the deadline ({elapsed:?})"
    );
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// `POST path` with a JSON body and an `Idempotency-Key` HTTP header. The shared
/// harness has no header helper and is off-limits to edit, so this issues the
/// request directly via `reqwest::blocking` against the harness's bound port
/// (reqwest is a workspace dev-dependency). Returns `(status, parsed-json)`.
fn post_with_idem_header(
    h: &Harness,
    path: &str,
    body: Value,
    idem_key: &str,
) -> (StatusCode, Value) {
    let client = reqwest::blocking::Client::new();
    let resp = client
        .post(format!("{}{}", h.base_url(), path))
        .header("Idempotency-Key", idem_key)
        .json(&body)
        .send()
        .expect("request failed to send");
    let status = resp.status();
    let bytes = resp.bytes().expect("read response body");
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, value)
}

/// Append `n` records (data = 1..=n) to `box_name`, one batch, asserting success.
fn seed(h: &Harness, box_name: &str, n: usize) {
    let records: Vec<Value> = (1..=n).map(|i| json!({ "data": i })).collect();
    let (status, body) = h.post(
        &format!("/v0/boxes/{box_name}"),
        json!({ "records": records }),
    );
    assert!(
        status == StatusCode::CREATED || status == StatusCode::OK,
        "seed write failed: {status} {body}"
    );
    assert_eq!(body["head_seq"], n as u64);
}
