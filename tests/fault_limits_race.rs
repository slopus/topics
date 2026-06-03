//! Phase-8A RELIABILITY iter-2 — **TOCTOU + concurrency on the new scopes/limits**.
//!
//! These probe the *recently-added* resource-limit layer ([`topics::limits`])
//! and the engine creation/write/queue paths that enforce it, under concurrent
//! racing actors — the check-then-act windows that a serial test never exercises.
//! The Phase-8A harness ([`topics::storage::testfs::FakeDisk`] +
//! [`topics::engine::Engine::with_data_dir_fs`]) drives the *real, fully-wired*
//! engine; one test adds a crash + recovery on top to prove the durable byte
//! quota (and the per-topic commit sequencer it shares the write path with) stays
//! consistent across a power loss.
//!
//! | id | what it pins |
//! |----|--------------|
//! | `L-MAX-TOPICS-RACE`        | N threads racing `put_topic` (distinct names) against `max_topics=N`: the surviving topic count never exceeds the cap; refusals are `429 throttled`. |
//! | `L-MAX-TOPICS-AUTOCREATE-RACE` | N threads racing auto-create-on-write against `max_topics`: the auto-create path honors the same cap (no write smuggles a topic past it). |
//! | `L-MAX-ROUTERS-RACE`      | N threads racing `put_router` against `max_routers=N`: router count never exceeds the cap; a refused router leaves no phantom dest topic. |
//! | `L-MAX-WATCH-SESSIONS-RACE` | N HTTP threads racing `POST /v0/watch` against `max_watch_sessions=N`: live session count never exceeds the cap (HTTP harness). |
//! | `L-SESSION-GC-RACE-OPEN`  | the idle-session GC reaping a session racing a stream open on a sibling session: no use-after-GC (the open either succeeds on a live session or cleanly 404s; the GC never frees a session with an open stream). |
//! | `L-TOTAL-BYTES-QUOTA-RACE`| many concurrent writers against `max_total_bytes`: the committed live total never exceeds the cap (TOCTOU on the admission read); refusals are `429 throttled`. |
//! | `L-TOTAL-BYTES-QUOTA-CRASH` | the durable byte quota + per-topic commit sequencer across a crash: every acked write survives, the recovered total stays at/under the cap, survivors are a dense prefix. |
//! | `L-SCOPE-CONSISTENT-RACE` | concurrent scoped-key requests through the HTTP auth layer: a write-scoped key can never read and a read-scoped key can never write, regardless of interleaving (scope check is per-request, stateless). |
//!
//! Reuses the harness exactly (per `tests/crash_oracle.rs` + `src/storage/testfs.rs`
//! + `tests/common/mod.rs`). All sweeps/stress are BOUNDED (small N, fixed seeds,
//! capped threads) so the whole file runs in well under a minute. Loom/shuttle are
//! not wired into this crate's Cargo.toml (no dev-dep), so the concurrency probes
//! are robust high-thread STRESS tests asserting the same invariants — the
//! documented Phase-8 fallback.
//!
//! ```text
//! cargo test --features test-fs --test fault_limits_race
//! ```

#![cfg(feature = "test-fs")]
#![allow(
    clippy::ptr_arg,
    clippy::manual_clamp,
    clippy::unusual_byte_groupings,
    clippy::doc_lazy_continuation
)]
#![allow(clippy::needless_range_loop)]

mod common;

use std::collections::BTreeSet;
use std::io::Read;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::Duration;

use serde_json::json;

use common::{Harness, StatusCode};
use topics::clock::{SharedClock, TestClock};
use topics::config::ServerConfig;
use topics::engine::Engine;
use topics::limits::Limits;
use topics::storage::testfs::{FakeDisk, TornDamage};
use topics::types::{
    DiffRequest, Durability, RecordIn, RouterCreateRequest, TopicConfig, TopicType, WriteRequest,
};

// ===========================================================================
// Shared plumbing (mirrors tests/crash_oracle.rs + tests/fault_concurrency_a.rs).
// ===========================================================================

const DATA_DIR: &str = "/data";

/// A server config pointed at the FakeDisk data dir with the given limits.
fn cfg_with(limits: Limits) -> ServerConfig {
    ServerConfig {
        data_dir: Some(DATA_DIR.to_string()),
        limits,
        ..Default::default()
    }
}

fn clock() -> SharedClock {
    Arc::new(TestClock::new(1_700_000_000_000))
}

/// Build a fresh durable engine over `disk` with the given limits.
fn open_engine_limits(disk: &FakeDisk, limits: Limits) -> Arc<Engine> {
    Engine::with_data_dir_fs(cfg_with(limits), clock(), disk.arc())
        .expect("engine opens through FakeDisk")
}

/// A one-record write request (auto-creating the topic) with `data` bytes.
fn write_req(data: serde_json::Value) -> WriteRequest {
    WriteRequest {
        records: vec![RecordIn {
            data,
            tag: None,
            node: None,
            meta: None,
        }],
        node: None,
        idempotency_key: None,
        create: Some(true),
        config: None,
        disable_backpressure: true,
    }
}

/// A durable (fsync) log topic config (the strongest class — the WAL-first commit
/// sequencer path).
fn durable_topic() -> TopicConfig {
    TopicConfig {
        r#type: TopicType::Log,
        durability: Some(Durability::Fsync),
        ..Default::default()
    }
}

// ===========================================================================
// L-MAX-TOPICS-RACE
// ---------------------------------------------------------------------------
// N threads each try to create a DISTINCT topic name against `max_topics = CAP`.
// The cap is a check-then-create guard (read topic_count, then insert). The
// INVARIANT the limit promises: the surviving topic count never exceeds CAP, and
// every refusal is a `429 throttled`. We assert the strong contract; if the
// non-atomic read-then-create overshoots the cap under the race, that is a real
// divergence from "never exceed N" and the test fails loudly (then would be
// #[ignore]'d with a repro per the bug policy).
// ===========================================================================

#[test]
fn l_max_topics_race_never_exceeds_cap() {
    const CAP: u64 = 16;
    const RACERS: usize = 48; // 3x the cap ⇒ most creates must be refused.

    let disk = FakeDisk::new();
    let engine = open_engine_limits(
        &disk,
        Limits {
            max_topics: CAP,
            ..Default::default()
        },
    );

    let start = Arc::new(Barrier::new(RACERS));
    let created = Arc::new(AtomicU64::new(0));
    let throttled = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for i in 0..RACERS {
        let engine = engine.clone();
        let start = start.clone();
        let created = created.clone();
        let throttled = throttled.clone();
        handles.push(std::thread::spawn(move || {
            let name = format!("topic-{i}");
            start.wait();
            match engine.put_topic(&name, durable_topic()) {
                Ok(_) => {
                    created.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    assert_eq!(
                        e.code,
                        topics::types::ErrorCode::Throttled,
                        "topic create over cap must be throttled, got {e:?}"
                    );
                    throttled.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }
    for h in handles {
        h.join().expect("no creator panicked");
    }

    let ok = created.load(Ordering::Relaxed);
    let refused = throttled.load(Ordering::Relaxed);
    // Every racer either created or was throttled (no silent drop).
    assert_eq!(ok + refused, RACERS as u64, "every create accounted for");
    // INVARIANT: the surviving topic count never exceeds the cap.
    assert!(
        engine.topic_count() <= CAP,
        "topic_count {} exceeded the cap {CAP} under the create race",
        engine.topic_count()
    );
    // The number of Ok creates equals the surviving topics (each Ok was a distinct
    // name, each refusal created nothing).
    assert_eq!(
        ok,
        engine.topic_count(),
        "every Ok create is a surviving topic, every refusal left none"
    );
    drop(engine);
}

// ===========================================================================
// L-MAX-TOPICS-AUTOCREATE-RACE
// ---------------------------------------------------------------------------
// The auto-create-on-write path is a SECOND door onto the topic registry and must
// honor the SAME `max_topics` cap. N threads each write to a distinct (new) topic;
// the surviving topic count never exceeds the cap and an over-cap write is `429`.
// ===========================================================================

#[test]
fn l_max_topics_autocreate_race_never_exceeds_cap() {
    const CAP: u64 = 12;
    const RACERS: usize = 40;

    let disk = FakeDisk::new();
    let engine = open_engine_limits(
        &disk,
        Limits {
            max_topics: CAP,
            ..Default::default()
        },
    );

    let start = Arc::new(Barrier::new(RACERS));
    let created = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for i in 0..RACERS {
        let engine = engine.clone();
        let start = start.clone();
        let created = created.clone();
        handles.push(std::thread::spawn(move || {
            let name = format!("auto-{i}");
            start.wait();
            match engine.write(&name, write_req(json!({ "v": i })), true) {
                Ok(_) => {
                    created.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => assert_eq!(
                    e.code,
                    topics::types::ErrorCode::Throttled,
                    "over-cap auto-create write must be throttled, got {e:?}"
                ),
            }
        }));
    }
    for h in handles {
        h.join().expect("no writer panicked");
    }

    assert!(
        engine.topic_count() <= CAP,
        "auto-create smuggled topics past the cap: topic_count {} > {CAP}",
        engine.topic_count()
    );
    assert_eq!(
        created.load(Ordering::Relaxed),
        engine.topic_count(),
        "each successful write created exactly one surviving topic"
    );
    drop(engine);
}

// ===========================================================================
// L-MAX-ROUTERS-RACE
// ---------------------------------------------------------------------------
// N threads racing `put_router` (distinct router names, shared source so the
// source-topic auto-create does not itself trip a topic cap) against
// `max_routers = CAP`. Router count never exceeds the cap; a refused router is
// `429` and (checked before any dest auto-create) leaves no phantom dest topic.
// ===========================================================================

#[test]
fn l_max_routers_race_never_exceeds_cap() {
    const CAP: u64 = 8;
    const RACERS: usize = 32;

    let disk = FakeDisk::new();
    let engine = open_engine_limits(
        &disk,
        Limits {
            max_routers: CAP,
            max_topics: 0, // unlimited topics so router dest auto-create is unconstrained.
            ..Default::default()
        },
    );
    // A shared source topic so every router's source already exists.
    engine.put_topic("src", durable_topic()).expect("src topic");

    let start = Arc::new(Barrier::new(RACERS));
    let created = Arc::new(AtomicU64::new(0));
    let refused_dests = Arc::new(Mutex::new(Vec::<String>::new()));
    let mut handles = Vec::new();
    for i in 0..RACERS {
        let engine = engine.clone();
        let start = start.clone();
        let created = created.clone();
        let refused_dests = refused_dests.clone();
        handles.push(std::thread::spawn(move || {
            let rname = format!("r-{i}");
            let dest = format!("dst-{i}");
            let req = RouterCreateRequest {
                source: "src".into(),
                dest: dest.clone(),
                preserve_node: true,
                preserve_tag: true,
                create_dest: true,
                filter: None,
                allow_cycle: false,
                guarantee: Default::default(),
            };
            start.wait();
            match engine.put_router(&rname, req) {
                Ok(_) => {
                    created.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    assert_eq!(
                        e.code,
                        topics::types::ErrorCode::Throttled,
                        "router over cap must be throttled, got {e:?}"
                    );
                    refused_dests.lock().unwrap().push(dest);
                }
            }
        }));
    }
    for h in handles {
        h.join().expect("no router creator panicked");
    }

    assert!(
        engine.router_count() <= CAP,
        "router_count {} exceeded the cap {CAP} under the race",
        engine.router_count()
    );
    assert_eq!(
        created.load(Ordering::Relaxed),
        engine.router_count(),
        "each Ok put_router is a surviving router"
    );
    // A refused router must NOT have auto-created its dest topic (the cap check is
    // before any topic auto-create).
    for dest in refused_dests.lock().unwrap().iter() {
        assert!(
            engine.get_topic(dest).is_none(),
            "refused router left a phantom dest topic {dest:?}"
        );
    }
    drop(engine);
}

// ===========================================================================
// L-MAX-WATCH-SESSIONS-RACE  (HTTP harness)
// ---------------------------------------------------------------------------
// N HTTP threads racing `POST /v0/watch` against `max_watch_sessions = CAP`.
// The session registry is a DashMap; the cap is checked against its `len()`
// (check-then-insert). The live session count never exceeds the cap and every
// over-cap create is `429 throttled`. (Black-topic over the real bound server.)
// ===========================================================================

#[test]
fn l_max_watch_sessions_race_never_exceeds_cap() {
    const CAP: u64 = 10;
    const RACERS: usize = 32;

    let h = Arc::new(Harness::start_with(ServerConfig {
        limits: Limits {
            max_watch_sessions: CAP,
            ..Default::default()
        },
        ..Default::default()
    }));
    let (s, _) = h.put("/v0/topics/events", json!({}));
    assert_eq!(s, StatusCode::CREATED);

    let start = Arc::new(Barrier::new(RACERS));
    let ok = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for _ in 0..RACERS {
        let h = h.clone();
        let start = start.clone();
        let ok = ok.clone();
        handles.push(std::thread::spawn(move || {
            start.wait();
            let (status, body) = h.post("/v0/watch", json!({ "topics": { "events": {} } }));
            if status == StatusCode::OK {
                ok.fetch_add(1, Ordering::Relaxed);
            } else {
                assert_eq!(
                    status,
                    StatusCode::TOO_MANY_REQUESTS,
                    "over-cap watch create must be 429: {body}"
                );
                assert_eq!(body["error"]["code"].as_str(), Some("throttled"));
            }
        }));
    }
    for hd in handles {
        hd.join().expect("no watcher panicked");
    }

    // The cap is checked against the live registry len; no GET stream was opened so
    // no session was GC'd. The successful creates never exceed the cap.
    let admitted = ok.load(Ordering::Relaxed);
    assert!(
        admitted <= CAP,
        "admitted {admitted} watch sessions exceeded the cap {CAP} under the race"
    );
    assert!(admitted >= 1, "at least one session admitted (sanity)");
    // Re-confirm: a fresh create after the storm is refused (the cap is full) iff we
    // actually filled it.
    if admitted == CAP {
        let (status, body) = h.post("/v0/watch", json!({ "topics": { "events": {} } }));
        assert_eq!(
            status,
            StatusCode::TOO_MANY_REQUESTS,
            "the registry is full ⇒ a new session is refused: {body}"
        );
    }
}

// ===========================================================================
// L-SESSION-GC-RACE-OPEN  (HTTP harness)
// ---------------------------------------------------------------------------
// The idle-session GC reaps sessions opportunistically on create/open. We race
// MANY creates (each create runs `gc_expired` then inserts) against MANY stream
// opens of previously-minted wids. The INVARIANT: no use-after-GC — a stream
// open either (a) succeeds with a clean SSE handshake on a still-live session,
// or (b) cleanly returns a non-2xx (404/410) because its session was reaped;
// never a panic / 5xx / torn response. The active-stream count protects an
// open session from GC, so the racing GC can never free a session out from
// under an in-flight open.
//
// The harness uses a real SystemClock (no TestClock injection), so sessions are
// not actually idle-expired within the test window; this stresses the
// concurrent gc_expired-vs-open code path (DashMap::retain racing get/insert)
// for crashes/UB, asserting every open is a well-formed HTTP outcome.
// ===========================================================================

#[test]
fn l_session_gc_race_open_no_use_after_gc() {
    const SESSIONS: usize = 24;
    const OPENERS: usize = 12;

    let h = Arc::new(Harness::start_with(ServerConfig {
        // Generous cap so creation is never throttled here — we stress GC vs open,
        // not the cap.
        limits: Limits {
            max_watch_sessions: 0,
            max_sse_connections: 0,
            ..Default::default()
        },
        ..Default::default()
    }));
    let (s, _) = h.put("/v0/topics/events", json!({}));
    assert_eq!(s, StatusCode::CREATED);

    // Mint a pool of wids to open concurrently.
    let mut wids = Vec::new();
    for _ in 0..SESSIONS {
        let (status, body) = h.post("/v0/watch", json!({ "topics": { "events": {} } }));
        assert_eq!(status, StatusCode::OK);
        wids.push(body["wid"].as_str().expect("wid").to_string());
    }
    let wids = Arc::new(wids);

    let start = Arc::new(Barrier::new(OPENERS * 2));
    let mut handles = Vec::new();

    // Half the threads hammer new session creates (each runs gc_expired+insert).
    for _ in 0..OPENERS {
        let h = h.clone();
        let start = start.clone();
        handles.push(std::thread::spawn(move || {
            start.wait();
            for _ in 0..8 {
                let (status, _) = h.post("/v0/watch", json!({ "topics": { "events": {} } }));
                assert_eq!(status, StatusCode::OK, "create during GC race must be 200");
            }
        }));
    }

    // The other half open SSE streams on the minted wids (each open touches the
    // session + bumps its active-stream count under the GC race).
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let base = h.base_url().to_string();
    for t in 0..OPENERS {
        let wids = wids.clone();
        let start = start.clone();
        let client = client.clone();
        let base = base.clone();
        handles.push(std::thread::spawn(move || {
            start.wait();
            for k in 0..8usize {
                let wid = &wids[(t * 8 + k) % wids.len()];
                let url = format!("{base}/v0/watch/{wid}");
                let resp = client
                    .get(&url)
                    .header(reqwest::header::ACCEPT, "text/event-stream")
                    .send();
                match resp {
                    Ok(mut r) => {
                        let st = r.status();
                        // INVARIANT: a well-formed outcome — either the stream
                        // opens (live session, 200) or it is a clean 4xx (the
                        // session was reaped / never existed). NEVER a 5xx (a
                        // use-after-GC / panic would surface as 500).
                        assert!(
                            st == StatusCode::OK || st.is_client_error(),
                            "stream open returned an unexpected status {st} \
                             (use-after-GC would be a 5xx)"
                        );
                        // Drain a little so the stream fully establishes then drops
                        // (releasing the StreamHandle, decrementing active count).
                        let mut chunk = [0u8; 64];
                        let _ = r.read(&mut chunk);
                    }
                    // A timeout/reset on the long-lived stream is acceptable; it is
                    // not a server-side fault.
                    Err(e) => assert!(
                        e.is_timeout() || e.is_request() || e.is_connect(),
                        "unexpected transport error opening stream: {e}"
                    ),
                }
            }
        }));
    }
    for hd in handles {
        hd.join()
            .expect("no GC/open racer panicked (no use-after-GC)");
    }

    // The server is still healthy after the race (no crash / deadlock).
    let (s, _) = h.get("/v0/health");
    assert_eq!(
        s,
        StatusCode::OK,
        "server healthy after the GC-vs-open race"
    );
}

// ===========================================================================
// L-TOTAL-BYTES-QUOTA-RACE
// ---------------------------------------------------------------------------
// The global byte quota (`max_total_bytes`) is enforced on the write path with an
// ATOMIC reserve against the running live-byte total (codex P2 #10). MANY
// concurrent writers each append a fixed-size record; the committed live total must
// never exceed the cap, and every refusal is `429 throttled`.
//
// The test calibrates the per-record byte cost FIRST, then sizes the cap to admit a
// KNOWN, sub-racer-count number of records, so the quota provably BITES under the
// race (some writers must be refused). With the atomic reserve the committed total
// can never exceed the cap at all (no coarse-guard slack), so we assert the strong
// invariant `total <= CAP` exactly, and that `admitted == cap_records` (the cap
// admitted exactly its capacity, no more, no fewer).
// ===========================================================================

#[test]
fn l_total_bytes_quota_race_stays_under_cap() {
    const RACERS: usize = 32;

    // Measure one record's accounted byte cost by doing a single write on a fresh
    // engine with an unlimited quota (so the cap does not interfere), then read the
    // topic's bytes(). This makes the cap self-calibrating regardless of the exact
    // per-record overhead.
    let per_record_bytes = {
        let probe_disk = FakeDisk::new();
        let probe = open_engine_limits(
            &probe_disk,
            Limits {
                max_total_bytes: 0,
                ..Default::default()
            },
        );
        probe.put_topic("p", durable_topic()).unwrap();
        probe
            .write("p", write_req(json!({ "v": "payload" })), true)
            .unwrap();
        let b = probe.topic_state("p", false).unwrap().bytes;
        drop(probe);
        b.max(1)
    };

    // Size the cap to admit EXACTLY `cap_records` writes — well below RACERS — so the
    // quota must refuse the rest under the race. The cap is an exact multiple of the
    // per-record cost, so a correct atomic reserve admits exactly `cap_records`.
    let cap_records: u64 = 10;
    assert!(
        (cap_records as usize) < RACERS,
        "cap must bite under the race"
    );
    let cap = per_record_bytes * cap_records;

    let disk = FakeDisk::new();
    let engine = open_engine_limits(
        &disk,
        Limits {
            max_total_bytes: cap,
            ..Default::default()
        },
    );
    // One durable topic; every writer appends to it (the total is summed across all
    // topics, but a single topic keeps the accounting crisp).
    engine
        .put_topic("quota", durable_topic())
        .expect("quota topic");

    let start = Arc::new(Barrier::new(RACERS));
    let committed = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for _ in 0..RACERS {
        let engine = engine.clone();
        let start = start.clone();
        let committed = committed.clone();
        handles.push(std::thread::spawn(move || {
            start.wait();
            match engine.write("quota", write_req(json!({ "v": "payload" })), true) {
                Ok(_) => {
                    committed.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => assert_eq!(
                    e.code,
                    topics::types::ErrorCode::Throttled,
                    "over-quota write must be throttled, got {e:?}"
                ),
            }
        }));
    }
    for hd in handles {
        hd.join().expect("no quota writer panicked");
    }

    let total = engine.topic_state("quota", false).unwrap().bytes;
    let admitted = committed.load(Ordering::Relaxed);
    // INVARIANT (the byte quota, codex HIGH #5 / P2 #10): the ATOMIC reserve never
    // admits a write that would push the live total over the cap, so the committed
    // total stays at/under the cap with NO overshoot — even under the full racer
    // race. (The prior read-then-write TOCTOU admitted all 32 and blew past the cap.)
    assert!(
        total <= cap,
        "live total {total} exceeded the quota {cap}; admitted {admitted}/{RACERS} \
         writers — the atomic byte reserve overshot the cap under the race"
    );
    // The cap is an exact multiple of the per-record cost, so a correct atomic
    // reserve admits EXACTLY `cap_records` writes (no more — no overshoot; no fewer
    // — every reservation that fits is granted).
    assert_eq!(
        admitted, cap_records,
        "atomic byte reserve admitted {admitted} writes; expected exactly the cap's \
         capacity of {cap_records} (per_record_bytes={per_record_bytes}, cap={cap})"
    );
    drop(engine);
}

// ===========================================================================
// L-TOTAL-BYTES-QUOTA-CRASH
// ---------------------------------------------------------------------------
// The durable byte quota shares the WAL-first commit-sequencer write path. Drive
// concurrent durable writers against a quota, then a power loss + recovery; every
// acked durable write survives (acked ⇒ durable), the survivor set is a dense
// contiguous prefix (the commit sequencer published in seq order), and the
// recovered total stays within the coarse-guard slack of the cap. This pins that
// the quota admission guard does not corrupt the durable commit ordering and that
// recovery reproduces exactly the acked set.
// ===========================================================================

#[test]
fn l_total_bytes_quota_crash_recovers_acked_dense_prefix() {
    const WRITERS: usize = 6;
    const PER_WRITER: u64 = 20;
    const CAP: u64 = 50_000; // generous: the quota is enabled but rarely the limiter.

    let disk = FakeDisk::new();
    let acked: Arc<Mutex<BTreeSet<u64>>> = Arc::new(Mutex::new(BTreeSet::new()));
    {
        let engine = open_engine_limits(
            &disk,
            Limits {
                max_total_bytes: CAP,
                ..Default::default()
            },
        );
        engine.put_topic("dq", durable_topic()).expect("dq topic");

        let start = Arc::new(Barrier::new(WRITERS));
        let mut handles = Vec::new();
        for w in 0..WRITERS {
            let engine = engine.clone();
            let start = start.clone();
            let acked = acked.clone();
            handles.push(std::thread::spawn(move || {
                start.wait();
                for i in 0..PER_WRITER {
                    let req = write_req(json!({ "v": format!("{w}-{i}") }));
                    // An Ok with the quota enabled means the write was admitted AND
                    // its fsync returned ⇒ acked ⇒ must survive. A Throttled is not
                    // acked (the quota refused it); ignore it.
                    if let Ok(resp) = engine.write("dq", req, true) {
                        acked.lock().unwrap().insert(resp.last_seq);
                    }
                }
            }));
        }
        for hd in handles {
            hd.join().unwrap();
        }

        // Power loss: harden the WAL dir name then freeze BEFORE dropping so the
        // writer's Drop drain hardens nothing un-acked.
        let fs = disk.arc();
        let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("wal"));
        let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("meta"));
        disk.crash(TornDamage::None);
        drop(engine);
    }
    disk.reset_power();

    // Recover and read every survivor through the real diff path.
    let engine = open_engine_limits(
        &disk,
        Limits {
            max_total_bytes: CAP,
            ..Default::default()
        },
    );
    let st = engine.topic_state("dq", false).unwrap();
    let mut survivors = Vec::new();
    let mut from = 0u64;
    loop {
        let d = engine
            .diff(
                "dq",
                DiffRequest {
                    from_seq: from,
                    limit: 1000,
                    node: None,
                    include_tags: false,
                    include_meta: false,
                    wait_ms: 0,
                    max_batch_bytes: 0,
                },
            )
            .unwrap();
        for r in &d.records {
            survivors.push(r.seq);
        }
        if d.caught_up || d.records.is_empty() {
            break;
        }
        from = d.next_from_seq;
    }

    // (1) Dense contiguous prefix [1..=k] — the commit sequencer published in seq
    //     order, so recovery surfaces no gap and no fabricated seq.
    for (i, s) in survivors.iter().enumerate() {
        assert_eq!(
            *s,
            i as u64 + 1,
            "survivors must be a dense prefix (commit sequencer ordering): {survivors:?}"
        );
    }
    // (2) Every acked durable write survives (acked ⇒ durable): acked ⊆ survivors.
    let survivor_set: BTreeSet<u64> = survivors.iter().copied().collect();
    let acked = acked.lock().unwrap();
    for &a in acked.iter() {
        assert!(
            survivor_set.contains(&a),
            "acked durable seq {a} LOST after recovery (survivors={survivors:?})"
        );
    }
    // (3) Recovered head == highest acked seq; no fabrication.
    let max_acked = acked.iter().copied().max().unwrap_or(0);
    assert_eq!(
        st.head_seq, max_acked,
        "recovered head == highest acked seq"
    );
    assert_eq!(
        st.count as usize,
        survivors.len(),
        "count matches the surviving record set"
    );
    // (4) The recovered live total respects the quota within the coarse slack (the
    //     accounting was rebuilt from the WAL, not corrupted by the admission guard).
    assert!(
        st.bytes <= CAP,
        "recovered live total {} exceeded the quota {CAP}",
        st.bytes
    );
    drop(engine);
}

// ===========================================================================
// L-SCOPE-CONSISTENT-RACE  (HTTP harness, auth enabled)
// ---------------------------------------------------------------------------
// Scope enforcement is a per-request, stateless check in the auth middleware
// (route_requirement -> principal.allows_scope). Under concurrency it must stay
// consistent: a WRITE-only key can never read, and a READ-only key can never
// write, regardless of interleaving. We race both key kinds against both verbs on
// a shared topic and assert every outcome matches the key's scope exactly (no
// request ever escalates scope under load, no 5xx).
//
// Key syntax (from integration_auth_scopes): `<secret>:<scopes>` where scopes is a
// '+'-joined set (e.g. "wonly:write", "ronly:read").
// ===========================================================================

#[test]
fn l_scope_consistent_under_concurrency() {
    const RACERS: usize = 16;
    const ITERS: usize = 12;

    let h = Arc::new(Harness::start_with(ServerConfig {
        api_keys: vec![
            "wonly:write".into(),
            "ronly:read".into(),
            "admin:admin".into(),
        ],
        ..Default::default()
    }));
    // Admin creates the shared topic (write/read keys lack admin scope).
    let (s, _) = h.put_auth("/v0/topics/shared", json!({}), "admin");
    assert_eq!(s, StatusCode::CREATED, "admin creates the topic");

    let start = Arc::new(Barrier::new(RACERS * 2));
    let mut handles = Vec::new();

    // Write-only key racers: writes must always succeed (200/201), reads must
    // always be forbidden (403) — never the reverse, under any interleaving.
    for _ in 0..RACERS {
        let h = h.clone();
        let start = start.clone();
        handles.push(std::thread::spawn(move || {
            start.wait();
            for _ in 0..ITERS {
                let (sw, _) = h.post_auth(
                    "/v0/topics/shared",
                    json!({ "records": [{ "data": 1 }] }),
                    "wonly",
                );
                assert!(
                    sw == StatusCode::OK || sw == StatusCode::CREATED,
                    "write-scoped key write must succeed, got {sw}"
                );
                let (sr, _) = h.get_auth("/v0/topics/shared", "wonly");
                assert_eq!(
                    sr,
                    StatusCode::FORBIDDEN,
                    "write-scoped key must NOT be able to read (scope escalation under race)"
                );
            }
        }));
    }
    // Read-only key racers: reads must always succeed (200), writes must always be
    // forbidden (403).
    for _ in 0..RACERS {
        let h = h.clone();
        let start = start.clone();
        handles.push(std::thread::spawn(move || {
            start.wait();
            for _ in 0..ITERS {
                let (sr, _) = h.get_auth("/v0/topics/shared", "ronly");
                assert_eq!(
                    sr,
                    StatusCode::OK,
                    "read-scoped key read must succeed, got {sr}"
                );
                let (sw, _) = h.post_auth(
                    "/v0/topics/shared",
                    json!({ "records": [{ "data": 1 }] }),
                    "ronly",
                );
                assert_eq!(
                    sw,
                    StatusCode::FORBIDDEN,
                    "read-scoped key must NOT be able to write (scope escalation under race)"
                );
            }
        }));
    }
    for hd in handles {
        hd.join()
            .expect("no scope racer panicked / no scope escalation");
    }

    let (s, _) = h.get("/v0/health");
    assert_eq!(s, StatusCode::OK, "server healthy after the scope race");
}
