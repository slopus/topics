//! Queue endpoints (API §10): POST `/v0/topics/:q/claim`, `/ack`, `/nack`,
//! `/extend`, and the SSE GET `/v0/topics/:q/work` auto-claim/push stream.
//!
//! These layer the lease lifecycle on top of the engine's queue API
//! ([`Engine::claim`]/`ack`/`nack`/`extend`/`claim_cohort`). A non-queue topic
//! rejects every one with `409 not_a_queue`; an absent topic is `404`.
//!
//! **Coalescing window.** When the topic's `claim_jitter_ms > 0`, concurrent poll
//! claims (and `/work` refills) arriving within the window are gathered into one
//! cohort by [`ClaimCoordinator`] and served in a single [`Engine::claim_cohort`]
//! pass that divides the available jobs evenly (DESIGN §10.3). `claim_jitter_ms
//! = 0` (default) serves each claim immediately (greedy, lowest latency). The
//! window wait is a *latency* knob (a real timer), never a correctness one — all
//! lease-deadline / expiry decisions use the [`Clock`](crate::clock::Clock).

use super::{parse_json_body, AppState};
use crate::config;
use crate::engine::queue::{Claimer, LeasedJob};
use crate::engine::Engine;
use crate::error::{Error, Result};
use crate::types::*;
use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::{header, HeaderMap},
    response::sse::{Event, KeepAlive, Sse},
    response::{IntoResponse, Json, Response},
};
use dashmap::DashMap;
use futures::stream::Stream;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

/// Wall-time elapsed since `start`, in fractional milliseconds (for the
/// `performance.server_total_ms` block).
fn elapsed_ms(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}

// ===========================================================================
// Coalescing-window cohort coordinator (DESIGN §10.3, API §10.2)
// ===========================================================================

/// One claimer waiting to be served in a coalescing cohort: its claim params
/// plus a oneshot to receive its slice of the batched pass.
struct PendingClaimer {
    claimer: Claimer,
    reply: oneshot::Sender<Vec<LeasedJob>>,
}

/// A forming cohort for one topic: the leader runs the pass after the window, all
/// joiners wait on their oneshot. `closed` is flipped (under the cohort lock)
/// when the leader drains it, so a late joiner racing the leader's drain sees the
/// closed cohort and opens a fresh one instead of pushing into a dead vec.
#[derive(Default)]
struct FormingCohort {
    claimers: Vec<PendingClaimer>,
    closed: bool,
}

/// Per-topic, per-process coordinator for the coalescing window. A leader claim
/// opens a cohort, sleeps the window so concurrent claims join, then runs ONE
/// [`Engine::claim_cohort`] pass and fans the results back.
#[derive(Default)]
pub struct ClaimCoordinator {
    forming: DashMap<String, Arc<Mutex<FormingCohort>>>,
    next_conn: AtomicU64,
}

impl ClaimCoordinator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Allocate a fresh `/work` connection id (release-on-disconnect key).
    pub fn alloc_conn(&self) -> u64 {
        self.next_conn.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Serve a claim through the coalescing window: returns this claimer's leased
    /// jobs. The leader (first claimer to open the cohort for this topic) sleeps
    /// `jitter_ms`, then runs the single batched pass for the whole gathered
    /// cohort under the engine queue lock and replies to every joiner.
    async fn claim_coalesced(
        &self,
        engine: &Arc<Engine>,
        topic_name: &str,
        claimer: Claimer,
        jitter_ms: u64,
        lease_ms: Option<u64>,
    ) -> Result<Vec<LeasedJob>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let pending = PendingClaimer {
            claimer,
            reply: reply_tx,
        };

        // Join an existing forming cohort, or become its leader. Loop so a
        // claimer that races a leader's drain (finds the cohort `closed`) retries
        // into a fresh cohort rather than pushing into a dead vec.
        let (cohort, is_leader) = loop {
            let entry = self
                .forming
                .entry(topic_name.to_string())
                .or_insert_with(|| Arc::new(Mutex::new(FormingCohort::default())));
            let cohort = entry.clone();
            drop(entry); // release the DashMap shard lock before the cohort lock.
            let mut guard = cohort.lock();
            if guard.closed {
                // This Arc was already drained by its leader; the map may still
                // hold it momentarily. Drop our reference and retry: either we
                // get a fresh Arc, or `or_insert_with` replaces the stale one.
                drop(guard);
                self.forming
                    .remove_if(topic_name, |_, v| Arc::ptr_eq(v, &cohort));
                continue;
            }
            let is_leader = guard.claimers.is_empty();
            guard.claimers.push(pending);
            drop(guard);
            break (cohort, is_leader);
        };

        if is_leader {
            // Wait the window so concurrent claimers gather, then run one pass.
            tokio::time::sleep(Duration::from_millis(
                jitter_ms.min(config::MAX_CLAIM_JITTER_MS),
            ))
            .await;

            // Drain under the cohort lock and mark it closed, so a late joiner
            // racing this drain opens a fresh cohort. Remove the (now-closed)
            // entry from the map so subsequent claims start clean.
            let drained: Vec<PendingClaimer> = {
                let mut guard = cohort.lock();
                guard.closed = true;
                std::mem::take(&mut guard.claimers)
            };
            self.forming
                .remove_if(topic_name, |_, v| Arc::ptr_eq(v, &cohort));

            let claimers: Vec<Claimer> = drained.iter().map(|p| p.claimer.clone()).collect();
            let name = topic_name.to_string();
            let engine = engine.clone();
            // The batched pass takes the queue lock; run it on the blocking pool.
            let result =
                super::run_blocking(move || engine.claim_cohort(&name, &claimers, lease_ms)).await;

            match result {
                Ok((mut results, _ready)) => {
                    // Fan results back to every joiner (parallel to `drained`).
                    for (i, p) in drained.into_iter().enumerate() {
                        let jobs = results.get_mut(i).map(std::mem::take).unwrap_or_default();
                        let _ = p.reply.send(jobs);
                    }
                }
                Err(e) => {
                    // Pass failed (e.g. topic deleted mid-window): every joiner gets
                    // an empty result; the leader returns the error to its caller.
                    for p in drained {
                        let _ = p.reply.send(Vec::new());
                    }
                    return Err(e);
                }
            }
        }

        // All claimers (leader included) await their slice.
        Ok(reply_rx.await.unwrap_or_default())
    }
}

/// Run a claim against a topic, honoring the topic's coalescing window. Returns the
/// leased jobs plus the post-pass `ready` count. With `claim_jitter_ms == 0` the
/// greedy single-claimer engine path is used directly (lowest latency).
async fn run_claim(
    state: &AppState,
    topic_name: &str,
    node: String,
    max: u32,
    lease_ms: Option<u64>,
    work_conn: Option<u64>,
) -> Result<(Vec<LeasedJob>, u64)> {
    // Resolve the jitter window from the topic config (and validate it is a queue).
    let b = state
        .engine
        .get_topic(topic_name)
        .ok_or_else(|| Error::topic_not_found(topic_name))?;
    if !b.is_queue() {
        return Err(Error::not_a_queue(topic_name));
    }
    let jitter = b
        .config
        .read()
        .claim_jitter_ms
        .min(config::MAX_CLAIM_JITTER_MS);

    if jitter == 0 {
        // Greedy path: one engine claim. Validate node here (the cohort pass
        // skips per-claim validation), then run a single-claimer cohort pass.
        if node.is_empty() {
            return Err(Error::invalid_request("claim requires a non-empty `node`"));
        }
        if node.len() > config::MAX_NODE_BYTES {
            return Err(Error::invalid_request("node too long"));
        }
        let engine = state.engine.clone();
        let name = topic_name.to_string();
        let claimer = Claimer {
            node,
            max: max.clamp(1, config::MAX_CLAIM),
            work_conn,
        };
        let (mut results, ready) =
            super::run_blocking(move || engine.claim_cohort(&name, &[claimer], lease_ms)).await?;
        let jobs = results.pop().unwrap_or_default();
        return Ok((jobs, ready));
    }

    // Coalescing path: validate node here (the cohort pass skips per-claim
    // validation), then join/lead the window.
    if node.is_empty() {
        return Err(Error::invalid_request("claim requires a non-empty `node`"));
    }
    if node.len() > config::MAX_NODE_BYTES {
        return Err(Error::invalid_request("node too long"));
    }
    let claimer = Claimer {
        node,
        max: max.clamp(1, config::MAX_CLAIM),
        work_conn,
    };
    let jobs = state
        .coordinator
        .claim_coalesced(&state.engine, topic_name, claimer, jitter, lease_ms)
        .await?;
    // Recompute `ready` after the pass for the response.
    let st = state.engine.topic_state(topic_name, false)?;
    let ready = st.queue.map(|q| q.ready).unwrap_or(0);
    Ok((jobs, ready))
}

/// Parse the optional per-seq fencing tokens from a request body's `lease_ids`
/// (R4). The wire form mirrors the claim response: `"lease_<hex>"`. An empty
/// `lease_ids` disables fencing (`Ok(vec![])`, node-only match). An empty
/// string entry is "no token for this seq" (`None`); a non-empty, malformed entry
/// is a `400 invalid_request` (a client sending a token must send a real one).
fn parse_lease_ids(lease_ids: &[String]) -> Result<Vec<Option<u64>>> {
    lease_ids
        .iter()
        .map(|s| {
            if s.is_empty() {
                return Ok(None);
            }
            s.strip_prefix("lease_")
                .and_then(|hex| u64::from_str_radix(hex, 16).ok())
                .map(Some)
                .ok_or_else(|| Error::invalid_request(format!("invalid lease_id token {s:?}")))
        })
        .collect()
}

/// Project an engine [`LeasedJob`] onto the wire [`ClaimedJob`].
fn to_claimed_job(j: LeasedJob) -> ClaimedJob {
    ClaimedJob {
        seq: j.seq,
        lease_id: format!("lease_{:x}", j.lease_id),
        deadline: j.deadline,
        ts: j.ts,
        tag: j.tag,
        deliveries: j.deliveries,
        data: j.data,
        meta: j.meta,
    }
}

// ===========================================================================
// POST handlers: claim / ack / nack / extend
// ===========================================================================

/// `POST /v0/topics/:q/claim` — lease up to `max` claimable jobs to `node`
/// (API §10.2). Honors the topic's coalescing window.
pub async fn claim(
    State(state): State<AppState>,
    Path(topic_name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<ClaimResponse>> {
    let start = Instant::now();
    let req: ClaimRequest = if body.is_empty() {
        return Err(Error::invalid_request("claim requires a `node`"));
    } else {
        parse_json_body(&headers, &body)?
    };

    let (jobs, ready) =
        run_claim(&state, &topic_name, req.node, req.max, req.lease_ms, None).await?;
    let claimed: Vec<ClaimedJob> = jobs.into_iter().map(to_claimed_job).collect();
    let count = claimed.len() as u64;
    Ok(Json(ClaimResponse {
        topic_name,
        claimed,
        count,
        ready,
        performance: Performance::with_total(elapsed_ms(start)),
    }))
}

/// `POST /v0/topics/:q/ack` — complete (delete) jobs held by `node` (API §10.4).
/// A durable queue fsyncs the delete before returning (ack durability == topic
/// `durable`), so run it on the blocking pool.
pub async fn ack(
    State(state): State<AppState>,
    Path(topic_name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<AckResponse>> {
    let req: AckRequest = parse_json_body(&headers, &body)?;
    let lease_ids = parse_lease_ids(&req.lease_ids)?;
    let engine = state.engine.clone();
    let resp = super::run_blocking(move || {
        engine.ack_fenced(&topic_name, &req.node, &req.seqs, &lease_ids)
    })
    .await?;
    Ok(Json(resp))
}

/// `POST /v0/topics/:q/nack` — release leased jobs held by `node` for immediate
/// or `delay_ms`-delayed reclaim (API §10.5).
pub async fn nack(
    State(state): State<AppState>,
    Path(topic_name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<NackResponse>> {
    let req: NackRequest = parse_json_body(&headers, &body)?;
    let lease_ids = parse_lease_ids(&req.lease_ids)?;
    let engine = state.engine.clone();
    let resp = super::run_blocking(move || {
        engine.nack_fenced(&topic_name, &req.node, &req.seqs, req.delay_ms, &lease_ids)
    })
    .await?;
    Ok(Json(resp))
}

/// `POST /v0/topics/:q/extend` — push out the deadline of leases held by `node`
/// (heartbeat; API §10.6).
pub async fn extend(
    State(state): State<AppState>,
    Path(topic_name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<ExtendResponse>> {
    let req: ExtendRequest = parse_json_body(&headers, &body)?;
    let lease_ids = parse_lease_ids(&req.lease_ids)?;
    let engine = state.engine.clone();
    let resp = super::run_blocking(move || {
        engine.extend_fenced(&topic_name, &req.node, &req.seqs, req.lease_ms, &lease_ids)
    })
    .await?;
    Ok(Json(resp))
}

// ===========================================================================
// SSE GET /v0/topics/:q/work — auto-claim / push (PUSH mode, API §10.8)
// ===========================================================================

/// `GET /v0/topics/:q/work?node=X&max=N` — keep up to `max` jobs leased+pushed to
/// this one connection (PUSH mode, API §10.8). Acks come out-of-band via §10.4;
/// the server claims replacements as in-flight depth drops below `max`.
/// On disconnect, all this connection's live leases are released immediately
/// (instant failover); lease expiry still covers hard crashes.
pub async fn work(
    State(state): State<AppState>,
    Path(topic_name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    extensions: axum::http::Extensions,
    headers: HeaderMap,
) -> Result<Response> {
    require_event_stream_accept(&headers)?;

    let node = params
        .get("node")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Error::invalid_request("work requires a non-empty `node`"))?
        .clone();
    if node.len() > config::MAX_NODE_BYTES {
        return Err(Error::invalid_request("node too long"));
    }
    let max = params
        .get("max")
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(1)
        .clamp(1, config::MAX_CLAIM);
    let lease_ms = params.get("lease_ms").and_then(|v| v.parse::<u64>().ok());

    // Validate the topic is a queue before opening the stream (so the error body
    // is readable; API §10.8 establishment errors).
    let b = state
        .engine
        .get_topic(&topic_name)
        .ok_or_else(|| Error::topic_not_found(&topic_name))?;
    if !b.is_queue() {
        return Err(Error::not_a_queue(&topic_name));
    }

    // Resource limit: admit this SSE connection under the global + per-key
    // connection caps (DoS hardening; [`crate::limits`]). The guard is moved into
    // the stream and released on drop, so a disconnect frees the slot. Attribute to
    // the authenticated key (full-access principal / `None` in dev mode). `0` ⇒
    // unlimited.
    let key_id = extensions
        .get::<crate::auth::Principal>()
        .and_then(|p| p.key_id);
    let sse_guard = state
        .live
        .try_acquire_sse(&state.engine.config.limits, key_id)
        .ok_or_else(|| {
            Error::new(ErrorCode::Throttled, "too many concurrent SSE connections")
                .with_retry_after(crate::limits::LIMIT_RETRY_AFTER_S)
        })?;

    let conn = state.coordinator.alloc_conn();
    let stream = build_work_stream(
        state.clone(),
        topic_name,
        node,
        max,
        lease_ms,
        conn,
        sse_guard,
    );

    let sse = Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text(": hb"),
    );
    let mut resp = sse.into_response();
    let h = resp.headers_mut();
    h.insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    h.insert("x-accel-buffering", "no".parse().unwrap());
    Ok(resp)
}

/// On-disconnect guard: releases this `/work` connection's live leases the moment
/// the stream future is dropped (clean close or broken pipe), recording
/// `released` events so the jobs are instantly claimable again (API §10.8).
struct WorkConnGuard {
    state: AppState,
    topic_name: String,
    conn: u64,
}

impl Drop for WorkConnGuard {
    fn drop(&mut self) {
        self.state
            .engine
            .release_work_conn(&self.topic_name, self.conn);
    }
}

/// Build the `/work` SSE stream: claim up to `max` jobs onto this connection,
/// push each as an `event: job` frame, then park on the topic `Notify` and refill
/// as in-flight depth drops (acks land out-of-band via §10.4). The
/// [`WorkConnGuard`] releases the connection's leases on disconnect.
fn build_work_stream(
    state: AppState,
    topic_name: String,
    node: String,
    max: u32,
    lease_ms: Option<u64>,
    conn: u64,
    sse_guard: crate::limits::SseGuard,
) -> impl Stream<Item = std::result::Result<Event, Infallible>> {
    let guard = WorkConnGuard {
        state: state.clone(),
        topic_name: topic_name.clone(),
        conn,
    };
    async_stream::stream! {
        // Move the guard into the stream body so disconnect (drop of this future)
        // triggers release-on-disconnect.
        let _guard = guard;
        // Hold the SSE-connection slot for the stream's lifetime; released on drop
        // (DoS hardening; [`crate::limits`]).
        let _sse_guard = sse_guard;

        // `retry:` once at open (deliberate 2 s backoff; API §7.5/§10.8).
        yield Ok(Event::default().retry(Duration::from_millis(config::SSE_RETRY_MS)));

        // In-flight depth tracked here (acks/nacks happen out-of-band, so we
        // reconcile against the engine's lease projection each pass).
        loop {
            // Graceful shutdown (M11): wind down + close so the bounded drain
            // completes. The connection's leases are released by `WorkConnGuard` on
            // drop, so the jobs are immediately re-claimable on the next instance.
            if state.shutdown.is_shutting_down() {
                let data = serde_json::json!({
                    "code": "server_shutting_down",
                    "error": "server is shutting down; reconnect",
                    "topic": topic_name
                });
                yield Ok(Event::default().event("error").data(data.to_string()));
                break;
            }

            // How many leases does this connection currently hold? Refill up to
            // `max`. We read it from a fresh claim attempt: claim the deficit.
            let in_flight = state.engine.work_conn_in_flight(&topic_name, conn);
            let deficit = max.saturating_sub(in_flight);

            if deficit > 0 {
                let jobs = match run_claim(
                    &state,
                    &topic_name,
                    node.clone(),
                    deficit,
                    lease_ms,
                    Some(conn),
                )
                .await
                {
                    Ok((jobs, _ready)) => jobs,
                    Err(_) => {
                        // Topic ceased to be a queue / was deleted: terminal error.
                        let data = serde_json::json!({
                            "code": 409, "error": "topic is no longer a queue",
                            "topic": topic_name
                        });
                        yield Ok(Event::default().event("error").data(data.to_string()));
                        break;
                    }
                };

                for j in jobs {
                    let frame = job_frame(&topic_name, &j);
                    yield Ok(Event::default()
                        .id(j.seq.to_string())
                        .event("job")
                        .data(frame.to_string()));
                }
            }

            // Park until the queue changes (an append makes more claimable, or an
            // out-of-band ack frees an in-flight slot) or the heartbeat window
            // elapses, then re-check. `Notify` wakeups avoid busy polling.
            let Some(b) = state.engine.get_topic(&topic_name) else {
                break; // topic gone.
            };
            let notified = b.notify.notified();
            tokio::select! {
                _ = state.shutdown.notified() => {}
                _ = notified => {}
                _ = tokio::time::sleep(Duration::from_millis(config::WORK_POLL_MS)) => {}
            }
        }
    }
}

/// Project a leased job onto the `/work` `event: job` frame JSON (API §10.8).
fn job_frame(topic_name: &str, j: &LeasedJob) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    obj.insert("topic".into(), serde_json::json!(topic_name));
    obj.insert("$seq".into(), serde_json::json!(j.seq));
    obj.insert(
        "lease_id".into(),
        serde_json::json!(format!("lease_{:x}", j.lease_id)),
    );
    obj.insert("deadline".into(), serde_json::json!(j.deadline));
    obj.insert("$ts".into(), serde_json::json!(j.ts));
    if let Some(tag) = &j.tag {
        obj.insert("$tag".into(), serde_json::json!(tag));
    }
    obj.insert("deliveries".into(), serde_json::json!(j.deliveries));
    obj.insert("data".into(), j.data.clone());
    if let Some(meta) = &j.meta {
        obj.insert("meta".into(), meta.clone());
    }
    serde_json::Value::Object(obj)
}

/// Reject a `/work` GET whose `Accept` is not `text/event-stream`
/// (`406 not_acceptable`, API §10.8). Mirrors the watch-stream guard.
fn require_event_stream_accept(headers: &HeaderMap) -> Result<()> {
    let accept = headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if accept.is_empty() || accept.contains("text/event-stream") || accept.contains("*/*") {
        Ok(())
    } else {
        Err(Error::new(
            ErrorCode::NotAcceptable,
            "Accept must be text/event-stream",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn work_accept_guard_rejects_non_sse() {
        let mut h = HeaderMap::new();
        h.insert(header::ACCEPT, "application/json".parse().unwrap());
        assert_eq!(
            require_event_stream_accept(&h).unwrap_err().code,
            ErrorCode::NotAcceptable
        );
        h.insert(header::ACCEPT, "text/event-stream".parse().unwrap());
        assert!(require_event_stream_accept(&h).is_ok());
    }

    #[test]
    fn job_frame_shape_matches_api() {
        let j = LeasedJob {
            seq: 480101,
            lease_id: 0x7f3a9c,
            deadline: 1_748_450_039_000,
            ts: 1_748_450_001_000,
            tag: Some("tenant42:job-8800".into()),
            deliveries: 1,
            data: serde_json::json!({ "type": "resize" }),
            meta: None,
        };
        let f = job_frame("jobs", &j);
        assert_eq!(f["topic"], serde_json::json!("jobs"));
        assert_eq!(f["$seq"], serde_json::json!(480101));
        assert_eq!(f["lease_id"], serde_json::json!("lease_7f3a9c"));
        assert_eq!(f["$tag"], serde_json::json!("tenant42:job-8800"));
        assert_eq!(f["deliveries"], serde_json::json!(1));
        assert!(f.get("meta").is_none(), "meta omitted when absent");
    }
}
