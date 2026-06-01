//! Multiplexed SSE watch: POST `/v0/watch` (create session) and
//! GET `/v0/watch/:wid` (open the SSE stream).
//!
//! Frame types (API §7.5): `record`, `tombstone`, `caught-up`, `box-deleted`,
//! `error`; data-bearing frames carry a composite base64url `id:` (the per-box
//! `box → seq` cursor map), heartbeats are bare `:` comments, and `retry:` is
//! sent once at open. Resume via `Last-Event-ID`.

use super::AppState;
use crate::config;
use crate::engine::broadcast::FrameVariant;
use crate::engine::Engine;
use crate::error::{Error, Result};
use crate::types::*;
use axum::{
    extract::{Path, Query, State},
    http::{header, HeaderMap},
    response::sse::{Event, KeepAlive, Sse},
    response::{IntoResponse, Json, Response},
};
use base64::Engine as _;
use dashmap::DashMap;
use futures::stream::Stream;
use parking_lot::Mutex;
use serde::Serialize;
use serde_json::value::RawValue;
use std::collections::{BTreeMap, HashMap};
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

/// The `record` SSE frame envelope. Fields are declared in **sorted key order**
/// (`box`,`from_seq`,`head_seq`,`records`,`to_seq`) so `serde_json` emits bytes
/// byte-identical to the prior `serde_json::json!` map (which sorts keys), while
/// `records` embeds the shared, pre-serialized [`RawValue`] frames verbatim
/// (zero re-serialization of the record bodies).
#[derive(Serialize)]
struct RecordEnvelope<'a> {
    #[serde(rename = "box")]
    box_name: &'a str,
    from_seq: u64,
    head_seq: u64,
    #[serde(serialize_with = "serialize_shared_frames")]
    records: Vec<Arc<RawValue>>,
    to_seq: u64,
}

/// Serialize a slice of shared `Arc<RawValue>` frames as a JSON array, embedding
/// each pre-serialized frame verbatim (`serde_json` recognizes `&RawValue` and
/// copies its bytes without re-parsing). Dereferencing `Arc` to `&RawValue` side-
/// steps the missing `Serialize for Arc<RawValue>` bound.
fn serialize_shared_frames<S>(
    frames: &[Arc<RawValue>],
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    use serde::ser::SerializeSeq;
    let mut seq = serializer.serialize_seq(Some(frames.len()))?;
    for f in frames {
        seq.serialize_element(f.as_ref())?;
    }
    seq.end()
}

/// A stored watch session: the immutable subscription definition plus the
/// authoritative, mutable per-box cursor map (so a GET reconnect resumes
/// exactly; API §7.1/§7.4).
pub struct Session {
    pub req: WatchCreateRequest,
    /// Authoritative `box → last-delivered seq` cursor map.
    pub cursors: Mutex<BTreeMap<String, u64>>,
    /// The id of the key that created this session, when auth is enabled (a
    /// non-secret SHA-256 digest, never the plaintext). `None` in dev mode. The
    /// GET stream is authorized by *possessing* the unguessable `wid` (a bearer
    /// capability); a presented bearer, if any, must also resolve to this same
    /// key (defense in depth). See [`Session::authorize`].
    pub key_id: Option<crate::auth::KeyId>,
    /// The creating key's effective scope set, captured at creation so the SSE
    /// stream can never exceed the creator's scope even if that key is later
    /// re-scoped or the wid leaks. A session bound to a key without [`Scope::READ`]
    /// could not have been created (POST /v0/watch requires read), so in practice
    /// this always contains READ; it is retained for completeness / future frames.
    pub scopes: crate::auth::Scope,
    /// Monotonic-ish wall-clock ms of the last create/GET access, for idle-session
    /// GC (codex MEDIUM #11): a session with no active stream and a last access
    /// older than [`config::SESSION_TTL_MS`] is reclaimed so a client cannot fill
    /// `max_watch_sessions` and hold the slots until restart.
    pub last_access_ms: std::sync::atomic::AtomicI64,
    /// Count of currently-open SSE streams for this session. A session with an
    /// active stream is never GC'd (the cursor map is in use); the count is bumped
    /// on stream open and decremented on close via [`StreamHandle`].
    pub active_streams: std::sync::atomic::AtomicU64,
}

impl Session {
    /// Stamp the last-access time (called on create + each GET-stream open) so the
    /// idle-GC TTL is measured from the most recent use.
    fn touch(&self, now_ms: i64) {
        self.last_access_ms
            .store(now_ms, std::sync::atomic::Ordering::Relaxed);
    }

    /// Whether this session is reclaimable at `now_ms`: no open stream AND idle
    /// past the TTL.
    fn is_expired(&self, now_ms: i64) -> bool {
        use std::sync::atomic::Ordering;
        self.active_streams.load(Ordering::Relaxed) == 0
            && now_ms.saturating_sub(self.last_access_ms.load(Ordering::Relaxed))
                > config::SESSION_TTL_MS as i64
    }
}

impl Session {
    /// Authorize a GET-stream request against this session's key binding.
    ///
    /// When the session is bound to a key (auth enabled), the caller MUST present
    /// a bearer (header, or the dev-only `?token=` fallback) that resolves — via
    /// the constant-time hashed [`KeyStore`](crate::auth::KeyStore) — to the
    /// *same* key that created the session. Holding the `wid` is necessary (it
    /// names the session) but NOT sufficient: a leaked high-scope `wid` opened by
    /// anyone, or replayed under a *different valid* key, is rejected (codex HIGH
    /// #3). An unbound session (dev mode, `key_id == None`) is always allowed —
    /// there is no key to bind to.
    pub fn authorize(&self, presented_bearer: Option<&str>, keys: &crate::auth::KeyStore) -> bool {
        match (&self.key_id, presented_bearer) {
            // Unbound (dev mode): the wid alone authorizes.
            (None, _) => true,
            // Bound, bearer presented: it must authenticate AND be the same key.
            (Some(bound), Some(b)) => match keys.authenticate(b) {
                Some(p) => p.key_id == Some(*bound),
                None => false,
            },
            // Bound, NO bearer presented: the wid is a capability *name*, not a
            // credential — reject so a leaked wid cannot be opened without the key.
            (Some(_), None) => false,
        }
    }
}

/// In-memory registry of watch sessions, keyed by `wid`. Phase 2 keeps them in
/// a `DashMap`; phase 4 may persist. GC of idle sessions is best-effort.
pub struct SessionStore {
    sessions: DashMap<String, Arc<Session>>,
    /// Live session count, maintained as an atomic gauge so the `max_watch_sessions`
    /// cap can be enforced with an **atomic reserve-then-insert** (codex P2 #10): the
    /// reserve CAS happens-before the registry insert, so a concurrent
    /// `POST /v0/watch` race can never push the live session count over the cap (the
    /// prior `len()`-read-then-insert was a TOCTOU). Kept in lockstep with
    /// `sessions`: bumped on insert, decremented on GC removal.
    count: std::sync::atomic::AtomicU64,
}

impl Default for SessionStore {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionStore {
    pub fn new() -> Self {
        SessionStore {
            sessions: DashMap::new(),
            count: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Mint an UNGUESSABLE watch capability: `wid_` + base64url of 16 random
    /// bytes (128 bits) from the OS CSPRNG. The `wid_` prefix keeps the documented
    /// shape and the path charset; the random suffix makes the `wid` a true bearer
    /// capability that cannot be enumerated (the old monotonic `wid_{n:010x}` was
    /// trivially guessable). Collisions are cryptographically negligible.
    fn alloc_wid() -> String {
        let mut bytes = [0u8; 16];
        rand::fill(&mut bytes);
        let suffix = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
        format!("wid_{suffix}")
    }

    /// Atomically reserve a session slot against `cap` (`0` ⇒ unlimited) and, if
    /// admitted, insert `session` under a fresh `wid` (codex P2 #10). Returns the new
    /// `wid`, or `None` when the live count is already at the cap (the caller returns
    /// `429 throttled`). The reserve CAS happens-before the insert and is the
    /// serialization point for the cap, so a concurrent create race can never push
    /// the live session count over `cap`.
    fn try_insert_capped(&self, session: Session, cap: u64) -> Option<String> {
        use std::sync::atomic::Ordering;
        if cap != 0 {
            let mut cur = self.count.load(Ordering::Relaxed);
            loop {
                if cur >= cap {
                    return None;
                }
                match self.count.compare_exchange_weak(
                    cur,
                    cur + 1,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(c) => cur = c,
                }
            }
        } else {
            self.count.fetch_add(1, Ordering::AcqRel);
        }
        let wid = Self::alloc_wid();
        self.sessions.insert(wid.clone(), Arc::new(session));
        Some(wid)
    }

    fn get(&self, wid: &str) -> Option<Arc<Session>> {
        self.sessions.get(wid).map(|s| s.clone())
    }

    /// Reclaim idle, expired sessions (codex MEDIUM #11): any session with no open
    /// stream whose last access is older than [`config::SESSION_TTL_MS`] at `now_ms`
    /// is removed. Called opportunistically on session create and on stream open,
    /// so a client that abandons sessions cannot pin `max_watch_sessions` slots
    /// until restart. `DashMap::retain` holds shard locks only briefly. Each
    /// reaped session releases its slot from the `count` gauge so the cap frees up.
    fn gc_expired(&self, now_ms: i64) {
        use std::sync::atomic::Ordering;
        self.sessions.retain(|_wid, s| {
            let keep = !s.is_expired(now_ms);
            if !keep {
                self.count.fetch_sub(1, Ordering::AcqRel);
            }
            keep
        });
    }

    /// Mark a stream as open on an ALREADY-FETCHED session `s` (its `wid`), bumping
    /// `active_streams` so a concurrent idle-GC cannot reap it (codex MEDIUM #11),
    /// and returning an RAII [`StreamHandle`] that decrements the count and re-stamps
    /// the last-access time on drop (clean close / broken pipe). The bump is applied
    /// to the caller's held `Arc`, so it takes effect even though the registry entry
    /// is looked up by `wid` on drop — closing the race where GC ran between the
    /// fetch and the bump. Always returns a handle (the session is the caller's, so
    /// the stream is always tracked).
    fn open_on(self: &Arc<Self>, wid: &str, s: &Arc<Session>, now_ms: i64) -> StreamHandle {
        s.active_streams
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        s.touch(now_ms);
        StreamHandle {
            store: self.clone(),
            wid: wid.to_string(),
            session: s.clone(),
        }
    }

    /// Number of live sessions in the registry (resource-limit check;
    /// [`crate::limits`]). Reads the atomic gauge kept in lockstep with the registry
    /// (the reservation point for `max_watch_sessions`).
    pub fn len(&self) -> usize {
        self.count.load(std::sync::atomic::Ordering::Relaxed) as usize
    }

    /// Whether the registry holds no sessions.
    pub fn is_empty(&self) -> bool {
        self.sessions.is_empty()
    }
}

/// RAII handle for an open SSE stream on a session: decrements the session's
/// active-stream count on drop and re-stamps its last-access time so the idle-GC
/// TTL is measured from when the stream *ended* (codex MEDIUM #11). Held inside the
/// stream future; a broken pipe / cancel frees it just like a clean close. The
/// handle holds its own `Arc<Session>` so the count decrement always lands on the
/// exact session it was opened on (even if the registry entry was meanwhile GC'd /
/// re-minted under the same `wid`), closing the GC-vs-open race.
pub struct StreamHandle {
    store: Arc<SessionStore>,
    wid: String,
    session: Arc<Session>,
}

impl Drop for StreamHandle {
    fn drop(&mut self) {
        use std::sync::atomic::Ordering;
        // Decrement on the held Arc — authoritative regardless of the registry's
        // current state — and re-stamp so the idle-TTL clock restarts when the
        // stream ENDED, not when it opened. The session can then be reaped by a
        // later `gc_expired` once it is genuinely idle (active_streams back to 0).
        self.session.active_streams.fetch_sub(1, Ordering::Relaxed);
        self.session.last_access_ms.store(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0),
            Ordering::Relaxed,
        );
        // Keep `store`/`wid` referenced (the handle's identity); no registry mutation
        // is needed on drop.
        let _ = (&self.store, &self.wid);
    }
}

/// `POST /v0/watch` — create a watch session; returns a `wid` + `stream_url`.
///
/// Validates the `boxes` map (size, names) and resolves each box's initial
/// `from_seq`/`tail` against current watermarks, returning per-box
/// head/earliest so the client can see fall-off before streaming. `?lenient=true`
/// skips unknown boxes instead of `404`.
pub async fn create_watch(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
    extensions: axum::http::Extensions,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Json<WatchCreateResponse>> {
    let mut req: WatchCreateRequest = super::parse_json_body(&headers, &body)?;

    if req.boxes.is_empty() {
        return Err(Error::invalid_request("watch must name >=1 box"));
    }
    if req.boxes.len() > config::MAX_WATCH_BOXES {
        return Err(Error::invalid_request(format!(
            "watch names {} boxes, exceeds max {}",
            req.boxes.len(),
            config::MAX_WATCH_BOXES
        )));
    }

    // Authorization: a prefix-restricted key may only watch boxes within its
    // allowlist. The route-level scope check (`POST /v0/watch` needs read) does
    // NOT see the body's box names — they arrive here — so the prefix allowlist
    // must be enforced against every requested box BEFORE resolving/storing the
    // session, or a prefix-limited read key could subscribe to any box (codex
    // HIGH #1). A full-access / unrestricted key (empty allowlist) passes
    // transparently. The dev-mode principal is full-access.
    let principal = extensions.get::<crate::auth::Principal>();
    if let Some(p) = principal {
        for name in req.boxes.keys() {
            if !p.allows_name(name) {
                return Err(Error::new(
                    ErrorCode::Forbidden,
                    "api key is not allowed to watch this box",
                )
                .with_detail(serde_json::json!({ "box": name })));
            }
        }
    }

    // Reclaim idle, expired sessions before the cap reservation (codex MEDIUM #11)
    // so a freed slot is reusable and a client that abandons sessions cannot pin the
    // `max_watch_sessions` slots until restart.
    let now_ms = state.engine.clock.now_ms();
    state.sessions.gc_expired(now_ms);

    // Clamp heartbeat into the documented bounds (API §7.2). The lower bound is
    // `MIN_HEARTBEAT_MS` (1000ms) in production; the test suite can lower it via
    // `STREAMS_TEST_MIN_HEARTBEAT_MS` so the SSE cadence test asserts the
    // keep-alive without a multi-second wall-clock wait (config::min_heartbeat_ms).
    req.heartbeat_ms = req
        .heartbeat_ms
        .clamp(config::min_heartbeat_ms(), config::MAX_HEARTBEAT_MS);

    let lenient = super::query_bool(&params, "lenient", false);
    let states = state.engine.watch_box_states(&req.boxes, lenient)?;

    // Seed the authoritative cursor map from the resolved per-box `from_seq`.
    let mut cursors = BTreeMap::new();
    for (name, st) in &states {
        cursors.insert(name.clone(), st.from_seq);
    }

    // Bind the session to the authenticated creator (when auth is enabled) so the
    // capability `wid` cannot be replayed under a different key, and capture the
    // creator's scope so the stream can never exceed it. The `Principal` is
    // stashed by the auth middleware (full-access in dev mode).
    let key_id = principal.and_then(|p| p.key_id);
    let scopes = principal
        .map(|p| p.scopes)
        .unwrap_or(crate::auth::Scope::ALL);

    // Resource limit: cap the number of live watch sessions (DoS hardening;
    // [`crate::limits`]). `0` ⇒ unlimited. The reservation is ATOMIC with the insert
    // (codex P2 #10) — a concurrent `POST /v0/watch` race can never push the live
    // session count over the cap. Capacity exhaustion is a transient `429 throttled`.
    let limits = &state.engine.config.limits;
    let wid = state
        .sessions
        .try_insert_capped(
            Session {
                req: req.clone(),
                cursors: Mutex::new(cursors),
                key_id,
                scopes,
                last_access_ms: std::sync::atomic::AtomicI64::new(now_ms),
                active_streams: std::sync::atomic::AtomicU64::new(0),
            },
            limits.max_watch_sessions,
        )
        .ok_or_else(|| {
            Error::new(ErrorCode::Throttled, "watch session limit reached")
                .with_retry_after(crate::limits::LIMIT_RETRY_AFTER_S)
                .with_detail(serde_json::json!({
                    "limit": "max_watch_sessions",
                    "max": limits.max_watch_sessions,
                }))
        })?;

    Ok(Json(WatchCreateResponse {
        stream_url: format!("/v0/watch/{wid}"),
        wid,
        session_ttl_ms: config::SESSION_TTL_MS,
        boxes: states,
        performance: Performance::default(),
    }))
}

/// `GET /v0/watch/:wid` — open the SSE stream for a session.
///
/// Validates `Accept: text/event-stream` (else `406`), resolves the session and
/// any `Last-Event-ID` rewind, then streams named events with low-latency
/// headers (`X-Accel-Buffering: no`, `Cache-Control: no-store`).
pub async fn stream_watch(
    State(state): State<AppState>,
    Path(wid): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<Response> {
    require_event_stream_accept(&headers)?;

    let session = state
        .sessions
        .get(&wid)
        .ok_or_else(|| Error::new(ErrorCode::NotFound, "watch session not found (re-POST)"))?;

    // Authorize: holding the unguessable `wid` is the capability. When the session
    // is bound to a principal (auth enabled), a *presented* bearer (header, or the
    // dev-only `?token=` fallback) must match the creating principal; presenting
    // none is allowed (the wid alone authorizes, the `EventSource` case).
    let presented_bearer = bearer_from_request(&headers, &params);
    if !session.authorize(presented_bearer.as_deref(), &state.keys) {
        return Err(Error::new(
            ErrorCode::Unauthorized,
            "watch token does not match the session's principal",
        ));
    }

    // Resource limit: admit this SSE connection under the global + per-key
    // connection caps (DoS hardening; [`crate::limits`]). The returned guard is
    // moved into the stream and released on drop (clean close / broken pipe), so a
    // dropped connection frees its slot. Attribute the connection to the session's
    // creating key (constant across reconnects), so a session a key created counts
    // against that key's cap. `0` ⇒ unlimited.
    let sse_guard = match state
        .live
        .try_acquire_sse(&state.engine.config.limits, session.key_id)
    {
        Some(g) => g,
        None => {
            return Err(
                Error::new(ErrorCode::Throttled, "too many concurrent SSE connections")
                    .with_retry_after(crate::limits::LIMIT_RETRY_AFTER_S),
            );
        }
    };

    // Mark the session as having an open stream + re-stamp its last-access time
    // (codex MEDIUM #11). The returned handle is moved into the stream future and
    // decrements the count / restarts the idle-TTL clock on drop, so a session is
    // never GC'd while a stream is live and its TTL resumes when the stream ends.
    //
    // GC-vs-open race (codex MEDIUM #11): we bump `active_streams` on the
    // ALREADY-FETCHED `Arc<Session>` BEFORE running the opportunistic GC, so the
    // session this open is about to stream can never be reaped out from under it
    // (`is_expired` requires `active_streams == 0`). `open_on` then attaches the RAII
    // handle to the registry entry (still present, since GC could not remove it),
    // restamps, and decrements on drop. The prior order (GC then open) could free the
    // session between the fetch and the count bump, leaving a stream live with the
    // registry/count gauge already decremented (a use-after-GC of the slot).
    let now_ms = state.engine.clock.now_ms();
    let stream_handle = state.sessions.open_on(&wid, &session, now_ms);
    state.sessions.gc_expired(now_ms);

    // `Last-Event-ID` (or the `cursor` query) may rewind the session cursors to
    // an exact prior map — never advance past the authoritative server state.
    if let Some(leid) = headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
    {
        if let Some(map) = decode_cursor_id(leid) {
            let mut cursors = session.cursors.lock();
            for (b, seq) in map {
                if let Some(cur) = cursors.get_mut(&b) {
                    // Rewind only: take the lower of stored vs resumed.
                    *cur = (*cur).min(seq);
                }
            }
        }
    }

    let engine = state.engine.clone();
    let shutdown = state.shutdown.clone();
    // Capture the keep-alive cadence before `session` is moved into the stream.
    let heartbeat_ms = session.req.heartbeat_ms;
    let stream = build_stream(engine, session, sse_guard, Some(stream_handle), shutdown);

    // Drive the axum keep-alive `: hb` cadence from the session's (already
    // clamped) `heartbeat_ms` rather than a hardcoded 15s, so a client that
    // requests a faster heartbeat — and the SSE cadence test in particular —
    // observes the comment on its chosen interval. Production sessions default to
    // 15_000ms and are floored at 1000ms, so the wire behavior is unchanged.
    let sse = Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_millis(heartbeat_ms))
            .text(": hb"),
    );

    // Low-latency headers (API §7.3).
    let mut resp = sse.into_response();
    let h = resp.headers_mut();
    h.insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    h.insert("x-accel-buffering", "no".parse().unwrap());
    Ok(resp)
}

/// Build the SSE event stream for a resolved session. Reuses the engine's diff
/// primitive per box (TTL + deleted skip + node filter + tombstone), emits
/// `record`/`tombstone`/`caught-up`/`box-deleted` frames with composite `id:`
/// cursors, and parks on each box's `Notify` between flushes (no busy poll).
fn build_stream(
    engine: Arc<Engine>,
    session: Arc<Session>,
    sse_guard: crate::limits::SseGuard,
    stream_handle: Option<StreamHandle>,
    shutdown: Arc<crate::serve::ShutdownSignal>,
) -> impl Stream<Item = std::result::Result<Event, Infallible>> {
    let heartbeat_ms = session.req.heartbeat_ms;
    // The projection variant for this session's record frames (drives which
    // shared broadcast-cache slot every record hits).
    let variant = FrameVariant::new(
        session.req.include_data,
        session.req.include_tags,
        session.req.include_meta,
    );
    async_stream::stream! {
        // Hold the SSE-connection slot guard for the stream's whole lifetime;
        // dropping it (clean close / broken pipe / cancel) releases the global +
        // per-key connection slot (DoS hardening; [`crate::limits`]).
        let _sse_guard = sse_guard;
        // Hold the session stream handle: keeps the session out of idle-GC while
        // streaming and restarts its TTL clock on close (codex MEDIUM #11).
        let _stream_handle = stream_handle;

        // `retry:` once at open (deliberate 2 s backoff; API §7.5).
        yield Ok(Event::default().retry(Duration::from_millis(config::SSE_RETRY_MS)));

        // Track which boxes we've already reported as deleted (terminal per box)
        // and whether each box was last seen caught-up (to re-emit on the
        // backlog→tailing transition only).
        let box_names: Vec<String> =
            session.cursors.lock().keys().cloned().collect();
        let mut deleted: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        let mut was_caught_up: HashMap<String, bool> = HashMap::new();
        // Boxes not yet read once: a tombstone on the *first* read is the
        // connect-time "offset out of range" case (`from_seq_too_old`; API §7.5),
        // distinct from a gap that crosses the cursor while live.
        let mut first_read: std::collections::HashSet<String> =
            box_names.iter().cloned().collect();

        loop {
            // Graceful shutdown (M11): if the server is winding down, emit a
            // terminal `error` frame (the client will reconnect after the `retry:`
            // backoff to whatever instance is up) and end the stream so the bounded
            // drain completes promptly instead of waiting on this tailing stream.
            if shutdown.is_shutting_down() {
                let id = encode_session_id(&session);
                let data = serde_json::json!({
                    "code": "server_shutting_down",
                    "message": "server is shutting down; reconnect",
                });
                yield Ok(Event::default().id(id).event("error").data(data.to_string()));
                break;
            }

            // Hold the live box `Arc`s for this pass so the `Notified` futures
            // we build at the end (which borrow each box's `Notify`) outlive the
            // per-box loop body.
            let mut live: Vec<Arc<crate::engine::box_state::BoxState>> = Vec::new();

            for name in &box_names {
                if deleted.contains(name) {
                    continue;
                }
                let Some(b) = engine.get_box(name) else {
                    // Box vanished mid-watch ⇒ terminal box-deleted frame.
                    let head = 0;
                    deleted.insert(name.clone());
                    let id = encode_session_id(&session);
                    let data = serde_json::json!({
                        "box": name, "head_seq": head, "reason": "deleted"
                    });
                    yield Ok(Event::default()
                        .id(id)
                        .event("box-deleted")
                        .data(data.to_string()));
                    continue;
                };
                live.push(b.clone());

                // Drain this box up to head in `limit`-sized batches.
                loop {
                    let from_seq = session
                        .cursors
                        .lock()
                        .get(name)
                        .copied()
                        .unwrap_or(0);
                    let req = DiffRequest {
                        from_seq,
                        limit: session.req.limit,
                        node: session.req.node.clone(),
                        include_tags: session.req.include_tags,
                        include_meta: session.req.include_meta,
                        wait_ms: 0,
                        // Bound each SSE record frame by the session's byte budget
                        // (codex HIGH #6); the diff loop stops at this many payload
                        // bytes so one frame cannot balloon to limit×record-cap.
                        max_batch_bytes: session.req.max_batch_bytes,
                    };
                    let Ok(d) = engine.diff(name, req) else {
                        // Diff only fails with box_not_found here.
                        deleted.insert(name.clone());
                        let id = encode_session_id(&session);
                        let data = serde_json::json!({
                            "box": name, "head_seq": 0, "reason": "deleted"
                        });
                        yield Ok(Event::default()
                            .id(id)
                            .event("box-deleted")
                            .data(data.to_string()));
                        break;
                    };

                    // A tombstone crossed this consumer's cursor: emit it first,
                    // its `id` already advances the box cursor to `gap_to`.
                    if let Some(tomb) = &d.tombstone {
                        session
                            .cursors
                            .lock()
                            .insert(name.clone(), tomb.gap_to);
                        // On the first read of a box, a below-floor cursor is the
                        // connect-time `from_seq_too_old` variant (API §7.5);
                        // afterward, report the engine's cap/ttl/mixed reason.
                        let reason = if first_read.contains(name) {
                            TombstoneReason::FromSeqTooOld
                        } else {
                            tomb.reason
                        };
                        let id = encode_session_id(&session);
                        let data = serde_json::json!({
                            "box": name,
                            "reason": reason,
                            "gap_from": tomb.gap_from,
                            "gap_to": tomb.gap_to,
                            "earliest_seq": tomb.earliest_seq,
                            "head_seq": tomb.head_seq,
                        });
                        yield Ok(Event::default()
                            .id(id)
                            .event("tombstone")
                            .data(data.to_string()));
                        was_caught_up.insert(name.clone(), false);
                    }
                    first_read.remove(name);

                    // Advance the authoritative cursor past everything examined.
                    let to_seq = d.next_from_seq;
                    if !d.records.is_empty() {
                        // Zero-copy broadcast: each record frame is serialized
                        // ONCE per box and shared (ref-counted `Arc<RawValue>`)
                        // across all watchers via the box's broadcast cache,
                        // instead of re-serializing per connection. The envelope
                        // (`box`/`from_seq`/`to_seq`/`head_seq`) and the composite
                        // `id:` cursor are still per-connection (they depend on
                        // this session's cursor map). The struct's field order is
                        // sorted to stay byte-identical to the old `json!` map.
                        let records: Vec<Arc<RawValue>> = d
                            .records
                            .iter()
                            .map(|r| b.broadcast.frame(r.seq, r, variant))
                            .collect();
                        session.cursors.lock().insert(name.clone(), to_seq);
                        let id = encode_session_id(&session);
                        let payload = RecordEnvelope {
                            box_name: name.as_str(),
                            from_seq,
                            head_seq: d.head_seq,
                            records,
                            to_seq,
                        };
                        let body = serde_json::to_string(&payload)
                            .unwrap_or_else(|_| "{}".to_string());
                        yield Ok(Event::default()
                            .id(id)
                            .event("record")
                            .data(body));
                        was_caught_up.insert(name.clone(), false);
                    } else if d.tombstone.is_none() {
                        // No records and no tombstone, but the cursor may still
                        // have advanced past filtered records; persist it.
                        session.cursors.lock().insert(name.clone(), to_seq);
                    }

                    if d.caught_up {
                        // Emit `caught-up` once per backlog→tailing transition.
                        if !was_caught_up.get(name).copied().unwrap_or(false) {
                            let id = encode_session_id(&session);
                            let data = serde_json::json!({
                                "box": name, "head_seq": d.head_seq
                            });
                            yield Ok(Event::default()
                                .id(id)
                                .event("caught-up")
                                .data(data.to_string()));
                            was_caught_up.insert(name.clone(), true);
                        }
                        break;
                    }
                }
            }

            // If every box is terminal (deleted), end the stream.
            if box_names.iter().all(|n| deleted.contains(n)) {
                break;
            }

            // Drained pass: park until any watched box appends or the heartbeat
            // window elapses, then re-check. Tokio `Notify` wakeups give the
            // ~1-5 ms push target without busy polling (API §7.6); the axum
            // `KeepAlive` layer emits the `: hb` comment on its own cadence.
            let notifies: Vec<_> = live.iter().map(|b| Box::pin(b.notify.notified())).collect();
            if notifies.is_empty() {
                // No live boxes to wait on; honor the heartbeat tick, but wake at
                // once on shutdown so the next loop pass emits the close frame (M11).
                tokio::select! {
                    _ = shutdown.notified() => {}
                    _ = tokio::time::sleep(Duration::from_millis(heartbeat_ms)) => {}
                }
            } else {
                let wake = futures::future::select_all(notifies);
                tokio::select! {
                    _ = shutdown.notified() => {}
                    _ = wake => {}
                    _ = tokio::time::sleep(Duration::from_millis(heartbeat_ms)) => {}
                }
            }
        }
    }
}

/// Project a read record onto the SSE `record`-frame JSON, honoring
/// `include_data` (lightweight metadata-only tailing; API §7.5).
///
/// Also used by the zero-copy broadcast cache
/// ([`crate::engine::broadcast`]) to serialize each frame **once** and share the
/// resulting buffer across all watchers — so this MUST stay the single source of
/// truth for a record frame's bytes.
pub(crate) fn record_frame(r: &RecordOut, include_data: bool) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    obj.insert("$seq".into(), serde_json::json!(r.seq));
    obj.insert("$ts".into(), serde_json::json!(r.ts));
    if let Some(node) = &r.node {
        obj.insert("$node".into(), serde_json::json!(node));
    }
    if let Some(tag) = &r.tag {
        obj.insert("$tag".into(), serde_json::json!(tag));
    }
    if include_data {
        obj.insert("data".into(), r.data.clone());
    }
    if let Some(meta) = &r.meta {
        obj.insert("meta".into(), meta.clone());
    }
    serde_json::Value::Object(obj)
}

/// Encode the session's current per-box cursor map as a base64url JSON id
/// (API §7.4). Used as both the SSE `id:` and the `Last-Event-ID` resume token.
fn encode_session_id(session: &Session) -> String {
    let map = session.cursors.lock().clone();
    let json = serde_json::to_vec(&map).unwrap_or_default();
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(json)
}

/// Decode a `Last-Event-ID` / `cursor` composite id back to a `box → seq` map.
fn decode_cursor_id(id: &str) -> Option<BTreeMap<String, u64>> {
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(id)
        .ok()?;
    serde_json::from_slice(&bytes).ok()
}

/// Extract a presented bearer for the stream GET: the `Authorization: Bearer`
/// header (preferred), falling back to the dev-only `?token=` query parameter
/// (already URL-decoded by axum's `Query` extractor). Returns `None` when neither
/// is present.
fn bearer_from_request(headers: &HeaderMap, params: &HashMap<String, String>) -> Option<String> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(|t| t.trim().to_string())
        .or_else(|| params.get("token").cloned())
}

/// Reject a stream GET whose `Accept` is not `text/event-stream` (API §7,
/// `406 not_acceptable`).
fn require_event_stream_accept(headers: &HeaderMap) -> Result<()> {
    let accept = headers
        .get(axum::http::header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    // An absent/`*/*` Accept is tolerated for curl-style clients.
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
    fn composite_id_round_trips() {
        let mut cursors = BTreeMap::new();
        cursors.insert("jobs".to_string(), 5210u64);
        cursors.insert("events".to_string(), 88130u64);
        let session = Session {
            req: WatchCreateRequest {
                node: None,
                boxes: HashMap::new(),
                limit: 256,
                max_batch_bytes: 262_144,
                heartbeat_ms: 15_000,
                include_meta: true,
                include_tags: false,
                include_data: true,
                consistency: Consistency::Eventual,
            },
            cursors: Mutex::new(cursors),
            key_id: None,
            scopes: crate::auth::Scope::ALL,
            last_access_ms: std::sync::atomic::AtomicI64::new(0),
            active_streams: std::sync::atomic::AtomicU64::new(0),
        };
        let id = encode_session_id(&session);
        let decoded = decode_cursor_id(&id).expect("decodes");
        assert_eq!(decoded.get("jobs"), Some(&5210));
        assert_eq!(decoded.get("events"), Some(&88130));
    }

    fn empty_session(key_id: Option<crate::auth::KeyId>) -> Session {
        Session {
            req: WatchCreateRequest {
                node: None,
                boxes: HashMap::new(),
                limit: 256,
                max_batch_bytes: 262_144,
                heartbeat_ms: 15_000,
                include_meta: true,
                include_tags: false,
                include_data: true,
                consistency: Consistency::Eventual,
            },
            cursors: Mutex::new(BTreeMap::new()),
            key_id,
            scopes: crate::auth::Scope::ALL,
            last_access_ms: std::sync::atomic::AtomicI64::new(0),
            active_streams: std::sync::atomic::AtomicU64::new(0),
        }
    }

    #[test]
    fn session_authorize_capability_and_binding() {
        use crate::auth::{KeyId, KeyStore};
        // A store holding the creating key `s3cr3t` plus an unrelated valid key.
        let keys = KeyStore::parse("s3cr3t,other").unwrap();

        // Unbound (dev mode): the wid alone authorizes, with or without a bearer.
        let dev = empty_session(None);
        assert!(dev.authorize(None, &keys));
        assert!(dev.authorize(Some("anything"), &keys));

        // Bound to the id of `s3cr3t`.
        let bound = empty_session(Some(KeyId::of("s3cr3t")));
        // No bearer presented ⇒ the wid alone is NOT sufficient (codex HIGH #3):
        // a leaked wid cannot be opened without the creating key.
        assert!(!bound.authorize(None, &keys));
        // Matching bearer ⇒ ok.
        assert!(bound.authorize(Some("s3cr3t"), &keys));
        // A DIFFERENT but valid key ⇒ rejected (cannot replay under another key).
        assert!(!bound.authorize(Some("other"), &keys));
        // An invalid bearer ⇒ rejected.
        assert!(!bound.authorize(Some("wrong"), &keys));
    }

    #[test]
    fn alloc_wid_is_prefixed_random_and_unique() {
        let a = SessionStore::alloc_wid();
        let b = SessionStore::alloc_wid();
        assert!(
            a.starts_with("wid_"),
            "wid keeps the documented prefix: {a}"
        );
        assert_ne!(a, b, "wids must be unique/random, not monotonic");
        // 16 random bytes ⇒ 22 base64url chars (no pad). Total len = 4 + 22 = 26.
        assert_eq!(a.len(), 26, "wid carries >=128 bits of randomness: {a}");
        // Path-safe (base64url + the `_` from the prefix), no `/` or `+` or `=`.
        let suffix = a.strip_prefix("wid_").unwrap();
        assert!(
            suffix
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_'),
            "suffix is base64url: {suffix}"
        );
    }

    #[test]
    fn accept_guard_rejects_non_sse() {
        let mut h = HeaderMap::new();
        h.insert(header::ACCEPT, "application/json".parse().unwrap());
        assert_eq!(
            require_event_stream_accept(&h).unwrap_err().code,
            ErrorCode::NotAcceptable
        );
        h.insert(header::ACCEPT, "text/event-stream".parse().unwrap());
        assert!(require_event_stream_accept(&h).is_ok());
    }
}
