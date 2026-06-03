//! HTTP layer: the axum `Router` for `/v0`, bearer-auth middleware,
//! content-type / body-size guards, the `Error` → HTTP envelope mapping, and
//! the per-response `performance` block plumbing.

pub mod delete;
pub mod health;
pub mod queue;
pub mod routers;
pub mod topics;
pub mod watch;
pub mod ws;

use crate::auth::{KeyStore, Principal, Scope};
use crate::engine::Engine;
use crate::error::Error;
use crate::types::ErrorCode;
use axum::{
    extract::{DefaultBodyLimit, Request},
    http::{header, HeaderMap, HeaderValue, Method, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Json, Response},
    routing::{get, post, put},
    Router,
};
use queue::ClaimCoordinator;
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::sync::Arc;
use watch::SessionStore;

/// Shared state handed to every handler.
#[derive(Clone)]
pub struct AppState {
    pub engine: Arc<Engine>,
    /// In-memory watch-session registry (API §7.1).
    pub sessions: Arc<SessionStore>,
    /// Per-topic coalescing-window claim coordinator + `/work` conn ids (API §10).
    pub coordinator: Arc<ClaimCoordinator>,
    /// Hashed, constant-time API-key store (scopes + topic-name prefix allowlist).
    /// Parsed **once** here from `TOPICS_API_KEYS`; the auth middleware reuses it
    /// per request. Empty ⇒ auth disabled (dev mode).
    pub keys: Arc<KeyStore>,
    /// Live, mutable per-instance resource counters for the concurrency limits
    /// (SSE connections, per-key in-flight requests; see [`crate::limits`]). The
    /// topic/router/session caps are checked against the registries directly.
    pub live: Arc<crate::limits::LiveCounts>,
    /// Graceful-shutdown coordination shared with the serve loop (M11): on
    /// shutdown the serve loop triggers this and every open SSE stream winds down
    /// and closes, so the bounded drain completes promptly.
    pub shutdown: Arc<crate::serve::ShutdownSignal>,
}

/// Build the full `/v0` axum router with middleware applied.
///
/// Parses the configured API keys into a hashed [`KeyStore`] once. A malformed
/// scope token in `TOPICS_API_KEYS` makes the parse fail; rather than booting
/// with auth silently degraded, this **panics** (the binary's startup also
/// validates via [`build_router_checked`], which surfaces the error cleanly).
pub fn build_router(engine: Arc<Engine>) -> Router {
    build_router_checked(engine).unwrap_or_else(|msg| {
        panic!("invalid TOPICS_API_KEYS configuration: {msg}");
    })
}

/// Like [`build_router`] but returns the key-parse error instead of panicking, so
/// the binary can fail closed with a clear message at startup. Creates a private
/// [`ShutdownSignal`](crate::serve::ShutdownSignal); use
/// [`build_router_with_shutdown`] when the serve loop must share the same signal
/// to wind down SSE streams on shutdown (M11).
pub fn build_router_checked(engine: Arc<Engine>) -> std::result::Result<Router, String> {
    build_router_with_shutdown(engine, Arc::new(crate::serve::ShutdownSignal::new()))
}

/// As [`build_router_checked`], but the caller supplies the
/// [`ShutdownSignal`](crate::serve::ShutdownSignal) it will also hand to
/// [`serve_with_signal`](crate::serve::serve_with_signal), so the serve loop can
/// actively wind down this router's in-flight SSE streams within the bounded
/// drain (M11).
pub fn build_router_with_shutdown(
    engine: Arc<Engine>,
    shutdown: Arc<crate::serve::ShutdownSignal>,
) -> std::result::Result<Router, String> {
    let max_body = engine.config.max_body_bytes;
    let keys = engine.config.key_store()?;
    let state = AppState {
        engine,
        sessions: Arc::new(SessionStore::new()),
        coordinator: Arc::new(ClaimCoordinator::new()),
        keys: Arc::new(keys),
        live: Arc::new(crate::limits::LiveCounts::new()),
        shutdown,
    };

    let v0 = Router::new()
        // Topics
        .route("/topics", get(topics::list_topics))
        .route(
            "/topics/{topic}",
            put(topics::put_topic)
                .get(topics::get_topic)
                .delete(topics::delete_topic)
                .post(topics::write),
        )
        .route("/topics/{topic}/diff", post(topics::diff))
        .route("/topics/{topic}/delete", post(delete::delete))
        // Queue lifecycle (API §10)
        .route("/topics/{topic}/claim", post(queue::claim))
        .route("/topics/{topic}/ack", post(queue::ack))
        .route("/topics/{topic}/nack", post(queue::nack))
        .route("/topics/{topic}/extend", post(queue::extend))
        .route("/topics/{topic}/work", get(queue::work))
        // Routers
        .route("/routers", get(routers::list_routers))
        .route(
            "/routers/{router}",
            put(routers::put_router)
                .get(routers::get_router)
                .delete(routers::delete_router),
        )
        // Watch / SSE
        .route("/watch", post(watch::create_watch))
        .route("/watch/{wid}", get(watch::stream_watch))
        // WebSocket dynamic subscribe/publish
        .route("/ws", get(ws::websocket))
        // Health / readiness / metrics
        .route("/health", get(health::health))
        .route("/ready", get(health::ready))
        .route("/metrics", get(health::metrics));

    Ok(Router::new()
        .nest("/v0", v0)
        // Root-level probe aliases for load balancers (API §8).
        .route("/healthz", get(health::health))
        .route("/readyz", get(health::ready))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ))
        // Hard body-size guard (`413 payload_too_large`) applied before parse.
        .layer(DefaultBodyLimit::max(max_body))
        // Rewrite bare 413/404/405/415 onto the canonical error envelope.
        .layer(middleware::from_fn(error_envelope_middleware))
        .with_state(state))
}

/// Bearer-auth middleware. Disabled when no keys are configured (dev mode).
/// Probe endpoints skip auth unless `TOPICS_PROBE_AUTH` is set.
///
/// When auth is enabled it (1) authenticates the bearer against the hashed
/// [`KeyStore`] in constant time, (2) enforces the route's required
/// [`Scope`] and the key's topic-name **prefix allowlist**, then (3) stashes the
/// matched [`Principal`] (scopes + prefixes, *no secret*) in request extensions
/// so a handler can bind a created resource (e.g. a watch session) to its
/// creator and scope. A key with no scopes / no prefixes is full-access.
///
/// `GET /v0/watch/:wid` is special: the `wid` is an unguessable bearer capability
/// (minted by the authenticated `POST /v0/watch`), so the stream GET is authorized
/// by *possessing* the wid and is NOT gated here — the handler enforces the
/// per-session principal binding (which already captured the creator's scope).
/// This lets browser `EventSource` (GET-only, no custom headers) open the stream
/// with just the secret URL, without putting a long-lived api key in a logged
/// query string.
async fn auth_middleware(
    axum::extract::State(state): axum::extract::State<AppState>,
    mut req: Request,
    next: Next,
) -> Response {
    let cfg = &state.engine.config;
    if !cfg.auth_enabled() {
        // Dev mode: every handler runs as an implicit full-access principal so
        // its scope/prefix checks are uniform with the auth-enabled path.
        req.extensions_mut().insert(Principal::full_access());
        return next.run(req).await;
    }

    let path = req.uri().path();
    let is_probe = is_probe_path(path);
    if is_probe && !cfg.probe_auth {
        return next.run(req).await;
    }

    // Capability-authorized: the SSE stream GET is gated by the wid, not by a
    // bearer in the URL. Defer to the handler (which checks the wid binding).
    if req.method() == Method::GET && is_watch_stream_path(path) {
        return next.run(req).await;
    }

    // Bearer token from the `Authorization` header. The `?token=` query-string
    // fallback (which leaks via proxy/access logs and browser history; codex
    // MEDIUM #8) is accepted ONLY for the long-lived SSE stream GETs, where a
    // browser `EventSource` cannot set a custom header — never for ordinary
    // data/control-plane routes, which must use the header. (The `/v0/watch/:wid`
    // stream GET is handled by its own handler above; this covers `/work`.)
    let allow_query_token = req.method() == Method::GET && is_sse_stream_path(path);
    let provided = extract_bearer(req.headers())
        .map(str::to_string)
        .or_else(|| {
            if allow_query_token {
                query_token(req.uri().query())
            } else {
                None
            }
        });

    let principal = match provided.as_deref().and_then(|t| state.keys.authenticate(t)) {
        Some(p) => p,
        None => {
            return Error::new(ErrorCode::Unauthorized, "missing or invalid bearer token")
                .into_response()
        }
    };

    // Per-key in-flight (concurrency) cap (DoS hardening; [`crate::limits`]). The
    // guard is held across `next.run` and released on drop (response sent or
    // handler panic), so a stuck handler frees its slot. `0` ⇒ unlimited; dev mode
    // (no key) is never capped. SSE streams are long-lived and are bounded by the
    // dedicated SSE-connection caps instead, so they do not consume an in-flight
    // slot for their whole lifetime — release it before the handler runs (the
    // stream's own `SseGuard` then bounds it).
    let limits = &state.engine.config.limits;
    let inflight_guard = match state.live.try_acquire_inflight(limits, principal.key_id) {
        Some(g) => g,
        None => {
            return Error::new(
                ErrorCode::Throttled,
                "too many concurrent requests for this api key",
            )
            .with_retry_after(crate::limits::LIMIT_RETRY_AFTER_S)
            .into_response();
        }
    };
    // The SSE stream GETs (`/v0/topics/:q/work`) are long-lived; do not pin an
    // in-flight slot for the whole stream (the per-key SSE-connection cap covers
    // them). They are bounded separately in their handlers.
    if req.method() == Method::GET && is_sse_stream_path(path) {
        drop(inflight_guard);
    }

    // Enforce the route's required scope + the key's topic-name prefix allowlist.
    // A full-access key (Scope::ALL, no prefixes) passes both unconditionally.
    if let Some((needed, name)) = route_requirement(req.method(), path) {
        if !principal.allows_scope(needed) {
            return Error::new(
                ErrorCode::Forbidden,
                "api key lacks the scope required for this operation",
            )
            .into_response();
        }
        if let Some(name) = name {
            if !principal.allows_name(name) {
                return Error::new(
                    ErrorCode::Forbidden,
                    "api key is not allowed to access this topic/router name",
                )
                .into_response();
            }
        }
    }

    req.extensions_mut().insert(principal);
    next.run(req).await
}

/// The scope a request requires plus the topic/router name it touches (for the
/// prefix-allowlist check), derived from the method + `/v0` path. `None` means
/// the route is unguarded by scope (e.g. probes, or `GET /v0/watch/:wid` which is
/// capability-gated and handled separately). The name is `None` for collection
/// routes (`/v0/topics`, `/v0/routers`, `POST /v0/watch`) where no single name is
/// addressed at the path; per-topic prefix filtering of *list* results is a future
/// refinement and not required for the security boundary (a list cannot mutate).
fn route_requirement<'a>(method: &Method, path: &'a str) -> Option<(Scope, Option<&'a str>)> {
    // Strip the `/v0` prefix; root probes (`/healthz`, `/readyz`) are unguarded.
    let rest = path.strip_prefix("/v0")?;

    // /topics ...
    if let Some(seg) = rest.strip_prefix("/topics") {
        // Collection: GET /v0/topics (list) needs read; no single name.
        if seg.is_empty() || seg == "/" {
            return Some((Scope::READ, None));
        }
        let seg = seg.strip_prefix('/')?;
        // seg is `:topic` or `:topic/<action>`.
        let (topic_name, action) = match seg.split_once('/') {
            Some((b, a)) => (b, Some(a)),
            None => (seg, None),
        };
        let scope = match (method, action) {
            // PUT /v0/topics/:topic — control-plane (create/configure).
            (&Method::PUT, None) => Scope::ADMIN,
            // GET /v0/topics/:topic — read state.
            (&Method::GET, None) => Scope::READ,
            // DELETE /v0/topics/:topic — delete.
            (&Method::DELETE, None) => Scope::DELETE,
            // POST /v0/topics/:topic — write records.
            (&Method::POST, None) => Scope::WRITE,
            // POST /v0/topics/:topic/diff — read.
            (&Method::POST, Some("diff")) => Scope::READ,
            // POST /v0/topics/:topic/delete — delete.
            (&Method::POST, Some("delete")) => Scope::DELETE,
            // Queue claim / work: lease (mutate) + return ⇒ read+write.
            (&Method::POST, Some("claim")) | (&Method::GET, Some("work")) => {
                Scope::READ.union(Scope::WRITE)
            }
            // Queue ack/nack/extend — write (mutate lease state).
            (&Method::POST, Some("ack"))
            | (&Method::POST, Some("nack"))
            | (&Method::POST, Some("extend")) => Scope::WRITE,
            // Anything else under /topics/:topic: require write as a safe default so
            // an unknown future mutating sub-route is never under-guarded. (A
            // method-not-allowed path still gets here but the scope check on a
            // full-access key is a no-op, and a scoped key fails closed.)
            _ => Scope::WRITE,
        };
        return Some((scope, Some(topic_name)));
    }

    // /routers ...
    if let Some(seg) = rest.strip_prefix("/routers") {
        if seg.is_empty() || seg == "/" {
            // GET /v0/routers (list) needs read; no single name.
            return Some((Scope::READ, None));
        }
        let router = seg.strip_prefix('/')?;
        // Router path is a single `:router` segment (no sub-actions).
        let scope = match *method {
            Method::PUT => Scope::ADMIN,     // create/configure
            Method::GET => Scope::READ,      // read
            Method::DELETE => Scope::DELETE, // delete
            _ => Scope::ADMIN,               // fail-closed default
        };
        return Some((scope, Some(router)));
    }

    // POST /v0/watch — create a watch session (a read subscription). The bound
    // topics are validated/scoped at session creation in the handler (it knows the
    // body's topic map); the GET stream is capability-gated. Require read here.
    if rest == "/watch" {
        return Some((Scope::READ, None));
    }

    // GET /v0/ws — open a dynamic WebSocket. The concrete operation is carried in
    // each WebSocket message, so the handler enforces read/write/admin and prefix
    // checks per command. This lets write-only clients publish without also holding
    // read scope, while read-only clients can subscribe but not publish.
    if rest == "/ws" && method == Method::GET {
        return None;
    }

    // GET /v0/metrics — operational telemetry (topic count). Gated behind auth by
    // default (codex LOW #12); a read-scoped monitoring key suffices.
    if rest == "/metrics" {
        return Some((Scope::READ, None));
    }

    // Liveness/readiness probes (`/v0/health`, `/v0/ready`) and anything else: no
    // scope requirement (probe auth, if enabled, is just authentication above).
    None
}

/// True for a long-lived SSE stream GET that should NOT hold a per-key in-flight
/// slot for its whole lifetime (it is bounded by the SSE-connection caps instead):
/// the watch stream `GET /v0/watch/:wid` and the queue `GET /v0/topics/:q/work`.
fn is_sse_stream_path(path: &str) -> bool {
    if is_watch_stream_path(path) {
        return true;
    }
    // /v0/topics/:topic/work
    matches!(
        path.strip_prefix("/v0/topics/")
            .and_then(|rest| rest.split_once('/')),
        Some((_topic, "work"))
    )
}

/// The unauthenticated probe set: only the **minimal liveness/readiness** checks
/// (`/healthz`, `/readyz`, `/v0/health`, `/v0/ready`) so a load balancer can poll
/// them without a key. `/v0/metrics` is deliberately NOT here — it exposes the topic
/// count and is gated behind auth by default (codex LOW #12); set
/// `TOPICS_PROBE_AUTH` to additionally require auth on the liveness/readiness
/// probes, or scrape `/v0/metrics` with a read-scoped key.
fn is_probe_path(path: &str) -> bool {
    matches!(path, "/healthz" | "/readyz" | "/v0/health" | "/v0/ready")
}

/// True for the SSE stream path `GET /v0/watch/:wid` (exactly one path segment
/// after `/v0/watch/`). The session-creating `POST /v0/watch` is NOT matched (it
/// must be authenticated normally).
fn is_watch_stream_path(path: &str) -> bool {
    match path.strip_prefix("/v0/watch/") {
        Some(rest) => !rest.is_empty() && !rest.contains('/'),
        None => false,
    }
}

fn extract_bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
        .map(str::trim)
}

/// Extract the `?token=` query parameter using a real
/// `application/x-www-form-urlencoded` parser: it URL-decodes percent-escapes and
/// `+`, and on a duplicate `token=` it takes the FIRST occurrence (deterministic).
/// This is a documented dev-only fallback for browser `EventSource`; prefer the
/// `Authorization: Bearer` header since a query string leaks via logs/history/
/// proxies.
fn query_token(query: Option<&str>) -> Option<String> {
    let q = query?;
    form_urlencoded::parse(q.as_bytes())
        .find(|(k, _)| k == "token")
        .map(|(_, v)| v.into_owned())
}

// ---------------------------------------------------------------------------
// Error -> HTTP response (API §0.5)
// ---------------------------------------------------------------------------

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let status =
            StatusCode::from_u16(self.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let mut resp = (status, Json(self.envelope())).into_response();
        if let Some(secs) = self.retry_after_s {
            if let Ok(v) = HeaderValue::from_str(&secs.to_string()) {
                resp.headers_mut().insert(header::RETRY_AFTER, v);
            }
        }
        resp
    }
}

/// Validate the `Content-Type` of a request body as JSON (API §0.3); used by
/// handlers with bodies to return `415 unsupported_media_type`.
pub fn require_json_content_type(headers: &HeaderMap) -> Result<(), Error> {
    match headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
    {
        Some(ct) if ct.trim_start().starts_with("application/json") => Ok(()),
        _ => Err(Error::new(
            ErrorCode::UnsupportedMediaType,
            "Content-Type must be application/json",
        )),
    }
}

/// Guard the `Content-Type` and deserialize a JSON request body into `T`.
///
/// Returns `415 unsupported_media_type` for a non-JSON content type and
/// `400 invalid_request` for a malformed/ill-typed body. Handlers extract the
/// raw [`Bytes`](axum::body::Bytes) so the content-type check happens *before*
/// parse and so an empty body can be special-cased by the caller.
pub fn parse_json_body<T: DeserializeOwned>(headers: &HeaderMap, body: &[u8]) -> Result<T, Error> {
    require_json_content_type(headers)?;
    serde_json::from_slice(body)
        .map_err(|e| Error::invalid_request(format!("malformed JSON body: {e}")))
}

/// Run a synchronous, possibly-blocking engine call (a mutating op that may wait
/// on a WAL group fsync) on tokio's blocking pool, so the fsync wait never parks
/// a reactor thread (ARCHITECTURE §8.5). Maps a join failure (only on panic) to a
/// `500`.
pub async fn run_blocking<T, F>(f: F) -> Result<T, Error>
where
    F: FnOnce() -> Result<T, Error> + Send + 'static,
    T: Send + 'static,
{
    match tokio::task::spawn_blocking(f).await {
        Ok(res) => res,
        Err(e) => Err(Error::internal(format!("engine task failed: {e}"))),
    }
}

/// Parse a boolean query parameter (`true`/`false`/`1`/`0`), falling back to
/// `default` when absent or unparseable.
pub fn query_bool(params: &HashMap<String, String>, key: &str, default: bool) -> bool {
    match params.get(key).map(String::as_str) {
        Some("true") | Some("1") => true,
        Some("false") | Some("0") => false,
        _ => default,
    }
}

/// Map axum's bare body-limit / fallback responses onto the canonical error
/// envelope. The `DefaultBodyLimit` layer rejects oversized bodies with a bare
/// `413` (no body); rewrite it to `413 payload_too_large` (API §0.6). A bare
/// `404`/`405` from routing likewise gets the envelope.
async fn error_envelope_middleware(req: Request, next: Next) -> Response {
    let resp = next.run(req).await;
    let status = resp.status();
    // Only rewrite responses that don't already carry a JSON body of our own.
    let is_ours = resp
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|c| c.starts_with("application/json"))
        .unwrap_or(false);
    if is_ours {
        return resp;
    }
    let code = match status {
        StatusCode::PAYLOAD_TOO_LARGE => Some(ErrorCode::PayloadTooLarge),
        StatusCode::NOT_FOUND => Some(ErrorCode::NotFound),
        StatusCode::METHOD_NOT_ALLOWED => Some(ErrorCode::MethodNotAllowed),
        StatusCode::UNSUPPORTED_MEDIA_TYPE => Some(ErrorCode::UnsupportedMediaType),
        _ => None,
    };
    match code {
        Some(c) => {
            let msg = match c {
                ErrorCode::PayloadTooLarge => "request body exceeds the server limit",
                ErrorCode::NotFound => "resource not found",
                ErrorCode::MethodNotAllowed => "method not allowed for this path",
                ErrorCode::UnsupportedMediaType => "Content-Type must be application/json",
                _ => "error",
            };
            Error::new(c, msg).into_response()
        }
        None => resp,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_bool_parses_common_forms() {
        let mut p = HashMap::new();
        assert!(query_bool(&p, "touch", true));
        assert!(!query_bool(&p, "touch", false));
        p.insert("touch".to_string(), "false".to_string());
        assert!(!query_bool(&p, "touch", true));
        p.insert("touch".to_string(), "1".to_string());
        assert!(query_bool(&p, "touch", false));
    }

    #[test]
    fn parse_json_body_requires_content_type() {
        let headers = HeaderMap::new();
        let r: Result<crate::types::DiffRequest, _> = parse_json_body(&headers, b"{}");
        assert_eq!(r.unwrap_err().code, ErrorCode::UnsupportedMediaType);
    }

    #[test]
    fn parse_json_body_rejects_bad_json() {
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
        let r: Result<crate::types::DiffRequest, _> = parse_json_body(&headers, b"{bad");
        assert_eq!(r.unwrap_err().code, ErrorCode::InvalidRequest);
    }

    #[test]
    fn query_token_basic_and_decoded() {
        assert_eq!(query_token(None), None);
        assert_eq!(query_token(Some("token=abc")), Some("abc".to_string()));
        assert_eq!(
            query_token(Some("x=1&token=abc&y=2")),
            Some("abc".to_string())
        );
        // Percent-escapes and `+` are decoded (a real form parser, not `strip_prefix`).
        assert_eq!(
            query_token(Some("token=a%2Bb%3Dc")),
            Some("a+b=c".to_string())
        );
        assert_eq!(query_token(Some("token=a+b")), Some("a b".to_string()));
        // No token param ⇒ None (even if another key has a `token`-ish prefix).
        assert_eq!(query_token(Some("tokenx=abc")), None);
    }

    #[test]
    fn query_token_takes_first_duplicate() {
        // Deterministic: first occurrence wins on a duplicated param.
        assert_eq!(
            query_token(Some("token=first&token=second")),
            Some("first".to_string())
        );
    }

    #[test]
    fn watch_stream_path_matches_only_the_get_stream() {
        assert!(is_watch_stream_path("/v0/watch/wid_abc"));
        // The POST create path (no trailing segment) is NOT the stream path.
        assert!(!is_watch_stream_path("/v0/watch"));
        assert!(!is_watch_stream_path("/v0/watch/"));
        // Extra segments must not match (no path traversal past the wid).
        assert!(!is_watch_stream_path("/v0/watch/wid/extra"));
        assert!(!is_watch_stream_path("/v0/topics/jobs"));
    }
}
