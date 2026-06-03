//! Dual-protocol (HTTP/1.1 keep-alive + HTTP/2 cleartext "prior-knowledge")
//! serve loop with graceful shutdown.
//!
//! Phase-5B Stage 1 replaces the plain `axum::serve` accept loop with a
//! `hyper-util` [`auto::Builder`] loop so the SAME listener serves both:
//!
//! - **HTTP/1.1** (with keep-alive) — every existing `reqwest` test/client, and
//! - **HTTP/2 cleartext** via prior knowledge — an `reqwest .http2_prior_knowledge()`
//!   (or any client that opens with the `PRI * HTTP/2.0` preface) is upgraded to
//!   h2 with no ALPN/TLS and no `h2c` Upgrade dance.
//!
//! The protocol is auto-detected per connection by [`auto::Builder`], which
//! sniffs the HTTP/2 connection preface on the first bytes and otherwise falls
//! back to HTTP/1. The axum [`Router`] is wrapped in [`TowerToHyperService`] so
//! the exact same app (middleware, SSE streaming, recovery/ready gate) is served
//! under either protocol.
//!
//! Graceful shutdown mirrors the previous `with_graceful_shutdown` behaviour:
//! once the shutdown future resolves we stop accepting new connections and wait
//! (via [`graceful::GracefulShutdown`]) for in-flight connections — including
//! long-lived SSE streams that are themselves told to wind down — to complete.

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto;
use hyper_util::server::graceful::GracefulShutdown;
use hyper_util::service::TowerToHyperService;
use tokio::net::TcpListener;
use tracing::{debug, info, trace, warn};

/// Wall-clock budget for the graceful-drain race (M11): once we stop accepting,
/// in-flight connections — including long-lived SSE streams that have been told
/// to wind down — get this long to close before we abandon the wait and return.
/// Bounds a rolling restart so a wedged/idle SSE client cannot pin the old
/// process indefinitely.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// Process-wide graceful-shutdown coordination shared between the serve loop and
/// the in-flight SSE streams (M11). The serve loop [`trigger`](Self::trigger)s it
/// once the shutdown signal fires; every open SSE stream parks on
/// [`notified`](Self::notified) and, when woken (or finding [`is_shutting_down`]
/// already set), emits a terminal close and ends — so the bounded drain actually
/// completes instead of waiting on streams that would otherwise tail forever.
#[derive(Debug)]
pub struct ShutdownSignal {
    flag: AtomicBool,
    notify: tokio::sync::Notify,
}

impl Default for ShutdownSignal {
    fn default() -> Self {
        Self::new()
    }
}

impl ShutdownSignal {
    pub fn new() -> Self {
        ShutdownSignal {
            flag: AtomicBool::new(false),
            notify: tokio::sync::Notify::new(),
        }
    }

    /// Begin shutdown: latch the flag and wake every parked SSE stream so they
    /// wind down. Idempotent.
    pub fn trigger(&self) {
        self.flag.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    /// Whether shutdown has begun (a stream opened *after* `trigger` sees this and
    /// closes immediately rather than racing the one-shot `notify_waiters`).
    pub fn is_shutting_down(&self) -> bool {
        self.flag.load(Ordering::SeqCst)
    }

    /// Await the shutdown notification (for an SSE stream's `select!`). Resolves
    /// immediately if shutdown has already begun.
    ///
    /// Race-free against [`Self::trigger`] (M11 / codex P2 #2): the prior version
    /// checked the flag and only THEN awaited the `Notify`, so a `trigger()` landing
    /// in that window armed nobody (`notify_waiters` only wakes ALREADY-registered
    /// waiters) and the stream slept until its (up-to-60s) heartbeat — past the 10s
    /// drain. Here we build the `Notified` future and `enable()` it FIRST (which
    /// registers this waiter), then re-check the flag: if `trigger()` already ran,
    /// the flag is set and we return at once; if it runs after, our registered
    /// waiter is woken by `notify_waiters`. Either ordering is covered.
    pub async fn notified(&self) {
        let fut = self.notify.notified();
        tokio::pin!(fut);
        // Register this waiter so a concurrent `notify_waiters()` cannot be missed.
        fut.as_mut().enable();
        if self.is_shutting_down() {
            return;
        }
        fut.await;
    }
}

// ---------------------------------------------------------------------------
// Tuning (keep-alive + HTTP/2 settings)
// ---------------------------------------------------------------------------

/// HTTP/2 initial connection + stream window (1 MiB). Bigger than the 64 KiB
/// default so a single SSE stream / large diff body isn't throttled by flow
/// control on a fast local link.
const H2_INITIAL_STREAM_WINDOW: u32 = 1024 * 1024;
/// HTTP/2 initial connection-wide window (4 MiB) so many multiplexed SSE streams
/// share ample aggregate window.
const H2_INITIAL_CONN_WINDOW: u32 = 4 * 1024 * 1024;
/// Cap concurrent HTTP/2 streams per connection. Generous (many SSE watchers may
/// multiplex over one h2 connection) but bounded so one peer can't exhaust us.
const H2_MAX_CONCURRENT_STREAMS: u32 = 1024;
/// HTTP/2 keep-alive ping interval / timeout: detect a dead peer holding open an
/// idle SSE stream without churning the socket.
const H2_KEEPALIVE_INTERVAL_SECS: u64 = 20;
const H2_KEEPALIVE_TIMEOUT_SECS: u64 = 20;
/// HTTP/1 header read timeout: bound how long a slow/idle keep-alive connection
/// may sit between requests before its next header line must arrive.
const H1_HEADER_READ_TIMEOUT_SECS: u64 = 60;

/// Configure the dual-protocol `auto::Builder` with the keep-alive + HTTP/2
/// settings chosen for the latency/throughput targets. Shared by the binary and
/// the integration harness so they serve byte-for-byte identically.
fn configure_builder(builder: &mut auto::Builder<TokioExecutor>) {
    // HTTP/1.1: keep-alive on (the default), with a header-read timeout so an
    // idle/slow keep-alive connection can't pin a slot indefinitely. TCP_NODELAY
    // is set on the accepted socket (see `serve`) so SSE flushes aren't Nagle'd.
    // A `TokioTimer` MUST be supplied or hyper panics when the timeout fires.
    builder
        .http1()
        .timer(TokioTimer::new())
        .keep_alive(true)
        .header_read_timeout(std::time::Duration::from_secs(H1_HEADER_READ_TIMEOUT_SECS));

    // HTTP/2: larger windows for streaming, a generous-but-bounded stream cap,
    // and keep-alive pings to reap dead peers on idle SSE streams. The keep-alive
    // ping likewise requires a `TokioTimer`.
    builder
        .http2()
        .timer(TokioTimer::new())
        .initial_stream_window_size(H2_INITIAL_STREAM_WINDOW)
        .initial_connection_window_size(H2_INITIAL_CONN_WINDOW)
        .max_concurrent_streams(H2_MAX_CONCURRENT_STREAMS)
        .keep_alive_interval(std::time::Duration::from_secs(H2_KEEPALIVE_INTERVAL_SECS))
        .keep_alive_timeout(std::time::Duration::from_secs(H2_KEEPALIVE_TIMEOUT_SECS));
}

/// Serve `app` on `listener`, auto-detecting HTTP/1.1 vs HTTP/2-prior-knowledge
/// per connection, until `shutdown` resolves; then gracefully drain in-flight
/// connections. This is the dual-protocol replacement for `axum::serve(..)
/// .with_graceful_shutdown(..)` and preserves all of its observable behaviour
/// (SSE under both protocols, graceful drain).
///
/// Entry point that creates a private [`ShutdownSignal`] that no SSE stream
/// observes, so the drain still relies on connections closing on their own.
/// New code should use [`serve_with_signal`] and share the same
/// [`ShutdownSignal`] with [`AppState`](crate::http::AppState) so SSE streams are
/// actively wound down within the bounded drain (M11).
pub async fn serve<S>(listener: TcpListener, app: Router, shutdown: S) -> std::io::Result<()>
where
    S: Future<Output = ()> + Send + 'static,
{
    serve_with_signal(listener, app, shutdown, Arc::new(ShutdownSignal::new())).await
}

/// As [`serve`], but the caller supplies the [`ShutdownSignal`] shared with the
/// router's [`AppState`](crate::http::AppState) (M11). On shutdown the serve loop
/// stops accepting, [`triggers`](ShutdownSignal::trigger) the signal so every
/// open SSE stream emits a terminal close, and then **bounds** the graceful drain
/// with [`DRAIN_TIMEOUT`]: if a connection still has not closed by then the wait
/// is abandoned and the function returns (rolling-restart safety — a wedged peer
/// cannot pin the old process forever).
pub async fn serve_with_signal<S>(
    listener: TcpListener,
    app: Router,
    shutdown: S,
    sse_signal: Arc<ShutdownSignal>,
) -> std::io::Result<()>
where
    S: Future<Output = ()> + Send + 'static,
{
    // One hyper service shared across connections: the axum Router adapted to a
    // hyper service. `TowerToHyperService` clones the inner service per call.
    let service = TowerToHyperService::new(app);

    let mut builder = auto::Builder::new(TokioExecutor::new());
    configure_builder(&mut builder);

    let graceful = GracefulShutdown::new();
    let mut shutdown = std::pin::pin!(shutdown);

    loop {
        tokio::select! {
            // Bias toward shutdown so a flood of new connections can't starve it.
            biased;

            () = &mut shutdown => {
                debug!("serve: shutdown signal received; no longer accepting connections");
                break;
            }

            accepted = listener.accept() => {
                let (stream, peer) = match accepted {
                    Ok(pair) => pair,
                    Err(e) => {
                        // A transient accept error (e.g. EMFILE) must not kill the
                        // server; log and keep accepting.
                        warn!(error = %e, "serve: accept error; continuing");
                        continue;
                    }
                };
                // Disable Nagle so SSE record/heartbeat frames flush immediately
                // (latency budget; ARCHITECTURE §9). Best-effort.
                if let Err(e) = stream.set_nodelay(true) {
                    trace!(error = %e, "serve: set_nodelay failed (continuing)");
                }

                let io = TokioIo::new(stream);
                // `TowerToHyperService<Router>` is `Clone` and implements
                // `hyper::service::Service<Request<Incoming>>`; clone it per
                // connection (the inner axum Router is cheap to clone).
                let svc = service.clone();
                let conn = builder.serve_connection_with_upgrades(io, svc);
                // Watch the connection so graceful shutdown can drive it to close.
                let fut = graceful.watch(conn.into_owned());
                tokio::spawn(async move {
                    if let Err(e) = fut.await {
                        // Connection-level errors (client reset, h2 GOAWAY, etc.)
                        // are normal and per-connection; never fatal.
                        trace!(peer = %peer, error = %e, "serve: connection ended with error");
                    }
                });
            }
        }
    }

    // Stop accepting new connections.
    drop(listener);

    // Tell every in-flight SSE stream to wind down + close (M11): without this an
    // idle/tailing watcher would hold its connection open and the graceful drain
    // would never complete on its own.
    sse_signal.trigger();

    // Bounded drain: race the graceful shutdown of in-flight connections against
    // a wall-clock budget. A connection that has not closed by the deadline is
    // abandoned (its task is detached) so a rolling restart cannot be pinned by a
    // wedged peer.
    let drain = graceful.shutdown();
    tokio::select! {
        () = drain => {
            debug!("serve: graceful shutdown complete");
        }
        () = tokio::time::sleep(DRAIN_TIMEOUT) => {
            warn!(
                timeout_ms = DRAIN_TIMEOUT.as_millis() as u64,
                "serve: drain timeout exceeded; abandoning remaining in-flight connections"
            );
        }
    }
    info!("serve: shutdown complete");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn shutdown_signal_wakes_waiters_and_is_sticky() {
        let sig = Arc::new(ShutdownSignal::new());
        assert!(!sig.is_shutting_down());

        // A waiter parked BEFORE trigger is woken by `notify_waiters`.
        let w = sig.clone();
        let parked = tokio::spawn(async move { w.notified().await });
        // Let the task reach the await point, then trigger.
        tokio::task::yield_now().await;
        sig.trigger();
        // Must resolve promptly (well under any drain budget).
        tokio::time::timeout(Duration::from_secs(1), parked)
            .await
            .expect("parked waiter must wake on trigger")
            .expect("join");

        assert!(sig.is_shutting_down(), "flag is latched (sticky)");

        // A waiter parking AFTER trigger returns immediately (does not miss the
        // one-shot `notify_waiters`).
        tokio::time::timeout(Duration::from_secs(1), sig.notified())
            .await
            .expect("a post-trigger waiter resolves at once");

        // `trigger` is idempotent.
        sig.trigger();
        assert!(sig.is_shutting_down());
    }
}
