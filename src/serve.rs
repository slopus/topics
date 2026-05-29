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

use axum::Router;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto;
use hyper_util::server::graceful::GracefulShutdown;
use hyper_util::service::TowerToHyperService;
use tokio::net::TcpListener;
use tracing::{debug, trace, warn};

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
pub async fn serve<S>(listener: TcpListener, app: Router, shutdown: S) -> std::io::Result<()>
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

    // Stop accepting and wait for in-flight connections (incl. SSE) to wind down.
    drop(listener);
    graceful.shutdown().await;
    debug!("serve: graceful shutdown complete");
    Ok(())
}
