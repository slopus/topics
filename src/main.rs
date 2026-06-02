//! topics — a persistent event engine (single binary).
//!
//! Entrypoint: load config, enforce the no-auth startup guard, init tracing,
//! build the durable engine (open the data dir, load the latest snapshot, replay
//! the WAL forward before serving), start the tokio + axum server with the
//! background snapshotter/relocator, and shut down gracefully on a signal
//! (writing a final snapshot).

use std::sync::Arc;
use topics::clock::{SharedClock, SystemClock};
use topics::config::ServerConfig;
use topics::engine::Engine;
use topics::http;
use tracing::{error, info, warn};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();

    let config = ServerConfig::from_env();

    // Fail closed BEFORE binding: a non-loopback bind with no api keys is an
    // accidental public, unauthenticated event store. `startup_guard` permits it
    // only with TOPICS_ALLOW_INSECURE_NO_AUTH=1 (logged loudly below).
    if let Err(msg) = config.startup_guard() {
        error!("{msg}");
        return Err(msg.into());
    }

    if config.auth_enabled() {
        info!(
            keys = config.key_count(),
            bind = %config.bind_addr,
            "bearer auth enabled (constant-time key comparison)"
        );
    } else if config.bind_is_loopback() {
        warn!(
            bind = %config.bind_addr,
            "TOPICS_API_KEYS not set: AUTH IS DISABLED (single-tenant dev mode, loopback-only)"
        );
    } else {
        // Reached only via the explicit TOPICS_ALLOW_INSECURE_NO_AUTH escape hatch.
        warn!(
            bind = %config.bind_addr,
            "INSECURE: AUTH IS DISABLED on a NON-LOOPBACK bind (TOPICS_ALLOW_INSECURE_NO_AUTH=1) \
             — this server is reachable on the network with NO authentication"
        );
    }

    // Resource/rate limits (DoS hardening). A `0` for any limit means unlimited;
    // the defaults are generous. Logged so an operator can see the active caps.
    let lim = &config.limits;
    info!(
        max_topics = lim.max_topics,
        max_routers = lim.max_routers,
        max_watch_sessions = lim.max_watch_sessions,
        max_sse_connections = lim.max_sse_connections,
        max_sse_connections_per_key = lim.max_sse_connections_per_key,
        max_inflight_per_key = lim.max_inflight_per_key,
        "resource limits active (0 = unlimited)"
    );

    let data_dir = config
        .data_dir
        .clone()
        .unwrap_or_else(|| topics::config::DEFAULT_DATA_DIR.to_string());
    info!(data_dir = %data_dir, "durable mode: WAL under <data_dir>/wal (replayed on start)");

    let clock: SharedClock = Arc::new(SystemClock);
    // Durable engine: opens/creates the data dir, loads the latest valid
    // snapshot, replays the WAL forward from its checkpoint (truncating any torn
    // tail), and resumes the writer — all BEFORE this returns. The engine starts
    // NOT ready and flips to ready only after that recovery completes, so the
    // readiness gate (`/v0/ready`) is 503 during replay and 200 after. Durable
    // writes are fsync-gated.
    let recover_started = std::time::Instant::now();
    let engine = Engine::with_data_dir(config.clone(), clock)?;
    info!(
        ready = engine.is_ready(),
        topics = engine.topic_count(),
        recover_ms = recover_started.elapsed().as_millis() as u64,
        "recovery complete; readiness gate open (/v0/ready -> 200)"
    );

    // Background snapshotter: periodically checks the size/time snapshot triggers
    // and writes an atomic snapshot when due (keeping WAL replay bounded). The
    // capture+fsync is blocking, so it runs on the blocking pool.
    let snap_engine = engine.clone();
    let snapshotter = tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_millis(
            topics::config::SNAPSHOT_CHECK_INTERVAL_MS,
        ));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            if snap_engine.snapshot_due() {
                let e = snap_engine.clone();
                let res = tokio::task::spawn_blocking(move || e.write_snapshot()).await;
                match res {
                    Ok(Ok(true)) => info!("periodic snapshot written"),
                    Ok(Ok(false)) => {}
                    Ok(Err(err)) => warn!(error = %err, "periodic snapshot failed"),
                    Err(join) => warn!(error = %join, "snapshot task panicked"),
                }
            }
        }
    });

    // Background router worker (async/derived forwarding, `TOPICS_FORWARD_V2`):
    // drains dirty router sources off the write/ack path so forwarding progresses
    // even with no dest reader (the read-path catch-up handles read-your-writes; this
    // bounds worst-case latency and frees the ack path entirely). Elastic tick;
    // `drain_router_sources` is a no-op when v2 is off, so the default path pays
    // nothing. Runs on the blocking pool (it may do segment I/O on dest seals).
    let router_engine = engine.clone();
    let router_worker = tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_millis(
            topics::config::ROUTER_TICK_INTERVAL_MS,
        ));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            let e = router_engine.clone();
            // Loop the drain until a pass makes no progress, so a large fan-out is
            // fully worked between ticks (each pass is bounded by ROUTER_BATCH).
            let _ = tokio::task::spawn_blocking(move || {
                let mut total = 0u64;
                loop {
                    let n = e.drain_router_sources();
                    total += n;
                    if n == 0 {
                        break;
                    }
                }
                total
            })
            .await;
        }
    });

    // Background relocator (Phase 6): when a cold tier is configured, periodically
    // sweep topics for sealed segments beyond the hot-retention bound and relocate
    // them HOT → COLD. The copy is blocking I/O, so it runs on the blocking pool —
    // off the hot path, never holding a topic write lock or blocking SSE delivery
    // (the HARD INVARIANT). Disabled (the task simply never relocates) when no
    // cold dir is set, so the default path is unchanged.
    let relocator = if config.cold_dir.is_some() {
        let reloc_engine = engine.clone();
        Some(tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_millis(
                topics::config::RELOCATE_CHECK_INTERVAL_MS,
            ));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                let e = reloc_engine.clone();
                match tokio::task::spawn_blocking(move || e.relocate_all_due()).await {
                    Ok(n) if n > 0 => info!(segments = n, "relocated sealed segments hot→cold"),
                    Ok(_) => {}
                    Err(join) => warn!(error = %join, "relocator task panicked"),
                }
            }
        }))
    } else {
        None
    };

    // Build the router, parsing TOPICS_API_KEYS into the hashed key store. A
    // malformed scope token fails closed here (rather than booting with auth
    // silently degraded) with a clear message. The shutdown signal is shared with
    // the serve loop so in-flight SSE streams are wound down on shutdown (M11).
    let sse_shutdown = std::sync::Arc::new(topics::serve::ShutdownSignal::new());
    let app = match http::build_router_with_shutdown(engine.clone(), sse_shutdown.clone()) {
        Ok(app) => app,
        Err(msg) => {
            error!("invalid TOPICS_API_KEYS: {msg}");
            return Err(msg.into());
        }
    };

    let listener = tokio::net::TcpListener::bind(&config.bind_addr).await?;
    // The *actual* bound address. When `TOPICS_PORT=0` (ephemeral) the OS picks a
    // free port, so this differs from `config.bind_addr`; everything below logs and
    // reports the resolved address.
    let local_addr = listener.local_addr()?;
    info!(
        addr = %local_addr,
        "topics listening (HTTP/1.1 keep-alive + HTTP/2 cleartext prior-knowledge)"
    );

    // Test/automation hook (`TOPICS_PORT_FILE`): once the listener is bound, write
    // the resolved `host:port` to this file ATOMICALLY (write a sibling temp file,
    // then rename). This lets a parent process spawn the server with `TOPICS_PORT=0`
    // and learn the OS-assigned port WITHOUT the reserve-a-port-then-release race
    // (the child holds the bound socket the whole time, so nothing else can steal
    // the port between reservation and bind). No-op when unset, so production is
    // unaffected. The atomic rename means a reader never observes a partial address.
    if let Ok(path) = std::env::var("TOPICS_PORT_FILE") {
        if !path.trim().is_empty() {
            let tmp = format!("{path}.tmp.{}", std::process::id());
            if let Err(e) = std::fs::write(&tmp, local_addr.to_string())
                .and_then(|()| std::fs::rename(&tmp, &path))
            {
                error!(error = %e, path = %path, "failed to write TOPICS_PORT_FILE");
                let _ = std::fs::remove_file(&tmp);
                return Err(e.into());
            }
        }
    }

    // Dual-protocol serve loop: auto-detects HTTP/1.1 vs HTTP/2-prior-knowledge
    // per connection (hyper-util auto::Builder) over the same listener, with the
    // tuned keep-alive/HTTP-2 settings and graceful drain (topics::serve).
    topics::serve::serve_with_signal(listener, app, shutdown_signal(), sse_shutdown).await?;

    // Graceful shutdown: stop the background snapshotter + relocator + router worker
    // and write a final snapshot so a clean restart starts from a current
    // checkpoint. Do one FINAL router drain first so any forwarding that the worker
    // had not yet picked up is reflected in the cursors the final snapshot captures
    // (no-op when v2 is off).
    snapshotter.abort();
    router_worker.abort();
    {
        let e = engine.clone();
        let _ = tokio::task::spawn_blocking(move || loop {
            if e.drain_router_sources() == 0 {
                break;
            }
        })
        .await;
    }
    if let Some(relocator) = relocator {
        relocator.abort();
    }
    let snap_engine = engine.clone();
    match tokio::task::spawn_blocking(move || snap_engine.write_snapshot()).await {
        Ok(Ok(true)) => info!("shutdown snapshot written"),
        Ok(Ok(false)) => {}
        Ok(Err(err)) => warn!(error = %err, "shutdown snapshot failed"),
        Err(join) => warn!(error = %join, "shutdown snapshot task panicked"),
    }

    info!("shutdown complete");
    Ok(())
}

/// Initialize the tracing subscriber from `RUST_LOG` (default `info`).
fn init_tracing() {
    use tracing_subscriber::{fmt, EnvFilter};
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).init();
}

/// Resolve when the process receives Ctrl-C or SIGTERM.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl-C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("shutdown signal received; draining");
}
