//! streams — a persistent event engine (single binary).
//!
//! Phase 2 entrypoint: load config, init tracing, build the in-memory engine,
//! start the tokio + axum server, and shut down gracefully on a signal.

use std::sync::Arc;
use streams::clock::{SharedClock, SystemClock};
use streams::config::ServerConfig;
use streams::engine::Engine;
use streams::http;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();

    let config = ServerConfig::from_env();

    if config.auth_enabled() {
        info!(keys = config.api_keys.len(), "bearer auth enabled");
    } else {
        warn!("STREAMS_API_KEYS not set: AUTH IS DISABLED (single-tenant dev mode)");
    }

    let data_dir = config
        .data_dir
        .clone()
        .unwrap_or_else(|| streams::config::DEFAULT_DATA_DIR.to_string());
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
        boxes = engine.box_count(),
        recover_ms = recover_started.elapsed().as_millis() as u64,
        "recovery complete; readiness gate open (/v0/ready -> 200)"
    );

    // Background snapshotter: periodically checks the size/time snapshot triggers
    // and writes an atomic snapshot when due (keeping WAL replay bounded). The
    // capture+fsync is blocking, so it runs on the blocking pool.
    let snap_engine = engine.clone();
    let snapshotter = tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_millis(
            streams::config::SNAPSHOT_CHECK_INTERVAL_MS,
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

    // Background relocator (Phase 6): when a cold tier is configured, periodically
    // sweep boxes for sealed segments beyond the hot-retention bound and relocate
    // them HOT → COLD. The copy is blocking I/O, so it runs on the blocking pool —
    // off the hot path, never holding a box write lock or blocking SSE delivery
    // (the HARD INVARIANT). Disabled (the task simply never relocates) when no
    // cold dir is set, so the default path is unchanged.
    let relocator = if config.cold_dir.is_some() {
        let reloc_engine = engine.clone();
        Some(tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_millis(
                streams::config::RELOCATE_CHECK_INTERVAL_MS,
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

    let app = http::build_router(engine.clone());

    let listener = tokio::net::TcpListener::bind(&config.bind_addr).await?;
    info!(
        addr = %config.bind_addr,
        "streams listening (HTTP/1.1 keep-alive + HTTP/2 cleartext prior-knowledge)"
    );

    // Dual-protocol serve loop: auto-detects HTTP/1.1 vs HTTP/2-prior-knowledge
    // per connection (hyper-util auto::Builder) over the same listener, with the
    // tuned keep-alive/HTTP-2 settings and graceful drain (streams::serve).
    streams::serve::serve(listener, app, shutdown_signal()).await?;

    // Graceful shutdown: stop the background snapshotter + relocator and write a
    // final snapshot so a clean restart starts from a current checkpoint.
    snapshotter.abort();
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
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));
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
