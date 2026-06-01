//! streams — a persistent event engine (single binary), library crate.
//!
//! The binary (`src/main.rs`) is a thin wrapper that loads config, builds the
//! [`engine::Engine`], and serves [`http::build_router`]. Exposing the modules
//! as a library lets the integration suite (`tests/`) drive the exact same
//! in-process app via `tower::ServiceExt::oneshot` — no sockets, no sleeps.

pub mod auth;
pub mod clock;
pub mod config;
pub mod engine;
pub mod error;
pub mod http;
pub mod limits;
pub mod sched;
pub mod serve;
pub mod storage;

/// Shared test utilities for the crash/fault integration corpus (tiered
/// crash-point sweeps, …). Test-only: gated behind `cfg(test)` or the `test-fs`
/// feature, so a release build never compiles it.
#[cfg(any(test, feature = "test-fs"))]
pub mod testutil;

pub mod types;
