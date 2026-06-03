//! Resource & rate limits (DoS hardening).
//!
//! A small layer that caps the resources a single
//! server instance (and a single api key) can consume, so an unauthenticated dev
//! topic stays unbounded-friendly while a hardened deployment can refuse to be
//! exhausted. Every cap is configurable via an env var with a sane default, and
//! **`0` always means "unlimited"** (the explicit opt-out). The defaults are
//! generous enough that the existing conformance/integration suites — which
//! create only a handful of topics/routers/sessions/streams — never hit them, so
//! enabling this module changes no existing behavior.
//!
//! # What is capped
//!
//! | Limit | Env var | Default | Enforced on |
//! |---|---|---|---|
//! | Max topics | `TOPICS_MAX_TOPICS` | `100_000` | every topic creation ([`crate::engine::Engine::put_topic`], auto-create) |
//! | Max routers | `TOPICS_MAX_ROUTERS` | `10_000` | every router creation ([`crate::engine::Engine::put_router`]) |
//! | Max watch sessions | `TOPICS_MAX_WATCH_SESSIONS` | `10_000` | `POST /v0/watch` |
//! | Max SSE conns (global) | `TOPICS_MAX_SSE_CONNECTIONS` | `10_000` | every SSE stream GET (`/v0/watch/:wid`, `/v0/topics/:q/work`) |
//! | Max SSE conns / key | `TOPICS_MAX_SSE_CONNECTIONS_PER_KEY` | `1_000` | same, per authenticated key |
//! | Max in-flight requests / key | `TOPICS_MAX_INFLIGHT_PER_KEY` | `1_000` | every request (concurrency cap in the auth middleware) |
//!
//! # How it fails
//!
//! Creation paths that exceed a cap return **`429 throttled`** with a `Retry-After`
//! (capacity is a transient condition the client can retry after shedding load).
//! This reuses the existing elastic-throttle signal (API §0.6), so clients that
//! already handle `429` need no change.
//!
//! # How counts are tracked
//!
//! Topic/router counts are read directly from the engine's registries at creation
//! time (no separate counter to drift). Live SSE connections and per-key in-flight
//! requests are tracked in [`LiveCounts`] via atomic gauges and a small per-key
//! map, with **RAII guards** ([`SseGuard`], [`InflightGuard`]) that decrement on
//! drop — so a dropped/broken stream or a panicking handler can never leak a slot.

use crate::auth::KeyId;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Default cap on the number of topics (`TOPICS_MAX_TOPICS`). `0` ⇒ unlimited.
pub const DEFAULT_MAX_TOPICS: u64 = 100_000;
/// Default cap on the number of routers (`TOPICS_MAX_ROUTERS`). `0` ⇒ unlimited.
pub const DEFAULT_MAX_ROUTERS: u64 = 10_000;
/// Default cap on live watch sessions (`TOPICS_MAX_WATCH_SESSIONS`). `0` ⇒ unlimited.
pub const DEFAULT_MAX_WATCH_SESSIONS: u64 = 10_000;
/// Default cap on concurrent SSE connections, server-wide
/// (`TOPICS_MAX_SSE_CONNECTIONS`). `0` ⇒ unlimited.
pub const DEFAULT_MAX_SSE_CONNECTIONS: u64 = 10_000;
/// Default cap on concurrent SSE connections per api key
/// (`TOPICS_MAX_SSE_CONNECTIONS_PER_KEY`). `0` ⇒ unlimited.
pub const DEFAULT_MAX_SSE_CONNECTIONS_PER_KEY: u64 = 1_000;
/// Default cap on concurrent in-flight requests per api key
/// (`TOPICS_MAX_INFLIGHT_PER_KEY`). `0` ⇒ unlimited.
pub const DEFAULT_MAX_INFLIGHT_PER_KEY: u64 = 1_000;
/// Default cap on the **total retained record bytes** across all topics
/// (`TOPICS_MAX_TOTAL_BYTES`). `0` ⇒ unlimited (the default). Bounds disk/RAM growth from authenticated
/// writers (codex HIGH #5); when set, a write that would push the live total over
/// the cap is refused with `429 throttled` (the client can shed/delete and retry).
pub const DEFAULT_MAX_TOTAL_BYTES: u64 = 0;

/// `Retry-After` (seconds) advertised on a capacity `429`. Short — capacity frees
/// as soon as another client sheds a topic/session/connection.
pub const LIMIT_RETRY_AFTER_S: u64 = 1;

/// The configured resource/rate limits. Each field is a hard cap; **`0` means
/// unlimited** (the opt-out). Cloned into [`crate::config::ServerConfig`] and read
/// on every creation path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Limits {
    /// Max topics that may exist at once. `0` ⇒ unlimited.
    pub max_topics: u64,
    /// Max routers that may exist at once. `0` ⇒ unlimited.
    pub max_routers: u64,
    /// Max live watch sessions in the registry. `0` ⇒ unlimited.
    pub max_watch_sessions: u64,
    /// Max concurrent SSE connections server-wide. `0` ⇒ unlimited.
    pub max_sse_connections: u64,
    /// Max concurrent SSE connections per authenticated api key. `0` ⇒ unlimited.
    /// Has no effect in dev mode (no key to attribute connections to).
    pub max_sse_connections_per_key: u64,
    /// Max concurrent in-flight requests per authenticated api key (a simple
    /// concurrency / in-flight cap). `0` ⇒ unlimited. No effect in dev mode.
    pub max_inflight_per_key: u64,
    /// Max total retained record bytes across all topics (a global disk/RAM growth
    /// quota; codex HIGH #5). `0` ⇒ unlimited (default). When set, a write that
    /// would push the live total over the cap is refused with `429 throttled`.
    pub max_total_bytes: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            max_topics: DEFAULT_MAX_TOPICS,
            max_routers: DEFAULT_MAX_ROUTERS,
            max_watch_sessions: DEFAULT_MAX_WATCH_SESSIONS,
            max_sse_connections: DEFAULT_MAX_SSE_CONNECTIONS,
            max_sse_connections_per_key: DEFAULT_MAX_SSE_CONNECTIONS_PER_KEY,
            max_inflight_per_key: DEFAULT_MAX_INFLIGHT_PER_KEY,
            max_total_bytes: DEFAULT_MAX_TOTAL_BYTES,
        }
    }
}

impl Limits {
    /// Build from environment, falling back to the default for any unset/unparsable
    /// var. A literal `0` disables that specific limit (unlimited).
    pub fn from_env() -> Self {
        let mut l = Limits::default();
        env_u64("TOPICS_MAX_TOPICS", &mut l.max_topics);
        env_u64("TOPICS_MAX_ROUTERS", &mut l.max_routers);
        env_u64("TOPICS_MAX_WATCH_SESSIONS", &mut l.max_watch_sessions);
        env_u64("TOPICS_MAX_SSE_CONNECTIONS", &mut l.max_sse_connections);
        env_u64(
            "TOPICS_MAX_SSE_CONNECTIONS_PER_KEY",
            &mut l.max_sse_connections_per_key,
        );
        env_u64("TOPICS_MAX_INFLIGHT_PER_KEY", &mut l.max_inflight_per_key);
        env_u64("TOPICS_MAX_TOTAL_BYTES", &mut l.max_total_bytes);
        l
    }

    /// Whether a write of `incoming_bytes` is allowed given the `current_total`
    /// live bytes across all topics. `true` when the quota is unlimited (`0`) or the
    /// resulting total stays at/under the cap. A single write larger than the whole
    /// quota is allowed only when the topic is currently empty-enough; the engine
    /// applies this as a coarse admission guard (codex HIGH #5).
    pub fn total_bytes_ok(&self, current_total: u64, incoming_bytes: u64) -> bool {
        self.max_total_bytes == 0
            || current_total.saturating_add(incoming_bytes) <= self.max_total_bytes
    }

    /// Whether creating one more topic is allowed given the `current` topic count.
    /// `true` when the cap is unlimited (`0`) or `current` is below it.
    pub fn topic_ok(&self, current: u64) -> bool {
        self.max_topics == 0 || current < self.max_topics
    }

    /// Whether creating one more router is allowed given the `current` count.
    pub fn router_ok(&self, current: u64) -> bool {
        self.max_routers == 0 || current < self.max_routers
    }

    /// Whether creating one more watch session is allowed given the `current`
    /// session count.
    pub fn watch_session_ok(&self, current: u64) -> bool {
        self.max_watch_sessions == 0 || current < self.max_watch_sessions
    }
}

/// Parse a `u64` env var into `slot`, leaving it unchanged on absence/parse error.
fn env_u64(key: &str, slot: &mut u64) {
    if let Ok(v) = std::env::var(key) {
        if let Ok(n) = v.trim().parse::<u64>() {
            *slot = n;
        }
    }
}

/// Live, mutable per-instance counters for the limits that track *concurrent*
/// resource use (SSE connections, per-key in-flight requests). Shared via `Arc` in
/// the HTTP `AppState`. The topic/router/session caps are checked against the
/// authoritative registries directly and need no counter here.
///
/// All slots are acquired through RAII guards ([`SseGuard`], [`InflightGuard`]) so
/// they are always released — even if the guarded future is dropped (a broken SSE
/// pipe) or the handler panics.
#[derive(Debug, Default)]
pub struct LiveCounts {
    /// Server-wide live SSE connection gauge.
    sse_total: AtomicU64,
    /// Per-key live SSE connection gauge (keyed by the non-secret [`KeyId`]).
    sse_per_key: DashMap<KeyId, u64>,
    /// Per-key in-flight request gauge.
    inflight_per_key: DashMap<KeyId, u64>,
}

impl LiveCounts {
    pub fn new() -> Self {
        LiveCounts::default()
    }

    /// Current server-wide live SSE connection count (observability / tests).
    pub fn sse_total(&self) -> u64 {
        self.sse_total.load(Ordering::Relaxed)
    }

    /// Try to admit one more SSE connection under both the global cap and (when a
    /// key is given) the per-key cap, returning an [`SseGuard`] that releases the
    /// slot on drop. Returns `None` (caller should `429`) when either cap is hit.
    ///
    /// The two reservations are made together and **rolled back atomically** if the
    /// per-key cap rejects after the global slot was taken, so a rejected attempt
    /// never leaks a global slot.
    pub fn try_acquire_sse(
        self: &Arc<Self>,
        limits: &Limits,
        key: Option<KeyId>,
    ) -> Option<SseGuard> {
        // Reserve the global slot first (CAS loop so the cap is never exceeded
        // under concurrency).
        if limits.max_sse_connections != 0 {
            let mut cur = self.sse_total.load(Ordering::Relaxed);
            loop {
                if cur >= limits.max_sse_connections {
                    return None;
                }
                match self.sse_total.compare_exchange_weak(
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
            self.sse_total.fetch_add(1, Ordering::AcqRel);
        }

        // Then the per-key slot (only when authenticated). Roll back the global
        // reservation if this rejects.
        if let Some(k) = key {
            if !self.bump_per_key(&self.sse_per_key, k, limits.max_sse_connections_per_key) {
                self.sse_total.fetch_sub(1, Ordering::AcqRel);
                return None;
            }
        }

        Some(SseGuard {
            counts: self.clone(),
            key,
        })
    }

    /// Try to admit one more in-flight request for `key` under the per-key cap,
    /// returning an [`InflightGuard`] released on drop. Returns `None` (caller
    /// should `429`) when the cap is hit. Dev-mode (`key == None`) is never capped.
    pub fn try_acquire_inflight(
        self: &Arc<Self>,
        limits: &Limits,
        key: Option<KeyId>,
    ) -> Option<InflightGuard> {
        if let Some(k) = key {
            if !self.bump_per_key(&self.inflight_per_key, k, limits.max_inflight_per_key) {
                return None;
            }
            Some(InflightGuard {
                counts: self.clone(),
                key: k,
            })
        } else {
            // No key to attribute to (dev mode): no per-key cap.
            Some(InflightGuard {
                counts: self.clone(),
                key: KeyId([0u8; 32]),
            })
        }
    }

    /// Increment a per-key gauge under `cap` (`0` ⇒ unlimited). Returns `false`
    /// (leaving the gauge unchanged) when the cap would be exceeded. The whole
    /// read-modify-write is done under the entry's shard lock so it is atomic.
    fn bump_per_key(&self, map: &DashMap<KeyId, u64>, key: KeyId, cap: u64) -> bool {
        let mut entry = map.entry(key).or_insert(0);
        if cap != 0 && *entry >= cap {
            return false;
        }
        *entry += 1;
        true
    }

    /// Decrement a per-key gauge, removing the entry at zero so the map does not
    /// accumulate dead keys.
    fn drop_per_key(&self, map: &DashMap<KeyId, u64>, key: KeyId) {
        if let dashmap::mapref::entry::Entry::Occupied(mut e) = map.entry(key) {
            let v = e.get_mut();
            *v = v.saturating_sub(1);
            if *v == 0 {
                e.remove();
            }
        }
    }
}

/// RAII release of one SSE connection slot (global + per-key). Held inside the SSE
/// stream future; dropping it (clean close, broken pipe, or task cancel) frees the
/// slot immediately so a leaked connection can never permanently consume capacity.
pub struct SseGuard {
    counts: Arc<LiveCounts>,
    key: Option<KeyId>,
}

impl Drop for SseGuard {
    fn drop(&mut self) {
        self.counts.sse_total.fetch_sub(1, Ordering::AcqRel);
        if let Some(k) = self.key {
            self.counts.drop_per_key(&self.counts.sse_per_key, k);
        }
    }
}

/// RAII release of one per-key in-flight request slot. Held for the duration of a
/// request in the auth middleware; dropping it (response sent, or handler panic)
/// frees the slot.
pub struct InflightGuard {
    counts: Arc<LiveCounts>,
    key: KeyId,
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        // The dev-mode sentinel key was never inserted, so this is a no-op for it.
        self.counts
            .drop_per_key(&self.counts.inflight_per_key, self.key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kid(b: u8) -> KeyId {
        KeyId([b; 32])
    }

    #[test]
    fn zero_means_unlimited() {
        let l = Limits {
            max_topics: 0,
            max_routers: 0,
            max_watch_sessions: 0,
            ..Limits::default()
        };
        assert!(l.topic_ok(u64::MAX - 1));
        assert!(l.router_ok(u64::MAX - 1));
        assert!(l.watch_session_ok(u64::MAX - 1));
    }

    #[test]
    fn total_bytes_quota_admits_within_cap() {
        let l = Limits {
            max_total_bytes: 1000,
            ..Limits::default()
        };
        assert!(l.total_bytes_ok(0, 1000), "exactly the cap is allowed");
        assert!(l.total_bytes_ok(900, 100), "at the cap is allowed");
        assert!(!l.total_bytes_ok(900, 101), "over the cap is refused");
        // 0 ⇒ unlimited.
        let unlimited = Limits {
            max_total_bytes: 0,
            ..Limits::default()
        };
        assert!(unlimited.total_bytes_ok(u64::MAX - 1, u64::MAX - 1));
    }

    #[test]
    fn creation_caps_are_strict_less_than() {
        let l = Limits {
            max_topics: 2,
            max_routers: 1,
            max_watch_sessions: 3,
            ..Limits::default()
        };
        assert!(l.topic_ok(0));
        assert!(l.topic_ok(1));
        assert!(!l.topic_ok(2)); // at the cap: refuse the 3rd.
        assert!(!l.topic_ok(3));

        assert!(l.router_ok(0));
        assert!(!l.router_ok(1));

        assert!(l.watch_session_ok(2));
        assert!(!l.watch_session_ok(3));
    }

    #[test]
    fn sse_global_cap_blocks_and_releases() {
        let counts = Arc::new(LiveCounts::new());
        let l = Limits {
            max_sse_connections: 2,
            max_sse_connections_per_key: 0,
            ..Limits::default()
        };
        let g1 = counts.try_acquire_sse(&l, None).expect("1st admitted");
        let g2 = counts.try_acquire_sse(&l, None).expect("2nd admitted");
        assert_eq!(counts.sse_total(), 2);
        assert!(counts.try_acquire_sse(&l, None).is_none(), "3rd rejected");
        drop(g1);
        assert_eq!(counts.sse_total(), 1);
        // A slot freed up ⇒ a new connection is admitted.
        let g3 = counts
            .try_acquire_sse(&l, None)
            .expect("admitted after free");
        assert_eq!(counts.sse_total(), 2);
        drop(g2);
        drop(g3);
        assert_eq!(counts.sse_total(), 0);
    }

    #[test]
    fn sse_per_key_cap_is_independent_and_rolls_back_global() {
        let counts = Arc::new(LiveCounts::new());
        let l = Limits {
            max_sse_connections: 100,
            max_sse_connections_per_key: 1,
            ..Limits::default()
        };
        let a = kid(0xAA);
        let b = kid(0xBB);
        let _g1 = counts.try_acquire_sse(&l, Some(a)).expect("key a 1st");
        // Key a is at its per-key cap; the global slot must NOT leak on rejection.
        assert!(counts.try_acquire_sse(&l, Some(a)).is_none());
        assert_eq!(
            counts.sse_total(),
            1,
            "rejected per-key attempt rolled back global"
        );
        // A different key is unaffected.
        let _g2 = counts.try_acquire_sse(&l, Some(b)).expect("key b 1st");
        assert_eq!(counts.sse_total(), 2);
    }

    #[test]
    fn inflight_per_key_cap_blocks_and_releases() {
        let counts = Arc::new(LiveCounts::new());
        let l = Limits {
            max_inflight_per_key: 2,
            ..Limits::default()
        };
        let a = kid(1);
        let g1 = counts.try_acquire_inflight(&l, Some(a)).expect("1");
        let g2 = counts.try_acquire_inflight(&l, Some(a)).expect("2");
        assert!(
            counts.try_acquire_inflight(&l, Some(a)).is_none(),
            "3rd over cap"
        );
        drop(g1);
        let _g3 = counts
            .try_acquire_inflight(&l, Some(a))
            .expect("after free");
        drop(g2);
        // Dev mode (no key) is never capped.
        let l_dev = Limits {
            max_inflight_per_key: 1,
            ..Limits::default()
        };
        let _d1 = counts.try_acquire_inflight(&l_dev, None).unwrap();
        let _d2 = counts.try_acquire_inflight(&l_dev, None).unwrap();
    }
}
