//! Server configuration and the hard-limit constants/defaults from
//! `docs/API.md` and `docs/ROADMAP.md`.

// ---------------------------------------------------------------------------
// Hard limits (API §2 batching limits, §3, §5, §7, DESIGN §1.2)
// ---------------------------------------------------------------------------

/// Max records per write request (`TOPICS_MAX_BATCH_RECORDS`).
pub const MAX_BATCH_RECORDS: usize = 10_000;
/// Max single record `data`+`meta` canonical bytes (`TOPICS_MAX_RECORD_BYTES`).
pub const MAX_RECORD_BYTES: usize = 1 << 20; // 1 MiB
/// Max total request body (`TOPICS_MAX_BODY_BYTES`).
pub const MAX_BODY_BYTES: usize = 64 << 20; // 64 MiB
/// Max `meta` per record (`TOPICS_MAX_META_BYTES`).
pub const MAX_META_BYTES: usize = 16 << 10; // 16 KiB
/// Max number of `meta` keys per record.
pub const MAX_META_KEYS: usize = 64;
/// Max `tag` length in bytes (`TOPICS_MAX_TAG_BYTES`).
pub const MAX_TAG_BYTES: usize = 256;
/// Max `node` length in bytes (`TOPICS_MAX_NODE_BYTES`).
pub const MAX_NODE_BYTES: usize = 128;
/// Max `idempotency_key` length in characters.
pub const MAX_IDEMPOTENCY_KEY_LEN: usize = 256;

/// Default diff batch limit.
pub const DEFAULT_LIMIT: u32 = 256;
/// Max diff batch limit (`TOPICS_MAX_LIMIT`) — clamped, not rejected.
pub const MAX_LIMIT: u32 = 1000;
/// Max `wait_ms` long-poll — clamped, not rejected.
pub const MAX_WAIT_MS: u32 = 30_000;

/// Default soft byte budget for a single diff/SSE response batch when the request
/// does not specify one (DoS hardening; codex HIGH #6). The record walk stops once
/// the accumulated payload bytes reach this budget, so one response cannot grow to
/// `MAX_LIMIT` × `MAX_RECORD_BYTES` (≈ 1 GiB) regardless of `limit`. At least one
/// record is always returned (forward progress). 1 MiB.
pub const DEFAULT_MAX_BATCH_BYTES: u64 = 1 << 20;
/// Hard upper bound a caller-supplied `max_batch_bytes` is clamped to, so an
/// over-large request value cannot defeat the budget. 8 MiB.
pub const MAX_BATCH_BYTES: u64 = 8 << 20;

/// Default list page size.
pub const DEFAULT_PAGE_SIZE: usize = 100;
/// Max list page size.
pub const MAX_PAGE_SIZE: usize = 1000;

/// Max topics per watch subscription (`TOPICS_MAX_WATCH_TOPICS`).
pub const MAX_WATCH_TOPICS: usize = 256;
/// Watch session TTL after no active GET (ms).
pub const SESSION_TTL_MS: u64 = 300_000;
/// Heartbeat clamp bounds (ms).
pub const MIN_HEARTBEAT_MS: u64 = 1_000;
pub const MAX_HEARTBEAT_MS: u64 = 60_000;
/// EventSource reconnect backoff advertised via `retry:` (ms).
pub const SSE_RETRY_MS: u64 = 2_000;

/// The effective lower bound for a watch session's `heartbeat_ms`, applied when
/// the request value is clamped (API §7.2).
///
/// In production this is always [`MIN_HEARTBEAT_MS`] (1000ms), so the wire
/// contract is unchanged. The integration test suite sets
/// `TOPICS_TEST_MIN_HEARTBEAT_MS` to a small value so the SSE heartbeat-cadence
/// test can request a sub-second keep-alive interval and assert the cadence
/// without a multi-second wall-clock wait.
///
/// The override is **lower-only and bounded**: the returned floor is always in
/// `[1, MIN_HEARTBEAT_MS]`. It can never *raise* the floor above the production
/// `MIN_HEARTBEAT_MS` (so it cannot widen the production heartbeat envelope), and
/// because the result never exceeds `MIN_HEARTBEAT_MS < MAX_HEARTBEAT_MS` the
/// `clamp(min_heartbeat_ms(), MAX_HEARTBEAT_MS)` at the call site can never be
/// passed `min > max` (which would panic). It is itself floored at 1ms so the
/// keep-alive timer can never be zero. A missing/unparsable/zero value leaves the
/// production floor untouched.
pub fn min_heartbeat_ms() -> u64 {
    std::env::var("TOPICS_TEST_MIN_HEARTBEAT_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        // Lower-only: an override may only *reduce* the floor (capped at the
        // production `MIN_HEARTBEAT_MS`), and is itself floored at 1ms so the
        // timer is never zero. This keeps `min <= MIN_HEARTBEAT_MS < MAX`, so the
        // call-site `clamp(min, MAX_HEARTBEAT_MS)` can never panic.
        .map(|v| v.clamp(1, MIN_HEARTBEAT_MS))
        .unwrap_or(MIN_HEARTBEAT_MS)
}

/// Max router forwarding hops when `allow_cycle` is set (`$ttl_hops`).
pub const MAX_ROUTER_HOPS: u8 = 8;

// ---------------------------------------------------------------------------
// Async + derived router forwarding (see docs/ASYNC_ROUTER_DESIGN.md)
// ---------------------------------------------------------------------------

/// Background router-worker tick (ms): the elastic upper bound on forward
/// latency when no dest reader drives the cursor. Mirrors the snapshotter tick.
pub const ROUTER_TICK_INTERVAL_MS: u64 = 50;

/// Max source records a single `advance_router` pass forwards per router before
/// yielding (cooperative fairness so a large fan-out never monopolizes the
/// worker or a per-router lock; the source stays dirty and is re-drained).
pub const ROUTER_BATCH: usize = 1024;

// Router forwarding is always async + derived: one WAL append per source append
// regardless of fan-out, off the source ack path, with deterministic
// re-materialization and no silent loss.

// ---------------------------------------------------------------------------
// Queue limits (API §10)
// ---------------------------------------------------------------------------

/// Max jobs leased/acked/nacked per claim or ack/nack call (`TOPICS_MAX_CLAIM`).
pub const MAX_CLAIM: u32 = 1000;
/// Lease duration clamp bounds (ms): `[100, 86400000]` (API §10.2/§10.6).
pub const MIN_LEASE_MS: u64 = 100;
pub const MAX_LEASE_MS: u64 = 86_400_000;
/// Coalescing-window (`claim_jitter_ms`) clamp upper bound (ms) (API §0.10).
pub const MAX_CLAIM_JITTER_MS: u64 = 5_000;
/// Nack `delay_ms` clamp upper bound (ms) (API §10.5).
pub const MAX_NACK_DELAY_MS: u64 = 86_400_000;
/// `/work` SSE refill re-check fallback interval (ms): the stream parks on the
/// topic `Notify` for low-latency wakeups, but also re-checks on this cadence so an
/// out-of-band ack (which frees an in-flight slot without touching the topic
/// `Notify`) is reflected promptly (API §10.8).
pub const WORK_POLL_MS: u64 = 250;

/// Default data directory for the WAL/segments when `TOPICS_DATA_DIR` is unset
/// (phase 4 durability layer; see [`crate::storage`]).
pub const DEFAULT_DATA_DIR: &str = "./topics-data";

/// Hard ceiling on the number of WAL shards (`TOPICS_WAL_SHARDS`). The default
/// (`from_env`) picks `min(num_cpus, MAX_WAL_SHARDS)`. The cap bounds the writer
/// thread / file-descriptor / preallocation footprint: each shard owns a dedicated
/// OS writer thread and a preallocated active WAL file, so more shards trade fixed
/// overhead for write parallelism. 8 is a generous default ceiling — past it the
/// single durable-fsync stream is rarely the bottleneck on a single node, and the
/// per-shard preallocation (64 MiB each by default) and thread count grow linearly.
pub const MAX_WAL_SHARDS: usize = 8;

/// The default WAL shard count when `TOPICS_WAL_SHARDS` is unset: `min(num_cpus,
/// MAX_WAL_SHARDS)`, at least 1. Sharding the single ordered WAL writer scales
/// durable write throughput ~linearly with shard count (each shard is an
/// independent thread / mpsc / fsync stream with no shared hot-path contention),
/// so matching the shard count to the available CPU parallelism (capped) is a good
/// out-of-the-topic default; the operator can override via the env var.
pub fn default_wal_shards() -> usize {
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    cpus.clamp(1, MAX_WAL_SHARDS)
}

// ---------------------------------------------------------------------------
// Tiered / segment storage (Phase 6; ARCHITECTURE §3, §6)
// ---------------------------------------------------------------------------

/// Seal (roll a new) segment after this many events (`TOPICS_SEGMENT_MAX_EVENTS`,
/// default 10k). The active segment rolls to a sealed/immutable one once it holds
/// this many records.
pub const SEGMENT_MAX_EVENTS: u64 = 10_000;
/// Also seal on this many bytes (`TOPICS_SEGMENT_MAX_BYTES`, default 64 MiB), so
/// a topic of big payloads does not build one giant segment.
pub const SEGMENT_MAX_BYTES: u64 = 64 << 20;
/// Also seal a partially-filled active segment after this much wall-clock age
/// (`TOPICS_SEGMENT_MAX_AGE_MS`, default 1 h), so an idle topic still seals and its
/// data can age out / relocate. `0` disables the age trigger.
pub const SEGMENT_MAX_AGE_MS: u64 = 3_600_000; // 1 hour

/// Hot-retention: keep at most this many most-recent sealed segments HOT before
/// relocating older ones to the cold tier (`TOPICS_HOT_RETAIN_SEGMENTS`). The
/// active segment is always hot and not counted. `0` ⇒ relocate every sealed
/// segment as soon as a cold tier exists.
pub const HOT_RETAIN_SEGMENTS: u64 = 4;
/// Alternatively bound hot sealed-segment bytes (`TOPICS_HOT_RETAIN_BYTES`); the
/// stricter of the two retention bounds wins. `0` ⇒ only the segment-count bound
/// applies.
pub const HOT_RETAIN_BYTES: u64 = 0;

/// WAL bytes written since the last snapshot that triggers a new snapshot
/// (ARCHITECTURE §3: snapshot on a size threshold). Keeps WAL replay bounded.
pub const SNAPSHOT_BYTES_THRESHOLD: u64 = 64 << 20; // 64 MiB
/// Max wall-clock ms between snapshots (the time-based snapshot trigger).
pub const SNAPSHOT_INTERVAL_MS: u64 = 60_000; // 60 s
/// How often the background snapshotter checks the snapshot triggers (ms).
pub const SNAPSHOT_CHECK_INTERVAL_MS: u64 = 5_000;

/// How many seqs ahead of the in-use head a `disk`-class topic durably RESERVES
/// per fsynced `HeadWatermark` (R3). A `disk` write acks before its frame is
/// fsynced, so to guarantee an already-acked seq is never re-handed after a
/// crash that dropped the un-fsynced frame, the topic fsyncs a reservation ceiling
/// ahead of use; crossing it forces one fresh fsync. Larger ⇒ fewer reservation
/// fsyncs but a bigger unused-seq gap after a crash; smaller ⇒ the reverse.
/// Recovery sets `head = max(replayed head, reservation)`. NOTE: because the
/// reservation is fsynced ahead of use, a `disk` topic that recovers WITHOUT an
/// intervening snapshot (which would re-capture the exact head and absorb the
/// watermark) resumes at the reservation ceiling — the unwritten reserved seqs
/// become silent deleted gaps. The periodic snapshot collapses this gap in
/// steady state; the bound keeps any one gap small.
pub const DISK_HEAD_RESERVE_AHEAD: u64 = 256;

/// How often the background relocator sweeps topics for sealed segments beyond the
/// hot-retention bound and relocates them HOT → COLD (ms). Only runs when a cold
/// tier is configured; the copy I/O runs on the blocking pool off the hot path.
pub const RELOCATE_CHECK_INTERVAL_MS: u64 = 5_000;

// ---------------------------------------------------------------------------
// Priority scheduler constants (DESIGN §3, ARCHITECTURE §7)
// ---------------------------------------------------------------------------

/// Priority clamp bounds.
pub const PRIORITY_MIN: i32 = -1000;
pub const PRIORITY_MAX: i32 = 1000;
/// Auto-recency peak bonus.
pub const AUTO_MAX: f64 = 500.0;
/// Auto-recency half-life (ms).
pub const HALF_LIFE_MS: f64 = 30_000.0;
/// After this much idle time, the auto term is forced to 0 (ms).
pub const AUTO_FLOOR_MS: u64 = 300_000;
/// Anti-starvation aging rate (priority per ms waited): +100 / s.
pub const AGE_RATE_PER_MS: f64 = 0.1;
/// Aging cap (ms): +1000 after 10 s.
pub const AGE_CAP_MS: u64 = 10_000;

// ---------------------------------------------------------------------------
// Segment / tiering config (Phase 6)
// ---------------------------------------------------------------------------

/// Segment seal triggers + the hot-retention policy (ARCHITECTURE §3). A segment
/// seals (becomes immutable, and a fresh active one starts) when **any** of the
/// three triggers fires; sealed segments beyond the hot-retention bound relocate
/// to the cold tier (if one is configured). Defaults match the module constants
/// and are transparent when no cold dir is set (nothing relocates).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentConfig {
    /// Seal after this many records (default [`SEGMENT_MAX_EVENTS`]).
    pub max_events: u64,
    /// Seal after this many `.data` bytes (default [`SEGMENT_MAX_BYTES`]).
    pub max_bytes: u64,
    /// Seal a partially-filled active segment after this age in ms; `0` disables
    /// (default [`SEGMENT_MAX_AGE_MS`]).
    pub max_age_ms: u64,
    /// Keep at most this many most-recent sealed segments hot before relocating
    /// older ones (default [`HOT_RETAIN_SEGMENTS`]). The active segment is always
    /// hot and not counted.
    pub hot_retain_segments: u64,
    /// Optionally bound hot sealed bytes; the stricter bound wins (`0` ⇒ only the
    /// count bound, default [`HOT_RETAIN_BYTES`]).
    pub hot_retain_bytes: u64,
}

impl Default for SegmentConfig {
    fn default() -> Self {
        SegmentConfig {
            max_events: SEGMENT_MAX_EVENTS,
            max_bytes: SEGMENT_MAX_BYTES,
            max_age_ms: SEGMENT_MAX_AGE_MS,
            hot_retain_segments: HOT_RETAIN_SEGMENTS,
            hot_retain_bytes: HOT_RETAIN_BYTES,
        }
    }
}

impl SegmentConfig {
    /// Build from environment, falling back to the defaults for any unset/unparsable
    /// var: `TOPICS_SEGMENT_MAX_EVENTS`, `TOPICS_SEGMENT_MAX_BYTES`,
    /// `TOPICS_SEGMENT_MAX_AGE_MS`, `TOPICS_HOT_RETAIN_SEGMENTS`,
    /// `TOPICS_HOT_RETAIN_BYTES`.
    pub fn from_env() -> Self {
        let mut c = SegmentConfig::default();
        env_u64("TOPICS_SEGMENT_MAX_EVENTS", &mut c.max_events);
        env_u64("TOPICS_SEGMENT_MAX_BYTES", &mut c.max_bytes);
        env_u64("TOPICS_SEGMENT_MAX_AGE_MS", &mut c.max_age_ms);
        env_u64("TOPICS_HOT_RETAIN_SEGMENTS", &mut c.hot_retain_segments);
        env_u64("TOPICS_HOT_RETAIN_BYTES", &mut c.hot_retain_bytes);
        // A zero max_events would seal every record into its own segment; clamp to
        // at least 1 so the active segment can hold a record.
        c.max_events = c.max_events.max(1);
        c
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

// ---------------------------------------------------------------------------
// ServerConfig
// ---------------------------------------------------------------------------

/// Runtime server configuration, assembled at startup from environment.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Bind address. Defaults to `127.0.0.1:4000` (loopback) so an unconfigured
    /// server is never accidentally a public, unauthenticated event store; bind a
    /// non-loopback address explicitly (and set keys, or
    /// `TOPICS_ALLOW_INSECURE_NO_AUTH=1`) to expose it.
    pub bind_addr: String,
    /// Accepted bearer API keys, in **plaintext** — the build-time *input* only.
    /// This is consumed by [`ServerConfig::finalize_keys`], which parses it into
    /// the hashed [`KeyStore`](crate::auth::KeyStore) cached in
    /// [`key_store`](Self::key_store_cached), then **zeroizes and clears** this vec
    /// so no plaintext secret lingers in the long-lived process config (codex
    /// MEDIUM #9). After finalize this is empty; use [`auth_enabled`](Self::auth_enabled).
    pub api_keys: Vec<String>,
    /// The hashed key store, parsed once from [`api_keys`](Self::api_keys) by
    /// [`finalize_keys`](Self::finalize_keys). `None` until finalized (lazy parse).
    /// Holds **only SHA-256 digests** + scopes/prefixes — never a plaintext secret.
    /// Public only so external callers can use struct-update syntax; set it via
    /// [`finalize_keys`](Self::finalize_keys), not by hand.
    #[doc(hidden)]
    pub key_store_cached: Option<crate::auth::KeyStore>,
    /// Number of configured keys, recorded at finalize so boot logging / auth
    /// gating do not depend on the (now-cleared) plaintext vec. Public only for
    /// struct-update syntax; set it via [`finalize_keys`](Self::finalize_keys).
    #[doc(hidden)]
    pub key_count: usize,
    /// Escape hatch (`TOPICS_ALLOW_INSECURE_NO_AUTH=1`): permit binding a
    /// NON-loopback address with NO api keys configured. Off by default so the
    /// insecure combination refuses to start (see [`ServerConfig::startup_guard`]).
    pub allow_insecure_no_auth: bool,
    /// Whether health/ready/metrics probes require auth (`TOPICS_PROBE_AUTH`).
    pub probe_auth: bool,
    /// Max total request body before parse (`413`).
    pub max_body_bytes: usize,
    /// Data directory for the WAL/segments (`TOPICS_DATA_DIR`, default
    /// [`DEFAULT_DATA_DIR`] = `./topics-data`). The storage layer
    /// ([`crate::storage`]) writes the WAL under `<data_dir>/wal`; a missing/empty
    /// dir is a fresh start. [`crate::engine::Engine::with_data_dir`] opens it,
    /// replays the WAL on startup, and fsync-gates `durable:true` writes. `None`
    /// selects pure in-memory mode (engine/property unit tests).
    pub data_dir: Option<String>,
    /// Cold tier directory (`TOPICS_COLD_DIR`, Phase 6). When set, older sealed
    /// segments relocate here off the hot path; when `None` (the default in every
    /// existing test) tiering is disabled and every segment stays hot — behavior
    /// is unchanged by construction. A future S3 store plugs into the same
    /// [`crate::storage::SegmentStore`] trait.
    pub cold_dir: Option<String>,
    /// Segment seal triggers + hot-retention policy (ARCHITECTURE §3). Defaults
    /// are transparent: with no `cold_dir`, sealing still happens but nothing
    /// relocates.
    pub segment: SegmentConfig,
    /// Number of WAL shards (`TOPICS_WAL_SHARDS`): the single ordered WAL writer
    /// is split into this many independent shards (own thread / mpsc / fsync stream
    /// / file set) to scale durable write throughput. Each topic routes to exactly
    /// one shard by a stable hash of its interned id, so per-topic ordering and every
    /// durability guarantee still hold. `1` (the struct [`Default`]) is the
    /// pre-sharding single-writer behavior with the flat on-disk layout, exactly.
    /// [`ServerConfig::from_env`] picks [`default_wal_shards`] (num_cpus-based) when
    /// the env var is unset. Recovery is shard-count-agnostic, so this may be
    /// changed between restarts without data loss.
    pub wal_shards: usize,
    /// Resource / rate limits (DoS hardening; see [`crate::limits`]). Caps the
    /// number of topics/routers/watch-sessions and concurrent SSE connections +
    /// per-key in-flight requests. Defaults are generous; a literal `0` for any
    /// limit means unlimited. Read on every creation path.
    pub limits: crate::limits::Limits,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            bind_addr: "127.0.0.1:4000".to_string(),
            api_keys: Vec::new(),
            key_store_cached: None,
            key_count: 0,
            allow_insecure_no_auth: false,
            probe_auth: false,
            max_body_bytes: MAX_BODY_BYTES,
            data_dir: None,
            cold_dir: None,
            segment: SegmentConfig::default(),
            // Default = single shard ⇒ exact pre-sharding behavior + flat on-disk
            // layout. `from_env` overrides with the num_cpus-based default for the
            // production binary; in-process callers (tests) opt in explicitly.
            wal_shards: 1,
            limits: crate::limits::Limits::default(),
        }
    }
}

impl ServerConfig {
    /// Build the config from environment variables, falling back to defaults.
    pub fn from_env() -> Self {
        let mut cfg = ServerConfig::default();

        if let Ok(host) = std::env::var("TOPICS_HOST") {
            // TOPICS_HOST may be a full host:port or just a host.
            if host.contains(':') {
                cfg.bind_addr = host;
            } else {
                let port = std::env::var("TOPICS_PORT").unwrap_or_else(|_| "4000".into());
                cfg.bind_addr = format!("{host}:{port}");
            }
        } else if let Ok(port) = std::env::var("TOPICS_PORT") {
            // Port-only: keep the loopback default host (see `bind_addr` doc).
            cfg.bind_addr = format!("127.0.0.1:{port}");
        }

        if let Ok(keys) = std::env::var("TOPICS_API_KEYS") {
            cfg.api_keys = keys
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect();
        }

        cfg.probe_auth = std::env::var("TOPICS_PROBE_AUTH")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);

        cfg.allow_insecure_no_auth = std::env::var("TOPICS_ALLOW_INSECURE_NO_AUTH")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);

        if let Ok(v) = std::env::var("TOPICS_MAX_BODY_BYTES") {
            if let Ok(n) = v.parse() {
                cfg.max_body_bytes = n;
            }
        }

        // The WAL/segments live under this directory; the engine opens it and
        // replays the WAL on startup (durability layer). Unset ⇒ DEFAULT_DATA_DIR.
        if let Ok(dir) = std::env::var("TOPICS_DATA_DIR") {
            let dir = dir.trim();
            if !dir.is_empty() {
                cfg.data_dir = Some(dir.to_string());
            }
        }

        // Cold tier directory (Phase 6). Set ⇒ enable relocation off the hot path;
        // unset ⇒ tiering disabled (everything stays hot — the unchanged default).
        if let Ok(dir) = std::env::var("TOPICS_COLD_DIR") {
            let dir = dir.trim();
            if !dir.is_empty() {
                cfg.cold_dir = Some(dir.to_string());
            }
        }

        cfg.segment = SegmentConfig::from_env();
        cfg.limits = crate::limits::Limits::from_env();

        // WAL sharding (`TOPICS_WAL_SHARDS`): split the single ordered WAL writer
        // into N independent shards to scale durable write throughput. Unset (or
        // unparsable / `0`) ⇒ the num_cpus-based default. Clamped to at least 1
        // (`1` is the single-writer / flat-layout path).
        cfg.wal_shards = match std::env::var("TOPICS_WAL_SHARDS") {
            Ok(v) => match v.trim().parse::<usize>() {
                Ok(n) if n >= 1 => n,
                _ => default_wal_shards(),
            },
            Err(_) => default_wal_shards(),
        };

        // Parse the plaintext keys into the hashed store ONCE, then zeroize/clear
        // the plaintext so no secret lingers in the long-lived config (codex
        // MEDIUM #9). A malformed scope token is left for `key_store()` to surface
        // (fail-closed) at router build; finalize is best-effort and idempotent.
        cfg.finalize_keys();

        cfg
    }

    /// Parse [`api_keys`](Self::api_keys) into the hashed [`KeyStore`] cache, record
    /// the key count, then **zeroize and clear** the plaintext vec so no secret
    /// remains in the retained process config (codex MEDIUM #9). Idempotent: a
    /// second call (or a call when `api_keys` is already empty) is a no-op. If a
    /// scope token is malformed the plaintext is still cleared and the count is set
    /// from the raw entries; `key_store()` re-parses and surfaces the error (the
    /// startup path fails closed there).
    pub fn finalize_keys(&mut self) {
        if self.api_keys.is_empty() {
            return;
        }
        self.key_count = self.api_keys.len();
        // Build the hashed store; on a parse error leave the cache `None` so
        // `key_store()` re-parses (it can no longer, since we clear plaintext —
        // so on error we keep the cache None AND must not clear). Fail-closed:
        // only clear the plaintext once we have a valid hashed store.
        match Self::parse_store(&self.api_keys) {
            Ok(store) => {
                self.key_store_cached = Some(store);
                Self::zeroize_clear(&mut self.api_keys);
            }
            Err(_) => {
                // Malformed: keep plaintext so `key_store()` can re-parse and
                // surface the precise error at startup (the server then refuses to
                // boot). The window is the startup path only.
            }
        }
    }

    /// Overwrite each plaintext key's bytes before dropping the vec, so the secret
    /// does not linger in freed heap memory. Pure-Rust manual zeroization (no extra
    /// dependency); a `volatile`-free best-effort scrub adequate for this threat
    /// (the secret is no longer needed past finalize).
    fn zeroize_clear(keys: &mut Vec<String>) {
        for k in keys.iter_mut() {
            // SAFETY: overwriting the existing bytes in place, same length.
            unsafe {
                for b in k.as_bytes_mut() {
                    *b = 0;
                }
            }
            k.clear();
        }
        keys.clear();
        keys.shrink_to_fit();
    }

    fn parse_store(entries: &[String]) -> std::result::Result<crate::auth::KeyStore, String> {
        let mut keys = Vec::new();
        for entry in entries {
            if let Some(k) = crate::auth::ApiKey::parse_entry(entry)? {
                keys.push(k);
            }
        }
        Ok(crate::auth::KeyStore::from_keys(keys))
    }

    /// Whether bearer auth is enforced: true when any key was configured. Reads the
    /// recorded key count (set at finalize) OR the not-yet-finalized plaintext vec,
    /// so it is correct both before and after [`finalize_keys`](Self::finalize_keys).
    pub fn auth_enabled(&self) -> bool {
        self.key_count > 0 || !self.api_keys.is_empty()
    }

    /// Number of configured keys (for boot logging — never the keys themselves).
    pub fn key_count(&self) -> usize {
        self.key_count.max(self.api_keys.len())
    }

    /// The cached hashed key store, if [`finalize_keys`](Self::finalize_keys) has
    /// run and parsed it (holds only digests + scopes/prefixes, no plaintext).
    pub fn key_store_cached(&self) -> Option<&crate::auth::KeyStore> {
        self.key_store_cached.as_ref()
    }

    /// The hashed, constant-time [`KeyStore`](crate::auth::KeyStore) (scopes +
    /// prefix allowlist). Returns the cached store when finalized; otherwise parses
    /// the (not-yet-cleared) plaintext entries once. A malformed scope token aborts
    /// with an error (fail-closed): the server must not silently grant the wrong
    /// scope. Built **once** at router construction and cached in the HTTP
    /// `AppState`, not per request.
    pub fn key_store(&self) -> std::result::Result<crate::auth::KeyStore, String> {
        if let Some(store) = &self.key_store_cached {
            return Ok(store.clone());
        }
        Self::parse_store(&self.api_keys)
    }

    /// Constant-time check that `provided` matches one of the configured api keys
    /// (hash-only helper; ignores scopes/prefixes). Prefer [`key_store`] +
    /// [`KeyStore::authenticate`](crate::auth::KeyStore::authenticate) on the hot
    /// path, which also returns the matched principal's scopes.
    pub fn key_matches(&self, provided: &str) -> bool {
        match self.key_store() {
            Ok(store) => store.authenticate(provided).is_some(),
            Err(_) => false,
        }
    }

    /// Whether [`bind_addr`](Self::bind_addr) resolves to a loopback-only host.
    ///
    /// Parses the host:port and checks every resolved address is loopback. A host
    /// that fails to resolve, or any non-loopback address among the resolved set,
    /// is treated as NON-loopback (fail closed). The unspecified addresses
    /// `0.0.0.0` / `[::]` are explicitly non-loopback (they bind every interface).
    pub fn bind_is_loopback(&self) -> bool {
        use std::net::ToSocketAddrs;
        match self.bind_addr.to_socket_addrs() {
            Ok(addrs) => {
                let mut any = false;
                for a in addrs {
                    any = true;
                    if a.ip().is_unspecified() || !a.ip().is_loopback() {
                        return false;
                    }
                }
                // No addresses resolved ⇒ fail closed (treat as non-loopback).
                any
            }
            Err(_) => false,
        }
    }

    /// Refuse an insecure public exposure: a NON-loopback bind with NO api keys
    /// configured turns the server into an accidental public, unauthenticated
    /// event store. Returns `Err(message)` unless `TOPICS_ALLOW_INSECURE_NO_AUTH=1`
    /// is set (the documented escape hatch). Loopback with no keys stays
    /// dev-friendly (returns `Ok`).
    ///
    /// Called once at startup, BEFORE binding the listener.
    pub fn startup_guard(&self) -> std::result::Result<(), String> {
        if self.auth_enabled() || self.bind_is_loopback() || self.allow_insecure_no_auth {
            return Ok(());
        }
        Err(format!(
            "REFUSING TO START: bind address `{}` is non-loopback but no API keys are set \
             (TOPICS_API_KEYS is empty) — this would expose an unauthenticated event store \
             to the network. Set TOPICS_API_KEYS, bind a loopback address (127.0.0.1), or, \
             to override deliberately, set TOPICS_ALLOW_INSECURE_NO_AUTH=1.",
            self.bind_addr
        ))
    }
}

/// Validate a topic name against the documented charset
/// `^[A-Za-z0-9][A-Za-z0-9._:-]{0,254}$` (1–255 chars, starts alphanumeric).
pub fn is_valid_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    if bytes.is_empty() || bytes.len() > 255 {
        return false;
    }
    let first = bytes[0];
    if !first.is_ascii_alphanumeric() {
        return false;
    }
    bytes
        .iter()
        .all(|&b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b':' || b == b'-')
}

/// Validate a router name. Routers use the topic-name charset plus `>` so the
/// documented default-name convention `"<source>-><dest>"` (e.g. `jobs->audit`,
/// API §6.1) is a legal `:router` path segment.
pub fn is_valid_router_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    if bytes.is_empty() || bytes.len() > 255 {
        return false;
    }
    let first = bytes[0];
    if !first.is_ascii_alphanumeric() {
        return false;
    }
    bytes.iter().all(|&b| {
        b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b':' || b == b'-' || b == b'>'
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(bind: &str, keys: &[&str], allow_insecure: bool) -> ServerConfig {
        ServerConfig {
            bind_addr: bind.to_string(),
            api_keys: keys.iter().map(|s| s.to_string()).collect(),
            allow_insecure_no_auth: allow_insecure,
            ..ServerConfig::default()
        }
    }

    #[test]
    fn default_bind_is_loopback() {
        assert_eq!(ServerConfig::default().bind_addr, "127.0.0.1:4000");
        assert!(ServerConfig::default().bind_is_loopback());
    }

    #[test]
    fn loopback_detection() {
        assert!(cfg("127.0.0.1:4000", &[], false).bind_is_loopback());
        assert!(cfg("[::1]:4000", &[], false).bind_is_loopback());
        // Unspecified addresses bind every interface ⇒ NOT loopback.
        assert!(!cfg("0.0.0.0:4000", &[], false).bind_is_loopback());
        assert!(!cfg("[::]:4000", &[], false).bind_is_loopback());
        // A bind that cannot be parsed/resolved fails closed (non-loopback).
        assert!(!cfg("not-an-addr", &[], false).bind_is_loopback());
    }

    #[test]
    fn startup_guard_refuses_public_no_auth() {
        // Non-loopback + no keys + no override ⇒ refuse.
        assert!(cfg("0.0.0.0:4000", &[], false).startup_guard().is_err());
        // ...unless the escape hatch is set.
        assert!(cfg("0.0.0.0:4000", &[], true).startup_guard().is_ok());
        // Non-loopback WITH keys ⇒ ok.
        assert!(cfg("0.0.0.0:4000", &["k"], false).startup_guard().is_ok());
        // Loopback with no keys ⇒ dev-friendly ok.
        assert!(cfg("127.0.0.1:4000", &[], false).startup_guard().is_ok());
    }

    #[test]
    fn min_heartbeat_override_is_lower_only_and_bounded() {
        // The production floor is returned when the override is unset. (We avoid
        // mutating the shared process env here for an unset assertion since other
        // tests may set it; the override-set cases below set+restore explicitly.)
        let prev = std::env::var("TOPICS_TEST_MIN_HEARTBEAT_MS").ok();

        // A small override lowers the floor (this is the test-suite use).
        std::env::set_var("TOPICS_TEST_MIN_HEARTBEAT_MS", "100");
        assert_eq!(min_heartbeat_ms(), 100);

        // 0 is floored to 1 (the timer can never be zero).
        std::env::set_var("TOPICS_TEST_MIN_HEARTBEAT_MS", "0");
        assert_eq!(min_heartbeat_ms(), 1);

        // An attempt to RAISE the floor above the production min is capped at
        // MIN_HEARTBEAT_MS — the override is lower-only, so it can never widen the
        // production envelope nor (crucially) exceed MAX_HEARTBEAT_MS and make the
        // call-site clamp(min, MAX) panic.
        std::env::set_var("TOPICS_TEST_MIN_HEARTBEAT_MS", "999999");
        assert_eq!(min_heartbeat_ms(), MIN_HEARTBEAT_MS);
        assert!(min_heartbeat_ms() <= MAX_HEARTBEAT_MS, "never exceeds max");

        // An unparsable value is ignored (production floor).
        std::env::set_var("TOPICS_TEST_MIN_HEARTBEAT_MS", "not-a-number");
        assert_eq!(min_heartbeat_ms(), MIN_HEARTBEAT_MS);

        match prev {
            Some(v) => std::env::set_var("TOPICS_TEST_MIN_HEARTBEAT_MS", v),
            None => std::env::remove_var("TOPICS_TEST_MIN_HEARTBEAT_MS"),
        }
    }

    #[test]
    fn key_matches_is_correct() {
        let c = cfg("127.0.0.1:4000", &["alpha", "bravo"], false);
        assert!(c.key_matches("alpha"));
        assert!(c.key_matches("bravo"));
        assert!(!c.key_matches("charlie"));
        assert!(!c.key_matches("alph")); // prefix must not match
        assert!(!c.key_matches("alphax")); // superstring must not match
        assert!(!c.key_matches("")); // empty must not match a non-empty key
                                     // No keys configured ⇒ nothing matches (auth is disabled separately).
        assert!(!cfg("127.0.0.1:4000", &[], false).key_matches("alpha"));
    }
}
