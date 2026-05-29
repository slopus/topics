//! Server configuration and the hard-limit constants/defaults from
//! `docs/API.md` and `docs/ROADMAP.md`.

// ---------------------------------------------------------------------------
// Hard limits (API §2 batching limits, §3, §5, §7, DESIGN §1.2)
// ---------------------------------------------------------------------------

/// Max records per write request (`STREAMS_MAX_BATCH_RECORDS`).
pub const MAX_BATCH_RECORDS: usize = 10_000;
/// Max single record `data`+`meta` canonical bytes (`STREAMS_MAX_RECORD_BYTES`).
pub const MAX_RECORD_BYTES: usize = 1 << 20; // 1 MiB
/// Max total request body (`STREAMS_MAX_BODY_BYTES`).
pub const MAX_BODY_BYTES: usize = 64 << 20; // 64 MiB
/// Max `meta` per record (`STREAMS_MAX_META_BYTES`).
pub const MAX_META_BYTES: usize = 16 << 10; // 16 KiB
/// Max number of `meta` keys per record.
pub const MAX_META_KEYS: usize = 64;
/// Max `tag` length in bytes (`STREAMS_MAX_TAG_BYTES`).
pub const MAX_TAG_BYTES: usize = 256;
/// Max `node` length in bytes (`STREAMS_MAX_NODE_BYTES`).
pub const MAX_NODE_BYTES: usize = 128;
/// Max `idempotency_key` length in characters.
pub const MAX_IDEMPOTENCY_KEY_LEN: usize = 256;

/// Default diff batch limit.
pub const DEFAULT_LIMIT: u32 = 256;
/// Max diff batch limit (`STREAMS_MAX_LIMIT`) — clamped, not rejected.
pub const MAX_LIMIT: u32 = 1000;
/// Max `wait_ms` long-poll — clamped, not rejected.
pub const MAX_WAIT_MS: u32 = 30_000;

/// Default list page size.
pub const DEFAULT_PAGE_SIZE: usize = 100;
/// Max list page size.
pub const MAX_PAGE_SIZE: usize = 1000;

/// Max boxes per watch subscription (`STREAMS_MAX_WATCH_BOXES`).
pub const MAX_WATCH_BOXES: usize = 256;
/// Watch session TTL after no active GET (ms).
pub const SESSION_TTL_MS: u64 = 300_000;
/// Heartbeat clamp bounds (ms).
pub const MIN_HEARTBEAT_MS: u64 = 1_000;
pub const MAX_HEARTBEAT_MS: u64 = 60_000;
/// EventSource reconnect backoff advertised via `retry:` (ms).
pub const SSE_RETRY_MS: u64 = 2_000;

/// Max router forwarding hops when `allow_cycle` is set (`$ttl_hops`).
pub const MAX_ROUTER_HOPS: u8 = 8;

// ---------------------------------------------------------------------------
// Queue limits (API §10)
// ---------------------------------------------------------------------------

/// Max jobs leased/acked/nacked per claim or ack/nack call (`STREAMS_MAX_CLAIM`).
pub const MAX_CLAIM: u32 = 1000;
/// Lease duration clamp bounds (ms): `[100, 86400000]` (API §10.2/§10.6).
pub const MIN_LEASE_MS: u64 = 100;
pub const MAX_LEASE_MS: u64 = 86_400_000;
/// Coalescing-window (`claim_jitter_ms`) clamp upper bound (ms) (API §0.10).
pub const MAX_CLAIM_JITTER_MS: u64 = 5_000;
/// Nack `delay_ms` clamp upper bound (ms) (API §10.5).
pub const MAX_NACK_DELAY_MS: u64 = 86_400_000;
/// `/work` SSE refill re-check fallback interval (ms): the stream parks on the
/// box `Notify` for low-latency wakeups, but also re-checks on this cadence so an
/// out-of-band ack (which frees an in-flight slot without touching the box
/// `Notify`) is reflected promptly (API §10.8).
pub const WORK_POLL_MS: u64 = 250;

/// Default data directory for the WAL/segments when `STREAMS_DATA_DIR` is unset
/// (phase 4 durability layer; see [`crate::storage`]).
pub const DEFAULT_DATA_DIR: &str = "./streams-data";

// ---------------------------------------------------------------------------
// Tiered / segment storage (Phase 6; ARCHITECTURE §3, §6)
// ---------------------------------------------------------------------------

/// Seal (roll a new) segment after this many events (`STREAMS_SEGMENT_MAX_EVENTS`,
/// default 10k). The active segment rolls to a sealed/immutable one once it holds
/// this many records.
pub const SEGMENT_MAX_EVENTS: u64 = 10_000;
/// Also seal on this many bytes (`STREAMS_SEGMENT_MAX_BYTES`, default 64 MiB), so
/// a box of big payloads does not build one giant segment.
pub const SEGMENT_MAX_BYTES: u64 = 64 << 20;
/// Also seal a partially-filled active segment after this much wall-clock age
/// (`STREAMS_SEGMENT_MAX_AGE_MS`, default 1 h), so an idle box still seals and its
/// data can age out / relocate. `0` disables the age trigger.
pub const SEGMENT_MAX_AGE_MS: u64 = 3_600_000; // 1 hour

/// Hot-retention: keep at most this many most-recent sealed segments HOT before
/// relocating older ones to the cold tier (`STREAMS_HOT_RETAIN_SEGMENTS`). The
/// active segment is always hot and not counted. `0` ⇒ relocate every sealed
/// segment as soon as a cold tier exists.
pub const HOT_RETAIN_SEGMENTS: u64 = 4;
/// Alternatively bound hot sealed-segment bytes (`STREAMS_HOT_RETAIN_BYTES`); the
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

/// How often the background relocator sweeps boxes for sealed segments beyond the
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
    /// var: `STREAMS_SEGMENT_MAX_EVENTS`, `STREAMS_SEGMENT_MAX_BYTES`,
    /// `STREAMS_SEGMENT_MAX_AGE_MS`, `STREAMS_HOT_RETAIN_SEGMENTS`,
    /// `STREAMS_HOT_RETAIN_BYTES`.
    pub fn from_env() -> Self {
        let mut c = SegmentConfig::default();
        env_u64("STREAMS_SEGMENT_MAX_EVENTS", &mut c.max_events);
        env_u64("STREAMS_SEGMENT_MAX_BYTES", &mut c.max_bytes);
        env_u64("STREAMS_SEGMENT_MAX_AGE_MS", &mut c.max_age_ms);
        env_u64("STREAMS_HOT_RETAIN_SEGMENTS", &mut c.hot_retain_segments);
        env_u64("STREAMS_HOT_RETAIN_BYTES", &mut c.hot_retain_bytes);
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
    /// Bind address, e.g. `0.0.0.0:4000`.
    pub bind_addr: String,
    /// Accepted bearer API keys. Empty ⇒ auth disabled (dev mode).
    pub api_keys: Vec<String>,
    /// Whether health/ready/metrics probes require auth (`STREAMS_PROBE_AUTH`).
    pub probe_auth: bool,
    /// Max total request body before parse (`413`).
    pub max_body_bytes: usize,
    /// Data directory for the WAL/segments (`STREAMS_DATA_DIR`, default
    /// [`DEFAULT_DATA_DIR`] = `./streams-data`). The storage layer
    /// ([`crate::storage`]) writes the WAL under `<data_dir>/wal`; a missing/empty
    /// dir is a fresh start. [`crate::engine::Engine::with_data_dir`] opens it,
    /// replays the WAL on startup, and fsync-gates `durable:true` writes. `None`
    /// selects pure in-memory mode (engine/property unit tests).
    pub data_dir: Option<String>,
    /// Cold tier directory (`STREAMS_COLD_DIR`, Phase 6). When set, older sealed
    /// segments relocate here off the hot path; when `None` (the default in every
    /// existing test) tiering is disabled and every segment stays hot — behavior
    /// is unchanged by construction. A future S3 store plugs into the same
    /// [`crate::storage::SegmentStore`] trait.
    pub cold_dir: Option<String>,
    /// Segment seal triggers + hot-retention policy (ARCHITECTURE §3). Defaults
    /// are transparent: with no `cold_dir`, sealing still happens but nothing
    /// relocates.
    pub segment: SegmentConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            bind_addr: "0.0.0.0:4000".to_string(),
            api_keys: Vec::new(),
            probe_auth: false,
            max_body_bytes: MAX_BODY_BYTES,
            data_dir: None,
            cold_dir: None,
            segment: SegmentConfig::default(),
        }
    }
}

impl ServerConfig {
    /// Build the config from environment variables, falling back to defaults.
    pub fn from_env() -> Self {
        let mut cfg = ServerConfig::default();

        if let Ok(host) = std::env::var("STREAMS_HOST") {
            // STREAMS_HOST may be a full host:port or just a host.
            if host.contains(':') {
                cfg.bind_addr = host;
            } else {
                let port = std::env::var("STREAMS_PORT").unwrap_or_else(|_| "4000".into());
                cfg.bind_addr = format!("{host}:{port}");
            }
        } else if let Ok(port) = std::env::var("STREAMS_PORT") {
            cfg.bind_addr = format!("0.0.0.0:{port}");
        }

        if let Ok(keys) = std::env::var("STREAMS_API_KEYS") {
            cfg.api_keys = keys
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect();
        }

        cfg.probe_auth = std::env::var("STREAMS_PROBE_AUTH")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);

        if let Ok(v) = std::env::var("STREAMS_MAX_BODY_BYTES") {
            if let Ok(n) = v.parse() {
                cfg.max_body_bytes = n;
            }
        }

        // The WAL/segments live under this directory; the engine opens it and
        // replays the WAL on startup (durability layer). Unset ⇒ DEFAULT_DATA_DIR.
        if let Ok(dir) = std::env::var("STREAMS_DATA_DIR") {
            let dir = dir.trim();
            if !dir.is_empty() {
                cfg.data_dir = Some(dir.to_string());
            }
        }

        // Cold tier directory (Phase 6). Set ⇒ enable relocation off the hot path;
        // unset ⇒ tiering disabled (everything stays hot — the unchanged default).
        if let Ok(dir) = std::env::var("STREAMS_COLD_DIR") {
            let dir = dir.trim();
            if !dir.is_empty() {
                cfg.cold_dir = Some(dir.to_string());
            }
        }

        cfg.segment = SegmentConfig::from_env();

        cfg
    }

    /// Whether bearer auth is enforced.
    pub fn auth_enabled(&self) -> bool {
        !self.api_keys.is_empty()
    }
}

/// Validate a box name against the documented charset
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
    bytes.iter().all(|&b| {
        b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b':' || b == b'-'
    })
}

/// Validate a router name. Routers use the box-name charset plus `>` so the
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
        b.is_ascii_alphanumeric()
            || b == b'.'
            || b == b'_'
            || b == b':'
            || b == b'-'
            || b == b'>'
    })
}
