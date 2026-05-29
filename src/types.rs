//! Shared wire/domain types for the `/v0` API.
//!
//! These types define the JSON contract exactly as specified in `docs/API.md`.
//! Server-computed per-record metadata uses `$`-prefixed keys (`$seq`, `$ts`,
//! `$node`, `$tag`, `$type`); user namespaces (`data`, `meta`) pass through
//! verbatim. Absent optional fields are omitted (absence, not `null`).

use serde::{Deserialize, Serialize};
use serde_json::Value;

// ---------------------------------------------------------------------------
// Box config (API §0.10)
// ---------------------------------------------------------------------------

/// The Box config object. Appears in box-create requests and box-state
/// responses. All fields are optional on create; omitted fields take the
/// documented default (via `#[serde(default)]`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BoxConfig {
    /// Box kind: `"log"` (default, plain append-only log) or `"queue"` (enables
    /// the lease-based claim/ack/nack/extend/work endpoints, API §10). Immutable
    /// after create (a `PUT` changing it ⇒ `409 box_exists_incompatible`).
    #[serde(default = "default_box_type")]
    pub r#type: BoxType,
    #[serde(default = "default_ttl_ms")]
    pub ttl_ms: u64,
    #[serde(default = "default_cap_records")]
    pub cap_records: u64,
    #[serde(default = "default_cap_bytes")]
    pub cap_bytes: u64,
    #[serde(default = "default_discard")]
    pub discard: Discard,
    #[serde(default = "default_durable")]
    pub durable: bool,
    #[serde(default = "default_priority")]
    pub priority: Option<i32>,
    #[serde(default = "default_auto_priority")]
    pub auto_priority: bool,
    #[serde(default = "default_auto_create")]
    pub auto_create: bool,
    #[serde(default = "default_idempotency_window_ms")]
    pub idempotency_window_ms: u64,
    #[serde(default = "default_dedupe_node")]
    pub dedupe_node: bool,

    // --- Queue-only config (meaningful only when `type:"queue"`; accepted but
    // inert on a "log" box). DESIGN §10, API §0.10/§10. ---
    /// Default lease (visibility-timeout) duration for a claim, ms. Clamped
    /// `[100, 86400000]`.
    #[serde(default = "default_lease_ms")]
    pub lease_ms: u64,
    /// Coalescing-window width, ms. `0` = greedy (serve each claim immediately);
    /// `>0` = gather the cohort and divide jobs evenly (API §10.2). Clamped
    /// `[0, 5000]`.
    #[serde(default = "default_claim_jitter_ms")]
    pub claim_jitter_ms: u64,
    /// After this many deliveries without an ack, dead-letter the job (§10.6).
    /// `0` = unlimited redelivery (never dead-letter on delivery count).
    #[serde(default = "default_max_deliveries")]
    pub max_deliveries: u64,
    /// Box to move a job to after it exceeds `max_deliveries` (§10.6). `null` =
    /// no dead-letter box (the job keeps being reclaimed).
    #[serde(default = "default_dead_letter")]
    pub dead_letter: Option<String>,
    /// Durability of the *leases* log (§10.1). Defaults `false` (self-healing:
    /// all in-flight jobs become claimable on restart).
    #[serde(default = "default_leases_durable")]
    pub leases_durable: bool,
}

fn default_box_type() -> BoxType {
    BoxType::Log
}
fn default_lease_ms() -> u64 {
    30_000
}
fn default_claim_jitter_ms() -> u64 {
    0
}
fn default_max_deliveries() -> u64 {
    0
}
fn default_dead_letter() -> Option<String> {
    None
}
fn default_leases_durable() -> bool {
    false
}

fn default_ttl_ms() -> u64 {
    0
}
fn default_cap_records() -> u64 {
    0
}
fn default_cap_bytes() -> u64 {
    0
}
fn default_discard() -> Discard {
    Discard::Old
}
fn default_durable() -> bool {
    false
}
fn default_priority() -> Option<i32> {
    None
}
fn default_auto_priority() -> bool {
    true
}
fn default_auto_create() -> bool {
    true
}
fn default_idempotency_window_ms() -> u64 {
    120_000
}
fn default_dedupe_node() -> bool {
    true
}

impl Default for BoxConfig {
    fn default() -> Self {
        BoxConfig {
            r#type: default_box_type(),
            ttl_ms: default_ttl_ms(),
            cap_records: default_cap_records(),
            cap_bytes: default_cap_bytes(),
            discard: default_discard(),
            durable: default_durable(),
            priority: default_priority(),
            auto_priority: default_auto_priority(),
            auto_create: default_auto_create(),
            idempotency_window_ms: default_idempotency_window_ms(),
            dedupe_node: default_dedupe_node(),
            lease_ms: default_lease_ms(),
            claim_jitter_ms: default_claim_jitter_ms(),
            max_deliveries: default_max_deliveries(),
            dead_letter: default_dead_letter(),
            leases_durable: default_leases_durable(),
        }
    }
}

impl BoxConfig {
    /// Whether this box is a queue (enables the §10 lease lifecycle).
    pub fn is_queue(&self) -> bool {
        self.r#type == BoxType::Queue
    }
}

/// The kind of a box: a plain append-only log, or a lease-based job queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BoxType {
    /// Plain append-only log (default). Rejects the §10 queue endpoints.
    Log,
    /// Lease-based job queue (enables claim/ack/nack/extend/work, §10).
    Queue,
}

/// Full-box overflow policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Discard {
    /// Evict oldest records to make room (pub/sub friendly). Default.
    Old,
    /// Refuse the write with `422 box_full`.
    Reject,
}

// ---------------------------------------------------------------------------
// Records (DESIGN §1, API §0.4)
// ---------------------------------------------------------------------------

/// An input record as supplied by a writer on `POST /v0/boxes/:box`.
/// `node`/`tag` are plain top-level keys (no sigil on write).
#[derive(Debug, Clone, Deserialize)]
pub struct RecordIn {
    /// Opaque payload; may be JSON `null`. Required.
    pub data: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<Value>,
}

/// A record as returned on a read. Server fields are `$`-prefixed. `$node`,
/// `$tag`, `meta` are omitted when absent; `data` is always present.
#[derive(Debug, Clone, Serialize)]
pub struct RecordOut {
    #[serde(rename = "$seq")]
    pub seq: u64,
    #[serde(rename = "$ts")]
    pub ts: i64,
    #[serde(rename = "$node", skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    #[serde(rename = "$tag", skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    /// `"tombstone"` for tombstone frames; omitted for plain records.
    #[serde(rename = "$type", skip_serializing_if = "Option::is_none")]
    pub type_: Option<String>,
    pub data: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<Value>,
}

// ---------------------------------------------------------------------------
// Box lifecycle: PUT / GET / list / DELETE (API §1)
// ---------------------------------------------------------------------------

/// Response for `PUT /v0/boxes/:box`.
#[derive(Debug, Clone, Serialize)]
pub struct BoxCreateResponse {
    #[serde(rename = "box")]
    pub box_name: String,
    pub created: bool,
    pub config: BoxConfig,
    pub performance: Performance,
}

/// Response for `GET /v0/boxes/:box` (box state).
#[derive(Debug, Clone, Serialize)]
pub struct BoxStateResponse {
    #[serde(rename = "box")]
    pub box_name: String,
    /// Box kind (`"queue"` for a queue, `"log"` otherwise) — API §10.7.
    pub r#type: BoxType,
    pub head_seq: u64,
    pub earliest_seq: u64,
    pub next_seq: u64,
    pub count: u64,
    pub bytes: u64,
    pub config: BoxConfig,
    pub effective_priority: i64,
    pub last_write_ts: Option<i64>,
    pub last_read_ts: Option<i64>,
    /// Queue counters (`ready`/`in_flight`/`dead_lettered`) — present only for a
    /// queue box (API §10.7); omitted on a plain log.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue: Option<QueueState>,
    pub performance: Performance,
}

/// One entry in the `GET /v0/boxes` listing.
#[derive(Debug, Clone, Serialize)]
pub struct BoxSummary {
    #[serde(rename = "box")]
    pub box_name: String,
    pub head_seq: u64,
    pub earliest_seq: u64,
    pub count: u64,
    pub bytes: u64,
    pub durable: bool,
    pub effective_priority: i64,
}

/// Response for `GET /v0/boxes`.
#[derive(Debug, Clone, Serialize)]
pub struct BoxListResponse {
    pub boxes: Vec<BoxSummary>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    pub performance: Performance,
}

/// Response for `DELETE /v0/boxes/:box`.
#[derive(Debug, Clone, Serialize)]
pub struct BoxDeleteResponse {
    #[serde(rename = "box")]
    pub box_name: String,
    pub deleted: bool,
    pub routers_removed: Vec<String>,
    pub performance: Performance,
}

// ---------------------------------------------------------------------------
// Write (API §2)
// ---------------------------------------------------------------------------

/// Request body for `POST /v0/boxes/:box`.
#[derive(Debug, Clone, Deserialize)]
pub struct WriteRequest {
    pub records: Vec<RecordIn>,
    #[serde(default)]
    pub node: Option<String>,
    #[serde(default)]
    pub idempotency_key: Option<String>,
    /// Overrides the box's `auto_create` for this write only.
    #[serde(default)]
    pub create: Option<bool>,
    /// Applied only if this write creates the box.
    #[serde(default)]
    pub config: Option<BoxConfig>,
    #[serde(default)]
    pub disable_backpressure: bool,
}

/// Response for `POST /v0/boxes/:box`.
#[derive(Debug, Clone, Serialize)]
pub struct WriteResponse {
    #[serde(rename = "box")]
    pub box_name: String,
    pub first_seq: u64,
    pub last_seq: u64,
    /// Suppressed with `?return_seqs=false`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seqs: Option<Vec<u64>>,
    pub head_seq: u64,
    pub count: u64,
    pub created: bool,
    pub deduped: bool,
    pub performance: Performance,
}

// ---------------------------------------------------------------------------
// Diff / getDifference (API §3)
// ---------------------------------------------------------------------------

/// Filter for the `node` field: a single id or a set of ids.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum NodeFilter {
    One(String),
    Many(Vec<String>),
}

impl NodeFilter {
    /// Returns true if the given `$node` value is in the filter set.
    pub fn matches(&self, node: &str) -> bool {
        match self {
            NodeFilter::One(s) => s == node,
            NodeFilter::Many(v) => v.iter().any(|s| s == node),
        }
    }
}

/// Request body for `POST /v0/boxes/:box/diff`.
#[derive(Debug, Clone, Deserialize)]
pub struct DiffRequest {
    #[serde(default)]
    pub from_seq: u64,
    #[serde(default)]
    pub limit: u32,
    #[serde(default)]
    pub node: Option<NodeFilter>,
    #[serde(default)]
    pub include_tags: bool,
    #[serde(default = "default_include_meta")]
    pub include_meta: bool,
    #[serde(default)]
    pub wait_ms: u32,
}

fn default_include_meta() -> bool {
    true
}

impl Default for DiffRequest {
    fn default() -> Self {
        DiffRequest {
            from_seq: 0,
            limit: 0,
            node: None,
            include_tags: false,
            include_meta: default_include_meta(),
            wait_ms: 0,
        }
    }
}

/// In-band gap marker (API §3.3, DESIGN §5.4). Never an error.
#[derive(Debug, Clone, Serialize)]
pub struct Tombstone {
    pub gap_from: u64,
    pub gap_to: u64,
    pub reason: TombstoneReason,
    pub missed_estimate: u64,
    pub earliest_seq: u64,
    pub head_seq: u64,
}

/// Why a gap exists. Informational; the gap range is authoritative.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TombstoneReason {
    Cap,
    Ttl,
    Mixed,
    Recreated,
    /// SSE connect-time variant only.
    FromSeqTooOld,
}

/// Response for `POST /v0/boxes/:box/diff`.
#[derive(Debug, Clone, Serialize)]
pub struct DiffResponse {
    #[serde(rename = "box")]
    pub box_name: String,
    pub records: Vec<RecordOut>,
    pub next_from_seq: u64,
    pub head_seq: u64,
    pub earliest_seq: u64,
    pub caught_up: bool,
    pub tombstone: Option<Tombstone>,
    pub lag: u64,
    pub performance: Performance,
}

// ---------------------------------------------------------------------------
// Tag predicate (router forward filter §6 + delete `match` §5)
// ---------------------------------------------------------------------------

/// A single tag predicate tuple `["tag", "Eq"|"Glob", value]`.
///
/// Also accepts the bare-string shorthand: `"tenant:*"` (trailing `*` ⇒ Glob,
/// otherwise Eq). The tuple form is canonical and used on serialize. Used both
/// as a router forward filter (§6) and as a delete `match` predicate (§5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Filter {
    pub op: FilterOp,
    /// For `Glob`, the literal prefix (the trailing `*` is stripped).
    pub value: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterOp {
    Eq,
    Glob,
}

impl Filter {
    /// Returns true if the given tag matches this filter.
    pub fn matches(&self, tag: &str) -> bool {
        match self.op {
            FilterOp::Eq => tag == self.value,
            FilterOp::Glob => tag.starts_with(&self.value),
        }
    }
}

impl Serialize for Filter {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeTuple;
        let mut t = serializer.serialize_tuple(3)?;
        t.serialize_element("tag")?;
        match self.op {
            FilterOp::Eq => {
                t.serialize_element("Eq")?;
                t.serialize_element(&self.value)?;
            }
            FilterOp::Glob => {
                t.serialize_element("Glob")?;
                // re-append the trailing `*` for the canonical wire form.
                t.serialize_element(&format!("{}*", self.value))?;
            }
        }
        t.end()
    }
}

impl<'de> Deserialize<'de> for Filter {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Accept either a bare string or a ["tag","Eq"/"Glob",value] tuple.
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Bare(String),
            Tuple(Vec<String>),
        }

        let raw = Raw::deserialize(deserializer)?;
        match raw {
            Raw::Bare(s) => Ok(Filter::from_shorthand(&s)),
            Raw::Tuple(v) => {
                if v.len() != 3 {
                    return Err(serde::de::Error::custom(
                        "filter tuple must be [\"tag\", op, value]",
                    ));
                }
                if v[0] != "tag" {
                    return Err(serde::de::Error::custom(
                        "filter field must be \"tag\"",
                    ));
                }
                match v[1].as_str() {
                    "Eq" => Ok(Filter {
                        op: FilterOp::Eq,
                        value: v[2].clone(),
                    }),
                    "Glob" => {
                        let val = &v[2];
                        if !val.ends_with('*') {
                            return Err(serde::de::Error::custom(
                                "Glob filter value must end with a trailing '*'",
                            ));
                        }
                        Ok(Filter {
                            op: FilterOp::Glob,
                            value: val[..val.len() - 1].to_string(),
                        })
                    }
                    other => Err(serde::de::Error::custom(format!(
                        "filter op must be Eq or Glob, got {other:?}"
                    ))),
                }
            }
        }
    }
}

impl Filter {
    /// Parse the bare-string shorthand: trailing `*` ⇒ prefix Glob, else Eq.
    pub fn from_shorthand(s: &str) -> Filter {
        if let Some(prefix) = s.strip_suffix('*') {
            Filter {
                op: FilterOp::Glob,
                value: prefix.to_string(),
            }
        } else {
            Filter {
                op: FilterOp::Eq,
                value: s.to_string(),
            }
        }
    }
}

/// Request body for `POST /v0/boxes/:box/delete` (API §5). Permanent,
/// point-in-time deletion by seq range and/or tag match. At least one of
/// `before_seq` / `match` is required (else `400 invalid_request`); supplying
/// both ANDs them.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct DeleteRequest {
    /// Delete every record with `$seq < before_seq` (snapshot / compaction).
    #[serde(default)]
    pub before_seq: Option<u64>,
    /// Tag predicate: `["tag","Eq",v]`, `["tag","Glob","p*"]`, or the
    /// bare-string shorthand `"v"` == `["tag","Eq","v"]`.
    #[serde(default, rename = "match")]
    pub match_: Option<Filter>,
}

/// Response for `POST /v0/boxes/:box/delete` (API §5).
#[derive(Debug, Clone, Serialize)]
pub struct DeleteResponse {
    #[serde(rename = "box")]
    pub box_name: String,
    /// Count of records removed by this call.
    pub deleted: u64,
    /// New first live seq (advanced past any deleted prefix).
    pub earliest_seq: u64,
    pub head_seq: u64,
    pub count: u64,
    pub bytes: u64,
    pub performance: Performance,
}

// ---------------------------------------------------------------------------
// Queues (API §10)
// ---------------------------------------------------------------------------

/// The `queue` sub-object on `GET /v0/boxes/:q` for a queue box (API §10.7).
#[derive(Debug, Clone, Serialize)]
pub struct QueueState {
    /// Claimable jobs right now (not acked, no active lease; includes
    /// reclaim-freelist seqs whose lease expired or whose nack delay elapsed).
    pub ready: u64,
    /// Jobs with an active (un-expired) lease — currently held by some worker.
    pub in_flight: u64,
    /// Cumulative jobs moved to the `dead_letter` box over this box instance's
    /// life (resets on delete+recreate).
    pub dead_lettered: u64,
}

/// Request body for `POST /v0/boxes/:q/claim` (API §10.2).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ClaimRequest {
    pub node: String,
    /// Max jobs to lease this call (default 1, clamped to `MAX_CLAIM`).
    #[serde(default)]
    pub max: u32,
    /// Lease duration override for this call (default = box `lease_ms`).
    #[serde(default)]
    pub lease_ms: Option<u64>,
}

/// One leased job in a claim response (API §10.2).
#[derive(Debug, Clone, Serialize)]
pub struct ClaimedJob {
    #[serde(rename = "$seq")]
    pub seq: u64,
    pub lease_id: String,
    pub deadline: i64,
    #[serde(rename = "$ts")]
    pub ts: i64,
    #[serde(rename = "$tag", skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    pub deliveries: u64,
    pub data: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<Value>,
}

/// Response for `POST /v0/boxes/:q/claim`.
#[derive(Debug, Clone, Serialize)]
pub struct ClaimResponse {
    #[serde(rename = "box")]
    pub box_name: String,
    pub claimed: Vec<ClaimedJob>,
    pub count: u64,
    pub ready: u64,
    pub performance: Performance,
}

/// Request body for `POST /v0/boxes/:q/ack` (API §10.4).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct AckRequest {
    pub node: String,
    pub seqs: Vec<u64>,
}

/// Response for `POST /v0/boxes/:q/ack`.
#[derive(Debug, Clone, Serialize)]
pub struct AckResponse {
    #[serde(rename = "box")]
    pub box_name: String,
    pub acked: u64,
    pub skipped: Vec<u64>,
    pub ready: u64,
    pub in_flight: u64,
    pub performance: Performance,
}

/// Request body for `POST /v0/boxes/:q/nack` (API §10.5).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct NackRequest {
    pub node: String,
    pub seqs: Vec<u64>,
    #[serde(default)]
    pub delay_ms: u64,
}

/// Response for `POST /v0/boxes/:q/nack`.
#[derive(Debug, Clone, Serialize)]
pub struct NackResponse {
    #[serde(rename = "box")]
    pub box_name: String,
    pub nacked: u64,
    pub skipped: Vec<u64>,
    pub ready: u64,
    pub in_flight: u64,
    pub performance: Performance,
}

/// Request body for `POST /v0/boxes/:q/extend` (API §10.6).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ExtendRequest {
    pub node: String,
    pub seqs: Vec<u64>,
    pub lease_ms: u64,
}

/// Response for `POST /v0/boxes/:q/extend`.
#[derive(Debug, Clone, Serialize)]
pub struct ExtendResponse {
    #[serde(rename = "box")]
    pub box_name: String,
    pub extended: u64,
    pub skipped: Vec<u64>,
    /// New absolute deadline (ms) per extended seq, keyed by seq as a string.
    pub deadlines: std::collections::HashMap<String, i64>,
    pub performance: Performance,
}

// ---------------------------------------------------------------------------
// Routers (API §6)
// ---------------------------------------------------------------------------

/// A router config / forwarding rule.
#[derive(Debug, Clone, PartialEq)]
pub struct Router {
    pub name: String,
    pub source: String,
    pub dest: String,
    pub preserve_node: bool,
    pub preserve_tag: bool,
    pub create_dest: bool,
    pub filter: Option<Filter>,
    pub allow_cycle: bool,
}

/// Request body for `PUT /v0/routers/:router`.
#[derive(Debug, Clone, Deserialize)]
pub struct RouterCreateRequest {
    pub source: String,
    pub dest: String,
    #[serde(default = "default_true")]
    pub preserve_node: bool,
    #[serde(default = "default_true")]
    pub preserve_tag: bool,
    #[serde(default = "default_true")]
    pub create_dest: bool,
    #[serde(default)]
    pub filter: Option<Filter>,
    #[serde(default)]
    pub allow_cycle: bool,
}

fn default_true() -> bool {
    true
}

/// Response for `PUT /v0/routers/:router`.
#[derive(Debug, Clone, Serialize)]
pub struct RouterCreateResponse {
    pub router: String,
    pub created: bool,
    pub source: String,
    pub dest: String,
    pub preserve_node: bool,
    pub preserve_tag: bool,
    pub filter: Option<Filter>,
    pub allow_cycle: bool,
    pub performance: Performance,
}

/// Response for `GET /v0/routers/:router`.
#[derive(Debug, Clone, Serialize)]
pub struct RouterGetResponse {
    pub router: String,
    pub source: String,
    pub dest: String,
    pub preserve_node: bool,
    pub preserve_tag: bool,
    pub filter: Option<Filter>,
    pub allow_cycle: bool,
    pub forwarded_total: u64,
    pub performance: Performance,
}

/// One entry in the `GET /v0/routers` listing.
#[derive(Debug, Clone, Serialize)]
pub struct RouterSummary {
    pub router: String,
    pub source: String,
    pub dest: String,
    pub forwarded_total: u64,
}

/// Response for `GET /v0/routers`.
#[derive(Debug, Clone, Serialize)]
pub struct RouterListResponse {
    pub routers: Vec<RouterSummary>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    pub performance: Performance,
}

/// Response for `DELETE /v0/routers/:router`.
#[derive(Debug, Clone, Serialize)]
pub struct RouterDeleteResponse {
    pub router: String,
    pub deleted: bool,
    pub performance: Performance,
}

// ---------------------------------------------------------------------------
// Watch / SSE (API §7)
// ---------------------------------------------------------------------------

/// Per-box options inside a watch subscription.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct WatchBoxOptions {
    #[serde(default)]
    pub from_seq: u64,
    #[serde(default)]
    pub tail: bool,
}

/// Request body for `POST /v0/watch`.
#[derive(Debug, Clone, Deserialize)]
pub struct WatchCreateRequest {
    #[serde(default)]
    pub node: Option<NodeFilter>,
    pub boxes: std::collections::HashMap<String, WatchBoxOptions>,
    #[serde(default = "default_watch_limit")]
    pub limit: u32,
    #[serde(default = "default_max_batch_bytes")]
    pub max_batch_bytes: u64,
    #[serde(default = "default_heartbeat_ms")]
    pub heartbeat_ms: u64,
    #[serde(default = "default_include_meta")]
    pub include_meta: bool,
    #[serde(default)]
    pub include_tags: bool,
    #[serde(default = "default_true")]
    pub include_data: bool,
    #[serde(default = "default_consistency")]
    pub consistency: Consistency,
}

fn default_watch_limit() -> u32 {
    256
}
fn default_max_batch_bytes() -> u64 {
    262_144
}
fn default_heartbeat_ms() -> u64 {
    15_000
}
fn default_consistency() -> Consistency {
    Consistency::Eventual
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Consistency {
    Eventual,
    Strong,
}

/// Per-box watermark info echoed in the watch-create response.
#[derive(Debug, Clone, Serialize)]
pub struct WatchBoxState {
    pub from_seq: u64,
    pub head_seq: u64,
    pub earliest_seq: u64,
}

/// Response for `POST /v0/watch`.
#[derive(Debug, Clone, Serialize)]
pub struct WatchCreateResponse {
    pub wid: String,
    pub stream_url: String,
    pub session_ttl_ms: u64,
    pub boxes: std::collections::HashMap<String, WatchBoxState>,
    pub performance: Performance,
}

// ---------------------------------------------------------------------------
// Health / readiness / metrics (API §8)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
    pub uptime_ms: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReadyResponse {
    pub status: String,
    pub wal_replay_complete: bool,
    pub boxes: u64,
}

// ---------------------------------------------------------------------------
// Performance block (API §0.9)
// ---------------------------------------------------------------------------

/// Best-effort per-response observability block. Fields are additive; clients
/// tolerate any subset. Omitted fields are skipped.
#[derive(Debug, Clone, Default, Serialize)]
pub struct Performance {
    pub server_total_ms: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wal_append_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fsync_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub records_scanned: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub throttle_wait_ms: Option<f64>,
    /// Number of records in this response served by a COLD-tier read (a degraded
    /// historical read; tiered storage, Phase 6). Omitted when zero so a
    /// fully-hot response is byte-identical to before.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cold_segments_read: Option<u64>,
}

impl Performance {
    /// A minimal performance block with only `server_total_ms`.
    pub fn with_total(server_total_ms: f64) -> Self {
        Performance {
            server_total_ms,
            ..Default::default()
        }
    }
}

// ---------------------------------------------------------------------------
// Error model (API §0.5, §0.6)
// ---------------------------------------------------------------------------

/// Canonical error body: `{"error": {code, message, detail?}}`.
#[derive(Debug, Clone, Serialize)]
pub struct ErrorEnvelope {
    pub error: ErrorBody,
}

#[derive(Debug, Clone, Serialize)]
pub struct ErrorBody {
    pub code: &'static str,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<Value>,
}

/// Stable machine-readable error codes. Each maps to an HTTP status and a
/// snake_case wire code (API §0.6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    InvalidRequest,
    BatchTooLarge,
    RecordTooLarge,
    Unauthorized,
    BoxNotFound,
    RouterNotFound,
    NotFound,
    MethodNotAllowed,
    NotAcceptable,
    RouterCycle,
    BoxExistsIncompatible,
    BoxNotEmpty,
    NotAQueue,
    PayloadTooLarge,
    UnsupportedMediaType,
    BoxFull,
    Throttled,
    Internal,
    NotReady,
    ShuttingDown,
}

impl ErrorCode {
    /// The HTTP status code for this error.
    pub fn status(self) -> u16 {
        use ErrorCode::*;
        match self {
            InvalidRequest | BatchTooLarge | RecordTooLarge => 400,
            Unauthorized => 401,
            BoxNotFound | RouterNotFound | NotFound => 404,
            MethodNotAllowed => 405,
            NotAcceptable => 406,
            RouterCycle | BoxExistsIncompatible | BoxNotEmpty | NotAQueue => 409,
            PayloadTooLarge => 413,
            UnsupportedMediaType => 415,
            BoxFull => 422,
            Throttled => 429,
            Internal => 500,
            NotReady | ShuttingDown => 503,
        }
    }

    /// The stable snake_case wire code.
    pub fn code(self) -> &'static str {
        use ErrorCode::*;
        match self {
            InvalidRequest => "invalid_request",
            BatchTooLarge => "batch_too_large",
            RecordTooLarge => "record_too_large",
            Unauthorized => "unauthorized",
            BoxNotFound => "box_not_found",
            RouterNotFound => "router_not_found",
            NotFound => "not_found",
            MethodNotAllowed => "method_not_allowed",
            NotAcceptable => "not_acceptable",
            RouterCycle => "router_cycle",
            BoxExistsIncompatible => "box_exists_incompatible",
            BoxNotEmpty => "box_not_empty",
            NotAQueue => "not_a_queue",
            PayloadTooLarge => "payload_too_large",
            UnsupportedMediaType => "unsupported_media_type",
            BoxFull => "box_full",
            Throttled => "throttled",
            Internal => "internal",
            NotReady => "not_ready",
            ShuttingDown => "shutting_down",
        }
    }
}
