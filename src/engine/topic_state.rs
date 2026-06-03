//! Per-topic in-memory state: the base+offset [`TopicIndex`], the watermark
//! atomics, retained payload bytes, the per-topic tag index, recency clocks, and
//! the `Notify` used to wake SSE/diff long-pollers (ARCHITECTURE §1).
//!
//! Phase 2 stores payload bytes directly on the heap (`StoredRecord`); phase 4
//! re-points `RecordLoc` at mmap'd segments. The serving/indexing logic here is
//! written once and reused.
//!
//! Deletion (DESIGN §7) is permanent, point-in-time, and silent. A record
//! deleted in the *middle* of the log keeps its slot as a lightweight
//! tombstone (`deleted` flag set; payload/tag freed; bytes/count adjusted) to
//! preserve O(1) seq→slot indexing; only fully-dead *front* slots are
//! physically popped (lazy reclaim), advancing `base_seq`.

use crate::engine::broadcast::BroadcastCache;
use crate::engine::eviction::Floors;
use crate::engine::queue::QueueProjection;
use crate::engine::segwriter::{read_locator, ResolvedPayload, SealedResolve, SegmentWriter};
use crate::storage::CommitProof;
#[cfg(debug_assertions)]
use crate::types::Durability;
use crate::types::{Filter, TopicConfig};
use parking_lot::{Mutex, RwLock};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

/// A remembered idempotent write: the seqs it assigned and when it landed, so a
/// retry within `idempotency_window_ms` returns the original seqs (API §0.8).
#[derive(Debug, Clone)]
pub struct DedupeEntry {
    pub seqs: Vec<u64>,
    pub head_seq: u64,
    pub created_ms: i64,
}

/// One stored record. In phase 2 the payload lives inline on the heap; the
/// `ts`/`tag`/`node` are kept for the read pipeline and TTL math.
///
/// A record deleted in the middle of the log keeps its slot as a lightweight
/// tombstone: `deleted` is set and `data`/`meta`/`tag` are freed (replaced with
/// `Null`/`None`) so the slot costs almost nothing while preserving O(1)
/// indexing. Such a slot is never delivered.
#[derive(Debug, Clone)]
pub struct StoredRecord {
    pub ts: i64,
    pub node: Option<String>,
    pub tag: Option<String>,
    pub data: Value,
    pub meta: Option<Value>,
    /// Accounted payload size (`data` + `meta` + framing estimate). Zeroed once
    /// the slot is deleted (its bytes already subtracted from the topic total).
    pub bytes: u64,
    /// Set when this record has been deleted (permanent, silent). The slot is
    /// retained only as a hole for O(1) indexing; its payload is freed.
    pub deleted: bool,
    /// Whether `data`/`meta` are still resident in this slot (Phase 6 memory
    /// bounding). `true` for a fresh append and for any topic without a segment
    /// writer (the unchanged default). Once a record is durably in a *sealed*
    /// segment, a writer-backed topic frees the slot's `data`/`meta` (sets this
    /// `false`) and the read path resolves the payload from the bounded cache or
    /// the segment. The `bytes` accounting is unchanged (the record is still
    /// live; only where its payload lives changed).
    pub payload_resident: bool,
    /// Router forward hop count for the async/derived cycle loop-breaker. A
    /// direct user write is `0`; a record forwarded by a router carries
    /// `source_record.hops + 1`. A record at/above [`crate::config::MAX_ROUTER_HOPS`]
    /// is not forwarded again, so an `allow_cycle` topology terminates cleanly.
    /// In-memory only (never
    /// WAL-logged — derived dest records are not logged at all; a direct write is
    /// always `0`), and re-derived identically on recovery by replaying forwarding
    /// from the cursor.
    pub hops: u8,
}

/// The seq→record index: a contiguous deque offset by `base_seq`
/// (ARCHITECTURE §1.1). `index i` corresponds to `seq = base_seq + i`.
///
/// Carries the per-topic **tag index** (`tag → ascending live seqs`) so a tag
/// delete is a point lookup (exact) or a bounded range scan (prefix) rather
/// than a full log scan (DESIGN §7.2), and a `delete_below` marker (the max
/// `before_seq` ever applied) so snapshot/prefix deletes are O(1) to apply: a
/// read starts at `max(from_seq + 1, base_seq)` and then skips any remaining
/// deleted slots.
#[derive(Debug, Default)]
pub struct TopicIndex {
    /// Seq of `records[0]`; the earliest physically present seq.
    pub base_seq: u64,
    pub records: VecDeque<StoredRecord>,
    /// `tag → ascending live seqs` for efficient match-deletes (DESIGN §7.2).
    pub tag_index: BTreeMap<String, Vec<u64>>,
    /// Max `before_seq` applied by a delete; every seq `< delete_below` is dead.
    pub delete_below: u64,
}

impl TopicIndex {
    pub fn new(base_seq: u64) -> Self {
        TopicIndex {
            base_seq,
            records: VecDeque::new(),
            tag_index: BTreeMap::new(),
            delete_below: 0,
        }
    }

    /// Lookup a record by seq, if physically present.
    pub fn get(&self, seq: u64) -> Option<&StoredRecord> {
        if seq < self.base_seq {
            return None;
        }
        self.records.get((seq - self.base_seq) as usize)
    }

    /// Index of `seq` in `records`, if physically present.
    fn slot_idx(&self, seq: u64) -> Option<usize> {
        if seq < self.base_seq {
            return None;
        }
        let i = (seq - self.base_seq) as usize;
        if i < self.records.len() {
            Some(i)
        } else {
            None
        }
    }

    /// Whether a seq is dead: physically gone, deleted, or below `delete_below`.
    pub fn is_dead(&self, seq: u64) -> bool {
        if seq < self.delete_below {
            return true;
        }
        match self.slot_idx(seq) {
            Some(i) => self.records[i].deleted,
            None => seq < self.base_seq, // popped front ⇒ dead; future ⇒ alive.
        }
    }

    /// Whether **every** seq in `[start, end]` (inclusive) is dead — used by
    /// segment-granular reclaim (ARCHITECTURE §3.3) to decide that a whole sealed
    /// segment can be dropped: either the segment is fully below the live floor
    /// (cap/TTL/prefix delete) or every record in it was point-deleted (an
    /// interior segment cleared by a `match` delete). A range that runs past the
    /// physically-present records (above `head`) is *not* fully dead (those slots
    /// are still live), so a still-growing range is never reclaimed.
    pub fn range_all_dead(&self, start: u64, end: u64) -> bool {
        let mut seq = start;
        while seq <= end {
            if !self.is_dead(seq) {
                return false;
            }
            seq += 1;
        }
        true
    }

    /// Live seqs (ascending) whose tag matches `filter`, bounded by `bound`
    /// (exclusive upper bound — the point-in-time head). Exact = point lookup;
    /// prefix = a range scan over `[prefix, next-key)` of the tag index. Never
    /// scans the whole log (DESIGN §7.2). Already-deleted slots are absent from
    /// the tag index, so all returned seqs are live.
    pub fn matching_live_seqs(&self, filter: &Filter, bound: u64) -> Vec<u64> {
        use crate::types::FilterOp;
        let mut out = Vec::new();
        match filter.op {
            FilterOp::Eq => {
                if let Some(seqs) = self.tag_index.get(&filter.value) {
                    out.extend(seqs.iter().copied().filter(|&s| s < bound));
                }
            }
            FilterOp::Glob => {
                let prefix = filter.value.as_str();
                // Range scan from `prefix` to the first key that no longer
                // starts with it. `BTreeMap<String,_>` is byte-ordered, so all
                // keys with `prefix` form one contiguous run starting at the
                // `range(prefix..)` lower bound (DESIGN §7.2).
                for (tag, seqs) in self.tag_index.range(prefix.to_string()..) {
                    if !tag.starts_with(prefix) {
                        break;
                    }
                    out.extend(seqs.iter().copied().filter(|&s| s < bound));
                }
            }
        }
        out.sort_unstable();
        out
    }

    /// Record a freshly-appended tagged record in the tag index.
    pub fn index_tag(&mut self, seq: u64, tag: &str) {
        // Appends are monotonic in seq, so push_back keeps the vec ascending.
        self.tag_index.entry(tag.to_string()).or_default().push(seq);
    }

    /// Remove `seq` from `tag`'s posting list (on delete/reclaim/rollback).
    pub fn unindex_tag(&mut self, tag: &str, seq: u64) {
        if let Some(v) = self.tag_index.get_mut(tag) {
            if let Ok(pos) = v.binary_search(&seq) {
                v.remove(pos);
            }
            if v.is_empty() {
                self.tag_index.remove(tag);
            }
        }
    }

    /// Drop the oldest `n` records, advancing `base_seq` and pruning their tag
    /// postings. Used by both cap/TTL front-eviction and lazy delete reclaim.
    /// Returns the number of popped slots that were still **live** (not already
    /// deleted) — i.e. the involuntary-eviction count to subtract from
    /// `live_count`.
    pub fn drain_front(&mut self, n: usize) -> u64 {
        let n = n.min(self.records.len());
        let mut live_popped = 0u64;
        for i in 0..n {
            let seq = self.base_seq + i as u64;
            if !self.records[i].deleted {
                live_popped += 1;
            }
            if let Some(tag) = self.records[i].tag.clone() {
                self.unindex_tag(&tag, seq);
            }
        }
        self.records.drain(..n);
        self.base_seq += n as u64;
        live_popped
    }

    /// Mark the slot at `seq` deleted (if live), freeing its payload/tag and
    /// pruning its tag posting. Returns `Some(bytes_freed)` if it transitioned a
    /// live slot to deleted, or `None` if the slot was absent or already dead.
    /// Public wrapper around [`Self::mark_deleted`] for the queue ack /
    /// dead-letter delete path (DESIGN §10).
    pub fn mark_deleted_pub(&mut self, seq: u64) -> Option<u64> {
        self.mark_deleted(seq)
    }

    fn mark_deleted(&mut self, seq: u64) -> Option<u64> {
        let i = self.slot_idx(seq)?;
        if self.records[i].deleted {
            return None;
        }
        let freed = self.records[i].bytes;
        if let Some(tag) = self.records[i].tag.take() {
            self.unindex_tag(&tag, seq);
        }
        let rec = &mut self.records[i];
        rec.deleted = true;
        rec.bytes = 0;
        rec.data = Value::Null;
        rec.meta = None;
        Some(freed)
    }
}

/// A staged-but-not-yet-published append batch (the WAL-first reservation,
/// ARCHITECTURE §2.2). Returned by [`TopicState::stage_append`]; consumed by
/// [`TopicState::publish_staged`] (on a successful WAL commit) or
/// [`TopicState::rollback_staged`] (on a WAL/fsync failure). The records are
/// already in the index deque (the contiguous tail past `pre_len`) but invisible
/// (`head_seq` unchanged) until published.
#[derive(Debug)]
pub struct StagedAppend {
    /// First seq assigned to the batch — the index deque tail at stage time
    /// (`base_seq + len`), which equals `head_seq + 1` only when no earlier batch
    /// is still staged-but-unpublished (see [`TopicState::stage_append`]).
    start: u64,
    /// Number of records in the batch.
    count: u64,
    /// Total accounted bytes of the batch (added to `bytes_retained` on publish).
    added_bytes: u64,
    /// Index-deque length before staging (the rollback truncation target).
    pre_len: usize,
}

impl StagedAppend {
    /// An empty stage (no records) — publish/rollback are no-ops.
    fn empty() -> Self {
        StagedAppend {
            start: 0,
            count: 0,
            added_bytes: 0,
            pre_len: 0,
        }
    }

    /// Whether this stage carries no records.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// The seqs this batch reserved (`start..=start+count-1`).
    pub fn seqs(&self) -> Vec<u64> {
        if self.count == 0 {
            Vec::new()
        } else {
            (self.start..=self.start + self.count - 1).collect()
        }
    }

    /// The head seq this batch will publish (its last reserved seq) — used to
    /// reserve the durable disk head ceiling BEFORE making the batch visible
    /// (R3 / codex P0 #3). `0` for an empty batch (never published).
    pub fn publish_head(&self) -> u64 {
        if self.count == 0 {
            0
        } else {
            self.start + self.count - 1
        }
    }
}

/// Capability required to make staged records visible by advancing `head_seq`.
///
/// The permit is intentionally lightweight: Rust cannot prove that storage
/// hardware persisted bytes, but requiring this value at the publish boundary
/// prevents casual callers from advancing visibility without first choosing one
/// of the trusted paths.
#[derive(Debug, Clone, Copy)]
pub struct PublishPermit {
    kind: PublishPermitKind,
}

#[derive(Debug, Clone, Copy)]
enum PublishPermitKind {
    /// Resident-only publication: pure in-memory engines, tests, and ephemeral
    /// topics whose record path intentionally skips the WAL.
    Resident,
    /// Publication after the WAL writer has resolved the batch's commit token.
    WalCommitted { fsynced: bool },
    /// Derived router publication; reconstructed from source WAL + router cursor.
    Derived,
    /// Recovery restored a durable head watermark and is padding lost disk tail.
    Recovery,
}

impl PublishPermit {
    /// Permit resident-only publication. This is public because some integration
    /// tests exercise `TopicState` directly without an `Engine`/WAL.
    pub fn resident() -> Self {
        Self {
            kind: PublishPermitKind::Resident,
        }
    }

    /// Permit publication after a WAL commit token resolved successfully.
    pub(crate) fn wal_committed(proof: CommitProof) -> Self {
        Self {
            kind: PublishPermitKind::WalCommitted {
                fsynced: proof.is_fsynced(),
            },
        }
    }

    /// Permit publication of records derived from already-committed source state.
    pub(crate) fn derived() -> Self {
        Self {
            kind: PublishPermitKind::Derived,
        }
    }

    /// Permit recovery-only publication of durable head watermark padding.
    fn recovery() -> Self {
        Self {
            kind: PublishPermitKind::Recovery,
        }
    }
}

/// A lightweight deleted-hole slot: a tombstone with no payload, used to keep
/// `seq - base_seq` indexing dense across seq gaps (a reclaimed-but-not-popped
/// middle delete, or a reserved-but-unwritten `disk` seq restored by a head
/// watermark on recovery, R3). Carries `0` bytes and is never delivered.
pub(crate) fn deleted_hole() -> StoredRecord {
    StoredRecord {
        ts: 0,
        node: None,
        tag: None,
        data: Value::Null,
        meta: None,
        bytes: 0,
        deleted: true,
        payload_resident: true,
        hops: 0,
    }
}

/// The outcome of one [`TopicState::enforce_retention`] pass: whether the
/// involuntary loss floor advanced, whether a NON-RE-DERIVABLE cause (TTL or
/// byte-cap) drove it (so the engine must durably persist the resolved floor,
/// R7), and the resolved involuntary floor `max(evict_floor, expiry_floor)`.
#[derive(Debug, Clone, Copy)]
pub struct RetentionAdvance {
    /// The involuntary loss floor advanced this pass (any cause).
    pub floor_advanced: bool,
    /// A non-re-derivable cause (TTL expiry or byte-cap eviction) advanced the
    /// floor: the engine must durably log the watermark (R7).
    pub durable_advance: bool,
    /// The cap-records / byte-cap floor after this pass (for the durable frame).
    pub evict_floor: u64,
    /// The TTL expiry floor after this pass (carried SEPARATELY so the durable
    /// watermark preserves the cap-vs-ttl tombstone reason across restart, R7).
    pub expiry_floor: u64,
    /// Bytes physically reclaimed from the topic front during this pass. The engine
    /// uses this to keep the global `max_total_bytes` reservation gauge exact.
    pub bytes_reclaimed: u64,
}

impl RetentionAdvance {
    /// No floor moved (the common hot-path / empty-topic case).
    pub const NONE: RetentionAdvance = RetentionAdvance {
        floor_advanced: false,
        durable_advance: false,
        evict_floor: 0,
        expiry_floor: 0,
        bytes_reclaimed: 0,
    };
}

/// Result of a permanent delete pass.
#[derive(Debug, Clone, Copy, Default)]
pub struct DeleteStats {
    /// Live records removed by this delete.
    pub deleted: u64,
    /// Payload bytes freed by this delete and any retention front-reclaim it drove.
    pub bytes_freed: u64,
}

/// The full in-memory state of one topic.
pub struct TopicState {
    /// The topic name (also the identity).
    pub name: String,
    /// Interned numeric id used in WAL frames (ARCHITECTURE §2.1). Stable for the
    /// lifetime of this topic instance; reassigned on delete+recreate.
    pub topic_id: u64,
    /// Live config (read-mostly; mutated under `index` write lock on `PUT`).
    pub config: RwLock<TopicConfig>,
    /// The seq→record index (carries the per-topic tag index + `delete_below`).
    pub index: RwLock<TopicIndex>,
    /// Eviction/expiry/delete floors driving `earliest_seq` (DESIGN §5.1).
    pub floors: RwLock<Floors>,
    /// `(idempotency_key → assigned seqs)` dedupe state (API §0.8). Entries are
    /// reclaimed lazily once older than the topic's `idempotency_window_ms`.
    pub dedupe: RwLock<HashMap<String, DedupeEntry>>,
    /// Per-key in-flight gates for idempotent writes. A write carrying an
    /// `idempotency_key` holds that key's gate across its WHOLE reservation
    /// (check dedupe → stage → WAL fsync → publish → record dedupe). This closes
    /// the check-then-act race (API §0.8 / model invariant 13): two concurrent
    /// writes with the same key serialize on the gate, so the loser re-checks
    /// the dedupe map under the gate, finds the winner's entry, and returns the
    /// winner's seqs instead of publishing a second distinct live batch. Writes
    /// with *different* keys (or none) take different gates and never contend.
    /// The registry is pruned lazily (a gate is removed once no writer holds it).
    pub dedupe_gates: Mutex<HashMap<String, Arc<Mutex<()>>>>,

    /// Highest assigned seq (`0` for a fresh empty topic).
    pub head_seq: AtomicU64,
    /// Highest seq this topic has DURABLY reserved via a fsynced `HeadWatermark`
    /// (R3). A `disk`-class write may only ack seqs `<= reserved_head`; crossing
    /// it forces a fresh fsynced reservation so a crash can never re-hand an
    /// already-acked `disk` seq. `0` ⇒ nothing reserved yet (the first disk write
    /// reserves the first block). Always `>= head_seq`.
    pub reserved_head: AtomicU64,
    /// First seq this topic instance will ever assign (`seq_base`, default 1).
    pub seq_base: u64,
    /// Bumped on create; detects delete+recreate (DESIGN §5.5).
    pub epoch: AtomicU64,
    /// Retained payload bytes (approximate under lazy eviction).
    pub bytes_retained: AtomicU64,
    /// Count of currently-live (deliverable) records: incremented on append,
    /// decremented on delete and on live cap/TTL eviction. Net of deletions.
    pub live_count: AtomicU64,

    /// Recency clocks (ms; `0`/`MIN` sentinel for never).
    pub last_write_ms: AtomicI64,
    pub last_read_ms: AtomicI64,
    /// `last_consumed_at` for auto-priority (DESIGN §3).
    pub last_consumed_ms: AtomicI64,

    /// Advisory "already marked dirty in the scheduler" flag (codex P1). The
    /// scheduler ready-set is drained only by the (not-yet-wired) phase-4 governor,
    /// so once a topic is dirty it stays dirty; this atomic lets the write hot path
    /// skip the global scheduler mutex + string alloc on every append after the
    /// first, removing a global lock that capped WAL-shard scaling. Reset to `false`
    /// when the ready-set is drained so the topic re-enters on its next write.
    pub sched_dirty: std::sync::atomic::AtomicBool,

    /// Wakes SSE/diff long-pollers on append (ARCHITECTURE §1.2).
    pub notify: Notify,

    /// Zero-copy SSE broadcast cache (ARCHITECTURE §8.4): each delivered record
    /// frame is serialized once and shared (ref-counted) across all watchers, so
    /// a 1→N broadcast pays serialization once, not N times. Bounded; populated
    /// lazily on the read path only (topics with no watchers pay nothing).
    pub broadcast: BroadcastCache,

    /// The materialized lease projection for a queue topic (DESIGN §10,
    /// ARCHITECTURE §12). `None` on a plain `"log"` topic. Lives under its own
    /// mutex (queue lifecycle transitions are rare relative to the read path);
    /// the single batched cohort claim pass holds it for one critical section.
    pub queue: Option<Mutex<QueueProjection>>,

    /// Serializes the **seq-assignment + WAL-enqueue** critical section on the
    /// write path so a topic's WAL frames are appended to the single ordered writer
    /// in the *same order* their seqs were assigned. Without this, two concurrent
    /// durable writers can assign seqs `A < B` under the index lock yet enqueue
    /// `B` before `A`; recovery (which applies frames in WAL order and skips any
    /// `seq <= head`) would then drop the lower-seq frame — a silent loss of an
    /// acked durable write. Held only across the (fast, non-blocking) channel
    /// enqueue; the fsync wait happens *after* the lock is released, so durable
    /// group commit still coalesces across topics AND across writers of THIS topic
    /// (ARCHITECTURE §2.2/§2.3, codex P0 #1).
    pub append_lock: Mutex<()>,

    /// Per-topic commit sequencer enabling concurrent same-topic durable writers to
    /// coalesce into ONE group-commit fsync (codex P0 #1). The `append_lock`
    /// covers only stage+enqueue; the fsync `wait()` then happens OFF the lock so
    /// many writers' frames batch into a single fsync. But publish (and rollback)
    /// must still happen in strict seq order, so each writer takes a monotonically
    /// increasing `publish_ticket` while holding `append_lock`, then blocks on
    /// this gate until `publish_next` reaches its ticket before
    /// publishing/rolling back, finally bumping `publish_next` and waking the next
    /// writer. Ordered publish makes the single ordered WAL writer's prefix-commit
    /// guarantee hold: when writer B's token fires, every lower-seq frame
    /// (writer A's) is already fsynced, so publishing in order never exposes a
    /// non-durable record.
    pub publish_ticket: AtomicU64,
    pub publish_gate: std::sync::Mutex<u64>,
    pub publish_cv: std::sync::Condvar,

    /// The per-topic HOT segment writer + bounded payload cache (Phase 6 Stage 2).
    /// `None` for a pure in-memory topic (every existing unit test) — then payloads
    /// stay resident and the read path is unchanged by construction. When present
    /// (a durable, writer-backed topic), committed records are materialized into
    /// HOT segments off the commit path, and once a record is sealed its resident
    /// payload is freed and reads resolve from the bounded cache or the segment.
    /// Lives behind a `Mutex`: touched on the commit path (append/seal) and the
    /// read path (segment-backed resolution) but never holding the index lock
    /// across a slow cold fetch (the Phase-6 HARD INVARIANT).
    pub segwriter: Option<Mutex<SegmentWriter>>,

    /// Ordered-materialization seam (R6 / codex P1 #7). With the segment seal moved
    /// OFF the publish gate, two same-topic writers can reach `materialize_published`
    /// out of seq order (writer B publishes + materializes `N+1` before writer A
    /// materializes `N`). [`SegmentWriter`] assumes strictly monotonic append order
    /// (a backwards seq trips its contiguity assert; a forward jump force-seals a
    /// phantom gap), so the seam serializes materialization back into seq order: each
    /// range is admitted to the writer only when it starts exactly at `next`, and any
    /// earlier-arriving later range is buffered until its predecessor lands. The gate
    /// stays seal-free (the fsync is still off the publish gate); only the cheap
    /// in-order hand-off is serialized here.
    pub materialize_seam: Mutex<MaterializeSeam>,
}

/// The ordered-materialization cursor + out-of-order buffer for one topic (R6 / codex
/// P1 #7). `next` is the next seq the segment writer expects; `pending` holds
/// published ranges that arrived before their predecessor (keyed by start seq).
#[derive(Debug, Default)]
pub struct MaterializeSeam {
    /// The next seq the writer expects to materialize (`seq_base` initially, then the
    /// seq just past the last materialized range). `0` ⇒ uninitialized (set on the
    /// first range from `seq_base`).
    pub next: u64,
    /// Ranges `(start, end)` published out of order, awaiting their predecessor.
    /// Keyed by `start`; small in practice (bounded by in-flight same-topic writers).
    pub pending: std::collections::BTreeMap<u64, u64>,
}

/// Sentinel for a recency clock that has never fired.
pub const TS_NEVER: i64 = i64::MIN;

impl TopicState {
    /// Create a fresh topic with the given config, interned id, and epoch.
    pub fn new(
        name: String,
        topic_id: u64,
        config: TopicConfig,
        seq_base: u64,
        epoch: u64,
    ) -> Self {
        // A queue topic carries a materialized lease projection (DESIGN §10); a
        // plain log does not. The claim cursor starts at `seq_base` (the first
        // job seq this topic instance will assign).
        let queue = if config.is_queue() {
            Some(Mutex::new(QueueProjection::new(seq_base)))
        } else {
            None
        };
        TopicState {
            name,
            topic_id,
            config: RwLock::new(config),
            index: RwLock::new(TopicIndex::new(seq_base)),
            floors: RwLock::new(Floors::default()),
            dedupe: RwLock::new(HashMap::new()),
            dedupe_gates: Mutex::new(HashMap::new()),
            head_seq: AtomicU64::new(seq_base.saturating_sub(1)),
            reserved_head: AtomicU64::new(seq_base.saturating_sub(1)),
            seq_base,
            epoch: AtomicU64::new(epoch),
            bytes_retained: AtomicU64::new(0),
            live_count: AtomicU64::new(0),
            last_write_ms: AtomicI64::new(TS_NEVER),
            last_read_ms: AtomicI64::new(TS_NEVER),
            last_consumed_ms: AtomicI64::new(TS_NEVER),
            sched_dirty: std::sync::atomic::AtomicBool::new(false),
            notify: Notify::new(),
            broadcast: BroadcastCache::new(),
            queue,
            append_lock: Mutex::new(()),
            publish_ticket: AtomicU64::new(0),
            publish_gate: std::sync::Mutex::new(0),
            publish_cv: std::sync::Condvar::new(),
            segwriter: None,
            materialize_seam: Mutex::new(MaterializeSeam {
                next: seq_base,
                pending: std::collections::BTreeMap::new(),
            }),
        }
    }

    /// Attach a HOT [`SegmentWriter`] to this topic (durable, writer-backed mode).
    /// Pre-seeds the writer's active segment with any records already in the
    /// index (e.g. after recovery/snapshot load) so the materialization starts
    /// consistent. Called by the engine at topic creation/recovery; absent for a
    /// pure in-memory topic. Takes `&mut self` because it runs during construction
    /// before the topic is shared in an `Arc`.
    pub fn attach_segwriter(&mut self, mut writer: SegmentWriter) {
        // Materialize any pre-existing records (recovery/snapshot) into the
        // writer so its sealed/active state is consistent with the index.
        // Segments mirror the topic's gapless append order, so a deleted middle
        // hole is materialized too (as a tombstone frame, never served) to keep
        // the segment seq run contiguous (a `SegmentBuilder` requires it). We do
        // NOT free resident payloads here (recovery correctness first);
        // steady-state eviction happens as new appends seal segments.
        let index = self.index.read();
        let base = index.base_seq;
        let len = index.records.len() as u64;
        for (i, rec) in index.records.iter().enumerate() {
            let seq = base + i as u64;
            writer.append_record(
                seq,
                rec.ts,
                rec.node.as_deref(),
                rec.tag.as_deref(),
                &rec.data,
                &rec.meta,
            );
        }
        drop(index);
        self.segwriter = Some(Mutex::new(writer));
        // The writer is now seeded through `base + len - 1`; the ordered-materialize
        // seam (R6 / codex P1 #7) must continue from the next seq so a post-restore
        // append materializes in order and never re-feeds a pre-seeded record.
        self.materialize_seam.lock().next = base + len;
    }

    /// Whether this topic is a queue (carries a lease projection).
    pub fn is_queue(&self) -> bool {
        self.queue.is_some()
    }

    pub fn head_seq(&self) -> u64 {
        self.head_seq.load(Ordering::Acquire)
    }

    fn publish_head_seq(&self, new_head: u64, permit: PublishPermit) {
        #[cfg(debug_assertions)]
        if let PublishPermitKind::WalCommitted { fsynced } = permit.kind {
            let class = self.config.read().durability_class();
            if class == Durability::Disk {
                debug_assert!(
                    new_head <= self.reserved_head(),
                    "disk publish requires a durable head reservation"
                );
            }
            if class == Durability::Fsync {
                debug_assert!(fsynced, "fsync publish requires an fsynced WAL proof");
            }
        }
        self.head_seq.store(new_head, Ordering::Release);
    }

    /// The highest seq durably reserved via a fsynced `HeadWatermark` (R3).
    pub fn reserved_head(&self) -> u64 {
        self.reserved_head.load(Ordering::Acquire)
    }

    /// Monotonically raise the durable reservation ceiling after a `HeadWatermark`
    /// is fsynced (R3). Never regresses.
    pub fn set_reserved_head(&self, reserved: u64) {
        self.reserved_head.fetch_max(reserved, Ordering::AcqRel);
    }

    /// **Recovery only**: restore a durable head reservation (R3). If `reserved`
    /// is beyond the seqs actually replayed, advance `head_seq` to it and PAD the
    /// index with deleted-hole tombstones so the next live append assigns
    /// `reserved + 1` (the seq counter never regresses and an already-acked
    /// `disk` seq is never re-handed). The padded seqs read as silent deleted
    /// gaps — exactly the "lost un-fsynced tail" the `disk` class contracts for.
    /// Monotone: a watermark `<=` the recovered head is a no-op. The reservation
    /// ceiling is also restored so post-recovery disk writes resume past it.
    pub fn restore_head_watermark(&self, reserved: u64) {
        self.set_reserved_head(reserved);
        let head = self.head_seq();
        if reserved <= head {
            return; // already covered by replayed appends.
        }
        {
            let mut index = self.index.write();
            // Pad the deque so `base_seq + len == reserved + 1`; the next
            // `stage_append` reserves from the tail and so assigns `reserved + 1`.
            let tail = index.base_seq + index.records.len() as u64; // next free seq.
            let mut next = tail;
            while next <= reserved {
                index.records.push_back(deleted_hole());
                next += 1;
            }
        }
        self.publish_head_seq(reserved, PublishPermit::recovery());
    }

    /// Acquire the in-flight gate for an idempotency `key`, serializing all
    /// concurrent writes carrying the *same* key for the whole reservation (API
    /// §0.8). The returned guard must be held until the write has published and
    /// recorded its dedupe entry; while held, a second same-key writer blocks,
    /// then on acquiring sees the winner's dedupe entry. Different keys never
    /// contend. Returns an `Arc<Mutex<()>>` whose lock is taken (then the inner
    /// guard released) by the caller; the registry entry is dropped lazily by
    /// the last releaser (`release_dedupe_gate`).
    pub fn dedupe_gate_for(&self, key: &str) -> Arc<Mutex<()>> {
        let mut gates = self.dedupe_gates.lock();
        gates
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    /// Drop a per-key gate from the registry once the holder releases it, if no
    /// other writer is still waiting on it (strong_count would be > 1). Keeps the
    /// registry from growing unbounded across distinct keys.
    pub fn release_dedupe_gate(&self, key: &str, gate: &Arc<Mutex<()>>) {
        let mut gates = self.dedupe_gates.lock();
        // The registry holds one ref; the caller holds one. If those are the
        // only two, no concurrent same-key writer is parked on it, so we can
        // safely forget it. (A new same-key write would just re-create it.)
        if let Some(existing) = gates.get(key) {
            if Arc::ptr_eq(existing, gate) && Arc::strong_count(gate) <= 2 {
                gates.remove(key);
            }
        }
    }

    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    /// Next seq an append will receive (`head_seq + 1`).
    pub fn next_seq(&self) -> u64 {
        self.head_seq().saturating_add(1)
    }

    /// Logical earliest retained, deliverable (first live) seq (DESIGN §5.1):
    /// the seq of the first currently-live record, or `head_seq + 1` when the
    /// topic is empty. Driven by cap/TTL **and** deletes.
    ///
    /// A `match`-only delete that removes the front of the log advances
    /// `base_seq` (via lazy front reclaim) without advancing any floor, so the
    /// floors alone under-report `earliest_seq`. Fold in `base_seq` — the first
    /// physically-present (hence first live, since reclaim pops leading holes)
    /// seq — so the reported value tracks all forms of front removal.
    pub fn earliest_seq(&self) -> u64 {
        let head = self.head_seq();
        let floor_earliest = self.floors.read().earliest_seq(self.seq_base, head);
        let base_seq = self.index.read().base_seq;
        floor_earliest.max(base_seq).min(head.saturating_add(1))
    }

    /// The **involuntary** earliest seq (cap/TTL only) — the tombstone trigger
    /// boundary (DESIGN §5.4). Excludes deletions, so a purely-deleted prefix
    /// gap reads silently.
    pub fn evict_earliest_seq(&self) -> u64 {
        let floors = self.floors.read();
        floors.evict_earliest(self.seq_base, self.head_seq())
    }

    /// The current **involuntary** loss floor `max(evict_floor, expiry_floor)` —
    /// the highest seq lost to cap/TTL eviction. Used by the engine to detect when
    /// a retention pass advanced the involuntary floor so it can durably log a
    /// monotone `EvictWatermark` (so a relaxed cap / backward clock never
    /// resurrects an evicted record after restart — codex P0 #2).
    pub fn involuntary_floor(&self) -> u64 {
        let floors = self.floors.read();
        floors.evict_floor.max(floors.expiry_floor)
    }

    /// Resolve a source record at `seq` for derived forwarding,
    /// transparently across the resident slot, the bounded cache, and a cold
    /// segment read. Returns `(record, alive)` where `alive` is `false` for a
    /// deleted/missing slot (a deleted record is not forwarded). `None` if the seq
    /// is physically below the topic's base (reclaimed). Never holds the index lock
    /// across a (possibly slow) segment read (the Phase-6 HARD INVARIANT).
    pub fn forward_lookup(&self, seq: u64) -> Option<(StoredRecord, bool)> {
        // Snapshot the slot's metadata under a brief read lock.
        let (mut rec, resident) = {
            let index = self.index.read();
            let r = index.get(seq)?;
            (r.clone(), r.payload_resident)
        };
        if rec.deleted {
            return Some((rec, false));
        }
        if !resident {
            // Payload was freed after sealing; resolve it off the index lock.
            let resolved = self.resolve_payload(seq, &rec);
            rec.data = resolved.data;
            rec.meta = resolved.meta;
        }
        Some((rec, true))
    }

    /// Record that a derived router could not materialize `count` forwarded records
    /// into this dest because the SOURCE trimmed them (async/derived forwarding,
    /// design §4 source-retention bound). Appends `count` deleted-hole slots so the
    /// dest's seq space carries a real gap at the right deterministic position, and
    /// advances `source_trim_floor` so a dest consumer crossing the gap reads a
    /// `source_trim` tombstone rather than a silent skip. Returns the first/last
    /// hole seqs (empty range for `count == 0`).
    pub fn note_source_trim(&self, now_ms: i64) {
        // One-hole step used by the engine per trimmed source record; the engine
        // calls this exactly the number of trimmed records (see `advance_router`).
        let _ = now_ms;
        let staged = self.stage_append(vec![deleted_hole()]);
        if staged.is_empty() {
            return;
        }
        let hole_seq = staged.start;
        // Publish the hole (advances head) and raise the source-trim floor to it.
        self.publish_head_seq(hole_seq, PublishPermit::derived());
        {
            let mut floors = self.floors.write();
            if hole_seq > floors.source_trim_floor {
                floors.source_trim_floor = hole_seq;
            }
        }
        // Materialize the hole into the segment writer through the ordered seam (R6 /
        // codex P1 #7), exactly like a deleted middle hole, so the seam's `next`
        // advances past it and a following derived record materializes contiguously
        // (otherwise the live record's range would start past `next` and stall
        // forever in the out-of-order buffer). A writer-less topic is a no-op.
        self.materialize_published(hole_seq, hole_seq);
        self.notify.notify_waiters();
    }

    /// Read a recency clock, mapping the sentinel to `None`.
    pub fn read_ts(value: &AtomicI64) -> Option<i64> {
        match value.load(Ordering::Relaxed) {
            TS_NEVER => None,
            v => Some(v),
        }
    }

    /// Append `records` with seqs assigned in order, publishing them visible
    /// **immediately** (no WAL durability gate). Returns the assigned seqs.
    ///
    /// This is the in-memory / recovery / derived-append convenience: it stages
    /// the records into the index and publishes them in one shot. The durable
    /// write path does NOT use this — it must hold nothing visible until the WAL
    /// frame is fsynced (the WAL-first reservation rule, ARCHITECTURE §2.2). For
    /// that, use [`Self::stage_append`] + [`Self::publish_staged`] /
    /// [`Self::rollback_staged`].
    pub fn append(&self, records: Vec<StoredRecord>, now_ms: i64) -> Vec<u64> {
        let staged = self.stage_append(records);
        if staged.is_empty() {
            return Vec::new();
        }
        let seqs = staged.seqs();
        self.publish_staged(staged, now_ms, PublishPermit::resident());
        seqs
    }

    /// **Stage** `records` into the index without publishing them: contiguous
    /// seqs are assigned starting at the DEQUE TAIL (`base_seq + records.len()`),
    /// which equals `head_seq + 1` only when no earlier batch is staged-but-
    /// unpublished — `head_seq` excludes unpublished stages, so the tail (not
    /// head) is the correct reservation point under concurrent stagers (see the
    /// body comment). The records are pushed into the index deque + tag index, but
    /// `head_seq` is **not** advanced and no waiter is notified — so a concurrent
    /// reader (which gates on `head_seq`) observes NOTHING yet (the WAL-first
    /// reservation rule, ARCHITECTURE §2.2).
    ///
    /// The caller MUST hold the topic's `append_lock` across the SEQ-ORDER critical
    /// section only — stage → enqueue the WAL frame(s) → take a publish ticket
    /// ([`Self::next_publish_ticket`]) — so a topic's WAL frames are enqueued in seq
    /// order. The fsync `wait()` then happens OFF the lock (codex P0 #1, so
    /// concurrent durable writers coalesce into one group commit), and publish /
    /// rollback is gated back into strict seq order by the ticket
    /// ([`Self::publish_wait_turn`] then [`Self::publish_staged`] on success, or
    /// [`Self::rollback_staged_by_seqs`] on a WAL/fsync failure). Because a LATER
    /// writer may stage past this batch once the lock is dropped (before this batch
    /// publishes), a post-lock rollback must target THIS batch's seqs in place
    /// (`rollback_staged_by_seqs`), not a tail truncation; [`Self::rollback_staged`]
    /// (the tail truncation) is only valid while the lock is still held and no
    /// ticket was taken (an enqueue failure inside the critical section). Either
    /// way nothing visible is non-durable: not acknowledged ⇒ not committed.
    pub fn stage_append(&self, records: Vec<StoredRecord>) -> StagedAppend {
        if records.is_empty() {
            return StagedAppend::empty();
        }
        let n = records.len() as u64;
        let mut index = self.index.write();
        let pre_len = index.records.len();
        // Reserve seqs from the DEQUE TAIL (`base_seq + len`), NOT `head_seq + 1`:
        // with the fsync now waiting off the `append_lock` (codex P0 #1), a later
        // writer can stage while an earlier writer has staged-but-not-yet-published
        // (head_seq not yet advanced). The deque tail already accounts for those
        // unpublished stages (they were pushed under this same index write lock),
        // so it yields contiguous, gapless seqs across concurrent stagers. In the
        // serial case this equals `head_seq + 1` exactly (head == base + len - 1
        // right after a publish), so single-writer behavior is unchanged.
        let start = index.base_seq + index.records.len() as u64;
        let mut added_bytes: u64 = 0;
        for (seq, rec) in (start..).zip(records) {
            added_bytes = added_bytes.saturating_add(rec.bytes);
            if let Some(tag) = &rec.tag {
                index.index_tag(seq, tag);
            }
            index.records.push_back(rec);
        }
        StagedAppend {
            start,
            count: n,
            added_bytes,
            pre_len,
        }
    }

    /// **Publish** a previously [`stage`](Self::stage_append)d batch: advance
    /// `head_seq` (making the records visible), account retained bytes/live
    /// count, set the write-recency clock, materialize the records into the HOT
    /// segment writer, and wake SSE/diff long-pollers. The caller must provide a
    /// [`PublishPermit`] showing which trusted path made visibility legal: WAL
    /// commit, resident-only publication, derived replay, or recovery padding.
    pub fn publish_staged(&self, staged: StagedAppend, now_ms: i64, permit: PublishPermit) {
        if let Some((start, end)) = self.publish_staged_no_seal(staged, now_ms, permit) {
            // Materialize + seal inline (the convenience path: `append`, in-memory
            // helpers, tests). Routed through the ORDERED seam (R6 / codex P1 #7) so
            // the inline path and the gated path share one materialization cursor —
            // a topic never mixes a direct `materialize_segment` (which would desync the
            // seam's `next`) with the ordered `materialize_published`. The gated write
            // paths call `publish_staged_no_seal` + `materialize_published` directly
            // AFTER releasing the publish gate (keep the seal fsync off the gate).
            self.materialize_published(start, end);
        }
    }

    /// **Publish** a staged batch but DO NOT materialize/seal: advance `head_seq`,
    /// account bytes/count, set the write-recency clock, and wake waiters. Returns
    /// `Some((start, end))` of the published range so the caller can run
    /// [`Self::materialize_segment`] **after releasing the publish gate** (R6: the
    /// segment seal `put` fsync no longer serializes same-topic writers behind the
    /// gate, which only orders head advances now). Returns `None` for an empty
    /// batch. Crash-safety is preserved for WAL-backed callers by requiring a
    /// commit-derived permit before visibility advances; a segment is a derivable
    /// materialization, and resident payloads are freed only after the seal `put`
    /// returns Ok inside `materialize_segment`.
    pub fn publish_staged_no_seal(
        &self,
        staged: StagedAppend,
        now_ms: i64,
        permit: PublishPermit,
    ) -> Option<(u64, u64)> {
        if staged.is_empty() {
            return None;
        }
        let new_head = staged.start + staged.count - 1;
        // Publish the new head_seq after the records are in the index so a
        // concurrent reader that observes the higher head also finds the slots.
        self.publish_head_seq(new_head, permit);

        self.bytes_retained
            .fetch_add(staged.added_bytes, Ordering::Relaxed);
        self.live_count.fetch_add(staged.count, Ordering::Relaxed);
        self.last_write_ms.store(now_ms, Ordering::Relaxed);

        // Wake long-pollers (diff `wait_ms`) and SSE streams.
        self.notify.notify_waiters();
        Some((staged.start, new_head))
    }

    /// Materialize a published `[start, end]` range into the HOT segment writer and
    /// free the resident payloads of any seqs sealing crossed. Safe to run OFF the
    /// publish gate (R6) — purely derivable from the already-published in-memory
    /// index. No-op for a writer-less topic. Public so the gated write paths can run
    /// it after `ticket.done()`.
    ///
    /// ORDERED (R6 / codex P1 #7): because the seal is off the publish gate, a later
    /// writer can call this for `N+1` before the earlier writer calls it for `N`.
    /// [`SegmentWriter`] requires strictly monotonic append order, so this admits a
    /// range to the writer ONLY when it starts exactly at the seam's `next`; an
    /// earlier-arriving later range is buffered and drained once its predecessor
    /// lands. A range entirely below `next` (already materialized — e.g. a duplicate
    /// after recovery) is dropped. The actual writer feed (`materialize_segment`)
    /// runs WITHOUT holding the seam lock, so a slow seal still never serializes the
    /// publish gate; only the cheap in-order bookkeeping is under the seam.
    pub fn materialize_published(&self, start: u64, end: u64) {
        if self.segwriter.is_none() {
            return; // writer-less topic: nothing to materialize, nothing to order.
        }
        // Collect the contiguous prefix of ranges to feed, under the seam lock, then
        // feed them off the lock.
        let mut to_feed: Vec<(u64, u64)> = Vec::new();
        {
            let mut seam = self.materialize_seam.lock();
            // Seam `next` defaults to seq_base; clamp this range's effective start up
            // to `next` so an overlapping/duplicate prefix is never re-fed.
            if end < seam.next {
                return; // wholly already materialized.
            }
            let eff_start = start.max(seam.next);
            if eff_start == seam.next {
                // In order: feed it now and advance the seam.
                to_feed.push((eff_start, end));
                seam.next = end + 1;
            } else {
                // Out of order: buffer until the gap before it fills. Keep the
                // largest end for a given start (idempotent re-publish is rare).
                let e = seam.pending.entry(start).or_insert(end);
                *e = (*e).max(end);
            }
            // Drain any now-contiguous buffered ranges.
            while let Some((&pstart, &pend)) = seam.pending.range(..=seam.next).next_back() {
                if pstart > seam.next {
                    break;
                }
                seam.pending.remove(&pstart);
                if pend < seam.next {
                    continue; // fully subsumed already.
                }
                let feed_start = seam.next;
                to_feed.push((feed_start, pend));
                seam.next = pend + 1;
            }
        }
        for (s, e) in to_feed {
            self.materialize_segment(s, e);
        }
    }

    /// **Roll back** a staged batch whose WAL append/fsync FAILED: pop the staged
    /// records (the contiguous tail of the deque — the caller holds `append_lock`,
    /// so nothing was appended after them) and prune their tag postings. `head_seq`
    /// was never advanced, so the records were never visible; this leaves the topic
    /// exactly as it was before [`Self::stage_append`]. The acknowledgement never
    /// happens (the caller returns an error), so the contract holds: not
    /// acknowledged ⇒ not committed, and nothing visible-but-not-durable remains.
    pub fn rollback_staged(&self, staged: StagedAppend) {
        if staged.is_empty() {
            return;
        }
        let mut index = self.index.write();
        // The staged records are the tail past `pre_len`; truncate back to it,
        // pruning each popped record's tag posting (reusing `unindex_tag`).
        while index.records.len() > staged.pre_len {
            let idx = index.records.len() - 1;
            let seq = index.base_seq + idx as u64;
            let tag = index.records[idx].tag.clone();
            index.records.pop_back();
            if let Some(tag) = tag {
                index.unindex_tag(&tag, seq);
            }
        }
    }

    /// Reserve the next commit ticket for this writer, wrapped in an RAII
    /// [`PublishGuard`] (R14). MUST be called while holding `append_lock`
    /// (immediately after `stage_append`), so tickets are handed out in exactly
    /// the same order seqs were assigned + WAL frames enqueued. The guard's
    /// [`PublishGuard::wait_turn`]/[`PublishGuard::done`] enforce in-order publish
    /// off the lock (codex P0 #1); crucially, if the writer PANICS (or returns
    /// early) between taking the ticket and calling `done`, the guard's `Drop`
    /// advances the gate on unwind so a panicking ticketed writer can never hang
    /// [`Self::quiesce_publishes`] or strand every later writer behind a ticket
    /// that is never released.
    pub fn next_publish_ticket(&self) -> PublishGuard<'_> {
        let ticket = self.publish_ticket.fetch_add(1, Ordering::Relaxed);
        PublishGuard {
            topic_state: self,
            ticket,
            waited: false,
            done: false,
            on_unwind: UnwindAction::None,
        }
    }

    /// Block until it is `ticket`'s turn to publish/rollback (its value equals
    /// `publish_next`). Used by the [`PublishGuard`] and its `Drop` path.
    fn publish_wait_turn(&self, ticket: u64) {
        let mut next = self.publish_gate.lock().unwrap();
        while *next != ticket {
            next = self.publish_cv.wait(next).unwrap();
        }
    }

    /// Advance the publish gate past `ticket`, waking the next writer in line.
    /// Called exactly once per ticket (by the [`PublishGuard`] or its `Drop`),
    /// only after this ticket's turn arrived (`*next == ticket`).
    fn publish_done(&self, ticket: u64) {
        let mut next = self.publish_gate.lock().unwrap();
        debug_assert_eq!(*next, ticket, "publish gate advanced out of order");
        *next = ticket.wrapping_add(1);
        self.publish_cv.notify_all();
    }

    /// Block until every ALREADY-TICKETED write has finished publishing/rolling
    /// back (`publish_next == publish_ticket`). The caller MUST hold
    /// `append_lock`, so no NEW ticket can be issued while we wait — the in-flight
    /// set only shrinks, so this always terminates. Used by snapshot capture: with
    /// the fsync now waiting off the `append_lock` (codex P0 #1), holding the lock
    /// alone no longer means the topic is quiescent (a writer may be mid-fsync,
    /// staged-but-unpublished, with its WAL frame already before the checkpoint
    /// offset). Quiescing the publish gate guarantees `head_seq` covers every such
    /// in-flight frame, so the snapshot never excludes an acked write the
    /// checkpoint position already covers.
    pub fn quiesce_publishes(&self) {
        let target = self.publish_ticket.load(Ordering::Relaxed);
        let mut next = self.publish_gate.lock().unwrap();
        while *next != target {
            next = self.publish_cv.wait(next).unwrap();
        }
    }

    /// **Roll back** a staged batch whose WAL append/fsync FAILED, robust to
    /// concurrent same-topic staging (codex P0 #1): because the fsync now waits off
    /// the `append_lock`, a *later* writer may have staged records past this batch
    /// in the deque, so a tail truncation would wrongly drop the later writer's
    /// records. Instead mark each of THIS batch's seqs deleted in place (freeing
    /// its payload + tag posting, just like [`Self::rollback_staged`] does for the
    /// tail), leaving any later-staged records untouched. `head_seq` was never
    /// advanced to include these seqs by this (failed) writer, and ordered publish
    /// guarantees no later writer published before this rollback ran, so the
    /// records were never visible. When a later writer subsequently advances head
    /// past them, they read silently as a deleted gap (DESIGN §6/§7) — exactly
    /// like any other deleted record. Not acknowledged ⇒ not committed.
    pub fn rollback_staged_by_seqs(&self, staged: &StagedAppend) {
        if staged.is_empty() {
            return;
        }
        let mut index = self.index.write();
        for i in 0..staged.count {
            let seq = staged.start + i;
            index.mark_deleted_pub(seq);
        }
    }

    /// Feed the records `[start, end]` to the HOT segment writer and free the
    /// resident payloads of any seqs that sealing pushed into an immutable
    /// segment. No-op for a topic without a writer. The index lock is taken only
    /// briefly to read the just-committed values and (separately) to free sealed
    /// payloads — never held across the writer's segment `put`/`read`.
    fn materialize_segment(&self, start: u64, end: u64) {
        let Some(sw) = &self.segwriter else { return };

        // Read the committed values out of the index (a short read lock), then
        // feed them to the writer with the index lock released.
        struct Pending {
            seq: u64,
            ts: i64,
            node: Option<String>,
            tag: Option<String>,
            data: serde_json::Value,
            meta: Option<serde_json::Value>,
        }
        // Capture every seq in the freshly-appended range (contiguous, gapless —
        // the segment mirrors the topic's append order). A just-appended record is
        // always present and non-deleted, so the resident payload is exact.
        let pending: Vec<Pending> = {
            let index = self.index.read();
            (start..=end)
                .filter_map(|seq| {
                    index.get(seq).map(|r| Pending {
                        seq,
                        ts: r.ts,
                        node: r.node.clone(),
                        tag: r.tag.clone(),
                        data: r.data.clone(),
                        meta: r.meta.clone(),
                    })
                })
                .collect()
        };

        let mut sealed_seqs: Vec<u64> = Vec::new();
        let evict_resident;
        {
            let mut writer = sw.lock();
            evict_resident = writer.evicts_resident();
            for p in &pending {
                sealed_seqs.extend(writer.append_record(
                    p.seq,
                    p.ts,
                    p.node.as_deref(),
                    p.tag.as_deref(),
                    &p.data,
                    &p.meta,
                ));
            }
        }

        // Free the resident payloads of the seqs that just sealed (the cache /
        // segment now serves them). `bytes`/`count`/floors are untouched — the
        // records are still live; only the payload's home moved.
        if evict_resident && !sealed_seqs.is_empty() {
            let mut index = self.index.write();
            let base = index.base_seq;
            for seq in sealed_seqs {
                if seq < base {
                    continue;
                }
                let i = (seq - base) as usize;
                if let Some(rec) = index.records.get_mut(i) {
                    if !rec.deleted && rec.payload_resident {
                        rec.payload_resident = false;
                        rec.data = Value::Null;
                        rec.meta = None;
                    }
                }
            }
        }
    }

    /// Resolve a record's payload `(data, meta)` for serving (diff/SSE/queue),
    /// transparently across the resident slot, the bounded cache, and a segment
    /// read (Phase 6 memory bounding). `rec` is the in-memory slot at `seq`.
    ///
    /// - If the slot still holds its payload (`payload_resident`) — the common
    ///   tail/active case and every writer-less topic — clone it directly.
    /// - Otherwise the payload was freed after sealing: resolve it from the
    ///   writer's bounded cache (hot) or a segment `read_range` (cold-ish). This
    ///   runs off the index write lock so a slow read never blocks writes/delivery.
    ///
    /// Falls back to the slot's (possibly `Null`) payload if the writer cannot
    /// resolve it (defensive — should not happen for a sealed live record).
    pub fn resolve_payload(&self, seq: u64, rec: &StoredRecord) -> ResolvedPayload {
        if rec.payload_resident {
            return ResolvedPayload {
                data: rec.data.clone(),
                meta: rec.meta.clone(),
            };
        }
        if let Some(sw) = &self.segwriter {
            // Capture a cache hit or a locator under the (brief) writer lock, then
            // do any actual segment read with the lock RELEASED so a slow cold
            // fetch never gates writes/delivery (the Phase-6 HARD INVARIANT).
            let resolve = sw.lock().resolve_sealed_fast(seq);
            match resolve {
                SealedResolve::Hit(p) => return p,
                SealedResolve::Read(loc) => {
                    if let Some(p) = read_locator(&loc) {
                        sw.lock().record_cold_read(&loc, &p);
                        return p;
                    }
                }
                SealedResolve::NotSealed => {}
            }
        }
        ResolvedPayload {
            data: rec.data.clone(),
            meta: rec.meta.clone(),
        }
    }

    /// Recompute eviction/expiry floors against caps + TTL at `now_ms`, drain
    /// the index front, and update retained bytes (DESIGN §5.2/§5.3).
    ///
    /// Idempotent: safe to call on both the write and read paths. After it runs,
    /// the physically-present records equal the logically-retained set.
    ///
    /// Returns a [`RetentionAdvance`] describing whether the involuntary loss
    /// floor advanced this pass and, if so, whether a NON-RE-DERIVABLE cause drove
    /// it (R7): a `cap_records` floor is a pure function of `head - cap_records`
    /// and is re-derived for free on restart, but a TTL expiry (clock-driven) or a
    /// byte-cap eviction (depends on physically-retained bytes at the time) is NOT
    /// reconstructible from the recovered head alone, so the caller must DURABLY
    /// persist the resolved floor or a relaxed cap / backward clock could resurrect
    /// an evicted record after a crash.
    pub fn enforce_retention(&self, now_ms: i64) -> RetentionAdvance {
        // Default path: no durable hardening hook. Commits the planned floors and
        // reclaims unconditionally. Used by recovery/internal callers and the
        // engine's best-effort path; the DURABLE non-re-derivable hazard (R7 /
        // codex P0 #4) is handled by `enforce_retention_hardened`, which fsyncs the
        // EvictWatermark BEFORE committing/reclaiming.
        self.enforce_retention_hardened(now_ms, |_| Ok::<(), crate::error::Error>(()))
            .expect("no-op harden never fails")
    }

    /// As [`Self::enforce_retention`], but `harden` is invoked with the PLANNED
    /// [`RetentionAdvance`] **before** any floor is committed or any record is
    /// physically reclaimed, and ONLY when a NON-RE-DERIVABLE cause (TTL expiry or
    /// byte-cap eviction) drove the advance (`durable_advance`). This is the R7 /
    /// codex P0 #4 fix: the eviction watermark must be durable BEFORE the in-memory
    /// floor advances and the records are reclaimed, otherwise a watermark fsync
    /// failure (or a crash in the window) would leave the floor advanced in memory
    /// (consumers saw `earliest_seq` move) but not durable, so a restart would
    /// regress the floor and resurrect the evicted records.
    ///
    /// If `harden` returns an error, NOTHING is committed: the floors are left
    /// exactly as they were and no record is reclaimed, and the error is returned
    /// so the caller can propagate it (instead of silently serving a tombstone for
    /// an un-hardened floor). A re-derivable advance (records-cap only) or no
    /// advance never calls `harden` and always commits.
    ///
    /// Plan→harden→commit is race-safe because the involuntary floors are
    /// MONOTONIC: the plan computes a target floor from a consistent snapshot, the
    /// commit re-takes the lock and raises the floor to `max(current, planned)`, so
    /// a concurrent advance (which hardened its own watermark) is never lowered and
    /// the committed floor never exceeds what was durably hardened.
    pub fn enforce_retention_hardened<F, E>(
        &self,
        now_ms: i64,
        harden: F,
    ) -> std::result::Result<RetentionAdvance, E>
    where
        F: FnOnce(&RetentionAdvance) -> std::result::Result<(), E>,
    {
        let config = self.config.read();
        let ttl_ms = config.ttl_ms;
        let cap_records = config.cap_records;
        let cap_bytes = config.cap_bytes;
        drop(config);

        let head = self.head_seq();
        if head == 0 {
            return Ok(RetentionAdvance::NONE); // empty topic, nothing retained.
        }

        // Hot read-path fast path (codex P2 #11): a topic with NO TTL and NO caps
        // has no involuntary floor that this call could advance, and every delete
        // already reclaims its own dead front (`delete_*`/`delete_seqs` call
        // `reclaim_front` directly), so there is no pending front prefix for this
        // call to drain either. Skip the index read + floors write lock entirely.
        if ttl_ms == 0 && cap_records == 0 && cap_bytes == 0 {
            return Ok(RetentionAdvance::NONE);
        }

        // --- PLAN (no mutation): compute the target floors from a consistent
        // snapshot under a READ lock, without advancing anything yet (R7 / codex
        // P0 #4). The durable watermark (if any) is hardened before we commit.
        let plan = {
            let index = self.index.read();
            let floors = self.floors.read();
            let mut floor_advanced = false;
            let mut durable_advance = false;
            let mut evict_floor = floors.evict_floor;
            let mut expiry_floor = floors.expiry_floor;

            // --- TTL: advance expiry_floor past every expired record. -----------
            // `$ts` is non-decreasing in seq, so all seqs <= X expired is a prefix
            // predicate; scan the index front (amortized O(1) under steady state).
            if ttl_ms > 0 {
                let ttl = ttl_ms as i64;
                let base = index.base_seq;
                let mut expired_upto = expiry_floor;
                for (i, rec) in index.records.iter().enumerate() {
                    if now_ms.saturating_sub(rec.ts) > ttl {
                        expired_upto = base + i as u64;
                    } else {
                        break; // first non-expired; the rest are younger.
                    }
                }
                if expired_upto > expiry_floor {
                    expiry_floor = expired_upto;
                    floor_advanced = true;
                    // TTL is clock-driven and NOT re-derivable from the recovered
                    // head: the floor must be durably persisted (R7).
                    durable_advance = true;
                }
            }

            // --- Cap (records): keep at most cap_records retained. --------------
            if cap_records > 0 && head > cap_records {
                let want_floor = head - cap_records; // highest seq to evict.
                if want_floor > evict_floor {
                    evict_floor = want_floor;
                    floor_advanced = true;
                    // A records-cap floor is `head - cap_records`, re-derived for
                    // free on restart, so it needs no durable watermark of its own.
                }
            }

            // --- Cap (bytes): evict oldest physically-present records until the
            // retained byte total is within cap_bytes. ---------------------------
            if cap_bytes > 0 {
                let retained_bytes = self.bytes_retained.load(Ordering::Relaxed);
                if retained_bytes > cap_bytes {
                    let mut over = retained_bytes - cap_bytes;
                    let base = index.base_seq;
                    let current_floor = evict_floor.max(expiry_floor);
                    let mut evict_to = evict_floor;
                    for (i, rec) in index.records.iter().enumerate() {
                        let seq = base + i as u64;
                        if seq <= current_floor {
                            continue; // already logically gone.
                        }
                        if over == 0 {
                            break;
                        }
                        over = over.saturating_sub(rec.bytes);
                        evict_to = seq;
                        if over == 0 {
                            break;
                        }
                    }
                    if evict_to > evict_floor {
                        evict_floor = evict_to;
                        floor_advanced = true;
                        // A byte-cap eviction depends on the physically-retained
                        // bytes at this instant, NOT on the head, so it is not
                        // re-derivable and must be durably persisted (R7).
                        durable_advance = true;
                    }
                }
            }

            RetentionAdvance {
                floor_advanced,
                durable_advance,
                evict_floor,
                expiry_floor,
                bytes_reclaimed: 0,
            }
        };

        // When a NEW floor advanced this pass:
        //   * HARDEN (off the floors lock) the durable watermark BEFORE committing
        //     the floor / reclaiming (R7 / codex P0 #4). On failure, commit nothing
        //     and propagate the error.
        //   * COMMIT the floors monotonically (never lower a concurrently advanced
        //     floor), then segment-reclaim (only a NEW advance can drop a whole
        //     sealed segment below the floor).
        // The lazy FRONT reclaim then ALWAYS runs (below), regardless of whether a
        // new floor advanced this pass: the index front may already sit below the
        // CURRENT floor — e.g. a floor restored by recovery's `EvictWatermark`
        // replay, where this is the first retention pass and `plan.floor_advanced`
        // is false (the clock may be rewound) — and that dead prefix must still be
        // popped so `count`/`earliest_seq` reflect the durable floor.
        if plan.floor_advanced {
            if plan.durable_advance {
                harden(&plan)?;
            }
            {
                let mut floors = self.floors.write();
                if plan.evict_floor > floors.evict_floor {
                    floors.evict_floor = plan.evict_floor;
                }
                if plan.expiry_floor > floors.expiry_floor {
                    floors.expiry_floor = plan.expiry_floor;
                }
            }
            self.reclaim_segments();
        }
        // Lazy front reclaim: pop the dead prefix physically (below the current
        // floor), advancing base_seq. Cheap no-op (`drop_n == 0`) when the front is
        // already clean.
        let mut result = plan;
        result.bytes_reclaimed = self.reclaim_front(head);
        Ok(result)
    }

    /// Physically pop the fully-dead front prefix (below `earliest_seq` or a run
    /// of already-deleted slots at the front), advancing `base_seq` and pruning
    /// their tag postings. Lazy reclaim shared by cap/TTL eviction and deletes
    /// (DESIGN §7, ARCHITECTURE §1.1). `head` is the topic head at call time.
    fn reclaim_front(&self, head: u64) -> u64 {
        let earliest = {
            let floors = self.floors.read();
            floors.earliest_seq(self.seq_base, head)
        };
        let mut index = self.index.write();
        let base = index.base_seq;

        // Pop everything strictly below the logical floor, plus any further run
        // of already-deleted slots that now sits at the front.
        let mut drop_n = if earliest > base {
            ((earliest - base) as usize).min(index.records.len())
        } else {
            0
        };
        while drop_n < index.records.len() && index.records[drop_n].deleted {
            drop_n += 1;
        }
        if drop_n == 0 {
            return 0;
        }
        let mut freed: u64 = 0;
        for rec in index.records.iter().take(drop_n) {
            freed = freed.saturating_add(rec.bytes);
        }
        let live_popped = index.drain_front(drop_n);
        drop(index);
        if freed > 0 {
            // saturating: bytes_retained is the authoritative retained sum.
            let prev = self.bytes_retained.load(Ordering::Relaxed);
            self.bytes_retained
                .store(prev.saturating_sub(freed), Ordering::Relaxed);
        }
        if live_popped > 0 {
            let prev = self.live_count.load(Ordering::Relaxed);
            self.live_count
                .store(prev.saturating_sub(live_popped), Ordering::Relaxed);
        }
        freed
    }

    /// Segment-granular physical reclaim (ARCHITECTURE §3.3, §5.6): drop whole
    /// **sealed** segment files (HOT or COLD) once every record they cover is dead
    /// — fully evicted/expired (below the live floor) or fully point-deleted (an
    /// interior segment cleared by a `match` delete). Cap/TTL/delete reclaim is
    /// segment-granular: it never rewrites a segment, it just unlinks the whole
    /// `.data`+`.idx` pair (in whichever tier holds it) and advances the watermark
    /// the in-memory floors already carry.
    ///
    /// This runs **off the hot path**: the index read lock is taken only to test
    /// segment dead-ness (a bounded scan over the candidate's seq range), and the
    /// segwriter lock is taken only to plan + drop — never held across a (possibly
    /// slow, cold) read, and never gating a concurrent write/delivery. A topic with
    /// no writer (pure in-memory / non-durable) skips all of this. Idempotent: a
    /// drop of an already-dropped segment is a no-op, so it is crash-safe to
    /// re-derive and re-run on restart.
    pub fn reclaim_segments(&self) {
        let Some(sw) = &self.segwriter else { return };

        // The active (unsealed) segment is never dropped; only sealed ones are
        // candidates. Snapshot the sealed registry under the (brief) writer lock.
        let sealed = {
            let w = sw.lock();
            w.sealed_segments()
        };
        if sealed.is_empty() {
            return;
        }

        // The live floor: the first currently-live seq. A sealed segment entirely
        // below it (`end_seq < earliest`) is fully gone via cap/TTL/prefix delete.
        let earliest = self.earliest_seq();

        // Decide droppable segments. The common fast path is the contiguous run of
        // oldest segments fully below `earliest` (cap/TTL/prefix delete). We also
        // catch an interior segment all of whose records were point-deleted
        // (`range_all_dead`), so a `match` delete that clears a whole segment
        // reclaims it silently — but only probe the index for segments not already
        // covered by the cheap floor test.
        let mut to_drop: Vec<u64> = Vec::new();
        {
            let index = self.index.read();
            for seg in &sealed {
                let dead =
                    seg.end_seq < earliest || index.range_all_dead(seg.start_seq, seg.end_seq);
                if dead {
                    to_drop.push(seg.start_seq);
                }
            }
        }
        if to_drop.is_empty() {
            return;
        }

        // Drop each fully-dead segment from whichever tier holds it (idempotent).
        let mut w = sw.lock();
        for id in to_drop {
            w.drop_segment(id);
        }
    }

    /// Flip the on-disk **delete-flag byte** for each just-deleted seq that lives in
    /// a sealed segment, so the deletion is durable in the segment file itself and
    /// survives a WAL trim/checkpoint that drops the Delete frame (DESIGN §7, the
    /// segment side). A WHOLE-SEGMENT clear is handled by [`Self::reclaim_segments`]
    /// (which unlinks the entire `.data`+`.idx` pair in ONE op); this per-record
    /// flip handles a PARTIALLY-cleared segment — only the seqs still on disk (their
    /// whole segment was not dropped) are flipped. Crash-safe per byte; best-effort
    /// (a flip failure leaves the WAL frame + in-memory mark as the witnesses). A
    /// topic without a writer, or a seq still in the active (unsealed) tail, is a
    /// no-op. Runs off the index lock (only the writer lock is taken per flip).
    pub fn flag_sealed_deletes(&self, seqs: &[u64]) {
        let Some(sw) = &self.segwriter else { return };
        if seqs.is_empty() {
            return;
        }
        let mut w = sw.lock();
        for &seq in seqs {
            // `flag_sealed_deleted` is a no-op for an active-tail / dropped-segment
            // seq (returns false) and otherwise flips + fsyncs the on-disk byte.
            w.flag_sealed_deleted(seq);
        }
    }

    /// **Recovery**: re-derive sealed-record deletions from the on-disk segment
    /// **delete-flag bytes** (DESIGN §7, the segment side). A sealed-record delete
    /// flips an on-disk byte that survives a WAL trim/checkpoint, so on restart the
    /// engine reads those flags back and marks the corresponding seqs deleted in the
    /// index — the deletion no longer depends on a retained WAL Delete frame. Marks
    /// each flagged seq deleted in place (freeing its payload/tag, adjusting
    /// `bytes`/`count`), exactly like the live-path delete, then reclaims the now-
    /// dead front. Idempotent (an already-deleted slot is skipped) and crash-safe to
    /// re-run. A no-op for a topic without a writer or with no on-disk-flagged records.
    pub fn apply_ondisk_segment_deletes_on_recovery(&self) {
        let Some(sw) = &self.segwriter else { return };
        let flagged = sw.lock().scan_ondisk_deleted();
        if flagged.is_empty() {
            return;
        }
        let head = self.head_seq();
        let mut freed_bytes: u64 = 0;
        let mut deleted: u64 = 0;
        {
            let mut index = self.index.write();
            for seq in &flagged {
                if let Some(f) = index.mark_deleted_pub(*seq) {
                    freed_bytes = freed_bytes.saturating_add(f);
                    deleted += 1;
                }
            }
        }
        if freed_bytes > 0 {
            let prev = self.bytes_retained.load(Ordering::Relaxed);
            self.bytes_retained
                .store(prev.saturating_sub(freed_bytes), Ordering::Relaxed);
        }
        if deleted > 0 {
            let prev = self.live_count.load(Ordering::Relaxed);
            self.live_count
                .store(prev.saturating_sub(deleted), Ordering::Relaxed);
        }
        self.reclaim_front(head);
    }

    /// On-restart segment reclaim (ARCHITECTURE §4 step 5): after recovery rebuilt
    /// this topic's index + floors + segment registry, drop (1) any registered sealed
    /// segment now fully below the live set, and (2) any **orphan** segment object
    /// left on disk (a pre-crash reclaim whose unlink never completed). Idempotent,
    /// off the hot path; a no-op for a topic without a writer. Returns the orphan
    /// count dropped (registry drops go through the normal `reclaim_segments`).
    ///
    /// FIRST re-derives sealed-record deletions from the on-disk segment delete-flag
    /// bytes ([`Self::apply_ondisk_segment_deletes_on_recovery`]) so a deletion whose
    /// WAL frame was trimmed is recovered from the segment file, THEN the fully-dead
    /// segment drop runs (a segment all of whose records are on-disk-flagged dead is
    /// then dropped whole).
    pub fn reclaim_segments_on_recovery(&self) -> usize {
        // Re-derive sealed deletions from the on-disk flags before reclaiming, so a
        // segment cleared on disk (but whose WAL Delete frame was trimmed) is seen as
        // dead by the reclaim pass below.
        self.apply_ondisk_segment_deletes_on_recovery();
        // Then the normal registry reclaim (fully-dead registered segments).
        self.reclaim_segments();
        let Some(sw) = &self.segwriter else { return 0 };
        // Then sweep on-disk orphans strictly below the recovered live floor.
        let floor = self.index.read().base_seq;
        sw.lock().reclaim_orphans_below(floor)
    }

    /// Current retained, **live** record count — net of deletions (DESIGN §5.6,
    /// §7). Maintained as a running counter so middle-of-log deletes (which keep
    /// a hole slot) are excluded.
    pub fn count(&self) -> u64 {
        self.live_count.load(Ordering::Relaxed)
    }

    /// Current retained payload bytes (approximate under lazy eviction).
    pub fn bytes(&self) -> u64 {
        self.bytes_retained.load(Ordering::Relaxed)
    }

    /// Apply a permanent, point-in-time, silent delete (DESIGN §7, API §5).
    ///
    /// Deletes records selected by `before_seq` (every live record with
    /// `seq < before_seq`) AND/OR `match` (every live record whose tag matches,
    /// bounded by the head at call time). At least one selector is supplied by
    /// the caller (the engine validates `>=1`). Returns the number of records
    /// removed by this call.
    ///
    /// Mechanics:
    /// - Frees each matched slot's payload/tag, subtracts its bytes, decrements
    ///   `live_count`, and prunes the tag index (O(matched), never a full scan).
    /// - Advances `delete_below` and `delete_floor` for the prefix removed by
    ///   `before_seq` — this advances `earliest_seq` but **never** `evict_floor`
    ///   (silent, no tombstone).
    /// - Triggers lazy front reclaim of the now-dead prefix.
    pub fn apply_delete(
        &self,
        before_seq: Option<u64>,
        match_: Option<&Filter>,
        bound_head: Option<u64>,
        now_ms: i64,
    ) -> u64 {
        self.apply_delete_stats(before_seq, match_, bound_head, now_ms)
            .deleted
    }

    /// As [`Self::apply_delete`], with byte-free accounting for engine quota gauges.
    pub fn apply_delete_stats(
        &self,
        before_seq: Option<u64>,
        match_: Option<&Filter>,
        bound_head: Option<u64>,
        now_ms: i64,
    ) -> DeleteStats {
        // Sync floors first so we operate on the current logical state.
        let retention = self.enforce_retention(now_ms);

        let head = self.head_seq();
        let earliest = self.earliest_seq();
        // Point-in-time bound: a bare `match` is bounded by the head at call time
        // (`head + 1`); combined with `before_seq` we take the tighter bound. On
        // the live API path the engine passes `bound_head = Some(head + 1)` captured
        // under the append lock BEFORE this call, so the bound is pinned to the
        // delete's point-in-time even though a concurrent append may have advanced
        // the head by the time we take the index lock. On replay the engine passes
        // the WAL-logged `bound_head`, so a record appended AFTER the original
        // delete (seq >= bound_head) is never swept. `None` is used only for an
        // explicit seq-set delete, where the selector bound is irrelevant.
        let pit = bound_head.unwrap_or_else(|| head.saturating_add(1));
        let bound = match before_seq {
            Some(b) => b.min(pit),
            None => pit,
        };

        let mut freed_bytes: u64 = 0;
        let mut deleted: u64 = 0;
        let mut index = self.index.write();

        // --- Collect the seqs to delete. -----------------------------------
        let mut victims: Vec<u64> = Vec::new();
        match match_ {
            // `match` (optionally ANDed with before_seq): use the tag index so
            // we never scan the whole log (DESIGN §7.2).
            Some(filter) => {
                victims = index.matching_live_seqs(filter, bound);
            }
            // `before_seq` only: every live record below the bound.
            None => {
                let base = index.base_seq;
                let lo = earliest.max(base);
                let mut seq = lo;
                while seq < bound {
                    if index.get(seq).map(|r| !r.deleted).unwrap_or(false) {
                        victims.push(seq);
                    }
                    seq += 1;
                }
            }
        }

        let mut deleted_seqs: Vec<u64> = Vec::new();
        for seq in victims {
            if let Some(f) = index.mark_deleted(seq) {
                freed_bytes = freed_bytes.saturating_add(f);
                deleted += 1;
                deleted_seqs.push(seq);
            }
        }

        // A `before_seq`-ONLY delete removed a contiguous prefix: advance the
        // delete watermark so `earliest_seq` jumps past it (silent). When a
        // `match` is also supplied, `before_seq` is merely the AND bound on the
        // tag match — it does NOT delete the whole prefix, so the watermark
        // must not advance (those non-matching priors stay live).
        if match_.is_none() {
            if let Some(b) = before_seq {
                let effective = b.min(pit);
                if effective > index.delete_below {
                    index.delete_below = effective; // every seq < effective dead.
                }
            }
        }
        let delete_below = index.delete_below;
        drop(index);

        if freed_bytes > 0 {
            let prev = self.bytes_retained.load(Ordering::Relaxed);
            self.bytes_retained
                .store(prev.saturating_sub(freed_bytes), Ordering::Relaxed);
        }
        if deleted > 0 {
            let prev = self.live_count.load(Ordering::Relaxed);
            self.live_count
                .store(prev.saturating_sub(deleted), Ordering::Relaxed);
        }
        // Advance the voluntary delete_floor (NOT evict_floor) for the prefix.
        if delete_below > 0 {
            let mut floors = self.floors.write();
            let new_floor = delete_below.saturating_sub(1);
            if new_floor > floors.delete_floor {
                floors.delete_floor = new_floor;
            }
        }

        // Lazy physical reclaim of the now-dead front prefix.
        let front_freed = self.reclaim_front(head);
        // Segment-granular reclaim: drop whole sealed segments cleared by this
        // delete — a prefix delete below the floor, or an interior segment all of
        // whose records were point-deleted (a `match` delete). Silent (voluntary).
        // This is the WHOLE-SEGMENT optimization: a segment all of whose records
        // are now dead is unlinked in ONE op instead of N per-record flips.
        self.reclaim_segments();
        // For a PARTIALLY-cleared segment (some records survive), flip each deleted
        // sealed record's on-disk delete-flag byte so the deletion is durable in the
        // segment file and survives a WAL trim/checkpoint. A seq whose whole segment
        // was just dropped above, or that is still in the active tail, is a no-op.
        self.flag_sealed_deletes(&deleted_seqs);
        DeleteStats {
            deleted,
            bytes_freed: retention
                .bytes_reclaimed
                .saturating_add(freed_bytes)
                .saturating_add(front_freed),
        }
    }

    /// Permanently delete an explicit set of seqs (the queue ack / dead-letter
    /// path, DESIGN §10.4): mark each live slot deleted, free its payload/tag,
    /// adjust `bytes`/`count`, prune the tag index, then lazily reclaim the
    /// now-dead front. Silent (never advances `evict_floor`). Returns the count
    /// actually removed (a seq that is absent / already dead is skipped).
    pub fn delete_seqs(&self, seqs: &[u64], now_ms: i64) -> u64 {
        self.delete_seqs_stats(seqs, now_ms).deleted
    }

    /// As [`Self::delete_seqs`], with byte-free accounting for engine quota gauges.
    pub fn delete_seqs_stats(&self, seqs: &[u64], now_ms: i64) -> DeleteStats {
        let head = self.head_seq();
        let mut freed_bytes: u64 = 0;
        let mut deleted: u64 = 0;
        let mut deleted_seqs: Vec<u64> = Vec::new();
        {
            let mut index = self.index.write();
            for &seq in seqs {
                if let Some(f) = index.mark_deleted_pub(seq) {
                    freed_bytes = freed_bytes.saturating_add(f);
                    deleted += 1;
                    deleted_seqs.push(seq);
                }
            }
        }
        if freed_bytes > 0 {
            let prev = self.bytes_retained.load(Ordering::Relaxed);
            self.bytes_retained
                .store(prev.saturating_sub(freed_bytes), Ordering::Relaxed);
        }
        if deleted > 0 {
            let prev = self.live_count.load(Ordering::Relaxed);
            self.live_count
                .store(prev.saturating_sub(deleted), Ordering::Relaxed);
        }
        let front_freed = self.reclaim_front(head);
        // Segment-granular reclaim for the queue ack / dead-letter delete path:
        // an acked job whose whole sealed segment is now dead drops that file
        // (the WHOLE-SEGMENT optimization, one unlink instead of N flips).
        self.reclaim_segments();
        // Durably flip the on-disk delete-flag for each acked seq still in a
        // partially-cleared sealed segment, so the ack survives a WAL trim.
        self.flag_sealed_deletes(&deleted_seqs);
        let _ = now_ms;
        DeleteStats {
            deleted,
            bytes_freed: freed_bytes.saturating_add(front_freed),
        }
    }
}

/// RAII guard for a publish ticket (R14). Created by
/// [`TopicState::next_publish_ticket`] under the append lock; the writer calls
/// [`Self::wait_turn`] then publishes/rolls back and finally [`Self::done`].
///
/// The guard exists to make the ticket release **panic-safe**: the publish gate
/// is a strict-order baton (`publish_next` must reach exactly `ticket` before the
/// gate advances), so if a ticketed writer panics — or returns early on an error
/// path — without releasing its ticket, every later writer parks forever on
/// [`TopicState::publish_wait_turn`] and [`TopicState::quiesce_publishes`] (snapshot
/// capture) hangs. The `Drop` impl closes that hole: on unwind it waits for this
/// ticket's turn (so the baton is advanced strictly in order, preserving the
/// prefix-durability invariant) and then advances the gate, releasing every
/// blocked successor. A normally-completing writer calls [`Self::done`] (which
/// disarms the `Drop` release), so the guard adds nothing to the happy path.
/// What a [`PublishGuard`] must do on an UNEXPECTED unwind (a panic between
/// taking the ticket and calling [`PublishGuard::done`]) so a not-acked batch can
/// never become visible and a committed-but-unapplied op can never be silently
/// dropped (R14 / codex P0 #2). Releasing the gate alone is NOT enough: a later
/// writer that advances `head_seq` past leaked staged seqs would expose them
/// without their WAL frame ever being durable.
enum UnwindAction {
    /// No staged mutation is attached yet, or the operation already completed and
    /// the guard was disarmed. `Drop` only releases the gate.
    None,
    /// A staged APPEND batch is in flight. On unwind, mark its seqs deleted in
    /// place (exactly like `rollback_staged_by_seqs`) BEFORE releasing the gate, so
    /// a later writer advancing head past them reads a deleted gap rather than a
    /// not-durable record. Carries `(start, count)` of the staged batch.
    RollbackAppend(u64, u64),
    /// A durable op (a WAL-first delete) committed its frame but had not yet
    /// applied it in memory. There is no safe way to reconstruct the correct
    /// visibility on the unwind path (the in-memory state would diverge from the
    /// durable log, and a snapshot taken after this point could checkpoint past
    /// the unapplied frame and lose it). Abort the process so recovery rebuilds a
    /// consistent state from the durable WAL (codex P0 #2).
    AbortProcess,
}

#[must_use = "a publish ticket must be waited on and released in order"]
pub struct PublishGuard<'a> {
    topic_state: &'a TopicState,
    ticket: u64,
    /// Whether [`Self::wait_turn`] already advanced this ticket to the front of
    /// the gate (so `Drop` need not wait again before releasing).
    waited: bool,
    /// Whether [`Self::done`] already released the ticket (disarms `Drop`).
    done: bool,
    /// What `Drop` must do on an unexpected unwind to keep visibility consistent
    /// (R14 / codex P0 #2).
    on_unwind: UnwindAction,
}

impl PublishGuard<'_> {
    /// The underlying ticket value (observability / debug only).
    pub fn ticket(&self) -> u64 {
        self.ticket
    }

    /// Attach a staged APPEND batch to this guard so an unexpected unwind rolls it
    /// back (marks its seqs deleted) before releasing the gate (R14 / codex P0 #2).
    /// Call immediately after `next_publish_ticket`, with the batch this writer
    /// staged. Disarmed by `done`.
    pub fn arm_append(&mut self, staged: &StagedAppend) {
        self.on_unwind = UnwindAction::RollbackAppend(staged.start, staged.count);
    }

    /// Mark this guard as guarding a durable op whose WAL frame is already committed
    /// but whose in-memory apply has not yet run (a WAL-first delete). An unexpected
    /// unwind here aborts the process so recovery rebuilds a consistent state from
    /// the durable log (R14 / codex P0 #2). Disarmed by `done`.
    pub fn arm_abort_on_unwind(&mut self) {
        self.on_unwind = UnwindAction::AbortProcess;
    }

    /// Block until it is this ticket's turn to publish/rollback. See
    /// [`TopicState::publish_wait_turn`].
    pub fn wait_turn(&mut self) {
        self.topic_state.publish_wait_turn(self.ticket);
        self.waited = true;
    }

    /// Release this ticket, advancing the gate to the next writer. MUST be
    /// called after [`Self::wait_turn`] returned and the writer finished
    /// publishing/rolling back. Disarms the `Drop` fallback (both the gate-leak
    /// release and any armed unwind action).
    pub fn done(mut self) {
        self.topic_state.publish_done(self.ticket);
        self.done = true;
    }
}

impl Drop for PublishGuard<'_> {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        // The writer never released this ticket (a panic, or an early return that
        // skipped `done`). Advance the gate so successors are not stranded — but
        // STRICTLY IN ORDER: if we have not yet reached the front of the baton,
        // wait for our turn first, so `publish_next` only ever advances through
        // `ticket` (the prefix-order invariant snapshot capture relies on holds
        // even on the unwind path). Reaching the front also means every earlier
        // ticketed writer already published/rolled back, so the in-place rollback
        // below targets exactly our (still-unpublished) seqs.
        if !self.waited {
            self.topic_state.publish_wait_turn(self.ticket);
        }
        // Complete the in-flight operation's required cleanup BEFORE releasing the
        // gate (R14 / codex P0 #2), so a later writer / a quiescing snapshot never
        // observes a not-acked batch as visible or skips a committed delete.
        match self.on_unwind {
            UnwindAction::None => {}
            UnwindAction::RollbackAppend(start, count) => {
                // Mark this batch's staged-but-unpublished seqs deleted in place.
                // `head_seq` was never advanced to include them by this (failed)
                // writer, so they were never visible; a later writer that advances
                // head past them now reads a deleted gap instead of a record whose
                // WAL frame may never have been durable.
                let mut index = self.topic_state.index.write();
                for i in 0..count {
                    index.mark_deleted_pub(start + i);
                }
            }
            UnwindAction::AbortProcess => {
                // A durable delete's frame is committed but its in-memory apply did
                // not run, and we cannot safely reconcile visibility on the unwind
                // path. Abort so recovery rebuilds consistent state from the WAL.
                tracing::error!(
                    topic_id = self.topic_state.topic_id,
                    ticket = self.ticket,
                    "publish guard unwound with a committed-but-unapplied durable op; aborting for crash-consistent recovery"
                );
                std::process::abort();
            }
        }
        self.topic_state.publish_done(self.ticket);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn topic_for_test() -> Arc<TopicState> {
        Arc::new(TopicState::new(
            "t".to_string(),
            1,
            TopicConfig::default(),
            1,
            1,
        ))
    }

    /// R14: a ticketed writer that PANICS between taking its publish ticket and
    /// releasing it must not strand the gate — the RAII `PublishGuard`'s `Drop`
    /// releases the ticket on unwind, so a later writer's `wait_turn` and a
    /// concurrent snapshot's `quiesce_publishes` both make progress instead of
    /// blocking forever on a ticket that is never advanced.
    #[test]
    fn publish_guard_releases_gate_on_panic() {
        let b = topic_for_test();

        // Writer A takes ticket 0 (under the would-be append lock), waits its turn
        // (it is first, so immediate), then PANICS without calling `done`. The
        // guard's Drop must advance the gate to ticket 1.
        let ba = b.clone();
        let a = std::thread::spawn(move || {
            let mut t = ba.next_publish_ticket();
            t.wait_turn();
            panic!("simulated writer panic with the publish ticket held");
        });
        // A panics; its guard's Drop runs during unwind and releases the gate.
        assert!(a.join().is_err(), "writer A panicked as designed");

        // Writer B (ticket 1) must now be able to take its turn and finish — it
        // would block forever if A's ticket had leaked. Run it on a thread bounded
        // by a watchdog so a regression FAILS (hangs are caught) rather than wedges
        // the suite.
        let bb = b.clone();
        let done = Arc::new(AtomicBool::new(false));
        let d2 = done.clone();
        let bw = std::thread::spawn(move || {
            let mut t = bb.next_publish_ticket(); // ticket 1
            t.wait_turn(); // reachable iff A's ticket 0 was released on its panic.
            t.done();
            d2.store(true, Ordering::SeqCst);
        });
        let start = std::time::Instant::now();
        while !done.load(Ordering::SeqCst) && start.elapsed() < std::time::Duration::from_secs(5) {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert!(
            done.load(Ordering::SeqCst),
            "later writer (ticket 1) hung — the panicked ticket 0 leaked the gate"
        );
        bw.join().unwrap();

        // And snapshot capture's `quiesce_publishes` (no ticket outstanding now)
        // returns promptly rather than hanging on the once-leaked ticket.
        let bq = b.clone();
        let qdone = Arc::new(AtomicBool::new(false));
        let q2 = qdone.clone();
        let q = std::thread::spawn(move || {
            bq.quiesce_publishes();
            q2.store(true, Ordering::SeqCst);
        });
        let start = std::time::Instant::now();
        while !qdone.load(Ordering::SeqCst) && start.elapsed() < std::time::Duration::from_secs(5) {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert!(
            qdone.load(Ordering::SeqCst),
            "quiesce_publishes hung after the panic"
        );
        q.join().unwrap();
    }

    /// The happy path is unchanged: a guard whose `done()` is called advances the
    /// gate exactly once and a subsequent `quiesce_publishes` sees a quiescent topic.
    #[test]
    fn publish_guard_done_advances_gate_once() {
        let b = topic_for_test();
        {
            let mut t = b.next_publish_ticket();
            t.wait_turn();
            t.done();
        }
        // Gate is quiescent (no outstanding ticket): quiesce returns immediately.
        b.quiesce_publishes();
        // The next ticket is the following integer and completes in order.
        let mut t = b.next_publish_ticket();
        t.wait_turn();
        t.done();
        b.quiesce_publishes();
    }

    /// R14 / codex P0 #2: a writer that stages an append, ARMS its guard, and then
    /// PANICS before publishing must not leave the staged (not-acked) records
    /// visible. The guard's `Drop` rolls the staged seqs back IN PLACE (marks them
    /// deleted); when a LATER writer publishes and advances `head_seq` past them,
    /// they read as a deleted gap rather than as live, never-durable records.
    #[test]
    fn publish_guard_rolls_back_staged_append_on_panic() {
        let b = topic_for_test();

        // Writer A stages one record, takes ticket 0, ARMS the guard with the
        // staged batch, then panics before publishing. `head_seq` stays 0.
        let ba = b.clone();
        let staged_start = std::thread::spawn(move || {
            let _g = ba.append_lock.lock();
            let staged = ba.stage_append(vec![StoredRecord {
                ts: 1,
                node: None,
                tag: None,
                data: serde_json::json!({"unacked": true}),
                meta: None,
                bytes: 16,
                deleted: false,
                payload_resident: true,
                hops: 0,
            }]);
            let start = staged.start;
            let mut t = ba.next_publish_ticket();
            t.arm_append(&staged);
            t.wait_turn();
            // Panic with the staged batch unpublished and the guard armed.
            std::panic::panic_any(start);
        })
        .join();
        let staged_seq = *staged_start
            .err()
            .and_then(|e| e.downcast::<u64>().ok())
            .expect("A panicked carrying its staged start seq");

        // The record was never published.
        assert_eq!(b.head_seq(), 0, "panicked writer never advanced head");

        // Writer B stages + publishes its own record. Its head advances PAST the
        // leaked seq. The leaked seq must read as deleted (rolled back on unwind),
        // not as a live record.
        {
            let _g = b.append_lock.lock();
            let staged = b.stage_append(vec![StoredRecord {
                ts: 2,
                node: None,
                tag: None,
                data: serde_json::json!({"acked": true}),
                meta: None,
                bytes: 16,
                deleted: false,
                payload_resident: true,
                hops: 0,
            }]);
            let mut t = b.next_publish_ticket();
            t.wait_turn();
            b.publish_staged(staged, 2, PublishPermit::resident());
            t.done();
        }
        assert!(
            b.head_seq() > staged_seq,
            "B's head advanced past the leaked seq"
        );

        // The leaked seq is a deleted hole: reading it yields no live record.
        let index = b.index.read();
        let rec = index.get(staged_seq);
        assert!(
            rec.map(|r| r.deleted).unwrap_or(true),
            "leaked staged seq must be a deleted hole, never a visible not-acked record"
        );
    }
}
