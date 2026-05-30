//! Per-box in-memory state: the base+offset [`BoxIndex`], the watermark
//! atomics, retained payload bytes, the per-box tag index, recency clocks, and
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
use crate::types::{BoxConfig, Filter};
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
    /// the slot is deleted (its bytes already subtracted from the box total).
    pub bytes: u64,
    /// Set when this record has been deleted (permanent, silent). The slot is
    /// retained only as a hole for O(1) indexing; its payload is freed.
    pub deleted: bool,
    /// Whether `data`/`meta` are still resident in this slot (Phase 6 memory
    /// bounding). `true` for a fresh append and for any box without a segment
    /// writer (the unchanged default). Once a record is durably in a *sealed*
    /// segment, a writer-backed box frees the slot's `data`/`meta` (sets this
    /// `false`) and the read path resolves the payload from the bounded cache or
    /// the segment. The `bytes` accounting is unchanged (the record is still
    /// live; only where its payload lives changed).
    pub payload_resident: bool,
}

/// The seq→record index: a contiguous deque offset by `base_seq`
/// (ARCHITECTURE §1.1). `index i` corresponds to `seq = base_seq + i`.
///
/// Carries the per-box **tag index** (`tag → ascending live seqs`) so a tag
/// delete is a point lookup (exact) or a bounded range scan (prefix) rather
/// than a full log scan (DESIGN §7.2), and a `delete_below` marker (the max
/// `before_seq` ever applied) so snapshot/prefix deletes are O(1) to apply: a
/// read starts at `max(from_seq + 1, base_seq)` and then skips any remaining
/// deleted slots.
#[derive(Debug, Default)]
pub struct BoxIndex {
    /// Seq of `records[0]`; the earliest physically present seq.
    pub base_seq: u64,
    pub records: VecDeque<StoredRecord>,
    /// `tag → ascending live seqs` for efficient match-deletes (DESIGN §7.2).
    pub tag_index: BTreeMap<String, Vec<u64>>,
    /// Max `before_seq` applied by a delete; every seq `< delete_below` is dead.
    pub delete_below: u64,
}

impl BoxIndex {
    pub fn new(base_seq: u64) -> Self {
        BoxIndex {
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
/// ARCHITECTURE §2.2). Returned by [`BoxState::stage_append`]; consumed by
/// [`BoxState::publish_staged`] (on a successful WAL commit) or
/// [`BoxState::rollback_staged`] (on a WAL/fsync failure). The records are
/// already in the index deque (the contiguous tail past `pre_len`) but invisible
/// (`head_seq` unchanged) until published.
#[derive(Debug)]
pub struct StagedAppend {
    /// First seq assigned to the batch (`head_seq + 1` at stage time).
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
}

/// The full in-memory state of one box.
pub struct BoxState {
    /// The box name (also the identity).
    pub name: String,
    /// Interned numeric id used in WAL frames (ARCHITECTURE §2.1). Stable for the
    /// lifetime of this box instance; reassigned on delete+recreate.
    pub box_id: u32,
    /// Live config (read-mostly; mutated under `index` write lock on `PUT`).
    pub config: RwLock<BoxConfig>,
    /// The seq→record index (carries the per-box tag index + `delete_below`).
    pub index: RwLock<BoxIndex>,
    /// Eviction/expiry/delete floors driving `earliest_seq` (DESIGN §5.1).
    pub floors: RwLock<Floors>,
    /// `(idempotency_key → assigned seqs)` dedupe state (API §0.8). Entries are
    /// reclaimed lazily once older than the box's `idempotency_window_ms`.
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

    /// Highest assigned seq (`0` for a fresh empty box).
    pub head_seq: AtomicU64,
    /// First seq this box instance will ever assign (`seq_base`, default 1).
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

    /// Wakes SSE/diff long-pollers on append (ARCHITECTURE §1.2).
    pub notify: Notify,

    /// Zero-copy SSE broadcast cache (ARCHITECTURE §8.4): each delivered record
    /// frame is serialized once and shared (ref-counted) across all watchers, so
    /// a 1→N broadcast pays serialization once, not N times. Bounded; populated
    /// lazily on the read path only (boxes with no watchers pay nothing).
    pub broadcast: BroadcastCache,

    /// The materialized lease projection for a queue box (DESIGN §10,
    /// ARCHITECTURE §12). `None` on a plain `"log"` box. Lives under its own
    /// mutex (queue lifecycle transitions are rare relative to the read path);
    /// the single batched cohort claim pass holds it for one critical section.
    pub queue: Option<Mutex<QueueProjection>>,

    /// Serializes the **seq-assignment + WAL-enqueue** critical section on the
    /// write path so a box's WAL frames are appended to the single ordered writer
    /// in the *same order* their seqs were assigned. Without this, two concurrent
    /// durable writers can assign seqs `A < B` under the index lock yet enqueue
    /// `B` before `A`; recovery (which applies frames in WAL order and skips any
    /// `seq <= head`) would then drop the lower-seq frame — a silent loss of an
    /// acked durable write. Held only across the (fast, non-blocking) channel
    /// enqueue; the fsync wait happens *after* the lock is released, so durable
    /// group commit still coalesces across boxes (ARCHITECTURE §2.2/§2.3).
    pub append_lock: Mutex<()>,

    /// The per-box HOT segment writer + bounded payload cache (Phase 6 Stage 2).
    /// `None` for a pure in-memory box (every existing unit test) — then payloads
    /// stay resident and the read path is unchanged by construction. When present
    /// (a durable, writer-backed box), committed records are materialized into
    /// HOT segments off the commit path, and once a record is sealed its resident
    /// payload is freed and reads resolve from the bounded cache or the segment.
    /// Lives behind a `Mutex`: touched on the commit path (append/seal) and the
    /// read path (segment-backed resolution) but never holding the index lock
    /// across a slow cold fetch (the Phase-6 HARD INVARIANT).
    pub segwriter: Option<Mutex<SegmentWriter>>,
}

/// Sentinel for a recency clock that has never fired.
pub const TS_NEVER: i64 = i64::MIN;

impl BoxState {
    /// Create a fresh box with the given config, interned id, and epoch.
    pub fn new(name: String, box_id: u32, config: BoxConfig, seq_base: u64, epoch: u64) -> Self {
        // A queue box carries a materialized lease projection (DESIGN §10); a
        // plain log does not. The claim cursor starts at `seq_base` (the first
        // job seq this box instance will assign).
        let queue = if config.is_queue() {
            Some(Mutex::new(QueueProjection::new(seq_base)))
        } else {
            None
        };
        BoxState {
            name,
            box_id,
            config: RwLock::new(config),
            index: RwLock::new(BoxIndex::new(seq_base)),
            floors: RwLock::new(Floors::default()),
            dedupe: RwLock::new(HashMap::new()),
            dedupe_gates: Mutex::new(HashMap::new()),
            head_seq: AtomicU64::new(seq_base.saturating_sub(1)),
            seq_base,
            epoch: AtomicU64::new(epoch),
            bytes_retained: AtomicU64::new(0),
            live_count: AtomicU64::new(0),
            last_write_ms: AtomicI64::new(TS_NEVER),
            last_read_ms: AtomicI64::new(TS_NEVER),
            last_consumed_ms: AtomicI64::new(TS_NEVER),
            notify: Notify::new(),
            broadcast: BroadcastCache::new(),
            queue,
            append_lock: Mutex::new(()),
            segwriter: None,
        }
    }

    /// Attach a HOT [`SegmentWriter`] to this box (durable, writer-backed mode).
    /// Pre-seeds the writer's active segment with any records already in the
    /// index (e.g. after recovery/snapshot load) so the materialization starts
    /// consistent. Called by the engine at box creation/recovery; absent for a
    /// pure in-memory box. Takes `&mut self` because it runs during construction
    /// before the box is shared in an `Arc`.
    pub fn attach_segwriter(&mut self, mut writer: SegmentWriter) {
        // Materialize any pre-existing records (recovery/snapshot) into the
        // writer so its sealed/active state is consistent with the index.
        // Segments mirror the box's gapless append order, so a deleted middle
        // hole is materialized too (as a tombstone frame, never served) to keep
        // the segment seq run contiguous (a `SegmentBuilder` requires it). We do
        // NOT free resident payloads here (recovery correctness first);
        // steady-state eviction happens as new appends seal segments.
        let index = self.index.read();
        let base = index.base_seq;
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
    }

    /// Whether this box is a queue (carries a lease projection).
    pub fn is_queue(&self) -> bool {
        self.queue.is_some()
    }

    pub fn head_seq(&self) -> u64 {
        self.head_seq.load(Ordering::Acquire)
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
    /// box is empty. Driven by cap/TTL **and** deletes.
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
        self.publish_staged(staged, now_ms);
        seqs
    }

    /// **Stage** `records` into the index without publishing them: contiguous
    /// seqs are assigned starting at `head_seq + 1` and the records are pushed
    /// into the index deque + tag index, but `head_seq` is **not** advanced and
    /// no waiter is notified — so a concurrent reader (which gates on `head_seq`)
    /// observes NOTHING yet (the WAL-first reservation rule, ARCHITECTURE §2.2).
    ///
    /// The caller MUST hold the box's `append_lock` across stage → WAL append →
    /// fsync → publish/rollback, so the staged records are always the contiguous
    /// tail of the deque (making [`Self::rollback_staged`] a tail truncation) and
    /// WAL frames are enqueued in seq order. On a durable write: stage, enqueue +
    /// fsync the WAL frame, then [`Self::publish_staged`] on success or
    /// [`Self::rollback_staged`] on a WAL/fsync failure (publish nothing visible,
    /// not acknowledged ⇒ not committed).
    pub fn stage_append(&self, records: Vec<StoredRecord>) -> StagedAppend {
        if records.is_empty() {
            return StagedAppend::empty();
        }
        let n = records.len() as u64;
        let mut index = self.index.write();
        let pre_len = index.records.len();
        let start = self.head_seq().saturating_add(1);
        let mut added_bytes: u64 = 0;
        let mut seq = start;
        for rec in records {
            added_bytes = added_bytes.saturating_add(rec.bytes);
            if let Some(tag) = &rec.tag {
                index.index_tag(seq, tag);
            }
            index.records.push_back(rec);
            seq += 1;
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
    /// segment writer, and wake SSE/diff long-pollers. Called only AFTER the
    /// batch's WAL frame is durably committed (for a durable box), so an
    /// acknowledged write is always already durable.
    pub fn publish_staged(&self, staged: StagedAppend, now_ms: i64) {
        if staged.is_empty() {
            return;
        }
        let new_head = staged.start + staged.count - 1;
        // Publish the new head_seq after the records are in the index so a
        // concurrent reader that observes the higher head also finds the slots.
        self.head_seq.store(new_head, Ordering::Release);

        self.bytes_retained
            .fetch_add(staged.added_bytes, Ordering::Relaxed);
        self.live_count.fetch_add(staged.count, Ordering::Relaxed);
        self.last_write_ms.store(now_ms, Ordering::Relaxed);

        // Materialize the freshly-committed records into the HOT segment writer
        // (Phase 6 Stage 2). This is a derivable materialization of records that
        // are already in the in-memory index (and, for a durable box, the WAL);
        // it runs after head is published so it never delays a reader observing
        // the new records, and frees the resident payloads of any seqs the seal
        // boundary just crossed (memory bounding). A box with no writer skips all
        // of this — the unchanged default path.
        self.materialize_segment(staged.start, new_head);

        // Wake long-pollers (diff `wait_ms`) and SSE streams.
        self.notify.notify_waiters();
    }

    /// **Roll back** a staged batch whose WAL append/fsync FAILED: pop the staged
    /// records (the contiguous tail of the deque — the caller holds `append_lock`,
    /// so nothing was appended after them) and prune their tag postings. `head_seq`
    /// was never advanced, so the records were never visible; this leaves the box
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

    /// Feed the records `[start, end]` to the HOT segment writer and free the
    /// resident payloads of any seqs that sealing pushed into an immutable
    /// segment. No-op for a box without a writer. The index lock is taken only
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
        // the segment mirrors the box's append order). A just-appended record is
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
    ///   tail/active case and every writer-less box — clone it directly.
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
    pub fn enforce_retention(&self, now_ms: i64) {
        let config = self.config.read();
        let ttl_ms = config.ttl_ms;
        let cap_records = config.cap_records;
        let cap_bytes = config.cap_bytes;
        drop(config);

        let head = self.head_seq();
        if head == 0 {
            return; // empty box, nothing retained.
        }

        let index = self.index.read();
        let mut floors = self.floors.write();
        // Track whether an involuntary floor advanced this pass: only then can a
        // whole sealed segment have newly fallen below the live floor, so segment
        // reclaim is skipped on the (common) read where nothing was evicted — the
        // hot read path pays nothing for boxes with no cap/TTL pressure.
        let mut floor_advanced = false;

        // --- TTL: advance expiry_floor past every expired record. -----------
        // `$ts` is non-decreasing in seq, so all seqs <= X expired is a prefix
        // predicate; scan the index front (bounded by the number of newly
        // expired records, amortized O(1) under steady state).
        if ttl_ms > 0 {
            let ttl = ttl_ms as i64;
            let base = index.base_seq;
            let mut expired_upto = floors.expiry_floor;
            for (i, rec) in index.records.iter().enumerate() {
                if now_ms.saturating_sub(rec.ts) > ttl {
                    expired_upto = base + i as u64;
                } else {
                    // First non-expired record; the rest are younger still.
                    break;
                }
            }
            if expired_upto > floors.expiry_floor {
                floors.expiry_floor = expired_upto;
                floor_advanced = true;
            }
        }

        // --- Cap (records): keep at most cap_records retained. --------------
        if cap_records > 0 && head > cap_records {
            let want_floor = head - cap_records; // highest seq to evict.
            if want_floor > floors.evict_floor {
                floors.evict_floor = want_floor;
                floor_advanced = true;
            }
        }

        // --- Cap (bytes): evict oldest physically-present records until the
        // retained byte total is within cap_bytes. Walk the front, summing the
        // bytes that must drop. -------------------------------------------
        if cap_bytes > 0 {
            let retained_bytes = self.bytes_retained.load(Ordering::Relaxed);
            if retained_bytes > cap_bytes {
                let mut over = retained_bytes - cap_bytes;
                let base = index.base_seq;
                // Only consider records that aren't already below the floor.
                let current_floor = floors.evict_floor.max(floors.expiry_floor);
                let mut evict_to = floors.evict_floor;
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
                if evict_to > floors.evict_floor {
                    floors.evict_floor = evict_to;
                    floor_advanced = true;
                }
            }
        }

        drop(floors);
        drop(index);
        // --- Lazy front reclaim: pop the dead prefix (evicted/expired/deleted)
        // physically, advancing base_seq. Deleted holes carry 0 bytes (already
        // subtracted on delete), so this never double-counts. -----------------
        self.reclaim_front(head);
        // Segment-granular physical reclaim: drop whole sealed segment files now
        // fully below the live floor (cap/TTL). Only when a floor advanced this
        // pass, so a quiet read (no eviction) never touches the segment store.
        if floor_advanced {
            self.reclaim_segments();
        }
    }

    /// Physically pop the fully-dead front prefix (below `earliest_seq` or a run
    /// of already-deleted slots at the front), advancing `base_seq` and pruning
    /// their tag postings. Lazy reclaim shared by cap/TTL eviction and deletes
    /// (DESIGN §7, ARCHITECTURE §1.1). `head` is the box head at call time.
    fn reclaim_front(&self, head: u64) {
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
            return;
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
    /// slow, cold) read, and never gating a concurrent write/delivery. A box with
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
                let dead = seg.end_seq < earliest
                    || index.range_all_dead(seg.start_seq, seg.end_seq);
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

    /// On-restart segment reclaim (ARCHITECTURE §4 step 5): after recovery rebuilt
    /// this box's index + floors + segment registry, drop (1) any registered sealed
    /// segment now fully below the live set, and (2) any **orphan** segment object
    /// left on disk (a pre-crash reclaim whose unlink never completed). Idempotent,
    /// off the hot path; a no-op for a box without a writer. Returns the orphan
    /// count dropped (registry drops go through the normal `reclaim_segments`).
    pub fn reclaim_segments_on_recovery(&self) -> usize {
        // First the normal registry reclaim (fully-dead registered segments).
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
    pub fn apply_delete(&self, before_seq: Option<u64>, match_: Option<&Filter>, now_ms: i64) -> u64 {
        // Sync floors first so we operate on the current logical state.
        self.enforce_retention(now_ms);

        let head = self.head_seq();
        let earliest = self.earliest_seq();
        // Point-in-time bound: a bare `match` is bounded by the head at call
        // time (head + 1); combined with `before_seq` we take the tighter bound.
        let bound = match before_seq {
            Some(b) => b.min(head.saturating_add(1)),
            None => head.saturating_add(1),
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

        for seq in victims {
            if let Some(f) = index.mark_deleted(seq) {
                freed_bytes = freed_bytes.saturating_add(f);
                deleted += 1;
            }
        }

        // A `before_seq`-ONLY delete removed a contiguous prefix: advance the
        // delete watermark so `earliest_seq` jumps past it (silent). When a
        // `match` is also supplied, `before_seq` is merely the AND bound on the
        // tag match — it does NOT delete the whole prefix, so the watermark
        // must not advance (those non-matching priors stay live).
        if match_.is_none() {
            if let Some(b) = before_seq {
                let effective = b.min(head.saturating_add(1));
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
        self.reclaim_front(head);
        // Segment-granular reclaim: drop whole sealed segments cleared by this
        // delete — a prefix delete below the floor, or an interior segment all of
        // whose records were point-deleted (a `match` delete). Silent (voluntary).
        self.reclaim_segments();
        deleted
    }

    /// Permanently delete an explicit set of seqs (the queue ack / dead-letter
    /// path, DESIGN §10.4): mark each live slot deleted, free its payload/tag,
    /// adjust `bytes`/`count`, prune the tag index, then lazily reclaim the
    /// now-dead front. Silent (never advances `evict_floor`). Returns the count
    /// actually removed (a seq that is absent / already dead is skipped).
    pub fn delete_seqs(&self, seqs: &[u64], now_ms: i64) -> u64 {
        let head = self.head_seq();
        let mut freed_bytes: u64 = 0;
        let mut deleted: u64 = 0;
        {
            let mut index = self.index.write();
            for &seq in seqs {
                if let Some(f) = index.mark_deleted_pub(seq) {
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
        // Segment-granular reclaim for the queue ack / dead-letter delete path:
        // an acked job whose whole sealed segment is now dead drops that file.
        self.reclaim_segments();
        let _ = now_ms;
        deleted
    }
}
