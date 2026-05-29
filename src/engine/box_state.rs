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
use crate::types::{BoxConfig, Filter};
use parking_lot::{Mutex, RwLock};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
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

    /// Remove `seq` from `tag`'s posting list (on delete/reclaim).
    fn unindex_tag(&mut self, tag: &str, seq: u64) {
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
        }
    }

    /// Whether this box is a queue (carries a lease projection).
    pub fn is_queue(&self) -> bool {
        self.queue.is_some()
    }

    pub fn head_seq(&self) -> u64 {
        self.head_seq.load(Ordering::Acquire)
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

    /// Append `records` with seqs assigned in order. Returns the assigned seqs.
    /// Caller has already validated and (for `discard:"reject"`) admitted.
    ///
    /// Assigns contiguous seqs starting at `head_seq + 1`, pushes records into
    /// the index, bumps `head_seq`, accounts retained bytes, sets the write
    /// recency clock, and wakes any SSE/diff long-pollers via [`Notify`].
    pub fn append(&self, records: Vec<StoredRecord>, now_ms: i64) -> Vec<u64> {
        if records.is_empty() {
            return Vec::new();
        }
        let n = records.len() as u64;
        let mut index = self.index.write();

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
        // Publish the new head_seq after the records are in the index so a
        // concurrent reader that observes the higher head also finds the slots.
        let new_head = start + n - 1;
        self.head_seq.store(new_head, Ordering::Release);
        drop(index);

        self.bytes_retained.fetch_add(added_bytes, Ordering::Relaxed);
        self.live_count.fetch_add(n, Ordering::Relaxed);
        self.last_write_ms.store(now_ms, Ordering::Relaxed);
        // Wake long-pollers (diff `wait_ms`) and SSE streams.
        self.notify.notify_waiters();

        (start..=new_head).collect()
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
            }
        }

        // --- Cap (records): keep at most cap_records retained. --------------
        if cap_records > 0 && head > cap_records {
            let want_floor = head - cap_records; // highest seq to evict.
            if want_floor > floors.evict_floor {
                floors.evict_floor = want_floor;
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
                }
            }
        }

        drop(floors);
        drop(index);
        // --- Lazy front reclaim: pop the dead prefix (evicted/expired/deleted)
        // physically, advancing base_seq. Deleted holes carry 0 bytes (already
        // subtracted on delete), so this never double-counts. -----------------
        self.reclaim_front(head);
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
        let _ = now_ms;
        deleted
    }
}
