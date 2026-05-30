//! Per-box **segment writer** + **bounded payload cache** (Phase 6 Stage 2).
//!
//! As records commit to a box, their bytes are appended to the box's *active*
//! HOT segment (a [`SegmentBuilder`]). When a seal trigger fires
//! (`segment_max_events` / `segment_max_bytes` / `segment_max_age_ms`, read
//! through the [`Clock`] so the age trigger is `TestClock`-drivable), the active
//! segment is durably persisted to the HOT [`SegmentStore`] via `put` (fsync'd),
//! recorded as **sealed/immutable**, and a fresh active segment is started at the
//! next seq.
//!
//! # Memory bounding (the Phase-6 point)
//!
//! Once a record is durably in a *sealed* segment its payload need not stay
//! resident on the heap. The writer keeps a **bounded** ring of the most-recent
//! sealed records' payloads (the hot tail) so a consumer 1–5 ms behind head is
//! served from memory; older payloads fall out of the ring and are read back
//! from the (hot) segment on demand via [`SegmentStore::read_range`]. The
//! in-memory index keeps only the locator (seq → segment/offset/len), not the
//! payload, so index memory is bounded by the live record count, not the payload
//! volume.
//!
//! # The WAL stays the durability boundary
//!
//! Segments are a **derivable materialization** of the WAL's `Append` frames
//! (ARCHITECTURE §0.3, §3). A write is acked once it is WAL-committed; sealing a
//! segment is a downstream, off-the-hot-path materialization. Crash recovery
//! still replays the WAL (Stage 4 finishes the segment side of recovery); here
//! the writer never weakens the existing durable-ack/recovery guarantees because
//! a record's payload is only freed once it is fsync'd into a sealed segment.
//!
//! # Default-safe / transparent
//!
//! A box only gets a [`SegmentWriter`] when the engine is durable (a data dir is
//! configured). Pure in-memory engines ([`crate::engine::Engine::new`], used by
//! almost every unit test) attach none, so the read path resolves payloads from
//! the resident in-memory slot exactly as before — behavior is unchanged by
//! construction.

use crate::clock::SharedClock;
use crate::storage::{
    decode_data_frame, lookup, BoxTier, SegmentBuilder, SegmentPart, SegmentRecord, Tier,
};
use std::collections::VecDeque;
use std::sync::Arc;

use crate::config::SegmentConfig;

/// Metadata for one sealed (immutable) segment held by the writer: its seq range,
/// `.data` byte length, and which tier currently holds it. The segment id is
/// `start_seq` (ARCHITECTURE §6).
#[derive(Debug, Clone, Copy)]
pub struct SealedSegment {
    /// First seq covered (the segment id / file name).
    pub start_seq: u64,
    /// Last seq covered (inclusive).
    pub end_seq: u64,
    /// `.data` byte length (for the hot-retention byte bound).
    pub data_len: u64,
    /// Which tier currently holds this segment's authoritative copy. Starts
    /// [`Tier::Hot`]; the relocator durably flips it to [`Tier::Cold`] only after
    /// the cold copy is fsync'd (the crash-safe ordering).
    pub tier: Tier,
}

/// A payload resolved for a record, regardless of where it currently lives
/// (resident slot, bounded cache, or a segment read). The seq/ts/node/tag are
/// the locator-side fields the in-memory index always retains; `data`/`meta` are
/// the payload that may need to be read back from a segment.
#[derive(Debug, Clone)]
pub struct ResolvedPayload {
    pub data: serde_json::Value,
    pub meta: Option<serde_json::Value>,
}

/// How many most-recent sealed-record payloads to keep resident in the bounded
/// cache (the hot tail). A consumer within this many records of the seal
/// boundary is served from memory; older payloads are read from the segment.
/// Bounded so the cache never grows with the live set.
pub const PAYLOAD_CACHE_CAP: usize = 4096;

/// Bound on the small LRU of payloads read back from the COLD tier. A repeated
/// historical scan over the same cold segment is then served from memory rather
/// than re-fetching the (slow) cold object every time. Bounded so it never grows
/// with the cold-data volume.
pub const COLD_CACHE_CAP: usize = 1024;

/// One cached payload (post-seal): the seq plus its `data`/`meta`. Kept in a
/// bounded ring so tail reads avoid a segment read.
struct CachedPayload {
    seq: u64,
    data: serde_json::Value,
    meta: Option<serde_json::Value>,
}

/// A locator captured under the writer lock so the actual (possibly slow, cold)
/// segment read can run **after** the lock is dropped (the Phase-6 HARD
/// INVARIANT: a cold fetch must never hold a lock that gates writes/delivery).
/// It carries an `Arc<BoxTier>` clone so the I/O touches the stores without the
/// writer mutex held.
pub struct SealedLocator {
    /// Shared tier handle for the off-lock `read_range`.
    tier: Arc<BoxTier>,
    /// The sealed segment holding the seq.
    seg: SealedSegment,
    /// The seq being resolved.
    seq: u64,
}

impl SealedLocator {
    /// Whether this resolves against the COLD tier (a potentially slow read).
    pub fn is_cold(&self) -> bool {
        self.seg.tier == Tier::Cold
    }
}

/// Outcome of [`SegmentWriter::resolve_sealed_fast`]: a cache hit (resolved with
/// no I/O), a miss carrying a [`SealedLocator`] to read off-lock, or a seq that
/// is not in any sealed segment (the caller falls back to the resident slot).
pub enum SealedResolve {
    /// Resolved from an in-memory cache; no segment read needed.
    Hit(ResolvedPayload),
    /// Cache miss: read this locator with the writer lock released.
    Read(SealedLocator),
    /// `seq` is not in a sealed segment (resident-slot / active-tail path).
    NotSealed,
}

/// Read one record's payload for a [`SealedLocator`] — a `read_range` of exactly
/// that record's frame (located via the `.idx`), so it touches one record, not
/// the whole file. **Runs with no writer lock held** (the caller dropped it), so
/// a slow cold fetch never gates writes/delivery. Returns `None` if the segment
/// vanished or the frame failed to decode (defensive; the WAL is the truth).
pub fn read_locator(loc: &SealedLocator) -> Option<ResolvedPayload> {
    let store = loc.tier.store_for(loc.seg.start_seq)?;
    let entry_off = (loc.seq - loc.seg.start_seq) * crate::storage::IDX_STRIDE as u64;
    let idx_bytes = store
        .read_range(
            loc.seg.start_seq,
            SegmentPart::Idx,
            entry_off,
            crate::storage::IDX_STRIDE as u64,
        )
        .ok()?;
    let e = crate::storage::idx_entry_at(&idx_bytes, 0)?;
    let frame = store
        .read_range(
            loc.seg.start_seq,
            SegmentPart::Data,
            e.offset as u64,
            e.len as u64,
        )
        .ok()?;
    let r = decode_data_frame(&frame).ok()?;
    let (data, meta) = decode_payload(&r.data);
    Some(ResolvedPayload { data, meta })
}

/// The per-box segment writer: the active [`SegmentBuilder`], the sealed-segment
/// registry, the bounded payload cache, and the seal policy/clock. Lives behind a
/// `Mutex` on [`crate::engine::box_state::BoxState`]; it is touched on the commit
/// path (append + seal) and the read path (segment-backed payload resolution) —
/// never holding a box write lock across a (potentially slow) cold fetch.
pub struct SegmentWriter {
    /// HOT + optional COLD stores for this box.
    tier: Arc<BoxTier>,
    /// Seal triggers + hot-retention policy.
    cfg: SegmentConfig,
    /// Time source (real or [`crate::clock::TestClock`]); the age seal trigger
    /// reads `now_ms` from here so it is test-drivable.
    clock: SharedClock,

    /// The open (unsealed) active segment. `None` until the first record arrives;
    /// always covers `[active_start, head]`.
    active: Option<SegmentBuilder>,
    /// When the active segment's first record was appended (ms), for the age seal
    /// trigger. `0` ⇒ no active segment.
    active_started_ms: i64,

    /// Sealed segments, oldest first (each is persisted in the HOT store).
    sealed: Vec<SealedSegment>,

    /// Bounded ring of recently-sealed payloads (the hot tail). Front = oldest.
    cache: VecDeque<CachedPayload>,
    /// The bound on `cache` length.
    cache_cap: usize,

    /// A small bounded LRU of payloads read back from the COLD tier, so a
    /// repeated historical scan over the same cold segment is served from memory
    /// instead of re-fetching the slow cold object. Front = oldest (LRU).
    cold_cache: VecDeque<CachedPayload>,
    /// The bound on `cold_cache`.
    cold_cache_cap: usize,

    /// Cumulative count of records resolved by an actual COLD-tier read (a slow
    /// fetch that missed both payload caches). Surfaced as a `cold_segments_read`
    /// performance hint so a degraded historical read is observable.
    cold_reads: u64,

    /// Whether resident payloads should be freed once sealed (memory bounding).
    /// Default `true` for a durable, writer-backed box; the read path then
    /// resolves from cache/segment. Tests can disable to exercise the resident
    /// path alongside the writer.
    evict_resident: bool,
}

impl SegmentWriter {
    /// Build a writer for a box whose first seq is `seq_base`.
    pub fn new(tier: Arc<BoxTier>, cfg: SegmentConfig, clock: SharedClock) -> Self {
        SegmentWriter {
            tier,
            cfg,
            clock,
            active: None,
            active_started_ms: 0,
            sealed: Vec::new(),
            cache: VecDeque::new(),
            cache_cap: PAYLOAD_CACHE_CAP,
            cold_cache: VecDeque::new(),
            cold_cache_cap: COLD_CACHE_CAP,
            cold_reads: 0,
            evict_resident: true,
        }
    }

    /// Override the bounded-cache capacity (tests). Must be >= 1 in practice; `0`
    /// means "never cache a sealed payload" (every sealed read hits the segment).
    pub fn set_cache_cap(&mut self, cap: usize) {
        self.cache_cap = cap;
        while self.cache.len() > self.cache_cap {
            self.cache.pop_front();
        }
    }

    /// Whether resident payloads are freed once sealed (memory bounding).
    pub fn evicts_resident(&self) -> bool {
        self.evict_resident
    }

    /// Toggle resident-payload eviction (tests can keep payloads resident).
    pub fn set_evict_resident(&mut self, on: bool) {
        self.evict_resident = on;
    }

    /// Append one committed record to the active segment, sealing first if a
    /// trigger has fired. Returns the seqs that were **sealed** by this call (so
    /// the caller can free their resident payloads) — empty when nothing sealed.
    ///
    /// The record's `data`/`meta`/`node`/`tag`/`ts` are the just-committed values
    /// (identical to what the in-memory slot holds), so the segment frame is a
    /// faithful materialization.
    pub fn append_record(
        &mut self,
        seq: u64,
        ts: i64,
        node: Option<&str>,
        tag: Option<&str>,
        data: &serde_json::Value,
        meta: &Option<serde_json::Value>,
    ) -> Vec<u64> {
        // Seal-before-append: if the *current* active segment is already at a
        // size/event/age cap, seal it so this record starts a fresh segment. This
        // keeps each sealed segment within the caps (the active one can exceed by
        // at most the in-flight record, then seals on the next append/flush).
        let mut sealed_seqs = Vec::new();
        if self.should_seal_before(seq) {
            sealed_seqs.extend(self.seal_active());
        }

        if self.active.is_none() {
            self.active = Some(SegmentBuilder::new(seq));
            self.active_started_ms = ts;
        }
        let blob = encode_payload(data, meta);
        let rec = SegmentRecord {
            seq,
            ts: ts.max(0) as u64,
            node: node.map(str::to_string),
            tag: tag.map(str::to_string),
            data: blob,
        };
        self.active
            .as_mut()
            .expect("active segment present")
            .push(&rec);
        sealed_seqs
    }

    /// Whether the active segment should seal before accepting the record at
    /// `next_seq` (event/byte/age caps). `false` if there is no active segment.
    fn should_seal_before(&self, _next_seq: u64) -> bool {
        let Some(active) = &self.active else {
            return false;
        };
        let count = active.record_count() as u64;
        if count == 0 {
            return false;
        }
        if count >= self.cfg.max_events {
            return true;
        }
        if self.cfg.max_bytes > 0 && active.data_len() as u64 >= self.cfg.max_bytes {
            return true;
        }
        if self.cfg.max_age_ms > 0 {
            let now = self.clock.now_ms();
            if now.saturating_sub(self.active_started_ms) >= self.cfg.max_age_ms as i64 {
                return true;
            }
        }
        false
    }

    /// Seal a partially-filled active segment if its age trigger has fired (the
    /// idle-box path: no new appends, but the age cap should still seal it).
    /// Returns the seqs sealed (empty if nothing sealed). Called off the commit
    /// path (e.g. a flush tick / read-path retention sync) so an idle box's data
    /// can age out / relocate.
    pub fn maybe_seal_idle(&mut self) -> Vec<u64> {
        let should = match &self.active {
            Some(a) if a.record_count() > 0 && self.cfg.max_age_ms > 0 => {
                let now = self.clock.now_ms();
                now.saturating_sub(self.active_started_ms) >= self.cfg.max_age_ms as i64
            }
            _ => false,
        };
        if should {
            self.seal_active()
        } else {
            Vec::new()
        }
    }

    /// Force-seal the active segment regardless of triggers (e.g. on snapshot /
    /// shutdown so all committed records are materialized). Returns sealed seqs.
    pub fn flush(&mut self) -> Vec<u64> {
        if self.active.as_ref().map(|a| a.record_count() > 0).unwrap_or(false) {
            self.seal_active()
        } else {
            Vec::new()
        }
    }

    /// Seal the current active segment: persist it to the HOT store (fsync'd),
    /// record it as sealed, push its payloads into the bounded cache, and clear
    /// the active builder. Returns the seqs that were sealed.
    fn seal_active(&mut self) -> Vec<u64> {
        let Some(builder) = self.active.take() else {
            return Vec::new();
        };
        self.active_started_ms = 0;
        if builder.record_count() == 0 {
            return Vec::new();
        }
        let start_seq = builder.start_seq();
        let end_seq = builder.end_seq();
        let (data, idx) = builder.finish();
        let data_len = data.len() as u64;

        // Where does this segment already live (if anywhere)? On the recovery
        // re-materialize path (`attach_segwriter` replays every live record) a
        // segment id may already be present in HOT or COLD from before the
        // restart. In that case the `put` is unnecessary (and would needlessly
        // pull a relocated segment back to HOT), so we skip it and record the
        // surviving tier — segments are a derivable materialization, and the
        // bytes are byte-identical by construction (same gapless seq run).
        let existing_tier = self.tier.resolve(start_seq);
        let tier = match existing_tier {
            Some(Tier::Cold) => Tier::Cold,
            Some(Tier::Hot) => Tier::Hot,
            None => {
                // Persist to the HOT store (fsync'd by `put`). The WAL already
                // made these records durable; the segment is the materialization,
                // so a `put` failure is not data loss — surface it as a warning
                // and keep the payloads resident (do NOT free them) so reads stay
                // correct. We report no sealed seqs in that case (the caller then
                // does not free anything).
                if let Err(e) = self.tier.hot().put(start_seq, &data, &idx) {
                    tracing::warn!(
                        segment = start_seq,
                        error = %e,
                        "segment seal: hot store put failed; keeping payloads resident"
                    );
                    // The active segment is simply gone; the next append starts a
                    // fresh one. Correctness is preserved (the WAL is the source of
                    // truth); only the materialization for this range is deferred
                    // to recovery/re-checkpoint.
                    return Vec::new();
                }
                Tier::Hot
            }
        };

        self.sealed.push(SealedSegment {
            start_seq,
            end_seq,
            data_len,
            tier,
        });

        // Decode the payloads back into the bounded cache so the hot tail is
        // served from memory. (We have the original `data`/`meta` values on the
        // commit path but `seal` is also reachable from a flush/idle path with no
        // values in hand, so decode from the freshly-built `.data` — cheap and
        // uniform.) Only the cache-cap most-recent are kept.
        let mut sealed_seqs = Vec::with_capacity((end_seq - start_seq + 1) as usize);
        for seq in start_seq..=end_seq {
            sealed_seqs.push(seq);
            if self.cache_cap == 0 {
                continue;
            }
            if let Some(e) = lookup(&idx, start_seq, seq) {
                let lo = e.offset as usize;
                let hi = lo + e.len as usize;
                if hi <= data.len() {
                    if let Ok(r) = decode_data_frame(&data[lo..hi]) {
                        let (d, m) = decode_payload(&r.data);
                        self.push_cache(seq, d, m);
                    }
                }
            }
        }
        sealed_seqs
    }

    /// Push a payload into the bounded cache (front = oldest, back = newest),
    /// evicting the oldest beyond the cap.
    fn push_cache(&mut self, seq: u64, data: serde_json::Value, meta: Option<serde_json::Value>) {
        self.cache.push_back(CachedPayload { seq, data, meta });
        while self.cache.len() > self.cache_cap {
            self.cache.pop_front();
        }
    }

    /// Resolve a sealed record's payload, **holding the writer lock for the whole
    /// read** (cache + segment). Kept for callers/tests that resolve a small set
    /// inline; for hot-path serving prefer [`Self::resolve_sealed_fast`] +
    /// [`read_locator`] so a (possibly slow, cold) read never holds the lock.
    /// Returns `None` if `seq` is not in any sealed segment.
    pub fn resolve_sealed(&mut self, seq: u64) -> Option<ResolvedPayload> {
        match self.resolve_sealed_fast(seq) {
            SealedResolve::Hit(p) => Some(p),
            SealedResolve::Read(loc) => {
                let p = read_locator(&loc)?;
                self.record_cold_read(&loc, &p);
                Some(p)
            }
            SealedResolve::NotSealed => None,
        }
    }

    /// Try to resolve `seq` from the in-memory caches (hot tail + cold LRU); on a
    /// miss return a [`SealedLocator`] the caller reads **off the writer lock**.
    /// This is the hot-path entry: it never touches a (slow) segment store, so
    /// the writer lock is released before any I/O — the Phase-6 HARD INVARIANT.
    pub fn resolve_sealed_fast(&self, seq: u64) -> SealedResolve {
        // Fast path: the recent-seal cache (newest first — tail consumers hit).
        if let Some(c) = self.cache.iter().rev().find(|c| c.seq == seq) {
            return SealedResolve::Hit(ResolvedPayload {
                data: c.data.clone(),
                meta: c.meta.clone(),
            });
        }
        // Then the cold LRU (a repeated historical scan hits here, no I/O).
        if let Some(c) = self.cold_cache.iter().rev().find(|c| c.seq == seq) {
            return SealedResolve::Hit(ResolvedPayload {
                data: c.data.clone(),
                meta: c.meta.clone(),
            });
        }
        // Miss: hand the caller a locator to read off-lock. Clone the tier `Arc`
        // so the read touches the stores without the writer mutex held.
        match self.segment_for(seq) {
            Some(seg) => SealedResolve::Read(SealedLocator {
                tier: self.tier.clone(),
                seg,
                seq,
            }),
            None => SealedResolve::NotSealed,
        }
    }

    /// After an off-lock read of a [`SealedLocator`], fold the payload into the
    /// cold LRU (so a re-scan is served from memory) and, if it came from the
    /// cold tier, bump the `cold_segments_read` counter. Cheap; under the lock.
    pub fn record_cold_read(&mut self, loc: &SealedLocator, p: &ResolvedPayload) {
        if loc.seg.tier == Tier::Cold {
            self.cold_reads = self.cold_reads.saturating_add(1);
            if self.cold_cache_cap > 0 {
                self.cold_cache.push_back(CachedPayload {
                    seq: loc.seq,
                    data: p.data.clone(),
                    meta: p.meta.clone(),
                });
                while self.cold_cache.len() > self.cold_cache_cap {
                    self.cold_cache.pop_front();
                }
            }
        }
    }

    /// Cumulative count of records served by an actual COLD-tier read (both
    /// payload caches missed). Surfaced as the `cold_segments_read` hint.
    pub fn cold_reads(&self) -> u64 {
        self.cold_reads
    }

    /// Whether `seq` lives in a sealed segment (so its resident payload may be
    /// freed / a read must resolve from cache/segment).
    pub fn is_sealed(&self, seq: u64) -> bool {
        self.segment_for(seq).is_some()
    }

    /// The sealed segment covering `seq`, if any (binary search over the sorted
    /// sealed list by start_seq).
    fn segment_for(&self, seq: u64) -> Option<SealedSegment> {
        // `sealed` is ascending by start_seq and ranges are contiguous/disjoint.
        let idx = self
            .sealed
            .partition_point(|s| s.start_seq <= seq)
            .checked_sub(1)?;
        let seg = self.sealed[idx];
        if seq >= seg.start_seq && seq <= seg.end_seq {
            Some(seg)
        } else {
            None
        }
    }

    /// Number of sealed segments currently held (HOT). For tests / hot-retention.
    pub fn sealed_count(&self) -> usize {
        self.sealed.len()
    }

    /// Snapshot of the sealed segment metadata (oldest first).
    pub fn sealed_segments(&self) -> Vec<SealedSegment> {
        self.sealed.clone()
    }

    /// First seq of the active (unsealed) segment, or `None` if no active segment.
    pub fn active_start(&self) -> Option<u64> {
        self.active.as_ref().map(|a| a.start_seq())
    }

    /// Number of records currently in the active (unsealed) segment.
    pub fn active_record_count(&self) -> usize {
        self.active.as_ref().map(|a| a.record_count()).unwrap_or(0)
    }

    /// Current bounded-cache occupancy (tests).
    pub fn cache_len(&self) -> usize {
        self.cache.len()
    }

    /// Current cold-LRU occupancy (tests).
    pub fn cold_cache_len(&self) -> usize {
        self.cold_cache.len()
    }

    /// Override the cold-LRU capacity (tests).
    pub fn set_cold_cache_cap(&mut self, cap: usize) {
        self.cold_cache_cap = cap;
        while self.cold_cache.len() > self.cold_cache_cap {
            self.cold_cache.pop_front();
        }
    }

    /// Shared tier handle (for the engine's off-lock copy/relocation I/O).
    pub fn tier(&self) -> Arc<BoxTier> {
        self.tier.clone()
    }

    // =======================================================================
    // Relocation (HOT -> COLD), the Stage-3 state machine.
    //
    // Driven by the engine off the hot path. The crash-safe order is:
    //   plan -> copy hot->cold (off-lock, fsync'd) -> durably flip the tier
    //   pointer -> delete the hot copy.
    // Each step is idempotent and `BoxTier::resolve` prefers HOT, so an interrupted
    // relocation (a segment present in both tiers with the pointer not yet flipped)
    // recovers cleanly: the surviving HOT copy is used and the relocation re-runs.
    // =======================================================================

    /// The sealed segments that should be relocated to COLD per the hot-retention
    /// policy (`hot_retain_segments` count and the optional `hot_retain_bytes`),
    /// returned oldest-first as `(id, data_len)`. Only segments still HOT are
    /// considered (an already-cold one is done). Empty when there is no cold tier,
    /// nothing exceeds the bound, or every old segment is already cold.
    ///
    /// The newest `hot_retain_segments` sealed segments (and the active segment,
    /// which is never sealed) stay hot; older ones spill. If `hot_retain_bytes > 0`
    /// the stricter bound wins (spill more when the kept hot bytes would exceed it).
    pub fn relocation_plan(&self) -> Vec<(u64, u64)> {
        if !self.tier.has_cold() {
            return Vec::new();
        }
        let n = self.sealed.len();
        // Index (into `sealed`, oldest-first) below which segments spill to cold.
        // Start from the count bound: keep the newest `hot_retain_segments`.
        let retain = self.cfg.hot_retain_segments as usize;
        let mut keep_from = n.saturating_sub(retain);
        // Tighten with the byte bound: walk newest->oldest, summing kept hot bytes;
        // once they exceed `hot_retain_bytes`, everything older also spills.
        if self.cfg.hot_retain_bytes > 0 {
            let mut kept_bytes: u64 = 0;
            let mut byte_keep_from = n;
            for i in (0..n).rev() {
                let next = kept_bytes.saturating_add(self.sealed[i].data_len);
                if next > self.cfg.hot_retain_bytes {
                    break;
                }
                kept_bytes = next;
                byte_keep_from = i;
            }
            keep_from = keep_from.max(byte_keep_from);
        }
        self.sealed[..keep_from]
            .iter()
            .filter(|s| s.tier == Tier::Hot)
            .map(|s| (s.start_seq, s.data_len))
            .collect()
    }

    /// Durably flip a segment's tier pointer to COLD **after** its cold copy is
    /// fsync'd, then delete the hot copy (idempotent). Called by the engine on the
    /// blocking pool once [`copy_segment_to_cold`] has succeeded. Drops the
    /// segment's payloads from the hot recent-seal cache (they now live cold) so
    /// the cache reflects the relocation. A no-op if the segment is unknown or
    /// already cold.
    pub fn confirm_relocated(&mut self, id: u64) {
        let Some(seg) = self.sealed.iter_mut().find(|s| s.start_seq == id) else {
            return;
        };
        if seg.tier == Tier::Cold {
            return;
        }
        // Flip the in-memory pointer first; the durable record of this flip is the
        // segment's presence in the cold store (re-derived on restart via
        // `BoxTier::resolve` + the relocator), so this is crash-safe: a crash after
        // the cold copy but before the hot delete leaves both copies, HOT preferred,
        // and the relocator simply re-runs the (idempotent) drop.
        let (start, end) = (seg.start_seq, seg.end_seq);
        seg.tier = Tier::Cold;
        // Named crash point: the tier pointer is flipped to COLD (the cold copy is
        // already durable) but the redundant HOT copy has NOT been deleted yet
        // (F-COLD-CRASH-AFTER-FLIP-BEFORE-DELETE). On restart the in-memory flip is
        // lost but the cold copy exists; `BoxTier::resolve` re-derives the tier
        // (HOT preferred while both exist) and the relocator re-runs the idempotent
        // drop — no loss, never zero copies. No-op without `--features failpoints`.
        fail::fail_point!("cold::after_flip_before_delete");
        // Delete the now-redundant HOT copy (idempotent).
        if let Err(e) = self.tier.hot().delete(id) {
            tracing::warn!(segment = id, error = %e, "relocate: hot delete failed (will retry)");
        }
        // Drop the relocated seqs from the recent-seal (hot) cache; a read of them
        // now resolves from the cold tier (and is then cold-LRU cached).
        self.cache.retain(|c| c.seq < start || c.seq > end);
    }

    /// Drop a whole sealed segment from **whichever tier holds it** (cap/TTL/delete
    /// reclaim is segment-granular — ARCHITECTURE §3.3). Removes it from the sealed
    /// registry and both payload caches. Idempotent. Used to reclaim a segment all
    /// of whose seqs are below the live floor.
    pub fn drop_segment(&mut self, id: u64) {
        let Some(pos) = self.sealed.iter().position(|s| s.start_seq == id) else {
            return;
        };
        let seg = self.sealed.remove(pos);
        // Delete from the resolved tier (and, defensively, the other) — idempotent.
        let _ = self.tier.hot().delete(id);
        if let Some(cold) = self.tier.cold() {
            let _ = cold.delete(id);
        }
        self.cache.retain(|c| c.seq < seg.start_seq || c.seq > seg.end_seq);
        self.cold_cache.retain(|c| c.seq < seg.start_seq || c.seq > seg.end_seq);
    }

    /// Sealed segments fully below `live_floor` (every seq `< live_floor`), i.e.
    /// entirely evicted/deleted — droppable for physical reclaim. Oldest-first.
    pub fn segments_below(&self, live_floor: u64) -> Vec<u64> {
        self.sealed
            .iter()
            .filter(|s| s.end_seq < live_floor)
            .map(|s| s.start_seq)
            .collect()
    }

    /// On-restart **orphan reclaim** (ARCHITECTURE §4 step 5): after the writer's
    /// registry was rebuilt from the recovered live record set (so it covers
    /// `[base_seq, head]`), delete any segment object **left on disk** — in either
    /// tier — whose `start_seq` is strictly below `live_floor` (the recovered
    /// `base_seq`) and which is **not** in the rebuilt registry. Such a file is a
    /// segment that was cap/TTL/delete-reclaimed before the crash but whose unlink
    /// never completed (or was fully front-reclaimed), so it is fully dead: drop it
    /// idempotently rather than leak it forever. Bounded (one `list` per tier), runs
    /// once at boot, off the hot path. Returns the number of orphan ids dropped.
    pub fn reclaim_orphans_below(&mut self, live_floor: u64) -> usize {
        let registered: std::collections::HashSet<u64> =
            self.sealed.iter().map(|s| s.start_seq).collect();
        // Enumerate by EITHER part (`list_all_ids`), so a stray `.idx` with no
        // `.data` — the remnant a crash between `delete()`'s two unlinks leaves —
        // is also a reclaim candidate (it is below floor and unregistered ⇒ dead).
        // Plain `list()` keys on `.data` only and would leak the lone `.idx`.
        let mut ids: std::collections::BTreeSet<u64> = std::collections::BTreeSet::new();
        if let Ok(hot_ids) = self.tier.hot().list_all_ids() {
            ids.extend(hot_ids);
        }
        if let Some(cold) = self.tier.cold() {
            if let Ok(cold_ids) = cold.list_all_ids() {
                ids.extend(cold_ids);
            }
        }
        let mut dropped = 0usize;
        for id in ids {
            // A segment object whose start is below the live floor and which the
            // rebuilt registry does not cover is fully dead ⇒ orphan. (Registered
            // segments are never touched — they hold live records.)
            if id < live_floor && !registered.contains(&id) {
                let _ = self.tier.hot().delete(id);
                if let Some(cold) = self.tier.cold() {
                    let _ = cold.delete(id);
                }
                dropped += 1;
            }
        }
        dropped
    }
}

/// Copy a segment's `.data`+`.idx` from a tier's HOT store to its COLD store and
/// fsync (the COLD `put` is fsync'd). **Runs with no writer lock held** (the
/// engine issues it on the blocking pool), so the copy never gates writes or
/// delivery. Idempotent: re-copying overwrites the same id. Returns the segment's
/// `.data` length on success. A `None`/`Err` leaves the HOT copy intact (the
/// relocation simply did not advance — never a loss).
pub fn copy_segment_to_cold(tier: &Arc<BoxTier>, id: u64) -> Result<(), crate::storage::StoreError> {
    let Some(cold) = tier.cold() else {
        return Ok(()); // no cold tier ⇒ nothing to do.
    };
    // If the cold copy already exists (a prior interrupted relocation), the copy
    // step is done; the caller's `confirm_relocated` will flip + drop hot.
    if cold.exists(id, SegmentPart::Data) && cold.exists(id, SegmentPart::Idx) {
        return Ok(());
    }
    let data = tier.hot().read_all(id, SegmentPart::Data)?;
    let idx = tier.hot().read_all(id, SegmentPart::Idx)?;
    cold.put(id, &data, &idx)
}

/// Encode `data` + optional `meta` into the segment payload blob — the same
/// `{"d":data,"m":meta}` envelope the WAL uses, so segment and WAL payloads are
/// interchangeable on recovery.
fn encode_payload(data: &serde_json::Value, meta: &Option<serde_json::Value>) -> Vec<u8> {
    let mut obj = serde_json::Map::with_capacity(2);
    obj.insert("d".to_string(), data.clone());
    if let Some(m) = meta {
        obj.insert("m".to_string(), m.clone());
    }
    serde_json::to_vec(&serde_json::Value::Object(obj)).unwrap_or_default()
}

/// Decode the segment payload blob back into `(data, meta)`.
fn decode_payload(blob: &[u8]) -> (serde_json::Value, Option<serde_json::Value>) {
    match serde_json::from_slice::<serde_json::Value>(blob) {
        Ok(serde_json::Value::Object(mut obj)) => {
            let data = obj.remove("d").unwrap_or(serde_json::Value::Null);
            let meta = obj.remove("m");
            (data, meta)
        }
        Ok(v) => (v, None),
        Err(_) => (serde_json::Value::Null, None),
    }
}

// ===========================================================================
// Unit tests (TestClock-driven; no wall-clock sleeps).
// ===========================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::TestClock;
    use crate::config::SegmentConfig;
    use crate::storage::LocalSegmentStore;
    use serde_json::json;
    use std::sync::Arc;

    fn writer_with(cfg: SegmentConfig, clock: SharedClock) -> (SegmentWriter, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let hot = Box::new(LocalSegmentStore::open(dir.path()).unwrap());
        let tier = Arc::new(BoxTier::new(hot, None));
        (SegmentWriter::new(tier, cfg, clock), dir)
    }

    fn append(w: &mut SegmentWriter, seq: u64, ts: i64, data: serde_json::Value) -> Vec<u64> {
        w.append_record(seq, ts, None, None, &data, &None)
    }

    #[test]
    fn rolls_at_event_threshold() {
        let clock: SharedClock = Arc::new(TestClock::new(1000));
        let cfg = SegmentConfig {
            max_events: 3,
            max_bytes: 0,
            max_age_ms: 0,
            ..SegmentConfig::default()
        };
        let (mut w, _d) = writer_with(cfg, clock);

        // 3 records fit in one active segment; the 4th seals the first.
        assert!(append(&mut w, 1, 1000, json!({"v":1})).is_empty());
        assert!(append(&mut w, 2, 1000, json!({"v":2})).is_empty());
        assert!(append(&mut w, 3, 1000, json!({"v":3})).is_empty());
        assert_eq!(w.sealed_count(), 0, "still active, not sealed yet");
        let sealed = append(&mut w, 4, 1000, json!({"v":4}));
        assert_eq!(sealed, vec![1, 2, 3], "the 4th append seals seqs 1..=3");
        assert_eq!(w.sealed_count(), 1);
        assert_eq!(w.active_record_count(), 1, "seq 4 is in the new active seg");
    }

    #[test]
    fn rolls_at_byte_threshold() {
        let clock: SharedClock = Arc::new(TestClock::new(0));
        // Tiny byte cap so two small records exceed it.
        let cfg = SegmentConfig {
            max_events: 1_000_000,
            max_bytes: 40,
            max_age_ms: 0,
            ..SegmentConfig::default()
        };
        let (mut w, _d) = writer_with(cfg, clock);
        // First record: active grows past 40 bytes (frame overhead alone > 40).
        append(&mut w, 1, 0, json!({"v":"aaaaaaaa"}));
        // Second append sees the active segment already over the byte cap → seals.
        let sealed = append(&mut w, 2, 0, json!({"v":"b"}));
        assert_eq!(sealed, vec![1]);
        assert_eq!(w.sealed_count(), 1);
    }

    #[test]
    fn rolls_at_age_threshold_via_test_clock() {
        let clock = TestClock::new(1000);
        let shared: SharedClock = Arc::new(clock.clone());
        let cfg = SegmentConfig {
            max_events: 1_000_000,
            max_bytes: 0,
            max_age_ms: 5000,
            ..SegmentConfig::default()
        };
        let (mut w, _d) = writer_with(cfg, shared);
        append(&mut w, 1, 1000, json!({"v":1}));
        // Not enough age yet.
        clock.advance(4000);
        assert!(append(&mut w, 2, 5000, json!({"v":2})).is_empty());
        assert_eq!(w.sealed_count(), 0);
        // Cross the age cap: the next append seals the (now-old) active segment.
        clock.advance(2000); // total age since start (1000) = 6000 >= 5000.
        let sealed = append(&mut w, 3, 7000, json!({"v":3}));
        assert_eq!(sealed, vec![1, 2]);
        assert_eq!(w.sealed_count(), 1);

        // The idle path also seals when no further append arrives.
        clock.advance(6000);
        let idle = w.maybe_seal_idle();
        assert_eq!(idle, vec![3], "idle box seals its partial segment on age");
        assert_eq!(w.sealed_count(), 2);
    }

    #[test]
    fn sealed_read_matches_original_via_cache_and_segment() {
        let clock: SharedClock = Arc::new(TestClock::new(0));
        let cfg = SegmentConfig {
            max_events: 2,
            max_bytes: 0,
            max_age_ms: 0,
            ..SegmentConfig::default()
        };
        let (mut w, _d) = writer_with(cfg, clock);
        // Seal seqs 1,2 (cached), then 3,4.
        append(&mut w, 1, 0, json!({"v":1}));
        append(&mut w, 2, 0, json!({"a":"x"}));
        append(&mut w, 3, 0, json!({"v":3})); // seals 1,2
        append(&mut w, 4, 0, json!({"v":4}));
        append(&mut w, 5, 0, json!({"v":5})); // seals 3,4

        // Sealed reads served from the bounded cache match the originals.
        assert_eq!(w.resolve_sealed(1).unwrap().data, json!({"v":1}));
        assert_eq!(w.resolve_sealed(2).unwrap().data, json!({"a":"x"}));
        // seq 5 is still active (not sealed) → no sealed payload.
        assert!(w.resolve_sealed(5).is_none());
        assert!(w.is_sealed(1) && w.is_sealed(4) && !w.is_sealed(5));
    }

    #[test]
    fn cache_eviction_falls_back_to_segment_read() {
        let clock: SharedClock = Arc::new(TestClock::new(0));
        let cfg = SegmentConfig {
            max_events: 1,
            max_bytes: 0,
            max_age_ms: 0,
            ..SegmentConfig::default()
        };
        let (mut w, _d) = writer_with(cfg, clock);
        // Cache holds at most 1 payload → older sealed reads must hit the segment.
        w.set_cache_cap(1);
        append(&mut w, 1, 0, json!({"old":1}));
        append(&mut w, 2, 0, json!({"mid":2})); // seals 1
        append(&mut w, 3, 0, json!({"new":3})); // seals 2 → evicts seq 1 from cache

        assert_eq!(w.cache_len(), 1, "cache bounded to 1");
        // seq 1 is no longer cached → resolved by a segment read_range.
        let p1 = w.resolve_sealed(1).expect("seq 1 read from segment");
        assert_eq!(p1.data, json!({"old":1}));
        // seq 2 is the cached one.
        assert_eq!(w.resolve_sealed(2).unwrap().data, json!({"mid":2}));
    }

    #[test]
    fn meta_round_trips_through_segment() {
        let clock: SharedClock = Arc::new(TestClock::new(0));
        let cfg = SegmentConfig {
            max_events: 1,
            max_bytes: 0,
            max_age_ms: 0,
            ..SegmentConfig::default()
        };
        let (mut w, _d) = writer_with(cfg, clock);
        w.set_cache_cap(0); // force segment reads.
        w.append_record(1, 5, Some("nodeA"), Some("t:1"), &json!({"k":"v"}), &Some(json!({"m":9})));
        w.append_record(2, 6, None, None, &json!({"z":1}), &None); // seals 1
        let p = w.resolve_sealed(1).expect("read seq 1 from segment");
        assert_eq!(p.data, json!({"k":"v"}));
        assert_eq!(p.meta, Some(json!({"m":9})));
    }

    // -----------------------------------------------------------------------
    // Stage 3: relocation (HOT -> COLD), tiered reads, and the HARD INVARIANT.
    // -----------------------------------------------------------------------

    use crate::storage::{SegmentId, SegmentPart, SegmentStore, StoreError, Tier};

    /// Build a writer whose tier has a real local HOT + COLD store (the v1 cold
    /// tier = a second folder), with a custom hot-retention count.
    fn writer_with_cold(
        cfg: SegmentConfig,
        clock: SharedClock,
    ) -> (SegmentWriter, tempfile::TempDir, tempfile::TempDir) {
        let hot_dir = tempfile::tempdir().unwrap();
        let cold_dir = tempfile::tempdir().unwrap();
        let hot = Box::new(LocalSegmentStore::open(hot_dir.path()).unwrap());
        let cold: Box<dyn SegmentStore> =
            Box::new(LocalSegmentStore::open(cold_dir.path()).unwrap());
        let tier = Arc::new(BoxTier::new(hot, Some(cold)));
        (SegmentWriter::new(tier, cfg, clock), hot_dir, cold_dir)
    }

    /// Drive a full relocation of `id` against a writer's tier the same way the
    /// engine does: plan-free here (caller picked the id), copy off-lock, then
    /// confirm. (No locks in this single-threaded test; this just mirrors the
    /// engine's copy → flip → drop order.)
    fn relocate(w: &mut SegmentWriter, id: u64) {
        let tier = w.tier();
        copy_segment_to_cold(&tier, id).expect("cold copy");
        w.confirm_relocated(id);
    }

    fn cfg_events(max_events: u64, hot_retain: u64) -> SegmentConfig {
        SegmentConfig {
            max_events,
            max_bytes: 0,
            max_age_ms: 0,
            hot_retain_segments: hot_retain,
            hot_retain_bytes: 0,
        }
    }

    #[test]
    fn relocated_segment_reads_identically_from_cold() {
        let clock: SharedClock = Arc::new(TestClock::new(0));
        // Seal every record; keep only the newest sealed segment hot.
        let (mut w, _h, _c) = writer_with_cold(cfg_events(1, 1), clock);
        w.set_cache_cap(0); // force a real segment read (no cache shortcut).
        for i in 1..=4u64 {
            w.append_record(i, i as i64, Some("n"), Some("t"), &json!({"i": i}), &Some(json!({"m": i})));
        }
        // 4 records / 1-per-segment → 3 sealed (1,2,3); seq 4 active.
        assert_eq!(w.sealed_count(), 3);

        // Plan: keep newest 1 sealed (id 3) hot; relocate ids 1,2.
        let plan: Vec<u64> = w.relocation_plan().into_iter().map(|(id, _)| id).collect();
        assert_eq!(plan, vec![1, 2], "two oldest sealed segments spill to cold");

        for id in plan {
            relocate(&mut w, id);
        }
        // The relocated segments now resolve to the COLD tier...
        let tier = w.tier();
        assert_eq!(tier.resolve(1), Some(Tier::Cold));
        assert_eq!(tier.resolve(2), Some(Tier::Cold));
        assert_eq!(tier.resolve(3), Some(Tier::Hot), "newest sealed kept hot");
        // ...and the hot copy of a relocated segment is gone.
        assert!(!tier.hot().exists(1, SegmentPart::Data));
        assert!(tier.cold().unwrap().exists(1, SegmentPart::Data));

        // Reads are byte-identical regardless of tier (data + meta round-trip).
        for i in 1..=3u64 {
            let p = w.resolve_sealed(i).expect("sealed read");
            assert_eq!(p.data, json!({"i": i}), "data identical after relocation");
            assert_eq!(p.meta, Some(json!({"m": i})), "meta identical after relocation");
        }
        // A cold read bumped the cold-read counter (seqs 1 and 2 came from cold).
        assert_eq!(w.cold_reads(), 2, "two records served from the cold tier");
        // And the second read of a cold seq is served from the cold LRU (no extra
        // cold read counted).
        let _ = w.resolve_sealed(1).unwrap();
        assert_eq!(w.cold_reads(), 2, "re-read served from cold LRU, no new cold read");
    }

    #[test]
    fn interrupted_relocation_recovers_without_loss() {
        let clock: SharedClock = Arc::new(TestClock::new(0));
        let (mut w, _h, _c) = writer_with_cold(cfg_events(1, 1), clock);
        w.set_cache_cap(0);
        for i in 1..=3u64 {
            w.append_record(i, i as i64, None, None, &json!({"i": i}), &None);
        }
        let tier = w.tier();

        // --- Interruption A: crash AFTER the cold copy but BEFORE the flip+drop.
        // Both tiers hold seg 1; the pointer is still HOT. `resolve` must prefer
        // the surviving HOT copy, and the record is fully readable.
        copy_segment_to_cold(&tier, 1).unwrap();
        assert!(tier.hot().exists(1, SegmentPart::Data));
        assert!(tier.cold().unwrap().exists(1, SegmentPart::Data));
        assert_eq!(tier.resolve(1), Some(Tier::Hot), "prefer hot mid-relocation");
        assert_eq!(w.resolve_sealed(1).unwrap().data, json!({"i": 1}), "no loss");

        // The relocator simply re-runs: the (idempotent) copy is a no-op since cold
        // exists, then the flip+drop completes. Nothing is lost.
        relocate(&mut w, 1);
        assert_eq!(tier.resolve(1), Some(Tier::Cold));
        assert!(!tier.hot().exists(1, SegmentPart::Data));
        assert_eq!(w.resolve_sealed(1).unwrap().data, json!({"i": 1}), "still readable from cold");

        // --- Interruption B: crash AFTER the flip+drop but the relocator re-runs
        // confirm on an already-cold segment ⇒ a harmless no-op (idempotent).
        w.confirm_relocated(1);
        assert_eq!(tier.resolve(1), Some(Tier::Cold));
        assert_eq!(w.resolve_sealed(1).unwrap().data, json!({"i": 1}));
    }

    #[test]
    fn hot_retain_bytes_bound_spills_more_aggressively() {
        let clock: SharedClock = Arc::new(TestClock::new(0));
        // Keep up to 10 segments by count, but bound hot bytes very tightly so the
        // byte bound is the binding one.
        let cfg = SegmentConfig {
            max_events: 1,
            max_bytes: 0,
            max_age_ms: 0,
            hot_retain_segments: 10,
            hot_retain_bytes: 1, // 1 byte ⇒ keep ~0 sealed segments hot.
        };
        let (mut w, _h, _c) = writer_with_cold(cfg, clock);
        for i in 1..=4u64 {
            w.append_record(i, i as i64, None, None, &json!({"i": i}), &None);
        }
        // 3 sealed (1,2,3). The byte bound keeps none hot (each > 1 byte), so all 3
        // sealed segments are relocation candidates despite the generous count bound.
        let plan: Vec<u64> = w.relocation_plan().into_iter().map(|(id, _)| id).collect();
        assert_eq!(plan, vec![1, 2, 3], "byte bound spills all sealed segments");
    }

    /// A cold store whose **first** `read_range` blocks on a barrier until the
    /// test releases it — used to prove a slow cold fetch does NOT hold the writer
    /// lock (the HARD INVARIANT): a concurrent thread can still take the writer
    /// lock (append/seal) while the cold read is parked. Subsequent reads pass
    /// through (so the relocation copy / `read_all` of `.idx`+`.data` is not gated;
    /// only the in-test resolve read blocks).
    struct BlockingColdStore {
        inner: LocalSegmentStore,
        gate: Arc<std::sync::Barrier>,
        armed: Arc<std::sync::atomic::AtomicBool>,
    }

    impl SegmentStore for BlockingColdStore {
        fn put(&self, id: SegmentId, data: &[u8], idx: &[u8]) -> Result<(), StoreError> {
            self.inner.put(id, data, idx)
        }
        fn read_range(
            &self,
            id: SegmentId,
            part: SegmentPart,
            offset: u64,
            len: u64,
        ) -> Result<Vec<u8>, StoreError> {
            // Block exactly once (the first read after the test arms the gate), so
            // the cold fetch is provably in flight while another thread does work.
            if self
                .armed
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                self.gate.wait();
            }
            self.inner.read_range(id, part, offset, len)
        }
        fn delete(&self, id: SegmentId) -> Result<(), StoreError> {
            self.inner.delete(id)
        }
        fn list(&self) -> Result<Vec<SegmentId>, StoreError> {
            self.inner.list()
        }
        fn list_all_ids(&self) -> Result<Vec<SegmentId>, StoreError> {
            self.inner.list_all_ids()
        }
        fn exists(&self, id: SegmentId, part: SegmentPart) -> bool {
            self.inner.exists(id, part)
        }
        fn len(&self, id: SegmentId, part: SegmentPart) -> Result<u64, StoreError> {
            self.inner.len(id, part)
        }
    }

    #[test]
    fn slow_cold_read_does_not_hold_the_writer_lock() {
        use parking_lot::Mutex;
        use std::sync::atomic::{AtomicBool, Ordering};

        let clock: SharedClock = Arc::new(TestClock::new(0));
        let hot_dir = tempfile::tempdir().unwrap();
        let cold_dir = tempfile::tempdir().unwrap();
        let hot = Box::new(LocalSegmentStore::open(hot_dir.path()).unwrap());
        let gate = Arc::new(std::sync::Barrier::new(2));
        let armed = Arc::new(AtomicBool::new(false));
        let cold: Box<dyn SegmentStore> = Box::new(BlockingColdStore {
            inner: LocalSegmentStore::open(cold_dir.path()).unwrap(),
            gate: gate.clone(),
            armed: armed.clone(),
        });
        let tier = Arc::new(BoxTier::new(hot, Some(cold)));
        let mut w = SegmentWriter::new(tier.clone(), cfg_events(1, 1), clock);
        w.set_cache_cap(0);
        w.set_cold_cache_cap(0); // never cache so the read truly hits cold.
        for i in 1..=3u64 {
            w.append_record(i, i as i64, None, None, &json!({"i": i}), &None);
        }
        // Relocate seg 1 to cold (the copy uses read_all on HOT, before the cold
        // store is blocking on reads — put/exists are not gated).
        copy_segment_to_cold(&tier, 1).expect("copy to cold");
        w.confirm_relocated(1);
        assert_eq!(tier.resolve(1), Some(Tier::Cold));
        // Arm the cold store: the NEXT read_range (the reader thread's resolve)
        // blocks on the gate until the test releases it.
        armed.store(true, Ordering::SeqCst);

        // Put the writer behind a shared Mutex (mirroring `BoxState.segwriter`).
        let writer = Arc::new(Mutex::new(w));

        // Thread 1: resolve seq 1 the off-lock way — capture the locator under the
        // lock, RELEASE the lock, then do the (blocking) cold read.
        let w1 = writer.clone();
        let reader = std::thread::spawn(move || {
            let resolve = w1.lock().resolve_sealed_fast(1);
            // Lock is dropped here (resolve_sealed_fast took &self briefly).
            match resolve {
                SealedResolve::Read(loc) => {
                    assert!(loc.is_cold());
                    let p = read_locator(&loc).expect("cold read"); // BLOCKS on the gate.
                    w1.lock().record_cold_read(&loc, &p);
                    p.data
                }
                _ => panic!("expected an off-lock cold read"),
            }
        });

        // Thread 2: while the cold read is parked on the gate, this MUST be able to
        // take the writer lock and make progress (append a new record). If the cold
        // read held the lock this would deadlock; the test would hang (then fail).
        let w2 = writer.clone();
        let writer_thread = std::thread::spawn(move || {
            // Spin briefly so the reader has reached the blocking cold read.
            for _ in 0..1000 {
                std::thread::yield_now();
            }
            let mut g = w2.lock();
            // Append + seal proceeds: proves the writer lock is free during the
            // in-flight cold read.
            g.append_record(4, 4, None, None, &json!({"i": 4}), &None);
            g.append_record(5, 5, None, None, &json!({"i": 5}), &None); // seals 4
            g.sealed_count()
        });

        // Let the writer thread finish first (it must NOT be blocked by the read).
        let sealed_after = writer_thread.join().expect("writer thread");
        assert!(sealed_after >= 4, "a concurrent append/seal proceeded during the cold read");

        // Now release the gate so the cold read completes and returns correct data.
        gate.wait();
        let data = reader.join().expect("reader thread");
        assert_eq!(data, json!({"i": 1}), "cold read returns the correct payload");
        // The concurrent appends are observable (live writes were never stalled).
        assert!(writer.lock().is_sealed(4));
    }

    // -----------------------------------------------------------------------
    // Stage 4: segment-granular reclaim (drop whole sealed segments) + the
    // on-restart orphan-segment sweep.
    // -----------------------------------------------------------------------

    #[test]
    fn drop_segment_removes_files_from_both_tiers_and_registry() {
        let clock: SharedClock = Arc::new(TestClock::new(0));
        let (mut w, _h, _c) = writer_with_cold(cfg_events(1, 1), clock);
        for i in 1..=4u64 {
            w.append_record(i, i as i64, None, None, &json!({"i": i}), &None);
        }
        // Sealed 1,2,3 (seq 4 active). Relocate seg 1 to cold; seg 2,3 stay hot.
        let tier = w.tier();
        relocate(&mut w, 1);
        assert_eq!(tier.resolve(1), Some(Tier::Cold));

        // `segments_below` reports the sealed segments fully under a live floor.
        assert_eq!(w.segments_below(3), vec![1, 2], "segs 1,2 are < floor 3");

        // Drop the cold seg 1: gone from cold (its tier) and from the registry.
        w.drop_segment(1);
        assert_eq!(tier.resolve(1), None, "seg 1 dropped from both tiers");
        assert!(!w.is_sealed(1));
        assert_eq!(w.sealed_count(), 2, "segs 2,3 remain sealed");
        // Drop the hot seg 2 too.
        w.drop_segment(2);
        assert_eq!(tier.resolve(2), None);
        assert_eq!(w.sealed_count(), 1);
        // Idempotent: dropping an absent id is a no-op.
        w.drop_segment(1);
        assert_eq!(w.sealed_count(), 1);
    }

    #[test]
    fn reclaim_orphans_below_drops_only_dead_below_floor() {
        let clock: SharedClock = Arc::new(TestClock::new(0));
        let (mut w, _h, _c) = writer_with_cold(cfg_events(1, 0), clock);
        // Seal segments 1,2,3,4 (1-per-segment; seq 5 active).
        for i in 1..=5u64 {
            w.append_record(i, i as i64, None, None, &json!({"i": i}), &None);
        }
        let tier = w.tier();
        // Relocate seg 1 and 2 to cold (orphans can live in either tier).
        relocate(&mut w, 1);
        relocate(&mut w, 2);
        assert_eq!(tier.resolve(1), Some(Tier::Cold));
        assert_eq!(tier.resolve(2), Some(Tier::Cold));

        // Simulate a restart that rebuilt the registry with base_seq=3 (seqs 1,2
        // were reclaimed pre-crash): forget segs 1,2 from the in-memory registry
        // WITHOUT deleting their files (the interrupted-unlink crash window).
        w.sealed.retain(|s| s.start_seq >= 3);
        assert_eq!(w.sealed_count(), 2, "registry now covers 3,4");
        // Both orphan files (cold 1,2) are still on disk.
        assert!(tier.cold().unwrap().exists(1, SegmentPart::Data));
        assert!(tier.cold().unwrap().exists(2, SegmentPart::Data));

        // Orphan reclaim below the recovered floor (3) drops the two dead files;
        // segs 3,4 (>= floor, registered) are untouched.
        let dropped = w.reclaim_orphans_below(3);
        assert_eq!(dropped, 2, "two orphan segment files reclaimed");
        assert_eq!(tier.resolve(1), None);
        assert_eq!(tier.resolve(2), None);
        assert_eq!(tier.resolve(3), Some(Tier::Hot), "live registered seg kept");
        assert_eq!(tier.resolve(4), Some(Tier::Hot));
        // Idempotent: a second sweep finds nothing.
        assert_eq!(w.reclaim_orphans_below(3), 0);
    }
}
