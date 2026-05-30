//! Engine facade: the box registry, lazy auto-create, and dispatch of
//! write/diff/state/delete plus router forwarding.
//!
//! Phase 2 keeps all state in memory behind a [`DashMap`] of boxes and a
//! single lock over the router graph. Module boundaries are kept clean so a
//! WAL/storage layer can slide underneath in phase 4.

pub mod box_state;
pub mod broadcast;
pub mod eviction;
pub mod filters;
pub mod queue;
pub mod router;
pub mod segwriter;

use crate::clock::SharedClock;
use crate::config::{self, ServerConfig};
use crate::error::{Error, Result};
use crate::sched::Scheduler;
use crate::storage::{MatchSel, RouterOp, WalRecord, WalWriter};
use crate::types::*;
use box_state::{BoxState, DedupeEntry, StoredRecord};
use dashmap::DashMap;
use eviction::AdmitDecision;
use parking_lot::Mutex;
use router::RouterGraph;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

mod recovery;
pub mod snapshot;
pub mod wal_glue;
pub use wal_glue::WalHandle;

/// Default first seq for a fresh box instance (`0` is reserved for "no
/// records").
pub const SEQ_BASE: u64 = 1;

/// The shared engine handle.
pub struct Engine {
    /// Box registry by name. `Arc<BoxState>` so handlers hold a box without
    /// keeping the shard locked.
    boxes: DashMap<String, Arc<BoxState>>,
    /// Router registry + forwarding graph.
    routers: Mutex<RouterGraph>,
    /// Monotonic interned box-id allocator (used by WAL framing, ARCHITECTURE §2.1).
    next_box_id: AtomicU64,
    /// The WAL writer, present once a data dir is configured (durability layer,
    /// phase 4). `None` ⇒ pure in-memory mode (engine unit tests / phase-2 shape):
    /// mutating ops skip WAL append and `fsync_ms`/`wal_append_ms` report `0.0`.
    wal: Option<WalWriter>,
    /// Keeps the owned [`crate::storage::Wal`] alive (its `Drop` drains + fsyncs
    /// the writer and joins the thread). `None` in pure in-memory mode.
    _wal_owner: Option<Arc<WalHandle>>,
    /// The resolved data directory (durable mode only). Snapshots are written
    /// under `<data_dir>/meta`; `None` in pure in-memory mode.
    data_dir: Option<std::path::PathBuf>,
    /// The filesystem seam snapshot writes route through when set — the crash
    /// harness installs a [`FakeDisk`] here (via [`Engine::with_data_dir_fs`]) so
    /// `write_snapshot` lands on the same fake the WAL does. `None` in production
    /// (and pure in-memory mode): `write_snapshot` uses [`RealFs`] transparently,
    /// so the production path is byte-for-byte unchanged.
    recovery_fs: Option<Arc<dyn crate::storage::Fs>>,
    /// `bytes_written` (WAL) observed at the last snapshot, for the size-based
    /// snapshot trigger (ARCHITECTURE §3: snapshot on a size/time threshold).
    last_snapshot_bytes: AtomicU64,
    /// Wall-clock ms of the last snapshot, for the time-based trigger.
    last_snapshot_ms: AtomicU64,
    /// Readiness gate (ARCHITECTURE §4, ROADMAP Phase-4). `false` until restart
    /// recovery (snapshot load + WAL replay) has rebuilt the in-memory state;
    /// flipped to `true` exactly once, just before data-plane traffic is served.
    /// `/v0/ready` returns `503 not_ready` while this is `false` and `200 ready`
    /// after. An in-memory engine ([`Engine::new`]) has nothing to replay and is
    /// ready immediately. `/v0/health` ignores this (liveness is independent).
    ready: AtomicBool,
    /// Total WAL frames seen during recovery, and how many have been replayed so
    /// far — drives `error.detail.replay_progress` (0.0–1.0) on a `not_ready`
    /// response (API §8.2). Both `0` (⇒ progress reported as `1.0`) once ready.
    replay_total: AtomicU64,
    replay_done: AtomicU64,
    /// The priority/delivery scheduler (simplified in phase 2).
    pub scheduler: Scheduler,
    /// Time source (real or test).
    pub clock: SharedClock,
    /// Server config (limits, auth).
    pub config: ServerConfig,
    /// Process start, for `uptime_ms`.
    pub started_at: Instant,
}

impl Engine {
    /// Build a new **pure in-memory** engine (no WAL). Used by engine unit tests,
    /// property tests, and any caller that supplies no data dir. Mutating ops do
    /// not touch disk and report `wal_append_ms`/`fsync_ms` as `0.0`.
    pub fn new(config: ServerConfig, clock: SharedClock) -> Arc<Self> {
        Arc::new(Engine {
            boxes: DashMap::new(),
            routers: Mutex::new(RouterGraph::new()),
            next_box_id: AtomicU64::new(1),
            wal: None,
            _wal_owner: None,
            data_dir: None,
            recovery_fs: None,
            last_snapshot_bytes: AtomicU64::new(0),
            last_snapshot_ms: AtomicU64::new(0),
            // Pure in-memory: no WAL to replay ⇒ ready as soon as it is built.
            ready: AtomicBool::new(true),
            replay_total: AtomicU64::new(0),
            replay_done: AtomicU64::new(0),
            scheduler: Scheduler::new(clock.clone()),
            clock,
            config,
            started_at: Instant::now(),
        })
    }

    /// Build a **durable** engine backed by a WAL under `config.data_dir` (falling
    /// back to [`config::DEFAULT_DATA_DIR`]). Opens (or creates) the data dir,
    /// **replays the WAL** to rebuild the in-memory index (so durable writes
    /// survive restart), truncates any torn tail, then resumes the writer for new
    /// appends. A missing/empty data dir is a fresh start.
    pub fn with_data_dir(config: ServerConfig, clock: SharedClock) -> Result<Arc<Self>> {
        Self::with_data_dir_inner(config, clock, None)
    }

    /// As [`Engine::with_data_dir`] but routing **every byte** of recovery I/O
    /// (snapshot load, WAL replay reads, torn-tail truncation) and the resumed WAL
    /// writer's appends/fsyncs through the injected `fs` instead of [`RealFs`].
    ///
    /// This is the in-process seam the crash-consistency harness uses: it builds a
    /// real, fully-wired [`Engine`] whose WAL lives on a [`FakeDisk`], drives ops,
    /// `crash()`es the disk, and recovers a *fresh* engine through the same fake to
    /// diff the survived state against the model oracle. Test-only (`test-fs`); the
    /// production path stays [`Engine::with_data_dir`] → [`RealFs`], byte-for-byte
    /// unchanged.
    #[cfg(any(test, feature = "test-fs"))]
    pub fn with_data_dir_fs(
        config: ServerConfig,
        clock: SharedClock,
        fs: Arc<dyn crate::storage::Fs>,
    ) -> Result<Arc<Self>> {
        Self::with_data_dir_inner(config, clock, Some(fs))
    }

    /// Shared body of [`Engine::with_data_dir`] / [`Engine::with_data_dir_fs`]:
    /// build the engine shell, recover through `fs` (or [`RealFs`] when `None`),
    /// install the resumed writer, and open the readiness gate. Production passes
    /// `None` (transparent); the harness passes a fake.
    fn with_data_dir_inner(
        config: ServerConfig,
        clock: SharedClock,
        fs: Option<Arc<dyn crate::storage::Fs>>,
    ) -> Result<Arc<Self>> {
        let data_dir = config
            .data_dir
            .clone()
            .unwrap_or_else(|| config::DEFAULT_DATA_DIR.to_string());

        let engine = Arc::new(Engine {
            boxes: DashMap::new(),
            routers: Mutex::new(RouterGraph::new()),
            next_box_id: AtomicU64::new(1),
            wal: None,
            _wal_owner: None,
            data_dir: Some(std::path::PathBuf::from(&data_dir)),
            recovery_fs: fs.clone(),
            last_snapshot_bytes: AtomicU64::new(0),
            last_snapshot_ms: AtomicU64::new(0),
            // Durable engine starts NOT ready: recovery (below) must finish before
            // `/v0/ready` flips to 200, so a consumer never reads a half-replayed
            // state across a restart (ARCHITECTURE §4, ROADMAP Phase-4 ready gate).
            ready: AtomicBool::new(false),
            replay_total: AtomicU64::new(0),
            replay_done: AtomicU64::new(0),
            scheduler: Scheduler::new(clock.clone()),
            clock,
            config,
            started_at: Instant::now(),
        });

        // Recover from any existing WAL, then open the writer for new appends.
        // The engine stays `not ready` for the whole of this call; recovery
        // rebuilds the box indexes, watermarks, routers, deletes, and name<->id
        // table BEFORE we mark ready and accept data-plane traffic. An injected
        // `fs` (the crash harness) routes recovery + the resumed writer through a
        // fake disk; `None` (production) uses `RealFs` transparently.
        let dir_path = std::path::Path::new(&data_dir);
        let (handle, writer) = match fs {
            Some(fs) => recovery::recover_and_open_with(fs, &engine, dir_path),
            None => recovery::recover_and_open(&engine, dir_path),
        }
        .map_err(|e| Error::internal(format!("WAL recovery failed: {e}")))?;

        // Install the writer + owner. `engine` is uniquely owned here (just built),
        // so `Arc::get_mut` succeeds.
        let engine = {
            let mut e = engine;
            let m = Arc::get_mut(&mut e).expect("unique Arc during construction");
            m.wal = Some(writer);
            m._wal_owner = Some(Arc::new(handle));
            // Seed the snapshot triggers from the just-recovered WAL byte total and
            // the current clock, so the first auto-snapshot fires on growth/age
            // measured from startup, not from zero.
            if let Some(w) = &m.wal {
                m.last_snapshot_bytes
                    .store(w.metrics().bytes_written.load(Ordering::Relaxed), Ordering::Relaxed);
            }
            m.last_snapshot_ms
                .store(m.clock.now_ms().max(0) as u64, Ordering::Relaxed);
            e
        };
        // Recovery is complete and the writer is open: open the readiness gate so
        // `/v0/ready` answers 200. Release ordering pairs with the Acquire load in
        // `is_ready` so a reader that observes `ready` also observes all replayed
        // state. `replay_done`/`replay_total` are cleared so progress reads as 1.0.
        engine.replay_total.store(0, Ordering::Relaxed);
        engine.replay_done.store(0, Ordering::Relaxed);
        engine.ready.store(true, Ordering::Release);
        Ok(engine)
    }

    /// Whether this engine is durable (has a WAL + data dir).
    pub fn is_durable(&self) -> bool {
        self.wal.is_some() && self.data_dir.is_some()
    }

    /// Whether restart recovery has completed and the data plane may be served
    /// (ROADMAP Phase-4 ready gate). `/v0/ready` returns `200 ready` iff this is
    /// `true`, `503 not_ready` otherwise. An in-memory engine is always ready.
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    /// WAL-replay progress in `[0.0, 1.0]` for the `not_ready` response detail
    /// (API §8.2). Reported as `1.0` once ready or when no frame count is known.
    pub fn replay_progress(&self) -> f64 {
        if self.is_ready() {
            return 1.0;
        }
        let total = self.replay_total.load(Ordering::Relaxed);
        if total == 0 {
            return 0.0;
        }
        let done = self.replay_done.load(Ordering::Relaxed);
        (done as f64 / total as f64).clamp(0.0, 1.0)
    }

    /// Set the total number of WAL frames recovery will replay (for progress
    /// reporting). Called once by recovery before the replay loop begins.
    pub(crate) fn set_replay_total(&self, total: u64) {
        self.replay_total.store(total, Ordering::Relaxed);
    }

    /// Record one replayed WAL frame (advances `replay_progress`).
    pub(crate) fn note_replayed_frame(&self) {
        self.replay_done.fetch_add(1, Ordering::Relaxed);
    }

    /// Force the readiness gate open/closed (with optional replay-progress
    /// figures). Exposed only so the readiness gate can be exercised end-to-end
    /// through the real `/v0/ready` handler in tests — production code flips the
    /// gate exactly once, inside [`Engine::with_data_dir`], after recovery.
    #[doc(hidden)]
    pub fn set_ready_for_test(&self, ready: bool, done: u64, total: u64) {
        self.replay_total.store(total, Ordering::Relaxed);
        self.replay_done.store(done, Ordering::Relaxed);
        self.ready.store(ready, Ordering::Release);
    }

    /// Capture a metadata + materialized-state snapshot, write it atomically
    /// under `<data_dir>/meta`, then truncate/drop the WAL files fully absorbed
    /// by the checkpoint (ARCHITECTURE §3.1). No-op (returns `Ok(false)`) for a
    /// pure in-memory engine. Returns `Ok(true)` once a snapshot is durably
    /// written. Safe to call concurrently with writes (capture records the WAL
    /// checkpoint position *before* materializing state; see [`snapshot`]).
    pub fn write_snapshot(&self) -> Result<bool> {
        let Some(dir) = &self.data_dir else {
            return Ok(false);
        };
        if self.wal.is_none() {
            return Ok(false);
        }
        let id = match &self.recovery_fs {
            Some(fs) => crate::storage::next_snapshot_id_with(fs, dir),
            None => crate::storage::next_snapshot_id(dir),
        };
        let Some(snap) = snapshot::capture(self, id) else {
            return Ok(false);
        };
        let checkpoint = snap.checkpoint;
        // Route the snapshot write through the injected fake disk (crash harness)
        // when one is installed, so the snapshot lands on the same image the WAL
        // does and a `crash()` exercises the real atomic-swap path. Production has
        // no injected fs ⇒ the transparent `RealFs` write below.
        match &self.recovery_fs {
            Some(fs) => crate::storage::write_snapshot_with(fs, dir, &snap)
                .map_err(|e| Error::internal(format!("snapshot write failed: {e}")))?,
            None => crate::storage::write_snapshot(dir, &snap)
                .map_err(|e| Error::internal(format!("snapshot write failed: {e}")))?,
        }

        // The snapshot is durably in place: WAL files numbered strictly below the
        // checkpoint's active file are fully absorbed and can be dropped
        // (ARCHITECTURE §3.1, §2.4). The active file is kept (replay resumes from
        // its checkpoint offset).
        recovery::drop_absorbed_wal_files(dir, checkpoint.wal_idx);

        // Reset the snapshot triggers.
        if let Some(w) = &self.wal {
            self.last_snapshot_bytes
                .store(w.metrics().bytes_written.load(Ordering::Relaxed), Ordering::Relaxed);
        }
        self.last_snapshot_ms
            .store(self.clock.now_ms().max(0) as u64, Ordering::Relaxed);
        Ok(true)
    }

    /// Whether an auto-snapshot threshold has been crossed: either
    /// [`config::SNAPSHOT_BYTES_THRESHOLD`] of WAL bytes written, or
    /// [`config::SNAPSHOT_INTERVAL_MS`] elapsed, since the last snapshot. Used by
    /// the background snapshotter (no-op when there are no boxes to snapshot).
    pub fn snapshot_due(&self) -> bool {
        let Some(w) = &self.wal else { return false };
        if self.boxes.is_empty() {
            return false;
        }
        let written = w.metrics().bytes_written.load(Ordering::Relaxed);
        let since_bytes = written.saturating_sub(self.last_snapshot_bytes.load(Ordering::Relaxed));
        if since_bytes >= config::SNAPSHOT_BYTES_THRESHOLD {
            return true;
        }
        let now = self.clock.now_ms().max(0) as u64;
        let since_ms = now.saturating_sub(self.last_snapshot_ms.load(Ordering::Relaxed));
        since_ms >= config::SNAPSHOT_INTERVAL_MS
    }

    /// Number of boxes currently registered.
    pub fn box_count(&self) -> u64 {
        self.boxes.len() as u64
    }

    /// Look up a box by name.
    pub fn get_box(&self, name: &str) -> Option<Arc<BoxState>> {
        self.boxes.get(name).map(|b| b.clone())
    }

    /// Allocate the next interned box id (ARCHITECTURE §2.1).
    fn alloc_box_id(&self) -> u32 {
        self.next_box_id.fetch_add(1, Ordering::Relaxed) as u32
    }

    /// Build a per-box [`segwriter::SegmentWriter`] for a durable engine, or
    /// `None` for a pure in-memory engine (no data dir). The HOT store is a
    /// per-box dir `<data_dir>/boxes/<box_id-hex>`; the optional COLD store
    /// mirrors that under `<cold_dir>/boxes/<box_id-hex>` (ARCHITECTURE §6). On
    /// any store-open error we fall back to `None` (no writer) so a box stays
    /// fully functional via resident in-memory payloads — sealing/relocation is
    /// derivable, never load-bearing for correctness.
    fn build_segment_writer(&self, box_id: u32) -> Option<segwriter::SegmentWriter> {
        use crate::storage::{BoxTier, LocalSegmentStore};
        let data_dir = self.data_dir.as_ref()?;
        let sub = format!("boxes/{box_id:08X}");
        let hot = LocalSegmentStore::open(data_dir.join(&sub)).ok()?;
        let cold: Option<Box<dyn crate::storage::SegmentStore>> = match &self.config.cold_dir {
            Some(cd) => Some(Box::new(
                LocalSegmentStore::open(std::path::Path::new(cd).join(&sub)).ok()?,
            )),
            None => None,
        };
        let tier = Arc::new(BoxTier::new(Box::new(hot), cold));
        Some(segwriter::SegmentWriter::new(
            tier,
            self.config.segment.clone(),
            self.clock.clone(),
        ))
    }

    /// Relocate a box's hot-retention-exceeding sealed segments HOT → COLD,
    /// running the (potentially slow) copy I/O **off every write/delivery-gating
    /// lock** (the Phase-6 HARD INVARIANT). Returns the number of segments
    /// relocated. A no-op when the box has no writer or no cold tier, or nothing
    /// exceeds the hot-retention bound.
    ///
    /// State machine (crash-safe, idempotent — ARCHITECTURE §3.6):
    ///
    /// 1. PLAN — under the (brief) writer lock, list the candidate segment ids.
    /// 2. COPY — for each, read the hot `.data`/`.idx` and `put` (fsync'd) into the
    ///    cold store, **with no writer lock held**.
    /// 3. FLIP+DROP — under the writer lock, flip the in-memory tier pointer to COLD
    ///    and delete the hot copy (`confirm_relocated`).
    ///
    /// An interruption between any steps recovers cleanly: `BoxTier::resolve`
    /// prefers the surviving HOT copy, so a half-relocated segment is still
    /// readable and the relocator re-runs the idempotent copy/drop.
    pub fn relocate_box_cold(&self, name: &str) -> usize {
        let Some(b) = self.get_box(name) else {
            return 0;
        };
        let Some(sw) = b.segwriter.as_ref() else {
            return 0;
        };
        // 1. PLAN (brief lock) + grab a tier handle for the off-lock copy.
        let (plan, tier) = {
            let w = sw.lock();
            (w.relocation_plan(), w.tier())
        };
        let mut relocated = 0usize;
        for (id, _len) in plan {
            // 2. COPY off-lock (the slow step). On failure, leave HOT intact and
            //    move on — never a loss; the relocator re-runs next pass.
            match segwriter::copy_segment_to_cold(&tier, id) {
                Ok(()) => {}
                Err(e) => {
                    tracing::warn!(box_name = name, segment = id, error = %e,
                        "relocate: cold copy failed; keeping hot copy");
                    continue;
                }
            }
            // Named crash point: the cold copy is durably written (fsync'd) but the
            // tier pointer has NOT been flipped and the hot copy is still present
            // (F-COLD-CRASH-AFTER-COPY-BEFORE-FLIP). Both copies exist;
            // `BoxTier::resolve` prefers HOT, the record stays readable, and the
            // relocator re-runs the idempotent copy(no-op)+flip+drop — no loss.
            // No-op without `--features failpoints`.
            fail::fail_point!("cold::after_copy_before_flip");
            // 3. FLIP the durable tier pointer + DROP the hot copy (brief lock).
            sw.lock().confirm_relocated(id);
            relocated += 1;
        }
        relocated
    }

    /// Post-recovery segment reclaim (the final recovery step, ARCHITECTURE §4).
    /// After the snapshot + WAL replay rebuilt every box's index/floors/segment
    /// registry, re-derive the droppable segments and reclaim them idempotently —
    /// both registered sealed segments now fully below the live floor and any
    /// **orphan** segment file left on disk by a pre-crash reclaim whose unlink
    /// never completed. This makes segment reclaim crash-safe: a drop interrupted
    /// by a crash is simply re-run on the next boot, so a reclaimed segment never
    /// silently resurfaces and a half-deleted segment never leaks. No-op for a pure
    /// in-memory engine.
    pub(crate) fn reclaim_segments_on_recovery(&self) {
        let mut orphans = 0usize;
        for entry in self.boxes.iter() {
            orphans += entry.value().reclaim_segments_on_recovery();
        }
        if orphans > 0 {
            tracing::info!(orphan_segments = orphans, "recovery: reclaimed orphan segment files");
        }
    }

    /// Relocate hot-retention-exceeding sealed segments for **every** box (the
    /// background relocator pass). No-op when no cold tier is configured. Returns
    /// the total number of segments relocated across all boxes.
    pub fn relocate_all_due(&self) -> usize {
        if self.config.cold_dir.is_none() {
            return 0;
        }
        let names: Vec<String> = self.boxes.iter().map(|e| e.key().clone()).collect();
        let mut total = 0usize;
        for name in names {
            total += self.relocate_box_cold(&name);
        }
        total
    }

    /// Append a WAL frame for a mutating op and return `(wal_append_ms,
    /// fsync_ms)`. In pure in-memory mode (no WAL) this is a no-op returning
    /// `(0.0, 0.0)`. For a `durable` frame the call blocks until the group fsync
    /// returns (so `fsync_ms` is real); a non-durable frame is fire-and-forget
    /// (its durability follows the next group fsync) and `fsync_ms` is `0.0`.
    ///
    /// On a WAL error the in-memory state is already applied; we surface the
    /// error so the durability contract isn't silently violated.
    fn wal_commit(&self, record: WalRecord, durable: bool) -> Result<(f64, f64)> {
        let Some(w) = &self.wal else {
            return Ok((0.0, 0.0));
        };
        let t0 = Instant::now();
        let token = w
            .submit(record, durable)
            .map_err(|e| Error::internal(format!("WAL append failed: {e}")))?;
        let wal_append_ms = elapsed_ms(t0);
        if durable {
            let t1 = Instant::now();
            token
                .wait()
                .map_err(|e| Error::internal(format!("WAL fsync failed: {e}")))?;
            return Ok((wal_append_ms, elapsed_ms(t1)));
        }
        // Non-durable: don't wait; durability follows on the next group fsync.
        drop(token);
        Ok((wal_append_ms, 0.0))
    }

    /// Log a control frame (box config/delete, routers, deletes) and **propagate**
    /// the WAL commit result. Control frames share the WAL's durability boundary
    /// (ARCHITECTURE §2.1) and are logged durably so a crash right after the HTTP
    /// response cannot lose the mutation. In pure in-memory mode this is a no-op
    /// returning `Ok`.
    ///
    /// The caller MUST propagate an `Err` so a control-plane mutation whose WAL
    /// append/fsync FAILED yields an error response instead of a silent success
    /// that a crash would then lose (the durability contract: a 2xx control-plane
    /// mutation is durably logged). Truly best-effort frames (whose loss is
    /// self-healing) use [`Self::wal_log_best_effort`] instead.
    fn wal_log(&self, record: WalRecord, durable: bool) -> Result<(f64, f64)> {
        self.wal_commit(record, durable)
    }

    /// Best-effort control-frame log: the commit result is intentionally dropped.
    /// Reserved for frames whose loss self-heals on restart (e.g. the non-durable
    /// queue leases log, DESIGN §10.6) — NOT for a mutation a client was told
    /// succeeded. Named explicitly so the swallow is a deliberate, documented
    /// choice rather than an accident (contrast [`Self::wal_log`]).
    fn wal_log_best_effort(&self, record: WalRecord, durable: bool) {
        let _ = self.wal_commit(record, durable);
    }

    /// Log a `Delete` control frame for a queue ack / dead-letter removal so the
    /// permanent delete replays deterministically (durability == the box's
    /// `durable`: ack durability == jobs-log durability, DESIGN §10.1/§10.4).
    ///
    /// Best-effort by the queue's self-healing contract (DESIGN §10.6): if this
    /// frame is lost to a crash, the acked job simply resurfaces as claimable
    /// (at-least-once redelivery), not a silent data loss — so the swallow is the
    /// documented, correct choice for the leases projection, distinct from the
    /// (propagated) API §5 `delete`.
    pub(crate) fn wal_log_delete_seqs(&self, box_id: u32, seqs: Vec<u64>, now: i64, durable: bool) {
        self.wal_log_best_effort(
            WalRecord::Delete {
                box_id,
                before_seq: None,
                match_: None,
                seqs,
                ts: now.max(0) as u64,
            },
            durable,
        );
    }

    /// Append one leases-log lifecycle event (DESIGN §10.1). Only called when the
    /// queue's leases log is durable; logged durably so a replayed lease frame
    /// reconstructs the projection exactly. Best-effort: a lost lease frame
    /// self-heals on restart (the in-flight job becomes claimable again, the
    /// self-healing visibility timeout, DESIGN §10.6).
    pub(crate) fn wal_log_lease(&self, record: WalRecord) {
        self.wal_log_best_effort(record, true);
    }

    /// Enqueue one WAL `Append` frame per record in a write batch to the single
    /// ordered writer, returning the **last** frame's commit token (the ordered
    /// writer guarantees every prior frame in the batch commits no later) plus
    /// the enqueue time. Does **not** wait — the caller blocks on the token
    /// *after* releasing the per-box append lock, so the fsync wait never
    /// serializes other boxes' writes and durable group commit still coalesces.
    ///
    /// MUST be called while holding the box's `append_lock`, immediately after
    /// `BoxState::append` assigned the seqs, so a box's WAL frames are enqueued
    /// in the same order their seqs were assigned (recovery applies frames in
    /// WAL order and skips `seq <= head`, so out-of-order enqueue would silently
    /// drop the lower-seq frame — see `BoxState::append_lock`).
    fn wal_enqueue_batch(
        &self,
        box_id: u32,
        seqs: &[u64],
        records: &[StoredRecord],
        now: i64,
        durable: bool,
    ) -> Result<(f64, Option<crate::storage::CommitToken>)> {
        let Some(w) = &self.wal else {
            return Ok((0.0, None));
        };
        let t0 = Instant::now();
        let ts = now.max(0) as u64;
        let mut last_token = None;
        for (seq, rec) in seqs.iter().zip(records.iter()) {
            // `data` carries the opaque payload blob (data + meta, as canonical
            // JSON) so a replayed Append fully reconstructs the StoredRecord.
            let data = encode_record_payload(&rec.data, &rec.meta);
            let token = w
                .submit(
                    WalRecord::Append {
                        box_id,
                        seq: *seq,
                        ts,
                        node: rec.node.clone(),
                        tag: rec.tag.clone(),
                        data,
                    },
                    durable,
                )
                .map_err(|e| Error::internal(format!("WAL append failed: {e}")))?;
            last_token = Some(token);
        }
        Ok((elapsed_ms(t0), last_token))
    }

    /// **WAL-first append** of `records` into `dest` (the shared durable-append
    /// path used by user writes' derived appends: router forwarding and queue
    /// dead-lettering). Stages the records, enqueues their WAL `Append` frame(s),
    /// fsyncs if the box is durable, then publishes — exactly like a user write,
    /// so a forwarded/dead-lettered copy into a durable box is durable BY
    /// CONSTRUCTION and recovers naturally via WAL replay (ARCHITECTURE §2.2;
    /// fixes the silent loss of routed copies on restart).
    ///
    /// Holds `dest.append_lock` across stage → enqueue → fsync → publish so the
    /// box's WAL frames are enqueued in seq order and nothing visible is
    /// non-durable. Returns the assigned seqs. On a WAL/fsync failure the staged
    /// records are rolled back (nothing published) and the error is returned, so
    /// a failed durable forward is never acknowledged as forwarded.
    fn durable_append(&self, dest: &BoxState, records: Vec<StoredRecord>, now: i64) -> Result<Vec<u64>> {
        if records.is_empty() {
            return Ok(Vec::new());
        }
        let durable = dest.config.read().durable;
        let box_id = dest.box_id;
        let snapshot = records.clone();
        let _guard = dest.append_lock.lock();
        let staged = dest.stage_append(records);
        let seqs = staged.seqs();
        let (_wal_ms, token) = match self.wal_enqueue_batch(box_id, &seqs, &snapshot, now, durable) {
            Ok(v) => v,
            Err(e) => {
                dest.rollback_staged(staged);
                return Err(e);
            }
        };
        if durable {
            if let Some(token) = token {
                if let Err(e) = token.wait() {
                    dest.rollback_staged(staged);
                    return Err(Error::internal(format!("WAL fsync failed: {e}")));
                }
            }
        }
        dest.publish_staged(staged, now);
        Ok(seqs)
    }

    /// Compute the effective priority of a box right now (DESIGN §3.1).
    fn effective_priority(&self, b: &BoxState) -> i64 {
        let cfg = b.config.read();
        let manual = cfg.priority;
        let auto = cfg.auto_priority;
        drop(cfg);
        let last_consumed = BoxState::read_ts(&b.last_consumed_ms);
        // The age boost wants the wait time of the oldest unread record; phase 2
        // uses the box's earliest retained write recency as a stand-in. With no
        // queued work the term is 0, which is correct for the state read.
        self.scheduler
            .effective_priority(manual, auto, last_consumed, None)
    }

    // -----------------------------------------------------------------------
    // Box lifecycle (API §1)
    // -----------------------------------------------------------------------

    /// `PUT /v0/boxes/:box` — create or update a box. Returns the config and
    /// whether it was created on this call. Logs a `BoxConfig` (create/update)
    /// WAL frame so config survives restart.
    pub fn put_box(&self, name: &str, config: BoxConfig) -> Result<(bool, BoxConfig)> {
        if !config::is_valid_name(name) {
            return Err(Error::invalid_request(format!(
                "invalid box name {name:?}"
            )));
        }
        validate_config(&config)?;

        // A queue's dead-letter box must differ from itself (API §0.10).
        if config.is_queue() {
            if let Some(dl) = &config.dead_letter {
                if dl == name {
                    return Err(Error::invalid_request(
                        "dead_letter must name a different box",
                    ));
                }
            }
        }

        // `type` is immutable once a box exists (API §0.10): a `PUT` that would
        // change it is rejected with `409 box_exists_incompatible`.
        if let Some(existing) = self.get_box(name) {
            let cur_type = existing.config.read().r#type;
            if cur_type != config.r#type {
                return Err(Error::new(
                    ErrorCode::BoxExistsIncompatible,
                    format!(
                        "box {name:?} already exists as type {cur_type:?}; type is immutable"
                    ),
                )
                .with_detail(serde_json::json!({
                    "box": name,
                    "existing_type": cur_type,
                    "requested_type": config.r#type,
                })));
            }
        }

        let (created, box_id) = self.apply_put_box(name, config.clone(), None, None);

        // Log the config mutation (create or update). Box config is durable as a
        // matter of policy (control frames share the WAL's durability boundary).
        // PROPAGATE a WAL failure so a control-plane mutation a crash would lose is
        // never reported as success (bug #1: the result was previously swallowed).
        if let Err(e) = self.wal_log(
            WalRecord::BoxConfig {
                box_id,
                op: crate::storage::BoxConfigOp {
                    name: name.to_string(),
                    config: serde_json::to_vec(&config).unwrap_or_default(),
                },
                tombstone: false,
                ts: self.clock.now_ms().max(0) as u64,
            },
            true,
        ) {
            // A fresh CREATE that failed to durably log is rolled back so no
            // phantom box survives the error (it would otherwise be a box the
            // client was told did NOT get created). A config UPDATE leaves the
            // in-memory config ahead of the WAL; the client got an error and must
            // retry — the divergence is bounded to this box and self-corrects on
            // the next durable PUT (documented tradeoff: rolling back an in-place
            // config replace would need the prior config saved).
            if created {
                self.boxes.remove(name);
            }
            return Err(e);
        }

        Ok((created, config))
    }

    /// Apply a box create/update to the in-memory registry (no WAL logging).
    /// Shared by the live `put_box` and WAL replay. `forced_id`/`forced_epoch`
    /// let recovery restore the interned id + epoch from the log; live calls pass
    /// `None` and allocate fresh. Returns `(created, box_id)`.
    fn apply_put_box(
        &self,
        name: &str,
        config: BoxConfig,
        forced_id: Option<u32>,
        forced_epoch: Option<u64>,
    ) -> (bool, u32) {
        use dashmap::mapref::entry::Entry;
        match self.boxes.entry(name.to_string()) {
            Entry::Occupied(e) => {
                // Existing box → replace config in place (no epoch bump, no
                // record rewrite). Tightened caps/ttl take effect immediately.
                let b = e.get();
                *b.config.write() = config;
                b.enforce_retention(self.clock.now_ms());
                (false, b.box_id)
            }
            Entry::Vacant(e) => {
                let box_id = forced_id.unwrap_or_else(|| self.alloc_box_id());
                if let Some(fid) = forced_id {
                    // Keep the allocator ahead of any replayed id.
                    let mut cur = self.next_box_id.load(Ordering::Relaxed);
                    while (fid as u64) >= cur {
                        match self.next_box_id.compare_exchange_weak(
                            cur,
                            fid as u64 + 1,
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        ) {
                            Ok(_) => break,
                            Err(c) => cur = c,
                        }
                    }
                }
                let mut state =
                    BoxState::new(name.to_string(), box_id, config, SEQ_BASE, forced_epoch.unwrap_or(1));
                // Attach a HOT segment writer for a durable engine so committed
                // records are materialized into segments (Phase 6 Stage 2). A
                // pure in-memory engine attaches none → payloads stay resident and
                // the read path is unchanged by construction.
                if let Some(writer) = self.build_segment_writer(box_id) {
                    state.attach_segwriter(writer);
                }
                e.insert(Arc::new(state));
                (true, box_id)
            }
        }
    }

    // -----------------------------------------------------------------------
    // WAL-replay apply paths (recovery only; never re-log to the WAL).
    // -----------------------------------------------------------------------

    /// Find a box by its interned id (linear over the registry; used only by
    /// recovery, which is one-shot at startup).
    pub(crate) fn get_box_by_id(&self, box_id: u32) -> Option<Arc<BoxState>> {
        self.boxes
            .iter()
            .find(|e| e.value().box_id == box_id)
            .map(|e| e.value().clone())
    }

    /// Create/update a box during replay (no WAL logging). Returns `(created,
    /// box_id)`.
    pub(crate) fn apply_put_box_for_recovery(
        &self,
        name: &str,
        config: BoxConfig,
        forced_id: Option<u32>,
    ) -> (bool, u32) {
        self.apply_put_box(name, config, forced_id, None)
    }

    /// Remove a box during replay (box-delete tombstone). No cascade logging.
    pub(crate) fn remove_box_for_recovery(&self, name: &str) {
        self.boxes.remove(name);
        self.routers.lock().remove_touching_box(name);
    }

    /// Re-insert a replayed record at its logged seq (no WAL logging). Appends in
    /// the WAL are in per-box seq order with no gaps, so `BoxState::append`
    /// reproduces the same seq; `expected_seq` is asserted in debug builds.
    pub(crate) fn apply_append_for_recovery(
        &self,
        b: &BoxState,
        expected_seq: u64,
        rec: ReplayRecord,
    ) {
        let bytes = payload_bytes(&rec.data, &rec.meta);
        let sr = StoredRecord {
            ts: rec.ts,
            node: rec.node,
            tag: rec.tag,
            data: rec.data,
            meta: rec.meta,
            bytes,
            deleted: false,
            payload_resident: true,
        };
        let assigned = b.append(vec![sr], rec.ts);
        debug_assert_eq!(
            assigned.first().copied(),
            Some(expected_seq),
            "replay seq mismatch (box {})",
            b.name
        );
    }

    /// Re-create a router during replay (no WAL logging, no auto-create — the
    /// boxes were already materialized by their own replayed config frames; if a
    /// box is missing the router simply has no effect until one exists).
    pub(crate) fn apply_router_create_for_recovery(&self, op: RouterOp) {
        let router = Router {
            name: op.name.clone(),
            source: op.source.clone(),
            dest: op.dest.clone(),
            preserve_node: op.preserve_node,
            preserve_tag: op.preserve_tag,
            create_dest: op.create_dest,
            filter: op.filter.as_ref().map(matchsel_to_filter),
            allow_cycle: op.allow_cycle,
        };
        // Use the source's current head so a replayed router doesn't re-forward
        // historical records (matches live `put_router` semantics).
        let src_head = self.get_box(&op.source).map(|b| b.head_seq()).unwrap_or(0);
        let mut graph = self.routers.lock();
        // `upsert` can only fail on a cycle; a logged router was already accepted
        // live, so ignore the (impossible-here) error to keep replay total.
        let _ = graph.upsert(router);
        graph.note_forwarded(&op.name, src_head, 0);
    }

    /// Remove a router during replay (no WAL logging).
    pub(crate) fn apply_router_delete_for_recovery(&self, name: &str) {
        self.routers.lock().remove(name);
    }

    /// `GET /v0/boxes/:box` — box state. Never auto-creates.
    pub fn box_state(&self, name: &str, touch: bool) -> Result<BoxStateResponse> {
        let start = Instant::now();
        let b = self.get_box(name).ok_or_else(|| Error::box_not_found(name))?;
        let now = self.clock.now_ms();

        // Lazily advance floors so count/earliest_seq reflect current TTL/cap.
        b.enforce_retention(now);

        if touch {
            // A state read bumps the box's auto-priority recency clock and the
            // read recency (DESIGN §3.1).
            b.last_read_ms.store(now, Ordering::Relaxed);
            b.last_consumed_ms.store(now, Ordering::Relaxed);
        }

        let head = b.head_seq();
        let earliest = b.earliest_seq();
        let config = b.config.read().clone();
        let effective_priority = self.effective_priority(&b);

        // A queue box exposes its lease counters (§10.7) alongside the normal
        // state; a plain log omits the `queue` sub-object.
        let queue = if b.is_queue() {
            Some(self.queue_counters(&b, now))
        } else {
            None
        };

        Ok(BoxStateResponse {
            box_name: name.to_string(),
            r#type: config.r#type,
            head_seq: head,
            earliest_seq: earliest,
            next_seq: head.saturating_add(1),
            count: b.count(),
            bytes: b.bytes(),
            config,
            effective_priority,
            last_write_ts: BoxState::read_ts(&b.last_write_ms),
            last_read_ts: BoxState::read_ts(&b.last_read_ms),
            queue,
            performance: Performance::with_total(elapsed_ms(start)),
        })
    }

    /// `GET /v0/boxes` — list boxes (opaque-cursor paginated).
    pub fn list_boxes(
        &self,
        prefix: Option<&str>,
        page_size: usize,
        cursor: Option<&str>,
        touch: bool,
    ) -> Result<BoxListResponse> {
        let start = Instant::now();
        let page_size = page_size.clamp(1, config::MAX_PAGE_SIZE);
        let after = decode_cursor(cursor)?;
        let now = self.clock.now_ms();

        // Collect + sort names for stable opaque-cursor paging (API §0.7).
        let mut names: Vec<String> = self
            .boxes
            .iter()
            .map(|e| e.key().clone())
            .filter(|n| prefix.map(|p| n.starts_with(p)).unwrap_or(true))
            .filter(|n| after.as_deref().map(|a| n.as_str() > a).unwrap_or(true))
            .collect();
        names.sort();

        let has_more = names.len() > page_size;
        names.truncate(page_size);

        let mut boxes = Vec::with_capacity(names.len());
        for n in &names {
            if let Some(b) = self.get_box(n) {
                b.enforce_retention(now);
                if touch {
                    b.last_consumed_ms.store(now, Ordering::Relaxed);
                }
                let cfg = b.config.read();
                let durable = cfg.durable;
                drop(cfg);
                boxes.push(BoxSummary {
                    box_name: n.clone(),
                    head_seq: b.head_seq(),
                    earliest_seq: b.earliest_seq(),
                    count: b.count(),
                    bytes: b.bytes(),
                    durable,
                    effective_priority: self.effective_priority(&b),
                });
            }
        }

        let next_cursor = if has_more {
            names.last().map(|n| encode_cursor(n))
        } else {
            None
        };

        Ok(BoxListResponse {
            boxes,
            next_cursor,
            performance: Performance::with_total(elapsed_ms(start)),
        })
    }

    /// `DELETE /v0/boxes/:box` — delete box + cascade routers.
    pub fn delete_box(&self, name: &str, if_empty: bool) -> Result<BoxDeleteResponse> {
        let start = Instant::now();

        // Absent box → idempotent no-op (API §1.4): deleted:false, no cascade.
        let b = match self.get_box(name) {
            Some(b) => b,
            None => {
                return Ok(BoxDeleteResponse {
                    box_name: name.to_string(),
                    deleted: false,
                    routers_removed: Vec::new(),
                    performance: Performance::with_total(elapsed_ms(start)),
                });
            }
        };

        if if_empty {
            b.enforce_retention(self.clock.now_ms());
            if b.count() != 0 {
                return Err(Error::new(
                    ErrorCode::BoxNotEmpty,
                    format!("box {name:?} is not empty"),
                )
                .with_detail(serde_json::json!({ "box": name, "count": b.count() })));
            }
        }

        let box_id = b.box_id;
        self.boxes.remove(name);
        let routers_removed = self.routers.lock().remove_touching_box(name);

        // Log the box tombstone + each cascaded router removal so the delete and
        // its cascade replay deterministically (durable control frames). PROPAGATE
        // a WAL failure so a delete a crash would undo is never reported as success
        // (bug #1). The in-memory removal already happened; on a WAL failure the
        // client gets an error and retries — a deterministic retry re-derives the
        // same tombstone (the box is gone), so the delete is idempotent.
        let now = self.clock.now_ms().max(0) as u64;
        self.wal_log(
            WalRecord::BoxConfig {
                box_id,
                op: crate::storage::BoxConfigOp {
                    name: name.to_string(),
                    config: Vec::new(),
                },
                tombstone: true,
                ts: now,
            },
            true,
        )?;
        for r in &routers_removed {
            self.wal_log(
                WalRecord::RouterDelete {
                    name: r.clone(),
                    ts: now,
                },
                true,
            )?;
        }

        Ok(BoxDeleteResponse {
            box_name: name.to_string(),
            deleted: true,
            routers_removed,
            performance: Performance::with_total(elapsed_ms(start)),
        })
    }

    // -----------------------------------------------------------------------
    // Write (API §2)
    // -----------------------------------------------------------------------

    /// `POST /v0/boxes/:box` — append records, assign seqs, forward to routers.
    pub fn write(&self, name: &str, req: WriteRequest, return_seqs: bool) -> Result<WriteResponse> {
        let start = Instant::now();
        let now = self.clock.now_ms();

        // --- Validate the batch + per-record limits (DESIGN §1.2). ----------
        if req.records.is_empty() {
            return Err(Error::invalid_request("write must contain >=1 record"));
        }
        if req.records.len() > config::MAX_BATCH_RECORDS {
            return Err(Error::new(
                ErrorCode::BatchTooLarge,
                format!(
                    "batch of {} exceeds max {}",
                    req.records.len(),
                    config::MAX_BATCH_RECORDS
                ),
            ));
        }
        if let Some(k) = &req.idempotency_key {
            if k.len() > config::MAX_IDEMPOTENCY_KEY_LEN {
                return Err(Error::invalid_request("idempotency_key too long"));
            }
        }

        // Build the stored records (validating tag/node/meta/data sizes). The
        // batch-level `node` is the default; a per-record `node` overrides it.
        let mut stored = Vec::with_capacity(req.records.len());
        let mut incoming_bytes: u64 = 0;
        for rec in &req.records {
            let node = rec.node.clone().or_else(|| req.node.clone());
            let sr = build_stored(rec, node, now)?;
            incoming_bytes = incoming_bytes.saturating_add(sr.bytes);
            stored.push(sr);
        }

        // --- Resolve the box, honoring create / auto_create. ----------------
        let (b, created) = match self.get_box(name) {
            Some(b) => (b, false),
            None => {
                // Box absent: may we create it? `create:false` always refuses;
                // `create:true` always creates; an absent flag defers to the
                // would-be config's `auto_create` (the inline `config`).
                let create_cfg = req.config.clone().unwrap_or_default();
                let may_create = match req.create {
                    Some(c) => c,
                    None => create_cfg.auto_create,
                };
                if !may_create {
                    return Err(Error::box_not_found(name));
                }
                if !config::is_valid_name(name) {
                    return Err(Error::invalid_request(format!(
                        "invalid box name {name:?}"
                    )));
                }
                validate_config(&create_cfg)?;
                let (was_created, _cfg) = self.put_box(name, create_cfg)?;
                let b = self
                    .get_box(name)
                    .ok_or_else(|| Error::internal("box vanished after create"))?;
                (b, was_created)
            }
        };

        // --- Idempotency dedupe (API §0.8). --------------------------------
        // For a keyed write, hold the per-key in-flight gate across the WHOLE
        // reservation (check → stage → fsync → publish → record). This closes the
        // check-then-act race (model invariant 13): two concurrent same-key writes
        // serialize on the gate, so the loser, on acquiring it, re-checks the
        // dedupe map (now carrying the winner's entry) and returns the winner's
        // seqs instead of publishing a second distinct live batch. Different keys
        // (or none) never contend. The gate is kept alive in `_dedupe_gate` for the
        // rest of this function; `_dedupe_gate_guard` is the held inner lock.
        let window_ms = b.config.read().idempotency_window_ms as i64;
        // `dedupe_gate` (the per-key `Arc<Mutex<()>>`) is declared before the
        // guard so it outlives the borrow; `_dedupe_gate_guard` is the held lock.
        let dedupe_gate = req
            .idempotency_key
            .as_ref()
            .map(|key| b.dedupe_gate_for(key));
        let _dedupe_gate_guard = dedupe_gate.as_ref().map(|g| g.lock());
        if let Some(key) = &req.idempotency_key {
            let mut dedupe = b.dedupe.write();
            // Prune stale entries lazily.
            dedupe.retain(|_, e| now.saturating_sub(e.created_ms) <= window_ms);
            if let Some(entry) = dedupe.get(key) {
                let seqs = entry.seqs.clone();
                let head = entry.head_seq;
                drop(dedupe);
                // Drop the gate before returning (the registry entry is reclaimed
                // when no other same-key writer is parked on it).
                drop(_dedupe_gate_guard);
                if let Some(g) = dedupe_gate {
                    b.release_dedupe_gate(key, &g);
                }
                return Ok(WriteResponse {
                    box_name: name.to_string(),
                    first_seq: *seqs.first().unwrap_or(&0),
                    last_seq: *seqs.last().unwrap_or(&0),
                    seqs: if return_seqs { Some(seqs.clone()) } else { None },
                    head_seq: head,
                    count: seqs.len() as u64,
                    created: false,
                    deduped: true,
                    performance: Performance::with_total(elapsed_ms(start)),
                });
            }
        }

        // --- Admission (discard:"reject" full-box check, DESIGN §5.3). ------
        // Enforce retention first so current occupancy is the logical floor.
        b.enforce_retention(now);
        let cfg = b.config.read();
        let discard = cfg.discard;
        let cap_records = cfg.cap_records;
        let cap_bytes = cfg.cap_bytes;
        let durable = cfg.durable;
        drop(cfg);
        let box_id = b.box_id;

        // A single write larger than the whole byte cap is a permanent
        // `400 record_too_large`, distinct from a retryable `422 box_full`.
        if cap_bytes > 0 && incoming_bytes > cap_bytes && discard == Discard::Reject {
            return Err(Error::new(
                ErrorCode::RecordTooLarge,
                "write exceeds the box's entire cap_bytes",
            ));
        }
        if cap_records > 0
            && stored.len() as u64 > cap_records
            && discard == Discard::Reject
        {
            return Err(Error::new(
                ErrorCode::RecordTooLarge,
                "write exceeds the box's entire cap_records",
            ));
        }

        let decision = eviction::admit(
            discard,
            cap_records,
            cap_bytes,
            b.count(),
            b.bytes(),
            stored.len() as u64,
            incoming_bytes,
        );
        if decision == AdmitDecision::Reject {
            return Err(Error::new(
                ErrorCode::BoxFull,
                format!("box {name:?} is full (discard=reject)"),
            )
            .with_detail(serde_json::json!({
                "box": name,
                "cap_records": cap_records,
                "cap_bytes": cap_bytes,
                "head_seq": b.head_seq(),
                "earliest_seq": b.earliest_seq(),
            })));
        }

        // --- WAL-FIRST append (ARCHITECTURE §2.2). -------------------------
        // Snapshot the resolved records for router forwarding before staging
        // consumes the vec (forwarding reads the canonical $node/$tag).
        let stored_snapshot = stored.clone();

        // The per-box append lock spans the WHOLE reservation: stage seqs →
        // enqueue the WAL frame → fsync (durable) → publish/rollback. Staged
        // records sit in the index deque but are INVISIBLE (head_seq unchanged)
        // until the WAL frame is durably committed, so a reader never observes a
        // record that has not (yet) become durable, and a crash after a failed
        // fsync loses nothing that was acknowledged (not acked ⇒ not committed).
        //
        // Holding the lock across the fsync serializes a single box's durable
        // writes, but the single ordered WAL writer still coalesces frames from
        // *other* boxes into the same group commit, so cross-box durable
        // throughput is unchanged. Within one box, durable writes already
        // serialize on the index lock + seq assignment.
        let (seqs, head, wal_append_ms, fsync_ms) = {
            let _guard = b.append_lock.lock();
            let staged = b.stage_append(stored);
            let seqs = staged.seqs();
            // Enqueue the WAL frame(s) for the staged seqs (still under the lock,
            // so a box's frames are enqueued in exactly their seq order).
            let (wal_append_ms, commit_token) =
                match self.wal_enqueue_batch(box_id, &seqs, &stored_snapshot, now, durable) {
                    Ok(v) => v,
                    Err(e) => {
                        // WAL append failed before commit: publish nothing.
                        b.rollback_staged(staged);
                        return Err(e);
                    }
                };

            // Durability gate: for a `durable` box block on the last frame's commit
            // token (the single ordered writer guarantees every prior frame in the
            // batch is fsynced by then) BEFORE publishing — so the response is
            // fsync-gated and nothing visible is non-durable. A non-durable write's
            // frames are buffered and group-committed shortly after; we don't wait.
            let fsync_ms = if durable {
                if let Some(token) = commit_token {
                    let t1 = Instant::now();
                    if let Err(e) = token.wait() {
                        // fsync FAILED: roll the staged records back (they were
                        // never visible) and return an error. Not acked ⇒ not
                        // committed; no visible-but-not-durable state remains.
                        b.rollback_staged(staged);
                        return Err(Error::internal(format!("WAL fsync failed: {e}")));
                    }
                    elapsed_ms(t1)
                } else {
                    0.0
                }
            } else {
                0.0
            };

            // Durably committed (or non-durable buffered write): NOW publish the
            // staged records, making them visible + notifying waiters.
            b.publish_staged(staged, now);
            let head = b.head_seq();
            (seqs, head, wal_append_ms, fsync_ms)
        };

        // Post-append eviction for discard:"old" (may surface as a tombstone to
        // lagging consumers later).
        b.enforce_retention(now);

        // Record dedupe state for retries, then release the per-key gate (a
        // parked same-key writer wakes and dedupes to the entry just inserted).
        if let Some(key) = &req.idempotency_key {
            b.dedupe.write().insert(
                key.clone(),
                DedupeEntry {
                    seqs: seqs.clone(),
                    head_seq: head,
                    created_ms: now,
                },
            );
            drop(_dedupe_gate_guard);
            if let Some(g) = &dedupe_gate {
                b.release_dedupe_gate(key, g);
            }
        }

        // --- Router forwarding (at-least-once, per-source FIFO). -----------
        // Forward off the freshly-stored records (carrying resolved $node/$tag),
        // recursing through chained routers with a bounded hop counter.
        let forwarded: Vec<ForwardRecord> = stored_snapshot
            .into_iter()
            .map(|sr| ForwardRecord {
                data: sr.data,
                tag: sr.tag,
                node: sr.node,
                meta: sr.meta,
            })
            .collect();
        self.forward_from(name, &forwarded, now, 0);

        // Mark the box dirty in the scheduler (advisory in phase 2).
        self.scheduler
            .mark_dirty(name, self.effective_priority(&b));

        // Populate WAL timings: real `fsync_ms` for a durable box (the response
        // is fsync-gated), `0.0` for non-durable and for pure in-memory mode.
        let mut perf = Performance::with_total(elapsed_ms(start));
        perf.wal_append_ms = Some(wal_append_ms);
        perf.fsync_ms = Some(fsync_ms);

        Ok(WriteResponse {
            box_name: name.to_string(),
            first_seq: *seqs.first().unwrap_or(&0),
            last_seq: *seqs.last().unwrap_or(&0),
            seqs: if return_seqs { Some(seqs.clone()) } else { None },
            head_seq: head,
            count: seqs.len() as u64,
            created,
            deduped: false,
            performance: perf,
        })
    }

    /// Forward freshly-committed records from `source` to every router whose
    /// source is this box (at-least-once, per-source FIFO; DESIGN §8).
    ///
    /// Recurses through chained routers (A→B→C) so a forward into a box that is
    /// itself a router source fans on. `hops` is the bounded loop-breaker: a
    /// record carrying `hops >= MAX_ROUTER_HOPS` is not forwarded again, so even
    /// `allow_cycle` topologies terminate (DESIGN §8.5).
    ///
    /// Phase 2 forwards synchronously on the append path but routes the delivery
    /// work through the scheduler (`mark_dirty` on each dest) so the phase-4
    /// DWRR governor can take over without changing call sites.
    fn forward_from(&self, source: &str, records: &[ForwardRecord], now: i64, hops: u8) {
        if hops >= config::MAX_ROUTER_HOPS {
            // Hop budget exhausted: stop forwarding (loop-breaking). The records
            // already landed in this box; we just don't fan them out further.
            return;
        }
        let routes = self.routers.lock().routers_for_source(source);
        for r in routes {
            let Some(dest) = self.get_box(&r.dest) else {
                continue; // dest gone; cascade should have removed the router.
            };

            // Build the forwarded copies, applying the optional forward filter
            // and preserve_node/preserve_tag.
            let mut to_append = Vec::new();
            let mut forwarded_next = Vec::new();
            for rec in records {
                if let Some(f) = &r.filter {
                    match &rec.tag {
                        Some(t) if f.matches(t) => {}
                        _ => continue, // no tag, or tag doesn't match ⇒ skip.
                    }
                }
                let fwd_node = if r.preserve_node {
                    rec.node.clone()
                } else {
                    None
                };
                let fwd_tag = if r.preserve_tag { rec.tag.clone() } else { None };
                if let Ok(sr) = build_stored_owned(
                    rec.data.clone(),
                    fwd_tag.clone(),
                    fwd_node.clone(),
                    rec.meta.clone(),
                    now,
                ) {
                    to_append.push(sr);
                    forwarded_next.push(ForwardRecord {
                        data: rec.data.clone(),
                        tag: fwd_tag,
                        node: fwd_node,
                        meta: rec.meta.clone(),
                    });
                }
            }

            if to_append.is_empty() {
                continue;
            }

            // dst.discard="reject": if the forward would overflow, drop it and do
            // not advance the cursor (backpressure; DESIGN §6.4). Phase 2 has no
            // background retry, so an unforwardable record is simply not
            // forwarded this tick — at-least-once is preserved by the source log.
            let cfg = dest.config.read();
            let discard = cfg.discard;
            let cap_records = cfg.cap_records;
            let cap_bytes = cfg.cap_bytes;
            drop(cfg);
            if discard == Discard::Reject {
                dest.enforce_retention(now);
                let incoming_bytes: u64 = to_append.iter().map(|s| s.bytes).sum();
                let decision = eviction::admit(
                    discard,
                    cap_records,
                    cap_bytes,
                    dest.count(),
                    dest.bytes(),
                    to_append.len() as u64,
                    incoming_bytes,
                );
                if decision == AdmitDecision::Reject {
                    continue; // backpressure: leave it in src, don't advance.
                }
            }

            let count = to_append.len() as u64;
            // Forwarded copies go through the SAME WAL-first durable append path
            // as user writes (ARCHITECTURE §2.2), so a routed copy into a durable
            // destination box is durable by construction and recovers naturally via
            // WAL replay — it no longer lives only in memory and vanishes on restart
            // (the bug this fixes). A WAL/fsync failure publishes nothing and is
            // treated as backpressure: don't advance the router cursor, so the
            // source log (the durable at-least-once source of truth) re-drives it.
            match self.durable_append(&dest, to_append, now) {
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(
                        router = %r.name, dest = %r.dest, error = %e,
                        "forward: durable dest append failed; leaving in source (backpressure)"
                    );
                    continue; // don't advance the cursor; recover via the source log.
                }
            }
            dest.enforce_retention(now);

            // Mark the dest dirty in the scheduler (delivery work; advisory).
            self.scheduler
                .mark_dirty(&r.dest, self.effective_priority(&dest));

            // Advance the per-router cursor + forwarded_total.
            let src_head = self.get_box(source).map(|b| b.head_seq()).unwrap_or(0);
            self.routers.lock().note_forwarded(&r.name, src_head, count);

            // Recurse: the dest may itself be a router source (chains / cycles).
            self.forward_from(&r.dest, &forwarded_next, now, hops + 1);
        }
    }

    // -----------------------------------------------------------------------
    // Diff (API §3)
    // -----------------------------------------------------------------------

    /// `POST /v0/boxes/:box/diff` — read difference from a cursor. Never
    /// auto-creates.
    pub fn diff(&self, name: &str, req: DiffRequest) -> Result<DiffResponse> {
        let start = Instant::now();
        let b = self.get_box(name).ok_or_else(|| Error::box_not_found(name))?;
        let now = self.clock.now_ms();

        // Advance floors so the retention/tombstone boundary reflects the clock.
        b.enforce_retention(now);

        // Bump auto-priority recency (a diff "consumes" the box; DESIGN §3.1).
        b.last_read_ms.store(now, Ordering::Relaxed);
        b.last_consumed_ms.store(now, Ordering::Relaxed);

        // `limit` is clamped, never rejected (API §3): `0` ⇒ default.
        let limit = if req.limit == 0 {
            config::DEFAULT_LIMIT
        } else {
            req.limit.min(config::MAX_LIMIT)
        } as usize;

        let head = b.head_seq();
        let earliest = b.earliest_seq();
        // The involuntary (cap/TTL-only) floor: the SOLE tombstone trigger
        // (DESIGN §5.4). A purely-deleted prefix gap sits below `earliest` but
        // at/above `evict_earliest`, so it reads silently (`tombstone: null`).
        let evict_earliest = b.evict_earliest_seq();
        let from_seq = req.from_seq;

        let cfg = b.config.read();
        let ttl_ms = cfg.ttl_ms;
        let dedupe_node = cfg.dedupe_node;
        drop(cfg);
        let node_filter = if dedupe_node { req.node.as_ref() } else { None };

        // --- Tombstone / recreate detection (DESIGN §5.4/§5.5). -------------
        let mut tombstone: Option<Tombstone> = None;
        // A cursor that fell below the INVOLUNTARY floor: `from_seq + 1 <
        // evict_earliest`. Deletions never trigger a tombstone.
        let mut cursor = from_seq;
        if from_seq > head {
            // From the future relative to this box instance ⇒ delete+recreate
            // (or a stale cursor). Emit a `recreated` tombstone and resume from
            // earliest (DESIGN §5.5).
            tombstone = Some(Tombstone {
                gap_from: b.seq_base,
                gap_to: head,
                reason: TombstoneReason::Recreated,
                missed_estimate: head.saturating_sub(b.seq_base).saturating_add(1),
                earliest_seq: earliest,
                head_seq: head,
            });
            cursor = earliest.saturating_sub(1);
        } else if from_seq.saturating_add(1) < evict_earliest {
            // Involuntary cap/TTL loss reached the cursor: emit a tombstone whose
            // gap ends at the involuntary floor (`evict_earliest - 1`).
            let reason = b.floors.read().reason_for_gap(from_seq.saturating_add(1));
            tombstone = Some(eviction::build_tombstone(
                from_seq,
                evict_earliest,
                head,
                reason,
            ));
            // Resume at `earliest` (which also accounts for deletions) so any
            // purely-deleted records between the floors are skipped silently.
            cursor = earliest.saturating_sub(1).max(from_seq);
        }

        // --- Walk records, applying the read pipeline (DESIGN §7.3). --------
        // Under the index read lock we only decide deliverability and capture the
        // light locator fields (`seq/ts/node/tag` + the resident payload if it is
        // still in the slot). Any payload that was freed after sealing (Phase 6)
        // is resolved from the writer's cache/segment **after** the lock is
        // dropped, so a (potentially slow) segment read never holds the index
        // lock or blocks a concurrent write/delivery (the HARD INVARIANT).
        let mut deliverable: Vec<DiffSlot> = Vec::new();
        let mut scanned: u64 = 0;
        let mut next_from_seq = cursor;
        {
            let index = b.index.read();
            let mut seq = cursor.saturating_add(1);
            while seq <= head && deliverable.len() < limit {
                let Some(rec) = index.get(seq) else {
                    // Below base_seq (reclaimed) — skip; cursor still advances.
                    next_from_seq = seq;
                    seq += 1;
                    continue;
                };
                scanned += 1;
                let decision = filters::evaluate(
                    node_filter,
                    ttl_ms,
                    now,
                    rec.ts,
                    rec.deleted,
                    rec.node.as_deref(),
                );
                if decision == filters::ReadDecision::Deliver {
                    deliverable.push(DiffSlot {
                        seq,
                        ts: rec.ts,
                        node: rec.node.clone(),
                        tag: rec.tag.clone(),
                        resident: if rec.payload_resident {
                            Some((rec.data.clone(), rec.meta.clone()))
                        } else {
                            None
                        },
                    });
                }
                // Deleted / NodeFiltered / Expired: silently skipped; the cursor
                // still advances past the seq (DESIGN §6/§7).
                next_from_seq = seq;
                seq += 1;
            }
        }

        // Resolve any non-resident (sealed) payloads off the index lock and build
        // the wire records. A resident slot is used directly (the unchanged
        // default + the hot tail); a sealed one resolves from the recent-seal
        // cache / cold LRU (no I/O) or, on a miss, a segment `read_range` issued
        // with the writer lock RELEASED — so a slow cold fetch never gates a
        // concurrent write/delivery (the HARD INVARIANT). `cold_segments_read`
        // counts records that hit an actual cold read (a degraded historical read).
        let mut records = Vec::with_capacity(deliverable.len());
        let mut cold_segments_read: u64 = 0;
        for slot in deliverable {
            let (data, meta) = match slot.resident {
                Some(p) => p,
                None => resolve_sealed_off_lock(b.as_ref(), slot.seq, &mut cold_segments_read),
            };
            records.push(RecordOut {
                seq: slot.seq,
                ts: slot.ts,
                node: slot.node,
                tag: if req.include_tags { slot.tag } else { None },
                type_: None,
                data,
                meta: if req.include_meta { meta } else { None },
            });
        }

        let caught_up = next_from_seq == head;
        let lag = head.saturating_sub(next_from_seq);

        let mut perf = Performance::with_total(elapsed_ms(start));
        perf.records_scanned = Some(scanned);
        if cold_segments_read > 0 {
            perf.cold_segments_read = Some(cold_segments_read);
        }

        Ok(DiffResponse {
            box_name: name.to_string(),
            records,
            next_from_seq,
            head_seq: head,
            earliest_seq: earliest,
            caught_up,
            tombstone,
            lag,
            performance: perf,
        })
    }

    // -----------------------------------------------------------------------
    // Deletion (permanent, point-in-time, silent — API §5, DESIGN §7)
    // -----------------------------------------------------------------------

    /// `POST /v0/boxes/:box/delete` — permanently delete records by seq range
    /// (`before_seq`) and/or tag `match`. At least one selector is required.
    pub fn delete(&self, name: &str, req: DeleteRequest) -> Result<DeleteResponse> {
        let start = Instant::now();
        // At least one of before_seq / match is required (API §5).
        if req.before_seq.is_none() && req.match_.is_none() {
            return Err(Error::invalid_request(
                "delete requires at least one of `before_seq` or `match`",
            ));
        }
        let b = self.get_box(name).ok_or_else(|| Error::box_not_found(name))?;
        let now = self.clock.now_ms();
        let box_id = b.box_id;

        let deleted = b.apply_delete(req.before_seq, req.match_.as_ref(), now);

        // Log the delete so it replays deterministically on recovery (the deleted
        // seqs are re-derived from the rebuilt index + tag index, not stored;
        // ARCHITECTURE §2.1). Durable so a crash after the response can't revive
        // deleted records. PROPAGATE a WAL failure so a delete a crash would undo is
        // never reported as success (bug #1). The selector replays deterministically,
        // so a client retry after an error re-derives exactly the same deletion.
        self.wal_log(
            WalRecord::Delete {
                box_id,
                before_seq: req.before_seq,
                match_: req.match_.as_ref().map(filter_to_matchsel),
                seqs: Vec::new(),
                ts: now.max(0) as u64,
            },
            true,
        )?;

        Ok(DeleteResponse {
            box_name: name.to_string(),
            deleted,
            earliest_seq: b.earliest_seq(),
            head_seq: b.head_seq(),
            count: b.count(),
            bytes: b.bytes(),
            performance: Performance::with_total(elapsed_ms(start)),
        })
    }

    // -----------------------------------------------------------------------
    // Routers (API §6)
    // -----------------------------------------------------------------------

    /// `PUT /v0/routers/:router` — create/configure a router (idempotent upsert).
    ///
    /// Validates the request, auto-creates `source`/`dest` (unless
    /// `create_dest:false`), runs the DAG cycle check, then upserts. The router's
    /// forward cursor starts at the source's current head so it only forwards
    /// records committed *after* creation (no historical backfill).
    pub fn put_router(
        &self,
        name: &str,
        req: RouterCreateRequest,
    ) -> Result<(bool, RouterCreateResponse)> {
        let start = Instant::now();
        if !config::is_valid_router_name(name) {
            return Err(Error::invalid_request(format!(
                "invalid router name {name:?}"
            )));
        }
        router::validate_router(&req.source, &req.dest)?;

        // Ensure `source` exists (lazy auto-create with defaults).
        if self.get_box(&req.source).is_none() {
            self.put_box(&req.source, BoxConfig::default())?;
        }
        // Ensure `dest` exists, honoring `create_dest`.
        if self.get_box(&req.dest).is_none() {
            if !req.create_dest {
                return Err(Error::box_not_found(&req.dest));
            }
            self.put_box(&req.dest, BoxConfig::default())?;
        }

        let router = Router {
            name: name.to_string(),
            source: req.source.clone(),
            dest: req.dest.clone(),
            preserve_node: req.preserve_node,
            preserve_tag: req.preserve_tag,
            create_dest: req.create_dest,
            filter: req.filter.clone(),
            allow_cycle: req.allow_cycle,
        };

        // Forward cursor starts at the source's current head: only records
        // committed after this PUT are forwarded (per-source FIFO from "now").
        let src_head = self
            .get_box(&req.source)
            .map(|b| b.head_seq())
            .unwrap_or(0);

        let created = {
            let mut graph = self.routers.lock();
            let created = graph.upsert(router)?;
            graph.note_forwarded(name, src_head, 0);
            created
        };

        // Log the router upsert (durable control frame) so it replays on restart.
        // PROPAGATE a WAL failure so a router a crash would lose is never reported
        // as created (bug #1). A fresh CREATE that failed to durably log is removed
        // again so no phantom router survives the error.
        if let Err(e) = self.wal_log(
            WalRecord::RouterCreate {
                op: RouterOp {
                    name: name.to_string(),
                    source: req.source.clone(),
                    dest: req.dest.clone(),
                    preserve_node: req.preserve_node,
                    preserve_tag: req.preserve_tag,
                    create_dest: req.create_dest,
                    allow_cycle: req.allow_cycle,
                    filter: req.filter.as_ref().map(filter_to_matchsel),
                },
                ts: self.clock.now_ms().max(0) as u64,
            },
            true,
        ) {
            if created {
                self.routers.lock().remove(name);
            }
            return Err(e);
        }

        Ok((
            created,
            RouterCreateResponse {
                router: name.to_string(),
                created,
                source: req.source,
                dest: req.dest,
                preserve_node: req.preserve_node,
                preserve_tag: req.preserve_tag,
                filter: req.filter,
                allow_cycle: req.allow_cycle,
                performance: Performance::with_total(elapsed_ms(start)),
            },
        ))
    }

    /// `GET /v0/routers/:router`.
    pub fn get_router(&self, name: &str) -> Result<RouterGetResponse> {
        let start = Instant::now();
        let graph = self.routers.lock();
        let r = graph.get(name).ok_or_else(|| Error::router_not_found(name))?;
        let resp = RouterGetResponse {
            router: r.name.clone(),
            source: r.source.clone(),
            dest: r.dest.clone(),
            preserve_node: r.preserve_node,
            preserve_tag: r.preserve_tag,
            filter: r.filter.clone(),
            allow_cycle: r.allow_cycle,
            forwarded_total: graph.forwarded_total(name),
            performance: Performance::with_total(elapsed_ms(start)),
        };
        Ok(resp)
    }

    /// `GET /v0/routers` — list routers (filtered + opaque-cursor paginated).
    pub fn list_routers(
        &self,
        prefix: Option<&str>,
        source: Option<&str>,
        dest: Option<&str>,
        page_size: usize,
        cursor: Option<&str>,
    ) -> Result<RouterListResponse> {
        let start = Instant::now();
        let page_size = page_size.clamp(1, config::MAX_PAGE_SIZE);
        let after = decode_cursor(cursor)?;

        let graph = self.routers.lock();
        let mut summaries: Vec<RouterSummary> = graph
            .iter()
            .filter(|r| prefix.map(|p| r.name.starts_with(p)).unwrap_or(true))
            .filter(|r| source.map(|s| r.source == s).unwrap_or(true))
            .filter(|r| dest.map(|d| r.dest == d).unwrap_or(true))
            .filter(|r| after.as_deref().map(|a| r.name.as_str() > a).unwrap_or(true))
            .map(|r| RouterSummary {
                router: r.name.clone(),
                source: r.source.clone(),
                dest: r.dest.clone(),
                forwarded_total: graph.forwarded_total(&r.name),
            })
            .collect();
        summaries.sort_by(|a, b| a.router.cmp(&b.router));

        let has_more = summaries.len() > page_size;
        summaries.truncate(page_size);
        let next_cursor = if has_more {
            summaries.last().map(|s| encode_cursor(&s.router))
        } else {
            None
        };

        Ok(RouterListResponse {
            routers: summaries,
            next_cursor,
            performance: Performance::with_total(elapsed_ms(start)),
        })
    }

    /// `DELETE /v0/routers/:router` — stops forwarding immediately. Idempotent.
    pub fn delete_router(&self, name: &str) -> Result<RouterDeleteResponse> {
        let start = Instant::now();
        let deleted = self.routers.lock().remove(name);
        if deleted {
            // Only log a real removal (idempotent no-op needn't be logged).
            // PROPAGATE a WAL failure so a router-delete a crash would undo is never
            // reported as success (bug #1). On error the client retries; a
            // deterministic retry re-derives the same tombstone (idempotent).
            self.wal_log(
                WalRecord::RouterDelete {
                    name: name.to_string(),
                    ts: self.clock.now_ms().max(0) as u64,
                },
                true,
            )?;
        }
        Ok(RouterDeleteResponse {
            router: name.to_string(),
            deleted,
            performance: Performance::with_total(elapsed_ms(start)),
        })
    }

    // -----------------------------------------------------------------------
    // Watch (API §7) — session bookkeeping lives in http::watch; the engine
    // exposes the per-box read primitive used by both diff and SSE.
    // -----------------------------------------------------------------------

    /// Resolve initial per-box watch state (head/earliest) for the create
    /// response, validating that each named box exists (unless lenient).
    pub fn watch_box_states(
        &self,
        boxes: &std::collections::HashMap<String, WatchBoxOptions>,
        lenient: bool,
    ) -> Result<std::collections::HashMap<String, WatchBoxState>> {
        let now = self.clock.now_ms();
        let mut out = std::collections::HashMap::with_capacity(boxes.len());
        for (name, opts) in boxes {
            match self.get_box(name) {
                Some(b) => {
                    b.enforce_retention(now);
                    let head = b.head_seq();
                    let earliest = b.earliest_seq();
                    // `tail:true` starts at the current head (only new records).
                    let from_seq = if opts.tail { head } else { opts.from_seq };
                    out.insert(
                        name.clone(),
                        WatchBoxState {
                            from_seq,
                            head_seq: head,
                            earliest_seq: earliest,
                        },
                    );
                }
                None if lenient => continue,
                None => return Err(Error::box_not_found(name)),
            }
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// Free helpers
// ---------------------------------------------------------------------------

/// A deliverable slot captured under the diff read lock: the light locator
/// fields plus the resident payload **if** it is still in the in-memory slot.
/// A `None` `resident` means the payload was freed after sealing (Phase 6) and
/// must be resolved from the segment writer's cache/segment off the lock.
struct DiffSlot {
    seq: u64,
    ts: i64,
    node: Option<String>,
    tag: Option<String>,
    resident: Option<(serde_json::Value, Option<serde_json::Value>)>,
}

/// A record in flight through the router fan-out. Carries the resolved
/// `$node`/`$tag` (post-`preserve_*`) so chained forwards see the canonical
/// values, decoupled from the `seq`/`ts` which each dest reassigns.
#[derive(Debug, Clone)]
struct ForwardRecord {
    data: serde_json::Value,
    tag: Option<String>,
    node: Option<String>,
    meta: Option<serde_json::Value>,
}

/// Resolve a sealed (non-resident) record's payload for `seq` **without holding
/// the index lock**, and increment `cold_reads` when an actual COLD-tier read was
/// needed. The writer lock is taken only to check the in-memory caches / capture
/// a locator and (after) to fold the result into the cold LRU; the (possibly
/// slow) segment `read_range` runs with NO lock held — the Phase-6 HARD
/// INVARIANT. Returns `(Null, None)` defensively if the writer cannot resolve it.
pub(crate) fn resolve_sealed_off_lock(
    b: &BoxState,
    seq: u64,
    cold_reads: &mut u64,
) -> (serde_json::Value, Option<serde_json::Value>) {
    use segwriter::{read_locator, SealedResolve};
    let Some(sw) = b.segwriter.as_ref() else {
        return (serde_json::Value::Null, None);
    };
    let resolve = sw.lock().resolve_sealed_fast(seq);
    match resolve {
        SealedResolve::Hit(p) => (p.data, p.meta),
        SealedResolve::Read(loc) => {
            if loc.is_cold() {
                *cold_reads += 1;
            }
            match read_locator(&loc) {
                Some(p) => {
                    sw.lock().record_cold_read(&loc, &p);
                    (p.data, p.meta)
                }
                None => (serde_json::Value::Null, None),
            }
        }
        SealedResolve::NotSealed => (serde_json::Value::Null, None),
    }
}

/// Wall-time elapsed since `start`, in fractional milliseconds.
fn elapsed_ms(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}

/// Validate a box config's value ranges (API §1.1). `priority` is clamped on
/// read, but an out-of-range value supplied here is accepted and clamped by the
/// scheduler; only structurally-impossible values are rejected. Phase 2 has no
/// additional invalid combinations, so this currently always succeeds.
fn validate_config(_config: &BoxConfig) -> Result<()> {
    Ok(())
}

/// The parts of a replayed `Append` frame handed to
/// [`Engine::apply_append_for_recovery`] (bundled to keep the arg count sane).
pub(crate) struct ReplayRecord {
    pub ts: i64,
    pub node: Option<String>,
    pub tag: Option<String>,
    pub data: serde_json::Value,
    pub meta: Option<serde_json::Value>,
}

/// Map a wire [`Filter`] onto the storage-layer [`MatchSel`] logged in a
/// `Delete`/`RouterCreate` frame (the storage layer must not depend on wire
/// types).
fn filter_to_matchsel(f: &Filter) -> MatchSel {
    match f.op {
        FilterOp::Eq => MatchSel::Eq(f.value.clone()),
        FilterOp::Glob => MatchSel::Glob(f.value.clone()),
    }
}

/// Inverse of [`filter_to_matchsel`], used by WAL replay.
fn matchsel_to_filter(m: &MatchSel) -> Filter {
    match m {
        MatchSel::Eq(v) => Filter {
            op: FilterOp::Eq,
            value: v.clone(),
        },
        MatchSel::Glob(v) => Filter {
            op: FilterOp::Glob,
            value: v.clone(),
        },
    }
}

/// Encode a record's `data` + optional `meta` into the opaque WAL `data` blob.
/// A tiny JSON envelope `{"d":<data>,"m":<meta>}` (meta omitted when absent) so
/// replay reconstructs the [`StoredRecord`] exactly. `node`/`tag` ride in the
/// frame's own fields, not this blob.
fn encode_record_payload(data: &serde_json::Value, meta: &Option<serde_json::Value>) -> Vec<u8> {
    let mut obj = serde_json::Map::with_capacity(2);
    obj.insert("d".to_string(), data.clone());
    if let Some(m) = meta {
        obj.insert("m".to_string(), m.clone());
    }
    serde_json::to_vec(&serde_json::Value::Object(obj)).unwrap_or_default()
}

/// Decode the opaque WAL `data` blob back into `(data, meta)` for replay.
fn decode_record_payload(blob: &[u8]) -> (serde_json::Value, Option<serde_json::Value>) {
    match serde_json::from_slice::<serde_json::Value>(blob) {
        Ok(serde_json::Value::Object(mut obj)) => {
            let data = obj.remove("d").unwrap_or(serde_json::Value::Null);
            let meta = obj.remove("m");
            (data, meta)
        }
        // Defensive: a malformed/legacy blob round-trips as raw data.
        _ => (serde_json::Value::Null, None),
    }
}

/// Estimate the accounted byte size of a record's payload (`data` + `meta` +
/// framing). Phase 2 uses the serialized JSON length as a stable proxy.
pub(crate) fn payload_bytes(data: &serde_json::Value, meta: &Option<serde_json::Value>) -> u64 {
    let mut n = serde_json::to_vec(data).map(|v| v.len()).unwrap_or(0);
    if let Some(m) = meta {
        n += serde_json::to_vec(m).map(|v| v.len()).unwrap_or(0);
    }
    n as u64
}

/// Build a [`StoredRecord`] from an input record, validating size limits
/// (DESIGN §1.2). `node` is already resolved (per-record over batch default).
fn build_stored(rec: &RecordIn, node: Option<String>, now: i64) -> Result<StoredRecord> {
    build_stored_owned(rec.data.clone(), rec.tag.clone(), node, rec.meta.clone(), now)
}

/// Build a [`StoredRecord`] from owned parts (shared by writes + forwarding).
fn build_stored_owned(
    data: serde_json::Value,
    tag: Option<String>,
    node: Option<String>,
    meta: Option<serde_json::Value>,
    now: i64,
) -> Result<StoredRecord> {
    if let Some(t) = &tag {
        if t.len() > config::MAX_TAG_BYTES {
            return Err(Error::invalid_request(format!(
                "tag exceeds {} bytes",
                config::MAX_TAG_BYTES
            )));
        }
    }
    if let Some(n) = &node {
        if n.len() > config::MAX_NODE_BYTES {
            return Err(Error::invalid_request(format!(
                "node exceeds {} bytes",
                config::MAX_NODE_BYTES
            )));
        }
    }
    if let Some(m) = &meta {
        let mbytes = serde_json::to_vec(m).map(|v| v.len()).unwrap_or(0);
        if mbytes > config::MAX_META_BYTES {
            return Err(Error::invalid_request(format!(
                "meta exceeds {} bytes",
                config::MAX_META_BYTES
            )));
        }
        if let Some(obj) = m.as_object() {
            if obj.len() > config::MAX_META_KEYS {
                return Err(Error::invalid_request(format!(
                    "meta exceeds {} keys",
                    config::MAX_META_KEYS
                )));
            }
        }
    }
    let bytes = payload_bytes(&data, &meta);
    if bytes as usize > config::MAX_RECORD_BYTES {
        return Err(Error::new(
            ErrorCode::RecordTooLarge,
            format!("record data+meta exceeds {} bytes", config::MAX_RECORD_BYTES),
        ));
    }
    Ok(StoredRecord {
        ts: now,
        node,
        tag,
        data,
        meta,
        bytes,
        deleted: false,
        payload_resident: true,
    })
}

/// Encode an opaque list-pagination cursor as base64url JSON `{"after": name}`.
fn encode_cursor(after: &str) -> String {
    use base64::Engine;
    let json = serde_json::json!({ "after": after }).to_string();
    base64::engine::general_purpose::STANDARD.encode(json.as_bytes())
}

/// Decode an opaque list cursor back to its `after` value; `400` if corrupt.
fn decode_cursor(cursor: Option<&str>) -> Result<Option<String>> {
    let Some(c) = cursor else { return Ok(None) };
    if c.is_empty() {
        return Ok(None);
    }
    use base64::Engine;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(c)
        .map_err(|_| Error::invalid_request("malformed cursor"))?;
    let val: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|_| Error::invalid_request("malformed cursor"))?;
    Ok(val
        .get("after")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string()))
}

// ---------------------------------------------------------------------------
// Unit tests (engine core, driven through the public API with a TestClock).
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::TestClock;
    use serde_json::json;

    /// Build an engine backed by a manually-advanceable clock.
    fn engine_with_clock() -> (Arc<Engine>, TestClock) {
        let clock = TestClock::new(1_000_000);
        let shared: SharedClock = Arc::new(clock.clone());
        let engine = Engine::new(ServerConfig::default(), shared);
        (engine, clock)
    }

    /// A write request of one record with the given data/tag/node.
    fn rec(data: serde_json::Value, tag: Option<&str>, node: Option<&str>) -> RecordIn {
        RecordIn {
            data,
            tag: tag.map(str::to_string),
            node: node.map(str::to_string),
            meta: None,
        }
    }

    fn write_req(records: Vec<RecordIn>) -> WriteRequest {
        WriteRequest {
            records,
            node: None,
            idempotency_key: None,
            create: None,
            config: None,
            disable_backpressure: false,
        }
    }

    fn diff_from(from_seq: u64) -> DiffRequest {
        DiffRequest {
            from_seq,
            ..DiffRequest::default()
        }
    }

    #[test]
    fn append_then_diff_happy_path() {
        let (engine, _clock) = engine_with_clock();
        let resp = engine
            .write(
                "jobs",
                write_req(vec![
                    rec(json!({"n": 1}), Some("t1"), None),
                    rec(json!({"n": 2}), Some("t2"), None),
                ]),
                true,
            )
            .unwrap();
        assert_eq!(resp.first_seq, 1);
        assert_eq!(resp.last_seq, 2);
        assert_eq!(resp.seqs, Some(vec![1, 2]));
        assert_eq!(resp.head_seq, 2);
        assert!(resp.created);
        assert!(!resp.deduped);

        // Read from the beginning.
        let d = engine.diff("jobs", diff_from(0)).unwrap();
        assert_eq!(d.records.len(), 2);
        assert_eq!(d.records[0].seq, 1);
        assert_eq!(d.records[1].seq, 2);
        assert_eq!(d.next_from_seq, 2);
        assert_eq!(d.head_seq, 2);
        assert_eq!(d.earliest_seq, 1);
        assert!(d.caught_up);
        assert!(d.tombstone.is_none());
        assert_eq!(d.lag, 0);
        // include_tags defaults false → $tag omitted.
        assert!(d.records[0].tag.is_none());

        // Reading from head yields nothing new but stays caught up.
        let d2 = engine.diff("jobs", diff_from(2)).unwrap();
        assert!(d2.records.is_empty());
        assert!(d2.caught_up);
        assert_eq!(d2.next_from_seq, 2);

        // include_tags=true surfaces $tag.
        let dt = engine
            .diff(
                "jobs",
                DiffRequest {
                    from_seq: 0,
                    include_tags: true,
                    ..DiffRequest::default()
                },
            )
            .unwrap();
        assert_eq!(dt.records[0].tag.as_deref(), Some("t1"));
    }

    #[test]
    fn diff_on_missing_box_is_404() {
        let (engine, _clock) = engine_with_clock();
        let err = engine.diff("nope", diff_from(0)).unwrap_err();
        assert_eq!(err.code, ErrorCode::BoxNotFound);
    }

    #[test]
    fn cap_eviction_emits_cap_tombstone() {
        let (engine, _clock) = engine_with_clock();
        // cap_records=3, discard:"old".
        let cfg = BoxConfig {
            cap_records: 3,
            ..BoxConfig::default()
        };
        engine.put_box("cap", cfg).unwrap();

        // Write 5 records → seqs 1..=5; cap=3 evicts 1,2 → earliest_seq=3.
        for i in 1..=5 {
            engine
                .write("cap", write_req(vec![rec(json!({"i": i}), None, None)]), true)
                .unwrap();
        }
        let st = engine.box_state("cap", false).unwrap();
        assert_eq!(st.head_seq, 5);
        assert_eq!(st.earliest_seq, 3);
        assert_eq!(st.count, 3);

        // A consumer at from_seq=0 fell below earliest (0+1 < 3) → tombstone.
        let d = engine.diff("cap", diff_from(0)).unwrap();
        let tomb = d.tombstone.expect("expected a cap tombstone");
        assert_eq!(tomb.reason, TombstoneReason::Cap);
        assert_eq!(tomb.gap_from, 1); // from_seq + 1
        assert_eq!(tomb.gap_to, 2); // earliest_seq - 1
        assert_eq!(tomb.earliest_seq, 3);
        assert_eq!(tomb.head_seq, 5);
        // Records resume at earliest_seq.
        assert_eq!(d.records.first().map(|r| r.seq), Some(3));
        assert_eq!(d.records.len(), 3);
        assert!(d.caught_up);
    }

    #[test]
    fn ttl_expiry_emits_ttl_tombstone() {
        let (engine, clock) = engine_with_clock();
        let cfg = BoxConfig {
            ttl_ms: 1000,
            ..BoxConfig::default()
        };
        engine.put_box("ttl", cfg).unwrap();

        // Write 3 records at t0.
        for i in 1..=3 {
            engine
                .write("ttl", write_req(vec![rec(json!({"i": i}), None, None)]), true)
                .unwrap();
        }
        // Advance past the TTL so all three expire (now - ts > ttl_ms).
        clock.advance(2000);
        // Write one more so head moves and earliest can advance past expired.
        engine
            .write("ttl", write_req(vec![rec(json!({"i": 4}), None, None)]), true)
            .unwrap();

        let st = engine.box_state("ttl", false).unwrap();
        assert_eq!(st.head_seq, 4);
        // Records 1..=3 expired; only seq 4 remains.
        assert_eq!(st.earliest_seq, 4);
        assert_eq!(st.count, 1);

        let d = engine.diff("ttl", diff_from(0)).unwrap();
        let tomb = d.tombstone.expect("expected a ttl tombstone");
        assert_eq!(tomb.reason, TombstoneReason::Ttl);
        assert_eq!(tomb.gap_from, 1);
        assert_eq!(tomb.gap_to, 3);
        assert_eq!(d.records.iter().map(|r| r.seq).collect::<Vec<_>>(), vec![4]);
    }

    /// Build a delete request from optional before_seq + match shorthand.
    fn delete_req(before_seq: Option<u64>, match_: Option<&str>) -> DeleteRequest {
        DeleteRequest {
            before_seq,
            match_: match_.map(Filter::from_shorthand),
        }
    }

    // (a) before_seq / snapshot delete: records below skipped silently,
    // tombstone null, earliest advances, count drops.
    #[test]
    fn delete_before_seq_snapshot_is_silent() {
        let (engine, _clock) = engine_with_clock();
        for i in 1..=5 {
            engine
                .write("snap", write_req(vec![rec(json!({"i": i}), None, None)]), true)
                .unwrap();
        }
        // Delete everything below seq 3 (snapshot/compaction).
        let resp = engine.delete("snap", delete_req(Some(3), None)).unwrap();
        assert_eq!(resp.deleted, 2); // seqs 1,2 removed.
        assert_eq!(resp.earliest_seq, 3);
        assert_eq!(resp.count, 3);

        let d = engine.diff("snap", diff_from(0)).unwrap();
        let seqs: Vec<u64> = d.records.iter().map(|r| r.seq).collect();
        assert_eq!(seqs, vec![3, 4, 5]); // 1,2 gone.
        assert!(d.tombstone.is_none(), "deletion is silent");
        assert_eq!(d.earliest_seq, 3);
        assert!(d.caught_up);
    }

    // (b) match Eq + match Glob prefix delete of EXISTING records: gone from
    // reads, silent, count drops.
    #[test]
    fn delete_match_exact_and_prefix_is_silent() {
        let (engine, _clock) = engine_with_clock();
        engine
            .write(
                "jobs",
                write_req(vec![
                    rec(json!({"i": 1}), Some("tenant42:job-1"), None),
                    rec(json!({"i": 2}), Some("tenant42:job-2"), None),
                    rec(json!({"i": 3}), Some("other:job-9"), None),
                    rec(json!({"i": 4}), None, None),
                ]),
                true,
            )
            .unwrap();

        // Exact match delete of job-1.
        let r1 = engine
            .delete("jobs", delete_req(None, Some("tenant42:job-1")))
            .unwrap();
        assert_eq!(r1.deleted, 1);
        assert_eq!(r1.count, 3);
        let d = engine.diff("jobs", diff_from(0)).unwrap();
        let seqs: Vec<u64> = d.records.iter().map(|r| r.seq).collect();
        assert_eq!(seqs, vec![2, 3, 4]); // 1 removed (middle hole), cursor at head.
        assert!(d.tombstone.is_none(), "delete is silent");
        assert!(d.caught_up);
        assert_eq!(d.next_from_seq, 4);

        // Prefix glob delete of all tenant42:* → removes 2 as well.
        let r2 = engine
            .delete("jobs", delete_req(None, Some("tenant42:*")))
            .unwrap();
        assert_eq!(r2.deleted, 1); // only seq 2 still matched (1 already gone).
        assert_eq!(r2.count, 2);
        let d2 = engine.diff("jobs", diff_from(0)).unwrap();
        let seqs2: Vec<u64> = d2.records.iter().map(|r| r.seq).collect();
        assert_eq!(seqs2, vec![3, 4]); // tenant42:* gone; untagged stays.
        assert!(d2.tombstone.is_none());
        assert!(d2.caught_up);
    }

    // (c) point-in-time: a same-tag record written AFTER the delete is NOT
    // deleted (deletion is not a standing filter).
    #[test]
    fn delete_is_point_in_time() {
        let (engine, _clock) = engine_with_clock();
        engine
            .write("jobs", write_req(vec![rec(json!({}), Some("a:1"), None)]), true)
            .unwrap();
        // Delete all existing a:* (just seq 1).
        let r = engine.delete("jobs", delete_req(None, Some("a:*"))).unwrap();
        assert_eq!(r.deleted, 1);
        // A record written AFTER the delete with a matching tag survives.
        engine
            .write("jobs", write_req(vec![rec(json!({}), Some("a:2"), None)]), true)
            .unwrap();
        let d = engine.diff("jobs", diff_from(0)).unwrap();
        let seqs: Vec<u64> = d.records.iter().map(|r| r.seq).collect();
        assert_eq!(seqs, vec![2], "future matching record is not deleted");
        assert!(d.tombstone.is_none());
        assert!(d.caught_up);
        assert_eq!(d.next_from_seq, 2);
    }

    // (d) match + before_seq: deletes prior versions, keeps the newer same-tag
    // record (publish v2 then delete priors of msg-123).
    #[test]
    fn delete_match_and_before_seq_keeps_newer() {
        let (engine, _clock) = engine_with_clock();
        // Three versions of msg-123 (seqs 1,2,3) interleaved with another tag.
        engine
            .write(
                "msgs",
                write_req(vec![
                    rec(json!({"v": 1}), Some("msg-123"), None), // seq 1
                    rec(json!({"x": 1}), Some("msg-999"), None), // seq 2
                    rec(json!({"v": 2}), Some("msg-123"), None), // seq 3
                ]),
                true,
            )
            .unwrap();
        // Delete prior versions of msg-123 (seq < 3 AND tag == msg-123) ⇒ seq 1.
        let r = engine
            .delete("msgs", DeleteRequest {
                before_seq: Some(3),
                match_: Some(Filter::from_shorthand("msg-123")),
            })
            .unwrap();
        assert_eq!(r.deleted, 1, "only the prior msg-123 (seq 1) is removed");
        let d = engine
            .diff("msgs", DiffRequest { from_seq: 0, include_tags: true, ..DiffRequest::default() })
            .unwrap();
        let seqs: Vec<u64> = d.records.iter().map(|r| r.seq).collect();
        // seq 1 gone; seq 2 (other tag) kept; seq 3 (newer msg-123) kept.
        assert_eq!(seqs, vec![2, 3]);
        assert!(d.tombstone.is_none());
    }

    // (e) DUAL WATERMARK: a deletion is silent while a cap eviction on the same
    // box still yields reason=cap.
    #[test]
    fn delete_silent_but_cap_still_tombstones() {
        let (engine, _clock) = engine_with_clock();
        let cfg = BoxConfig {
            cap_records: 4,
            ..BoxConfig::default()
        };
        engine.put_box("dual", cfg).unwrap();
        // Write 4 (seqs 1..=4), all within cap. Delete seq 2 (a middle hole).
        for i in 1..=4 {
            engine
                .write("dual", write_req(vec![rec(json!({"i": i}), None, None)]), true)
                .unwrap();
        }
        engine.delete("dual", delete_req(Some(3), None)).unwrap(); // removes 1,2.
        // Reading from 0 across the purely-deleted prefix is SILENT.
        let d = engine.diff("dual", diff_from(0)).unwrap();
        assert!(d.tombstone.is_none(), "delete gap is silent");
        assert_eq!(d.records.iter().map(|r| r.seq).collect::<Vec<_>>(), vec![3, 4]);

        // Now overflow the cap so seqs are involuntarily evicted → reason=cap.
        for i in 5..=10 {
            engine
                .write("dual", write_req(vec![rec(json!({"i": i}), None, None)]), true)
                .unwrap();
        }
        // head=10, cap=4 ⇒ evict_floor reaches 6, earliest=7.
        let d2 = engine.diff("dual", diff_from(0)).unwrap();
        let tomb = d2.tombstone.expect("cap eviction still tombstones");
        assert_eq!(tomb.reason, TombstoneReason::Cap);
    }

    // (f) tag index efficiency path: exact + prefix matching resolve via the
    // per-box tag index (verified by correctness of the matched sets).
    #[test]
    fn tag_index_exact_and_prefix_paths() {
        let (engine, _clock) = engine_with_clock();
        engine
            .write(
                "tix",
                write_req(vec![
                    rec(json!({}), Some("chat-42:a"), None),  // seq 1
                    rec(json!({}), Some("chat-42:b"), None),  // seq 2
                    rec(json!({}), Some("chat-420:c"), None), // seq 3 (not chat-42:*)
                    rec(json!({}), Some("zzz"), None),        // seq 4
                ]),
                true,
            )
            .unwrap();
        // Exact: deletes only the one exact tag.
        let e = engine.delete("tix", delete_req(None, Some("chat-42:a"))).unwrap();
        assert_eq!(e.deleted, 1);
        // Prefix chat-42:* matches seq 2 only now (seq 1 gone, seq 3 is chat-420).
        let p = engine.delete("tix", delete_req(None, Some("chat-42:*"))).unwrap();
        assert_eq!(p.deleted, 1, "prefix range scan must not match chat-420:c");
        let d = engine
            .diff("tix", DiffRequest { from_seq: 0, include_tags: true, ..DiffRequest::default() })
            .unwrap();
        let tags: Vec<&str> = d.records.iter().filter_map(|r| r.tag.as_deref()).collect();
        assert_eq!(tags, vec!["chat-420:c", "zzz"]);
    }

    #[test]
    fn delete_requires_a_selector() {
        let (engine, _clock) = engine_with_clock();
        engine
            .write("b", write_req(vec![rec(json!({}), None, None)]), true)
            .unwrap();
        let err = engine
            .delete("b", DeleteRequest::default())
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidRequest);
    }

    #[test]
    fn node_loop_prevention_advances_cursor_to_caught_up() {
        let (engine, _clock) = engine_with_clock();
        // All records written by node "self".
        engine
            .write(
                "box",
                WriteRequest {
                    records: vec![
                        rec(json!({"i": 1}), None, Some("self")),
                        rec(json!({"i": 2}), None, Some("self")),
                        rec(json!({"i": 3}), None, Some("other")),
                    ],
                    node: None,
                    idempotency_key: None,
                    create: None,
                    config: None,
                    disable_backpressure: false,
                },
                true,
            )
            .unwrap();

        // Reader presenting node "self" never receives its own records, but the
        // cursor advances past them to caught_up (no infinite empty loop).
        let d = engine
            .diff(
                "box",
                DiffRequest {
                    from_seq: 0,
                    node: Some(NodeFilter::One("self".to_string())),
                    ..DiffRequest::default()
                },
            )
            .unwrap();
        let seqs: Vec<u64> = d.records.iter().map(|r| r.seq).collect();
        assert_eq!(seqs, vec![3]); // only the "other"-node record.
        assert!(d.caught_up);
        assert_eq!(d.next_from_seq, 3);
        assert!(d.tombstone.is_none()); // node filtering is silent.

        // A box of ONLY own-node records: zero delivered but caught_up reached.
        engine
            .write(
                "selfbox",
                WriteRequest {
                    records: vec![
                        rec(json!({}), None, Some("me")),
                        rec(json!({}), None, Some("me")),
                    ],
                    node: None,
                    idempotency_key: None,
                    create: None,
                    config: None,
                    disable_backpressure: false,
                },
                true,
            )
            .unwrap();
        let d2 = engine
            .diff(
                "selfbox",
                DiffRequest {
                    from_seq: 0,
                    node: Some(NodeFilter::One("me".to_string())),
                    ..DiffRequest::default()
                },
            )
            .unwrap();
        assert!(d2.records.is_empty());
        assert!(d2.caught_up);
        assert_eq!(d2.next_from_seq, 2);
    }

    #[test]
    fn idempotency_dedupe_returns_original_seqs() {
        let (engine, clock) = engine_with_clock();
        let req = || WriteRequest {
            records: vec![rec(json!({"job": 1}), None, None)],
            node: None,
            idempotency_key: Some("batch-7".to_string()),
            create: None,
            config: None,
            disable_backpressure: false,
        };

        let first = engine.write("q", req(), true).unwrap();
        assert_eq!(first.seqs, Some(vec![1]));
        assert!(!first.deduped);

        // Retry with the same key in-window → original seqs, no new append.
        let second = engine.write("q", req(), true).unwrap();
        assert!(second.deduped);
        assert_eq!(second.seqs, Some(vec![1]));
        assert_eq!(second.head_seq, 1);

        // Box still has exactly one record.
        assert_eq!(engine.box_state("q", false).unwrap().head_seq, 1);

        // After the dedupe window elapses, the same key appends again.
        clock.advance(default_idempotency_window_ms_for_test() + 1);
        let third = engine.write("q", req(), true).unwrap();
        assert!(!third.deduped);
        assert_eq!(third.seqs, Some(vec![2]));
    }

    fn default_idempotency_window_ms_for_test() -> i64 {
        BoxConfig::default().idempotency_window_ms as i64
    }

    #[test]
    fn discard_reject_full_box_is_422() {
        let (engine, _clock) = engine_with_clock();
        let cfg = BoxConfig {
            cap_records: 2,
            discard: Discard::Reject,
            ..BoxConfig::default()
        };
        engine.put_box("q", cfg).unwrap();
        engine
            .write("q", write_req(vec![rec(json!({}), None, None)]), true)
            .unwrap();
        engine
            .write("q", write_req(vec![rec(json!({}), None, None)]), true)
            .unwrap();
        // Third write overflows cap=2 with discard:reject → 422 box_full.
        let err = engine
            .write("q", write_req(vec![rec(json!({}), None, None)]), true)
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::BoxFull);
        // Nothing appended (all-or-nothing).
        assert_eq!(engine.box_state("q", false).unwrap().head_seq, 2);
    }

    #[test]
    fn create_false_on_missing_box_is_404() {
        let (engine, _clock) = engine_with_clock();
        let req = WriteRequest {
            records: vec![rec(json!({}), None, None)],
            node: None,
            idempotency_key: None,
            create: Some(false),
            config: None,
            disable_backpressure: false,
        };
        let err = engine.write("typo", req, true).unwrap_err();
        assert_eq!(err.code, ErrorCode::BoxNotFound);
    }

    #[test]
    fn delete_recreate_emits_recreated_tombstone() {
        let (engine, _clock) = engine_with_clock();
        for _ in 0..5 {
            engine
                .write("b", write_req(vec![rec(json!({}), None, None)]), true)
                .unwrap();
        }
        // A stale consumer is at from_seq=5 (== head).
        engine.delete_box("b", false).unwrap();
        // Recreate (lazy) — seq restarts at 1.
        engine
            .write("b", write_req(vec![rec(json!({}), None, None)]), true)
            .unwrap();
        // Consumer's old cursor 5 is now from the future (head=1).
        let d = engine.diff("b", diff_from(5)).unwrap();
        let tomb = d.tombstone.expect("expected a recreated tombstone");
        assert_eq!(tomb.reason, TombstoneReason::Recreated);
        // New record delivered after the rewind.
        assert_eq!(d.records.iter().map(|r| r.seq).collect::<Vec<_>>(), vec![1]);
    }

    /// A router-create request with the documented defaults.
    fn router_req(source: &str, dest: &str) -> RouterCreateRequest {
        RouterCreateRequest {
            source: source.to_string(),
            dest: dest.to_string(),
            preserve_node: true,
            preserve_tag: true,
            create_dest: true,
            filter: None,
            allow_cycle: false,
        }
    }

    #[test]
    fn router_fanout_forwards_and_preserves_node() {
        let (engine, _clock) = engine_with_clock();
        // src exists; router auto-creates dst.
        let (created, resp) = engine
            .put_router("src->dst", router_req("src", "dst"))
            .unwrap();
        assert!(created);
        assert_eq!(resp.source, "src");
        assert_eq!(resp.dest, "dst");

        // Write to src with an origin node; it must appear in dst with $node kept.
        engine
            .write(
                "src",
                write_req(vec![
                    rec(json!({"i": 1}), Some("t1"), Some("nodeA")),
                    rec(json!({"i": 2}), None, Some("nodeB")),
                ]),
                true,
            )
            .unwrap();

        // dst received both, in src commit order, $node preserved.
        let d = engine
            .diff(
                "dst",
                DiffRequest {
                    from_seq: 0,
                    include_tags: true,
                    ..DiffRequest::default()
                },
            )
            .unwrap();
        assert_eq!(d.records.len(), 2);
        assert_eq!(d.records[0].data, json!({"i": 1}));
        assert_eq!(d.records[0].node.as_deref(), Some("nodeA"));
        assert_eq!(d.records[0].tag.as_deref(), Some("t1")); // preserve_tag.
        assert_eq!(d.records[1].node.as_deref(), Some("nodeB"));
        // dst assigned its own fresh seqs starting at 1.
        assert_eq!(d.records[0].seq, 1);
        assert_eq!(d.records[1].seq, 2);

        // forwarded_total reflects the two forwarded records.
        let g = engine.get_router("src->dst").unwrap();
        assert_eq!(g.forwarded_total, 2);

        // Deleting the router stops forwarding; already-forwarded records remain.
        assert!(engine.delete_router("src->dst").unwrap().deleted);
        engine
            .write("src", write_req(vec![rec(json!({"i": 3}), None, None)]), true)
            .unwrap();
        let d2 = engine.diff("dst", diff_from(0)).unwrap();
        assert_eq!(d2.records.len(), 2); // still just the first two.

        // Re-deleting is idempotent (deleted:false).
        assert!(!engine.delete_router("src->dst").unwrap().deleted);
    }

    #[test]
    fn router_preserve_node_false_clears_node() {
        let (engine, _clock) = engine_with_clock();
        engine
            .put_router(
                "s->d",
                RouterCreateRequest {
                    preserve_node: false,
                    ..router_req("s", "d")
                },
            )
            .unwrap();
        engine
            .write("s", write_req(vec![rec(json!({}), None, Some("origin"))]), true)
            .unwrap();
        let d = engine.diff("d", diff_from(0)).unwrap();
        assert_eq!(d.records.len(), 1);
        assert!(d.records[0].node.is_none()); // cleared.
    }

    #[test]
    fn router_forward_filter_drops_nonmatching() {
        let (engine, _clock) = engine_with_clock();
        engine
            .put_router(
                "s->d",
                RouterCreateRequest {
                    filter: Some(Filter::from_shorthand("public:*")),
                    ..router_req("s", "d")
                },
            )
            .unwrap();
        engine
            .write(
                "s",
                write_req(vec![
                    rec(json!({"a": 1}), Some("public:1"), None),
                    rec(json!({"a": 2}), Some("private:1"), None),
                    rec(json!({"a": 3}), None, None), // no tag ⇒ never matches.
                ]),
                true,
            )
            .unwrap();
        let d = engine.diff("d", diff_from(0)).unwrap();
        let data: Vec<_> = d.records.iter().map(|r| r.data.clone()).collect();
        assert_eq!(data, vec![json!({"a": 1})]); // only public:1 forwarded.
    }

    #[test]
    fn router_cycle_rejected_409() {
        let (engine, _clock) = engine_with_clock();
        engine.put_router("a->b", router_req("a", "b")).unwrap();
        engine.put_router("b->c", router_req("b", "c")).unwrap();
        // c->a would close a cycle a->b->c->a.
        let err = engine
            .put_router("c->a", router_req("c", "a"))
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::RouterCycle);
        // The cycle path is reported in detail.
        let detail = err.detail.expect("cycle detail");
        assert!(detail.get("cycle").is_some());

        // A re-PUT of an existing (non-cycle) router is idempotent, not a cycle.
        let (created, _) = engine.put_router("a->b", router_req("a", "b")).unwrap();
        assert!(!created);
    }

    #[test]
    fn router_allow_cycle_terminates_via_hop_cap() {
        let (engine, _clock) = engine_with_clock();
        // A two-box mirror a<->b with allow_cycle on both edges.
        let edge = |s, d| RouterCreateRequest {
            allow_cycle: true,
            ..router_req(s, d)
        };
        engine.put_router("a->b", edge("a", "b")).unwrap();
        engine.put_router("b->a", edge("b", "a")).unwrap();

        // One write to `a` would loop forever without the hop cap; it must
        // terminate. Just assert the call returns and both boxes have a bounded
        // number of records (no hang / unbounded growth).
        engine
            .write("a", write_req(vec![rec(json!({"x": 1}), None, Some("A"))]), true)
            .unwrap();

        let a = engine.box_state("a", false).unwrap();
        let b = engine.box_state("b", false).unwrap();
        // Bounded by the hop cap (MAX_ROUTER_HOPS=8): a handful of copies, never
        // unbounded. The exact count is implementation-defined but small.
        assert!(a.head_seq >= 1 && a.head_seq <= config::MAX_ROUTER_HOPS as u64 + 1);
        assert!(b.head_seq >= 1 && b.head_seq <= config::MAX_ROUTER_HOPS as u64 + 1);
        // $node is preserved through the cycle (loop-prevention key intact).
        let d = engine.diff("b", diff_from(0)).unwrap();
        assert_eq!(d.records[0].node.as_deref(), Some("A"));
    }

    #[test]
    fn router_create_dest_false_on_missing_is_404() {
        let (engine, _clock) = engine_with_clock();
        let err = engine
            .put_router(
                "s->d",
                RouterCreateRequest {
                    create_dest: false,
                    ..router_req("s", "d")
                },
            )
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::BoxNotFound);
    }

    #[test]
    fn delete_box_cascades_routers() {
        let (engine, _clock) = engine_with_clock();
        engine.put_router("a->b", router_req("a", "b")).unwrap();
        engine.put_router("b->c", router_req("b", "c")).unwrap();
        // Deleting `b` removes both routers touching it.
        let resp = engine.delete_box("b", false).unwrap();
        assert!(resp.deleted);
        let mut removed = resp.routers_removed.clone();
        removed.sort();
        assert_eq!(removed, vec!["a->b".to_string(), "b->c".to_string()]);
        // Neither router resolvable anymore.
        assert!(engine.get_router("a->b").is_err());
        assert!(engine.list_routers(None, None, None, 100, None).unwrap().routers.is_empty());
    }

    #[test]
    fn router_get_missing_is_404_and_list_routers() {
        let (engine, _clock) = engine_with_clock();
        let err = engine.get_router("nope").unwrap_err();
        assert_eq!(err.code, ErrorCode::RouterNotFound);

        engine.put_router("a->b", router_req("a", "b")).unwrap();
        engine.put_router("a->c", router_req("a", "c")).unwrap();
        // Filter by source.
        let listed = engine
            .list_routers(None, Some("a"), None, 100, None)
            .unwrap();
        assert_eq!(listed.routers.len(), 2);
        // Filter by dest.
        let by_dest = engine
            .list_routers(None, None, Some("c"), 100, None)
            .unwrap();
        assert_eq!(by_dest.routers.len(), 1);
        assert_eq!(by_dest.routers[0].router, "a->c");
    }

    #[test]
    fn list_boxes_prefix_and_paging() {
        let (engine, _clock) = engine_with_clock();
        for n in ["a1", "a2", "a3", "b1"] {
            engine
                .write(n, write_req(vec![rec(json!({}), None, None)]), true)
                .unwrap();
        }
        // Prefix filter.
        let page = engine.list_boxes(Some("a"), 100, None, false).unwrap();
        assert_eq!(page.boxes.len(), 3);
        assert!(page.next_cursor.is_none());

        // Paging: page_size 2 → cursor → next page.
        let p1 = engine.list_boxes(Some("a"), 2, None, false).unwrap();
        assert_eq!(p1.boxes.len(), 2);
        let cursor = p1.next_cursor.expect("more pages");
        let p2 = engine
            .list_boxes(Some("a"), 2, Some(&cursor), false)
            .unwrap();
        assert_eq!(p2.boxes.len(), 1);
        assert!(p2.next_cursor.is_none());
    }
}
