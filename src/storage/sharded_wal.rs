//! Sharded write-ahead log (WAL sharding): N independent [`Wal`] shards, each
//! with its own WAL file set + writer thread + mpsc ingest + adaptive group
//! commit + per-shard rotation/backpressure/EIO handling, fronted by a stable
//! topic-id → shard router.
//!
//! # Why shard
//!
//! A single ordered WAL writer (one thread / mpsc / fsync stream) serializes ALL
//! durable writes — the write-throughput bottleneck. Splitting it into N shards
//! removes the global contention on the per-write hot path: there is **no** single
//! global seq allocator, registry lock, notify, or hot metrics atomic shared
//! across shards. Each topic is routed to exactly ONE shard for the lifetime of a
//! run (a stable hash of its interned `topic_id`), so every per-topic ordering /
//! durability guarantee still holds within that shard's single ordered writer:
//!
//! - **per-topic seq order** — a topic's frames all go to one shard's ordered writer,
//!   so they are written in seq order exactly as before.
//! - **commit sequencer / publish tickets** — per-topic, unaffected by the shard
//!   split (they coordinate writers of the same topic, all on one shard).
//! - **R3 durable head watermark** — the `HeadWatermark` frame rides the same
//!   shard as the topic's appends, so its fsync still orders ahead of the acked seq.
//! - **R5 atomic batch** — a caller batch is one topic's records; `submit_batch`
//!   lands it on that topic's shard as one bounded-channel slot (all-or-none).
//! - **durability classes** — the per-shard writer fsyncs (or not) exactly as the
//!   single writer did.
//! - **WAL-first delete** — a `Delete` frame rides the topic's shard.
//!
//! Cross-topic global order was never a guarantee, so spreading different topics
//! across shards changes nothing observable.
//!
//! # On-disk layout (shard-count-agnostic recovery)
//!
//! - `shards == 1`: the flat single-shard layout `<data_dir>/wal/wal-<idx>.log`.
//! - `shards > 1`: `<data_dir>/wal/shard-<NN>/wal-<idx>.log`, one subdir per shard.
//!
//! Recovery (see [`crate::engine::recovery`]) replays **every** WAL file it finds
//! — the flat files AND every `shard-*/` subdir — dispatching each frame to its
//! topic by `topic_id`, never assuming a topic lives in `topic_id % N`. This is what lets
//! `TOPICS_WAL_SHARDS` be reconfigured between restarts with no data loss: a dir
//! written with 8 shards recovers correctly when reopened with 1 (or 4, or 16).

use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::fs::Fs;
use super::wal::{CommitToken, Wal, WalConfig, WalError, WalMetrics, WalRecord, WalWriter};

/// The subdirectory name for shard `s` of `n` (`shard-00`, `shard-01`, …). Only
/// used when `n > 1`; a single shard uses the flat layout (no subdir).
pub fn shard_subdir(s: usize) -> String {
    format!("shard-{s:02}")
}

/// The PHYSICAL group key for shard `s` of `n`: the relative dir name under `wal/`
/// — `""` for the flat single-shard layout (`n == 1`), or `shard-NN` for a sharded
/// layout (`n > 1`). This is the stable identity the snapshot checkpoint records so
/// a position is only ever applied to the exact physical group it was measured
/// against (a flat group and a `shard-00/` group are distinct keys, never
/// conflated across a `TOPICS_WAL_SHARDS` reconfigure).
pub fn shard_group_key(s: usize, n: usize) -> String {
    if n <= 1 {
        String::new()
    } else {
        shard_subdir(s)
    }
}

/// Map an interned `topic_id` to a shard index in `0..n`. A stable hash (XXH3) of
/// the id keeps the distribution even and the mapping deterministic within a run.
/// `topic_id == 0` (topic-agnostic control frames: routers, checkpoints) is pinned to
/// shard 0 so those frames always have a home; they are topic-agnostic and replay
/// shard-independently, so their shard is irrelevant to recovery.
#[inline]
pub fn shard_for_topic(topic_id: u64, n: usize) -> usize {
    debug_assert!(n >= 1);
    if n <= 1 || topic_id == 0 {
        // Single shard ⇒ everything on shard 0. topic_id 0 (topic-agnostic control
        // frames: routers, checkpoints) is pinned to shard 0 — they replay
        // shard-independently, so a deterministic home keeps placement predictable.
        return 0;
    }
    // XXH3 of the 8 id bytes, modulo the shard count. Cheap, even, deterministic.
    let h = xxhash_rust::xxh3::xxh3_64(&topic_id.to_le_bytes());
    (h % n as u64) as usize
}

/// A handle the engine holds to submit records to the sharded WAL: it routes each
/// record to its topic's shard writer. Cloneable; all clones feed the same set of
/// shard writers. The per-write hot path touches exactly ONE shard (its writer +
/// its metrics), so there is no global contention across shards.
#[derive(Clone)]
pub struct ShardedWalWriter {
    /// One [`WalWriter`] per shard.
    writers: Arc<Vec<WalWriter>>,
}

impl ShardedWalWriter {
    /// Number of shards.
    #[inline]
    pub fn shards(&self) -> usize {
        self.writers.len()
    }

    /// The writer for shard `s` (panics out of range — internal use).
    #[inline]
    fn writer(&self, s: usize) -> &WalWriter {
        &self.writers[s]
    }

    /// The writer for the shard `record` routes to (by its `topic_id`).
    #[inline]
    fn writer_for(&self, record: &WalRecord) -> &WalWriter {
        self.writer(shard_for_topic(record.topic_id(), self.writers.len()))
    }

    /// Submit `record` to its topic's shard with durability class `durable`. See
    /// [`WalWriter::submit`].
    pub fn submit(&self, record: WalRecord, durable: bool) -> Result<CommitToken, WalError> {
        let w = self.writer_for(&record);
        w.submit(record, durable)
    }

    /// Submit an atomic caller batch (one topic's records) to that topic's shard.
    /// Every record in a caller batch shares one `topic_id`, so the whole batch
    /// routes to a single shard and is accepted all-or-none on that shard's
    /// bounded channel (R5). `topic_id` selects the shard up front (the batch is
    /// non-empty for any real write; an empty batch routes to shard 0 and returns
    /// a pre-committed token, exactly like [`WalWriter::submit_batch`]).
    pub fn submit_batch(
        &self,
        topic_id: u64,
        records: Vec<WalRecord>,
        durable: bool,
    ) -> Result<CommitToken, WalError> {
        let s = shard_for_topic(topic_id, self.writers.len());
        self.writer(s).submit_batch(records, durable)
    }

    /// Submit and block until commit, in one call (routes by `record.topic_id()`).
    pub fn append(&self, record: WalRecord, durable: bool) -> Result<(), WalError> {
        self.submit(record, durable)?.wait().map(drop)
    }

    /// Write a durable `CheckpointMark` flush barrier to **every** shard and block
    /// until each one's group fsync returns, so after this call every shard's
    /// published position (`positions()`) covers all of that shard's prior
    /// committed frames (the snapshot consistency barrier, ARCHITECTURE §3). Fails
    /// (propagated) if any shard's barrier cannot be durably synced — the caller
    /// abandons the snapshot rather than record a position that races ahead of
    /// durability. Submits to all shards first, then waits, so the per-shard
    /// fsyncs run concurrently.
    pub fn checkpoint_barrier_all(&self, now: u64) -> Result<(), WalError> {
        let mut tokens = Vec::with_capacity(self.writers.len());
        for w in self.writers.iter() {
            let token = w.submit(
                WalRecord::CheckpointMark {
                    last_checkpoint_seq: 0,
                    ts: now,
                },
                true,
            )?;
            tokens.push(token);
        }
        for token in tokens {
            token.wait()?;
        }
        Ok(())
    }

    /// An **aggregated** snapshot of all shard metrics summed into one
    /// [`WalMetrics`] for the observability exporter (M3). Computed on demand (the
    /// metrics-scrape path, not the hot write path), so summing across shards adds
    /// no per-write contention. Counters are summed; the queue-depth/peak gauges
    /// are summed and the read-only flag is OR-ed (any shard read-only ⇒ 1).
    pub fn aggregated_metrics(&self) -> Arc<WalMetrics> {
        let agg = WalMetrics::default();
        for w in self.writers.iter() {
            let m = w.metrics();
            agg.fsyncs
                .fetch_add(m.fsyncs.load(Ordering::Relaxed), Ordering::Relaxed);
            agg.frames
                .fetch_add(m.frames.load(Ordering::Relaxed), Ordering::Relaxed);
            agg.batches
                .fetch_add(m.batches.load(Ordering::Relaxed), Ordering::Relaxed);
            agg.bytes_written
                .fetch_add(m.bytes_written.load(Ordering::Relaxed), Ordering::Relaxed);
            agg.rotations
                .fetch_add(m.rotations.load(Ordering::Relaxed), Ordering::Relaxed);
            agg.queued
                .fetch_add(m.queued.load(Ordering::Relaxed), Ordering::Relaxed);
            agg.queued_peak
                .fetch_add(m.queued_peak.load(Ordering::Relaxed), Ordering::Relaxed);
            agg.submit_full
                .fetch_add(m.submit_full.load(Ordering::Relaxed), Ordering::Relaxed);
            // Read-only is a sticky per-shard health flag; OR them (a stalled shard
            // surfaces as read-only without claiming the whole WAL is read-only).
            if m.read_only.load(Ordering::Relaxed) != 0 {
                agg.read_only.store(1, Ordering::Relaxed);
            }
            agg.fsync_count
                .fetch_add(m.fsync_count.load(Ordering::Relaxed), Ordering::Relaxed);
            agg.fsync_micros_total.fetch_add(
                m.fsync_micros_total.load(Ordering::Relaxed),
                Ordering::Relaxed,
            );
            for (i, b) in agg.fsync_buckets.iter().enumerate() {
                b.fetch_add(
                    m.fsync_buckets[i].load(Ordering::Relaxed),
                    Ordering::Relaxed,
                );
            }
        }
        // The active_idx/active_len gauges are per-shard checkpoint positions, not
        // meaningful to sum; leave them at their defaults in the aggregate (the
        // checkpoint path reads per-shard positions via `positions()`).
        Arc::new(agg)
    }

    /// The committed append position of every shard: `positions[s] = (active_idx,
    /// active_len)` for shard `s`. The snapshot checkpoint records all of these so
    /// recovery resumes each shard's WAL replay from exactly its own offset
    /// (ARCHITECTURE §3). Indexed by shard.
    pub fn positions(&self) -> Vec<(u64, u64)> {
        self.writers.iter().map(|w| w.position()).collect()
    }

    /// Each live shard's `(physical group key, (active_idx, active_len))`. The key
    /// is the relative dir under `wal/` (`""` flat, `shard-NN` sharded), so the
    /// snapshot checkpoint records each position against its PHYSICAL group identity
    /// — recovery then applies an offset only to the exact group it was measured
    /// against (no flat ↔ `shard-00/` conflation across a shard-count reconfigure).
    pub fn keyed_positions(&self) -> Vec<(String, (u64, u64))> {
        let n = self.writers.len();
        self.writers
            .iter()
            .enumerate()
            .map(|(s, w)| (shard_group_key(s, n), w.position()))
            .collect()
    }

    /// Total WAL bytes written across all shards (the snapshot size-trigger input).
    pub fn bytes_written(&self) -> u64 {
        self.writers
            .iter()
            .map(|w| w.metrics().bytes_written.load(Ordering::Relaxed))
            .fold(0u64, |a, x| a.saturating_add(x))
    }
}

/// The sharded WAL facade: owns the N shard [`Wal`]s (each owning its writer
/// thread) and exposes a [`ShardedWalWriter`]. Dropping this drains + fsyncs every
/// shard's queue and joins every writer thread (each shard's `Wal::Drop`), so no
/// committed batch is lost on teardown.
pub struct ShardedWal {
    shards: Vec<Wal>,
    writer: ShardedWalWriter,
}

impl ShardedWal {
    /// Number of shards.
    pub fn shards(&self) -> usize {
        self.shards.len()
    }

    /// Open `n` WAL shards under `cfg.dir`, each resuming at the per-shard
    /// `first_idx[s]` / `existing_len[s]` (recovery resumes each shard after its
    /// own truncated tail), routing all I/O through `fs`.
    ///
    /// `n == 1` uses the flat single-shard layout (no shard subdir). `n > 1` gives each shard its own
    /// `shard-<NN>/` subdir. `first_idx`/`existing_len` are indexed by shard and
    /// must have length `n` (recovery computes them per shard); a fresh start
    /// passes `(1, 0)` for every shard.
    pub fn open_at_with(
        fs: Arc<dyn Fs>,
        cfg: WalConfig,
        n: usize,
        first_idx: &[u64],
        existing_len: &[u64],
    ) -> Result<ShardedWal, WalError> {
        assert!(n >= 1, "wal shards must be >= 1");
        assert_eq!(first_idx.len(), n, "first_idx must be indexed by shard");
        assert_eq!(
            existing_len.len(),
            n,
            "existing_len must be indexed by shard"
        );

        let mut shards = Vec::with_capacity(n);
        let mut writers = Vec::with_capacity(n);
        for s in 0..n {
            let mut shard_cfg = cfg.clone();
            // n == 1 keeps the flat layout (None); n > 1 gives each shard a subdir.
            shard_cfg.shard_subdir = if n == 1 { None } else { Some(shard_subdir(s)) };
            let wal = Wal::open_at_with(fs.clone(), shard_cfg, first_idx[s], existing_len[s])?;
            writers.push(wal.writer());
            shards.push(wal);
        }
        Ok(ShardedWal {
            shards,
            writer: ShardedWalWriter {
                writers: Arc::new(writers),
            },
        })
    }

    /// A cloneable handle for submitting records (routes by topic_id → shard).
    pub fn writer(&self) -> ShardedWalWriter {
        self.writer.clone()
    }

    /// Stop every shard's writer thread (drain + final fsync + join), consuming
    /// `self`. Convenient for tests; the engine uses the `Drop` path.
    pub fn shutdown(self) {
        // Dropping each owned `Wal` drains + fsyncs + joins its writer thread.
        drop(self.shards);
    }
}

/// The directory beneath `<data_dir>/wal` that this `WalConfig` shard writes to
/// — kept here as the single source of truth for the per-shard path so recovery
/// and the writer agree. `PathBuf` so callers can `join("wal-<idx>.log")`.
pub fn shard_wal_dir(data_dir: &std::path::Path, s: usize, n: usize) -> PathBuf {
    let base = data_dir.join("wal");
    if n <= 1 {
        base
    } else {
        base.join(shard_subdir(s))
    }
}

// ===========================================================================
// Unit tests
// ===========================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::fs::{File, OpenOpts, RealFs};
    use crate::storage::WalReader;
    use std::io;
    use std::path::{Path, PathBuf};
    use std::sync::{Condvar, Mutex};
    use std::time::Duration;

    fn append(topic_id: u64, seq: u64) -> WalRecord {
        WalRecord::Append {
            topic_id,
            seq,
            ts: 1_700_000_000_000 + seq,
            node: None,
            tag: None,
            data: b"x".to_vec(),
        }
    }

    /// Routing is deterministic and stable for a given (topic_id, n).
    #[test]
    fn routing_is_deterministic_and_in_range() {
        for n in [1usize, 2, 4, 8, 16] {
            for id in 0..1000u64 {
                let s = shard_for_topic(id, n);
                assert!(s < n, "shard {s} in range for n={n}");
                assert_eq!(s, shard_for_topic(id, n), "stable");
            }
        }
        // n == 1 routes everything to shard 0.
        for id in 0..100u64 {
            assert_eq!(shard_for_topic(id, 1), 0);
        }
        // topic_id 0 (control frames) pins to shard 0.
        for n in [1usize, 2, 4, 8] {
            assert_eq!(shard_for_topic(0, n), 0);
        }
    }

    /// Across many topic ids the hash spreads topics over all shards (no shard is
    /// starved) — the property that makes sharding scale.
    #[test]
    fn routing_spreads_across_all_shards() {
        let n = 8usize;
        let mut counts = vec![0usize; n];
        for id in 1..=10_000u64 {
            counts[shard_for_topic(id, n)] += 1;
        }
        for (s, c) in counts.iter().enumerate() {
            assert!(*c > 0, "shard {s} received some topics (got {c})");
        }
    }

    /// A multi-shard WAL round-trips records to the right shard subdirs, and the
    /// frames replay from those subdirs.
    #[test]
    fn multi_shard_writes_route_to_subdirs_and_replay() {
        let dir = tempfile::tempdir().unwrap();
        let n = 4usize;
        let wal = ShardedWal::open_at_with(
            RealFs::arc(),
            WalConfig::new(dir.path()),
            n,
            &vec![1u64; n],
            &vec![0u64; n],
        )
        .unwrap();
        let w = wal.writer();
        // Write a handful of distinct topics; record which shard each routes to.
        let ids: Vec<u64> = (1..=12).collect();
        for &id in &ids {
            for seq in 1..=3u64 {
                w.append(append(id, seq), true).unwrap();
            }
        }
        wal.shutdown();

        // Each topic's Append frames appear in exactly its routed shard subdir.
        for &id in &ids {
            let expect = shard_for_topic(id, n);
            let mut found_in: Vec<usize> = Vec::new();
            for s in 0..n {
                let sub = dir.path().join("wal").join(shard_subdir(s));
                let mut files: Vec<PathBuf> = std::fs::read_dir(&sub)
                    .unwrap()
                    .map(|e| e.unwrap().path())
                    .collect();
                files.sort();
                let mut seen = false;
                for f in files {
                    for fr in WalReader::open(&f).unwrap() {
                        if matches!(&fr.record, WalRecord::Append { topic_id, .. } if *topic_id == id)
                        {
                            seen = true;
                        }
                    }
                }
                if seen {
                    found_in.push(s);
                }
            }
            assert_eq!(
                found_in,
                vec![expect],
                "topic {id} routed to shard {expect}"
            );
        }
    }

    /// `n == 1` uses the flat single-shard layout (no shard subdir).
    #[test]
    fn single_shard_is_flat_layout() {
        let dir = tempfile::tempdir().unwrap();
        let wal =
            ShardedWal::open_at_with(RealFs::arc(), WalConfig::new(dir.path()), 1, &[1], &[0])
                .unwrap();
        let w = wal.writer();
        w.append(append(7, 1), true).unwrap();
        wal.shutdown();
        let wal_dir = dir.path().join("wal");
        // Flat file present, no shard subdir.
        let flat = wal_dir.join(format!("wal-{:016}.log", 1));
        assert!(flat.exists(), "flat wal-<idx>.log exists for n=1");
        let has_sub = std::fs::read_dir(&wal_dir).unwrap().any(|e| {
            e.unwrap()
                .path()
                .file_name()
                .and_then(|x| x.to_str())
                .map(|x| x.starts_with("shard-"))
                .unwrap_or(false)
        });
        assert!(!has_sub, "n=1 creates no shard-NN subdir");
    }

    /// An `Fs` that blocks `sync_data` ONLY for files under a chosen shard subdir,
    /// so we can stall exactly one shard's writer and prove the OTHER shards keep
    /// committing — per-shard failure isolation.
    struct StallShardFs {
        inner: Arc<dyn crate::storage::Fs>,
        stalled_sub: String,
        gate: Arc<(Mutex<bool>, Condvar)>,
    }
    struct StallFile {
        inner: Box<dyn File>,
        stalled: bool,
        gate: Arc<(Mutex<bool>, Condvar)>,
    }
    impl File for StallFile {
        fn read_at(&self, off: u64, buf: &mut [u8]) -> io::Result<usize> {
            self.inner.read_at(off, buf)
        }
        fn write_at(&mut self, off: u64, buf: &[u8]) -> io::Result<usize> {
            self.inner.write_at(off, buf)
        }
        fn set_len(&mut self, len: u64) -> io::Result<()> {
            self.inner.set_len(len)
        }
        fn sync_data(&self) -> io::Result<()> {
            if self.stalled {
                let (lock, cv) = &*self.gate;
                let mut released = lock.lock().unwrap();
                while !*released {
                    released = cv.wait(released).unwrap();
                }
            }
            self.inner.sync_data()
        }
        fn sync_all(&self) -> io::Result<()> {
            self.inner.sync_all()
        }
        fn metadata_len(&self) -> io::Result<u64> {
            self.inner.metadata_len()
        }
    }
    impl crate::storage::Fs for StallShardFs {
        fn open(&self, path: &Path, opts: OpenOpts) -> io::Result<Box<dyn File>> {
            let stalled = path
                .components()
                .any(|c| c.as_os_str().to_str() == Some(self.stalled_sub.as_str()));
            Ok(Box::new(StallFile {
                inner: self.inner.open(path, opts)?,
                stalled,
                gate: self.gate.clone(),
            }))
        }
        fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
            self.inner.rename(from, to)
        }
        fn remove_file(&self, path: &Path) -> io::Result<()> {
            self.inner.remove_file(path)
        }
        fn read_dir(&self, dir: &Path) -> io::Result<Vec<PathBuf>> {
            self.inner.read_dir(dir)
        }
        fn sync_dir(&self, dir: &Path) -> io::Result<()> {
            self.inner.sync_dir(dir)
        }
        fn create_dir_all(&self, dir: &Path) -> io::Result<()> {
            self.inner.create_dir_all(dir)
        }
        fn exists(&self, path: &Path) -> bool {
            self.inner.exists(path)
        }
        fn metadata_len(&self, path: &Path) -> io::Result<u64> {
            self.inner.metadata_len(path)
        }
    }

    /// Per-shard failure isolation: stalling shard 0's fsync must NOT block a
    /// durable write to a topic that routes to a DIFFERENT shard. We pick two topic
    /// ids that route to different shards, stall the first's shard, and prove the
    /// second's durable `append` still returns while the first is parked in fsync.
    #[test]
    fn a_stalled_shard_does_not_block_others() {
        let n = 4usize;
        // Find two topic ids on different shards (one of them shard 0).
        let id_a = (1..1000u64)
            .find(|&id| shard_for_topic(id, n) == 0)
            .unwrap();
        let id_b = (1..1000u64)
            .find(|&id| shard_for_topic(id, n) != 0)
            .unwrap();
        let stalled_sub = shard_subdir(0);

        let dir = tempfile::tempdir().unwrap();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let fs: Arc<dyn crate::storage::Fs> = Arc::new(StallShardFs {
            inner: RealFs::arc(),
            stalled_sub,
            gate: gate.clone(),
        });
        let wal = ShardedWal::open_at_with(
            fs,
            WalConfig::new(dir.path()),
            n,
            &vec![1u64; n],
            &vec![0u64; n],
        )
        .unwrap();
        let w = wal.writer();

        // Park shard 0 in fsync: a durable submit to id_a whose token we never wait
        // on (the writer blocks in the gated sync_data).
        let _parked = w.submit(append(id_a, 1), true).unwrap();
        // Give the writer a moment to enter the gated fsync.
        std::thread::sleep(Duration::from_millis(50));

        // A durable write to id_b (a different shard) must complete WITHOUT waiting
        // on shard 0's stalled fsync. Do it on a thread with a timeout so a
        // regression (shared writer) is caught as a hang, not a deadlock.
        let w2 = w.clone();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let h = std::thread::spawn(move || {
            let r = w2.append(append(id_b, 1), true);
            let _ = done_tx.send(r);
        });
        let got = done_rx.recv_timeout(Duration::from_secs(5));
        assert!(
            matches!(got, Ok(Ok(()))),
            "a durable write to a healthy shard completed while another shard stalled"
        );
        h.join().unwrap();

        // Release the gate so shard 0 can drain + the WAL shuts down cleanly.
        {
            let (lock, cv) = &*gate;
            *lock.lock().unwrap() = true;
            cv.notify_all();
        }
        drop(_parked);
        wal.shutdown();
    }
}
