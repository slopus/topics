//! Phase-8B fault/crash batch — 5 `recovery-replay` boundary strategies from the
//! catalog (`/tmp/streams-fault-catalog.json`), each one test fn named after its
//! catalog id. Every test asserts the CORRECT crash-consistency behavior via the
//! durability contract, reusing the Phase-8A harness (`FakeDisk` / `FaultFs` from
//! `streams::storage::testfs`, the real WAL + recovery wired through
//! `Engine::with_data_dir_fs` and the `*_with` constructors, per
//! tests/crash_oracle.rs).
//!
//! Strategies (boundary: recovery-replay):
//!   - F-WAL-EIO-OPEN               — EIO opening the active WAL file on recovery ⇒
//!                                     recovery returns an io error; the engine does
//!                                     NOT silently start empty over real data.
//!   - F-WAL-MISDIRECTED-FRAME      — a valid-CRC frame carries a wrong (non-
//!                                     contiguous / duplicate) seq; replay's seq-skip
//!                                     + contiguity must not open a gap or fabricate.
//!   - F-WAL-EIO-MIDLOG-READ-RECOVERY — EIO reading a WAL frame during replay (not the
//!                                     tail) ⇒ recovery surfaces the io error
//!                                     deterministically; re-running converges.
//!   - F-SNAP-CHECKPOINT-AHEAD-OF-WAL — the snapshot's checkpoint (wal_idx,wal_offset)
//!                                     points past the actual WAL end (lost WAL
//!                                     writes) ⇒ recovery must not panic on an offset
//!                                     beyond EOF; state = the snapshot's durable set.
//!   - F-EVICT-FLOOR-MONOTONE-REPLAY — out-of-order EvictWatermark frames (an older
//!                                     floor follows a newer one) ⇒ replay only
//!                                     advances evict_floor if greater; never regresses.
//!
//! ```text
//! cargo test --features test-fs --test fault_recovery_b
//! ```

#![cfg(feature = "test-fs")]

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde_json::json;

use streams::clock::{SharedClock, TestClock};
use streams::config::ServerConfig;
use streams::engine::Engine;
use streams::storage::testfs::{FakeDisk, FaultFs, FaultKind, FaultOp};
use streams::storage::wal::encode_frame;
use streams::storage::{
    write_snapshot_with, BoxConfigOp, Checkpoint, File, Fs, OpenOpts, Snapshot, WalReader,
    WalRecord,
};
use streams::types::{BoxConfig, BoxType, DiffRequest, RecordIn, WriteRequest};

// ===========================================================================
// Shared plumbing (mirrors tests/crash_oracle.rs + tests/fault_wal_append_b.rs)
// ===========================================================================

const DATA_DIR: &str = "/data";

fn cfg() -> ServerConfig {
    ServerConfig {
        data_dir: Some(DATA_DIR.to_string()),
        ..Default::default()
    }
}

fn clock() -> SharedClock {
    Arc::new(TestClock::new(1_700_000_000_000))
}

/// Build a fresh durable engine whose WAL + snapshots live on `disk`.
fn open_engine(disk: &FakeDisk) -> Arc<Engine> {
    Engine::with_data_dir_fs(cfg(), clock(), disk.arc()).expect("engine opens through FakeDisk")
}

fn wal_dir() -> PathBuf {
    PathBuf::from(DATA_DIR).join("wal")
}

fn meta_dir() -> PathBuf {
    PathBuf::from(DATA_DIR).join("meta")
}

/// Make the WAL (and meta) directory names durable — the create+dir-fsync
/// production does at open time, modeled explicitly so files survive a crash.
fn sync_dirs(disk: &FakeDisk) {
    let fs = disk.arc();
    let _ = fs.sync_dir(&wal_dir());
    let _ = fs.sync_dir(&meta_dir());
}

/// A durable append frame for box id 1 at `seq`.
fn ap(seq: u64) -> WalRecord {
    WalRecord::Append {
        box_id: 1,
        seq,
        ts: 1_700_000_000_000 + seq,
        node: None,
        tag: Some("t".into()),
        data: format!("payload-{seq}").into_bytes(),
    }
}

/// Replay every `wal-*.log` file on the (post-crash) disk image, stopping each
/// file at its torn tail — exactly the recovery read path. Returns the dense
/// sequence of recovered Append seqs.
fn recover_seqs(disk: &FakeDisk) -> Vec<u64> {
    let fs = disk.arc();
    let mut files: Vec<PathBuf> = fs
        .read_dir(&wal_dir())
        .unwrap()
        .into_iter()
        .filter(|p| {
            p.file_name()
                .and_then(|s| s.to_str())
                .map(|n| n.starts_with("wal-") && n.ends_with(".log"))
                .unwrap_or(false)
        })
        .collect();
    files.sort();
    let mut seqs = Vec::new();
    for f in files {
        let r = WalReader::open_with(&fs, &f).unwrap();
        for frame in r {
            let s = frame.record.seq();
            if s > 0 {
                seqs.push(s);
            }
        }
    }
    seqs
}

/// Read the live record seqs of `name` through the engine's diff path (the same
/// bytes a consumer sees). Returns `(seqs, tombstone_reason)`.
fn dump_seqs(engine: &Engine, name: &str) -> (Vec<u64>, Option<String>) {
    let d = engine
        .diff(
            name,
            DiffRequest {
                from_seq: 0,
                limit: 1000,
                node: None,
                include_tags: true,
                include_meta: true,
                wait_ms: 0,
            },
        )
        .expect("diff after recovery");
    let seqs = d.records.iter().map(|r| r.seq).collect();
    let tomb = d.tombstone.map(|t| format!("{:?}", t.reason).to_lowercase());
    (seqs, tomb)
}

/// Overwrite a (durable) WAL file's bytes wholesale and fsync, so `bytes` is the
/// post-crash image recovery reads. Used to craft adversarial frames directly.
fn install_wal_bytes(disk: &FakeDisk, path: &Path, bytes: &[u8]) {
    let mut f = disk
        .open(path, OpenOpts::create_truncate())
        .expect("open WAL file rw");
    let mut off = 0usize;
    while off < bytes.len() {
        let n = f.write_at(off as u64, &bytes[off..]).expect("write_at");
        off += n;
    }
    f.sync_all().expect("fsync ⇒ durable");
    sync_dirs(disk);
}

/// A durable `WriteRequest` carrying one record with value `v`.
fn write_req(v: &str) -> WriteRequest {
    WriteRequest {
        records: vec![RecordIn {
            data: json!({ "v": v }),
            tag: None,
            node: None,
            meta: None,
        }],
        node: None,
        idempotency_key: None,
        create: Some(true),
        config: None,
        disable_backpressure: true,
    }
}

fn durable_box(name: &str, cap: u64) -> (&str, BoxConfig) {
    (
        name,
        BoxConfig {
            r#type: BoxType::Log,
            durable: true,
            cap_records: cap,
            ..Default::default()
        },
    )
}

// ===========================================================================
// A path-aware `Open` fault wrapper: fails `open` for a file whose name contains
// a needle (e.g. `wal-`), forwarding every other call to the inner `Fs`. Faults
// the first `n` matching opens (`n == u64::MAX` ⇒ a dead, always-failing file).
// FaultFs's `Open` fault fires on the Nth open of ANY path, which is brittle
// across the several opens recovery issues (dirs, snapshot, WAL); this wrapper
// targets exactly the WAL-file open recovery reads from.
//
// NB: recovery opens a `wal-*.log` more than once (a cheap framing pre-scan in
// `count_replay_frames`, the replay read in `WalReader::open_with`, then the
// resumed writer's `open_for_append`). A fail-ALWAYS models a persistently
// unreadable WAL file (the safety-critical case: recovery must not silently
// start empty over it).
// ===========================================================================
#[derive(Clone)]
struct OpenFailFs {
    inner: Arc<dyn Fs>,
    /// How many more matching opens to fail (saturating-decremented per match).
    remaining: Arc<std::sync::atomic::AtomicU64>,
    /// Substring a path's file name must contain to be eligible for the fault.
    needle: &'static str,
    err: io::ErrorKind,
}

impl OpenFailFs {
    /// Fail the first `n` opens whose file name contains `needle` (`u64::MAX` ⇒
    /// always fail a matching open — a dead file).
    fn new(inner: Arc<dyn Fs>, needle: &'static str, err: io::ErrorKind, n: u64) -> Self {
        OpenFailFs {
            inner,
            remaining: Arc::new(std::sync::atomic::AtomicU64::new(n)),
            needle,
            err,
        }
    }
    fn arc(&self) -> Arc<dyn Fs> {
        Arc::new(self.clone())
    }
}

impl Fs for OpenFailFs {
    fn open(&self, path: &Path, opts: OpenOpts) -> io::Result<Box<dyn File>> {
        let matches = path
            .file_name()
            .and_then(|s| s.to_str())
            .map(|n| n.contains(self.needle))
            .unwrap_or(false);
        if matches {
            let cur = self.remaining.load(std::sync::atomic::Ordering::SeqCst);
            if cur > 0 {
                // u64::MAX stays saturated (fail-always); a finite count decrements.
                if cur != u64::MAX {
                    self.remaining
                        .store(cur - 1, std::sync::atomic::Ordering::SeqCst);
                }
                return Err(io::Error::new(self.err, "injected open fault"));
            }
        }
        self.inner.open(path, opts)
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

// ===========================================================================
// F-WAL-EIO-OPEN  (io-error, sev critical)
// EIO opening the active WAL file on recovery (WalReader::open). The engine must
// NOT silently start empty (discarding durable data) — recovery surfaces the io
// error and refuses to start, so a retry/abort never overwrites real data with a
// fresh-start image.
// ===========================================================================
#[test]
fn f_wal_eio_open() {
    let disk = FakeDisk::new();

    // Phase 1: a durable acked workload on a healthy disk ⇒ real WAL data exists.
    {
        let engine = open_engine(&disk);
        let (n, c) = durable_box("jobs", 0);
        engine.put_box(n, c).unwrap();
        for i in 1..=3 {
            engine
                .write("jobs", write_req(&i.to_string()), true)
                .expect("durable append acked");
        }
        sync_dirs(&disk);
        drop(engine);
    }
    // Sanity: the WAL really holds the 3 frames the recovery would read.
    assert_eq!(recover_seqs(&disk), vec![1, 2, 3], "WAL holds real durable data");

    // Phase 2: reopen with EIO on EVERY open of a `wal-*.log` file (a persistently
    // unreadable WAL — the safety-critical case). Recovery reads the WAL via
    // WalReader::open_with; a dead open there must propagate so the engine REFUSES
    // to start rather than silently starting empty over the durable WAL.
    //
    // (A single-shot/transient open glitch is benign: recovery's `count_replay_
    // frames` pre-scan swallows it and the replay open then retries successfully —
    // full data recovers. The dangerous case asserted here is the persistent one.)
    let faulty = OpenFailFs::new(disk.arc(), "wal-", io::ErrorKind::Other, u64::MAX).arc();
    let res = Engine::with_data_dir_fs(cfg(), clock(), faulty);
    assert!(
        res.is_err(),
        "EIO opening the WAL file on recovery ⇒ the engine must refuse to start \
         (no silent fresh-start discarding durable data)"
    );

    // Phase 3: the durable data is untouched — a recovery through a HEALTHY disk
    // still surfaces all 3 acked frames (the failed open left no trace, never
    // truncated or fresh-started over the real WAL).
    let engine = open_engine(&disk);
    let (seqs, _tomb) = dump_seqs(&engine, "jobs");
    assert_eq!(
        seqs,
        vec![1, 2, 3],
        "after a failed recovery open, the durable data is intact (never discarded)"
    );
}

// ===========================================================================
// F-WAL-MISDIRECTED-FRAME  (corruption, sev high)
// A frame's bytes land at the wrong offset (right data, wrong place) producing a
// frame whose logged seq is NOT contiguous with the prior frames. We craft this
// directly: a valid prefix (box-create + appends seq 1..=3) followed by a single
// valid-CRC Append frame carrying a misdirected seq.
//
// Oracle: replay's seq-skip (`seq <= head` ⇒ skip) catches a duplicate/stale
// seq; for a future-but-non-contiguous seq, recovery must NOT insert a record at
// a non-contiguous seq creating a gap — survivors stay a dense prefix, nothing is
// fabricated at a fictitious seq.
//
// FIXED (Phase-8B fix): a misdirected frame whose logged seq is GREATER than head
// (e.g. head=3, frame seq=99) used to pass the `seq <= head` skip guard in
// recovery::replay_frame and reach apply_append_for_recovery, where `b.append(..)`
// assigns the next CONTIGUOUS seq (head+1=4) and the debug_assert_eq! fired
// (4 != 99) ⇒ a PANIC that aborted recovery. recovery::replay_frame now adds a
// contiguity guard (skip any frame with `seq != head + 1`), so a non-contiguous
// misdirected frame is treated as torn / ignored: never panic, never a gap, never
// adopting seq 99 as head.
// ===========================================================================

/// Build a raw WAL byte image: a BoxConfig create for box id 1 ("jobs") then the
/// given append seqs, plus a trailing custom-seq Append (the misdirected frame).
fn craft_wal_with_misdirected(append_seqs: &[u64], misdirected_seq: u64) -> Vec<u8> {
    let mut out = Vec::new();
    let mut frame = Vec::new();

    let cfg_bytes = serde_json::to_vec(&BoxConfig {
        r#type: BoxType::Log,
        durable: true,
        cap_records: 0,
        ..Default::default()
    })
    .unwrap();
    let create = WalRecord::BoxConfig {
        box_id: 1,
        op: BoxConfigOp {
            name: "jobs".to_string(),
            config: cfg_bytes,
        },
        tombstone: false,
        ts: 1_700_000_000_000,
    };
    encode_frame(&mut frame, &create, true);
    out.extend_from_slice(&frame);

    for &s in append_seqs {
        encode_frame(&mut frame, &ap(s), true);
        out.extend_from_slice(&frame);
    }
    // The misdirected frame: a perfectly valid CRC frame, but its logged seq is
    // wrong (a frame from elsewhere landed here). All bytes are genuine, so the
    // CRC passes — only the seq is non-contiguous.
    encode_frame(&mut frame, &ap(misdirected_seq), true);
    out.extend_from_slice(&frame);
    out
}

#[test]
fn f_wal_misdirected_frame() {
    // --- Case A: a DUPLICATE/stale seq (misdirected seq <= head). Replay's
    //     `seq <= head` skip drops it; survivors are the dense prefix, no
    //     resurrection, no double-insert. This is the well-behaved path. ---
    {
        let disk = FakeDisk::new();
        let path = wal_dir().join(format!("wal-{:016}.log", 1));
        disk.create_dir_all(&wal_dir()).unwrap();
        let bytes = craft_wal_with_misdirected(&[1, 2, 3], 2); // dup seq 2.
        install_wal_bytes(&disk, &path, &bytes);

        let engine = open_engine(&disk);
        let (seqs, _tomb) = dump_seqs(&engine, "jobs");
        assert_eq!(
            seqs,
            vec![1, 2, 3],
            "a misdirected DUPLICATE seq (<= head) is skipped by replay; survivors \
             stay a dense prefix, no double-insert, no gap"
        );
        let st = engine.box_state("jobs", false).unwrap();
        assert_eq!(st.head_seq, 3, "head reflects only the contiguous frames");
        drop(engine);
    }

    // --- Case B: a FUTURE, NON-CONTIGUOUS seq (misdirected seq > head, skipping
    //     ahead). The contract: recovery must NOT insert a record at a
    //     non-contiguous seq creating a gap. Either it is treated as torn / a
    //     non-contiguous frame is ignored, OR the engine re-densifies it at head+1
    //     — but never a hole at [head+1 .. misdirected_seq). The survivor set must
    //     be a dense prefix with no fabricated middle gap. ---
    {
        let disk = FakeDisk::new();
        let path = wal_dir().join(format!("wal-{:016}.log", 1));
        disk.create_dir_all(&wal_dir()).unwrap();
        // appends 1..=3 then a frame logged at seq 99 (a wildly non-contiguous,
        // misdirected seq). head after 1..=3 is 3.
        let bytes = craft_wal_with_misdirected(&[1, 2, 3], 99);
        install_wal_bytes(&disk, &path, &bytes);

        let engine = open_engine(&disk);
        let (seqs, _tomb) = dump_seqs(&engine, "jobs");

        // The survivor set must be DENSE — no hole punched between seq 3 and 99.
        if let (Some(&lo), Some(&hi)) = (seqs.first(), seqs.last()) {
            let expected: Vec<u64> = (lo..=hi).collect();
            assert_eq!(
                seqs, expected,
                "no non-contiguous gap may be created by a misdirected frame \
                 (survivors must be dense): {seqs:?}"
            );
        }
        // And the recovered head must never jump to the misdirected future seq 99
        // (that would leave seqs 4..=98 as a fabricated gap / phantom head).
        let st = engine.box_state("jobs", false).unwrap();
        assert!(
            st.head_seq < 99,
            "recovered head {} must not adopt the misdirected future seq 99 \
             (no phantom gap)",
            st.head_seq
        );
        drop(engine);
    }
}

// ===========================================================================
// F-WAL-EIO-MIDLOG-READ-RECOVERY  (compound, sev critical)
// EIO reading a WAL frame during replay (NOT the tail). WalReader::open_with
// reads the whole file with read_to_end_from (a read_at loop); an EIO there
// propagates up.
//
// The contract: recovery surfaces the io error / stops deterministically;
// re-running recovery converges; NO partial index is left committed as final
// state. We assert two regimes:
//   (a) a PERSISTENT read fault (dead device) ⇒ recovery must surface the error
//       and refuse to start (never commit a truncated/partial index), and the
//       durable WAL is left untouched so a healthy re-run recovers the full set;
//   (b) a TRANSIENT (single-shot) read glitch ⇒ recovery's framing pre-scan
//       (`count_replay_frames`, which tolerates an open/read error) absorbs it and
//       the replay read retries successfully, converging to the FULL durable state
//       — also a valid outcome (no partial index, no loss).
// ===========================================================================
#[test]
fn f_wal_eio_midlog_read_recovery() {
    let disk = FakeDisk::new();

    // Phase 1: a durable workload (several frames so a mid-log read fault lands on
    // a real frame, not the tail).
    {
        let engine = open_engine(&disk);
        let (n, c) = durable_box("jobs", 0);
        engine.put_box(n, c).unwrap();
        for i in 1..=6 {
            engine
                .write("jobs", write_req(&i.to_string()), true)
                .expect("durable append acked");
        }
        sync_dirs(&disk);
        drop(engine);
    }
    assert_eq!(recover_seqs(&disk), vec![1, 2, 3, 4, 5, 6], "WAL holds 6 frames");

    // (a) PERSISTENT read fault: every read_at EIOs (a dead device). Recovery must
    //     surface the io error rather than silently committing a partial index.
    {
        let faulty: Arc<dyn Fs> =
            FaultFs::new(disk.arc(), FaultOp::ReadAt, FaultKind::Eio, 0, false).arc();
        let res = Engine::with_data_dir_fs(cfg(), clock(), faulty);
        assert!(
            res.is_err(),
            "a persistent EIO reading the WAL during replay ⇒ recovery surfaces the \
             io error and refuses to start (no partial index committed as final state)"
        );
    }

    // The failed attempt wrote nothing (recovery is read-then-truncate; a read
    // error aborts before any truncate), so the durable WAL is intact: a healthy
    // re-run recovers the FULL set.
    {
        let engine = open_engine(&disk);
        let (seqs, _tomb) = dump_seqs(&engine, "jobs");
        assert_eq!(
            seqs,
            vec![1, 2, 3, 4, 5, 6],
            "after the EIO'd recovery the durable WAL is untouched; a healthy re-run \
             converges to the full state (no partial/truncated index persisted)"
        );
        drop(engine);
    }

    // (b) TRANSIENT single-shot read glitch: the pre-scan swallows it; the replay
    //     read retries and converges to the full durable state.
    {
        let glitch: Arc<dyn Fs> =
            FaultFs::new(disk.arc(), FaultOp::ReadAt, FaultKind::Eio, 0, true).arc();
        let engine = Engine::with_data_dir_fs(cfg(), clock(), glitch)
            .expect("a transient read glitch is absorbed; recovery converges");
        let (seqs, _t) = dump_seqs(&engine, "jobs");
        assert_eq!(
            seqs,
            vec![1, 2, 3, 4, 5, 6],
            "a transient read glitch converges to the full durable state (no loss, \
             no partial index): {seqs:?}"
        );
        drop(engine);
    }

    // Idempotent re-recovery: a second clean recovery is byte-identical.
    let engine = open_engine(&disk);
    let (seqs, _t) = dump_seqs(&engine, "jobs");
    drop(engine);
    let engine2 = open_engine(&disk);
    let (seqs2, _t2) = dump_seqs(&engine2, "jobs");
    assert_eq!(seqs, seqs2, "recovery is convergent/idempotent after the glitch");
}

// ===========================================================================
// F-SNAP-CHECKPOINT-AHEAD-OF-WAL  (corruption, sev critical)
// The snapshot's checkpoint (wal_idx, wal_offset) points PAST the actual WAL end
// (lost WAL writes after the snapshot's recorded position). Replay from the
// checkpoint finds fewer frames than the offset implies; recovery must NOT panic
// on an offset beyond EOF. The recovered state = the snapshot's materialized set
// (which was durable), with no gap and no resurrection.
// ===========================================================================
#[test]
fn f_snap_checkpoint_ahead_of_wal() {
    let disk = FakeDisk::new();

    // Phase 1: a durable workload + a snapshot capturing the materialized set. The
    // snapshot's checkpoint records (wal_idx, wal_offset) at capture time.
    {
        let engine = open_engine(&disk);
        let (n, c) = durable_box("jobs", 0);
        engine.put_box(n, c).unwrap();
        for i in 1..=4 {
            engine
                .write("jobs", write_req(&i.to_string()), true)
                .expect("durable append acked");
        }
        // Snapshot now: materializes seqs 1..=4 and records the WAL checkpoint.
        engine.write_snapshot().expect("snapshot written");
        sync_dirs(&disk);
        drop(engine);
    }

    // The latest snapshot's checkpoint position (the legitimate WAL end at capture).
    let snap = streams::storage::load_latest_with(&disk.arc(), Path::new(DATA_DIR))
        .expect("load snapshot")
        .expect("a snapshot exists");
    let real_ckpt = snap.checkpoint;

    // Phase 2: rewrite the snapshot with a checkpoint offset pushed FAR past the
    // actual WAL end (simulating lost WAL writes after the recorded position). The
    // materialized box set is unchanged (it was durable); only the replay boundary
    // now points beyond EOF.
    let mut bad = snap.clone();
    bad.checkpoint = Checkpoint {
        wal_idx: real_ckpt.wal_idx,
        wal_offset: real_ckpt.wal_offset + 1_000_000, // way past the file end.
        last_checkpoint_seq: real_ckpt.last_checkpoint_seq,
    };
    rewrite_latest_snapshot(&disk, &bad);

    // Phase 3: recovery must not panic on the beyond-EOF offset; it replays
    // whatever frames exist (here, none past the real end ⇒ zero post-checkpoint
    // frames) and presents the snapshot's durable materialized set.
    let engine = open_engine(&disk);
    let (seqs, _tomb) = dump_seqs(&engine, "jobs");
    assert_eq!(
        seqs,
        vec![1, 2, 3, 4],
        "checkpoint past EOF ⇒ recovery presents the snapshot's durable set; \
         no panic, no gap, no resurrection: {seqs:?}"
    );
    let st = engine.box_state("jobs", false).unwrap();
    assert_eq!(st.head_seq, 4, "head = snapshot's materialized head, not a phantom");
    drop(engine);

    // Idempotent: a second recovery over the same (checkpoint-ahead) image is
    // byte-identical.
    let engine2 = open_engine(&disk);
    let (seqs2, _t2) = dump_seqs(&engine2, "jobs");
    assert_eq!(seqs, seqs2, "checkpoint-ahead recovery is convergent");
}

/// Overwrite the latest `snapshot-<n>.bin` on the disk with `snap` (same id ⇒
/// same final name), routing through the real `write_snapshot_with` so the body
/// framing/crc is valid; then make the meta dir durable.
fn rewrite_latest_snapshot(disk: &FakeDisk, snap: &Snapshot) {
    write_snapshot_with(&disk.arc(), Path::new(DATA_DIR), snap).expect("rewrite snapshot");
    sync_dirs(disk);
}

// ===========================================================================
// F-EVICT-FLOOR-MONOTONE-REPLAY  (reorder, sev high)
// EvictWatermark frames replayed where an OLDER (lower) floor follows a NEWER
// (higher) one. Replay only advances evict_floor if greater (the
// `if evict_floor > floors.evict_floor` guard in recovery::replay_frame), so the
// floor settles at the NEWER value and never regresses to the older one.
//
// We craft a raw WAL: box-create, appends 1..=6, then two EvictWatermark frames
// in the adversarial (decreasing) order — high floor first, low floor second.
// After recovery the involuntary floor (observable via earliest_seq + a from-0
// tombstone) reflects the HIGH floor, not the low one.
// ===========================================================================

fn evict_frame(evict_floor: u64, earliest_seq: u64) -> WalRecord {
    WalRecord::EvictWatermark {
        box_id: 1,
        evict_floor,
        earliest_seq,
        ts: 1_700_000_000_000,
    }
}

/// Craft a raw WAL: box-create("jobs"), appends 1..=6, then `evicts` in order.
fn craft_wal_with_evicts(evicts: &[WalRecord]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut frame = Vec::new();

    let cfg_bytes = serde_json::to_vec(&BoxConfig {
        r#type: BoxType::Log,
        durable: true,
        cap_records: 0,
        ..Default::default()
    })
    .unwrap();
    let create = WalRecord::BoxConfig {
        box_id: 1,
        op: BoxConfigOp {
            name: "jobs".to_string(),
            config: cfg_bytes,
        },
        tombstone: false,
        ts: 1_700_000_000_000,
    };
    encode_frame(&mut frame, &create, true);
    out.extend_from_slice(&frame);

    for s in 1..=6u64 {
        encode_frame(&mut frame, &ap(s), true);
        out.extend_from_slice(&frame);
    }
    for ev in evicts {
        encode_frame(&mut frame, ev, true);
        out.extend_from_slice(&frame);
    }
    out
}

#[test]
fn f_evict_floor_monotone_replay() {
    // Evict-floor semantics (src/engine/eviction.rs): `evict_floor = F` evicts all
    // seqs <= F (earliest live seq = F+1). So floor 5 ⇒ live {6}; floor 2 ⇒ live
    // {3,4,5,6}. The monotone guard (`if evict_floor > floors.evict_floor`) must
    // keep the floor at the NEWER (higher) value 5 regardless of replay order.

    // Out-of-order: a HIGH floor (5) replayed FIRST, then an OLDER LOW floor (2)
    // replayed second. The guard must keep the floor at 5 (NOT regress to 2).
    let disk = FakeDisk::new();
    let path = wal_dir().join(format!("wal-{:016}.log", 1));
    disk.create_dir_all(&wal_dir()).unwrap();
    let bytes = craft_wal_with_evicts(&[
        evict_frame(5, 6), // newer: floor 5 ⇒ seqs 1..=5 evicted, live {6}.
        evict_frame(2, 3), // older: floor 2 — must NOT regress the floor.
    ]);
    install_wal_bytes(&disk, &path, &bytes);

    let engine = open_engine(&disk);
    let (seqs, tomb) = dump_seqs(&engine, "jobs");

    // The floor settled at 5 (the newer/higher value): only seq 6 is live. A
    // regression to floor 2 would have left {3,4,5,6} live — the bug this guards.
    assert_eq!(
        seqs,
        vec![6],
        "evict_floor settled at the NEWER floor 5 (live={{6}}); a regression to the \
         older floor 2 would have surfaced {{3,4,5,6}}: live={seqs:?}"
    );
    // earliest_seq = evict_floor + 1 = 6, reflecting the monotone (non-regressing)
    // floor, never the older floor 2 (which would give earliest_seq 3).
    let st = engine.box_state("jobs", false).unwrap();
    assert_eq!(
        st.earliest_seq, 6,
        "earliest_seq = evict_floor+1 = 6 (the monotone, non-regressing floor)"
    );
    // An involuntary cap/TTL floor surfaces a tombstone on a from-0 read (cap loss
    // is never silent); the older floor 2 never un-tombstones the 5-floor window.
    assert_eq!(
        tomb.as_deref(),
        Some("cap"),
        "an involuntary evict floor tombstones a from-0 read after recovery"
    );

    drop(engine);

    // Convergence: the already-monotone (low-then-high) order yields the SAME
    // floor — low(2)-then-high(5) also settles at 5 ⇒ live {6}.
    let disk2 = FakeDisk::new();
    let path2 = wal_dir().join(format!("wal-{:016}.log", 1));
    disk2.create_dir_all(&wal_dir()).unwrap();
    let bytes2 = craft_wal_with_evicts(&[evict_frame(2, 3), evict_frame(5, 6)]);
    install_wal_bytes(&disk2, &path2, &bytes2);
    let engine2 = open_engine(&disk2);
    let (seqs2, _t2) = dump_seqs(&engine2, "jobs");
    assert_eq!(
        seqs2, seqs,
        "monotone replay is order-independent: low-then-high settles at the same \
         floor as high-then-low (both ⇒ live {{6}})"
    );
}
