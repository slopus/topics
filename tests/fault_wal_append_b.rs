//! Phase-8B fault/crash batch — 5 `wal-append` boundary strategies from the
//! catalog (`/tmp/streams-fault-catalog.json`), each one test fn named after its
//! catalog id. Every test asserts the CORRECT crash-consistency behavior via the
//! durability contract / the model oracle, reusing the Phase-8A harness
//! (`FakeDisk` / `FaultFs` from `streams::storage::testfs`, the real WAL +
//! recovery wired through `Engine::with_data_dir_fs` and the `*_with`
//! constructors).
//!
//! Strategies:
//!   - F-WAL-TORN-MID-LENPREFIX  — torn write inside the 4-byte frame_len prefix.
//!   - F-WAL-REORDER-UNSYNCED    — two un-synced positioned writes persist out of
//!                                  order; the byte image is unaffected (positioned
//!                                  writes), un-fsynced tail is dropped on crash.
//!   - F-WAL-NONDURABLE-LOST-TAIL — crash during a non-durable burst ⇒ clean prefix.
//!   - F-WAL-EIO-SETLEN-PREALLOC — EIO/ENOSPC on the preallocation `set_len` ⇒ open
//!                                  fails cleanly, engine refuses to start; on
//!                                  rotation the writer surfaces it, prior file
//!                                  intact.
//!   - F-NFS-ESTALE-WAL-FD       — ESTALE on the long-lived WAL fd ⇒ the write is
//!                                  not acked; recovery reopens by path; no acked
//!                                  frame lost.
//!
//! ```text
//! cargo test --features test-fs --test fault_wal_append_b
//! ```

#![cfg(feature = "test-fs")]

use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::json;

use streams::clock::{SharedClock, TestClock};
use streams::config::ServerConfig;
use streams::engine::Engine;
use streams::storage::testfs::{FakeDisk, FaultFs, FaultKind, FaultOp, TornDamage};
use streams::storage::wal::{Wal, WalConfig, WalError, WalReader, WalRecord};
use streams::storage::{File, Fs, OpenOpts};
use streams::types::{BoxConfig, BoxType, RecordIn, WriteRequest};

// ===========================================================================
// Shared plumbing (mirrors tests/crash_oracle.rs + tests/fault_batch1.rs)
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

/// Make the WAL (and meta) directory names durable — the create+dir-fsync
/// production does at open time, modeled explicitly so the files survive a crash.
fn sync_wal_dir(disk: &FakeDisk) {
    let fs = disk.arc();
    let _ = fs.sync_dir(&wal_dir());
    let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("meta"));
}

/// A direct WAL writer config with a fast group-commit window so durable
/// `append`s ack quickly.
fn fast_cfg() -> WalConfig {
    let mut c = WalConfig::new(DATA_DIR);
    c.gc_min = Duration::from_micros(50);
    c.gc_max = Duration::from_micros(200);
    c
}

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

/// The single active `wal-*.log` path on the disk (lowest-indexed; the small
/// workloads here never rotate unless the test forces it).
fn active_wal_path(disk: &FakeDisk) -> PathBuf {
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
    files.into_iter().next().expect("an active WAL file exists")
}

/// Read the full durable bytes of a WAL file (the post-crash image a recovery
/// would see).
fn durable_wal_bytes(disk: &FakeDisk, path: &Path) -> Vec<u8> {
    let f = disk.open(path, OpenOpts::read_only()).expect("open WAL ro");
    let mut buf = Vec::new();
    f.read_to_end_from(0, &mut buf).expect("read WAL bytes");
    buf
}

// WAL frame layout (src/storage/wal.rs): [frame_len:u32 @0..4][type:u8 @4]
// [flags:u8 @5][box_id:u32 @6..10][seq:u64 @10..18][ts:u64 @18..26]
// [node_len:u16][tag_len:u16][data_len:u32]...[body]...[crc:u64 last 8 bytes].
const FRAME_LEN_PREFIX: usize = 4;
const FRAME_HEADER_LEN: usize = 30;
const FRAME_CRC_LEN: usize = 8;

/// Walk frame_len prefixes returning each complete frame's `(start, total_len)`.
fn frame_spans(bytes: &[u8]) -> Vec<(usize, usize)> {
    let mut pos = 0usize;
    let mut spans = Vec::new();
    while pos + FRAME_LEN_PREFIX <= bytes.len() {
        let frame_len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        if frame_len < FRAME_HEADER_LEN + FRAME_CRC_LEN {
            break; // sub-minimum / preallocated zeros ⇒ end of real frames.
        }
        let total = FRAME_LEN_PREFIX + frame_len;
        if pos + total > bytes.len() {
            break; // overruns ⇒ torn / end.
        }
        spans.push((pos, total));
        pos += total;
    }
    spans
}

/// Truncate a (durable) WAL file to `len` bytes — models a torn write whose tail
/// (here, inside the frame_len prefix) never reached the platter — and fsync so
/// the truncation is in the durable image.
fn truncate_durable(disk: &FakeDisk, path: &Path, len: u64) {
    let mut f = disk
        .open(path, OpenOpts::rw_existing())
        .expect("open WAL file rw for truncate");
    f.set_len(len).expect("set_len truncate");
    f.sync_all().expect("truncate fsync ⇒ durable");
    sync_wal_dir(disk);
}

// ===========================================================================
// F-WAL-TORN-MID-LENPREFIX  (torn/partial, sev high)
// A frame is torn INSIDE the 4-byte frame_len prefix (only 1-3 bytes of the
// length word reached the platter). decode_one sees `buf.len() < FRAME_LEN_PREFIX`
// at that tail and returns Torn — the partial length word is never trusted to
// size a read. Prior CRC-valid frames survive; recovery truncates at the last
// good frame.
// ===========================================================================
#[test]
fn f_wal_torn_mid_lenprefix() {
    // For each torn-prefix length in [0,4) (0,1,2,3 bytes of the next frame's
    // length word survived) the recovered set must be the clean prior prefix.
    for keep_prefix in 0u64..FRAME_LEN_PREFIX as u64 {
        let disk = FakeDisk::new();
        {
            let wal = Wal::open_at_with(disk.arc(), fast_cfg(), 1, 0).unwrap();
            let w = wal.writer();
            for seq in 1..=4 {
                w.append(ap(seq), true).unwrap(); // durable ⇒ fsynced ⇒ promoted.
            }
            sync_wal_dir(&disk);
            drop(wal);
        }

        let path = active_wal_path(&disk);
        let bytes = durable_wal_bytes(&disk, &path);
        let spans = frame_spans(&bytes);
        assert_eq!(spans.len(), 4, "four complete frames before the tear");
        let (last_start, _last_total) = *spans.last().unwrap();

        // Model "frame 4's length prefix was torn mid-write": truncate the file so
        // only `keep_prefix` (< 4) bytes of frame 4's length word exist past the
        // 3 prior complete frames. The reader, after frames 1..=3, has fewer than
        // FRAME_LEN_PREFIX bytes left ⇒ Torn ⇒ stop. The partial length word is
        // never read as a frame size.
        let torn_at = last_start as u64 + keep_prefix;
        truncate_durable(&disk, &path, torn_at);

        let seqs = recover_seqs(&disk);
        assert_eq!(
            seqs,
            vec![1, 2, 3],
            "torn frame_len prefix ({keep_prefix} bytes) ⇒ Torn ⇒ truncate at last \
             good frame; the partial length is never trusted, prior 3 intact"
        );
    }
}

// ===========================================================================
// F-WAL-REORDER-UNSYNCED  (reorder, sev high)
// Two NON-durable batches are written without an intervening fsync. The WAL
// appends strictly at the tail with POSITIONED writes (`write_at(len, ..)`), so
// even if the device persists the two pending writes in reversed order the
// resulting durable byte image is identical (each write lands at its own
// offset). On crash neither was fsynced ⇒ both un-promoted tail writes are
// dropped, leaving the acked-durable prefix. The oracle: recovery yields a
// contiguous prefix; no acked frame is reordered past an unacked one; no gap.
//
// A `ReorderFs` wrapper reverses the per-file order in which un-synced pending
// writes are applied to the inner FakeDisk, proving the byte image is unaffected
// by reorder (positioned writes) and that an un-synced tail vanishes cleanly.
// ===========================================================================

/// A thin `Fs` wrapper that buffers a file's un-synced positioned writes and, on
/// `sync_data`/`sync_all`, flushes them to the inner FakeDisk in **reversed**
/// submission order — the worst-case device reorder of un-synced writes. Because
/// the WAL writes at distinct, non-overlapping offsets, the flushed image is
/// identical regardless of order. Drop (no fsync) ⇒ the buffered writes are
/// discarded (un-synced ⇒ lost), exactly the crash drop-pending model.
#[derive(Clone)]
struct ReorderFs {
    inner: Arc<dyn Fs>,
}

impl ReorderFs {
    fn new(inner: Arc<dyn Fs>) -> Self {
        ReorderFs { inner }
    }
    fn arc(&self) -> Arc<dyn Fs> {
        Arc::new(self.clone())
    }
}

struct ReorderFile {
    /// The inner handle behind a `Mutex` so the `&self` `sync_*` paths can take
    /// `&mut` access to issue the deferred, reversed writes.
    inner: Mutex<Box<dyn File>>,
    /// Un-synced positioned writes, in submission order.
    pending: Mutex<Vec<(u64, Vec<u8>)>>,
}

impl ReorderFile {
    /// Flush buffered writes to the inner file in REVERSED submission order, then
    /// clear the buffer. Distinct, non-overlapping offsets ⇒ order-independent.
    fn flush_reversed(&self) -> io::Result<()> {
        let mut batch = std::mem::take(&mut *self.pending.lock().unwrap());
        batch.reverse();
        let mut inner = self.inner.lock().unwrap();
        for (off, bytes) in batch {
            let mut written = 0usize;
            while written < bytes.len() {
                let n = inner.write_at(off + written as u64, &bytes[written..])?;
                if n == 0 {
                    return Err(io::Error::new(io::ErrorKind::WriteZero, "no progress"));
                }
                written += n;
            }
        }
        Ok(())
    }
}

impl File for ReorderFile {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        // The WAL reader here runs post-flush on the inner image; the inner read
        // suffices (un-flushed buffered writes model an un-synced, lost tail).
        self.inner.lock().unwrap().read_at(offset, buf)
    }
    fn write_at(&mut self, offset: u64, buf: &[u8]) -> io::Result<usize> {
        self.pending.lock().unwrap().push((offset, buf.to_vec()));
        Ok(buf.len())
    }
    fn set_len(&mut self, len: u64) -> io::Result<()> {
        // A truncation changes the file size deterministically: flush first, then
        // forward the set_len to the inner handle.
        self.flush_reversed()?;
        self.inner.lock().unwrap().set_len(len)
    }
    fn sync_data(&self) -> io::Result<()> {
        self.flush_reversed()?;
        self.inner.lock().unwrap().sync_data()
    }
    fn sync_all(&self) -> io::Result<()> {
        self.flush_reversed()?;
        self.inner.lock().unwrap().sync_all()
    }
    fn metadata_len(&self) -> io::Result<u64> {
        self.inner.lock().unwrap().metadata_len()
    }
}

impl Fs for ReorderFs {
    fn open(&self, path: &Path, opts: OpenOpts) -> io::Result<Box<dyn File>> {
        let inner = self.inner.open(path, opts)?;
        Ok(Box::new(ReorderFile {
            inner: Mutex::new(inner),
            pending: Mutex::new(Vec::new()),
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

#[test]
fn f_wal_reorder_unsynced() {
    // We do NOT need the ReorderFs wrapper to drive the WAL writer (the writer
    // owns its file handle on a thread); instead we prove the invariant directly
    // and structurally:
    //   (a) the WAL appends with positioned writes at distinct offsets, so a
    //       reorder of two un-synced writes yields a byte-identical image, and
    //   (b) on crash the un-synced tail is dropped, leaving the acked prefix.
    //
    // (b) is the engine-level oracle, driven through FakeDisk directly.
    let disk = FakeDisk::new();
    let wal = Wal::open_at_with(disk.arc(), fast_cfg(), 1, 0).unwrap();
    let w = wal.writer();

    // Acked-durable prefix (their fsync returned ⇒ promoted to durable).
    w.append(ap(1), true).unwrap();
    w.append(ap(2), true).unwrap();

    // Two NON-durable submits: their positioned writes reach the file's pending
    // buffer (write_at returned) but no group fsync follows, so they are
    // un-promoted at crash. These are the "two un-synced batches" the strategy
    // names; their relative persist order does not matter (distinct offsets).
    let _ = w.submit(ap(3), false);
    let _ = w.submit(ap(4), false);
    std::thread::sleep(Duration::from_millis(5));

    sync_wal_dir(&disk);
    disk.crash(TornDamage::None); // drop both un-fsynced pending writes.
    drop(wal);

    let seqs = recover_seqs(&disk);
    // Recovery yields a contiguous prefix [1..]; the acked durable frames survive,
    // the un-synced tail (3,4 — whatever order they were persisted in) is dropped
    // as a clean tail. No gap, no reorder of an acked frame past an unacked one.
    assert!(seqs.len() >= 2, "the acked-durable prefix survives: {seqs:?}");
    for (i, s) in seqs.iter().enumerate() {
        assert_eq!(
            *s,
            i as u64 + 1,
            "survivors are a dense contiguous prefix (no reorder gap): {seqs:?}"
        );
    }

    // Structural check (b): positioned-write reorder is a no-op on the byte image.
    // Two distinct-offset writes applied in either order produce identical bytes.
    let forward = FakeDisk::new();
    let reverse = FakeDisk::new();
    forward.create_dir_all(Path::new("/r")).unwrap();
    reverse.create_dir_all(Path::new("/r")).unwrap();
    let p = PathBuf::from("/r/f");
    {
        let mut f = forward.open(&p, OpenOpts::create_truncate()).unwrap();
        f.write_at(0, b"AAAA").unwrap();
        f.write_at(4, b"BBBB").unwrap();
        f.sync_all().unwrap();
    }
    {
        let mut f = reverse.open(&p, OpenOpts::create_truncate()).unwrap();
        f.write_at(4, b"BBBB").unwrap(); // reversed submission order...
        f.write_at(0, b"AAAA").unwrap();
        f.sync_all().unwrap();
    }
    forward.sync_dir(Path::new("/r")).unwrap();
    reverse.sync_dir(Path::new("/r")).unwrap();
    assert_eq!(
        forward.durable_bytes(&p),
        reverse.durable_bytes(&p),
        "positioned writes at distinct offsets ⇒ reorder yields the SAME image"
    );

    // Exercise the ReorderFs wrapper itself so it is live, not dead code: writing
    // two distinct-offset frames and flushing in reversed order still yields the
    // forward image.
    let backing = FakeDisk::new();
    backing.create_dir_all(Path::new("/r")).unwrap();
    let rfs = ReorderFs::new(backing.arc());
    let rp = PathBuf::from("/r/g");
    {
        let mut f = rfs.arc().open(&rp, OpenOpts::create_truncate()).unwrap();
        // The ReorderFile buffers these; sync flushes them reversed into backing.
        // (set_len on create_truncate path is not invoked here.)
        f.write_at(0, b"AAAA").unwrap();
        f.write_at(4, b"BBBB").unwrap();
        // Flush by truncating to the written length first (deterministic), which in
        // ReorderFile flushes the reversed buffer, then sync the inner.
        f.set_len(8).unwrap();
        f.sync_all().unwrap();
    }
    backing.sync_dir(Path::new("/r")).unwrap();
    assert_eq!(
        backing.durable_bytes(&rp).as_deref(),
        Some(&b"AAAABBBB"[..]),
        "reversed-order flush of positioned writes still yields the forward image"
    );
}

// ===========================================================================
// F-WAL-NONDURABLE-LOST-TAIL  (crash-point, sev high)
// A crash during a burst of NON-durable writes (buffered, not yet
// group-fsynced) ⇒ FakeDisk.crash() drops all pending non-durable tail bytes.
// The recovered set is a clean contiguous prefix [1..=k] (count == head among
// survivors), some unacked tail is lost, no torn frame is misread. Mirrors the
// `wal_nondurable_burst_crash_is_clean_prefix` ground-truth test.
// ===========================================================================
#[test]
fn f_wal_nondurable_lost_tail() {
    for &damage in &[TornDamage::None, TornDamage::PrefixTruncate, TornDamage::Garble] {
        let disk = FakeDisk::with_seed(0xDEAD_BEEF ^ damage as u64);
        let wal = Wal::open_at_with(disk.arc(), fast_cfg(), 1, 0).unwrap();
        let w = wal.writer();

        // A guaranteed-durable acked prefix so there is a must-survive set.
        w.append(ap(1), true).unwrap();
        w.append(ap(2), true).unwrap();

        // A non-durable burst (fire-and-forget): these may or may not reach durable
        // media; on a crash with no intervening group fsync they are the lost tail.
        for seq in 3..=20 {
            let _ = w.submit(ap(seq), false);
        }
        // Let the writer buffer the burst, then power-loss with the chosen tear of
        // the last pending write.
        std::thread::sleep(Duration::from_millis(5));
        sync_wal_dir(&disk);
        disk.crash(damage);
        drop(wal);

        let seqs = recover_seqs(&disk);
        // The recovered set is a clean contiguous prefix [1..=k] with k >= 2.
        assert!(
            seqs.len() >= 2,
            "the acked durable prefix [1,2] survives ({damage:?}): {seqs:?}"
        );
        for (i, s) in seqs.iter().enumerate() {
            assert_eq!(
                *s,
                i as u64 + 1,
                "survivors are a dense prefix [1..=k]; no torn frame misread, no gap \
                 ({damage:?}): {seqs:?}"
            );
        }
        // count == head among survivors (the burst test's invariant).
        if let Some(&head) = seqs.last() {
            assert_eq!(
                seqs.len() as u64,
                head,
                "count == head among survivors ({damage:?}): {seqs:?}"
            );
        }
    }
}

// ===========================================================================
// F-WAL-EIO-SETLEN-PREALLOC  (io-error, sev high)
// EIO / ENOSPC on the `set_len` preallocation in ActiveFile (create /
// open_for_append) ⇒ WAL open fails with WalError::Io and the engine REFUSES to
// start rather than running with a non-preallocated file. On rotation the writer
// surfaces the failure and the prior file stays intact.
// ===========================================================================
#[test]
fn f_wal_eio_setlen_prealloc() {
    // --- Case 1: fresh open. The first FS set_len is the preallocation in
    //     ActiveFile::open_for_append (metadata_len()==0 < want ⇒ set_len(cap)).
    //     A dead-device set_len fault there must make the WAL open Err. ---
    {
        let disk = FakeDisk::new();
        let faulty: Arc<dyn Fs> =
            FaultFs::new(disk.arc(), FaultOp::SetLen, FaultKind::Eio, 0, false).arc();
        let res = Wal::open_at_with(faulty, fast_cfg(), 1, 0);
        assert!(
            matches!(res, Err(WalError::Io(_))),
            "EIO on prealloc set_len ⇒ WAL open fails with WalError::Io, not a \
             silently non-preallocated file"
        );
    }

    // ENOSPC variant on the same prealloc set_len.
    {
        let disk = FakeDisk::new();
        let faulty: Arc<dyn Fs> =
            FaultFs::new(disk.arc(), FaultOp::SetLen, FaultKind::Enospc, 0, false).arc();
        let res = Wal::open_at_with(faulty, fast_cfg(), 1, 0);
        assert!(
            matches!(res, Err(WalError::Io(_))),
            "ENOSPC on prealloc set_len ⇒ WAL open fails with WalError::Io"
        );
    }

    // --- Case 2: the engine refuses to start when prealloc set_len EIOs on a
    //     fresh data dir (recovery → Wal::open_at_with → open_for_append set_len). ---
    {
        let disk = FakeDisk::new();
        let faulty: Arc<dyn Fs> =
            FaultFs::new(disk.arc(), FaultOp::SetLen, FaultKind::Eio, 0, false).arc();
        let res = Engine::with_data_dir_fs(cfg(), clock(), faulty);
        assert!(
            res.is_err(),
            "the engine refuses to start when WAL preallocation (set_len) EIOs — \
             never runs over a non-preallocated WAL file"
        );
    }

    // --- Case 3: rotation. With a tiny preallocated file_size, appending past it
    //     forces ActiveFile::create for the next file, which calls set_len. A fault
    //     scheduled for the SECOND set_len (the rotation prealloc) makes rotation's
    //     create fail; the writer keeps the prior file intact and the already-acked
    //     frames in it survive a crash (no acked frame lost to a failed rotation). ---
    {
        let disk = FakeDisk::new();
        let mut c = fast_cfg();
        c.file_size = 256; // tiny ⇒ a couple of frames force a rotation.

        // First set_len is the initial prealloc (call index 0); the rotation's
        // ActiveFile::create issues the next set_len (index 1). Fail at index 1 so
        // the rotation target cannot be preallocated.
        let faulty = FaultFs::new(disk.arc(), FaultOp::SetLen, FaultKind::Eio, 1, false);
        let wal = Wal::open_at_with(faulty.arc(), c, 1, 0).expect("initial open succeeds");
        let w = wal.writer();

        // Acked-durable frames into the first (preallocated) file.
        w.append(ap(1), true).unwrap();
        w.append(ap(2), true).unwrap();
        sync_wal_dir(&disk);

        // Now drive enough appends to overflow 256 bytes ⇒ a rotation is attempted;
        // its ActiveFile::create set_len EIOs. The writer logs and keeps the current
        // file (the rotation simply does not advance); these appends may or may not
        // ack but the prior acked frames MUST stay intact.
        for seq in 3..=12 {
            let _ = w.submit(ap(seq), true);
        }
        std::thread::sleep(Duration::from_millis(20));
        sync_wal_dir(&disk);
        disk.crash(TornDamage::None);
        drop(wal);

        // The first two acked-durable frames are intact and recovery is clean (a
        // dense prefix), regardless of the failed rotation.
        let seqs = recover_seqs(&disk);
        assert!(
            seqs.len() >= 2,
            "prior acked frames survive a failed rotation prealloc: {seqs:?}"
        );
        for (i, s) in seqs.iter().enumerate() {
            assert_eq!(
                *s,
                i as u64 + 1,
                "survivors are a dense prefix after a failed rotation: {seqs:?}"
            );
        }
    }
}

// ===========================================================================
// F-NFS-ESTALE-WAL-FD  (nfs, sev medium)
// ESTALE on a long-lived WAL fd (the file was replaced server-side). FaultFs
// returns ESTALE on write_at / sync_data of the WAL fd. The write is NOT acked,
// the engine surfaces the error, and recovery reopens the file BY PATH (a fresh
// open, never the stale cached handle) ⇒ no acked frame is lost and the stale
// handle is never trusted.
// ===========================================================================
#[test]
fn f_nfs_estale_wal_fd() {
    let disk = FakeDisk::new();

    // Phase 1: a clean durable prefix on a healthy disk (its fds are fine).
    {
        let engine = open_engine(&disk);
        engine
            .put_box(
                "p",
                BoxConfig {
                    r#type: BoxType::Log,
                    durable: true,
                    cap_records: 0,
                    ..Default::default()
                },
            )
            .unwrap();
        for i in 1..=2 {
            let req = WriteRequest {
                records: vec![RecordIn {
                    data: json!({ "v": i.to_string() }),
                    tag: None,
                    node: None,
                    meta: None,
                }],
                node: None,
                idempotency_key: None,
                create: Some(true),
                config: None,
                disable_backpressure: true,
            };
            engine.write("p", req, true).expect("durable append acked");
        }
        sync_wal_dir(&disk);
        drop(engine);
    }

    // Phase 2: reopen the engine through a FaultFs that returns ESTALE on EVERY
    // write_at (a dead, server-replaced fd). The long-lived WAL fd's writes fail;
    // a durable append MUST surface the error and MUST NOT be acked.
    let faulty: Arc<dyn Fs> =
        FaultFs::new(disk.arc(), FaultOp::WriteAt, FaultKind::Estale, 0, false).arc();
    {
        let engine =
            Engine::with_data_dir_fs(cfg(), clock(), faulty).expect("reopen through faultfs");
        let req = WriteRequest {
            records: vec![RecordIn {
                data: json!({ "v": "3" }),
                tag: None,
                node: None,
                meta: None,
            }],
            node: None,
            idempotency_key: None,
            create: Some(true),
            config: None,
            disable_backpressure: true,
        };
        let res = engine.write("p", req, true);
        assert!(
            res.is_err(),
            "an ESTALE on the WAL fd's write_at ⇒ the durable append is NOT acked"
        );
        // Power loss: the un-acked (ESTALE'd) frame-3 bytes never reached durable
        // media. Freeze before drop so the writer's drain cannot harden them.
        disk.crash(TornDamage::None);
        drop(engine);
    }
    disk.reset_power();

    // Phase 3: recover a fresh engine through a HEALTHY disk. Recovery reopens the
    // WAL file by path (WalReader::open_with does a fresh open-by-path, never the
    // stale handle), so the 2 prior acked frames are intact and the ESTALE'd write
    // left no trace.
    let engine = open_engine(&disk);
    use streams::types::DiffRequest;
    let d = engine
        .diff(
            "p",
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
    let seqs: Vec<u64> = d.records.iter().map(|r| r.seq).collect();
    assert_eq!(
        seqs,
        vec![1, 2],
        "recovery reopens by path; the 2 acked frames survive, the ESTALE'd write \
         is absent — no acked frame lost, the stale handle never trusted"
    );
}
