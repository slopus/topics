//! Phase-8B fault catalog — **segment-write boundary**, group B.
//!
//! Three fault/crash strategies that exercise the segment `.data`/`.idx` integrity
//! contract at the storage seam (DESIGN invariant 6 "corruption explicit, never
//! served"; invariant 8 ".idx never references .data bytes that aren't durably
//! present"). Each test drives the REAL segment decoders ([`decode_data_frame`] /
//! [`idx_entry_at`] / [`lookup`]) and a real [`LocalSegmentStore`] through the
//! Phase-8A harness ([`FakeDisk`] / [`FaultFs`] from `streams::storage::testfs`),
//! asserting the CORRECT crash-consistency behavior. Bounded, fixed-seed,
//! single-segment workloads keep the file well under a second.
//!
//! ```text
//! cargo test --features test-fs --test fault_seg_write_b
//! ```
//!
//! Strategies implemented (see docs/FAULT_TESTING.md / the catalog):
//!   F-METADATA-VS-DATA-REORDER  file-size metadata persists before the data
//!       blocks for a segment (or vice versa). FakeDisk persists the `set_len`
//!       (length) ahead of the `write_at` content on crash, so the `.data` reports
//!       its full length but its tail bytes are a lost (zero-filled) region.
//!       Oracle: the `.idx` never serves a `.data` byte range that isn't durably
//!       present — the in-bounds prefix decodes correctly, but a record whose frame
//!       falls in the non-durable tail decodes as `SegmentError::Corrupt` (never a
//!       fabricated record from zero/garbage bytes); the engine falls back to the
//!       WAL for that range. Validating the `.idx`-vs-durable-`.data` boundary is
//!       exactly "never trust idx past durable data end".
//!   F-MONITOR-PAGE-CHECKSUM     an always-on page-checksum FS shim (a cksumvfs
//!       analog) appends + verifies an 8-byte XXH3 checksum per page on every
//!       write/read, catching a silent byte change between write and read at the FS
//!       layer — isolating relocation/copy bit-rot from the engine's own logic. We
//!       relocate a segment hot→cold through the shim, inject a silent flip in the
//!       at-rest cold bytes, and assert the page-checksum shim catches it on read
//!       (independent of, and in addition to, the engine's per-frame XXH3).
//!   F-FUZZ-SEGMENT-DECODER      arbitrary garbage fed to `decode_data_frame` /
//!       `idx_entry_at` / `lookup`. Oracle: the decoders never panic / OOM / read
//!       out of bounds — a corrupt/short frame is `SegmentError::Corrupt`, an idx
//!       lookup past the end is `None`. Driven by a deterministic in-test PRNG over
//!       random buffers, random-length truncations of valid frames, and single-byte
//!       mutations of valid frames/indexes (an `arbitrary`-style structured sweep).

#![cfg(feature = "test-fs")]

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use streams::storage::testfs::{FakeDisk, TornDamage};
use streams::storage::{
    data_name, decode_data_frame, idx_entry_at, idx_name, idx_len, lookup, File, Fs,
    LocalSegmentStore, OpenOpts, SegmentBuilder, SegmentError, SegmentId, SegmentPart,
    SegmentRecord, SegmentStore, IDX_STRIDE,
};

const ROOT: &str = "/seg";
const COLD_ROOT: &str = "/cold";

// ===========================================================================
// Shared plumbing (mirrors tests/fault_seg_idx_a.rs)
// ===========================================================================

/// Build a small, contiguous single-segment `(data, idx)` pair starting at
/// `start` with `n` records. Deterministic bytes so torn/garble damage is
/// reproducible across runs.
fn build_segment(start: u64, n: u64) -> (Vec<u8>, Vec<u8>) {
    let mut b = SegmentBuilder::new(start);
    for i in 0..n {
        b.push(&SegmentRecord {
            seq: start + i,
            ts: 1_700_000_000_000 + i,
            node: (i % 2 == 0).then(|| format!("n{i}")),
            tag: Some(format!("t{i}")),
            data: format!("{{\"v\":{i},\"pad\":\"xxxxxxxxxxxxxxxx\"}}").into_bytes(),
        });
    }
    b.finish()
}

/// Write `bytes` fully (looping over short writes) to `path` through `fs` into the
/// pending image; the caller decides when/whether to fsync + dir-fsync.
fn write_pending(fs: &Arc<dyn Fs>, path: &Path, bytes: &[u8]) {
    let mut f = fs.open(path, OpenOpts::create_truncate()).unwrap();
    let mut written = 0usize;
    while written < bytes.len() {
        let n = f
            .write_at(written as u64, &bytes[written..])
            .expect("pending write");
        written += n;
    }
}

/// `seg-<id>.data` / `seg-<id>.idx` paths under `root`.
fn data_path(root: &str, id: SegmentId) -> PathBuf {
    PathBuf::from(root).join(data_name(id))
}
fn idx_path(root: &str, id: SegmentId) -> PathBuf {
    PathBuf::from(root).join(idx_name(id))
}

/// Durably install `bytes` as a final file at `final_path` via the production
/// recipe (tmp create → write → fsync bytes → dir-fsync tmp name → rename →
/// dir-fsync final name) so it survives a `crash()`.
fn install_durable(fs: &Arc<dyn Fs>, root: &str, final_path: &Path, bytes: &[u8]) {
    let tmp = final_path.with_extension({
        let cur = final_path.extension().and_then(|s| s.to_str()).unwrap_or("");
        format!("{cur}.tmp")
    });
    write_pending(fs, &tmp, bytes);
    fs.open(&tmp, OpenOpts::rw_existing())
        .unwrap()
        .sync_all()
        .unwrap();
    fs.sync_dir(Path::new(root)).unwrap();
    fs.rename(&tmp, final_path).unwrap();
    fs.sync_dir(Path::new(root)).unwrap();
}

/// Open a [`LocalSegmentStore`] over `disk` rooted at `root` (post-crash view).
fn open_store(disk: &FakeDisk, root: &str) -> LocalSegmentStore {
    LocalSegmentStore::open_with(root, disk.arc()).expect("store opens through FakeDisk")
}

// ===========================================================================
// F-METADATA-VS-DATA-REORDER
//
// File-size metadata reaches the platter before the data blocks for a segment.
// We model it with FakeDisk's two-layer durability: a `set_len` to the FULL
// segment length is fsynced (the length metadata is durable), while the tail
// CONTENT writes for the last records stay pending and are dropped on crash —
// so on recovery the `.data` reports its full length but the tail region reads
// back as zeros (a lost write that metadata already accounts for).
//
// Oracle: the `.idx` never serves a `.data` byte range that isn't durably
// present. The records whose frames lie wholly in the durable prefix decode
// correctly; a record whose frame falls in the lost (zeroed) tail decodes as
// SegmentError::Corrupt (zeros ⇒ frame_len below minimum / CRC mismatch), never
// a fabricated record — recovery rebuilds that range from the WAL instead.
// ===========================================================================

#[test]
fn f_metadata_vs_data_reorder() {
    let disk = FakeDisk::with_seed(0x_DA7A_DA7A);
    let fs = disk.arc();
    fs.create_dir_all(Path::new(ROOT)).unwrap();
    let id: SegmentId = 1;
    let n = 5u64;
    let (full_data, idx) = build_segment(id, n);

    // The boundary in the .data where the lost tail begins: the offset of the 4th
    // record's frame (records 0..=2 are in the durable prefix; 3..=4 are lost).
    let split = lookup(&idx, id, id + 3).expect("entry for the 4th record").offset as usize;
    assert!(split > 0 && split < full_data.len(), "split inside the .data");

    // Install the durable .data PREFIX (records 0..=2) at the final name, the
    // normal way (write → fsync → rename → dir-fsync). This is the content that
    // actually reached the platter.
    install_durable(&fs, ROOT, &data_path(ROOT, id), &full_data[..split]);

    // Now the reorder: the file-size METADATA jumps ahead of the data blocks. We
    // extend the durable .data to the FULL segment length via a fsynced set_len
    // (the length metadata is durable), while the tail CONTENT writes [split..]
    // stay pending and are dropped on the crash — exactly "size metadata persists
    // before the data blocks". The lost tail reads back as zeros.
    {
        let mut f = fs.open(&data_path(ROOT, id), OpenOpts::rw_existing()).unwrap();
        // 1. Length metadata reaches the platter (durable) ...
        f.set_len(full_data.len() as u64).unwrap();
        f.sync_all().unwrap();
        // 2. ... but the tail data blocks are only pending (lost on crash).
        let mut w = split;
        while w < full_data.len() {
            w += f.write_at(w as u64, &full_data[w..]).unwrap();
        }
        // NO fsync of the tail content ⇒ it is dropped on crash.
    }

    // The .idx is the FULL, durable index (all 5 entries) — the metadata twin that
    // outran the data.
    install_durable(&fs, ROOT, &idx_path(ROOT, id), &idx);

    disk.crash(TornDamage::None);
    disk.reset_power();

    let store = open_store(&disk, ROOT);
    assert_eq!(store.list().unwrap(), vec![id], "segment lists (has .data)");

    // The .data reports its FULL length (the metadata is durable) even though the
    // tail content is lost — the precise metadata-vs-data reorder window.
    let data_len = store.len(id, SegmentPart::Data).unwrap();
    assert_eq!(
        data_len,
        full_data.len() as u64,
        "file-size metadata is durable at the full length (it outran the data)"
    );

    // Read the durable bytes back: the prefix is intact, the tail is zeros (the
    // lost write the metadata already accounted for).
    let on_disk = store.read_all(id, SegmentPart::Data).unwrap();
    assert_eq!(on_disk.len(), full_data.len());
    assert_eq!(&on_disk[..split], &full_data[..split], "durable prefix intact");
    assert!(
        on_disk[split..].iter().all(|&b| b == 0),
        "the lost tail (metadata ahead of data) reads back as zeros"
    );

    let idx_buf = store.read_all(id, SegmentPart::Idx).unwrap();
    assert_eq!(idx_len(&idx_buf), n as usize, "the .idx outran the data: full");

    // ORACLE 1: the records wholly inside the DURABLE PREFIX read + decode exactly
    // — the in-bounds, truly-present bytes are served correctly.
    for s in id..id + 3 {
        let e = lookup(&idx_buf, id, s).expect("entry present");
        assert!(
            e.offset as u64 + e.len as u64 <= split as u64,
            "record {s} is wholly in the durable prefix"
        );
        let frame = store
            .read_range(id, SegmentPart::Data, e.offset as u64, e.len as u64)
            .expect("in-range frame reads");
        let rec = decode_data_frame(&frame).expect("durable-prefix frame decodes");
        assert_eq!(rec.seq, s, "decoded the right record");
    }

    // ORACLE 2: the records whose `.idx` entry points into the LOST (zeroed) tail
    // are within the file's (metadata) length, so `read_range` returns the bytes
    // — but those bytes are NOT durably present: the frame is all-zeros and
    // `decode_data_frame` MUST surface SegmentError::Corrupt (a zeroed frame_len is
    // below the minimum / the CRC cannot match), never fabricating a record. The
    // engine never trusts the `.idx` past the durable data end; it falls back to
    // the WAL for these seqs.
    for s in id + 3..id + n {
        let e = lookup(&idx_buf, id, s).expect("entry present in the full .idx");
        assert!(
            e.offset as u64 >= split as u64,
            "record {s}'s frame starts in the lost tail"
        );
        // read_range succeeds (within the metadata length) but the bytes are zeros.
        let frame = store
            .read_range(id, SegmentPart::Data, e.offset as u64, e.len as u64)
            .expect("range is within the (durable-metadata) file length");
        assert!(
            frame.iter().all(|&b| b == 0),
            "the frame for seq {s} is the lost zero-filled tail"
        );
        match decode_data_frame(&frame) {
            Err(SegmentError::Corrupt(_)) => {}
            other => panic!(
                "a frame in the lost tail must decode as Corrupt (never trust idx \
                 past durable data end), got {other:?} for seq {s}"
            ),
        }
    }
}

// ===========================================================================
// F-MONITOR-PAGE-CHECKSUM
//
// An always-on page-checksum FS shim (cksumvfs analog) that appends an 8-byte
// XXH3 checksum per logical page on write and verifies it on read. It catches a
// silent byte change between write and read at the FS layer — independent of the
// engine's own per-frame XXH3 — isolating relocation/copy bit-rot from engine
// logic. We relocate a segment hot→cold through the shim, inject a silent flip in
// the cold bytes at rest, and assert BOTH the page-checksum shim (FS layer) AND
// the engine's segment decoder (logic layer) catch it. A clean copy round-trips.
// ===========================================================================

/// A logical page size for the checksum shim. Each `PAGE`-byte page of file data
/// is stored alongside an 8-byte XXH3 trailer, so a silent flip in the data is
/// detected on read by recomputing the trailer.
const PAGE: usize = 256;
const CK: usize = 8;

/// The shared backing store of a [`PageCksumFs`]: every file is a list of pages,
/// each page = up to `PAGE` data bytes + an 8-byte XXH3 checksum. A "silent flip"
/// helper mutates one data byte WITHOUT updating the trailer, modelling bit-rot.
#[derive(Default)]
struct PageStore {
    files: BTreeMap<PathBuf, Vec<u8>>, // path -> raw page-framed bytes
    /// Exact logical (data) byte length per file — the page framing rounds the
    /// physical size up to a whole page, so the true length is tracked separately.
    lens: BTreeMap<PathBuf, u64>,
    /// Number of page-checksum mismatches the shim has caught (for assertions).
    caught: u64,
}

#[derive(Clone)]
struct PageCksumFs {
    inner: Arc<Mutex<PageStore>>,
}

impl PageCksumFs {
    fn new() -> Self {
        PageCksumFs {
            inner: Arc::new(Mutex::new(PageStore::default())),
        }
    }
    fn arc(&self) -> Arc<dyn Fs> {
        Arc::new(self.clone())
    }
    fn caught(&self) -> u64 {
        self.inner.lock().unwrap().caught
    }
    /// Silently flip one data byte of `path` at logical data `offset` WITHOUT
    /// updating that page's checksum trailer — models at-rest bit-rot the shim
    /// must catch on the next read.
    fn silent_flip(&self, path: &Path, offset: usize) {
        let mut st = self.inner.lock().unwrap();
        let framed = st.files.get_mut(path).expect("file exists");
        let page = offset / PAGE;
        let in_page = offset % PAGE;
        let raw = page * (PAGE + CK) + in_page;
        framed[raw] ^= 0xFF; // flip data, leave the trailer stale.
    }
}

/// Number of (PAGE+CK) frames needed to hold `data_len` data bytes.
fn page_frames(data_len: usize) -> usize {
    data_len.div_ceil(PAGE).max(1)
}

/// One open handle into a [`PageCksumFs`]. Reads verify each touched page's XXH3
/// trailer (an error + a `caught` tick on mismatch); writes recompute it. All
/// state (bytes + exact logical length) lives in the shared [`PageStore`].
struct PageCksumFile {
    fs: PageCksumFs,
    path: PathBuf,
}

impl File for PageCksumFile {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let mut st = self.fs.inner.lock().unwrap();
        let len = st.lens.get(&self.path).copied().unwrap_or(0);
        if offset >= len {
            return Ok(0);
        }
        let framed = st.files.get(&self.path).cloned().unwrap_or_default();
        let mut n = 0usize;
        while n < buf.len() && offset + (n as u64) < len {
            let o = (offset + n as u64) as usize;
            let page = o / PAGE;
            let rp = page * (PAGE + CK);
            if rp + PAGE + CK > framed.len() {
                break;
            }
            let data = &framed[rp..rp + PAGE];
            let stored =
                u64::from_le_bytes(framed[rp + PAGE..rp + PAGE + CK].try_into().unwrap());
            if stored != xxhash_rust::xxh3::xxh3_64(data) {
                st.caught += 1;
                return Err(io::Error::other(format!(
                    "page-checksum mismatch at page {page} of a relocated/copied file \
                     (silent bit-rot caught at the FS layer)"
                )));
            }
            buf[n] = data[o % PAGE];
            n += 1;
        }
        Ok(n)
    }

    fn write_at(&mut self, offset: u64, buf: &[u8]) -> io::Result<usize> {
        let mut st = self.fs.inner.lock().unwrap();
        let end = offset as usize + buf.len();
        let frames = page_frames(end.max(1));
        let framed = st.files.entry(self.path.clone()).or_default();
        if framed.len() < frames * (PAGE + CK) {
            framed.resize(frames * (PAGE + CK), 0);
        }
        for (i, &b) in buf.iter().enumerate() {
            let o = offset as usize + i;
            let page = o / PAGE;
            framed[page * (PAGE + CK) + (o % PAGE)] = b;
        }
        // Recompute the checksum trailer of every page this write touched.
        let first = offset as usize / PAGE;
        let last = (end - 1) / PAGE;
        for p in first..=last {
            let rp = p * (PAGE + CK);
            let ck = xxhash_rust::xxh3::xxh3_64(&framed[rp..rp + PAGE]);
            framed[rp + PAGE..rp + PAGE + CK].copy_from_slice(&ck.to_le_bytes());
        }
        let cur = st.lens.get(&self.path).copied().unwrap_or(0);
        st.lens.insert(self.path.clone(), cur.max(end as u64));
        Ok(buf.len())
    }

    fn set_len(&mut self, len: u64) -> io::Result<()> {
        let mut st = self.fs.inner.lock().unwrap();
        let frames = page_frames(len as usize);
        let framed = st.files.entry(self.path.clone()).or_default();
        framed.resize(frames * (PAGE + CK), 0);
        for p in 0..frames {
            let rp = p * (PAGE + CK);
            let ck = xxhash_rust::xxh3::xxh3_64(&framed[rp..rp + PAGE]);
            framed[rp + PAGE..rp + PAGE + CK].copy_from_slice(&ck.to_le_bytes());
        }
        st.lens.insert(self.path.clone(), len);
        Ok(())
    }

    fn sync_data(&self) -> io::Result<()> {
        Ok(())
    }
    fn sync_all(&self) -> io::Result<()> {
        Ok(())
    }
    fn metadata_len(&self) -> io::Result<u64> {
        Ok(self
            .fs
            .inner
            .lock()
            .unwrap()
            .lens
            .get(&self.path)
            .copied()
            .unwrap_or(0))
    }
}

impl Fs for PageCksumFs {
    fn open(&self, path: &Path, opts: OpenOpts) -> io::Result<Box<dyn File>> {
        let mut st = self.inner.lock().unwrap();
        let exists = st.files.contains_key(path);
        if !exists && !opts.create {
            return Err(io::Error::new(io::ErrorKind::NotFound, "no such file"));
        }
        if opts.truncate || !exists {
            st.files.insert(path.to_path_buf(), Vec::new());
            st.lens.insert(path.to_path_buf(), 0);
        }
        Ok(Box::new(PageCksumFile {
            fs: self.clone(),
            path: path.to_path_buf(),
        }))
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        let mut st = self.inner.lock().unwrap();
        let Some(bytes) = st.files.remove(from) else {
            return Err(io::Error::new(io::ErrorKind::NotFound, "rename src missing"));
        };
        let len = st.lens.remove(from).unwrap_or(0);
        st.files.insert(to.to_path_buf(), bytes);
        st.lens.insert(to.to_path_buf(), len);
        Ok(())
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        let mut st = self.inner.lock().unwrap();
        if st.files.remove(path).is_none() {
            return Err(io::Error::new(io::ErrorKind::NotFound, "no such file"));
        }
        st.lens.remove(path);
        Ok(())
    }

    fn read_dir(&self, dir: &Path) -> io::Result<Vec<PathBuf>> {
        let st = self.inner.lock().unwrap();
        Ok(st
            .files
            .keys()
            .filter(|p| p.parent() == Some(dir))
            .cloned()
            .collect())
    }

    fn sync_dir(&self, _dir: &Path) -> io::Result<()> {
        Ok(())
    }
    fn create_dir_all(&self, _dir: &Path) -> io::Result<()> {
        Ok(())
    }
    fn exists(&self, path: &Path) -> bool {
        self.inner.lock().unwrap().files.contains_key(path)
    }
    fn metadata_len(&self, path: &Path) -> io::Result<u64> {
        let st = self.inner.lock().unwrap();
        if st.files.contains_key(path) {
            Ok(st.lens.get(path).copied().unwrap_or(0))
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, "no such file"))
        }
    }
}

#[test]
fn f_monitor_page_checksum() {
    // Two segment stores sharing one page-checksum FS: a HOT store and a COLD
    // store. A relocation copies the segment hot→cold; the shim checksums every
    // page on write and verifies on read.
    let pfs = PageCksumFs::new();
    let fs = pfs.arc();
    let id: SegmentId = 1;
    let (data, idx) = build_segment(id, 6);

    let hot = LocalSegmentStore::open_with(ROOT, fs.clone()).unwrap();
    let cold = LocalSegmentStore::open_with(COLD_ROOT, fs.clone()).unwrap();

    // Seal into HOT (the page-checksum shim trails every page).
    hot.put(id, &data, &idx).unwrap();

    // --- Clean relocation round-trips through the shim -----------------------
    // Copy hot→cold exactly as the relocator would (read both parts, put to cold).
    let hot_data = hot.read_all(id, SegmentPart::Data).unwrap();
    let hot_idx = hot.read_all(id, SegmentPart::Idx).unwrap();
    assert_eq!(hot_data, data, "the shim round-trips the .data on a clean copy");
    assert_eq!(hot_idx, idx, "the shim round-trips the .idx on a clean copy");
    cold.put(id, &hot_data, &hot_idx).unwrap();
    // A clean copy is readable from cold and decodes (no bit-rot ⇒ no catch).
    let cold_data = cold.read_all(id, SegmentPart::Data).unwrap();
    assert_eq!(cold_data, data, "clean cold copy matches the source");
    assert_eq!(pfs.caught(), 0, "no page-checksum mismatch on a clean relocation");

    // --- Silent bit-rot in the at-rest COLD copy is caught -------------------
    // Flip one data byte of the cold .data WITHOUT updating its page checksum
    // trailer — the classic silent corruption a naive copy would propagate.
    let cold_data_path = data_path(COLD_ROOT, id);
    let flip_at = data.len() / 2;
    pfs.silent_flip(&cold_data_path, flip_at);

    // ORACLE 1 (FS layer): the page-checksum shim catches the silent flip on the
    // very next read — independent of any engine logic. read_all surfaces an io
    // error and the `caught` counter ticks.
    let err = cold.read_all(id, SegmentPart::Data);
    assert!(
        err.is_err(),
        "the page-checksum shim must surface the silent bit-rot as an io error"
    );
    assert_eq!(
        pfs.caught(),
        1,
        "the page-checksum shim caught exactly the one injected silent flip"
    );

    // ORACLE 2 (logic layer, belt-and-suspenders): even if the FS shim were absent
    // (a real disk that doesn't checksum), the engine's OWN per-frame XXH3 catches
    // the same flip — decoding the corrupted frame yields SegmentError::Corrupt,
    // never a served record. We prove this directly on the corrupted bytes: read
    // the raw flipped data out-of-band and decode the affected frame.
    let mut corrupted = data.clone();
    corrupted[flip_at] ^= 0xFF; // the same silent flip, in a plain buffer.
    // Locate the frame covering `flip_at` and decode it: XXH3 must reject it.
    let mut hit_corrupt = false;
    for s in id..id + 6 {
        let e = lookup(&idx, id, s).unwrap();
        let (lo, hi) = (e.offset as usize, e.offset as usize + e.len as usize);
        if (lo..hi).contains(&flip_at) {
            let frame = &corrupted[lo..hi];
            assert!(
                matches!(decode_data_frame(frame), Err(SegmentError::Corrupt(_))),
                "the engine's own XXH3 also catches the flip (corruption explicit)"
            );
            hit_corrupt = true;
        } else {
            // Every untouched frame still decodes fine (the corruption is isolated).
            let frame = &corrupted[lo..hi];
            assert!(
                decode_data_frame(frame).is_ok(),
                "an untouched frame still decodes (corruption is local)"
            );
        }
    }
    assert!(hit_corrupt, "the flip landed inside a decodable frame");

    // The HOT copy was never touched — it still reads + decodes cleanly (the bit-rot
    // was isolated to the cold-at-rest copy; resolve would prefer the good copy).
    let hot_again = hot.read_all(id, SegmentPart::Data).unwrap();
    assert_eq!(hot_again, data, "the untouched hot copy is still good");
    assert_eq!(
        pfs.caught(),
        1,
        "reading the good hot copy adds no false-positive catch"
    );
}

// ===========================================================================
// F-FUZZ-SEGMENT-DECODER
//
// Arbitrary garbage fed to the segment decoders. Oracle: decode_data_frame /
// idx_entry_at / lookup NEVER panic / OOM / read OOB — a corrupt/short frame is
// SegmentError::Corrupt, an idx lookup past the end is None. Driven by a
// deterministic in-test PRNG (xorshift) over: (a) fully-random buffers of random
// length; (b) random-length truncations of a valid frame; (c) single-byte
// mutations of a valid frame; (d) random/garbled `.idx` buffers fed to
// idx_entry_at / idx_len / lookup. Bounded iteration count keeps it fast.
// ===========================================================================

/// A tiny deterministic xorshift64* PRNG (no external rng dep; reproducible).
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed | 1)
    }
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: usize) -> usize {
        if n == 0 {
            0
        } else {
            (self.next_u64() % n as u64) as usize
        }
    }
}

/// One valid `.data` frame plus its `.idx`, to mutate/truncate in the fuzz loop.
fn valid_frame() -> (Vec<u8>, Vec<u8>) {
    let mut b = SegmentBuilder::new(7);
    b.push(&SegmentRecord {
        seq: 7,
        ts: 1_700_000_000_123,
        node: Some("node-A".into()),
        tag: Some("tenant:job".into()),
        data: b"{\"payload\":\"hello world\",\"k\":42}".to_vec(),
    });
    b.finish()
}

#[test]
fn f_fuzz_segment_decoder() {
    let (frame, idx) = valid_frame();
    // Sanity: the seed frame/index are themselves valid (so mutations are
    // meaningful departures from a good frame).
    assert!(decode_data_frame(&frame).is_ok(), "seed frame is valid");
    assert!(idx_entry_at(&idx, 0).is_some(), "seed idx entry 0 present");

    let mut rng = Rng::new(0xF0_F1_F2_F3_F4_F5_F6_F7);
    const ITERS: usize = 4000;

    for iter in 0..ITERS {
        // (a) A fully-random buffer of random length [0, 512). The decoder must
        //     return Ok(record) or Err(Corrupt) — never panic / OOB.
        {
            let len = rng.below(512);
            let mut buf = vec![0u8; len];
            for b in &mut buf {
                *b = rng.next_u64() as u8;
            }
            // The only contract: it returns (no panic). Whatever it returns is fine.
            let _ = decode_data_frame(&buf);
            // A garbage buffer fed to the idx decoders must also be panic-free; an
            // entry past the end is a clean None.
            let entry_idx = rng.below(64);
            let _ = idx_entry_at(&buf, entry_idx);
            let _ = idx_len(&buf);
            let _ = lookup(&buf, rng.next_u64(), rng.next_u64());
        }

        // (b) A random-length TRUNCATION of the valid frame — every short prefix
        //     must be rejected as Corrupt (frame_len overrun / below-min), never a
        //     partial record, never a panic.
        {
            let keep = rng.below(frame.len() + 1);
            let truncated = &frame[..keep];
            match decode_data_frame(truncated) {
                Ok(rec) => {
                    // The ONLY length that may decode is the whole, untouched frame.
                    assert_eq!(
                        keep,
                        frame.len(),
                        "only the full frame may decode; a truncation decoded a \
                         record at iter {iter} (keep={keep}): {rec:?}"
                    );
                }
                Err(SegmentError::Corrupt(_)) => {}
                Err(other) => panic!("unexpected error on a truncated frame: {other:?}"),
            }
        }

        // (c) A single-byte MUTATION of the valid frame. Flipping any byte must
        //     either still decode (only if it lands somewhere the CRC happens to
        //     still cover identically — vanishingly unlikely, but we don't require
        //     a catch) — the hard contract is NO PANIC and, if it errors, it errors
        //     as Corrupt. A mutation in the length prefix / header that overruns is
        //     Corrupt; a payload/CRC mutation is a CRC mismatch ⇒ Corrupt.
        {
            let mut m = frame.clone();
            let pos = rng.below(m.len());
            let xor = (rng.next_u64() as u8) | 1; // non-zero ⇒ a real change.
            m[pos] ^= xor;
            match decode_data_frame(&m) {
                Ok(rec) => {
                    // A decoded record must be self-consistent (the decoder only
                    // returns Ok when frame_len + section lengths + CRC all agree).
                    // We don't pin the seq (a mutated-but-CRC-valid frame is
                    // possible in principle); we only require it didn't panic and
                    // the returned struct is well-formed (already guaranteed by the
                    // type). Touch a field so the value is observed.
                    let _ = rec.seq;
                }
                Err(SegmentError::Corrupt(_)) => {}
                Err(other) => panic!("mutated frame must be Ok or Corrupt, got {other:?}"),
            }
        }

        // (d) A random `.idx` buffer of a random (possibly non-stride) length fed to
        //     idx_entry_at / idx_len / lookup: idx_len floors to whole strides, an
        //     entry past the end is None, and a huge entry index never overflows.
        {
            let len = rng.below(5 * IDX_STRIDE + 7);
            let mut ibuf = vec![0u8; len];
            for b in &mut ibuf {
                *b = rng.next_u64() as u8;
            }
            let count = idx_len(&ibuf);
            assert_eq!(count, len / IDX_STRIDE, "idx_len floors to whole strides");
            // Every in-range entry decodes to Some; the first out-of-range is None.
            for e in 0..count {
                assert!(
                    idx_entry_at(&ibuf, e).is_some(),
                    "entry {e} within {count} whole strides resolves"
                );
            }
            assert!(
                idx_entry_at(&ibuf, count).is_none(),
                "the first past-the-end entry is a clean None"
            );
            // A gigantic entry index must not overflow the offset multiply (checked
            // arithmetic ⇒ None, never a panic).
            assert!(idx_entry_at(&ibuf, usize::MAX).is_none(), "usize::MAX ⇒ None");
            // lookup with random start/seq is panic-free and bounded.
            let start = rng.next_u64();
            let seq = rng.next_u64();
            let _ = lookup(&ibuf, start, seq);
        }
    }

    // A couple of explicit adversarial frames from the catalog (huge/garbage
    // frame_len, sub-minimum frame_len, zeroed frame) — each must be Corrupt, never
    // a panic or an OOB read past the slice.
    {
        // Huge frame_len past EOF.
        let mut huge = frame.clone();
        huge[0..4].copy_from_slice(&0xFFFF_FFFFu32.to_le_bytes());
        assert!(matches!(decode_data_frame(&huge), Err(SegmentError::Corrupt(_))));
        // Sub-minimum frame_len (1).
        let mut tiny = frame.clone();
        tiny[0..4].copy_from_slice(&1u32.to_le_bytes());
        assert!(matches!(decode_data_frame(&tiny), Err(SegmentError::Corrupt(_))));
        // All-zeros buffer (a zeroed/lost frame).
        let zeros = vec![0u8; 64];
        assert!(matches!(
            decode_data_frame(&zeros),
            Err(SegmentError::Corrupt(_))
        ));
        // Empty buffer (shorter than the frame_len prefix).
        assert!(matches!(decode_data_frame(&[]), Err(SegmentError::Corrupt(_))));
        // A frame_len that claims a valid size but whose internal section lengths
        // overflow: poke node_len/tag_len/data_len to u16/u32 max in the header.
        let mut overflow = frame.clone();
        // header starts at offset 4: flags(1) seq(8) ts(8) node_len(2)@21 tag_len(2)@23 data_len(4)@25
        overflow[21..23].copy_from_slice(&u16::MAX.to_le_bytes());
        overflow[23..25].copy_from_slice(&u16::MAX.to_le_bytes());
        overflow[25..29].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(
            matches!(decode_data_frame(&overflow), Err(SegmentError::Corrupt(_))),
            "internal-length overflow ⇒ Corrupt, never OOB"
        );
    }
}
