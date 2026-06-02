//! Phase-8B fault catalog — **segment-idx boundary**, group B.
//!
//! Two fault/crash strategies for the `.data`/`.idx` consistency contract of
//! [`LocalSegmentStore`] (DESIGN invariant 8: a recovered segment's `.idx` never
//! references a `.data` byte range that isn't durably present; a segment is
//! "complete" only once BOTH parts exist — `list()` requires the `.data` part, so
//! the WAL/snapshot remain the source of truth and no live record is lost).
//!
//! These oracles live entirely at the segment-store seam, so each test drives a
//! real [`LocalSegmentStore`] (the exact production `put`/`list`/`read_range`/
//! `delete`/`resolve` code) through an in-memory [`FakeDisk`] (the Phase-8A
//! harness). Bounded, fixed-seed, single-segment workloads keep the whole file
//! well under a second.
//!
//! ```text
//! cargo test --features test-fs --test fault_seg_idx_b
//! ```
//!
//! Strategies implemented (see docs/FAULT_TESTING.md / topics-fault-catalog.json):
//!   - F-SEG-IDX-WITHOUT-DATA  stray `.idx` with no `.data`: `list()` requires the
//!       `.data` part, so the orphan `.idx` is NOT reported as a complete segment;
//!       `TopicTier::resolve` returns `None`; orphan reclaim removes it; nothing
//!       panics.
//!   - F-SWEEP-SEGMENT-SEAL    exhaustive crash-point sweep across a single
//!       `LocalSegmentStore.put` (.data tmp/fsync/rename, .idx tmp/fsync/rename,
//!       dir fsync). At every crash point the segment is either COMPLETE (both
//!       parts, listable + readable) or treated as INCOMPLETE (no `.idx` ⇒
//!       non-authoritative; the WAL is the source of truth so no live record is
//!       lost); a re-`put` over the crashed image re-materializes idempotently.

#![cfg(feature = "test-fs")]
#![allow(
    clippy::ptr_arg,
    clippy::manual_clamp,
    clippy::unusual_byte_groupings,
    clippy::doc_lazy_continuation
)]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use topics::storage::testfs::{FakeDisk, TornDamage};
use topics::storage::{
    decode_data_frame, idx_name, lookup, File, Fs, LocalSegmentStore, OpenOpts, SegmentBuilder,
    SegmentId, SegmentPart, SegmentRecord, SegmentStore, StoreError, Tier, TopicTier,
};

const ROOT: &str = "/seg";

/// Build a small, contiguous single-segment `(data, idx)` pair starting at
/// `start` with `n` records. Deterministic bytes so a re-build is byte-identical.
fn build_segment(start: u64, n: u64) -> (Vec<u8>, Vec<u8>) {
    let mut b = SegmentBuilder::new(start);
    for i in 0..n {
        b.push(&SegmentRecord {
            seq: start + i,
            ts: 1_700_000_000_000 + i,
            node: (i % 2 == 0).then(|| format!("n{i}")),
            tag: Some(format!("t{i}")),
            data: format!("{{\"v\":{i}}}").into_bytes(),
        });
    }
    b.finish()
}

/// `seg-<id>.idx` path under [`ROOT`].
fn idx_path(id: SegmentId) -> PathBuf {
    PathBuf::from(ROOT).join(idx_name(id))
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

/// Durably install one segment part (`tmp` write → fsync → dir-fsync → rename →
/// dir-fsync), i.e. the exact byte/dir-fsync ordering `LocalSegmentStore::put`
/// uses for one `write_atomic`. After this the final name survives a crash.
fn install_part_durable(fs: &Arc<dyn Fs>, final_path: &Path, bytes: &[u8]) {
    let ext = final_path
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("");
    let tmp = final_path.with_extension(format!("{ext}.tmp"));
    write_pending(fs, &tmp, bytes);
    fs.open(&tmp, OpenOpts::rw_existing())
        .unwrap()
        .sync_all()
        .unwrap();
    fs.sync_dir(Path::new(ROOT)).unwrap(); // tmp name durable
    fs.rename(&tmp, final_path).unwrap();
    fs.sync_dir(Path::new(ROOT)).unwrap(); // final name durable
}

/// Open a [`LocalSegmentStore`] over `disk` rooted at [`ROOT`] (post-crash view).
fn open_store(disk: &FakeDisk) -> LocalSegmentStore {
    LocalSegmentStore::open_with(ROOT, disk.arc()).expect("store opens through FakeDisk")
}

// ===========================================================================
// F-SEG-IDX-WITHOUT-DATA
//
// A stray `.idx` survives a crash with NO matching `.data` (the catalog notes
// this is rare — `put` writes `.data` before `.idx` — but possible via external
// corruption or a relocation that copied the `.idx` part first). Oracle:
//   - `list()` keys on the `.data` part, so the orphan `.idx` is NOT reported as
//     a complete segment (an `.idx` that indexes nothing must never be served);
//   - `TopicTier::resolve` (which requires `.data`) returns `None` for the id;
//   - a read that needs the `.data` is a clean `NotFound`, never a panic;
//   - orphan reclaim (`delete`) removes the stray `.idx` idempotently.
// ===========================================================================

#[test]
fn f_seg_idx_without_data() {
    let disk = FakeDisk::with_seed(0x1D_0000);
    let fs = disk.arc();
    fs.create_dir_all(Path::new(ROOT)).unwrap();
    let id: SegmentId = 7;
    let (_data, idx) = build_segment(id, 4);

    // Install a lone `.idx` durably (the `.data` is NEVER written) — the crash
    // leaves only the `.idx` on disk, the inverse of the common
    // F-SEG-DATA-WITHOUT-IDX case.
    install_part_durable(&fs, &idx_path(id), &idx);
    disk.crash(TornDamage::None);
    disk.reset_power();

    let store = open_store(&disk);

    // The `.idx` survived; the `.data` never existed.
    assert!(
        store.exists(id, SegmentPart::Idx),
        "the durable orphan .idx survives the crash"
    );
    assert!(
        !store.exists(id, SegmentPart::Data),
        "the .data was never written"
    );

    // ORACLE 1: `list()` requires the `.data` part, so the orphan `.idx` is NOT
    // reported as a complete segment — an index that points at no data bytes is
    // never surfaced as a listable segment.
    assert!(
        store.list().unwrap().is_empty(),
        "an orphan .idx with no .data is NOT a complete (listable) segment"
    );

    // ORACLE 2: `TopicTier::resolve` (HOT-only) keys on the `.data` part too, so the
    // orphan resolves to NO tier — it is non-authoritative and never served.
    let hot = Box::new(LocalSegmentStore::open_with(ROOT, disk.arc()).unwrap());
    let tier = TopicTier::new(hot, None);
    assert_eq!(
        tier.resolve(id),
        None,
        "resolve returns None for an .idx with no .data"
    );
    assert!(
        tier.store_for(id).is_none(),
        "no store resolves an .idx-only orphan"
    );

    // ORACLE 3: a read that needs the missing `.data` is a clean `NotFound`, never
    // a panic and never neighbor bytes decoded as a frame. We resolve a record's
    // locator from the present `.idx` and then attempt the `.data` read it points
    // at — the read fails cleanly because the `.data` file is absent.
    let idx_buf = store
        .read_all(id, SegmentPart::Idx)
        .expect("the .idx reads");
    let e = lookup(&idx_buf, id, id).expect("the orphan .idx still locates seq");
    match store.read_range(id, SegmentPart::Data, e.offset as u64, e.len as u64) {
        Err(StoreError::NotFound(got)) => assert_eq!(got, id),
        other => panic!("a .data read with no .data file must be NotFound, got {other:?}"),
    }
    assert!(
        matches!(
            store.len(id, SegmentPart::Data),
            Err(StoreError::NotFound(_))
        ),
        "len() on the absent .data is NotFound"
    );

    // ORACLE 4: orphan reclaim removes the stray `.idx` idempotently (no live
    // record is lost — there was no `.data`, so nothing was ever authoritative;
    // the WAL/snapshot remains the source of truth). A second sweep finds nothing.
    store.delete(id).expect("orphan .idx reclaims cleanly");
    assert!(
        !store.exists(id, SegmentPart::Idx),
        "the stray .idx is removed by reclaim"
    );
    assert!(
        store.list().unwrap().is_empty(),
        "store is clean after reclaim"
    );
    store.delete(id).expect("idempotent second reclaim sweep");
}

// ===========================================================================
// F-SWEEP-SEGMENT-SEAL
//
// SQLite-style exhaustive crash-point sweep across ONE segment seal `put`
// (`.data` tmp-write/fsync/rename, `.idx` tmp-write/fsync/rename, the final dir
// fsync). For each crash point in `0..=M` over the put's FS mutating calls:
//   - replay the put on a FRESH disk (whose pre-put image is empty);
//   - `crash()` after exactly that many FS mutating calls;
//   - reopen the store on the crashed image and assert the oracle:
//       * the segment is EITHER complete (both parts present, listable, every
//         record reads + decodes correctly through the real `.idx`→`.data` path),
//       * OR it is incomplete (the `.data` part is absent ⇒ NOT listed; a present
//         but data-less `.idx` is non-authoritative) — in which case the WAL is
//         the source of truth, so NO live record is lost;
//       * never a half-state where `list()` reports the segment yet a record read
//         returns garbage / panics;
//   - a re-`put` of the SAME id over the crashed image re-materializes the segment
//     byte-identically and idempotently (recovery re-seals from the WAL).
//
// Bounded: one tiny single-segment put issues a small, fixed number of FS calls,
// and the sweep iterates each crash point once with a couple of torn-damage
// seeds, so the whole test runs in well under a second.
// ===========================================================================

/// An [`Fs`] wrapper over a [`FakeDisk`] that fires `disk.crash(damage)` exactly
/// after the `at`-th (0-based) FS *mutating* call (`write_at` / `set_len` /
/// `sync_data` / `sync_all` / `rename` / `sync_dir`), then lets the now-frozen
/// disk swallow the rest of the `put`. This is the harness-level "power loss after
/// the Nth FS call" injector, scoped to the segment-store call space (it mirrors
/// the `CrashAfter` wrapper in tests/crash_oracle.rs but counts ALL mutating op
/// classes so a single `put`'s every boundary is a crash point).
#[derive(Clone)]
struct CrashAfterAny {
    disk: FakeDisk,
    at: u64,
    damage: TornDamage,
    seen: Arc<AtomicU64>,
    tripped: Arc<std::sync::atomic::AtomicBool>,
}

impl CrashAfterAny {
    fn new(disk: FakeDisk, at: u64, damage: TornDamage) -> Self {
        CrashAfterAny {
            disk,
            at,
            damage,
            seen: Arc::new(AtomicU64::new(0)),
            tripped: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    fn arc(&self) -> Arc<dyn Fs> {
        Arc::new(self.clone())
    }

    /// Count how many FS mutating calls this wrapper saw (run once with
    /// `at = u64::MAX` to size the sweep).
    fn calls_seen(&self) -> u64 {
        self.seen.load(Ordering::SeqCst)
    }

    /// Count one mutating call; if it reaches `at`, trip the crash exactly once.
    fn tick(&self) {
        let idx = self.seen.fetch_add(1, Ordering::SeqCst);
        if idx == self.at
            && self
                .tripped
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            self.disk.crash(self.damage);
        }
    }
}

struct CrashAfterAnyFile {
    inner: Box<dyn File>,
    owner: CrashAfterAny,
}

impl File for CrashAfterAnyFile {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read_at(offset, buf)
    }
    fn write_at(&mut self, offset: u64, buf: &[u8]) -> std::io::Result<usize> {
        // Trip AFTER the write reaches the (pending) image, so a crash at index N
        // keeps the first N writes' pending bytes available for a following fsync
        // but drops anything not yet fsynced — the power-loss-after-Nth-write model.
        let r = self.inner.write_at(offset, buf);
        self.owner.tick();
        r
    }
    fn set_len(&mut self, len: u64) -> std::io::Result<()> {
        let r = self.inner.set_len(len);
        self.owner.tick();
        r
    }
    fn sync_data(&self) -> std::io::Result<()> {
        let r = self.inner.sync_data();
        self.owner.tick();
        r
    }
    fn sync_all(&self) -> std::io::Result<()> {
        let r = self.inner.sync_all();
        self.owner.tick();
        r
    }
    fn metadata_len(&self) -> std::io::Result<u64> {
        self.inner.metadata_len()
    }
    fn read_to_end_from(&self, offset: u64, out: &mut Vec<u8>) -> std::io::Result<()> {
        self.inner.read_to_end_from(offset, out)
    }
}

impl Fs for CrashAfterAny {
    fn open(&self, path: &Path, opts: OpenOpts) -> std::io::Result<Box<dyn File>> {
        let inner = self.disk.open(path, opts)?;
        Ok(Box::new(CrashAfterAnyFile {
            inner,
            owner: self.clone(),
        }))
    }
    fn rename(&self, from: &Path, to: &Path) -> std::io::Result<()> {
        let r = self.disk.rename(from, to);
        self.tick();
        r
    }
    fn remove_file(&self, path: &Path) -> std::io::Result<()> {
        self.disk.remove_file(path)
    }
    fn read_dir(&self, dir: &Path) -> std::io::Result<Vec<PathBuf>> {
        self.disk.read_dir(dir)
    }
    fn sync_dir(&self, dir: &Path) -> std::io::Result<()> {
        let r = self.disk.sync_dir(dir);
        self.tick();
        r
    }
    fn create_dir_all(&self, dir: &Path) -> std::io::Result<()> {
        self.disk.create_dir_all(dir)
    }
    fn exists(&self, path: &Path) -> bool {
        self.disk.exists(path)
    }
    fn metadata_len(&self, path: &Path) -> std::io::Result<u64> {
        self.disk.metadata_len(path)
    }
}

/// Assert the recovered store's invariant after a crash mid-`put`: the segment is
/// either COMPLETE (both parts present + every record reads and decodes) or
/// INCOMPLETE (the `.data` is absent ⇒ not listed; the WAL is the source of
/// truth) — never a served half-state. `n` is the record count the put covered.
fn assert_seg_complete_or_incomplete(disk: &FakeDisk, id: SegmentId, n: u64) {
    let store = open_store(disk);
    let data_present = store.exists(id, SegmentPart::Data);
    let idx_present = store.exists(id, SegmentPart::Idx);
    let listed = store.list().unwrap();

    // `list()` keys on `.data`: a segment is listed IFF its `.data` part exists.
    assert_eq!(
        listed.contains(&id),
        data_present,
        "list() reports the segment IFF its .data part exists (data={data_present}, \
         listed={listed:?})"
    );

    if data_present && idx_present {
        // COMPLETE: both parts present. Every record must read + decode correctly
        // through the real `.idx`→`.data` path — no torn/garbled frame served, no
        // out-of-bounds, no panic. (A crashed-but-renamed `.data`/`.idx` whose
        // bytes were fully fsynced before the crash is a valid complete segment.)
        let idx_buf = store
            .read_all(id, SegmentPart::Idx)
            .expect("a present .idx bulk-reads");
        for s in id..id + n {
            // If the `.idx` is itself a fully-durable copy, every seq locates and
            // its frame is in-bounds and decodes to the right record.
            let e = lookup(&idx_buf, id, s)
                .unwrap_or_else(|| panic!("complete segment must locate seq {s}"));
            let frame = store
                .read_range(id, SegmentPart::Data, e.offset as u64, e.len as u64)
                .unwrap_or_else(|err| {
                    panic!("complete segment seq {s} must read in-bounds, got {err:?}")
                });
            let rec = decode_data_frame(&frame)
                .unwrap_or_else(|err| panic!("complete segment seq {s} must decode, got {err:?}"));
            assert_eq!(rec.seq, s, "decoded the right record for seq {s}");
        }
    } else if !data_present {
        // INCOMPLETE (no `.data`): the segment is NOT listed, so it is
        // non-authoritative — the WAL/snapshot still holds every record, so no live
        // record is lost. A `.data` read is a clean NotFound (never a panic). A
        // stray data-less `.idx`, if present, is harmless (the orphan case).
        assert!(
            !listed.contains(&id),
            "an incomplete segment with no .data is not listed"
        );
        assert!(
            matches!(
                store.read_all(id, SegmentPart::Data),
                Err(StoreError::NotFound(_))
            ),
            "a .data read with no .data file is a clean NotFound"
        );
        // `TopicTier::resolve` agrees: no `.data` ⇒ no tier ⇒ never served.
        let hot = Box::new(LocalSegmentStore::open_with(ROOT, disk.arc()).unwrap());
        let tier = TopicTier::new(hot, None);
        assert_eq!(tier.resolve(id), None, "no .data ⇒ resolve None");
    } else {
        // `.data` present but `.idx` absent: a listed-but-incomplete segment (the
        // F-SEG-CRASH-AFTER-DATA-BEFORE-IDX shape). It IS listed (keys on `.data`)
        // but a read needing the `.idx` is a clean NotFound — never a half-index
        // served. The WAL re-materializes the `.idx` on the next seal.
        assert!(
            listed.contains(&id),
            "a present .data is listed even without its .idx"
        );
        assert!(
            matches!(
                store.read_all(id, SegmentPart::Idx),
                Err(StoreError::NotFound(_))
            ),
            "a missing .idx ⇒ NotFound, never a served half-index"
        );
        // resolve still keys on `.data`, so the segment resolves HOT — but it is
        // non-authoritative for reads until the `.idx` is re-materialized.
        let hot = Box::new(LocalSegmentStore::open_with(ROOT, disk.arc()).unwrap());
        let tier = TopicTier::new(hot, None);
        assert_eq!(
            tier.resolve(id),
            Some(Tier::Hot),
            "a lone .data resolves HOT"
        );
    }
}

#[test]
fn f_sweep_segment_seal() {
    let id: SegmentId = 1;
    let n = 4u64;
    let (data, idx) = build_segment(id, n);

    // Probe M: how many FS mutating calls one `put` issues (at = u64::MAX ⇒ never
    // crashes; just counts). The put is `.data` write_atomic (open+write+fsync+
    // rename) + `.idx` write_atomic + a final dir fsync.
    let probe_disk = FakeDisk::new();
    probe_disk.create_dir_all(Path::new(ROOT)).unwrap();
    let probe = CrashAfterAny::new(probe_disk.clone(), u64::MAX, TornDamage::None);
    {
        let store = LocalSegmentStore::open_with(ROOT, probe.arc()).expect("probe store");
        store.put(id, &data, &idx).expect("probe put succeeds");
    }
    let total = probe.calls_seen();
    assert!(
        total >= 4,
        "a segment put issues several FS mutating calls (M={total})"
    );

    // Sweep every crash point across the put, with a few torn-damage seeds so the
    // atomicity WITHIN a single write is exercised too (ALICE-style).
    for &damage in &[
        TornDamage::None,
        TornDamage::PrefixTruncate,
        TornDamage::Garble,
    ] {
        // Tiered sweep (topics::testutil::crash_points): bounded deterministic
        // sample per damage mode by default, full `0..=total` under
        // TOPICS_TEST_EXHAUSTIVE. The 3 torn-damage modes always run in full.
        for crash_point in topics::testutil::crash_points(total) {
            // A FRESH disk whose pre-put durable image is empty; crash after exactly
            // `crash_point` FS mutating calls of the put.
            let disk = FakeDisk::with_seed(crash_point.wrapping_mul(0x9E37_79B9) ^ damage as u64);
            disk.create_dir_all(Path::new(ROOT)).unwrap();
            let trip = CrashAfterAny::new(disk.clone(), crash_point, damage);
            {
                let store =
                    LocalSegmentStore::open_with(ROOT, trip.arc()).expect("sweep store opens");
                // The put may fail partway (a write past the crash point lands on the
                // frozen device and is dropped) — tolerate either outcome; the disk
                // image after the crash is what the oracle inspects.
                let _ = store.put(id, &data, &idx);
            }
            disk.reset_power();

            // ORACLE: at every crash point the segment is complete-or-incomplete,
            // never a served half-state, and no live record is lost (the WAL is the
            // source of truth for an incomplete segment).
            assert_seg_complete_or_incomplete(&disk, id, n);

            // RE-MATERIALIZE IDEMPOTENT: a re-`put` of the same id over the crashed
            // image (what recovery does — re-seal from the WAL) completes the segment
            // byte-identically. After it, the segment is unconditionally complete and
            // every record reads + decodes correctly.
            {
                let store = open_store(&disk);
                store
                    .put(id, &data, &idx)
                    .expect("re-put over the crashed image re-materializes the segment");
            }
            let store = open_store(&disk);
            assert!(
                store.list().unwrap().contains(&id),
                "after re-put the segment is complete and listed (crash_point={crash_point}, \
                 {damage:?})"
            );
            assert_eq!(
                store.read_all(id, SegmentPart::Data).unwrap(),
                data,
                "re-put restored the exact .data bytes (crash_point={crash_point}, {damage:?})"
            );
            assert_eq!(
                store.read_all(id, SegmentPart::Idx).unwrap(),
                idx,
                "re-put restored the exact .idx bytes (crash_point={crash_point}, {damage:?})"
            );
            // Every record reads + decodes through the re-materialized segment.
            let idx_buf = store.read_all(id, SegmentPart::Idx).unwrap();
            for s in id..id + n {
                let e = lookup(&idx_buf, id, s).expect("re-put segment locates every seq");
                let frame = store
                    .read_range(id, SegmentPart::Data, e.offset as u64, e.len as u64)
                    .expect("re-put record in-bounds");
                assert_eq!(
                    decode_data_frame(&frame).unwrap().seq,
                    s,
                    "re-put record decodes (crash_point={crash_point}, {damage:?})"
                );
            }
        }
    }
}
