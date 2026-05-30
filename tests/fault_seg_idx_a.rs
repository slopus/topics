//! Phase-8B fault catalog — **segment-idx boundary**, group A.
//!
//! Five fault/crash strategies that exercise the `.data`/`.idx` consistency
//! contract of [`LocalSegmentStore`] (DESIGN invariant 8: a recovered segment's
//! `.idx` never references a `.data` byte range that isn't durably present; a
//! crash between the `.data` and `.idx` put leaves a segment recovery treats as
//! *incomplete* — `list()` requires the `.data` part, and the WAL/snapshot remain
//! the source of truth so no live record is lost).
//!
//! These oracles live entirely at the segment-store seam, so each test drives a
//! real [`LocalSegmentStore`] through an in-memory [`FakeDisk`] / [`FaultFs`] (the
//! Phase-8A harness) and asserts the store's `list`/`resolve`/`read_range`
//! behavior after the injected fault. Bounded, fixed-seed, single-segment
//! workloads keep the whole file well under a second.
//!
//! ```text
//! cargo test --features test-fs --test fault_seg_idx_a
//! ```
//!
//! Strategies implemented (see docs/FAULT_TESTING.md):
//!   F-SEG-CRASH-AFTER-DATA-BEFORE-IDX   crash after .data put, before .idx put
//!   F-SEG-CRASH-IDX-TMP-NOT-RENAMED     crash after .idx .tmp fsync, before rename
//!   F-SEG-IDX-TORN-STRIDE               .idx truncated to a non-stride length
//!   F-SEG-IDX-OFFSET-PAST-DATA          .idx offset/len past a (stale shorter) .data
//!   F-SEG-DATA-WITHOUT-IDX-LISTED       stray .data, no matching .idx

#![cfg(feature = "test-fs")]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use streams::storage::testfs::{FakeDisk, TornDamage};
use streams::storage::{
    data_name, decode_data_frame, idx_entry_at, idx_len, idx_name, lookup, Fs, LocalSegmentStore,
    OpenOpts, SegmentBuilder, SegmentId, SegmentPart, SegmentRecord, SegmentStore, StoreError,
    IDX_STRIDE,
};

const ROOT: &str = "/seg";

/// Build a small, contiguous single-segment `(data, idx)` pair starting at
/// `start` with `n` records. Deterministic bytes so torn/garble damage is
/// reproducible.
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

/// `seg-<id>.data` / `seg-<id>.idx` paths under [`ROOT`].
fn data_path(id: SegmentId) -> PathBuf {
    PathBuf::from(ROOT).join(data_name(id))
}
fn idx_path(id: SegmentId) -> PathBuf {
    PathBuf::from(ROOT).join(idx_name(id))
}

/// Open a [`LocalSegmentStore`] over `disk` rooted at [`ROOT`] (post-crash view).
fn open_store(disk: &FakeDisk) -> LocalSegmentStore {
    LocalSegmentStore::open_with(ROOT, disk.arc()).expect("store opens through FakeDisk")
}

// ===========================================================================
// F-SEG-CRASH-AFTER-DATA-BEFORE-IDX
// crash after .data put (fsynced + renamed + dir-fsynced) but before .idx put.
// Oracle: list() requires .data, so the segment IS listed (the .data is durable),
// but it is INCOMPLETE — its .idx is absent, so a read needs .idx and finds
// nothing; the WAL/snapshot still holds every record so no live record is lost.
// ===========================================================================

#[test]
fn f_seg_crash_after_data_before_idx() {
    let disk = FakeDisk::with_seed(0xDA7A);
    let fs = disk.arc();
    fs.create_dir_all(Path::new(ROOT)).unwrap();
    let id: SegmentId = 1;
    let (data, _idx) = build_segment(id, 4);

    // Durably install ONLY the .data (write tmp → fsync → rename → dir-fsync),
    // exactly the prefix of LocalSegmentStore::put before the .idx write. Then
    // power loss before the .idx is ever written.
    let dtmp = data_path(id).with_extension("data.tmp");
    write_pending(&fs, &dtmp, &data);
    fs.open(&dtmp, OpenOpts::rw_existing())
        .unwrap()
        .sync_all()
        .unwrap();
    fs.sync_dir(Path::new(ROOT)).unwrap(); // tmp name durable
    fs.rename(&dtmp, &data_path(id)).unwrap();
    fs.sync_dir(Path::new(ROOT)).unwrap(); // final .data name durable
    // (No .idx written at all — this is the crash point.)
    disk.crash(TornDamage::None);
    disk.reset_power();

    // The .data survived; the .idx never existed.
    let store = open_store(&disk);
    assert!(
        store.exists(id, SegmentPart::Data),
        "durable .data survives the crash"
    );
    assert!(
        !store.exists(id, SegmentPart::Idx),
        "the .idx was never written"
    );

    // ORACLE 1: list() requires the .data part, so the segment is reported — but as
    // an incomplete unit (no .idx). The engine treats a missing .idx as
    // non-authoritative; the WAL/snapshot remains the source of truth.
    assert_eq!(store.list().unwrap(), vec![id], "list() reports the lone .data");

    // ORACLE 2: a read that needs the .idx (bulk-read it to locate any seq) gets a
    // clean NotFound, never a panic, never neighbor bytes decoded as an index.
    match store.read_all(id, SegmentPart::Idx) {
        Err(StoreError::NotFound(got)) => assert_eq!(got, id),
        other => panic!("missing .idx must read as NotFound, got {other:?}"),
    }

    // ORACLE 3: orphan reclaim handles the stray .data — delete() is idempotent and
    // tolerant of the absent .idx, leaving the store clean. (No live record is lost
    // because the WAL/snapshot still has them; recovery re-materializes.)
    store.delete(id).expect("incomplete segment reclaims cleanly");
    assert!(store.list().unwrap().is_empty(), "stray .data reclaimed");
    assert!(!store.exists(id, SegmentPart::Data));
}

// ===========================================================================
// F-SEG-CRASH-IDX-TMP-NOT-RENAMED
// crash after the .idx .tmp is fsynced but before its rename to the final name.
// Oracle: the final .idx is absent (the un-dir-fsynced rename never happened /
// rolls back); the segment is incomplete; recovery rebuilds from WAL/snapshot;
// no half-indexed segment is served.
// ===========================================================================

#[test]
fn f_seg_crash_idx_tmp_not_renamed() {
    let disk = FakeDisk::with_seed(0x1D7);
    let fs = disk.arc();
    fs.create_dir_all(Path::new(ROOT)).unwrap();
    let id: SegmentId = 1;
    let (data, idx) = build_segment(id, 4);

    // Install the .data durably (the put's first half).
    let dtmp = data_path(id).with_extension("data.tmp");
    write_pending(&fs, &dtmp, &data);
    fs.open(&dtmp, OpenOpts::rw_existing())
        .unwrap()
        .sync_all()
        .unwrap();
    fs.sync_dir(Path::new(ROOT)).unwrap();
    fs.rename(&dtmp, &data_path(id)).unwrap();
    fs.sync_dir(Path::new(ROOT)).unwrap();

    // Write the .idx .tmp and fsync its BYTES — but crash before the rename to the
    // final .idx name (and before any post-rename dir fsync). The tmp's NAME was
    // never dir-fsynced after this, so the crash drops the final name entirely.
    let itmp = idx_path(id).with_extension("idx.tmp");
    write_pending(&fs, &itmp, &idx);
    fs.open(&itmp, OpenOpts::rw_existing())
        .unwrap()
        .sync_all()
        .unwrap();
    // CRASH POINT: .idx .tmp fsynced, rename NOT done.
    disk.crash(TornDamage::None);
    disk.reset_power();

    let store = open_store(&disk);
    // The final .idx must be absent (the rename never ran).
    assert!(
        !store.exists(id, SegmentPart::Idx),
        "final .idx absent — rename never happened"
    );
    assert!(
        store.exists(id, SegmentPart::Data),
        "the durable .data survives"
    );

    // ORACLE: list() reports the segment by its .data, but it is incomplete (no
    // final .idx). No half-indexed segment is served — an .idx read is NotFound.
    assert_eq!(store.list().unwrap(), vec![id]);
    assert!(
        matches!(
            store.read_all(id, SegmentPart::Idx),
            Err(StoreError::NotFound(_))
        ),
        "no final .idx ⇒ NotFound, not a served half-index"
    );

    // Idempotent reclaim of the incomplete segment (recovery would rebuild it from
    // the WAL/snapshot).
    store.delete(id).expect("incomplete segment reclaims cleanly");
    assert!(store.list().unwrap().is_empty());
}

// ===========================================================================
// F-SEG-IDX-TORN-STRIDE
// the .idx file is truncated to a length that is NOT a multiple of IDX_STRIDE
// (a partial last entry). Oracle: idx_len() floors to whole strides; idx_entry_at
// / lookup past the last whole entry returns None (a clean miss, never a panic or
// an OOB read); a seq mapping to the partial entry falls back to the WAL.
// ===========================================================================

#[test]
fn f_seg_idx_torn_stride() {
    let disk = FakeDisk::with_seed(0x57417DE);
    let fs = disk.arc();
    fs.create_dir_all(Path::new(ROOT)).unwrap();
    let id: SegmentId = 1;
    let n = 5u64;
    let (data, idx) = build_segment(id, n);
    assert_eq!(idx.len(), n as usize * IDX_STRIDE, "5 whole strides");

    // Install the .data durably.
    let dtmp = data_path(id).with_extension("data.tmp");
    write_pending(&fs, &dtmp, &data);
    fs.open(&dtmp, OpenOpts::rw_existing())
        .unwrap()
        .sync_all()
        .unwrap();
    fs.sync_dir(Path::new(ROOT)).unwrap();
    fs.rename(&dtmp, &data_path(id)).unwrap();
    fs.sync_dir(Path::new(ROOT)).unwrap();

    // Install a TORN .idx: only the first 3 whole entries plus a half stride of the
    // 4th — i.e. truncated to a non-multiple of IDX_STRIDE (the tail write was torn
    // mid-entry). Write it as a durable file directly (the torn bytes are what hit
    // the platter); the partial 4th entry models the in-flight tear.
    let torn_len = 3 * IDX_STRIDE + IDX_STRIDE / 2; // 3.5 entries
    let torn_idx = idx[..torn_len].to_vec();
    let itmp = idx_path(id).with_extension("idx.tmp");
    write_pending(&fs, &itmp, &torn_idx);
    fs.open(&itmp, OpenOpts::rw_existing())
        .unwrap()
        .sync_all()
        .unwrap();
    fs.sync_dir(Path::new(ROOT)).unwrap();
    fs.rename(&itmp, &idx_path(id)).unwrap();
    fs.sync_dir(Path::new(ROOT)).unwrap();
    disk.crash(TornDamage::None);
    disk.reset_power();

    let store = open_store(&disk);
    assert_eq!(store.list().unwrap(), vec![id], "segment still lists (has .data)");

    // Read the torn .idx back; it is a non-stride length on disk.
    let idx_buf = store.read_all(id, SegmentPart::Idx).unwrap();
    assert_eq!(idx_buf.len(), torn_len);
    assert_ne!(idx_buf.len() % IDX_STRIDE, 0, "non-stride length");

    // ORACLE 1: idx_len() floors to the 3 WHOLE strides (the partial entry is not
    // counted).
    assert_eq!(idx_len(&idx_buf), 3, "idx_len floors to whole strides");

    // ORACLE 2: the 3 whole entries resolve and decode their frames correctly.
    for s in id..id + 3 {
        let e = lookup(&idx_buf, id, s).expect("whole entry resolves");
        let frame = store
            .read_range(id, SegmentPart::Data, e.offset as u64, e.len as u64)
            .expect("frame in range");
        let rec = decode_data_frame(&frame).expect("frame decodes");
        assert_eq!(rec.seq, s, "decoded the right record");
    }

    // ORACLE 3: the partial 4th entry (and beyond) is a CLEAN MISS — idx_entry_at /
    // lookup return None, never a panic or an out-of-bounds read. A seq mapping
    // there falls back to the WAL (modeled here as "no index entry ⇒ None").
    assert!(idx_entry_at(&idx_buf, 3).is_none(), "partial entry ⇒ None");
    assert!(idx_entry_at(&idx_buf, 4).is_none());
    assert!(lookup(&idx_buf, id, id + 3).is_none(), "seq 4 → WAL fallback");
    assert!(lookup(&idx_buf, id, id + 4).is_none(), "seq 5 → WAL fallback");
}

// ===========================================================================
// F-SEG-IDX-OFFSET-PAST-DATA
// an .idx entry's offset/len points past the .data file end (a mismatched pair:
// a newer/full .idx alongside a stale, shorter .data — e.g. a partial relocation
// or corruption). Oracle: read_range returns RangeOutOfBounds (NOT a silent short
// read), so the record resolves from the WAL/resident set instead, and neighbor
// bytes are never decoded as the frame.
// ===========================================================================

#[test]
fn f_seg_idx_offset_past_data() {
    let disk = FakeDisk::with_seed(0x0FF5E7);
    let fs = disk.arc();
    fs.create_dir_all(Path::new(ROOT)).unwrap();
    let id: SegmentId = 1;
    let n = 4u64;
    let (full_data, idx) = build_segment(id, n);

    // The .idx is the FULL, newer index (4 entries). The .data is a STALE, shorter
    // copy holding only the first 2 records' frames — so the .idx entries for the
    // last 2 records point past the truncated .data end.
    let e2 = lookup(&idx, id, id + 2).expect("entry for the 3rd record");
    let stale_data = full_data[..e2.offset as usize].to_vec(); // only records 0..2

    // Install the stale .data durably.
    let dtmp = data_path(id).with_extension("data.tmp");
    write_pending(&fs, &dtmp, &stale_data);
    fs.open(&dtmp, OpenOpts::rw_existing())
        .unwrap()
        .sync_all()
        .unwrap();
    fs.sync_dir(Path::new(ROOT)).unwrap();
    fs.rename(&dtmp, &data_path(id)).unwrap();
    fs.sync_dir(Path::new(ROOT)).unwrap();

    // Install the full .idx durably.
    let itmp = idx_path(id).with_extension("idx.tmp");
    write_pending(&fs, &itmp, &idx);
    fs.open(&itmp, OpenOpts::rw_existing())
        .unwrap()
        .sync_all()
        .unwrap();
    fs.sync_dir(Path::new(ROOT)).unwrap();
    fs.rename(&itmp, &idx_path(id)).unwrap();
    fs.sync_dir(Path::new(ROOT)).unwrap();
    disk.crash(TornDamage::None);
    disk.reset_power();

    let store = open_store(&disk);
    assert_eq!(store.list().unwrap(), vec![id]);
    let data_len = store.len(id, SegmentPart::Data).unwrap();
    assert_eq!(data_len, stale_data.len() as u64, "the .data is the stale short copy");

    let idx_buf = store.read_all(id, SegmentPart::Idx).unwrap();

    // ORACLE 1: the first 2 records are wholly within the stale .data ⇒ they read
    // and decode fine (the in-bounds prefix is served correctly).
    for s in id..id + 2 {
        let e = lookup(&idx_buf, id, s).expect("entry present");
        assert!(
            e.offset as u64 + e.len as u64 <= data_len,
            "record {s} is within the stale .data"
        );
        let frame = store
            .read_range(id, SegmentPart::Data, e.offset as u64, e.len as u64)
            .expect("in-range frame reads");
        assert_eq!(decode_data_frame(&frame).unwrap().seq, s);
    }

    // ORACLE 2: the .idx entries for the last 2 records point PAST the stale .data
    // end ⇒ read_range returns RangeOutOfBounds, never a silent short read and
    // never neighbor bytes decoded as the frame. The record resolves from the WAL
    // instead (modeled as "the segment read errored out-of-bounds").
    for s in id + 2..id + n {
        let e = lookup(&idx_buf, id, s).expect("entry present in the full .idx");
        assert!(
            e.offset as u64 + e.len as u64 > data_len,
            "record {s}'s .idx entry overruns the stale .data"
        );
        match store.read_range(id, SegmentPart::Data, e.offset as u64, e.len as u64) {
            Err(StoreError::RangeOutOfBounds) => {}
            other => panic!(
                "an .idx entry past the .data end must be RangeOutOfBounds for seq {s}, \
                 got {other:?}"
            ),
        }
    }
}

// ===========================================================================
// F-SEG-DATA-WITHOUT-IDX-LISTED
// a stray .data with NO matching .idx survives a crash. Oracle: list() counts the
// segment only because .data exists, but resolve/read need the .idx too — a
// missing .idx makes the segment non-authoritative; reclaim/re-materialize cleans
// it; nothing panics.
// ===========================================================================

#[test]
fn f_seg_data_without_idx_listed() {
    let disk = FakeDisk::with_seed(0xDA7A0);
    let fs = disk.arc();
    fs.create_dir_all(Path::new(ROOT)).unwrap();
    let id: SegmentId = 42;
    let (data, _idx) = build_segment(id, 3);

    // Install a lone .data durably (no .idx ever written) — the crash leaves only
    // the .data on disk.
    let dtmp = data_path(id).with_extension("data.tmp");
    write_pending(&fs, &dtmp, &data);
    fs.open(&dtmp, OpenOpts::rw_existing())
        .unwrap()
        .sync_all()
        .unwrap();
    fs.sync_dir(Path::new(ROOT)).unwrap();
    fs.rename(&dtmp, &data_path(id)).unwrap();
    fs.sync_dir(Path::new(ROOT)).unwrap();
    disk.crash(TornDamage::None);
    disk.reset_power();

    let store = open_store(&disk);
    // ORACLE 1: list() counts the segment (it requires .data, which exists).
    assert_eq!(
        store.list().unwrap(),
        vec![id],
        "a stray .data is listed (list() keys on .data)"
    );
    assert!(store.exists(id, SegmentPart::Data));
    assert!(!store.exists(id, SegmentPart::Idx), "no matching .idx");

    // ORACLE 2: resolve/read need the .idx too — a missing .idx makes the segment
    // non-authoritative. Bulk-reading the .idx (the recovery re-index path) is a
    // clean NotFound, never a panic. len() on the absent .idx is also NotFound.
    assert!(
        matches!(
            store.read_all(id, SegmentPart::Idx),
            Err(StoreError::NotFound(_))
        ),
        "missing .idx ⇒ NotFound (non-authoritative segment)"
    );
    assert!(matches!(
        store.len(id, SegmentPart::Idx),
        Err(StoreError::NotFound(_))
    ));

    // ORACLE 3: reclaim/re-materialize cleans the stray .data idempotently; no
    // crash, and a second sweep finds nothing.
    store.delete(id).expect("stray .data reclaims cleanly");
    assert!(store.list().unwrap().is_empty(), "stray .data cleaned up");
    store.delete(id).expect("idempotent second sweep");
}

// NOTE on F-SEG-IDX-OFFSET-PAST-DATA: the catalog's "FaultFs returns a stale
// shorter .data while idx is newer" is realized here deterministically by writing
// a durably-shorter .data alongside the full .idx (rather than a FaultFs read
// glitch), which exercises the exact same code path — read_range checks
// `offset + len > metadata_len()` and returns RangeOutOfBounds — without any
// timing/flakiness. The four oracle assertions in f_seg_idx_offset_past_data fully
// cover the "offset/len past .data end ⇒ RangeOutOfBounds, never neighbor bytes"
// contract.
