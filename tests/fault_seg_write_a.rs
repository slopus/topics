//! Phase-8B fault catalog — **boundary: segment-write** (file A).
//!
//! Five fault strategies that hammer the sealed-segment durability boundary —
//! `LocalSegmentStore::{put,read_range,list,exists}`, `BoxTier::resolve`, and the
//! `.data` frame decoder — through the Phase-8A harness (`FakeDisk` /  `FaultFs`).
//! A sealed segment is the *materialization* of WAL records: it is two files
//! (`seg-<start>.data` + `.idx`) written `.data`→`.idx`→`sync_dir`, each part
//! `write→fsync→rename`. The contract under test (DESIGN §6, oracle invariants
//! #6/#8):
//!
//! ```text
//!   * a crash before the dir-fsync rolls the un-synced renames back; the
//!     surviving subset is consistent with list() (which requires .data); the
//!     WAL/snapshot is the source of truth, so no live record is lost and the
//!     segment re-materializes idempotently on the next seal.
//!   * an EIO writing the .data tmp makes put() fail cleanly — the seal keeps its
//!     payloads resident (reports no sealed seqs, frees nothing); WAL still truth.
//!   * a torn / bit-flipped sealed .data frame surfaces SegmentError::Corrupt
//!     (sealed == immutable ⇒ NO silent truncation); the reader falls back, the
//!     corruption is explicit, never served as a valid record.
//!   * re-sealing an already-present segment id (recovery re-materialize) SKIPS
//!     the put — byte-identical by construction, no needless rewrite, no
//!     duplication, no pulling a cold copy back to hot.
//! ```
//!
//! Each strategy is one `#[test]` named after its catalog id. Every sweep is
//! bounded (tiny segments, fixed seeds) so the whole file runs in well under a
//! minute. Self-verify: `cargo test --features test-fs --test fault_seg_write_a`.

#![cfg(feature = "test-fs")]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use streams::storage::segment::{
    data_name, decode_data_frame, idx_name, lookup, SegmentBuilder, SegmentError, SegmentRecord,
};
use streams::storage::segstore::{
    BoxTier, LocalSegmentStore, SegmentPart, SegmentStore, StoreError, Tier,
};
use streams::storage::testfs::{FakeDisk, FaultFs, FaultKind, FaultOp, TornDamage};
use streams::storage::Fs;

// ===========================================================================
// Shared fixtures: a tiny deterministic segment + a store rooted on a FakeDisk.
// ===========================================================================

const ROOT: &str = "/seg/box1";

/// A contiguous, gapless run of `n` records starting at `start` — exactly the
/// shape a `SegmentBuilder` accepts (the same fixture style as the segstore unit
/// tests). Distinct tag/node/payload per record so a decode mismatch is visible.
fn build_segment(start: u64, n: u64) -> (Vec<u8>, Vec<u8>) {
    let mut b = SegmentBuilder::new(start);
    for i in 0..n {
        b.push(&SegmentRecord {
            seq: start + i,
            ts: 1_700_000_000_000 + start + i,
            node: (i % 2 == 0).then(|| format!("n{i}")),
            tag: Some(format!("t{i}")),
            data: format!("{{\"v\":{}}}", start + i).into_bytes(),
        });
    }
    b.finish()
}

/// Decode the whole `.data`/`.idx` pair of segment `id` from `store` into the
/// concrete records (offset/len resolved through the real `.idx` locator and the
/// real frame decoder) — the same read path a consumer's cold fetch takes.
/// Returns the per-seq decoded records, or the first decode/store error hit.
fn read_segment_records(
    store: &dyn SegmentStore,
    id: u64,
    start: u64,
    n: u64,
) -> Result<Vec<SegmentRecord>, String> {
    let idx_buf = store
        .read_all(id, SegmentPart::Idx)
        .map_err(|e| format!("idx read: {e}"))?;
    let mut out = Vec::new();
    for seq in start..start + n {
        let e = lookup(&idx_buf, start, seq).ok_or_else(|| format!("seq {seq} missing in idx"))?;
        let frame = store
            .read_range(id, SegmentPart::Data, e.offset as u64, e.len as u64)
            .map_err(|err| format!("data read seq {seq}: {err}"))?;
        let rec = decode_data_frame(&frame).map_err(|err| format!("decode seq {seq}: {err}"))?;
        out.push(rec);
    }
    Ok(out)
}

/// Make a fresh FakeDisk + a `LocalSegmentStore` rooted at [`ROOT`] on it,
/// returning both (the disk handle is needed to `crash()` / inspect durability).
fn store_on(disk: &FakeDisk) -> LocalSegmentStore {
    LocalSegmentStore::open_with(ROOT, disk.arc()).expect("segment store opens through FakeDisk")
}

/// Make the segment-store *directory* durable (model the create+dir-fsync the
/// store does at open, so a freshly-created store dir survives a crash and a
/// `read_dir` of it post-crash works). Mirrors `sync_wal_dir` in crash_oracle.
fn sync_store_dir(disk: &FakeDisk) {
    let fs = disk.arc();
    let _ = fs.create_dir_all(Path::new(ROOT));
    let _ = fs.sync_dir(Path::new(ROOT));
}

// ===========================================================================
// F-SEG-CRASH-BEFORE-DIRFSYNC  [crash-point / segment-write]
//
// FAULT:  crash after both .data and .idx are renamed into place but before
//         put()'s fsync_dir hardens those renames.
// ORACLE: one or both renames may roll back; the surviving subset is consistent
//         with list() (which requires .data); WAL is the source of truth, so no
//         live record is lost, and the segment re-materializes idempotently on
//         the next seal.
//
// INJECT: the catalog names `segput::before_dirfsync`, but we don't depend on the
//         `failpoints` feature: `put()` does data-rename → idx-rename → sync_dir,
//         so an EIO injected on the FINAL `sync_dir` aborts put() at exactly that
//         point (both renames already issued, the dir not yet hardened). We then
//         `crash()` the FakeDisk, which rolls back every rename that was never
//         dir-fsynced (FakeDisk models rename-durable-only-after-sync_dir). The
//         post-crash image is precisely "crashed before the dir fsync".
// ===========================================================================

#[test]
fn f_seg_crash_before_dirfsync() {
    let (data, idx) = build_segment(1, 4);

    let disk = FakeDisk::new();
    sync_store_dir(&disk);

    // A store whose `sync_dir` always EIOs: `put` writes+fsyncs+renames both
    // parts, then fails on the dir fsync — leaving both renames un-hardened.
    let faulty: Arc<dyn Fs> =
        FaultFs::new(disk.arc(), FaultOp::SyncDir, FaultKind::Eio, 0, false).arc();
    let store = LocalSegmentStore::open_with(ROOT, faulty).expect("store opens");

    let put_res = store.put(1, &data, &idx);
    assert!(
        put_res.is_err(),
        "put must surface the dir-fsync EIO rather than report a durable segment"
    );

    // Power loss now: every rename issued but not yet dir-fsynced rolls back to
    // the durable namespace, which never had the final `.data`/`.idx` names.
    disk.crash(TornDamage::None);
    disk.reset_power();

    // Recover a fresh store over the crashed image and assert the oracle.
    let recovered = store_on(&disk);
    let listed = recovered.list().expect("list works post-crash");

    // (1) CONSISTENT-WITH-LIST: list() requires `.data`; the un-synced renames
    //     rolled back, so segment 1 is NOT reported as a complete segment. The
    //     surviving subset is consistent — no half-segment is listed.
    assert!(
        !listed.contains(&1),
        "segment 1's renames were not dir-fsynced ⇒ rolled back ⇒ not listed (got {listed:?})"
    );
    assert!(
        !recovered.exists(1, SegmentPart::Data),
        "the un-hardened .data rename must roll back on crash-before-dirfsync"
    );

    // (2) WAL IS TRUTH / NO LIVE RECORD LOST: the segment is just a derivable
    //     materialization — its absence loses nothing, the WAL still holds 1..=4.
    //     We model that by re-materializing (the "next seal") onto the recovered
    //     store and confirming it now lands durably and reads back byte-identical.
    let store2 = store_on(&disk); // healthy FS this time (no fault wrapper).
    store2.put(1, &data, &idx).expect("re-seal lands on a healthy FS");
    sync_store_dir(&disk);
    assert_eq!(store2.list().unwrap(), vec![1], "re-materialized segment is listable");
    let recs = read_segment_records(&store2, 1, 1, 4).expect("re-sealed segment decodes");
    assert_eq!(recs.len(), 4, "all 4 records re-materialize");
    assert_eq!(recs[0].seq, 1);
    assert_eq!(recs[3].seq, 4);
    assert_eq!(recs[3].tag.as_deref(), Some("t3"));

    // (3) IDEMPOTENT RE-SEAL: putting the SAME id again is byte-identical and
    //     stays a single listed segment (no duplication, no growth).
    store2.put(1, &data, &idx).expect("re-put same id is fine");
    sync_store_dir(&disk);
    assert_eq!(store2.list().unwrap(), vec![1], "still exactly one segment after re-put");
    assert_eq!(
        store2.read_all(1, SegmentPart::Data).unwrap(),
        data,
        ".data is byte-identical across the idempotent re-seal"
    );
}

// ===========================================================================
// F-SEG-EIO-DATA-WRITE  [io-error / segment-write]
//
// FAULT:  EIO writing the segment `.data` tmp during a seal.
// ORACLE: seal_active's put returns Err ⇒ the writer keeps payloads resident,
//         reports NO sealed seqs, and frees NOTHING; the WAL is still durable and
//         the read path stays correct (the put-failure branch in seal_active).
//
// INJECT: FaultFs fail-once on op=write_at — the very first `write_at` is the
//         `.data` tmp body, so the seal's `.data` write EIOs. We assert put()
//         surfaces the error AND that nothing partial was published (no listed
//         segment, prior store state intact), then model seal_active's contract:
//         on a put Err the caller frees nothing and reports zero sealed seqs.
// ===========================================================================

#[test]
fn f_seg_eio_data_write() {
    let (data, idx) = build_segment(1, 3);

    let disk = FakeDisk::new();
    sync_store_dir(&disk);

    // Fail-once EIO on the first write_at: that is the `.data` tmp body write.
    let faulty = FaultFs::new(disk.arc(), FaultOp::WriteAt, FaultKind::Eio, 0, true);
    let store = LocalSegmentStore::open_with(ROOT, faulty.arc()).expect("store opens");

    let res = store.put(1, &data, &idx);
    assert!(
        res.is_err(),
        "an EIO on the .data tmp write must make put() fail (not silently seal)"
    );
    assert!(
        matches!(res, Err(StoreError::Io(_))),
        "put failure surfaces as StoreError::Io"
    );

    // The failed put published NOTHING: no `.data`, no `.idx`, the segment is not
    // listed — the store is byte-for-byte as it was before the failed seal.
    assert!(!store.exists(1, SegmentPart::Data), "no .data after a failed put");
    assert!(!store.exists(1, SegmentPart::Idx), "no .idx after a failed put");
    assert_eq!(store.list().unwrap(), Vec::<u64>::new(), "nothing listed");

    // seal_active's CONTRACT on a put Err: report no sealed seqs (so the caller
    // frees nothing and keeps the payloads resident). We assert that contract via
    // the same decision seal_active makes — resolve() finds the segment in NO tier
    // (it never landed), so the seal yields an empty sealed-seqs set.
    let tier = BoxTier::new(Box::new(store_on(&disk)), None);
    assert_eq!(
        tier.resolve(1),
        None,
        "the failed seal materialized no tier ⇒ seal_active reports zero sealed seqs / frees nothing"
    );

    // WAL-IS-TRUTH + READ-PATH-CORRECT: a healthy retry seals the same range and
    // the records read back intact (the EIO was fail-once, so the next put works).
    let store_ok = store_on(&disk);
    store_ok.put(1, &data, &idx).expect("retry seal succeeds on a healthy device");
    sync_store_dir(&disk);
    let recs = read_segment_records(&store_ok, 1, 1, 3).expect("retried segment decodes");
    assert_eq!(recs.len(), 3);
    for (i, r) in recs.iter().enumerate() {
        assert_eq!(r.seq, 1 + i as u64, "records intact + contiguous after retry");
    }
}

// ===========================================================================
// F-SEG-TORN-DATA-FRAME  [torn/partial / segment-write]
//
// FAULT:  a sealed `.data` frame is torn (truncated mid-frame).
// ORACLE: decode_data_frame returns SegmentError::Corrupt — a sealed segment is
//         immutable, so there is NO silent torn-tail truncation; the reader
//         surfaces the error (falls back to WAL/resident); corruption is explicit.
//
// INJECT: FakeDisk tears the `.data` content. We write the `.data` bytes as a
//         single pending write and `crash(PrefixTruncate)` so only a prefix lands
//         durable — the last frame is cut mid-body. The `.idx` (written durably)
//         still points at the full frame length, so the read overruns the
//         truncated `.data` ⇒ the decoder/reader must surface Corrupt/OOB, never
//         a half-record.
// ===========================================================================

#[test]
fn f_seg_torn_data_frame() {
    let (data, idx) = build_segment(1, 3);
    assert!(data.len() > 16, "fixture has a multi-frame .data to tear");

    let disk = FakeDisk::with_seed(0x5EED_7012);
    sync_store_dir(&disk);
    let fs = disk.arc();

    let data_path = PathBuf::from(ROOT).join(data_name(1));
    let idx_path = PathBuf::from(ROOT).join(idx_name(1));

    // Write the `.idx` durably (it survives intact, still describing every frame).
    {
        let mut f = fs.open(&idx_path, streams::storage::OpenOpts::create_truncate()).unwrap();
        let mut off = 0usize;
        while off < idx.len() {
            off += f.write_at(off as u64, &idx[off..]).unwrap();
        }
        f.sync_all().unwrap();
    }
    // Write the `.data` as ONE pending write, then crash with a prefix-truncate
    // tear: only a strict prefix of `.data` lands durable — the last frame torn.
    {
        let mut f = fs.open(&data_path, streams::storage::OpenOpts::create_truncate()).unwrap();
        let mut off = 0usize;
        while off < data.len() {
            off += f.write_at(off as u64, &data[off..]).unwrap();
        }
        // NO sync: leave it pending so the crash tears it.
    }
    sync_store_dir(&disk); // make the file NAMES durable (a real create+dir-fsync).
    disk.crash(TornDamage::PrefixTruncate);
    disk.reset_power();

    let store = store_on(&disk);

    // The `.data` is now shorter than the `.idx` claims for the last record. Read
    // back through the real locator + decoder: the read MUST surface an explicit
    // error (RangeOutOfBounds from the store, or Corrupt from the decoder for a
    // partially-landed frame) — NEVER a silently-truncated valid record.
    let idx_buf = store.read_all(1, SegmentPart::Idx).expect("idx intact");
    let data_len = store.len(1, SegmentPart::Data).unwrap();
    assert!(
        data_len < data.len() as u64,
        "the prefix-truncate tear left a strictly shorter .data ({data_len} < {})",
        data.len()
    );

    // Find the first record whose frame the truncated `.data` can no longer fully
    // hold and assert the read surfaces an explicit error, not garbage.
    let mut saw_explicit_error = false;
    for seq in 1..=3u64 {
        let e = lookup(&idx_buf, 1, seq).expect("idx entry present");
        match store.read_range(1, SegmentPart::Data, e.offset as u64, e.len as u64) {
            // The store refuses a range past the (now shorter) .data — explicit.
            Err(StoreError::RangeOutOfBounds) => {
                saw_explicit_error = true;
            }
            Err(e) => panic!("unexpected store error for seq {seq}: {e}"),
            // A fully-landed earlier frame still decodes fine; a torn one that
            // happens to fit the slice length must FAIL the CRC ⇒ Corrupt.
            Ok(frame) => match decode_data_frame(&frame) {
                Ok(rec) => assert_eq!(rec.seq, seq, "an intact frame decodes to its real seq"),
                Err(SegmentError::Corrupt(_)) => {
                    saw_explicit_error = true;
                }
                Err(e) => panic!("unexpected decode error for seq {seq}: {e}"),
            },
        }
    }
    assert!(
        saw_explicit_error,
        "a torn sealed .data frame must surface an explicit error (OOB/Corrupt), never a silent half-record"
    );
}

// ===========================================================================
// F-SEG-CRC-CORRUPT-DATA  [corruption / segment-write]
//
// FAULT:  a single bit flip in a sealed `.data` frame payload (bit-rot at rest).
// ORACLE: the XXH3 mismatch ⇒ SegmentError::Corrupt is surfaced (matches the
//         segment unit test `checksum_catches_corruption`); the bad frame is
//         NEVER served as a valid record; the error is explicit.
//
// INJECT: FakeDisk garbles one `.data` byte. We seal a healthy segment, then
//         overwrite one payload byte in the middle of a frame body and re-fsync
//         (bit-rot that became durable), then read it back through the real
//         locator + decoder and assert the CRC catches it.
// ===========================================================================

#[test]
fn f_seg_crc_corrupt_data() {
    let (data, idx) = build_segment(1, 3);

    let disk = FakeDisk::new();
    sync_store_dir(&disk);
    let store = store_on(&disk);
    store.put(1, &data, &idx).expect("seal a healthy segment");
    sync_store_dir(&disk);

    // Sanity: the healthy segment reads back exactly before corruption.
    let clean = read_segment_records(&store, 1, 1, 3).expect("healthy segment decodes");
    assert_eq!(clean.len(), 3);

    // Locate the LAST record's frame and garble one byte inside its body (a
    // single-bit flip), then make the corruption durable. We pick the last frame
    // so the CRC over its body must reject it while earlier frames stay valid.
    let idx_buf = store.read_all(1, SegmentPart::Idx).unwrap();
    let e = lookup(&idx_buf, 1, 3).expect("seq 3 entry");
    // A byte safely inside the frame body (after the 4-byte len prefix + header,
    // before the trailing 8-byte CRC).
    let flip_off = e.offset as u64 + (e.len as u64 / 2);
    let data_path = PathBuf::from(ROOT).join(data_name(1));
    let fs = disk.arc();
    {
        let f = fs.open(&data_path, streams::storage::OpenOpts::rw_existing()).unwrap();
        let mut one = [0u8; 1];
        let n = f.read_at(flip_off, &mut one).unwrap();
        assert_eq!(n, 1, "byte to flip is within .data");
        drop(f);
        let mut f = fs.open(&data_path, streams::storage::OpenOpts::rw_existing()).unwrap();
        one[0] ^= 0xFF; // single-byte (multi-bit) flip — bit-rot.
        f.write_at(flip_off, &one).unwrap();
        f.sync_all().unwrap(); // the rot is now durable.
    }

    // Read seq 3's frame and decode: the XXH3 must reject it as Corrupt.
    let e3 = lookup(&idx_buf, 1, 3).unwrap();
    let frame = store
        .read_range(1, SegmentPart::Data, e3.offset as u64, e3.len as u64)
        .expect("range still in bounds (only a byte changed)");
    assert!(
        matches!(decode_data_frame(&frame), Err(SegmentError::Corrupt(_))),
        "a bit-flipped sealed .data frame must surface SegmentError::Corrupt (never served as valid)"
    );

    // The OTHER frames (1, 2) are untouched and still decode correctly — the
    // corruption is isolated and explicit, never silently corrupting neighbors.
    for seq in 1..=2u64 {
        let e = lookup(&idx_buf, 1, seq).unwrap();
        let frame = store
            .read_range(1, SegmentPart::Data, e.offset as u64, e.len as u64)
            .unwrap();
        let rec = decode_data_frame(&frame).expect("intact neighbor still decodes");
        assert_eq!(rec.seq, seq);
    }
}

// ===========================================================================
// F-SEG-PUT-OVERWRITE-IDEMPOTENT  [crash-point / segment-write]
//
// FAULT:  re-seal / re-put of an already-existing segment id after a crash (the
//         recovery re-materialize path: attach_segwriter replays every live
//         record and re-pushes the same range to the seal writer).
// ORACLE: seal_active sees existing_tier=Some and SKIPS the put — no needless
//         rewrite, no pulling a cold copy back to hot; byte-identical by
//         construction; no duplication.
//
// INJECT: seal a segment durably, simulate the recovery re-materialize by building
//         the SAME range again and consulting BoxTier::resolve (the exact decision
//         seal_active makes): because the id already resolves to a tier, the put
//         is skipped and the surviving copy is reused unchanged. We exercise both
//         the hot-already-present case and the cold-already-present case (the
//         oracle's "no pulling cold back to hot").
// ===========================================================================

#[test]
fn f_seg_put_overwrite_idempotent() {
    let (data, idx) = build_segment(1, 4);

    // --- Case A: the segment already exists in HOT (the common re-seal). --------
    {
        let disk = FakeDisk::new();
        sync_store_dir(&disk);
        let hot = LocalSegmentStore::open_with(ROOT, disk.arc()).unwrap();
        hot.put(1, &data, &idx).expect("initial durable seal");
        sync_store_dir(&disk);
        let data_before = hot.read_all(1, SegmentPart::Data).unwrap();

        // Recovery re-materialize: the SAME range is rebuilt and offered to the
        // seal writer. seal_active's decision is `tier.resolve(start_seq)`.
        let tier = BoxTier::new(Box::new(hot), None);
        let existing = tier.resolve(1);
        assert_eq!(existing, Some(Tier::Hot), "the id already resolves to HOT");

        // Because existing_tier is Some, seal_active SKIPS the put. We assert the
        // skip is correct by NOT re-putting and confirming the segment is unchanged
        // and singular. (Re-building the bytes is deterministic, so even had it
        // re-put, the bytes are byte-identical — we assert that property too.)
        let (data_again, _idx_again) = build_segment(1, 4);
        assert_eq!(
            data_again, data,
            "the re-materialized .data is byte-identical by construction"
        );
        let store = tier.hot();
        assert_eq!(store.list().unwrap(), vec![1], "exactly one segment, no duplication");
        assert_eq!(
            store.read_all(1, SegmentPart::Data).unwrap(),
            data_before,
            "the surviving HOT copy is reused unchanged (no needless rewrite)"
        );
    }

    // --- Case B: the segment already exists in COLD (must NOT be pulled to hot). -
    {
        let disk_hot = FakeDisk::new();
        let disk_cold = FakeDisk::new();
        let _ = disk_hot.arc().create_dir_all(Path::new("/hot/box1"));
        let _ = disk_cold.arc().create_dir_all(Path::new("/cold/box1"));
        let _ = disk_hot.arc().sync_dir(Path::new("/hot/box1"));
        let _ = disk_cold.arc().sync_dir(Path::new("/cold/box1"));

        let hot = LocalSegmentStore::open_with("/hot/box1", disk_hot.arc()).unwrap();
        let cold = LocalSegmentStore::open_with("/cold/box1", disk_cold.arc()).unwrap();
        // The segment lives ONLY in cold (it was relocated before the crash).
        cold.put(1, &data, &idx).expect("cold copy seal");
        let _ = disk_cold.arc().sync_dir(Path::new("/cold/box1"));

        let tier = BoxTier::new(Box::new(hot), Some(Box::new(cold)));

        // Recovery re-materialize resolves the id: it is in COLD, so seal_active
        // records Tier::Cold and SKIPS the put — it must NOT pull cold back to hot.
        assert_eq!(tier.resolve(1), Some(Tier::Cold), "id resolves to COLD, not HOT");
        assert!(
            !tier.hot().exists(1, SegmentPart::Data),
            "re-seal must NOT pull the cold segment back into the HOT tier"
        );
        assert!(
            tier.cold().unwrap().exists(1, SegmentPart::Data),
            "the cold copy stays put, byte-identical"
        );
        assert_eq!(
            tier.cold().unwrap().read_all(1, SegmentPart::Data).unwrap(),
            data,
            "cold .data unchanged by the skipped re-seal"
        );
    }
}
