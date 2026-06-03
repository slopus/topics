//! Phase-8B fault/crash batch — **in-place segment delete-flag flip** (DESIGN §7,
//! the segment side). A sealed-record delete flips a single trailing delete-flag
//! BYTE in the segment `.data` file in place (+ fsync), so the deletion is durable
//! in the segment itself and survives a WAL trim/checkpoint that drops the Delete
//! frame. The WAL Delete frame stays the append-only witness for not-yet-sealed
//! records + ordering; the in-memory mark, the WAL frame, and the on-disk flag must
//! agree, and recovery re-derives the deletion from the on-disk flag.
//!
//! These oracles live at the segment-store + segment-writer + topic-state seam, so
//! each test drives the REAL `SegmentWriter` / `LocalSegmentStore` / `TopicState`
//! through an in-memory `FakeDisk` / `FaultFs` (the Phase-8A harness, injected via
//! the `*_with` constructors). The engine's own `build_segment_writer` opens the
//! hot store with `RealFs` (never the injected FS), so — exactly like
//! `tests/fault_seg_seal.rs` — we wire the segment path's FS seam directly.
//!
//! Strategies:
//!   - F-SEG-DELFLIP-SURVIVES-WAL-TRIM — a sealed-record delete survives a
//!       checkpoint that trims the WAL Delete frame: the on-disk flag is the witness
//!       recovery reads back (no dependence on a retained Delete frame).
//!   - F-SEG-DELFLIP-CRASH-MID-FLIP — a crash mid-flip (byte written, fsync not
//!       returned, OR the flip never reached durable media) recovers correctly:
//!       live records intact, framing intact, a completed delete stays deleted, an
//!       un-landed flip simply reads live (never corrupts, never resurrects/skips).
//!   - F-SEG-DELFLIP-WHOLE-SEGMENT-CLEAR — a delete that clears ALL records of a
//!       segment drops the WHOLE segment in one op (unlink), not N per-record flips.
//!   - F-SEG-DELFLIP-PARTIAL-PER-RECORD — a partial delete flips the on-disk byte
//!       per-record and leaves the survivors live + the segment present.
//!
//! ```text
//! cargo test --features test-fs --test fault_seg_delete
//! ```

#![cfg(feature = "test-fs")]
#![allow(
    clippy::ptr_arg,
    clippy::manual_clamp,
    clippy::unusual_byte_groupings,
    clippy::doc_lazy_continuation
)]

use std::path::PathBuf;
use std::sync::Arc;

use serde_json::json;

use topics::clock::{SharedClock, TestClock};
use topics::config::SegmentConfig;
use topics::engine::segwriter::SegmentWriter;
use topics::engine::topic_state::{StoredRecord, TopicState};
use topics::storage::testfs::FakeDisk;
use topics::storage::{
    frame_is_deleted, lookup, Fs, LocalSegmentStore, SegmentPart, SegmentStore, TopicTier,
};
use topics::types::TopicConfig;

const SEG_ROOT: &str = "/data/topics/00000001";

fn test_clock(ms: i64) -> (SharedClock, Arc<TestClock>) {
    let tc = Arc::new(TestClock::new(ms));
    (tc.clone() as SharedClock, tc)
}

/// A HOT-only `SegmentWriter` backed by `fs`, sealing every `max_events` records.
/// Resident payloads are freed on seal so a sealed record's only home is the
/// segment (the case the on-disk delete flag governs).
fn seg_writer(fs: Arc<dyn Fs>, clock: SharedClock, max_events: u64) -> SegmentWriter {
    let hot = LocalSegmentStore::open_with(PathBuf::from(SEG_ROOT), fs)
        .expect("hot segment store opens through the injected fs");
    let tier = Arc::new(TopicTier::new(Box::new(hot), None));
    let cfg = SegmentConfig {
        max_events,
        max_bytes: 0,
        max_age_ms: 0,
        hot_retain_segments: u64::MAX,
        hot_retain_bytes: 0,
    };
    let mut w = SegmentWriter::new(tier, cfg, clock);
    w.set_evict_resident(true);
    w
}

fn seg_append(w: &mut SegmentWriter, seq: u64) -> Vec<u64> {
    w.append_record(
        seq,
        1_700_000_000_000 + seq as i64,
        None,
        Some("t"),
        &json!({ "v": seq }),
        &None,
    )
}

/// Read whether `seq`'s on-disk frame is flagged deleted, by re-reading the
/// segment's `.data`+`.idx` directly through `fs` (the recovery view).
fn ondisk_deleted(fs: &Arc<dyn Fs>, start_seq: u64, seq: u64) -> bool {
    let store = LocalSegmentStore::open_with(PathBuf::from(SEG_ROOT), fs.clone()).unwrap();
    let data = store.read_all(start_seq, SegmentPart::Data).unwrap();
    let idx = store.read_all(start_seq, SegmentPart::Idx).unwrap();
    let e = lookup(&idx, start_seq, seq).expect("seq present in idx");
    let lo = e.offset as usize;
    let hi = lo + e.len as usize;
    frame_is_deleted(&data[lo..hi])
}

// ===========================================================================
// F-SEG-DELFLIP-SURVIVES-WAL-TRIM
// ===========================================================================

/// A sealed-record delete flips the on-disk delete-flag byte; the deletion is then
/// re-derivable from the segment file alone. Simulating a WAL trim/checkpoint (the
/// Delete frame is GONE), recovery's on-disk scan re-marks exactly the deleted seq
/// in a fresh index — the deletion no longer depends on a retained WAL Delete frame.
#[test]
fn f_seg_delflip_survives_wal_trim() {
    let disk = FakeDisk::new();
    let fs = disk.arc();
    let (clock, _tc) = test_clock(1_700_000_000_000);
    let mut w = seg_writer(fs.clone(), clock, 1); // 1-per-segment ⇒ seqs 1..3 sealed.
    for seq in 1..=4u64 {
        seg_append(&mut w, seq);
    }
    assert_eq!(w.sealed_count(), 3, "seqs 1,2,3 sealed; 4 active");

    // Delete sealed seq 2 in place. The on-disk byte is the durable witness now —
    // modelling a checkpoint that trimmed the WAL Delete frame (no WAL in this seam).
    assert!(
        w.flag_sealed_deleted(2),
        "seq 2 sealed ⇒ on-disk flip issued"
    );

    // The on-disk flag is set for seq 2 only.
    assert!(ondisk_deleted(&fs, 2, 2), "seq 2 flagged deleted on disk");
    assert!(!ondisk_deleted(&fs, 1, 1), "seq 1 stays live on disk");
    assert!(!ondisk_deleted(&fs, 3, 3), "seq 3 stays live on disk");
    drop(w);

    // RECOVERY: a fresh writer over the SAME on-disk segments rebuilds its sealed
    // registry by re-feeding the (rebuilt-as-live) records — the existing on-disk
    // files are preserved (the seal `put` is skipped because the segment already
    // exists), so the flags survive. The on-disk scan enumerates the segment files
    // directly (not the in-memory registry), so it re-derives exactly seq 2.
    let mut w2 = seg_writer(fs.clone(), test_clock(0).0, 1);
    for seq in 1..=4u64 {
        seg_append(&mut w2, seq);
    }
    assert_eq!(
        w2.scan_ondisk_deleted(),
        vec![2],
        "recovery re-derives exactly the on-disk-flagged deletion (seq 2)"
    );
    // The live records are still byte-identical after the flip.
    assert_eq!(w2.resolve_sealed(1).unwrap().data, json!({"v":1}));
    assert_eq!(w2.resolve_sealed(3).unwrap().data, json!({"v":3}));
}

// ===========================================================================
// F-SEG-DELFLIP-CRASH-MID-FLIP
// ===========================================================================

/// A crash mid-flip: the delete-flag byte either lands fully (the sentinel) or not
/// at all (the old live byte) — a single sector-atomic write. We model BOTH crash
/// outcomes against the SAME pre-flip image and assert recovery is correct each way:
///   (a) flip landed   ⇒ on-disk scan reports the seq deleted; live records intact.
///   (b) flip lost      ⇒ on-disk scan reports nothing; the record reads LIVE (the
///       delete simply did not take durably — the in-memory mark + WAL frame still
///       cover it until the next checkpoint re-flips). Framing is intact either way.
#[test]
fn f_seg_delflip_crash_mid_flip() {
    // --- (a) the flip LANDED before the crash.
    {
        let disk = FakeDisk::new();
        let fs = disk.arc();
        let mut w = seg_writer(fs.clone(), test_clock(0).0, 2); // 2-per-seg.
        for seq in 1..=5u64 {
            seg_append(&mut w, seq);
        }
        // seg 1 = {1,2}, seg 2 = {3,4}, seq 5 active. Delete seq 3 (in seg 2).
        assert!(w.flag_sealed_deleted(3));
        // The flip + fsync returned ⇒ the byte is durable. Recovery sees it.
        assert!(ondisk_deleted(&fs, 3, 3), "landed flip is durable on disk");
        let mut w2 = seg_writer(fs.clone(), test_clock(0).0, 2);
        for seq in 1..=5u64 {
            seg_append(&mut w2, seq);
        }
        assert_eq!(w2.scan_ondisk_deleted(), vec![3], "deleted stays deleted");
        // The neighbour seq 4 (same segment) is intact + live; framing not corrupted.
        assert!(!ondisk_deleted(&fs, 3, 4), "neighbour seq 4 stays live");
        assert_eq!(w2.resolve_sealed(4).unwrap().data, json!({"v":4}));
        assert_eq!(w2.resolve_sealed(1).unwrap().data, json!({"v":1}));
    }

    // --- (b) the flip was LOST to the crash (byte never reached durable media). We
    // model this by simply NOT issuing the flip on the surviving image, then asserting
    // the record reads LIVE and decodes — the framing is untouched, nothing is
    // resurrected or skipped, and a later re-flip is still possible (idempotent).
    {
        let disk = FakeDisk::new();
        let fs = disk.arc();
        let mut w = seg_writer(fs.clone(), test_clock(0).0, 2);
        for seq in 1..=5u64 {
            seg_append(&mut w, seq);
        }
        // The mid-flip crash dropped the byte: seq 3 still reads LIVE on disk.
        assert!(
            !ondisk_deleted(&fs, 3, 3),
            "un-landed flip ⇒ record reads live"
        );
        let mut w2 = seg_writer(fs.clone(), test_clock(0).0, 2);
        for seq in 1..=5u64 {
            seg_append(&mut w2, seq);
        }
        assert!(
            w2.scan_ondisk_deleted().is_empty(),
            "no deletion recovered from a lost flip (never resurrects/skips)"
        );
        assert_eq!(
            w2.resolve_sealed(3).unwrap().data,
            json!({"v":3}),
            "live + intact"
        );
        // A re-issued flip now lands cleanly (the delete is simply retried).
        assert!(w2.flag_sealed_deleted(3));
        assert!(ondisk_deleted(&fs, 3, 3), "re-flip lands");
    }
}

// ===========================================================================
// Topic-level: WHOLE-SEGMENT clear vs PARTIAL per-record flip + recovery scan.
// Drives the real TopicState (writer-backed) through the injected FS.
// ===========================================================================

/// Build a writer-backed `TopicState` whose segments live under `fs`, sealing every
/// `max_events`. (Mirrors `attach_segwriter`, but with the FS seam injected so the
/// segment path is testable under the hostile FS.)
fn writer_backed_topic(fs: Arc<dyn Fs>, clock: SharedClock, max_events: u64) -> TopicState {
    let mut state = TopicState::new("b".into(), 1, TopicConfig::default(), 1, 1);
    let hot = LocalSegmentStore::open_with(PathBuf::from(SEG_ROOT), fs).unwrap();
    let tier = Arc::new(TopicTier::new(Box::new(hot), None));
    let cfg = SegmentConfig {
        max_events,
        max_bytes: 0,
        max_age_ms: 0,
        hot_retain_segments: u64::MAX,
        hot_retain_bytes: 0,
    };
    let mut writer = SegmentWriter::new(tier, cfg, clock);
    writer.set_evict_resident(true);
    state.attach_segwriter(writer);
    state
}

fn rec(seq: u64) -> StoredRecord {
    StoredRecord {
        ts: 1_700_000_000_000 + seq as i64,
        node: None,
        tag: Some("t".into()),
        data: json!({ "v": seq }),
        meta: None,
        bytes: 16,
        deleted: false,
        payload_resident: true,
        hops: 0,
    }
}

// ===========================================================================
// F-SEG-DELFLIP-WHOLE-SEGMENT-CLEAR
// ===========================================================================

/// A `before_seq` delete that clears EVERY record of the oldest sealed segments
/// drops those WHOLE segments in one op (unlink) rather than flipping N per-record
/// bytes — and a recovery scan over the surviving segments finds no flagged record
/// (the cleared ones are gone entirely).
#[test]
fn f_seg_delflip_whole_segment_clear() {
    let disk = FakeDisk::new();
    let fs = disk.arc();
    let b = writer_backed_topic(fs.clone(), test_clock(0).0, 2); // 2-per-segment.

    // Append seqs 1..=6 (seg{1,2}, seg{3,4}, active{5,6 once 7 appends}).
    let now = 1_700_000_000_000;
    for seq in 1..=6u64 {
        b.append(vec![rec(seq)], now);
    }
    // Force the active {5,6} to seal so all of 1..=4 + 5..=6 are sealed segments.
    b.append(vec![rec(7)], now); // active becomes {7}; 5,6 sealed.
    let sealed_before = b.segwriter.as_ref().unwrap().lock().sealed_count();
    assert!(
        sealed_before >= 3,
        "at least segs {{1,2}},{{3,4}},{{5,6}} sealed"
    );

    // Delete everything before seq 5 ⇒ clears segs {1,2} and {3,4} ENTIRELY.
    let deleted = b.apply_delete(Some(5), None, Some(8), now);
    assert_eq!(deleted, 4, "seqs 1..=4 deleted");

    // The two fully-cleared segments are DROPPED (whole-segment optimization): their
    // files are gone from disk, not left as N flagged records.
    let store = LocalSegmentStore::open_with(PathBuf::from(SEG_ROOT), fs.clone()).unwrap();
    assert!(
        !store.exists(1, SegmentPart::Data) && !store.exists(3, SegmentPart::Data),
        "whole cleared segments {{1,2}},{{3,4}} are unlinked, not per-record flipped"
    );
    // The surviving seg {5,6} is intact, and no on-disk record there is flagged.
    assert!(store.exists(5, SegmentPart::Data), "survivor segment kept");
    let scan = b.segwriter.as_ref().unwrap().lock().scan_ondisk_deleted();
    assert!(
        scan.is_empty(),
        "no per-record flips left after a whole-segment clear"
    );

    // Live state: seqs 5,6,7 remain; earliest advanced to 5.
    let st_count = b.count();
    assert_eq!(st_count, 3, "3 live records remain (5,6,7)");
    assert_eq!(
        b.earliest_seq(),
        5,
        "earliest advanced past the cleared prefix"
    );
}

// ===========================================================================
// F-SEG-DELFLIP-PARTIAL-PER-RECORD + recovery re-derives the deletion
// ===========================================================================

/// A `match`/seq delete that removes SOME records of a sealed segment (not all)
/// flips the on-disk byte PER-RECORD, leaving the segment present and its survivors
/// live. Recovery over the surviving segment re-derives exactly the deleted seqs
/// from the on-disk flags (independently of any WAL Delete frame).
#[test]
fn f_seg_delflip_partial_per_record() {
    let disk = FakeDisk::new();
    let fs = disk.arc();
    let b = writer_backed_topic(fs.clone(), test_clock(0).0, 4); // 4-per-segment.

    let now = 1_700_000_000_000;
    for seq in 1..=5u64 {
        b.append(vec![rec(seq)], now);
    }
    // seg {1,2,3,4} sealed once seq 5 appends; seq 5 is active.
    let sw = b.segwriter.as_ref().unwrap();
    assert_eq!(
        sw.lock().sealed_count(),
        1,
        "one sealed segment {{1,2,3,4}}"
    );

    // Delete an interior subset of the sealed segment via explicit seqs (the queue
    // ack / dead-letter path): remove 2 and 4 only. The segment is NOT fully dead, so
    // it stays on disk and each removed sealed record is flipped per-record.
    let deleted = b.delete_seqs(&[2, 4], now);
    assert_eq!(deleted, 2, "seqs 2,4 deleted");

    // The segment file still exists (partial clear — not unlinked).
    let store = LocalSegmentStore::open_with(PathBuf::from(SEG_ROOT), fs.clone()).unwrap();
    assert!(
        store.exists(1, SegmentPart::Data),
        "partially-cleared segment kept"
    );
    // On disk exactly seqs 2 and 4 are flagged; 1 and 3 stay live.
    assert!(ondisk_deleted(&fs, 1, 2), "seq 2 flagged on disk");
    assert!(ondisk_deleted(&fs, 1, 4), "seq 4 flagged on disk");
    assert!(!ondisk_deleted(&fs, 1, 1), "seq 1 live on disk");
    assert!(!ondisk_deleted(&fs, 1, 3), "seq 3 live on disk");
    assert_eq!(
        sw.lock().scan_ondisk_deleted(),
        vec![2, 4],
        "per-record flips are exactly the deleted seqs"
    );

    // RECOVERY: rebuild a fresh writer-backed topic that pre-seeds its index with ALL
    // sealed records as LIVE (as if a WAL trim lost the Delete frame AND a snapshot
    // had captured them live), then re-derive the deletions from the on-disk flags.
    let b2 = {
        let mut state = TopicState::new("b".into(), 1, TopicConfig::default(), 1, 1);
        // Seed the index with seqs 1..=5 all live (simulating a pre-delete snapshot).
        for seq in 1..=5u64 {
            state.append(vec![rec(seq)], now);
        }
        let hot = LocalSegmentStore::open_with(PathBuf::from(SEG_ROOT), fs.clone()).unwrap();
        let tier = Arc::new(TopicTier::new(Box::new(hot), None));
        let cfg = SegmentConfig {
            max_events: 4,
            max_bytes: 0,
            max_age_ms: 0,
            hot_retain_segments: u64::MAX,
            hot_retain_bytes: 0,
        };
        let mut writer = SegmentWriter::new(tier, cfg, test_clock(0).0);
        writer.set_evict_resident(true);
        // attach pre-seeds the writer from the (all-live) index; the existing on-disk
        // segment is preserved (put skipped because the files already exist).
        state.attach_segwriter(writer);
        state
    };
    // The rebuilt index thinks all 5 are live...
    assert_eq!(b2.count(), 5, "pre-recovery index has all 5 live");
    // ...until the on-disk scan re-derives the deletions:
    b2.apply_ondisk_segment_deletes_on_recovery();
    assert_eq!(
        b2.count(),
        3,
        "recovery re-marked seqs 2,4 deleted from the flags"
    );
    // The deleted seqs are now holes; the live ones survive.
    let index = b2.index.read();
    assert!(
        index.is_dead(2) && index.is_dead(4),
        "2,4 deleted post-recovery"
    );
    assert!(
        !index.is_dead(1) && !index.is_dead(3) && !index.is_dead(5),
        "1,3,5 live"
    );
}
