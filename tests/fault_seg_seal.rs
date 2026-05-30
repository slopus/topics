//! Phase-8B fault/crash batch — the four **segment-seal boundary** strategies
//! from the catalog (`/tmp/streams-fault-catalog.json` / `docs/FAULT_TESTING.md`),
//! each one test fn named after its catalog id. Every test asserts the CORRECT
//! crash-consistency behavior via the durability contract, reusing the Phase-8A
//! harness (`FakeDisk` / `FaultFs` from `streams::storage::testfs`, the real WAL +
//! recovery via the `*_with` constructors, the real `SegmentWriter` + segment
//! store wired through an injectable `Arc<dyn Fs>`).
//!
//! Strategies:
//!   - F-WAL-ROTATE-CRASH-PRE-SEAL — crash during WAL rotation (after the old-file
//!       fdatasync, before / around the new file create). Old file fully durable;
//!       recovery resumes from the highest WAL file; no acked frame lost across the
//!       rotation boundary.
//!   - F-WAL-ROTATE-NEW-FILE-TORN  — the new rotated file's first append is torn on
//!       crash. The torn tail truncates to a clean prefix; the old file's data is
//!       intact; recovery picks the highest-index file and truncates its torn tail.
//!   - F-SEG-ENOSPC-SEAL           — ENOSPC sealing a segment to the hot store: the
//!       seal fails gracefully, the payloads stay resident (resolvable), no data
//!       loss — the WAL is the source of truth and materialization is deferred.
//!   - F-CLOCK-FORWARD-SEG-AGE     — a forward clock jump triggers an off-schedule
//!       age-based seal. The seal still produces dense, gapless sealed segments;
//!       no record is lost or duplicated.
//!
//! ```text
//! cargo test --features test-fs --test fault_seg_seal
//! ```

#![cfg(feature = "test-fs")]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;

use streams::clock::{SharedClock, TestClock};
use streams::config::SegmentConfig;
use streams::engine::segwriter::{SealedResolve, SegmentWriter};
use streams::storage::testfs::{FakeDisk, FaultFs, FaultKind, FaultOp, TornDamage};
use streams::storage::wal::{Wal, WalConfig, WalReader, WalRecord};
use streams::storage::{BoxTier, Fs, LocalSegmentStore};

// ===========================================================================
// Shared plumbing (mirrors tests/crash_oracle.rs + tests/fault_wal_append_b.rs)
// ===========================================================================

const DATA_DIR: &str = "/data";

fn wal_dir() -> PathBuf {
    PathBuf::from(DATA_DIR).join("wal")
}

/// A direct WAL writer config with a fast group-commit window so durable
/// `append`s ack quickly. `file_size` is the per-file rotation threshold and is
/// set tiny by the rotation tests so a few frames force a rotation.
fn wal_cfg(file_size: u64) -> WalConfig {
    let mut c = WalConfig::new(DATA_DIR);
    c.gc_min = Duration::from_micros(50);
    c.gc_max = Duration::from_micros(200);
    c.file_size = file_size;
    c
}

/// One durable Append record (same shape as the crash_oracle `ap` helper).
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

/// The sorted list of `wal-*.log` files on the (post-crash) disk image.
fn wal_files(disk: &FakeDisk) -> Vec<PathBuf> {
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
    files
}

/// Replay every `wal-*.log` file on the (post-crash) disk image, stopping each
/// file at its torn tail — exactly the recovery read path. Returns the recovered
/// Append seqs across every file in WAL order.
fn recover_seqs(disk: &FakeDisk) -> Vec<u64> {
    let fs = disk.arc();
    let mut seqs = Vec::new();
    for f in wal_files(disk) {
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

/// Make the WAL directory's NAMES durable (the create+dir-fsync production does at
/// open / rotation, modeled explicitly so the files survive a crash).
fn sync_wal_dir(disk: &FakeDisk) {
    let _ = disk.arc().sync_dir(&wal_dir());
}

// ===========================================================================
// Segment-writer plumbing (unit-level, for the seal strategies)
// ===========================================================================
//
// The engine's `build_segment_writer` opens the hot store via
// `LocalSegmentStore::open` (always `RealFs`, never the injected `Arc<dyn Fs>`),
// so a `FakeDisk`/`FaultFs` installed through `Engine::with_data_dir_fs` does NOT
// reach the segment path — a durable box in that harness simply runs with no
// segment writer (resident-only). To exercise the segment-SEAL boundary under an
// injected FS we therefore drive the REAL `SegmentWriter` directly over a
// `BoxTier` whose hot store is opened with the hostile `Arc<dyn Fs>` — the exact
// production seal/put/resolve code, just with the FS seam injected.

const SEG_ROOT: &str = "/data/boxes/00000001";

fn test_clock(ms: i64) -> (SharedClock, Arc<TestClock>) {
    let tc = Arc::new(TestClock::new(ms));
    (tc.clone() as SharedClock, tc)
}

/// A `SegmentWriter` over a HOT-only `BoxTier` backed by `fs`, with the given seal
/// caps and clock. `max_events`/`max_age_ms` drive the seal triggers under test.
fn seg_writer(
    fs: Arc<dyn Fs>,
    clock: SharedClock,
    max_events: u64,
    max_age_ms: u64,
) -> SegmentWriter {
    let hot = LocalSegmentStore::open_with(PathBuf::from(SEG_ROOT), fs)
        .expect("hot segment store opens through the injected fs");
    let tier = Arc::new(BoxTier::new(Box::new(hot), None));
    let cfg = SegmentConfig {
        max_events,
        max_bytes: 0,
        max_age_ms,
        hot_retain_segments: u64::MAX,
        hot_retain_bytes: 0,
    };
    let mut w = SegmentWriter::new(tier, cfg, clock);
    // Free resident payloads on seal so a successful seal is observable as "no
    // longer resident, served from the segment/cache"; a FAILED seal keeps them.
    w.set_evict_resident(true);
    w
}

/// Append one record to a `SegmentWriter`, returning the seqs it sealed.
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

// ===========================================================================
// F-WAL-ROTATE-CRASH-PRE-SEAL
// ===========================================================================

/// Crash during WAL file rotation: every frame written before the rotation is
/// durable (the rotation fdatasyncs the old file), so after a power loss the old
/// file holds a contiguous acked prefix and recovery resumes from the highest WAL
/// file with no frame lost across the rotation boundary.
///
/// The catalog `inject_how` names a `wal::after_rotate_fsync` failpoint, but the
/// engine has no such site; the rotation boundary is exercised here at the FS
/// seam — a tiny `file_size` forces a real rotation mid-burst, and `FakeDisk`
/// decides what survives the crash (the rotation's own `fdatasync` is the
/// durability barrier the oracle relies on).
#[test]
fn f_wal_rotate_crash_pre_seal() {
    // A frame is ~70 bytes; a 160-byte file holds ~2 frames before rotating, so a
    // burst of durable appends rotates several times.
    let disk = FakeDisk::new();
    let wal = Wal::open_at_with(disk.arc(), wal_cfg(160), 1, 0).unwrap();
    let w = wal.writer();
    // Every append is durable ⇒ blocks until its group fsync returns ⇒ acked. The
    // rotation `fdatasync`s the old file before opening the next, so an acked
    // frame is durable on whichever file it landed on.
    for seq in 1..=10 {
        w.append(ap(seq), true).unwrap();
    }
    // The rotations created several files; make their NAMES durable (the dir
    // fsync production does post-rotation).
    sync_wal_dir(&disk);
    // Power loss right at the rotation boundary: freeze + drop un-fsynced bytes.
    disk.crash(TornDamage::None);
    drop(wal);

    // The burst really did rotate (more than one WAL file exists).
    assert!(
        wal_files(&disk).len() >= 2,
        "the tiny file_size forced a rotation: {:?}",
        wal_files(&disk)
    );

    // Every acked durable seq survives, dense and contiguous across the rotation
    // boundary — no frame lost when the active file rolled over.
    let seqs = recover_seqs(&disk);
    assert_eq!(
        seqs,
        (1..=10).collect::<Vec<_>>(),
        "all acked seqs survive across the rotation boundary"
    );

    // Idempotent: replaying the same crashed image again yields the same set.
    assert_eq!(recover_seqs(&disk), seqs, "rotation recovery is idempotent");
}

// ===========================================================================
// F-WAL-ROTATE-NEW-FILE-TORN
// ===========================================================================

/// The first append into the freshly-rotated WAL file is torn on a power loss
/// (it was still pending — non-durable — when the crash hit). The torn tail of
/// the new file truncates to a clean prefix; the OLD file's fsynced data is
/// intact; recovery replays the highest-index file's valid prefix and never
/// materializes the torn frame as a record.
#[test]
fn f_wal_rotate_new_file_torn() {
    for &damage in &[
        TornDamage::PrefixTruncate,
        TornDamage::ZeroSector,
        TornDamage::Garble,
    ] {
        let disk = FakeDisk::with_seed(0x5EA1 ^ damage as u64);
        let wal = Wal::open_at_with(disk.arc(), wal_cfg(160), 1, 0).unwrap();
        let w = wal.writer();
        // A durable burst that forces a rotation: every frame up to the rotation
        // is fsynced (acked) and lands durably on its file.
        for seq in 1..=6 {
            w.append(ap(seq), true).unwrap();
        }
        let files_before = wal_files(&disk).len();
        assert!(files_before >= 2, "the burst rotated: {files_before} files");

        // Now a NON-durable append: it buffers into the current (highest) file but
        // is never group-fsynced, so it is the in-flight tail a torn write damages.
        let _ = w.submit(ap(7), false);
        std::thread::sleep(Duration::from_millis(5));
        sync_wal_dir(&disk);
        // Power loss tearing the last pending write (the new file's torn first/last
        // append) — prefix-truncate / zero-sector / garble.
        disk.crash(damage);
        drop(wal);

        // The recovered seqs are a dense prefix of 1..=7 with no torn frame
        // misread: the 6 fsynced frames survive across the rotation, and the torn
        // frame 7 truncates (it may be entirely gone or its file truncates to the
        // clean prefix before it).
        let seqs = recover_seqs(&disk);
        for (i, s) in seqs.iter().enumerate() {
            assert_eq!(
                *s,
                i as u64 + 1,
                "dense prefix, no torn/garbled frame misread ({damage:?}): {seqs:?}"
            );
        }
        assert!(
            seqs.len() >= 6,
            "the 6 fsynced-before-the-torn frames all survive ({damage:?}): {seqs:?}"
        );
        assert!(
            seqs.len() <= 7,
            "never a fabricated frame beyond the torn one ({damage:?}): {seqs:?}"
        );
        // The old (lowest-index) file's fsynced data is fully intact — its frames
        // are all present in the recovered set.
        assert!(
            wal_files(&disk).len() >= 2,
            "the rotation files all survive the crash ({damage:?})"
        );
        // Idempotent re-replay.
        assert_eq!(
            recover_seqs(&disk),
            seqs,
            "torn-new-file recovery is idempotent ({damage:?})"
        );
    }
}

// ===========================================================================
// F-SEG-ENOSPC-SEAL
// ===========================================================================

/// ENOSPC sealing a segment to the HOT store: the seal's `put` fails, so the
/// `SegmentWriter` keeps the payloads resident (reports NO sealed seqs), nothing
/// is registered as sealed, and the records stay fully resolvable. The WAL is the
/// source of truth — a failed materialization is never data loss; it is deferred
/// to recovery / a re-checkpoint.
#[test]
fn f_seg_enospc_seal() {
    // ENOSPC on EVERY `write_at` (a full disk): the segment store's `.data`/`.idx`
    // tmp writes can never land, so the seal `put` fails.
    let disk = FakeDisk::new();
    let faulty: Arc<dyn Fs> =
        FaultFs::new(disk.arc(), FaultOp::WriteAt, FaultKind::Enospc, 0, false).arc();
    let (clock, _tc) = test_clock(1_700_000_000_000);
    // max_events=3 ⇒ the 4th append seals the first three before accepting it.
    let mut w = seg_writer(faulty, clock, 3, 0);

    // Fill the active segment to the cap (seqs 1..=3): no seal yet.
    for seq in 1..=3 {
        assert!(
            seg_append(&mut w, seq).is_empty(),
            "no seal until the cap is exceeded"
        );
    }
    // The 4th append crosses the cap ⇒ seal-before-append ⇒ `put` ⇒ ENOSPC. The
    // seal must fail GRACEFULLY: no sealed seqs reported, so the caller frees
    // nothing and the payloads stay resident.
    let sealed = seg_append(&mut w, 4);
    assert!(
        sealed.is_empty(),
        "ENOSPC seal reports no sealed seqs (payloads stay resident): {sealed:?}"
    );
    // No segment was registered (the put failed before the sealed registry push).
    assert!(
        w.sealed_segments().is_empty(),
        "a failed seal registers no segment"
    );

    // No data loss: every record 1..=3 from the (failed) seal is still resolvable
    // resident — `resolve_sealed_fast` reports them as NotSealed (the caller falls
    // back to the resident slot, which still holds the payload). The WAL remains
    // the source of truth, so the live records survive the ENOSPC entirely.
    for seq in 1..=3 {
        assert!(
            matches!(w.resolve_sealed_fast(seq), SealedResolve::NotSealed),
            "seq {seq} is NOT sealed after the failed put — resolved from the \
             resident slot (no data loss)"
        );
    }
    // The active segment now holds seq 4 (the seal failed but the append proceeded
    // onto a fresh active builder), gapless and ready to seal once space returns.
    assert_eq!(w.active_start(), Some(4), "a fresh active segment holds seq 4");
}

// ===========================================================================
// F-CLOCK-FORWARD-SEG-AGE
// ===========================================================================

/// A forward clock jump (e.g. across a restart) makes the age trigger fire on the
/// NEXT append, sealing the partially-filled active segment off its usual
/// schedule. The off-schedule seal still produces a dense, gapless sealed segment
/// — no record lost or duplicated — and the fresh active segment continues
/// exactly at the next seq.
#[test]
fn f_clock_forward_seg_age() {
    let disk = FakeDisk::new();
    let (clock, tc) = test_clock(1_700_000_000_000);
    // A large event cap so ONLY the age trigger can seal; max_age_ms=1000.
    let mut w = seg_writer(disk.arc(), clock, 1_000_000, 1_000);

    // Three appends well within the age window: nothing seals yet.
    for seq in 1..=3 {
        assert!(
            seg_append(&mut w, seq).is_empty(),
            "no age seal inside the window"
        );
    }
    assert!(w.sealed_segments().is_empty(), "nothing sealed yet");
    assert_eq!(w.active_start(), Some(1), "active segment started at seq 1");

    // Clock jumps far forward (past max_age_ms) — the kind of jump a restart with a
    // forward wall-clock skew produces. The NEXT append sees the active segment as
    // aged-out and seals it before accepting seq 4.
    tc.advance(60_000);
    let sealed = seg_append(&mut w, 4);
    assert_eq!(
        sealed,
        vec![1, 2, 3],
        "the forward clock jump seals the aged active segment (seqs 1..=3), dense \
         and gapless"
    );

    // Exactly one sealed segment, covering exactly the boundary crossed — gapless,
    // no record lost or duplicated by the off-schedule seal.
    let segs = w.sealed_segments();
    assert_eq!(segs.len(), 1, "one sealed segment from the age-triggered seal");
    assert_eq!(segs[0].start_seq, 1, "sealed segment starts at seq 1");
    assert_eq!(segs[0].end_seq, 3, "sealed segment ends at seq 3 (dense 1..=3)");

    // The sealed records resolve from the segment/cache (their resident payloads
    // were freed on seal), proving the seal actually materialized them — no loss.
    for seq in 1..=3 {
        assert!(
            matches!(w.resolve_sealed_fast(seq), SealedResolve::Hit(_)),
            "sealed seq {seq} resolves from the segment cache after the age seal"
        );
    }
    // The fresh active segment continues exactly at seq 4 — no duplication, no gap.
    assert_eq!(
        w.active_start(),
        Some(4),
        "the post-seal active segment continues at seq 4"
    );

    // A second forward jump + append seals seq 4 too; the boundaries stay dense and
    // contiguous (4..=4 follows 1..=3 with no overlap or hole).
    tc.advance(60_000);
    let sealed2 = seg_append(&mut w, 5);
    assert_eq!(sealed2, vec![4], "the next aged seal covers exactly seq 4");
    let segs = w.sealed_segments();
    assert_eq!(segs.len(), 2, "two dense, non-overlapping sealed segments");
    assert_eq!(
        (segs[0].start_seq, segs[0].end_seq, segs[1].start_seq, segs[1].end_seq),
        (1, 3, 4, 4),
        "sealed segments are contiguous and gapless across the off-schedule seals"
    );
}
