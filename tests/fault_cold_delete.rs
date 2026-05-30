//! Phase-8B fault/crash batch — the three **cold-delete-hot boundary** strategies
//! from the catalog (`/tmp/streams-fault-catalog.json` / `docs/FAULT_TESTING.md`),
//! each one test fn named after its catalog id. Every test asserts the CORRECT
//! crash-consistency behavior at the cold-relocation `confirm_relocated` boundary
//! (flip the tier pointer → delete the now-redundant HOT copy), reusing the
//! Phase-8A harness (`FakeDisk` / `FaultFs` / `MonitorFs` from
//! `streams::storage::testfs`, the real `SegmentWriter` + `BoxTier` +
//! `LocalSegmentStore` wired through an injectable `Arc<dyn Fs>`, exactly as
//! `tests/fault_seg_seal.rs` does).
//!
//! Strategies:
//!   - F-COLD-CRASH-AFTER-FLIP-BEFORE-DELETE — power loss after the in-memory tier
//!       flip + the durable cold copy, before `hot().delete` completes. On restart
//!       the in-memory flip is lost but the durable cold copy exists; both copies
//!       survive, `BoxTier::resolve` re-derives the tier preferring HOT, the record
//!       is readable, and the relocator re-runs the idempotent drop — no loss,
//!       never zero copies.
//!   - F-COLD-EIO-DELETE-HOT — EIO deleting the hot copy after the flip. The
//!       `confirm_relocated` swallows + logs the failed delete (it will retry); both
//!       copies remain, the cold copy is authoritative by re-derivation, and an
//!       idempotent re-run drops the hot copy — no loss.
//!   - F-COLD-DELETE-BEFORE-FLIP-FORBIDDEN — adversarial: delete the HOT copy BEFORE
//!       the cold copy is durable (an ordering-violation injection). The
//!       `MonitorFs` live ordering guard is supposed to PANIC — proving a regression
//!       that drops the only durable copy is caught the instant it happens.
//!
//! The engine's `build_segment_writer` opens the hot store via
//! `LocalSegmentStore::open` (always `RealFs`, never the injected `Arc<dyn Fs>`),
//! so a `FakeDisk`/`FaultFs` installed through `Engine::with_data_dir_fs` does NOT
//! reach the segment / cold path. To exercise the cold-delete-hot boundary under an
//! injected FS we therefore drive the REAL `SegmentWriter` + `BoxTier` + the real
//! `copy_segment_to_cold` / `confirm_relocated` code directly over stores opened
//! with the hostile `Arc<dyn Fs>` — the exact production relocation code, just with
//! the FS seam injected.

#![cfg(feature = "test-fs")]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde_json::json;

use streams::clock::{SharedClock, TestClock};
use streams::config::SegmentConfig;
use streams::engine::segwriter::{copy_segment_to_cold, SegmentWriter};
use streams::storage::testfs::{FakeDisk, FaultFs, FaultKind, FaultOp, MonitorFs, TornDamage};
use streams::storage::{
    data_name, BoxTier, Fs, LocalSegmentStore, SegmentPart, SegmentStore, StoreError, Tier,
};

// ===========================================================================
// Plumbing (mirrors tests/fault_seg_seal.rs)
// ===========================================================================

/// HOT store root (under the data dir) and COLD store root (the cold tier folder),
/// both modeled on the SAME `FakeDisk` image so a single `crash()` is one device's
/// power loss across both tiers (the real contract: one machine, two folders).
const HOT_ROOT: &str = "/data/boxes/00000001";
const COLD_ROOT: &str = "/cold/boxes/00000001";

fn test_clock() -> SharedClock {
    Arc::new(TestClock::new(1_700_000_000_000))
}

/// A `SegmentWriter` over a HOT + COLD `BoxTier`, both stores opened through `fs`.
/// `max_events=1` seals every record into its own segment; `hot_retain_segments=1`
/// keeps only the newest sealed segment hot so the older ones are relocation
/// candidates. `set_cache_cap(0)` forces real segment reads (no cache shortcut).
fn seg_writer_cold(fs: Arc<dyn Fs>, clock: SharedClock) -> SegmentWriter {
    let hot = Box::new(
        LocalSegmentStore::open_with(PathBuf::from(HOT_ROOT), fs.clone())
            .expect("hot segment store opens through the injected fs"),
    );
    let cold: Box<dyn SegmentStore> = Box::new(
        LocalSegmentStore::open_with(PathBuf::from(COLD_ROOT), fs)
            .expect("cold segment store opens through the injected fs"),
    );
    let tier = Arc::new(BoxTier::new(hot, Some(cold)));
    let cfg = SegmentConfig {
        max_events: 1,
        max_bytes: 0,
        max_age_ms: 0,
        hot_retain_segments: 1,
        hot_retain_bytes: 0,
    };
    let mut w = SegmentWriter::new(tier, cfg, clock);
    w.set_cache_cap(0);
    w
}

/// Open a fresh `BoxTier` over `fs` (a "restart" — re-derives every tier pointer
/// from what is durably on disk, exactly as the engine does at boot). Used after a
/// `crash()` to prove the relocation re-runs idempotently from the surviving copies.
fn reopen_tier(fs: Arc<dyn Fs>) -> Arc<BoxTier> {
    let hot = Box::new(LocalSegmentStore::open_with(PathBuf::from(HOT_ROOT), fs.clone()).unwrap());
    let cold: Box<dyn SegmentStore> =
        Box::new(LocalSegmentStore::open_with(PathBuf::from(COLD_ROOT), fs).unwrap());
    Arc::new(BoxTier::new(hot, Some(cold)))
}

/// Append one record (its own segment under `max_events=1`); returns sealed seqs.
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

/// fsync the directories that hold the segment files so their NAMES (and bytes)
/// are durable on the `FakeDisk` image — the dir fsync `LocalSegmentStore::put`
/// already issues, but we also harden the WAL-less data dirs so a `crash()` keeps
/// the segment files. (`put` itself fsyncs the store root; this also covers parent
/// dirs the fake tracks by exact path.)
fn sync_seg_dirs(disk: &FakeDisk) {
    let fs = disk.arc();
    let _ = fs.sync_dir(Path::new(HOT_ROOT));
    let _ = fs.sync_dir(Path::new(COLD_ROOT));
}

// ===========================================================================
// F-COLD-CRASH-AFTER-FLIP-BEFORE-DELETE
// ===========================================================================

/// Power loss after the in-memory tier flip + the durable cold copy, but BEFORE
/// `hot().delete` completes (the catalog names a `cold::after_flip_before_delete`
/// failpoint; the in-memory flip is not persisted, so we model the crash by doing
/// the durable cold copy, then `crash()`ing the device before the hot unlink lands
/// durably — equivalently, the hot delete simply never happened).
///
/// ORACLE: on restart the in-memory flip is lost but the durable cold copy exists;
/// both copies survive, `BoxTier::resolve` re-derives the tier preferring HOT, the
/// record is readable, and the relocator re-runs the idempotent copy(no-op)+flip+
/// drop — no loss, never zero copies.
#[test]
fn f_cold_crash_after_flip_before_delete() {
    let disk = FakeDisk::new();
    let clock = test_clock();
    let mut w = seg_writer_cold(disk.arc(), clock);

    // Seal three single-record segments (ids 1,2,3); id 3 stays hot (retain=1),
    // ids 1,2 are relocation candidates.
    for seq in 1..=4u64 {
        seg_append(&mut w, seq);
    }
    assert_eq!(w.sealed_count(), 3, "three sealed segments (1,2,3); seq 4 active");
    let plan: Vec<u64> = w.relocation_plan().into_iter().map(|(id, _)| id).collect();
    assert_eq!(plan, vec![1, 2], "the two oldest sealed segments spill to cold");

    // Drive the relocation of segment 1 the way the engine does, but STOP at the
    // crash point: copy hot→cold (fsync'd + dir-fsync'd ⇒ durable on the fake), the
    // in-memory flip happens inside `confirm_relocated` — which then deletes hot.
    // To model a crash AFTER the durable cold copy but BEFORE the hot delete, we
    // run the copy, harden the dirs, then crash WITHOUT calling the hot delete.
    let tier = w.tier();
    copy_segment_to_cold(&tier, 1).expect("cold copy of segment 1");
    sync_seg_dirs(&disk);

    // Pre-crash sanity: both copies are now durable on disk.
    assert!(
        disk.durable_bytes(&data_path(HOT_ROOT, 1)).is_some(),
        "hot .data durable before the crash"
    );
    assert!(
        disk.durable_bytes(&data_path(COLD_ROOT, 1)).is_some(),
        "cold .data durable before the crash"
    );

    // Power loss before the hot delete: the in-memory flip (not persisted) is lost,
    // the durable cold copy survives, the still-present hot copy survives.
    disk.crash(TornDamage::None);
    drop(w);
    disk.reset_power();

    // --- "Restart": a fresh tier re-derives every pointer from what is on disk.
    let tier2 = reopen_tier(disk.arc());

    // NEVER ZERO COPIES: segment 1 resolves to a tier (it is readable).
    assert!(
        tier2.resolve(1).is_some(),
        "segment 1 readable after crash (never zero copies)"
    );
    // BOTH copies exist; resolve PREFERS HOT in the overlap window.
    assert!(
        tier2.hot().exists(1, SegmentPart::Data),
        "the un-deleted hot copy survived the crash"
    );
    assert!(
        tier2.cold().unwrap().exists(1, SegmentPart::Data),
        "the durable cold copy survived the crash"
    );
    assert_eq!(
        tier2.resolve(1),
        Some(Tier::Hot),
        "resolve prefers HOT while both copies exist (mid-relocation re-derivation)"
    );

    // NO LOSS: the record bytes are byte-identical from the surviving copies.
    let hot_data = tier2.hot().read_all(1, SegmentPart::Data).expect("hot .data readable");
    let cold_data = tier2
        .cold()
        .unwrap()
        .read_all(1, SegmentPart::Data)
        .expect("cold .data readable");
    assert_eq!(hot_data, cold_data, "hot and cold copies are byte-identical");
    assert!(!hot_data.is_empty(), "the segment payload survived");

    // The relocator RE-RUNS idempotently: the copy is a no-op (cold exists), then
    // the flip+drop completes and the hot copy is dropped — converging to exactly
    // one (cold) copy, still readable.
    copy_segment_to_cold(&tier2, 1).expect("idempotent re-copy is a no-op");
    let _ = tier2.hot().delete(1); // the deferred hot drop the relocator re-runs.
    sync_seg_dirs(&disk);
    assert!(!tier2.hot().exists(1, SegmentPart::Data), "hot copy dropped on re-run");
    assert_eq!(tier2.resolve(1), Some(Tier::Cold), "now resolves to cold");
    assert_eq!(
        tier2.cold().unwrap().read_all(1, SegmentPart::Data).unwrap(),
        cold_data,
        "still byte-identical after the drop (no loss)"
    );
}

// ===========================================================================
// F-COLD-EIO-DELETE-HOT
// ===========================================================================

/// EIO deleting the hot copy after the flip: `confirm_relocated` flips the tier
/// pointer (the durable cold copy already exists), then calls `hot().delete(id)` —
/// which EIOs on the underlying `remove_file`.
///
/// ORACLE: `confirm_relocated` logs the failed delete (it will retry — the error is
/// swallowed, NOT propagated); both copies still exist, the cold copy is
/// authoritative by re-derivation, no loss, and an idempotent drop re-runs cleanly.
#[test]
fn f_cold_eio_delete_hot() {
    let disk = FakeDisk::new();
    let clock = test_clock();

    // First, materialize the segments + the durable cold copy on a CLEAN disk (no
    // fault yet), so only the hot DELETE later sees the injected EIO.
    let mut w = seg_writer_cold(disk.arc(), clock.clone());
    for seq in 1..=4u64 {
        seg_append(&mut w, seq);
    }
    let tier = w.tier();
    copy_segment_to_cold(&tier, 1).expect("cold copy of segment 1");
    sync_seg_dirs(&disk);
    assert!(tier.hot().exists(1, SegmentPart::Data), "hot copy present pre-delete");
    assert!(
        tier.cold().unwrap().exists(1, SegmentPart::Data),
        "cold copy durable pre-delete"
    );
    drop(w);

    // Now wrap the disk in a FaultFs that fails `remove_file` once (the hot delete's
    // very first unlink). Rebuild the writer over the faulty FS so its `hot()` store
    // routes deletes through the fault.
    let faulty: Arc<dyn Fs> =
        FaultFs::new(disk.arc(), FaultOp::RemoveFile, FaultKind::Eio, 0, true).arc();
    let mut w2 = seg_writer_cold(faulty, clock);
    // Re-seal so the writer's in-memory registry knows segment 1 (it must be in
    // `sealed` for `confirm_relocated` to act on it). The seal is a no-op put since
    // both copies already exist on disk (`existing_tier` is Hot ⇒ skip the put).
    for seq in 1..=4u64 {
        seg_append(&mut w2, seq);
    }

    // confirm_relocated: flip pointer to COLD, then hot().delete EIOs. The error is
    // swallowed + logged inside confirm_relocated (it must NOT panic / propagate).
    w2.confirm_relocated(1);

    // NO LOSS, BOTH COPIES EXIST: the failed hot delete left the hot copy in place,
    // the cold copy is authoritative by re-derivation, the record is readable.
    let tier2 = w2.tier();
    assert!(
        tier2.hot().exists(1, SegmentPart::Data),
        "the EIO'd hot delete left the hot copy in place (will retry)"
    );
    assert!(
        tier2.cold().unwrap().exists(1, SegmentPart::Data),
        "the cold copy remains authoritative"
    );
    assert!(tier2.resolve(1).is_some(), "segment 1 still readable (no loss)");
    assert_eq!(
        w2.resolve_sealed(1).expect("record readable").data,
        json!({ "v": 1 }),
        "the record is byte-identical despite the failed hot delete"
    );

    // IDEMPOTENT DROP RE-RUNS: the FaultFs was fail-ONCE, so a retried delete now
    // succeeds and drops the redundant hot copy, converging to a single cold copy.
    match tier2.hot().delete(1) {
        Ok(()) => {}
        Err(StoreError::NotFound(_)) => {}
        Err(e) => panic!("retried hot delete should succeed, got {e:?}"),
    }
    assert!(
        !tier2.hot().exists(1, SegmentPart::Data),
        "the retried hot delete drops the redundant copy"
    );
    assert_eq!(tier2.resolve(1), Some(Tier::Cold), "converges to cold");
    assert_eq!(
        w2.resolve_sealed(1).expect("still readable from cold").data,
        json!({ "v": 1 }),
        "no loss after the drop re-ran"
    );
}

// ===========================================================================
// F-COLD-DELETE-BEFORE-FLIP-FORBIDDEN
// ===========================================================================

/// Adversarial: deliberately delete the HOT copy BEFORE the cold copy is durable
/// (an ordering-violation injection). The `MonitorFs` live ordering guard is
/// supposed to PANIC the instant the only durable copy is dropped — proving a
/// regression that violates the cold-relocation order (cold.put durable BEFORE
/// hot().delete) is caught immediately, independent of any crash test.
///
/// FIXED (Phase-8B fix): `MonitorFs::remove_file` in `src/storage/testfs.rs` now
/// implements the cold-relocation ordering guard FAULT_TESTING.md §MonitorFS(d) /
/// model invariant #9 specifies ("the hot copy is deleted only after the durable
/// cold copy exists"). Deleting a sealed segment's `.data` while it is the LAST
/// durable copy on the device panics immediately. This test drives exactly that
/// violation (seal seg 1 hot-only, then `hot().delete(1)` with no cold copy) and
/// asserts via `#[should_panic]` that the guard fires. The trailing `panic!` (with
/// a different message) is the failure path if the guard ever stops firing — the
/// `expected = ...` substring matches only the guard's panic, so a silent `Ok`
/// would still fail the test.
#[test]
#[should_panic(expected = "deleting the last durable copy of segment")]
fn f_cold_delete_before_flip_forbidden() {
    let disk = FakeDisk::new();
    // Wrap the device in the live ordering monitor (it forwards to the fake but is
    // supposed to assert persistence-ordering invariants and panic on a violation).
    let monitor = MonitorFs::new(disk.arc());
    let clock = test_clock();
    let mut w = seg_writer_cold(monitor.arc(), clock);

    // Seal segment 1 to HOT (its only durable copy). NO cold copy is written.
    for seq in 1..=2u64 {
        seg_append(&mut w, seq);
    }
    sync_seg_dirs(&disk);
    let tier = w.tier();
    assert!(tier.hot().exists(1, SegmentPart::Data), "segment 1 is hot");
    assert!(
        !tier.cold().unwrap().exists(1, SegmentPart::Data),
        "no cold copy has been written yet"
    );

    // ADVERSARIAL ORDERING VIOLATION: delete the HOT copy before the cold copy is
    // durable. This drops the ONLY durable copy — the cold-relocation order
    // (cold.put durable BEFORE hot().delete) is violated. The MonitorFs guard
    // PANICS here (its `remove_file` now enforces §MonitorFS(d)); `#[should_panic]`
    // on this fn asserts the panic fires with the guard's message.
    let _ = tier.hot().delete(1);

    // If we reach here the guard did NOT fire — the only durable copy was silently
    // dropped, which is the regression the monitor is supposed to catch.
    panic!(
        "MonitorFs ordering guard did NOT fire: the hot copy was deleted before a \
         durable cold copy existed (the only durable copy was silently dropped)"
    );
}

// ===========================================================================
// helpers
// ===========================================================================

/// The on-disk path of a segment's `.data` part under a store root (mirrors
/// `LocalSegmentStore::part_path`, using the public `data_name` so the naming never
/// drifts from production).
fn data_path(root: &str, id: u64) -> PathBuf {
    PathBuf::from(root).join(data_name(id))
}
