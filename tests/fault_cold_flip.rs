//! Phase-8B fault catalog — boundary **cold-flip-pointer**.
//!
//! These four strategies exercise the HOT→COLD relocation state machine
//! (`copy_segment_to_cold` → `confirm_relocated` = flip the tier pointer + drop
//! the hot copy) against a hostile, crash-modelling filesystem ([`FakeDisk`] /
//! [`FaultFs`] from the Phase-8A harness). The relocation is driven through the
//! *real* [`TopicTier`] + [`LocalSegmentStore`] wired onto a [`FakeDisk`] (via
//! `LocalSegmentStore::open_with`), so a `crash()` exercises the genuine
//! copy→fsync→flip→drop ordering the engine relies on — exactly the layer every
//! strategy's `inject_how` targets.
//!
//! The invariant under test (DESIGN oracle §9, COLD RELOCATION ALL-OR-NOTHING):
//! after a crash at *any* point in copy→fsync→flip→delete, **exactly one** (never
//! zero) readable copy of every segment exists; `TopicTier::resolve` prefers HOT in
//! the both-exist window; the hot copy is dropped only after the cold copy is
//! durable; and the relocator re-runs the idempotent copy(no-op)+flip+drop and
//! loses nothing.
//!
//! Strategies implemented (catalog ids):
//!   - F-COLD-CRASH-AFTER-COPY-BEFORE-FLIP
//!   - F-COLD-RESOLVE-PREFERS-HOT
//!   - F-COMPOUND-RELOCATE-DURING-RECOVERY
//!   - F-SWEEP-COLD-RELOCATION
//!
//! Run: `cargo test --features test-fs --test fault_cold_flip`
//!
//! NOTE on failpoints: the catalog lists `fail-rs cold::after_copy_before_flip`
//! etc. as one way to crash precisely. This file is built with `test-fs` only
//! (not `failpoints`), so instead it crashes at exactly those code points by
//! *structuring the call sequence* — do the copy + fsync, then `FakeDisk.crash()`
//! BEFORE invoking `confirm_relocated` — which reproduces the identical on-disk
//! state (the harness-level crash injector, same discipline as the
//! `crash_oracle.rs` sweep). The relocation steps themselves are the real engine
//! functions, so the assertions test real crash-consistency behaviour.

#![cfg(feature = "test-fs")]
#![allow(
    clippy::ptr_arg,
    clippy::manual_clamp,
    clippy::unusual_byte_groupings,
    clippy::doc_lazy_continuation
)]

use std::path::PathBuf;
use std::sync::Arc;

use topics::storage::testfs::{FakeDisk, TornDamage};
use topics::storage::{
    decode_data_frame, lookup, LocalSegmentStore, SegmentBuilder, SegmentPart, SegmentRecord, Tier,
    TopicTier,
};

// ===========================================================================
// Helpers: build a real segment, wire a HOT+COLD tier onto a FakeDisk, and
// re-derive a tier from the post-crash durable image (the recovery view).
// ===========================================================================

const HOT_DIR: &str = "/data/topics/00000001";
const COLD_DIR: &str = "/cold/topics/00000001";

/// Build a real segment (`.data` + `.idx`) of `n` records starting at `start`.
fn build_segment(start: u64, n: u64) -> (Vec<u8>, Vec<u8>) {
    let mut b = SegmentBuilder::new(start);
    for i in 0..n {
        b.push(&SegmentRecord {
            seq: start + i,
            ts: 1000 + i,
            node: Some(format!("node{i}")),
            tag: Some(format!("t{i}")),
            data: format!("{{\"v\":{}}}", start + i).into_bytes(),
        });
    }
    b.finish()
}

/// Open a HOT+COLD [`TopicTier`] whose every byte of segment I/O routes through
/// `disk` (so a `crash()` decides what survives). The two stores live at distinct
/// roots, exactly as the engine wires `<data_dir>/topics/<id>` (hot) and
/// `<cold_dir>/topics/<id>` (cold).
fn open_tier(disk: &FakeDisk) -> Arc<TopicTier> {
    let hot = LocalSegmentStore::open_with(PathBuf::from(HOT_DIR), disk.arc()).unwrap();
    let cold = LocalSegmentStore::open_with(PathBuf::from(COLD_DIR), disk.arc()).unwrap();
    Arc::new(TopicTier::new(Box::new(hot), Some(Box::new(cold))))
}

/// Make every directory entry written so far durable (the dir-fsync production
/// does after each `put`/`rename` lands — the in-memory model needs the names
/// fsynced for them to survive a `crash()`). Covers both tier roots + parents.
fn sync_all_dirs(disk: &FakeDisk) {
    let fs = disk.arc();
    for d in [
        "/data/topics/00000001",
        "/cold/topics/00000001",
        "/data/topics",
        "/cold/topics",
        "/data",
        "/cold",
    ] {
        let _ = fs.sync_dir(&PathBuf::from(d));
    }
}

/// Seal a segment into the HOT store and make it durable (its bytes are fsync'd
/// by `put`; we fsync the dir so the name survives a crash). This is the
/// pre-relocation steady state: one sealed segment, hot only.
fn seal_hot(tier: &TopicTier, disk: &FakeDisk, id: u64, n: u64) -> (Vec<u8>, Vec<u8>) {
    let (data, idx) = build_segment(id, n);
    tier.hot().put(id, &data, &idx).unwrap();
    sync_all_dirs(disk);
    (data, idx)
}

/// Read segment `id`'s record `seq` through whichever tier `resolve` selects, the
/// way a consumer read does (resolve → bulk-read `.idx` → locate → read the frame
/// → decode). Returns the decoded payload bytes. Panics if the segment resolves
/// to no tier (a lost segment) — the thing the oracle forbids.
fn read_record(tier: &TopicTier, id: u64, seq: u64) -> Vec<u8> {
    let store = tier
        .store_for(id)
        .unwrap_or_else(|| panic!("segment {id} resolved to NO tier (lost!)"));
    let idx_buf = store.read_all(id, SegmentPart::Idx).unwrap();
    let e = lookup(&idx_buf, id, seq).expect("seq present in idx");
    let frame = store
        .read_range(id, SegmentPart::Data, e.offset as u64, e.len as u64)
        .unwrap();
    decode_data_frame(&frame).expect("frame decodes").data
}

/// The in-engine relocation order, driven the way `Engine::relocate_topic_cold`
/// does it: copy hot→cold (fsync'd by `put`), then flip the durable tier pointer
/// + drop the hot copy. The "durable pointer" IS the cold copy's existence, so a
/// crash between the two steps is what every strategy probes.
fn copy_to_cold(tier: &Arc<TopicTier>, id: u64) {
    topics::engine::segwriter::copy_segment_to_cold(tier, id).expect("cold copy");
}

/// After `copy_to_cold` + the cold copy durable, "flip + drop hot" the way the
/// engine's `confirm_relocated` does. Mirrored here at the tier level (the
/// in-memory sealed-registry flip is re-derived from `resolve` on restart, so the
/// only durable action is the hot delete).
fn flip_drop_hot(tier: &Arc<TopicTier>, id: u64) {
    tier.hot().delete(id).expect("drop hot copy");
}

// ===========================================================================
// F-COLD-CRASH-AFTER-COPY-BEFORE-FLIP  (crash-point, critical)
//
//   fault:  crash after cold.put fsync but before confirm_relocated flips the
//           tier pointer.
//   oracle: both hot and cold copies exist; TopicTier::resolve prefers HOT; record
//           fully readable; relocator re-runs idempotent copy(no-op)+flip+drop
//           (matches interrupted_relocation_recovers_without_loss).
// ===========================================================================

#[test]
fn f_cold_crash_after_copy_before_flip() {
    let disk = FakeDisk::with_seed(0xC01D_0001);
    let id = 1u64;

    // Steady state: one sealed segment, HOT only, durable.
    {
        let tier = open_tier(&disk);
        seal_hot(&tier, &disk, id, 3);
    }

    // Relocate: copy hot→cold + fsync (put fsyncs the bytes), make the cold name
    // durable, then CRASH before the flip/drop. This is exactly "after cold.put
    // fsync but before confirm_relocated".
    {
        let tier = open_tier(&disk);
        copy_to_cold(&tier, id);
        sync_all_dirs(&disk); // cold copy's name becomes durable.
                              // ... crash here, before flip_drop_hot ...
        disk.crash(TornDamage::None);
    }
    disk.reset_power();

    // Recovery view: re-derive the tier from the durable image. BOTH copies must
    // survive (the hot copy was never deleted; the cold copy was fsync'd).
    let tier = open_tier(&disk);
    assert!(
        tier.hot().exists(id, SegmentPart::Data),
        "hot copy must survive (never deleted before the crash)"
    );
    assert!(
        tier.cold().unwrap().exists(id, SegmentPart::Data),
        "cold copy must survive (it was fsync'd before the crash)"
    );
    // resolve prefers HOT in the both-exist window.
    assert_eq!(tier.resolve(id), Some(Tier::Hot), "resolve prefers HOT");
    // The record is fully readable (from the surviving HOT copy).
    assert_eq!(read_record(&tier, id, id + 1), br#"{"v":2}"#.to_vec());

    // The relocator simply RE-RUNS, idempotently, to completion: the copy is a
    // no-op (cold already exists), then flip+drop hot lands. No loss.
    copy_to_cold(&tier, id); // idempotent no-op (cold exists).
    flip_drop_hot(&tier, id);
    sync_all_dirs(&disk);
    assert!(
        !tier.hot().exists(id, SegmentPart::Data),
        "hot dropped after re-run"
    );
    assert_eq!(
        tier.resolve(id),
        Some(Tier::Cold),
        "now authoritatively cold"
    );
    assert_eq!(
        read_record(&tier, id, id + 1),
        br#"{"v":2}"#.to_vec(),
        "still readable from cold after the completed relocation"
    );
}

// ===========================================================================
// F-COLD-RESOLVE-PREFERS-HOT  (crash-point, high)
//
//   fault:  both-copies window with a TORN cold copy (the cold put crashed
//           mid-write, so its bytes are damaged/incomplete) while the hot copy is
//           complete.
//   oracle: resolve prefers HOT (complete); a torn cold is overwritten by the
//           idempotent re-copy; record always readable from the surviving good
//           copy.
// ===========================================================================

#[test]
fn f_cold_resolve_prefers_hot() {
    let disk = FakeDisk::with_seed(0xC01D_0002);
    let id = 1u64;

    // Steady state: HOT-only sealed segment, durable.
    let (good_data, _good_idx) = {
        let tier = open_tier(&disk);
        seal_hot(&tier, &disk, id, 3)
    };

    // Begin a relocation whose cold `.data` write is TORN by the crash: we write
    // the cold copy WITHOUT fsync (so its bytes are pending) and crash with a
    // prefix-truncate tear. The cold `.data` lands as a damaged/partial file; the
    // hot copy is untouched and complete.
    {
        let tier = open_tier(&disk);
        // Hand-roll the cold put so the bytes stay pending (un-fsynced) and get
        // torn on crash — modelling "cold .data torn during copy".
        let cold = tier.cold().unwrap();
        // Use a complete idx but a torn data: write data un-fsynced.
        let data_path = PathBuf::from(COLD_DIR).join(topics::storage::data_name(id));
        {
            use topics::storage::OpenOpts;
            let mut f = disk
                .arc()
                .open(&data_path, OpenOpts::create_truncate())
                .unwrap();
            // Write the full good bytes, but leave them PENDING (no sync) so the
            // crash tears them to a strict prefix.
            let mut off = 0u64;
            while (off as usize) < good_data.len() {
                let n = f.write_at(off, &good_data[off as usize..]).unwrap();
                off += n as u64;
            }
            // deliberately NO sync_data ⇒ pending ⇒ torn on crash.
            let _ = cold; // (cold store handle unused beyond pathing)
        }
        // Make the (about-to-be-torn) name durable so the partial file is visible
        // post-crash — this is the adversarial "a torn cold copy exists".
        sync_all_dirs(&disk);
        disk.crash(TornDamage::PrefixTruncate);
    }
    disk.reset_power();

    let tier = open_tier(&disk);
    // The cold `.data` is present-but-torn (a strict prefix of the good bytes) and
    // has NO `.idx` — so it is an incomplete/damaged cold copy. The hot copy is
    // complete. resolve must prefer the complete HOT copy.
    let cold_data_len = tier.cold().unwrap().len(id, SegmentPart::Data).unwrap_or(0);
    assert!(
        cold_data_len < good_data.len() as u64,
        "cold .data is torn (strict prefix): {cold_data_len} < {}",
        good_data.len()
    );
    assert_eq!(
        tier.resolve(id),
        Some(Tier::Hot),
        "resolve prefers the complete HOT copy over a torn cold one"
    );
    // The record is always readable from the surviving good (hot) copy.
    assert_eq!(read_record(&tier, id, id + 1), br#"{"v":2}"#.to_vec());

    // The idempotent re-copy OVERWRITES the torn cold copy with good bytes. Note
    // `copy_segment_to_cold`'s fast-path skips when BOTH cold parts exist; here
    // only a torn `.data` exists (no `.idx`), so the copy re-runs and repairs.
    copy_to_cold(&Arc::new(rederive(&disk)), id);
    sync_all_dirs(&disk);
    let tier = open_tier(&disk);
    let repaired_len = tier.cold().unwrap().len(id, SegmentPart::Data).unwrap();
    assert_eq!(
        repaired_len,
        good_data.len() as u64,
        "torn cold copy repaired by the idempotent re-copy"
    );
    // After repair the relocation can complete and the record reads from cold.
    let tier = Arc::new(rederive(&disk));
    flip_drop_hot(&tier, id);
    sync_all_dirs(&disk);
    assert_eq!(tier.resolve(id), Some(Tier::Cold));
    assert_eq!(read_record(&tier, id, id + 1), br#"{"v":2}"#.to_vec());
}

/// Re-derive a fresh tier from the current durable disk image (a "recovery" open).
fn rederive(disk: &FakeDisk) -> TopicTier {
    let hot = LocalSegmentStore::open_with(PathBuf::from(HOT_DIR), disk.arc()).unwrap();
    let cold = LocalSegmentStore::open_with(PathBuf::from(COLD_DIR), disk.arc()).unwrap();
    TopicTier::new(Box::new(hot), Some(Box::new(cold)))
}

// ===========================================================================
// F-COMPOUND-RELOCATE-DURING-RECOVERY  (compound, high)
//
//   fault:  a relocation in the both-copies window, then a crash DURING
//           recovery's reclaim_segments_on_recovery (the orphan sweep).
//   oracle: resolve prefers hot; the interrupted reclaim re-runs idempotently
//           next boot; exactly one copy ends authoritative; no segment lost or
//           double-counted.
// ===========================================================================

#[test]
fn f_compound_relocate_during_recovery() {
    let disk = FakeDisk::with_seed(0xC01D_0003);
    let id = 1u64;

    // Steady state + a relocation left in the both-copies window (copy durable,
    // flip/drop not yet done) — the F-COLD-CRASH-AFTER-COPY-BEFORE-FLIP state.
    {
        let tier = open_tier(&disk);
        seal_hot(&tier, &disk, id, 3);
    }
    {
        let tier = open_tier(&disk);
        copy_to_cold(&tier, id);
        sync_all_dirs(&disk);
        disk.crash(TornDamage::None); // both copies durable; flip never happened.
    }
    disk.reset_power();

    // --- First recovery boot: it runs the orphan sweep
    // (`reclaim_orphans_below`) but is itself INTERRUPTED by a second crash before
    // it can fsync the namespace change. The live floor here is the segment's own
    // start (the segment is LIVE — its records are in the recovered set), so the
    // sweep must NOT touch it: a live, registered segment is never an orphan.
    {
        let tier = open_tier(&disk);
        // resolve prefers hot in the both-copies window.
        assert_eq!(
            tier.resolve(id),
            Some(Tier::Hot),
            "boot1: resolve prefers HOT"
        );
        assert_eq!(
            read_record(&tier, id, id),
            br#"{"v":1}"#.to_vec(),
            "boot1: readable"
        );

        // Simulate recovery's reclaim_segments_on_recovery: a live floor at/below
        // the segment start means the segment is NOT below-floor ⇒ it is kept. We
        // drive the real sweep through a SegmentWriter to use the genuine
        // `reclaim_orphans_below`. Then crash mid-recovery (before any dir fsync).
        let dropped = run_orphan_sweep(&tier, /*live_floor=*/ id);
        assert_eq!(
            dropped, 0,
            "boot1: the live segment is not an orphan, nothing dropped"
        );
        disk.crash(TornDamage::None); // crash DURING recovery (post-sweep, pre-fsync).
    }
    disk.reset_power();

    // --- Second recovery boot: the interrupted reclaim re-runs idempotently.
    // Exactly one copy ends authoritative once the relocation also completes; no
    // segment lost or double-counted.
    let tier = open_tier(&disk);
    // Still both copies (the sweep correctly left the live segment alone, and the
    // crash dropped no durable state since nothing dead was reclaimed).
    assert!(
        tier.hot().exists(id, SegmentPart::Data),
        "boot2: hot survives"
    );
    assert!(
        tier.cold().unwrap().exists(id, SegmentPart::Data),
        "boot2: cold survives"
    );
    assert_eq!(
        tier.resolve(id),
        Some(Tier::Hot),
        "boot2: resolve still prefers HOT"
    );
    assert_eq!(
        read_record(&tier, id, id + 2),
        br#"{"v":3}"#.to_vec(),
        "boot2: readable"
    );

    // Re-running the sweep is idempotent (a live segment is still not an orphan).
    let dropped = run_orphan_sweep(&tier, id);
    assert_eq!(dropped, 0, "boot2: idempotent sweep drops nothing");

    // Finish the relocation: flip+drop hot ⇒ exactly ONE authoritative copy.
    let tier = Arc::new(rederive(&disk));
    copy_to_cold(&tier, id); // idempotent no-op.
    flip_drop_hot(&tier, id);
    sync_all_dirs(&disk);
    let tier = rederive(&disk);
    assert!(
        !tier.hot().exists(id, SegmentPart::Data),
        "exactly one copy: cold"
    );
    assert_eq!(tier.resolve(id), Some(Tier::Cold));
    assert_eq!(
        read_record(&tier, id, id + 1),
        br#"{"v":2}"#.to_vec(),
        "no segment lost"
    );
}

/// Drive the REAL on-recovery orphan sweep (`SegmentWriter::reclaim_orphans_below`)
/// over a tier, returning the number of orphan segment ids dropped. Builds a
/// throwaway `SegmentWriter` around the tier (its sealed registry is empty, so the
/// sweep's "not registered" test is governed purely by the `live_floor` argument —
/// exactly what recovery passes after rebuilding the registry).
fn run_orphan_sweep(tier: &Arc<TopicTier>, live_floor: u64) -> usize {
    use topics::clock::{SharedClock, TestClock};
    use topics::config::SegmentConfig;
    use topics::engine::segwriter::SegmentWriter;
    let cfg = SegmentConfig {
        max_events: 1,
        max_bytes: 0,
        max_age_ms: 0,
        hot_retain_segments: 0,
        hot_retain_bytes: 0,
    };
    let clock: SharedClock = Arc::new(TestClock::new(0));
    let mut w = SegmentWriter::new(tier.clone(), cfg, clock);
    w.reclaim_orphans_below(live_floor)
}

// ===========================================================================
// F-SWEEP-COLD-RELOCATION  (crash-point, critical)
//
//   fault:  exhaustive crash-point sweep across a FULL relocation (cold copy
//           data/idx/dirfsync, flip, hot delete data/idx/dirfsync); crash() after
//           each FS mutating call.
//   oracle: at every point at least one readable copy exists (never zero);
//           resolve prefers hot in the overlap; relocator re-runs to a single
//           authoritative copy.
// ===========================================================================

#[test]
fn f_sweep_cold_relocation() {
    let id = 1u64;
    let (good_data, _good_idx) = build_segment(id, 3);

    // Probe M: how many FS mutating calls does ONE full relocation issue? Count
    // write_at + rename + sync_dir + remove_file over copy + flip/drop on a clean
    // disk (no crash). We size the sweep from this.
    let probe_m = {
        let disk = FakeDisk::with_seed(1);
        {
            let tier = open_tier(&disk);
            seal_hot(&tier, &disk, id, 3);
        }
        let counter = MutCounter::wrap(disk.arc());
        let hot = LocalSegmentStore::open_with(PathBuf::from(HOT_DIR), counter.arc()).unwrap();
        let cold = LocalSegmentStore::open_with(PathBuf::from(COLD_DIR), counter.arc()).unwrap();
        let tier = Arc::new(TopicTier::new(Box::new(hot), Some(Box::new(cold))));
        copy_to_cold(&tier, id);
        flip_drop_hot(&tier, id);
        counter.count()
    };
    assert!(
        probe_m >= 3,
        "a full relocation issues several FS mutating calls (M={probe_m})"
    );

    // Cap the sweep so it stays well under a minute but still covers every FS
    // boundary of this small relocation.
    let cap = probe_m.min(24);
    // Tiered sweep (topics::testutil::crash_points): bounded deterministic sample
    // by default, full `0..=cap` under TOPICS_TEST_EXHAUSTIVE.
    for crash_point in topics::testutil::crash_points(cap) {
        let disk = FakeDisk::with_seed(0xC01D_5EE0 ^ crash_point);

        // Steady state: HOT-only durable segment.
        {
            let tier = open_tier(&disk);
            seal_hot(&tier, &disk, id, 3);
        }

        // Run a full relocation through a CrashAfter wrapper that fires
        // `disk.crash()` after exactly `crash_point` FS mutating calls — the
        // harness-level "power loss after the Nth FS call".
        {
            let trip = CrashAfter::new(disk.clone(), crash_point);
            let hot = LocalSegmentStore::open_with(PathBuf::from(HOT_DIR), trip.arc()).unwrap();
            let cold = LocalSegmentStore::open_with(PathBuf::from(COLD_DIR), trip.arc()).unwrap();
            let tier = Arc::new(TopicTier::new(Box::new(hot), Some(Box::new(cold))));
            // copy (may crash partway), dir fsync, flip+drop (may crash partway).
            // All FS calls past the trip point land on a frozen device and are lost.
            let _ = topics::engine::segwriter::copy_segment_to_cold(&tier, id);
            let _ = tier.hot().delete(id);
            let _ = trip; // ensure the wrapper lives across the relocation.
        }
        disk.reset_power();

        // ORACLE: at EVERY crash point, at least one readable copy exists (never
        // zero), and resolve prefers HOT in the overlap.
        let tier = rederive(&disk);
        let tier_resolve = tier.resolve(id);
        assert!(
            tier_resolve.is_some(),
            "crash_point {crash_point}: segment lost — NEITHER tier has a complete .data \
             (hot={}, cold={})",
            tier.hot().exists(id, SegmentPart::Data),
            tier.cold().unwrap().exists(id, SegmentPart::Data),
        );
        // If both complete copies exist, HOT is preferred.
        let hot_complete =
            tier.hot().exists(id, SegmentPart::Data) && tier.hot().exists(id, SegmentPart::Idx);
        let cold_complete = tier.cold().unwrap().exists(id, SegmentPart::Data)
            && tier.cold().unwrap().exists(id, SegmentPart::Idx);
        if hot_complete && cold_complete {
            assert_eq!(
                tier_resolve,
                Some(Tier::Hot),
                "crash_point {crash_point}: resolve prefers HOT in the both-exist window"
            );
        }

        // The record resolves and reads correctly from whichever copy survived
        // (the authoritative one), and its bytes are the genuine record. We only
        // assert a clean read when the resolved tier has a COMPLETE segment (both
        // parts) — a torn/half copy in the *other* tier is irrelevant since resolve
        // routes to the complete one.
        let store = tier.store_for(id).unwrap();
        if store.exists(id, SegmentPart::Idx) {
            assert_eq!(
                read_record(&tier, id, id + 1),
                br#"{"v":2}"#.to_vec(),
                "crash_point {crash_point}: record reads correctly from the surviving copy"
            );
        } else {
            // The resolved tier's .data exists but its .idx is torn/missing — the
            // segment is incomplete in BOTH tiers only if neither idx exists. The
            // good `.data` is still present, so the relocator re-copy will repair
            // it from the other tier if available. Assert at least the .data of the
            // resolved copy matches the good prefix (no fabrication).
            let n = store.len(id, SegmentPart::Data).unwrap();
            let got = store.read_range(id, SegmentPart::Data, 0, n).unwrap();
            assert_eq!(
                &got[..],
                &good_data[..n as usize],
                "crash_point {crash_point}: surviving .data is a genuine prefix, never garbage"
            );
        }

        // RE-RUN the relocator to completion (idempotent) ⇒ a single authoritative
        // copy, record still readable. The re-copy repairs an incomplete cold copy
        // from hot (when hot survives); if only cold survived, it is already the
        // single copy.
        let tier = Arc::new(rederive(&disk));
        let _ = topics::engine::segwriter::copy_segment_to_cold(&tier, id);
        // Only drop hot if a complete cold copy now exists (the engine's invariant:
        // never delete hot before the cold copy is durable).
        if tier.cold().unwrap().exists(id, SegmentPart::Data)
            && tier.cold().unwrap().exists(id, SegmentPart::Idx)
        {
            let _ = tier.hot().delete(id);
        }
        sync_all_dirs(&disk);

        let tier = rederive(&disk);
        assert!(
            tier.resolve(id).is_some(),
            "crash_point {crash_point}: after re-run a single authoritative copy exists"
        );
        assert_eq!(
            read_record(&tier, id, id + 1),
            br#"{"v":2}"#.to_vec(),
            "crash_point {crash_point}: record readable after the relocator re-runs"
        );
    }
}

// ===========================================================================
// Test-only Fs wrappers used by the sweep: a mutating-call counter (to size M)
// and a CrashAfter that fires FakeDisk.crash() after the Nth mutating FS call.
// ===========================================================================

use std::sync::atomic::{AtomicU64, Ordering};
use topics::storage::{File, Fs, OpenOpts};

/// Counts FS *mutating* calls (write_at / set_len / rename / sync_dir /
/// remove_file) over an inner `Fs`, to probe the crash-point space M.
#[derive(Clone)]
struct MutCounter {
    inner: Arc<dyn Fs>,
    n: Arc<AtomicU64>,
}

impl MutCounter {
    fn wrap(inner: Arc<dyn Fs>) -> Self {
        MutCounter {
            inner,
            n: Arc::new(AtomicU64::new(0)),
        }
    }
    fn arc(&self) -> Arc<dyn Fs> {
        Arc::new(self.clone())
    }
    fn count(&self) -> u64 {
        self.n.load(Ordering::SeqCst)
    }
    fn bump(&self) {
        self.n.fetch_add(1, Ordering::SeqCst);
    }
}

struct MutCounterFile {
    inner: Box<dyn File>,
    owner: MutCounter,
}

impl File for MutCounterFile {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read_at(offset, buf)
    }
    fn write_at(&mut self, offset: u64, buf: &[u8]) -> std::io::Result<usize> {
        self.owner.bump();
        self.inner.write_at(offset, buf)
    }
    fn set_len(&mut self, len: u64) -> std::io::Result<()> {
        self.owner.bump();
        self.inner.set_len(len)
    }
    fn sync_data(&self) -> std::io::Result<()> {
        self.inner.sync_data()
    }
    fn sync_all(&self) -> std::io::Result<()> {
        self.inner.sync_all()
    }
    fn metadata_len(&self) -> std::io::Result<u64> {
        self.inner.metadata_len()
    }
    fn read_to_end_from(&self, offset: u64, out: &mut Vec<u8>) -> std::io::Result<()> {
        self.inner.read_to_end_from(offset, out)
    }
}

impl Fs for MutCounter {
    fn open(&self, path: &std::path::Path, opts: OpenOpts) -> std::io::Result<Box<dyn File>> {
        let inner = self.inner.open(path, opts)?;
        Ok(Box::new(MutCounterFile {
            inner,
            owner: self.clone(),
        }))
    }
    fn rename(&self, from: &std::path::Path, to: &std::path::Path) -> std::io::Result<()> {
        self.bump();
        self.inner.rename(from, to)
    }
    fn remove_file(&self, path: &std::path::Path) -> std::io::Result<()> {
        self.bump();
        self.inner.remove_file(path)
    }
    fn read_dir(&self, dir: &std::path::Path) -> std::io::Result<Vec<PathBuf>> {
        self.inner.read_dir(dir)
    }
    fn sync_dir(&self, dir: &std::path::Path) -> std::io::Result<()> {
        self.bump();
        self.inner.sync_dir(dir)
    }
    fn create_dir_all(&self, dir: &std::path::Path) -> std::io::Result<()> {
        self.inner.create_dir_all(dir)
    }
    fn exists(&self, path: &std::path::Path) -> bool {
        self.inner.exists(path)
    }
    fn metadata_len(&self, path: &std::path::Path) -> std::io::Result<u64> {
        self.inner.metadata_len(path)
    }
}

/// Fires `disk.crash(None)` exactly after the `at`-th (0-based) FS *mutating* call
/// (write_at / set_len / rename / sync_dir / remove_file), then lets the frozen
/// disk swallow the rest. The in-process "SIGKILL after the Nth FS mutating call",
/// scoped to the relocation under test.
#[derive(Clone)]
struct CrashAfter {
    disk: FakeDisk,
    at: u64,
    seen: Arc<AtomicU64>,
    tripped: Arc<std::sync::atomic::AtomicBool>,
}

impl CrashAfter {
    fn new(disk: FakeDisk, at: u64) -> Self {
        CrashAfter {
            disk,
            at,
            seen: Arc::new(AtomicU64::new(0)),
            tripped: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }
    fn arc(&self) -> Arc<dyn Fs> {
        Arc::new(self.clone())
    }
    fn tick(&self) {
        let idx = self.seen.fetch_add(1, Ordering::SeqCst);
        if idx == self.at
            && self
                .tripped
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            self.disk.crash(TornDamage::None);
        }
    }
}

struct CrashAfterFile {
    inner: Box<dyn File>,
    owner: CrashAfter,
}

impl File for CrashAfterFile {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read_at(offset, buf)
    }
    fn write_at(&mut self, offset: u64, buf: &[u8]) -> std::io::Result<usize> {
        // Trip AFTER the write reaches the pending image, so a crash at index N
        // keeps the first N writes available for fsync but drops anything not yet
        // fsynced — the power-loss-after-Nth-write model.
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
        self.inner.sync_data()
    }
    fn sync_all(&self) -> std::io::Result<()> {
        self.inner.sync_all()
    }
    fn metadata_len(&self) -> std::io::Result<u64> {
        self.inner.metadata_len()
    }
    fn read_to_end_from(&self, offset: u64, out: &mut Vec<u8>) -> std::io::Result<()> {
        self.inner.read_to_end_from(offset, out)
    }
}

impl Fs for CrashAfter {
    fn open(&self, path: &std::path::Path, opts: OpenOpts) -> std::io::Result<Box<dyn File>> {
        let inner = self.disk.open(path, opts)?;
        Ok(Box::new(CrashAfterFile {
            inner,
            owner: self.clone(),
        }))
    }
    fn rename(&self, from: &std::path::Path, to: &std::path::Path) -> std::io::Result<()> {
        let r = self.disk.rename(from, to);
        self.tick();
        r
    }
    fn remove_file(&self, path: &std::path::Path) -> std::io::Result<()> {
        let r = self.disk.remove_file(path);
        self.tick();
        r
    }
    fn read_dir(&self, dir: &std::path::Path) -> std::io::Result<Vec<PathBuf>> {
        self.disk.read_dir(dir)
    }
    fn sync_dir(&self, dir: &std::path::Path) -> std::io::Result<()> {
        let r = self.disk.sync_dir(dir);
        self.tick();
        r
    }
    fn create_dir_all(&self, dir: &std::path::Path) -> std::io::Result<()> {
        self.disk.create_dir_all(dir)
    }
    fn exists(&self, path: &std::path::Path) -> bool {
        self.disk.exists(path)
    }
    fn metadata_len(&self, path: &std::path::Path) -> std::io::Result<u64> {
        self.disk.metadata_len(path)
    }
}
