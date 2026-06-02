//! Phase-8B fault catalog — **cold-copy** boundary (HOT → COLD relocation copy).
//!
//! Four strategies from `docs/FAULT_TESTING.md` / `/tmp/topics-fault-catalog.json`,
//! all on the `copy_segment_to_cold` step of a relocation (the slow off-lock COPY
//! that precedes the FLIP+DROP). The contract the engine relies on
//! (ARCHITECTURE §3.6, oracle invariant #9 COLD RELOCATION ALL-OR-NOTHING):
//!
//!   * a *failed* cold copy must leave the HOT copy intact and the tier pointer
//!     un-flipped — relocation simply does not advance, never a loss;
//!   * a half-written / torn cold copy must be non-authoritative: `TopicTier::resolve`
//!     prefers the surviving HOT copy and a re-run of the idempotent copy completes
//!     it — at every point at least one readable copy of the segment exists.
//!
//! These drive the real `copy_segment_to_cold` / `confirm_relocated` against a
//! `TopicTier` whose COLD store routes through the Phase-8A hostile FS impls
//! (`FaultFs` for EIO/ENOSPC/ESTALE, `FakeDisk` for the torn crash), exactly the
//! `inject_how` the catalog prescribes ("FaultFs fail-once on the cold put
//! write_at", "FakeDisk tears the cold tmp before rename", "FaultFs returns ESTALE
//! on cold put"). The HOT store sits on a clean fully-durable `FakeDisk` so only
//! the cold path is hostile.
//!
//! Bounded + deterministic: a single tiny segment, fixed seeds, no sweeps.
//!
//! ```text
//! cargo test --features test-fs --test fault_cold_copy
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

use topics::storage::testfs::{FakeDisk, FaultFs, FaultKind, FaultOp, TornDamage};
use topics::storage::{
    decode_data_frame, lookup, Fs, LocalSegmentStore, SegmentBuilder, SegmentId, SegmentPart,
    SegmentRecord, SegmentStore, Tier, TopicTier,
};

// ===========================================================================
// Fixtures: a one-segment HOT store + a (hostile-FS) COLD store in a TopicTier.
// ===========================================================================

const SEG_ID: SegmentId = 1;
const HOT_ROOT: &str = "/hot/topics/00000001";
const COLD_ROOT: &str = "/cold/topics/00000001";

/// Build a tiny segment (`SEG_ID` covering seqs 1..=3) and return its
/// `(data, idx)` byte buffers — the same bytes a real seal would persist.
fn build_segment() -> (Vec<u8>, Vec<u8>) {
    let mut b = SegmentBuilder::new(SEG_ID);
    for i in 0..3u64 {
        b.push(&SegmentRecord {
            seq: SEG_ID + i,
            ts: 1_700_000_000_000 + i,
            node: Some(format!("n{i}")),
            tag: Some(format!("t{i}")),
            data: format!("{{\"v\":{i}}}").into_bytes(),
        });
    }
    b.finish()
}

/// Open a HOT `LocalSegmentStore` on a clean (fully durable) `FakeDisk` and seal
/// `SEG_ID` into it; return the disk (so the hot copy stays durable) plus the
/// store boxed for a `TopicTier`. The hot side is never hostile — only the cold
/// path under test is.
fn hot_store_with_segment() -> (FakeDisk, Box<dyn SegmentStore>) {
    let hot_disk = FakeDisk::new();
    hot_disk.create_dir_all(&PathBuf::from(HOT_ROOT)).unwrap();
    let hot = LocalSegmentStore::open_with(HOT_ROOT, hot_disk.arc()).unwrap();
    let (data, idx) = build_segment();
    hot.put(SEG_ID, &data, &idx).expect("seal hot segment");
    // Make the hot directory entries durable (a real seal dir-fsyncs; model it so
    // the hot copy is a stable, crash-surviving baseline for these tests).
    hot_disk.arc().sync_dir(&PathBuf::from(HOT_ROOT)).unwrap();
    assert!(hot.exists(SEG_ID, SegmentPart::Data));
    assert!(hot.exists(SEG_ID, SegmentPart::Idx));
    (hot_disk, Box::new(hot))
}

/// Read seq `seq` of `SEG_ID` back through whichever tier currently holds it and
/// assert it decodes to the genuine record bytes — the "still readable, no loss"
/// half of every cold-copy oracle.
fn assert_record_readable(tier: &TopicTier, seq: u64) {
    let store = tier
        .store_for(SEG_ID)
        .expect("segment resolvable in some tier (never zero copies)");
    let idx_buf = store.read_all(SEG_ID, SegmentPart::Idx).expect("read idx");
    let e = lookup(&idx_buf, SEG_ID, seq).expect("seq present in idx");
    let frame = store
        .read_range(SEG_ID, SegmentPart::Data, e.offset as u64, e.len as u64)
        .expect("read data frame");
    let rec = decode_data_frame(&frame).expect("decode frame");
    assert_eq!(rec.seq, seq, "readable record has the right seq");
    assert_eq!(
        rec.data,
        format!("{{\"v\":{}}}", seq - SEG_ID).into_bytes(),
        "readable record has the genuine payload bytes"
    );
}

/// Drive the real relocation COPY step against `tier` for `SEG_ID`, as the engine
/// does on its blocking pool (`relocate_topic_cold` step 2). Returns the copy
/// result so the caller can assert it failed/succeeded.
fn copy_to_cold(tier: &Arc<TopicTier>) -> Result<(), topics::storage::StoreError> {
    topics::engine::segwriter::copy_segment_to_cold(tier, SEG_ID)
}

// ===========================================================================
// F-COLD-EIO-COPY — EIO on the cold put write_at: copy aborts, HOT intact.
// ===========================================================================

/// EIO fired (fail-once) on the cold store's `write_at` during the relocation
/// copy. ORACLE: `copy_segment_to_cold` returns `Err`; the engine leaves HOT
/// intact, never flips the pointer nor deletes hot on a failed copy — relocation
/// simply does not advance. `TopicTier::resolve` stays HOT and the record is still
/// readable.
#[test]
fn f_cold_eio_copy() {
    let (_hot_disk, hot) = hot_store_with_segment();

    // COLD store on a FakeDisk wrapped so the FIRST cold write_at returns EIO
    // (the segment .data tmp body write inside cold.put → write_atomic).
    let cold_disk = FakeDisk::new();
    cold_disk.create_dir_all(&PathBuf::from(COLD_ROOT)).unwrap();
    let cold_fs = FaultFs::new(cold_disk.arc(), FaultOp::WriteAt, FaultKind::Eio, 0, true);
    let cold = LocalSegmentStore::open_with(COLD_ROOT, cold_fs.arc()).unwrap();

    let tier = Arc::new(TopicTier::new(hot, Some(Box::new(cold))));

    // The copy MUST fail (the engine then logs + moves on, never flipping).
    let res = copy_to_cold(&tier);
    assert!(res.is_err(), "EIO on cold write_at must fail the copy");

    // HOT copy untouched; the pointer never flipped (we did NOT call
    // confirm_relocated, mirroring the engine's `continue` on a failed copy).
    assert!(
        tier.hot().exists(SEG_ID, SegmentPart::Data),
        "hot .data intact"
    );
    assert!(
        tier.hot().exists(SEG_ID, SegmentPart::Idx),
        "hot .idx intact"
    );
    assert_eq!(
        tier.resolve(SEG_ID),
        Some(Tier::Hot),
        "resolve stays HOT after a failed cold copy"
    );
    // No half-copied cold treated as authoritative: even if a torn .data tmp body
    // landed, the final .data name is not present (write_atomic never renamed it).
    assert!(
        !tier.cold().unwrap().exists(SEG_ID, SegmentPart::Data),
        "failed copy leaves no final cold .data"
    );
    // The record is still fully readable from the surviving HOT copy.
    assert_record_readable(&tier, SEG_ID);
    assert_record_readable(&tier, SEG_ID + 2);
}

// ===========================================================================
// F-COLD-ENOSPC-COPY — cold tier full: abort before flip, hot retained.
// ===========================================================================

/// ENOSPC (fail-always — a dead/full cold device) on the cold put write_at.
/// ORACLE: abort before flip; the hot copy is retained and the segment is still
/// readable from hot; no half-copied cold segment is treated as authoritative
/// (`resolve` prefers hot).
#[test]
fn f_cold_enospc_copy() {
    let (_hot_disk, hot) = hot_store_with_segment();

    let cold_disk = FakeDisk::new();
    cold_disk.create_dir_all(&PathBuf::from(COLD_ROOT)).unwrap();
    // fail-always ENOSPC: the cold tier is full, every write refuses.
    let cold_fs = FaultFs::new(
        cold_disk.arc(),
        FaultOp::WriteAt,
        FaultKind::Enospc,
        0,
        false,
    );
    let cold = LocalSegmentStore::open_with(COLD_ROOT, cold_fs.arc()).unwrap();

    let tier = Arc::new(TopicTier::new(hot, Some(Box::new(cold))));

    let res = copy_to_cold(&tier);
    assert!(res.is_err(), "ENOSPC on cold must fail the copy");

    // Hot retained, no flip, resolve prefers hot, still readable.
    assert!(
        tier.hot().exists(SEG_ID, SegmentPart::Data),
        "hot retained on ENOSPC"
    );
    assert_eq!(tier.resolve(SEG_ID), Some(Tier::Hot), "resolve prefers hot");
    assert!(
        !tier.cold().unwrap().exists(SEG_ID, SegmentPart::Data),
        "no half-copied authoritative cold .data"
    );
    assert_record_readable(&tier, SEG_ID + 1);
}

// ===========================================================================
// F-COLD-TORN-COPY — crash mid cold put: tmp not renamed ⇒ cold incomplete.
// ===========================================================================

/// A crash mid cold `put` tears the cold copy: the `.data`/`.idx` tmp bodies were
/// being written but the renames (durable only after the put's `sync_dir`) never
/// hardened, so on crash the cold store has no final segment files. ORACLE:
/// `write_atomic` leaves no final file ⇒ the cold copy is incomplete ⇒
/// `exists()` is false ⇒ `resolve` stays HOT; the relocation re-runs and loses
/// nothing.
///
/// We model the crash by aborting the cold `put` partway (a fault that fails the
/// idx write, so `put` returns before its final `sync_dir`) and then `crash()`ing
/// the cold disk with a torn-write tear — the un-`sync_dir`'d renames roll back,
/// exactly the "tmp not renamed durably" state the oracle describes.
#[test]
fn f_cold_torn_copy() {
    let (_hot_disk, hot) = hot_store_with_segment();

    // Cold on a seeded FakeDisk. A FaultFs aborts the SECOND write_at (the `.idx`
    // tmp body) — `put` writes+renames `.data`, then fails on `.idx` before the
    // final `sync_dir`. Neither rename is dir-fsynced, so the crash rolls them
    // back: the cold copy is torn/incomplete.
    let cold_disk = FakeDisk::with_seed(0xC01D7012);
    cold_disk.create_dir_all(&PathBuf::from(COLD_ROOT)).unwrap();
    let cold_fs = FaultFs::new(cold_disk.arc(), FaultOp::WriteAt, FaultKind::Eio, 1, true);
    let cold = LocalSegmentStore::open_with(COLD_ROOT, cold_fs.arc()).unwrap();

    let tier = Arc::new(TopicTier::new(hot, Some(Box::new(cold))));

    // The put aborts mid-copy (returns Err); the engine would NOT flip.
    let res = copy_to_cold(&tier);
    assert!(res.is_err(), "torn cold put aborts the copy");

    // Power loss tears the last pending write and rolls back un-sync_dir'd renames.
    cold_disk.crash(TornDamage::PrefixTruncate);

    // The cold copy is incomplete: no final .data (and no final .idx) durably
    // present ⇒ resolve stays HOT, never zero copies.
    assert!(
        !tier.cold().unwrap().exists(SEG_ID, SegmentPart::Data),
        "torn cold copy: no final .data ⇒ incomplete"
    );
    assert_eq!(
        tier.resolve(SEG_ID),
        Some(Tier::Hot),
        "resolve stays HOT over a torn/incomplete cold copy"
    );
    assert!(
        tier.hot().exists(SEG_ID, SegmentPart::Data),
        "hot copy survives"
    );
    assert_record_readable(&tier, SEG_ID);

    // RE-RUN the relocation copy on a healed cold device: it completes (idempotent
    // re-copy writes both parts) — relocation re-runs without loss. We reset the
    // cold device power and re-open the store fresh (reopen-by-path).
    cold_disk.reset_power();
    let cold2 = LocalSegmentStore::open_with(COLD_ROOT, cold_disk.arc()).unwrap();
    let tier2 = Arc::new(TopicTier::new(
        Box::new(LocalSegmentStore::open_with(HOT_ROOT, _hot_disk.arc()).unwrap()),
        Some(Box::new(cold2)),
    ));
    copy_to_cold(&tier2).expect("re-run cold copy completes after heal");
    assert!(
        tier2.cold().unwrap().exists(SEG_ID, SegmentPart::Data),
        "re-run copy installs a complete cold .data"
    );
    assert!(
        tier2.cold().unwrap().exists(SEG_ID, SegmentPart::Idx),
        "re-run copy installs a complete cold .idx"
    );
}

// ===========================================================================
// F-COLD-NFS-ESTALE-COPY — ESTALE on the cold fd mid-copy: hot retained, retry.
// ===========================================================================

/// ESTALE (a stale NFS handle — the cold file replaced server-side) on the cold
/// put, fail-once. ORACLE: the copy fails, the hot copy is retained, and a retry
/// reopens the cold path fresh and succeeds; a relocation that hit ESTALE is never
/// acked (the pointer is not flipped until a clean copy), and nothing is lost.
#[test]
fn f_cold_nfs_estale_copy() {
    let (hot_disk, hot) = hot_store_with_segment();

    let cold_disk = FakeDisk::new();
    cold_disk.create_dir_all(&PathBuf::from(COLD_ROOT)).unwrap();
    // ESTALE on the first cold write_at, fail-once (a transient stale handle).
    let cold_fs = FaultFs::new(
        cold_disk.arc(),
        FaultOp::WriteAt,
        FaultKind::Estale,
        0,
        true,
    );
    let cold = LocalSegmentStore::open_with(COLD_ROOT, cold_fs.arc()).unwrap();

    let tier = Arc::new(TopicTier::new(hot, Some(Box::new(cold))));

    // First attempt hits ESTALE and fails.
    let res = copy_to_cold(&tier);
    assert!(res.is_err(), "ESTALE on cold must fail the copy");

    // Hot retained, pointer not flipped, resolve prefers hot, still readable —
    // the ESTALE relocation was never acked.
    assert!(
        tier.hot().exists(SEG_ID, SegmentPart::Data),
        "hot retained on ESTALE"
    );
    assert_eq!(tier.resolve(SEG_ID), Some(Tier::Hot), "resolve stays HOT");
    assert!(
        !tier.cold().unwrap().exists(SEG_ID, SegmentPart::Data),
        "no authoritative cold copy after an ESTALE abort"
    );
    assert_record_readable(&tier, SEG_ID);

    // RETRY: the fail-once ESTALE has cleared. Reopen the cold store by path (the
    // NFS reopen-by-path on retry) and re-run the copy — it now succeeds and
    // installs a complete cold copy. Nothing was lost across the stale handle.
    let cold_retry = LocalSegmentStore::open_with(COLD_ROOT, cold_fs.arc()).unwrap();
    let tier_retry = Arc::new(TopicTier::new(
        Box::new(LocalSegmentStore::open_with(HOT_ROOT, hot_disk.arc()).unwrap()),
        Some(Box::new(cold_retry)),
    ));
    copy_to_cold(&tier_retry).expect("retry copy succeeds after the stale handle clears");
    assert!(
        tier_retry.cold().unwrap().exists(SEG_ID, SegmentPart::Data),
        "retry installs a complete cold .data"
    );
    assert!(
        tier_retry.cold().unwrap().exists(SEG_ID, SegmentPart::Idx),
        "retry installs a complete cold .idx"
    );
    // And the record is still readable (now resolvable from either tier).
    assert_record_readable(&tier_retry, SEG_ID + 2);
}
