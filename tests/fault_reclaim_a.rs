//! Phase-8B fault catalog — boundary **reclaim** (part A).
//!
//! Five strategies exercising segment-reclaim crash-consistency (the cap/TTL/
//! delete segment-granular reclaim path + the on-recovery orphan sweep + the
//! voluntary front-prefix delete) against the Phase-8A hostile filesystem harness
//! ([`FakeDisk`] / [`FaultFs`] from `src/storage/testfs.rs`). The lower-level
//! strategies drive the *real* [`LocalSegmentStore`] / [`TopicTier`] /
//! [`SegmentWriter::reclaim_orphans_below`] wired onto a [`FakeDisk`]; the
//! WAL-level strategies drive the *real, fully-wired* [`Engine`] through
//! `Engine::with_data_dir_fs` (the same plumbing as `tests/crash_oracle.rs`), so a
//! `crash()` exercises the genuine durability ordering the engine relies on.
//!
//! Invariants under test (DESIGN oracle §10 DELETE NEVER RESURRECTS, §11 FLOORS
//! PRESERVED + MONOTONIC): a reclaimed/orphan segment whose unlink was interrupted
//! is dropped on the next boot and never resurfaces; a `delete`-removed seq stays
//! absent across repeated recovery; a voluntary front-prefix delete advances
//! `earliest_seq` SILENTLY (no tombstone), while count/head stay consistent.
//!
//! Strategies implemented (catalog ids):
//!   - F-RECLAIM-CRASH-BETWEEN-UNLINKS  (crash after .data unlink, before .idx)
//!   - F-RECLAIM-ORPHAN-BELOW-FLOOR     (reclaim_orphans_below drops the dead file)
//!   - F-RECLAIM-EIO-UNLINK             (EIO on remove_file ⇒ dead seg stays, retry)
//!   - F-RECLAIM-DELETE-NEVER-RESURRECTS(crash after Delete fsync ⇒ replay re-deletes)
//!   - F-RECLAIM-FRONT-PREFIX           (crash mid front-prefix reclaim ⇒ silent)
//!
//! Run: `cargo test --features test-fs --test fault_reclaim_a`
//!
//! NOTE on failpoints: the catalog lists `fail-rs reclaim::between_unlinks` /
//! `after delete commit` as one way to crash precisely. This file is built with
//! `test-fs` only (not `failpoints`), so instead it reproduces the *identical
//! on-disk state* at those exact code points by structuring the call sequence —
//! e.g. unlink `.data` (un-dir-fsynced), `crash()` before the `.idx` unlink — the
//! same harness-level crash discipline as `crash_oracle.rs` / `fault_cold_flip.rs`.
//! The reclaim/delete steps themselves are the real engine functions, so the
//! assertions test real crash-consistency behaviour.

#![cfg(feature = "test-fs")]
#![allow(
    clippy::ptr_arg,
    clippy::manual_clamp,
    clippy::unusual_byte_groupings,
    clippy::doc_lazy_continuation
)]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use serde_json::json;

use topics::clock::{SharedClock, TestClock};
use topics::config::{SegmentConfig, ServerConfig};
use topics::engine::segwriter::SegmentWriter;
use topics::engine::Engine;
use topics::storage::testfs::{FakeDisk, FaultFs, FaultKind, FaultOp, TornDamage};
use topics::storage::Fs;
use topics::storage::{
    data_name, LocalSegmentStore, SegmentBuilder, SegmentPart, SegmentRecord, Tier, TopicTier,
};
use topics::types::{DeleteRequest, DiffRequest, RecordIn, TopicConfig, TopicType, WriteRequest};

// ===========================================================================
// Segment-store-level helpers (mirror tests/fault_cold_flip.rs): a real segment
// pair, a HOT+COLD tier wired onto a FakeDisk, dir-fsync, and a real on-recovery
// orphan sweep through SegmentWriter::reclaim_orphans_below.
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
/// `fs` (so a `crash()` on the underlying disk decides what survives).
fn open_tier_on(fs: Arc<dyn Fs>) -> Arc<TopicTier> {
    let hot = LocalSegmentStore::open_with(PathBuf::from(HOT_DIR), fs.clone()).unwrap();
    let cold = LocalSegmentStore::open_with(PathBuf::from(COLD_DIR), fs).unwrap();
    Arc::new(TopicTier::new(Box::new(hot), Some(Box::new(cold))))
}

/// The HOT+COLD tier on the FakeDisk directly (the common case).
fn open_tier(disk: &FakeDisk) -> Arc<TopicTier> {
    open_tier_on(disk.arc())
}

/// Make every directory entry written so far durable (the dir-fsync production
/// does after each `put`/`rename`/`unlink` lands — the in-memory model needs the
/// names fsynced for the namespace change to survive a `crash()`).
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
/// by `put`; we fsync the dir so the name survives a crash).
fn seal_hot(tier: &TopicTier, disk: &FakeDisk, id: u64, n: u64) {
    let (data, idx) = build_segment(id, n);
    tier.hot().put(id, &data, &idx).unwrap();
    sync_all_dirs(disk);
}

/// Re-derive a fresh tier from the current durable disk image (a "recovery" open).
fn rederive(disk: &FakeDisk) -> Arc<TopicTier> {
    open_tier(disk)
}

/// Drive the REAL on-recovery orphan sweep
/// (`SegmentWriter::reclaim_orphans_below`) over a tier, returning the number of
/// orphan segment ids dropped. The sweep's registry is empty, so the
/// "not registered" test is governed purely by the `live_floor` argument —
/// exactly what recovery passes after rebuilding the registry.
fn run_orphan_sweep(tier: &Arc<TopicTier>, live_floor: u64) -> usize {
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

/// Whether a tier currently considers `id` a complete, resolvable segment.
fn resolvable(tier: &TopicTier, id: u64) -> bool {
    tier.resolve(id).is_some()
}

// ===========================================================================
// F-RECLAIM-CRASH-BETWEEN-UNLINKS  (crash-point, high)
//
//   fault:  crash after deleting `.data` but before `.idx` in `delete()`.
//   oracle: delete is idempotent (NotFound tolerated); next reclaim/orphan-sweep
//           removes the leftover `.idx`; segment never resurrects;
//           reclaim_orphans_below cleans below-floor remnants.
// ===========================================================================

#[test]
fn f_reclaim_crash_between_unlinks() {
    let disk = FakeDisk::with_seed(0x5EC0_0001);
    let id = 1u64;

    // Steady state: one sealed segment, HOT only, durable (both parts present).
    {
        let tier = open_tier(&disk);
        seal_hot(&tier, &disk, id, 3);
        assert!(tier.hot().exists(id, SegmentPart::Data));
        assert!(tier.hot().exists(id, SegmentPart::Idx));
    }

    // Reclaim the segment, but crash BETWEEN the two unlinks: remove `.data`
    // (its directory-entry op is pending), make THAT change durable (dir fsync),
    // then crash BEFORE the `.idx` unlink. This reproduces the on-disk state at
    // the `reclaim::between_unlinks` failpoint: a stray `.idx` with no `.data`.
    {
        let fs = disk.arc();
        let data_path = PathBuf::from(HOT_DIR).join(data_name(id));
        fs.remove_file(&data_path).unwrap(); // unlink .data ...
        sync_all_dirs(&disk); // ... and make the unlink durable.
                              // ... crash here, before the .idx unlink ...
        disk.crash(TornDamage::None);
    }
    disk.reset_power();

    // Recovery view: the `.data` is gone (durably unlinked), the `.idx` lingers.
    let tier = rederive(&disk);
    assert!(
        !tier.hot().exists(id, SegmentPart::Data),
        ".data unlink was durable ⇒ gone"
    );
    assert!(
        tier.hot().exists(id, SegmentPart::Idx),
        "stray .idx lingers (its unlink never ran)"
    );
    // The segment NEVER resurrects: `list()` requires `.data`, so a lone `.idx`
    // is not a complete segment and `resolve` returns None.
    assert!(
        !resolvable(&tier, id),
        "segment with only a stray .idx must not resolve (never resurrects)"
    );
    assert!(
        tier.hot().list().unwrap().is_empty(),
        "list() requires .data ⇒ the stray .idx is not listed as a segment"
    );

    // The next reclaim is idempotent and removes the leftover `.idx`: a plain
    // `delete()` tolerates the already-missing `.data` (NotFound) and unlinks the
    // surviving `.idx`.
    tier.hot()
        .delete(id)
        .expect("delete idempotent over a half-unlinked segment");
    sync_all_dirs(&disk);
    let tier = rederive(&disk);
    assert!(
        !tier.hot().exists(id, SegmentPart::Idx),
        "the leftover .idx is removed by the next reclaim"
    );

    // And the on-recovery orphan sweep ALSO cleans below-floor remnants: re-create
    // the half-state, then prove `reclaim_orphans_below` drops the dead remnant.
    {
        // Re-seal then crash between unlinks again for the sweep variant.
        let tier = open_tier(&disk);
        seal_hot(&tier, &disk, id, 3);
        let fs = disk.arc();
        fs.remove_file(&PathBuf::from(HOT_DIR).join(data_name(id)))
            .unwrap();
        sync_all_dirs(&disk);
        disk.crash(TornDamage::None);
    }
    disk.reset_power();
    let tier = rederive(&disk);
    // live_floor above the dead segment's start ⇒ the orphan sweep drops the
    // remnant `.idx` (it is below floor and not registered). Idempotent re-sweep.
    let dropped = run_orphan_sweep(&tier, id + 10);
    sync_all_dirs(&disk);
    let tier = rederive(&disk);
    assert!(
        !tier.hot().exists(id, SegmentPart::Idx),
        "orphan sweep removed the below-floor stray .idx (dropped={dropped})"
    );
    assert_eq!(
        run_orphan_sweep(&tier, id + 10),
        0,
        "idempotent second sweep finds nothing"
    );
}

// ===========================================================================
// F-RECLAIM-ORPHAN-BELOW-FLOOR  (crash-point, high)
//
//   fault:  a segment cap/TTL/delete-reclaimed pre-crash whose unlink never
//           completed (a full segment file left below the recovered floor).
//   oracle: reclaim_orphans_below(live_floor) drops the dead below-floor file in
//           BOTH tiers; registered live segments untouched; idempotent second
//           sweep finds nothing (matches reclaim_orphans_below test).
// ===========================================================================

#[test]
fn f_reclaim_orphan_below_floor() {
    let disk = FakeDisk::with_seed(0x5EC0_0002);

    // Steady state: four sealed segments at distinct starts. Segs 1 & 2 live in
    // BOTH tiers (an old relocation), segs 3 & 4 hot only.
    {
        let tier = open_tier(&disk);
        for id in [1u64, 2, 3, 4] {
            let (data, idx) = build_segment(id, 1);
            tier.hot().put(id, &data, &idx).unwrap();
        }
        // Also place 1 & 2 in cold (orphans can live in either tier).
        for id in [1u64, 2] {
            let (data, idx) = build_segment(id, 1);
            tier.cold().unwrap().put(id, &data, &idx).unwrap();
        }
        sync_all_dirs(&disk);
        disk.crash(TornDamage::None); // everything durable ⇒ all survive.
    }
    disk.reset_power();

    // Recovery rebuilt the registry with base_seq=3 (seqs 1,2 were reclaimed
    // pre-crash but their unlinks never completed): the live floor is 3. The
    // orphan sweep must drop the two dead below-floor files (in BOTH tiers) and
    // leave the registered live segs 3,4 untouched.
    let tier = rederive(&disk);
    assert!(
        tier.hot().exists(1, SegmentPart::Data),
        "orphan 1 present pre-sweep"
    );
    assert!(
        tier.cold().unwrap().exists(2, SegmentPart::Data),
        "orphan 2 (cold) present"
    );

    let dropped = run_orphan_sweep(&tier, /*live_floor=*/ 3);
    sync_all_dirs(&disk);
    assert_eq!(
        dropped, 2,
        "two orphan segment ids (1,2) reclaimed below floor 3"
    );

    let tier = rederive(&disk);
    assert_eq!(tier.resolve(1), None, "orphan 1 dropped in both tiers");
    assert_eq!(tier.resolve(2), None, "orphan 2 dropped in both tiers");
    assert_eq!(
        tier.resolve(3),
        Some(Tier::Hot),
        "live registered seg 3 kept"
    );
    assert_eq!(
        tier.resolve(4),
        Some(Tier::Hot),
        "live registered seg 4 kept"
    );
    // Idempotent: a second sweep at the same floor finds nothing.
    assert_eq!(
        run_orphan_sweep(&tier, 3),
        0,
        "idempotent second sweep finds nothing"
    );
}

// ===========================================================================
// F-RECLAIM-EIO-UNLINK  (io-error, medium)
//
//   fault:  EIO on remove_file during a segment drop.
//   oracle: drop surfaces/swallows-and-retries; the segment stays on disk but is
//           below floor and dead ⇒ never served; the next sweep retries; floors
//           preserved, no resurrection.
// ===========================================================================

#[test]
fn f_reclaim_eio_unlink() {
    let disk = FakeDisk::with_seed(0x5EC0_0003);
    let id = 1u64;

    // Steady state: one durable HOT segment.
    {
        let tier = open_tier(&disk);
        seal_hot(&tier, &disk, id, 3);
    }

    // Drop the segment through a FaultFs that fails remove_file ONCE (a dead-ish
    // unlink). The first reclaim's `delete()` surfaces the io error and the
    // segment file STAYS on disk.
    {
        let faulty: Arc<dyn Fs> =
            FaultFs::new(disk.arc(), FaultOp::RemoveFile, FaultKind::Eio, 0, true).arc();
        let tier = open_tier_on(faulty);
        let res = tier.hot().delete(id);
        assert!(res.is_err(), "remove_file EIO surfaces from delete()");
    }

    // The segment is still on disk but is DEAD (below the live floor): even though
    // its files linger, a recovery that rebuilt the registry with base_seq above
    // it never registers it, so it is never served. Re-derive on the real disk
    // (no fault now) and prove it resolves only as a stray, not a live segment.
    let tier = rederive(&disk);
    // The EIO hit the FIRST remove_file (the `.data`), so the delete aborted with
    // both parts still present — the segment file lingers intact.
    assert!(
        tier.hot().exists(id, SegmentPart::Data) || tier.hot().exists(id, SegmentPart::Idx),
        "the segment file lingers after the failed unlink (will be retried)"
    );

    // The next sweep RETRIES the drop (no fault this time) and succeeds: a
    // below-floor, unregistered segment is reclaimed. Floors are preserved by
    // construction (the sweep only deletes files; it never advances/regresses a
    // floor), and the segment never resurrects.
    let dropped = run_orphan_sweep(&tier, id + 10);
    sync_all_dirs(&disk);
    let tier = rederive(&disk);
    assert_eq!(
        dropped, 1,
        "the next sweep retries and reclaims the dead segment"
    );
    assert_eq!(
        tier.resolve(id),
        None,
        "segment gone after the retried unlink"
    );
    assert!(!tier.hot().exists(id, SegmentPart::Data));
    assert!(!tier.hot().exists(id, SegmentPart::Idx));
    // Idempotent: nothing left to reclaim.
    assert_eq!(
        run_orphan_sweep(&tier, id + 10),
        0,
        "no resurrection; idempotent"
    );
}

// ===========================================================================
// Engine-level helpers (a tiny self-contained model, mirror crash_oracle.rs) for
// the WAL-driven reclaim strategies that need the fully-wired Engine.
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

fn open_engine(disk: &FakeDisk) -> Arc<Engine> {
    Engine::with_data_dir_fs(cfg(), clock(), disk.arc()).expect("engine opens through FakeDisk")
}

/// Make the WAL + meta directory names durable (the create+dir-fsync production
/// does at WAL open — modelled explicitly so the files survive a crash).
fn sync_wal_dir(disk: &FakeDisk) {
    let fs = disk.arc();
    let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("wal"));
    let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("meta"));
}

/// A flat dump of one recovered topic: head / earliest / count / live seqs /
/// per-seq data, and whether a from-0 read surfaces a tombstone.
#[derive(Debug, Clone, PartialEq, Eq)]
struct TopicDump {
    head: u64,
    earliest: u64,
    count: u64,
    seqs: Vec<u64>,
    data: BTreeMap<u64, String>,
    tombstone: bool,
}

/// Read the full recovered state of `name` through the engine's public API.
fn dump_topic(engine: &Engine, name: &str) -> Option<TopicDump> {
    let st = engine.topic_state(name, false).ok()?;
    let mut data = BTreeMap::new();
    let mut tombstone = false;
    let mut from = 0u64;
    loop {
        let d = engine
            .diff(
                name,
                DiffRequest {
                    from_seq: from,
                    limit: 1000,
                    node: None,
                    include_tags: true,
                    include_meta: true,
                    wait_ms: 0,
                    max_batch_bytes: 0,
                },
            )
            .ok()?;
        if d.tombstone.is_some() {
            tombstone = true;
        }
        for r in &d.records {
            let v = r
                .data
                .get("v")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| r.data.to_string());
            data.insert(r.seq, v);
        }
        if d.caught_up || d.records.is_empty() {
            break;
        }
        from = d.next_from_seq;
    }
    Some(TopicDump {
        head: st.head_seq,
        earliest: st.earliest_seq,
        count: st.count,
        seqs: data.keys().copied().collect(),
        data,
        tombstone,
    })
}

/// Append one durable record `data` with tag `tag`; returns the assigned seq.
fn append(engine: &Engine, name: &str, data: &str, tag: Option<&str>) -> u64 {
    let req = WriteRequest {
        records: vec![RecordIn {
            data: json!({ "v": data }),
            tag: tag.map(|t| t.to_string()),
            node: None,
            meta: None,
        }],
        node: None,
        idempotency_key: None,
        create: Some(true),
        config: None,
        disable_backpressure: true,
    };
    let resp = engine.write(name, req, true).expect("durable append acks");
    resp.last_seq
}

/// Create a durable topic.
fn put_durable_topic(engine: &Engine, name: &str) {
    engine
        .put_topic(
            name,
            TopicConfig {
                r#type: TopicType::Log,
                durable: true,
                cap_records: 0,
                ..Default::default()
            },
        )
        .expect("put_topic");
}

// ===========================================================================
// F-RECLAIM-DELETE-NEVER-RESURRECTS  (crash-point, critical)
//
//   fault:  crash immediately after a Delete WAL frame fsync but before segment
//           reclaim.
//   oracle: replay re-applies the Delete (idempotent); the deleted seqs are
//           absent after recovery AND across repeated recovery; delete_floor
//           preserved; no resurrection.
// ===========================================================================

#[test]
fn f_reclaim_delete_never_resurrects() {
    let disk = FakeDisk::new();

    // Workload: a durable topic with 5 records, then a voluntary prefix delete
    // (before_seq=3 ⇒ seqs 1,2 deleted) AND a tag delete (tag "drop" ⇒ seq 4).
    // The delete's WAL frame is fsync'd (durable append blocks on the group fsync,
    // and `delete()` logs the frame with durable=true and propagates its result).
    {
        let engine = open_engine(&disk);
        put_durable_topic(&engine, "jobs");
        append(&engine, "jobs", "a", Some("keep"));
        append(&engine, "jobs", "b", Some("keep"));
        append(&engine, "jobs", "c", Some("keep"));
        append(&engine, "jobs", "d", Some("drop"));
        append(&engine, "jobs", "e", Some("keep"));
        // Voluntary prefix delete: removes seqs 1,2 (silent, front-prefix).
        engine
            .delete(
                "jobs",
                DeleteRequest {
                    before_seq: Some(3),
                    match_: None,
                },
            )
            .expect("prefix delete acks (WAL frame fsync'd)");
        // Tag delete: removes seq 4 ("drop").
        engine
            .delete(
                "jobs",
                DeleteRequest {
                    before_seq: None,
                    match_: Some(topics::types::Filter::from_shorthand("drop")),
                },
            )
            .expect("tag delete acks");
        sync_wal_dir(&disk);
        // Crash immediately after the Delete fsync, before any background segment
        // reclaim — the Delete frames are durable; the reclaim has not run.
        disk.crash(TornDamage::None);
    }
    disk.reset_power();

    // Recovery #1: the Delete frames replay deterministically ⇒ seqs 1,2,4 are
    // absent, only 3 & 5 survive. No deleted seq resurrects.
    let d1 = {
        let engine = open_engine(&disk);
        let d = dump_topic(&engine, "jobs").expect("jobs survives recovery #1");
        assert_eq!(d.seqs, vec![3, 5], "deleted seqs 1,2,4 absent; 3,5 survive");
        assert_eq!(d.data[&3], "c");
        assert_eq!(d.data[&5], "e");
        assert_eq!(d.head, 5, "head preserved");
        assert!(
            !d.tombstone,
            "a voluntary delete is SILENT — no tombstone for the deleted prefix"
        );
        // delete_floor preserved: earliest jumps to the prefix-delete boundary (3).
        assert_eq!(
            d.earliest, 3,
            "delete_floor recovered ⇒ earliest at the boundary"
        );
        d
    };

    // Recovery #2 (idempotent): re-running recovery yields byte-identical state —
    // the deleted seqs STAY absent across repeated recovery (never resurrect).
    let d2 = {
        let engine = open_engine(&disk);
        dump_topic(&engine, "jobs").expect("jobs survives recovery #2")
    };
    assert_eq!(
        d1, d2,
        "recover(recover(x)) == recover(x); deletes never resurrect"
    );
}

// ===========================================================================
// F-RECLAIM-FRONT-PREFIX  (crash-point, high)
//
//   fault:  crash during front-prefix reclaim (earliest_seq advance) after a
//           voluntary delete.
//   oracle: earliest_seq recovers to the delete boundary; the deleted prefix
//           stays gone and SILENT (no tombstone); count/head consistent (matches
//           sigkill_durable test's jobs topic).
// ===========================================================================

#[test]
fn f_reclaim_front_prefix() {
    let disk = FakeDisk::new();

    // A durable "jobs" topic: append 6, then voluntarily delete the front prefix
    // (before_seq=4 ⇒ seqs 1,2,3 gone). The front reclaim (earliest_seq advance +
    // index compaction) runs inside `apply_delete`; we crash right after the
    // Delete fsync, which is "between delete commit and index compaction" as far
    // as durability is concerned (the compaction is in-memory; only the WAL Delete
    // frame is durable, and it replays the same boundary on recovery).
    {
        let engine = open_engine(&disk);
        put_durable_topic(&engine, "jobs");
        for i in 1..=6 {
            append(&engine, "jobs", &i.to_string(), None);
        }
        engine
            .delete(
                "jobs",
                DeleteRequest {
                    before_seq: Some(4),
                    match_: None,
                },
            )
            .expect("front-prefix delete acks");
        sync_wal_dir(&disk);
        disk.crash(TornDamage::None);
    }
    disk.reset_power();

    let engine = open_engine(&disk);
    let d = dump_topic(&engine, "jobs").expect("jobs survives recovery");

    // earliest_seq recovered to the delete boundary (4); the deleted prefix is gone
    // and SILENT (no tombstone); count/head consistent.
    assert_eq!(
        d.seqs,
        vec![4, 5, 6],
        "the deleted front prefix [1..=3] stays gone"
    );
    assert_eq!(
        d.earliest, 4,
        "earliest_seq recovers to the delete boundary"
    );
    assert_eq!(d.head, 6, "head consistent");
    assert_eq!(d.count, 3, "count == surviving live records (4,5,6)");
    assert!(
        !d.tombstone,
        "a voluntary front-prefix delete is SILENT — no tombstone"
    );
    assert_eq!(d.data[&4], "4");
    assert_eq!(d.data[&6], "6");

    // Idempotent recovery: the boundary stays put across a second recovery.
    let engine2 = open_engine(&disk);
    let d2 = dump_topic(&engine2, "jobs").expect("jobs survives recovery #2");
    assert_eq!(
        d, d2,
        "front-prefix boundary stable across repeated recovery"
    );
}
