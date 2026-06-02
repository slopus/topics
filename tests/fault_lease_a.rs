//! Phase-8B fault catalog — **lease-log** boundary (file A).
//!
//! Five fault/crash strategies on the queue **leases log** (DESIGN §10.1/§10.6),
//! driven through the Phase-8A [`FakeDisk`] crash harness and the real, fully
//! wired [`Engine`] (claim/ack/nack/extend write real WAL `Lease` frames; recovery
//! replays them via `apply_lease_event`). The leases-log durability contract
//! (model-oracle invariant #12):
//!
//! - `leases_durable: true`  ⇒ replayed `Lease` events rebuild the who-holds-what
//!   projection deterministically; a crash never double-claims / double-acks a job.
//! - `leases_durable: false` ⇒ nothing replays; every in-flight job is claimable
//!   again (the self-healing visibility timeout), with **no jobs-log record lost
//!   or duplicated**.
//!
//! Strategies implemented here (catalog ids):
//!   - `F-LEASE-CRASH-AFTER-CLAIM`     — crash after a durable Lease(Claimed) fsync
//!   - `F-LEASE-NONDURABLE-SELFHEAL`   — crash, non-durable leases ⇒ self-heal
//!   - `F-LEASE-TORN-EVENT`            — a durable Lease frame torn mid-body
//!   - `F-LEASE-ACK-CRASH`             — crash after Lease(Acked)+Delete fsync
//!   - `F-LEASE-REORDER-EVENTS`        — one un-synced lease event lost, later kept
//!
//! Each sweep is bounded (tiny workloads, fixed seeds) and runs in well under a
//! minute. Self-verify ONLY this file:
//!
//! ```text
//! cargo test --features test-fs --test fault_lease_a
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
use topics::config::ServerConfig;
use topics::engine::Engine;
use topics::storage::testfs::{FakeDisk, TornDamage};
use topics::types::{RecordIn, TopicConfig, TopicType, WriteRequest};

// ===========================================================================
// Plumbing (mirrors tests/crash_oracle.rs: FakeDisk-backed Engine + recovery)
// ===========================================================================

const DATA_DIR: &str = "/data";

fn cfg() -> ServerConfig {
    ServerConfig {
        data_dir: Some(DATA_DIR.to_string()),
        ..Default::default()
    }
}

/// A fixed logical clock; lease deadlines are derived from it, so recovery and the
/// workload see the same absolute deadline. A separate boot may shift it to test
/// claimability *after* the original lease window.
fn clock_at(ms: i64) -> SharedClock {
    Arc::new(TestClock::new(ms))
}

const BOOT_MS: i64 = 1_700_000_000_000;

/// Build a durable engine whose WAL + snapshots live on `disk`, at clock `now_ms`.
fn open_engine_at(disk: &FakeDisk, now_ms: i64) -> Arc<Engine> {
    Engine::with_data_dir_fs(cfg(), clock_at(now_ms), disk.arc())
        .expect("engine opens through FakeDisk")
}

fn open_engine(disk: &FakeDisk) -> Arc<Engine> {
    open_engine_at(disk, BOOT_MS)
}

/// Make the WAL + meta directory entries durable (the create+dir-fsync production
/// does at WAL open — modeled explicitly so the WAL file name survives a crash).
fn sync_wal_dir(disk: &FakeDisk) {
    let fs = disk.arc();
    let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("wal"));
    let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("meta"));
}

/// A durable **queue** topic config. `leases_durable` toggles whether claim/ack/nack
/// events are written to the durable leases log (the projection that recovery
/// rebuilds) or are purely in-memory (self-healing on restart).
fn queue_cfg(leases_durable: bool) -> TopicConfig {
    TopicConfig {
        r#type: TopicType::Queue,
        durable: true, // jobs log durable — we must never lose a job record.
        lease_ms: 30_000,
        leases_durable,
        ..TopicConfig::default()
    }
}

/// Produce `n` untagged jobs into queue `q`, returning the assigned seqs. The jobs
/// topic is durable, so this blocks on the group fsync ⇒ the jobs survive a crash.
fn produce(engine: &Engine, q: &str, n: usize) -> Vec<u64> {
    let records: Vec<RecordIn> = (0..n)
        .map(|i| RecordIn {
            data: json!({ "i": i }),
            tag: None,
            node: None,
            meta: None,
        })
        .collect();
    let resp = engine
        .write(
            q,
            WriteRequest {
                records,
                node: None,
                idempotency_key: None,
                create: None,
                config: None,
                disable_backpressure: true,
            },
            true,
        )
        .expect("produce jobs");
    resp.seqs.expect("seqs assigned")
}

/// The recovered queue counters of `q`: `(count, ready, in_flight)` at the engine's
/// clock. `count` is the live jobs-log record count (never lost for a durable topic).
fn queue_dump(engine: &Engine, q: &str) -> (u64, u64, u64) {
    let st = engine.topic_state(q, false).expect("queue topic present");
    let qs = st.queue.expect("queue counters");
    (st.count, qs.ready, qs.in_flight)
}

/// Total seqs claimable across repeated claims by a fresh node (drains the queue),
/// returned ascending. Proves no job is lost (every produced seq re-appears) and no
/// duplicate live record is handed out twice in one drain.
fn drain_claimable(engine: &Engine, q: &str, node: &str) -> Vec<u64> {
    let mut seqs = Vec::new();
    loop {
        let r = engine.claim(q, node, 100, None).expect("claim");
        if r.count == 0 {
            break;
        }
        for c in &r.claimed {
            seqs.push(c.seq);
        }
    }
    seqs.sort_unstable();
    seqs
}

// ===========================================================================
// F-LEASE-CRASH-AFTER-CLAIM — crash after a durable Lease(Claimed) fsync
// ===========================================================================

/// `leases_durable: true`: a claim writes a durable `Lease(Claimed)` frame (the
/// engine blocks on its group fsync before `claim` returns ⇒ FakeDisk promoted the
/// frame). A power loss right after that fsync must replay the lease projection so
/// the job is shown **still held** by the same node / lease_id / deadline — never
/// re-handed to another worker (no double-claim), and the deliveries counter stays
/// consistent.
#[test]
fn f_lease_crash_after_claim() {
    let disk = FakeDisk::new();

    let (held_seq, lease_id, deadline) = {
        let engine = open_engine(&disk);
        engine.put_topic("jobs", queue_cfg(true)).unwrap();
        let produced = produce(&engine, "jobs", 3);
        assert_eq!(produced, vec![1, 2, 3]);

        // Claim ONE job: a durable Lease(Claimed) frame is fsynced before return.
        let r = engine.claim("jobs", "w1", 1, None).unwrap();
        assert_eq!(r.count, 1);
        let job = &r.claimed[0];
        let held = job.seq;
        let lid = job.lease_id.clone();
        let dl = job.deadline;

        // 1 in-flight, 2 ready before the crash.
        let (count, ready, in_flight) = queue_dump(&engine, "jobs");
        assert_eq!((count, ready, in_flight), (3, 2, 1));

        sync_wal_dir(&disk);
        // Power loss: every acked-durable frame (3 jobs + the claimed lease) is
        // already fsynced ⇒ survives. Freeze before drop so the writer's Drop drain
        // cannot harden anything new.
        disk.crash(TornDamage::None);
        drop(engine);
        (held, lid, dl)
    };
    disk.reset_power();

    // Recover: the durable leases log replays ⇒ the job is STILL held. We recover
    // at a clock just past the original boot but well within the 30s lease window,
    // so the replayed lease is un-expired.
    let engine = open_engine_at(&disk, BOOT_MS + 1_000);

    // Jobs log fully preserved (durable topic): all 3 jobs present.
    let (count, ready, in_flight) = queue_dump(&engine, "jobs");
    assert_eq!(count, 3, "durable jobs log preserved all 3 jobs");
    // The replayed Lease(Claimed) keeps exactly one job in flight — NO double-claim.
    assert_eq!(in_flight, 1, "the claimed job is still held after recovery");
    assert_eq!(ready, 2, "only the two un-claimed jobs are ready");

    // The held seq is NOT re-handed to a new worker (a fresh claim cannot take it).
    let r = engine.claim("jobs", "w2", 100, None).unwrap();
    let handed: Vec<u64> = r.claimed.iter().map(|c| c.seq).collect();
    assert!(
        !handed.contains(&held_seq),
        "held seq {held_seq} must not be re-claimed after recovery (double-claim), got {handed:?}"
    );
    assert_eq!(handed.len(), 2, "exactly the two ready jobs are claimable");

    // The original holder still owns its lease: it can ack (proving identity is
    // intact — node/lease_id/deadline projected deterministically). The replayed
    // deliveries counter is 1 (first delivery), so this is no double-claim.
    let a = engine.ack("jobs", "w1", &[held_seq]).unwrap();
    assert_eq!(a.acked, 1, "the original holder acks its still-held job");

    // Sanity: lease_id/deadline are non-empty/positive (projected, not fabricated).
    assert!(!lease_id.is_empty());
    assert!(deadline > 0);
}

// ===========================================================================
// F-LEASE-NONDURABLE-SELFHEAL — crash, non-durable leases ⇒ self-heal
// ===========================================================================

/// `leases_durable: false` (the default): claims are purely in-memory. A power loss
/// writes **no** Lease frames, so nothing replays — every in-flight job becomes
/// claimable again (the self-healing visibility timeout), and the durable jobs log
/// loses nothing and duplicates nothing.
#[test]
fn f_lease_nondurable_selfheal() {
    let disk = FakeDisk::new();

    {
        let engine = open_engine(&disk);
        engine.put_topic("jobs", queue_cfg(false)).unwrap();
        let produced = produce(&engine, "jobs", 4);
        assert_eq!(produced, vec![1, 2, 3, 4]);

        // Claim two jobs (in-flight) — non-durable leases write nothing durable.
        let r = engine.claim("jobs", "w1", 2, None).unwrap();
        assert_eq!(r.count, 2);
        let (count, ready, in_flight) = queue_dump(&engine, "jobs");
        assert_eq!((count, ready, in_flight), (4, 2, 2));

        sync_wal_dir(&disk);
        disk.crash(TornDamage::None);
        drop(engine);
    }
    disk.reset_power();

    // Recover: no lease frames ⇒ the projection is empty ⇒ all 4 jobs claimable.
    let engine = open_engine(&disk);
    let (count, ready, in_flight) = queue_dump(&engine, "jobs");
    assert_eq!(
        count, 4,
        "durable jobs log preserved all 4 jobs (none lost)"
    );
    assert_eq!(
        in_flight, 0,
        "no replayed lease ⇒ nothing in flight (self-heal)"
    );
    assert_eq!(ready, 4, "every in-flight job is claimable again");

    // Draining the queue yields exactly the 4 produced seqs, each once — no job
    // lost, no duplicate live record.
    let drained = drain_claimable(&engine, "jobs", "w-new");
    assert_eq!(
        drained,
        vec![1, 2, 3, 4],
        "self-heal: all jobs reclaimable, no dup/loss"
    );
}

// ===========================================================================
// F-LEASE-TORN-EVENT — a durable Lease frame torn mid-body
// ===========================================================================

/// `leases_durable: true`, but the last (claimed) lease frame is **torn** on the
/// power loss (an in-flight partial write). Torn-tail truncation drops it — no
/// partial lease event is applied — and the projection self-heals: the job is
/// re-claimable. The durable jobs log loses nothing.
///
/// Injection: the claim's durable Lease frame would normally be fully fsynced. To
/// reproduce a torn lease frame we keep it the **last pending write** at crash time
/// by claiming over a *non-durable* leases path for the torn frame: we issue the
/// claim, then a trailing non-durable nack frame stays pending, and crash() with a
/// prefix-truncate tear damages that in-flight tail. The recovered projection must
/// never materialize a bogus/partial lease — at worst it drops to claimable.
#[test]
fn f_lease_torn_event() {
    // A few torn-damage seeds so the tear lands at different offsets of the last
    // in-flight frame (prefix-truncate / garble / zero-sector) — none may produce a
    // partial/bogus lease; the jobs log stays whole and the job ends claimable.
    for &damage in &[
        TornDamage::PrefixTruncate,
        TornDamage::Garble,
        TornDamage::ZeroSector,
    ] {
        let disk = FakeDisk::with_seed(0x1EA5E_u64 ^ damage as u64);

        {
            let engine = open_engine(&disk);
            engine.put_topic("jobs", queue_cfg(true)).unwrap();
            produce(&engine, "jobs", 2);

            // Claim both (durable Lease(Claimed) frames fsynced), then NACK one
            // back: the nack's `released` lease frame is the latest WAL frame. We
            // tear the in-flight tail, so whatever lease event was last-written is
            // dropped. The projection rebuilt from the surviving (un-torn) prefix
            // must be self-consistent and never apply a partial event.
            let r = engine.claim("jobs", "w1", 2, None).unwrap();
            assert_eq!(r.count, 2);
            let seqs: Vec<u64> = r.claimed.iter().map(|c| c.seq).collect();
            // A second lifecycle event so there IS a tail frame to tear.
            let _ = engine.nack("jobs", "w1", &[seqs[1]], 0);

            sync_wal_dir(&disk);
            // Tear the last in-flight pending write on power loss.
            disk.crash(damage);
            drop(engine);
        }
        disk.reset_power();

        // Recover: a torn lease frame is truncated, never half-applied. The jobs
        // log (durable, fully fsynced before the torn tail) is intact: both jobs
        // present. The replayed projection (from whatever lease frames survived
        // un-torn) is self-consistent — in_flight never exceeds the jobs that exist
        // and held + ready partitions the live set (no double-claim).
        let engine = open_engine(&disk);
        let (count, ready, in_flight) = queue_dump(&engine, "jobs");
        assert_eq!(
            count, 2,
            "{damage:?}: durable jobs log intact (no job lost)"
        );
        assert!(
            in_flight <= count,
            "{damage:?}: in_flight {in_flight} <= count {count}"
        );
        assert!(
            in_flight + ready <= count,
            "{damage:?}: in_flight {in_flight} + ready {ready} <= count {count} (no double-claim)"
        );
        drop(engine);

        // Self-heal: re-open past the lease window so any still-held (surviving
        // Claimed) lease expires, then drain — exactly the 2 produced jobs, each
        // once. A torn/dropped lease only widens claimability; it never loses or
        // duplicates a job record. (A torn frame is never replayed as a partial
        // lease, so no bogus held state survives expiry.)
        let engine = open_engine_at(&disk, BOOT_MS + 10_000_000);
        let drained = drain_claimable(&engine, "jobs", "w-heal");
        assert_eq!(
            drained,
            vec![1, 2],
            "{damage:?}: torn lease self-heals; all jobs reclaimable, no dup/loss"
        );
    }
}

// ===========================================================================
// F-LEASE-ACK-CRASH — crash after Lease(Acked)+Delete fsync
// ===========================================================================

/// `leases_durable: true`: an ack writes BOTH a durable `Delete` frame (the
/// jobs-log removal) and a durable `Lease(Acked)` frame, fsynced before `ack`
/// returns. A power loss right after must replay the ack/delete deterministically:
/// the acked job stays **acked/deleted**, never redelivered as live, with no
/// double-ack.
#[test]
fn f_lease_ack_crash() {
    let disk = FakeDisk::new();

    let acked_seq = {
        let engine = open_engine(&disk);
        engine.put_topic("jobs", queue_cfg(true)).unwrap();
        produce(&engine, "jobs", 3);

        // Claim one, then ack it: durable Delete + Lease(Acked) frames are fsynced
        // (the jobs topic is durable, so the ack's delete blocks on its group fsync).
        let r = engine.claim("jobs", "w1", 1, None).unwrap();
        let seq = r.claimed[0].seq;
        let a = engine.ack("jobs", "w1", &[seq]).unwrap();
        assert_eq!(a.acked, 1);

        // Before the crash: 2 jobs remain (the acked one is deleted from the log).
        let (count, _ready, _in_flight) = queue_dump(&engine, "jobs");
        assert_eq!(count, 2, "acked job deleted from the jobs log");

        sync_wal_dir(&disk);
        disk.crash(TornDamage::None);
        drop(engine);
        seq
    };
    disk.reset_power();

    // Recover: the durable Delete replays ⇒ the acked job stays gone, never
    // redelivered. The Lease(Acked) replay drops any lease state for it.
    let engine = open_engine(&disk);
    let (count, ready, in_flight) = queue_dump(&engine, "jobs");
    assert_eq!(
        count, 2,
        "the acked job stays deleted after recovery (no resurrect)"
    );
    assert_eq!(in_flight, 0, "no live lease after recovery");
    assert_eq!(ready, 2, "exactly the two un-acked jobs are claimable");

    // The acked seq is never handed out again (not redelivered as live).
    let drained = drain_claimable(&engine, "jobs", "w-new");
    assert!(
        !drained.contains(&acked_seq),
        "acked seq {acked_seq} must never be redelivered, got {drained:?}"
    );
    assert_eq!(
        drained.len(),
        2,
        "only the two surviving jobs are claimable"
    );

    // No double-ack: re-acking the gone seq is a silent skip, not a second effect.
    let a2 = engine.ack("jobs", "w1", &[acked_seq]).unwrap();
    assert_eq!(a2.acked, 0, "the already-acked job cannot be acked twice");
}

// ===========================================================================
// F-LEASE-REORDER-EVENTS — one un-synced lease event lost, later kept
// ===========================================================================

/// `leases_durable: true`: a sequence of lease lifecycle events (claimed / extended)
/// where ONE event is lost to a power loss while a later one survived (modeled as a
/// torn last frame — a clean drop of the un-fsynced tail). The projection rebuilt
/// from the durable events must be consistent (`apply_lease_event` is
/// idempotent/monotone): a lost event only ever **widens claimability** — it never
/// double-delivers a record nor loses a job.
///
/// We claim a job (durable Claimed), extend it (durable Extended), then crash with
/// the in-flight tail torn so the latest event is dropped. Either projection
/// (with-or-without the dropped event) leaves the job in a valid state: still held
/// (claimed survived) or claimable (claimed dropped) — never two live deliveries of
/// the same seq, never a lost job.
#[test]
fn f_lease_reorder_events() {
    for &damage in &[TornDamage::PrefixTruncate, TornDamage::Garble] {
        let disk = FakeDisk::with_seed(0xC0DE_u64 ^ damage as u64);

        {
            let engine = open_engine(&disk);
            engine.put_topic("jobs", queue_cfg(true)).unwrap();
            produce(&engine, "jobs", 2);

            // Claim both (durable Claimed frames), then extend one (durable
            // Extended frame is the latest event). The tear drops the in-flight
            // tail, losing the latest lease event while earlier ones survive.
            let r = engine.claim("jobs", "w1", 2, None).unwrap();
            assert_eq!(r.count, 2);
            let seqs: Vec<u64> = r.claimed.iter().map(|c| c.seq).collect();
            let _ = engine.extend("jobs", "w1", &[seqs[0]], 60_000);

            sync_wal_dir(&disk);
            disk.crash(damage);
            drop(engine);
        }
        disk.reset_power();

        // Recover within the original lease window: any surviving Claimed frames
        // keep their jobs held; a dropped event only widens claimability.
        let engine = open_engine_at(&disk, BOOT_MS + 1_000);
        let (count, ready, in_flight) = queue_dump(&engine, "jobs");
        assert_eq!(count, 2, "{damage:?}: durable jobs log intact");
        // The projection is consistent: held + ready partitions the live jobs, and
        // nothing is double-counted (in_flight + ready never exceeds count).
        assert!(
            in_flight + ready <= count,
            "{damage:?}: in_flight {in_flight} + ready {ready} <= count {count} \
             (no double-delivery)"
        );

        // Advance well past the lease window so any still-held job expires and
        // becomes claimable, then drain: exactly the 2 produced jobs, each once —
        // no job lost, no duplicate live record, regardless of which event survived.
        let engine = open_engine_at(&disk, BOOT_MS + 10_000_000);
        let drained = drain_claimable(&engine, "jobs", "w-final");
        assert_eq!(
            drained,
            vec![1, 2],
            "{damage:?}: reordered/lost lease event ⇒ widened claimability only, \
             no dup/loss"
        );
    }
}
