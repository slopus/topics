//! Phase-8B fault catalog — **lease-log** boundary, file B.
//!
//! Strategy implemented (see `docs/FAULT_TESTING.md` / the fault catalog):
//!
//!   * `F-CLOCK-BACKWARD-LEASE` (crash-point, lease-log, sev medium):
//!     the wall-clock jumps **backward** across a restart while lease deadlines
//!     are stored as absolute epoch-ms. The oracle:
//!       - `seq` stays monotonic (it is a *logical* counter, not wall-clock);
//!       - a backward jump must **not** revive an expired lease nor reorder
//!         frames;
//!       - lease validity uses the **durable absolute deadline**, never a
//!         relative `now`.
//!
//! Built on the Phase-8A harness exactly like `tests/crash_oracle.rs`:
//! a real, fully-wired [`Engine`] driven through an in-memory [`FakeDisk`],
//! `disk.crash()` for the power loss, `disk.reset_power()` + a *second*
//! `Engine::with_data_dir_fs` (with a clock set to an **earlier** epoch) for the
//! backward-jump restart, then the recovered engine is inspected through the
//! public claim/ack/state API.
//!
//! ```text
//! cargo test --features test-fs --test fault_lease_b
//! ```

#![cfg(feature = "test-fs")]

use std::path::PathBuf;
use std::sync::Arc;

use serde_json::json;

use streams::clock::{SharedClock, TestClock};
use streams::config::ServerConfig;
use streams::engine::Engine;
use streams::storage::testfs::FakeDisk;
use streams::types::{BoxConfig, BoxType, RecordIn, WriteRequest};

const DATA_DIR: &str = "/data";

/// A large, plausible epoch-ms the FIRST boot runs at (well past the Unix
/// epoch so a backward jump still stays positive).
const T0: i64 = 1_900_000_000_000;

/// The lease (visibility timeout) for the queue under test, ms.
const LEASE_MS: u64 = 30_000;

fn cfg() -> ServerConfig {
    ServerConfig {
        data_dir: Some(DATA_DIR.to_string()),
        ..Default::default()
    }
}

/// Build an engine on `disk` whose clock starts at `start_ms` (so the second
/// boot can present an *earlier* wall-clock than the first). Returns the engine
/// plus the [`TestClock`] handle so the caller can advance it post-recovery (to
/// prove the recovered lease deadline is the genuine *absolute* one).
fn open_engine_at(disk: &FakeDisk, start_ms: i64) -> (Arc<Engine>, TestClock) {
    let tc = TestClock::new(start_ms);
    let clock: SharedClock = Arc::new(tc.clone());
    let engine =
        Engine::with_data_dir_fs(cfg(), clock, disk.arc()).expect("engine opens through FakeDisk");
    (engine, tc)
}

/// Make every WAL/meta directory entry durable (the create+dir-fsync prod does
/// at WAL open) so the files survive the simulated power loss.
fn sync_dirs(disk: &FakeDisk) {
    let fs = disk.arc();
    let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("wal"));
    let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("meta"));
}

/// A durable queue with a durable leases log (so claim frames are fsynced and
/// replay rebuilds the who-holds-what projection — DESIGN §10.1, invariant 12).
fn durable_queue_cfg() -> BoxConfig {
    BoxConfig {
        r#type: BoxType::Queue,
        durable: true,
        leases_durable: true,
        lease_ms: LEASE_MS,
        ..Default::default()
    }
}

/// Append one durable job; returns its assigned seq. The durable box fsyncs, so
/// the returned seq is acked ⇒ must survive any later crash.
fn append_job(engine: &Engine, name: &str, data: &str) -> u64 {
    let req = WriteRequest {
        records: vec![RecordIn {
            data: json!({ "v": data }),
            tag: None,
            node: None,
            meta: None,
        }],
        node: None,
        idempotency_key: None,
        create: Some(true),
        config: None,
        disable_backpressure: true,
    };
    engine
        .write(name, req, true)
        .expect("durable append acks")
        .last_seq
}

// ===========================================================================
// F-CLOCK-BACKWARD-LEASE
// ===========================================================================

/// Boot at T0, durably claim a job (absolute deadline = T0 + LEASE_MS recorded
/// in a durable Lease frame), power-loss, then reboot with the clock set
/// **backward** to an earlier epoch. The recovered engine must:
///
///   1. keep `head_seq` monotonic — seq is a logical counter, the backward jump
///      cannot regress it (invariant 3);
///   2. NOT lose or fabricate any acked durable job record (invariant 1/2);
///   3. rebuild the lease projection from the **durable absolute deadline**, so
///      the still-future-deadline lease is shown HELD (`in_flight == 1`) even
///      though `now_back < T0` — and the held job is NOT re-claimable by another
///      worker (no double-claim, invariant 12);
///   4. let a fresh append continue at exactly `head + 1` (clock-independent);
///   5. let the original holder `ack` the job (lease identity survived) and the
///      ack take effect deterministically (no double-ack / resurrection).
///
/// The backward jump (`T_back < T0`) keeps the absolute deadline in the future,
/// which is precisely the trap: an implementation that recomputed validity from
/// a *relative* `now` (e.g. "deadline = now + lease") would either revive an
/// expired lease or mis-expire a live one. Absolute durable deadlines are
/// immune.
#[test]
fn f_clock_backward_lease() {
    let disk = FakeDisk::new();

    // The backward target: an epoch EARLIER than T0 but still strictly less than
    // the durable deadline T0 + LEASE_MS, so a correctly-stored absolute deadline
    // is still in the future at the second boot (the lease is genuinely live).
    let t_back = T0 - 60_000; // one minute before the first boot.
    assert!(t_back < T0 + LEASE_MS as i64, "deadline stays in the future after the jump");

    // ---- Boot 1 @ T0: durable queue, 3 durable jobs, claim job seq=1. --------
    let (claimed_seq, deadline, head_before) = {
        let (engine, _tc) = open_engine_at(&disk, T0);
        let (_created, _cfg) = engine
            .put_box("q", durable_queue_cfg())
            .expect("create durable queue");

        let s1 = append_job(&engine, "q", "j1");
        let s2 = append_job(&engine, "q", "j2");
        let s3 = append_job(&engine, "q", "j3");
        assert_eq!((s1, s2, s3), (1, 2, 3), "seqs are the logical 1..=3");

        // Claim exactly one job for node "w1" with the box lease. The Claimed
        // lease frame is fsynced (leases_durable) ⇒ acked ⇒ must replay.
        let resp = engine.claim("q", "w1", 1, None).expect("claim succeeds");
        assert_eq!(resp.count, 1, "exactly one job leased");
        let job = &resp.claimed[0];
        assert_eq!(job.seq, 1, "the oldest claimable seq is leased first");
        assert_eq!(job.deadline, T0 + LEASE_MS as i64, "absolute deadline = T0 + lease");
        assert_eq!(resp.ready, 2, "2 jobs remain claimable");

        let head = engine.box_state("q", false).unwrap().head_seq;
        assert_eq!(head, 3);

        // Persist directory entries, then a clean power loss with no torn damage:
        // every acked durable frame (3 appends + 1 lease) is already fsynced.
        sync_dirs(&disk);
        disk.crash(streams::storage::testfs::TornDamage::None);
        drop(engine);
        (job.seq, job.deadline, head)
    };
    disk.reset_power();

    // ---- Boot 2 @ T_back (clock JUMPED BACKWARD): recover + assert oracle. ----
    let (engine, tc2) = open_engine_at(&disk, t_back);

    // (1) SEQ MONOTONIC: head is the logical counter, untouched by the clock jump.
    let st = engine.box_state("q", false).expect("queue recovered");
    assert_eq!(st.head_seq, head_before, "head_seq monotonic across a backward clock jump");
    assert_eq!(st.head_seq, 3);

    // (2) NO LOSS / NO FABRICATION: all 3 acked durable job records survive, and
    //     the queue counters are derived against the (earlier) now.
    let q = st.queue.as_ref().expect("queue sub-state present");
    // (3) The durable absolute deadline is honored: the still-future lease is
    //     HELD, not revived/expired by the backward jump.
    assert_eq!(
        q.in_flight, 1,
        "the durably-claimed lease is still held after the backward jump \
         (absolute deadline {deadline} > now {t_back}); a backward jump must NOT \
         expire/revive it"
    );
    assert_eq!(
        q.ready, 2,
        "the other 2 jobs are claimable; the held one is not double-counted"
    );

    // (3a) DEADLINE IS GENUINELY ABSOLUTE (not a relative "now + lease" recomputed
    //      at boot): briefly advance the recovered clock to one ms PAST the durable
    //      absolute deadline and confirm the lease then expires (in_flight drops,
    //      the job becomes claimable). A relative recompute anchored to the (earlier)
    //      boot time would still show it held here. Then rewind so the rest of the
    //      checks run at `t_back` as before.
    tc2.set(deadline + 1);
    let q_expired = engine.box_state("q", false).unwrap().queue.expect("queue");
    assert_eq!(
        q_expired.in_flight, 0,
        "past the durable absolute deadline {deadline} the lease expires — proving \
         recovery restored the ABSOLUTE deadline, not a boot-relative one"
    );
    assert_eq!(q_expired.ready, 3, "all 3 jobs claimable once the lease truly expired");
    tc2.set(t_back); // rewind to the backward-jumped time for the remaining oracle.

    // The expiry sweep above released seq=1 onto the reclaim freelist; re-claim it
    // for w1 so the held-lease invariants below continue against a held job.
    let reclaim = engine.claim("q", "w1", 1, None).expect("w1 re-claims the expired job");
    assert_eq!(reclaim.claimed.len(), 1);
    assert_eq!(reclaim.claimed[0].seq, claimed_seq, "the expired job is re-handed to w1 first");

    // (3b) NO DOUBLE-CLAIM: a different worker claiming now must NOT be handed the
    //      held seq=1 — only the 2 free jobs (seqs 2,3) are claimable.
    let other = engine.claim("q", "w2", 5, None).expect("w2 claim");
    let other_seqs: Vec<u64> = other.claimed.iter().map(|j| j.seq).collect();
    assert_eq!(
        other_seqs,
        vec![2, 3],
        "the held lease (seq {claimed_seq}) is not re-handed-out after the backward jump"
    );

    // (4) FRESH APPEND continues at head+1 — logical, clock-independent.
    let s4 = append_job(&engine, "q", "j4");
    assert_eq!(s4, 4, "a post-recovery append continues at head+1, not affected by the clock");

    // (5) The original holder can still ack its job (lease identity survived the
    //     backward jump); the ack deletes the job deterministically (no
    //     resurrection on a further recovery).
    let ack = engine.ack("q", "w1", &[claimed_seq]).expect("w1 acks its held job");
    assert_eq!(ack.acked, 1, "the durably-held lease is ackable by its original holder");
    assert_eq!(ack.skipped, Vec::<u64>::new());

    drop(engine);

    // ---- Boot 3 (recover again, clock still earlier): convergent + no revival.
    let (engine2, _tc3) = open_engine_at(&disk, t_back);
    let st2 = engine2.box_state("q", false).expect("queue recovered again");
    assert_eq!(st2.head_seq, 4, "head still monotonic after the ack + re-recovery");
    // seq 1 was acked (deleted) before this boot; it must NOT resurrect.
    let d = engine2
        .diff(
            "q",
            streams::types::DiffRequest {
                from_seq: 0,
                limit: 1000,
                node: None,
                include_tags: true,
                include_meta: false,
                wait_ms: 0,
            },
        )
        .expect("diff");
    let surviving: Vec<u64> = d.records.iter().map(|r| r.seq).collect();
    assert!(
        !surviving.contains(&claimed_seq),
        "the acked/deleted job (seq {claimed_seq}) must not resurrect after a backward-clock reboot"
    );
    // The remaining jobs (2,3,4) are intact and contiguous (no gap punched).
    assert_eq!(surviving, vec![2, 3, 4], "survivors are a dense set, no gap, no fabrication");
}
