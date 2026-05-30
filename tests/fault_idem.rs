//! Phase-8B fault catalog — **idempotency** boundary (2 strategies).
//!
//! The dedupe map (`idempotency_key → assigned seqs`, API §0.8) is an in-memory,
//! best-effort window: it is **never** persisted (not to the WAL, not to a
//! snapshot — see `src/storage/snapshot.rs` "Idempotency-dedupe state is
//! intentionally not persisted"). These tests pin the contract from
//! `oracle_invariants[12]`:
//!
//! ```text
//!   IDEMPOTENCY WINDOW IS BEST-EFFORT, NEVER UNSAFE: the dedupe map is not
//!   persisted, so after a crash a retried idempotency_key may re-execute
//!   (allowed) but must never produce a gap, never assign a duplicate seq to a
//!   live record, and never resurrect a deleted seq — the retry is a fresh
//!   contiguous append at head+1 or a no-op.
//! ```
//!
//! Both drive the *real, fully-wired* [`Engine`] through an in-memory
//! [`FakeDisk`] (the Phase-8A harness) — `Engine::with_data_dir_fs` for recovery
//! and the WAL writer, `disk.crash()` / `disk.reset_power()` for the power-loss
//! model — and reuse the `crash_oracle.rs` scaffolding patterns.
//!
//! ```text
//! cargo test --features test-fs --test fault_idem
//! ```

#![cfg(feature = "test-fs")]

use std::path::PathBuf;
use std::sync::Arc;

use serde_json::json;

use streams::clock::{SharedClock, TestClock};
use streams::config::ServerConfig;
use streams::engine::Engine;
use streams::storage::testfs::{FakeDisk, TornDamage};
use streams::types::{BoxConfig, BoxType, DiffRequest, RecordIn, WriteRequest};

// ===========================================================================
// Plumbing (mirrors tests/crash_oracle.rs)
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

/// Build / recover a durable engine whose WAL + snapshots live on `disk`.
fn open_engine(disk: &FakeDisk) -> Arc<Engine> {
    Engine::with_data_dir_fs(cfg(), clock(), disk.arc()).expect("engine opens through FakeDisk")
}

/// Make the WAL + meta directory entries durable (the create+dir-fsync that
/// production does at WAL open — modeled explicitly so files survive a crash).
fn sync_dirs(disk: &FakeDisk) {
    let fs = disk.arc();
    let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("wal"));
    let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("meta"));
}

/// Append one record to `name` with an optional idempotency key, on a durable
/// box (`durable=true` ⇒ the write blocks on the group fsync ⇒ acked ⇒ durable).
fn write_one(
    engine: &Engine,
    name: &str,
    data: &str,
    idempotency_key: Option<&str>,
) -> streams::types::WriteResponse {
    let req = WriteRequest {
        records: vec![RecordIn {
            data: json!({ "v": data }),
            tag: None,
            node: None,
            meta: None,
        }],
        node: None,
        idempotency_key: idempotency_key.map(|s| s.to_string()),
        create: Some(true),
        config: Some(BoxConfig {
            r#type: BoxType::Log,
            durable: true,
            ..Default::default()
        }),
        disable_backpressure: true,
    };
    engine.write(name, req, true).expect("durable write acks")
}

/// Read back every recovered seq → its `v` payload, through the public diff path.
fn read_records(engine: &Engine, name: &str) -> Vec<(u64, String)> {
    let mut out = Vec::new();
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
                },
            )
            .expect("diff ok");
        for r in &d.records {
            let v = r
                .data
                .get("v")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| r.data.to_string());
            out.push((r.seq, v));
        }
        if d.caught_up || d.records.is_empty() {
            break;
        }
        from = d.next_from_seq;
    }
    out
}

/// Assert the recovered seq set is a dense, gap-free ascending run (no hole, no
/// duplicate). Returns the seqs for further checks.
fn assert_no_gap(records: &[(u64, String)]) -> Vec<u64> {
    let seqs: Vec<u64> = records.iter().map(|(s, _)| *s).collect();
    for w in seqs.windows(2) {
        assert_eq!(
            w[1],
            w[0] + 1,
            "recovered seqs must be a dense gap-free run (got {seqs:?})"
        );
    }
    seqs
}

// ===========================================================================
// F-IDEMP-RETRY-AFTER-CRASH
//
// fault:  crash after a write acked; the dedupe map (not persisted) is lost;
//         client retries the same idempotency_key.
// oracle: retry re-executes (allowed, best-effort window) but appends a fresh
//         contiguous batch at head+1 — no gap, no duplicate seq for a live
//         record, no resurrection of deleted data.
// ===========================================================================
#[test]
fn f_idemp_retry_after_crash() {
    let disk = FakeDisk::new();

    // Phase 1: durable acked write with idempotency key "k1", plus a couple of
    // plain writes so head>1, then a power loss with everything fsynced.
    let (orig_seqs, head_before) = {
        let engine = open_engine(&disk);
        let r0 = write_one(&engine, "jobs", "a", None); // seq 1
        let dedup = write_one(&engine, "jobs", "b", Some("k1")); // seq 2 (keyed)
        let r2 = write_one(&engine, "jobs", "c", None); // seq 3
        assert!(!dedup.deduped, "first use of the key is not a dedupe hit");
        assert_eq!(r0.last_seq, 1);
        assert_eq!(dedup.seqs.as_deref(), Some(&[2u64][..]));
        assert_eq!(r2.last_seq, 3);

        sync_dirs(&disk);
        // Power loss with every write durable+acked (durable box ⇒ all fsynced).
        disk.crash(TornDamage::None);
        drop(engine);
        (dedup.seqs.clone().unwrap(), r2.head_seq)
    };
    assert_eq!(head_before, 3);
    disk.reset_power();

    // Phase 2: recover. The records survive (acked durable), but the dedupe map
    // did NOT — it is in-memory only and a fresh engine starts with none.
    let engine = open_engine(&disk);
    let before = read_records(&engine, "jobs");
    let seqs_before = assert_no_gap(&before);
    assert_eq!(
        seqs_before,
        vec![1, 2, 3],
        "all 3 acked durable writes survive the crash"
    );
    assert_eq!(before[1].1, "b", "the keyed record's payload is intact");

    // The retry of the SAME idempotency_key after recovery. Because the dedupe
    // map was lost, this re-executes (best-effort window) — it is NOT a dedupe
    // hit and assigns a FRESH seq at head+1.
    let retry = write_one(&engine, "jobs", "b", Some("k1"));
    assert!(
        !retry.deduped,
        "post-crash retry re-executes (dedupe window not persisted)"
    );

    // ---- THE CONTRACT (oracle_invariants[12]) ----
    // (a) The retry appended a fresh contiguous batch at exactly head+1.
    assert_eq!(
        retry.seqs.as_deref(),
        Some(&[4u64][..]),
        "retry is a fresh append at head+1, not a resurrected/duplicate seq"
    );
    // (b) NO DUPLICATE seq for a live record: the retry's seq differs from the
    //     original keyed seq; the original record is untouched.
    assert_ne!(
        retry.last_seq, orig_seqs[0],
        "retry must never reuse the original keyed write's live seq"
    );

    let after = read_records(&engine, "jobs");
    let seqs_after = assert_no_gap(&after);
    // (c) NO GAP and NO RESURRECTION: a dense run 1..=4; the original keyed
    //     record (seq 2) still present unchanged; the retry is the new seq 4.
    assert_eq!(seqs_after, vec![1, 2, 3, 4], "dense gap-free run after retry");
    assert_eq!(after[1], (2, "b".to_string()), "original keyed record intact");
    assert_eq!(after[3], (4, "b".to_string()), "retry materialized at head+1");

    // (d) Head monotone & advanced by exactly the one retried record.
    let st = engine.box_state("jobs", false).expect("state");
    assert_eq!(st.head_seq, 4, "head advanced to head_before+1");
    assert!(st.head_seq > head_before, "head never regresses across restart");

    drop(engine);

    // (e) Idempotent recovery: recovering again yields the same dense state and
    //     the dedupe map is STILL empty (a second retry would re-execute again).
    let engine2 = open_engine(&disk);
    let again = read_records(&engine2, "jobs");
    assert_eq!(assert_no_gap(&again), vec![1, 2, 3, 4], "recovery is idempotent");
    let b = engine2.get_box("jobs").expect("box present");
    assert!(
        b.dedupe.read().is_empty(),
        "dedupe map is never rebuilt from durable state on recovery"
    );
}

// ===========================================================================
// F-IDEMP-WINDOW-NOT-PERSISTED
//
// fault:  verify dedupe state is intentionally absent from the snapshot.
// inject: capture+restore via FakeDisk, inspect the restored dedupe map.
// oracle: restored dedupe is empty (documented best-effort); correctness never
//         depends on it; no invariant violated by its loss.
// ===========================================================================
#[test]
fn f_idemp_window_not_persisted() {
    let disk = FakeDisk::new();

    // Phase 1: durable keyed writes, then take a real snapshot (capture →
    // write_snapshot_with → atomic swap) so the dedupe map is live in memory at
    // the moment the snapshot body is materialized.
    {
        let engine = open_engine(&disk);
        let a = write_one(&engine, "q", "j1", Some("key-a")); // seq 1
        let bb = write_one(&engine, "q", "j2", Some("key-b")); // seq 2
        assert_eq!(a.last_seq, 1);
        assert_eq!(bb.last_seq, 2);

        // Sanity: the dedupe map IS populated in the live engine pre-snapshot —
        // so a non-empty restore would be a real persistence regression.
        let live = engine.get_box("q").expect("box present");
        assert_eq!(
            live.dedupe.read().len(),
            2,
            "both keys are remembered in the live in-memory dedupe window"
        );

        // A real snapshot lands on the same FakeDisk image (and is durable: the
        // write_snapshot_with path does write→fsync→rename→dir-fsync).
        assert!(engine.write_snapshot().expect("snapshot ok"), "snapshot written");

        sync_dirs(&disk);
        disk.crash(TornDamage::None);
        drop(engine);
    }
    disk.reset_power();

    // Phase 2: recover (from the snapshot + WAL) and inspect the restored dedupe.
    let engine = open_engine(&disk);

    // (1) CORRECTNESS DOES NOT DEPEND ON DEDUPE: the records themselves are fully
    //     recovered (the snapshot materialized them), dense and gap-free.
    let recs = read_records(&engine, "q");
    let seqs = assert_no_gap(&recs);
    assert_eq!(seqs, vec![1, 2], "both keyed records recovered from snapshot+WAL");
    assert_eq!(recs[0].1, "j1");
    assert_eq!(recs[1].1, "j2");

    // (2) THE INVARIANT UNDER TEST: the restored dedupe map is EMPTY — the
    //     snapshot intentionally carries no dedupe state (best-effort window).
    let b = engine.get_box("q").expect("box present after recovery");
    assert!(
        b.dedupe.read().is_empty(),
        "restored dedupe map MUST be empty (dedupe is never persisted to the snapshot)"
    );

    // (3) NO INVARIANT VIOLATED BY ITS LOSS: a write reusing a previously-keyed
    //     key re-executes as a fresh head+1 append (no dedupe hit), and the state
    //     stays dense — exactly the best-effort, never-unsafe behavior.
    let retry = write_one(&engine, "q", "j1-again", Some("key-a"));
    assert!(!retry.deduped, "lost dedupe window ⇒ retry re-executes");
    assert_eq!(retry.seqs.as_deref(), Some(&[3u64][..]), "fresh append at head+1");
    let after = read_records(&engine, "q");
    assert_eq!(assert_no_gap(&after), vec![1, 2, 3], "still dense, no gap/duplicate");
}
