//! Crash/race test for the WAL-first, point-in-time, append-ordered API delete
//! (RELIABILITY fix #1). Drives the *real, fully-wired* [`Engine`] through an
//! in-memory [`FakeDisk`], races a delete against a concurrent same-tag append,
//! crashes, recovers a fresh engine through the same image, and asserts the
//! point-in-time guarantee survived the crash:
//!
//!   a `match`-tag delete is bounded by the box head AT THE MOMENT IT WAS LOGGED,
//!   so a record that arrived AFTER the delete frame (carrying a higher seq) is
//!   NEVER swept on replay.
//!
//! This is the regression test for the old APPLY-FIRST delete bug: because the
//! WAL `Delete` frame stored only the SELECTOR (no bound), a concurrent append
//! that interleaved between the in-memory delete and the WAL frame could be
//! re-derived as a match and DELETED on replay — a silent loss of an acked,
//! never-deleted record. The fix logs the frame WAL-first under the append lock,
//! pinning `bound_head = head + 1`, and replay honors that bound.
//!
//! ```text
//! cargo test --features test-fs --test crash_delete_race -- --test-threads=1
//! ```

#![cfg(feature = "test-fs")]

use std::collections::BTreeMap;
use std::sync::Arc;

use serde_json::json;

use streams::clock::{SharedClock, TestClock};
use streams::config::ServerConfig;
use streams::engine::Engine;
use streams::storage::testfs::{FakeDisk, TornDamage};
use streams::storage::{BoxConfigOp, MatchSel, Wal, WalConfig, WalRecord};
use streams::types::{
    BoxConfig, BoxType, DeleteRequest, DiffRequest, Filter, RecordIn, WriteRequest,
};

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

/// A durable log box.
fn put_durable_log(engine: &Engine, name: &str) {
    engine
        .put_box(
            name,
            BoxConfig {
                r#type: BoxType::Log,
                durable: true,
                ..Default::default()
            },
        )
        .expect("create durable box");
}

/// Append one record `(data, tag)` to a durable box, returning the assigned seq.
fn append(engine: &Engine, name: &str, data: &str, tag: &str) -> u64 {
    let req = WriteRequest {
        records: vec![RecordIn {
            data: json!({ "v": data }),
            tag: Some(tag.to_string()),
            node: None,
            meta: None,
        }],
        node: None,
        idempotency_key: None,
        create: Some(true),
        config: None,
        disable_backpressure: true,
    };
    let resp = engine.write(name, req, true).expect("durable append acked");
    resp.last_seq
}

/// Make every WAL/snapshot file's NAME durable (model the dir fsync the
/// production WAL open does, so files survive a crash).
fn sync_dirs(disk: &FakeDisk) {
    let fs = disk.arc();
    let _ = fs.sync_dir(std::path::Path::new(DATA_DIR).join("wal").as_path());
    let _ = fs.sync_dir(std::path::Path::new(DATA_DIR).join("meta").as_path());
}

/// Read back the live records of `name` by seq through the public diff path.
fn live_records(engine: &Engine, name: &str) -> BTreeMap<u64, String> {
    let mut out = BTreeMap::new();
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
                    include_meta: false,
                    wait_ms: 0,
                    max_batch_bytes: 0,
                },
            )
            .expect("diff");
        for r in &d.records {
            let v = r
                .data
                .get("v")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            out.insert(r.seq, v);
        }
        if d.caught_up || d.records.is_empty() {
            break;
        }
        from = d.next_from_seq;
    }
    out
}

/// DELETE RACE (deterministic ordering): append tag=x (seq 1); delete tag=x
/// (logs the WAL Delete frame bounded at head+1 = 2); THEN append a NEW tag=x
/// record (seq 2, simulating arrival right after the delete was logged); crash +
/// recover. The second (newer) append MUST survive — the point-in-time delete did
/// not reach it — and the first MUST be gone.
#[test]
fn delete_race_newer_same_tag_append_survives_crash() {
    let disk = FakeDisk::new();
    {
        let engine = open_engine(&disk);
        put_durable_log(&engine, "msgs");

        let s1 = append(&engine, "msgs", "old", "x");
        assert_eq!(s1, 1);

        // Point-in-time delete of every current tag=x (just seq 1). WAL-first:
        // the Delete frame is fsynced (durable box) with bound_head = 2 BEFORE the
        // in-memory delete applies.
        let r = engine
            .delete(
                "msgs",
                DeleteRequest {
                    before_seq: None,
                    match_: Some(Filter::from_shorthand("x")),
                },
            )
            .expect("delete acked");
        assert_eq!(r.deleted, 1, "only the pre-delete tag=x (seq 1) is removed");

        // A NEW tag=x record arrives AFTER the delete frame was logged ⇒ seq 2,
        // which is >= bound_head (2), so replay must NOT sweep it.
        let s2 = append(&engine, "msgs", "new", "x");
        assert_eq!(s2, 2);

        sync_dirs(&disk);
        // Power loss: freeze + drop un-fsynced bytes. Both the delete frame and
        // both appends are durable (fsync'd) by the time we crash.
        disk.crash(TornDamage::None);
        drop(engine);
    }
    disk.reset_power();

    let engine = open_engine(&disk);
    let live = live_records(&engine, "msgs");
    assert_eq!(
        live.keys().copied().collect::<Vec<_>>(),
        vec![2],
        "the newer tag=x (seq 2) survives; the deleted seq 1 stays gone (no over-delete on replay)"
    );
    assert_eq!(live[&2], "new");
    // Head recovered to the newer append.
    let st = engine.box_state("msgs", false).unwrap();
    assert_eq!(st.head_seq, 2, "head recovered to the newer append");
}

/// DELETE RACE (concurrent threads): a writer thread hammers NEW tag=x appends
/// while the main thread issues a `match`-tag delete. Whatever the interleaving,
/// every append whose seq is >= the delete's logged point-in-time bound MUST
/// survive the crash; nothing acked is fabricated or lost as a hole. We assert
/// the strong invariant: after recovery the survivors are a dense suffix of the
/// acked appends with NO gap, and at least the appends acked AFTER the delete
/// returned are all present.
#[test]
fn delete_race_concurrent_appends_no_overdelete_on_crash() {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::thread;

    let disk = FakeDisk::new();
    // Track the highest seq acked AFTER the delete call returned: every such
    // record arrived strictly after the delete was logged, so none may be deleted.
    let post_delete_min: Arc<AtomicU64> = Arc::new(AtomicU64::new(u64::MAX));

    {
        let engine = open_engine(&disk);
        put_durable_log(&engine, "race");

        // Seed a pre-delete tag=x record (must be removed by the delete).
        let s0 = append(&engine, "race", "pre", "x");
        assert_eq!(s0, 1);

        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let writer = {
            let engine = engine.clone();
            let stop = stop.clone();
            thread::spawn(move || {
                let mut acked: Vec<u64> = Vec::new();
                let mut i = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let seq = append(&engine, "race", &format!("c{i}"), "x");
                    acked.push(seq);
                    i += 1;
                    if i > 200 {
                        break; // bound the burst
                    }
                }
                acked
            })
        };

        // Let a few concurrent appends land, then delete tag=x point-in-time.
        thread::sleep(std::time::Duration::from_millis(2));
        let _ = engine
            .delete(
                "race",
                DeleteRequest {
                    before_seq: None,
                    match_: Some(Filter::from_shorthand("x")),
                },
            )
            .expect("delete acked");
        // The delete has returned. The box head right now is the point-in-time
        // upper bound on what the delete could have swept; every append acked from
        // here on carries a strictly higher seq.
        let head_after_delete = engine.box_state("race", false).unwrap().head_seq;
        post_delete_min.store(head_after_delete + 1, Ordering::SeqCst);

        // Append a few MORE NEW tag=x records that are guaranteed post-delete.
        let mut guaranteed_survivors: Vec<u64> = Vec::new();
        for i in 0..5 {
            let seq = append(&engine, "race", &format!("post{i}"), "x");
            guaranteed_survivors.push(seq);
        }

        stop.store(true, Ordering::Relaxed);
        let _ = writer.join().unwrap();

        sync_dirs(&disk);
        disk.crash(TornDamage::None);
        drop(engine);

        // Stash the guaranteed-survivor seqs for the post-recovery assertion via
        // the atomic min (already the floor); also remember the explicit list.
        // (We re-derive them below from post_delete_min.)
        let _ = guaranteed_survivors;
    }
    disk.reset_power();

    let engine = open_engine(&disk);
    let live = live_records(&engine, "race");
    // `post_delete_min` is an UPPER bound on the delete's real point-in-time
    // bound: the engine pins that bound to `head + 1` at the instant it grabbed
    // the append lock, which (under a concurrent writer) may be lower than the
    // head we observed after the delete returned. So the guaranteed-survivor
    // appends (acked strictly after `delete` returned) are exactly the seqs
    // `>= post_delete_min`; every one of them MUST be present (none over-deleted).
    let floor = post_delete_min.load(std::sync::atomic::Ordering::SeqCst);
    let survivors: Vec<u64> = live.keys().copied().collect();

    // (a) The pre-delete tag=x (seq 1) must be GONE — the delete took effect.
    assert!(
        !live.contains_key(&1),
        "pre-delete tag=x (seq 1) must stay deleted, survivors={survivors:?}"
    );

    // (b) NO OVER-DELETE: every append acked AFTER the delete returned (seq >=
    //     floor, and <= the recovered head) is present. These records provably
    //     arrived after the delete frame was logged, so the point-in-time bound
    //     must never have reached them — losing one would be the over-delete bug.
    let head = engine.box_state("race", false).unwrap().head_seq;
    for seq in floor..=head {
        assert!(
            live.contains_key(&seq),
            "post-point-in-time append seq {seq} over-deleted on replay \
             (floor={floor}, head={head}, survivors={survivors:?})"
        );
    }

    // (c) NO HOLE among survivors: a crash drops a tail, never punches an
    //     interior hole into the live set (and never resurrects a deleted middle).
    if let (Some(&lo), Some(&hi)) = (survivors.first(), survivors.last()) {
        let expected: Vec<u64> = (lo..=hi).collect();
        assert_eq!(
            survivors, expected,
            "survivors must be a dense contiguous run (no hole / no torn record): {survivors:?}"
        );
    }
}

/// Directly construct the ADVERSARIAL WAL layout that the old APPLY-FIRST delete
/// bug produced and prove the logged `bound_head` defeats it on replay. The
/// layout: `Append(seq 1, tag=x)`, `Append(seq 2, tag=x)`, then `Delete(match=x,
/// bound_head=2)` — i.e. a newer same-tag append's frame is ordered BEFORE the
/// delete frame (exactly what a concurrent append landing between the in-memory
/// delete and the WAL Delete frame would have written). If replay re-derived the
/// match against the recovered head (the pre-fix behavior) it would compute
/// bound = 3 and delete BOTH seqs; honoring the logged `bound_head = 2` deletes
/// only seq 1, so the newer seq 2 SURVIVES.
#[test]
fn replay_honors_logged_delete_bound_against_later_append_frame() {
    let disk = FakeDisk::new();
    let data_dir = std::path::PathBuf::from(DATA_DIR);
    let box_id = 1u32;
    let cfg_blob = serde_json::to_vec(&BoxConfig {
        r#type: BoxType::Log,
        durable: true,
        ..Default::default()
    })
    .unwrap();

    // Write the frames in the adversarial order through the real WAL writer.
    {
        let wal = Wal::open_at_with(disk.arc(), WalConfig::new(&data_dir), 1, 0)
            .expect("open wal through fakedisk");
        let w = wal.writer();
        // BoxConfig create so recovery materializes box_id=1 as a durable log.
        w.append(
            WalRecord::BoxConfig {
                box_id,
                op: BoxConfigOp {
                    name: "msgs".into(),
                    config: cfg_blob,
                },
                tombstone: false,
                ts: 1,
            },
            true,
        )
        .unwrap();
        // The WAL `Append.data` is the opaque payload blob `{"d": <data>}` (data +
        // optional meta), exactly what `encode_record_payload` writes; recovery's
        // `decode_record_payload` reads `d` back out.
        let ap = |seq: u64| WalRecord::Append {
            box_id,
            seq,
            ts: 10 + seq,
            node: None,
            tag: Some("x".into()),
            data: serde_json::to_vec(&json!({ "d": { "v": format!("r{seq}") } })).unwrap(),
        };
        w.append(ap(1), true).unwrap();
        // The NEWER same-tag append's frame lands BEFORE the delete frame.
        w.append(ap(2), true).unwrap();
        // Point-in-time delete: bounded at head+1 == 2 as captured WHEN it was
        // logged (so it only ever sweeps seq < 2).
        w.append(
            WalRecord::Delete {
                box_id,
                before_seq: None,
                match_: Some(MatchSel::Eq("x".into())),
                seqs: Vec::new(),
                bound_head: Some(2),
                ts: 30,
            },
            true,
        )
        .unwrap();
        // Make the WAL file's name durable, then power-loss + drop.
        let _ = disk.arc().sync_dir(data_dir.join("wal").as_path());
        disk.crash(TornDamage::None);
        drop(wal);
    }
    disk.reset_power();

    let engine = open_engine(&disk);
    let live = live_records(&engine, "msgs");
    assert_eq!(
        live.keys().copied().collect::<Vec<_>>(),
        vec![2],
        "replay honored bound_head=2: seq 1 deleted, the newer seq 2 (>= bound) survives \
         (a re-derive against the recovered head would have wrongly deleted seq 2 too)"
    );
    assert_eq!(live[&2], "r2");
}
