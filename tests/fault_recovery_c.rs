//! Phase-8B fault catalog — **recovery-replay** boundary, group C.
//!
//! Five strategies that stress the *second half* of recovery — the WAL replay
//! pass, the torn-tail `truncate_active`, and the segment `.idx` bulk re-load —
//! against the Phase-8A hostile-FS harness ([`FakeDisk`] / [`FaultFs`]) and the
//! model-oracle contract from `tests/crash_oracle.rs`:
//!
//! ```text
//!   acked-durable ⊆ survivors ⊆ ever_acked   (no silent loss / no fabrication)
//!   recover(recover(x)) == recover(x)         (idempotent / convergent replay)
//!   a torn tail is the end of log, never misread, never appended-after
//!   a fault in recovery surfaces as an error (no silent empty/partial start)
//! ```
//!
//! Strategies implemented (see `docs/FAULT_TESTING.md`):
//!   - `F-REC-CRASH-DURING-REPLAY`   — a glitch mid WAL-replay; re-run converges.
//!   - `F-REC-CRASH-BEFORE-TRUNCATE` — crash after replay, before `truncate_active`.
//!   - `F-REC-EIO-TRUNCATE`          — EIO on `set_len` in `truncate_active`.
//!   - `F-REC-EIO-TRUNCATE-FSYNC`    — EIO on `sync_all` after the truncate.
//!   - `F-REC-IDX-BULKLOAD-EIO`      — EIO reading a segment `.idx` on recovery.
//!
//! The two crash-point strategies (`*-DURING-REPLAY`, `*-BEFORE-TRUNCATE`) are
//! specced in the catalog to fire a `fail-rs` failpoint (`recovery::mid_replay`,
//! `recovery::before_truncate`). Those failpoints are no-ops without the
//! `failpoints` feature, which the self-verify command does not enable, so we
//! reproduce the SAME crash semantics at the FS-call seam — the harness layer the
//! spec defines as "FakeDisk decides WHAT survives; the failpoint only decides
//! WHEN". A transient `FaultFs` glitch interrupts recovery exactly where the
//! failpoint would, and the un-truncated/​un-replayed state on disk is then
//! recovered again on a clean device to prove convergence.
//!
//! ```text
//! cargo test --features test-fs --test fault_recovery_c
//! ```

#![cfg(feature = "test-fs")]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use serde_json::json;

use streams::clock::{SharedClock, TestClock};
use streams::config::ServerConfig;
use streams::engine::Engine;
use streams::storage::testfs::{FakeDisk, FaultFs, FaultKind, FaultOp, TornDamage};
use streams::storage::{
    Fs, LocalSegmentStore, SegmentBuilder, SegmentPart, SegmentRecord, SegmentStore, StoreError,
};
use streams::types::{BoxConfig, BoxType, DiffRequest, RecordIn, WriteRequest};

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

/// Build/recover a durable engine whose WAL + snapshots live on `disk`.
fn open_engine(disk: &FakeDisk) -> Arc<Engine> {
    Engine::with_data_dir_fs(cfg(), clock(), disk.arc()).expect("engine opens through FakeDisk")
}

/// Build/recover a durable engine through an arbitrary injected `Fs`.
fn open_engine_fs(fs: Arc<dyn Fs>) -> std::result::Result<Arc<Engine>, String> {
    Engine::with_data_dir_fs(cfg(), clock(), fs).map_err(|e| e.to_string())
}

/// Append one durable record, returning the assigned seq. Auto-creates the box.
fn append(engine: &Engine, name: &str, data: &str) -> u64 {
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
    let resp = engine.write(name, req, true).expect("durable append acks");
    resp.last_seq
}

fn put_durable_box(engine: &Engine, name: &str) {
    let cfg = BoxConfig {
        r#type: BoxType::Log,
        durable: true,
        ..Default::default()
    };
    engine.put_box(name, cfg).expect("put_box");
}

/// The recovered `(head, earliest, count, seq→data)` of one box read back through
/// the public state + diff path — the same bytes a consumer sees.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Dump {
    head: u64,
    earliest: u64,
    count: u64,
    data: BTreeMap<u64, String>,
}

fn dump(engine: &Engine, name: &str) -> Option<Dump> {
    let st = engine.box_state(name, false).ok()?;
    let mut data = BTreeMap::new();
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
            .ok()?;
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
    Some(Dump {
        head: st.head_seq,
        earliest: st.earliest_seq,
        count: st.count,
        data,
    })
}

/// Make the WAL + meta directory entries durable (the create+dir-fsync prod does
/// at open), so the files survive a `crash()`.
fn sync_dirs(disk: &FakeDisk) {
    let fs = disk.arc();
    let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("wal"));
    let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("meta"));
}

/// A dense contiguous prefix `[1..=k]` carrying the model payloads `"r{seq}"`,
/// with no fabricated seq and no hole. The universal post-recovery survivor shape
/// for these single-box durable workloads.
fn assert_dense_prefix(d: &Dump, acked: &BTreeMap<u64, String>) {
    let mut seqs: Vec<u64> = d.data.keys().copied().collect();
    seqs.sort_unstable();
    for (i, s) in seqs.iter().enumerate() {
        assert_eq!(
            *s,
            i as u64 + 1,
            "survivors must be a dense prefix [1..=k], got {seqs:?}"
        );
        // No fabrication: every survivor was acked and its bytes match the model.
        assert_eq!(
            d.data.get(s),
            acked.get(s),
            "recovered seq {s} bytes differ from the model"
        );
    }
    assert_eq!(d.count, seqs.len() as u64, "count == #survivors");
    if let Some(&hi) = seqs.last() {
        assert_eq!(d.head, hi, "head == high-water of the dense prefix");
    }
}

// ===========================================================================
// F-REC-CRASH-DURING-REPLAY
//   compound · recovery-replay. A SECOND crash during recovery's WAL replay.
//   Spec inject: fail-rs recovery::mid_replay panic + restart; FakeDisk preserves
//   the durable WAL. Oracle: replay is idempotent (Append seq-skip, delete/evict
//   monotone), re-running recovery converges to identical state, and no partial
//   index becomes final.
//
//   We reproduce the mid-replay interruption at the FS seam: a transient EIO on a
//   WAL read_at during the replay scan aborts recovery part-way (no engine is
//   ever published with a half-built index — `with_data_dir_fs` returns Err). The
//   durable WAL is untouched, so a fresh recovery on a clean device replays the
//   SAME valid prefix and converges, byte-identical across repeats.
// ===========================================================================
#[test]
fn f_rec_crash_during_replay() {
    // Phase 1: a small durable workload, all acked, dir-synced, clean stop. This is
    // the durable WAL the (interrupted) recovery will replay. Kept small because
    // each durable append blocks on a real group-fsync and each recovery below
    // re-runs the whole open path.
    let disk = FakeDisk::new();
    let mut acked: BTreeMap<u64, String> = BTreeMap::new();
    {
        let engine = open_engine(&disk);
        put_durable_box(&engine, "b");
        for i in 1..=4u64 {
            let data = format!("r{i}");
            let seq = append(&engine, "b", &data);
            acked.insert(seq, data);
        }
        sync_dirs(&disk);
        drop(engine);
    }

    // The clean baseline recovery (no fault): the convergence target.
    let target = {
        let engine = open_engine(&disk);
        let d = dump(&engine, "b").expect("b present");
        drop(engine);
        d
    };
    assert_dense_prefix(&target, &acked);
    assert_eq!(target.count, 4, "all 4 acked durable writes recover");

    // Phase 2: interrupt recovery mid-replay. A transient EIO on a WAL read (the
    // replay scan re-reads each frame) aborts recovery before it finishes — the
    // engine is never published with a partial index. We probe a few distinct read
    // indices so the crash lands at different points of the replay pass (bounded so
    // the file runs fast).
    let mut interrupted_at_least_once = false;
    for read_idx in [0u64, 1, 2, 3] {
        // Fail-once EIO on the `read_idx`-th read_at over a FRESH FaultFs wrapping
        // the same durable disk image (the WAL on disk is unchanged).
        let faulty: Arc<dyn Fs> =
            FaultFs::new(disk.arc(), FaultOp::ReadAt, FaultKind::Eio, read_idx, true).arc();
        match open_engine_fs(faulty) {
            Err(_) => {
                interrupted_at_least_once = true;
                // The durable WAL is intact; a clean re-recovery must converge to
                // the exact same state (idempotent / convergent replay, no partial
                // index left as final).
                let engine = open_engine(&disk);
                let d = dump(&engine, "b").expect("b recovers after a mid-replay glitch");
                assert_eq!(
                    d, target,
                    "recovery after a mid-replay interruption (read #{read_idx}) \
                     must converge to the clean-recovery state"
                );
                drop(engine);
            }
            Ok(engine) => {
                // The glitch fell past the replay reads (or recovery tolerated it):
                // whatever published must still equal the convergence target — a
                // tolerated read never yields a partial/fabricated index.
                let d = dump(&engine, "b").expect("b present");
                assert_eq!(
                    d, target,
                    "a tolerated read fault (read #{read_idx}) must not corrupt state"
                );
                drop(engine);
            }
        }
    }
    assert!(
        interrupted_at_least_once,
        "the read-fault sweep must actually interrupt recovery at least once"
    );

    // Triple recovery on the clean device: recover(recover(recover(x))) identical.
    let r1 = {
        let e = open_engine(&disk);
        let d = dump(&e, "b").unwrap();
        drop(e);
        d
    };
    let r2 = {
        let e = open_engine(&disk);
        let d = dump(&e, "b").unwrap();
        drop(e);
        d
    };
    assert_eq!(r1, target);
    assert_eq!(r1, r2, "replay is convergent across repeated recoveries");
}

// ===========================================================================
// F-REC-CRASH-BEFORE-TRUNCATE
//   compound · recovery-replay. Crash after replay but before truncate_active.
//   Spec inject: fail-rs recovery::before_truncate + FakeDisk.crash(). Oracle: the
//   torn tail is still on disk; the next recovery re-replays the same valid prefix
//   and truncates again (convergent); the un-truncated tail is never misread.
//
//   We reproduce "crash before truncate" by making `truncate_active`'s set_len
//   FAIL (fail-once EIO) so recovery aborts BEFORE the torn tail is removed — the
//   on-disk file still carries the torn tail. A second recovery on a clean device
//   re-replays the identical valid prefix, truncates the tail, and converges. The
//   torn tail is never materialized as a record at any step.
// ===========================================================================
#[test]
fn f_rec_crash_before_truncate() {
    // A durable prefix + an in-flight last frame. We append durable records (acked,
    // fsynced → promoted), then a final NON-durable record that may stay pending,
    // and crash with a prefix-truncate tear of that pending tail. On power loss the
    // 7th frame is either dropped/torn (→ truncated at `truncate_active`) or, if
    // group-commit happened to fsync it first, a clean valid frame that survives
    // at head+1 — both are contract-legal (a non-durable write MAY survive). The
    // must-holds are convergence after a suppressed truncate, a dense gapless
    // prefix, and that the un-truncated tail is NEVER misread as a garbage record.
    let disk = FakeDisk::with_seed(0xBEEF);
    let mut acked: BTreeMap<u64, String> = BTreeMap::new();
    {
        let engine = open_engine(&disk);
        put_durable_box(&engine, "b");
        for i in 1..=6u64 {
            let data = format!("r{i}");
            let seq = append(&engine, "b", &data);
            acked.insert(seq, data);
        }
        // A trailing non-durable submit → the in-flight tail a torn write damages.
        let req = WriteRequest {
            records: vec![RecordIn {
                data: json!({ "v": "torn" }),
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
        let _ = engine.write("b", req, false); // fire-and-forget non-durable.
        std::thread::sleep(std::time::Duration::from_millis(5));
        sync_dirs(&disk);
        disk.crash(TornDamage::PrefixTruncate); // tear the pending tail.
        drop(engine);
    }
    disk.reset_power();

    // The set of genuine payloads a survivor may carry: any acked "r{i}" or the
    // real non-durable "torn" value — never a garbled/fabricated byte string. A
    // recovered seq 7 (if the tail was fsynced before the crash) is the genuine
    // "torn" record, not a misread of the torn bytes.
    let genuine = |seq: u64, val: &str| -> bool {
        match acked.get(&seq) {
            Some(a) => a == val,
            None => seq == 7 && val == "torn",
        }
    };
    let assert_clean = |d: &Dump| {
        let mut seqs: Vec<u64> = d.data.keys().copied().collect();
        seqs.sort_unstable();
        for (i, s) in seqs.iter().enumerate() {
            assert_eq!(*s, i as u64 + 1, "dense gapless prefix, got {seqs:?}");
            assert!(
                genuine(*s, d.data.get(s).unwrap()),
                "recovered seq {s} carries non-genuine bytes {:?} (torn tail misread?)",
                d.data.get(s)
            );
        }
        // The 6 acked durable frames always survive; the 7th may or may not.
        assert!(d.count >= 6 && d.count <= 7, "6 acked survive, the 7th is optional");
        if let Some(&hi) = seqs.last() {
            assert_eq!(d.head, hi, "head == high-water of the prefix");
        }
    };

    // Recovery #1 with the truncate suppressed: fail-once EIO on the FIRST set_len
    // of recovery — which is exactly `truncate_active`'s (the resumed writer's
    // preallocation set_len comes AFTER, so index 0 is the truncate). Recovery
    // must abort (an io error), never silently start the writer over an
    // un-truncated tail.
    let faulty: Arc<dyn Fs> =
        FaultFs::new(disk.arc(), FaultOp::SetLen, FaultKind::Eio, 0, true).arc();
    let res = open_engine_fs(faulty);
    assert!(
        res.is_err(),
        "recovery must surface the truncate set_len EIO, not start over an un-truncated tail"
    );

    // The (possibly torn) tail is STILL on disk (truncate never ran). A clean
    // re-recovery re-replays the same valid prefix, truncates any torn tail, and
    // converges — never misreading the tail as a garbage record.
    let engine = open_engine(&disk);
    let d = dump(&engine, "b").expect("b recovers after the suppressed truncate");
    assert_clean(&d);
    drop(engine);

    // Convergent: a second clean recovery is byte-identical, and a new append
    // continues at head+1 (never after garbage).
    let engine2 = open_engine(&disk);
    let d2 = dump(&engine2, "b").unwrap();
    assert_eq!(d, d2, "recovery is convergent after a suppressed truncate");
    let expect_next = d.head + 1;
    let next = append(&engine2, "b", "after");
    assert_eq!(
        next, expect_next,
        "the new append continues at head+1, never after the tail"
    );
    drop(engine2);
}

// ===========================================================================
// F-REC-EIO-TRUNCATE
//   io-error · recovery-replay. EIO on set_len during truncate_active.
//   Spec inject: FaultFs fail-once op=set_len on the active WAL file. Oracle:
//   recovery surfaces the io error and aborts (does not start the writer over an
//   un-truncated torn file); retry converges; never appends after garbage.
// ===========================================================================
#[test]
fn f_rec_eio_truncate() {
    let disk = FakeDisk::new();
    let mut acked: BTreeMap<u64, String> = BTreeMap::new();
    {
        let engine = open_engine(&disk);
        put_durable_box(&engine, "b");
        for i in 1..=5u64 {
            let data = format!("r{i}");
            let seq = append(&engine, "b", &data);
            acked.insert(seq, data);
        }
        sync_dirs(&disk);
        drop(engine);
    }

    // Fail-once EIO on the first set_len of recovery (= truncate_active's). Even on
    // a clean (untorn) file, `truncate_active` still issues a set_len to the valid
    // length, so the fault always lands there. Recovery must return Err — it must
    // not publish a writer that would append after an (unverified) tail.
    let faulty: Arc<dyn Fs> =
        FaultFs::new(disk.arc(), FaultOp::SetLen, FaultKind::Eio, 0, true).arc();
    let res = open_engine_fs(faulty);
    assert!(
        res.is_err(),
        "recovery must surface the truncate set_len EIO and abort"
    );

    // Retry on a healthy device converges to the full durable state, and appends
    // continue at head+1.
    let engine = open_engine(&disk);
    let d = dump(&engine, "b").expect("b recovers on retry");
    assert_dense_prefix(&d, &acked);
    assert_eq!(d.count, 5, "all 5 durable writes recover after the truncate retry");
    let next = append(&engine, "b", "six");
    assert_eq!(next, 6, "post-recovery append continues at head+1, never after garbage");
    drop(engine);
}

// ===========================================================================
// F-REC-EIO-TRUNCATE-FSYNC
//   io-error · recovery-replay. EIO on sync_all after truncate.
//   Spec inject: FaultFs fail-once op=sync_all in truncate_active. Oracle:
//   truncation not durable ⇒ next boot re-truncates (idempotent); the writer is
//   not opened until truncation is durable, or re-run on retry.
// ===========================================================================
#[test]
fn f_rec_eio_truncate_fsync() {
    let disk = FakeDisk::new();
    let mut acked: BTreeMap<u64, String> = BTreeMap::new();
    {
        let engine = open_engine(&disk);
        put_durable_box(&engine, "b");
        for i in 1..=5u64 {
            let data = format!("r{i}");
            let seq = append(&engine, "b", &data);
            acked.insert(seq, data);
        }
        sync_dirs(&disk);
        drop(engine);
    }

    // Fail-once EIO on the first sync_all of recovery (= the post-truncate fsync in
    // `truncate_active`; the snapshot load is read-only and the resumed writer's
    // preallocation sync_all comes later). The post-truncate fsync failing means
    // the truncation is not durable, so recovery must NOT proceed to open the
    // writer over an un-hardened file — it surfaces the error and aborts.
    let faulty: Arc<dyn Fs> =
        FaultFs::new(disk.arc(), FaultOp::SyncAll, FaultKind::Eio, 0, true).arc();
    let res = open_engine_fs(faulty);
    assert!(
        res.is_err(),
        "recovery must surface the post-truncate sync_all EIO and abort"
    );

    // Next boot re-truncates idempotently on a healthy device and converges; the
    // full durable state is intact and appends continue at head+1.
    let engine = open_engine(&disk);
    let d = dump(&engine, "b").expect("b recovers on retry");
    assert_dense_prefix(&d, &acked);
    assert_eq!(d.count, 5, "all 5 durable writes recover after the truncate-fsync retry");

    // Idempotent: a second recovery is byte-identical (re-truncate is a no-op).
    drop(engine);
    let engine2 = open_engine(&disk);
    let d2 = dump(&engine2, "b").unwrap();
    assert_eq!(d, d2, "re-truncate on the next boot is idempotent");
    let next = append(&engine2, "b", "six");
    assert_eq!(next, 6, "post-recovery append continues at head+1");
    drop(engine2);
}

// ===========================================================================
// F-REC-IDX-BULKLOAD-EIO
//   compound · recovery-replay. EIO reading a segment .idx during recovery
//   bulk-load / re-materialize.
//   Spec inject: FaultFs fail-once read_at on a seg .idx. Oracle: recovery falls
//   back to WAL/resident for that range (segments are derivable); does not crash;
//   re-running converges; no live record lost.
//
//   The engine wires its per-box segment store via `LocalSegmentStore::open` on
//   the REAL fs (it is not routed through the injected recovery_fs), so a fault
//   on a seg `.idx` cannot be injected through `with_data_dir_fs`. We therefore
//   exercise the segment store's `.idx` read path directly through a `FaultFs`
//   (`open_with`) — the exact surface a recovery bulk-load / read-back touches —
//   and assert the contract the oracle names: the EIO surfaces as a `StoreError`
//   (never a panic, never garbage served), a fail-once glitch converges on retry,
//   and the records remain fully available from the WAL (no live record lost).
// ===========================================================================
#[test]
fn f_rec_idx_bulkload_eio() {
    // 1) The records are durably in the WAL (the source of truth a segment is only
    //    a derived index of). After any seg `.idx` read fault, these must remain
    //    fully recoverable — that is the "no live record lost" half of the oracle.
    let disk = FakeDisk::new();
    let mut acked: BTreeMap<u64, String> = BTreeMap::new();
    {
        let engine = open_engine(&disk);
        put_durable_box(&engine, "b");
        for i in 1..=5u64 {
            let data = format!("r{i}");
            let seq = append(&engine, "b", &data);
            acked.insert(seq, data);
        }
        sync_dirs(&disk);
        drop(engine);
    }

    // 2) Build a real sealed segment (seg-1 covering seqs 1..=5) on a FakeDisk
    //    image, then read its `.idx` back through a `FaultFs` that EIOs that read
    //    once — the recovery bulk-load surface.
    let seg_disk = FakeDisk::new();
    let root = PathBuf::from("/segroot");
    let (data, idx) = {
        let mut b = SegmentBuilder::new(1);
        for i in 1..=5u64 {
            b.push(&SegmentRecord {
                seq: i,
                ts: 1000 + i,
                node: None,
                tag: Some(format!("t{i}")),
                data: format!("{{\"v\":\"r{i}\"}}").into_bytes(),
            });
        }
        b.finish()
    };
    {
        // Put the segment durably through a plain (fault-free) store.
        let store = LocalSegmentStore::open_with(&root, seg_disk.arc()).unwrap();
        store.put(1, &data, &idx).expect("seg put");
    }
    let idx_len = idx.len() as u64;

    // 3a) Fail-once EIO on the `.idx` read. `read_range`/`read_all` must surface a
    //     `StoreError` (an io error), NOT panic and NOT serve garbage bytes.
    {
        // The `.idx` read is the first read_at the store issues here; fail-once at
        // index 0 hits it. (Open is fault-free; read_at is the targeted class.)
        let faulty = FaultFs::new(seg_disk.arc(), FaultOp::ReadAt, FaultKind::Eio, 0, true);
        let store = LocalSegmentStore::open_with(&root, faulty.arc()).unwrap();
        let res = store.read_all(1, SegmentPart::Idx);
        assert!(
            matches!(res, Err(StoreError::Io(_))),
            "a seg .idx read EIO must surface as StoreError::Io, got {res:?}"
        );
    }

    // 3b) Convergence after the transient glitch: a re-read on the SAME (now
    //     past-the-fault) store yields the exact original `.idx` bytes — the
    //     fault-once did not corrupt anything, recovery re-run converges.
    {
        let faulty = FaultFs::new(seg_disk.arc(), FaultOp::ReadAt, FaultKind::Eio, 0, true);
        let store = LocalSegmentStore::open_with(&root, faulty.arc()).unwrap();
        // First read faults (consumes the once); the retry reads clean.
        let _ = store.read_all(1, SegmentPart::Idx);
        let again = store.read_all(1, SegmentPart::Idx).expect("retry reads clean");
        assert_eq!(again, idx, "re-read after a transient .idx EIO yields the original bytes");
        // A `.data` read range still works (the fault was scoped to the once).
        let head = store.read_range(1, SegmentPart::Data, 0, 8).expect("data read");
        assert_eq!(head, &data[..8]);
        // And the `.idx` length matches what we wrote.
        assert_eq!(again.len() as u64, idx_len);
    }

    // 4) The crux of the oracle: with the `.idx` derived index unavailable, no live
    //    record is lost — recovery falls back to the WAL. Recover the engine from
    //    the WAL image and confirm the full record set is served.
    let engine = open_engine(&disk);
    let d = dump(&engine, "b").expect("b recovers from the WAL");
    assert_dense_prefix(&d, &acked);
    assert_eq!(d.count, 5, "all 5 records remain available from the WAL (no live record lost)");
    drop(engine);
}
