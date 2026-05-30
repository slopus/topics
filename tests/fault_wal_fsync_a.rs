//! Phase-8B fault catalog — **boundary: wal-fsync** (group A).
//!
//! Implements 5 fault/crash strategies from `docs/FAULT_TESTING.md` /
//! `/tmp/streams-fault-catalog.json`, each driving the *real* durability layer
//! (the WAL writer with its single-fsync-per-group-commit barrier, or the fully
//! wired [`Engine`]) through the Phase-8A hostile-FS harness ([`FakeDisk`] +
//! [`FaultFs`]) and asserting the crash-consistency contract:
//!
//! ```text
//!   acked-durable ⊆ survivors ⊆ ever-acked    (no silent loss / no fabrication)
//!   seq monotonic + contiguous (a dense prefix)
//!   an fsync that errored never acks its batch
//!   a torn / un-fsynced tail truncates, never misread
//! ```
//!
//! | id | what it pins |
//! |----|--------------|
//! | `F-WAL-EIO-FSYNC`                 | EIO on the group-commit `sync_data` fails the WHOLE batch (no token acked); recovery has no batch frame. |
//! | `F-WAL-FSYNC-EIO-PERSIST`        | fsyncgate: an `sync_data` that errored once poisons the in-flight batch even though a later fsync succeeds. |
//! | `F-WAL-CRASH-AFTER-WRITE-PRE-FSYNC` | power loss after `write_at` but before `sync_data`: the un-promoted bytes vanish; prior fsynced batches survive. |
//! | `F-WAL-CRASH-AFTER-FSYNC`        | power loss right after `sync_data` returns: the just-promoted (acked) batch survives intact, seq monotonic. |
//! | `F-WAL-PARTIAL-BATCH-FSYNC`      | a multi-frame batch half-flushed (prefix durable, last frame torn): durable prefix replays, torn frame truncates, no acked record lost. |
//!
//! Bounded by construction (≤8-frame workloads, a handful of fixed seeds) so the
//! whole file runs in well under a second.
//!
//! ```text
//! cargo test --features test-fs --test fault_wal_fsync_a
//! ```

#![cfg(feature = "test-fs")]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;

use streams::clock::{SharedClock, TestClock};
use streams::config::ServerConfig;
use streams::engine::Engine;
use streams::storage::testfs::{FakeDisk, FaultFs, FaultKind, FaultOp, TornDamage};
use streams::storage::wal::{Wal, WalConfig, WalReader, WalRecord};
use streams::storage::Fs;
use streams::types::{BoxConfig, BoxType, RecordIn, WriteRequest};

// ===========================================================================
// Shared plumbing (mirrors tests/crash_oracle.rs — reused, not reinvented).
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

/// Recover a fresh engine whose WAL + snapshots live on `disk`.
fn open_engine(disk: &FakeDisk) -> Arc<Engine> {
    Engine::with_data_dir_fs(cfg(), clock(), disk.arc()).expect("engine opens through FakeDisk")
}

/// A durable write of one record through the public engine API. Returns the
/// assigned seqs on ack, or `Err` if the durable commit (fsync) failed.
fn durable_write(engine: &Engine, name: &str, data: &str) -> Result<Vec<u64>, ()> {
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
    match engine.write(name, req, true) {
        Ok(resp) => Ok(resp.seqs.unwrap_or_else(|| vec![resp.last_seq])),
        Err(_) => Err(()),
    }
}

/// Create a durable Log box.
fn put_durable_box(engine: &Engine, name: &str) {
    let c = BoxConfig {
        r#type: BoxType::Log,
        durable: true,
        cap_records: 0,
        ..Default::default()
    };
    engine.put_box(name, c).expect("put_box");
}

/// The live seqs present after recovery, read back through the real diff path
/// (the same bytes a consumer sees) — plus the recovered head.
fn recovered_seqs(engine: &Engine, name: &str) -> (Vec<u64>, u64) {
    let st = match engine.box_state(name, false) {
        Ok(st) => st,
        Err(_) => return (Vec::new(), 0),
    };
    let mut seqs = Vec::new();
    let mut from = 0u64;
    loop {
        let d = engine
            .diff(
                name,
                streams::types::DiffRequest {
                    from_seq: from,
                    limit: 1000,
                    node: None,
                    include_tags: true,
                    include_meta: true,
                    wait_ms: 0,
                },
            )
            .expect("diff");
        for r in &d.records {
            seqs.push(r.seq);
        }
        if d.caught_up || d.records.is_empty() {
            break;
        }
        from = d.next_from_seq;
    }
    (seqs, st.head_seq)
}

/// Make the WAL + meta directory entries durable (the create+dir-fsync
/// production does at WAL open; modelled explicitly so the file name survives a
/// crash).
fn sync_wal_dir(disk: &FakeDisk) {
    let fs = disk.arc();
    let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("wal"));
    let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("meta"));
}

/// Assert a recovered seq set is a dense contiguous prefix `[1..=k]` (no gap, no
/// fabricated/torn frame misread as a record).
fn assert_dense_prefix(seqs: &[u64]) {
    for (i, s) in seqs.iter().enumerate() {
        assert_eq!(
            *s,
            i as u64 + 1,
            "survivors must be a dense contiguous prefix, got {seqs:?}"
        );
    }
}

// --- Direct WAL-writer helpers (mirror the crash_oracle module tests) -------

/// A fast group-commit config so a lone durable append fsyncs ~immediately.
fn fast_cfg(dir: &std::path::Path) -> WalConfig {
    let mut c = WalConfig::new(dir);
    c.gc_min = Duration::from_micros(50);
    c.gc_max = Duration::from_micros(200);
    c
}

/// An Append frame for box 1 at `seq` with a fixed payload.
fn ap(seq: u64) -> WalRecord {
    WalRecord::Append {
        box_id: 1,
        seq,
        ts: 1_700_000_000_000 + seq,
        node: None,
        tag: Some("t".into()),
        data: b"payload".to_vec(),
    }
}

/// Replay every `wal-*.log` on a (post-crash) disk image, stopping each file at
/// its torn tail — exactly the recovery read path. Returns the recovered seqs.
fn replay_wal_seqs(disk: &FakeDisk, data_dir: &std::path::Path) -> Vec<u64> {
    let fs = disk.arc();
    let wal_dir = data_dir.join("wal");
    let mut files: Vec<PathBuf> = fs
        .read_dir(&wal_dir)
        .unwrap_or_default()
        .into_iter()
        .filter(|p| {
            p.file_name()
                .and_then(|s| s.to_str())
                .map(|n| n.starts_with("wal-") && n.ends_with(".log"))
                .unwrap_or(false)
        })
        .collect();
    files.sort();
    let mut seqs = Vec::new();
    for f in files {
        let r = WalReader::open_with(&fs, &f).expect("open wal file");
        for frame in r {
            let s = frame.record.seq();
            if s > 0 {
                seqs.push(s);
            }
        }
    }
    seqs
}

// ===========================================================================
// F-WAL-EIO-FSYNC
// ---------------------------------------------------------------------------
// EIO on sync_data (fdatasync) of a durable group-commit batch ⇒ the WHOLE
// batch is signalled Failed (no token acked); recovery contains no batch frame;
// no silent ack of an unsynced batch.
// ===========================================================================

#[test]
fn f_wal_eio_fsync() {
    let disk = FakeDisk::new();

    // Phase 1: three durable acked writes on a clean disk (the must-survive set).
    {
        let engine = open_engine(&disk);
        put_durable_box(&engine, "p");
        assert_eq!(durable_write(&engine, "p", "1").unwrap(), vec![1]);
        assert_eq!(durable_write(&engine, "p", "2").unwrap(), vec![2]);
        assert_eq!(durable_write(&engine, "p", "3").unwrap(), vec![3]);
        sync_wal_dir(&disk);
        drop(engine);
    }

    // Phase 2: wrap the disk so the group-commit sync_data EIOs. A durable append
    // MUST fail (the writer signals the whole batch Failed ⇒ token.wait returns
    // WriterGone ⇒ engine.write rolls back the staged batch and returns Err) and
    // MUST NOT ack. fail-always so any retried fsync also fails.
    let faulty: Arc<dyn Fs> = FaultFs::new(disk.arc(), FaultOp::SyncData, FaultKind::Eio, 0, false).arc();
    {
        let engine = Engine::with_data_dir_fs(cfg(), clock(), faulty).expect("reopen through faultfs");
        assert!(
            durable_write(&engine, "p", "4").is_err(),
            "durable append must FAIL when the group-commit sync_data EIOs (no silent ack)"
        );
        // The frame-4 bytes were buffered (pending) but their fsync errored, so
        // they never reached durable media. A power loss now drops them. Freeze
        // BEFORE dropping so the writer's Drop drain cannot harden the buffered
        // tail on a (faulted-anyway) device.
        disk.crash(TornDamage::None);
        drop(engine);
    }
    disk.reset_power();

    // Recovery: exactly the 3 prior durable frames, no trace of the failed 4th.
    let engine = open_engine(&disk);
    let (seqs, head) = recovered_seqs(&engine, "p");
    assert_eq!(seqs, vec![1, 2, 3], "the fsync-EIO batch left no frame; prior 3 intact");
    assert_eq!(head, 3, "head did not advance past the unacked failed batch");
    assert_dense_prefix(&seqs);
}

// ===========================================================================
// F-WAL-FSYNC-EIO-PERSIST
// ---------------------------------------------------------------------------
// fsyncgate: sync_data reports EIO **once** then succeeds (the page is marked
// clean and the error is lost on retry). The batch whose fsync errored must
// NEVER be acked even though a later fsync succeeds — the error poisons the
// in-flight batch. FakeDisk drops the just-written-but-failed bytes on crash.
// ===========================================================================

#[test]
fn f_wal_fsync_eio_persist() {
    let disk = FakeDisk::new();

    // Phase 1: two durable acked writes (the survivor set). These consume the
    // first group-commit fsync calls cleanly.
    {
        let engine = open_engine(&disk);
        put_durable_box(&engine, "q");
        assert_eq!(durable_write(&engine, "q", "1").unwrap(), vec![1]);
        assert_eq!(durable_write(&engine, "q", "2").unwrap(), vec![2]);
        sync_wal_dir(&disk);
        drop(engine);
    }

    // Phase 2: fail-ONCE on the very next sync_data, then succeed forever after
    // (the fsyncgate "error lost on retry" scenario). The poisoned durable write
    // MUST be reported failed (not acked); a *subsequent* successful fsync must
    // not retroactively ack the poisoned batch.
    let faulty = FaultFs::new(disk.arc(), FaultOp::SyncData, FaultKind::Eio, 0, true);
    let arc: Arc<dyn Fs> = faulty.arc();
    {
        let engine = Engine::with_data_dir_fs(cfg(), clock(), arc).expect("reopen through faultfs");

        // The first durable write hits the one-shot EIO ⇒ must fail (no ack).
        let poisoned = durable_write(&engine, "q", "3");
        assert!(
            poisoned.is_err(),
            "the durable write whose fsync EIOd must NOT be acked (fsyncgate poison)"
        );

        // A later durable write now fsyncs successfully (the device "recovered").
        // It is a fresh batch and may ack — but it must continue the seq space
        // contiguously and must never resurrect the poisoned frame as acked.
        let later = durable_write(&engine, "q", "4");
        assert!(
            later.is_ok(),
            "a later durable write succeeds once the transient fsync glitch clears"
        );

        sync_wal_dir(&disk);
        disk.crash(TornDamage::None);
        drop(engine);
    }
    disk.reset_power();

    // Recovery: the poisoned write must not appear as an acked durable record
    // ahead of an absent earlier one — survivors are a dense prefix, no gap, no
    // fabrication. The two phase-1 writes survive; whatever the retry-batch did,
    // the result is contiguous with no hole.
    let engine = open_engine(&disk);
    let (seqs, head) = recovered_seqs(&engine, "q");
    assert!(seqs.len() >= 2, "the two acked phase-1 durable writes survive: {seqs:?}");
    assert_eq!(&seqs[..2], &[1, 2], "the acked prefix survives intact");
    assert_dense_prefix(&seqs);
    assert!(head >= 2, "head covers at least the acked prefix");
    // No fabrication: every recovered seq is a genuine assigned seq (1..=4).
    for s in &seqs {
        assert!(*s <= 4, "no fabricated/future seq recovered: {seqs:?}");
    }
}

// ===========================================================================
// F-WAL-CRASH-AFTER-WRITE-PRE-FSYNC
// ---------------------------------------------------------------------------
// Power loss after write_at returns but before sync_data: FakeDisk.crash() with
// the batch bytes un-promoted ⇒ they vanish (drop-pending); recovery yields a
// clean prefix; an unacked durable write may be lost but acked ones (prior
// fsynced batches) survive.
//
// Driven at the WAL-writer layer for precise control: durable appends (acked,
// fsynced ⇒ promoted) form the survivor prefix; a trailing NON-durable submit
// leaves un-fsynced pending bytes — the exact "written but not yet fsynced"
// state — which the crash drops whole.
// ===========================================================================

#[test]
fn f_wal_crash_after_write_pre_fsync() {
    let disk = FakeDisk::new();
    let data_dir = PathBuf::from(DATA_DIR);
    let wal = Wal::open_at_with(disk.arc(), fast_cfg(&data_dir), 1, 0).unwrap();
    let w = wal.writer();

    // Acked durable prefix: each append blocks on the group fsync ⇒ promoted.
    for seq in 1..=5 {
        w.append(ap(seq), true).unwrap();
    }
    // A trailing NON-durable submit: the writer buffers it (write_at lands in the
    // pending image) but NO group fsync follows, so it is exactly "written but
    // not yet fsynced". Drop the token (fire-and-forget; never acked).
    let _ = w.submit(ap(6), false);
    std::thread::sleep(Duration::from_millis(5)); // let the writer buffer it.

    sync_wal_dir(&disk);
    // Power loss before any fsync of the trailing write ⇒ its pending bytes drop.
    disk.crash(TornDamage::None);
    drop(wal);

    let seqs = replay_wal_seqs(&disk, &data_dir);
    // The 5 fsynced (acked) frames survive; the un-fsynced frame 6 vanished.
    assert_eq!(seqs, vec![1, 2, 3, 4, 5], "un-promoted pre-fsync write dropped; acked prefix survives");
    assert_dense_prefix(&seqs);
}

// ===========================================================================
// F-WAL-CRASH-AFTER-FSYNC
// ---------------------------------------------------------------------------
// Power loss immediately after sync_data returns (the token is about to ack):
// FakeDisk.crash() with the batch already promoted to durable ⇒ the acked batch
// survives intact (acked ⇒ durable); recovery replays it; seq monotonic.
//
// Validated at BOTH layers: the raw WAL writer (every acked seq replays) and the
// fully-wired engine (every durable acked write is readable post-recovery).
// ===========================================================================

#[test]
fn f_wal_crash_after_fsync() {
    // --- Layer 1: the raw WAL writer. ---
    {
        let disk = FakeDisk::new();
        let data_dir = PathBuf::from(DATA_DIR);
        let wal = Wal::open_at_with(disk.arc(), fast_cfg(&data_dir), 1, 0).unwrap();
        let w = wal.writer();
        for seq in 1..=8 {
            // append blocks until the group fsync returns ⇒ FakeDisk promoted ⇒
            // acked. This is the instant *after* sync_data returns.
            w.append(ap(seq), true).unwrap();
        }
        sync_wal_dir(&disk);
        // Power loss right here: every acked batch is already durable.
        disk.crash(TornDamage::None);
        drop(wal);

        let seqs = replay_wal_seqs(&disk, &data_dir);
        assert_eq!(seqs, vec![1, 2, 3, 4, 5, 6, 7, 8], "every acked-after-fsync seq survives");
        assert_dense_prefix(&seqs);
    }

    // --- Layer 2: the fully wired engine (acked durable writes are readable). ---
    {
        let disk = FakeDisk::new();
        {
            let engine = open_engine(&disk);
            put_durable_box(&engine, "k");
            for i in 1..=5 {
                // engine.write blocks on the durable commit token ⇒ returns Ok only
                // after sync_data returned ⇒ acked & durable.
                assert_eq!(durable_write(&engine, "k", &i.to_string()).unwrap(), vec![i]);
            }
            sync_wal_dir(&disk);
            disk.crash(TornDamage::None);
            drop(engine);
        }
        disk.reset_power();

        let engine = open_engine(&disk);
        let (seqs, head) = recovered_seqs(&engine, "k");
        assert_eq!(seqs, vec![1, 2, 3, 4, 5], "all acked durable writes survive the crash");
        assert_eq!(head, 5, "head monotone, covers every acked write");
        assert_dense_prefix(&seqs);
    }
}

// ===========================================================================
// F-WAL-PARTIAL-BATCH-FSYNC
// ---------------------------------------------------------------------------
// A multi-frame batch is half-flushed: a durable prefix is promoted, the last
// frame is torn. The durable prefix frames replay; the torn last frame
// truncates. Because a batch is acked as a unit only after a FULL fsync, a torn
// last frame means that frame was never acked — assert no ACKED record is among
// the lost (no acked frame is misread / truncated away, no gap, no fabrication).
//
// Modelled exactly per the catalog inject_how ("FakeDisk promotes a prefix of
// the batch, tears the last frame on crash"): the acked durable appends are the
// promoted prefix; a trailing NON-durable (never-acked) submit is the in-flight
// last frame the crash tears. Swept over every torn-damage flavor + seeds.
// ===========================================================================

#[test]
fn f_wal_partial_batch_fsync() {
    for &damage in &[
        TornDamage::PrefixTruncate,
        TornDamage::ZeroSector,
        TornDamage::Garble,
    ] {
        for seed in [0xC0FFEEu64, 0x1234_5678, 0xDEAD_BEEF] {
            let disk = FakeDisk::with_seed(seed ^ damage as u64);
            let data_dir = PathBuf::from(DATA_DIR);
            let wal = Wal::open_at_with(disk.arc(), fast_cfg(&data_dir), 1, 0).unwrap();
            let w = wal.writer();

            // The acked durable prefix: each append fsyncs (the promoted prefix of
            // the "batch"). These MUST all survive (acked ⇒ durable).
            for seq in 1..=5 {
                w.append(ap(seq), true).unwrap();
            }
            // The in-flight last frame: a NON-durable submit, never group-fsynced,
            // so it is the un-acked tail the crash tears (prefix-truncate / zeroed
            // sector / garble). Its token is dropped ⇒ never acked.
            let _ = w.submit(ap(6), false);
            std::thread::sleep(Duration::from_millis(5));

            sync_wal_dir(&disk);
            disk.crash(damage);
            drop(wal);

            let seqs = replay_wal_seqs(&disk, &data_dir);

            // (1) NO ACKED RECORD LOST: the 5 fsynced-as-acked frames all replay.
            assert!(
                seqs.len() >= 5,
                "the acked durable prefix (5 frames) must all survive a torn last \
                 frame [{damage:?} seed={seed:#x}], got {seqs:?}"
            );
            // (2) NO FABRICATION / NO GAP: survivors are a dense contiguous prefix;
            //     the torn frame 6 is truncated, never misread as a record, and the
            //     payload of every survivor is genuine (seq <= 6).
            assert_dense_prefix(&seqs);
            for s in &seqs {
                assert!(*s <= 6, "no fabricated seq from the torn frame: {seqs:?}");
            }
            // (3) The torn frame either truncated (5 survivors) — or, if the tear
            //     happened to leave a CRC-valid frame, frame 6 replayed whole; both
            //     are contiguous and lose no acked record. Never a half-record.
            assert!(
                seqs.len() == 5 || seqs == vec![1, 2, 3, 4, 5, 6],
                "torn last frame truncates OR replays whole — never a partial \
                 record [{damage:?} seed={seed:#x}]: {seqs:?}"
            );
        }
    }
}
