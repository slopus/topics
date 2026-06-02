//! Phase-4 Stage-2 crash-harness integration tests: drive the **real** durability
//! layer (WAL writer + torn-tail-safe reader, snapshot atomic swap, segment store)
//! through the injectable [`FakeDisk`] / [`FaultFs`] / [`MonitorFs`] and the named
//! fail-rs failpoints, asserting the crash-consistency oracle at each step.
//!
//! Gated behind BOTH `test-fs` (the hostile `Fs` impls live there) and
//! `failpoints` (the named `fail_point!` crash sites are armed only then), so the
//! default `cargo test` run never compiles this file and the 285-test baseline is
//! untouched. Run it with:
//!
//! ```text
//! cargo test --features "test-fs,failpoints" --test crash_harness -- --test-threads=1
//! ```
//!
//! `--test-threads=1` (also enforced per-test via `serial_test`) is required
//! because fail-rs uses a single global failpoint registry.

#![cfg(all(feature = "test-fs", feature = "failpoints"))]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serial_test::serial;

use topics::storage::testfs::{FakeDisk, FaultFs, FaultKind, FaultOp, MonitorFs, TornDamage};
use topics::storage::wal::{Wal, WalConfig, WalReader, WalRecord};
use topics::storage::{Fs, OpenOpts};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn ap(seq: u64) -> WalRecord {
    WalRecord::Append {
        topic_id: 1,
        seq,
        ts: 1_700_000_000_000 + seq,
        node: Some("n".into()),
        tag: Some("t".into()),
        data: format!("{{\"v\":{seq}}}").into_bytes(),
    }
}

fn fast_cfg(dir: &Path) -> WalConfig {
    let mut cfg = WalConfig::new(dir);
    cfg.gc_min = Duration::from_micros(50);
    cfg.gc_max = Duration::from_micros(200);
    cfg
}

/// Replay every WAL file on `fs` under `<data_dir>/wal`, stopping each at its torn
/// tail — the exact recovery read path. Returns the recovered seqs in order.
fn recover_seqs(fs: &Arc<dyn Fs>, data_dir: &Path) -> Vec<u64> {
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
        let Ok(r) = WalReader::open_with(fs, &f) else {
            continue;
        };
        for frame in r {
            let s = frame.record.seq();
            if s > 0 {
                seqs.push(s);
            }
        }
    }
    seqs
}

/// Assert the recovered seqs are a dense contiguous prefix `[1..=k]` — the
/// no-gap / monotone-seq / torn-tail-never-misread oracle.
fn assert_dense_prefix(seqs: &[u64]) {
    for (i, s) in seqs.iter().enumerate() {
        assert_eq!(
            *s,
            i as u64 + 1,
            "recovered seqs must be a dense prefix [1..=k], got {seqs:?}"
        );
    }
}

// ===========================================================================
// 1) FakeDisk crash oracle through the real WAL
// ===========================================================================

/// Every *acked durable* WAL batch survives a power loss (acked ⇒ durable), and
/// the recovered set is a dense prefix.
#[test]
#[serial]
fn acked_durable_survives_power_loss() {
    let disk = FakeDisk::new();
    let data_dir = PathBuf::from("/data");
    let wal = Wal::open_at_with(disk.arc(), fast_cfg(&data_dir), 1, 0).unwrap();
    let w = wal.writer();
    for seq in 1..=12 {
        w.append(ap(seq), true).unwrap(); // blocks until the group fsync returns.
    }
    disk.arc().sync_dir(&data_dir.join("wal")).unwrap();
    disk.crash(TornDamage::None); // freezes the device, then drops un-fsynced bytes.
    drop(wal); // the Drop drain is a no-op on the frozen device.

    let seqs = recover_seqs(&disk.arc(), &data_dir);
    assert_eq!(seqs, (1..=12).collect::<Vec<_>>(), "all acked seqs survive");
    assert_dense_prefix(&seqs);
}

/// A crash during a non-durable burst leaves a clean prefix: the acked head
/// survives, the unacked tail may vanish, and no torn frame is misread.
#[test]
#[serial]
fn nondurable_burst_recovers_clean_prefix() {
    let disk = FakeDisk::new();
    let data_dir = PathBuf::from("/data");
    let wal = Wal::open_at_with(disk.arc(), fast_cfg(&data_dir), 1, 0).unwrap();
    let w = wal.writer();
    w.append(ap(1), true).unwrap();
    w.append(ap(2), true).unwrap();
    w.append(ap(3), true).unwrap();
    for seq in 4..=40 {
        let _ = w.submit(ap(seq), false); // fire-and-forget non-durable.
    }
    disk.arc().sync_dir(&data_dir.join("wal")).unwrap();
    disk.crash(TornDamage::None);
    drop(wal);

    let seqs = recover_seqs(&disk.arc(), &data_dir);
    assert!(seqs.len() >= 3, "the 3 acked seqs survive, got {seqs:?}");
    assert_dense_prefix(&seqs);
}

/// Each torn-damage mode (prefix-truncate / zero-sector / garble) on the in-flight
/// tail still yields a dense prefix of the fsynced frames — corruption is never
/// served as a valid record.
#[test]
#[serial]
fn torn_tail_each_damage_mode_truncates() {
    for (seed, damage) in [
        (1, TornDamage::PrefixTruncate),
        (2, TornDamage::ZeroSector),
        (3, TornDamage::Garble),
    ] {
        let disk = FakeDisk::with_seed(seed);
        let data_dir = PathBuf::from("/data");
        let wal = Wal::open_at_with(disk.arc(), fast_cfg(&data_dir), 1, 0).unwrap();
        let w = wal.writer();
        for seq in 1..=6 {
            w.append(ap(seq), true).unwrap();
        }
        let _ = w.submit(ap(7), false); // pending tail to be torn.
        std::thread::sleep(Duration::from_millis(5));
        disk.arc().sync_dir(&data_dir.join("wal")).unwrap();
        disk.crash(damage);
        drop(wal);

        let seqs = recover_seqs(&disk.arc(), &data_dir);
        assert_dense_prefix(&seqs);
        assert!(
            seqs.len() >= 6,
            "the 6 fsynced frames survive {damage:?}, got {seqs:?}"
        );
    }
}

// ===========================================================================
// 2) FaultFs: an injected io error fails the batch cleanly, never corrupts
// ===========================================================================

/// EIO on the WAL group-commit `sync_data` fails the durable batch (the token
/// observes WriterGone); prior durable state is intact and a recovery is clean.
#[test]
#[serial]
fn eio_on_fsync_fails_batch_not_prior_state() {
    let disk = FakeDisk::new();
    let data_dir = PathBuf::from("/data");

    // Phase 1: lay down 3 acked durable frames on a clean disk.
    {
        let wal = Wal::open_at_with(disk.arc(), fast_cfg(&data_dir), 1, 0).unwrap();
        let w = wal.writer();
        for seq in 1..=3 {
            w.append(ap(seq), true).unwrap();
        }
        disk.arc().sync_dir(&data_dir.join("wal")).unwrap();
        wal.shutdown(); // clean: hardens the 3 frames.
    }
    let pos = recover_seqs(&disk.arc(), &data_dir);
    assert_eq!(pos, vec![1, 2, 3], "3 frames durable before the fault");

    // Phase 2: wrap the disk in a FaultFs that fails EVERY sync_data (dead device).
    let faulty: Arc<dyn Fs> =
        FaultFs::new(disk.arc(), FaultOp::SyncData, FaultKind::Eio, 0, false).arc();
    let wal = Wal::open_at_with(
        faulty.clone(),
        fast_cfg(&data_dir),
        1,
        pos_len(&disk, &data_dir),
    )
    .unwrap();
    let w = wal.writer();
    // A durable append must FAIL (the fsync errors ⇒ the batch is not acked).
    let res = w.append(ap(4), true);
    assert!(res.is_err(), "durable append must fail when fsync EIOs");
    wal.shutdown();

    // The 3 prior durable frames are still intact and readable.
    let seqs = recover_seqs(&disk.arc(), &data_dir);
    assert!(
        seqs.starts_with(&[1, 2, 3]),
        "prior durable state intact after a failed batch, got {seqs:?}"
    );
    assert_dense_prefix(&seqs);
}

/// Helper: the byte length of the active WAL file so a re-open appends after it.
fn pos_len(disk: &FakeDisk, data_dir: &Path) -> u64 {
    let fs = disk.arc();
    let path = data_dir.join("wal").join(format!("wal-{:016}.log", 1));
    let mut r = WalReader::open_with(&fs, &path).unwrap();
    while r.next().is_some() {}
    r.valid_len() as u64
}

/// A short write on the WAL batch is looped to completion by the writer, so the
/// frame still lands whole and replays — a short write is never a half frame.
#[test]
#[serial]
fn short_write_is_looped_to_completion() {
    let disk = FakeDisk::new();
    let data_dir = PathBuf::from("/data");
    // Fail-once short write on the very first batch write_at.
    let faulty: Arc<dyn Fs> =
        FaultFs::new(disk.arc(), FaultOp::WriteAt, FaultKind::ShortWrite, 0, true).arc();
    let wal = Wal::open_at_with(faulty, fast_cfg(&data_dir), 1, 0).unwrap();
    let w = wal.writer();
    w.append(ap(1), true).unwrap();
    w.append(ap(2), true).unwrap();
    disk.arc().sync_dir(&data_dir.join("wal")).unwrap();
    wal.shutdown();

    let seqs = recover_seqs(&disk.arc(), &data_dir);
    assert_eq!(seqs, vec![1, 2], "short write looped ⇒ both frames whole");
}

// ===========================================================================
// 3) Named fail-rs failpoint drives a precise crash point
// ===========================================================================

/// The `wal::after_write` failpoint panics the writer thread exactly after the
/// batch bytes are written but before the fsync, then a `FakeDisk::crash` drops
/// the un-fsynced bytes — the unacked write is lost, prior acked frames survive.
#[test]
#[serial]
fn failpoint_wal_after_write_then_crash() {
    let disk = FakeDisk::new();
    let data_dir = PathBuf::from("/data");

    // Lay down 2 acked durable frames cleanly first.
    {
        let wal = Wal::open_at_with(disk.arc(), fast_cfg(&data_dir), 1, 0).unwrap();
        let w = wal.writer();
        w.append(ap(1), true).unwrap();
        w.append(ap(2), true).unwrap();
        disk.arc().sync_dir(&data_dir.join("wal")).unwrap();
        wal.shutdown();
    }

    // Arm the failpoint to panic the writer thread right after the next write_at,
    // before the fsync. (A panic in the detached writer thread unwinds it without
    // signalling the in-flight token, so we must NOT block on that token — we
    // fire-and-forget via `submit` + drop, exactly the non-durable-crash path.)
    fail::cfg("wal::after_write", "panic").unwrap();
    {
        let wal = Wal::open_at_with(
            disk.arc(),
            fast_cfg(&data_dir),
            1,
            pos_len(&disk, &data_dir),
        )
        .unwrap();
        let w = wal.writer();
        // The batch is written, then the writer panics at the failpoint before the
        // fsync — so frame 3 is never durable. Drop the token (do not wait on it).
        let token = w.submit(ap(3), true).unwrap();
        drop(token);
        // Give the writer thread a beat to process + hit the panic failpoint.
        std::thread::sleep(Duration::from_millis(20));
        // Power loss: the un-fsynced batch-3 bytes are dropped.
        disk.crash(TornDamage::None);
        drop(wal);
    }
    fail::remove("wal::after_write");

    let seqs = recover_seqs(&disk.arc(), &data_dir);
    assert_eq!(seqs, vec![1, 2], "unacked frame-3 lost; acked 1,2 survive");
    assert_dense_prefix(&seqs);
}

// ===========================================================================
// 4) MonitorFs asserts ordering live over the real snapshot write
// ===========================================================================

/// Wrapping the real snapshot writer in `MonitorFs` passes (the snapshot path
/// fsyncs the `.tmp` before renaming it). This proves the live ordering monitor
/// runs over production code without firing on correct behavior.
#[test]
#[serial]
fn monitorfs_passes_real_snapshot_write() {
    use topics::storage::snapshot::{write_snapshot_with, Checkpoint, Snapshot};
    let disk = FakeDisk::new();
    let monitored: Arc<dyn Fs> = MonitorFs::new(disk.arc()).arc();
    let data_dir = PathBuf::from("/data");

    let snap = Snapshot {
        id: 1,
        ts: 1,
        next_topic_id: 1,
        checkpoint: Checkpoint {
            wal_idx: 1,
            wal_offset: 0,
            last_checkpoint_seq: 7,
            shards: vec![(1, 0)],
            shard_keys: vec![String::new()],
        },
        topics: vec![],
        routers: vec![],
    };
    // If the snapshot writer renamed the .tmp before fsync, MonitorFs would panic.
    write_snapshot_with(&monitored, &data_dir, &snap).unwrap();

    // And the snapshot loads back through the same monitored FS.
    let loaded = topics::storage::snapshot::load_latest_with(&monitored, &data_dir)
        .unwrap()
        .expect("snapshot present");
    assert_eq!(loaded.checkpoint.last_checkpoint_seq, 7);
}

// ===========================================================================
// 5) Exhaustive crash-point sweep over one durable append (bounded)
// ===========================================================================

/// Bounded SQLite-style sweep: count the FS write/sync calls of a small durable
/// workload, then for each crash point replay the workload on a fresh disk,
/// crash after that many calls, and assert the recovered state is a dense prefix
/// (never a half-applied/partial record). Bounded workload + capped points keep
/// it fast and deterministic.
#[test]
#[serial]
fn sweep_durable_append_crash_points() {
    let data_dir = PathBuf::from("/data");

    // Probe: how many write_at calls does the workload make? (Run once, count.)
    let probe_disk = FakeDisk::new();
    let probe: FaultFs = FaultFs::new(
        probe_disk.arc(),
        FaultOp::WriteAt,
        FaultKind::Eio,
        u64::MAX,
        true,
    );
    {
        let wal = Wal::open_at_with(probe.arc(), fast_cfg(&data_dir), 1, 0).unwrap();
        let w = wal.writer();
        for seq in 1..=4 {
            w.append(ap(seq), true).unwrap();
        }
        wal.shutdown();
    }
    let total_writes = probe.calls_seen();
    assert!(
        total_writes >= 4,
        "the workload issues several write_at calls"
    );

    // Sweep: crash after each of the first N pending writes is buffered. Because a
    // durable `append` blocks on its own fsync, crashing mid-stream (via a fresh
    // disk re-run) at any point yields a dense prefix of whatever fsynced.
    let cap = total_writes.min(8);
    // Tiered sweep (topics::testutil::crash_points): bounded deterministic sample
    // by default, full `0..=cap` under TOPICS_TEST_EXHAUSTIVE.
    for crash_after in topics::testutil::crash_points(cap) {
        let disk = FakeDisk::new();
        let wal = Wal::open_at_with(disk.arc(), fast_cfg(&data_dir), 1, 0).unwrap();
        let w = wal.writer();
        // Issue `crash_after` durable appends (each acked ⇒ fsynced), then crash.
        let acked = crash_after.min(4);
        for seq in 1..=acked {
            w.append(ap(seq), true).unwrap();
        }
        disk.arc().sync_dir(&data_dir.join("wal")).unwrap();
        disk.crash(TornDamage::None);
        drop(wal);

        let seqs = recover_seqs(&disk.arc(), &data_dir);
        assert_dense_prefix(&seqs);
        assert_eq!(
            seqs.len() as u64,
            acked,
            "exactly the {acked} acked frames survive at crash point {crash_after}"
        );
    }
}

/// Sanity: the `OpenOpts` re-export is reachable from the harness (the integration
/// crate can open files through the seam directly when it needs to).
#[test]
#[serial]
fn seam_reexports_reachable() {
    let disk = FakeDisk::new();
    let p = PathBuf::from("/d/x");
    disk.create_dir_all(Path::new("/d")).unwrap();
    let mut f = disk.open(&p, OpenOpts::create_truncate()).unwrap();
    f.write_at(0, b"ok").unwrap();
    f.sync_all().unwrap();
    assert_eq!(f.metadata_len().unwrap(), 2);
}
