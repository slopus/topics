//! Phase-8B fault catalog — **snapshot-rename boundary** (4 strategies).
//!
//! These drive the *real* snapshot writer (`write_snapshot_with`) and loader
//! (`load_latest_with`) — and, for the integration oracle, a fully-wired
//! [`Engine`] — through the Phase-8A [`FakeDisk`] / [`FaultFs`] harness, injecting
//! a power-loss or an I/O error precisely on the snapshot install path
//! (tmp-write → fsync → rename → dir-fsync → prune) and asserting the
//! **ATOMIC SNAPSHOT SWAP** invariant (FAULT_TESTING §7):
//!
//! > After a crash anywhere in tmp-write→fsync→rename→dir-fsync→prune-old,
//! > recovery loads **exactly one** consistent snapshot — either the new one (if
//! > rename+dir-fsync durable) or the previous valid one — never a half-written
//! > body and never zero snapshots when a valid older one existed.
//!
//! Strategies implemented (catalog ids):
//!   * `F-SNAP-CRASH-AFTER-TMP-BEFORE-RENAME` — crash after the tmp fsync, before
//!     the rename: `load_latest` sees only the previous `snapshot-<n>.bin`; the
//!     stray `.tmp` is ignored; recovery = old snapshot + WAL replay.
//!   * `F-SNAP-EIO-RENAME` — EIO on the tmp→final rename: the write surfaces an
//!     error, the old snapshot stays authoritative and is NOT pruned, no partial
//!     install.
//!   * `F-SNAP-CRASH-BEFORE-PRUNE` — crash after the new snapshot is durable but
//!     before old snapshots are pruned: both files exist, `load_latest` picks the
//!     highest valid id (new), the leftover old is harmless and pruned on the next
//!     successful write — no double-load.
//!   * `F-SWEEP-SNAPSHOT-WRITE` — exhaustive bounded crash-point sweep over every
//!     FS mutating call of one snapshot write: at every crash point exactly one
//!     valid snapshot loads (old or new) and an end-to-end engine recovery is
//!     consistent; never zero valid snapshots; never a half body loaded.
//!
//! `failpoints` is *not* required (and not enabled by `--features test-fs`): the
//! named `snapshot::*` fail-rs sites are no-ops here, so every crash point is
//! driven at the harness level — `FakeDisk.crash()` after the Nth snapshot-path
//! FS call (the in-process "power loss after the Nth FS mutating call" injector),
//! exactly as `crash_oracle.rs`'s sweep does for the WAL path.
//!
//! ```text
//! cargo test --features test-fs --test fault_snap_rename
//! ```

#![cfg(feature = "test-fs")]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use serde_json::json;

use streams::clock::{SharedClock, TestClock};
use streams::config::ServerConfig;
use streams::engine::Engine;
use streams::storage::testfs::{FakeDisk, FaultFs, FaultKind, FaultOp, TornDamage};
use streams::storage::{
    load_latest_with, next_snapshot_id_with, write_snapshot_with, Checkpoint, Fs, Snapshot,
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

/// Append one durable (group-fsynced ⇒ acked) record to `name`, auto-creating a
/// durable box. Returns the assigned seq.
fn append_durable(engine: &Engine, name: &str, data: &str) -> u64 {
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
    let resp = engine.write(name, req, true).expect("durable append acked");
    resp.last_seq
}

/// Create a durable box explicitly (so auto-create defaults don't make it
/// non-durable).
fn put_durable_box(engine: &Engine, name: &str) {
    engine
        .put_box(
            name,
            BoxConfig {
                r#type: BoxType::Log,
                durable: true,
                ..Default::default()
            },
        )
        .expect("put durable box");
}

/// Read every live record's `data` string back through the real diff path.
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
                    include_meta: false,
                    wait_ms: 0,
                },
            )
            .expect("diff");
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

/// Make the WAL + meta dir names durable (production does this create+dir-fsync at
/// open; we model it explicitly so the files survive a `crash()`).
fn sync_dirs(disk: &FakeDisk) {
    let fs = disk.arc();
    let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("wal"));
    let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("meta"));
}

/// The `meta/` dir on the data disk.
fn meta_dir() -> PathBuf {
    PathBuf::from(DATA_DIR).join("meta")
}

/// Count `snapshot-*.bin` files (ignoring `.tmp`) currently durable+live on disk.
fn snapshot_file_count(disk: &FakeDisk) -> usize {
    let fs = disk.arc();
    fs.read_dir(&meta_dir())
        .map(|v| {
            v.into_iter()
                .filter(|p| {
                    p.file_name()
                        .and_then(|s| s.to_str())
                        .map(|n| n.starts_with("snapshot-") && n.ends_with(".bin"))
                        .unwrap_or(false)
                })
                .count()
        })
        .unwrap_or(0)
}

/// A tiny synthetic snapshot for direct write/load tests (the engine-level
/// strategies use the real `capture` path instead).
fn mk_snapshot(id: u64, seq: u64) -> Snapshot {
    Snapshot {
        id,
        ts: 1,
        next_box_id: 1,
        checkpoint: Checkpoint {
            wal_idx: 1,
            wal_offset: 0,
            last_checkpoint_seq: seq,
        },
        boxes: vec![],
        routers: vec![],
    }
}

// ===========================================================================
// CrashAfterSnap — drive FakeDisk.crash() after the Nth snapshot-path FS call.
//
// The snapshot writer is a single synchronous call on the test thread, so a
// global FS-call counter over THIS wrapper sees exactly (and only) the snapshot
// path's calls when we route just `write_snapshot_with` through it — no racing
// WAL-writer thread. We count the mutating namespace/byte ops the snapshot install
// issues (open/write_at/sync/rename/sync_dir/remove_file) and fire one crash after
// the chosen index.
// ===========================================================================

/// Which FS call classes the snapshot sweep crashes "after".
#[derive(Clone)]
struct CrashAfterSnap {
    disk: FakeDisk,
    at: u64,
    seen: Arc<AtomicU64>,
    tripped: Arc<std::sync::atomic::AtomicBool>,
}

impl CrashAfterSnap {
    fn new(disk: FakeDisk, at: u64) -> Self {
        CrashAfterSnap {
            disk,
            at,
            seen: Arc::new(AtomicU64::new(0)),
            tripped: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    fn arc(&self) -> Arc<dyn Fs> {
        Arc::new(self.clone())
    }

    /// Total mutating FS calls observed (size the sweep with `at = u64::MAX`).
    fn calls_seen(&self) -> u64 {
        self.seen.load(Ordering::SeqCst)
    }

    /// Count one mutating FS call; trip the (single) crash once when we pass `at`.
    fn tick(&self) {
        let idx = self.seen.fetch_add(1, Ordering::SeqCst);
        if idx == self.at
            && self
                .tripped
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            // Power loss right AFTER this FS call landed (its bytes/dir-op are in
            // the pending image; only fsynced-before ops are durable). No torn
            // damage — a clean inter-call power cut is the snapshot-swap case.
            self.disk.crash(TornDamage::None);
        }
    }
}

struct CrashAfterSnapFile {
    inner: Box<dyn streams::storage::File>,
    owner: CrashAfterSnap,
}

impl streams::storage::File for CrashAfterSnapFile {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read_at(offset, buf)
    }
    fn write_at(&mut self, offset: u64, buf: &[u8]) -> std::io::Result<usize> {
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
        let r = self.inner.sync_data();
        self.owner.tick();
        r
    }
    fn sync_all(&self) -> std::io::Result<()> {
        let r = self.inner.sync_all();
        self.owner.tick();
        r
    }
    fn metadata_len(&self) -> std::io::Result<u64> {
        self.inner.metadata_len()
    }
    fn read_to_end_from(&self, offset: u64, out: &mut Vec<u8>) -> std::io::Result<()> {
        self.inner.read_to_end_from(offset, out)
    }
}

impl Fs for CrashAfterSnap {
    fn open(
        &self,
        path: &Path,
        opts: streams::storage::OpenOpts,
    ) -> std::io::Result<Box<dyn streams::storage::File>> {
        let inner = self.disk.open(path, opts)?;
        // A create-truncate open of the `.tmp` is itself a mutating namespace op.
        if opts.create || opts.truncate {
            self.tick();
        }
        Ok(Box::new(CrashAfterSnapFile {
            inner,
            owner: self.clone(),
        }))
    }
    fn rename(&self, from: &Path, to: &Path) -> std::io::Result<()> {
        let r = self.disk.rename(from, to);
        self.tick();
        r
    }
    fn remove_file(&self, path: &Path) -> std::io::Result<()> {
        let r = self.disk.remove_file(path);
        self.tick();
        r
    }
    fn read_dir(&self, dir: &Path) -> std::io::Result<Vec<PathBuf>> {
        self.disk.read_dir(dir)
    }
    fn sync_dir(&self, dir: &Path) -> std::io::Result<()> {
        let r = self.disk.sync_dir(dir);
        self.tick();
        r
    }
    fn create_dir_all(&self, dir: &Path) -> std::io::Result<()> {
        self.disk.create_dir_all(dir)
    }
    fn exists(&self, path: &Path) -> bool {
        self.disk.exists(path)
    }
    fn metadata_len(&self, path: &Path) -> std::io::Result<u64> {
        self.disk.metadata_len(path)
    }
}

// ===========================================================================
// F-SNAP-CRASH-AFTER-TMP-BEFORE-RENAME
// ===========================================================================

/// Crash after the snapshot `.tmp` is written + fsynced but BEFORE the rename:
/// `load_latest` finds only the previous `snapshot-<n>.bin` (the stray `.tmp` is
/// ignored — `list_snapshot_files` only matches `snapshot-<n>.bin`), and a full
/// engine recovery loads the OLD snapshot + replays the WAL, recovering every
/// acked record. No half-snapshot is ever loaded.
#[test]
fn f_snap_crash_after_tmp_before_rename() {
    let disk = FakeDisk::new();

    // Phase 1: a durable workload + a FIRST durable snapshot (fully installed).
    {
        let engine = open_engine(&disk);
        put_durable_box(&engine, "jobs");
        append_durable(&engine, "jobs", "a");
        append_durable(&engine, "jobs", "b");
        engine.write_snapshot().expect("snapshot #1 installs");
        // More acked appends AFTER the snapshot (covered by the WAL, not snap #1).
        append_durable(&engine, "jobs", "c");
        sync_dirs(&disk);
        drop(engine);
    }
    assert_eq!(snapshot_file_count(&disk), 1, "snapshot #1 durably installed");
    let snap1_id = next_snapshot_id_with(&disk.arc(), Path::new(DATA_DIR)) - 1;

    // Phase 2: write a SECOND snapshot directly, but freeze the device after the
    // tmp fsync and before the rename (drive the snapshot path on the test thread
    // through CrashAfterSnap and stop at the rename boundary). The tmp bytes are
    // pending+un-renamed; the crash drops the un-fsynced dir op (the tmp's create
    // was dir-fsynced? no — we never sync_dir before the crash here), so the `.tmp`
    // does not even survive; either way it is never a `snapshot-<n>.bin`.
    let snap2 = mk_snapshot(snap1_id + 1, 999);
    // Mutating snapshot-path FS calls in order: open(tmp), write_at(body),
    // sync_all(tmp), rename, sync_dir, [read_dir], remove_file(old)... Crash AFTER
    // index 2 (the tmp sync_all) ⇒ before the rename (index 3).
    let trip = CrashAfterSnap::new(disk.clone(), 2);
    let _ = write_snapshot_with(&trip.arc(), Path::new(DATA_DIR), &snap2);
    disk.reset_power();

    // The new snapshot id never became a real `snapshot-<n>.bin` (no rename
    // happened), so exactly the previous valid snapshot remains loadable.
    let loaded = load_latest_with(&disk.arc(), Path::new(DATA_DIR))
        .expect("load ok")
        .expect("a previous valid snapshot remains");
    assert_eq!(loaded.id, snap1_id, "stray .tmp ignored; previous snapshot loads");

    // Full engine recovery: old snapshot + WAL replay restores every acked record.
    let engine = open_engine(&disk);
    let recs = read_records(&engine, "jobs");
    assert_eq!(
        recs.iter().map(|(_, v)| v.clone()).collect::<Vec<_>>(),
        vec!["a", "b", "c"],
        "old snapshot + WAL replay recovers all acked records, no half-snapshot"
    );
}

// ===========================================================================
// F-SNAP-EIO-RENAME
// ===========================================================================

/// EIO on the tmp→final rename: `write_snapshot_with` returns an error, the
/// previous snapshot stays authoritative and is NOT pruned (prune runs only after
/// a successful rename + dir-fsync), and recovery loads the old snapshot + WAL.
#[test]
fn f_snap_eio_rename() {
    let disk = FakeDisk::new();

    // Phase 1: durable workload + first installed snapshot.
    {
        let engine = open_engine(&disk);
        put_durable_box(&engine, "jobs");
        append_durable(&engine, "jobs", "a");
        append_durable(&engine, "jobs", "b");
        engine.write_snapshot().expect("snapshot #1 installs");
        append_durable(&engine, "jobs", "c");
        sync_dirs(&disk);
        drop(engine);
    }
    assert_eq!(snapshot_file_count(&disk), 1);
    let snap1_id = next_snapshot_id_with(&disk.arc(), Path::new(DATA_DIR)) - 1;

    // Phase 2: a fail-once EIO on the FIRST rename (the snapshot install rename).
    let faulty = FaultFs::new(disk.arc(), FaultOp::Rename, FaultKind::Eio, 0, true);
    let snap2 = mk_snapshot(snap1_id + 1, 999);
    let res = write_snapshot_with(&faulty.arc(), Path::new(DATA_DIR), &snap2);
    assert!(res.is_err(), "rename EIO surfaces as a snapshot error");

    // The old snapshot is untouched and still the only `snapshot-<n>.bin`: no
    // partial install, the new id was never renamed into place, and the prune step
    // (which would remove the old) never ran.
    assert_eq!(
        snapshot_file_count(&disk),
        1,
        "old snapshot not pruned; new not installed"
    );
    let loaded = load_latest_with(&disk.arc(), Path::new(DATA_DIR))
        .expect("load ok")
        .expect("old snapshot still loadable");
    assert_eq!(loaded.id, snap1_id, "old snapshot stays authoritative");

    // Full recovery still yields every acked record (old snapshot + WAL).
    let engine = open_engine(&disk);
    let recs = read_records(&engine, "jobs");
    assert_eq!(
        recs.iter().map(|(_, v)| v.clone()).collect::<Vec<_>>(),
        vec!["a", "b", "c"],
    );
}

// ===========================================================================
// F-SNAP-CRASH-BEFORE-PRUNE
// ===========================================================================

/// Crash after the NEW snapshot is durable (renamed + dir-fsynced) but before the
/// old snapshots are pruned: both `snapshot-<old>.bin` and `snapshot-<new>.bin`
/// exist; `load_latest` walks newest-first and returns the new (highest valid id);
/// the leftover old is harmless and gets pruned on the next successful write. No
/// double-load, no zero-snapshot window.
#[test]
fn f_snap_crash_before_prune() {
    let disk = FakeDisk::new();

    // Phase 1: durable workload + first installed snapshot.
    {
        let engine = open_engine(&disk);
        put_durable_box(&engine, "jobs");
        append_durable(&engine, "jobs", "a");
        append_durable(&engine, "jobs", "b");
        engine.write_snapshot().expect("snapshot #1 installs");
        append_durable(&engine, "jobs", "c");
        sync_dirs(&disk);
        drop(engine);
    }
    assert_eq!(snapshot_file_count(&disk), 1);
    let snap1_id = next_snapshot_id_with(&disk.arc(), Path::new(DATA_DIR)) - 1;

    // Phase 2: write snapshot #2; crash AFTER the post-rename sync_dir (the new
    // snapshot is durable) but BEFORE the prune remove_file of the old one.
    // Probe the FS-call index of that boundary first, then drive a crash there.
    //
    // Snapshot-path mutating FS calls, in order:
    //   0: open(tmp, create_truncate)
    //   1: write_at(body)
    //   2: sync_all(tmp)
    //   3: rename(tmp -> final)
    //   4: sync_dir(meta)          <-- new snapshot durable AFTER this returns
    //   5: remove_file(old)        <-- prune (we crash before this)
    // Crash AFTER index 4 ⇒ new durable, prune not yet run.
    let snap2 = mk_snapshot(snap1_id + 1, 999);

    // Probe to confirm the boundary index (defensive: don't hard-code if the path
    // changes). Run the write on a throwaway disk image to count calls.
    let probe_disk = FakeDisk::new();
    {
        // Seed the probe disk with a prior snapshot so the prune step has an old
        // file to remove (matching Phase 1's single old snapshot).
        write_snapshot_with(&probe_disk.arc(), Path::new(DATA_DIR), &mk_snapshot(snap1_id, 1))
            .expect("seed old snapshot");
        probe_disk.arc().sync_dir(&meta_dir()).unwrap();
    }
    let probe = CrashAfterSnap::new(probe_disk.clone(), u64::MAX);
    write_snapshot_with(&probe.arc(), Path::new(DATA_DIR), &snap2).expect("probe write");
    // calls: open,write,sync_all,rename,sync_dir,remove_file = 6; sync_dir is the
    // 5th call (index 4), the prune remove_file is index 5.
    let dirfsync_idx = probe.calls_seen().saturating_sub(2); // index of sync_dir.

    let trip = CrashAfterSnap::new(disk.clone(), dirfsync_idx);
    let _ = write_snapshot_with(&trip.arc(), Path::new(DATA_DIR), &snap2);
    disk.reset_power();

    // Both snapshots exist (the new is durable, the old was not pruned).
    assert_eq!(
        snapshot_file_count(&disk),
        2,
        "new durable + old not yet pruned ⇒ both files present"
    );
    // load_latest picks the highest valid id (the new snapshot), no double-load.
    let loaded = load_latest_with(&disk.arc(), Path::new(DATA_DIR))
        .expect("load ok")
        .expect("a valid snapshot loads");
    assert_eq!(loaded.id, snap1_id + 1, "highest valid id (new) loads");

    // The next SUCCESSFUL snapshot write prunes the harmless leftover old one.
    let snap3 = mk_snapshot(snap1_id + 2, 1234);
    write_snapshot_with(&disk.arc(), Path::new(DATA_DIR), &snap3).expect("snapshot #3 installs");
    assert_eq!(
        snapshot_file_count(&disk),
        1,
        "the next successful write prunes all lower ids"
    );
    assert_eq!(
        load_latest_with(&disk.arc(), Path::new(DATA_DIR))
            .unwrap()
            .unwrap()
            .id,
        snap1_id + 2,
    );
}

// ===========================================================================
// F-SWEEP-SNAPSHOT-WRITE
// ===========================================================================

/// Exhaustive bounded crash-point sweep across one snapshot write
/// (tmp-write/sync_all/rename/sync_dir/prune). For every crash point `0..=M`:
/// reset to the pre-op durable image (a fully-installed snapshot #1 + a durable
/// WAL covering every acked record), run the snapshot #2 write but `crash()` after
/// the crash_point-th snapshot-path FS call, then assert the ATOMIC SNAPSHOT SWAP
/// oracle:
///   * `load_latest` returns exactly one valid snapshot (the old #1 or the new
///     #2) — never zero, never a half-decoded body;
///   * a full engine recovery (snapshot + WAL replay) recovers every acked record
///     with no gap, regardless of which snapshot survived.
#[test]
fn f_sweep_snapshot_write() {
    // Build the canonical pre-op durable image once, then snapshot its bytes by
    // rebuilding it fresh per crash point (FakeDisk has no cheap clone-of-durable,
    // so we replay the small workload — it is bounded and deterministic).
    let build_preop = |disk: &FakeDisk| -> (u64, Vec<String>) {
        let engine = open_engine(disk);
        put_durable_box(&engine, "jobs");
        append_durable(&engine, "jobs", "a");
        engine.write_snapshot().expect("pre-op snapshot #1");
        // One acked append AFTER the snapshot, covered by the WAL (not snap #1), so
        // recovery must replay it on top of whichever snapshot survives the crash.
        append_durable(&engine, "jobs", "b");
        sync_dirs(disk);
        let id1 = next_snapshot_id_with(&disk.arc(), Path::new(DATA_DIR)) - 1;
        let recs = read_records(&engine, "jobs")
            .into_iter()
            .map(|(_, v)| v)
            .collect();
        drop(engine);
        (id1, recs)
    };

    // The acked record set the pre-op image guarantees (used as the recovery
    // oracle at every crash point).
    let expected_recs = {
        let d = FakeDisk::new();
        build_preop(&d).1
    };
    assert_eq!(expected_recs, vec!["a", "b"], "pre-op image is deterministic");

    // Probe M: count the snapshot-path FS calls of one snapshot #2 write.
    let total_calls = {
        let d = FakeDisk::new();
        let (id1, _) = build_preop(&d);
        let probe = CrashAfterSnap::new(d.clone(), u64::MAX);
        let snap2 = mk_snapshot(id1 + 1, 4242);
        write_snapshot_with(&probe.arc(), Path::new(DATA_DIR), &snap2).expect("probe write");
        probe.calls_seen()
    };
    assert!(
        total_calls >= 5,
        "snapshot write issues several FS calls (M={total_calls})"
    );

    // Sweep every crash point (bounded; the small workload + handful of FS calls
    // keep this well under a second).
    for crash_point in 0..=total_calls {
        let disk = FakeDisk::new();
        let (id1, _) = build_preop(&disk);

        // Drive snapshot #2 through a crash-after-Nth wrapper.
        let snap2 = mk_snapshot(id1 + 1, 4242);
        let trip = CrashAfterSnap::new(disk.clone(), crash_point);
        let _ = write_snapshot_with(&trip.arc(), Path::new(DATA_DIR), &snap2);
        disk.reset_power();

        // (a) Exactly one valid snapshot loads — never zero, never half-decoded.
        let loaded = load_latest_with(&disk.arc(), Path::new(DATA_DIR))
            .expect("snapshot load never errors")
            .unwrap_or_else(|| {
                panic!("crash_point {crash_point}: ZERO valid snapshots (lost the old one)")
            });
        assert!(
            loaded.id == id1 || loaded.id == id1 + 1,
            "crash_point {crash_point}: loaded snapshot id {} is neither old ({id1}) nor new ({})",
            loaded.id,
            id1 + 1
        );

        // (b) Full engine recovery (snapshot + WAL replay) recovers every acked
        //     record, a dense contiguous prefix, regardless of which snapshot won.
        let engine = open_engine(&disk);
        let recs: Vec<String> = read_records(&engine, "jobs")
            .into_iter()
            .map(|(_, v)| v)
            .collect();
        assert_eq!(
            recs, expected_recs,
            "crash_point {crash_point}: recovery must restore all acked records (no gap, no half body)"
        );
        drop(engine);

        // (c) Idempotent recovery at a few representative points.
        if crash_point % 3 == 0 {
            let engine2 = open_engine(&disk);
            let recs2: Vec<String> = read_records(&engine2, "jobs")
                .into_iter()
                .map(|(_, v)| v)
                .collect();
            assert_eq!(recs2, expected_recs, "recovery idempotent at {crash_point}");
            drop(engine2);
        }
    }
}
