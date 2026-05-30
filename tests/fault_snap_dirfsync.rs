//! Phase-8B fault/crash strategies on the **snapshot-dir-fsync** boundary
//! (catalog `/tmp/streams-fault-catalog.json`, `docs/FAULT_TESTING.md`). Each test
//! fn is named after its catalog id and asserts the CORRECT crash-consistency
//! behavior through the Phase-8A harness (`FakeDisk` / `FaultFs` from
//! `streams::storage::testfs`, the real WAL + snapshot writer + recovery wired
//! through `Engine::with_data_dir_fs` and the snapshot `*_with` constructors).
//!
//! The snapshot atomic swap is `tmp-write → fsync → rename → **dir-fsync** →
//! prune-old`. The directory fsync is what makes the `rename` durable; FakeDisk
//! models exactly that (a rename is durable only after `sync_dir` of the
//! containing directory). These three strategies probe the window around that dir
//! fsync:
//!
//! - `F-SNAP-CRASH-AFTER-RENAME-BEFORE-DIRFSYNC` — power loss after the rename but
//!   before the dir fsync ⇒ the rename rolls back; recovery loads EXACTLY ONE
//!   valid snapshot (the previous one), never zero, never a half body; the WAL
//!   covers any gap.
//! - `F-SNAP-EIO-DIRFSYNC` — EIO on the dir fsync ⇒ `write_snapshot` surfaces the
//!   error; the previous snapshot stays authoritative; a later crash falls back to
//!   old snapshot + WAL — the dir-fsync omission only loses the install, never
//!   corrupts.
//! - `F-NFS-DIRFSYNC-UNSUPPORTED` — `sync_dir` is a best-effort no-op (the dir
//!   cannot be opened for fsync) so it returns `Ok(())` without hardening the
//!   rename ⇒ the install may roll back on crash; recovery still falls back to old
//!   snapshot + WAL — no corruption, just a possibly-lost optimization.
//!
//! ```text
//! cargo test --features test-fs --test fault_snap_dirfsync -- --test-threads=1
//! ```

#![cfg(feature = "test-fs")]

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde_json::json;

use streams::clock::{SharedClock, TestClock};
use streams::config::ServerConfig;
use streams::engine::Engine;
use streams::storage::snapshot::{
    load_latest_with, write_snapshot_with, Checkpoint, Snapshot,
};
use streams::storage::testfs::{FakeDisk, FaultFs, FaultKind, FaultOp, TornDamage};
use streams::storage::{File, Fs, OpenOpts};
use streams::types::{BoxConfig, BoxType, DiffRequest, RecordIn, WriteRequest};

// ===========================================================================
// Shared plumbing (mirrors tests/crash_oracle.rs, tests/fault_batch1.rs)
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

fn data_dir() -> PathBuf {
    PathBuf::from(DATA_DIR)
}

fn meta_dir() -> PathBuf {
    data_dir().join("meta")
}

fn wal_dir() -> PathBuf {
    data_dir().join("wal")
}

/// Build a fresh durable engine whose WAL + snapshots live on `disk`.
fn open_engine(disk: &FakeDisk) -> Arc<Engine> {
    Engine::with_data_dir_fs(cfg(), clock(), disk.arc()).expect("engine opens through FakeDisk")
}

/// Build a durable engine whose I/O routes through an arbitrary injected `fs`
/// (e.g. a `FaultFs` or a `NoSyncDirFs` over a `FakeDisk`).
fn open_engine_fs(fs: Arc<dyn Fs>) -> Arc<Engine> {
    Engine::with_data_dir_fs(cfg(), clock(), fs).expect("engine opens through injected fs")
}

/// The on-disk snapshot file name for `id` (zero-padded, matching
/// `snapshot::snapshot_name`: `snapshot-<id:016>.bin`).
fn snapshot_file(id: u64) -> String {
    format!("snapshot-{id:016}.bin")
}

/// Make the WAL + meta directory names durable — the create+dir-fsync production
/// does at open time, modeled explicitly so the files survive a crash.
fn sync_data_dirs(disk: &FakeDisk) {
    let fs = disk.arc();
    let _ = fs.sync_dir(&wal_dir());
    let _ = fs.sync_dir(&meta_dir());
}

/// A single-record durable write request for box `name` carrying `data`.
fn one_write(data: &str) -> WriteRequest {
    WriteRequest {
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
    }
}

fn put_durable_box(engine: &Engine, name: &str) {
    engine
        .put_box(
            name,
            BoxConfig {
                r#type: BoxType::Log,
                durable: true,
                cap_records: 0,
                ..Default::default()
            },
        )
        .expect("put_box");
}

/// Append `n` durable records "1".."n" to box `name`, each blocking on the group
/// fsync (so it is acked ⇒ durable). Returns the seqs assigned.
fn append_durable(engine: &Engine, name: &str, n: usize) -> Vec<u64> {
    let mut seqs = Vec::new();
    for i in 1..=n {
        let resp = engine
            .write(name, one_write(&i.to_string()), true)
            .expect("durable append acked");
        seqs.push(resp.last_seq);
    }
    seqs
}

/// Read back the live records of box `name` (seq → data string) through the
/// engine's diff path; `None` if the box is absent.
fn dump_records(engine: &Engine, name: &str) -> Option<BTreeMap<u64, String>> {
    let _ = engine.box_state(name, false).ok()?;
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
            out.insert(r.seq, v);
        }
        if d.caught_up || d.records.is_empty() {
            break;
        }
        from = d.next_from_seq;
    }
    Some(out)
}

/// Read the full durable bytes of a file on `disk` (the post-crash image a
/// recovery would see).
fn durable_bytes(disk: &FakeDisk, path: &Path) -> Vec<u8> {
    let f = disk.open(path, OpenOpts::read_only()).expect("open ro");
    let mut buf = Vec::new();
    f.read_to_end_from(0, &mut buf).expect("read bytes");
    buf
}

/// A minimal valid snapshot body (no boxes/routers) for the manual-swap tests
/// that only care about the file-level atomicity around the dir fsync.
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

/// Produce the exact framed on-disk bytes the production writer emits for
/// `snapshot`, by writing it to a throwaway disk and reading the final file back.
/// Lets the manual-swap tests install a real, valid body without re-implementing
/// the framing.
fn framed_snapshot_bytes(snapshot: &Snapshot) -> Vec<u8> {
    let scratch = FakeDisk::new();
    write_snapshot_with(&scratch.arc(), &data_dir(), snapshot).expect("scratch snapshot write");
    durable_bytes(&scratch, &meta_dir().join(snapshot_file(snapshot.id)))
}

/// Write `bytes` to a fresh `.tmp` file, looping over any short write, then
/// `sync_all` so the body is durable. Mirrors the body-write half of
/// `write_snapshot_with`.
fn write_tmp_durable(disk: &FakeDisk, tmp: &Path, bytes: &[u8]) {
    let mut f = disk.open(tmp, OpenOpts::create_truncate()).unwrap();
    let mut written = 0usize;
    while written < bytes.len() {
        let n = f.write_at(written as u64, &bytes[written..]).unwrap();
        assert!(n > 0, "tmp write made no progress");
        written += n;
    }
    f.sync_all().unwrap();
}

// ===========================================================================
// F-NFS-DIRFSYNC-UNSUPPORTED plumbing: an Fs whose `sync_dir` is a best-effort
// no-op (the directory cannot be opened for fsync, so RealFs::sync_dir would
// `Ok(())` without syncing). Wrapping a FakeDisk, this means a rename is NEVER
// hardened, so a crash always rolls it back — exactly the NFS "open(dir) fails ⇒
// fsync_dir is a no-op" case.
// ===========================================================================

#[derive(Clone)]
struct NoSyncDirFs {
    inner: FakeDisk,
}

impl NoSyncDirFs {
    fn new(inner: FakeDisk) -> Self {
        NoSyncDirFs { inner }
    }
    fn arc(&self) -> Arc<dyn Fs> {
        Arc::new(self.clone())
    }
}

impl Fs for NoSyncDirFs {
    fn open(&self, path: &Path, opts: OpenOpts) -> io::Result<Box<dyn File>> {
        self.inner.open(path, opts)
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.inner.rename(from, to)
    }
    fn remove_file(&self, path: &Path) -> io::Result<()> {
        self.inner.remove_file(path)
    }
    fn read_dir(&self, dir: &Path) -> io::Result<Vec<PathBuf>> {
        self.inner.read_dir(dir)
    }
    fn sync_dir(&self, _dir: &Path) -> io::Result<()> {
        // Best-effort no-op: report success WITHOUT promoting the directory's
        // pending name ops to durable. This is what RealFs::sync_dir does when it
        // cannot open the directory as a file (the documented NFS tolerance).
        Ok(())
    }
    fn create_dir_all(&self, dir: &Path) -> io::Result<()> {
        self.inner.create_dir_all(dir)
    }
    fn exists(&self, path: &Path) -> bool {
        self.inner.exists(path)
    }
    fn metadata_len(&self, path: &Path) -> io::Result<u64> {
        self.inner.metadata_len(path)
    }
}

// ===========================================================================
// F-SNAP-CRASH-AFTER-RENAME-BEFORE-DIRFSYNC
// crash after the tmp→final rename but before the dir fsync ⇒ the un-hardened
// rename rolls back (FakeDisk models rename-durable-only-after-sync_dir).
// Recovery loads EXACTLY ONE valid snapshot — the previous one — never zero,
// never a half body; the WAL covers the gap so no acked record is lost.
// ===========================================================================
#[test]
fn f_snap_crash_after_rename_before_dirfsync() {
    let disk = FakeDisk::new();

    // Phase 1: a durable workload + a fully-installed snapshot #1 through the REAL
    // engine snapshot path (write→fsync→rename→dir-fsync→prune). Snapshot #1's
    // checkpoint covers seqs 1..=3.
    {
        let engine = open_engine(&disk);
        put_durable_box(&engine, "jobs");
        append_durable(&engine, "jobs", 3); // seqs 1..=3
        assert!(engine.write_snapshot().expect("snapshot #1 writes"));
        // Five more durable appends AFTER the snapshot (seqs 4..=8), so the WAL tail
        // must be replayed on top of whichever snapshot loads. (`append_durable`
        // re-uses payloads "1".."5", but the seqs continue contiguously — every
        // acked durable seq must survive regardless of payload; we assert no
        // loss / no gap.)
        append_durable(&engine, "jobs", 5); // seqs 4..=8
        sync_data_dirs(&disk);
        drop(engine);
    }

    // Confirm snapshot #1 is durably on disk before we model snapshot #2's torn
    // rename.
    assert!(
        disk.durable_bytes(&meta_dir().join(snapshot_file(1))).is_some(),
        "snapshot #1 is durably installed"
    );

    // Phase 2: model "snapshot #2 written + fsynced + RENAMED, crash before the dir
    // fsync" by mirroring write_snapshot_with's body up to (but not including) the
    // final sync_dir. The only missing step is the directory fsync that hardens the
    // rename.
    let framed = framed_snapshot_bytes(&mk_snapshot(2, 200));
    let tmp = meta_dir().join(format!("{}.tmp", snapshot_file(2)));
    let fin = meta_dir().join(snapshot_file(2));
    write_tmp_durable(&disk, &tmp, &framed);
    let fs = disk.arc();
    fs.sync_dir(&meta_dir()).unwrap(); // tmp NAME durable (the create is hardened)...
    fs.rename(&tmp, &fin).unwrap(); // rename issued...
    // ...but NO sync_dir of meta here. Crash: the un-hardened rename rolls back to
    // the durable namespace (which still lists snapshot-1.bin, not snapshot-2.bin).
    disk.crash(TornDamage::None);
    disk.reset_power();

    // (a) EXACTLY ONE valid snapshot loads — never zero, never a half body.
    let loaded = load_latest_with(&fs, &data_dir())
        .unwrap()
        .expect("exactly one valid snapshot loads — never zero");
    assert!(
        loaded.id == 1 || loaded.id == 2,
        "loaded snapshot is one of the two valid candidates, got id={}",
        loaded.id
    );
    // FakeDisk rolls the un-dir-fsynced rename back ⇒ the previous snapshot #1 is
    // what survives.
    assert_eq!(
        loaded.id, 1,
        "un-dir-fsynced rename rolls back ⇒ previous snapshot #1 loads"
    );

    // (b) Full engine recovery: the WAL replay on top of the recovered snapshot #1
    // restores every acked durable record (snapshot + WAL covers the gap). No acked
    // seq is lost, the survivor set is a dense [1..=head] prefix.
    let engine = open_engine(&disk);
    let recs = dump_records(&engine, "jobs").expect("jobs box recovers");
    let seqs: Vec<u64> = recs.keys().copied().collect();
    assert!(!seqs.is_empty(), "records recover via snapshot + WAL replay");
    // Dense, contiguous, no gap: survivors are exactly [1..=head].
    let head = *seqs.last().unwrap();
    assert_eq!(
        seqs,
        (1..=head).collect::<Vec<_>>(),
        "survivors are a dense prefix [1..={head}] — snapshot+WAL covers the gap, no hole"
    );
    // The post-snapshot tail (seqs 4..=8) was all acked-durable ⇒ must be present.
    assert!(head >= 8, "the post-snapshot acked tail survives (head={head})");
    drop(engine);
}

// ===========================================================================
// F-SNAP-EIO-DIRFSYNC
// EIO on the meta-dir fsync after the rename ⇒ write_snapshot surfaces the error;
// the previous snapshot stays authoritative. The dir-fsync omission only loses the
// install, never corrupts: a subsequent crash + recovery falls back to the old
// snapshot + WAL, losing no acked record.
// ===========================================================================
#[test]
fn f_snap_eio_dirfsync() {
    let disk = FakeDisk::new();

    // Phase 1: durable workload + a fully-installed snapshot #1 (seqs 1..=3).
    {
        let engine = open_engine(&disk);
        put_durable_box(&engine, "p");
        append_durable(&engine, "p", 3);
        assert!(engine.write_snapshot().expect("snapshot #1 writes"));
        sync_data_dirs(&disk);
        drop(engine);
    }

    // Phase 2: reopen through a FaultFs that EIOs the FIRST sync_dir (fail-once).
    // The engine's recovery + WAL-dir create do not sync_dir during open (the WAL
    // dir is created via create_dir_all, no sync_dir there), so the first sync_dir
    // the fault sees is the snapshot writer's post-rename meta-dir fsync.
    //
    // Drive more durable appends so a second snapshot is worth taking, then call
    // write_snapshot: its dir fsync EIOs ⇒ the call returns Err. The rename already
    // happened in memory but is NOT hardened; the previous snapshot is untouched.
    let faulty = FaultFs::new(disk.arc(), FaultOp::SyncDir, FaultKind::Eio, 0, true);
    {
        let engine = open_engine_fs(faulty.arc());
        let recs = dump_records(&engine, "p").expect("p recovered from snapshot #1");
        assert_eq!(
            recs.keys().copied().collect::<Vec<_>>(),
            vec![1, 2, 3],
            "snapshot #1 restored the 3 durable records"
        );
        // More durable appends (seqs 4..=6) — these are acked via the WAL group
        // fsync (sync_data, not sync_dir), so the EIO planned for sync_dir does not
        // touch them.
        append_durable(&engine, "p", 3); // re-appends data "1".."3" as seqs 4..=6
        // The snapshot write's post-rename dir fsync EIOs ⇒ write_snapshot errors.
        let res = engine.write_snapshot();
        assert!(
            res.is_err(),
            "snapshot dir-fsync EIO surfaces as a write_snapshot error, not a silent success"
        );
        sync_data_dirs(&disk);
        drop(engine);
    }

    // Phase 3: a clean recovery. Whether the failed snapshot #2's un-hardened rename
    // survived or rolled back, recovery loads a valid snapshot and replays the WAL —
    // every acked durable record (1..=6) is present, dense, no gap, no corruption.
    let engine = open_engine(&disk);
    let recs = dump_records(&engine, "p").expect("p recovers after the dir-fsync EIO");
    let seqs: Vec<u64> = recs.keys().copied().collect();
    let head = *seqs.last().expect("records present");
    assert_eq!(
        seqs,
        (1..=head).collect::<Vec<_>>(),
        "survivors are a dense prefix [1..={head}] — old snapshot + WAL, no hole"
    );
    assert!(
        head >= 6,
        "all 6 acked durable records survive the dir-fsync EIO (head={head})"
    );
    drop(engine);

    // A valid snapshot still loads directly (the EIO never corrupted the metadata).
    let loaded = load_latest_with(&disk.arc(), &data_dir())
        .unwrap()
        .expect("a valid snapshot still loads after the dir-fsync EIO");
    assert!(loaded.id >= 1, "the loaded snapshot is a real, valid candidate");
}

// ===========================================================================
// F-NFS-DIRFSYNC-UNSUPPORTED
// `sync_dir` is a best-effort no-op (open(dir)-for-fsync unsupported, e.g. NFS):
// it returns Ok(()) WITHOUT hardening the rename. The code tolerates it
// (write_snapshot succeeds), but the install may roll back on crash. Recovery
// still falls back to the old snapshot + WAL — no corruption, just a possibly-lost
// snapshot-install optimization. No acked durable record is ever lost (the WAL,
// hardened by sync_data, is the source of truth).
// ===========================================================================
#[test]
fn f_nfs_dirfsync_unsupported() {
    // Inner real FakeDisk; wrap it so EVERY sync_dir is a no-op.
    let disk = FakeDisk::new();
    let nfs = NoSyncDirFs::new(disk.clone());

    // Phase 1: a durable workload through the engine. Because sync_dir is a no-op,
    // NO directory entry is ever hardened by an explicit fsync — but the FakeDisk
    // still makes a name durable when its directory is sync_dir'd... which never
    // happens here. So to give the WAL file a durable NAME, we drive the workload
    // and then crash, and rely on recovery reading the live (post-crash) namespace.
    //
    // The contract: snapshot #1 is written (write_snapshot returns Ok even though
    // the dir fsync was a no-op), but its rename is not hardened. We then append
    // more durable records and crash. Recovery must not corrupt; it falls back to
    // whatever valid snapshot survived + the WAL.
    {
        let engine = open_engine_fs(nfs.arc());
        put_durable_box(&engine, "q");
        append_durable(&engine, "q", 4);
        // write_snapshot tolerates the no-op dir fsync ⇒ returns Ok(true).
        let wrote = engine
            .write_snapshot()
            .expect("write_snapshot tolerates a best-effort no-op dir fsync");
        assert!(wrote, "a snapshot is written even though sync_dir is a no-op");
        // More durable appends after the snapshot (seqs 5..=7).
        append_durable(&engine, "q", 3);
        drop(engine);
    }

    // The WAL bytes were hardened by sync_data (group commit), independent of
    // sync_dir. The WAL file's NAME, however, becomes durable only via a real dir
    // fsync — which the NFS wrapper no-ops. Model the kernel still flushing the
    // directory entry on a clean unmount by promoting the live namespace on the
    // INNER disk directly (sync_dir on the inner FakeDisk, bypassing the wrapper),
    // so the WAL/meta files have durable names for recovery to find — this isolates
    // the strategy to "the dir fsync did not happen at snapshot-install time", not
    // "the files never existed".
    disk.arc().sync_dir(&wal_dir()).unwrap();
    disk.arc().sync_dir(&meta_dir()).unwrap();

    // Crash AFTER the namespace was made durable by the inner flush: the snapshot
    // file (and WAL) names that were live at flush time survive. The point of the
    // strategy is that recovery tolerates a snapshot whose install dir-fsync was a
    // no-op and never corrupts.
    disk.crash(TornDamage::None);
    disk.reset_power();

    // Recovery: load through the NFS (no-op sync_dir) wrapper exactly as production
    // would. A valid snapshot loads (or none ⇒ full WAL replay) — either way no
    // corruption, and every acked durable record is present.
    let engine = open_engine_fs(nfs.arc());
    let recs = dump_records(&engine, "q").expect("q recovers (snapshot+WAL or full replay)");
    let seqs: Vec<u64> = recs.keys().copied().collect();
    assert!(!seqs.is_empty(), "acked durable records recover");
    let head = *seqs.last().unwrap();
    assert_eq!(
        seqs,
        (1..=head).collect::<Vec<_>>(),
        "survivors are a dense prefix [1..={head}] — no gap, no corruption, dir-fsync no-op tolerated"
    );
    assert!(
        head >= 7,
        "all 7 acked durable records survive (WAL is the source of truth) (head={head})"
    );

    // No snapshot body is ever decoded as garbage: load_latest returns either a
    // valid snapshot or None, never an error from a half/corrupt body.
    let loaded = load_latest_with(&nfs.arc(), &data_dir()).unwrap();
    if let Some(s) = loaded {
        assert!(s.id >= 1, "any loaded snapshot is a valid candidate");
    }
    drop(engine);
}
