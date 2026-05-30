//! Phase-8B fault catalog — **snapshot-temp boundary** strategies (file A).
//!
//! Five fault strategies from `docs/FAULT_TESTING.md` / the catalog, all probing
//! the snapshot `.tmp` write path: an EIO / ENOSPC / fsync-EIO writing the tmp
//! body, a torn tmp body, and bit-rot in the snapshot body at rest. Each drives a
//! real, fully-wired [`Engine`] whose WAL + snapshots live on a [`FakeDisk`]
//! (optionally wrapped in a [`FaultFs`]), injects the fault at the snapshot-temp
//! boundary, then asserts the oracle: the snapshot install fails or is skipped, the
//! WAL stays authoritative, and recovery reconstructs the full acked state with no
//! loss / no gap / no fabrication.
//!
//! Strategies implemented here:
//!   - `F-SNAP-EIO-TMP-WRITE`   — EIO writing the snapshot `.tmp` body
//!   - `F-SNAP-ENOSPC-TMP`      — ENOSPC writing the `.tmp`
//!   - `F-SNAP-EIO-TMP-FSYNC`   — EIO on `sync_all` of the `.tmp` before rename
//!   - `F-SNAP-TORN-BODY`       — `.tmp` body torn mid-body then installed
//!   - `F-SNAP-CRC-CORRUPT`     — bit-rot (XXH3 mismatch) in the snapshot body
//!
//! ```text
//! cargo test --features test-fs --test fault_snap_temp_a -- --test-threads=1
//! ```

#![cfg(feature = "test-fs")]

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use serde_json::json;

use streams::clock::{SharedClock, TestClock};
use streams::config::ServerConfig;
use streams::engine::Engine;
use streams::storage::testfs::FakeDisk;
use streams::storage::{File, Fs, OpenOpts};
use streams::types::{BoxConfig, BoxType, RecordIn, WriteRequest};

// ===========================================================================
// TmpFaulter — inject an io::Error on the FIRST write_at (or sync_all) to a
// snapshot `.tmp` file, deterministically and independent of FS-call counting.
//
// Why path-targeted, not a global FaultFs index: the WAL group-commit writer
// issues a *non-deterministic* number of `write_at` calls (batching depends on
// timing), so a pre-probed global index will not reliably land on the snapshot's
// tmp write. The snapshot body, however, is the only `write_at` to a `*.tmp`
// path, so matching on the path is exact and stable.
// ===========================================================================

/// Which snapshot-tmp op to fault.
#[derive(Clone, Copy)]
enum TmpFault {
    /// Fail the first `write_at` to the tmp body.
    Write(io::ErrorKind, &'static str),
    /// Fail the first `sync_all` of the tmp.
    Fsync,
}

/// Wraps a [`FakeDisk`], firing a one-shot fault on the first matching op against
/// a path whose name ends in `.tmp`.
#[derive(Clone)]
struct TmpFaulter {
    disk: FakeDisk,
    fault: TmpFault,
    fired: Arc<AtomicBool>,
}

impl TmpFaulter {
    fn new(disk: FakeDisk, fault: TmpFault) -> Self {
        TmpFaulter {
            disk,
            fault,
            fired: Arc::new(AtomicBool::new(false)),
        }
    }
    fn arc(&self) -> Arc<dyn Fs> {
        Arc::new(self.clone())
    }
}

fn is_tmp(path: &Path) -> bool {
    path.extension().and_then(|e| e.to_str()) == Some("tmp")
}

struct TmpFaultFile {
    inner: Box<dyn File>,
    owner: TmpFaulter,
    is_tmp: bool,
}

impl File for TmpFaultFile {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read_at(offset, buf)
    }
    fn write_at(&mut self, offset: u64, buf: &[u8]) -> io::Result<usize> {
        if self.is_tmp {
            if let TmpFault::Write(kind, msg) = self.owner.fault {
                if self
                    .owner
                    .fired
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    return Err(io::Error::new(kind, msg));
                }
            }
        }
        self.inner.write_at(offset, buf)
    }
    fn set_len(&mut self, len: u64) -> io::Result<()> {
        self.inner.set_len(len)
    }
    fn sync_data(&self) -> io::Result<()> {
        self.inner.sync_data()
    }
    fn sync_all(&self) -> io::Result<()> {
        if self.is_tmp {
            if let TmpFault::Fsync = self.owner.fault {
                if self
                    .owner
                    .fired
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    return Err(io::Error::other("injected EIO on tmp sync_all"));
                }
            }
        }
        self.inner.sync_all()
    }
    fn metadata_len(&self) -> io::Result<u64> {
        self.inner.metadata_len()
    }
    fn read_to_end_from(&self, offset: u64, out: &mut Vec<u8>) -> io::Result<()> {
        self.inner.read_to_end_from(offset, out)
    }
}

impl Fs for TmpFaulter {
    fn open(&self, path: &Path, opts: OpenOpts) -> io::Result<Box<dyn File>> {
        let is_tmp = is_tmp(path);
        let inner = self.disk.open(path, opts)?;
        Ok(Box::new(TmpFaultFile {
            inner,
            owner: self.clone(),
            is_tmp,
        }))
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.disk.rename(from, to)
    }
    fn remove_file(&self, path: &Path) -> io::Result<()> {
        self.disk.remove_file(path)
    }
    fn read_dir(&self, dir: &Path) -> io::Result<Vec<PathBuf>> {
        self.disk.read_dir(dir)
    }
    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        self.disk.sync_dir(dir)
    }
    fn create_dir_all(&self, dir: &Path) -> io::Result<()> {
        self.disk.create_dir_all(dir)
    }
    fn exists(&self, path: &Path) -> bool {
        self.disk.exists(path)
    }
    fn metadata_len(&self, path: &Path) -> io::Result<u64> {
        self.disk.metadata_len(path)
    }
}

// ===========================================================================
// Reference model (minimal — the must-survive acked set per box)
// ===========================================================================

#[derive(Debug, Clone, PartialEq, Eq)]
struct ModelRecord {
    data: String,
    tag: Option<String>,
    node: Option<String>,
}

#[derive(Debug, Default)]
struct ModelBox {
    acked: BTreeMap<u64, ModelRecord>,
    head: u64,
}

#[derive(Debug, Default)]
struct RefModel {
    boxes: BTreeMap<String, ModelBox>,
}

impl RefModel {
    fn ack_append(&mut self, name: &str, seq: u64, rec: ModelRecord) {
        let b = self.boxes.entry(name.to_string()).or_default();
        b.acked.insert(seq, rec);
        b.head = b.head.max(seq);
    }
}

// ===========================================================================
// Engine plumbing through FakeDisk (mirrors tests/crash_oracle.rs)
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

fn open_engine_fs(fs: Arc<dyn Fs>) -> Arc<Engine> {
    Engine::with_data_dir_fs(cfg(), clock(), fs).expect("engine opens through fs")
}

fn open_engine(disk: &FakeDisk) -> Arc<Engine> {
    open_engine_fs(disk.arc())
}

/// Create a durable box.
fn put_durable_box(engine: &Engine, name: &str) {
    let c = BoxConfig {
        r#type: BoxType::Log,
        durable: true,
        ..Default::default()
    };
    engine.put_box(name, c).expect("put_box");
}

/// Append one durable record, blocking on its group fsync (acked ⇒ durable),
/// mirroring the seq into the model.
fn append(engine: &Engine, model: &mut RefModel, name: &str, data: &str, tag: Option<&str>) {
    let req = WriteRequest {
        records: vec![RecordIn {
            data: json!({ "v": data }),
            tag: tag.map(|s| s.to_string()),
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
    let seqs = resp.seqs.clone().unwrap_or_else(|| vec![resp.last_seq]);
    for s in seqs {
        model.ack_append(
            name,
            s,
            ModelRecord {
                data: data.to_string(),
                tag: tag.map(|s| s.to_string()),
                node: None,
            },
        );
    }
}

/// Make the WAL + meta directory entries durable (production does this via the
/// dir fsync at WAL open / snapshot install; model it explicitly so file names
/// survive a crash-free reopen).
fn sync_dirs(disk: &FakeDisk) {
    let fs = disk.arc();
    let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("wal"));
    let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("meta"));
}

/// Read back the recovered live records of `name` through the public diff path.
fn dump_records(engine: &Engine, name: &str) -> BTreeMap<u64, ModelRecord> {
    use streams::types::DiffRequest;
    let mut records = BTreeMap::new();
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
            .expect("diff");
        for r in &d.records {
            let data = r
                .data
                .get("v")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| r.data.to_string());
            records.insert(
                r.seq,
                ModelRecord {
                    data,
                    tag: r.tag.clone(),
                    node: r.node.clone(),
                },
            );
        }
        if d.caught_up || d.records.is_empty() {
            break;
        }
        from = d.next_from_seq;
    }
    records
}

/// Assert the recovered box equals the model's acked set exactly (every durable
/// acked record present byte-for-byte, head matches, nothing fabricated).
fn assert_full_recovery(engine: &Engine, model: &RefModel, name: &str) {
    let mbox = &model.boxes[name];
    let recovered = dump_records(engine, name);
    let model_live: BTreeMap<u64, ModelRecord> = mbox.acked.clone();
    assert_eq!(
        recovered, model_live,
        "{name}: recovered live set must equal the model's acked set"
    );
    let st = engine.box_state(name, false).expect("box_state");
    assert_eq!(st.head_seq, mbox.head, "{name}: head_seq must match model");
}

// ===========================================================================
// Snapshot-path call indexing
// ===========================================================================

/// The canonical durable workload all five tests share: one durable box, four
/// acked appends. Mirrored into `model`.
fn build_workload(engine: &Engine, model: &mut RefModel, name: &str) {
    put_durable_box(engine, name);
    append(engine, model, name, "a", Some("t1"));
    append(engine, model, name, "b", Some("t2"));
    append(engine, model, name, "c", None);
    append(engine, model, name, "d", Some("t2"));
}

// ===========================================================================
// F-SNAP-EIO-TMP-WRITE — EIO writing the snapshot .tmp body
// ===========================================================================

/// EIO on `write_at` of the snapshot `.tmp` body: `write_snapshot` returns an
/// error, no rename happens, the previous snapshot (none here) is untouched, and
/// recovery reconstructs the full acked state from the WAL. The failed snapshot
/// leaves no half-installed checkpoint.
#[test]
fn f_snap_eio_tmp_write() {
    let disk = FakeDisk::new();
    // Fail the first write_at to the snapshot `.tmp` body with EIO (one-shot).
    let fs = TmpFaulter::new(
        disk.clone(),
        TmpFault::Write(io::ErrorKind::Other, "injected EIO on tmp write"),
    );
    let mut model = RefModel::default();
    {
        let engine = open_engine_fs(fs.arc());
        build_workload(&engine, &mut model, "s");
        // The snapshot's tmp body write hits the armed EIO ⇒ Err, no install.
        let res = engine.write_snapshot();
        assert!(
            res.is_err(),
            "F-SNAP-EIO-TMP-WRITE: snapshot must fail when the tmp write EIOs"
        );
        // Make WAL durable, then cleanly stop (no crash — the WAL is authoritative).
        sync_dirs(&disk);
        drop(engine);
    }

    // No snapshot was installed; recovery replays the WAL to the full acked set.
    assert!(
        load_latest_id(&disk).is_none(),
        "F-SNAP-EIO-TMP-WRITE: no snapshot-<n>.bin installed after a failed tmp write"
    );
    let engine = open_engine(&disk);
    assert_full_recovery(&engine, &model, "s");
}

// ===========================================================================
// F-SNAP-ENOSPC-TMP — ENOSPC writing the .tmp
// ===========================================================================

/// ENOSPC (disk full) writing the snapshot `.tmp`: the snapshot fails, the old
/// snapshot (none) is untouched, the WAL stays authoritative, and the engine
/// continues without a new checkpoint — no corruption. Recovery yields the full
/// acked set.
#[test]
fn f_snap_enospc_tmp() {
    let disk = FakeDisk::new();
    // Fail the first write_at to the snapshot `.tmp` body with ENOSPC.
    let fs = TmpFaulter::new(
        disk.clone(),
        TmpFault::Write(io::ErrorKind::StorageFull, "injected ENOSPC on tmp write"),
    );
    let mut model = RefModel::default();
    {
        let engine = open_engine_fs(fs.arc());
        build_workload(&engine, &mut model, "s");
        let res = engine.write_snapshot();
        assert!(
            res.is_err(),
            "F-SNAP-ENOSPC-TMP: snapshot must fail on ENOSPC writing the tmp"
        );
        sync_dirs(&disk);
        drop(engine);
    }

    assert!(
        load_latest_id(&disk).is_none(),
        "F-SNAP-ENOSPC-TMP: no snapshot installed when the tmp write hits ENOSPC"
    );
    let engine = open_engine(&disk);
    assert_full_recovery(&engine, &model, "s");
}

// ===========================================================================
// F-SNAP-EIO-TMP-FSYNC — EIO on sync_all of the .tmp before rename
// ===========================================================================

/// EIO on `sync_all` of the snapshot `.tmp` (before the rename): the snapshot
/// fails, no rename happens (so `load_latest` — which only reads
/// `snapshot-<n>.bin` — finds nothing), the previous snapshot loads (none here ⇒
/// WAL replay), and a retry is idempotent. Recovery reconstructs the full state.
#[test]
fn f_snap_eio_tmp_fsync() {
    let disk = FakeDisk::new();
    // Fail-once EIO on the snapshot tmp's sync_all (before the rename).
    let fs = TmpFaulter::new(disk.clone(), TmpFault::Fsync);
    let mut model = RefModel::default();
    {
        let engine = open_engine_fs(fs.arc());
        build_workload(&engine, &mut model, "s");
        let res = engine.write_snapshot();
        assert!(
            res.is_err(),
            "F-SNAP-EIO-TMP-FSYNC: snapshot must fail when the tmp fsync EIOs"
        );
        // A stray `.tmp` may exist but is ignored by load_latest; no final snapshot.
        assert!(
            load_latest_id(&disk).is_none(),
            "F-SNAP-EIO-TMP-FSYNC: no snapshot-<n>.bin (rename never reached)"
        );
        sync_dirs(&disk);
        drop(engine);
    }

    // Recovery (fault cleared) reconstructs the full acked set from the WAL.
    let engine = open_engine(&disk);
    assert_full_recovery(&engine, &model, "s");

    // Idempotent retry: a fresh engine on the (now healthy) disk can snapshot, and
    // a subsequent recovery still yields the full state.
    {
        let mut m2 = RefModel::default();
        // Re-derive the model for the surviving box from the recovered records.
        let recovered = dump_records(&engine, "s");
        for (seq, rec) in recovered {
            m2.ack_append("s", seq, rec);
        }
        engine.write_snapshot().expect("retry snapshot now succeeds");
        sync_dirs(&disk);
        drop(engine);
        assert!(
            load_latest_id(&disk).is_some(),
            "F-SNAP-EIO-TMP-FSYNC: retry installs a snapshot"
        );
        let engine2 = open_engine(&disk);
        assert_full_recovery(&engine2, &m2, "s");
    }
}

// ===========================================================================
// F-SNAP-TORN-BODY — snapshot body torn mid-body then installed
// ===========================================================================

/// A snapshot whose body is torn (the durable file truncated mid-body) but whose
/// rename nonetheless installed: `read_snapshot_file` detects the body overrun ⇒
/// `Framing` error ⇒ the snapshot is skipped; `load_latest` falls back to the
/// previous valid snapshot (or full WAL replay). Recovery reconstructs the full
/// acked set, never decoding the half-written body as state.
#[test]
fn f_snap_torn_body() {
    let disk = FakeDisk::new();
    let mut model = RefModel::default();
    {
        let engine = open_engine(&disk);
        build_workload(&engine, &mut model, "s");
        // Install a fully-durable snapshot (write → fsync → rename → dir-fsync).
        engine.write_snapshot().expect("clean snapshot installs");
        sync_dirs(&disk);
        drop(engine);
    }

    // The snapshot installed; tear its durable body (truncate mid-body), modelling
    // a tmp body that was torn before the rename hardened it.
    let snap = latest_snapshot_path(&disk).expect("a snapshot file exists");
    let durable = disk
        .durable_bytes(&snap)
        .expect("snapshot file is durable");
    assert!(durable.len() > 20, "snapshot has a header + body");
    // Keep the 20-byte header + a sliver of body, dropping the rest (body overrun).
    let torn_len = 20 + (durable.len() - 20) / 2;
    truncate_durable(&disk, &snap, torn_len as u64);

    // load_latest must skip the torn snapshot (no older valid one ⇒ full WAL
    // replay) and recovery reconstructs the full acked set.
    let engine = open_engine(&disk);
    assert_full_recovery(&engine, &model, "s");
}

// ===========================================================================
// F-SNAP-CRC-CORRUPT — bit-rot (XXH3 mismatch) in the snapshot body
// ===========================================================================

/// Bit-rot at rest in the snapshot body (a single byte flipped in the CRC'd
/// region): the XXH3 check fails ⇒ `Framing` error ⇒ the snapshot is skipped;
/// recovery falls back to the WAL (or an older valid snapshot), never decoding a
/// corrupt body as state. The full acked set is reconstructed.
#[test]
fn f_snap_crc_corrupt() {
    let disk = FakeDisk::new();
    let mut model = RefModel::default();
    {
        let engine = open_engine(&disk);
        build_workload(&engine, &mut model, "s");
        engine.write_snapshot().expect("clean snapshot installs");
        sync_dirs(&disk);
        drop(engine);
    }

    // Flip one byte deep in the durable snapshot body (well past the 20-byte
    // header, inside the XXH3-covered region) ⇒ CRC mismatch on load.
    let snap = latest_snapshot_path(&disk).expect("a snapshot file exists");
    let durable = disk.durable_bytes(&snap).expect("durable snapshot");
    assert!(durable.len() > 24, "snapshot body present");
    let flip = durable.len() - 1; // last body byte (CRC'd).
    garble_durable_byte(&disk, &snap, flip as u64);

    // The corrupt snapshot is skipped; recovery reconstructs from the WAL.
    let engine = open_engine(&disk);
    assert_full_recovery(&engine, &model, "s");
}

// ===========================================================================
// FakeDisk durable-image helpers (read/poke the post-fsync bytes)
// ===========================================================================

/// The latest `snapshot-<n>.bin` path under `<data_dir>/meta`, if any.
fn latest_snapshot_path(disk: &FakeDisk) -> Option<PathBuf> {
    let fs = disk.arc();
    let dir = PathBuf::from(DATA_DIR).join("meta");
    let mut snaps: Vec<PathBuf> = fs
        .read_dir(&dir)
        .unwrap_or_default()
        .into_iter()
        .filter(|p| {
            p.file_name()
                .and_then(|s| s.to_str())
                .map(|n| n.starts_with("snapshot-") && n.ends_with(".bin"))
                .unwrap_or(false)
        })
        .collect();
    snaps.sort();
    snaps.pop()
}

/// The id of the latest installed snapshot file, or None.
fn load_latest_id(disk: &FakeDisk) -> Option<u64> {
    let p = latest_snapshot_path(disk)?;
    let name = p.file_name()?.to_str()?;
    name.strip_prefix("snapshot-")
        .and_then(|s| s.strip_suffix(".bin"))
        .and_then(|s| s.parse::<u64>().ok())
}

/// Truncate a file's DURABLE image to `len` bytes and re-fsync, so the truncation
/// itself is durable (models a torn body that reached the platter short).
fn truncate_durable(disk: &FakeDisk, path: &std::path::Path, len: u64) {
    let fs = disk.arc();
    let mut f = fs.open(path, OpenOpts::rw_existing()).expect("open snapshot");
    f.set_len(len).expect("set_len");
    f.sync_all().expect("sync truncation durable");
}

/// Flip one byte of a file's DURABLE image at `off` and re-fsync (bit-rot at
/// rest). Reads the current durable bytes, XORs the target byte, writes it back.
fn garble_durable_byte(disk: &FakeDisk, path: &std::path::Path, off: u64) {
    let fs = disk.arc();
    let current = disk.durable_bytes(path).expect("durable bytes");
    let mut b = current[off as usize];
    b ^= 0xFF;
    let mut f = fs.open(path, OpenOpts::rw_existing()).expect("open snapshot");
    let mut written = 0;
    let buf = [b];
    while written < buf.len() {
        let n = f.write_at(off + written as u64, &buf[written..]).expect("write");
        written += n.max(1);
    }
    f.sync_all().expect("sync garble durable");
}
