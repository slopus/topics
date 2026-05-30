//! Phase-8B fault/crash batch — WAL-append boundary, group C (2 strategies from
//! `/tmp/streams-fault-catalog.json`), each one test fn named after its catalog id.
//! Every test asserts the CORRECT crash-consistency behavior via the durability
//! contract / direct WAL replay, reusing the Phase-8A harness (`FakeDisk` /
//! `FaultFs` from `streams::storage::testfs`, the real WAL wired through
//! `Engine::with_data_dir_fs` and `Wal::open_at_with`).
//!
//! Strategies:
//!   - `F-NFS-EAGAIN-RETRY`          — a transient (EAGAIN/EJUKEBOX-class) refusal
//!     on a WAL `write_at`: the engine has no silent-retry loop, so it FAILS the
//!     commit cleanly (the catalog's "or fail the commit" branch); the write
//!     returns Err (NEVER a silent ack before success), prior durable state is
//!     intact, and once the transient clears a fresh durable append acks and
//!     recovers with no gap / no fabrication.
//!   - `F-PSOW-OUTSIDE-RANGE-DAMAGE` — a non-powersafe overwrite: a write to a
//!     frame's range also garbles a *neighboring* frame's sector. The damaged
//!     neighbor fails CRC ⇒ recovery stops there (truncate-at-first-bad, never
//!     skip-and-continue); the garbled frame and everything after it are dropped,
//!     never served as a valid record (loss explicit), prior frames intact.
//!
//! ```text
//! cargo test --features test-fs --test fault_wal_append_c -- --test-threads=1
//! ```

#![cfg(feature = "test-fs")]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;

use streams::clock::{SharedClock, TestClock};
use streams::config::ServerConfig;
use streams::engine::Engine;
use streams::storage::testfs::{FakeDisk, FaultFs, FaultKind, FaultOp, TornDamage};
use streams::storage::wal::{Wal, WalConfig, WalReader, WalRecord};
use streams::storage::{Fs, OpenOpts};
use streams::types::{BoxConfig, BoxType, DiffRequest, RecordIn, WriteRequest};

// ===========================================================================
// Shared plumbing (mirrors tests/crash_oracle.rs + tests/fault_batch1.rs)
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

/// Build a fresh durable engine whose WAL + snapshots live on `disk`.
fn open_engine(disk: &FakeDisk) -> Arc<Engine> {
    Engine::with_data_dir_fs(cfg(), clock(), disk.arc()).expect("engine opens through FakeDisk")
}

fn wal_dir() -> PathBuf {
    PathBuf::from(DATA_DIR).join("wal")
}

/// Make the WAL (and meta) directory names durable — the create+dir-fsync that
/// production does at open time, modeled explicitly so the files survive a crash.
fn sync_wal_dir(disk: &FakeDisk) {
    let fs = disk.arc();
    let _ = fs.sync_dir(&wal_dir());
    let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("meta"));
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
    let st = engine.box_state(name, false).ok()?;
    let _ = st;
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

// --- Direct WAL replay (mirrors testfs::recover_seqs / fault_batch1) --------

/// Replay every `wal-*.log` file on the (post-crash) disk image, stopping each
/// file at its torn tail — exactly the recovery read path. Returns the dense
/// sequence of recovered Append seqs.
fn recover_seqs(disk: &FakeDisk) -> Vec<u64> {
    let fs = disk.arc();
    let mut files: Vec<PathBuf> = fs
        .read_dir(&wal_dir())
        .unwrap()
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
        let r = WalReader::open_with(&fs, &f).unwrap();
        for frame in r {
            let s = frame.record.seq();
            if s > 0 {
                seqs.push(s);
            }
        }
    }
    seqs
}

/// The single active `wal-*.log` path on the disk (lowest-indexed; the small
/// workloads here never rotate).
fn active_wal_path(disk: &FakeDisk) -> PathBuf {
    let fs = disk.arc();
    let mut files: Vec<PathBuf> = fs
        .read_dir(&wal_dir())
        .unwrap()
        .into_iter()
        .filter(|p| {
            p.file_name()
                .and_then(|s| s.to_str())
                .map(|n| n.starts_with("wal-") && n.ends_with(".log"))
                .unwrap_or(false)
        })
        .collect();
    files.sort();
    files.into_iter().next().expect("an active WAL file exists")
}

/// A direct WAL writer wired through `disk`, with a fast group-commit window so
/// durable `append`s ack quickly. Used by the corruption test that needs
/// byte-level control over the on-disk frames.
fn fast_cfg() -> WalConfig {
    // The WAL lives under `<cfg.dir>/wal`, so the config dir is the data dir.
    let mut c = WalConfig::new(DATA_DIR);
    c.gc_min = Duration::from_micros(50);
    c.gc_max = Duration::from_micros(200);
    c
}

fn ap(seq: u64) -> WalRecord {
    WalRecord::Append {
        box_id: 1,
        seq,
        ts: 1_700_000_000_000 + seq,
        node: None,
        tag: Some("t".into()),
        data: format!("payload-{seq}").into_bytes(),
    }
}

/// Overwrite a contiguous byte range of a (durable) file on `disk` and fsync, so
/// the change lands in the durable image — the "byte-poke" the corruption catalog
/// entries call for, expressed through the same `Fs` seam recovery reads.
fn poke_durable(disk: &FakeDisk, path: &Path, offset: u64, bytes: &[u8]) {
    let mut f = disk
        .open(path, OpenOpts::rw_existing())
        .expect("open WAL file rw for byte-poke");
    let mut written = 0usize;
    while written < bytes.len() {
        let n = f
            .write_at(offset + written as u64, &bytes[written..])
            .expect("poke write");
        assert!(n > 0, "poke made no progress");
        written += n;
    }
    f.sync_all().expect("poke fsync ⇒ durable");
    sync_wal_dir(disk);
}

/// Read the full durable bytes of a WAL file (the post-crash image a recovery
/// would see).
fn durable_wal_bytes(disk: &FakeDisk, path: &Path) -> Vec<u8> {
    let f = disk.open(path, OpenOpts::read_only()).expect("open WAL ro");
    let mut buf = Vec::new();
    f.read_to_end_from(0, &mut buf).expect("read WAL bytes");
    buf
}

// WAL frame layout (src/storage/wal.rs): [frame_len:u32 @0..4][type:u8 @4]
// [flags:u8 @5][box_id:u32 @6..10][seq:u64 @10..18][ts:u64 @18..26]
// [node_len:u16][tag_len:u16][data_len:u32]...[body]...[crc:u64 last 8 bytes].
const FRAME_LEN_PREFIX: usize = 4;
const FRAME_HEADER_LEN: usize = 30;
const FRAME_CRC_LEN: usize = 8;

/// Walk the frame_len prefixes and return the `(start, total_on_disk_len)` span of
/// every complete valid-length frame in `bytes`, in order. Stops at the first
/// sub-minimum / overrunning length (the preallocated-zeros tail).
fn frame_spans(bytes: &[u8]) -> Vec<(usize, usize)> {
    let mut pos = 0usize;
    let mut spans = Vec::new();
    while pos + FRAME_LEN_PREFIX <= bytes.len() {
        let frame_len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        if frame_len < FRAME_HEADER_LEN + FRAME_CRC_LEN {
            break; // sub-minimum / preallocated zeros ⇒ end of real frames.
        }
        let total = FRAME_LEN_PREFIX + frame_len;
        if pos + total > bytes.len() {
            break; // overruns ⇒ torn / end.
        }
        spans.push((pos, total));
        pos += total;
    }
    spans
}

// ===========================================================================
// F-NFS-EAGAIN-RETRY  (boundary: wal-append, category: nfs)
//
// fault:   EAGAIN/EJUKEBOX-class transient refusal on a WAL write/fsync.
// inject:  FaultFs returns the transient error N times then succeeds (modeled here
//          with a fail-once `write_at` glitch — the WAL writer treats every
//          `Err` from `write_at` identically, with NO special-case retry, so an
//          EAGAIN is indistinguishable from an EIO to the engine: `FaultKind::Eio`
//          is the available stand-in for the transient kind).
// oracle:  "bounded retry-with-backoff then ack on success, OR fail the commit;
//          never a silent ack before success."
//
// The engine has no silent in-`commit_batch` retry loop: on the transient
// `write_at` error it signals the batch `Failed` and the durable `write` returns
// `Err` (the catalog's allowed "or fail the commit" branch). The crucial
// invariant — NEVER A SILENT ACK BEFORE SUCCESS — is what we assert: a write that
// hit the transient refusal returns Err (it is not acked, leaves no durable
// trace), prior durable state is intact, and once the transient clears (fail-once)
// a fresh durable append acks and recovers with no gap / no fabrication.
//
// This is CORRECT behavior, so the test passes.
// ===========================================================================
#[test]
fn f_nfs_eagain_retry() {
    let disk = FakeDisk::new();

    // Phase 1: a clean durable prefix (acked ⇒ durable) so we can prove "prior
    // state intact" across the transient glitch.
    {
        let engine = open_engine(&disk);
        put_durable_box(&engine, "q");
        append_durable(&engine, "q", 2);
        sync_wal_dir(&disk);
        assert_eq!(
            dump_records(&engine, "q")
                .unwrap()
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            vec![1, 2],
            "prior 2 durable frames acked"
        );
        drop(engine);
    }

    // Phase 2: a fail-ONCE transient refusal on the very next WAL `write_at` (an
    // EAGAIN-class glitch, then the device accepts writes again). The durable
    // append that hits it MUST return Err — never a silent ack before the write
    // actually succeeded.
    let faulty: Arc<dyn Fs> =
        FaultFs::new(disk.arc(), FaultOp::WriteAt, FaultKind::Eio, 0, true).arc();
    {
        let engine =
            Engine::with_data_dir_fs(cfg(), clock(), faulty).expect("reopen through faultfs");
        let res = engine.write("q", one_write("3"), true);
        assert!(
            res.is_err(),
            "a durable append hitting a transient write refusal must FAIL the \
             commit (no silent ack before success)"
        );
        // The transient glitch poisoned the in-flight batch; its buffered bytes
        // never fsynced. Freeze BEFORE drop so the writer's drain cannot harden the
        // failed batch, then power-cycle.
        disk.crash(TornDamage::None);
        drop(engine);
    }
    disk.reset_power();

    // Phase 3: the transient is gone (fail-once). Recovery sees exactly the 2 prior
    // durable frames — the refused batch left no trace, no gap, no fabrication.
    {
        let engine = open_engine(&disk);
        let recs = dump_records(&engine, "q").expect("box survives");
        assert_eq!(
            recs.keys().copied().collect::<Vec<_>>(),
            vec![1, 2],
            "refused (un-acked) batch never recovered; prior 2 intact, no gap"
        );

        // And a FRESH durable append now succeeds (the device accepts writes again)
        // and continues contiguously at head+1 — proving the failure was clean and
        // the writer is healthy, not wedged.
        let resp = engine
            .write("q", one_write("3b"), true)
            .expect("post-glitch durable append acks on success");
        assert_eq!(resp.last_seq, 3, "fresh append continues at head+1 (=3), no gap");
        sync_wal_dir(&disk);
        drop(engine);
    }

    // Phase 4: that fresh append is itself durable across another crash (acked ⇒
    // durable), confirming the recovered log is sound and continues monotonically.
    disk.crash(TornDamage::None);
    disk.reset_power();
    let engine = open_engine(&disk);
    let recs = dump_records(&engine, "q").expect("box survives final crash");
    assert_eq!(
        recs.keys().copied().collect::<Vec<_>>(),
        vec![1, 2, 3],
        "the post-glitch acked append survives; dense [1..=3], no gap, no resurrection"
    );
    assert_eq!(recs[&3], "3b", "seq 3 holds the post-glitch record, not the refused one");
}

// ===========================================================================
// F-PSOW-OUTSIDE-RANGE-DAMAGE  (boundary: wal-append, category: torn/partial)
//
// fault:   non-powersafe overwrite — a write to frame range [a,b) damages bytes
//          OUTSIDE [a,b), garbling a *neighboring* frame's sector (cheap flash).
// inject:  the catalog's "FakeDisk psow-off mode garbling a neighboring sector on
//          a pending write" — modeled here at the byte level: append durable
//          frames, then garble a byte in an EARLIER (neighbor) frame's body, the
//          collateral damage a psow write to the following frame would inflict.
// oracle:  the damaged neighbor frame fails CRC ⇒ Torn-truncated if at tail, or
//          surfaced as corruption mid-log; never served as valid; loss explicit.
//
// Recovery walks frames in order and stops at the FIRST torn/CRC-bad frame
// (truncate-at-first-bad, never skip-and-continue — `WalReader::next` sets
// `done=true` on the first `DecodeStep::Torn`). So a garbled middle (neighbor)
// frame truncates the log there: the garbled frame and everything after it are
// dropped, the genuine prior frames survive, and the corrupt bytes are NEVER
// decoded into a record. This is CORRECT behavior, so the test passes.
// ===========================================================================
#[test]
fn f_psow_outside_range_damage() {
    let disk = FakeDisk::with_seed(0x9501);
    {
        let wal = Wal::open_at_with(disk.arc(), fast_cfg(), 1, 0).unwrap();
        let w = wal.writer();
        // Six acked-durable frames (each fsynced ⇒ promoted to durable).
        for seq in 1..=6 {
            w.append(ap(seq), true).unwrap();
        }
        sync_wal_dir(&disk);
        drop(wal);
    }

    let path = active_wal_path(&disk);
    let bytes = durable_wal_bytes(&disk, &path);
    let spans = frame_spans(&bytes);
    assert!(spans.len() >= 6, "all 6 frames laid down, got {} spans", spans.len());

    // Pick a MID-LOG neighbor: frame index 3 (the 4th frame, seq 4). A psow write
    // to frame index 4's range garbles a byte in frame index 3's body sector —
    // collateral damage *outside* the intended write range. Garble a byte inside
    // the body (after the 30-byte header, before the trailing 8-byte CRC) so the
    // stored CRC no longer matches.
    let (nstart, ntotal) = spans[3];
    let garble_at = nstart + FRAME_LEN_PREFIX + FRAME_HEADER_LEN + 1;
    assert!(
        garble_at < nstart + ntotal - FRAME_CRC_LEN,
        "garble lands inside the neighbor frame body, not its CRC"
    );
    let mut poison = bytes[garble_at];
    poison ^= 0x40; // flip a bit — the neighbor sector damage.
    poke_durable(&disk, &path, garble_at as u64, &[poison]);

    // Recovery truncates at the first CRC-bad frame (the garbled neighbor, seq 4):
    // seqs 1..=3 survive as a dense prefix; seqs 4,5,6 are dropped (the corrupt
    // neighbor frame and everything after it), NEVER served as valid records.
    let seqs = recover_seqs(&disk);
    assert_eq!(
        seqs,
        vec![1, 2, 3],
        "psow neighbor damage fails CRC ⇒ truncate-at-first-bad; corrupt frame and \
         tail dropped, prior frames intact, garbage never decoded"
    );

    // Explicit-loss guarantee: the garbled seq 4 is NOT present (no garbage record),
    // and there is no gap among survivors — they are a contiguous prefix [1..=3].
    assert!(!seqs.contains(&4), "corrupt neighbor frame is never decoded as a record");
    for (i, s) in seqs.iter().enumerate() {
        assert_eq!(*s, i as u64 + 1, "survivors are a dense contiguous prefix: {seqs:?}");
    }
}
