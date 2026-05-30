//! Phase-8B fault/crash catalog — boundary **wal-append**, batch A (5 strategies
//! from `/tmp/streams-fault-catalog.json` / `docs/FAULT_TESTING.md`). Each test fn
//! is named after its catalog id and asserts the CORRECT crash-consistency
//! behavior through the Phase-8A harness: the real WAL wired through `FakeDisk` /
//! `FaultFs` (`streams::storage::testfs`), recovered via the engine + the direct
//! `WalReader` replay path, diffed against the durability contract
//! (acked ⇒ durable, no silent loss / no gap, torn tail truncated never misread).
//!
//! Strategies:
//!   - F-WAL-EIO-WRITE       — EIO on the batch `write_at` ⇒ batch Failed, nothing
//!                             acked, recovery shows none of it, no gap.
//!   - F-WAL-ENOSPC-WRITE    — ENOSPC (dead device) on `write_at` ⇒ batch fails
//!                             cleanly, prior durable state intact + readable.
//!   - F-WAL-SHORT-WRITE     — `write_at` returns a short count ⇒ the writer loops
//!                             to full length (correct branch); the frame is whole,
//!                             acked, and recovered — never a half-frame record.
//!   - F-WAL-TORN-MID-PAYLOAD— last pending frame torn inside its body ⇒ CRC/len
//!                             mismatch ⇒ Torn ⇒ truncate at the prior frame.
//!   - F-WAL-TORN-MID-HEADER — last frame torn inside the 30-byte fixed header
//!                             (off in [4,34)) ⇒ Torn ⇒ truncate; header garbage is
//!                             never read back as a record.
//!
//! ```text
//! cargo test --features test-fs --test fault_wal_append_a -- --test-threads=1
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
use streams::types::{BoxConfig, BoxType, RecordIn, WriteRequest};

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

/// Make the WAL (and meta) directory names durable — the create+dir-fsync
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
    use streams::types::DiffRequest;
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

// --- Direct WAL replay (mirrors testfs::recover_seqs) ----------------------

fn wal_files(disk: &FakeDisk) -> Vec<PathBuf> {
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
    files
}

/// Replay every `wal-*.log` file on the (post-crash) disk image, stopping each
/// file at its torn tail — exactly the recovery read path. Returns the dense
/// sequence of recovered Append seqs (with their decoded data payloads).
fn recover_frames(disk: &FakeDisk) -> Vec<(u64, Vec<u8>)> {
    let fs = disk.arc();
    let mut out = Vec::new();
    for f in wal_files(disk) {
        let r = WalReader::open_with(&fs, &f).unwrap();
        for frame in r {
            if let WalRecord::Append { seq, data, .. } = &frame.record {
                if *seq > 0 {
                    out.push((*seq, data.clone()));
                }
            }
        }
    }
    out
}

/// Just the recovered Append seqs (dense prefix oracle).
fn recover_seqs(disk: &FakeDisk) -> Vec<u64> {
    recover_frames(disk).into_iter().map(|(s, _)| s).collect()
}

/// The single active `wal-*.log` path (lowest-indexed; these small workloads
/// never rotate).
fn active_wal_path(disk: &FakeDisk) -> PathBuf {
    wal_files(disk)
        .into_iter()
        .next()
        .expect("an active WAL file exists")
}

/// Read the full durable bytes of a WAL file (the post-crash image a recovery
/// would see).
fn durable_wal_bytes(disk: &FakeDisk, path: &Path) -> Vec<u8> {
    let f = disk.open(path, OpenOpts::read_only()).expect("open WAL ro");
    let mut buf = Vec::new();
    f.read_to_end_from(0, &mut buf).expect("read WAL bytes");
    buf
}

/// A direct WAL writer wired through `disk`, with a fast group-commit window so
/// durable `append`s ack quickly.
fn fast_cfg() -> WalConfig {
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

// WAL frame layout (src/storage/wal.rs):
//   [frame_len:u32 @0..4][type:u8 @4][flags:u8 @5][box_id:u32 @6..10]
//   [seq:u64 @10..18][ts:u64 @18..26][node_len:u16][tag_len:u16][data_len:u32]
//   ... body ... [crc:u64 last 8 bytes].
// FRAME_LEN_PREFIX = 4, FRAME_HEADER_LEN = 30, FRAME_CRC_LEN = 8.
const FRAME_LEN_PREFIX: usize = 4;
const FRAME_HEADER_LEN: usize = 30;
const FRAME_CRC_LEN: usize = 8;

/// The byte offset of the start of the **last** complete valid frame in `bytes`,
/// and that frame's total on-disk length, by walking frame_len prefixes. `None`
/// if there is no complete frame.
fn last_frame_span(bytes: &[u8]) -> Option<(usize, usize)> {
    let mut pos = 0usize;
    let mut last: Option<(usize, usize)> = None;
    while pos + FRAME_LEN_PREFIX <= bytes.len() {
        let frame_len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        if frame_len < FRAME_HEADER_LEN + FRAME_CRC_LEN {
            break; // sub-minimum / preallocated zeros ⇒ end of real frames.
        }
        let total = FRAME_LEN_PREFIX + frame_len;
        if pos + total > bytes.len() {
            break; // overruns ⇒ torn / end.
        }
        last = Some((pos, total));
        pos += total;
    }
    last
}

/// Durably truncate the active WAL file to `new_len` (a torn-write tear that
/// reached the platter as a strict prefix). Routes through the same `Fs` seam
/// recovery reads, fsyncing so it lands in the durable image.
fn truncate_durable(disk: &FakeDisk, path: &Path, new_len: u64) {
    let mut f = disk
        .open(path, OpenOpts::rw_existing())
        .expect("open WAL rw for truncate");
    f.set_len(new_len).expect("set_len");
    f.sync_all().expect("truncate fsync ⇒ durable");
    sync_wal_dir(disk);
}

// ===========================================================================
// F-WAL-EIO-WRITE  (io-error, severity critical)
// EIO on write_at of the batch bytes (write_all_at_tail). The WAL writer's
// commit_batch signals the whole batch Failed (callers observe WriterGone), so
// the durable append returns Err and is NOT acked / NOT published. Recovery
// shows none of the failed batch (not acked ⇒ not committed), prior durable
// frames intact, no gap.
// ===========================================================================
#[test]
fn f_wal_eio_write() {
    let disk = FakeDisk::new();

    // Phase 1: a clean durable prefix (3 acked frames) so "prior intact" is real.
    {
        let engine = open_engine(&disk);
        put_durable_box(&engine, "p");
        append_durable(&engine, "p", 3);
        sync_wal_dir(&disk);
        assert_eq!(
            dump_records(&engine, "p").unwrap().len(),
            3,
            "3 durable frames acked"
        );
        drop(engine);
    }

    // Phase 2: fail-once EIO on the very next write_at — that is the batch write
    // of the 4th frame. write_all_at_tail returns Err ⇒ commit_batch signals
    // Failed ⇒ the durable append returns Err (WriterGone), never acked.
    let faulty: Arc<dyn Fs> =
        FaultFs::new(disk.arc(), FaultOp::WriteAt, FaultKind::Eio, 0, true).arc();
    {
        let engine = Engine::with_data_dir_fs(cfg(), clock(), faulty).expect("reopen via faultfs");
        let res = engine.write("p", one_write("4"), true);
        assert!(res.is_err(), "durable append must fail when its batch write EIOs");
        // Power loss: the EIO'd batch never wrote any pending bytes (the write_at
        // itself errored), so nothing to drop beyond the already-clean image.
        disk.crash(TornDamage::None);
        drop(engine);
    }
    disk.reset_power();

    // Recovery: exactly the 3 prior durable frames; the EIO'd batch left no trace.
    let engine = open_engine(&disk);
    let recs = dump_records(&engine, "p").expect("box survives recovery");
    assert_eq!(
        recs.keys().copied().collect::<Vec<_>>(),
        vec![1, 2, 3],
        "EIO-on-write batch never acked ⇒ never recovered; prior 3 intact, no gap"
    );
    // The on-disk WAL replay agrees (dense prefix, no fabricated frame).
    assert_eq!(recover_seqs(&disk), vec![1, 2, 3], "WAL replay is a dense prefix");
}

// ===========================================================================
// F-WAL-ENOSPC-WRITE  (resource-exhaustion, severity critical)
// ENOSPC on write_at (disk full mid-batch), fail-ALWAYS (dead device). The batch
// fails cleanly (no ack), prior durable state is intact and STILL READABLE; the
// engine refuses the write rather than corrupting anything.
// ===========================================================================
#[test]
fn f_wal_enospc_write() {
    let disk = FakeDisk::new();

    // Phase 1: a clean durable prefix.
    {
        let engine = open_engine(&disk);
        put_durable_box(&engine, "q");
        append_durable(&engine, "q", 2);
        sync_wal_dir(&disk);
        drop(engine);
    }

    // Phase 2: a dead device — every write_at returns ENOSPC from now on.
    let faulty: Arc<dyn Fs> =
        FaultFs::new(disk.arc(), FaultOp::WriteAt, FaultKind::Enospc, 0, false).arc();
    {
        let engine = Engine::with_data_dir_fs(cfg(), clock(), faulty).expect("reopen via faultfs");

        // The prior durable state must STILL BE READABLE even though the device is
        // full for writes (reads don't touch write_at).
        let before = dump_records(&engine, "q").expect("prior box readable on a full disk");
        assert_eq!(
            before.keys().copied().collect::<Vec<_>>(),
            vec![1, 2],
            "prior durable state intact + readable under ENOSPC"
        );

        // Every durable append now fails cleanly (ENOSPC on the batch write).
        for v in ["3", "4"] {
            let res = engine.write("q", one_write(v), true);
            assert!(res.is_err(), "append on a full disk must fail, never corrupt");
        }
        disk.crash(TornDamage::None);
        drop(engine);
    }
    disk.reset_power();

    // Recovery: only the 2 acked-durable frames; the refused writes left no trace.
    let engine = open_engine(&disk);
    let recs = dump_records(&engine, "q").expect("box survives recovery");
    assert_eq!(
        recs.keys().copied().collect::<Vec<_>>(),
        vec![1, 2],
        "ENOSPC writes never acked ⇒ absent after recovery; prior 2 intact, no gap"
    );
    assert_eq!(recover_seqs(&disk), vec![1, 2]);
}

// ===========================================================================
// F-WAL-SHORT-WRITE  (torn/partial, severity high)
// write_at returns fewer bytes than requested (len/2) for the batch write. The
// WAL's write_all_at_tail LOOPS over the short write to full length (the correct
// branch), so the frame lands WHOLE, the durable append is acked, and recovery
// replays the complete frame — never a half-frame materialized as a record.
// ===========================================================================
#[test]
fn f_wal_short_write() {
    let disk = FakeDisk::new();

    // Phase 1: a clean durable prefix.
    {
        let engine = open_engine(&disk);
        put_durable_box(&engine, "s");
        append_durable(&engine, "s", 2);
        sync_wal_dir(&disk);
        drop(engine);
    }

    // Phase 2: a fail-once short write on the next batch write_at. The writer's
    // loop must finish the frame; the append acks normally.
    let faulty: Arc<dyn Fs> =
        FaultFs::new(disk.arc(), FaultOp::WriteAt, FaultKind::ShortWrite, 0, true).arc();
    {
        let engine = Engine::with_data_dir_fs(cfg(), clock(), faulty).expect("reopen via faultfs");
        let res = engine.write("s", one_write("3"), true);
        assert!(
            res.is_ok(),
            "the writer loops over the short write to full length ⇒ append acks"
        );
        sync_wal_dir(&disk);
        drop(engine);
    }

    // Recovery: all 3 frames present and WHOLE — the short-written frame is not a
    // half-frame. Read it back through the engine AND the raw WAL to confirm the
    // payload is the complete, correct record.
    let engine = open_engine(&disk);
    let recs = dump_records(&engine, "s").expect("box survives recovery");
    assert_eq!(
        recs.keys().copied().collect::<Vec<_>>(),
        vec![1, 2, 3],
        "the short-written frame completed and recovered; no half-frame"
    );
    assert_eq!(recs[&3], "3", "the short-written frame's payload is whole + correct");

    // Raw WAL replay: frame 3 decodes fully (CRC valid ⇒ never a torn misread).
    // The body is the engine's own JSON encoding of the record; the load-bearing
    // assertion is that the frame is COMPLETE (it appears at all, with a non-empty
    // body, and its CRC validated so the reader yielded it instead of stopping at a
    // torn tail) — the short-write loop landed every byte.
    let frames = recover_frames(&disk);
    assert_eq!(
        frames.iter().map(|(s, _)| *s).collect::<Vec<_>>(),
        vec![1, 2, 3],
        "raw WAL replay yields the complete frame 3 (looped to full length)"
    );
    let (_, data3) = frames.iter().find(|(s, _)| *s == 3).expect("frame 3 on disk");
    assert!(
        !data3.is_empty() && data3.windows(3).any(|w| w == b"\"3\""),
        "frame 3 body is whole + carries the record's data (no truncation): {data3:?}"
    );
}

// ===========================================================================
// F-WAL-TORN-MID-PAYLOAD  (torn/partial, severity critical)
// The last frame persisted as a prefix ending INSIDE the data+meta payload (a
// torn write that reached the platter only partway through the body). frame_len
// then overruns / CRC mismatches ⇒ DecodeStep::Torn ⇒ recovery truncates at the
// prior frame. No partial record is ever materialized.
// ===========================================================================
#[test]
fn f_wal_torn_mid_payload() {
    let disk = FakeDisk::new();
    {
        let wal = Wal::open_at_with(disk.arc(), fast_cfg(), 1, 0).unwrap();
        let w = wal.writer();
        for seq in 1..=5 {
            w.append(ap(seq), true).unwrap(); // durable ⇒ fsynced ⇒ promoted.
        }
        sync_wal_dir(&disk);
        drop(wal);
    }

    // Locate the last (5th) durable frame and tear it mid-PAYLOAD: truncate to an
    // offset strictly inside the body region (after the 30-byte header, before the
    // 8-byte CRC). The frame_len prefix is left intact, so on replay frame_len
    // claims more bytes than survive ⇒ overrun/CRC ⇒ Torn.
    let path = active_wal_path(&disk);
    let bytes = durable_wal_bytes(&disk, &path);
    let (start, total) = last_frame_span(&bytes).expect("a last frame exists");
    let body_start = start + FRAME_LEN_PREFIX + FRAME_HEADER_LEN;
    let crc_start = start + total - FRAME_CRC_LEN;
    assert!(
        crc_start > body_start + 1,
        "the frame has a payload region to tear into"
    );
    let tear_at = body_start + 1; // one byte into the payload.
    truncate_durable(&disk, &path, tear_at as u64);

    let seqs = recover_seqs(&disk);
    // The torn 5th frame truncates; the 4 fully-durable frames survive, dense.
    assert_eq!(
        seqs,
        vec![1, 2, 3, 4],
        "mid-payload torn frame ⇒ Torn ⇒ truncate at prior frame; no partial record"
    );
}

// ===========================================================================
// F-WAL-TORN-MID-HEADER  (torn/partial, severity critical)
// The last frame is torn INSIDE the 30-byte fixed header (after frame_len, before
// the body) — torn truncate to an off in [4,34). frame_len then overruns the
// surviving bytes / the internal length fields are inconsistent ⇒ Torn ⇒ truncate.
// Garbage header fields are never read back as a record.
// ===========================================================================
#[test]
fn f_wal_torn_mid_header() {
    let disk = FakeDisk::new();
    {
        let wal = Wal::open_at_with(disk.arc(), fast_cfg(), 1, 0).unwrap();
        let w = wal.writer();
        for seq in 1..=5 {
            w.append(ap(seq), true).unwrap();
        }
        sync_wal_dir(&disk);
        drop(wal);
    }

    let path = active_wal_path(&disk);
    let bytes = durable_wal_bytes(&disk, &path);
    let (start, _total) = last_frame_span(&bytes).expect("a last frame exists");

    // Tear at every offset INSIDE the header band [4,34) relative to the frame
    // start: the 4-byte frame_len survives whole, but the header is cut short, so
    // frame_len claims a full frame's worth of bytes that no longer exist ⇒ overrun
    // ⇒ Torn. The recovered prefix must be exactly 1..=4 at every tear point, and
    // never surface a 5th (garbage-header) record.
    for off in FRAME_LEN_PREFIX..(FRAME_LEN_PREFIX + FRAME_HEADER_LEN) {
        let disk = disk.clone(); // share the durable image; truncate is per-iteration.
        let tear_at = (start + off) as u64;
        truncate_durable(&disk, &path, tear_at);

        let seqs = recover_seqs(&disk);
        assert_eq!(
            seqs,
            vec![1, 2, 3, 4],
            "mid-header tear at off {off} ⇒ Torn ⇒ truncate; no garbage-header record"
        );

        // Restore the full image for the next tear offset by rewriting the original
        // bytes durably (the truncate above shrank the file).
        let mut f = disk
            .open(&path, OpenOpts::rw_existing())
            .expect("reopen WAL to restore");
        let mut written = 0usize;
        while written < bytes.len() {
            let n = f
                .write_at(written as u64, &bytes[written..])
                .expect("restore write");
            assert!(n > 0, "restore made progress");
            written += n;
        }
        f.sync_all().expect("restore fsync");
        sync_wal_dir(&disk);
    }
}
