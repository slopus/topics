//! Phase-8B fault catalog — **snapshot-temp** boundary, batch B.
//!
//! Five fault strategies that attack the snapshot framing/decoder
//! (`read_snapshot_file` → `load_latest_with` in `src/storage/snapshot.rs`).
//! Every one corrupts a *durable* snapshot file (or feeds the decoder arbitrary
//! garbage) and asserts the documented contract:
//!
//! > a torn/corrupt snapshot is **detected** (`SnapshotError::Framing`), **skipped**,
//! > and `load_latest` falls back to an older valid snapshot or to a full WAL
//! > replay — never decoded as state, never an OOB read, never a panic.
//!
//! Strategies (see `docs/FAULT_TESTING.md` / `/tmp/topics-fault-catalog.json`):
//!
//! - `F-SNAP-BAD-MAGIC` — magic bytes `0..4` corrupted ⇒ Framing ⇒ skipped.
//! - `F-SNAP-VERSION-MISMATCH` — version field `4..8` bumped to an unsupported
//!   value ⇒ Framing ⇒ skipped; recovery uses the older snapshot / WAL.
//! - `F-SNAP-BODYLEN-OVERFLOW` — body_len `8..12` poked huge (`u32::MAX`) so
//!   `body_start + body_len` overruns the file ⇒ Framing; no OOB, no panic.
//! - `F-SNAP-TRUNCATED-HEADER` — file truncated below the 20-byte header ⇒
//!   "file shorter than header" Framing ⇒ skipped; recovery falls back.
//! - `F-FUZZ-SNAPSHOT-DECODER` — arbitrary garbage fed to the framing+body
//!   decode: never panics/OOMs/OOB; always a clean `Ok(_)`/`Err(Framing/Decode)`.
//!
//! The harness is the Phase-8A `FakeDisk` (durable bytes survive; all I/O routes
//! through the injected `Arc<dyn Fs>`) plus the real `write_snapshot_with` /
//! `load_latest_with` snapshot path and a full `Engine::with_data_dir_fs`
//! recovery to prove the fallback end-to-end. Everything is bounded (tiny
//! snapshots, a capped fuzz corpus, fixed seeds) so the file runs in well under
//! a minute.
//!
//! ```text
//! cargo test --features test-fs --test fault_snap_temp_b
//! ```

#![cfg(feature = "test-fs")]
#![allow(
    clippy::ptr_arg,
    clippy::manual_clamp,
    clippy::unusual_byte_groupings,
    clippy::doc_lazy_continuation
)]

use std::path::PathBuf;
use std::sync::Arc;

use serde_json::json;

use topics::clock::{SharedClock, TestClock};
use topics::config::ServerConfig;
use topics::engine::Engine;
use topics::storage::snapshot::{
    load_latest_with, write_snapshot_with, Checkpoint, Snapshot, SnapshotRecord, SnapshotTopic,
};
use topics::storage::testfs::FakeDisk;
use topics::storage::{Fs, OpenOpts};
use topics::types::{RecordIn, TopicConfig, TopicType, WriteRequest};

// ---------------------------------------------------------------------------
// On-disk snapshot framing layout (mirrors src/storage/snapshot.rs; the
// constants there are private so the offsets are duplicated here as the format
// contract these fault tests poke at).
// ---------------------------------------------------------------------------

/// `magic(4) + version(4) + body_len(4) + crc(8)`.
const HEADER_LEN: usize = 20;
const MAGIC: [u8; 4] = *b"SNP1";
/// The current snapshot version the decoder accepts (any other ⇒ Framing).
const VERSION: u32 = 3;
const OFF_MAGIC: usize = 0;
const OFF_VERSION: usize = 4;
const OFF_BODYLEN: usize = 8;

const DATA_DIR: &str = "/data";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn data_dir() -> PathBuf {
    PathBuf::from(DATA_DIR)
}

fn meta_dir() -> PathBuf {
    data_dir().join("meta")
}

fn snapshot_path(id: u64) -> PathBuf {
    meta_dir().join(format!("snapshot-{id:016}.bin"))
}

/// A small but non-empty snapshot (a single topic with two live records) so the
/// body is a real postcard payload, not an empty vec — corruption then has a
/// meaningful body to overrun/mis-decode.
fn sample(id: u64, last_seq: u64) -> Snapshot {
    Snapshot {
        id,
        ts: 1_700_000_000_000,
        next_topic_id: 2,
        checkpoint: Checkpoint {
            wal_idx: 1,
            wal_offset: 0,
            last_checkpoint_seq: last_seq,
            shards: vec![(1, 0)],
            shard_keys: vec![String::new()],
        },
        topics: vec![SnapshotTopic {
            name: "jobs".into(),
            topic_id: 1,
            epoch: 1,
            config_json: b"{\"durable\":true}".to_vec(),
            base_seq: 1,
            head_seq: 2,
            evict_floor: 0,
            expiry_floor: 0,
            delete_floor: 0,
            delete_below: 0,
            bytes_retained: 20,
            live_count: 2,
            records: vec![
                SnapshotRecord {
                    seq: 1,
                    ts: 100,
                    node: Some("n".into()),
                    tag: Some("t".into()),
                    data_json: b"{\"v\":\"a\"}".to_vec(),
                    meta_json: None,
                    bytes: 10,
                },
                SnapshotRecord {
                    seq: 2,
                    ts: 101,
                    node: None,
                    tag: None,
                    data_json: b"{\"v\":\"b\"}".to_vec(),
                    meta_json: None,
                    bytes: 10,
                },
            ],
            source_trim_floor: 0,
        }],
        routers: vec![],
    }
}

/// Read a file's full durable bytes back through the injected `Fs` (the same
/// read path recovery uses).
fn read_file(fs: &Arc<dyn Fs>, path: &PathBuf) -> Vec<u8> {
    let f = fs.open(path, OpenOpts::read_only()).expect("open snapshot");
    let mut buf = Vec::new();
    f.read_to_end_from(0, &mut buf).expect("read snapshot");
    buf
}

/// Replace a file's durable contents with exactly `bytes` (open without
/// truncating to keep the inode, overwrite, set_len to the new length, then
/// fsync so the corruption is durable for the subsequent load). Goes through the
/// `Fs` seam so it works on any injected disk.
fn rewrite_file(fs: &Arc<dyn Fs>, path: &PathBuf, bytes: &[u8]) {
    let mut f = fs
        .open(path, OpenOpts::rw_existing())
        .expect("reopen snapshot rw");
    let mut off = 0usize;
    while off < bytes.len() {
        let n = f.write_at(off as u64, &bytes[off..]).expect("poke write");
        assert!(n > 0, "write made no progress");
        off += n;
    }
    f.set_len(bytes.len() as u64).expect("set_len");
    f.sync_all().expect("sync poked snapshot");
}

/// Write a snapshot file containing exactly `bytes` (arbitrary content — used by
/// the fuzz target to hand the decoder garbage that never went through
/// `write_snapshot_with`). Creates `meta/`, the file, and dir-fsyncs so the name
/// is durable.
fn install_raw_snapshot(fs: &Arc<dyn Fs>, id: u64, bytes: &[u8]) {
    fs.create_dir_all(&meta_dir()).unwrap();
    let path = snapshot_path(id);
    {
        let mut f = fs.open(&path, OpenOpts::create_truncate()).unwrap();
        let mut off = 0usize;
        while off < bytes.len() {
            let n = f.write_at(off as u64, &bytes[off..]).unwrap();
            if n == 0 {
                break;
            }
            off += n;
        }
        f.sync_all().unwrap();
    }
    fs.sync_dir(&meta_dir()).unwrap();
}

fn cfg() -> ServerConfig {
    ServerConfig {
        data_dir: Some(DATA_DIR.to_string()),
        ..Default::default()
    }
}

fn clock() -> SharedClock {
    Arc::new(TestClock::new(1_700_000_000_000))
}

// ===========================================================================
// F-SNAP-BAD-MAGIC — magic bytes corrupted ⇒ Framing ⇒ skipped, never decoded.
// ===========================================================================
//
// Oracle: bad magic ⇒ Framing error ⇒ skipped. We corrupt bytes 0..4 of the only
// snapshot and assert (a) `load_latest_with` returns `Ok(None)` (no valid
// snapshot remains), and (b) with a *valid older* snapshot present it falls back
// to that older one rather than ever decoding the bad-magic newer file.
#[test]
fn f_snap_bad_magic() {
    // --- Single snapshot: corrupt magic ⇒ no valid snapshot loads. ---
    {
        let disk = FakeDisk::new();
        let fs = disk.arc();
        write_snapshot_with(&fs, &data_dir(), &sample(1, 100)).unwrap();

        let mut bytes = read_file(&fs, &snapshot_path(1));
        // Flip the magic so it can never equal SNP1.
        for i in 0..4 {
            bytes[OFF_MAGIC + i] ^= 0xFF;
        }
        assert_ne!(bytes[0..4], MAGIC, "magic must be corrupted");
        rewrite_file(&fs, &snapshot_path(1), &bytes);

        let loaded = load_latest_with(&fs, &data_dir()).unwrap();
        assert!(
            loaded.is_none(),
            "bad-magic snapshot must be skipped, not decoded (got {:?})",
            loaded.map(|s| s.id)
        );
    }

    // --- Older valid + newer bad-magic: load_latest falls back to the older. ---
    {
        let disk = FakeDisk::new();
        let fs = disk.arc();
        // id=1 valid; install id=2 directly (so write_snapshot_with doesn't prune
        // id=1) by raw-copying a freshly-encoded id=2 frame then corrupting magic.
        write_snapshot_with(&fs, &data_dir(), &sample(1, 100)).unwrap();
        // Build a valid id=2 frame in memory by writing it to a scratch disk.
        let scratch = FakeDisk::new();
        let sfs = scratch.arc();
        write_snapshot_with(&sfs, &data_dir(), &sample(2, 200)).unwrap();
        let mut id2 = read_file(&sfs, &snapshot_path(2));
        // Corrupt id=2's magic, then install it alongside the valid id=1.
        id2[OFF_MAGIC] ^= 0xFF;
        install_raw_snapshot(&fs, 2, &id2);

        let loaded = load_latest_with(&fs, &data_dir())
            .unwrap()
            .expect("falls back to the valid older snapshot");
        assert_eq!(loaded.id, 1, "newest is bad-magic ⇒ load the valid id=1");
        assert_eq!(loaded.checkpoint.last_checkpoint_seq, 100);
    }
}

// ===========================================================================
// F-SNAP-VERSION-MISMATCH — version field altered to an unsupported value.
// ===========================================================================
//
// Oracle: unsupported version ⇒ Framing ⇒ skipped; recovery uses an older
// snapshot / WAL, never misparses a future-format body as the current one. We
// poke the u32 version to VERSION+1 (and also to a wildly different value) and
// assert the snapshot is skipped — AND prove an end-to-end engine recovery still
// reconstructs the durable topic from the WAL when its only snapshot is a future
// version.
#[test]
fn f_snap_version_mismatch() {
    // --- Decoder-level: a bumped version is skipped. ---
    for bad_version in [VERSION + 1, VERSION.wrapping_sub(1), 0xDEAD_BEEF] {
        let disk = FakeDisk::new();
        let fs = disk.arc();
        write_snapshot_with(&fs, &data_dir(), &sample(1, 100)).unwrap();

        let mut bytes = read_file(&fs, &snapshot_path(1));
        bytes[OFF_VERSION..OFF_VERSION + 4].copy_from_slice(&bad_version.to_le_bytes());
        rewrite_file(&fs, &snapshot_path(1), &bytes);

        let loaded = load_latest_with(&fs, &data_dir()).unwrap();
        assert!(
            loaded.is_none(),
            "version {bad_version} (≠ {VERSION}) must be skipped, not decoded"
        );
    }

    // --- End-to-end: an engine whose ONLY snapshot is a future version must fall
    //     back to full WAL replay and reconstruct every durable acked record. ---
    let disk = FakeDisk::new();
    {
        let engine = Engine::with_data_dir_fs(cfg(), clock(), disk.arc())
            .expect("engine opens through FakeDisk");
        let bcfg = TopicConfig {
            r#type: TopicType::Log,
            durable: true,
            ..Default::default()
        };
        engine.put_topic("jobs", bcfg).unwrap();
        for v in ["a", "b", "c"] {
            let req = WriteRequest {
                records: vec![RecordIn {
                    data: json!({ "v": v }),
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
            engine
                .write("jobs", req, true)
                .expect("durable append acked");
        }
        // Force a durable checkpoint so a real snapshot exists on disk.
        let wrote = engine.write_snapshot().expect("write a snapshot");
        assert!(wrote, "a snapshot must have been written");
        let fs = disk.arc();
        fs.sync_dir(&data_dir().join("wal")).unwrap();
        fs.sync_dir(&meta_dir()).unwrap();
        drop(engine);
    }

    // Corrupt the version of every snapshot now on disk.
    let fs = disk.arc();
    let mut corrupted_any = false;
    for p in fs.read_dir(&meta_dir()).unwrap() {
        let name = p.file_name().and_then(|s| s.to_str()).unwrap_or("");
        if name.starts_with("snapshot-") && name.ends_with(".bin") {
            let mut bytes = read_file(&fs, &p);
            if bytes.len() >= HEADER_LEN {
                bytes[OFF_VERSION..OFF_VERSION + 4].copy_from_slice(&(VERSION + 7).to_le_bytes());
                rewrite_file(&fs, &p, &bytes);
                corrupted_any = true;
            }
        }
    }
    assert!(corrupted_any, "a snapshot file must exist to corrupt");
    // The decoder must skip the future-version snapshot.
    assert!(
        load_latest_with(&fs, &data_dir()).unwrap().is_none(),
        "future-version snapshot skipped"
    );

    // Recovery falls back to full WAL replay and rebuilds all 3 durable records.
    let engine = Engine::with_data_dir_fs(cfg(), clock(), disk.arc())
        .expect("recovery falls back to WAL when the snapshot is a bad version");
    let st = engine
        .topic_state("jobs", false)
        .expect("jobs recovered from WAL");
    assert_eq!(st.head_seq, 3, "all 3 durable seqs recovered from the WAL");
    assert_eq!(
        st.count, 3,
        "no record lost when the snapshot was unreadable"
    );
}

// ===========================================================================
// F-SNAP-BODYLEN-OVERFLOW — body_len poked to u32::MAX (overruns the file).
// ===========================================================================
//
// Oracle: `body_start + body_len` overrun guard ⇒ Framing; no OOB read, no panic.
// `body_start (=20) + u32::MAX` does not overflow usize on a 64-bit target but
// vastly overruns the file, so the "body overruns file" guard must fire. We poke
// the body_len field to u32::MAX and to several other oversize values and assert
// every one is rejected (and never panics).
#[test]
fn f_snap_bodylen_overflow() {
    for bad_len in [u32::MAX, u32::MAX - 1, 0x7FFF_FFFF, 1_000_000] {
        let disk = FakeDisk::new();
        let fs = disk.arc();
        write_snapshot_with(&fs, &data_dir(), &sample(1, 100)).unwrap();

        let mut bytes = read_file(&fs, &snapshot_path(1));
        bytes[OFF_BODYLEN..OFF_BODYLEN + 4].copy_from_slice(&bad_len.to_le_bytes());
        rewrite_file(&fs, &snapshot_path(1), &bytes);

        // Must not panic / OOM / OOB-read — just a skip (Ok(None) here).
        let loaded = load_latest_with(&fs, &data_dir()).unwrap();
        assert!(
            loaded.is_none(),
            "body_len={bad_len} overruns the file ⇒ must be rejected, not decoded"
        );
    }

    // Also a *shrunk* body_len so body_end lands BEFORE the real body end: the
    // CRC then covers the wrong span ⇒ crc mismatch ⇒ Framing (still no panic).
    {
        let disk = FakeDisk::new();
        let fs = disk.arc();
        write_snapshot_with(&fs, &data_dir(), &sample(1, 100)).unwrap();
        let mut bytes = read_file(&fs, &snapshot_path(1));
        bytes[OFF_BODYLEN..OFF_BODYLEN + 4].copy_from_slice(&0u32.to_le_bytes());
        rewrite_file(&fs, &snapshot_path(1), &bytes);
        assert!(
            load_latest_with(&fs, &data_dir()).unwrap().is_none(),
            "a zero body_len mis-frames the body ⇒ rejected"
        );
    }
}

// ===========================================================================
// F-SNAP-TRUNCATED-HEADER — file shorter than the 20-byte header.
// ===========================================================================
//
// Oracle: "file shorter than header" Framing ⇒ skipped; recovery falls back.
// We truncate the snapshot to every length in 0..HEADER_LEN and assert each is
// rejected (no panic on the slicing of buf[0..4] / buf[4..8] etc.), then prove a
// valid older snapshot is still chosen when the newest is a stub header.
#[test]
fn f_snap_truncated_header() {
    for short_len in 0..HEADER_LEN {
        let disk = FakeDisk::new();
        let fs = disk.arc();
        write_snapshot_with(&fs, &data_dir(), &sample(1, 100)).unwrap();

        let mut bytes = read_file(&fs, &snapshot_path(1));
        assert!(
            bytes.len() > HEADER_LEN,
            "a full snapshot is longer than the header"
        );
        bytes.truncate(short_len);
        rewrite_file(&fs, &snapshot_path(1), &bytes);

        let loaded = load_latest_with(&fs, &data_dir()).unwrap();
        assert!(
            loaded.is_none(),
            "a {short_len}-byte (< {HEADER_LEN}) snapshot must be skipped, not parsed"
        );
    }

    // Older valid + newer truncated-to-stub: fall back to the valid older.
    {
        let disk = FakeDisk::new();
        let fs = disk.arc();
        write_snapshot_with(&fs, &data_dir(), &sample(1, 100)).unwrap();
        // Install a 5-byte stub as id=2 alongside the valid id=1.
        install_raw_snapshot(&fs, 2, &[0x53, 0x4E, 0x50, 0x31, 0x02]);
        let loaded = load_latest_with(&fs, &data_dir())
            .unwrap()
            .expect("falls back to the valid older snapshot");
        assert_eq!(loaded.id, 1, "truncated id=2 skipped ⇒ load valid id=1");
    }
}

// ===========================================================================
// F-FUZZ-SNAPSHOT-DECODER — arbitrary garbage fed to the framing+body decode.
// ===========================================================================
//
// Oracle: never panics/OOMs/OOB; bad magic/version/len/crc rejected as Framing;
// a postcard decode error is surfaced (not a panic). We drive the *real* decode
// path (`load_latest_with` → `read_snapshot_file`) with a large deterministic
// corpus of arbitrary byte buffers installed as snapshot files. The only
// acceptable outcomes are `Ok(None)`, `Ok(Some(_))` (if a buffer happens to be a
// valid frame), or an `Err` — never a panic, hang, or OOB.
#[test]
fn f_fuzz_snapshot_decoder() {
    // A small spl/xorshift PRNG so the corpus is deterministic and dependency-free.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            // SplitMix64.
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }
        fn byte(&mut self) -> u8 {
            (self.next() & 0xFF) as u8
        }
        fn range(&mut self, n: usize) -> usize {
            (self.next() % (n as u64)) as usize
        }
    }

    // A genuine valid frame to use as a structured seed (mutating a real frame is
    // far more likely to reach the body/postcard decoder than pure noise).
    let template = {
        let disk = FakeDisk::new();
        let fs = disk.arc();
        write_snapshot_with(&fs, &data_dir(), &sample(1, 100)).unwrap();
        read_file(&fs, &snapshot_path(1))
    };

    let mut rng = Rng(0xC0FFEE_1234_5678);
    let iterations = 400usize;

    for it in 0..iterations {
        // Build a buffer one of several ways for breadth of coverage.
        let buf: Vec<u8> = match it % 5 {
            // 0: pure random of a random small length (incl. 0 and sub-header).
            0 => {
                let len = rng.range(64);
                (0..len).map(|_| rng.byte()).collect()
            }
            // 1: a valid frame with a handful of bytes flipped (structured fuzz).
            1 => {
                let mut b = template.clone();
                let flips = 1 + rng.range(6);
                for _ in 0..flips {
                    if !b.is_empty() {
                        let i = rng.range(b.len());
                        b[i] ^= rng.byte();
                    }
                }
                b
            }
            // 2: a valid header but a body_len field set to a huge/odd value with
            //    random trailing bytes (drives the overrun + crc guards).
            2 => {
                let mut b = template.clone();
                if b.len() >= HEADER_LEN {
                    let bad = rng.next() as u32;
                    b[OFF_BODYLEN..OFF_BODYLEN + 4].copy_from_slice(&bad.to_le_bytes());
                    let extra = rng.range(48);
                    for _ in 0..extra {
                        b.push(rng.byte());
                    }
                }
                b
            }
            // 3: a valid magic+version prefix then random garbage body of random
            //    length (forces postcard to chew on adversarial bytes).
            3 => {
                let mut b = Vec::new();
                b.extend_from_slice(&MAGIC);
                b.extend_from_slice(&VERSION.to_le_bytes());
                let body_len = rng.range(40);
                b.extend_from_slice(&(body_len as u32).to_le_bytes());
                b.extend_from_slice(&rng.next().to_le_bytes()); // crc (random)
                for _ in 0..body_len {
                    b.push(rng.byte());
                }
                b
            }
            // 4: a truncated prefix of the valid frame at a random cut point.
            _ => {
                let cut = if template.is_empty() {
                    0
                } else {
                    rng.range(template.len() + 1)
                };
                template[..cut].to_vec()
            }
        };

        // Install the garbage as the sole snapshot and decode it through the real
        // path. The contract is simply: it returns (never panics / OOBs / hangs).
        let disk = FakeDisk::new();
        let fs = disk.arc();
        install_raw_snapshot(&fs, 1, &buf);
        let result = load_latest_with(&fs, &data_dir());
        // Any of Ok(None) / Ok(Some) / Err is acceptable — we only require that
        // the decoder *returned* (no panic/OOB/overflow). `result` is consumed by
        // the match so the optimizer can't drop the call.
        match result {
            Ok(_) | Err(_) => {}
        }
    }
}
