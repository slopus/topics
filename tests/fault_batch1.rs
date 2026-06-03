//! Phase-8A fault/crash batch #1 — 12 high-value fault-injection strategies from
//! the catalog (`/tmp/topics-fault-catalog.json`), each one test fn named after
//! its catalog id. Every test asserts the CORRECT crash-consistency behavior via
//! the model oracle / the durability contract, reusing the Phase-8A harness
//! (`FakeDisk` / `FaultFs` from `topics::storage::testfs`, the real WAL /
//! snapshot / segment store / recovery wired through `Engine::with_data_dir_fs`
//! and the `*_with` constructors).
//!
//! Boundaries covered: WAL append/fsync crash points, WAL fsync EIO, a
//! fsync-fail-then-crash compound, three WAL on-disk-corruption decode cases
//! (CRC flip / huge frame_len / zeroed frame), idempotent double recovery, two
//! snapshot atomic-swap crash points, a segment .data-before-.idx crash, and a
//! cold-tier copy-before-flip crash.
//!
//! ```text
//! cargo test --features test-fs --test fault_batch1 -- --test-threads=1
//! ```

#![cfg(feature = "test-fs")]
#![allow(
    clippy::ptr_arg,
    clippy::manual_clamp,
    clippy::unusual_byte_groupings,
    clippy::doc_lazy_continuation
)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;

use topics::clock::{SharedClock, TestClock};
use topics::config::ServerConfig;
use topics::engine::Engine;
use topics::storage::testfs::{FakeDisk, FaultFs, FaultKind, FaultOp, TornDamage};
use topics::storage::wal::{Wal, WalConfig, WalReader, WalRecord};
use topics::storage::{Fs, OpenOpts};
use topics::types::{RecordIn, TopicConfig, TopicType, WriteRequest};

// ===========================================================================
// Shared plumbing (mirrors tests/crash_oracle.rs and src/storage/testfs.rs)
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

/// The on-disk snapshot file name for `id` (zero-padded, matching
/// `snapshot::snapshot_name`: `snapshot-<id:016>.bin`).
fn snapshot_file(id: u64) -> String {
    format!("snapshot-{id:016}.bin")
}

/// Make the WAL (and meta) directory names durable — the create+dir-fsync
/// production does at open time, modeled explicitly so the files survive a crash.
fn sync_wal_dir(disk: &FakeDisk) {
    let fs = disk.arc();
    let _ = fs.sync_dir(&wal_dir());
    let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("meta"));
}

/// A single-record durable write request for topic `name` carrying `data`.
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

fn put_durable_topic(engine: &Engine, name: &str) {
    engine
        .put_topic(
            name,
            TopicConfig {
                r#type: TopicType::Log,
                durable: true,
                cap_records: 0,
                ..Default::default()
            },
        )
        .expect("put_topic");
}

/// Append `n` durable records "1".."n" to topic `name`, each blocking on the group
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

/// Read back the live records of topic `name` (seq → data string) through the
/// engine's diff path; `None` if the topic is absent.
fn dump_records(engine: &Engine, name: &str) -> Option<BTreeMap<u64, String>> {
    use topics::types::DiffRequest;
    let st = engine.topic_state(name, false).ok()?;
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
                    max_batch_bytes: 0,
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

/// A flat, comparable dump of one topic's recovered state (head/earliest/count +
/// records + tombstone), for the idempotent-recovery byte-identity check.
#[derive(Debug, Clone, PartialEq, Eq)]
struct FullDump {
    head: u64,
    earliest: u64,
    count: u64,
    records: BTreeMap<u64, String>,
    tombstone: Option<String>,
}

fn full_dump(engine: &Engine, name: &str) -> Option<FullDump> {
    use topics::types::DiffRequest;
    let st = engine.topic_state(name, false).ok()?;
    let mut records = BTreeMap::new();
    let mut tombstone = None;
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
                    max_batch_bytes: 0,
                },
            )
            .ok()?;
        if let Some(t) = &d.tombstone {
            tombstone = Some(format!("{:?}", t.reason).to_lowercase());
        }
        for r in &d.records {
            let v = r
                .data
                .get("v")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| r.data.to_string());
            records.insert(r.seq, v);
        }
        if d.caught_up || d.records.is_empty() {
            break;
        }
        from = d.next_from_seq;
    }
    Some(FullDump {
        head: st.head_seq,
        earliest: st.earliest_seq,
        count: st.count,
        records,
        tombstone,
    })
}

// --- Direct WAL replay (mirrors testfs::recover_seqs) ----------------------

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
/// durable `append`s ack quickly. Used by the corruption tests that need
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
        topic_id: 1,
        seq,
        ts: 1_700_000_000_000 + seq,
        node: None,
        tag: Some("t".into()),
        data: format!("payload-{seq}").into_bytes(),
    }
}

/// Overwrite a contiguous byte range of a (durable) file on `disk` and fsync, so
/// the change lands in the durable image — the "byte-poke" the corruption
/// catalog entries call for, expressed through the same `Fs` seam recovery reads.
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
// [flags:u8 @5][topic_id:u64 @6..14][seq:u64 @14..22][ts:u64 @22..30]
// [node_len:u16][tag_len:u16][data_len:u32]...[body]...[crc:u64 last 8 bytes].
const FRAME_LEN_PREFIX: usize = 4;

/// The byte offset of the start of the **last** complete valid frame in `bytes`,
/// and that frame's total on-disk length, by walking frame_len prefixes. Returns
/// `None` if there is no complete frame.
fn last_frame_span(bytes: &[u8]) -> Option<(usize, usize)> {
    let mut pos = 0usize;
    let mut last: Option<(usize, usize)> = None;
    while pos + FRAME_LEN_PREFIX <= bytes.len() {
        let frame_len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        if frame_len < 30 + 8 {
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

// ===========================================================================
// F-WAL-CRASH-AFTER-WRITE-PRE-FSYNC
// power loss after write_at returns but before sync_data ⇒ the un-fsynced batch
// bytes vanish (drop-pending); recovery yields a clean prefix; acked (prior
// fsynced) writes survive.
// ===========================================================================
#[test]
fn f_wal_crash_after_write_pre_fsync() {
    let disk = FakeDisk::new();
    let wal = Wal::open_at_with(disk.arc(), fast_cfg(), 1, 0).unwrap();
    let w = wal.writer();

    // Two acked-durable frames (their fsync returned ⇒ promoted to durable).
    w.append(ap(1), true).unwrap();
    w.append(ap(2), true).unwrap();

    // A NON-durable submit: its bytes reach the file's pending buffer (write_at
    // returned) but no group fsync follows, so they are un-promoted at crash —
    // exactly "after write_at, before sync_data".
    let _ = w.submit(ap(3), false);
    std::thread::sleep(Duration::from_millis(5));

    sync_wal_dir(&disk);
    disk.crash(TornDamage::None); // drop the un-fsynced pending bytes.
    drop(wal);

    let seqs = recover_seqs(&disk);
    // The two fsynced frames survive; the pre-fsync frame 3 is gone. Dense prefix.
    assert_eq!(
        seqs,
        vec![1, 2],
        "acked frames survive; pre-fsync batch dropped"
    );
}

// ===========================================================================
// F-WAL-CRASH-AFTER-FSYNC
// power loss immediately after sync_data returns ⇒ the acked batch is already
// durable and survives intact (acked ⇒ durable); seq monotone.
// ===========================================================================
#[test]
fn f_wal_crash_after_fsync() {
    let disk = FakeDisk::new();
    let wal = Wal::open_at_with(disk.arc(), fast_cfg(), 1, 0).unwrap();
    let w = wal.writer();

    // Each `append(.., true)` blocks until the group fsync returns ⇒ the bytes
    // are promoted to durable BEFORE the next line runs.
    for seq in 1..=5 {
        w.append(ap(seq), true).unwrap();
    }
    sync_wal_dir(&disk);

    // Power loss right after the last fsync returned: nothing is pending.
    disk.crash(TornDamage::None);
    drop(wal);

    let seqs = recover_seqs(&disk);
    assert_eq!(
        seqs,
        vec![1, 2, 3, 4, 5],
        "every acked-after-fsync frame survives"
    );
}

// ===========================================================================
// F-WAL-EIO-FSYNC
// EIO on the durable group-commit sync_data ⇒ the WHOLE batch is signalled
// Failed (no token acked); the write returns Err and a subsequent recovery
// contains no batch frame — no silent ack of an unsynced batch.
// ===========================================================================
#[test]
fn f_wal_eio_fsync() {
    let disk = FakeDisk::new();

    // Phase 1: a clean durable prefix so we can prove "prior intact".
    {
        let engine = open_engine(&disk);
        put_durable_topic(&engine, "p");
        append_durable(&engine, "p", 2);
        sync_wal_dir(&disk);
        assert!(!dump_records(&engine, "p").unwrap().is_empty());
        drop(engine);
    }

    // Phase 2: a dead-device sync_data fault. A durable append MUST return Err
    // (the group fsync EIOs) and MUST NOT be acked.
    let faulty: Arc<dyn Fs> =
        FaultFs::new(disk.arc(), FaultOp::SyncData, FaultKind::Eio, 0, false).arc();
    {
        let engine =
            Engine::with_data_dir_fs(cfg(), clock(), faulty).expect("reopen through faultfs");
        let res = engine.write("p", one_write("3"), true);
        assert!(res.is_err(), "durable append must fail when its fsync EIOs");
        // Power loss now: the un-fsynced frame-3 bytes (buffered, fsync errored)
        // never reached durable media, so they are dropped. Freeze before drop so
        // the writer's drain cannot harden them on a faulted device.
        disk.crash(TornDamage::None);
        drop(engine);
    }
    disk.reset_power();

    // Recovery: exactly the 2 prior durable frames; the EIO'd batch left no trace.
    let engine = open_engine(&disk);
    let recs = dump_records(&engine, "p").expect("topic survives");
    assert_eq!(
        recs.keys().copied().collect::<Vec<_>>(),
        vec![1, 2],
        "fsync-EIO batch never acked ⇒ never recovered; prior 2 intact"
    );
}

// ===========================================================================
// F-COMPOUND-FSYNC-FAIL-THEN-CRASH
// fsync fails for an in-flight batch, THEN a power loss before any later success
// ⇒ the failed batch leaves no trace and the prior durable state recovers
// exactly (acked⇒durable, failed⇒absent, no fabrication, no gap).
// ===========================================================================
#[test]
fn f_compound_fsync_fail_then_crash() {
    let disk = FakeDisk::new();

    // Phase 1: 3 durable acked writes on a clean disk.
    {
        let engine = open_engine(&disk);
        put_durable_topic(&engine, "c");
        append_durable(&engine, "c", 3);
        sync_wal_dir(&disk);
        drop(engine);
    }

    // Phase 2: fail-once sync_data, then it would succeed — but we crash before a
    // later fsync can land the buffered bytes. The failed batch poisons in-flight.
    let faulty = FaultFs::new(disk.arc(), FaultOp::SyncData, FaultKind::Eio, 0, true);
    {
        let engine =
            Engine::with_data_dir_fs(cfg(), clock(), faulty.arc()).expect("reopen through faultfs");
        let res = engine.write("c", one_write("4"), true);
        assert!(
            res.is_err(),
            "the fsync-failing durable append is not acked"
        );
        // Crash: the buffered frame-4 bytes were never durably fsynced (the only
        // fsync that touched them errored). They vanish.
        disk.crash(TornDamage::None);
        drop(engine);
    }
    disk.reset_power();

    let engine = open_engine(&disk);
    let recs = dump_records(&engine, "c").expect("topic survives");
    assert_eq!(
        recs.keys().copied().collect::<Vec<_>>(),
        vec![1, 2, 3],
        "failed-then-crashed batch leaves no trace; prior 3 intact, no gap"
    );
}

// ===========================================================================
// F-WAL-CRC-FLIP-TAIL
// a single-bit flip in the last durable frame's body ⇒ XXH3/CRC mismatch ⇒ that
// frame is the torn tail and truncates; all prior frames stay intact.
// ===========================================================================
#[test]
fn f_wal_crc_flip_tail() {
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
    let (start, total) = last_frame_span(&bytes).expect("a last frame exists");
    // Flip one bit in the middle of the last frame's body (after the header,
    // before the CRC) so the stored CRC no longer matches.
    let flip_at = start + FRAME_LEN_PREFIX + 30 + 1; // inside the body region.
    assert!(
        flip_at < start + total - 8,
        "flip lands inside the body, not the CRC"
    );
    let mut poison = bytes[flip_at];
    poison ^= 0x40;
    poke_durable(&disk, &path, flip_at as u64, &[poison]);

    let seqs = recover_seqs(&disk);
    // The 5th frame fails CRC and truncates; 1..=4 survive as a dense prefix.
    assert_eq!(
        seqs,
        vec![1, 2, 3, 4],
        "CRC-flipped tail frame truncates; prior intact"
    );
}

// ===========================================================================
// F-WAL-LEN-GARBAGE-HUGE
// the frame_len of the last frame is garbled to a huge value past EOF ⇒
// frame_len overruns available bytes ⇒ Torn ⇒ truncate, bounded before any body
// read (no OOM / no panic).
// ===========================================================================
#[test]
fn f_wal_len_garbage_huge() {
    let disk = FakeDisk::new();
    {
        let wal = Wal::open_at_with(disk.arc(), fast_cfg(), 1, 0).unwrap();
        let w = wal.writer();
        for seq in 1..=4 {
            w.append(ap(seq), true).unwrap();
        }
        sync_wal_dir(&disk);
        drop(wal);
    }

    let path = active_wal_path(&disk);
    let bytes = durable_wal_bytes(&disk, &path);
    let (start, _total) = last_frame_span(&bytes).expect("a last frame exists");
    // Overwrite the last frame's 4-byte frame_len prefix with 0xFFFFFFFF (a value
    // far past EOF). decode_one must treat it as Torn before reading the body.
    poke_durable(&disk, &path, start as u64, &0xFFFF_FFFFu32.to_le_bytes());

    let seqs = recover_seqs(&disk);
    // The bogus-length tail frame is rejected as Torn; the 3 prior frames survive.
    assert_eq!(
        seqs,
        vec![1, 2, 3],
        "huge frame_len overruns EOF ⇒ Torn ⇒ truncate; no OOM, prior intact"
    );
}

// ===========================================================================
// F-WAL-ZEROED-FRAME
// the last frame reads back all-zeros (a lost write / sparse hole) ⇒ frame_len==0
// < minimum ⇒ Torn ⇒ stop; the zeroed region is treated as end-of-data, not an
// empty record.
// ===========================================================================
#[test]
fn f_wal_zeroed_frame() {
    let disk = FakeDisk::new();
    {
        let wal = Wal::open_at_with(disk.arc(), fast_cfg(), 1, 0).unwrap();
        let w = wal.writer();
        for seq in 1..=4 {
            w.append(ap(seq), true).unwrap();
        }
        sync_wal_dir(&disk);
        drop(wal);
    }

    let path = active_wal_path(&disk);
    let bytes = durable_wal_bytes(&disk, &path);
    let (start, total) = last_frame_span(&bytes).expect("a last frame exists");
    // Zero out the whole last frame (a lost-write / sparse hole). Its frame_len
    // prefix becomes 0 ⇒ below minimum ⇒ Torn.
    let zeros = vec![0u8; total];
    poke_durable(&disk, &path, start as u64, &zeros);

    let seqs = recover_seqs(&disk);
    assert_eq!(
        seqs,
        vec![1, 2, 3],
        "zeroed tail frame ⇒ frame_len 0 < min ⇒ Torn ⇒ stop; prior intact"
    );
}

// ===========================================================================
// F-REC-RUN-TWICE-IDENTICAL
// no fault — recover(recover(x)) == recover(x) byte-for-byte (head / earliest /
// count / per-seq records / tombstone) — the PG idempotent-replay oracle.
// ===========================================================================
#[test]
fn f_rec_run_twice_identical() {
    use topics::types::DeleteRequest;
    let disk = FakeDisk::new();

    // A non-trivial durable workload: appends + a prefix delete + a cap topic that
    // evicts (so floors and tombstones are exercised across recovery).
    {
        let engine = open_engine(&disk);
        put_durable_topic(&engine, "b");
        append_durable(&engine, "b", 5);
        engine
            .delete(
                "b",
                DeleteRequest {
                    before_seq: Some(3),
                    match_: None,
                },
            )
            .expect("prefix delete acked");
        // A capped durable topic that overflows ⇒ involuntary evict floor + tombstone.
        engine
            .put_topic(
                "cap",
                TopicConfig {
                    r#type: TopicType::Log,
                    durable: true,
                    cap_records: 2,
                    ..Default::default()
                },
            )
            .unwrap();
        for i in 1..=5 {
            engine
                .write("cap", one_write(&format!("c{i}")), true)
                .unwrap();
        }
        sync_wal_dir(&disk);
        drop(engine);
    }

    let first_b;
    let first_cap;
    {
        let engine = open_engine(&disk);
        first_b = full_dump(&engine, "b").expect("b after recovery #1");
        first_cap = full_dump(&engine, "cap").expect("cap after recovery #1");
        drop(engine);
    }
    let second_b;
    let second_cap;
    {
        let engine = open_engine(&disk);
        second_b = full_dump(&engine, "b").expect("b after recovery #2");
        second_cap = full_dump(&engine, "cap").expect("cap after recovery #2");
        drop(engine);
    }

    assert_eq!(
        first_b, second_b,
        "recover(recover(x)) == recover(x) for topic b"
    );
    assert_eq!(
        first_cap, second_cap,
        "recover(recover(x)) == recover(x) for the capped topic"
    );
    // Spot-check the concrete state: prefix delete removed 1,2; 3,4,5 remain.
    assert_eq!(
        first_b.records.keys().copied().collect::<Vec<_>>(),
        vec![3, 4, 5]
    );
    // cap=2 ⇒ only the newest two survive, with a cap tombstone.
    assert_eq!(first_cap.records.len(), 2, "cap window of 2 survives");
    assert_eq!(
        first_cap.tombstone.as_deref(),
        Some("cap"),
        "cap tombstone persists"
    );
}

// ===========================================================================
// F-SNAP-CRASH-AFTER-TMP-BEFORE-RENAME
// crash after the snapshot .tmp is fsynced but before it is renamed ⇒ load_latest
// finds only the previous snapshot-<n>.bin (the stray .tmp is ignored); never a
// half-snapshot loaded.
// ===========================================================================
#[test]
fn f_snap_crash_after_tmp_before_rename() {
    use topics::storage::snapshot::{load_latest_with, write_snapshot_with, Checkpoint, Snapshot};
    use topics::storage::OpenOpts as OO;

    let disk = FakeDisk::new();
    let fs = disk.arc();
    let data_dir = PathBuf::from(DATA_DIR);
    let meta = PathBuf::from(DATA_DIR).join("meta");

    let mk = |id: u64, seq: u64| Snapshot {
        id,
        ts: 1,
        next_topic_id: 1,
        checkpoint: Checkpoint {
            wal_idx: 1,
            wal_offset: 0,
            last_checkpoint_seq: seq,
            shards: vec![(1, 0)],
            shard_keys: vec![String::new()],
        },
        topics: vec![],
        routers: vec![],
    };

    // Snapshot #1 fully durable (write→fsync→rename→dir-fsync).
    write_snapshot_with(&fs, &data_dir, &mk(1, 100)).unwrap();

    // Now model "snapshot #2 written to .tmp + fsynced, crash before rename":
    // create snapshot-2.bin.tmp with a fsynced body, make its NAME durable, but
    // never rename it to the final name.
    let framed = {
        // Build a real, valid framed snapshot body for id=2 so the .tmp is not
        // itself corrupt — the point is purely that it was never renamed.
        let mut tmp_disk_body = Vec::new();
        // Reuse the production encoder by writing to a throwaway path on a fresh
        // disk, then reading the bytes back.
        let scratch = FakeDisk::new();
        let sfs = scratch.arc();
        write_snapshot_with(&sfs, &data_dir, &mk(2, 200)).unwrap();
        let p = meta.join(snapshot_file(2));
        let f = scratch.open(&p, OO::read_only()).unwrap();
        f.read_to_end_from(0, &mut tmp_disk_body).unwrap();
        tmp_disk_body
    };
    let tmp_path = meta.join(format!("{}.tmp", snapshot_file(2)));
    {
        let mut f = disk.open(&tmp_path, OpenOpts::create_truncate()).unwrap();
        let mut written = 0;
        while written < framed.len() {
            let n = f.write_at(written as u64, &framed[written..]).unwrap();
            written += n;
        }
        f.sync_all().unwrap(); // tmp body durable...
    }
    fs.sync_dir(&meta).unwrap(); // ...and its name durable, but NO rename happened.

    // Crash: nothing pending. The .tmp exists durably; the final snapshot-2.bin
    // never does.
    disk.crash(TornDamage::None);

    // load_latest only considers `snapshot-<n>.bin` and ignores `.tmp`, so it
    // returns the PREVIOUS fully-installed snapshot #1.
    let loaded = load_latest_with(&fs, &data_dir)
        .unwrap()
        .expect("a valid snapshot still loads");
    assert_eq!(
        loaded.id, 1,
        "stray .tmp ignored; previous snapshot #1 loads"
    );
    assert_eq!(loaded.checkpoint.last_checkpoint_seq, 100);
}

// ===========================================================================
// F-SNAP-CRASH-AFTER-RENAME-BEFORE-DIRFSYNC
// crash after the rename but before sync_dir ⇒ the rename may roll back (FakeDisk
// models rename-durable-only-after-sync_dir). Recovery loads EXACTLY ONE valid
// snapshot — either the new one or the previous — never zero.
// ===========================================================================
#[test]
fn f_snap_crash_after_rename_before_dirfsync() {
    use topics::storage::snapshot::{load_latest_with, write_snapshot_with, Checkpoint, Snapshot};

    let mk = |id: u64, seq: u64| Snapshot {
        id,
        ts: 1,
        next_topic_id: 1,
        checkpoint: Checkpoint {
            wal_idx: 1,
            wal_offset: 0,
            last_checkpoint_seq: seq,
            shards: vec![(1, 0)],
            shard_keys: vec![String::new()],
        },
        topics: vec![],
        routers: vec![],
    };

    let data_dir = PathBuf::from(DATA_DIR);
    let meta = PathBuf::from(DATA_DIR).join("meta");

    let disk = FakeDisk::new();
    let fs = disk.arc();

    // Snapshot #1 fully durable.
    write_snapshot_with(&fs, &data_dir, &mk(1, 100)).unwrap();

    // Manually drive snapshot #2's tmp-write → fsync → rename, then STOP before
    // sync_dir, mirroring write_snapshot_with's body so the only missing step is
    // the directory fsync.
    let framed = {
        let scratch = FakeDisk::new();
        let sfs = scratch.arc();
        write_snapshot_with(&sfs, &data_dir, &mk(2, 200)).unwrap();
        durable_wal_bytes(&scratch, &meta.join(snapshot_file(2)))
    };
    let tmp = meta.join(format!("{}.tmp", snapshot_file(2)));
    let fin = meta.join(snapshot_file(2));
    {
        let mut f = disk.open(&tmp, OpenOpts::create_truncate()).unwrap();
        let mut written = 0;
        while written < framed.len() {
            written += f.write_at(written as u64, &framed[written..]).unwrap();
        }
        f.sync_all().unwrap();
    }
    fs.sync_dir(&meta).unwrap(); // tmp name durable.
    fs.rename(&tmp, &fin).unwrap(); // rename issued...
                                    // ...but NO sync_dir of meta here. Crash: the rename is un-fsynced ⇒ rolls back
                                    // to the durable namespace (which has snapshot-1.bin, not snapshot-2.bin).
    disk.crash(TornDamage::None);

    // Exactly one valid snapshot loads. The rolled-back rename means #1 loads; had
    // it been hardened, #2 would — either way NEVER zero, never a half body.
    let loaded = load_latest_with(&fs, &data_dir)
        .unwrap()
        .expect("exactly one valid snapshot loads — never zero");
    assert!(
        loaded.id == 1 || loaded.id == 2,
        "loaded snapshot is one of the two valid candidates, got id={}",
        loaded.id
    );
    // FakeDisk's rename-durable-only-after-sync_dir model rolls the un-fsynced
    // rename back, so the previous snapshot #1 is what survives here.
    assert_eq!(
        loaded.id, 1,
        "un-dir-fsynced rename rolls back ⇒ previous snapshot #1"
    );
    assert_eq!(loaded.checkpoint.last_checkpoint_seq, 100);
}

// ===========================================================================
// F-SEG-CRASH-AFTER-DATA-BEFORE-IDX
// crash after a segment's .data is durably renamed but before its .idx is
// written ⇒ list() requires .data so the segment is "listed", but resolve/read
// needs .idx; a lone .data is an incomplete segment. The WAL/snapshot still holds
// the records ⇒ no live record lost; orphan reclaim handles the stray .data.
// ===========================================================================
#[test]
fn f_seg_crash_after_data_before_idx() {
    use topics::storage::segment::{data_name, idx_name, SegmentBuilder, SegmentRecord};
    use topics::storage::{LocalSegmentStore, SegmentPart, SegmentStore};

    let disk = FakeDisk::new();
    let root = PathBuf::from(DATA_DIR).join("seg");
    disk.create_dir_all(&root).unwrap();

    // Build a real segment (seqs 10..=12) and persist ONLY its .data (the crash
    // lands after .data's atomic rename, before .idx is written).
    let mut b = SegmentBuilder::new(10);
    for seq in 10..=12 {
        b.push(&SegmentRecord {
            seq,
            ts: 1_700_000_000_000 + seq,
            node: None,
            tag: Some("t".into()),
            data: format!("v{seq}").into_bytes(),
        });
    }
    let (data, _idx) = b.finish();

    // write_atomic for the .data part only: tmp write → fsync → rename → dir-fsync.
    let data_path = root.join(data_name(10));
    let tmp = root.join(format!("{}.tmp", data_name(10)));
    {
        let mut f = disk.open(&tmp, OpenOpts::create_truncate()).unwrap();
        let mut w = 0;
        while w < data.len() {
            w += f.write_at(w as u64, &data[w..]).unwrap();
        }
        f.sync_all().unwrap();
    }
    disk.arc().rename(&tmp, &data_path).unwrap();
    disk.arc().sync_dir(&root).unwrap();

    // Crash: the .data is durable, the .idx never existed.
    disk.crash(TornDamage::None);

    // Reopen the store through the crashed image. list() requires .data ⇒ the
    // segment id 10 is listed, but the .idx part is absent ⇒ incomplete.
    let store = LocalSegmentStore::open_with(&root, disk.arc()).unwrap();
    let ids = store.list().unwrap();
    assert_eq!(ids, vec![10], ".data present ⇒ segment id listed");
    assert!(
        store.exists(10, SegmentPart::Data),
        ".data part durably survives the crash"
    );
    assert!(
        !store.exists(10, SegmentPart::Idx),
        "the un-written .idx is absent ⇒ segment is incomplete, not authoritative"
    );
    // A read needing the .idx must surface NotFound (no half-indexed segment is
    // served as valid) — the WAL/snapshot remains the source of truth.
    let idx_read = store.read_range(10, SegmentPart::Idx, 0, 20);
    assert!(
        idx_read.is_err(),
        "reading the missing .idx errors, never decodes neighbor bytes"
    );
    let _ = idx_name(10);
}

// ===========================================================================
// F-COLD-CRASH-AFTER-COPY-BEFORE-FLIP
// crash after the cold.put fsync but before the (in-memory) tier-pointer flip ⇒
// both the hot and cold copies exist; TopicTier::resolve prefers HOT; the record is
// fully readable; the relocator can re-run the idempotent copy+flip+drop with no
// loss.
// ===========================================================================
#[test]
fn f_cold_crash_after_copy_before_flip() {
    use topics::storage::segment::{data_name, idx_name, SegmentBuilder, SegmentRecord};
    use topics::storage::{LocalSegmentStore, SegmentPart, SegmentStore, Tier, TopicTier};

    let disk = FakeDisk::new();
    let hot_root = PathBuf::from(DATA_DIR).join("hot");
    let cold_root = PathBuf::from(DATA_DIR).join("cold");
    disk.create_dir_all(&hot_root).unwrap();
    disk.create_dir_all(&cold_root).unwrap();

    // Seal a segment (seqs 1..=3) durably into the HOT store.
    let mut b = SegmentBuilder::new(1);
    for seq in 1..=3 {
        b.push(&SegmentRecord {
            seq,
            ts: 1_700_000_000_000 + seq,
            node: None,
            tag: None,
            data: format!("h{seq}").into_bytes(),
        });
    }
    let (data, idx) = b.finish();
    let hot = LocalSegmentStore::open_with(&hot_root, disk.arc()).unwrap();
    hot.put(1, &data, &idx).expect("hot put durable");

    // Relocation step 1: copy the segment to COLD (put = write+fsync+rename+
    // dir-fsync), then CRASH before the tier pointer flips (the flip is in-memory
    // only, so on restart it is as if it never happened).
    let cold = LocalSegmentStore::open_with(&cold_root, disk.arc()).unwrap();
    cold.put(1, &data, &idx).expect("cold copy durable");
    disk.crash(TornDamage::None);

    // Restart: reopen both stores through the crashed image and build the tier.
    let hot2: Box<dyn SegmentStore> =
        Box::new(LocalSegmentStore::open_with(&hot_root, disk.arc()).unwrap());
    let cold2: Box<dyn SegmentStore> =
        Box::new(LocalSegmentStore::open_with(&cold_root, disk.arc()).unwrap());
    let tier = TopicTier::new(hot2, Some(cold2));

    // Both copies exist (the flip never persisted ⇒ hot was never dropped).
    assert!(tier.hot().exists(1, SegmentPart::Data), "hot copy survives");
    assert!(
        tier.cold().unwrap().exists(1, SegmentPart::Data),
        "cold copy is durable (its put fsynced before the crash)"
    );
    // resolve prefers HOT in the both-exist window ⇒ never loses the segment.
    assert_eq!(tier.resolve(1), Some(Tier::Hot), "resolve prefers HOT");

    // The record is fully readable from the resolved (hot) store: read the first
    // frame via its .idx locator and confirm it decodes to the original record.
    use topics::storage::segment::{decode_data_frame, idx_entry_at};
    let store = tier.store_for(1).unwrap();
    let idx_bytes = store
        .read_range(1, SegmentPart::Idx, 0, 20)
        .expect("first idx entry readable");
    let e = idx_entry_at(&idx_bytes, 0).expect("idx entry 0");
    let frame = store
        .read_range(1, SegmentPart::Data, e.offset as u64, e.len as u64)
        .expect("data frame readable");
    let rec = decode_data_frame(&frame).expect("frame decodes");
    assert_eq!(rec.seq, 1);
    assert_eq!(
        rec.data, b"h1",
        "the relocated record is intact and readable from HOT"
    );
    let _ = (data_name(1), idx_name(1));
}
