//! Phase-8B fault catalog — **recovery-replay corruption** strategies (file A).
//!
//! Five WAL-decoder corruption strategies, all at the `recovery-replay`
//! boundary. Each drives a real, fully-wired [`Engine`] through a [`FakeDisk`],
//! writes a durable workload, cleanly stops, then **byte-pokes** the durable WAL
//! file to inject a specific corruption and re-hardens it durably (open →
//! `write_at` → `sync_data` → `sync_dir`). A fresh engine then recovers through
//! the same fake and the model oracle (copied from `tests/crash_oracle.rs`)
//! asserts the crash-consistency contract: the corrupt frame is treated as the
//! logical end of log (DecodeStep::Torn ⇒ truncate-at-first-bad), no bogus/torn
//! record is ever materialized (no fabrication), survivors are a dense prefix,
//! and recovery never panics / OOMs / reads out of bounds.
//!
//! IDs implemented:
//!   - F-WAL-LEN-GARBAGE-HUGE  — frame_len garbled to 0xFFFFFFFF (overrun past EOF)
//!   - F-WAL-LEN-BELOW-MIN     — frame_len set below FRAME_HEADER_LEN+FRAME_CRC_LEN
//!   - F-WAL-CRC-FLIP-TAIL     — single-bit flip in the LAST frame body (XXH3 fail)
//!   - F-WAL-CRC-FLIP-MIDLOG   — bit flip in a MID-log frame (truncate-at-first-bad)
//!   - F-WAL-ZEROED-FRAME      — a frame reads back all-zeros (lost/sparse write)
//!
//! ```text
//! cargo test --features test-fs --test fault_recovery_a
//! ```

#![cfg(feature = "test-fs")]
#![allow(
    clippy::ptr_arg,
    clippy::manual_clamp,
    clippy::unusual_byte_groupings,
    clippy::doc_lazy_continuation
)]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use serde_json::json;

use topics::clock::{SharedClock, TestClock};
use topics::config::ServerConfig;
use topics::engine::Engine;
use topics::storage::testfs::FakeDisk;
use topics::storage::OpenOpts;
use topics::types::{DiffRequest, RecordIn, TopicConfig, TopicType, WriteRequest};

// WAL frame layout constants (mirrors src/storage/wal.rs; those are crate-private
// so we re-declare the few we need for byte-poking).
const FRAME_LEN_PREFIX: usize = 4;
const FRAME_HEADER_LEN: usize = 30;
const FRAME_CRC_LEN: usize = 8;
/// Smallest legal `frame_len` value (header + crc, no body).
const MIN_FRAME_LEN: usize = FRAME_HEADER_LEN + FRAME_CRC_LEN;

// ===========================================================================
// Reference model (the oracle) — copied verbatim from tests/crash_oracle.rs.
// ===========================================================================

#[derive(Debug, Clone, PartialEq, Eq)]
struct ModelRecord {
    data: String,
    tag: Option<String>,
    node: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct ModelTopic {
    durable: bool,
    acked: BTreeMap<u64, ModelRecord>,
    ever_acked: BTreeMap<u64, ModelRecord>,
    head: u64,
    delete_floor: u64,
    evict_floor: u64,
    cap: u64,
}

impl ModelTopic {
    fn live_seqs(&self) -> Vec<u64> {
        let floor = self.delete_floor.max(self.evict_floor);
        self.acked.keys().copied().filter(|s| *s >= floor).collect()
    }
    fn earliest(&self) -> u64 {
        self.live_seqs().into_iter().min().unwrap_or(self.head + 1)
    }
}

#[derive(Debug, Default)]
struct RefModel {
    topics: BTreeMap<String, ModelTopic>,
}

impl RefModel {
    fn ensure_topic(&mut self, name: &str, durable: bool, cap: u64) {
        self.topics.entry(name.to_string()).or_insert(ModelTopic {
            durable,
            cap,
            ..Default::default()
        });
    }
    fn ack_append(&mut self, name: &str, seqs: &[u64], recs: &[ModelRecord]) {
        let b = self
            .topics
            .get_mut(name)
            .expect("topic modeled before append");
        for (s, r) in seqs.iter().zip(recs.iter()) {
            b.acked.insert(*s, r.clone());
            b.ever_acked.insert(*s, r.clone());
            b.head = b.head.max(*s);
        }
        if b.cap > 0 {
            let live: Vec<u64> = b
                .acked
                .keys()
                .copied()
                .filter(|s| *s >= b.delete_floor.max(b.evict_floor))
                .collect();
            if live.len() as u64 > b.cap {
                let drop_n = live.len() as u64 - b.cap;
                let new_floor = live[drop_n as usize];
                if new_floor > b.evict_floor {
                    b.evict_floor = new_floor;
                }
            }
        }
    }
}

// ===========================================================================
// Op stream + engine driver — copied / trimmed from tests/crash_oracle.rs.
// ===========================================================================

#[derive(Debug, Clone)]
enum Op {
    PutTopic {
        name: String,
        durable: bool,
        cap: u64,
    },
    Append {
        name: String,
        data: String,
        tag: Option<String>,
        node: Option<String>,
    },
}

fn run_ops(engine: &Engine, model: &mut RefModel, ops: &[Op]) {
    for op in ops {
        match op {
            Op::PutTopic { name, durable, cap } => {
                let cfg = TopicConfig {
                    r#type: TopicType::Log,
                    durable: *durable,
                    cap_records: *cap,
                    ..Default::default()
                };
                if engine.put_topic(name, cfg).is_ok() {
                    model.ensure_topic(name, *durable, *cap);
                }
            }
            Op::Append {
                name,
                data,
                tag,
                node,
            } => {
                let req = WriteRequest {
                    records: vec![RecordIn {
                        data: json!({ "v": data }),
                        tag: tag.clone(),
                        node: node.clone(),
                        meta: None,
                    }],
                    node: None,
                    idempotency_key: None,
                    create: Some(true),
                    config: None,
                    disable_backpressure: true,
                };
                if !model.topics.contains_key(name) {
                    model.ensure_topic(name, false, 0);
                }
                if let Ok(resp) = engine.write(name, req, true) {
                    let seqs = resp.seqs.clone().unwrap_or_else(|| vec![resp.last_seq]);
                    let rec = ModelRecord {
                        data: data.clone(),
                        tag: tag.clone(),
                        node: node.clone(),
                    };
                    model.ack_append(name, &seqs, std::slice::from_ref(&rec));
                }
            }
        }
    }
}

// ===========================================================================
// Engine build / dump plumbing — copied from tests/crash_oracle.rs.
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

fn open_engine(disk: &FakeDisk) -> Arc<Engine> {
    Engine::with_data_dir_fs(cfg(), clock(), disk.arc()).expect("engine opens through FakeDisk")
}

fn stop(engine: Arc<Engine>) {
    drop(engine);
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TopicDump {
    head: u64,
    earliest: u64,
    count: u64,
    records: BTreeMap<u64, ModelRecord>,
    tombstone_reason: Option<String>,
}

fn dump_topic(engine: &Engine, name: &str) -> Option<TopicDump> {
    let st = engine.topic_state(name, false).ok()?;
    let mut records = BTreeMap::new();
    let mut tombstone_reason = None;
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
        if let Some(tomb) = &d.tombstone {
            tombstone_reason = Some(format!("{:?}", tomb.reason).to_lowercase());
        }
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
    Some(TopicDump {
        head: st.head_seq,
        earliest: st.earliest_seq,
        count: st.count,
        records,
        tombstone_reason,
    })
}

/// Make every WAL/meta file's NAME durable (the create+dir-fsync production does).
fn sync_wal_dir(disk: &FakeDisk) {
    let fs = disk.arc();
    let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("wal"));
    let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("meta"));
}

// ===========================================================================
// Oracle contract assertions — copied from tests/crash_oracle.rs.
// ===========================================================================

/// Assert the crash-consistency contract for one topic (the post-corruption,
/// torn-tail relaxation: `whole_tail_durable=false`).
fn assert_topic_contract(
    name: &str,
    model: &ModelTopic,
    dump: &TopicDump,
    whole_tail_durable: bool,
) {
    let live = model.live_seqs();
    let survivors: Vec<u64> = dump.records.keys().copied().collect();

    // (1) NO FABRICATION: survivors ⊆ ever_acked, byte-for-byte. A corrupt/torn
    //     frame must NEVER be materialized as a (bogus) record.
    for (seq, rec) in &dump.records {
        let m = model.ever_acked.get(seq).unwrap_or_else(|| {
            panic!(
                "{name}: recovered seq {seq} was never acked by the model \
                 (fabricated/torn record): {rec:?}"
            )
        });
        assert_eq!(
            m, rec,
            "{name}: recovered record at seq {seq} differs from the model"
        );
    }

    // (2) NO SILENT LOSS (only when the whole tail was durable — not used for the
    //     mid-log corruption cases, which legitimately drop an acked suffix).
    if model.durable && whole_tail_durable {
        for seq in &live {
            assert!(
                dump.records.contains_key(seq),
                "{name}: acked durable seq {seq} LOST after recovery \
                 (survivors={survivors:?}, model live={live:?})"
            );
        }
    }

    // (3) NO GAP — survivors are a dense prefix of the model's live set up to the
    //     surviving high-water mark (a corruption truncates a TAIL, never a hole).
    if let Some(&hi) = survivors.last() {
        for &s in &live {
            if s <= hi {
                assert!(
                    dump.records.contains_key(&s),
                    "{name}: model-live seq {s} missing below surviving high-water \
                     {hi} (hole in the live set): survivors={survivors:?}"
                );
            }
        }
    }

    // (4) HEAD MONOTONE / NO SEQ REUSE (R3). An `fsync` topic's head never exceeds
    //     the acked head; a `disk` topic (acked before fsync) recovers at its durable
    //     head RESERVATION — its head may sit ABOVE the acked head by up to
    //     `DISK_HEAD_RESERVE_AHEAD` (dropped un-fsynced seqs become silent gaps),
    //     never regressing below an acked seq when the whole tail was durable.
    if model.durable {
        assert!(
            dump.head <= model.head,
            "{name}: fsync recovered head {} exceeds model head {} (future seq?)",
            dump.head,
            model.head
        );
    } else {
        if whole_tail_durable && model.head > 0 {
            assert!(
                dump.head >= model.head,
                "{name}: disk recovered head {} REGRESSED below acked head {} (reuse!)",
                dump.head,
                model.head
            );
        }
        assert!(
            dump.head <= model.head + topics::config::DISK_HEAD_RESERVE_AHEAD,
            "{name}: disk recovered head {} exceeds reservation ceiling {}",
            dump.head,
            model.head + topics::config::DISK_HEAD_RESERVE_AHEAD
        );
    }

    // (6) EARLIEST: when the whole durable tail survived, earliest matches.
    if model.durable && whole_tail_durable && !live.is_empty() {
        assert_eq!(
            dump.earliest,
            model.earliest(),
            "{name}: earliest must match model"
        );
    }
}

// ===========================================================================
// WAL byte-poke helpers (the corruption injectors).
// ===========================================================================

/// The single active WAL file path the small workloads write to.
fn wal_path() -> PathBuf {
    PathBuf::from(DATA_DIR)
        .join("wal")
        .join(format!("wal-{:016}.log", 1))
}

/// Read the durable bytes of the active WAL file (what survives a crash now).
fn read_wal_durable(disk: &FakeDisk) -> Vec<u8> {
    disk.durable_bytes(&wal_path())
        .expect("active WAL file is durable after sync_wal_dir")
}

/// Overwrite the active WAL file's durable bytes with `bytes` and re-harden it
/// (open → write_at → sync_data → sync_dir), so the corruption survives the
/// recovery read exactly as a real on-disk corruption would.
fn rewrite_wal_durable(disk: &FakeDisk, bytes: &[u8]) {
    let fs = disk.arc();
    let path = wal_path();
    {
        let mut f = fs
            .open(&path, OpenOpts::rw_existing())
            .expect("reopen active WAL for poke");
        let mut written = 0usize;
        while written < bytes.len() {
            let n = f
                .write_at(written as u64, &bytes[written..])
                .expect("write poked WAL bytes");
            written += n;
        }
        f.sync_data().expect("fsync poked WAL bytes");
    }
    let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("wal"));
    // The poke must now be the durable image.
    assert_eq!(
        &disk.durable_bytes(&path).expect("WAL still durable")[..bytes.len()],
        bytes,
        "byte-poke did not become durable"
    );
}

/// Walk the WAL buffer frame-by-frame using only the `frame_len` prefix and the
/// minimum-length guard (the same framing-only scan recovery does before any
/// body decode). Returns the start offset of every well-formed frame whose
/// declared length fits inside the buffer; stops at the first torn/zero frame.
/// Used to locate which bytes to poke.
fn frame_offsets(buf: &[u8]) -> Vec<usize> {
    let mut offs = Vec::new();
    let mut pos = 0usize;
    while pos + FRAME_LEN_PREFIX <= buf.len() {
        let frame_len = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
        if frame_len < MIN_FRAME_LEN {
            break; // zero/sub-min ⇒ preallocation tail / torn — end of frames.
        }
        let total = FRAME_LEN_PREFIX + frame_len;
        if pos + total > buf.len() {
            break; // overrun ⇒ end of complete frames.
        }
        offs.push(pos);
        pos += total;
    }
    offs
}

/// The end offset (exclusive) of the frame that starts at `start`.
fn frame_end(buf: &[u8], start: usize) -> usize {
    let frame_len = u32::from_le_bytes(buf[start..start + 4].try_into().unwrap()) as usize;
    start + FRAME_LEN_PREFIX + frame_len
}

// ===========================================================================
// Shared workload driver: write N durable appends, sync, stop, return the
// model. The disk holds the durable WAL ready for byte-poking.
// ===========================================================================

/// Drive a fixed durable workload of `n` appends into topic `b` and return the
/// reference model. After this returns the engine is stopped and the WAL is
/// durable on `disk`.
fn run_durable_workload(disk: &FakeDisk, n: u64) -> RefModel {
    let mut model = RefModel::default();
    let mut ops = vec![Op::PutTopic {
        name: "b".into(),
        durable: true,
        cap: 0,
    }];
    for i in 1..=n {
        ops.push(Op::Append {
            name: "b".into(),
            data: format!("d{i}"),
            tag: Some(format!("t{i}")),
            node: if i % 2 == 0 {
                Some(format!("n{i}"))
            } else {
                None
            },
        });
    }
    let engine = open_engine(disk);
    run_ops(&engine, &mut model, &ops);
    sync_wal_dir(disk);
    stop(engine);
    model
}

// ===========================================================================
// F-WAL-LEN-GARBAGE-HUGE
// frame_len garbled to a huge value (0xFFFFFFFF) past EOF on the last frame.
// Oracle: overrun ⇒ Torn ⇒ truncate; bounded before any body read; no OOM/panic.
// ===========================================================================
#[test]
fn f_wal_len_garbage_huge() {
    let disk = FakeDisk::new();
    let model = run_durable_workload(&disk, 4);

    let mut buf = read_wal_durable(&disk);
    let offs = frame_offsets(&buf);
    assert!(
        offs.len() >= 4,
        "workload wrote >=4 frames, got {}",
        offs.len()
    );

    // Garble the LAST frame's frame_len to 0xFFFFFFFF (overruns the buffer).
    let last = *offs.last().unwrap();
    buf[last..last + 4].copy_from_slice(&0xFFFF_FFFFu32.to_le_bytes());
    rewrite_wal_durable(&disk, &buf);

    // Recovery must NOT OOM/panic and must truncate at the bad frame.
    let engine = open_engine(&disk);
    let dump = dump_topic(&engine, "b").expect("topic recovers");
    assert_topic_contract("b", &model.topics["b"], &dump, false);

    // The 3 frames before the poked last one survive as a dense prefix [1..=3];
    // the overrun frame (seq 4) is truncated, never materialized.
    let survivors: Vec<u64> = dump.records.keys().copied().collect();
    assert_eq!(
        survivors,
        vec![1, 2, 3],
        "huge-len frame truncated, prior intact"
    );
    assert!(
        !dump.records.contains_key(&4),
        "overrun frame never materialized"
    );
    stop(engine);
}

// ===========================================================================
// F-WAL-LEN-BELOW-MIN
// frame_len set below FRAME_HEADER_LEN+FRAME_CRC_LEN (e.g. 1) on the last frame.
// Oracle: decode_one rejects as Torn (also the prealloc-zeros stop condition);
// no underflow, no panic.
// ===========================================================================
#[test]
fn f_wal_len_below_min() {
    let disk = FakeDisk::new();
    let model = run_durable_workload(&disk, 4);

    let mut buf = read_wal_durable(&disk);
    let offs = frame_offsets(&buf);
    assert!(offs.len() >= 4, "workload wrote >=4 frames");

    // Poke the LAST frame's frame_len to 1 (< MIN_FRAME_LEN ⇒ sub-minimum).
    let last = *offs.last().unwrap();
    buf[last..last + 4].copy_from_slice(&1u32.to_le_bytes());
    rewrite_wal_durable(&disk, &buf);

    let engine = open_engine(&disk);
    let dump = dump_topic(&engine, "b").expect("topic recovers");
    assert_topic_contract("b", &model.topics["b"], &dump, false);

    // Sub-min frame_len ⇒ Torn ⇒ the last frame and anything after is dropped;
    // the prior 3 frames are a dense prefix, no underflow/panic.
    let survivors: Vec<u64> = dump.records.keys().copied().collect();
    assert_eq!(
        survivors,
        vec![1, 2, 3],
        "sub-min frame_len truncated, prior intact"
    );
    stop(engine);
}

// ===========================================================================
// F-WAL-CRC-FLIP-TAIL
// single-bit flip in the LAST frame body (XXH3 mismatch).
// Oracle: CRC mismatch ⇒ Torn ⇒ truncate that frame; prior frames intact;
// recovery succeeds.
// ===========================================================================
#[test]
fn f_wal_crc_flip_tail() {
    let disk = FakeDisk::new();
    let model = run_durable_workload(&disk, 4);

    let mut buf = read_wal_durable(&disk);
    let offs = frame_offsets(&buf);
    assert!(offs.len() >= 4, "workload wrote >=4 frames");

    // Flip one bit in the LAST frame's body (1 byte past its fixed header, well
    // inside the CRC-protected region). This breaks the XXH3 over [4..crc_start).
    let last = *offs.last().unwrap();
    let flip_at = last + FRAME_LEN_PREFIX + FRAME_HEADER_LEN; // first body byte
    assert!(
        flip_at < frame_end(&buf, last) - FRAME_CRC_LEN,
        "flip is inside the body"
    );
    buf[flip_at] ^= 0x01;
    rewrite_wal_durable(&disk, &buf);

    let engine = open_engine(&disk);
    let dump = dump_topic(&engine, "b").expect("topic recovers");
    assert_topic_contract("b", &model.topics["b"], &dump, false);

    // The flipped last frame fails CRC ⇒ truncated; the prior 3 survive intact.
    let survivors: Vec<u64> = dump.records.keys().copied().collect();
    assert_eq!(
        survivors,
        vec![1, 2, 3],
        "CRC-bad tail truncated, prior intact"
    );
    // Fidelity of the survivors (the corruption did not bleed into prior frames).
    assert_eq!(dump.records[&1].data, "d1");
    assert_eq!(dump.records[&3].tag.as_deref(), Some("t3"));
    stop(engine);
}

// ===========================================================================
// F-WAL-CRC-FLIP-MIDLOG
// bit flip in a frame in the MIDDLE of an otherwise-valid log (lost/bit-rot).
// Oracle: replay stops at the corrupt frame (truncate-at-first-bad, never
// skip-and-continue); everything after is dropped. Documented behavior asserted:
// no fabrication, no gap among survivors. (Acked frames AFTER the mid-log
// corruption are legitimately lost — a single linear WAL cannot recover a frame
// past an unreadable one; recovery must NOT silently skip it.)
// ===========================================================================
#[test]
fn f_wal_crc_flip_midlog() {
    let disk = FakeDisk::new();
    let model = run_durable_workload(&disk, 6);

    let mut buf = read_wal_durable(&disk);
    let offs = frame_offsets(&buf);
    assert!(
        offs.len() >= 6,
        "workload wrote >=6 frames, got {}",
        offs.len()
    );

    // Flip a bit in the body of a MID-log frame (the 4th of 6+, i.e. seq 4). The
    // create-topic frame may be first, so index by the LAST 6 (the appends).
    let appends: Vec<usize> = offs[offs.len() - 6..].to_vec();
    let mid = appends[3]; // the 4th append frame (seq 4)
    let flip_at = mid + FRAME_LEN_PREFIX + FRAME_HEADER_LEN;
    assert!(
        flip_at < frame_end(&buf, mid) - FRAME_CRC_LEN,
        "flip is inside the body"
    );
    buf[flip_at] ^= 0x01;
    rewrite_wal_durable(&disk, &buf);

    let engine = open_engine(&disk);
    let dump = dump_topic(&engine, "b").expect("topic recovers");

    // DOCUMENTED BEHAVIOR: truncate-at-first-bad. Replay stops at the corrupt
    // mid-log frame; everything from it onward (seqs 4,5,6) is dropped. The
    // survivors are exactly the dense prefix [1..=3] — NO fabrication, NO gap,
    // NO skip-and-continue that would resurface seqs 5/6 over a hole at 4.
    assert_topic_contract("b", &model.topics["b"], &dump, false);
    let survivors: Vec<u64> = dump.records.keys().copied().collect();
    assert_eq!(
        survivors,
        vec![1, 2, 3],
        "mid-log corruption truncates at first bad frame; never skip-and-continue"
    );
    assert!(
        !dump.records.contains_key(&5) && !dump.records.contains_key(&6),
        "frames AFTER the corruption are dropped, not skip-resurfaced (no gap)"
    );
    stop(engine);
}

// ===========================================================================
// F-WAL-ZEROED-FRAME
// a frame reads back all-zeros (lost write / sparse hole in preallocated region).
// Oracle: frame_len==0 < minimum ⇒ Torn ⇒ stop; the zeroed region is treated as
// end-of-data, not an empty record.
// ===========================================================================
#[test]
fn f_wal_zeroed_frame() {
    let disk = FakeDisk::new();
    let model = run_durable_workload(&disk, 4);

    let mut buf = read_wal_durable(&disk);
    let offs = frame_offsets(&buf);
    assert!(offs.len() >= 4, "workload wrote >=4 frames");

    // Zero out the LAST frame entirely (a lost write / sparse hole — the bytes
    // read back as zeros, exactly like an un-written preallocated region).
    let last = *offs.last().unwrap();
    let end = frame_end(&buf, last);
    for b in &mut buf[last..end] {
        *b = 0;
    }
    rewrite_wal_durable(&disk, &buf);

    let engine = open_engine(&disk);
    let dump = dump_topic(&engine, "b").expect("topic recovers");
    assert_topic_contract("b", &model.topics["b"], &dump, false);

    // frame_len==0 (< MIN_FRAME_LEN) ⇒ Torn ⇒ the zeroed region stops replay; it
    // is NOT decoded as an empty record. Prior 3 frames are a dense prefix.
    let survivors: Vec<u64> = dump.records.keys().copied().collect();
    assert_eq!(
        survivors,
        vec![1, 2, 3],
        "zeroed frame treated as end-of-data, not a record"
    );
    assert!(
        !dump.records.contains_key(&4),
        "zeroed region never materialized as a record"
    );
    stop(engine);
}
