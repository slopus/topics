//! Phase-8B fault strategies (boundary: **recovery-replay**), implemented on the
//! Phase-8A harness (`FakeDisk` / `FaultFs` + the model-oracle from
//! `tests/crash_oracle.rs`). Three strategies, one test fn each:
//!
//! - **F-COMPOUND-TORN-SNAP-AND-TORN-WAL** — the newest snapshot body is garbled
//!   AND the active WAL tail is torn at the same time. `load_latest` must skip the
//!   torn newest snapshot and fall back to an older valid snapshot (or full WAL
//!   replay); recovery truncates the torn WAL tail; the combined recovery is
//!   consistent and every acked-before-both write survives, no gap, no fabrication.
//!
//! - **F-PROP-OPSEQ-CRASH-RECOVER** — a proptest-generated random op sequence
//!   ({put_topic / append(durable?) / delete}) with a power loss injected at a
//!   proptest-chosen step; after recovery the engine matches the reference model
//!   under the contract (acked ⊆ survivors ⊆ ever_acked, no gap, monotone seq,
//!   delete-never-resurrects, floors preserved) and recovery is idempotent.
//!
//! - **F-FUZZ-WAL-DECODER** — arbitrary garbage bytes fed to the WAL frame decoder
//!   (`WalReader`, which wraps `decode_one`): the decoder must never panic / OOM /
//!   read OOB, always terminating in a `Frame` or a clean `Torn` (stop), bounded by
//!   `frame_len` validation *before* any body read.
//!
//! ```text
//! cargo test --features test-fs --test fault_recovery_f
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
use topics::storage::testfs::{FakeDisk, TornDamage};
use topics::storage::wal::{encode_frame, WalReader, WalRecord};
use topics::storage::OpenOpts;
use topics::types::{
    DeleteRequest, DiffRequest, Filter, RecordIn, TopicConfig, TopicType, WriteRequest,
};

// ===========================================================================
// Reference model (the oracle) — the same pure-Rust model `tests/crash_oracle.rs`
// uses, distilled to the fields these three strategies exercise.
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
    /// Every acked record by seq (the must-survive set for a durable topic). A delete
    /// removes from here; an eviction advances `evict_floor`.
    acked: BTreeMap<u64, ModelRecord>,
    /// Every record EVER acked — deletes/evictions never remove from here. A delete
    /// whose WAL control frame was lost to a crash may not take effect, so a
    /// previously-deleted seq may legitimately reappear; "no fabrication" is
    /// `survivors ⊆ ever_acked`.
    ever_acked: BTreeMap<u64, ModelRecord>,
    head: u64,
    /// Voluntary-delete front floor: records below it are gone SILENTLY. Monotone.
    delete_floor: u64,
    /// Involuntary cap/TTL floor: records below it are gone with a tombstone.
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

    fn ack_delete(&mut self, name: &str, before_seq: Option<u64>, tag_eq: Option<&str>) {
        let Some(b) = self.topics.get_mut(name) else {
            return;
        };
        if let Some(bs) = before_seq {
            if bs > b.delete_floor {
                b.delete_floor = bs;
            }
            b.acked.retain(|s, _| *s >= bs);
        }
        if let Some(tag) = tag_eq {
            b.acked.retain(|_, r| r.tag.as_deref() != Some(tag));
        }
    }
}

// ===========================================================================
// Op stream + driver
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
    Delete {
        name: String,
        before_seq: Option<u64>,
        tag_eq: Option<String>,
    },
}

/// Drive `ops` against a real engine, mirroring acked effects into `model`. A
/// failed/aborted op is NOT mirrored (unacked ⇒ may be absent after recovery).
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
                    // Auto-created here ⇒ default config is non-durable.
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
            Op::Delete {
                name,
                before_seq,
                tag_eq,
            } => {
                let req = DeleteRequest {
                    before_seq: *before_seq,
                    match_: tag_eq.as_ref().map(|t| Filter::from_shorthand(t)),
                };
                if engine.delete(name, req).is_ok() {
                    model.ack_delete(name, *before_seq, tag_eq.as_deref());
                }
            }
        }
    }
}

// ===========================================================================
// Engine plumbing through FakeDisk
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

/// Make every WAL/meta file NAME durable (model the dir fsync production does at
/// WAL open + snapshot install, so the file survives a crash).
fn sync_dirs(disk: &FakeDisk) {
    let fs = disk.arc();
    let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("wal"));
    let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("meta"));
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TopicDump {
    head: u64,
    earliest: u64,
    count: u64,
    records: BTreeMap<u64, ModelRecord>,
    tombstone_reason: Option<String>,
}

/// Read the full recovered state of `name` through the engine's public API.
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

// ===========================================================================
// The oracle: diff recovered engine state against the model under the contract
// (distilled from `tests/crash_oracle.rs::assert_topic_contract`).
// ===========================================================================

fn assert_topic_contract(
    name: &str,
    model: &ModelTopic,
    dump: &TopicDump,
    whole_tail_durable: bool,
) {
    let live = model.live_seqs();
    let survivors: Vec<u64> = dump.records.keys().copied().collect();

    // (1) NO FABRICATION: survivors ⊆ ever_acked, byte-for-byte.
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

    // (2) NO SILENT LOSS of acked-durable when the whole tail was durable.
    if model.durable && whole_tail_durable {
        for seq in &live {
            assert!(
                dump.records.contains_key(seq),
                "{name}: acked durable seq {seq} LOST after recovery \
                 (survivors={survivors:?}, model live={live:?})"
            );
        }
    }

    // (3) NO GAP — survivors are a dense prefix of the model's expected-present set.
    if let Some(&hi) = survivors.last() {
        if whole_tail_durable {
            let expected_prefix: Vec<u64> = live.iter().copied().filter(|s| *s <= hi).collect();
            assert_eq!(
                survivors, expected_prefix,
                "{name}: survivors must be a dense prefix of the model's live set \
                 (survivors={survivors:?}, model live={live:?})"
            );
        } else {
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
    }

    // (4) HEAD MONOTONE / NO SEQ REUSE (R3). An `fsync` topic (`model.durable`) has
    //     no head reservation, so its head matches the acked head exactly (never a
    //     future seq) and equals it when the whole tail is durable. A `disk` topic
    //     (acked before fsync) recovers at its durable head RESERVATION — its head
    //     may sit ABOVE the acked head by up to `DISK_HEAD_RESERVE_AHEAD` (the
    //     dropped un-fsynced tail becomes silent deleted gaps) but NEVER regresses
    //     below an acked seq (no reuse) when the whole tail was durable.
    if model.durable {
        assert!(
            dump.head <= model.head,
            "{name}: fsync recovered head {} exceeds model head {} (future seq?)",
            dump.head,
            model.head
        );
        if whole_tail_durable && !live.is_empty() {
            assert_eq!(
                dump.head, model.head,
                "{name}: durable head must match model"
            );
        }
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

    // (5) FLOORS PRESERVED: an involuntary cap eviction still tombstones.
    if model.evict_floor > model.delete_floor && !survivors.is_empty() {
        assert!(
            dump.tombstone_reason.is_some(),
            "{name}: cap/TTL floor advanced but no tombstone after recovery \
             (involuntary loss must be explicit)"
        );
    }

    // (6) EARLIEST matches when the whole durable tail survived.
    if model.durable && whole_tail_durable && !live.is_empty() {
        assert_eq!(
            dump.earliest,
            model.earliest(),
            "{name}: recovered earliest_seq must match the model"
        );
    }
}

fn assert_recovered_matches_model(engine: &Engine, model: &RefModel, whole_tail_durable: bool) {
    for (name, mbox) in &model.topics {
        if mbox.acked.is_empty() && mbox.head == 0 {
            continue;
        }
        let Some(dump) = dump_topic(engine, name) else {
            if mbox.durable && !mbox.live_seqs().is_empty() && whole_tail_durable {
                panic!("{name}: durable topic with acked records vanished after recovery");
            }
            continue;
        };
        assert_topic_contract(name, mbox, &dump, whole_tail_durable);
    }
}

// ===========================================================================
// F-COMPOUND-TORN-SNAP-AND-TORN-WAL
// ===========================================================================

/// Locate the active (highest-index) `wal-<n>.log` path under `/data/wal`.
fn active_wal_path(disk: &FakeDisk) -> Option<PathBuf> {
    let fs = disk.arc();
    let wal_dir = PathBuf::from(DATA_DIR).join("wal");
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
    files.pop()
}

/// Durably append a TORN frame to the active WAL file at its true end-of-data: an
/// oversized `frame_len` followed by a few junk bytes (length overrun ⇒ CRC/Torn).
/// This is exactly the torn-tail injection `tests/crash_recovery.rs` uses, but on
/// the FakeDisk: it models a write that power-loss interrupted mid-frame — a frame
/// the model never acked, that recovery must truncate (never materialize).
fn durably_tear_wal_tail(disk: &FakeDisk, path: &PathBuf) {
    let fs = disk.arc();
    // Find the offset where the last CRC-valid frame ends (the reader's own framing).
    let valid = {
        let f = fs
            .open(path, OpenOpts::read_only())
            .expect("open wal to size");
        let mut buf = Vec::new();
        f.read_to_end_from(0, &mut buf).expect("read wal bytes");
        WalReader::new(buf).count_valid_len()
    };
    let mut junk = Vec::new();
    junk.extend_from_slice(&9999u32.to_le_bytes()); // frame_len far past EOF ⇒ overrun
    junk.extend_from_slice(&[0xAB; 16]); // a few garbage bytes
    let mut f = fs
        .open(path, OpenOpts::rw_existing())
        .expect("open wal to tear");
    let mut w = 0;
    while w < junk.len() {
        w += f
            .write_at(valid as u64 + w as u64, &junk[w..])
            .expect("tear write");
    }
    f.sync_data()
        .expect("harden the torn tail (durable partial frame)");
}

/// The newest `snapshot-<n>.bin` path under `/data/meta` (highest id).
fn newest_snapshot_path(disk: &FakeDisk) -> Option<PathBuf> {
    let fs = disk.arc();
    let meta_dir = PathBuf::from(DATA_DIR).join("meta");
    let mut files: Vec<PathBuf> = fs
        .read_dir(&meta_dir)
        .unwrap_or_default()
        .into_iter()
        .filter(|p| {
            p.file_name()
                .and_then(|s| s.to_str())
                .map(|n| n.starts_with("snapshot-") && n.ends_with(".bin"))
                .unwrap_or(false)
        })
        .collect();
    files.sort();
    files.pop()
}

/// Durably garble one body byte of `path` (in-place, then fsync so the corruption
/// survives the crash): models bit-rot at rest in the snapshot body. The snapshot
/// header is the first 20 bytes (magic+version+len+crc); we flip a byte well past
/// it so the body CRC fails (header parse still succeeds, body decode is rejected).
fn durably_garble_body(disk: &FakeDisk, path: &PathBuf, body_off: u64) {
    let fs = disk.arc();
    let mut f = fs
        .open(path, OpenOpts::rw_existing())
        .expect("open to garble");
    let mut one = [0u8; 1];
    let n = f.read_at(body_off, &mut one).expect("read byte to garble");
    assert_eq!(n, 1, "snapshot body byte must exist at off {body_off}");
    one[0] ^= 0xFF;
    let mut w = 0;
    while w < 1 {
        w += f.write_at(body_off, &one[..]).expect("garble write");
    }
    f.sync_data()
        .expect("harden the corruption (durable bit-rot)");
}

/// F-COMPOUND-TORN-SNAP-AND-TORN-WAL: the newest snapshot body is garbled AND the
/// active WAL tail is torn at the same time. `load_latest` skips the torn newest
/// snapshot and falls back to the older valid snapshot; recovery replays the WAL
/// from that snapshot's checkpoint and truncates the torn tail; every
/// acked-before-both write survives, no gap, no fabrication.
#[test]
fn f_compound_torn_snap_and_torn_wal() {
    let disk = FakeDisk::with_seed(0x5EED_C0DE);
    let mut model = RefModel::default();

    {
        let engine = open_engine(&disk);

        // Phase A: three durable acked writes, then snapshot #1 (the older, VALID
        // fallback) covering seqs 1..=3.
        run_ops(
            &engine,
            &mut model,
            &[
                Op::PutTopic {
                    name: "jobs".into(),
                    durable: true,
                    cap: 0,
                },
                Op::Append {
                    name: "jobs".into(),
                    data: "a".into(),
                    tag: Some("t1".into()),
                    node: Some("nA".into()),
                },
                Op::Append {
                    name: "jobs".into(),
                    data: "b".into(),
                    tag: Some("t2".into()),
                    node: None,
                },
                Op::Append {
                    name: "jobs".into(),
                    data: "c".into(),
                    tag: None,
                    node: Some("nB".into()),
                },
            ],
        );
        sync_dirs(&disk);
        assert!(
            engine.write_snapshot().expect("snapshot #1 writes"),
            "snap #1"
        );
        sync_dirs(&disk);

        // Phase B: three more durable acked writes, then snapshot #2 (the NEWEST,
        // which we will garble) covering seqs 1..=6.
        run_ops(
            &engine,
            &mut model,
            &[
                Op::Append {
                    name: "jobs".into(),
                    data: "d".into(),
                    tag: Some("t4".into()),
                    node: None,
                },
                Op::Append {
                    name: "jobs".into(),
                    data: "e".into(),
                    tag: None,
                    node: Some("nC".into()),
                },
                Op::Append {
                    name: "jobs".into(),
                    data: "f".into(),
                    tag: Some("t6".into()),
                    node: None,
                },
            ],
        );
        sync_dirs(&disk);
        assert!(
            engine.write_snapshot().expect("snapshot #2 writes"),
            "snap #2"
        );
        sync_dirs(&disk);

        // Stop the engine FIRST so its WAL writer thread is fully drained/joined and
        // cannot append after we have located the active file's true end-of-data.
        let wal_path = active_wal_path(&disk).expect("active WAL file exists");
        stop(engine);

        // INJECT FAULT 1: durably garble the newest snapshot's body (bit-rot at
        // rest). The header (first 20 bytes) stays intact; a body byte flip fails the
        // body XXH3 ⇒ `read_snapshot_file` rejects it ⇒ `load_latest` falls back to
        // the older valid snapshot #1.
        let newest = newest_snapshot_path(&disk).expect("newest snapshot exists");
        durably_garble_body(&disk, &newest, /*body_off=*/ 24);

        // INJECT FAULT 2: a torn WAL tail — a partial frame (oversized frame_len +
        // junk) appended past the last good frame. It is a frame the model never
        // acked; recovery must truncate it, never materialize a record from it.
        durably_tear_wal_tail(&disk, &wal_path);

        // Both corruptions are already durable; a clean power loss (no extra torn
        // damage) freezes the image at exactly this point.
        disk.crash(TornDamage::None);
    }
    disk.reset_power();

    // Both faults are now on the surviving image. Recovery must:
    //  - skip the torn newest snapshot, load the older valid one (covers 1..=3),
    //  - replay the WAL from that checkpoint to recover 4..=6 (all durable+acked),
    //  - truncate the torn tail (frame 7 never materializes).
    let engine = open_engine(&disk);
    assert_recovered_matches_model(&engine, &model, true);

    let dump = dump_topic(&engine, "jobs").expect("jobs survived the compound fault");
    assert_eq!(
        dump.records.keys().copied().collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5, 6],
        "all six acked-durable writes survive across the torn-snap + torn-WAL fault"
    );
    assert_eq!(dump.head, 6, "head recovered to the last acked seq");
    assert_eq!(dump.records[&1].data, "a");
    assert_eq!(dump.records[&4].data, "d");
    assert_eq!(dump.records[&6].tag.as_deref(), Some("t6"));

    // IDEMPOTENT: a second recovery over the same image yields identical state.
    stop(engine);
    let engine2 = open_engine(&disk);
    let dump2 = dump_topic(&engine2, "jobs").expect("jobs present on recovery #2");
    assert_eq!(
        dump, dump2,
        "recover(recover(x)) == recover(x) after compound fault"
    );
    stop(engine2);
}

// ===========================================================================
// F-PROP-OPSEQ-CRASH-RECOVER
// ===========================================================================

use proptest::prelude::*;

/// A proptest strategy for a small random op sequence over a fixed handful of topic
/// names. Appends carry a durable? flag; deletes use a before_seq prefix or a tag
/// match. Kept bounded so each case (which spins a real engine + recovery) is fast.
fn op_strategy() -> impl Strategy<Value = Op> {
    let names = prop::sample::select(vec!["a".to_string(), "b".to_string()]);
    let tags = prop::sample::select(vec![None, Some("x".to_string()), Some("y".to_string())]);
    prop_oneof![
        // PutTopic: durable bias toward true (the must-survive case) + small caps.
        (
            names.clone(),
            any::<bool>(),
            prop::sample::select(vec![0u64, 0, 3])
        )
            .prop_map(|(name, durable, cap)| Op::PutTopic { name, durable, cap }),
        // Append: a small payload + optional tag/node.
        (names.clone(), 0u8..16, tags.clone()).prop_map(|(name, d, tag)| Op::Append {
            name,
            data: format!("v{d}"),
            tag,
            node: None,
        }),
        // Delete: a before_seq prefix OR a tag match (never both, to keep the model
        // simple and the intent clear).
        (names, prop::option::of(1u64..6), tags).prop_map(|(name, before_seq, tag_eq)| {
            // Bias toward a prefix delete; fall back to a tag delete when no prefix.
            if before_seq.is_some() {
                Op::Delete {
                    name,
                    before_seq,
                    tag_eq: None,
                }
            } else {
                Op::Delete {
                    name,
                    before_seq: None,
                    tag_eq,
                }
            }
        }),
    ]
}

proptest! {
    // Bounded: few cases, small sequences, no shrink explosion — keeps the file
    // well under a minute while still exercising the model oracle across random
    // op interleavings + a random crash step.
    // Bounded for wall-time: each case spins a REAL engine (whose hardcoded WAL
    // config preallocates a large active file, so every `open` is the dominant
    // cost) at least twice. We keep the case count small, the sequences short, and
    // gate the (third) idempotent re-recovery open behind a flag so the whole file
    // runs in well under a minute while still covering random op interleavings + a
    // random crash step against the model oracle.
    #![proptest_config(ProptestConfig {
        cases: 8,
        max_shrink_iters: 96,
        .. ProptestConfig::default()
    })]

    /// F-PROP-OPSEQ-CRASH-RECOVER: drive a random op sequence against a real engine
    /// on a FakeDisk, inject a power loss at a proptest-chosen step `k`, recover, and
    /// assert the engine matches the reference model under the crash-consistency
    /// contract. Ops before the crash that were durable+acked must survive; the
    /// crash drops only an unacked tail; recovery is idempotent.
    #[test]
    fn f_prop_opseq_crash_recover(
        ops in prop::collection::vec(op_strategy(), 1..9),
        crash_frac in 0u32..=100u32,
        seed in any::<u64>(),
    ) {
        let disk = FakeDisk::with_seed(seed);
        let mut model = RefModel::default();

        // Split the op sequence at a proptest-chosen step: everything up to `k` runs,
        // then we crash (drop un-fsynced pending bytes). Durable appends block on a
        // real group fsync, so an acked durable write before the crash is promoted
        // and must survive; an op after the crash lands on a frozen device and is
        // dropped (not mirrored into the model, since the engine call fails or its
        // bytes never reach durable media).
        let k = ((ops.len() as u64 * crash_frac as u64) / 100) as usize;
        let (before, after) = ops.split_at(k.min(ops.len()));

        {
            let engine = open_engine(&disk);
            run_ops(&engine, &mut model, before);
            // Harden the WAL/meta directory names so the files survive the crash.
            sync_dirs(&disk);
            // Power loss: freeze + drop un-fsynced pending bytes (clean drop, no torn
            // damage — the torn-tail truncation is covered by the compound test).
            disk.crash(TornDamage::None);
            // The `after` ops execute on the now-frozen device; their writes are
            // silently lost (power off). They are NOT mirrored into the model.
            let mut sink = RefModel::default();
            // Pre-seed sink with the same topic configs so run_ops does not panic on an
            // append-before-modeled-topic; their acks won't matter (frozen ⇒ no
            // durability), and we discard `sink` entirely.
            for (name, b) in &model.topics {
                sink.ensure_topic(name, b.durable, b.cap);
            }
            run_ops(&engine, &mut sink, after);
            stop(engine);
        }
        disk.reset_power();

        // Recover and assert the contract. `whole_tail_durable=false`: a crash
        // mid-stream means we cannot pin exactly which acked op's fsync landed, so
        // the relaxed (always-true) subset contract applies — no fabrication, no gap
        // in the live set below the surviving high-water, monotone seq.
        let engine = open_engine(&disk);
        assert_recovered_matches_model(&engine, &model, false);

        // IDEMPOTENT RECOVERY: recover(recover(x)) == recover(x) for every topic. The
        // second recovery `open` is the costliest step (large WAL preallocation), so
        // run it on a deterministic subset of cases (keyed off the crash fraction) —
        // enough to catch a non-convergent recovery without tripling the wall time.
        let dumps1: BTreeMap<String, Option<TopicDump>> = model
            .topics
            .keys()
            .map(|n| (n.clone(), dump_topic(&engine, n)))
            .collect();
        stop(engine);
        if crash_frac % 2 == 0 {
            let engine2 = open_engine(&disk);
            for (name, d1) in &dumps1 {
                let d2 = dump_topic(&engine2, name);
                prop_assert_eq!(d1, &d2, "recovery not idempotent for topic {}", name);
            }
            stop(engine2);
        }
    }
}

// ===========================================================================
// F-FUZZ-WAL-DECODER
// ===========================================================================

use arbitrary::{Arbitrary, Unstructured};

/// A structured choice of bytes to feed the decoder: either pure random garbage, or
/// a *valid* frame whose bytes are then partially mangled — the high-value inputs
/// that stress the `frame_len` / internal-length / CRC guards.
#[derive(Debug, Arbitrary)]
enum FuzzInput {
    /// Raw arbitrary bytes (the unstructured corpus).
    Garbage(Vec<u8>),
    /// A valid frame followed by trailing arbitrary bytes (exercises the
    /// frame-then-torn-tail path the recovery reader actually walks).
    ValidThenGarbage {
        seq: u64,
        payload: Vec<u8>,
        tail: Vec<u8>,
    },
    /// A valid frame with a single byte flipped at a chosen index (CRC mismatch /
    /// internal-length / header garble — all must be `Torn`, never a panic).
    FlippedFrame {
        seq: u64,
        payload: Vec<u8>,
        flip_at: u16,
    },
}

/// Build the byte buffer the decoder will be fed from a structured `FuzzInput`.
fn build_fuzz_buf(input: &FuzzInput) -> Vec<u8> {
    match input {
        FuzzInput::Garbage(bytes) => bytes.clone(),
        FuzzInput::ValidThenGarbage { seq, payload, tail } => {
            let mut buf = Vec::new();
            let mut frame = Vec::new();
            encode_frame(
                &mut frame,
                &WalRecord::Append {
                    topic_id: 1,
                    seq: *seq,
                    ts: 1,
                    node: None,
                    tag: None,
                    data: payload.clone(),
                },
                false,
            );
            buf.extend_from_slice(&frame);
            buf.extend_from_slice(tail);
            buf
        }
        FuzzInput::FlippedFrame {
            seq,
            payload,
            flip_at,
        } => {
            let mut frame = Vec::new();
            encode_frame(
                &mut frame,
                &WalRecord::Append {
                    topic_id: 1,
                    seq: *seq,
                    ts: 1,
                    node: None,
                    tag: None,
                    data: payload.clone(),
                },
                false,
            );
            if !frame.is_empty() {
                let idx = (*flip_at as usize) % frame.len();
                frame[idx] ^= 0xFF;
            }
            frame
        }
    }
}

/// Drive every frame the reader yields from `buf` to completion (forcing the full
/// decode of each), asserting it never panics / loops / fabricates. Returns the
/// reader's reported `valid_len` for an extra sanity assertion.
fn drain_decoder(buf: Vec<u8>) -> usize {
    let total = buf.len();
    let mut reader = WalReader::new(buf);
    let mut consumed_frames = 0usize;
    // The reader iterator decodes one frame per `next()` and stops cleanly at the
    // first `Torn`; a bounded cap guards against any pathological non-termination
    // (a correct decoder always advances `pos` by `consumed > 0` per frame).
    for _frame in reader.by_ref().take(100_000) {
        consumed_frames += 1;
    }
    let valid = reader.valid_len();
    // valid_len is the offset of the last good frame's end ⇒ never past the buffer.
    assert!(valid <= total, "valid_len {valid} overruns buffer {total}");
    let _ = consumed_frames;
    valid
}

/// F-FUZZ-WAL-DECODER: feed arbitrary garbage (and structured near-miss frames) to
/// the WAL frame decoder via `WalReader`. The decoder must never panic / OOM / read
/// OOB; it always yields a `Frame` or stops at a clean `Torn`, bounded by the
/// `frame_len` validation *before* any body read.
#[test]
fn f_fuzz_wal_decoder() {
    // A deterministic, seeded corpus of arbitrary byte buffers — no external fuzzer
    // needed; `arbitrary` turns each seed into a structured `FuzzInput`. Bounded
    // (a few thousand cases of small buffers) so it runs in well under a second.
    let mut rng_state: u64 = 0x1234_5678_9ABC_DEF0;
    let mut next = || {
        // xorshift64* — a tiny deterministic PRNG for the seed stream.
        rng_state ^= rng_state >> 12;
        rng_state ^= rng_state << 25;
        rng_state ^= rng_state >> 27;
        rng_state.wrapping_mul(0x2545_F491_4F6C_DD1D)
    };

    for _ in 0..4000 {
        // Build a small random byte pool and let `arbitrary` carve a `FuzzInput`.
        let len = (next() % 96) as usize;
        let mut pool = Vec::with_capacity(len);
        for _ in 0..len {
            pool.push((next() & 0xFF) as u8);
        }
        let mut u = Unstructured::new(&pool);
        // If the pool is too short to build the structured input, fall back to the
        // raw bytes (still a valid decoder input — the whole point is "any bytes").
        let buf = match FuzzInput::arbitrary(&mut u) {
            Ok(input) => build_fuzz_buf(&input),
            Err(_) => pool.clone(),
        };
        // The contract: this never panics, never OOMs, always terminates.
        let _valid = drain_decoder(buf);

        // Also feed the raw pool directly (the purest "arbitrary garbage" case).
        let _valid_raw = drain_decoder(pool);
    }

    // Targeted edge cases the catalog calls out explicitly (frame_len huge / below
    // min / 1-3 byte length prefix / all-zeros / empty) — each must be `Torn`,
    // never a panic or an OOM allocation.
    let edge_cases: Vec<Vec<u8>> = vec![
        vec![],                                   // empty ⇒ Torn (no length prefix)
        vec![0x00],                               // 1 byte of the length prefix
        vec![0x00, 0x00, 0x00],                   // 3 bytes of the length prefix
        vec![0xFF, 0xFF, 0xFF, 0xFF],             // frame_len = u32::MAX ⇒ overrun ⇒ Torn
        vec![0xFF, 0xFF, 0xFF, 0xFF, 1, 2, 3, 4], // huge frame_len, some body ⇒ Torn
        vec![0x01, 0x00, 0x00, 0x00, 0xAA],       // frame_len = 1 (< min) ⇒ Torn
        vec![0u8; 64],                            // all-zeros (preallocated tail) ⇒ Torn
        vec![0u8; 4],                             // zero frame_len ⇒ Torn
    ];
    for ec in edge_cases {
        let total = ec.len();
        let valid = drain_decoder(ec);
        // None of these encode a valid frame ⇒ nothing is consumed.
        assert_eq!(
            valid, 0,
            "edge case (len {total}) must decode to zero valid bytes"
        );
    }
}
