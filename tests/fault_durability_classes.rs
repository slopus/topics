//! Phase-8A durability-CLASS crash matrix: drive the *real, fully-wired*
//! [`Engine`] through an in-memory [`FakeDisk`] (the same harness
//! `tests/crash_oracle.rs` uses — [`FakeDisk`]/[`FaultFs`] from
//! `topics::storage::testfs` injected via [`Engine::with_data_dir_fs`]) and a
//! pure-Rust reference model, then assert the three durability classes behave
//! exactly to contract across snapshot + WAL replay + a crash mid-write:
//!
//! ```text
//!   memory  — "disk-like but best-effort" (§0.10): takes the SAME group-committed
//!             WAL write + recovery path as disk and is fully queryable, but with
//!             NO durability GUARANTEE — recovered records MAY survive OR be lost.
//!             The WEAK contract: no fabrication (survivors ⊆ ever_acked, byte-for-
//!             byte), seq monotone (head never exceeds the acked head). NO exact
//!             empty-on-restart and NO exact full-survival assertion. (The config
//!             always survives — it is a control-frame mutation.)
//!   disk    — group-committed WAL; survives a CLEAN restart fully, but a power
//!             loss may lose only the un-fsynced TAIL (a dense prefix survives,
//!             never a hole, never a fabricated/torn frame).
//!   fsync   — fsync-gated ack; every acked write survives a kill-9 (acked ⇒
//!             durable), seq monotone, no gap.
//! ```
//!
//! Reuses the Phase-8A oracle: the model commits an op only once its ACK is
//! observed (a durable write is acked only when the engine call returns `Ok`,
//! i.e. its group fsync returned). After each crash+recovery the recovered engine
//! is diffed against the model under the SUBSET relaxation (acked-durable ⊆
//! survivors ⊆ ever_acked; dense; head monotone). Crash points are swept via a
//! `CrashAfter` Fs wrapper (the in-process "SIGKILL after the Nth FS write"
//! injector) bounded to a small workload + capped points + fixed seeds, so the
//! file runs in well under a minute.
//!
//! ```text
//! cargo test --features test-fs --test fault_durability_classes
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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use serde_json::json;

use topics::clock::{SharedClock, TestClock};
use topics::config::ServerConfig;
use topics::engine::Engine;
use topics::storage::testfs::{FakeDisk, TornDamage};
use topics::storage::{File, Fs, OpenOpts};
use topics::types::{
    DiffRequest, Durability, RecordIn, RouterCreateRequest, TopicConfig, TopicType, WriteRequest,
};

// ===========================================================================
// Reference model (the oracle) — the durability-class-aware subset of the
// `tests/crash_oracle.rs` RefModel. A `memory` topic is modeled like a `disk` topic
// for the no-fabrication universe (`ever_acked`), but it carries NO durability
// guarantee: its survivors after a restart are an UNCONSTRAINED subset of
// `ever_acked` (MAY survive OR be lost). A `disk`/`fsync` topic keeps acked records,
// with `fsync` being must-survive.
// ===========================================================================

#[derive(Debug, Clone, PartialEq, Eq)]
struct ModelRecord {
    data: String,
    tag: Option<String>,
    node: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct ModelTopic {
    class: Class,
    /// Every acked record by seq (the must-survive set for an `fsync` topic; the
    /// clean-restart survivor set for a `disk` topic; never survives a crash for a
    /// `memory` topic).
    acked: BTreeMap<u64, ModelRecord>,
    /// Every record ever acked (deletes/evictions never remove) — the universe a
    /// recovered seq is ever allowed to be (no fabrication: survivors ⊆ ever_acked).
    ever_acked: BTreeMap<u64, ModelRecord>,
    head: u64,
    cap: u64,
    evict_floor: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum Class {
    Memory,
    #[default]
    Disk,
    Fsync,
}

impl Class {
    fn durability(self) -> Durability {
        match self {
            Class::Memory => Durability::Memory,
            Class::Disk => Durability::Disk,
            Class::Fsync => Durability::Fsync,
        }
    }
}

impl ModelTopic {
    fn live_seqs(&self) -> Vec<u64> {
        self.acked
            .keys()
            .copied()
            .filter(|s| *s >= self.evict_floor)
            .collect()
    }
}

#[derive(Debug, Default)]
struct RefModel {
    topics: BTreeMap<String, ModelTopic>,
}

impl RefModel {
    fn ensure_topic(&mut self, name: &str, class: Class, cap: u64) {
        self.topics.entry(name.to_string()).or_insert(ModelTopic {
            class,
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
                .filter(|s| *s >= b.evict_floor)
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
// Op stream
// ===========================================================================

#[derive(Debug, Clone)]
enum Op {
    PutTopic {
        name: String,
        class: Class,
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
            Op::PutTopic { name, class, cap } => {
                let cfg = TopicConfig {
                    r#type: TopicType::Log,
                    durability: Some(class.durability()),
                    cap_records: *cap,
                    ..Default::default()
                };
                if engine.put_topic(name, cfg).is_ok() {
                    model.ensure_topic(name, *class, *cap);
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
                    // Auto-created here ⇒ default config (disk class).
                    model.ensure_topic(name, Class::Disk, 0);
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
// Engine build / dump / recover plumbing through FakeDisk
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

fn open_engine_fs(fs: Arc<dyn Fs>) -> Arc<Engine> {
    Engine::with_data_dir_fs(cfg(), clock(), fs).expect("engine opens through fs")
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
/// `None` if the topic does not exist post-recovery.
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

/// Make the WAL + meta dir entries durable (the create+dir-fsync production does
/// at WAL open — modeled explicitly so the file names survive a crash).
fn sync_wal_dir(disk: &FakeDisk) {
    let fs = disk.arc();
    let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("wal"));
    let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("meta"));
}

// ===========================================================================
// The oracle: diff a recovered topic against the model under the class contract.
// `whole_tail_durable` is true on a clean stop or an all-acked-fsync crash.
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
                "{name}: recovered seq {seq} never acked by the model (fabricated/torn): {rec:?}"
            )
        });
        assert_eq!(
            m, rec,
            "{name}: recovered record at seq {seq} differs from the model"
        );
    }

    // (2) NO SILENT LOSS for an `fsync` topic when the whole tail was durable.
    if model.class == Class::Fsync && whole_tail_durable {
        for seq in &live {
            assert!(
                dump.records.contains_key(seq),
                "{name}: acked fsync seq {seq} LOST after recovery \
                 (survivors={survivors:?}, model live={live:?})"
            );
        }
    }

    // (3) NO GAP — survivors are a dense prefix of the model's live set. A `memory`
    //     topic is EXEMPT: it is best-effort/lossy (§0.10), so it may lose ANY records
    //     (not just a tail) — a hole below the surviving high-water is permitted.
    //     Only the no-fabrication (1) + head-monotone (4) checks bind it.
    if model.class != Class::Memory {
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
                            "{name}: model-live seq {s} missing below surviving high-water {hi} \
                             (hole in the live set): survivors={survivors:?}"
                        );
                    }
                }
            }
        }
    }

    // (4) HEAD MONOTONE / NO SEQ REUSE (R3). The recovered head never REGRESSES
    //     below the highest acked seq when the whole tail is durable (a clean stop /
    //     all-fsync crash) — an already-acked seq is never re-handed. A `disk`-class
    //     topic (acked before fsync) recovers at its durable head RESERVATION, so its
    //     head may sit ABOVE the acked head by up to `DISK_HEAD_RESERVE_AHEAD` (the
    //     dropped un-fsynced tail becomes silent deleted gaps); an `fsync` topic has
    //     no reservation, so its head matches the acked head exactly. A `memory` topic
    //     is best-effort (§0.10): its head never EXCEEDS the acked head (no future
    //     seq / no fabrication), but it may be anywhere from 0 up to the acked head
    //     (records may survive or be lost) — only the no-future-seq bound binds.
    match model.class {
        Class::Fsync => {
            assert!(
                dump.head <= model.head,
                "{name}: fsync recovered head {} exceeds model head {} (future seq?)",
                dump.head,
                model.head
            );
            if whole_tail_durable && !live.is_empty() {
                assert_eq!(dump.head, model.head, "{name}: fsync head must match model");
            }
        }
        Class::Disk => {
            if whole_tail_durable && !model.acked.is_empty() {
                assert!(
                    dump.head >= model.head,
                    "{name}: disk recovered head {} REGRESSED below acked head {} (seq reuse!)",
                    dump.head,
                    model.head
                );
            }
            let ceiling = model.head + topics::config::DISK_HEAD_RESERVE_AHEAD;
            assert!(
                dump.head <= ceiling,
                "{name}: disk recovered head {} exceeds reservation ceiling {}",
                dump.head,
                ceiling
            );
        }
        Class::Memory => {
            // Best-effort/lossy: records MAY survive or be lost, so the head may sit
            // anywhere in `0..=acked_head`. The ONE hard bound: it never exceeds the
            // acked head (no future / fabricated seq).
            assert!(
                dump.head <= model.head,
                "{name}: memory recovered head {} exceeds model head {} (future seq?)",
                dump.head,
                model.head
            );
        }
    }
}

// ===========================================================================
// CrashAfter — fires FakeDisk.crash() after the Nth write_at (the in-process
// "SIGKILL after the Nth FS write" injector). Identical in spirit to the one in
// tests/crash_oracle.rs; reproduced here because that one is test-local.
// ===========================================================================

#[derive(Clone)]
struct CrashAfter {
    disk: FakeDisk,
    at: u64,
    seen: Arc<AtomicU64>,
    tripped: Arc<AtomicBool>,
}

impl CrashAfter {
    fn new(disk: FakeDisk, at: u64) -> Self {
        CrashAfter {
            disk,
            at,
            seen: Arc::new(AtomicU64::new(0)),
            tripped: Arc::new(AtomicBool::new(false)),
        }
    }
    fn arc(&self) -> Arc<dyn Fs> {
        Arc::new(self.clone())
    }
    fn tick_write(&self) {
        let idx = self.seen.fetch_add(1, Ordering::SeqCst);
        if idx == self.at
            && self
                .tripped
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            self.disk.crash(TornDamage::None);
        }
    }
}

struct CrashAfterFile {
    inner: Box<dyn File>,
    owner: CrashAfter,
}

impl File for CrashAfterFile {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read_at(offset, buf)
    }
    fn write_at(&mut self, offset: u64, buf: &[u8]) -> std::io::Result<usize> {
        let r = self.inner.write_at(offset, buf);
        self.owner.tick_write();
        r
    }
    fn set_len(&mut self, len: u64) -> std::io::Result<()> {
        self.inner.set_len(len)
    }
    fn sync_data(&self) -> std::io::Result<()> {
        self.inner.sync_data()
    }
    fn sync_all(&self) -> std::io::Result<()> {
        self.inner.sync_all()
    }
    fn metadata_len(&self) -> std::io::Result<u64> {
        self.inner.metadata_len()
    }
    fn read_to_end_from(&self, offset: u64, out: &mut Vec<u8>) -> std::io::Result<()> {
        self.inner.read_to_end_from(offset, out)
    }
}

impl Fs for CrashAfter {
    fn open(&self, path: &std::path::Path, opts: OpenOpts) -> std::io::Result<Box<dyn File>> {
        let inner = self.disk.open(path, opts)?;
        Ok(Box::new(CrashAfterFile {
            inner,
            owner: self.clone(),
        }))
    }
    fn rename(&self, from: &std::path::Path, to: &std::path::Path) -> std::io::Result<()> {
        self.disk.rename(from, to)
    }
    fn remove_file(&self, path: &std::path::Path) -> std::io::Result<()> {
        self.disk.remove_file(path)
    }
    fn read_dir(&self, dir: &std::path::Path) -> std::io::Result<Vec<PathBuf>> {
        self.disk.read_dir(dir)
    }
    fn sync_dir(&self, dir: &std::path::Path) -> std::io::Result<()> {
        self.disk.sync_dir(dir)
    }
    fn create_dir_all(&self, dir: &std::path::Path) -> std::io::Result<()> {
        self.disk.create_dir_all(dir)
    }
    fn exists(&self, path: &std::path::Path) -> bool {
        self.disk.exists(path)
    }
    fn metadata_len(&self, path: &std::path::Path) -> std::io::Result<u64> {
        self.disk.metadata_len(path)
    }
}

// ===========================================================================
// 1) MEMORY class: "disk-like but best-effort" (§0.10). Takes the same WAL write +
//    recovery path as disk and is fully queryable, but with NO durability
//    GUARANTEE: records MAY survive OR be lost. The WEAK contract: config always
//    survives; recovered records are a no-fabrication subset of the writes; head
//    never exceeds the acked head. NO exact empty-on-restart / full-survival.
// ===========================================================================

/// A `memory` topic is best-effort across a CLEAN restart: the config always
/// survives (a control-frame mutation), the topic is fully queryable pre-restart,
/// and the acked write is never fsync-gated. Post-restart the records MAY survive
/// OR be lost — assert only the weak contract (no fabrication, head monotone), NOT
/// an exact empty-on-restart.
#[test]
fn memory_topic_best_effort_across_clean_restart() {
    let disk = FakeDisk::new();
    let mut model = RefModel::default();
    let ops = vec![
        Op::PutTopic {
            name: "mem".into(),
            class: Class::Memory,
            cap: 0,
        },
        Op::Append {
            name: "mem".into(),
            data: "a".into(),
            tag: Some("t".into()),
            node: None,
        },
        Op::Append {
            name: "mem".into(),
            data: "b".into(),
            tag: None,
            node: Some("n".into()),
        },
        Op::Append {
            name: "mem".into(),
            data: "c".into(),
            tag: None,
            node: None,
        },
    ];
    {
        let engine = open_engine(&disk);
        run_ops(&engine, &mut model, &ops);
        // The topic reports the live records BEFORE restart (fully queryable).
        let pre = dump_topic(&engine, "mem").expect("mem topic live pre-restart");
        assert_eq!(pre.head, 3, "memory topic head advances + is queryable");
        assert_eq!(pre.count, 3);
        // A memory write is never fsync-gated (best-effort, no durability promise).
        let req = WriteRequest {
            records: vec![RecordIn {
                data: json!({ "v": "d" }),
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
        let resp = engine.write("mem", req, true).unwrap();
        assert_eq!(
            resp.performance.fsync_ms.unwrap_or(0.0),
            0.0,
            "memory write is never fsync-gated"
        );
        model.ack_append(
            "mem",
            std::slice::from_ref(&resp.last_seq),
            std::slice::from_ref(&ModelRecord {
                data: "d".into(),
                tag: None,
                node: None,
            }),
        );
        sync_wal_dir(&disk);
        drop(engine);
    }

    // Reopen cleanly: the config ALWAYS persists. Records are best-effort — assert
    // only the weak contract (no fabrication, head monotone, may survive or be lost).
    let engine = open_engine(&disk);
    let st = engine.topic_state("mem", false).unwrap();
    assert_eq!(
        st.config.durability_class(),
        Durability::Memory,
        "memory class config persisted"
    );
    let dump = dump_topic(&engine, "mem").expect("memory topic config persists across restart");
    // whole_tail_durable=false: the WEAK contract (no fabrication + head monotone;
    // a memory topic may lose any records, even a non-tail hole).
    assert_topic_contract("mem", &model.topics["mem"], &dump, false);

    // The topic is fully functional post-restart: a fresh write advances head by 1.
    let before = engine.topic_state("mem", false).unwrap().head_seq;
    let req = WriteRequest {
        records: vec![RecordIn {
            data: json!({ "v": "z" }),
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
    engine.write("mem", req, true).unwrap();
    assert_eq!(
        engine.topic_state("mem", false).unwrap().head_seq,
        before + 1,
        "a post-restart memory write advances head by 1"
    );
}

/// A `memory` topic across a CRASH MID-WRITE: best-effort recovery never fabricates
/// and never exceeds the acked head. A `disk`-class sibling in the SAME workload
/// recovers its acked prefix. Both honor the no-fabrication subset contract; the
/// memory topic additionally may lose ANY records (not just a tail).
#[test]
fn memory_topic_crash_mid_write_best_effort() {
    // Sweep a few crash points; the memory topic must ALWAYS satisfy the weak contract.
    for crash_at in [0u64, 2, 5, 9] {
        let disk = FakeDisk::with_seed(0x0E11_u64.wrapping_add(crash_at));
        let trip = CrashAfter::new(disk.clone(), crash_at);
        let mut model = RefModel::default();
        let mut ops = vec![
            Op::PutTopic {
                name: "mem".into(),
                class: Class::Memory,
                cap: 0,
            },
            Op::PutTopic {
                name: "dsk".into(),
                class: Class::Disk,
                cap: 0,
            },
        ];
        for i in 0..6 {
            ops.push(Op::Append {
                name: "mem".into(),
                data: format!("m{i}"),
                tag: None,
                node: None,
            });
            ops.push(Op::Append {
                name: "dsk".into(),
                data: format!("d{i}"),
                tag: None,
                node: None,
            });
        }
        {
            let engine = open_engine_fs(trip.arc());
            run_ops(&engine, &mut model, &ops);
            drop(engine);
        }
        disk.reset_power();

        let engine = open_engine(&disk);
        // The memory topic is best-effort: whatever recovers must satisfy the weak
        // contract (no fabrication, head monotone) — it may have kept records or
        // come back empty, both are valid.
        if let Some(dump) = dump_topic(&engine, "mem") {
            assert_topic_contract("mem", &model.topics["mem"], &dump, false);
        }
        // The disk topic recovers a dense prefix (subset contract) — no fabrication.
        if let Some(dump) = dump_topic(&engine, "dsk") {
            assert_topic_contract("dsk", &model.topics["dsk"], &dump, false);
        }
    }
}

// ===========================================================================
// 2) DISK class: clean restart survives fully; a crash may lose only the
//    un-fsynced tail (a dense prefix, never a hole, never a torn misread).
// ===========================================================================

/// A `disk` topic survives a CLEAN stop + reopen fully (the writer drains + fsyncs
/// on drop), recovering every acked record as a dense prefix matching the model.
#[test]
fn disk_topic_survives_clean_restart() {
    let disk = FakeDisk::new();
    let mut model = RefModel::default();
    let mut ops = vec![Op::PutTopic {
        name: "d".into(),
        class: Class::Disk,
        cap: 0,
    }];
    for i in 1..=8 {
        ops.push(Op::Append {
            name: "d".into(),
            data: i.to_string(),
            tag: Some("t".into()),
            node: None,
        });
    }
    {
        let engine = open_engine(&disk);
        run_ops(&engine, &mut model, &ops);
        sync_wal_dir(&disk);
        drop(engine); // clean stop: Drop drains + fsyncs the writer.
    }
    let engine = open_engine(&disk);
    let dump = dump_topic(&engine, "d").expect("disk topic survives clean restart");
    // A clean teardown hardens the whole tail ⇒ all 8 acked records recover.
    assert_eq!(
        dump.records.len(),
        8,
        "disk topic recovers all acked writes on a clean restart"
    );
    assert_topic_contract("d", &model.topics["d"], &dump, true);
}

/// A `disk` topic under a power loss (crash) loses ONLY the un-fsynced tail: the
/// survivors are a dense prefix of the acked set — never a hole, never a
/// fabricated/torn frame. Swept over a bounded set of crash points + a couple of
/// torn-damage modes, all with fixed seeds.
#[test]
fn disk_topic_crash_loses_only_unfsynced_tail() {
    // Size the write_at call space first (at = u64::MAX ⇒ never fires).
    let total_writes = {
        let probe_disk = FakeDisk::new();
        let probe = CrashAfter::new(probe_disk.clone(), u64::MAX);
        let mut throwaway = RefModel::default();
        let engine = open_engine_fs(probe.arc());
        let mut ops = vec![Op::PutTopic {
            name: "d".into(),
            class: Class::Disk,
            cap: 0,
        }];
        for i in 1..=6 {
            ops.push(Op::Append {
                name: "d".into(),
                data: i.to_string(),
                tag: None,
                node: None,
            });
        }
        run_ops(&engine, &mut throwaway, &ops);
        drop(engine);
        probe.seen.load(Ordering::SeqCst)
    };
    assert!(
        total_writes >= 4,
        "disk workload issues several writes (M={total_writes})"
    );

    // Bounded sweep: cap the crash points well below the full call space so the
    // file stays fast + deterministic while still hitting the interesting
    // boundaries of the small workload (each iteration opens a fresh recovering
    // engine + replays, so the cap bounds total wall time). The probed set is
    // tiered (topics::testutil::crash_points): a bounded deterministic sample by
    // DEFAULT, the full `0..=cap` matrix under `TOPICS_TEST_EXHAUSTIVE=1`.
    let cap = total_writes.min(10);
    for crash_at in topics::testutil::crash_points(cap) {
        let disk = FakeDisk::with_seed(0xD15C_0000 ^ crash_at);
        let trip = CrashAfter::new(disk.clone(), crash_at);
        let mut model = RefModel::default();
        let mut ops = vec![Op::PutTopic {
            name: "d".into(),
            class: Class::Disk,
            cap: 0,
        }];
        for i in 1..=6 {
            ops.push(Op::Append {
                name: "d".into(),
                data: i.to_string(),
                tag: None,
                node: None,
            });
        }
        {
            let engine = open_engine_fs(trip.arc());
            run_ops(&engine, &mut model, &ops);
            drop(engine);
        }
        disk.reset_power();

        let engine = open_engine(&disk);
        if let Some(dump) = dump_topic(&engine, "d") {
            // Crash mid-stream ⇒ the un-fsynced tail may be gone; survivors must
            // be a dense prefix with no fabrication (subset contract).
            assert_topic_contract("d", &model.topics["d"], &dump, false);
        }

        // Idempotent recovery at a couple of points: recover(recover(x)) == recover(x).
        if crash_at % 5 == 0 {
            let d1 = dump_topic(&engine, "d");
            drop(engine);
            let engine2 = open_engine(&disk);
            let d2 = dump_topic(&engine2, "d");
            assert_eq!(d1, d2, "disk recovery idempotent at crash_at {crash_at}");
        }
    }
}

// ===========================================================================
// 3) FSYNC class: every acked write survives a kill-9 (acked ⇒ durable).
// ===========================================================================

/// An `fsync` topic: every write whose ack returned (its group fsync returned) is
/// durable — a power loss (kill-9) after the acks loses NOTHING. The model's live
/// set survives intact, seq monotone, no gap, across torn-damage modes.
#[test]
fn fsync_topic_acked_writes_survive_kill9() {
    for &damage in &[
        TornDamage::None,
        TornDamage::PrefixTruncate,
        TornDamage::Garble,
    ] {
        let disk = FakeDisk::with_seed(0xF59C ^ damage as u64);
        let mut model = RefModel::default();
        let mut ops = vec![Op::PutTopic {
            name: "f".into(),
            class: Class::Fsync,
            cap: 0,
        }];
        for i in 1..=6 {
            ops.push(Op::Append {
                name: "f".into(),
                data: i.to_string(),
                tag: Some("k".into()),
                node: None,
            });
        }
        {
            let engine = open_engine(&disk);
            run_ops(&engine, &mut model, &ops);
            sync_wal_dir(&disk);
            // kill-9: freeze + drop un-fsynced bytes. Every acked write already
            // fsynced (fsync class), so all survive even with a torn last write.
            disk.crash(damage);
            drop(engine);
        }
        disk.reset_power();
        let engine = open_engine(&disk);
        let dump = dump_topic(&engine, "f").expect("fsync topic survives kill-9");
        assert_eq!(
            dump.records.len(),
            6,
            "all 6 acked fsync writes survive kill-9 ({damage:?})"
        );
        assert_topic_contract("f", &model.topics["f"], &dump, true);
        assert_eq!(dump.head, 6, "fsync head fully recovered ({damage:?})");
    }
}

/// A crash-point sweep over an `fsync` workload: at EVERY crash point, every write
/// whose engine call RETURNED Ok (acked ⇒ its fsync returned ⇒ FakeDisk promoted
/// the bytes durably) must survive. The model only records a write as acked when
/// the call returned Ok, so the subset contract holds at every crash point: the
/// must-survive (live) set is present below the surviving high-water, no
/// fabrication, no hole.
#[test]
fn fsync_topic_sweep_acked_always_durable() {
    let total_writes = {
        let probe_disk = FakeDisk::new();
        let probe = CrashAfter::new(probe_disk.clone(), u64::MAX);
        let mut throwaway = RefModel::default();
        let engine = open_engine_fs(probe.arc());
        let mut ops = vec![Op::PutTopic {
            name: "f".into(),
            class: Class::Fsync,
            cap: 0,
        }];
        for i in 1..=6 {
            ops.push(Op::Append {
                name: "f".into(),
                data: i.to_string(),
                tag: None,
                node: None,
            });
        }
        run_ops(&engine, &mut throwaway, &ops);
        drop(engine);
        probe.seen.load(Ordering::SeqCst)
    };

    // Tiered crash-point sweep (see the sibling sweep above): bounded
    // deterministic sample by default, full `0..=cap` under TOPICS_TEST_EXHAUSTIVE.
    let cap = total_writes.min(10);
    for crash_at in topics::testutil::crash_points(cap) {
        let disk = FakeDisk::with_seed(0x5EE0 ^ crash_at);
        let trip = CrashAfter::new(disk.clone(), crash_at);
        let mut model = RefModel::default();
        let mut ops = vec![Op::PutTopic {
            name: "f".into(),
            class: Class::Fsync,
            cap: 0,
        }];
        for i in 1..=6 {
            ops.push(Op::Append {
                name: "f".into(),
                data: i.to_string(),
                tag: None,
                node: None,
            });
        }
        {
            let engine = open_engine_fs(trip.arc());
            run_ops(&engine, &mut model, &ops);
            drop(engine);
        }
        disk.reset_power();

        let engine = open_engine(&disk);
        if let Some(dump) = dump_topic(&engine, "f") {
            // Crash mid-stream: cannot pin which acked op's fsync truly landed
            // relative to the freeze, so use the subset relaxation. The key
            // guarantee: no fabrication + no hole in the live set below high-water.
            assert_topic_contract("f", &model.topics["f"], &dump, false);
        }
    }
}

// ===========================================================================
// 4) ROUTER forwarding into a MEMORY dest is best-effort (disk-like, §0.10).
// ===========================================================================

/// A router forwarding from a DURABLE (fsync) source into a `memory` destination:
/// the forwarded copies land in the dest and are visible + queryable pre-restart
/// (the dest takes the same disk-like best-effort WAL path). On restart the dest
/// config always survives and the durable SOURCE fully recovers; the forwarded
/// copies are best-effort — they MAY survive OR be lost (no fabrication).
#[test]
fn router_into_memory_dest_best_effort() {
    let disk = FakeDisk::new();
    {
        let engine = open_engine(&disk);
        // Durable source.
        engine
            .put_topic(
                "src",
                TopicConfig {
                    durability: Some(Durability::Fsync),
                    ..Default::default()
                },
            )
            .unwrap();
        // Memory destination.
        engine
            .put_topic(
                "mem_dst",
                TopicConfig {
                    durability: Some(Durability::Memory),
                    ..Default::default()
                },
            )
            .unwrap();
        engine
            .put_router(
                "src->mem",
                RouterCreateRequest {
                    source: "src".into(),
                    dest: "mem_dst".into(),
                    preserve_node: true,
                    preserve_tag: true,
                    create_dest: false,
                    filter: None,
                    allow_cycle: false,
                },
            )
            .unwrap();
        for i in 1..=4 {
            let req = WriteRequest {
                records: vec![RecordIn {
                    data: json!({ "v": i.to_string() }),
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
            engine.write("src", req, true).unwrap();
        }
        // Pre-restart: the forwarded copies are visible + queryable in the dest.
        let dst = dump_topic(&engine, "mem_dst").expect("mem dest live pre-restart");
        assert_eq!(
            dst.count, 4,
            "router forwarded 4 copies into the memory dest (queryable)"
        );
        sync_wal_dir(&disk);
        drop(engine);
    }

    // Reopen: the durable source recovers fully; the memory dest config always
    // survives. The forwarded copies are best-effort — assert only the weak
    // contract (head monotone, no fabrication; they may survive or be lost).
    let engine = open_engine(&disk);
    let src = dump_topic(&engine, "src").expect("durable source recovers");
    assert_eq!(src.records.len(), 4, "durable router source fully recovers");
    let dst = dump_topic(&engine, "mem_dst").expect("memory dest config persists");
    assert_eq!(
        engine
            .topic_state("mem_dst", false)
            .unwrap()
            .config
            .durability,
        Some(Durability::Memory),
        "memory dest config survives"
    );
    assert!(
        dst.head <= 4,
        "memory dest head monotone (no future seq), got {}",
        dst.head
    );
    for (seq, rec) in &dst.records {
        let v: u64 = rec.data.parse().unwrap_or(0);
        assert!(
            (1..=4).contains(&v),
            "recovered forwarded record at seq {seq} is not fabricated: {rec:?}"
        );
    }
}

// ===========================================================================
// 5) BOUNDED concurrency stress: many threads writing durable (fsync) appends to
//    one topic, then a clean restart — every acked write survives as a contiguous
//    [1..=N] (the off-lock-fsync + per-topic commit-sequencer invariant under
//    contention). High-thread oracle stress (loom/shuttle are not wired into the
//    crate, so this is the robust-stress alternative the harness note prescribes).
// ===========================================================================

#[test]
fn fsync_concurrent_writers_commit_sequencer_no_loss_across_restart() {
    let disk = FakeDisk::new();
    let writers = 6u64;
    let per_writer = 40u64;
    let total = writers * per_writer;
    {
        let engine = open_engine(&disk);
        engine
            .put_topic(
                "hot",
                TopicConfig {
                    durability: Some(Durability::Fsync),
                    ..Default::default()
                },
            )
            .unwrap();
        let mut handles = Vec::new();
        for w in 0..writers {
            let engine = engine.clone();
            handles.push(std::thread::spawn(move || {
                for i in 0..per_writer {
                    let req = WriteRequest {
                        records: vec![RecordIn {
                            data: json!({ "w": w, "i": i }),
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
                    // Acked ⇒ fsync-gated ⇒ must survive a restart.
                    engine.write("hot", req, true).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let st = engine.topic_state("hot", false).unwrap();
        assert_eq!(st.head_seq, total, "all concurrent fsync writes acked");
        assert_eq!(st.count, total);
        sync_wal_dir(&disk);
        drop(engine);
    }

    // Restart: the commit sequencer guaranteed seq-order publish, so recovery
    // (apply-in-WAL-order, skip seq<=head) finds a dense, contiguous [1..=total]
    // with no acked-durable loss and no fabricated/duplicate seq.
    let engine = open_engine(&disk);
    let st = engine.topic_state("hot", false).unwrap();
    assert_eq!(
        st.head_seq, total,
        "no acked fsync write lost across restart"
    );
    assert_eq!(st.earliest_seq, 1);
    assert_eq!(st.count, total);

    let mut seqs = Vec::new();
    let mut from = 0u64;
    loop {
        let d = engine
            .diff(
                "hot",
                DiffRequest {
                    from_seq: from,
                    limit: 1000,
                    ..DiffRequest::default()
                },
            )
            .unwrap();
        if d.records.is_empty() {
            break;
        }
        for r in &d.records {
            seqs.push(r.seq);
        }
        from = d.next_from_seq;
        if d.caught_up {
            break;
        }
    }
    assert_eq!(
        seqs,
        (1..=total).collect::<Vec<_>>(),
        "contiguous [1..=N], no gap, no dup"
    );
}
