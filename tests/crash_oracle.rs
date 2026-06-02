//! Phase-4 Stage-3 **model-oracle crash harness**: drive the *real, fully-wired*
//! [`Engine`] through an in-memory [`FakeDisk`], advance a pure-Rust reference
//! model by the same op stream, then — after an injected fault + recovery — diff
//! the recovered engine against the model under the crash-consistency contract:
//!
//! ```text
//!   acked-durable  ⊆  survivors  ⊆  model ∪ acked          (no silent loss / no fabrication)
//!   seq monotonic + contiguous (a dense [earliest..=head] minus only deleted)
//!   replay idempotent: recover(recover(x)) == recover(x)
//!   torn tail truncated, never misread
//!   delete never resurrects; floors preserved + monotone
//! ```
//!
//! The model commits an op **only once its ACK is observed** (a durable write is
//! acked only when the engine call returns `Ok`, i.e. its group fsync returned),
//! so the model's `acked` set is exactly the must-survive set. After every
//! crash+recovery the recovered engine state (head / earliest / count / per-seq
//! data·tag·node / tombstone) is compared against the model with the SUBSET
//! relaxation: unacked writes *may* be absent, but no acked write may be lost, no
//! record may be fabricated, and no gap may open among survivors.
//!
//! Gated behind `test-fs` (the hostile `Fs` impls live there). The exhaustive
//! crash-point sweeps additionally drive the named fail-rs sites but do not
//! *require* `failpoints`; they reset to the pre-op durable image and `crash()`
//! after the Nth FS mutating call, which is the harness-level crash injector.
//!
//! ```text
//! cargo test --features test-fs --test crash_oracle -- --test-threads=1
//! ```

#![cfg(feature = "test-fs")]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;

use topics::clock::{SharedClock, TestClock};
use topics::config::ServerConfig;
use topics::engine::Engine;
use topics::storage::testfs::{FakeDisk, FaultFs, FaultKind, FaultOp, TornDamage};
use topics::storage::Fs;
use topics::types::{
    DeleteRequest, DiffRequest, Filter, RecordIn, TopicConfig, TopicType, WriteRequest,
};

// ===========================================================================
// The pure-Rust reference model (the oracle)
// ===========================================================================

/// One record as the model tracks it — exactly the fields the durability
/// contract must preserve byte-for-byte across a crash (DESIGN §0.3): the opaque
/// `data` payload, the `$tag`, and the `$node`. (`$ts` is asserted only as
/// "present"; the model does not pin wall-clock.)
#[derive(Debug, Clone, PartialEq, Eq)]
struct ModelRecord {
    data: String,
    tag: Option<String>,
    node: Option<String>,
}

/// The model of one topic: the acked records by seq, the floors a crash must
/// preserve, and whether the topic is durable (only durable acked writes are
/// must-survive; a non-durable acked write *may* vanish as a clean tail).
#[derive(Debug, Clone, Default)]
struct ModelTopic {
    durable: bool,
    /// Every acked record by its assigned seq (the must-survive set for a durable
    /// topic). A delete removes from here; an eviction advances `evict_floor` and
    /// drops below it.
    acked: BTreeMap<u64, ModelRecord>,
    /// Every record ever acked, by seq — deletes and evictions never remove from
    /// here. This is the universe of seqs recovery is ever *allowed* to surface:
    /// a delete whose WAL control frame was lost to a power loss may not take
    /// effect, so a previously-deleted seq may legitimately reappear after a crash
    /// (the documented best-effort durability of an un-fsynced control frame).
    /// "No fabrication" is `survivors ⊆ ever_acked`.
    ever_acked: BTreeMap<u64, ModelRecord>,
    /// Highest seq ever assigned to an acked write (head never regresses).
    head: u64,
    /// Voluntary-delete front floor: records with `seq < delete_floor` are gone
    /// **silently** (no tombstone). Monotone.
    delete_floor: u64,
    /// Involuntary cap/TTL floor: records below it are gone with a **tombstone**.
    /// Monotone.
    evict_floor: u64,
    /// Topic record cap (0 = unbounded); used to predict the evict floor.
    cap: u64,
}

impl ModelTopic {
    /// The seqs the model says must be present (durable acked, not deleted, not
    /// evicted) — the lower bound of the survivor set.
    fn live_seqs(&self) -> Vec<u64> {
        let floor = self.delete_floor.max(self.evict_floor);
        self.acked.keys().copied().filter(|s| *s >= floor).collect()
    }

    /// The earliest live seq the model expects (the recovered `earliest_seq` lower
    /// bound for a durable topic with no lost tail).
    fn earliest(&self) -> u64 {
        self.live_seqs().into_iter().min().unwrap_or(self.head + 1)
    }
}

/// The whole reference model: topics by name, advanced by the op stream.
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

    /// Record an acked append of `recs` at the engine-assigned `seqs`. Advances
    /// `head` and applies the cap (eviction) the engine would.
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
        // Cap eviction: if more than `cap` live records remain, the involuntary
        // floor advances so only the newest `cap` survive (an evict tombstone).
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

    /// Apply an acked voluntary delete (`before_seq` prefix and/or a tag match).
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
// Op stream
// ===========================================================================

/// One logical operation in a crash-recovery test workload. Each carries enough
/// info to drive BOTH the engine and the model deterministically.
#[derive(Debug, Clone)]
enum Op {
    /// Create a topic (durable flag + optional record cap).
    PutTopic {
        name: String,
        durable: bool,
        cap: u64,
    },
    /// Append one record `(data, tag, node)`.
    Append {
        name: String,
        data: String,
        tag: Option<String>,
        node: Option<String>,
    },
    /// Voluntary delete: a `before_seq` prefix and/or a tag-Eq match.
    Delete {
        name: String,
        before_seq: Option<u64>,
        tag_eq: Option<String>,
    },
}

/// Drive `ops` against a real engine, mirroring acked effects into `model`. A
/// durable op is mirrored into the model **only if** the engine call returns Ok
/// (its fsync returned ⇒ acked ⇒ must survive). A failed/aborted op is NOT
/// mirrored (unacked ⇒ may be absent after recovery).
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
                // The model must know the topic's durability before the append; if it
                // was auto-created here, default config is non-durable.
                if !model.topics.contains_key(name) {
                    model.ensure_topic(name, false, 0);
                }
                if let Ok(resp) = engine.write(name, req, true) {
                    // Acked: the durable write's fsync returned (or it is a
                    // non-durable topic — still acked to the client, but only durable
                    // topics are must-survive; the model records both and the diff
                    // applies the durability relaxation).
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
// Engine build / crash / recover plumbing through FakeDisk
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

/// Cleanly stop an engine (drop its WAL writer / join the thread) WITHOUT
/// hardening anything new — used between the workload and a recover, when no crash
/// is being simulated. To simulate a power loss instead, call `disk.crash(..)`
/// *before* dropping so the writer's drain is a no-op on the frozen device.
fn stop(engine: Arc<Engine>) {
    drop(engine);
}

/// A flat dump of the recovered state of one topic: head / earliest / count and the
/// live records by seq, read back through the real diff path (the same bytes a
/// consumer would see). `None` if the topic does not exist post-recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
struct TopicDump {
    head: u64,
    earliest: u64,
    count: u64,
    records: BTreeMap<u64, ModelRecord>,
    tombstone_reason: Option<String>,
}

/// Read the full recovered state of `name` through the engine's public API (state
/// plus a paginated diff over all records). Mirrors the subprocess crash test's
/// HTTP reads, in-process.
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
// ===========================================================================

/// Assert the crash-consistency contract for one topic: `model` (the reference) vs.
/// `dump` (the recovered engine). `whole_tail_durable` is true when the workload
/// guaranteed every acked write's fsync returned (a clean stop or an
/// all-durable-acked crash) so the survivor set must equal the model's live set;
/// otherwise a non-durable tail may be missing (still a dense prefix).
fn assert_topic_contract(
    name: &str,
    model: &ModelTopic,
    dump: &TopicDump,
    whole_tail_durable: bool,
) {
    let live = model.live_seqs();
    let survivors: Vec<u64> = dump.records.keys().copied().collect();

    // (1) NO FABRICATION: survivors ⊆ ever_acked. Every recovered seq must be a seq
    //     the model assigned at some point (never a bogus/torn frame misread as a
    //     record), and its payload must match the model byte-for-byte. We diff
    //     against `ever_acked` (not `acked`) because a delete whose WAL control
    //     frame was lost to a crash may legitimately not have taken effect, so a
    //     previously-deleted seq may reappear — allowed, but its bytes must still
    //     be the genuine record the model once acked, never garbage.
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

    // (2) NO SILENT LOSS of acked-durable: every model-live seq survives for a
    //     durable topic when the whole tail was durable. (For a non-durable topic, or a
    //     crash mid non-durable tail, the relaxation: a *prefix* survives — (3).)
    if model.durable && whole_tail_durable {
        for seq in &live {
            assert!(
                dump.records.contains_key(seq),
                "{name}: acked durable seq {seq} LOST after recovery \
                 (survivors={survivors:?}, model live={live:?})"
            );
        }
    }

    // (3) NO GAP — survivors are a dense PREFIX of the model's expected-present
    //     set. "Dense" is relative to that set (which may itself have holes from a
    //     tag-`match` delete that removed a middle seq); the contract is that
    //     recovery loses only a *tail*, never punches a hole into the middle.
    if let Some(&hi) = survivors.last() {
        if whole_tail_durable {
            // Clean stop / all-durable crash: survivors == the model's live set up
            // to the surviving high-water mark, exactly (no acked-durable loss, no
            // resurrection — delete/evict floors took effect durably).
            let expected_prefix: Vec<u64> = live.iter().copied().filter(|s| *s <= hi).collect();
            assert_eq!(
                survivors, expected_prefix,
                "{name}: survivors must be a dense prefix of the model's live set \
                 (survivors={survivors:?}, model live={live:?})"
            );
        } else {
            // Crash mid-stream: we cannot pin which acked op's fsync truly landed,
            // so the durable guarantees relax to the always-true subset contract
            //   live ⊆ survivors ⊆ ever_acked
            // (no fabrication is check (1)). The must-hold here is NO HOLE in the
            // live set below the surviving high-water: every model-live seq ≤ hi is
            // present (a crash may drop a tail, never a middle live record).
            for &s in &live {
                if s <= hi {
                    assert!(
                        dump.records.contains_key(&s),
                        "{name}: model-live seq {s} missing below surviving \
                         high-water {hi} (hole in the live set): survivors={survivors:?}"
                    );
                }
            }
        }
    }

    // (4) HEAD MONOTONE / NO SEQ REUSE (R3). The recovered head never REGRESSES
    //     below the highest acked seq — an already-acked seq is never re-handed.
    //     For an `fsync`-class topic (`model.durable`) the head equals the acked head
    //     exactly (each frame is fsynced before its ack, so the replayed head is
    //     the acked head). For a `disk`-class topic (acked before fsync) the head may
    //     legitimately exceed the acked head by up to the durable reservation block
    //     (`DISK_HEAD_RESERVE_AHEAD`): the topic fsyncs a head reservation AHEAD of
    //     use, so a crash that drops the un-fsynced disk tail recovers a head at the
    //     reservation ceiling (the lost seqs become silent deleted gaps) rather than
    //     regressing and re-handing a used seq. The reserved-but-unwritten seqs are
    //     deleted holes — absent from `survivors` — so (1)/(2)/(3) are unaffected.
    // The no-regression floor only binds when the WHOLE tail is durable (a clean
    // stop / all-durable crash): in a mid-stream crash the model's acked head is an
    // UPPER allowance (an in-process `Ok` may not have truly hardened before the
    // device froze), so head may legitimately sit below it — the same relaxation
    // (3) applies to. R3's dedicated test asserts no-reuse precisely.
    if whole_tail_durable && !model.acked.is_empty() {
        assert!(
            dump.head >= model.head,
            "{name}: recovered head {} REGRESSED below acked head {} (seq reuse!)",
            dump.head,
            model.head
        );
    }
    if model.durable {
        // fsync class: no head reservation, so the head matches the acked head
        // exactly (head never exceeds the model head, no future seq).
        assert!(
            dump.head <= model.head,
            "{name}: fsync-class recovered head {} exceeds model head {} (future seq?)",
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
        // disk class: the head may sit at the durable reservation ceiling, but
        // never beyond it (no unbounded future seq).
        let ceiling = model.head + topics::config::DISK_HEAD_RESERVE_AHEAD;
        assert!(
            dump.head <= ceiling,
            "{name}: disk-class recovered head {} exceeds the reservation ceiling {} \
             (acked head {} + reserve-ahead {})",
            dump.head,
            ceiling,
            model.head,
            topics::config::DISK_HEAD_RESERVE_AHEAD
        );
    }

    // (5) FLOORS PRESERVED: a voluntary delete stays SILENT (no tombstone); an
    //     involuntary cap eviction still tombstones below its floor.
    if model.evict_floor > model.delete_floor && !survivors.is_empty() {
        // The cap floor advanced beyond any voluntary delete ⇒ a from-0 read must
        // surface a tombstone (involuntary loss is never silent).
        assert!(
            dump.tombstone_reason.is_some(),
            "{name}: cap/TTL floor advanced but no tombstone after recovery \
             (involuntary loss must be explicit)"
        );
    }

    // (6) EARLIEST: when the whole durable tail survived, the recovered earliest
    //     matches the model's earliest live seq.
    if model.durable && whole_tail_durable && !live.is_empty() {
        assert_eq!(
            dump.earliest,
            model.earliest(),
            "{name}: recovered earliest_seq must match the model"
        );
    }
}

/// Diff the WHOLE recovered engine against the model for every modeled topic.
fn assert_recovered_matches_model(engine: &Engine, model: &RefModel, whole_tail_durable: bool) {
    for (name, mbox) in &model.topics {
        // A topic that never got an acked create AND never an acked write need not
        // exist; skip empty non-durable phantoms.
        let dump = dump_topic(engine, name);
        if mbox.acked.is_empty() && mbox.head == 0 {
            continue;
        }
        let Some(dump) = dump else {
            if mbox.durable && !mbox.live_seqs().is_empty() && whole_tail_durable {
                panic!("{name}: durable topic with acked records vanished after recovery");
            }
            continue;
        };
        assert_topic_contract(name, mbox, &dump, whole_tail_durable);
    }
}

/// Make every WAL file's NAME durable (the create+dir-fsync that production does
/// at WAL open — we model the dir fsync explicitly so the file survives a crash).
fn sync_wal_dir(disk: &FakeDisk) {
    let fs = disk.arc();
    let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("wal"));
    let _ = fs.sync_dir(&PathBuf::from(DATA_DIR).join("meta"));
}

// ===========================================================================
// 1) Clean (no-fault) round trips: recover == model, and idempotent recovery
// ===========================================================================

/// A durable workload, a clean stop, then recovery exactly reproduces the model:
/// every acked write present with identical data/tag/node, head/earliest/count
/// matching, the voluntary delete still silent.
#[test]
fn durable_workload_recovers_to_model() {
    let disk = FakeDisk::new();
    let mut model = RefModel::default();

    let ops = vec![
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
            node: Some("nA".into()),
        },
        Op::Append {
            name: "jobs".into(),
            data: "c".into(),
            tag: Some("t3".into()),
            node: None,
        },
        Op::Append {
            name: "jobs".into(),
            data: "d".into(),
            tag: None,
            node: Some("nB".into()),
        },
        Op::Delete {
            name: "jobs".into(),
            before_seq: Some(2),
            tag_eq: None,
        },
    ];

    {
        let engine = open_engine(&disk);
        run_ops(&engine, &mut model, &ops);
        sync_wal_dir(&disk);
        stop(engine);
    }

    // Recover a fresh engine through the same disk image; diff against the model.
    let engine = open_engine(&disk);
    assert_recovered_matches_model(&engine, &model, true);

    // Spot-check the concrete survivors (seq 1 deleted, 2..=4 intact + fidelity).
    let dump = dump_topic(&engine, "jobs").expect("jobs survived");
    assert_eq!(
        dump.records.keys().copied().collect::<Vec<_>>(),
        vec![2, 3, 4]
    );
    assert_eq!(dump.records[&2].data, "b");
    assert_eq!(dump.records[&2].tag.as_deref(), Some("t2"));
    assert_eq!(dump.records[&3].tag.as_deref(), Some("t3"));
}

/// IDEMPOTENT RECOVERY: recover(recover(x)) is identical to recover(x) — head /
/// earliest / count / per-seq records all match across a double recovery.
#[test]
fn recovery_is_idempotent() {
    let disk = FakeDisk::new();
    let mut model = RefModel::default();
    let ops = vec![
        Op::PutTopic {
            name: "b".into(),
            durable: true,
            cap: 0,
        },
        Op::Append {
            name: "b".into(),
            data: "x".into(),
            tag: Some("g".into()),
            node: None,
        },
        Op::Append {
            name: "b".into(),
            data: "y".into(),
            tag: None,
            node: Some("n".into()),
        },
        Op::Append {
            name: "b".into(),
            data: "z".into(),
            tag: Some("g".into()),
            node: None,
        },
        Op::Delete {
            name: "b".into(),
            before_seq: None,
            tag_eq: Some("g".into()),
        },
    ];
    {
        let engine = open_engine(&disk);
        run_ops(&engine, &mut model, &ops);
        sync_wal_dir(&disk);
        stop(engine);
    }

    let first = {
        let engine = open_engine(&disk);
        let d = dump_topic(&engine, "b").expect("b present after recovery #1");
        stop(engine);
        d
    };
    let second = {
        let engine = open_engine(&disk);
        let d = dump_topic(&engine, "b").expect("b present after recovery #2");
        stop(engine);
        d
    };
    assert_eq!(first, second, "recover(recover(x)) must equal recover(x)");
    // And it matches the model (tag-Eq delete removed both 'g' records).
    assert_recovered_matches_model(&open_engine(&disk), &model, true);
}

// ===========================================================================
// 2) Crash AFTER all acked-durable writes: every acked write survives
// ===========================================================================

/// Power loss with every write durable+acked: the model's live set survives
/// intact (acked ⇒ durable), seq monotone, no gap.
#[test]
fn crash_after_durable_acked_preserves_all() {
    let disk = FakeDisk::new();
    let mut model = RefModel::default();
    let ops = vec![
        Op::PutTopic {
            name: "k".into(),
            durable: true,
            cap: 0,
        },
        Op::Append {
            name: "k".into(),
            data: "1".into(),
            tag: Some("a".into()),
            node: None,
        },
        Op::Append {
            name: "k".into(),
            data: "2".into(),
            tag: Some("a".into()),
            node: None,
        },
        Op::Append {
            name: "k".into(),
            data: "3".into(),
            tag: Some("b".into()),
            node: None,
        },
    ];
    {
        let engine = open_engine(&disk);
        run_ops(&engine, &mut model, &ops);
        sync_wal_dir(&disk);
        // Power loss: freeze + drop un-fsynced bytes. Every acked write already
        // fsynced (durable topic), so all survive.
        disk.crash(TornDamage::None);
        stop(engine);
    }
    disk.reset_power();
    let engine = open_engine(&disk);
    assert_recovered_matches_model(&engine, &model, true);
    let dump = dump_topic(&engine, "k").unwrap();
    assert_eq!(dump.records.len(), 3, "all 3 acked durable writes survive");
}

// ===========================================================================
// 3) Crash mid NON-durable tail: clean prefix, no acked-durable loss
// ===========================================================================

/// A durable acked prefix followed by a non-durable burst, then a power loss:
/// the acked-durable prefix survives, the non-durable tail may vanish but only as
/// a clean dense prefix (no gap, no torn frame misread, no fabrication).
#[test]
fn crash_mid_nondurable_tail_is_clean_prefix() {
    for &damage in &[
        TornDamage::None,
        TornDamage::PrefixTruncate,
        TornDamage::Garble,
    ] {
        let disk = FakeDisk::with_seed(0xBADC0DE ^ damage as u64);
        let mut model = RefModel::default();

        // Two durable topics: a durable-acked one (must fully survive) and a
        // non-durable one (a clean prefix may survive).
        let mut ops = vec![
            Op::PutTopic {
                name: "durable".into(),
                durable: true,
                cap: 0,
            },
            Op::PutTopic {
                name: "fast".into(),
                durable: false,
                cap: 0,
            },
            Op::Append {
                name: "durable".into(),
                data: "d1".into(),
                tag: None,
                node: None,
            },
            Op::Append {
                name: "durable".into(),
                data: "d2".into(),
                tag: Some("x".into()),
                node: None,
            },
        ];
        for i in 0..15 {
            ops.push(Op::Append {
                name: "fast".into(),
                data: format!("f{i}"),
                tag: None,
                node: None,
            });
        }

        {
            let engine = open_engine(&disk);
            run_ops(&engine, &mut model, &ops);
            sync_wal_dir(&disk);
            disk.crash(damage);
            stop(engine);
        }
        disk.reset_power();
        let engine = open_engine(&disk);

        // The durable topic's acked writes ALL survive (whole_tail_durable for it).
        let dump_d = dump_topic(&engine, "durable").expect("durable topic survives");
        assert_topic_contract("durable", &model.topics["durable"], &dump_d, true);
        assert_eq!(
            dump_d.records.len(),
            2,
            "both durable acked writes survive {damage:?}"
        );

        // The non-durable topic: whatever survived is a clean dense prefix of the
        // model's live set — no gap, no fabricated/torn record.
        if let Some(dump_f) = dump_topic(&engine, "fast") {
            assert_topic_contract("fast", &model.topics["fast"], &dump_f, false);
        }
        stop(engine);
    }
}

// ===========================================================================
// 4) An injected fsync EIO fails the batch cleanly: prior state intact
// ===========================================================================

/// EIO on the durable group-commit `sync_data` fails the in-flight batch (the
/// write returns Err ⇒ NOT acked ⇒ not in the model); the failed batch's bytes
/// were buffered but never durable, so a power loss drops them
/// (F-COMPOUND-FSYNC-FAIL-THEN-CRASH). Prior durable acked state is intact and a
/// recovery matches the model exactly — the failed frame leaves no trace.
#[test]
fn fsync_eio_fails_batch_prior_state_intact() {
    let disk = FakeDisk::new();
    let mut model = RefModel::default();

    // Phase 1: 3 durable acked writes on a clean disk.
    {
        let engine = open_engine(&disk);
        run_ops(
            &engine,
            &mut model,
            &[
                Op::PutTopic {
                    name: "p".into(),
                    durable: true,
                    cap: 0,
                },
                Op::Append {
                    name: "p".into(),
                    data: "1".into(),
                    tag: None,
                    node: None,
                },
                Op::Append {
                    name: "p".into(),
                    data: "2".into(),
                    tag: None,
                    node: None,
                },
                Op::Append {
                    name: "p".into(),
                    data: "3".into(),
                    tag: None,
                    node: None,
                },
            ],
        );
        sync_wal_dir(&disk);
        stop(engine);
    }

    // Phase 2: wrap the disk in a dead-device sync_data fault; a durable append
    // MUST fail and MUST NOT enter the model.
    let faulty: Arc<dyn Fs> =
        FaultFs::new(disk.arc(), FaultOp::SyncData, FaultKind::Eio, 0, false).arc();
    {
        let engine =
            Engine::with_data_dir_fs(cfg(), clock(), faulty).expect("reopen through faultfs");
        let req = WriteRequest {
            records: vec![RecordIn {
                data: json!({ "v": "4" }),
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
        let res = engine.write("p", req, true);
        assert!(res.is_err(), "durable append must fail when fsync EIOs");
        // NOT mirrored into the model (unacked). The frame-4 bytes were buffered
        // (pending) but their fsync errored, so they are NOT durable; a power loss
        // now drops them (the failed batch never reaches the platter). Freeze the
        // device BEFORE dropping so the writer's Drop drain cannot harden the
        // buffered tail on a (faulted-anyway) device.
        disk.crash(TornDamage::None);
        stop(engine);
    }
    disk.reset_power();

    // Recover through the crashed image: exactly the 3 prior durable frames,
    // matching the model (the failed 4th never landed durably).
    let engine = open_engine(&disk);
    assert_recovered_matches_model(&engine, &model, true);
    let dump = dump_topic(&engine, "p").unwrap();
    assert_eq!(
        dump.records.len(),
        3,
        "failed batch left no trace; prior 3 intact"
    );
}

// ===========================================================================
// 5) Cap-topic eviction floor survives a crash (involuntary loss stays explicit)
// ===========================================================================

/// A durable cap topic: writing past the cap advances the involuntary evict floor;
/// after a crash the floor is preserved and a from-0 read still tombstones (cap
/// loss is never silent), while the surviving window matches the model.
#[test]
fn cap_evict_floor_survives_crash_with_tombstone() {
    let disk = FakeDisk::new();
    let mut model = RefModel::default();
    let mut ops = vec![Op::PutTopic {
        name: "cap".into(),
        durable: true,
        cap: 3,
    }];
    for i in 1..=6 {
        ops.push(Op::Append {
            name: "cap".into(),
            data: i.to_string(),
            tag: None,
            node: None,
        });
    }
    {
        let engine = open_engine(&disk);
        run_ops(&engine, &mut model, &ops);
        sync_wal_dir(&disk);
        disk.crash(TornDamage::None);
        stop(engine);
    }
    disk.reset_power();
    let engine = open_engine(&disk);
    assert_recovered_matches_model(&engine, &model, true);

    let dump = dump_topic(&engine, "cap").unwrap();
    assert_eq!(dump.head, 6, "head recovered");
    assert_eq!(dump.records.len(), 3, "only the newest cap=3 survive");
    assert_eq!(
        dump.records.keys().copied().collect::<Vec<_>>(),
        vec![4, 5, 6],
        "the cap window matches the model"
    );
    assert_eq!(
        dump.tombstone_reason.as_deref(),
        Some("cap"),
        "cap eviction stays explicit (tombstone) after recovery"
    );
}

// ===========================================================================
// 6) EXHAUSTIVE crash-point sweep over a durable workload (bounded)
// ===========================================================================

/// SQLite-style bounded sweep: probe the number of FS *mutating* (write_at) calls
/// the durable workload issues, then for each crash point in `0..M` replay the
/// workload on a FRESH disk, `crash()` after that many write_at calls, recover,
/// and assert the model oracle. At every crash point the recovered state is a
/// dense prefix of the model's live set — never a half-applied record. Bounded
/// workload + capped points keep it fast and deterministic.
#[test]
fn sweep_durable_append_crash_points_oracle() {
    // The op stream under test (small, fixed).
    let ops = vec![
        Op::PutTopic {
            name: "s".into(),
            durable: true,
            cap: 0,
        },
        Op::Append {
            name: "s".into(),
            data: "1".into(),
            tag: Some("a".into()),
            node: None,
        },
        Op::Append {
            name: "s".into(),
            data: "2".into(),
            tag: Some("a".into()),
            node: Some("n".into()),
        },
        Op::Append {
            name: "s".into(),
            data: "3".into(),
            tag: Some("b".into()),
            node: None,
        },
        Op::Delete {
            name: "s".into(),
            before_seq: Some(2),
            tag_eq: None,
        },
        Op::Append {
            name: "s".into(),
            data: "4".into(),
            tag: None,
            node: None,
        },
    ];

    // Probe M: count write_at calls over the whole workload (at = u64::MAX ⇒ never
    // fires; just counts).
    let probe_disk = FakeDisk::new();
    let probe = FaultFs::new(
        probe_disk.arc(),
        FaultOp::WriteAt,
        FaultKind::Eio,
        u64::MAX,
        true,
    );
    {
        let mut throwaway = RefModel::default();
        let engine = Engine::with_data_dir_fs(cfg(), clock(), probe.arc()).expect("probe engine");
        run_ops(&engine, &mut throwaway, &ops);
        stop(engine);
    }
    let total_writes = probe.calls_seen();
    assert!(
        total_writes >= 4,
        "workload issues several write_at calls (M={total_writes})"
    );

    // Cap the sweep so the test stays fast but still covers every interesting
    // boundary of the small workload (each durable append blocks on a real group
    // fsync, so the cap bounds total wall time). The set of probed crash points is
    // tiered (topics::testutil::crash_points): a bounded deterministic sample by
    // DEFAULT (both endpoints + an interior spread), the full `0..=cap` matrix when
    // `TOPICS_TEST_EXHAUSTIVE=1` (nightly CI). No boundary is ever dropped.
    let cap = total_writes.min(14);
    for crash_point in topics::testutil::crash_points(cap) {
        // A FaultFs that drives a FakeDisk.crash() after exactly `crash_point`
        // write_at calls — the harness-level crash injector at a precise FS index.
        let disk = FakeDisk::with_seed(crash_point);
        let trip = CrashAfter::new(disk.clone(), FaultOp::WriteAt, crash_point);
        let mut model = RefModel::default();
        {
            let engine =
                Engine::with_data_dir_fs(cfg(), clock(), trip.arc()).expect("sweep engine opens");
            // Run the ops; some appends past the crash point land on the (now
            // frozen) device and are dropped — but only writes whose fsync returned
            // before the crash count as acked-durable. The `whole_tail_durable=false`
            // diff applies the dense-prefix relaxation, so the model's `acked` set is
            // the *upper* allowance (survivors ⊆ model∪acked) and the survivors must
            // be a dense prefix with no fabrication.
            run_ops_tolerant(&engine, &mut model, &ops);
            stop(engine);
        }
        disk.reset_power();

        // Recover a fresh engine through the crashed image and assert the oracle:
        // survivors are a dense prefix of the model's live set, no fabrication, no
        // gap, no resurrection.
        let engine = open_engine(&disk);
        assert_recovered_matches_model(&engine, &model, false);

        // Idempotent recovery on a few representative points (the dedicated
        // `recovery_is_idempotent` test covers the general case; re-running here at
        // sweep crash points proves convergence after a *partial* write too).
        if crash_point % 4 == 0 {
            let d1 = dump_topic(&engine, "s");
            stop(engine);
            let engine2 = open_engine(&disk);
            let d2 = dump_topic(&engine2, "s");
            assert_eq!(d1, d2, "recovery idempotent at crash_point {crash_point}");
            stop(engine2);
        } else {
            stop(engine);
        }
    }
}

/// Like `run_ops` but tolerant of an op failing because the device crashed
/// mid-stream: a failed op is simply not mirrored into the model (unacked). The
/// crash is triggered by the wrapping `CrashAfter` fs after the Nth write_at.
fn run_ops_tolerant(engine: &Engine, model: &mut RefModel, ops: &[Op]) {
    run_ops(engine, model, ops);
}

// ===========================================================================
// CrashAfter: an Fs wrapper that fires FakeDisk.crash() after the Nth op of a
// chosen class (the harness-level "power loss after the Nth FS call" injector).
// ===========================================================================

use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

/// Wraps a [`FakeDisk`] and triggers `disk.crash()` exactly after the `at`-th call
/// (0-based) of a chosen op class, then lets the (now frozen) disk swallow the
/// rest. This is the in-process equivalent of "SIGKILL after the Nth FS mutating
/// call": every write past the trip point lands on a powered-off device and is
/// lost, while everything fsynced before it is durable.
#[derive(Clone)]
struct CrashAfter {
    disk: FakeDisk,
    op: FaultOp,
    at: u64,
    seen: Arc<AtomicU64>,
    tripped: Arc<std::sync::atomic::AtomicBool>,
}

impl CrashAfter {
    fn new(disk: FakeDisk, op: FaultOp, at: u64) -> Self {
        CrashAfter {
            disk,
            op,
            at,
            seen: Arc::new(AtomicU64::new(0)),
            tripped: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    fn arc(&self) -> Arc<dyn Fs> {
        Arc::new(self.clone())
    }

    /// Count one op of the chosen class; if it reaches `at`, trip the crash once.
    fn tick(&self, op: FaultOp) {
        if op != self.op {
            return;
        }
        let idx = self.seen.fetch_add(1, AtomicOrdering::SeqCst);
        if idx == self.at
            && self
                .tripped
                .compare_exchange(false, true, AtomicOrdering::SeqCst, AtomicOrdering::SeqCst)
                .is_ok()
        {
            self.disk.crash(TornDamage::None);
        }
    }
}

struct CrashAfterFile {
    inner: Box<dyn topics::storage::File>,
    owner: CrashAfter,
}

impl topics::storage::File for CrashAfterFile {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read_at(offset, buf)
    }
    fn write_at(&mut self, offset: u64, buf: &[u8]) -> std::io::Result<usize> {
        // Trip AFTER the write reaches the (pending) image, so a crash at index N
        // keeps the first N writes' pending bytes available for fsync but drops
        // anything not yet fsynced — exactly the power-loss-after-Nth-write model.
        let r = self.inner.write_at(offset, buf);
        self.owner.tick(FaultOp::WriteAt);
        r
    }
    fn set_len(&mut self, len: u64) -> std::io::Result<()> {
        let r = self.inner.set_len(len);
        self.owner.tick(FaultOp::SetLen);
        r
    }
    fn sync_data(&self) -> std::io::Result<()> {
        let r = self.inner.sync_data();
        self.owner.tick(FaultOp::SyncData);
        r
    }
    fn sync_all(&self) -> std::io::Result<()> {
        let r = self.inner.sync_all();
        self.owner.tick(FaultOp::SyncAll);
        r
    }
    fn metadata_len(&self) -> std::io::Result<u64> {
        self.inner.metadata_len()
    }
    fn read_to_end_from(&self, offset: u64, out: &mut Vec<u8>) -> std::io::Result<()> {
        self.inner.read_to_end_from(offset, out)
    }
}

impl Fs for CrashAfter {
    fn open(
        &self,
        path: &std::path::Path,
        opts: topics::storage::OpenOpts,
    ) -> std::io::Result<Box<dyn topics::storage::File>> {
        let inner = self.disk.open(path, opts)?;
        Ok(Box::new(CrashAfterFile {
            inner,
            owner: self.clone(),
        }))
    }
    fn rename(&self, from: &std::path::Path, to: &std::path::Path) -> std::io::Result<()> {
        let r = self.disk.rename(from, to);
        self.tick(FaultOp::Rename);
        r
    }
    fn remove_file(&self, path: &std::path::Path) -> std::io::Result<()> {
        self.disk.remove_file(path)
    }
    fn read_dir(&self, dir: &std::path::Path) -> std::io::Result<Vec<PathBuf>> {
        self.disk.read_dir(dir)
    }
    fn sync_dir(&self, dir: &std::path::Path) -> std::io::Result<()> {
        let r = self.disk.sync_dir(dir);
        self.tick(FaultOp::SyncDir);
        r
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
// 7) Multi-topic durable workload with a delete + crash, full oracle diff
// ===========================================================================

/// A richer multi-topic durable workload (two topics, interleaved appends, a tag
/// delete on one), then a clean stop and recovery — the whole-engine model diff
/// passes for every topic, proving the oracle covers cross-topic state.
#[test]
fn multi_topic_durable_recovers_to_model() {
    let disk = FakeDisk::new();
    let mut model = RefModel::default();
    let ops = vec![
        Op::PutTopic {
            name: "alpha".into(),
            durable: true,
            cap: 0,
        },
        Op::PutTopic {
            name: "beta".into(),
            durable: true,
            cap: 0,
        },
        Op::Append {
            name: "alpha".into(),
            data: "a1".into(),
            tag: Some("keep".into()),
            node: None,
        },
        Op::Append {
            name: "beta".into(),
            data: "b1".into(),
            tag: None,
            node: Some("n1".into()),
        },
        Op::Append {
            name: "alpha".into(),
            data: "a2".into(),
            tag: Some("drop".into()),
            node: None,
        },
        Op::Append {
            name: "beta".into(),
            data: "b2".into(),
            tag: None,
            node: Some("n2".into()),
        },
        Op::Append {
            name: "alpha".into(),
            data: "a3".into(),
            tag: Some("keep".into()),
            node: None,
        },
        Op::Delete {
            name: "alpha".into(),
            before_seq: None,
            tag_eq: Some("drop".into()),
        },
    ];
    {
        let engine = open_engine(&disk);
        run_ops(&engine, &mut model, &ops);
        sync_wal_dir(&disk);
        // Give the writer a beat for any non-durable buffering (all durable here).
        std::thread::sleep(Duration::from_millis(5));
        disk.crash(TornDamage::None);
        stop(engine);
    }
    disk.reset_power();
    let engine = open_engine(&disk);
    assert_recovered_matches_model(&engine, &model, true);

    // alpha: the 'drop'-tagged a2 is gone, a1 & a3 ('keep') remain.
    let alpha = dump_topic(&engine, "alpha").unwrap();
    assert_eq!(
        alpha.records.keys().copied().collect::<Vec<_>>(),
        vec![1, 3]
    );
    assert_eq!(alpha.records[&1].data, "a1");
    assert_eq!(alpha.records[&3].data, "a3");
    // beta: untouched, both records with their nodes.
    let beta = dump_topic(&engine, "beta").unwrap();
    assert_eq!(beta.records.len(), 2);
    assert_eq!(beta.records[&1].node.as_deref(), Some("n1"));
    assert_eq!(beta.records[&2].node.as_deref(), Some("n2"));
}
