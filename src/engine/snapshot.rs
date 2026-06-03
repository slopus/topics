//! Engine ↔ snapshot glue (ARCHITECTURE §3 checkpoint, §5 metadata store).
//!
//! [`capture`] materializes the engine's durable state into a
//! [`crate::storage::Snapshot`]; [`restore`] rebuilds the in-memory engine from
//! a loaded one. A snapshot lets recovery start without replaying the WAL from
//! frame zero — only the frames after the recorded checkpoint position are
//! replayed (and re-applied Appends are skipped by seq, so the small overlap
//! between the captured position and the materialized state is idempotent).
//!
//! ## Consistency ordering (why this loses nothing)
//!
//! [`capture`] records the WAL checkpoint position **first** (after a durable
//! `CheckpointMark` flush barrier so the published position covers every
//! prior frame), *then* materializes topic/router state. Any write committed
//! after the position-read has a WAL offset `>=` the checkpoint, so recovery
//! replays it; any such write that also raced into the materialized snapshot is
//! skipped on replay by seq (`seq <= recovered head`). No acked write can fall
//! into the gap.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::engine::topic_state::{StoredRecord, TopicState};
use crate::engine::{Engine, SEQ_BASE};
use crate::storage::{Checkpoint, Snapshot, SnapshotRecord, SnapshotRouter, SnapshotTopic};
use crate::types::{Filter, FilterOp, Router, RouterGuarantee, TopicConfig};

/// Capture the engine's current durable state into a [`Snapshot`].
///
/// Requires a WAL (durable engine); returns `None` for a pure in-memory engine
/// (nothing to checkpoint). `id` is the snapshot's monotonic file id.
pub fn capture(engine: &Engine, id: u64) -> Option<Snapshot> {
    let writer = engine.wal.as_ref()?;
    let now = engine.clock.now_ms().max(0) as u64;

    // 1) Flush barrier on EVERY shard: write a durable CheckpointMark to each shard
    //    and wait for its fsync so each shard's published position covers every
    //    prior committed frame on that shard. (Replay treats CheckpointMark as a
    //    no-op, so logging it is harmless.) PROPAGATE a barrier failure (codex P0):
    //    if any shard's CheckpointMark cannot be durably synced, its published
    //    position does NOT cover prior frames, so a snapshot taken against it could
    //    exclude an acked write — abandon the snapshot rather than record a
    //    checkpoint that races ahead of durability.
    if writer.checkpoint_barrier_all(now).is_err() {
        return None;
    }

    // 2) Record the per-group checkpoint positions FIRST (before materializing
    //    state), so a racing write lands at/after its group's offset and is
    //    replayed. Each position is keyed by its PHYSICAL group identity (the
    //    relative dir under `wal/`: `""` flat, `shard-NN` sharded), so recovery only
    //    ever applies an offset to the exact group it was measured against — a flat
    //    group and a `shard-00/` group are never conflated across a shard-count
    //    reconfigure (codex P0 #1/#3).
    let mut keyed = writer.keyed_positions();
    // Also record an absorbed position for EVERY leftover/orphan WAL group still on
    // disk from a PRIOR layout (a group the current writers do not cover). Its
    // frames were already replayed + materialized into this snapshot, so its current
    // tail is the fully-absorbed boundary; recording it means a later recovery skips
    // that group entirely even if its files were not yet (or could not be) deleted —
    // so an absorbed group's control frames (Delete/TopicConfig/EvictWatermark/
    // HeadWatermark) can never replay-from-zero and regress snapshotted state
    // (codex P0 #2). Best-effort: a group we cannot stat is simply not recorded
    // (recovery then replays it, still correct — Appends are seq-idempotent).
    if let Some(dir) = &engine.data_dir {
        let fs = engine
            .recovery_fs
            .clone()
            .unwrap_or_else(crate::storage::RealFs::arc);
        let have: std::collections::HashSet<String> =
            keyed.iter().map(|(k, _)| k.clone()).collect();
        for (key, pos) in crate::engine::recovery::discover_group_tails(&fs, dir) {
            if !have.contains(&key) {
                keyed.push((key, pos));
            }
        }
    }
    let shard_positions: Vec<(u64, u64)> = keyed.iter().map(|(_, p)| *p).collect();
    let shard_keys: Vec<String> = keyed.iter().map(|(k, _)| k.clone()).collect();
    let (wal_idx, wal_offset) = shard_positions.first().copied().unwrap_or((0, 0));

    // Quiesce router advancement across BOTH the topic capture and the router-cursor
    // capture below (codex P0 #1). `advance_router` holds this lock SHARED while it
    // publishes a router's derived dest records and advances that router's cursor;
    // taking it EXCLUSIVE here freezes every router so the captured (derived dest
    // content, cursor) pair is one consistent checkpoint unit — a snapshot can never
    // record a dest at one cursor and the cursor at another, which would re-derive a
    // duplicate (old cursor + new dest) or silently drop (new cursor + old dest) on
    // recovery. Held for the rest of `capture` (released on return).
    let _router_freeze = engine.router_snapshot_lock.write();

    // 3) Materialize every topic's live state + the router set. Each topic is captured
    //    under its own `append_lock` (codex P0): a durable write may have its WAL
    //    frame *before* the checkpoint offset yet still be staged-but-unpublished
    //    when we read the index. Holding the lock blocks any NEW stage; we then
    //    `quiesce_publishes()` to drain every already-ticketed in-flight write
    //    (its fsync now waits OFF the lock, codex P0 #1) so `head_seq` covers
    //    every frame the checkpoint offset already includes. After that we observe
    //    a consistent `(head_seq, index)` and never exclude a covered frame.
    //    `capture_topic` additionally snapshots only `seq <= head_seq`, so a
    //    concurrently-staged (invisible) tail is never persisted.
    let mut topics = Vec::with_capacity(engine.topics.len());
    let mut max_seq = 0u64;
    for entry in engine.topics.iter() {
        let b = entry.value();
        // Enforce retention so the captured floors/records are current. A
        // non-re-derivable floor must be hardened before it enters the snapshot.
        if engine
            .enforce_retention_durable(b, engine.clock.now_ms())
            .is_err()
        {
            return None;
        }
        let _append_guard = b.append_lock.lock();
        // Drain in-flight (ticketed-but-unpublished) durable writes so head_seq
        // covers every frame at/before the checkpoint offset captured above.
        b.quiesce_publishes();
        let snap_topic = capture_topic(b);
        max_seq = max_seq.max(snap_topic.head_seq);
        topics.push(snap_topic);
    }

    let routers = engine
        .routers
        .lock()
        .snapshot_all()
        .into_iter()
        .map(|(r, cursor, total, dest_base)| router_to_snapshot(r, cursor, total, dest_base))
        .collect();

    Some(Snapshot {
        id,
        ts: now,
        next_topic_id: engine.next_topic_id.load(Ordering::Relaxed),
        checkpoint: Checkpoint {
            wal_idx,
            wal_offset,
            last_checkpoint_seq: max_seq,
            shards: shard_positions,
            shard_keys,
        },
        topics,
        routers,
    })
}

/// Materialize one topic into a [`SnapshotTopic`] (its live record set + floors).
///
/// Caller holds the topic's `append_lock`, so `head_seq` and the index are
/// consistent and any staged-but-unpublished tail is excluded by the `seq <=
/// head` clamp.
fn capture_topic(b: &TopicState) -> SnapshotTopic {
    let config = b.config.read().clone();
    let config_json = serde_json::to_vec(&config).unwrap_or_default();

    // An `ephemeral` topic has durable config but resident-only records. Capture
    // the topic shell without payloads, but preserve the published head so a
    // checkpointed topic keeps monotonic sequence allocation after restart.
    if !config.uses_persistent_record_store() {
        let head_seq = b.head_seq();
        return SnapshotTopic {
            name: b.name.clone(),
            topic_id: b.topic_id,
            epoch: b.epoch(),
            config_json,
            base_seq: head_seq.saturating_add(1),
            head_seq,
            evict_floor: 0,
            expiry_floor: 0,
            delete_floor: 0,
            delete_below: 0,
            bytes_retained: 0,
            live_count: 0,
            records: Vec::new(),
            source_trim_floor: 0,
        };
    }

    let floors = *b.floors.read();
    // The published head: never persist a staged-but-unpublished (invisible) tail.
    let head_seq = b.head_seq();

    // The live record set: every physically-present, non-deleted record. Deleted
    // middle-holes and front-reclaimed prefixes are simply absent — the compacted
    // form (ARCHITECTURE §3.1). A record whose payload was freed after sealing
    // (Phase 6) is resolved from the segment **after** the index lock is dropped,
    // so a snapshot never holds the index lock across a segment read and never
    // persists a `Null` payload for a still-live record.
    struct CapSlot {
        seq: u64,
        ts: i64,
        node: Option<String>,
        tag: Option<String>,
        bytes: u64,
        resident: Option<(serde_json::Value, Option<serde_json::Value>)>,
    }
    let (base_seq, delete_below, slots) = {
        let index = b.index.read();
        let base = index.base_seq;
        let mut slots = Vec::with_capacity(index.records.len());
        for (i, rec) in index.records.iter().enumerate() {
            if rec.deleted {
                continue;
            }
            let seq = base + i as u64;
            // Never persist a staged-but-unpublished tail (`seq > head`): the
            // append lock is held, but a record can sit in the deque before
            // `head_seq` is advanced (the WAL-first reservation). Such a record is
            // not yet acknowledged/visible; its WAL frame (if any) lands at/after
            // the checkpoint and replays, so the snapshot must exclude it.
            if seq > head_seq {
                break;
            }
            slots.push(CapSlot {
                seq,
                ts: rec.ts,
                node: rec.node.clone(),
                tag: rec.tag.clone(),
                bytes: rec.bytes,
                resident: if rec.payload_resident {
                    Some((rec.data.clone(), rec.meta.clone()))
                } else {
                    None
                },
            });
        }
        (base, index.delete_below, slots)
    };

    let mut records = Vec::with_capacity(slots.len());
    for slot in slots {
        let (data, meta) = match slot.resident {
            Some(p) => p,
            None => {
                let mut ignored = 0u64;
                crate::engine::resolve_sealed_off_lock(b, slot.seq, &mut ignored)
            }
        };
        records.push(SnapshotRecord {
            seq: slot.seq,
            ts: slot.ts,
            node: slot.node,
            tag: slot.tag,
            data_json: serde_json::to_vec(&data).unwrap_or_default(),
            meta_json: meta
                .as_ref()
                .map(|m| serde_json::to_vec(m).unwrap_or_default()),
            bytes: slot.bytes,
        });
    }

    SnapshotTopic {
        name: b.name.clone(),
        topic_id: b.topic_id,
        epoch: b.epoch(),
        config_json,
        base_seq,
        head_seq,
        evict_floor: floors.evict_floor,
        expiry_floor: floors.expiry_floor,
        delete_floor: floors.delete_floor,
        delete_below,
        bytes_retained: b.bytes(),
        live_count: b.count(),
        records,
        source_trim_floor: floors.source_trim_floor,
    }
}

/// Encode a [`Router`] into a [`SnapshotRouter`].
fn router_to_snapshot(r: Router, cursor: u64, total: u64, dest_base: u64) -> SnapshotRouter {
    SnapshotRouter {
        name: r.name,
        source: r.source,
        dest: r.dest,
        preserve_node: r.preserve_node,
        preserve_tag: r.preserve_tag,
        create_dest: r.create_dest,
        allow_cycle: r.allow_cycle,
        exactly_once: r.guarantee == RouterGuarantee::ExactlyOnce,
        filter: r.filter.map(|f| {
            (
                match f.op {
                    FilterOp::Eq => 0u8,
                    FilterOp::Glob => 1u8,
                },
                f.value,
            )
        }),
        forward_cursor: cursor,
        forwarded_total: total,
        dest_base,
    }
}

/// Rebuild the engine's in-memory state from a loaded [`Snapshot`]. Called
/// before WAL replay during recovery (the WAL frames after the checkpoint are
/// then layered on top). Returns the checkpoint position to replay from.
pub fn restore(engine: &Engine, snapshot: Snapshot) -> Checkpoint {
    // Restore the id allocator so newly-created topics never collide with a
    // snapshotted id.
    engine
        .next_topic_id
        .store(snapshot.next_topic_id, Ordering::Relaxed);

    for sb in snapshot.topics {
        let config: TopicConfig = serde_json::from_slice(&sb.config_json).unwrap_or_default();
        let persistent_records = config.uses_persistent_record_store();
        let topic_id = sb.topic_id;
        let mut state = restore_topic(sb, config);
        // Attach a HOT segment writer only for persistent record classes. An
        // ephemeral topic restores as an empty resident-only shell.
        if persistent_records {
            if let Some(writer) = engine.build_segment_writer(topic_id) {
                state.attach_segwriter(writer);
            }
        }
        let bx = Arc::new(state);
        // Keep the live gauges in lockstep with the restored registry so the
        // `max_topics` / `max_total_bytes` reservation counters reflect recovered
        // state (a fresh insert bumps the topic count by 1 and the byte total by the
        // topic's retained bytes).
        if engine.topics.insert(bx.name.clone(), bx.clone()).is_none() {
            engine.topic_count.fetch_add(1, Ordering::AcqRel);
        }
        engine
            .total_bytes_live
            .fetch_add(bx.bytes(), Ordering::AcqRel);
    }

    {
        let mut graph = engine.routers.lock();
        for sr in snapshot.routers {
            let filter = sr.filter.map(|(op, value)| Filter {
                op: if op == 0 {
                    FilterOp::Eq
                } else {
                    FilterOp::Glob
                },
                value,
            });
            let router = Router {
                name: sr.name,
                source: sr.source,
                dest: sr.dest,
                preserve_node: sr.preserve_node,
                preserve_tag: sr.preserve_tag,
                create_dest: sr.create_dest,
                filter,
                allow_cycle: sr.allow_cycle,
                guarantee: if sr.exactly_once {
                    RouterGuarantee::ExactlyOnce
                } else {
                    RouterGuarantee::AtLeastOnce
                },
            };
            graph.restore(router, sr.forward_cursor, sr.forwarded_total, sr.dest_base);
        }
    }

    snapshot.checkpoint
}

/// Rebuild a single [`TopicState`] from a [`SnapshotTopic`], re-inserting its live
/// record set at the recorded seqs (so `base_seq`/`head_seq` and the tag index
/// match exactly) and restoring the floors + counters.
fn restore_topic(sb: SnapshotTopic, config: TopicConfig) -> TopicState {
    let state = TopicState::new(sb.name, sb.topic_id, config, SEQ_BASE, sb.epoch);

    {
        let mut index = state.index.write();
        index.base_seq = sb.base_seq;
        index.delete_below = sb.delete_below;
        // Re-insert each live record at its seq. The snapshot is the compacted
        // (deleted-hole-free) form, but holes between live seqs are possible
        // (a middle delete reclaimed neither slot before snapshot). Fill any gap
        // with a deleted tombstone so `seq - base_seq` indexing stays O(1).
        let mut next = sb.base_seq;
        for r in sb.records {
            while next < r.seq {
                index.records.push_back(deleted_hole());
                next += 1;
            }
            if let Some(tag) = &r.tag {
                index.index_tag(r.seq, tag);
            }
            let data: serde_json::Value =
                serde_json::from_slice(&r.data_json).unwrap_or(serde_json::Value::Null);
            let meta = r
                .meta_json
                .as_ref()
                .and_then(|m| serde_json::from_slice(m).ok());
            index.records.push_back(StoredRecord {
                ts: r.ts,
                node: r.node,
                tag: r.tag,
                data,
                meta,
                bytes: r.bytes,
                deleted: false,
                payload_resident: true,
                hops: 0,
            });
            next = r.seq + 1;
        }
    }

    // Floors + watermarks.
    {
        let mut floors = state.floors.write();
        floors.evict_floor = sb.evict_floor;
        floors.expiry_floor = sb.expiry_floor;
        floors.delete_floor = sb.delete_floor;
        // Restore the derived-router source-trim floor (codex P1 #5) so a
        // previously-surfaced `source_trim` gap stays a tombstone, not a silent gap.
        floors.source_trim_floor = sb.source_trim_floor;
    }
    state.head_seq.store(sb.head_seq, Ordering::Release);
    // The snapshot's head is durable, so the reservation ceiling starts there
    // (R3); a later replayed `HeadWatermark` may raise it further.
    state.set_reserved_head(sb.head_seq);
    state
        .bytes_retained
        .store(sb.bytes_retained, Ordering::Relaxed);
    state.live_count.store(sb.live_count, Ordering::Relaxed);

    state
}

use crate::engine::topic_state::deleted_hole;
