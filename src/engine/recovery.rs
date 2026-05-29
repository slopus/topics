//! WAL + snapshot recovery (ARCHITECTURE §4): on startup, load the latest valid
//! metadata snapshot, then replay the WAL from the snapshot's checkpoint
//! position to rebuild the in-memory index, truncate any torn tail, then resume
//! the writer for new appends.
//!
//! Recovery order (ARCHITECTURE §4):
//!
//! 1. Load the latest valid snapshot under `<data_dir>/meta` (if any) and
//!    restore the box registry, per-box materialized state + floors, routers,
//!    and `next_box_id`. A missing/torn snapshot ⇒ start from an empty state and
//!    replay the WAL from frame zero (the Stage-2 behavior).
//! 2. Replay WAL frames **after the checkpoint position**: WAL files numbered
//!    below the checkpoint's active file are fully absorbed (skipped); the
//!    active file is replayed from the checkpoint byte offset onward. Frames are
//!    applied in global (file-index, then in-file) order, reproducing the
//!    pre-crash state on top of the snapshot:
//!
//!    - `Append`  → re-insert the record at its logged seq, **unless** its seq is
//!      already `<= head` for that box (already covered by the snapshot) ⇒
//!      skipped (idempotent overlap; ARCHITECTURE §4).
//!    - `Delete`  → re-apply the `before_seq`/`match` selector (idempotent).
//!    - `BoxConfig` (create/update / tombstone) → create/update or remove a box.
//!    - `RouterCreate`/`RouterDelete` → rebuild the router graph.
//!    - `EvictWatermark` → restore the cap/TTL floor.
//!
//! 3. Truncate the active WAL file's torn tail (length overrun / CRC) so a new
//!    append can never be confused with a partial one.

use std::path::{Path, PathBuf};

use crate::engine::{
    decode_record_payload, matchsel_to_filter, snapshot as engine_snapshot, wal_glue::WalHandle,
    Engine, ReplayRecord,
};
use crate::storage::{Wal, WalConfig, WalReader, WalRecord, WalWriter};
use crate::types::BoxConfig;

/// The parsed numeric suffix + path of a `wal-<n>.log` file.
struct WalFile {
    idx: u64,
    path: PathBuf,
}

/// Enumerate `wal-<n>.log` files under `wal_dir`, ascending by numeric suffix.
fn list_wal_files(wal_dir: &Path) -> std::io::Result<Vec<WalFile>> {
    let mut files: Vec<WalFile> = Vec::new();
    for entry in std::fs::read_dir(wal_dir)? {
        let entry = entry?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if let Some(rest) = name.strip_prefix("wal-").and_then(|s| s.strip_suffix(".log")) {
            if let Ok(idx) = rest.parse::<u64>() {
                files.push(WalFile { idx, path });
            }
        }
    }
    files.sort_by_key(|f| f.idx);
    Ok(files)
}

/// Recover the engine's in-memory state from any existing snapshot + WAL under
/// `data_dir`, truncate the torn tail of the active file, then open the writer
/// for new appends. Returns `(handle, writer)`; `handle` owns the running writer
/// thread.
pub fn recover_and_open(
    engine: &Engine,
    data_dir: &Path,
) -> std::io::Result<(WalHandle, WalWriter)> {
    let wal_dir = data_dir.join("wal");
    std::fs::create_dir_all(&wal_dir)?;

    // 1) Load + restore the latest valid snapshot. The checkpoint tells us where
    //    in the WAL to resume replay (file index + byte offset). With no valid
    //    snapshot, replay the whole WAL from frame zero.
    let (ckpt_idx, ckpt_offset) = match crate::storage::load_latest(data_dir) {
        Ok(Some(snap)) => {
            tracing::info!(
                snapshot_id = snap.id,
                boxes = snap.boxes.len(),
                routers = snap.routers.len(),
                "restored snapshot"
            );
            let ckpt = engine_snapshot::restore(engine, snap);
            (ckpt.wal_idx, ckpt.wal_offset)
        }
        Ok(None) => (0, 0),
        Err(e) => {
            tracing::warn!(error = %e, "snapshot load failed; replaying WAL from start");
            (0, 0)
        }
    };

    let files = list_wal_files(&wal_dir)?;

    // Pre-count the frames that will be replayed (post-checkpoint) so the
    // readiness gate can report `replay_progress` (API §8.2). This is a cheap
    // framing-only scan (no body decode); the apply pass below decodes + applies.
    let total_frames = count_replay_frames(&files, ckpt_idx, ckpt_offset);
    engine.set_replay_total(total_frames);

    // 2) Replay frames after the checkpoint. Files numbered below `ckpt_idx` are
    //    fully absorbed by the snapshot ⇒ skipped. The checkpoint's own file is
    //    replayed starting at `ckpt_offset`; higher files are replayed in full.
    let mut active_idx = ckpt_idx.max(1);
    let mut active_valid_len = ckpt_offset;
    for (pos, wf) in files.iter().enumerate() {
        if wf.idx < ckpt_idx {
            continue; // absorbed by the snapshot.
        }
        let start_offset = if wf.idx == ckpt_idx { ckpt_offset } else { 0 };
        let mut r = WalReader::open(&wf.path)?;
        // Apply only frames whose *end* offset is strictly greater than the
        // checkpoint offset (the absorbed prefix of the checkpoint file is
        // skipped). `next()` then `valid_len()` reads the per-frame boundary
        // without an overlapping borrow. The Append seq-skip is the safety net,
        // but skipping by offset also protects idempotent-but-stale control
        // frames (e.g. an older BoxConfig) from overwriting snapshotted state.
        while let Some(frame) = r.next() {
            let end = r.valid_len() as u64;
            if end > start_offset {
                replay_frame(engine, frame.record);
                engine.note_replayed_frame();
            }
        }
        // The last (highest-index) file is the active one we resume appending to.
        if pos + 1 == files.len() {
            active_idx = wf.idx;
            active_valid_len = r.valid_len() as u64;
        }
    }

    // 3) Truncate the active file's torn tail (idempotent on a clean file). For a
    //    fresh dir there is no active file yet.
    if !files.is_empty() {
        let active_path = wal_dir.join(format!("wal-{:016}.log", active_idx));
        truncate_active(&active_path, active_valid_len)?;
    } else {
        active_idx = active_idx.max(1);
        active_valid_len = 0;
    }

    // 5) Re-derive droppable/orphan segments and reclaim them idempotently
    //    (ARCHITECTURE §4 step 5): a cap/TTL/delete reclaim interrupted by a crash
    //    (segment registered-dead, or its unlink never completed) is re-run here,
    //    so a reclaimed segment never resurfaces and a half-dropped one never
    //    leaks. Runs after the full index/floors/registry are rebuilt; a no-op when
    //    there are no segments (pure in-memory boxes carry no writer).
    engine.reclaim_segments_on_recovery();

    // Open the writer positioned to append after the recovered/truncated tail.
    let cfg = WalConfig::new(data_dir);
    let wal = Wal::open_at(cfg, active_idx, active_valid_len)
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    let writer = wal.writer();
    Ok((WalHandle::new(wal), writer))
}

/// Drop `wal-<n>.log` files whose index is strictly below `keep_from` — they are
/// fully absorbed by a durable snapshot's checkpoint (ARCHITECTURE §3.1). The
/// active (checkpoint) file is retained so replay can resume from its offset.
pub fn drop_absorbed_wal_files(data_dir: &Path, keep_from: u64) {
    let wal_dir = data_dir.join("wal");
    let Ok(files) = list_wal_files(&wal_dir) else {
        return;
    };
    for wf in files {
        if wf.idx < keep_from {
            let _ = std::fs::remove_file(&wf.path);
        }
    }
}

/// Count the WAL frames that will be replayed (post-checkpoint) without
/// decoding their bodies — a cheap framing scan to size the readiness gate's
/// `replay_progress`. Mirrors the apply pass's file/offset selection exactly so
/// the count matches the number of `note_replayed_frame` calls.
fn count_replay_frames(files: &[WalFile], ckpt_idx: u64, ckpt_offset: u64) -> u64 {
    let mut n = 0u64;
    for wf in files {
        if wf.idx < ckpt_idx {
            continue;
        }
        let start_offset = if wf.idx == ckpt_idx { ckpt_offset } else { 0 };
        let Ok(mut r) = WalReader::open(&wf.path) else {
            continue;
        };
        while r.next().is_some() {
            if (r.valid_len() as u64) > start_offset {
                n += 1;
            }
        }
    }
    n
}

/// Truncate `path` so trailing bytes past `valid_len` (a torn tail or
/// preallocation zeros) cannot be misread as data, then fsync.
fn truncate_active(path: &Path, valid_len: u64) -> std::io::Result<()> {
    let f = std::fs::OpenOptions::new().read(true).write(true).open(path)?;
    f.set_len(valid_len)?;
    f.sync_all()?;
    Ok(())
}

/// Apply one replayed WAL record to the engine's in-memory state **without**
/// re-logging it (recovery must not write back to the WAL).
fn replay_frame(engine: &Engine, record: WalRecord) {
    match record {
        WalRecord::Append {
            box_id,
            seq,
            ts,
            node,
            tag,
            data,
        } => {
            // The box must exist (its BoxConfig create frame preceded this in WAL
            // order, or it came from the snapshot). If somehow absent, lazily
            // materialize it with defaults.
            let b = engine.get_box_by_id(box_id).unwrap_or_else(|| {
                let (_c, _id) = engine.apply_put_box_for_recovery(
                    &format!("box-{box_id}"),
                    BoxConfig::default(),
                    Some(box_id),
                );
                engine
                    .get_box_by_id(box_id)
                    .expect("box materialized for replay")
            });
            // Idempotent overlap: a frame whose seq is already covered by the
            // snapshot (<= head) was materialized — skip it (ARCHITECTURE §4).
            if seq <= b.head_seq() {
                return;
            }
            let (rdata, meta) = decode_record_payload(&data);
            engine.apply_append_for_recovery(
                &b,
                seq,
                ReplayRecord {
                    ts: ts as i64,
                    node,
                    tag,
                    data: rdata,
                    meta,
                },
            );
        }
        WalRecord::Delete {
            box_id,
            before_seq,
            match_,
            seqs,
            ts,
        } => {
            if let Some(b) = engine.get_box_by_id(box_id) {
                if !seqs.is_empty() {
                    // Explicit seq set (queue ack / dead-letter delete): remove
                    // exactly these seqs (deterministic replay, DESIGN §10.4).
                    b.delete_seqs(&seqs, ts as i64);
                } else {
                    let filter = match_.map(|m| matchsel_to_filter(&m));
                    b.apply_delete(before_seq, filter.as_ref(), ts as i64);
                }
            }
        }
        WalRecord::BoxConfig {
            box_id,
            op,
            tombstone,
            ..
        } => {
            if tombstone {
                engine.remove_box_for_recovery(&op.name);
            } else {
                let config: BoxConfig = serde_json::from_slice(&op.config).unwrap_or_default();
                engine.apply_put_box_for_recovery(&op.name, config, Some(box_id));
            }
        }
        WalRecord::RouterCreate { op, .. } => {
            engine.apply_router_create_for_recovery(op);
        }
        WalRecord::RouterDelete { name, .. } => {
            engine.apply_router_delete_for_recovery(&name);
        }
        WalRecord::EvictWatermark {
            box_id,
            evict_floor,
            ..
        } => {
            if let Some(b) = engine.get_box_by_id(box_id) {
                let mut floors = b.floors.write();
                if evict_floor > floors.evict_floor {
                    floors.evict_floor = evict_floor;
                }
            }
        }
        WalRecord::Lease {
            box_id,
            seq,
            event,
            node,
            lease_id,
            deadline,
            deliveries,
            ..
        } => {
            // Replay a durable leases-log event into the box's lease projection
            // (DESIGN §10.1). Only durable lease frames survive a crash; with the
            // default non-durable leases log nothing replays here and every
            // in-flight job is claimable again (self-healing, DESIGN §10.6).
            if let Some(b) = engine.get_box_by_id(box_id) {
                if let Some(q) = &b.queue {
                    let mut q = q.lock();
                    q.apply_lease_event(event, seq, node, lease_id, deadline as i64, deliveries);
                }
            }
        }
        // CheckpointMark is the snapshot flush barrier / boundary; replay no-op.
        WalRecord::CheckpointMark { .. } => {}
    }
}
