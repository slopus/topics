//! WAL + snapshot recovery (ARCHITECTURE §4): on startup, load the latest valid
//! metadata snapshot, then replay **every** WAL shard from its checkpoint
//! position to rebuild the in-memory index, truncate any torn tail, then resume
//! the (sharded) writer for new appends.
//!
//! # Shard-count-agnostic recovery (WAL sharding)
//!
//! The WAL is split into N independent shards (one writer thread / file set each,
//! see [`crate::storage::sharded_wal`]). Recovery is driven by the WAL FILES on
//! disk, NOT by the configured shard count: it discovers and replays the flat
//! layout (`wal/wal-<idx>.log`, the single-shard / legacy layout) AND every
//! `wal/shard-NN/` subdir, dispatching each frame to its topic by `topic_id` (never
//! assuming a topic lives in `topic_id % N`). This lets `TOPICS_WAL_SHARDS` be
//! reconfigured between restarts with no data loss — a dir written with K shards
//! recovers correctly when reopened with any N. The NEW writers use the current
//! layout; previous-layout files are absorbed + dropped at the next snapshot.
//!
//! Recovery order (ARCHITECTURE §4):
//!
//! 1. Load the latest valid snapshot under `<data_dir>/meta` (if any) and restore
//!    the topic registry, per-topic materialized state + floors, routers, and
//!    `next_topic_id`. The snapshot's checkpoint carries a PER-SHARD `(wal_idx,
//!    wal_offset)`; replay resumes each shard from its own offset. A missing/torn
//!    snapshot ⇒ start empty and replay every WAL file from frame zero.
//! 2. Replay frames **after each shard's checkpoint**, in-stream, dispatched by
//!    `topic_id`. Files below a shard's checkpoint index are absorbed (skipped):
//!
//!    - `Append`  → re-insert at its logged seq, unless `seq <= head` (snapshot
//!      overlap ⇒ skipped, idempotent). An out-of-contiguity append from a
//!      shard-count reconfigure is deferred + re-applied seq-sorted.
//!    - `Delete`  → re-apply the `before_seq`/`match` selector (idempotent).
//!    - `TopicConfig` (create/update / tombstone) → create/update or remove a topic.
//!    - `RouterCreate`/`RouterDelete` → rebuild the router graph.
//!    - `EvictWatermark` → restore the cap/TTL floor.
//!
//! 3. Truncate each shard's active-file torn tail (length overrun / CRC) so a new
//!    append can never be confused with a partial one.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::engine::{
    decode_record_payload, matchsel_to_filter, snapshot as engine_snapshot, wal_glue::WalHandle,
    Engine, ReplayRecord,
};
use crate::storage::{
    shard_wal_dir, Fs, OpenOpts, RealFs, ShardedWal, ShardedWalWriter, WalConfig, WalReader,
    WalRecord,
};
use crate::types::TopicConfig;

/// The parsed numeric suffix + path of a `wal-<n>.log` file.
struct WalFile {
    idx: u64,
    path: PathBuf,
}

/// A discovered on-disk WAL shard group: the files of one writer (the flat
/// `wal/` layout, or one `wal/shard-NN/` subdir), plus the shard index that
/// names it. The flat layout is shard index `0` (it is the single-shard / legacy
/// layout). Recovery replays EVERY discovered group regardless of how many shards
/// the current config asks for — the shard-count-agnostic-replay property.
struct WalShardGroup {
    /// The shard index this group's name implies (flat ⇒ 0, `shard-NN` ⇒ NN). Used
    /// only for a deterministic sort order; the AUTHORITATIVE checkpoint match is by
    /// physical group `key()`, never this bare index (codex P0 #1/#3).
    shard_idx: usize,
    /// The physical directory this group's files live in (`wal/` for the flat
    /// layout, or `wal/shard-NN/`). Stored explicitly so a flat group and a
    /// `shard-00/` group (both `shard_idx == 0`) are never conflated.
    dir: PathBuf,
    /// `wal-<n>.log` files in this group, ascending by index.
    files: Vec<WalFile>,
}

impl WalShardGroup {
    /// This group's PHYSICAL identity for checkpoint matching: the relative dir name
    /// under `wal/` (`""` for the flat layout, `shard-NN` for a sharded subdir).
    /// Matches the key the snapshot recorded via [`crate::storage::shard_group_key`],
    /// so a position is only ever applied to the exact physical group it was measured
    /// against. `wal_dir` is `<data_dir>/wal`.
    fn key(&self, wal_dir: &Path) -> String {
        if self.dir == wal_dir {
            String::new()
        } else {
            self.dir
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_string()
        }
    }
}

/// Enumerate `wal-<n>.log` files under `wal_dir`, ascending by numeric suffix.
fn list_wal_files(fs: &Arc<dyn Fs>, wal_dir: &Path) -> std::io::Result<Vec<WalFile>> {
    let mut files: Vec<WalFile> = Vec::new();
    let entries = match fs.read_dir(wal_dir) {
        Ok(e) => e,
        // A missing shard dir is simply an empty group (e.g. a shard that never
        // received a write, or a layout that does not use this subdir).
        Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(files),
        Err(e) => return Err(e),
    };
    for path in entries {
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if let Some(rest) = name
            .strip_prefix("wal-")
            .and_then(|s| s.strip_suffix(".log"))
        {
            if let Ok(idx) = rest.parse::<u64>() {
                files.push(WalFile { idx, path });
            }
        }
    }
    files.sort_by_key(|f| f.idx);
    Ok(files)
}

/// Parse a `shard-NN` directory name into its shard index.
fn parse_shard_dir(name: &str) -> Option<usize> {
    name.strip_prefix("shard-").and_then(|s| s.parse().ok())
}

/// Discover **every** WAL shard group on disk under `<data_dir>/wal`,
/// shard-count-agnostically: the flat `wal/` layout (legacy / single-shard) AND
/// every `wal/shard-NN/` subdirectory, regardless of how many shards the current
/// config requests. This is the basis of the shard-count-agnostic-replay
/// property: a dir written with K shards is fully replayed when reopened with any
/// N (the configured count only governs the NEW writers, not which files replay).
///
/// Returns the groups sorted by `shard_idx`, each with its files ascending by
/// index. An empty result means a fresh data dir.
fn discover_shard_groups(fs: &Arc<dyn Fs>, wal_dir: &Path) -> std::io::Result<Vec<WalShardGroup>> {
    let mut groups: Vec<WalShardGroup> = Vec::new();

    // The flat layout: `wal-<n>.log` files directly under `wal/` (shard 0 / legacy
    // single-shard). Present iff a single-shard run wrote here.
    let flat = list_wal_files(fs, wal_dir)?;
    if !flat.is_empty() {
        groups.push(WalShardGroup {
            shard_idx: 0,
            dir: wal_dir.to_path_buf(),
            files: flat,
        });
    }

    // Every `shard-NN/` subdir (a multi-shard run). Discovered independently of the
    // current shard count.
    let entries = match fs.read_dir(wal_dir) {
        Ok(e) => e,
        Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(groups),
        Err(e) => return Err(e),
    };
    for path in entries {
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if let Some(shard_idx) = parse_shard_dir(name) {
            let files = list_wal_files(fs, &path)?;
            // A `shard-00` subdir AND a flat layout (both `shard_idx == 0`) can
            // coexist after a reconfigure (a prior multi-shard run wrote `shard-00`,
            // a current single-shard run writes flat). They are kept as SEPARATE
            // groups with their own `dir`, so nothing is conflated or dropped — both
            // replay, and the Append seq-skip makes any overlap idempotent.
            groups.push(WalShardGroup {
                shard_idx,
                dir: path.clone(),
                files,
            });
        }
    }

    groups.sort_by_key(|g| g.shard_idx);
    Ok(groups)
}

/// Enumerate every on-disk WAL group under `<data_dir>/wal` and return each one's
/// `(physical group key, (last_file_idx, valid_tail_len))`. The key is the relative
/// dir under `wal/` (`""` flat, `shard-NN` sharded). The tail position is the index
/// of the highest `wal-<n>.log` and its CRC-valid byte length — i.e. the offset just
/// past the last intact frame.
///
/// Used by snapshot capture (codex P0 #2) to record an ABSORBED checkpoint position
/// for every leftover/orphan group from a prior layout, so a later recovery skips an
/// already-materialized group entirely even if its files were not yet deleted — its
/// control frames can never replay-from-zero and regress snapshotted state. A group
/// whose files cannot be read is omitted (best-effort; recovery then replays it,
/// still correct).
pub(crate) fn discover_group_tails(fs: &Arc<dyn Fs>, data_dir: &Path) -> Vec<(String, (u64, u64))> {
    let wal_dir = data_dir.join("wal");
    let Ok(groups) = discover_shard_groups(fs, &wal_dir) else {
        return Vec::new();
    };
    let mut out = Vec::with_capacity(groups.len());
    for g in &groups {
        let Some(last) = g.files.last() else { continue };
        let Ok(reader) = WalReader::open_with(fs, &last.path) else {
            continue;
        };
        let valid = reader.count_valid_len() as u64;
        out.push((g.key(&wal_dir), (last.idx, valid)));
    }
    out
}

/// Pre-scan: the set of `topic_id`s whose (post-checkpoint) frames appear in MORE
/// THAN ONE discovered WAL group — i.e. topics split across groups by a shard-count
/// reconfigure. Only these need ordered buffering on replay; everything else applies
/// in-stream. A cheap framing scan (no payload decode). `topic_id == 0` (topic-agnostic
/// control frames) is excluded — those replay shard-independently in stream order.
fn find_split_topics<F>(
    fs: &Arc<dyn Fs>,
    groups: &[WalShardGroup],
    ckpt_for: &F,
) -> std::collections::HashSet<u32>
where
    F: Fn(&WalShardGroup) -> (u64, u64),
{
    // topic_id → count of distinct groups it appears in.
    let mut groups_per_topic: std::collections::HashMap<u32, usize> =
        std::collections::HashMap::new();
    for g in groups {
        let (ckpt_idx, ckpt_offset) = ckpt_for(g);
        let mut seen_here: std::collections::HashSet<u32> = std::collections::HashSet::new();
        for wf in &g.files {
            if wf.idx < ckpt_idx {
                continue;
            }
            let start_offset = if wf.idx == ckpt_idx { ckpt_offset } else { 0 };
            let Ok(mut r) = WalReader::open_with(fs, &wf.path) else {
                continue;
            };
            while let Some(frame) = r.next() {
                if (r.valid_len() as u64) > start_offset {
                    let bid = frame.record.topic_id();
                    if bid != 0 {
                        seen_here.insert(bid);
                    }
                }
            }
        }
        for bid in seen_here {
            *groups_per_topic.entry(bid).or_default() += 1;
        }
    }
    groups_per_topic
        .into_iter()
        .filter(|&(_, c)| c > 1)
        .map(|(bid, _)| bid)
        .collect()
}

/// Replay each split topic's buffered frames in reconstructed logged order. For each
/// topic we order its GROUPS by the lowest `Append` seq each group holds for that topic
/// (the older run has the lower seqs — a topic's seqs increase monotonically across
/// runs and never reset), then concatenate each group's frames preserving in-group
/// order. That is the topic's true logged order across the reconfigure, so its
/// create → config-update → append → delete frames apply in order (codex P0 #3). A
/// group that holds no `Append` for the topic (e.g. a control-only fragment) sorts by
/// its on-disk group order as a stable fallback.
fn replay_split_topics(
    engine: &Engine,
    split_frames: std::collections::HashMap<u32, Vec<(usize, usize, WalRecord)>>,
) {
    for (_topic_id, mut frames) in split_frames {
        // Per group_order, the minimum Append seq for this topic (None ⇒ no appends).
        let mut min_seq: std::collections::HashMap<usize, u64> = std::collections::HashMap::new();
        for (go, _idx, rec) in &frames {
            if let WalRecord::Append { seq, .. } = rec {
                let e = min_seq.entry(*go).or_insert(u64::MAX);
                if *seq < *e {
                    *e = *seq;
                }
            }
        }
        // Order key for a frame: (group has appends?, group rank, group_order,
        // in-group position). Append-bearing groups (`false` sorts first) order by
        // their min Append seq, so the older run replays first; an append-less group
        // (a rare control-only fragment) sorts AFTER, by its on-disk group order —
        // order-insensitive in practice. `group_order`+`in_group_idx` are the stable
        // tiebreak that preserves each group's logged order.
        frames.sort_by_key(|(go, idx, _)| {
            let rank = min_seq.get(go).copied();
            (rank.is_none(), rank.unwrap_or(0), *go, *idx)
        });
        for (_go, _idx, rec) in frames {
            replay_frame(engine, rec);
        }
    }
}

/// Recover the engine's in-memory state from any existing snapshot + WAL under
/// `data_dir`, truncate the torn tail of the active file, then open the writer
/// for new appends. Returns `(handle, writer)`; `handle` owns the running writer
/// thread.
pub fn recover_and_open(
    engine: &Engine,
    data_dir: &Path,
) -> std::io::Result<(WalHandle, ShardedWalWriter)> {
    recover_and_open_with(RealFs::arc(), engine, data_dir)
}

/// As [`recover_and_open`], routing every byte of recovery I/O (snapshot load,
/// WAL replay reads, torn-tail truncation, the resumed writer) through `fs`.
/// Production passes a [`RealFs`] (transparent); the crash harness passes the
/// same fake the pre-crash run used so recovery sees exactly the survived image.
pub fn recover_and_open_with(
    fs: Arc<dyn Fs>,
    engine: &Engine,
    data_dir: &Path,
) -> std::io::Result<(WalHandle, ShardedWalWriter)> {
    let wal_dir = data_dir.join("wal");
    fs.create_dir_all(&wal_dir)?;
    let n_shards = engine.config.wal_shards.max(1);

    // 1) Load + restore the latest valid snapshot. The checkpoint tells us where in
    //    EACH WAL group to resume replay (per-group file index + byte offset, keyed
    //    by the group's PHYSICAL identity — the relative dir under `wal/`). With no
    //    valid snapshot, replay every WAL file from frame zero.
    let checkpoint: Option<crate::storage::Checkpoint> =
        match crate::storage::load_latest_with(&fs, data_dir) {
            Ok(Some(snap)) => {
                tracing::info!(
                    snapshot_id = snap.id,
                    topics = snap.topics.len(),
                    routers = snap.routers.len(),
                    shards = snap.checkpoint.shards.len(),
                    "restored snapshot"
                );
                Some(engine_snapshot::restore(engine, snap))
            }
            Ok(None) => None,
            Err(e) => {
                tracing::warn!(error = %e, "snapshot load failed; replaying WAL from start");
                None
            }
        };
    // The checkpoint offset for a discovered WAL group, matched by its PHYSICAL group
    // key (the relative dir under `wal/`), NEVER by a bare numeric shard index. This
    // is the fix for the flat ↔ `shard-00/` conflation across a shard-count
    // reconfigure (codex P0 #1/#3): the flat group (`""`) and a `shard-00/` group
    // resolve to different recorded positions. A group the snapshot did not record
    // (key absent) replays from frame zero — never an over-skip.
    let ckpt_for = |g: &WalShardGroup| -> (u64, u64) {
        checkpoint
            .as_ref()
            .and_then(|c| c.position_for_key(&g.key(&wal_dir)))
            .unwrap_or((0, 0))
    };

    // 2) DISCOVER every WAL shard group on disk (flat + every `shard-NN/`),
    //    shard-count-agnostically. This is the property that lets TOPICS_WAL_SHARDS
    //    be reconfigured between restarts: replay is driven by the files on disk,
    //    NOT by the configured shard count.
    //
    //    Order groups so the CURRENT layout's groups replay LAST. After a shard-count
    //    reconfigure, the previous layout's WAL files coexist with the current one
    //    until the next snapshot absorbs + drops them. A topic's frames live in one
    //    group per run, but across the reconfigure its OLD frames (create + earlier
    //    seqs) sit in a previous-layout group while NEW frames (later seqs, written
    //    after reopen) sit in a current-layout group. Replaying previous-layout
    //    groups first keeps each topic's frames in seq order (older seqs before newer),
    //    so the per-topic contiguity holds (`seq == head + 1`) and nothing is dropped.
    //    Within a single layout the groups are ordered by shard_idx (irrelevant to
    //    correctness — different topics — but deterministic).
    let mut groups = discover_shard_groups(&fs, &wal_dir)?;
    // The set of physical dirs the CURRENT layout (count `n_shards`) writes to. A
    // group whose dir is in this set is "current"; one in a leftover dir from a
    // prior layout is "previous". Identified by DIR (not shard_idx) so a flat group
    // and a `shard-00/` group are distinguished when `n_shards == 1` writes flat but
    // a prior multi-shard run left `shard-00/`.
    let current_dirs: std::collections::HashSet<PathBuf> = (0..n_shards)
        .map(|s| shard_wal_dir(data_dir, s, n_shards))
        .collect();
    let is_current = |g: &WalShardGroup| current_dirs.contains(&g.dir);
    groups.sort_by_key(|g| (is_current(g), g.shard_idx));

    // Pre-count the frames that will be replayed (post-checkpoint, across ALL
    // groups) so the readiness gate can report `replay_progress` (API §8.2).
    let total_frames: u64 = groups
        .iter()
        .map(|g| {
            let (ci, co) = ckpt_for(g);
            count_replay_frames(&fs, &g.files, ci, co)
        })
        .sum();
    engine.set_replay_total(total_frames);

    // 3) Replay frames after each group's checkpoint, dispatching every frame to
    //    its topic by `topic_id` (NEVER assuming a topic lives in `topic_id % N`). Frames
    //    apply IN-STREAM in group order, so a topic's appends, deletes, config updates,
    //    and watermarks keep their logged relative order — a selector `Delete`
    //    re-derives its matched seqs from the records appended BEFORE it, exactly as
    //    logged, and a config UPDATE replays after the topic's create.
    //
    //    A topic's frames live in ONE shard per run, so within a run (within a group)
    //    they are already in logged order. A shard-count RECONFIGURE, however, splits
    //    a topic's frames across the previous-layout group and the current-layout group
    //    — and after a NON-multiple reconfigure (e.g. 3→7) the topic's NEW group can
    //    sort BEFORE its OLD group, so a naive in-stream pass would replay a NEW
    //    `Delete`/`TopicConfig`-update/append BEFORE the topic's OLD create + earlier
    //    appends. That regresses delete/config durability and breaks per-topic
    //    contiguity (codex P0 #3).
    //
    //    Fix: a topic whose frames appear in MORE THAN ONE group is "split". For split
    //    topics we BUFFER every frame and replay them in a final per-topic pass that
    //    orders the topic's groups by the LOWEST append seq each group holds for it
    //    (the older run has the lower seqs, since a topic's seqs increase monotonically
    //    across runs and never reset), preserving each group's in-group order. That
    //    reconstructs the true logged order regardless of how the groups sort on disk,
    //    so create→update→append→delete all replay in order. Non-split topics (the
    //    common case, incl. every topic in a single-layout multi-shard run where each
    //    topic is in exactly one group) apply IN-STREAM with zero buffering.
    let maybe_split = groups.len() > 1;
    // Pre-scan: which topic_ids appear in more than one group (post-checkpoint)? Only
    // these need ordered buffering. Cheap framing scan (no payload decode). topic_id 0
    // (topic-agnostic control frames) is never "split" — those frames replay in stream.
    let split_topics: std::collections::HashSet<u32> = if maybe_split {
        find_split_topics(&fs, &groups, &ckpt_for)
    } else {
        std::collections::HashSet::new()
    };
    // Buffered frames for split topics: topic_id → list of (group_order, in_group_idx,
    // record). `group_order` is the index of the group in the replay-sorted `groups`.
    let mut split_frames: std::collections::HashMap<u32, Vec<(usize, usize, WalRecord)>> =
        std::collections::HashMap::new();
    let mut group_tails: Vec<(PathBuf, u64, u64)> = Vec::with_capacity(groups.len());
    for (group_order, g) in groups.iter().enumerate() {
        let (ckpt_idx, ckpt_offset) = ckpt_for(g);
        let mut active_idx = ckpt_idx.max(1);
        let mut active_valid_len = ckpt_offset;
        let mut in_group_idx = 0usize;
        for (pos, wf) in g.files.iter().enumerate() {
            if wf.idx < ckpt_idx {
                continue; // absorbed by the snapshot for this shard.
            }
            let start_offset = if wf.idx == ckpt_idx { ckpt_offset } else { 0 };
            let mut r = WalReader::open_with(&fs, &wf.path)?;
            while let Some(frame) = r.next() {
                let end = r.valid_len() as u64;
                if end > start_offset {
                    let bid = frame.record.topic_id();
                    if !split_topics.is_empty() && split_topics.contains(&bid) {
                        // A split topic: buffer for the final ordered pass.
                        split_frames.entry(bid).or_default().push((
                            group_order,
                            in_group_idx,
                            frame.record,
                        ));
                        in_group_idx += 1;
                        engine.note_replayed_frame();
                        fail::fail_point!("recovery::mid_replay");
                        continue;
                    }
                    replay_frame(engine, frame.record);
                    in_group_idx += 1;
                    engine.note_replayed_frame();
                    // Named crash point: a SECOND crash partway through WAL replay
                    // (F-REC-CRASH-DURING-REPLAY). Replay is idempotent (Append
                    // seq-skip, delete/evict monotone), so a re-run from the durable
                    // WAL converges to the same state. No-op without failpoints.
                    fail::fail_point!("recovery::mid_replay");
                }
            }
            if pos + 1 == g.files.len() {
                active_idx = wf.idx;
                active_valid_len = r.valid_len() as u64;
            }
        }
        if !g.files.is_empty() {
            group_tails.push((g.dir.clone(), active_idx, active_valid_len));
        }
    }
    // Final pass: replay each split topic's frames in reconstructed logged order. Empty
    // in the common (no-reconfigure / no-split) path, so zero-cost there.
    if !split_frames.is_empty() {
        replay_split_topics(engine, split_frames);
    }

    // 4) Truncate each discovered group's active-file torn tail (idempotent on a
    //    clean file), in its OWN physical dir.
    fail::fail_point!("recovery::before_truncate");
    for (dir, active_idx, active_valid_len) in &group_tails {
        let active_path = dir.join(format!("wal-{:016}.log", active_idx));
        truncate_active(&fs, &active_path, *active_valid_len)?;
    }

    // 5) Compute each NEW shard's resume position. The new layout uses
    //    `shard_wal_dir(data_dir, s, n_shards)`; a new shard resumes after the
    //    highest existing valid tail in its physical dir (which may be a dir a
    //    prior run already wrote, under a different shard count). Files were already
    //    replayed above, so resuming after the (truncated) tail never loses data.
    let mut first_idx = vec![1u64; n_shards];
    let mut existing_len = vec![0u64; n_shards];
    for s in 0..n_shards {
        let dir = shard_wal_dir(data_dir, s, n_shards);
        let files = list_wal_files(&fs, &dir)?;
        if let Some(last) = files.last() {
            // Re-derive the valid tail of this physical file (it was truncated above
            // if it belonged to a discovered group; otherwise read it fresh).
            let valid = WalReader::open_with(&fs, &last.path)?.count_valid_len() as u64;
            first_idx[s] = last.idx;
            existing_len[s] = valid;
        }
    }

    // 6) Apply durable head reservations (R3). Every Append has now replayed, so
    //    any topic whose fsynced `HeadWatermark` reserved a seq BEYOND its replayed
    //    head lost the un-fsynced `disk` tail to the crash: advance its head to the
    //    reservation and pad the reserved-but-unwritten seqs as silent deleted gaps
    //    so the seq counter never regresses and an already-acked `disk` seq is
    //    never re-handed (disk-class seq monotonicity across restart).
    engine.apply_head_watermarks();

    // 7) Re-derive droppable/orphan segments and reclaim them idempotently
    //    (ARCHITECTURE §4 step 5): a cap/TTL/delete reclaim interrupted by a crash
    //    (segment registered-dead, or its unlink never completed) is re-run here,
    //    so a reclaimed segment never resurfaces and a half-dropped one never
    //    leaks. Runs after the full index/floors/registry are rebuilt; a no-op when
    //    there are no segments (pure in-memory topics carry no writer).
    engine.reclaim_segments_on_recovery();

    // Seed every topic's lock-free `is_router_source` flag from the recovered router
    // graph (codex P1), so the post-recovery write hot path forwards without taking
    // the global graph lock per append. Covers a snapshot-restored router whose
    // source topic was materialized separately, regardless of replay order.
    engine.refresh_router_source_flags();

    // Derived-router re-materialization (`forward_v2`): forwarded dest records were
    // never WAL-logged, so each derived dest's content past the last snapshot is
    // re-derived here by replaying forwarding from each router's recovered cursor
    // with deterministic dest seqs (a consumer cursor into a dest stays valid). A
    // source trimmed below a cursor surfaces a `source_trim` tombstone, never a
    // silent gap. No-op under v2-off (the legacy WAL-replayed dest Append path).
    engine.reforward_routers_on_recovery();

    // 8) Open the `n_shards` writers, each positioned to append after its recovered/
    //    truncated tail, through the same FS seam recovery read from. `n_shards == 1`
    //    uses the flat legacy layout (byte-for-byte the pre-sharding WAL).
    let cfg = WalConfig::new(data_dir);
    let wal = ShardedWal::open_at_with(fs.clone(), cfg, n_shards, &first_idx, &existing_len)
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    let writer = wal.writer();
    Ok((WalHandle::new(wal), writer))
}

/// Drop, in EACH shard, the `wal-<n>.log` files whose index is strictly below that
/// shard's checkpoint active file — they are fully absorbed by a durable
/// snapshot's checkpoint (ARCHITECTURE §3.1). Each shard's active (checkpoint)
/// file is retained so replay can resume from its offset.
///
/// `positions[s] = (wal_idx, _offset)` is shard `s`'s checkpoint; `shard_count` is
/// the live shard count (so the per-shard subdir layout matches). Also prunes any
/// ORPHAN `shard-NN/` subdir left by a prior run with MORE shards than the current
/// `shard_count` (its data was already replayed + absorbed by the snapshot), so a
/// shrink in `TOPICS_WAL_SHARDS` does not leak files forever.
pub fn drop_absorbed_wal_files(data_dir: &Path, positions: &[(u64, u64)], shard_count: usize) {
    let fs = RealFs::arc();
    let wal_dir = data_dir.join("wal");
    let n = shard_count.max(1);
    for s in 0..n {
        let keep_from = positions.get(s).map(|(idx, _)| *idx).unwrap_or(0);
        let dir = shard_wal_dir(data_dir, s, n);
        if let Ok(files) = list_wal_files(&fs, &dir) {
            for wf in files {
                if wf.idx < keep_from {
                    let _ = fs.remove_file(&wf.path);
                }
            }
        }
    }
    // Prune orphan `shard-NN/` subdirs beyond the current shard count. The active
    // layout never names these (the snapshot absorbed their frames + the live
    // writers replayed them into shards `0..n`), so their files are dead.
    if let Ok(entries) = fs.read_dir(&wal_dir) {
        for path in entries {
            let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
                continue;
            };
            if let Some(idx) = parse_shard_dir(name) {
                // Keep `shard-NN` only when the CURRENT layout uses it (`n > 1` and
                // `idx < n`). A flat (`n == 1`) layout uses no subdir, so every
                // `shard-NN/` is orphaned.
                let in_use = n > 1 && idx < n;
                if !in_use {
                    if let Ok(files) = list_wal_files(&fs, &path) {
                        for wf in files {
                            let _ = fs.remove_file(&wf.path);
                        }
                    }
                }
            }
        }
    }
}

/// Count the WAL frames that will be replayed (post-checkpoint) without
/// decoding their bodies — a cheap framing scan to size the readiness gate's
/// `replay_progress`. Mirrors the apply pass's file/offset selection exactly so
/// the count matches the number of `note_replayed_frame` calls.
fn count_replay_frames(
    fs: &Arc<dyn Fs>,
    files: &[WalFile],
    ckpt_idx: u64,
    ckpt_offset: u64,
) -> u64 {
    let mut n = 0u64;
    for wf in files {
        if wf.idx < ckpt_idx {
            continue;
        }
        let start_offset = if wf.idx == ckpt_idx { ckpt_offset } else { 0 };
        let Ok(mut r) = WalReader::open_with(fs, &wf.path) else {
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
fn truncate_active(fs: &Arc<dyn Fs>, path: &Path, valid_len: u64) -> std::io::Result<()> {
    let mut f = fs.open(path, OpenOpts::rw_existing())?;
    f.set_len(valid_len)?;
    f.sync_all()?;
    Ok(())
}

/// Apply one replayed WAL record to the engine's in-memory state **without**
/// re-logging it (recovery must not write back to the WAL).
fn replay_frame(engine: &Engine, record: WalRecord) {
    match record {
        WalRecord::Append {
            topic_id,
            seq,
            ts,
            node,
            tag,
            data,
        } => {
            // The topic must exist (its TopicConfig create frame preceded this in WAL
            // order, or it came from the snapshot). If somehow absent, lazily
            // materialize it with defaults.
            let b = engine.get_topic_by_id(topic_id).unwrap_or_else(|| {
                let (_c, _id) = engine.apply_put_topic_for_recovery(
                    &format!("topic-{topic_id}"),
                    TopicConfig::default(),
                    Some(topic_id),
                );
                engine
                    .get_topic_by_id(topic_id)
                    .expect("topic materialized for replay")
            });
            // Idempotent overlap: a frame whose seq is already covered by the
            // snapshot (<= head) was materialized — skip it (ARCHITECTURE §4).
            //
            // Contiguity guard: replay assigns the next contiguous seq
            // (`head + 1`). A *misdirected* frame whose logged seq jumps ahead
            // (`seq > head + 1`) would either open a phantom gap or (caught by the
            // debug_assert in `apply_append_for_recovery`) abort recovery. Such a
            // frame is not a record this topic legitimately produced at this point in
            // the log, so treat it as torn and ignore it: never panic, never adopt
            // a future seq as head, never punch a non-contiguous gap. The single
            // ordered WAL writer only ever appends dense per-topic seqs, so under
            // normal operation `seq == head + 1` always holds; this guard only
            // fires on a corrupted/misdirected frame.
            let head = b.head_seq();
            if seq <= head {
                return;
            }
            if seq != head + 1 {
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
            topic_id,
            before_seq,
            match_,
            seqs,
            bound_head,
            ts,
        } => {
            if let Some(b) = engine.get_topic_by_id(topic_id) {
                if !seqs.is_empty() {
                    // Explicit seq set (queue ack / dead-letter delete): remove
                    // exactly these seqs (deterministic replay, DESIGN §10.4).
                    b.delete_seqs(&seqs, ts as i64);
                } else {
                    // Selector-based delete: re-derive the matched seqs, HONORING the
                    // point-in-time `bound_head` logged with the frame so a record
                    // appended AFTER the original delete (seq >= bound_head) is never
                    // swept on replay (the point-in-time guarantee survives a crash).
                    // A legacy frame carries `bound_head = None` ⇒ fall back to the
                    // recovered head (pre-fix behavior).
                    let filter = match_.map(|m| matchsel_to_filter(&m));
                    b.apply_delete(before_seq, filter.as_ref(), bound_head, ts as i64);
                }
            }
        }
        WalRecord::TopicConfig {
            topic_id,
            op,
            tombstone,
            ..
        } => {
            if tombstone {
                engine.remove_topic_for_recovery(&op.name);
            } else {
                let config: TopicConfig = serde_json::from_slice(&op.config).unwrap_or_default();
                engine.apply_put_topic_for_recovery(&op.name, config, Some(topic_id));
            }
        }
        WalRecord::RouterCreate { op, .. } => {
            engine.apply_router_create_for_recovery(op);
        }
        WalRecord::RouterDelete { name, .. } => {
            engine.apply_router_delete_for_recovery(&name);
        }
        WalRecord::EvictWatermark {
            topic_id,
            evict_floor,
            expiry_floor,
            ..
        } => {
            // Restore the involuntary loss floors monotonically (R7 / codex P0 #2).
            // The `evict_floor` (cap-records / byte-cap) and `expiry_floor` (TTL)
            // are restored into their OWN floor fields so a relaxed cap or a
            // backward clock can never resurrect a record below a durably-logged
            // floor after restart AND the from-0 tombstone reason (ttl / cap /
            // mixed) is preserved. Each floor only ever advances (`>`), never
            // regresses, regardless of replay order. A legacy frame carries
            // `expiry_floor: 0` (folds into `evict_floor` only — the prior
            // best-effort behavior, reason fidelity not preserved for old logs).
            if let Some(b) = engine.get_topic_by_id(topic_id) {
                let mut floors = b.floors.write();
                if evict_floor > floors.evict_floor {
                    floors.evict_floor = evict_floor;
                }
                if expiry_floor > floors.expiry_floor {
                    floors.expiry_floor = expiry_floor;
                }
            }
        }
        WalRecord::Lease {
            topic_id,
            seq,
            event,
            node,
            lease_id,
            deadline,
            deliveries,
            ..
        } => {
            // Replay a durable leases-log event into the topic's lease projection
            // (DESIGN §10.1). Only durable lease frames survive a crash; with the
            // default non-durable leases log nothing replays here and every
            // in-flight job is claimable again (self-healing, DESIGN §10.6).
            //
            // A `memory`-class queue now takes the same disk-like best-effort path
            // (§0.10): its records may survive a restart, so a replayed lease is no
            // longer necessarily a ghost — replay it generically with no class
            // special-casing (the self-healing visibility timeout, DESIGN §10.6,
            // still makes any genuinely-orphaned lease claimable again).
            if let Some(b) = engine.get_topic_by_id(topic_id) {
                if let Some(q) = &b.queue {
                    let mut q = q.lock();
                    q.apply_lease_event(event, seq, node, lease_id, deadline as i64, deliveries);
                }
            }
        }
        WalRecord::HeadWatermark {
            topic_id, head_seq, ..
        } => {
            // Record the durable head reservation (R3). We DON'T pad the index
            // here: a later Append frame in the log may legitimately fill seqs
            // below this reservation, and padding now would make
            // `apply_append_for_recovery` skip them as `seq <= head`. Instead we
            // only raise the (monotone) reservation ceiling; the final
            // `apply_head_watermarks` pass (after every Append replayed) pads any
            // reserved-but-unwritten tail as deleted gaps and advances head, so an
            // already-acked `disk` seq is never re-handed. (A `memory` topic never
            // logs a HeadWatermark — it forgoes the seq-ceiling fsync, §0.10 — but
            // recovery applies any frame generically, no class special-casing.)
            if let Some(b) = engine.get_topic_by_id(topic_id) {
                b.set_reserved_head(head_seq);
            }
        }
        // CheckpointMark is the snapshot flush barrier / boundary; replay no-op.
        WalRecord::CheckpointMark { .. } => {}
    }
}
