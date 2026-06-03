//! Metadata + materialized-state snapshot (ARCHITECTURE §5 metadata store, §3
//! checkpoint). A snapshot lets recovery start without replaying the WAL from
//! frame zero: it captures everything needed to rebuild the in-memory state,
//! plus the **checkpoint position** — the WAL `(file index, byte offset)` the
//! snapshot corresponds to — so replay resumes from exactly there.
//!
//! # What a snapshot captures
//!
//! - the topic registry: name↔interned `topic_id`, per-topic [`SnapshotTopicConfig`]
//!   (the serialized [`crate::types::TopicConfig`]) and `epoch`;
//! - per-topic materialized state: `base_seq`, `head_seq`, the three floors
//!   (`evict_floor`/`expiry_floor`/`delete_floor`), `delete_below`, retained
//!   `bytes`/`count`, and the **live record set** (every physically-present,
//!   non-deleted record at snapshot time — the compacted form, so deleted
//!   middle-holes and front-reclaimed prefixes are simply absent);
//! - routers (the full forwarding rules);
//! - `next_topic_id` (so ids stay stable across restart);
//! - the checkpoint: `(wal_idx, wal_offset)` + `last_checkpoint_seq`.
//!
//! Idempotency-dedupe state is intentionally **not** persisted (a best-effort
//! retry window, not durable state — matching the Stage-2 note).
//!
//! # On-disk format & atomicity (ARCHITECTURE §5)
//!
//! Snapshots live under `<data_dir>/meta/` as `snapshot-<n>.bin` (zero-padded,
//! monotonically increasing `n`). The body is postcard-encoded (compact binary;
//! `serde`) framed by a small fixed header: a 4-byte magic, a `u32` version, a
//! `u32` body length, and an XXH3-64 over the body — so a torn/partial snapshot
//! is detected and skipped on load (recovery falls back to the previous valid
//! snapshot, or to a full WAL replay if none is valid). A write is atomic:
//! body → `snapshot-<n>.bin.tmp`, fsync the tmp file, rename over the final
//! name, fsync the directory. The previous snapshot is removed only after the
//! new one is durably in place.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::fs::{Fs, OpenOpts, RealFs};

/// Magic bytes prefixing every snapshot file (`"SNP1"`).
const SNAPSHOT_MAGIC: [u8; 4] = *b"SNP1";
/// Snapshot format version (bumped on any incompatible body change).
const SNAPSHOT_VERSION: u32 = 3;
/// Header: magic(4) + version(4) + body_len(4) + crc(4).
const SNAPSHOT_HEADER_LEN: usize = 20;

// ---------------------------------------------------------------------------
// Serialized snapshot model
// ---------------------------------------------------------------------------

/// One record as captured in a snapshot. The compacted/materialized form: only
/// physically-present, non-deleted records are written (deleted holes and
/// front-reclaimed prefixes are simply omitted), so the snapshot is the live
/// set at checkpoint time. `data`/`meta` are JSON encoded as UTF-8 bytes (the
/// opaque payload blob) so the snapshot body stays self-describing without
/// pulling `serde_json::Value` into postcard's model.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotRecord {
    pub seq: u64,
    pub ts: i64,
    pub node: Option<String>,
    pub tag: Option<String>,
    /// JSON-encoded `data` value.
    pub data_json: Vec<u8>,
    /// JSON-encoded `meta` value, absent when the record had no meta.
    pub meta_json: Option<Vec<u8>>,
    /// Accounted payload bytes (kept so retained-byte totals match exactly).
    pub bytes: u64,
}

/// Per-topic materialized state captured in a snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotTopic {
    pub name: String,
    pub topic_id: u64,
    pub epoch: u64,
    /// Postcard-opaque: the JSON-encoded [`crate::types::TopicConfig`].
    pub config_json: Vec<u8>,
    pub base_seq: u64,
    pub head_seq: u64,
    pub evict_floor: u64,
    pub expiry_floor: u64,
    pub delete_floor: u64,
    pub delete_below: u64,
    pub bytes_retained: u64,
    pub live_count: u64,
    /// The live record set (ascending by seq).
    pub records: Vec<SnapshotRecord>,
    /// The derived-router source-trim floor: the highest
    /// dest seq a router could not materialize because the SOURCE trimmed the
    /// corresponding record. It surfaces a `source_trim` tombstone instead of a
    /// silent gap, so it MUST persist across a snapshot or a previously-surfaced
    /// `source_trim` gap would degrade into a silent deleted gap after restart.
    /// Trailing + `#[serde(default)]` so a snapshot from a build predating it decodes
    /// with `0` (no source-trim history).
    #[serde(default)]
    pub source_trim_floor: u64,
}

/// A router as captured in a snapshot. Mirrors [`crate::storage::RouterOp`] plus
/// the forward cursor / total so forwarding resumes from the right point.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotRouter {
    pub name: String,
    pub source: String,
    pub dest: String,
    pub preserve_node: bool,
    pub preserve_tag: bool,
    pub create_dest: bool,
    pub allow_cycle: bool,
    #[serde(default)]
    pub exactly_once: bool,
    /// Forward filter encoded as `(op, value)`: op `0`=Eq `1`=Glob; `None` ⇒ no
    /// filter.
    pub filter: Option<(u8, String)>,
    pub forward_cursor: u64,
    pub forwarded_total: u64,
    /// Deterministic dest-seq base (the dest seq just below this router's first
    /// forwarded record); the next forwarded dest seq is `dest_base +
    /// forwarded_total + 1`. Trailing + `#[serde(default)]` so a snapshot from a
    /// build predating the async/derived router decodes with `0` (postcard leaves a
    /// missing trailing field empty); recovery then re-seeds it deterministically.
    #[serde(default)]
    pub dest_base: u64,
}

/// The checkpoint position a snapshot corresponds to. With WAL sharding the
/// snapshot records a position **per WAL group**: `shards[i] = (wal_idx,
/// wal_offset)` is group `i`'s active WAL file index + byte offset at snapshot
/// time, and `shard_keys[i]` is that group's PHYSICAL identity (the relative dir
/// name under `wal/`: `""` for the flat single-shard layout, `shard-NN` for shard
/// `NN`). Recovery resumes each group's replay from its own offset and drops that
/// group's WAL files numbered below its `wal_idx` (they are fully absorbed).
///
/// # Why positions are keyed by physical group, not bare shard index
///
/// A bare numeric shard index conflates physically-distinct WAL groups across a
/// `TOPICS_WAL_SHARDS` reconfigure: a multi-shard snapshot records `shards[0]` for
/// `shard-00/`, but a later single-shard run writes to the FLAT `wal/` group (also
/// "index 0"). Applying `shard-00`'s old offset to the flat group's freshly-written
/// frames would skip acked frames or regress control state (codex P0 #1/#3). Keying
/// by `shard_keys[i]` (the relative dir) makes the flat group and `shard-00/`
/// distinct, so an offset is only ever applied to the group it was measured against.
/// The snapshot also records absorbed positions for EVERY discovered group on disk
/// (current writers AND any leftover/orphan group from a prior layout), so even if
/// orphan removal later fails, recovery still has each group's absorbed offset and
/// never replays an absorbed group's control frames from zero (codex P0 #2).
///
/// The leading `wal_idx`/`wal_offset` mirror `shards[0]` for the single-shard
/// layout and serve as the flat checkpoint position when `shards` is empty.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checkpoint {
    /// Numeric suffix of the (shard-0 / single-shard) WAL file the snapshot's tail
    /// is in. Mirrors `shards[0].0` when `shards` is populated.
    pub wal_idx: u64,
    /// Byte offset within `wal-<wal_idx>.log` of the first un-checkpointed frame
    /// (shard 0 / single shard). Mirrors `shards[0].1`.
    pub wal_offset: u64,
    /// Highest global seq absorbed by the snapshot (informational; the byte
    /// offset is the authoritative replay boundary).
    pub last_checkpoint_seq: u64,
    /// Per-group `(wal_idx, wal_offset)` positions. Empty means a single flat-group
    /// position stored in `(wal_idx, wal_offset)`. Includes a position for EVERY WAL group present on
    /// disk at snapshot time, not just the current shard count, so a leftover group
    /// from a prior layout is also recorded as fully absorbed.
    #[serde(default)]
    pub shards: Vec<(u64, u64)>,
    /// The physical identity of each entry in `shards`: `shard_keys[i]` is the
    /// relative dir name under `wal/` for group `i` (`""` flat, `shard-NN` sharded).
    /// Parallel to `shards`. Empty means the flat group is matched by the leading
    /// `(wal_idx, wal_offset)` fields, or sharded groups are matched by index.
    #[serde(default)]
    pub shard_keys: Vec<String>,
}

impl Checkpoint {
    /// The per-group `(wal_idx, wal_offset)` positions, falling back to the flat
    /// single position for an older (pre-sharding) snapshot whose `shards` is empty.
    /// Used by absorbed-file dropping, which only needs the offsets.
    pub fn shard_positions(&self) -> Vec<(u64, u64)> {
        if self.shards.is_empty() {
            vec![(self.wal_idx, self.wal_offset)]
        } else {
            self.shards.clone()
        }
    }

    /// The checkpoint position for the WAL group whose relative dir name is `key`
    /// (`""` flat, `shard-NN` sharded), or `None` when this snapshot recorded no
    /// position for that physical group (⇒ recovery replays it from frame zero,
    /// never an over-skip). Matches by PHYSICAL group identity so a flat group and a
    /// `shard-00/` group are never conflated across a shard-count reconfigure.
    ///
    /// When `shard_keys` is empty, the flat group (`key == ""`) uses
    /// `(wal_idx, wal_offset)` only when the snapshot was itself single-position/flat;
    /// a `shard-NN` key maps positionally to `shards[NN]`.
    pub fn position_for_key(&self, key: &str) -> Option<(u64, u64)> {
        if !self.shard_keys.is_empty() {
            return self
                .shard_keys
                .iter()
                .position(|k| k == key)
                .and_then(|i| self.shards.get(i).copied());
        }
        if key.is_empty() {
            if self.shards.len() <= 1 {
                return Some((self.wal_idx, self.wal_offset));
            }
            return None;
        }
        key.strip_prefix("shard-")
            .and_then(|s| s.parse::<usize>().ok())
            .and_then(|idx| self.shards.get(idx).copied())
    }
}

/// The full snapshot body.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Snapshot {
    /// Monotonic snapshot id (== the file's `<n>`); informational on load.
    pub id: u64,
    /// Server commit ms when the snapshot was taken.
    pub ts: u64,
    pub next_topic_id: u64,
    pub checkpoint: Checkpoint,
    pub topics: Vec<SnapshotTopic>,
    pub routers: Vec<SnapshotRouter>,
}

impl Snapshot {
    /// Encode the body with postcard.
    fn encode_body(&self) -> Result<Vec<u8>, SnapshotError> {
        postcard::to_allocvec(self).map_err(|e| SnapshotError::Encode(e.to_string()))
    }

    /// Decode a body from postcard bytes.
    fn decode_body(bytes: &[u8]) -> Result<Snapshot, SnapshotError> {
        postcard::from_bytes(bytes).map_err(|e| SnapshotError::Decode(e.to_string()))
    }
}

/// Snapshot read/write errors.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("snapshot io error: {0}")]
    Io(#[from] io::Error),
    #[error("snapshot encode error: {0}")]
    Encode(String),
    #[error("snapshot decode error: {0}")]
    Decode(String),
    #[error("snapshot framing invalid: {0}")]
    Framing(String),
}

// ---------------------------------------------------------------------------
// Atomic write / torn-safe read
// ---------------------------------------------------------------------------

/// The `meta` subdirectory under a data dir.
pub fn meta_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("meta")
}

/// `snapshot-<n>.bin` for id `n` (zero-padded, sorts numerically).
fn snapshot_name(id: u64) -> String {
    format!("snapshot-{id:016}.bin")
}

/// Write `snapshot` atomically under `<data_dir>/meta/snapshot-<id>.bin`:
/// encode → frame (magic+version+len+crc) → write to a `.tmp` → fsync tmp →
/// rename over the final name → fsync the directory. Then remove any older
/// snapshot files (kept until this one is durably in place — crash-atomic swap).
pub fn write_snapshot(data_dir: &Path, snapshot: &Snapshot) -> Result<(), SnapshotError> {
    write_snapshot_with(&RealFs::arc(), data_dir, snapshot)
}

/// As [`write_snapshot`], routing every byte through the injected `fs`. Production
/// passes a [`RealFs`] (transparent); the crash harness passes a fake so a power
/// loss anywhere in tmp-write → fsync → rename → dir-fsync → prune-old can be
/// modelled.
pub fn write_snapshot_with(
    fs: &Arc<dyn Fs>,
    data_dir: &Path,
    snapshot: &Snapshot,
) -> Result<(), SnapshotError> {
    let dir = meta_dir(data_dir);
    fs.create_dir_all(&dir)?;

    let body = snapshot.encode_body()?;
    let crc = xxhash_rust::xxh3::xxh3_64(&body);

    let mut framed = Vec::with_capacity(SNAPSHOT_HEADER_LEN + body.len());
    framed.extend_from_slice(&SNAPSHOT_MAGIC);
    framed.extend_from_slice(&SNAPSHOT_VERSION.to_le_bytes());
    framed.extend_from_slice(&(body.len() as u32).to_le_bytes());
    framed.extend_from_slice(&crc.to_le_bytes());
    framed.extend_from_slice(&body);

    let final_path = dir.join(snapshot_name(snapshot.id));
    let tmp_path = dir.join(format!("{}.tmp", snapshot_name(snapshot.id)));

    // Write + fsync the tmp file (looping over any short write).
    {
        let mut f = fs.open(&tmp_path, OpenOpts::create_truncate())?;
        write_all_at(f.as_mut(), 0, &framed)?;
        f.sync_all()?;
    }
    // Atomic rename over the final name, then fsync the directory so the rename
    // (a directory metadata change) is itself durable.
    fs.rename(&tmp_path, &final_path)?;
    // Named crash point: the tmp→final rename has issued but the directory has NOT
    // been fsynced yet, so the rename may roll back on crash (FakeDisk models
    // rename-durable-only-after-sync_dir). The F-SNAP-CRASH-AFTER-RENAME-BEFORE-
    // DIRFSYNC oracle: recovery loads either the new or the previous snapshot —
    // exactly one valid snapshot, never zero. No-op without `--features failpoints`.
    fail::fail_point!("snapshot::after_rename");
    // Named crash point: just before the directory fsync that hardens the rename
    // (F-SNAP-CRASH-AFTER-TMP-BEFORE-RENAME's sibling — see also recovery). A crash
    // here leaves the rename non-durable; recovery falls back to the old snapshot.
    fail::fail_point!("snapshot::before_dirfsync");
    fs.sync_dir(&dir)?;

    // Remove older snapshots now that the new one is durably in place.
    for existing in list_snapshot_files(fs, &dir)? {
        if existing.id < snapshot.id {
            let _ = fs.remove_file(&existing.path);
        }
    }
    Ok(())
}

/// Write the whole of `bytes` at `offset`, looping over short writes.
fn write_all_at(f: &mut dyn super::fs::File, offset: u64, bytes: &[u8]) -> io::Result<()> {
    let mut written = 0usize;
    while written < bytes.len() {
        let n = f.write_at(offset + written as u64, &bytes[written..])?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "write_at made no progress",
            ));
        }
        written += n;
    }
    Ok(())
}

/// A discovered snapshot file: parsed id + path.
struct SnapshotFile {
    id: u64,
    path: PathBuf,
}

/// Enumerate `snapshot-<n>.bin` files in `dir`, ascending by id (ignores `.tmp`).
fn list_snapshot_files(fs: &Arc<dyn Fs>, dir: &Path) -> io::Result<Vec<SnapshotFile>> {
    let mut out = Vec::new();
    for path in fs.read_dir(dir)? {
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if let Some(rest) = name
            .strip_prefix("snapshot-")
            .and_then(|s| s.strip_suffix(".bin"))
        {
            if let Ok(id) = rest.parse::<u64>() {
                out.push(SnapshotFile { id, path });
            }
        }
    }
    out.sort_by_key(|f| f.id);
    Ok(out)
}

/// The next snapshot id to use under `<data_dir>/meta` (highest existing + 1, or
/// 1 for a fresh dir).
pub fn next_snapshot_id(data_dir: &Path) -> u64 {
    next_snapshot_id_with(&RealFs::arc(), data_dir)
}

/// As [`next_snapshot_id`], routed through `fs`.
pub fn next_snapshot_id_with(fs: &Arc<dyn Fs>, data_dir: &Path) -> u64 {
    let dir = meta_dir(data_dir);
    list_snapshot_files(fs, &dir)
        .ok()
        .and_then(|v| v.last().map(|f| f.id + 1))
        .unwrap_or(1)
}

/// Load the latest **valid** snapshot under `<data_dir>/meta`, if any. Walks
/// snapshots newest-first and returns the first that frames + CRC-validates +
/// decodes; a torn/corrupt newest snapshot is skipped in favour of an older
/// valid one (and `None` if none is valid ⇒ caller falls back to full WAL
/// replay). A missing `meta` dir is a clean `None` (fresh start).
pub fn load_latest(data_dir: &Path) -> Result<Option<Snapshot>, SnapshotError> {
    load_latest_with(&RealFs::arc(), data_dir)
}

/// As [`load_latest`], routed through `fs`. Recovery passes the same `fs` the WAL
/// reads through so the whole load path is governed by one injected FS.
pub fn load_latest_with(
    fs: &Arc<dyn Fs>,
    data_dir: &Path,
) -> Result<Option<Snapshot>, SnapshotError> {
    let dir = meta_dir(data_dir);
    let files = list_snapshot_files(fs, &dir)?;
    for f in files.into_iter().rev() {
        match read_snapshot_file(fs, &f.path) {
            Ok(snap) => return Ok(Some(snap)),
            Err(e) => {
                tracing::warn!(path = %f.path.display(), error = %e, "skipping invalid snapshot");
                continue;
            }
        }
    }
    Ok(None)
}

/// Read + validate a single snapshot file.
fn read_snapshot_file(fs: &Arc<dyn Fs>, path: &Path) -> Result<Snapshot, SnapshotError> {
    let f = fs.open(path, OpenOpts::read_only())?;
    let mut buf = Vec::new();
    f.read_to_end_from(0, &mut buf)?;
    if buf.len() < SNAPSHOT_HEADER_LEN {
        return Err(SnapshotError::Framing("file shorter than header".into()));
    }
    if buf[0..4] != SNAPSHOT_MAGIC {
        return Err(SnapshotError::Framing("bad magic".into()));
    }
    let version = u32::from_le_bytes(buf[4..8].try_into().unwrap());
    if version != SNAPSHOT_VERSION {
        return Err(SnapshotError::Framing(format!(
            "unsupported version {version}"
        )));
    }
    let body_len = u32::from_le_bytes(buf[8..12].try_into().unwrap()) as usize;
    let stored_crc = u64::from_le_bytes(buf[12..20].try_into().unwrap());
    let body_start = SNAPSHOT_HEADER_LEN;
    let body_end = body_start
        .checked_add(body_len)
        .ok_or_else(|| SnapshotError::Framing("body_len overflow".into()))?;
    if buf.len() < body_end {
        return Err(SnapshotError::Framing(
            "body overruns file (torn write)".into(),
        ));
    }
    let body = &buf[body_start..body_end];
    if xxhash_rust::xxh3::xxh3_64(body) != stored_crc {
        return Err(SnapshotError::Framing("crc mismatch (torn/corrupt)".into()));
    }
    Snapshot::decode_body(body)
}

// ===========================================================================
// Unit tests
// ===========================================================================
#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Snapshot {
        Snapshot {
            id: 7,
            ts: 1_700_000_000_000,
            next_topic_id: 4,
            checkpoint: Checkpoint {
                wal_idx: 3,
                wal_offset: 4096,
                last_checkpoint_seq: 1234,
                shards: vec![(3, 4096)],
                shard_keys: vec![String::new()],
            },
            topics: vec![SnapshotTopic {
                name: "jobs".into(),
                topic_id: 1,
                epoch: 1,
                config_json: b"{\"durable\":true}".to_vec(),
                base_seq: 1,
                head_seq: 3,
                evict_floor: 0,
                expiry_floor: 0,
                delete_floor: 0,
                delete_below: 0,
                bytes_retained: 30,
                live_count: 3,
                records: vec![
                    SnapshotRecord {
                        seq: 1,
                        ts: 100,
                        node: Some("n".into()),
                        tag: Some("t".into()),
                        data_json: b"{\"i\":1}".to_vec(),
                        meta_json: None,
                        bytes: 10,
                    },
                    SnapshotRecord {
                        seq: 2,
                        ts: 101,
                        node: None,
                        tag: None,
                        data_json: b"{\"i\":2}".to_vec(),
                        meta_json: Some(b"{\"k\":1}".to_vec()),
                        bytes: 20,
                    },
                ],
                source_trim_floor: 0,
            }],
            routers: vec![SnapshotRouter {
                name: "jobs->audit".into(),
                source: "jobs".into(),
                dest: "audit".into(),
                preserve_node: true,
                preserve_tag: false,
                create_dest: true,
                allow_cycle: false,
                exactly_once: false,
                filter: Some((1, "t:".into())),
                forward_cursor: 3,
                forwarded_total: 3,
                dest_base: 0,
            }],
        }
    }

    #[test]
    fn snapshot_round_trips_identically() {
        let dir = tempfile::tempdir().unwrap();
        let snap = sample();
        write_snapshot(dir.path(), &snap).unwrap();
        let loaded = load_latest(dir.path()).unwrap().expect("a snapshot");
        assert_eq!(loaded, snap, "snapshot must round-trip byte-for-byte");
    }

    #[test]
    fn fresh_dir_has_no_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        assert!(load_latest(dir.path()).unwrap().is_none());
        assert_eq!(next_snapshot_id(dir.path()), 1);
    }

    #[test]
    fn newer_snapshot_supersedes_and_old_is_removed() {
        let dir = tempfile::tempdir().unwrap();
        let mut s1 = sample();
        s1.id = 1;
        write_snapshot(dir.path(), &s1).unwrap();
        let mut s2 = sample();
        s2.id = 2;
        s2.checkpoint.last_checkpoint_seq = 9999;
        write_snapshot(dir.path(), &s2).unwrap();

        let loaded = load_latest(dir.path()).unwrap().unwrap();
        assert_eq!(loaded.id, 2);
        assert_eq!(loaded.checkpoint.last_checkpoint_seq, 9999);
        // The older snapshot file was pruned.
        let remaining = list_snapshot_files(&RealFs::arc(), &meta_dir(dir.path())).unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].id, 2);
        // next id advances past the latest.
        assert_eq!(next_snapshot_id(dir.path()), 3);
    }

    #[test]
    fn torn_snapshot_is_skipped_for_previous_valid() {
        let dir = tempfile::tempdir().unwrap();
        let mut s1 = sample();
        s1.id = 1;
        write_snapshot(dir.path(), &s1).unwrap();
        // Write a newer snapshot, then corrupt its body (flip a CRC'd byte).
        let mut s2 = sample();
        s2.id = 2;
        write_snapshot(dir.path(), &s2).unwrap();
        // write_snapshot pruned s1 (id<2); re-create s1 so a valid older exists.
        write_snapshot(dir.path(), &s1).unwrap(); // id=1 again; prunes nothing (1<1 false)
                                                  // Now corrupt s2.
        let s2_path = meta_dir(dir.path()).join(snapshot_name(2));
        let mut bytes = std::fs::read(&s2_path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF; // body byte ⇒ CRC mismatch.
        std::fs::write(&s2_path, &bytes).unwrap();

        // load_latest must skip the corrupt id=2 and return the valid id=1.
        let loaded = load_latest(dir.path())
            .unwrap()
            .expect("falls back to valid");
        assert_eq!(loaded.id, 1);
    }

    #[test]
    fn truncated_snapshot_is_detected() {
        let dir = tempfile::tempdir().unwrap();
        let snap = sample();
        write_snapshot(dir.path(), &snap).unwrap();
        let path = meta_dir(dir.path()).join(snapshot_name(snap.id));
        let bytes = std::fs::read(&path).unwrap();
        // Chop the body in half (simulate an interrupted write past the header).
        std::fs::write(&path, &bytes[..bytes.len() / 2]).unwrap();
        // No valid snapshot remains.
        assert!(load_latest(dir.path()).unwrap().is_none());
    }
}
