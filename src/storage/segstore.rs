//! The [`SegmentStore`] trait and its [`LocalSegmentStore`] implementation, plus
//! the per-box [`BoxTier`] (a HOT store + an optional COLD store).
//!
//! # Why a trait
//!
//! Tiered storage (the Phase-6 milestone) keeps the active + recent sealed
//! segments on fast NVMe (HOT, under the data dir) and relocates older sealed
//! segments to a slower tier (COLD). v1's cold tier is a different configured
//! folder (`STREAMS_COLD_DIR`); putting both behind one trait lets an object
//! store (S3) drop in later as another impl **without touching the engine**. The
//! S3 impl is explicitly future work — only [`LocalSegmentStore`] exists now.
//!
//! # Blocking-pool friendly (the hot-path invariant)
//!
//! The methods are **synchronous and self-contained** (each does its own file
//! I/O and returns owned bytes). That is deliberate: cold reads and the
//! relocator run on a separate blocking/IO pool (`spawn_blocking`), so a slow
//! cold fetch never holds a box write lock or blocks an SSE push (the Phase-6
//! HARD INVARIANT). A future async/S3 impl can wrap these same signatures.
//!
//! # Segment identity
//!
//! A segment is named by its **first seq** (`seg-<start_seq>`, ARCHITECTURE §6),
//! and is two objects: the `.data` frames and the `.idx` locator (see
//! [`crate::storage::segment`]). A [`SegmentId`] is that `start_seq`; the store
//! reads/writes the two files as a unit.

use std::collections::BTreeSet;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::fs::{Fs, OpenOpts, RealFs};
use super::segment::{data_name, idx_name};

/// A segment's identity: its first seq (`seg-<start_seq>`). Cheap, `Copy`, and
/// sorts in seq order, so listing a store yields segments oldest-first.
pub type SegmentId = u64;

/// Which file of a segment a `read_range`/`len`/`exists` call targets — a
/// segment is the `.data` frames plus the `.idx` locator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentPart {
    /// The `.data` file: concatenated framed records.
    Data,
    /// The `.idx` file: the fixed-stride seq→offset locator.
    Idx,
}

/// Errors from a [`SegmentStore`] operation.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("segment {0} not found")]
    NotFound(SegmentId),
    #[error("requested range is outside the segment object")]
    RangeOutOfBounds,
    #[error("segment store io error: {0}")]
    Io(#[from] io::Error),
}

/// A pluggable place to keep segment objects. One physical tier (a local folder
/// now; an object store later). A box's storage is a HOT store plus an optional
/// COLD store (see [`BoxTier`]).
///
/// All methods are synchronous so the relocator + cold reads run on a blocking
/// pool off the hot path (module docs). Implementations MUST be `Send + Sync` so
/// the engine can share one store across worker threads.
pub trait SegmentStore: Send + Sync {
    /// Durably write a segment's two parts. `data`/`idx` are the byte buffers a
    /// [`crate::storage::segment::SegmentBuilder`] produced. Overwrites any
    /// existing object with the same id (relocation re-puts the same id into a
    /// new tier). The write is fsync'd so a crash after `put` returns keeps the
    /// segment (the relocation crash-safety contract: copy → fsync → flip → drop).
    fn put(&self, id: SegmentId, data: &[u8], idx: &[u8]) -> Result<(), StoreError>;

    /// Read `len` bytes at `offset` from a segment part. Used to fetch one
    /// record's frame (`offset`/`len` from the `.idx`) or to bulk-read a whole
    /// `.idx` (offset 0, full length). A range past the object end is
    /// [`StoreError::RangeOutOfBounds`]; a missing segment is
    /// [`StoreError::NotFound`].
    fn read_range(
        &self,
        id: SegmentId,
        part: SegmentPart,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>, StoreError>;

    /// Read an entire segment part (convenience over [`Self::read_range`] for the
    /// `.idx` bulk read on recovery / for a small `.data`).
    fn read_all(&self, id: SegmentId, part: SegmentPart) -> Result<Vec<u8>, StoreError> {
        let n = self.len(id, part)?;
        self.read_range(id, part, 0, n)
    }

    /// Remove a segment (both parts). Idempotent: deleting an absent segment is
    /// `Ok(())` (segment-granular cap/TTL/delete reclaim re-issues drops; a crash
    /// between the two unlinks must be safe to repeat).
    fn delete(&self, id: SegmentId) -> Result<(), StoreError>;

    /// List the ids of all *complete* segments in this store, ascending (oldest
    /// first). "Complete" requires the `.data` part — a stray/torn `.idx` alone is
    /// not a resolvable segment, so it is **not** reported here (resolve/serve
    /// semantics).
    fn list(&self) -> Result<Vec<SegmentId>, StoreError>;

    /// List the ids of every segment with *any* part on disk (`.data` OR a stray
    /// `.idx`), ascending. Unlike [`Self::list`], this also reports an orphaned
    /// `.idx` with no `.data` — the remnant a crash between `delete()`'s two
    /// unlinks leaves behind. Used only by on-restart orphan reclaim so a half-
    /// unlinked segment is enumerated and dropped rather than leaked forever; it
    /// must never feed resolve/serve (a lone `.idx` is not a complete segment).
    /// Defaults to [`Self::list`] for stores whose objects are inseparable.
    fn list_all_ids(&self) -> Result<Vec<SegmentId>, StoreError> {
        self.list()
    }

    /// Whether a segment's part exists — a cheap probe used by relocation
    /// idempotency ("which copy survived a crash?").
    fn exists(&self, id: SegmentId, part: SegmentPart) -> bool;

    /// Byte length of a segment part. [`StoreError::NotFound`] if absent.
    fn len(&self, id: SegmentId, part: SegmentPart) -> Result<u64, StoreError>;
}

/// A [`SegmentStore`] backed by a local directory: each segment is the file pair
/// `seg-<start_seq>.data` + `seg-<start_seq>.idx` under `root`. This is both the
/// HOT store (a per-box dir under the data dir) and v1's COLD store (a per-box
/// dir under `STREAMS_COLD_DIR`).
pub struct LocalSegmentStore {
    root: PathBuf,
    /// The filesystem seam every byte of segment I/O routes through. Production
    /// wires [`RealFs`] (transparent); tests inject a fake/fault FS.
    fs: Arc<dyn Fs>,
}

impl LocalSegmentStore {
    /// Open (creating it if absent) a store rooted at `root` on the real
    /// filesystem. Equivalent to [`Self::open_with`] with [`RealFs`].
    pub fn open(root: impl Into<PathBuf>) -> io::Result<Self> {
        Self::open_with(root, RealFs::arc())
    }

    /// Open a store rooted at `root`, routing all I/O through `fs`. Production
    /// passes a [`RealFs`]; the crash/fault harness passes a fake.
    pub fn open_with(root: impl Into<PathBuf>, fs: Arc<dyn Fs>) -> io::Result<Self> {
        let root = root.into();
        fs.create_dir_all(&root)?;
        Ok(LocalSegmentStore { root, fs })
    }

    /// The store's root directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn part_path(&self, id: SegmentId, part: SegmentPart) -> PathBuf {
        let name = match part {
            SegmentPart::Data => data_name(id),
            SegmentPart::Idx => idx_name(id),
        };
        self.root.join(name)
    }

    /// Atomically write `bytes` to `path`: write a sibling `.tmp` (fully, looping
    /// over short writes), fsync it, rename over the final name. (The directory
    /// fsync that hardens the rename is done once in [`Self::put`] after both
    /// parts land.)
    fn write_atomic(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        let tmp = path.with_extension({
            let cur = path.extension().and_then(|s| s.to_str()).unwrap_or("");
            format!("{cur}.tmp")
        });
        {
            let mut f = self.fs.open(&tmp, OpenOpts::create_truncate())?;
            write_all_at(f.as_mut(), 0, bytes)?;
            f.sync_all()?;
        }
        self.fs.rename(&tmp, path)?;
        Ok(())
    }
}

/// Write the whole of `bytes` at `offset`, looping over short writes (a
/// `write_at` may report fewer bytes than offered, like `pwrite(2)`). A
/// zero-length write that makes no progress is an io error (avoids a spin).
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

impl SegmentStore for LocalSegmentStore {
    fn put(&self, id: SegmentId, data: &[u8], idx: &[u8]) -> Result<(), StoreError> {
        // Write .data first then .idx so a crash mid-put never leaves an .idx
        // pointing past a missing .data (the .idx is the index of record; a
        // segment is only "complete" once both exist — recovery checks both).
        self.write_atomic(&self.part_path(id, SegmentPart::Data), data)?;
        // Named crash point: the segment's `.data` is durably renamed into place
        // but the `.idx` has NOT been written yet. The F-SEG-CRASH-AFTER-DATA-
        // BEFORE-IDX oracle: `list()` requires `.data`, but a lone `.data` with no
        // `.idx` is an incomplete segment — the WAL/snapshot still holds the
        // records, so no live record is lost; orphan reclaim handles the stray
        // `.data`. No-op without `--features failpoints`.
        fail::fail_point!("segput::after_data_before_idx");
        self.write_atomic(&self.part_path(id, SegmentPart::Idx), idx)?;
        self.fs.sync_dir(&self.root)?;
        Ok(())
    }

    fn read_range(
        &self,
        id: SegmentId,
        part: SegmentPart,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>, StoreError> {
        let path = self.part_path(id, part);
        let f = match self.fs.open(&path, OpenOpts::read_only()) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                return Err(StoreError::NotFound(id))
            }
            Err(e) => return Err(e.into()),
        };
        let file_len = f.metadata_len()?;
        let end = offset
            .checked_add(len)
            .ok_or(StoreError::RangeOutOfBounds)?;
        if end > file_len {
            return Err(StoreError::RangeOutOfBounds);
        }
        let mut buf = vec![0u8; len as usize];
        read_exact_at(f.as_ref(), offset, &mut buf)?;
        Ok(buf)
    }

    fn delete(&self, id: SegmentId) -> Result<(), StoreError> {
        let mut removed_any = false;
        for part in [SegmentPart::Data, SegmentPart::Idx] {
            match self.fs.remove_file(&self.part_path(id, part)) {
                Ok(()) => removed_any = true,
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
            // Named crash point: between the two unlinks (`.data` removed, `.idx`
            // not yet). The F-RECLAIM-CRASH-BETWEEN-UNLINKS oracle: delete is
            // idempotent (NotFound tolerated), the next reclaim/orphan-sweep removes
            // the leftover `.idx`, and the segment never resurrects. No-op without
            // `--features failpoints`.
            fail::fail_point!("reclaim::between_unlinks");
        }
        if removed_any {
            self.fs.sync_dir(&self.root)?;
        }
        Ok(())
    }

    fn list(&self) -> Result<Vec<SegmentId>, StoreError> {
        let mut ids: BTreeSet<SegmentId> = BTreeSet::new();
        for path in self.fs.read_dir(&self.root)? {
            let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
                continue;
            };
            // A segment is counted once; require the `.data` part so a stray/torn
            // `.idx` alone is not reported as a complete segment.
            if let Some(rest) = name.strip_prefix("seg-").and_then(|s| s.strip_suffix(".data")) {
                if let Ok(id) = rest.parse::<SegmentId>() {
                    ids.insert(id);
                }
            }
        }
        Ok(ids.into_iter().collect())
    }

    fn list_all_ids(&self) -> Result<Vec<SegmentId>, StoreError> {
        // Enumerate by EITHER part so a stray `.idx` (no `.data`) — the remnant a
        // crash between `delete()`'s two unlinks leaves — is reported and can be
        // reclaimed (orphan sweep only; never resolve/serve).
        let mut ids: BTreeSet<SegmentId> = BTreeSet::new();
        for path in self.fs.read_dir(&self.root)? {
            let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
                continue;
            };
            let id = name
                .strip_prefix("seg-")
                .and_then(|s| s.strip_suffix(".data").or_else(|| s.strip_suffix(".idx")))
                .and_then(|rest| rest.parse::<SegmentId>().ok());
            if let Some(id) = id {
                ids.insert(id);
            }
        }
        Ok(ids.into_iter().collect())
    }

    fn exists(&self, id: SegmentId, part: SegmentPart) -> bool {
        self.fs.exists(&self.part_path(id, part))
    }

    fn len(&self, id: SegmentId, part: SegmentPart) -> Result<u64, StoreError> {
        match self.fs.metadata_len(&self.part_path(id, part)) {
            Ok(n) => Ok(n),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Err(StoreError::NotFound(id)),
            Err(e) => Err(e.into()),
        }
    }
}

/// Read exactly `buf.len()` bytes at `offset`, looping over short reads; an EOF
/// before the buffer is filled is an `UnexpectedEof` io error (matches
/// `read_exact`).
fn read_exact_at(f: &dyn super::fs::File, offset: u64, buf: &mut [u8]) -> io::Result<()> {
    let mut read = 0usize;
    while read < buf.len() {
        let n = f.read_at(offset + read as u64, &mut buf[read..])?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "failed to fill whole buffer",
            ));
        }
        read += n;
    }
    Ok(())
}

/// A box's two-tier segment storage: a required HOT store (fast NVMe, under the
/// data dir) and an optional COLD store (`STREAMS_COLD_DIR`; `None` ⇒ tiering is
/// disabled and everything stays hot — the default in every existing test, so
/// behavior is unchanged by construction).
///
/// [`Self::resolve`] reports which tier holds a segment, with HOT preferred when
/// a copy exists in both (the transient state during a relocation: copy to cold,
/// then drop hot — prefer the surviving copy, never lose a segment).
pub struct BoxTier {
    hot: Box<dyn SegmentStore>,
    cold: Option<Box<dyn SegmentStore>>,
}

/// Which tier a segment currently lives in (per [`BoxTier::resolve`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier {
    Hot,
    Cold,
}

impl BoxTier {
    /// Build a tier from a hot store and an optional cold store.
    pub fn new(hot: Box<dyn SegmentStore>, cold: Option<Box<dyn SegmentStore>>) -> Self {
        BoxTier { hot, cold }
    }

    /// The hot store (active + recent sealed segments live here).
    pub fn hot(&self) -> &dyn SegmentStore {
        self.hot.as_ref()
    }

    /// The cold store, if a cold tier is configured.
    pub fn cold(&self) -> Option<&dyn SegmentStore> {
        self.cold.as_deref()
    }

    /// Whether a cold tier is configured (relocation possible).
    pub fn has_cold(&self) -> bool {
        self.cold.is_some()
    }

    /// Report which tier currently holds `id`, preferring HOT when both have a
    /// copy (the mid-relocation window). `None` ⇒ the segment is in neither tier.
    /// A "complete" segment requires its `.data` part to exist in that tier.
    pub fn resolve(&self, id: SegmentId) -> Option<Tier> {
        if self.hot.exists(id, SegmentPart::Data) {
            return Some(Tier::Hot);
        }
        if let Some(cold) = &self.cold {
            if cold.exists(id, SegmentPart::Data) {
                return Some(Tier::Cold);
            }
        }
        None
    }

    /// The store that currently holds `id`, or `None` if it is in neither tier.
    pub fn store_for(&self, id: SegmentId) -> Option<&dyn SegmentStore> {
        match self.resolve(id)? {
            Tier::Hot => Some(self.hot()),
            Tier::Cold => self.cold(),
        }
    }
}

// ===========================================================================
// Unit tests
// ===========================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::segment::{SegmentBuilder, SegmentRecord};

    fn build_segment(start: u64, n: u64) -> (Vec<u8>, Vec<u8>) {
        let mut b = SegmentBuilder::new(start);
        for i in 0..n {
            b.push(&SegmentRecord {
                seq: start + i,
                ts: 1000 + i,
                node: None,
                tag: Some(format!("t{i}")),
                data: format!("{{\"v\":{i}}}").into_bytes(),
            });
        }
        b.finish()
    }

    #[test]
    fn put_read_range_delete_list_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalSegmentStore::open(dir.path()).unwrap();

        let (data1, idx1) = build_segment(1, 3);
        let (data2, idx2) = build_segment(10001, 5);
        store.put(1, &data1, &idx1).unwrap();
        store.put(10001, &data2, &idx2).unwrap();

        // list yields ids oldest-first.
        assert_eq!(store.list().unwrap(), vec![1, 10001]);

        // exists + len for both parts.
        assert!(store.exists(1, SegmentPart::Data));
        assert!(store.exists(1, SegmentPart::Idx));
        assert!(!store.exists(2, SegmentPart::Data));
        assert_eq!(store.len(1, SegmentPart::Data).unwrap(), data1.len() as u64);
        assert_eq!(store.len(1, SegmentPart::Idx).unwrap(), idx1.len() as u64);

        // read_all reproduces the bytes exactly.
        assert_eq!(store.read_all(1, SegmentPart::Data).unwrap(), data1);
        assert_eq!(store.read_all(10001, SegmentPart::Idx).unwrap(), idx2);

        // read_range fetches a sub-slice.
        let head = store.read_range(1, SegmentPart::Data, 0, 4).unwrap();
        assert_eq!(head, &data1[0..4]);

        // A range past the end is an error, not a silent short read.
        assert!(matches!(
            store.read_range(1, SegmentPart::Data, 0, data1.len() as u64 + 1),
            Err(StoreError::RangeOutOfBounds)
        ));

        // A missing segment is NotFound.
        assert!(matches!(
            store.read_range(999, SegmentPart::Data, 0, 1),
            Err(StoreError::NotFound(999))
        ));

        // delete removes both parts and is idempotent.
        store.delete(1).unwrap();
        assert!(!store.exists(1, SegmentPart::Data));
        assert!(!store.exists(1, SegmentPart::Idx));
        store.delete(1).unwrap(); // idempotent.
        assert_eq!(store.list().unwrap(), vec![10001]);
    }

    #[test]
    fn read_range_then_decode_one_record() {
        use crate::storage::segment::{decode_data_frame, lookup};
        let dir = tempfile::tempdir().unwrap();
        let store = LocalSegmentStore::open(dir.path()).unwrap();
        let (data, idx) = build_segment(1, 4);
        store.put(1, &data, &idx).unwrap();

        // Bulk-read the .idx, locate seq 3, then read exactly that record's frame.
        let idx_buf = store.read_all(1, SegmentPart::Idx).unwrap();
        let e = lookup(&idx_buf, 1, 3).expect("seq 3 present");
        let frame = store
            .read_range(1, SegmentPart::Data, e.offset as u64, e.len as u64)
            .unwrap();
        let got = decode_data_frame(&frame).expect("decodes");
        assert_eq!(got.seq, 3);
        assert_eq!(got.tag.as_deref(), Some("t2"));
    }

    #[test]
    fn box_tier_resolves_hot_then_cold_prefers_hot() {
        let hot_dir = tempfile::tempdir().unwrap();
        let cold_dir = tempfile::tempdir().unwrap();
        let hot = Box::new(LocalSegmentStore::open(hot_dir.path()).unwrap());
        let cold = Box::new(LocalSegmentStore::open(cold_dir.path()).unwrap());

        let (data, idx) = build_segment(1, 2);
        // Segment 1 lives only in cold; segment 2 lives in both (mid-relocation).
        cold.put(1, &data, &idx).unwrap();
        let (d2, i2) = build_segment(3, 2);
        hot.put(2, &d2, &i2).unwrap();
        cold.put(2, &d2, &i2).unwrap();

        let tier = BoxTier::new(hot, Some(cold));
        assert!(tier.has_cold());
        assert_eq!(tier.resolve(1), Some(Tier::Cold));
        assert_eq!(tier.resolve(2), Some(Tier::Hot)); // prefer the hot copy.
        assert_eq!(tier.resolve(999), None);
        // store_for routes to the resolved tier.
        assert!(tier.store_for(1).unwrap().exists(1, SegmentPart::Data));
    }

    #[test]
    fn box_tier_without_cold_keeps_everything_hot() {
        let hot_dir = tempfile::tempdir().unwrap();
        let hot = Box::new(LocalSegmentStore::open(hot_dir.path()).unwrap());
        let (data, idx) = build_segment(1, 1);
        hot.put(1, &data, &idx).unwrap();
        let tier = BoxTier::new(hot, None);
        assert!(!tier.has_cold());
        assert!(tier.cold().is_none());
        assert_eq!(tier.resolve(1), Some(Tier::Hot));
    }
}
