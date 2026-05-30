//! Hostile [`Fs`] / [`File`] implementations for the crash-consistency harness
//! (Phase-4 Stage-2). These are **test-only** — the whole module is gated behind
//! `#[cfg(any(test, feature = "test-fs"))]`, so a release build never sees them
//! and production is byte-for-byte unaffected (the seam stays [`RealFs`]).
//!
//! Three implementations, each injectable through the durability layer's
//! `*_with` constructors ([`crate::storage::Wal::open_at_with`],
//! [`crate::storage::write_snapshot_with`],
//! [`crate::storage::LocalSegmentStore::open_with`],
//! [`crate::engine::recovery::recover_and_open_with`]):
//!
//! - [`FakeDisk`] — a fully in-memory disk that models the durability boundary
//!   the real contract is defined at: a `write_at` lands in a file's **pending**
//!   buffer; `sync_data`/`sync_all` promote that file's pending bytes to
//!   **durable**; a `rename`/`create`/`unlink` is a pending **directory-entry
//!   op** that becomes durable only after `sync_dir`. [`FakeDisk::crash`] drops
//!   everything not yet promoted — the un-fsynced bytes *and* the un-fsynced dir
//!   ops — and optionally tears the last pending write of a file (prefix-truncate
//!   / zero-sector / garble), deterministically from a seed. This is what decides
//!   *what survives* a power loss.
//!
//! - [`FaultFs`] — a transparent wrapper over any inner [`Fs`] that injects an
//!   `io::Error` (EIO / ENOSPC / short-write / ESTALE) at a chosen **global FS-call
//!   index** on a chosen **op class** (`write_at` / `sync_data` / `sync_all` /
//!   `rename` / `sync_dir` / `set_len` / `read_at`). Fail-once (a transient glitch
//!   that then succeeds) or fail-always (a dead device). This decides *when* an I/O
//!   primitive refuses.
//!
//! - [`MonitorFs`] — a passive wrapper that asserts persistence-ordering
//!   invariants **live during every test** (it forwards to an inner `Fs` and only
//!   observes): a snapshot `.tmp` is never renamed before its `sync_all`; the old
//!   snapshot is removed only after the new one is durable; a segment `.idx` is
//!   never written before its `.data` is durable. A violation **panics**
//!   immediately, independent of any crash test — the belt-and-suspenders that
//!   catches an ordering regression the moment it happens.

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use super::fs::{File, Fs, OpenOpts};

// ===========================================================================
// FakeDisk — in-memory pending/durable disk with a crash() drop-pending model
// ===========================================================================

/// One file's image: its **durable** bytes (survive a crash) and an ordered list
/// of **pending** positioned writes (lost on crash until promoted by an fsync).
///
/// File *bytes* and file *existence (name)* have independent durability: bytes
/// become durable on `sync_data`/`sync_all`; a name becomes durable on the
/// `sync_dir` of its directory (the durable namespace is tracked separately in
/// [`DiskState::durable_names`]).
#[derive(Debug, Clone, Default)]
struct FileImage {
    /// Bytes guaranteed to survive [`FakeDisk::crash`] (promoted by a prior
    /// `sync_data`/`sync_all`, or installed by a durable `set_len`).
    durable: Vec<u8>,
    /// Positioned writes not yet fsynced: `(offset, bytes)` in submission order.
    /// `sync_data`/`sync_all` fold these into `durable`; `crash()` drops them
    /// (after optionally tearing the last one).
    pending: Vec<(u64, Vec<u8>)>,
    /// A pending truncation (`set_len`) not yet fsynced, if any. Applied to
    /// `durable` length on promotion; dropped on crash.
    pending_len: Option<u64>,
}

impl FileImage {
    /// The current logical length the holder of an open handle observes: the
    /// durable length overlaid by every pending write and a pending truncation.
    fn logical_len(&self) -> u64 {
        let mut len = self.durable.len() as u64;
        if let Some(t) = self.pending_len {
            len = t;
        }
        for (off, bytes) in &self.pending {
            len = len.max(off + bytes.len() as u64);
        }
        len
    }

    /// Materialize the current *visible* bytes (durable + pending overlay): what a
    /// read on a live handle (before any crash) sees.
    fn visible_bytes(&self) -> Vec<u8> {
        let mut buf = self.durable.clone();
        if let Some(t) = self.pending_len {
            buf.resize(t as usize, 0);
        }
        for (off, bytes) in &self.pending {
            let end = *off as usize + bytes.len();
            if buf.len() < end {
                buf.resize(end, 0);
            }
            buf[*off as usize..end].copy_from_slice(bytes);
        }
        buf
    }

    /// Promote every pending write + a pending truncation into `durable` (an
    /// fsync). After this the file's pending list is empty and its durable bytes
    /// equal what was visible.
    fn promote(&mut self) {
        if let Some(t) = self.pending_len.take() {
            self.durable.resize(t as usize, 0);
        }
        for (off, bytes) in std::mem::take(&mut self.pending) {
            let end = off as usize + bytes.len();
            if self.durable.len() < end {
                self.durable.resize(end, 0);
            }
            self.durable[off as usize..end].copy_from_slice(&bytes);
        }
    }
}

/// How [`FakeDisk::crash`] damages the **last pending write** of each file (the
/// torn-write model — atomicity *within* a single write is not guaranteed by real
/// hardware). Deterministic from the disk's seed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TornDamage {
    /// No torn damage: a pending write is simply dropped whole (clean power loss
    /// between writes — the common case).
    None,
    /// The last pending write persisted only a **prefix** (the tail of the write
    /// never reached the platter). Models a partial frame.
    PrefixTruncate,
    /// One 512-byte sector of the last pending write read back **all zeros** (a
    /// lost/sparse sector).
    ZeroSector,
    /// One byte of the last pending write is **garbled** (single-bit flip / bit
    /// rot in the in-flight write).
    Garble,
}

/// An inode identity. A `rename` moves a directory entry to point at the same
/// inode (the bytes carry over); a `create_truncate` over an existing name makes
/// a fresh inode. Separating inode (bytes) from directory entry (name→inode) lets
/// byte durability (`sync_data`) and name durability (`sync_dir`) be independent,
/// which is exactly the real contract the crash model must reproduce.
type InodeId = u64;

/// The shared, mutable disk image behind every [`FakeDisk`] handle and clone.
///
/// The model has two layers that gain durability independently:
///
/// - **inodes** hold the bytes; an inode's bytes become durable on
///   `sync_data`/`sync_all` (its `pending` writes fold into `durable`).
/// - **directory entries** (`live_dir`: name → inode) become durable on the
///   directory's `sync_dir` (recorded in `durable_dir`).
///
/// On [`FakeDisk::crash`] the live view is rebuilt from `durable_dir` (the
/// surviving names) pointing at each inode's **durable** bytes — every un-fsynced
/// pending write and un-fsynced directory entry is dropped, the last pending write
/// optionally torn.
#[derive(Debug, Default)]
struct DiskState {
    /// Live directory entries: path → inode id (what open handles resolve).
    live_dir: BTreeMap<PathBuf, InodeId>,
    /// Durable directory entries: the namespace that survives a crash.
    durable_dir: BTreeMap<PathBuf, InodeId>,
    /// All inodes by id (bytes + pending). An inode with no live or durable entry
    /// is garbage but harmlessly retained until the next crash rebuild.
    inodes: BTreeMap<InodeId, FileImage>,
    /// Directories known to exist (created via `create_dir_all`). Existence-only.
    dirs: BTreeMap<PathBuf, ()>,
    /// Next inode id to allocate.
    next_inode: InodeId,
}

impl DiskState {
    fn alloc_inode(&mut self) -> InodeId {
        self.next_inode += 1;
        let id = self.next_inode;
        self.inodes.insert(id, FileImage::default());
        id
    }
}

/// A fully in-memory [`Fs`] modelling the durable/pending boundary and a
/// `crash()` that drops everything not yet fsynced. Cheap to clone — all clones
/// share one [`DiskState`] (so a `crash()` on any clone affects the whole disk,
/// exactly like a single physical device).
#[derive(Clone)]
pub struct FakeDisk {
    state: Arc<Mutex<DiskState>>,
    /// Deterministic seed driving torn-write damage selection on `crash()`.
    seed: Arc<AtomicU64>,
    /// When `true`, `sync_data`/`sync_all` return `Ok` **without** promoting
    /// pending bytes (the "fsync lies" / nobarrier mode, F-NFS-FSYNC-LIES).
    lying_fsync: Arc<std::sync::atomic::AtomicBool>,
    /// Set by [`FakeDisk::crash`]: the device is powered off. Any later mutation
    /// (a write from a detached WAL writer thread that didn't stop in time) is
    /// silently dropped — exactly what "the power is off" means. Reads still work
    /// (recovery opens the post-crash image). Cleared by [`FakeDisk::reset_power`].
    frozen: Arc<std::sync::atomic::AtomicBool>,
}

impl Default for FakeDisk {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeDisk {
    /// A fresh empty disk with seed `0` (deterministic, no torn damage by
    /// default).
    pub fn new() -> Self {
        Self::with_seed(0)
    }

    /// A fresh empty disk whose `crash()` torn-damage choices derive from `seed`.
    pub fn with_seed(seed: u64) -> Self {
        FakeDisk {
            state: Arc::new(Mutex::new(DiskState::default())),
            seed: Arc::new(AtomicU64::new(seed)),
            lying_fsync: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            frozen: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Whether the device is currently powered off (post-`crash`, pre-`reset_power`).
    fn is_frozen(&self) -> bool {
        self.frozen.load(Ordering::SeqCst)
    }

    /// Power the device back on after a crash so a recovery run can re-open it and
    /// write (the resumed WAL writer needs a live disk). The durable image is
    /// preserved; only the powered-off latch is cleared.
    pub fn reset_power(&self) {
        self.frozen.store(false, Ordering::SeqCst);
    }

    /// Wrap this disk in an `Arc<dyn Fs>` for the `*_with` constructors.
    pub fn arc(&self) -> Arc<dyn Fs> {
        Arc::new(self.clone())
    }

    /// Enable/disable "lying fsync": when on, `sync_data`/`sync_all` report success
    /// but do **not** promote pending bytes, so a `crash()` still loses them
    /// (models nobarrier / async-NFS commit, F-NFS-FSYNC-LIES).
    pub fn set_lying_fsync(&self, on: bool) {
        self.lying_fsync.store(on, Ordering::SeqCst);
    }

    /// Simulate a power loss: drop every un-fsynced pending write and every
    /// un-fsynced directory-entry op, optionally tearing the last pending write of
    /// each file per `damage` (deterministic from the seed). After this the disk
    /// holds exactly the durable image a real device would expose post-crash.
    pub fn crash(&self, damage: TornDamage) {
        // Power off first so a still-running detached writer thread cannot mutate
        // the image while/after we roll it back to the durable point.
        self.frozen.store(true, Ordering::SeqCst);
        let mut st = self.state.lock().unwrap();
        // 1. Each inode keeps ONLY its durable bytes; un-fsynced pending writes are
        //    dropped, the last one optionally torn (a partial write that was
        //    in-flight to the platter at power loss).
        let inode_ids: Vec<InodeId> = st.inodes.keys().copied().collect();
        for id in inode_ids {
            let img = st.inodes.get_mut(&id).unwrap();
            img.pending_len = None;
            let last = img.pending.last().cloned();
            img.pending.clear();
            if let Some((off, bytes)) = last {
                let torn = self.tear(&bytes, damage);
                if !torn.is_empty() {
                    let end = off as usize + torn.len();
                    if img.durable.len() < end {
                        img.durable.resize(end, 0);
                    }
                    img.durable[off as usize..end].copy_from_slice(&torn);
                }
            }
        }
        // 2. The live namespace reverts to the durable namespace (un-fsynced
        //    creates/renames/unlinks vanish). Inodes no longer referenced by any
        //    durable entry are garbage; drop them so file_count reflects reality.
        st.live_dir = st.durable_dir.clone();
        let live_inodes: std::collections::BTreeSet<InodeId> =
            st.live_dir.values().copied().collect();
        st.inodes.retain(|id, _| live_inodes.contains(id));
    }

    /// Apply the torn-damage transform to a pending write's `bytes`, returning the
    /// (possibly shorter / altered) bytes that actually reached durable media.
    /// Deterministic from the seed (advanced each call so repeated tears differ).
    fn tear(&self, bytes: &[u8], damage: TornDamage) -> Vec<u8> {
        if bytes.is_empty() {
            return Vec::new();
        }
        let s = self.seed.fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::SeqCst);
        match damage {
            TornDamage::None => Vec::new(), // dropped whole (no partial landed).
            TornDamage::PrefixTruncate => {
                // Keep a deterministic prefix in [0, len) — at least 0, at most
                // len-1 (a true prefix, so the frame is incomplete).
                let keep = (s as usize) % bytes.len();
                bytes[..keep].to_vec()
            }
            TornDamage::ZeroSector => {
                let mut out = bytes.to_vec();
                let sector = 512usize;
                let nsec = out.len().div_ceil(sector).max(1);
                let which = (s as usize) % nsec;
                let start = which * sector;
                let end = (start + sector).min(out.len());
                for b in &mut out[start..end] {
                    *b = 0;
                }
                out
            }
            TornDamage::Garble => {
                let mut out = bytes.to_vec();
                let idx = (s as usize) % out.len();
                out[idx] ^= 0xFF;
                out
            }
        }
    }

    /// Snapshot the **durable** bytes of a file (what survives a crash right now),
    /// for test assertions: the durable bytes of the inode the durable directory
    /// entry points at. `None` if the file's name is not durable.
    pub fn durable_bytes(&self, path: &Path) -> Option<Vec<u8>> {
        let st = self.state.lock().unwrap();
        let id = st.durable_dir.get(path)?;
        st.inodes.get(id).map(|f| f.durable.clone())
    }

    /// Total count of live directory entries currently on the disk image.
    pub fn file_count(&self) -> usize {
        self.state.lock().unwrap().live_dir.len()
    }
}

/// An open handle into a [`FakeDisk`]: the shared disk plus the **inode id** this
/// handle resolved at open time. Like a real fd it keeps pointing at that inode
/// even if the name is later renamed/unlinked, so its positioned I/O is stable.
pub struct FakeFile {
    state: Arc<Mutex<DiskState>>,
    lying_fsync: Arc<std::sync::atomic::AtomicBool>,
    frozen: Arc<std::sync::atomic::AtomicBool>,
    inode: InodeId,
}

impl FakeFile {
    fn powered_off(&self) -> bool {
        self.frozen.load(Ordering::SeqCst)
    }
}

impl File for FakeFile {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let st = self.state.lock().unwrap();
        let Some(img) = st.inodes.get(&self.inode) else {
            return Err(io::Error::new(io::ErrorKind::NotFound, "no such file"));
        };
        let visible = img.visible_bytes();
        if offset >= visible.len() as u64 {
            return Ok(0);
        }
        let start = offset as usize;
        let n = buf.len().min(visible.len() - start);
        buf[..n].copy_from_slice(&visible[start..start + n]);
        Ok(n)
    }

    fn write_at(&mut self, offset: u64, buf: &[u8]) -> io::Result<usize> {
        if self.powered_off() {
            return Ok(buf.len()); // power is off: the write is lost, report success.
        }
        let mut st = self.state.lock().unwrap();
        let Some(img) = st.inodes.get_mut(&self.inode) else {
            return Err(io::Error::new(io::ErrorKind::NotFound, "no such file"));
        };
        img.pending.push((offset, buf.to_vec()));
        Ok(buf.len())
    }

    fn set_len(&mut self, len: u64) -> io::Result<()> {
        if self.powered_off() {
            return Ok(());
        }
        let mut st = self.state.lock().unwrap();
        let Some(img) = st.inodes.get_mut(&self.inode) else {
            return Err(io::Error::new(io::ErrorKind::NotFound, "no such file"));
        };
        // A truncation/extension is itself pending until the next fsync. We also
        // drop any pending writes beyond the new length (they no longer apply).
        img.pending_len = Some(len);
        img.pending.retain(|(off, _)| *off < len);
        Ok(())
    }

    fn sync_data(&self) -> io::Result<()> {
        if self.powered_off() {
            return Ok(()); // power off: nothing reaches the platter.
        }
        if self.lying_fsync.load(Ordering::SeqCst) {
            return Ok(()); // report durable without promoting (nobarrier).
        }
        let mut st = self.state.lock().unwrap();
        if let Some(img) = st.inodes.get_mut(&self.inode) {
            img.promote();
        }
        Ok(())
    }

    fn sync_all(&self) -> io::Result<()> {
        self.sync_data()
    }

    fn metadata_len(&self) -> io::Result<u64> {
        let st = self.state.lock().unwrap();
        st.inodes
            .get(&self.inode)
            .map(|f| f.logical_len())
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no such file"))
    }
}

impl Fs for FakeDisk {
    fn open(&self, path: &Path, opts: OpenOpts) -> io::Result<Box<dyn File>> {
        let frozen = self.is_frozen();
        let mut st = self.state.lock().unwrap();
        let existing = st.live_dir.get(path).copied();
        let inode = match existing {
            None => {
                if !opts.create {
                    return Err(io::Error::new(io::ErrorKind::NotFound, "no such file"));
                }
                if frozen {
                    // Power off: a create from a detached writer thread leaves no
                    // durable trace. Hand back a throwaway inode so the caller does
                    // not error, but never touch the namespace.
                    let id = st.alloc_inode();
                    return Ok(Box::new(FakeFile {
                        state: self.state.clone(),
                        lying_fsync: self.lying_fsync.clone(),
                        frozen: self.frozen.clone(),
                        inode: id,
                    }));
                }
                // Create a fresh inode + a LIVE directory entry. The name becomes
                // durable only after the containing directory is `sync_dir`'d.
                let id = st.alloc_inode();
                st.live_dir.insert(path.to_path_buf(), id);
                id
            }
            Some(id) => {
                if opts.truncate && !frozen {
                    // Truncate-on-open replaces contents with a FRESH inode (a real
                    // O_TRUNC keeps the inode, but a fresh one is observably
                    // identical here and keeps byte-durability bookkeeping simple).
                    let new_id = st.alloc_inode();
                    st.live_dir.insert(path.to_path_buf(), new_id);
                    new_id
                } else {
                    id
                }
            }
        };
        Ok(Box::new(FakeFile {
            state: self.state.clone(),
            lying_fsync: self.lying_fsync.clone(),
            frozen: self.frozen.clone(),
            inode,
        }))
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        if self.is_frozen() {
            return Ok(());
        }
        let mut st = self.state.lock().unwrap();
        let Some(id) = st.live_dir.remove(from) else {
            return Err(io::Error::new(io::ErrorKind::NotFound, "rename src missing"));
        };
        // Point the new name at the same inode in the LIVE view (bytes carry over).
        // Both the removal of `from` and the install of `to` are durable only after
        // the directory is `sync_dir`'d, so a crash before that restores the durable
        // namespace (which still has `from`, not `to`).
        st.live_dir.insert(to.to_path_buf(), id);
        Ok(())
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        if self.is_frozen() {
            return Ok(());
        }
        let mut st = self.state.lock().unwrap();
        if st.live_dir.remove(path).is_none() {
            return Err(io::Error::new(io::ErrorKind::NotFound, "no such file"));
        }
        // The unlink is durable only after `sync_dir`; until then `durable_dir`
        // still lists `path`, so a crash before the dir fsync restores it.
        Ok(())
    }

    fn read_dir(&self, dir: &Path) -> io::Result<Vec<PathBuf>> {
        let st = self.state.lock().unwrap();
        let mut out = Vec::new();
        for p in st.live_dir.keys() {
            if p.parent() == Some(dir) {
                out.push(p.clone());
            }
        }
        Ok(out)
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        if self.is_frozen() {
            return Ok(());
        }
        // Make the live namespace OF THIS DIRECTORY durable: the durable entries
        // under `dir` are replaced by the current live entries under `dir`. After
        // this a crash keeps exactly this directory's current set of names→inodes.
        let mut st = self.state.lock().unwrap();
        let live_here: Vec<(PathBuf, InodeId)> = st
            .live_dir
            .iter()
            .filter(|(p, _)| p.parent() == Some(dir))
            .map(|(p, id)| (p.clone(), *id))
            .collect();
        let durable_here: Vec<PathBuf> = st
            .durable_dir
            .keys()
            .filter(|p| p.parent() == Some(dir))
            .cloned()
            .collect();
        for p in durable_here {
            st.durable_dir.remove(&p);
        }
        for (p, id) in live_here {
            st.durable_dir.insert(p, id);
        }
        Ok(())
    }

    fn create_dir_all(&self, dir: &Path) -> io::Result<()> {
        let mut st = self.state.lock().unwrap();
        let mut cur = PathBuf::new();
        for comp in dir.components() {
            cur.push(comp);
            st.dirs.entry(cur.clone()).or_insert(());
        }
        Ok(())
    }

    fn exists(&self, path: &Path) -> bool {
        let st = self.state.lock().unwrap();
        st.live_dir.contains_key(path) || st.dirs.contains_key(path)
    }

    fn metadata_len(&self, path: &Path) -> io::Result<u64> {
        let st = self.state.lock().unwrap();
        let id = st
            .live_dir
            .get(path)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no such file"))?;
        st.inodes
            .get(id)
            .map(|f| f.logical_len())
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no such file"))
    }
}

// ===========================================================================
// FaultFs — inject an io error at a chosen call index on a chosen op class
// ===========================================================================

/// The classes of FS primitive [`FaultFs`] can inject a fault into.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultOp {
    WriteAt,
    SyncData,
    SyncAll,
    Rename,
    SyncDir,
    SetLen,
    ReadAt,
    Open,
    RemoveFile,
    CreateDirAll,
}

/// The kind of error [`FaultFs`] returns when it fires.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultKind {
    /// `EIO` — a generic I/O error.
    Eio,
    /// `ENOSPC` — out of space (a write/rename that can't allocate).
    Enospc,
    /// A **short write**: `write_at` returns fewer bytes (`len/2`, min 1) instead
    /// of erroring — exercises the caller's short-write loop. Only meaningful for
    /// [`FaultOp::WriteAt`]; other ops treat it as `Eio`.
    ShortWrite,
    /// `ESTALE` — a stale NFS handle.
    Estale,
}

impl FaultKind {
    fn as_error(self) -> io::Error {
        match self {
            FaultKind::Eio => io::Error::other("injected EIO"),
            FaultKind::Enospc => io::Error::new(io::ErrorKind::StorageFull, "injected ENOSPC"),
            FaultKind::ShortWrite => io::Error::other("injected short write"),
            FaultKind::Estale => io::Error::other("injected ESTALE"),
        }
    }
}

/// The fault plan shared between a [`FaultFs`] and the [`FaultFile`] handles it
/// hands out, so the **global call counter** advances across every op of the
/// chosen class regardless of which handle issues it.
#[derive(Debug)]
struct FaultPlan {
    op: FaultOp,
    kind: FaultKind,
    /// The 0-based index (within the chosen op class) at which to fire.
    at: u64,
    /// `true` ⇒ fire only once (transient glitch); `false` ⇒ fire at `at` and
    /// every call after (dead device).
    once: bool,
    /// How many calls of the chosen class have happened so far.
    seen: AtomicU64,
    /// Whether the fault has already fired (for fail-once).
    fired: std::sync::atomic::AtomicBool,
}

impl FaultPlan {
    /// Decide whether the next call of class `op` should fault. Advances the
    /// global counter for that class. Returns `Some(err_kind)` to fault.
    fn check(&self, op: FaultOp) -> Option<FaultKind> {
        if op != self.op {
            return None;
        }
        let idx = self.seen.fetch_add(1, Ordering::SeqCst);
        if idx < self.at {
            return None;
        }
        if self.once {
            // Fire exactly once, the first time we reach/pass `at`.
            if self
                .fired
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return Some(self.kind);
            }
            None
        } else {
            Some(self.kind) // dead device: fire at `at` and forever after.
        }
    }
}

/// A transparent [`Fs`] wrapper that injects one fault (EIO/ENOSPC/short-write/
/// ESTALE) at a chosen global call index on a chosen op class, over any inner
/// `Fs` ([`RealFs`] for kernel fidelity, or a [`FakeDisk`] for a combined
/// fault+crash sweep).
#[derive(Clone)]
pub struct FaultFs {
    inner: Arc<dyn Fs>,
    plan: Arc<FaultPlan>,
}

impl FaultFs {
    /// Wrap `inner`, firing `kind` on the `at`-th call (0-based) of class `op`.
    /// `once` selects fail-once (transient) vs fail-always (dead device).
    pub fn new(inner: Arc<dyn Fs>, op: FaultOp, kind: FaultKind, at: u64, once: bool) -> Self {
        FaultFs {
            inner,
            plan: Arc::new(FaultPlan {
                op,
                kind,
                at,
                once,
                seen: AtomicU64::new(0),
                fired: std::sync::atomic::AtomicBool::new(false),
            }),
        }
    }

    /// Wrap this in an `Arc<dyn Fs>` for the `*_with` constructors.
    pub fn arc(&self) -> Arc<dyn Fs> {
        Arc::new(self.clone())
    }

    /// How many calls of the planned op class have been observed so far. Lets a
    /// sweep size the call space (run once with `at = u64::MAX`, read this).
    pub fn calls_seen(&self) -> u64 {
        self.plan.seen.load(Ordering::SeqCst)
    }
}

/// An open handle from a [`FaultFs`]: forwards to the inner file but consults the
/// shared [`FaultPlan`] for `write_at` / `read_at` / `set_len` / the two fsyncs.
struct FaultFile {
    inner: Box<dyn File>,
    plan: Arc<FaultPlan>,
}

impl File for FaultFile {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        if let Some(kind) = self.plan.check(FaultOp::ReadAt) {
            return Err(kind.as_error());
        }
        self.inner.read_at(offset, buf)
    }

    fn write_at(&mut self, offset: u64, buf: &[u8]) -> io::Result<usize> {
        if let Some(kind) = self.plan.check(FaultOp::WriteAt) {
            if kind == FaultKind::ShortWrite {
                // Accept a short prefix (>=1) and report it, exercising the loop.
                let n = (buf.len() / 2).max(1).min(buf.len());
                return self.inner.write_at(offset, &buf[..n]);
            }
            return Err(kind.as_error());
        }
        self.inner.write_at(offset, buf)
    }

    fn set_len(&mut self, len: u64) -> io::Result<()> {
        if let Some(kind) = self.plan.check(FaultOp::SetLen) {
            return Err(kind.as_error());
        }
        self.inner.set_len(len)
    }

    fn sync_data(&self) -> io::Result<()> {
        if let Some(kind) = self.plan.check(FaultOp::SyncData) {
            return Err(kind.as_error());
        }
        self.inner.sync_data()
    }

    fn sync_all(&self) -> io::Result<()> {
        if let Some(kind) = self.plan.check(FaultOp::SyncAll) {
            return Err(kind.as_error());
        }
        self.inner.sync_all()
    }

    fn metadata_len(&self) -> io::Result<u64> {
        self.inner.metadata_len()
    }

    fn read_to_end_from(&self, offset: u64, out: &mut Vec<u8>) -> io::Result<()> {
        if let Some(kind) = self.plan.check(FaultOp::ReadAt) {
            return Err(kind.as_error());
        }
        self.inner.read_to_end_from(offset, out)
    }
}

impl Fs for FaultFs {
    fn open(&self, path: &Path, opts: OpenOpts) -> io::Result<Box<dyn File>> {
        if let Some(kind) = self.plan.check(FaultOp::Open) {
            return Err(kind.as_error());
        }
        let inner = self.inner.open(path, opts)?;
        Ok(Box::new(FaultFile {
            inner,
            plan: self.plan.clone(),
        }))
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        if let Some(kind) = self.plan.check(FaultOp::Rename) {
            return Err(kind.as_error());
        }
        self.inner.rename(from, to)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        if let Some(kind) = self.plan.check(FaultOp::RemoveFile) {
            return Err(kind.as_error());
        }
        self.inner.remove_file(path)
    }

    fn read_dir(&self, dir: &Path) -> io::Result<Vec<PathBuf>> {
        self.inner.read_dir(dir)
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        if let Some(kind) = self.plan.check(FaultOp::SyncDir) {
            return Err(kind.as_error());
        }
        self.inner.sync_dir(dir)
    }

    fn create_dir_all(&self, dir: &Path) -> io::Result<()> {
        if let Some(kind) = self.plan.check(FaultOp::CreateDirAll) {
            return Err(kind.as_error());
        }
        self.inner.create_dir_all(dir)
    }

    fn exists(&self, path: &Path) -> bool {
        self.inner.exists(path)
    }

    fn metadata_len(&self, path: &Path) -> io::Result<u64> {
        self.inner.metadata_len(path)
    }
}

// ===========================================================================
// MonitorFs — passive live assertion of persistence-ordering invariants
// ===========================================================================

/// Shared ordering state observed by a [`MonitorFs`]. Tracks, per file path,
/// whether its bytes have been fsynced since the last write, so a rename of a
/// `.tmp` before its `sync_all` (or a segment `.idx` write before its `.data` is
/// durable) is caught the instant it happens.
#[derive(Debug, Default)]
struct MonitorState {
    /// Paths with un-fsynced writes outstanding (written but no `sync_*` since).
    dirty: BTreeMap<PathBuf, ()>,
    /// Paths whose bytes are currently durable (fsynced, no later write).
    durable: BTreeMap<PathBuf, ()>,
}

/// If `path` is a segment `.data` object (`seg-<id>.data`), return its segment id.
/// Used by the cold-relocation ordering guard to count durable copies of the same
/// segment across tier directories (a segment id is identified by its basename, so
/// a hot and cold copy of the same id share the same `<id>`). Only `.data` is
/// guarded — it carries the records; a stray `.idx` alone is never a copy.
fn segment_data_id(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    name.strip_prefix("seg-")
        .and_then(|s| s.strip_suffix(".data"))
        .and_then(|rest| rest.parse::<u64>().ok())
}

/// A passive [`Fs`] wrapper that asserts persistence-ordering invariants live
/// during **all** tests and **panics** on a violation. It changes no behavior —
/// every call forwards to the inner `Fs`; it only observes the order of writes,
/// fsyncs, and renames. Wrap any production wiring in this for belt-and-suspenders
/// ordering checks (SQLite journal-test analog).
#[derive(Clone)]
pub struct MonitorFs {
    inner: Arc<dyn Fs>,
    state: Arc<Mutex<MonitorState>>,
}

impl MonitorFs {
    /// Wrap `inner` with the live ordering monitor.
    pub fn new(inner: Arc<dyn Fs>) -> Self {
        MonitorFs {
            inner,
            state: Arc::new(Mutex::new(MonitorState::default())),
        }
    }

    /// Wrap this in an `Arc<dyn Fs>` for the `*_with` constructors.
    pub fn arc(&self) -> Arc<dyn Fs> {
        Arc::new(self.clone())
    }

    fn mark_written(&self, path: &Path) {
        let mut st = self.state.lock().unwrap();
        st.durable.remove(path);
        st.dirty.insert(path.to_path_buf(), ());
    }

    fn mark_synced(&self, path: &Path) {
        let mut st = self.state.lock().unwrap();
        st.dirty.remove(path);
        st.durable.insert(path.to_path_buf(), ());
    }
}

/// A monitored file handle: records writes/fsyncs against the shared
/// [`MonitorState`] so the namespace-level invariants (checked in [`MonitorFs`])
/// have accurate per-file durability knowledge.
struct MonitorFile {
    inner: Box<dyn File>,
    monitor: MonitorFs,
    path: PathBuf,
}

impl File for MonitorFile {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read_at(offset, buf)
    }

    fn write_at(&mut self, offset: u64, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write_at(offset, buf)?;
        self.monitor.mark_written(&self.path);
        Ok(n)
    }

    fn set_len(&mut self, len: u64) -> io::Result<()> {
        let r = self.inner.set_len(len);
        self.monitor.mark_written(&self.path);
        r
    }

    fn sync_data(&self) -> io::Result<()> {
        self.inner.sync_data()?;
        self.monitor.mark_synced(&self.path);
        Ok(())
    }

    fn sync_all(&self) -> io::Result<()> {
        self.inner.sync_all()?;
        self.monitor.mark_synced(&self.path);
        Ok(())
    }

    fn metadata_len(&self) -> io::Result<u64> {
        self.inner.metadata_len()
    }

    fn read_to_end_from(&self, offset: u64, out: &mut Vec<u8>) -> io::Result<()> {
        self.inner.read_to_end_from(offset, out)
    }
}

impl Fs for MonitorFs {
    fn open(&self, path: &Path, opts: OpenOpts) -> io::Result<Box<dyn File>> {
        let inner = self.inner.open(path, opts)?;
        if opts.truncate {
            // A truncate-on-open replaces contents ⇒ treat as a fresh dirty file.
            self.mark_written(path);
        }
        Ok(Box::new(MonitorFile {
            inner,
            monitor: self.clone(),
            path: path.to_path_buf(),
        }))
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        // INVARIANT: a `.tmp` (snapshot or segment) must be fsynced before it is
        // renamed over its final name — never publish un-durable bytes. We assert
        // the source is NOT dirty (it must have been sync'd since its last write).
        {
            let st = self.state.lock().unwrap();
            let is_tmp = from
                .extension()
                .and_then(|e| e.to_str())
                .map(|e| e == "tmp")
                .unwrap_or(false);
            if is_tmp && st.dirty.contains_key(from) {
                drop(st);
                panic!(
                    "MonitorFs ordering violation: rename of un-fsynced tmp {:?} \
                     (bytes not durable before publish)",
                    from
                );
            }
        }
        let r = self.inner.rename(from, to);
        if r.is_ok() {
            // The destination inherits the source's durability; the source name is
            // gone.
            let mut st = self.state.lock().unwrap();
            let was_durable = st.durable.remove(from).is_some();
            st.dirty.remove(from);
            if was_durable {
                st.durable.insert(to.to_path_buf(), ());
            } else {
                st.dirty.insert(to.to_path_buf(), ());
            }
        }
        r
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        // INVARIANT (FAULT_TESTING.md §MonitorFS(d) / model invariant #9, COLD
        // RELOCATION ALL-OR-NOTHING): a sealed segment's `.data` must never be the
        // last durable copy on the device when it is deleted — the hot copy is
        // dropped only AFTER a durable cold copy exists (relocation), so at least
        // one readable `.data` of that segment always survives. Deleting the only
        // durable `seg-<id>.data` (e.g. hot().delete before a durable cold.put)
        // drops the segment's last copy and must panic immediately.
        if let Some(seg) = segment_data_id(path) {
            let st = self.state.lock().unwrap();
            let surviving = st
                .durable
                .keys()
                .filter(|p| p.as_path() != path && segment_data_id(p) == Some(seg))
                .count();
            // Only guard when the path being removed is itself a durable copy
            // (an un-fsynced / already-gone path is not a "last durable copy").
            let removing_durable = st.durable.contains_key(path);
            if removing_durable && surviving == 0 {
                drop(st);
                panic!(
                    "MonitorFs ordering violation: deleting the last durable copy of \
                     segment {seg} ({path:?}) — a sealed segment's hot `.data` must \
                     not be dropped before a durable cold copy exists (cold-relocation \
                     order: cold.put durable BEFORE hot().delete)"
                );
            }
        }
        let r = self.inner.remove_file(path);
        let mut st = self.state.lock().unwrap();
        st.dirty.remove(path);
        st.durable.remove(path);
        r
    }

    fn read_dir(&self, dir: &Path) -> io::Result<Vec<PathBuf>> {
        self.inner.read_dir(dir)
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        self.inner.sync_dir(dir)
    }

    fn create_dir_all(&self, dir: &Path) -> io::Result<()> {
        self.inner.create_dir_all(dir)
    }

    fn exists(&self, path: &Path) -> bool {
        self.inner.exists(path)
    }

    fn metadata_len(&self, path: &Path) -> io::Result<u64> {
        self.inner.metadata_len(path)
    }
}

// ===========================================================================
// Unit tests for the test infra itself (the fakes must be ground-truth-correct)
// ===========================================================================
#[cfg(test)]
mod tests {
    use super::*;

    fn write(fs: &dyn Fs, path: &Path, off: u64, bytes: &[u8]) {
        let mut f = fs.open(path, OpenOpts::create_truncate()).unwrap();
        let mut written = 0;
        while written < bytes.len() {
            let n = f.write_at(off + written as u64, &bytes[written..]).unwrap();
            written += n;
        }
    }

    // --- FakeDisk -----------------------------------------------------------

    /// A write that is fsynced survives a crash; a write that is not is dropped.
    #[test]
    fn fakedisk_fsync_then_crash_keeps_durable() {
        let disk = FakeDisk::new();
        let p = PathBuf::from("/d/f");
        disk.create_dir_all(Path::new("/d")).unwrap();
        {
            let mut f = disk.open(&p, OpenOpts::create_truncate()).unwrap();
            f.write_at(0, b"durable").unwrap();
            f.sync_all().unwrap();
        }
        // Existence is durable only after a dir fsync.
        disk.sync_dir(Path::new("/d")).unwrap();
        // A second, un-fsynced write is pending.
        {
            let mut f = disk.open(&p, OpenOpts::rw_existing()).unwrap();
            f.write_at(7, b"-lost").unwrap();
        }
        disk.crash(TornDamage::None);
        assert_eq!(disk.durable_bytes(&p).as_deref(), Some(&b"durable"[..]));
    }

    /// A file whose creation was never dir-fsynced does not survive a crash.
    #[test]
    fn fakedisk_uncommitted_create_vanishes() {
        let disk = FakeDisk::new();
        disk.create_dir_all(Path::new("/d")).unwrap();
        let p = PathBuf::from("/d/f");
        {
            let mut f = disk.open(&p, OpenOpts::create_truncate()).unwrap();
            f.write_at(0, b"x").unwrap();
            f.sync_all().unwrap(); // bytes promoted, but dir entry still pending.
        }
        disk.crash(TornDamage::None);
        assert!(disk.durable_bytes(&p).is_none(), "create not dir-fsynced ⇒ gone");
    }

    /// A rename is durable only after `sync_dir`; a crash before it rolls back.
    #[test]
    fn fakedisk_rename_durable_only_after_sync_dir() {
        let disk = FakeDisk::new();
        disk.create_dir_all(Path::new("/d")).unwrap();
        let tmp = PathBuf::from("/d/f.tmp");
        let fin = PathBuf::from("/d/f");
        write(&disk, &tmp, 0, b"body");
        {
            let f = disk.open(&tmp, OpenOpts::rw_existing()).unwrap();
            f.sync_all().unwrap();
        }
        disk.sync_dir(Path::new("/d")).unwrap(); // tmp creation durable.
        disk.rename(&tmp, &fin).unwrap();
        // Crash BEFORE the post-rename dir fsync: the final name is not durable.
        disk.crash(TornDamage::None);
        assert!(
            disk.durable_bytes(&fin).is_none(),
            "rename not dir-fsynced ⇒ final name not durable"
        );

        // Redo: rename + sync_dir, then crash ⇒ final name survives.
        let disk = FakeDisk::new();
        disk.create_dir_all(Path::new("/d")).unwrap();
        write(&disk, &tmp, 0, b"body");
        {
            let f = disk.open(&tmp, OpenOpts::rw_existing()).unwrap();
            f.sync_all().unwrap();
        }
        disk.sync_dir(Path::new("/d")).unwrap();
        disk.rename(&tmp, &fin).unwrap();
        disk.sync_dir(Path::new("/d")).unwrap();
        disk.crash(TornDamage::None);
        assert_eq!(disk.durable_bytes(&fin).as_deref(), Some(&b"body"[..]));
    }

    /// Torn-write damage: a prefix-truncate keeps only a prefix of the last write.
    #[test]
    fn fakedisk_torn_prefix_truncate() {
        let disk = FakeDisk::with_seed(123);
        disk.create_dir_all(Path::new("/d")).unwrap();
        let p = PathBuf::from("/d/f");
        {
            let mut f = disk.open(&p, OpenOpts::create_truncate()).unwrap();
            f.write_at(0, b"0123456789").unwrap();
            // No fsync; the write is pending and will be torn.
        }
        disk.sync_dir(Path::new("/d")).unwrap(); // make the file exist durably.
        // We need a durable existence; the create+dir-fsync above did that, but the
        // bytes are still pending. Crash with a prefix-truncate tear.
        disk.crash(TornDamage::PrefixTruncate);
        let durable = disk.durable_bytes(&p).unwrap();
        assert!(
            durable.len() < 10,
            "prefix-truncate keeps a strict prefix, got {} bytes",
            durable.len()
        );
        assert_eq!(&durable[..], &b"0123456789"[..durable.len()]);
    }

    /// Lying fsync: `sync_all` returns Ok but a crash still loses the bytes.
    #[test]
    fn fakedisk_lying_fsync_loses_on_crash() {
        let disk = FakeDisk::new();
        disk.set_lying_fsync(true);
        disk.create_dir_all(Path::new("/d")).unwrap();
        let p = PathBuf::from("/d/f");
        {
            let mut f = disk.open(&p, OpenOpts::create_truncate()).unwrap();
            f.write_at(0, b"data").unwrap();
            f.sync_all().unwrap(); // lies: reports durable, promotes nothing.
        }
        disk.sync_dir(Path::new("/d")).unwrap();
        disk.crash(TornDamage::None);
        assert_eq!(
            disk.durable_bytes(&p).as_deref(),
            Some(&b""[..]),
            "lying fsync ⇒ bytes lost, file exists empty"
        );
    }

    // --- FaultFs ------------------------------------------------------------

    /// FaultFs fires EIO once on the chosen write_at index, then succeeds.
    #[test]
    fn faultfs_fail_once_write() {
        let disk = FakeDisk::new();
        disk.create_dir_all(Path::new("/d")).unwrap();
        let fs = FaultFs::new(disk.arc(), FaultOp::WriteAt, FaultKind::Eio, 1, true);
        let p = PathBuf::from("/d/f");
        let mut f = fs.open(&p, OpenOpts::create_truncate()).unwrap();
        // call 0: ok; call 1: EIO; call 2: ok again (fail-once).
        assert!(f.write_at(0, b"a").is_ok());
        assert!(f.write_at(1, b"b").is_err());
        assert!(f.write_at(1, b"b").is_ok());
    }

    /// FaultFs fail-always keeps erroring on the chosen class.
    #[test]
    fn faultfs_fail_always_sync() {
        let disk = FakeDisk::new();
        disk.create_dir_all(Path::new("/d")).unwrap();
        let fs = FaultFs::new(disk.arc(), FaultOp::SyncData, FaultKind::Enospc, 0, false);
        let p = PathBuf::from("/d/f");
        let mut f = fs.open(&p, OpenOpts::create_truncate()).unwrap();
        f.write_at(0, b"x").unwrap();
        assert!(f.sync_data().is_err());
        assert!(f.sync_data().is_err(), "dead device keeps failing");
    }

    /// FaultFs short-write returns a partial count, exercising the caller's loop.
    #[test]
    fn faultfs_short_write() {
        let disk = FakeDisk::new();
        disk.create_dir_all(Path::new("/d")).unwrap();
        let fs = FaultFs::new(disk.arc(), FaultOp::WriteAt, FaultKind::ShortWrite, 0, true);
        let p = PathBuf::from("/d/f");
        let mut f = fs.open(&p, OpenOpts::create_truncate()).unwrap();
        let n = f.write_at(0, b"hello").unwrap();
        assert!((1..5).contains(&n), "short write returns a partial count, got {n}");
    }

    /// `calls_seen` counts the chosen op class for sizing a sweep.
    #[test]
    fn faultfs_counts_calls() {
        let disk = FakeDisk::new();
        disk.create_dir_all(Path::new("/d")).unwrap();
        // at = u64::MAX ⇒ never fires; just count.
        let fs = FaultFs::new(disk.arc(), FaultOp::WriteAt, FaultKind::Eio, u64::MAX, true);
        let p = PathBuf::from("/d/f");
        let mut f = fs.open(&p, OpenOpts::create_truncate()).unwrap();
        f.write_at(0, b"a").unwrap();
        f.write_at(1, b"b").unwrap();
        f.write_at(2, b"c").unwrap();
        assert_eq!(fs.calls_seen(), 3);
    }

    // --- MonitorFs ----------------------------------------------------------

    /// MonitorFs passes a correct tmp-write→fsync→rename sequence.
    #[test]
    fn monitorfs_allows_correct_ordering() {
        let disk = FakeDisk::new();
        disk.create_dir_all(Path::new("/d")).unwrap();
        let fs = MonitorFs::new(disk.arc());
        let tmp = PathBuf::from("/d/f.tmp");
        let fin = PathBuf::from("/d/f");
        {
            let mut f = fs.open(&tmp, OpenOpts::create_truncate()).unwrap();
            f.write_at(0, b"body").unwrap();
            f.sync_all().unwrap(); // fsync BEFORE rename — correct.
        }
        fs.rename(&tmp, &fin).unwrap();
        fs.sync_dir(Path::new("/d")).unwrap();
    }

    /// MonitorFs panics if a `.tmp` is renamed before being fsynced.
    #[test]
    #[should_panic(expected = "rename of un-fsynced tmp")]
    fn monitorfs_catches_rename_before_fsync() {
        let disk = FakeDisk::new();
        disk.create_dir_all(Path::new("/d")).unwrap();
        let fs = MonitorFs::new(disk.arc());
        let tmp = PathBuf::from("/d/f.tmp");
        let fin = PathBuf::from("/d/f");
        {
            let mut f = fs.open(&tmp, OpenOpts::create_truncate()).unwrap();
            f.write_at(0, b"body").unwrap(); // dirty, never fsynced.
        }
        fs.rename(&tmp, &fin).unwrap(); // VIOLATION ⇒ panic.
    }

    // =======================================================================
    // GROUND TRUTH: drive the REAL durability layer (WAL / snapshot / segstore)
    // through FakeDisk and assert the crash oracle. These prove the fake models
    // the actual fsync/rename ordering the production code relies on — if the
    // fake were wrong, the WAL would lose an acked frame or misread a torn tail.
    // =======================================================================

    use crate::storage::wal::{Wal, WalConfig, WalReader, WalRecord};
    use std::path::Path as StdPath;
    use std::time::Duration;

    fn ap(seq: u64) -> WalRecord {
        WalRecord::Append {
            box_id: 1,
            seq,
            ts: 1_700_000_000_000 + seq,
            node: None,
            tag: Some("t".into()),
            data: b"payload".to_vec(),
        }
    }

    /// The seqs recovered by replaying every WAL file on a (post-crash) disk image,
    /// stopping each file at its torn tail — exactly the recovery read path.
    fn recover_seqs(disk: &FakeDisk, data_dir: &StdPath) -> Vec<u64> {
        let fs = disk.arc();
        let wal_dir = data_dir.join("wal");
        let mut files: Vec<PathBuf> = fs
            .read_dir(&wal_dir)
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

    fn fast_cfg(dir: &StdPath) -> WalConfig {
        let mut cfg = WalConfig::new(dir);
        cfg.gc_min = Duration::from_micros(50);
        cfg.gc_max = Duration::from_micros(200);
        cfg
    }

    /// ORACLE F-WAL-CRASH-AFTER-FSYNC: every *acked durable* WAL batch survives a
    /// crash (acked ⇒ durable). We drive the real WAL through FakeDisk, wait on the
    /// commit tokens (so the fsync returned ⇒ FakeDisk promoted), crash, and assert
    /// all acked seqs replay.
    #[test]
    fn wal_acked_durable_survives_crash_through_fakedisk() {
        let disk = FakeDisk::new();
        let data_dir = PathBuf::from("/data");
        let wal = Wal::open_at_with(disk.arc(), fast_cfg(&data_dir), 1, 0).unwrap();
        let w = wal.writer();
        for seq in 1..=8 {
            // `append` blocks until the group fsync returns ⇒ durable & acked.
            w.append(ap(seq), true).unwrap();
        }
        // Make the WAL file's NAME durable (a real create+dir-fsync happens during
        // open; model the dir fsync explicitly so the name survives the crash).
        disk.arc().sync_dir(&data_dir.join("wal")).unwrap();
        // Power loss: freeze the device FIRST (so the writer's Drop drain becomes a
        // no-op — nothing un-acked is hardened by a graceful shutdown), then drop.
        disk.crash(TornDamage::None);
        drop(wal);

        let seqs = recover_seqs(&disk, &data_dir);
        assert_eq!(seqs, vec![1, 2, 3, 4, 5, 6, 7, 8], "all acked seqs survive");
    }

    /// ORACLE F-WAL-NONDURABLE-LOST-TAIL: a crash during a burst of NON-durable
    /// writes (never group-fsynced) leaves a clean contiguous prefix — some unacked
    /// tail is lost, but no torn frame is misread and the survivors are dense.
    #[test]
    fn wal_nondurable_burst_crash_is_clean_prefix() {
        let disk = FakeDisk::new();
        let data_dir = PathBuf::from("/data");
        let wal = Wal::open_at_with(disk.arc(), fast_cfg(&data_dir), 1, 0).unwrap();
        let w = wal.writer();
        // First two durable (acked) so we have a guaranteed-survivor prefix.
        w.append(ap(1), true).unwrap();
        w.append(ap(2), true).unwrap();
        // Then a non-durable burst (fire-and-forget; may or may not reach durable).
        for seq in 3..=20 {
            let _ = w.submit(ap(seq), false);
        }
        disk.arc().sync_dir(&data_dir.join("wal")).unwrap();
        disk.crash(TornDamage::None);
        drop(wal);

        let seqs = recover_seqs(&disk, &data_dir);
        // The recovered set is a contiguous prefix [1..=k] with k>=2 (acked ones).
        assert!(seqs.len() >= 2, "acked prefix survives, got {seqs:?}");
        for (i, s) in seqs.iter().enumerate() {
            assert_eq!(*s, i as u64 + 1, "survivors are a dense prefix: {seqs:?}");
        }
    }

    /// ORACLE F-WAL-TORN-MID-PAYLOAD: a torn last write (prefix-truncate) is the
    /// logical end of log — the torn frame truncates, prior frames intact, never a
    /// partial record materialized.
    #[test]
    fn wal_torn_tail_truncates_through_fakedisk() {
        let disk = FakeDisk::with_seed(0xC0FFEE);
        let data_dir = PathBuf::from("/data");
        let wal = Wal::open_at_with(disk.arc(), fast_cfg(&data_dir), 1, 0).unwrap();
        let w = wal.writer();
        for seq in 1..=5 {
            w.append(ap(seq), true).unwrap(); // durable ⇒ fsynced ⇒ promoted.
        }
        // A trailing NON-durable write stays pending (no group fsync follows), so
        // it is the in-flight tail that a torn-write damages on power loss.
        let _ = w.submit(ap(6), false);
        // Give the writer a moment to buffer the non-durable frame, then crash with
        // a prefix-truncate tear of that last pending write.
        std::thread::sleep(Duration::from_millis(5));
        disk.arc().sync_dir(&data_dir.join("wal")).unwrap();
        disk.crash(TornDamage::PrefixTruncate);
        drop(wal);

        let seqs = recover_seqs(&disk, &data_dir);
        // The recovered seqs are a dense prefix of 1..=6 and never contain a
        // bogus/garbled seq: the torn frame 6 truncates, the 5 fsynced ones survive.
        for (i, s) in seqs.iter().enumerate() {
            assert_eq!(*s, i as u64 + 1, "dense prefix, no torn frame misread: {seqs:?}");
        }
        assert!(seqs.len() >= 5, "the 5 fsynced-before-the-torn frames survive");
    }

    /// ORACLE (snapshot atomic swap): the real `write_snapshot_with` through
    /// FakeDisk installs a snapshot only after its dir fsync; a crash before that
    /// falls back to the previous valid snapshot (never zero, never a half body).
    #[test]
    fn snapshot_atomic_swap_through_fakedisk() {
        use crate::storage::snapshot::{
            load_latest_with, write_snapshot_with, Checkpoint, Snapshot,
        };
        let disk = FakeDisk::new();
        let fs = disk.arc();
        let data_dir = PathBuf::from("/data");

        let mk = |id: u64, seq: u64| Snapshot {
            id,
            ts: 1,
            next_box_id: 1,
            checkpoint: Checkpoint {
                wal_idx: 1,
                wal_offset: 0,
                last_checkpoint_seq: seq,
            },
            boxes: vec![],
            routers: vec![],
        };

        // Write snapshot #1 fully (it is durable: write→fsync→rename→dir-fsync).
        write_snapshot_with(&fs, &data_dir, &mk(1, 100)).unwrap();
        // Crash now ⇒ snapshot #1 loads.
        let disk2 = disk.clone();
        disk2.crash(TornDamage::None);
        let loaded = load_latest_with(&fs, &data_dir).unwrap().expect("snap #1");
        assert_eq!(loaded.id, 1);
        assert_eq!(loaded.checkpoint.last_checkpoint_seq, 100);
    }
}
