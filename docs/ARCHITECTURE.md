# streams — Architecture

This document specifies the on-disk and in-memory architecture: storage, WAL, group commit,
segments, indexing, recovery/crash-consistency, metadata, on-disk layout, the priority scheduler
and elastic throttling, the concurrency model, recommended Rust crates, and the latency-budget
analysis for the 1–5 ms target.

It is written for the **scalable phase (phase 4)** but calls out the **simple phase-2** shape at
each layer so the implementation can grow into it without rework. Assumptions: single process,
single machine, good CPU, local NVMe SSD (not HDD, not networked). The semantics being enforced
are specified in [DESIGN.md](DESIGN.md); the wire contract is [API.md](API.md).

---

## 0. Design principles

1. **Never silent loss.** Every **involuntary** eviction/TTL crossing that passes a consumer's
   cursor surfaces an in-band tombstone carrying `[gap_from, gap_to]`. The storage layer's job is
   to make the two floors — `earliest_seq` (first live seq) and `evict_floor` (the tombstone
   trigger, involuntary loss only) — always cheaply queryable. Voluntary deletion advances
   `earliest_seq`, never `evict_floor`, so a deleted gap reads silently (DESIGN §5.1).
2. **Trim at segment granularity, lazily.** Cap/TTL eviction never rewrites data or deletes
   individual records on the hot path; it advances a watermark and drops whole sealed segment
   files. **Deletion** (voluntary) frees a record's payload immediately but reclaims physical
   storage lazily too — popping front slots in-memory (§1), and rewriting/dropping segments in
   the background in phase 4 (§3.5).
3. **The WAL is the durability boundary.** "Only data not yet in the WAL is lost." Everything
   downstream (in-memory index, segments) is a derivable cache of WAL + checkpoints.
4. **Seqs are mostly-sequential u64** → represent the seq→location index as a base+offset vector,
   not a hash map (§1).

---

## 1. In-memory representation of a box

### 1.1 The core: a base+offset location vector

Cap/TTL eviction only ever removes a contiguous prefix; writes never skip; node-filtering is a
read-time filter, not a hole. A **deletion in the middle** of the log is the one source of holes,
and it is handled by keeping the slot in place as a lightweight tombstone (so the array stays
dense and O(1)-indexable) — see below. So the seq→location map is a **contiguous integer-keyed
array offset by the earliest physical seq**:

```rust
struct BoxIndex {
    base_seq: u64,             // seq of locs[0]; == earliest physical (not-yet-popped) seq
    locs: VecDeque<RecordLoc>, // index i  <=>  seq (base_seq + i)
    delete_below: u64,         // max before_seq ever applied (O(1) snapshot/prefix delete)
    tag_index: BTreeMap<Box<str>, Vec<u64>>, // tag -> ascending LIVE seqs (§1.4)
}
struct RecordLoc {
    location: u32,  // which segment file (or sentinel = WAL)
    offset: u32,    // byte offset within that file
    len: u32,       // framed length (read a record without touching neighbors)
    ts: u64,        // server commit ms — kept inline for TTL binary search
    flags: u8,      // has_tag, has_node, in_wal_only (not yet checkpointed), deleted
}
```

**Lookup** `seq → loc` is `locs[seq - base_seq]` — O(1), no hashing, cache-friendly. **Cap/TTL
eviction** of a prefix is `locs.drain(..n)` plus `base_seq += n`; we drop whole segments so this is
bounded. `getDifference(from_seq)` becomes "slice `locs[from_seq - base_seq ..]`" then skip
deleted/expired/foreign slots — exactly the batched-diff primitive.

**Why a vector, not `HashMap<u64,Loc>` / `BTreeMap`:** the base+offset trick eliminates the key
entirely (the key *is* the array position). A `HashMap` costs ~3–4× the per-entry memory and
random access on the hot read path; a `BTreeMap` is log(n) + pointer-chasing. With small
`RecordLoc` entries the index packs into contiguous cache lines, and index memory is bounded by
`cap_records` regardless.

**Deletion & lazy front-reclaim (the one hole source).** A record deleted in the **middle** keeps
its slot as a lightweight tombstone: the `deleted` flag is set, its payload/tag is freed
immediately (subtract `bytes`, decrement `count`, prune its `tag_index` entry), but the slot stays
so `seq - base_seq` indexing remains O(1). Physical slots are popped only from the **front**:
when the prefix of `locs` is fully dead (deleted/evicted/expired), `drain` it and advance
`base_seq` — the same path that serves cap/TTL front-eviction. So a delete is *logically*
immediate (invisible to reads, `count`/`bytes` updated) while physical reclaim is lazy. `base_seq`
is the earliest **physical** seq; `earliest_seq` (DESIGN §5.1) is the earliest **live** seq and may
be greater (the front holds dead-but-not-yet-popped slots).

### 1.2 Per-box in-memory state

```rust
struct Box {
    config: BoxConfig,                  // ttl_ms, cap_records, cap_bytes, discard, durable,
                                        //   priority, auto_priority, ...
    index: parking_lot::RwLock<BoxIndex>, // locs + delete_below + tag_index (§1.1, §1.4)
    head_seq: AtomicU64,                // last assigned seq (log end)
    earliest_seq: AtomicU64,            // earliest LIVE seq, the read watermark (DESIGN §5.1)
    evict_floor: AtomicU64,             // involuntary cap/TTL floor; sole tombstone trigger
    epoch: AtomicU64,                   // bumped on create; detects delete+recreate
    bytes_retained: AtomicU64,          // live payload bytes (for byte-cap eviction + state)
    eff_priority: AtomicI64,            // effective priority, recomputed lazily
    last_consumed_ms: AtomicU64,        // for auto-priority by recency
    waiters: tokio::sync::Notify,       // wakes SSE/diff long-pollers on append
    hot_tail: SegmentWriter,            // the open (unsealed) segment + WAL coupling
}
```

`head_seq`/`earliest_seq`/`evict_floor`/`epoch`/`eff_priority` are atomics so `GET /v0/boxes/:box`
is lock-free and the diff path can decide tombstone-vs-silent with two atomic loads
(`from_seq + 1 < evict_floor` ⇒ tombstone; below `earliest_seq` but not `evict_floor` ⇒ silent
deleted gap) before taking the index read lock. **The dual floor is the on-disk/in-memory
expression of DESIGN §5.1:** `evict_floor` advances only on involuntary cap/TTL eviction of live
records; deletion advances `earliest_seq` only. Invariant `evict_floor <= earliest_seq` is held by
construction. `Notify` is the wakeup primitive that lets SSE/diff hit 1–5 ms without polling. The
global registry is `DashMap<BoxId, Arc<Box>>` for sharded concurrent access across many boxes.

### 1.3 The per-box tag index (efficient match-deletes)

A `match` delete (DESIGN §7) MUST be efficient over many records — it must not scan the whole log.
Each box keeps, inside `BoxIndex`, a **tag index** mapping a tag to its live seqs in ascending
order:

```rust
tag_index: BTreeMap<Box<str>, Vec<u64>>,   // tag -> ascending live seqs
```

- **Maintained on append:** a tagged record pushes its seq onto `tag_index[tag]` (always
  appending, so the vec stays sorted).
- **Maintained on delete & front-reclaim:** when a record is deleted (or its front slot is
  popped), its seq is removed from `tag_index[tag]`; an emptied key is dropped.
- **`match ["tag","Eq","X"]`** → point lookup `tag_index["X"]`, mark those slots deleted (and, for
  a combined `before_seq`, only the seqs `< before_seq`).
- **`match ["tag","Glob","X*"]`** → range scan over keys in `["X", next-key)` (the lexicographic
  successor of the prefix), unioning their seq vectors.

A `before_seq`-only (snapshot/prefix) delete is O(1): bump `BoxIndex.delete_below = max(delete_below,
before_seq)`; reads start at `max(from_seq + 1, base_seq)` and skip any slot `< delete_below`, while
the background reclaimer pops the now-dead front. The tag index is held under the same per-box
index `RwLock` as `locs` (a delete is a rare, mutating operation; the hot read path doesn't touch
it).

### 1.4 Phase-2 (simple) shape

Phase-2 is this exact structure minus segments and WAL: `RecordLoc.location` is unused and payload
bytes live in a `VecDeque<Bytes>` parallel to `locs`. No persistence; restart = empty. Everything
else — API, base+offset index, the dual floor (`earliest_seq`/`evict_floor`) + tombstone logic,
the tag index + permanent-delete path with lazy front-reclaim, node filtering, priority, `Notify`
wakeups — is **identical and fully exercised**. Deletion in phase 2 frees the payload `Bytes`
immediately (drop the slot's `Bytes`, subtract `bytes`, decrement `count`, prune the tag index) and
pops fully-dead front slots; middle-deleted slots stay as flagged tombstones. Phase 4 only
re-points `RecordLoc` from heap `Bytes` to mmap'd segment bytes, inserts the WAL on the write path,
and adds background segment rewrite/drop for physical reclaim (§3.5); the serving and indexing logic
is written once.

---

## 2. WAL (Write-Ahead Log)

### 2.1 Record framing

The WAL is an append-only sequence of length-prefixed, CRC-protected frames (one frame per record;
multi-record writes produce many frames committed as one batch). Multi-byte integers little-endian.

```
 off  size  field
   0    4   frame_len   u32   bytes of this frame EXCLUDING this field
   4    1   type        u8    1=Append 2=BoxCreate 3=BoxDelete 4=RouterCreate
                                5=RouterDelete 6=Delete 7=EvictWatermark
                                8=CheckpointMark 9=ConfigUpdate
   5    1   flags       u8    bit0=has_tag bit1=has_node bit2=durable
   6    4   box_id      u32   interned numeric box id (string<->id in meta store)
  10    8   seq         u64   server-assigned (0 for non-Append control frames)
  18    8   ts          u64   server commit ms
  26    2   node_len    u16
  28    2   tag_len     u16
  30    4   data_len    u32
  34    N   node        bytes (node_len)
   .    M   tag         bytes (tag_len)
   .    P   data+meta   bytes (data_len)   -- opaque payload
   .    8   xxh3        u64   XXH3-64 over bytes [4 .. crc_start)
```

- **`frame_len` first** lets recovery validate frame boundaries without parsing the body and
  detect a torn tail (frame_len past EOF ⇒ truncated write ⇒ discard from here).
- **XXH3-64** (fast, modern 64-bit hash; ~2³² lower false-accept than a 32-bit CRC) over everything
  between `frame_len` and the checksum. A mismatch ⇒ torn/partial frame ⇒ logical end of log
  (truncate). This is the crash-consistency anchor (§4).
- **`box_id` is an interned u32** (not the string name), keeping frames small; the name↔id mapping
  lives in the metadata store (§5).
- **Control frames** (BoxCreate, Delete, EvictWatermark, ConfigUpdate, …) share the same WAL, so
  config, deletes, and data live on one ordered, crash-consistent timeline — there is exactly one
  truth: WAL order. A `Delete` frame records the operation (`before_seq` and/or `match`) so the
  permanent removal is replayed deterministically on recovery (the deleted seqs are re-derived from
  the rebuilt index + tag index, not stored individually).

### 2.2 Append path

```
write request
  -> validate; resolve box_id; assign seq = head_seq.fetch_add(n)   (after a discard:"reject" cap check)
  -> serialize frame(s) into a reusable per-writer scratch BytesMut
  -> hand (frames, durability_class, completion-oneshot) to the single WAL writer task
  -> writer appends bytes to the active wal file (buffered write())
  -> on commit (fsync for durable, or group-commit tick): fulfill oneshots
  -> update in-memory: push RecordLoc into BoxIndex, bump head_seq visibility, Notify watchers
  -> respond { seqs, head_seq, performance }
```

The seq is assigned **before** the WAL commit (so it can be returned) but the record is only
*visible to readers* and *acked to the writer* after its commit class is satisfied. Guarantee: **if
a write was acked, it is in the WAL.** A **single WAL writer task** (fed by an MPSC channel)
serializes all appends — the disk is a single sequential resource, so a single ordered append stream
matches the hardware, makes group commit trivial, and removes write-side lock contention.

### 2.3 Durability classes & group commit

Durability is per-box. Two commit classes, one writer:

| `durable` | Commit class | Behavior |
|---|---|---|
| `true` | fsync-on-commit | Acked only after `fdatasync()` returns. Still **group-committed**: the writer coalesces all pending durable frames in a small window into one `write()` + one `fdatasync()`, then acks them all. |
| `false` | group-commit, no wait | `write()`-en to the page cache and acked immediately. A background `fdatasync()` runs on a timer; writers do not wait. Loss window on crash = un-fsynced tail. |

**Group-commit loop:**

```
loop:
  batch = drain channel (non-blocking) up to MAX_BATCH frames or MAX_BATCH_BYTES
  if empty: park on a Notify until a frame arrives
  write(wal_fd, batch_bytes)                       // one write/writev syscall
  if batch has any durable frame OR fsync timer elapsed:
      fdatasync(wal_fd)                            // one fsync for the whole batch
      ack durable frames
  ack non-durable frames                           // already in page cache
  publish all frames to in-memory indexes; Notify per-box waiters
```

**Tuning for 1–5 ms on NVMe.** NVMe `fdatasync` is ~50–500 µs. An **adaptive window** ≤ 1 ms
amortizes one fsync across hundreds of durable writes under load but collapses to ~0 when quiet (a
lone durable write fsyncs immediately) — group commit only helps under load, never penalizes a lone
write. Use **`fdatasync`** (not `fsync`) — no inode-metadata flush per commit. WAL files are
**preallocated** (`fallocate` to e.g. 64–256 MB) so appends don't extend the inode; the next file is
preallocated ahead of rotation. The fd stays open; no `O_DIRECT` (we want the page cache for fast
read-back and OS coalescing), relying on explicit `fdatasync` for durability.

### 2.4 WAL rotation

The active WAL rotates at its preallocated size (or on checkpoint). Files are named
`wal-<first-frame-seq>.log` (zero-padded); the highest-numbered is active. Rotation: fsync current,
open/preallocate next, atomically update a `CURRENT` pointer in the meta store. Old WAL files are
deletable only after a checkpoint durably absorbs all their frames (§3).

---

## 3. Commit / checkpoint → segments

The WAL is a fast, append-ordered, mixed-box, short-lived log. WAL frames are periodically applied
into per-box **segment files** — the long-term store and read source for `getDifference` — to keep
recovery bounded and reads efficient.

### 3.1 Checkpoint process

A background **compactor** task (triggered by time or WAL rotation):

```
for each box with new frames since last checkpoint:
    append those records (in seq order) to the box's active segment file
    (segment frames are byte-identical to WAL Append frames — a buffered copy of
     contiguous byte ranges, split by box; no re-serialization)
    update the box's .idx file
fsync touched segment + idx files
write a CheckpointMark frame to the WAL (per box: highest_seq_checkpointed, watermarks,
     active-segment positions); fsync the WAL
WAL files whose every frame's seq <= the global min checkpointed seq are now deletable
```

### 3.2 Segment format

Per box, a directory of numbered pairs (named by first seq, `seg-<first_seq>`, zero-padded so they
sort into seq order — ARCHITECTURE §6). A segment covers a contiguous range `[start_seq, end_seq]`.
Implemented in `src/storage/segment.rs`:

```
seg-<first_seq>.data    append-ordered record frames (a CLOSE VARIANT of the §2.1 WAL frame:
                        every frame is an Append, so there is no `type` byte — just
                        len + flags + seq/ts + node/tag/payload + XXH3-64)
seg-<first_seq>.idx     fixed-stride 20 B/entry: [offset:u32, len:u32, ts:u64, flags:u8, pad:3];
                        entry i  <=>  seq (first_seq + i)
```

A `.data` frame is `frame_len:u32` (excludes itself) + `flags:u8` (`has_tag`/`has_node`) +
`seq:u64` + `ts:u64` + `node_len:u16` + `tag_len:u16` + `data_len:u32` + `node` + `tag` +
`data+meta` blob + `xxh3:u64` (over `[4..crc_start)` — the same crash anchor as the WAL §2.1). On a
**sealed/immutable** segment a checksum mismatch is corruption, surfaced rather than silently
truncated (unlike the WAL's torn tail).

The `.idx` is the on-disk twin of `BoxIndex`: fixed-stride (**20 bytes/entry**), so `seq → entry` is
`(seq - first_seq) * 20` — a direct seek, no scan. This makes rebuilding the in-memory index on
restart a **bulk read of `.idx` files** rather than a re-parse of all data. The inline `ts` enables
**binary search for the TTL boundary**, and the inline `flags` a cheap tag/node presence probe,
without touching the data file. A segment is **sealed** when any of three triggers fires
(`segment_max_events` ≈ 10k, `segment_max_bytes` ≈ 64 MB, or `segment_max_age_ms` for an idle box —
§3.6); the newest is "active" (still appended), older ones immutable.

A `SegmentBuilder` accumulates a contiguous, gapless run of records into the `(.data, .idx)` byte
buffers; a `SegmentStore` (§3.6) persists/reads/drops them.

### 3.3 TTL / cap eviction — cheap, no rewrite

Cap/TTL eviction is **involuntary** loss and advances **`evict_floor`** (the tombstone trigger).
It never rewrites a segment. Two mechanisms:
1. **Watermark advance (logical eviction).** For count cap, when `head_seq - earliest_seq >
   cap_records`, advance `evict_floor` past the oldest live records (which also advances
   `earliest_seq`). For TTL, advance past records whose `ts < now - ttl_ms` (binary search on
   inline `ts`). Advancing is an `AtomicU64` store + `VecDeque::drain` on the index front. Popping
   **already-deleted** front slots is *not* eviction and does **not** advance `evict_floor` (only
   `earliest_seq`/`base_seq`); evicting **live** records does. A read with `from_seq + 1 <
   evict_floor` returns a tombstone (DESIGN §5.4).
2. **Segment dropping (physical reclaim).** A whole **sealed** segment whose highest seq <
   `earliest_seq` is entirely gone (evicted or deleted) → its `.data`+`.idx` files are deleted.
   Reclaim is segment-granular and lazy (Redis `~` / Kafka), so the box may retain slightly more
   than cap (only whole sealed segments drop) — the documented, accepted approximation. The active
   segment is never dropped.

The new watermark is persisted via an **`EvictWatermark`** control frame (folded into the next
CheckpointMark), so eviction and the tombstone boundary survive restart. A crash between
watermark-advance and file-delete is harmless: on restart we re-derive which segments are fully
below the watermark and delete them (idempotent reclaim).

**Full-box policy.** `discard:"old"` (default) evicts oldest as above. `discard:"reject"` (durable
queue): the cap check happens on the append path *before* WAL write and seq assignment, so a rejected
write (`422 box_full`) never enters the log — never ack-then-drop (the NATS DiscardNew foot-gun).

### 3.4 Serving `getDifference`: mmap vs buffered

- **Sealed (immutable) segments → mmap** (`memmap2`): map `.data` once, slice `[offset..offset+len]`
  per record (zero-copy, page-cache-backed). A diff is: bound-check against `evict_floor`
  (tombstone?) and `earliest_seq` (live floor), slice the index, then per entry copy framed bytes
  out of the mmap into the response, skipping deleted/expired/own-node slots during the copy,
  bounded by `limit`.
- **Active segment → buffered `pread`** (the growing file is usually still in page cache from the
  write; mmap past EOF is UB and remapping per append is wasteful).
- **Newest records (written, not yet checkpointed) → served directly from WAL bytes** via the same
  `RecordLoc` mechanism (`location = WAL`). So a consumer 1–5 ms behind head reads from the WAL/page
  cache, never waiting for checkpoint — essential to the latency target.

### 3.5 Permanent deletion & the background reclaimer (async physical reclaim)

Deletion (DESIGN §7) is **logically immediate but physically lazy**, so it never stalls the hot
path. The two halves:

1. **Logical removal (synchronous, on the delete call).** Under the per-box index lock: resolve the
   target seqs (`before_seq` via `delete_below`; `match` via the tag index, §1.3), set each slot's
   `deleted` flag, free its payload/tag, subtract `bytes`, decrement `count`, prune tag-index
   entries, and advance `earliest_seq` if the front became dead. Write the `Delete` control frame
   (§2.1) and ack. Reads see the effect at once. **`evict_floor` is untouched** (deletion is
   voluntary), so no tombstone results.
2. **Physical reclaim (asynchronous, background).** A **reclaimer** task (sharing the compactor's
   schedule, or its own low-priority lane) reclaims storage for deleted records:
   - **In-memory (phase 2):** the payload `Bytes` were already dropped at delete time; the reclaimer
     just pops fully-dead front slots (`locs.drain` + `base_seq +=`), the same path as cap/TTL
     front-eviction. Middle holes remain as flag-only tombstones until the front catches up.
   - **On-disk (phase 4):** a **sealed** segment whose records are *all* deleted is dropped whole
     (delete `.data`+`.idx`), exactly like cap eviction. A segment that is only **partially**
     deleted is reclaimed by **background segment rewrite**: copy its surviving (live) records into
     a new sealed segment, fsync, atomically swap the `.idx`/`.data` + rebuild that segment's index
     range, then unlink the old files. Rewrite is off the hot path, rate-limited, and idempotent
     across crashes (the `Delete` control frames replay deterministically, so a crash mid-rewrite
     re-derives the same live set). Until a segment is rewritten/dropped, deleted records still
     occupy disk but are invisible to every read — the documented async-reclaim tradeoff (the
     analog of cap eviction's "may transiently exceed cap by one segment").

The reclaimer never affects correctness or visibility — only when the freed bytes are returned to
the OS.

### 3.6 Tiered storage: the `SegmentStore` trait + HOT/COLD tiers (phase 6)

Data outgrows RAM and the fast NVMe. Each box's segments are split across **two tiers**:

- **HOT** — the active segment + recent sealed segments, on fast local NVMe (a per-box dir under the
  data dir). Reads here are buffered/mmap-fast; the live tail (active segment + the in-memory index
  + a bounded recent-record cache) is always hot and independent of cold access.
- **COLD** — older sealed segments, on a slower tier. **v1's cold tier is a different configured
  folder** (`STREAMS_COLD_DIR`); when unset (the default in every existing test), **tiering is
  disabled — nothing relocates and behavior is unchanged by construction.**

Both tiers sit behind one trait, `SegmentStore` (`src/storage/segstore.rs`), so an **object store
(S3) drops in later as another impl without touching the engine** (S3 is explicitly future work;
only `LocalSegmentStore` exists now):

```rust
trait SegmentStore: Send + Sync {           // synchronous ⇒ runs on a blocking/IO pool off the hot path
    fn put(&self, id, data, idx) -> Result<()>;          // durable (fsync'd) write of both parts
    fn read_range(&self, id, part, offset, len) -> Result<Vec<u8>>;  // one record's frame, or a bulk .idx
    fn delete(&self, id) -> Result<()>;                  // drop a whole segment (idempotent)
    fn list(&self) -> Result<Vec<SegmentId>>;            // ids ascending (oldest first)
    fn exists(&self, id, part) -> bool;                  // cheap probe (relocation idempotency)
    fn len(&self, id, part) -> Result<u64>;
}
```

A `SegmentId` is the segment's first seq; `part` is `Data` or `Idx`. A per-box `BoxTier` bundles a
required HOT store + an optional COLD store and `resolve(id)`s which tier holds a segment, **preferring
the HOT copy when both exist** (the transient mid-relocation window).

**The HARD INVARIANT.** Cold reads MAY degrade `getDifference`/historical reads but MUST NOT affect
**writes** or **live delivery** (SSE/tail). Cold I/O + the relocator run on a **separate blocking/IO
pool**; they never hold a box write lock or block an SSE push during a slow cold fetch. The trait is
deliberately **synchronous and self-contained** so each call can be issued via `spawn_blocking`.

**Memory bounding.** The in-memory index maps `seq → (tier, segment, offset, len)`; recent records
live in a **bounded cache**, older payloads are read from segments on demand. Index memory stays
bounded by the index entry count, not the payload volume.

**Hot-retention + relocation (later stage).** Sealed segments beyond the hot-retention bound
(`hot_retain_segments` ≈ last 4, or `hot_retain_bytes`) relocate to cold. Relocation is **crash-safe
and idempotent**: copy the segment to cold → fsync → durably flip the tier pointer (meta/WAL) →
delete the hot copy. If interrupted, restart prefers the surviving copy (`BoxTier::resolve` favors
HOT) — a segment is never lost. Cap/TTL/delete reclaim drops a whole segment file/object in **either**
tier. The WAL remains the durability boundary; segments are a derivable materialization.

Stage 1 builds the trait, the segment format, the `BoxTier`, and the config knobs (§3.2, below).
Sealing-on-the-write-path, the relocator, the bounded cache, and cold serving land in later stages.

---

## 4. Recovery on restart

Goal: rebuild all in-memory state, lose only data not yet in the WAL, tolerate a crash at any
instant.

```
1. Open data dir; load latest valid metadata snapshot (boxes, routers, name<->id,
   watermarks (evict_floor + earliest_seq), delete_below per box, CURRENT wal ptr,
   last_checkpoint_seq).
2. Per box: bulk-load segment .idx files into BoxIndex (fixed-stride sequential read). Set
   base_seq from the lowest surviving segment, evict_floor/earliest_seq from the persisted
   watermarks, head_seq from the highest segment seq. Rebuild the tag index from the
   surviving tagged records.
3. Replay the WAL from the frame after the last CheckpointMark. For each frame, in order:
     - frame_len fits remaining bytes? else torn tail -> STOP (truncate here).
     - xxh3 valid? else torn/partial -> STOP (truncate here).
     - apply: Append -> push RecordLoc (location=WAL), index tag, bump head_seq.
              Delete -> re-apply before_seq/match: mark slots deleted, free payloads,
                        prune tag index, advance earliest_seq (NOT evict_floor).
              other Control frames -> mutate config/watermarks/routers.
4. Truncate the WAL at the first bad/partial frame boundary (ftruncate) -> clean for new appends.
5. Re-derive droppable/rewritable segments (sealed, fully or partially below the live set) and
   reclaim them (idempotent — §3.5).
6. Resume: open the truncated/fresh active WAL, start the writer + compactor + reclaimer.
```

**Crash-consistency guarantees:**
- **Torn tail:** detected by `frame_len` overrunning EOF or checksum mismatch; stop at the last fully
  written, checksum-valid frame and truncate. Since a write is acked only after its frame is committed
  (and fsynced, for durable boxes), an **acked durable write is always a complete checksum-valid frame ⇒
  never lost.**
- **Partial `write()`:** the trailing partial frame fails CRC/length and is discarded; never
  interpreted as data.
- **`CheckpointMark` is itself CRC-protected and fsynced.** Crash after writing segments but before
  the CheckpointMark is durable ⇒ recovery re-replays those WAL frames into segments; duplicate
  appends are skipped by seq (a seq already in the segment index is ignored). Crash after the
  CheckpointMark but before deleting absorbed WAL files ⇒ those files are replayed-and-skipped
  (seqs ≤ checkpointed) — harmless.
- **"Only data not yet in the WAL is lost":** for `durable=true` an acked write survives (ack waits
  for fsync); for `durable=false` writes acked but not yet fsynced (within the group-commit timer)
  can be lost on power loss — the documented fast-path tradeoff, surfacing to consumers as ordinary
  eviction-style gaps. In both cases the boundary is precisely "what reached the WAL on disk."

---

## 5. Metadata store

Two-tier, mirroring the WAL philosophy: **mutations are control frames in the WAL** (crash-consistent
and ordered with data), and a **periodically-snapshotted metadata file** lets recovery start without
replaying the WAL from time zero.

```rust
struct Meta {
    boxes:   HashMap<String, BoxId>,    // name -> interned u32 id (stable across restart)
    box_cfg: HashMap<BoxId, BoxConfig>,
    watermarks: HashMap<BoxId, (u64, u64)>, // persisted (evict_floor, earliest_seq) per box
    delete_below: HashMap<BoxId, u64>,  // persisted max before_seq applied (snapshot delete)
    routers: Vec<Router>,               // {name, source, dest, preserve_*, filter, allow_cycle}
    epochs: HashMap<BoxId, u64>,        // delete+recreate detection
    next_box_id: u32,
    current_wal: String,
    last_checkpoint_seq: u64,           // global lower bound for WAL replay
}
```

**Deletes are not standing state.** Unlike the old read-time filter set, a permanent delete (DESIGN
§7) leaves **no** per-box rule structure to persist: it is a one-shot operation logged as a `Delete`
control frame and reflected immediately in the index (slots flagged deleted, payloads freed) and in
the two persisted watermarks + `delete_below`. The only deletion-related structure carried at
runtime is the per-box **tag index** (§1.3) used to *find* matching seqs efficiently; it is derived
from the live records and rebuilt on recovery, not snapshotted.

**The read loop is filter-free for deletion.** Because a deleted slot carries a `deleted` flag, the
read loop just skips flagged slots — O(1) per slot, no set/prefix lookup per record. The same loop
skips TTL-expired slots (involuntary → feeds `evict_floor`) and own-node records (skip if `$node ∈
reader set`). One pass for TTL + deleted + node skipping; the tag index is consulted only at
*delete* time, never on the read path.

**Durability & recovery.** The snapshot is written atomically (`snapshot.<n+1>.tmp` → fsync → rename
→ fsync dir → delete old); atomic rename gives crash-atomic metadata swaps. On recovery, load the
latest valid snapshot, then replay WAL control frames after `last_checkpoint_seq` — the same single
pass as §4, so config and data are restored consistently relative to each other. bincode is used for
the compact snapshot; metadata is tiny and changes rarely.

---

## 6. On-disk layout

```
<data_dir>/
├── meta/
│   ├── snapshot.0007.bin            # latest atomic metadata snapshot
│   └── snapshot.0006.bin            # previous (kept until next snapshot fsynced)
├── wal/
│   ├── CURRENT                      # tiny file naming the active wal segment (atomic-renamed)
│   ├── wal-0000000000001024.log     # preallocated, append-only, mixed-box framed records
│   └── wal-0000000000004096.log     # active wal segment (highest first-seq)
└── boxes/                          # HOT tier (fast NVMe)
    ├── 0000000A/                    # one dir per box, named by interned box_id (hex)
    │   ├── seg-0000000000000001.data
    │   ├── seg-0000000000000001.idx # fixed-stride 20 B [offset,len,ts,flags,pad]; seq->entry by arithmetic
    │   ├── seg-0000000000010001.data
    │   ├── seg-0000000000010001.idx
    │   └── seg-0000000000020001.data  (active segment, newest; + .idx)
    └── 0000000B/
        └── ...

<STREAMS_COLD_DIR>/                  # COLD tier (optional; absent ⇒ tiering disabled, all hot)
└── boxes/
    └── 0000000A/                    # relocated older sealed segments, same seg-<first_seq> naming
        ├── seg-0000000000000001.data
        └── seg-0000000000000001.idx
```

WAL is **process-global** (one ordered stream → trivial group commit, matches the single sequential
disk). Segments are **per-box** (independent eviction, per-box mmap, locality for `getDifference`).
Segment files named by first seq sort into seq order; finding a segment for a seq is a binary search
over first-seqs. The same `seg-<first_seq>` naming is used in both tiers, so a relocated segment keeps
its identity (§3.6); the cold tier mirrors the per-box layout under `STREAMS_COLD_DIR`. A box delete
is a control frame + a fast rename `boxes/0000000A.deleted` then background unlink (fast and
crash-safe).

### 6.1 Storage config knobs (phase 6)

| Knob (env) | Default | Meaning |
|---|---|---|
| `STREAMS_DATA_DIR` | `./streams-data` | Hot tier + WAL + meta root. |
| `STREAMS_COLD_DIR` | *(unset)* | Cold tier root. **Unset ⇒ tiering disabled (all hot).** |
| `STREAMS_SEGMENT_MAX_EVENTS` | `10000` | Seal a segment after this many records. |
| `STREAMS_SEGMENT_MAX_BYTES` | `64 MiB` | Seal after this many `.data` bytes (big-payload guard). |
| `STREAMS_SEGMENT_MAX_AGE_MS` | `3600000` | Seal an idle/partial segment after this age; `0` disables. |
| `STREAMS_HOT_RETAIN_SEGMENTS` | `4` | Keep this many newest sealed segments hot before relocating. |
| `STREAMS_HOT_RETAIN_BYTES` | `0` | Optional hot sealed-byte bound; stricter of the two wins (`0` ⇒ off). |

These live on `ServerConfig` (`cold_dir` + a `SegmentConfig`); the seal triggers are read through the
`Clock` so the age trigger and the relocator are drivable by `TestClock` (no wall-clock sleeps in
tests).

---

## 7. Priority scheduler & elastic throttling

The unit of scheduling is **delivery work** for a box: waking SSE watchers, running routers, and
flushing pending write batches / group commit. Writes are admitted on the request path; scheduling
governs the *post-write propagation* that must hit the latency target. The priority **formula and
defaults** are in [DESIGN.md §3](DESIGN.md).

### 7.1 Shape: a bounded pool draining a banded ready-set of *dirty boxes*

```
write/router makes a box "dirty" -> insert into its shard's ready set (at most once)
                                         |
                                         v
   banded weighted-fair queue (DWRR) keyed by effective priority + aging
                                         |
                       pop highest-credit band -> bounded worker pool (N_workers tasks)
                       each worker drains ONE box fully, requeues if more work arrived
```

The schedulable entity is a **box, not a record/watcher**: a write marks the box dirty and inserts
it into the ready set if not already present (a membership bit prevents duplicates). This bounds the
queue to O(#dirty boxes) and coalesces a box's burst of writes into one unit of work. A worker that
picks up box B **drains B fully** (wakes all its SSE watchers, forwards to all router dests, flushes
its commit batch) before moving on — preserving per-box ordering and amortizing the lock.

### 7.2 Banded weighted-fair queue (anti-starvation) + aging

A pure max-heap on priority starves low-priority boxes. Instead, priorities bucket into bands drawn
by **deficit weighted round-robin (DWRR)**:

```
Band  P_eff range    weight
 B4   >= 750           8
 B3   500..749         4
 B2   250..499         2
 B1   0..249           1
 B0   < 0              1   (explicitly deprioritized)
```

Within a band, FIFO by `enqueued_at`. Across bands, each round grants credit proportional to weight;
with the defaults, for every 1 low-priority box serviced up to 8 top-band boxes may be — high
priority strongly favored, but B1/B0 always make forward progress every round.

**Aging** prevents a box stuck at the bottom of a busy band from waiting forever:
`age_boost = AGE_RATE * min(now - enqueued_at, AGE_CAP_MS)` (+100/s, capped at +1000 after 10 s). A
50 ms aging tick promotes boxes across band boundaries. `enqueued_at` resets only when the box is
actually serviced, so a continuously-rewritten box still ages. **Combined guarantee:** no box waits
more than 10 s before reaching the top band, and DWRR drains the top band every round — worst-case
scheduling latency is bounded even under sustained high-priority load. Under unsaturated load the
ready set is near-empty and boxes are serviced within microseconds of being marked dirty (1–5 ms
target).

### 7.3 Elastic throttling — shed cost, never data

A **governor task** every 100 ms samples three cheap signals into `pressure ∈ [0,1]`: ready-set
depth vs `N_workers`, EWMA scheduling latency vs the 5 ms ceiling, and the blocking/compute-pool busy
ratio. `pressure` is published as a lock-free atomic and drives an escalating, composable ladder:

1. **Batch coalescing (`pressure > 0.2`).** Stop waking watchers per-record; coalesce a box's
   pending records into one multi-record frame / diff. Cheap, lossless, often improves throughput.
   Window grows `0..20 ms` with pressure.
2. **Widen group-commit window (`pressure > 0.4`).** `commit_window_ms = lerp(0.5, 10, pressure)` —
   fewer fsyncs/sec, more headroom; cost is up to +9.5 ms write-ack latency, observed as latency,
   never loss.
3. **Defer lowest-value work (`pressure > 0.8`, sustained).** Routers (fan-out) are enqueued one band
   lower; `B0`/negative-priority boxes stop receiving DWRR credit until `pressure < 0.6` (hysteresis)
   — their data is still durably stored and fully pollable via `getDifference`, only the *push* is
   paused. If a per-shard ingest channel is full and `pressure ≈ 1.0`, the write endpoint returns
   **`429` + `Retry-After`** (writers may bypass with `disable_backpressure: true`).

The cardinal rule: **throttling degrades latency and push-eagerness, never correctness.** A deferred
box is always fully consistent on the next `getDifference`. All data loss remains the explicit,
configured cap/TTL path with in-band tombstones; full-write rejection is synchronous (`422`/`429`),
never ack-then-drop.

| Condition | Client-visible effect | Loss? |
|---|---|---|
| Healthy (`pressure < 0.2`) | 1–5 ms delivery, per-record frames | No |
| Mild pressure | Coalesced multi-record frames, ~5–15 ms | No |
| Heavy pressure | Slower write-acks; low-priority pushes paused but pollable | No |
| Saturation on write | `429 + Retry-After` (write rejected synchronously) | No |
| Cap/TTL crosses cursor (independent of pressure) | In-band `tombstone` with `[gap_from, gap_to]` | Explicit, never silent |

---

## 8. Concurrency model

### 8.1 Sharding

Boxes are partitioned across `S` shards by `shard = hash(box_id) % S`, with `S = N_workers` (one
shard per core) by default. Each shard owns its slice of the box map, its ready-set, and its WAL
ingest lane. **State is sharded, not globally locked.** The only global structures are the lock-free
`pressure` atomic and the read-mostly box-name→shard directory (`dashmap`). The single WAL writer
(§2.2) is fed by per-shard MPSC lanes.

### 8.2 Lock strategy: short shard lock + per-box fine lock

- **Per-shard mutex** held only for the O(1) ready-set splice (push/pop a deque, flip a bitset bit) —
  a few instructions, negligible contention even when workers share a shard.
- **Per-box `RwLock`** guarding the append tail, watcher list, and pending-work buffer. A worker
  draining box B holds only B's lock, so two workers drain two different boxes in the same shard
  fully in parallel. Reads (`getDifference`) take the box read lock against committed segments; the
  append tail uses a seqlock so reads rarely block writes.
- **Lock ordering** to avoid deadlock: shard-ready lock → box lock, never reverse; routers acquire
  source then dest in ascending `(shard, box_id)` order.

### 8.3 How operations interleave

| Operation | Path | Contention |
|---|---|---|
| **Write** | HTTP task → shard lane → append under box lock → assign seqs → mark dirty (short shard lock) → return | box's own lock + brief splice; independent boxes never contend |
| **getState** | lock-free atomic loads (head/earliest/count) + `last_consumed_ms` store | lock-free |
| **getDifference** | box read lock over committed segments; bounded batch; bump recency; tombstone if `from_seq+1 < earliest_seq` | box read lock; doesn't block other boxes; rarely blocks the append tail (seqlock) |
| **SSE push** | worker draining the box pushes frames to each watcher's bounded channel; slow consumer's channel full → degrade that connection, not the box | per-box during drain; per-connection channel isolates a slow client |
| **Router** | at drain time, new src records handed to the dest shard via its ingest MPSC (no cross-shard lock); dest box scheduling/priority applies; node filtering at dest read time | cross-shard hop only when src/dst differ; no cross-shard lock acquisition |

### 8.4 Slow-consumer isolation (SSE)

Each SSE connection has a bounded outbound channel (default 1024 frames). If a worker can't enqueue,
it does **not** block the box drain; the connection is marked **lagged**, the server stops buffering
for it, records the last-delivered composite cursor, and on the next successful send emits a tombstone
for the skipped range (for lossy boxes) so the client catches up via `getDifference`. One slow client
is contained to its own connection; the box and all other watchers proceed at full speed.

### 8.5 Mapping onto tokio + a bounded compute pool

- **One multi-threaded tokio runtime** (`worker_threads = num_cpus`) runs all async I/O: HTTP
  (axum/hyper), SSE connections, channel plumbing.
- **Delivery workers** are long-lived tokio tasks (one per shard) running the §7.1 loop; they park on
  a per-shard `Notify`/MPSC when their ready set is empty (no busy-spin) and are woken by mark-dirty.
- **A separate bounded blocking/compute pool** quarantines genuinely blocking or CPU-heavy work so it
  can't starve the reactor: fsync/WAL durability via `spawn_blocking` onto a bounded pool
  (`max_blocking_threads ≈ 2·N_workers`; group commit keeps it small), and large diff serialization /
  segment compaction on a dedicated rayon lane.

**Why this hits the target:** the hot delivery path stays on the async runtime and is pure in-memory
work (heap splice + channel sends), completing in microseconds when unsaturated; blocking work is
quarantined in a bounded pool that can never consume all async threads; backpressure is structural
(every ingest and SSE channel is bounded, with defined `429`/tombstone behavior rather than unbounded
memory growth); and there is no global lock on writes, reads, or pushes for distinct boxes, so
throughput scales ~linearly with cores until the durability pool or NVMe is the bottleneck — at which
point group-commit widening trades latency for throughput, gracefully.

---

## 9. Latency budget (how 1–5 ms is achieved)

The push chain on a non-durable write, unsaturated:

1. **Append + wake** (~tens of µs): append to the in-memory tail, assign seq, write frame bytes to
   the WAL page cache, signal the box's `Notify`. (For `consistency:strong` SSE / `durable` boxes,
   the signal/ack waits for the group-commit fsync — see below.)
2. **Watcher registry, not scan** (~µs): each box keeps its registered watchers; the `Notify` wakes
   only those connections. No periodic poll; idle boxes cost nothing.
3. **Coalesced flush** (~tens of µs): each woken worker reads from its per-box cursor up to
   `limit`/`max_batch_bytes`, skipping deleted/expired/own-node slots, builds one frame, writes to
   the socket and flushes (`X-Accel-Buffering: no` + `TCP_NODELAY` → no proxy/Nagle buffering).
4. **Routers add one hop** (~µs): a forwarded record triggers the dest box's `Notify` exactly like a
   direct write — one extra in-process append.
5. **Backpressure cannot stall the writer**: the write path only *signals*; slow-consumer buffering
   happens in the consumer's own task, so fast-consumer latency is independent of slow ones.

Budget breakdown (NVMe-class hardware, unsaturated):

| Stage | Typical | Notes |
|---|---|---|
| HTTP parse + validate | 50–200 µs | small JSON bodies |
| WAL frame serialize + buffered write | 10–50 µs | reusable scratch buffer, page cache |
| `fdatasync` (durable / strong only) | 50–500 µs | one per group-commit batch |
| Index update + `Notify` | < 10 µs | atomic + deque push |
| Worker wake + filter + frame build | 20–100 µs | per-box read lock, in-memory slice |
| Socket write + flush | 10–50 µs | `TCP_NODELAY`, explicit flush |

Non-durable / `eventual`: end-to-end well under 1 ms typical, comfortably inside the 1–5 ms target.
Durable / `strong`: add the group-commit fsync window (≤ 1 ms adaptive), still inside budget. The
only intentional latency knobs are `consistency:strong` (adds the fsync window) and the scheduler's
deliberate pacing of low-priority boxes under CPU pressure — both explicit and visible (in the
`performance.fsync_ms`/`throttle_wait_ms` fields and SSE `error` frames).

---

## 10. Recommended Rust crates

| Crate | Role | Justification |
|---|---|---|
| `tokio` | async runtime | Multi-threaded executor, timers, MPSC, `Notify` — backbone for the WAL writer task, compactor, SSE fan-out. |
| `axum` | HTTP framework | Ergonomic typed routing over hyper; first-class streaming responses for the SSE endpoint. |
| `hyper` | HTTP core | Underlies axum; direct access for fine control over SSE flushing / `X-Accel-Buffering`. |
| `tower` / `tower-http` | middleware | Timeouts, the `429`+`Retry-After` elastic-throttle layer, compression negotiation as middleware. |
| `serde` + `serde_json` | (de)serialization | JSON-first API bodies; `#[serde]` on request/response structs. |
| `bincode` | compact meta snapshots | Fast compact binary for the metadata snapshot. |
| `bytes` | zero-copy buffers | `Bytes`/`BytesMut` for reference-counted payload slices and reusable WAL framing scratch. |
| `xxhash-rust` (xxh3) | frame integrity | XXH3-64 for WAL/segment checksums — modern, fast, 64-bit (~2³² lower false-accept than 32-bit CRC); the torn-tail crash anchor. |
| `memmap2` | segment reads | mmap sealed immutable segments for zero-copy, page-cache-backed `getDifference`. |
| `parking_lot` | locks | Faster, smaller `RwLock`/`Mutex` for the per-box index lock on the hot path. |
| `dashmap` | box registry | Sharded concurrent `HashMap<BoxId, Arc<Box>>` — many boxes without a global lock. |
| `arc-swap` | COW config | Wait-free `load()` of a box's current config/router set on the hot path; rare writers publish a new `Arc`. |
| `smallvec` | tiny allocations | Per-write seq batches / small node/tag buffers avoid heap allocation in the common single-record case. |
| `rustix` (or `nix`) | raw fs syscalls | `fdatasync`, `fallocate`, `pread`, atomic `renameat` + dir fsync — durability primitives std doesn't expose. |
| `ahash` | fast hashing | Backing hasher for `dashmap` / exact-tag `HashSet`. |
| `tracing` + `tracing-subscriber` | observability | Structured spans populate the per-response `performance` block. |
| `metrics` / `prometheus` | metrics | Backs `GET /v0/metrics` (Prometheus text + JSON snapshot). |
| `thiserror` | error model | Ergonomic error enum mapping to the uniform `{"error":{...}}` body and HTTP codes. |
| `rayon` | compute pool | Bounded lane for large diff serialization / segment compaction off the reactor. |

---

## 11. Phase-2 → Phase-4 summary

**Unchanged across phases** (write once in phase 2): the HTTP API surface, the base+offset
`BoxIndex`, the dual floor (`earliest_seq`/`evict_floor`) + `epoch` atomics, tombstone/gap
computation, the per-box tag index + permanent-delete path (logical removal + lazy front-reclaim) +
node loop-prevention read loop, priority/recency tracking, `Notify`-based SSE/diff wakeups, the
banded scheduler (in-memory it just has nothing to fsync).

**Added in phase 4:** the WAL (framing, single-writer group commit, per-box durable fsync), the
compactor (WAL→segment checkpointing), segment files + `.idx` + mmap serving, segment-granular lazy
cap/TTL eviction, the **background reclaimer** (async physical reclaim of deleted records via
whole-segment drop and partial-segment rewrite, §3.5), metadata snapshots + control-frame replay
(incl. `Delete` frames), and restart recovery. Phase 4 only re-points `RecordLoc` from heap `Bytes`
to `(location, offset, len)`, inserts the WAL on the append path, and adds background segment
rewrite/drop — the serving and indexing logic is reused intact.

**Added in phase 6 (tiered storage, §3.6):** the `SegmentStore` trait + `LocalSegmentStore` + per-box
`BoxTier` (HOT + optional COLD), the segment file format (`src/storage/segment.rs`), and the
seal/hot-retention/`STREAMS_COLD_DIR` config. Tiering is **additive and transparent**: with no cold
dir, nothing relocates and the phase-4 behavior is unchanged. Cold I/O + the relocator run off the
hot path; the HARD INVARIANT is that cold reads may degrade historical reads but never affect writes
or live delivery. Later stages wire sealing into the write path, add the relocator + bounded recent
cache, and serve cold reads on the blocking pool — the `RecordLoc` already carries the `(location,
offset, len)` triple, so it grows a `tier` discriminator without reshaping the index.

**The three highest-value invariants the storage layer enforces:** (1) *never silent involuntary
loss* — `evict_floor` is always durable and cheaply queryable, so any read crossing it yields an
in-band tombstone, while a purely-deleted gap (below `earliest_seq`, at/above `evict_floor`) reads
silently; (2) *segment-granular, lazy cap/TTL eviction* — never rewrite on the hot path; advance a
watermark and drop whole sealed segments; (3) *deletion is logically immediate, physically lazy* —
free the payload and advance `earliest_seq` synchronously, reclaim disk/memory in the background.

---

## 12. Queue layer (materialized lease view, reclaim freelist, claim cursor)

A **queue** box (DESIGN §10) reuses every structure above for its **jobs log** (the box's own
`BoxIndex` + WAL + segments) and adds a thin lease layer on top — purely additive, no change to
the §1–§11 storage path. A queue holds **two logs**: the jobs log (the box) and a companion
**leases log** of lifecycle events, both WAL-framed. The live who-holds-what state is the
**materialized projection** of the leases log (event-sourced, DESIGN §10.1), held in memory:

```rust
struct QueueState {
    leases:        HashMap<u64, Lease>,    // seq -> active lease (the materialized view)
    deliveries:    HashMap<u64, u32>,      // seq -> delivery count (dead-letter trigger)
    reclaim:       BinaryHeap<Reverse<u64>>, // freelist: expired-lease / nacked seqs, drained first
    delayed:       BinaryHeap<(u64, u64)>, // (ready_at_ms, seq) for delayed nacks
    claim_cursor:  u64,                    // next never-yet-leased seq (fresh-job hand-out)
    dead_lettered: u64,                    // cumulative DLQ count (observability)
}
struct Lease { node: Box<str>, lease_id: u64, deadline_ms: u64, by_work_conn: Option<ConnId> }
```

- **Materialized lease view** (`leases`): rebuilt by replaying the leases log on restart; since
  that log is non-durable by default (`leases_durable:false`), a crash typically replays *no*
  active leases, so every in-flight job is immediately claimable — the self-healing
  visibility-timeout (DESIGN §10.6). `acked` events remove the seq (it is also deleted from the
  jobs log via §3.5); `released`/expiry remove the lease and push the seq to `reclaim`.
- **Reclaim freelist** (`reclaim`): a min-heap of seqs whose lease expired or whose nack
  `delay_ms` has elapsed. A claim pass **drains this first** (reclaimed work jumps ahead of
  never-delivered work, bounding redelivery latency) before advancing `claim_cursor`. Lease
  expiry is **lazy** — no per-job timers; a claim pass sweeps `delayed` (whose `ready_at` ≤ now,
  via the Clock) and any `leases` past `deadline_ms` into `reclaim`.
- **Claim cursor** (`claim_cursor`): a monotonic pointer over the jobs log handing out the next
  never-yet-leased seq once the freelist is empty — the fresh-job source.

**Coalescing claim pass.** With `claim_jitter_ms > 0`, claimers (poll claims and `/work` SSE
streams alike) are gathered into a cohort over a Clock-driven window, then served in **one
batched coordinator pass under the queue lock** that divides the available set (`reclaim` ++
fresh-from-cursor) **evenly** across the cohort (round-robin proportional to each `max`). One
critical section per cohort replaces N per-claim atomic races on the queue head — fewer
contended atomics, predictable fairness (DESIGN §10.3). `claim_jitter_ms = 0` serves each claim
immediately. All windows/deadlines use the **Clock trait** so `TestClock` drives lease expiry,
the jitter window, and delayed nacks deterministically — no wall-clock sleep is load-bearing.

The queue state lives under the box's existing per-box lock (DESIGN §10 transitions are rare
relative to the read hot path); ack reuses the permanent-delete path (§3.5), so an acked job's
storage is reclaimed exactly like any deleted record. Dead-lettering (DESIGN §10.7) is an
internal append into the `dead_letter` box plus a permanent delete from the jobs log — no new
storage mechanism.
