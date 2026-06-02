# Async + Derived Router Forwarding — Design Note

Status: SHIPPED DEFAULT (IMPLEMENTED + HARDENED; `TOPICS_FORWARD_V2` default ON).
The async + derived forwarding path is fully wired, tested, the codex review findings
are fixed, and it is now THE default forwarding model. The legacy synchronous
`forward_from` path remains available as an explicit opt-out via `TOPICS_FORWARD_V2=0`
(`false`/`no`/`off`). The flag is captured per-engine at construction
(`Engine::forward_v2`).

What Stage 4 fixed (codex findings):
- **P0 #1 — snapshot ⇄ cursor/dest atomicity.** A new engine `router_snapshot_lock`
  (`RwLock`): `advance_router` holds it SHARED for its whole pass (dest publish +
  cursor advance are one unit); snapshot `capture` holds it EXCLUSIVE across BOTH
  topic capture and router-cursor capture, so the captured (derived dest, cursor) pair
  is one consistent checkpoint unit (no duplicate re-forward, no silent loss across a
  snapshot). Verified by `snapshot_cursor_and_dest_stay_consistent_under_concurrency`.
- **P0 #3 — durable cursor under WAL sharding.** The `RouterCreate` WAL frame now
  carries `initial_cursor` + `initial_dest_base` (trailing, back-compat); recovery
  seeds the cursor from the LOGGED value, not the live source head at replay time, so
  it is independent of cross-shard replay order. Verified by
  `router_create_cursor_is_durable_without_snapshot`.
- **P0 #2 / #4 — single-OWNER derived dest.** `put_router` rejects multi-source
  fan-in into one derived dest (`409 router_dest_fan_in`); see §4.1 (the over-strict
  "no direct writes / no cycle" variant was dropped — it broke the `/v0`
  `allow_cycle` contract — and the residual mixed-topic recovery limitation is
  documented there).
- **P1 #5 — durable source-trim floor.** `SnapshotTopic.source_trim_floor` is captured
  + restored so a previously-surfaced `source_trim` tombstone never degrades into a
  silent gap after restart.
- **P1 #6 — retention before forward.** `advance_router` now runs
  `enforce_retention_durable(source)` BEFORE reading the source head / evict floor,
  so a TTL-expired record is never forwarded past an un-hardened floor.
- **P1 #7 — ordered materialization (R6).** A per-topic `MaterializeSeam` feeds
  published ranges to the `SegmentWriter` strictly by seq, so the off-gate seal
  (R6) can never materialize `N+1` before `N` (which would trip the writer's
  contiguity assert / seal a phantom gap).

What landed in Stage 2 (vs. the Stage-1 skeleton below):
- `Engine::advance_router` / `catch_up_dest` / `drain_router_sources` are
  implemented (cursor-driven, per-router-mutex-serialized). The write/ack path no
  longer forwards under `forward_v2` — it only marks the source dirty.
- Derived dest appends go through `Engine::derived_append` which writes **NO WAL
  frame** (the no-amplification win: one source append fanning to N dests is ONE
  WAL append). Verified: `tests/integration_forward_v2.rs` asserts the WAL frame
  delta == 1 for a 1→5 fan-out (ratio 0.200 writes/dest).
- No silent loss (R2 fix): the cursor advances over the contiguous
  forwarded|filtered|dead prefix and STOPS at the first back-pressured record,
  which is retried when the dest drains (`forwarded_total` pins committed count).
- Deterministic dest seqs: `RouterGraph` carries `dest_base` (durable via the
  snapshot, `SnapshotRouter.dest_base`); the dest re-materializes with identical
  seqs across a restart (`reforward_routers_on_recovery` replays from the cursor).
- Source-retention bound: a source trimmed below a cursor stamps `source_trim`
  deleted holes on the dest + advances a new `Floors.source_trim_floor`, surfacing
  a `TombstoneReason::SourceTrim` (never a silent gap).
- Cycle/hop termination: each derived record carries `StoredRecord.hops`
  (source.hops + 1); a record at `MAX_ROUTER_HOPS` is consumed but not re-forwarded.
- R6: `TopicState::publish_staged_no_seal` advances head + releases the publish gate;
  the segment seal/fsync (`materialize_published`) runs OFF the gate. Verified by
  `tests/integration_seal_offgate.rs` (a blocked seal does not stall same-topic
  visibility).
- A background router worker runs in `main.rs` (elastic tick) so forwarding
  progresses with no dest reader; a final drain runs before the shutdown snapshot.

---

(Original Stage-1 design follows.)

Status: STAGE 1 (design + skeleton). The build stays green; the live forwarding
path is unchanged until later stages flip the feature behind `forward_v2`.

## 0. Problem (today)

`Engine::forward_from` (src/engine/mod.rs) runs **synchronously on the source
write/ack path**: each fan-out destination gets its own inline `durable_append`,
which is a full WAL-first append. So:

- **Amplification (R1/R12):** one source write fanning to N dests does ~N WAL
  appends (the source's own append + one per dest), recursing on chains. A 50k
  fan-out is 50k WAL writes blocking one ack.
- **Seal serialization (R6):** `publish_staged` → `materialize_segment` →
  `SegmentWriter::seal_active` → `SegmentStore::put` (fsync) runs **under the
  per-topic publish-gate turn**, so a slow seal fsync serializes same-topic writers.
- **Silent loss (R2):** `forward_from` advances the per-router cursor to the
  *current source head* (`note_forwarded(&r.name, src_head, count)`, line ~2317)
  **regardless** of records that were filtered out, back-pressured
  (`discard:"reject"` full dest), or rejected (WAL error → `continue`). Nothing
  re-drives them: the cursor jumped past them. They are permanently dropped.

## 1. Target model

```
write(src) ──► src topic + ONE WAL append ──► return (no inline forwarding)
                    │
                    └─ mark src dirty (sched_dirty)  ┐
                                                     ├─► router worker (async, elastic)
   read(dest) ──► catch-up routers feeding dest ─────┘     for each router on src:
                                                              replay src[cursor..head]
                                                              ▸ filter ▸ build dest recs
                                                              ▸ durable_append(dest)   (commits)
                                                              ▸ advance cursor by #committed
```

Two truths only are durable: **(a) the source WAL** (source topics + their
records) and **(b) the router definitions + a durable per-router cursor**
(persisted via snapshot/checkpoint). Forwarded dest records are **derived** —
NOT separately WAL-logged — so one source append fanning to N dests is **one**
WAL append + the periodic cursor, never N (no amplification).

### 1.1 Why a *read-path catch-up* (the hard constraint)

`tests/integration_routers.rs` and the `topics-probe` conformance both write to
`source` and then **immediately** (no sleep, same connection) `diff` the dest,
asserting the forwarded copy is already visible. A purely time-driven background
worker would make that racy. So forwarding is **cursor-driven** and runnable from
**two drivers** over the same idempotent step (`advance_router(name)`):

1. **Read-path (synchronous, read-your-writes):** a `diff`/SSE/`GET state`/queue
   read of topic `D` first drains every router whose `dest == D` up to the source
   head. This is what keeps the existing no-sleep tests green: by the time the
   dest read serves, its feeding routers are caught up.
2. **Background worker (async, elastic):** a maintenance task (mirrors the
   existing snapshotter in src/main.rs) drains dirty router sources off the
   write path, so progress happens even with no dest reader and so a write ack
   never pays for forwarding. Woken by the per-topic `sched_dirty` hook.

Both call the same `advance_router` under a per-router mutex, so they never
double-forward and the cursor is the single source of truth for progress. (The
read-path catch-up is what preserves the *observable* synchronous semantics
while the *ack path* becomes async — the ack no longer forwards.)

## 2. The router worker (wake / throttle / ordering / backpressure)

### Wake / throttle
- Reuse `TopicState::sched_dirty` + `Scheduler::mark_dirty_fast`. A source write
  marks its topic dirty (already done, line ~2160). The worker loop calls
  `scheduler.drain_order_clearing(...)` (already exists, currently unused) to get
  dirty topic names in priority-band order, clearing each flag, and runs
  `advance_router` for every router whose `source` is a drained topic.
- Elastic: an interval tick (`ROUTER_TICK_INTERVAL_MS`, new const) bounds the
  worst-case latency; a Notify/condvar woken on `mark_dirty` gives low-latency
  wake. Each pass forwards at most `ROUTER_BATCH` source records per router, then
  re-marks the source dirty if it is still behind (cooperative yielding — a 50k
  fan-out never monopolizes the worker or a single lock).

### Ordering (per-source FIFO, at-least-once)
- `advance_router(name)` replays `source[cursor+1 ..= head]` strictly in seq
  order, in bounded chunks, so per-source FIFO into each dest is preserved.
- Idempotency: cursor advances **only by the number of records actually
  committed into the dest**, so a crash/retry re-runs from the un-advanced cursor
  (at-least-once). Two drivers are serialized by a per-router `Mutex` so the same
  source records are never forwarded twice concurrently.
- Chains (A→B→C): forwarding into B marks B dirty (existing `mark_dirty_fast` on
  the dest), so B's routers are drained on the next pass / by B's reader. The hop
  cap is no longer a recursion depth; it falls out of the chain because the
  derived cursor on each edge advances independently. Cycle detection
  (`RouterGraph::would_create_cycle`) and `allow_cycle` hop-cap loop-breaking are
  unchanged at the graph level; the per-edge cursor naturally terminates a cycle
  once every source record has been forwarded once per edge.

### Backpressure (the R2 fix)
- Per source record, the forward outcome is one of: **forwarded** (committed to
  dest), **filtered** (router filter dropped it — counts as *consumed*, the
  cursor may pass it), or **deferred** (dest `discard:"reject"` full, or a WAL
  error). The cursor advances over a **contiguous prefix** of `forwarded |
  filtered` records and **stops at the first deferred record**. The deferred
  record stays below the cursor and is retried on the next pass (when the dest
  drains or the WAL recovers). Nothing is silently skipped.
- A filtered record advancing the cursor is correct: it is genuinely not destined
  for this dest, it is not "lost", and the source retains it. (A filter is a
  consume, not a drop-with-loss.)

`$node` passthrough, `preserve_node`/`preserve_tag`, and the filter are applied
exactly as today (the per-record transform in `forward_from` moves verbatim into
`advance_router`).

## 3. Durable per-router cursor

- **Lives where it already lives:** `RouterGraph.cursors: HashMap<String, u64>`
  (source seq forwarded through) + `forwarded_total`. Accessors `note_forwarded`,
  `snapshot_all`, `restore` already exist.
- **Checkpointed:** `SnapshotRouter { forward_cursor, forwarded_total }` is
  already captured by `snapshot::capture` and restored by `engine_snapshot::
  restore` — no schema change. The cursor is durable **only** at snapshot
  cadence; between snapshots it is reconstructed from the source WAL.
- **Recovered:** on a snapshot restore the cursor is the snapshot value. With **no
  snapshot** (or a router created after the last snapshot), the cursor is seeded
  to the recovered source head **only for an empty dest** — but the *derived
  re-materialization* (§4) re-runs forwarding from the cursor regardless, so a
  dest is rebuilt from `source[cursor..head]`. This is the key change from
  today's `note_forwarded(.., 0)` (which set the cursor to source head and never
  re-forwarded).

### Cursor crash-safety vs. the dest commit (no silent loss across restart)
- The cursor MUST advance only after the dest append **commits**. Because the
  dest record is derived (not WAL-logged), a crash between "dest committed" and
  "cursor snapshotted" simply re-forwards those records on restart — they are
  re-derived with the **same deterministic dest seqs** (§4), so a consumer cursor
  into the dest stays valid (at-least-once, no gap, no shift). A crash *before*
  the dest commit leaves the cursor un-advanced; the records are re-forwarded.
  Either way: no silent loss.

## 4. Deterministic dest-seq re-materialization

The recovery contract: a consumer holding a cursor into a **dest** topic must stay
valid across a restart. So a re-derived dest record MUST get the **same seq** it
had pre-crash, independent of replay timing/order.

### Scheme
A dest record's seq is a deterministic function of the **router edge** and the
**source seq**, not of wall-clock forwarding order:

```
dest_seq(router r, source_seq s) = r.dest_base + (number of source records in
                                   (r.first_forwarded_src .. s] that PASS r.filter)
```

Concretely, the router carries two durable scalars beside its cursor:
- `dest_base: u64` — the dest seq just below this router's first forwarded
  record (captured when the router starts forwarding into a dest), and
- the cursor `c` (source seq forwarded through) + `forwarded_total` (count
  committed), which together pin the **next** dest seq = `dest_base +
  forwarded_total + 1`.

Because forwarding is **strictly in source-seq order** and advances the cursor by
the exact committed count, replaying `source[cursor..head]` through the same
filter assigns the **same** dest seqs in the same order on every run — the dest
is a pure function of `(source records, router definition, dest_base, cursor)`.

### Constraint: single derived OWNER per dest (enforced + documented)
Determinism holds iff a derived dest's seq stream is owned by **one** producer.
We therefore **enforce** (validation in `put_router`): a topic may not be the dest of
a **second router with a different source** (multi-source fan-in) while `forward_v2`
is on — two independent derived seq streams cannot share a dest, because the
re-forward replay order across a restart is not pinned, so their interleaving (and
hence the assigned dest seqs) would differ. A violation is a `409
topic_exists_incompatible {reason:"router_dest_fan_in"}` at definition time, never a
silent seq collision. (An idempotent re-PUT of the SAME router, or re-pointing it,
keeps single ownership and is allowed.)

**What is NOT forbidden (a deliberate scope decision — codex Stage 4):** a topic may
be BOTH a router dest AND a direct-write topic, and may close a cycle (`A→B→A` with
`allow_cycle:true`). The `/v0` contract REQUIRES this (the `allow_cycle` conformance
case writes directly into a topic that is also a router dest), so it cannot be
rejected. The original "a derived dest may not also take direct writes" rule was too
strict and broke the contract; only genuine multi-source fan-in is refused.

#### Known limitation: a MIXED direct-write + derived topic is best-effort on recovery
A topic that receives BOTH direct writes (each a WAL `Append` frame) AND derived
forwards (no WAL frame) — e.g. an `allow_cycle` loop, or a topic used as both a
direct-write target and a router dest — has an **interleaved** seq stream where some
seqs are WAL-logged and some are not. Recovery replays the WAL `Append` frames in
strict per-topic seq contiguity (`seq == head + 1`), so a direct-write frame whose seq
sits ABOVE a derived (un-logged) seq is dropped by the contiguity guard (its seq is
not `head + 1`). The derived tail is then re-materialized by
`reforward_routers_on_recovery`, but the **interleaving order is not guaranteed
identical** to the pre-crash assignment. Consequence: for a mixed topic, a consumer
cursor may not stay byte-exact across a crash. This is acceptable because: (a) the
amplification + no-silent-loss + async-ack goals do NOT depend on it; (b) the
*normal* derived-router shape — a **dedicated** dest topic fed by exactly one router,
with NO direct writes — re-materializes deterministically (verified:
`integration_forward_v2::dest_rematerializes_deterministically_across_restart` and
the kill-9 live test); (c) a mixed topic is an advanced topology, and the alternative
(forbidding it) would break the `/v0` `allow_cycle` contract. A user wanting a
crash-exact dest should use a dedicated single-router dest (the common case).

### Source-retention bound (surfaced as a tombstone, never silent)
A derived dest record is bounded by the **source retained window**. If the source
**evicted/trimmed** records (TTL / byte-cap involuntary loss, tracked by
`evict_earliest_seq`) below a router's cursor before the router forwarded them,
those records **cannot be re-derived**. On recovery the router's cursor is
clamped up to `max(cursor, source.evict_earliest - 1)`; the un-derivable gap is
surfaced to dest consumers as a **`tombstone` with `reason:"source_trim"`** (a
new `TombstoneReason`) on the dest's first diff across the gap — exactly the
existing involuntary-loss tombstone machinery (`eviction::build_tombstone`),
never a silent skip. This makes the dest faithfully reflect the source's
retention: independent durable retention *beyond* the source is not a
derived-router property.

## 5. R6 — background segment seal off the publish gate

Today: `publish_staged` (under the publish-gate turn) → `materialize_segment` →
`seal_active` → `SegmentStore::put` (fsync). A slow seal fsync serializes
same-topic writers behind the gate.

Plan:
- `publish_staged` advances `head_seq` (makes records visible), releases the
  publish gate (`ticket.done()`), and **only then** materializes/seals.
- Split `materialize_segment` into: (a) the in-index append into the active
  builder (cheap, still synchronous so reads of just-published records resolve
  from the writer cache), and (b) the **seal+fsync (`seal_active` → `put`)**,
  which moves to a **background seal task** (or is deferred to the maintenance
  worker / a per-topic seal queue). The gate is released before (b).
- **Crash-safety preserved:** the WAL already made the records durable; a segment
  is a derivable materialization (the existing `seal_active` comment confirms a
  `put` failure is *not* data loss — payloads stay resident and the range
  re-materializes on recovery). Payload eviction (`evict_resident` → free
  resident payloads) happens **only after** the seal `put` returns Ok, so a
  record is never freed before its sealed copy is fsynced — unchanged invariant,
  just moved off the gate. A pending (un-sealed) range on shutdown is force-sealed
  by the existing `flush()` path during snapshot/teardown.

This keeps the publish gate's job to **ordering only** (head advances in seq
order), with no fsync on it.

## 6. File-level change plan (exact functions/types)

| File | Change |
|---|---|
| `src/config.rs` | Add `ROUTER_TICK_INTERVAL_MS`, `ROUTER_BATCH` consts; a `forward_v2` runtime flag (env `TOPICS_FORWARD_V2`). Was default off in Stage 1; the cutover flipped it to **default ON** (`TOPICS_FORWARD_V2=0` is now the legacy opt-out). |
| `src/engine/router.rs` | `RouterGraph`: add `dest_base: HashMap<String,u64>` and per-router `Mutex` (or move per-router advance state into a small `RouterRuntime`). Add `dest_base(name)`, `set_dest_base`, `pending_deferred` bookkeeping. `note_forwarded` keeps advancing cursor+total; add `cursor(name)`. |
| `src/engine/mod.rs` | New `advance_router(&self, name)` (the idempotent forward step, per-router-mutex-guarded) carrying the per-record transform + filter + backpressure-prefix logic extracted from `forward_from`. New `catch_up_dest(&self, dest)` called at the top of `diff`/SSE/queue/`topic_state` GET read paths. New `drain_router_sources(&self)` for the worker. `write()`: when `forward_v2`, **drop** the inline `forward_from` call (lines ~2143-2154) — just `mark_dirty` the source. Keep `forward_from` as the v1 path under the flag so all tests stay green until cutover. |
| `src/engine/topic_state.rs` | `publish_staged`: advance head + notify, then materialize **after** the caller releases the gate; split out `seal_pending()` to run the seal/fsync off-gate. Add a per-topic `pending_seal` marker the maintenance worker / read path drains. |
| `src/engine/recovery.rs` | After topics + routers are rebuilt (step ~7, before opening writers), for each router replay `source[cursor..head]` through its filter and re-forward (derived re-materialization) instead of relying on replayed dest Append frames. Clamp cursor to `source.evict_earliest-1` and stamp the dest `source_trim` tombstone boundary when the source trimmed below the cursor. `apply_router_create_for_recovery`: seed `dest_base` deterministically. |
| `src/engine/snapshot.rs` / `src/storage/snapshot.rs` | `SnapshotRouter`: add `dest_base: u64` (defaulted for back-compat). `router_to_snapshot` / `restore` carry it. |
| `src/types.rs` | Add `TombstoneReason::SourceTrim` (+ wire string `"source_trim"`). |
| `src/main.rs` | Add a **router worker** task (mirrors the snapshotter): interval tick + `drain_router_sources`, on the blocking pool; `abort()` on shutdown after a final drain. |
| `src/engine/wal_glue.rs` / `src/storage/wal.rs` | **No new WAL record types** — forwarded dest records are derived, so nothing is logged for them (this is the no-amplification win). |

### Tests impact
- `tests/integration_routers.rs`, `topics-probe`: stay green because the
  read-path catch-up (`catch_up_dest`) makes a forward visible on the immediate
  dest read with no sleep.
- `tests/crash_recovery.rs` (the `mixed_durability...` test): already does NOT
  assert `fwd:1` is *absent* post-restart (it only `find`s `fwd:2`), so
  re-deriving `fwd:1` is compatible; the comment about "recovery does not
  re-forward history" is updated to the derived behavior.
- New unit/integration tests: backpressure-defer-then-retry (R2), no-amplification
  (assert WAL append count == source appends), deterministic dest seq across
  restart, source-trim tombstone, off-gate seal (R6).

### Fallback (if full derived recovery proves too risky for green)
Land **async + cursor-driven-no-silent-loss + compact-WAL (no amplification)**
solidly; if the deterministic dest-seq re-materialization on recovery cannot be
made test-green safely, keep recovery re-seeding the cursor to source head for an
**empty** dest and clearly document the derived-recovery limitation. The
**no-amplification (1 WAL write/fan-out)** and **no-silent-loss** goals are
non-negotiable and do not depend on the recovery re-materialization.
