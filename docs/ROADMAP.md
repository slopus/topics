# streams — Roadmap

Four build phases, each with crisp acceptance criteria, plus the benchmark plan. The guiding rule:
**the API and its semantics are fixed in phase 1 and never change**; later phases add persistence,
tests, and scalability *underneath* an unchanging contract. See [API.md](API.md),
[DESIGN.md](DESIGN.md), [ARCHITECTURE.md](ARCHITECTURE.md).

---

## Phase 1 — Define API + docs (this repository)

Produce the complete, internally consistent specification the implementation must satisfy.

**Deliverables**
- `README.md` — pitch, mental model, quickstart, use-case recipes, feature list, phases, doc links.
- `docs/API.md` — the complete `/v0` HTTP reference (the contract).
- `docs/DESIGN.md` — data model & semantics (seq, dual watermark, tombstones, permanent deletion,
  node loop-prevention, routers, priority).
- `docs/ARCHITECTURE.md` — storage, WAL, group commit, segments, recovery, scheduler, concurrency,
  crates, latency budget.
- `docs/ROADMAP.md` — this file.

**Acceptance criteria**
- [ ] Every endpoint in the §9 index of API.md has a documented method, path, request, response, and
      error set.
- [ ] Field names, config keys, defaults, and status codes are **identical** across all four docs
      (one vocabulary: `cap_records`/`cap_bytes`/`discard`/`durable`/`priority`+`auto_priority`,
      `from_seq`/`next_from_seq`/`head_seq`/`earliest_seq`/`evict_floor`, deletion via
      `before_seq`/`match` (`Eq`/`Glob`), deletion described as permanent/async/silent/point-in-time,
      tombstone `reason ∈ {cap, ttl, mixed, recreated, from_seq_too_old}`, `meta` for record headers,
      `$`-prefixed server fields).
- [ ] The three use-case recipes (job queue / pub/sub / strong delivery) are expressible purely with
      documented endpoints.
- [ ] The seven safety invariants (DESIGN §9) are stated and traceable to specific mechanisms.

---

## Phase 2 — Simplest possible server (in-memory, no WAL)

A correct, **complete** implementation of the entire `/v0` API with all data in memory. Not
persistent, not yet scalable — but every endpoint, every semantic, and every error path works.

**Scope**
- Full HTTP surface (axum/hyper): boxes CRUD, write, diff, delete (`before_seq`/`match`), routers
  CRUD, SSE (POST-create + GET-stream), health/ready/metrics.
- In-memory `BoxIndex` (base+offset vector) + per-box tag index, dual floor
  (`earliest_seq`/`evict_floor`) + `epoch` atomics.
- Cap (`cap_records`/`cap_bytes`) + TTL eviction advancing `evict_floor` with in-band tombstones.
- `discard: "old" | "reject"` full-box policy (`422 box_full`).
- Permanent deletion via `POST .../delete` (`before_seq` snapshot, tag `match` exact + `tag*` prefix,
  combined): point-in-time, silent (no tombstone), effective immediately on reads, `count`/`bytes`/
  `earliest_seq` updated synchronously, lazy front-reclaim of dead slots. Node loop-prevention,
  cursor-advance reads.
- Routers: at-least-once forwarding, per-source FIFO, DAG cycle check (`409`), `allow_cycle` hop cap.
- Multiplexed SSE: named events (`record`, `tombstone`, `caught-up`, `box-deleted`, `error`),
  composite `id:` cursors, heartbeats, `retry:`, resume via `Last-Event-ID`.
- Idempotency keys, `performance` blocks, error envelope, auth bearer.
- Priority scheduler present in simplified form (banded ready-set + recency); no fsync to gate.

**Explicitly out of scope (phase 4):** WAL, fsync/durability gating, segments, restart recovery,
metadata snapshots, elastic throttling under real CPU pressure. `durable: true` is *accepted* but is
a no-op fast path in phase 2 (documented).

**Acceptance criteria**
- [ ] A conformance test suite drives every endpoint and asserts the documented JSON shapes and
      status codes.
- [ ] Tombstone behavior verified: a consumer whose `from_seq + 1` falls below `evict_floor` after
      cap eviction and after TTL expiry receives the correct `reason`, `gap_from`, `gap_to`; a
      consumer whose cursor fell into a purely-**deleted** gap (below `earliest_seq` but at/above
      `evict_floor`) receives **no** tombstone (`tombstone: null`) and the cursor advances silently.
- [ ] Node loop-prevention verified: a node never receives its own records via `diff` or `watch`, and
      the cursor advances past them (`caught_up` reached, not an infinite empty loop).
- [ ] Deletion verified: `before_seq` (snapshot), tag `match` exact, and `tag*` prefix remove the
      matching records present at call time from all subsequent reads and SSE; `count`/`bytes`/
      `earliest_seq` update immediately; the delete is **point-in-time** (a later record with the
      same tag is NOT deleted); it is **permanent** (no un-delete) and **silent** (no tombstone for
      the deleted seqs); cap/TTL eviction STILL emits a tombstone (deletion never touches
      `evict_floor`).
- [ ] Router fan-out verified: a write to `src` appears in all `dst` boxes with `$node` preserved; a
      cycle-creating router is rejected `409`; an `allow_cycle` mirror terminates via the hop cap.
- [ ] SSE verified: multi-box stream delivers `record`/`tombstone`/`caught-up`/heartbeat frames; a
      reconnect with `Last-Event-ID` resumes all per-box cursors atomically.
- [ ] Server starts, serves, and shuts down cleanly; restart loses all data (expected, documented).

---

## Phase 3 — Maximum-coverage tests + benchmarks (baseline)

Lock in correctness and record initial performance numbers against the phase-2 server.

**Scope**
- Unit tests for every module (index, eviction, deletion + tag index, scheduler, SSE framing,
  cursor math).
- Property/fuzz tests for the seq/dual-watermark/tombstone invariants over randomized
  write/evict/expire/delete/read sequences (e.g. "a consumer reading from any `from_seq` either sees
  a strictly-increasing stream with the deleted/expired/own-node seqs silently skipped, or exactly
  one tombstone iff its cursor fell below `evict_floor` — never silent involuntary loss, and never a
  tombstone for a purely-deleted gap").
- Integration tests for the use-case recipes end to end.
- A criterion-based benchmark suite (see Benchmark plan below).
- `docs/BENCHMARKS.md` recording the **initial baseline numbers** (in-memory phase-2), with hardware
  and methodology noted.

**Acceptance criteria**
- [ ] Line/branch coverage target met on the core engine modules (goal: ≥ 90% on
      index/eviction/deletion/scheduler).
- [ ] Invariant property tests pass over randomized write/evict/expire/delete/read sequences.
- [ ] The benchmark suite runs reproducibly and `docs/BENCHMARKS.md` contains baseline figures for
      every metric listed below.
- [ ] No test depends on wall-clock sleeps for correctness (use injected clocks for TTL/priority).

---

## Phase 4 — Make it scalable (WAL, durability, segments, scheduler, throttling)

Add persistence and scale underneath the unchanged API, staying a single restartable process.

**Scope**
- WAL: framing (§ARCHITECTURE 2.1), **sharded writers** (`STREAMS_WAL_SHARDS`, default
  `min(num_cpus, 8)`; each shard an ordered writer with its own file set), adaptive group commit,
  per-box durable fsync, preallocation, rotation; shard-count-agnostic recovery.
- Compactor: WAL→segment checkpointing; segment `.data`/`.idx`; mmap serving of sealed segments,
  buffered `pread` of the active one, WAL-direct serving of the newest records.
- Segment-granular lazy cap/TTL eviction + persisted `EvictWatermark` (advances `evict_floor`).
- Async deletion (background, ARCHITECTURE §3.5), **no compaction / no per-record reclaim**: a
  delete flips an **in-place delete-flag byte** in segment files (the WAL stays append-only — a
  `Delete` frame is appended, never mutated); a **whole segment is dropped** only when a delete
  clears it entirely. There is no partial-segment rewrite and no general reclaim of marked records.
  `Delete` control frames replay deterministically (idempotent across crashes).
- Metadata store: WAL control frames + atomic bincode snapshots; interned `box_id`s.
- Restart recovery: snapshot load → segment `.idx` bulk load + tag-index rebuild → WAL replay
  (**all shards by box_id**) from last checkpoint (incl. `Delete` frames) → **XXH3-64** torn-tail
  truncation → idempotent segment reclaim.
- Full priority scheduler: banded DWRR + aging; governor-driven elastic throttling
  (coalesce → widen group commit → defer low priority → `429`).
- Slow-consumer isolation for SSE (bounded channels, lagged-degrade-to-tombstone).

**Acceptance criteria**
- [ ] **Durability:** for `durable: true` boxes, an acked write survives a hard kill (`SIGKILL`) at
      any instant and is present after restart; no acked durable write is ever lost.
- [ ] **Crash consistency:** a kill during a write leaves the WAL recoverable — recovery truncates the
      torn tail (XXH3-64 checksum / length), and no partial frame is ever interpreted as data.
- [ ] **Recovery correctness:** after restart, `head_seq`/`earliest_seq`/`evict_floor`/`count`,
      config, routers, and the set of deleted records match the pre-crash state (modulo un-fsynced
      non-durable tail); previously-deleted records stay gone after replay of their `Delete` frames.
- [ ] **No silent loss across restart:** a consumer whose cursor fell below the recovered
      `evict_floor` receives a tombstone, never silent skip; a cursor in a purely-deleted gap stays
      silent.
- [ ] **Eviction is segment-granular; deletion is no-compaction / no-reclaim:** physical occupancy
      may transiently exceed cap by ≤ one segment, and deleted records **stay on disk just marked**
      (only whole cleared segments are dropped — no per-record reclaim); `earliest_seq`/`count`/
      `bytes` always report the live logical floor regardless.
- [ ] **Latency target met:** non-durable / `eventual` SSE delivery p99 ≤ 5 ms at a defined sustained
      load (see benchmark plan); durable write-ack p99 within budget with adaptive group commit.
- [ ] **Elastic throttling:** under induced CPU pressure, high-priority boxes keep their latency while
      low-priority boxes degrade visibly (`429` / SSE `error` frames), with **zero** data loss
      attributable to throttling.
- [ ] **No regressions:** the full phase-3 test suite still passes; `docs/BENCHMARKS.md` is updated
      with phase-4 numbers alongside the phase-2 baseline.

---

## Benchmark plan

Measured with criterion (micro) + a load harness (macro). Each metric is recorded for the in-memory
baseline (phase 3) and re-recorded for the persistent build (phase 4), so the cost of durability is
explicit. Results land in `docs/BENCHMARKS.md` with hardware (CPU model, NVMe model), OS, build
flags (`--release`), and methodology.

| Metric | What | How |
|---|---|---|
| **Write throughput** | records/s and MB/s appended | sustained `POST` batches at varied batch sizes (1, 10, 100, 1000) and payload sizes (64 B, 1 KiB, 64 KiB); durable vs non-durable. |
| **Append latency p50/p99/p999** | end-to-end ack latency | single-record and batched writes, durable (fsync-gated, group-committed) vs non-durable; report the group-commit batch-size distribution. |
| **getDifference throughput** | records/s served | replay reads at varied `limit` (1, 256, 1000) from cold (mmap fault) and warm (page cache) segments; with and without deleted/node-skipped records in the scanned range. |
| **getDifference latency p50/p99** | per-call latency | tail reads (caught-up, near head) vs deep replay (cold segments). |
| **SSE fan-out latency** | write→deliver p50/p99 | 1 writer, N watchers (1, 10, 100, 1000) on one box; measure per-watcher delivery latency; verify the 1–5 ms target for `eventual`. |
| **SSE fan-out scale** | max watchers / connection churn | connection setup cost, heartbeat overhead, memory per idle connection. |
| **Router forwarding** | added latency + throughput | src→dst delivery latency vs direct write; fan-out to N dests; chain depth cost. |
| **Eviction / TTL cost** | impact on write path | sustained writes against a small cap and short TTL; confirm segment-granular drop, measure write-latency impact and tombstone-emission rate. |
| **Recovery time** | time to ready after restart | WAL replay + segment `.idx` load for boxes holding 10⁶ / 10⁷ / 10⁸ records; report seconds-to-`ready` vs data size. |
| **Throttling behavior** | latency under pressure | drive CPU saturation; chart high- vs low-priority box latency and `429` rate; assert zero loss. |
| **Memory footprint** | bytes/record resident | index + buffers per retained record at varied payload sizes; confirm the base+offset index overhead. |

**Baseline doc:** `docs/BENCHMARKS.md` (created in phase 3). It records the phase-2/3 in-memory
numbers first; phase 4 appends a persistent-build column and a short analysis of the durability and
recovery costs.

---

## Open questions (carried into implementation)

- **Tombstone placement in `diff`:** chosen — a dedicated top-level `tombstone` field (not inline in
  `records`), so consumers branch cleanly. (Resolved; documented in API §3.3.)
- **Cursor epoch encoding:** recommended yes — include an opaque `epoch` so delete+recreate is
  detected exactly rather than heuristically (DESIGN §5.5). Whether to expose the epoch in
  `next_from_seq`/SSE `id:` or keep it server-side is an implementation detail to settle in phase 4.
- **Delete un-delete:** **Resolved — deletion is permanent by design.** Deleted records are
  logically gone instantly but **stay on disk just marked** — there is no compaction and no
  per-record reclaim (only a whole cleared segment is dropped) — and there is no un-delete in `/v0`;
  to restore a value, write a new record. (This supersedes the earlier read-time-filter model, where
  removal was a reversible filter.)
- **Per-message explicit ack / lease / heartbeat (BullMQ stalled-job mode):** **Resolved —
  *implemented* as the `queue` box type** (`type:"queue"`): claim/ack/nack/extend + the `/work`
  auto-claim SSE stream, visibility-timeout leases, redelivery, capped-redelivery → dead-letter, and
  optional `lease_id` fencing (validate-when-supplied). See API §10 / DESIGN §10.
- **Compacted box type (Kafka log compaction, last-record-per-key):** **Out of scope** — LSM / keyed
  compaction is not implemented and not planned. The last-record-per-key pattern is built at the
  application level with a tag + a point-in-time `match` delete of prior versions.
- **Durable consumer groups as a server primitive:** **Out of scope** — they are an application-level
  pattern (a box per consumer + delete-as-ack), not a built-in server feature.
- **Multi-server / replication / HA / single-writer fencing, native TLS, hard multi-tenancy:**
  **Out of scope** — streams is single-server (TLS terminates at a reverse proxy; tenancy is per-key
  scopes + box-name-prefix allowlists).
- **Auto-priority constants** (`AUTO_MAX=500`, `HALF_LIFE_MS=30000`) and **band weights/boundaries**
  are starting defaults; phase 3/4 benchmarks may retune them. The formula and knobs are stable; the
  numbers are tunable.
