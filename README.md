# streams

A persistent event engine in a single binary. **streams** is an append-only log
service exposed over a clean, JSON-first HTTP API: write events to named **boxes**,
read them back by sequence, fan them out with **routers**, and watch many boxes at
once over a single Server-Sent Events connection — all from one static binary on one
machine, backed by a write-ahead log on local NVMe. It is the persistence layer for
job queues, pub/sub, and durable event streams, with a design that makes data loss
**always explicit, never silent**.

> Status: **implemented and durable.** The full `/v0` API is built and tested
> (boxes, diff reads, deletes, routers, multiplexed SSE, and lease-based queues),
> backed by a write-ahead log with group commit, segments, snapshots, and
> crash-recovery replay on a single restartable process. A `durable` box's writes
> are **fsync-gated** — an acknowledged write is committed, and a restart replays
> the WAL to recover it. See [docs/](docs/) for the specification and the
> [roadmap](docs/ROADMAP.md) for how it was built.

---

## Mental model

Five concepts. That's the whole product.

- **Box** — a named, append-only log of records ordered by a monotonic `seq`. Think
  inbox/outbox. A box is created lazily on first write (or explicitly with config).
- **Record** — one immutable event in a box. The server assigns it a `$seq` (u64) and
  `$ts` (ms). It carries an opaque `data` payload, and optionally a `tag` (a match key
  for deletion), a `node` (origin id, for loop prevention), and small `meta`.
- **seq** — the cursor. Reads are "give me everything after `from_seq`." There is no
  opaque cursor token for box reads: the monotonic `seq` *is* the cursor. The client
  owns its position; advancing it is the ack.
- **Router** — a server-side forwarding rule `source → dest`. Every record appended to
  `source` is copied into `dest` **asynchronously, off the write/ack path**: forwarding
  is driven by a durable per-router cursor, so the source write acks immediately and the
  copy follows. Routers fan out, and because the origin `node` rides through untouched, N
  symmetric nodes can mirror to each other without echo or loops.
- **Tombstone** — the explicit "you missed data" signal. If records you wanted were
  evicted (cap) or expired (TTL) before you read them, the read returns an in-band
  tombstone with the exact `[gap_from, gap_to]` range — at HTTP 200, never silently.
- **Delete** — a permanent, asynchronous, point-in-time removal of records, by `seq`
  range and/or by `tag` match. A delete is **silent** (never a tombstone) and takes
  effect immediately on all reads. It only removes records that exist at call time — it
  is not a standing filter, so future records are never affected. There is **no
  compaction and no per-record disk reclaim**: a deleted record stays on disk, just
  marked. For a record already sealed into a segment, the delete is made durable **in
  place** by flipping a single delete-flag byte in the segment file (and fsyncing) while
  the WAL stays append-only (a `Delete` frame is appended, never mutated in place), so
  the deletion survives a checkpoint that trims the WAL `Delete` frame and is re-derived
  from the on-disk flag on restart. The only physical space released is a **whole
  segment dropped in one op** when a delete clears it entirely (crash-safe); individual
  deleted records inside a still-live segment are never reclaimed.

The load-bearing invariant: **involuntary capacity-driven loss you didn't ask for
(cap eviction, TTL expiry) always produces a tombstone; voluntary removal you did ask
for (permanent delete, your own node's events) is silently filtered.** Mixing those
would make the gap alarm useless. A delete advances the box's `earliest_seq` (first
live seq) but never its `evict_floor` (the cap/TTL tombstone trigger), so reading
across a purely-deleted gap is silent while reading below an evicted floor tombstones.

---

## Goals & constraints

What streams **aims** for — the design targets the implementation is built and tuned against:

- **Throughput: ~1,000,000 events/sec (batched).** Writes are batched (a single `POST`
  carries up to thousands of records) and the WAL is **sharded** (`STREAMS_WAL_SHARDS`,
  default `min(num_cpus, 8)`) into independent ordered writers, each with **adaptive group
  commit** — one `fsync` amortized across a whole batch of concurrent durable writers — so
  the per-event cost approaches the cost of a sequential disk append and durable throughput
  scales with shard count. Each box routes to exactly one shard by a stable hash of its id,
  so per-box ordering and every durability guarantee still hold; recovery is
  shard-count-agnostic (it replays all shards by `box_id`, so the shard count may change
  between restarts). Reads are `seq`-indexed (`O(1)` slot lookup) and SSE broadcasts
  serialize each record **once**, shared ref-counted across all watchers (a 1→N fan-out
  pays serialization once).
- **Latency: ~1 ms delivery / response.** On local NVMe, a `disk` write acks as soon as its
  WAL frame is enqueued to its box's shard writer (group-committed shortly after — no
  per-write fsync, the ack is not fsync-gated) and an `fsync` write acks after one group
  `fsync`; both target single-digit-millisecond p99. Under CPU pressure, delivery degrades in
  **latency, never in correctness** (the elastic scheduler), and a long-poll/SSE consumer is
  woken on append rather than polling.

The central tradeoff is the **three durability commit classes**, chosen per box, so each box
buys exactly the guarantee it needs without taxing the others:

- **`memory`** — _"disk-like but best-effort"_. Takes the **same** group-committed WAL write +
  recovery path as `disk` and is fully queryable, but with **no durability guarantee**: after a
  restart data **may survive or be lost** (recovery is gradual/best-effort — it never blocks
  readiness). Never fsync-gated. For caches / scratch / ephemeral fan-out where occasional loss
  is fine.
- **`disk`** — WAL + **group commit**, no per-write fsync. Survives a crash minus the
  un-fsynced tail. The pub/sub default.
- **`fsync`** — **fsync-gated** ack. Survives any crash; an acked write is always recovered.
  For queues / ledgers / anything that must not lose acknowledged work.

These are **not** global modes: a `memory` cache box, a `disk` pub/sub feed, and an `fsync`
job queue coexist in one process, and routers bridge them (forwarding honors the destination
box's class).

---

## Quickstart

Assume the binary is running on `localhost:4000` with auth disabled (dev mode).

### 1. Create a box (optional — first write auto-creates it)

```bash
curl -X PUT localhost:4000/v0/boxes/jobs \
  -H 'content-type: application/json' \
  -d '{ "durable": true, "cap_records": 1000000, "ttl_ms": 0 }'
```

```json
{ "box": "jobs", "created": true,
  "config": { "ttl_ms": 0, "cap_records": 1000000, "cap_bytes": 0,
              "discard": "old", "durable": true, "durability": "fsync", "priority": null,
              "auto_priority": true, "auto_create": true,
              "idempotency_window_ms": 120000, "dedupe_node": true },
  "performance": { "server_total_ms": 0.22 } }
```

### 2. Write records (server assigns the seqs)

```bash
curl -X POST localhost:4000/v0/boxes/jobs \
  -H 'content-type: application/json' \
  -d '{ "node": "worker-eu-1",
        "records": [
          { "data": { "url": "s3://b/a.png", "w": 256 }, "tag": "tenant42:job-9001" },
          { "data": { "url": "s3://b/b.png", "w": 512 }, "tag": "tenant42:job-9002" }
        ] }'
```

```json
{ "box": "jobs", "first_seq": 1, "last_seq": 2, "seqs": [1, 2],
  "head_seq": 2, "count": 2, "created": false, "deduped": false,
  "performance": { "server_total_ms": 0.62, "fsync_ms": 0.39 } }
```

### 3. Read current state (head, earliest, count, config)

```bash
curl localhost:4000/v0/boxes/jobs
```

```json
{ "box": "jobs", "head_seq": 2, "earliest_seq": 1, "next_seq": 3,
  "count": 2, "bytes": 184, "effective_priority": 500, "config": { "...": "..." },
  "performance": { "server_total_ms": 0.05 } }
```

### 4. Read the difference from a cursor (batched, with tombstones)

```bash
curl -X POST localhost:4000/v0/boxes/jobs/diff \
  -H 'content-type: application/json' \
  -d '{ "from_seq": 0, "limit": 500, "node": "worker-eu-1" }'
```

```json
{ "box": "jobs",
  "records": [
    { "$seq": 1, "$ts": 1748470000123, "$tag": "tenant42:job-9001",
      "data": { "url": "s3://b/a.png", "w": 256 } },
    { "$seq": 2, "$ts": 1748470000140, "$tag": "tenant42:job-9002",
      "data": { "url": "s3://b/b.png", "w": 512 } }
  ],
  "next_from_seq": 2, "head_seq": 2, "earliest_seq": 1,
  "caught_up": true, "tombstone": null, "lag": 0,
  "performance": { "server_total_ms": 0.30 } }
```

(Records written by `worker-eu-1` are filtered out when `worker-eu-1` reads — loop
prevention. The cursor still advances past them.)

### 5. Watch many boxes over one SSE stream

```bash
# Step 1: create the watch session (carries the full subscription)
curl -X POST localhost:4000/v0/watch \
  -H 'content-type: application/json' \
  -d '{ "node": "worker-eu-1",
        "boxes": { "jobs": { "from_seq": 0 }, "events": { "tail": true } } }'
# -> { "wid": "wid_BuRguGorNdVFWNQULz-rrw", "stream_url": "/v0/watch/wid_BuRguGorNdVFWNQULz-rrw", ... }
# The wid is an unguessable random capability that names the GET stream. In dev
# mode (no auth) the wid alone opens it. When auth is ON the GET also requires the
# creating key (Authorization header, or the dev-only ?token= on the SSE GET), so
# a leaked wid is not a credential; the stream can never exceed the creator's scope.

# Step 2: open the stream (EventSource-compatible)
curl -N localhost:4000/v0/watch/wid_BuRguGorNdVFWNQULz-rrw
```

```
retry: 2000

id: eyJqb2JzIjoxfQ
event: record
data: {"box":"jobs","records":[{"$seq":1,"$ts":1748470000123,"data":{"url":"s3://b/a.png"}}],"from_seq":0,"to_seq":1,"head_seq":2}

id: eyJqb2JzIjoyfQ
event: caught-up
data: {"box":"jobs","head_seq":2}

: hb 1748470015000
```

### 6. Delete records (permanent, point-in-time, by seq and/or tag)

```bash
# cancel one job (exact tag match — removes records present right now)
curl -X POST localhost:4000/v0/boxes/jobs/delete \
  -H 'content-type: application/json' \
  -d '{ "match": ["tag", "Eq", "tenant42:job-9001"] }'

# cancel an entire tenant (trailing-prefix match)
curl -X POST localhost:4000/v0/boxes/jobs/delete \
  -H 'content-type: application/json' \
  -d '{ "match": ["tag", "Glob", "tenant42:*"] }'
```

```json
{ "box": "jobs", "deleted": 1, "earliest_seq": 3, "head_seq": 2, "count": 0, "bytes": 0,
  "performance": { "server_total_ms": 0.12 } }
```

(Both records carried `tenant42:` tags, so after the two deletes the box is empty:
`count` 0 and `earliest_seq` = `head_seq + 1` = 3. `deleted` is the count removed by
*this* call.)

The delete is **permanent** (no un-delete), **silent** (no tombstone), takes effect
**immediately** on all reads, and is **point-in-time**: a `match`-only delete is
bounded by the current head, so a job enqueued a moment later by an in-flight producer
is *not* deleted. Three patterns:

```bash
# Snapshot / compaction: drop everything before a seq (e.g. after a checkpoint)
curl -X POST localhost:4000/v0/boxes/jobs/delete -d '{ "before_seq": 480000 }'

# Message update: publish v2, then delete the prior versions but keep the new one
curl -X POST localhost:4000/v0/boxes/chat/delete \
  -d '{ "match": ["tag", "Eq", "msg-123"], "before_seq": 5012 }'   # 5012 = seq of v2

# Chat revoke: a kicked user's whole sub-stream (prefix), point-in-time
curl -X POST localhost:4000/v0/boxes/chat/delete \
  -d '{ "match": ["tag", "Glob", "chat-42:*"] }'
```

---

## Running it

The build is a **durable single binary**: the complete `/v0` API backed by a
write-ahead log on local disk. On start it opens the data directory, loads the
latest snapshot, and **replays the WAL forward** (truncating any torn tail) before
serving — so an acknowledged durable write survives a restart. The readiness gate
(`GET /v0/ready`) returns `503` during replay and `200` once recovery completes.

```bash
# build the single binary
cargo build --release

# run it (defaults to 127.0.0.1:4000 loopback, auth disabled in dev mode;
# WAL + segments + snapshots live under ./streams-data)
./target/release/streams
```

### Running with Docker

A multi-arch image is published to GHCR at `ghcr.io/slopus/streams`. The image
binds `0.0.0.0:4000` and stores durable state in the `/data` volume. Because the
server refuses to start on a non-loopback bind with no API keys, pass
`STREAMS_API_KEYS=...` (or, for local/dev only, `STREAMS_ALLOW_INSECURE_NO_AUTH=1`):

```bash
docker run --rm -p 4000:4000 -v streams-data:/data \
  -e STREAMS_API_KEYS=replace-with-a-real-secret \
  ghcr.io/slopus/streams:latest
```

See [RELEASING.md](RELEASING.md) for the full run/release details.

Configuration is read from the environment:

| Variable | Default | Meaning |
|---|---|---|
| `STREAMS_HOST` | `127.0.0.1` | Bind host (loopback-only by default; may also be a full `host:port`). |
| `STREAMS_PORT` | `4000` | Listen port. |
| `STREAMS_API_KEYS` | _(unset)_ | Comma-separated bearer keys. **Hashed at rest** (SHA-256) and constant-time compared. Each entry may carry optional scopes + a box-name prefix allowlist: `key` \| `key:scopes` \| `key:scopes:prefixes` (§0.2). A bare `key` = full access. Unset ⇒ **auth disabled** (dev mode). |
| `STREAMS_ALLOW_INSECURE_NO_AUTH` | `0` | Required to start on a **non-loopback** bind with **no** keys — otherwise the server refuses to start (it would be an open, unauthenticated event store). |
| `STREAMS_PROBE_AUTH` | `0` | Require auth on the health/ready/metrics probes too (`/v0/health`, `/v0/ready`, `/v0/metrics`). Off ⇒ probes are unauthenticated. |
| `STREAMS_MAX_BODY_BYTES` | `67108864` (64 MiB) | Max total request body before parse; larger ⇒ `413`. |
| `STREAMS_DATA_DIR` | `./streams-data` | Directory for the WAL, segments, and snapshots. Replayed on start; a missing/empty dir is a fresh start. |
| `STREAMS_WAL_SHARDS` | `min(num_cpus, 8)` | Number of independent WAL shards (each its own writer thread / file set / group-commit). Each box maps to one shard by a stable hash of its id, so per-box ordering + durability still hold; recovery is shard-count-agnostic (replays all shards by `box_id`), so this may change between restarts. `1` = the flat single-writer layout. |
| `STREAMS_COLD_DIR` | _(unset)_ | Optional cold-tier directory. Set ⇒ sealed segments past the hot-retention bound relocate here off the hot path; unset ⇒ tiering disabled (everything stays hot). Cold reads never affect writes or delivery. |
| `STREAMS_SEGMENT_MAX_EVENTS` | `10000` | Seal (roll) the active segment after this many records. |
| `STREAMS_SEGMENT_MAX_BYTES` | `67108864` (64 MiB) | Also seal a segment after this many `.data` bytes. |
| `STREAMS_SEGMENT_MAX_AGE_MS` | `3600000` (1 h) | Also seal a partially-filled active segment after this wall-clock age. `0` disables the age trigger. |
| `STREAMS_HOT_RETAIN_SEGMENTS` | `4` | Keep at most this many most-recent sealed segments hot before relocating older ones to the cold tier (the active segment is always hot). |
| `STREAMS_HOT_RETAIN_BYTES` | `0` (count-only) | Optionally bound hot sealed-segment bytes; the stricter of the two retention bounds wins. |
| `STREAMS_FORWARD_V2` | `0` | Selects the async + derived router-forwarding path (durable per-router cursor, forwarded copies not WAL-logged, single-source-per-dest) described in [Routers](#features). Default `0` keeps the legacy synchronous in-line forward; set `1` to enable the cursor-driven async path. |
| `STREAMS_MAX_BOXES` | `100000` | Max boxes (DoS hardening). `0` = unlimited. Creating past it ⇒ `429 throttled`. |
| `STREAMS_MAX_ROUTERS` | `10000` | Max routers. `0` = unlimited. |
| `STREAMS_MAX_WATCH_SESSIONS` | `10000` | Max live watch sessions. `0` = unlimited. |
| `STREAMS_MAX_SSE_CONNECTIONS` | `10000` | Max concurrent SSE connections, server-wide. `0` = unlimited. |
| `STREAMS_MAX_SSE_CONNECTIONS_PER_KEY` | `1000` | Max concurrent SSE connections per api key. `0` = unlimited. |
| `STREAMS_MAX_INFLIGHT_PER_KEY` | `1000` | Max concurrent in-flight requests per api key. `0` = unlimited. |
| `STREAMS_MAX_TOTAL_BYTES` | `0` (unlimited) | Global quota on total retained record bytes across all boxes. A write past it ⇒ `429 throttled`. |
| `RUST_LOG` | `info` | Tracing filter. |

Durability is **per box**, in three commit classes (`durability`, §0.10) — the
durability/performance tradeoff:

| Class | Where it lands | Ack timing | Survives a crash? |
|---|---|---|---|
| `memory` | WAL, **group-committed** (same path as `disk`) | on WAL-frame enqueue, not fsync-gated (`fsync_ms` 0) | **best-effort — no guarantee.** Disk-like but lossy: records **MAY survive OR be lost** on restart (recovery is gradual/best-effort, never blocks readiness); the box is fully queryable and its CONFIG always survives. Lowest-latency, for caches/scratch. |
| `disk` | WAL, **group-committed** (no per-write fsync) | on WAL-frame enqueue, not fsync-gated (`fsync_ms` 0) | yes, **minus the un-fsynced tail** (the not-yet-group-committed frames). |
| `fsync` | WAL, **fsync-gated** | after the group `fsync` (real `fsync_ms`) | **yes, any crash** — an acked write is recovered by WAL replay. |

The legacy `durable` bool is a back-compat alias: `durable:true` ⇒ `fsync`,
`durable:false` ⇒ `disk` (set `durability:"memory"` explicitly for the
best-effort/lossy class). The resolved `durability` is reported on every box-state
response, and `durable` is normalized to `durable == (class == "fsync")`.
Router-forwarded and dead-lettered copies honor the **destination** box's class (a
`memory` dest persists a best-effort copy — it may survive or be lost). Regardless
of class, **an acknowledged write is published; a
write that fails to commit publishes nothing visible** (no readable-but-not-durable
state). The server shuts down gracefully on `SIGINT`/`SIGTERM`, writing a final
snapshot so a clean restart starts from a current checkpoint. The quickstart
commands above work verbatim.

### Security

- **Default bind is loopback** (`127.0.0.1:4000`), so an unconfigured server is never
  accidentally a public, unauthenticated event store. Binding a **non-loopback** address with
  **no** `STREAMS_API_KEYS` makes the server **refuse to start** unless you set
  `STREAMS_ALLOW_INSECURE_NO_AUTH=1` (it logs the reason loudly).
- **Bearer keys are hashed at rest** (SHA-256 — the plaintext is parsed once at startup, then
  **zeroized and dropped**; only the digest is kept in memory, and tokens are never logged) and
  **constant-time** compared with no early-exit.
- **Optional scopes + prefix allowlist** — a key may carry a scope set
  (`read`/`write`/`delete`/`admin`) and a box-name prefix allowlist, enforced per route; a key
  outside its scope/prefix gets `403 forbidden`. The prefix allowlist is enforced on **body**
  names too (the boxes a watch subscribes to; a router's `source`/`dest`), and **list** results
  are filtered to the allowlist, so a scoped key cannot watch, route into, or enumerate
  cross-tenant boxes. `/v0/metrics` requires the `read` scope. A bare `key` is full access
  (back-compat). A malformed scope token makes the server refuse to start (fail-closed). See
  `docs/API.md` §0.2.
- **Resource / rate limits (DoS hardening)** — configurable caps on boxes, routers, watch
  sessions, concurrent SSE connections (global + per-key), per-key in-flight requests, and a
  global total-bytes quota; plus a per-response byte budget, a length bound on queue `seqs`
  arrays, and idle watch-session GC. Past a cap ⇒ `429 throttled`. Sane defaults; `0` =
  unlimited. See §11.
- **Watch streams** are gated by an **unguessable 128-bit `wid` capability** minted by the
  authenticated `POST /v0/watch`, bound to the creating key **and** its scope. When auth is on,
  the GET stream requires the **creating key** (header or the dev-only `?token=`) — the `wid`
  alone is not a credential, so a leaked `wid` cannot be opened by a holder who lacks the key —
  and can never exceed the creator's scope. The `?token=` query fallback is accepted **only on
  the SSE stream GETs** and leaks via logs — prefer the `Authorization` header.
- **streams speaks plain HTTP** (no built-in TLS, by design). For any non-loopback exposure,
  run it behind a **TLS-terminating reverse proxy** (or bind loopback). Native TLS is **out of
  scope** — terminate it at the proxy. See `docs/API.md` §0.2 / §0.11.

### Running the tests

```bash
cargo test                              # default suite
cargo test --features test-fs           # + the fault-injection corpus (fake-disk crash sweeps)
cargo test --features test-fs,failpoints # + failpoint-driven faults
```

The default test run is the **fast** loop. Two things keep it to a few minutes
instead of ~30:

- **Tiered crash-point sweeps.** The crash/fault corpus replays a small durable
  workload and crashes after each mutating filesystem call, then recovers and
  asserts the oracle. By default each sweep probes a **bounded, deterministic
  sample** of crash points (always including both boundaries — crash-before-any-write
  and crash-after-the-last-write — plus an evenly-spread interior set), which
  exercises every boundary on every run. Timing-dependent assertions (heartbeat
  cadence, TTL/lease expiry) are driven by an injected `TestClock` or a short
  configurable interval, so there are no real-time waits for correctness.
- **Opt-in exhaustive matrix.** To replay the **full** `0..=M` crash matrix for
  every boundary, set `STREAMS_TEST_EXHAUSTIVE=1`:

  ```bash
  STREAMS_TEST_EXHAUSTIVE=1 cargo test --features test-fs,failpoints
  ```

  This runs nightly (and on demand) in CI — see `.github/workflows/ci.yml`'s
  `exhaustive-crash-matrix` job — so full crash-consistency coverage is always
  exercised even though it is not on the per-PR critical path.
  `STREAMS_TEST_SAMPLE=N` is a middle ground: a wider but still bounded sample.

Subprocess crash-recovery tests spawn the real binary on an **ephemeral port**
(`STREAMS_PORT=0`; the child reports its OS-assigned port via `STREAMS_PORT_FILE`),
so the suite runs at full `--test-threads` with no port-bind races.

---

## Use-case recipes

### Job queue (Bull-style)

```bash
curl -X PUT localhost:4000/v0/boxes/jobs -d '{ "durable": true, "cap_records": 0 }'
```
Producers `POST /v0/boxes/jobs`. Each worker calls
`POST /v0/boxes/jobs/diff {from_seq, node:"worker-N", limit:50}`, processes the batch,
then persists `next_from_seq` as its ack (cursor-advance = ack-all). Cancel a job with a
`match ["tag","Eq",jobid]` delete; cancel a tenant with a `match ["tag","Glob","tenant*"]`
delete (both permanent and point-in-time). Durable + unbounded cap means nothing is lost
to eviction; replay is just reading from an earlier `from_seq`.

For competing workers that need leases and redelivery rather than a shared cursor,
create the box with `"type": "queue"` instead and use claim/ack/nack/extend (or the
`/work` auto-claim SSE stream): jobs are leased with a visibility timeout, redelivered
if not acked, and optionally dead-lettered. See [docs/API.md](docs/API.md) §10.

### Pub/sub (Redis-style, weak guarantees)

```bash
curl -X PUT localhost:4000/v0/boxes/feed \
  -d '{ "ttl_ms": 5000, "cap_records": 10000, "discard": "old", "durable": false }'
curl -X PUT localhost:4000/v0/routers/feed-to-a -d '{ "source":"feed", "dest":"sub-a" }'
curl -X PUT localhost:4000/v0/routers/feed-to-b -d '{ "source":"feed", "dest":"sub-b" }'
```
Subscribers `POST /v0/watch` on `sub-a` / `sub-b` with `tail: true`. Small cap + TTL keep
memory bounded; subscribers tolerate gaps, which arrive as explicit `tombstone` frames.

### Strong delivery / replay

```bash
curl -X PUT localhost:4000/v0/boxes/ledger \
  -d '{ "durable": true, "cap_records": 0, "ttl_ms": 0, "discard": "reject" }'
```
Unbounded + durable + `discard:"reject"` means eviction is impossible and TTL is off, so
there is no tombstone source at all — guaranteed no silent loss. Consumers persist their
cursor and replay from the last acked `from_seq`. If a cap is ever configured and hit,
`discard:"reject"` fails the producer's write synchronously rather than dropping data.

---

## Features

- **Append-only boxes** with server-assigned monotonic `seq` and `ts`.
- **Batched diff reads** — bounded batches, a plain `seq` cursor, in-band tombstones.
- **Explicit gap detection** — cap eviction and TTL expiry crossing a cursor always
  yield a tombstone with the exact missed range and a `reason`.
- **Permanent deletion** — remove records by `seq` range (`before_seq`) and/or by tag
  (exact or `tag*` prefix), backed by a per-box tag index for efficiency. Permanent,
  silent (never a tombstone), effective immediately on reads, point-in-time (never
  affects future records). No compaction and no per-record reclaim: on disk a delete
  flips an in-place delete-flag byte in segment files (the WAL stays append-only); a
  whole segment is dropped only when a delete clears it entirely.
- **Node loop-prevention** — a node never receives back events it produced, making
  N-way multi-master fan-out safe by construction.
- **Routers** — server-side `source → dest` forwarding that is **async** (off the
  write/ack path) and **derived**: forwarded copies are **not separately WAL-logged**, so
  one source append produces exactly **one WAL write regardless of fan-out**. A durable
  per-router cursor drives forwarding and replay-from-cursor recovery. At-least-once,
  per-source FIFO, cycle-rejecting by default; `$node` is preserved (loop prevention), with
  a hop cap. A derived destination is **single-source**: a second router with a *different*
  source into the same dest is rejected with `409 box_exists_incompatible`
  (`error.detail.reason: "router_dest_fan_in"`). (This async/derived
  path is gated behind `STREAMS_FORWARD_V2`; the current default is the legacy synchronous
  in-line forward — see the config table.)
- **Lease-based queues** — set `type: "queue"` to layer claim/ack/nack/extend (and a
  `/work` auto-claim SSE stream) on the same log: visibility-timeout leases,
  coalesced fair fan-out, redelivery, and optional dead-lettering (see API §10).
- **Multiplexed SSE** — watch many boxes over one resumable connection with composite
  cursors, named events, heartbeats, and tombstones.
- **Per-box durability, three commit classes** — `memory` (disk-like but best-effort: same WAL
  write+recovery path, fully queryable, but data may survive or be lost on restart), `disk`
  (group-committed WAL, survives a crash minus the un-fsynced tail), and `fsync` (fsync-gated
  ack, survives any crash). WAL-first: a `disk`/`fsync`-acknowledged write is committed and
  nothing visible is ever un-durable. Crash-recovery via snapshot + WAL replay on start.
- **Priority + elastic throttling** — manual or recency-based auto priority; under CPU
  pressure delivery degrades in latency, never in correctness.
- **Single static binary** — WAL + segments on local NVMe, restartable at any instant;
  only data not yet in the WAL is lost.
- **Hardened auth + resource model** — bearer keys hashed at rest (SHA-256) and constant-time
  compared; optional per-key scopes (`read`/`write`/`delete`/`admin`) + a box-name prefix
  allowlist enforced per route; configurable DoS-hardening limits on boxes / routers / watch
  sessions / concurrent SSE connections / per-key in-flight requests. Loopback-default bind.

### Status — what's implemented vs planned

Honest about maturity (the `/v0` contract is fully implemented; some operational edges are
partial or planned):

- **Implemented:** the full `/v0` API (boxes, batched diff reads, permanent deletes,
  routers, multiplexed SSE, lease-based queues); the **sharded** WAL with adaptive group
  commit (`STREAMS_WAL_SHARDS`, default `min(num_cpus, 8)`, shard-count-agnostic recovery);
  **async + derived router forwarding** (off the write/ack path, forwarded copies not
  WAL-logged so one source append is one WAL write, durable per-router cursor with
  replay-from-cursor recovery, single-source-per-dest enforced via `409 box_exists_incompatible`
  with `error.detail.reason: "router_dest_fan_in"`);
  the three durability commit classes (`memory`/`disk`/`fsync`); segments + snapshots +
  crash-recovery replay (including the directory-fsync hardening so a rotated/first WAL
  file's entry is durable before an ack); per-box tag index; node loop-prevention; bearer
  auth with **keys hashed at rest** (SHA-256, constant-time compare, plaintext zeroized after
  startup), **optional per-key scopes + box-name prefix allowlist** (enforced on path, request
  body, and list results), **resource/rate limits** (boxes / routers / watch sessions / SSE
  connections / per-key in-flight / total-bytes quota, a per-response byte budget, queue `seqs`
  length bound, idle watch-session GC), the `wid`-plus-key watch-stream binding, and the
  loopback-default bind.
- **Partial:** the cap-vs-TTL tombstone **reason** is best-effort across a restart (the gap
  *range* is always authoritative). The throughput/latency targets above are design goals
  validated by the in-memory baseline benchmarks, not yet a tuned production SLO on durable
  boxes.
  With `leases_durable:true`, a queue **claim** durably logs its lease BUT the lease-log
  append is best-effort: if it fails (a transient WAL error), the claim still succeeds and the
  job degrades to the queue's baseline **at-least-once** guarantee (it becomes reclaimable
  early, on the visibility timeout, instead of replaying its in-flight lease across a restart).
  This never loses or duplicates a job beyond at-least-once; the in-flight-lease *durability*
  is the only relaxation. A fully fail-closed durable lease (stage → durably append → publish,
  propagating the error) is planned.
- **Out of scope (by design, not on the roadmap):** native TLS (terminate at a reverse
  proxy — see Security below), hard multi-tenant *namespace* isolation beyond the per-key
  scope + box-name-prefix allowlist that ships today, multi-server / replication / HA /
  single-writer fencing, LSM / keyed log compaction, and durable consumer groups as a server
  primitive (the consumer-group pattern is built at the application level with a box per
  consumer plus delete). See `docs/API.md` §0.2 / §0.11 and `docs/ROADMAP.md`.

---

## How it was built

The API and its semantics were fixed first and never changed; persistence and
scalability were added *underneath* that contract.

1. **Define API + docs** — the contract the implementation satisfies (`docs/`). ✅
2. **In-memory server** — the complete, correct `/v0` API. ✅
3. **Tests + benchmarks** — maximum-coverage tests and a baseline benchmark suite. ✅
4. **Make it durable + scalable** — WAL, group commit, segments, snapshots, crash
   recovery, priority scheduler, elastic throttling — all on one restartable
   process. ✅
5. **Lease-based queues** — claim/ack/nack/extend + `/work` stream on the same log. ✅
6. **Tiered storage** — optional hot→cold segment relocation off the hot path. ✅

See [docs/ROADMAP.md](docs/ROADMAP.md) for the original phase plan and acceptance
criteria.

---

## Documentation

- [docs/API.md](docs/API.md) — complete `/v0` HTTP API reference (the contract).
- [docs/DESIGN.md](docs/DESIGN.md) — data model & semantics: seq, dual watermark
  (`evict_floor`/`earliest_seq`), tombstones, permanent deletion, node loop-prevention,
  routers, priority.
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — storage, WAL, group commit, segments,
  recovery, scheduler, concurrency, crate choices, latency budget.
- [docs/ROADMAP.md](docs/ROADMAP.md) — build phases, acceptance criteria, benchmark plan.
- [docs/BENCHMARKS.md](docs/BENCHMARKS.md) — recorded Phase-2/3 in-memory baseline numbers (hardware, methodology, every applicable benchmark-plan metric).
