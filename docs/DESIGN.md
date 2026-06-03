# topics — Data Model & Semantics

This document specifies the data model and runtime semantics of topics precisely enough to
implement directly. It is **normative**: where it says MUST / MUST NOT / SHOULD, treat it as a
requirement on every implementation phase (in-memory, then WAL-backed). The wire encoding is
JSON; see [API.md](API.md) for the HTTP surface and [ARCHITECTURE.md](ARCHITECTURE.md) for how
the storage layer enforces these guarantees.

**Conventions.** Server-computed fields on the wire are `$`-prefixed (`$seq`, `$ts`, `$node`,
`$tag`, `$type`) so they never collide with the user-controlled `data` namespace. All times are
integer milliseconds since Unix epoch (`ts`) or integer millisecond durations (`*_ms`). `u64`
and `i64` are the literal Rust types.

---

## 1. Record

A record is one immutable event in a topic. Once assigned a `seq`, a record's fields never change.
Deletion (§7) and eviction (§5) never mutate a record's fields; a record is either present (and
unchanged) or removed (gone). Deletion is permanent removal, not a mutation.

### 1.1 Fields

| Field | Wire key (in → out) | Type | Origin | Required | Notes |
|---|---|---|---|---|---|
| Sequence | — / `$seq` | u64 | server | n/a | Per-topic monotonic id, assigned at commit (§4). |
| Timestamp | — / `$ts` | u64 ms | server | n/a | Server wall-clock at commit; used for TTL (§5.2). |
| Origin node | `node` / `$node` | string | client | optional | Loop-prevention key (§6). |
| Tag | `tag` / `$tag` | string | client | optional | Deletion match key (§7). |
| Meta | `meta` / `meta` | object<string,string> | client | optional | Small opaque metadata/headers. |
| Data | `data` / `data` | arbitrary JSON | client | required | Opaque payload; the product treats it as bytes. |

On **write** the client sets `node`/`tag` as plain top-level keys; on **read** the server echoes
them as `$node`/`$tag` (now server-canonical, immutable). `data`/`meta` keep the same key both
ways (pure passthrough). `$node`/`$tag`/`meta` are omitted from a response when absent (absence,
not `null`); `data` is always present (may be JSON `null`). A read-returned record:

```json
{ "$seq": 4096, "$ts": 1748470000123, "$node": "worker-7", "$tag": "render:tenantA:1234",
  "meta": { "content-type": "application/json" },
  "data": { "url": "https://...", "attempts": 0 } }
```

### 1.2 Size limits (hard, enforced at write; violation → `400`)

| Limit | Default | Justification |
|---|---|---|
| `data`+`meta` (canonical bytes) | 1 MiB | Large task payloads / batched events, yet small enough that one record can't blow a WAL frame or group-commit budget. |
| `tag` length | 256 bytes | Bounds the per-entry cost of the tag index (§7) and keeps prefix matching cheap. |
| `node` length | 128 bytes | Node ids are identifiers, not data. |
| `meta` total | 16 KiB, ≤ 64 keys | "Small metadata." |
| Records per write request | 10 000 | Bounds a single append's latency and WAL frame size. |
| Total write body | 64 MiB | Backstop against batch-size × per-record interaction. |

`seq`/`ts` are server-assigned and do not count against the `data`+`meta` size.

---

## 2. Topic

A topic is an append-only log of records ordered by `seq`, plus a small config and derived
watermarks.

### 2.1 Identity & naming

- Addressed by **name** in the path: `/v0/topics/:topic`. The name *is* the identity.
- Naming rule: `^[A-Za-z0-9][A-Za-z0-9._:-]{0,254}$` (1–255 chars, starts alphanumeric, allows
  `. _ : -`; `:` enables namespacing like `render-queue:tenantA`). Case-sensitive, byte-exact, no Unicode
  normalization.
- **Creation policy** is per-request, defaulting to lazy-create (turbopuffer ergonomics) with an
  opt-out (Redis `NOMKSTREAM` lesson against typo-topics):
  - Write with `create: true` (or the topic's `auto_create: true`) auto-creates using inline
    `config` or defaults.
  - Write with `create: false` to a missing topic → `404 topic_not_found`.
  - Explicit `PUT /v0/topics/:topic` creates-or-updates per the upsert rules (API §1.1).
- **Deleting** a topic tears down all records, the tag index, dedupe state, and any routers
  with it as `source` or `dest`. Irreversible. A later lazy-create makes a **new, empty** topic
  whose `seq` restarts at the base; stale consumers detect this exactly like full eviction
  (§5.5).

### 2.2 Config — see [API.md §0.10](API.md) for the canonical field table

The config struct is `{ ttl_ms, cap_records, cap_bytes, discard, durable, durability, priority,
auto_priority, auto_create, idempotency_window_ms, dedupe_node }`. Defaults: `ttl_ms=0`,
`cap_records=0`, `cap_bytes=0` (all "off"/unbounded), `discard="old"`, `durable=false`,
`durability` resolved from `durable`, `priority=null` + `auto_priority=true`, `auto_create=true`,
`dedupe_node=true`.

**Durability commit class (`durability`).** A topic has one of four commit classes — the
durability/performance tradeoff — resolved from its current config as `durability_class()`
(the topic `type` is immutable, but durability/config can be updated in place, and the resolved
class always reflects the current config):

- **`ephemeral`** — resident-only record payloads. The topic CONFIG always survives (a control
  frame), but appends/deletes skip the WAL and HOT segment writer even in a durable engine.
  Records are fully queryable while the process is running and are intentionally lost on
  restart. Checkpoints preserve the published head without payloads, so post-checkpoint writes
  do not reuse seqs. Never `fsync`-gated, so `fsync_ms` is `0`. A router / dead-letter copy whose
  **destination** is an ephemeral topic is resident-only too.
- **`memory`** — _"disk-like but best-effort"_. Takes the **same** group-committed WAL write
  **and** recovery path as `disk` (the SAME write/recovery code — no special-casing) and is
  fully queryable (getState / getDifference / SSE; its records may persist), but carries **NO
  durability GUARANTEE**: after a restart its records **MAY survive OR be lost**, and recovery
  is **gradual / best-effort** — it does **not** block readiness and does **not** guarantee
  completeness or emptiness. Never `fsync`-gated, so `fsync_ms` is `0`. The topic CONFIG always
  survives (a control frame). It forgoes the disk-class seq-ceiling fsync (`HeadWatermark`), so
  on a lost tail its `head_seq` may legitimately regress; it never fabricates a record or hands
  out a future seq. A router / dead-letter copy whose **destination** is a memory topic takes the
  same best-effort path (the copy may survive or be lost). Effectively `disk` minus the
  durability promise — for caches / scratch where occasional loss is acceptable.
- **`disk`** — written to the WAL and **group-committed** (no per-write `fsync`); `fsync_ms` is
  `0` (the fast path). The write is acked as soon as its frame is enqueued to its topic's WAL-shard
  writer (the WAL is sharded — see ARCHITECTURE §2; the ack is **not** fsync-gated — the engine
  drops the commit token); the shard writer group-commits and `fdatasync`s the batch shortly
  after. Survives a crash **minus the un-fsynced tail** (the frames enqueued but not yet
  group-fsynced). This is today's `durable:false`.
- **`fsync`** — the ack is **`fsync`-gated** (held until the WAL frame is durably synced; real
  `fsync_ms`). Survives **any** crash. This is today's `durable:true`.

**Resolution.** An explicit `durability` always wins; absent it, the class is
derived from the `durable` bool (`true ⇒ fsync`, `false ⇒ disk`) — so `ephemeral` and
`memory` are reachable only by setting `durability` explicitly. The resolved class is reported
on every topic-state / topic-create response, and `durable` is normalized to
`durable == (class == fsync)`.
`is_durable()` is `class == fsync`.

**Defaults rationale.**
- All caps/TTL off ⇒ an out-of-the-topic topic loses nothing. The safe default for a persistence
  product is "keep everything"; pub/sub users *opt into* small cap+TTL. Silent loss must be a
  deliberate choice.
- `discard="old"` matches the "append log" mental model and the pub/sub/recent-state cases;
  durable-queue users flip to `"reject"`. With both caps off, `discard` is inert.
- `durable=false` (class `disk`): the 1–5 ms target on NVMe means fsync-by-default would make
  the common pub/sub case pay for a guarantee it doesn't want. One bool (or `durability`) away
  from `fsync`. `memory` is `disk` with **no durability guarantee** — best-effort/lossy
  caches/scratch topics that take the same write+recovery path but where data MAY survive OR be
  lost on restart (recovery is gradual, never blocks readiness). `ephemeral` is the explicit
  RAM-only class for transient streams that must skip record WAL/segment work.
- `priority=null` + `auto_priority=true`: most users never think about priority; recency-based
  auto does the right thing. Power users pin an integer.

When both `cap_records` and `cap_bytes` are set, **whichever is hit first** triggers eviction
(Kafka dual-retention). TTL and cap are independent and both apply; a record leaves the retained
set when *any* of (ttl expired) OR (beyond `cap_records`) OR (beyond `cap_bytes`) holds.

### 2.3 State (read via `GET /v0/topics/:topic`)

`head_seq` (highest assigned seq, log end; `0` if never written), `earliest_seq` (lowest
retained non-expired deliverable seq, log start; `head_seq + 1` when empty), `next_seq`,
`count`, `bytes`, `config`, `effective_priority`, `last_write_ts`, `last_read_ts`. See §5.1 for
the exact definition of `earliest_seq` and API §1.2 for the response shape.

---

## 3. Priority & effective priority

Every topic has an **effective priority** combining a manual component and an automatic
recency component. It affects **delivery scheduling only** — never `seq` order, retention, or
which records are visible.

### 3.1 Formula & defaults

```
P_eff(topic, now) =
      W_manual * clamp(priority, -1000, 1000)
    + W_auto   * auto_recency(topic, now)
    + W_age    * age_boost(topic, now)

auto_recency = AUTO_MAX * 2^( -(now - last_consumed_at) / HALF_LIFE_MS )
age_boost    = AGE_RATE * min(now - enqueued_at, AGE_CAP_MS)
```

| Constant | Default | Meaning |
|---|---|---|
| `W_manual`, `W_auto`, `W_age` | `1.0` each | Component weights. |
| `priority` | `0` (config `null` ⇒ auto-only) | Operator-set base, clamped `[-1000, 1000]`. |
| `AUTO_MAX` | `500` | A freshly-consumed topic ≈ a +500 manual topic. |
| `HALF_LIFE_MS` | `30000` | Auto bonus halves every 30 s of inactivity. |
| `AUTO_FLOOR_MS` | `300000` | After 5 min untouched, auto term forced to 0 (skip the math). |
| `AGE_RATE` | `+100 / s waited` | Anti-starvation climb. |
| `AGE_CAP_MS` | `10000` | Aging capped at +1000 after 10 s. |

A topic is "consumed" by any `GET /v0/topics/:topic` (untouched if `touch=false`),
`POST /v0/topics/:topic/diff`, or SSE attach/delivery; each sets `last_consumed_at = now`.

Decay (AUTO_MAX 500, half-life 30 s): 0 s → 500, 15 s → 354, 30 s → 250, 60 s → 125, 120 s →
31, ≥300 s → 0. Half-life 30 s means an actively-polling consumer keeps its topic "hot" with
negligible upkeep, while a quiet topic sheds priority within a couple of minutes — matching
"recently consumed topics get higher effective priority" without letting a once-busy topic hog the
scheduler. `AUTO_MAX=500` keeps recency worth roughly half the manual range, so an operator can
still force a topic above all auto traffic with `priority=1000` (or below with `-1000`).

`P_eff` is never stored as ground truth; it is computed on demand at enqueue time and on a 50 ms
aging tick, using integer/fixed-point math (a 64-entry LUT for `2^-x`, no `powf` on the hot
path). Higher `P_eff` is served first; under CPU pressure the scheduler throttles elastically
but always drains higher priority before lower, with aging guaranteeing no starvation. See
ARCHITECTURE §4–5.

---

## 4. Seq assignment

### 4.1 Exact contract

- Each topic has its own `u64` counter `next_seq`, starting at `seq_base` (default `1`; `0` is
  reserved to mean "no records").
- On commit of a write of N records, the server atomically assigns `next_seq, …, next_seq+N-1`,
  sets `next_seq += N`, and returns the seqs in write order. Assignment is at **commit**
  (post-WAL-ordering), so `seq` order == durable commit order == delivery order.
- `seq` is **strictly increasing and contiguous at assignment** — no gaps in the assigned
  sequence. A single write request is **atomic**: all N records commit (contiguous seqs) or none.
- **"Mostly sequential" refers to what a consumer reading the retained window observes:** after
  eviction/TTL/deletion/node-filtering the seqs a consumer sees can have holes (4097, 4098,
  4101, …). The underlying assignment never skips; visibility does. A consumer MUST NOT assume
  received seqs are contiguous, but MAY assume they are strictly increasing, and that any missing
  seq below `head_seq` was either involuntarily evicted/expired (tombstone if it crossed the
  cursor — §5) or voluntarily deleted/node-filtered (silently skipped — §6, §7). **The distinction
  between "you missed data" (involuntary, tombstone) and "data was intentionally removed for you"
  (voluntary, silent) is the core safety property.**

### 4.2 Restart / recreate

- After restart, `next_seq` is recovered as `max(committed seq) + 1`. Records buffered in the WAL
  but not yet durably committed (for non-durable topics) are lost; their seqs are never reused, so
  seq is monotonic across restarts. Gaps from lost-but-acked non-durable writes are treated
  exactly like eviction by consumers.
- A deleted-and-recreated topic restarts `next_seq` at `seq_base`; because the new `head_seq` can
  be *below* a stale consumer's cursor, this is made non-silent via §5.5.

### 4.3 Cursors

- The canonical order is ascending `seq`; there is no secondary sort.
- A **cursor** is a plain `seq`, interpreted as exclusive lower bound: "records with
  `$seq > from_seq`." `from_seq = 0` means "from the beginning of what's retained." A tail /
  only-new cursor is `from_seq = head_seq` at subscription (Redis `$`).
- Reads are cursor-free in the turbopuffer sense (no opaque token); `diff` and SSE also return an
  explicit `next_from_seq` for convenience and batch boundaries.

---

## 5. Watermarks, eviction, TTL, tombstones

### 5.1 The dual watermark — `evict_floor` and `earliest_seq`

Each topic distinguishes **two floors** so the tombstone (involuntary loss) is decoupled from
voluntary deletion:

- **`earliest_seq`** — the seq of the **first currently-live record**: not eviction-reclaimed,
  not TTL-expired, and **not deleted**. Reported in topic state and `diff`. If no live record
  exists it is `head_seq + 1`. It is **monotonically non-decreasing** over a topic instance's
  life (eviction, TTL, and **deletion** all advance it); it resets only on delete+recreate
  (§5.5).
- **`evict_floor`** — advanced **only** by **involuntary** loss of live records: cap eviction
  and TTL expiry. It is the **sole tombstone trigger** (§5.4). Voluntary deletion (§7) advances
  `earliest_seq` but **never** `evict_floor`.

**Invariant: `evict_floor <= earliest_seq`, always.** Consequences:
- Reading below `earliest_seq` but at/above `evict_floor` means the gap is **purely deleted**
  (voluntary) ⇒ the read is **silent** (`tombstone: null`); the cursor advances past the
  deleted seqs.
- Reading below `evict_floor` means live records were lost to cap/TTL (involuntary) ⇒ a
  **tombstone** is emitted.

A consumer cursor `from_seq` is below the live floor iff `from_seq + 1 < earliest_seq`; it
gets a tombstone only iff `from_seq + 1 < evict_floor` (§5.4).

`evict_floor` is itself driven by two involuntary sub-floors, tracked so the tombstone can
report *why*:
- cap eviction — the highest seq removed by **cap** eviction of live records.
- TTL expiry — as a function of time, the highest seq that is **TTL-expired**; moves
  continuously with wall-clock even with no writes.

`evict_floor = max(cap-evicted seq, TTL-expired seq) + 1`. When the front of the log is mixed,
popping **already-deleted** slots (voluntary) does **not** advance `evict_floor`; evicting
**live** records (cap/TTL, involuntary) **does**. Both floors are clamped into
`[seq_base, head_seq + 1]`.

### 5.2 TTL expiry

- A record is **expired** when `now - $ts > ttl_ms` (strict). Expired records are never delivered
  and are excluded from `count`/`bytes`/`earliest_seq`.
- Expiry is evaluated at read/delivery time against current `now`; the implementation MAY also
  reclaim expired records lazily in the background (segment-granular, §5.6). A record can be
  logically expired (invisible) before physically reclaimed — never observable as anything but
  "expired."
- Because `$ts` is commit-assigned and `seq` is commit-ordered, `$ts` is **non-decreasing in
  `seq`** within a topic. So "all seqs ≤ X are expired" is a binary-searchable predicate (Redis
  time-id lesson) — finding the TTL sub-floor of `evict_floor` is O(log n) over segments, not
  O(n).

### 5.3 Eviction by cap & full policy

When a write would push the topic beyond `cap_records` or `cap_bytes`:
- `discard = "old"` (default): commit the write, then advance `evict_floor` by removing oldest
  records until back within cap — segment-granular and lazy (§5.6), so the topic may transiently
  exceed cap by up to one segment; `earliest_seq`/`count` reflect the logical floor.
- `discard = "reject"`: the write is **rejected synchronously before assigning any seq**, with
  `422 topic_full` and `error.detail` carrying `cap_records`/`cap_bytes` and current
  `head_seq`/`earliest_seq`. No record is acked-then-dropped (the NATS DiscardNew foot-gun is
  avoided). A single write larger than the entire cap is a permanent `400 record_too_large` (not
  retryable), distinguished from `422 topic_full` (retryable after consumers drain).

### 5.4 Tombstone / gap — the exact consumer contract

A tombstone is an **in-band 200-level signal** (never an HTTP error) telling a consumer "there is
a gap below where you're reading; you missed `[gap_from, gap_to]` and here is why." It is the
single mechanism for *all* non-silent loss.

**When emitted.** A `diff` or SSE delivery for cursor `from_seq` MUST emit a tombstone (before
any subsequent records) iff `from_seq + 1 < evict_floor` — i.e. live records below the cursor
were lost **involuntarily** to cap/TTL. After emitting it the read continues from `earliest_seq`
(cursor advanced to `earliest_seq - 1`). A cursor that fell below `earliest_seq` but stays at or
above `evict_floor` fell into a **purely-deleted** gap (voluntary, §7): **no** tombstone, the
cursor simply advances past the deleted seqs.

**Shape** (a pseudo-record; carries a resumable position so SSE `id:` works on it too):

```json
{ "$type": "tombstone", "$seq": 479101,
  "gap_from": 478501, "gap_to": 479100,
  "reason": "cap", "missed_estimate": 600,
  "earliest_seq": 479101, "head_seq": 480234 }
```

- `gap_from = from_seq + 1` (what they asked for next), `gap_to = earliest_seq - 1` (last seq
  before the first live record); the reported range is inclusive both ends. (Some seqs in the
  range may have been deleted rather than evicted; the consumer cannot tell and does not need to
  — the tombstone fires because *some* live data below the cursor was lost involuntarily.)
- `$seq` of the tombstone equals `earliest_seq` (first live seq after the gap), so it slots into
  the monotonic id stream as a valid resume point.
- `reason` ∈ `"cap"` (evicted for capacity), `"ttl"` (TTL-expired), `"mixed"` (both contributed),
  `"recreated"` (topic deleted+recreated, §5.5). In SSE the connect-time variant `"from_seq_too_old"`
  is also used (same meaning as cap/ttl discovered at connect). The reason is informational; the
  **gap range is authoritative**.

**In `diff`:** the tombstone is the `tombstone` field (`null` when none); `records` begin at
`earliest_seq`; `next_from_seq` continues past it. At most one tombstone per response (the gap is
always one contiguous range because `earliest_seq` is monotonic).

**In SSE:** a framed `event: tombstone` with `id:` encoding the post-gap cursor (so reconnect
resumes correctly). The consumer handles `record` and `tombstone` uniformly.

**The loss/removal kinds — a hard contract:**

| Kind | Detectable? | Mechanism | Consumer sees |
|---|---|---|---|
| **Eviction by cap** (involuntary) | YES, never silent | `evict_floor` advanced; crossing a cursor ⇒ tombstone `reason:"cap"` | gap range + tombstone |
| **Expiry by TTL** (involuntary) | YES, never silent | `evict_floor` advanced by clock; crossing a cursor ⇒ tombstone `reason:"ttl"` | gap range + tombstone |
| **Permanent deletion** (voluntary) | NO, intentionally silent | records removed by `before_seq`/tag `match` (§7); advances `earliest_seq`, not `evict_floor` | deleted seqs simply absent; no tombstone |
| **Node loop-prevention** (voluntary) | NO, intentionally silent | reader's own-node records dropped (§6) | own seqs absent; no tombstone |

The principle: **involuntary loss the consumer did not ask for ⇒ tombstone; voluntary removal
deliberately requested ⇒ silent skip.** Permanent deletion and node-filtering are intentional
drops, so they don't trip the gap alarm; a consumer that wants to detect them compares received
seqs against `head_seq`/`earliest_seq`. Cap/TTL are capacity-driven drops the consumer never
consented to, so they always alarm. The dual watermark (§5.1) keeps these separate:
`evict_floor` only ever moves on involuntary loss, so a purely-deleted gap reads silently and
mixing the two signals is structurally impossible.

### 5.5 Delete+recreate / seq rewind

If a topic is deleted and a new topic of the same name is created, a stale consumer presenting
`from_seq >= new head_seq` (from the future relative to the new topic) MUST receive a tombstone with
`reason:"recreated"`, `gap_from = seq_base`, `gap_to = new head_seq` (possibly empty), then the
read proceeds from the new `earliest_seq`. The server detects this via a per-topic-instance
**epoch** (monotonic counter bumped on create); cursors MAY encode the epoch so the rewind is
detected exactly. Absent an epoch, the server treats `from_seq > head_seq` as a recreate signal.

### 5.6 Eviction is segment-granular and lazy (perf contract)

Records are stored in append-ordered **segments**. Eviction and TTL reclamation remove **whole
segments** once *all* records in a segment are beyond cap or expired (Redis `~` / Kafka segment
lesson). Per-record physical deletion on the hot path is forbidden. Consequence (documented
honestly): `earliest_seq`/`count`/`bytes` are computed against the *logical* floor (exact);
physical occupancy may transiently exceed cap by up to one segment. **Consumers always reason
from the reported `earliest_seq`, never from cap arithmetic.**

**Tiered storage (transparent).** Segments are also the unit of **tiering**: the active + recent
sealed segments stay HOT (fast NVMe), older sealed segments may relocate to a COLD tier
(`TOPICS_COLD_DIR`; a different folder now, an object store later). Tiering changes **nothing** about
the `/v0` API or semantics — a cold read may be slower for `getDifference`/historical reads, but
writes and live delivery (SSE/tail) are unaffected (the relocator and cold I/O run off the hot path).
When no cold tier is configured, nothing relocates. Cap/TTL/delete reclaim drops a whole segment in
either tier. See [ARCHITECTURE §3.6](ARCHITECTURE.md).

---

## 6. Node loop-prevention

Purpose: make router fan-out across N symmetric *logical* nodes safe (a multi-master *topology*
built from local routers — `node` is a content/origin label, **not** a separate machine; topics
is single-server and does not do remote/multi-server replication, §12), so a node never receives
back events it produced.

- Every record MAY carry an origin `node` string (set by the writing client), recorded as
  `$node`, immutable, and **preserved verbatim through router forwards** (§8.3).
- A read (`diff` or SSE) MAY carry a reader node id `node` (the filter). When present the read
  MUST drop every record whose `$node == node` (byte-exact) before delivery. Records with no
  `$node`, or a different `$node`, pass through.
- The filter is applied **after** retention/TTL but is content-based and intentional, so dropping
  own-node records is **silent** (no tombstone — §5.4). Dropped seqs are simply absent; the cursor
  still advances past them.
- Matching is exact equality only (no prefix). A read MAY pass multiple node ids
  (`"node": ["a","b"]`) → "drop if `$node` ∈ set."
- **Batching interaction:** because node-filtered records are dropped *after* the bounded batch
  window is selected, a batch may return fewer than `limit` records (even zero) while still
  advancing the cursor. The consumer relies on `next_from_seq`/`head_seq`/`caught_up`, not on
  batch fullness, to know whether it is caught up (avoids unbounded scanning to "fill" a batch).
- Disable per-topic with `dedupe_node: false` if you genuinely want echoes.

---

## 7. Deletion (permanent, point-in-time)

### 7.1 Model — permanent, async, immediate, silent, point-in-time

`POST /v0/topics/:topic/delete` removes records by **seq range** (`before_seq`) and/or by **tag**
`match`. It is a one-shot operation against the records present at call time, **not** a standing
filter. Five properties define it:

- **PERMANENT.** Deleted records are gone for good; there is no un-delete in `/v0` (this resolves
  the old open question — see ROADMAP). To "resurrect," write a new record.
- **EFFECTIVE IMMEDIATELY.** The delete is invisible to **all** reads at once — `diff`, topic state
  `count`/`bytes`, and SSE. A reader's cursor simply advances past the deleted seqs.
- **ASYNCHRONOUS, NO COMPACTION / NO RECLAIM.** Records are *logically* gone instantly (the work
  runs off the call path), but a deleted record **stays on disk, just marked** — there is no
  compaction and no per-record disk reclaim. In memory the payload/tag is freed and front-of-log
  physical slots are popped only as the prefix becomes fully dead. On disk a record already sealed
  into a segment has its **delete-flag byte flipped in place** in the segment file (the WAL stays
  append-only — a `Delete` frame is appended, never mutated in place); the only space released is
  a **whole segment dropped in one op** when a delete clears it entirely (ARCHITECTURE §1, §3,
  §3.5). A still-live segment's marked records are never rewritten or reclaimed.
- **SILENT.** A delete **never** produces a tombstone. Tombstones stay reserved for **involuntary**
  cap/TTL loss (§5.4). A delete advances `earliest_seq` but **not** `evict_floor` (§5.1), so reading
  across a purely-deleted gap returns `tombstone: null`.
- **POINT-IN-TIME.** A `match`-only delete is bounded by the **current head** at call time
  (bound = `head_seq + 1`); future records with that tag are **not** deleted (e.g. "revoke a kicked
  user's chat" via `match ["tag","Glob","chat-42:*"]` cancels only what exists now, not a message
  sent a moment later by an in-flight producer).

**Request grammar.** At least one of `before_seq` / `match` is required (else `400
invalid_request`):
- `before_seq` (u64): delete records with `$seq < before_seq`.
- `match`: `["tag","Eq","X"]` exact, or `["tag","Glob","X*"]` **trailing-prefix only** (the rule's
  pattern ends in a single `*`; a record matches iff `$tag` has the literal prefix preceding it).
  No general globbing — this keeps a tag delete a point lookup or a bounded prefix range scan over
  the tag index (§7.2), never a regex. Bare string `"X"` is shorthand for `["tag","Eq","X"]`.

**Semantics by combination:**
- `before_seq` only → delete every record with `$seq < before_seq` (SNAPSHOT / compaction by seq).
- `match` only → delete every **existing** record whose tag matches, bounded by `head_seq + 1`
  at call time.
- `match` + `before_seq` → delete records with `$seq < before_seq` **AND** tag matches (e.g.
  publish v2 of a message then delete its prior versions, keeping the new one: `match
  ["tag","Eq","msg-123"]`, `before_seq = <seq of v2>`).

Records with **no tag** are never matched by a `match`. A delete is committed and WAL-logged
(survives restart). The response returns `deleted` (count removed) plus the post-delete
`earliest_seq`/`head_seq`/`count`/`bytes` (API §5).

### 7.2 The per-topic tag index (efficient match-deletes)

A `match` delete MUST NOT scan the whole log. Each topic keeps a **tag index** mapping tag → its
live seqs in ascending order — conceptually a `BTreeMap<String, Vec<u64>>`:
- **Exact** `Eq "X"` → point lookup of `index["X"]`.
- **Prefix** `Glob "X*"` → range scan over keys in `["X", next-key)`.

The index is populated on **append** (for tagged records) and pruned on **delete** and on
**front reclaim**, so a tag delete touches only the matching seqs (then drops their payloads and
prunes their index entries). Since `$tag` ≤ 256 bytes the per-key cost is bounded.

A `before_seq` (snapshot/prefix) delete is applied in O(1) via a `delete_below` marker (the max
`before_seq` ever applied); reads start at `max(from_seq + 1, base_seq)` and skip any remaining
deleted/expired/foreign slots.

### 7.3 Composition with reads / SSE / eviction (the read pipeline)

Per candidate seq from the cursor, in order, for both `diff` and SSE (ARCHITECTURE §-read-path):

1. **Live-floor gate** — skip if before the earliest live record. If `from_seq + 1 < evict_floor`
   (involuntary cap/TTL loss), emit a tombstone (§5.4); a purely-deleted gap is skipped silently.
2. **TTL** — skip if TTL-expired (*involuntary* → contributes to a tombstone via `evict_floor`).
3. **Deleted** — skip if the slot is deleted (*voluntary* → silent).
4. **Node filter** — skip if `$node` ∈ reader node set (§6) (*voluntary* → silent).
5. Deliver the surviving record (respecting batch `limit` / SSE flow).

Skipped seqs (any reason) **still advance** `next_from_seq`. The tombstone is computed **solely**
from `from_seq` vs `evict_floor`, independent of deletions.

Cap/TTL eviction is computed against the live records that remain after deletion — a deleted
record no longer counts toward `cap`/`age` (its payload is already freed). Deletion never
propagates through routers: a delete on `src` removes records from `src`, but a copy may already
have been forwarded to `dst`; to remove it in `dst` too, issue a delete on `dst` (§8.3).

### 7.4 Durable deletion of already-sealed records (the segment side)

A delete must be durable for records that have already been sealed into an immutable segment, even
after a checkpoint trims the WAL. Three witnesses are kept in agreement:

- **The in-memory mark** — the slot's `deleted` flag (the read pipeline, §7.3 step 3, skips it).
- **The WAL `Delete` frame** — append-only, carrying the `before_seq`/`match`/`seqs` selector and
  the point-in-time `bound_head`. The WAL is **never mutated** for a delete (it stays append-only);
  the frame covers not-yet-sealed (tail) records, replay ordering, and the point-in-time bound.
- **The on-disk segment delete-flag byte** — for an **already-sealed** record, the delete also
  flips a single trailing **delete-flag byte** in the segment `.data` frame **in place** (then
  fsyncs). The byte sits **after** the frame's XXH3 checksum (outside the checksummed body) and is
  **inside** `frame_len`, so the flip rewrites neither the frame nor the checksum and never changes
  the framing stride. Liveness is an exact sentinel: a frame is deleted **only** when the byte is
  `0xD5`; every other value — the `0x00` an encode writes, or any partial value a torn one-byte
  write could leave — reads **live**. A single-byte write is sector-atomic, so a **mid-flip crash**
  either lands the full sentinel (deleted) or leaves the old byte (live): it can never corrupt the
  framing, skip a live record, or resurrect a deleted one. An un-landed flip simply means the
  delete did not take durably on the segment — the WAL frame + in-memory mark still cover it until
  the next checkpoint re-flips.

This makes a sealed-record deletion survive a **WAL trim / checkpoint** that drops the `Delete`
frame: on recovery the engine **reads the on-disk flags back** (it lists each topic's segment files
and scans their `.data` frames) and re-marks the corresponding seqs deleted in the rebuilt index —
the deletion no longer depends on a retained WAL frame. Reads (`diff`/SSE) skip the flagged
records, and the recovery scan is idempotent and crash-safe to re-run.

**Whole-segment optimization.** When a delete (e.g. a `before_seq` trim, or a `match` that clears
an interior segment) makes **every** record of a segment dead, the segment is dropped in **one op**
— the existing segment-granular reclaim unlinks the whole `.data`+`.idx` pair (composing with the
cap/TTL segment eviction) — instead of N per-record flips. A **partially**-cleared segment stays on
disk and flips its deleted records **per-record**. Both paths are crash-safe (an unlink is
idempotent; a per-record flip is sector-atomic).

---

## 8. Router semantics

A router is a forwarding rule `src → dst`: every record committed to `src` is forwarded (appended)
to `dst`. `{ name, source, dest, preserve_node, preserve_tag, filter, allow_cycle, guarantee,
created_ts }`.

The **async + derived** model described below is the router forwarding model: one WAL write per
source append, off the ack path, with no silent loss.

### 8.1 Forward mechanics & ordering

- Forwarding is **async** and **off the source write/ack path**: a write to `src` acks
  immediately, and a background per-router worker forwards copies driven by a **durable
  per-router cursor**. When record `r` commits to `src` at `src.$seq = s`, the router appends a
  forwarded copy to `dst`, which assigns it a **fresh `dst.$seq`** (dst's own counter).
  `dst.$seq` is unrelated to `s`.
- **DERIVED — no WAL amplification.** A forwarded copy is **derived**: it is **not separately
  WAL-logged**. One source append produces exactly **one WAL write regardless of fan-out** (a
  source feeding N derived dests still costs one WAL write); the derived dest copies are
  reconstructed by replaying from the durable router cursor on recovery, not from per-copy WAL
  frames.
- **Single-source per derived dest.** A derived destination has exactly **one** source. A second
  router with a *different* source into the same dest is rejected with **`409`**
  (`error.detail.reason: "router_dest_fan_in"`). This keeps derived-recovery unambiguous (one
  cursor, one source order per dest). Direct writes to a dest, or a mixed direct+derived graph
  under `allow_cycle`, remain best-effort interleavings.
- **Ordering:** records forwarded from a single `src` to a single `dst` preserve `src` commit
  order in `dst` (per-source FIFO). Because each derived dest is single-source, its forwarded
  stream is a single ordered sequence; direct writes plus that one router can still interleave by
  `dst` commit time, with only **per-source FIFO** guaranteed.

### 8.2 Delivery guarantees

- The default is **at-least-once**. The router persists its forward cursor over `src`; on restart
  it **replays from the cursor** (re-derives un-forwarded copies). A crash between "appended to
  dst" and "advanced router cursor" can re-forward → duplicates in `dst`.
- **Exactly-once router delivery** is opt-in with `guarantee:"exactly_once"`. It keeps the
  derived/no-WAL model: forwarded copies are still not separately WAL-logged. The forwarded
  copy carries a stable idempotency key in `meta._topics_router`, derived from router name,
  source topic id, source epoch, and source seq. If catch-up/recovery sees that key already
  present in `dst`, it advances the cursor without appending another copy. The key must still
  be retained in `dst`; a delete or eviction removes the evidence.
- Exactly-once router delivery is scoped to the router's destination append. It does not make
  downstream side effects or arbitrary consumers transactional; consumers still need their own
  idempotency for external effects.
- Because forwarding is async and the copies are derived (not WAL-logged), the source ack never
  waits on the destination; an `ephemeral`/`memory`/`disk`/`fsync` dest governs only how/whether
  the *re-derived* copy is retained and recovered, not when the source write acks.

### 8.3 What carries through a forward

- The forwarded record in `dst` preserves `$node` (origin node — **never** rewritten when
  `preserve_node`; this is what makes loop-prevention work across the route), `$tag` (when
  `preserve_tag`), `meta`, and `data` verbatim.
- `$seq` and `$ts` are reassigned by `dst`. `exactly_once` routers reserve
  `meta._topics_router` for source identity and the router idempotency key.
- Deletes and node filters are **per-topic** and do not propagate through routers. A delete on
  `src` removes records from `src`, but a copy may already have been forwarded to `dst`; to remove
  it in `dst` too, issue a delete on `dst`. Honest and predictable.

### 8.4 dst cap / ttl behavior

A forward into `dst` is an ordinary append obeying `dst`'s config:
- `dst.discard = "old"`: forward succeeds, oldest `dst` records evict, router cursor advances.
- `dst.discard = "reject"`: the forward is rejected (`dst` full); the router **does not advance**
  its cursor and **retries with backoff** (the record is still in `src`) — backpressure up the
  route rather than data loss. If `src` itself then caps out under `discard:"old"`, an unforwarded
  src record can evict before being forwarded; under the default async/derived model the dest
  surfaces the un-derivable range as a **`source_trim` tombstone** on diff/SSE (an in-band,
  reader-visible signal — never a silent skip; see API §3), reflecting source retention faithfully.
  A durable fan-out should size `dst ≥ src` or use `discard:"old"` on `dst`. The single-process
  design favors local backpressure over unbounded buffering.
- `dst` TTL applies to forwarded records by `dst.$ts` (forward time), not src time.

### 8.5 Loop prevention across routers — two complementary layers

1. **Node loop-prevention (content-level, primary).** Because `$node` is preserved verbatim
   through every forward, a symmetric topology works: node A writes `$node=A`; routers fan it to
   B's and C's topics; B and C read with their own `node` filters, so they receive A's record but
   never re-emit it as their own. Routers forwarding a record do not change `$node`, so a record
   that loops back to a topic read by its origin node is filtered at read. This makes N-way
   multi-master safe even with cyclic router graphs, *as long as nodes set and filter their node
   ids* — the documented contract for the multi-master use case.

2. **Router graph cycle control (topology-level, resource safety).** Node filtering stops a node
   from *consuming* its own events but does not stop a record from being *forwarded* around a cycle
   forever (wasteful, can amplify). Therefore:
   - The server maintains the router graph. **Creating a router that would introduce a directed
     cycle is rejected at creation with `409 router_cycle`** (`error.detail.cycle:["a","b","a"]`).
     DAG-by-default is the simplest correct rule.
   - For intentional cyclic/multi-master topologies (A↔B mirrors), set `allow_cycle: true` on the
     router; the route then uses **runtime hop-cap loop-breaking**: each record carries a bounded
     **internal, in-memory hop count** (NOT exposed on the wire — there is no `$ttl_hops`,
     `$route_path`, or persisted hop set in the record); once the cap is reached the record is not
     forwarded again, so forwarding always terminates. An `allow_cycle` graph that mixes direct and
     derived routers is best-effort.

   Complementary: node-filtering prevents *delivery* of your own events (correctness); the
   DAG/hop-cap prevents *unbounded forwarding* (resource safety).

### 8.6 Router lifecycle

- `PUT /v0/routers/:router` creates after the DAG/cycle check; `source`/`dest` auto-created (lazy)
  if missing unless `create_dest: false`.
- `DELETE /v0/routers/:router` stops forwarding immediately; already-forwarded records in `dst`
  remain.
- Deleting a topic deletes the routers touching it (a router referencing a missing endpoint cannot
  exist).

---

## 9. Summary of the safety invariants (the load-bearing contract)

1. **`seq` is strictly increasing and gap-free at assignment**; visibility holes are normal and
   are categorized for the consumer.
2. **No silent capacity loss.** Any cap-eviction or TTL-expiry of live records crossing a
   consumer's cursor (i.e. `from_seq + 1 < evict_floor`) produces an in-band tombstone with an
   authoritative `[gap_from, gap_to]` and a best-effort `reason`. Identical contract in `diff` and
   SSE.
3. **Voluntary removal is silent and, for deletion, permanent.** Permanent deletion (by
   `before_seq`/tag `match`) and own-node filtering drop records without tombstones; they advance
   `earliest_seq` but never `evict_floor`. Consumers detect them (if they care) via
   `head_seq`/`earliest_seq` arithmetic, never confusing them with data loss. Deletion is
   permanent, effective immediately, async, and point-in-time (never a standing filter), with no
   compaction / no per-record reclaim (on disk a delete-flag byte is flipped in place; a whole
   cleared segment may be dropped).
4. **The dual watermark separates the two.** `earliest_seq` (first live seq) is the single source
   of truth for "what can I still read"; `evict_floor` (involuntary cap/TTL only, `evict_floor <=
   earliest_seq`) is the single tombstone trigger. Cap arithmetic is never authoritative (lazy,
   segment-granular eviction may exceed cap transiently).
5. **`$node` is immutable and preserved through routers**, making N-way multi-master fan-out safe
   by construction.
6. **Routers are async + derived, at-least-once, per-source FIFO, single-source-per-dest,
   DAG-by-default** (with an explicit hop-capped `allow_cycle` escape hatch): forwarding runs off
   the write/ack path, derived copies are not WAL-logged (one WAL write per source append
   regardless of fan-out), a durable per-router cursor drives replay-from-cursor recovery, and a
   second router with a different source into one dest is rejected `409 topic_exists_incompatible`
   (`error.detail.reason: "router_dest_fan_in"`). `dst`
   enforces its own retention and `discard:"reject"` applies backpressure rather than silently
   dropping.
7. **Durability is per-topic and explicit**; only data not yet in the (fsynced, for durable topics)
   WAL is lost on crash, and such loss appears to consumers as ordinary eviction-style gaps.

---

## 10. Queues (lease-based job delivery)

A **queue** is a *type* of topic (`type:"queue"`, API §0.10). It is purely **additive**: a
queue is an ordinary topic (everything in §1–§9 still holds — seq assignment, the dual
watermark, tombstones, node filtering, routers, durability) with a lease lifecycle layered
on top. A `"log"` topic (the default) behaves exactly as before and rejects the queue
endpoints with `409 not_a_queue` (API §10). This section is normative for the queue layer
only; it changes **no** existing semantics.

### 10.1 The two internal logs (event-sourced lease state)

A queue is **two logs**:

1. **The jobs log** — the topic itself. You `POST` records into it (API §2); each record is a
   job, identified by its `$seq`. Durability follows the topic's `durable` config. This is the
   queue's source of truth for *what work exists*.
2. **The leases log** — an append-only log of **lifecycle events** describing *who holds
   what*: `claimed`, `released`, `extended`, `acked` (one event per lifecycle transition,
   each naming the job `$seq`, the holder `node`, a `lease_id`, a `deadline`, and the event
   `$ts`). The **pending lease state is the materialized projection of this log** — it is
   event-sourced, not a separately-mutated table. Rebuilding the projection by replaying the
   leases log yields the exact current who-holds-what.

The leases log is built on the **same topic machinery** (it is an internal companion log of
the queue topic), so it inherits WAL framing, group commit, and crash recovery for free. Its
**materialized projection** (the live lease table + reclaim freelist + claim cursor) is held
in memory and rebuilt on restart (ARCHITECTURE §12).

**Durability nuance (a deliberate perf win).** The **jobs log** is durable per the topic's
`durable` config — *we must not lose jobs*. The **leases log defaults non-durable**
(`leases_durable:false`) because **losing leases on a crash is self-healing**: on restart, a
job with no replayed active lease is simply claimable again, which is exactly correct
visibility-timeout behaviour (§10.6). So the cheap path (don't fsync lease events) costs
nothing in correctness. **Ack durability == jobs-log durability**: an acked job is deleted
from the jobs log via the §7 permanent delete, and an acked+deleted job stays gone across a
crash iff that delete was durable (i.e. iff `durable:true`).

### 10.2 Lease lifecycle

A job moves through these states (all transitions are leases-log events; all time decisions
use the Clock, never wall-clock sleeps):

```
        produce (append to jobs log)
              |
              v
        ┌──────────┐  claim (claimed event, +1 delivery)   ┌───────────┐
        │  READY   │ ──────────────────────────────────►   │ IN_FLIGHT │
        │(claimable)│ ◄──────────────────────────────────  │ (leased)  │
        └──────────┘   nack (released, immediate/delayed)   └───────────┘
              ▲         lease expiry (deadline passed, lazy)      │  │
              │                                                   │  │ extend
              │                                                   │  │ (extended,
              │                              ack (acked event +   │  │  new deadline)
              │                              permanent delete)    │  ▼
              │                                                   ▼
              │                                              ┌─────────┐
              └──── (delivery > max_deliveries) ───────────► │  DONE   │
                    dead-letter: move to dead_letter topic     │(deleted)│
                    + permanent delete                       └─────────┘
```

- **READY → IN_FLIGHT** — a `claim` (API §10.2) leases the job to a `node`: appends a
  `claimed` event, sets `deadline = now + lease_ms`, increments the job's **delivery
  counter**.
- **IN_FLIGHT → DONE** — an `ack` (§10.4) appends an `acked` event and **permanently deletes
  the job from the jobs log** (delete *is* the ack, reusing §7). Acks are matched on
  `node` + seq; an ack from a worker that no longer holds the lease is silently skipped
  (idempotent). `lease_id` is **validate-when-supplied**, not strictly required: a caller MAY
  pass the per-seq `lease_ids` returned by its claim to **fence** ack/nack/extend (a stale token
  whose lease has been superseded is rejected/skipped); omitting it falls back to the
  `node` + seq match.
- **IN_FLIGHT → READY** — either a `nack` (§10.5, voluntary early release, optionally delayed
  by `delay_ms`) or **lease expiry** (§10.6, the deadline passed). Both append a `released`
  event (expiry is recorded lazily, on the reclaiming claim pass) and return the job to the
  claimable set.
- **IN_FLIGHT → IN_FLIGHT** — an `extend` (§10.6 heartbeat) appends an `extended` event
  setting a new `deadline = now + lease_ms`; the delivery counter is **not** touched.
- **READY → DONE (dead-letter)** — when a job would be delivered past `max_deliveries`, it is
  moved to the `dead_letter` topic and permanently deleted instead of re-delivered (§10.7).

### 10.3 The coalescing-window claim + even distribution

`claim_jitter_ms` is a **fairness/coalescing window**, *not* a backoff. Its meaning:

- **`claim_jitter_ms = 0` (default, greedy).** A claim is served immediately, lowest latency.
  First-arrival drains the head of the available set.
- **`claim_jitter_ms > 0` (coalescing).** A claim **waits up to that window** (Clock-driven).
  The server gathers **every** claimer that arrived during the window into a **cohort**, then
  in **one batched coordinator pass** (a single critical section over the queue) **divides
  the available jobs evenly across the whole cohort** — round-robin, proportional to each
  claimer's `max`. This is *not* first-arrival-drains-the-head: ten workers each asking for
  `max:10` against 50 available jobs get ~5 each, not 10/10/10/10/10/0/0/0/0/0.

The single-pass cohort design is also a **scalability win**: instead of N independent
per-claim atomic races over the head of the queue, one coordinator pass assigns the whole
cohort under one lock — fewer contended atomics, more predictable fairness. The `/work` SSE
topics (§10.8 of the API) participate in the same cohort, so polling claimers and pushed
workers are balanced together.

**The available set for a pass** is, in order: (1) the **reclaim freelist** — seqs whose
lease expired or whose nack `delay_ms` elapsed — **drained first** (so reclaimed work is
prioritised over never-delivered work, bounding redelivery latency); then (2) **fresh jobs**
handed out by a monotonic **claim cursor** over the jobs log (the next never-yet-leased seq).

### 10.4 At-least-once + idempotent consumers

The queue is **at-least-once**, matching routers (§8.2) and the BullMQ lesson:

- A **slow-but-alive** worker whose lease expires while it is still processing will have its
  job reclaimed and possibly delivered to another worker; if the slow worker later acks past
  its deadline, that ack is skipped but the job was already processed twice. **Duplicates are
  inherent**; consumers MUST be idempotent (e.g. dedupe on `$seq` or a job-level key in
  `data`/`meta`).
- **Per-job FIFO is not guaranteed across parallel workers**: claim order is roughly seq
  order, but reclaimed (freelist) seqs are served ahead of fresh ones, and parallel workers
  process at different rates. A single worker (`max:1`, one connection) sees near-FIFO; a
  fleet does not.
- **Exactly-once is not offered** — same rationale as routers (§8.2): it would require
  distributed-style dedup the single-process design avoids.

### 10.5 Reads, watch, and routers still work

A queue topic is still a topic. `GET /v0/topics/:q` returns the normal state **plus** a `queue`
sub-object (`ready`/`in_flight`/`dead_lettered`). `diff` (§3) and SSE `watch` (§7) read the
**jobs log** read-only — useful for monitoring/auditing the backlog — and never claim, ack,
or touch leases. Routers (§8) may forward into or out of a queue's jobs log like any topic
(a router into a queue is a producer; the dead-letter move is an internal append, not a
router). Node loop-prevention, tombstones, TTL, and caps apply to the jobs log unchanged
(e.g. `discard:"reject"` makes a full durable queue refuse new jobs rather than drop them).

### 10.6 Lease expiry is the visibility timeout (lazy, self-healing)

There are **no per-job timers**. A lease carries an absolute `deadline`; a job is reclaimable
once `now > deadline`. Reclaim is **lazy**: the next claim pass scans for expired leases,
moves those seqs onto the reclaim freelist (recording `released` events), and drains the
freelist first. This is the visibility-timeout primitive (SQS/BullMQ semantics) with zero
timer overhead.

Because the live lease state is a **purely derived projection** of a non-durable leases log,
a crash that loses un-fsynced lease events is **self-healing**: on restart the projection is
rebuilt from whatever lease events survived (possibly none), so any job without a replayed
active lease is immediately claimable — identical to every lease having expired. No job is
lost (the jobs log is durable per config); at worst an in-flight job is redelivered, which
at-least-once already permits.

### 10.7 Dead-lettering

Each job carries a **delivery counter** (incremented on every claim, including reclaims). When
a job is about to be delivered for the `(max_deliveries + 1)`-th time and the topic has a
non-null `dead_letter`, the server instead **moves** the job to the `dead_letter` topic —
appending its record there (preserving `$tag`/`meta`/`data`, stamping `meta.$dead_letter_*`
provenance) and permanently deleting it from the jobs log (the §7 delete path). With
`max_deliveries = 0` or `dead_letter = null`, jobs are reclaimed indefinitely. Dead-lettering
increments the `dead_lettered` observability counter. The dead-letter topic is an ordinary topic
(log or queue), so a poison job can be inspected via `diff`/`watch` or re-driven by reading it
and re-producing into the source queue.
