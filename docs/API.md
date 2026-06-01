# streams — HTTP API Reference (`/v0`)

A persistent event engine exposed as a JSON-first HTTP API. Single binary, single
machine. This document is the complete `/v0` surface — every endpoint, its body, its
response, and its errors. It is the contract the implementation must satisfy.

---

## 0. Conventions

### 0.1 Base URL & versioning

```
http://{host}:{port}/v0/...
```

The API version is the **first path segment** (`/v0`). Breaking changes ship as a new
prefix (`/v1`) and may run concurrently. Within a version only additive changes are made
(new optional request fields, new response fields, new endpoints). **Clients must ignore
unknown response fields.** There is no region in the host — streams is single-machine.

### 0.2 Auth

```
Authorization: Bearer <API_KEY>
```

Plain bearer token. Keys are supplied at startup (`STREAMS_API_KEYS`, comma-separated). A
missing/unknown key returns `401`. **Keys are hashed at rest** (SHA-256) — only the digest is
retained in memory, never the plaintext, and tokens are never logged. The check is
**constant-time**: the presented token is hashed and its digest compared against every
configured key's digest with no early-exit, so it does not leak which key — or how many
leading bytes — matched via a timing side-channel.

If the server starts with **no** keys configured, auth is disabled (single-tenant dev mode)
and the header is ignored — logged loudly at boot.

#### Optional scopes & box-name prefix allowlist

A key **may** carry a scope set and a box-name prefix allowlist. **This is fully additive and
back-compatible**: a bare `key` (no scopes, no prefixes) is a **full-access** key, exactly as
before. The `STREAMS_API_KEYS` entry syntax is extended:

```
key                       # full access (back-compat): all scopes, all boxes
key:scopes                # scopes only, all boxes
key:scopes:prefixes       # scopes + a box-name PREFIX allowlist
key::prefixes             # empty scopes field = ALL scopes, prefix-restricted
```

- **`scopes`** — a `+`-separated subset of `{read, write, delete, admin}` (single letters
  `r`/`w`/`d`/`a` and the alias `rw` are accepted). An **empty** scopes field means **all**
  scopes. Per-route requirements:
  | Scope | Routes |
  |---|---|
  | `read` | `GET /v0/boxes`, `GET /v0/boxes/:box`, `POST /v0/boxes/:box/diff`, `GET /v0/routers`, `GET /v0/routers/:r`, `POST /v0/watch` (+ the SSE GET, capability-gated) |
  | `write` | `POST /v0/boxes/:box` (records), queue `ack`/`nack`/`extend` |
  | `read`+`write` | queue `claim` and the `GET /v0/boxes/:q/work` stream (a lease *mutates* then returns jobs) |
  | `delete` | `DELETE /v0/boxes/:box`, `DELETE /v0/routers/:r`, `POST /v0/boxes/:box/delete` |
  | `admin` | `PUT /v0/boxes/:box`, `PUT /v0/routers/:r` (control-plane create/configure) |
  The `read` scope additionally gates `GET /v0/metrics` (§8.3).
- **`prefixes`** — a `|`-separated list of box/router-name prefixes the key may touch
  (e.g. `tenant42:|shared.`). An **empty** prefixes field means **any** name. The match is a
  byte prefix against the raw name, so the `tenant:` convention (§3) becomes a real boundary.
  The key secret may not contain `:` (the field delimiter); everything before the first `:` is
  the secret. The allowlist is enforced against **both** the path name and the relevant
  **request-body** names: the boxes a `POST /v0/watch` names, and a router's `source`/`dest`
  (auto-created on its behalf). The **list** endpoints (`GET /v0/boxes`, `GET /v0/routers`) are
  filtered to the allowlist, so a prefix-limited key cannot enumerate cross-tenant names.

A request that authenticates but lacks the required scope, or addresses a box/router name
outside its prefix allowlist (path or relevant body name), returns **`403 forbidden`**. A
**malformed scope token** in `STREAMS_API_KEYS` makes the server **refuse to start**
(fail-closed — it will not silently grant the wrong scope). The plaintext keys are parsed into
the hashed store once at startup and then **zeroized**, so no plaintext secret lingers in the
process config.

The watch `wid` (§7.1) is bound to **both** the creating key and its scope: the SSE GET can
never exceed the scope of the key that created the session, and a *valid but different* key
presented on the GET is rejected. **When auth is enabled the `wid` alone does not authorize the
GET** — the creating key must be presented (header or the dev-only `?token=`), so a leaked
`wid` is not a credential.

**Bind & the no-auth refusal.** The default bind is **`127.0.0.1:4000` (loopback only)** so an
unconfigured server is never an accidental public, unauthenticated event store. If the
configured bind is **non-loopback** (e.g. `0.0.0.0`) **and** no api keys are set, the server
**refuses to start** (and logs loudly) unless you explicitly set
`STREAMS_ALLOW_INSECURE_NO_AUTH=1`. Loopback with no keys stays dev-friendly. Configure the
bind via `STREAMS_HOST`/`STREAMS_PORT`.

> streams speaks **plain HTTP** — it does not terminate TLS, **by design**. For any non-loopback
> exposure, run it behind a TLS-terminating reverse proxy (or bind loopback and tunnel); native
> TLS is **out of scope** (§0.11). Bearer keys are secrets: a token sent over plain HTTP, or in a
> URL query string, can be observed in transit or in logs — see the watch `?token=` note in §7.1.

### 0.11 Security scope (what's deliberately out of scope)

streams is a **single-machine** event store; some platform concerns are intentionally left to the
deployment rather than built in:

- **TLS — out of scope.** streams speaks plain HTTP; terminate TLS at a reverse proxy (or bind
  loopback). This is a transport concern handled outside the binary, not a planned feature.
- **Scopes / prefix allowlist — *implemented* (§0.2).** A key may carry a scope set
  (`read`/`write`/`delete`/`admin`) and a box-name prefix allowlist; keys are hashed at rest.
  A bare key is still full-access for back-compat. The `tenant:` prefix convention (§3) becomes
  a real boundary when a key is prefix-restricted. **Multi-tenancy beyond these per-key scopes +
  prefix allowlists is out of scope** — there is no hard per-tenant namespace partition (and the
  prefix allowlist is a filter, not an isolated namespace).
- **Resource / rate limits — *implemented* (§11).** Configurable caps on boxes, routers, watch
  sessions, concurrent SSE connections (global + per-key), per-key in-flight requests, and a
  global **total-bytes** quota (`STREAMS_MAX_TOTAL_BYTES`), enforced on every creation/write path
  with a `429 throttled`; plus a per-response **byte budget**, a length bound on queue `seqs`
  arrays, and idle watch-session GC. See §11 for the env vars and §12 for the consolidated
  threat model.

### 0.3 Content types & encoding

- **Request bodies:** `Content-Type: application/json; charset=utf-8`, required on every
  `POST`/`PUT` with a body.
- **Response bodies:** `application/json`, except the SSE stream which is `text/event-stream`.
- **`data` payloads** are arbitrary JSON (object, array, string, number, bool, null),
  stored and returned **verbatim** — never parsed, indexed, or validated for shape. For
  binary, put a base64 string in `data` and a hint in `meta`.
- **Compression:** the server honors `Accept-Encoding: gzip` / `Content-Encoding: gzip`,
  but clients default it **off** — on a local-NVMe single-machine deployment the
  bottleneck is CPU, not bandwidth. Turn it on only for large bulk writes over a slow link.
- **Timestamps** are integer **milliseconds since Unix epoch** (`ts`, `*_ms`). Durations
  are integer milliseconds. There are no string dates anywhere.
- **`seq`** is a `u64` rendered as a JSON number. Seqs fit in IEEE-754 doubles until
  ~9 quadrillion, well beyond any single-machine box lifetime, so no string encoding is
  needed. Clients SHOULD parse as 64-bit integers regardless.

### 0.4 The `$`-prefixed metadata convention

Server-computed, per-record metadata is returned under `$`-prefixed keys so it can never
collide with user content (`data` and `meta` are the user's namespaces). On **write**, a
client sets `node`/`tag` as plain top-level keys (no sigil to remember); on **read**, the
server echoes them as `$node`/`$tag` to signal they are now server-canonical and immutable.

| Field | Type | Meaning |
|---|---|---|
| `$seq` | `u64` | Server-assigned sequence id. |
| `$ts` | `i64` (ms) | Server commit time. |
| `$node` | `string` | Origin node id supplied by the writer (loop prevention). Omitted if absent. |
| `$tag` | `string` | Tag supplied by the writer (deletion-match key). Omitted if absent. |
| (SSE event name) | — | SSE framing distinguishes payload kinds by the **event name** (`event: record`, `event: tombstone`, `event: caught-up`, …), **not** a `$type` field. Record payloads carry `$seq`/`$ts`/`$node`/`$tag`/`data`/`meta` — there is no `$type` key. |

`data`/`meta` keep the same key in both directions (pure passthrough). `$node`/`$tag`/
`meta` are omitted from a response object when absent (absence, not `null`). `data` is
always present (may be JSON `null`).

### 0.5 Canonical error body

Every non-2xx response (except in-stream SSE errors, §7) carries this exact shape:

```json
{
  "error": {
    "code": "box_not_found",
    "message": "box \"jobs\" does not exist",
    "detail": { "box": "jobs" }
  }
}
```

- `error.code` — stable machine-readable snake_case string. Clients branch on this.
- `error.message` — human-readable, may change between versions, never parsed.
- `error.detail` — optional structured context. May be absent.

Success responses carry **bare data** — no `{"status":"ok"}` envelope. The presence of a
top-level `error` key is the only success/failure discriminator.

> **Tombstones/gaps are NOT errors.** Eviction and TTL crossings surface as in-band `200`
> payload signals (a `tombstone` object in a diff, an `event: tombstone` frame in SSE).
> Data loss is always explicit but never an HTTP error. See §4.4.

### 0.6 Status codes (global table)

| Code | Meaning | `error.code` examples |
|---|---|---|
| `200` | OK (read, idempotent write/create/delete) | — |
| `201` | Created (box/router created on this call) | — |
| `400` | Malformed request (bad JSON, bad type, value out of range) | `invalid_request`, `batch_too_large`, `record_too_large` |
| `401` | Missing/invalid bearer token | `unauthorized` |
| `403` | Authenticated but the key lacks the required scope, or the box/router name is outside its prefix allowlist (§0.2) | `forbidden` |
| `404` | Box/router does not exist (and was not auto-created) | `box_not_found`, `router_not_found` |
| `405` | Wrong method for path | `method_not_allowed` |
| `406` | `Accept` not `text/event-stream` (SSE GET) | `not_acceptable` |
| `409` | Conflict: router cycle, config conflict, queue op on non-queue box | `router_cycle`, `box_exists_incompatible`, `box_not_empty`, `not_a_queue` |
| `413` | Body exceeds server hard limit (pre-parse) | `payload_too_large` |
| `415` | Wrong/missing `Content-Type` | `unsupported_media_type` |
| `422` | Semantically invalid (write to a full `discard:"reject"` box) | `box_full` |
| `429` | Elastic throttle / backpressure under CPU pressure, **or** a resource cap reached (§11) | `throttled` — carries `Retry-After`; a CPU-pressure throttle adds `error.detail.retry_after_ms`, a resource cap adds `error.detail.limit` |
| `500` | Internal error (bug) | `internal` |
| `503` | Not ready (WAL replay on boot) / shutting down | `not_ready`, `shutting_down` — carries `Retry-After` |

`429` is the elastic-throttle signal. Bulk writers that prefer to push through may set
`"disable_backpressure": true` in the write body (trusted-loader opt-out); the server then
admits the write but may queue it, trading latency for not failing.

### 0.7 Pagination & cursors

Three cursor styles for three read shapes:

1. **Box reads are seq-cursored, not opaque.** The cursor *is* `from_seq`. `POST .../diff`
   returns `next_from_seq`; pass it back. When fully caught up, `next_from_seq == head_seq`
   and `"caught_up": true`. We always return `next_from_seq`; `caught_up` is the "done for
   now" flag (for a live log "done for now" is not "done forever").
2. **List endpoints use opaque cursors.** `GET /v0/boxes` and `GET /v0/routers` return
   `next_cursor` (opaque base64), **present only when more pages exist** — its absence means
   the last page. Pass it back as `?cursor=`. `page_size` default `100`, max `1000`.
3. **SSE uses a composite cursor.** The multiplex watch encodes per-box `(box → seq)`
   positions as a base64url-encoded JSON map, usable as both `Last-Event-ID` and the
   `cursor` request field. See §7.

### 0.8 Idempotency

Two complementary mechanisms; no global idempotency-token service.

1. **Control-plane is idempotent by construction.** Create/configure is `PUT` and
   upsertable: an identical `PUT` is a no-op `200`; a changed `PUT` applies the diff.
   `DELETE` of an absent resource returns `200` with `"deleted": false`.
2. **Writes use an optional `idempotency_key`.** If supplied, the server remembers
   `(box, idempotency_key) → assigned seqs` for `idempotency_window_ms` (default 120000,
   per box). A retried write with the same key in-window returns the **original** seqs with
   `"deduped": true` and does **not** append again. The key may also be sent as header
   `Idempotency-Key:` (body field wins). A log has no "old value," so a dedup key — not a
   CAS condition — is the right primitive for safe append retries.

### 0.9 The `performance` block

Every JSON response (and most errors) includes a `performance` object so observability
lives in the response, not a side channel:

```json
"performance": {
  "server_total_ms": 0.41,
  "wal_append_ms": 0.12,
  "fsync_ms": 0.0,
  "records_scanned": 128,
  "throttle_wait_ms": 0.0
}
```

Fields are best-effort and additive; clients must tolerate any subset. `fsync_ms` is `0.0`
for non-durable boxes. `throttle_wait_ms` is time parked behind the elastic scheduler.

### 0.10 The Box config object

This object appears in box-create requests and box-state responses. All fields are
optional on create; omitted fields take the documented default.

```json
{
  "type": "log",
  "ttl_ms": 0,
  "cap_records": 0,
  "cap_bytes": 0,
  "discard": "old",
  "durable": false,
  "durability": "disk",
  "priority": null,
  "auto_priority": true,
  "auto_create": true,
  "idempotency_window_ms": 120000,
  "dedupe_node": true,
  "lease_ms": 30000,
  "claim_jitter_ms": 0,
  "max_deliveries": 0,
  "dead_letter": null,
  "leases_durable": false
}
```

| Field | Type | Default | Meaning |
|---|---|---|---|
| `type` | `"log" \| "queue"` | `"log"` | Box kind. `"log"` is the plain append-only log (every endpoint in §1–§7). `"queue"` additionally enables the claim/ack/nack/extend/work endpoints (§10) — lease-based at-least-once job delivery layered on the same log. The five `lease_ms`/`claim_jitter_ms`/`max_deliveries`/`dead_letter`/`leases_durable` fields below are **only meaningful when `type:"queue"`** (ignored, but accepted, on a `"log"` box). `type` is **not** mutable via `PUT` once set (a `PUT` changing it returns `409 box_exists_incompatible`). |
| `ttl_ms` | `u64` | `0` (off) | Records older than this (by `$ts`) are not delivered (expired). `0` = no TTL. Expiry is a read-time filter plus lazy segment reclamation; crossing a consumer's cursor yields a tombstone (§4.4). |
| `cap_records` | `u64` | `0` (off) | Max retained record count. On overflow the box evicts per `discard`. `0` = unbounded. |
| `cap_bytes` | `u64` | `0` (off) | Max retained payload bytes (`data` + `meta` + framing). `0` = unbounded. Whichever of `cap_records`/`cap_bytes` is hit first triggers eviction. |
| `discard` | `"old" \| "reject"` | `"old"` | Full-box policy. `old` = evict oldest (pub/sub friendly). `reject` = refuse the write with `422 box_full` so durable queues fail loudly rather than dropping unconsumed work. |
| `durable` | `bool` | `false` | **Back-compat alias** for `durability` (below). On create: `durable:true` ⇒ class `fsync`, `durable:false` ⇒ class `disk` (only consulted when `durability` is absent). On every response it is reported as `durable == (durability == "fsync")`, so a legacy client reading `durable` still sees the right boolean. Prefer `durability` for new clients. |
| `durability` | `"memory" \| "disk" \| "fsync"` | _(resolved from `durable`)_ | The **durability commit class** — where a write lands and when it is acked (the durability/perf tradeoff). **`memory`** — _"disk-like but best-effort"_: takes the **same** group-committed WAL write **and** recovery path as `disk` and is fully queryable (getState / getDifference / SSE), but carries **NO durability GUARANTEE**. After a restart its records **MAY survive OR be lost** (recovery is gradual / best-effort: it does **not** block readiness and does **not** guarantee completeness or emptiness). The box CONFIG always survives. Never `fsync`-gated, so `fsync_ms` is `0`. Effectively `disk` minus the durability promise — for caches / scratch where occasional loss is fine. **`disk`**: written to the WAL and **group-committed** (no per-write `fsync`); acked on frame enqueue (the ack is **not** `fsync`-gated — group-committed shortly after by its box's WAL-shard writer; the WAL is sharded — see ARCHITECTURE §2); survives a crash **minus the un-fsynced tail**; reports `fsync_ms` as `0` (the fast path). **`fsync`**: the ack is **`fsync`-gated** (held until the WAL frame is durably synced, real `fsync_ms`); survives any crash. **Resolution:** an explicit `durability` always wins; otherwise it is derived from `durable` (`true`⇒`fsync`, `false`⇒`disk`) — so `memory` is reachable only by setting `durability:"memory"` explicitly. The resolved class is always reported in box-state/box-create responses, and the class is freely mutable in place. Router-forwarded / dead-lettered copies honor the **destination** box's class (a `memory` dest persists a best-effort copy — it may survive or be lost). |
| `priority` | `i32 \| null` | `null` | Manual delivery priority (higher served first under pressure), clamped `[-1000, 1000]`. `null` ⇒ use auto-priority. |
| `auto_priority` | `bool` | `true` | If `priority` is `null`, derive effective priority from recency of the last read/SSE/state call on this box. A manual `priority` always overrides. |
| `auto_create` | `bool` | `true` | Whether a write to this box name may lazily create it. The per-write `create` flag can override downward. |
| `idempotency_window_ms` | `u64` | `120000` | How long `(box, idempotency_key)` dedupe state is retained (§0.8). |
| `dedupe_node` | `bool` | `true` | Whether node loop-prevention filtering is enabled on reads of this box (§4.5/§7.1). Essentially always `true`; exposed so a box can opt out of node filtering entirely. |
| `lease_ms` | `u64` | `30000` | **Queue only.** Default lease (visibility-timeout) duration for a claim. After `lease_ms` with no ack/extend, the job becomes claimable again (§10.3). Per-claim `lease_ms` overrides this. Clamped `[100, 86400000]`. |
| `claim_jitter_ms` | `u64` | `0` (greedy) | **Queue only.** Coalescing-window width. `0` = serve each claim immediately, lowest latency, first-arrival drains the head. `>0` = a claim waits up to this window, the server gathers the whole cohort of claimers that arrived in it, then divides the available jobs **evenly** across the cohort (§10.2). Clamped `[0, 5000]`. |
| `max_deliveries` | `u64` | `0` (off) | **Queue only.** After a job has been delivered (claimed) this many times without an ack, it is dead-lettered (§10.6) instead of reclaimed. `0` = unlimited redelivery (never dead-letter on delivery count). |
| `dead_letter` | `string \| null` | `null` | **Queue only.** Name of the box to move a job to when it exceeds `max_deliveries` (§10.6). `null` = no dead-letter box (the job keeps being reclaimed). May name any box (log or queue); auto-created on first dead-letter if absent and `auto_create` allows. Must differ from this box. |
| `leases_durable` | `bool` | `false` | **Queue only.** Durability of the *leases* log (the lifecycle-events log, §10.1). Defaults `false` because losing leases on a crash is **self-healing** — all in-flight jobs simply become claimable again on restart (correct visibility-timeout semantics), a deliberate perf win. The *jobs* log durability is governed by the normal `durable` field; **ack durability == `durable`** (an acked+deleted job stays gone iff its delete was durable). |

**Caps, deletes, and `earliest_seq`:** `earliest_seq` is the seq of the **first
currently-live record** (not evicted, not TTL-expired, not deleted; `head_seq + 1` when
the box is empty). Eviction is *segment-granular and lazy* (whole segments dropped, may
transiently exceed cap) and deletes advance `earliest_seq` past removed records, so the
true retained floor **must be read from the server** — never computed by the client from
`head_seq - cap_records`. A separate `evict_floor` (not in box state) tracks only
**involuntary** cap/TTL loss and is the sole tombstone trigger (§3.3): `evict_floor <=
earliest_seq` always, and a purely-deleted gap (below `earliest_seq` but at/above
`evict_floor`) reads silently with `tombstone: null`.

---

## 1. Box lifecycle

### 1.1 Create / configure a box — `PUT /v0/boxes/:box`

Create a box, or update its config. Idempotent and upsertable.

**Path** — `:box`, name `^[A-Za-z0-9][A-Za-z0-9._:-]{0,254}$` (1–255 chars, starts
alphanumeric, allows `. _ : -`; `:` enables namespacing like `jobs:tenantA`). Case-sensitive,
byte-exact.

**Request body** — the Box config object (§0.10). All fields optional; empty `{}` creates
all-default. Set `"type":"queue"` to make this a queue (enables §10); the queue tuning
fields (`lease_ms`, `claim_jitter_ms`, `max_deliveries`, `dead_letter`, `leases_durable`)
go here too.

```json
{ "ttl_ms": 60000, "cap_records": 1000000, "discard": "old", "durable": true, "priority": 10 }
```

A queue (with `discard:"reject"` so unconsumed work fails loudly rather than dropping):

```json
{ "type": "queue", "durable": true, "discard": "reject",
  "lease_ms": 30000, "claim_jitter_ms": 0, "max_deliveries": 5, "dead_letter": "jobs.dlq" }
```

**Behavior**
- Box absent → created with given config merged over defaults → `201`.
- Box exists, identical config → no-op → `200`.
- Box exists, different config → updated → `200`. Changes apply going forward; they never
  rewrite stored records, but a tightened `cap`/`ttl` makes existing records eligible for
  lazy eviction/expiry. **`type` is the one immutable field**: a `PUT` that changes `type`
  on an existing box (`log`→`queue` or `queue`→`log`) returns `409 box_exists_incompatible`.
  Every other field is mutable in `/v0` (including the queue tuning fields, applied going
  forward).

**Response** (`201` / `200`)

```json
{
  "box": "jobs",
  "created": true,
  "config": { "type": "log", "ttl_ms": 60000, "cap_records": 1000000, "cap_bytes": 0,
              "discard": "old", "durable": true, "durability": "fsync", "priority": 10,
              "auto_priority": true, "auto_create": true,
              "idempotency_window_ms": 120000, "dedupe_node": true,
              "lease_ms": 30000, "claim_jitter_ms": 0, "max_deliveries": 0,
              "dead_letter": null, "leases_durable": false },
  "performance": { "server_total_ms": 0.22 }
}
```

`created` is `true` only when this call brought the box into existence. `config` always
echoes the full object including the queue fields (inert on a `"log"` box).

**Errors** — `400 invalid_request` (bad name/field/value; e.g. `dead_letter` == this box);
`409 box_exists_incompatible` (changed `type`, or other incompatible change).

### 1.2 Get box state — `GET /v0/boxes/:box`

Read current state: head seq, earliest retained seq, count, byte size, config. The cheap
"where am I / how much lag" call. By default it **bumps the box's auto-priority recency
clock**.

**Query** — `touch` (bool, default `true`): if `false`, do not update auto-priority recency
(for monitoring scrapes that shouldn't make a box look "hot").

**Response** (`200`)

```json
{
  "box": "jobs",
  "type": "log",
  "head_seq": 480231,
  "earliest_seq": 479101,
  "next_seq": 480232,
  "count": 1130,
  "bytes": 2310912,
  "config": { "...": "..." },
  "effective_priority": 500,
  "last_write_ts": 1748450000123,
  "last_read_ts": 1748450009000,
  "performance": { "server_total_ms": 0.05 }
}
```

A **queue** box additionally returns a `queue` sub-object (§10.7) and `type:"queue"`:

```json
{
  "box": "jobs",
  "type": "queue",
  "head_seq": 480231, "earliest_seq": 479101, "next_seq": 480232,
  "count": 1130, "bytes": 2310912,
  "config": { "type": "queue", "lease_ms": 30000, "...": "..." },
  "queue": { "ready": 842, "in_flight": 288, "dead_lettered": 4 },
  "effective_priority": 500,
  "last_write_ts": 1748450000123, "last_read_ts": 1748450009000,
  "performance": { "server_total_ms": 0.06 }
}
```

| Field | Meaning |
|---|---|
| `type` | `"log"` (default, may be omitted) or `"queue"`. |
| `head_seq` | Highest assigned seq (log end). `0` for a fresh empty box. |
| `earliest_seq` | Seq of the first currently-live record — not evicted, not TTL-expired, **not deleted** (log start). If the box is empty, `earliest_seq = head_seq + 1`. A consumer whose `from_seq + 1 < earliest_seq` has fallen below the floor; it receives a tombstone only if `from_seq + 1 < evict_floor` (involuntary cap/TTL loss), otherwise the cursor advances silently past deleted seqs. |
| `next_seq` | Seq the next append will receive (`head_seq + 1`). A handy "tail" cursor: read from `next_seq - 1` to get only new records. |
| `count`, `bytes` | Currently retained records and payload bytes (approximate under lazy eviction). |
| `effective_priority` | The priority the scheduler is using right now (manual, or auto-derived). |
| `last_write_ts` / `last_read_ts` | Recency clocks (ms); `null` if never. |
| `queue` | **Queue only.** `{ ready, in_flight, dead_lettered }` counters (§10.7). Absent on a `"log"` box. |

**Errors** — `404 box_not_found` (state read never auto-creates).

### 1.3 List boxes — `GET /v0/boxes`

Enumerate boxes with summary state. Opaque-cursor paginated.

**Query** — `prefix` (string), `page_size` (default 100, max 1000), `cursor` (opaque),
`touch` (bool, default `false` — listing does not bump auto-priority).

**Response** (`200`)

```json
{
  "boxes": [
    { "box": "jobs",   "head_seq": 480231, "earliest_seq": 479101, "count": 1130, "bytes": 2310912, "durable": true,  "effective_priority": 500 },
    { "box": "events", "head_seq": 99,     "earliest_seq": 1,      "count": 99,   "bytes": 8112,    "durable": false, "effective_priority": 3 }
  ],
  "next_cursor": "eyJhZnRlciI6ImpvYnMifQ==",
  "performance": { "server_total_ms": 0.18 }
}
```

`next_cursor` is omitted on the final page.

**Errors** — `400 invalid_request` (malformed/corrupt cursor).

### 1.4 Delete a box — `DELETE /v0/boxes/:box`

Permanently remove a box and all its records, dedupe state, the per-box tag index, and
routers referencing it. Idempotent. Irreversible.

**Query** — `if_empty` (bool, default `false`): if `true`, delete only when `count == 0`,
else `409 box_not_empty`.

**Response** (`200`)

```json
{ "box": "jobs", "deleted": true,
  "routers_removed": ["jobs->audit", "jobs->mirror"],
  "performance": { "server_total_ms": 0.31 } }
```

Deleting an absent box returns `200` with `"deleted": false` and empty `routers_removed`. A
later lazy-create makes a **new, empty** box (seq restarts); stale consumers detect this via
the `recreated` tombstone (§4.4 / DESIGN §5.5).

**Errors** — `409 box_not_empty`.

---

## 2. Write — `POST /v0/boxes/:box`

Append one or many records. The server assigns `seq`s and returns them. The single write
endpoint also carries inline config (applied only on lazy create) so the common case needs
no separate configure call.

**Request body**

```json
{
  "records": [
    { "data": { "type": "resize", "url": "s3://b/a.png", "w": 256 },
      "tag": "tenant42:job-9001", "node": "worker-eu-1", "meta": { "trace": "abc123" } },
    { "data": "raw-opaque-or-base64", "tag": "tenant42:job-9002" }
  ],
  "node": "worker-eu-1",
  "idempotency_key": "client-batch-7f3a",
  "create": true,
  "config": { "ttl_ms": 60000, "cap_records": 1000000 },
  "disable_backpressure": false
}
```

| Field | Type | Req? | Meaning |
|---|---|---|---|
| `records` | array | yes | 1..=`MAX_BATCH_RECORDS`. Seqs assigned in array order, contiguously. |
| `records[].data` | any JSON | yes | Opaque payload (may be `null`). |
| `records[].tag` | string | no | Match key for deletion (§5). ≤ `MAX_TAG_BYTES`. |
| `records[].node` | string | no | Per-record origin node; overrides batch-level `node`. ≤ `MAX_NODE_BYTES`. |
| `records[].meta` | object | no | Small headers, returned verbatim. ≤ `MAX_META_BYTES`, ≤ 64 keys. |
| `node` | string | no | Batch-level default origin node (the common one-writer case). |
| `idempotency_key` | string | no | Dedupe key (§0.8), scoped to this box. ≤ 256 chars. |
| `create` | bool | no | Overrides the box's `auto_create` for this write only. `true` ⇒ auto-create if absent; `false` ⇒ `404 box_not_found` if absent (Redis `NOMKSTREAM`, prevents typo-boxes). Default = box's `auto_create`. |
| `config` | Box config | no | Applied **only if this write creates the box**. Ignored if the box exists (config changes on an existing box go through `PUT`). |
| `disable_backpressure` | bool | no | If `true`, opt out of `429` for this write; server may queue instead. Default `false`. |

**Behavior**
- A write is **atomic**: all N records commit with contiguous seqs, or none do. No partial
  append.
- `node` is recorded as `$node`; on later reads/SSE by that same node these records are
  filtered out (§4.5).
- Durable boxes: response held until `fsync` completes. Non-durable: acked on buffered
  append.
- **Full box:** `discard:"old"` evicts oldest and the write succeeds (eviction may later
  surface as a tombstone to lagging consumers). `discard:"reject"` ⇒ `422 box_full`,
  **nothing appended** (all-or-nothing), writer learns synchronously (never ack-then-drop).

**Response** (`200`, or `201` if this write created the box)

```json
{
  "box": "jobs",
  "first_seq": 480232, "last_seq": 480234,
  "seqs": [480232, 480233, 480234],
  "head_seq": 480234, "count": 3,
  "created": false, "deduped": false,
  "performance": { "server_total_ms": 0.62, "wal_append_ms": 0.10, "fsync_ms": 0.39 }
}
```

`seqs` may be suppressed with `?return_seqs=false` for huge batches (then only
`first_seq`/`last_seq` are returned). `deduped: true` means an `idempotency_key` matched a
prior in-window write; `seqs` are the original ones and no new append happened.

**Batching limits** (documented hard limits — design within them)

| Limit | Default | Config var |
|---|---|---|
| Max records per write | `10000` | `STREAMS_MAX_BATCH_RECORDS` |
| Max single record `data`+`meta` | `1 MiB` | `STREAMS_MAX_RECORD_BYTES` |
| Max total request body | `64 MiB` | `STREAMS_MAX_BODY_BYTES` |
| Max `meta` per record | `16 KiB`, ≤ 64 keys | `STREAMS_MAX_META_BYTES` |
| Max `tag` length | `256` bytes | `STREAMS_MAX_TAG_BYTES` |
| Max `node` length | `128` bytes | `STREAMS_MAX_NODE_BYTES` |

**Errors** — `400 invalid_request` / `batch_too_large` / `record_too_large`;
`404 box_not_found` (`create:false` and absent); `413 payload_too_large`;
`415 unsupported_media_type`; `422 box_full`; `429 throttled`.

---

## 3. Read difference (getDifference) — `POST /v0/boxes/:box/diff`

The core consume operation. Given a cursor `from_seq`, return the records after it, bounded
to a batch, with a continuation cursor and any tombstone. Skips TTL-expired, deleted, and
own-node records at read time. Bumps auto-priority recency. `POST` (not `GET`)
because the operation is described by the JSON body; it is read-only and safe to retry.

**Request body**

```json
{
  "from_seq": 479100,
  "limit": 500,
  "node": "worker-eu-1",
  "include_tags": false,
  "include_meta": true,
  "wait_ms": 0
}
```

| Field | Type | Default | Meaning |
|---|---|---|---|
| `from_seq` | `u64` | `0` | Exclusive lower bound: return records with `$seq > from_seq`. `0` = from earliest retained. To tail, pass the current `head_seq`. |
| `limit` | `u32` | `256` | Max records this call. Clamped to `STREAMS_MAX_LIMIT` (`1000`), not rejected. `0` ⇒ default. |
| `node` | string \| array | none | Node loop-prevention filter (§4.5): records whose `$node` ∈ this set are omitted (but still advance the cursor). |
| `include_tags` | bool | `false` | Include `$tag` on each record. |
| `include_meta` | bool | `true` | Include each record's `meta`. |
| `wait_ms` | `u32` | `0` | Long-poll: if nothing is available, block up to this many ms for new records. Clamped to `30000`. SSE is preferred for true streaming; this is the XREAD `BLOCK` analog. |

**Response** (`200`)

```json
{
  "box": "jobs",
  "records": [
    { "$seq": 479101, "$ts": 1748450001000, "$node": "worker-us-2", "$tag": "tenant42:job-8800",
      "data": { "type": "resize", "url": "s3://b/x.png" }, "meta": { "trace": "z9" } },
    { "$seq": 479102, "$ts": 1748450001050,
      "data": { "type": "thumbnail", "url": "s3://b/y.png" } }
  ],
  "next_from_seq": 479102,
  "head_seq": 480234,
  "earliest_seq": 479101,
  "caught_up": false,
  "tombstone": null,
  "lag": 1132,
  "performance": { "server_total_ms": 0.30, "records_scanned": 134 }
}
```

| Field | Meaning |
|---|---|
| `records` | Up to `limit` records with `$seq > from_seq`, ascending, after skipping TTL-expired, deleted, and own-node records. `$tag` only if `include_tags`; `meta` only if `include_meta`. |
| `next_from_seq` | Pass back as `from_seq`. Equals the `$seq` of the last **examined** record (filtered records still advance it), so filtered records are never re-scanned. |
| `head_seq` / `earliest_seq` | Log end / retained floor — for lag math and fall-off detection. |
| `caught_up` | `true` when `next_from_seq == head_seq`. The reliable "no more right now" signal. |
| `tombstone` | `null`, or a gap marker (§3.2). |
| `lag` | `head_seq - next_from_seq` — records still behind the cursor. |

### 3.1 The cursor is the ack (ack-all / cursor-advance)

The default consume model is **cursor-advance = ack-all** (Kafka offset, NATS AckAll):
advancing your stored `from_seq` past seq N acks 1..N. The client owns its cursor; the
server keeps **no** per-consumer state on this path. Per-message explicit-ack / lease /
heartbeat (the BullMQ stalled-job model) is not in `/v0`; it is a planned higher-level mode
(ROADMAP) layered on tags + a side in-flight box, kept out of the core to preserve the
stateless-log primitive.

### 3.2 Skipped records still advance the cursor

Node-filtered and deleted records (and TTL-expired ones) are **omitted from `records`**
but `next_from_seq` advances **past** them. Otherwise a consumer reading a box full of its
own (node-filtered) events would loop forever. So `records.length` may be less than the
number of seqs traversed, and may be `0` while `next_from_seq` advanced. **`caught_up` is
the reliable "no more" signal — never `records.length == 0`.**

### 3.3 Tombstone / gap markers (never silent loss)

If `from_seq + 1 < evict_floor` — the cursor fell below the **involuntary** floor because
records were evicted (cap) or expired (TTL) — the response returns a `tombstone` and
resumes from `earliest_seq`. (A cursor below `earliest_seq` but at/above `evict_floor` fell
into a purely-**deleted** gap: that is silent, `tombstone: null`, the cursor simply advances
past the deleted seqs. `evict_floor <= earliest_seq` always.)

```json
{
  "box": "jobs",
  "records": [ { "$seq": 479101, "...": "..." } ],
  "tombstone": {
    "gap_from": 478501,
    "gap_to": 479100,
    "reason": "cap",
    "missed_estimate": 600,
    "earliest_seq": 479101,
    "head_seq": 480234
  },
  "next_from_seq": 479200,
  "head_seq": 480234,
  "earliest_seq": 479101,
  "caught_up": false
}
```

| `tombstone` field | Meaning |
|---|---|
| `gap_from` | First missing seq (= stale `from_seq + 1`). |
| `gap_to` | Last missing seq (= `earliest_seq - 1`). The lost range `[gap_from, gap_to]` is inclusive both ends. |
| `reason` | `"cap"` (evicted for capacity), `"ttl"` (expired), `"mixed"` (both), or `"recreated"` (box was deleted+recreated). Best-effort/informational; the range is authoritative. |
| `missed_estimate` | Approximate count of dropped records (approximate because eviction is segment-granular). |
| `earliest_seq` / `head_seq` | Current watermarks, echoed for convenience. |

`records` begin at `earliest_seq`; `next_from_seq` continues normally. The tombstone is a
`200` in-band signal, never an error. At most one tombstone per response (the gap is always
one contiguous range because `earliest_seq` is monotonic). The same shape is emitted by SSE
as `event: tombstone` (§7).

> **Deletion is silent and permanent:** a delete (§5) removes records that exist at call
> time, effective immediately for every reader, and **never** emits a tombstone — it is a
> voluntary, content/seq-based removal, not capacity loss. The reader's cursor simply
> advances past the deleted seqs. A lagging consumer cannot "miss a deletion" because the
> record is gone for all readers at once; it only ever sees a tombstone for involuntary
> cap/TTL loss (`reason ∈ cap|ttl|mixed`), tracked by `evict_floor`, never by deletes.

**Errors** — `400 invalid_request` (type errors only; `wait_ms`/`limit` over max are
clamped, not errored); `404 box_not_found` (diff never auto-creates); `429 throttled`.

---

## 4. Node loop-prevention (applies to diff §3 and watch §7)

A record is stamped with the writer's `$node`. When a reader presents the **same** `node`
value on `/diff` or `/watch`, records whose `$node` equals it are **filtered out** — a node
never receives back events it produced. Combined with routers (§6) preserving `$node` across
forwards, this makes symmetric multi-master fan-out safe: N nodes all write to and watch a
shared topology, each sees only the *others'* records, with no echo and no infinite loop.

- Matching is byte-exact equality only (no prefix); a node id is an opaque identity token.
- A read MAY pass multiple node ids (`"node": ["a","b"]`) to filter several of its own
  identities; semantics are "drop if `$node` ∈ set."
- Filtering happens **after** retention/TTL but is **content-based and intentional**, so it
  is **silent** (no tombstone — see DESIGN §5.4). Dropped seqs are simply absent; the cursor
  advances past them (§3.2).
- Disable per-box with `dedupe_node: false` (§0.10) if you genuinely want echoes.

---

## 5. Deletion (permanent, point-in-time) — `POST /v0/boxes/:box/delete`

Permanently remove records by **seq range** (`before_seq`) and/or by **tag** match
(`match`). Deletion is:

- **Permanent** — deleted records are gone for good; there is no un-delete in `/v0`.
- **Effective immediately** — invisible to all reads at once (diff, box state `count`/
  `bytes`, and SSE); the reader's cursor simply advances past the deleted seqs.
- **Asynchronous, no compaction / no reclaim** — records are logically gone instantly (the
  work runs off the call path), but a deleted record **stays on disk, just marked**: there is
  no compaction and no per-record disk reclaim. In memory the payload/tag is freed and
  front-of-log slots pop as the prefix becomes fully dead; on disk a record sealed into a
  segment has its **delete-flag byte flipped in place** (the WAL stays append-only — a `Delete`
  frame is appended, never mutated). The only space released is a **whole segment dropped** when
  a delete clears it entirely.
- **Silent** — a delete **never** produces a tombstone. Tombstones are reserved for
  **involuntary** cap/TTL loss (§3.3).
- **Point-in-time** — it is **not** a standing filter. It only removes records present at
  call time; future records (even with a matching tag) are never affected.

A delete advances the box's `earliest_seq` (first live seq) but **not** its `evict_floor`,
so reading across a purely-deleted gap is silent (§3.3).

**Durable for already-sealed records.** A delete of a record that has already been sealed into an
immutable segment is made durable **in the segment itself**: it flips a single trailing
**delete-flag byte** in the segment frame **in place** (then fsyncs) — the WAL stays append-only (a
`Delete` frame is still appended, never mutated). Because the on-disk flag is an independent
witness, a sealed-record deletion **survives a checkpoint that trims the WAL `Delete` frame**: on
restart the engine reads the on-disk flags back and re-marks those records deleted. The flip is
crash-safe (a sector-atomic byte; a mid-flip crash never corrupts framing, skips a live record, or
resurrects a deleted one). A delete that clears **every** record of a segment drops the whole
segment in one op instead of flagging each record (DESIGN §7.4).

**Request body** — at least one of `before_seq` / `match` is **required** (else `400
invalid_request`); supply both to AND them. A snapshot/compaction by seq:

```json
{ "before_seq": 480001 }
```

| Field | Type | Meaning |
|---|---|---|
| `before_seq` | `u64` | Delete every record with `$seq < before_seq` (snapshot / compaction by seq). |
| `match` | predicate | `["tag","Eq","X"]` exact match, or `["tag","Glob","X*"]` **trailing-prefix only** (a literal prefix followed by a single `*`; full glob/regex is deliberately excluded so a delete is a point lookup or a bounded range scan over the per-box tag index). A bare string is shorthand for `Eq`: `"match": "tenant42:job-9001"` == `["tag","Eq","tenant42:job-9001"]`. |

**Semantics by combination**

| Body | Effect |
|---|---|
| `before_seq` only | Delete every record with `$seq < before_seq` (SNAPSHOT / compaction). |
| `match` only | Delete every **existing** record whose tag matches, bounded by the current head at call time (bound = `head_seq + 1`). Point-in-time: future records with that tag are **not** deleted (e.g. revoke a kicked user's chat: `match ["tag","Glob","chat-42:*"]`). |
| `match` + `before_seq` | Delete records with `$seq < before_seq` **AND** tag matches (e.g. publish v2 of a message then delete its prior versions, keeping the new one: `match ["tag","Eq","msg-123"]`, `before_seq = <seq of v2>`). |

Records with **no tag** are never matched by `match`. A tag delete uses the per-box tag
index (exact = point lookup, prefix = range scan); it never scans the whole log.

**Response** (`200`)

```json
{ "box": "jobs",
  "deleted": 14,
  "earliest_seq": 480001,
  "head_seq": 480234,
  "count": 234,
  "bytes": 478820,
  "performance": { "server_total_ms": 0.12 } }
```

| Field | Meaning |
|---|---|
| `deleted` | Count of records removed by this call. |
| `earliest_seq` | New first live seq (advanced past any deleted prefix). |
| `head_seq` / `count` / `bytes` | Box state after the delete (`count`/`bytes` already exclude the deleted records). |

**Errors** — `400 invalid_request` (neither `before_seq` nor `match` supplied; bad tuple;
`match` op not in `{Eq, Glob}`; `Glob` without a trailing `*`); `404 box_not_found`.

---

## 6. Routers (fan-out)

A **router** is a server-side forwarding rule: every record appended to `source` is also
appended to `dest`. Routers make pub/sub fan-out safe across N symmetric nodes because
forwarded records keep their origin `$node`, and node loop-prevention stops a node from
receiving back what it produced. Routers live in their own namespace (`/v0/routers`).
Forwarding is **async** (off the source write/ack path — a write to `source` acks immediately
and a background per-router worker forwards) and **derived** (the forwarded copies are **not
separately WAL-logged**, so one source append is **one WAL write regardless of fan-out**; the
copies are re-derived on recovery by replaying from a **durable per-router cursor**). It is
**at-least-once, per-source FIFO** (a crash between "appended to dest" and "advanced router
cursor" can re-forward; consumers must be idempotent — see DESIGN §6). A derived `dest` is
**single-source**: a second router with a *different* `source` into the same `dest` is rejected
`409 box_exists_incompatible` with `error.detail.reason: "router_dest_fan_in"`.

### 6.1 Create / configure — `PUT /v0/routers/:router`

**Path** — `:router`, same charset as box names. Convention default name `"<source>-><dest>"`.

**Request body**

```json
{ "source": "jobs", "dest": "audit",
  "preserve_node": true, "preserve_tag": true,
  "create_dest": true, "filter": null, "allow_cycle": false }
```

| Field | Type | Req? | Default | Meaning |
|---|---|---|---|---|
| `source` | string | yes | — | Source box; records here are forwarded. |
| `dest` | string | yes | — | Destination box; must differ from `source`. |
| `preserve_node` | bool | no | `true` | Keep original `$node` on forwarded records (required for loop prevention across the fan-out). `false` clears it. |
| `preserve_tag` | bool | no | `true` | Keep `$tag` (so a tag delete can be applied at the dest too). |
| `create_dest` | bool | no | `true` | Auto-create `dest` if absent. `false` ⇒ `404` if missing. |
| `filter` | tuple \| null | no | `null` | Optional forward-time filter (same tuple language as §5), e.g. `["tag","Glob","public:*"]`. `null` = forward all. |
| `allow_cycle` | bool | no | `false` | If `false`, creating a router that would introduce a directed cycle is rejected `409 router_cycle` (DAG-by-default). If `true`, the route is permitted and runtime hop-cap loop-breaking applies instead (for intentional A↔B multi-master). |

**Behavior**
- Forwarding runs **asynchronously** off the `source` write/ack path (driven by the durable
  per-router cursor), not inline at append. The forwarded copy gets its own fresh `$seq` and
  `$ts` in `dest` (an independent log); `$node`/`$tag`/`data`/`meta` carry through verbatim
  (subject to `preserve_*`). Optional reserved meta `$src_box`/`$src_seq` aid traceability (off
  by default).
- `dest`'s own config governs the forward: `discard:"old"` evicts to make room; `discard:"reject"`
  rejects the forward, the router **does not advance** its cursor and retries with backoff
  (backpressure up the route). See DESIGN §6.4.
- Idempotent: identical `PUT` ⇒ `200`; changed ⇒ `200`; new ⇒ `201`.

**Response** (`201` / `200`)

```json
{ "router": "jobs->audit", "created": true,
  "source": "jobs", "dest": "audit",
  "preserve_node": true, "preserve_tag": true, "filter": null, "allow_cycle": false,
  "performance": { "server_total_ms": 0.20 } }
```

**Errors** — `400 invalid_request` (missing `source`/`dest`, `source == dest`, bad filter);
`404 box_not_found`; `409 router_cycle` (`error.detail.cycle: ["A","B","A"]`);
`409 box_exists_incompatible` with `error.detail.reason: "router_dest_fan_in"` (a second router
with a different `source` into a `dest` that is already a derived destination — single-source).

### 6.2 Get a router — `GET /v0/routers/:router`

```json
{ "router": "jobs->audit", "source": "jobs", "dest": "audit",
  "preserve_node": true, "preserve_tag": true, "filter": null, "allow_cycle": false,
  "forwarded_total": 480231, "performance": { "server_total_ms": 0.04 } }
```
`404 router_not_found` if absent.

### 6.3 List routers — `GET /v0/routers`

**Query** — `prefix`, `source`, `dest`, `page_size` (default 100, max 1000), `cursor`.

```json
{ "routers": [
    { "router": "jobs->audit",  "source": "jobs", "dest": "audit",  "forwarded_total": 480231 },
    { "router": "jobs->mirror", "source": "jobs", "dest": "mirror", "forwarded_total": 480231 }
  ],
  "next_cursor": "eyJhZnRlciI6ImpvYnMtPmF1ZGl0In0=",
  "performance": { "server_total_ms": 0.09 } }
```
`next_cursor` omitted on last page.

### 6.4 Delete a router — `DELETE /v0/routers/:router`

Idempotent. Stops forwarding immediately; already-forwarded records in `dest` are untouched
(forwarding is a copy, not a link).

```json
{ "router": "jobs->audit", "deleted": true, "performance": { "server_total_ms": 0.06 } }
```
`"deleted": false` if absent. Deleting a **box** (§1.4) cascades to every router with that
box as `source` or `dest`.

---

## 7. Multiplexed SSE watch

Watch **many boxes** over a single long-lived connection. The server pushes new records,
tombstone/gap events, and periodic heartbeats, all resumable via a composite cursor.

### 7.1 Two-step shape — POST to create, GET to stream

```
POST /v0/watch          -> creates a watch session, returns a wid + stream_url
GET  /v0/watch/:wid     -> opens the SSE stream (text/event-stream)
```

A multiplexed watch can name dozens of boxes with per-box cursors and options — too much
for a query string, and browser `EventSource` is GET-only with no body/headers. So the
**POST** carries the full JSON subscription (and returns `400`/`404` *before* any stream is
open, while the client can still read the error body), and the **GET** is a tiny,
`EventSource`-compatible URL. A session holds the subscription definition plus the
last-delivered cursor per box (so GET reconnects resume exactly) and is **reclaimed** after
`session_ttl_ms` (default 300000) of no active GET. The idle-session GC runs opportunistically
on every `POST /v0/watch` and stream-open: a session with no live stream whose last access is
older than the TTL is removed, so an abandoned session cannot pin a `STREAMS_MAX_WATCH_SESSIONS`
slot until restart. A session with an open stream is never reclaimed (its cursor map is in use).

**Auth & the `wid` capability.** The **POST** is authenticated normally
(`Authorization: Bearer`) and additionally enforces the key's **box-name prefix allowlist**
against every box named in the body — a prefix-limited key cannot watch a box outside its
allowlist (`403 forbidden`). The returned `wid` is an **unguessable random bearer capability**
(≥128 bits of entropy, base64url, e.g. `wid_BuRguGorNdVFWNQULz-rrw`) — it is not a guessable
counter, and the GET stream is found by the `wid` alone (a browser `EventSource`-compatible URL).
**When auth is enabled, the `wid` alone is NOT sufficient to open the stream**: the GET must
present the **creating key** (via the `Authorization: Bearer` header or the dev-only `?token=`
fallback below), and a *different valid* key is rejected just like an invalid one. This is
defense-in-depth: a `wid` that leaks via logs/history cannot be opened by a holder who does not
also have the key. In dev mode (no auth) the `wid` alone opens the stream. The session is bound
to the creating key's scope so the stream can never exceed it.

> **`?token=<key>` is a documented dev-only fallback** for browser `EventSource` (which cannot
> send custom headers) on the **SSE stream GETs only** (`/v0/watch/:wid`, `/v0/boxes/:q/work`);
> it is **never** accepted on ordinary data/control-plane routes (use the header there). A query
> string leaks via server logs, browser history, and proxies, so **prefer the
> `Authorization: Bearer` header**, and never put a long-lived api key in a URL in production.
> The parameter is parsed with a real `x-www-form-urlencoded` decoder (percent-escapes and `+`
> are decoded; a duplicated `token=` takes the first occurrence).

### 7.2 POST /v0/watch — create session

```json
{
  "node": "worker-eu-1",
  "boxes": {
    "jobs":   { "from_seq": 4096 },
    "events": { "tail": true },
    "audit":  { "from_seq": 1 }
  },
  "limit": 256,
  "max_batch_bytes": 262144,
  "heartbeat_ms": 15000,
  "include_meta": true,
  "include_tags": false,
  "include_data": true,
  "consistency": "eventual"
}
```

| Field | Meaning | Default |
|---|---|---|
| `node` | Loop-prevention filter applied to all watched boxes (this node never receives its own records). Omit for none. | none |
| `boxes` | Map of `box → per-box options`. The key is the box (keeps cursors unambiguous and doubles as the resume map). Up to `STREAMS_MAX_WATCH_BOXES` (default 256). | required, ≥1 |
| `boxes[b].from_seq` | Deliver records with `$seq > from_seq`. `0` = from earliest. | `0` |
| `boxes[b].tail` | If `true`, ignore `from_seq` and start at the box's current head (only records after subscribe; the SSE analog of Redis `XREAD $`). | `false` |
| `limit` | Max records per `record` frame (per box, per flush). | `256` |
| `max_batch_bytes` | **Enforced** soft byte budget for a single `record` frame: the per-box read stops once the batch reaches this many payload bytes (at least one record is always delivered), so a frame cannot balloon to `limit`×record-cap. `0` ⇒ server default (1 MiB), clamped to 8 MiB. | `262144` (256 KiB) |
| `heartbeat_ms` | Heartbeat interval. Clamped to `[1000, 60000]`. | `15000` |
| `include_meta` | Include record `meta` in frames. | `true` |
| `include_tags` | Include `$tag` in frames. | `false` |
| `include_data` | If `false`, frames carry only `$seq`/`$ts`/`$tag`/`$node` metadata, not `data` (lightweight tailing). | `true` |
| `consistency` | `eventual` (push as soon as in WAL buffer) or `strong` (push only after fsync/commit). | `eventual` |

**Response** (`200`)

```json
{
  "wid": "wid_BuRguGorNdVFWNQULz-rrw",
  "stream_url": "/v0/watch/wid_BuRguGorNdVFWNQULz-rrw",
  "session_ttl_ms": 300000,
  "boxes": {
    "jobs":   { "from_seq": 4096,  "head_seq": 5210,  "earliest_seq": 3001 },
    "events": { "from_seq": 88123, "head_seq": 88123, "earliest_seq": 80000 },
    "audit":  { "from_seq": 1,     "head_seq": 990,   "earliest_seq": 1 }
  },
  "performance": { "server_total_ms": 0.7 }
}
```

Per-box `head_seq`/`earliest_seq` let the client compute lag and see, before streaming,
whether a cursor has already fallen off the start (it gets a tombstone on connect if so).

**Errors** — `400 invalid_request` (malformed `boxes`, too many boxes); `404 box_not_found`
(unknown box, unless `?lenient=true`, which simply **drops** unknown boxes — they are absent
from the response `boxes` map; there is no separate warning frame).

### 7.3 GET /v0/watch/:wid — open the stream

```
GET /v0/watch/wid_BuRguGorNdVFWNQULz-rrw HTTP/1.1
Accept: text/event-stream
Last-Event-ID: eyJqb2JzIjo1MjEwLCJldmVudHMiOjg4MTMwfQ
```

Response headers (tuned for low latency through proxies):

```
HTTP/1.1 200 OK
Content-Type: text/event-stream; charset=utf-8
Cache-Control: no-store
Connection: keep-alive
X-Accel-Buffering: no
```

`X-Accel-Buffering: no` disables nginx-style proxy buffering; the server flushes after every
frame and sets `TCP_NODELAY` to hit the 1–5 ms target.

### 7.4 Resume via composite cursor (`Last-Event-ID`)

Every data-bearing frame carries an `id:` encoding the **entire per-box cursor map** at that
moment, as base64url-encoded JSON:

```
id = base64url( {"jobs":5210,"events":88130,"audit":990} )
```

On reconnect, resolution order: (1) the server-side session cursors (authoritative, survive a
lost `Last-Event-ID`); (2) the `Last-Event-ID` header, if present, used only to **rewind**
the session to that exact map (never advance past it — protects the gap between "server
flushed" and "client processed"); (3) the session's initial `from_seq`/`tail` if neither.
Because the id is a full map, one reconnect restores all per-box positions atomically. For
very large box sets (>64) the server may emit an opaque session-checkpoint token instead of
the full map; clients treat `id` as opaque either way.

### 7.5 Frame types

`retry:` is sent once at open (deliberate 2 s backoff, not the EventSource default 3 s):

```
retry: 2000
```

**`event: record`** — new records for one box, batched. `id` is the post-batch composite cursor.

```
id: eyJqb2JzIjo0MDk4fQ
event: record
data: {"box":"jobs","records":[{"$seq":4097,"$ts":1748467200111,"$tag":"job:render","$node":"node-B","data":{"url":"..."}},{"$seq":4098,"$ts":1748467200119,"$node":"node-C","data":{"id":42}}],"from_seq":4096,"to_seq":4098,"head_seq":5210}
```

Payload: `box`, `records[]`, and a `from_seq`/`to_seq`/`head_seq` triple per batch (lag =
`head_seq - to_seq`). `$tag` present only if `include_tags`; `$node` present when set;
`data` omitted per-record if `include_data:false`.

**`event: tombstone`** — explicit, never-silent loss. Emitted whenever a gap crosses *this
consumer's* cursor for a box.

```
id: eyJldmVudHMiOjgzMDAwfQ
event: tombstone
data: {"box":"events","reason":"cap","gap_from":80000,"gap_to":83000,"earliest_seq":83001,"head_seq":88130}
```

`reason` ∈ `cap` | `ttl` | `mixed` | `recreated` | `from_seq_too_old`. The `from_seq_too_old`
value is emitted **immediately on connect** when the requested `from_seq + 1 < earliest_seq`
(the SSE expression of Kafka `OffsetOutOfRange`). The frame's `id` already advances the box
cursor to `gap_to`, so a resume after it is correct.

**`event: caught-up`** — the box is drained to head; the client is now live (one per box,
re-emitted on each backlog→tailing transition).

```
id: eyJqb2JzIjo1MjEwfQ
event: caught-up
data: {"box":"jobs","head_seq":5210}
```

**Heartbeat** — a bare SSE comment, no `id:` (never perturbs the resume cursor), trailing
epoch-ms for liveness/skew. Suppressed when real data went out within the window.

```
: hb 1748467205000
```

**`event: box-deleted`** — the box was deleted while watched; terminal for that box only,
the rest of the stream continues.

```
id: eyJqb2JzLmRscSI6MTJ9
event: box-deleted
data: {"box":"jobs.dlq","head_seq":12,"reason":"deleted"}
```

**`event: error`** — a per-stream problem with an HTTP-aligned `code`. `code: 429` is
advisory (the stream stays open but the named boxes are paced); `code: 410` (session GC'd) is
terminal (re-POST).

```
event: error
data: {"code":429,"error":"watch throttled under CPU pressure","retry_after_ms":1500,"boxes":["events"]}
```

### 7.6 Heartbeats, liveness, backpressure

- **Heartbeat cadence:** emitted when the connection has been idle `heartbeat_ms`; suppressed
  if real data went out in the window. Comment frames don't disturb the event stream or `id`.
- **Client liveness watchdog:** arm at `2 × heartbeat_ms + slack` (~35 s for the 15 s
  default); any frame resets it; on timeout, reconnect with `Last-Event-ID`.
- **Slow consumers** (see ARCHITECTURE §5): each connection has a bounded outbound queue.
  While it can't drain, records coalesce into larger/fewer batches. If it stays full past
  `buffer_grace_ms` (5 s), a lossy box degrades to a `tombstone(from_seq_too_old)` on resume;
  a durable/unbounded box simply lags and replays. Past `slow_consumer_timeout` (30 s) the
  server sends a final `error` frame and closes; the session survives `session_ttl_ms` so the
  client resumes with no loss beyond the box's own cap/TTL.

**Errors at establishment** — `200` (stream opened); `400 invalid_request` (bad/expired wid
or boxes); `401 unauthorized` — when the session was created **with auth enabled** it is
bound to the creating key, so the SSE GET must present that **same** bearer (via the
`Authorization` header **or** the dev-only `?token=` on the GET); a wrong key *or* **no
bearer at all** is rejected (a leaked `wid` alone is not a credential). When the session was
created **without auth** (dev mode) the `wid` alone authorizes and no bearer is needed (the
`EventSource` case). See §7.1. `404 not_found` (wid GC'd or unknown → POST again);
`406 not_acceptable` (Accept not `text/event-stream`); `429 throttled`.

---

## 8. Health / readiness / metrics

These live at the version root (and aliases at the server root for load balancers). They do
not require auth by default (`STREAMS_PROBE_AUTH=true` to require it).

### 8.1 Liveness — `GET /v0/health` (alias `GET /healthz`)

```json
{ "status": "ok", "version": "0.1.0", "uptime_ms": 84012 }
```
`200` always while the process can serve.

### 8.2 Readiness — `GET /v0/ready` (alias `GET /readyz`)

```json
{ "status": "ready", "wal_replay_complete": true, "boxes": 42 }
```
`200 ready` when serving; `503 not_ready` during WAL replay (carries `Retry-After` and
`error.detail.replay_progress` 0.0–1.0); `503 shutting_down` while draining.

### 8.3 Metrics — `GET /v0/metrics`

Prometheus text exposition (`text/plain; version=0.0.4`) by default; `Accept:
application/json` for a JSON snapshot. Exposes process/aggregate gauges (`streams_boxes`,
`streams_boxes_by_class{class=...}`, `streams_routers`, `streams_records_live`,
`streams_bytes_live`, `streams_queue_boxes`, `streams_queue_leases_in_flight`,
`streams_sse_connections`, `streams_watch_sessions`, `streams_ready`,
`streams_recovery_progress`, `streams_uptime_ms`), **per-box gauges**
(`streams_box_head_seq` / `_earliest_seq` / `_records_live` / `_bytes_live` /
`_queue_ready` / `_queue_in_flight`, labelled `{box=...}`; the per-box block is bounded and
sets `streams_box_metrics_truncated` if capped), the real **WAL metrics**
(`streams_wal_frames_total`, `_batches_total`, `_fsyncs_total`, `_bytes_written_total`,
`_rotations_total`, `_queue_depth`, `_queue_depth_peak`, `_submit_full_total`,
`_read_only`), and a **fsync-latency histogram** `streams_wal_fsync_latency_us` (with
`_bucket{le=...}` / `_sum` / `_count`).

```
# HELP streams_box_head_seq Highest assigned seq per box.
# TYPE streams_box_head_seq gauge
streams_box_head_seq{box="jobs"} 480231
streams_box_earliest_seq{box="jobs"} 468188
streams_box_records_live{box="jobs"} 12043
streams_wal_fsyncs_total 88241
streams_wal_fsync_latency_us_bucket{le="500"} 84120
streams_wal_fsync_latency_us_count 88241
```
`200` always (even when not ready — metrics describe the recovering process). (There are no
per-box append/read/eviction/tombstone counters and no scheduler-throttle metric — the
surface is gauges + WAL counters + the fsync histogram above.)

> **Auth.** Unlike `/v0/health` and `/v0/ready` (which stay unauthenticated so a load balancer
> can poll liveness/readiness), `/v0/metrics` exposes operational state (box count, …) and is
> therefore **gated behind auth by default** when keys are configured: it needs a key with the
> `read` scope (a full-access key suffices). In dev mode (no keys) it is open. Set
> `STREAMS_PROBE_AUTH` to additionally require auth on the liveness/readiness probes.

---

## 9. Endpoint index

| Method | Path | Purpose |
|---|---|---|
| `PUT` | `/v0/boxes/:box` | Create/configure box (idempotent upsert) |
| `GET` | `/v0/boxes/:box` | Get box state (head/earliest/count/config) |
| `GET` | `/v0/boxes` | List boxes (opaque-cursor paginated) |
| `DELETE` | `/v0/boxes/:box` | Delete box (cascades routers) |
| `POST` | `/v0/boxes/:box` | Append record(s); returns assigned seqs + head |
| `POST` | `/v0/boxes/:box/diff` | Read difference from cursor (batched + tombstones) |
| `POST` | `/v0/boxes/:box/delete` | Permanently delete records by `before_seq` and/or tag `match` |
| `PUT` | `/v0/routers/:router` | Create/configure router (idempotent upsert) |
| `GET` | `/v0/routers/:router` | Get router |
| `GET` | `/v0/routers` | List routers |
| `DELETE` | `/v0/routers/:router` | Delete router |
| `POST` | `/v0/watch` | Create a multiplexed SSE watch session |
| `GET` | `/v0/watch/:wid` | Open the SSE stream for a session |
| `POST` | `/v0/boxes/:q/claim` | **Queue:** lease up to N claimable jobs to a node (§10.2) |
| `POST` | `/v0/boxes/:q/ack` | **Queue:** complete jobs (ack == permanent delete) (§10.4) |
| `POST` | `/v0/boxes/:q/nack` | **Queue:** release leased jobs for (delayed) reclaim (§10.5) |
| `POST` | `/v0/boxes/:q/extend` | **Queue:** extend lease deadlines (heartbeat) (§10.6) |
| `GET` | `/v0/boxes/:q/work` | **Queue:** SSE auto-claim/push (PUSH mode) (§10.8) |
| `GET` | `/v0/health` (`/healthz`) | Liveness |
| `GET` | `/v0/ready` (`/readyz`) | Readiness (WAL replay / drain aware) |
| `GET` | `/v0/metrics` | Prometheus / JSON metrics |

---

## 10. Queues (lease-based job delivery)

A **queue** is a box created with `type:"queue"` (§0.10). It layers lease-based,
at-least-once job delivery on top of the same persistent log machinery (WAL, recovery,
SSE, priority) — a queue **is** a box, so everything in §1–§7 still applies to it
read-only. Internally a queue is **two logs**: the **jobs log** (the queue itself — the box
you write to with §2) and an append-only **leases log** of lifecycle events; the pending
who-holds-what state is the materialized projection of that leases log (DESIGN §10,
ARCHITECTURE §12). The endpoints below add the lease lifecycle on top.

All §0 conventions apply unchanged: the bearer auth (§0.2), the canonical error envelope
(§0.5), the `performance` block (§0.9), the `$`-metadata convention (§0.4), and write
idempotency (§0.8, on the produce path). Queue lifecycle calls (claim/ack/nack/extend) are
`POST` and carry their parameters in the JSON body.

**A non-queue box rejects every endpoint in this section with `409 not_a_queue`** (so a
typo'd box name or a plain log can never silently swallow a claim). All §10 endpoints
return `404 box_not_found` if the box does not exist (they never auto-create — produce via
§2 / create via §1.1 first).

### 10.1 Model in one paragraph

You **produce** jobs with a normal append (§2 `POST /v0/boxes/:q`). A worker **claims** up
to N jobs (§10.2): the server leases them to that worker's `node`, returning each job's data
plus a `lease_id` and a `deadline`. The worker processes them and **acks** (§10.4 — ack
*is* the permanent delete of the job from the jobs log), **nacks** to release for immediate
or delayed reclaim (§10.5), or **extends** the lease to keep working (§10.6 heartbeat). If a
lease's `deadline` passes with no ack/extend, the job becomes claimable again — the
**visibility timeout** (§10.3), no per-job timers. After `max_deliveries` reclaims without
an ack a job is **dead-lettered** (§10.6). A worker may instead open an SSE **/work** stream
(§10.8) to have jobs auto-claimed and pushed to it (PUSH mode).

Semantics are **at-least-once with idempotent consumers**: a slow-but-alive worker acking
past its deadline can cause a duplicate delivery (inherent and documented). Per-job FIFO is
**not** guaranteed across parallel workers.

### 10.2 Claim jobs — `POST /v0/boxes/:q/claim`

Lease up to `max` claimable jobs to a worker `node`.

**Request body**

```json
{ "node": "worker-eu-1", "max": 16, "lease_ms": 30000 }
```

| Field | Type | Req? | Default | Meaning |
|---|---|---|---|---|
| `node` | string | yes | — | The claiming worker's identity. Recorded as the lease holder; used for `nack`/`extend`/`/work` ownership and for instant release on `/work` disconnect (§10.8). ≤ `MAX_NODE_BYTES`. |
| `max` | `u32` | no | `1` | Max jobs to lease this call. Clamped to `STREAMS_MAX_CLAIM` (`1000`). The response may contain **fewer** (or zero) if the queue has less work available — `count < max` is the reliable "queue (near-)empty" signal, never an error. |
| `lease_ms` | `u64` | no | box `lease_ms` | Lease duration for the jobs claimed by *this* call; overrides the box default. Clamped `[100, 86400000]`. |

A job is **claimable** iff it is not acked (still in the jobs log) and not currently leased
(no active lease, or its lease has expired). Each returned job records a `claimed` event in
the leases log and increments that job's delivery counter (§10.6).

**Coalescing window (`claim_jitter_ms`).** If the box's `claim_jitter_ms > 0`, the claim
**waits up to that window**; the server gathers **all** claimers that arrived during the
window into a cohort and **divides the available jobs evenly** across the whole cohort
(round-robin, proportional to each claimer's `max`) — *not* first-arrival-drains-the-head.
The cohort is served in **one batched coordinator pass** (a single critical section). With
`claim_jitter_ms = 0` (default) a claim is served immediately (greedy, lowest latency). The
available set for a pass is: reclaimed expired-lease seqs (drained first) + fresh jobs (a
claim cursor handing out never-yet-leased seqs). All waiting uses the Clock (no wall-clock
sleep affects correctness).

**Response** (`200`)

```json
{
  "box": "jobs",
  "claimed": [
    { "$seq": 480101, "lease_id": "lease_7f3a9c", "deadline": 1748450039000,
      "$ts": 1748450001000, "$tag": "tenant42:job-8800", "deliveries": 1,
      "data": { "type": "resize", "url": "s3://b/x.png" }, "meta": { "trace": "z9" } },
    { "$seq": 480104, "lease_id": "lease_7f3a9d", "deadline": 1748450039000,
      "$ts": 1748450001050, "deliveries": 2,
      "data": { "type": "thumbnail", "url": "s3://b/y.png" } }
  ],
  "count": 2,
  "ready": 840,
  "performance": { "server_total_ms": 0.42, "throttle_wait_ms": 0.0 }
}
```

| Field | Meaning |
|---|---|
| `claimed[]` | The leased jobs, ascending by `$seq`. Each carries the record's `$seq`/`$ts`/`data` (and `$tag`/`meta` when present — same omit-when-absent rule as §0.4), plus the lease fields below. |
| `claimed[].lease_id` | Opaque lease identity for this delivery (format `lease_<hex>`). **Validate-when-supplied, not strictly required:** ack/nack/extend match on `node`+`seqs` by default, but a worker MAY echo these tokens back in the optional `lease_ids` array (§10.4/§10.5/§10.6) to **fence** stale workers — a token whose lease has since been superseded is rejected and that seq is skipped. Also useful for logging/observability and disambiguating redeliveries. |
| `claimed[].deadline` | Absolute ms epoch when the lease expires if not acked/extended. `deadline = claim_ts + effective lease_ms`. |
| `claimed[].deliveries` | How many times this job has now been delivered (this claim is counted). Starts at `1`; compared against `max_deliveries` (§10.6). |
| `count` | `claimed.length`. |
| `ready` | Claimable jobs still waiting after this claim (the §10.7 `ready` counter). |

**Errors** — `400 invalid_request` (missing `node`, bad `max`/`lease_ms` type);
`404 box_not_found`; `409 not_a_queue`; `429 throttled`.

### 10.3 Lease expiry / visibility timeout

A lease has an absolute `deadline`. Once `now > deadline` (evaluated via the Clock) the
job's lease is **expired** and the seq becomes **claimable again** — this **is** the
visibility timeout. Expiry is **lazy**: there are no per-job timers; expired-lease seqs are
collected into a **reclaim freelist** that the next claim pass drains **first** (before
handing out fresh jobs), so a reclaimed job jumps the queue ahead of never-delivered ones.
Each reclaim increments the job's delivery counter on its *next* claim and is subject to
dead-lettering (§10.6). Because expiry is clock-driven and lazy, **losing the leases log on
a crash is self-healing**: every in-flight job simply has no active lease after restart and
is immediately claimable (§10.1 durability note, DESIGN §10.6).

### 10.4 Ack jobs — `POST /v0/boxes/:q/ack`

Complete jobs: the **ack is the delete**. The server records an `acked` event in the leases
log and removes each seq from the jobs log via the existing permanent delete (§5). An
acked+deleted job stays gone iff its delete was durable — i.e. **ack durability == the box's
`durable`** (§0.10).

**Request body**

```json
{ "node": "worker-eu-1", "seqs": [480101, 480104], "lease_ids": ["lease_1a2b", "lease_3c4d"] }
```

| Field | Type | Req? | Meaning |
|---|---|---|---|
| `node` | string | yes | The worker acking. Must be the current lease holder of each seq for the ack to count. |
| `seqs` | array<u64> | yes | 1..=`STREAMS_MAX_CLAIM` job seqs to complete. |
| `lease_ids` | array<string> | no | Optional **per-seq lease fence** (validate-when-supplied): the `claimed[].lease_id` tokens from the originating claim, one per entry of `seqs` (same length and order). When present, a seq is acked only if its supplied token matches the current lease — a **stale** token (the lease was superseded by a re-claim/extend by another worker) is rejected and that seq is **skipped**. Omit to fall back to the legacy `node`+`seqs` match. |

**Behavior** — only seqs currently leased to `node` are acked (deleted). A seq that is not
leased to `node` (never claimed, already acked, or its lease expired and was reclaimed/leased
to someone else) is **silently skipped** and reported in `skipped` — ack is idempotent and
safe to retry (a duplicate ack of an already-deleted job is a no-op). This is the
at-least-once seam: a worker acking **past its deadline** may find another worker already
holds the lease, so its ack is skipped and the job may be processed twice (idempotent
consumers required, §10.1).

**Response** (`200`)

```json
{ "box": "jobs", "acked": 2, "skipped": [],
  "ready": 840, "in_flight": 286,
  "performance": { "server_total_ms": 0.30, "fsync_ms": 0.21 } }
```

| Field | Meaning |
|---|---|
| `acked` | Count of seqs actually completed+deleted by this call. |
| `skipped` | Seqs in the request that were **not** acked (not held by `node`), for observability. May be empty. |
| `ready` / `in_flight` | Post-ack queue counters (§10.7). |

`fsync_ms > 0` only on a `durable` queue (the delete is fsynced before the ack returns).

**Errors** — `400 invalid_request` (missing `node`/`seqs`, bad seq type);
`404 box_not_found`; `409 not_a_queue`.

### 10.5 Nack jobs — `POST /v0/boxes/:q/nack`

Release leased jobs back to the queue for **immediate** (or delayed) reclaim, without an ack.
Records a `released` event in the leases log.

**Request body**

```json
{ "node": "worker-eu-1", "seqs": [480104], "delay_ms": 0, "lease_ids": ["lease_3c4d"] }
```

| Field | Type | Req? | Default | Meaning |
|---|---|---|---|---|
| `node` | string | yes | — | Must be the current lease holder (else that seq is skipped). |
| `seqs` | array<u64> | yes | — | Job seqs to release. |
| `delay_ms` | `u64` | no | `0` | Hold the job invisible for this long before it becomes claimable again (delayed retry / backoff). `0` = claimable immediately (added to the reclaim freelist now). Clamped `[0, 86400000]`. |
| `lease_ids` | array<string> | no | — | Optional **per-seq lease fence** (validate-when-supplied), one per `seqs` entry — same semantics as §10.4: a stale token's seq is skipped; omit for the legacy `node`+`seqs` match. |

A nack drops the active lease and makes the seq claimable again at `now + delay_ms` (via the
Clock), incrementing the delivery counter on its next claim and subject to dead-lettering
(§10.6) — a nack is a voluntary early reclaim, semantically identical to letting the lease
expire, just sooner (or after `delay_ms`).

**Response** (`200`)

```json
{ "box": "jobs", "nacked": 1, "skipped": [],
  "ready": 841, "in_flight": 285,
  "performance": { "server_total_ms": 0.18 } }
```

| Field | Meaning |
|---|---|
| `nacked` | Seqs released by this call (held by `node`). |
| `skipped` | Seqs not held by `node` (silently skipped). |
| `ready` / `in_flight` | Post-nack counters (§10.7). A delayed nack does **not** count toward `ready` until `delay_ms` elapses. |

**Errors** — `400 invalid_request`; `404 box_not_found`; `409 not_a_queue`.

### 10.6 Extend a lease — `POST /v0/boxes/:q/extend`

Push out the deadline of held leases (the heartbeat for long jobs). Records an `extended`
event in the leases log.

**Request body**

```json
{ "node": "worker-eu-1", "seqs": [480101], "lease_ms": 30000, "lease_ids": ["lease_1a2b"] }
```

| Field | Type | Req? | Default | Meaning |
|---|---|---|---|---|
| `node` | string | yes | — | Must be the current lease holder. |
| `seqs` | array<u64> | yes | — | Held job seqs to extend. |
| `lease_ms` | `u64` | yes | — | New lease duration from **now**; the new `deadline = now + lease_ms`. Clamped `[100, 86400000]`. (Extend sets, not adds — the worker asserts "I need this much more time from now.") |
| `lease_ids` | array<string> | no | — | Optional **per-seq lease fence** (validate-when-supplied), one per `seqs` entry — same semantics as §10.4: a stale token's seq is skipped; omit for the legacy `node`+`seqs` match. |

A seq whose lease has **already expired** (and was reclaimed) cannot be extended — it is
skipped; the worker should re-claim. Extending does **not** change the delivery counter.

**Response** (`200`)

```json
{ "box": "jobs", "extended": 1, "skipped": [],
  "deadlines": { "480101": 1748450069000 },
  "performance": { "server_total_ms": 0.12 } }
```

| Field | Meaning |
|---|---|
| `extended` | Seqs whose deadline was pushed out. |
| `skipped` | Seqs not held by `node` (expired/reclaimed/never-claimed). |
| `deadlines` | New absolute deadline (ms) per extended seq. |

**Errors** — `400 invalid_request` (missing `lease_ms`); `404 box_not_found`;
`409 not_a_queue`.

#### Dead-lettering (`max_deliveries` + `dead_letter`)

Each job carries a **delivery counter** incremented on every claim (including reclaims of
expired leases and re-claims after a nack). When a job is about to be delivered for the
`(max_deliveries + 1)`-th time and the box has a non-`null` `dead_letter`, it is **not**
re-delivered: instead the server appends the job's record to the `dead_letter` box (preserving
`$tag`/`meta`/`data`, stamping `meta.$dead_letter_from`/`meta.$dead_letter_deliveries`/
`meta.$dead_letter_src_seq` for traceability) and permanently deletes it from the jobs log
(the same delete path as an ack). With `max_deliveries = 0` (default) or `dead_letter = null`,
a job is reclaimed forever and never dead-lettered. Dead-lettering increments the §10.7
`dead_lettered` counter.

### 10.7 Queue observability

`GET /v0/boxes/:q` (§1.2) on a queue returns `type:"queue"` and a `queue` sub-object beside
the normal box state:

| `queue` field | Meaning |
|---|---|
| `ready` | Claimable jobs right now — in the jobs log, not acked, with no active lease (includes reclaim-freelist seqs whose lease expired or whose nack `delay_ms` elapsed). |
| `in_flight` | Jobs with an active (un-expired) lease — currently held by some worker. |
| `dead_lettered` | Cumulative count of jobs moved to the `dead_letter` box over this box instance's life (resets on delete+recreate). |

`ready + in_flight` equals the live job count (`count`) modulo jobs whose nack `delay_ms`
has not yet elapsed (counted in neither until they become claimable). A queue box stays
fully **readable via normal §3 `diff` and §7 `watch`** (read-only) for monitoring — those
paths observe the jobs log and never claim, ack, or mutate leases.

### 10.8 Auto-claim over SSE (PUSH mode) — `GET /v0/boxes/:q/work`

A streaming alternative to the claim→process→claim poll loop: the server keeps up to `max`
jobs leased-and-pushed to this one connection, claiming more as the worker acks, applying
backpressure at `max` in-flight. **The stream is one-way** (SSE) — the worker still **acks**
(and may nack/extend) via the separate `POST` endpoints above; the stream only *delivers*.

```
GET /v0/boxes/:q/work?node=worker-eu-1&max=8 HTTP/1.1
Accept: text/event-stream
```

| Query | Req? | Default | Meaning |
|---|---|---|---|
| `node` | yes | — | The worker identity these jobs are leased to (as in §10.2). |
| `max` | no | `1` | Target in-flight depth: the server keeps at most this many jobs leased to this connection at once (the backpressure bound). Clamped to `STREAMS_MAX_CLAIM`. |
| `lease_ms` | no | box `lease_ms` | Lease duration for jobs pushed on this stream. |
| `token` | no | — | Dev-only `?token=<key>` fallback for browser `EventSource` (as in §7.1); prefer the `Authorization: Bearer` header — a query string leaks via logs/history/proxies. |

Response headers are the SSE set from §7.3 (`Content-Type: text/event-stream`,
`Cache-Control: no-store`, `X-Accel-Buffering: no`). A `retry: 2000` and an initial
heartbeat are sent at open, exactly as §7.5/§7.6. The **same coalescing-window logic**
(§10.2) feeds connected `/work` streams fairly alongside polling claimers.

**`event: job`** — one leased job (the streaming analog of one `claimed[]` entry). `id:` is
the job `$seq` (an opaque resume hint; the authoritative lease state lives server-side).

```
id: 480101
event: job
data: {"box":"jobs","$seq":480101,"lease_id":"lease_7f3a9c","deadline":1748450039000,"$ts":1748450001000,"$tag":"tenant42:job-8800","deliveries":1,"data":{"type":"resize","url":"s3://b/x.png"}}
```

The worker processes the job and **acks** it via `POST /v0/boxes/:q/ack` (§10.4); on each ack
the server claims and pushes replacement jobs to keep the stream at `max` in-flight. To
reject, the worker `nack`s (§10.5); to keep a long job alive, it `extend`s (§10.6).

**Heartbeats** — bare SSE comments on the §7.6 cadence (`: hb <epoch-ms>`), suppressed when a
real `job` frame went out in the window.

**`event: error`** — a per-stream problem with an HTTP-aligned `code` (same shape as §7.5),
e.g. `code: 429` advisory pacing under pressure, `code: 409` if the box ceased to be a queue
(deleted+recreated as a log). Terminal errors close the stream; the worker reconnects.

**On disconnect (instant failover).** When the `/work` connection drops (clean close or
detected broken pipe), the server **immediately releases all of that node's leases that were
delivered on this connection** — recording `released` events so the jobs are instantly
claimable again, rather than waiting for lease expiry. Lease expiry (§10.3) still covers hard
crashes where the disconnect is not observed. (Because release-on-disconnect is keyed to this
connection's deliveries, a worker holding leases from a separate `claim` poll is unaffected.)

**Errors at establishment** — `200` (stream opened); `400 invalid_request` (missing `node`,
bad `max`); `404 box_not_found`; `406 not_acceptable` (`Accept` not `text/event-stream`);
`409 not_a_queue`; `429 throttled`.

---

## 11. Resource & rate limits (DoS hardening)

streams enforces a small set of **configurable resource caps** so a single instance — or a
single api key — cannot exhaust the server by creating unbounded resources or opening unbounded
streams. Every cap has a **sane default** and is **disabled by setting it to `0`** (unlimited).
The defaults are generous; a normal deployment never trips them, and an unconfigured dev box on
loopback behaves exactly as before.

| Limit | Env var | Default | Scope | Enforced on |
|---|---|---|---|---|
| Max boxes | `STREAMS_MAX_BOXES` | `100000` | instance | every box **creation** (`PUT /v0/boxes/:box` and write auto-create) |
| Max routers | `STREAMS_MAX_ROUTERS` | `10000` | instance | every router **creation** (`PUT /v0/routers/:r`) |
| Max watch sessions | `STREAMS_MAX_WATCH_SESSIONS` | `10000` | instance | `POST /v0/watch` |
| Max SSE connections | `STREAMS_MAX_SSE_CONNECTIONS` | `10000` | instance | every SSE stream GET (`/v0/watch/:wid`, `/v0/boxes/:q/work`) |
| Max SSE connections / key | `STREAMS_MAX_SSE_CONNECTIONS_PER_KEY` | `1000` | per key | same, attributed to the authenticated key |
| Max in-flight requests / key | `STREAMS_MAX_INFLIGHT_PER_KEY` | `1000` | per key | every request — a per-key **concurrency** cap (held for the request's duration) |
| Max total bytes | `STREAMS_MAX_TOTAL_BYTES` | `0` (unlimited) | instance | every **write** — a global disk/RAM growth quota over the sum of retained record bytes across all boxes |

Two additional, **non-configurable** hard bounds protect the read/response paths:

- A single `record` frame / `diff` response is bounded by a **byte budget** (`max_batch_bytes`,
  default 1 MiB, clamped to 8 MiB) as well as by `limit`, so one response cannot grow to
  `MAX_LIMIT`×record-cap (§7.2). At least one record is always delivered (forward progress).
- Queue `ack`/`nack`/`extend` reject a `seqs` array longer than `STREAMS_MAX_CLAIM` (1000) with
  `400 batch_too_large`, so a request cannot make the server allocate/echo an unbounded vec.

**Semantics.**

- A **creation** that would exceed a cap returns **`429 throttled`** with a `Retry-After`
  header and `error.detail.limit` naming the cap (e.g. `"max_boxes"`) and `error.detail.max`.
  Capacity is transient — the client retries after another client sheds load. This reuses the
  existing elastic-throttle signal (§0.6), so a client that already handles `429` needs no
  change.
- Only **new** resources count against the box/router caps: an idempotent re-`PUT` of an
  **existing** box/router is an *update* and always succeeds, so a saturated server can still
  be reconfigured. A router refused by `max_routers` is rejected **before** any source/dest box
  auto-create, so it leaves no phantom boxes.
- The **total-bytes** quota (`STREAMS_MAX_TOTAL_BYTES`, default `0`/unlimited) bounds disk/RAM
  growth from authenticated writers: a write that would push the live total over the cap is a
  `429 throttled` (`error.detail.limit:"max_total_bytes"`). It is checked only when enabled, so
  the default path is unchanged. Combine with per-box `cap_bytes`/`cap_records` + `discard:"old"`
  (§0.10) for fine-grained, self-trimming per-box bounds.
- Idle **watch sessions** are reclaimed after `session_ttl_ms` of no active stream (§7.1), so an
  abandoned session does not pin a `max_watch_sessions` slot until restart.
- **Concurrency caps free their slot when the request/stream ends** (the response is sent, or
  the SSE connection closes — clean close, broken pipe, or cancel), via RAII guards, so a
  dropped stream or a panicking handler can never permanently consume capacity. SSE streams are
  long-lived and are bounded by the **SSE-connection** caps, not the per-key in-flight cap (so
  holding a stream open does not block ordinary requests for that key).
- The **per-key** caps apply only when **auth is enabled** (there is a key to attribute use
  to). In dev mode (no keys) only the instance-wide caps apply. An SSE connection on a watch
  session is attributed to the **session's creating key** (constant across reconnects).

These caps are independent of the per-request **batching limits** (§2) and the elastic
**CPU-pressure** throttle (§0.6); all three can produce `429`. A `429` from a resource cap
carries `error.detail.limit`.

---

## 12. Security & operations (threat model)

This section is the operator-facing security note: what streams protects, what it does **not**,
and how to deploy it safely. It restates the model spread across §0.2 / §0.11 / §11 in one
place.

### 12.1 What's in scope

- **Authentication** — bearer tokens (`Authorization: Bearer <key>`), supplied at startup via
  `STREAMS_API_KEYS`. **Keys are hashed at rest**: the plaintext is parsed into the hashed store
  **once** at startup and then **zeroized and dropped** — only the SHA-256 digest is held in
  memory for the process lifetime, never the plaintext, and **tokens are never logged**. The
  presented token is hashed and its digest compared against every configured key's digest in
  **constant time** with no early exit, so neither *which* key nor *how many leading bytes*
  matched leaks via timing.
- **Authorization** — optional per-key **scopes** (`read`/`write`/`delete`/`admin`) and a
  box-name **prefix allowlist**, enforced per route (§0.2). A key outside its scope or prefix
  gets **`403 forbidden`**. The prefix allowlist is enforced not only on the path name but on
  the **request body's** box names where relevant: the boxes a `POST /v0/watch` subscribes to,
  and a router's `source` **and** `dest` (which the engine would otherwise auto-create) — so a
  scoped key cannot watch, or route data into, a box outside its allowlist. **List** endpoints
  (`GET /v0/boxes`, `GET /v0/routers`) are filtered to the key's allowlist, so a prefix-limited
  key cannot even enumerate cross-tenant names. A bare `key` is full access (back-compat). A
  **malformed scope token** makes the server **refuse to start** (fail-closed — it never
  silently grants the wrong scope). `/v0/metrics` requires the `read` scope by default (§8.3).
- **Watch capability** — the `wid` is an unguessable 128-bit random capability bound to the
  creating key **and** its scope. **When auth is enabled, the `wid` alone does not open the
  stream**: the GET must present the creating key (header or the dev-only `?token=`), so a
  leaked `wid` is not a credential. The SSE GET cannot exceed the creator's scope, and a *valid
  but different* key presented on the GET is rejected (§7.1).
- **Resource exhaustion (DoS)** — configurable caps on boxes / routers / watch sessions /
  concurrent SSE connections (global + per-key) / per-key in-flight requests / **total retained
  bytes**, plus a **byte budget** on each read response and a **length bound** on queue
  ack/nack/extend `seqs` arrays, and **idle watch-session GC** so abandoned sessions free their
  slot (§11).
- **Accidental public exposure** — the default bind is **loopback** (`127.0.0.1:4000`); a
  non-loopback bind with no keys **refuses to start** unless `STREAMS_ALLOW_INSECURE_NO_AUTH=1`
  (§0.2).
- **No path traversal** — user-controlled box/router **names never reach the filesystem** as
  path components. On-disk segment/WAL/snapshot files are keyed by an interned **numeric
  box id**, not the name; names are strictly validated to a fixed charset
  (`^[A-Za-z0-9][A-Za-z0-9._:-]{0,254}$`, plus `>` for routers) at the engine boundary before
  any keyed-by-id filesystem use.

### 12.2 What's NOT in scope (deploy accordingly)

- **No TLS.** streams speaks **plain HTTP** and does not terminate TLS. A bearer token on plain
  HTTP — or in a URL query string (the dev-only `?token=` SSE fallback, §7.1) — can be observed
  in transit or in logs. **For any non-loopback exposure, run streams behind a TLS-terminating
  reverse proxy** (nginx / Caddy / Envoy / a cloud LB), or bind loopback and tunnel (SSH /
  WireGuard). Native TLS is **out of scope by design** — terminate it at the proxy.
- **No hard tenant isolation.** The prefix allowlist is a *filter*, not a namespace partition:
  two keys with overlapping prefixes share the same box namespace. Multi-tenancy beyond per-key
  scopes + prefix allowlists is **out of scope** (there is no hard per-tenant partition).
- **No audit log / no key rotation API.** Keys are static for the process lifetime (set at
  startup); rotate by restarting with a new `STREAMS_API_KEYS`. There is no per-request audit
  trail beyond the operational tracing logs (which never contain tokens).
- **Trusted operator.** Anyone who can read the process environment or the boot logs can see
  the configured key **plaintext** (it is supplied via `STREAMS_API_KEYS`); the *hashing at
  rest* protects the in-memory/runtime surface and crash dumps, not the startup configuration.
  Manage `STREAMS_API_KEYS` as a secret (e.g. a secrets manager / systemd `LoadCredential`),
  not a committed file.

### 12.3 Recommended hardened deployment

1. **Terminate TLS at a reverse proxy** in front of streams; have the proxy forward to a
   **loopback** streams bind (`STREAMS_HOST=127.0.0.1`). Then a leaked plain-HTTP token is not
   network-observable.
2. **Set `STREAMS_API_KEYS`** with **least-privilege scopes + prefixes** per client — e.g. a
   read-only dashboard key `dash:read:tenant42:`, a producer `prod:write:tenant42:`, an admin
   key `ops::` (all scopes, all boxes). Avoid bare full-access keys in production.
3. **Tune the resource caps (§11)** to your capacity. Leave the defaults unless you have a
   specific reason; set a cap to `0` only to deliberately disable it.
4. **Never set `STREAMS_ALLOW_INSECURE_NO_AUTH=1`** outside a trusted private network — it
   disables the no-auth refusal that exists to prevent an accidental open event store.
5. **Treat the data directory** (`STREAMS_DATA_DIR`) as sensitive: it contains every record's
   payload in the WAL/segments in the clear. Protect it with filesystem permissions and
   at-rest disk encryption as your threat model requires (streams does not encrypt payloads).

