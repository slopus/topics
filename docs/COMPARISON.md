# topics — Comparative Engineering Report

A synthesis of six independent analyses (codex full-codebase pass + five dimension
deep-dives: storage engine, durability/recovery, API & data model, operational
readiness, correctness/concurrency/perf). Every claim is grounded in `file:line`.
This is a build-decision document: what is missing, what could be better, what is
incorrect or risky — and an ordered plan.

> **STATUS — this is a historical point-in-time analysis, not the current backlog.**
> Several items below have since been **implemented** or **decided out of scope**; the
> `file:line` cites and "Recommend" blocks reflect the state *at the time of writing*. Read
> it as a record of the engineering reasoning, not a live to-do list. The authoritative
> current state is README.md / API.md / DESIGN.md / ARCHITECTURE.md / ROADMAP.md. Key
> reconciliations (see the inline **UPDATE** notes for `file:line` corrections):
> - **Implemented since:** `/v0/metrics` is a full gauge/WAL/histogram surface, **not** a one-gauge
>   stub (M3); bounded graceful drain with SSE wind-down (M11); queue `lease_id` fencing —
>   validate-when-supplied (R4); bounded WAL submission with `503` backpressure (R5); off-gate
>   segment seal (R6); the **sharded** WAL (`TOPICS_WAL_SHARDS`); **async + derived** routers; the
>   `queue` topic type; advisory data-dir single-writer fencing. The WAL is **sharded**,
>   not a single global writer.
> - **Decided OUT OF SCOPE (will not be built):** multi-server / replication / HA
>   beyond the single-process data-dir lock (M1); durable consumer groups as a server primitive (M4 — they are an app pattern: a topic
>   per consumer + delete); LSM / keyed log compaction / compacted topic type and any intra-segment
>   per-record reclaim (M5, the M8 compaction half); native TLS (terminate at a reverse proxy) and
>   hard multi-tenancy beyond per-key scopes + prefix allowlists (M13). Deletion is **no compaction /
>   no reclaim** by design.

**One-paragraph framing.** topics is a genuinely well-built *single-node, append-mostly*
event engine. Its durability core (sharded ordered WAL writers, XXH3 framing,
torn-tail truncation, adaptive group commit, tail-rewind on fsync failure,
checkpoint = `(wal_idx, wal_offset)`, idempotent convergent replay) and its
crash-test harness (FakeDisk/FaultFs + ~60 enumerated faults + a model oracle
that is an explicit `wal_consistency_checking` analog) match or beat mature
engines on crash-correctness *for what it covers*. The gaps are not in the core —
they are in (a) the **router/queue/list layers** built on top of it, (b) **operational
surface** (metrics, backup, repair, replication), and (c) **topic-cardinality scaling**.
The cheapest high-value wins reuse machinery that already exists.

Legend: severity is **high / med / low**. "Borrows from" names the battle-tested
system that does it well and how.

---

## (1) MISSING

### M1 — Replication / HA / failover  · **high**
> **UPDATE — OUT OF SCOPE (decided).** Multi-server / replication / HA / failover are
> **not** on the roadmap; topics is single-server by design. Single-writer data-dir
> fencing is now implemented with an advisory lock. The text below is retained as the
> original analysis only.

No replication, standby, or failover anywhere (`grep replicat|raft|failover|standby`
→ nothing). Any node loss = downtime; checkpointed-but-unreplicated data is exposed
to single-disk loss. Single-writer ownership is now guarded by an advisory data-dir
lock; it is still not a distributed leader lease or failover mechanism.
- **Borrows from:** Kafka (replicated partitions + ISR), NATS JetStream (RAFT streams/consumers), PostgreSQL (streaming WAL replication), MySQL (binlog replicas).
- **Recommend:** the append-only monotonic-seq WAL is the natural shipping unit. Build in stages — (1) WAL shipping to a read replica, (2) leader epoch + quorum commit for the `fsync` class. The local lock is only a single-host guard; distributed fencing would still be needed for HA.
- **Where:** `docs/ARCHITECTURE.md:8-11`, `src/engine/mod.rs:40-107`, `docs/FAULT_TESTING.md:82`.

### M2 — Backup / restore / PITR / WAL archiving  · **high**
Snapshots exist only for *internal* recovery. Absorbed WAL is **dropped** at checkpoint
(`src/engine/recovery.rs:187-198`, `src/engine/mod.rs:343-347`) and the prior snapshot
is **pruned the instant the new one is durable** (`src/storage/snapshot.rs:232-237`).
There is no archived-WAL retention, no replay-to-a-point/timeline, no online consistent
backup, and no `--snapshot-now` admin trigger. After checkpoint a record lives in exactly
**one** durable place (see also R10) — this is the single biggest durability gap. Two
analyses independently rank it #1–2.
- **Borrows from:** PostgreSQL (base backups + `archive_command`/`restore_command` + timelines), MySQL (binlogs), SQLite (backup API / copyable file), RocksDB (checkpoints).
- **Recommend:** retain absorbed WAL behind an archive hook + keep N prior snapshots; add `backup`/`restore` plus replay-to-checkpoint / replay-to-timestamp. Reuses the existing `(wal_idx, wal_offset)` checkpoint + idempotent replay almost directly. A "do not drop absorbed WAL until archived" mode closes the gap.
- **Where:** `src/engine/recovery.rs:184-197`, `src/storage/snapshot.rs:182-238`, `src/engine/mod.rs:311-357`, `src/main.rs:85-107`.

### M3 — Observability / `/v0/metrics` is a stub  · **high** (low effort)
> **UPDATE — RESOLVED (implemented).** `/v0/metrics` now exposes the full surface: process/aggregate
> gauges, per-topic gauges (head/earliest/records/bytes/queue), real `WalMetrics`, and a fsync-latency
> histogram (`src/http/health.rs`). It is **not** a one-gauge stub. The text below describes the old
> state.

`render_prometheus` emits exactly **one** gauge (`topics_topics`) with an in-code TODO
(`src/http/health.rs:79-86`), even though the data already exists: `WalMetrics`
(fsyncs/frames/batches/bytes/rotations, `src/storage/wal.rs:989`) and per-topic
head/earliest/count/bytes/last_write/last_read/queue counters (`src/engine/mod.rs:1222`).
No latency histograms, no recovery-progress/router-lag/queue-lease/SSE-connection gauges,
no slow-op log, no request tracing spans (only coarse `info!`/`warn!`).
- **Borrows from:** PostgreSQL (`pg_stat_*`, `pg_stat_statements`), MySQL (`performance_schema`), Kafka/JMX, RocksDB stats.
- **Recommend:** wire the existing atomics into the exporter; add fsync histograms, WAL queue depth, recovery progress, router lag, queue-lease stats, segment compaction/read latency, slow diff/SSE spans. **Highest ROI in the whole report** — days of work against code that already exists.
- **Where:** `src/http/health.rs:50-86`, `src/storage/wal.rs:989`, `src/engine/mod.rs:1222`.

### M4 — Durable consumer groups / committed offsets  · **high**
> **UPDATE — OUT OF SCOPE (decided).** Durable consumer groups as a *server primitive* will not be
> built. The supported pattern is application-level: a topic per consumer (or per group) plus
> delete-as-ack; the lease-based `queue` topic type covers competing-worker delivery. The text below
> is the original analysis.

Every consumer is bring-your-own-cursor: the `diff` path keeps **no** server-side
per-consumer state (`docs/API.md:614-617`, `src/types.rs:410-434`). The lease queue is
the only server-tracked delivery state. No named groups, no committed offsets, no
"lag per group". Both the API analysis and codex flag this.
- **Borrows from:** Kafka (group coordinator + committed offsets), Redis Streams (XGROUP/PEL), JetStream (durable consumers + ack floor).
- **Recommend:** add named stream groups with WAL-persisted cursor/ack state (the queue lease-log projection is a precedent for storing such state); keep the lease queue as a separate work-queue mode. Buys observability (lag) for free.
- **Where:** `src/types.rs:410-434`, `src/engine/queue.rs:1-9`, `docs/API.md:612-617`.

### M5 — Keyed log compaction / compacted topic type  · **high**
> **UPDATE — OUT OF SCOPE (decided).** LSM / keyed log compaction and a compacted topic type will not
> be built; deletion is **no compaction / no per-record reclaim** by design (a delete-flag byte is
> flipped in place; only a whole cleared segment is dropped). The last-value-per-key pattern is built
> at the application level (a tag + a point-in-time `match` delete of prior versions). The text below
> is the original analysis.

No keyed last-value compaction; `before_seq` delete is snapshot-compaction, not keyed
(`docs/DESIGN.md:429`). Records have no optional `key`. Already flagged as future
(`docs/ROADMAP.md:196-203`). Combined with **no intra-segment reclaim** (see B3/storage),
sparse-delete workloads pin dead bytes indefinitely. The per-topic `tag` index is the
natural compaction key. Three analyses converge here.
- **Borrows from:** Kafka (`cleanup.policy=compact`), RocksDB (LSM drops obsolete versions), PostgreSQL/SQLite (VACUUM).
- **Recommend:** add optional record `key` + key tombstones + a compacted topic mode that rewrites segments to last-value-per-key. Unlocks Kafka-KTable / config-topic / materialized-current-state use cases an append-only log can't serve.
- **Where:** `src/types.rs:246-258`, `src/engine/topic_state.rs:1031-1089`, `docs/ROADMAP.md:196-203`.

### M6 — Read-time filtering on `diff` / `watch`  · **med** (low effort)
The `Eq`/`Glob` predicate language **already exists** (`src/engine/filters.rs`) and is
used for delete (§5) and router rules (§6.1) — but is **not exposed on reads**. Today
`meta`/`tag` are effectively write-only metadata: a record can only be found by seq or
by tag-at-delete-time. The API analysis calls this the single highest-leverage *ergonomic*
add (lowest cost, reuses existing machinery).
- **Borrows from:** turbopuffer (attribute filters on query), Redis (`XRANGE` + client filter), Kafka (no native, so this is a differentiator).
- **Recommend:** expose the existing filter tuples on `diff`/`watch` (`match`/`filter` param). Together with M5 this converts topics from a pure log into a queryable, materializable store with minimal new surface.
- **Where:** `src/engine/filters.rs`, `src/http/topics.rs`, `docs/API.md:122-124`.

### M7 — Admin verify / repair / scrub tooling  · **med**
Checksum (XXH3-64) validation happens **only lazily on the read path** (`src/storage/wal.rs:564`,
`src/storage/segment.rs:204-208`, `src/storage/snapshot.rs:299`). There is no offline/online
`verify`/`fsck`, no `integrity_check`-style full scan, no `dump-wal`/`scan-segments`/
`repair-orphans`, no rebuild-index. Bit-rot in a cold segment is found only when something
reads it. Worse, `F-WAL-CRC-FLIP-MIDLOG` (`docs/FAULT_TESTING.md:73`): a mid-log checksum (XXH3-64) flip
on the active WAL truncates *everything after it* — silently dropping acked `disk`/`fsync`
records (overlaps R8). An operator cannot proactively scrub or salvage without booting the server.
- **Borrows from:** PostgreSQL (`pg_amcheck`, `pg_checksums`), SQLite (`PRAGMA integrity_check`), InnoDB (page checksums + recovery modes), RocksDB (`ldb`/repair/checkpoint).
- **Recommend:** add startup/background full-scan XXH3 verification + offline `verify`/`dump-wal`/`scan-segments`/`repair-orphans`, reusing the XXH3-64 readers already in place. Turns lazy detection into proactive.
- **Where:** `src/main.rs:16-174`, `src/storage/wal.rs:564`, `src/storage/segment.rs:204-208`, `docs/FAULT_TESTING.md:73`.

### M8 — Topic-cardinality scaling: file/inode explosion + no segment coalescing  · **med**
*(The storage deep-dive ranks this its #1 structural risk; codex did not surface it — noted disagreement.)*
> **UPDATE — partially OUT OF SCOPE.** The file/inode-cardinality concern remains a real
> single-node scaling note, but the **compaction half** of the recommendation (rewriting
> sparsely-deleted segments to drop dead frames / intra-segment per-record reclaim) is **out of
> scope** — deletion is no-compaction / no-reclaim by design (only whole cleared segments drop).

Each topic is its own directory (`src/engine/mod.rs:485`) with ≥2 files per sealed segment,
mirrored under cold. With the 1h age trigger sealing partial segments, even idle topics
accrete tiny 2-file segments forever. 100k low-traffic topics ⇒ 100k+ directories, O(files)
`read_dir` per topic per recovery (`reclaim_orphans_below`), dentry-cache churn. No segment
coalescing, no per-topic file budget, no column-family-style sharing.
- **Borrows from:** RocksDB (column families share one LSM/WAL across logical topics), InnoDB/SQLite (single-file packing).
- **Recommend:** (a) a bounded **segment-coalescing** pass that merges adjacent small sealed segments (and rewrites sparsely-deleted segments to drop dead frames — also closes the intra-segment reclaim gap, B3), and/or (b) a "packed" `SegmentStore` impl (the trait seam already exists) that packs cold/idle segments into a shared blob. Threshold-triggered so the common append-mostly case keeps its zero-rewrite property.
- **Where:** `src/engine/mod.rs:485`, `src/storage/segment.rs:38-53`, `src/engine/segwriter.rs`, `src/engine/topic_state.rs:1031-1089`.

### M9 — Subject / wildcard subscription  · **med**
Topic names are flat; `:` is a *convention* with no read-time meaning. Watch and routers
require explicit topic enumeration (`docs/API.md:919`); no `tenant42:*` subscribe. Biggest
scaling-ergonomics gap vs JetStream.
- **Borrows from:** NATS JetStream (subject hierarchy `a.b.*`).
- **Recommend:** add prefix/wildcard subscribe to watch (and optionally a fan-out router group). Leverages the dangling `:` convention.
- **Where:** `docs/API.md:919`, `src/http/watch.rs`, `src/engine/router.rs`.

### M10 — Schema / content validation per topic  · **med**
`data`/`meta` are opaque verbatim JSON, never parsed/indexed/validated
(`docs/API.md:122-124`, `src/engine/topic_state.rs:66-83`). No way to reject malformed writes.
- **Borrows from:** Kafka Schema Registry; PostgreSQL/SQLite constraints/CHECKs.
- **Recommend:** optional per-topic JSON-Schema validator (reject-on-write); purely additive. Optionally bounded secondary indexes over selected JSON fields / tag / key.
- **Where:** `src/engine/topic_state.rs:66-83,146-176`, `docs/API.md:122-124`.

### M11 — Bounded drain / rolling-restart safety  · **med** (low effort)
> **UPDATE — RESOLVED (implemented).** Graceful shutdown now races a `DRAIN_BUDGET` timeout and
> actively winds down SSE streams (`src/serve.rs`). The text below describes the old state.

`graceful.shutdown().await` (`src/serve.rs:148`) has **no deadline** — a long-lived SSE or
`/work` stream stalls a rolling restart indefinitely; SSE streams are not told to wind down.
- **Borrows from:** any production HTTP server (drain timeout + forced close).
- **Recommend:** `tokio::time::timeout` race + actively signal SSE streams to close, then forced-close fallback. Small change, fixes rolling-restart safety.
- **Where:** `src/serve.rs:146-149`, `src/main.rs:156-172`.

### M12 — Online config change + on-disk format versioning / migration  · **med**
Config is env-only, read once at boot (`src/config.rs:290`); no SIGHUP/reload. WAL/segment/
snapshot frames carry an XXH3-64 checksum but no explicit surfaced **format-version field** for upgrades —
cross-version compatibility is undefined. Limits/segment-policy/keys all need a full restart.
- **Borrows from:** PostgreSQL (catalog/extension migrations), MySQL (`SET PERSIST`), Kafka (dynamic configs), JetStream (stream updates).
- **Recommend:** add a format-version field (cheap insurance before more users pin data) + documented upgrade policy; version persisted config and validate online changes before any format churn.
- **Where:** `src/config.rs:288-356`, `src/engine/mod.rs:875-920`.

### M13 — Multi-tenancy / TLS / quotas / audit  · **low**
> **UPDATE — mostly OUT OF SCOPE.** Native TLS will not be built (terminate at a reverse proxy), and
> hard multi-tenancy beyond per-key scopes + topic-name-prefix allowlists is **out of scope**. Per-key
> resource limits and a global total-bytes quota are implemented (§11); audit logging remains a
> possible future op concern. The text below is the original analysis.

"Tenancy" = per-key topic-name prefix allowlist (a filter, not a partition;
`docs/API.md:1578`, acknowledged future at `docs/API.md:108`); quotas are global
(`max_topics`/`max_total_bytes`), not per-tenant. No in-process TLS (plain HTTP, relies on
external proxy, `README.md:336`). No audit log.
- **Borrows from:** PostgreSQL/MySQL (TLS + roles), Kafka (SASL/TLS/ACLs), managed turbopuffer (service-side auth boundaries).
- **Recommend:** either ship rustls TLS or document reverse-proxy hardening; add tenant IDs + per-tenant quotas + audit logs. Lower priority than M1–M12.
- **Where:** `docs/API.md:91-94,108,1578`, `src/main.rs:147-156`, `README.md:336`.

---

## (2) COULD BE BETTER

### B1 — `disk` group-commit/fsync discipline is weaker than stated  · **high**
`disk` acks after the buffered *write*, not an fsync, and there is no periodic flush of the
`disk` tail (`src/storage/wal.rs:1470-1472`, `docs/ARCHITECTURE.md:213-214`). See also R7
(the *risky* consequence: seq rollback after power loss).
- **Borrows from:** PostgreSQL/InnoDB (group commit flushes durable commit records), SQLite (clear `NORMAL`/`FULL`), Kafka (explicit flush/ack tradeoffs).
- **Recommend:** add periodic `fdatasync` for `disk` batches and expose a flushed-WAL watermark; or document `disk` precisely as lossy.
- **Where:** `src/storage/wal.rs:1470-1472`.

### B2 — Recovery is correct but not bounded; full-live-set snapshots  · **high**
WAL replay does `read_to_end` (`src/storage/wal.rs:852-857`) — unbounded memory. Snapshots
rewrite the **entire live record set of every topic** each checkpoint
(`src/engine/snapshot.rs:60-92`), so checkpoint cost and double-storage (segments + snapshot)
grow with total live data. Three analyses converge.
- **Borrows from:** PostgreSQL (checkpoint dirty pages from LSN), RocksDB (MANIFEST + SSTable metadata), InnoDB (redo checkpointing), Kafka (segment indexes).
- **Recommend:** stream WAL replay (bounded memory); make snapshots **incremental** — only topics changed since last snapshot, or lean on segments as the live-data store and shrink the snapshot to metadata + the un-sealed WAL tail.
- **Where:** `src/storage/wal.rs:852-857`, `src/engine/recovery.rs:82-181`, `src/engine/snapshot.rs:60-92`.

### B3 — Segment/tier store lacks LSM-style maintenance + intra-segment reclaim  · **med**
> **UPDATE — intra-segment reclaim is OUT OF SCOPE (decided).** Whole-segment-only reclaim is the
> intended design: deletion is no-compaction / no-reclaim, so dead bytes inside a still-live segment
> are retained on purpose (a delete-flag byte is flipped in place). Partial rewrite / LSM compaction
> will not be built. The cold-path fd/block-cache and filter-block ideas remain valid perf notes.

Reclaim is whole-segment only (`src/engine/topic_state.rs:135,1046`): a `match`-delete killing
99% of a 10k-record segment keeps the whole `.data`+`.idx` until the last record dies — dead
bytes pinned indefinitely (space amplification). No MANIFEST, no Bloom/filter blocks, no
partial rewrite. Cold reads are `open`+2-`pread` per record with no fd/block cache beyond a
1024-entry LRU (`src/engine/segwriter.rs:133`). Overlaps M8.
- **Borrows from:** RocksDB (MANIFEST, block index, Bloom filters, leveled/universal compaction, block cache), Kafka (segment indexes + retention).
- **Recommend:** add partial rewrite for delete-heavy segments (shares the coalescing pass from M8), a per-segment cached `.idx` + fd LRU on the cold path, and optional Bloom/filter blocks.
- **Where:** `src/storage/segment.rs:38-53`, `src/engine/topic_state.rs:135,1046`, `src/engine/segwriter.rs:133`.

### B4 — In-memory tag index ages poorly under sparse deletes / high fanout  · **med**
Tag postings are `BTreeMap<String, Vec<u64>>` (`src/engine/topic_state.rs:81`); high-fanout tags
and sparse base+offset holes (`VecDeque` deletions) age poorly. `range_all_dead` is O(range)
per sealed segment (`src/engine/topic_state.rs:135-144`) — see R/perf below.
- **Borrows from:** PostgreSQL/MySQL (B-trees), RocksDB (compressed postings, prefix iteration), Redis (listpack/compact stream).
- **Recommend:** chunked/bitmap postings, compact sparse holes, snapshot index state.
- **Where:** `src/engine/topic_state.rs:66-83,184-193,218-242`.

### B5 — Diff/SSE waiting adds latency + lacks subscriber flow control  · **med**
Waiters are registered *after* the final seq check (race window adding latency,
`src/http/topics.rs:192-212`); every watched topic is scanned per wake; per-subscriber buffers/
overflow policy absent. SSE re-serializes the whole cursor `BTreeMap` per frame under a mutex
(`src/http/watch.rs:759-762`).
- **Borrows from:** Kafka (fetch sessions + quotas), JetStream (flow-control heartbeats), Redis (blocking reads).
- **Recommend:** register waiters before the final seq check; add per-subscriber buffers + overflow policy; avoid scanning every topic per wake; cache the encoded session id.
- **Where:** `src/http/topics.rs:192-212`, `src/http/watch.rs:712-725,759-762`, `src/engine/topic_state.rs:654-655`.

### B6 — Queue lease durability weaker than durable consumer state  · **med**
`leases_durable` defaults `false` (`src/types.rs:73-76`); lease state is normally in-memory and
"self-heals" by re-claiming everything after crash (deliberate at-least-once). When durable, claims
are not WAL-logged before being exposed (`src/engine/queue.rs:574-587`, `src/engine/mod.rs:696-703`).
- **Borrows from:** JetStream (persists ack floor), Redis (PEL is stream state), Kafka (offsets by generation/member).
- **Recommend:** when `leases_durable=true`, WAL-log claims before exposing them and gate the claim response on the lease WAL result.
- **Where:** `src/engine/queue.rs:574-587`, `src/engine/mod.rs:696-703`, `src/types.rs:73-76`.

### B7 — API ergonomics rougher than turbopuffer/Kafka  · **med**
Make cursors opaque/versioned; implement-or-remove `watch.consistency`; persist or clearly mark
idempotency as best-effort; align watch auth behavior with docs (see R/inconsistency below). Also:
`diff` is POST-only (a `GET …/diff?from_seq=&limit=` alias would be cache/curl-friendly);
no `XAUTOCLAIM`-style claim-a-specific-seq; no multi-topic atomic write; `include_meta`
defaults true while `include_tags` defaults false (asymmetry, `docs/API.md:578-579`).
- **Borrows from:** turbopuffer (explicit namespace/query/upsert/delete), Kafka (stable diagnostic offsets), PostgreSQL (stable SQLSTATEs).
- **Recommend:** the above, prioritizing opaque cursors + resolving `watch.consistency`.
- **Where:** `src/types.rs:887-909`, `src/http/watch.rs:126-142`, `src/http/mod.rs:135-141`, `docs/API.md:578-579`.

### B8 — Resource limits / accounting are coarse + `bytes_retained` can drift  · **low**
Global, not per-tenant; `disable_backpressure` is not real; no token-bucket rate limit. Byte
accounting uses load→`saturating_sub`/`store` not `fetch_sub` (`src/engine/topic_state.rs:1020-1022,
1210-1212`), and the index lock is dropped before the byte store, so concurrent reclaim/delete can
lose an update — and the byte-cap eviction loop reads this possibly-drifted gauge
(`src/engine/topic_state.rs:945`). Over/under-eviction by design, but drift makes it worse.
- **Borrows from:** Kafka (quotas/write throttles), RocksDB (write stalls), PostgreSQL (connection/work-mem limits).
- **Recommend:** token-bucket rate limits; use atomic `fetch_*` for byte accounting or reconcile exact live-bytes after eviction; implement real `disable_backpressure` semantics.
- **Where:** `src/engine/mod.rs:401-418`, `src/types.rs:353-369`, `src/engine/topic_state.rs:945,1020-1022,1210-1212`.

### B9 — `list_topics` is O(N log N) over the whole registry per call  · **med**
`src/engine/mod.rs:1258-1266` iterates the entire `DashMap`, clones every key, sorts, *then*
paginates — page 2 of a 1M-topic registry still allocates+sorts 1M strings. `routers_for_source`
clones owned `Router`s on the hot write path (`src/engine/router.rs:158-164`).
- **Borrows from:** PostgreSQL/Kafka (indexed/paginated catalog scans).
- **Recommend:** maintain a sorted index for listing, or cursor-paginate without full collect+sort; avoid cloning routers on the write path.
- **Where:** `src/engine/mod.rs:1258-1266`, `src/engine/router.rs:158-164`.

### B10 — Hot-path allocation churn  · **low**
`wal_enqueue_batch` allocates a fresh `Arc<CommitState>` + channel send **per record**, so a
1000-record batch is 1000 sends/Arcs/tokens (`src/storage/wal.rs:1030-1041`,
`src/engine/mod.rs:731-749`). Forward path clones records again per dest
(`src/engine/mod.rs:1812-1825`). `dedupe.retain` is O(entries) per keyed write
(`src/engine/mod.rs:1474`).
- **Borrows from:** RocksDB/Kafka (per-batch, not per-record, commit accounting).
- **Recommend:** one commit token per batch; reuse buffers on forward; bound/streamline dedupe pruning.
- **Where:** `src/storage/wal.rs:1030-1041`, `src/engine/mod.rs:731-749,1474,1812-1825`.

---

## (3) INCORRECT / RISKY

### R1 — Router fanout: synchronous, serial, WAL-amplified, blocks the source ACK  · **high**
*Named the single biggest liability by both the codex pass and the perf deep-dive.*
`write()` calls `forward_from` on the request thread *before* returning
(`src/engine/mod.rs:1736-1747`); per route it does a full `durable_append` →
`token.wait()` cycle (`src/engine/mod.rs:770-828,1783-1889`). One source write to a topic with
K routers (+ chains, recursion at `:1888`) blocks the caller through K serial group-commit
fsync waits and physically writes 1+K WAL `Append` frames + 1+K segment copies — payload
duplicated N+1× on disk. No batching across dests, no async handoff despite the `mark_dirty`
scheduler hook.
- **Borrows from:** Kafka (stores once per partition; consumers fan out on read), JetStream (consumers share one stream).
- **Recommend:** prefer projection/consumer fanout over physical copy; failing that, async background forwarding (use the scheduler hook) and batch source+dest into one WAL transaction with idempotent dest keys.
- **Where:** `src/engine/mod.rs:1736-1747,1783-1889,770-828`.

### R2 — Router cursor over-advances → silent forward loss; no durable catch-up  · **high**
`note_forwarded` stores the *source head* regardless of how many records the filter dropped or
the dest rejected as backpressure (`src/engine/mod.rs:1800-1804,1854,1885`,
`src/engine/router.rs:177`). Forwarding is purely append-triggered, never cursor-driven — there is
**no background sweep that replays from `cursors`**, and source WAL replay does not re-drive
routers after crash. So the "at-least-once via the source log" claim in the comments
(`src/engine/mod.rs:1836,1863-1866`) is not implemented: a filtered/backpressured/crashed
forward is never reconsidered; a dest `discard:reject` overflow drops the forward permanently
(`src/engine/mod.rs:1854` `continue`). codex and the perf analysis agree this is a real
correctness gap, not just perf.
- **Borrows from:** Kafka Connect / outbox + consumer offsets, JetStream durable consumers, PostgreSQL logical replication slots.
- **Recommend:** make routers durable consumers over source topics with replay-from-cursor on restart; advance the cursor only after the dest append commits, and only by the count actually forwarded.
- **Where:** `src/engine/mod.rs:1800-1804,1854,1885`, `src/engine/router.rs:177`, `src/engine/recovery.rs:241-293`.

### R3 — `disk` class can ack page-cache-only records, then recover an earlier head and reuse seqs  · **high**
A `disk` write is acked before fsync; after power loss recovery can come back with an earlier head
and **reuse already-acknowledged seqs** (`src/engine/mod.rs:1637-1697`,
`src/engine/recovery.rs:241-293`, `src/types.rs:207-218`). Seq monotonicity-across-restart is
violated for the `disk` class. Coupled with B1.
- **Borrows from:** PostgreSQL/InnoDB (never report durable commit before WAL flush), Kafka (committed offsets monotonic within a partition).
- **Recommend:** persist a per-topic high-water mark / periodic fsynced watermark so recovered head never goes backward; or document `disk` precisely as lossy-with-possible-seq-rollback.
- **Where:** `src/engine/mod.rs:1637-1697`, `src/engine/recovery.rs:241-293`, `src/types.rs:207-218`.

### R4 — Queue ack/nack/extend keyed on `node`, not `lease_id` — stale-worker fencing gap  · **high**
> **UPDATE — RESOLVED (implemented).** ack/nack/extend now accept an optional `lease_ids` array and
> fence on it (`ack_fenced`/`lease_token_ok`, `src/engine/queue.rs`): **validate-when-supplied** — a
> stale token is rejected and that seq skipped — while remaining optional (legacy `node`+`seqs` match
> when omitted). The text below describes the old state.

The jobs path matches on `l.node == node` (`src/engine/queue.rs:624`) and does not require the
returned `lease_id` (`src/types.rs:704-750`, `src/engine/queue.rs:622-630,721-733,799-808`). A stale
worker reusing the same `node` after its lease expired and the job was re-delivered can ack/nack/extend
a *newer* delivery — wrong-delivery acknowledgement.
- **Borrows from:** JetStream (ack tokens), Redis (PEL ownership), Kafka (member-generation fencing).
- **Recommend:** require `lease_id` on ack/nack/extend and reject stale delivery tokens.
- **Where:** `src/types.rs:704-750`, `src/engine/queue.rs:622-630,721-733,799-808`.

### R5 — WAL submission unbounded despite `channel_cap`  · **high**
> **UPDATE — RESOLVED (implemented).** WAL ingest now uses a bounded channel with `try_send` →
> `WalError::Full` → `503` backpressure (`src/storage/wal.rs`). The text below describes the old
> state.

The WAL ingest uses an unbounded `mpsc::channel` (`src/storage/wal.rs:911-912,1129-1131,
1368-1373`), so a stalled WAL device turns backpressure into unbounded memory growth — the
configured `channel_cap` is not honored. (Related to B10's per-record sends.)
- **Borrows from:** RocksDB (write stalls), Kafka/NATS (max-pending), PostgreSQL (backends block on WAL pressure).
- **Recommend:** bounded queue honoring `channel_cap`; return 429/503 under pressure.
- **Where:** `src/storage/wal.rs:911-912,1129-1131,1368-1373`.

### R6 — Segment seal does fsync'd I/O while holding the publish gate  · **high**
> **UPDATE — RESOLVED (implemented).** Segment seal now runs off the publish gate
> (`publish_staged_no_seal` + off-gate `materialize_published`, used as the default path). The text
> below describes the old state.

`publish_staged` → `materialize_segment` → `seal_active` → `hot().put(...)` fsyncs inline
(`src/engine/topic_state.rs:652,762-831`, `src/engine/segwriter.rs:377`) *while the writer holds the
publish-gate turn* (`src/engine/mod.rs:1677-1696`). A slow seal fsync serializes every subsequent
same-topic writer's publish behind it — defeating the group-commit win whenever a seal boundary is
crossed on a write-heavy durable topic. (The index lock is correctly *not* held across it; the gate is.)
- **Borrows from:** PostgreSQL/InnoDB (background writer/checkpointer flush off the commit path).
- **Recommend:** move segment seal/fsync off the publish gate (background seal, gate released first).
- **Where:** `src/engine/topic_state.rs:652,762-831`, `src/engine/segwriter.rs:377`, `src/engine/mod.rs:1677-1696`.

### R7 — EvictWatermark / TTL / byte-cap eviction not always re-derivable  · **med**
`log_evict_watermark` is best-effort and swallowed (`src/engine/mod.rs:685`). The comment claims
eviction re-derives, but only the **records-cap** case is a pure function of `head-cap_records`. A
**byte-cap or TTL** floor that advanced (`src/engine/topic_state.rs:914-930,944-971`) and whose
watermark frame is lost is **not** re-derivable — after a backward clock or relaxed `cap_bytes`,
evicted records can resurrect on restart.
- **Borrows from:** PostgreSQL/InnoDB (eviction/vacuum decisions are WAL-logged, replay-deterministic).
- **Recommend:** make the evict watermark durable (not best-effort) for the TTL/byte-cap cases, or persist the resolved floor.
- **Where:** `src/engine/mod.rs:675-685`, `src/engine/topic_state.rs:914-930,944-971`.

### R8 — Mid-log corruption / recovery contiguity guard silently truncates  · **med**
Two faces of the same hazard. (a) `F-WAL-CRC-FLIP-MIDLOG` (`docs/FAULT_TESTING.md:73`): a mid-log
checksum (XXH3-64) flip on the active WAL is treated as logical EOF, silently discarding all later frames —
including acked `disk`/`fsync` records — with no alarm. (b) Recovery ignores any Append whose
`seq != head+1` (`src/engine/recovery.rs:279-281`): a torn *middle* (not just tail) drops the
higher frame and every later frame for that topic silently, converging to a truncated topic rather than
detecting corruption. Torn-tail handling is correct; torn-middle is the gap. The integrity scrub
(M7) is the mitigation.
- **Borrows from:** PostgreSQL/InnoDB (page checksums + explicit corruption errors), RocksDB (MANIFEST/SSTable metadata reject incomplete files).
- **Recommend:** distinguish torn-tail from torn-middle; surface mid-log/contiguity corruption as an error (and recover via M2 archived WAL / M1 replica) rather than silent truncation.
- **Where:** `src/storage/wal.rs:553-560`, `src/engine/recovery.rs:279-281`, `docs/FAULT_TESTING.md:73`.

### R9 — Segment completeness treats `.data` alone as authoritative  · **med**
`list/resolve` consider a segment complete on `.data` presence, but reads need the `.idx`
(`src/storage/segstore.rs:268-282,377-389`, `src/engine/segwriter.rs:133-156`). A `.data`-without-`.idx`
state can degrade into defensive `Null` payload fallback for sealed live records
(`src/engine/topic_state.rs:843-871`) — masking corruption as data loss.
- **Borrows from:** RocksDB (MANIFEST/SSTable metadata), InnoDB (page checksums reject incomplete files).
- **Recommend:** require both `.data` and `.idx` for completeness; surface segment read corruption instead of `Null` fallback for sealed live records.
- **Where:** `src/storage/segstore.rs:268-282,377-389`, `src/engine/segwriter.rs:133-156`, `src/engine/topic_state.rs:843-871`.

### R10 — Single durable copy after checkpoint (no full-page/redo retention)  · **med**
WAL frames are logical redo, not page images, and absorbed WAL is deleted at checkpoint. Once a
record lives only in a segment, a torn/bit-rot segment frame is *detected*
(`src/storage/segment.rs:208`) but **cannot be repaired** — the redo source is gone. PG's
`full_page_writes` re-derives; topics surfaces `Corrupt` with no recovery path. The root cause
behind M2/M7's urgency.
- **Borrows from:** PostgreSQL (`full_page_writes` + WAL retention), any replicated system (recover from a peer).
- **Recommend:** retain a redo window (don't drop absorbed WAL covering still-only-in-segment data until a second copy exists) — folds into M2 (archiving) and M1 (replica).
- **Where:** `src/engine/recovery.rs:187-198`, `src/storage/segment.rs:208`.

### R11 — WAL rotation failure is swallowed  · **med**
If next-WAL-file creation fails, the error is swallowed and the writer continues past the intended
preallocated boundary (`src/storage/wal.rs:1415-1435`).
- **Borrows from:** PostgreSQL/InnoDB/Kafka (log-allocation failure ⇒ commit failure or partition offline).
- **Recommend:** on rotation failure, fail the batch or enter read-only mode.
- **Where:** `src/storage/wal.rs:1415-1435`.

### R12 — `forward_from` recursion: O(routes) relocks + unbounded synchronous stack on chains  · **med**
Each recursion level re-locks `self.routers` (`src/engine/mod.rs:1789`) and `get_topic` per route;
a chain A→B→C recurses synchronously up to `MAX_ROUTER_HOPS` deep on the request thread, each hop
paying a fsync wait (R1). An `allow_cycle` diamond re-forwards the same record exponentially within
the hop budget. Folds into the R1 fix (async + batched fanout).
- **Borrows from:** Kafka/JetStream (read-side fanout, no recursive write amplification).
- **Where:** `src/engine/mod.rs:1789,1888`.

### R13 — Watch auth semantics internally inconsistent  · **low**
Whether `wid` is a bearer capability or requires Authorization differs across middleware, the stream
handler, and docs (`src/http/mod.rs:135-141`, `src/http/watch.rs:126-142,431-434`).
- **Borrows from:** Kafka/JetStream (stable scoped credentials for long-lived consumers).
- **Recommend:** decide the model and make middleware + handler + docs match.
- **Where:** `src/http/mod.rs:135-141`, `src/http/watch.rs:126-142,431-434`.

### R14 — `quiesce_publishes` can hang if a ticketed writer panics in the gate gap  · **low**
A panic after `next_publish_ticket` but before `publish_done` (`src/engine/mod.rs:1633`) leaves
`publish_next` stuck; `quiesce_publishes` and every later writer block forever
(`src/engine/topic_state.rs:726-732,699-701`). Lock poisoning only triggers if the panic *holds the
gate mutex*, not in the ticket gap.
- **Borrows from:** any system using poison-aware/condvar recovery on the commit sequencer.
- **Recommend:** make the gate panic-safe (release/advance the ticket on unwind, e.g. a guard).
- **Where:** `src/engine/mod.rs:1633`, `src/engine/topic_state.rs:699-701,726-732`.

---

## Disagreements / notes across sources
- **Topic-cardinality (M8)** is the storage deep-dive's #1 structural risk but absent from codex's list. Kept as high-impact **med** — it only bites at high topic counts, but coalescing also closes the intra-segment-reclaim gap (B3), so it earns its place.
- **Router fanout** appears as both a *perf* liability (R1/R12) and a *correctness* gap (R2). All three sources that touched it agree; merged into R1/R2/R12.
- **Compaction (M5)**, **read-time filter (M6)**, and **snapshot incremental-ization (B2)** were each independently nominated as "single highest-leverage." They serve different goals (store-vs-log capability, ergonomics, checkpoint cost). Reconciled in the Top 8 below by effort.
- The **crash core and supply chain** (CI, multi-arch Docker, SBOM/provenance, pinned lockfile) are praised by every source — do **not** rebuild these.

---

## Top 8 next things to build, in order (impact × effort)

> **UPDATE — historical.** Items 1 (metrics, M3), 2 (bounded drain, M11), 3 (queue `lease_id`
> fencing, R4), 4 (bounded WAL submission + per-batch token, R5/B10), and 6 (async + derived
> replay-from-cursor routers, R1/R2/R12) are **implemented**. Item 8's compaction / intra-segment
> reclaim half (M8/B3) is **out of scope** (no compaction / no reclaim by design; only the
> file-cardinality / packed-cold-store concern remains a valid note). The list is kept as the
> original prioritization record.

1. **Flesh out `/v0/metrics`** (M3) — wire existing `WalMetrics` + per-topic atomics + latency histograms + recovery/router-lag/queue/SSE gauges. *Days, huge ROI, zero new subsystems.* `src/http/health.rs:79-86`.
2. **Bounded drain + SSE wind-down** (M11) — timeout-race in `serve.rs` + signal SSE to close. *Small, fixes rolling-restart safety.* `src/serve.rs:146-149`.
3. **Require `lease_id` on queue ack/nack/extend** (R4) — close the stale-worker fencing bug. *Small, correctness.* `src/engine/queue.rs:622-630`.
4. **Bound WAL submission + per-batch commit token** (R5 + B10) — honor `channel_cap`, return 429/503, stop one-Arc-per-record. *Small–med, prevents OOM-under-stall.* `src/storage/wal.rs:911-912,1030-1041`.
5. **Read-time `filter` on `diff`/`watch`** (M6) — expose the existing `filters.rs` `Eq`/`Glob` tuples on reads. *Med, biggest ergonomic unlock, reuses existing machinery.* `src/engine/filters.rs`.
6. **Async + batched router forwarding, with a durable replay-from-cursor catch-up loop** (R1 + R2 + R12) — move fanout off the request thread, advance cursors only after dest commit, re-drive on restart. *Med–high, fixes the biggest perf liability AND a silent-loss correctness gap.* `src/engine/mod.rs:1736-1889`.
7. **Backup / archived-WAL retention / PITR + offline `verify`/`fsck` scrub** (M2 + M7 + R10) — retain absorbed WAL behind an archive hook, keep N snapshots, add restore + replay-to-point, and a full-scan integrity check reusing the XXH3-64 readers. *High effort, closes the single-durable-copy risk and gives operators a recovery path.* `src/engine/recovery.rs:187-198`, `src/storage/snapshot.rs:232-237`.
8. **Segment coalescing + intra-segment reclaim** (M8 + B3) — threshold-triggered merge of small/sparse sealed segments (and a packed cold store for many small topics), preserving the append-mostly zero-rewrite property in the common case. *High effort, the structural fix for topic-cardinality + sparse-delete space amplification.* `src/engine/topic_state.rs:1031-1089`, `src/engine/segwriter.rs`.

*Out of scope (will not be built):* WAL shipping / replication / HA (M1),
keyed / LSM compaction and a compacted topic type (M5) plus intra-segment per-record reclaim (B3/M8),
durable consumer groups as a server primitive (M4 — an app pattern instead), native TLS (M13 —
reverse proxy), and hard multi-tenancy beyond per-key scopes + prefix allowlists (M13). *Still a
valid future op concern:* backup / archived-WAL / PITR + offline scrub (M2/M7), incremental snapshots
(B2).
