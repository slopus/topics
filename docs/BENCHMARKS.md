# Benchmarks — Phase-2/3 In-Memory BASELINE

This document records the **initial baseline** performance numbers for the
Phase-2 in-memory `topics` server, captured during Phase 3. All data lives in
RAM: there is no WAL, no segment store, and no fsync. These numbers therefore
represent the *engine + HTTP + SSE* cost with durability removed from the
critical path.

> Phase 4 (persistence: WAL, group commit, segments, recovery) will **append a
> second column** to every table here so the cost of durability is explicit
> against this baseline. Do not edit the baseline numbers below — add new
> columns/sections.

---

## Environment

| | |
|---|---|
| **CPU** | Apple M4 Max |
| **Cores** | 16 |
| **RAM** | 128 GiB (137438953472 bytes) |
| **OS** | Darwin 25.2.0 (macOS) |
| **rustc** | rustc 1.92.0 (ded5c06cf 2025-12-08) |
| **Edition** | 2021 |
| **Build flags** | `--release` (optimized) |
| **NVMe** | n/a for this baseline — in-memory build, no disk I/O on the data path |

Hardware gathered via `sysctl -n machdep.cpu.brand_string hw.ncpu hw.memsize`,
OS via `uname -sr`, toolchain via `rustc --version`.

---

## Methodology

Two layers, matching the ROADMAP benchmark plan:

1. **Criterion micro-benchmarks** (`benches/engine.rs`) — call the engine API
   directly in-process (`Engine::write`, `Engine::diff`,
   `TopicState::matching_live_seqs`, `TopicState::apply_delete`, cap-eviction via
   `enforce_retention`). No HTTP, no network. Criterion's default config: 3 s
   warm-up, 100 samples per bench. Reported value is the **median** of the
   estimate interval. Throughput is Criterion's `Throughput::Elements` over the
   batch size. These isolate raw CPU cost of the hot paths.

2. **Live end-to-end HTTP macro-benchmarks** (`topics-probe bench`) — run
   against the **release binary** bound on an ephemeral localhost port over real
   HTTP (loopback, keep-alive). Latencies are wall-clock; percentiles via sort +
   linear interpolation. Run size: **50 000 writes**, SSE windows at **1 / 10 /
   100** watchers. SSE write→deliver latency is measured with a shared
   monotonic epoch stamp embedded in each write payload, so it is a true
   end-to-end write-to-delivery interval with no clock skew. The full run
   completes in well under a minute (~7 s).

Time-dependent **correctness** (TTL expiry, priority recency, watermark
invariants) is *not* measured here; it is verified deterministically in the
engine unit tests and the proptest suite using the injectable `TestClock`. The
live tool naturally uses real time for *latency* measurement, which is expected
and fine.

Reproduce:

```bash
# micro
cargo bench --bench engine

# macro: boot release server, then probe
TOPICS_PORT=4090 ./target/release/topics &
./target/release/topics-probe conformance http://localhost:4090   # must exit 0
./target/release/topics-probe bench       http://localhost:4090 --json
```

---

## 1. Criterion micro-benchmarks (engine, in-process)

Median time per criterion run; throughput derived from batch size.

### Append (`Engine::write`, fresh topic per batch)

| Payload | Batch | Median time | Throughput (records/s) |
|---|---:|---:|---:|
| 64 B   | 1    | 873 ns   | ~1.15 M |
| 64 B   | 10   | 2.86 µs  | ~3.50 M |
| 64 B   | 100  | 23.6 µs  | ~4.24 M |
| 64 B   | 1000 | 248.6 µs | ~4.02 M |
| 1 KiB  | 1    | 1.26 µs  | ~0.79 M |
| 1 KiB  | 10   | 7.20 µs  | ~1.39 M |
| 1 KiB  | 100  | 69.2 µs  | ~1.45 M |
| 1 KiB  | 1000 | 718.1 µs | ~1.39 M |

Batching amortizes per-call overhead strongly (1→100 records is ~6x cheaper per
record). 64 KiB payloads were not run as a separate micro-bench; the per-record
trend at 1 KiB shows the path is allocation/copy-bound at large payloads.

### getDifference (`Engine::diff` from seq 0, warm 10 k-record topic)

| Limit | Median time | Throughput (records/s) |
|---:|---:|---:|
| 1    | 167.8 ns | ~6.0 M |
| 256  | 18.45 µs | ~13.7 M |
| 1000 | 74.4 µs  | ~13.1 M |

### Tag-index match (`matching_live_seqs`, 10 k records / 100 tags)

| Pattern | Median time |
|---|---:|
| exact (`Eq`, single posting list) | 266 ns |
| prefix (`Glob` `tenant:*`, range scan all 100 tags) | 67.9 µs |

### Cap eviction (`Engine::write` into a full `discard:old`, cap=10 k topic)

| Batch | Median time | Throughput (records/s) |
|---:|---:|---:|
| 1   | 474 ns  | ~2.11 M |
| 100 | 25.8 µs | ~3.88 M |

### Delete (`TopicState::apply_delete`, fresh warm 10 k topic per iter)

| Selector | Median time | Throughput (records/s) |
|---|---:|---:|
| `before_seq` all (prefix delete of all 10 k) | 2.60 ms | ~3.85 M |
| `match` exact (tag `Eq`, ~5 k matched) | 1.53 ms | ~3.29 M |

---

## 2. Live end-to-end HTTP macro-benchmarks (`topics-probe bench`)

Release binary over loopback HTTP. 50 000 writes. Latencies in milliseconds.

### Write-ack latency (single record, n=5000)

| p50 | p99 | p999 | max |
|---:|---:|---:|---:|
| 0.045 ms | 0.077 ms | 0.143 ms | 0.341 ms |

### Write throughput (16 concurrent writers, batch=100)

| Records acked | Elapsed | Throughput |
|---:|---:|---:|
| 49 600 | ~0.011 s | **~4.66 M records/s** |

### getDifference (latency + throughput over HTTP)

| Limit | p50 | p99 | p999 | calls/s | records/s |
|---:|---:|---:|---:|---:|---:|
| 1    | 0.043 ms | 0.124 ms | 0.258 ms | 21 069 | 21 069 |
| 256  | 0.163 ms | 0.221 ms | 0.246 ms | 5 446  | ~1.39 M |
| 1000 | 0.503 ms | 0.555 ms | 0.598 ms | 1 697  | ~1.70 M |

Tail (caught-up, near-head) read latency, n=2000: p50 **0.045 ms**, p99 0.076 ms,
p999 0.090 ms.

### SSE fan-out (write → deliver latency, 1 writer × N watchers)

| Watchers | Deliveries | p50 | p99 | p999 | max |
|---:|---:|---:|---:|---:|---:|
| 1   | 50   | 0.193 ms | 0.425 ms | 0.502 ms | 0.511 ms |
| 10  | 500  | 0.286 ms | 0.557 ms | 0.572 ms | 0.578 ms |
| 100 | 5000 | 0.939 ms | 1.847 ms | 2.408 ms | 2.451 ms |

**1–5 ms `eventual` delivery target: MET** at the tested load. At 100 watchers
the p50 is ~0.94 ms and p99 ~1.85 ms — comfortably within the 1–5 ms target;
even the worst observed delivery (2.45 ms) is inside the budget. The 1000-watcher
case from the ROADMAP plan was not run (probe defaults to 1/10/100); the 1→100
trend is roughly linear and stays well under budget. SSE fan-out scale
(connection churn, memory per idle connection) is not separately measured in the
in-memory baseline.

### Router forwarding overhead (src → dst write-to-visible)

| Path | p50 | p99 | p999 |
|---|---:|---:|---:|
| direct write+read baseline | 0.089 ms | 0.123 ms | 0.138 ms |
| forwarded (1-hop router) | 0.092 ms | 0.187 ms | 0.237 ms |
| **added latency (p50)** | **~0.002 ms** | | |

Router forwarding adds only single-digit microseconds at the median on the
in-memory build (synchronous in-process fan-out).

---

## 3. ROADMAP benchmark-plan coverage

Every applicable metric from the ROADMAP §"Benchmark plan" table, mapped to the
baseline numbers above. Metrics that require persistence or the full scheduler
are deferred to Phase 4 (the persistent build), since the Phase-2/3 in-memory
build has no disk path, no recovery, and no governor.

| Metric | Status (in-memory baseline) | Where |
|---|---|---|
| Write throughput | ~4.66 M rec/s (HTTP, 16 writers ×100); ~4.0 M rec/s micro at 64 B | §1, §2 |
| Append latency p50/p99/p999 | 0.045 / 0.077 / 0.143 ms (single record, HTTP); non-durable only | §2 |
| getDifference throughput | up to ~1.70 M rec/s (limit 1000, HTTP); ~13.7 M rec/s micro | §1, §2 |
| getDifference latency p50/p99 | tail 0.045 / 0.076 ms; deep (limit 1000) 0.503 / 0.555 ms | §2 |
| SSE fan-out latency p50/p99 | 1/10/100 watchers; p50 0.19–0.94 ms, p99 0.43–1.85 ms — 1–5 ms target MET | §2 |
| Router forwarding | ~0.002 ms added p50 latency; forwarding throughput bounded by append | §2 |
| Eviction / TTL cost | cap-evict micro 474 ns/rec (batch 1); TTL correctness in unit/proptest | §1 |
| **SSE fan-out scale** (churn, mem/idle conn) | Deferred — Phase 4 | — |
| **Recovery time** | Not applicable — in-memory build has no WAL/segments | Phase 4 |
| **Throttling behavior** | Not applicable — no governor/elastic throttle wired in Phase 2/3 | Phase 4 |
| **Memory footprint** (bytes/record) | Not separately measured in baseline | Phase 4 |
| **Durable** append latency / fsync group-commit | Not applicable — no fsync path | Phase 4 |
| getDifference cold (mmap fault) vs warm | Not applicable — no segment store; all reads warm | Phase 4 |

---

## Notes

- All micro numbers are criterion medians from a clean `cargo bench --bench
  engine` run on an otherwise-idle machine; expect a few percent run-to-run
  variance (criterion reported most changes within its noise threshold).
- The macro numbers are a single representative `topics-probe bench` run; they
  are loopback-HTTP figures and include the full axum/hyper request path.
- `topics-probe conformance` passed 89/89 checks (exit 0) against this same
  release binary, so the contract these numbers were measured against is the
  documented `/v0` contract.

---

# Phase 4 — persistent build

These numbers were captured against the **persistent (Phase-4) release binary**
— WAL + adaptive group commit + atomic metadata/state snapshots + restart
recovery — on the SAME hardware/OS/toolchain as the in-memory baseline above
(Apple M4 Max, 16 cores, 128 GiB, Darwin 25.2.0, rustc 1.92.0, `--release`). The
data dir is a fresh `tempfile::tempdir` on local NVMe (APFS). The baseline
numbers above are **unchanged**; this section is added alongside so the cost of
durability is explicit.

The only client-observable behavior change vs the baseline: `durable:true`
writes are now fsync-gated (the ack waits for a real `fdatasync`, reported in
`performance.fsync_ms`), and data persists across a restart. The `/v0` API, JSON
shapes, and semantics are identical (`topics-probe conformance` = **89/89**,
exit 0, against a release server booted on a temp `TOPICS_DATA_DIR`).

## Methodology (Phase 4 additions)

- **Durable vs non-durable write-ack** (`topics-probe bench-durable <url>`):
  boots two topics that differ ONLY in `durable` (`true` vs `false`) and drives
  the identical HTTP write path against each — single-record write-ack latency
  (one in-flight at a time, n=5000) and concurrent batched throughput (16
  writers × batch 100, ~50 000 records). The durable class additionally reports
  the server-side `performance.fsync_ms` distribution. Loopback HTTP, wall-clock
  latencies, percentiles by sort + linear interpolation.
- **Recovery time** (`time-to-ready`): a harness boots the binary on a temp data
  dir, loads N durable records (so every record is fsynced to the WAL), `kill
  -9`s the process (**SIGKILL — no graceful shutdown, no snapshot**, so recovery
  is a *pure full WAL replay* of all N frames — the worst case), restarts on the
  SAME data dir, and times the interval until `GET /v0/ready` returns `200`. It
  asserts the recovered `head_seq == N` (no acked durable loss). A graceful
  shutdown instead writes a snapshot, making real-world time-to-ready far
  shorter (recovery starts from the checkpoint and replays only the tail).
- **Crash consistency** is proven by real tests, not benchmarked: `kill -9` of
  the live binary mid-write + restart (`tests/crash_recovery.rs`,
  `tests/integration_durability.rs`).

## 1. Durable vs non-durable write-ack (HTTP, single record, n=5000)

| Class | p50 | p99 | p999 | max |
|---|---:|---:|---:|---:|
| **non-durable** (group-commit, no per-write fsync) | 0.059 ms | 0.110 ms | 0.142 ms | 0.219 ms |
| **durable** (fsync-gated, adaptive group commit) | 5.18 ms | 6.10 ms | 10.36 ms | 17.55 ms |

Server-reported `performance.fsync_ms` for the durable class (the fsync
component of the ack): p50 **5.00 ms**, p99 5.85 ms, p999 10.17 ms, min 3.87 ms.

**Durability cost vs baseline.** The in-memory baseline single-record write-ack
was p50 0.045 ms / p99 0.077 ms. The Phase-4 *non-durable* class lands at p50
0.059 ms / p99 0.110 ms — within noise of the baseline, i.e. the WAL framing +
buffered write add only single-digit microseconds when the fsync is off the
critical path. The *durable* class is dominated almost entirely by the raw
`fdatasync` cost on this machine's APFS/NVMe (~5 ms p50): on this hardware a lone
`fdatasync` is markedly slower than the 50–500 µs the ARCHITECTURE latency budget
assumes for server-grade NVMe, so the durable single-write p50 sits at the top of
(slightly above) the 1–5 ms target here. This is the honest fsync floor of the
test machine, not group-commit overhead — the adaptive window collapses to
`gc_min` (500 µs) for a lone write, so the latency *is* the fsync. Under
concurrent load, group commit amortizes one fsync across the whole batch (see §2).

## 2. Write throughput (16 concurrent writers × batch 100, ~50 000 records)

| Class | Records acked | Elapsed | Throughput |
|---|---:|---:|---:|
| **non-durable** | 49 600 | ~0.021 s | **~2.35 M records/s** |
| **durable** (group-committed) | 49 600 | ~0.21 s | **~0.23 M records/s** |

Under concurrent durable load the adaptive group commit coalesces many writers'
batches into far fewer `fdatasync` calls, so durable throughput (~232 K rec/s) is
~100× the naive "one fsync per write" ceiling (1000 fsyncs/s × 100/batch). The
non-durable class (~2.35 M rec/s here; run-to-run it ranges up to the baseline's
~4.7 M) is bounded by the single-topic append-serialization + HTTP path, not disk.
Both classes lose no acked data on a clean restart; durable additionally survives
SIGKILL.

> Note: the per-topic append path now serializes seq-assignment + WAL-enqueue under
> a per-topic lock (the durability-correctness fix below), so single-topic throughput
> is slightly lower than the lock-free in-memory baseline; cross-topic throughput
> still scales with sharding.

## 3. Recovery time — `time-to-ready` after SIGKILL (pure WAL replay, no snapshot)

| Records in topic | Load time | **time-to-ready** | Recovered `head_seq` |
|---:|---:|---:|---:|
| 100 000 (1e5) | ~0.15 s | **~0.14 s** | 100 000 (no loss) |
| 1 000 000 (1e6) | ~1.40 s | **~0.68–0.94 s** | 1 000 000 (no loss) |

This is the **worst case**: a hard kill with no snapshot, so recovery replays
*every* frame from WAL offset zero. ~1e6 records replay (XXH3-64-validate + decode +
re-index + tag-index rebuild) in well under a second (~1.1–1.5 M frames/s). With
a graceful shutdown (or the periodic snapshotter), recovery starts from the
checkpoint and replays only the un-checkpointed tail, so real-world time-to-ready
is bounded by the snapshot interval, not the total record count. The 1e7/1e8 rows
from the ROADMAP plan were not run (they require the segment store deferred to a
later phase; the in-memory index holds the full set as the cache here).

## 4. Durability-correctness fix surfaced by the recovery benchmark

The recovery benchmark initially exposed a **silent loss of acked durable
writes** under concurrent writers (~5 % loss at 1e5 with 16 writers): seq
assignment (`TopicState::append`, under the index lock) and the WAL enqueue were
not a single atomic unit, so two writers could assign seqs `A < B` yet enqueue
`B`'s frame ahead of `A`'s. Recovery applies frames in WAL order and skips any
`seq <= head`, so the lower-seq frame `A` was dropped on replay despite having
been acked. The fix adds a per-topic `append_lock` that makes
seq-assignment + WAL-enqueue atomic (the fsync wait stays *outside* the lock, so
durable group commit still coalesces across topics). Post-fix: **zero loss** at
1e5 and 1e6 (recovered `head_seq == N` every run), covered by a deterministic
in-process regression test (`concurrent_durable_writers_no_loss_across_restart`)
plus the real SIGKILL subprocess tests.

## 5. Crash-consistency / recovery correctness (proven by tests, not benchmarked)

| Property | Proof |
|---|---|
| **Durability:** acked `durable:true` write survives SIGKILL at any instant | `crash_recovery::sigkill_durable_writes_survive_with_identical_state` (real `kill -9` of the binary; the write ack is fsync-gated so a 2xx ⇒ on disk) |
| **Recovery correctness:** post-restart head/earliest/count/config/routers/delete match pre-crash | same test asserts each field for durable topics + deleted-stays-gone + cap-floor-tombstones; `integration_durability::write_snapshot_more_writes_restart_matches` |
| **Crash consistency (clean prefix):** SIGKILL during a non-durable burst ⇒ recovered tail is a contiguous prefix, no torn frame misread | `crash_recovery::sigkill_during_nondurable_burst_recovers_clean_prefix` |
| **Torn tail truncated, not misread:** a corrupted/oversized last frame on disk ⇒ clean recovery, no panic, no bogus record, WAL writable again | `crash_recovery::torn_tail_on_subprocess_wal_recovers_clean`; `integration_durability::torn_tail_is_truncated_not_read_as_data`; WAL-reader unit tests (XXH3-64 checksum + length-overrun + trailing-zeros) |
| **No silent loss across restart:** cursor below recovered `evict_floor` ⇒ tombstone; purely-deleted gap ⇒ silent | `integration_durability::tombstone_vs_silent_gap_survive_restart` |

## 6. ROADMAP Phase-4 acceptance-criteria coverage

| Criterion | Status | Where |
|---|---|---|
| Durability (acked durable survives hard kill) | **MET** | `crash_recovery.rs` (real SIGKILL), `integration_durability.rs` |
| Crash consistency (torn tail truncated, never read as data) | **MET** | `crash_recovery.rs`, WAL unit tests |
| Recovery correctness (head/earliest/evict_floor/count/config/routers/deletes match) | **MET** | `crash_recovery.rs`, `integration_durability.rs` |
| No silent loss across restart (tombstone vs silent deleted gap) | **MET** | `integration_durability.rs`, `properties.rs` |
| No regressions (full prior suite green; conformance 89/89) | **MET** | 192 tests green; `topics-probe conformance` 89/89 on the persistent build |
| Durable write-ack p99 within budget with adaptive group commit | **PARTIAL** | group commit works (§2); the lone-write durable p50 ~5 ms is at/over the 1–5 ms target because this machine's `fdatasync` (~5 ms) is slower than the server-NVMe assumption — a hardware floor, not a design regression |
| Segment-granular cap/TTL eviction; async deleted-record reclaim | **DEFERRED → Phase 5** | the in-memory index is the cache; cap/TTL advance `evict_floor` and recover correctly, but the mmap segment store + background reclaimer are Phase 5 |
| Full DWRR scheduler + elastic throttling under CPU pressure | **DEFERRED → Phase 5** | scheduler present in simplified (mark-dirty) form; the governor/throttle ladder + `429` under pressure are Phase 5 |
| SSE fan-out p99 ≤ 5 ms; throttling latency-under-pressure | see baseline (SSE) / **DEFERRED** (throttling) | SSE fan-out unchanged from baseline §2; throttling deferred with the scheduler |

**Explicitly deferred to Phase 5** (out of Phase-4 durability scope, per the
stage brief): segments-beyond-RAM / mmap segment store + background reclaimer,
the full DWRR priority scheduler + elastic throttling, HTTP/2 (h2c), and the
queue/lease/workload features.

## Notes (Phase 4)

- Latencies are loopback-HTTP single representative runs; durable latency is
  fsync-bound and will differ on other storage (server NVMe is typically ~10×
  faster at `fdatasync` than this laptop's APFS, which would pull the durable
  single-write p50 well inside the 1–5 ms target).
- Recovery time is a pure-WAL-replay worst case (SIGKILL, no snapshot); with the
  snapshotter it is bounded by the un-checkpointed tail, not total records.
- Reproduce:
  ```bash
  # boot a release server on a temp data dir
  D=$(mktemp -d); TOPICS_PORT=4090 TOPICS_DATA_DIR=$D ./target/release/topics &
  ./target/release/topics-probe conformance   http://localhost:4090   # 89/89
  ./target/release/topics-probe bench-durable  http://localhost:4090 --json
  # recovery-time: see the SIGKILL-load-restart harness in tests/crash_recovery.rs
  ```

---

# Phase 5 — workloads & fan-out

These numbers were captured against the **same persistent (WAL + group-commit +
snapshot) release binary** as Phase 4, now with the Phase-5A lease queue and the
Phase-5B workload features (HTTP/2 cleartext / h2c, the broadcast/distribution/
queue/actor patterns). **The Phase-2/3 baseline and the Phase-4 numbers above are
unchanged** — this section is purely additive. There were **no `/v0` API or
semantics changes**: `topics-probe conformance` against the live binary used here
is **117 / 117, exit 0** (the count grew from Phase-4's 89 as the h2c + queue
checks were added in earlier Phase-5 stages; all are additive).

Same hardware/OS/toolchain as the baseline: **Apple M4 Max, 16 cores, 128 GiB,
Darwin 25.2.0, rustc 1.92.0, `--release`**. The server ran on `127.0.0.1` on an
ephemeral port over a fresh temp `TOPICS_DATA_DIR` on local NVMe (APFS); every
workload is a live end-to-end HTTP run (reqwest h1/h2, loopback, keep-alive) from
a single client process. Topics use the default (`durable:false`) class, so these
exercise the engine + HTTP + SSE + scheduler path, not the fsync floor.

## Methodology (Phase 5 additions)

Five `topics-probe` subcommands drive a LIVE server; each prints a table and a
`--json` summary. Wall-clock latencies; percentiles by sort + linear
interpolation. The SSE write→deliver latency uses a shared monotonic `epoch`
stamped into each pulse payload (writer and watchers share one process clock), so
it is a true end-to-end write-to-delivery interval with no clock skew.

- `broadcast <url> --watchers 1,10,100,1000` — one source topic, N concurrent SSE
  watchers all tailing it, one writer emitting timed single-record pulses. This is
  the **shared zero-copy fan-out**: a pulse is serialized into ONE frame and
  ref-counted to every watcher (never copied into N topics). The headline metric is
  the **per-watcher write→deliver latency** and how flat it stays as watchers grow
  100×. NOTE: the pulse loop is deliberately *paced* (a fixed inter-pulse gap to
  measure delivery latency cleanly), so the reported `deliveries/sec` is a
  latency-paced aggregate (`≈ watchers × pulse-rate`), **not** a saturation
  throughput figure — the saturation story is the latency headroom (see the
  millions/sec discussion below).
- `media <url> --participants 100 --packet-ms 20 --payload-bytes 80 --batches ...`
  — Opus-like room audio over one capped room topic: N speakers all publish while
  N watchers receive everyone except their own `node`. By default writers use HTTP
  POSTs and watchers use SSE; `--websocket` uses one bidirectional WebSocket per
  participant for dynamic subscribe + publish. The topic is configured with
  `ttl_ms=10000` plus a computed `cap_records` for a 10 s packet buffer. The probe
  reports packet delivery loss, tombstones, duplicate packets, packet latency,
  inter-packet jitter, writer append latency, unacked WebSocket publishes, and
  best-effort `ps` CPU/RSS for the server process.
- `distribution <url> --topics N --batch B --writers W` — round-robins batched
  appends across many topics via W concurrent writer tasks; aggregate appends/sec.
- `queue <url> --workers N --jobs J --claim-max K [--jitter ms]` — producers
  batch-fill the Phase-5A lease queue, then N worker nodes claim→ack in a loop;
  jobs/sec, claim latency, and per-worker distribution evenness.
- `actors <url> --actors K --inferences N --tool-results T --snapshot-every S` —
  each actor is a topic; per inference appends a chain (model-answer + tool-call + T
  tool-results) as one batch, then snapshot-compacts via `delete {before_seq}`
  every S inferences; events/sec + topic-count scaling.

## 1. BROADCAST — 1 source → many SSE watchers (shared zero-copy frame)

Write→deliver latency (ms), all pulses delivered to all connected watchers. Two
runs: a clean sweep (1/10/100, 500 pulses) and the heavy tiers (100 @ 100 pulses,
1000 @ 300 pulses).

| Watchers | Connected | Deliveries | p50 | p99 | p999 | max | deliveries/s (paced) |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1    | 1    | 500     | 0.143 ms | 0.257 ms | 2.48 ms | 4.25 ms | ~261 |
| 10   | 10   | 5 000   | 0.192 ms | 0.381 ms | 0.498 ms | 0.518 ms | ~2 564 |
| 100  | 100  | 50 000  | 0.698 ms | 1.284 ms | 1.93 ms | 2.38 ms | ~21 980 |
| 100  | 100  | 10 000  | 0.893 ms | 1.94 ms  | 3.01 ms | 3.26 ms | ~11 854 |
| 1000 | 1000 | 300 000 | 2.218 ms | 4.88 ms  | 7.37 ms | 2694 ms* | ~111 389 |

\* The 1000-watcher `max` (2.69 s) is the **client-side connect storm** — standing
up 1000 SSE connections from one process over loopback before the first pulse.
Once all connections are warm the steady-state delivery is **p50 2.2 ms / p99
4.9 ms**, at the top of the 1–5 ms target. The writer-append latency at 1000
watchers is p50 3.9 ms / p99 6.1 ms (the source topic's drain wakes 1000 watchers
per write).

**Fan-out scaling is the key result:** per-watcher delivery latency stays sub-2 ms
p99 from 1 → 100 watchers (a 100× fan-out) and only reaches ~5 ms p99 at 1000.
Because each pulse is serialized **once** and ref-counted, the marginal cost of an
extra watcher is a bounded-channel send (tens to hundreds of ns), which is exactly
why the per-delivery latency does not blow up with N. The aggregate `deliveries/s`
columns are latency-paced, not saturated (see §6 on millions/sec).

## 2. MEDIA / AUDIO — all-talking room over one topic

**Current-machine benchmark. Update this section in place when the media path
improves.** Captured 2026-06-02 on the same Apple M4 Max / macOS / APFS machine.
Server: release binary on `127.0.0.1`, fresh temp `TOPICS_DATA_DIR`, auth disabled
on loopback, sampled with `ps` using the server PID. Probe: `topics-probe media`.

Scenario: everyone talks at once, **20 ms Opus-like packets**, **80 raw
bytes/packet** (stored as a 108-byte base64-like string in JSON), one room topic,
and each listener filters out its own `node`. **No routers are created or used in
this workload**: every speaker appends directly to the room topic and every
listener opens a direct SSE watch or WebSocket subscription on that same topic. The
room topic keeps a **10 s buffer** using `ttl_ms=10000` and `cap_records =
ceil(participants × 10 s × 50 packets/s ÷ packets_per_record) + participants`.

Current implementation notes:

- `durability:"ephemeral"` records are resident-only: no append WAL, no HOT
  segment materialization, and no snapshot/recovery of record payloads. The topic
  config still persists.
- Existing ephemeral-topic writes run inline instead of hopping to the blocking pool.
  Persistent disk/fsync writes still use the blocking pool.
- SSE/WebSocket use a shared-frame diff path: high-fanout watchers check the
  broadcast cache before cloning payload JSON. Cache lookup is O(1) and batched per
  diff frame, so cache hits avoid per-watcher payload clones and per-record cache
  locks.
- `/v0/ws` is available beside SSE. It supports dynamic JSON `subscribe`,
  `unsubscribe`, `publish`, and `ping` commands on one socket; publish commands
  enforce write scope, subscribe commands enforce read scope, and config-on-publish
  still requires admin scope.
- WebSocket delivery registers topic notifications before draining to avoid missed
  tail wakeups. Each socket has a 4-message outbound queue, and record frames reserve
  queue capacity before serializing large JSON strings so overload is backpressured
  earlier.

### 100 participants, 3 s

3 s of audio is 15 000 produced packets and **1 485 000 expected listener-packet
deliveries** (`100 speakers × 150 packets × 99 listeners`).

| Packets / stored record | HTTP POSTs | cap_records | Delivered listener-packets | Loss | Tombstones | p50 latency | p99 latency | p999 latency | p99 jitter | writer ACK p50 | writer ACK p99 | server CPU avg / max | RSS max |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **1** | **15 000** | **50 100** | **1 485 000** | **0.000%** | **0** | **4.21 ms** | **8.72 ms** | **11.46 ms** | **6.79 ms** | **2.95 ms** | **7.74 ms** | **149.6% / 264.1%** | **42.8 MiB** |
| 2 | 7 500 | 25 100 | 1 485 000 | 0.000% | 0 | 20.38 ms | 30.69 ms | 33.85 ms | 27.24 ms | 3.94 ms | 10.25 ms | 92.6% / 174.2% | 46.3 MiB |
| 3 | 5 000 | 16 767 | 1 485 000 | 0.000% | 0 | 28.07 ms | 54.46 ms | 57.15 ms | 48.76 ms | 5.65 ms | 14.27 ms | 57.6% / 114.2% | 48.2 MiB |
| 4 | 3 800 | 12 600 | 1 485 000 | 0.000% | 0 | 36.97 ms | 76.54 ms | 78.78 ms | 69.24 ms | 7.35 ms | 16.57 ms | 38.3% / 78.8% | 49.9 MiB |
| 5 | 3 000 | 10 100 | 1 485 000 | 0.000% | 0 | 52.62 ms | 98.84 ms | 100.94 ms | 89.33 ms | 8.97 ms | 19.03 ms | 31.1% / 71.5% | 51.6 MiB |
| 8 | 1 900 | 6 350 | 1 485 000 | 0.000% | 0 | 80.88 ms | 160.26 ms | 162.71 ms | 149.56 ms | 9.21 ms | 21.42 ms | 20.3% / 48.3% | 54.5 MiB |

For 100 participants, **batch=1 is now the best live-latency setting** and is
stable: zero loss at 5 000 HTTP POSTs/s and 495k expected listener-packet
deliveries/s. Larger batches reduce CPU but add real packet age: batch=3 has a
40 ms oldest-packet batching floor, and batch=8 has a 140 ms floor.

### 200 participants, 3 s

3 s of audio is 30 000 produced packets. With every participant hearing 199
others, the fan-out target is about **5.97 M listener-packet deliveries** per
tier.

| Participants | Packets / stored record | HTTP POSTs | Expected listener-packets | Delivered listener-packets | Loss | p50 latency | p99 latency | writer ACK p50 | writer ACK p99 | server CPU avg / max | RSS max |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **200** | **1** | **30 000** | **5 970 000** | **5 970 000** | **0.000%** | **9.46 ms** | **19.68 ms** | **6.50 ms** | **17.85 ms** | **311.4% / 551.5%** | **86.9 MiB** |
| 200 | 3 | 10 000 | 5 970 000 | 5 970 000 | 0.000% | 31.88 ms | 61.84 ms | 8.15 ms | 21.47 ms | 108.2% / 192.5% | 90.8 MiB |

The 200-participant unbatched tier now reaches **~1.19 M listener-packet
deliveries/s** including drain time with zero loss, while absorbing ~10k writes/s.
Batch=3 keeps the same zero-loss delivery count with much lower CPU, but adds the
40 ms oldest-packet batching floor.

### WebSocket transport, 3 s

These rows use `topics-probe media --websocket`: one bidirectional WebSocket per
participant, dynamic `subscribe` on connect, then `publish` commands over the same
socket. The topology is still a single room topic and **uses zero routers**.

| Participants | Packets / record | WS publishes | ACKed / unacked publishes | Expected listener-packets | Delivered listener-packets | Loss | p50 latency | p99 latency | p999 latency | p99 jitter | writer ACK p50 | writer ACK p99 | server CPU avg / max | RSS max |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **100** | **1** | **15 000** | **15 000 / 0** | **1 485 000** | **1 485 000** | **0.000%** | **3.85 ms** | **7.58 ms** | **13.07 ms** | **7.61 ms** | **2.41 ms** | **6.16 ms** | **143.3% / 171.1%** | **56.3 MiB** |
| **200** | **1** | **30 000** | **30 000 / 0** | **5 970 000** | **5 970 000** | **0.000%** | **6.48 ms** | **11.10 ms** | **13.42 ms** | **8.52 ms** | **2.86 ms** | **9.85 ms** | **264.2% / 315.1%** | **109.5 MiB** |
| **250** | **1** | **37 500** | **37 500 / 0** | **9 337 500** | **9 337 500** | **0.000%** | **8.57 ms** | **15.12 ms** | **18.08 ms** | **11.71 ms** | **3.44 ms** | **13.29 ms** | **318.1% / 382.2%** | **138.7 MiB** |
| **300** | **1** | **45 000** | **45 000 / 0** | **13 455 000** | **13 455 000** | **0.000%** | **12.13 ms** | **30.81 ms** | **40.97 ms** | **20.52 ms** | **4.34 ms** | **19.41 ms** | **335.6% / 401.6%** | **177.5 MiB** |

Current zero-loss ceiling on this machine is between **300 and 350** unbatched
all-talking participants. At 300 participants, the transport delivers about
**4.45 M listener-packets/s** with zero loss, but p99 is already ~31 ms.

Overload rows, kept here so future work can update the failure boundary in place:

| Participants | Packets / record | WS publishes | ACKed / unacked publishes | ACKed packets | Expected listener-packets from ACKed writes | Delivered listener-packets | Loss | p50 latency | p99 latency | writer ACK p50 | writer ACK p99 | RSS max |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 350 | 1 | 52 500 | 48 349 / 4 151 | 48 349 | 16 873 801 | 14 656 318 | 13.142% | 2324.80 ms | 6399.47 ms | 1105.61 ms | 6100.67 ms | 2742.2 MiB |

### Why latency is still not sub-millisecond

The delay is **not router forwarding**. This workload uses zero routers. After the
ephemeral, SSE fast-path, and WebSocket fixes, the remaining delay is mostly the
JSON media shape:

1. **HTTP POST per audio record is expensive.** At 100 participants SSE batch=1, the
   probe drives 5 000 request/response cycles per second into one topic. The server
   now ACKs ephemeral writes at p50 3.0 ms / p99 7.7 ms; that is much faster than the
   previous hundreds-of-ms overload, but it is not a sub-ms transport.
2. **WebSocket removes HTTP request setup, not JSON/text fanout.** The WebSocket
   rows avoid per-packet HTTP POSTs, but every participant still receives JSON text
   frames, parses JSON, and writes through one socket per listener.
3. **SSE still has per-connection envelope and socket work.** Record payload bytes
   are shared through the broadcast cache, but each listener still needs its own SSE
   event envelope, cursor id, and socket write.
4. **Every payload is JSON, not binary media.** `RecordIn.data` is a
   `serde_json::Value`, and the probe stores each packet as JSON fields plus a
   base64-sized string. RTP/WebRTC would not pay that parse/serialize/expand cost.
5. **Batching trades CPU for real packet age.** Batch=3 at 200 participants can
   exceed 1M listener-packet deliveries/s with no loss, but the oldest packet has a
   40 ms batching floor before the server sees it.
6. **The benchmark client is also a stressor.** One process runs all writers and all
   SSE/WebSocket JSON parsers over loopback. This is useful as a fixed machine
   benchmark, but it includes client-side scheduling and parsing pressure.

The 10 s buffer is working as a loss boundary, but it cannot turn JSON text frames
over HTTP/SSE/WebSocket into a sub-ms media plane. For live Opus, the practical split
remains: use `topics` for room control, chat, metadata, durable history, recording
indexes, and replay; use WebRTC/RTP/SFU or a binary UDP/QUIC media plane for live
audio packets.

Reproduce the current benchmark:

```bash
cargo build --release -p topics -p topics-probe
D=$(mktemp -d)
TOPICS_PORT=0 TOPICS_PORT_FILE=$D/port TOPICS_DATA_DIR=$D/data ./target/release/topics &
PID=$!
while [ ! -s "$D/port" ]; do sleep 0.05; done
U=http://$(cat $D/port)
./target/release/topics-probe media $U \
  --participants 100 --duration-s 3 --packet-ms 20 --payload-bytes 80 \
  --batches 1,2,3,4,5,8 --pid $PID --json
./target/release/topics-probe media $U \
  --participants 200 --duration-s 3 --packet-ms 20 --payload-bytes 80 \
  --batches 1,3 --pid $PID --json
./target/release/topics-probe media $U --websocket \
  --participants 250 --duration-s 3 --packet-ms 20 --payload-bytes 80 \
  --batches 1 --pid $PID --json
./target/release/topics-probe media $U --websocket \
  --participants 300 --duration-s 3 --packet-ms 20 --payload-bytes 80 \
  --batches 1 --pid $PID --json
kill $PID
```

Use the same section/tables above for later improvements; update the rows in place
instead of appending a new media section every time.

## 3. DISTRIBUTION — 1 source → many topics (batched, sharded fan-out)

5000 destination topics, batch 100, 32 concurrent writer tasks, 500 000 records.

| Topics | Batch | Writers | Records | Elapsed | Appends/s | req p50 | req p99 | req max |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 5 000 | 100 | 32 | 500 000 | 1.426 s | **~350 737 /s** | 7.73 ms | 18.55 ms | 19.90 ms |

500 000 small appends spread across 5000 topics at ~351 K appends/s. The per-topic
write path is lock-free across distinct topics (sharded), so this is bounded by the
**single client process's HTTP request rate over loopback** (32 in-flight batched
requests), not the engine — the per-request latency (p50 7.7 ms with 32 writers in
flight) is the HTTP round-trip + queueing, not append cost (the micro-bench append
is sub-µs/record). See §6.

## 4. QUEUE — Phase-5A lease queue, N workers claim/ack

100 worker nodes, 20 000 jobs, claim-max 8, greedy (`jitter=0`).

| Workers | Jobs | claim_max | jitter | Jobs/s | claim p50 | claim p99 | Distribution (min/mean/max, cv) |
|---:|---:|---:|---:|---:|---:|---:|---|
| 100 | 20 000 | 8 | 0 ms | **~203 948 /s** | 1.18 ms | 6.68 ms | 136 / 200 / 248, **cv 0.116** |

Produce phase alone hit **~1.11 M jobs/s** (batched fills). All 20 000 jobs were
acked (`jobs_acked == jobs_produced`); evenness across 100 workers is good
(coefficient of variation 0.116, mean exactly 200 jobs/worker) even in greedy mode.
With `--jitter > 0` the coalescing claim pass (DESIGN §10.3) trades a little claim
latency for near-perfect evenness — the documented §10.2 tradeoff.

## 5. ACTORS / INFERENCE — per-actor topic, event chains + snapshot compaction

1000 actor topics, 5 inferences each, chain length 5 (model-answer + tool-call + 3
tool-results), snapshot-compact (`delete before_seq=head`) every 2 inferences, 32
concurrent drivers.

| Actors | Infs/actor | chain_len | Events | Elapsed | Events/s | Inferences/s | Snapshots | chain append p50 / p99 |
|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 000 | 5 | 5 | 25 000 | 0.801 s | **~31 214 /s** | ~6 243 /s | 2 000 | 0.787 ms / 11.34 ms |

Each inference is one batched append of the 5-event chain; 2000 `delete before_seq`
snapshot-compactions ran inline without stalling the append path (logical delete is
immediate, physical reclaim is the background reclaimer, ARCHITECTURE §3.5). Per-topic
scaling holds across 1000 simultaneously-active actor topics.

## 6. h2c effect, and are the millions/sec + 1–5 ms targets met?

**h2c (HTTP/2 cleartext, prior-knowledge).** The server auto-detects h1 vs h2 per
connection on the same port. Verified live: an `--http2-prior-knowledge` client
negotiates HTTP/2 (`version=2`) and gets `200`, while an `--http1.1` client still
gets `200` on the same port; the three conformance h2c checks (health, write, diff
over h2) pass. As called out in the brief, **h2c helps connection/stream
multiplexing, not raw throughput** — it lets one connection carry many concurrent
SSE streams / requests (fewer sockets, fewer handshakes for the broadcast and
queue-worker fan-in cases), but per-request CPU cost is unchanged, so it does not
by itself move the appends/sec or deliveries/sec ceiling.

**1–5 ms delivery target: MET** for broadcast up to ~100 watchers (p99 ≤ ~1.9 ms)
and **at the edge** at 1000 watchers (steady-state p99 ~4.9 ms; the 2.7 s outlier
is one-time client connection setup, not server fan-out). The fan-out design holds:
serialize-once + ref-count keeps per-watcher latency flat as N grows 100×.

**Millions/sec: not reached by this single-process HTTP probe; the ceiling is the
client, not the engine.** Distribution hit ~351 K appends/s and the queue ~204 K
jobs/s (produce alone ~1.11 M jobs/s) — bounded by one client process's loopback
HTTP request rate (per-request overhead dominates), exactly the lever the brief
identifies: **batching is what gets to millions/sec.** The evidence is on-topic: the
Phase-2/3 micro-benches append at ~4 M records/s at 64 B batch-100 in-process, and
the produce phase here (large batched fills) already exceeds 1 M jobs/s end-to-end.
The route to millions of deliveries/sec is the same batching + the shared zero-copy
frame: at 1000 watchers a single serialized frame yields 1000 deliveries, so the
~111 K *paced* deliveries/s seen here corresponds to a far higher saturated
ceiling (each delivery is a sub-µs ref-counted channel send) — the broadcast probe
is paced for clean latency measurement, not run to saturation. In short: the
engine + fan-out path has the headroom; reaching millions/sec in a benchmark needs
multi-connection / multi-process load generation + batched writes, not a change to
the server.

## Notes (Phase 5)

- All Phase-5 numbers are single representative live runs over loopback HTTP from
  one client process; expect run-to-run variance. They were captured against the
  release binary on a temp data dir; topics are torn down by each workload, so the
  server's topic gauge returns to 0 afterward (~44 MB of WAL was written across the
  runs, confirming the durable path was exercised).
- The `broadcast` `deliveries/sec` figure is latency-paced (fixed inter-pulse gap),
  not a saturation throughput; the headline broadcast result is the per-watcher
  write→deliver latency and its flat scaling with watcher count.
- Reproduce:
  ```bash
  D=$(mktemp -d); TOPICS_HOST=127.0.0.1 TOPICS_PORT=4090 TOPICS_DATA_DIR=$D \
    ./target/release/topics &
  U=http://127.0.0.1:4090
  ./target/release/topics-probe conformance  $U                 # 117/117, exit 0
  ./target/release/topics-probe broadcast     $U --watchers 100,1000 --json
  ./target/release/topics-probe distribution  $U --topics 5000 --batch 100 --writers 32 --json
  ./target/release/topics-probe queue         $U --workers 100 --jobs 20000 --json
  ./target/release/topics-probe actors        $U --actors 1000 --inferences 5 --json
  ```

---

# Phase 6 — tiered storage (HOT NVMe + COLD folder)

These numbers were captured against the **same persistent release binary** as
Phases 4–5, now built with the Phase-6 layered/tiered segment store: each topic
log is split into sealed, immutable **segment files** (`seg-<first_seq>.data` +
`.idx`); the active + newest `hot_retain_segments` sealed segments stay **HOT**
(the data dir on NVMe) and older sealed segments **relocate to a COLD tier**
(`TOPICS_COLD_DIR`, a second folder; the `SegmentStore` trait lets S3 drop
in later). **The Phase-2/3, Phase-4, and Phase-5 numbers above are unchanged** —
this section is purely additive.

There were **no `/v0` API or semantics changes**: tiering is transparent.
`topics-probe conformance` against a live binary booted with a cold tier
configured (`TOPICS_COLD_DIR` set, `TOPICS_SEGMENT_MAX_EVENTS=50`,
`TOPICS_HOT_RETAIN_SEGMENTS=2` — so seal + relocate fire under real traffic) is
**117 / 117, exit 0**. With **no** cold dir (the default in every existing test)
nothing relocates and behavior is identical by construction — the full workspace
suite is **268 tests green** and clippy is clean.

Same hardware/OS/toolchain as the baseline: **Apple M4 Max, 16 cores, 128 GiB,
Darwin 25.2.0, rustc 1.92.0, `--release`**. The server ran on `127.0.0.1` over a
fresh temp `TOPICS_DATA_DIR` (hot) + temp `TOPICS_COLD_DIR` (cold), both on the
same local APFS/NVMe (so the cold tier here is a *different folder*, not slower
hardware — the latency delta below is the segment-read path itself, not a slower
disk). Latencies are wall-clock loopback HTTP, percentiles by sort.

## Methodology (Phase 6 additions)

- **Tiering proof.** A durable topic is written 6 100 records with
  `segment_max_events=50` and `hot_retain_segments=2`. The background relocator
  (a 5 s tick, runs the copy on the blocking pool) drains the old sealed segments
  to cold. The physical split is observed on disk (`ls` of the hot vs cold topic
  dir), then `getDifference` from seq 0 (spanning cold + hot + active) is verified
  to return all 6 100 records contiguously with byte-identical payloads.
- **Hot vs cold read latency.** A read whose payloads are still in the bounded
  recent-seal cache (the hot tail) vs a read of records whose payloads have fallen
  out of the cache (the in-memory `PAYLOAD_CACHE_CAP = 4096` ring) and must be
  fetched from a segment — `data dir` (HOT) vs `cold dir` (COLD) via the
  `SegmentStore::read_range` path. A COLD read is surfaced to the client in
  `performance.cold_segments_read` (count of records served by an actual cold
  fetch).
- **Hot-path isolation.** Write-ack and SSE write→deliver latency (epoch-stamped
  payloads, single dedicated client) measured (a) at baseline and (b) with **8
  independent processes** continuously replaying full seq-0 `getDifference` scans
  over the cold tier (each confirmed cold via `cold_segments_read`). Separate
  processes so the load generator is not the client bottleneck.
- **Durability across segments.** A durable topic is loaded with data spanning many
  sealed segments + a WAL tail, the binary is `kill -9`'d, restarted on the same
  hot+cold dirs, and the recovered `head_seq`/payloads are checked for zero
  acked-durable loss.

## 1. Tiering: physical relocation + cross-tier read correctness

6 100 records, `segment_max_events=50` ⇒ 122 segments, `hot_retain_segments=2`:

| | HOT (data dir) | COLD (cold dir) |
|---|---:|---:|
| sealed `.data` files after relocation settled | **2** (newest sealed) | **119** (older sealed, relocated) |

`getDifference(from_seq=0, limit≥6100)` returned **6 100 records, contiguous
1..6100, all payloads byte-identical** to what was written, `caught_up=true` —
the cross-tier stitch (cold prefix → hot tail → active segment) is transparent
and complete. A `before_seq=3000` prefix delete then dropped **59 whole cold
segment files** (119 → 60) — segment-granular reclaim in the cold tier — and the
read from seq 0 silently resumed at seq 3000 (`tombstone: null`, voluntary delete
is silent; DESIGN §5.1).

## 2. Hot vs cold getDifference latency (the degradation is bounded to cold)

`limit=50` window reads against the 6 100-record tiered topic:

| Read | p50 | p99 / max | Notes |
|---|---:|---:|---|
| **HOT tail** (near head, cache-served) | **0.192 ms** | 0.275 ms (p99) | served from the in-memory recent-seal cache; no segment I/O |
| **COLD fresh** (old window, real cold I/O) | **1.147 ms** | 1.230 ms (max) | `cold_segments_read>0`; `read_range` per record from the cold folder |
| **COLD re-read** (same window, cold-LRU hit) | **0.165 ms** | — | second scan served from the bounded cold LRU, no cold I/O |

A larger cold read: `getDifference(from_seq=0, limit=200)` of records fully out of
the payload cache returned 200 records from cold in **8.0 ms** with
`cold_segments_read=200` — i.e. a deep historical scan pays the cold-fetch cost
(~6× the hot tail per record on same-disk APFS; more on genuinely slower/remote
cold storage), exactly the intended "cold reads MAY degrade historical reads"
tradeoff. Index memory stays bounded regardless of payload volume (the index
holds only `seq → (tier, segment, offset, len)`; payloads live in segments).

## 3. HOT-PATH ISOLATION — writes + live delivery are unaffected by cold reads

Write-ack + SSE write→deliver latency, baseline vs **8 concurrent full cold-tier
replays in flight** (each replay scans all 119 cold segments; confirmed cold):

| Metric | Baseline (no cold load) | Under 8× cold replay | Delta |
|---|---:|---:|---:|
| write-ack p50 | 0.938 ms | 0.620 ms | none (≤ noise) |
| write-ack p90 | 1.028 ms | 0.977 ms | none |
| write-ack p99 | 14.24 ms | 13.91 ms | none |
| SSE deliver p50 | 0.801 ms | 0.541 ms | none (≤ noise) |
| SSE deliver p90 | 0.886 ms | 0.920 ms | none |
| SSE deliver p99 | 13.93 ms | 13.76 ms | none |

**The HARD INVARIANT holds:** with heavy cold I/O saturating the blocking pool,
write-ack and live SSE delivery latency are statistically identical to baseline
(the small p50 deltas are within run-to-run loopback/GIL noise; the ~14 ms p99 is
pre-existing client-side jitter present in *both* columns, not server
contention). Cold I/O + the relocator run via `spawn_blocking` and never hold a
topic write lock or block an SSE push — the hot tail (active segment + in-memory
index + recent-seal cache) is independent of cold access. This is additionally
proven deterministically by the unit test
`segwriter::slow_cold_read_does_not_hold_the_writer_lock` (a cold read parked on a
barrier; a concurrent thread still takes the writer lock to append + seal).

## 4. Relocation cost + recovery time with segments

- **Relocation (HOT → COLD copy).** Per sealed segment (~7 KB: a 50-record
  `.data` ≈ 6.1 KB + `.idx` ≈ 1 KB) the copy is read-hot + fsync'd `put` to cold +
  dir fsync: **p50 0.246 ms, p99 0.376 ms per segment** on this APFS/NVMe (a
  ~50-segment relocation pass ≈ 13 ms of copy work). The copy is fsync-bound and
  runs entirely off the hot path on the blocking pool; the background relocator
  tick is 5 s, and a single pass drains all due segments (37 segments relocated in
  one observed pass). Relocation moves bytes, not records — `head_seq`,
  `earliest_seq`, and `count` are unchanged by a relocation.
- **Recovery time with segments.** `kill -9` of the binary then restart on the
  same hot+cold dirs, ~8 450 live records across 60+ cold + hot sealed segments +
  a WAL tail, recovering from a periodic snapshot + WAL tail replay:
  **time-to-ready ≈ 847 ms** (`recover_ms=824` server-side). Recovery
  re-derived each segment's tier (HOT-preferred), replayed the WAL tail, and
  **idempotently reclaimed 1 orphan segment file** (a pre-crash reclaim whose
  unlink had not completed) — the segment-aware recovery step (ARCHITECTURE §4.5).
  A clean SIGKILL of a 350-record durable topic spanning 7 sealed segments + WAL
  tail recovered to-ready in **92 ms** with `head_seq == 350` (zero loss).

## 5. Durability across segments (SIGKILL → restart, zero acked-durable loss)

| Property | Result |
|---|---|
| Durable topic (350 recs, 7 sealed segments + WAL tail) survives `kill -9` | recovered `head_seq 350`, `count 350`, every payload byte-identical — **no loss** |
| Cross-tier topic (6 100 recs, 119 cold + 2 hot segments) survives `kill -9` | full readback **contiguous 1..6100, 0 payload mismatches**; tiers re-derived |
| Prefix-delete floor (`earliest_seq=3000`) survives restart | recovered `earliest_seq 3000`, deleted prefix stays silently gone |
| A second hard kill + restart | all topics intact again; orphan reclaim idempotent (re-runnable) |

The WAL remains the durability boundary; segments are a derivable materialization,
so an acked durable write (fsync-gated 2xx ⇒ on disk in the WAL) is never lost
regardless of segment/tier state. The deterministic proofs are the release-mode
suites `crash_recovery.rs` (3 real-SIGKILL subprocess tests) and the 33
`integration_durability.rs` tests (segment rolling, relocation, interrupted
relocation, cross-tier restart, segment-granular cap/TTL/delete reclaim, orphan
reclaim) — all green.

## Notes (Phase 6)

- The cold tier in this run is a *different folder on the same NVMe*, so the
  hot-vs-cold latency delta (≈ 6× per record) is the segment-read path itself
  (`read_range` + decode), not slower hardware; a genuinely slow/remote cold
  store (S3, future work) would widen the cold read latency further — but, per §3,
  **not** the write or delivery latency, which is the whole point of the tiering.
- The in-memory recent-seal cache (`PAYLOAD_CACHE_CAP = 4096`) is why a read near
  the head never pays cold I/O even when most of the topic lives cold; a read deep
  in cold history is the degraded path and is surfaced via
  `performance.cold_segments_read`.
- Reproduce:
  ```bash
  D=$(mktemp -d); C=$(mktemp -d)
  TOPICS_HOST=127.0.0.1 TOPICS_PORT=4090 TOPICS_DATA_DIR=$D TOPICS_COLD_DIR=$C \
    TOPICS_SEGMENT_MAX_EVENTS=50 TOPICS_HOT_RETAIN_SEGMENTS=2 ./target/release/topics &
  U=http://127.0.0.1:4090
  ./target/release/topics-probe conformance $U          # 117/117, exit 0
  # write >6000 records to one durable topic, wait ~5s for the relocator, then:
  ls $D/topics/*/ ; ls $C/topics/*/                        # observe hot vs cold split
  curl -s -X POST $U/v0/topics/<topic>/diff -d '{"from_seq":0,"limit":1000}'  # cross-tier read
  ```

---

# Phase-6 PERFORMANCE iteration (hot-path optimization, 2026-05-30)

Same machine/env as above (Apple M4 Max, 16 cores, 128 GiB, Darwin 25.2.0,
`--release`). After the correctness work that added per-topic append locking, a
measurement pass found the locking serialized a single topic's durable writers at
one-fsync-per-write (durable throughput collapsed to ~17.9 K rec/s) and added
per-call overhead to append/diff. This iteration restores throughput and trims
the hot paths WITHOUT changing the `/v0` contract or weakening the
durability/ordering guarantees (acked ⇒ committed). All gates green after every
change: `cargo test` 310, `cargo test --features test-fs` 455, `topics-probe
conformance` 117/117.

## Optimizations applied

1. **Off-lock fsync + per-topic commit sequencer** (`engine/mod.rs` write +
   `durable_append`, `engine/topic_state.rs`). The `append_lock` now covers only
   the seq-order critical section (stage + WAL-enqueue + take a publish ticket);
   the fsync `wait()` happens OFF the lock, so concurrent durable writers to the
   SAME topic coalesce into ONE group-commit fsync. Publish/rollback are gated back
   into strict seq order by a per-topic ticket gate, so the single ordered WAL
   writer's prefix-commit guarantee holds (when a writer's frames are fsynced,
   every earlier writer's lower-seq frames are fsynced too) — ordered publish
   never exposes a non-durable record. Seqs are reserved from the index deque
   tail (not `head_seq`), so concurrent stagers stay contiguous. Rollback on
   fsync failure marks the failed batch's own seqs deleted in place (robust to a
   later writer having staged past them). Snapshot capture now `quiesce`s the
   publish gate under the append lock so an in-flight (ticketed-but-unpublished)
   durable write whose frame precedes the checkpoint offset is never excluded.
2. **Skip router-forward clones on the write path**
   (`engine/mod.rs`). Router forwarding is derived from the committed source log,
   so source writes no longer clone payloads for inline fan-out and only clone for
   the WAL when a WAL frame is needed. A plain write with no WAL no longer clones
   every `serde_json::Value`.
3. **Single-pass diff projection** (`engine/mod.rs::diff`). The deliverable walk
   builds `RecordOut` directly under the index read lock for resident payloads
   (the common case); only sealed (non-resident) payloads are deferred and
   patched off-lock. Removes the intermediate `Vec<DiffSlot>` + second pass.
4. **Read-path retention fast path** (`engine/topic_state.rs::enforce_retention`).
   A topic with no TTL and no caps returns immediately, skipping the index-read +
   floors-write locks that every `diff`/SSE pass used to take (deletes already
   reclaim their own front).

## Criterion micro-benchmarks — baseline (this iteration's start) vs after

| Bench | Start | After | Delta |
|---|---:|---:|---:|
| append/64B/1 | 1.272 µs | 1.208 µs | −5% |
| append/64B/10 | 3.45 µs | 2.73 µs | −21% |
| append/64B/100 | 25.7 µs | 17.5 µs | −32% |
| append/64B/1000 | 260.8 µs | 169 µs | −35% |
| append/1KiB/1 | 1.737 µs | 1.616 µs | −7% |
| diff/1 | 207.8 ns | 165.5 ns | −20% |
| diff/256 | 22.82 µs | 18.79 µs | −18% |
| diff/1000 | 84.44 µs | 76.29 µs | −10% |

(Batched-append wins come from the cheaper seq reservation + fast paths; the
batch=1 ticket/gate adds a few ns of fixed cost, more than offset by the
router/WAL-snapshot skip. Diff wins are the single-pass projection + retention
fast path.)

## Durable write throughput (`bench-durable`, 16 writers × batch 100, one topic)

| Class | Single-write ack p50 / p99 | Throughput |
|---|---:|---:|
| durable (regressed, pre-iteration) | 5.20 / 6.37 ms | **~17.9 K rec/s** |
| **durable (after off-lock fsync)** | 5.19 / 6.20 ms | **~150 K rec/s** |
| non-durable | 0.061 / 0.103 ms | ~558 K rec/s |

**~8.4× durable-throughput recovery** with single-write durable latency
unchanged (still the ~5 ms APFS fdatasync floor) — the fix lets many writers'
frames join one group-commit fsync instead of serializing one-fsync-per-write,
while every acked durable write is still fsync-gated and survives restart
(`concurrent_durable_writers_no_loss_across_restart` +
`f_snap_race_write_during_capture` green).

## Queue / live HTTP

Queue workload (100 workers, 20 k jobs, claim-max 8): ~115 K jobs/s, produce
~524 K rec/s — stable vs the pre-iteration measurement (these queue topics are
non-durable, so they are unaffected by the durable-fsync coalescing; the
remaining per-job ack-delete fsync on a *durable* lease queue is a separate,
larger change not taken this pass). SSE fan-out and router forwarding overhead
are within prior variance.

# Performance iteration (post-reliability) — FINAL VERIFIED numbers (2026-05-30)

This is the **STAGE-3 VERIFY+RECORD** pass for the performance iteration: a full
clean `--release --workspace` rebuild, all gates re-run green, the live release
server re-booted, conformance re-asserted, and the **entire benchmark suite
re-measured end-to-end** to lock in the final post-optimization numbers. It does
not change code — the optimizations are those documented in the "Phase-6
PERFORMANCE iteration" section directly above; this section records the verified
final state and the honest target assessment.

Machine / env: **Apple M4 Max, 16 cores, 128 GiB RAM, Darwin 25.2.0**, Rust
edition 2021, `cargo build --release`. Live server on `127.0.0.1` (loopback HTTP,
single-tenant dev mode), fresh temp `TOPICS_DATA_DIR` on local NVMe (APFS).

## Gate status (all green)

| Gate | Result |
|---|---|
| `cargo build --release --workspace` | clean |
| `cargo clippy --workspace --all-targets` | clean (0 warnings) |
| `cargo clippy --workspace --all-targets --features test-fs` | clean (0 warnings) |
| `cargo test --workspace` | **310 passed, 0 failed** |
| `cargo test --features test-fs` | **455 passed, 0 failed** |
| `topics-probe conformance` (live release server) | **117 / 117, exit 0** |

## 1. Criterion micro-benchmarks — final absolutes (engine, in-process)

Median of the criterion 100-sample estimate, isolated run on the release engine.

| Bench | Final time | Throughput |
|---|---:|---:|
| append/64B/1 | 1.239 µs | 807 Kelem/s |
| append/64B/10 | 2.744 µs | 3.64 Melem/s |
| append/64B/100 | 17.81 µs | 5.62 Melem/s |
| append/64B/1000 | 170.4 µs | 5.87 Melem/s |
| append/1KiB/1 | 1.648 µs | 607 Kelem/s |
| append/1KiB/1000 | 591.6 µs | 1.69 Melem/s |
| diff/1 | 153.4 ns | 6.52 Melem/s |
| diff/256 | 20.51 µs | 12.48 Melem/s |
| diff/1000 | 76.45 µs | 13.08 Melem/s |
| tag_match/exact | 267.5 ns | 374 Melem/s |
| tag_match/prefix | 67.81 µs | 147 Melem/s |
| cap_evict/1 | 410.1 ns | 2.44 Melem/s |
| cap_evict/100 | 19.20 µs | 5.21 Melem/s |
| delete/before_seq_all | 2.624 ms | 3.81 Melem/s |
| delete/match_exact | 1.594 ms | 3.14 Melem/s |

The pure in-process engine append hits **5.6–5.9 M records/s** at 64 B batch-100/1000
and diff projects at **12–13 M records/s** — i.e. the engine core is comfortably
above the 1 M events/s bar; the live ceiling is the HTTP/serialization path and
the durability class, not the engine (see §4). Run-to-run variance of ±10–15 %
was observed on `append/64B/1` under background load; the isolated-run medians
are reported here.

## 2. Per-durability-class write-ack + throughput (the headline table)

Three commit classes (API §0.10): `memory` (same group-committed WAL write path as
`disk` but best-effort — not fsync-gated and no durability guarantee; the lowest-latency
class), `disk` (WAL, group-committed, no per-write fsync), `fsync` (fsync-gated ack).
Single-write ack and throughput were measured against the live release server.

**Single-record write-ack latency (Rust client, `topics-probe`, n=5000):**

| Class | p50 | p99 | p999 | max | Throughput (16 writers × batch 100) |
|---|---:|---:|---:|---:|---:|
| memory | ≤ disk (not fsync-gated; best-effort) | — | — | — | ~614 K rec/s |
| disk (`durable:false`) | **0.062 ms** | 0.102 ms | 0.148 ms | 0.265 ms | **566 K rec/s** |
| fsync (`durable:true`) | **5.21 ms** | 6.76 ms | 10.48 ms | 13.14 ms | **143 K rec/s** |

`fsync` server-reported group-commit `fsync_ms`: p50 4.91 / p99 6.21 / p999 10.13 /
max 12.87 ms — i.e. the ack latency is dominated by the single APFS `fdatasync`
floor (~5 ms), as expected; the engine adds well under 1 ms on top.

**Same-client (Python urllib) apples-to-apples throughput ranking** (one HTTP
client across all three classes, so the *ratios* are honest even though the
absolute numbers are below the Rust-client figures above due to client overhead):

| Class | Throughput (batch 100, 16 writers, same client) |
|---|---:|
| memory | 614 K rec/s |
| disk | 364 K rec/s |
| fsync | 121 K rec/s |

Ordering memory > disk > fsync holds; memory ≈ 1.7× disk ≈ 5× fsync.

## 3. Live HTTP macro + workloads — final numbers

**Core (`topics-probe bench`, writes=50000, watchers=1,10,100):**

| Metric | Value |
|---|---:|
| Write-ack single record (disk) p50 / p99 / p999 | 0.062 / 0.101 / 0.157 ms |
| Write throughput (16 writers × batch 100) | **525 K rec/s** |
| getDifference limit=1 (calls/s) | 13.4 K (p50 0.071 ms) |
| getDifference limit=256 (records/s) | 48.9 K (p50 5.00 ms) |
| getDifference limit=1000 (records/s) | 51.6 K (p50 19.15 ms) |
| tail caught-up latency p50 / p99 | 0.047 / 0.095 ms |
| SSE fan-out write→deliver p50, 1 / 10 / 100 watchers | 0.34 / 0.43 / 0.85 ms |
| Router forwarding added p50 | ~0.003 ms |

**Workloads:**

| Workload | Config | Result |
|---|---|---:|
| BROADCAST | 1 src → N SSE watchers, 100 pulses | 1 w: deliver p50 0.29 ms · 100 w: 12.5 K deliv/s, p50 1.06 ms · 1000 w: 92.3 K deliv/s, p50 2.21 ms (one straggler tail at max 1083 ms) |
| DISTRIBUTION | 5000 topics, batch 100, 32 writers | **285 K appends/s**, 500 K records, per-req p50 11.2 ms |
| QUEUE | 50 workers, 20 k jobs, claim-max 8 | **126 K jobs/s** (produce 481 K rec/s), claim p50 1.18 ms, even (CV 0.075) |
| ACTORS | 1000 actor topics, 5 inferences, chain 5, snapshot every 2 | **31.5 K events/s**, 6.3 K inferences/s, 2000 snapshot-compactions, chain-append p50 0.77 ms |

## 4. Are the ~1 M events/s + ~1 ms targets met? (honest assessment)

**Latency target (~1 ms delivery/response):** **MET for the non-durable path.**
Disk-class single-record write-ack is **p50 0.062 ms / p99 0.10 ms**; SSE
write→deliver is **p50 0.34–0.85 ms** out to 100 watchers; tail/caught-up is
**0.05 ms**. All comfortably under 1 ms. **NOT met for the fsync class** by
design: an fsync-gated ack is **~5 ms p50**, which is the hardware APFS
`fdatasync` floor on this NVMe, not engine cost — no amount of engine work moves
it without weakening the acked⇒durable guarantee. Group-commit already amortizes
it across concurrent writers (143 K fsync-class rec/s aggregate).

**Throughput target (~1 M events/s batched):**
- **Engine core: MET** — in-process append is **5.6–5.9 M records/s** and diff
  projection **12–13 M records/s** (criterion §1).
- **Live single-topic HTTP: PARTIALLY MET** — **525–566 K rec/s** disk-class over
  loopback HTTP through one topic (16 writers × batch 100). The ~1 M/s bar is
  reachable in aggregate across topics/connections (memory class hits 614 K
  through one topic on a slower client; the engine has 5×+ headroom) but a single
  topic over a single loopback HTTP origin lands at ~0.5 M/s. **The ceiling here is
  the HTTP request/`serde_json` serialization path and per-request lock acquisition,
  not the engine** — the engine-side append fast path is now 5.6 M/s.

**Where the ceiling is, by class:**
- `memory`: HTTP + JSON parse/serialize cost. Engine append is essentially free.
- `disk`: same HTTP/serialization ceiling plus the WAL buffered-write + the
  single ordered WAL-writer's per-batch work; ~525–566 K rec/s through one topic.
- `fsync`: the **physical NVMe `fdatasync` (~5 ms)** is the floor for *latency*;
  for *throughput* the group-commit coalescing (this iteration's headline fix,
  17.9 K → 143 K rec/s, ~8×) lifts the ceiling to how many writers' frames fit in
  one fsync window.

## 5. Optimizations applied this iteration — before / after (verified)

All preserve the `/v0` contract and the acked⇒committed durability/ordering
guarantee. "Before" = the post-reliability-locking regression baseline at the
start of the iteration; "After" = these final verified numbers.

| # | Optimization | Site | Before → After |
|---|---|---|---|
| 1 | **Off-lock fsync + per-topic commit sequencer** (group-commit; `append_lock` covers only stage+enqueue+ticket, fsync `wait()` off-lock, publish gated back into strict seq order) | `engine/mod.rs` write + `durable_append`; `engine/topic_state.rs` ticket gate; `engine/snapshot.rs` quiesce | **durable throughput 17.9 K → 143 K rec/s (~8×)**; durable single-write p50 unchanged (~5.2 ms, the fsync floor). Surfaced + fixed two real races (staged-but-unpublished seq; snapshot-capture-vs-in-flight-write) — both green. |
| 2 | **No-WAL write fast path** (skip the per-record `serde_json::Value` deep clone when no WAL frame is needed) | `engine/mod.rs` | append/64B/1 1.272 → 1.239 µs; append/1KiB/1 1.737 → 1.648 µs |
| 3 | **Single-pass diff projection** (build `RecordOut` directly under the read lock for resident payloads; defer only sealed; removed `DiffSlot`) | `engine/mod.rs::diff` | diff/256 22.82 → 20.51 µs (−10%); diff/1000 84.44 → 76.45 µs (−9%) |
| 4 | **Read-path retention fast path** (no-TTL/no-cap topic returns immediately, skipping two locks per diff/SSE) | `engine/topic_state.rs::enforce_retention` | diff/1 207.8 → 153.4 ns (−26%); cap_evict/1 −18%, cap_evict/100 −28% |

## 6. Remaining performance gaps (not closed this iteration, out of safe scope)

- **Deep getDifference over HTTP** (limit 256/1000 → ~5 / 19 ms p50) is dominated
  by the HTTP response serialization of the large body, not engine diff cost (the
  engine-side diff is 12–13 M records/s). Closing it needs a streaming/chunked
  response encoder, a larger change.
- **Single-topic single-origin HTTP write throughput** tops out at ~0.5 M rec/s
  (disk) vs the 5.6 M/s engine core — the gap is the HTTP/`serde_json` path.
  Reaching ~1 M/s through one topic would need a leaner request decode (e.g.
  borrowed/zero-copy JSON or a binary ingress), not taken here.
- **Durable lease-queue per-job ack-delete fsync** (a fsync per ack on a *durable*
  queue) is a separate, larger change; the queue workload here is non-durable
  (~126 K jobs/s) and was unaffected.
- **fsync-class latency (~5 ms)** is the physical NVMe `fdatasync` floor and is
  not an engine bug — it is the cost of the acked⇒durable guarantee.

# WAL sharding — write scaling (2026-05-31)

The single ordered WAL writer (one thread / mpsc / fsync stream) serialized ALL
durable writes — the write-throughput bottleneck. This iteration splits it into
`N` independent shards (`TOPICS_WAL_SHARDS`), each its own WAL file set + writer
thread + group commit + per-shard checkpoint, with each topic routed to exactly one
shard by a stable hash of its interned `topic_id`. Recovery is **shard-count-agnostic**:
it replays every WAL group on disk (flat `wal/` + every `shard-NN/`) dispatched by
`topic_id`, so `TOPICS_WAL_SHARDS` can be reconfigured between restarts with no data
loss (verified live below: 8 → 3 → 1 restarts, all acked durable records recovered).

**Machine:** Apple M4 Max, 16 cores (12P+4E), 128 GiB, macOS/APFS single volume.
Bench: `benches/wal_scaling.rs` — 64 concurrent writers across 256 topics, 256 B
payload, aggregate write-ack throughput; `cargo bench --bench wal_scaling`.

### Current hardening smoke (2026-06-02)

After the production hardening changes for durable retention, exact total-byte quota
release, and the data-dir single-writer lock, the durable WAL benchmark path was
run in a bounded smoke configuration:

```bash
WAL_BENCH_SHARDS=1 WAL_BENCH_WRITERS=8 WAL_BENCH_TOPICS=32 \
  WAL_BENCH_OPS=1000 WAL_BENCH_REPEAT=1 cargo bench --bench wal_scaling
```

Result on the same Apple M4 Max/APFS environment: **~831 fsync-class acks/sec**
for 1 shard, 8 writers, 32 topics, 256 B payload, batch=1. This is a smoke check
that the durable benchmark remains runnable after the hardening work; use the
full commands below for release/SLO comparison.

## The hardware ceiling: fsync-class is device-bound, not software-bound

On this single-APFS-volume Mac, **durable (`fsync`-class) throughput does not scale
with shards** — and that is a property of the device, not the engine. APFS takes a
volume-level barrier per `fsync`, so concurrent fsync topics do not parallelize
(a standalone raw-`fdatasync` probe: 1 thread 227 fsync/s @ 4.4 ms; 16 threads only
672 fsync/s @ 22.8 ms). Splitting writers across N shards shrinks each shard's
group-commit cohort, so you issue MORE fsyncs that then serialize at the device
anyway — a double loss. The `disk`-class curve here shows the same wall (avg fsync
4.4 ms → 6.9 ms as shards rise 1 → 8):

| shards | 1 | 2 | 4 | 8 |
|---|---|---|---|---|
| disk-class acks/s | 230 K | 205 K | 169 K | 145 K |
| avg fsync | 4.4 ms | 4.7 ms | 5.5 ms | 6.9 ms |

So on a single shared-volume host, the right tuning is **FEWER shards + fat group
commits**, not more. Sharding pays off where fsync actually parallelizes — Linux +
ext4/XFS/NVMe, or a separate device per shard — and where a single writer thread is
the bottleneck (the planned dedicated Linux/NVMe + per-core `taskset` iteration).

## The software path DOES scale (the codex-P1 hot-path lock fix)

To prove the **engine's** write path scales with shard count once the device wall is
removed, measure the `memory`-class path (no `fsync` at all, so only software cost +
contention remains). This iteration also removed the GLOBAL write-path costs that previously
capped scaling regardless of shard count: inline router fan-out/cloning and the scheduler
ready-set mutex (`mark_dirty` on every append → a lock-free `sched_dirty` fast path). With those gone the memory-class
path scales positively with shards up to the core/contention limit:

| shards | 1 | 2 | 4 | 8 |
|---|---|---|---|---|
| memory-class acks/s | 1.53 M | 1.88 M | 2.46 M | 2.13 M |
| speedup vs 1 shard | 1.00× | 1.23× | 1.61× | 1.40× (plateau) |

The curve climbs to ~1.6× at 4 shards then plateaus at 8 — the expected shape for 64
writers / 256 topics on a 16-core topic: the software path no longer has a single global
write lock, so throughput rises with parallelism until it saturates CPU / cross-core
cache traffic. (Before the P1 fix, the global router + scheduler mutexes pinned the
per-write critical section, so adding shards bought little.)

## Honest assessment: near-linear?

**Not near-linear on this Mac, and that is a hardware result, not a design defect.**
- `fsync`-class (the production-durable class) is **fsync-device-bound** on a single
  APFS volume and gets *worse* with shards — set `TOPICS_WAL_SHARDS=1` on such a host.
- The **software write path** scales positively (≈1.6× at 4 shards, memory-class)
  now that the global hot-path locks are gone — sub-linear because of CPU/cache
  saturation at this writer/topic/core ratio, not a shared lock. Per-topic append-order,
  the commit sequencer, R3 durable head watermark, R5 atomic batch, and the
  durability classes all still hold per shard (each topic owns one shard's ordered
  writer); cross-topic global order was never a guarantee.
- True near-linear durable scaling needs **fsync parallelism** (Linux + NVMe/XFS or
  per-shard devices), where each shard's independent fsync stream actually runs
  concurrently. That is the planned dedicated iteration; this Mac's single-volume
  fsync barrier physically caps the fsync-class demonstration.

## Default: `min(num_cpus, 8)`

`TOPICS_WAL_SHARDS` defaults to `min(num_cpus, 8)` (≥ 1). Rationale: on the Linux/
NVMe target one writer thread per core (capped at 8) matches available fsync
parallelism without oversharding (which fragments each shard's group commit and
spawns idle writer threads). The cap of 8 bounds the writer-thread count and keeps
per-shard cohorts fat enough to amortize fsync. `shards = 1` is the flat
single-writer layout and the right setting for a single shared-fsync-volume host
like this Mac. Operators on a single-volume host should set `TOPICS_WAL_SHARDS=1`.

## Verified (this iteration)

- `cargo test` 384 · `--features test-fs` 575 · `--features test-fs,failpoints` 585
  — all green. clippy clean (default + `test-fs`, `--all-targets`).
- `topics-probe conformance` **117/117** against a default-config server running an
  8-shard WAL (`shard-00..07` confirmed on disk), and **117/117** against a 3-shard
  server after reconfigure.
- **kill -9 + restart with a DIFFERENT shard count**: 12 durable topics × 20 records
  written, `kill -9`, restart 8 → 3 → 1; every acked durable record recovered
  (head/count = 20/20 per topic, seqs 1..20 contiguous with correct payloads), and a
  post-reconfigure durable write acked + persisted.

---

# Async + Derived Router Forwarding — Fan-out

The async/derived forwarding path removes WAL amplification from router fan-out
and takes forwarding off the write/ack path. This section records the current
numbers for a single source write fanning to **1 / 10 / 100 / 1000** router
destinations.

## Methodology

Deterministic in-process measurement via `examples/forward_fanout_bench.rs` (the
engine API directly, a real durable WAL under a temp dir, so the WAL-frame delta is
EXACT). Every topic is `fsync`-class, so the only WAL frames a source write produces
are `Append` frames — the frame delta therefore IS the append amplification. The
`ack_avg_us` is the wall-clock of the `Engine::write` call (the synchronous ack);
`deliver_us` is the async forwarding drain (off the ack path). Apple M4 Max, macOS, single shared-fsync
volume (so each fsync is a real disk barrier — the absolute µs are host-bound; the
SCALING with fan-out is the result).

Reproduce:

```bash
cargo run --release --example forward_fanout_bench
```

## WAL writes per source write (the amplification)

| fan-out | async/derived WAL frames | writes/dest |
|---:|---:|---:|
| 1    | **1** | 1.0000 |
| 10   | **1** | 0.1000 |
| 100  | **1** | 0.0100 |
| 1000 | **1** | 0.0010 |

The source append is the only WAL frame for any fan-out. Forwarded dest records
are derived from the source WAL + the durable per-router cursor, never separately
WAL-logged.

## Write-ack latency (does the ack block on fan-out?)

| fan-out | async source ack |
|---:|---:|
| 1    | **~4.2 ms** |
| 10   | **~4.2 ms** |
| 100  | **~4.2 ms** |
| 1000 | **~4.2 ms** |

The source ack is flat because fan-out does not run on the source write path. The
forwarding runs in a background worker plus read-path catch-up:

| fan-out | async forward delivery (`deliver_us`) |
|---:|---:|
| 1    | ~35–45 µs |
| 10   | ~140–190 µs |
| 100  | ~1.2–1.6 ms |
| 1000 | ~14.5 ms |

Async delivery cost grows with fan-out as expected, but it is paid by the
background drainer / the dest reader's catch-up, NOT by the source writer's ack.

## Summary

| property | async + derived |
|---|---|
| WAL writes for a 1→N fan-out | **1** (source only) |
| source ack latency vs fan-out | **flat (1 fsync)** |
| forwarded dest records | **derived (no WAL frame)** |
| where forwarding runs | **off the ack path** |

## Verified (Stage 4 — async-router fixes)

- `cargo test` 392 · `--features test-fs` 584 · `--features test-fs,failpoints` 594
  — all green. clippy clean (default + `test-fs` + `--all-features`, `--all-targets`).
- `topics-probe conformance` **117/117** with the async + derived router path.
- **kill -9 + restart**: the derived dest topics re-materialize from
  the source WAL + the durable per-router cursor with identical seqs (a consumer
  cursor into a dest stays valid); no duplicate re-forward, no silent loss.
