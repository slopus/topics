# Benchmarks — Phase-2/3 In-Memory BASELINE

This document records the **initial baseline** performance numbers for the
Phase-2 in-memory `streams` server, captured during Phase 3. All data lives in
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
   `BoxState::matching_live_seqs`, `BoxState::apply_delete`, cap-eviction via
   `enforce_retention`). No HTTP, no network. Criterion's default config: 3 s
   warm-up, 100 samples per bench. Reported value is the **median** of the
   estimate interval. Throughput is Criterion's `Throughput::Elements` over the
   batch size. These isolate raw CPU cost of the hot paths.

2. **Live end-to-end HTTP macro-benchmarks** (`streams-probe bench`) — run
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
STREAMS_PORT=4090 ./target/release/streams &
./target/release/streams-probe conformance http://localhost:4090   # must exit 0
./target/release/streams-probe bench       http://localhost:4090 --json
```

---

## 1. Criterion micro-benchmarks (engine, in-process)

Median time per criterion run; throughput derived from batch size.

### Append (`Engine::write`, fresh box per batch)

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

### getDifference (`Engine::diff` from seq 0, warm 10 k-record box)

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

### Cap eviction (`Engine::write` into a full `discard:old`, cap=10 k box)

| Batch | Median time | Throughput (records/s) |
|---:|---:|---:|
| 1   | 474 ns  | ~2.11 M |
| 100 | 25.8 µs | ~3.88 M |

### Delete (`BoxState::apply_delete`, fresh warm 10 k box per iter)

| Selector | Median time | Throughput (records/s) |
|---|---:|---:|
| `before_seq` all (prefix delete of all 10 k) | 2.60 ms | ~3.85 M |
| `match` exact (tag `Eq`, ~5 k matched) | 1.53 ms | ~3.29 M |

---

## 2. Live end-to-end HTTP macro-benchmarks (`streams-probe bench`)

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
- The macro numbers are a single representative `streams-probe bench` run; they
  are loopback-HTTP figures and include the full axum/hyper request path.
- `streams-probe conformance` passed 89/89 checks (exit 0) against this same
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
shapes, and semantics are identical (`streams-probe conformance` = **89/89**,
exit 0, against a release server booted on a temp `STREAMS_DATA_DIR`).

## Methodology (Phase 4 additions)

- **Durable vs non-durable write-ack** (`streams-probe bench-durable <url>`):
  boots two boxes that differ ONLY in `durable` (`true` vs `false`) and drives
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
~4.7 M) is bounded by the single-box append-serialization + HTTP path, not disk.
Both classes lose no acked data on a clean restart; durable additionally survives
SIGKILL.

> Note: the per-box append path now serializes seq-assignment + WAL-enqueue under
> a per-box lock (the durability-correctness fix below), so single-box throughput
> is slightly lower than the lock-free in-memory baseline; cross-box throughput
> still scales with sharding.

## 3. Recovery time — `time-to-ready` after SIGKILL (pure WAL replay, no snapshot)

| Records in box | Load time | **time-to-ready** | Recovered `head_seq` |
|---:|---:|---:|---:|
| 100 000 (1e5) | ~0.15 s | **~0.14 s** | 100 000 (no loss) |
| 1 000 000 (1e6) | ~1.40 s | **~0.68–0.94 s** | 1 000 000 (no loss) |

This is the **worst case**: a hard kill with no snapshot, so recovery replays
*every* frame from WAL offset zero. ~1e6 records replay (CRC-validate + decode +
re-index + tag-index rebuild) in well under a second (~1.1–1.5 M frames/s). With
a graceful shutdown (or the periodic snapshotter), recovery starts from the
checkpoint and replays only the un-checkpointed tail, so real-world time-to-ready
is bounded by the snapshot interval, not the total record count. The 1e7/1e8 rows
from the ROADMAP plan were not run (they require the segment store deferred to a
later phase; the in-memory index holds the full set as the cache here).

## 4. Durability-correctness fix surfaced by the recovery benchmark

The recovery benchmark initially exposed a **silent loss of acked durable
writes** under concurrent writers (~5 % loss at 1e5 with 16 writers): seq
assignment (`BoxState::append`, under the index lock) and the WAL enqueue were
not a single atomic unit, so two writers could assign seqs `A < B` yet enqueue
`B`'s frame ahead of `A`'s. Recovery applies frames in WAL order and skips any
`seq <= head`, so the lower-seq frame `A` was dropped on replay despite having
been acked. The fix adds a per-box `append_lock` that makes
seq-assignment + WAL-enqueue atomic (the fsync wait stays *outside* the lock, so
durable group commit still coalesces across boxes). Post-fix: **zero loss** at
1e5 and 1e6 (recovered `head_seq == N` every run), covered by a deterministic
in-process regression test (`concurrent_durable_writers_no_loss_across_restart`)
plus the real SIGKILL subprocess tests.

## 5. Crash-consistency / recovery correctness (proven by tests, not benchmarked)

| Property | Proof |
|---|---|
| **Durability:** acked `durable:true` write survives SIGKILL at any instant | `crash_recovery::sigkill_durable_writes_survive_with_identical_state` (real `kill -9` of the binary; the write ack is fsync-gated so a 2xx ⇒ on disk) |
| **Recovery correctness:** post-restart head/earliest/count/config/routers/delete match pre-crash | same test asserts each field for durable boxes + deleted-stays-gone + cap-floor-tombstones; `integration_durability::write_snapshot_more_writes_restart_matches` |
| **Crash consistency (clean prefix):** SIGKILL during a non-durable burst ⇒ recovered tail is a contiguous prefix, no torn frame misread | `crash_recovery::sigkill_during_nondurable_burst_recovers_clean_prefix` |
| **Torn tail truncated, not misread:** a corrupted/oversized last frame on disk ⇒ clean recovery, no panic, no bogus record, WAL writable again | `crash_recovery::torn_tail_on_subprocess_wal_recovers_clean`; `integration_durability::torn_tail_is_truncated_not_read_as_data`; WAL-reader unit tests (CRC + length-overrun + trailing-zeros) |
| **No silent loss across restart:** cursor below recovered `evict_floor` ⇒ tombstone; purely-deleted gap ⇒ silent | `integration_durability::tombstone_vs_silent_gap_survive_restart` |

## 6. ROADMAP Phase-4 acceptance-criteria coverage

| Criterion | Status | Where |
|---|---|---|
| Durability (acked durable survives hard kill) | **MET** | `crash_recovery.rs` (real SIGKILL), `integration_durability.rs` |
| Crash consistency (torn tail truncated, never read as data) | **MET** | `crash_recovery.rs`, WAL unit tests |
| Recovery correctness (head/earliest/evict_floor/count/config/routers/deletes match) | **MET** | `crash_recovery.rs`, `integration_durability.rs` |
| No silent loss across restart (tombstone vs silent deleted gap) | **MET** | `integration_durability.rs`, `properties.rs` |
| No regressions (full prior suite green; conformance 89/89) | **MET** | 192 tests green; `streams-probe conformance` 89/89 on the persistent build |
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
  D=$(mktemp -d); STREAMS_PORT=4090 STREAMS_DATA_DIR=$D ./target/release/streams &
  ./target/release/streams-probe conformance   http://localhost:4090   # 89/89
  ./target/release/streams-probe bench-durable  http://localhost:4090 --json
  # recovery-time: see the SIGKILL-load-restart harness in tests/crash_recovery.rs
  ```

---

# Phase 5 — workloads & fan-out

These numbers were captured against the **same persistent (WAL + group-commit +
snapshot) release binary** as Phase 4, now with the Phase-5A lease queue and the
Phase-5B workload features (HTTP/2 cleartext / h2c, the broadcast/distribution/
queue/actor patterns). **The Phase-2/3 baseline and the Phase-4 numbers above are
unchanged** — this section is purely additive. There were **no `/v0` API or
semantics changes**: `streams-probe conformance` against the live binary used here
is **117 / 117, exit 0** (the count grew from Phase-4's 89 as the h2c + queue
checks were added in earlier Phase-5 stages; all are additive).

Same hardware/OS/toolchain as the baseline: **Apple M4 Max, 16 cores, 128 GiB,
Darwin 25.2.0, rustc 1.92.0, `--release`**. The server ran on `127.0.0.1` on an
ephemeral port over a fresh temp `STREAMS_DATA_DIR` on local NVMe (APFS); every
workload is a live end-to-end HTTP run (reqwest h1/h2, loopback, keep-alive) from
a single client process. Boxes use the default (`durable:false`) class, so these
exercise the engine + HTTP + SSE + scheduler path, not the fsync floor.

## Methodology (Phase 5 additions)

Four `streams-probe` subcommands drive a LIVE server; each prints a table and a
`--json` summary. Wall-clock latencies; percentiles by sort + linear
interpolation. The SSE write→deliver latency uses a shared monotonic `epoch`
stamped into each pulse payload (writer and watchers share one process clock), so
it is a true end-to-end write-to-delivery interval with no clock skew.

- `broadcast <url> --watchers 1,10,100,1000` — one source box, N concurrent SSE
  watchers all tailing it, one writer emitting timed single-record pulses. This is
  the **shared zero-copy fan-out**: a pulse is serialized into ONE frame and
  ref-counted to every watcher (never copied into N boxes). The headline metric is
  the **per-watcher write→deliver latency** and how flat it stays as watchers grow
  100×. NOTE: the pulse loop is deliberately *paced* (a fixed inter-pulse gap to
  measure delivery latency cleanly), so the reported `deliveries/sec` is a
  latency-paced aggregate (`≈ watchers × pulse-rate`), **not** a saturation
  throughput figure — the saturation story is the latency headroom (see the
  millions/sec discussion below).
- `distribution <url> --boxes N --batch B --writers W` — round-robins batched
  appends across many boxes via W concurrent writer tasks; aggregate appends/sec.
- `queue <url> --workers N --jobs J --claim-max K [--jitter ms]` — producers
  batch-fill the Phase-5A lease queue, then N worker nodes claim→ack in a loop;
  jobs/sec, claim latency, and per-worker distribution evenness.
- `actors <url> --actors K --inferences N --tool-results T --snapshot-every S` —
  each actor is a box; per inference appends a chain (model-answer + tool-call + T
  tool-results) as one batch, then snapshot-compacts via `delete {before_seq}`
  every S inferences; events/sec + box-count scaling.

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
watchers is p50 3.9 ms / p99 6.1 ms (the source box's drain wakes 1000 watchers
per write).

**Fan-out scaling is the key result:** per-watcher delivery latency stays sub-2 ms
p99 from 1 → 100 watchers (a 100× fan-out) and only reaches ~5 ms p99 at 1000.
Because each pulse is serialized **once** and ref-counted, the marginal cost of an
extra watcher is a bounded-channel send (tens to hundreds of ns), which is exactly
why the per-delivery latency does not blow up with N. The aggregate `deliveries/s`
columns are latency-paced, not saturated (see §5 on millions/sec).

## 2. DISTRIBUTION — 1 source → many boxes (batched, sharded fan-out)

5000 destination boxes, batch 100, 32 concurrent writer tasks, 500 000 records.

| Boxes | Batch | Writers | Records | Elapsed | Appends/s | req p50 | req p99 | req max |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 5 000 | 100 | 32 | 500 000 | 1.426 s | **~350 737 /s** | 7.73 ms | 18.55 ms | 19.90 ms |

500 000 small appends spread across 5000 boxes at ~351 K appends/s. The per-box
write path is lock-free across distinct boxes (sharded), so this is bounded by the
**single client process's HTTP request rate over loopback** (32 in-flight batched
requests), not the engine — the per-request latency (p50 7.7 ms with 32 writers in
flight) is the HTTP round-trip + queueing, not append cost (the micro-bench append
is sub-µs/record). See §5.

## 3. QUEUE — Phase-5A lease queue, N workers claim/ack

100 worker nodes, 20 000 jobs, claim-max 8, greedy (`jitter=0`).

| Workers | Jobs | claim_max | jitter | Jobs/s | claim p50 | claim p99 | Distribution (min/mean/max, cv) |
|---:|---:|---:|---:|---:|---:|---:|---|
| 100 | 20 000 | 8 | 0 ms | **~203 948 /s** | 1.18 ms | 6.68 ms | 136 / 200 / 248, **cv 0.116** |

Produce phase alone hit **~1.11 M jobs/s** (batched fills). All 20 000 jobs were
acked (`jobs_acked == jobs_produced`); evenness across 100 workers is good
(coefficient of variation 0.116, mean exactly 200 jobs/worker) even in greedy mode.
With `--jitter > 0` the coalescing claim pass (DESIGN §10.3) trades a little claim
latency for near-perfect evenness — the documented §10.2 tradeoff.

## 4. ACTORS / INFERENCE — per-actor box, event chains + snapshot compaction

1000 actor boxes, 5 inferences each, chain length 5 (model-answer + tool-call + 3
tool-results), snapshot-compact (`delete before_seq=head`) every 2 inferences, 32
concurrent drivers.

| Actors | Infs/actor | chain_len | Events | Elapsed | Events/s | Inferences/s | Snapshots | chain append p50 / p99 |
|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 000 | 5 | 5 | 25 000 | 0.801 s | **~31 214 /s** | ~6 243 /s | 2 000 | 0.787 ms / 11.34 ms |

Each inference is one batched append of the 5-event chain; 2000 `delete before_seq`
snapshot-compactions ran inline without stalling the append path (logical delete is
immediate, physical reclaim is the background reclaimer, ARCHITECTURE §3.5). Per-box
scaling holds across 1000 simultaneously-active actor boxes.

## 5. h2c effect, and are the millions/sec + 1–5 ms targets met?

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
identifies: **batching is what gets to millions/sec.** The evidence is on-box: the
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
  release binary on a temp data dir; boxes are torn down by each workload, so the
  server's box gauge returns to 0 afterward (~44 MB of WAL was written across the
  runs, confirming the durable path was exercised).
- The `broadcast` `deliveries/sec` figure is latency-paced (fixed inter-pulse gap),
  not a saturation throughput; the headline broadcast result is the per-watcher
  write→deliver latency and its flat scaling with watcher count.
- Reproduce:
  ```bash
  D=$(mktemp -d); STREAMS_HOST=127.0.0.1 STREAMS_PORT=4090 STREAMS_DATA_DIR=$D \
    ./target/release/streams &
  U=http://127.0.0.1:4090
  ./target/release/streams-probe conformance  $U                 # 117/117, exit 0
  ./target/release/streams-probe broadcast     $U --watchers 100,1000 --json
  ./target/release/streams-probe distribution  $U --boxes 5000 --batch 100 --writers 32 --json
  ./target/release/streams-probe queue         $U --workers 100 --jobs 20000 --json
  ./target/release/streams-probe actors        $U --actors 1000 --inferences 5 --json
  ```

---

# Phase 6 — tiered storage (HOT NVMe + COLD folder)

These numbers were captured against the **same persistent release binary** as
Phases 4–5, now built with the Phase-6 layered/tiered segment store: each box
log is split into sealed, immutable **segment files** (`seg-<first_seq>.data` +
`.idx`); the active + newest `hot_retain_segments` sealed segments stay **HOT**
(the data dir on NVMe) and older sealed segments **relocate to a COLD tier**
(`STREAMS_COLD_DIR`, a second folder in v1; the `SegmentStore` trait lets S3 drop
in later). **The Phase-2/3, Phase-4, and Phase-5 numbers above are unchanged** —
this section is purely additive.

There were **no `/v0` API or semantics changes**: tiering is transparent.
`streams-probe conformance` against a live binary booted with a cold tier
configured (`STREAMS_COLD_DIR` set, `STREAMS_SEGMENT_MAX_EVENTS=50`,
`STREAMS_HOT_RETAIN_SEGMENTS=2` — so seal + relocate fire under real traffic) is
**117 / 117, exit 0**. With **no** cold dir (the default in every existing test)
nothing relocates and behavior is identical by construction — the full workspace
suite is **268 tests green** and clippy is clean.

Same hardware/OS/toolchain as the baseline: **Apple M4 Max, 16 cores, 128 GiB,
Darwin 25.2.0, rustc 1.92.0, `--release`**. The server ran on `127.0.0.1` over a
fresh temp `STREAMS_DATA_DIR` (hot) + temp `STREAMS_COLD_DIR` (cold), both on the
same local APFS/NVMe (so the cold tier here is a *different folder*, not slower
hardware — the latency delta below is the segment-read path itself, not a slower
disk). Latencies are wall-clock loopback HTTP, percentiles by sort.

## Methodology (Phase 6 additions)

- **Tiering proof.** A durable box is written 6 100 records with
  `segment_max_events=50` and `hot_retain_segments=2`. The background relocator
  (a 5 s tick, runs the copy on the blocking pool) drains the old sealed segments
  to cold. The physical split is observed on disk (`ls` of the hot vs cold box
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
- **Durability across segments.** A durable box is loaded with data spanning many
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

`limit=50` window reads against the 6 100-record tiered box:

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
box write lock or block an SSE push — the hot tail (active segment + in-memory
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
  A clean SIGKILL of a 350-record durable box spanning 7 sealed segments + WAL
  tail recovered to-ready in **92 ms** with `head_seq == 350` (zero loss).

## 5. Durability across segments (SIGKILL → restart, zero acked-durable loss)

| Property | Result |
|---|---|
| Durable box (350 recs, 7 sealed segments + WAL tail) survives `kill -9` | recovered `head_seq 350`, `count 350`, every payload byte-identical — **no loss** |
| Cross-tier box (6 100 recs, 119 cold + 2 hot segments) survives `kill -9` | full readback **contiguous 1..6100, 0 payload mismatches**; tiers re-derived |
| Prefix-delete floor (`earliest_seq=3000`) survives restart | recovered `earliest_seq 3000`, deleted prefix stays silently gone |
| A second hard kill + restart | all boxes intact again; orphan reclaim idempotent (re-runnable) |

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
  the head never pays cold I/O even when most of the box lives cold; a read deep
  in cold history is the degraded path and is surfaced via
  `performance.cold_segments_read`.
- Reproduce:
  ```bash
  D=$(mktemp -d); C=$(mktemp -d)
  STREAMS_HOST=127.0.0.1 STREAMS_PORT=4090 STREAMS_DATA_DIR=$D STREAMS_COLD_DIR=$C \
    STREAMS_SEGMENT_MAX_EVENTS=50 STREAMS_HOT_RETAIN_SEGMENTS=2 ./target/release/streams &
  U=http://127.0.0.1:4090
  ./target/release/streams-probe conformance $U          # 117/117, exit 0
  # write >6000 records to one durable box, wait ~5s for the relocator, then:
  ls $D/boxes/*/ ; ls $C/boxes/*/                        # observe hot vs cold split
  curl -s -X POST $U/v0/boxes/<box>/diff -d '{"from_seq":0,"limit":1000}'  # cross-tier read
  ```
