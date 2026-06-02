//! Criterion micro-benchmarks for the engine core (Phase-3 §5).
//!
//! These call the engine **directly** (no HTTP) with a real `SystemClock`, so
//! the numbers reflect the in-memory append/read/index/evict/delete hot paths
//! without router/transport overhead. Groups are named so the Baseline stage
//! can read medians per metric:
//!
//!   * `append`     — single-record and batched (1/10/100/1000) at 64 B / 1 KiB.
//!   * `diff`       — getDifference at limit 1 / 256 / 1000 over a warm topic.
//!   * `tag_match`  — tag-index match cost, exact (`Eq`) and prefix (`Glob`).
//!   * `cap_evict`  — cap eviction cost on the write path (`discard:"old"`).
//!   * `delete`     — `before_seq` (prefix) delete and `match` (tag) delete.
//!
//! Each group uses `Throughput::Elements` where a per-record rate is meaningful
//! so criterion reports elements/sec alongside wall-clock medians.

use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use serde_json::{json, Value};

use topics::clock::{Clock, SharedClock, SystemClock};
use topics::config::ServerConfig;
use topics::engine::topic_state::{StoredRecord, TopicIndex, TopicState};
use topics::engine::{Engine, SEQ_BASE};
use topics::types::{DiffRequest, Discard, Filter, FilterOp, RecordIn, TopicConfig, WriteRequest};

const TOPIC: &str = "bench";

/// A real (wall-clock) shared clock; correctness is asserted elsewhere with
/// `TestClock`, here we only measure CPU work.
fn clock() -> SharedClock {
    Arc::new(SystemClock)
}

/// A fresh engine with the default config.
fn fresh_engine() -> Arc<Engine> {
    Engine::new(ServerConfig::default(), clock())
}

/// A JSON payload of roughly `target` bytes (a string field padded out).
fn payload(target: usize) -> Value {
    // `{"p":"<pad>"}` framing is ~9 bytes; pad the rest.
    let pad = target.saturating_sub(9).max(1);
    json!({ "p": "x".repeat(pad) })
}

/// Build a write request of `n` records carrying `data`, no tags.
fn write_req(n: usize, data: &Value) -> WriteRequest {
    WriteRequest {
        records: (0..n)
            .map(|_| RecordIn {
                data: data.clone(),
                tag: None,
                node: None,
                meta: None,
            })
            .collect(),
        node: None,
        idempotency_key: None,
        create: None,
        config: None,
        disable_backpressure: false,
    }
}

// ---------------------------------------------------------------------------
// append — single + batched, at 64 B and 1 KiB payloads.
// ---------------------------------------------------------------------------

fn bench_append(c: &mut Criterion) {
    let mut g = c.benchmark_group("append");
    // Each batch is created against a FRESH engine so the topic starts empty and
    // we measure pure append (no growing-index artifacts), via iter_batched.
    for &payload_bytes in &[64usize, 1024] {
        let data = payload(payload_bytes);
        for &batch in &[1usize, 10, 100, 1000] {
            g.throughput(Throughput::Elements(batch as u64));
            let id = BenchmarkId::new(format!("{payload_bytes}B"), batch);
            g.bench_with_input(id, &batch, |b, &batch| {
                b.iter_batched(
                    || (fresh_engine(), write_req(batch, &data)),
                    |(engine, req)| {
                        engine.write(TOPIC, req, false).unwrap();
                    },
                    BatchSize::SmallInput,
                );
            });
        }
    }
    g.finish();
}

// ---------------------------------------------------------------------------
// diff — getDifference at limit 1 / 256 / 1000 over a warm, populated topic.
// ---------------------------------------------------------------------------

/// A warm engine whose `TOPIC` holds `n` records of `payload_bytes` each.
fn warm_engine(n: usize, payload_bytes: usize) -> Arc<Engine> {
    let engine = fresh_engine();
    let data = payload(payload_bytes);
    // Populate in chunks so a single write stays well under the batch cap.
    let mut remaining = n;
    while remaining > 0 {
        let chunk = remaining.min(1000);
        engine.write(TOPIC, write_req(chunk, &data), false).unwrap();
        remaining -= chunk;
    }
    engine
}

fn bench_diff(c: &mut Criterion) {
    const WARM: usize = 10_000;
    let engine = warm_engine(WARM, 64);
    let mut g = c.benchmark_group("diff");
    for &limit in &[1u32, 256, 1000] {
        // The number of records actually returned per call is min(limit, WARM).
        g.throughput(Throughput::Elements(limit.min(WARM as u32) as u64));
        g.bench_with_input(BenchmarkId::from_parameter(limit), &limit, |b, &limit| {
            b.iter(|| {
                let req = DiffRequest {
                    from_seq: 0,
                    limit,
                    ..Default::default()
                };
                let resp = engine.diff(TOPIC, req).unwrap();
                resp.records.len()
            });
        });
    }
    g.finish();
}

// ---------------------------------------------------------------------------
// tag_match — pure tag-index match cost (no delete), exact + prefix.
//
// Builds a TopicIndex with many tagged records spread over many tags so the
// prefix scan is meaningful, then measures `matching_live_seqs` directly.
// ---------------------------------------------------------------------------

/// A `TopicIndex` with `n` records, tagged `tenant:<i % tenants>`, so each tag
/// holds ~`n / tenants` postings. The prefix `tenant:` matches all of them.
fn tagged_index(n: usize, tenants: usize) -> (TopicIndex, u64) {
    let mut index = TopicIndex::new(SEQ_BASE);
    for i in 0..n {
        let seq = SEQ_BASE + i as u64;
        let tag = format!("tenant:{}", i % tenants);
        index.records.push_back(StoredRecord {
            ts: 0,
            node: None,
            tag: Some(tag.clone()),
            data: Value::Null,
            meta: None,
            bytes: 0,
            deleted: false,
            payload_resident: true,
            hops: 0,
        });
        index.index_tag(seq, &tag);
    }
    let bound = SEQ_BASE + n as u64; // exclusive upper bound = head + 1.
    (index, bound)
}

fn bench_tag_match(c: &mut Criterion) {
    const N: usize = 10_000;
    const TENANTS: usize = 100; // ~100 postings per exact tag, all under prefix.
    let (index, bound) = tagged_index(N, TENANTS);

    let mut g = c.benchmark_group("tag_match");

    // Exact: a single tag's posting list (~N/TENANTS seqs).
    let eq = Filter {
        op: FilterOp::Eq,
        value: "tenant:42".to_string(),
    };
    g.throughput(Throughput::Elements((N / TENANTS) as u64));
    g.bench_function("exact", |b| {
        b.iter(|| index.matching_live_seqs(&eq, bound).len());
    });

    // Prefix `tenant:*`: a range scan over every tag (matches all N).
    let glob = Filter {
        op: FilterOp::Glob,
        value: "tenant:".to_string(),
    };
    g.throughput(Throughput::Elements(N as u64));
    g.bench_function("prefix", |b| {
        b.iter(|| index.matching_live_seqs(&glob, bound).len());
    });

    g.finish();
}

// ---------------------------------------------------------------------------
// cap_evict — cap eviction cost on the write path. A `discard:"old"` topic with
// a small `cap_records` is kept full, so every write evicts the oldest front
// records (lazy reclaim + tag/byte accounting) inside enforce_retention.
// ---------------------------------------------------------------------------

fn capped_topic_config(cap: u64) -> TopicConfig {
    TopicConfig {
        cap_records: cap,
        discard: Discard::Old,
        ..Default::default()
    }
}

/// An engine whose `TOPIC` is at its `cap_records` so the next writes evict.
fn full_capped_engine(cap: u64, payload_bytes: usize) -> Arc<Engine> {
    let engine = fresh_engine();
    engine.put_topic(TOPIC, capped_topic_config(cap)).unwrap();
    let data = payload(payload_bytes);
    // Fill to the cap.
    let mut remaining = cap as usize;
    while remaining > 0 {
        let chunk = remaining.min(1000);
        engine.write(TOPIC, write_req(chunk, &data), false).unwrap();
        remaining -= chunk;
    }
    engine
}

fn bench_cap_evict(c: &mut Criterion) {
    const CAP: u64 = 10_000;
    let mut g = c.benchmark_group("cap_evict");

    // Steady-state: topic at cap, append one batch which evicts an equal number
    // of front records. Measures the per-batch evict + append cost.
    for &batch in &[1usize, 100] {
        g.throughput(Throughput::Elements(batch as u64));
        g.bench_with_input(BenchmarkId::from_parameter(batch), &batch, |b, &batch| {
            let engine = full_capped_engine(CAP, 64);
            let data = payload(64);
            b.iter(|| {
                engine.write(TOPIC, write_req(batch, &data), false).unwrap();
            });
        });
    }
    g.finish();
}

// ---------------------------------------------------------------------------
// delete — before_seq (prefix) delete and match (tag) delete cost.
//
// Each iteration deletes from a FRESH warm topic (via iter_batched) so we always
// measure the cost of removing live records, never a no-op second delete.
// ---------------------------------------------------------------------------

/// A `TopicState` with `n` records, half tagged `hot`, half tagged `cold`.
fn warm_topic(n: usize) -> Arc<TopicState> {
    let b = Arc::new(TopicState::new(
        TOPIC.to_string(),
        1, // interned topic_id (irrelevant to the in-memory bench).
        TopicConfig::default(),
        SEQ_BASE,
        1,
    ));
    let now = SystemClock.now_ms();
    let records: Vec<StoredRecord> = (0..n)
        .map(|i| StoredRecord {
            ts: now,
            node: None,
            tag: Some(if i % 2 == 0 { "hot" } else { "cold" }.to_string()),
            data: json!({ "p": "x".repeat(55) }),
            meta: None,
            bytes: 64,
            deleted: false,
            payload_resident: true,
            hops: 0,
        })
        .collect();
    b.append(records, now);
    b
}

fn bench_delete(c: &mut Criterion) {
    const N: usize = 10_000;
    let now = SystemClock.now_ms();
    let mut g = c.benchmark_group("delete");
    g.throughput(Throughput::Elements(N as u64));

    // before_seq: delete the entire live prefix (all N records).
    g.bench_function("before_seq_all", |b| {
        b.iter_batched(
            || warm_topic(N),
            |bx| bx.apply_delete(Some(SEQ_BASE + N as u64), None, None, now),
            BatchSize::SmallInput,
        );
    });

    // match exact `hot`: deletes ~N/2 records via the tag index point lookup.
    let hot = Filter {
        op: FilterOp::Eq,
        value: "hot".to_string(),
    };
    g.throughput(Throughput::Elements((N / 2) as u64));
    g.bench_function("match_exact", |b| {
        b.iter_batched(
            || warm_topic(N),
            |bx| bx.apply_delete(None, Some(&hot), None, now),
            BatchSize::SmallInput,
        );
    });

    g.finish();
}

criterion_group!(
    benches,
    bench_append,
    bench_diff,
    bench_tag_match,
    bench_cap_evict,
    bench_delete,
);
criterion_main!(benches);
