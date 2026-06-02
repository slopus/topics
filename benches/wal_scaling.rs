//! WAL-sharding scaling benchmark (Stage 2).
//!
//! Drives many concurrent **durable** (`fsync`-class) writers spread across many
//! topics against a real, on-disk durable [`Engine`], and measures the AGGREGATE
//! write-ack throughput (acks/sec) at several `TOPICS_WAL_SHARDS` values. The
//! goal is to show write throughput scaling ~linearly with shard count up to the
//! core / NVMe-fsync limit: each shard is an independent writer thread + mpsc +
//! fsync stream, so spreading topics across shards lets N group-commit fsyncs run
//! in parallel instead of serializing on one writer.
//!
//! This is a plain `main` (`harness = false`) rather than a criterion bench: we
//! want a single run that sweeps shard counts and prints a scaling table, with
//! full control over the thread pool (one OS thread per concurrent writer, so the
//! blocking `CommitToken::wait` is real). Criterion's per-iteration harness fits
//! a single micro-op, not a multi-threaded aggregate-throughput sweep.
//!
//! Run (release is important — the fsync path is the point, not debug overhead):
//!
//! ```text
//! cargo bench --bench wal_scaling
//! # or with overrides:
//! WAL_BENCH_SHARDS=1,2,4,8,16 WAL_BENCH_WRITERS=64 WAL_BENCH_TOPICS=256 \
//!   WAL_BENCH_OPS=20000 WAL_BENCH_PAYLOAD=256 cargo bench --bench wal_scaling
//! ```
//!
//! Each (shards) point is measured `WAL_BENCH_REPEAT` times (default 3) and the
//! BEST (highest-throughput) run is reported, to reduce the effect of background
//! noise / one-off fsync stalls on the shared device.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Instant;

use serde_json::json;
use topics::clock::{SharedClock, SystemClock};
use topics::config::ServerConfig;
use topics::engine::Engine;
use topics::types::{Durability, RecordIn, TopicConfig, WriteRequest};

/// One benchmark configuration.
struct Params {
    /// Shard counts to sweep.
    shards: Vec<usize>,
    /// Number of concurrent OS writer threads.
    writers: usize,
    /// Number of distinct topics the writers spread their writes across.
    topics: usize,
    /// Total durable write-acks to perform per measured run (across all writers).
    ops: usize,
    /// Payload size in bytes per record.
    payload: usize,
    /// Records per write request (batch size). 1 = one ack per record.
    batch: usize,
    /// Durability class for the topics under test.
    durability: Durability,
    /// Repeats per shard count; the best run is reported.
    repeat: usize,
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .filter(|&n: &usize| n > 0)
        .unwrap_or(default)
}

fn env_list(key: &str, default: &[usize]) -> Vec<usize> {
    match std::env::var(key) {
        Ok(v) => {
            let parsed: Vec<usize> = v
                .split(',')
                .filter_map(|s| s.trim().parse().ok())
                .filter(|&n: &usize| n > 0)
                .collect();
            if parsed.is_empty() {
                default.to_vec()
            } else {
                parsed
            }
        }
        Err(_) => default.to_vec(),
    }
}

impl Params {
    fn from_env() -> Self {
        let durability = match std::env::var("WAL_BENCH_DURABILITY")
            .unwrap_or_default()
            .as_str()
        {
            "disk" => Durability::Disk,
            "memory" => Durability::Memory,
            _ => Durability::Fsync,
        };
        Params {
            shards: env_list("WAL_BENCH_SHARDS", &[1, 2, 4, 8, 16]),
            writers: env_usize("WAL_BENCH_WRITERS", 64),
            topics: env_usize("WAL_BENCH_TOPICS", 256),
            ops: env_usize("WAL_BENCH_OPS", 20_000),
            payload: env_usize("WAL_BENCH_PAYLOAD", 256),
            batch: env_usize("WAL_BENCH_BATCH", 1),
            durability,
            repeat: env_usize("WAL_BENCH_REPEAT", 3),
        }
    }
}

/// A topic config of the requested durability class.
fn topic_config(d: Durability) -> TopicConfig {
    TopicConfig {
        durability: Some(d),
        // `durable` is kept consistent with the class for back-compat (fsync ⇒ true).
        durable: d == Durability::Fsync,
        ..TopicConfig::default()
    }
}

/// A JSON payload of roughly `target` bytes.
fn payload(target: usize) -> serde_json::Value {
    let pad = target.saturating_sub(9).max(1);
    json!({ "p": "x".repeat(pad) })
}

fn write_req(batch: usize, data: &serde_json::Value) -> WriteRequest {
    WriteRequest {
        records: (0..batch)
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

/// Aggregate WAL counters captured at the end of a run, for diagnosing whether
/// throughput is fsync-bound or batching-bound.
#[derive(Default, Clone, Copy)]
struct Diag {
    fsyncs: u64,
    frames: u64,
    batches: u64,
    fsync_count: u64,
    fsync_micros_total: u64,
}

/// Run one measured pass: `writers` threads each drive durable writes round-robin
/// across `topics` topics until `ops` total writes are done. Returns (acks/sec, diag).
fn measure_once(p: &Params, shards: usize) -> (f64, Diag) {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = ServerConfig {
        data_dir: Some(dir.path().to_string_lossy().into_owned()),
        wal_shards: shards,
        ..ServerConfig::default()
    };
    let clock: SharedClock = Arc::new(SystemClock);
    let engine = Engine::with_data_dir(config, clock).expect("durable engine");

    // Pre-create every topic (so the create-lock path is out of the measured loop).
    let names: Vec<String> = (0..p.topics).map(|i| format!("bench-{i:05}")).collect();
    for name in &names {
        engine
            .put_topic(name, topic_config(p.durability))
            .expect("put_topic");
    }

    let data = payload(p.payload);
    let names = Arc::new(names);
    let engine_c = engine.clone();
    // A shared monotonically-increasing op dispenser so the total work is exactly
    // `ops` regardless of how the threads interleave; each claimed index picks a
    // topic round-robin so the writes spread evenly across topics (and thus shards).
    let next = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(p.writers + 1));
    let total_ops = p.ops;

    let mut handles = Vec::with_capacity(p.writers);
    for _ in 0..p.writers {
        let engine = engine_c.clone();
        let names = names.clone();
        let next = next.clone();
        let data = data.clone();
        let barrier = barrier.clone();
        let batch = p.batch;
        let nboxes = p.topics;
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                if i >= total_ops {
                    break;
                }
                let name = &names[i % nboxes];
                let req = write_req(batch, &data);
                engine.write(name, req, false).expect("durable write ack");
            }
        }));
    }

    // Start the clock only once every writer is parked on the barrier, so thread
    // spawn time is excluded from the measured window.
    barrier.wait();
    let t0 = Instant::now();
    for h in handles {
        h.join().expect("writer joined");
    }
    let elapsed = t0.elapsed();

    // One ack == one completed write request (a batch acks as a unit). Report acks
    // (write requests) per second — the aggregate durable write-ack throughput.
    let acks = total_ops as f64;
    let secs = elapsed.as_secs_f64().max(1e-9);

    let diag = match engine.wal_metrics() {
        Some(m) => Diag {
            fsyncs: m.fsyncs.load(Ordering::Relaxed),
            frames: m.frames.load(Ordering::Relaxed),
            batches: m.batches.load(Ordering::Relaxed),
            fsync_count: m.fsync_count.load(Ordering::Relaxed),
            fsync_micros_total: m.fsync_micros_total.load(Ordering::Relaxed),
        },
        None => Diag::default(),
    };
    (acks / secs, diag)
}

fn main() {
    let p = Params::from_env();
    let cls = match p.durability {
        Durability::Fsync => "fsync",
        Durability::Disk => "disk",
        Durability::Memory => "memory",
    };
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(0);

    println!("# topics WAL-sharding scaling benchmark");
    println!(
        "# cores(available_parallelism)={cores}  writers={}  topics={}  ops/run={}  \
         payload={}B  batch={}  durability={}  repeat={}",
        p.writers, p.topics, p.ops, p.payload, p.batch, cls, p.repeat
    );
    println!("#");
    println!("# shards   acks/sec     speedup-vs-1   per-shard-eff");
    println!("# ------   ----------   ------------   -------------");

    let diag_on = std::env::var("WAL_BENCH_DIAG").is_ok();
    let mut baseline: Option<f64> = None;
    let mut rows: Vec<(usize, f64)> = Vec::new();
    for &shards in &p.shards {
        let mut best = 0.0f64;
        let mut best_diag = Diag::default();
        for _ in 0..p.repeat {
            let (tput, diag) = measure_once(&p, shards);
            if tput > best {
                best = tput;
                best_diag = diag;
            }
        }
        if baseline.is_none() {
            baseline = Some(best);
        }
        let base = baseline.unwrap();
        let speedup = best / base;
        // Per-shard efficiency relative to the 1-shard baseline: 1.0 == perfectly
        // linear (speedup == shard count, normalized to the first measured point).
        let eff = speedup / (shards as f64 / p.shards[0] as f64);
        println!(
            "  {shards:>6}   {best:>10.0}   {speedup:>11.2}x   {eff:>11.2}",
            shards = shards,
            best = best,
            speedup = speedup,
            eff = eff,
        );
        if diag_on {
            let d = best_diag;
            let batch_factor = if d.fsyncs > 0 {
                d.frames as f64 / d.fsyncs as f64
            } else {
                0.0
            };
            let avg_fsync_us = if d.fsync_count > 0 {
                d.fsync_micros_total as f64 / d.fsync_count as f64
            } else {
                0.0
            };
            println!(
                "#          diag: fsyncs={} frames={} batches={} frames/fsync={:.1} avg_fsync={:.1}us",
                d.fsyncs, d.frames, d.batches, batch_factor, avg_fsync_us
            );
        }
        rows.push((shards, best));
    }

    println!("#");
    println!("# acks/sec by shard count (raw): {rows:?}");
}
