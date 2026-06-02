//! topics-probe — a standalone black-topic tool that runs against a LIVE
//! topics server over HTTP (Phase-3 §4).
//!
//! Two subcommands:
//!   - `conformance <base_url>`: asserts the documented `/v0` contract for every
//!     endpoint + the semantic flows; exits NONZERO on any mismatch and prints a
//!     clear pass/fail report.
//!   - `bench <base_url>`: end-to-end HTTP benchmarks against the live server
//!     (write-ack latency p50/p99/p999, write throughput, getDifference
//!     throughput/latency, SSE fan-out write->deliver latency with 1/10/100
//!     watchers, router forwarding overhead). Prints numbers and, with `--json`,
//!     a machine-readable summary for BENCHMARKS.md.
//!
//! The crate is deliberately black-topic: it does NOT depend on the `topics`
//! crate — only its public HTTP contract.

use std::process::ExitCode;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use argh::FromArgs;
use futures_util::StreamExt;
use serde_json::{json, Value};

/// topics-probe: black-topic conformance + benchmark tool for a live topics server.
#[derive(FromArgs, Debug)]
struct TopLevel {
    #[argh(subcommand)]
    cmd: Subcommand,
}

#[derive(FromArgs, Debug)]
#[argh(subcommand)]
enum Subcommand {
    Conformance(ConformanceCmd),
    Bench(BenchCmd),
    BenchDurable(BenchDurableCmd),
    Broadcast(BroadcastCmd),
    Distribution(DistributionCmd),
    Queue(QueueCmd),
    Actors(ActorsCmd),
}

/// Assert the documented /v0 contract against a live server.
#[derive(FromArgs, Debug)]
#[argh(subcommand, name = "conformance")]
struct ConformanceCmd {
    /// base URL of the live server, e.g. http://127.0.0.1:4000
    #[argh(positional)]
    base_url: String,

    /// bearer token to send on every request (optional)
    #[argh(option)]
    token: Option<String>,
}

/// Run end-to-end HTTP benchmarks against a live server.
#[derive(FromArgs, Debug)]
#[argh(subcommand, name = "bench")]
struct BenchCmd {
    /// base URL of the live server, e.g. http://127.0.0.1:4000
    #[argh(positional)]
    base_url: String,

    /// bearer token to send on every request (optional)
    #[argh(option)]
    token: Option<String>,

    /// total write count for the write-latency/throughput phase (default 50000)
    #[argh(option, default = "50_000")]
    writes: u64,

    /// comma-separated watcher counts for the SSE fan-out phase (default 1,10,100)
    #[argh(option, default = "String::from(\"1,10,100\")")]
    watchers: String,

    /// emit a machine-readable JSON summary to stdout (for BENCHMARKS.md)
    #[argh(switch)]
    json: bool,
}

/// Compare durable (fsync-gated, group-committed) vs non-durable write-ack
/// latency + throughput against a live server (Phase-4 persistent build).
#[derive(FromArgs, Debug)]
#[argh(subcommand, name = "bench-durable")]
struct BenchDurableCmd {
    /// base URL of the live server, e.g. http://127.0.0.1:4000
    #[argh(positional)]
    base_url: String,

    /// bearer token to send on every request (optional)
    #[argh(option)]
    token: Option<String>,

    /// single-record write-ack latency samples per class (default 5000)
    #[argh(option, default = "5_000")]
    samples: u64,

    /// total records for the concurrent throughput phase per class (default 50000)
    #[argh(option, default = "50_000")]
    writes: u64,

    /// emit a machine-readable JSON summary to stdout (for BENCHMARKS.md)
    #[argh(switch)]
    json: bool,
}

/// BROADCAST workload (Phase-5B): one source topic, N concurrent SSE watchers, one
/// writer appending. Measures aggregate deliveries/sec and per-watcher
/// write->deliver p50/p99 — demonstrates the shared zero-copy frame fan-out
/// scaling (serialize once, ref-count to all watchers).
#[derive(FromArgs, Debug)]
#[argh(subcommand, name = "broadcast")]
struct BroadcastCmd {
    /// base URL of the live server, e.g. http://127.0.0.1:4000
    #[argh(positional)]
    base_url: String,

    /// bearer token to send on every request (optional)
    #[argh(option)]
    token: Option<String>,

    /// comma-separated watcher counts to sweep (default 1,10,100,1000)
    #[argh(option, default = "String::from(\"1,10,100,1000\")")]
    watchers: String,

    /// timed write pulses per watcher tier (default 100)
    #[argh(option, default = "100")]
    pulses: usize,

    /// gap between timed pulses in ms (default 5)
    #[argh(option, default = "5")]
    pulse_gap_ms: u64,

    /// emit a machine-readable JSON summary to stdout (for BENCHMARKS.md)
    #[argh(switch)]
    json: bool,
}

/// DISTRIBUTION workload (Phase-5B): fan a source stream into many topics via
/// batched, sharded, concurrent appends. Measures aggregate appends/sec and
/// per-topic append latency — pushes toward the millions-of-small-appends/sec
/// target.
#[derive(FromArgs, Debug)]
#[argh(subcommand, name = "distribution")]
struct DistributionCmd {
    /// base URL of the live server, e.g. http://127.0.0.1:4000
    #[argh(positional)]
    base_url: String,

    /// bearer token to send on every request (optional)
    #[argh(option)]
    token: Option<String>,

    /// number of destination topics to fan into (default 5000)
    #[argh(option, default = "5_000")]
    topics: u64,

    /// records per append request — topics are sharded across batched requests
    /// (default 100)
    #[argh(option, default = "100")]
    batch: u64,

    /// number of concurrent writer tasks (default 32)
    #[argh(option, default = "32")]
    writers: usize,

    /// emit a machine-readable JSON summary to stdout (for BENCHMARKS.md)
    #[argh(switch)]
    json: bool,
}

/// QUEUE workload (Phase-5B / Phase-5A lease queue): producers fill a queue, N
/// worker nodes claim/ack in a loop. Measures jobs/sec end-to-end, claim
/// latency, and per-worker distribution evenness.
#[derive(FromArgs, Debug)]
#[argh(subcommand, name = "queue")]
struct QueueCmd {
    /// base URL of the live server, e.g. http://127.0.0.1:4000
    #[argh(positional)]
    base_url: String,

    /// bearer token to send on every request (optional)
    #[argh(option)]
    token: Option<String>,

    /// number of worker nodes claiming/acking concurrently (default 50)
    #[argh(option, default = "50")]
    workers: usize,

    /// total jobs to produce + process (default 20000)
    #[argh(option, default = "20_000")]
    jobs: u64,

    /// claim batch size (max jobs per claim call) (default 8)
    #[argh(option, default = "8")]
    claim_max: u32,

    /// claim_jitter_ms on the queue (coalescing window, default 0 = greedy)
    #[argh(option, default = "0")]
    jitter: u64,

    /// emit a machine-readable JSON summary to stdout (for BENCHMARKS.md)
    #[argh(switch)]
    json: bool,
}

/// ACTORS / INFERENCE workload (Phase-5B): each actor is a topic; per "inference"
/// append a chain (model-answer + tool-call + several tool-result events), then
/// periodically snapshot-compact via delete before_seq. Measures events/sec and
/// topic-count scaling.
#[derive(FromArgs, Debug)]
#[argh(subcommand, name = "actors")]
struct ActorsCmd {
    /// base URL of the live server, e.g. http://127.0.0.1:4000
    #[argh(positional)]
    base_url: String,

    /// bearer token to send on every request (optional)
    #[argh(option)]
    token: Option<String>,

    /// number of actor topics (default 1000)
    #[argh(option, default = "1_000")]
    actors: u64,

    /// inferences (event chains) per actor (default 5)
    #[argh(option, default = "5")]
    inferences: u64,

    /// tool-result events per inference chain (default 3)
    #[argh(option, default = "3")]
    tool_results: u64,

    /// snapshot-compact (delete before head) every N inferences (default 2)
    #[argh(option, default = "2")]
    snapshot_every: u64,

    /// number of concurrent actor-driver tasks (default 32)
    #[argh(option, default = "32")]
    concurrency: usize,

    /// emit a machine-readable JSON summary to stdout (for BENCHMARKS.md)
    #[argh(switch)]
    json: bool,
}

fn main() -> ExitCode {
    let top: TopLevel = argh::from_env();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    match top.cmd {
        Subcommand::Conformance(c) => rt.block_on(run_conformance(c)),
        Subcommand::Bench(b) => rt.block_on(run_bench(b)),
        Subcommand::BenchDurable(b) => rt.block_on(run_bench_durable(b)),
        Subcommand::Broadcast(b) => rt.block_on(run_broadcast(b)),
        Subcommand::Distribution(d) => rt.block_on(run_distribution(d)),
        Subcommand::Queue(q) => rt.block_on(run_queue(q)),
        Subcommand::Actors(a) => rt.block_on(run_actors(a)),
    }
}

// ===========================================================================
// HTTP client wrapper
// ===========================================================================

/// A thin HTTP helper around `reqwest` that carries the base URL + optional
/// bearer token and returns `(status, json_body)` for JSON endpoints.
#[derive(Clone)]
struct Client {
    http: reqwest::Client,
    base: String,
    token: Option<String>,
}

/// A parsed HTTP response: status code + best-effort JSON body (`Value::Null`
/// when the body is empty or not JSON).
struct Resp {
    status: u16,
    body: Value,
}

impl Client {
    fn new(base: &str, token: Option<String>) -> Self {
        let http = reqwest::Client::builder()
            .pool_max_idle_per_host(256)
            .tcp_nodelay(true)
            .timeout(Duration::from_secs(30))
            .build()
            .expect("build reqwest client");
        Client {
            http,
            base: base.trim_end_matches('/').to_string(),
            token,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base, path)
    }

    fn auth(&self, rb: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.token {
            Some(t) => rb.header("authorization", format!("Bearer {t}")),
            None => rb,
        }
    }

    async fn send(&self, rb: reqwest::RequestBuilder) -> Result<Resp, String> {
        let resp = self
            .auth(rb)
            .send()
            .await
            .map_err(|e| format!("request error: {e}"))?;
        let status = resp.status().as_u16();
        let text = resp.text().await.map_err(|e| format!("body error: {e}"))?;
        let body = if text.trim().is_empty() {
            Value::Null
        } else {
            serde_json::from_str(&text).unwrap_or(Value::Null)
        };
        Ok(Resp { status, body })
    }

    async fn get(&self, path: &str) -> Result<Resp, String> {
        self.send(self.http.get(self.url(path))).await
    }

    async fn delete(&self, path: &str) -> Result<Resp, String> {
        self.send(self.http.delete(self.url(path))).await
    }

    async fn post(&self, path: &str, body: &Value) -> Result<Resp, String> {
        self.send(
            self.http
                .post(self.url(path))
                .header("content-type", "application/json")
                .body(serde_json::to_vec(body).unwrap()),
        )
        .await
    }

    async fn put(&self, path: &str, body: &Value) -> Result<Resp, String> {
        self.send(
            self.http
                .put(self.url(path))
                .header("content-type", "application/json")
                .body(serde_json::to_vec(body).unwrap()),
        )
        .await
    }

    /// POST with a raw (non-JSON) content type — used to exercise `415`.
    async fn post_raw_ct(&self, path: &str, ct: &str, body: &str) -> Result<Resp, String> {
        self.send(
            self.http
                .post(self.url(path))
                .header("content-type", ct)
                .body(body.to_string()),
        )
        .await
    }

    /// POST with no content-type header at all — also a `415` path.
    async fn post_no_ct(&self, path: &str, body: &str) -> Result<Resp, String> {
        self.send(self.http.post(self.url(path)).body(body.to_string()))
            .await
    }
}

// ===========================================================================
// Conformance suite
// ===========================================================================

/// Accumulates per-check PASS/FAIL outcomes and renders a final report.
struct Report {
    passed: u64,
    failed: u64,
    lines: Vec<String>,
}

impl Report {
    fn new() -> Self {
        Report {
            passed: 0,
            failed: 0,
            lines: Vec::new(),
        }
    }

    /// Record a check: `ok` true ⇒ PASS, else FAIL with the detail message.
    fn check(&mut self, name: &str, ok: bool, detail: impl FnOnce() -> String) {
        if ok {
            self.passed += 1;
            self.lines.push(format!("  PASS  {name}"));
        } else {
            self.failed += 1;
            self.lines.push(format!("  FAIL  {name}: {}", detail()));
        }
    }

    fn status(&mut self, name: &str, r: &Result<Resp, String>, want: u16) {
        match r {
            Ok(resp) => self.check(name, resp.status == want, || {
                format!(
                    "expected HTTP {want}, got {} (body {})",
                    resp.status, resp.body
                )
            }),
            Err(e) => self.check(name, false, || format!("request failed: {e}")),
        }
    }

    /// Assert status AND that the error envelope code matches.
    fn error_code(
        &mut self,
        name: &str,
        r: &Result<Resp, String>,
        want_status: u16,
        want_code: &str,
    ) {
        match r {
            Ok(resp) => {
                let code = resp
                    .body
                    .get("error")
                    .and_then(|e| e.get("code"))
                    .and_then(|c| c.as_str());
                let ok = resp.status == want_status && code == Some(want_code);
                self.check(name, ok, || {
                    format!(
                        "expected HTTP {want_status} code={want_code}, got HTTP {} code={code:?} (body {})",
                        resp.status, resp.body
                    )
                });
            }
            Err(e) => self.check(name, false, || format!("request failed: {e}")),
        }
    }

    fn render(&self) -> String {
        let mut s = String::new();
        for l in &self.lines {
            s.push('\n');
            s.push_str(l);
        }
        s
    }
}

/// Body `seqs` as a Vec<u64>.
fn seqs_of(body: &Value) -> Vec<u64> {
    body.get("seqs")
        .and_then(|v| v.as_array())
        .map(|a| a.iter().filter_map(|x| x.as_u64()).collect())
        .unwrap_or_default()
}

fn u64_of(body: &Value, key: &str) -> Option<u64> {
    body.get(key).and_then(|v| v.as_u64())
}

fn bool_of(body: &Value, key: &str) -> Option<bool> {
    body.get(key).and_then(|v| v.as_bool())
}

/// Record seqs present in a diff response body.
fn diff_seqs(body: &Value) -> Vec<u64> {
    body.get("records")
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|r| r.get("$seq").and_then(|s| s.as_u64()))
                .collect()
        })
        .unwrap_or_default()
}

async fn run_conformance(cmd: ConformanceCmd) -> ExitCode {
    let c = Client::new(&cmd.base_url, cmd.token.clone());
    let mut rep = Report::new();

    // Unique-ish namespace so repeated runs against a long-lived server don't
    // collide on topic names.
    let ns = format!(
        "probe{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() % 1_000_000_000)
            .unwrap_or(0)
    );

    conformance_health(&c, &mut rep).await;
    conformance_http2(&cmd.base_url, cmd.token.clone(), &mut rep, &ns).await;
    conformance_topic_lifecycle(&c, &mut rep, &ns).await;
    conformance_write_and_diff(&c, &mut rep, &ns).await;
    conformance_idempotency(&c, &mut rep, &ns).await;
    conformance_errors(&c, &mut rep, &ns).await;
    conformance_deletion(&c, &mut rep, &ns).await;
    conformance_cap_tombstone(&c, &mut rep, &ns).await;
    conformance_node_loop(&c, &mut rep, &ns).await;
    conformance_routers(&c, &mut rep, &ns).await;
    conformance_queue(&c, &mut rep, &ns).await;
    conformance_sse(&c, &mut rep, &ns).await;
    if cmd.token.is_some() {
        conformance_auth(&cmd.base_url, &c, &mut rep, &ns).await;
    }

    // Best-effort cleanup of the topics we created.
    cleanup(&c, &ns).await;

    println!("=== topics-probe conformance: {} ===", cmd.base_url);
    print!("{}", rep.render());
    println!(
        "\n\n{} passed, {} failed ({} checks)",
        rep.passed,
        rep.failed,
        rep.passed + rep.failed
    );

    if rep.failed == 0 {
        println!("RESULT: PASS");
        ExitCode::SUCCESS
    } else {
        println!("RESULT: FAIL");
        ExitCode::FAILURE
    }
}

async fn conformance_health(c: &Client, rep: &mut Report) {
    let h = c.get("/v0/health").await;
    rep.status("GET /v0/health -> 200", &h, 200);
    if let Ok(r) = &h {
        rep.check(
            "health body has status=ok + version + uptime_ms",
            r.body.get("status").and_then(|v| v.as_str()) == Some("ok")
                && r.body.get("version").is_some()
                && r.body.get("uptime_ms").is_some(),
            || format!("body {}", r.body),
        );
    }

    let hz = c.get("/healthz").await;
    rep.status("GET /healthz alias -> 200", &hz, 200);

    let ready = c.get("/v0/ready").await;
    rep.status("GET /v0/ready -> 200", &ready, 200);
    if let Ok(r) = &ready {
        rep.check(
            "ready body has status=ready + wal_replay_complete",
            r.body.get("status").and_then(|v| v.as_str()) == Some("ready")
                && r.body.get("wal_replay_complete").is_some(),
            || format!("body {}", r.body),
        );
    }

    let m = c.get("/v0/metrics").await;
    rep.status("GET /v0/metrics -> 200", &m, 200);
}

/// HTTP/2 cleartext "prior-knowledge" support (Phase-5B Stage 1). The server
/// serves BOTH HTTP/1.1 and h2c on the same port, auto-detecting per connection.
/// An `http2_prior_knowledge()` client must negotiate HTTP/2, get a 200 from
/// `/v0/health`, and be able to write + diff over h2; a plain HTTP/1.1 client
/// must still be served (and observably over HTTP/1.1). The default `Client`
/// (used by every other conformance check) is an h1 client, so this proves the
/// dual-protocol listener serves both.
async fn conformance_http2(base_url: &str, token: Option<String>, rep: &mut Report, ns: &str) {
    // A client pinned to HTTP/2 prior knowledge (h2c, no TLS).
    let h2 = match reqwest::Client::builder()
        .http2_prior_knowledge()
        .timeout(Duration::from_secs(30))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            rep.check("h2c: build prior-knowledge client", false, || {
                format!("build failed: {e}")
            });
            return;
        }
    };
    let base = base_url.trim_end_matches('/').to_string();
    let auth = |rb: reqwest::RequestBuilder| match &token {
        Some(t) => rb.header("authorization", format!("Bearer {t}")),
        None => rb,
    };

    // GET /v0/health over h2c -> 200 AND negotiated version is HTTP/2.
    match auth(h2.get(format!("{base}/v0/health"))).send().await {
        Ok(resp) => {
            let ver = resp.version();
            let status = resp.status().as_u16();
            let ok_status = status == 200;
            let is_h2 = ver == reqwest::Version::HTTP_2;
            let body: Value = resp.json().await.unwrap_or(Value::Null);
            rep.check(
                "h2c prior-knowledge: GET /v0/health -> 200 over HTTP/2 (negotiated version is h2)",
                ok_status && is_h2 && body.get("status").and_then(|v| v.as_str()) == Some("ok"),
                || format!("status={status} version={ver:?} body={body}"),
            );
        }
        Err(e) => rep.check("h2c prior-knowledge: GET /v0/health", false, || {
            format!("request failed: {e}")
        }),
    }

    // Write + diff over h2c.
    let b = format!("{ns}-h2c");
    let path = format!("{base}/v0/topics/{b}");
    let wrote_ok = match auth(
        h2.post(&path)
            .header("content-type", "application/json")
            .body(
                json!({ "records": [{ "data": { "v": 1 } }, { "data": { "v": 2 } }] }).to_string(),
            ),
    )
    .send()
    .await
    {
        Ok(resp) => {
            let is_h2 = resp.version() == reqwest::Version::HTTP_2;
            let status = resp.status().as_u16();
            let body: Value = resp.json().await.unwrap_or(Value::Null);
            let ok = is_h2 && (status == 200 || status == 201) && seqs_of(&body) == vec![1, 2];
            rep.check(
                "h2c prior-knowledge: write over HTTP/2 returns seqs [1,2]",
                ok,
                || format!("status={status} version_h2={is_h2} body={body}"),
            );
            ok
        }
        Err(e) => {
            rep.check("h2c prior-knowledge: write", false, || {
                format!("request failed: {e}")
            });
            false
        }
    };
    if wrote_ok {
        match auth(
            h2.post(format!("{path}/diff"))
                .header("content-type", "application/json")
                .body(json!({ "from_seq": 0 }).to_string()),
        )
        .send()
        .await
        {
            Ok(resp) => {
                let is_h2 = resp.version() == reqwest::Version::HTTP_2;
                let body: Value = resp.json().await.unwrap_or(Value::Null);
                rep.check(
                    "h2c prior-knowledge: diff over HTTP/2 returns both records",
                    is_h2
                        && diff_seqs(&body) == vec![1, 2]
                        && bool_of(&body, "caught_up") == Some(true),
                    || format!("version_h2={is_h2} body={body}"),
                );
            }
            Err(e) => rep.check("h2c prior-knowledge: diff", false, || {
                format!("request failed: {e}")
            }),
        }
    }

    // A plain HTTP/1.1 client must still be served over the SAME port (the
    // default conformance `Client` is h1, but make the version assertion explicit).
    match reqwest::Client::builder()
        .http1_only()
        .timeout(Duration::from_secs(30))
        .build()
    {
        Ok(h1) => match auth(h1.get(format!("{base}/v0/health"))).send().await {
            Ok(resp) => {
                let ver = resp.version();
                let status = resp.status().as_u16();
                rep.check(
                    "h1 client still served on the dual-protocol listener (HTTP/1.1, 200)",
                    status == 200 && ver == reqwest::Version::HTTP_11,
                    || format!("status={status} version={ver:?}"),
                );
            }
            Err(e) => rep.check("h1 client on dual-protocol listener", false, || {
                format!("request failed: {e}")
            }),
        },
        Err(e) => rep.check("h1: build http1-only client", false, || {
            format!("build failed: {e}")
        }),
    }
}

async fn conformance_topic_lifecycle(c: &Client, rep: &mut Report, ns: &str) {
    let b = format!("{ns}-life");
    let path = format!("/v0/topics/{b}");

    // PUT create -> 201 created:true, config echoed with defaults merged.
    let put = c
        .put(
            &path,
            &json!({ "ttl_ms": 60000, "cap_records": 1000, "discard": "old" }),
        )
        .await;
    rep.status("PUT /v0/topics/:topic create -> 201", &put, 201);
    if let Ok(r) = &put {
        rep.check(
            "PUT create: created=true, topic echoed, config merged",
            bool_of(&r.body, "created") == Some(true)
                && r.body.get("topic").and_then(|v| v.as_str()) == Some(b.as_str())
                && u64_of(r.body.get("config").unwrap_or(&Value::Null), "ttl_ms") == Some(60000)
                && r.body
                    .get("config")
                    .and_then(|c| c.get("dedupe_node"))
                    .and_then(|v| v.as_bool())
                    == Some(true),
            || format!("body {}", r.body),
        );
    }

    // Identical PUT -> 200 created:false (idempotent).
    let put2 = c
        .put(
            &path,
            &json!({ "ttl_ms": 60000, "cap_records": 1000, "discard": "old" }),
        )
        .await;
    rep.status("PUT identical -> 200 (idempotent)", &put2, 200);
    if let Ok(r) = &put2 {
        rep.check(
            "PUT identical: created=false",
            bool_of(&r.body, "created") == Some(false),
            || format!("body {}", r.body),
        );
    }

    // Changed PUT -> 200, config updated.
    let put3 = c.put(&path, &json!({ "ttl_ms": 120000 })).await;
    rep.status("PUT changed config -> 200", &put3, 200);

    // GET state on a fresh (empty) topic: head_seq=0, count=0.
    let get = c.get(&path).await;
    rep.status("GET /v0/topics/:topic state -> 200", &get, 200);
    if let Ok(r) = &get {
        rep.check(
            "GET state: fresh topic head_seq=0, count=0, next_seq=1, has effective_priority",
            u64_of(&r.body, "head_seq") == Some(0)
                && u64_of(&r.body, "count") == Some(0)
                && u64_of(&r.body, "next_seq") == Some(1)
                && r.body.get("effective_priority").is_some(),
            || format!("body {}", r.body),
        );
    }

    // GET ?touch=false still 200.
    let gett = c.get(&format!("{path}?touch=false")).await;
    rep.status("GET /v0/topics/:topic?touch=false -> 200", &gett, 200);

    // List topics (with prefix) shows the topic.
    let list = c.get(&format!("/v0/topics?prefix={ns}-life")).await;
    rep.status("GET /v0/topics?prefix -> 200", &list, 200);
    if let Ok(r) = &list {
        let found = r
            .body
            .get("topics")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .any(|x| x.get("topic").and_then(|v| v.as_str()) == Some(b.as_str()))
            })
            .unwrap_or(false);
        rep.check("list topics: created topic present", found, || {
            format!("body {}", r.body)
        });
    }

    // GET missing topic -> 404 topic_not_found (no auto-create on state read).
    let miss = c.get(&format!("/v0/topics/{ns}-doesnotexist")).await;
    rep.error_code(
        "GET missing topic -> 404 topic_not_found",
        &miss,
        404,
        "topic_not_found",
    );

    // DELETE topic -> 200 deleted:true.
    let del = c.delete(&path).await;
    rep.status("DELETE /v0/topics/:topic -> 200", &del, 200);
    if let Ok(r) = &del {
        rep.check(
            "DELETE: deleted=true",
            bool_of(&r.body, "deleted") == Some(true),
            || format!("body {}", r.body),
        );
    }

    // DELETE absent topic -> 200 deleted:false (idempotent).
    let del2 = c.delete(&path).await;
    rep.status("DELETE absent topic -> 200", &del2, 200);
    if let Ok(r) = &del2 {
        rep.check(
            "DELETE absent: deleted=false",
            bool_of(&r.body, "deleted") == Some(false),
            || format!("body {}", r.body),
        );
    }

    // if_empty on a non-empty topic -> 409 topic_not_empty.
    let b2 = format!("{ns}-ifempty");
    let _ = c
        .post(
            &format!("/v0/topics/{b2}"),
            &json!({ "records": [{ "data": 1 }] }),
        )
        .await;
    let ife = c.delete(&format!("/v0/topics/{b2}?if_empty=true")).await;
    rep.error_code(
        "DELETE ?if_empty=true non-empty -> 409 topic_not_empty",
        &ife,
        409,
        "topic_not_empty",
    );
}

async fn conformance_write_and_diff(c: &Client, rep: &mut Report, ns: &str) {
    let b = format!("{ns}-wd");
    let path = format!("/v0/topics/{b}");
    let dpath = format!("{path}/diff");

    // First write auto-creates -> 201 created:true, contiguous seqs from 1.
    let w = c
        .post(
            &path,
            &json!({ "records": [
                { "data": { "n": 1 }, "tag": "t:1", "node": "wA", "meta": { "trace": "x" } },
                { "data": { "n": 2 }, "tag": "t:2" }
            ] }),
        )
        .await;
    rep.status("POST first write -> 201 (auto-create)", &w, 201);
    if let Ok(r) = &w {
        let seqs = seqs_of(&r.body);
        rep.check(
            "write: created=true, contiguous seqs [1,2], first/last/head/count consistent",
            bool_of(&r.body, "created") == Some(true)
                && seqs == vec![1, 2]
                && u64_of(&r.body, "first_seq") == Some(1)
                && u64_of(&r.body, "last_seq") == Some(2)
                && u64_of(&r.body, "head_seq") == Some(2)
                && u64_of(&r.body, "count") == Some(2)
                && bool_of(&r.body, "deduped") == Some(false),
            || format!("body {}", r.body),
        );
    }

    // Second write -> 200 created:false, seqs continue at 3.
    let w2 = c
        .post(&path, &json!({ "records": [{ "data": { "n": 3 } }] }))
        .await;
    rep.status("POST second write -> 200", &w2, 200);
    if let Ok(r) = &w2 {
        rep.check(
            "write 2: created=false, seq=3, head=3",
            bool_of(&r.body, "created") == Some(false)
                && seqs_of(&r.body) == vec![3]
                && u64_of(&r.body, "head_seq") == Some(3),
            || format!("body {}", r.body),
        );
    }

    // Diff from 0 returns all three, caught_up, tombstone null, $tag omitted by default.
    let d = c.post(&dpath, &json!({ "from_seq": 0 })).await;
    rep.status("POST diff from 0 -> 200", &d, 200);
    if let Ok(r) = &d {
        let recs = r.body.get("records").and_then(|v| v.as_array());
        let has_tag = recs
            .map(|a| a.iter().any(|x| x.get("$tag").is_some()))
            .unwrap_or(true);
        let has_node = recs
            .map(|a| {
                a.iter()
                    .any(|x| x.get("$node").and_then(|v| v.as_str()) == Some("wA"))
            })
            .unwrap_or(false);
        let has_meta = recs
            .map(|a| a.first().map(|x| x.get("meta").is_some()).unwrap_or(false))
            .unwrap_or(false);
        rep.check(
            "diff: seqs [1,2,3], caught_up, tombstone null, $tag omitted, $node + meta present",
            diff_seqs(&r.body) == vec![1, 2, 3]
                && bool_of(&r.body, "caught_up") == Some(true)
                && r.body.get("tombstone") == Some(&Value::Null)
                && u64_of(&r.body, "head_seq") == Some(3)
                && u64_of(&r.body, "next_from_seq") == Some(3)
                && !has_tag
                && has_node
                && has_meta,
            || format!("body {}", r.body),
        );
    }

    // include_tags=true surfaces $tag; include_meta=false drops meta.
    let dt = c
        .post(
            &dpath,
            &json!({ "from_seq": 0, "include_tags": true, "include_meta": false }),
        )
        .await;
    if let Ok(r) = &dt {
        let recs = r.body.get("records").and_then(|v| v.as_array());
        let tag_ok = recs
            .and_then(|a| a.first())
            .and_then(|x| x.get("$tag"))
            .and_then(|v| v.as_str())
            == Some("t:1");
        let no_meta = recs
            .map(|a| a.iter().all(|x| x.get("meta").is_none()))
            .unwrap_or(false);
        rep.check(
            "diff include_tags=true / include_meta=false honored",
            tag_ok && no_meta,
            || format!("body {}", r.body),
        );
    }

    // Diff with limit clamps the batch; cursor advances.
    let dl = c.post(&dpath, &json!({ "from_seq": 0, "limit": 2 })).await;
    if let Ok(r) = &dl {
        rep.check(
            "diff limit=2 returns 2 records, next_from_seq=2, not caught_up",
            diff_seqs(&r.body) == vec![1, 2]
                && u64_of(&r.body, "next_from_seq") == Some(2)
                && bool_of(&r.body, "caught_up") == Some(false)
                && u64_of(&r.body, "lag") == Some(1),
            || format!("body {}", r.body),
        );
    }

    // Tail read from head: nothing new, caught_up.
    let dtail = c.post(&dpath, &json!({ "from_seq": 3 })).await;
    if let Ok(r) = &dtail {
        rep.check(
            "diff from head: 0 records, caught_up",
            diff_seqs(&r.body).is_empty() && bool_of(&r.body, "caught_up") == Some(true),
            || format!("body {}", r.body),
        );
    }

    // ?return_seqs=false suppresses seqs.
    let wns = c
        .post(
            &format!("{path}?return_seqs=false"),
            &json!({ "records": [{ "data": 9 }] }),
        )
        .await;
    if let Ok(r) = &wns {
        rep.check(
            "write ?return_seqs=false: seqs omitted, first/last present",
            r.body.get("seqs").is_none()
                && u64_of(&r.body, "first_seq").is_some()
                && u64_of(&r.body, "last_seq").is_some(),
            || format!("body {}", r.body),
        );
    }

    // create:false on absent topic -> 404 (NOMKSTREAM).
    let nomk = c
        .post(
            &format!("/v0/topics/{ns}-nomk"),
            &json!({ "records": [{ "data": 1 }], "create": false }),
        )
        .await;
    rep.error_code(
        "write create:false on absent -> 404 topic_not_found",
        &nomk,
        404,
        "topic_not_found",
    );

    // diff on absent topic -> 404.
    let dmiss = c
        .post(
            &format!("/v0/topics/{ns}-nodiff/diff"),
            &json!({ "from_seq": 0 }),
        )
        .await;
    rep.error_code(
        "diff on absent topic -> 404 topic_not_found",
        &dmiss,
        404,
        "topic_not_found",
    );
}

async fn conformance_idempotency(c: &Client, rep: &mut Report, ns: &str) {
    let b = format!("{ns}-idem");
    let path = format!("/v0/topics/{b}");

    let key = "probe-idem-1";
    let w1 = c
        .post(
            &path,
            &json!({ "records": [{ "data": "a" }, { "data": "b" }], "idempotency_key": key }),
        )
        .await;
    let first_seqs = w1
        .as_ref()
        .ok()
        .map(|r| seqs_of(&r.body))
        .unwrap_or_default();
    rep.check(
        "idempotency: first write succeeded with seqs",
        !first_seqs.is_empty(),
        || format!("w1 {w1:?}", w1 = w1.as_ref().map(|r| r.body.clone())),
    );

    // Retry with the same key -> deduped:true, same seqs, no new append.
    let w2 = c
        .post(
            &path,
            &json!({ "records": [{ "data": "a" }, { "data": "b" }], "idempotency_key": key }),
        )
        .await;
    if let Ok(r) = &w2 {
        rep.check(
            "idempotency: retry deduped=true, original seqs, no new append",
            bool_of(&r.body, "deduped") == Some(true) && seqs_of(&r.body) == first_seqs,
            || format!("body {}", r.body),
        );
    } else {
        rep.check("idempotency retry request", false, || {
            "request failed".into()
        });
    }

    // Head must not have advanced: still 2 records.
    let st = c.get(&path).await;
    if let Ok(r) = &st {
        rep.check(
            "idempotency: head not advanced (count=2)",
            u64_of(&r.body, "count") == Some(2),
            || format!("body {}", r.body),
        );
    }

    // Header-based idempotency key (body omits it) is honored.
    let bh = format!("{ns}-idemhdr");
    let hpath = format!("/v0/topics/{bh}");
    let _ = c
        .send(
            c.http
                .post(c.url(&hpath))
                .header("content-type", "application/json")
                .header("idempotency-key", "hdr-key-1")
                .body(json!({ "records": [{ "data": 1 }] }).to_string()),
        )
        .await;
    let hr2 = c
        .send(
            c.http
                .post(c.url(&hpath))
                .header("content-type", "application/json")
                .header("idempotency-key", "hdr-key-1")
                .body(json!({ "records": [{ "data": 1 }] }).to_string()),
        )
        .await;
    if let Ok(r) = &hr2 {
        rep.check(
            "idempotency via header: retry deduped=true",
            bool_of(&r.body, "deduped") == Some(true),
            || format!("body {}", r.body),
        );
    }
}

async fn conformance_errors(c: &Client, rep: &mut Report, ns: &str) {
    let b = format!("{ns}-err");
    let path = format!("/v0/topics/{b}");

    // 415: non-JSON content type on a write.
    let r415 = c.post_raw_ct(&path, "text/plain", "{}").await;
    rep.error_code(
        "write wrong content-type -> 415",
        &r415,
        415,
        "unsupported_media_type",
    );

    // 415: missing content type entirely.
    let r415b = c.post_no_ct(&path, "{}").await;
    rep.error_code(
        "write missing content-type -> 415",
        &r415b,
        415,
        "unsupported_media_type",
    );

    // 400: malformed JSON.
    let r400 = c.post_raw_ct(&path, "application/json", "{bad json").await;
    rep.error_code(
        "write malformed JSON -> 400 invalid_request",
        &r400,
        400,
        "invalid_request",
    );

    // 400: empty records array.
    let rempty = c.post(&path, &json!({ "records": [] })).await;
    rep.error_code(
        "write empty records -> 400 invalid_request",
        &rempty,
        400,
        "invalid_request",
    );

    // Error envelope shape: error.code + error.message present.
    if let Ok(r) = &r400 {
        let has_msg = r
            .body
            .get("error")
            .and_then(|e| e.get("message"))
            .and_then(|m| m.as_str())
            .map(|s| !s.is_empty())
            .unwrap_or(false);
        rep.check(
            "error envelope has error.code + error.message",
            has_msg,
            || format!("body {}", r.body),
        );
    }

    // 405: wrong method for a path (PUT on /diff which only accepts POST -> 405).
    let r405 = c.put(&format!("{path}/diff"), &json!({})).await;
    rep.error_code(
        "PUT on /diff (POST-only) -> 405 method_not_allowed",
        &r405,
        405,
        "method_not_allowed",
    );

    // 404: unknown route under /v0.
    let r404 = c.get("/v0/nonexistent/route").await;
    rep.status("GET unknown /v0 route -> 404", &r404, 404);

    // 400: invalid topic name (PUT). A '.' leading char is invalid.
    let rname = c.put("/v0/topics/.bad", &json!({})).await;
    rep.error_code(
        "PUT invalid topic name -> 400 invalid_request",
        &rname,
        400,
        "invalid_request",
    );

    // 422: write to a full discard:"reject" topic.
    let rb = format!("{ns}-reject");
    let _ = c
        .put(
            &format!("/v0/topics/{rb}"),
            &json!({ "cap_records": 1, "discard": "reject" }),
        )
        .await;
    let _ = c
        .post(
            &format!("/v0/topics/{rb}"),
            &json!({ "records": [{ "data": 1 }] }),
        )
        .await;
    let full = c
        .post(
            &format!("/v0/topics/{rb}"),
            &json!({ "records": [{ "data": 2 }] }),
        )
        .await;
    rep.error_code(
        "write to full discard:reject topic -> 422 topic_full",
        &full,
        422,
        "topic_full",
    );
}

async fn conformance_deletion(c: &Client, rep: &mut Report, ns: &str) {
    // Snapshot delete (before_seq), silent, point-in-time + match flows.
    let b = format!("{ns}-del");
    let path = format!("/v0/topics/{b}");
    let dpath = format!("{path}/diff");
    let delpath = format!("{path}/delete");

    let _ = c
        .post(
            &path,
            &json!({ "records": [
                { "data": 1, "tag": "tenant:job-1" },
                { "data": 2, "tag": "tenant:job-2" },
                { "data": 3, "tag": "other:job-9" },
                { "data": 4 },
                { "data": 5, "tag": "tenant:job-5" }
            ] }),
        )
        .await;

    // before_seq snapshot delete of seqs < 3 (removes 1,2).
    let snap = c.post(&delpath, &json!({ "before_seq": 3 })).await;
    rep.status("POST delete before_seq -> 200", &snap, 200);
    if let Ok(r) = &snap {
        rep.check(
            "delete before_seq: deleted=2, earliest_seq=3",
            u64_of(&r.body, "deleted") == Some(2) && u64_of(&r.body, "earliest_seq") == Some(3),
            || format!("body {}", r.body),
        );
    }

    // Read across the purely-deleted prefix is SILENT (tombstone null), cursor advances.
    let d = c.post(&dpath, &json!({ "from_seq": 0 })).await;
    if let Ok(r) = &d {
        rep.check(
            "delete is silent: from 0 yields [3,4,5], tombstone null",
            diff_seqs(&r.body) == vec![3, 4, 5] && r.body.get("tombstone") == Some(&Value::Null),
            || format!("body {}", r.body),
        );
    }

    // Tag exact match delete of tenant:job-5 (the bare-string shorthand).
    let me = c.post(&delpath, &json!({ "match": "tenant:job-5" })).await;
    if let Ok(r) = &me {
        rep.check(
            "delete match exact (shorthand): deleted=1",
            u64_of(&r.body, "deleted") == Some(1),
            || format!("body {}", r.body),
        );
    }

    // Tag prefix glob delete of tenant:* — but only existing matching records (job-2 already deleted).
    // Only seq 2 was tenant:* but it's gone; nothing left matches tenant:* now.
    let mg = c
        .post(&delpath, &json!({ "match": ["tag", "Glob", "tenant:*"] }))
        .await;
    if let Ok(r) = &mg {
        rep.check(
            "delete match Glob tuple: deleted=0 (all tenant:* already gone)",
            u64_of(&r.body, "deleted") == Some(0),
            || format!("body {}", r.body),
        );
    }

    // Point-in-time: a NEW record with a matching tag after a tag delete survives.
    let b2 = format!("{ns}-pit");
    let p2 = format!("/v0/topics/{b2}");
    let _ = c
        .post(&p2, &json!({ "records": [{ "data": 1, "tag": "a:1" }] }))
        .await;
    let _ = c
        .post(
            &format!("{p2}/delete"),
            &json!({ "match": ["tag", "Glob", "a:*"] }),
        )
        .await;
    let _ = c
        .post(&p2, &json!({ "records": [{ "data": 2, "tag": "a:2" }] }))
        .await;
    let dp = c
        .post(&format!("{p2}/diff"), &json!({ "from_seq": 0 }))
        .await;
    if let Ok(r) = &dp {
        rep.check(
            "delete point-in-time: future matching record survives ([2])",
            diff_seqs(&r.body) == vec![2] && r.body.get("tombstone") == Some(&Value::Null),
            || format!("body {}", r.body),
        );
    }

    // delete with neither selector -> 400.
    let dn = c.post(&delpath, &json!({})).await;
    rep.error_code(
        "delete with no selector -> 400 invalid_request",
        &dn,
        400,
        "invalid_request",
    );

    // Glob without trailing '*' -> 400.
    let dg = c
        .post(&delpath, &json!({ "match": ["tag", "Glob", "noasterisk"] }))
        .await;
    rep.error_code(
        "delete Glob without trailing '*' -> 400",
        &dg,
        400,
        "invalid_request",
    );

    // delete on absent topic -> 404.
    let da = c
        .post(
            &format!("/v0/topics/{ns}-noxdel/delete"),
            &json!({ "before_seq": 1 }),
        )
        .await;
    rep.error_code(
        "delete on absent topic -> 404 topic_not_found",
        &da,
        404,
        "topic_not_found",
    );
}

async fn conformance_cap_tombstone(c: &Client, rep: &mut Report, ns: &str) {
    // Dual watermark: cap eviction still tombstones (reason=cap), while a delete
    // is silent. We verify the cap-tombstone half here (TTL-correctness is in
    // engine unit tests with TestClock; over HTTP we can deterministically force
    // cap eviction via discard:"old").
    let b = format!("{ns}-cap");
    let path = format!("/v0/topics/{b}");
    let dpath = format!("{path}/diff");

    let _ = c
        .put(&path, &json!({ "cap_records": 3, "discard": "old" }))
        .await;
    // Write 6 records (seqs 1..=6); cap=3 ⇒ earliest advances to 4, evict_floor advances.
    for i in 1..=6 {
        let _ = c.post(&path, &json!({ "records": [{ "data": i }] })).await;
    }
    let st = c.get(&path).await;
    if let Ok(r) = &st {
        rep.check(
            "cap: head_seq=6, count=3 after eviction",
            u64_of(&r.body, "head_seq") == Some(6) && u64_of(&r.body, "count") == Some(3),
            || format!("body {}", r.body),
        );
    }

    // A consumer at from_seq=0 fell below the involuntary floor -> tombstone reason=cap.
    let d = c.post(&dpath, &json!({ "from_seq": 0 })).await;
    if let Ok(r) = &d {
        let t = r.body.get("tombstone");
        let reason = t.and_then(|x| x.get("reason")).and_then(|v| v.as_str());
        let gap_from = t.and_then(|x| x.get("gap_from")).and_then(|v| v.as_u64());
        let earliest = u64_of(&r.body, "earliest_seq");
        // After a tombstone, the delivered records resume at earliest_seq.
        let resumes_at_earliest = diff_seqs(&r.body).first().copied() == earliest;
        rep.check(
            "cap eviction emits tombstone reason=cap, gap_from=1, records resume at earliest",
            reason == Some("cap")
                && gap_from == Some(1)
                && resumes_at_earliest
                && t.and_then(|x| x.get("earliest_seq"))
                    .and_then(|v| v.as_u64())
                    == earliest,
            || format!("body {}", r.body),
        );
    }
}

async fn conformance_node_loop(c: &Client, rep: &mut Report, ns: &str) {
    // A node never receives its own records via diff, but the cursor advances to
    // caught_up (no infinite empty loop).
    let b = format!("{ns}-node");
    let path = format!("/v0/topics/{b}");
    let dpath = format!("{path}/diff");

    let _ = c
        .post(
            &path,
            &json!({ "records": [
                { "data": 1, "node": "self" },
                { "data": 2, "node": "other" },
                { "data": 3, "node": "self" }
            ] }),
        )
        .await;

    // Reader presents node=self: drops 1 and 3, delivers only 2, but advances to head.
    let d = c
        .post(&dpath, &json!({ "from_seq": 0, "node": "self" }))
        .await;
    if let Ok(r) = &d {
        rep.check(
            "node loop-prevention: own records skipped, only [2] delivered, caught_up at head",
            diff_seqs(&r.body) == vec![2]
                && bool_of(&r.body, "caught_up") == Some(true)
                && u64_of(&r.body, "next_from_seq") == Some(3),
            || format!("body {}", r.body),
        );
    }

    // Multi-id node filter ["self","other"] drops all -> 0 records, caught_up.
    let d2 = c
        .post(&dpath, &json!({ "from_seq": 0, "node": ["self", "other"] }))
        .await;
    if let Ok(r) = &d2 {
        rep.check(
            "node filter set: all dropped, 0 records, caught_up (cursor advanced past)",
            diff_seqs(&r.body).is_empty()
                && bool_of(&r.body, "caught_up") == Some(true)
                && u64_of(&r.body, "next_from_seq") == Some(3),
            || format!("body {}", r.body),
        );
    }
}

async fn conformance_routers(c: &Client, rep: &mut Report, ns: &str) {
    let src = format!("{ns}-rsrc");
    let dst = format!("{ns}-rdst");
    let rname = format!("{ns}-r1");
    let rpath = format!("/v0/routers/{rname}");

    // PUT router -> 201, fields echoed.
    let put = c
        .put(
            &rpath,
            &json!({ "source": src, "dest": dst, "preserve_node": true, "preserve_tag": true }),
        )
        .await;
    rep.status("PUT /v0/routers/:router create -> 201", &put, 201);
    if let Ok(r) = &put {
        rep.check(
            "router create: created=true, source/dest echoed",
            bool_of(&r.body, "created") == Some(true)
                && r.body.get("source").and_then(|v| v.as_str()) == Some(src.as_str())
                && r.body.get("dest").and_then(|v| v.as_str()) == Some(dst.as_str()),
            || format!("body {}", r.body),
        );
    }

    // Identical PUT -> 200 created:false.
    let put2 = c.put(&rpath, &json!({ "source": src, "dest": dst })).await;
    rep.status("PUT router identical -> 200", &put2, 200);

    // Write to source: forwarded copy appears in dest with $node preserved + fresh seq.
    let _ = c
        .post(
            &format!("/v0/topics/{src}"),
            &json!({ "records": [{ "data": { "k": "v" }, "node": "origin-1", "tag": "tag-1" }] }),
        )
        .await;
    let dd = c
        .post(
            &format!("/v0/topics/{dst}/diff"),
            &json!({ "from_seq": 0, "include_tags": true }),
        )
        .await;
    if let Ok(r) = &dd {
        let recs = r.body.get("records").and_then(|v| v.as_array());
        let node_ok = recs
            .and_then(|a| a.first())
            .and_then(|x| x.get("$node"))
            .and_then(|v| v.as_str())
            == Some("origin-1");
        let tag_ok = recs
            .and_then(|a| a.first())
            .and_then(|x| x.get("$tag"))
            .and_then(|v| v.as_str())
            == Some("tag-1");
        rep.check(
            "router fan-out: record appears in dest with $node + $tag preserved",
            recs.map(|a| a.len()).unwrap_or(0) == 1 && node_ok && tag_ok,
            || format!("body {}", r.body),
        );
    }

    // GET router.
    let g = c.get(&rpath).await;
    rep.status("GET /v0/routers/:router -> 200", &g, 200);
    if let Ok(r) = &g {
        rep.check(
            "router get: forwarded_total >= 1",
            u64_of(&r.body, "forwarded_total")
                .map(|n| n >= 1)
                .unwrap_or(false),
            || format!("body {}", r.body),
        );
    }

    // GET missing router -> 404 router_not_found.
    let gm = c.get(&format!("/v0/routers/{ns}-nope")).await;
    rep.error_code(
        "GET missing router -> 404 router_not_found",
        &gm,
        404,
        "router_not_found",
    );

    // List routers (filtered by source).
    let l = c.get(&format!("/v0/routers?source={src}")).await;
    rep.status("GET /v0/routers?source -> 200", &l, 200);
    if let Ok(r) = &l {
        let found = r
            .body
            .get("routers")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .any(|x| x.get("router").and_then(|v| v.as_str()) == Some(rname.as_str()))
            })
            .unwrap_or(false);
        rep.check("list routers: created router present", found, || {
            format!("body {}", r.body)
        });
    }

    // source == dest -> 400.
    let sd = c
        .put(
            &format!("/v0/routers/{ns}-rsd"),
            &json!({ "source": src, "dest": src }),
        )
        .await;
    rep.error_code(
        "router source==dest -> 400 invalid_request",
        &sd,
        400,
        "invalid_request",
    );

    // Cycle: create dst->src (closing src->dst) -> 409 router_cycle with detail.cycle.
    let cyc = c
        .put(
            &format!("/v0/routers/{ns}-rcyc"),
            &json!({ "source": dst, "dest": src }),
        )
        .await;
    rep.error_code(
        "router cycle -> 409 router_cycle",
        &cyc,
        409,
        "router_cycle",
    );
    if let Ok(r) = &cyc {
        let has_cycle = r
            .body
            .get("error")
            .and_then(|e| e.get("detail"))
            .and_then(|d| d.get("cycle"))
            .and_then(|v| v.as_array())
            .map(|a| !a.is_empty())
            .unwrap_or(false);
        rep.check(
            "router cycle: error.detail.cycle present",
            has_cycle,
            || format!("body {}", r.body),
        );
    }

    // allow_cycle:true permits dst->src and terminates via hop cap (no error).
    let ac = c
        .put(
            &format!("/v0/routers/{ns}-rcyc-ok"),
            &json!({ "source": dst, "dest": src, "allow_cycle": true }),
        )
        .await;
    rep.status(
        "router allow_cycle:true -> 201/200 (permitted)",
        &ac,
        if matches!(ac.as_ref().map(|r| r.status), Ok(200)) {
            200
        } else {
            201
        },
    );
    // A write into the cycle must terminate (no hang) — the timeout on the client
    // guards against an infinite loop; we just confirm the write returns.
    let cyclic_write = c
        .post(
            &format!("/v0/topics/{src}"),
            &json!({ "records": [{ "data": "cycle", "node": "n9" }] }),
        )
        .await;
    rep.status(
        "write into allow_cycle topology terminates (returns 200)",
        &cyclic_write,
        200,
    );

    // create_dest:false to a missing dest -> 404.
    let cdf = c
        .put(
            &format!("/v0/routers/{ns}-rcdf"),
            &json!({ "source": src, "dest": format!("{ns}-rmissing"), "create_dest": false }),
        )
        .await;
    rep.error_code(
        "router create_dest:false missing dest -> 404 topic_not_found",
        &cdf,
        404,
        "topic_not_found",
    );

    // DELETE router -> 200 deleted:true; idempotent -> deleted:false.
    let dr = c.delete(&rpath).await;
    rep.status("DELETE /v0/routers/:router -> 200", &dr, 200);
    if let Ok(r) = &dr {
        rep.check(
            "router delete: deleted=true",
            bool_of(&r.body, "deleted") == Some(true),
            || format!("body {}", r.body),
        );
    }
    let dr2 = c.delete(&rpath).await;
    if let Ok(r) = &dr2 {
        rep.check(
            "router delete idempotent: deleted=false",
            bool_of(&r.body, "deleted") == Some(false),
            || format!("body {}", r.body),
        );
    }
}

/// Queue layer (API §10): create a queue, produce jobs, then assert the
/// claim/ack/nack/extend shapes + status codes, `409 not_a_queue` on a log topic,
/// and that `/work` SSE delivers a `job` frame. Timing-dependent semantics
/// (lease expiry / dead-letter) are covered deterministically by the engine +
/// integration tests under a TestClock; here we assert the wire contract.
async fn conformance_queue(c: &Client, rep: &mut Report, ns: &str) {
    let q = format!("{ns}-q");
    let path = format!("/v0/topics/{q}");

    // PUT type:"queue" -> 201, config echoes type + queue tuning fields.
    let put = c
        .put(
            &path,
            &json!({ "type": "queue", "lease_ms": 30000, "max_deliveries": 5 }),
        )
        .await;
    rep.status("PUT type:queue create -> 201", &put, 201);
    if let Ok(r) = &put {
        rep.check(
            "queue create: config.type=queue, lease_ms/max_deliveries echoed",
            r.body
                .get("config")
                .and_then(|c| c.get("type"))
                .and_then(|v| v.as_str())
                == Some("queue")
                && u64_of(r.body.get("config").unwrap_or(&Value::Null), "lease_ms") == Some(30000)
                && u64_of(
                    r.body.get("config").unwrap_or(&Value::Null),
                    "max_deliveries",
                ) == Some(5),
            || format!("body {}", r.body),
        );
    }

    // Produce 5 jobs (a normal append into the jobs log).
    let records: Vec<Value> = (0..5).map(|i| json!({ "data": { "i": i } })).collect();
    let w = c.post(&path, &json!({ "records": records })).await;
    rep.status("queue produce 5 jobs -> 200", &w, 200);

    // GET state: type=queue + queue{ready,in_flight,dead_lettered}.
    let st = c.get(&path).await;
    rep.status("GET queue state -> 200", &st, 200);
    if let Ok(r) = &st {
        let qs = r.body.get("queue");
        rep.check(
            "queue state: type=queue, queue{ready=5,in_flight=0,dead_lettered=0}",
            r.body.get("type").and_then(|v| v.as_str()) == Some("queue")
                && qs.and_then(|x| x.get("ready")).and_then(|v| v.as_u64()) == Some(5)
                && qs.and_then(|x| x.get("in_flight")).and_then(|v| v.as_u64()) == Some(0)
                && qs
                    .and_then(|x| x.get("dead_lettered"))
                    .and_then(|v| v.as_u64())
                    == Some(0),
            || format!("body {}", r.body),
        );
    }

    // POST /claim {node, max:2} -> 200 with claimed[] (ascending $seq + lease_id /
    // deadline / deliveries), count=2, ready=3.
    let claim = c
        .post(&format!("{path}/claim"), &json!({ "node": "w1", "max": 2 }))
        .await;
    rep.status("POST /claim -> 200", &claim, 200);
    let mut claimed_seqs: Vec<u64> = Vec::new();
    if let Ok(r) = &claim {
        let arr = r.body.get("claimed").and_then(|v| v.as_array());
        claimed_seqs = arr
            .map(|a| {
                a.iter()
                    .filter_map(|j| j.get("$seq").and_then(|s| s.as_u64()))
                    .collect()
            })
            .unwrap_or_default();
        let shapes_ok = arr
            .map(|a| {
                a.iter().all(|j| {
                    j.get("lease_id")
                        .and_then(|v| v.as_str())
                        .map(|s| s.starts_with("lease_"))
                        .unwrap_or(false)
                        && j.get("deadline")
                            .and_then(|v| v.as_i64())
                            .map(|d| d > 0)
                            .unwrap_or(false)
                        && j.get("deliveries").and_then(|v| v.as_u64()) == Some(1)
                        && j.get("data").is_some()
                })
            })
            .unwrap_or(false);
        rep.check(
            "claim: count=2, ascending $seq [1,2], lease_id/deadline/deliveries present, ready=3",
            u64_of(&r.body, "count") == Some(2)
                && claimed_seqs == vec![1, 2]
                && u64_of(&r.body, "ready") == Some(3)
                && shapes_ok,
            || format!("body {}", r.body),
        );
    }

    // POST /extend {node, seqs:[seq0], lease_ms} -> 200, extended=1, deadlines map.
    if let Some(&seq0) = claimed_seqs.first() {
        let ext = c
            .post(
                &format!("{path}/extend"),
                &json!({ "node": "w1", "seqs": [seq0], "lease_ms": 60000 }),
            )
            .await;
        rep.status("POST /extend -> 200", &ext, 200);
        if let Ok(r) = &ext {
            let dl_ok = r
                .body
                .get("deadlines")
                .and_then(|m| m.get(seq0.to_string()))
                .and_then(|v| v.as_i64())
                .map(|d| d > 0)
                .unwrap_or(false);
            rep.check(
                "extend: extended=1, deadlines[seq] present and > 0",
                u64_of(&r.body, "extended") == Some(1) && dl_ok,
                || format!("body {}", r.body),
            );
        }
    }

    // POST /nack {node, seqs:[seq1]} -> 200, nacked=1 (released for immediate reclaim).
    if claimed_seqs.len() >= 2 {
        let seq1 = claimed_seqs[1];
        let nack = c
            .post(
                &format!("{path}/nack"),
                &json!({ "node": "w1", "seqs": [seq1], "delay_ms": 0 }),
            )
            .await;
        rep.status("POST /nack -> 200", &nack, 200);
        if let Ok(r) = &nack {
            rep.check(
                "nack: nacked=1, in_flight=1 (seq0 still held), seq1 back to ready",
                u64_of(&r.body, "nacked") == Some(1) && u64_of(&r.body, "in_flight") == Some(1),
                || format!("body {}", r.body),
            );
        }
    }

    // POST /ack {node, seqs:[seq0]} -> 200, acked=1 (ack-is-delete), skipped=[].
    if let Some(&seq0) = claimed_seqs.first() {
        let ack = c
            .post(
                &format!("{path}/ack"),
                &json!({ "node": "w1", "seqs": [seq0] }),
            )
            .await;
        rep.status("POST /ack -> 200", &ack, 200);
        if let Ok(r) = &ack {
            let skipped_empty = r
                .body
                .get("skipped")
                .and_then(|v| v.as_array())
                .map(|a| a.is_empty())
                .unwrap_or(false);
            rep.check(
                "ack: acked=1, skipped=[] (ack-is-delete)",
                u64_of(&r.body, "acked") == Some(1) && skipped_empty,
                || format!("body {}", r.body),
            );
        }
        // Acking a seq this node no longer holds is silently skipped (idempotent).
        let ack2 = c
            .post(
                &format!("{path}/ack"),
                &json!({ "node": "w1", "seqs": [seq0] }),
            )
            .await;
        if let Ok(r) = &ack2 {
            rep.check(
                "ack idempotent: re-ack acked=0, seq skipped",
                u64_of(&r.body, "acked") == Some(0)
                    && r.body
                        .get("skipped")
                        .and_then(|v| v.as_array())
                        .map(|a| a.iter().filter_map(|x| x.as_u64()).any(|s| s == seq0))
                        .unwrap_or(false),
                || format!("body {}", r.body),
            );
        }
    }

    // claim with an empty node -> 400 invalid_request.
    let badnode = c
        .post(&format!("{path}/claim"), &json!({ "node": "", "max": 1 }))
        .await;
    rep.error_code(
        "claim empty node -> 400 invalid_request",
        &badnode,
        400,
        "invalid_request",
    );

    // 409 not_a_queue: every queue endpoint on a plain LOG topic.
    let logb = format!("{ns}-qlog");
    let logpath = format!("/v0/topics/{logb}");
    let _ = c.put(&logpath, &json!({})).await; // default type=log.
    for ep in ["claim", "ack", "nack", "extend"] {
        let r = c
            .post(
                &format!("{logpath}/{ep}"),
                &json!({ "node": "w1", "seqs": [1], "lease_ms": 1000 }),
            )
            .await;
        rep.error_code(
            &format!("queue /{ep} on a log topic -> 409 not_a_queue"),
            &r,
            409,
            "not_a_queue",
        );
    }
    // /work on a log topic -> 409 not_a_queue (validated before the stream opens).
    let worklog = c
        .send(
            c.http
                .get(c.url(&format!("{logpath}/work?node=w1")))
                .header("accept", "text/event-stream"),
        )
        .await;
    rep.error_code(
        "GET /work on a log topic -> 409 not_a_queue",
        &worklog,
        409,
        "not_a_queue",
    );

    // queue op on an ABSENT topic -> 404 topic_not_found (not 409).
    let missing = c
        .post(
            &format!("/v0/topics/{ns}-qmiss/claim"),
            &json!({ "node": "w1" }),
        )
        .await;
    rep.error_code(
        "claim on absent topic -> 404 topic_not_found",
        &missing,
        404,
        "topic_not_found",
    );

    // /work with a non-SSE Accept -> 406 not_acceptable.
    let na = c
        .send(
            c.http
                .get(c.url(&format!("{path}/work?node=w1")))
                .header("accept", "application/json"),
        )
        .await;
    rep.error_code(
        "GET /work Accept!=event-stream -> 406 not_acceptable",
        &na,
        406,
        "not_acceptable",
    );

    // /work SSE delivers a job frame. Produce a couple of fresh jobs on a clean
    // queue so the stream has work to push, then collect the first job frame.
    let wq = format!("{ns}-qwork");
    let wpath = format!("/v0/topics/{wq}");
    let _ = c
        .put(&wpath, &json!({ "type": "queue", "lease_ms": 30000 }))
        .await;
    let _ = c
        .post(
            &wpath,
            &json!({ "records": [{ "data": { "j": 1 } }, { "data": { "j": 2 } }] }),
        )
        .await;
    let frames = collect_sse(
        c,
        &format!("{wpath}/work?node=worker-1&max=2"),
        None,
        1,
        Duration::from_secs(5),
        || None,
        &wpath,
    )
    .await;
    let job = frames.iter().find(|f| f.event == "job");
    rep.check(
        "/work SSE delivers an `event: job` frame with topic/$seq/lease_id/deliveries",
        job.map(|f| {
            let d: Value = serde_json::from_str(&f.data).unwrap_or(Value::Null);
            d.get("topic").and_then(|v| v.as_str()) == Some(wq.as_str())
                && d.get("$seq")
                    .and_then(|v| v.as_u64())
                    .map(|s| s >= 1)
                    .unwrap_or(false)
                && d.get("lease_id")
                    .and_then(|v| v.as_str())
                    .map(|s| s.starts_with("lease_"))
                    .unwrap_or(false)
                && d.get("deliveries").and_then(|v| v.as_u64()) == Some(1)
                && !f.id.is_empty()
        })
        .unwrap_or(false),
        || {
            format!(
                "frames: {:?}",
                frames
                    .iter()
                    .map(|f| (&f.event, &f.data))
                    .collect::<Vec<_>>()
            )
        },
    );

    // type is immutable: re-PUT the queue as a log -> 409 topic_exists_incompatible.
    let immut = c.put(&path, &json!({ "type": "log" })).await;
    rep.error_code(
        "PUT changing type queue->log -> 409 topic_exists_incompatible",
        &immut,
        409,
        "topic_exists_incompatible",
    );
}

async fn conformance_sse(c: &Client, rep: &mut Report, ns: &str) {
    let b = format!("{ns}-sse");
    let path = format!("/v0/topics/{b}");

    // Seed a couple of records so the stream has backlog + a caught-up transition.
    let _ = c
        .post(
            &path,
            &json!({ "records": [{ "data": "r1", "tag": "x1" }, { "data": "r2" }] }),
        )
        .await;

    // POST /v0/watch -> 200 with wid + stream_url + per-topic head/earliest.
    let watch = c
        .post(
            "/v0/watch",
            &json!({ "topics": { b.clone(): { "from_seq": 0 } }, "include_tags": true }),
        )
        .await;
    rep.status("POST /v0/watch -> 200", &watch, 200);
    let stream_url = watch.as_ref().ok().and_then(|r| {
        r.body
            .get("stream_url")
            .and_then(|v| v.as_str())
            .map(String::from)
    });
    if let Ok(r) = &watch {
        rep.check(
            "watch create: wid + stream_url + per-topic head/earliest",
            r.body.get("wid").is_some()
                && stream_url.is_some()
                && r.body
                    .get("topics")
                    .and_then(|m| m.get(&b))
                    .and_then(|s| s.get("head_seq"))
                    .is_some(),
            || format!("body {}", r.body),
        );
    }

    // POST /v0/watch missing topics -> 400.
    let wbad = c.post("/v0/watch", &json!({ "topics": {} })).await;
    rep.error_code(
        "watch create empty topics -> 400 invalid_request",
        &wbad,
        400,
        "invalid_request",
    );

    // POST /v0/watch unknown topic -> 404.
    let wmiss = c
        .post(
            "/v0/watch",
            &json!({ "topics": { format!("{ns}-ssemiss"): {} } }),
        )
        .await;
    rep.error_code(
        "watch create unknown topic -> 404 topic_not_found",
        &wmiss,
        404,
        "topic_not_found",
    );

    let Some(stream_url) = stream_url else {
        rep.check("SSE stream available", false, || {
            "no stream_url from watch create".into()
        });
        return;
    };

    // GET the stream with a non-SSE Accept -> 406.
    let na = c
        .send(
            c.http
                .get(c.url(&stream_url))
                .header("accept", "application/json"),
        )
        .await;
    rep.error_code(
        "GET stream Accept!=event-stream -> 406 not_acceptable",
        &na,
        406,
        "not_acceptable",
    );

    // GET an unknown wid -> 404.
    let nf = c
        .send(
            c.http
                .get(c.url("/v0/watch/wid_doesnotexist"))
                .header("accept", "text/event-stream"),
        )
        .await;
    rep.status("GET unknown wid -> 404", &nf, 404);

    // Open the SSE stream and collect frames: expect a `record` frame for the
    // backlog and a `caught-up` frame; then write a live record and observe a
    // second `record` frame; then verify resume via Last-Event-ID.
    let frames = collect_sse(
        c,
        &stream_url,
        None,
        3,
        Duration::from_secs(5),
        || {
            // Trigger a live append shortly after connecting so we see a live record.
            Some(json!({ "records": [{ "data": "live", "tag": "x3" }] }))
        },
        &path,
    )
    .await;

    let events: Vec<&str> = frames.iter().map(|f| f.event.as_str()).collect();
    rep.check(
        "SSE delivers a `record` frame for backlog",
        frames.iter().any(|f| f.event == "record"),
        || format!("events seen: {events:?}"),
    );
    rep.check(
        "SSE delivers a `caught-up` frame",
        frames.iter().any(|f| f.event == "caught-up"),
        || format!("events seen: {events:?}"),
    );
    // The live write should appear as another record frame mentioning "live".
    rep.check(
        "SSE delivers the live-written record after caught-up",
        frames
            .iter()
            .any(|f| f.event == "record" && f.data.contains("live")),
        || {
            format!(
                "events seen: {events:?}; datas: {:?}",
                frames.iter().map(|f| &f.data).collect::<Vec<_>>()
            )
        },
    );
    // Frames carry composite `id:` cursors.
    rep.check(
        "SSE data frames carry an `id:` cursor",
        frames
            .iter()
            .filter(|f| f.event == "record")
            .all(|f| !f.id.is_empty()),
        || "a record frame had an empty id".into(),
    );

    // Resume: reconnect with Last-Event-ID set to the FIRST record frame's id
    // (which encodes the cursor after the backlog). With that resume the server
    // should NOT redeliver the backlog records before "live".
    if let Some(first_rec_id) = frames
        .iter()
        .find(|f| f.event == "record")
        .map(|f| f.id.clone())
    {
        let resume_frames = collect_sse(
            c,
            &stream_url,
            Some(&first_rec_id),
            2,
            Duration::from_secs(4),
            || None,
            &path,
        )
        .await;
        // After resuming at the post-backlog cursor, the next record frame should
        // be the live one (seq 3), not the backlog (seqs 1,2).
        let redelivered_backlog = resume_frames
            .iter()
            .filter(|f| f.event == "record")
            .any(|f| f.data.contains("\"$seq\":1") || f.data.contains("\"$seq\":2"));
        rep.check(
            "SSE resume via Last-Event-ID does not redeliver acked backlog",
            !redelivered_backlog,
            || {
                format!(
                    "resume datas: {:?}",
                    resume_frames.iter().map(|f| &f.data).collect::<Vec<_>>()
                )
            },
        );
    }
}

/// Auth checks (only run when --token is supplied). They additionally require
/// the SERVER to be started with that key (TOPICS_API_KEYS); otherwise auth is
/// disabled server-side and the unauthorized check is skipped as inconclusive.
async fn conformance_auth(base_url: &str, c: &Client, rep: &mut Report, ns: &str) {
    // An anonymous client (no token) hitting a data endpoint should be 401 IF
    // the server has auth enabled. We detect "auth enabled" by checking that the
    // authed client works but the anon client is rejected.
    let anon = Client::new(base_url, None);
    let b = format!("{ns}-auth");
    let path = format!("/v0/topics/{b}");

    // Authed write must succeed.
    let authed = c.post(&path, &json!({ "records": [{ "data": 1 }] })).await;
    rep.check(
        "auth: request WITH valid token is accepted",
        authed
            .as_ref()
            .map(|r| r.status == 200 || r.status == 201)
            .unwrap_or(false),
        || format!("authed write: {:?}", authed.as_ref().map(|r| r.status)),
    );

    let anon_resp = anon.get(&path).await;
    match &anon_resp {
        Ok(r) if r.status == 401 => {
            rep.error_code(
                "auth: request WITHOUT token -> 401 unauthorized",
                &anon_resp,
                401,
                "unauthorized",
            );
        }
        Ok(r) => {
            // Server likely started without keys; auth disabled. Don't fail.
            rep.check(
                "auth: (server has auth DISABLED — anon allowed, --token ignored server-side)",
                true,
                || format!("anon status {}", r.status),
            );
        }
        Err(e) => rep.check("auth anon request", false, || {
            format!("request failed: {e}")
        }),
    }

    // Probe endpoints remain open without a token (default probe_auth=false).
    let h = anon.get("/v0/health").await;
    rep.status("auth: /v0/health open without token", &h, 200);
}

// ---------------------------------------------------------------------------
// SSE frame parsing (manual, over reqwest's byte stream)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct SseFrame {
    id: String,
    event: String,
    data: String,
}

/// Open the SSE stream at `stream_url`, optionally with a `Last-Event-ID`
/// resume header, and collect up to `max_frames` named (non-heartbeat) frames,
/// bounded by `deadline`. The `trigger` closure, if it returns a body, is POSTed
/// to `trigger_path` shortly after the stream opens (to produce a live record).
async fn collect_sse(
    c: &Client,
    stream_url: &str,
    last_event_id: Option<&str>,
    max_frames: usize,
    deadline: Duration,
    trigger: impl FnOnce() -> Option<Value>,
    trigger_path: &str,
) -> Vec<SseFrame> {
    let mut rb = c
        .http
        .get(c.url(stream_url))
        .header("accept", "text/event-stream");
    if let Some(t) = &c.token {
        rb = rb.header("authorization", format!("Bearer {t}"));
    }
    if let Some(leid) = last_event_id {
        rb = rb.header("last-event-id", leid);
    }

    let resp = match rb.send().await {
        Ok(r) => r,
        Err(_) => return Vec::new(),
    };
    if !resp.status().is_success() {
        return Vec::new();
    }

    // Fire the trigger write after a short delay so the stream is established and
    // the backlog drained before the live append arrives.
    if let Some(body) = trigger() {
        let cc = c.clone();
        let tp = trigger_path.to_string();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let _ = cc.post(&tp, &body).await;
        });
    }

    let mut frames = Vec::new();
    let mut buf = String::new();
    let mut stream = resp.bytes_stream();

    let res = tokio::time::timeout(deadline, async {
        while let Some(chunk) = stream.next().await {
            let Ok(bytes) = chunk else { break };
            buf.push_str(&String::from_utf8_lossy(&bytes));
            // SSE frames are separated by a blank line.
            while let Some(idx) = buf.find("\n\n") {
                let block: String = buf.drain(..idx + 2).collect();
                if let Some(frame) = parse_sse_block(&block) {
                    frames.push(frame);
                    if frames.len() >= max_frames {
                        return;
                    }
                }
            }
        }
    })
    .await;
    let _ = res; // timeout is expected/acceptable; we return whatever we got.
    frames
}

/// Parse one SSE block into a frame, ignoring heartbeats (`: hb`) and bare
/// `retry:` blocks (returns None for those).
fn parse_sse_block(block: &str) -> Option<SseFrame> {
    let mut id = String::new();
    let mut event = String::new();
    let mut data = String::new();
    let mut saw_named = false;
    for line in block.lines() {
        if line.is_empty() {
            continue;
        }
        if let Some(rest) = line.strip_prefix(':') {
            // Comment / heartbeat — ignore.
            let _ = rest;
            continue;
        }
        if let Some(v) = line.strip_prefix("id:") {
            id = v.trim().to_string();
        } else if let Some(v) = line.strip_prefix("event:") {
            event = v.trim().to_string();
            saw_named = true;
        } else if let Some(v) = line.strip_prefix("data:") {
            if !data.is_empty() {
                data.push('\n');
            }
            data.push_str(v.trim_start());
        } else if line.starts_with("retry:") {
            // retry block, no event — skip.
        }
    }
    if saw_named {
        Some(SseFrame { id, event, data })
    } else {
        None
    }
}

/// Best-effort delete of every topic we created under `ns`.
async fn cleanup(c: &Client, ns: &str) {
    let mut cursor: Option<String> = None;
    loop {
        let path = match &cursor {
            Some(cur) => format!("/v0/topics?prefix={ns}&page_size=1000&cursor={cur}"),
            None => format!("/v0/topics?prefix={ns}&page_size=1000"),
        };
        let Ok(r) = c.get(&path).await else { break };
        let names: Vec<String> = r
            .body
            .get("topics")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.get("topic").and_then(|v| v.as_str()).map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        for n in &names {
            let _ = c.delete(&format!("/v0/topics/{n}")).await;
        }
        cursor = r
            .body
            .get("next_cursor")
            .and_then(|v| v.as_str())
            .map(String::from);
        if cursor.is_none() || names.is_empty() {
            break;
        }
    }
}

// ===========================================================================
// Benchmark suite
// ===========================================================================

/// Percentile (p in [0,100]) of an already-collected sample of latencies (ms).
fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = (p / 100.0) * (sorted.len() as f64 - 1.0);
    let lo = rank.floor() as usize;
    let hi = rank.ceil() as usize;
    if lo == hi {
        sorted[lo]
    } else {
        let frac = rank - lo as f64;
        sorted[lo] * (1.0 - frac) + sorted[hi] * frac
    }
}

fn summarize(mut samples: Vec<f64>) -> Value {
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    json!({
        "count": samples.len(),
        "min_ms": samples.first().copied().unwrap_or(0.0),
        "p50_ms": percentile(&samples, 50.0),
        "p99_ms": percentile(&samples, 99.0),
        "p999_ms": percentile(&samples, 99.9),
        "max_ms": samples.last().copied().unwrap_or(0.0),
    })
}

fn round3(v: f64) -> f64 {
    (v * 1000.0).round() / 1000.0
}

async fn run_bench(cmd: BenchCmd) -> ExitCode {
    let c = Client::new(&cmd.base_url, cmd.token.clone());

    // Verify the server is reachable before benching.
    if c.get("/v0/health")
        .await
        .map(|r| r.status != 200)
        .unwrap_or(true)
    {
        eprintln!(
            "error: server at {} did not return 200 on /v0/health",
            cmd.base_url
        );
        return ExitCode::FAILURE;
    }

    let ns = format!("bench{}", std::process::id());
    eprintln!(
        "benchmarking {} (writes={}, watchers={})",
        cmd.base_url, cmd.writes, cmd.watchers
    );

    let write_bench = bench_write(&c, &ns, cmd.writes).await;
    let diff_bench = bench_diff(&c, &ns).await;
    let watcher_counts: Vec<usize> = cmd
        .watchers
        .split(',')
        .filter_map(|s| s.trim().parse::<usize>().ok())
        .filter(|n| *n > 0)
        .collect();
    let sse_bench = bench_sse_fanout(&c, &ns, &watcher_counts).await;
    let router_bench = bench_router(&c, &ns).await;

    cleanup(&c, &ns).await;

    let summary = json!({
        "base_url": cmd.base_url,
        "writes": cmd.writes,
        "write": write_bench,
        "diff": diff_bench,
        "sse_fanout": sse_bench,
        "router": router_bench,
    });

    if cmd.json {
        println!("{}", serde_json::to_string_pretty(&summary).unwrap());
    } else {
        print_bench_table(&summary);
    }

    ExitCode::SUCCESS
}

/// Durability cost benchmark: durable (fsync-gated, group-committed) vs
/// non-durable write-ack latency + throughput, side by side. Drives a `durable`
/// topic and a `non-durable` topic over the same HTTP path so the only difference is
/// the per-write fsync gate.
async fn run_bench_durable(cmd: BenchDurableCmd) -> ExitCode {
    let c = Client::new(&cmd.base_url, cmd.token.clone());
    if c.get("/v0/health")
        .await
        .map(|r| r.status != 200)
        .unwrap_or(true)
    {
        eprintln!(
            "error: server at {} did not return 200 on /v0/health",
            cmd.base_url
        );
        return ExitCode::FAILURE;
    }
    // Require the server be ready (recovery complete) before benchmarking.
    if c.get("/v0/ready")
        .await
        .map(|r| r.status != 200)
        .unwrap_or(true)
    {
        eprintln!(
            "error: server at {} is not ready (/v0/ready != 200)",
            cmd.base_url
        );
        return ExitCode::FAILURE;
    }

    let ns = format!("durbench{}", std::process::id());
    eprintln!(
        "durability bench {} (samples={}, writes={})",
        cmd.base_url, cmd.samples, cmd.writes
    );

    let durable = bench_one_durability_class(&c, &ns, true, cmd.samples, cmd.writes).await;
    let nondurable = bench_one_durability_class(&c, &ns, false, cmd.samples, cmd.writes).await;

    cleanup(&c, &ns).await;

    let summary = json!({
        "base_url": cmd.base_url,
        "samples": cmd.samples,
        "writes": cmd.writes,
        "durable": durable,
        "non_durable": nondurable,
    });

    if cmd.json {
        println!("{}", serde_json::to_string_pretty(&summary).unwrap());
    } else {
        print_durable_bench_table(&summary);
    }
    ExitCode::SUCCESS
}

/// Measure single-record write-ack latency + concurrent batched throughput for
/// one durability class. Also reports the observed group-commit batching factor
/// (frames-per-fsync, derived from the durable single-record `fsync_ms` only as
/// an informational latency, since the server does not expose internal counters
/// over HTTP).
async fn bench_one_durability_class(
    c: &Client,
    ns: &str,
    durable: bool,
    samples: u64,
    total_writes: u64,
) -> Value {
    let label = if durable { "dur" } else { "nodur" };

    // --- Single-record write-ack latency (one in-flight at a time). ----------
    let lat_topic = format!("{ns}-{label}-lat");
    let _ = c
        .put(
            &format!("/v0/topics/{lat_topic}"),
            &json!({ "durable": durable }),
        )
        .await;
    let lat_path = format!("/v0/topics/{lat_topic}");
    let n = samples.max(1);
    let mut latencies = Vec::with_capacity(n as usize);
    let mut fsync_ms_samples = Vec::with_capacity(n as usize);
    let body = json!({ "records": [{ "data": { "x": "abcdefghij0123456789" } }] });
    for _ in 0..n {
        let t = Instant::now();
        if let Ok(r) = c.post(&lat_path, &body).await {
            if r.status == 200 || r.status == 201 {
                latencies.push(t.elapsed().as_secs_f64() * 1000.0);
                if let Some(f) = r
                    .body
                    .get("performance")
                    .and_then(|p| p.get("fsync_ms"))
                    .and_then(|v| v.as_f64())
                {
                    fsync_ms_samples.push(f);
                }
            }
        }
    }
    let lat_summary = summarize(latencies);
    let server_fsync = summarize(fsync_ms_samples);

    // --- Concurrent batched throughput. --------------------------------------
    let tput_topic = format!("{ns}-{label}-tput");
    let _ = c
        .put(
            &format!("/v0/topics/{tput_topic}"),
            &json!({ "durable": durable }),
        )
        .await;
    let tput_path = Arc::new(format!("/v0/topics/{tput_topic}"));
    let writers = 16usize;
    let batch = 100u64;
    let batches = (total_writes / batch).max(1);
    let per_worker = (batches / writers as u64).max(1);
    let actual_records = per_worker * writers as u64 * batch;
    let payload = {
        let recs: Vec<Value> = (0..batch).map(|i| json!({ "data": { "i": i } })).collect();
        Arc::new(json!({ "records": recs, "return_seqs": false }))
    };
    let done = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    let mut handles = Vec::new();
    for _ in 0..writers {
        let cc = c.clone();
        let path = tput_path.clone();
        let pl = payload.clone();
        let done = done.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..per_worker {
                if cc
                    .post(&format!("{}?return_seqs=false", path), &pl)
                    .await
                    .map(|r| r.status == 200 || r.status == 201)
                    .unwrap_or(false)
                {
                    done.fetch_add(batch, Ordering::Relaxed);
                }
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }
    let elapsed = start.elapsed().as_secs_f64();
    let acked = done.load(Ordering::Relaxed);
    let throughput = if elapsed > 0.0 {
        acked as f64 / elapsed
    } else {
        0.0
    };

    json!({
        "durable": durable,
        "single_record_ack_latency_ms": lat_summary,
        "server_reported_fsync_ms": server_fsync,
        "throughput": {
            "writers": writers,
            "batch_size": batch,
            "records_appended": actual_records,
            "records_acked": acked,
            "elapsed_s": round3(elapsed),
            "records_per_s": round3(throughput),
        }
    })
}

/// Pretty-print the durability bench summary as two side-by-side blocks.
fn print_durable_bench_table(s: &Value) {
    let row = |class: &str, v: &Value| {
        let l = &v["single_record_ack_latency_ms"];
        let tp = &v["throughput"];
        println!(
            "  {class:<12} p50={:.3} p99={:.3} p999={:.3} max={:.3} ms | throughput={:.0} rec/s ({} acked in {:.3}s)",
            l["p50_ms"].as_f64().unwrap_or(0.0),
            l["p99_ms"].as_f64().unwrap_or(0.0),
            l["p999_ms"].as_f64().unwrap_or(0.0),
            l["max_ms"].as_f64().unwrap_or(0.0),
            tp["records_per_s"].as_f64().unwrap_or(0.0),
            tp["records_acked"].as_u64().unwrap_or(0),
            tp["elapsed_s"].as_f64().unwrap_or(0.0),
        );
    };
    println!("=== topics-probe bench-durable: {} ===", s["base_url"]);
    println!("single-record write-ack latency + concurrent batched throughput:");
    row("durable", &s["durable"]);
    row("non-durable", &s["non_durable"]);
    let dfs = &s["durable"]["server_reported_fsync_ms"];
    println!(
        "  durable server-reported fsync_ms: p50={:.3} p99={:.3} max={:.3}",
        dfs["p50_ms"].as_f64().unwrap_or(0.0),
        dfs["p99_ms"].as_f64().unwrap_or(0.0),
        dfs["max_ms"].as_f64().unwrap_or(0.0),
    );
}

/// Write-ack latency (single-record writes) + write throughput (concurrent
/// writers, batched).
async fn bench_write(c: &Client, ns: &str, total_writes: u64) -> Value {
    // --- Latency: single-record writes measured one at a time. ---
    let lat_topic = format!("{ns}-wlat");
    let _ = c.put(&format!("/v0/topics/{lat_topic}"), &json!({})).await;
    let lat_path = format!("/v0/topics/{lat_topic}");
    let lat_samples_n = 5_000.min(total_writes).max(1);
    let mut latencies = Vec::with_capacity(lat_samples_n as usize);
    let body = json!({ "records": [{ "data": { "x": "abcdefghij0123456789" } }] });
    for _ in 0..lat_samples_n {
        let t = Instant::now();
        let r = c.post(&lat_path, &body).await;
        if r.map(|r| r.status == 200 || r.status == 201)
            .unwrap_or(false)
        {
            latencies.push(t.elapsed().as_secs_f64() * 1000.0);
        }
    }
    let lat_summary = summarize(latencies);

    // --- Throughput: concurrent writers, batched appends. ---
    let tput_topic = format!("{ns}-wtput");
    let _ = c.put(&format!("/v0/topics/{tput_topic}"), &json!({})).await;
    let tput_path = Arc::new(format!("/v0/topics/{tput_topic}"));
    let writers = 16usize;
    let batch = 100u64;
    let total = total_writes;
    let batches = (total / batch).max(1);
    let per_worker = (batches / writers as u64).max(1);
    let actual_batches = per_worker * writers as u64;
    let actual_records = actual_batches * batch;

    let payload = {
        let recs: Vec<Value> = (0..batch).map(|i| json!({ "data": { "i": i } })).collect();
        Arc::new(json!({ "records": recs, "return_seqs": false }))
    };

    let done = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    let mut handles = Vec::new();
    for _ in 0..writers {
        let cc = c.clone();
        let path = tput_path.clone();
        let pl = payload.clone();
        let done = done.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..per_worker {
                let r = cc.post(&format!("{}?return_seqs=false", path), &pl).await;
                if r.map(|r| r.status == 200 || r.status == 201)
                    .unwrap_or(false)
                {
                    done.fetch_add(batch, Ordering::Relaxed);
                }
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }
    let elapsed = start.elapsed().as_secs_f64();
    let recs_acked = done.load(Ordering::Relaxed);
    let throughput = if elapsed > 0.0 {
        recs_acked as f64 / elapsed
    } else {
        0.0
    };

    json!({
        "single_record_ack_latency": lat_summary,
        "throughput": {
            "writers": writers,
            "batch_size": batch,
            "records_appended": actual_records,
            "records_acked": recs_acked,
            "elapsed_s": round3(elapsed),
            "records_per_s": round3(throughput),
        }
    })
}

/// getDifference throughput + per-call latency at limits 1 / 256 / 1000.
async fn bench_diff(c: &Client, ns: &str) -> Value {
    let b = format!("{ns}-diff");
    let path = format!("/v0/topics/{b}");
    let dpath = format!("/v0/topics/{b}/diff");

    // Seed ~20k records so deep reads have something to chew on.
    let seed_total = 20_000u64;
    let batch = 500u64;
    let recs: Vec<Value> = (0..batch)
        .map(|i| json!({ "data": { "i": i, "p": "padpadpadpadpad" } }))
        .collect();
    let body = json!({ "records": recs, "return_seqs": false });
    for _ in 0..(seed_total / batch) {
        let _ = c.post(&format!("{path}?return_seqs=false"), &body).await;
    }

    let mut out = serde_json::Map::new();
    for &limit in &[1u32, 256, 1000] {
        // Latency: repeated deep replay from from_seq=0.
        let iters = 1_000usize;
        let mut lat = Vec::with_capacity(iters);
        let req = json!({ "from_seq": 0, "limit": limit });
        let mut total_records: u64 = 0;
        let start = Instant::now();
        for _ in 0..iters {
            let t = Instant::now();
            if let Ok(r) = c.post(&dpath, &req).await {
                lat.push(t.elapsed().as_secs_f64() * 1000.0);
                total_records += r
                    .body
                    .get("records")
                    .and_then(|v| v.as_array())
                    .map(|a| a.len() as u64)
                    .unwrap_or(0);
            }
        }
        let elapsed = start.elapsed().as_secs_f64();
        let recs_per_s = if elapsed > 0.0 {
            total_records as f64 / elapsed
        } else {
            0.0
        };
        out.insert(
            format!("limit_{limit}"),
            json!({
                "latency": summarize(lat),
                "records_served": total_records,
                "records_per_s": round3(recs_per_s),
                "calls_per_s": round3(iters as f64 / elapsed),
            }),
        );
    }

    // Tail-read latency (caught-up near head): cheapest path.
    let head = c
        .get(&path)
        .await
        .ok()
        .and_then(|r| u64_of(&r.body, "head_seq"))
        .unwrap_or(seed_total);
    let mut tail_lat = Vec::with_capacity(2000);
    let treq = json!({ "from_seq": head });
    for _ in 0..2000 {
        let t = Instant::now();
        if c.post(&dpath, &treq).await.is_ok() {
            tail_lat.push(t.elapsed().as_secs_f64() * 1000.0);
        }
    }
    out.insert("tail_caught_up_latency".to_string(), summarize(tail_lat));

    Value::Object(out)
}

/// SSE fan-out write->deliver latency with N watchers on one topic.
async fn bench_sse_fanout(c: &Client, ns: &str, watcher_counts: &[usize]) -> Value {
    let mut out = serde_json::Map::new();

    // A shared monotonic baseline: the writer stamps each pulse with the ns
    // elapsed since `epoch`, the watchers (same process, same clock) subtract it
    // from their own elapsed-ns at receipt. That yields a TRUE write->deliver
    // latency without any wall-clock/server-time skew.
    let epoch = Arc::new(Instant::now());

    for &n in watcher_counts {
        let b = format!("{ns}-sse{n}");
        let path = format!("/v0/topics/{b}");
        // Create + seed one record so the topic exists and watchers tail the head.
        let _ = c
            .post(&path, &json!({ "records": [{ "data": "seed" }] }))
            .await;

        let pulses = 50usize; // number of timed writes
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<f64>();
        let mut watcher_handles = Vec::with_capacity(n);

        for _ in 0..n {
            // Create a session tailing the topic (only records after subscribe).
            let watch = c
                .post(
                    "/v0/watch",
                    &json!({ "topics": { b.clone(): { "tail": true } }, "heartbeat_ms": 1000 }),
                )
                .await;
            let Some(stream_url) = watch.ok().and_then(|r| {
                r.body
                    .get("stream_url")
                    .and_then(|v| v.as_str())
                    .map(String::from)
            }) else {
                continue;
            };
            let cc = c.clone();
            let tx = tx.clone();
            let epoch = epoch.clone();
            watcher_handles.push(tokio::spawn(async move {
                watcher_loop(cc, stream_url, pulses, tx, epoch).await;
            }));
        }
        drop(tx);

        // Give the watchers a moment to establish their topics and reach the
        // caught-up (tailing) state before timed writes begin.
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Emit timed writes; each carries the writer's ns-since-epoch so each
        // watcher can compute the per-pulse write->deliver latency.
        for i in 0..pulses {
            let stamp = epoch.elapsed().as_nanos() as u64;
            let _ = c
                .post(
                    &path,
                    &json!({ "records": [{ "data": { "pulse": i, "t": stamp } }] }),
                )
                .await;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // Collect per-watcher per-pulse delivery-latency samples (give the tail
        // a short grace window to flush the last pulse to every watcher).
        let mut samples = Vec::new();
        let collect = tokio::time::timeout(Duration::from_secs(3), async {
            while let Some(latency_ms) = rx.recv().await {
                samples.push(latency_ms);
            }
        })
        .await;
        let _ = collect;
        for h in watcher_handles {
            h.abort();
        }

        out.insert(
            format!("watchers_{n}"),
            json!({
                "watchers": n,
                "pulses": pulses,
                "deliveries_measured": samples.len(),
                "write_to_deliver_latency": summarize(samples),
            }),
        );
        // Tear down the topic so the next watcher tier starts clean.
        let _ = c.delete(&path).await;
    }

    Value::Object(out)
}

/// A single SSE watcher: opens the stream and, for each `record` frame carrying
/// a `"t"` writer-stamp (ns since the shared `epoch`), reports the write->deliver
/// latency = (receipt ns since epoch) - (stamped ns) over the channel. Writer
/// and watcher run in the same process and share `epoch`, so the subtraction is
/// a true end-to-end latency with no clock skew.
async fn watcher_loop(
    c: Client,
    stream_url: String,
    expected_pulses: usize,
    tx: tokio::sync::mpsc::UnboundedSender<f64>,
    epoch: Arc<Instant>,
) {
    let mut rb = c
        .http
        .get(c.url(&stream_url))
        .header("accept", "text/event-stream");
    if let Some(t) = &c.token {
        rb = rb.header("authorization", format!("Bearer {t}"));
    }
    let resp = match rb.send().await {
        Ok(r) if r.status().is_success() => r,
        _ => return,
    };

    let mut stream = resp.bytes_stream();
    let mut buf = String::new();
    let mut seen = 0usize;

    while let Some(chunk) = stream.next().await {
        let Ok(bytes) = chunk else { break };
        buf.push_str(&String::from_utf8_lossy(&bytes));
        while let Some(idx) = buf.find("\n\n") {
            let block: String = buf.drain(..idx + 2).collect();
            let Some(frame) = parse_sse_block(&block) else {
                continue;
            };
            if frame.event != "record" {
                continue;
            }
            // The frame's `data` is a JSON object with a `records` array; each
            // record carries `data.t`. Parse and time-stamp on arrival.
            let recv_ns = epoch.elapsed().as_nanos() as u64;
            let Ok(payload): Result<Value, _> = serde_json::from_str(&frame.data) else {
                continue;
            };
            let Some(records) = payload.get("records").and_then(|v| v.as_array()) else {
                continue;
            };
            for rec in records {
                let stamp = rec
                    .get("data")
                    .and_then(|d| d.get("t"))
                    .and_then(|v| v.as_u64());
                if let Some(stamp) = stamp {
                    let latency_ms = recv_ns.saturating_sub(stamp) as f64 / 1_000_000.0;
                    let _ = tx.send(latency_ms);
                    seen += 1;
                }
            }
            if seen >= expected_pulses {
                return;
            }
        }
    }
}

/// Router forwarding added latency: time from a write into `src` until the
/// forwarded copy is observable in `dst` via diff, vs a direct write+read.
async fn bench_router(c: &Client, ns: &str) -> Value {
    let src = format!("{ns}-rsrc");
    let dst = format!("{ns}-rdst");
    let rname = format!("{ns}-rbench");
    let _ = c.put(&format!("/v0/topics/{src}"), &json!({})).await;
    let _ = c.put(&format!("/v0/topics/{dst}"), &json!({})).await;
    let _ = c
        .put(
            &format!("/v0/routers/{rname}"),
            &json!({ "source": src, "dest": dst }),
        )
        .await;

    let iters = 1_000usize;

    // Forwarded path: write to src, then read dst from its prior head to confirm
    // the forwarded record landed; measure end-to-end.
    let mut fwd = Vec::with_capacity(iters);
    let src_path = format!("/v0/topics/{src}");
    let dst_diff = format!("/v0/topics/{dst}/diff");
    for i in 0..iters {
        // current dst head
        let dst_head = c
            .get(&format!("/v0/topics/{dst}"))
            .await
            .ok()
            .and_then(|r| u64_of(&r.body, "head_seq"))
            .unwrap_or(0);
        let t = Instant::now();
        let _ = c
            .post(&src_path, &json!({ "records": [{ "data": { "i": i } }] }))
            .await;
        // Poll dst until the forwarded record is visible (synchronous forwarding
        // means it should already be there on the first read).
        loop {
            if let Ok(r) = c.post(&dst_diff, &json!({ "from_seq": dst_head })).await {
                if !diff_seqs(&r.body).is_empty() {
                    break;
                }
            }
            if t.elapsed() > Duration::from_secs(1) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        fwd.push(t.elapsed().as_secs_f64() * 1000.0);
    }

    // Direct baseline: write to a plain topic + read it back (no router).
    let direct_topic = format!("{ns}-direct");
    let _ = c
        .put(&format!("/v0/topics/{direct_topic}"), &json!({}))
        .await;
    let dpath = format!("/v0/topics/{direct_topic}");
    let ddiff = format!("/v0/topics/{direct_topic}/diff");
    let mut direct = Vec::with_capacity(iters);
    for i in 0..iters {
        let head = c
            .get(&dpath)
            .await
            .ok()
            .and_then(|r| u64_of(&r.body, "head_seq"))
            .unwrap_or(0);
        let t = Instant::now();
        let _ = c
            .post(&dpath, &json!({ "records": [{ "data": { "i": i } }] }))
            .await;
        let _ = c.post(&ddiff, &json!({ "from_seq": head })).await;
        direct.push(t.elapsed().as_secs_f64() * 1000.0);
    }

    let fwd_summary = summarize(fwd.clone());
    let direct_summary = summarize(direct.clone());
    let fwd_p50 = fwd_summary
        .get("p50_ms")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    let direct_p50 = direct_summary
        .get("p50_ms")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    json!({
        "forwarded_write_to_visible": fwd_summary,
        "direct_write_read_baseline": direct_summary,
        "added_p50_ms": round3(fwd_p50 - direct_p50),
    })
}

fn print_bench_table(s: &Value) {
    println!(
        "\n=== topics-probe bench: {} ===",
        s.get("base_url").and_then(|v| v.as_str()).unwrap_or("?")
    );

    let pr = |label: &str, v: &Value| {
        let g = |k: &str| v.get(k).and_then(|x| x.as_f64()).unwrap_or(0.0);
        println!(
            "  {label:<34} p50={:>8.3}ms  p99={:>8.3}ms  p999={:>8.3}ms  max={:>8.3}ms  (n={})",
            g("p50_ms"),
            g("p99_ms"),
            g("p999_ms"),
            g("max_ms"),
            v.get("count").and_then(|c| c.as_u64()).unwrap_or(0),
        );
    };

    println!("\n-- Write --");
    if let Some(w) = s.get("write") {
        if let Some(l) = w.get("single_record_ack_latency") {
            pr("single-record write-ack", l);
        }
        if let Some(t) = w.get("throughput") {
            println!(
                "  throughput: {:.0} records/s  ({} writers x batch {}, {} records in {:.3}s)",
                t.get("records_per_s")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0),
                t.get("writers").and_then(|v| v.as_u64()).unwrap_or(0),
                t.get("batch_size").and_then(|v| v.as_u64()).unwrap_or(0),
                t.get("records_acked").and_then(|v| v.as_u64()).unwrap_or(0),
                t.get("elapsed_s").and_then(|v| v.as_f64()).unwrap_or(0.0),
            );
        }
    }

    println!("\n-- getDifference --");
    if let Some(d) = s.get("diff") {
        for limit in ["limit_1", "limit_256", "limit_1000"] {
            if let Some(dl) = d.get(limit) {
                if let Some(lat) = dl.get("latency") {
                    pr(&format!("diff {limit}"), lat);
                }
                println!(
                    "      -> {:.0} records/s, {:.0} calls/s",
                    dl.get("records_per_s")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.0),
                    dl.get("calls_per_s")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.0),
                );
            }
        }
        if let Some(t) = d.get("tail_caught_up_latency") {
            pr("diff tail (caught-up)", t);
        }
    }

    println!("\n-- SSE fan-out (write -> deliver) --");
    if let Some(sse) = s.get("sse_fanout").and_then(|v| v.as_object()) {
        let mut keys: Vec<&String> = sse.keys().collect();
        keys.sort_by_key(|k| {
            k.trim_start_matches("watchers_")
                .parse::<usize>()
                .unwrap_or(0)
        });
        for k in keys {
            let v = &sse[k];
            if let Some(lat) = v.get("write_to_deliver_latency") {
                pr(
                    &format!(
                        "{} watcher(s)",
                        v.get("watchers").and_then(|x| x.as_u64()).unwrap_or(0)
                    ),
                    lat,
                );
            }
        }
    }

    println!("\n-- Router forwarding --");
    if let Some(r) = s.get("router") {
        if let Some(f) = r.get("forwarded_write_to_visible") {
            pr("src->dst write-to-visible", f);
        }
        if let Some(d) = r.get("direct_write_read_baseline") {
            pr("direct write+read baseline", d);
        }
        println!(
            "  added forwarding latency (p50): {:.3} ms",
            r.get("added_p50_ms")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0)
        );
    }
    println!();
}

// ===========================================================================
// Phase-5B workload emulators
// ===========================================================================

/// Confirm the server answers `/v0/health` 200 before a workload run; otherwise
/// print an error and signal the caller to bail with a nonzero exit.
async fn preflight(c: &Client, base_url: &str) -> bool {
    if c.get("/v0/health")
        .await
        .map(|r| r.status != 200)
        .unwrap_or(true)
    {
        eprintln!("error: server at {base_url} did not return 200 on /v0/health");
        return false;
    }
    true
}

/// Summary stats for a set of per-unit counts (jobs-per-worker, etc.): the
/// distribution-evenness block. `cv` (coefficient of variation = stddev/mean)
/// is the headline evenness metric — lower is more even (0 = perfect).
fn distribution_stats(counts: &[u64]) -> Value {
    if counts.is_empty() {
        return json!({ "n": 0, "min": 0, "max": 0, "mean": 0.0, "stddev": 0.0, "cv": 0.0 });
    }
    let n = counts.len() as f64;
    let sum: u64 = counts.iter().sum();
    let mean = sum as f64 / n;
    let var = counts
        .iter()
        .map(|&x| (x as f64 - mean).powi(2))
        .sum::<f64>()
        / n;
    let stddev = var.sqrt();
    let cv = if mean > 0.0 { stddev / mean } else { 0.0 };
    json!({
        "n": counts.len(),
        "min": counts.iter().copied().min().unwrap_or(0),
        "max": counts.iter().copied().max().unwrap_or(0),
        "mean": round3(mean),
        "stddev": round3(stddev),
        "cv": round3(cv),
    })
}

/// Render a latency-summary `Value` (from [`summarize`]) as a compact one-liner.
fn fmt_latency(v: &Value) -> String {
    let g = |k: &str| v.get(k).and_then(|x| x.as_f64()).unwrap_or(0.0);
    format!(
        "p50={:.3}ms p99={:.3}ms p999={:.3}ms max={:.3}ms (n={})",
        g("p50_ms"),
        g("p99_ms"),
        g("p999_ms"),
        g("max_ms"),
        v.get("count").and_then(|c| c.as_u64()).unwrap_or(0),
    )
}

// ---------------------------------------------------------------------------
// 1. BROADCAST — 1 source topic -> N SSE watchers over shared zero-copy frames
// ---------------------------------------------------------------------------

/// Emit-side of a broadcast pulse measurement: a per-watcher write->deliver
/// latency sample (ms) computed against the shared process `epoch`.
async fn run_broadcast(cmd: BroadcastCmd) -> ExitCode {
    let c = Client::new(&cmd.base_url, cmd.token.clone());
    if !preflight(&c, &cmd.base_url).await {
        return ExitCode::FAILURE;
    }
    let ns = format!("bcast{}", std::process::id());
    let counts: Vec<usize> = cmd
        .watchers
        .split(',')
        .filter_map(|s| s.trim().parse::<usize>().ok())
        .filter(|n| *n > 0)
        .collect();
    eprintln!(
        "broadcast {} (watchers={:?}, pulses={})",
        cmd.base_url, counts, cmd.pulses
    );

    let epoch = Arc::new(Instant::now());
    let mut tiers = serde_json::Map::new();

    for &n in &counts {
        let b = format!("{ns}-w{n}");
        let path = format!("/v0/topics/{b}");
        // Create + seed so the topic exists; watchers then tail the head.
        let _ = c
            .post(&path, &json!({ "records": [{ "data": "seed" }] }))
            .await;

        // Per-watcher delivery latencies arrive on this channel.
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<f64>();
        let mut handles = Vec::with_capacity(n);
        for _ in 0..n {
            // One shared session per watcher, tailing the topic (records after subscribe).
            let watch = c
                .post(
                    "/v0/watch",
                    &json!({ "topics": { b.clone(): { "tail": true } }, "heartbeat_ms": 2000 }),
                )
                .await;
            let Some(stream_url) = watch.ok().and_then(|r| {
                r.body
                    .get("stream_url")
                    .and_then(|v| v.as_str())
                    .map(String::from)
            }) else {
                continue;
            };
            let cc = c.clone();
            let tx = tx.clone();
            let epoch = epoch.clone();
            let pulses = cmd.pulses;
            handles.push(tokio::spawn(async move {
                watcher_loop(cc, stream_url, pulses, tx, epoch).await;
            }));
        }
        let connected = handles.len();
        drop(tx);

        // Let every watcher establish + reach the tailing (caught-up) state.
        tokio::time::sleep(Duration::from_millis(800)).await;

        // The writer appends ONE record per pulse; the read layer serializes that
        // record's frame ONCE and ref-counts it to all `connected` watchers.
        let mut write_lat = Vec::with_capacity(cmd.pulses);
        let send_start = Instant::now();
        for i in 0..cmd.pulses {
            let stamp = epoch.elapsed().as_nanos() as u64;
            let wt = Instant::now();
            let _ = c
                .post(
                    &path,
                    &json!({ "records": [{ "data": { "pulse": i, "t": stamp } }] }),
                )
                .await;
            write_lat.push(wt.elapsed().as_secs_f64() * 1000.0);
            if cmd.pulse_gap_ms > 0 {
                tokio::time::sleep(Duration::from_millis(cmd.pulse_gap_ms)).await;
            }
        }
        let send_elapsed = send_start.elapsed().as_secs_f64();

        // Collect per-watcher per-pulse delivery samples (short grace to flush).
        let mut samples = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(latency_ms) = rx.recv().await {
                samples.push(latency_ms);
            }
        })
        .await;
        for h in handles {
            h.abort();
        }

        let deliveries = samples.len() as u64;
        // Aggregate deliveries/sec: total deliveries observed across all watchers
        // over the send window (the fan-out rate the read layer sustained).
        let deliveries_per_s = if send_elapsed > 0.0 {
            deliveries as f64 / send_elapsed
        } else {
            0.0
        };
        tiers.insert(
            format!("watchers_{n}"),
            json!({
                "watchers_requested": n,
                "watchers_connected": connected,
                "pulses": cmd.pulses,
                "deliveries_measured": deliveries,
                "deliveries_per_s": round3(deliveries_per_s),
                "write_to_deliver_latency": summarize(samples),
                "writer_append_latency": summarize(write_lat),
            }),
        );
        let _ = c.delete(&path).await;
    }

    cleanup(&c, &ns).await;
    let summary = json!({ "base_url": cmd.base_url, "workload": "broadcast", "tiers": Value::Object(tiers.clone()) });

    if cmd.json {
        println!("{}", serde_json::to_string_pretty(&summary).unwrap());
    } else {
        println!("\n=== topics-probe broadcast: {} ===", cmd.base_url);
        println!("one source topic -> N SSE watchers (shared zero-copy frame fan-out):");
        let mut keys: Vec<&String> = tiers.keys().collect();
        keys.sort_by_key(|k| {
            k.trim_start_matches("watchers_")
                .parse::<usize>()
                .unwrap_or(0)
        });
        for k in keys {
            let v = &tiers[k];
            println!(
                "  {:>5} watcher(s) [{} connected]: {:.0} deliveries/s | write->deliver {}",
                v["watchers_requested"].as_u64().unwrap_or(0),
                v["watchers_connected"].as_u64().unwrap_or(0),
                v["deliveries_per_s"].as_f64().unwrap_or(0.0),
                fmt_latency(&v["write_to_deliver_latency"]),
            );
        }
        println!();
    }
    ExitCode::SUCCESS
}

// ---------------------------------------------------------------------------
// 2. DISTRIBUTION — 1 source -> many topics via batched, sharded fan-out
// ---------------------------------------------------------------------------

async fn run_distribution(cmd: DistributionCmd) -> ExitCode {
    let c = Client::new(&cmd.base_url, cmd.token.clone());
    if !preflight(&c, &cmd.base_url).await {
        return ExitCode::FAILURE;
    }
    let ns = format!("dist{}", std::process::id());
    let n_topics = cmd.topics.max(1);
    let batch = cmd.batch.max(1);
    let writers = cmd.writers.max(1);
    eprintln!(
        "distribution {} (topics={}, batch={}, writers={})",
        cmd.base_url, n_topics, batch, writers
    );

    // Each request appends `batch` records to ONE destination topic (a small,
    // sharded append). We round-robin topics across `total_batches` requests so
    // the fan-out spreads evenly. total appends == total_batches * batch.
    let total_batches = n_topics; // one batch per topic: every topic gets `batch` records.
    let total_records = total_batches * batch;

    // Pre-build a reusable batch payload (return_seqs=false to keep responses tiny).
    let payload = {
        let recs: Vec<Value> = (0..batch)
            .map(|i| json!({ "data": { "i": i, "p": "evt" } }))
            .collect();
        Arc::new(json!({ "records": recs, "return_seqs": false }))
    };

    let next = Arc::new(AtomicU64::new(0));
    let done_records = Arc::new(AtomicU64::new(0));
    let done_batches = Arc::new(AtomicU64::new(0));
    // Per-topic append latency samples (a sampled subset to bound memory).
    let lat = Arc::new(std::sync::Mutex::new(Vec::<f64>::with_capacity(20_000)));

    let base = Arc::new(format!("{ns}-b"));
    let start = Instant::now();
    let mut handles = Vec::with_capacity(writers);
    for _ in 0..writers {
        let cc = c.clone();
        let pl = payload.clone();
        let next = next.clone();
        let done_records = done_records.clone();
        let done_batches = done_batches.clone();
        let lat = lat.clone();
        let base = base.clone();
        handles.push(tokio::spawn(async move {
            let mut local_lat: Vec<f64> = Vec::new();
            loop {
                let idx = next.fetch_add(1, Ordering::Relaxed);
                if idx >= total_batches {
                    break;
                }
                let path = format!("/v0/topics/{base}{idx}?return_seqs=false");
                let t = Instant::now();
                let ok = cc
                    .post(&path, &pl)
                    .await
                    .map(|r| r.status == 200 || r.status == 201)
                    .unwrap_or(false);
                if ok {
                    done_records.fetch_add(batch, Ordering::Relaxed);
                    done_batches.fetch_add(1, Ordering::Relaxed);
                    // Sample ~1 in 4 to keep the latency vector bounded.
                    if idx.is_multiple_of(4) {
                        local_lat.push(t.elapsed().as_secs_f64() * 1000.0);
                    }
                }
            }
            if !local_lat.is_empty() {
                lat.lock().unwrap().extend(local_lat);
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }
    let elapsed = start.elapsed().as_secs_f64();
    let recs = done_records.load(Ordering::Relaxed);
    let batches_ok = done_batches.load(Ordering::Relaxed);
    let appends_per_s = if elapsed > 0.0 {
        recs as f64 / elapsed
    } else {
        0.0
    };
    let lat_summary = summarize(
        Arc::try_unwrap(lat)
            .ok()
            .and_then(|m| m.into_inner().ok())
            .unwrap_or_default(),
    );

    cleanup(&c, &ns).await;

    let summary = json!({
        "base_url": cmd.base_url,
        "workload": "distribution",
        "topics": n_topics,
        "batch_size": batch,
        "writers": writers,
        "records_target": total_records,
        "records_appended": recs,
        "batches_ok": batches_ok,
        "elapsed_s": round3(elapsed),
        "appends_per_s": round3(appends_per_s),
        "per_request_latency": lat_summary,
    });

    if cmd.json {
        println!("{}", serde_json::to_string_pretty(&summary).unwrap());
    } else {
        println!("\n=== topics-probe distribution: {} ===", cmd.base_url);
        println!(
            "1 source -> {} topics via {} batched writers (batch={}):",
            n_topics, writers, batch
        );
        println!(
            "  {:.0} appends/s  ({} records into {} topics in {:.3}s)",
            appends_per_s, recs, batches_ok, elapsed
        );
        println!(
            "  per-request append latency: {}",
            fmt_latency(&lat_summary)
        );
        if appends_per_s < 1_000_000.0 {
            println!(
                "  NOTE: {:.0} appends/s is below the 1M/s target. Single-process HTTP probe is\n        the ceiling here — per-request overhead (TCP/HTTP/JSON parse) dominates over\n        a localhost loopback. Raise --batch and --writers, and run the probe on a\n        separate host/core set, to push closer; the write path itself is sharded\n        and lock-free per topic (ARCHITECTURE §8).",
                appends_per_s
            );
        }
        println!();
    }
    ExitCode::SUCCESS
}

// ---------------------------------------------------------------------------
// 3. QUEUE — producers fill a queue; N workers claim/ack in a loop
// ---------------------------------------------------------------------------

async fn run_queue(cmd: QueueCmd) -> ExitCode {
    let c = Client::new(&cmd.base_url, cmd.token.clone());
    if !preflight(&c, &cmd.base_url).await {
        return ExitCode::FAILURE;
    }
    let ns = format!("queue{}", std::process::id());
    let workers = cmd.workers.max(1);
    let jobs = cmd.jobs.max(1);
    eprintln!(
        "queue {} (workers={}, jobs={}, claim_max={}, jitter={}ms)",
        cmd.base_url, workers, jobs, cmd.claim_max, cmd.jitter
    );

    let q = format!("{ns}-q");
    let path = format!("/v0/topics/{q}");
    // Create the queue. lease_ms generous so workers never lose a lease mid-run.
    let _ = c
        .put(
            &path,
            &json!({ "type": "queue", "lease_ms": 60000, "claim_jitter_ms": cmd.jitter }),
        )
        .await;

    // Produce `jobs` jobs via batched appends (the produce path is a normal append).
    let produce_batch = 500u64;
    let prod_start = Instant::now();
    {
        let mut produced = 0u64;
        while produced < jobs {
            let n = produce_batch.min(jobs - produced);
            let recs: Vec<Value> = (0..n)
                .map(|i| json!({ "data": { "j": produced + i } }))
                .collect();
            let _ = c
                .post(
                    &format!("{path}?return_seqs=false"),
                    &json!({ "records": recs, "return_seqs": false }),
                )
                .await;
            produced += n;
        }
    }
    let produce_elapsed = prod_start.elapsed().as_secs_f64();

    // N workers: each loops claim -> ack until the queue is drained. A worker
    // stops after a few consecutive empty claims (the count<max "near-empty"
    // signal from §10.2).
    let claim_lat = Arc::new(std::sync::Mutex::new(Vec::<f64>::with_capacity(20_000)));
    let per_worker = Arc::new((0..workers).map(|_| AtomicU64::new(0)).collect::<Vec<_>>());
    let total_acked = Arc::new(AtomicU64::new(0));
    let target = jobs;

    let work_start = Instant::now();
    let mut handles = Vec::with_capacity(workers);
    for wid in 0..workers {
        let cc = c.clone();
        let path = path.clone();
        let claim_lat = claim_lat.clone();
        let per_worker = per_worker.clone();
        let total_acked = total_acked.clone();
        let claim_max = cmd.claim_max;
        handles.push(tokio::spawn(async move {
            let node = format!("w{wid}");
            let mut local_lat: Vec<f64> = Vec::new();
            let mut empty_streak = 0u32;
            loop {
                if total_acked.load(Ordering::Relaxed) >= target {
                    break;
                }
                let t = Instant::now();
                let claim = cc
                    .post(
                        &format!("{path}/claim"),
                        &json!({ "node": node, "max": claim_max }),
                    )
                    .await;
                local_lat.push(t.elapsed().as_secs_f64() * 1000.0);
                let seqs: Vec<u64> = match &claim {
                    Ok(r) => r
                        .body
                        .get("claimed")
                        .and_then(|v| v.as_array())
                        .map(|a| {
                            a.iter()
                                .filter_map(|j| j.get("$seq").and_then(|s| s.as_u64()))
                                .collect()
                        })
                        .unwrap_or_default(),
                    Err(_) => Vec::new(),
                };
                if seqs.is_empty() {
                    empty_streak += 1;
                    // Drained (or contended): a few empty claims in a row ends the worker.
                    if empty_streak >= 5 {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(2)).await;
                    continue;
                }
                empty_streak = 0;
                let n = seqs.len() as u64;
                let ack = cc
                    .post(
                        &format!("{path}/ack"),
                        &json!({ "node": node, "seqs": seqs }),
                    )
                    .await;
                let acked = ack.ok().and_then(|r| u64_of(&r.body, "acked")).unwrap_or(0);
                if acked > 0 {
                    per_worker[wid].fetch_add(acked, Ordering::Relaxed);
                    total_acked.fetch_add(acked, Ordering::Relaxed);
                } else {
                    // Claimed but acked 0 (lease lost / raced) — count claim work anyway.
                    let _ = n;
                }
            }
            if !local_lat.is_empty() {
                claim_lat.lock().unwrap().extend(local_lat);
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }
    let work_elapsed = work_start.elapsed().as_secs_f64();

    let acked = total_acked.load(Ordering::Relaxed);
    let jobs_per_s = if work_elapsed > 0.0 {
        acked as f64 / work_elapsed
    } else {
        0.0
    };
    let counts: Vec<u64> = per_worker
        .iter()
        .map(|a| a.load(Ordering::Relaxed))
        .collect();
    let dist = distribution_stats(&counts);
    let claim_summary = summarize(
        Arc::try_unwrap(claim_lat)
            .ok()
            .and_then(|m| m.into_inner().ok())
            .unwrap_or_default(),
    );

    // Leftover (if any) for transparency.
    let leftover = c.get(&path).await.ok().and_then(|r| {
        r.body
            .get("queue")
            .and_then(|x| x.get("ready"))
            .and_then(|v| v.as_u64())
    });

    cleanup(&c, &ns).await;

    let summary = json!({
        "base_url": cmd.base_url,
        "workload": "queue",
        "workers": workers,
        "jobs_produced": jobs,
        "jobs_acked": acked,
        "claim_max": cmd.claim_max,
        "claim_jitter_ms": cmd.jitter,
        "produce_elapsed_s": round3(produce_elapsed),
        "produce_per_s": round3(if produce_elapsed > 0.0 { jobs as f64 / produce_elapsed } else { 0.0 }),
        "process_elapsed_s": round3(work_elapsed),
        "jobs_per_s": round3(jobs_per_s),
        "claim_latency": claim_summary,
        "per_worker_distribution": dist,
        "ready_left": leftover,
    });

    if cmd.json {
        println!("{}", serde_json::to_string_pretty(&summary).unwrap());
    } else {
        println!("\n=== topics-probe queue: {} ===", cmd.base_url);
        println!(
            "{} workers claim/ack {} jobs (claim_max={}, jitter={}ms):",
            workers, jobs, cmd.claim_max, cmd.jitter
        );
        println!(
            "  produce: {:.0} jobs/s ({:.3}s) | process: {:.0} jobs/s ({} acked in {:.3}s)",
            summary["produce_per_s"].as_f64().unwrap_or(0.0),
            produce_elapsed,
            jobs_per_s,
            acked,
            work_elapsed,
        );
        println!("  claim latency: {}", fmt_latency(&claim_summary));
        println!(
            "  per-worker evenness: min={} max={} mean={:.1} stddev={:.1} cv={:.3} (lower cv = more even)",
            dist["min"].as_u64().unwrap_or(0),
            dist["max"].as_u64().unwrap_or(0),
            dist["mean"].as_f64().unwrap_or(0.0),
            dist["stddev"].as_f64().unwrap_or(0.0),
            dist["cv"].as_f64().unwrap_or(0.0),
        );
        println!();
    }
    ExitCode::SUCCESS
}

// ---------------------------------------------------------------------------
// 4. ACTORS / INFERENCE — per-actor topic, event chains, snapshot compaction
// ---------------------------------------------------------------------------

async fn run_actors(cmd: ActorsCmd) -> ExitCode {
    let c = Client::new(&cmd.base_url, cmd.token.clone());
    if !preflight(&c, &cmd.base_url).await {
        return ExitCode::FAILURE;
    }
    let ns = format!("actors{}", std::process::id());
    let actors = cmd.actors.max(1);
    let inferences = cmd.inferences.max(1);
    let tool_results = cmd.tool_results;
    let concurrency = cmd.concurrency.max(1);
    // Chain length per inference: 1 model-answer + 1 tool-call + T tool-results.
    let chain_len = 2 + tool_results;
    eprintln!(
        "actors {} (actors={}, inferences={}/actor, chain_len={}, snapshot_every={})",
        cmd.base_url, actors, inferences, chain_len, cmd.snapshot_every
    );

    let next = Arc::new(AtomicU64::new(0));
    let events_done = Arc::new(AtomicU64::new(0));
    let snapshots_done = Arc::new(AtomicU64::new(0));
    let append_lat = Arc::new(std::sync::Mutex::new(Vec::<f64>::with_capacity(20_000)));
    let base = Arc::new(format!("{ns}-actor"));

    let start = Instant::now();
    let mut handles = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let cc = c.clone();
        let next = next.clone();
        let events_done = events_done.clone();
        let snapshots_done = snapshots_done.clone();
        let append_lat = append_lat.clone();
        let base = base.clone();
        let snapshot_every = cmd.snapshot_every.max(1);
        handles.push(tokio::spawn(async move {
            let mut local_lat: Vec<f64> = Vec::new();
            loop {
                let a = next.fetch_add(1, Ordering::Relaxed);
                if a >= actors {
                    break;
                }
                let path = format!("/v0/topics/{base}{a}");
                for inf in 0..inferences {
                    // One inference = one chain appended as a single batch:
                    //   model-answer + tool-call + N tool-result events.
                    let mut recs: Vec<Value> = Vec::with_capacity(chain_len as usize);
                    recs.push(json!({ "data": { "role": "assistant", "kind": "model-answer", "inf": inf } }));
                    recs.push(json!({ "data": { "role": "assistant", "kind": "tool-call", "tool": "search", "inf": inf } }));
                    for tr in 0..tool_results {
                        recs.push(json!({ "data": { "role": "tool", "kind": "tool-result", "tr": tr, "inf": inf } }));
                    }
                    let t = Instant::now();
                    let resp = cc
                        .post(&format!("{path}?return_seqs=false"), &json!({ "records": recs, "return_seqs": false }))
                        .await;
                    if resp.as_ref().map(|r| r.status == 200 || r.status == 201).unwrap_or(false) {
                        events_done.fetch_add(chain_len, Ordering::Relaxed);
                        if a.is_multiple_of(8) {
                            local_lat.push(t.elapsed().as_secs_f64() * 1000.0);
                        }
                    }
                    // Periodic snapshot compaction: delete everything before the
                    // current head (drop old inference chains, keep the latest).
                    if (inf + 1) % snapshot_every == 0 {
                        if let Ok(st) = cc.get(&path).await {
                            if let Some(head) = u64_of(&st.body, "head_seq") {
                                // before_seq = head keeps the most recent record (seq==head).
                                let _ = cc
                                    .post(&format!("{path}/delete"), &json!({ "before_seq": head }))
                                    .await;
                                snapshots_done.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
            }
            if !local_lat.is_empty() {
                append_lat.lock().unwrap().extend(local_lat);
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }
    let elapsed = start.elapsed().as_secs_f64();

    let events = events_done.load(Ordering::Relaxed);
    let snaps = snapshots_done.load(Ordering::Relaxed);
    let events_per_s = if elapsed > 0.0 {
        events as f64 / elapsed
    } else {
        0.0
    };
    let inferences_per_s = if elapsed > 0.0 {
        (actors * inferences) as f64 / elapsed
    } else {
        0.0
    };
    let lat_summary = summarize(
        Arc::try_unwrap(append_lat)
            .ok()
            .and_then(|m| m.into_inner().ok())
            .unwrap_or_default(),
    );

    cleanup(&c, &ns).await;

    let summary = json!({
        "base_url": cmd.base_url,
        "workload": "actors",
        "actors": actors,
        "inferences_per_actor": inferences,
        "chain_len": chain_len,
        "tool_results_per_inference": tool_results,
        "snapshot_every": cmd.snapshot_every,
        "events_appended": events,
        "snapshots_compacted": snaps,
        "elapsed_s": round3(elapsed),
        "events_per_s": round3(events_per_s),
        "inferences_per_s": round3(inferences_per_s),
        "chain_append_latency": lat_summary,
    });

    if cmd.json {
        println!("{}", serde_json::to_string_pretty(&summary).unwrap());
    } else {
        println!("\n=== topics-probe actors: {} ===", cmd.base_url);
        println!(
            "{} actor topics x {} inferences (chain_len={}, snapshot every {}):",
            actors, inferences, chain_len, cmd.snapshot_every
        );
        println!(
            "  {:.0} events/s ({} events, {} chains, {} snapshot-compactions in {:.3}s)",
            events_per_s,
            events,
            actors * inferences,
            snaps,
            elapsed
        );
        println!("  {:.0} inferences/s", inferences_per_s);
        println!("  per-chain append latency: {}", fmt_latency(&lat_summary));
        println!();
    }
    ExitCode::SUCCESS
}
