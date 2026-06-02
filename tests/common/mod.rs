//! Shared in-process integration harness (Phase-3 §2).
//!
//! Boots the *real* axum server (`topics::http::build_router`) on an ephemeral
//! `127.0.0.1:0` port inside a dedicated background tokio runtime/thread, waits
//! for `/v0/health` to answer `200`, and exposes a small synchronous HTTP client
//! built on `reqwest::blocking` so integration tests read top-to-bottom without
//! `async`/`await`. The server uses a real `SystemClock` (live HTTP), so any
//! TTL/priority *correctness* assertions belong in the engine unit/property
//! tests with a `TestClock`; this harness is for black-topic wire-contract flows.
//!
//! Each `Harness` owns its own engine + port, so tests are fully isolated and
//! may run in parallel.
//!
//! # Public API
//!
//! ```ignore
//! use common::Harness;
//! use serde_json::json;
//!
//! let h = Harness::start();                  // boot a fresh server, wait for health
//! let url = h.base_url();                     // e.g. "http://127.0.0.1:53124"
//!
//! // JSON helpers -> (StatusCode, serde_json::Value):
//! let (status, body) = h.put("/v0/topics/jobs", json!({ "durable": true }));
//! let (status, body) = h.post("/v0/topics/jobs", json!({ "records": [{ "data": 1 }] }));
//! let (status, body) = h.get("/v0/topics/jobs");
//! let (status, body) = h.delete("/v0/topics/jobs");
//! // `post`/`put`/`delete` send `Content-Type: application/json` automatically.
//! // For an explicit empty body use `post_empty(path)`.
//!
//! // SSE helper: open a watch stream and collect the first N named frames with a
//! // bounded timeout (never hangs on the long-lived stream):
//! let frames = h.sse_frames("/v0/watch/wid_0000000001", 2, Duration::from_secs(5));
//! for f in &frames { println!("{} {} {}", f.id, f.event, f.data); }
//! ```

#![allow(dead_code)] // not every test uses every helper.

use std::net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use reqwest::blocking::Client;
pub use reqwest::StatusCode;
use serde_json::Value;

use topics::clock::{SharedClock, SystemClock, TestClock};
use topics::config::ServerConfig;
use topics::engine::Engine;
use topics::http;

/// A booted in-process server plus a blocking HTTP client pointed at it.
pub struct Harness {
    base_url: String,
    client: Client,
    /// Signals graceful shutdown; taken in `Drop` so we can join the server
    /// thread (which flushes + joins the WAL writer) BEFORE the harness goes
    /// away — essential for restart tests where the WAL must hit disk.
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<thread::JoinHandle<()>>,
    /// Unique per-harness data dir for the WAL; removed on drop so tests stay
    /// isolated and leave no on-disk residue.
    _data_dir: tempfile::TempDir,
    /// `Some` when this harness was booted with an injected [`TestClock`]
    /// ([`Harness::start_with_test_clock`]) so timing-dependent flows (lease
    /// expiry / visibility timeout, delayed nacks) can be driven deterministically
    /// over the real bound server — no wall-clock sleeps for correctness.
    clock: Option<TestClock>,
}

impl Harness {
    /// Boot a fresh server on an ephemeral port with default (auth-disabled)
    /// config and wait until `/v0/health` returns `200`. Panics on any failure
    /// so the calling test fails loudly.
    pub fn start() -> Harness {
        Harness::start_with(ServerConfig::default())
    }

    /// Like [`start`](Self::start) but with an injected [`TestClock`] (returned
    /// alongside via [`Harness::clock`]) so a black-topic test can advance the
    /// server's notion of time deterministically — used for lease-expiry /
    /// visibility-timeout and delayed-nack flows that would otherwise need real
    /// sleeps. The server runs on this exact clock, so a `clock.advance(ms)`
    /// followed by a claim observes the post-advance lease state with no race.
    pub fn start_with_test_clock() -> Harness {
        let clock = TestClock::new(1_000_000_000);
        Harness::boot(
            ServerConfig::default(),
            Arc::new(clock.clone()),
            Some(clock),
            None,
        )
    }

    /// Like [`start`](Self::start) but with a caller-supplied [`ServerConfig`]
    /// (e.g. to enable bearer auth via `api_keys`). Each harness gets a UNIQUE
    /// temp data dir for the WAL (via `tempfile::tempdir`), so the durable write
    /// path is exercised while tests stay isolated and leave nothing behind.
    pub fn start_with(config: ServerConfig) -> Harness {
        let clock: SharedClock = Arc::new(SystemClock);
        Harness::boot(config, clock, None, None)
    }

    /// Boot a server whose WAL lives in `data_dir` (owned by the *caller*, not the
    /// harness) so the on-disk state SURVIVES this harness being dropped. Used to
    /// simulate a process restart: boot, drop, then re-boot a second harness on
    /// the same dir and assert recovery. The data dir is NOT removed on drop.
    pub fn start_persistent(data_dir: &std::path::Path) -> Harness {
        let config = ServerConfig {
            data_dir: Some(data_dir.to_string_lossy().into_owned()),
            ..ServerConfig::default()
        };
        let clock: SharedClock = Arc::new(SystemClock);
        Harness::boot(config, clock, None, Some(data_dir.to_path_buf()))
    }

    /// The injected [`TestClock`], present only for a harness booted via
    /// [`Harness::start_with_test_clock`]. Advance it to move the server's clock
    /// forward (e.g. past a lease deadline) deterministically.
    pub fn clock(&self) -> &TestClock {
        self.clock
            .as_ref()
            .expect("clock() requires Harness::start_with_test_clock")
    }

    /// Shared boot path: bind an ephemeral port, spawn the server on a dedicated
    /// runtime/thread with `clock`, and wait for health. When `external_dir` is
    /// `Some`, the WAL lives there and outlives this harness (restart tests);
    /// otherwise a fresh per-harness tempdir is created and removed on drop.
    fn boot(
        mut config: ServerConfig,
        clock: SharedClock,
        test_clock: Option<TestClock>,
        external_dir: Option<std::path::PathBuf>,
    ) -> Harness {
        // Reserve an ephemeral port on the std listener, then hand the address to
        // the async runtime (which re-binds it). Closing this std listener first
        // avoids the address being held while tokio binds.
        let std_listener =
            StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind ephemeral port");
        let addr: SocketAddr = std_listener.local_addr().expect("local_addr");
        drop(std_listener);

        // Per-harness data dir. For a restart test the caller owns the dir (it
        // must survive this harness being dropped), so we keep `_data_dir` as a
        // throwaway tempdir but point the config at the external path.
        let data_dir = tempfile::tempdir().expect("create temp data dir");
        match &external_dir {
            Some(p) => config.data_dir = Some(p.to_string_lossy().into_owned()),
            None => config.data_dir = Some(data_dir.path().to_string_lossy().into_owned()),
        }

        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<()>();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        let thread = thread::Builder::new()
            .name("topics-harness".to_string())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .expect("build harness runtime");
                rt.block_on(async move {
                    let engine = Engine::with_data_dir(config, clock).expect("open durable engine");
                    // Share the shutdown signal with the serve loop so in-flight SSE
                    // streams are wound down on shutdown, exactly as the binary does
                    // (M11). The harness exercises the production drain path.
                    let sse_shutdown = std::sync::Arc::new(topics::serve::ShutdownSignal::new());
                    let app = http::build_router_with_shutdown(engine, sse_shutdown.clone())
                        .expect("build router");

                    let listener = tokio::net::TcpListener::bind(addr)
                        .await
                        .expect("rebind ephemeral port");
                    let _ = ready_tx.send(());
                    // Same dual-protocol (HTTP/1.1 keep-alive + h2c prior-knowledge)
                    // serve loop the binary uses, so the harness exercises the exact
                    // production path under both protocols.
                    let _ = topics::serve::serve_with_signal(
                        listener,
                        app,
                        async {
                            let _ = shutdown_rx.await;
                        },
                        sse_shutdown,
                    )
                    .await;
                });
            })
            .expect("spawn harness thread");

        // Wait for the listener to be bound before issuing requests.
        ready_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("server failed to bind");

        let base_url = format!("http://{addr}");
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("build reqwest client");

        let h = Harness {
            base_url,
            client,
            shutdown: Some(shutdown_tx),
            thread: Some(thread),
            _data_dir: data_dir,
            clock: test_clock,
        };
        h.wait_healthy(Duration::from_secs(5));
        h
    }

    /// Gracefully shut the server down and join its thread, draining + fsyncing
    /// the WAL writer (via the engine's `Drop`). Idempotent. Called automatically
    /// on drop; a restart test can call it explicitly to be sure the on-disk WAL
    /// is complete before re-booting on the same data dir.
    pub fn shutdown(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }

    /// The server base URL, e.g. `http://127.0.0.1:53124` (no trailing slash).
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Block until `GET /v0/health` returns `200`, or panic after `deadline`.
    fn wait_healthy(&self, deadline: Duration) {
        let start = Instant::now();
        loop {
            if let Ok(resp) = self.client.get(self.url("/v0/health")).send() {
                if resp.status().is_success() {
                    return;
                }
            }
            if start.elapsed() > deadline {
                panic!("server did not become healthy within {deadline:?}");
            }
            thread::sleep(Duration::from_millis(20));
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    // -- JSON request helpers -> (StatusCode, Value) -------------------------

    /// `GET path` (no body). Returns `(status, parsed-json-or-Null)`.
    pub fn get(&self, path: &str) -> (StatusCode, Value) {
        self.send(self.client.get(self.url(path)))
    }

    /// `POST path` with a JSON body and `Content-Type: application/json`.
    pub fn post(&self, path: &str, body: Value) -> (StatusCode, Value) {
        self.send(self.client.post(self.url(path)).json(&body))
    }

    /// `PUT path` with a JSON body and `Content-Type: application/json`.
    pub fn put(&self, path: &str, body: Value) -> (StatusCode, Value) {
        self.send(self.client.put(self.url(path)).json(&body))
    }

    /// `DELETE path` (no body). Returns `(status, parsed-json-or-Null)`.
    pub fn delete(&self, path: &str) -> (StatusCode, Value) {
        self.send(self.client.delete(self.url(path)))
    }

    /// `POST path` with an explicit empty body (no `Content-Type`). Useful to
    /// exercise the `415`/empty-body paths.
    pub fn post_empty(&self, path: &str) -> (StatusCode, Value) {
        self.send(self.client.post(self.url(path)))
    }

    /// `POST path` with a JSON body and a bearer token header.
    pub fn post_auth(&self, path: &str, body: Value, token: &str) -> (StatusCode, Value) {
        self.send(
            self.client
                .post(self.url(path))
                .bearer_auth(token)
                .json(&body),
        )
    }

    /// `GET path` with a bearer token header.
    pub fn get_auth(&self, path: &str, token: &str) -> (StatusCode, Value) {
        self.send(self.client.get(self.url(path)).bearer_auth(token))
    }

    /// `PUT path` with a JSON body and a bearer token header.
    pub fn put_auth(&self, path: &str, body: Value, token: &str) -> (StatusCode, Value) {
        self.send(
            self.client
                .put(self.url(path))
                .bearer_auth(token)
                .json(&body),
        )
    }

    /// `DELETE path` (no body) with a bearer token header.
    pub fn delete_auth(&self, path: &str, token: &str) -> (StatusCode, Value) {
        self.send(self.client.delete(self.url(path)).bearer_auth(token))
    }

    /// Send `method` to `path` (no body) with a bearer token header. Supports the
    /// no-body verbs the scope tests exercise (`DELETE`); other methods get their
    /// own typed helper.
    pub fn send_auth(&self, method: &str, path: &str, token: &str) -> (StatusCode, Value) {
        let url = self.url(path);
        let req = match method {
            "DELETE" => self.client.delete(url),
            "GET" => self.client.get(url),
            other => panic!("send_auth: unsupported method {other}"),
        };
        self.send(req.bearer_auth(token))
    }

    /// `GET path` returning the raw body as a string (for non-JSON responses like
    /// the Prometheus text exposition). `token` is sent as a bearer when `Some`.
    pub fn get_text(&self, path: &str, token: Option<&str>) -> (StatusCode, String) {
        let mut req = self.client.get(self.url(path));
        if let Some(t) = token {
            req = req.bearer_auth(t);
        }
        let resp = req.send().expect("request failed to send");
        let status = resp.status();
        let body = resp.text().expect("read response body as text");
        (status, body)
    }

    /// Send a prepared request, returning `(status, body-as-json-or-Null)`.
    fn send(&self, req: reqwest::blocking::RequestBuilder) -> (StatusCode, Value) {
        let resp = req.send().expect("request failed to send");
        let status = resp.status();
        let bytes = resp.bytes().expect("read response body");
        let value = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap_or(Value::Null)
        };
        (status, value)
    }

    // -- SSE helper ----------------------------------------------------------

    /// Open the SSE stream at `path` and collect up to `max_frames` *named-event*
    /// frames (`id`/`event`/`data`), abandoning the read after `deadline` so a
    /// long-lived stream can never hang the test. Heartbeat comments and bare
    /// `retry:` frames are skipped. Panics if the stream does not open `200` with
    /// a `text/event-stream` content-type.
    pub fn sse_frames(&self, path: &str, max_frames: usize, deadline: Duration) -> Vec<SseFrame> {
        use std::io::Read;

        let mut resp = self
            .client
            .get(self.url(path))
            .header(reqwest::header::ACCEPT, "text/event-stream")
            // A read timeout bounds each chunk read so the loop can't block past
            // the deadline if the producer parks.
            .timeout(deadline)
            .send()
            .expect("open SSE stream");
        assert_eq!(resp.status(), StatusCode::OK, "SSE stream must open 200");
        let ct = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert!(
            ct.starts_with("text/event-stream"),
            "SSE content-type must be text/event-stream, got {ct:?}"
        );

        let mut buf: Vec<u8> = Vec::new();
        let mut frames: Vec<SseFrame> = Vec::new();
        let mut chunk = [0u8; 4096];
        let start = Instant::now();
        while frames.len() < max_frames && start.elapsed() < deadline {
            match resp.read(&mut chunk) {
                Ok(0) => break, // stream closed.
                Ok(n) => {
                    buf.extend_from_slice(&chunk[..n]);
                    drain_frames(&mut buf, &mut frames);
                }
                Err(_) => break, // timeout / reset: return what we have.
            }
        }
        drain_frames(&mut buf, &mut frames);
        frames
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        // Signal shutdown and join the server thread so the WAL writer flushes
        // and joins before the (possibly shared) data dir is reused or removed.
        self.shutdown();
    }
}

/// A parsed SSE frame: a named event with its `id`/`event`/`data` lines joined.
#[derive(Debug, Clone)]
pub struct SseFrame {
    pub id: String,
    pub event: String,
    pub data: String,
}

/// Split the byte buffer on blank-line (`\n\n`) frame boundaries and parse each
/// complete block, leaving any partial trailing block in `buf`.
fn drain_frames(buf: &mut Vec<u8>, out: &mut Vec<SseFrame>) {
    let text = String::from_utf8_lossy(buf).into_owned();
    let bytes = text.as_bytes();
    let mut start = 0usize;
    let mut last_end = 0usize;
    let mut i = 0usize;
    while i + 1 < bytes.len() {
        if bytes[i] == b'\n' && bytes[i + 1] == b'\n' {
            let end = i + 2;
            if let Some(frame) = parse_frame(&text[start..end]) {
                out.push(frame);
            }
            start = end;
            last_end = end;
            i += 2;
        } else {
            i += 1;
        }
    }
    if last_end > 0 {
        buf.drain(0..last_end.min(buf.len()));
    }
}

/// Parse one SSE block into a named-event frame, or `None` for a comment
/// (`:` heartbeat) or a block that carries only `retry:`.
fn parse_frame(raw: &str) -> Option<SseFrame> {
    let mut id = String::new();
    let mut event = String::new();
    let mut data = String::new();
    let mut has_event = false;
    for line in raw.lines() {
        if line.is_empty() || line.starts_with(':') {
            continue;
        }
        if let Some(v) = line.strip_prefix("id:") {
            id = v.trim().to_string();
        } else if let Some(v) = line.strip_prefix("event:") {
            event = v.trim().to_string();
            has_event = true;
        } else if let Some(v) = line.strip_prefix("data:") {
            if !data.is_empty() {
                data.push('\n');
            }
            data.push_str(v.strip_prefix(' ').unwrap_or(v));
        }
    }
    if has_event {
        Some(SseFrame { id, event, data })
    } else {
        None
    }
}
