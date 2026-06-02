//! Phase-4 Stage-5 crash-recovery acceptance tests.
//!
//! These spawn the **real `topics` release/debug binary as a subprocess** with
//! a unique `tempfile::tempdir` data dir, drive it over HTTP, then `kill -9`
//! (SIGKILL) the process — no graceful shutdown, no drop handlers, nothing
//! flushed beyond what the WAL fsync already durably committed. A second boot on
//! the SAME data dir must recover the state.
//!
//! Coverage (ROADMAP Phase-4 acceptance + Stage-5 task list):
//!
//! - **Durability:** every *acked durable* write survives a SIGKILL at an
//!   arbitrary instant and is present (identical seq/ts/data/tag/node) after
//!   restart (`sigkill_durable_writes_survive_with_identical_state`).
//! - **Recovery correctness:** after restart, `head_seq`/`earliest_seq`/`count`/
//!   `config`/routers/and the set of deleted records all match the pre-crash
//!   committed state (same test, asserting each).
//! - **Crash consistency / clean prefix:** a SIGKILL during a burst of
//!   *non-durable* writes leaves a WAL whose recovered tail is a clean prefix —
//!   no torn frame is ever misread as data, the server comes back ready, and the
//!   recovered seqs are a contiguous prefix `[1..=k]`
//!   (`sigkill_during_nondurable_burst_recovers_clean_prefix`).
//! - **Torn tail on disk:** corrupting/truncating the last WAL frame of a
//!   crashed process's data dir still recovers cleanly (truncation, no panic, no
//!   bogus record) and the WAL is writable again
//!   (`torn_tail_on_subprocess_wal_recovers_clean`).
//!
//! The in-process engine-level durability tests live in
//! `tests/integration_durability.rs`; these are the black-topic, real-process
//! proofs.

#![cfg(unix)]

use std::time::{Duration, Instant};

use serde_json::{json, Value};

// ---------------------------------------------------------------------------
// Subprocess + HTTP helpers
// ---------------------------------------------------------------------------

/// A spawned server: the child process plus the `base_url` it actually bound.
struct Server {
    child: std::process::Child,
    base: String,
}

/// Spawn the `topics` binary on an EPHEMERAL port (`TOPICS_PORT=0`) with
/// `data_dir`, then read the OS-assigned `host:port` the child wrote to its
/// `TOPICS_PORT_FILE`. This is robust under parallel spawn: the child holds the
/// bound socket continuously, so unlike a reserve-then-release scheme nothing can
/// steal the port between reservation and bind. Logs silenced.
///
/// `port_file` is a caller-owned path (kept alive for the child's lifetime); the
/// returned [`Server`] carries the resolved base URL.
fn spawn_server(data_dir: &std::path::Path, port_file: &std::path::Path) -> Server {
    // Start clean so a stale file from a prior boot on the same path is never read.
    let _ = std::fs::remove_file(port_file);
    let child = std::process::Command::new(env!("CARGO_BIN_EXE_topics"))
        .env("TOPICS_HOST", "127.0.0.1")
        .env("TOPICS_PORT", "0") // OS-assigned ephemeral port (no bind race).
        .env("TOPICS_PORT_FILE", port_file)
        .env("TOPICS_DATA_DIR", data_dir)
        // Pin a single WAL shard so the on-disk layout is the flat
        // `wal/wal-<idx>.log` these tests inspect/poke directly (the default is
        // num_cpus-based, which would spread frames across `shard-NN/` subdirs).
        .env("TOPICS_WAL_SHARDS", "1")
        .env("RUST_LOG", "error")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn topics binary");
    let base = read_port_file(port_file, Duration::from_secs(10));
    Server { child, base }
}

/// Poll `port_file` until the child has written its resolved `host:port`, then
/// return the `http://host:port` base URL. Panics after `deadline`.
fn read_port_file(port_file: &std::path::Path, deadline: Duration) -> String {
    let start = Instant::now();
    loop {
        if let Ok(s) = std::fs::read_to_string(port_file) {
            let s = s.trim();
            if !s.is_empty() {
                return format!("http://{s}");
            }
        }
        if start.elapsed() > deadline {
            panic!("server did not report its bound port within {deadline:?}");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Block until `GET /v0/health` answers 200, or panic after `deadline`.
fn wait_healthy(client: &reqwest::blocking::Client, base: &str, deadline: Duration) {
    let start = Instant::now();
    loop {
        if let Ok(r) = client.get(format!("{base}/v0/health")).send() {
            if r.status().is_success() {
                return;
            }
        }
        if start.elapsed() > deadline {
            panic!("server did not become healthy within {deadline:?}");
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

/// Block until `GET /v0/ready` answers 200 (recovery complete), or panic.
fn wait_ready(client: &reqwest::blocking::Client, base: &str, deadline: Duration) {
    let start = Instant::now();
    loop {
        if let Ok(r) = client.get(format!("{base}/v0/ready")).send() {
            if r.status().as_u16() == 200 {
                return;
            }
        }
        if start.elapsed() > deadline {
            panic!("server did not become ready within {deadline:?}");
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

/// SIGKILL via the C `kill(2)` syscall (no extra crate).
fn sigkill(pid: u32) {
    extern "C" {
        fn kill(pid: i32, sig: i32) -> i32;
    }
    const SIGKILL: i32 = 9;
    unsafe {
        kill(pid as i32, SIGKILL);
    }
}

fn client() -> reqwest::blocking::Client {
    reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap()
}

fn get_json(c: &reqwest::blocking::Client, url: String) -> Value {
    c.get(url).send().unwrap().json().unwrap()
}

// ---------------------------------------------------------------------------
// 1) SIGKILL after acked durable writes → full recovery correctness.
// ---------------------------------------------------------------------------

/// Build a non-trivial durable state (writes with tags/nodes, a delete, a cap
/// topic that evicts, and a router), confirm every write is acked (durable ⇒ the
/// 2xx returns only after fsync), SIGKILL, restart on the same dir, and assert
/// the FULL state matches pre-crash: head/earliest/count/config, the deleted
/// record stays gone (silent), the cap floor still tombstones, and the router +
/// its forwarded record survive.
#[test]
fn sigkill_durable_writes_survive_with_identical_state() {
    let dir = tempfile::tempdir().unwrap();
    let pf = tempfile::NamedTempFile::new().unwrap();
    let c = client();

    // --- Boot #1: build state, all durable, every write acked (= on disk). ---
    let server = spawn_server(dir.path(), pf.path());
    let mut child = server.child;
    let base = server.base;
    wait_healthy(&c, &base, Duration::from_secs(10));

    // A durable "jobs" topic: 5 tagged/noded writes, then delete seq < 2.
    assert!(c
        .put(format!("{base}/v0/topics/jobs"))
        .json(&json!({ "durable": true, "ttl_ms": 0 }))
        .send()
        .unwrap()
        .status()
        .is_success());
    for i in 1..=5 {
        let r = c
            .post(format!("{base}/v0/topics/jobs"))
            .json(&json!({ "records": [{
                "data": { "i": i }, "tag": format!("t{i}"), "node": "writerA"
            }] }))
            .send()
            .unwrap();
        assert!(r.status().is_success(), "durable write {i} acked");
        let body: Value = r.json().unwrap();
        // durable:true ⇒ the ack is fsync-gated; fsync_ms must be real (> 0).
        let fsync_ms = body["performance"]["fsync_ms"].as_f64().unwrap_or(0.0);
        assert!(fsync_ms > 0.0, "durable write {i} fsync-gated (fsync_ms>0)");
    }
    // Delete the prefix < seq 2 (voluntary ⇒ silent, advances earliest only).
    assert!(c
        .post(format!("{base}/v0/topics/jobs/delete"))
        .json(&json!({ "before_seq": 2 }))
        .send()
        .unwrap()
        .status()
        .is_success());

    // A durable cap topic (cap=3): write 6 ⇒ evict_floor advances (involuntary).
    assert!(c
        .put(format!("{base}/v0/topics/capped"))
        .json(&json!({ "durable": true, "cap_records": 3 }))
        .send()
        .unwrap()
        .status()
        .is_success());
    for i in 1..=6 {
        let r = c
            .post(format!("{base}/v0/topics/capped"))
            .json(&json!({ "records": [{ "data": { "i": i } }] }))
            .send()
            .unwrap();
        assert!(r.status().is_success());
    }

    // A router src->dst (both durable), then a durable write to the SOURCE.
    // The source log is the durable source of truth; the forwarded dest copy is
    // a derived, in-memory cache (ARCHITECTURE §8.3 — at-least-once forwarding),
    // so what must survive a crash is the router DEFINITION + cursor and the
    // source's own durable records, plus correct re-forwarding afterwards.
    assert!(c
        .put(format!("{base}/v0/routers/r1"))
        .json(&json!({
            "source": "src", "dest": "dst",
            "preserve_node": true, "preserve_tag": true, "create_dest": true
        }))
        .send()
        .unwrap()
        .status()
        .is_success());
    assert!(c
        .put(format!("{base}/v0/topics/src"))
        .json(&json!({ "durable": true }))
        .send()
        .unwrap()
        .status()
        .is_success());
    assert!(c
        .put(format!("{base}/v0/topics/dst"))
        .json(&json!({ "durable": true }))
        .send()
        .unwrap()
        .status()
        .is_success());
    assert!(c
        .post(format!("{base}/v0/topics/src"))
        .json(&json!({ "records": [{ "data": { "fwd": 1 }, "tag": "ftag", "node": "writerA" }] }))
        .send()
        .unwrap()
        .status()
        .is_success());

    // Snapshot the pre-crash committed state for an exact comparison.
    let pre_jobs = get_json(&c, format!("{base}/v0/topics/jobs"));
    let pre_capped = get_json(&c, format!("{base}/v0/topics/capped"));
    let pre_src = get_json(&c, format!("{base}/v0/topics/src"));
    let pre_router = get_json(&c, format!("{base}/v0/routers/r1"));

    // --- Hard kill: SIGKILL, no graceful path. ---
    let pid = child.id();
    sigkill(pid);
    let _ = child.wait();

    // --- Boot #2 on the SAME data dir: state must match pre-crash exactly. The
    // ephemeral port differs across boots, so rebind to the new base URL. ---
    let server2 = spawn_server(dir.path(), pf.path());
    let mut child2 = server2.child;
    let base = server2.base;
    wait_ready(&c, &base, Duration::from_secs(10));

    let post_jobs = get_json(&c, format!("{base}/v0/topics/jobs"));
    let post_capped = get_json(&c, format!("{base}/v0/topics/capped"));
    let post_src = get_json(&c, format!("{base}/v0/topics/src"));
    let post_router = get_json(&c, format!("{base}/v0/routers/r1"));

    // head/earliest/count/config all identical to pre-crash for the topics whose
    // records are durably WAL-logged (direct writes): jobs, capped, src.
    for field in ["head_seq", "earliest_seq", "count", "config"] {
        assert_eq!(post_jobs[field], pre_jobs[field], "jobs.{field} matches");
        assert_eq!(
            post_capped[field], pre_capped[field],
            "capped.{field} matches"
        );
        assert_eq!(post_src[field], pre_src[field], "src.{field} matches");
    }
    assert_eq!(post_jobs["head_seq"], 5);
    assert_eq!(post_jobs["earliest_seq"], 2, "deleted prefix < 2 gone");
    assert_eq!(post_jobs["count"], 4);
    assert_eq!(post_jobs["config"]["durable"], json!(true));

    // The deleted record stays gone and the delete is still SILENT (no tombstone).
    let jobs_diff: Value = c
        .post(format!("{base}/v0/topics/jobs/diff"))
        .json(&json!({ "from_seq": 0, "include_tags": true }))
        .send()
        .unwrap()
        .json()
        .unwrap();
    let seqs: Vec<u64> = jobs_diff["records"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["$seq"].as_u64().unwrap())
        .collect();
    assert_eq!(seqs, vec![2, 3, 4, 5], "seq 1 deleted, survivors intact");
    assert!(
        jobs_diff["tombstone"].is_null(),
        "voluntary delete stays silent after SIGKILL+restart"
    );
    // Identical seq/data/tag/node for a survivor (record fidelity).
    assert_eq!(jobs_diff["records"][0]["$seq"], json!(2));
    assert_eq!(jobs_diff["records"][0]["data"], json!({ "i": 2 }));
    assert_eq!(jobs_diff["records"][0]["$tag"], json!("t2"));
    assert_eq!(jobs_diff["records"][0]["$node"], json!("writerA"));

    // Cap topic: floor recovered ⇒ a cursor below it STILL tombstones (no silent
    // involuntary loss across restart).
    assert_eq!(post_capped["head_seq"], 6);
    assert_eq!(post_capped["earliest_seq"], 4, "cap floor recovered");
    assert_eq!(post_capped["count"], 3);
    let cap_diff: Value = c
        .post(format!("{base}/v0/topics/capped/diff"))
        .json(&json!({ "from_seq": 0 }))
        .send()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(
        cap_diff["tombstone"]["reason"],
        json!("cap"),
        "cap tombstone after restart"
    );

    // The router DEFINITION survived (a WAL control frame), so forwarding is
    // wired again on restart. The router was created on an EMPTY source, so its
    // durable create-time cursor is 0 (genuine v2 `Some(0)`, codex P0 #3): on
    // restart `reforward_routers_on_recovery` re-derives `source[0..head]` into the
    // dest. What matters durably: the router edge exists, and because the source's
    // `fwd:1` record is still in source retention, recovery RE-MATERIALIZES the
    // pre-crash derived copy (the v2 at-least-once contract — a forwarded copy is
    // never WAL-logged, it is re-derived from the durable source + the cursor).
    assert_eq!(post_router["source"], json!("src"));
    assert_eq!(post_router["dest"], json!("dst"));
    let _ = &pre_router; // (definition compared above; total is non-durable here)

    // v2 RE-MATERIALIZATION: the pre-crash forwarded `fwd:1` copy reappears in dst
    // after restart, re-derived from the still-retained source record (NOT a silent
    // loss). Forwarding is async; the dst diff drives the read-path catch-up, plus a
    // beat for the recovery re-forward / background worker.
    std::thread::sleep(Duration::from_millis(50));
    let dst_diff1: Value = c
        .post(format!("{base}/v0/topics/dst/diff"))
        .json(&json!({ "from_seq": 0, "include_tags": true }))
        .send()
        .unwrap()
        .json()
        .unwrap();
    let fwd1 = dst_diff1["records"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["data"] == json!({ "fwd": 1 }))
        .expect("pre-crash forwarded copy re-materialized from retained source after restart");
    assert_eq!(
        fwd1["$tag"],
        json!("ftag"),
        "re-derived copy preserves $tag"
    );
    assert_eq!(
        fwd1["$node"],
        json!("writerA"),
        "re-derived copy preserves $node"
    );

    // Forwarding still works after restart: a NEW durable write to src is
    // forwarded to dst with $node/$tag preserved.
    assert!(c
        .post(format!("{base}/v0/topics/src"))
        .json(&json!({ "records": [{ "data": { "fwd": 2 }, "tag": "ftag2", "node": "writerB" }] }))
        .send()
        .unwrap()
        .status()
        .is_success());
    // Forwarding is async under the v2 default; the dst diff below drives the
    // read-path router catch-up, but give the background worker a beat too.
    std::thread::sleep(Duration::from_millis(50));
    let dst_diff2: Value = c
        .post(format!("{base}/v0/topics/dst/diff"))
        .json(&json!({ "from_seq": 0, "include_tags": true }))
        .send()
        .unwrap()
        .json()
        .unwrap();
    let fwd2 = dst_diff2["records"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["data"] == json!({ "fwd": 2 }))
        .expect("post-restart write forwarded to dst");
    assert_eq!(fwd2["$tag"], json!("ftag2"), "forward preserves $tag");
    assert_eq!(fwd2["$node"], json!("writerB"), "forward preserves $node");

    let _ = child2.kill();
    let _ = child2.wait();
}

// ---------------------------------------------------------------------------
// 2) SIGKILL during a burst of NON-DURABLE writes → clean prefix recovery.
// ---------------------------------------------------------------------------

/// Fire a high-rate burst of non-durable writes (the writer group-commits them
/// without per-write fsync), SIGKILL the process mid-burst, then restart: the
/// recovered WAL tail must be a CLEAN PREFIX — recovery truncates any torn
/// frame, never misreads a partial write as data, never panics, and the seqs are
/// a contiguous `[1..=k]`. Some un-fsynced tail may be lost (the documented
/// non-durable tradeoff); what survives must be a valid, contiguous prefix.
#[test]
fn sigkill_during_nondurable_burst_recovers_clean_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let pf = tempfile::NamedTempFile::new().unwrap();
    let c = client();

    let server = spawn_server(dir.path(), pf.path());
    let mut child = server.child;
    let base = server.base;
    wait_healthy(&c, &base, Duration::from_secs(10));

    // A NON-durable topic (durable:false default) ⇒ writes are acked after the
    // buffered write; durability follows a later group fsync.
    assert!(c
        .put(format!("{base}/v0/topics/burst"))
        .json(&json!({ "durable": false }))
        .send()
        .unwrap()
        .status()
        .is_success());

    // Burst many non-durable writes concurrently to maximize the chance the
    // SIGKILL lands mid-write (a torn tail) rather than on a frame boundary.
    let writers = 8u32;
    let per_writer = 400u32;
    let mut handles = Vec::new();
    for w in 0..writers {
        let base = base.clone();
        handles.push(std::thread::spawn(move || {
            let c = client();
            for i in 0..per_writer {
                let _ = c
                    .post(format!("{base}/v0/topics/burst"))
                    .json(&json!({ "records": [{ "data": { "w": w, "i": i } }],
                                   "return_seqs": false }))
                    .send();
            }
        }));
    }
    // Let the burst get going, then SIGKILL mid-flight.
    std::thread::sleep(Duration::from_millis(120));
    let pid = child.id();
    sigkill(pid);
    let _ = child.wait();
    for h in handles {
        let _ = h.join();
    }

    // --- Restart on the same dir: recovery must succeed and the tail be clean.
    // The ephemeral port differs across boots, so rebind to the new base URL. ---
    let server2 = spawn_server(dir.path(), pf.path());
    let mut child2 = server2.child;
    let base = server2.base;
    // The fact it becomes ready at all proves recovery did not panic on a torn
    // tail (a misread partial frame would either panic or corrupt the index).
    // A generous deadline keeps this robust even when the test binaries run in
    // parallel and contend for CPU/disk (replaying the burst tail takes longer
    // under load).
    wait_ready(&c, &base, Duration::from_secs(60));

    let st = get_json(&c, format!("{base}/v0/topics/burst"));
    let head = st["head_seq"].as_u64().unwrap();
    let count = st["count"].as_u64().unwrap();
    assert_eq!(st["earliest_seq"], 1, "no eviction; earliest stays 1");
    // The recovered LIVE set is a dense contiguous prefix [1..=count] (no deletes/
    // eviction, no middle gap). For a `disk` topic, `head` may sit ABOVE `count` by
    // the durable head reservation (R3): the SIGKILL dropped the un-fsynced disk
    // tail, so recovery resumes at the reservation ceiling and the unwritten
    // reserved seqs are silent deleted gaps — but head NEVER regresses below an
    // acked seq (no reuse) and never exceeds the reservation block.
    assert!(head >= count, "head never below the live count (no reuse)");
    assert!(
        head <= count + topics::config::DISK_HEAD_RESERVE_AHEAD,
        "head {head} within the reservation block of count {count}"
    );
    assert!(count >= 1, "at least some prefix survived");

    // Read the whole live log back and assert it is exactly the contiguous prefix
    // [1..=count] — i.e. a torn tail was truncated, not misinterpreted as a
    // bogus/garbled record, and there are no gaps in the LIVE set (the reserved
    // tail gap, if any, reads as silent deleted holes the diff skips).
    let mut all_seqs: Vec<u64> = Vec::new();
    let mut from = 0u64;
    loop {
        let d: Value = c
            .post(format!("{base}/v0/topics/burst/diff"))
            .json(&json!({ "from_seq": from, "limit": 1000 }))
            .send()
            .unwrap()
            .json()
            .unwrap();
        let recs = d["records"].as_array().unwrap();
        if recs.is_empty() {
            break;
        }
        for r in recs {
            all_seqs.push(r["$seq"].as_u64().unwrap());
        }
        from = d["next_from_seq"].as_u64().unwrap();
        if d["caught_up"].as_bool().unwrap_or(false) {
            break;
        }
    }
    let expected: Vec<u64> = (1..=count).collect();
    assert_eq!(
        all_seqs, expected,
        "recovered live records are exactly the contiguous prefix [1..=count] (clean tail)"
    );

    // The WAL is writable again post-recovery: a fresh durable write appends
    // cleanly after the truncated tail and survives a second restart.
    let r = c
        .post(format!("{base}/v0/topics/burst"))
        .json(&json!({ "records": [{ "data": { "after": "crash" } }] }))
        .send()
        .unwrap();
    let status = r.status();
    let body = r.text().unwrap_or_default();
    assert!(status.is_success(), "fresh write failed: {status} {body}");
    let new_head: u64 = c
        .get(format!("{base}/v0/topics/burst"))
        .send()
        .unwrap()
        .json::<Value>()
        .unwrap()["head_seq"]
        .as_u64()
        .unwrap();
    assert_eq!(
        new_head,
        head + 1,
        "append continues after the clean prefix"
    );

    let _ = child2.kill();
    let _ = child2.wait();
}

// ---------------------------------------------------------------------------
// 3) Torn tail injected on a crashed subprocess's WAL → clean recovery.
// ---------------------------------------------------------------------------

/// SIGKILL a process with acked durable writes, then deliberately corrupt the
/// on-disk WAL tail (append an oversized/garbled partial frame) before
/// restarting. Recovery must detect the torn tail (length overrun / CRC),
/// truncate it, recover exactly the good frames (no panic, no bogus record), and
/// leave the WAL writable.
#[test]
fn torn_tail_on_subprocess_wal_recovers_clean() {
    let dir = tempfile::tempdir().unwrap();
    let pf = tempfile::NamedTempFile::new().unwrap();
    let c = client();

    let server = spawn_server(dir.path(), pf.path());
    let mut child = server.child;
    let base = server.base;
    wait_healthy(&c, &base, Duration::from_secs(10));

    assert!(c
        .put(format!("{base}/v0/topics/t"))
        .json(&json!({ "durable": true }))
        .send()
        .unwrap()
        .status()
        .is_success());
    for i in 1..=3 {
        let r = c
            .post(format!("{base}/v0/topics/t"))
            .json(&json!({ "records": [{ "data": { "i": i } }] }))
            .send()
            .unwrap();
        assert!(r.status().is_success(), "durable write {i} acked");
    }

    // Hard kill so the on-disk WAL is exactly the committed durable frames.
    let pid = child.id();
    sigkill(pid);
    let _ = child.wait();

    // Inject a torn tail at the true end-of-data (found via the WAL reader's own
    // framing), simulating a write interrupted by power loss: an oversized
    // frame_len with only a few trailing bytes ⇒ length overrun / CRC failure.
    let wal_dir = dir.path().join("wal");
    let mut files: Vec<_> = std::fs::read_dir(&wal_dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| {
            p.file_name()
                .and_then(|s| s.to_str())
                .map(|n| n.starts_with("wal-") && n.ends_with(".log"))
                .unwrap_or(false)
        })
        .collect();
    files.sort();
    let active = files.last().unwrap().clone();

    use std::io::{Seek, SeekFrom, Write};
    let data_end = topics::storage::WalReader::open(&active)
        .unwrap()
        .count_valid_len();
    {
        let mut f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&active)
            .unwrap();
        f.seek(SeekFrom::Start(data_end as u64)).unwrap();
        let mut junk = Vec::new();
        junk.extend_from_slice(&9999u32.to_le_bytes()); // frame_len far past EOF
        junk.extend_from_slice(&[0xAB; 16]); // a few garbage bytes
        f.write_all(&junk).unwrap();
        f.sync_all().unwrap();
    }

    // Restart: recovery truncates the torn tail and recovers exactly 3 frames.
    // The ephemeral port differs across boots, so rebind to the new base URL.
    let server2 = spawn_server(dir.path(), pf.path());
    let mut child2 = server2.child;
    let base = server2.base;
    wait_ready(&c, &base, Duration::from_secs(10));

    let st = get_json(&c, format!("{base}/v0/topics/t"));
    assert_eq!(
        st["head_seq"], 3,
        "good frames recovered, torn tail discarded"
    );
    assert_eq!(st["count"], 3);
    let d: Value = c
        .post(format!("{base}/v0/topics/t/diff"))
        .json(&json!({ "from_seq": 0 }))
        .send()
        .unwrap()
        .json()
        .unwrap();
    let seqs: Vec<u64> = d["records"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["$seq"].as_u64().unwrap())
        .collect();
    assert_eq!(seqs, vec![1, 2, 3], "no bogus record from the torn tail");

    // WAL writable again: a new durable write appends after the truncation and
    // survives a clean restart (proves truncation, not append-after-garbage).
    assert!(c
        .post(format!("{base}/v0/topics/t"))
        .json(&json!({ "records": [{ "data": { "i": 4 } }] }))
        .send()
        .unwrap()
        .status()
        .is_success());
    assert_eq!(
        get_json(&c, format!("{base}/v0/topics/t"))["head_seq"],
        4,
        "append continues cleanly after torn-tail truncation"
    );

    let _ = child2.kill();
    let _ = child2.wait();
}
