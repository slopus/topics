//! R6 — the segment seal fsync runs OFF the publish gate, so a slow seal `put`
//! does not serialize same-topic writers.
//!
//! Stage-2 moved the seal+fsync off the per-topic publish gate: `publish_staged`
//! advances `head_seq` (makes records visible, wakes readers) and releases the gate
//! FIRST, then materializes/seals the segment. This test proves the structural
//! property directly: with a segment store whose durable `put` is BLOCKED, the
//! topic's published records are already VISIBLE (head advanced, records readable)
//! while the seal is still blocked — i.e. visibility never waits on the seal.
//!
//! The seal runs through the real `SegmentWriter` + `LocalSegmentStore` over an
//! injected `Fs` whose `rename` (the atomic-publish step of the segment `put`)
//! blocks until released, so the seal is deterministically stalled with no sleeps.
//!
//! ```text
//! cargo test --features test-fs --test integration_seal_offgate
//! ```

#![cfg(feature = "test-fs")]

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use serde_json::json;
use topics::clock::{SharedClock, TestClock};
use topics::config::SegmentConfig;
use topics::engine::segwriter::SegmentWriter;
use topics::engine::topic_state::{StoredRecord, TopicState};
use topics::storage::{File, Fs, LocalSegmentStore, OpenOpts, RealFs, TopicTier};
use topics::types::TopicConfig;

/// An `Fs` wrapper that BLOCKS the first `rename` whose destination is a segment
/// `.data`/`.idx` file (the atomic-publish step of a segment `put`/seal) until
/// `release()` is called. Every other op forwards to the inner `RealFs`. This lets
/// a test stall a seal deterministically (no sleeps) and observe that the topic's
/// published records are visible while the seal is parked.
struct BlockingSealFs {
    inner: Arc<dyn Fs>,
    gate: Mutex<bool>,
    cv: Condvar,
    blocked_once: AtomicBool,
    seal_entered: AtomicBool,
}

impl BlockingSealFs {
    fn new(inner: Arc<dyn Fs>) -> Arc<Self> {
        Arc::new(BlockingSealFs {
            inner,
            gate: Mutex::new(false),
            cv: Condvar::new(),
            blocked_once: AtomicBool::new(false),
            seal_entered: AtomicBool::new(false),
        })
    }
    fn release(&self) {
        *self.gate.lock().unwrap() = true;
        self.cv.notify_all();
    }
    fn seal_has_entered(&self) -> bool {
        self.seal_entered.load(Ordering::SeqCst)
    }
}

fn is_segment_target(to: &Path) -> bool {
    to.extension()
        .and_then(|e| e.to_str())
        .map(|e| e == "data" || e == "idx")
        .unwrap_or(false)
}

impl Fs for BlockingSealFs {
    fn open(&self, path: &Path, opts: OpenOpts) -> io::Result<Box<dyn File>> {
        self.inner.open(path, opts)
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        // Block the FIRST segment-publishing rename until released.
        if is_segment_target(to) && !self.blocked_once.swap(true, Ordering::SeqCst) {
            self.seal_entered.store(true, Ordering::SeqCst);
            let mut g = self.gate.lock().unwrap();
            while !*g {
                g = self.cv.wait(g).unwrap();
            }
        }
        self.inner.rename(from, to)
    }
    fn remove_file(&self, path: &Path) -> io::Result<()> {
        self.inner.remove_file(path)
    }
    fn read_dir(&self, dir: &Path) -> io::Result<Vec<PathBuf>> {
        self.inner.read_dir(dir)
    }
    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        self.inner.sync_dir(dir)
    }
    fn create_dir_all(&self, dir: &Path) -> io::Result<()> {
        self.inner.create_dir_all(dir)
    }
    fn exists(&self, path: &Path) -> bool {
        self.inner.exists(path)
    }
    fn metadata_len(&self, path: &Path) -> io::Result<u64> {
        self.inner.metadata_len(path)
    }
}

fn rec(i: u64) -> StoredRecord {
    StoredRecord {
        ts: 1_700_000_000_000 + i as i64,
        node: None,
        tag: None,
        data: json!({ "i": i }),
        meta: None,
        bytes: 16,
        deleted: false,
        payload_resident: true,
        hops: 0,
    }
}

#[test]
fn slow_seal_does_not_block_same_topic_visibility() {
    let tmp = tempfile::tempdir().unwrap();
    let fs = BlockingSealFs::new(RealFs::arc());

    // A real SegmentWriter that seals after EVERY record (max_events:1), over a hot
    // store backed by the blocking fs. So the 2nd publish triggers a seal whose
    // segment `put` (rename) blocks.
    let clock: SharedClock = Arc::new(TestClock::new(1_000_000));
    let hot = LocalSegmentStore::open_with(tmp.path().join("topics/1"), fs.clone() as Arc<dyn Fs>)
        .expect("hot store opens through the blocking fs");
    let tier = Arc::new(TopicTier::new(Box::new(hot), None));
    let cfg = SegmentConfig {
        max_events: 1,
        max_bytes: 0,
        max_age_ms: 0,
        hot_retain_segments: u64::MAX,
        hot_retain_bytes: 0,
    };
    let writer = SegmentWriter::new(tier, cfg, clock.clone());

    let mut bs = TopicState::new("d".to_string(), 1, TopicConfig::default(), 1, 0);
    bs.attach_segwriter(writer);
    let b = Arc::new(bs);

    // Publish record 1 (active segment, no seal yet), then publish record 2: with
    // max_events:1 the second append seals segment-1 (record 1) before accepting
    // record 2 — and that seal's segment rename is the FIRST ⇒ BLOCKED. Run the
    // second publish on a worker so the main thread can observe visibility while the
    // seal is parked.
    {
        let staged = {
            let _g = b.append_lock.lock();
            b.stage_append(vec![rec(1)])
        };
        let range = b.publish_staged_no_seal(staged, 1_000_000).unwrap();
        b.materialize_published(range.0, range.1); // record 1 active, no seal.
    }
    assert_eq!(b.head_seq(), 1, "record 1 published");

    let b1 = b.clone();
    let seal_worker = std::thread::spawn(move || {
        let staged = {
            let _g = b1.append_lock.lock();
            b1.stage_append(vec![rec(2)])
        };
        // publish_staged_no_seal advances head to 2 + wakes readers (gate released),
        // THEN materialize_published seals segment-1 — parking in the blocked rename.
        let range = b1.publish_staged_no_seal(staged, 1_000_000).unwrap();
        b1.materialize_published(range.0, range.1);
    });

    // Wait (bounded) until the seal has actually entered the blocked rename.
    let deadline = Instant::now() + Duration::from_secs(5);
    while !fs.seal_has_entered() {
        assert!(
            Instant::now() < deadline,
            "seal never reached the blocking rename"
        );
        std::thread::yield_now();
    }

    // THE R6 PROPERTY: while the seal is parked, record 2 is ALREADY visible — head
    // advanced and the records are readable. Visibility did not wait on the seal
    // fsync (the gate / head advance happened first, the seal runs off-gate).
    assert_eq!(
        b.head_seq(),
        2,
        "head advanced to 2 before the seal completed (off-gate)"
    );
    assert!(
        b.forward_lookup(2).expect("record 2 present").1,
        "record 2 is live/visible while record 1's seal is still blocked"
    );

    // Release the seal and let the worker finish cleanly.
    fs.release();
    seal_worker.join().unwrap();
    assert_eq!(b.head_seq(), 2, "head stable after the seal completes");
}
