//! Zero-copy SSE broadcast fan-out (ARCHITECTURE §8.4, Phase-5B Stage 2).
//!
//! When one box has many SSE watchers, each watcher used to independently
//! re-serialize every record it delivered (`serde_json` over the same
//! [`RecordOut`] N times). For a broadcast (1 writer → N watchers on one box)
//! that is N× the serialization cost on the hot read path.
//!
//! This module hands every watcher the **same ref-counted serialized frame**.
//! Each record's `record`-frame JSON body is serialized **once** into an
//! `Arc<RawValue>` and cached in a small per-box ring keyed by `(seq, variant)`,
//! then shared (one `Arc` clone — a refcount bump, no copy) to all watchers. The
//! per-connection envelope (`{box, records, from_seq, to_seq, head_seq}`) and the
//! composite `id:` cursor are still assembled per connection (they depend on the
//! session's cursor map), but the expensive per-record serialization is paid
//! once and amortized across the whole fan-out.
//!
//! The cache is **bounded** (a fixed-capacity ring of the most recently
//! delivered seqs) so a slow or lagging watcher can never grow it without bound;
//! a miss simply re-serializes (correct, just not shared). Eviction of an old
//! seq from the ring drops its `Arc`s — the bytes are freed once the last watcher
//! holding a clone finishes writing it. This is purely a read-path accelerator:
//! it changes no wire output and holds no lock across a socket write.

use crate::types::RecordOut;
use parking_lot::Mutex;
use serde_json::value::RawValue;
use std::collections::VecDeque;
use std::sync::Arc;

/// How many recent seqs to keep serialized per box. Watchers tail near head, so
/// a small ring captures the shared fan-out window; lagging watchers re-serialize
/// (a miss) rather than pinning unbounded memory.
const RING_CAP: usize = 1024;

/// The three projection flags that change a record frame's bytes
/// (`include_data`, `include_tags`, `include_meta`), packed into a 0..8 index so
/// each `(seq, variant)` is cached independently. Watchers sharing a projection
/// (the common broadcast case) all hit the same slot.
#[derive(Clone, Copy)]
pub struct FrameVariant {
    include_data: bool,
    bits: u8,
}

impl FrameVariant {
    pub fn new(include_data: bool, include_tags: bool, include_meta: bool) -> Self {
        FrameVariant {
            include_data,
            bits: (include_data as u8)
                | ((include_tags as u8) << 1)
                | ((include_meta as u8) << 2),
        }
    }
    fn idx(self) -> usize {
        self.bits as usize
    }
}

/// Number of distinct projection variants (2^3).
const N_VARIANTS: usize = 8;

/// One cached record frame: the seq plus a lazily-filled `Arc<RawValue>` per
/// projection variant. The `Arc` is the shared, ref-counted buffer handed to
/// every watcher.
struct CachedFrame {
    seq: u64,
    variants: [Option<Arc<RawValue>>; N_VARIANTS],
}

/// Per-box bounded ring of recently-serialized record frames. Cheap to clone the
/// `Arc<BroadcastCache>` onto each box; the inner ring is mutex-guarded and only
/// touched on the SSE delivery path (never on the write path, so boxes with zero
/// watchers pay nothing).
#[derive(Default)]
pub struct BroadcastCache {
    ring: Mutex<VecDeque<CachedFrame>>,
}

impl BroadcastCache {
    pub fn new() -> Self {
        BroadcastCache {
            ring: Mutex::new(VecDeque::new()),
        }
    }

    /// Get the shared serialized frame for `rec` at `seq` under `variant`,
    /// serializing-and-caching once on a miss. Returns an `Arc` clone (a refcount
    /// bump) so N watchers share one buffer.
    pub fn frame(&self, seq: u64, rec: &RecordOut, variant: FrameVariant) -> Arc<RawValue> {
        let v = variant.idx();
        {
            let ring = self.ring.lock();
            // The ring is ordered by seq ascending (push_back on delivery); a
            // small linear scan from the back finds a near-head hit fast.
            if let Some(found) = ring.iter().rev().find(|f| f.seq == seq) {
                if let Some(arc) = &found.variants[v] {
                    return arc.clone();
                }
            }
        }

        // Miss: serialize once (outside the lock), then publish into the ring.
        let arc: Arc<RawValue> = serialize_frame(rec, variant.include_data);

        let mut ring = self.ring.lock();
        // Re-check: another watcher may have filled it while we serialized.
        if let Some(found) = ring.iter_mut().rev().find(|f| f.seq == seq) {
            if let Some(existing) = &found.variants[v] {
                return existing.clone();
            }
            found.variants[v] = Some(arc.clone());
            return arc;
        }
        // New seq: keep the ring seq-ordered and bounded.
        let mut variants: [Option<Arc<RawValue>>; N_VARIANTS] = Default::default();
        variants[v] = Some(arc.clone());
        // Common case: seq is the new max ⇒ push_back. Otherwise insert in order.
        if ring.back().map(|f| f.seq < seq).unwrap_or(true) {
            ring.push_back(CachedFrame { seq, variants });
        } else {
            let pos = ring.partition_point(|f| f.seq < seq);
            ring.insert(pos, CachedFrame { seq, variants });
        }
        while ring.len() > RING_CAP {
            ring.pop_front();
        }
        arc
    }
}

/// Serialize one record frame body to a shared `Arc<RawValue>`, **byte-identical**
/// to [`record_frame`](crate::http::watch::record_frame) (which the watch loop
/// used per-connection). It builds the same sorted `serde_json::Map`
/// (`$seq,$ts,$node,$tag,data,meta`, sorted on serialization) and serializes it
/// once. `rec` is already projected for `include_tags`/`include_meta` by
/// `record_out`; `include_data` gates the `data` field here, matching the
/// original.
fn serialize_frame(rec: &RecordOut, include_data: bool) -> Arc<RawValue> {
    let val = crate::http::watch::record_frame(rec, include_data);
    // `to_string` then `from_string` is the supported path to a `RawValue`
    // (its serializer round-trips through the textual form). Errors are
    // impossible for a well-formed JSON object; fall back to `null` defensively.
    let s = serde_json::to_string(&val).unwrap_or_else(|_| "null".to_string());
    let boxed: Box<RawValue> = RawValue::from_string(s).unwrap_or_else(|_| {
        RawValue::from_string("null".to_string()).expect("null is valid json")
    });
    Arc::from(boxed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn rec(seq: u64) -> RecordOut {
        RecordOut {
            seq,
            ts: 1234,
            node: Some("n1".to_string()),
            tag: Some("t".to_string()),
            type_: None,
            data: json!({"k": "v"}),
            meta: Some(json!({"m": 1})),
        }
    }

    #[test]
    fn shared_frame_is_byte_identical_to_record_frame() {
        let cache = BroadcastCache::new();
        let r = rec(7);
        let variant = FrameVariant::new(true, true, true);
        let shared = cache.frame(7, &r, variant);
        // Must match the per-connection path's bytes exactly (sorted keys).
        let expected = crate::http::watch::record_frame(&r, true).to_string();
        assert_eq!(shared.get(), expected);
    }

    #[test]
    fn second_call_returns_same_shared_arc() {
        let cache = BroadcastCache::new();
        let r = rec(3);
        let variant = FrameVariant::new(true, false, false);
        let a = cache.frame(3, &r, variant);
        let b = cache.frame(3, &r, variant);
        // Same backing allocation ⇒ the buffer is shared, not re-serialized.
        assert!(Arc::ptr_eq(&a, &b));
        // A different projection variant is cached independently.
        let c = cache.frame(3, &r, FrameVariant::new(false, false, false));
        assert!(!Arc::ptr_eq(&a, &c));
    }

    #[test]
    fn ring_is_bounded() {
        let cache = BroadcastCache::new();
        let variant = FrameVariant::new(true, true, true);
        for seq in 1..=(RING_CAP as u64 + 50) {
            let _ = cache.frame(seq, &rec(seq), variant);
        }
        assert!(cache.ring.lock().len() <= RING_CAP);
        // The oldest seqs were evicted (front-dropped).
        assert!(cache.ring.lock().front().map(|f| f.seq > 1).unwrap_or(false));
    }
}
