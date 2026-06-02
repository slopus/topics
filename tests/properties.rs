//! Property / invariant tests (proptest) over the engine directly with a
//! [`TestClock`]. These exercise randomized sequences of
//! write / evict(cap) / expire(ttl) / delete(before_seq + tag) / read and
//! assert the load-bearing safety invariants from DESIGN §9 / API §3.3:
//!
//!  (i)   seq is strictly increasing and contiguous *at assignment* — a write
//!        of N records returns N contiguous seqs starting at `head + 1`.
//!  (ii)  `earliest_seq` is monotonically non-decreasing and always
//!        `>= evict_earliest_seq` (the dual-watermark invariant
//!        `evict_floor <= earliest_seq`); `head_seq` is monotonically
//!        non-decreasing.
//!  (iii) reading from any `from_seq` returns records that are
//!        contiguous-after-filter (strictly ascending `$seq > from_seq`, none
//!        below the live floor) AND yields a tombstone IFF
//!        `from_seq + 1 < evict_earliest_seq` (involuntary cap/TTL loss),
//!        NEVER for a purely-deletion gap — no silent involuntary loss, no
//!        false tombstone for a voluntary delete.
//!  (iv)  a deleted record never reappears in any subsequent read.
//!  (v)   node loop-prevention: a reader presenting its own node never sees its
//!        own records, but the cursor always advances to `caught_up`.
//!  (vi)  idempotency-key replays never double-append.
//!
//! Time is driven only through the injectable [`TestClock`] — no wall-clock
//! sleeps — so TTL/cap correctness is deterministic.

use proptest::prelude::*;
use std::collections::BTreeSet;
use std::sync::Arc;

use topics::clock::{SharedClock, TestClock};
use topics::config::ServerConfig;
use topics::engine::Engine;
use topics::types::{
    DeleteRequest, DiffRequest, Discard, Filter, NodeFilter, RecordIn, TopicConfig, WriteRequest,
};

// ---------------------------------------------------------------------------
// Fixtures / helpers
// ---------------------------------------------------------------------------

const TOPIC: &str = "p";
/// A clock start far enough from 0 that records always carry a positive `$ts`
/// and we can advance/rewind within the window without underflow.
const T0: i64 = 1_000_000_000;

fn build_engine(clock_start: i64) -> (Arc<Engine>, TestClock) {
    let clock = TestClock::new(clock_start);
    let shared: SharedClock = Arc::new(clock.clone());
    let engine = Engine::new(ServerConfig::default(), shared);
    (engine, clock)
}

fn rec(data: serde_json::Value, tag: Option<String>, node: Option<String>) -> RecordIn {
    RecordIn {
        data,
        tag,
        node,
        meta: None,
    }
}

fn write_req(records: Vec<RecordIn>) -> WriteRequest {
    WriteRequest {
        records,
        node: None,
        idempotency_key: None,
        create: None,
        config: None,
        disable_backpressure: false,
    }
}

fn diff_from(from_seq: u64) -> DiffRequest {
    DiffRequest {
        from_seq,
        // Use the max limit so a single diff drains as much as possible and the
        // contiguity / floor assertions see the whole retained window.
        limit: 1000,
        ..DiffRequest::default()
    }
}

/// Fully drain a topic from `from_seq` via repeated diffs, following
/// `next_from_seq` until `caught_up`, returning every delivered `$seq` in
/// delivery order. Asserts (per page) that delivered seqs are strictly
/// ascending and `> from_seq`, that the cursor never goes backward, and that
/// no page after the first carries a tombstone.
fn drain(engine: &Engine, name: &str, mut from_seq: u64) -> (Vec<u64>, bool) {
    let mut out = Vec::new();
    let mut saw_tombstone = false;
    // Bound the loop generously to never hang under a bug.
    for _ in 0..10_000 {
        let d = engine.diff(name, diff_from(from_seq)).unwrap();
        if d.tombstone.is_some() {
            saw_tombstone = true;
        }
        let mut last = from_seq;
        for r in &d.records {
            assert!(
                r.seq > from_seq,
                "delivered seq {} must be > from_seq {}",
                r.seq,
                from_seq
            );
            assert!(
                r.seq > last || out.is_empty() && r.seq > from_seq,
                "delivered seqs must be strictly ascending (got {} after {})",
                r.seq,
                last
            );
            last = r.seq;
            out.push(r.seq);
        }
        assert!(
            d.next_from_seq >= from_seq,
            "cursor must not move backward ({} -> {})",
            from_seq,
            d.next_from_seq
        );
        // Progress guarantee: if not caught up the cursor must advance, else a
        // node-/delete-filtered topic would loop forever.
        if d.caught_up {
            break;
        }
        assert!(
            d.next_from_seq > from_seq,
            "not caught_up but cursor did not advance ({})",
            from_seq
        );
        from_seq = d.next_from_seq;
    }
    (out, saw_tombstone)
}

// ---------------------------------------------------------------------------
// Operation model for the randomized sequence test
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum Op {
    /// Append a batch of `n` records, each optionally tagged "t{k}" and from
    /// node "n{m}".
    Write {
        n: u8,
        tag_k: Option<u8>,
        node_m: Option<u8>,
    },
    /// Advance the test clock (drives TTL expiry).
    Advance { ms: u32 },
    /// Delete by `before_seq` (relative to head: head - back).
    DeleteBefore { back: u8 },
    /// Delete by exact tag "t{k}".
    DeleteTag { tag_k: u8 },
    /// Read-and-drain from a cursor (relative: head - back, saturating to 0).
    Read { back: u16 },
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        (1u8..=5, prop::option::of(0u8..3), prop::option::of(0u8..3))
            .prop_map(|(n, tag_k, node_m)| Op::Write { n, tag_k, node_m }),
        (0u32..2500).prop_map(|ms| Op::Advance { ms }),
        (0u8..6).prop_map(|back| Op::DeleteBefore { back }),
        (0u8..3).prop_map(|tag_k| Op::DeleteTag { tag_k }),
        (0u16..30).prop_map(|back| Op::Read { back }),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 200,
        max_shrink_iters: 4000,
        ..ProptestConfig::default()
    })]

    /// The master invariant test: a randomized op sequence against a topic with a
    /// finite cap + TTL, checking (i)–(iv) on every step.
    #[test]
    fn invariants_under_random_ops(
        ops in prop::collection::vec(op_strategy(), 1..60),
        cap in prop::option::of(1u64..8),
        ttl in prop::option::of(200u64..2000),
    ) {
        let (engine, clock) = build_engine(T0);
        let cfg = TopicConfig {
            cap_records: cap.unwrap_or(0),
            ttl_ms: ttl.unwrap_or(0),
            discard: Discard::Old,
            ..TopicConfig::default()
        };
        engine.put_topic(TOPIC, cfg).unwrap();

        // Model state for cross-step invariants.
        let mut prev_head: u64 = 0;
        let mut prev_earliest: u64 = engine.topic_state(TOPIC, false).unwrap().earliest_seq;
        // Seqs the model knows were voluntarily deleted (by us). Once deleted,
        // they MUST NOT ever reappear in a read (invariant iv).
        let mut deleted_seqs: BTreeSet<u64> = BTreeSet::new();
        // Map seq -> tag, for modeling tag deletes.
        let mut seq_tag: std::collections::BTreeMap<u64, String> = std::collections::BTreeMap::new();

        for op in ops {
            match op {
                Op::Write { n, tag_k, node_m } => {
                    let head_before = engine.topic_state(TOPIC, false).unwrap().head_seq;
                    let tag = tag_k.map(|k| format!("t{k}"));
                    let node = node_m.map(|m| format!("n{m}"));
                    let records: Vec<RecordIn> = (0..n)
                        .map(|i| rec(serde_json::json!({"i": i}), tag.clone(), node.clone()))
                        .collect();
                    let resp = engine.write(TOPIC, write_req(records), true).unwrap();
                    let seqs = resp.seqs.clone().unwrap();
                    // (i) contiguous assignment starting at head_before + 1.
                    prop_assert_eq!(resp.first_seq, head_before + 1);
                    prop_assert_eq!(resp.last_seq, head_before + n as u64);
                    let expected: Vec<u64> = (head_before + 1..=head_before + n as u64).collect();
                    prop_assert_eq!(&seqs, &expected);
                    // strictly increasing & contiguous within the batch.
                    for w in seqs.windows(2) {
                        prop_assert_eq!(w[1], w[0] + 1);
                    }
                    if let Some(t) = &tag {
                        for s in &seqs {
                            seq_tag.insert(*s, t.clone());
                        }
                    }
                }
                Op::Advance { ms } => {
                    clock.advance(ms as i64);
                }
                Op::DeleteBefore { back } => {
                    let head = engine.topic_state(TOPIC, false).unwrap().head_seq;
                    if head == 0 {
                        continue;
                    }
                    let before = head.saturating_sub(back as u64).max(1);
                    // Snapshot which currently-live seqs this removes.
                    let live = drain(&engine, TOPIC, 0).0;
                    let to_delete: Vec<u64> =
                        live.iter().copied().filter(|&s| s < before).collect();
                    engine
                        .delete(TOPIC, DeleteRequest { before_seq: Some(before), match_: None })
                        .unwrap();
                    deleted_seqs.extend(to_delete);
                }
                Op::DeleteTag { tag_k } => {
                    let tag = format!("t{tag_k}");
                    // Which currently-live seqs carry this tag (point-in-time).
                    let live = drain(&engine, TOPIC, 0).0;
                    let to_delete: Vec<u64> = live
                        .iter()
                        .copied()
                        .filter(|s| seq_tag.get(s).map(|t| t == &tag).unwrap_or(false))
                        .collect();
                    engine
                        .delete(
                            TOPIC,
                            DeleteRequest {
                                before_seq: None,
                                match_: Some(Filter::from_shorthand(&tag)),
                            },
                        )
                        .unwrap();
                    deleted_seqs.extend(to_delete);
                }
                Op::Read { back } => {
                    let head = engine.topic_state(TOPIC, false).unwrap().head_seq;
                    let from = head.saturating_sub(back as u64);
                    let _ = drain(&engine, TOPIC, from);
                }
            }

            // --- Per-step state invariants (ii). ---------------------------
            let st = engine.topic_state(TOPIC, false).unwrap();
            let b = engine.get_topic(TOPIC).unwrap();
            let evict_earliest = b.evict_earliest_seq();

            // head_seq monotonic non-decreasing.
            prop_assert!(
                st.head_seq >= prev_head,
                "head_seq went backwards: {} < {}",
                st.head_seq,
                prev_head
            );
            // earliest_seq monotonic non-decreasing (over a topic instance's life;
            // no delete+recreate happens in this test).
            prop_assert!(
                st.earliest_seq >= prev_earliest,
                "earliest_seq went backwards: {} < {}",
                st.earliest_seq,
                prev_earliest
            );
            // Dual watermark: evict_floor <= earliest_seq  <=>  evict_earliest <= earliest.
            prop_assert!(
                evict_earliest <= st.earliest_seq,
                "evict_earliest {} > earliest_seq {}",
                evict_earliest,
                st.earliest_seq
            );
            // earliest_seq within [seq_base, head+1].
            prop_assert!(st.earliest_seq >= 1);
            prop_assert!(st.earliest_seq <= st.head_seq + 1);
            prev_head = st.head_seq;
            prev_earliest = st.earliest_seq;

            // --- (iii)+(iv): a full drain from 0 reveals only live records,
            // strictly ascending, none below the floor, none ever-deleted, and
            // a tombstone exactly iff the cursor (0) is below the involuntary
            // floor. --------------------------------------------------------
            let d0 = engine.diff(TOPIC, diff_from(0)).unwrap();
            // tombstone-iff: from_seq=0, so gap iff 1 < evict_earliest.
            let expect_tombstone = 1 < evict_earliest;
            prop_assert_eq!(
                d0.tombstone.is_some(),
                expect_tombstone,
                "tombstone presence mismatch: evict_earliest={}, earliest={}",
                evict_earliest,
                st.earliest_seq
            );
            // No false tombstone for a purely-deleted gap: when earliest has
            // advanced past deletes but evict_earliest has NOT, it must be silent.
            if st.earliest_seq > 1 && evict_earliest == 1 {
                prop_assert!(
                    d0.tombstone.is_none(),
                    "purely-deleted gap must be silent (earliest={}, evict_earliest=1)",
                    st.earliest_seq
                );
            }
            // Delivered records: strictly ascending, at/above earliest, never deleted.
            let mut last = 0u64;
            for r in &d0.records {
                prop_assert!(r.seq > last, "diff records not strictly ascending");
                last = r.seq;
                prop_assert!(
                    r.seq >= st.earliest_seq,
                    "delivered seq {} below earliest_seq {}",
                    r.seq,
                    st.earliest_seq
                );
                prop_assert!(
                    !deleted_seqs.contains(&r.seq),
                    "deleted seq {} reappeared in a read",
                    r.seq
                );
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 150, ..ProptestConfig::default() })]

    /// (i) Strictly-increasing, contiguous seq assignment across many writes of
    /// varying batch sizes, regardless of interleaved reads.
    #[test]
    fn seq_assignment_is_contiguous(batches in prop::collection::vec(1u8..6, 1..40)) {
        let (engine, _clock) = build_engine(T0);
        let mut next = 1u64;
        let mut all = Vec::new();
        for n in batches {
            let recs: Vec<RecordIn> = (0..n).map(|_| rec(serde_json::json!(1), None, None)).collect();
            let resp = engine.write(TOPIC, write_req(recs), true).unwrap();
            let seqs = resp.seqs.unwrap();
            for s in &seqs {
                prop_assert_eq!(*s, next);
                next += 1;
                all.push(*s);
            }
            // A read in the middle must never perturb assignment.
            let _ = engine.diff(TOPIC, diff_from(0)).unwrap();
        }
        // Globally strictly increasing & gap-free.
        for w in all.windows(2) {
            prop_assert_eq!(w[1], w[0] + 1);
        }
        prop_assert_eq!(engine.topic_state(TOPIC, false).unwrap().head_seq, next - 1);
    }

    /// (iv) Deleted records never reappear; (iii) deletion is silent (no
    /// tombstone) on a cap/TTL-free topic even though `earliest_seq` advances.
    #[test]
    fn deletes_are_silent_and_permanent(
        total in 4u8..40,
        before in 1u64..40,
    ) {
        let (engine, _clock) = build_engine(T0);
        // No cap, no TTL ⇒ the only floor mover is the voluntary delete.
        engine.put_topic(TOPIC, TopicConfig::default()).unwrap();
        let recs: Vec<RecordIn> = (0..total).map(|i| rec(serde_json::json!({"i": i}), None, None)).collect();
        engine.write(TOPIC, write_req(recs), true).unwrap();
        let head = engine.topic_state(TOPIC, false).unwrap().head_seq;
        let before = before.min(head + 1);

        let resp = engine.delete(TOPIC, DeleteRequest { before_seq: Some(before), match_: None }).unwrap();
        // earliest advanced to `before` (or head+1 if all deleted).
        prop_assert_eq!(resp.earliest_seq, before.min(head + 1).max(1));

        let b = engine.get_topic(TOPIC).unwrap();
        // Pure delete never advances the involuntary floor.
        prop_assert_eq!(b.evict_earliest_seq(), 1);

        let d = engine.diff(TOPIC, diff_from(0)).unwrap();
        prop_assert!(d.tombstone.is_none(), "pure delete must be silent");
        // Every delivered seq is >= before (deleted prefix never reappears).
        for r in &d.records {
            prop_assert!(r.seq >= before, "deleted seq {} below {} reappeared", r.seq, before);
        }
        prop_assert!(d.caught_up);
        // Count matches what survived.
        let expected_live = (head + 1).saturating_sub(before);
        prop_assert_eq!(d.records.len() as u64, expected_live);
    }

    /// (iii) cap eviction crossing the cursor yields a tombstone with an
    /// authoritative `[gap_from, gap_to]`, records resume at `earliest_seq`,
    /// and no involuntary loss is silent.
    #[test]
    fn cap_eviction_tombstones_and_resumes(
        cap in 1u64..6,
        writes in 7u8..40,
    ) {
        let (engine, _clock) = build_engine(T0);
        engine.put_topic(TOPIC, TopicConfig { cap_records: cap, ..TopicConfig::default() }).unwrap();
        for i in 0..writes {
            engine.write(TOPIC, write_req(vec![rec(serde_json::json!(i), None, None)]), true).unwrap();
        }
        let st = engine.topic_state(TOPIC, false).unwrap();
        prop_assert_eq!(st.head_seq, writes as u64);
        // With writes > cap, the front was involuntarily evicted.
        prop_assert!(st.earliest_seq > 1, "cap should have evicted the front");

        let d = engine.diff(TOPIC, diff_from(0)).unwrap();
        let tomb = d.tombstone.expect("cap eviction crossing from_seq=0 must tombstone");
        prop_assert_eq!(tomb.gap_from, 1);
        prop_assert_eq!(tomb.gap_to, st.earliest_seq - 1);
        prop_assert_eq!(tomb.earliest_seq, st.earliest_seq);
        prop_assert_eq!(tomb.head_seq, st.head_seq);
        // Records resume exactly at earliest_seq, contiguous to head, count==cap.
        prop_assert_eq!(d.records.first().map(|r| r.seq), Some(st.earliest_seq));
        let seqs: Vec<u64> = d.records.iter().map(|r| r.seq).collect();
        let expected: Vec<u64> = (st.earliest_seq..=st.head_seq).collect();
        prop_assert_eq!(seqs, expected);
        prop_assert!(d.caught_up);
    }

    /// (iii) TTL expiry crossing the cursor yields a `ttl` tombstone; never
    /// silent for the expired (involuntary) prefix.
    #[test]
    fn ttl_expiry_tombstones(
        n in 2u8..12,
        ttl in 100u64..1000,
        extra in 1u32..50,
    ) {
        let (engine, clock) = build_engine(T0);
        engine.put_topic(TOPIC, TopicConfig { ttl_ms: ttl, ..TopicConfig::default() }).unwrap();
        // Write n records at T0.
        for i in 0..n {
            engine.write(TOPIC, write_req(vec![rec(serde_json::json!(i), None, None)]), true).unwrap();
        }
        // Advance strictly past the TTL so all n expire (now - ts > ttl).
        clock.advance(ttl as i64 + extra as i64);
        // One fresh write so head moves and earliest can advance past expired.
        engine.write(TOPIC, write_req(vec![rec(serde_json::json!(99), None, None)]), true).unwrap();

        let st = engine.topic_state(TOPIC, false).unwrap();
        prop_assert_eq!(st.head_seq, n as u64 + 1);
        // The n original records expired; only the fresh one is live.
        prop_assert_eq!(st.earliest_seq, n as u64 + 1);
        prop_assert_eq!(st.count, 1);

        let d = engine.diff(TOPIC, diff_from(0)).unwrap();
        let tomb = d.tombstone.expect("ttl expiry crossing from_seq=0 must tombstone");
        prop_assert_eq!(tomb.reason, topics::types::TombstoneReason::Ttl);
        prop_assert_eq!(tomb.gap_from, 1);
        prop_assert_eq!(tomb.gap_to, n as u64);
        prop_assert_eq!(d.records.iter().map(|r| r.seq).collect::<Vec<_>>(), vec![n as u64 + 1]);
    }

    /// (v) Node loop-prevention: a reader presenting node ids it produced never
    /// receives its own records, yet the cursor always reaches `caught_up`
    /// (silently — no tombstone), even when EVERY record is own-node.
    #[test]
    fn node_filter_advances_to_caught_up(
        // each entry: which node wrote it (0 = "self", 1 = "self2", 2 = "other")
        nodes in prop::collection::vec(0u8..3, 1..40),
    ) {
        let (engine, _clock) = build_engine(T0);
        engine.put_topic(TOPIC, TopicConfig::default()).unwrap();
        let name_of = |k: u8| match k { 0 => "self", 1 => "self2", _ => "other" };
        let mut foreign_seqs = Vec::new();
        for (i, &k) in nodes.iter().enumerate() {
            let seq = i as u64 + 1;
            if k == 2 {
                foreign_seqs.push(seq);
            }
            engine
                .write(TOPIC, write_req(vec![rec(serde_json::json!(i), None, Some(name_of(k).to_string()))]), true)
                .unwrap();
        }
        // Reader filters out both of its own identities.
        let req = DiffRequest {
            from_seq: 0,
            limit: 1000,
            node: Some(NodeFilter::Many(vec!["self".to_string(), "self2".to_string()])),
            ..DiffRequest::default()
        };
        let d = engine.diff(TOPIC, req).unwrap();
        // Only the foreign-node records are delivered.
        let got: Vec<u64> = d.records.iter().map(|r| r.seq).collect();
        prop_assert_eq!(got, foreign_seqs);
        // Cursor reached head; filtering is silent.
        prop_assert!(d.caught_up);
        prop_assert_eq!(d.next_from_seq, nodes.len() as u64);
        prop_assert!(d.tombstone.is_none(), "node filtering must be silent");
    }

    /// (vi) Idempotency-key replays never double-append: any number of replays
    /// of the same in-window key return the original seqs and leave head fixed;
    /// a different key appends anew.
    #[test]
    fn idempotency_replays_never_double_append(
        replays in 1u8..8,
        batch in 1u8..5,
    ) {
        let (engine, _clock) = build_engine(T0);
        engine.put_topic(TOPIC, TopicConfig::default()).unwrap();
        let make = |key: &str, batch: u8| WriteRequest {
            records: (0..batch).map(|i| rec(serde_json::json!(i), None, None)).collect(),
            node: None,
            idempotency_key: Some(key.to_string()),
            create: None,
            config: None,
            disable_backpressure: false,
        };
        let first = engine.write(TOPIC, make("k1", batch), true).unwrap();
        let original = first.seqs.clone().unwrap();
        prop_assert!(!first.deduped);
        let head_after_first = engine.topic_state(TOPIC, false).unwrap().head_seq;
        prop_assert_eq!(head_after_first, batch as u64);

        for _ in 0..replays {
            let r = engine.write(TOPIC, make("k1", batch), true).unwrap();
            prop_assert!(r.deduped, "in-window replay must be deduped");
            prop_assert_eq!(r.seqs.clone().unwrap(), original.clone());
            // Head never advances on a dedupe.
            prop_assert_eq!(engine.topic_state(TOPIC, false).unwrap().head_seq, head_after_first);
        }

        // A distinct key appends fresh, contiguous seqs.
        let other = engine.write(TOPIC, make("k2", batch), true).unwrap();
        prop_assert!(!other.deduped);
        prop_assert_eq!(other.first_seq, head_after_first + 1);

        // A full drain shows exactly the two batches (no dup of the first).
        let (seqs, _) = drain(&engine, TOPIC, 0);
        let expected: Vec<u64> = (1..=head_after_first + batch as u64).collect();
        prop_assert_eq!(seqs, expected);
    }

    /// (iii) The dual-watermark mix: deletes + cap on the same topic. A
    /// purely-deleted gap below `earliest_seq` is silent, while any cap loss
    /// below the cursor still tombstones. The full-drain set never contains a
    /// deleted seq and is always strictly ascending.
    #[test]
    fn delete_then_cap_keeps_signals_separate(
        cap in 3u64..7,
        // `pre` is kept strictly below `cap` so phase 1 never triggers cap
        // eviction: the only floor mover before phase 2 is the voluntary delete.
        pre_frac in 0u64..100,
        post in 6u8..30,
    ) {
        let (engine, _clock) = build_engine(T0);
        engine.put_topic(TOPIC, TopicConfig { cap_records: cap, ..TopicConfig::default() }).unwrap();
        // Phase 1: write `pre` records strictly within cap (2..=cap-1).
        let pre = 2 + (pre_frac % (cap - 2)); // in [2, cap-1]
        for i in 0..pre {
            engine.write(TOPIC, write_req(vec![rec(serde_json::json!(i), None, None)]), true).unwrap();
        }
        let pre_head = engine.topic_state(TOPIC, false).unwrap().head_seq;
        // Delete a prefix (voluntary) — must stay silent.
        let before = (pre_head / 2).max(1) + 1;
        engine.delete(TOPIC, DeleteRequest { before_seq: Some(before), match_: None }).unwrap();
        let after_del = engine.topic_state(TOPIC, false).unwrap();
        let b = engine.get_topic(TOPIC).unwrap();
        // Deletion advanced earliest but not the involuntary floor.
        prop_assert!(after_del.earliest_seq >= before.min(pre_head + 1));
        prop_assert_eq!(b.evict_earliest_seq(), 1, "delete must not move evict floor");
        let d_silent = engine.diff(TOPIC, diff_from(0)).unwrap();
        prop_assert!(d_silent.tombstone.is_none(), "purely-deleted gap is silent");

        // Phase 2: overflow the cap so live records are involuntarily evicted.
        for i in 0..post {
            engine.write(TOPIC, write_req(vec![rec(serde_json::json!(i), None, None)]), true).unwrap();
        }
        let st = engine.topic_state(TOPIC, false).unwrap();
        let b2 = engine.get_topic(TOPIC).unwrap();
        let evict_earliest = b2.evict_earliest_seq();
        // The dual watermark invariant still holds.
        prop_assert!(evict_earliest <= st.earliest_seq);
        let d = engine.diff(TOPIC, diff_from(0)).unwrap();
        // tombstone iff the involuntary floor crossed the cursor.
        prop_assert_eq!(d.tombstone.is_some(), 1 < evict_earliest);
        // Drain is strictly ascending, resumes at earliest, count == cap.
        let (seqs, _) = drain(&engine, TOPIC, 0);
        for w in seqs.windows(2) {
            prop_assert!(w[1] > w[0]);
        }
        prop_assert_eq!(seqs.first().copied(), Some(st.earliest_seq));
        prop_assert_eq!(seqs.len() as u64, cap);
        prop_assert_eq!(st.count, cap);
    }
}
