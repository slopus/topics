//! The materialized lease projection for a queue topic (DESIGN §10,
//! ARCHITECTURE §12).
//!
//! A queue is **two logs**: the jobs log (the topic's own [`TopicIndex`]) and an
//! append-only **leases log** of lifecycle events. The pending who-holds-what
//! state is the **materialized projection** of the leases log — held here in
//! memory and rebuilt on restart by replaying whatever lease events survived
//! (none, for the default non-durable leases log ⇒ every in-flight job becomes
//! claimable again, the self-healing visibility timeout, DESIGN §10.6).
//!
//! All time decisions (lease deadlines, the jitter window, delayed nacks) read
//! the [`Clock`](crate::clock::Clock); no wall-clock sleep is load-bearing.
//!
//! [`TopicIndex`]: crate::engine::topic_state::TopicIndex

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};

/// One active lease in the projection: who holds a job seq, until when.
#[derive(Debug, Clone)]
pub struct Lease {
    /// The holder node.
    pub node: String,
    /// Opaque lease identity for this delivery (monotonic per topic).
    pub lease_id: u64,
    /// Absolute deadline ms; the job is reclaimable once `now > deadline`.
    pub deadline_ms: i64,
    /// `Some(conn)` when delivered over a `/work` SSE stream (release-on-disconnect
    /// is keyed to this connection); `None` for poll-claims. Stage 2 only stores
    /// it; the SSE wiring lands with the HTTP layer.
    pub by_work_conn: Option<u64>,
}

/// The in-memory materialized lease view + reclaim/claim scheduling state. Lives
/// under the topic's `queue` mutex; one batched cohort claim pass holds it for a
/// single critical section (DESIGN §10.3, ARCHITECTURE §12).
#[derive(Debug)]
pub struct QueueProjection {
    /// `seq -> active lease` (the materialized who-holds-what view).
    pub leases: HashMap<u64, Lease>,
    /// `seq -> delivery count` (the dead-letter trigger; survives reclaim).
    pub deliveries: HashMap<u64, u64>,
    /// Reclaim freelist: seqs whose lease expired or whose nack `delay_ms`
    /// elapsed — a min-heap, **drained first** so reclaimed work jumps ahead of
    /// never-delivered work (bounding redelivery latency).
    pub reclaim: BinaryHeap<Reverse<u64>>,
    /// Delayed nacks: `(ready_at_ms, seq)` — a min-heap by `ready_at`. Swept into
    /// `reclaim` once `ready_at <= now` on the next claim pass.
    pub delayed: BinaryHeap<Reverse<(i64, u64)>>,
    /// Monotonic claim cursor over the jobs log: the next never-yet-leased seq to
    /// hand out once the freelist is empty (the fresh-job source).
    pub claim_cursor: u64,
    /// Cumulative jobs moved to the dead-letter topic (observability §10.7).
    pub dead_lettered: u64,
    /// Monotonic lease-id allocator (per topic instance).
    next_lease_id: u64,
    /// Seqs currently sitting on `reclaim`, so we never double-enqueue a seq.
    in_reclaim: std::collections::HashSet<u64>,
}

impl QueueProjection {
    /// A fresh projection for a queue whose first job seq is `seq_base`.
    pub fn new(seq_base: u64) -> Self {
        QueueProjection {
            leases: HashMap::new(),
            deliveries: HashMap::new(),
            reclaim: BinaryHeap::new(),
            delayed: BinaryHeap::new(),
            claim_cursor: seq_base,
            dead_lettered: 0,
            next_lease_id: 1,
            in_reclaim: std::collections::HashSet::new(),
        }
    }

    /// Allocate the next opaque lease id.
    pub fn alloc_lease_id(&mut self) -> u64 {
        let id = self.next_lease_id;
        self.next_lease_id += 1;
        id
    }

    /// Push a seq onto the reclaim freelist (idempotent — never double-enqueues).
    pub fn push_reclaim(&mut self, seq: u64) {
        if self.in_reclaim.insert(seq) {
            self.reclaim.push(Reverse(seq));
        }
    }

    /// Pop the lowest reclaimable seq, if any.
    pub fn pop_reclaim(&mut self) -> Option<u64> {
        let Reverse(seq) = self.reclaim.pop()?;
        self.in_reclaim.remove(&seq);
        Some(seq)
    }

    /// Schedule a delayed reclaim: the seq becomes claimable at `ready_at_ms`.
    pub fn push_delayed(&mut self, ready_at_ms: i64, seq: u64) {
        self.delayed.push(Reverse((ready_at_ms, seq)));
    }

    /// Sweep expired leases and elapsed delayed-nacks onto the reclaim freelist
    /// (DESIGN §10.3/§10.6 — lazy, no per-job timers). Called at the start of
    /// every claim pass with the current `now`.
    pub fn sweep_expired(&mut self, now_ms: i64) {
        // Delayed nacks whose `ready_at` has elapsed.
        while let Some(&Reverse((ready_at, seq))) = self.delayed.peek() {
            if ready_at > now_ms {
                break;
            }
            self.delayed.pop();
            // Only reclaim if the seq isn't currently re-leased (a delayed nack
            // could in theory be superseded; the lease map is authoritative).
            if !self.leases.contains_key(&seq) {
                self.push_reclaim(seq);
            }
        }
        // Expired leases: deadline passed ⇒ reclaimable. Collect first to avoid
        // mutating `leases` while iterating it.
        let expired: Vec<u64> = self
            .leases
            .iter()
            .filter(|(_, l)| now_ms > l.deadline_ms)
            .map(|(s, _)| *s)
            .collect();
        for seq in expired {
            self.leases.remove(&seq);
            self.push_reclaim(seq);
        }
    }

    /// Count of jobs with an active (un-expired) lease at `now` (§10.7
    /// `in_flight`). Expired leases are not counted (they are logically
    /// reclaimable even before the next sweep records it).
    pub fn in_flight(&self, now_ms: i64) -> u64 {
        self.leases
            .values()
            .filter(|l| now_ms <= l.deadline_ms)
            .count() as u64
    }

    /// Re-apply a replayed leases-log event during recovery (DESIGN §10.1): fold
    /// the event into the materialized projection exactly as the live path would.
    /// `event` is the [`crate::storage::LeaseEvent`] discriminant byte.
    pub fn apply_lease_event(
        &mut self,
        event: u8,
        seq: u64,
        node: String,
        lease_id: u64,
        deadline_ms: i64,
        deliveries: u64,
    ) {
        // Keep the lease-id allocator ahead of any replayed id.
        if lease_id >= self.next_lease_id {
            self.next_lease_id = lease_id + 1;
        }
        // Whether the lease currently materialized for `seq` was the one this
        // event acted on (codex P1 #2): a Released/Extended/Acked event must only
        // mutate the projection when its `lease_id` matches the CURRENT lease, so a
        // delayed/out-of-order STALE event (from a prior delivery) can never clear
        // or extend a NEWER recovered lease. WAL order is the live event order, but
        // a torn/partial tail or an interleaved re-claim+release could otherwise let
        // an older release frame replay over a newer claim's lease.
        let current_lease_matches = |q: &Self| -> bool {
            q.leases
                .get(&seq)
                .map(|l| l.lease_id == lease_id)
                .unwrap_or(false)
        };
        match event {
            // claimed
            0 => {
                self.deliveries.insert(seq, deliveries);
                self.leases.insert(
                    seq,
                    Lease {
                        node,
                        lease_id,
                        deadline_ms,
                        by_work_conn: None,
                    },
                );
                // A fresh claim implies the claim cursor passed this seq.
                if seq >= self.claim_cursor {
                    self.claim_cursor = seq + 1;
                }
            }
            // released (nack / expiry) — only if it targets the current lease.
            1 => {
                if current_lease_matches(self) {
                    self.leases.remove(&seq);
                    self.push_reclaim(seq);
                }
            }
            // extended — only the current lease's deadline moves.
            2 => {
                if let Some(l) = self.leases.get_mut(&seq) {
                    if l.lease_id == lease_id {
                        l.deadline_ms = deadline_ms;
                    }
                }
            }
            // acked (job is deleted from the jobs log; drop all lease state) — only
            // if it acks the current lease. A stale ack for an old delivery must not
            // drop a newer lease's state.
            3 if current_lease_matches(self) => {
                self.leases.remove(&seq);
                self.deliveries.remove(&seq);
            }
            _ => {}
        }
    }
}

// ===========================================================================
// Engine queue API (DESIGN §10, API §10) — claim / ack / nack / extend +
// observability. The leases log is the topic's WAL stream (lease frames); the
// projection above is its materialized view.
// ===========================================================================

use crate::config;
use crate::engine::segwriter::SealedResolve;
use crate::engine::topic_state::TopicState;
use crate::engine::Engine;
use crate::error::{Error, Result};
use crate::storage::{LeaseEvent, WalRecord};
use crate::types::*;
use std::sync::Arc;
use std::time::Instant;

/// One claimer in a cohort (node identity + the max it wants).
#[derive(Debug, Clone)]
pub struct Claimer {
    pub node: String,
    pub max: u32,
    /// `Some(conn)` when this claimer is a `/work` SSE stream (release-on-disconnect
    /// is keyed to the connection); `None` for a poll-claim.
    pub work_conn: Option<u64>,
}

/// A job leased during a claim pass (engine-internal; the HTTP layer projects it
/// onto the wire [`ClaimedJob`]).
#[derive(Debug, Clone)]
pub struct LeasedJob {
    pub seq: u64,
    pub lease_id: u64,
    pub deadline: i64,
    pub ts: i64,
    pub tag: Option<String>,
    pub deliveries: u64,
    pub data: serde_json::Value,
    pub meta: Option<serde_json::Value>,
}

/// Wall-time elapsed since `start`, in fractional milliseconds.
fn elapsed_ms(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}

impl Engine {
    /// Resolve a topic that MUST be a queue, returning `404 topic_not_found` if
    /// absent or `409 not_a_queue` if it is a plain log (API §10).
    fn get_queue(&self, name: &str) -> Result<Arc<TopicState>> {
        let b = self
            .get_topic(name)
            .ok_or_else(|| Error::topic_not_found(name))?;
        if !b.is_queue() {
            return Err(Error::not_a_queue(name));
        }
        Ok(b)
    }

    /// Push a job seq back onto the reclaim freelist so it becomes claimable
    /// again. Used to recover a job whose ack/dead-letter durable delete failed
    /// after its lease was already dropped (codex HIGH #4) — without this the seq
    /// is below the advanced claim cursor and off the freelist, so it would never
    /// be handed out again.
    fn reclaim_seq(&self, b: &TopicState, seq: u64) {
        if let Some(q) = &b.queue {
            q.lock().push_reclaim(seq);
        }
    }

    /// Compute the live `queue` counters (§10.7) for a queue topic at `now`.
    pub(crate) fn queue_counters(&self, b: &TopicState, now: i64) -> QueueState {
        let Some(q) = &b.queue else {
            return QueueState {
                ready: 0,
                in_flight: 0,
                dead_lettered: 0,
            };
        };
        if let Err(e) = self.enforce_retention_durable(b, now) {
            tracing::warn!(
                topic = %b.name, error = %e,
                "queue counters: retention harden failed; using prior floor"
            );
        }
        let mut q = q.lock();
        q.sweep_expired(now);
        let (ready, in_flight) = self.ready_in_flight_locked(b, &mut q, now);
        let dead_lettered = q.dead_lettered;
        QueueState {
            ready,
            in_flight,
            dead_lettered,
        }
    }

    /// Compute `(ready, in_flight)` in O(active-leases + pending-delayed) — the
    /// queue's working set — rather than O(retained jobs). Phase-5A's
    /// `count_ready` scanned every retained seq; this derives the same value from
    /// the topic's maintained `live_count` (records present and not deleted, net of
    /// cap/TTL eviction and ack/dead-letter deletes):
    ///
    /// ```text
    /// ready = live_count - in_flight_live - delayed_pending_live
    /// ```
    ///
    /// `in_flight_live` is the count of un-expired leases whose job is still
    /// present (a leased seq that was evicted/deleted out from under its lease no
    /// longer counts); `delayed_pending_live` is the count of delayed-nack entries
    /// whose `ready_at` is still in the future and whose job is still present.
    /// Both are bounded by the active working set, never by the log length.
    /// Must be called with `q` already swept (so every remaining lease is
    /// un-expired and every elapsed delayed entry has been promoted to `reclaim`).
    fn ready_in_flight_locked(
        &self,
        b: &TopicState,
        q: &mut QueueProjection,
        now: i64,
    ) -> (u64, u64) {
        let index = b.index.read();
        let is_live = |seq: u64| index.get(seq).map(|r| !r.deleted).unwrap_or(false);

        // In-flight = un-expired leases still backed by a live job. Post-sweep
        // every lease is un-expired, so the `deadline` check only guards a clock
        // that moved between sweep and here.
        let in_flight = q
            .leases
            .iter()
            .filter(|(seq, l)| now <= l.deadline_ms && is_live(**seq))
            .count() as u64;
        // All leases (including any whose job is gone) are excluded from ready.
        let leased_live = q.leases.keys().copied().filter(|&s| is_live(s)).count() as u64;
        // Delayed-nack entries still pending (ready_at in the future) and live.
        let delayed_live = q
            .delayed
            .iter()
            .filter(|Reverse((ready_at, seq))| *ready_at > now && is_live(*seq))
            .count() as u64;
        drop(index);

        let live = b.live_count.load(std::sync::atomic::Ordering::Relaxed);
        let ready = live
            .saturating_sub(leased_live)
            .saturating_sub(delayed_live);
        (ready, in_flight)
    }

    /// `POST /v0/topics/:q/claim` — lease up to `max` claimable jobs to `node`
    /// (the greedy, single-claimer path used when `claim_jitter_ms == 0`).
    /// Returns the leased jobs ascending by seq (DESIGN §10.2).
    pub fn claim(
        &self,
        name: &str,
        node: &str,
        max: u32,
        lease_ms: Option<u64>,
    ) -> Result<ClaimResponse> {
        let start = Instant::now();
        if node.is_empty() {
            return Err(Error::invalid_request("claim requires a non-empty `node`"));
        }
        if node.len() > config::MAX_NODE_BYTES {
            return Err(Error::invalid_request("node too long"));
        }
        // Read-path catch-up: drain routers feeding this queue so records
        // forwarded by a router are claimable on an immediate claim after a source
        // write (read-your-writes).
        self.catch_up_dest(name);
        let b = self.get_queue(name)?;
        let claimers = vec![Claimer {
            node: node.to_string(),
            max: max.clamp(1, config::MAX_CLAIM),
            work_conn: None,
        }];
        let (mut results, ready) = self.run_claim_cohort(&b, &claimers, lease_ms)?;
        let jobs = results.pop().unwrap_or_default();
        let claimed: Vec<ClaimedJob> = jobs
            .into_iter()
            .map(|j| ClaimedJob {
                seq: j.seq,
                lease_id: format!("lease_{:x}", j.lease_id),
                deadline: j.deadline,
                ts: j.ts,
                tag: j.tag,
                deliveries: j.deliveries,
                data: j.data,
                meta: j.meta,
            })
            .collect();
        let count = claimed.len() as u64;
        Ok(ClaimResponse {
            topic_name: name.to_string(),
            claimed,
            count,
            ready,
            performance: Performance::with_total(elapsed_ms(start)),
        })
    }

    /// The **coalescing-window** claim entry point (DESIGN §10.3): serve a whole
    /// cohort of claimers in ONE batched coordinator pass, dividing the available
    /// jobs **evenly** across the cohort (round-robin, proportional to each
    /// `max`). Returns per-claimer leased-job lists (parallel to `claimers`) plus
    /// the post-pass `ready` count. The HTTP layer gathers the cohort over the
    /// Clock-driven jitter window; this method is the single critical section.
    pub fn claim_cohort(
        &self,
        name: &str,
        claimers: &[Claimer],
        lease_ms: Option<u64>,
    ) -> Result<(Vec<Vec<LeasedJob>>, u64)> {
        // Read-path catch-up: forwarded records are claimable read-your-writes.
        self.catch_up_dest(name);
        let b = self.get_queue(name)?;
        self.run_claim_cohort(&b, claimers, lease_ms)
    }

    /// The single batched coordinator pass over the queue (DESIGN §10.3,
    /// ARCHITECTURE §12). Holds the queue lock for one critical section: sweep
    /// expired leases, gather the available set (reclaim freelist drained first,
    /// then fresh seqs from the claim cursor), apply dead-lettering, then divide
    /// the survivors evenly across the cohort (round-robin proportional to each
    /// `max`). Records a `claimed` lease event per lease.
    fn run_claim_cohort(
        &self,
        b: &Arc<TopicState>,
        claimers: &[Claimer],
        lease_ms: Option<u64>,
    ) -> Result<(Vec<Vec<LeasedJob>>, u64)> {
        let now = self.clock.now_ms();
        self.enforce_retention_durable(b, now)?;

        let cfg = b.config.read();
        let topic_lease_ms = cfg.lease_ms;
        let max_deliveries = cfg.max_deliveries;
        let dead_letter = cfg.dead_letter.clone();
        // An `ephemeral` queue is resident-only; its jobs are lost on restart, so
        // lease frames would be ghosts. Only persistent queues log durable lease events.
        let leases_durable = cfg.leases_durable && cfg.uses_persistent_record_store();
        let topic_id = b.topic_id;
        drop(cfg);

        let effective_lease = lease_ms
            .unwrap_or(topic_lease_ms)
            .clamp(config::MIN_LEASE_MS, config::MAX_LEASE_MS) as i64;
        let deadline = now.saturating_add(effective_lease);

        // Total demand across the cohort (each claimer clamped to MAX_CLAIM).
        let demands: Vec<u32> = claimers
            .iter()
            .map(|c| c.max.clamp(1, config::MAX_CLAIM))
            .collect();
        let total_demand: u64 = demands.iter().map(|&m| m as u64).sum();

        let mut out: Vec<Vec<LeasedJob>> = vec![Vec::new(); claimers.len()];
        // Jobs to dead-letter (resolved + deleted after releasing the lock).
        let mut to_dead_letter: Vec<u64> = Vec::new();
        // (claimer_idx, seq, lease_id, deliveries) for the lease frames we log.
        let mut lease_events: Vec<(usize, u64, u64, u64)> = Vec::new();
        // (claimer_idx, job_idx_in_out, seq) for leased jobs whose payload was
        // freed after sealing (Phase 6) — resolved from the segment off the lock.
        let mut to_resolve: Vec<(usize, usize, u64)> = Vec::new();

        {
            let mut q = b.queue.as_ref().expect("queue projection").lock();
            q.sweep_expired(now);

            let head = b.head_seq();
            let index = b.index.read();

            // The per-claimer remaining demand; we hand out round-robin.
            let mut remaining: Vec<u32> = demands.clone();
            let mut total_remaining = total_demand;

            // Round-robin index into the cohort.
            let mut rr = 0usize;

            // Pull the next available, live, claimable seq: reclaim freelist first
            // (drained ahead of fresh jobs), then fresh seqs from the claim cursor.
            // Returns `None` when no more work is available.
            //
            // Dead-letter check happens here so a job over `max_deliveries` is
            // diverted instead of leased.
            loop {
                if total_remaining == 0 {
                    break;
                }
                // Find a claimer with remaining demand (round-robin).
                let mut tries = 0;
                while remaining[rr] == 0 && tries < claimers.len() {
                    rr = (rr + 1) % claimers.len();
                    tries += 1;
                }
                if remaining[rr] == 0 {
                    break; // no claimer wants more.
                }

                // Acquire the next claimable seq.
                let Some(seq) = next_claimable(&mut q, &index, head) else {
                    break; // queue (near-)empty.
                };

                // Delivery count *if claimed now* (the (n+1)-th delivery).
                let prev_deliveries = *q.deliveries.get(&seq).unwrap_or(&0);
                let this_delivery = prev_deliveries + 1;

                // Dead-letter when delivering past max_deliveries (DESIGN §10.7).
                if max_deliveries > 0 && this_delivery > max_deliveries && dead_letter.is_some() {
                    to_dead_letter.push(seq);
                    q.deliveries.remove(&seq);
                    q.dead_lettered += 1;
                    continue; // not leased; try the next seq.
                }

                // Lease it to the chosen claimer.
                let lease_id = q.alloc_lease_id();
                q.deliveries.insert(seq, this_delivery);
                q.leases.insert(
                    seq,
                    Lease {
                        node: claimers[rr].node.clone(),
                        lease_id,
                        deadline_ms: deadline,
                        by_work_conn: claimers[rr].work_conn,
                    },
                );

                let rec = index.get(seq);
                let (ts, tag, data, meta, resident) = match rec {
                    Some(r) => (
                        r.ts,
                        r.tag.clone(),
                        r.data.clone(),
                        r.meta.clone(),
                        r.payload_resident,
                    ),
                    None => (now, None, serde_json::Value::Null, None, true),
                };
                // A sealed job's payload is no longer resident; remember where in
                // `out` it landed so we can resolve it from the segment AFTER the
                // index/queue locks are released (never a segment read under lock).
                if !resident {
                    to_resolve.push((rr, out[rr].len(), seq));
                }
                out[rr].push(LeasedJob {
                    seq,
                    lease_id,
                    deadline,
                    ts,
                    tag,
                    deliveries: this_delivery,
                    data,
                    meta,
                });
                lease_events.push((rr, seq, lease_id, this_delivery));

                remaining[rr] -= 1;
                total_remaining -= 1;
                rr = (rr + 1) % claimers.len();
            }
            drop(index);
            drop(q);
        }

        // Resolve the payloads of any sealed (non-resident) leased jobs from the
        // segment writer, now that the index + queue locks are released (the HARD
        // INVARIANT: never a segment read while holding a lock that gates writes).
        // `resolve_sealed_fast` + an off-lock `read_locator` keeps even a slow COLD
        // fetch off every write-gating lock.
        for (rr, job_idx, seq) in &to_resolve {
            if let Some(sw) = b.segwriter.as_ref() {
                let resolve = sw.lock().resolve_sealed_fast(*seq);
                let p = match resolve {
                    SealedResolve::Hit(p) => Some(p),
                    SealedResolve::Read(loc) => crate::engine::segwriter::read_locator(&loc)
                        .inspect(|p| sw.lock().record_cold_read(&loc, p)),
                    SealedResolve::NotSealed => None,
                };
                if let Some(p) = p {
                    let job = &mut out[*rr][*job_idx];
                    job.data = p.data;
                    job.meta = p.meta;
                }
            }
        }

        // Dead-letter the diverted jobs (append to the DL topic + permanent delete
        // from the jobs log), outside the queue lock.
        if !to_dead_letter.is_empty() {
            if let Some(dl) = &dead_letter {
                self.dead_letter_jobs(b, dl, &to_dead_letter, max_deliveries, now);
            }
        }

        // Append `claimed` events to the leases log (durable iff leases_durable).
        for (idx, seq, lease_id, deliveries) in &lease_events {
            self.log_lease_event(
                topic_id,
                *seq,
                LeaseEvent::Claimed,
                &claimers[*idx].node,
                *lease_id,
                deadline,
                *deliveries,
                now,
                leases_durable,
            );
        }

        // Post-pass `ready` count (O(working-set), derived from `live_count`).
        let ready = {
            let mut q = b.queue.as_ref().expect("queue").lock();
            self.ready_in_flight_locked(b, &mut q, now).0
        };
        Ok((out, ready))
    }

    /// `POST /v0/topics/:q/ack` — complete jobs held by `node`: record an `acked`
    /// event and permanently delete each from the jobs log (the ack *is* the
    /// delete, DESIGN §10.4). Seqs not held by `node` are silently skipped.
    pub fn ack(&self, name: &str, node: &str, seqs: &[u64]) -> Result<AckResponse> {
        self.ack_fenced(name, node, seqs, &[])
    }

    /// As [`Self::ack`], with optional per-seq stale-worker fencing (R4). When
    /// `lease_ids[i]` is `Some(id)`, `seqs[i]` is acked only if `node` currently
    /// holds it *under that exact lease id*; a mismatched/stale token is rejected
    /// (skipped), so a worker reusing the same `node` after its lease expired (the
    /// job re-delivered under a new lease) cannot ack the newer delivery. An empty
    /// `lease_ids` (or a `None` entry) preserves node-only matching.
    pub fn ack_fenced(
        &self,
        name: &str,
        node: &str,
        seqs: &[u64],
        lease_ids: &[Option<u64>],
    ) -> Result<AckResponse> {
        let start = Instant::now();
        if node.is_empty() {
            return Err(Error::invalid_request("ack requires a non-empty `node`"));
        }
        check_seqs_len("ack", seqs)?;
        check_lease_ids_len("ack", seqs, lease_ids)?;
        let b = self.get_queue(name)?;
        let now = self.clock.now_ms();
        let topic_id = b.topic_id;
        let leases_durable = {
            let cfg = b.config.read();
            cfg.leases_durable && cfg.uses_persistent_record_store()
        };

        let mut acked_seqs: Vec<(u64, u64)> = Vec::new(); // (seq, lease_id)
        let mut skipped: Vec<u64> = Vec::new();
        {
            let mut q = b.queue.as_ref().expect("queue").lock();
            q.sweep_expired(now);
            for (i, &seq) in seqs.iter().enumerate() {
                match q.leases.get(&seq) {
                    Some(l) if l.node == node && lease_token_ok(lease_ids, i, l.lease_id) => {
                        let lease_id = l.lease_id;
                        q.leases.remove(&seq);
                        q.deliveries.remove(&seq);
                        acked_seqs.push((seq, lease_id));
                    }
                    _ => skipped.push(seq),
                }
            }
        }

        // Delete the acked jobs from the jobs log (the §7 permanent delete). Ack
        // durability == jobs-log durability: a durable topic fsyncs the delete
        // BEFORE the ack returns (codex P0).
        //
        // The leases were ALL removed from the projection above. If a durable delete
        // then FAILS, the affected jobs are no longer leased AND not deleted — and
        // `next_claimable` consults only the reclaim freelist + claim cursor (the
        // cursor has already advanced past these seqs), so they would be stranded
        // forever (codex HIGH #4 / P1 #5). On the FIRST failure we re-push the
        // failing seq AND every not-yet-deleted seq remaining in this ack batch onto
        // the reclaim freelist before propagating the error, so the whole un-deleted
        // suffix resurfaces as claimable (at-least-once) instead of being lost — not
        // just the single failing seq (the prior bug stranded the later batch seqs
        // whose leases were already dropped).
        let mut fsync_ms = 0.0;
        for (i, &(seq, lease_id)) in acked_seqs.iter().enumerate() {
            match self.delete_one_seq(&b, seq, now) {
                Ok(ms) => fsync_ms += ms,
                Err(e) => {
                    // Re-queue this seq and every still-undeleted seq after it.
                    for &(later_seq, _) in &acked_seqs[i..] {
                        self.reclaim_seq(&b, later_seq);
                    }
                    return Err(e);
                }
            }
            self.log_lease_event(
                topic_id,
                seq,
                LeaseEvent::Acked,
                node,
                lease_id,
                0,
                0,
                now,
                leases_durable,
            );
        }

        let (ready, in_flight) = self.queue_ready_inflight(&b, now);
        let mut perf = Performance::with_total(elapsed_ms(start));
        if fsync_ms > 0.0 {
            perf.fsync_ms = Some(fsync_ms);
        }
        Ok(AckResponse {
            topic_name: name.to_string(),
            acked: acked_seqs.len() as u64,
            skipped,
            ready,
            in_flight,
            performance: perf,
        })
    }

    /// `POST /v0/topics/:q/nack` — release leased jobs held by `node` for
    /// immediate (or `delay_ms`-delayed) reclaim, recording a `released` event
    /// (DESIGN §10.5).
    pub fn nack(
        &self,
        name: &str,
        node: &str,
        seqs: &[u64],
        delay_ms: u64,
    ) -> Result<NackResponse> {
        self.nack_fenced(name, node, seqs, delay_ms, &[])
    }

    /// As [`Self::nack`], with optional per-seq stale-worker fencing (R4); see
    /// [`Self::ack_fenced`].
    pub fn nack_fenced(
        &self,
        name: &str,
        node: &str,
        seqs: &[u64],
        delay_ms: u64,
        lease_ids: &[Option<u64>],
    ) -> Result<NackResponse> {
        let start = Instant::now();
        if node.is_empty() {
            return Err(Error::invalid_request("nack requires a non-empty `node`"));
        }
        check_seqs_len("nack", seqs)?;
        check_lease_ids_len("nack", seqs, lease_ids)?;
        let b = self.get_queue(name)?;
        let now = self.clock.now_ms();
        let topic_id = b.topic_id;
        let leases_durable = {
            let cfg = b.config.read();
            cfg.leases_durable && cfg.uses_persistent_record_store()
        };
        let delay = delay_ms.min(config::MAX_NACK_DELAY_MS) as i64;

        let mut nacked: Vec<(u64, u64)> = Vec::new();
        let mut skipped: Vec<u64> = Vec::new();
        {
            let mut q = b.queue.as_ref().expect("queue").lock();
            q.sweep_expired(now);
            for (i, &seq) in seqs.iter().enumerate() {
                match q.leases.get(&seq) {
                    Some(l) if l.node == node && lease_token_ok(lease_ids, i, l.lease_id) => {
                        let lease_id = l.lease_id;
                        q.leases.remove(&seq);
                        if delay > 0 {
                            q.push_delayed(now.saturating_add(delay), seq);
                        } else {
                            q.push_reclaim(seq);
                        }
                        nacked.push((seq, lease_id));
                    }
                    _ => skipped.push(seq),
                }
            }
        }

        for &(seq, lease_id) in &nacked {
            self.log_lease_event(
                topic_id,
                seq,
                LeaseEvent::Released,
                node,
                lease_id,
                0,
                0,
                now,
                leases_durable,
            );
        }

        let (ready, in_flight) = self.queue_ready_inflight(&b, now);
        Ok(NackResponse {
            topic_name: name.to_string(),
            nacked: nacked.len() as u64,
            skipped,
            ready,
            in_flight,
            performance: Performance::with_total(elapsed_ms(start)),
        })
    }

    /// `POST /v0/topics/:q/extend` — push out the deadline of leases held by
    /// `node` (the heartbeat for long jobs). The delivery counter is untouched
    /// (DESIGN §10.6). An expired/reclaimed seq is skipped.
    pub fn extend(
        &self,
        name: &str,
        node: &str,
        seqs: &[u64],
        lease_ms: u64,
    ) -> Result<ExtendResponse> {
        self.extend_fenced(name, node, seqs, lease_ms, &[])
    }

    /// As [`Self::extend`], with optional per-seq stale-worker fencing (R4); see
    /// [`Self::ack_fenced`].
    pub fn extend_fenced(
        &self,
        name: &str,
        node: &str,
        seqs: &[u64],
        lease_ms: u64,
        lease_ids: &[Option<u64>],
    ) -> Result<ExtendResponse> {
        let start = Instant::now();
        if node.is_empty() {
            return Err(Error::invalid_request("extend requires a non-empty `node`"));
        }
        check_seqs_len("extend", seqs)?;
        check_lease_ids_len("extend", seqs, lease_ids)?;
        let b = self.get_queue(name)?;
        let now = self.clock.now_ms();
        let topic_id = b.topic_id;
        let leases_durable = {
            let cfg = b.config.read();
            cfg.leases_durable && cfg.uses_persistent_record_store()
        };
        let effective = lease_ms.clamp(config::MIN_LEASE_MS, config::MAX_LEASE_MS) as i64;
        let deadline = now.saturating_add(effective);

        let mut extended: Vec<(u64, u64)> = Vec::new();
        let mut skipped: Vec<u64> = Vec::new();
        let mut deadlines: std::collections::HashMap<String, i64> =
            std::collections::HashMap::new();
        {
            let mut q = b.queue.as_ref().expect("queue").lock();
            // Sweep first so an already-expired lease is treated as reclaimed and
            // cannot be extended (DESIGN §10.6).
            q.sweep_expired(now);
            for (i, &seq) in seqs.iter().enumerate() {
                match q.leases.get_mut(&seq) {
                    Some(l) if l.node == node && lease_token_ok(lease_ids, i, l.lease_id) => {
                        l.deadline_ms = deadline;
                        let lease_id = l.lease_id;
                        extended.push((seq, lease_id));
                        deadlines.insert(seq.to_string(), deadline);
                    }
                    _ => skipped.push(seq),
                }
            }
        }

        for &(seq, lease_id) in &extended {
            self.log_lease_event(
                topic_id,
                seq,
                LeaseEvent::Extended,
                node,
                lease_id,
                deadline,
                0,
                now,
                leases_durable,
            );
        }

        Ok(ExtendResponse {
            topic_name: name.to_string(),
            extended: extended.len() as u64,
            skipped,
            deadlines,
            performance: Performance::with_total(elapsed_ms(start)),
        })
    }

    /// Compute `(ready, in_flight)` for a queue topic at `now`.
    pub(crate) fn queue_ready_inflight(&self, b: &TopicState, now: i64) -> (u64, u64) {
        let q = self.queue_counters(b, now);
        (q.ready, q.in_flight)
    }

    /// Permanently delete a single job seq from the jobs log (the ack/dead-letter
    /// delete path). Reuses [`TopicState::apply_delete`] with a bounded `before_seq`
    /// AND-ed against the seq's tag so exactly this seq is removed; falls back to
    /// a direct index mark when the record is untagged.
    fn delete_one_seq(&self, b: &TopicState, seq: u64, now: i64) -> Result<f64> {
        let topic_id = b.topic_id;
        let class = b.config.read().durability_class();
        if class == crate::types::Durability::Fsync {
            // Durable jobs log (codex P0): log the explicit-seq Delete frame and
            // WAIT on its fsync BEFORE the in-memory removal, propagating any
            // failure. This makes ack durability real — a 200 ack means the delete
            // is durably synced. The explicit-seq frame replays deterministically,
            // so a crash after the fsync (before/after the in-memory delete)
            // converges to the job being gone. On a WAL failure nothing is deleted
            // (the job stays claimable) and the caller surfaces the error.
            let (_a, fsync_ms) = self.wal_commit(
                WalRecord::Delete {
                    topic_id,
                    before_seq: None,
                    match_: None,
                    seqs: vec![seq],
                    // Explicit-seq delete: the seqs are the exact set; no bound.
                    bound_head: None,
                    ts: now.max(0) as u64,
                },
                true,
            )?;
            let stats = b.delete_seqs_stats(&[seq], now);
            self.release_total_bytes(stats.bytes_freed);
            return Ok(fsync_ms);
        }
        // Disk / memory class: the in-memory removal first, then a best-effort
        // frame (its loss self-heals — the job resurfaces as claimable, DESIGN
        // §10.6, at-least-once). A `memory`-class queue logs the same best-effort
        // frame: it shares the disk-like path (§0.10), just with no durability
        // GUARANTEE (the frame may persist or be lost).
        let stats = b.delete_seqs_stats(&[seq], now);
        self.release_total_bytes(stats.bytes_freed);
        self.wal_log_delete_seqs(topic_id, vec![seq], now, false);
        Ok(0.0)
    }

    /// Move jobs to the dead-letter topic (append + permanent delete), stamping
    /// provenance meta (DESIGN §10.7).
    fn dead_letter_jobs(
        &self,
        src: &TopicState,
        dl_topic: &str,
        seqs: &[u64],
        max_deliveries: u64,
        now: i64,
    ) {
        // Re-queue every diverted source seq onto the reclaim freelist and roll back
        // the `dead_lettered` counter (codex P1 #6): used on any dead-letter failure
        // so a diverted job — already popped off the claim cursor / reclaim freelist
        // in the claim pass — is never stranded (it resurfaces as claimable and is
        // re-dead-lettered later, at-least-once).
        let requeue_diverted = |src: &TopicState| {
            if let Some(q) = &src.queue {
                let mut q = q.lock();
                for &seq in seqs {
                    q.push_reclaim(seq);
                }
                q.dead_lettered = q.dead_lettered.saturating_sub(seqs.len() as u64);
            }
        };

        // Ensure the dead-letter topic exists (auto-create with defaults). If it does
        // not exist and cannot be created, do NOT strand the diverted jobs: re-queue
        // them for reclaim and bail (codex P1 #6).
        if self.get_topic(dl_topic).is_none() {
            let _ = self.put_topic(dl_topic, TopicConfig::default());
        }
        let Some(dl) = self.get_topic(dl_topic) else {
            tracing::warn!(
                src = %src.name, dead_letter = %dl_topic,
                "dead-letter: DL topic missing/uncreatable; re-queuing source jobs for reclaim"
            );
            requeue_diverted(src);
            return;
        };
        let src_name = src.name.clone();

        // Capture each job's locator fields + resident payload (if any) under a
        // short read lock; a sealed job's payload (`None` resident) is resolved
        // from the segment AFTER the lock is dropped (the HARD INVARIANT).
        struct DlSlot {
            seq: u64,
            node: Option<String>,
            tag: Option<String>,
            resident: Option<(serde_json::Value, Option<serde_json::Value>)>,
        }
        let slots: Vec<DlSlot> = {
            let index = src.index.read();
            seqs.iter()
                .filter_map(|&seq| {
                    index.get(seq).filter(|r| !r.deleted).map(|rec| DlSlot {
                        seq,
                        node: rec.node.clone(),
                        tag: rec.tag.clone(),
                        resident: if rec.payload_resident {
                            Some((rec.data.clone(), rec.meta.clone()))
                        } else {
                            None
                        },
                    })
                })
                .collect()
        };

        let mut records: Vec<crate::engine::topic_state::StoredRecord> = Vec::new();
        for slot in slots {
            let (data, src_meta) = match slot.resident {
                Some(p) => p,
                None => {
                    let mut ignored = 0u64;
                    crate::engine::resolve_sealed_off_lock(src, slot.seq, &mut ignored)
                }
            };
            // Stamp provenance into meta.
            let mut meta = match src_meta {
                Some(serde_json::Value::Object(m)) => m,
                _ => serde_json::Map::new(),
            };
            meta.insert(
                "$dead_letter_from".to_string(),
                serde_json::Value::String(src_name.clone()),
            );
            meta.insert(
                "$dead_letter_deliveries".to_string(),
                serde_json::Value::String(max_deliveries.to_string()),
            );
            meta.insert(
                "$dead_letter_src_seq".to_string(),
                serde_json::Value::String(slot.seq.to_string()),
            );
            let meta_val = serde_json::Value::Object(meta);
            let bytes = crate::engine::payload_bytes(&data, &Some(meta_val.clone()));
            records.push(crate::engine::topic_state::StoredRecord {
                ts: now,
                node: slot.node,
                tag: slot.tag,
                data,
                meta: Some(meta_val),
                bytes,
                deleted: false,
                payload_resident: true,
                hops: 0,
            });
        }
        if !records.is_empty() {
            // Dead-lettered copies go through the SAME WAL-first durable append
            // path as user writes (ARCHITECTURE §2.2): a dead-letter record into a
            // durable DL topic is durable by construction and recovers via WAL
            // replay, instead of living only in memory and vanishing on restart.
            if let Err(e) = self.durable_append(&dl, records, now) {
                tracing::warn!(
                    src = %src.name, dead_letter = %dl_topic, error = %e,
                    "dead-letter: durable DL append failed; re-queuing source jobs for reclaim"
                );
                // The DL append failed and published nothing. The diverted seqs were
                // already popped off the claim cursor / reclaim freelist and their
                // delivery counters cleared in the claim pass (and `dead_lettered`
                // bumped), so they are now neither leased nor reclaimable — stranded
                // (codex P1 #6). Re-queue every diverted seq for reclaim (it
                // resurfaces as claimable and is re-dead-lettered later,
                // at-least-once) and roll back the `dead_lettered` counter for the
                // copies that never landed. Do NOT delete from the source.
                requeue_diverted(src);
                return;
            }
            if let Err(e) = self.enforce_retention_durable(&dl, now) {
                tracing::warn!(
                    src = %src.name, dead_letter = %dl_topic, error = %e,
                    "dead-letter: DL retention harden failed; deferring eviction"
                );
            }
        }
        // Permanently delete the dead-lettered jobs from the source jobs log. The
        // DL copy is already durably appended; a failure to durably log the source
        // delete here must NOT strand the job: the diverted seq was popped off the
        // claim cursor / reclaim freelist and never leased, so on a delete failure
        // it is neither claimable nor reclaimable (codex HIGH #4). Push it back onto
        // the reclaim freelist so it resurfaces as claimable and is re-dead-lettered
        // (at-least-once) rather than being lost. We log a warning and continue
        // rather than failing the whole claim pass.
        for &seq in seqs {
            if let Err(e) = self.delete_one_seq(src, seq, now) {
                self.reclaim_seq(src, seq);
                tracing::warn!(
                    src = %src.name, seq, error = %e,
                    "dead-letter: durable source delete failed; re-queued for reclaim (self-heals)"
                );
            }
        }
    }

    /// Count the active (un-expired) leases currently held by a `/work`
    /// connection (the in-flight depth for backpressure, API §10.8). Expired
    /// leases are not counted — they are logically reclaimable. Returns 0 for a
    /// missing / non-queue topic.
    pub fn work_conn_in_flight(&self, name: &str, conn: u64) -> u32 {
        let Some(b) = self.get_topic(name) else {
            return 0;
        };
        if !b.is_queue() {
            return 0;
        }
        let now = self.clock.now_ms();
        let q = b.queue.as_ref().expect("queue").lock();
        q.leases
            .values()
            .filter(|l| l.by_work_conn == Some(conn) && now <= l.deadline_ms)
            .count() as u32
    }

    /// Release every lease delivered to a `/work` SSE connection (instant
    /// failover on disconnect, API §10.8): drop each lease keyed to `conn` and
    /// push its seq onto the reclaim freelist so it is immediately claimable
    /// again, recording a `released` event per seq. A no-op for a missing /
    /// non-queue topic (the connection is gone; nothing to fail loudly about).
    /// Lease expiry (§10.3) still covers hard crashes where the disconnect is
    /// never observed.
    pub fn release_work_conn(&self, name: &str, conn: u64) {
        let Some(b) = self.get_topic(name) else {
            return;
        };
        if !b.is_queue() {
            return;
        }
        let now = self.clock.now_ms();
        let topic_id = b.topic_id;
        let leases_durable = {
            let cfg = b.config.read();
            cfg.leases_durable && cfg.uses_persistent_record_store()
        };

        let mut released: Vec<(u64, String, u64)> = Vec::new(); // (seq, node, lease_id)
        {
            let mut q = b.queue.as_ref().expect("queue").lock();
            let seqs: Vec<u64> = q
                .leases
                .iter()
                .filter(|(_, l)| l.by_work_conn == Some(conn))
                .map(|(s, _)| *s)
                .collect();
            for seq in seqs {
                if let Some(l) = q.leases.remove(&seq) {
                    released.push((seq, l.node, l.lease_id));
                    q.push_reclaim(seq);
                }
            }
        }
        for (seq, node, lease_id) in &released {
            self.log_lease_event(
                topic_id,
                *seq,
                LeaseEvent::Released,
                node,
                *lease_id,
                0,
                0,
                now,
                leases_durable,
            );
        }
    }

    /// Append one leases-log lifecycle event to the WAL when the queue's leases
    /// log is durable; a no-op for the default non-durable leases log (the
    /// projection self-heals on restart, DESIGN §10.6) and for pure in-memory
    /// engines.
    #[allow(clippy::too_many_arguments)]
    fn log_lease_event(
        &self,
        topic_id: u64,
        seq: u64,
        event: LeaseEvent,
        node: &str,
        lease_id: u64,
        deadline: i64,
        deliveries: u64,
        now: i64,
        leases_durable: bool,
    ) {
        if !leases_durable {
            return;
        }
        self.wal_log_lease(WalRecord::Lease {
            topic_id,
            seq,
            event: event as u8,
            node: node.to_string(),
            lease_id,
            deadline: deadline.max(0) as u64,
            deliveries,
            ts: now.max(0) as u64,
        });
    }
}

/// Reject an unbounded `seqs` array on ack/nack/extend (codex MEDIUM #10): a
/// single request must not carry more than [`config::MAX_CLAIM`] seqs, so an
/// attacker cannot make the server allocate/scan/echo an arbitrarily large vec
/// (the `skipped` array echoes unmatched seqs back). The same bound already caps
/// a `claim`'s `max`. An empty array is allowed (a no-op).
fn check_seqs_len(op: &str, seqs: &[u64]) -> Result<()> {
    if seqs.len() > config::MAX_CLAIM as usize {
        return Err(Error::new(
            ErrorCode::BatchTooLarge,
            format!(
                "{op} names {} seqs, exceeds max {}",
                seqs.len(),
                config::MAX_CLAIM
            ),
        )
        .with_detail(serde_json::json!({
            "seqs": seqs.len(),
            "max": config::MAX_CLAIM,
        })));
    }
    Ok(())
}

/// Validate the optional per-seq fencing tokens (R4): an empty `lease_ids`
/// disables fencing (node-only match); otherwise it must be exactly
/// `seqs`-aligned so `lease_ids[i]` pairs with `seqs[i]`.
fn check_lease_ids_len(op: &str, seqs: &[u64], lease_ids: &[Option<u64>]) -> Result<()> {
    if !lease_ids.is_empty() && lease_ids.len() != seqs.len() {
        return Err(Error::invalid_request(format!(
            "{op} `lease_ids` length {} must equal `seqs` length {}",
            lease_ids.len(),
            seqs.len()
        )));
    }
    Ok(())
}

/// Whether the fencing token for `seqs[i]` (if supplied) authorizes acting on the
/// lease currently held under `held` (R4 stale-worker fencing). Returns `true`
/// when no token was supplied (empty slice or `None` entry) — the node-only
/// node-only match — or when the supplied token matches the held `lease_id`.
#[inline]
fn lease_token_ok(lease_ids: &[Option<u64>], i: usize, held: u64) -> bool {
    match lease_ids.get(i).copied().flatten() {
        Some(want) => want == held,
        None => true,
    }
}

/// Pull the next claimable, live seq for a claim pass: the reclaim freelist is
/// drained first (reclaimed work jumps ahead of never-delivered work), then the
/// monotonic claim cursor hands out fresh, never-yet-leased seqs (DESIGN §10.3).
/// Skips dead (deleted/evicted) seqs and any seq that is currently leased.
/// Returns `None` when the queue is (near-)empty.
fn next_claimable(
    q: &mut QueueProjection,
    index: &crate::engine::topic_state::TopicIndex,
    head: u64,
) -> Option<u64> {
    // 1) Reclaim freelist first.
    while let Some(seq) = q.pop_reclaim() {
        // Skip if the seq has since been acked/deleted, or is somehow leased.
        if q.leases.contains_key(&seq) {
            continue;
        }
        match index.get(seq) {
            Some(r) if !r.deleted => return Some(seq),
            // Below base_seq / deleted ⇒ gone; drop it from the freelist.
            _ => continue,
        }
    }
    // 2) Fresh jobs from the claim cursor.
    while q.claim_cursor <= head {
        let seq = q.claim_cursor;
        q.claim_cursor += 1;
        if q.leases.contains_key(&seq) {
            continue; // already leased (shouldn't happen for a fresh seq).
        }
        match index.get(seq) {
            Some(r) if !r.deleted => return Some(seq),
            _ => continue, // deleted/evicted ⇒ skip.
        }
    }
    None
}

// ===========================================================================
// Unit tests (queue engine, TestClock — no wall-clock sleeps).
// ===========================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::{SharedClock, TestClock};
    use crate::config::ServerConfig;
    use serde_json::json;

    fn engine_with_clock() -> (Arc<Engine>, TestClock) {
        let clock = TestClock::new(1_000_000);
        let shared: SharedClock = Arc::new(clock.clone());
        (Engine::new(ServerConfig::default(), shared), clock)
    }

    fn queue_cfg() -> TopicConfig {
        TopicConfig {
            r#type: TopicType::Queue,
            lease_ms: 30_000,
            ..TopicConfig::default()
        }
    }

    /// Write `n` untagged jobs to a queue topic, returning the assigned seqs.
    fn produce(engine: &Engine, q: &str, n: usize) -> Vec<u64> {
        let records: Vec<RecordIn> = (0..n)
            .map(|i| RecordIn {
                data: json!({ "i": i }),
                tag: None,
                node: None,
                meta: None,
            })
            .collect();
        let resp = engine
            .write(
                q,
                WriteRequest {
                    records,
                    node: None,
                    idempotency_key: None,
                    create: None,
                    config: None,
                    disable_backpressure: false,
                },
                true,
            )
            .unwrap();
        resp.seqs.unwrap()
    }

    #[test]
    fn claim_distributes_and_limits() {
        let (engine, _clock) = engine_with_clock();
        engine.put_topic("jobs", queue_cfg()).unwrap();
        produce(&engine, "jobs", 10);

        // max=4 leases the 4 lowest seqs, in ascending order.
        let r = engine.claim("jobs", "w1", 4, None).unwrap();
        assert_eq!(r.count, 4);
        let seqs: Vec<u64> = r.claimed.iter().map(|c| c.seq).collect();
        assert_eq!(seqs, vec![1, 2, 3, 4]);
        assert_eq!(r.ready, 6); // 10 - 4 leased.
                                // deliveries starts at 1.
        assert!(r.claimed.iter().all(|c| c.deliveries == 1));

        // A second worker gets the next 4 (a claimed job is not re-leased).
        let r2 = engine.claim("jobs", "w2", 4, None).unwrap();
        assert_eq!(
            r2.claimed.iter().map(|c| c.seq).collect::<Vec<_>>(),
            vec![5, 6, 7, 8]
        );
        assert_eq!(r2.ready, 2);

        // A claim asking for more than is available returns fewer (the empty
        // signal), never an error.
        let r3 = engine.claim("jobs", "w3", 100, None).unwrap();
        assert_eq!(r3.count, 2);
        assert_eq!(r3.ready, 0);
        let r4 = engine.claim("jobs", "w3", 100, None).unwrap();
        assert_eq!(r4.count, 0);
    }

    #[test]
    fn ready_counter_tracks_cap_eviction_of_unleased_jobs() {
        // The O(1) `ready` counter is derived from `live_count` minus the live
        // working set (leases + pending delayed). When cap/TTL eviction removes
        // an unleased (ready) job, `live_count` drops and `ready` must follow
        // exactly — without rescanning the log.
        let (engine, _clock) = engine_with_clock();
        let cfg = TopicConfig {
            cap_records: 5,
            discard: Discard::Old,
            ..queue_cfg()
        };
        engine.put_topic("jobs", cfg).unwrap();
        produce(&engine, "jobs", 5);
        // All 5 ready, none leased.
        let st = engine.topic_state("jobs", false).unwrap();
        assert_eq!(st.queue.as_ref().unwrap().ready, 5);
        assert_eq!(st.queue.as_ref().unwrap().in_flight, 0);

        // Lease 2 ⇒ ready=3, in_flight=2.
        let r = engine.claim("jobs", "w1", 2, None).unwrap();
        assert_eq!(r.ready, 3);

        // Append 3 more: cap=5 evicts the 3 oldest. Two of the evicted were
        // leased (seqs 1,2) and one was a ready job (seq 3). After eviction the
        // log holds seqs 4..=8 (5 live); seqs 4,5 are still ready, 6,7,8 new ⇒
        // 5 ready, and the 2 leases whose jobs were evicted no longer count as
        // in-flight.
        produce(&engine, "jobs", 3);
        let st = engine.topic_state("jobs", false).unwrap();
        let q = st.queue.as_ref().unwrap();
        // 5 live, 0 live leases (the 2 leases' jobs were evicted) ⇒ ready=5.
        assert_eq!(q.ready, 5);
        assert_eq!(q.in_flight, 0);
    }

    #[test]
    fn coalescing_window_divides_evenly_across_cohort() {
        let (engine, _clock) = engine_with_clock();
        // claim_jitter_ms>0 is the coalescing window; the engine's cohort pass
        // divides evenly regardless of the (HTTP-layer) window timing.
        let cfg = TopicConfig {
            claim_jitter_ms: 50,
            ..queue_cfg()
        };
        engine.put_topic("jobs", cfg).unwrap();
        produce(&engine, "jobs", 50);

        // Ten workers each asking for max:10 against 50 available ⇒ ~5 each, NOT
        // 10/10/10/10/10/0/0/0/0/0 (first-arrival-drains-the-head).
        let claimers: Vec<Claimer> = (0..10)
            .map(|i| Claimer {
                node: format!("w{i}"),
                max: 10,
                work_conn: None,
            })
            .collect();
        let (results, ready) = engine.claim_cohort("jobs", &claimers, None).unwrap();
        let counts: Vec<usize> = results.iter().map(|r| r.len()).collect();
        assert_eq!(counts, vec![5; 10], "50 jobs / 10 claimers = 5 each");
        assert_eq!(ready, 0);
        // Every seq leased exactly once across the cohort.
        let mut all: Vec<u64> = results.iter().flatten().map(|j| j.seq).collect();
        all.sort_unstable();
        assert_eq!(all, (1..=50).collect::<Vec<_>>());
    }

    #[test]
    fn coalescing_window_respects_each_max() {
        let (engine, _clock) = engine_with_clock();
        engine.put_topic("jobs", queue_cfg()).unwrap();
        produce(&engine, "jobs", 9);
        // Two claimers: max 2 and max 100. Round-robin fills the small one to its
        // cap (2) and gives the rest to the larger (7).
        let claimers = vec![
            Claimer {
                node: "small".into(),
                max: 2,
                work_conn: None,
            },
            Claimer {
                node: "big".into(),
                max: 100,
                work_conn: None,
            },
        ];
        let (results, _ready) = engine.claim_cohort("jobs", &claimers, None).unwrap();
        assert_eq!(results[0].len(), 2);
        assert_eq!(results[1].len(), 7);
    }

    #[test]
    fn ack_removes_jobs() {
        let (engine, _clock) = engine_with_clock();
        engine.put_topic("jobs", queue_cfg()).unwrap();
        produce(&engine, "jobs", 3);
        let r = engine.claim("jobs", "w1", 3, None).unwrap();
        let seqs: Vec<u64> = r.claimed.iter().map(|c| c.seq).collect();

        let a = engine.ack("jobs", "w1", &seqs).unwrap();
        assert_eq!(a.acked, 3);
        assert!(a.skipped.is_empty());
        assert_eq!(a.ready, 0);
        assert_eq!(a.in_flight, 0);
        // Acked jobs are deleted from the jobs log.
        assert_eq!(engine.topic_state("jobs", false).unwrap().count, 0);

        // Acking a non-held seq is silently skipped (idempotent).
        let a2 = engine.ack("jobs", "w1", &seqs).unwrap();
        assert_eq!(a2.acked, 0);
        assert_eq!(a2.skipped, seqs);
    }

    #[test]
    fn ack_nack_extend_reject_unbounded_seqs() {
        // codex MEDIUM #10: a seqs array longer than MAX_CLAIM is rejected with
        // batch_too_large before any allocation/echo; a bounded array is accepted.
        let (engine, _clock) = engine_with_clock();
        engine.put_topic("jobs", queue_cfg()).unwrap();
        let over: Vec<u64> = (1..=(config::MAX_CLAIM as u64 + 1)).collect();

        let e = engine.ack("jobs", "w1", &over).unwrap_err();
        assert_eq!(e.code, ErrorCode::BatchTooLarge);
        let e = engine.nack("jobs", "w1", &over, 0).unwrap_err();
        assert_eq!(e.code, ErrorCode::BatchTooLarge);
        let e = engine.extend("jobs", "w1", &over, 30_000).unwrap_err();
        assert_eq!(e.code, ErrorCode::BatchTooLarge);

        // Exactly MAX_CLAIM is allowed (boundary).
        let at: Vec<u64> = (1..=(config::MAX_CLAIM as u64)).collect();
        assert!(engine.ack("jobs", "w1", &at).is_ok());
    }

    #[test]
    fn nack_requeues_for_immediate_reclaim() {
        let (engine, _clock) = engine_with_clock();
        engine.put_topic("jobs", queue_cfg()).unwrap();
        produce(&engine, "jobs", 2);
        let r = engine.claim("jobs", "w1", 2, None).unwrap();
        let seqs: Vec<u64> = r.claimed.iter().map(|c| c.seq).collect();

        let n = engine.nack("jobs", "w1", &seqs, 0).unwrap();
        assert_eq!(n.nacked, 2);
        assert_eq!(n.in_flight, 0);
        assert_eq!(n.ready, 2);

        // Reclaimed seqs are claimable again (freelist drained first), and the
        // delivery counter increments on the re-claim.
        let r2 = engine.claim("jobs", "w2", 2, None).unwrap();
        assert_eq!(r2.claimed.iter().map(|c| c.seq).collect::<Vec<_>>(), seqs,);
        assert!(r2.claimed.iter().all(|c| c.deliveries == 2));
    }

    #[test]
    fn nack_delay_holds_until_elapsed() {
        let (engine, clock) = engine_with_clock();
        engine.put_topic("jobs", queue_cfg()).unwrap();
        produce(&engine, "jobs", 1);
        let r = engine.claim("jobs", "w1", 1, None).unwrap();
        let seq = r.claimed[0].seq;

        // Delayed nack: invisible for 5s.
        let n = engine.nack("jobs", "w1", &[seq], 5_000).unwrap();
        assert_eq!(n.nacked, 1);
        assert_eq!(n.ready, 0); // not claimable yet (delay pending).
                                // Not claimable before the delay elapses.
        assert_eq!(engine.claim("jobs", "w2", 1, None).unwrap().count, 0);

        // After 5s it becomes claimable.
        clock.advance(5_001);
        let r2 = engine.claim("jobs", "w2", 1, None).unwrap();
        assert_eq!(r2.count, 1);
        assert_eq!(r2.claimed[0].seq, seq);
    }

    #[test]
    fn extend_pushes_deadline() {
        let (engine, clock) = engine_with_clock();
        engine.put_topic("jobs", queue_cfg()).unwrap();
        produce(&engine, "jobs", 1);
        let r = engine.claim("jobs", "w1", 1, Some(10_000)).unwrap();
        let seq = r.claimed[0].seq;
        let first_deadline = r.claimed[0].deadline;

        // Heartbeat just before expiry: extend by 10s from now.
        clock.advance(9_000);
        let e = engine.extend("jobs", "w1", &[seq], 10_000).unwrap();
        assert_eq!(e.extended, 1);
        let new_deadline = e.deadlines[&seq.to_string()];
        assert!(new_deadline > first_deadline);

        // Past the original deadline but within the extended one ⇒ still leased.
        clock.advance(5_000); // now +14s; original was +10s, extended is +19s.
        assert_eq!(engine.claim("jobs", "w2", 1, None).unwrap().count, 0);
        let st = engine.topic_state("jobs", false).unwrap();
        assert_eq!(st.queue.as_ref().unwrap().in_flight, 1);

        // Extending an already-expired/never-held seq is skipped.
        let e2 = engine.extend("jobs", "w9", &[seq], 10_000).unwrap();
        assert_eq!(e2.extended, 0);
        assert_eq!(e2.skipped, vec![seq]);
    }

    /// Parse the `"lease_<hex>"` wire token back to the raw lease id (test-only).
    fn parse_lease(token: &str) -> u64 {
        u64::from_str_radix(token.strip_prefix("lease_").unwrap(), 16).unwrap()
    }

    #[test]
    fn fenced_ack_rejects_stale_lease_token() {
        // R4: a worker that reuses the same `node` after its lease expired (the job
        // re-delivered under a NEW lease id) must NOT be able to ack the newer
        // delivery with its stale token.
        let (engine, clock) = engine_with_clock();
        engine.put_topic("jobs", queue_cfg()).unwrap();
        produce(&engine, "jobs", 1);

        // First delivery to w1 under lease L1 with a short lease.
        let r1 = engine.claim("jobs", "w1", 1, Some(1_000)).unwrap();
        let seq = r1.claimed[0].seq;
        let stale_token = parse_lease(&r1.claimed[0].lease_id);

        // Lease expires; the job is reclaimed and re-delivered to the SAME node
        // under a new lease id L2 (L2 != L1 since the allocator is monotonic).
        clock.advance(1_001);
        let r2 = engine.claim("jobs", "w1", 1, Some(60_000)).unwrap();
        assert_eq!(r2.claimed[0].seq, seq);
        let fresh_token = parse_lease(&r2.claimed[0].lease_id);
        assert_ne!(stale_token, fresh_token, "redelivery gets a new lease id");

        // The stale worker acks with its OLD token: node matches but the fencing
        // token does not ⇒ rejected (skipped), the newer delivery is untouched.
        let a = engine
            .ack_fenced("jobs", "w1", &[seq], &[Some(stale_token)])
            .unwrap();
        assert_eq!(a.acked, 0, "stale-token ack rejected");
        assert_eq!(a.skipped, vec![seq]);
        assert_eq!(a.in_flight, 1, "the fresh lease still holds the job");
        assert_eq!(
            engine.topic_state("jobs", false).unwrap().count,
            1,
            "not deleted"
        );

        // The current holder acks with the CORRECT token ⇒ accepted+deleted.
        let a2 = engine
            .ack_fenced("jobs", "w1", &[seq], &[Some(fresh_token)])
            .unwrap();
        assert_eq!(a2.acked, 1);
        assert!(a2.skipped.is_empty());
        assert_eq!(
            engine.topic_state("jobs", false).unwrap().count,
            0,
            "deleted"
        );
    }

    #[test]
    fn fenced_nack_and_extend_reject_stale_token() {
        // R4 for nack + extend: a stale token is rejected (skipped); the correct
        // token is honored. An empty `lease_ids` preserves node-only matching.
        let (engine, clock) = engine_with_clock();
        engine.put_topic("jobs", queue_cfg()).unwrap();
        produce(&engine, "jobs", 1);

        let r1 = engine.claim("jobs", "w1", 1, Some(1_000)).unwrap();
        let seq = r1.claimed[0].seq;
        let stale = parse_lease(&r1.claimed[0].lease_id);

        clock.advance(1_001);
        let r2 = engine.claim("jobs", "w1", 1, Some(60_000)).unwrap();
        let fresh = parse_lease(&r2.claimed[0].lease_id);
        assert_ne!(stale, fresh);

        // extend with the stale token: rejected.
        let e = engine
            .extend_fenced("jobs", "w1", &[seq], 30_000, &[Some(stale)])
            .unwrap();
        assert_eq!(e.extended, 0);
        assert_eq!(e.skipped, vec![seq]);

        // nack with the stale token: rejected (the job is NOT released).
        let n = engine
            .nack_fenced("jobs", "w1", &[seq], 0, &[Some(stale)])
            .unwrap();
        assert_eq!(n.nacked, 0);
        assert_eq!(n.skipped, vec![seq]);
        assert_eq!(n.in_flight, 1, "still held under the fresh lease");

        // nack with the correct token: released for reclaim.
        let n2 = engine
            .nack_fenced("jobs", "w1", &[seq], 0, &[Some(fresh)])
            .unwrap();
        assert_eq!(n2.nacked, 1);
        assert_eq!(n2.ready, 1);

        // A `None` token (node-only match) still works: re-claim + bare ack.
        let r3 = engine.claim("jobs", "w1", 1, None).unwrap();
        let s3 = r3.claimed[0].seq;
        let a = engine.ack("jobs", "w1", &[s3]).unwrap();
        assert_eq!(a.acked, 1);
    }

    #[test]
    fn fenced_ops_reject_mismatched_lease_ids_length() {
        // R4: a non-empty `lease_ids` must be exactly seqs-aligned.
        let (engine, _clock) = engine_with_clock();
        engine.put_topic("jobs", queue_cfg()).unwrap();
        let e = engine
            .ack_fenced("jobs", "w1", &[1, 2], &[Some(7)])
            .unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidRequest);
    }

    #[test]
    fn lease_expiry_makes_seq_claimable() {
        let (engine, clock) = engine_with_clock();
        engine.put_topic("jobs", queue_cfg()).unwrap();
        produce(&engine, "jobs", 1);
        let r = engine.claim("jobs", "w1", 1, Some(1_000)).unwrap();
        let seq = r.claimed[0].seq;
        assert_eq!(r.count, 1);

        // Before the deadline: not claimable by anyone else.
        assert_eq!(engine.claim("jobs", "w2", 1, None).unwrap().count, 0);

        // After the deadline passes (visibility timeout): claimable again, and the
        // delivery counter increments on reclaim.
        clock.advance(1_001);
        let r2 = engine.claim("jobs", "w2", 1, None).unwrap();
        assert_eq!(r2.count, 1);
        assert_eq!(r2.claimed[0].seq, seq);
        assert_eq!(r2.claimed[0].deliveries, 2);
    }

    #[test]
    fn dead_letter_after_max_deliveries() {
        let (engine, clock) = engine_with_clock();
        engine.put_topic("dlq", TopicConfig::default()).unwrap();
        let cfg = TopicConfig {
            max_deliveries: 2,
            dead_letter: Some("dlq".to_string()),
            lease_ms: 1_000,
            ..queue_cfg()
        };
        engine.put_topic("jobs", cfg).unwrap();
        produce(&engine, "jobs", 1);

        // Delivery 1, expire.
        let r1 = engine.claim("jobs", "w", 1, None).unwrap();
        assert_eq!(r1.claimed[0].deliveries, 1);
        clock.advance(1_001);
        // Delivery 2, expire.
        let r2 = engine.claim("jobs", "w", 1, None).unwrap();
        assert_eq!(r2.claimed[0].deliveries, 2);
        clock.advance(1_001);
        // The next claim would be delivery 3 > max_deliveries(2) ⇒ dead-lettered,
        // not re-delivered. The claim returns empty (the job left the queue).
        let r3 = engine.claim("jobs", "w", 1, None).unwrap();
        assert_eq!(r3.count, 0);

        // The job moved to the dead-letter topic with provenance meta.
        assert_eq!(engine.topic_state("jobs", false).unwrap().count, 0);
        let dlq = engine.topic_state("jobs", false).unwrap();
        assert_eq!(dlq.queue.as_ref().unwrap().dead_lettered, 1);

        let d = engine
            .diff(
                "dlq",
                DiffRequest {
                    from_seq: 0,
                    include_meta: true,
                    ..DiffRequest::default()
                },
            )
            .unwrap();
        assert_eq!(d.records.len(), 1);
        let meta = d.records[0].meta.as_ref().unwrap();
        assert_eq!(meta["$dead_letter_from"], json!("jobs"));
        assert_eq!(meta["$dead_letter_src_seq"], json!("1"));
    }

    #[test]
    fn non_queue_topic_rejects_claim() {
        let (engine, _clock) = engine_with_clock();
        engine.put_topic("log", TopicConfig::default()).unwrap();
        let err = engine.claim("log", "w", 1, None).unwrap_err();
        assert_eq!(err.code, ErrorCode::NotAQueue);
        // A missing topic is 404, not 409.
        let err2 = engine.claim("nope", "w", 1, None).unwrap_err();
        assert_eq!(err2.code, ErrorCode::TopicNotFound);
    }

    #[test]
    fn type_is_immutable_on_put() {
        let (engine, _clock) = engine_with_clock();
        engine.put_topic("jobs", queue_cfg()).unwrap();
        // Re-PUT as a log ⇒ 409 topic_exists_incompatible.
        let err = engine
            .put_topic("jobs", TopicConfig::default())
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::TopicExistsIncompatible);
        // Re-PUT as a queue (same type) is fine (idempotent config update).
        assert!(engine.put_topic("jobs", queue_cfg()).is_ok());
    }

    #[test]
    fn queue_type_survives_restart_and_self_heals_leases() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().to_string_lossy().to_string();
        let clock = TestClock::new(1_000_000);

        // First boot: create a durable queue, produce jobs, claim one (in-flight).
        {
            let cfg = ServerConfig {
                data_dir: Some(data_dir.clone()),
                ..ServerConfig::default()
            };
            let engine = Engine::with_data_dir(cfg, Arc::new(clock.clone())).unwrap();
            let qcfg = TopicConfig {
                r#type: TopicType::Queue,
                durable: true, // jobs log durable (we must not lose jobs).
                lease_ms: 30_000,
                ..TopicConfig::default()
            };
            engine.put_topic("jobs", qcfg).unwrap();
            produce(&engine, "jobs", 3);
            let r = engine.claim("jobs", "w1", 1, None).unwrap();
            assert_eq!(r.count, 1);
            // 1 in-flight, 2 ready before restart.
            let st = engine.topic_state("jobs", false).unwrap();
            assert_eq!(st.queue.as_ref().unwrap().in_flight, 1);
            assert_eq!(st.queue.as_ref().unwrap().ready, 2);
            // Drop the engine (its WAL Drop flushes + joins the writer thread).
            drop(engine);
        }

        // Second boot: recovery rebuilds the topic from the WAL. The queue TYPE
        // survives (config frame); the jobs survive (durable jobs log); the
        // non-durable leases log is gone ⇒ all 3 jobs are claimable again
        // (self-healing visibility timeout, DESIGN §10.6).
        {
            let cfg = ServerConfig {
                data_dir: Some(data_dir.clone()),
                ..ServerConfig::default()
            };
            let engine = Engine::with_data_dir(cfg, Arc::new(clock.clone())).unwrap();
            let st = engine.topic_state("jobs", false).unwrap();
            assert_eq!(st.r#type, TopicType::Queue, "queue type survives restart");
            assert!(engine.get_topic("jobs").unwrap().is_queue());
            // The previously in-flight job has no replayed lease ⇒ claimable.
            assert_eq!(st.queue.as_ref().unwrap().in_flight, 0);
            assert_eq!(st.queue.as_ref().unwrap().ready, 3, "all jobs claimable");
            assert_eq!(st.count, 3, "durable jobs log preserved all jobs");
            // And they can all be claimed.
            let r = engine.claim("jobs", "w-new", 3, None).unwrap();
            assert_eq!(r.count, 3);
        }
    }

    /// codex P1 #2: a replayed leases-log event must only mutate the projection
    /// when its `lease_id` matches the CURRENT lease. A delayed/out-of-order STALE
    /// Released (from a prior delivery) must NOT clear a newer recovered lease.
    #[test]
    fn lease_replay_ignores_stale_lease_id() {
        let mut q = QueueProjection::new(1);
        let seq = 5u64;

        // Delivery #1: claimed under lease 100, then released (stale-to-come).
        q.apply_lease_event(LeaseEvent::Claimed as u8, seq, "w1".into(), 100, 1_000, 1);
        assert!(q.leases.contains_key(&seq), "lease 100 active");

        // Delivery #2: re-claimed under a NEWER lease 200 (the seq was reclaimed and
        // handed to a different worker).
        q.apply_lease_event(LeaseEvent::Claimed as u8, seq, "w2".into(), 200, 2_000, 2);
        assert_eq!(
            q.leases.get(&seq).unwrap().lease_id,
            200,
            "newer lease 200 active"
        );

        // A STALE Released for the OLD lease 100 arrives (out of order): it must be
        // ignored — the current lease is 200.
        q.apply_lease_event(LeaseEvent::Released as u8, seq, "w1".into(), 100, 0, 0);
        assert_eq!(
            q.leases.get(&seq).map(|l| l.lease_id),
            Some(200),
            "stale release for lease 100 must not clear the newer lease 200"
        );

        // A stale Extended for lease 100 likewise leaves lease 200's deadline alone.
        let dl_before = q.leases.get(&seq).unwrap().deadline_ms;
        q.apply_lease_event(LeaseEvent::Extended as u8, seq, "w1".into(), 100, 9_999, 0);
        assert_eq!(
            q.leases.get(&seq).unwrap().deadline_ms,
            dl_before,
            "stale extend ignored"
        );

        // The CURRENT lease's own Released does clear it.
        q.apply_lease_event(LeaseEvent::Released as u8, seq, "w2".into(), 200, 0, 0);
        assert!(
            !q.leases.contains_key(&seq),
            "current lease's release clears it"
        );
    }
}
