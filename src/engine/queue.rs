//! The materialized lease projection for a queue box (DESIGN §10,
//! ARCHITECTURE §12).
//!
//! A queue is **two logs**: the jobs log (the box's own [`BoxIndex`]) and an
//! append-only **leases log** of lifecycle events. The pending who-holds-what
//! state is the **materialized projection** of the leases log — held here in
//! memory and rebuilt on restart by replaying whatever lease events survived
//! (none, for the default non-durable leases log ⇒ every in-flight job becomes
//! claimable again, the self-healing visibility timeout, DESIGN §10.6).
//!
//! All time decisions (lease deadlines, the jitter window, delayed nacks) read
//! the [`Clock`](crate::clock::Clock); no wall-clock sleep is load-bearing.
//!
//! [`BoxIndex`]: crate::engine::box_state::BoxIndex

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};

/// One active lease in the projection: who holds a job seq, until when.
#[derive(Debug, Clone)]
pub struct Lease {
    /// The holder node.
    pub node: String,
    /// Opaque lease identity for this delivery (monotonic per box).
    pub lease_id: u64,
    /// Absolute deadline ms; the job is reclaimable once `now > deadline`.
    pub deadline_ms: i64,
    /// `Some(conn)` when delivered over a `/work` SSE stream (release-on-disconnect
    /// is keyed to this connection); `None` for poll-claims. Stage 2 only stores
    /// it; the SSE wiring lands with the HTTP layer.
    pub by_work_conn: Option<u64>,
}

/// The in-memory materialized lease view + reclaim/claim scheduling state. Lives
/// under the box's `queue` mutex; one batched cohort claim pass holds it for a
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
    /// Cumulative jobs moved to the dead-letter box (observability §10.7).
    pub dead_lettered: u64,
    /// Monotonic lease-id allocator (per box instance).
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
        self.leases.values().filter(|l| now_ms <= l.deadline_ms).count() as u64
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
            // released (nack / expiry)
            1 => {
                self.leases.remove(&seq);
                self.push_reclaim(seq);
            }
            // extended
            2 => {
                if let Some(l) = self.leases.get_mut(&seq) {
                    l.deadline_ms = deadline_ms;
                }
            }
            // acked (job is deleted from the jobs log; drop all lease state)
            3 => {
                self.leases.remove(&seq);
                self.deliveries.remove(&seq);
            }
            _ => {}
        }
    }
}

// ===========================================================================
// Engine queue API (DESIGN §10, API §10) — claim / ack / nack / extend +
// observability. The leases log is the box's WAL stream (lease frames); the
// projection above is its materialized view.
// ===========================================================================

use crate::config;
use crate::error::{Error, Result};
use crate::engine::box_state::BoxState;
use crate::engine::Engine;
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
    /// Resolve a box that MUST be a queue, returning `404 box_not_found` if
    /// absent or `409 not_a_queue` if it is a plain log (API §10).
    fn get_queue(&self, name: &str) -> Result<Arc<BoxState>> {
        let b = self.get_box(name).ok_or_else(|| Error::box_not_found(name))?;
        if !b.is_queue() {
            return Err(Error::not_a_queue(name));
        }
        Ok(b)
    }

    /// Compute the live `queue` counters (§10.7) for a queue box at `now`.
    pub(crate) fn queue_counters(&self, b: &BoxState, now: i64) -> QueueState {
        let Some(q) = &b.queue else {
            return QueueState {
                ready: 0,
                in_flight: 0,
                dead_lettered: 0,
            };
        };
        b.enforce_retention(now);
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
    /// the box's maintained `live_count` (records present and not deleted, net of
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
        b: &BoxState,
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
        let ready = live.saturating_sub(leased_live).saturating_sub(delayed_live);
        (ready, in_flight)
    }

    /// `POST /v0/boxes/:q/claim` — lease up to `max` claimable jobs to `node`
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
            box_name: name.to_string(),
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
        b: &Arc<BoxState>,
        claimers: &[Claimer],
        lease_ms: Option<u64>,
    ) -> Result<(Vec<Vec<LeasedJob>>, u64)> {
        let now = self.clock.now_ms();
        b.enforce_retention(now);

        let cfg = b.config.read();
        let box_lease_ms = cfg.lease_ms;
        let max_deliveries = cfg.max_deliveries;
        let dead_letter = cfg.dead_letter.clone();
        let leases_durable = cfg.leases_durable;
        let box_id = b.box_id;
        drop(cfg);

        let effective_lease = lease_ms
            .unwrap_or(box_lease_ms)
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
                let (ts, tag, data, meta) = match rec {
                    Some(r) => (r.ts, r.tag.clone(), r.data.clone(), r.meta.clone()),
                    None => (now, None, serde_json::Value::Null, None),
                };
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

        // Dead-letter the diverted jobs (append to the DL box + permanent delete
        // from the jobs log), outside the queue lock.
        if !to_dead_letter.is_empty() {
            if let Some(dl) = &dead_letter {
                self.dead_letter_jobs(b, dl, &to_dead_letter, max_deliveries, now);
            }
        }

        // Append `claimed` events to the leases log (durable iff leases_durable).
        for (idx, seq, lease_id, deliveries) in &lease_events {
            self.log_lease_event(
                box_id,
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

    /// `POST /v0/boxes/:q/ack` — complete jobs held by `node`: record an `acked`
    /// event and permanently delete each from the jobs log (the ack *is* the
    /// delete, DESIGN §10.4). Seqs not held by `node` are silently skipped.
    pub fn ack(&self, name: &str, node: &str, seqs: &[u64]) -> Result<AckResponse> {
        let start = Instant::now();
        if node.is_empty() {
            return Err(Error::invalid_request("ack requires a non-empty `node`"));
        }
        let b = self.get_queue(name)?;
        let now = self.clock.now_ms();
        let box_id = b.box_id;
        let leases_durable = b.config.read().leases_durable;

        let mut acked_seqs: Vec<(u64, u64)> = Vec::new(); // (seq, lease_id)
        let mut skipped: Vec<u64> = Vec::new();
        {
            let mut q = b.queue.as_ref().expect("queue").lock();
            q.sweep_expired(now);
            for &seq in seqs {
                match q.leases.get(&seq) {
                    Some(l) if l.node == node => {
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
        // durability == jobs-log durability: a durable box fsyncs the delete.
        for &(seq, lease_id) in &acked_seqs {
            // `before_seq = seq + 1` AND no match would delete the whole prefix;
            // instead delete exactly this seq via a tag-free point delete: use the
            // delete-by-seq-range bounded to this single seq is not exact, so we
            // mark it deleted directly on the index (the same path apply_delete
            // uses) and log a Delete frame for replay determinism.
            self.delete_one_seq(&b, seq, now);
            self.log_lease_event(
                box_id,
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
        Ok(AckResponse {
            box_name: name.to_string(),
            acked: acked_seqs.len() as u64,
            skipped,
            ready,
            in_flight,
            performance: Performance::with_total(elapsed_ms(start)),
        })
    }

    /// `POST /v0/boxes/:q/nack` — release leased jobs held by `node` for
    /// immediate (or `delay_ms`-delayed) reclaim, recording a `released` event
    /// (DESIGN §10.5).
    pub fn nack(
        &self,
        name: &str,
        node: &str,
        seqs: &[u64],
        delay_ms: u64,
    ) -> Result<NackResponse> {
        let start = Instant::now();
        if node.is_empty() {
            return Err(Error::invalid_request("nack requires a non-empty `node`"));
        }
        let b = self.get_queue(name)?;
        let now = self.clock.now_ms();
        let box_id = b.box_id;
        let leases_durable = b.config.read().leases_durable;
        let delay = delay_ms.min(config::MAX_NACK_DELAY_MS) as i64;

        let mut nacked: Vec<(u64, u64)> = Vec::new();
        let mut skipped: Vec<u64> = Vec::new();
        {
            let mut q = b.queue.as_ref().expect("queue").lock();
            q.sweep_expired(now);
            for &seq in seqs {
                match q.leases.get(&seq) {
                    Some(l) if l.node == node => {
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
                box_id,
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
            box_name: name.to_string(),
            nacked: nacked.len() as u64,
            skipped,
            ready,
            in_flight,
            performance: Performance::with_total(elapsed_ms(start)),
        })
    }

    /// `POST /v0/boxes/:q/extend` — push out the deadline of leases held by
    /// `node` (the heartbeat for long jobs). The delivery counter is untouched
    /// (DESIGN §10.6). An expired/reclaimed seq is skipped.
    pub fn extend(
        &self,
        name: &str,
        node: &str,
        seqs: &[u64],
        lease_ms: u64,
    ) -> Result<ExtendResponse> {
        let start = Instant::now();
        if node.is_empty() {
            return Err(Error::invalid_request("extend requires a non-empty `node`"));
        }
        let b = self.get_queue(name)?;
        let now = self.clock.now_ms();
        let box_id = b.box_id;
        let leases_durable = b.config.read().leases_durable;
        let effective = lease_ms.clamp(config::MIN_LEASE_MS, config::MAX_LEASE_MS) as i64;
        let deadline = now.saturating_add(effective);

        let mut extended: Vec<(u64, u64)> = Vec::new();
        let mut skipped: Vec<u64> = Vec::new();
        let mut deadlines: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
        {
            let mut q = b.queue.as_ref().expect("queue").lock();
            // Sweep first so an already-expired lease is treated as reclaimed and
            // cannot be extended (DESIGN §10.6).
            q.sweep_expired(now);
            for &seq in seqs {
                match q.leases.get_mut(&seq) {
                    Some(l) if l.node == node => {
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
                box_id,
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
            box_name: name.to_string(),
            extended: extended.len() as u64,
            skipped,
            deadlines,
            performance: Performance::with_total(elapsed_ms(start)),
        })
    }

    /// Compute `(ready, in_flight)` for a queue box at `now`.
    fn queue_ready_inflight(&self, b: &BoxState, now: i64) -> (u64, u64) {
        let q = self.queue_counters(b, now);
        (q.ready, q.in_flight)
    }

    /// Permanently delete a single job seq from the jobs log (the ack/dead-letter
    /// delete path). Reuses [`BoxState::apply_delete`] with a bounded `before_seq`
    /// AND-ed against the seq's tag so exactly this seq is removed; falls back to
    /// a direct index mark when the record is untagged.
    fn delete_one_seq(&self, b: &BoxState, seq: u64, now: i64) {
        let box_id = b.box_id;
        let durable = b.config.read().durable;
        // Logical removal of exactly this seq from the jobs log.
        b.delete_seqs(&[seq], now);
        // Log an explicit-seq Delete control frame so the removal replays
        // deterministically (the exact seq is logged, not re-derived). Ack
        // durability == jobs-log durability (DESIGN §10.1): a durable queue
        // fsyncs the delete before the ack returns.
        self.wal_log_delete_seqs(box_id, vec![seq], now, durable);
    }

    /// Move jobs to the dead-letter box (append + permanent delete), stamping
    /// provenance meta (DESIGN §10.7).
    fn dead_letter_jobs(
        &self,
        src: &BoxState,
        dl_box: &str,
        seqs: &[u64],
        max_deliveries: u64,
        now: i64,
    ) {
        // Ensure the dead-letter box exists (auto-create with defaults).
        if self.get_box(dl_box).is_none() {
            let _ = self.put_box(dl_box, BoxConfig::default());
        }
        let Some(dl) = self.get_box(dl_box) else {
            return;
        };
        let src_name = src.name.clone();
        let mut records: Vec<crate::engine::box_state::StoredRecord> = Vec::new();
        {
            let index = src.index.read();
            for &seq in seqs {
                let Some(rec) = index.get(seq) else { continue };
                if rec.deleted {
                    continue;
                }
                // Stamp provenance into meta.
                let mut meta = match &rec.meta {
                    Some(serde_json::Value::Object(m)) => m.clone(),
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
                    serde_json::Value::String(seq.to_string()),
                );
                let meta_val = serde_json::Value::Object(meta);
                let data = rec.data.clone();
                let tag = rec.tag.clone();
                let bytes = crate::engine::payload_bytes(&data, &Some(meta_val.clone()));
                records.push(crate::engine::box_state::StoredRecord {
                    ts: now,
                    node: rec.node.clone(),
                    tag,
                    data,
                    meta: Some(meta_val),
                    bytes,
                    deleted: false,
                });
            }
        }
        if !records.is_empty() {
            dl.append(records, now);
            dl.enforce_retention(now);
        }
        // Permanently delete the dead-lettered jobs from the source jobs log.
        for &seq in seqs {
            self.delete_one_seq(src, seq, now);
        }
    }

    /// Count the active (un-expired) leases currently held by a `/work`
    /// connection (the in-flight depth for backpressure, API §10.8). Expired
    /// leases are not counted — they are logically reclaimable. Returns 0 for a
    /// missing / non-queue box.
    pub fn work_conn_in_flight(&self, name: &str, conn: u64) -> u32 {
        let Some(b) = self.get_box(name) else {
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
    /// non-queue box (the connection is gone; nothing to fail loudly about).
    /// Lease expiry (§10.3) still covers hard crashes where the disconnect is
    /// never observed.
    pub fn release_work_conn(&self, name: &str, conn: u64) {
        let Some(b) = self.get_box(name) else { return };
        if !b.is_queue() {
            return;
        }
        let now = self.clock.now_ms();
        let box_id = b.box_id;
        let leases_durable = b.config.read().leases_durable;

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
                box_id,
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
        box_id: u32,
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
            box_id,
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

/// Pull the next claimable, live seq for a claim pass: the reclaim freelist is
/// drained first (reclaimed work jumps ahead of never-delivered work), then the
/// monotonic claim cursor hands out fresh, never-yet-leased seqs (DESIGN §10.3).
/// Skips dead (deleted/evicted) seqs and any seq that is currently leased.
/// Returns `None` when the queue is (near-)empty.
fn next_claimable(
    q: &mut QueueProjection,
    index: &crate::engine::box_state::BoxIndex,
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

    fn queue_cfg() -> BoxConfig {
        BoxConfig {
            r#type: BoxType::Queue,
            lease_ms: 30_000,
            ..BoxConfig::default()
        }
    }

    /// Write `n` untagged jobs to a queue box, returning the assigned seqs.
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
        engine.put_box("jobs", queue_cfg()).unwrap();
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
        let cfg = BoxConfig {
            cap_records: 5,
            discard: Discard::Old,
            ..queue_cfg()
        };
        engine.put_box("jobs", cfg).unwrap();
        produce(&engine, "jobs", 5);
        // All 5 ready, none leased.
        let st = engine.box_state("jobs", false).unwrap();
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
        let st = engine.box_state("jobs", false).unwrap();
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
        let cfg = BoxConfig {
            claim_jitter_ms: 50,
            ..queue_cfg()
        };
        engine.put_box("jobs", cfg).unwrap();
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
        engine.put_box("jobs", queue_cfg()).unwrap();
        produce(&engine, "jobs", 9);
        // Two claimers: max 2 and max 100. Round-robin fills the small one to its
        // cap (2) and gives the rest to the larger (7).
        let claimers = vec![
            Claimer { node: "small".into(), max: 2, work_conn: None },
            Claimer { node: "big".into(), max: 100, work_conn: None },
        ];
        let (results, _ready) = engine.claim_cohort("jobs", &claimers, None).unwrap();
        assert_eq!(results[0].len(), 2);
        assert_eq!(results[1].len(), 7);
    }

    #[test]
    fn ack_removes_jobs() {
        let (engine, _clock) = engine_with_clock();
        engine.put_box("jobs", queue_cfg()).unwrap();
        produce(&engine, "jobs", 3);
        let r = engine.claim("jobs", "w1", 3, None).unwrap();
        let seqs: Vec<u64> = r.claimed.iter().map(|c| c.seq).collect();

        let a = engine.ack("jobs", "w1", &seqs).unwrap();
        assert_eq!(a.acked, 3);
        assert!(a.skipped.is_empty());
        assert_eq!(a.ready, 0);
        assert_eq!(a.in_flight, 0);
        // Acked jobs are deleted from the jobs log.
        assert_eq!(engine.box_state("jobs", false).unwrap().count, 0);

        // Acking a non-held seq is silently skipped (idempotent).
        let a2 = engine.ack("jobs", "w1", &seqs).unwrap();
        assert_eq!(a2.acked, 0);
        assert_eq!(a2.skipped, seqs);
    }

    #[test]
    fn nack_requeues_for_immediate_reclaim() {
        let (engine, _clock) = engine_with_clock();
        engine.put_box("jobs", queue_cfg()).unwrap();
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
        assert_eq!(
            r2.claimed.iter().map(|c| c.seq).collect::<Vec<_>>(),
            seqs,
        );
        assert!(r2.claimed.iter().all(|c| c.deliveries == 2));
    }

    #[test]
    fn nack_delay_holds_until_elapsed() {
        let (engine, clock) = engine_with_clock();
        engine.put_box("jobs", queue_cfg()).unwrap();
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
        engine.put_box("jobs", queue_cfg()).unwrap();
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
        let st = engine.box_state("jobs", false).unwrap();
        assert_eq!(st.queue.as_ref().unwrap().in_flight, 1);

        // Extending an already-expired/never-held seq is skipped.
        let e2 = engine.extend("jobs", "w9", &[seq], 10_000).unwrap();
        assert_eq!(e2.extended, 0);
        assert_eq!(e2.skipped, vec![seq]);
    }

    #[test]
    fn lease_expiry_makes_seq_claimable() {
        let (engine, clock) = engine_with_clock();
        engine.put_box("jobs", queue_cfg()).unwrap();
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
        engine.put_box("dlq", BoxConfig::default()).unwrap();
        let cfg = BoxConfig {
            max_deliveries: 2,
            dead_letter: Some("dlq".to_string()),
            lease_ms: 1_000,
            ..queue_cfg()
        };
        engine.put_box("jobs", cfg).unwrap();
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

        // The job moved to the dead-letter box with provenance meta.
        assert_eq!(engine.box_state("jobs", false).unwrap().count, 0);
        let dlq = engine.box_state("jobs", false).unwrap();
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
    fn non_queue_box_rejects_claim() {
        let (engine, _clock) = engine_with_clock();
        engine.put_box("log", BoxConfig::default()).unwrap();
        let err = engine.claim("log", "w", 1, None).unwrap_err();
        assert_eq!(err.code, ErrorCode::NotAQueue);
        // A missing box is 404, not 409.
        let err2 = engine.claim("nope", "w", 1, None).unwrap_err();
        assert_eq!(err2.code, ErrorCode::BoxNotFound);
    }

    #[test]
    fn type_is_immutable_on_put() {
        let (engine, _clock) = engine_with_clock();
        engine.put_box("jobs", queue_cfg()).unwrap();
        // Re-PUT as a log ⇒ 409 box_exists_incompatible.
        let err = engine.put_box("jobs", BoxConfig::default()).unwrap_err();
        assert_eq!(err.code, ErrorCode::BoxExistsIncompatible);
        // Re-PUT as a queue (same type) is fine (idempotent config update).
        assert!(engine.put_box("jobs", queue_cfg()).is_ok());
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
            let qcfg = BoxConfig {
                r#type: BoxType::Queue,
                durable: true, // jobs log durable (we must not lose jobs).
                lease_ms: 30_000,
                ..BoxConfig::default()
            };
            engine.put_box("jobs", qcfg).unwrap();
            produce(&engine, "jobs", 3);
            let r = engine.claim("jobs", "w1", 1, None).unwrap();
            assert_eq!(r.count, 1);
            // 1 in-flight, 2 ready before restart.
            let st = engine.box_state("jobs", false).unwrap();
            assert_eq!(st.queue.as_ref().unwrap().in_flight, 1);
            assert_eq!(st.queue.as_ref().unwrap().ready, 2);
            // Drop the engine (its WAL Drop flushes + joins the writer thread).
            drop(engine);
        }

        // Second boot: recovery rebuilds the box from the WAL. The queue TYPE
        // survives (config frame); the jobs survive (durable jobs log); the
        // non-durable leases log is gone ⇒ all 3 jobs are claimable again
        // (self-healing visibility timeout, DESIGN §10.6).
        {
            let cfg = ServerConfig {
                data_dir: Some(data_dir.clone()),
                ..ServerConfig::default()
            };
            let engine = Engine::with_data_dir(cfg, Arc::new(clock.clone())).unwrap();
            let st = engine.box_state("jobs", false).unwrap();
            assert_eq!(st.r#type, BoxType::Queue, "queue type survives restart");
            assert!(engine.get_box("jobs").unwrap().is_queue());
            // The previously in-flight job has no replayed lease ⇒ claimable.
            assert_eq!(st.queue.as_ref().unwrap().in_flight, 0);
            assert_eq!(st.queue.as_ref().unwrap().ready, 3, "all jobs claimable");
            assert_eq!(st.count, 3, "durable jobs log preserved all jobs");
            // And they can all be claimed.
            let r = engine.claim("jobs", "w-new", 3, None).unwrap();
            assert_eq!(r.count, 3);
        }
    }
}
