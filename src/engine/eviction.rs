//! Cap (records + bytes) and TTL evaluation: the eviction/expiry floors and
//! tombstone gap computation (DESIGN §5, ARCHITECTURE §3.3).
//!
//! Phase 2 evaluates these logically (no segments); `earliest_seq` is the
//! authoritative retained floor and is always computed/read from the topic.

use crate::types::{Tombstone, TombstoneReason};

/// The loss floors tracked separately (DESIGN §5.1, the **dual watermark**).
///
/// `earliest_seq` (first live, reported) is driven by all three floors:
/// `earliest_seq = max(evict_floor, expiry_floor, delete_floor) + 1`. But the
/// tombstone trigger is **`evict_floor` only** — advanced solely by
/// **involuntary** cap/TTL eviction of *live* records. A deletion advances
/// `delete_floor` (and thus `earliest_seq`) but **never** `evict_floor`, so
/// reading across a purely-deleted prefix gap is silent (`tombstone: null`).
///
/// Invariant: `evict_floor <= earliest_seq` always.
#[derive(Debug, Clone, Copy, Default)]
pub struct Floors {
    /// Highest seq removed by **involuntary** cap eviction (`0` if none). This
    /// is the sole tombstone trigger.
    pub evict_floor: u64,
    /// Highest seq that is TTL-expired (`0` if none). Involuntary; also a
    /// tombstone trigger (it reaches into the gap as `ttl`).
    pub expiry_floor: u64,
    /// Highest seq removed by a **voluntary** prefix/snapshot delete (`0` if
    /// none). Advances `earliest_seq` but never produces a tombstone.
    pub delete_floor: u64,
    /// Highest **dest** seq that a derived router could not materialize because
    /// the SOURCE had already trimmed the corresponding record (async/derived
    /// forwarding; design §4 source-retention bound). Involuntary loss bounded by
    /// the source's retention: it advances `earliest_seq` AND surfaces a tombstone
    /// (reason `source_trim`), so the dest faithfully reflects the source retention
    /// instead of opening a silent gap. `0` if none.
    pub source_trim_floor: u64,
}

impl Floors {
    /// Combined logical earliest retained (first live) seq, clamped into
    /// `[seq_base, head_seq + 1]`. Driven by all three floors.
    pub fn earliest_seq(&self, seq_base: u64, head_seq: u64) -> u64 {
        let floor = self
            .evict_floor
            .max(self.expiry_floor)
            .max(self.delete_floor)
            .max(self.source_trim_floor);
        let earliest = floor.saturating_add(1).max(seq_base);
        earliest.min(head_seq.saturating_add(1))
    }

    /// The involuntary floor (cap/TTL/source-trim) used to decide whether a gap
    /// produces a tombstone (DESIGN §5.4). `delete_floor` is excluded (voluntary).
    pub fn evict_earliest(&self, seq_base: u64, head_seq: u64) -> u64 {
        let floor = self
            .evict_floor
            .max(self.expiry_floor)
            .max(self.source_trim_floor);
        let earliest = floor.saturating_add(1).max(seq_base);
        earliest.min(head_seq.saturating_add(1))
    }

    /// Which reason applies to a gap `[gap_from, gap_to]` (DESIGN §5.4).
    ///
    /// A floor "contributes" to the gap iff it reaches into the lost range, i.e.
    /// `floor >= gap_from`. Both contribute ⇒ `mixed`; only cap ⇒ `cap`; only
    /// ttl ⇒ `ttl`. A source-trim floor reaching the gap reports `source_trim`
    /// (derived-router source-retention bound). The reason is best-effort; the gap
    /// range is authoritative.
    pub fn reason_for_gap(&self, gap_from: u64) -> TombstoneReason {
        let cap = self.evict_floor >= gap_from && self.evict_floor > 0;
        let ttl = self.expiry_floor >= gap_from && self.expiry_floor > 0;
        let src_trim = self.source_trim_floor >= gap_from && self.source_trim_floor > 0;
        match (cap, ttl) {
            (true, true) => TombstoneReason::Mixed,
            (false, true) => TombstoneReason::Ttl,
            (true, false) => TombstoneReason::Cap,
            // Neither cap nor ttl reaches the gap: a source-trim gap reports
            // `source_trim`; otherwise default to cap (capacity-driven).
            (false, false) => {
                if src_trim {
                    TombstoneReason::SourceTrim
                } else {
                    TombstoneReason::Cap
                }
            }
        }
    }
}

/// How a full topic reacts to an incoming write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmitDecision {
    /// Admit the write; possibly evict oldest afterward (`discard:"old"`).
    Admit,
    /// Reject with `422 topic_full` (`discard:"reject"`).
    Reject,
}

/// Decide whether a write of `incoming_records`/`incoming_bytes` is admitted
/// given current retention and caps (DESIGN §5.3).
pub fn admit(
    discard: crate::types::Discard,
    cap_records: u64,
    cap_bytes: u64,
    current_records: u64,
    current_bytes: u64,
    incoming_records: u64,
    incoming_bytes: u64,
) -> AdmitDecision {
    // `discard:"old"` always admits (it evicts oldest afterward). Only
    // `discard:"reject"` can refuse, and only when a cap is set and the write
    // would push retained occupancy beyond it (DESIGN §5.3).
    if discard == crate::types::Discard::Old {
        return AdmitDecision::Admit;
    }
    let proj_records = current_records.saturating_add(incoming_records);
    let proj_bytes = current_bytes.saturating_add(incoming_bytes);
    if cap_records > 0 && proj_records > cap_records {
        return AdmitDecision::Reject;
    }
    if cap_bytes > 0 && proj_bytes > cap_bytes {
        return AdmitDecision::Reject;
    }
    AdmitDecision::Admit
}

/// Build a tombstone for a cursor that fell below `earliest_seq`
/// (DESIGN §5.4). `gap_from = from_seq + 1`, `gap_to = earliest_seq - 1`.
pub fn build_tombstone(
    from_seq: u64,
    earliest_seq: u64,
    head_seq: u64,
    reason: TombstoneReason,
) -> Tombstone {
    let gap_from = from_seq.saturating_add(1);
    let gap_to = earliest_seq.saturating_sub(1);
    let missed_estimate = gap_to.saturating_sub(gap_from).saturating_add(1);
    Tombstone {
        gap_from,
        gap_to,
        reason,
        missed_estimate,
        earliest_seq,
        head_seq,
    }
}
