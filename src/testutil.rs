//! Shared test utilities for the crash/fault integration corpus.
//!
//! Test-only: gated behind `cfg(test)` (the lib's own unit tests) or the
//! `test-fs` feature (the integration crash harness in `tests/`), so a release
//! build never compiles it and production is byte-for-byte unaffected.
//!
//! # Tiered crash-point sweeps
//!
//! The crash/fault corpus is built around *exhaustive* sweeps: for a small
//! durable workload that issues `M` mutating FS calls, replay the workload on a
//! fresh disk and `crash()` after each of `0..=M` calls, then recover and assert
//! the oracle. Running the full `0..=M` matrix across ~35 files x 3 feature
//! combos costs ~30 minutes of wall time, which kills the edit/test loop.
//!
//! [`crash_points`] tiers that sweep WITHOUT losing any boundary coverage:
//!
//!   * By DEFAULT it returns a small, deterministic, fixed-seed *sample* of the
//!     `0..=total` range that always includes both endpoints (`0` and `total`),
//!     so every sweep still exercises the no-op-before-any-write and
//!     after-the-last-write boundaries plus a spread of interior crash points.
//!   * When `STREAMS_TEST_EXHAUSTIVE` is set to a truthy value (`1`/`true`/…),
//!     it returns the FULL `0..=total` range, so nightly/opt-in CI runs the
//!     complete matrix. (See `.github/workflows/ci.yml`.)
//!
//! The sample is deterministic (seeded by `total`, no RNG state) so a failure is
//! always reproducible, and it never collapses to zero points: even `total == 0`
//! yields `[0]`. Crucially, the default and exhaustive sets agree on the
//! endpoints, so a bug at a boundary is caught in both modes.

/// Whether the opt-in exhaustive crash matrix is enabled
/// (`STREAMS_TEST_EXHAUSTIVE=1`). Off by default.
pub fn exhaustive_enabled() -> bool {
    std::env::var("STREAMS_TEST_EXHAUSTIVE")
        .map(|v| matches!(v.as_str(), "1" | "true" | "yes" | "on" | "TRUE"))
        .unwrap_or(false)
}

/// The default cap on the number of distinct crash points sampled per sweep when
/// the exhaustive matrix is OFF. Small enough to keep each sweep to a handful of
/// engine boots (each sweep iteration boots + recovers an engine, ~1-3s of CPU),
/// large enough to always cover both endpoints (`0` and `total`) plus a couple of
/// deterministic interior crash points. Can be raised (never below the two
/// endpoints) via `STREAMS_TEST_SAMPLE` for a middle-ground "wider but still fast"
/// run, or bypassed entirely with `STREAMS_TEST_EXHAUSTIVE=1`.
const DEFAULT_SAMPLE_CAP: usize = 4;

fn sample_cap() -> usize {
    std::env::var("STREAMS_TEST_SAMPLE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .map(|v| v.max(2))
        .unwrap_or(DEFAULT_SAMPLE_CAP)
}

/// The crash points a sweep over a workload of `total` mutating FS calls should
/// probe, tiered by [`exhaustive_enabled`].
///
/// * Exhaustive (`STREAMS_TEST_EXHAUSTIVE=1`): every point in `0..=total`.
/// * Default: a deterministic sample of at most [`sample_cap`] points,
///   ALWAYS including `0` and `total`, with the interior points spread evenly
///   across the range (a fixed, RNG-free stride seeded only by `total`). The
///   result is sorted, de-duplicated, and never empty (`total == 0` ⇒ `[0]`).
///
/// Use the returned `Vec<u64>` directly as the sweep's iteration set:
///
/// ```ignore
/// for crash_point in streams::testutil::crash_points(total_writes) {
///     // ... boot a fresh disk, crash() after `crash_point` FS calls, recover ...
/// }
/// ```
pub fn crash_points(total: u64) -> Vec<u64> {
    if exhaustive_enabled() {
        return (0..=total).collect();
    }
    sampled_points(total, sample_cap())
}

/// Deterministic interior-spread sampler shared by [`crash_points`]; split out so
/// it is unit-testable without touching the environment.
fn sampled_points(total: u64, cap: usize) -> Vec<u64> {
    // The full range has `total + 1` points (0..=total). If it already fits under
    // the cap, probe all of them — no need to sample.
    let full_len = total.saturating_add(1);
    if full_len <= cap as u64 {
        return (0..=total).collect();
    }

    // Sample `cap` points spread evenly across [0, total], anchored at both
    // endpoints. `cap >= 2` is guaranteed by `sample_cap()`; this helper is also
    // called from tests with explicit caps, so clamp here too.
    let cap = cap.max(2);
    let mut pts: Vec<u64> = Vec::with_capacity(cap);
    let last = (cap - 1) as u64;
    for i in 0..cap as u64 {
        // Evenly spaced including both ends: round(i * total / (cap - 1)).
        // Done in u128 to avoid overflow for large `total`.
        let p = ((i as u128 * total as u128) / last as u128) as u64;
        pts.push(p);
    }
    pts.sort_unstable();
    pts.dedup();
    pts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_ranges_are_returned_whole() {
        // When the full 0..=total range fits under the cap, every point is kept.
        assert_eq!(sampled_points(0, 6), vec![0]);
        assert_eq!(sampled_points(3, 6), vec![0, 1, 2, 3]);
        assert_eq!(sampled_points(5, 6), vec![0, 1, 2, 3, 4, 5]);
    }

    #[test]
    fn large_ranges_are_sampled_with_endpoints() {
        let pts = sampled_points(100, 6);
        // Capped at the requested size, endpoints always present, sorted+unique.
        assert!(pts.len() <= 6, "len {} exceeds cap", pts.len());
        assert_eq!(*pts.first().unwrap(), 0, "0 boundary always sampled");
        assert_eq!(*pts.last().unwrap(), 100, "total boundary always sampled");
        let mut sorted = pts.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(pts, sorted, "result is sorted + de-duplicated");
    }

    #[test]
    fn deterministic_across_calls() {
        assert_eq!(sampled_points(1000, 6), sampled_points(1000, 6));
    }

    #[test]
    fn never_empty_and_min_cap_two() {
        assert!(!sampled_points(0, 2).is_empty());
        let pts = sampled_points(50, 2);
        assert_eq!(pts, vec![0, 50], "cap 2 ⇒ just the endpoints");
    }
}
