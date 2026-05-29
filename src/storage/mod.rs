//! Durability/persistence layer (phase 4).
//!
//! The WAL ([`wal`]) is the durability boundary: "only data not yet in the WAL
//! is lost" (ARCHITECTURE §0.3). Everything downstream — the in-memory
//! [`crate::engine`] index, segments — is a derivable cache of WAL + snapshots.
//!
//! The WAL ([`wal`]) provides the **format + single-writer group commit** and a
//! torn-tail-safe reader. The engine appends a frame for every mutating op and,
//! for a `durable` box, blocks the write until the group `fdatasync` returns
//! (Stage 2 wiring). Later stages add the compactor, segments, metadata
//! snapshots, and full restart recovery on top of these primitives; Stage 2
//! already replays the active WAL on startup so durable writes survive restart.
//!
//! # Tiered storage (Phase 6)
//!
//! [`segment`] defines the per-box segment file format (`.data` framed records +
//! `.idx` fixed-stride locator) — the long-term materialization of the WAL's
//! `Append` records, sealed at a size/event/age cap. [`segstore`] defines the
//! [`SegmentStore`] trait and its [`LocalSegmentStore`], plus a per-box
//! [`BoxTier`] = a HOT store (fast NVMe, under the data dir) and an optional COLD
//! store (`STREAMS_COLD_DIR`). Cold reads / relocation run on a blocking pool off
//! the hot path so they never block writes or live delivery. When no cold tier is
//! configured (the default), nothing relocates and behavior is unchanged. An S3
//! store is a future impl of the same trait. Stage 1 builds the trait, format,
//! and config; wiring into the write/serve path lands in later stages.

pub mod segment;
pub mod segstore;
pub mod snapshot;
pub mod wal;

pub use segment::{
    data_name, decode_data_frame, encode_data_frame, encode_idx_entry, idx_entry_at, idx_len,
    idx_name, lookup, IdxEntry, SegmentBuilder, SegmentError, SegmentRecord, IDX_STRIDE,
    SEG_FRAME_CRC_LEN, SEG_FRAME_HEADER_LEN,
};
pub use segstore::{
    BoxTier, LocalSegmentStore, SegmentId, SegmentPart, SegmentStore, StoreError, Tier,
};
pub use snapshot::{
    load_latest, next_snapshot_id, write_snapshot, Checkpoint, Snapshot, SnapshotBox,
    SnapshotError, SnapshotRecord, SnapshotRouter,
};
pub use wal::{
    BoxConfigOp, CommitToken, LeaseEvent, MatchSel, RouterOp, Wal, WalConfig, WalError, WalFrame,
    WalReader, WalRecord, WalWriter, FRAME_CRC_LEN, FRAME_HEADER_LEN,
};
