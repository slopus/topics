//! Durability/persistence layer (phase 4).
//!
//! The WAL ([`wal`]) is the durability boundary: "only data not yet in the WAL
//! is lost" (ARCHITECTURE §0.3). Everything downstream — the in-memory
//! [`crate::engine`] index, segments — is a derivable cache of WAL + snapshots.
//!
//! The WAL ([`wal`]) provides the **format + single-writer group commit** and a
//! torn-tail-safe reader. The engine appends a frame for every mutating op and,
//! for a `durable` topic, blocks the write until the group `fdatasync` returns
//! (Stage 2 wiring). Later stages add the compactor, segments, metadata
//! snapshots, and full restart recovery on top of these primitives; Stage 2
//! already replays the active WAL on startup so durable writes survive restart.
//!
//! # Tiered storage (Phase 6)
//!
//! [`segment`] defines the per-topic segment file format (`.data` framed records +
//! `.idx` fixed-stride locator) — the long-term materialization of the WAL's
//! `Append` records, sealed at a size/event/age cap. [`segstore`] defines the
//! [`SegmentStore`] trait and its [`LocalSegmentStore`], plus a per-topic
//! [`TopicTier`] = a HOT store (fast NVMe, under the data dir) and an optional COLD
//! store (`TOPICS_COLD_DIR`). Cold reads / relocation run on a blocking pool off
//! the hot path so they never block writes or live delivery. When no cold tier is
//! configured (the default), nothing relocates and behavior is unchanged. An S3
//! store is a future impl of the same trait. Stage 1 builds the trait, format,
//! and config; wiring into the write/serve path lands in later stages.

pub mod fs;
pub mod segment;
pub mod segstore;
pub mod sharded_wal;
pub mod snapshot;
pub mod wal;

/// The hostile filesystem implementations (FakeDisk / FaultFs / MonitorFs) the
/// crash-consistency harness injects through the `*_with` constructors. Test-only:
/// gated behind `cfg(test)` (the lib's own unit tests) or the `test-fs` feature
/// (the integration crash harness in `tests/`), so a release build never compiles
/// them and production stays on [`RealFs`].
#[cfg(any(test, feature = "test-fs"))]
pub mod testfs;

pub use fs::{File, Fs, OpenOpts, RealFs};

pub use segment::{
    data_name, decode_data_frame, decode_data_frame_full, del_flag_offset_in_frame,
    encode_data_frame, encode_idx_entry, frame_is_deleted, idx_entry_at, idx_len, idx_name, lookup,
    IdxEntry, SegmentBuilder, SegmentError, SegmentRecord, IDX_STRIDE, SEG_DEL_LIVE,
    SEG_DEL_SENTINEL, SEG_FRAME_CRC_LEN, SEG_FRAME_DEL_LEN, SEG_FRAME_HEADER_LEN,
};
pub use segstore::{
    LocalSegmentStore, SegmentId, SegmentPart, SegmentStore, StoreError, Tier, TopicTier,
};
pub use sharded_wal::{shard_for_topic, shard_wal_dir, ShardedWal, ShardedWalWriter};
pub use snapshot::{
    load_latest, load_latest_with, next_snapshot_id, next_snapshot_id_with, write_snapshot,
    write_snapshot_with, Checkpoint, Snapshot, SnapshotError, SnapshotRecord, SnapshotRouter,
    SnapshotTopic,
};
pub use wal::{
    CommitToken, LeaseEvent, MatchSel, RouterOp, TopicConfigOp, Wal, WalConfig, WalError, WalFrame,
    WalMetrics, WalReader, WalRecord, WalWriter, FRAME_CRC_LEN, FRAME_HEADER_LEN, FSYNC_BUCKETS_US,
};
