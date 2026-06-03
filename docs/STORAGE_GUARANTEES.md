# Storage Guarantees

This project owns its WAL, segment, and snapshot formats. The goal is SQLite-style
discipline for a log service: never trust a partial write, never publish a failed
commit, make file-format changes explicit, and make recovery deterministic.

For byte-level layouts, see `docs/STORAGE_FORMATS.md`.

## Format versions

Current on-disk contract:

| Layer | Version | Where it is defined |
|---|---:|---|
| Storage contract | `2` | `src/storage/mod.rs` (`STORAGE_FORMAT_VERSION`) |
| WAL frame bodies | `2` | `src/storage/wal.rs` (`WAL_FORMAT_VERSION`) |
| Segment `.data` / `.idx` | `1` | `src/storage/segment.rs` (`SEGMENT_FORMAT_VERSION`) |
| Snapshot body | `3` | `src/storage/snapshot.rs` (`SNAPSHOT_VERSION`) |

Any incompatible storage change must bump the relevant version and document the
new recovery behavior before it ships. Snapshots carry an embedded version and
unsupported versions are skipped rather than decoded. WAL and segment readers
accept the current frame layouts only; a future incompatible WAL or segment
layout must use a new frame type, explicit versioned envelope, or a new file
namespace so old readers do not misparse it.

## Crash contract

- `fsync` topics: an acknowledged write survives process crash, kernel crash, and
  power loss once the disk honors `fdatasync`.
- `disk` topics: acknowledged writes survive clean shutdown and normal process
  restart, but a crash may lose the most recent un-fsynced WAL tail.
- `memory` topics: records are best-effort and may survive or be lost after
  restart. The topic config persists.
- `ephemeral` topics: records are RAM-only by design. Topic config/control state
  persists, and sequence numbers remain monotonic.
- A write that fails the WAL commit path publishes nothing visible.
- Recovery never fabricates records from torn or corrupt bytes. At worst it
  truncates to the last valid WAL frame or surfaces sealed-segment corruption.

## File rules

- WAL frames are length-prefixed and XXH3-64 protected. A torn length, overrun,
  inconsistent body, unknown type, invalid UTF-8, or checksum mismatch is the
  logical end of the WAL shard and is truncated before appending again.
- Snapshots are written through temp file, file sync, rename, and directory sync.
  A torn or checksum-bad snapshot is ignored; recovery falls back to the previous
  valid snapshot or WAL replay.
- Segment data is immutable once sealed. A checksum-bad sealed frame is explicit
  corruption, not a silent tail. Segment deletes flip one trailing byte outside
  the checksum so a crash mid-flip leaves the record either live or deleted, never
  structurally corrupt.
- WAL recovery is shard-count agnostic. It replays every discovered WAL group by
  `topic_id`, so changing `TOPICS_WAL_SHARDS` between restarts does not skip data.

## Verification

Use `./scripts/test-quick.sh` for the default local check. It exercises formatting,
compile-only checks for the library/server binary, and one in-process HTTP smoke
test. It deliberately skips the broad unit/integration corpus, fault injection,
failpoints, crash matrices, proptest/fuzz, benchmark sweeps, docs builds, and
deeper queue/router/SSE/WebSocket matrices.

The full gate is `./scripts/test-full.sh`. Do not run it by default; use it only
when explicitly requested, before release/landing, or when storage/recovery
behavior changed in a way the quick gate cannot cover. See `docs/FAULT_TESTING.md`
for the model-oracle invariants.
