# Storage Formats

This document records the current on-disk format. It is separate from
`docs/STORAGE_GUARANTEES.md`: this file describes bytes and versions; the guarantees file
describes crash behavior and recovery rules.

## Current versions

| Layer | Version | Constant |
|---|---:|---|
| Storage contract | `2` | `src/storage/mod.rs::STORAGE_FORMAT_VERSION` |
| WAL frame bodies | `2` | `src/storage/wal.rs::WAL_FORMAT_VERSION` |
| Segment `.data` / `.idx` | `1` | `src/storage/segment.rs::SEGMENT_FORMAT_VERSION` |
| Snapshot body | `3` | `src/storage/snapshot.rs::SNAPSHOT_VERSION` |

Event sequence numbers are `u64` everywhere: API records, WAL append frames, segment records,
snapshots, router cursors, lease events, and watermarks.

Topic ids are interned `u64` storage ids. The public topic identity remains the topic name.
Delete + recreate is a new topic instance; use `(topic_id, epoch)` internally when the
distinction matters.

## WAL frame format v2

Each WAL record is one length-prefixed frame. Multi-byte integers are little-endian.

```text
off  size  field
  0    4   frame_len   u32   bytes after this prefix
  4    1   type        u8    1=Append 2=TopicCreate 3=TopicDelete 4=RouterCreate
                               5=RouterDelete 6=Delete 7=EvictWatermark
                               8=CheckpointMark 9=ConfigUpdate 10=Lease
                               11=HeadWatermark
  5    1   flags       u8    bit0=has_tag bit1=has_node bit2=durable
  6    8   topic_id    u64   interned storage topic id; 0 for topic-agnostic frames
 14    8   seq         u64   append/lease seq; 0 for most control frames
 22    8   ts          u64   commit timestamp in ms
 30    2   node_len    u16
 32    2   tag_len     u16
 34    4   data_len    u32
 38    N   node        bytes
  .    M   tag         bytes
  .    P   data        bytes
  .    8   xxh3        u64   XXH3-64 over bytes [4..crc_start)
```

Control records encode their body in the `data` section. `RouterCreate` carries the router
definition, filter, initial source cursor, initial destination base, and guarantee bit.

## Segment format v1

Segment `.data` files contain sealed immutable record frames:

```text
off  size  field
  0    4   frame_len   u32   bytes after this prefix
  4    1   flags       u8    bit0=has_tag bit1=has_node
  5    8   seq         u64
 13    8   ts          u64
 21    2   node_len    u16
 23    2   tag_len     u16
 25    4   data_len    u32
 29    N   node
  .    M   tag
  .    P   data+meta JSON bytes
  .    8   xxh3        u64   checksum over the frame body before delete flag
  .    1   del_flag    u8    live/deleted marker outside the checksum
```

Segment `.idx` files are fixed-stride entries:

```text
offset:u32, len:u32, ts:u64, flags:u8, pad:3
```

The entry for seq `s` is at `(s - first_seq) * 20`.

## Snapshot format v3

Snapshot files are a small fixed header plus a postcard-encoded body:

```text
magic:u32, version:u32, body_len:u32, reserved:u32, xxh3:u64, body:bytes
```

The v3 body stores:

- `next_topic_id: u64`
- per-topic materialized state, including `topic_id: u64`, `epoch: u64`, head/base seqs,
  floors, live records, retained bytes, and source-trim floor
- per-router definitions, forward cursor, forwarded total, destination base, and guarantee bit
- per-WAL-group checkpoint positions keyed by physical WAL group identity

Unsupported snapshot versions are skipped instead of decoded.
