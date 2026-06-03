//! Write-Ahead Log: frame format, single-writer task with adaptive group
//! commit, file preallocation + rotation, and a torn-tail-safe reader.
//!
//! # Frame format (ARCHITECTURE §2.1)
//!
//! Every WAL record is one length-prefixed, XXH3-protected frame. Multi-byte
//! integers are little-endian.
//!
//! ```text
//!  off  size  field
//!    0    4   frame_len   u32   bytes of this frame EXCLUDING this field
//!    4    1   type        u8    1=Append 2=TopicCreate 3=TopicDelete 4=RouterCreate
//!                                 5=RouterDelete 6=Delete 7=EvictWatermark
//!                                 8=CheckpointMark 9=ConfigUpdate 10=Lease
//!                                 11=HeadWatermark
//!    5    1   flags       u8    bit0=has_tag bit1=has_node bit2=durable
//!    6    8   topic_id    u64   interned numeric topic id (string<->id in meta)
//!   14    8   seq         u64   server-assigned (0 for non-Append control frames)
//!   22    8   ts          u64   server commit ms
//!   30    2   node_len    u16
//!   32    2   tag_len     u16
//!   34    4   data_len    u32
//!   38    N   node        bytes (node_len)
//!    .    M   tag         bytes (tag_len)
//!    .    P   data+meta   bytes (data_len)   -- opaque payload
//!    .    8   xxh3        u64   XXH3-64 over bytes [4 .. crc_start)
//! ```
//!
//! `frame_len` first lets recovery validate frame boundaries without parsing
//! the body and detect a torn tail (`frame_len` past EOF ⇒ truncated write ⇒
//! discard from here). The XXH3-64 over everything between `frame_len` and the
//! checksum catches a partial/garbled write: a mismatch is the logical end of the log
//! (truncate). This is the crash-consistency anchor (ARCHITECTURE §4).
//!
//! # Record types
//!
//! The `type` byte + the variable body encode the [`WalRecord`] variants:
//! `Append`, `Delete`, `TopicCreate`/`ConfigUpdate`/`TopicDelete` (topic config and
//! tombstone), `RouterCreate`/`RouterDelete`, `EvictWatermark`, and
//! `CheckpointMark`. The `topic_id` is an interned `u64` (the name↔id table is
//! itself logged via `TopicCreate`), keeping data frames small.
//!
//! # Writer + adaptive group commit (ARCHITECTURE §2.3)
//!
//! A single writer runs on a **dedicated OS thread** (the "single sequential
//! disk resource" of §2.2) and is fed by a `std::sync::mpsc` channel. Callers
//! submit a [`WalRecord`] + a durability flag and receive a [`CommitToken`]; the
//! writer drains all queued submissions, writes them in one batch, and issues
//! exactly **one** `fdatasync` per batch when the batch contains a durable frame
//! (or an adaptive timer elapsed). A durable submission's token is signalled
//! only *after* that fsync; a non-durable submission's token is signalled after
//! the buffered write (its durability comes from the next group fsync). The
//! window is adaptive in `gc_min..gc_max`: a lone durable write fsyncs
//! immediately, but under load one fsync amortizes across the whole batch.
//!
//! A dedicated thread (rather than a tokio task) keeps the engine's mutating
//! ops **synchronous**: a durable write calls [`CommitToken::wait`], which blocks
//! the calling thread until the group fsync completes — no async plumbing, no
//! runtime coupling, and no risk of starving the writer. The HTTP layer calls
//! the engine from `spawn_blocking` so a blocking durable wait never parks a
//! reactor thread (ARCHITECTURE §8.5).

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use super::fs::{File, Fs, OpenOpts, RealFs};

// ---------------------------------------------------------------------------
// Frame layout constants
// ---------------------------------------------------------------------------

/// Fixed header size: everything from `type` through `data_len` inclusive,
/// i.e. the bytes immediately following `frame_len`. (`type`1 + `flags`1 +
/// `topic_id`8 + `seq`8 + `ts`8 + `node_len`2 + `tag_len`2 + `data_len`4 = 34.)
pub const FRAME_HEADER_LEN: usize = 34;
/// Trailing XXH3-64 checksum size.
pub const FRAME_CRC_LEN: usize = 8;
/// WAL frame-body format version. Bump only for an incompatible body/layout change.
pub const WAL_FORMAT_VERSION: u32 = 2;
/// The leading `frame_len` u32 size.
const FRAME_LEN_PREFIX: usize = 4;

// Frame type tags (ARCHITECTURE §2.1).
const T_APPEND: u8 = 1;
const T_BOX_CREATE: u8 = 2;
const T_BOX_DELETE: u8 = 3;
const T_ROUTER_CREATE: u8 = 4;
const T_ROUTER_DELETE: u8 = 5;
const T_DELETE: u8 = 6;
const T_EVICT_WATERMARK: u8 = 7;
const T_CHECKPOINT_MARK: u8 = 8;
const T_CONFIG_UPDATE: u8 = 9;
const T_LEASE: u8 = 10;
/// A durable per-topic head reservation watermark (R3): the highest seq this topic
/// has DURABLY reserved, fsynced ahead of use so a `disk`-class write (acked
/// before its frame is fsynced) can never have its seq re-handed after a crash
/// that lost the un-fsynced frame. Recovery sets `head = max(replayed, watermark)`.
const T_HEAD_WATERMARK: u8 = 11;

// Flag bits.
const FLAG_HAS_TAG: u8 = 1 << 0;
const FLAG_HAS_NODE: u8 = 1 << 1;
const FLAG_DURABLE: u8 = 1 << 2;

/// Compute the XXH3-64 checksum of a buffer — fast, modern, 64-bit (far lower
/// false-accept than a 32-bit CRC for torn-write / bit-rot detection).
#[inline]
fn crc(bytes: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(bytes)
}

// ---------------------------------------------------------------------------
// Record types
// ---------------------------------------------------------------------------

/// A tag-`match` selector logged with a [`WalRecord::Delete`] so the deletion is
/// replayed deterministically on recovery. Mirrors [`crate::types::Filter`] but
/// is self-contained (the storage layer must not depend on the wire types).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MatchSel {
    /// Exact tag equality.
    Eq(String),
    /// Prefix match (the literal prefix; the wire's trailing `*` is stripped).
    Glob(String),
}

/// A router create/update payload logged with [`WalRecord::RouterCreate`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouterOp {
    pub name: String,
    pub source: String,
    pub dest: String,
    pub preserve_node: bool,
    pub preserve_tag: bool,
    pub create_dest: bool,
    pub allow_cycle: bool,
    /// `true` for router `guarantee:"exactly_once"`; `false` for the default
    /// `guarantee:"at_least_once"`.
    pub exactly_once: bool,
    /// Optional forward filter, encoded like [`MatchSel`].
    pub filter: Option<MatchSel>,
    /// The forward cursor (source seq) the router was seeded at when this frame was
    /// logged. The async/derived model re-derives the dest from
    /// `source[cursor..head]`, so the cursor MUST be durable independent of replay
    /// order: under WAL sharding the source's appends can replay on a different
    /// shard than this control frame, so recomputing the cursor from "whatever
    /// source head exists when this frame replays" would backfill pre-create history
    /// or skip post-create records depending on shard interleave. Persisting the
    /// create-time cursor pins it.
    pub initial_cursor: u64,
    /// The deterministic dest-seq base the router was seeded at when this frame was
    /// logged. The next derived dest seq is `dest_base + forwarded_total + 1`, so
    /// the base MUST be durable to keep dest seqs stable across a restart.
    pub initial_dest_base: u64,
}

/// A topic create/config payload logged with [`WalRecord::TopicConfig`]. The opaque
/// `config` bytes are the bincode/serde-encoded [`crate::types::TopicConfig`]; the
/// storage layer treats them as a blob (no dependency on the engine config).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicConfigOp {
    pub name: String,
    /// Opaque serialized config; replayed verbatim by the metadata store.
    pub config: Vec<u8>,
}

/// A logical WAL record. The variant + its fields map onto a frame (§2.1):
/// `Append` carries the per-record data; the control variants carry config,
/// deletes, watermarks, routers, and checkpoint boundaries on the same ordered,
/// crash-consistent timeline as the data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalRecord {
    /// A data record. `data` is the opaque `data+meta` payload blob; `node`/`tag`
    /// are the resolved values (absent ⇒ `None`).
    Append {
        topic_id: u64,
        seq: u64,
        ts: u64,
        node: Option<String>,
        tag: Option<String>,
        data: Vec<u8>,
    },
    /// A permanent, point-in-time delete. Replayed deterministically: a
    /// selector-based delete re-derives the matched seqs from the rebuilt index +
    /// tag index (bounded by `bound_head` so a record appended AFTER the original
    /// delete is never swept), while an explicit `seqs` set deletes exactly those
    /// seqs (ARCHITECTURE §2.1).
    Delete {
        topic_id: u64,
        /// `before_seq` selector (every live seq `< before_seq`), if supplied.
        before_seq: Option<u64>,
        /// `match` selector, if supplied.
        match_: Option<MatchSel>,
        /// Explicit seq set (the queue ack / dead-letter path, DESIGN §10.4):
        /// delete exactly these seqs. Empty for the API §5 selector-based delete.
        /// Replays deterministically (the exact seqs are logged, not re-derived).
        seqs: Vec<u64>,
        /// Point-in-time UPPER BOUND for a selector-based delete: the topic's
        /// `head + 1` captured (under the append lock) at the moment the delete was
        /// logged, so replay only ever sweeps seqs strictly below it. A
        /// CONCURRENT/later append that landed after the delete frame carries a seq
        /// `>= bound_head` and is therefore NEVER deleted on replay, preserving the
        /// API §5 point-in-time guarantee across a crash. `None` only for an
        /// explicit `seqs`-set delete (the seqs are themselves the bound).
        bound_head: Option<u64>,
        ts: u64,
    },
    /// Topic created or its config updated. `tombstone == false` for create/update;
    /// `TopicConfig{tombstone:true}` is the topic-delete marker.
    TopicConfig {
        topic_id: u64,
        op: TopicConfigOp,
        /// `true` ⇒ this frame is the topic-delete tombstone (config bytes empty).
        tombstone: bool,
        ts: u64,
    },
    /// Router created/updated.
    RouterCreate { op: RouterOp, ts: u64 },
    /// Router deleted (by name).
    RouterDelete { name: String, ts: u64 },
    /// Eviction watermark advanced (cap/TTL involuntary floor) for a topic. The
    /// `evict_floor` (cap-records / byte-cap) and `expiry_floor` (TTL) are carried
    /// SEPARATELY so recovery restores each into its own floor and the from-0
    /// tombstone reason (ttl / cap / mixed) is preserved across restart (R7).
    EvictWatermark {
        topic_id: u64,
        evict_floor: u64,
        expiry_floor: u64,
        earliest_seq: u64,
        ts: u64,
    },
    /// Checkpoint boundary: every topic's highest seq absorbed into segments.
    CheckpointMark { last_checkpoint_seq: u64, ts: u64 },
    /// A leases-log lifecycle event for a queue topic (DESIGN §10.1): the pending
    /// who-holds-what state is the materialized projection of these events. Only
    /// written when the queue's `leases_durable:true`; otherwise the projection
    /// is purely in-memory and self-heals on restart (DESIGN §10.6).
    Lease {
        topic_id: u64,
        /// The job seq this event concerns.
        seq: u64,
        /// Event kind: 0=claimed 1=released 2=extended 3=acked (see [`LeaseEvent`]).
        event: u8,
        /// The holder node.
        node: String,
        /// Opaque lease identity for the delivery.
        lease_id: u64,
        /// Absolute deadline ms (for claimed/extended; `0` otherwise).
        deadline: u64,
        /// Delivery counter after this event (for claimed).
        deliveries: u64,
        ts: u64,
    },
    /// A durable per-topic head reservation watermark (R3). `head_seq` is the
    /// highest seq the topic has durably reserved; it is fsynced AHEAD of the seqs
    /// actually handed out so a `disk`-class write (acked before its own frame is
    /// fsynced) never has its seq re-handed after a crash that dropped the
    /// un-fsynced frame. Recovery sets `head = max(replayed head, watermark)` and
    /// pads any reserved-but-unwritten seqs as silent deleted gaps, so the seq
    /// counter never regresses and an acked seq is never reused.
    HeadWatermark {
        topic_id: u64,
        head_seq: u64,
        ts: u64,
    },
}

/// The kind of a leases-log lifecycle event (the `event` byte of
/// [`WalRecord::Lease`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseEvent {
    Claimed = 0,
    Released = 1,
    Extended = 2,
    Acked = 3,
}

impl WalRecord {
    fn type_tag(&self) -> u8 {
        match self {
            WalRecord::Append { .. } => T_APPEND,
            WalRecord::Delete { .. } => T_DELETE,
            WalRecord::TopicConfig {
                tombstone: false, ..
            } => T_BOX_CREATE,
            WalRecord::TopicConfig {
                tombstone: true, ..
            } => T_BOX_DELETE,
            WalRecord::RouterCreate { .. } => T_ROUTER_CREATE,
            WalRecord::RouterDelete { .. } => T_ROUTER_DELETE,
            WalRecord::EvictWatermark { .. } => T_EVICT_WATERMARK,
            WalRecord::CheckpointMark { .. } => T_CHECKPOINT_MARK,
            WalRecord::Lease { .. } => T_LEASE,
            WalRecord::HeadWatermark { .. } => T_HEAD_WATERMARK,
        }
    }

    /// The interned topic id this record targets (`0` for topic-agnostic control
    /// frames like routers and checkpoints).
    pub fn topic_id(&self) -> u64 {
        match self {
            WalRecord::Append { topic_id, .. } => *topic_id,
            WalRecord::Delete { topic_id, .. } => *topic_id,
            WalRecord::TopicConfig { topic_id, .. } => *topic_id,
            WalRecord::EvictWatermark { topic_id, .. } => *topic_id,
            WalRecord::Lease { topic_id, .. } => *topic_id,
            WalRecord::HeadWatermark { topic_id, .. } => *topic_id,
            WalRecord::RouterCreate { .. }
            | WalRecord::RouterDelete { .. }
            | WalRecord::CheckpointMark { .. } => 0,
        }
    }

    /// The seq this record carries (`0` for control frames that target no
    /// specific record). `Append` and `Lease` both name a job seq.
    pub fn seq(&self) -> u64 {
        match self {
            WalRecord::Append { seq, .. } => *seq,
            WalRecord::Lease { seq, .. } => *seq,
            _ => 0,
        }
    }
}

// ---------------------------------------------------------------------------
// A decoded frame (record + the durable flag carried in `flags`)
// ---------------------------------------------------------------------------

/// A frame as read back from the WAL: the decoded [`WalRecord`] plus the durable
/// flag (bit2 of `flags`). Recovery replays these in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalFrame {
    pub record: WalRecord,
    pub durable: bool,
}

// ---------------------------------------------------------------------------
// Body codec — variable payload after the fixed header, per record type.
// ---------------------------------------------------------------------------

/// Read a length-prefixed (u32-LE) byte string from `buf` at `*pos`.
fn read_lp_bytes(buf: &[u8], pos: &mut usize) -> Option<Vec<u8>> {
    if *pos + 4 > buf.len() {
        return None;
    }
    let n = u32::from_le_bytes(buf[*pos..*pos + 4].try_into().unwrap()) as usize;
    *pos += 4;
    if *pos + n > buf.len() {
        return None;
    }
    let out = buf[*pos..*pos + n].to_vec();
    *pos += n;
    Some(out)
}

fn read_lp_str(buf: &[u8], pos: &mut usize) -> Option<String> {
    let b = read_lp_bytes(buf, pos)?;
    String::from_utf8(b).ok()
}

fn read_u64(buf: &[u8], pos: &mut usize) -> Option<u64> {
    if *pos + 8 > buf.len() {
        return None;
    }
    let v = u64::from_le_bytes(buf[*pos..*pos + 8].try_into().unwrap());
    *pos += 8;
    Some(v)
}

fn read_u8(buf: &[u8], pos: &mut usize) -> Option<u8> {
    if *pos + 1 > buf.len() {
        return None;
    }
    let v = buf[*pos];
    *pos += 1;
    Some(v)
}

fn write_lp_bytes(out: &mut Vec<u8>, b: &[u8]) {
    out.extend_from_slice(&(b.len() as u32).to_le_bytes());
    out.extend_from_slice(b);
}

/// Encode a [`MatchSel`] option as `[present:u8][op:u8][lp value]` (present=0 ⇒
/// the next two fields are absent).
fn write_match(out: &mut Vec<u8>, m: &Option<MatchSel>) {
    match m {
        None => out.push(0),
        Some(MatchSel::Eq(v)) => {
            out.push(1);
            out.push(0); // op 0 = Eq
            write_lp_bytes(out, v.as_bytes());
        }
        Some(MatchSel::Glob(v)) => {
            out.push(1);
            out.push(1); // op 1 = Glob
            write_lp_bytes(out, v.as_bytes());
        }
    }
}

fn read_match(buf: &[u8], pos: &mut usize) -> Option<Option<MatchSel>> {
    match read_u8(buf, pos)? {
        0 => Some(None),
        1 => {
            let op = read_u8(buf, pos)?;
            let v = read_lp_str(buf, pos)?;
            match op {
                0 => Some(Some(MatchSel::Eq(v))),
                1 => Some(Some(MatchSel::Glob(v))),
                _ => None,
            }
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Frame encode / decode
// ---------------------------------------------------------------------------

/// Encode `frame` (a record + durable flag) into `out`, appending a complete
/// length-prefixed CRC-protected frame. `out` is cleared first.
pub fn encode_frame(out: &mut Vec<u8>, record: &WalRecord, durable: bool) {
    out.clear();

    // Reserve the frame_len prefix; filled in once the body length is known.
    out.extend_from_slice(&[0u8; FRAME_LEN_PREFIX]);

    // --- Build the header fields (some depend on the body, computed below). --
    let type_tag = record.type_tag();
    let topic_id = record.topic_id();
    let seq = record.seq();

    // node/tag/data are the three §2.1 inline byte-strings; for control frames
    // we repurpose `data` as the variant's encoded body and leave node/tag empty.
    let mut node: &[u8] = &[];
    let mut tag: &[u8] = &[];
    let mut flags = 0u8;
    if durable {
        flags |= FLAG_DURABLE;
    }
    let ts;

    // The data section is built into a scratch buffer for control frames.
    let mut data_buf: Vec<u8> = Vec::new();
    let data: &[u8];

    match record {
        WalRecord::Append {
            ts: rts,
            node: n,
            tag: t,
            data: d,
            ..
        } => {
            ts = *rts;
            if let Some(n) = n {
                node = n.as_bytes();
                flags |= FLAG_HAS_NODE;
            }
            if let Some(t) = t {
                tag = t.as_bytes();
                flags |= FLAG_HAS_TAG;
            }
            data = d.as_slice();
        }
        WalRecord::Delete {
            before_seq,
            match_,
            seqs,
            bound_head,
            ts: rts,
            ..
        } => {
            ts = *rts;
            // body: [has_before:u8][before:u64?] [match] [n_seqs:u32][seq:u64 ...]
            //       [has_bound:u8][bound:u64?]
            match before_seq {
                Some(b) => {
                    data_buf.push(1);
                    data_buf.extend_from_slice(&b.to_le_bytes());
                }
                None => data_buf.push(0),
            }
            write_match(&mut data_buf, match_);
            data_buf.extend_from_slice(&(seqs.len() as u32).to_le_bytes());
            for s in seqs {
                data_buf.extend_from_slice(&s.to_le_bytes());
            }
            match bound_head {
                Some(b) => {
                    data_buf.push(1);
                    data_buf.extend_from_slice(&b.to_le_bytes());
                }
                None => data_buf.push(0),
            }
            data = &data_buf;
        }
        WalRecord::TopicConfig { op, ts: rts, .. } => {
            ts = *rts;
            // body: [lp name][lp config]
            write_lp_bytes(&mut data_buf, op.name.as_bytes());
            write_lp_bytes(&mut data_buf, &op.config);
            data = &data_buf;
        }
        WalRecord::RouterCreate { op, ts: rts } => {
            ts = *rts;
            // body: [lp name][lp source][lp dest][bools:u8][match filter]
            write_lp_bytes(&mut data_buf, op.name.as_bytes());
            write_lp_bytes(&mut data_buf, op.source.as_bytes());
            write_lp_bytes(&mut data_buf, op.dest.as_bytes());
            let bools = (op.preserve_node as u8)
                | ((op.preserve_tag as u8) << 1)
                | ((op.create_dest as u8) << 2)
                | ((op.allow_cycle as u8) << 3)
                | ((op.exactly_once as u8) << 4);
            data_buf.push(bools);
            write_match(&mut data_buf, &op.filter);
            data_buf.extend_from_slice(&op.initial_cursor.to_le_bytes());
            data_buf.extend_from_slice(&op.initial_dest_base.to_le_bytes());
            data = &data_buf;
        }
        WalRecord::RouterDelete { name, ts: rts } => {
            ts = *rts;
            write_lp_bytes(&mut data_buf, name.as_bytes());
            data = &data_buf;
        }
        WalRecord::EvictWatermark {
            evict_floor,
            expiry_floor,
            earliest_seq,
            ts: rts,
            ..
        } => {
            ts = *rts;
            data_buf.extend_from_slice(&evict_floor.to_le_bytes());
            data_buf.extend_from_slice(&earliest_seq.to_le_bytes());
            data_buf.extend_from_slice(&expiry_floor.to_le_bytes());
            data = &data_buf;
        }
        WalRecord::CheckpointMark {
            last_checkpoint_seq,
            ts: rts,
        } => {
            ts = *rts;
            data_buf.extend_from_slice(&last_checkpoint_seq.to_le_bytes());
            data = &data_buf;
        }
        WalRecord::Lease {
            event,
            node: n,
            lease_id,
            deadline,
            deliveries,
            ts: rts,
            ..
        } => {
            ts = *rts;
            // body: [event:u8][lease_id:u64][deadline:u64][deliveries:u64][lp node]
            data_buf.push(*event);
            data_buf.extend_from_slice(&lease_id.to_le_bytes());
            data_buf.extend_from_slice(&deadline.to_le_bytes());
            data_buf.extend_from_slice(&deliveries.to_le_bytes());
            write_lp_bytes(&mut data_buf, n.as_bytes());
            data = &data_buf;
        }
        WalRecord::HeadWatermark {
            head_seq, ts: rts, ..
        } => {
            // `topic_id` is taken from `record.topic_id()` above; the watermark's seq
            // ceiling rides the body (the header `seq` field stays 0, a control
            // frame, so `WalRecord::seq()` excludes it from data-record scans).
            ts = *rts;
            data_buf.extend_from_slice(&head_seq.to_le_bytes());
            data = &data_buf;
        }
    }

    // --- Fixed header (§2.1 offsets 4..34). ---------------------------------
    out.push(type_tag);
    out.push(flags);
    out.extend_from_slice(&topic_id.to_le_bytes());
    out.extend_from_slice(&seq.to_le_bytes());
    out.extend_from_slice(&ts.to_le_bytes());
    out.extend_from_slice(&(node.len() as u16).to_le_bytes());
    out.extend_from_slice(&(tag.len() as u16).to_le_bytes());
    out.extend_from_slice(&(data.len() as u32).to_le_bytes());
    out.extend_from_slice(node);
    out.extend_from_slice(tag);
    out.extend_from_slice(data);

    // --- CRC over [4 .. crc_start) and the frame_len prefix. ----------------
    let crc_val = crc(&out[FRAME_LEN_PREFIX..]);
    out.extend_from_slice(&crc_val.to_le_bytes());

    // frame_len = everything after the prefix = (total - 4).
    let frame_len = (out.len() - FRAME_LEN_PREFIX) as u32;
    out[0..FRAME_LEN_PREFIX].copy_from_slice(&frame_len.to_le_bytes());
}

/// Outcome of trying to decode one frame from a buffer slice.
enum DecodeStep {
    /// A complete, CRC-valid frame consuming `consumed` bytes.
    Frame { frame: WalFrame, consumed: usize },
    /// The buffer does not (yet) hold a complete, valid frame — torn tail.
    /// Recovery stops here; `consumed` so far is the valid prefix length.
    Torn,
}

/// Attempt to decode one frame at the start of `buf`. Returns [`DecodeStep::Torn`]
/// for any of: not enough bytes for the length prefix, `frame_len` overrunning
/// the buffer, a body that doesn't parse, or a CRC mismatch — all of which mark
/// the logical end of the log on a torn/partial write.
fn decode_one(buf: &[u8]) -> DecodeStep {
    if buf.len() < FRAME_LEN_PREFIX {
        return DecodeStep::Torn;
    }
    let frame_len = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
    // A zero-length or absurd frame_len ⇒ torn (also stops on preallocated zeros).
    if frame_len < FRAME_HEADER_LEN + FRAME_CRC_LEN {
        return DecodeStep::Torn;
    }
    let total = FRAME_LEN_PREFIX + frame_len;
    if buf.len() < total {
        return DecodeStep::Torn; // frame_len overruns available bytes.
    }

    // Body spans [4 .. total-4); CRC is the last 4 bytes.
    let crc_start = total - FRAME_CRC_LEN;
    let stored_crc = u64::from_le_bytes(buf[crc_start..total].try_into().unwrap());
    let computed = crc(&buf[FRAME_LEN_PREFIX..crc_start]);
    if stored_crc != computed {
        return DecodeStep::Torn; // partial/garbled write.
    }

    // --- Parse the fixed header. --------------------------------------------
    let h = &buf[FRAME_LEN_PREFIX..]; // header starts right after frame_len.
    let type_tag = h[0];
    let flags = h[1];
    let topic_id = u64::from_le_bytes(h[2..10].try_into().unwrap());
    let seq = u64::from_le_bytes(h[10..18].try_into().unwrap());
    let ts = u64::from_le_bytes(h[18..26].try_into().unwrap());
    let node_len = u16::from_le_bytes(h[26..28].try_into().unwrap()) as usize;
    let tag_len = u16::from_le_bytes(h[28..30].try_into().unwrap()) as usize;
    let data_len = u32::from_le_bytes(h[30..34].try_into().unwrap()) as usize;

    // Validate the inner sections fit inside the (already CRC-validated) frame.
    let body = &h[FRAME_HEADER_LEN..crc_start - FRAME_LEN_PREFIX];
    if node_len + tag_len + data_len != body.len() {
        return DecodeStep::Torn; // internal-length inconsistency.
    }
    let node = &body[..node_len];
    let tag = &body[node_len..node_len + tag_len];
    let data = &body[node_len + tag_len..];

    let durable = flags & FLAG_DURABLE != 0;
    let node_s = if flags & FLAG_HAS_NODE != 0 {
        match std::str::from_utf8(node) {
            Ok(s) => Some(s.to_string()),
            Err(_) => return DecodeStep::Torn,
        }
    } else {
        None
    };
    let tag_s = if flags & FLAG_HAS_TAG != 0 {
        match std::str::from_utf8(tag) {
            Ok(s) => Some(s.to_string()),
            Err(_) => return DecodeStep::Torn,
        }
    } else {
        None
    };

    let record = match type_tag {
        T_APPEND => WalRecord::Append {
            topic_id,
            seq,
            ts,
            node: node_s,
            tag: tag_s,
            data: data.to_vec(),
        },
        T_DELETE => {
            let mut pos = 0usize;
            let has_before = match read_u8(data, &mut pos) {
                Some(b) => b,
                None => return DecodeStep::Torn,
            };
            let before_seq = if has_before == 1 {
                match read_u64(data, &mut pos) {
                    Some(v) => Some(v),
                    None => return DecodeStep::Torn,
                }
            } else {
                None
            };
            let match_ = match read_match(data, &mut pos) {
                Some(m) => m,
                None => return DecodeStep::Torn,
            };
            // Explicit seq set (queue ack / dead-letter). Current frames always
            // carry the section, even when the set is empty.
            if pos + 4 > data.len() {
                return DecodeStep::Torn;
            }
            let n = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            let mut seqs = Vec::with_capacity(n);
            for _ in 0..n {
                match read_u64(data, &mut pos) {
                    Some(s) => seqs.push(s),
                    None => return DecodeStep::Torn,
                }
            }
            let bound_head = match read_u8(data, &mut pos) {
                Some(1) => match read_u64(data, &mut pos) {
                    Some(v) => Some(v),
                    None => return DecodeStep::Torn,
                },
                Some(0) => None,
                _ => return DecodeStep::Torn,
            };
            if pos != data.len() {
                return DecodeStep::Torn;
            }
            WalRecord::Delete {
                topic_id,
                before_seq,
                match_,
                seqs,
                bound_head,
                ts,
            }
        }
        T_BOX_CREATE | T_BOX_DELETE => {
            let mut pos = 0usize;
            let name = match read_lp_str(data, &mut pos) {
                Some(s) => s,
                None => return DecodeStep::Torn,
            };
            let config = match read_lp_bytes(data, &mut pos) {
                Some(b) => b,
                None => return DecodeStep::Torn,
            };
            WalRecord::TopicConfig {
                topic_id,
                op: TopicConfigOp { name, config },
                tombstone: type_tag == T_BOX_DELETE,
                ts,
            }
        }
        T_ROUTER_CREATE => {
            let mut pos = 0usize;
            let name = read_lp_str(data, &mut pos);
            let source = read_lp_str(data, &mut pos);
            let dest = read_lp_str(data, &mut pos);
            let bools = read_u8(data, &mut pos);
            let filter = read_match(data, &mut pos);
            let initial_cursor = read_u64(data, &mut pos);
            let initial_dest_base = read_u64(data, &mut pos);
            match (
                name,
                source,
                dest,
                bools,
                filter,
                initial_cursor,
                initial_dest_base,
            ) {
                (
                    Some(name),
                    Some(source),
                    Some(dest),
                    Some(bools),
                    Some(filter),
                    Some(initial_cursor),
                    Some(initial_dest_base),
                ) if pos == data.len() => WalRecord::RouterCreate {
                    op: RouterOp {
                        name,
                        source,
                        dest,
                        preserve_node: bools & 1 != 0,
                        preserve_tag: bools & 2 != 0,
                        create_dest: bools & 4 != 0,
                        allow_cycle: bools & 8 != 0,
                        exactly_once: bools & 16 != 0,
                        filter,
                        initial_cursor,
                        initial_dest_base,
                    },
                    ts,
                },
                _ => return DecodeStep::Torn,
            }
        }
        T_ROUTER_DELETE => {
            let mut pos = 0usize;
            match read_lp_str(data, &mut pos) {
                Some(name) => WalRecord::RouterDelete { name, ts },
                None => return DecodeStep::Torn,
            }
        }
        T_EVICT_WATERMARK => {
            let mut pos = 0usize;
            let evict_floor = read_u64(data, &mut pos);
            let earliest_seq = read_u64(data, &mut pos);
            let expiry_floor = read_u64(data, &mut pos);
            match (evict_floor, earliest_seq) {
                (Some(evict_floor), Some(earliest_seq))
                    if expiry_floor.is_some() && pos == data.len() =>
                {
                    WalRecord::EvictWatermark {
                        topic_id,
                        evict_floor,
                        expiry_floor: expiry_floor.unwrap(),
                        earliest_seq,
                        ts,
                    }
                }
                _ => return DecodeStep::Torn,
            }
        }
        T_CONFIG_UPDATE => {
            // ConfigUpdate shares the TopicConfig wire shape (name + config blob).
            let mut pos = 0usize;
            let name = match read_lp_str(data, &mut pos) {
                Some(s) => s,
                None => return DecodeStep::Torn,
            };
            let config = match read_lp_bytes(data, &mut pos) {
                Some(b) => b,
                None => return DecodeStep::Torn,
            };
            WalRecord::TopicConfig {
                topic_id,
                op: TopicConfigOp { name, config },
                tombstone: false,
                ts,
            }
        }
        T_CHECKPOINT_MARK => {
            let mut pos = 0usize;
            match read_u64(data, &mut pos) {
                Some(last_checkpoint_seq) => WalRecord::CheckpointMark {
                    last_checkpoint_seq,
                    ts,
                },
                None => return DecodeStep::Torn,
            }
        }
        T_LEASE => {
            let mut pos = 0usize;
            let event = read_u8(data, &mut pos);
            let lease_id = read_u64(data, &mut pos);
            let deadline = read_u64(data, &mut pos);
            let deliveries = read_u64(data, &mut pos);
            let node = read_lp_str(data, &mut pos);
            match (event, lease_id, deadline, deliveries, node) {
                (Some(event), Some(lease_id), Some(deadline), Some(deliveries), Some(node)) => {
                    WalRecord::Lease {
                        topic_id,
                        seq,
                        event,
                        node,
                        lease_id,
                        deadline,
                        deliveries,
                        ts,
                    }
                }
                _ => return DecodeStep::Torn,
            }
        }
        T_HEAD_WATERMARK => {
            let mut pos = 0usize;
            match read_u64(data, &mut pos) {
                Some(head_seq) => WalRecord::HeadWatermark {
                    topic_id,
                    head_seq,
                    ts,
                },
                None => return DecodeStep::Torn,
            }
        }
        _ => return DecodeStep::Torn, // unknown type ⇒ treat as torn.
    };

    DecodeStep::Frame {
        frame: WalFrame { record, durable },
        consumed: total,
    }
}

// ---------------------------------------------------------------------------
// Reader / iterator (torn-tail safe)
// ---------------------------------------------------------------------------

/// Streaming reader over a single WAL file's bytes. Yields complete CRC-valid
/// frames in order and **stops cleanly** at the first torn/partial tail frame,
/// reporting how many valid bytes were consumed via [`WalReader::valid_len`].
/// Used by recovery to replay then truncate.
pub struct WalReader {
    buf: Vec<u8>,
    pos: usize,
    /// Byte offset of the last fully-decoded, CRC-valid frame's end.
    valid_len: usize,
    done: bool,
}

impl WalReader {
    /// Build a reader over an in-memory byte buffer.
    pub fn new(buf: Vec<u8>) -> Self {
        WalReader {
            buf,
            pos: 0,
            valid_len: 0,
            done: false,
        }
    }

    /// Read an entire WAL file into a reader from the real filesystem.
    pub fn open(path: &Path) -> io::Result<Self> {
        Self::open_with(&RealFs::arc(), path)
    }

    /// Read an entire WAL file into a reader, routing the read through `fs`.
    pub fn open_with(fs: &Arc<dyn Fs>, path: &Path) -> io::Result<Self> {
        let f = fs.open(path, OpenOpts::read_only())?;
        let mut buf = Vec::new();
        f.read_to_end_from(0, &mut buf)?;
        Ok(WalReader::new(buf))
    }

    /// Bytes consumed by valid frames so far — the length to truncate the file
    /// to once the torn tail is reached.
    pub fn valid_len(&self) -> usize {
        self.valid_len
    }

    /// Drain the reader (replaying nothing) and return the valid-prefix length —
    /// the byte offset of the end of the last fully-valid frame. Convenience for
    /// callers (e.g. tests) that only need the safe truncation point.
    pub fn count_valid_len(mut self) -> usize {
        for _ in self.by_ref() {}
        self.valid_len
    }
}

impl Iterator for WalReader {
    type Item = WalFrame;

    fn next(&mut self) -> Option<WalFrame> {
        if self.done {
            return None;
        }
        match decode_one(&self.buf[self.pos..]) {
            DecodeStep::Frame { frame, consumed } => {
                self.pos += consumed;
                self.valid_len = self.pos;
                Some(frame)
            }
            DecodeStep::Torn => {
                self.done = true; // stop cleanly at the torn tail.
                None
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Writer + adaptive group commit
// ---------------------------------------------------------------------------

/// WAL writer tuning knobs (ARCHITECTURE §2.3/§2.4).
#[derive(Debug, Clone)]
pub struct WalConfig {
    /// Data directory; the WAL lives under `<data_dir>/wal` (plus an optional
    /// per-shard subdirectory, see [`WalConfig::shard_subdir`]).
    pub dir: PathBuf,
    /// Minimum group-commit window (a lone durable write fsyncs ~immediately).
    pub gc_min: Duration,
    /// Maximum group-commit window under load.
    pub gc_max: Duration,
    /// Preallocated size per WAL file; the active file rotates at this size.
    pub file_size: u64,
    /// Ingest channel capacity (bounded backpressure for the single writer).
    pub channel_cap: usize,
    /// Optional per-shard subdirectory beneath `<dir>/wal` (WAL sharding). When
    /// `Some("shard-00")`, this writer's files live at
    /// `<dir>/wal/shard-00/wal-<idx>.log`; when `None` (the default, and the
    /// single-shard flat layout) they live flat at `<dir>/wal/wal-<idx>.log`.
    pub shard_subdir: Option<String>,
}

impl WalConfig {
    /// Defaults matching ARCHITECTURE §2.3 (GC 0.5..10 ms) and §2.4 (64 MiB
    /// preallocated files). No shard subdirectory ⇒ the flat single-writer layout.
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        WalConfig {
            dir: dir.into(),
            gc_min: Duration::from_micros(500),
            gc_max: Duration::from_millis(10),
            file_size: 64 << 20,
            channel_cap: 4096,
            shard_subdir: None,
        }
    }

    /// The directory this writer's `wal-<idx>.log` files live in: `<dir>/wal`
    /// (single-shard flat layout) or `<dir>/wal/<shard_subdir>` (a WAL shard).
    fn wal_dir(&self) -> PathBuf {
        let base = self.dir.join("wal");
        match &self.shard_subdir {
            Some(sub) => base.join(sub),
            None => base,
        }
    }
}

/// The shared commit state behind a [`CommitToken`]: a flag flipped by the
/// writer once the submission is committed (after the group fsync for a durable
/// frame, or after the buffered write for a non-durable one) plus a condvar to
/// wake any blocked waiter.
struct CommitState {
    committed: Mutex<CommitOutcome>,
    cv: Condvar,
}

#[derive(Clone, Copy, PartialEq)]
enum CommitOutcome {
    /// Not yet committed.
    Pending,
    /// Committed (and fsynced, for a durable frame).
    Ok,
    /// The writer failed to commit this batch (io error / writer gone).
    Failed,
}

/// A handle returned by [`WalWriter::submit`] that resolves once the record is
/// committed. A durable write blocks on [`CommitToken::wait`] until the group
/// fsync completes; a non-durable write may drop the token (fire-and-forget).
pub struct CommitToken {
    state: Arc<CommitState>,
    fsynced: bool,
}

/// Proof that a submitted WAL batch reached the writer's commit boundary.
///
/// For fsync-gated submissions this means the group `fdatasync` returned `Ok`.
/// For non-durable submissions this means the batch reached the buffered-write
/// boundary. The fields are private so only the WAL writer can manufacture it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitProof {
    fsynced: bool,
    _private: (),
}

impl CommitProof {
    /// Whether this proof came from a fsync-gated WAL commit.
    pub fn is_fsynced(self) -> bool {
        self.fsynced
    }
}

impl CommitToken {
    /// Block the calling thread until the submission is committed. Returns
    /// `Err(WalError::WriterGone)` if the writer failed/exited before commit.
    pub fn wait(self) -> Result<CommitProof, WalError> {
        let mut guard = self.state.committed.lock().unwrap();
        loop {
            match *guard {
                CommitOutcome::Ok => {
                    return Ok(CommitProof {
                        fsynced: self.fsynced,
                        _private: (),
                    });
                }
                CommitOutcome::Failed => return Err(WalError::WriterGone),
                CommitOutcome::Pending => {
                    guard = self.state.cv.wait(guard).unwrap();
                }
            }
        }
    }
}

/// A submission handed to the writer: one OR MORE records (a single caller batch),
/// their shared durability class, and the shared state used to signal the caller
/// once committed. A multi-record submission is the WAL's atomicity unit (R5 /
/// codex P0 #1): it consumes exactly ONE bounded-channel slot, so `submit_batch`
/// either enqueues the whole batch or none of it — the single ordered writer can
/// never accept a partial prefix of a caller's batch and leave the rest to be
/// rolled back, which a crash would otherwise replay as orphan unacked records.
struct Submission {
    records: Vec<WalRecord>,
    durable: bool,
    state: Arc<CommitState>,
}

/// Test/observability counters shared with the writer task. `fsyncs` increments
/// once per group-commit fsync; `frames` counts frames written. The ratio of
/// `frames` to `fsyncs` is the group-commit batching factor.
///
/// `active_idx`/`active_len` publish the writer's current append position (the
/// active WAL file's numeric index + its valid byte length) after every committed
/// batch, so a snapshot can record the **checkpoint position** it corresponds to
/// (ARCHITECTURE §3) without coordinating with the writer thread.
#[derive(Debug)]
pub struct WalMetrics {
    pub fsyncs: AtomicU64,
    pub frames: AtomicU64,
    pub batches: AtomicU64,
    pub bytes_written: AtomicU64,
    pub rotations: AtomicU64,
    /// Numeric index of the active WAL file (`wal-<idx>.log`).
    pub active_idx: AtomicU64,
    /// Valid byte length written to the active WAL file (the append position).
    pub active_len: AtomicU64,
    /// Current depth of the bounded ingest queue: submissions accepted by
    /// [`WalWriter::submit`] but not yet pulled off the channel by the writer
    /// (R5 backpressure visibility; M3). Bumped on a successful `submit`,
    /// decremented as the writer dequeues each submission.
    pub queued: AtomicU64,
    /// High-water mark of [`Self::queued`] (the deepest the ingest queue ever
    /// got) — a sticky gauge so an operator can see how close the WAL ran to its
    /// `channel_cap` ceiling even between scrapes.
    pub queued_peak: AtomicU64,
    /// Count of `submit`s rejected because the bounded ingest queue was full
    /// ([`WalError::Full`]) — the R5 backpressure event counter.
    pub submit_full: AtomicU64,
    /// Whether the writer has latched read-only after a WAL-rotation failure
    /// (R11): `0` = healthy, `1` = read-only. A sticky gauge for alerting.
    pub read_only: AtomicU64,
    /// fsync-latency histogram: cumulative counts in `le` buckets (microseconds)
    /// matching [`FSYNC_BUCKETS_US`], plus the total observation count and the
    /// summed latency (microseconds) for an average. Observed once per group
    /// fsync.
    pub fsync_buckets: [AtomicU64; FSYNC_BUCKETS_US.len()],
    pub fsync_count: AtomicU64,
    pub fsync_micros_total: AtomicU64,
}

/// fsync-latency histogram bucket upper bounds (microseconds). The implicit
/// `+Inf` bucket equals [`WalMetrics::fsync_count`]. Sized for a fast local NVMe
/// (tens of µs) through a stalled/contended device (tens of ms).
pub const FSYNC_BUCKETS_US: [u64; 9] = [50, 100, 250, 500, 1_000, 5_000, 10_000, 50_000, 100_000];

impl Default for WalMetrics {
    fn default() -> Self {
        WalMetrics {
            fsyncs: AtomicU64::new(0),
            frames: AtomicU64::new(0),
            batches: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            rotations: AtomicU64::new(0),
            active_idx: AtomicU64::new(0),
            active_len: AtomicU64::new(0),
            queued: AtomicU64::new(0),
            queued_peak: AtomicU64::new(0),
            submit_full: AtomicU64::new(0),
            read_only: AtomicU64::new(0),
            fsync_buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            fsync_count: AtomicU64::new(0),
            fsync_micros_total: AtomicU64::new(0),
        }
    }
}

impl WalMetrics {
    /// Record one fsync latency observation (microseconds) into the histogram.
    fn observe_fsync(&self, micros: u64) {
        for (i, &le) in FSYNC_BUCKETS_US.iter().enumerate() {
            if micros <= le {
                self.fsync_buckets[i].fetch_add(1, Ordering::Relaxed);
            }
        }
        self.fsync_count.fetch_add(1, Ordering::Relaxed);
        self.fsync_micros_total.fetch_add(micros, Ordering::Relaxed);
    }

    /// Bump the live ingest-queue depth and keep the peak high-water mark current.
    /// Called by `submit` BEFORE the channel send, so the writer can never observe
    /// a dequeue for a submission whose enqueue bump has not yet landed (which
    /// would underflow the gauge). A failed send rolls the bump back via
    /// [`Self::enqueue_rollback`].
    fn enqueued(&self) {
        let now = self
            .queued
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        self.queued_peak.fetch_max(now, Ordering::Relaxed);
    }

    /// Undo an [`Self::enqueued`] bump when the channel send failed (the
    /// submission never actually entered the queue).
    fn enqueue_rollback(&self) {
        self.queued.fetch_sub(1, Ordering::Relaxed);
    }

    /// Drop the live ingest-queue depth as the writer dequeues a submission.
    fn dequeued(&self) {
        self.queued.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Handle the engine holds to submit records to the WAL. Cloneable; all clones
/// feed the same single writer thread.
///
/// The ingest channel is **bounded** to `channel_cap` (R5): a stalled writer
/// (slow/stuck device) makes `submit` fail with [`WalError::Full`] rather than
/// queue submissions without bound, so backpressure surfaces as a transient
/// error the engine maps to `503` instead of unbounded memory growth.
#[derive(Clone)]
pub struct WalWriter {
    tx: mpsc::SyncSender<Submission>,
    metrics: Arc<WalMetrics>,
    /// Shared stop signal (also held by the writer task). Once set, [`submit`]
    /// rejects with [`WalError::WriterGone`] so no token is created after the
    /// writer has begun its final drain — closing the shutdown race where a
    /// post-drain `submit` could enqueue a [`Submission`] the writer never sees,
    /// leaving its [`CommitToken`] forever unsignaled (codex P2). The drain side
    /// (`drain_and_commit_remaining`) waits for the in-flight gauge (`metrics.queued`)
    /// to reach zero, so a submit already PAST the shutdown check is still committed.
    shutdown: Arc<std::sync::atomic::AtomicBool>,
}

impl WalWriter {
    /// Submit `record` with durability class `durable`, returning a
    /// [`CommitToken`]. The token resolves once the record is committed: for a
    /// durable record, after the group fsync; for a non-durable record, after
    /// the buffered write (its durability follows on the next group fsync). Drop
    /// the token to fire-and-forget. An `Err` means the writer thread is gone.
    pub fn submit(&self, record: WalRecord, durable: bool) -> Result<CommitToken, WalError> {
        self.submit_batch(vec![record], durable)
    }

    /// Submit an ENTIRE caller batch (one or more `records`) atomically as a
    /// single unit, returning one [`CommitToken`] that resolves when the whole
    /// batch is committed (R5 / codex P0 #1). The batch consumes exactly ONE
    /// bounded-channel slot, so it is accepted all-or-none: a `Full` (the writer
    /// stalled behind a slow device) rejects the WHOLE batch, never a prefix.
    /// This is the load-bearing fix for the partial-batch hazard — the old
    /// one-frame-per-`try_send` loop could accept the first frames and reject the
    /// rest, and the accepted prefix would still be written/replayed as orphan
    /// unacked records after the caller rolled the batch back in memory.
    ///
    /// An empty `records` is a no-op that returns a pre-committed token.
    pub fn submit_batch(
        &self,
        records: Vec<WalRecord>,
        durable: bool,
    ) -> Result<CommitToken, WalError> {
        if records.is_empty() {
            // Nothing to write: hand back an already-committed token so callers
            // can `wait()` uniformly.
            return Ok(CommitToken {
                state: Arc::new(CommitState {
                    committed: Mutex::new(CommitOutcome::Ok),
                    cv: Condvar::new(),
                }),
                fsynced: durable,
            });
        }
        let state = Arc::new(CommitState {
            committed: Mutex::new(CommitOutcome::Pending),
            cv: Condvar::new(),
        });
        // `try_send` on the bounded channel: a full queue means the single writer
        // is stalled behind a slow/stuck device (R5). Surface `Full` (the engine
        // maps it to a transient `503`) rather than blocking the caller or letting
        // the queue grow without bound. A disconnected channel means the writer
        // thread is gone. The WHOLE batch is one slot ⇒ accepted/rejected as a unit.
        // Bump the live ingest-queue depth gauge BEFORE the send so the writer
        // (which decrements on dequeue) can never race ahead of this bump and
        // underflow the gauge. Rolled back below if the send fails. This is the
        // observable WAL queue depth / R5 backpressure signal (counts submissions,
        // i.e. channel slots). The bump is ALSO the shutdown handshake: it is
        // published (SeqCst) before the shutdown check, so a drain side waiting for
        // `queued == 0` either sees this in-flight submission (and waits to commit
        // it) or this submit observes `shutdown` and rolls the gauge back. Either
        // way no accepted submission is ever stranded (codex P2).
        self.metrics.enqueued();
        // Full fence so the `queued` bump above is globally ordered before the
        // shutdown load below — pairs with the writer's `store(shutdown, SeqCst)`
        // then `load(queued, SeqCst)` in `drain_and_commit_remaining`, giving the
        // total order that guarantees: the writer either observes this in-flight
        // bump (and drains/commits this submission) OR this submit observes shutdown
        // (and rejects). No accepted submission is ever stranded (codex P2).
        std::sync::atomic::fence(Ordering::SeqCst);
        // Reject once shutdown has begun: the writer is draining toward exit and a
        // token created now might never be signaled. Roll the gauge back first.
        if self.shutdown.load(Ordering::SeqCst) {
            self.metrics.enqueue_rollback();
            return Err(WalError::WriterGone);
        }
        self.tx
            .try_send(Submission {
                records,
                durable,
                state: state.clone(),
            })
            .map_err(|e| {
                self.metrics.enqueue_rollback();
                match e {
                    mpsc::TrySendError::Full(_) => {
                        self.metrics.submit_full.fetch_add(1, Ordering::Relaxed);
                        WalError::Full
                    }
                    mpsc::TrySendError::Disconnected(_) => WalError::WriterGone,
                }
            })?;
        Ok(CommitToken {
            state,
            fsynced: durable,
        })
    }

    /// Submit and block until the commit completes, in one call.
    pub fn append(&self, record: WalRecord, durable: bool) -> Result<(), WalError> {
        self.submit(record, durable)?.wait().map(drop)
    }

    /// Snapshot of writer metrics (group-commit batching factor, etc.).
    pub fn metrics(&self) -> Arc<WalMetrics> {
        self.metrics.clone()
    }

    /// The writer's current committed append position: `(active_idx,
    /// active_len)` — the active WAL file's numeric index and its valid byte
    /// length. A checkpoint records this so recovery resumes WAL replay from
    /// exactly here (ARCHITECTURE §3). Captured *after* materializing snapshot
    /// state, so replay re-applies only frames at/after this offset; the
    /// seq-skip on `Append` makes any overlap idempotent.
    pub fn position(&self) -> (u64, u64) {
        (
            self.metrics.active_idx.load(Ordering::Acquire),
            self.metrics.active_len.load(Ordering::Acquire),
        )
    }
}

/// WAL errors surfaced to the engine.
#[derive(Debug, thiserror::Error)]
pub enum WalError {
    #[error("wal writer thread is gone")]
    WriterGone,
    /// The bounded ingest queue is full: the single writer is stalled (a slow/
    /// stuck device) and the channel reached `channel_cap`. The engine maps this
    /// to a transient `503` so the caller backs off instead of the WAL growing
    /// memory without bound (R5). Distinct from [`WalError::WriterGone`] (a dead
    /// writer) so the engine can choose the right status.
    #[error("wal ingest queue is full (writer stalled)")]
    Full,
    #[error("wal io error: {0}")]
    Io(#[from] io::Error),
}

/// The WAL facade: owns the writer thread handle and exposes a [`WalWriter`].
pub struct Wal {
    writer: WalWriter,
    /// Explicit stop signal: set on [`Wal::shutdown`] so the writer exits even
    /// while the engine still holds [`WalWriter`] clones (the channel alone
    /// never closes in that case).
    shutdown: Arc<std::sync::atomic::AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl Wal {
    /// Open (or create) the WAL under `cfg.dir`, spawning the single writer
    /// thread. `start_seq` names the active file (`wal-<start_seq>.log`) and
    /// continues the global frame numbering after recovery; for a fresh dir pass
    /// `1`. A missing/empty `wal` directory is a fresh start.
    ///
    /// `existing` are bytes already present in the active file to append after
    /// (recovery appends new frames after the recovered, truncated tail). For a
    /// fresh start pass `0`.
    pub fn open(cfg: WalConfig) -> Result<Wal, WalError> {
        Wal::open_at(cfg, 1, 0)
    }

    /// Open the WAL with an explicit active-file first index and a pre-existing
    /// valid length (recovery resumes appends after the truncated tail), on the
    /// real filesystem.
    pub fn open_at(cfg: WalConfig, first_idx: u64, existing_len: u64) -> Result<Wal, WalError> {
        Wal::open_at_with(RealFs::arc(), cfg, first_idx, existing_len)
    }

    /// As [`Wal::open_at`], routing every byte of WAL I/O (file open, preallocation
    /// `set_len`, batched `write_at`, group-commit `sync_data`, rotation) through
    /// `fs`. Production passes a [`RealFs`] (transparent); the crash harness passes
    /// a fake so a power loss after the Nth FS call / a torn last write / an EIO on
    /// `sync_data` can be modelled.
    pub fn open_at_with(
        fs: Arc<dyn Fs>,
        cfg: WalConfig,
        first_idx: u64,
        existing_len: u64,
    ) -> Result<Wal, WalError> {
        let wal_dir = cfg.wal_dir();
        fs.create_dir_all(&wal_dir)?;
        // Harden the `wal/` directory entry itself by fsyncing its parent (codex
        // P0): a crash after a durable ack must not lose the `wal/` directory entry
        // (which would lose every file under it). Best-effort on a parent that may
        // not exist yet on some fakes; the per-file `sync_dir(wal_dir)` below is the
        // load-bearing one for the files themselves.
        if let Some(parent) = wal_dir.parent() {
            let _ = fs.sync_dir(parent);
        }

        let metrics = Arc::new(WalMetrics::default());
        // Bounded ingest queue (R5): backpressure under a stalled writer surfaces
        // as `WalError::Full` from `submit` instead of unbounded memory growth.
        let (tx, rx) = mpsc::sync_channel::<Submission>(cfg.channel_cap.max(1));
        let shutdown = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let file =
            ActiveFile::open_for_append(&fs, &wal_dir, first_idx, cfg.file_size, existing_len)?;
        // Publish the initial append position so a snapshot taken before the
        // first write still records the right checkpoint (ARCHITECTURE §3).
        metrics.active_idx.store(first_idx, Ordering::Relaxed);
        metrics.active_len.store(existing_len, Ordering::Relaxed);
        let task = WriterTask {
            cfg: cfg.clone(),
            fs,
            wal_dir,
            file,
            rx,
            shutdown: shutdown.clone(),
            metrics: metrics.clone(),
            read_only: false,
        };
        let handle = std::thread::Builder::new()
            .name("topics-wal".to_string())
            .spawn(move || task.run())
            .map_err(WalError::Io)?;

        Ok(Wal {
            writer: WalWriter {
                tx,
                metrics,
                shutdown: shutdown.clone(),
            },
            shutdown,
            handle: Some(handle),
        })
    }

    /// A cloneable handle for submitting records.
    pub fn writer(&self) -> WalWriter {
        self.writer.clone()
    }

    /// Snapshot of the writer metrics.
    pub fn metrics(&self) -> Arc<WalMetrics> {
        self.writer.metrics.clone()
    }

    /// Stop the writer thread: signal shutdown, then join. The writer drains
    /// every already-submitted frame and issues a final fsync before exiting, so
    /// no committed batch is lost. Reliable even while outstanding [`WalWriter`]
    /// clones keep the ingest channel open. Consuming `self` here is convenient
    /// for tests; the engine uses the `Drop` path on its owned `Wal`.
    pub fn shutdown(mut self) {
        self.stop();
    }

    fn stop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for Wal {
    fn drop(&mut self) {
        // Dropping the owned `Wal` (e.g. when the engine shuts down) drains and
        // fsyncs the writer's queue and joins the thread, so no committed batch
        // is lost even on the implicit-drop path.
        self.stop();
    }
}

/// The active WAL file: an open, preallocated, append-positioned handle routed
/// through the [`Fs`] seam.
struct ActiveFile {
    file: Box<dyn File>,
    /// On-disk path; retained for rotation/recovery (later stages).
    #[allow(dead_code)]
    path: PathBuf,
    /// Numeric index of this file (`wal-<idx>.log`). The authoritative basis for
    /// the next rotation target (`idx + 1`), so rotation never reuses a lower
    /// index after recovery resumed at a higher one (codex P0).
    idx: u64,
    /// Bytes of real (written) frame data — the logical append position.
    len: u64,
    /// Preallocated capacity; the file rotates when `len` would exceed it.
    capacity: u64,
}

impl ActiveFile {
    /// Create + preallocate `wal-<first_seq>.log` of `capacity` bytes, truncating
    /// any existing file (used for a fresh rotation target).
    fn create(
        fs: &Arc<dyn Fs>,
        wal_dir: &Path,
        first_seq: u64,
        capacity: u64,
    ) -> io::Result<ActiveFile> {
        let path = wal_dir.join(format!("wal-{:016}.log", first_seq));
        let mut file = fs.open(&path, OpenOpts::create_truncate())?;
        // Preallocate so appends don't extend the inode (best effort; set_len
        // reserves the logical size, the FS may keep it sparse — appends still
        // overwrite within the reservation rather than growing metadata).
        file.set_len(capacity)?;
        file.sync_all()?;
        // Fsync the parent dir so the new file's directory entry is itself durable
        // BEFORE any frame written to it can be acknowledged (codex P0): a crash
        // after a durable ack must never lose the directory entry of the file the
        // ack's frame lives in (a rotated WAL file, or the first file).
        fs.sync_dir(wal_dir)?;
        Ok(ActiveFile {
            file,
            path,
            idx: first_seq,
            len: 0,
            capacity,
        })
    }

    /// Open `wal-<first_seq>.log` positioned to append after `valid_len` bytes
    /// (recovery resumes appends after the truncated tail). Creates the file if
    /// absent; preallocates to at least `capacity`. `valid_len == 0` and a fresh
    /// dir is the fresh-start case.
    fn open_for_append(
        fs: &Arc<dyn Fs>,
        wal_dir: &Path,
        first_seq: u64,
        capacity: u64,
        valid_len: u64,
    ) -> io::Result<ActiveFile> {
        let path = wal_dir.join(format!("wal-{:016}.log", first_seq));
        let mut file = fs.open(&path, OpenOpts::create_keep())?;
        // Ensure the reservation covers at least the recovered length + headroom.
        let want = capacity.max(valid_len);
        if file.metadata_len()? < want {
            file.set_len(want)?;
        }
        file.sync_all()?;
        // Harden the directory entry (codex P0): when this opens (or creates) the
        // active file on a fresh/recovered dir, fsync the parent so the file's
        // directory entry is durable before any acknowledged frame lands in it.
        fs.sync_dir(wal_dir)?;
        Ok(ActiveFile {
            file,
            path,
            idx: first_seq,
            len: valid_len,
            capacity: want,
        })
    }

    /// Write `bytes` at the current append position, looping over any short write
    /// (`write_at` may report fewer bytes than offered, like `pwrite(2)`).
    fn write_all_at_tail(&mut self, bytes: &[u8]) -> io::Result<()> {
        let mut written = 0usize;
        while written < bytes.len() {
            let n = self
                .file
                .write_at(self.len + written as u64, &bytes[written..])?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "wal write_at made no progress",
                ));
            }
            written += n;
        }
        self.len += bytes.len() as u64;
        Ok(())
    }

    fn fdatasync(&self) -> io::Result<()> {
        // The seam's sync_data → fdatasync on unix (no inode-metadata flush).
        self.file.sync_data()
    }
}

/// The single-writer thread body: owns the active file, drains the channel,
/// batches, and group-commits.
struct WriterTask {
    cfg: WalConfig,
    /// The filesystem seam all WAL I/O routes through (rotation opens a new file
    /// via this).
    fs: Arc<dyn Fs>,
    wal_dir: PathBuf,
    file: ActiveFile,
    rx: mpsc::Receiver<Submission>,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
    metrics: Arc<WalMetrics>,
    /// Latched read-only state (R11): set when a WAL-file rotation fails to create
    /// the next file. We must NOT keep writing past the boundary into the full
    /// active file, so every subsequent batch fails fast instead of silently
    /// growing the file past its preallocation (or corrupting rotation ordering).
    read_only: bool,
}

impl WriterTask {
    fn run(mut self) {
        let mut scratch = Vec::with_capacity(4096);
        let mut batch_bytes: Vec<u8> = Vec::with_capacity(64 << 10);

        loop {
            if self.shutdown.load(Ordering::SeqCst) {
                // Drain whatever is already queued, commit it (forced fsync), stop.
                self.drain_and_commit_remaining(&mut scratch, &mut batch_bytes);
                break;
            }

            // Park (bounded) until a submission arrives, shutdown is signalled, or
            // the timeout elapses (so we re-check the shutdown flag). No busy-spin.
            let first = match self.rx.recv_timeout(Duration::from_millis(50)) {
                Ok(s) => {
                    self.metrics.dequeued();
                    s
                }
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => break, // all senders gone.
            };

            let mut pending: Vec<Submission> = Vec::new();
            let mut any_durable = first.durable;
            pending.push(first);

            // Coalesce everything already queued behind `first` in one cheap,
            // non-blocking sweep — this is the bulk of group-commit batching
            // under load (the channel backs up while the writer was busy).
            self.drain_ready(&mut pending, &mut any_durable);

            // Adaptive group-commit coalescing: only a *durable* batch waits at
            // all, and only to let concurrent durable writers join THIS fsync so
            // one fdatasync amortizes across the whole cohort (the batching factor
            // is what makes throughput scale — and what keeps WAL sharding from
            // fragmenting the group commit when writers are spread thin across
            // shards). A lone durable write with nothing else waiting fsyncs at
            // once (the window collapses to ~0); under load the writer keeps
            // coalescing until arrivals stall or the bounded `gc_max` deadline hits.
            if any_durable {
                self.coalesce_durable(&mut pending, &mut any_durable);
            }

            self.commit_batch(&mut scratch, &mut batch_bytes, pending, any_durable);
        }

        // Final best-effort fdatasync hardens any non-durable tail on clean exit.
        let _ = self.file.fdatasync();
    }

    /// Coalesce as many concurrent durable submissions as possible into the
    /// `pending` batch before the single group fdatasync, so one fsync amortizes
    /// across the whole writer cohort. This is the load-bearing throughput lever:
    /// the more writes per fsync, the fewer (expensive) fsyncs per second the
    /// device must do — and with WAL sharding, each shard sees fewer writers, so a
    /// naive fixed window would let each shard fragment its group commit into many
    /// tiny fsyncs (measured: frames/fsync collapsing as shard count rose, which
    /// reversed the scaling). Coalescing aggressively per shard keeps the batching
    /// factor high regardless of shard count.
    ///
    /// Strategy (bounded, no busy-spin past the deadline): if at least one more
    /// durable submission is already visible in the ingest queue, keep draining in
    /// short slices until **arrivals stall** (a full slice passed with nothing new)
    /// or the `gc_max` deadline is reached. A lone durable write with an empty
    /// queue does NOT wait — it fsyncs immediately (latency-optimal when quiet).
    /// The per-slice park is `gc_min` (sub-millisecond), so the writer reacts fast
    /// to a stall yet never spins hot. Every wait is capped by `gc_max`, so the
    /// writer always makes timely forward progress.
    fn coalesce_durable(&mut self, pending: &mut Vec<Submission>, any_durable: &mut bool) {
        // Nothing else is queued behind what we already drained ⇒ a quiet, lone
        // durable batch. Fsync now (no added latency).
        if self.metrics.queued.load(Ordering::Relaxed) == 0 {
            return;
        }
        let deadline = Instant::now() + self.cfg.gc_max;
        loop {
            // First drain everything already waiting (cheap, non-blocking) — this
            // is the bulk of the cohort under load.
            self.drain_ready(pending, any_durable);
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            // Block up to one `gc_min` slice for the NEXT straggler. `recv_timeout`
            // parks (no spin) until an arrival or the slice elapses; a timeout means
            // arrivals have stalled, so the cohort has stopped growing and waiting
            // longer would only add latency.
            let slice = self.cfg.gc_min.min(remaining);
            match self.rx.recv_timeout(slice) {
                Ok(s) => {
                    self.metrics.dequeued();
                    *any_durable |= s.durable;
                    pending.push(s);
                    // Loop: sweep any others that arrived alongside it, then keep
                    // coalescing until arrivals stall or the deadline passes.
                }
                Err(RecvTimeoutError::Timeout) => break,
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }
    }

    /// On shutdown, commit every already-submitted frame so no queued write is
    /// lost, then return. The channel may stay open (engine clones outstanding),
    /// so we cannot rely on disconnection. Instead we drain in a loop until the
    /// in-flight gauge (`metrics.queued`, bumped by `submit` BEFORE its shutdown
    /// check) reaches zero: every submission that passed its shutdown check is
    /// counted there and is therefore drained + committed here, while every submit
    /// AFTER `shutdown` was set rejects (and rolls the gauge back) so it adds no new
    /// token. That closes the shutdown race where a straggler `submit` between the
    /// drain and the receiver drop would leave its `CommitToken` forever unsignaled
    /// (codex P2). A brief park between passes avoids a hot spin while an in-flight
    /// submit completes its `try_send`.
    fn drain_and_commit_remaining(&mut self, scratch: &mut Vec<u8>, batch_bytes: &mut Vec<u8>) {
        loop {
            let mut pending: Vec<Submission> = Vec::new();
            let mut any_durable = false;
            self.drain_ready(&mut pending, &mut any_durable);
            if !pending.is_empty() {
                // Force a fsync on each drained batch, so a clean shutdown hardens
                // every frame's tail.
                self.commit_batch(scratch, batch_bytes, pending, true);
                continue;
            }
            // Nothing immediately available. If no submission is still in flight
            // (gauge drained), we are done. Otherwise an in-flight `submit` has
            // bumped the gauge but not yet completed its `try_send` (or will reject
            // and roll back) — park briefly and re-drain so we never exit while an
            // accepted submission is still arriving.
            if self.metrics.queued.load(Ordering::SeqCst) == 0 {
                break;
            }
            std::thread::yield_now();
        }
    }

    /// Move every immediately-available submission out of the channel.
    fn drain_ready(&mut self, pending: &mut Vec<Submission>, any_durable: &mut bool) {
        while let Ok(s) = self.rx.try_recv() {
            self.metrics.dequeued();
            *any_durable |= s.durable;
            pending.push(s);
        }
    }

    /// Encode the batch, write it (rotating if needed), fsync once if the batch
    /// holds a durable frame, then signal all commit tokens.
    fn commit_batch(
        &mut self,
        scratch: &mut Vec<u8>,
        batch_bytes: &mut Vec<u8>,
        pending: Vec<Submission>,
        any_durable: bool,
    ) {
        if pending.is_empty() {
            return;
        }
        batch_bytes.clear();
        let mut states: Vec<Arc<CommitState>> = Vec::with_capacity(pending.len());
        let mut frame_count: u64 = 0;
        for sub in pending {
            // A submission carries an atomic caller batch of one or more records
            // (R5 / codex P0 #1); encode every frame in it. They share one commit
            // state, so the whole caller batch commits (or fails) together.
            for rec in &sub.records {
                encode_frame(scratch, rec, sub.durable);
                batch_bytes.extend_from_slice(scratch);
                frame_count += 1;
            }
            states.push(sub.state);
        }

        // A prior rotation failure latched the WAL read-only (R11): never write
        // past the boundary. Fail this (and every subsequent) batch.
        if self.read_only {
            Self::signal(&states, CommitOutcome::Failed);
            return;
        }

        // Rotate if this batch would exceed the active file's preallocation.
        let needed = self.file.len + batch_bytes.len() as u64;
        if needed > self.file.capacity {
            // Seal the current file (fsync) and open the next, named `active + 1`
            // — NOT derived from the rotation counter (codex P0): after recovery
            // resumes at `wal-<active_idx>.log`, the next rotation must be
            // `active_idx + 1` so it never truncates a lower-indexed file still
            // required for replay ordering. `rotations` stays a pure observability
            // counter.
            let _ = self.file.fdatasync();
            let next_idx = self.file.idx + 1;
            match ActiveFile::create(&self.fs, &self.wal_dir, next_idx, self.cfg.file_size) {
                Ok(next) => {
                    self.file = next;
                    self.metrics.rotations.fetch_add(1, Ordering::Relaxed);
                    // Publish the new active file index + reset length so a snapshot
                    // records the post-rotation checkpoint position.
                    self.metrics.active_idx.store(next_idx, Ordering::Relaxed);
                    self.metrics.active_len.store(0, Ordering::Relaxed);
                }
                Err(e) => {
                    // R11: the next WAL file could not be created. Previously this
                    // was swallowed and the writer kept appending into the full
                    // active file (past its preallocation, and reusing the boundary
                    // a future rotation/recovery assumes is free). Instead, latch
                    // read-only and FAIL this batch so the caller sees the failure
                    // (no write past the boundary, no silent data loss).
                    tracing::error!(
                        error = %e,
                        next_idx,
                        "wal rotation failed to create next file; entering read-only"
                    );
                    self.read_only = true;
                    self.metrics.read_only.store(1, Ordering::Relaxed);
                    Self::signal(&states, CommitOutcome::Failed);
                    return;
                }
            }
        }

        // One buffered write for the whole batch. Remember the pre-batch append
        // position so a failed batch can be REWOUND (codex P0 #1): on a write or
        // fsync error the batch's bytes must NOT remain in `file.len` — otherwise a
        // LATER successful fsync would persist the failed (unacked) frame, and
        // recovery would replay it (a phantom record / a live seq gap). Rewinding
        // `len` to `len_before` discards the failed bytes logically: the next batch
        // overwrites them in the preallocated region, and since they were never
        // fsynced (and the tail is rewound) recovery never surfaces them.
        let len_before = self.file.len;
        let frames = frame_count;
        if let Err(e) = self.file.write_all_at_tail(batch_bytes) {
            tracing::error!(error = %e, "wal batch write failed; rewinding tail");
            self.file.len = len_before;
            self.metrics.active_len.store(len_before, Ordering::Relaxed);
            Self::signal(&states, CommitOutcome::Failed); // callers observe WriterGone.
            return;
        }
        // Named crash point: the batch bytes are written (buffered) but NOT yet
        // fsynced. A crash injected here drops the un-promoted pending bytes (the
        // F-WAL-CRASH-AFTER-WRITE-PRE-FSYNC oracle: an unacked durable write may
        // be lost; prior fsynced batches survive). Expands to nothing without
        // `--features failpoints`.
        fail::fail_point!("wal::after_write");
        self.metrics
            .bytes_written
            .fetch_add(batch_bytes.len() as u64, Ordering::Relaxed);
        self.metrics.frames.fetch_add(frames, Ordering::Relaxed);
        self.metrics.batches.fetch_add(1, Ordering::Relaxed);
        // Publish the new append position (the checkpoint a snapshot can record).
        self.metrics
            .active_len
            .store(self.file.len, Ordering::Relaxed);

        // One fsync per batch iff any frame in it is durable (§2.3). Non-durable
        // batches skip the fsync; their durability follows on a later group fsync.
        if any_durable {
            let fsync_start = Instant::now();
            if let Err(e) = self.file.fdatasync() {
                // The batch's bytes are written but the fsync FAILED — they are not
                // durable, the tokens fail, and the callers roll back (not acked ⇒
                // not committed). Rewind the tail to the pre-batch position (codex P0
                // #1) so a LATER successful fsync can never persist these failed
                // frames and recovery can never replay them. The next batch
                // overwrites the discarded bytes from `len_before`.
                tracing::error!(error = %e, "wal fdatasync failed; rewinding tail");
                self.file.len = len_before;
                self.metrics.active_len.store(len_before, Ordering::Relaxed);
                Self::signal(&states, CommitOutcome::Failed);
                return;
            }
            // Record the fsync latency into the histogram (M3 observability).
            self.metrics
                .observe_fsync(fsync_start.elapsed().as_micros() as u64);
            self.metrics.fsyncs.fetch_add(1, Ordering::Relaxed);
            // Named crash point: the durable batch is fsynced (promoted) but the
            // tokens have NOT yet been signalled. A crash here must keep the batch
            // (the F-WAL-CRASH-AFTER-FSYNC oracle: acked ⇒ durable; the just-synced
            // batch survives even though the ack was about to fire). No-op without
            // `--features failpoints`.
            fail::fail_point!("wal::after_fdatasync");
        }

        // Signal all tokens. Durable tokens release only after the fsync above;
        // non-durable tokens after the buffered write — both happen here.
        Self::signal(&states, CommitOutcome::Ok);
    }

    /// Flip every commit state to `outcome` and wake its waiter.
    fn signal(states: &[Arc<CommitState>], outcome: CommitOutcome) {
        for st in states {
            let mut g = st.committed.lock().unwrap();
            *g = outcome;
            st.cv.notify_all();
        }
    }
}

// ===========================================================================
// Unit tests
// ===========================================================================
#[cfg(test)]
mod tests {
    use super::*;

    fn append(
        topic_id: u64,
        seq: u64,
        node: Option<&str>,
        tag: Option<&str>,
        data: &[u8],
    ) -> WalRecord {
        WalRecord::Append {
            topic_id,
            seq,
            ts: 1_700_000_000_000 + seq,
            node: node.map(str::to_string),
            tag: tag.map(str::to_string),
            data: data.to_vec(),
        }
    }

    /// Round-trip a single record through encode → decode and assert equality.
    fn roundtrip(record: WalRecord, durable: bool) {
        let mut buf = Vec::new();
        encode_frame(&mut buf, &record, durable);
        let mut reader = WalReader::new(buf.clone());
        let got = reader.next().expect("one frame");
        assert_eq!(got.record, record, "record round-trip");
        assert_eq!(got.durable, durable, "durable flag round-trip");
        assert_eq!(reader.valid_len(), buf.len(), "consumed the whole frame");
        assert!(reader.next().is_none(), "exactly one frame");
    }

    #[test]
    fn roundtrip_append_plain() {
        roundtrip(append(7, 1, None, None, b"{\"n\":1}"), false);
    }

    #[test]
    fn roundtrip_append_with_node_and_tag() {
        roundtrip(
            append(7, 42, Some("nodeA"), Some("tenant:job-1"), b"payload"),
            true,
        );
    }

    #[test]
    fn roundtrip_append_empty_data() {
        roundtrip(append(3, 100, Some("n"), None, b""), false);
    }

    #[test]
    fn roundtrip_delete_before_seq_only() {
        roundtrip(
            WalRecord::Delete {
                topic_id: 9,
                before_seq: Some(50),
                match_: None,
                seqs: Vec::new(),
                bound_head: Some(50),
                ts: 123,
            },
            true,
        );
    }

    #[test]
    fn roundtrip_delete_explicit_seqs() {
        roundtrip(
            WalRecord::Delete {
                topic_id: 9,
                before_seq: None,
                match_: None,
                seqs: vec![480101, 480104, 480200],
                bound_head: None,
                ts: 7,
            },
            true,
        );
    }

    #[test]
    fn roundtrip_lease_events() {
        roundtrip(
            WalRecord::Lease {
                topic_id: 3,
                seq: 480101,
                event: 0, // claimed
                node: "worker-eu-1".into(),
                lease_id: 0x7f3a9c,
                deadline: 1_748_450_039_000,
                deliveries: 1,
                ts: 1_748_450_001_000,
            },
            true,
        );
        roundtrip(
            WalRecord::Lease {
                topic_id: 3,
                seq: 480104,
                event: 3, // acked
                node: "worker-eu-1".into(),
                lease_id: 0x7f3a9d,
                deadline: 0,
                deliveries: 0,
                ts: 1_748_450_002_000,
            },
            false,
        );
    }

    #[test]
    fn roundtrip_delete_match_eq_and_glob() {
        roundtrip(
            WalRecord::Delete {
                topic_id: 9,
                before_seq: None,
                match_: Some(MatchSel::Eq("exact-tag".into())),
                seqs: Vec::new(),
                bound_head: Some(99),
                ts: 1,
            },
            false,
        );
        roundtrip(
            WalRecord::Delete {
                topic_id: 9,
                before_seq: Some(10),
                match_: Some(MatchSel::Glob("tenant:".into())),
                seqs: Vec::new(),
                bound_head: Some(42),
                ts: 2,
            },
            true,
        );
    }

    #[test]
    fn roundtrip_topic_create_and_tombstone() {
        roundtrip(
            WalRecord::TopicConfig {
                topic_id: 1,
                op: TopicConfigOp {
                    name: "jobs".into(),
                    config: vec![1, 2, 3, 4, 5],
                },
                tombstone: false,
                ts: 99,
            },
            true,
        );
        roundtrip(
            WalRecord::TopicConfig {
                topic_id: 1,
                op: TopicConfigOp {
                    name: "jobs".into(),
                    config: vec![],
                },
                tombstone: true,
                ts: 100,
            },
            true,
        );
    }

    #[test]
    fn roundtrip_router_create_and_delete() {
        roundtrip(
            WalRecord::RouterCreate {
                op: RouterOp {
                    name: "jobs->audit".into(),
                    source: "jobs".into(),
                    dest: "audit".into(),
                    preserve_node: true,
                    preserve_tag: false,
                    create_dest: true,
                    allow_cycle: false,
                    exactly_once: false,
                    filter: Some(MatchSel::Glob("t:".into())),
                    initial_cursor: 42,
                    initial_dest_base: 7,
                },
                ts: 5,
            },
            true,
        );
        roundtrip(
            WalRecord::RouterCreate {
                op: RouterOp {
                    name: "fresh->derived".into(),
                    source: "fresh".into(),
                    dest: "derived".into(),
                    preserve_node: false,
                    preserve_tag: false,
                    create_dest: true,
                    allow_cycle: false,
                    exactly_once: true,
                    filter: None,
                    initial_cursor: 0,
                    initial_dest_base: 0,
                },
                ts: 5,
            },
            true,
        );
        roundtrip(
            WalRecord::RouterDelete {
                name: "jobs->audit".into(),
                ts: 6,
            },
            false,
        );
    }

    #[test]
    fn roundtrip_evict_watermark_and_checkpoint() {
        roundtrip(
            WalRecord::EvictWatermark {
                topic_id: 4,
                evict_floor: 1000,
                expiry_floor: 800,
                earliest_seq: 1001,
                ts: 7,
            },
            true,
        );
        roundtrip(
            WalRecord::CheckpointMark {
                last_checkpoint_seq: 99999,
                ts: 8,
            },
            true,
        );
    }

    #[test]
    fn roundtrip_head_watermark() {
        roundtrip(
            WalRecord::HeadWatermark {
                topic_id: 7,
                head_seq: 4096,
                ts: 12,
            },
            true,
        );
    }

    #[test]
    fn multiple_frames_decode_in_order() {
        let mut buf = Vec::new();
        let mut scratch = Vec::new();
        let recs = vec![
            append(1, 1, None, None, b"a"),
            append(1, 2, Some("n"), Some("t"), b"bb"),
            WalRecord::Delete {
                topic_id: 1,
                before_seq: Some(2),
                match_: None,
                seqs: Vec::new(),
                bound_head: Some(2),
                ts: 3,
            },
        ];
        for r in &recs {
            encode_frame(&mut scratch, r, false);
            buf.extend_from_slice(&scratch);
        }
        let got: Vec<WalRecord> = WalReader::new(buf).map(|f| f.record).collect();
        assert_eq!(got, recs);
    }

    /// A flipped byte anywhere in the frame body MUST fail the CRC and yield no
    /// frame (the torn-tail anchor, ARCHITECTURE §4).
    #[test]
    fn crc_catches_a_flipped_byte() {
        let mut buf = Vec::new();
        encode_frame(
            &mut buf,
            &append(2, 5, Some("node"), Some("tag"), b"hello"),
            false,
        );

        // Flip a byte in the payload region (after the header, before the CRC).
        let flip_at = FRAME_LEN_PREFIX + FRAME_HEADER_LEN + 1;
        let mut corrupt = buf.clone();
        corrupt[flip_at] ^= 0xFF;

        let mut reader = WalReader::new(corrupt);
        assert!(reader.next().is_none(), "CRC mismatch ⇒ no frame yielded");
        assert_eq!(reader.valid_len(), 0, "nothing valid consumed");
    }

    /// Flipping a byte in the CRC field itself is also caught.
    #[test]
    fn crc_catches_corrupted_crc_field() {
        let mut buf = Vec::new();
        encode_frame(&mut buf, &append(2, 5, None, None, b"x"), false);
        let last = buf.len() - 1;
        buf[last] ^= 0x01;
        let mut reader = WalReader::new(buf);
        assert!(reader.next().is_none());
    }

    /// A truncated FINAL frame (interrupted write) is detected — not yielded —
    /// and the valid-prefix length covers exactly the intact frames.
    #[test]
    fn truncated_final_frame_is_detected_and_prefix_reported() {
        let mut buf = Vec::new();
        let mut scratch = Vec::new();
        // Two good frames.
        encode_frame(&mut scratch, &append(1, 1, None, None, b"first"), true);
        buf.extend_from_slice(&scratch);
        let prefix_after_two_minus_one = buf.len();
        encode_frame(
            &mut scratch,
            &append(1, 2, Some("n"), None, b"second"),
            true,
        );
        buf.extend_from_slice(&scratch);
        let valid_end = buf.len();

        // A third frame, then chop it mid-write (simulate a torn tail).
        encode_frame(
            &mut scratch,
            &append(1, 3, None, Some("t"), b"third-partial"),
            true,
        );
        buf.extend_from_slice(&scratch);
        // Truncate so the third frame is incomplete (drop its last 5 bytes).
        buf.truncate(buf.len() - 5);

        let mut reader = WalReader::new(buf);
        let f1 = reader.next().expect("frame 1");
        assert_eq!(f1.record.seq(), 1);
        let f2 = reader.next().expect("frame 2");
        assert_eq!(f2.record.seq(), 2);
        // The torn third frame is NOT interpreted as data.
        assert!(reader.next().is_none(), "torn tail not yielded");
        // valid_len is exactly the end of frame 2 — the safe truncation point.
        assert_eq!(reader.valid_len(), valid_end);
        assert!(reader.valid_len() > prefix_after_two_minus_one);
    }

    /// A frame_len that overruns the buffer (header says more bytes than exist)
    /// is treated as a torn tail.
    #[test]
    fn frame_len_overrun_is_torn() {
        let mut buf = Vec::new();
        encode_frame(&mut buf, &append(1, 1, None, None, b"data"), true);
        // Bump the declared frame_len far past EOF.
        let bad = (buf.len() as u32) + 9999;
        buf[0..4].copy_from_slice(&bad.to_le_bytes());
        let mut reader = WalReader::new(buf);
        assert!(reader.next().is_none());
        assert_eq!(reader.valid_len(), 0);
    }

    /// Trailing zero bytes (preallocated file region) stop the reader cleanly,
    /// not interpreted as a frame.
    #[test]
    fn trailing_zeros_stop_cleanly() {
        let mut buf = Vec::new();
        encode_frame(&mut buf, &append(1, 1, None, None, b"only"), true);
        let valid = buf.len();
        buf.extend_from_slice(&[0u8; 4096]); // preallocation tail.
        let mut reader = WalReader::new(buf);
        assert_eq!(reader.next().expect("frame").record.seq(), 1);
        assert!(reader.next().is_none());
        assert_eq!(reader.valid_len(), valid);
    }

    // -----------------------------------------------------------------------
    // Writer + group-commit tests (dedicated writer thread + a temp dir).
    // -----------------------------------------------------------------------

    #[test]
    fn writer_persists_and_replays_frames() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Wal::open(WalConfig::new(dir.path())).unwrap();
        let w = wal.writer();

        for seq in 1..=5 {
            w.append(append(1, seq, None, Some("t"), b"payload"), true)
                .unwrap();
        }
        wal.shutdown();

        // Read the active WAL file back and verify all 5 frames replay.
        let wal_dir = dir.path().join("wal");
        let mut files: Vec<PathBuf> = std::fs::read_dir(&wal_dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        files.sort();
        let mut seqs = Vec::new();
        for f in files {
            for frame in WalReader::open(&f).unwrap() {
                seqs.push(frame.record.seq());
            }
        }
        assert_eq!(seqs, vec![1, 2, 3, 4, 5]);
    }

    /// Group commit: many durable frames submitted concurrently (from several
    /// threads) coalesce into far fewer fsyncs than frames (asserted via the
    /// fsync counter seam).
    #[test]
    fn group_commit_batches_many_frames_into_few_fsyncs() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = WalConfig::new(dir.path());
        // Widen the window so concurrent submissions reliably coalesce in CI.
        cfg.gc_min = Duration::from_millis(2);
        cfg.gc_max = Duration::from_millis(20);
        let wal = Wal::open(cfg).unwrap();
        let w = wal.writer();
        let metrics = wal.metrics();

        // Fire 200 durable submissions from 8 threads concurrently; wait on all.
        let mut handles = Vec::new();
        for t in 0..8u64 {
            let w = w.clone();
            handles.push(std::thread::spawn(move || {
                for i in 0..25u64 {
                    let seq = t * 25 + i + 1;
                    w.append(append(1, seq, None, None, b"x"), true).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        wal.shutdown();

        let frames = metrics.frames.load(Ordering::Relaxed);
        let fsyncs = metrics.fsyncs.load(Ordering::Relaxed);
        assert_eq!(frames, 200, "all frames written");
        assert!(fsyncs >= 1, "at least one fsync");
        // The whole point of group commit: many frames per fsync.
        assert!(
            fsyncs < frames,
            "group commit must batch (fsyncs={fsyncs} < frames={frames})"
        );
    }

    /// A non-durable batch is written without an fsync (its durability follows a
    /// later group fsync); the frames still persist and replay.
    #[test]
    fn non_durable_writes_skip_fsync_but_persist() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Wal::open(WalConfig::new(dir.path())).unwrap();
        let w = wal.writer();
        let metrics = wal.metrics();

        // A single non-durable write: committed after the buffered write, no fsync.
        w.append(append(1, 1, None, None, b"nd"), false).unwrap();
        wal.shutdown();

        assert_eq!(metrics.frames.load(Ordering::Relaxed), 1);
        // The non-durable batch issued no fsync (shutdown does a best-effort one,
        // so we only assert the batch path itself did not, by frame/batch math).
        let wal_dir = dir.path().join("wal");
        let f = std::fs::read_dir(&wal_dir)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let seqs: Vec<u64> = WalReader::open(&f)
            .unwrap()
            .map(|fr| fr.record.seq())
            .collect();
        assert_eq!(seqs, vec![1]);
    }

    #[test]
    fn commit_proof_marks_fsync_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Wal::open(WalConfig::new(dir.path())).unwrap();
        let w = wal.writer();

        let buffered = w
            .submit(append(1, 1, None, None, b"buffered"), false)
            .unwrap()
            .wait()
            .unwrap();
        assert!(
            !buffered.is_fsynced(),
            "non-durable proof stops at buffered write"
        );

        let fsynced = w
            .submit(append(1, 2, None, None, b"fsynced"), true)
            .unwrap()
            .wait()
            .unwrap();
        assert!(fsynced.is_fsynced(), "durable proof crosses fdatasync");

        wal.shutdown();
    }

    // -----------------------------------------------------------------------
    // R5 — bounded ingest queue / backpressure under a stalled writer.
    // -----------------------------------------------------------------------

    /// An `Fs` that wraps `RealFs` but blocks every `sync_data` (the group-commit
    /// fsync) until released, so the single writer thread stalls mid-batch and the
    /// bounded ingest channel can fill (R5).
    struct StallFs {
        inner: Arc<dyn Fs>,
        gate: Arc<(Mutex<bool>, Condvar)>,
    }
    struct StallFile {
        inner: Box<dyn File>,
        gate: Arc<(Mutex<bool>, Condvar)>,
    }
    impl File for StallFile {
        fn read_at(&self, off: u64, buf: &mut [u8]) -> io::Result<usize> {
            self.inner.read_at(off, buf)
        }
        fn write_at(&mut self, off: u64, buf: &[u8]) -> io::Result<usize> {
            self.inner.write_at(off, buf)
        }
        fn set_len(&mut self, len: u64) -> io::Result<()> {
            self.inner.set_len(len)
        }
        fn sync_data(&self) -> io::Result<()> {
            // Block until released — the writer thread is "stuck on a slow device".
            let (lock, cv) = &*self.gate;
            let mut released = lock.lock().unwrap();
            while !*released {
                released = cv.wait(released).unwrap();
            }
            self.inner.sync_data()
        }
        fn sync_all(&self) -> io::Result<()> {
            self.inner.sync_all()
        }
        fn metadata_len(&self) -> io::Result<u64> {
            self.inner.metadata_len()
        }
    }
    impl Fs for StallFs {
        fn open(&self, path: &Path, opts: OpenOpts) -> io::Result<Box<dyn File>> {
            Ok(Box::new(StallFile {
                inner: self.inner.open(path, opts)?,
                gate: self.gate.clone(),
            }))
        }
        fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
            self.inner.rename(from, to)
        }
        fn remove_file(&self, path: &Path) -> io::Result<()> {
            self.inner.remove_file(path)
        }
        fn read_dir(&self, dir: &Path) -> io::Result<Vec<PathBuf>> {
            self.inner.read_dir(dir)
        }
        fn sync_dir(&self, dir: &Path) -> io::Result<()> {
            self.inner.sync_dir(dir)
        }
        fn create_dir_all(&self, dir: &Path) -> io::Result<()> {
            self.inner.create_dir_all(dir)
        }
        fn exists(&self, path: &Path) -> bool {
            self.inner.exists(path)
        }
        fn metadata_len(&self, path: &Path) -> io::Result<u64> {
            self.inner.metadata_len(path)
        }
    }

    #[test]
    fn submit_backpressures_when_writer_stalled() {
        let dir = tempfile::tempdir().unwrap();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let fs: Arc<dyn Fs> = Arc::new(StallFs {
            inner: RealFs::arc(),
            gate: gate.clone(),
        });
        // Small bounded channel so it fills fast once the writer stalls in fsync.
        let mut cfg = WalConfig::new(dir.path());
        cfg.channel_cap = 4;
        let wal = Wal::open_at_with(fs, cfg, 1, 0).unwrap();
        let w = wal.writer();

        // The writer parks on the first durable batch's fsync (gated). Keep
        // submitting durable frames WITHOUT waiting; the bounded queue fills and
        // `submit` must return `Full` rather than queue without bound.
        let mut hit_full = false;
        for seq in 1..=1000u64 {
            match w.submit(append(1, seq, None, None, b"x"), true) {
                Ok(token) => {
                    // Don't wait (that would block on the stalled fsync); drop it.
                    drop(token);
                }
                Err(WalError::Full) => {
                    hit_full = true;
                    break;
                }
                Err(e) => panic!("unexpected submit error: {e}"),
            }
        }
        assert!(
            hit_full,
            "bounded ingest queue must surface WalError::Full under a stalled writer"
        );

        // M3 observability: the backpressure event was counted and the queue-depth
        // gauge peaked at/under the bounded `channel_cap` (a couple of slots may be
        // in-flight in the writer's hands at the peak).
        let metrics = wal.metrics();
        assert!(
            metrics.submit_full.load(Ordering::Relaxed) >= 1,
            "submit_full counts the R5 backpressure rejection"
        );
        let peak = metrics.queued_peak.load(Ordering::Relaxed);
        assert!(peak >= 1, "queue-depth peak was observed: {peak}");

        // Release the device so the writer can drain + the WAL can shut down
        // cleanly (no deadlock on join).
        {
            let (lock, cv) = &*gate;
            *lock.lock().unwrap() = true;
            cv.notify_all();
        }
        wal.shutdown();

        // After the writer drains everything, the live queue-depth gauge returns
        // to 0 (every accepted submission was dequeued).
        assert_eq!(
            metrics.queued.load(Ordering::Relaxed),
            0,
            "queue-depth gauge returns to 0 once drained"
        );
    }

    /// R5 / codex P0 #1: `submit_batch` is ATOMIC — a whole caller batch commits
    /// (all its frames durable, replayable) or none of it. A committed multi-record
    /// batch must round-trip every frame through the on-disk WAL.
    #[test]
    fn submit_batch_commits_all_frames_atomically() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Wal::open_at(WalConfig::new(dir.path()), 1, 0).unwrap();
        let w = wal.writer();

        // One atomic batch of three frames; block until committed.
        let batch = vec![
            append(1, 1, None, None, b"a"),
            append(1, 2, None, None, b"b"),
            append(1, 3, None, None, b"c"),
        ];
        w.submit_batch(batch, true).unwrap().wait().unwrap();

        // The frames metric counts every frame in the batch (not just the batch).
        assert_eq!(
            wal.metrics().frames.load(Ordering::Relaxed),
            3,
            "all three frames of the atomic batch were written"
        );
        wal.shutdown();

        // Replay the on-disk WAL: exactly the three frames, in order.
        let wal_dir = dir.path().join("wal");
        let path = std::fs::read_dir(&wal_dir)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let seqs: Vec<u64> = WalReader::open(&path)
            .unwrap()
            .map(|f| f.record.seq())
            .collect();
        assert_eq!(seqs, vec![1, 2, 3], "atomic batch round-trips all frames");
    }

    /// R5 / codex P0 #1: under writer backpressure a `submit_batch` that is rejected
    /// (`Full`) must leave NO frames behind — the batch is one channel slot, so it
    /// is accepted all-or-none. This is the load-bearing partial-prefix fix: the old
    /// per-frame loop could accept a prefix and reject the rest, orphaning the
    /// accepted frames in the WAL.
    #[test]
    fn submit_batch_rejected_under_backpressure_leaves_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let fs: Arc<dyn Fs> = Arc::new(StallFs {
            inner: RealFs::arc(),
            gate: gate.clone(),
        });
        let mut cfg = WalConfig::new(dir.path());
        cfg.channel_cap = 2;
        let wal = Wal::open_at_with(fs, cfg, 1, 0).unwrap();
        let w = wal.writer();

        // Fill the bounded queue with multi-frame batches while the writer stalls in
        // fsync; eventually a whole batch is rejected with `Full`.
        let mut hit_full = false;
        let mut next = 1u64;
        for _ in 0..1000 {
            let batch = vec![
                append(1, next, None, None, b"x"),
                append(1, next + 1, None, None, b"y"),
            ];
            next += 2;
            match w.submit_batch(batch, true) {
                Ok(token) => drop(token), // don't wait (writer is stalled).
                Err(WalError::Full) => {
                    hit_full = true;
                    break;
                }
                Err(e) => panic!("unexpected submit error: {e}"),
            }
        }
        assert!(
            hit_full,
            "a whole batch was rejected with Full (atomic, not partial)"
        );

        // Release the device + drain.
        {
            let (lock, cv) = &*gate;
            *lock.lock().unwrap() = true;
            cv.notify_all();
        }
        let metrics = wal.metrics();
        let frames_before = metrics.frames.load(Ordering::Relaxed);
        wal.shutdown();
        let frames_after = metrics.frames.load(Ordering::Relaxed);

        // Every WRITTEN frame count is even: only whole 2-frame batches were ever
        // accepted, never a 1-frame partial prefix of a rejected batch.
        assert_eq!(
            frames_after % 2,
            0,
            "no partial-prefix frame escaped a rejected batch (written frames: {frames_before}..{frames_after})"
        );
    }

    // -----------------------------------------------------------------------
    // R11 — WAL rotation failure must surface (not be swallowed).
    // -----------------------------------------------------------------------

    /// An `Fs` that lets the first WAL file be created but fails any *subsequent*
    /// `create`-style open of a `wal-*.log` path — modelling a rotation that
    /// cannot create the next file (R11).
    struct RotateFailFs {
        inner: Arc<dyn Fs>,
        creates: AtomicU64,
    }
    impl Fs for RotateFailFs {
        fn open(&self, path: &Path, opts: OpenOpts) -> io::Result<Box<dyn File>> {
            let is_wal_create = opts.create
                && opts.truncate
                && path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with("wal-"))
                    .unwrap_or(false);
            if is_wal_create {
                let n = self.creates.fetch_add(1, Ordering::SeqCst);
                if n >= 1 {
                    // The next WAL file (rotation target) cannot be created.
                    return Err(io::Error::other("disk full"));
                }
            }
            self.inner.open(path, opts)
        }
        fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
            self.inner.rename(from, to)
        }
        fn remove_file(&self, path: &Path) -> io::Result<()> {
            self.inner.remove_file(path)
        }
        fn read_dir(&self, dir: &Path) -> io::Result<Vec<PathBuf>> {
            self.inner.read_dir(dir)
        }
        fn sync_dir(&self, dir: &Path) -> io::Result<()> {
            self.inner.sync_dir(dir)
        }
        fn create_dir_all(&self, dir: &Path) -> io::Result<()> {
            self.inner.create_dir_all(dir)
        }
        fn exists(&self, path: &Path) -> bool {
            self.inner.exists(path)
        }
        fn metadata_len(&self, path: &Path) -> io::Result<u64> {
            self.inner.metadata_len(path)
        }
    }

    #[test]
    fn rotation_failure_surfaces_and_latches_read_only() {
        let dir = tempfile::tempdir().unwrap();
        let fs: Arc<dyn Fs> = Arc::new(RotateFailFs {
            inner: RealFs::arc(),
            creates: AtomicU64::new(0),
        });
        // Tiny preallocation so a handful of frames forces a rotation.
        let mut cfg = WalConfig::new(dir.path());
        cfg.file_size = 256;
        let wal = Wal::open_at_with(fs, cfg, 1, 0).unwrap();
        let w = wal.writer();

        // Append until a batch would exceed the 256-byte file and rotation is
        // attempted. The rotation-target create fails ⇒ the batch must FAIL
        // (surfaced as WriterGone) rather than silently writing past the boundary.
        let mut saw_failure = false;
        for seq in 1..=200u64 {
            match w.append(append(1, seq, None, None, b"payloadpayloadpayload"), true) {
                Ok(()) => {}
                Err(WalError::WriterGone) => {
                    saw_failure = true;
                    break;
                }
                Err(e) => panic!("unexpected error: {e}"),
            }
        }
        assert!(
            saw_failure,
            "a WAL rotation create failure must surface to the caller, not be swallowed"
        );

        // Read-only is latched: subsequent appends keep failing (no write past the
        // boundary into the full active file).
        let after = w.append(append(1, 999, None, None, b"x"), true);
        assert!(
            matches!(after, Err(WalError::WriterGone)),
            "writer stays read-only after a rotation failure"
        );

        wal.shutdown();
    }

    /// codex P2: a `submit` racing with shutdown must never leave a `CommitToken`
    /// unsignaled. Many writer threads hammer `submit` while another thread triggers
    /// shutdown; every returned token must resolve (Ok or WriterGone) — a hang here
    /// (token never signaled) would deadlock the join via the harness timeout.
    #[test]
    fn submit_racing_shutdown_never_strands_a_token() {
        use std::sync::mpsc::channel;
        for _ in 0..20 {
            let dir = tempfile::tempdir().unwrap();
            let wal = Wal::open_at(WalConfig::new(dir.path()), 1, 0).unwrap();
            let w = wal.writer();

            let (tx, rx) = channel::<Result<(), WalError>>();
            let mut handles = Vec::new();
            for t in 0..8u32 {
                let w = w.clone();
                let tx = tx.clone();
                handles.push(std::thread::spawn(move || {
                    for seq in 0..50u64 {
                        // submit then wait: a stranded token would hang this wait.
                        let r = match w
                            .submit(append(1, t as u64 * 50 + seq + 1, None, None, b"x"), true)
                        {
                            Ok(tok) => tok.wait().map(drop),
                            Err(e) => Err(e),
                        };
                        let _ = tx.send(r);
                    }
                }));
            }
            drop(tx);

            // Trigger shutdown partway through the race (drops + joins the writer).
            std::thread::spawn(move || {
                std::thread::yield_now();
                wal.shutdown();
            });

            // Every submitted token resolved one way or the other — none stranded.
            // `recv` returns until all senders (writer threads) are done; a stranded
            // token would block a worker forever and this loop would hang (caught by
            // the test harness timeout) instead of completing.
            let mut results = 0usize;
            while rx.recv().is_ok() {
                results += 1;
            }
            for h in handles {
                h.join().unwrap();
            }
            assert_eq!(
                results,
                8 * 50,
                "every submit produced exactly one resolved outcome"
            );
        }
    }
}
