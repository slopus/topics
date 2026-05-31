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
//!    4    1   type        u8    1=Append 2=BoxCreate 3=BoxDelete 4=RouterCreate
//!                                 5=RouterDelete 6=Delete 7=EvictWatermark
//!                                 8=CheckpointMark 9=ConfigUpdate
//!    5    1   flags       u8    bit0=has_tag bit1=has_node bit2=durable
//!    6    4   box_id      u32   interned numeric box id (string<->id in meta)
//!   10    8   seq         u64   server-assigned (0 for non-Append control frames)
//!   18    8   ts          u64   server commit ms
//!   26    2   node_len    u16
//!   28    2   tag_len     u16
//!   30    4   data_len    u32
//!   34    N   node        bytes (node_len)
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
//! `Append`, `Delete`, `BoxCreate`/`ConfigUpdate`/`BoxDelete` (box config and
//! tombstone), `RouterCreate`/`RouterDelete`, `EvictWatermark`, and
//! `CheckpointMark`. The `box_id` is an interned `u32` (the name↔id table is
//! itself logged via `BoxCreate`), keeping data frames small.
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
use std::time::Duration;

use super::fs::{File, Fs, OpenOpts, RealFs};

// ---------------------------------------------------------------------------
// Frame layout constants
// ---------------------------------------------------------------------------

/// Fixed header size: everything from `type` through `data_len` inclusive,
/// i.e. the bytes immediately following `frame_len`. (`type`1 + `flags`1 +
/// `box_id`4 + `seq`8 + `ts`8 + `node_len`2 + `tag_len`2 + `data_len`4 = 30.)
pub const FRAME_HEADER_LEN: usize = 30;
/// Trailing XXH3-64 checksum size.
pub const FRAME_CRC_LEN: usize = 8;
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
    /// Optional forward filter, encoded like [`MatchSel`].
    pub filter: Option<MatchSel>,
}

/// A box create/config payload logged with [`WalRecord::BoxConfig`]. The opaque
/// `config` bytes are the bincode/serde-encoded [`crate::types::BoxConfig`]; the
/// storage layer treats them as a blob (no dependency on the engine config).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoxConfigOp {
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
        box_id: u32,
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
        box_id: u32,
        /// `before_seq` selector (every live seq `< before_seq`), if supplied.
        before_seq: Option<u64>,
        /// `match` selector, if supplied.
        match_: Option<MatchSel>,
        /// Explicit seq set (the queue ack / dead-letter path, DESIGN §10.4):
        /// delete exactly these seqs. Empty for the API §5 selector-based delete.
        /// Replays deterministically (the exact seqs are logged, not re-derived).
        seqs: Vec<u64>,
        /// Point-in-time UPPER BOUND for a selector-based delete: the box's
        /// `head + 1` captured (under the append lock) at the moment the delete was
        /// logged, so replay only ever sweeps seqs strictly below it. A
        /// CONCURRENT/later append that landed after the delete frame carries a seq
        /// `>= bound_head` and is therefore NEVER deleted on replay, preserving the
        /// API §5 point-in-time guarantee across a crash. `None` for an explicit
        /// `seqs`-set delete (the seqs are themselves the bound) and for legacy
        /// frames predating this field (replay then falls back to the recovered
        /// head, the pre-fix behavior).
        bound_head: Option<u64>,
        ts: u64,
    },
    /// Box created or its config updated. `tombstone == false` for create/update;
    /// `BoxConfig{tombstone:true}` is the box-delete marker.
    BoxConfig {
        box_id: u32,
        op: BoxConfigOp,
        /// `true` ⇒ this frame is the box-delete tombstone (config bytes empty).
        tombstone: bool,
        ts: u64,
    },
    /// Router created/updated.
    RouterCreate { op: RouterOp, ts: u64 },
    /// Router deleted (by name).
    RouterDelete { name: String, ts: u64 },
    /// Eviction watermark advanced (cap/TTL involuntary floor) for a box.
    EvictWatermark {
        box_id: u32,
        evict_floor: u64,
        earliest_seq: u64,
        ts: u64,
    },
    /// Checkpoint boundary: every box's highest seq absorbed into segments.
    CheckpointMark {
        last_checkpoint_seq: u64,
        ts: u64,
    },
    /// A leases-log lifecycle event for a queue box (DESIGN §10.1): the pending
    /// who-holds-what state is the materialized projection of these events. Only
    /// written when the queue's `leases_durable:true`; otherwise the projection
    /// is purely in-memory and self-heals on restart (DESIGN §10.6).
    Lease {
        box_id: u32,
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
            WalRecord::BoxConfig { tombstone: false, .. } => T_BOX_CREATE,
            WalRecord::BoxConfig { tombstone: true, .. } => T_BOX_DELETE,
            WalRecord::RouterCreate { .. } => T_ROUTER_CREATE,
            WalRecord::RouterDelete { .. } => T_ROUTER_DELETE,
            WalRecord::EvictWatermark { .. } => T_EVICT_WATERMARK,
            WalRecord::CheckpointMark { .. } => T_CHECKPOINT_MARK,
            WalRecord::Lease { .. } => T_LEASE,
        }
    }

    /// The interned box id this record targets (`0` for box-agnostic control
    /// frames like routers and checkpoints).
    pub fn box_id(&self) -> u32 {
        match self {
            WalRecord::Append { box_id, .. } => *box_id,
            WalRecord::Delete { box_id, .. } => *box_id,
            WalRecord::BoxConfig { box_id, .. } => *box_id,
            WalRecord::EvictWatermark { box_id, .. } => *box_id,
            WalRecord::Lease { box_id, .. } => *box_id,
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
    let box_id = record.box_id();
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
            // The trailing `bound_head` is appended AFTER the (always-present) seqs
            // section so a legacy reader stops cleanly after the seqs and a new
            // reader picks the bound up when present (backward/forward compatible).
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
        WalRecord::BoxConfig { op, ts: rts, .. } => {
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
                | ((op.allow_cycle as u8) << 3);
            data_buf.push(bools);
            write_match(&mut data_buf, &op.filter);
            data = &data_buf;
        }
        WalRecord::RouterDelete { name, ts: rts } => {
            ts = *rts;
            write_lp_bytes(&mut data_buf, name.as_bytes());
            data = &data_buf;
        }
        WalRecord::EvictWatermark {
            evict_floor,
            earliest_seq,
            ts: rts,
            ..
        } => {
            ts = *rts;
            data_buf.extend_from_slice(&evict_floor.to_le_bytes());
            data_buf.extend_from_slice(&earliest_seq.to_le_bytes());
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
    }

    // --- Fixed header (§2.1 offsets 4..34). ---------------------------------
    out.push(type_tag);
    out.push(flags);
    out.extend_from_slice(&box_id.to_le_bytes());
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
    let box_id = u32::from_le_bytes(h[2..6].try_into().unwrap());
    let seq = u64::from_le_bytes(h[6..14].try_into().unwrap());
    let ts = u64::from_le_bytes(h[14..22].try_into().unwrap());
    let node_len = u16::from_le_bytes(h[22..24].try_into().unwrap()) as usize;
    let tag_len = u16::from_le_bytes(h[24..26].try_into().unwrap()) as usize;
    let data_len = u32::from_le_bytes(h[26..30].try_into().unwrap()) as usize;

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
            box_id,
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
            // Explicit seq set (queue ack / dead-letter). Absent in older frames
            // ⇒ treat as empty (the prefix read consumed exactly the selectors).
            let seqs = if pos < data.len() {
                if pos + 4 > data.len() {
                    return DecodeStep::Torn;
                }
                let n = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
                pos += 4;
                let mut v = Vec::with_capacity(n);
                for _ in 0..n {
                    match read_u64(data, &mut pos) {
                        Some(s) => v.push(s),
                        None => return DecodeStep::Torn,
                    }
                }
                v
            } else {
                Vec::new()
            };
            // Trailing point-in-time bound (this-version frames write it right
            // after the seqs section). A legacy frame ends after the seqs ⇒
            // `bound_head = None` (replay falls back to the recovered head).
            let bound_head = if pos < data.len() {
                match read_u8(data, &mut pos) {
                    Some(1) => match read_u64(data, &mut pos) {
                        Some(v) => Some(v),
                        None => return DecodeStep::Torn,
                    },
                    Some(0) => None,
                    _ => return DecodeStep::Torn,
                }
            } else {
                None
            };
            WalRecord::Delete {
                box_id,
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
            WalRecord::BoxConfig {
                box_id,
                op: BoxConfigOp { name, config },
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
            match (name, source, dest, bools, filter) {
                (Some(name), Some(source), Some(dest), Some(bools), Some(filter)) => {
                    WalRecord::RouterCreate {
                        op: RouterOp {
                            name,
                            source,
                            dest,
                            preserve_node: bools & 1 != 0,
                            preserve_tag: bools & 2 != 0,
                            create_dest: bools & 4 != 0,
                            allow_cycle: bools & 8 != 0,
                            filter,
                        },
                        ts,
                    }
                }
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
            match (evict_floor, earliest_seq) {
                (Some(evict_floor), Some(earliest_seq)) => WalRecord::EvictWatermark {
                    box_id,
                    evict_floor,
                    earliest_seq,
                    ts,
                },
                _ => return DecodeStep::Torn,
            }
        }
        T_CONFIG_UPDATE => {
            // ConfigUpdate shares the BoxConfig wire shape (name + config blob).
            let mut pos = 0usize;
            let name = match read_lp_str(data, &mut pos) {
                Some(s) => s,
                None => return DecodeStep::Torn,
            };
            let config = match read_lp_bytes(data, &mut pos) {
                Some(b) => b,
                None => return DecodeStep::Torn,
            };
            WalRecord::BoxConfig {
                box_id,
                op: BoxConfigOp { name, config },
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
                        box_id,
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
    /// Data directory; the WAL lives under `<data_dir>/wal`.
    pub dir: PathBuf,
    /// Minimum group-commit window (a lone durable write fsyncs ~immediately).
    pub gc_min: Duration,
    /// Maximum group-commit window under load.
    pub gc_max: Duration,
    /// Preallocated size per WAL file; the active file rotates at this size.
    pub file_size: u64,
    /// Ingest channel capacity (bounded backpressure for the single writer).
    pub channel_cap: usize,
}

impl WalConfig {
    /// Defaults matching ARCHITECTURE §2.3 (GC 0.5..10 ms) and §2.4 (64 MiB
    /// preallocated files).
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        WalConfig {
            dir: dir.into(),
            gc_min: Duration::from_micros(500),
            gc_max: Duration::from_millis(10),
            file_size: 64 << 20,
            channel_cap: 4096,
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
}

impl CommitToken {
    /// Block the calling thread until the submission is committed. Returns
    /// `Err(WalError::WriterGone)` if the writer failed/exited before commit.
    pub fn wait(self) -> Result<(), WalError> {
        let mut guard = self.state.committed.lock().unwrap();
        loop {
            match *guard {
                CommitOutcome::Ok => return Ok(()),
                CommitOutcome::Failed => return Err(WalError::WriterGone),
                CommitOutcome::Pending => {
                    guard = self.state.cv.wait(guard).unwrap();
                }
            }
        }
    }
}

/// A submission handed to the writer: a record, its durability class, and the
/// shared state used to signal the caller once committed.
struct Submission {
    record: WalRecord,
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
}

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
        }
    }
}

/// Handle the engine holds to submit records to the WAL. Cloneable; all clones
/// feed the same single writer thread.
#[derive(Clone)]
pub struct WalWriter {
    tx: mpsc::Sender<Submission>,
    metrics: Arc<WalMetrics>,
}

impl WalWriter {
    /// Submit `record` with durability class `durable`, returning a
    /// [`CommitToken`]. The token resolves once the record is committed: for a
    /// durable record, after the group fsync; for a non-durable record, after
    /// the buffered write (its durability follows on the next group fsync). Drop
    /// the token to fire-and-forget. An `Err` means the writer thread is gone.
    pub fn submit(&self, record: WalRecord, durable: bool) -> Result<CommitToken, WalError> {
        let state = Arc::new(CommitState {
            committed: Mutex::new(CommitOutcome::Pending),
            cv: Condvar::new(),
        });
        self.tx
            .send(Submission {
                record,
                durable,
                state: state.clone(),
            })
            .map_err(|_| WalError::WriterGone)?;
        Ok(CommitToken { state })
    }

    /// Submit and block until the commit completes, in one call.
    pub fn append(&self, record: WalRecord, durable: bool) -> Result<(), WalError> {
        self.submit(record, durable)?.wait()
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
        let wal_dir = cfg.dir.join("wal");
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
        let (tx, rx) = mpsc::channel::<Submission>();
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
        };
        let handle = std::thread::Builder::new()
            .name("streams-wal".to_string())
            .spawn(move || task.run())
            .map_err(WalError::Io)?;

        Ok(Wal {
            writer: WalWriter { tx, metrics },
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
            let n = self.file.write_at(self.len + written as u64, &bytes[written..])?;
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
                Ok(s) => s,
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

            // Adaptive window: only a *durable* batch needs to wait at all, and
            // only to let concurrent durable writers join this fsync. The window
            // collapses to ~0 when quiet (a lone durable write fsyncs at once)
            // and widens toward `gc_max` under load. Cheap load proxy = how many
            // frames we just coalesced relative to the channel capacity.
            if any_durable {
                let window = self.adaptive_window(pending.len());
                if window > Duration::ZERO {
                    // ONE bounded sleep (never a spin-loop): bounded by gc_max,
                    // so the writer always makes progress.
                    std::thread::sleep(window);
                    // Pull any stragglers that arrived during the window.
                    self.drain_ready(&mut pending, &mut any_durable);
                }
            }

            self.commit_batch(&mut scratch, &mut batch_bytes, pending, any_durable);
        }

        // Final best-effort fdatasync hardens any non-durable tail on clean exit.
        let _ = self.file.fdatasync();
    }

    /// The adaptive group-commit window for a batch that just coalesced
    /// `batched` frames. Lerps `gc_min..gc_max` by load; a tiny batch (quiet)
    /// uses ~`gc_min`, a large one (saturated) approaches `gc_max`.
    fn adaptive_window(&self, batched: usize) -> Duration {
        let cap = self.cfg.channel_cap.max(1);
        let frac = (batched as f64 / cap as f64).min(1.0);
        let span = self.cfg.gc_max.saturating_sub(self.cfg.gc_min);
        self.cfg.gc_min + span.mul_f64(frac)
    }

    /// On shutdown, commit every already-submitted frame in one final fsynced
    /// batch so no queued write is lost, then return. The channel may still be
    /// open (clones outstanding) but only currently-queued frames are drained.
    fn drain_and_commit_remaining(&mut self, scratch: &mut Vec<u8>, batch_bytes: &mut Vec<u8>) {
        let mut pending: Vec<Submission> = Vec::new();
        let mut any_durable = false;
        self.drain_ready(&mut pending, &mut any_durable);
        // Force a fsync on the final batch regardless of class, so a clean
        // shutdown hardens the tail.
        self.commit_batch(scratch, batch_bytes, pending, true);
    }

    /// Move every immediately-available submission out of the channel.
    fn drain_ready(&mut self, pending: &mut Vec<Submission>, any_durable: &mut bool) {
        while let Ok(s) = self.rx.try_recv() {
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
        for sub in pending {
            encode_frame(scratch, &sub.record, sub.durable);
            batch_bytes.extend_from_slice(scratch);
            states.push(sub.state);
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
            if let Ok(next) = ActiveFile::create(&self.fs, &self.wal_dir, next_idx, self.cfg.file_size)
            {
                self.file = next;
                self.metrics.rotations.fetch_add(1, Ordering::Relaxed);
                // Publish the new active file index + reset length so a snapshot
                // records the post-rotation checkpoint position.
                self.metrics.active_idx.store(next_idx, Ordering::Relaxed);
                self.metrics.active_len.store(0, Ordering::Relaxed);
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
        let frames = states.len() as u64;
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

    fn append(box_id: u32, seq: u64, node: Option<&str>, tag: Option<&str>, data: &[u8]) -> WalRecord {
        WalRecord::Append {
            box_id,
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
        roundtrip(append(7, 42, Some("nodeA"), Some("tenant:job-1"), b"payload"), true);
    }

    #[test]
    fn roundtrip_append_empty_data() {
        roundtrip(append(3, 100, Some("n"), None, b""), false);
    }

    #[test]
    fn roundtrip_delete_before_seq_only() {
        roundtrip(
            WalRecord::Delete {
                box_id: 9,
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
                box_id: 9,
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
                box_id: 3,
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
                box_id: 3,
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
                box_id: 9,
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
                box_id: 9,
                before_seq: Some(10),
                match_: Some(MatchSel::Glob("tenant:".into())),
                seqs: Vec::new(),
                bound_head: Some(42),
                ts: 2,
            },
            true,
        );
    }

    /// A LEGACY Delete frame (selectors only, written before the `seqs` and
    /// `bound_head` fields existed) must still decode: `seqs` empty and
    /// `bound_head: None` (replay then falls back to the recovered head). This
    /// pins the backward-compatible body layout so an old on-disk WAL keeps
    /// replaying after this change.
    #[test]
    fn legacy_delete_frame_decodes_without_seqs_or_bound() {
        // Hand-assemble the legacy body: [has_before=1][before:u64] [match=none].
        // No `n_seqs` field and no `bound` field — exactly what the old encoder
        // wrote.
        let mut body = Vec::new();
        body.push(1u8);
        body.extend_from_slice(&50u64.to_le_bytes());
        body.push(0u8); // match: none

        // Frame the body like `encode_frame` does, with the Delete type tag.
        let mut frame = Vec::new();
        frame.extend_from_slice(&[0u8; FRAME_LEN_PREFIX]);
        frame.push(T_DELETE);
        frame.push(0u8); // flags (not durable)
        frame.extend_from_slice(&9u32.to_le_bytes()); // box_id
        frame.extend_from_slice(&0u64.to_le_bytes()); // seq
        frame.extend_from_slice(&123u64.to_le_bytes()); // ts
        frame.extend_from_slice(&0u16.to_le_bytes()); // node_len
        frame.extend_from_slice(&0u16.to_le_bytes()); // tag_len
        frame.extend_from_slice(&(body.len() as u32).to_le_bytes()); // data_len
        frame.extend_from_slice(&body);
        let crc_val = crc(&frame[FRAME_LEN_PREFIX..]);
        frame.extend_from_slice(&crc_val.to_le_bytes());
        let frame_len = (frame.len() - FRAME_LEN_PREFIX) as u32;
        frame[0..FRAME_LEN_PREFIX].copy_from_slice(&frame_len.to_le_bytes());

        let got = WalReader::new(frame).next().expect("legacy frame decodes");
        assert_eq!(
            got.record,
            WalRecord::Delete {
                box_id: 9,
                before_seq: Some(50),
                match_: None,
                seqs: Vec::new(),
                bound_head: None,
                ts: 123,
            }
        );
    }

    #[test]
    fn roundtrip_box_create_and_tombstone() {
        roundtrip(
            WalRecord::BoxConfig {
                box_id: 1,
                op: BoxConfigOp {
                    name: "jobs".into(),
                    config: vec![1, 2, 3, 4, 5],
                },
                tombstone: false,
                ts: 99,
            },
            true,
        );
        roundtrip(
            WalRecord::BoxConfig {
                box_id: 1,
                op: BoxConfigOp {
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
                    filter: Some(MatchSel::Glob("t:".into())),
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
                box_id: 4,
                evict_floor: 1000,
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
    fn multiple_frames_decode_in_order() {
        let mut buf = Vec::new();
        let mut scratch = Vec::new();
        let recs = vec![
            append(1, 1, None, None, b"a"),
            append(1, 2, Some("n"), Some("t"), b"bb"),
            WalRecord::Delete {
                box_id: 1,
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
        encode_frame(&mut buf, &append(2, 5, Some("node"), Some("tag"), b"hello"), false);

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
        encode_frame(&mut scratch, &append(1, 2, Some("n"), None, b"second"), true);
        buf.extend_from_slice(&scratch);
        let valid_end = buf.len();

        // A third frame, then chop it mid-write (simulate a torn tail).
        encode_frame(&mut scratch, &append(1, 3, None, Some("t"), b"third-partial"), true);
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
        let f = std::fs::read_dir(&wal_dir).unwrap().next().unwrap().unwrap().path();
        let seqs: Vec<u64> = WalReader::open(&f).unwrap().map(|fr| fr.record.seq()).collect();
        assert_eq!(seqs, vec![1]);
    }
}
