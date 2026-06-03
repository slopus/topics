//! Sealed/active **segment** file format: the long-term, per-topic materialization
//! of the WAL's `Append` records (ARCHITECTURE §3.2). A segment covers a
//! contiguous seq range `[start_seq, end_seq]` and is two files:
//!
//! ```text
//! seg-<start_seq>.data    append-ordered framed records (one frame per record)
//! seg-<start_seq>.idx     fixed-stride per-record locator; entry i <=> seq (start_seq + i)
//! ```
//!
//! # `.data` frame format
//!
//! A **close variant** of the WAL frame (ARCHITECTURE §2.1): every record is one
//! length-prefixed, XXH3-protected frame, but a segment holds **only data
//! records** so the frame carries just `seq/ts/flags/node/tag/payload` (no record
//! `type` byte — every frame is an Append). Multi-byte integers little-endian.
//!
//! ```text
//!  off  size  field
//!    0    4   frame_len   u32   bytes of this frame EXCLUDING this field
//!    4    1   flags       u8    bit0=has_tag bit1=has_node
//!    5    8   seq         u64   the record's seq
//!   13    8   ts          u64   server commit ms
//!   21    2   node_len    u16
//!   23    2   tag_len     u16
//!   25    4   data_len    u32
//!   29    N   node        bytes (node_len)
//!    .    M   tag         bytes (tag_len)
//!    .    P   data+meta   bytes (data_len)   -- opaque payload blob
//!    .    8   xxh3        u64   XXH3-64 over bytes [4 .. crc_start)
//!    .    1   del_flag    u8    DELETED sentinel (0xD5) ⇒ deleted; anything else ⇒ live
//! ```
//!
//! `frame_len` first lets a reader validate a frame boundary without parsing the
//! body; the trailing XXH3-64 over everything between `frame_len` and the checksum
//! detects bit-rot / a torn write (the same crash anchor as the WAL, §2.1). A
//! mismatch on a *sealed* (immutable) segment is corruption, not a torn tail — the
//! caller surfaces it rather than silently truncating.
//!
//! # In-place delete (DESIGN §7, the segment side)
//!
//! `del_flag` is a **single trailing byte AFTER the XXH3 checksum** — outside the
//! checksum-covered region on purpose, so a deletion flips ONE byte in place and
//! re-fsyncs WITHOUT rewriting the frame or recomputing the checksum (`frame_len`
//! INCLUDES this byte, so the framing stride is unchanged). Liveness is decided by
//! an exact sentinel: a frame is deleted ONLY when `del_flag == 0xD5`
//! ([`SEG_DEL_SENTINEL`]); every other value — a fresh `0x00`, or any intermediate
//! value a torn single-byte write could leave — reads as **LIVE**. That makes the
//! flip crash-safe: a single-byte write is sector-atomic, so a mid-flip crash
//! either lands the full sentinel (deleted) or leaves the old byte (live) — it can
//! never corrupt the framing, skip a live record, or resurrect a deleted one. A
//! torn flip that somehow left a partial value is read as live (the delete simply
//! did not take durably; the WAL Delete frame + in-memory mark still cover it until
//! the next checkpoint re-flips). The XXH3 covers the record BODY only, so it is
//! untouched by a delete and still detects bit-rot of the payload.
//!
//! # `.idx` format
//!
//! A fixed 20-byte stride per record — the on-disk twin of the in-memory
//! `RecordLoc` (ARCHITECTURE §1.1, §3.2):
//!
//! ```text
//!  off  size  field
//!    0    4   offset   u32   byte offset of the record's frame in the .data file
//!    4    4   len      u32   framed length (so a record is read without neighbors)
//!    8    8   ts       u64   server commit ms (inline ⇒ TTL binary search w/o .data)
//!   16    1   flags    u8    bit0=has_tag bit1=has_node (cheap presence probe)
//!   17    3   pad      u8x3  reserved (keeps the stride a round 20 bytes)
//! ```
//!
//! `seq → entry` is a direct seek: `(seq - start_seq) * IDX_STRIDE`. No scan, no
//! hashing — rebuilding the in-memory index on restart is a bulk read of `.idx`.
//!
//! Stage 1 defines the **format + encode/decode + lookup** only; wiring segments
//! into the checkpoint/serving path lands in a later stage.

use std::io;

/// Segment `.data` fixed header: everything from `flags` through `data_len`,
/// i.e. the bytes immediately following `frame_len`. (`flags`1 + `seq`8 + `ts`8 +
/// `node_len`2 + `tag_len`2 + `data_len`4 = 25.)
pub const SEG_FRAME_HEADER_LEN: usize = 25;
/// Trailing XXH3-64 checksum size on each `.data` frame.
pub const SEG_FRAME_CRC_LEN: usize = 8;
/// Segment `.data` / `.idx` format version. Bump only for incompatible layout changes.
pub const SEGMENT_FORMAT_VERSION: u32 = 1;
/// The trailing in-place **delete-flag** byte after the checksum. One byte so a
/// deletion is a single sector-atomic flip + fsync (crash-safe; see module docs).
pub const SEG_FRAME_DEL_LEN: usize = 1;
/// The exact `del_flag` value that marks a frame DELETED. Any other byte value —
/// `0x00` (the encode default), or an intermediate value a torn flip could leave —
/// reads as LIVE, so a mid-flip crash never resurrects/skips a record.
pub const SEG_DEL_SENTINEL: u8 = 0xD5;
/// The `del_flag` value of a LIVE frame as written by [`encode_data_frame`].
pub const SEG_DEL_LIVE: u8 = 0x00;
/// The leading `frame_len` u32 size.
const SEG_FRAME_LEN_PREFIX: usize = 4;

/// Fixed `.idx` entry stride: `offset`4 + `len`4 + `ts`8 + `flags`1 + `pad`3.
pub const IDX_STRIDE: usize = 20;

// `.data` flag bits.
const SEG_FLAG_HAS_TAG: u8 = 1 << 0;
const SEG_FLAG_HAS_NODE: u8 = 1 << 1;

/// Compute the XXH3-64 checksum of a buffer — the same fast 64-bit hash the WAL
/// uses for torn-write / bit-rot detection (ARCHITECTURE §2.1).
#[inline]
fn crc(bytes: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(bytes)
}

/// One data record as it lives in (or is read from) a segment `.data` file. The
/// `data` blob is the opaque `data+meta` payload (the same envelope the WAL
/// `Append` frame carries), so a segment record reconstructs a `StoredRecord`
/// exactly. Decoupled from the engine/wire types on purpose (the storage layer
/// owns its own format).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentRecord {
    pub seq: u64,
    pub ts: u64,
    pub node: Option<String>,
    pub tag: Option<String>,
    pub data: Vec<u8>,
}

/// A fixed-stride `.idx` entry: where a record's frame lives in the `.data` file
/// plus the inline `ts`/presence flags (so TTL math + a presence probe avoid
/// touching `.data`). `entry i` corresponds to `seq = start_seq + i`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdxEntry {
    /// Byte offset of the record's frame in the `.data` file.
    pub offset: u32,
    /// Framed length of the record (the slice `[offset .. offset+len]`).
    pub len: u32,
    /// Server commit ms (inline for TTL binary search without `.data`).
    pub ts: u64,
    /// `has_tag`/`has_node` presence bits (mirrors the `.data` flags).
    pub flags: u8,
}

impl IdxEntry {
    /// Whether the record carries a tag (without reading `.data`).
    pub fn has_tag(&self) -> bool {
        self.flags & SEG_FLAG_HAS_TAG != 0
    }
    /// Whether the record carries a node (without reading `.data`).
    pub fn has_node(&self) -> bool {
        self.flags & SEG_FLAG_HAS_NODE != 0
    }
}

/// Errors building/parsing a segment's files.
#[derive(Debug, thiserror::Error)]
pub enum SegmentError {
    #[error("segment frame is corrupt or truncated: {0}")]
    Corrupt(String),
    #[error("segment seq {seq} is outside this segment's range [{start}, {end}]")]
    OutOfRange { seq: u64, start: u64, end: u64 },
    #[error("segment io error: {0}")]
    Io(#[from] io::Error),
}

// ---------------------------------------------------------------------------
// .data frame encode / decode
// ---------------------------------------------------------------------------

/// Encode one record into `out`, appending a complete length-prefixed,
/// CRC-protected `.data` frame. Returns the framed length (so the caller can
/// record the `.idx` offset/len as it builds the segment).
pub fn encode_data_frame(out: &mut Vec<u8>, rec: &SegmentRecord) -> u32 {
    let frame_start = out.len();

    // Reserve the frame_len prefix; backfilled once the body length is known.
    out.extend_from_slice(&[0u8; SEG_FRAME_LEN_PREFIX]);

    let mut flags = 0u8;
    let node: &[u8] = match &rec.node {
        Some(n) => {
            flags |= SEG_FLAG_HAS_NODE;
            n.as_bytes()
        }
        None => &[],
    };
    let tag: &[u8] = match &rec.tag {
        Some(t) => {
            flags |= SEG_FLAG_HAS_TAG;
            t.as_bytes()
        }
        None => &[],
    };

    // Fixed header (offsets 4..29).
    out.push(flags);
    out.extend_from_slice(&rec.seq.to_le_bytes());
    out.extend_from_slice(&rec.ts.to_le_bytes());
    out.extend_from_slice(&(node.len() as u16).to_le_bytes());
    out.extend_from_slice(&(tag.len() as u16).to_le_bytes());
    out.extend_from_slice(&(rec.data.len() as u32).to_le_bytes());
    out.extend_from_slice(node);
    out.extend_from_slice(tag);
    out.extend_from_slice(&rec.data);

    // CRC over [4 .. crc_start) (the body, excluding the frame_len prefix AND the
    // trailing del_flag byte — the flag is flipped in place after sealing, so it
    // must NOT be covered by the checksum).
    let crc_val = crc(&out[frame_start + SEG_FRAME_LEN_PREFIX..]);
    out.extend_from_slice(&crc_val.to_le_bytes());

    // The in-place delete flag (after the CRC, INSIDE frame_len). A fresh frame is
    // LIVE; a delete flips this one byte to `SEG_DEL_SENTINEL` and re-fsyncs.
    out.push(SEG_DEL_LIVE);

    // frame_len = everything after the prefix = (total - 4), now incl. del_flag.
    let frame_len = (out.len() - frame_start - SEG_FRAME_LEN_PREFIX) as u32;
    out[frame_start..frame_start + SEG_FRAME_LEN_PREFIX].copy_from_slice(&frame_len.to_le_bytes());

    (out.len() - frame_start) as u32
}

/// The byte offset of a frame's `del_flag` within the framed slice (relative to
/// the frame start): `frame_len` prefix + the frame body + the CRC. Used to flip
/// the flag in place in the `.data` file (an absolute file offset is this plus the
/// frame's `.idx` offset). Only meaningful for a current-format frame (one written
/// by this build's [`encode_data_frame`], whose `frame_len` includes the byte).
#[inline]
pub fn del_flag_offset_in_frame(framed_len: u32) -> u64 {
    framed_len as u64 - SEG_FRAME_DEL_LEN as u64
}

/// Whether a framed slice is a DELETED record — **CRC-validated**, not a raw
/// trailing-byte peek. A frame is deleted ONLY when it decodes cleanly under the
/// current layout (trailing `del_flag` byte inside `frame_len`, CRC over the body
/// before it) AND that `del_flag` is exactly [`SEG_DEL_SENTINEL`]. Anything else
/// reads LIVE:
/// - a CORRUPT / truncated range — never validates, so it cannot fabricate a
///   deletion that skips a live record;
/// - a torn/partial flip that left an intermediate value — not the sentinel ⇒ live.
///
/// This makes the recovery delete scan and the read-path liveness probe robust to
/// a corrupt `.idx`/`.data` range.
#[inline]
pub fn frame_is_deleted(buf: &[u8]) -> bool {
    matches!(decode_data_frame_full(buf), Ok((_, true)))
}

/// Decode exactly one `.data` frame from the start of `buf`, returning just the
/// record. See [`decode_data_frame_full`] for the deleted flag. Validates
/// `frame_len`, the XXH3, the delete flag, and the internal section lengths. A
/// mismatch is corruption (a sealed segment is immutable, so there is no torn-tail
/// truncation — the caller surfaces the error).
pub fn decode_data_frame(buf: &[u8]) -> Result<SegmentRecord, SegmentError> {
    decode_data_frame_full(buf).map(|(r, _)| r)
}

/// Decode one `.data` frame, returning `(record, is_deleted)`. The current format
/// stores a trailing `del_flag` byte inside `frame_len`; the CRC covers the body
/// before that flag. Frames without the flag do not match the current format and
/// are rejected as corrupt.
pub fn decode_data_frame_full(buf: &[u8]) -> Result<(SegmentRecord, bool), SegmentError> {
    if buf.len() < SEG_FRAME_LEN_PREFIX {
        return Err(SegmentError::Corrupt(
            "shorter than frame_len prefix".into(),
        ));
    }
    let frame_len = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
    let total = SEG_FRAME_LEN_PREFIX + frame_len;
    if buf.len() < total {
        return Err(SegmentError::Corrupt("frame_len overruns the slice".into()));
    }

    if frame_len >= SEG_FRAME_HEADER_LEN + SEG_FRAME_CRC_LEN + SEG_FRAME_DEL_LEN {
        let crc_end = total - SEG_FRAME_DEL_LEN;
        let crc_start = crc_end - SEG_FRAME_CRC_LEN;
        let stored_crc = u64::from_le_bytes(buf[crc_start..crc_end].try_into().unwrap());
        if stored_crc == crc(&buf[SEG_FRAME_LEN_PREFIX..crc_start]) {
            let rec = parse_body(&buf[SEG_FRAME_LEN_PREFIX..crc_start])?;
            let deleted = buf[crc_end] == SEG_DEL_SENTINEL;
            return Ok((rec, deleted));
        }
    }

    if frame_len < SEG_FRAME_HEADER_LEN + SEG_FRAME_CRC_LEN + SEG_FRAME_DEL_LEN {
        return Err(SegmentError::Corrupt("frame_len below the minimum".into()));
    }
    Err(SegmentError::Corrupt("checksum mismatch".into()))
}

/// Parse the record fields out of the body slice `[flags .. data]` (the bytes
/// between `frame_len` and the CRC, in either layout). Validates the internal
/// section lengths and UTF-8 of node/tag.
fn parse_body(h: &[u8]) -> Result<SegmentRecord, SegmentError> {
    if h.len() < SEG_FRAME_HEADER_LEN {
        return Err(SegmentError::Corrupt("frame body too short".into()));
    }
    let flags = h[0];
    let seq = u64::from_le_bytes(h[1..9].try_into().unwrap());
    let ts = u64::from_le_bytes(h[9..17].try_into().unwrap());
    let node_len = u16::from_le_bytes(h[17..19].try_into().unwrap()) as usize;
    let tag_len = u16::from_le_bytes(h[19..21].try_into().unwrap()) as usize;
    let data_len = u32::from_le_bytes(h[21..25].try_into().unwrap()) as usize;

    let body = &h[SEG_FRAME_HEADER_LEN..];
    if node_len + tag_len + data_len != body.len() {
        return Err(SegmentError::Corrupt(
            "internal length inconsistency".into(),
        ));
    }
    let node_bytes = &body[..node_len];
    let tag_bytes = &body[node_len..node_len + tag_len];
    let data = body[node_len + tag_len..].to_vec();

    let node = if flags & SEG_FLAG_HAS_NODE != 0 {
        Some(
            String::from_utf8(node_bytes.to_vec())
                .map_err(|_| SegmentError::Corrupt("node not utf-8".into()))?,
        )
    } else {
        None
    };
    let tag = if flags & SEG_FLAG_HAS_TAG != 0 {
        Some(
            String::from_utf8(tag_bytes.to_vec())
                .map_err(|_| SegmentError::Corrupt("tag not utf-8".into()))?,
        )
    } else {
        None
    };

    Ok(SegmentRecord {
        seq,
        ts,
        node,
        tag,
        data,
    })
}

// ---------------------------------------------------------------------------
// .idx encode / decode + lookup
// ---------------------------------------------------------------------------

/// Encode one `.idx` entry, appending `IDX_STRIDE` bytes to `out`.
pub fn encode_idx_entry(out: &mut Vec<u8>, e: &IdxEntry) {
    out.extend_from_slice(&e.offset.to_le_bytes());
    out.extend_from_slice(&e.len.to_le_bytes());
    out.extend_from_slice(&e.ts.to_le_bytes());
    out.push(e.flags);
    out.extend_from_slice(&[0u8; 3]); // pad to the 20-byte stride.
}

/// Decode the `.idx` entry at `entry_idx` from a full `.idx` byte buffer. Returns
/// `None` if the index is past the end (so a `seq` outside the segment is a clean
/// miss, not a panic).
pub fn idx_entry_at(idx_buf: &[u8], entry_idx: usize) -> Option<IdxEntry> {
    let start = entry_idx.checked_mul(IDX_STRIDE)?;
    let end = start.checked_add(IDX_STRIDE)?;
    if end > idx_buf.len() {
        return None;
    }
    let e = &idx_buf[start..end];
    Some(IdxEntry {
        offset: u32::from_le_bytes(e[0..4].try_into().unwrap()),
        len: u32::from_le_bytes(e[4..8].try_into().unwrap()),
        ts: u64::from_le_bytes(e[8..16].try_into().unwrap()),
        flags: e[16],
    })
}

/// Number of records an `.idx` buffer holds (`len / IDX_STRIDE`).
pub fn idx_len(idx_buf: &[u8]) -> usize {
    idx_buf.len() / IDX_STRIDE
}

/// Locate a `seq` in a segment whose first record is `start_seq`: a direct seek
/// to entry `(seq - start_seq)` in the `.idx` buffer. `None` if `seq` is below
/// `start_seq` or past the segment's last record.
pub fn lookup(idx_buf: &[u8], start_seq: u64, seq: u64) -> Option<IdxEntry> {
    if seq < start_seq {
        return None;
    }
    idx_entry_at(idx_buf, (seq - start_seq) as usize)
}

// ---------------------------------------------------------------------------
// Builder: encode a contiguous run of records into a (.data, .idx) pair.
// ---------------------------------------------------------------------------

/// Accumulates a contiguous run of records into the `.data` + `.idx` byte
/// buffers of one segment. Records MUST be pushed in ascending, gapless seq order
/// starting at `start_seq` (segments mirror the append order of a topic).
#[derive(Debug)]
pub struct SegmentBuilder {
    start_seq: u64,
    next_seq: u64,
    data: Vec<u8>,
    idx: Vec<u8>,
}

impl SegmentBuilder {
    /// Begin a segment whose first record is `start_seq`.
    pub fn new(start_seq: u64) -> Self {
        SegmentBuilder {
            start_seq,
            next_seq: start_seq,
            data: Vec::new(),
            idx: Vec::new(),
        }
    }

    /// Append one record. Panics (debug) if `rec.seq` is not the expected next
    /// contiguous seq — segments are dense and gapless by construction.
    pub fn push(&mut self, rec: &SegmentRecord) {
        debug_assert_eq!(
            rec.seq, self.next_seq,
            "segment records must be contiguous and ascending"
        );
        let offset = self.data.len() as u32;
        let len = encode_data_frame(&mut self.data, rec);
        let mut flags = 0u8;
        if rec.tag.is_some() {
            flags |= SEG_FLAG_HAS_TAG;
        }
        if rec.node.is_some() {
            flags |= SEG_FLAG_HAS_NODE;
        }
        encode_idx_entry(
            &mut self.idx,
            &IdxEntry {
                offset,
                len,
                ts: rec.ts,
                flags,
            },
        );
        self.next_seq += 1;
    }

    /// First seq covered by this segment (the `seg-<start_seq>` name).
    pub fn start_seq(&self) -> u64 {
        self.start_seq
    }

    /// Last seq covered, or `start_seq - 1` (empty) if nothing was pushed.
    pub fn end_seq(&self) -> u64 {
        self.next_seq.saturating_sub(1)
    }

    /// The seq the next [`Self::push`] must carry (`start_seq` while empty).
    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    /// Number of records pushed so far.
    pub fn record_count(&self) -> usize {
        idx_len(&self.idx)
    }

    /// Current `.data` byte length (for the byte-cap seal trigger).
    pub fn data_len(&self) -> usize {
        self.data.len()
    }

    /// Finish, yielding the `(data, idx)` byte buffers ready to persist.
    pub fn finish(self) -> (Vec<u8>, Vec<u8>) {
        (self.data, self.idx)
    }
}

// ---------------------------------------------------------------------------
// Naming helpers (ARCHITECTURE §6 on-disk layout).
// ---------------------------------------------------------------------------

/// The `.data` file name for a segment whose first seq is `start_seq`
/// (zero-padded so files sort into seq order).
pub fn data_name(start_seq: u64) -> String {
    format!("seg-{start_seq:016}.data")
}

/// The `.idx` file name for a segment whose first seq is `start_seq`.
pub fn idx_name(start_seq: u64) -> String {
    format!("seg-{start_seq:016}.idx")
}

// ===========================================================================
// Unit tests
// ===========================================================================
#[cfg(test)]
mod tests {
    use super::*;

    fn rec(seq: u64, node: Option<&str>, tag: Option<&str>, data: &[u8]) -> SegmentRecord {
        SegmentRecord {
            seq,
            ts: 1_700_000_000_000 + seq,
            node: node.map(str::to_string),
            tag: tag.map(str::to_string),
            data: data.to_vec(),
        }
    }

    #[test]
    fn data_frame_round_trips() {
        for r in [
            rec(1, None, None, b"{\"d\":1}"),
            rec(2, Some("nodeA"), Some("tenant:job"), b"payload"),
            rec(3, Some("n"), None, b""),
            rec(4, None, Some("t"), b"x"),
        ] {
            let mut buf = Vec::new();
            let len = encode_data_frame(&mut buf, &r);
            assert_eq!(len as usize, buf.len(), "framed length matches buffer");
            let got = decode_data_frame(&buf).expect("decodes");
            assert_eq!(got, r, "record round-trips");
        }
    }

    #[test]
    fn idx_entry_round_trips_and_strides() {
        let e = IdxEntry {
            offset: 12345,
            len: 678,
            ts: 1_700_000_000_999,
            flags: SEG_FLAG_HAS_TAG | SEG_FLAG_HAS_NODE,
        };
        let mut buf = Vec::new();
        encode_idx_entry(&mut buf, &e);
        assert_eq!(buf.len(), IDX_STRIDE, "entry is exactly one stride");
        let got = idx_entry_at(&buf, 0).expect("entry 0");
        assert_eq!(got, e);
        assert!(got.has_tag() && got.has_node());
        // Past the end is a clean miss, not a panic.
        assert!(idx_entry_at(&buf, 1).is_none());
    }

    #[test]
    fn builder_produces_dense_indexable_segment() {
        let start = 1001;
        let mut b = SegmentBuilder::new(start);
        let records: Vec<SegmentRecord> = (0..5)
            .map(|i| {
                rec(
                    start + i,
                    (i % 2 == 0).then_some("n"),
                    Some("tag"),
                    b"{\"v\":7}",
                )
            })
            .collect();
        for r in &records {
            b.push(r);
        }
        assert_eq!(b.record_count(), 5);
        assert_eq!(b.start_seq(), start);
        assert_eq!(b.end_seq(), start + 4);
        let (data, idx) = b.finish();
        assert_eq!(idx_len(&idx), 5);

        // Every seq resolves by direct seek, and the framed slice decodes to the
        // original record.
        for r in &records {
            let e = lookup(&idx, start, r.seq).expect("seq present");
            let slice = &data[e.offset as usize..e.offset as usize + e.len as usize];
            let got = decode_data_frame(slice).expect("frame decodes");
            assert_eq!(&got, r);
            assert_eq!(e.ts, r.ts, "inline ts matches");
        }
        // Out-of-range seqs miss cleanly.
        assert!(lookup(&idx, start, start - 1).is_none());
        assert!(lookup(&idx, start, start + 5).is_none());
    }

    #[test]
    fn checksum_catches_corruption() {
        let mut buf = Vec::new();
        encode_data_frame(&mut buf, &rec(42, Some("n"), Some("t"), b"important"));
        // Flip a payload byte: the CRC must reject it.
        let mid = buf.len() / 2;
        buf[mid] ^= 0xFF;
        assert!(
            matches!(decode_data_frame(&buf), Err(SegmentError::Corrupt(_))),
            "corrupted frame must be rejected by the checksum"
        );
    }

    #[test]
    fn truncated_frame_is_rejected() {
        let mut buf = Vec::new();
        encode_data_frame(&mut buf, &rec(1, None, None, b"hello world"));
        // Chop off the trailing CRC: frame_len now overruns the slice.
        buf.truncate(buf.len() - 4);
        assert!(matches!(
            decode_data_frame(&buf),
            Err(SegmentError::Corrupt(_))
        ));
    }

    #[test]
    fn names_sort_in_seq_order() {
        assert_eq!(data_name(1), "seg-0000000000000001.data");
        assert_eq!(idx_name(10001), "seg-0000000000010001.idx");
        assert!(data_name(1) < data_name(10001), "names sort by seq");
    }

    #[test]
    fn fresh_frame_is_live_and_decodes() {
        let mut buf = Vec::new();
        let len = encode_data_frame(&mut buf, &rec(7, Some("n"), Some("t"), b"x"));
        assert_eq!(len as usize, buf.len());
        // A fresh frame's trailing del_flag is LIVE (the sentinel is absent).
        assert!(!frame_is_deleted(&buf), "fresh frame is live");
        assert_eq!(*buf.last().unwrap(), SEG_DEL_LIVE);
        // It still decodes (the del_flag is outside the body the CRC covers).
        let got = decode_data_frame(&buf).expect("decodes");
        assert_eq!(got.seq, 7);
    }

    #[test]
    fn flipping_del_flag_marks_deleted_without_breaking_decode() {
        let mut buf = Vec::new();
        let len = encode_data_frame(&mut buf, &rec(9, None, None, b"hello"));
        // The del_flag is the last byte of the framed slice.
        let off = del_flag_offset_in_frame(len) as usize;
        assert_eq!(off, buf.len() - 1, "del_flag is the trailing byte");
        // Flip it to the sentinel in place — exactly what mark_record_deleted does.
        buf[off] = SEG_DEL_SENTINEL;
        assert!(frame_is_deleted(&buf), "flipped frame reads deleted");
        // The CRC is unchanged (it never covered the del_flag), so the record body
        // still decodes — the flip never corrupts framing.
        let got = decode_data_frame(&buf).expect("still decodes after the flip");
        assert_eq!(got.seq, 9);
        assert_eq!(got.data, b"hello");
    }

    #[test]
    fn torn_flip_intermediate_value_reads_live() {
        // A torn single-byte flip that left an arbitrary intermediate value (NOT the
        // exact sentinel) reads as LIVE — never resurrects/skips, never corrupts.
        let mut buf = Vec::new();
        let len = encode_data_frame(&mut buf, &rec(3, None, None, b"z"));
        let off = del_flag_offset_in_frame(len) as usize;
        for partial in [0x01u8, 0x55, 0xD4, 0xFF, 0x7F] {
            buf[off] = partial;
            assert!(
                !frame_is_deleted(&buf),
                "intermediate del_flag {partial:#x} reads live (torn-flip safe)"
            );
            assert!(
                decode_data_frame(&buf).is_ok(),
                "frame still decodes with a partial del_flag {partial:#x}"
            );
        }
        // Only the exact sentinel means deleted.
        buf[off] = SEG_DEL_SENTINEL;
        assert!(frame_is_deleted(&buf));
    }

    #[test]
    fn builder_frames_carry_a_live_del_flag() {
        let start = 100;
        let mut b = SegmentBuilder::new(start);
        for i in 0..3 {
            b.push(&rec(start + i, None, Some("t"), b"v"));
        }
        let (data, idx) = b.finish();
        for seq in start..start + 3 {
            let e = lookup(&idx, start, seq).expect("seq present");
            let lo = e.offset as usize;
            let hi = lo + e.len as usize;
            assert!(!frame_is_deleted(&data[lo..hi]), "builder frame is live");
            assert_eq!(
                del_flag_offset_in_frame(e.len) as usize,
                e.len as usize - 1,
                "del_flag offset is the trailing byte of the framed len"
            );
        }
    }

    #[test]
    fn corrupt_range_never_reads_deleted() {
        // A garbled framed slice must never be reported deleted (it cannot fabricate
        // a deletion that skips a live record on the recovery scan). codex P1 #3.
        let mut buf = Vec::new();
        encode_data_frame(&mut buf, &rec(42, Some("n"), Some("t"), b"important"));
        // Corrupt the body: the current-layout CRC no longer validates ⇒ not deleted.
        let mid = buf.len() / 2;
        buf[mid] ^= 0xFF;
        assert!(!frame_is_deleted(&buf), "corrupt frame is never deleted");
        // And it surfaces as Corrupt on a decode (the caller handles it).
        assert!(matches!(
            decode_data_frame(&buf),
            Err(SegmentError::Corrupt(_))
        ));
    }
}
