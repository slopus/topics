//! Sealed/active **segment** file format: the long-term, per-box materialization
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
//! ```
//!
//! `frame_len` first lets a reader validate a frame boundary without parsing the
//! body; the trailing XXH3-64 over everything between `frame_len` and the checksum
//! detects bit-rot / a torn write (the same crash anchor as the WAL, §2.1). A
//! mismatch on a *sealed* (immutable) segment is corruption, not a torn tail — the
//! caller surfaces it rather than silently truncating.
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

    // CRC over [4 .. crc_start) (the body, excluding the frame_len prefix).
    let crc_val = crc(&out[frame_start + SEG_FRAME_LEN_PREFIX..]);
    out.extend_from_slice(&crc_val.to_le_bytes());

    // frame_len = everything after the prefix = (total - 4).
    let frame_len = (out.len() - frame_start - SEG_FRAME_LEN_PREFIX) as u32;
    out[frame_start..frame_start + SEG_FRAME_LEN_PREFIX]
        .copy_from_slice(&frame_len.to_le_bytes());

    (out.len() - frame_start) as u32
}

/// Decode exactly one `.data` frame from the start of `buf` (a slice carved out
/// of the `.data` file by the `.idx` offset/len). Validates `frame_len`, the
/// XXH3, and the internal section lengths; a mismatch is corruption (a sealed
/// segment is immutable, so there is no torn-tail truncation — the caller
/// surfaces the error).
pub fn decode_data_frame(buf: &[u8]) -> Result<SegmentRecord, SegmentError> {
    if buf.len() < SEG_FRAME_LEN_PREFIX {
        return Err(SegmentError::Corrupt("shorter than frame_len prefix".into()));
    }
    let frame_len = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
    if frame_len < SEG_FRAME_HEADER_LEN + SEG_FRAME_CRC_LEN {
        return Err(SegmentError::Corrupt("frame_len below the minimum".into()));
    }
    let total = SEG_FRAME_LEN_PREFIX + frame_len;
    if buf.len() < total {
        return Err(SegmentError::Corrupt("frame_len overruns the slice".into()));
    }

    // CRC over [4 .. crc_start); the checksum is the last 8 bytes.
    let crc_start = total - SEG_FRAME_CRC_LEN;
    let stored_crc = u64::from_le_bytes(buf[crc_start..total].try_into().unwrap());
    if stored_crc != crc(&buf[SEG_FRAME_LEN_PREFIX..crc_start]) {
        return Err(SegmentError::Corrupt("checksum mismatch".into()));
    }

    let h = &buf[SEG_FRAME_LEN_PREFIX..];
    let flags = h[0];
    let seq = u64::from_le_bytes(h[1..9].try_into().unwrap());
    let ts = u64::from_le_bytes(h[9..17].try_into().unwrap());
    let node_len = u16::from_le_bytes(h[17..19].try_into().unwrap()) as usize;
    let tag_len = u16::from_le_bytes(h[19..21].try_into().unwrap()) as usize;
    let data_len = u32::from_le_bytes(h[21..25].try_into().unwrap()) as usize;

    let body = &h[SEG_FRAME_HEADER_LEN..crc_start - SEG_FRAME_LEN_PREFIX];
    if node_len + tag_len + data_len != body.len() {
        return Err(SegmentError::Corrupt("internal length inconsistency".into()));
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
/// starting at `start_seq` (segments mirror the append order of a box).
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
            .map(|i| rec(start + i, (i % 2 == 0).then_some("n"), Some("tag"), b"{\"v\":7}"))
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
}
