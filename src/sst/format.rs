//! Port of `table/format.{h,cc}`.
//!
//! Defines the low-level structures that sit at the bottom of every
//! SST file:
//!
//! - [`BlockHandle`] — a `(offset, size)` pair pointing at a block
//!   inside the file. Varint-encoded on disk.
//! - [`Footer`] — the fixed-size structure at the end of every SST
//!   that points at the metaindex block and the index block, plus
//!   a magic number for format identification.
//!
//! # On-disk layout of an SST file
//!
//! ```text
//!   +-----------------------+
//!   |  data block 0         |
//!   +-----------------------+
//!   |  data block 1         |
//!   +-----------------------+
//!   |          ...          |
//!   +-----------------------+
//!   |  data block N-1       |
//!   +-----------------------+
//!   |  metaindex block      |  ← contains filter block handle, etc.
//!   +-----------------------+
//!   |  index block          |  ← one entry per data block
//!   +-----------------------+
//!   |  Footer               |  ← fixed size
//!   +-----------------------+
//! ```
//!
//! The footer's position (last 53 bytes of the file in the legacy
//! format) lets a reader open any SST without knowing its size in
//! advance — you just mmap the tail.
//!
//! # Block trailer
//!
//! Each block (data, metaindex, index) is followed by a 5-byte
//! trailer on disk: `[compression_type: u8][checksum: u32 LE]`. The
//! checksum covers the block bytes plus the compression type byte.
//! Layer 3a ports the trailer constants but leaves compression and
//! checksum computation to Layer 3b, when they can be round-tripped
//! end-to-end.

use crate::core::status::{Result, Status};
use crate::util::coding::{
    decode_fixed32, get_varint64, put_fixed32, put_fixed64, put_varint64, MAX_VARINT64_LENGTH,
};

/// Size of the per-block trailer on disk: 1 byte compression type +
/// 4 bytes CRC32C. Matches upstream `kBlockTrailerSize`.
pub const BLOCK_TRAILER_SIZE: usize = 5;

/// Legacy magic number at the very end of every SST file.
/// Matches upstream `kBlockBasedTableMagicNumber` on little-endian
/// disk layouts. This is the canonical SST version 0 magic.
pub const BLOCK_BASED_TABLE_MAGIC_NUMBER: u64 = 0x88e2_41b7_85f4_cff7;

/// Current SST format version written by the builder.
pub const DEFAULT_FORMAT_VERSION: u32 = 5;

/// Maximum encoded length of a [`BlockHandle`] in bytes.
/// Two varint64s, each up to 10 bytes.
pub const MAX_BLOCK_HANDLE_ENCODED_LENGTH: usize = MAX_VARINT64_LENGTH * 2;

// ---------------------------------------------------------------------------
// BlockHandle
// ---------------------------------------------------------------------------

/// Offset + size of a block inside an SST file. Encoded as two
/// consecutive varint64s.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BlockHandle {
    /// Byte offset of the block within the SST file.
    pub offset: u64,
    /// Block size in bytes, **excluding** the trailing
    /// `BLOCK_TRAILER_SIZE` bytes.
    pub size: u64,
}

impl BlockHandle {
    /// Sentinel "null" block handle, encoded as offset=0, size=0.
    /// Used when a handle is optional (e.g. no filter block).
    pub const NULL: BlockHandle = BlockHandle { offset: 0, size: 0 };

    /// Construct a new handle.
    pub const fn new(offset: u64, size: u64) -> Self {
        Self { offset, size }
    }

    /// Is this the null/unset handle?
    pub const fn is_null(&self) -> bool {
        self.offset == 0 && self.size == 0
    }

    /// Append the encoded form (two varint64s) to `dst`. Matches
    /// upstream `BlockHandle::EncodeTo`.
    pub fn encode_to(&self, dst: &mut Vec<u8>) {
        put_varint64(dst, self.offset);
        put_varint64(dst, self.size);
    }

    /// Decode a `BlockHandle` from the front of `src`. Returns the
    /// handle and the remainder of the input. Matches upstream
    /// `BlockHandle::DecodeFrom`.
    pub fn decode_from(src: &[u8]) -> Result<(Self, &[u8])> {
        let (offset, rest) = get_varint64(src)
            .map_err(|_| Status::corruption("BlockHandle: bad offset varint"))?;
        let (size, rest) = get_varint64(rest)
            .map_err(|_| Status::corruption("BlockHandle: bad size varint"))?;
        Ok((Self { offset, size }, rest))
    }
}

// ---------------------------------------------------------------------------
// Footer
// ---------------------------------------------------------------------------

/// Fixed-size footer written at the end of an SST file.
///
/// **Layout** (matches upstream's legacy format that the Layer 2 port
/// used for `format_version == 0`):
///
/// ```text
///   +-----------------------------------------------+
///   | metaindex_handle (up to MAX_BLOCK_HANDLE...)  |
///   | index_handle     (up to MAX_BLOCK_HANDLE...)  |
///   | zero padding to 40 bytes                      |
///   | magic number (fixed64, LE)                    |
///   +-----------------------------------------------+
/// ```
///
/// Total footer size is [`Footer::ENCODED_LENGTH`] = 48 bytes: 40
/// bytes for two varint64-encoded block handles (worst case) plus
/// 8 bytes for the magic number. Shorter encodings are zero-padded
/// so the footer always lands at exactly 48 bytes before EOF.
///
/// Upstream has a newer "extended footer" that adds a version byte
/// and a checksum type — we'll add it in Layer 3b together with the
/// compression byte; at Layer 3a we only need the legacy form.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Footer {
    /// Handle pointing at the metaindex block.
    pub metaindex_handle: BlockHandle,
    /// Handle pointing at the top-level index block.
    pub index_handle: BlockHandle,
}

impl Footer {
    /// Encoded length of the footer in bytes. Matches upstream
    /// `Footer::kEncodedLength` (legacy format).
    pub const ENCODED_LENGTH: usize = 2 * MAX_BLOCK_HANDLE_ENCODED_LENGTH + 8;

    /// Create a footer from the two handles.
    pub const fn new(metaindex_handle: BlockHandle, index_handle: BlockHandle) -> Self {
        Self {
            metaindex_handle,
            index_handle,
        }
    }

    /// Append the encoded footer to `dst`. Always writes exactly
    /// [`Self::ENCODED_LENGTH`] bytes.
    pub fn encode_to(&self, dst: &mut Vec<u8>) {
        let start = dst.len();
        self.metaindex_handle.encode_to(dst);
        self.index_handle.encode_to(dst);
        // Zero-pad to `2 * MAX_BLOCK_HANDLE_ENCODED_LENGTH` bytes so
        // the magic number always lands at the same offset.
        let handles_end = start + 2 * MAX_BLOCK_HANDLE_ENCODED_LENGTH;
        while dst.len() < handles_end {
            dst.push(0);
        }
        // Magic number (8 bytes LE).
        put_fixed64(dst, BLOCK_BASED_TABLE_MAGIC_NUMBER);
        debug_assert_eq!(dst.len() - start, Self::ENCODED_LENGTH);
    }

    /// Decode a footer from `src`. Requires `src.len() >=
    /// ENCODED_LENGTH`; extra leading bytes are ignored by treating
    /// only the last `ENCODED_LENGTH` bytes as the footer. Matches
    /// upstream's "open at end of file" pattern.
    pub fn decode_from(src: &[u8]) -> Result<Self> {
        if src.len() < Self::ENCODED_LENGTH {
            return Err(Status::corruption(format!(
                "footer requires {} bytes, got {}",
                Self::ENCODED_LENGTH,
                src.len()
            )));
        }
        let footer_start = src.len() - Self::ENCODED_LENGTH;
        let footer = &src[footer_start..];

        // Verify magic number in the last 8 bytes.
        let magic_offset = footer.len() - 8;
        let magic_lo = decode_fixed32(&footer[magic_offset..])?;
        let magic_hi = decode_fixed32(&footer[magic_offset + 4..])?;
        let magic = (magic_hi as u64) << 32 | magic_lo as u64;
        if magic != BLOCK_BASED_TABLE_MAGIC_NUMBER {
            return Err(Status::corruption(format!(
                "bad table magic number: {magic:#018x}"
            )));
        }

        // Decode handles from the front of the footer. Each varint64
        // consumes at most `MAX_VARINT64_LENGTH` bytes; the remainder
        // (up to the magic number) is zero padding.
        let (metaindex_handle, rest) = BlockHandle::decode_from(footer)?;
        let (index_handle, _rest) = BlockHandle::decode_from(rest)?;
        Ok(Self {
            metaindex_handle,
            index_handle,
        })
    }
}

/// Compute the CRC32C-like checksum of a block for the block trailer.
/// Layer 3a uses `crate::util::hash::hash` as a placeholder — **not**
/// wire-compatible with upstream's `kCRC32c`. Layer 3b will swap in
/// a real CRC32C once it lands in `util::hash`.
#[allow(dead_code)]
pub(crate) fn block_checksum_placeholder(data: &[u8], compression_byte: u8) -> u32 {
    // Feed the data + compression byte through the Layer 1 hash.
    // This isn't the right algorithm — it's just here so the
    // trailer-writing path has *something* to write — but every
    // caller will round-trip through the same function, so
    // intra-crate round trips still work.
    let seed = compression_byte as u32;
    crate::util::hash::hash(data, seed)
}

/// Helper used by builders to emit a five-byte block trailer onto
/// the output buffer. Writes `[compression: u8][checksum: u32 LE]`.
pub fn put_block_trailer(dst: &mut Vec<u8>, compression_type: u8, block_bytes: &[u8]) {
    dst.push(compression_type);
    let checksum = block_checksum_placeholder(block_bytes, compression_type);
    put_fixed32(dst, checksum);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_handle_round_trip() {
        let h = BlockHandle::new(0x1234_5678, 0xabcd);
        let mut buf = Vec::new();
        h.encode_to(&mut buf);
        let (decoded, rest) = BlockHandle::decode_from(&buf).unwrap();
        assert_eq!(decoded, h);
        assert!(rest.is_empty());
    }

    #[test]
    fn block_handle_null_round_trip() {
        let h = BlockHandle::NULL;
        let mut buf = Vec::new();
        h.encode_to(&mut buf);
        let (decoded, _) = BlockHandle::decode_from(&buf).unwrap();
        assert_eq!(decoded, h);
        assert!(decoded.is_null());
    }

    #[test]
    fn footer_round_trip() {
        let f = Footer::new(
            BlockHandle::new(100, 200),
            BlockHandle::new(300, 400),
        );
        let mut buf = Vec::new();
        f.encode_to(&mut buf);
        assert_eq!(buf.len(), Footer::ENCODED_LENGTH);
        let decoded = Footer::decode_from(&buf).unwrap();
        assert_eq!(decoded, f);
    }

    #[test]
    fn footer_decodes_from_tail_of_larger_buffer() {
        // Simulating "read last 48 bytes of an SST file."
        let f = Footer::new(
            BlockHandle::new(1, 2),
            BlockHandle::new(3, 4),
        );
        let mut buf = vec![0xaa; 1000]; // dummy SST body
        f.encode_to(&mut buf);
        let decoded = Footer::decode_from(&buf).unwrap();
        assert_eq!(decoded, f);
    }

    #[test]
    fn footer_rejects_short_input() {
        assert!(Footer::decode_from(&[0u8; 10]).unwrap_err().is_corruption());
    }

    #[test]
    fn footer_rejects_bad_magic() {
        let mut buf = vec![0u8; Footer::ENCODED_LENGTH];
        // Tail = zero, magic = 0, not the expected value.
        assert!(Footer::decode_from(&buf).unwrap_err().is_corruption());
        // Also verify that a random tail is rejected.
        for b in buf.iter_mut() {
            *b = 0xff;
        }
        assert!(Footer::decode_from(&buf).unwrap_err().is_corruption());
    }

    #[test]
    fn block_trailer_is_five_bytes() {
        let mut dst = Vec::new();
        put_block_trailer(&mut dst, 0, b"hello block");
        assert_eq!(dst.len(), BLOCK_TRAILER_SIZE);
        assert_eq!(dst[0], 0); // compression type
    }
}
