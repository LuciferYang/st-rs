//! Port of `db/log_format.h`.
//!
//! On-disk framing for the write-ahead log. Records are stored in
//! fixed 32 KiB blocks, each containing one or more records or
//! record fragments. Every record carries a header with a CRC, a
//! length, and a type telling the reader whether the record is
//! complete (`Full`), the start of a multi-block record (`First`),
//! a middle fragment (`Middle`), or the end (`Last`). This makes
//! crash recovery resilient: a torn block is detected and skipped
//! without losing earlier records.
//!
//! Wire format of a record header (7 bytes):
//!
//! ```text
//!   +--------------+----------+------+
//!   | masked_crc32c| length   | type |
//!   +--------------+----------+------+
//!         4              2       1     bytes  (LE)
//! ```
//!
//! The CRC is computed over `[type byte][payload bytes]` and then
//! passed through [`crate::util::crc32c::mask`] before storage —
//! the same masking trick the SST block trailer uses.
//!
//! This format is **wire-exact** with upstream `db/log_format.h`.

/// Size of a single WAL block. Records that don't fit in the
/// remaining space of the current block roll over with a `First` /
/// `Middle` / `Last` chain.
pub const BLOCK_SIZE: usize = 32 * 1024;

/// Size of the per-record header in bytes.
pub const HEADER_SIZE: usize = 4 + 2 + 1;

/// Record kind. The numeric values match upstream and are part of
/// the on-disk format — do not renumber.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum RecordType {
    /// Padding bytes at the end of a block, written when the
    /// remaining space can't fit a full header.
    Zero = 0,
    /// A complete record contained in one block.
    Full = 1,
    /// First fragment of a multi-block record.
    First = 2,
    /// Middle fragment.
    Middle = 3,
    /// Last fragment.
    Last = 4,
}

impl RecordType {
    /// Convert a wire byte to a `RecordType`. Returns `None` for
    /// unknown values, which the reader treats as a corrupt header.
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            0 => Some(Self::Zero),
            1 => Some(Self::Full),
            2 => Some(Self::First),
            3 => Some(Self::Middle),
            4 => Some(Self::Last),
            _ => None,
        }
    }
}
