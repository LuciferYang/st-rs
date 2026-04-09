//! Port of `util/crc32c.{h,cc}`.
//!
//! CRC-32C (Castagnoli) — the checksum RocksDB uses in the block
//! trailer of every SST. Unlike the FNV-1a placeholder that Layer 3a
//! shipped in `sst::format`, this implementation is **wire-exact**:
//! a block written by `st-rs` produces the same 32-bit CRC as a block
//! written by upstream RocksDB for the same bytes.
//!
//! # Algorithm
//!
//! Table-based Sarwate algorithm: 1 KiB of precomputed table, one
//! XOR + table lookup per byte. This is O(n) and ~10× slower than
//! the SSE 4.2 `crc32c` intrinsic upstream uses on modern x86, but:
//!
//! - It's portable (runs on any target, including aarch64).
//! - It produces identical output.
//! - It's ~30 lines and auditable.
//!
//! A follow-up can switch to slice-by-8 or the hardware intrinsic
//! behind a `cfg(target_feature = "sse4.2")` fast path.
//!
//! # The "masked" form
//!
//! RocksDB's block trailer doesn't store the raw CRC — it stores a
//! masked form to avoid pathological inputs that could produce
//! all-zero CRCs. The mask is:
//!
//! ```text
//!   masked = ((crc >> 15) | (crc << 17)) + 0xa282ead8
//! ```
//!
//! and the inverse is applied on read. Always use [`mask`] before
//! writing and [`unmask`] after reading — never store the raw CRC
//! inside an SST.

/// The Castagnoli polynomial in reversed form. `0x1EDC6F41` →
/// `0x82F63B78` when you reverse the bits.
const POLY: u32 = 0x82F6_3B78;

/// Precomputed byte-lookup table. `TABLE[b]` is the CRC of a
/// single-byte stream of value `b` with initial state `0`.
const TABLE: [u32; 256] = {
    let mut table = [0u32; 256];
    let mut i = 0usize;
    while i < 256 {
        let mut crc = i as u32;
        let mut j = 0;
        while j < 8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ POLY
            } else {
                crc >> 1
            };
            j += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
};

/// Compute the CRC-32C of `data`. Equivalent to `crc32c_extend(0, data)`.
#[inline]
pub fn crc32c(data: &[u8]) -> u32 {
    crc32c_extend(0, data)
}

/// Extend a running CRC with more data. Used when the input is
/// processed in chunks (e.g. the block trailer computes one CRC over
/// `block_bytes || compression_byte`).
pub fn crc32c_extend(init: u32, data: &[u8]) -> u32 {
    let mut crc = !init;
    for &b in data {
        let idx = ((crc ^ b as u32) & 0xff) as usize;
        crc = (crc >> 8) ^ TABLE[idx];
    }
    !crc
}

/// The "mask delta" from upstream `crc32c.h`.
const MASK_DELTA: u32 = 0xa282_ead8;

/// Apply the leveldb/RocksDB CRC mask. Always call before writing
/// a CRC into a block trailer.
#[inline]
pub fn mask(crc: u32) -> u32 {
    // 15-bit right rotate + offset.
    crc.rotate_right(15).wrapping_add(MASK_DELTA)
}

/// Invert [`mask`]. Always call after reading a CRC from a block
/// trailer, then compare the result against a freshly-computed
/// `crc32c` over the block bytes + compression byte.
#[inline]
pub fn unmask(masked: u32) -> u32 {
    masked.wrapping_sub(MASK_DELTA).rotate_left(15)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Reference values verified against Python's `crc32c.crc32c`
    // (the `crc32c` pip package) and upstream RocksDB's
    // `util/crc32c_test.cc`.

    #[test]
    fn empty_input() {
        assert_eq!(crc32c(b""), 0);
    }

    #[test]
    fn single_byte() {
        assert_eq!(crc32c(b"a"), 0xc1d0_4330);
    }

    #[test]
    fn classic_reference_string() {
        // "123456789" is the standard CRC-32C check value from the
        // Rocksoft test vectors.
        assert_eq!(crc32c(b"123456789"), 0xe306_9283);
    }

    #[test]
    fn hello_world() {
        // Verified against the `crc32c` Python package.
        assert_eq!(crc32c(b"hello world"), 0xc994_65aa);
    }

    #[test]
    fn extend_is_equivalent_to_single_call() {
        let full = crc32c(b"hello world");
        let half = crc32c_extend(0, b"hello ");
        let stitched = crc32c_extend(half, b"world");
        assert_eq!(full, stitched);
    }

    #[test]
    fn extend_on_empty_is_identity() {
        let c = crc32c(b"abc");
        assert_eq!(crc32c_extend(c, b""), c);
    }

    #[test]
    fn mask_and_unmask_round_trip() {
        for crc in [0u32, 1, 0xdead_beef, 0xffff_ffff, 0x8000_0000] {
            assert_eq!(unmask(mask(crc)), crc);
        }
    }

    #[test]
    fn masked_form_is_not_identity() {
        // The whole point of the mask is that `masked != raw`.
        assert_ne!(mask(0xdead_beef), 0xdead_beef);
        // And zero doesn't stay zero (avoids the "all-zero" pathology).
        assert_ne!(mask(0), 0);
    }

    #[test]
    fn large_buffer_matches_piecewise() {
        // Compute on a 10 KiB buffer both in one call and byte-by-byte.
        let data: Vec<u8> = (0..10_000).map(|i| (i % 251) as u8).collect();
        let full = crc32c(&data);
        let mut piecewise = 0;
        for b in &data {
            piecewise = crc32c_extend(piecewise, std::slice::from_ref(b));
        }
        assert_eq!(full, piecewise);
    }
}
