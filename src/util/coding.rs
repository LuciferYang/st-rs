//! Port of `util/coding.h` + `util/coding.cc`.
//!
//! Integer and length-prefixed-slice encoding helpers. This is the
//! rosetta stone of the engine's on-disk format: every SST block, every
//! manifest record, every WAL entry passes through one of these routines.
//! Wire-format compatibility with upstream is **mandatory**:
//!
//! - Fixed-width integers are **little-endian**.
//! - Varints are LEB128: 7 bits of payload per byte, high bit set on all
//!   but the last byte. Max 5 bytes for `u32`, 10 bytes for `u64`.
//! - Length-prefixed slices are encoded as `varint32(len) || bytes`.
//!
//! The Rust API differs slightly from upstream: where C++ uses in/out
//! string pointers, we use `Vec<u8>` appenders (`encode_*_into`) and
//! `(value, rest)` tuple decoders. This is safer and composes better with
//! the standard library.

use crate::core::status::{Result, Status};

// ---------------------------------------------------------------------------
// Fixed-width little-endian
// ---------------------------------------------------------------------------

/// Append `value` as 4 little-endian bytes to `dst`.
#[inline]
pub fn put_fixed32(dst: &mut Vec<u8>, value: u32) {
    dst.extend_from_slice(&value.to_le_bytes());
}

/// Append `value` as 8 little-endian bytes to `dst`.
#[inline]
pub fn put_fixed64(dst: &mut Vec<u8>, value: u64) {
    dst.extend_from_slice(&value.to_le_bytes());
}

/// Decode a little-endian `u32` from the first 4 bytes of `src`.
///
/// Returns `InvalidArgument` if `src.len() < 4`.
#[inline]
pub fn decode_fixed32(src: &[u8]) -> Result<u32> {
    if src.len() < 4 {
        return Err(Status::invalid_argument("decode_fixed32: short input"));
    }
    Ok(u32::from_le_bytes([src[0], src[1], src[2], src[3]]))
}

/// Decode a little-endian `u64` from the first 8 bytes of `src`.
#[inline]
pub fn decode_fixed64(src: &[u8]) -> Result<u64> {
    if src.len() < 8 {
        return Err(Status::invalid_argument("decode_fixed64: short input"));
    }
    Ok(u64::from_le_bytes([
        src[0], src[1], src[2], src[3], src[4], src[5], src[6], src[7],
    ]))
}

// ---------------------------------------------------------------------------
// Varint (LEB128)
// ---------------------------------------------------------------------------

/// Maximum number of bytes a `u32` varint may consume. Mirrors
/// `kMaxVarint32Length`.
pub const MAX_VARINT32_LENGTH: usize = 5;

/// Maximum number of bytes a `u64` varint may consume. Mirrors
/// `kMaxVarint64Length`.
pub const MAX_VARINT64_LENGTH: usize = 10;

/// Append a varint-encoded `u32` to `dst`.
pub fn put_varint32(dst: &mut Vec<u8>, mut value: u32) {
    while value >= 0x80 {
        dst.push((value as u8) | 0x80);
        value >>= 7;
    }
    dst.push(value as u8);
}

/// Append a varint-encoded `u64` to `dst`.
pub fn put_varint64(dst: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        dst.push((value as u8) | 0x80);
        value >>= 7;
    }
    dst.push(value as u8);
}

/// Decode a varint-encoded `u32` from the front of `src`. Returns
/// `(value, rest)` on success where `rest` points at the byte *after* the
/// varint. Returns `Corruption` if the varint runs off the end of the
/// input, or `InvalidArgument` if the value overflows 32 bits.
pub fn get_varint32(src: &[u8]) -> Result<(u32, &[u8])> {
    let mut result: u32 = 0;
    for (i, &byte) in src.iter().enumerate().take(MAX_VARINT32_LENGTH) {
        let v = (byte & 0x7f) as u32;
        let shift = 7 * i as u32;
        // Check that shifting `v` left by `shift` fits in a u32. The
        // first 4 bytes are always fine (shift <= 21); on the 5th byte
        // we have shift == 28, so `v` must fit in 4 bits.
        if i == 4 && v > 0x0f {
            return Err(Status::invalid_argument("get_varint32: overflow"));
        }
        result |= v << shift;
        if byte & 0x80 == 0 {
            return Ok((result, &src[i + 1..]));
        }
    }
    Err(Status::corruption("get_varint32: truncated"))
}

/// Decode a varint-encoded `u64` from the front of `src`.
pub fn get_varint64(src: &[u8]) -> Result<(u64, &[u8])> {
    let mut result: u64 = 0;
    for (i, &byte) in src.iter().enumerate().take(MAX_VARINT64_LENGTH) {
        let v = (byte & 0x7f) as u64;
        let shift = 7 * i as u32;
        if i == 9 && v > 0x01 {
            return Err(Status::invalid_argument("get_varint64: overflow"));
        }
        result |= v << shift;
        if byte & 0x80 == 0 {
            return Ok((result, &src[i + 1..]));
        }
    }
    Err(Status::corruption("get_varint64: truncated"))
}

/// Return the number of bytes required to varint-encode `value`. Matches
/// upstream `VarintLength`.
pub fn varint_length(mut value: u64) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

// ---------------------------------------------------------------------------
// Length-prefixed slice
// ---------------------------------------------------------------------------

/// Append `slice` to `dst` prefixed by a varint length header. Matches
/// upstream `PutLengthPrefixedSlice`.
pub fn put_length_prefixed_slice(dst: &mut Vec<u8>, slice: &[u8]) {
    put_varint32(dst, slice.len() as u32);
    dst.extend_from_slice(slice);
}

/// Decode a length-prefixed slice from the front of `src`. Returns
/// `(slice, rest)`.
pub fn get_length_prefixed_slice(src: &[u8]) -> Result<(&[u8], &[u8])> {
    let (len, rest) = get_varint32(src)?;
    let len = len as usize;
    if rest.len() < len {
        return Err(Status::corruption(
            "get_length_prefixed_slice: payload shorter than declared length",
        ));
    }
    Ok((&rest[..len], &rest[len..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed32_round_trip() {
        let mut buf = Vec::new();
        put_fixed32(&mut buf, 0x12345678);
        assert_eq!(buf, [0x78, 0x56, 0x34, 0x12]);
        assert_eq!(decode_fixed32(&buf).unwrap(), 0x12345678);
    }

    #[test]
    fn fixed64_round_trip() {
        let mut buf = Vec::new();
        put_fixed64(&mut buf, 0x0123456789abcdef);
        assert_eq!(decode_fixed64(&buf).unwrap(), 0x0123456789abcdef);
    }

    #[test]
    fn fixed_decode_rejects_short_input() {
        assert!(decode_fixed32(b"abc").unwrap_err().is_invalid_argument());
        assert!(decode_fixed64(b"1234567").unwrap_err().is_invalid_argument());
    }

    #[test]
    fn varint32_round_trip_edge_values() {
        for v in [0u32, 1, 127, 128, 16383, 16384, u32::MAX] {
            let mut buf = Vec::new();
            put_varint32(&mut buf, v);
            let (decoded, rest) = get_varint32(&buf).unwrap();
            assert_eq!(decoded, v, "round trip for {v}");
            assert!(rest.is_empty());
            assert_eq!(buf.len(), varint_length(v as u64));
        }
    }

    #[test]
    fn varint64_round_trip_edge_values() {
        for v in [0u64, 1, 127, 128, u64::MAX / 2, u64::MAX] {
            let mut buf = Vec::new();
            put_varint64(&mut buf, v);
            let (decoded, rest) = get_varint64(&buf).unwrap();
            assert_eq!(decoded, v);
            assert!(rest.is_empty());
        }
    }

    #[test]
    fn varint_length_matches_actual_encoding() {
        for v in [0u64, 0x7f, 0x80, 0x3fff, 0x4000, u64::MAX] {
            let mut buf = Vec::new();
            put_varint64(&mut buf, v);
            assert_eq!(varint_length(v), buf.len(), "length for {v}");
        }
    }

    #[test]
    fn varint32_rejects_overflow() {
        // 5-byte varint with top nibble bits set beyond 32-bit range.
        let buf = [0xff, 0xff, 0xff, 0xff, 0x7f];
        assert!(get_varint32(&buf).is_err());
    }

    #[test]
    fn varint_rejects_truncated_input() {
        // All continuation bits set — never terminates.
        let buf = [0x80, 0x80];
        assert!(get_varint32(&buf).unwrap_err().is_corruption());
    }

    #[test]
    fn length_prefixed_slice_round_trip() {
        let payload = b"hello world";
        let mut buf = Vec::new();
        put_length_prefixed_slice(&mut buf, payload);
        let (decoded, rest) = get_length_prefixed_slice(&buf).unwrap();
        assert_eq!(decoded, payload);
        assert!(rest.is_empty());
    }

    #[test]
    fn length_prefixed_slice_rejects_short_payload() {
        // Varint 5 followed by only 3 bytes.
        let buf = [0x05, b'a', b'b', b'c'];
        assert!(get_length_prefixed_slice(&buf).unwrap_err().is_corruption());
    }

    #[test]
    fn chained_decode_produces_expected_remainder() {
        let mut buf = Vec::new();
        put_varint32(&mut buf, 42);
        put_fixed64(&mut buf, 0xdead_beef);
        let (a, rest) = get_varint32(&buf).unwrap();
        assert_eq!(a, 42);
        assert_eq!(decode_fixed64(rest).unwrap(), 0xdead_beef);
    }
}
