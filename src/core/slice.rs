// Copyright 2025 The st-rs Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Port of `include/rocksdb/slice.h`.
//!
//! RocksDB's `Slice` is a `(const char*, size_t)` borrowed view of bytes. In
//! Rust, the idiomatic counterpart is simply `&[u8]` — it has the same
//! representation, the same lifetime semantics, and the same zero-cost
//! comparison helpers (`cmp`, `starts_with`, `ends_with`).
//!
//! So instead of defining a newtype, we expose [`Slice`] as a type alias and
//! add a [`SliceExt`] trait for the two upstream helpers that are *not* in
//! `core`: [`SliceExt::difference_offset`] and a byte-level [`SliceExt::to_hex`].
//!
//! `PinnableSlice` from upstream exists so the block cache can hand out a
//! reference into a cached block that must be released on drop. In Rust that
//! role is played by a separate type that couples `&[u8]` with a drop guard
//! — it belongs in the cache layer (Layer 2), not Layer 0. It is intentionally
//! omitted here.

/// Borrowed view of a byte sequence.
///
/// This is the direct equivalent of RocksDB's `rocksdb::Slice`. When a method
/// takes a `Slice<'_>` it is shorthand for "this function reads bytes but does
/// not take ownership; the caller must keep the backing storage alive for the
/// duration of the call."
pub type Slice<'a> = &'a [u8];

/// Extension helpers matching the upstream `Slice` methods that are not
/// already provided by `core::slice` / `core::cmp`.
///
/// These are provided on `[u8]` so you can call them on anything that derefs
/// to a byte slice (`&[u8]`, `Vec<u8>`, `String::as_bytes()`, etc.).
pub trait SliceExt {
    /// Returns the offset of the first byte at which `self` and `other`
    /// differ. If one is a prefix of the other, returns the length of the
    /// shorter. Matches `Slice::difference_offset`.
    fn difference_offset(&self, other: &[u8]) -> usize;

    /// Returns an uppercase hex-encoded `String` of the slice contents.
    /// Matches `Slice::ToString(hex=true)`.
    fn to_hex_string(&self) -> String;
}

impl SliceExt for [u8] {
    fn difference_offset(&self, other: &[u8]) -> usize {
        let len = self.len().min(other.len());
        for i in 0..len {
            if self[i] != other[i] {
                return i;
            }
        }
        len
    }

    fn to_hex_string(&self) -> String {
        const HEX: &[u8; 16] = b"0123456789ABCDEF";
        let mut out = String::with_capacity(self.len() * 2);
        for &b in self {
            out.push(HEX[(b >> 4) as usize] as char);
            out.push(HEX[(b & 0x0f) as usize] as char);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn difference_offset_identical() {
        assert_eq!(b"abc".difference_offset(b"abc"), 3);
    }

    #[test]
    fn difference_offset_first_byte() {
        assert_eq!(b"abc".difference_offset(b"xbc"), 0);
    }

    #[test]
    fn difference_offset_prefix() {
        assert_eq!(b"abc".difference_offset(b"abcdef"), 3);
        assert_eq!(b"abcdef".difference_offset(b"abc"), 3);
    }

    #[test]
    fn to_hex_string_basics() {
        assert_eq!(b"".to_hex_string(), "");
        assert_eq!(b"\x00\xffA".to_hex_string(), "00FF41");
    }

    #[test]
    fn slice_type_alias_is_plain_byte_slice() {
        // This test is really just a compile check: the alias should work
        // wherever you'd use `&[u8]`.
        fn takes_slice(s: Slice<'_>) -> usize {
            s.len()
        }
        let v = vec![1u8, 2, 3];
        assert_eq!(takes_slice(&v), 3);
        assert_eq!(takes_slice(b"hello"), 5);
    }
}
