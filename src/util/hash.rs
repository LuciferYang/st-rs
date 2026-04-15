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

//! Port of `util/hash.h` + `util/hash.cc`.
//!
//! Upstream RocksDB defines a family of hash functions: `Hash()` (32-bit,
//! murmur-like), `Hash64()` (64-bit), and `Lower32of64()` helpers. The
//! Bloom and Ribbon filter blocks **must** use the exact same hash
//! function on both the writer and reader side; any mismatch silently
//! corrupts filter lookups.
//!
//! This Layer 1 port provides:
//! - [`hash`] — a 32-bit FNV-1a variant. **Not wire-compatible** with
//!   upstream `util::Hash`. A Layer 2 port will replace this with a
//!   faithful RocksDB hash that produces identical output for identical
//!   inputs — see the issue tracker entry
//!   [*"port util::Hash with upstream parity"*] when it's filed.
//! - [`hash64`] — a 64-bit xxHash-like mixer, also layer-0-compatible
//!   only.
//!
//! Both functions are deterministic, stable across runs, and suitable for
//! *in-memory* hash maps, bloom filter development, and tests. They are
//! not suitable for persistent filter blocks until the Layer 2 replacement
//! lands.

/// A 32-bit hash over `data` seeded with `seed`. FNV-1a variant — stable
/// and fast, but **not** bit-compatible with upstream `rocksdb::Hash`.
/// Layer 2 will replace this with a compatibility-preserving implementation.
pub fn hash(data: &[u8], seed: u32) -> u32 {
    const FNV_OFFSET_BASIS: u32 = 0x811c_9dc5;
    const FNV_PRIME: u32 = 0x0100_0193;
    let mut h = FNV_OFFSET_BASIS ^ seed;
    for &b in data {
        h ^= b as u32;
        h = h.wrapping_mul(FNV_PRIME);
    }
    h
}

/// A 64-bit hash over `data` seeded with `seed`. Simple mix function
/// based on xxHash's finalizer.
pub fn hash64(data: &[u8], seed: u64) -> u64 {
    const PRIME1: u64 = 0x9e37_79b1_85eb_ca87;
    const PRIME2: u64 = 0xc2b2_ae3d_27d4_eb4f;
    let mut h = seed.wrapping_add(PRIME1);
    for &b in data {
        h ^= b as u64;
        h = h.wrapping_mul(PRIME2);
        h = h.rotate_left(31);
    }
    // Final mixing (xxHash-style).
    h ^= h >> 33;
    h = h.wrapping_mul(PRIME2);
    h ^= h >> 29;
    h = h.wrapping_mul(PRIME1);
    h ^= h >> 32;
    h
}

/// Extract the low 32 bits of a `u64` hash. Matches upstream
/// `Lower32of64`.
#[inline]
pub fn lower32_of_64(h: u64) -> u32 {
    h as u32
}

/// Extract the upper 32 bits of a `u64` hash. Matches upstream
/// `Upper32of64`.
#[inline]
pub fn upper32_of_64(h: u64) -> u32 {
    (h >> 32) as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_is_deterministic() {
        assert_eq!(hash(b"hello", 0), hash(b"hello", 0));
    }

    #[test]
    fn hash_changes_with_seed() {
        assert_ne!(hash(b"hello", 0), hash(b"hello", 1));
    }

    #[test]
    fn hash_changes_with_input() {
        assert_ne!(hash(b"hello", 0), hash(b"world", 0));
    }

    #[test]
    fn hash_of_empty_depends_only_on_seed() {
        let a = hash(b"", 42);
        let b = hash(b"", 42);
        assert_eq!(a, b);
    }

    #[test]
    fn hash64_spreads_similar_inputs() {
        // Sanity check: two single-byte inputs should land in very
        // different hash buckets.
        let a = hash64(b"a", 0);
        let b = hash64(b"b", 0);
        assert_ne!(a, b);
        // The low 32 bits should also differ.
        assert_ne!(lower32_of_64(a), lower32_of_64(b));
    }

    #[test]
    fn split_32_of_64_round_trip() {
        let h = 0x0123_4567_89ab_cdefu64;
        assert_eq!(upper32_of_64(h), 0x0123_4567);
        assert_eq!(lower32_of_64(h), 0x89ab_cdef);
    }
}
