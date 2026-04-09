//! Port of `table/block_based/filter_policy.cc` +
//! `util/bloom_impl.h` — the bloom-filter side of a block-based SST.
//!
//! This module provides three public types:
//!
//! - [`BloomFilterPolicy`] — the user-facing factory that implements
//!   the Layer 0 [`crate::ext::filter_policy::FilterPolicy`] trait.
//! - [`BloomFilterBuilder`] — accumulates keys during SST construction
//!   and emits the encoded filter bytes.
//! - [`BloomFilterReader`] — parses an encoded filter block and
//!   answers `may_match` queries.
//!
//! # Encoding
//!
//! The Layer 3b filter block is a single "full-filter" bloom — a
//! monolithic bit array covering **every** key in the SST. Upstream
//! also supports "block-based" filters (one per data block) but the
//! full-filter form is the modern default since RocksDB 5.15.
//!
//! Wire format:
//!
//! ```text
//!   [ bit array bytes ]
//!   [ k (num probes) : u8 ]
//! ```
//!
//! The k byte is stored at the very end so readers can parse the
//! bit array by subtracting one from the buffer length.
//!
//! # FP rate
//!
//! At the default `bits_per_key = 10`, this configuration uses
//! `k = round(10 * ln 2) = 7` hash probes and yields an FPR around
//! 0.8% — matching upstream's `kBuiltinBloomFilter` with the same
//! tuning.
//!
//! # Hash compatibility
//!
//! The hash function is [`crate::util::hash::hash`], which at the
//! time of writing is **not** bit-compatible with upstream's
//! `rocksdb::Hash`. That means filter blocks built by this crate
//! cannot be read by upstream RocksDB and vice-versa. Within
//! `st-rs` the builder and reader use the same hash, so intra-crate
//! round trips work. When `util::hash` is made wire-compatible, no
//! changes to this file are required — the filter format itself is
//! the same.

use crate::ext::filter_policy::{FilterBitsBuilder, FilterBitsReader, FilterPolicy};
use crate::util::hash::hash;

/// Default bits per key used when constructing a bloom filter
/// without an explicit setting. Matches upstream
/// `BloomFilterPolicy::kDefaultBitsPerKey`.
pub const DEFAULT_BITS_PER_KEY: u32 = 10;

/// Canonical name for the bloom-filter metaindex entry. The table
/// builder writes this; the reader looks for it.
pub const FILTER_METAINDEX_KEY: &str = "filter.st_rs.BloomFilter";

/// User-facing factory that produces [`BloomFilterBuilder`] and
/// [`BloomFilterReader`] instances. Implements
/// [`crate::ext::filter_policy::FilterPolicy`].
#[derive(Debug, Clone, Copy)]
pub struct BloomFilterPolicy {
    bits_per_key: u32,
    k: u32,
}

impl BloomFilterPolicy {
    /// Create a new policy with a given tuning. `bits_per_key` is
    /// the primary knob — higher = lower FPR, more memory.
    /// `bits_per_key = 0` is treated as "no filter" by the upstream
    /// table builder; in this port we clamp to `>= 1`.
    pub fn new(bits_per_key: u32) -> Self {
        let bits_per_key = bits_per_key.max(1);
        // Classic bloom-filter formula: k = round(m/n * ln 2).
        // We compute this as bits_per_key * 0.69, clamped to [1, 30].
        let k = ((bits_per_key as f64 * 0.69).round() as u32).clamp(1, 30);
        Self { bits_per_key, k }
    }

    /// Convenience constructor at the default (`10 bits/key`).
    pub fn default_policy() -> Self {
        Self::new(DEFAULT_BITS_PER_KEY)
    }

    /// Expose the configured k (number of hash probes).
    pub fn num_probes(&self) -> u32 {
        self.k
    }
}

impl Default for BloomFilterPolicy {
    fn default() -> Self {
        Self::default_policy()
    }
}

impl FilterPolicy for BloomFilterPolicy {
    fn name(&self) -> &'static str {
        "st_rs.BloomFilter"
    }

    fn new_builder(&self) -> Box<dyn FilterBitsBuilder> {
        Box::new(BloomFilterBuilder::new(self.bits_per_key, self.k))
    }

    fn new_reader(&self, contents: &[u8]) -> Box<dyn FilterBitsReader> {
        Box::new(BloomFilterReader::from_bytes(contents))
    }
}

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

/// Accumulates keys during SST construction and emits the encoded
/// filter block in [`Self::finish`].
pub struct BloomFilterBuilder {
    bits_per_key: u32,
    k: u32,
    hashes: Vec<u32>,
}

impl BloomFilterBuilder {
    /// Create a fresh builder with explicit tuning. Callers that go
    /// through [`BloomFilterPolicy::new_builder`] don't need this.
    pub fn new(bits_per_key: u32, k: u32) -> Self {
        Self {
            bits_per_key,
            k,
            hashes: Vec::new(),
        }
    }
}

impl FilterBitsBuilder for BloomFilterBuilder {
    fn add_key(&mut self, key: &[u8]) {
        self.hashes.push(hash(key, 0xbc9f_1d34));
    }

    fn num_added(&self) -> usize {
        self.hashes.len()
    }

    fn finish(&mut self) -> Vec<u8> {
        // Number of bits in the filter. We size the bit array at
        // `num_keys * bits_per_key` with a minimum of 64 bits (to
        // avoid pathological tiny filters) and round up to a whole
        // byte.
        let num_keys = self.hashes.len();
        let mut bits = (num_keys * self.bits_per_key as usize).max(64);
        // Round up to 8-bit alignment.
        let bytes = bits.div_ceil(8);
        bits = bytes * 8;

        let mut array = vec![0u8; bytes + 1];

        for &h in &self.hashes {
            // Double-hashing: use the rotated h as the delta between
            // probes. This is the same trick upstream's
            // `FullFilterBitsBuilder` uses.
            let delta = h.rotate_left(15);
            let mut h = h;
            for _ in 0..self.k {
                let bit_pos = (h as usize) % bits;
                array[bit_pos / 8] |= 1u8 << (bit_pos % 8);
                h = h.wrapping_add(delta);
            }
        }
        // Trailing k byte.
        array[bytes] = self.k as u8;
        // Clear accumulated keys so the builder can be re-used.
        self.hashes.clear();
        array
    }

    fn approximate_num_entries(&self) -> usize {
        self.hashes.len()
    }
}

// ---------------------------------------------------------------------------
// Reader
// ---------------------------------------------------------------------------

/// Queries an encoded bloom filter block.
pub struct BloomFilterReader {
    bits: Vec<u8>,
    k: u32,
    /// Number of bits in `bits` (always `bits.len() * 8`). Cached
    /// so `may_match` doesn't have to recompute.
    num_bits: usize,
}

impl BloomFilterReader {
    /// Parse an encoded filter block. An empty or short input is
    /// treated as "match everything" so the caller always falls
    /// through to a real lookup — this is the safe default for a
    /// missing/corrupt filter.
    pub fn from_bytes(contents: &[u8]) -> Self {
        if contents.len() < 2 {
            return Self {
                bits: Vec::new(),
                k: 0,
                num_bits: 0,
            };
        }
        let k = contents[contents.len() - 1] as u32;
        let bits = contents[..contents.len() - 1].to_vec();
        let num_bits = bits.len() * 8;
        Self { bits, k, num_bits }
    }
}

impl FilterBitsReader for BloomFilterReader {
    fn may_match(&self, key: &[u8]) -> bool {
        if self.num_bits == 0 || self.k == 0 {
            // Degenerate filter — be conservative and say "yes".
            return true;
        }
        let h = hash(key, 0xbc9f_1d34);
        let delta = h.rotate_left(15);
        let mut h = h;
        for _ in 0..self.k {
            let bit_pos = (h as usize) % self.num_bits;
            if self.bits[bit_pos / 8] & (1u8 << (bit_pos % 8)) == 0 {
                return false;
            }
            h = h.wrapping_add(delta);
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build(policy: &BloomFilterPolicy, keys: &[&[u8]]) -> Vec<u8> {
        let mut b = policy.new_builder();
        for k in keys {
            b.add_key(k);
        }
        assert_eq!(b.num_added(), keys.len());
        b.finish()
    }

    #[test]
    fn no_false_negatives_for_inserted_keys() {
        let policy = BloomFilterPolicy::default_policy();
        let keys: &[&[u8]] = &[b"apple", b"banana", b"cherry", b"durian", b"elderberry"];
        let bytes = build(&policy, keys);
        let reader = policy.new_reader(&bytes);
        for k in keys {
            assert!(reader.may_match(k), "missing inserted key: {:?}", k);
        }
    }

    #[test]
    fn fpr_is_reasonable_on_random_keys() {
        let policy = BloomFilterPolicy::new(10);
        // 1000 inserted keys, query 10_000 random non-members.
        let inserted: Vec<Vec<u8>> = (0..1000u32)
            .map(|i| format!("key{i}").into_bytes())
            .collect();
        let inserted_refs: Vec<&[u8]> = inserted.iter().map(|k| k.as_slice()).collect();
        let bytes = build(&policy, &inserted_refs);
        let reader = policy.new_reader(&bytes);

        let mut false_positives = 0;
        let queries = 10_000u32;
        for i in 10_000..(10_000 + queries) {
            let k = format!("key{i}");
            if reader.may_match(k.as_bytes()) {
                false_positives += 1;
            }
        }
        // Default tuning targets ~1%; allow generous slack.
        let rate = false_positives as f64 / queries as f64;
        assert!(
            rate < 0.05,
            "false positive rate too high: {rate} ({false_positives} / {queries})"
        );
    }

    #[test]
    fn degenerate_empty_reader_matches_everything() {
        let reader = BloomFilterReader::from_bytes(&[]);
        assert!(reader.may_match(b"anything"));
        let reader2 = BloomFilterReader::from_bytes(&[0x5]);
        assert!(reader2.may_match(b"anything"));
    }

    #[test]
    fn builder_can_be_reused_after_finish() {
        let policy = BloomFilterPolicy::default_policy();
        let mut b = policy.new_builder();
        b.add_key(b"k1");
        let _ = b.finish();
        // After finish, num_added should be 0.
        assert_eq!(b.num_added(), 0);
        b.add_key(b"k2");
        assert_eq!(b.num_added(), 1);
        let _ = b.finish();
    }

    #[test]
    fn k_derivation_matches_formula() {
        // 10 bits/key → k ≈ 7.
        assert_eq!(BloomFilterPolicy::new(10).num_probes(), 7);
        // 4 bits/key → k ≈ 3.
        assert_eq!(BloomFilterPolicy::new(4).num_probes(), 3);
        // 1 bit/key → k ≈ 1.
        assert_eq!(BloomFilterPolicy::new(1).num_probes(), 1);
    }

    #[test]
    fn name_is_stable() {
        assert_eq!(BloomFilterPolicy::default_policy().name(), "st_rs.BloomFilter");
    }
}
