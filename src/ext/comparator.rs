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

//! Port of `include/rocksdb/comparator.h`.
//!
//! A `Comparator` provides a total order over keys. The LSM-tree uses it to
//! (a) sort keys inside memtables and SSTs, (b) merge sorted runs during
//! compaction, (c) speed up iterator seeks, and (d) decide "is this key a
//! prefix of that one" for filter blocks. Any two DBs claiming the same
//! comparator name must produce the same ordering — otherwise existing SSTs
//! become garbage.
//!
//! In Rust we express the contract as a trait with a `cmp` method returning
//! `core::cmp::Ordering`. Implementations must be `Send + Sync` because the
//! engine calls them from background compaction threads without any external
//! synchronisation.

use core::cmp::Ordering;

/// A total order over byte slices.
///
/// Any type implementing this trait must be thread-safe (`Send + Sync`) and
/// deterministic — repeated calls with the same inputs must produce the same
/// ordering. See the upstream header for more detail.
pub trait Comparator: Send + Sync {
    /// A stable, globally unique name for this comparator. Used to detect
    /// ordering mismatches when reopening a DB. Names starting with
    /// `"rocksdb."` or `"forst."` are reserved.
    fn name(&self) -> &'static str;

    /// Three-way comparison. Must induce a total order.
    fn cmp(&self, a: &[u8], b: &[u8]) -> Ordering;

    /// Equality check. Default: `cmp(a, b) == Equal`. Override only when an
    /// equality check can be materially faster than a three-way compare.
    fn equal(&self, a: &[u8], b: &[u8]) -> bool {
        self.cmp(a, b) == Ordering::Equal
    }

    /// If `*start < limit`, shrink `*start` to a shorter value that is still
    /// `>= *start` and `< limit`. Used to compact SST index blocks. The
    /// default behaviour ("do nothing") is always correct; overrides exist
    /// for space optimisation only.
    fn find_shortest_separator(&self, _start: &mut Vec<u8>, _limit: &[u8]) {}

    /// Shrink `*key` to a shorter value that is still `>= *key`. Used to
    /// compact SST index blocks. The default is a no-op.
    fn find_short_successor(&self, _key: &mut Vec<u8>) {}

    /// Size in bytes of the timestamp suffix encoded into each key, or 0 if
    /// timestamps are not used. Mirrors `Comparator::timestamp_size`.
    fn timestamp_size(&self) -> usize {
        0
    }
}

/// Lexicographic byte-for-byte comparator — the default. Stable name:
/// `"leveldb.BytewiseComparator"`, matching upstream for on-disk compatibility.
#[derive(Debug, Default, Clone, Copy)]
pub struct BytewiseComparator;

impl Comparator for BytewiseComparator {
    fn name(&self) -> &'static str {
        "leveldb.BytewiseComparator"
    }

    fn cmp(&self, a: &[u8], b: &[u8]) -> Ordering {
        a.cmp(b)
    }

    fn find_shortest_separator(&self, start: &mut Vec<u8>, limit: &[u8]) {
        // Find length of common prefix.
        let min = start.len().min(limit.len());
        let mut diff_index = 0usize;
        while diff_index < min && start[diff_index] == limit[diff_index] {
            diff_index += 1;
        }
        if diff_index >= min {
            // One is a prefix of the other; cannot shorten.
            return;
        }
        let diff_byte = start[diff_index];
        if diff_byte < 0xff && diff_byte + 1 < limit[diff_index] {
            start[diff_index] = diff_byte + 1;
            start.truncate(diff_index + 1);
        }
    }

    fn find_short_successor(&self, key: &mut Vec<u8>) {
        // Find first byte that is not 0xff and increment it.
        for i in 0..key.len() {
            if key[i] != 0xff {
                key[i] += 1;
                key.truncate(i + 1);
                return;
            }
        }
        // `key` is all 0xff — cannot shorten.
    }
}

/// Reverse of [`BytewiseComparator`]. Mirrors upstream
/// `ReverseBytewiseComparator` (`"rocksdb.ReverseBytewiseComparator"`).
#[derive(Debug, Default, Clone, Copy)]
pub struct ReverseBytewiseComparator;

impl Comparator for ReverseBytewiseComparator {
    fn name(&self) -> &'static str {
        "rocksdb.ReverseBytewiseComparator"
    }

    fn cmp(&self, a: &[u8], b: &[u8]) -> Ordering {
        b.cmp(a)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytewise_name() {
        assert_eq!(BytewiseComparator.name(), "leveldb.BytewiseComparator");
    }

    #[test]
    fn bytewise_cmp_lexicographic() {
        let c = BytewiseComparator;
        assert_eq!(c.cmp(b"abc", b"abd"), Ordering::Less);
        assert_eq!(c.cmp(b"abc", b"abc"), Ordering::Equal);
        assert_eq!(c.cmp(b"abcd", b"abc"), Ordering::Greater);
    }

    #[test]
    fn reverse_bytewise_swaps_order() {
        let c = ReverseBytewiseComparator;
        assert_eq!(c.cmp(b"abc", b"abd"), Ordering::Greater);
        assert_eq!(c.cmp(b"abd", b"abc"), Ordering::Less);
    }

    #[test]
    fn shortest_separator_trivially_truncates() {
        let c = BytewiseComparator;
        let mut start = b"abcdef".to_vec();
        c.find_shortest_separator(&mut start, b"abz");
        // "abc" is a valid separator: >= "abcdef" … wait, that's the rule
        // upstream uses: the result must be >= original start and < limit.
        // Our simple impl picks the first differing byte of `start` and
        // bumps it; for ("abcdef", "abz") that produces "abd".
        assert!(start.as_slice() >= b"abcdef".as_slice());
        assert!(start.as_slice() < b"abz".as_slice());
    }

    #[test]
    fn short_successor_increments_first_non_ff() {
        let c = BytewiseComparator;
        let mut k = vec![0xff, 0x01, 0x02];
        c.find_short_successor(&mut k);
        assert_eq!(k, vec![0xff, 0x02]);
    }

    #[test]
    fn short_successor_all_ff_is_noop() {
        let c = BytewiseComparator;
        let mut k = vec![0xff, 0xff];
        c.find_short_successor(&mut k);
        assert_eq!(k, vec![0xff, 0xff]);
    }
}
