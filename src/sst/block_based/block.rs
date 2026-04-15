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

//! Port of `table/block_based/block.{h,cc}`.
//!
//! Reader side of the data-block format emitted by
//! [`crate::sst::block_based::block_builder::BlockBuilder`]. See
//! that module for the on-disk layout; this file does the reverse.
//!
//! # Usage
//!
//! ```ignore
//! let block = Block::new(encoded_bytes.to_vec())?;
//! let mut it = block.iter();
//! it.seek_to_first();
//! while it.valid() {
//!     use_entry(it.key(), it.value());
//!     it.next();
//! }
//! ```
//!
//! # Seek
//!
//! `seek` performs a binary search over the restart array: each
//! restart points at a "fresh" key with no prefix compression, so we
//! can compare restart keys against the target directly. Once we've
//! narrowed down to one restart interval (`block_restart_interval`
//! records), we walk forward to find the exact entry.

use crate::core::status::{Result, Status};
use crate::util::coding::{decode_fixed32, get_varint32};

/// Parsed data block. Owns its backing buffer.
pub struct Block {
    /// Raw block bytes as emitted by `BlockBuilder::finish`.
    bytes: Vec<u8>,
    /// Offset of the restart array within `bytes`.
    restart_offset: usize,
    /// Number of restart points.
    num_restarts: u32,
}

impl Block {
    /// Parse an encoded block. Validates the tail structure but not
    /// individual records — iterator calls surface record-level
    /// corruption lazily.
    pub fn new(bytes: Vec<u8>) -> Result<Self> {
        if bytes.len() < 4 {
            return Err(Status::corruption("block too small: no num_restarts"));
        }
        let num_restarts = decode_fixed32(&bytes[bytes.len() - 4..])?;
        let max_restarts = (bytes.len() - 4) / 4;
        if num_restarts as usize > max_restarts {
            return Err(Status::corruption(format!(
                "block claims {num_restarts} restarts but only {max_restarts} fit"
            )));
        }
        let restart_offset = bytes.len() - (1 + num_restarts as usize) * 4;
        Ok(Self {
            bytes,
            restart_offset,
            num_restarts,
        })
    }

    /// Total encoded size (including restart array and count).
    pub fn size(&self) -> usize {
        self.bytes.len()
    }

    /// Number of restart points. Records per restart is at most
    /// `block_restart_interval`.
    pub fn num_restarts(&self) -> u32 {
        self.num_restarts
    }

    /// Borrow the raw bytes — useful for re-emitting to a test sink.
    pub fn raw(&self) -> &[u8] {
        &self.bytes
    }

    /// Create a new iterator over the block. Position is initially
    /// invalid; call `seek_to_first` or `seek` before reading.
    pub fn iter(&self) -> BlockIter<'_> {
        BlockIter::new(self)
    }

    /// Return the byte offset of restart point `i`.
    fn restart_point(&self, i: u32) -> u32 {
        let off = self.restart_offset + (i as usize) * 4;
        u32::from_le_bytes(self.bytes[off..off + 4].try_into().unwrap())
    }
}

/// Forward + backward iterator over a [`Block`].
///
/// The iterator holds a decoded "current entry" in its own buffers
/// (`key_buf`, `value_range`) so the `key()` / `value()` accessors
/// can return borrowed slices with the iterator's lifetime.
pub struct BlockIter<'a> {
    block: &'a Block,
    /// Byte offset of the current record's payload (after the three
    /// varint header fields). `None` means "invalid / not positioned."
    current_offset: Option<usize>,
    /// Decoded current key. Rebuilt on every step to account for
    /// prefix compression.
    key_buf: Vec<u8>,
    /// Byte range of the current value within `block.bytes`.
    value_range: (usize, usize),
    /// Index of the restart point at or before `current_offset`.
    restart_index: u32,
    /// First recoverable status.
    status: Result<()>,
}

impl<'a> BlockIter<'a> {
    fn new(block: &'a Block) -> Self {
        Self {
            block,
            current_offset: None,
            key_buf: Vec::new(),
            value_range: (0, 0),
            restart_index: 0,
            status: Ok(()),
        }
    }

    /// Is the iterator currently positioned on a record?
    pub fn valid(&self) -> bool {
        self.status.is_ok() && self.current_offset.is_some()
    }

    /// Current key. Requires [`Self::valid`].
    pub fn key(&self) -> &[u8] {
        debug_assert!(self.valid());
        &self.key_buf
    }

    /// Current value. Requires [`Self::valid`].
    pub fn value(&self) -> &[u8] {
        debug_assert!(self.valid());
        &self.block.bytes[self.value_range.0..self.value_range.1]
    }

    /// Propagated error, if any.
    pub fn status(&self) -> Result<()> {
        self.status.clone()
    }

    fn mark_invalid(&mut self) {
        self.current_offset = None;
        self.key_buf.clear();
        self.value_range = (0, 0);
    }

    fn mark_corrupt(&mut self, msg: &str) {
        self.mark_invalid();
        self.status = Err(Status::corruption(msg.to_string()));
    }

    /// Seek to the first record.
    pub fn seek_to_first(&mut self) {
        self.mark_invalid();
        self.status = Ok(());
        if self.block.num_restarts == 0 {
            return;
        }
        self.seek_to_restart(0);
        // Load the first record at this restart.
        self.parse_next_at(self.block.restart_point(0) as usize);
    }

    /// Seek to the last record. Walks from the last restart forward.
    pub fn seek_to_last(&mut self) {
        self.mark_invalid();
        self.status = Ok(());
        if self.block.num_restarts == 0 {
            return;
        }
        let last_restart = self.block.num_restarts - 1;
        self.seek_to_restart(last_restart);
        self.parse_next_at(self.block.restart_point(last_restart) as usize);
        while self.valid() {
            // Peek at the next record. If another one exists within
            // the data region, advance.
            let next_off = self.value_range.1;
            if next_off >= self.block.restart_offset {
                break;
            }
            self.parse_next_at(next_off);
        }
    }

    /// Seek to the first record with key `>= target`.
    pub fn seek(&mut self, target: &[u8]) {
        // Binary search over restart keys first.
        let mut left = 0u32;
        let mut right = self.block.num_restarts.saturating_sub(1);
        // Each restart point's key is the first record in that
        // restart, so comparing restart keys narrows to a range.
        while left < right {
            let mid = left + (right - left).div_ceil(2);
            let offset = self.block.restart_point(mid) as usize;
            match self.decode_key_at(offset) {
                Some((key, _value_off, _key_end)) => {
                    if key.as_slice() < target {
                        left = mid;
                    } else {
                        right = mid - 1;
                    }
                }
                None => {
                    self.mark_corrupt("bad block entry during seek");
                    return;
                }
            }
        }
        // Linear scan from `left` forward.
        self.seek_to_restart(left);
        self.parse_next_at(self.block.restart_point(left) as usize);
        while self.valid() && self.key_buf.as_slice() < target {
            let next_off = self.value_range.1;
            if next_off >= self.block.restart_offset {
                self.mark_invalid();
                return;
            }
            self.parse_next_at(next_off);
        }
    }

    /// Advance to the next record.
    pub fn next(&mut self) {
        debug_assert!(self.valid());
        let next_off = self.value_range.1;
        if next_off >= self.block.restart_offset {
            self.mark_invalid();
            return;
        }
        // Update restart_index if we've crossed a boundary.
        while self.restart_index + 1 < self.block.num_restarts
            && self.block.restart_point(self.restart_index + 1) as usize <= next_off
        {
            self.restart_index += 1;
        }
        self.parse_next_at(next_off);
    }

    /// Retreat to the previous record. Because records are
    /// prefix-compressed forward, we must restart at the previous
    /// restart point and walk forward until we land on the record
    /// just before the current one.
    pub fn prev(&mut self) {
        debug_assert!(self.valid());
        let current_offset = self.current_offset.unwrap();
        // Walk back to the restart at/before the current position.
        while self.block.restart_point(self.restart_index) as usize >= current_offset {
            if self.restart_index == 0 {
                self.mark_invalid();
                return;
            }
            self.restart_index -= 1;
        }
        self.seek_to_restart(self.restart_index);
        let mut off = self.block.restart_point(self.restart_index) as usize;
        self.parse_next_at(off);
        // Walk forward until the next record would be `current_offset`.
        while self.valid() && self.value_range.1 < current_offset {
            off = self.value_range.1;
            self.parse_next_at(off);
        }
    }

    /// Reset internal state to the given restart index. Does not
    /// parse the record yet — call `parse_next_at` after.
    fn seek_to_restart(&mut self, idx: u32) {
        self.restart_index = idx;
        self.key_buf.clear();
    }

    /// Decode a single entry at byte offset `offset` into the block,
    /// updating `current_offset`, `key_buf`, and `value_range`.
    /// Corruption marks the iterator as invalid with an error status.
    fn parse_next_at(&mut self, offset: usize) {
        let bytes = &self.block.bytes[offset..self.block.restart_offset];
        let (shared, rest) = match get_varint32(bytes) {
            Ok(v) => v,
            Err(_) => {
                self.mark_corrupt("block entry: bad shared varint");
                return;
            }
        };
        let (non_shared, rest) = match get_varint32(rest) {
            Ok(v) => v,
            Err(_) => {
                self.mark_corrupt("block entry: bad non_shared varint");
                return;
            }
        };
        let (value_len, rest) = match get_varint32(rest) {
            Ok(v) => v,
            Err(_) => {
                self.mark_corrupt("block entry: bad value_len varint");
                return;
            }
        };
        if rest.len() < non_shared as usize + value_len as usize {
            self.mark_corrupt("block entry: truncated key/value");
            return;
        }
        // Rebuild the key: keep `shared` bytes of the previous key,
        // append the `non_shared` new bytes.
        if shared as usize > self.key_buf.len() {
            self.mark_corrupt("block entry: shared exceeds previous key length");
            return;
        }
        self.key_buf.truncate(shared as usize);
        let non_shared_bytes = &rest[..non_shared as usize];
        self.key_buf.extend_from_slice(non_shared_bytes);

        // Compute absolute offsets within the full block buffer.
        let header_len = bytes.len() - rest.len();
        let value_start = offset + header_len + non_shared as usize;
        let value_end = value_start + value_len as usize;

        self.current_offset = Some(offset);
        self.value_range = (value_start, value_end);
    }

    /// Same as `parse_next_at` but returns the decoded key to the
    /// caller without mutating the iterator state. Used by `seek`'s
    /// binary search, which needs to peek at restart-point keys
    /// without actually committing a move.
    fn decode_key_at(&self, offset: usize) -> Option<(Vec<u8>, usize, usize)> {
        let bytes = &self.block.bytes[offset..self.block.restart_offset];
        let (shared, rest) = get_varint32(bytes).ok()?;
        let (non_shared, rest) = get_varint32(rest).ok()?;
        let (_value_len, rest) = get_varint32(rest).ok()?;
        if shared != 0 {
            // At a restart point, shared is always 0 — anything else
            // means we're peeking at a non-restart entry (caller bug).
            return None;
        }
        if rest.len() < non_shared as usize {
            return None;
        }
        let key = rest[..non_shared as usize].to_vec();
        let header_len = bytes.len() - rest.len();
        Some((key, offset + header_len + non_shared as usize, 0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::block_based::block_builder::BlockBuilder;

    fn build_block(records: &[(&[u8], &[u8])], restart_interval: usize) -> Block {
        let mut b = BlockBuilder::with_restart_interval(restart_interval);
        for (k, v) in records {
            b.add(k, v);
        }
        let bytes = b.finish().to_vec();
        Block::new(bytes).unwrap()
    }

    #[test]
    fn single_record_round_trip() {
        let block = build_block(&[(b"key1" as &[u8], b"val1" as &[u8])], 16);
        let mut it = block.iter();
        it.seek_to_first();
        assert!(it.valid());
        assert_eq!(it.key(), b"key1");
        assert_eq!(it.value(), b"val1");
        it.next();
        assert!(!it.valid());
    }

    #[test]
    fn multi_record_forward_iteration() {
        let records: Vec<(&[u8], &[u8])> = vec![
            (b"a", b"1"),
            (b"b", b"2"),
            (b"c", b"3"),
            (b"d", b"4"),
        ];
        let block = build_block(&records, 16);
        let mut it = block.iter();
        it.seek_to_first();
        let mut collected: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        while it.valid() {
            collected.push((it.key().to_vec(), it.value().to_vec()));
            it.next();
        }
        assert_eq!(collected.len(), 4);
        for (i, (k, v)) in records.iter().enumerate() {
            assert_eq!(collected[i].0, k.to_vec());
            assert_eq!(collected[i].1, v.to_vec());
        }
    }

    #[test]
    fn prefix_compression_round_trips() {
        // Keys share a long prefix to exercise the prefix-compression path.
        let records: Vec<(&[u8], &[u8])> = vec![
            (b"user_1001_attr_a", b"v1"),
            (b"user_1001_attr_b", b"v2"),
            (b"user_1001_attr_c", b"v3"),
            (b"user_1002_attr_a", b"v4"),
        ];
        let block = build_block(&records, 2); // small interval
        let mut it = block.iter();
        it.seek_to_first();
        for (k, v) in &records {
            assert!(it.valid(), "iter ended early");
            assert_eq!(it.key(), *k);
            assert_eq!(it.value(), *v);
            it.next();
        }
        assert!(!it.valid());
    }

    #[test]
    fn seek_hits_exact_match() {
        let records: Vec<(&[u8], &[u8])> = vec![
            (b"aa", b"1"),
            (b"bb", b"2"),
            (b"cc", b"3"),
            (b"dd", b"4"),
            (b"ee", b"5"),
        ];
        let block = build_block(&records, 2);
        let mut it = block.iter();
        it.seek(b"cc");
        assert!(it.valid());
        assert_eq!(it.key(), b"cc");
        assert_eq!(it.value(), b"3");
    }

    #[test]
    fn seek_rounds_up_to_next_key() {
        let block = build_block(
            &[
                (b"aa" as &[u8], b"1" as &[u8]),
                (b"cc", b"3"),
                (b"ee", b"5"),
            ],
            16,
        );
        let mut it = block.iter();
        it.seek(b"bb");
        assert!(it.valid());
        assert_eq!(it.key(), b"cc");
    }

    #[test]
    fn seek_past_end_is_invalid() {
        let block = build_block(&[(b"aa" as &[u8], b"1" as &[u8])], 16);
        let mut it = block.iter();
        it.seek(b"zz");
        assert!(!it.valid());
    }

    #[test]
    fn seek_to_last() {
        let records: Vec<(&[u8], &[u8])> = vec![
            (b"aa", b"1"),
            (b"bb", b"2"),
            (b"cc", b"3"),
        ];
        let block = build_block(&records, 2);
        let mut it = block.iter();
        it.seek_to_last();
        assert!(it.valid());
        assert_eq!(it.key(), b"cc");
    }

    #[test]
    fn backward_iteration() {
        let records: Vec<(&[u8], &[u8])> = vec![
            (b"aa", b"1"),
            (b"bb", b"2"),
            (b"cc", b"3"),
            (b"dd", b"4"),
            (b"ee", b"5"),
        ];
        let block = build_block(&records, 2);
        let mut it = block.iter();
        it.seek_to_last();
        let mut collected = Vec::new();
        while it.valid() {
            collected.push(it.key().to_vec());
            it.prev();
        }
        let expected: Vec<Vec<u8>> = [b"ee", b"dd", b"cc", b"bb", b"aa"]
            .iter()
            .map(|k| k.to_vec())
            .collect();
        assert_eq!(collected, expected);
    }

    #[test]
    fn large_block_with_many_restarts() {
        // 100 records, restart every 8 → ~13 restart points.
        let mut records_owned: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for i in 0..100u32 {
            records_owned.push((format!("k{i:04}").into_bytes(), format!("v{i}").into_bytes()));
        }
        let records: Vec<(&[u8], &[u8])> = records_owned
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();
        let block = build_block(&records, 8);

        let mut it = block.iter();
        it.seek_to_first();
        let mut count = 0;
        while it.valid() {
            let expected_k = format!("k{count:04}");
            let expected_v = format!("v{count}");
            assert_eq!(it.key(), expected_k.as_bytes());
            assert_eq!(it.value(), expected_v.as_bytes());
            count += 1;
            it.next();
        }
        assert_eq!(count, 100);

        // Seek into the middle.
        it.seek(b"k0050");
        assert!(it.valid());
        assert_eq!(it.key(), b"k0050");
    }
}
