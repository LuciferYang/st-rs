//! Port of `table/block_based/block_builder.{h,cc}`.
//!
//! Accumulates `(key, value)` records into a single SST data block
//! using **prefix compression** anchored at periodic **restart
//! points**, then emits the encoded block via [`BlockBuilder::finish`].
//!
//! # Encoding
//!
//! The block is a sequence of records followed by a restart array:
//!
//! ```text
//!   record_0 record_1 ... record_{N-1}
//!   restart_0 (fixed32 LE)
//!   restart_1 (fixed32 LE)
//!   ...
//!   restart_{M-1} (fixed32 LE)
//!   num_restarts (fixed32 LE)
//! ```
//!
//! Each record is:
//!
//! ```text
//!   shared (varint32)        bytes shared with previous key
//!   non_shared (varint32)    bytes new from the previous key
//!   value_len (varint32)
//!   key_delta (non_shared bytes)
//!   value (value_len bytes)
//! ```
//!
//! At a restart point, `shared == 0` — the full key is written. A
//! restart is forced every `block_restart_interval` records (default
//! 16), and whenever the builder is first initialised (so the first
//! record is always a restart).
//!
//! This encoding is the **on-disk** format of every data block in
//! every block-based SST. Changing it is backwards-incompatible.

use crate::util::coding::{put_fixed32, put_varint32};

/// Default number of records between restart points. Matches
/// upstream's `BlockBasedTableOptions::block_restart_interval`.
pub const DEFAULT_BLOCK_RESTART_INTERVAL: usize = 16;

/// Builder that accumulates key-value records and emits a data block.
pub struct BlockBuilder {
    /// Output buffer — holds encoded records as they're added.
    buffer: Vec<u8>,
    /// Offsets of the restart points within `buffer`.
    restarts: Vec<u32>,
    /// Number of records since the last restart. When this hits
    /// `block_restart_interval`, the next record gets a new restart.
    counter: usize,
    /// Restart cadence.
    block_restart_interval: usize,
    /// The most recently added key, retained so we can compute the
    /// shared-prefix length with the next key.
    last_key: Vec<u8>,
    /// `true` iff `finish` has been called. After finishing, no more
    /// `add` calls are allowed.
    finished: bool,
}

impl BlockBuilder {
    /// Create a new builder with the default restart interval.
    pub fn new() -> Self {
        Self::with_restart_interval(DEFAULT_BLOCK_RESTART_INTERVAL)
    }

    /// Create a builder with a custom restart interval. `interval`
    /// must be `>= 1`; pass `1` for "no prefix compression" (every
    /// record is its own restart).
    pub fn with_restart_interval(interval: usize) -> Self {
        assert!(interval >= 1, "block_restart_interval must be >= 1");
        Self {
            buffer: Vec::new(),
            // Offset 0 is always a restart point — the very first
            // record kicks off a new restart.
            restarts: vec![0],
            counter: 0,
            block_restart_interval: interval,
            last_key: Vec::new(),
            finished: false,
        }
    }

    /// Clear state and prepare to build a new block. Keeps allocated
    /// capacity.
    pub fn reset(&mut self) {
        self.buffer.clear();
        self.restarts.clear();
        self.restarts.push(0);
        self.counter = 0;
        self.last_key.clear();
        self.finished = false;
    }

    /// Number of records added so far.
    pub fn num_records(&self) -> usize {
        // Every non-restart record increments `counter` (capped at
        // `block_restart_interval - 1`). Total records == (num_restarts
        // - 1) * interval + (counter after the last restart). We
        // don't track that explicitly; callers rarely need it.
        // For tests, approximate:
        (self.restarts.len() - 1) * self.block_restart_interval + self.counter
    }

    /// Whether any records have been added.
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Approximate size of the block if `finish` were called now.
    /// Used by the table builder to decide when to flush the block.
    pub fn current_size_estimate(&self) -> usize {
        self.buffer.len() + self.restarts.len() * 4 + 4
    }

    /// Append a `(key, value)` record. Keys must be added in strictly
    /// increasing order — the block format relies on prefix
    /// compression against the *previous* key.
    ///
    /// # Panics
    ///
    /// Panics in debug builds if the key is not strictly greater
    /// than the previously-added key.
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        assert!(!self.finished, "add() after finish()");
        debug_assert!(
            self.last_key.is_empty() || key > self.last_key.as_slice(),
            "keys must be added in strictly increasing order"
        );

        // Decide whether this record starts a new restart.
        let shared = if self.counter < self.block_restart_interval {
            // Compute shared prefix length with the previous key.
            let min_len = self.last_key.len().min(key.len());
            let mut i = 0;
            while i < min_len && self.last_key[i] == key[i] {
                i += 1;
            }
            i
        } else {
            // New restart point → shared is 0, full key stored.
            self.restarts.push(self.buffer.len() as u32);
            self.counter = 0;
            0
        };
        let non_shared = key.len() - shared;

        // [shared varint][non_shared varint][value_len varint]
        put_varint32(&mut self.buffer, shared as u32);
        put_varint32(&mut self.buffer, non_shared as u32);
        put_varint32(&mut self.buffer, value.len() as u32);
        // [key delta bytes][value bytes]
        self.buffer.extend_from_slice(&key[shared..]);
        self.buffer.extend_from_slice(value);

        // Update `last_key` to match the new key in-place.
        self.last_key.truncate(shared);
        self.last_key.extend_from_slice(&key[shared..]);
        debug_assert_eq!(self.last_key.as_slice(), key);

        self.counter += 1;
    }

    /// Emit the final block bytes. Appends the restart array + count
    /// to the buffer and returns a slice over the finished contents.
    ///
    /// After `finish`, subsequent `add` calls panic. Call [`reset`](Self::reset)
    /// to reuse the builder.
    pub fn finish(&mut self) -> &[u8] {
        assert!(!self.finished, "finish() called twice");
        for &r in &self.restarts {
            put_fixed32(&mut self.buffer, r);
        }
        put_fixed32(&mut self.buffer, self.restarts.len() as u32);
        self.finished = true;
        &self.buffer
    }
}

impl Default for BlockBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_record() {
        let mut b = BlockBuilder::new();
        b.add(b"key1", b"value1");
        let out = b.finish();
        // We can't easily assert the exact byte layout without
        // reimplementing the parser here; instead we round-trip
        // through `Block` in the block.rs test file.
        assert!(!out.is_empty());
    }

    #[test]
    fn restart_interval_boundary() {
        let mut b = BlockBuilder::with_restart_interval(2);
        b.add(b"key1", b"v");
        b.add(b"key2", b"v");
        b.add(b"key3", b"v"); // This one starts a new restart.
        let _out = b.finish();
        // Restart points: [0, <some_offset>] (2 restarts for 3 records
        // at interval 2). Verified via round-trip tests in block.rs.
    }

    #[test]
    fn reset_allows_reuse() {
        let mut b = BlockBuilder::new();
        b.add(b"a", b"1");
        b.finish();
        b.reset();
        assert!(b.is_empty());
        b.add(b"x", b"2");
        let _ = b.finish();
    }

    #[test]
    #[should_panic(expected = "add() after finish()")]
    fn add_after_finish_panics() {
        let mut b = BlockBuilder::new();
        b.add(b"a", b"1");
        b.finish();
        b.add(b"b", b"2");
    }

    #[test]
    fn current_size_estimate_grows() {
        let mut b = BlockBuilder::new();
        let s0 = b.current_size_estimate();
        b.add(b"k", b"v");
        let s1 = b.current_size_estimate();
        assert!(s1 > s0);
    }
}
