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

//! Port of `table/two_level_iterator.{h,cc}` specialised for the
//! block-based SST format.
//!
//! [`SstIter`] is a forward iterator over every record in a
//! [`BlockBasedTableReader`]. It walks the in-memory index block to
//! discover data block handles, fetches each data block via the
//! reader's `pub(crate)` `read_block_bytes` accessor, and parses the
//! data block records inline (rather than reusing
//! [`crate::sst::block_based::block::BlockIter`], which would
//! introduce a self-referential lifetime problem since the iterator
//! would need to borrow from a `Box<Block>` field of the same
//! struct).
//!
//! # What's supported
//!
//! - `seek_to_first` + repeated `next` — full forward scan
//! - `seek(target)` — position at the first key `>= target`
//!
//! # What's not (Layer 4c)
//!
//! - `seek_to_last` / `prev` — backward iteration. Compaction and
//!   the read-path merging iterator only need forward, so this is
//!   deferred.
//! - Block cache integration — every data-block read goes straight
//!   to the file. Layer 2's `LruCache` will plug in here.

use crate::core::status::{Result, Status};
use crate::sst::block_based::block::BlockIter;
use crate::sst::block_based::table_reader::BlockBasedTableReader;
use crate::sst::format::BlockHandle;
use crate::util::coding::get_varint32;

/// Forward iterator over every key/value pair in an SST file.
///
/// The iterator borrows the [`BlockBasedTableReader`] for the
/// duration of its life — clone the reader behind an `Arc` if you
/// need to spawn multiple iterators.
pub struct SstIter<'a> {
    table: &'a BlockBasedTableReader,
    /// Iterator over the index block. Each entry's key is the
    /// separator and the value is the encoded `BlockHandle` of
    /// the data block that holds keys `<= separator`.
    index_iter: BlockIter<'a>,
    /// Bytes of the currently-loaded data block (no trailer).
    /// `None` until [`Self::seek_to_first`] or [`Self::seek`] is
    /// called for the first time, or after running off the end.
    current_block: Option<Vec<u8>>,
    /// Byte offset of the next record to parse within
    /// `current_block`. Records live in `[0, restart_offset)`.
    cursor: usize,
    /// Cached `restart_offset` of `current_block` (where the
    /// per-block restart array begins). `0` when no block loaded.
    restart_offset: usize,
    /// Decoded current key. Rebuilt on each step to handle prefix
    /// compression with the previous key.
    key: Vec<u8>,
    /// Range of the current value within `current_block`.
    value_range: (usize, usize),
    /// Whether `key` and `value_range` reflect a real positioned
    /// entry.
    valid: bool,
    /// Sticky error status. Once set, the iterator stays invalid.
    status: Result<()>,
}

impl<'a> SstIter<'a> {
    /// Create a new iterator. Position is initially invalid; call
    /// [`Self::seek_to_first`] or [`Self::seek`] before reading.
    pub(crate) fn new(table: &'a BlockBasedTableReader) -> Self {
        Self {
            table,
            index_iter: table.index_block().iter(),
            current_block: None,
            cursor: 0,
            restart_offset: 0,
            key: Vec::new(),
            value_range: (0, 0),
            valid: false,
            status: Ok(()),
        }
    }

    /// Is the iterator currently positioned on a real record?
    pub fn valid(&self) -> bool {
        self.valid && self.status.is_ok()
    }

    /// Current key. Requires [`Self::valid`].
    pub fn key(&self) -> &[u8] {
        debug_assert!(self.valid());
        &self.key
    }

    /// Current value. Requires [`Self::valid`].
    pub fn value(&self) -> &[u8] {
        debug_assert!(self.valid());
        let block = self.current_block.as_ref().expect("valid implies a loaded block");
        &block[self.value_range.0..self.value_range.1]
    }

    /// Sticky status. Returns `Ok(())` until corruption or I/O
    /// failure is observed.
    pub fn status(&self) -> Result<()> {
        self.status.clone()
    }

    /// Position at the first record in the SST.
    pub fn seek_to_first(&mut self) {
        self.invalidate();
        self.index_iter.seek_to_first();
        if !self.index_iter.valid() {
            return;
        }
        self.load_current_index_block_into_data();
    }

    /// Position at the first record with key `>= target`.
    pub fn seek(&mut self, target: &[u8]) {
        self.invalidate();
        self.index_iter.seek(target);
        if !self.index_iter.valid() {
            return;
        }
        self.load_current_index_block_into_data();
        // After loading and reading the first record of the
        // candidate data block, walk forward until we land on the
        // first key `>= target`.
        while self.valid() && self.key.as_slice() < target {
            self.next();
        }
    }

    /// Advance to the next record.
    pub fn next(&mut self) {
        debug_assert!(self.valid());
        // Try to read another record from the current block.
        if self.parse_next_record() {
            return;
        }
        // Current block exhausted; advance to the next data block.
        self.index_iter.next();
        if !self.index_iter.valid() {
            self.invalidate();
            return;
        }
        self.load_current_index_block_into_data();
    }

    /// Decode the BlockHandle pointed at by the index iterator's
    /// current entry, fetch the data block, and parse the first
    /// record. Sets `valid = false` on error.
    fn load_current_index_block_into_data(&mut self) {
        let handle_bytes = self.index_iter.value();
        let handle = match BlockHandle::decode_from(handle_bytes) {
            Ok((h, _)) => h,
            Err(e) => {
                self.set_error(e);
                return;
            }
        };
        let bytes = match self.table.read_block_bytes(handle) {
            Ok(b) => b,
            Err(e) => {
                self.set_error(e);
                return;
            }
        };
        if bytes.len() < 4 {
            self.set_error(Status::corruption("data block too small"));
            return;
        }
        let num_restarts =
            u32::from_le_bytes(bytes[bytes.len() - 4..].try_into().unwrap()) as usize;
        let restart_offset = bytes.len().saturating_sub((1 + num_restarts) * 4);

        self.current_block = Some(bytes);
        self.cursor = 0;
        self.restart_offset = restart_offset;
        self.key.clear();
        self.value_range = (0, 0);
        self.valid = false;

        if !self.parse_next_record() {
            // Empty block — try the next index entry.
            self.index_iter.next();
            if self.index_iter.valid() {
                self.load_current_index_block_into_data();
            } else {
                self.invalidate();
            }
        }
    }

    /// Parse one record at `self.cursor` within `self.current_block`,
    /// updating `self.key` and `self.value_range`. Returns `true` on
    /// success, `false` if there are no more records in the block.
    fn parse_next_record(&mut self) -> bool {
        let block = match self.current_block.as_ref() {
            Some(b) => b,
            None => {
                self.invalidate();
                return false;
            }
        };
        if self.cursor >= self.restart_offset {
            self.valid = false;
            return false;
        }
        let bytes = &block[self.cursor..self.restart_offset];
        let (shared, rest) = match get_varint32(bytes) {
            Ok(v) => v,
            Err(_) => {
                self.set_error(Status::corruption("SstIter: bad shared varint"));
                return false;
            }
        };
        let (non_shared, rest) = match get_varint32(rest) {
            Ok(v) => v,
            Err(_) => {
                self.set_error(Status::corruption("SstIter: bad non_shared varint"));
                return false;
            }
        };
        let (value_len, rest) = match get_varint32(rest) {
            Ok(v) => v,
            Err(_) => {
                self.set_error(Status::corruption("SstIter: bad value_len varint"));
                return false;
            }
        };
        if rest.len() < non_shared as usize + value_len as usize {
            self.set_error(Status::corruption("SstIter: truncated entry"));
            return false;
        }
        if shared as usize > self.key.len() {
            self.set_error(Status::corruption(
                "SstIter: shared exceeds previous key length",
            ));
            return false;
        }

        // Rebuild key.
        self.key.truncate(shared as usize);
        self.key.extend_from_slice(&rest[..non_shared as usize]);

        // Compute absolute offsets for the value.
        let header_len = bytes.len() - rest.len();
        let value_start = self.cursor + header_len + non_shared as usize;
        let value_end = value_start + value_len as usize;
        self.value_range = (value_start, value_end);
        self.cursor = value_end;
        self.valid = true;
        true
    }

    fn invalidate(&mut self) {
        self.valid = false;
        self.key.clear();
        self.value_range = (0, 0);
    }

    fn set_error(&mut self, err: Status) {
        self.invalidate();
        self.status = Err(err);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::env::file_system::{FsRandomAccessFile, FsWritableFile, IoOptions};
    use crate::file::random_access_file_reader::RandomAccessFileReader;
    use crate::file::writable_file_writer::WritableFileWriter;
    use crate::sst::block_based::table_builder::{
        BlockBasedTableBuilder, BlockBasedTableOptions,
    };
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct InMem {
        bytes: Mutex<Vec<u8>>,
    }
    struct W {
        f: Arc<InMem>,
    }
    impl FsWritableFile for W {
        fn append(&mut self, data: &[u8], _: &IoOptions) -> Result<()> {
            self.f.bytes.lock().unwrap().extend_from_slice(data);
            Ok(())
        }
        fn flush(&mut self, _: &IoOptions) -> Result<()> {
            Ok(())
        }
        fn sync(&mut self, _: &IoOptions) -> Result<()> {
            Ok(())
        }
        fn close(&mut self, _: &IoOptions) -> Result<()> {
            Ok(())
        }
        fn file_size(&self) -> u64 {
            self.f.bytes.lock().unwrap().len() as u64
        }
    }
    struct R {
        f: Arc<InMem>,
    }
    impl FsRandomAccessFile for R {
        fn read_at(&self, offset: u64, buf: &mut [u8], _: &IoOptions) -> Result<usize> {
            let inner = self.f.bytes.lock().unwrap();
            let start = offset as usize;
            if start >= inner.len() {
                return Ok(0);
            }
            let end = (start + buf.len()).min(inner.len());
            buf[..end - start].copy_from_slice(&inner[start..end]);
            Ok(end - start)
        }
        fn size(&self) -> Result<u64> {
            Ok(self.f.bytes.lock().unwrap().len() as u64)
        }
    }

    fn build(
        records: &[(&[u8], &[u8])],
        opts: BlockBasedTableOptions,
    ) -> Arc<BlockBasedTableReader> {
        let f = Arc::new(InMem::default());
        let writer: Box<dyn FsWritableFile> = Box::new(W { f: Arc::clone(&f) });
        let writer = WritableFileWriter::new(writer);
        let mut tb = BlockBasedTableBuilder::new(writer, opts);
        for (k, v) in records {
            tb.add(k, v).unwrap();
        }
        tb.finish().unwrap();
        let r: Box<dyn FsRandomAccessFile> = Box::new(R { f: Arc::clone(&f) });
        let raf = RandomAccessFileReader::new(r, "t").unwrap();
        Arc::new(BlockBasedTableReader::open(Arc::new(raf)).unwrap())
    }

    #[test]
    fn empty_iter() {
        let table = build(&[], BlockBasedTableOptions::default());
        let mut it = table.iter();
        it.seek_to_first();
        assert!(!it.valid());
    }

    #[test]
    fn forward_iter_single_block() {
        let recs: &[(&[u8], &[u8])] = &[
            (b"a", b"1"),
            (b"b", b"2"),
            (b"c", b"3"),
        ];
        let table = build(recs, BlockBasedTableOptions::default());
        let mut it = table.iter();
        it.seek_to_first();
        let mut got = Vec::new();
        while it.valid() {
            got.push((it.key().to_vec(), it.value().to_vec()));
            it.next();
        }
        assert_eq!(got.len(), 3);
        assert_eq!(got[0], (b"a".to_vec(), b"1".to_vec()));
        assert_eq!(got[2], (b"c".to_vec(), b"3".to_vec()));
    }

    #[test]
    fn forward_iter_across_blocks() {
        let owned: Vec<(Vec<u8>, Vec<u8>)> = (0..200u32)
            .map(|i| (format!("k{i:04}").into_bytes(), format!("v{i}").into_bytes()))
            .collect();
        let recs: Vec<(&[u8], &[u8])> = owned
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();
        let opts = BlockBasedTableOptions {
            block_size: 64,
            ..Default::default()
        };
        let table = build(&recs, opts);

        let mut it = table.iter();
        it.seek_to_first();
        let mut count = 0u32;
        while it.valid() {
            let expected_k = format!("k{count:04}");
            let expected_v = format!("v{count}");
            assert_eq!(it.key(), expected_k.as_bytes());
            assert_eq!(it.value(), expected_v.as_bytes());
            count += 1;
            it.next();
        }
        assert_eq!(count, 200);
    }

    #[test]
    fn seek_lands_on_or_after_target() {
        let recs: Vec<(Vec<u8>, Vec<u8>)> = (0..50u32)
            .map(|i| (format!("k{i:03}").into_bytes(), format!("v{i}").into_bytes()))
            .collect();
        let recs_refs: Vec<(&[u8], &[u8])> = recs
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();
        let opts = BlockBasedTableOptions {
            block_size: 64,
            ..Default::default()
        };
        let table = build(&recs_refs, opts);

        let mut it = table.iter();
        it.seek(b"k025");
        assert!(it.valid());
        assert_eq!(it.key(), b"k025");
        // Walk a few more.
        it.next();
        assert_eq!(it.key(), b"k026");

        // Seek past the end.
        it.seek(b"zzz");
        assert!(!it.valid());

        // Seek to a non-existing key in the middle (should round up).
        it.seek(b"k025x");
        assert!(it.valid());
        assert_eq!(it.key(), b"k026");
    }

    #[test]
    fn status_is_ok_after_normal_iteration() {
        let recs: &[(&[u8], &[u8])] = &[
            (b"a", b"1"),
            (b"b", b"2"),
            (b"c", b"3"),
        ];
        let table = build(recs, BlockBasedTableOptions::default());
        let mut it = table.iter();

        // Before seeking, status should be ok.
        assert!(it.status().is_ok());

        it.seek_to_first();
        assert!(it.status().is_ok());

        // Walk all records.
        while it.valid() {
            assert!(it.status().is_ok());
            it.next();
        }
        // After exhausting the iterator, status should still be ok.
        assert!(it.status().is_ok());
    }

    #[test]
    fn seek_to_first_key() {
        let recs: &[(&[u8], &[u8])] = &[
            (b"alpha", b"first"),
            (b"beta", b"second"),
            (b"gamma", b"third"),
        ];
        let table = build(recs, BlockBasedTableOptions::default());
        let mut it = table.iter();

        it.seek(b"alpha");
        assert!(it.valid());
        assert_eq!(it.key(), b"alpha");
        assert_eq!(it.value(), b"first");
    }

    #[test]
    fn seek_to_last_key() {
        let recs: &[(&[u8], &[u8])] = &[
            (b"alpha", b"first"),
            (b"beta", b"second"),
            (b"gamma", b"third"),
        ];
        let table = build(recs, BlockBasedTableOptions::default());
        let mut it = table.iter();

        it.seek(b"gamma");
        assert!(it.valid());
        assert_eq!(it.key(), b"gamma");
        assert_eq!(it.value(), b"third");

        // Next should exhaust the iterator.
        it.next();
        assert!(!it.valid());
    }

    #[test]
    fn seek_before_first_key_lands_on_first() {
        let recs: &[(&[u8], &[u8])] = &[
            (b"bbb", b"1"),
            (b"ccc", b"2"),
        ];
        let table = build(recs, BlockBasedTableOptions::default());
        let mut it = table.iter();

        // Seek to a key before all entries.
        it.seek(b"aaa");
        assert!(it.valid());
        assert_eq!(it.key(), b"bbb");
    }

    #[test]
    fn multiple_seeks_reset_position() {
        let recs: &[(&[u8], &[u8])] = &[
            (b"a", b"1"),
            (b"b", b"2"),
            (b"c", b"3"),
            (b"d", b"4"),
        ];
        let table = build(recs, BlockBasedTableOptions::default());
        let mut it = table.iter();

        // Seek to "c".
        it.seek(b"c");
        assert!(it.valid());
        assert_eq!(it.key(), b"c");

        // Re-seek to "a".
        it.seek(b"a");
        assert!(it.valid());
        assert_eq!(it.key(), b"a");

        // Re-seek to first.
        it.seek_to_first();
        assert!(it.valid());
        assert_eq!(it.key(), b"a");
    }

    #[test]
    fn seek_on_empty_table() {
        let table = build(&[], BlockBasedTableOptions::default());
        let mut it = table.iter();
        it.seek(b"anything");
        assert!(!it.valid());
        assert!(it.status().is_ok());
    }

    #[test]
    fn forward_iter_single_entry() {
        let recs: &[(&[u8], &[u8])] = &[(b"only", b"one")];
        let table = build(recs, BlockBasedTableOptions::default());
        let mut it = table.iter();
        it.seek_to_first();
        assert!(it.valid());
        assert_eq!(it.key(), b"only");
        assert_eq!(it.value(), b"one");
        it.next();
        assert!(!it.valid());
    }
}
