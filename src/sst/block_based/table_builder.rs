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

//! Port of `table/block_based/block_based_table_builder.{h,cc}`.
//!
//! Stitches the Layer 3a primitives — data blocks, the bloom filter,
//! the metaindex, the index, and the footer — into a complete SST
//! file written through a [`WritableFileWriter`].
//!
//! # File layout (matches upstream's legacy block-based format)
//!
//! ```text
//!   +------------------------+  offset = 0
//!   | data block 0           |
//!   | trailer (5 bytes)      |
//!   +------------------------+
//!   | data block 1           |
//!   | trailer                |
//!   +------------------------+
//!   |          ...           |
//!   +------------------------+
//!   | filter block           |
//!   | trailer                |
//!   +------------------------+
//!   | metaindex block        |
//!   | trailer                |
//!   +------------------------+
//!   | index block            |
//!   | trailer                |
//!   +------------------------+
//!   | footer (48 bytes)      |
//!   +------------------------+  offset = file_size
//! ```
//!
//! Each "trailer" is `[compression_byte: u8][masked_crc32c: u32 LE]`,
//! covering the block bytes plus the compression byte.
//!
//! # API
//!
//! The builder takes ownership of the writer at construction and
//! consumes itself in [`Self::finish`] so misuse-after-finish is a
//! compile error:
//!
//! ```ignore
//! let writer = fs.new_writable_file(&path, &Default::default())?;
//! let writer = WritableFileWriter::new(writer);
//! let mut tb = BlockBasedTableBuilder::new(writer, BlockBasedTableOptions::default());
//! tb.add(b"k1", b"v1")?;
//! tb.add(b"k2", b"v2")?;
//! tb.finish()?;
//! ```
//!
//! # Out of scope
//!
//! - **Compression** — every block is written with compression byte 0
//!   (`kNoCompression`). Adding Snappy/Zstd is mechanical once those
//!   crates are available.
//! - **Multi-level partitioned index** — the index is a single
//!   block-based block. For SSTs above ~1 GB upstream uses a
//!   two-level partitioned index; deferred.
//! - **Range deletion block** — handled by a separate aggregator in
//!   upstream; deferred.
//! - **Table properties block** — Layer 4 will add it once the
//!   engine starts caring about per-SST stats.

use crate::core::status::Result;
use crate::env::file_system::IoOptions;
use crate::file::writable_file_writer::WritableFileWriter;
use crate::sst::block_based::block_builder::{BlockBuilder, DEFAULT_BLOCK_RESTART_INTERVAL};
use crate::ext::filter_policy::{FilterBitsBuilder, FilterPolicy};
use crate::sst::block_based::filter_block::{BloomFilterPolicy, FILTER_METAINDEX_KEY};
use crate::sst::format::{put_block_trailer, BlockHandle, Footer, BLOCK_TRAILER_SIZE};

/// Builder configuration. Lives next to the builder so callers don't
/// have to import a separate `options` module.
#[derive(Debug, Clone)]
pub struct BlockBasedTableOptions {
    /// Target uncompressed size of each data block in bytes. The
    /// builder flushes a block once `current_size_estimate >= block_size`.
    /// Default 4 KiB matches upstream.
    pub block_size: usize,
    /// Number of records between restart points within a data block.
    pub block_restart_interval: usize,
    /// Bloom filter tuning. `None` disables filters entirely (the
    /// SST then has no filter block and no metaindex entry for one).
    pub bloom_filter_bits_per_key: Option<u32>,
    /// Compression type for data blocks. 0=None, 1=Snappy, 4=LZ4.
    /// Default 0 (no compression) for backward compatibility.
    pub compression_type: u8,
}

impl Default for BlockBasedTableOptions {
    fn default() -> Self {
        Self {
            block_size: 4 * 1024,
            block_restart_interval: DEFAULT_BLOCK_RESTART_INTERVAL,
            bloom_filter_bits_per_key: Some(10),
            compression_type: 0,
        }
    }
}

/// Single-pass SST writer.
pub struct BlockBasedTableBuilder {
    writer: WritableFileWriter,
    opts: BlockBasedTableOptions,
    /// Current data block being assembled.
    data_block: BlockBuilder,
    /// Index block (one entry per finished data block).
    index_block: BlockBuilder,
    /// Optional bloom filter accumulator.
    filter_builder: Option<Box<dyn FilterBitsBuilder>>,
    /// Last key passed to [`Self::add`]. Used as the index entry's
    /// key when the *next* `add` triggers a flush.
    last_key: Vec<u8>,
    /// Number of entries (key-value pairs) added so far.
    num_entries: u64,
    /// Current write offset within the output file. Tracks raw
    /// bytes — including trailers — so block handles are correct.
    offset: u64,
    /// Handle of the most-recently-written data block. Pending
    /// because the index entry can only be emitted once we see the
    /// next add (so we know whether more entries are coming and
    /// what separator to use).
    pending_handle: Option<BlockHandle>,
    /// Set after `finish` so further `add` calls panic.
    finished: bool,
}

impl BlockBasedTableBuilder {
    /// Create a new builder, taking ownership of `writer` and
    /// applying `opts`.
    pub fn new(writer: WritableFileWriter, opts: BlockBasedTableOptions) -> Self {
        let filter_builder: Option<Box<dyn FilterBitsBuilder>> = opts
            .bloom_filter_bits_per_key
            .map(|b| BloomFilterPolicy::new(b).new_builder());
        Self {
            writer,
            data_block: BlockBuilder::with_restart_interval(opts.block_restart_interval),
            index_block: BlockBuilder::with_restart_interval(1),
            filter_builder,
            last_key: Vec::new(),
            num_entries: 0,
            offset: 0,
            pending_handle: None,
            finished: false,
            opts,
        }
    }

    /// Number of entries added so far.
    pub fn num_entries(&self) -> u64 {
        self.num_entries
    }

    /// Current file offset (== bytes already written through the
    /// writer, not counting buffered data).
    pub fn file_offset(&self) -> u64 {
        self.offset
    }

    /// Append a `(key, value)` pair. Keys must be added in strictly
    /// increasing order; debug builds assert.
    ///
    /// # Panics
    ///
    /// Panics if called after [`Self::finish`].
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        assert!(!self.finished, "add() after finish()");
        debug_assert!(
            self.last_key.is_empty() || key > self.last_key.as_slice(),
            "keys must be added in strictly increasing order"
        );

        // If a previous data block was flushed, emit its index
        // entry now. We use `last_key` as the separator — a future
        // optimisation can use the user comparator's
        // `find_shortest_separator` between `last_key` and `key`
        // to shrink the index entry.
        if let Some(handle) = self.pending_handle.take() {
            let mut handle_bytes = Vec::new();
            handle.encode_to(&mut handle_bytes);
            self.index_block.add(&self.last_key, &handle_bytes);
        }

        if let Some(filter) = &mut self.filter_builder {
            filter.add_key(key);
        }

        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.data_block.add(key, value);
        self.num_entries += 1;

        if self.data_block.current_size_estimate() >= self.opts.block_size {
            self.flush_data_block()?;
        }
        Ok(())
    }

    /// Force-flush the current data block (if any). Used internally
    /// once the block hits its target size, and at finish.
    fn flush_data_block(&mut self) -> Result<()> {
        if self.data_block.is_empty() {
            return Ok(());
        }
        let bytes = self.data_block.finish().to_vec();
        let handle = self.write_block(&bytes)?;
        self.data_block = BlockBuilder::with_restart_interval(self.opts.block_restart_interval);
        self.pending_handle = Some(handle);
        Ok(())
    }

    /// Append `data` to the file with the standard 5-byte trailer
    /// and return the handle that points at it (offset + raw size,
    /// **not** including the trailer).
    fn write_block(&mut self, data: &[u8]) -> Result<BlockHandle> {
        // Compress if requested. If compression is disabled (type 0),
        // skip the compress call entirely to avoid a needless copy.
        let (block_bytes_owned, actual_type) = if self.opts.compression_type != 0 {
            match crate::sst::format::compress_block(data, self.opts.compression_type) {
                Ok(compressed) if compressed.len() < data.len() => {
                    (compressed, self.opts.compression_type)
                }
                _ => (data.to_vec(), 0u8), // fallback to uncompressed
            }
        } else {
            (data.to_vec(), 0u8)
        };
        let block_bytes = block_bytes_owned.as_slice();

        let handle = BlockHandle::new(self.offset, block_bytes.len() as u64);
        let io = IoOptions::default();
        self.writer.append(block_bytes, &io)?;
        let mut trailer = Vec::with_capacity(BLOCK_TRAILER_SIZE);
        put_block_trailer(&mut trailer, actual_type, block_bytes);
        self.writer.append(&trailer, &io)?;
        self.offset += block_bytes.len() as u64 + BLOCK_TRAILER_SIZE as u64;
        Ok(handle)
    }

    /// Finalise the SST: flush any pending data block, write the
    /// filter block (if filters are enabled), the metaindex, the
    /// index, and the footer. Closes the underlying file.
    pub fn finish(mut self) -> Result<()> {
        assert!(!self.finished, "finish() called twice");
        self.finished = true;

        // 1. Flush the last data block.
        self.flush_data_block()?;

        // 2. Emit the trailing index entry, if there is one.
        if let Some(handle) = self.pending_handle.take() {
            let mut handle_bytes = Vec::new();
            handle.encode_to(&mut handle_bytes);
            self.index_block.add(&self.last_key, &handle_bytes);
        }

        // 3. Filter block — only if filters are enabled.
        let filter_handle = if let Some(filter) = self.filter_builder.as_mut() {
            let bytes = filter.finish();
            Some(self.write_block(&bytes)?)
        } else {
            None
        };

        // 4. Metaindex block. Maps a name → block handle. The only
        //    entry we write at Layer 3b is the bloom filter; future
        //    layers can add table-properties etc. here.
        let mut metaindex = BlockBuilder::with_restart_interval(1);
        if let Some(fh) = filter_handle {
            let mut handle_bytes = Vec::new();
            fh.encode_to(&mut handle_bytes);
            metaindex.add(FILTER_METAINDEX_KEY.as_bytes(), &handle_bytes);
        }
        let metaindex_bytes = metaindex.finish().to_vec();
        let metaindex_handle = self.write_block(&metaindex_bytes)?;

        // 5. Index block.
        let index_bytes = self.index_block.finish().to_vec();
        let index_handle = self.write_block(&index_bytes)?;

        // 6. Footer (last 48 bytes).
        let footer = Footer::new(metaindex_handle, index_handle);
        let mut footer_bytes = Vec::with_capacity(Footer::ENCODED_LENGTH);
        footer.encode_to(&mut footer_bytes);
        let io = IoOptions::default();
        self.writer.append(&footer_bytes, &io)?;
        self.offset += footer_bytes.len() as u64;

        // 7. Flush + close. The writer's own buffer plus the
        //    underlying FsWritableFile's OS buffer get pushed here.
        self.writer.flush(&io)?;
        self.writer.close(&io)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::env::file_system::FsWritableFile;

    /// In-memory `FsWritableFile` so the builder tests don't need
    /// to touch the actual filesystem. Captures everything written
    /// in an owned buffer for assertions.
    #[derive(Default)]
    struct MemFile {
        buffer: Vec<u8>,
    }

    impl FsWritableFile for MemFile {
        fn append(&mut self, data: &[u8], _opts: &IoOptions) -> Result<()> {
            self.buffer.extend_from_slice(data);
            Ok(())
        }
        fn flush(&mut self, _opts: &IoOptions) -> Result<()> {
            Ok(())
        }
        fn sync(&mut self, _opts: &IoOptions) -> Result<()> {
            Ok(())
        }
        fn close(&mut self, _opts: &IoOptions) -> Result<()> {
            Ok(())
        }
        fn file_size(&self) -> u64 {
            self.buffer.len() as u64
        }
    }

    /// A wrapper that lets us peek at the underlying buffer after
    /// the table builder has consumed the writer.
    struct CaptureFile {
        captured: std::sync::Arc<std::sync::Mutex<Vec<u8>>>,
    }

    impl CaptureFile {
        fn new(captured: std::sync::Arc<std::sync::Mutex<Vec<u8>>>) -> Self {
            Self { captured }
        }
    }

    impl FsWritableFile for CaptureFile {
        fn append(&mut self, data: &[u8], _opts: &IoOptions) -> Result<()> {
            self.captured.lock().unwrap().extend_from_slice(data);
            Ok(())
        }
        fn flush(&mut self, _opts: &IoOptions) -> Result<()> {
            Ok(())
        }
        fn sync(&mut self, _opts: &IoOptions) -> Result<()> {
            Ok(())
        }
        fn close(&mut self, _opts: &IoOptions) -> Result<()> {
            Ok(())
        }
        fn file_size(&self) -> u64 {
            self.captured.lock().unwrap().len() as u64
        }
    }

    fn build_to_buffer(records: &[(&[u8], &[u8])]) -> Vec<u8> {
        let cap = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let writer: Box<dyn FsWritableFile> = Box::new(CaptureFile::new(cap.clone()));
        let writer = WritableFileWriter::new(writer);
        let mut tb = BlockBasedTableBuilder::new(writer, BlockBasedTableOptions::default());
        for (k, v) in records {
            tb.add(k, v).unwrap();
        }
        let n = tb.num_entries();
        assert_eq!(n, records.len() as u64);
        tb.finish().unwrap();
        let inner = cap.lock().unwrap();
        inner.clone()
    }

    #[test]
    fn finishes_with_valid_footer() {
        let bytes = build_to_buffer(&[(b"k1" as &[u8], b"v1" as &[u8])]);
        // The last 48 bytes should decode as a Footer.
        let footer = Footer::decode_from(&bytes).unwrap();
        // Both metaindex and index handles must point inside the file.
        assert!(footer.metaindex_handle.offset + footer.metaindex_handle.size <= bytes.len() as u64);
        assert!(footer.index_handle.offset + footer.index_handle.size <= bytes.len() as u64);
    }

    #[test]
    fn empty_table_still_emits_footer() {
        let bytes = build_to_buffer(&[]);
        // 48 bytes for the footer + a bit for metaindex/index (which
        // are empty blocks but still take some space) + their trailers.
        assert!(bytes.len() >= Footer::ENCODED_LENGTH);
        let _footer = Footer::decode_from(&bytes).unwrap();
    }

    #[test]
    fn many_records_produce_multiple_data_blocks() {
        // Use a tiny block_size to force many flushes.
        let cap = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let writer: Box<dyn FsWritableFile> = Box::new(CaptureFile::new(cap.clone()));
        let writer = WritableFileWriter::new(writer);
        let opts = BlockBasedTableOptions {
            block_size: 64,
            ..Default::default()
        };
        let mut tb = BlockBasedTableBuilder::new(writer, opts);
        for i in 0..50u32 {
            let k = format!("key{i:04}");
            let v = format!("value{i}");
            tb.add(k.as_bytes(), v.as_bytes()).unwrap();
        }
        tb.finish().unwrap();
        let bytes = cap.lock().unwrap().clone();
        // The footer should be the last 48 bytes regardless of file size.
        let _ = Footer::decode_from(&bytes).unwrap();
    }

    #[test]
    fn num_entries_tracks_calls_to_add() {
        let cap = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let writer: Box<dyn FsWritableFile> = Box::new(CaptureFile::new(cap));
        let writer = WritableFileWriter::new(writer);
        let mut tb = BlockBasedTableBuilder::new(writer, BlockBasedTableOptions::default());
        assert_eq!(tb.num_entries(), 0);
        tb.add(b"a", b"1").unwrap();
        assert_eq!(tb.num_entries(), 1);
        tb.add(b"b", b"2").unwrap();
        assert_eq!(tb.num_entries(), 2);
        tb.finish().unwrap();
    }

    #[test]
    #[should_panic(expected = "add() after finish()")]
    fn add_after_finish_panics() {
        let cap = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let writer: Box<dyn FsWritableFile> = Box::new(CaptureFile::new(cap));
        let writer = WritableFileWriter::new(writer);
        let mut tb = BlockBasedTableBuilder::new(writer, BlockBasedTableOptions::default());
        tb.add(b"a", b"1").unwrap();
        // Need a way to call add after finish... since finish takes
        // self by value, we test a slightly different flow: simulate
        // a manual `finished = true` via the contract by reaching
        // through the public API. Since Rust prevents this directly,
        // we instead trigger the assertion by toggling internal state
        // through a handcrafted call sequence inside a non-finish
        // path. The straightforward way: set finished and call add.
        tb.finished = true;
        tb.add(b"b", b"2").unwrap();
    }

    #[test]
    fn unused_in_memory_writer_compiles() {
        // Smoke test that MemFile satisfies the trait — the body is
        // a no-op assertion just to make the type-system happy.
        let _: Box<dyn FsWritableFile> = Box::<MemFile>::default();
    }
}
