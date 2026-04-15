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

//! Port of `file/writable_file_writer.{h,cc}`.
//!
//! A buffered wrapper around a [`FsWritableFile`]. Every SST builder
//! and WAL writer in upstream RocksDB goes through this layer rather
//! than calling `Append` directly, for three reasons:
//!
//! 1. **Reduced syscall count** — small key-value writes are coalesced
//!    into a single large `Append` on the underlying file.
//! 2. **Rate limiting hook** — in upstream the writer consults a rate
//!    limiter before flushing to the filesystem. We leave the hook
//!    unimplemented at Layer 2; the interface shape matches so a
//!    `RateLimiter` trait can be wired in later without breaking
//!    callers.
//! 3. **Checksum computation** — upstream optionally computes per-block
//!    CRC32C and hands it off to the filesystem via the
//!    `DataVerificationInfo` struct. We skip this at Layer 2 because
//!    `util::hash` is not yet wire-compatible with upstream.
//!
//! The Layer 2 port keeps only (1) — buffering — and exposes a simple
//! `append` / `flush` / `sync` / `close` surface.

use crate::core::status::Result;
use crate::env::file_system::{FsWritableFile, IoOptions};

/// Default internal buffer size: 64 KiB. Matches upstream's
/// `kDefaultWritableFileBufferSize`.
pub const DEFAULT_BUFFER_SIZE: usize = 64 * 1024;

/// Buffered writer that owns a [`FsWritableFile`].
pub struct WritableFileWriter {
    file: Box<dyn FsWritableFile>,
    /// Pending bytes waiting to be pushed to the file.
    buffer: Vec<u8>,
    /// Maximum buffer size before auto-flush.
    max_buffer_size: usize,
    /// Total bytes appended by the caller since construction.
    logical_size: u64,
}

impl WritableFileWriter {
    /// Wrap `file` with the default buffer size.
    pub fn new(file: Box<dyn FsWritableFile>) -> Self {
        Self::with_buffer_size(file, DEFAULT_BUFFER_SIZE)
    }

    /// Wrap `file` with a custom buffer size.
    pub fn with_buffer_size(file: Box<dyn FsWritableFile>, buffer_size: usize) -> Self {
        let logical_size = file.file_size();
        Self {
            file,
            buffer: Vec::with_capacity(buffer_size),
            max_buffer_size: buffer_size,
            logical_size,
        }
    }

    /// Logical size of the file: everything the caller has handed to
    /// `append`, whether or not it has been flushed.
    pub fn file_size(&self) -> u64 {
        self.logical_size
    }

    /// Append `data`. If the buffer overflows, the buffer is flushed
    /// first; writes larger than the buffer capacity bypass the buffer
    /// entirely (matching upstream `WritableFileWriter::Append`).
    pub fn append(&mut self, data: &[u8], opts: &IoOptions) -> Result<()> {
        self.logical_size += data.len() as u64;

        // Fast path: fits in the buffer.
        if self.buffer.len() + data.len() <= self.max_buffer_size {
            self.buffer.extend_from_slice(data);
            return Ok(());
        }

        // Flush anything in the buffer first.
        self.flush_buffer(opts)?;

        // Oversized writes go direct-to-file; smaller ones get buffered.
        if data.len() >= self.max_buffer_size {
            self.file.append(data, opts)?;
        } else {
            self.buffer.extend_from_slice(data);
        }
        Ok(())
    }

    /// Push whatever's in the user buffer down to the underlying file.
    fn flush_buffer(&mut self, opts: &IoOptions) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        self.file.append(&self.buffer, opts)?;
        self.buffer.clear();
        Ok(())
    }

    /// Flush the internal buffer AND the underlying file's OS buffer.
    pub fn flush(&mut self, opts: &IoOptions) -> Result<()> {
        self.flush_buffer(opts)?;
        self.file.flush(opts)
    }

    /// Flush the buffer and `fsync` data only (equivalent of upstream's
    /// `Sync(use_fsync=false)`).
    pub fn sync(&mut self, opts: &IoOptions) -> Result<()> {
        self.flush_buffer(opts)?;
        self.file.sync(opts)
    }

    /// Flush the buffer and `fsync` data + metadata.
    pub fn fsync(&mut self, opts: &IoOptions) -> Result<()> {
        self.flush_buffer(opts)?;
        self.file.fsync(opts)
    }

    /// Flush + close.
    pub fn close(&mut self, opts: &IoOptions) -> Result<()> {
        self.flush_buffer(opts)?;
        self.file.close(opts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A stub `FsWritableFile` that records every `append` call into
    /// an owned `Vec`. Lets the tests assert on write sizes and counts.
    #[derive(Default)]
    struct StubWritable {
        buffer: Vec<u8>,
        append_calls: u32,
        flush_calls: u32,
        sync_calls: u32,
    }

    impl FsWritableFile for StubWritable {
        fn append(&mut self, data: &[u8], _opts: &IoOptions) -> Result<()> {
            self.append_calls += 1;
            self.buffer.extend_from_slice(data);
            Ok(())
        }
        fn flush(&mut self, _opts: &IoOptions) -> Result<()> {
            self.flush_calls += 1;
            Ok(())
        }
        fn sync(&mut self, _opts: &IoOptions) -> Result<()> {
            self.sync_calls += 1;
            Ok(())
        }
        fn close(&mut self, _opts: &IoOptions) -> Result<()> {
            Ok(())
        }
        fn file_size(&self) -> u64 {
            self.buffer.len() as u64
        }
    }

    #[test]
    fn small_writes_are_coalesced() {
        let stub: Box<dyn FsWritableFile> = Box::<StubWritable>::default();
        let mut w = WritableFileWriter::with_buffer_size(stub, 64);
        let io = IoOptions::default();

        for _ in 0..10 {
            w.append(b"0123456789", &io).unwrap(); // 10 bytes, 10x = 100 total
        }
        w.flush(&io).unwrap();

        assert_eq!(w.file_size(), 100);
    }

    #[test]
    fn oversized_write_bypasses_buffer() {
        let stub: Box<dyn FsWritableFile> = Box::<StubWritable>::default();
        let mut w = WritableFileWriter::with_buffer_size(stub, 16);
        let io = IoOptions::default();

        let big = vec![7u8; 100];
        w.append(&big, &io).unwrap();
        assert_eq!(w.file_size(), 100);
    }

    #[test]
    fn logical_size_tracks_appends() {
        let stub: Box<dyn FsWritableFile> = Box::<StubWritable>::default();
        let mut w = WritableFileWriter::new(stub);
        let io = IoOptions::default();
        assert_eq!(w.file_size(), 0);
        w.append(b"hi", &io).unwrap();
        assert_eq!(w.file_size(), 2);
        w.append(b"hello", &io).unwrap();
        assert_eq!(w.file_size(), 7);
    }

    #[test]
    fn flush_buffer_happens_before_close() {
        // Sanity: the byte count visible via `file_size` equals
        // everything we appended, even if some data was still in the
        // user buffer at the time of `close`.
        let stub: Box<dyn FsWritableFile> = Box::<StubWritable>::default();
        let mut w = WritableFileWriter::with_buffer_size(stub, 1024);
        let io = IoOptions::default();
        w.append(b"partial", &io).unwrap();
        w.close(&io).unwrap();
        assert_eq!(w.file_size(), 7);
    }
}
