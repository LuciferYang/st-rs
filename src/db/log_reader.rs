//! Port of `db/log_reader.{h,cc}`.
//!
//! Replays records written by [`crate::db::log_writer::LogWriter`].
//! Reassembles `First`/`Middle`/`Last` fragments into complete
//! payloads, verifies each header's CRC, and surfaces corruption
//! lazily through the per-record `read_record` call.
//!
//! On a torn block (CRC mismatch, truncated header, unknown record
//! type), the reader logs the problem via the optional reporter
//! callback and skips to the next block — matching upstream's
//! `WALRecoveryMode::kSkipAnyCorruptedRecords` behaviour. Strict
//! recovery is a Layer 4b refinement.

use crate::core::status::{Result, Status};
use crate::db::log_format::{RecordType, BLOCK_SIZE, HEADER_SIZE};
use crate::env::file_system::IoOptions;
use crate::file::sequence_file_reader::SequentialFileReader;
use crate::util::crc32c::{crc32c, crc32c_extend, mask};

/// Replay reader. Holds a [`SequentialFileReader`] over the WAL
/// and a 32 KiB scratch buffer that backs the current block.
pub struct LogReader {
    file: SequentialFileReader,
    /// Current block buffer. Re-filled as we cross block boundaries.
    block: Vec<u8>,
    /// Bytes valid within `block` (`<= BLOCK_SIZE`).
    block_len: usize,
    /// Cursor inside `block` of the next byte to consume.
    block_pos: usize,
    /// Set when the underlying file has returned a short block,
    /// indicating we've hit EOF.
    eof: bool,
}

impl LogReader {
    /// Wrap a sequential reader over the WAL.
    pub fn new(file: SequentialFileReader) -> Self {
        Self {
            file,
            block: vec![0u8; BLOCK_SIZE],
            block_len: 0,
            block_pos: 0,
            eof: false,
        }
    }

    /// Read the next complete record into `out`. Returns:
    /// - `Ok(true)`  — `out` was overwritten with a fresh payload.
    /// - `Ok(false)` — clean EOF, no more records.
    /// - `Err(_)`    — I/O error from the underlying file.
    ///
    /// Corrupted physical records are skipped silently; the next
    /// good record is returned.
    pub fn read_record(&mut self, out: &mut Vec<u8>) -> Result<bool> {
        out.clear();
        let mut in_fragmented_record = false;
        loop {
            match self.read_physical_record()? {
                PhysicalRecord::Full(payload) => {
                    if in_fragmented_record {
                        // Recovery from a torn record: discard the
                        // partial buffer and start fresh.
                        out.clear();
                    }
                    out.extend_from_slice(payload);
                    return Ok(true);
                }
                PhysicalRecord::First(payload) => {
                    if in_fragmented_record {
                        out.clear();
                    }
                    in_fragmented_record = true;
                    out.extend_from_slice(payload);
                }
                PhysicalRecord::Middle(payload) => {
                    if !in_fragmented_record {
                        // Spurious middle without a first — skip.
                        out.clear();
                        continue;
                    }
                    out.extend_from_slice(payload);
                }
                PhysicalRecord::Last(payload) => {
                    if !in_fragmented_record {
                        out.clear();
                        continue;
                    }
                    out.extend_from_slice(payload);
                    return Ok(true);
                }
                PhysicalRecord::Eof => {
                    return Ok(false);
                }
                PhysicalRecord::BadHeader | PhysicalRecord::BadChecksum => {
                    // Discard whatever we have and try again.
                    out.clear();
                    in_fragmented_record = false;
                }
            }
        }
    }

    /// Read one physical record from the current block. May refill
    /// the block from the file as needed.
    fn read_physical_record(&mut self) -> Result<PhysicalRecord<'_>> {
        loop {
            // Need at least HEADER_SIZE bytes left in the block.
            let remaining = self.block_len.saturating_sub(self.block_pos);
            if remaining < HEADER_SIZE {
                if !self.refill_block()? {
                    return Ok(PhysicalRecord::Eof);
                }
                continue;
            }

            let header = &self.block[self.block_pos..self.block_pos + HEADER_SIZE];
            let masked = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
            let len = (header[4] as usize) | ((header[5] as usize) << 8);
            let type_byte = header[6];

            // The record (header + payload) must fit entirely in
            // what's left of the block — otherwise the block is
            // corrupt.
            if HEADER_SIZE + len > self.block_len - self.block_pos {
                self.block_pos = self.block_len; // skip to next block
                return Ok(PhysicalRecord::BadHeader);
            }

            // RecordType::Zero is padding — skip.
            let record_type = match RecordType::from_byte(type_byte) {
                Some(t) => t,
                None => {
                    self.block_pos += HEADER_SIZE + len;
                    return Ok(PhysicalRecord::BadHeader);
                }
            };
            if matches!(record_type, RecordType::Zero) {
                self.block_pos += HEADER_SIZE + len;
                continue;
            }

            // Verify CRC over [type byte][payload bytes].
            let payload_start = self.block_pos + HEADER_SIZE;
            let payload_end = payload_start + len;
            let payload = &self.block[payload_start..payload_end];
            let mut computed = crc32c(&[type_byte]);
            computed = crc32c_extend(computed, payload);
            if mask(computed) != masked {
                self.block_pos += HEADER_SIZE + len;
                return Ok(PhysicalRecord::BadChecksum);
            }

            self.block_pos = payload_end;
            return Ok(match record_type {
                RecordType::Full => PhysicalRecord::Full(payload),
                RecordType::First => PhysicalRecord::First(payload),
                RecordType::Middle => PhysicalRecord::Middle(payload),
                RecordType::Last => PhysicalRecord::Last(payload),
                RecordType::Zero => unreachable!(),
            });
        }
    }

    /// Refill the block buffer from the underlying file. Returns
    /// `true` if any new bytes were read, `false` on clean EOF.
    fn refill_block(&mut self) -> Result<bool> {
        if self.eof {
            return Ok(false);
        }
        let n = self
            .file
            .read(&mut self.block, &IoOptions::default())?;
        self.block_len = n;
        self.block_pos = 0;
        if n == 0 {
            self.eof = true;
            return Ok(false);
        }
        if n < BLOCK_SIZE {
            // Last (partial) block; mark EOF so the next refill is
            // a clean exit.
            self.eof = true;
        }
        Ok(true)
    }
}

/// Internal: result of attempting to read a single physical record.
/// The borrowed payload variants point into [`LogReader::block`].
enum PhysicalRecord<'a> {
    Full(&'a [u8]),
    First(&'a [u8]),
    Middle(&'a [u8]),
    Last(&'a [u8]),
    /// Clean end-of-file.
    Eof,
    /// Header is malformed (bad type byte or impossible length).
    BadHeader,
    /// CRC mismatch.
    BadChecksum,
}

/// Helper: surface a corruption error if the caller wants strict
/// recovery semantics.
#[allow(dead_code)]
pub(crate) fn corruption(msg: &str) -> Status {
    Status::corruption(msg.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::log_writer::LogWriter;
    use crate::env::file_system::{FsSequentialFile, FsWritableFile};
    use crate::file::sequence_file_reader::SequentialFileReader;
    use crate::file::writable_file_writer::WritableFileWriter;
    use std::sync::{Arc, Mutex};

    /// Shared in-memory file used for round-trip tests.
    #[derive(Default)]
    struct SharedFile {
        bytes: Mutex<Vec<u8>>,
    }

    struct WHandle {
        file: Arc<SharedFile>,
    }
    impl FsWritableFile for WHandle {
        fn append(&mut self, data: &[u8], _: &IoOptions) -> Result<()> {
            self.file.bytes.lock().unwrap().extend_from_slice(data);
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
            self.file.bytes.lock().unwrap().len() as u64
        }
    }

    /// Sequential reader half: tracks our own position so the
    /// underlying SequentialFileReader doesn't double-buffer.
    struct RHandle {
        file: Arc<SharedFile>,
        pos: usize,
    }
    impl FsSequentialFile for RHandle {
        fn read(&mut self, buf: &mut [u8], _: &IoOptions) -> Result<usize> {
            let inner = self.file.bytes.lock().unwrap();
            let avail = inner.len() - self.pos;
            let n = buf.len().min(avail);
            buf[..n].copy_from_slice(&inner[self.pos..self.pos + n]);
            self.pos += n;
            Ok(n)
        }
        fn skip(&mut self, n: u64, _: &IoOptions) -> Result<()> {
            let inner_len = self.file.bytes.lock().unwrap().len();
            self.pos = (self.pos + n as usize).min(inner_len);
            Ok(())
        }
    }

    fn make_writer_reader() -> (LogWriter, Box<dyn Fn() -> LogReader>) {
        let file = Arc::new(SharedFile::default());
        let writer_file: Box<dyn FsWritableFile> = Box::new(WHandle {
            file: Arc::clone(&file),
        });
        let writer = WritableFileWriter::new(writer_file);
        let log_writer = LogWriter::new(writer);

        let file_for_reader = Arc::clone(&file);
        let reader_factory: Box<dyn Fn() -> LogReader> = Box::new(move || {
            let r: Box<dyn FsSequentialFile> = Box::new(RHandle {
                file: Arc::clone(&file_for_reader),
                pos: 0,
            });
            // Use a small buffer in the SequentialFileReader so it
            // doesn't pre-buffer beyond what we want.
            let r = SequentialFileReader::with_buffer_size(r, "wal", 16);
            LogReader::new(r)
        });
        (log_writer, reader_factory)
    }

    #[test]
    fn round_trip_small_records() {
        let (mut writer, reader_factory) = make_writer_reader();
        for i in 0..10u32 {
            let payload = format!("record-{i}");
            writer.add_record(payload.as_bytes()).unwrap();
        }
        writer.sync().unwrap();

        let mut reader = reader_factory();
        let mut out = Vec::new();
        for i in 0..10u32 {
            assert!(reader.read_record(&mut out).unwrap());
            assert_eq!(out, format!("record-{i}").into_bytes());
        }
        assert!(!reader.read_record(&mut out).unwrap()); // EOF
    }

    #[test]
    fn round_trip_large_record_spans_blocks() {
        let (mut writer, reader_factory) = make_writer_reader();
        let payload = vec![0xa5u8; 100 * 1024];
        writer.add_record(&payload).unwrap();
        writer.sync().unwrap();

        let mut reader = reader_factory();
        let mut out = Vec::new();
        assert!(reader.read_record(&mut out).unwrap());
        assert_eq!(out, payload);
        assert!(!reader.read_record(&mut out).unwrap());
    }

    #[test]
    fn empty_log_reads_nothing() {
        let (_writer, reader_factory) = make_writer_reader();
        let mut reader = reader_factory();
        let mut out = Vec::new();
        assert!(!reader.read_record(&mut out).unwrap());
    }
}
