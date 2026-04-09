//! Port of `db/log_writer.{h,cc}`.
//!
//! Append-only writer for the WAL. The caller hands a payload to
//! [`LogWriter::add_record`]; the writer breaks it into one or more
//! 32 KiB-aligned framed records and pushes them through a Layer 2
//! [`WritableFileWriter`].
//!
//! The framing handles the case where a record is too large to fit
//! in the remaining space of the current block: the writer emits a
//! [`RecordType::First`] fragment with the leading bytes, then
//! [`RecordType::Middle`] fragments for full intermediate blocks,
//! then [`RecordType::Last`] for the trailing bytes. The reader
//! reassembles them.
//!
//! When the remaining space is smaller than the 7-byte header, the
//! writer pads it out with zeros so each block always begins at a
//! fresh `BLOCK_SIZE` boundary.

use crate::core::status::Result;
use crate::db::log_format::{RecordType, BLOCK_SIZE, HEADER_SIZE};
use crate::env::file_system::IoOptions;
use crate::file::writable_file_writer::WritableFileWriter;
use crate::util::crc32c::{crc32c, crc32c_extend, mask};

/// Append-only WAL writer.
pub struct LogWriter {
    file: WritableFileWriter,
    /// Bytes already written into the *current* block. Reset to 0
    /// each time we cross a block boundary.
    block_offset: usize,
}

impl LogWriter {
    /// Wrap a writable file. The file is assumed to be empty —
    /// callers reopening an existing WAL should create the writer
    /// only after replaying or truncating.
    pub fn new(file: WritableFileWriter) -> Self {
        Self {
            file,
            block_offset: 0,
        }
    }

    /// Append a record. The framing layer is invisible to callers —
    /// they hand in opaque payload bytes (typically a serialised
    /// `WriteBatch`), and the writer takes care of header, CRC,
    /// and block boundaries.
    ///
    /// On return, the bytes are in the writer's user buffer but not
    /// necessarily flushed to the OS. Call [`Self::sync`] for
    /// durability.
    pub fn add_record(&mut self, mut payload: &[u8]) -> Result<()> {
        let io = IoOptions::default();
        let mut begin = true;
        loop {
            let leftover = BLOCK_SIZE - self.block_offset;
            debug_assert!(leftover <= BLOCK_SIZE);

            // Pad the rest of the block if there isn't room for a
            // header. Each block always starts at offset 0 from the
            // reader's perspective.
            if leftover < HEADER_SIZE {
                if leftover > 0 {
                    let pad = [0u8; HEADER_SIZE];
                    self.file.append(&pad[..leftover], &io)?;
                }
                self.block_offset = 0;
            }

            let avail = BLOCK_SIZE - self.block_offset - HEADER_SIZE;
            let fragment_len = payload.len().min(avail);
            let end = fragment_len == payload.len();
            let record_type = match (begin, end) {
                (true, true) => RecordType::Full,
                (true, false) => RecordType::First,
                (false, true) => RecordType::Last,
                (false, false) => RecordType::Middle,
            };

            self.emit_physical_record(record_type, &payload[..fragment_len])?;
            payload = &payload[fragment_len..];
            begin = false;
            if payload.is_empty() {
                return Ok(());
            }
        }
    }

    /// Force the WAL to disk: flush user buffers, then `fsync`.
    pub fn sync(&mut self) -> Result<()> {
        let io = IoOptions::default();
        self.file.flush(&io)?;
        self.file.sync(&io)
    }

    /// Close the underlying file. The writer is unusable afterwards.
    pub fn close(mut self) -> Result<()> {
        self.file.close(&IoOptions::default())
    }

    /// Current logical file size (bytes written through the writer
    /// from the caller's perspective, including buffered data).
    pub fn file_size(&self) -> u64 {
        self.file.file_size()
    }

    fn emit_physical_record(&mut self, record_type: RecordType, payload: &[u8]) -> Result<()> {
        debug_assert!(payload.len() <= 0xffff);
        let io = IoOptions::default();

        // Compute CRC over [type byte][payload bytes].
        let mut crc = crc32c(&[record_type as u8]);
        crc = crc32c_extend(crc, payload);
        let masked = mask(crc);

        // Header buffer: [crc:4][len:2][type:1]
        let mut header = [0u8; HEADER_SIZE];
        let len = payload.len() as u16;
        header[0..4].copy_from_slice(&masked.to_le_bytes());
        header[4] = (len & 0xff) as u8;
        header[5] = (len >> 8) as u8;
        header[6] = record_type as u8;

        self.file.append(&header, &io)?;
        self.file.append(payload, &io)?;
        self.block_offset += HEADER_SIZE + payload.len();
        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::env::file_system::FsWritableFile;

    fn writer() -> (LogWriter, std::sync::Arc<std::sync::Mutex<Vec<u8>>>) {
        let buf = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let buf2 = buf.clone();
        struct Capture {
            inner: std::sync::Arc<std::sync::Mutex<Vec<u8>>>,
        }
        impl FsWritableFile for Capture {
            fn append(&mut self, data: &[u8], _: &IoOptions) -> Result<()> {
                self.inner.lock().unwrap().extend_from_slice(data);
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
                self.inner.lock().unwrap().len() as u64
            }
        }
        let file: Box<dyn FsWritableFile> = Box::new(Capture { inner: buf2 });
        let writer = WritableFileWriter::new(file);
        (LogWriter::new(writer), buf)
    }

    #[test]
    fn small_record_fits_in_one_block() {
        let (mut w, buf) = writer();
        w.add_record(b"hello").unwrap();
        w.sync().unwrap();
        let bytes = buf.lock().unwrap().clone();
        // 7-byte header + 5-byte payload = 12.
        assert_eq!(bytes.len(), 12);
        assert_eq!(bytes[6], RecordType::Full as u8);
    }

    #[test]
    fn record_larger_than_block_uses_first_middle_last() {
        let (mut w, buf) = writer();
        // 70 KiB payload spans more than two blocks.
        let payload = vec![0xa5u8; 70 * 1024];
        w.add_record(&payload).unwrap();
        w.sync().unwrap();
        let bytes = buf.lock().unwrap().clone();
        // First record at offset 0 should be RecordType::First.
        assert_eq!(bytes[6], RecordType::First as u8);
        // Beyond byte 32 KiB, the next header should start a Middle.
        assert_eq!(bytes[BLOCK_SIZE + 6], RecordType::Middle as u8);
        // After two full blocks, the third should be Last.
        assert_eq!(bytes[2 * BLOCK_SIZE + 6], RecordType::Last as u8);
    }

    #[test]
    fn block_padding_when_header_does_not_fit() {
        let (mut w, buf) = writer();
        // First record fills the block leaving 5 bytes free (less
        // than HEADER_SIZE = 7), forcing padding before the next
        // record.
        let payload = vec![0u8; BLOCK_SIZE - HEADER_SIZE - 5];
        w.add_record(&payload).unwrap();
        // Second record should land at offset BLOCK_SIZE (start of
        // a fresh block), not at the awkward middle position.
        w.add_record(b"x").unwrap();
        w.sync().unwrap();
        let bytes = buf.lock().unwrap().clone();
        // Verify byte at BLOCK_SIZE is the start of a Full record.
        assert_eq!(bytes[BLOCK_SIZE + 6], RecordType::Full as u8);
    }
}
