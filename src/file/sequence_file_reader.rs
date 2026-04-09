//! Port of `file/sequence_file_reader.{h,cc}`.
//!
//! Thin buffered wrapper around [`FsSequentialFile`]. Used by WAL
//! replay and MANIFEST scanning. Upstream's version has an optional
//! rate limiter hook and checksum verification; both are omitted at
//! Layer 2 until the rate limiter and CRC32C lands.

use crate::core::status::Result;
use crate::env::file_system::{FsSequentialFile, IoOptions};

/// Default buffer size: 32 KiB. Matches upstream's default for
/// sequential read buffers.
pub const DEFAULT_BUFFER_SIZE: usize = 32 * 1024;

/// Buffered sequential reader.
pub struct SequentialFileReader {
    file: Box<dyn FsSequentialFile>,
    file_name: String,
    /// Read-ahead buffer. `pos` points at the next byte to return;
    /// `len` is the number of valid bytes in `buf`.
    buf: Vec<u8>,
    pos: usize,
    len: usize,
}

impl SequentialFileReader {
    /// Wrap `file` with the default buffer size.
    pub fn new(file: Box<dyn FsSequentialFile>, file_name: impl Into<String>) -> Self {
        Self::with_buffer_size(file, file_name, DEFAULT_BUFFER_SIZE)
    }

    /// Wrap `file` with a custom buffer size.
    pub fn with_buffer_size(
        file: Box<dyn FsSequentialFile>,
        file_name: impl Into<String>,
        buffer_size: usize,
    ) -> Self {
        Self {
            file,
            file_name: file_name.into(),
            buf: vec![0u8; buffer_size.max(1)],
            pos: 0,
            len: 0,
        }
    }

    /// File name for diagnostics.
    pub fn file_name(&self) -> &str {
        &self.file_name
    }

    /// Read up to `out.len()` bytes into `out`. Returns the number of
    /// bytes actually read; `0` means end-of-file.
    pub fn read(&mut self, out: &mut [u8], opts: &IoOptions) -> Result<usize> {
        let mut total = 0usize;
        while total < out.len() {
            // Drain buffer first.
            if self.pos < self.len {
                let available = self.len - self.pos;
                let to_copy = (out.len() - total).min(available);
                out[total..total + to_copy]
                    .copy_from_slice(&self.buf[self.pos..self.pos + to_copy]);
                self.pos += to_copy;
                total += to_copy;
                continue;
            }
            // Buffer empty → refill.
            let n = self.file.read(&mut self.buf, opts)?;
            if n == 0 {
                break; // EOF
            }
            self.pos = 0;
            self.len = n;
        }
        Ok(total)
    }

    /// Skip forward `n` bytes. Drains the internal buffer first, then
    /// asks the underlying file to skip the remainder.
    pub fn skip(&mut self, mut n: u64, opts: &IoOptions) -> Result<()> {
        let buffered = (self.len - self.pos) as u64;
        if n <= buffered {
            self.pos += n as usize;
            return Ok(());
        }
        n -= buffered;
        self.pos = self.len; // drain
        self.file.skip(n, opts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Stub sequential file backed by an owned `Vec`. Tracks position
    /// internally.
    struct StubSeq {
        data: Vec<u8>,
        pos: usize,
    }

    impl FsSequentialFile for StubSeq {
        fn read(&mut self, buf: &mut [u8], _opts: &IoOptions) -> Result<usize> {
            let available = self.data.len() - self.pos;
            let n = buf.len().min(available);
            buf[..n].copy_from_slice(&self.data[self.pos..self.pos + n]);
            self.pos += n;
            Ok(n)
        }
        fn skip(&mut self, n: u64, _opts: &IoOptions) -> Result<()> {
            self.pos = (self.pos + n as usize).min(self.data.len());
            Ok(())
        }
    }

    #[test]
    fn read_whole_file() {
        let stub = StubSeq {
            data: (0..50u8).collect(),
            pos: 0,
        };
        let mut r = SequentialFileReader::new(Box::new(stub), "test");
        let mut buf = [0u8; 100];
        let n = r.read(&mut buf, &IoOptions::default()).unwrap();
        assert_eq!(n, 50);
        assert_eq!(&buf[..n], &(0..50u8).collect::<Vec<_>>()[..]);
    }

    #[test]
    fn read_multiple_chunks_spans_refills() {
        let stub = StubSeq {
            data: (0..100u8).collect(),
            pos: 0,
        };
        // Tiny internal buffer forces multiple refills.
        let mut r = SequentialFileReader::with_buffer_size(Box::new(stub), "t", 16);
        let mut out = vec![0u8; 100];
        let mut total = 0;
        while total < 100 {
            let n = r.read(&mut out[total..], &IoOptions::default()).unwrap();
            if n == 0 {
                break;
            }
            total += n;
        }
        assert_eq!(total, 100);
        assert_eq!(out, (0..100u8).collect::<Vec<_>>());
    }

    #[test]
    fn skip_advances_past_buffered_bytes() {
        let stub = StubSeq {
            data: (0..50u8).collect(),
            pos: 0,
        };
        let mut r = SequentialFileReader::new(Box::new(stub), "t");
        // Force the internal buffer to fill.
        let mut buf = [0u8; 5];
        r.read(&mut buf, &IoOptions::default()).unwrap();
        assert_eq!(&buf, &[0, 1, 2, 3, 4]);
        // Skip 10 bytes.
        r.skip(10, &IoOptions::default()).unwrap();
        r.read(&mut buf, &IoOptions::default()).unwrap();
        assert_eq!(&buf, &[15, 16, 17, 18, 19]);
    }
}
