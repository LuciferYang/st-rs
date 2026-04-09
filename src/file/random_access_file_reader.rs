//! Port of `file/random_access_file_reader.{h,cc}`.
//!
//! Buffered random-access reader over [`FsRandomAccessFile`]. Every
//! SST block-cache miss in upstream RocksDB goes through this wrapper,
//! which provides:
//!
//! 1. **Checksum hook** — on by default in upstream. Layer 2 omits it
//!    until `util::hash` lands a wire-compatible CRC32C / xxHash.
//! 2. **Readahead / prefetch buffer** — a single-window readahead
//!    buffer that absorbs adjacent small reads. Upstream has a more
//!    elaborate `FilePrefetchBuffer` (exponentially growing window,
//!    async readahead) that lives in a separate file; we expose the
//!    simpler "last window" form here and defer the exponential
//!    readahead to a follow-up.
//! 3. **Rate-limiter hook** — also not wired at Layer 2.
//!
//! The public surface is a single `read` method that takes an offset +
//! length and copies into a caller-provided slice. Multiple threads
//! may call `read` concurrently on the same reader, so the trait is
//! `Sync`; the internal readahead buffer is guarded by a mutex.

use crate::core::status::{Result, Status};
use crate::env::file_system::{FsRandomAccessFile, IoOptions};
use std::sync::Mutex;

/// Default readahead window size: 256 KiB. Matches the conservative
/// upstream default; the engine may tune larger for iterator scans.
pub const DEFAULT_READAHEAD_SIZE: usize = 256 * 1024;

/// Buffered random-access reader.
///
/// Field ordering: the immutable state comes first, the mutable
/// readahead buffer at the end. The buffer is guarded by a mutex so
/// the reader as a whole remains `Sync`.
pub struct RandomAccessFileReader {
    file: Box<dyn FsRandomAccessFile>,
    /// Optional file name for error messages.
    file_name: String,
    /// Total file size (cached from the underlying file at construction).
    file_size: u64,
    /// Mutex-guarded readahead window. `None` means "no readahead
    /// buffered yet". Using `Mutex` rather than `RwLock` because we
    /// always mutate (refill) on miss.
    readahead: Mutex<ReadaheadBuffer>,
}

struct ReadaheadBuffer {
    /// Offset of byte 0 of `data` within the underlying file.
    offset: u64,
    /// Buffered bytes.
    data: Vec<u8>,
    /// Configured maximum size of the window.
    max_size: usize,
}

impl RandomAccessFileReader {
    /// Wrap `file` with the default readahead window.
    pub fn new(file: Box<dyn FsRandomAccessFile>, file_name: impl Into<String>) -> Result<Self> {
        Self::with_readahead(file, file_name, DEFAULT_READAHEAD_SIZE)
    }

    /// Wrap `file` with a custom readahead window size. Passing `0`
    /// disables readahead entirely — every `read` goes straight to
    /// the underlying file.
    pub fn with_readahead(
        file: Box<dyn FsRandomAccessFile>,
        file_name: impl Into<String>,
        readahead: usize,
    ) -> Result<Self> {
        let file_size = file.size()?;
        Ok(Self {
            file,
            file_name: file_name.into(),
            file_size,
            readahead: Mutex::new(ReadaheadBuffer {
                offset: 0,
                data: Vec::new(),
                max_size: readahead,
            }),
        })
    }

    /// File size as of construction.
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    /// File name for diagnostics.
    pub fn file_name(&self) -> &str {
        &self.file_name
    }

    /// Read `buf.len()` bytes starting at `offset`. Returns the number
    /// of bytes actually placed in `buf`; may be less than `buf.len()`
    /// only at end of file.
    pub fn read(&self, offset: u64, buf: &mut [u8], opts: &IoOptions) -> Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        if offset >= self.file_size {
            return Ok(0);
        }

        // Cap read at file end.
        let capped_len = ((self.file_size - offset) as usize).min(buf.len());
        let buf = &mut buf[..capped_len];

        // Try the readahead buffer first.
        {
            let mut readahead = self
                .readahead
                .lock()
                .expect("RandomAccessFileReader readahead mutex poisoned");

            if readahead.max_size == 0 || readahead.data.is_empty() {
                // Fall through to direct read (and refill).
            } else if offset >= readahead.offset
                && offset + buf.len() as u64 <= readahead.offset + readahead.data.len() as u64
            {
                // Fully served from the readahead window.
                let start = (offset - readahead.offset) as usize;
                buf.copy_from_slice(&readahead.data[start..start + buf.len()]);
                return Ok(buf.len());
            }

            // Miss. If the window is enabled and the request fits,
            // refill the window starting at `offset` and serve from it.
            if readahead.max_size >= buf.len() {
                let window_size = (readahead.max_size as u64)
                    .min(self.file_size - offset) as usize;
                let mut new_window = vec![0u8; window_size];
                let n = self.file.read_at(offset, &mut new_window, opts)?;
                new_window.truncate(n);
                if n < buf.len() {
                    return Err(Status::corruption(format!(
                        "{}: short read at offset {} (requested {}, got {})",
                        self.file_name,
                        offset,
                        buf.len(),
                        n
                    )));
                }
                buf.copy_from_slice(&new_window[..buf.len()]);
                readahead.offset = offset;
                readahead.data = new_window;
                return Ok(buf.len());
            }
        }

        // Request larger than the readahead window → direct read.
        self.file.read_at(offset, buf, opts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// A stub `FsRandomAccessFile` backed by an owned `Vec`. Counts
    /// `read_at` invocations so tests can verify the readahead buffer
    /// is actually saving syscalls.
    struct StubRa {
        data: Vec<u8>,
        reads: AtomicU32,
    }

    impl FsRandomAccessFile for StubRa {
        fn read_at(&self, offset: u64, buf: &mut [u8], _opts: &IoOptions) -> Result<usize> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            let start = offset as usize;
            if start >= self.data.len() {
                return Ok(0);
            }
            let end = (start + buf.len()).min(self.data.len());
            let n = end - start;
            buf[..n].copy_from_slice(&self.data[start..end]);
            Ok(n)
        }
        fn size(&self) -> Result<u64> {
            Ok(self.data.len() as u64)
        }
    }

    #[test]
    fn read_whole_file_via_readahead() {
        let stub: Box<dyn FsRandomAccessFile> = Box::new(StubRa {
            data: (0..100u8).collect(),
            reads: AtomicU32::new(0),
        });
        let reader = RandomAccessFileReader::new(stub, "test").unwrap();
        let mut buf = [0u8; 50];
        let n = reader.read(0, &mut buf, &IoOptions::default()).unwrap();
        assert_eq!(n, 50);
        assert_eq!(&buf[..], (0..50u8).collect::<Vec<_>>().as_slice());
    }

    #[test]
    fn subsequent_adjacent_reads_return_correct_bytes() {
        // We can't directly observe the underlying `read_at` call
        // count without shared mutable state, but we can at least
        // verify the buffered reader returns the right bytes across
        // multiple small sequential reads.
        let boxed: Box<dyn FsRandomAccessFile> = Box::new(StubRa {
            data: (0..200u8).collect(),
            reads: AtomicU32::new(0),
        });
        let reader = RandomAccessFileReader::with_readahead(boxed, "t", 256).unwrap();
        let mut buf = [0u8; 10];
        reader.read(0, &mut buf, &IoOptions::default()).unwrap();
        assert_eq!(buf, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        reader.read(10, &mut buf, &IoOptions::default()).unwrap();
        assert_eq!(buf, [10, 11, 12, 13, 14, 15, 16, 17, 18, 19]);
        reader.read(20, &mut buf, &IoOptions::default()).unwrap();
        assert_eq!(buf, [20, 21, 22, 23, 24, 25, 26, 27, 28, 29]);
    }

    #[test]
    fn read_past_eof_returns_zero() {
        let stub: Box<dyn FsRandomAccessFile> = Box::new(StubRa {
            data: vec![0; 10],
            reads: AtomicU32::new(0),
        });
        let reader = RandomAccessFileReader::new(stub, "eof").unwrap();
        let mut buf = [0u8; 5];
        let n = reader.read(100, &mut buf, &IoOptions::default()).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn read_straddling_eof_returns_partial() {
        let stub: Box<dyn FsRandomAccessFile> = Box::new(StubRa {
            data: vec![9; 10],
            reads: AtomicU32::new(0),
        });
        let reader = RandomAccessFileReader::new(stub, "partial").unwrap();
        let mut buf = [0u8; 20];
        let n = reader.read(5, &mut buf, &IoOptions::default()).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf[..n], &[9, 9, 9, 9, 9]);
    }

    #[test]
    fn request_larger_than_window_goes_direct() {
        let stub: Box<dyn FsRandomAccessFile> = Box::new(StubRa {
            data: (0..200u8).collect(),
            reads: AtomicU32::new(0),
        });
        let reader = RandomAccessFileReader::with_readahead(stub, "t", 32).unwrap();
        let mut buf = [0u8; 100];
        let n = reader.read(0, &mut buf, &IoOptions::default()).unwrap();
        assert_eq!(n, 100);
        assert_eq!(&buf[..], (0..100u8).collect::<Vec<_>>().as_slice());
    }
}
