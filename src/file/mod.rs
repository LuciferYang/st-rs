//! Port of `file/` from upstream.
//!
//! Buffered wrappers over the raw [`FileSystem`](crate::env::file_system::FileSystem)
//! per-file traits, plus filename conventions and small utility helpers.
//!
//! The engine rarely talks to [`FsRandomAccessFile`](crate::env::file_system::FsRandomAccessFile)
//! or [`FsWritableFile`](crate::env::file_system::FsWritableFile) directly;
//! it goes through [`random_access_file_reader::RandomAccessFileReader`]
//! and [`writable_file_writer::WritableFileWriter`] which add buffering,
//! range reads, and (in upstream) rate-limiting and checksum hooks.

pub mod file_util;
pub mod filename;
pub mod random_access_file_reader;
pub mod sequence_file_reader;
pub mod writable_file_writer;
