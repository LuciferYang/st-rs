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

//! Port of `include/rocksdb/file_system.h`.
//!
//! This is **the** integration point for `FlinkFileSystem` in ForSt. The
//! upstream C++ `FileSystem` is a big abstract class with ~30 virtual
//! methods; the Rust port trims it to the subset that the engine actually
//! calls, keeping each method focused on a single operation so alternative
//! backends (local POSIX, Flink JVM, in-memory, test mock) can implement
//! just what they need.
//!
//! Method return type: every I/O call returns [`crate::status::Result<T>`],
//! whose error variant is a [`crate::status::Status`] with `code ==
//! Code::IoError` (or any other appropriate code). At the documentation
//! level the alias [`crate::status::IoStatus`] flags "this came from the
//! filesystem layer" — upstream uses a subclass for the same purpose.
//!
//! Thread-safety: every `FileSystem` implementation must be safe for
//! concurrent calls from multiple threads. Per-file objects
//! (`FsSequentialFile` / `FsRandomAccessFile` / `FsWritableFile`) are *not*
//! required to be thread-safe — the engine owns them behind a mutex or
//! thread-local as needed. `FsRandomAccessFile` is the one exception: it
//! must be safe for *concurrent reads* so the block cache miss path can
//! parallelise.

use crate::api::options::FileOptions;
use crate::core::status::{Result, Status};
use crate::core::types::Temperature;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// I/O priority hint passed to the FS per request. Maps to upstream
/// `IOPriority`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum IoPriority {
    /// Background work (compaction, flush).
    #[default]
    Low,
    /// Foreground work (user read, WAL sync).
    High,
}

/// Type of the data being read or written. Lets advanced filesystems
/// optimise placement. Mirrors upstream `IOType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[allow(missing_docs)]
pub enum IoType {
    Data,
    Filter,
    Index,
    Metadata,
    Wal,
    Manifest,
    Log,
    #[default]
    Unknown,
}

/// Per-request I/O options. Mirrors a trimmed `IOOptions` from upstream.
#[derive(Debug, Clone, Default)]
pub struct IoOptions {
    /// Timeout for the operation. `Duration::ZERO` means "no timeout".
    pub timeout: Duration,
    /// Priority of the request.
    pub rate_limiter_priority: IoPriority,
    /// Type of data for placement hints.
    pub io_type: IoType,
    /// Force `fsync` on directory metadata. Needed on btrfs.
    pub force_dir_fsync: bool,
    /// Skip recursing into subdirectories in `get_children`.
    pub do_not_recurse: bool,
}

/// Advisory access pattern hint for [`FsRandomAccessFile::hint`]. Mirrors
/// upstream `RandomAccessFile::AccessPattern`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[allow(missing_docs)]
pub enum AccessPattern {
    Normal,
    Random,
    Sequential,
    WillNeed,
    WontNeed,
}

/// Attributes of a file returned by [`FileSystem::get_children_with_attributes`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileAttributes {
    /// File name relative to the directory.
    pub name: String,
    /// Size in bytes.
    pub size_bytes: u64,
}

/// Opaque lock handle returned by [`FileSystem::lock_file`].
///
/// Implementations typically store OS-level state here (fd, named lock, …).
/// The trait object is consumed when the lock is released.
pub trait FileLock: Send + Sync {}

// ----------------------------------------------------------------------------
// Per-file traits
// ----------------------------------------------------------------------------

/// A file opened for sequential reading (e.g. WAL replay, MANIFEST scan).
///
/// Mirrors `FSSequentialFile`. Not required to be thread-safe.
pub trait FsSequentialFile: Send {
    /// Read up to `buf.len()` bytes starting at the current position.
    /// Returns the number of bytes actually read (0 == EOF).
    fn read(&mut self, buf: &mut [u8], opts: &IoOptions) -> Result<usize>;

    /// Skip forward `n` bytes. Cheaper than reading and discarding.
    fn skip(&mut self, n: u64, opts: &IoOptions) -> Result<()>;

    /// Report whether the underlying implementation is using direct I/O.
    fn use_direct_io(&self) -> bool {
        false
    }

    /// Required buffer alignment for direct I/O. Ignored unless
    /// [`Self::use_direct_io`] returns `true`.
    fn required_buffer_alignment(&self) -> usize {
        crate::port::DEFAULT_PAGE_SIZE
    }
}

/// A file opened for random-access reads (e.g. SST block reads).
///
/// Mirrors `FSRandomAccessFile`. Must be safe for *concurrent reads* from
/// multiple threads — the engine does not serialise per-file read calls
/// for random-access files, unlike sequential files.
pub trait FsRandomAccessFile: Send + Sync {
    /// Read up to `buf.len()` bytes starting at `offset`. Returns the
    /// number of bytes actually read; may return fewer than requested only
    /// when EOF is reached.
    fn read_at(&self, offset: u64, buf: &mut [u8], opts: &IoOptions) -> Result<usize>;

    /// Prefetch a region of the file into the OS page cache. Default:
    /// returns `NotSupported`, matching upstream.
    fn prefetch(&self, _offset: u64, _n: u64, _opts: &IoOptions) -> Result<()> {
        Err(Status::not_supported("Prefetch"))
    }

    /// Advisory access pattern hint.
    fn hint(&self, _pattern: AccessPattern) {}

    /// Is the file using direct I/O?
    fn use_direct_io(&self) -> bool {
        false
    }

    /// Alignment required for direct I/O buffers.
    fn required_buffer_alignment(&self) -> usize {
        crate::port::DEFAULT_PAGE_SIZE
    }

    /// Total file size.
    fn size(&self) -> Result<u64>;
}

/// A file opened for sequential writing (WAL, SST builder, MANIFEST writer).
///
/// Mirrors `FSWritableFile`. Not required to be thread-safe — the engine
/// owns exactly one writer per file.
pub trait FsWritableFile: Send {
    /// Append `data` to the end of the file.
    fn append(&mut self, data: &[u8], opts: &IoOptions) -> Result<()>;

    /// Position-tagged append. Default: returns `NotSupported`. Override
    /// when direct I/O or `O_APPEND` semantics require sector-aligned writes.
    fn positioned_append(
        &mut self,
        _data: &[u8],
        _offset: u64,
        _opts: &IoOptions,
    ) -> Result<()> {
        Err(Status::not_supported("PositionedAppend"))
    }

    /// Truncate the file to `size` bytes.
    fn truncate(&mut self, _size: u64, _opts: &IoOptions) -> Result<()> {
        Ok(())
    }

    /// Flush user-space buffers to the OS. Does not `fsync`.
    fn flush(&mut self, opts: &IoOptions) -> Result<()>;

    /// `fsync` file data.
    fn sync(&mut self, opts: &IoOptions) -> Result<()>;

    /// `fsync` both data and metadata. Default: same as [`Self::sync`].
    fn fsync(&mut self, opts: &IoOptions) -> Result<()> {
        self.sync(opts)
    }

    /// Close the file. The engine treats the file as closed regardless of
    /// return status; implementations must still release resources on error.
    fn close(&mut self, opts: &IoOptions) -> Result<()>;

    /// Current file size.
    fn file_size(&self) -> u64;

    /// Is the file using direct I/O?
    fn use_direct_io(&self) -> bool {
        false
    }

    /// Alignment required for direct I/O writes.
    fn required_buffer_alignment(&self) -> usize {
        crate::port::DEFAULT_PAGE_SIZE
    }
}

/// Handle to an open directory. Mirrors `FSDirectory`.
///
/// Used to `fsync` a directory after file renames/deletes — some filesystems
/// require this for durability. Not thread-safe.
pub trait FsDirectory: Send {
    /// `fsync` the directory metadata.
    fn fsync(&mut self, opts: &IoOptions) -> Result<()>;

    /// Close the directory handle.
    fn close(&mut self, opts: &IoOptions) -> Result<()>;
}

// ----------------------------------------------------------------------------
// FileSystem trait
// ----------------------------------------------------------------------------

/// The storage abstraction the LSM engine talks to. Mirrors upstream
/// `FileSystem`, trimmed to the methods the engine actually calls.
///
/// **This is the trait `FlinkFileSystem` implements in ForSt.** An
/// implementation that forwards every call to a Java `org.apache.flink.
/// core.fs.FileSystem` instance via JNI is the Layer 7 port of
/// `env/flink/env_flink.cc`.
///
/// Thread-safety: every implementation must be `Send + Sync` and safe for
/// concurrent calls from multiple threads.
pub trait FileSystem: Send + Sync {
    /// A stable, human-readable name (for logs and introspection).
    fn name(&self) -> &'static str;

    // -- File open --------------------------------------------------------

    /// Open a file for sequential reading.
    fn new_sequential_file(
        &self,
        path: &Path,
        opts: &FileOptions,
    ) -> Result<Box<dyn FsSequentialFile>>;

    /// Open a file for random-access reading.
    fn new_random_access_file(
        &self,
        path: &Path,
        opts: &FileOptions,
    ) -> Result<Box<dyn FsRandomAccessFile>>;

    /// Create or truncate a file for writing.
    fn new_writable_file(
        &self,
        path: &Path,
        opts: &FileOptions,
    ) -> Result<Box<dyn FsWritableFile>>;

    /// Open a file for writing *in append mode*. Must exist.
    fn reopen_writable_file(
        &self,
        path: &Path,
        opts: &FileOptions,
    ) -> Result<Box<dyn FsWritableFile>>;

    /// Open a directory for fsync.
    fn new_directory(&self, path: &Path) -> Result<Box<dyn FsDirectory>>;

    // -- Metadata ---------------------------------------------------------

    /// Does `path` exist?
    fn file_exists(&self, path: &Path) -> Result<bool>;

    /// List the names inside `dir`, excluding `.` and `..`. Sub-directories
    /// are returned with a trailing `/`.
    fn get_children(&self, dir: &Path) -> Result<Vec<String>>;

    /// Like [`Self::get_children`] but also returns size metadata.
    fn get_children_with_attributes(&self, dir: &Path) -> Result<Vec<FileAttributes>>;

    /// Size of `path` in bytes.
    fn get_file_size(&self, path: &Path) -> Result<u64>;

    /// Modification time in seconds since the UNIX epoch.
    fn get_file_modification_time(&self, path: &Path) -> Result<u64>;

    /// Is `path` a directory?
    fn is_directory(&self, path: &Path) -> Result<bool>;

    // -- Mutation ---------------------------------------------------------

    /// Create `dir` including parent directories as needed.
    fn create_dir(&self, dir: &Path) -> Result<()>;

    /// Same as [`Self::create_dir`] but succeeds if `dir` already exists.
    fn create_dir_if_missing(&self, dir: &Path) -> Result<()>;

    /// Remove an empty directory.
    fn delete_dir(&self, dir: &Path) -> Result<()>;

    /// Remove a file.
    fn delete_file(&self, path: &Path) -> Result<()>;

    /// Atomically rename `src` to `dst`. If `dst` exists it is replaced.
    fn rename_file(&self, src: &Path, dst: &Path) -> Result<()>;

    /// Create a hard link from `src` to `dst`. On filesystems without hard
    /// link support, implementations should return `NotSupported` and the
    /// caller will fall back to copy-and-delete.
    fn link_file(&self, src: &Path, dst: &Path) -> Result<()>;

    // -- Locking ----------------------------------------------------------

    /// Acquire an advisory lock on `path`, creating it if needed. Typically
    /// used on the DB's LOCK file to prevent multiple processes from
    /// opening the same DB.
    fn lock_file(&self, path: &Path) -> Result<Box<dyn FileLock>>;

    /// Release an advisory lock previously obtained via [`Self::lock_file`].
    fn unlock_file(&self, lock: Box<dyn FileLock>) -> Result<()>;

    // -- Misc -------------------------------------------------------------

    /// Return an absolute path for the given relative path, resolving
    /// against the FS's current working directory. Only used by `Db::open`
    /// to normalise paths in error messages.
    fn get_absolute_path(&self, db_path: &Path) -> Result<PathBuf> {
        Ok(db_path.to_path_buf())
    }

    /// Temperature hint override. Most filesystems don't care.
    fn get_temperature(&self, _path: &Path) -> Result<Temperature> {
        Ok(Temperature::Unknown)
    }
}
