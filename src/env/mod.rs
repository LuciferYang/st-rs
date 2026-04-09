//! Environment and filesystem abstractions.
//!
//! `Env` is the RocksDB composition of a filesystem, a clock, and a
//! background thread pool. `FileSystem` is the newer split-out storage
//! abstraction that `FlinkFileSystem` plugs into.
//!
//! The concrete `Env` trait lives in [`env_trait`]; filesystem and file
//! traits live in [`file_system`]. Both are re-exported from this module's
//! root for convenience: users can write `crate::env::Env` or
//! `crate::env::FileSystem` without knowing the sub-file names.

pub mod env_trait;
pub mod file_system;
#[cfg(unix)]
pub mod posix;
pub mod thread_pool;

pub use env_trait::{Clock, CompositeEnv, Env, Priority, ScheduleHandle, SystemClockImpl, ThreadPool};
pub use file_system::{
    AccessPattern, FileAttributes, FileLock, FileSystem, FsDirectory, FsRandomAccessFile,
    FsSequentialFile, FsWritableFile, IoOptions, IoPriority, IoType,
};
#[cfg(unix)]
pub use posix::PosixFileSystem;
pub use thread_pool::StdThreadPool;
