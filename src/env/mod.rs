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
