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

//! Port of the Flink `FileSystem` Java API surface that
//! `env/flink/env_flink.cc` calls via JNI.
//!
//! In upstream C++, `FlinkFileSystem` calls JNI methods like
//! `CallObjectMethod(fs, "open", ...)`. In this Rust port, those
//! calls become method invocations on a [`FlinkFsBackend`] trait
//! object. The trait's methods mirror the Java
//! `org.apache.flink.core.fs.FileSystem` class — the names and
//! semantics are intentionally the same so a future JNI backend
//! can implement them as thin `env->CallObjectMethod` wrappers.
//!
//! The trait is `Send + Sync` because `FlinkFileSystem` is
//! installed as the engine's `FileSystem`, which must be
//! thread-safe.

use crate::core::status::Result;
use std::io::{Read, Write};

/// Metadata about a file or directory, matching Flink's
/// `FileStatus` Java class.
#[derive(Debug, Clone)]
pub struct FlinkFileStatus {
    /// Full path.
    pub path: String,
    /// File size in bytes. `0` for directories.
    pub length: u64,
    /// `true` if the path is a directory.
    pub is_dir: bool,
    /// Modification time in milliseconds since the UNIX epoch.
    pub modification_time: u64,
}

/// The Flink filesystem operations that `FlinkFileSystem` needs.
///
/// Each method mirrors a method on `org.apache.flink.core.fs.
/// FileSystem`:
///
/// | Rust method | Java method |
/// |---|---|
/// | `open` | `open(Path) → FSDataInputStream` |
/// | `create` | `create(Path) → FSDataOutputStream` |
/// | `exists` | `exists(Path) → boolean` |
/// | `list_status` | `listStatus(Path) → FileStatus[]` |
/// | `get_file_status` | `getFileStatus(Path) → FileStatus` |
/// | `delete` | `delete(Path, boolean) → boolean` |
/// | `mkdirs` | `mkdirs(Path) → boolean` |
/// | `rename` | `rename(Path, Path) → boolean` |
///
/// A future `JniFsBackend` will implement these by calling into
/// the JVM via the `jni` crate. The mock
/// [`crate::flink::mock_backend::InMemoryFsBackend`] implements
/// them with an in-memory `HashMap`.
pub trait FlinkFsBackend: Send + Sync {
    /// Open a file for sequential reading. Returns a boxed
    /// `Read` — the caller reads bytes sequentially, then drops
    /// the handle to close.
    fn open(&self, path: &str) -> Result<Box<dyn Read + Send>>;

    /// Create (or truncate) a file for writing. Returns a boxed
    /// `Write` — the caller writes bytes, then drops the handle
    /// to close.
    fn create(&self, path: &str) -> Result<Box<dyn Write + Send>>;

    /// Does `path` exist?
    fn exists(&self, path: &str) -> Result<bool>;

    /// List children of `path`. Returns a `FileStatus` for each
    /// direct child (not recursive).
    fn list_status(&self, path: &str) -> Result<Vec<FlinkFileStatus>>;

    /// Metadata of `path`.
    fn get_file_status(&self, path: &str) -> Result<FlinkFileStatus>;

    /// Delete `path`. If `recursive` and `path` is a directory,
    /// delete its contents too. Returns `true` if the path
    /// existed and was deleted.
    fn delete(&self, path: &str, recursive: bool) -> Result<bool>;

    /// Create `path` and every missing parent directory. Returns
    /// `true` if the directory exists after the call (either
    /// already existed or was created).
    fn mkdirs(&self, path: &str) -> Result<bool>;

    /// Atomically rename `src` to `dst`. Returns `true` on
    /// success.
    fn rename(&self, src: &str, dst: &str) -> Result<bool>;
}
