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

//! Port of `env/flink/` — the ForSt-specific Flink filesystem bridge.
//!
//! This is **the** reason ForSt exists as a fork of RocksDB. Upstream
//! RocksDB talks to the OS via POSIX; ForSt routes I/O through
//! Apache Flink's `FileSystem` abstraction so state can be read
//! from and written to remote durable storage (S3, HDFS, OSS, GCS)
//! without going through a local filesystem.
//!
//! In the C++ codebase, the bridge works via JNI: `FlinkFileSystem`
//! (a C++ `FileSystemWrapper` subclass) calls into the JVM to
//! invoke methods on `org.apache.flink.core.fs.FileSystem`. In
//! this Rust port, we replace the JNI boundary with a **Rust trait
//! boundary** — [`backend::FlinkFsBackend`] — which can be backed
//! by:
//!
//! - [`mock_backend::InMemoryFsBackend`] — an in-memory mock for
//!   testing. This is the Rust equivalent of upstream's
//!   `java/flinktestmock/`.
//! - A future `JniFsBackend` that uses the [`jni`](https://crates.io/crates/jni)
//!   crate to call into a real Flink `FileSystem` instance. That
//!   implementation would add `jni` as an optional dependency
//!   behind a `feature = "jni"` gate.
//!
//! The [`flink_file_system::FlinkFileSystem`] struct implements
//! the Layer 0 [`crate::env::file_system::FileSystem`] trait by
//! delegating every method to the backend. The engine sees it as
//! just another `FileSystem` — it doesn't know or care whether
//! the backend is in-memory, JNI-backed, or something else.
//!
//! ## Relationship to the reading order
//!
//! Layer 7 in `FORST-READING-ORDER.md`:
//!
//! | Upstream file | Rust module |
//! |---|---|
//! | `env/flink/env_flink.{h,cc}` | [`flink_file_system`] |
//! | `env/flink/jni_helper.{h,cc}` | (replaced by the trait boundary) |
//! | `env/flink/jvm_util.{h,cc}` | (replaced by the trait boundary) |
//! | `java/flinktestmock/` | [`mock_backend`] |

pub mod backend;
pub mod compaction_filter;
pub mod flink_file_system;
pub mod mock_backend;
pub mod state_backend;
