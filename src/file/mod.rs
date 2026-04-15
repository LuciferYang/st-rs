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
