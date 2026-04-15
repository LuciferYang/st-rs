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

//! SST file ingestion — injects pre-built SST files into the DB.
//!
//! Used by Flink during state restoration and rescaling: SST files
//! downloaded from a checkpoint are ingested directly rather than
//! replaying key-value pairs through the write path.
//!
//! # What's included
//!
//! - [`validate_sst`] — checks that an SST file has a valid footer
//!   and magic number.
//! - [`IngestExternalFileOptions`] — options controlling the ingest
//!   behavior (move vs copy, etc.).

use crate::core::status::{Result, Status};
use crate::env::file_system::FileSystem;
use crate::file::random_access_file_reader::RandomAccessFileReader;
use crate::sst::block_based::table_reader::BlockBasedTableReader;
use crate::sst::format::Footer;
use std::path::Path;
use std::sync::Arc;

/// Options for ingesting external SST files.
#[derive(Debug, Clone)]
pub struct IngestExternalFileOptions {
    /// If `true`, move the file instead of copying it. The original
    /// file is deleted after a successful ingest. Default: `false`.
    pub move_files: bool,
    /// If `true`, verify the file's checksum before ingesting.
    /// Default: `true`.
    pub verify_checksums: bool,
}

impl Default for IngestExternalFileOptions {
    fn default() -> Self {
        Self {
            move_files: false,
            verify_checksums: true,
        }
    }
}

/// Validate that a file at `path` is a valid SST by reading its
/// footer and checking the magic number.
pub fn validate_sst(fs: &dyn FileSystem, path: &Path) -> Result<()> {
    let file_size = fs.get_file_size(path)?;
    if file_size < Footer::ENCODED_LENGTH as u64 {
        return Err(Status::corruption(format!(
            "{}: file too small for SST footer ({file_size} bytes)",
            path.display()
        )));
    }

    let raw = fs.new_random_access_file(path, &Default::default())?;
    let reader = RandomAccessFileReader::new(raw, path.display().to_string())?;

    // Read footer.
    let mut footer_buf = vec![0u8; Footer::ENCODED_LENGTH];
    reader.read(
        file_size - Footer::ENCODED_LENGTH as u64,
        &mut footer_buf,
        &Default::default(),
    )?;
    Footer::decode_from(&footer_buf)?;

    Ok(())
}

/// Open an SST file and return a table reader. Used during ingest
/// to get key range metadata.
pub fn open_sst_for_ingest(
    fs: &dyn FileSystem,
    path: &Path,
) -> Result<Arc<BlockBasedTableReader>> {
    let raw = fs.new_random_access_file(path, &Default::default())?;
    let reader = RandomAccessFileReader::new(raw, path.display().to_string())?;
    let table = BlockBasedTableReader::open(Arc::new(reader))?;
    Ok(Arc::new(table))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_sst_rejects_empty_file() {
        let dir = std::env::temp_dir().join("st-rs-ingest-test");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("empty.sst");
        std::fs::write(&path, b"too short").unwrap();

        let fs = crate::env::posix::PosixFileSystem::new();
        let result = validate_sst(&fs, &path);
        assert!(result.is_err());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn validate_sst_accepts_valid_file() {
        use crate::file::writable_file_writer::WritableFileWriter;
        use crate::sst::block_based::table_builder::{
            BlockBasedTableBuilder, BlockBasedTableOptions,
        };

        let dir = std::env::temp_dir().join("st-rs-ingest-valid");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("valid.sst");

        let fs = crate::env::posix::PosixFileSystem::new();
        let writable = fs.new_writable_file(&path, &Default::default()).unwrap();
        let mut tb = BlockBasedTableBuilder::new(
            WritableFileWriter::new(writable),
            BlockBasedTableOptions::default(),
        );
        tb.add(b"key", b"value").unwrap();
        tb.finish().unwrap();

        assert!(validate_sst(&fs, &path).is_ok());

        let _ = std::fs::remove_dir_all(&dir);
    }
}
