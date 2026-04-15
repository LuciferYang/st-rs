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

//! Port of `file/filename.{h,cc}`.
//!
//! RocksDB stores every kind of file in a DB directory with a
//! deterministic naming scheme. `filename.cc` is the rosetta stone for
//! translating between file numbers and paths, and for detecting what
//! kind of file any given name refers to at recovery time.
//!
//! Naming conventions (from upstream `filename.cc`):
//!
//! | File kind    | Name pattern                   | Example         |
//! |--------------|--------------------------------|-----------------|
//! | WAL          | `{number:06}.log`              | `000042.log`    |
//! | SST          | `{number:06}.sst`              | `000042.sst`    |
//! | Blob         | `{number:06}.blob`             | `000042.blob`   |
//! | Manifest     | `MANIFEST-{number:06}`         | `MANIFEST-000042` |
//! | Current      | `CURRENT`                      | `CURRENT`       |
//! | Lock         | `LOCK`                         | `LOCK`          |
//! | Info log     | `LOG` (current) or `LOG.old.*` | `LOG`, `LOG.old.123` |
//! | Temp         | `{number:06}.dbtmp`            | `000042.dbtmp`  |
//! | Options      | `OPTIONS-{number:06}`          | `OPTIONS-000042`|
//! | Identity     | `IDENTITY`                     | `IDENTITY`      |
//!
//! Every function here is pure — no I/O, just string manipulation.

use crate::core::status::{Result, Status};
use crate::core::types::FileType;
use std::path::{Path, PathBuf};

/// Format a 64-bit file number in the six-digit zero-padded form used
/// by upstream. Matches `MakeFileName`.
fn format_file_number(number: u64) -> String {
    format!("{number:06}")
}

/// WAL file path: `{dir}/{number:06}.log`.
pub fn make_wal_file_name(dir: &Path, number: u64) -> PathBuf {
    dir.join(format!("{}.log", format_file_number(number)))
}

/// SST file path: `{dir}/{number:06}.sst`.
pub fn make_table_file_name(dir: &Path, number: u64) -> PathBuf {
    dir.join(format!("{}.sst", format_file_number(number)))
}

/// Blob file path: `{dir}/{number:06}.blob`.
pub fn make_blob_file_name(dir: &Path, number: u64) -> PathBuf {
    dir.join(format!("{}.blob", format_file_number(number)))
}

/// MANIFEST file path: `{dir}/MANIFEST-{number:06}`.
pub fn make_descriptor_file_name(dir: &Path, number: u64) -> PathBuf {
    dir.join(format!("MANIFEST-{}", format_file_number(number)))
}

/// OPTIONS file path: `{dir}/OPTIONS-{number:06}`.
pub fn make_options_file_name(dir: &Path, number: u64) -> PathBuf {
    dir.join(format!("OPTIONS-{}", format_file_number(number)))
}

/// CURRENT file path: `{dir}/CURRENT`.
pub fn make_current_file_name(dir: &Path) -> PathBuf {
    dir.join("CURRENT")
}

/// LOCK file path: `{dir}/LOCK`.
pub fn make_lock_file_name(dir: &Path) -> PathBuf {
    dir.join("LOCK")
}

/// IDENTITY file path: `{dir}/IDENTITY`.
pub fn make_identity_file_name(dir: &Path) -> PathBuf {
    dir.join("IDENTITY")
}

/// Current info log path: `{dir}/LOG`.
pub fn make_info_log_file_name(dir: &Path) -> PathBuf {
    dir.join("LOG")
}

/// Rolled info log path: `{dir}/LOG.old.{ts}`.
pub fn make_old_info_log_file_name(dir: &Path, ts_micros: u64) -> PathBuf {
    dir.join(format!("LOG.old.{ts_micros}"))
}

/// Temp file path: `{dir}/{number:06}.dbtmp`.
pub fn make_temp_file_name(dir: &Path, number: u64) -> PathBuf {
    dir.join(format!("{}.dbtmp", format_file_number(number)))
}

/// Parse an arbitrary filename (no directory component) and return its
/// [`FileType`] and, where applicable, the file number. For files that
/// don't carry a number (CURRENT, LOCK, IDENTITY, LOG), the returned
/// number is `0`.
///
/// Returns an `InvalidArgument` `Status` if the name doesn't match any
/// known pattern.
pub fn parse_file_name(name: &str) -> Result<(FileType, u64)> {
    // Bare names first — no number.
    match name {
        "CURRENT" => return Ok((FileType::CurrentFile, 0)),
        "LOCK" => return Ok((FileType::DbLockFile, 0)),
        "IDENTITY" => return Ok((FileType::IdentityFile, 0)),
        "LOG" => return Ok((FileType::InfoLogFile, 0)),
        _ => {}
    }

    // Old info log: LOG.old.<ts>
    if let Some(rest) = name.strip_prefix("LOG.old.") {
        let _ts: u64 = rest
            .parse()
            .map_err(|_| Status::invalid_argument(format!("bad old log name: {name}")))?;
        return Ok((FileType::InfoLogFile, 0));
    }

    // MANIFEST-<num>
    if let Some(rest) = name.strip_prefix("MANIFEST-") {
        let num: u64 = rest
            .parse()
            .map_err(|_| Status::invalid_argument(format!("bad manifest name: {name}")))?;
        return Ok((FileType::DescriptorFile, num));
    }

    // OPTIONS-<num>
    if let Some(rest) = name.strip_prefix("OPTIONS-") {
        let num: u64 = rest
            .parse()
            .map_err(|_| Status::invalid_argument(format!("bad options name: {name}")))?;
        return Ok((FileType::OptionsFile, num));
    }

    // Everything else has the form `{digits}.{suffix}`.
    let dot = name
        .find('.')
        .ok_or_else(|| Status::invalid_argument(format!("no suffix in: {name}")))?;
    let (num_part, suffix) = name.split_at(dot);
    let suffix = &suffix[1..]; // drop the dot

    let num: u64 = num_part
        .parse()
        .map_err(|_| Status::invalid_argument(format!("bad file number: {name}")))?;

    let ft = match suffix {
        "log" => FileType::WalFile,
        "sst" => FileType::TableFile,
        "blob" => FileType::BlobFile,
        "dbtmp" => FileType::TempFile,
        _ => {
            return Err(Status::invalid_argument(format!(
                "unknown file suffix: {name}"
            )))
        }
    };
    Ok((ft, num))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_pads_to_six_digits() {
        assert_eq!(format_file_number(0), "000000");
        assert_eq!(format_file_number(1), "000001");
        assert_eq!(format_file_number(42), "000042");
        assert_eq!(format_file_number(123_456), "123456");
        assert_eq!(format_file_number(1_234_567), "1234567");
    }

    #[test]
    fn make_functions_produce_expected_paths() {
        let dir = Path::new("/db");
        assert_eq!(
            make_wal_file_name(dir, 42),
            PathBuf::from("/db/000042.log")
        );
        assert_eq!(
            make_table_file_name(dir, 42),
            PathBuf::from("/db/000042.sst")
        );
        assert_eq!(
            make_descriptor_file_name(dir, 7),
            PathBuf::from("/db/MANIFEST-000007")
        );
        assert_eq!(
            make_options_file_name(dir, 7),
            PathBuf::from("/db/OPTIONS-000007")
        );
        assert_eq!(
            make_current_file_name(dir),
            PathBuf::from("/db/CURRENT")
        );
    }

    #[test]
    fn parse_numbered_files() {
        assert_eq!(parse_file_name("000042.log").unwrap(), (FileType::WalFile, 42));
        assert_eq!(
            parse_file_name("000042.sst").unwrap(),
            (FileType::TableFile, 42)
        );
        assert_eq!(
            parse_file_name("000042.blob").unwrap(),
            (FileType::BlobFile, 42)
        );
        assert_eq!(
            parse_file_name("000042.dbtmp").unwrap(),
            (FileType::TempFile, 42)
        );
    }

    #[test]
    fn parse_bare_files() {
        assert_eq!(parse_file_name("CURRENT").unwrap(), (FileType::CurrentFile, 0));
        assert_eq!(parse_file_name("LOCK").unwrap(), (FileType::DbLockFile, 0));
        assert_eq!(parse_file_name("IDENTITY").unwrap(), (FileType::IdentityFile, 0));
        assert_eq!(parse_file_name("LOG").unwrap(), (FileType::InfoLogFile, 0));
    }

    #[test]
    fn parse_prefix_files() {
        assert_eq!(
            parse_file_name("MANIFEST-000013").unwrap(),
            (FileType::DescriptorFile, 13)
        );
        assert_eq!(
            parse_file_name("OPTIONS-000013").unwrap(),
            (FileType::OptionsFile, 13)
        );
        assert_eq!(
            parse_file_name("LOG.old.1234567890").unwrap(),
            (FileType::InfoLogFile, 0)
        );
    }

    #[test]
    fn parse_rejects_junk() {
        assert!(parse_file_name("junk").is_err());
        assert!(parse_file_name("MANIFEST-abc").is_err());
        assert!(parse_file_name("abc.log").is_err());
        assert!(parse_file_name("000042.unknown").is_err());
    }

    #[test]
    fn round_trip_table_file() {
        let path = make_table_file_name(Path::new("/db"), 9999);
        let name = path.file_name().unwrap().to_str().unwrap();
        let (ft, num) = parse_file_name(name).unwrap();
        assert_eq!(ft, FileType::TableFile);
        assert_eq!(num, 9999);
    }
}
