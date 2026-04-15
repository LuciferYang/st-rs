//! Port of `db/version_edit.{h,cc}`.
//!
//! A `VersionEdit` encodes a delta between two versions of the LSM
//! state: files added, files deleted, comparator name, sequence
//! counters, column family mutations, etc. Edits are serialised into
//! the MANIFEST log as tagged varint records, allowing forward and
//! backward compatibility as new tags are introduced.
//!
//! The on-disk encoding is a sequence of `(tag, value)` pairs where
//! the tag is a `varint32` and the value format depends on the tag.
//! Unknown tags cause a decode error — matching upstream behaviour for
//! tags that are not forwards-compatible.

#![warn(missing_docs)]

use crate::core::status::{Result, Status};
use crate::util::coding::{
    get_length_prefixed_slice, get_varint32, get_varint64, put_length_prefixed_slice, put_varint32,
    put_varint64,
};

// ---------------------------------------------------------------------------
// Tag constants — must match upstream `db/version_edit.h`.
// ---------------------------------------------------------------------------

const TAG_COMPARATOR: u32 = 1;
const TAG_LOG_NUMBER: u32 = 2;
const TAG_NEXT_FILE_NUMBER: u32 = 3;
const TAG_LAST_SEQUENCE: u32 = 4;
const TAG_DELETED_FILE: u32 = 5;
const TAG_NEW_FILE: u32 = 6;
const TAG_COLUMN_FAMILY: u32 = 7;
const TAG_COLUMN_FAMILY_ADD: u32 = 8;
const TAG_COLUMN_FAMILY_DROP: u32 = 9;

// ---------------------------------------------------------------------------
// FileMetaData
// ---------------------------------------------------------------------------

/// Metadata for a single SST file in the LSM tree.
///
/// This is a simplified version of the upstream `FileMetaData` — it
/// carries just enough to round-trip through the MANIFEST format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileMetaData {
    /// File number — the DB-unique identifier for this SST.
    pub number: u64,
    /// On-disk size of the file in bytes.
    pub file_size: u64,
    /// Smallest internal key contained in the file.
    pub smallest_key: Vec<u8>,
    /// Largest internal key contained in the file.
    pub largest_key: Vec<u8>,
}

// ---------------------------------------------------------------------------
// VersionEdit
// ---------------------------------------------------------------------------

/// A single delta to the LSM version state. All fields are optional;
/// only the tags that are set are serialised.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct VersionEdit {
    /// Name of the comparator used by the DB. Set once during initial
    /// creation.
    pub comparator_name: Option<String>,
    /// Current WAL log file number.
    pub log_number: Option<u64>,
    /// Next file number to allocate.
    pub next_file_number: Option<u64>,
    /// Last sequence number written.
    pub last_sequence: Option<u64>,
    /// Files deleted in this edit: `(level, file_number)`.
    pub deleted_files: Vec<(u32, u64)>,
    /// Files added in this edit: `(level, file_metadata)`.
    pub new_files: Vec<(u32, FileMetaData)>,
    /// Column family ID this edit applies to.
    pub column_family_id: Option<u32>,
    /// If set, this edit creates a new column family with the given name.
    pub is_column_family_add: Option<String>,
    /// If `true`, this edit drops a column family.
    pub is_column_family_drop: bool,
}

impl VersionEdit {
    /// Create a new, empty `VersionEdit`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Serialise this edit into `dst` as a sequence of tagged varint
    /// fields.
    pub fn encode_to(&self, dst: &mut Vec<u8>) {
        if let Some(ref name) = self.comparator_name {
            put_varint32(dst, TAG_COMPARATOR);
            put_length_prefixed_slice(dst, name.as_bytes());
        }
        if let Some(v) = self.log_number {
            put_varint32(dst, TAG_LOG_NUMBER);
            put_varint64(dst, v);
        }
        if let Some(v) = self.next_file_number {
            put_varint32(dst, TAG_NEXT_FILE_NUMBER);
            put_varint64(dst, v);
        }
        if let Some(v) = self.last_sequence {
            put_varint32(dst, TAG_LAST_SEQUENCE);
            put_varint64(dst, v);
        }
        for &(level, file_num) in &self.deleted_files {
            put_varint32(dst, TAG_DELETED_FILE);
            put_varint32(dst, level);
            put_varint64(dst, file_num);
        }
        for (level, meta) in &self.new_files {
            put_varint32(dst, TAG_NEW_FILE);
            put_varint32(dst, *level);
            put_varint64(dst, meta.number);
            put_varint64(dst, meta.file_size);
            put_length_prefixed_slice(dst, &meta.smallest_key);
            put_length_prefixed_slice(dst, &meta.largest_key);
        }
        if let Some(cf_id) = self.column_family_id {
            put_varint32(dst, TAG_COLUMN_FAMILY);
            put_varint32(dst, cf_id);
        }
        if let Some(ref cf_name) = self.is_column_family_add {
            put_varint32(dst, TAG_COLUMN_FAMILY_ADD);
            put_length_prefixed_slice(dst, cf_name.as_bytes());
        }
        if self.is_column_family_drop {
            put_varint32(dst, TAG_COLUMN_FAMILY_DROP);
        }
    }

    /// Deserialise a `VersionEdit` from its binary representation.
    ///
    /// Returns `Corruption` if the data is malformed or contains an
    /// unknown tag.
    pub fn decode_from(src: &[u8]) -> Result<VersionEdit> {
        let mut edit = VersionEdit::new();
        let mut input = src;

        while !input.is_empty() {
            let (tag, rest) = get_varint32(input)?;
            input = rest;

            match tag {
                TAG_COMPARATOR => {
                    let (slice, rest) = get_length_prefixed_slice(input)?;
                    edit.comparator_name = Some(
                        String::from_utf8(slice.to_vec())
                            .map_err(|_| Status::corruption("bad comparator name UTF-8"))?,
                    );
                    input = rest;
                }
                TAG_LOG_NUMBER => {
                    let (v, rest) = get_varint64(input)?;
                    edit.log_number = Some(v);
                    input = rest;
                }
                TAG_NEXT_FILE_NUMBER => {
                    let (v, rest) = get_varint64(input)?;
                    edit.next_file_number = Some(v);
                    input = rest;
                }
                TAG_LAST_SEQUENCE => {
                    let (v, rest) = get_varint64(input)?;
                    edit.last_sequence = Some(v);
                    input = rest;
                }
                TAG_DELETED_FILE => {
                    let (level, rest) = get_varint32(input)?;
                    let (file_num, rest) = get_varint64(rest)?;
                    edit.deleted_files.push((level, file_num));
                    input = rest;
                }
                TAG_NEW_FILE => {
                    let (level, rest) = get_varint32(input)?;
                    let (number, rest) = get_varint64(rest)?;
                    let (file_size, rest) = get_varint64(rest)?;
                    let (smallest, rest) = get_length_prefixed_slice(rest)?;
                    let (largest, rest) = get_length_prefixed_slice(rest)?;
                    edit.new_files.push((
                        level,
                        FileMetaData {
                            number,
                            file_size,
                            smallest_key: smallest.to_vec(),
                            largest_key: largest.to_vec(),
                        },
                    ));
                    input = rest;
                }
                TAG_COLUMN_FAMILY => {
                    let (cf_id, rest) = get_varint32(input)?;
                    edit.column_family_id = Some(cf_id);
                    input = rest;
                }
                TAG_COLUMN_FAMILY_ADD => {
                    let (slice, rest) = get_length_prefixed_slice(input)?;
                    edit.is_column_family_add = Some(
                        String::from_utf8(slice.to_vec())
                            .map_err(|_| Status::corruption("bad CF name UTF-8"))?,
                    );
                    input = rest;
                }
                TAG_COLUMN_FAMILY_DROP => {
                    edit.is_column_family_drop = true;
                }
                _ => {
                    return Err(Status::corruption(format!("unknown VersionEdit tag: {tag}")));
                }
            }
        }
        Ok(edit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: encode then decode, assert the round-trip is lossless.
    fn round_trip(edit: &VersionEdit) -> VersionEdit {
        let mut buf = Vec::new();
        edit.encode_to(&mut buf);
        VersionEdit::decode_from(&buf).expect("decode should succeed")
    }

    #[test]
    fn round_trip_empty() {
        let edit = VersionEdit::new();
        let decoded = round_trip(&edit);
        assert_eq!(edit, decoded);
    }

    #[test]
    fn round_trip_all_fields() {
        let edit = VersionEdit {
            comparator_name: Some("leveldb.BytewiseComparator".into()),
            log_number: Some(42),
            next_file_number: Some(100),
            last_sequence: Some(999),
            deleted_files: vec![(0, 10), (1, 20)],
            new_files: vec![
                (
                    0,
                    FileMetaData {
                        number: 50,
                        file_size: 4096,
                        smallest_key: b"aaa".to_vec(),
                        largest_key: b"zzz".to_vec(),
                    },
                ),
                (
                    1,
                    FileMetaData {
                        number: 51,
                        file_size: 8192,
                        smallest_key: b"bbb".to_vec(),
                        largest_key: b"yyy".to_vec(),
                    },
                ),
            ],
            column_family_id: Some(3),
            is_column_family_add: Some("my_cf".into()),
            is_column_family_drop: false,
        };
        let decoded = round_trip(&edit);
        assert_eq!(edit, decoded);
    }

    #[test]
    fn round_trip_new_files_only() {
        let edit = VersionEdit {
            new_files: vec![(
                0,
                FileMetaData {
                    number: 7,
                    file_size: 1024,
                    smallest_key: b"start".to_vec(),
                    largest_key: b"end".to_vec(),
                },
            )],
            ..Default::default()
        };
        let decoded = round_trip(&edit);
        assert_eq!(edit, decoded);
    }

    #[test]
    fn round_trip_deleted_files_only() {
        let edit = VersionEdit {
            deleted_files: vec![(0, 1), (0, 2), (1, 3)],
            ..Default::default()
        };
        let decoded = round_trip(&edit);
        assert_eq!(edit, decoded);
    }

    #[test]
    fn decode_rejects_unknown_tag() {
        let mut buf = Vec::new();
        // Valid tag + value followed by an unknown tag.
        put_varint32(&mut buf, TAG_LOG_NUMBER);
        put_varint64(&mut buf, 10);
        put_varint32(&mut buf, 255); // unknown tag
        let err = VersionEdit::decode_from(&buf).unwrap_err();
        assert!(err.is_corruption());
    }
}
