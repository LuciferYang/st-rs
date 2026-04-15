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

//! Port of `include/rocksdb/write_batch.h`.
//!
//! A [`WriteBatch`] is an ordered list of mutations (Put / Delete /
//! SingleDelete / Merge / DeleteRange) that will be applied atomically to
//! the DB. The `Db::write` entry point takes a `WriteBatch`, which is
//! serialised to the WAL and then replayed into the memtable.
//!
//! Upstream serialises the batch into a compact byte stream up front so it
//! can be written to the WAL without a second pass. We preserve that
//! invariant here: every mutation on a `WriteBatch` mutates the `contents`
//! vector directly. The exact wire format is NOT frozen at Layer 0 — engine
//! implementations may pick their own encoding — but the *logical* format
//! (an ordered list of tagged records with optional column-family IDs) is
//! stable.
//!
//! For Layer 0 purposes, we expose:
//! - A concrete [`WriteBatch`] struct with the user-visible mutation methods.
//! - A [`WriteBatchHandler`] trait used to replay a batch into any target
//!   (memtable, another batch, a logging shim, …). This is the equivalent
//!   of upstream `WriteBatch::Handler`.

use crate::core::status::Result;
use crate::core::types::ColumnFamilyId;

/// Default column family ID. Column family 0 is always the "default" CF
/// which must exist for the lifetime of the DB.
pub const DEFAULT_CF: ColumnFamilyId = 0;

/// An ordered, atomic batch of mutations.
///
/// Construction is cheap (an empty `Vec`). The underlying encoded form is
/// kept opaque at this layer — engine layers below should define the
/// concrete wire format and document it in a `format.md`.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct WriteBatch {
    /// User-visible record list. Each entry captures the logical operation
    /// and its column family. Real engines will collapse this into a single
    /// binary buffer; this representation exists at Layer 0 purely so the
    /// API can be exercised and tested without an engine.
    records: Vec<Record>,
}

/// One logical mutation inside a [`WriteBatch`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Record {
    /// Insert or overwrite a key.
    Put {
        /// Column family.
        cf: ColumnFamilyId,
        /// Key bytes.
        key: Vec<u8>,
        /// Value bytes.
        value: Vec<u8>,
    },
    /// Delete a key (normal tombstone).
    Delete {
        /// Column family.
        cf: ColumnFamilyId,
        /// Key bytes.
        key: Vec<u8>,
    },
    /// Single-delete — asserts the key was written exactly once.
    SingleDelete {
        /// Column family.
        cf: ColumnFamilyId,
        /// Key bytes.
        key: Vec<u8>,
    },
    /// Delete every key in `[begin, end)`.
    DeleteRange {
        /// Column family.
        cf: ColumnFamilyId,
        /// Inclusive lower bound.
        begin: Vec<u8>,
        /// Exclusive upper bound.
        end: Vec<u8>,
    },
    /// Merge operand.
    Merge {
        /// Column family.
        cf: ColumnFamilyId,
        /// Key bytes.
        key: Vec<u8>,
        /// Merge operand.
        value: Vec<u8>,
    },
}

impl WriteBatch {
    /// Create an empty batch.
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert or overwrite `key → value` in the default column family.
    pub fn put(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) {
        self.put_cf(DEFAULT_CF, key, value);
    }

    /// Insert or overwrite `key → value` in the given column family.
    pub fn put_cf(
        &mut self,
        cf: ColumnFamilyId,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) {
        self.records.push(Record::Put {
            cf,
            key: key.into(),
            value: value.into(),
        });
    }

    /// Delete `key` from the default column family.
    pub fn delete(&mut self, key: impl Into<Vec<u8>>) {
        self.delete_cf(DEFAULT_CF, key);
    }

    /// Delete `key` from the given column family.
    pub fn delete_cf(&mut self, cf: ColumnFamilyId, key: impl Into<Vec<u8>>) {
        self.records.push(Record::Delete {
            cf,
            key: key.into(),
        });
    }

    /// Single-delete `key` from the default column family.
    pub fn single_delete(&mut self, key: impl Into<Vec<u8>>) {
        self.records.push(Record::SingleDelete {
            cf: DEFAULT_CF,
            key: key.into(),
        });
    }

    /// Delete every key in `[begin, end)` in the default column family.
    pub fn delete_range(&mut self, begin: impl Into<Vec<u8>>, end: impl Into<Vec<u8>>) {
        self.delete_range_cf(DEFAULT_CF, begin, end);
    }

    /// Delete every key in `[begin, end)` in the given column family.
    pub fn delete_range_cf(
        &mut self,
        cf: ColumnFamilyId,
        begin: impl Into<Vec<u8>>,
        end: impl Into<Vec<u8>>,
    ) {
        self.records.push(Record::DeleteRange {
            cf,
            begin: begin.into(),
            end: end.into(),
        });
    }

    /// Append a merge operand to `key` in the default column family.
    pub fn merge(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) {
        self.records.push(Record::Merge {
            cf: DEFAULT_CF,
            key: key.into(),
            value: value.into(),
        });
    }

    /// Append a merge operand to `key` in a specific column family.
    pub fn merge_cf(
        &mut self,
        cf: ColumnFamilyId,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) {
        self.records.push(Record::Merge {
            cf,
            key: key.into(),
            value: value.into(),
        });
    }

    /// Number of mutations in the batch.
    pub fn count(&self) -> usize {
        self.records.len()
    }

    /// Whether the batch contains any mutations.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Discard all mutations.
    pub fn clear(&mut self) {
        self.records.clear();
    }

    /// Iterate over the logical records in the order they were added.
    pub fn records(&self) -> &[Record] {
        &self.records
    }

    /// Replay this batch into a [`WriteBatchHandler`] (e.g. a memtable).
    /// Short-circuits on the first error returned by the handler.
    pub fn iterate<H: WriteBatchHandler + ?Sized>(&self, handler: &mut H) -> Result<()> {
        for record in &self.records {
            match record {
                Record::Put { cf, key, value } => handler.put_cf(*cf, key, value)?,
                Record::Delete { cf, key } => handler.delete_cf(*cf, key)?,
                Record::SingleDelete { cf, key } => handler.single_delete_cf(*cf, key)?,
                Record::DeleteRange { cf, begin, end } => {
                    handler.delete_range_cf(*cf, begin, end)?
                }
                Record::Merge { cf, key, value } => handler.merge_cf(*cf, key, value)?,
            }
        }
        Ok(())
    }
}

/// Visitor trait used to replay a [`WriteBatch`] into some target. Mirrors
/// upstream `WriteBatch::Handler`.
///
/// All methods have a default no-op implementation so implementors only
/// override the record kinds they care about.
#[allow(unused_variables)]
pub trait WriteBatchHandler {
    /// Called for each [`Record::Put`].
    fn put_cf(&mut self, cf: ColumnFamilyId, key: &[u8], value: &[u8]) -> Result<()> {
        Ok(())
    }
    /// Called for each [`Record::Delete`].
    fn delete_cf(&mut self, cf: ColumnFamilyId, key: &[u8]) -> Result<()> {
        Ok(())
    }
    /// Called for each [`Record::SingleDelete`].
    fn single_delete_cf(&mut self, cf: ColumnFamilyId, key: &[u8]) -> Result<()> {
        Ok(())
    }
    /// Called for each [`Record::DeleteRange`].
    fn delete_range_cf(&mut self, cf: ColumnFamilyId, begin: &[u8], end: &[u8]) -> Result<()> {
        Ok(())
    }
    /// Called for each [`Record::Merge`].
    fn merge_cf(&mut self, cf: ColumnFamilyId, key: &[u8], value: &[u8]) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_and_delete() {
        let mut wb = WriteBatch::new();
        wb.put(b"k1".to_vec(), b"v1".to_vec());
        wb.delete(b"k2".to_vec());
        assert_eq!(wb.count(), 2);
        assert!(!wb.is_empty());
    }

    #[test]
    fn handler_replays_all_records() {
        #[derive(Default)]
        struct Counter {
            puts: usize,
            dels: usize,
        }
        impl WriteBatchHandler for Counter {
            fn put_cf(&mut self, _cf: ColumnFamilyId, _k: &[u8], _v: &[u8]) -> Result<()> {
                self.puts += 1;
                Ok(())
            }
            fn delete_cf(&mut self, _cf: ColumnFamilyId, _k: &[u8]) -> Result<()> {
                self.dels += 1;
                Ok(())
            }
        }

        let mut wb = WriteBatch::new();
        wb.put(b"a".to_vec(), b"1".to_vec());
        wb.put(b"b".to_vec(), b"2".to_vec());
        wb.delete(b"c".to_vec());

        let mut counter = Counter::default();
        wb.iterate(&mut counter).unwrap();
        assert_eq!(counter.puts, 2);
        assert_eq!(counter.dels, 1);
    }

    #[test]
    fn handler_short_circuits_on_error() {
        struct FailOnSecond(u32);
        impl WriteBatchHandler for FailOnSecond {
            fn put_cf(&mut self, _cf: ColumnFamilyId, _k: &[u8], _v: &[u8]) -> Result<()> {
                self.0 += 1;
                if self.0 >= 2 {
                    Err(crate::core::status::Status::io_error("boom"))
                } else {
                    Ok(())
                }
            }
        }
        let mut wb = WriteBatch::new();
        wb.put(b"a".to_vec(), b"1".to_vec());
        wb.put(b"b".to_vec(), b"2".to_vec());
        wb.put(b"c".to_vec(), b"3".to_vec());
        let mut h = FailOnSecond(0);
        let res = wb.iterate(&mut h);
        assert!(res.unwrap_err().is_io_error());
        assert_eq!(h.0, 2, "should stop on the failing record");
    }
}
