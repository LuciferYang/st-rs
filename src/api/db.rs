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

//! Port of `include/rocksdb/db.h`.
//!
//! [`Db`] is the top-level handle. It owns the underlying LSM state and
//! exposes point operations (`put`, `get`, `delete`), batch writes,
//! iteration, snapshots, flush, and compaction.
//!
//! Upstream provides a large abstract `DB` class with ~100 virtual methods.
//! This Layer 0 port extracts only the **user-visible**, **commonly-used**
//! operations. Internal features (backup, ingest, replication, trace,
//! blob-db-specific calls, …) will be added in follow-up layers.

use crate::api::iterator::DbIterator;
use crate::api::options::{
    ColumnFamilyOptions, DbOptions, FlushOptions, ReadOptions, WriteOptions,
};
use crate::api::snapshot::Snapshot;
use crate::api::write_batch::WriteBatch;
use crate::core::status::Result;
use crate::core::types::ColumnFamilyId;
use std::path::Path;
use std::sync::Arc;

/// Result of opening a DB with a list of column families. Mirrors the
/// tuple returned by upstream `DB::Open(options, path, cf_descriptors, handles, db)`.
pub type OpenCfResult<D> = (D, Vec<Arc<dyn ColumnFamilyHandle>>);

/// Handle to an open column family. Mirrors `ColumnFamilyHandle` from
/// upstream. Returned by [`Db::create_column_family`] and accepted by all
/// `*_cf` methods.
pub trait ColumnFamilyHandle: Send + Sync {
    /// Numeric ID of this column family. Stable for the lifetime of the CF.
    fn id(&self) -> ColumnFamilyId;

    /// Name of this column family. Stable for the lifetime of the CF.
    fn name(&self) -> &str;
}

/// A descriptor used to open / create a column family: a name plus the
/// options to apply. Mirrors upstream `ColumnFamilyDescriptor`.
#[derive(Debug, Clone)]
pub struct ColumnFamilyDescriptor {
    /// Name of the column family.
    pub name: String,
    /// Options to apply at open time.
    pub options: ColumnFamilyOptions,
}

impl ColumnFamilyDescriptor {
    /// Create a new descriptor.
    pub fn new(name: impl Into<String>, options: ColumnFamilyOptions) -> Self {
        Self {
            name: name.into(),
            options,
        }
    }
}

/// The name of the "default" column family. Every DB is opened with this
/// CF pre-created; it cannot be dropped. Matches upstream `kDefaultColumnFamilyName`.
pub const DEFAULT_COLUMN_FAMILY_NAME: &str = "default";

/// The open handle to a DB.
///
/// Not `Sync` in the upstream sense (it *is* safe for concurrent use from
/// multiple threads — that's a trait requirement — but callers usually hold
/// an `Arc<dyn Db>` rather than a bare `&dyn Db`).
pub trait Db: Send + Sync {
    // -- Column families --------------------------------------------------

    /// Create a new column family. Returns the handle.
    fn create_column_family(
        &self,
        name: &str,
        options: &ColumnFamilyOptions,
    ) -> Result<Arc<dyn ColumnFamilyHandle>>;

    /// Drop a column family. After this call the handle is invalid.
    fn drop_column_family(&self, cf: &dyn ColumnFamilyHandle) -> Result<()>;

    /// Handle for the default column family. Always present.
    fn default_column_family(&self) -> Arc<dyn ColumnFamilyHandle>;

    // -- Point operations -------------------------------------------------

    /// Insert or overwrite `key → value` in the default column family.
    fn put(&self, opts: &WriteOptions, key: &[u8], value: &[u8]) -> Result<()>;

    /// `put` for a specific column family.
    fn put_cf(
        &self,
        opts: &WriteOptions,
        cf: &dyn ColumnFamilyHandle,
        key: &[u8],
        value: &[u8],
    ) -> Result<()>;

    /// Delete a key from the default column family.
    fn delete(&self, opts: &WriteOptions, key: &[u8]) -> Result<()>;

    /// `delete` for a specific column family.
    fn delete_cf(
        &self,
        opts: &WriteOptions,
        cf: &dyn ColumnFamilyHandle,
        key: &[u8],
    ) -> Result<()>;

    /// Single-delete: asserts the key was written exactly once since the
    /// last compaction. Used by streaming workloads where a `Put` is never
    /// overwritten before being deleted.
    fn single_delete(&self, opts: &WriteOptions, key: &[u8]) -> Result<()>;

    /// Merge a new operand into the default column family. Requires a
    /// `MergeOperator` to be configured.
    fn merge(&self, opts: &WriteOptions, key: &[u8], value: &[u8]) -> Result<()>;

    /// Read `key` from the default column family. Returns `Ok(None)` on
    /// key-not-found, `Ok(Some(value))` on success, `Err` on I/O or
    /// corruption errors.
    fn get(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// `get` for a specific column family.
    fn get_cf(
        &self,
        opts: &ReadOptions,
        cf: &dyn ColumnFamilyHandle,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>>;

    // -- Batched operations -----------------------------------------------

    /// Apply `batch` atomically.
    fn write(&self, opts: &WriteOptions, batch: &WriteBatch) -> Result<()>;

    /// Multi-get: look up a batch of keys in one call. Returns a `Vec` with
    /// one entry per input key, in the same order.
    fn multi_get(
        &self,
        opts: &ReadOptions,
        keys: &[&[u8]],
    ) -> Vec<Result<Option<Vec<u8>>>>;

    // -- Iteration --------------------------------------------------------

    /// Open a new iterator over the default column family. The iterator
    /// takes a snapshot of the DB as of the call (unless one is specified
    /// via `opts.snapshot`).
    fn new_iterator(&self, opts: &ReadOptions) -> Box<dyn DbIterator>;

    /// Iterator scoped to a column family.
    fn new_iterator_cf(
        &self,
        opts: &ReadOptions,
        cf: &dyn ColumnFamilyHandle,
    ) -> Box<dyn DbIterator>;

    // -- Snapshots --------------------------------------------------------

    /// Take a new snapshot. The snapshot is released via
    /// [`Self::release_snapshot`]; failing to release it pins sequence
    /// numbers and prevents compaction from dropping older versions.
    fn snapshot(&self) -> Arc<dyn Snapshot>;

    /// Release a snapshot previously obtained via [`Self::snapshot`].
    fn release_snapshot(&self, snap: Arc<dyn Snapshot>);

    // -- Admin ------------------------------------------------------------

    /// Flush the memtable of the default column family to L0.
    fn flush(&self, opts: &FlushOptions) -> Result<()>;

    /// Flush a specific column family.
    fn flush_cf(&self, opts: &FlushOptions, cf: &dyn ColumnFamilyHandle) -> Result<()>;

    /// Run a manual compaction over the given key range in the default
    /// column family. A range of `None..None` compacts the entire CF.
    fn compact_range(&self, begin: Option<&[u8]>, end: Option<&[u8]>) -> Result<()>;

    /// Query a DB-wide string property. Matches upstream `GetProperty`.
    /// Returns `None` if the property is unknown.
    fn get_property(&self, property: &str) -> Option<String>;

    /// Flush the WAL to disk. With `sync == true`, also `fsync`.
    fn flush_wal(&self, sync: bool) -> Result<()>;

    /// Close the DB. All column family handles become invalid.
    fn close(&self) -> Result<()>;
}

/// Open-time entry point.
///
/// Engines implement a factory function of this shape. Layer 0 does not
/// ship an engine, so this trait exists purely to document the expected
/// surface.
pub trait DbOpener {
    /// The concrete `Db` implementation opened by this opener.
    type Db: Db;

    /// Open a DB with only the default column family.
    fn open(opts: &DbOptions, path: &Path) -> Result<Self::Db>;

    /// Open a DB with the given list of column families.
    ///
    /// Returns the DB handle and a vector of column family handles, in the
    /// same order as `column_families`.
    fn open_cf(
        opts: &DbOptions,
        path: &Path,
        column_families: &[ColumnFamilyDescriptor],
    ) -> Result<OpenCfResult<Self::Db>>;

    /// Open a DB in read-only mode.
    fn open_read_only(
        opts: &DbOptions,
        path: &Path,
        error_if_wal_file_exists: bool,
    ) -> Result<Self::Db>;
}
