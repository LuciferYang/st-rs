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

//! High-level Flink state backend facade.
//!
//! [`FlinkStateBackend`] wraps [`DbImpl`](crate::db::db_impl::DbImpl)
//! and exposes the operations Flink needs in a single, ergonomic API:
//!
//! - Open with multiple column families
//! - Put / get / merge per CF
//! - Incremental checkpoint (`disable_file_deletions` → `get_live_files`
//!   → `enable_file_deletions`)
//! - Register TTL compaction filters
//! - Per-CF merge operator (defaults to `StringAppendOperator`)
//!
//! This facade does NOT replace `DbImpl` — it's a convenience layer
//! for the Flink integration path. Direct `DbImpl` usage is still
//! fine for non-Flink use cases.

use crate::api::db::ColumnFamilyHandle;
use crate::api::options::{ColumnFamilyOptions, DbOptions};
use crate::core::status::Result;
use crate::db::db_impl::{ColumnFamilyHandleImpl, DbImpl, LiveFileMetaData};
use crate::ext::merge_operator::StringAppendOperator;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Flink-oriented wrapper around `DbImpl`. Provides a streamlined
/// API for the Flink state backend integration.
pub struct FlinkStateBackend {
    /// The underlying database engine.
    db: Arc<DbImpl>,
}

impl FlinkStateBackend {
    /// Open a Flink state backend at `path`. Creates the DB if
    /// missing.
    pub fn open(path: &Path) -> Result<Self> {
        let opts = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024 * 1024, // 64 MiB
            ..DbOptions::default()
        };
        let db = DbImpl::open(&opts, path)?;
        Ok(Self { db })
    }

    /// Open with custom DB options.
    pub fn open_with_options(opts: &DbOptions, path: &Path) -> Result<Self> {
        let db = DbImpl::open(opts, path)?;
        Ok(Self { db })
    }

    /// Create a column family with the default Flink configuration:
    /// `StringAppendOperator` merge operator enabled.
    pub fn create_column_family(&self, name: &str) -> Result<Arc<ColumnFamilyHandleImpl>> {
        let cf_opts = ColumnFamilyOptions {
            merge_operator_name: StringAppendOperator::NAME.to_string(),
            ..ColumnFamilyOptions::default()
        };
        self.db.create_column_family(name, &cf_opts)
    }

    /// Create a column family with custom options.
    pub fn create_column_family_with_options(
        &self,
        name: &str,
        options: &ColumnFamilyOptions,
    ) -> Result<Arc<ColumnFamilyHandleImpl>> {
        self.db.create_column_family(name, options)
    }

    /// Drop a column family.
    pub fn drop_column_family(&self, cf: &dyn ColumnFamilyHandle) -> Result<()> {
        self.db.drop_column_family(cf)
    }

    /// Handle to the default column family.
    pub fn default_column_family(&self) -> Arc<ColumnFamilyHandleImpl> {
        self.db.default_column_family()
    }

    // ---- Point operations ----

    /// Put in the default CF.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.db.put(key, value)
    }

    /// Put in a specific CF.
    pub fn put_cf(
        &self,
        cf: &dyn ColumnFamilyHandle,
        key: &[u8],
        value: &[u8],
    ) -> Result<()> {
        self.db.put_cf(cf, key, value)
    }

    /// Get from the default CF.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.db.get(key)
    }

    /// Get from a specific CF.
    pub fn get_cf(
        &self,
        cf: &dyn ColumnFamilyHandle,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        self.db.get_cf(cf, key)
    }

    /// Delete from the default CF.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.db.delete(key)
    }

    /// Delete from a specific CF.
    pub fn delete_cf(
        &self,
        cf: &dyn ColumnFamilyHandle,
        key: &[u8],
    ) -> Result<()> {
        self.db.delete_cf(cf, key)
    }

    /// Merge in a specific CF. Requires a merge operator on the CF.
    pub fn merge_cf(
        &self,
        cf: &dyn ColumnFamilyHandle,
        key: &[u8],
        value: &[u8],
    ) -> Result<()> {
        let mut batch = crate::api::write_batch::WriteBatch::new();
        batch.merge_cf(cf.id(), key.to_vec(), value.to_vec());
        self.db.write(&batch)
    }

    /// Apply a write batch atomically.
    pub fn write(&self, batch: &crate::api::write_batch::WriteBatch) -> Result<()> {
        self.db.write(batch)
    }

    /// Batch get from the default CF.
    pub fn multi_get(&self, keys: &[&[u8]]) -> Vec<Result<Option<Vec<u8>>>> {
        self.db.multi_get(keys)
    }

    // ---- Flush / compaction ----

    /// Flush the default CF.
    pub fn flush(&self) -> Result<()> {
        self.db.flush()
    }

    /// Flush a specific CF.
    pub fn flush_cf(&self, cf: &dyn ColumnFamilyHandle) -> Result<()> {
        self.db.flush_cf(cf)
    }

    /// Wait for all background work to complete.
    pub fn wait_for_pending_work(&self) -> Result<()> {
        self.db.wait_for_pending_work()
    }

    // ---- Incremental checkpoint ----

    /// Prevent obsolete SST file deletion. Call before
    /// `get_live_files` to ensure files remain stable during upload.
    pub fn disable_file_deletions(&self) -> Result<()> {
        self.db.disable_file_deletions()
    }

    /// Re-enable SST file deletion. Deferred deletions are processed
    /// when the count returns to 0.
    pub fn enable_file_deletions(&self) -> Result<()> {
        self.db.enable_file_deletions()
    }

    /// Return paths of all live files. If `flush_memtable` is true,
    /// flushes first.
    pub fn get_live_files(&self, flush_memtable: bool) -> Result<Vec<PathBuf>> {
        self.db.get_live_files(flush_memtable)
    }

    /// Return metadata for all live SST files.
    pub fn get_live_files_metadata(&self) -> Vec<LiveFileMetaData> {
        self.db.get_live_files_metadata()
    }

    // ---- TTL / compaction filter ----

    /// Register a TTL compaction filter on a column family. Entries
    /// older than `ttl_millis` (based on embedded timestamp) are
    /// dropped during compaction.
    pub fn set_ttl_compaction_filter(
        &self,
        cf: &dyn ColumnFamilyHandle,
        ttl_millis: u64,
        extract_timestamp: std::sync::Arc<crate::flink::compaction_filter::TimestampExtractor>,
    ) -> Result<()> {
        let factory = Arc::new(
            crate::flink::compaction_filter::TtlCompactionFilterFactory::new(
                ttl_millis,
                extract_timestamp,
                Box::new(|| {
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64
                }),
            ),
        );
        self.db.set_compaction_filter_factory(cf, factory)
    }

    // ---- Properties ----

    /// Query an engine property.
    pub fn get_property(&self, name: &str) -> Option<String> {
        self.db.get_property(name)
    }

    /// Query a numeric engine property.
    pub fn get_int_property(&self, name: &str) -> Option<u64> {
        self.db.get_int_property(name)
    }

    /// Is the engine currently stalling writes?
    pub fn is_write_stopped(&self) -> bool {
        self.db.is_write_stopped()
    }

    // ---- Snapshot ----

    /// Take a snapshot at the current sequence.
    pub fn snapshot(&self) -> Arc<crate::db::db_impl::DbSnapshot> {
        self.db.snapshot()
    }

    // ---- Lifecycle ----

    /// Get the underlying `DbImpl` for advanced operations.
    pub fn inner(&self) -> &Arc<DbImpl> {
        &self.db
    }

    /// Close the state backend.
    pub fn close(&self) -> Result<()> {
        self.db.close()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(tag: &str) -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static C: AtomicU64 = AtomicU64::new(0);
        let n = C.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        std::env::temp_dir().join(format!("st-rs-flink-{tag}-{pid}-{n}"))
    }

    #[test]
    fn flink_backend_basic_workflow() {
        let dir = temp_dir("basic");
        let sb = FlinkStateBackend::open(&dir).unwrap();

        // Create a CF with merge operator (Flink pattern).
        let cf = sb.create_column_family("state1").unwrap();

        // Put + merge (ListState pattern).
        sb.put_cf(&*cf, b"list", b"a").unwrap();
        sb.merge_cf(&*cf, b"list", b"b").unwrap();
        sb.merge_cf(&*cf, b"list", b"c").unwrap();

        assert_eq!(
            sb.get_cf(&*cf, b"list").unwrap(),
            Some(b"a,b,c".to_vec())
        );

        // Default CF is isolated.
        sb.put(b"dk", b"dv").unwrap();
        assert_eq!(sb.get(b"dk").unwrap(), Some(b"dv".to_vec()));
        assert_eq!(sb.get_cf(&*cf, b"dk").unwrap(), None);

        sb.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn flink_backend_incremental_checkpoint() {
        let dir = temp_dir("checkpoint");
        let sb = FlinkStateBackend::open(&dir).unwrap();

        sb.put(b"k1", b"v1").unwrap();
        sb.put(b"k2", b"v2").unwrap();

        // Simulate Flink's incremental checkpoint workflow.
        sb.disable_file_deletions().unwrap();
        let files = sb.get_live_files(true).unwrap();
        assert!(!files.is_empty());

        let meta = sb.get_live_files_metadata();
        // After flush, we should have at least one SST.
        assert!(!meta.is_empty());

        sb.enable_file_deletions().unwrap();

        // Data still readable.
        assert_eq!(sb.get(b"k1").unwrap(), Some(b"v1".to_vec()));

        sb.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn flink_backend_properties() {
        let dir = temp_dir("props");
        let sb = FlinkStateBackend::open(&dir).unwrap();

        sb.put(b"k", b"v").unwrap();
        assert_eq!(sb.get_int_property("num-entries-active-mem-table"), Some(1));
        assert!(!sb.is_write_stopped());

        sb.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
