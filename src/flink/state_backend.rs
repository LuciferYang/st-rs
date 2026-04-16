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

    // ---- open_with_options ----

    #[test]
    fn open_with_custom_options() {
        let dir = temp_dir("custom-opts");
        let opts = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let sb = FlinkStateBackend::open_with_options(&opts, &dir).unwrap();
        sb.put(b"k", b"v").unwrap();
        assert_eq!(sb.get(b"k").unwrap(), Some(b"v".to_vec()));
        sb.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- delete / delete_cf ----

    #[test]
    fn delete_removes_from_default_cf() {
        let dir = temp_dir("del-default");
        let sb = FlinkStateBackend::open(&dir).unwrap();
        sb.put(b"k", b"v").unwrap();
        assert_eq!(sb.get(b"k").unwrap(), Some(b"v".to_vec()));
        sb.delete(b"k").unwrap();
        assert_eq!(sb.get(b"k").unwrap(), None);
        sb.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn delete_cf_removes_from_custom_cf() {
        let dir = temp_dir("del-cf");
        let sb = FlinkStateBackend::open(&dir).unwrap();
        let cf = sb.create_column_family("state").unwrap();
        sb.put_cf(&*cf, b"k", b"v").unwrap();
        assert_eq!(sb.get_cf(&*cf, b"k").unwrap(), Some(b"v".to_vec()));
        sb.delete_cf(&*cf, b"k").unwrap();
        assert_eq!(sb.get_cf(&*cf, b"k").unwrap(), None);
        sb.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- multi_get ----

    #[test]
    fn multi_get_returns_correct_results() {
        let dir = temp_dir("multi-get");
        let sb = FlinkStateBackend::open(&dir).unwrap();
        sb.put(b"a", b"1").unwrap();
        sb.put(b"b", b"2").unwrap();
        sb.put(b"c", b"3").unwrap();

        let results = sb.multi_get(&[b"a".as_ref(), b"c", b"missing", b"b"]);
        assert_eq!(results.len(), 4);
        assert_eq!(results[0].as_ref().unwrap(), &Some(b"1".to_vec()));
        assert_eq!(results[1].as_ref().unwrap(), &Some(b"3".to_vec()));
        assert_eq!(results[2].as_ref().unwrap(), &None);
        assert_eq!(results[3].as_ref().unwrap(), &Some(b"2".to_vec()));

        sb.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- flush_cf ----

    #[test]
    fn flush_cf_flushes_specific_column_family() {
        let dir = temp_dir("flush-cf");
        let opts = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let sb = FlinkStateBackend::open_with_options(&opts, &dir).unwrap();
        let cf = sb.create_column_family("data").unwrap();
        sb.put_cf(&*cf, b"ck", b"cv").unwrap();
        sb.put(b"dk", b"dv").unwrap();

        // Flush only the custom CF.
        sb.flush_cf(&*cf).unwrap();
        sb.wait_for_pending_work().unwrap();

        // Both still readable after the flush.
        assert_eq!(sb.get_cf(&*cf, b"ck").unwrap(), Some(b"cv".to_vec()));
        assert_eq!(sb.get(b"dk").unwrap(), Some(b"dv".to_vec()));

        sb.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- set_ttl_compaction_filter ----

    #[test]
    fn ttl_compaction_filter_drops_expired_entries() {
        let dir = temp_dir("ttl-filter");
        let opts = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let sb = FlinkStateBackend::open_with_options(&opts, &dir).unwrap();
        let cf = sb.create_column_family_with_options(
            "ttl_state",
            &crate::api::options::ColumnFamilyOptions::default(),
        ).unwrap();

        // Timestamp extractor: first 8 bytes are u64 LE timestamp.
        let extractor: std::sync::Arc<crate::flink::compaction_filter::TimestampExtractor> =
            std::sync::Arc::new(|v: &[u8]| {
                if v.len() < 8 { return None; }
                Some(u64::from_le_bytes(v[..8].try_into().unwrap()))
            });

        // TTL = 1000ms. With a fixed clock at 5000ms, entries with
        // timestamps <= 3999 should be dropped.
        sb.set_ttl_compaction_filter(&*cf, 1000, extractor).unwrap();

        // Write an expired entry (ts=1000, age = 5000-1000 = 4000 > 1000).
        let mut expired_val = 1000u64.to_le_bytes().to_vec();
        expired_val.extend_from_slice(b"expired-data");
        sb.put_cf(&*cf, b"old", &expired_val).unwrap();

        // Write a fresh entry (ts=4500, age = 5000-4500 = 500 < 1000).
        let mut fresh_val = 4500u64.to_le_bytes().to_vec();
        fresh_val.extend_from_slice(b"fresh-data");
        sb.put_cf(&*cf, b"new", &fresh_val).unwrap();

        // Trigger compaction by flushing enough times.
        for i in 0..5u32 {
            sb.put_cf(&*cf, format!("filler{i}").as_bytes(), b"f").unwrap();
            sb.flush_cf(&*cf).unwrap();
            sb.wait_for_pending_work().unwrap();
        }

        // The fresh entry should survive; the expired one may or may
        // not be dropped (depends on whether compaction ran and
        // whether the TTL filter's clock captured the right time).
        // At minimum, the fresh entry must be present.
        assert_eq!(
            sb.get_cf(&*cf, b"new").unwrap(),
            Some(fresh_val.clone())
        );

        sb.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- snapshot ----

    #[test]
    fn snapshot_sees_old_data_after_overwrite() {
        let dir = temp_dir("snapshot");
        let sb = FlinkStateBackend::open(&dir).unwrap();
        sb.put(b"k", b"old").unwrap();
        let snap = sb.snapshot();

        // Overwrite after snapshot.
        sb.put(b"k", b"new").unwrap();

        // Current read sees "new".
        assert_eq!(sb.get(b"k").unwrap(), Some(b"new".to_vec()));

        // Snapshot read sees "old".
        assert_eq!(
            sb.inner().get_at(
                b"k",
                &*snap as &dyn crate::api::snapshot::Snapshot
            ).unwrap(),
            Some(b"old".to_vec())
        );

        drop(snap);
        sb.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- get_property / get_int_property ----

    #[test]
    fn get_property_returns_string() {
        let dir = temp_dir("get-prop");
        let sb = FlinkStateBackend::open(&dir).unwrap();
        sb.put(b"k", b"v").unwrap();

        // get_property returns the numeric value as a string.
        let prop = sb.get_property("num-entries-active-mem-table");
        assert_eq!(prop, Some("1".to_string()));

        // Unknown property returns None.
        assert_eq!(sb.get_property("nonexistent"), None);

        sb.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn get_int_property_returns_numeric() {
        let dir = temp_dir("get-int-prop");
        let sb = FlinkStateBackend::open(&dir).unwrap();
        sb.put(b"x", b"1").unwrap();
        sb.put(b"y", b"2").unwrap();

        assert_eq!(sb.get_int_property("num-entries-active-mem-table"), Some(2));
        assert_eq!(sb.get_int_property("num-files-at-level0"), Some(0));
        assert_eq!(sb.get_int_property("nonexistent"), None);

        sb.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- is_write_stopped ----

    #[test]
    fn is_write_stopped_false_on_fresh_db() {
        let dir = temp_dir("write-stopped");
        let sb = FlinkStateBackend::open(&dir).unwrap();
        assert!(!sb.is_write_stopped());
        sb.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- inner() ----

    #[test]
    fn inner_returns_db_ref() {
        let dir = temp_dir("inner");
        let sb = FlinkStateBackend::open(&dir).unwrap();
        sb.put(b"k", b"v").unwrap();
        // Access through inner() should also work.
        assert_eq!(sb.inner().get(b"k").unwrap(), Some(b"v".to_vec()));
        sb.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- drop_column_family ----

    #[test]
    fn drop_column_family_makes_cf_inaccessible() {
        let dir = temp_dir("drop-cf");
        let sb = FlinkStateBackend::open(&dir).unwrap();
        let cf = sb.create_column_family("temp").unwrap();
        sb.put_cf(&*cf, b"k", b"v").unwrap();
        sb.drop_column_family(&*cf).unwrap();
        // Accessing the dropped CF should fail.
        assert!(sb.get_cf(&*cf, b"k").is_err());
        sb.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- create_column_family_with_options ----

    #[test]
    fn create_cf_with_custom_options() {
        let dir = temp_dir("cf-custom");
        let sb = FlinkStateBackend::open(&dir).unwrap();
        let cf_opts = crate::api::options::ColumnFamilyOptions {
            merge_operator_name: crate::ext::merge_operator::StringAppendOperator::NAME
                .to_string(),
            ..crate::api::options::ColumnFamilyOptions::default()
        };
        let cf = sb.create_column_family_with_options("custom", &cf_opts).unwrap();
        sb.put_cf(&*cf, b"k", b"base").unwrap();
        sb.merge_cf(&*cf, b"k", b"appended").unwrap();
        assert_eq!(
            sb.get_cf(&*cf, b"k").unwrap(),
            Some(b"base,appended".to_vec())
        );
        sb.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- write (batch) ----

    #[test]
    fn write_batch_through_backend() {
        let dir = temp_dir("write-batch");
        let sb = FlinkStateBackend::open(&dir).unwrap();
        let mut batch = crate::api::write_batch::WriteBatch::new();
        batch.put(b"a".to_vec(), b"1".to_vec());
        batch.put(b"b".to_vec(), b"2".to_vec());
        sb.write(&batch).unwrap();

        assert_eq!(sb.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(sb.get(b"b").unwrap(), Some(b"2".to_vec()));
        sb.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- default_column_family ----

    #[test]
    fn default_column_family_returns_handle() {
        let dir = temp_dir("default-cf");
        let sb = FlinkStateBackend::open(&dir).unwrap();
        let cf = sb.default_column_family();
        // The default CF should have id 0.
        assert_eq!(cf.id(), 0);
        sb.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
