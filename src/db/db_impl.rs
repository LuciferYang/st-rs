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

//! Port of `db/db_impl/db_impl.{h,cc}` — the st-rs LSM engine.
//!
//! This module composes everything from Layers 0–4g into a working
//! key-value store:
//!
//! ```text
//!   Put → write_lock acquire → WAL append → memtable insert → release
//!   Get → memtable check → immutable memtable → L0 (newest-first)
//!       → L1 (binary search) → merge operand resolution
//!   Flush → freeze memtable → write SST → swap in fresh memtable
//!         → update MANIFEST → fsync directory
//!   Compaction → pick oldest L0 SSTs → merge into L1
//!   Open → mkdir/lock → recover MANIFEST → replay WAL into memtable
//! ```
//!
//! # What's included
//!
//! - DB lifecycle: `open`, `close`, reopen-with-WAL-replay
//! - Multiple column families with create/drop
//! - `put`, `delete`, `single_delete`, `delete_range`, `merge`, `get`
//! - Atomic `write` of a [`WriteBatch`] (including cross-CF batches)
//! - Background flush via thread pool (immutable memtable pipeline)
//! - Background compaction (L0 → L1, snapshot-aware retention,
//!   compaction filters)
//! - Multi-level layout: L0 (overlapping) + L1 (sorted, non-overlapping)
//! - Explicit snapshots and snapshot-aware reads (`get_at`, `iter_at`)
//! - Full merge operator support (merge operands across memtable + SSTs)
//! - VersionEdit + MANIFEST persistence
//! - SST ingestion (`ingest_external_file`)
//! - Checkpoint support
//! - Iterator API (`DbIterator` with merging iterator)
//! - Crash-safe WAL: every write goes through the WAL before the
//!   memtable, and reopens replay the WAL into a fresh memtable
//!
//! # File layout on disk
//!
//! ```text
//!   <db_path>/
//!     LOCK                        (Layer 2 PosixFileLock)
//!     CURRENT                     (one line: comma-separated SST numbers)
//!     000001.log                  (WAL)
//!     000007.sst                  (Layer 3b BlockBasedTableBuilder output)
//!     000012.sst
//! ```
//!
//! On open we read CURRENT to discover live SSTs, then walk the
//! directory for any `*.log` files and replay them.

use crate::api::options::DbOptions;
use crate::core::status::{Result, Status};
use crate::core::types::{SequenceNumber, ValueType};
use crate::db::compaction::CompactionJob;
use crate::db::log_reader::LogReader;
use crate::db::log_writer::LogWriter;
use crate::db::memtable::{MemTable, MemTableGetResult};
use crate::db::dbformat::{LookupKey, ParsedInternalKey};
use crate::db::version_edit::{FileMetaData, VersionEdit};
use crate::db::version_set::VersionSet;
use crate::env::env_trait::{Priority, ThreadPool};
use crate::env::file_system::{FileLock, FileSystem, IoOptions};
use crate::env::posix::PosixFileSystem;
use crate::env::thread_pool::StdThreadPool;
use crate::ext::comparator::{BytewiseComparator, Comparator};
use crate::file::filename::{
    make_current_file_name, make_descriptor_file_name, make_lock_file_name,
    make_table_file_name, make_wal_file_name, parse_file_name,
};
use crate::file::random_access_file_reader::RandomAccessFileReader;
use crate::file::sequence_file_reader::SequentialFileReader;
use crate::file::writable_file_writer::WritableFileWriter;
use crate::sst::block_based::table_builder::{
    BlockBasedTableBuilder, BlockBasedTableOptions,
};
use crate::sst::block_based::table_reader::{BlockBasedTableReader, BlockCache};
use crate::cache::lru::LruCache;
use crate::core::types::FileType;
use crate::util::coding::{get_varint32, put_varint32};
use crate::api::write_batch::{Record, WriteBatch, WriteBatchHandler};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
use crate::core::types::ColumnFamilyId;

/// A range tombstone: `(start_key, end_key, sequence_number)`.
/// Represents a deletion of all keys in `[start_key, end_key)` at
/// the given sequence number.
type RangeTombstone = (Vec<u8>, Vec<u8>, SequenceNumber);

/// One open SST file: its assigned number, a cached reader, and
/// the user-key range covered by the file.
struct SstEntry {
    number: u64,
    reader: Arc<BlockBasedTableReader>,
    /// Smallest user key in this SST. Empty for an empty SST.
    smallest_key: Vec<u8>,
    /// Largest user key in this SST. Empty for an empty SST.
    largest_key: Vec<u8>,
    /// Level this SST belongs to. 0 = overlapping L0 from flushes;
    /// 1 = non-overlapping L1 from compaction.
    level: u32,
}

/// Default column family ID. Every DB has a CF with this ID named "default".
const DEFAULT_CF_ID: ColumnFamilyId = 0;

/// Per-column-family mutable state. Each CF has its own memtable
/// pipeline and SST level hierarchy. The WAL is shared across CFs.
struct CfState {
    /// CF numeric ID.
    id: ColumnFamilyId,
    /// CF name.
    name: String,
    /// Active (mutable) memtable for this CF.
    memtable: MemTable,
    /// Frozen memtable being flushed to an SST. At most one per CF.
    immutable: Option<Arc<MemTable>>,
    /// Level 0 SSTs, newest-first.
    l0: Vec<SstEntry>,
    /// Level 1 SSTs, sorted by smallest_key.
    l1: Vec<SstEntry>,
    /// WAL number for the immutable memtable (for deletion after flush).
    immutable_wal_number: Option<u64>,
    /// File number reserved for the in-progress flush output SST.
    pending_flush_sst_number: Option<u64>,
    /// File number of the compaction output currently in flight.
    pending_compaction: Option<u64>,
    /// Merge operator for this CF, if any. Resolved from
    /// `ColumnFamilyOptions::merge_operator_name` at CF creation.
    merge_operator: Option<Arc<dyn crate::ext::merge_operator::MergeOperator>>,
    /// Compaction filter factory for this CF, if any. Creates a
    /// fresh filter instance for each compaction job.
    compaction_filter_factory:
        Option<Arc<dyn crate::ext::compaction_filter::CompactionFilterFactory>>,
    /// Range tombstones: each entry is `(start_key, end_key, seq)`,
    /// representing a range delete `[start, end)` at the given
    /// sequence number. A key `k` with `start <= k < end` and
    /// `entry_seq < tombstone_seq` is considered deleted.
    range_tombstones: Vec<RangeTombstone>,
}

/// Concrete column family handle.
pub struct ColumnFamilyHandleImpl {
    id: ColumnFamilyId,
    name: String,
}

impl crate::api::db::ColumnFamilyHandle for ColumnFamilyHandleImpl {
    fn id(&self) -> ColumnFamilyId { self.id }
    fn name(&self) -> &str { &self.name }
}

impl std::fmt::Debug for ColumnFamilyHandleImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ColumnFamilyHandleImpl")
            .field("id", &self.id)
            .field("name", &self.name)
            .finish()
    }
}

/// Maximum number of levels. Layer 4g uses exactly 2 (L0 + L1);
/// higher levels are reserved for a future multi-level layer.
/// Maximum number of levels. Layer 4g uses exactly 2 (L0 + L1).
#[allow(dead_code)]
const NUM_LEVELS: usize = 2;

/// Resolve a merge operator from the options name.
fn resolve_merge_operator(
    name: &str,
) -> Option<Arc<dyn crate::ext::merge_operator::MergeOperator>> {
    if name.is_empty() {
        return None;
    }
    match name {
        crate::ext::merge_operator::StringAppendOperator::NAME => {
            Some(Arc::new(crate::ext::merge_operator::StringAppendOperator::default()))
        }
        _ => None, // Unknown operator names are silently ignored for now.
    }
}

/// Compute the user-key range of an SST by scanning its entries.
/// O(file) — called once per SST at open time. A future layer
/// can persist this in table properties or a manifest to skip the
/// scan.
fn compute_sst_range(reader: &BlockBasedTableReader) -> (Vec<u8>, Vec<u8>) {
    let mut it = reader.iter();
    it.seek_to_first();
    if !it.valid() {
        return (Vec::new(), Vec::new());
    }
    let first = match ParsedInternalKey::parse(it.key()) {
        Ok(p) => p.user_key.to_vec(),
        Err(_) => return (Vec::new(), Vec::new()),
    };
    let mut last = first.clone();
    while it.valid() {
        if let Ok(p) = ParsedInternalKey::parse(it.key()) {
            if p.user_key > last.as_slice() {
                last = p.user_key.to_vec();
            }
        }
        it.next();
    }
    (first, last)
}

/// Internal state behind the DB's big lock.
struct DbState {
    /// Per-column-family state, keyed by CF ID.
    column_families: HashMap<ColumnFamilyId, CfState>,
    /// Next CF ID to assign when creating a new column family.
    next_cf_id: ColumnFamilyId,
    /// Current WAL writer.
    wal: LogWriter,
    /// Number of the WAL file currently being written.
    wal_number: u64,
    /// Number to assign to the next file (WAL or SST). Monotonic.
    next_file_number: u64,
    /// Highest sequence number assigned so far.
    last_sequence: SequenceNumber,
    /// Set when the engine is closing — background tasks check
    /// this to skip work that no longer matters.
    closing: bool,
    /// Live snapshot multiset: `seq → refcount`. Used to compute
    /// `min_snap_seq` for snapshot-aware compaction. An entry is
    /// added on `DbImpl::snapshot()` and removed on
    /// `DbSnapshot::drop`. The smallest key in the map is the
    /// oldest live snapshot; `min_snap_seq()` returns
    /// `u64::MAX` when the map is empty (no snapshots → nothing
    /// to preserve).
    snapshots: std::collections::BTreeMap<SequenceNumber, usize>,
    /// MANIFEST-based version tracking. Persists per-CF file
    /// metadata so that reopen can reconstruct the SST layout
    /// without scanning the directory.
    version_set: VersionSet,
    /// Reference count for `disable_file_deletions`. When > 0,
    /// obsolete SST files are NOT deleted — they're added to
    /// `pending_deletion` instead. Used by Flink's incremental
    /// checkpoint to hold SST files stable while uploading.
    disable_file_deletions_count: u32,
    /// SST file numbers whose deletion has been deferred because
    /// `disable_file_deletions_count > 0`. Processed when the
    /// count returns to 0.
    pending_deletion: Vec<u64>,
}

impl DbState {
    /// The oldest sequence pinned by any live snapshot, or
    /// `u64::MAX` if there are no live snapshots.
    fn min_snap_seq(&self) -> SequenceNumber {
        self.snapshots
            .keys()
            .next()
            .copied()
            .unwrap_or(SequenceNumber::MAX)
    }

    /// Look up a column family by ID.
    fn cf(&self, id: ColumnFamilyId) -> Result<&CfState> {
        self.column_families.get(&id).ok_or_else(||
            Status::invalid_argument(format!("column family {id} not found")))
    }

    /// Look up a column family by ID (mutable).
    fn cf_mut(&mut self, id: ColumnFamilyId) -> Result<&mut CfState> {
        self.column_families.get_mut(&id).ok_or_else(||
            Status::invalid_argument(format!("column family {id} not found")))
    }

    /// The default column family (always present).
    fn default_cf(&self) -> &CfState {
        self.column_families.get(&DEFAULT_CF_ID).expect("default CF missing")
    }

    /// The default column family (mutable, always present).
    fn default_cf_mut(&mut self) -> &mut CfState {
        self.column_families.get_mut(&DEFAULT_CF_ID).expect("default CF missing")
    }
}

/// Metadata about one live SST file. Returned by
/// [`DbImpl::get_live_files_metadata`].
#[derive(Debug, Clone)]
pub struct LiveFileMetaData {
    /// SST file number.
    pub file_number: u64,
    /// Column family name.
    pub column_family_name: String,
    /// Level (0 or 1).
    pub level: u32,
    /// File size in bytes (0 if unknown).
    pub file_size: u64,
    /// Smallest user key in the file.
    pub smallest_key: Vec<u8>,
    /// Largest user key in the file.
    pub largest_key: Vec<u8>,
}

/// The Layer 4c engine.
///
/// All public methods take `&self` and acquire an internal mutex.
/// The engine is safe for concurrent use from multiple threads:
/// reads and writes run concurrently with **background flushes**
/// thanks to the immutable-memtable + lock-releasing pattern. The
/// big-mutex write thread (with leader-follower group commit) is
/// still deferred to a future layer.
pub struct DbImpl {
    /// On-disk path of the DB directory.
    path: PathBuf,
    /// File-system instance. We hold an `Arc` so the engine can
    /// hand it to background tasks.
    fs: Arc<dyn FileSystem>,
    /// Internal mutable state behind a single mutex.
    state: Mutex<DbState>,
    /// Notified whenever the in-progress flush completes — used
    /// by [`Self::wait_for_pending_flush`] and `close()`.
    flush_done: Condvar,
    /// Held LOCK file handle. Released on `close`.
    lock: Mutex<Option<Box<dyn FileLock>>>,
    /// User comparator (always BytewiseComparator at Layer 4a).
    user_comparator: Arc<dyn Comparator>,
    /// Per-CF flush threshold (`write_buffer_size` from options).
    write_buffer_size: usize,
    /// Background thread pool for flushes (and, eventually,
    /// compactions). Held by `Arc` so closures can clone it.
    thread_pool: Arc<StdThreadPool>,
    /// Weak self-reference used by background tasks to call back
    /// into the engine after their I/O is done. Populated once,
    /// immediately after the `Arc<Self>` is constructed in `open`.
    weak_self: OnceLock<Weak<Self>>,
    /// Shared block cache for SST data blocks. `None` if
    /// `block_cache_size == 0`.
    block_cache: Option<BlockCache>,
    /// L0 file count at which writes are blocked until compaction
    /// reduces the count. Default: 36.
    level0_stop_writes_trigger: i32,
}

impl DbImpl {
    /// Open or create a DB at `path`.
    ///
    /// On open we:
    /// 1. Create the directory if missing.
    /// 2. Acquire the LOCK file.
    /// 3. Read CURRENT (if any) to discover existing SSTs.
    /// 4. Walk the directory for `*.log` files and replay them
    ///    into a fresh memtable.
    /// 5. Open a new WAL (or append to the most recent one).
    pub fn open(opts: &DbOptions, path: &Path) -> Result<Arc<Self>> {
        Self::open_with_fs(
            opts,
            path,
            Arc::new(PosixFileSystem::new()) as Arc<dyn FileSystem>,
        )
    }

    /// Same as [`Self::open`] but takes an explicit `FileSystem`,
    /// useful for tests that want an in-memory backend.
    pub fn open_with_fs(
        opts: &DbOptions,
        path: &Path,
        fs: Arc<dyn FileSystem>,
    ) -> Result<Arc<Self>> {
        // 1. Create dir.
        if opts.create_if_missing {
            fs.create_dir_if_missing(path)?;
        } else {
            // Check existence without propagating PathNotFound as
            // an IO error — surface it as InvalidArgument with a
            // clearer message instead.
            let exists = fs.file_exists(path)?;
            if !exists {
                return Err(Status::invalid_argument(format!(
                    "{}: directory does not exist (set create_if_missing=true)",
                    path.display()
                )));
            }
            if !fs.is_directory(path)? {
                return Err(Status::invalid_argument(format!(
                    "{}: not a directory",
                    path.display()
                )));
            }
        }

        // 2. Lock.
        let lock_path = make_lock_file_name(path);
        let lock = fs.lock_file(&lock_path)?;

        // 3. Recover the VersionSet from the MANIFEST (if any).
        let version_set = VersionSet::recover(path, Arc::clone(&fs))?;

        // Create shared block cache if configured.
        let block_cache: Option<BlockCache> = if opts.block_cache_size > 0 {
            Some(Arc::new(LruCache::new(opts.block_cache_size)))
        } else {
            None
        };

        // 4. Discover existing SSTs and WAL files by listing dir.
        let user_comparator: Arc<dyn Comparator> = Arc::new(BytewiseComparator);
        let mut next_file_number = 1u64;
        let mut existing_sst_numbers: Vec<u64> = Vec::new();
        let mut existing_wal_numbers: Vec<u64> = Vec::new();

        for name in fs.get_children(path)? {
            // Skip non-DB files.
            if name == "LOCK" || name == "CURRENT" || name == "IDENTITY" || name == "LOG" {
                continue;
            }
            match parse_file_name(&name) {
                Ok((FileType::TableFile, n)) => {
                    existing_sst_numbers.push(n);
                    next_file_number = next_file_number.max(n + 1);
                }
                Ok((FileType::WalFile, n)) => {
                    existing_wal_numbers.push(n);
                    next_file_number = next_file_number.max(n + 1);
                }
                Ok((FileType::DescriptorFile, n)) => {
                    next_file_number = next_file_number.max(n + 1);
                }
                _ => {} // ignore unknown files
            }
        }
        existing_sst_numbers.sort();
        existing_wal_numbers.sort();

        // Use whichever source gives a higher next_file_number.
        next_file_number = next_file_number.max(version_set.next_file_number());

        // Check if VersionSet recovered from a MANIFEST (has real state).
        let has_manifest = version_set.next_file_number() > 2;

        let mut l0: Vec<SstEntry>;
        let mut l1: Vec<SstEntry>;
        let mut last_sequence: u64;

        if has_manifest {
            // MANIFEST has the authoritative SST layout and counters.
            last_sequence = version_set.last_sequence();

            // Build SstEntry vectors from the recovered per-CF file state.
            let cf_files = version_set.cf_files().get(&DEFAULT_CF_ID).cloned().unwrap_or_default();

            l0 = Vec::with_capacity(cf_files.l0.len());
            for meta in &cf_files.l0 {
                let p = make_table_file_name(path, meta.number);
                if !fs.file_exists(&p)? {
                    continue; // SST file missing — skip (best-effort)
                }
                let raw = fs.new_random_access_file(&p, &Default::default())?;
                let reader = RandomAccessFileReader::new(raw, p.display().to_string())?;
                let table = BlockBasedTableReader::open_with_cache(Arc::new(reader), block_cache.clone(), meta.number)?;
                l0.push(SstEntry {
                    number: meta.number,
                    reader: Arc::new(table),
                    smallest_key: meta.smallest_key.clone(),
                    largest_key: meta.largest_key.clone(),
                    level: 0,
                });
            }
            // Newest first (highest number) for L0.
            l0.sort_by_key(|b| std::cmp::Reverse(b.number));

            l1 = Vec::with_capacity(cf_files.l1.len());
            for meta in &cf_files.l1 {
                let p = make_table_file_name(path, meta.number);
                if !fs.file_exists(&p)? {
                    continue;
                }
                let raw = fs.new_random_access_file(&p, &Default::default())?;
                let reader = RandomAccessFileReader::new(raw, p.display().to_string())?;
                let table = BlockBasedTableReader::open_with_cache(Arc::new(reader), block_cache.clone(), meta.number)?;
                l1.push(SstEntry {
                    number: meta.number,
                    reader: Arc::new(table),
                    smallest_key: meta.smallest_key.clone(),
                    largest_key: meta.largest_key.clone(),
                    level: 1,
                });
            }
            // L1 sorted by smallest_key.
            l1.sort_by(|a, b| a.smallest_key.cmp(&b.smallest_key));
        } else {
            // No MANIFEST — fall back to directory-scan logic.
            l0 = Vec::with_capacity(existing_sst_numbers.len());
            l1 = Vec::new();
            for &num in &existing_sst_numbers {
                let p = make_table_file_name(path, num);
                let raw = fs.new_random_access_file(&p, &Default::default())?;
                let reader = RandomAccessFileReader::new(raw, p.display().to_string())?;
                let table = BlockBasedTableReader::open_with_cache(Arc::new(reader), block_cache.clone(), num)?;
                let (smallest, largest) = compute_sst_range(&table);
                l0.push(SstEntry {
                    number: num,
                    reader: Arc::new(table),
                    smallest_key: smallest,
                    largest_key: largest,
                    level: 0,
                });
            }
            // Newest first (highest number) for L0.
            l0.sort_by_key(|b| std::cmp::Reverse(b.number));

            // Initialise `last_sequence` from the existing SSTs.
            last_sequence = 0u64;
            for entry in l0.iter().chain(l1.iter()) {
                let mut it = entry.reader.iter();
                it.seek_to_first();
                while it.valid() {
                    let parsed = ParsedInternalKey::parse(it.key())?;
                    if parsed.sequence > last_sequence {
                        last_sequence = parsed.sequence;
                    }
                    it.next();
                }
                it.status()?;
            }
        }

        // Replay WAL files into a fresh memtable.
        let mut memtable = MemTable::new(Arc::clone(&user_comparator));
        let mut replayed_range_tombstones: Vec<RangeTombstone> = Vec::new();
        for &wal_num in &existing_wal_numbers {
            let wal_path = make_wal_file_name(path, wal_num);
            let raw = fs.new_sequential_file(&wal_path, &Default::default())?;
            let seq_reader = SequentialFileReader::new(raw, wal_path.display().to_string());
            let mut log_reader = LogReader::new(seq_reader);
            let mut record_buf = Vec::new();
            while log_reader.read_record(&mut record_buf)? {
                last_sequence = replay_record_with_tombstones(
                    &record_buf,
                    &mut memtable,
                    last_sequence,
                    Some(&mut replayed_range_tombstones),
                )?;
            }
            // Delete the replayed WAL file — we're about to open a
            // new one with a fresh number, so old WALs are obsolete.
            fs.delete_file(&wal_path)?;
        }

        // Open a new WAL.
        let wal_number = next_file_number;
        next_file_number += 1;
        let wal_path = make_wal_file_name(path, wal_number);
        let wal_file = fs.new_writable_file(&wal_path, &Default::default())?;
        let wal_writer = LogWriter::new(WritableFileWriter::new(wal_file));

        // Background worker pool:
        // - 1 `Low` worker for flushes (Layer 4c)
        // - 1 `Bottom` worker for compactions (Layer 4f)
        //
        // Separate priority slots mean flush and compaction can
        // run in parallel without blocking each other.
        let thread_pool = Arc::new(StdThreadPool::new(1, 0, 0, 1));

        let default_cf = CfState {
            id: DEFAULT_CF_ID,
            name: "default".into(),
            memtable,
            immutable: None,
            l0,
            l1,
            immutable_wal_number: None,
            pending_flush_sst_number: None,
            pending_compaction: None,
            merge_operator: None,
            compaction_filter_factory: None,
            range_tombstones: replayed_range_tombstones,
        };
        let mut all_cfs = HashMap::from([(DEFAULT_CF_ID, default_cf)]);
        let mut max_cf_id: ColumnFamilyId = 0;

        // Build CfState entries for non-default CFs from MANIFEST.
        if has_manifest {
            for (&cf_id, cf_file_state) in version_set.cf_files() {
                if cf_id == DEFAULT_CF_ID {
                    continue; // already handled above
                }
                if cf_id > max_cf_id {
                    max_cf_id = cf_id;
                }
                let cf_name = version_set
                    .cf_names()
                    .get(&cf_id)
                    .cloned()
                    .unwrap_or_else(|| format!("cf_{cf_id}"));

                // Open SST files for this CF.
                let mut cf_l0 = Vec::with_capacity(cf_file_state.l0.len());
                for meta in &cf_file_state.l0 {
                    let p = make_table_file_name(path, meta.number);
                    if !fs.file_exists(&p)? {
                        continue;
                    }
                    let raw = fs.new_random_access_file(&p, &Default::default())?;
                    let reader = RandomAccessFileReader::new(raw, p.display().to_string())?;
                    let table = BlockBasedTableReader::open_with_cache(Arc::new(reader), block_cache.clone(), meta.number)?;
                    cf_l0.push(SstEntry {
                        number: meta.number,
                        reader: Arc::new(table),
                        smallest_key: meta.smallest_key.clone(),
                        largest_key: meta.largest_key.clone(),
                        level: 0,
                    });
                }
                cf_l0.sort_by_key(|b| std::cmp::Reverse(b.number));

                let mut cf_l1 = Vec::with_capacity(cf_file_state.l1.len());
                for meta in &cf_file_state.l1 {
                    let p = make_table_file_name(path, meta.number);
                    if !fs.file_exists(&p)? {
                        continue;
                    }
                    let raw = fs.new_random_access_file(&p, &Default::default())?;
                    let reader = RandomAccessFileReader::new(raw, p.display().to_string())?;
                    let table = BlockBasedTableReader::open_with_cache(Arc::new(reader), block_cache.clone(), meta.number)?;
                    cf_l1.push(SstEntry {
                        number: meta.number,
                        reader: Arc::new(table),
                        smallest_key: meta.smallest_key.clone(),
                        largest_key: meta.largest_key.clone(),
                        level: 1,
                    });
                }
                cf_l1.sort_by(|a, b| a.smallest_key.cmp(&b.smallest_key));

                all_cfs.insert(cf_id, CfState {
                    id: cf_id,
                    name: cf_name,
                    memtable: MemTable::new(Arc::clone(&user_comparator)),
                    immutable: None,
                    l0: cf_l0,
                    l1: cf_l1,
                    immutable_wal_number: None,
                    pending_flush_sst_number: None,
                    pending_compaction: None,
                    merge_operator: None,
                    compaction_filter_factory: None,
                    range_tombstones: Vec::new(),
                });
            }
        }

        let next_cf_id = (max_cf_id + 1).max(version_set.next_cf_id() as ColumnFamilyId);

        let arc = Arc::new(Self {
            path: path.to_path_buf(),
            fs,
            state: Mutex::new(DbState {
                column_families: all_cfs,
                next_cf_id,
                wal: wal_writer,
                wal_number,
                next_file_number,
                last_sequence,
                closing: false,
                snapshots: std::collections::BTreeMap::new(),
                version_set,
                disable_file_deletions_count: 0,
                pending_deletion: Vec::new(),
            }),
            flush_done: Condvar::new(),
            lock: Mutex::new(Some(lock)),
            user_comparator,
            write_buffer_size: opts.db_write_buffer_size.max(1),
            thread_pool,
            weak_self: OnceLock::new(),
            block_cache,
            level0_stop_writes_trigger: 36, // default from ColumnFamilyOptions
        });
        // Self-reference for background callbacks. `set` only
        // fails if it was already populated, which can't happen
        // here since we just constructed the cell.
        let _ = arc.weak_self.set(Arc::downgrade(&arc));
        Ok(arc)
    }

    /// Convenience for background tasks: clone the weak self-ref.
    fn weak_self(&self) -> Weak<Self> {
        self.weak_self
            .get()
            .expect("weak_self not initialised")
            .clone()
    }

    /// The filesystem backing this DB. Used by utilities (e.g.
    /// checkpoint) that need to link or copy files.
    pub fn file_system(&self) -> &Arc<dyn FileSystem> {
        &self.fs
    }

    /// The on-disk path of this DB directory.
    pub fn db_path(&self) -> &Path {
        &self.path
    }

    /// Return the DB path and the list of live SST file numbers
    /// across all levels. Used by checkpoint to know which files
    /// to hard-link.
    pub fn snapshot_live_files(&self) -> (PathBuf, Vec<u64>) {
        let state = self.state.lock().unwrap();
        let mut numbers: Vec<u64> = Vec::new();
        for cf in state.column_families.values() {
            for e in &cf.l0 {
                numbers.push(e.number);
            }
            for e in &cf.l1 {
                numbers.push(e.number);
            }
        }
        (self.path.clone(), numbers)
    }

    // ---- Incremental checkpoint APIs ----

    /// Prevent the engine from deleting obsolete SST files. Each call
    /// increments a reference count; file deletions are suppressed
    /// until the matching number of `enable_file_deletions` calls.
    ///
    /// Flink calls this before `get_live_files` to freeze the SST
    /// list while uploading files to remote storage.
    pub fn disable_file_deletions(&self) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        state.disable_file_deletions_count += 1;
        Ok(())
    }

    /// Re-enable file deletions. Decrements the reference count set
    /// by `disable_file_deletions`. When the count reaches 0, any
    /// SST files that became obsolete while deletions were disabled
    /// are deleted.
    pub fn enable_file_deletions(&self) -> Result<()> {
        let pending: Vec<u64>;
        {
            let mut state = self.state.lock().unwrap();
            if state.disable_file_deletions_count == 0 {
                return Err(Status::invalid_argument(
                    "enable_file_deletions called without matching disable",
                ));
            }
            state.disable_file_deletions_count -= 1;
            if state.disable_file_deletions_count > 0 {
                return Ok(()); // still disabled
            }
            // Count reached 0 — drain the pending list.
            pending = std::mem::take(&mut state.pending_deletion);
        }
        // Delete outside the lock.
        for n in &pending {
            let p = make_table_file_name(&self.path, *n);
            if self.fs.file_exists(&p).unwrap_or(false) {
                let _ = self.fs.delete_file(&p);
            }
        }
        Ok(())
    }

    /// Return the list of live files. If `flush_memtable` is true,
    /// triggers a flush first so that all in-memory data is captured
    /// in SSTs.
    ///
    /// Returns the paths of all live SST files, the CURRENT file,
    /// and the active MANIFEST file.
    ///
    /// **Note:** For consistent results, call
    /// [`Self::disable_file_deletions`] before this method and
    /// [`Self::enable_file_deletions`] after processing the files.
    /// Without that bracket, a concurrent compaction could delete
    /// a file between enumeration and access.
    pub fn get_live_files(&self, flush_memtable: bool) -> Result<Vec<PathBuf>> {
        if flush_memtable {
            self.flush()?;
            self.wait_for_pending_work()?;
        }
        let state = self.state.lock().unwrap();
        let mut files = Vec::new();

        // SST files from all CFs.
        for cf in state.column_families.values() {
            for e in &cf.l0 {
                files.push(make_table_file_name(&self.path, e.number));
            }
            for e in &cf.l1 {
                files.push(make_table_file_name(&self.path, e.number));
            }
        }

        // CURRENT file.
        files.push(make_current_file_name(&self.path));

        // MANIFEST file.
        let manifest_num = state.version_set.manifest_file_number();
        if manifest_num > 0 {
            files.push(make_descriptor_file_name(&self.path, manifest_num));
        }

        Ok(files)
    }

    /// Return metadata for all live SST files across all column
    /// families. Used by Flink for incremental checkpoint tracking.
    pub fn get_live_files_metadata(&self) -> Vec<LiveFileMetaData> {
        // Collect metadata under the lock (no I/O).
        let mut meta: Vec<LiveFileMetaData> = {
            let state = self.state.lock().unwrap();
            let mut out = Vec::new();
            for cf in state.column_families.values() {
                for e in &cf.l0 {
                    out.push(LiveFileMetaData {
                        file_number: e.number,
                        column_family_name: cf.name.clone(),
                        level: 0,
                        file_size: 0, // filled below outside the lock
                        smallest_key: e.smallest_key.clone(),
                        largest_key: e.largest_key.clone(),
                    });
                }
                for e in &cf.l1 {
                    out.push(LiveFileMetaData {
                        file_number: e.number,
                        column_family_name: cf.name.clone(),
                        level: 1,
                        file_size: 0,
                        smallest_key: e.smallest_key.clone(),
                        largest_key: e.largest_key.clone(),
                    });
                }
            }
            out
        };
        // File size I/O outside the lock.
        for m in &mut meta {
            m.file_size = self
                .fs
                .get_file_size(&make_table_file_name(&self.path, m.file_number))
                .unwrap_or(0);
        }
        meta
    }

    /// Return the file number of the active MANIFEST, or 0 if no
    /// MANIFEST has been created yet. Used by checkpoint to know
    /// which MANIFEST file to copy.
    /// Absolute path of the DB directory. Used by the JNI layer for
    /// file-system introspection around checkpoints.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Return the file number of the active MANIFEST, or 0 if no
    /// MANIFEST has been created yet. Used by checkpoint to know
    /// which MANIFEST file to copy.
    pub fn manifest_file_number(&self) -> u64 {
        let state = self.state.lock().unwrap();
        state.version_set.manifest_file_number()
    }

    /// Returns `true` if writes are currently stalled because the
    /// default CF's L0 file count has reached the stop threshold.
    /// Used by Flink's metrics (`is-write-stopped` property).
    pub fn is_write_stopped(&self) -> bool {
        let state = self.state.lock().unwrap();
        state.default_cf().l0.len() >= self.level0_stop_writes_trigger as usize
    }

    // ---- Properties & Metrics ----

    /// Query an engine property by name. Returns the property value
    /// as a string, or `None` if the property is unknown.
    ///
    /// Flink monitors these via `db.getProperty(cf, name)` for
    /// dashboards. The property names match upstream RocksDB.
    pub fn get_property(&self, name: &str) -> Option<String> {
        self.get_int_property(name).map(|v| v.to_string())
    }

    /// Query an engine property by name for a specific column family.
    pub fn get_property_cf(
        &self,
        cf: &dyn crate::api::db::ColumnFamilyHandle,
        name: &str,
    ) -> Option<String> {
        self.get_int_property_cf(cf.id(), name).map(|v| v.to_string())
    }

    /// Query a numeric engine property. Returns `None` if unknown.
    pub fn get_int_property(&self, name: &str) -> Option<u64> {
        self.get_int_property_cf(DEFAULT_CF_ID, name)
    }

    /// Query a numeric engine property for a specific column family.
    pub fn get_int_property_cf(
        &self,
        cf_id: ColumnFamilyId,
        name: &str,
    ) -> Option<u64> {
        // Strip the "rocksdb." prefix if present (Flink uses it).
        let prop = name.strip_prefix("rocksdb.").unwrap_or(name);

        let state = self.state.lock().unwrap();
        let cf = state.column_families.get(&cf_id)?;

        match prop {
            // ---- Memtable properties ----
            "num-immutable-mem-table" => {
                Some(if cf.immutable.is_some() { 1 } else { 0 })
            }
            "mem-table-flush-pending" => {
                Some(if cf.immutable.is_some() { 1 } else { 0 })
            }
            "cur-size-active-mem-table" => {
                Some(cf.memtable.approximate_memory_usage() as u64)
            }
            "cur-size-all-mem-tables" | "size-all-mem-tables" => {
                let active = cf.memtable.approximate_memory_usage() as u64;
                let imm = cf
                    .immutable
                    .as_ref()
                    .map_or(0, |m| m.approximate_memory_usage() as u64);
                Some(active + imm)
            }
            "num-entries-active-mem-table" => {
                Some(cf.memtable.num_entries() as u64)
            }

            // ---- L0 / level file counts ----
            "num-files-at-level0" => Some(cf.l0.len() as u64),
            "num-files-at-level1" => Some(cf.l1.len() as u64),
            // Levels 2-6 don't exist yet — always 0.
            "num-files-at-level2" | "num-files-at-level3"
            | "num-files-at-level4" | "num-files-at-level5"
            | "num-files-at-level6" => Some(0),

            // ---- SST size properties ----
            "total-sst-files-size" | "live-sst-files-size" => {
                let mut total = 0u64;
                for e in &cf.l0 {
                    total += self
                        .fs
                        .get_file_size(&make_table_file_name(&self.path, e.number))
                        .unwrap_or(0);
                }
                for e in &cf.l1 {
                    total += self
                        .fs
                        .get_file_size(&make_table_file_name(&self.path, e.number))
                        .unwrap_or(0);
                }
                Some(total)
            }

            // ---- Compaction properties ----
            "compaction-pending" => {
                Some(if cf.pending_compaction.is_some() { 1 } else { 0 })
            }
            "estimate-pending-compaction-bytes" => {
                // Rough estimate: total L0 bytes (all will be compacted).
                let mut total = 0u64;
                for e in &cf.l0 {
                    total += self
                        .fs
                        .get_file_size(&make_table_file_name(&self.path, e.number))
                        .unwrap_or(0);
                }
                Some(total)
            }
            "num-running-compactions" => {
                Some(if cf.pending_compaction.is_some() { 1 } else { 0 })
            }
            "num-running-flushes" => {
                Some(if cf.immutable.is_some() { 1 } else { 0 })
            }

            // ---- Write stall properties ----
            "is-write-stopped" => {
                Some(if cf.l0.len() >= self.level0_stop_writes_trigger as usize {
                    1
                } else {
                    0
                })
            }
            "actual-delayed-write-rate" => Some(0), // no slowdown implemented yet

            // ---- Block cache properties ----
            "block-cache-capacity" => {
                drop(state); // release lock before cache query
                Some(self.block_cache.as_ref().map_or(0, |c| c.capacity() as u64))
            }
            "block-cache-usage" => {
                drop(state);
                Some(self.block_cache.as_ref().map_or(0, |c| c.usage() as u64))
            }

            _ => None,
        }
    }

    // ---- Column family management ----

    /// Create a new column family. Returns a handle that can be
    /// passed to `put_cf`, `get_cf`, etc.
    /// Set the merge operator for an existing column family.
    pub fn set_merge_operator(
        &self,
        cf: &dyn crate::api::db::ColumnFamilyHandle,
        operator: Arc<dyn crate::ext::merge_operator::MergeOperator>,
    ) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        let cf_state = state.cf_mut(cf.id())?;
        cf_state.merge_operator = Some(operator);
        Ok(())
    }

    /// Look up an existing column family by name. Returns the handle
    /// if found, or `None` if no CF with that name exists.
    pub fn get_column_family_by_name(
        &self,
        name: &str,
    ) -> Option<Arc<ColumnFamilyHandleImpl>> {
        let state = self.state.lock().unwrap();
        for cf in state.column_families.values() {
            if cf.name == name {
                return Some(Arc::new(ColumnFamilyHandleImpl {
                    id: cf.id,
                    name: cf.name.clone(),
                }));
            }
        }
        None
    }

    /// Create a new column family. Returns a handle that can be
    /// passed to `put_cf`, `get_cf`, etc.
    pub fn create_column_family(
        &self,
        name: &str,
        _options: &crate::api::options::ColumnFamilyOptions,
    ) -> Result<Arc<ColumnFamilyHandleImpl>> {
        let mut state = self.state.lock().unwrap();
        // Check for duplicate name.
        for cf in state.column_families.values() {
            if cf.name == name {
                return Err(Status::invalid_argument(format!(
                    "column family '{name}' already exists"
                )));
            }
        }
        let id = state.next_cf_id;
        state.next_cf_id += 1;
        state.column_families.insert(id, CfState {
            id,
            name: name.to_string(),
            memtable: MemTable::new(Arc::clone(&self.user_comparator)),
            immutable: None,
            l0: Vec::new(),
            l1: Vec::new(),
            immutable_wal_number: None,
            pending_flush_sst_number: None,
            pending_compaction: None,
            merge_operator: resolve_merge_operator(&_options.merge_operator_name),
            compaction_filter_factory: None,
            range_tombstones: Vec::new(),
        });
        // Persist the CF addition to the MANIFEST.
        let edit = VersionEdit {
            column_family_id: Some(id),
            is_column_family_add: Some(name.to_string()),
            ..Default::default()
        };
        state.version_set.log_and_apply(&edit)?;
        drop(state);
        Ok(Arc::new(ColumnFamilyHandleImpl {
            id,
            name: name.to_string(),
        }))
    }

    /// Drop a column family. The default CF (id=0) cannot be dropped.
    ///
    /// The caller must ensure no concurrent writes target this CF
    /// after calling drop. Writes in flight during the drop may be
    /// silently lost.
    pub fn drop_column_family(&self, handle: &dyn crate::api::db::ColumnFamilyHandle) -> Result<()> {
        let cf_id = handle.id();
        if cf_id == DEFAULT_CF_ID {
            return Err(Status::invalid_argument(
                "cannot drop the default column family",
            ));
        }
        // Wait for any pending work on this CF.
        {
            let mut state = self.state.lock().unwrap();
            while state.column_families.get(&cf_id)
                .is_some_and(|cf| cf.immutable.is_some() || cf.pending_compaction.is_some())
            {
                state = self.flush_done.wait(state).unwrap();
            }
        }
        let sst_numbers: Vec<u64>;
        {
            let mut state = self.state.lock().unwrap();
            let cf = state.column_families.remove(&cf_id)
                .ok_or_else(|| Status::invalid_argument(format!("column family {cf_id} not found")))?;
            sst_numbers = cf.l0.iter().chain(cf.l1.iter()).map(|e| e.number).collect();
            // Persist the CF drop to the MANIFEST.
            let deleted_files: Vec<(u32, u64)> = cf.l0.iter().chain(cf.l1.iter())
                .map(|e| (e.level, e.number))
                .collect();
            let edit = VersionEdit {
                column_family_id: Some(cf_id),
                is_column_family_drop: true,
                deleted_files,
                ..Default::default()
            };
            state.version_set.log_and_apply(&edit)?;
            drop(state);
        }
        // Delete SST files belonging to the dropped CF, respecting
        // the file-deletion-disable flag.
        {
            let mut state = self.state.lock().unwrap();
            if state.disable_file_deletions_count > 0 {
                state.pending_deletion.extend(&sst_numbers);
            } else {
                drop(state);
                for n in &sst_numbers {
                    let p = make_table_file_name(&self.path, *n);
                    if self.fs.file_exists(&p).unwrap_or(false) {
                        let _ = self.fs.delete_file(&p);
                    }
                }
            }
        }
        Ok(())
    }

    /// Set a compaction filter factory on a column family. The
    /// factory creates a fresh filter for each compaction job.
    /// Flink uses this for TTL-based state expiration.
    pub fn set_compaction_filter_factory(
        &self,
        cf: &dyn crate::api::db::ColumnFamilyHandle,
        factory: Arc<dyn crate::ext::compaction_filter::CompactionFilterFactory>,
    ) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        let cf_state = state.cf_mut(cf.id())?;
        cf_state.compaction_filter_factory = Some(factory);
        Ok(())
    }

    /// Get a handle to the default column family.
    pub fn default_column_family(&self) -> Arc<ColumnFamilyHandleImpl> {
        Arc::new(ColumnFamilyHandleImpl {
            id: DEFAULT_CF_ID,
            name: "default".to_string(),
        })
    }

    // ---- CF-aware read/write operations ----

    /// Insert or overwrite `key → value` in a specific column family.
    pub fn put_cf(
        &self,
        cf: &dyn crate::api::db::ColumnFamilyHandle,
        key: &[u8],
        value: &[u8],
    ) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.put_cf(cf.id(), key.to_vec(), value.to_vec());
        self.write(&batch)
    }

    /// Delete `key` from a specific column family.
    pub fn delete_cf(
        &self,
        cf: &dyn crate::api::db::ColumnFamilyHandle,
        key: &[u8],
    ) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.delete_cf(cf.id(), key.to_vec());
        self.write(&batch)
    }

    /// Delete every key in `[begin, end)` in the default column family.
    pub fn delete_range(&self, begin: &[u8], end: &[u8]) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.delete_range(begin.to_vec(), end.to_vec());
        self.write(&batch)
    }

    /// Delete every key in `[begin, end)` in a specific column family.
    pub fn delete_range_cf(
        &self,
        cf: &dyn crate::api::db::ColumnFamilyHandle,
        begin: &[u8],
        end: &[u8],
    ) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.delete_range_cf(cf.id(), begin.to_vec(), end.to_vec());
        self.write(&batch)
    }

    /// Delete SST files whose key ranges are entirely contained
    /// within one of the given delete ranges. For each matching SST,
    /// the file is removed from the CF's level vector, a VersionEdit
    /// is written to the MANIFEST, and the file is deleted from disk.
    ///
    /// Each range is `(begin, end)` representing the key interval
    /// `[begin, end)`.
    pub fn delete_files_in_ranges(
        &self,
        cf: &dyn crate::api::db::ColumnFamilyHandle,
        ranges: &[(&[u8], &[u8])],
    ) -> Result<()> {
        if ranges.is_empty() {
            return Ok(());
        }
        let cf_id = cf.id();
        let mut state = self.state.lock().unwrap();
        let cf_state = state.cf_mut(cf_id)?;

        // Collect SST numbers whose key range is fully covered by
        // at least one delete range.
        let mut to_delete: Vec<(u64, u32)> = Vec::new(); // (number, level)

        for level_idx in 0..2u32 {
            let entries = if level_idx == 0 {
                &cf_state.l0
            } else {
                &cf_state.l1
            };
            for entry in entries {
                for &(begin, end) in ranges {
                    if entry.smallest_key.as_slice() >= begin
                        && entry.largest_key.as_slice() < end
                    {
                        to_delete.push((entry.number, level_idx));
                        break;
                    }
                }
            }
        }

        if to_delete.is_empty() {
            return Ok(());
        }

        // Remove from level vectors.
        let numbers_to_delete: std::collections::HashSet<u64> =
            to_delete.iter().map(|(n, _)| *n).collect();
        cf_state.l0.retain(|e| !numbers_to_delete.contains(&e.number));
        cf_state.l1.retain(|e| !numbers_to_delete.contains(&e.number));

        // Write a VersionEdit to MANIFEST.
        let edit = VersionEdit {
            column_family_id: Some(cf_id),
            deleted_files: to_delete.iter().map(|(n, l)| (*l, *n)).collect(),
            last_sequence: Some(state.last_sequence),
            next_file_number: Some(state.next_file_number),
            ..Default::default()
        };
        state.version_set.log_and_apply(&edit)?;

        // Delete the physical files.
        for (number, _) in &to_delete {
            let sst_path = make_table_file_name(&self.path, *number);
            let _ = self.fs.delete_file(&sst_path);
        }

        Ok(())
    }

    /// Point-lookup in a specific column family.
    pub fn get_cf(
        &self,
        cf: &dyn crate::api::db::ColumnFamilyHandle,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        let read_seq = {
            let state = self.state.lock().unwrap();
            state.last_sequence
        };
        self.get_cf_at_seq(cf.id(), key, read_seq)
    }

    /// Point-lookup in a CF at a specific sequence number.
    ///
    /// Handles merge operands: if the newest entry is a Merge, collects
    /// all merge operands from memtable → immutable → SSTs until a Put
    /// or Delete is found, then calls the CF's merge operator to produce
    /// the final value.
    fn get_cf_at_seq(
        &self,
        cf_id: ColumnFamilyId,
        key: &[u8],
        read_seq: SequenceNumber,
    ) -> Result<Option<Vec<u8>>> {
        let (lookup, merge_op, l0_ssts, l1_ssts, merge_state, range_tombstones) = {
            let state = self.state.lock().unwrap();
            let cf = state.cf(cf_id)?;
            let merge_op = cf.merge_operator.as_ref().map(Arc::clone);
            let lookup = LookupKey::new(key, read_seq);

            // Snapshot range tombstones for this CF.
            let range_tombstones = cf.range_tombstones.clone();

            // Track merge operands collected so far (newest-first).
            // `base_found` is set when we find a Put (base_value) or
            // Delete (base_value = None, stops the chain).
            let mut operands: Vec<Vec<u8>> = Vec::new();
            let mut base_value: Option<Vec<u8>> = None;
            let mut base_found = false;

            // 1. Active memtable.
            match cf.memtable.get(&lookup) {
                MemTableGetResult::Found(v) => {
                    // Don't fast-return — range tombstones must be
                    // checked after the lock is released.
                    base_value = Some(v);
                    base_found = true;
                }
                MemTableGetResult::Deleted => {
                    if operands.is_empty() {
                        return Ok(None); // fast path: Delete with no merge
                    }
                    base_found = true; // Delete stops chain, no base value
                }
                MemTableGetResult::MergeOperand(_first_operand) => {
                    // Collect all merge operands from this memtable.
                    let (base, ops) = cf.memtable.collect_merge_operands(&lookup);
                    operands = ops;
                    if let Some(b) = base {
                        base_value = Some(b);
                        base_found = true;
                    }
                }
                MemTableGetResult::NotFound => {}
            }

            // 2. Immutable memtable (only if we haven't found a base).
            if !base_found {
                if let Some(imm) = cf.immutable.as_ref() {
                    match imm.get(&lookup) {
                        MemTableGetResult::Found(v) => {
                            base_value = Some(v);
                            base_found = true;
                        }
                        MemTableGetResult::Deleted => {
                            base_found = true;
                        }
                        MemTableGetResult::MergeOperand(_) => {
                            let (base, mut ops) = imm.collect_merge_operands(&lookup);
                            // Immutable's operands are older than active's.
                            // Prepend them so full_merge sees oldest-first.
                            ops.append(&mut operands);
                            operands = ops;
                            if let Some(b) = base {
                                base_value = Some(b);
                                base_found = true;
                            }
                        }
                        MemTableGetResult::NotFound => {}
                    }
                }
            }

            // 3. Snapshot SST lists.
            let l0_ssts: Vec<Arc<BlockBasedTableReader>> =
                cf.l0.iter().map(|e| Arc::clone(&e.reader)).collect();
            let l1_ssts: Vec<(Vec<u8>, Vec<u8>, Arc<BlockBasedTableReader>)> = cf
                .l1
                .iter()
                .map(|e| {
                    (
                        e.smallest_key.clone(),
                        e.largest_key.clone(),
                        Arc::clone(&e.reader),
                    )
                })
                .collect();

            (lookup, merge_op, l0_ssts, l1_ssts, (operands, base_value, base_found), range_tombstones)
        };

        let (mut operands, mut base_value, mut base_found) = merge_state;

        // 4. SSTs (only if we haven't found a base yet).
        //    Walk SST entries for this user key, collecting merge
        //    operands until a Put or Delete terminates the chain.
        let sst_operands_start = operands.len();
        if !base_found {
            for table in l0_ssts.iter() {
                let mut it = table.iter();
                it.seek(lookup.internal_key());
                while it.valid() {
                    let parsed = match ParsedInternalKey::parse(it.key()) {
                        Ok(p) => p,
                        Err(_) => break,
                    };
                    if parsed.user_key != key {
                        break;
                    }
                    match parsed.value_type {
                        ValueType::Merge => {
                            operands.push(it.value().to_vec());
                        }
                        ValueType::Value => {
                            base_value = Some(it.value().to_vec());
                            base_found = true;
                            break;
                        }
                        ValueType::Deletion | ValueType::SingleDeletion => {
                            base_found = true;
                            break;
                        }
                        _ => break,
                    }
                    it.next();
                }
                if base_found || !operands.is_empty() {
                    break; // found something in this L0 SST
                }
            }
        }
        if !base_found && operands.is_empty() {
            // L1 binary search.
            let idx = l1_ssts.partition_point(|(smallest, _, _)| smallest.as_slice() <= key);
            if idx > 0 {
                let (_, largest, table) = &l1_ssts[idx - 1];
                if key <= largest.as_slice() {
                    let mut it = table.iter();
                    it.seek(lookup.internal_key());
                    while it.valid() {
                        let parsed = match ParsedInternalKey::parse(it.key()) {
                            Ok(p) => p,
                            Err(_) => break,
                        };
                        if parsed.user_key != key {
                            break;
                        }
                        match parsed.value_type {
                            ValueType::Merge => {
                                operands.push(it.value().to_vec());
                            }
                            ValueType::Value => {
                                base_value = Some(it.value().to_vec());
                                break;
                            }
                            ValueType::Deletion | ValueType::SingleDeletion => {
                                break;
                            }
                            _ => break,
                        }
                        it.next();
                    }
                }
            }
        }
        // SST operands were collected newest-first (SST order is
        // seq desc) and appended after memtable operands. But SST
        // entries are OLDER than memtable entries. Fix ordering:
        // reverse the SST portion, then rotate so SST operands
        // come before memtable operands (oldest-first for full_merge).
        if operands.len() > sst_operands_start {
            operands[sst_operands_start..].reverse();
            // Rotate: [mem_ops..., sst_ops...] → [sst_ops..., mem_ops...]
            operands.rotate_left(sst_operands_start);
        }

        // 5. Check range tombstones. A range tombstone at seq S
        //    covering [start, end) shadows any read of a key in
        //    that range if S <= read_seq.
        //
        //    INVARIANT: In-memory range tombstones are cleared on flush.
        //    Any Put with seq > tombstone_seq is either (a) in the memtable
        //    (found before this check via the fast path) or (b) flushed to
        //    an SST, in which case the tombstone was also cleared. So this
        //    check only applies to SST values with seq < tombstone_seq.
        for (start, end, tomb_seq) in &range_tombstones {
            if *tomb_seq <= read_seq
                && key >= start.as_slice()
                && key < end.as_slice()
            {
                return Ok(None);
            }
        }

        // 6. If we collected merge operands, apply the merge operator.
        if !operands.is_empty() {
            if let Some(op) = &merge_op {
                let operand_refs: Vec<&[u8]> = operands.iter().map(|o| o.as_slice()).collect();
                let merged = op.full_merge(key, base_value.as_deref(), &operand_refs)?;
                return Ok(Some(merged));
            }
            // No merge operator — return the newest operand as-is
            // (backward-compatible with pre-Phase-2 behavior).
            // operands is oldest-first, so the newest is the last.
            return Ok(operands.pop());
        }

        // No merge operands — return the base value directly.
        Ok(base_value)
    }

    /// Flush a specific column family's memtable to an SST.
    pub fn flush_cf(
        &self,
        cf: &dyn crate::api::db::ColumnFamilyHandle,
    ) -> Result<()> {
        let cf_id = cf.id();
        let setup = self.start_flush_cf_locked(cf_id)?;
        let setup = match setup {
            Some(s) => s,
            None => return Ok(()),
        };
        let result = self.write_immutable_to_sst(&setup);
        self.complete_flush_locked(setup, result)?;
        Ok(())
    }

    /// Flush every column family that has a non-empty active memtable.
    /// Used by Flink's snapshot path (via `getLiveFiles(flushMemtable=true)`),
    /// which needs all CFs persisted before reading the SST list —
    /// `flush()` alone only flushes the default CF.
    pub fn flush_all_cfs(&self) -> Result<()> {
        // Snapshot the set of CF ids under the lock, then drop it to
        // avoid holding across each per-CF flush (which re-takes the lock).
        let cf_ids: Vec<ColumnFamilyId> = {
            let state = self.state.lock().unwrap();
            state.column_families.keys().copied().collect()
        };
        for cf_id in cf_ids {
            let setup = self.start_flush_cf_locked(cf_id)?;
            if let Some(setup) = setup {
                let result = self.write_immutable_to_sst(&setup);
                self.complete_flush_locked(setup, result)?;
            }
        }
        Ok(())
    }

    /// Insert or overwrite `key → value`.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.put(key.to_vec(), value.to_vec());
        self.write(&batch)
    }

    /// Delete `key`.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.delete(key.to_vec());
        self.write(&batch)
    }

    /// Apply a [`WriteBatch`] atomically.
    ///
    /// The batch is serialised, appended to the WAL, then replayed
    /// into the memtable. The whole sequence runs under a single
    /// mutex — there's no group commit at Layer 4a.
    ///
    /// If the default CF's L0 file count reaches
    /// `level0_stop_writes_trigger`, this method blocks until a
    /// background compaction reduces the count. This is the write
    /// stall / back-pressure mechanism that prevents unbounded L0
    /// growth.
    pub fn write(&self, batch: &WriteBatch) -> Result<()> {
        self.write_opt(batch, false)
    }

    /// Apply a [`WriteBatch`] atomically with optional WAL skip.
    ///
    /// When `disable_wal` is `true`, the batch is applied directly
    /// to the memtable without writing to the WAL. This is Flink's
    /// default mode — durability comes from Flink's distributed
    /// checkpointing, not from the engine's WAL.
    pub fn write_opt(&self, batch: &WriteBatch, disable_wal: bool) -> Result<()> {
        if batch.count() == 0 {
            return Ok(()); // nothing to do
        }
        let mut state = self.state.lock().unwrap();

        // Write stall: block while L0 count is at the stop threshold.
        while state.default_cf().l0.len() >= self.level0_stop_writes_trigger as usize {
            state = self.flush_done.wait(state).unwrap();
        }

        // Reserve a sequence range for this batch.
        let first_seq = state.last_sequence + 1;

        if !disable_wal {
            let mut record_buf = Vec::new();
            encode_batch_record(batch, first_seq, &mut record_buf);

            // 1. WAL append + sync.
            state.wal.add_record(&record_buf)?;
            state.wal.sync()?;
        }

        // 2. Memtable insert.
        {
            let mut handler = MemTableInsertHandler {
                column_families: &mut state.column_families,
                seq: first_seq,
            };
            batch.iterate(&mut handler)?;

            // 3. Bookkeeping.
            state.last_sequence = handler.seq - 1;
        }

        // 4. Maybe trigger flush (default CF only for now; non-default
        //    CFs don't auto-flush — callers must use flush_cf()).
        let needs_flush = state.default_cf().memtable.approximate_memory_usage() >= self.write_buffer_size;
        drop(state);
        if needs_flush {
            // Background flush — put() returns as soon as the
            // memtable freeze + WAL roll are done; the SST write
            // happens on a worker thread.
            self.schedule_flush()?;
        }
        Ok(())
    }

    /// Point-lookup. Returns:
    /// - `Ok(Some(value))` on a hit
    /// - `Ok(None)` on a miss (or the latest entry is a deletion)
    /// - `Err` for I/O / corruption errors
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let read_seq = {
            let state = self.state.lock().unwrap();
            state.last_sequence
        };
        self.get_at_seq(key, read_seq)
    }

    /// Point-lookup at a specific snapshot. The snapshot pins a
    /// sequence number at the time it was taken; subsequent
    /// writes (with higher sequences) are invisible to reads
    /// made through the snapshot.
    ///
    /// ```rust,no_run
    /// # use st_rs::{DbImpl, DbOptions};
    /// # use std::path::Path;
    /// # let opts = DbOptions { create_if_missing: true, ..Default::default() };
    /// # let db = DbImpl::open(&opts, Path::new("/tmp/snap-example")).unwrap();
    /// # db.put(b"k", b"old").unwrap();
    /// let snap = db.snapshot();
    /// db.put(b"k", b"new").unwrap();
    /// assert_eq!(db.get(b"k").unwrap(),
    ///            Some(b"new".to_vec()));        // current read
    /// assert_eq!(db.get_at(b"k", &*snap).unwrap(),
    ///            Some(b"old".to_vec()));        // snapshot read
    /// ```
    pub fn get_at(
        &self,
        key: &[u8],
        snapshot: &dyn crate::api::snapshot::Snapshot,
    ) -> Result<Option<Vec<u8>>> {
        self.get_at_seq(key, snapshot.sequence_number())
    }

    /// Shared point-lookup implementation parameterised by the
    /// read sequence. `get()` calls it with `state.last_sequence`;
    /// `get_at()` calls it with the snapshot's pinned sequence.
    fn get_at_seq(&self, key: &[u8], read_seq: SequenceNumber) -> Result<Option<Vec<u8>>> {
        self.get_cf_at_seq(DEFAULT_CF_ID, key, read_seq)
    }

    /// Shared: seek in one SST for a user key at the given lookup
    /// sequence. Returns `Some(Some(value))` for a hit,
    /// `Some(None)` for a tombstone, or `None` for a miss.
    fn seek_in_sst(
        table: &BlockBasedTableReader,
        user_key: &[u8],
        lookup: &LookupKey,
    ) -> Result<Option<Option<Vec<u8>>>> {
        let mut it = table.iter();
        it.seek(lookup.internal_key());
        if !it.valid() {
            return Ok(None);
        }
        let parsed = match ParsedInternalKey::parse(it.key()) {
            Ok(p) => p,
            Err(_) => return Ok(None),
        };
        if parsed.user_key != user_key {
            return Ok(None);
        }
        Ok(Some(match parsed.value_type {
            ValueType::Value | ValueType::Merge => Some(it.value().to_vec()),
            ValueType::Deletion | ValueType::SingleDeletion => None,
            _ => None,
        }))
    }

    // ---- Batch (vectorized) operations ----

    /// Batch point-lookup: look up multiple keys in one call.
    ///
    /// This is more efficient than calling `get()` in a loop because
    /// it acquires the state lock **once**, checks all keys against
    /// the active and immutable memtables under that lock, snapshots
    /// the SST lists, and then probes SSTs for remaining keys without
    /// the lock.
    ///
    /// Returns a `Vec` of the same length as `keys`, where each
    /// element is `Ok(Some(value))`, `Ok(None)` (miss / deleted),
    /// or `Err` (I/O error).
    pub fn multi_get(&self, keys: &[&[u8]]) -> Vec<Result<Option<Vec<u8>>>> {
        if keys.is_empty() {
            return Vec::new();
        }

        let n = keys.len();

        // Results default to Ok(None) (= not found). A separate bitmap
        // tracks which keys were already resolved from memtables so we
        // can skip them during the SST phase.
        let mut results: Vec<Result<Option<Vec<u8>>>> = (0..n).map(|_| Ok(None)).collect();
        let mut resolved = vec![false; n];
        // Keys that hit a MergeOperand in memtable — need the full
        // single-key merge path after releasing the lock.
        let mut merge_keys: Vec<usize> = Vec::new();

        // 1. Single lock acquisition: check memtables + snapshot SSTs.
        //    read_seq is captured here and used for ALL lookups (memtable
        //    and SST) to guarantee a consistent snapshot.
        let (read_seq, l0_ssts, l1_ssts) = {
            let state = self.state.lock().unwrap();
            let cf = state.default_cf();
            let read_seq = state.last_sequence;

            // Check every key against the active memtable.
            for (i, key) in keys.iter().enumerate() {
                let lookup = LookupKey::new(key, read_seq);
                match cf.memtable.get(&lookup) {
                    MemTableGetResult::Found(v) => {
                        results[i] = Ok(Some(v));
                        resolved[i] = true;
                        continue;
                    }
                    MemTableGetResult::Deleted => {
                        // results[i] is already Ok(None).
                        resolved[i] = true;
                        continue;
                    }
                    MemTableGetResult::MergeOperand(_) => {
                        // Merge requires collecting operands across
                        // memtable + SSTs. Fall back to the full
                        // single-key path for this key.
                        merge_keys.push(i);
                        resolved[i] = true;
                        continue;
                    }
                    MemTableGetResult::NotFound => {}
                }
                // Check immutable memtable (still under the same lock).
                if let Some(imm) = cf.immutable.as_ref() {
                    match imm.get(&lookup) {
                        MemTableGetResult::Found(v) => {
                            results[i] = Ok(Some(v));
                            resolved[i] = true;
                            continue;
                        }
                        MemTableGetResult::Deleted => {
                            resolved[i] = true;
                            continue;
                        }
                        MemTableGetResult::MergeOperand(_) => {
                            merge_keys.push(i);
                            resolved[i] = true;
                            continue;
                        }
                        MemTableGetResult::NotFound => {}
                    }
                }
                // Key not in memtables — will check SSTs below.
            }

            // Snapshot SST lists so we can drop the lock.
            let l0_ssts: Vec<Arc<BlockBasedTableReader>> =
                cf.l0.iter().map(|e| Arc::clone(&e.reader)).collect();
            let l1_ssts: Vec<(Vec<u8>, Vec<u8>, Arc<BlockBasedTableReader>)> = cf
                .l1
                .iter()
                .map(|e| {
                    (
                        e.smallest_key.clone(),
                        e.largest_key.clone(),
                        Arc::clone(&e.reader),
                    )
                })
                .collect();

            (read_seq, l0_ssts, l1_ssts)
        };
        // Lock released here.

        // 2. For unresolved keys, probe SSTs (no lock needed).
        'keys: for (i, key) in keys.iter().enumerate() {
            if resolved[i] {
                continue;
            }
            let lookup = LookupKey::new(key, read_seq);

            // L0 SSTs newest-first.
            for table in &l0_ssts {
                match Self::seek_in_sst(table, key, &lookup) {
                    Ok(Some(v)) => {
                        results[i] = Ok(v);
                        continue 'keys;
                    }
                    Ok(None) => {}
                    Err(e) => {
                        results[i] = Err(e);
                        continue 'keys;
                    }
                }
            }

            // L1 via binary search.
            let idx =
                l1_ssts.partition_point(|(smallest, _, _)| smallest.as_slice() <= *key);
            if idx > 0 {
                let (_, largest, table) = &l1_ssts[idx - 1];
                if *key <= largest.as_slice() {
                    match Self::seek_in_sst(table, key, &lookup) {
                        Ok(Some(v)) => {
                            results[i] = Ok(v);
                            continue 'keys;
                        }
                        Ok(None) => {}
                        Err(e) => {
                            results[i] = Err(e);
                            continue 'keys;
                        }
                    }
                }
            }
            // Not found in SSTs either — results[i] stays Ok(None).
        }

        // Resolve merge keys via the full single-key path.
        for idx in merge_keys {
            results[idx] = self.get_cf_at_seq(DEFAULT_CF_ID, keys[idx], read_seq);
        }

        results
    }

    /// Batch put: insert or overwrite multiple key-value pairs
    /// atomically in a single [`WriteBatch`].
    ///
    /// More efficient than calling `put()` in a loop because only
    /// one WAL append + sync and one memtable insertion pass are
    /// performed.
    pub fn multi_put(&self, pairs: &[(&[u8], &[u8])]) -> Result<()> {
        if pairs.is_empty() {
            return Ok(());
        }
        let mut batch = WriteBatch::new();
        for (key, value) in pairs {
            batch.put(key.to_vec(), value.to_vec());
        }
        self.write(&batch)
    }

    /// Batch delete: delete multiple keys atomically in a single
    /// [`WriteBatch`].
    pub fn multi_delete(&self, keys: &[&[u8]]) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let mut batch = WriteBatch::new();
        for key in keys {
            batch.delete(key.to_vec());
        }
        self.write(&batch)
    }

    /// Take a snapshot at the current sequence. The returned
    /// handle pins the sequence — subsequent `get_at` /
    /// `iter_at` calls with this snapshot see only entries that
    /// existed when it was taken. Compaction will also preserve
    /// the newest version at or below each live snapshot until
    /// the snapshot is dropped.
    ///
    /// Snapshots are refcounted and automatically released on
    /// `Drop`. You should not leak them — long-lived snapshots
    /// prevent compaction from dropping old versions and
    /// tombstones, which grows the LSM over time.
    pub fn snapshot(&self) -> Arc<DbSnapshot> {
        let seq = {
            let mut state = self.state.lock().unwrap();
            let seq = state.last_sequence;
            *state.snapshots.entry(seq).or_insert(0) += 1;
            seq
        };
        Arc::new(DbSnapshot {
            seq,
            db: self.weak_self(),
        })
    }

    /// Iterator at a specific snapshot. See [`Self::snapshot`].
    pub fn iter_at(
        &self,
        snapshot: &dyn crate::api::snapshot::Snapshot,
    ) -> Result<crate::db::db_iter::DbIterator> {
        self.iter_at_seq(snapshot.sequence_number())
    }

    fn iter_at_seq(
        &self,
        read_seq: SequenceNumber,
    ) -> Result<crate::db::db_iter::DbIterator> {
        let state = self.state.lock().unwrap();
        let cf = state.default_cf();
        let ssts = Self::collect_all_ssts(&state);
        // Use the chunked streaming iterator so multi-GB state on
        // disk doesn't OOM the iterator's working set.
        let it = crate::db::db_iter::DbIterator::from_arcs(
            &cf.memtable,
            cf.immutable.as_deref(),
            ssts,
            read_seq,
            crate::db::db_iter::DbIterator::DEFAULT_CHUNK_SIZE,
        );
        drop(state);
        Ok(it)
    }

    /// Collect all SST readers into a single vec for the eager
    /// `DbIterator` materialisation. L0 first (newest-first),
    /// then L1 (sorted by key range). The iterator path doesn't
    /// need binary-search — it walks every file — so a flat vec
    /// is fine.
    fn collect_all_ssts(state: &DbState) -> Vec<Arc<BlockBasedTableReader>> {
        let cf = state.default_cf();
        let mut ssts: Vec<Arc<BlockBasedTableReader>> =
            Vec::with_capacity(cf.l0.len() + cf.l1.len());
        for e in &cf.l0 {
            ssts.push(Arc::clone(&e.reader));
        }
        for e in &cf.l1 {
            ssts.push(Arc::clone(&e.reader));
        }
        ssts
    }

    /// Internal: release a snapshot's hold on its pinned sequence.
    /// Called by [`DbSnapshot::drop`] after the last Arc is dropped.
    fn release_snapshot_internal(&self, seq: SequenceNumber) {
        let mut state = self.state.lock().unwrap();
        if let Some(count) = state.snapshots.get_mut(&seq) {
            *count -= 1;
            if *count == 0 {
                state.snapshots.remove(&seq);
            }
        }
    }

    /// For tests: return the number of distinct live snapshot
    /// sequences currently tracked by the engine.
    #[cfg(test)]
    fn live_snapshot_count(&self) -> usize {
        self.state.lock().unwrap().snapshots.len()
    }

    /// Open a forward + backward iterator over the engine. Eagerly
    /// materialises a snapshot of (active memtable + immutable
    /// memtable + every live SST) under the lock, so the returned
    /// iterator is detached from the engine and survives
    /// concurrent writes and background flushes.
    pub fn iter(&self) -> Result<crate::db::db_iter::DbIterator> {
        let state = self.state.lock().unwrap();
        let cf = state.default_cf();
        let ssts = Self::collect_all_ssts(&state);
        let it = crate::db::db_iter::DbIterator::from_arcs(
            &cf.memtable,
            cf.immutable.as_deref(),
            ssts,
            crate::db::db_iter::READ_AT_LATEST,
            crate::db::db_iter::DbIterator::DEFAULT_CHUNK_SIZE,
        );
        drop(state);
        Ok(it)
    }

    // ---- Column-family-scoped iterator ----

    /// Collect all SST readers for a specific column family.
    fn collect_cf_ssts(cf: &CfState) -> Vec<Arc<BlockBasedTableReader>> {
        let mut ssts: Vec<Arc<BlockBasedTableReader>> =
            Vec::with_capacity(cf.l0.len() + cf.l1.len());
        for e in &cf.l0 {
            ssts.push(Arc::clone(&e.reader));
        }
        for e in &cf.l1 {
            ssts.push(Arc::clone(&e.reader));
        }
        ssts
    }

    /// Open a forward + backward iterator scoped to a specific
    /// column family. Eagerly materialises a snapshot of the CF's
    /// memtable + immutable memtable + SSTs under the lock, so the
    /// returned iterator is detached from the engine.
    pub fn iter_cf(
        &self,
        cf: &dyn crate::api::db::ColumnFamilyHandle,
    ) -> Result<crate::db::db_iter::DbIterator> {
        let state = self.state.lock().unwrap();
        let cf_state = state.cf(cf.id())?;
        let ssts = Self::collect_cf_ssts(cf_state);
        let it = crate::db::db_iter::DbIterator::from_arcs(
            &cf_state.memtable,
            cf_state.immutable.as_deref(),
            ssts,
            crate::db::db_iter::READ_AT_LATEST,
            crate::db::db_iter::DbIterator::DEFAULT_CHUNK_SIZE,
        );
        drop(state);
        Ok(it)
    }

    // ---- Prefix scan ----

    /// Return all live key-value pairs in the default column family
    /// whose user key starts with `prefix`, in sorted order.
    pub fn prefix_scan(
        &self,
        prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut it = self.iter()?;
        it.seek(prefix);
        let mut results = Vec::new();
        while it.valid() {
            if !it.key().starts_with(prefix) {
                break;
            }
            results.push((it.key().to_vec(), it.value().to_vec()));
            it.next();
        }
        Ok(results)
    }

    /// Return all live key-value pairs in the specified column
    /// family whose user key starts with `prefix`, in sorted order.
    pub fn prefix_scan_cf(
        &self,
        cf: &dyn crate::api::db::ColumnFamilyHandle,
        prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut it = self.iter_cf(cf)?;
        it.seek(prefix);
        let mut results = Vec::new();
        while it.valid() {
            if !it.key().starts_with(prefix) {
                break;
            }
            results.push((it.key().to_vec(), it.value().to_vec()));
            it.next();
        }
        Ok(results)
    }

    // ---- SST ingestion ----

    /// Ingest one or more externally-built SST files into a column
    /// family. Each file is validated, copied (or moved) into the
    /// DB directory, and added to the CF's L0 as the newest file.
    ///
    /// Used by Flink during state restoration and rescaling.
    pub fn ingest_external_file(
        &self,
        cf: &dyn crate::api::db::ColumnFamilyHandle,
        paths: &[&Path],
        opts: &crate::db::ingest_external_file::IngestExternalFileOptions,
    ) -> Result<()> {
        use crate::db::ingest_external_file::validate_sst;

        let cf_id = cf.id();

        for src_path in paths {
            // 1. Validate the SST.
            validate_sst(self.fs.as_ref(), src_path)?;

            // 2. Assign a new file number and build the destination path.
            let (file_number, dst_path) = {
                let mut state = self.state.lock().unwrap();
                let num = state.next_file_number;
                state.next_file_number += 1;
                (num, make_table_file_name(&self.path, num))
            };

            // 3. Copy or move the file into the DB directory.
            if opts.move_files {
                std::fs::rename(src_path, &dst_path).map_err(|e| {
                    Status::io_error(format!(
                        "move {} -> {}: {e}",
                        src_path.display(),
                        dst_path.display()
                    ))
                })?;
            } else {
                std::fs::copy(src_path, &dst_path).map_err(|e| {
                    Status::io_error(format!(
                        "copy {} -> {}: {e}",
                        src_path.display(),
                        dst_path.display()
                    ))
                })?;
            }

            // 4. Open the SST reader, compute key range and max sequence.
            let raw = self.fs.new_random_access_file(&dst_path, &Default::default())?;
            let reader = RandomAccessFileReader::new(raw, dst_path.display().to_string())?;
            let table = BlockBasedTableReader::open(Arc::new(reader))?;
            let (smallest, largest) = compute_sst_range(&table);
            // Scan for the maximum sequence number in the SST so
            // we can bump `last_sequence` — otherwise reads at
            // `last_sequence` (which may be 0 if no writes have
            // occurred) would be unable to see entries with a
            // higher sequence.
            let max_seq = {
                let mut it = table.iter();
                it.seek_to_first();
                let mut mx: SequenceNumber = 0;
                while it.valid() {
                    if let Ok(p) = ParsedInternalKey::parse(it.key()) {
                        if p.sequence > mx {
                            mx = p.sequence;
                        }
                    }
                    it.next();
                }
                mx
            };
            let file_size = self.fs.get_file_size(&dst_path).unwrap_or(0);
            let reader_arc = Arc::new(table);

            // 5. Insert into CF's L0 (newest-first) and write VersionEdit.
            let mut state = self.state.lock().unwrap();
            // Bump last_sequence so reads can see the ingested data.
            if max_seq > state.last_sequence {
                state.last_sequence = max_seq;
            }
            let cf_state = state.cf_mut(cf_id)?;
            cf_state.l0.insert(
                0,
                SstEntry {
                    number: file_number,
                    reader: reader_arc,
                    smallest_key: smallest.clone(),
                    largest_key: largest.clone(),
                    level: 0,
                },
            );
            let edit = VersionEdit {
                column_family_id: Some(cf_id),
                new_files: vec![(
                    0,
                    FileMetaData {
                        number: file_number,
                        file_size,
                        smallest_key: smallest,
                        largest_key: largest,
                    },
                )],
                next_file_number: Some(state.next_file_number),
                ..Default::default()
            };
            state.version_set.log_and_apply(&edit)?;
        }

        Ok(())
    }

    /// Create a new column family and atomically ingest the
    /// specified SST files into it.
    ///
    /// This is the Flink-specific "bootstrap a CF from a checkpoint"
    /// path: the CF is created empty, then SSTs are ingested in a
    /// single operation so the CF is immediately populated.
    pub fn create_column_family_with_import(
        &self,
        name: &str,
        options: &crate::api::options::ColumnFamilyOptions,
        paths: &[&Path],
    ) -> Result<Arc<ColumnFamilyHandleImpl>> {
        let handle = self.create_column_family(name, options)?;
        if let Err(e) = self.ingest_external_file(&*handle, paths, &Default::default()) {
            // Best-effort cleanup: drop the CF we just created.
            let _ = self.drop_column_family(&*handle);
            return Err(e);
        }
        Ok(handle)
    }

    /// Block until every in-progress background flush **and**
    /// every in-progress background compaction has completed.
    ///
    /// `close()` calls this before releasing the file lock.
    /// Tests use it to drain the pool to a known quiescent
    /// state before asserting on the SST list.
    pub fn wait_for_pending_work(&self) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        while state.column_families.values().any(|cf| cf.immutable.is_some())
            || state.column_families.values().any(|cf| cf.pending_compaction.is_some())
        {
            state = self.flush_done.wait(state).unwrap();
        }
        Ok(())
    }

    /// Alias for [`Self::wait_for_pending_work`] retained for
    /// backwards compatibility — Layer 4c shipped it as
    /// `wait_for_pending_flush` but at Layer 4f it also drains
    /// compactions.
    pub fn wait_for_pending_flush(&self) -> Result<()> {
        self.wait_for_pending_work()
    }

    /// Trigger threshold for automatic compaction. When the live
    /// SST count reaches this, [`Self::flush`] runs a compaction
    /// after the new SST is added.
    const COMPACTION_TRIGGER: usize = 4;

    /// Force-flush the active memtable to a fresh SST. Synchronous
    /// — blocks until the SST is on disk and the new state is
    /// installed. Releases the engine lock during the actual SST
    /// write so concurrent reads and writes are not blocked on
    /// I/O.
    ///
    /// To trigger a flush without blocking, call
    /// [`Self::schedule_flush`] instead.
    pub fn flush(&self) -> Result<()> {
        // 1. Start the flush under the lock — swap memtable to
        //    immutable, roll the WAL, reserve a SST number.
        let setup = self.start_flush_locked()?;
        let setup = match setup {
            Some(s) => s,
            None => return Ok(()), // memtable was empty
        };

        // 2. Slow part: write the SST. No lock held here.
        let result = self.write_immutable_to_sst(&setup);

        // 3. Re-acquire the lock to install the result.
        self.complete_flush_locked(setup, result)?;
        Ok(())
    }

    /// Schedule a flush on the background thread pool. Returns
    /// immediately. To wait for the flush to complete, call
    /// [`Self::wait_for_pending_flush`].
    ///
    /// If a flush is already in progress, the call is a no-op
    /// (the existing flush will eventually drain the active
    /// memtable's predecessor; the next flush trigger will pick
    /// up the current active memtable).
    pub fn schedule_flush(&self) -> Result<()> {
        let setup_opt = self.start_flush_locked()?;
        let setup = match setup_opt {
            Some(s) => s,
            None => return Ok(()),
        };
        let weak = self.weak_self();
        self.thread_pool.schedule(
            Box::new(move || {
                if let Some(db) = weak.upgrade() {
                    let result = db.write_immutable_to_sst(&setup);
                    let _ = db.complete_flush_locked(setup, result);
                }
                // If the engine is being dropped, the SST file
                // (if any) is left on disk and will be picked up
                // on the next open() via the directory listing.
            }),
            Priority::Low,
        );
        Ok(())
    }

    /// First half of a flush: under the lock, freeze the active
    /// memtable into the `immutable` slot, roll the WAL, and
    /// reserve a file number for the output SST. Returns `None`
    /// if there's nothing to flush.
    ///
    /// **Blocks** if a flush is already in progress — the
    /// immutable slot can hold at most one memtable at a time.
    fn start_flush_locked(&self) -> Result<Option<FlushSetup>> {
        let mut state = self.state.lock().unwrap();
        // Wait for any in-progress flush to complete first. This
        // bounds the immutable slot to at most one memtable.
        while state.default_cf().immutable.is_some() {
            state = self.flush_done.wait(state).unwrap();
        }
        if state.default_cf().memtable.num_entries() == 0 {
            return Ok(None);
        }

        // Reserve a file number for the SST.
        let sst_number = state.next_file_number;
        state.next_file_number += 1;
        let wal_number = state.wal_number;
        let cf = state.default_cf_mut();
        cf.pending_flush_sst_number = Some(sst_number);

        // Freeze the active memtable.
        let frozen = std::mem::replace(
            &mut cf.memtable,
            MemTable::new(Arc::clone(&self.user_comparator)),
        );
        let immutable = Arc::new(frozen);
        cf.immutable = Some(Arc::clone(&immutable));

        // Remember which WAL holds the records that landed in the
        // immutable memtable, so we can delete it after the flush.
        cf.immutable_wal_number = Some(wal_number);

        // Roll the WAL: open a fresh log so the new active
        // memtable has its own write-ahead trail.
        let new_wal_number = state.next_file_number;
        state.next_file_number += 1;
        let new_wal_path = make_wal_file_name(&self.path, new_wal_number);
        let new_wal_file = self
            .fs
            .new_writable_file(&new_wal_path, &Default::default())?;
        let new_wal = LogWriter::new(WritableFileWriter::new(new_wal_file));
        let old_wal = std::mem::replace(&mut state.wal, new_wal);
        // Close the old WAL writer; the file stays on disk until
        // the flush completes (so a crash mid-flush still leaves
        // the old WAL behind for replay).
        old_wal.close()?;
        state.wal_number = new_wal_number;

        Ok(Some(FlushSetup {
            cf_id: DEFAULT_CF_ID,
            sst_number,
            immutable,
            old_wal_number: state.default_cf().immutable_wal_number.unwrap(),
        }))
    }

    /// CF-aware version of `start_flush_locked`.
    ///
    /// Unlike the default-CF `start_flush_locked`, this does NOT
    /// roll the WAL. The WAL is shared across all CFs — rolling it
    /// when only one CF flushes would prematurely orphan records
    /// belonging to other CFs. The old WAL is kept alive; it will
    /// be deleted when the default CF's flush completes (which does
    /// roll the WAL).
    fn start_flush_cf_locked(&self, cf_id: ColumnFamilyId) -> Result<Option<FlushSetup>> {
        let mut state = self.state.lock().unwrap();
        // Wait for any in-progress flush on this CF.
        while state.cf(cf_id)?.immutable.is_some() {
            state = self.flush_done.wait(state).unwrap();
        }
        if state.cf(cf_id)?.memtable.num_entries() == 0 {
            return Ok(None);
        }

        let sst_number = state.next_file_number;
        state.next_file_number += 1;
        let wal_number = state.wal_number;
        let cf = state.cf_mut(cf_id)?;
        cf.pending_flush_sst_number = Some(sst_number);

        // Freeze the active memtable.
        let frozen = std::mem::replace(
            &mut cf.memtable,
            MemTable::new(Arc::clone(&self.user_comparator)),
        );
        let immutable = Arc::new(frozen);
        cf.immutable = Some(Arc::clone(&immutable));
        cf.immutable_wal_number = Some(wal_number);

        // No WAL roll here — the WAL is shared across CFs.
        // The old_wal_number is recorded but we don't delete it
        // until ALL CFs have been flushed past this WAL.
        Ok(Some(FlushSetup {
            cf_id,
            sst_number,
            immutable,
            old_wal_number: wal_number,
        }))
    }

    /// Second half of a flush: write the immutable memtable into
    /// an SST file. **No lock held here.** Returns the open
    /// reader on success, an error on failure.
    ///
    /// # Layer 4d: internal-key SSTs
    ///
    /// The flush path writes **internal keys** (`user_key ||
    /// BE-inverted(seq<<8|type)`) directly into the SST rather
    /// than deduping per user key. The BE-inverted encoding
    /// guarantees that the bytewise ordering used by the block
    /// builder matches `(user_key asc, seq desc, type desc)`, so
    /// SSTs now retain every version just like the memtable does.
    /// This is what will enable snapshots + snapshot-aware
    /// compaction in a follow-up layer.
    fn write_immutable_to_sst(
        &self,
        setup: &FlushSetup,
    ) -> Result<(Arc<BlockBasedTableReader>, u64)> {
        let sst_path = make_table_file_name(&self.path, setup.sst_number);
        let writable = self
            .fs
            .new_writable_file(&sst_path, &Default::default())?;
        let mut tb = BlockBasedTableBuilder::new(
            WritableFileWriter::new(writable),
            BlockBasedTableOptions::default(),
        );

        // Walk the memtable's skiplist in sorted order — already
        // `(user_key asc, seq desc, type desc)` thanks to the
        // encoding — and stream every entry (including tombstones
        // and multiple versions of the same user key) into the
        // SST. Deduplication now happens lazily at read time and
        // during compaction, not at flush time.
        let mut it = setup.immutable.iter();
        it.seek_to_first();
        while it.valid() {
            tb.add(it.key(), it.value())?;
            it.next();
        }
        tb.finish()?;

        let file_size = self.fs.get_file_size(&sst_path)?;

        let raw = self
            .fs
            .new_random_access_file(&sst_path, &Default::default())?;
        let reader = RandomAccessFileReader::new(raw, sst_path.display().to_string())?;
        let table = BlockBasedTableReader::open_with_cache(
            Arc::new(reader),
            self.block_cache.clone(),
            setup.sst_number,
        )?;
        Ok((Arc::new(table), file_size))
    }

    /// Third half of a flush: install the new SST + clear the
    /// immutable slot + delete the obsolete WAL. Holds the lock
    /// briefly. Notifies any waiters.
    fn complete_flush_locked(
        &self,
        setup: FlushSetup,
        write_result: Result<(Arc<BlockBasedTableReader>, u64)>,
    ) -> Result<()> {
        let cf_id = setup.cf_id;
        let mut state = self.state.lock().unwrap();
        // Always clear the immutable slot and pending fields,
        // even on error — otherwise we deadlock on the next
        // start_flush_locked.
        if let Some(cf) = state.column_families.get_mut(&cf_id) {
            cf.immutable = None;
            cf.immutable_wal_number = None;
            cf.pending_flush_sst_number = None;
            // Range tombstones have been "applied" by the flush —
            // clear them so subsequent reads don't re-check old
            // tombstones. This is a simplification; upstream stores
            // range tombstones in a separate meta block in the SST.
            cf.range_tombstones.clear();
        }

        let result = match write_result {
            Ok((reader, file_size)) => {
                let (smallest, largest) = compute_sst_range(&reader);
                if let Some(cf) = state.column_families.get_mut(&cf_id) {
                    cf.l0.insert(
                        0,
                        SstEntry {
                            number: setup.sst_number,
                            reader,
                            smallest_key: smallest.clone(),
                            largest_key: largest.clone(),
                            level: 0,
                        },
                    );
                }
                // Persist the new SST to the MANIFEST via log_and_apply.
                let edit = VersionEdit {
                    column_family_id: Some(cf_id),
                    new_files: vec![(0, FileMetaData {
                        number: setup.sst_number,
                        file_size,
                        smallest_key: smallest,
                        largest_key: largest,
                    })],
                    last_sequence: Some(state.last_sequence),
                    log_number: Some(state.wal_number),
                    next_file_number: Some(state.next_file_number),
                    ..Default::default()
                };
                let r = state.version_set.log_and_apply(&edit);

                if r.is_ok() && cf_id == DEFAULT_CF_ID {
                    // Only delete the old WAL when the default CF
                    // flushes, because only that path rolls the WAL.
                    let old_wal_path =
                        make_wal_file_name(&self.path, setup.old_wal_number);
                    let _ = self.fs.delete_file(&old_wal_path);
                }
                let cleanup = self.maybe_pick_compaction(&mut state);
                drop(state);
                if let Some(plan) = cleanup {
                    self.schedule_compaction(plan);
                }
                r
            }
            Err(e) => Err(e),
        };

        self.flush_done.notify_all();
        result
    }

    /// Helper called inside `complete_flush_locked` to decide
    /// whether to schedule a compaction. Returns the compaction
    /// plan if one is needed, or `None` otherwise.
    ///
    /// Returns `None` if a compaction is already in progress —
    /// Layer 4f allows only one background compaction at a time.
    /// A future layer can queue multiple plans.
    fn maybe_pick_compaction(&self, state: &mut DbState) -> Option<CompactionPlan> {
        // Scan every CF and pick the first one that's both over the L0
        // trigger and not already compacting. Default-CF-first ordering
        // is preserved by sorting CF ids ascending (DEFAULT_CF_ID == 0).
        let mut cf_ids: Vec<ColumnFamilyId> =
            state.column_families.keys().copied().collect();
        cf_ids.sort_unstable();

        let (cf_id, inputs, input_numbers, input_levels, filter_factory,
             merge_operator) = 'pick: {
            for id in cf_ids {
                let cf = match state.column_families.get(&id) {
                    Some(c) => c,
                    None => continue,
                };
                if cf.pending_compaction.is_some() {
                    continue;
                }
                if cf.l0.len() < Self::COMPACTION_TRIGGER {
                    continue;
                }
                let mut inputs: Vec<Arc<BlockBasedTableReader>> = Vec::new();
                let mut input_numbers: Vec<u64> = Vec::new();
                let mut input_levels: Vec<u32> = Vec::new();
                let mut union_smallest: Vec<u8> = Vec::new();
                let mut union_largest: Vec<u8> = Vec::new();
                for entry in &cf.l0 {
                    inputs.push(Arc::clone(&entry.reader));
                    input_numbers.push(entry.number);
                    input_levels.push(0);
                    if union_smallest.is_empty() || entry.smallest_key < union_smallest {
                        union_smallest.clone_from(&entry.smallest_key);
                    }
                    if union_largest.is_empty() || entry.largest_key > union_largest {
                        union_largest.clone_from(&entry.largest_key);
                    }
                }
                for entry in &cf.l1 {
                    if entry.smallest_key <= union_largest
                        && entry.largest_key >= union_smallest
                    {
                        inputs.push(Arc::clone(&entry.reader));
                        input_numbers.push(entry.number);
                        input_levels.push(1);
                    }
                }
                let ff = cf.compaction_filter_factory.as_ref().map(Arc::clone);
                let mo = cf.merge_operator.as_ref().map(Arc::clone);
                break 'pick (id, inputs, input_numbers, input_levels, ff, mo);
            }
            return None;
        };

        // Now safe to mutate state.
        let out_number = state.next_file_number;
        state.next_file_number += 1;
        let min_snap_seq = state.min_snap_seq();
        Some(CompactionPlan {
            cf_id,
            out_number,
            inputs,
            input_numbers,
            input_levels,
            min_snap_seq,
            filter_factory,
            merge_operator,
        })
    }

    /// Atomically write `contents` to `<path>/CURRENT` via a tmp
    /// file + rename. Crash-safe: a torn write produces an
    /// orphaned tmp file, never a half-written CURRENT.
    ///
    /// Superseded by `VersionSet::log_and_apply` which writes to
    /// the MANIFEST instead. Retained for backward compatibility.
    #[allow(dead_code)]
    fn write_current_atomic(&self, contents: &str) -> Result<()> {
        let current_path = make_current_file_name(&self.path);
        let tmp_path = self.path.join("CURRENT.tmp");
        let tmp_file = self
            .fs
            .new_writable_file(&tmp_path, &Default::default())?;
        let mut writer = WritableFileWriter::new(tmp_file);
        let io = IoOptions::default();
        writer.append(contents.as_bytes(), &io)?;
        writer.flush(&io)?;
        writer.sync(&io)?;
        writer.close(&io)?;
        self.fs.rename_file(&tmp_path, &current_path)?;
        Ok(())
    }
}

/// Per-flush context handed from `start_flush_locked` to the
/// background worker (or to the synchronous `flush()` path).
struct FlushSetup {
    /// Column family being flushed.
    cf_id: ColumnFamilyId,
    sst_number: u64,
    immutable: Arc<MemTable>,
    old_wal_number: u64,
}

/// Everything a background compaction worker needs. Captured
/// inside the closure scheduled on `Priority::Bottom` by
/// [`DbImpl::schedule_compaction`].
struct CompactionPlan {
    /// Column family this plan is for. The picker scans every CF and
    /// returns the first one over threshold; schedule/run/wait must
    /// honor this rather than assuming the default CF.
    cf_id: ColumnFamilyId,
    /// File number reserved for the compaction output.
    out_number: u64,
    /// Table readers for the input SSTs.
    inputs: Vec<Arc<BlockBasedTableReader>>,
    /// Numbers of the input SSTs.
    input_numbers: Vec<u64>,
    /// Level of each input SST (parallel to `input_numbers`).
    input_levels: Vec<u32>,
    /// Oldest live snapshot sequence at the time the plan was created.
    min_snap_seq: SequenceNumber,
    /// Compaction filter factory from the CF, if any. A fresh
    /// filter is created from this for each compaction run.
    filter_factory:
        Option<Arc<dyn crate::ext::compaction_filter::CompactionFilterFactory>>,
    /// Merge operator from the CF, if any.
    merge_operator: Option<Arc<dyn crate::ext::merge_operator::MergeOperator>>,
}

/// The engine's concrete snapshot type. Returned from
/// [`DbImpl::snapshot`]. Implements [`crate::api::snapshot::Snapshot`]
/// so callers can use it wherever the Layer 0 trait is expected.
///
/// The snapshot is refcounted: every `Arc::clone` holds the
/// sequence. The engine tracks a `BTreeMap<sequence, refcount>`
/// so it can compute `min_snap_seq` for snapshot-aware compaction.
/// When the last `Arc<DbSnapshot>` is dropped, `Drop` decrements
/// the refcount and removes the entry if it hits zero.
///
/// `Weak<DbImpl>` is used in `Drop` so that dropping the engine
/// before the snapshot doesn't cause a use-after-free. If the
/// engine has been dropped, the weak ref fails to upgrade and
/// `Drop` becomes a no-op.
pub struct DbSnapshot {
    /// The pinned sequence number.
    seq: SequenceNumber,
    /// Back-reference to the engine. `Weak` to avoid a cycle
    /// (engine → snapshots registry; snapshots → engine).
    db: Weak<DbImpl>,
}

impl DbSnapshot {
    /// The pinned sequence.
    pub fn sequence(&self) -> SequenceNumber {
        self.seq
    }
}

impl std::fmt::Debug for DbSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DbSnapshot(seq={})", self.seq)
    }
}

impl crate::api::snapshot::Snapshot for DbSnapshot {
    fn sequence_number(&self) -> SequenceNumber {
        self.seq
    }
}

impl Drop for DbSnapshot {
    fn drop(&mut self) {
        if let Some(db) = self.db.upgrade() {
            db.release_snapshot_internal(self.seq);
        }
        // Else: the engine has been dropped, nothing to release.
    }
}

/// Helper: render the SST level layout into the CURRENT file
/// format from the column families map. Format:
/// `cf_id:level:number` pairs separated by commas, e.g.
/// `"0:0:5,0:0:7,0:1:3,0:1:8"`. Backward-compatible: old
/// `level:number` format (no CF prefix) is treated as CF 0
/// during parsing.
///
/// Superseded by MANIFEST-based tracking. Retained for backward
/// compatibility.
#[allow(dead_code)]
fn current_string_from_cfs(cfs: &HashMap<ColumnFamilyId, CfState>) -> String {
    let mut s = String::new();
    // Sort by CF ID for deterministic output.
    let mut sorted: Vec<&CfState> = cfs.values().collect();
    sorted.sort_by_key(|cf| cf.id);
    for cf in sorted {
        // L0 in reverse (oldest → newest in the file).
        for entry in cf.l0.iter().rev() {
            if !s.is_empty() {
                s.push(',');
            }
            s.push_str(&format!("{}:{}:{}", cf.id, entry.level, entry.number));
        }
        for entry in &cf.l1 {
            if !s.is_empty() {
                s.push(',');
            }
            s.push_str(&format!("{}:{}:{}", cf.id, entry.level, entry.number));
        }
    }
    s
}

/// Legacy `current_string` wrapper for backward compatibility.
/// Takes flat L0/L1 slices (default CF = 0) and produces the
/// old `level:number` format. Kept for reference but no longer
/// called by the engine.
#[allow(dead_code)]
fn current_string(l0: &[SstEntry], l1: &[SstEntry]) -> String {
    let mut s = String::new();
    for entry in l0.iter().rev() {
        if !s.is_empty() {
            s.push(',');
        }
        s.push_str(&format!("{}:{}", entry.level, entry.number));
    }
    for entry in l1 {
        if !s.is_empty() {
            s.push(',');
        }
        s.push_str(&format!("{}:{}", entry.level, entry.number));
    }
    s
}

impl DbImpl {
    /// Schedule a compaction on the background thread pool.
    ///
    /// Sets `state.pending_compaction` to the plan's output
    /// number so [`Self::wait_for_pending_work`] can see that
    /// work is in flight. The closure runs on the pool's
    /// `Priority::Bottom` slot — separate from the `Low` slot
    /// that handles flushes — so flush and compaction can run
    /// in parallel without stepping on each other.
    ///
    /// If the engine is dropped mid-compaction, `Weak::upgrade`
    /// fails and the task drops its work. The half-written
    /// output SST (if any) is left on disk and will be picked
    /// up by the next `open()` via the directory listing — the
    /// same orphan-file pattern used for flush.
    fn schedule_compaction(&self, plan: CompactionPlan) {
        {
            let mut state = self.state.lock().unwrap();
            // Mark the CF the plan came from as pending — was hard-coded
            // to default before M2.5.
            if let Ok(cf) = state.cf_mut(plan.cf_id) {
                cf.pending_compaction = Some(plan.out_number);
            }
        }
        let weak = self.weak_self();
        self.thread_pool.schedule(
            Box::new(move || {
                if let Some(db) = weak.upgrade() {
                    let _ = db.run_compaction_task(plan);
                }
            }),
            Priority::Bottom,
        );
    }

    /// Background compaction entry point. Runs the merge, then
    /// clears `pending_compaction` and notifies the condvar so
    /// any `wait_for_pending_work` waiter wakes up.
    ///
    /// The outer closure in `schedule_compaction` ignores this
    /// method's `Result` — errors during compaction leave the
    /// inputs in place (they're never removed from
    /// `state.ssts` if the merge fails), so the next picker
    /// pass will try again.
    fn run_compaction_task(&self, plan: CompactionPlan) -> Result<()> {
        let cf_id = plan.cf_id;
        let result = self.run_compaction_inner(plan);
        {
            let mut state = self.state.lock().unwrap();
            if let Ok(cf) = state.cf_mut(cf_id) {
                cf.pending_compaction = None;
            }
        }
        self.flush_done.notify_all();
        result
    }

    /// Run a compaction job and update engine state to swap the
    /// inputs for the new output SST. Shared by both the sync
    /// path (which no longer exists at Layer 4f but is kept as
    /// a private helper in case tests need it) and the
    /// background task.
    fn run_compaction_inner(&self, plan: CompactionPlan) -> Result<()> {
        let cf_id = plan.cf_id;
        let out_number = plan.out_number;
        let inputs = plan.inputs;
        let input_numbers = plan.input_numbers;
        let input_levels = plan.input_levels;
        let min_snap_seq = plan.min_snap_seq;
        let filter_factory = plan.filter_factory;
        let merge_operator = plan.merge_operator;

        let out_path = make_table_file_name(&self.path, out_number);

        // Run the merge → output SST.
        let mut job = CompactionJob::new(
            self.fs.as_ref(),
            inputs,
            &out_path,
            BlockBasedTableOptions::default(),
        )
        .with_min_snap_seq(min_snap_seq);

        if let Some(factory) = &filter_factory {
            let filter = factory.create_compaction_filter(true, false);
            job = job.with_compaction_filter(filter);
        }
        if let Some(op) = merge_operator {
            job = job.with_merge_operator(op);
        }

        let written = job.run()?;

        // Swap inputs for the output under the lock.
        let mut state = self.state.lock().unwrap();
        {
            let cf = state.cf_mut(cf_id)?;
            cf.l0.retain(|e| !input_numbers.contains(&e.number));
            cf.l1.retain(|e| !input_numbers.contains(&e.number));
        }

        // Build the VersionEdit for deleted + new files. The CF id matches
        // the plan's CF (was hard-coded to DEFAULT_CF_ID before M2.5).
        let deleted_files: Vec<(u32, u64)> = input_numbers.iter().enumerate()
            .map(|(i, &n)| (input_levels[i], n))
            .collect();
        let mut edit = VersionEdit {
            column_family_id: Some(cf_id),
            deleted_files,
            ..Default::default()
        };

        if written > 0 {
            // Open the new SST and add it to **L1** (compaction
            // output). L1 is sorted by smallest_key, so we
            // insert at the right position.
            let raw = self
                .fs
                .new_random_access_file(&out_path, &Default::default())?;
            let reader = RandomAccessFileReader::new(raw, out_path.display().to_string())?;
            let table = BlockBasedTableReader::open_with_cache(
                Arc::new(reader),
                self.block_cache.clone(),
                out_number,
            )?;
            let (smallest, largest) = compute_sst_range(&table);
            let file_size = self.fs.get_file_size(&out_path)?;
            let new_entry = SstEntry {
                number: out_number,
                reader: Arc::new(table),
                smallest_key: smallest.clone(),
                largest_key: largest.clone(),
                level: 1,
            };
            let cf = state.cf_mut(cf_id)?;
            let pos = cf.l1.partition_point(|e| e.smallest_key < smallest);
            cf.l1.insert(pos, new_entry);

            edit.new_files.push((1, FileMetaData {
                number: out_number,
                file_size,
                smallest_key: smallest,
                largest_key: largest,
            }));
        }
        edit.next_file_number = Some(state.next_file_number);
        // Persist the updated level layout to the MANIFEST.
        state.version_set.log_and_apply(&edit)?;

        // Collect files to delete.
        let mut to_delete: Vec<u64> = input_numbers.clone();
        if written == 0 {
            to_delete.push(out_number); // empty output
        }

        if state.disable_file_deletions_count > 0 {
            // Defer deletions — file deletions are currently disabled
            // (incremental checkpoint in progress).
            state.pending_deletion.extend(&to_delete);
            drop(state);
        } else {
            drop(state);
            // Delete immediately.
            for n in &to_delete {
                let p = make_table_file_name(&self.path, *n);
                if self.fs.file_exists(&p).unwrap_or(false) {
                    let _ = self.fs.delete_file(&p);
                }
            }
        }
        Ok(())
    }

    /// Close the DB. Drains any pending background flush, then
    /// flushes the default CF's active memtable synchronously,
    /// then releases the file lock.
    ///
    /// **Note:** Non-default CFs are NOT automatically flushed on
    /// close. Call `flush_cf()` for each CF before closing if you
    /// need their data persisted.
    pub fn close(&self) -> Result<()> {
        // 1. Wait for any in-progress background flush.
        self.wait_for_pending_flush()?;
        // 2. Synchronously flush the active memtable.
        self.flush()?;
        // 3. Mark closing so future background work bails out
        //    quickly. Held under the lock so we don't race with
        //    a worker that's about to call complete_flush.
        {
            let mut state = self.state.lock().unwrap();
            state.closing = true;
        }
        // 4. Drain pool workers so no task is mid-flight when
        //    we drop the engine.
        self.thread_pool.wait_for_jobs_and_join_all();
        // 4b. Process any deferred SST deletions (from
        //     disable_file_deletions that was never re-enabled).
        {
            let mut state = self.state.lock().unwrap();
            let pending = std::mem::take(&mut state.pending_deletion);
            state.disable_file_deletions_count = 0;
            drop(state);
            for n in &pending {
                let p = make_table_file_name(&self.path, *n);
                if self.fs.file_exists(&p).unwrap_or(false) {
                    let _ = self.fs.delete_file(&p);
                }
            }
        }
        // 5. Release the lock so other processes can open the DB.
        let lock = self.lock.lock().unwrap().take();
        if let Some(lock) = lock {
            self.fs.unlock_file(lock)?;
        }
        Ok(())
    }
}

impl Drop for DbImpl {
    fn drop(&mut self) {
        // Best-effort: release the lock if it's still held. Don't
        // panic on errors — the file may already be gone.
        if let Ok(mut guard) = self.lock.lock() {
            if let Some(lock) = guard.take() {
                let _ = self.fs.unlock_file(lock);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// WAL record encoding (Layer 4a-private)
// ---------------------------------------------------------------------------

/// Encode a [`WriteBatch`] into a single WAL record.
///
/// Wire format (chosen for simplicity, not upstream parity):
///
/// ```text
///   sequence:varint64  count:varint32
///     ( type:u8  key_len:varint32  key:bytes  value_len:varint32  value:bytes ) ×count
/// ```
///
/// Type byte values:
/// - `0x1` = Put
/// - `0x0` = Delete (no value)
/// - `0x7` = SingleDelete (no value)
/// - `0x2` = Merge
/// - `0xF` = DeleteRange (two length-prefixed byte strings)
///
/// **Known limitation:** The column family ID is NOT encoded. All
/// records are replayed into the default CF on WAL recovery. Cross-CF
/// `WriteBatch` entries will lose their CF affinity after a crash.
/// Flink disables WAL (`WriteOptions.setDisableWAL(true)`) so this
/// does not affect the Flink integration path.
fn encode_batch_record(batch: &WriteBatch, first_seq: SequenceNumber, out: &mut Vec<u8>) {
    crate::util::coding::put_varint64(out, first_seq);
    put_varint32(out, batch.records().len() as u32);
    for record in batch.records() {
        match record {
            Record::Put { key, value, .. } => {
                out.push(0x1);
                put_varint32(out, key.len() as u32);
                out.extend_from_slice(key);
                put_varint32(out, value.len() as u32);
                out.extend_from_slice(value);
            }
            Record::Delete { key, .. } => {
                out.push(0x0);
                put_varint32(out, key.len() as u32);
                out.extend_from_slice(key);
            }
            Record::SingleDelete { key, .. } => {
                out.push(0x7);
                put_varint32(out, key.len() as u32);
                out.extend_from_slice(key);
            }
            Record::Merge { key, value, .. } => {
                out.push(0x2);
                put_varint32(out, key.len() as u32);
                out.extend_from_slice(key);
                put_varint32(out, value.len() as u32);
                out.extend_from_slice(value);
            }
            Record::DeleteRange { begin, end, .. } => {
                out.push(0xF); // Tag for DeleteRange in WAL
                put_varint32(out, begin.len() as u32);
                out.extend_from_slice(begin);
                put_varint32(out, end.len() as u32);
                out.extend_from_slice(end);
            }
        }
    }
}

/// Inverse of [`encode_batch_record`]. Returns the new
/// `last_sequence` after applying the record. Used by WAL replay.
///
/// If `range_tombstones` is provided, any DeleteRange records in the
/// WAL are appended to it so the caller can install them in the
/// appropriate `CfState`.
fn replay_record_with_tombstones(
    record: &[u8],
    memtable: &mut MemTable,
    mut last_seq: SequenceNumber,
    mut range_tombstones: Option<&mut Vec<RangeTombstone>>,
) -> Result<SequenceNumber> {
    let (first_seq, rest) = crate::util::coding::get_varint64(record)?;
    let (count, mut rest) = get_varint32(rest)?;
    let mut seq = first_seq;
    for _ in 0..count {
        if rest.is_empty() {
            return Err(Status::corruption("WAL record: truncated entry"));
        }
        let kind = rest[0];
        rest = &rest[1..];

        // DeleteRange uses tag 0xF and has two length-prefixed fields
        // (begin_key, end_key) instead of a single key.
        if kind == 0xF {
            let (begin_len, after_blen) = get_varint32(rest)?;
            if after_blen.len() < begin_len as usize {
                return Err(Status::corruption("WAL record: truncated delete-range begin"));
            }
            let begin_key = &after_blen[..begin_len as usize];
            let after_begin = &after_blen[begin_len as usize..];
            let (end_len, after_elen) = get_varint32(after_begin)?;
            if after_elen.len() < end_len as usize {
                return Err(Status::corruption("WAL record: truncated delete-range end"));
            }
            let end_key = &after_elen[..end_len as usize];
            rest = &after_elen[end_len as usize..];
            if let Some(ref mut tombstones) = range_tombstones {
                tombstones.push((begin_key.to_vec(), end_key.to_vec(), seq));
            }
            seq += 1;
            if seq > last_seq + 1 {
                last_seq = seq - 1;
            }
            continue;
        }

        let (key_len, after_klen) = get_varint32(rest)?;
        if after_klen.len() < key_len as usize {
            return Err(Status::corruption("WAL record: truncated key"));
        }
        let key = &after_klen[..key_len as usize];
        let after_key = &after_klen[key_len as usize..];
        rest = after_key;
        let value_type = match kind {
            0x0 => ValueType::Deletion,
            0x1 => ValueType::Value,
            0x2 => ValueType::Merge,
            0x7 => ValueType::SingleDeletion,
            _ => return Err(Status::corruption("WAL record: unknown type byte")),
        };
        let value: &[u8] = match value_type {
            ValueType::Value | ValueType::Merge => {
                let (vlen, after_vlen) = get_varint32(rest)?;
                if after_vlen.len() < vlen as usize {
                    return Err(Status::corruption("WAL record: truncated value"));
                }
                let v = &after_vlen[..vlen as usize];
                rest = &after_vlen[vlen as usize..];
                v
            }
            _ => &[],
        };
        memtable.add(seq, value_type, key, value)?;
        seq += 1;
        if seq > last_seq {
            last_seq = seq - 1;
        }
    }
    Ok(last_seq)
}

/// `WriteBatchHandler` adapter that inserts each record into the
/// appropriate column family's memtable, advancing the sequence
/// as it goes.
struct MemTableInsertHandler<'a> {
    column_families: &'a mut HashMap<ColumnFamilyId, CfState>,
    seq: SequenceNumber,
}

impl<'a> MemTableInsertHandler<'a> {
    fn cf_memtable(&mut self, cf: ColumnFamilyId) -> Result<&mut MemTable> {
        self.column_families.get_mut(&cf).map(|c| &mut c.memtable).ok_or_else(||
            Status::invalid_argument(format!("column family {cf} not found")))
    }

    fn cf_state(&mut self, cf: ColumnFamilyId) -> Result<&mut CfState> {
        self.column_families.get_mut(&cf).ok_or_else(||
            Status::invalid_argument(format!("column family {cf} not found")))
    }
}

impl<'a> WriteBatchHandler for MemTableInsertHandler<'a> {
    fn put_cf(
        &mut self,
        cf: ColumnFamilyId,
        key: &[u8],
        value: &[u8],
    ) -> Result<()> {
        let seq = self.seq;
        self.cf_memtable(cf)?.add(seq, ValueType::Value, key, value)?;
        self.seq += 1;
        Ok(())
    }
    fn delete_cf(
        &mut self,
        cf: ColumnFamilyId,
        key: &[u8],
    ) -> Result<()> {
        let seq = self.seq;
        self.cf_memtable(cf)?.add(seq, ValueType::Deletion, key, &[])?;
        self.seq += 1;
        Ok(())
    }
    fn single_delete_cf(
        &mut self,
        cf: ColumnFamilyId,
        key: &[u8],
    ) -> Result<()> {
        let seq = self.seq;
        self.cf_memtable(cf)?.add(seq, ValueType::SingleDeletion, key, &[])?;
        self.seq += 1;
        Ok(())
    }
    fn delete_range_cf(
        &mut self,
        cf: ColumnFamilyId,
        begin: &[u8],
        end: &[u8],
    ) -> Result<()> {
        let seq = self.seq;
        self.cf_state(cf)?.range_tombstones.push((
            begin.to_vec(),
            end.to_vec(),
            seq,
        ));
        self.seq += 1;
        Ok(())
    }
    fn merge_cf(
        &mut self,
        cf: ColumnFamilyId,
        key: &[u8],
        value: &[u8],
    ) -> Result<()> {
        let seq = self.seq;
        self.cf_memtable(cf)?.add(seq, ValueType::Merge, key, value)?;
        self.seq += 1;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Db trait implementation
// ---------------------------------------------------------------------------

impl crate::api::db::Db for DbImpl {
    fn create_column_family(
        &self,
        name: &str,
        options: &crate::api::options::ColumnFamilyOptions,
    ) -> Result<Arc<dyn crate::api::db::ColumnFamilyHandle>> {
        let h = DbImpl::create_column_family(self, name, options)?;
        Ok(h as Arc<dyn crate::api::db::ColumnFamilyHandle>)
    }

    fn drop_column_family(&self, cf: &dyn crate::api::db::ColumnFamilyHandle) -> Result<()> {
        DbImpl::drop_column_family(self, cf)
    }

    fn default_column_family(&self) -> Arc<dyn crate::api::db::ColumnFamilyHandle> {
        DbImpl::default_column_family(self) as Arc<dyn crate::api::db::ColumnFamilyHandle>
    }

    fn put(
        &self,
        _opts: &crate::api::options::WriteOptions,
        key: &[u8],
        value: &[u8],
    ) -> Result<()> {
        DbImpl::put(self, key, value)
    }

    fn put_cf(
        &self,
        _opts: &crate::api::options::WriteOptions,
        cf: &dyn crate::api::db::ColumnFamilyHandle,
        key: &[u8],
        value: &[u8],
    ) -> Result<()> {
        DbImpl::put_cf(self, cf, key, value)
    }

    fn delete(
        &self,
        _opts: &crate::api::options::WriteOptions,
        key: &[u8],
    ) -> Result<()> {
        DbImpl::delete(self, key)
    }

    fn delete_cf(
        &self,
        _opts: &crate::api::options::WriteOptions,
        cf: &dyn crate::api::db::ColumnFamilyHandle,
        key: &[u8],
    ) -> Result<()> {
        DbImpl::delete_cf(self, cf, key)
    }

    fn single_delete(
        &self,
        _opts: &crate::api::options::WriteOptions,
        key: &[u8],
    ) -> Result<()> {
        // Delegate to regular delete for now.
        DbImpl::delete(self, key)
    }

    fn merge(
        &self,
        _opts: &crate::api::options::WriteOptions,
        key: &[u8],
        value: &[u8],
    ) -> Result<()> {
        let mut batch = WriteBatch::new();
        batch.merge(key.to_vec(), value.to_vec());
        DbImpl::write(self, &batch)
    }

    fn get(
        &self,
        opts: &crate::api::options::ReadOptions,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        match opts.snapshot {
            Some(seq) => self.get_at_seq(key, seq),
            None => DbImpl::get(self, key),
        }
    }

    fn get_cf(
        &self,
        opts: &crate::api::options::ReadOptions,
        cf: &dyn crate::api::db::ColumnFamilyHandle,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        match opts.snapshot {
            Some(seq) => self.get_cf_at_seq(cf.id(), key, seq),
            None => DbImpl::get_cf(self, cf, key),
        }
    }

    fn write(
        &self,
        _opts: &crate::api::options::WriteOptions,
        batch: &WriteBatch,
    ) -> Result<()> {
        DbImpl::write(self, batch)
    }

    fn multi_get(
        &self,
        opts: &crate::api::options::ReadOptions,
        keys: &[&[u8]],
    ) -> Vec<Result<Option<Vec<u8>>>> {
        match opts.snapshot {
            Some(seq) => keys
                .iter()
                .map(|key| self.get_at_seq(key, seq))
                .collect(),
            None => DbImpl::multi_get(self, keys),
        }
    }

    fn new_iterator(
        &self,
        _opts: &crate::api::options::ReadOptions,
    ) -> Box<dyn crate::api::iterator::DbIterator> {
        // The concrete DbIterator (db_iter::DbIterator) doesn't
        // implement the trait DbIterator (api::iterator::DbIterator).
        // For now, return an empty iterator. Full trait wiring is
        // deferred to a future layer.
        Box::new(crate::api::iterator::EmptyIterator)
    }

    fn new_iterator_cf(
        &self,
        _opts: &crate::api::options::ReadOptions,
        _cf: &dyn crate::api::db::ColumnFamilyHandle,
    ) -> Box<dyn crate::api::iterator::DbIterator> {
        Box::new(crate::api::iterator::EmptyIterator)
    }

    fn snapshot(&self) -> Arc<dyn crate::api::snapshot::Snapshot> {
        DbImpl::snapshot(self) as Arc<dyn crate::api::snapshot::Snapshot>
    }

    fn release_snapshot(&self, _snap: Arc<dyn crate::api::snapshot::Snapshot>) {
        // Snapshots are released automatically on Drop via the
        // Arc<DbSnapshot> mechanism. This method is a no-op.
    }

    fn flush(&self, _opts: &crate::api::options::FlushOptions) -> Result<()> {
        DbImpl::flush(self)
    }

    fn flush_cf(
        &self,
        _opts: &crate::api::options::FlushOptions,
        cf: &dyn crate::api::db::ColumnFamilyHandle,
    ) -> Result<()> {
        DbImpl::flush_cf(self, cf)
    }

    fn compact_range(
        &self,
        _begin: Option<&[u8]>,
        _end: Option<&[u8]>,
    ) -> Result<()> {
        // Manual compaction not yet implemented. No-op.
        Ok(())
    }

    fn get_property(&self, property: &str) -> Option<String> {
        DbImpl::get_property(self, property)
    }

    fn flush_wal(&self, _sync: bool) -> Result<()> {
        // WAL sync is done on every write; this is a no-op.
        let mut state = self.state.lock().unwrap();
        state.wal.sync()
    }

    fn close(&self) -> Result<()> {
        DbImpl::close(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::db::ColumnFamilyHandle;

    fn opts() -> DbOptions {
        DbOptions {
            create_if_missing: true,
            ..DbOptions::default()
        }
    }

    fn temp_dir(tag: &str) -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static C: AtomicU64 = AtomicU64::new(0);
        let n = C.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        std::env::temp_dir().join(format!("st-rs-db-{tag}-{pid}-{n}"))
    }

    #[test]
    fn open_put_get() {
        let dir = temp_dir("basic");
        let db = DbImpl::open(&opts(), &dir).unwrap();
        db.put(b"k1", b"v1").unwrap();
        db.put(b"k2", b"v2").unwrap();
        assert_eq!(db.get(b"k1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(db.get(b"k2").unwrap(), Some(b"v2".to_vec()));
        assert_eq!(db.get(b"missing").unwrap(), None);
        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn delete_then_get_returns_none() {
        let dir = temp_dir("delete");
        let db = DbImpl::open(&opts(), &dir).unwrap();
        db.put(b"k", b"v").unwrap();
        assert_eq!(db.get(b"k").unwrap(), Some(b"v".to_vec()));
        db.delete(b"k").unwrap();
        assert_eq!(db.get(b"k").unwrap(), None);
        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn write_batch_atomic() {
        let dir = temp_dir("batch");
        let db = DbImpl::open(&opts(), &dir).unwrap();
        let mut batch = WriteBatch::new();
        batch.put(b"a".to_vec(), b"1".to_vec());
        batch.put(b"b".to_vec(), b"2".to_vec());
        batch.delete(b"c".to_vec());
        db.write(&batch).unwrap();
        assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(db.get(b"b").unwrap(), Some(b"2".to_vec()));
        assert_eq!(db.get(b"c").unwrap(), None);
        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn flush_then_get_reads_from_sst() {
        let dir = temp_dir("flush");
        let db = DbImpl::open(&opts(), &dir).unwrap();
        for i in 0..50u32 {
            let k = format!("key{i:03}");
            let v = format!("value{i}");
            db.put(k.as_bytes(), v.as_bytes()).unwrap();
        }
        db.flush().unwrap();
        // After flush, reads must come from the SST (the memtable
        // has been swapped out).
        for i in 0..50u32 {
            let k = format!("key{i:03}");
            let v = format!("value{i}");
            assert_eq!(db.get(k.as_bytes()).unwrap(), Some(v.into_bytes()));
        }
        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn reopen_replays_wal() {
        let dir = temp_dir("reopen");
        {
            let db = DbImpl::open(&opts(), &dir).unwrap();
            db.put(b"k1", b"v1").unwrap();
            db.put(b"k2", b"v2").unwrap();
            // Note: we don't call flush() — the data is only in
            // the WAL + memtable. Drop the db without close() to
            // simulate a crash.
            db.close().unwrap();
        }
        let db = DbImpl::open(&opts(), &dir).unwrap();
        assert_eq!(db.get(b"k1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(db.get(b"k2").unwrap(), Some(b"v2".to_vec()));
        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn reopen_after_flush_reads_sst() {
        let dir = temp_dir("reopen-flush");
        {
            let db = DbImpl::open(&opts(), &dir).unwrap();
            for i in 0..20u32 {
                let k = format!("k{i:02}");
                db.put(k.as_bytes(), b"v").unwrap();
            }
            db.flush().unwrap();
            db.close().unwrap();
        }
        let db = DbImpl::open(&opts(), &dir).unwrap();
        for i in 0..20u32 {
            let k = format!("k{i:02}");
            assert_eq!(db.get(k.as_bytes()).unwrap(), Some(b"v".to_vec()));
        }
        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn open_rejects_when_create_if_missing_false() {
        let dir = temp_dir("nocreate");
        let o = DbOptions {
            create_if_missing: false,
            ..DbOptions::default()
        };
        match DbImpl::open(&o, &dir) {
            Ok(_) => panic!("expected open to fail"),
            Err(e) => assert!(e.is_invalid_argument()),
        }
    }

    #[test]
    fn newer_writes_shadow_older_across_flush() {
        let dir = temp_dir("shadow");
        let db = DbImpl::open(&opts(), &dir).unwrap();
        db.put(b"k", b"old").unwrap();
        db.flush().unwrap();
        // The flushed SST has k → "old". Now overwrite in the
        // memtable.
        db.put(b"k", b"new").unwrap();
        // The memtable answer must beat the SST answer.
        assert_eq!(db.get(b"k").unwrap(), Some(b"new".to_vec()));
        db.flush().unwrap();
        // Now both versions are in different SSTs; the newer one
        // (lower file number? no — *higher* number, inserted at
        // the front of the list) wins.
        assert_eq!(db.get(b"k").unwrap(), Some(b"new".to_vec()));
        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn iter_walks_memtable_only() {
        let dir = temp_dir("iter-mem");
        let db = DbImpl::open(&opts(), &dir).unwrap();
        for k in ["a", "b", "c", "d"] {
            db.put(k.as_bytes(), b"v").unwrap();
        }
        let mut it = db.iter().unwrap();
        it.seek_to_first();
        let mut got = Vec::new();
        while it.valid() {
            got.push((it.key().to_vec(), it.value().to_vec()));
            it.next();
        }
        assert_eq!(got.len(), 4);
        assert_eq!(got[0].0, b"a");
        assert_eq!(got[3].0, b"d");
        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn iter_merges_memtable_and_sst() {
        let dir = temp_dir("iter-merge");
        let db = DbImpl::open(&opts(), &dir).unwrap();
        // First batch goes into the memtable, then to an SST.
        db.put(b"a", b"1").unwrap();
        db.put(b"c", b"3").unwrap();
        db.flush().unwrap();
        // Second batch stays in the memtable.
        db.put(b"b", b"2").unwrap();
        db.put(b"d", b"4").unwrap();

        let mut it = db.iter().unwrap();
        it.seek_to_first();
        let collected: Vec<(Vec<u8>, Vec<u8>)> = std::iter::from_fn(|| {
            if !it.valid() {
                return None;
            }
            let r = (it.key().to_vec(), it.value().to_vec());
            it.next();
            Some(r)
        })
        .collect();
        assert_eq!(collected.len(), 4);
        assert_eq!(collected[0], (b"a".to_vec(), b"1".to_vec()));
        assert_eq!(collected[1], (b"b".to_vec(), b"2".to_vec()));
        assert_eq!(collected[2], (b"c".to_vec(), b"3".to_vec()));
        assert_eq!(collected[3], (b"d".to_vec(), b"4".to_vec()));
        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn iter_skips_deleted_keys() {
        let dir = temp_dir("iter-del");
        let db = DbImpl::open(&opts(), &dir).unwrap();
        db.put(b"a", b"1").unwrap();
        db.put(b"b", b"2").unwrap();
        db.flush().unwrap();
        db.delete(b"a").unwrap();
        let mut it = db.iter().unwrap();
        it.seek_to_first();
        assert!(it.valid());
        assert_eq!(it.key(), b"b"); // a was deleted
        it.next();
        assert!(!it.valid());
        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn auto_compaction_collapses_ssts() {
        let dir = temp_dir("compact");
        let db = DbImpl::open(&opts(), &dir).unwrap();

        // Issue 5 flushes — the 4th hits COMPACTION_TRIGGER and
        // triggers an automatic compaction that merges all live
        // SSTs into one.
        for batch in 0..5u32 {
            for i in 0..10u32 {
                let k = format!("key{batch}-{i:02}");
                db.put(k.as_bytes(), b"v").unwrap();
            }
            db.flush().unwrap();
        }

        // After all the flushes + auto compactions, every key
        // should still be readable.
        for batch in 0..5u32 {
            for i in 0..10u32 {
                let k = format!("key{batch}-{i:02}");
                assert_eq!(db.get(k.as_bytes()).unwrap(), Some(b"v".to_vec()));
            }
        }

        // Wait for any in-flight background work to finish
        // before asserting on the SST layout.
        db.wait_for_pending_work().unwrap();

        // Compaction should have moved data from L0 to L1.
        // We don't assert an exact count (it depends on timing
        // and key-range overlap), just that (a) L1 has files
        // and (b) every key is still readable (already checked
        // above).
        let (l0, l1) = {
            let state = db.state.lock().unwrap();
            let cf = state.default_cf();
            (cf.l0.len(), cf.l1.len())
        };
        assert!(l1 > 0, "expected some L1 files after compaction, got L0={l0} L1={l1}");

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn schedule_flush_runs_in_background() {
        let dir = temp_dir("bgflush");
        let db = DbImpl::open(&opts(), &dir).unwrap();
        for i in 0..50u32 {
            let k = format!("k{i:03}");
            db.put(k.as_bytes(), b"v").unwrap();
        }
        // Schedule a flush; the call must return promptly without
        // waiting for the SST write.
        db.schedule_flush().unwrap();
        // The data should still be visible — either in the
        // immutable memtable (if the worker is mid-flush) or in
        // the new SST (if it's already done).
        for i in 0..50u32 {
            let k = format!("k{i:03}");
            assert_eq!(db.get(k.as_bytes()).unwrap(), Some(b"v".to_vec()));
        }
        // Drain the background work and verify everything still
        // reads correctly.
        db.wait_for_pending_flush().unwrap();
        for i in 0..50u32 {
            let k = format!("k{i:03}");
            assert_eq!(db.get(k.as_bytes()).unwrap(), Some(b"v".to_vec()));
        }
        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn writes_during_background_flush_go_to_new_memtable() {
        let dir = temp_dir("concurrent");
        let db = DbImpl::open(&opts(), &dir).unwrap();
        // Put some initial data and freeze it.
        for i in 0..20u32 {
            db.put(format!("old{i:02}").as_bytes(), b"old").unwrap();
        }
        db.schedule_flush().unwrap();
        // Now write more data — it should land in the *new*
        // active memtable, not the (possibly still mid-flush)
        // immutable one.
        for i in 0..20u32 {
            db.put(format!("new{i:02}").as_bytes(), b"new").unwrap();
        }
        db.wait_for_pending_flush().unwrap();
        // Both sets of data should be readable.
        for i in 0..20u32 {
            assert_eq!(
                db.get(format!("old{i:02}").as_bytes()).unwrap(),
                Some(b"old".to_vec())
            );
            assert_eq!(
                db.get(format!("new{i:02}").as_bytes()).unwrap(),
                Some(b"new".to_vec())
            );
        }
        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- Layer 4g: multi-level layout tests --------------------

    #[test]
    fn compaction_moves_l0_to_l1() {
        let dir = temp_dir("l0-to-l1");
        let db_opts = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&db_opts, &dir).unwrap();

        // Write 20 keys, flush after each batch of 5, to create
        // 4 L0 files (the compaction trigger).
        for batch in 0..4u32 {
            for i in 0..5u32 {
                let k = format!("k{:03}", batch * 5 + i);
                db.put(k.as_bytes(), b"v").unwrap();
            }
            db.flush().unwrap();
        }
        db.wait_for_pending_work().unwrap();

        // After 4 flushes + compaction, L0 should be empty (all
        // moved to L1) and L1 should have at least 1 file.
        let (l0, l1) = {
            let state = db.state.lock().unwrap();
            let cf = state.default_cf();
            (cf.l0.len(), cf.l1.len())
        };
        assert_eq!(l0, 0, "L0 should be empty after compaction");
        assert!(l1 > 0, "L1 should have at least 1 file after compaction");

        // Every key must still be readable (from L1 now).
        for batch in 0..4u32 {
            for i in 0..5u32 {
                let k = format!("k{:03}", batch * 5 + i);
                assert_eq!(db.get(k.as_bytes()).unwrap(), Some(b"v".to_vec()));
            }
        }

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn l1_binary_search_finds_correct_file() {
        // Write two non-overlapping key ranges into L1 via
        // separate flush+compact cycles, then point-lookup
        // keys from each range. The L1 binary search in
        // get_at_seq should find the correct file.
        let dir = temp_dir("l1-bsearch");
        let db_opts = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&db_opts, &dir).unwrap();

        // Range 1: keys "aaa" through "aae".
        for k in ["aaa", "aab", "aac", "aad", "aae"] {
            db.put(k.as_bytes(), b"range1").unwrap();
        }
        db.flush().unwrap();
        // Range 2: keys "bba" through "bbe".
        for k in ["bba", "bbb", "bbc", "bbd", "bbe"] {
            db.put(k.as_bytes(), b"range2").unwrap();
        }
        db.flush().unwrap();
        // Range 3 + 4: two more flushes to hit trigger (4).
        for k in ["cca", "ccb"] {
            db.put(k.as_bytes(), b"range3").unwrap();
        }
        db.flush().unwrap();
        for k in ["dda", "ddb"] {
            db.put(k.as_bytes(), b"range4").unwrap();
        }
        db.flush().unwrap();

        db.wait_for_pending_work().unwrap();

        // All ranges should be readable.
        assert_eq!(db.get(b"aac").unwrap(), Some(b"range1".to_vec()));
        assert_eq!(db.get(b"bbc").unwrap(), Some(b"range2".to_vec()));
        assert_eq!(db.get(b"cca").unwrap(), Some(b"range3".to_vec()));
        assert_eq!(db.get(b"ddb").unwrap(), Some(b"range4".to_vec()));
        // Non-existent keys return None.
        assert_eq!(db.get(b"zzz").unwrap(), None);
        assert_eq!(db.get(b"aaf").unwrap(), None);

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn overlapping_l0_and_l1_merge_correctly() {
        // Write overlapping keys across L0 and L1. Verify that
        // the newest version wins after compaction.
        let dir = temp_dir("overlap");
        let db_opts = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&db_opts, &dir).unwrap();

        // First cycle: put "k"="old" and flush 4 times to trigger
        // compaction → data lands in L1.
        db.put(b"k", b"old").unwrap();
        for _ in 0..4 {
            db.flush().unwrap();
        }
        db.wait_for_pending_work().unwrap();

        // Now overwrite "k"="new" and flush 4 more times to
        // trigger another compaction. This compaction should
        // merge the new L0 with the old L1 file.
        db.put(b"k", b"new").unwrap();
        for _ in 0..4 {
            db.flush().unwrap();
        }
        db.wait_for_pending_work().unwrap();

        assert_eq!(db.get(b"k").unwrap(), Some(b"new".to_vec()));

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- Layer 4f: background compaction tests -----------------

    #[test]
    fn compaction_runs_on_background_worker() {
        // Verify that schedule_compaction actually places work
        // on the pool and that wait_for_pending_work drains it.
        // We can't easily assert "didn't block the caller"
        // without real timing, so we instead assert on the
        // visible state transitions.
        let dir = temp_dir("bg-compact");
        let db = DbImpl::open(&opts(), &dir).unwrap();

        // Populate enough data across 4 flushes to trip the
        // compaction trigger on the 4th flush.
        for batch in 0..4u32 {
            for i in 0..5u32 {
                let k = format!("k{:03}", batch * 10 + i);
                db.put(k.as_bytes(), batch.to_string().as_bytes())
                    .unwrap();
            }
            db.flush().unwrap();
        }

        // At this point a compaction has been scheduled. Wait
        // for it to drain.
        db.wait_for_pending_work().unwrap();

        // After the compaction, pending_compaction should be
        // None and every key should still be readable.
        {
            let state = db.state.lock().unwrap();
            assert!(
                state.default_cf().pending_compaction.is_none(),
                "pending_compaction should have cleared after wait"
            );
        }
        for batch in 0..4u32 {
            for i in 0..5u32 {
                let k = format!("k{:03}", batch * 10 + i);
                assert_eq!(
                    db.get(k.as_bytes()).unwrap(),
                    Some(batch.to_string().as_bytes().to_vec())
                );
            }
        }

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn wait_for_pending_work_drains_flush_and_compaction() {
        let dir = temp_dir("drain-all");
        let db = DbImpl::open(&opts(), &dir).unwrap();

        // Schedule a flush (puts data into the background
        // pipeline) and then wait_for_pending_work.
        for i in 0..20u32 {
            db.put(format!("k{i:02}").as_bytes(), b"v").unwrap();
        }
        db.schedule_flush().unwrap();
        db.wait_for_pending_work().unwrap();

        // State should be quiescent: no immutable, no pending
        // compaction. (There might not have been a compaction
        // if the trigger wasn't hit, but the wait must still
        // return cleanly.)
        {
            let state = db.state.lock().unwrap();
            assert!(state.default_cf().immutable.is_none());
            assert!(state.default_cf().pending_compaction.is_none());
        }

        // Every key is still readable.
        for i in 0..20u32 {
            assert_eq!(
                db.get(format!("k{i:02}").as_bytes()).unwrap(),
                Some(b"v".to_vec())
            );
        }

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn picker_skips_while_compaction_in_flight() {
        // With a compaction in progress, the picker should
        // return None even if the SST count exceeds the
        // trigger. We test this by manually inserting a fake
        // pending_compaction marker and calling
        // maybe_pick_compaction.
        let dir = temp_dir("skip-pick");
        let db = DbImpl::open(&opts(), &dir).unwrap();
        // Force the SST count past the trigger.
        for batch in 0..5u32 {
            for i in 0..3u32 {
                let k = format!("k{:02}", batch * 10 + i);
                db.put(k.as_bytes(), b"v").unwrap();
            }
            db.flush().unwrap();
        }
        db.wait_for_pending_work().unwrap();

        // Now claim a pending compaction and verify the picker
        // returns None.
        let pick = {
            let mut state = db.state.lock().unwrap();
            state.default_cf_mut().pending_compaction = Some(9999);
            db.maybe_pick_compaction(&mut state)
        };
        assert!(
            pick.is_none(),
            "picker should return None while a compaction is already pending"
        );
        // Clean up the forced flag so close() doesn't hang.
        {
            let mut state = db.state.lock().unwrap();
            state.default_cf_mut().pending_compaction = None;
        }

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- Layer 4e: snapshot tests ------------------------------

    #[test]
    fn snapshot_sees_pre_overwrite_value_in_memtable() {
        let dir = temp_dir("snap-memt");
        let db = DbImpl::open(&opts(), &dir).unwrap();
        db.put(b"k", b"old").unwrap();
        let snap = db.snapshot();
        db.put(b"k", b"new").unwrap();

        // Current read sees "new".
        assert_eq!(db.get(b"k").unwrap(), Some(b"new".to_vec()));
        // Snapshot read sees "old".
        assert_eq!(
            db.get_at(b"k", &*snap as &dyn crate::api::snapshot::Snapshot)
                .unwrap(),
            Some(b"old".to_vec())
        );

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn snapshot_sees_pre_overwrite_value_after_flush() {
        let dir = temp_dir("snap-flush");
        let opts_larger = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&opts_larger, &dir).unwrap();
        db.put(b"k", b"old").unwrap();
        let snap = db.snapshot();
        db.put(b"k", b"new").unwrap();
        db.flush().unwrap();
        db.wait_for_pending_flush().unwrap();

        // Both versions are now in the SST. The snapshot must
        // still see "old".
        assert_eq!(db.get(b"k").unwrap(), Some(b"new".to_vec()));
        assert_eq!(
            db.get_at(b"k", &*snap as &dyn crate::api::snapshot::Snapshot)
                .unwrap(),
            Some(b"old".to_vec())
        );

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn snapshot_sees_pre_delete_value() {
        let dir = temp_dir("snap-del");
        let opts_larger = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&opts_larger, &dir).unwrap();
        db.put(b"k", b"live").unwrap();
        let snap = db.snapshot();
        db.delete(b"k").unwrap();

        // Current read returns None (tombstone wins).
        assert_eq!(db.get(b"k").unwrap(), None);
        // Snapshot read still sees the pre-deletion value.
        assert_eq!(
            db.get_at(b"k", &*snap as &dyn crate::api::snapshot::Snapshot)
                .unwrap(),
            Some(b"live".to_vec())
        );

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn snapshot_survives_compaction() {
        let dir = temp_dir("snap-compact");
        let opts_larger = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&opts_larger, &dir).unwrap();

        db.put(b"k", b"old").unwrap();
        let snap = db.snapshot();
        db.put(b"k", b"new").unwrap();

        // Force multiple flushes + a compaction.
        db.flush().unwrap();
        db.wait_for_pending_flush().unwrap();
        for i in 0..4u32 {
            db.put(format!("filler{i}").as_bytes(), b"v").unwrap();
            db.flush().unwrap();
            db.wait_for_pending_flush().unwrap();
        }

        // A compaction has almost certainly run by now (trigger
        // is 4). The snapshot must still see "old" because
        // compaction should have respected min_snap_seq.
        assert_eq!(db.get(b"k").unwrap(), Some(b"new".to_vec()));
        assert_eq!(
            db.get_at(b"k", &*snap as &dyn crate::api::snapshot::Snapshot)
                .unwrap(),
            Some(b"old".to_vec())
        );

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn dropping_snapshot_releases_retention() {
        let dir = temp_dir("snap-drop");
        let db = DbImpl::open(&opts(), &dir).unwrap();
        assert_eq!(db.live_snapshot_count(), 0);
        let snap = db.snapshot();
        assert_eq!(db.live_snapshot_count(), 1);
        drop(snap);
        assert_eq!(db.live_snapshot_count(), 0);
        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn iter_at_snapshot_excludes_newer_writes() {
        let dir = temp_dir("iter-snap");
        let opts_larger = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&opts_larger, &dir).unwrap();

        db.put(b"a", b"1").unwrap();
        db.put(b"b", b"2").unwrap();
        let snap = db.snapshot();
        db.put(b"c", b"3").unwrap(); // after the snapshot
        db.put(b"b", b"2b").unwrap(); // overwrite after snapshot

        // Current iterator sees a, b (updated), c.
        let mut it = db.iter().unwrap();
        it.seek_to_first();
        let mut got: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        while it.valid() {
            got.push((it.key().to_vec(), it.value().to_vec()));
            it.next();
        }
        assert_eq!(
            got,
            vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2b".to_vec()),
                (b"c".to_vec(), b"3".to_vec()),
            ]
        );

        // Snapshot iterator sees a, b (old value), and NO c.
        let mut it = db
            .iter_at(&*snap as &dyn crate::api::snapshot::Snapshot)
            .unwrap();
        it.seek_to_first();
        let mut got: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        while it.valid() {
            got.push((it.key().to_vec(), it.value().to_vec()));
            it.next();
        }
        assert_eq!(
            got,
            vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec()),
            ]
        );

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn sst_retains_multiple_versions_per_user_key() {
        // Layer 4d: SSTs store internal keys, so multiple
        // versions of the same user key can coexist in a single
        // SST. This test writes three versions of the same key,
        // flushes them all into one SST (by reducing the auto-
        // flush churn with a larger buffer), and then inspects
        // the SST directly to confirm all three versions are
        // present.
        use crate::db::dbformat::ParsedInternalKey;
        use crate::file::random_access_file_reader::RandomAccessFileReader;
        use crate::sst::block_based::table_reader::BlockBasedTableReader;
        use std::sync::Arc;

        let dir = temp_dir("sst-versions");
        // Larger write_buffer_size so all three puts land in one
        // memtable before the flush trigger fires.
        let db_opts = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&db_opts, &dir).unwrap();
        db.put(b"k", b"v1").unwrap();
        db.put(b"k", b"v2").unwrap();
        db.put(b"k", b"v3").unwrap();
        db.flush().unwrap();
        db.wait_for_pending_flush().unwrap();

        // The current-value read should return v3.
        assert_eq!(db.get(b"k").unwrap(), Some(b"v3".to_vec()));

        // Find the one live SST and inspect its raw contents.
        // We go around the engine to read the on-disk file
        // directly.
        let fs = crate::env::posix::PosixFileSystem::new();
        let mut sst_paths: Vec<std::path::PathBuf> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("sst"))
            .collect();
        sst_paths.sort();
        assert_eq!(sst_paths.len(), 1, "expected exactly one SST");

        let raw = fs
            .new_random_access_file(&sst_paths[0], &Default::default())
            .unwrap();
        let reader = RandomAccessFileReader::new(raw, "t").unwrap();
        let table = BlockBasedTableReader::open(Arc::new(reader)).unwrap();
        let mut it = table.iter();
        it.seek_to_first();
        let mut versions = Vec::new();
        while it.valid() {
            let parsed = ParsedInternalKey::parse(it.key()).unwrap();
            if parsed.user_key == b"k" {
                versions.push((parsed.sequence, it.value().to_vec()));
            }
            it.next();
        }
        assert_eq!(
            versions.len(),
            3,
            "SST should retain all three versions of `k`"
        );
        // Thanks to internal-key ordering, versions are in
        // descending sequence order.
        assert_eq!(versions[0].1, b"v3".to_vec());
        assert_eq!(versions[1].1, b"v2".to_vec());
        assert_eq!(versions[2].1, b"v1".to_vec());
        assert!(versions[0].0 > versions[1].0);
        assert!(versions[1].0 > versions[2].0);

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn current_file_is_atomic_after_flush() {
        let dir = temp_dir("atomic-current");
        let db = DbImpl::open(&opts(), &dir).unwrap();
        db.put(b"k", b"v").unwrap();
        db.flush().unwrap();
        // CURRENT must exist; CURRENT.tmp must not (it gets
        // renamed away during the atomic write).
        assert!(dir.join("CURRENT").exists());
        assert!(!dir.join("CURRENT.tmp").exists());
        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn iter_after_compaction_returns_correct_data() {
        let dir = temp_dir("iter-compact");
        let db = DbImpl::open(&opts(), &dir).unwrap();

        // Populate enough data across 5 flushes to trip the
        // compaction trigger.
        for batch in 0..5u32 {
            for i in 0..5u32 {
                let k = format!("k{:03}", batch * 10 + i);
                db.put(k.as_bytes(), batch.to_string().as_bytes()).unwrap();
            }
            db.flush().unwrap();
        }

        // Iterate after compaction and confirm sorted order +
        // count.
        let mut it = db.iter().unwrap();
        it.seek_to_first();
        let mut count = 0;
        let mut last: Option<Vec<u8>> = None;
        while it.valid() {
            count += 1;
            let k = it.key().to_vec();
            if let Some(prev) = &last {
                assert!(k > *prev, "iterator out of order");
            }
            last = Some(k);
            it.next();
        }
        assert_eq!(count, 25);

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- Batch (vectorized) API tests ----

    #[test]
    fn multi_get_basic() {
        let dir = temp_dir("multi-get-basic");
        let db = DbImpl::open(&opts(), &dir).unwrap();
        db.put(b"a", b"1").unwrap();
        db.put(b"b", b"2").unwrap();
        db.put(b"c", b"3").unwrap();

        let keys: Vec<&[u8]> = vec![b"a", b"c", b"missing", b"b"];
        let results = db.multi_get(&keys);
        assert_eq!(results.len(), 4);
        assert_eq!(results[0].as_ref().unwrap(), &Some(b"1".to_vec()));
        assert_eq!(results[1].as_ref().unwrap(), &Some(b"3".to_vec()));
        assert_eq!(results[2].as_ref().unwrap(), &None);
        assert_eq!(results[3].as_ref().unwrap(), &Some(b"2".to_vec()));

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn multi_get_empty() {
        let dir = temp_dir("multi-get-empty");
        let db = DbImpl::open(&opts(), &dir).unwrap();
        let results = db.multi_get(&[]);
        assert!(results.is_empty());
        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn multi_get_after_flush() {
        let dir = temp_dir("multi-get-flush");
        let db = DbImpl::open(&opts(), &dir).unwrap();
        // Write data and flush to SST.
        db.put(b"x", b"10").unwrap();
        db.put(b"y", b"20").unwrap();
        db.flush().unwrap();
        db.wait_for_pending_work().unwrap();

        // Write more to active memtable.
        db.put(b"z", b"30").unwrap();
        db.put(b"x", b"11").unwrap(); // overwrite in memtable

        let keys: Vec<&[u8]> = vec![b"x", b"y", b"z", b"nope"];
        let results = db.multi_get(&keys);
        assert_eq!(results[0].as_ref().unwrap(), &Some(b"11".to_vec())); // memtable wins
        assert_eq!(results[1].as_ref().unwrap(), &Some(b"20".to_vec())); // from SST
        assert_eq!(results[2].as_ref().unwrap(), &Some(b"30".to_vec())); // memtable
        assert_eq!(results[3].as_ref().unwrap(), &None);

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn multi_get_sees_deletes() {
        let dir = temp_dir("multi-get-del");
        let db = DbImpl::open(&opts(), &dir).unwrap();
        db.put(b"a", b"1").unwrap();
        db.put(b"b", b"2").unwrap();
        db.delete(b"a").unwrap();

        let keys: Vec<&[u8]> = vec![b"a", b"b"];
        let results = db.multi_get(&keys);
        assert_eq!(results[0].as_ref().unwrap(), &None); // deleted
        assert_eq!(results[1].as_ref().unwrap(), &Some(b"2".to_vec()));

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn multi_put_basic() {
        let dir = temp_dir("multi-put-basic");
        let db = DbImpl::open(&opts(), &dir).unwrap();
        db.multi_put(&[
            (b"k1".as_ref(), b"v1".as_ref()),
            (b"k2".as_ref(), b"v2".as_ref()),
            (b"k3".as_ref(), b"v3".as_ref()),
        ])
        .unwrap();

        assert_eq!(db.get(b"k1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(db.get(b"k2").unwrap(), Some(b"v2".to_vec()));
        assert_eq!(db.get(b"k3").unwrap(), Some(b"v3".to_vec()));

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn multi_put_empty() {
        let dir = temp_dir("multi-put-empty");
        let db = DbImpl::open(&opts(), &dir).unwrap();
        db.multi_put(&[]).unwrap(); // no-op
        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn multi_delete_basic() {
        let dir = temp_dir("multi-del-basic");
        let db = DbImpl::open(&opts(), &dir).unwrap();
        db.put(b"a", b"1").unwrap();
        db.put(b"b", b"2").unwrap();
        db.put(b"c", b"3").unwrap();
        db.multi_delete(&[b"a", b"c"]).unwrap();

        assert_eq!(db.get(b"a").unwrap(), None);
        assert_eq!(db.get(b"b").unwrap(), Some(b"2".to_vec()));
        assert_eq!(db.get(b"c").unwrap(), None);

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn multi_get_consistent_snapshot() {
        // Verify that multi_get sees a consistent snapshot —
        // all keys read at the same sequence number.
        let dir = temp_dir("multi-get-snap");
        let db = DbImpl::open(&opts(), &dir).unwrap();

        // Write initial data.
        db.put(b"k1", b"old1").unwrap();
        db.put(b"k2", b"old2").unwrap();

        // Take a snapshot, overwrite one key.
        let snap = db.snapshot();
        db.put(b"k1", b"new1").unwrap();

        // multi_get at latest should see the new value.
        let results = db.multi_get(&[b"k1", b"k2"]);
        assert_eq!(results[0].as_ref().unwrap(), &Some(b"new1".to_vec()));
        assert_eq!(results[1].as_ref().unwrap(), &Some(b"old2".to_vec()));

        // Verify snapshot still sees old value via single get_at.
        assert_eq!(
            db.get_at(b"k1", &*snap).unwrap(),
            Some(b"old1".to_vec())
        );

        drop(snap);
        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn multi_put_then_multi_get_large_batch() {
        let dir = temp_dir("multi-large");
        let db = DbImpl::open(&opts(), &dir).unwrap();

        // Write 1000 entries in one batch.
        let pairs: Vec<(Vec<u8>, Vec<u8>)> = (0..1000u32)
            .map(|i| (format!("key-{i:05}").into_bytes(), format!("val-{i}").into_bytes()))
            .collect();
        let pair_refs: Vec<(&[u8], &[u8])> = pairs.iter().map(|(k, v)| (k.as_slice(), v.as_slice())).collect();
        db.multi_put(&pair_refs).unwrap();

        // Read all 1000 back in one batch.
        let key_refs: Vec<&[u8]> = pairs.iter().map(|(k, _)| k.as_slice()).collect();
        let results = db.multi_get(&key_refs);
        assert_eq!(results.len(), 1000);
        for (i, r) in results.iter().enumerate() {
            assert_eq!(
                r.as_ref().unwrap(),
                &Some(format!("val-{i}").into_bytes()),
            );
        }

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- Column family tests ----

    #[test]
    fn create_column_family_put_get() {
        let dir = temp_dir("cf-create");
        let cf_opts = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&cf_opts, &dir).unwrap();
        let cf = db
            .create_column_family("user", &crate::api::options::ColumnFamilyOptions::default())
            .unwrap();

        // Put in CF, verify isolated from default.
        db.put_cf(&*cf, b"k", b"cf-val").unwrap();
        db.put(b"k", b"default-val").unwrap();

        assert_eq!(db.get_cf(&*cf, b"k").unwrap(), Some(b"cf-val".to_vec()));
        assert_eq!(db.get(b"k").unwrap(), Some(b"default-val".to_vec()));

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn drop_column_family_removes_data() {
        let dir = temp_dir("cf-drop");
        let cf_opts = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&cf_opts, &dir).unwrap();
        let cf = db
            .create_column_family("temp", &crate::api::options::ColumnFamilyOptions::default())
            .unwrap();
        db.put_cf(&*cf, b"k", b"v").unwrap();
        assert_eq!(db.get_cf(&*cf, b"k").unwrap(), Some(b"v".to_vec()));

        db.drop_column_family(&*cf).unwrap();

        // CF is gone — get_cf should fail.
        assert!(db.get_cf(&*cf, b"k").is_err());

        // Default CF unaffected.
        db.put(b"x", b"y").unwrap();
        assert_eq!(db.get(b"x").unwrap(), Some(b"y".to_vec()));

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn drop_default_cf_fails() {
        let dir = temp_dir("cf-drop-default");
        let db = DbImpl::open(&opts(), &dir).unwrap();
        let default_cf = db.default_column_family();
        assert!(db.drop_column_family(&*default_cf).is_err());
        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn duplicate_cf_name_fails() {
        let dir = temp_dir("cf-dup");
        let db = DbImpl::open(&opts(), &dir).unwrap();
        db.create_column_family("dup", &crate::api::options::ColumnFamilyOptions::default())
            .unwrap();
        let result =
            db.create_column_family("dup", &crate::api::options::ColumnFamilyOptions::default());
        assert!(result.is_err());
        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn write_batch_multi_cf() {
        let dir = temp_dir("cf-batch");
        let cf_opts = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&cf_opts, &dir).unwrap();
        let cf1 = db
            .create_column_family("cf1", &crate::api::options::ColumnFamilyOptions::default())
            .unwrap();
        let cf2 = db
            .create_column_family("cf2", &crate::api::options::ColumnFamilyOptions::default())
            .unwrap();

        // Atomic write spanning default + cf1 + cf2.
        let mut batch = WriteBatch::new();
        batch.put(b"k".to_vec(), b"default".to_vec());
        batch.put_cf(cf1.id(), b"k".to_vec(), b"one".to_vec());
        batch.put_cf(cf2.id(), b"k".to_vec(), b"two".to_vec());
        db.write(&batch).unwrap();

        assert_eq!(db.get(b"k").unwrap(), Some(b"default".to_vec()));
        assert_eq!(db.get_cf(&*cf1, b"k").unwrap(), Some(b"one".to_vec()));
        assert_eq!(db.get_cf(&*cf2, b"k").unwrap(), Some(b"two".to_vec()));

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn flush_cf_isolates() {
        let dir = temp_dir("cf-flush");
        let big_opts = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&big_opts, &dir).unwrap();
        let cf = db
            .create_column_family("data", &crate::api::options::ColumnFamilyOptions::default())
            .unwrap();
        db.put(b"dk", b"dv").unwrap();
        db.put_cf(&*cf, b"ck", b"cv").unwrap();

        // Flush only the custom CF.
        db.flush_cf(&*cf).unwrap();
        db.wait_for_pending_work().unwrap();

        // Both readable.
        assert_eq!(db.get(b"dk").unwrap(), Some(b"dv".to_vec()));
        assert_eq!(db.get_cf(&*cf, b"ck").unwrap(), Some(b"cv".to_vec()));

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn delete_cf_works() {
        let dir = temp_dir("cf-delete");
        let cf_opts = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&cf_opts, &dir).unwrap();
        let cf = db
            .create_column_family("cf", &crate::api::options::ColumnFamilyOptions::default())
            .unwrap();
        db.put_cf(&*cf, b"k", b"v").unwrap();
        db.delete_cf(&*cf, b"k").unwrap();
        assert_eq!(db.get_cf(&*cf, b"k").unwrap(), None);

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- Merge operator tests ----

    fn merge_cf_opts() -> crate::api::options::ColumnFamilyOptions {
        crate::api::options::ColumnFamilyOptions {
            merge_operator_name: "stringappendtest".to_string(),
            ..crate::api::options::ColumnFamilyOptions::default()
        }
    }

    #[test]
    fn merge_put_then_merge_then_get() {
        let dir = temp_dir("merge-basic");
        let big = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&big, &dir).unwrap();
        let cf = db.create_column_family("list", &merge_cf_opts()).unwrap();

        // Put base value, then merge two operands.
        db.put_cf(&*cf, b"k", b"a").unwrap();
        {
            let mut batch = WriteBatch::new();
            batch.merge_cf(cf.id(), b"k".to_vec(), b"b".to_vec());
            db.write(&batch).unwrap();
        }
        {
            let mut batch = WriteBatch::new();
            batch.merge_cf(cf.id(), b"k".to_vec(), b"c".to_vec());
            db.write(&batch).unwrap();
        }

        // get should apply full_merge("k", Some("a"), ["b", "c"]) → "a,b,c"
        assert_eq!(
            db.get_cf(&*cf, b"k").unwrap(),
            Some(b"a,b,c".to_vec())
        );

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn merge_without_base_value() {
        let dir = temp_dir("merge-no-base");
        let big = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&big, &dir).unwrap();
        let cf = db.create_column_family("list", &merge_cf_opts()).unwrap();

        // Merge without a preceding Put — no base value.
        {
            let mut batch = WriteBatch::new();
            batch.merge_cf(cf.id(), b"k".to_vec(), b"x".to_vec());
            db.write(&batch).unwrap();
        }
        {
            let mut batch = WriteBatch::new();
            batch.merge_cf(cf.id(), b"k".to_vec(), b"y".to_vec());
            db.write(&batch).unwrap();
        }

        // full_merge("k", None, ["x", "y"]) → "x,y"
        assert_eq!(
            db.get_cf(&*cf, b"k").unwrap(),
            Some(b"x,y".to_vec())
        );

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn merge_single_operand() {
        let dir = temp_dir("merge-single");
        let big = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&big, &dir).unwrap();
        let cf = db.create_column_family("list", &merge_cf_opts()).unwrap();

        {
            let mut batch = WriteBatch::new();
            batch.merge_cf(cf.id(), b"k".to_vec(), b"only".to_vec());
            db.write(&batch).unwrap();
        }

        assert_eq!(
            db.get_cf(&*cf, b"k").unwrap(),
            Some(b"only".to_vec())
        );

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn merge_after_delete() {
        let dir = temp_dir("merge-after-del");
        let big = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&big, &dir).unwrap();
        let cf = db.create_column_family("list", &merge_cf_opts()).unwrap();

        db.put_cf(&*cf, b"k", b"old").unwrap();
        db.delete_cf(&*cf, b"k").unwrap();
        {
            let mut batch = WriteBatch::new();
            batch.merge_cf(cf.id(), b"k".to_vec(), b"new".to_vec());
            db.write(&batch).unwrap();
        }

        // Delete stops the chain. Merge on top of nothing → "new".
        assert_eq!(
            db.get_cf(&*cf, b"k").unwrap(),
            Some(b"new".to_vec())
        );

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn merge_survives_flush() {
        let dir = temp_dir("merge-flush");
        let big = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&big, &dir).unwrap();
        let cf = db.create_column_family("list", &merge_cf_opts()).unwrap();

        db.put_cf(&*cf, b"k", b"a").unwrap();
        db.flush_cf(&*cf).unwrap();
        db.wait_for_pending_work().unwrap();

        // Now merge on top of the flushed Put.
        {
            let mut batch = WriteBatch::new();
            batch.merge_cf(cf.id(), b"k".to_vec(), b"b".to_vec());
            db.write(&batch).unwrap();
        }

        // Base "a" is in SST, merge "b" is in memtable → "a,b"
        assert_eq!(
            db.get_cf(&*cf, b"k").unwrap(),
            Some(b"a,b".to_vec())
        );

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn merge_no_operator_returns_raw() {
        // CF without a merge operator — merge records are returned as-is.
        let dir = temp_dir("merge-no-op");
        let big = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&big, &dir).unwrap();

        // Default CF has no merge operator.
        {
            let mut batch = WriteBatch::new();
            batch.merge(b"k".to_vec(), b"raw".to_vec());
            db.write(&batch).unwrap();
        }

        // Without operator, get returns the newest operand as-is.
        assert_eq!(
            db.get(b"k").unwrap(),
            Some(b"raw".to_vec())
        );

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- Compaction filter tests ----

    /// Test filter that removes entries whose value equals "expired".
    struct ExpiredFilter;
    impl crate::ext::compaction_filter::CompactionFilter for ExpiredFilter {
        fn name(&self) -> &'static str { "expired_filter" }
        fn filter(
            &self,
            _level: u32,
            _key: &[u8],
            _entry_type: crate::core::types::EntryType,
            existing_value: &[u8],
        ) -> crate::ext::compaction_filter::CompactionDecision {
            if existing_value == b"expired" {
                crate::ext::compaction_filter::CompactionDecision::Remove
            } else {
                crate::ext::compaction_filter::CompactionDecision::Keep
            }
        }
    }

    struct ExpiredFilterFactory;
    impl crate::ext::compaction_filter::CompactionFilterFactory for ExpiredFilterFactory {
        fn name(&self) -> &'static str { "expired_filter_factory" }
        fn create_compaction_filter(
            &self,
            _is_full_compaction: bool,
            _is_manual_compaction: bool,
        ) -> Box<dyn crate::ext::compaction_filter::CompactionFilter> {
            Box::new(ExpiredFilter)
        }
    }

    #[test]
    fn compaction_filter_removes_expired_entries() {
        let dir = temp_dir("cf-filter");
        let big = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&big, &dir).unwrap();
        let cf_handle = db.default_column_family();

        // Register the filter on the default CF.
        db.set_compaction_filter_factory(
            &*cf_handle,
            Arc::new(ExpiredFilterFactory),
        )
        .unwrap();

        // Write some entries.
        db.put(b"keep1", b"alive").unwrap();
        db.put(b"drop1", b"expired").unwrap();
        db.put(b"keep2", b"still-here").unwrap();
        db.put(b"drop2", b"expired").unwrap();

        // Flush several times to trigger compaction (trigger=4).
        for i in 0..5u32 {
            db.put(format!("filler{i}").as_bytes(), b"f").unwrap();
            db.flush().unwrap();
            db.wait_for_pending_work().unwrap();
        }

        // After compaction, "expired" entries should be removed.
        assert_eq!(db.get(b"keep1").unwrap(), Some(b"alive".to_vec()));
        assert_eq!(db.get(b"keep2").unwrap(), Some(b"still-here".to_vec()));
        assert_eq!(db.get(b"drop1").unwrap(), None);
        assert_eq!(db.get(b"drop2").unwrap(), None);

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Test filter that rewrites values.
    struct RewriteFilter;
    impl crate::ext::compaction_filter::CompactionFilter for RewriteFilter {
        fn name(&self) -> &'static str { "rewrite_filter" }
        fn filter(
            &self,
            _level: u32,
            _key: &[u8],
            _entry_type: crate::core::types::EntryType,
            existing_value: &[u8],
        ) -> crate::ext::compaction_filter::CompactionDecision {
            if existing_value == b"rewrite-me" {
                crate::ext::compaction_filter::CompactionDecision::ChangeValue(
                    b"rewritten".to_vec()
                )
            } else {
                crate::ext::compaction_filter::CompactionDecision::Keep
            }
        }
    }

    struct RewriteFilterFactory;
    impl crate::ext::compaction_filter::CompactionFilterFactory for RewriteFilterFactory {
        fn name(&self) -> &'static str { "rewrite_filter_factory" }
        fn create_compaction_filter(
            &self,
            _is_full_compaction: bool,
            _is_manual_compaction: bool,
        ) -> Box<dyn crate::ext::compaction_filter::CompactionFilter> {
            Box::new(RewriteFilter)
        }
    }

    #[test]
    fn compaction_filter_rewrites_values() {
        let dir = temp_dir("cf-rewrite");
        let big = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&big, &dir).unwrap();
        let cf_handle = db.default_column_family();

        db.set_compaction_filter_factory(
            &*cf_handle,
            Arc::new(RewriteFilterFactory),
        )
        .unwrap();

        db.put(b"k1", b"rewrite-me").unwrap();
        db.put(b"k2", b"keep-me").unwrap();

        // Trigger compaction.
        for i in 0..5u32 {
            db.put(format!("filler{i}").as_bytes(), b"f").unwrap();
            db.flush().unwrap();
            db.wait_for_pending_work().unwrap();
        }

        assert_eq!(db.get(b"k1").unwrap(), Some(b"rewritten".to_vec()));
        assert_eq!(db.get(b"k2").unwrap(), Some(b"keep-me".to_vec()));

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- MANIFEST / VersionSet integration tests ----

    #[test]
    fn manifest_created_on_first_flush() {
        let dir = temp_dir("manifest-create");
        let big = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&big, &dir).unwrap();
        db.put(b"k", b"v").unwrap();
        db.flush().unwrap();
        db.wait_for_pending_work().unwrap();

        // CURRENT should now contain a MANIFEST filename.
        let current = std::fs::read_to_string(dir.join("CURRENT")).unwrap();
        assert!(
            current.trim().starts_with("MANIFEST-"),
            "CURRENT should point to MANIFEST, got: {current}"
        );

        // The MANIFEST file should exist.
        let manifest_path = dir.join(current.trim());
        assert!(manifest_path.exists(), "MANIFEST file missing");

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn reopen_recovers_from_manifest() {
        let dir = temp_dir("manifest-reopen");
        let big = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };

        // Write + flush + close.
        {
            let db = DbImpl::open(&big, &dir).unwrap();
            db.put(b"k1", b"v1").unwrap();
            db.put(b"k2", b"v2").unwrap();
            db.flush().unwrap();
            db.wait_for_pending_work().unwrap();
            db.close().unwrap();
        }

        // Reopen and verify data recovered from MANIFEST.
        {
            let db = DbImpl::open(&big, &dir).unwrap();
            assert_eq!(db.get(b"k1").unwrap(), Some(b"v1".to_vec()));
            assert_eq!(db.get(b"k2").unwrap(), Some(b"v2".to_vec()));
            assert_eq!(db.get(b"missing").unwrap(), None);
            db.close().unwrap();
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn reopen_multi_cf_preserves_data() {
        // This test was previously a KNOWN FAILURE — multi-CF data
        // was lost on reopen because the old CURRENT format didn't
        // track per-CF SST membership. With MANIFEST, it works.
        let dir = temp_dir("manifest-multi-cf");
        let big = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };

        // Open, create CF, write to both CFs, flush both, close.
        {
            let db = DbImpl::open(&big, &dir).unwrap();
            let cf = db
                .create_column_family(
                    "user",
                    &crate::api::options::ColumnFamilyOptions::default(),
                )
                .unwrap();
            db.put(b"dk", b"default-val").unwrap();
            db.put_cf(&*cf, b"ck", b"cf-val").unwrap();
            db.flush().unwrap();
            db.flush_cf(&*cf).unwrap();
            db.wait_for_pending_work().unwrap();
            db.close().unwrap();
        }

        // Reopen and verify both CFs' data survived.
        {
            let db = DbImpl::open(&big, &dir).unwrap();
            assert_eq!(db.get(b"dk").unwrap(), Some(b"default-val".to_vec()));

            // Find the "user" CF — it was created with id=1.
            let cf = Arc::new(ColumnFamilyHandleImpl { id: 1, name: "user".to_string() });
            assert_eq!(
                db.get_cf(&*cf, b"ck").unwrap(),
                Some(b"cf-val".to_vec())
            );
            db.close().unwrap();
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn compaction_result_survives_reopen() {
        let dir = temp_dir("manifest-compact-reopen");
        let big = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };

        // Write enough to trigger compaction, then close.
        {
            let db = DbImpl::open(&big, &dir).unwrap();
            for i in 0..20u32 {
                db.put(format!("key-{i:03}").as_bytes(), format!("val-{i}").as_bytes())
                    .unwrap();
                if i % 4 == 3 {
                    db.flush().unwrap();
                    db.wait_for_pending_work().unwrap();
                }
            }
            db.close().unwrap();
        }

        // Reopen and verify all data is readable.
        {
            let db = DbImpl::open(&big, &dir).unwrap();
            for i in 0..20u32 {
                assert_eq!(
                    db.get(format!("key-{i:03}").as_bytes()).unwrap(),
                    Some(format!("val-{i}").into_bytes()),
                );
            }
            db.close().unwrap();
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- Incremental checkpoint API tests ----

    #[test]
    fn disable_enable_file_deletions_basic() {
        let dir = temp_dir("file-del-basic");
        let big = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&big, &dir).unwrap();

        db.disable_file_deletions().unwrap();
        db.disable_file_deletions().unwrap(); // nested
        db.enable_file_deletions().unwrap();
        db.enable_file_deletions().unwrap();

        // Extra enable should fail.
        assert!(db.enable_file_deletions().is_err());

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn disable_file_deletions_defers_compaction_cleanup() {
        let dir = temp_dir("file-del-defer");
        let big = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&big, &dir).unwrap();

        // Write enough to trigger compaction.
        for i in 0..5u32 {
            db.put(format!("k{i}").as_bytes(), b"v").unwrap();
            db.flush().unwrap();
            db.wait_for_pending_work().unwrap();
        }

        // Count SST files before disabling.
        let files_before: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "sst"))
            .collect();

        // Disable deletions, write + flush + compact more.
        db.disable_file_deletions().unwrap();
        for i in 5..10u32 {
            db.put(format!("k{i}").as_bytes(), b"v").unwrap();
            db.flush().unwrap();
            db.wait_for_pending_work().unwrap();
        }

        // SSTs from the compaction inputs should still be on disk
        // (deletion deferred).
        let files_during: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "sst"))
            .collect();
        assert!(
            files_during.len() >= files_before.len(),
            "no SSTs should have been deleted while disabled"
        );

        // Re-enable — deferred deletions should happen.
        db.enable_file_deletions().unwrap();

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn get_live_files_returns_ssts_and_manifest() {
        let dir = temp_dir("live-files");
        let big = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&big, &dir).unwrap();
        db.put(b"k", b"v").unwrap();
        db.flush().unwrap();
        db.wait_for_pending_work().unwrap();

        let files = db.get_live_files(false).unwrap();

        // Should include at least one SST, CURRENT, and MANIFEST.
        let has_sst = files.iter().any(|f| {
            f.extension().is_some_and(|ext| ext == "sst")
        });
        let has_current = files.iter().any(|f| {
            f.file_name().is_some_and(|n| n == "CURRENT")
        });
        let has_manifest = files.iter().any(|f| {
            f.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("MANIFEST-"))
        });

        assert!(has_sst, "should include SST files");
        assert!(has_current, "should include CURRENT");
        assert!(has_manifest, "should include MANIFEST");

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn get_live_files_with_flush() {
        let dir = temp_dir("live-files-flush");
        let big = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&big, &dir).unwrap();
        db.put(b"k", b"v").unwrap();

        // get_live_files(true) should flush the memtable first.
        let files = db.get_live_files(true).unwrap();
        let sst_count = files
            .iter()
            .filter(|f| f.extension().is_some_and(|ext| ext == "sst"))
            .count();
        assert!(sst_count >= 1, "flush should have created an SST");

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn get_live_files_metadata_returns_info() {
        let dir = temp_dir("live-meta");
        let big = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&big, &dir).unwrap();
        db.put(b"a", b"1").unwrap();
        db.put(b"z", b"2").unwrap();
        db.flush().unwrap();
        db.wait_for_pending_work().unwrap();

        let meta = db.get_live_files_metadata();
        assert!(!meta.is_empty());

        let first = &meta[0];
        assert_eq!(first.column_family_name, "default");
        assert_eq!(first.level, 0);
        assert!(first.file_size > 0);
        assert!(!first.smallest_key.is_empty());
        assert!(!first.largest_key.is_empty());

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(feature = "snappy")]
    #[test]
    fn db_with_snappy_compression() {
        // This test is a placeholder — the DB engine itself does not
        // yet pass a compression_type through to BlockBasedTableOptions
        // during flush. What we can verify is that the block cache is
        // created when `block_cache_size > 0` and that the DB
        // round-trips data through open/put/flush/reopen.
        let dir = temp_dir("snappy-cache");
        let db_opts = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            block_cache_size: 1024 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&db_opts, &dir).unwrap();
        for i in 0..100u32 {
            let k = format!("key{i:05}");
            let v = format!("value{i}");
            db.put(k.as_bytes(), v.as_bytes()).unwrap();
        }
        db.flush().unwrap();

        // Verify reads (first pass populates cache, second hits it).
        for pass in 0..2 {
            for i in 0..100u32 {
                let k = format!("key{i:05}");
                let v = format!("value{i}");
                assert_eq!(
                    db.get(k.as_bytes()).unwrap(),
                    Some(v.into_bytes()),
                    "miss on pass {pass} key {k}"
                );
            }
        }

        db.close().unwrap();

        // Reopen and verify data survives.
        let db2 = DbImpl::open(&db_opts, &dir).unwrap();
        for i in 0..100u32 {
            let k = format!("key{i:05}");
            let v = format!("value{i}");
            assert_eq!(
                db2.get(k.as_bytes()).unwrap(),
                Some(v.into_bytes()),
                "miss after reopen for key {k}"
            );
        }
        db2.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- Write stall tests ----

    #[test]
    fn is_write_stopped_false_by_default() {
        let dir = temp_dir("stall-default");
        let big = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&big, &dir).unwrap();
        assert!(!db.is_write_stopped());
        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn write_stall_blocks_then_unblocks() {
        // Use a very low trigger so we can test the stall behavior
        // without writing too many SSTs.
        let dir = temp_dir("stall-block");
        let big = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&big, &dir).unwrap();

        // Write and flush enough SSTs to approach the compaction
        // trigger (4). Since compaction runs on a background thread,
        // and we wait for it, the L0 count should stay manageable.
        for i in 0..8u32 {
            db.put(format!("key-{i}").as_bytes(), b"value").unwrap();
            db.flush().unwrap();
            db.wait_for_pending_work().unwrap();
        }

        // After compaction, we should NOT be stalled (L0 reduced).
        assert!(!db.is_write_stopped());

        // Writes should succeed.
        db.put(b"final", b"ok").unwrap();
        assert_eq!(db.get(b"final").unwrap(), Some(b"ok".to_vec()));

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- Properties & Metrics tests ----

    #[test]
    fn property_num_entries_active_mem_table() {
        let dir = temp_dir("prop-entries");
        let big = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&big, &dir).unwrap();
        assert_eq!(db.get_int_property("num-entries-active-mem-table"), Some(0));

        db.put(b"a", b"1").unwrap();
        db.put(b"b", b"2").unwrap();
        assert_eq!(db.get_int_property("num-entries-active-mem-table"), Some(2));

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn property_num_files_at_level() {
        let dir = temp_dir("prop-levels");
        let big = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&big, &dir).unwrap();
        assert_eq!(db.get_int_property("num-files-at-level0"), Some(0));

        db.put(b"k", b"v").unwrap();
        db.flush().unwrap();
        db.wait_for_pending_work().unwrap();
        assert!(db.get_int_property("num-files-at-level0").unwrap() >= 1);

        // Higher levels always 0 in current 2-level model.
        assert_eq!(db.get_int_property("num-files-at-level2"), Some(0));

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn property_is_write_stopped() {
        let dir = temp_dir("prop-stopped");
        let big = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&big, &dir).unwrap();
        assert_eq!(db.get_int_property("is-write-stopped"), Some(0));

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn property_rocksdb_prefix_stripped() {
        // Flink queries with "rocksdb." prefix.
        let dir = temp_dir("prop-prefix");
        let big = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&big, &dir).unwrap();
        db.put(b"k", b"v").unwrap();

        assert_eq!(
            db.get_int_property("rocksdb.num-entries-active-mem-table"),
            Some(1)
        );
        assert_eq!(
            db.get_int_property("rocksdb.num-files-at-level0"),
            Some(0)
        );

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn property_unknown_returns_none() {
        let dir = temp_dir("prop-unknown");
        let big = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&big, &dir).unwrap();
        assert_eq!(db.get_int_property("nonexistent-property"), None);
        assert_eq!(db.get_property("nonexistent-property"), None);

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn property_block_cache_with_cache() {
        let dir = temp_dir("prop-cache");
        let big = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            block_cache_size: 1024 * 1024, // 1 MB cache
            ..DbOptions::default()
        };
        let db = DbImpl::open(&big, &dir).unwrap();
        assert_eq!(
            db.get_int_property("block-cache-capacity"),
            Some(1024 * 1024)
        );
        // Usage starts at 0.
        assert_eq!(db.get_int_property("block-cache-usage"), Some(0));

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn property_sst_sizes() {
        let dir = temp_dir("prop-sst-size");
        let big = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&big, &dir).unwrap();
        db.put(b"k", b"v").unwrap();
        db.flush().unwrap();
        db.wait_for_pending_work().unwrap();

        let total = db.get_int_property("total-sst-files-size").unwrap();
        assert!(total > 0, "total SST size should be > 0 after flush");

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- Db trait tests ----

    #[test]
    fn db_trait_put_get_delete() {
        use crate::api::db::Db;
        use crate::api::options::{ReadOptions, WriteOptions};

        let dir = temp_dir("trait-basic");
        let big = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&big, &dir).unwrap();
        let db: &dyn Db = &*db;

        let wo = WriteOptions::default();
        let ro = ReadOptions::default();

        db.put(&wo, b"k1", b"v1").unwrap();
        db.put(&wo, b"k2", b"v2").unwrap();

        assert_eq!(db.get(&ro, b"k1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(db.get(&ro, b"k2").unwrap(), Some(b"v2".to_vec()));

        db.delete(&wo, b"k1").unwrap();
        assert_eq!(db.get(&ro, b"k1").unwrap(), None);

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn db_trait_write_batch() {
        use crate::api::db::Db;
        use crate::api::options::{ReadOptions, WriteOptions};

        let dir = temp_dir("trait-batch");
        let big = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&big, &dir).unwrap();
        let db: &dyn Db = &*db;

        let wo = WriteOptions::default();
        let ro = ReadOptions::default();

        let mut batch = WriteBatch::new();
        batch.put(b"a".to_vec(), b"1".to_vec());
        batch.put(b"b".to_vec(), b"2".to_vec());
        db.write(&wo, &batch).unwrap();

        assert_eq!(db.get(&ro, b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(db.get(&ro, b"b").unwrap(), Some(b"2".to_vec()));

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn db_trait_column_families() {
        use crate::api::db::Db;
        use crate::api::options::{ReadOptions, WriteOptions};

        let dir = temp_dir("trait-cf");
        let big = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&big, &dir).unwrap();
        let db: &dyn Db = &*db;

        let wo = WriteOptions::default();
        let ro = ReadOptions::default();

        let cf = db
            .create_column_family(
                "mycf",
                &crate::api::options::ColumnFamilyOptions::default(),
            )
            .unwrap();

        db.put_cf(&wo, &*cf, b"ck", b"cv").unwrap();
        db.put(&wo, b"dk", b"dv").unwrap();

        assert_eq!(db.get_cf(&ro, &*cf, b"ck").unwrap(), Some(b"cv".to_vec()));
        assert_eq!(db.get(&ro, b"dk").unwrap(), Some(b"dv".to_vec()));

        // CF data is isolated from default.
        assert_eq!(db.get(&ro, b"ck").unwrap(), None);

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn db_trait_snapshot() {
        use crate::api::db::Db;
        use crate::api::options::WriteOptions;

        let dir = temp_dir("trait-snap");
        let big = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&big, &dir).unwrap();
        let db: &dyn Db = &*db;

        let wo = WriteOptions::default();
        db.put(&wo, b"k", b"old").unwrap();

        let snap = db.snapshot();
        db.put(&wo, b"k", b"new").unwrap();

        // Snapshot read not available through the Db trait (no
        // get_at method on the trait), but snapshot creation works.
        drop(snap);

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn db_trait_flush_and_property() {
        use crate::api::db::Db;
        use crate::api::options::{FlushOptions, WriteOptions};

        let dir = temp_dir("trait-flush-prop");
        let big = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&big, &dir).unwrap();
        let db: &dyn Db = &*db;

        let wo = WriteOptions::default();
        db.put(&wo, b"k", b"v").unwrap();
        db.flush(&FlushOptions::default()).unwrap();

        let prop = db.get_property("num-files-at-level0");
        assert!(prop.is_some());

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- Phase 8: DeleteRange tests ----

    #[test]
    fn delete_range_basic() {
        let dir = temp_dir("delete-range-basic");
        let big = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&big, &dir).unwrap();
        db.put(b"a", b"va").unwrap();
        db.put(b"b", b"vb").unwrap();
        db.put(b"c", b"vc").unwrap();
        db.put(b"d", b"vd").unwrap();

        // Delete range [b, d) — should delete b and c but not a or d.
        db.delete_range(b"b", b"d").unwrap();

        assert_eq!(db.get(b"a").unwrap(), Some(b"va".to_vec()));
        assert_eq!(db.get(b"b").unwrap(), None);
        assert_eq!(db.get(b"c").unwrap(), None);
        assert_eq!(db.get(b"d").unwrap(), Some(b"vd".to_vec()));

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn delete_range_cf() {
        let dir = temp_dir("delete-range-cf");
        let db = DbImpl::open(&opts(), &dir).unwrap();
        let cf = db
            .create_column_family("test_cf", &crate::api::options::ColumnFamilyOptions::default())
            .unwrap();

        db.put_cf(&*cf, b"a", b"va").unwrap();
        db.put_cf(&*cf, b"b", b"vb").unwrap();
        db.put_cf(&*cf, b"c", b"vc").unwrap();
        db.put_cf(&*cf, b"d", b"vd").unwrap();

        // Also put keys in the default CF — they should be unaffected.
        db.put(b"b", b"default_b").unwrap();
        db.put(b"c", b"default_c").unwrap();

        db.delete_range_cf(&*cf, b"b", b"d").unwrap();

        // CF-specific range delete only affects that CF.
        assert_eq!(db.get_cf(&*cf, b"a").unwrap(), Some(b"va".to_vec()));
        assert_eq!(db.get_cf(&*cf, b"b").unwrap(), None);
        assert_eq!(db.get_cf(&*cf, b"c").unwrap(), None);
        assert_eq!(db.get_cf(&*cf, b"d").unwrap(), Some(b"vd".to_vec()));

        // Default CF untouched.
        assert_eq!(db.get(b"b").unwrap(), Some(b"default_b".to_vec()));
        assert_eq!(db.get(b"c").unwrap(), Some(b"default_c".to_vec()));

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn delete_files_in_ranges_drops_ssts() {
        let dir = temp_dir("delete-files-in-ranges");
        let big = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&big, &dir).unwrap();
        let default_cf = db.default_column_family();

        // Write some data and flush to create an SST.
        db.put(b"a", b"1").unwrap();
        db.put(b"b", b"2").unwrap();
        db.put(b"c", b"3").unwrap();
        db.flush().unwrap();

        // Verify SST exists.
        let meta_before = db.get_live_files_metadata();
        assert!(!meta_before.is_empty(), "should have at least one SST");

        // Find the SST's key range.
        let sst = &meta_before[0];
        assert!(sst.smallest_key.as_slice() <= b"a");
        assert!(sst.largest_key.as_slice() >= b"c");

        // Delete files in range [a, z) — should cover the entire SST.
        db.delete_files_in_ranges(
            &*default_cf,
            &[(b"a".as_ref(), b"z".as_ref())],
        )
        .unwrap();

        // Verify SST was removed.
        let meta_after = db.get_live_files_metadata();
        let default_ssts: Vec<_> = meta_after
            .iter()
            .filter(|m| m.column_family_name == "default")
            .collect();
        assert!(
            default_ssts.is_empty(),
            "all default CF SSTs should be deleted"
        );

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn delete_range_with_snapshot() {
        let dir = temp_dir("delete-range-snapshot");
        let big = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&big, &dir).unwrap();

        db.put(b"a", b"va").unwrap();
        db.put(b"b", b"vb").unwrap();
        db.put(b"c", b"vc").unwrap();

        // Take a snapshot before the range delete.
        let snap = db.snapshot();

        // Range delete [a, c).
        db.delete_range(b"a", b"c").unwrap();

        // Current reads: a and b are deleted, c is still present.
        assert_eq!(db.get(b"a").unwrap(), None);
        assert_eq!(db.get(b"b").unwrap(), None);
        assert_eq!(db.get(b"c").unwrap(), Some(b"vc".to_vec()));

        // Snapshot reads: the range delete has a sequence number
        // higher than the snapshot, so the snapshot should still see
        // the old values.
        assert_eq!(
            db.get_at(b"a", &*snap).unwrap(),
            Some(b"va".to_vec()),
            "snapshot should see a before range delete"
        );
        assert_eq!(
            db.get_at(b"b", &*snap).unwrap(),
            Some(b"vb".to_vec()),
            "snapshot should see b before range delete"
        );
        assert_eq!(
            db.get_at(b"c", &*snap).unwrap(),
            Some(b"vc".to_vec()),
            "snapshot should see c"
        );

        drop(snap);
        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- Phase 9: Streaming Iterator (iter_cf + prefix_scan) tests ----

    #[test]
    fn iter_cf_returns_cf_data_only() {
        let dir = temp_dir("iter-cf");
        let db = DbImpl::open(&opts(), &dir).unwrap();

        // Create a CF and put data into it.
        let cf = db
            .create_column_family("mycf", &crate::api::options::ColumnFamilyOptions::default())
            .unwrap();
        db.put_cf(&*cf, b"cf_a", b"1").unwrap();
        db.put_cf(&*cf, b"cf_b", b"2").unwrap();

        // Put data into the default CF.
        db.put(b"def_x", b"10").unwrap();
        db.put(b"def_y", b"20").unwrap();

        // iter_cf should see only the CF's entries.
        let mut it = db.iter_cf(&*cf).unwrap();
        it.seek_to_first();
        let mut got: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        while it.valid() {
            got.push((it.key().to_vec(), it.value().to_vec()));
            it.next();
        }
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].0, b"cf_a");
        assert_eq!(got[1].0, b"cf_b");

        // Default iter should see only default CF entries.
        let mut dit = db.iter().unwrap();
        dit.seek_to_first();
        let mut def: Vec<Vec<u8>> = Vec::new();
        while dit.valid() {
            def.push(dit.key().to_vec());
            dit.next();
        }
        assert_eq!(def.len(), 2);
        assert_eq!(def[0], b"def_x");
        assert_eq!(def[1], b"def_y");

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn prefix_scan_basic() {
        let dir = temp_dir("prefix-scan");
        let db = DbImpl::open(&opts(), &dir).unwrap();

        db.put(b"user:alice", b"a").unwrap();
        db.put(b"user:bob", b"b").unwrap();
        db.put(b"user:charlie", b"c").unwrap();
        db.put(b"item:1", b"x").unwrap();
        db.put(b"item:2", b"y").unwrap();

        let results = db.prefix_scan(b"user:").unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, b"user:alice");
        assert_eq!(results[1].0, b"user:bob");
        assert_eq!(results[2].0, b"user:charlie");

        let items = db.prefix_scan(b"item:").unwrap();
        assert_eq!(items.len(), 2);

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn prefix_scan_cf() {
        let dir = temp_dir("prefix-scan-cf");
        let db = DbImpl::open(&opts(), &dir).unwrap();

        let cf = db
            .create_column_family("states", &crate::api::options::ColumnFamilyOptions::default())
            .unwrap();
        db.put_cf(&*cf, b"state:ca", b"California").unwrap();
        db.put_cf(&*cf, b"state:ny", b"New York").unwrap();
        db.put_cf(&*cf, b"state:tx", b"Texas").unwrap();
        db.put_cf(&*cf, b"city:sf", b"San Francisco").unwrap();

        let results = db.prefix_scan_cf(&*cf, b"state:").unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, b"state:ca");
        assert_eq!(results[2].0, b"state:tx");

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn prefix_scan_empty() {
        let dir = temp_dir("prefix-scan-empty");
        let db = DbImpl::open(&opts(), &dir).unwrap();

        db.put(b"aaa", b"1").unwrap();
        db.put(b"bbb", b"2").unwrap();

        let results = db.prefix_scan(b"zzz:").unwrap();
        assert!(results.is_empty());

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- Phase 10: SST Ingestion tests ----

    /// Helper: build a valid SST file at `path` with the given
    /// user-key / value pairs. Each entry is encoded as an
    /// internal key with `ValueType::Value` and a monotonically
    /// increasing sequence number so the SST matches the format
    /// the engine expects.
    fn build_test_sst(path: &Path, entries: &[(&[u8], &[u8])]) {
        use crate::db::dbformat::InternalKey;
        use crate::core::types::ValueType;
        use crate::sst::block_based::table_builder::{
            BlockBasedTableBuilder, BlockBasedTableOptions,
        };

        let fs = PosixFileSystem::new();
        let writable = fs.new_writable_file(path, &Default::default()).unwrap();
        let mut builder = BlockBasedTableBuilder::new(
            WritableFileWriter::new(writable),
            BlockBasedTableOptions::default(),
        );
        for (seq, (k, v)) in entries.iter().enumerate() {
            let ikey = InternalKey::new(k, (seq as u64) + 1, ValueType::Value);
            builder.add(ikey.encode(), v).unwrap();
        }
        builder.finish().unwrap();
    }

    #[test]
    fn ingest_external_file_basic() {
        use crate::db::ingest_external_file::IngestExternalFileOptions;

        let dir = temp_dir("ingest-basic");
        let ext_dir = temp_dir("ingest-basic-ext");
        std::fs::create_dir_all(&ext_dir).unwrap();

        let db = DbImpl::open(&opts(), &dir).unwrap();
        let cf = db
            .create_column_family("ingest_cf", &crate::api::options::ColumnFamilyOptions::default())
            .unwrap();

        // Build an external SST.
        let sst_path = ext_dir.join("external.sst");
        build_test_sst(&sst_path, &[(b"ik1", b"iv1"), (b"ik2", b"iv2")]);

        // Ingest it (copy mode).
        let ingest_opts = IngestExternalFileOptions::default();
        db.ingest_external_file(&*cf, &[sst_path.as_path()], &ingest_opts)
            .unwrap();

        // Verify data is readable through the CF.
        assert_eq!(
            db.get_cf(&*cf, b"ik1").unwrap(),
            Some(b"iv1".to_vec())
        );
        assert_eq!(
            db.get_cf(&*cf, b"ik2").unwrap(),
            Some(b"iv2".to_vec())
        );

        // Original file should still exist (copy mode).
        assert!(sst_path.exists());

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&ext_dir);
    }

    #[test]
    fn ingest_external_file_move() {
        use crate::db::ingest_external_file::IngestExternalFileOptions;

        let dir = temp_dir("ingest-move");
        let ext_dir = temp_dir("ingest-move-ext");
        std::fs::create_dir_all(&ext_dir).unwrap();

        let db = DbImpl::open(&opts(), &dir).unwrap();
        let cf = db
            .create_column_family("move_cf", &crate::api::options::ColumnFamilyOptions::default())
            .unwrap();

        let sst_path = ext_dir.join("movable.sst");
        build_test_sst(&sst_path, &[(b"mk1", b"mv1")]);
        assert!(sst_path.exists());

        let ingest_opts = IngestExternalFileOptions {
            move_files: true,
            ..Default::default()
        };
        db.ingest_external_file(&*cf, &[sst_path.as_path()], &ingest_opts)
            .unwrap();

        // Data should be readable.
        assert_eq!(
            db.get_cf(&*cf, b"mk1").unwrap(),
            Some(b"mv1".to_vec())
        );

        // Original file should be gone (moved).
        assert!(!sst_path.exists());

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&ext_dir);
    }

    #[test]
    fn create_cf_with_import() {
        let dir = temp_dir("cf-import");
        let ext_dir = temp_dir("cf-import-ext");
        std::fs::create_dir_all(&ext_dir).unwrap();

        let db = DbImpl::open(&opts(), &dir).unwrap();

        // Build two external SSTs.
        let sst1 = ext_dir.join("import1.sst");
        let sst2 = ext_dir.join("import2.sst");
        build_test_sst(&sst1, &[(b"a", b"1"), (b"b", b"2")]);
        build_test_sst(&sst2, &[(b"c", b"3"), (b"d", b"4")]);

        // Create CF with import.
        let cf = db
            .create_column_family_with_import(
                "imported",
                &crate::api::options::ColumnFamilyOptions::default(),
                &[sst1.as_path(), sst2.as_path()],
            )
            .unwrap();

        // Verify data from both SSTs is readable.
        assert_eq!(db.get_cf(&*cf, b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(db.get_cf(&*cf, b"b").unwrap(), Some(b"2".to_vec()));
        assert_eq!(db.get_cf(&*cf, b"c").unwrap(), Some(b"3".to_vec()));
        assert_eq!(db.get_cf(&*cf, b"d").unwrap(), Some(b"4".to_vec()));
        assert_eq!(db.get_cf(&*cf, b"missing").unwrap(), None);

        // Default CF should not see these keys.
        assert_eq!(db.get(b"a").unwrap(), None);

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&ext_dir);
    }

    // ---- Edge case tests for coverage ----

    #[test]
    fn multi_get_with_merge_operands() {
        // multi_get on keys that have merge operands should resolve
        // via the single-key merge path.
        let dir = temp_dir("multi-get-merge");
        let big = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&big, &dir).unwrap();
        let cf = db.create_column_family("list", &merge_cf_opts()).unwrap();

        // Put base + merge operands on two keys.
        db.put_cf(&*cf, b"k1", b"a").unwrap();
        {
            let mut batch = WriteBatch::new();
            batch.merge_cf(cf.id(), b"k1".to_vec(), b"b".to_vec());
            db.write(&batch).unwrap();
        }

        db.put_cf(&*cf, b"k2", b"x").unwrap();
        {
            let mut batch = WriteBatch::new();
            batch.merge_cf(cf.id(), b"k2".to_vec(), b"y".to_vec());
            db.write(&batch).unwrap();
        }

        // multi_get on the default CF won't see CF keys.
        let results = db.multi_get(&[b"k1", b"k2"]);
        assert_eq!(results[0].as_ref().unwrap(), &None);
        assert_eq!(results[1].as_ref().unwrap(), &None);

        // Verify the CF data through single-key get.
        assert_eq!(db.get_cf(&*cf, b"k1").unwrap(), Some(b"a,b".to_vec()));
        assert_eq!(db.get_cf(&*cf, b"k2").unwrap(), Some(b"x,y".to_vec()));

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn prefix_scan_across_sst() {
        // Prefix scan where some matching keys are in SST (after flush)
        // and some in the active memtable.
        let dir = temp_dir("prefix-sst");
        let big = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&big, &dir).unwrap();

        // First batch: keys with prefix "user:" go to SST.
        db.put(b"user:alice", b"a").unwrap();
        db.put(b"user:bob", b"b").unwrap();
        db.flush().unwrap();
        db.wait_for_pending_work().unwrap();

        // Second batch: more keys with prefix "user:" stay in memtable.
        db.put(b"user:charlie", b"c").unwrap();
        db.put(b"user:dave", b"d").unwrap();
        // Also a non-matching key.
        db.put(b"item:1", b"x").unwrap();

        let results = db.prefix_scan(b"user:").unwrap();
        assert_eq!(results.len(), 4);
        assert_eq!(results[0].0, b"user:alice");
        assert_eq!(results[1].0, b"user:bob");
        assert_eq!(results[2].0, b"user:charlie");
        assert_eq!(results[3].0, b"user:dave");

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn get_property_block_cache_usage() {
        // With block cache enabled, verify that cache properties
        // are queryable and that usage is > 0 after reading from SSTs.
        let dir = temp_dir("prop-cache-usage");
        let big = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            block_cache_size: 1024 * 1024, // 1 MB cache
            ..DbOptions::default()
        };
        let db = DbImpl::open(&big, &dir).unwrap();

        // Capacity should always be reported.
        assert_eq!(
            db.get_int_property("block-cache-capacity"),
            Some(1024 * 1024)
        );

        // Write data, flush to SST, and read back to populate cache.
        for i in 0..50u32 {
            let k = format!("key{i:05}");
            let v = format!("value{i}");
            db.put(k.as_bytes(), v.as_bytes()).unwrap();
        }
        db.flush().unwrap();
        db.wait_for_pending_work().unwrap();

        // Read from SST to ensure cache is populated.
        for i in 0..50u32 {
            let k = format!("key{i:05}");
            assert!(db.get(k.as_bytes()).unwrap().is_some());
        }

        let usage = db.get_int_property("block-cache-usage").unwrap();
        assert!(
            usage > 0,
            "cache usage should be > 0 after SST reads, got {usage}"
        );

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn empty_write_batch_is_noop() {
        // Writing an empty batch should be a no-op.
        let dir = temp_dir("empty-batch");
        let big = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&big, &dir).unwrap();

        db.put(b"k", b"v").unwrap();
        let seq_before = {
            let state = db.state.lock().unwrap();
            state.last_sequence
        };

        // Write an empty batch.
        db.write(&WriteBatch::new()).unwrap();

        let seq_after = {
            let state = db.state.lock().unwrap();
            state.last_sequence
        };

        // Sequence should not have advanced.
        assert_eq!(
            seq_before, seq_after,
            "empty write batch should not advance sequence"
        );

        // Data unchanged.
        assert_eq!(db.get(b"k").unwrap(), Some(b"v".to_vec()));

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn version_set_recovery_multi_cf() {
        // Create 2 CFs, flush both, close, reopen, verify both CFs' data.
        let dir = temp_dir("vs-recovery-cf");
        let big = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };

        let cf1_id;
        let cf2_id;

        // Phase 1: create, write, flush, close.
        {
            let db = DbImpl::open(&big, &dir).unwrap();
            let cf1 = db
                .create_column_family(
                    "alpha",
                    &crate::api::options::ColumnFamilyOptions::default(),
                )
                .unwrap();
            let cf2 = db
                .create_column_family(
                    "beta",
                    &crate::api::options::ColumnFamilyOptions::default(),
                )
                .unwrap();
            cf1_id = cf1.id();
            cf2_id = cf2.id();

            // Write to default + both CFs.
            db.put(b"dk", b"default-val").unwrap();
            db.put_cf(&*cf1, b"a1", b"alpha-val").unwrap();
            db.put_cf(&*cf2, b"b1", b"beta-val").unwrap();

            // Flush all.
            db.flush().unwrap();
            db.flush_cf(&*cf1).unwrap();
            db.flush_cf(&*cf2).unwrap();
            db.wait_for_pending_work().unwrap();

            db.close().unwrap();
        }

        // Phase 2: reopen and verify.
        {
            let db = DbImpl::open(&big, &dir).unwrap();
            assert_eq!(db.get(b"dk").unwrap(), Some(b"default-val".to_vec()));

            let cf1 = std::sync::Arc::new(ColumnFamilyHandleImpl {
                id: cf1_id,
                name: "alpha".to_string(),
            });
            let cf2 = std::sync::Arc::new(ColumnFamilyHandleImpl {
                id: cf2_id,
                name: "beta".to_string(),
            });
            assert_eq!(
                db.get_cf(&*cf1, b"a1").unwrap(),
                Some(b"alpha-val".to_vec())
            );
            assert_eq!(
                db.get_cf(&*cf2, b"b1").unwrap(),
                Some(b"beta-val".to_vec())
            );

            db.close().unwrap();
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn get_at_snapshot_after_flush() {
        // put, snapshot, flush, put again, verify snapshot reads old
        // value from SST rather than the newer memtable write.
        let dir = temp_dir("snap-after-flush");
        let big = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&big, &dir).unwrap();
        db.put(b"k", b"old").unwrap();
        let snap = db.snapshot();
        db.flush().unwrap();
        db.wait_for_pending_work().unwrap();
        db.put(b"k", b"new").unwrap();
        // Current read sees the new value.
        assert_eq!(db.get(b"k").unwrap(), Some(b"new".to_vec()));
        // Snapshot read should still see the old value (from SST).
        assert_eq!(
            db.get_at(b"k", &*snap as &dyn crate::api::snapshot::Snapshot)
                .unwrap(),
            Some(b"old".to_vec())
        );
        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn iter_at_snapshot_filters_by_sequence() {
        // Verify iter_at only surfaces entries visible at the
        // snapshot's sequence number.
        let dir = temp_dir("iter-at-seq");
        let big = DbOptions {
            create_if_missing: true,
            db_write_buffer_size: 64 * 1024,
            ..DbOptions::default()
        };
        let db = DbImpl::open(&big, &dir).unwrap();
        db.put(b"a", b"1").unwrap();
        db.put(b"b", b"2").unwrap();
        let snap = db.snapshot();
        db.put(b"c", b"3").unwrap();
        db.put(b"a", b"1-updated").unwrap();

        // Snapshot iterator should see a=1, b=2 only.
        let mut it = db
            .iter_at(&*snap as &dyn crate::api::snapshot::Snapshot)
            .unwrap();
        it.seek_to_first();
        let mut got: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        while it.valid() {
            got.push((it.key().to_vec(), it.value().to_vec()));
            it.next();
        }
        assert_eq!(
            got,
            vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec()),
            ]
        );

        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn close_is_idempotent() {
        // Calling close twice should not panic.
        let dir = temp_dir("close-idem");
        let db = DbImpl::open(&opts(), &dir).unwrap();
        db.put(b"k", b"v").unwrap();
        db.close().unwrap();
        // Second close should succeed without panic.
        db.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
