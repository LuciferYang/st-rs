//! Port of `db/db_impl/db_impl.{h,cc}` — **minimum viable engine**.
//!
//! This is the Layer 4a `Db`. It composes everything from Layers
//! 0–3 into a working key-value store:
//!
//! ```text
//!   Put → write_lock acquire → WAL append → memtable insert → release
//!   Get → memtable check → walk SSTs in newest-first order
//!   Flush → freeze memtable → write SST → swap in fresh memtable
//!         → drop frozen memtable → fsync directory
//!   Open → mkdir/lock → load CURRENT manifest → replay WAL into memtable
//! ```
//!
//! # What's included
//!
//! - DB lifecycle: `open`, `close`, reopen-with-WAL-replay
//! - Single column family (the `default`)
//! - `put`, `delete`, `single_delete`, `merge` (merge is treated as
//!   value), `get`
//! - Atomic `write` of a [`WriteBatch`]
//! - Manual `flush` that turns the active memtable into an SST
//! - Multi-SST point lookup (newest first)
//! - Crash-safe WAL: every write goes through the WAL before the
//!   memtable, and reopens replay the WAL into a fresh memtable
//!
//! # What's deferred to Layer 4b
//!
//! - Background flush thread (today's `flush()` is synchronous)
//! - Compaction (SSTs accumulate forever; in real use a layer 4b
//!   compaction job will merge them)
//! - True `Db` trait method shape: `new_iterator`, `snapshot`,
//!   column families beyond default, multi-CF transactions
//! - Group commit / write thread (today's writes hold a single big
//!   mutex)
//! - VersionEdit + Manifest (today's "manifest" is a flat list of
//!   SST file numbers persisted in CURRENT as a single line)
//! - Range deletion handling
//! - Snapshots beyond the implicit "now" view
//! - Iterator API (we expose `iter` over the memtable only for tests)
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
use crate::env::env_trait::{Priority, ThreadPool};
use crate::env::file_system::{FileLock, FileSystem, IoOptions};
use crate::env::posix::PosixFileSystem;
use crate::env::thread_pool::StdThreadPool;
use crate::ext::comparator::{BytewiseComparator, Comparator};
use crate::file::filename::{
    make_current_file_name, make_lock_file_name, make_table_file_name, make_wal_file_name,
    parse_file_name,
};
use crate::file::random_access_file_reader::RandomAccessFileReader;
use crate::file::sequence_file_reader::SequentialFileReader;
use crate::file::writable_file_writer::WritableFileWriter;
use crate::sst::block_based::table_builder::{
    BlockBasedTableBuilder, BlockBasedTableOptions,
};
use crate::sst::block_based::table_reader::BlockBasedTableReader;
use crate::core::types::FileType;
use crate::util::coding::{get_varint32, put_varint32};
use crate::api::write_batch::{Record, WriteBatch, WriteBatchHandler};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};

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

/// Maximum number of levels. Layer 4g uses exactly 2 (L0 + L1);
/// higher levels are reserved for a future multi-level layer.
/// Maximum number of levels. Layer 4g uses exactly 2 (L0 + L1).
#[allow(dead_code)]
const NUM_LEVELS: usize = 2;

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
    /// Active (mutable) memtable.
    memtable: MemTable,
    /// Memtable that has been frozen and handed to a flush task,
    /// but not yet turned into an SST. `None` when no flush is
    /// in progress.
    ///
    /// Held by `Arc` so the flush task can read it concurrently
    /// with the engine's read path. Reads consult this *after*
    /// the active memtable but *before* the SSTs.
    immutable: Option<Arc<MemTable>>,
    /// Level 0 SST files, **newest-first** (overlapping ranges
    /// allowed). Flushed memtables land here.
    l0: Vec<SstEntry>,
    /// Level 1 SST files, sorted by `smallest_key` (non-
    /// overlapping ranges guaranteed by the compaction output).
    /// L0→L1 compaction moves data here.
    l1: Vec<SstEntry>,
    /// Current WAL writer.
    wal: LogWriter,
    /// Number of the WAL file currently being written.
    wal_number: u64,
    /// Number of the WAL whose records ended up in the immutable
    /// memtable. Used to delete the obsolete WAL after the SST
    /// write completes. `None` when no flush is pending.
    immutable_wal_number: Option<u64>,
    /// File number reserved for the in-progress flush's output
    /// SST. `None` when no flush is pending.
    pending_flush_sst_number: Option<u64>,
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
    /// File number of the compaction output currently being
    /// produced by a background worker. `None` when no compaction
    /// is in flight. Layer 4f only allows one compaction at a
    /// time — the picker skips scheduling while this is `Some`.
    pending_compaction: Option<u64>,
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

        // 3. Discover existing SSTs and WAL files by listing dir.
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
                _ => {} // ignore unknown files
            }
        }
        existing_sst_numbers.sort();
        existing_wal_numbers.sort();

        // 4. Open existing SSTs into the read path. On reopen we
        //    don't read the CURRENT file for level assignments
        //    (Layer 4g doesn't yet parse that format); instead
        //    all discovered SSTs are placed in L0 and will be
        //    moved to L1 by the next compaction.
        let mut l0: Vec<SstEntry> = Vec::with_capacity(existing_sst_numbers.len());
        let l1: Vec<SstEntry> = Vec::new();
        for &num in &existing_sst_numbers {
            let p = make_table_file_name(path, num);
            let raw = fs.new_random_access_file(&p, &Default::default())?;
            let reader = RandomAccessFileReader::new(raw, p.display().to_string())?;
            let table = BlockBasedTableReader::open(Arc::new(reader))?;
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
        l0.sort_by(|a, b| b.number.cmp(&a.number));

        // 5a. Initialise `last_sequence` from the existing SSTs.
        let mut last_sequence = 0u64;
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

        // 5b. Replay WAL files into a fresh memtable.
        let mut memtable = MemTable::new(Arc::clone(&user_comparator));
        for &wal_num in &existing_wal_numbers {
            let wal_path = make_wal_file_name(path, wal_num);
            let raw = fs.new_sequential_file(&wal_path, &Default::default())?;
            let seq_reader = SequentialFileReader::new(raw, wal_path.display().to_string());
            let mut log_reader = LogReader::new(seq_reader);
            let mut record_buf = Vec::new();
            while log_reader.read_record(&mut record_buf)? {
                last_sequence = replay_record(&record_buf, &mut memtable, last_sequence)?;
            }
            // Delete the replayed WAL file — we're about to open a
            // new one with a fresh number, so old WALs are obsolete.
            fs.delete_file(&wal_path)?;
        }

        // 6. Open a new WAL.
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

        let arc = Arc::new(Self {
            path: path.to_path_buf(),
            fs,
            state: Mutex::new(DbState {
                memtable,
                immutable: None,
                l0,
                l1,
                wal: wal_writer,
                wal_number,
                immutable_wal_number: None,
                pending_flush_sst_number: None,
                next_file_number,
                last_sequence,
                closing: false,
                snapshots: std::collections::BTreeMap::new(),
                pending_compaction: None,
            }),
            flush_done: Condvar::new(),
            lock: Mutex::new(Some(lock)),
            user_comparator,
            write_buffer_size: opts.db_write_buffer_size.max(1),
            thread_pool,
            weak_self: OnceLock::new(),
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
    pub fn write(&self, batch: &WriteBatch) -> Result<()> {
        let mut state = self.state.lock().unwrap();

        // Reserve a sequence range for this batch.
        let first_seq = state.last_sequence + 1;
        let mut record_buf = Vec::new();
        encode_batch_record(batch, first_seq, &mut record_buf);

        // 1. WAL append + sync.
        state.wal.add_record(&record_buf)?;
        state.wal.sync()?;

        // 2. Memtable insert.
        let mut handler = MemTableInsertHandler {
            memtable: &mut state.memtable,
            seq: first_seq,
        };
        batch.iterate(&mut handler)?;

        // 3. Bookkeeping.
        state.last_sequence = handler.seq - 1;

        // 4. Maybe trigger flush.
        let needs_flush = state.memtable.approximate_memory_usage() >= self.write_buffer_size;
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
    /// ```ignore
    /// let snap = db.snapshot();
    /// db.put(b"k", b"new")?;            // after snapshot
    /// assert_eq!(db.get(b"k")?,
    ///            Some(b"new".to_vec()));        // current read
    /// assert_eq!(db.get_at(b"k", &*snap)?,
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
        let (lookup, l0_ssts, l1_ssts) = {
            let state = self.state.lock().unwrap();
            let lookup = LookupKey::new(key, read_seq);
            // 1. Try the active memtable first under the lock.
            match state.memtable.get(&lookup) {
                MemTableGetResult::Found(v) => return Ok(Some(v)),
                MemTableGetResult::Deleted => return Ok(None),
                MemTableGetResult::NotFound => {}
            }
            // 2. Try the immutable memtable (a flush in progress).
            if let Some(imm) = state.immutable.as_ref() {
                match imm.get(&lookup) {
                    MemTableGetResult::Found(v) => return Ok(Some(v)),
                    MemTableGetResult::Deleted => return Ok(None),
                    MemTableGetResult::NotFound => {}
                }
            }
            // 3. Snapshot the SST lists so we can drop the lock.
            //    L0 is newest-first; L1 is sorted by key range.
            let l0_ssts: Vec<Arc<BlockBasedTableReader>> =
                state.l0.iter().map(|e| Arc::clone(&e.reader)).collect();
            let l1_ssts: Vec<(Vec<u8>, Vec<u8>, Arc<BlockBasedTableReader>)> = state
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
            (lookup, l0_ssts, l1_ssts)
        };

        // 4a. Walk L0 SSTs newest-first (overlapping ranges).
        for table in &l0_ssts {
            if let Some(result) = Self::seek_in_sst(table, key, &lookup)? {
                return Ok(result);
            }
        }
        // 4b. Walk L1 via binary search (non-overlapping ranges).
        //     Find the file whose range might contain `key`.
        let idx = l1_ssts.partition_point(|(smallest, _, _): &(Vec<u8>, Vec<u8>, Arc<BlockBasedTableReader>)| smallest.as_slice() <= key);
        if idx > 0 {
            let (_, largest, table) = &l1_ssts[idx - 1];
            if key <= largest.as_slice() {
                if let Some(result) = Self::seek_in_sst(table, key, &lookup)? {
                    return Ok(result);
                }
            }
        }
        Ok(None)
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
        let ssts = Self::collect_all_ssts(&state);
        let it = crate::db::db_iter::DbIterator::from_snapshot_with_immutable_at(
            &state.memtable,
            state.immutable.as_deref(),
            &ssts,
            read_seq,
        )?;
        drop(state);
        Ok(it)
    }

    /// Collect all SST readers into a single vec for the eager
    /// `DbIterator` materialisation. L0 first (newest-first),
    /// then L1 (sorted by key range). The iterator path doesn't
    /// need binary-search — it walks every file — so a flat vec
    /// is fine.
    fn collect_all_ssts(state: &DbState) -> Vec<Arc<BlockBasedTableReader>> {
        let mut ssts: Vec<Arc<BlockBasedTableReader>> =
            Vec::with_capacity(state.l0.len() + state.l1.len());
        for e in &state.l0 {
            ssts.push(Arc::clone(&e.reader));
        }
        for e in &state.l1 {
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
        let ssts = Self::collect_all_ssts(&state);
        let it = crate::db::db_iter::DbIterator::from_snapshot_with_immutable(
            &state.memtable,
            state.immutable.as_deref(),
            &ssts,
        )?;
        drop(state);
        Ok(it)
    }

    /// Block until every in-progress background flush **and**
    /// every in-progress background compaction has completed.
    ///
    /// `close()` calls this before releasing the file lock.
    /// Tests use it to drain the pool to a known quiescent
    /// state before asserting on the SST list.
    pub fn wait_for_pending_work(&self) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        while state.immutable.is_some() || state.pending_compaction.is_some() {
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
        while state.immutable.is_some() {
            state = self.flush_done.wait(state).unwrap();
        }
        if state.memtable.num_entries() == 0 {
            return Ok(None);
        }

        // Reserve a file number for the SST.
        let sst_number = state.next_file_number;
        state.next_file_number += 1;
        state.pending_flush_sst_number = Some(sst_number);

        // Freeze the active memtable.
        let frozen = std::mem::replace(
            &mut state.memtable,
            MemTable::new(Arc::clone(&self.user_comparator)),
        );
        let immutable = Arc::new(frozen);
        state.immutable = Some(Arc::clone(&immutable));

        // Remember which WAL holds the records that landed in the
        // immutable memtable, so we can delete it after the flush.
        state.immutable_wal_number = Some(state.wal_number);

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
            sst_number,
            immutable,
            old_wal_number: state.immutable_wal_number.unwrap(),
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
    ) -> Result<Arc<BlockBasedTableReader>> {
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

        let raw = self
            .fs
            .new_random_access_file(&sst_path, &Default::default())?;
        let reader = RandomAccessFileReader::new(raw, sst_path.display().to_string())?;
        let table = BlockBasedTableReader::open(Arc::new(reader))?;
        Ok(Arc::new(table))
    }

    /// Third half of a flush: install the new SST + clear the
    /// immutable slot + delete the obsolete WAL. Holds the lock
    /// briefly. Notifies any waiters.
    fn complete_flush_locked(
        &self,
        setup: FlushSetup,
        write_result: Result<Arc<BlockBasedTableReader>>,
    ) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        // Always clear the immutable slot and pending fields,
        // even on error — otherwise we deadlock on the next
        // start_flush_locked.
        state.immutable = None;
        state.immutable_wal_number = None;
        state.pending_flush_sst_number = None;

        let result = match write_result {
            Ok(reader) => {
                let (smallest, largest) = compute_sst_range(&reader);
                state.l0.insert(
                    0,
                    SstEntry {
                        number: setup.sst_number,
                        reader,
                        smallest_key: smallest,
                        largest_key: largest,
                        level: 0,
                    },
                );
                // Persist the new SST list to CURRENT.
                let current = current_string(&state.l0, &state.l1);
                drop(state);
                let r = self.write_current_atomic(&current);
                // Reacquire to maybe schedule compaction.
                let mut state = self.state.lock().unwrap();
                if r.is_ok() {
                    // Best-effort: delete the obsolete WAL now
                    // that the SST + CURRENT are durable.
                    let old_wal_path =
                        make_wal_file_name(&self.path, setup.old_wal_number);
                    let _ = self.fs.delete_file(&old_wal_path);
                }
                let cleanup = self.maybe_pick_compaction(&mut state);
                drop(state);
                if let Some(plan) = cleanup {
                    // Background compaction — returns immediately
                    // after putting the plan on the pool. The
                    // flush worker that called us can then finish
                    // and pick up the next flush; the compaction
                    // worker runs in parallel.
                    self.schedule_compaction(plan);
                }
                r
            }
            Err(e) => {
                // SST write failed. The obsolete WAL is preserved
                // — on the next open(), its records will be
                // replayed into a fresh memtable.
                Err(e)
            }
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
        if state.pending_compaction.is_some() {
            return None;
        }
        if state.l0.len() < Self::COMPACTION_TRIGGER {
            return None;
        }
        // All L0 files participate.
        let mut inputs: Vec<Arc<BlockBasedTableReader>> = Vec::new();
        let mut input_numbers: Vec<u64> = Vec::new();
        let mut union_smallest: Vec<u8> = Vec::new();
        let mut union_largest: Vec<u8> = Vec::new();
        for entry in &state.l0 {
            inputs.push(Arc::clone(&entry.reader));
            input_numbers.push(entry.number);
            if union_smallest.is_empty() || entry.smallest_key < union_smallest {
                union_smallest.clone_from(&entry.smallest_key);
            }
            if union_largest.is_empty() || entry.largest_key > union_largest {
                union_largest.clone_from(&entry.largest_key);
            }
        }
        // Include overlapping L1 files.
        for entry in &state.l1 {
            if entry.smallest_key <= union_largest
                && entry.largest_key >= union_smallest
            {
                inputs.push(Arc::clone(&entry.reader));
                input_numbers.push(entry.number);
            }
        }
        let out_number = state.next_file_number;
        state.next_file_number += 1;
        let min_snap_seq = state.min_snap_seq();
        Some(CompactionPlan {
            out_number,
            inputs,
            input_numbers,
            min_snap_seq,
        })
    }

    /// Atomically write `contents` to `<path>/CURRENT` via a tmp
    /// file + rename. Crash-safe: a torn write produces an
    /// orphaned tmp file, never a half-written CURRENT.
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
    sst_number: u64,
    immutable: Arc<MemTable>,
    old_wal_number: u64,
}

/// Everything a background compaction worker needs. Captured
/// inside the closure scheduled on `Priority::Bottom` by
/// [`DbImpl::schedule_compaction`].
struct CompactionPlan {
    /// File number reserved for the compaction output.
    out_number: u64,
    /// Table readers for the input SSTs. The inputs stay in
    /// `state.ssts` until the compaction completes so ongoing
    /// reads can still find their data.
    inputs: Vec<Arc<BlockBasedTableReader>>,
    /// Numbers of the input SSTs — used to identify them for
    /// removal from `state.ssts` and deletion from disk when
    /// the merge completes.
    input_numbers: Vec<u64>,
    /// Oldest live snapshot sequence at the time the plan was
    /// created. Captured under the state lock so it's consistent
    /// with the chosen inputs. Snapshots taken *after* plan
    /// creation but *before* the compaction runs don't affect
    /// retention — that's a minor race-window that costs at
    /// most one extra compaction cycle of retention, never
    /// corruption.
    min_snap_seq: SequenceNumber,
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
/// format. Layer 4g format: `level:number` pairs separated by
/// commas, e.g. `"0:5,0:7,1:3,1:8"`. L0 entries are written
/// newest-first; L1 entries are written sorted by key range.
fn current_string(l0: &[SstEntry], l1: &[SstEntry]) -> String {
    let mut s = String::new();
    // L0 in reverse (oldest → newest in the file; but order
    // within a level doesn't matter for the format).
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
            state.pending_compaction = Some(plan.out_number);
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
        let result = self.run_compaction_inner(plan);
        {
            let mut state = self.state.lock().unwrap();
            state.pending_compaction = None;
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
        let out_number = plan.out_number;
        let inputs = plan.inputs;
        let input_numbers = plan.input_numbers;
        let min_snap_seq = plan.min_snap_seq;

        let out_path = make_table_file_name(&self.path, out_number);

        // Run the merge → output SST. CompactionJob borrows the
        // file system; the inputs are owned via Arc.
        let job = CompactionJob::new(
            self.fs.as_ref(),
            inputs,
            &out_path,
            BlockBasedTableOptions::default(),
        )
        .with_min_snap_seq(min_snap_seq);
        let written = job.run()?;

        // Swap inputs for the output under the lock.
        let mut state = self.state.lock().unwrap();
        state
            .l0
            .retain(|e| !input_numbers.contains(&e.number));
        state
            .l1
            .retain(|e| !input_numbers.contains(&e.number));

        if written > 0 {
            // Open the new SST and add it to **L1** (compaction
            // output). L1 is sorted by smallest_key, so we
            // insert at the right position.
            let raw = self
                .fs
                .new_random_access_file(&out_path, &Default::default())?;
            let reader = RandomAccessFileReader::new(raw, out_path.display().to_string())?;
            let table = BlockBasedTableReader::open(Arc::new(reader))?;
            let (smallest, largest) = compute_sst_range(&table);
            let new_entry = SstEntry {
                number: out_number,
                reader: Arc::new(table),
                smallest_key: smallest.clone(),
                largest_key: largest,
                level: 1,
            };
            let pos = state
                .l1
                .partition_point(|e| e.smallest_key < smallest);
            state.l1.insert(pos, new_entry);
        }
        // Persist the updated level layout to CURRENT.
        let current = current_string(&state.l0, &state.l1);
        drop(state);
        self.write_current_atomic(&current)?;

        // Delete the input SST files. Best-effort: a leaked file
        // is harmless because the next reopen ignores files not
        // listed in CURRENT.
        for n in &input_numbers {
            let p = make_table_file_name(&self.path, *n);
            if self.fs.file_exists(&p)? {
                self.fs.delete_file(&p)?;
            }
        }

        // If the compaction produced no output (everything was
        // tombstones), remove the empty output file too.
        if written == 0 && self.fs.file_exists(&out_path)? {
            self.fs.delete_file(&out_path)?;
        }
        Ok(())
    }

    /// Close the DB. Drains any pending background flush, then
    /// flushes the active memtable synchronously, then releases
    /// the file lock.
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
            Record::DeleteRange { .. } => {
                // Layer 4a doesn't support range deletes; the WriteBatch
                // API still accepts them, but they become no-ops on
                // the WAL replay path.
            }
        }
    }
}

/// Inverse of [`encode_batch_record`]. Returns the new
/// `last_sequence` after applying the record. Used by WAL replay.
fn replay_record(
    record: &[u8],
    memtable: &mut MemTable,
    mut last_seq: SequenceNumber,
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

/// `WriteBatchHandler` adapter that inserts each record into a
/// memtable, advancing the sequence as it goes.
struct MemTableInsertHandler<'a> {
    memtable: &'a mut MemTable,
    seq: SequenceNumber,
}

impl<'a> WriteBatchHandler for MemTableInsertHandler<'a> {
    fn put_cf(
        &mut self,
        _cf: crate::core::types::ColumnFamilyId,
        key: &[u8],
        value: &[u8],
    ) -> Result<()> {
        self.memtable.add(self.seq, ValueType::Value, key, value)?;
        self.seq += 1;
        Ok(())
    }
    fn delete_cf(
        &mut self,
        _cf: crate::core::types::ColumnFamilyId,
        key: &[u8],
    ) -> Result<()> {
        self.memtable.add(self.seq, ValueType::Deletion, key, &[])?;
        self.seq += 1;
        Ok(())
    }
    fn single_delete_cf(
        &mut self,
        _cf: crate::core::types::ColumnFamilyId,
        key: &[u8],
    ) -> Result<()> {
        self.memtable
            .add(self.seq, ValueType::SingleDeletion, key, &[])?;
        self.seq += 1;
        Ok(())
    }
    fn merge_cf(
        &mut self,
        _cf: crate::core::types::ColumnFamilyId,
        key: &[u8],
        value: &[u8],
    ) -> Result<()> {
        self.memtable.add(self.seq, ValueType::Merge, key, value)?;
        self.seq += 1;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            (state.l0.len(), state.l1.len())
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
            (state.l0.len(), state.l1.len())
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
                state.pending_compaction.is_none(),
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
            assert!(state.immutable.is_none());
            assert!(state.pending_compaction.is_none());
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
            state.pending_compaction = Some(9999);
            db.maybe_pick_compaction(&mut state)
        };
        assert!(
            pick.is_none(),
            "picker should return None while a compaction is already pending"
        );
        // Clean up the forced flag so close() doesn't hang.
        {
            let mut state = db.state.lock().unwrap();
            state.pending_compaction = None;
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
}
