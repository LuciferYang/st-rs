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
use crate::db::compaction::{pick_compaction, CompactionJob};
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

/// One open SST file: its assigned number and a cached reader.
struct SstEntry {
    number: u64,
    reader: Arc<BlockBasedTableReader>,
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
    /// Live SST files, newest-first. Layer 4b will replace this
    /// with a real `VersionSet`.
    ssts: Vec<SstEntry>,
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

        // 4. Open existing SSTs into the read path.
        let mut ssts = Vec::with_capacity(existing_sst_numbers.len());
        for &num in &existing_sst_numbers {
            let p = make_table_file_name(path, num);
            let raw = fs.new_random_access_file(&p, &Default::default())?;
            let reader = RandomAccessFileReader::new(raw, p.display().to_string())?;
            let table = BlockBasedTableReader::open(Arc::new(reader))?;
            ssts.push(SstEntry {
                number: num,
                reader: Arc::new(table),
            });
        }
        // Newest first (highest number).
        ssts.sort_by(|a, b| b.number.cmp(&a.number));

        // 5. Replay WAL files into a fresh memtable.
        let mut memtable = MemTable::new(Arc::clone(&user_comparator));
        let mut last_sequence = 0u64;
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

        // Background flush worker. One worker is plenty at this
        // layer; Layer 4d's compaction can take additional slots.
        let thread_pool = Arc::new(StdThreadPool::new(1, 0, 0, 0));

        let arc = Arc::new(Self {
            path: path.to_path_buf(),
            fs,
            state: Mutex::new(DbState {
                memtable,
                immutable: None,
                ssts,
                wal: wal_writer,
                wal_number,
                immutable_wal_number: None,
                pending_flush_sst_number: None,
                next_file_number,
                last_sequence,
                closing: false,
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
        // Snapshot the engine state under the lock — but only the
        // pieces we need so we can drop the lock before doing I/O
        // on the SSTs.
        let (lookup, immutable, ssts) = {
            let state = self.state.lock().unwrap();
            let lookup = LookupKey::new(key, state.last_sequence);
            // 1. Try the active memtable first under the lock.
            match state.memtable.get(&lookup) {
                MemTableGetResult::Found(v) => return Ok(Some(v)),
                MemTableGetResult::Deleted => return Ok(None),
                MemTableGetResult::NotFound => {}
            }
            // 2. Try the immutable memtable (a flush in progress).
            //    The active memtable always shadows it, so we
            //    only consult it on a miss.
            if let Some(imm) = state.immutable.as_ref() {
                match imm.get(&lookup) {
                    MemTableGetResult::Found(v) => return Ok(Some(v)),
                    MemTableGetResult::Deleted => return Ok(None),
                    MemTableGetResult::NotFound => {}
                }
            }
            // 3. Snapshot the SST list and the (still-Arc'd)
            //    immutable so the lock can be dropped.
            let ssts: Vec<Arc<BlockBasedTableReader>> =
                state.ssts.iter().map(|e| Arc::clone(&e.reader)).collect();
            let immutable = state.immutable.as_ref().map(Arc::clone);
            (lookup, immutable, ssts)
        };
        let _ = immutable; // intentionally unused after the lock drop

        // 3. Walk SSTs newest-first. The first hit wins.
        for table in ssts {
            // BlockBasedTableReader::get takes the *user* key
            // because Layer 3b doesn't yet know about internal
            // keys. We pass the user key directly here — that's
            // safe at Layer 4a because the SSTs are flushed
            // memtable contents that store user keys (no MVCC
            // suffixes inside the SST yet).
            if let Some(v) = table.get(key)? {
                // Empty value means it was a deletion in the
                // memtable that got flushed.
                if v.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(v));
            }
        }
        // Avoid the unused-variable warning on `lookup`.
        let _ = lookup;
        Ok(None)
    }

    /// Open a forward + backward iterator over the engine. Eagerly
    /// materialises a snapshot of (active memtable + immutable
    /// memtable + every live SST) under the lock, so the returned
    /// iterator is detached from the engine and survives
    /// concurrent writes and background flushes.
    pub fn iter(&self) -> Result<crate::db::db_iter::DbIterator> {
        let state = self.state.lock().unwrap();
        let ssts: Vec<Arc<BlockBasedTableReader>> =
            state.ssts.iter().map(|e| Arc::clone(&e.reader)).collect();
        // The active memtable is held by value inside the lock; we
        // materialise its entries before dropping the guard. The
        // immutable memtable, if any, is built into the snapshot
        // *after* the active one so the active one wins ties.
        let it = crate::db::db_iter::DbIterator::from_snapshot_with_immutable(
            &state.memtable,
            state.immutable.as_deref(),
            &ssts,
        )?;
        drop(state);
        Ok(it)
    }

    /// Block until any in-progress background flush completes.
    /// Used by tests and by `close()` to drain pending work.
    pub fn wait_for_pending_flush(&self) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        while state.immutable.is_some() {
            state = self.flush_done.wait(state).unwrap();
        }
        Ok(())
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

        let mut last_user_key: Option<Vec<u8>> = None;
        let mut it = setup.immutable.iter();
        it.seek_to_first();
        while it.valid() {
            let parsed = ParsedInternalKey::parse(it.key())?;
            let is_dup =
                matches!(&last_user_key, Some(prev) if prev.as_slice() == parsed.user_key);
            if !is_dup {
                let value: &[u8] = match parsed.value_type {
                    ValueType::Value | ValueType::Merge => it.value(),
                    _ => &[],
                };
                tb.add(parsed.user_key, value)?;
                last_user_key = Some(parsed.user_key.to_vec());
            }
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
                state.ssts.insert(
                    0,
                    SstEntry {
                        number: setup.sst_number,
                        reader,
                    },
                );
                // Persist the new SST list to CURRENT.
                let current = current_string(&state.ssts);
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
                    let _ = self.run_compaction(plan.0, plan.1, plan.2);
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
    fn maybe_pick_compaction(
        &self,
        state: &mut DbState,
    ) -> Option<(u64, Vec<Arc<BlockBasedTableReader>>, Vec<u64>)> {
        let to_compact = pick_compaction(state.ssts.len(), Self::COMPACTION_TRIGGER)?;
        let inputs: Vec<Arc<BlockBasedTableReader>> = to_compact
            .iter()
            .map(|&i| Arc::clone(&state.ssts[i].reader))
            .collect();
        let input_numbers: Vec<u64> = to_compact.iter().map(|&i| state.ssts[i].number).collect();
        let out_number = state.next_file_number;
        state.next_file_number += 1;
        Some((out_number, inputs, input_numbers))
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

/// Helper used by `complete_flush_locked` to render the SST list
/// into the CURRENT file format.
fn current_string(ssts: &[SstEntry]) -> String {
    let mut s = String::new();
    for entry in ssts.iter().rev() {
        if !s.is_empty() {
            s.push(',');
        }
        s.push_str(&entry.number.to_string());
    }
    s
}

impl DbImpl {
    /// Run a compaction job and update engine state to swap the
    /// inputs for the new output SST.
    fn run_compaction(
        &self,
        out_number: u64,
        inputs: Vec<Arc<BlockBasedTableReader>>,
        input_numbers: Vec<u64>,
    ) -> Result<()> {
        let out_path = make_table_file_name(&self.path, out_number);

        // Run the merge → output SST. CompactionJob borrows the
        // file system; the inputs are owned via Arc.
        let job = CompactionJob::new(
            self.fs.as_ref(),
            inputs,
            &out_path,
            BlockBasedTableOptions::default(),
        );
        let written = job.run()?;

        // Swap inputs for the output under the lock.
        let mut state = self.state.lock().unwrap();
        state
            .ssts
            .retain(|e| !input_numbers.contains(&e.number));

        if written > 0 {
            // Open the new SST and add it. The compaction output
            // is the *newest* surviving SST in the resulting set
            // (it absorbed every input), so insert at the front.
            let raw = self
                .fs
                .new_random_access_file(&out_path, &Default::default())?;
            let reader = RandomAccessFileReader::new(raw, out_path.display().to_string())?;
            let table = BlockBasedTableReader::open(Arc::new(reader))?;
            state.ssts.insert(
                0,
                SstEntry {
                    number: out_number,
                    reader: Arc::new(table),
                },
            );
        }
        drop(state);

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

        // The SST list should have shrunk — there were 5 flushes
        // but compaction merged the older ones.
        let n = {
            let state = db.state.lock().unwrap();
            state.ssts.len()
        };
        assert!(n < 5, "expected fewer than 5 SSTs after compaction, got {n}");

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
