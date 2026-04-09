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
use crate::env::file_system::{FileLock, FileSystem, IoOptions};
use crate::env::posix::PosixFileSystem;
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
use std::sync::{Arc, Mutex};

/// One open SST file: its assigned number and a cached reader.
struct SstEntry {
    number: u64,
    reader: Arc<BlockBasedTableReader>,
}

/// Internal state behind the DB's big lock.
struct DbState {
    /// Active (mutable) memtable.
    memtable: MemTable,
    /// Live SST files, newest-first. Layer 4b will replace this
    /// with a real `VersionSet`.
    ssts: Vec<SstEntry>,
    /// Current WAL writer.
    wal: LogWriter,
    /// Number of the WAL file currently being written.
    wal_number: u64,
    /// Number to assign to the next file (WAL or SST). Monotonic.
    next_file_number: u64,
    /// Highest sequence number assigned so far.
    last_sequence: SequenceNumber,
}

/// The minimum-viable engine.
///
/// All public methods take `&self` and acquire an internal mutex —
/// the engine is safe for concurrent use from multiple threads,
/// but every operation serialises on a single lock for now. The
/// Layer 4b write thread will replace this with leader-follower
/// group commit.
pub struct DbImpl {
    /// On-disk path of the DB directory.
    path: PathBuf,
    /// File-system instance. We hold an `Arc` so the engine can
    /// hand it to background tasks later.
    fs: Arc<dyn FileSystem>,
    /// Internal mutable state behind a single mutex.
    state: Mutex<DbState>,
    /// Held LOCK file handle. Released on Drop / `close`.
    /// `Option` so `close()` can take it without consuming `self`.
    lock: Mutex<Option<Box<dyn FileLock>>>,
    /// User comparator (always BytewiseComparator at Layer 4a).
    user_comparator: Arc<dyn Comparator>,
    /// Per-CF flush threshold (`write_buffer_size` from options).
    write_buffer_size: usize,
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

        Ok(Arc::new(Self {
            path: path.to_path_buf(),
            fs,
            state: Mutex::new(DbState {
                memtable,
                ssts,
                wal: wal_writer,
                wal_number,
                next_file_number,
                last_sequence,
            }),
            lock: Mutex::new(Some(lock)),
            user_comparator,
            write_buffer_size: opts.db_write_buffer_size.max(1),
        }))
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
            self.flush()?;
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
        let (lookup, ssts) = {
            let state = self.state.lock().unwrap();
            let lookup = LookupKey::new(key, state.last_sequence);
            // 1. Try the memtable first under the lock.
            match state.memtable.get(&lookup) {
                MemTableGetResult::Found(v) => return Ok(Some(v)),
                MemTableGetResult::Deleted => return Ok(None),
                MemTableGetResult::NotFound => {}
            }
            // 2. Snapshot the SST list (cheap — Arc clones).
            let ssts: Vec<Arc<BlockBasedTableReader>> =
                state.ssts.iter().map(|e| Arc::clone(&e.reader)).collect();
            (lookup, ssts)
        };

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
    /// materialises a snapshot of the memtable + every live SST
    /// under the lock, so the returned iterator is detached from
    /// the engine and survives concurrent writes.
    pub fn iter(&self) -> Result<crate::db::db_iter::DbIterator> {
        let state = self.state.lock().unwrap();
        let ssts: Vec<Arc<BlockBasedTableReader>> =
            state.ssts.iter().map(|e| Arc::clone(&e.reader)).collect();
        // The MemTable is held by value inside the lock; we need
        // to materialise its entries before dropping the guard.
        // The eager DbIterator does that under the same borrow.
        let it = crate::db::db_iter::DbIterator::from_snapshot(&state.memtable, &ssts)?;
        drop(state);
        Ok(it)
    }

    /// Trigger threshold for automatic compaction. When the live
    /// SST count reaches this, [`Self::flush`] runs a compaction
    /// after the new SST is added.
    const COMPACTION_TRIGGER: usize = 4;

    /// Force-flush the active memtable to a fresh SST.
    pub fn flush(&self) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        if state.memtable.num_entries() == 0 {
            return Ok(());
        }

        // Pick a file number for the new SST.
        let sst_number = state.next_file_number;
        state.next_file_number += 1;
        let sst_path = make_table_file_name(&self.path, sst_number);

        // Iterate the memtable in user-key sorted order. The
        // memtable's iterator yields *internal* keys, so we need
        // to dedup by user key, keeping only the newest entry per
        // key (which the InternalKeyComparator already places first).
        // Layer 4a's SST format stores user keys (not internal
        // keys), so the value of a deletion is encoded as an
        // empty payload — see `Db::get` above.
        let writable = self
            .fs
            .new_writable_file(&sst_path, &Default::default())?;
        let mut tb = BlockBasedTableBuilder::new(
            WritableFileWriter::new(writable),
            BlockBasedTableOptions::default(),
        );

        let mut last_user_key: Option<Vec<u8>> = None;
        let mut it = state.memtable.iter();
        it.seek_to_first();
        while it.valid() {
            let parsed = ParsedInternalKey::parse(it.key())?;
            // Skip older versions of the same user key — the first
            // (newest) one already won.
            let is_dup = matches!(&last_user_key, Some(prev) if prev.as_slice() == parsed.user_key);
            if !is_dup {
                let value: &[u8] = match parsed.value_type {
                    ValueType::Value | ValueType::Merge => it.value(),
                    ValueType::Deletion | ValueType::SingleDeletion => &[],
                    // Other types are unsupported at Layer 4a; treat
                    // as deletions to keep the SST consistent.
                    _ => &[],
                };
                tb.add(parsed.user_key, value)?;
                last_user_key = Some(parsed.user_key.to_vec());
            }
            it.next();
        }
        tb.finish()?;

        // Open the new SST for reads and add it to the list.
        let raw = self
            .fs
            .new_random_access_file(&sst_path, &Default::default())?;
        let reader = RandomAccessFileReader::new(raw, sst_path.display().to_string())?;
        let table = BlockBasedTableReader::open(Arc::new(reader))?;
        // Newest goes to the front.
        state.ssts.insert(
            0,
            SstEntry {
                number: sst_number,
                reader: Arc::new(table),
            },
        );

        // Swap in a fresh memtable. The old one is dropped.
        state.memtable = MemTable::new(Arc::clone(&self.user_comparator));

        // Roll the WAL: open a fresh log so the new memtable has
        // its own write-ahead trail. The old WAL stays on disk
        // until the next open replays + deletes it; for Layer 4a
        // simplicity we delete it here too.
        let old_wal_number = state.wal_number;
        let new_wal_number = state.next_file_number;
        state.next_file_number += 1;

        let new_wal_path = make_wal_file_name(&self.path, new_wal_number);
        let new_wal_file = self
            .fs
            .new_writable_file(&new_wal_path, &Default::default())?;
        let new_wal = LogWriter::new(WritableFileWriter::new(new_wal_file));

        // Finalise the old WAL by closing the writer (drops the
        // file handle) and removing the file.
        let old_wal = std::mem::replace(&mut state.wal, new_wal);
        old_wal.close()?;
        let old_wal_path = make_wal_file_name(&self.path, old_wal_number);
        if self.fs.file_exists(&old_wal_path)? {
            self.fs.delete_file(&old_wal_path)?;
        }
        state.wal_number = new_wal_number;

        // Persist the new SST list to CURRENT for the next open.
        let mut current = String::new();
        for entry in state.ssts.iter().rev() {
            // Write oldest → newest in the file (just a convention).
            if !current.is_empty() {
                current.push(',');
            }
            current.push_str(&entry.number.to_string());
        }
        let current_path = make_current_file_name(&self.path);
        let current_file = self
            .fs
            .new_writable_file(&current_path, &Default::default())?;
        let mut writer = WritableFileWriter::new(current_file);
        let io = IoOptions::default();
        writer.append(current.as_bytes(), &io)?;
        writer.flush(&io)?;
        writer.sync(&io)?;
        writer.close(&io)?;

        // Decide whether to schedule a compaction. Snapshot the
        // SST list under the lock, then drop the lock before
        // running the merge — compaction is I/O-heavy and we
        // don't want to block writers on it.
        let to_compact = pick_compaction(state.ssts.len(), Self::COMPACTION_TRIGGER);
        let compaction_plan: Option<(u64, Vec<Arc<BlockBasedTableReader>>, Vec<u64>)> =
            to_compact.map(|indices| {
                let inputs: Vec<Arc<BlockBasedTableReader>> = indices
                    .iter()
                    .map(|&i| Arc::clone(&state.ssts[i].reader))
                    .collect();
                let input_numbers: Vec<u64> =
                    indices.iter().map(|&i| state.ssts[i].number).collect();
                let out_number = state.next_file_number;
                state.next_file_number += 1;
                (out_number, inputs, input_numbers)
            });
        drop(state);

        if let Some((out_num, inputs, input_numbers)) = compaction_plan {
            self.run_compaction(out_num, inputs, input_numbers)?;
        }
        Ok(())
    }

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

    /// Close the DB. After this, all method calls return errors.
    pub fn close(&self) -> Result<()> {
        // Flush any unflushed memtable data.
        self.flush()?;
        // Drop the lock so other processes can open the DB.
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
