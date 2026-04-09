//! Port of `db/compaction/compaction_job.{h,cc}` simplified for
//! Layer 4b.
//!
//! At Layer 4a, the engine flushes memtables into SST files but
//! never deletes them — a long-running DB ends up with hundreds
//! of L0 files and reads get linearly slower. Layer 4b adds
//! compaction: when the live SST count crosses a threshold, the
//! engine merges the oldest N SSTs into a single output SST and
//! drops the inputs.
//!
//! # What's included
//!
//! - [`CompactionJob`] — runs a single compaction: open the input
//!   SSTs as iterators, k-way merge them via the
//!   [`MergingIterator`](crate::db::merging_iterator::MergingIterator),
//!   write a single output SST, drop tombstones (since this is the
//!   bottommost level — there's nothing older to shadow).
//! - [`pick_compaction`] — picker policy: returns the indices of
//!   the SSTs to compact, or `None` if no compaction is needed.
//!
//! # What's deferred to Layer 4c
//!
//! - **Multi-level layout (L0/L1/L2…)** — Layer 4b treats every
//!   SST as if it lived at the same level. Compaction merges N
//!   into 1 unconditionally. Picking by level + by overlap range
//!   is a Layer 4c refinement.
//! - **Background scheduling** — `CompactionJob::run` is called
//!   synchronously by `DbImpl`. Layer 4c will spawn it on the
//!   `StdThreadPool` from Layer 2.
//! - **Snapshot retention** — without snapshots, every compaction
//!   is "bottommost" and can drop tombstones outright.
//! - **Range delete handling** — no range delete block at this
//!   layer.

use crate::core::status::Result;
use crate::db::db_iter::SstUserKeyIter;
use crate::db::merging_iterator::{MergingIterator, UserKeyIter};
use crate::env::file_system::FileSystem;
use crate::file::writable_file_writer::WritableFileWriter;
use crate::sst::block_based::table_builder::{
    BlockBasedTableBuilder, BlockBasedTableOptions,
};
use crate::sst::block_based::table_reader::BlockBasedTableReader;
use std::path::Path;
use std::sync::Arc;

/// Picker policy. Returns the indices into a "newest-first" SST
/// list to compact, or `None` if the SST count is below the
/// trigger.
///
/// At Layer 4b the policy is dead simple: if there are at least
/// `trigger` SSTs, compact **all** of them into one. A real engine
/// would compact only the oldest few and respect overlap ranges,
/// but a single compaction-of-everything is correct (just slow on
/// large databases).
pub fn pick_compaction(num_ssts: usize, trigger: usize) -> Option<Vec<usize>> {
    if num_ssts < trigger {
        return None;
    }
    Some((0..num_ssts).collect())
}

/// One compaction job: input SSTs → one output SST.
pub struct CompactionJob<'a> {
    fs: &'a dyn FileSystem,
    /// SSTs to merge. The engine passes them in **newest-first**
    /// order; the merging iterator's first-source-wins tie-break
    /// then ensures the newest version of any duplicated user key
    /// survives.
    inputs: Vec<Arc<BlockBasedTableReader>>,
    /// Path of the output SST file.
    output_path: &'a Path,
    /// Builder options for the output SST.
    table_options: BlockBasedTableOptions,
}

impl<'a> CompactionJob<'a> {
    /// Construct a new compaction job. Caller is responsible for
    /// choosing a unique output file number / path.
    pub fn new(
        fs: &'a dyn FileSystem,
        inputs: Vec<Arc<BlockBasedTableReader>>,
        output_path: &'a Path,
        table_options: BlockBasedTableOptions,
    ) -> Self {
        Self {
            fs,
            inputs,
            output_path,
            table_options,
        }
    }

    /// Number of input keys (across all input SSTs, including
    /// duplicates and tombstones). Useful for tests.
    pub fn input_count(&self) -> usize {
        self.inputs.len()
    }

    /// Run the compaction. Writes a single output SST containing
    /// the live (non-tombstone) keys from the inputs, with
    /// duplicates collapsed (newer wins).
    ///
    /// Returns `Ok(num_output_records)` on success.
    pub fn run(self) -> Result<usize> {
        // 1. Build the merging iterator over the input SSTs.
        // We need to keep each `SstIter` alive for the duration
        // of the merge — that means borrowing each table reader.
        // The simplest correct shape is: hold the readers in a
        // local Vec and build iterators that borrow from them.
        let readers: Vec<Arc<BlockBasedTableReader>> = self.inputs;
        let mut sources: Vec<Box<dyn UserKeyIter>> = Vec::with_capacity(readers.len());
        // SAFETY-of-borrow: each `SstIter` borrows from the
        // corresponding `BlockBasedTableReader`. We pin the
        // readers in `readers` (they're `Arc`s, so cloning would
        // be cheap, but we don't even need to — we hold them by
        // value). The `SstIter` borrow lives no longer than the
        // surrounding `for` loop's borrow + the duration of the
        // merging iterator below, both of which end before
        // `readers` is dropped.
        //
        // The Rust borrow checker doesn't see the lifetime
        // relationship between `readers[i]` and the iterators we
        // push into `sources`. The cleanest way to satisfy it is
        // to keep `readers` alive in this scope and use a
        // self-contained `'static`-free closure region. Here we
        // build everything in one block:
        for reader in &readers {
            // Reborrow as a long-lived reference tied to `readers`.
            let it = reader.iter();
            // The boxed iterator's lifetime is bound by `&reader`,
            // which itself is bound by `&readers`. Since `readers`
            // outlives this entire scope, the box is valid for the
            // life of `sources`.
            sources.push(Box::new(SstUserKeyIter::new(it)));
        }
        let mut merging = MergingIterator::new(sources);

        // 2. Open the output writer.
        let writable = self
            .fs
            .new_writable_file(self.output_path, &Default::default())?;
        let mut tb = BlockBasedTableBuilder::new(
            WritableFileWriter::new(writable),
            self.table_options,
        );

        // 3. Stream the merged keys into the output, dropping
        //    tombstones (this is the bottommost level — nothing
        //    older to shadow).
        merging.seek_to_first();
        let mut written = 0usize;
        while merging.valid() {
            if !merging.is_tombstone() {
                tb.add(merging.key(), merging.value())?;
                written += 1;
            }
            merging.next();
        }
        // Surface any error captured by the merging iterator.
        merging.status()?;

        // 4. Finalise the output SST. Builder takes self.
        tb.finish()?;
        Ok(written)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::env::posix::PosixFileSystem;
    use crate::file::random_access_file_reader::RandomAccessFileReader;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn temp_dir(tag: &str) -> PathBuf {
        static C: AtomicU64 = AtomicU64::new(0);
        let n = C.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let dir = std::env::temp_dir().join(format!("st-rs-compact-{tag}-{pid}-{n}"));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn write_sst(
        fs: &dyn FileSystem,
        path: &Path,
        records: &[(&[u8], &[u8])],
    ) -> Arc<BlockBasedTableReader> {
        let writable = fs.new_writable_file(path, &Default::default()).unwrap();
        let mut tb = BlockBasedTableBuilder::new(
            WritableFileWriter::new(writable),
            BlockBasedTableOptions::default(),
        );
        for (k, v) in records {
            tb.add(k, v).unwrap();
        }
        tb.finish().unwrap();
        let raw = fs.new_random_access_file(path, &Default::default()).unwrap();
        let reader = RandomAccessFileReader::new(raw, path.display().to_string()).unwrap();
        Arc::new(BlockBasedTableReader::open(Arc::new(reader)).unwrap())
    }

    #[test]
    fn pick_compaction_returns_none_below_trigger() {
        assert_eq!(pick_compaction(0, 4), None);
        assert_eq!(pick_compaction(3, 4), None);
    }

    #[test]
    fn pick_compaction_returns_all_at_trigger() {
        let r = pick_compaction(4, 4).unwrap();
        assert_eq!(r, vec![0, 1, 2, 3]);
    }

    #[test]
    fn compaction_merges_two_disjoint_ssts() {
        let dir = temp_dir("merge");
        let fs = PosixFileSystem::new();

        let p1 = dir.join("a.sst");
        let p2 = dir.join("b.sst");
        let out = dir.join("out.sst");

        let t1 = write_sst(
            &fs,
            &p1,
            &[(b"a" as &[u8], b"1" as &[u8]), (b"c", b"3"), (b"e", b"5")],
        );
        let t2 = write_sst(
            &fs,
            &p2,
            &[(b"b" as &[u8], b"2" as &[u8]), (b"d", b"4"), (b"f", b"6")],
        );

        let job = CompactionJob::new(
            &fs,
            vec![t1, t2],
            &out,
            BlockBasedTableOptions::default(),
        );
        let n = job.run().unwrap();
        assert_eq!(n, 6);

        // Re-open the output and verify it has all six entries in order.
        let raw = fs
            .new_random_access_file(&out, &Default::default())
            .unwrap();
        let reader = RandomAccessFileReader::new(raw, out.display().to_string()).unwrap();
        let table = BlockBasedTableReader::open(Arc::new(reader)).unwrap();
        let mut it = table.iter();
        it.seek_to_first();
        let expected: &[&[u8]] = &[b"a", b"b", b"c", b"d", b"e", b"f"];
        let mut i = 0;
        while it.valid() {
            assert_eq!(it.key(), expected[i]);
            i += 1;
            it.next();
        }
        assert_eq!(i, 6);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn compaction_dedups_newer_wins() {
        let dir = temp_dir("dedup");
        let fs = PosixFileSystem::new();
        let p_new = dir.join("new.sst");
        let p_old = dir.join("old.sst");
        let out = dir.join("out.sst");

        // Newer SST has the live value.
        let t_new = write_sst(
            &fs,
            &p_new,
            &[(b"k1" as &[u8], b"new" as &[u8])],
        );
        let t_old = write_sst(
            &fs,
            &p_old,
            &[(b"k1" as &[u8], b"old" as &[u8])],
        );

        let job = CompactionJob::new(
            &fs,
            vec![t_new, t_old], // newest first
            &out,
            BlockBasedTableOptions::default(),
        );
        let n = job.run().unwrap();
        assert_eq!(n, 1);

        let raw = fs
            .new_random_access_file(&out, &Default::default())
            .unwrap();
        let reader = RandomAccessFileReader::new(raw, "out").unwrap();
        let table = BlockBasedTableReader::open(Arc::new(reader)).unwrap();
        assert_eq!(table.get(b"k1").unwrap(), Some(b"new".to_vec()));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn compaction_drops_tombstones() {
        let dir = temp_dir("tomb");
        let fs = PosixFileSystem::new();
        let p_new = dir.join("new.sst");
        let p_old = dir.join("old.sst");
        let out = dir.join("out.sst");

        // Newer SST has a tombstone (empty value) shadowing an old one.
        let t_new = write_sst(
            &fs,
            &p_new,
            &[(b"a" as &[u8], b"" as &[u8]), (b"b", b"keep")],
        );
        let t_old = write_sst(
            &fs,
            &p_old,
            &[(b"a" as &[u8], b"old" as &[u8])],
        );

        let job = CompactionJob::new(
            &fs,
            vec![t_new, t_old],
            &out,
            BlockBasedTableOptions::default(),
        );
        let n = job.run().unwrap();
        assert_eq!(n, 1, "only b should survive");

        let raw = fs
            .new_random_access_file(&out, &Default::default())
            .unwrap();
        let reader = RandomAccessFileReader::new(raw, "out").unwrap();
        let table = BlockBasedTableReader::open(Arc::new(reader)).unwrap();
        assert_eq!(table.get(b"a").unwrap(), None);
        assert_eq!(table.get(b"b").unwrap(), Some(b"keep".to_vec()));

        let _ = std::fs::remove_dir_all(&dir);
    }
}
