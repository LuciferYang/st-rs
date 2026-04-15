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
use crate::core::types::{EntryType, SequenceNumber, ValueType};
use crate::db::dbformat::ParsedInternalKey;
use crate::env::file_system::FileSystem;
use crate::ext::compaction_filter::{CompactionDecision, CompactionFilter};
use crate::file::writable_file_writer::WritableFileWriter;
use crate::sst::block_based::table_builder::{
    BlockBasedTableBuilder, BlockBasedTableOptions,
};
use crate::sst::block_based::table_reader::BlockBasedTableReader;
use std::path::Path;
use std::sync::Arc;

/// Picker policy. Returns the indices into a **newest-first** SST
/// list to compact, or `None` if the SST count is below the
/// trigger.
///
/// At Layer 4c the policy is "compact the oldest `batch_size` SSTs
/// when there are at least `trigger`". This is a meaningful
/// improvement over Layer 4b's "compact everything" because:
///
/// - It bounds the per-compaction work (otherwise a 1000-SST DB
///   would do a single 1000-input compaction).
/// - It preserves recent writes' fast path — the newest SST is
///   never touched until enough older ones accumulate, so a
///   subsequent point lookup that hits the freshest SST stays
///   fast.
///
/// The returned indices are **into the newest-first SST list** —
/// since we're compacting the *oldest* SSTs, they're the indices
/// at the end of the list. Concretely: for a list `[s_new, s,
/// s, s_old]` and `batch_size = 2`, the picker returns `[2, 3]`.
///
/// A future Layer 4d will replace this with overlap-aware
/// per-level picking.
pub fn pick_compaction(num_ssts: usize, trigger: usize) -> Option<Vec<usize>> {
    pick_compaction_batch(num_ssts, trigger, trigger)
}

/// Same as [`pick_compaction`] but with an explicit `batch_size`.
/// `batch_size <= num_ssts` is required; the picker will use
/// `min(batch_size, num_ssts)` if you pass too many. Returns the
/// **last `batch_size` indices** of the newest-first list, which
/// correspond to the oldest SSTs.
pub fn pick_compaction_batch(
    num_ssts: usize,
    trigger: usize,
    batch_size: usize,
) -> Option<Vec<usize>> {
    if num_ssts < trigger {
        return None;
    }
    let batch = batch_size.min(num_ssts);
    let start = num_ssts - batch;
    Some((start..num_ssts).collect())
}

/// One compaction job: input SSTs → one output SST.
pub struct CompactionJob<'a> {
    fs: &'a dyn FileSystem,
    /// SSTs to merge. The engine passes them in **newest-first**
    /// order; with internal-key SSTs this only matters for the
    /// collect-into-a-vec step's tie-breaking on exact internal
    /// keys (which shouldn't happen anyway — each put gets a
    /// unique sequence).
    inputs: Vec<Arc<BlockBasedTableReader>>,
    /// Path of the output SST file.
    output_path: &'a Path,
    /// Builder options for the output SST.
    table_options: BlockBasedTableOptions,
    /// Oldest live snapshot sequence. Versions with
    /// `seq > min_snap_seq` are preserved because some live
    /// snapshot above may need them; the newest version with
    /// `seq <= min_snap_seq` is also preserved because the
    /// oldest snapshot needs it. Older versions below
    /// `min_snap_seq` are dropped.
    ///
    /// Defaults to `u64::MAX` which means "no live snapshots" —
    /// the retention rule then collapses to the Layer 4d
    /// behaviour of keeping only the newest version per user key.
    min_snap_seq: SequenceNumber,
    /// Optional compaction filter. Applied to each entry the
    /// retention rule would emit. Can drop, keep, or rewrite entries.
    compaction_filter: Option<Box<dyn CompactionFilter>>,
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
            min_snap_seq: SequenceNumber::MAX,
            compaction_filter: None,
        }
    }

    /// Builder: set the oldest live snapshot sequence. See the
    /// `min_snap_seq` field doc for how this affects retention.
    #[must_use]
    pub fn with_min_snap_seq(mut self, min_snap_seq: SequenceNumber) -> Self {
        self.min_snap_seq = min_snap_seq;
        self
    }

    /// Builder: set a compaction filter.
    #[must_use]
    pub fn with_compaction_filter(mut self, filter: Box<dyn CompactionFilter>) -> Self {
        self.compaction_filter = Some(filter);
        self
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
    ///
    /// # Layer 4d: internal-key compaction
    ///
    /// Input SSTs store internal keys (`user_key || BE-inverted(
    /// (seq<<8)|type)`), and so does the output. The merge logic is:
    ///
    /// 1. Collect every `(internal_key, value)` pair from every
    ///    input SST into a `Vec`.
    /// 2. Sort bytewise — this yields `(user_key asc, seq desc,
    ///    type desc)` order thanks to the BE-inverted encoding.
    /// 3. Walk the sorted vec, emitting the **first** entry per
    ///    unique user key. Subsequent entries for the same user
    ///    key are older versions, dropped.
    /// 4. Tombstones encountered as the newest version are
    ///    dropped outright because Layer 4d compaction is always
    ///    bottommost. Snapshot-aware retention lands in Layer 4e.
    ///
    /// O(N) memory, O(N log N) time. A streaming k-way merge
    /// arrives in Layer 4e alongside multi-level compaction.
    pub fn run(self) -> Result<usize> {
        let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for reader in &self.inputs {
            let mut it = reader.iter();
            it.seek_to_first();
            while it.valid() {
                entries.push((it.key().to_vec(), it.value().to_vec()));
                it.next();
            }
            it.status()?;
        }
        entries.sort_by(|a, b| a.0.cmp(&b.0));

        let writable = self
            .fs
            .new_writable_file(self.output_path, &Default::default())?;
        let mut tb = BlockBasedTableBuilder::new(
            WritableFileWriter::new(writable),
            self.table_options,
        );

        // Snapshot-aware retention rule (Layer 4e):
        //
        //   For each user key, walk versions newest-first:
        //   - Emit the first version unconditionally (it's the
        //     newest, needed by any current read).
        //   - Emit subsequent versions whose seq > min_snap_seq
        //     (they're above the oldest live snapshot and some
        //     snapshot above may need them).
        //   - Emit the first version with seq <= min_snap_seq
        //     that we encounter (needed by the min_snap_seq
        //     snapshot for this user key).
        //   - Drop everything older than that.
        //
        //   When `min_snap_seq == u64::MAX` (no live snapshots),
        //   this collapses to "emit only the first version per
        //   user key" — the Layer 4d behaviour.
        //
        // Tombstones are dropped only when they *would* be the
        // sole entry for a user key AND there's no live snapshot
        // needing the shadowed version. At this layer we keep
        // tombstones whenever the retention rule says "emit" —
        // that includes the "newest version is a tombstone"
        // case, because a future layer with multi-level
        // compaction will want the tombstone to shadow older
        // versions at lower levels. The exception is the
        // `min_snap_seq == MAX` case where we have no lower
        // level and no live snapshot, which is the Layer 4d
        // bottommost rule: drop tombstones outright.
        let mut written = 0usize;
        let mut last_user_key: Option<Vec<u8>> = None;
        let mut emitted_below_min = false;
        let drop_tombstones = self.min_snap_seq == SequenceNumber::MAX;
        for (ikey, value) in &entries {
            let parsed = ParsedInternalKey::parse(ikey)?;
            let new_user_key = last_user_key.as_deref() != Some(parsed.user_key);

            // Decide whether to emit this entry.
            let emit = if new_user_key {
                last_user_key = Some(parsed.user_key.to_vec());
                emitted_below_min = parsed.sequence <= self.min_snap_seq;
                true
            } else if parsed.sequence > self.min_snap_seq {
                // Still above the oldest live snapshot — preserve.
                true
            } else if !emitted_below_min {
                // First version at or below min_snap_seq for
                // this user key — preserve (snapshots need it).
                emitted_below_min = true;
                true
            } else {
                // Older version below min_snap_seq already
                // shadowed for every live snapshot.
                false
            };

            if !emit {
                continue;
            }

            match parsed.value_type {
                ValueType::Value | ValueType::Merge => {
                    // Apply compaction filter if present. Merge entries
                    // are only filtered if allow_merge() returns true.
                    let should_filter = self.compaction_filter.as_ref().is_some_and(|f| {
                        parsed.value_type != ValueType::Merge || f.allow_merge()
                    });
                    if should_filter {
                        let filter = self.compaction_filter.as_ref().unwrap();
                        let entry_type = match parsed.value_type {
                            ValueType::Value => EntryType::Put,
                            ValueType::Merge => EntryType::Merge,
                            _ => EntryType::Other,
                        };
                        // Output level is always 1 (L0→L1 compaction).
                        match filter.filter(1, parsed.user_key, entry_type, value) {
                            CompactionDecision::Keep => {
                                tb.add(ikey, value)?;
                                written += 1;
                            }
                            CompactionDecision::Remove => {
                                // Filtered out — don't write to output.
                            }
                            CompactionDecision::ChangeValue(new_val) => {
                                tb.add(ikey, &new_val)?;
                                written += 1;
                            }
                            CompactionDecision::RemoveAndSkipUntil(_skip_key) => {
                                // TODO: implement skip-until semantics.
                                // For now, treat as Remove (drops only this
                                // entry, does NOT skip ahead). Flink's
                                // FlinkCompactionFilter does not use this
                                // variant, so the simplification is safe
                                // for the Flink integration path.
                            }
                        }
                    } else {
                        tb.add(ikey, value)?;
                        written += 1;
                    }
                }
                ValueType::Deletion | ValueType::SingleDeletion => {
                    if !drop_tombstones {
                        tb.add(ikey, value)?;
                        written += 1;
                    }
                }
                _ => {}
            }
        }
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

    /// Write an **internal-key** SST for the tests. Each record is
    /// `(user_key, seq, value_type, value)`; the test code
    /// constructs the internal key via `InternalKey::new` before
    /// passing it to the block builder. Records must be provided
    /// in internal-key sorted order (typically user-key ascending
    /// with at most one version per user key in each test SST,
    /// which is already sorted).
    fn write_internal_sst(
        fs: &dyn FileSystem,
        path: &Path,
        records: &[(&[u8], crate::core::types::SequenceNumber, ValueType, &[u8])],
    ) -> Arc<BlockBasedTableReader> {
        use crate::db::dbformat::InternalKey;
        let writable = fs.new_writable_file(path, &Default::default()).unwrap();
        let mut tb = BlockBasedTableBuilder::new(
            WritableFileWriter::new(writable),
            BlockBasedTableOptions::default(),
        );
        // Build internal keys and collect into a Vec so we can
        // sort them (the encoding guarantees bytewise sort =
        // internal-key order).
        let mut entries: Vec<(Vec<u8>, Vec<u8>)> = records
            .iter()
            .map(|(k, seq, t, v)| {
                let ikey = InternalKey::new(k, *seq, *t).into_bytes();
                (ikey, v.to_vec())
            })
            .collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        for (ikey, value) in &entries {
            tb.add(ikey, value).unwrap();
        }
        tb.finish().unwrap();
        let raw = fs.new_random_access_file(path, &Default::default()).unwrap();
        let reader = RandomAccessFileReader::new(raw, path.display().to_string()).unwrap();
        Arc::new(BlockBasedTableReader::open(Arc::new(reader)).unwrap())
    }

    /// Open an SST and collect its entries as `(user_key, value)`
    /// pairs, decoding the stored internal keys. Used by tests
    /// to assert the contents of a compaction output.
    fn read_internal_sst_entries(
        fs: &dyn FileSystem,
        path: &Path,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let raw = fs.new_random_access_file(path, &Default::default()).unwrap();
        let reader = RandomAccessFileReader::new(raw, path.display().to_string()).unwrap();
        let table = BlockBasedTableReader::open(Arc::new(reader)).unwrap();
        let mut out = Vec::new();
        let mut it = table.iter();
        it.seek_to_first();
        while it.valid() {
            let parsed = ParsedInternalKey::parse(it.key()).unwrap();
            out.push((parsed.user_key.to_vec(), it.value().to_vec()));
            it.next();
        }
        out
    }

    #[test]
    fn pick_compaction_returns_none_below_trigger() {
        assert_eq!(pick_compaction(0, 4), None);
        assert_eq!(pick_compaction(3, 4), None);
    }

    #[test]
    fn pick_compaction_returns_oldest_batch_at_trigger() {
        // 4 SSTs, trigger = 4, default batch = trigger.
        // Returns the LAST 4 indices of a newest-first list = the
        // 4 oldest SSTs.
        let r = pick_compaction(4, 4).unwrap();
        assert_eq!(r, vec![0, 1, 2, 3]);
    }

    #[test]
    fn pick_compaction_with_more_than_trigger_picks_oldest() {
        // 7 SSTs, trigger = 4, batch = 4 (default).
        // Should pick the 4 OLDEST = indices [3, 4, 5, 6] (the
        // tail of the newest-first list).
        let r = pick_compaction(7, 4).unwrap();
        assert_eq!(r, vec![3, 4, 5, 6]);
    }

    #[test]
    fn pick_compaction_batch_explicit_size() {
        // Custom batch size: pick only the 2 oldest.
        let r = pick_compaction_batch(7, 4, 2).unwrap();
        assert_eq!(r, vec![5, 6]);
    }

    #[test]
    fn compaction_merges_two_disjoint_ssts() {
        let dir = temp_dir("merge");
        let fs = PosixFileSystem::new();

        let p1 = dir.join("a.sst");
        let p2 = dir.join("b.sst");
        let out = dir.join("out.sst");

        // Each SST has three entries. Sequence numbers are chosen
        // so every entry has a unique (user_key, seq).
        let t1 = write_internal_sst(
            &fs,
            &p1,
            &[
                (b"a", 1, ValueType::Value, b"1"),
                (b"c", 3, ValueType::Value, b"3"),
                (b"e", 5, ValueType::Value, b"5"),
            ],
        );
        let t2 = write_internal_sst(
            &fs,
            &p2,
            &[
                (b"b", 2, ValueType::Value, b"2"),
                (b"d", 4, ValueType::Value, b"4"),
                (b"f", 6, ValueType::Value, b"6"),
            ],
        );

        let job = CompactionJob::new(
            &fs,
            vec![t1, t2],
            &out,
            BlockBasedTableOptions::default(),
        );
        let n = job.run().unwrap();
        assert_eq!(n, 6);

        let entries = read_internal_sst_entries(&fs, &out);
        assert_eq!(entries.len(), 6);
        let expected: Vec<(Vec<u8>, Vec<u8>)> = [
            ("a", "1"),
            ("b", "2"),
            ("c", "3"),
            ("d", "4"),
            ("e", "5"),
            ("f", "6"),
        ]
        .iter()
        .map(|(k, v)| (k.as_bytes().to_vec(), v.as_bytes().to_vec()))
        .collect();
        assert_eq!(entries, expected);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn compaction_dedups_newer_wins() {
        let dir = temp_dir("dedup");
        let fs = PosixFileSystem::new();
        let p_new = dir.join("new.sst");
        let p_old = dir.join("old.sst");
        let out = dir.join("out.sst");

        // Newer SST has the live value at a higher sequence.
        let t_new = write_internal_sst(
            &fs,
            &p_new,
            &[(b"k1", 10, ValueType::Value, b"new")],
        );
        let t_old = write_internal_sst(
            &fs,
            &p_old,
            &[(b"k1", 1, ValueType::Value, b"old")],
        );

        let job = CompactionJob::new(
            &fs,
            vec![t_new, t_old],
            &out,
            BlockBasedTableOptions::default(),
        );
        let n = job.run().unwrap();
        assert_eq!(n, 1);

        let entries = read_internal_sst_entries(&fs, &out);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, b"k1".to_vec());
        assert_eq!(entries[0].1, b"new".to_vec());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn compaction_drops_tombstones() {
        let dir = temp_dir("tomb");
        let fs = PosixFileSystem::new();
        let p_new = dir.join("new.sst");
        let p_old = dir.join("old.sst");
        let out = dir.join("out.sst");

        // Newer SST has a Deletion tombstone for "a" + a live "b".
        let t_new = write_internal_sst(
            &fs,
            &p_new,
            &[
                (b"a", 10, ValueType::Deletion, b""),
                (b"b", 11, ValueType::Value, b"keep"),
            ],
        );
        let t_old = write_internal_sst(
            &fs,
            &p_old,
            &[(b"a", 1, ValueType::Value, b"old")],
        );

        let job = CompactionJob::new(
            &fs,
            vec![t_new, t_old],
            &out,
            BlockBasedTableOptions::default(),
        );
        let n = job.run().unwrap();
        assert_eq!(n, 1, "only b should survive");

        let entries = read_internal_sst_entries(&fs, &out);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, b"b".to_vec());
        assert_eq!(entries[0].1, b"keep".to_vec());

        let _ = std::fs::remove_dir_all(&dir);
    }
}
