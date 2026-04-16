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

//! Port of `db/db_iter.{h,cc}` simplified for Layer 4b.
//!
//! Two things live here:
//!
//! 1. **Adapter types** ([`MemtableUserKeyIter`] and
//!    [`SstUserKeyIter`]) that present the engine's heterogeneous
//!    sources (memtable internal-key skiplist, SST user-key block
//!    iterator) through the uniform
//!    [`crate::db::merging_iterator::UserKeyIter`] interface.
//!    These are used by **compaction** to k-way-merge SSTs.
//!
//! 2. The user-facing [`DbIterator`] returned by
//!    [`crate::db::db_impl::DbImpl::iter`]. At Layer 4b this is
//!    **eager**: it materialises every live (non-tombstone) entry
//!    in the engine into a sorted owned `Vec` at construction
//!    time, then walks the vec on each `next` call.
//!
//! # Why eager?
//!
//! A streaming `DbIterator` would need to hold a `MergingIterator`
//! whose sources include `SstIter`s that borrow from
//! `BlockBasedTableReader` instances owned by the engine state.
//! That's a textbook self-referential lifetime problem in Rust:
//! the iterator can't simultaneously own the table-reader Arcs
//! and borrow from them. Workarounds (`ouroboros`, `Pin` + raw
//! pointers, full unsafe) are out of scope at this layer.
//!
//! Eager materialisation is O(N) memory and time per `iter()`
//! call but makes the iterator a plain `Vec` walk with no
//! lifetimes — which keeps Layer 4b safe and reviewable. A
//! Layer 4c follow-up can introduce a streaming variant behind
//! `unsafe` (matching upstream).

use crate::core::status::Result;
use crate::core::types::{SequenceNumber, ValueType};
use crate::db::dbformat::ParsedInternalKey;
use crate::db::memtable::MemTable;
use crate::db::merging_iterator::{MergingIterator, UserKeyIter};
use crate::memtable::skip_list::SkipListIter;
use crate::sst::block_based::sst_iterator::SstIter;
use crate::sst::block_based::table_reader::BlockBasedTableReader;
use std::collections::BTreeMap;
use std::sync::Arc;

/// "Read at the latest possible sequence" — used by default when
/// no explicit snapshot is passed. The 56-bit sequence field has
/// plenty of headroom.
pub const READ_AT_LATEST: SequenceNumber = u64::MAX >> 8;

// ---------------------------------------------------------------------------
// MemtableUserKeyIter — adapts the memtable's internal-key iterator
// ---------------------------------------------------------------------------

/// Wraps a [`SkipListIter`] over the memtable so it presents a
/// **user-key**-shaped view to the merging iterator.
///
/// The memtable stores entries as `(internal_key, value)` where
/// `internal_key = user_key || trailer(seq, type)`. The
/// `InternalKeyComparator` orders these by user-key ascending and
/// then sequence descending, so the *first* entry for any given
/// user key is always the newest version.
///
/// This wrapper presents only the newest version per user key.
/// Tombstones (Deletion / SingleDeletion) are presented with an
/// empty value, matching the engine convention.
pub struct MemtableUserKeyIter<'a> {
    inner: SkipListIter<'a>,
    cur_key: Vec<u8>,
    cur_value: Vec<u8>,
    valid: bool,
    /// Read-at sequence — only entries with `seq <= read_seq` are
    /// visible. Defaults to [`READ_AT_LATEST`] when the caller
    /// didn't pick a specific snapshot.
    read_seq: SequenceNumber,
}

impl<'a> MemtableUserKeyIter<'a> {
    /// Build a new wrapper that reads at the latest sequence.
    /// Caller must call `seek_to_first` or `seek` before reading.
    pub fn new(memtable: &'a MemTable) -> Self {
        Self::new_at(memtable, READ_AT_LATEST)
    }

    /// Build a wrapper that reads at a specific snapshot sequence.
    /// Entries with `seq > read_seq` are invisible — the iterator
    /// skips past them when positioning.
    pub fn new_at(memtable: &'a MemTable, read_seq: SequenceNumber) -> Self {
        Self {
            inner: memtable.iter(),
            cur_key: Vec::new(),
            cur_value: Vec::new(),
            valid: false,
            read_seq,
        }
    }

    /// Walk the inner iterator forward until we find an entry
    /// whose sequence is `<= read_seq` (visible at our snapshot).
    /// Thanks to internal-key ordering (newest-first per user
    /// key), the first visible entry per user key is the newest
    /// version at or below `read_seq` — exactly what a snapshot
    /// read wants.
    ///
    /// If an entire user key's versions are all above `read_seq`,
    /// this function's loop naturally advances to the next user
    /// key without surfacing anything for the invisible one.
    fn read_current(&mut self) {
        while self.inner.valid() {
            let parsed = match ParsedInternalKey::parse(self.inner.key()) {
                Ok(p) => p,
                Err(_) => {
                    self.valid = false;
                    return;
                }
            };
            if parsed.sequence > self.read_seq {
                // Invisible — skip and keep scanning.
                self.inner.next();
                continue;
            }
            // First visible version for this user key. Surface it.
            self.cur_key.clear();
            self.cur_key.extend_from_slice(parsed.user_key);
            self.cur_value.clear();
            match parsed.value_type {
                ValueType::Value | ValueType::Merge => {
                    self.cur_value.extend_from_slice(self.inner.value());
                }
                _ => {
                    // Deletion / SingleDeletion / RangeDeletion /
                    // blob / wide-column → empty value (tombstone).
                }
            }
            self.valid = true;
            return;
        }
        self.valid = false;
        self.cur_key.clear();
        self.cur_value.clear();
    }

    /// Advance past every remaining entry that shares the current
    /// user key — they're all older versions of a key we've
    /// already surfaced.
    fn skip_duplicates(&mut self) {
        let cur_key = self.cur_key.clone();
        loop {
            self.inner.next();
            if !self.inner.valid() {
                break;
            }
            let parsed = match ParsedInternalKey::parse(self.inner.key()) {
                Ok(p) => p,
                Err(_) => break,
            };
            if parsed.user_key != cur_key.as_slice() {
                break;
            }
        }
    }
}

impl<'a> UserKeyIter for MemtableUserKeyIter<'a> {
    fn valid(&self) -> bool {
        self.valid
    }
    fn key(&self) -> &[u8] {
        &self.cur_key
    }
    fn value(&self) -> &[u8] {
        &self.cur_value
    }
    fn seek_to_first(&mut self) {
        self.inner.seek_to_first();
        self.read_current();
    }
    fn seek(&mut self, target: &[u8]) {
        // Seek to `read_seq` — entries at higher seqs (not visible
        // to this snapshot) sort *earlier* in the internal-key
        // order, so seeking at `read_seq` lands us on or just
        // before the first visible version at this user key.
        // `read_current` then skips forward past any invisible
        // versions.
        let lookup = crate::db::dbformat::LookupKey::new(target, self.read_seq);
        self.inner.seek(lookup.internal_key());
        self.read_current();
    }
    fn next(&mut self) {
        if !self.valid {
            return;
        }
        self.skip_duplicates();
        self.read_current();
    }
}

// ---------------------------------------------------------------------------
// SstUserKeyIter — adapter from SstIter to UserKeyIter
// ---------------------------------------------------------------------------

/// Wraps an [`SstIter`] so the merging iterator can consume it.
///
/// # Layer 4d: internal-key SSTs
///
/// Starting at Layer 4d, SSTs store **internal keys** in
/// `(user_key asc, seq desc, type desc)` order. This wrapper
/// parses each entry's internal key, decodes the user key + value
/// type, and presents only the **newest** version per user key —
/// mirroring what `MemtableUserKeyIter` does for the memtable.
/// Tombstones (Deletion / SingleDeletion) are surfaced with an
/// empty value, matching the engine convention.
pub struct SstUserKeyIter<'a> {
    inner: SstIter<'a>,
    /// Decoded current user key (parsed from the SST's internal key).
    cur_key: Vec<u8>,
    /// Decoded current value (empty for tombstones).
    cur_value: Vec<u8>,
    /// True iff `cur_key`/`cur_value` are populated.
    valid: bool,
    /// Snapshot sequence — see [`MemtableUserKeyIter::new_at`].
    read_seq: SequenceNumber,
}

impl<'a> SstUserKeyIter<'a> {
    /// Wrap an existing SST iterator at the latest sequence.
    pub fn new(inner: SstIter<'a>) -> Self {
        Self::new_at(inner, READ_AT_LATEST)
    }

    /// Wrap an SST iterator that reads at a specific snapshot
    /// sequence. Entries with `seq > read_seq` are skipped.
    pub fn new_at(inner: SstIter<'a>, read_seq: SequenceNumber) -> Self {
        Self {
            inner,
            cur_key: Vec::new(),
            cur_value: Vec::new(),
            valid: false,
            read_seq,
        }
    }

    /// Skip invisible entries (seq > read_seq) and surface the
    /// first visible one. Identical in shape to
    /// [`MemtableUserKeyIter::read_current`].
    fn read_current(&mut self) {
        while self.inner.valid() {
            let parsed = match ParsedInternalKey::parse(self.inner.key()) {
                Ok(p) => p,
                Err(_) => {
                    self.valid = false;
                    return;
                }
            };
            if parsed.sequence > self.read_seq {
                self.inner.next();
                continue;
            }
            self.cur_key.clear();
            self.cur_key.extend_from_slice(parsed.user_key);
            self.cur_value.clear();
            match parsed.value_type {
                ValueType::Value | ValueType::Merge => {
                    self.cur_value.extend_from_slice(self.inner.value());
                }
                _ => {
                    // Deletion / SingleDeletion / RangeDeletion / blob /
                    // wide-column → empty value (tombstone).
                }
            }
            self.valid = true;
            return;
        }
        self.valid = false;
        self.cur_key.clear();
        self.cur_value.clear();
    }

    /// Skip past every additional entry in the SST that shares
    /// the current user key. The first one we landed on (newest,
    /// thanks to internal-key ordering) is what we surface; the
    /// rest are older versions that must be hidden from the
    /// merging iterator.
    fn skip_duplicates(&mut self) {
        let cur_key = self.cur_key.clone();
        loop {
            self.inner.next();
            if !self.inner.valid() {
                break;
            }
            let parsed = match ParsedInternalKey::parse(self.inner.key()) {
                Ok(p) => p,
                Err(_) => break,
            };
            if parsed.user_key != cur_key.as_slice() {
                break;
            }
        }
    }
}

impl<'a> UserKeyIter for SstUserKeyIter<'a> {
    fn valid(&self) -> bool {
        self.valid
    }
    fn key(&self) -> &[u8] {
        &self.cur_key
    }
    fn value(&self) -> &[u8] {
        &self.cur_value
    }
    fn seek_to_first(&mut self) {
        self.inner.seek_to_first();
        self.read_current();
    }
    fn seek(&mut self, target: &[u8]) {
        // Seek at our snapshot sequence.
        let lookup = crate::db::dbformat::LookupKey::new(target, self.read_seq);
        self.inner.seek(lookup.internal_key());
        self.read_current();
    }
    fn next(&mut self) {
        if !self.valid {
            return;
        }
        self.skip_duplicates();
        self.read_current();
    }
    fn status(&self) -> Result<()> {
        self.inner.status()
    }
}

// ---------------------------------------------------------------------------
// DbIterator — eager, vec-backed
// ---------------------------------------------------------------------------

/// User-facing forward + backward iterator over the engine.
/// Tombstones are skipped at materialisation time; the user only
/// sees live `(key, value)` pairs in sorted order.
///
/// Returned by [`crate::db::db_impl::DbImpl::iter`]. Owns its own
/// snapshot of the engine state, so it survives concurrent writes
/// to the engine without interference.
pub struct DbIterator {
    items: Vec<(Vec<u8>, Vec<u8>)>,
    /// Current position. `None` means "invalid"; `Some(i)` means
    /// `items[i]` is the current entry.
    pos: Option<usize>,
}

impl DbIterator {
    /// Create an empty iterator. Used when the engine has no
    /// data; the test path uses [`Self::from_snapshot`].
    pub fn empty() -> Self {
        Self {
            items: Vec::new(),
            pos: None,
        }
    }

    /// Build a `DbIterator` by eagerly merging the memtable + a
    /// snapshot of SST readers. The merge proceeds in
    /// "newest-first" priority order: the memtable wins ties
    /// against any SST, and earlier SSTs in the list win against
    /// later ones (call sites should pass SSTs newest-first).
    /// Tombstones — entries with an empty value — are filtered
    /// out before the iterator is returned.
    pub fn from_snapshot(
        memtable: &MemTable,
        ssts: &[Arc<BlockBasedTableReader>],
    ) -> Result<Self> {
        Self::from_snapshot_with_immutable(memtable, None, ssts)
    }

    /// Same as [`Self::from_snapshot`] but also includes an
    /// **immutable memtable** (a memtable that has been frozen
    /// by a flush job but not yet turned into an SST). Reads
    /// at the latest sequence (`READ_AT_LATEST`).
    ///
    /// Priority order (first wins):
    ///
    /// 1. Active memtable (newest)
    /// 2. Immutable memtable (in-progress flush)
    /// 3. SSTs in the order passed (newest-first by convention)
    pub fn from_snapshot_with_immutable(
        memtable: &MemTable,
        immutable: Option<&MemTable>,
        ssts: &[Arc<BlockBasedTableReader>],
    ) -> Result<Self> {
        Self::from_snapshot_with_immutable_at(memtable, immutable, ssts, READ_AT_LATEST)
    }

    /// Same as [`Self::from_snapshot_with_immutable`] but reads
    /// at a specific snapshot sequence. Entries with
    /// `seq > read_seq` are filtered out by the adapters, so the
    /// resulting iterator only sees user keys that existed at
    /// the time the snapshot was taken.
    pub fn from_snapshot_with_immutable_at(
        memtable: &MemTable,
        immutable: Option<&MemTable>,
        ssts: &[Arc<BlockBasedTableReader>],
        read_seq: SequenceNumber,
    ) -> Result<Self> {
        // BTreeMap collapses duplicates by user key. We insert
        // the active memtable first; later sources only insert
        // if the key isn't already present, so the first source
        // wins on ties.
        let mut map: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

        // 1. Active memtable.
        let mut mt = MemtableUserKeyIter::new_at(memtable, read_seq);
        mt.seek_to_first();
        while mt.valid() {
            let key = mt.key().to_vec();
            if mt.is_tombstone() {
                map.insert(key, Vec::new()); // sentinel
            } else {
                map.insert(key, mt.value().to_vec());
            }
            mt.next();
        }

        // 2. Immutable memtable (only inserts not already present).
        if let Some(imm) = immutable {
            let mut it = MemtableUserKeyIter::new_at(imm, read_seq);
            it.seek_to_first();
            while it.valid() {
                let key = it.key().to_vec();
                map.entry(key).or_insert_with(|| {
                    if it.is_tombstone() {
                        Vec::new()
                    } else {
                        it.value().to_vec()
                    }
                });
                it.next();
            }
        }

        // 3. SSTs in newest-first order. We go through
        //    `SstUserKeyIter::new_at` so internal-key SSTs are
        //    filtered by the snapshot sequence, then parsed and
        //    deduplicated per user key.
        for table in ssts {
            let mut it = SstUserKeyIter::new_at(table.iter(), read_seq);
            it.seek_to_first();
            while it.valid() {
                let key = it.key().to_vec();
                map.entry(key).or_insert_with(|| {
                    if it.is_tombstone() {
                        Vec::new()
                    } else {
                        it.value().to_vec()
                    }
                });
                it.next();
            }
            it.status()?;
        }

        // 4. Filter tombstones (empty values).
        let items: Vec<(Vec<u8>, Vec<u8>)> = map
            .into_iter()
            .filter(|(_, v)| !v.is_empty())
            .collect();

        Ok(Self { items, pos: None })
    }

    /// Number of live entries materialised.
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Whether the materialised snapshot is empty.
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Is the iterator positioned on a live entry?
    pub fn valid(&self) -> bool {
        self.pos.is_some()
    }

    /// Current key. Requires [`Self::valid`].
    pub fn key(&self) -> &[u8] {
        let i = self.pos.expect("DbIterator::key on invalid iter");
        &self.items[i].0
    }

    /// Current value. Requires [`Self::valid`].
    pub fn value(&self) -> &[u8] {
        let i = self.pos.expect("DbIterator::value on invalid iter");
        &self.items[i].1
    }

    /// Position at the first live key in the snapshot.
    pub fn seek_to_first(&mut self) {
        self.pos = if self.items.is_empty() { None } else { Some(0) };
    }

    /// Position at the last live key in the snapshot.
    pub fn seek_to_last(&mut self) {
        self.pos = if self.items.is_empty() {
            None
        } else {
            Some(self.items.len() - 1)
        };
    }

    /// Position at the first key `>= target`.
    pub fn seek(&mut self, target: &[u8]) {
        let idx = self
            .items
            .partition_point(|(k, _)| k.as_slice() < target);
        self.pos = if idx >= self.items.len() {
            None
        } else {
            Some(idx)
        };
    }

    /// Position at the last key `<= target`.
    pub fn seek_for_prev(&mut self, target: &[u8]) {
        let idx = self
            .items
            .partition_point(|(k, _)| k.as_slice() <= target);
        self.pos = if idx == 0 { None } else { Some(idx - 1) };
    }

    /// Advance to the next live key.
    pub fn next(&mut self) {
        if let Some(i) = self.pos {
            let next = i + 1;
            self.pos = if next >= self.items.len() {
                None
            } else {
                Some(next)
            };
        }
    }

    /// Step back to the previous live key.
    pub fn prev(&mut self) {
        if let Some(i) = self.pos {
            self.pos = if i == 0 { None } else { Some(i - 1) };
        }
    }
}

/// Helper: build a merging iterator over a memtable + a snapshot
/// of live SSTs. Used internally by compaction. The public read
/// path goes through [`DbIterator::from_snapshot`] which has no
/// lifetime constraints.
pub fn build_merging_iter<'a>(
    memtable: &'a MemTable,
    ssts: &'a [Arc<BlockBasedTableReader>],
) -> MergingIterator<'a> {
    let mut sources: Vec<Box<dyn UserKeyIter + 'a>> = Vec::with_capacity(ssts.len() + 1);
    sources.push(Box::new(MemtableUserKeyIter::new(memtable)));
    for table in ssts {
        sources.push(Box::new(SstUserKeyIter::new(table.iter())));
    }
    MergingIterator::new(sources)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ext::comparator::BytewiseComparator;

    fn empty_ssts() -> Vec<Arc<BlockBasedTableReader>> {
        Vec::new()
    }

    #[test]
    fn empty_iterator() {
        let mt = MemTable::new(Arc::new(BytewiseComparator));
        let it = DbIterator::from_snapshot(&mt, &empty_ssts()).unwrap();
        assert!(it.is_empty());
        assert!(!it.valid());
    }

    #[test]
    fn iterator_walks_memtable_entries() {
        let mut mt = MemTable::new(Arc::new(BytewiseComparator));
        mt.add(1, ValueType::Value, b"a", b"1").unwrap();
        mt.add(2, ValueType::Value, b"b", b"2").unwrap();
        mt.add(3, ValueType::Value, b"c", b"3").unwrap();
        let mut it = DbIterator::from_snapshot(&mt, &empty_ssts()).unwrap();
        it.seek_to_first();
        let mut got: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        while it.valid() {
            got.push((it.key().to_vec(), it.value().to_vec()));
            it.next();
        }
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].0, b"a");
        assert_eq!(got[2].0, b"c");
    }

    #[test]
    fn iterator_skips_tombstones() {
        let mut mt = MemTable::new(Arc::new(BytewiseComparator));
        mt.add(1, ValueType::Value, b"a", b"1").unwrap();
        mt.add(2, ValueType::Deletion, b"a", b"").unwrap();
        mt.add(3, ValueType::Value, b"b", b"2").unwrap();
        let mut it = DbIterator::from_snapshot(&mt, &empty_ssts()).unwrap();
        it.seek_to_first();
        // `a` was deleted; we should only see `b`.
        assert!(it.valid());
        assert_eq!(it.key(), b"b");
        it.next();
        assert!(!it.valid());
    }

    #[test]
    fn newer_memtable_versions_shadow_older() {
        let mut mt = MemTable::new(Arc::new(BytewiseComparator));
        mt.add(1, ValueType::Value, b"k", b"old").unwrap();
        mt.add(2, ValueType::Value, b"k", b"new").unwrap();
        let mut it = DbIterator::from_snapshot(&mt, &empty_ssts()).unwrap();
        it.seek_to_first();
        assert!(it.valid());
        assert_eq!(it.value(), b"new");
        it.next();
        assert!(!it.valid());
    }

    #[test]
    fn seek_lands_at_or_after_target() {
        let mut mt = MemTable::new(Arc::new(BytewiseComparator));
        for &k in &[b"a", b"c", b"e", b"g"] {
            mt.add(1, ValueType::Value, k, b"v").unwrap();
        }
        let mut it = DbIterator::from_snapshot(&mt, &empty_ssts()).unwrap();
        it.seek(b"c");
        assert_eq!(it.key(), b"c");
        it.seek(b"d");
        assert_eq!(it.key(), b"e");
        it.seek(b"z");
        assert!(!it.valid());
    }

    #[test]
    fn seek_for_prev_works() {
        let mut mt = MemTable::new(Arc::new(BytewiseComparator));
        for &k in &[b"a", b"c", b"e"] {
            mt.add(1, ValueType::Value, k, b"v").unwrap();
        }
        let mut it = DbIterator::from_snapshot(&mt, &empty_ssts()).unwrap();
        it.seek_for_prev(b"d");
        assert_eq!(it.key(), b"c");
        it.seek_for_prev(b"a");
        assert_eq!(it.key(), b"a");
        it.seek_for_prev(b" ");
        assert!(!it.valid());
    }

    #[test]
    fn backward_iteration() {
        let mut mt = MemTable::new(Arc::new(BytewiseComparator));
        for (i, k) in [b"a" as &[u8], b"b", b"c"].iter().enumerate() {
            mt.add(i as u64 + 1, ValueType::Value, k, b"v").unwrap();
        }
        let mut it = DbIterator::from_snapshot(&mt, &empty_ssts()).unwrap();
        it.seek_to_last();
        assert_eq!(it.key(), b"c");
        it.prev();
        assert_eq!(it.key(), b"b");
        it.prev();
        assert_eq!(it.key(), b"a");
        it.prev();
        assert!(!it.valid());
    }

    #[test]
    fn db_iter_seek_to_last_returns_last_user_key() {
        let mut mt = MemTable::new(Arc::new(BytewiseComparator));
        mt.add(1, ValueType::Value, b"alpha", b"v1").unwrap();
        mt.add(2, ValueType::Value, b"beta", b"v2").unwrap();
        mt.add(3, ValueType::Value, b"delta", b"v3").unwrap();
        mt.add(4, ValueType::Value, b"gamma", b"v4").unwrap();
        let mut it = DbIterator::from_snapshot(&mt, &empty_ssts()).unwrap();
        it.seek_to_last();
        assert!(it.valid());
        assert_eq!(it.key(), b"gamma"); // lexicographically last
        assert_eq!(it.value(), b"v4");
    }

    #[test]
    fn db_iter_prev_walks_backward_through_all() {
        let mut mt = MemTable::new(Arc::new(BytewiseComparator));
        for (i, k) in ["a", "b", "c", "d", "e"].iter().enumerate() {
            mt.add(i as u64 + 1, ValueType::Value, k.as_bytes(), b"v")
                .unwrap();
        }
        let mut it = DbIterator::from_snapshot(&mt, &empty_ssts()).unwrap();
        it.seek_to_last();
        let mut backward_keys = Vec::new();
        while it.valid() {
            backward_keys.push(it.key().to_vec());
            it.prev();
        }
        assert_eq!(
            backward_keys,
            vec![
                b"e".to_vec(),
                b"d".to_vec(),
                b"c".to_vec(),
                b"b".to_vec(),
                b"a".to_vec(),
            ]
        );
    }

    #[test]
    fn db_iter_seek_for_prev_between_entries() {
        let mut mt = MemTable::new(Arc::new(BytewiseComparator));
        mt.add(1, ValueType::Value, b"aa", b"1").unwrap();
        mt.add(2, ValueType::Value, b"cc", b"3").unwrap();
        mt.add(3, ValueType::Value, b"ee", b"5").unwrap();
        mt.add(4, ValueType::Value, b"gg", b"7").unwrap();
        let mut it = DbIterator::from_snapshot(&mt, &empty_ssts()).unwrap();
        // seek_for_prev("dd") should land on "cc" (last key <= "dd")
        it.seek_for_prev(b"dd");
        assert!(it.valid());
        assert_eq!(it.key(), b"cc");
        assert_eq!(it.value(), b"3");
        // seek_for_prev at exact key should land on that key
        it.seek_for_prev(b"ee");
        assert!(it.valid());
        assert_eq!(it.key(), b"ee");
        assert_eq!(it.value(), b"5");
        // seek_for_prev before all keys -> invalid
        it.seek_for_prev(b"a");
        assert!(!it.valid());
    }

    #[test]
    fn db_iter_seek_to_last_on_empty() {
        let mt = MemTable::new(Arc::new(BytewiseComparator));
        let mut it = DbIterator::from_snapshot(&mt, &empty_ssts()).unwrap();
        it.seek_to_last();
        assert!(!it.valid());
    }

    #[test]
    fn db_iter_prev_at_first_invalidates() {
        let mut mt = MemTable::new(Arc::new(BytewiseComparator));
        mt.add(1, ValueType::Value, b"only", b"one").unwrap();
        let mut it = DbIterator::from_snapshot(&mt, &empty_ssts()).unwrap();
        it.seek_to_first();
        assert!(it.valid());
        assert_eq!(it.key(), b"only");
        it.prev();
        assert!(!it.valid());
    }
}
