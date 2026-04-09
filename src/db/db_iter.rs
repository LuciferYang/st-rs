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
use crate::core::types::ValueType;
use crate::db::dbformat::ParsedInternalKey;
use crate::db::memtable::MemTable;
use crate::db::merging_iterator::{MergingIterator, UserKeyIter};
use crate::memtable::skip_list::SkipListIter;
use crate::sst::block_based::sst_iterator::SstIter;
use crate::sst::block_based::table_reader::BlockBasedTableReader;
use std::collections::BTreeMap;
use std::sync::Arc;

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
}

impl<'a> MemtableUserKeyIter<'a> {
    /// Build a new wrapper. Caller must call `seek_to_first` or
    /// `seek` before reading.
    pub fn new(memtable: &'a MemTable) -> Self {
        Self {
            inner: memtable.iter(),
            cur_key: Vec::new(),
            cur_value: Vec::new(),
            valid: false,
        }
    }

    fn read_current(&mut self) {
        if !self.inner.valid() {
            self.valid = false;
            self.cur_key.clear();
            self.cur_value.clear();
            return;
        }
        let parsed = match ParsedInternalKey::parse(self.inner.key()) {
            Ok(p) => p,
            Err(_) => {
                self.valid = false;
                return;
            }
        };
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
    }

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
        // Seek to the largest possible sequence at this user key
        // — that's the newest version, which the internal-key
        // ordering places first.
        let lookup = crate::db::dbformat::LookupKey::new(target, u64::MAX >> 8);
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
/// SSTs already store user keys (the Layer 4a flush path collapses
/// the memtable's internal-key view to one user-key entry per key
/// before writing), so this is mostly a passthrough.
pub struct SstUserKeyIter<'a> {
    inner: SstIter<'a>,
}

impl<'a> SstUserKeyIter<'a> {
    /// Wrap an existing SST iterator.
    pub fn new(inner: SstIter<'a>) -> Self {
        Self { inner }
    }
}

impl<'a> UserKeyIter for SstUserKeyIter<'a> {
    fn valid(&self) -> bool {
        self.inner.valid()
    }
    fn key(&self) -> &[u8] {
        self.inner.key()
    }
    fn value(&self) -> &[u8] {
        self.inner.value()
    }
    fn seek_to_first(&mut self) {
        self.inner.seek_to_first();
    }
    fn seek(&mut self, target: &[u8]) {
        self.inner.seek(target);
    }
    fn next(&mut self) {
        self.inner.next();
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
        // BTreeMap collapses duplicates by user key. Memtable
        // entries get inserted first; SST entries are only
        // inserted if the key isn't already present, so the
        // first source (memtable, then newest SST, …) wins.
        let mut map: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

        // 1. Memtable.
        let mut mt = MemtableUserKeyIter::new(memtable);
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

        // 2. SSTs in newest-first order.
        for table in ssts {
            let mut it = table.iter();
            it.seek_to_first();
            while it.valid() {
                let key = it.key();
                if !map.contains_key(key) {
                    if it.value().is_empty() {
                        map.insert(key.to_vec(), Vec::new());
                    } else {
                        map.insert(key.to_vec(), it.value().to_vec());
                    }
                }
                it.next();
            }
            it.status()?;
        }

        // 3. Filter tombstones (empty values).
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
}
