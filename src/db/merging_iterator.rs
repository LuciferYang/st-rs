//! Port of `table/merging_iterator.{h,cc}` simplified for Layer 4b.
//!
//! K-way merge over a set of forward sorted user-key sources. Used
//! by the engine in two places:
//!
//! 1. **Read path** — `DbIterator` merges the memtable and every
//!    live SST into a single sorted stream.
//! 2. **Compaction** — merges the input SSTs of a compaction job.
//!
//! # Source contract
//!
//! Every input source implements [`UserKeyIter`]: forward-only,
//! sorted by user key, with `is_tombstone` reporting whether the
//! current entry is a deletion (signified by an empty value
//! payload — the convention used by Layer 4a's flush path).
//!
//! # Merge semantics
//!
//! When two or more sources have the same user key, the **first**
//! one (lowest index in the input vector) wins. The other matching
//! sources are advanced past the duplicate. This is how the read
//! path can express "memtable shadows SSTs" and "newer SSTs shadow
//! older ones": the caller passes the memtable first, then SSTs in
//! newest-first order.
//!
//! # Algorithm
//!
//! Naive linear-scan k-way merge: every `next` call walks all
//! sources to find the smallest current key. O(k) per step. For
//! `k <= 32` this is competitive with a heap on real workloads
//! and avoids the complexity of a custom heap-with-comparator.
//! A future Layer 4c can swap in `util::heap` for hot scans.

use crate::core::status::Result;
use std::cmp::Ordering;

/// Forward iterator over a sorted (key, value) source. Implementors:
///
/// - The memtable wrapped to dedup by user key
/// - An [`crate::sst::block_based::sst_iterator::SstIter`]
///
/// All keys must be **user keys** — there is no internal-key
/// awareness in this layer. The merging iterator orders strictly
/// by `Vec<u8>` lexicographic comparison; the caller is responsible
/// for choosing a comparator-compatible byte representation.
pub trait UserKeyIter {
    /// Is the iterator currently positioned on an entry?
    fn valid(&self) -> bool;

    /// Current key. Requires [`Self::valid`].
    fn key(&self) -> &[u8];

    /// Current value. Requires [`Self::valid`]. An empty slice
    /// indicates a tombstone (deletion).
    fn value(&self) -> &[u8];

    /// Whether the current entry is a tombstone. Default checks
    /// for an empty value, which matches the engine's convention.
    fn is_tombstone(&self) -> bool {
        self.value().is_empty()
    }

    /// Position at the first key in the source.
    fn seek_to_first(&mut self);

    /// Position at the first key `>= target`.
    fn seek(&mut self, target: &[u8]);

    /// Advance to the next entry.
    fn next(&mut self);

    /// Sticky status; defaults to OK.
    fn status(&self) -> Result<()> {
        Ok(())
    }
}

/// K-way merging iterator over a set of [`UserKeyIter`] sources.
///
/// Borrow lifetimes: the iterator owns its sources by `Box<dyn>`,
/// so each source can carry whatever state it needs (a borrow into
/// a memtable, an `Arc<BlockBasedTableReader>`, …).
pub struct MergingIterator<'a> {
    sources: Vec<Box<dyn UserKeyIter + 'a>>,
    /// Index of the source currently winning the merge, or `None`
    /// if no source is positioned on a key.
    current: Option<usize>,
}

impl<'a> MergingIterator<'a> {
    /// Build a merging iterator over `sources`. The vector order is
    /// the **priority order** for tie-breaking — the source at
    /// index 0 wins ties. Pass the memtable first, then SSTs in
    /// newest-first order.
    pub fn new(sources: Vec<Box<dyn UserKeyIter + 'a>>) -> Self {
        Self {
            sources,
            current: None,
        }
    }

    /// Is the iterator currently positioned?
    pub fn valid(&self) -> bool {
        self.current.is_some()
    }

    /// Current key.
    pub fn key(&self) -> &[u8] {
        let idx = self.current.expect("MergingIterator::key on invalid iter");
        self.sources[idx].key()
    }

    /// Current value (empty for tombstones).
    pub fn value(&self) -> &[u8] {
        let idx = self
            .current
            .expect("MergingIterator::value on invalid iter");
        self.sources[idx].value()
    }

    /// Whether the current entry is a tombstone.
    pub fn is_tombstone(&self) -> bool {
        let idx = self
            .current
            .expect("MergingIterator::is_tombstone on invalid iter");
        self.sources[idx].is_tombstone()
    }

    /// Sticky status — surfaces the first error from any source.
    pub fn status(&self) -> Result<()> {
        for s in &self.sources {
            s.status()?;
        }
        Ok(())
    }

    /// Position every source at its first entry, then pick the
    /// smallest as `current`.
    pub fn seek_to_first(&mut self) {
        for s in self.sources.iter_mut() {
            s.seek_to_first();
        }
        self.find_smallest();
    }

    /// Position every source at the first entry `>= target`, then
    /// pick the smallest.
    pub fn seek(&mut self, target: &[u8]) {
        for s in self.sources.iter_mut() {
            s.seek(target);
        }
        self.find_smallest();
    }

    /// Advance the merging iterator. The current source is
    /// advanced; any other source whose current key matches the
    /// previous output is also advanced (so duplicates are
    /// suppressed). Then we re-pick the smallest.
    pub fn next(&mut self) {
        let cur = match self.current {
            Some(c) => c,
            None => return,
        };
        // Snapshot the current key so we can compare against it
        // after we advance the winning source.
        let last_key = self.sources[cur].key().to_vec();
        self.sources[cur].next();
        // Drop equal keys from later sources too — first source wins.
        for (i, src) in self.sources.iter_mut().enumerate() {
            if i == cur {
                continue;
            }
            if src.valid() && src.key() == last_key.as_slice() {
                src.next();
            }
        }
        self.find_smallest();
    }

    /// Re-pick `self.current` as the source with the smallest valid
    /// key. Lexicographic comparison.
    fn find_smallest(&mut self) {
        let mut best: Option<(usize, &[u8])> = None;
        for (i, src) in self.sources.iter().enumerate() {
            if !src.valid() {
                continue;
            }
            let key = src.key();
            best = match best {
                None => Some((i, key)),
                Some((_, current_best)) => match key.cmp(current_best) {
                    Ordering::Less => Some((i, key)),
                    // Tie: keep the earlier source (lower index).
                    _ => best,
                },
            };
        }
        self.current = best.map(|(i, _)| i);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Trivial Vec-backed UserKeyIter for tests.
    struct VecIter {
        items: Vec<(Vec<u8>, Vec<u8>)>,
        pos: usize,
    }

    impl VecIter {
        fn new(items: Vec<(&[u8], &[u8])>) -> Self {
            Self {
                items: items
                    .into_iter()
                    .map(|(k, v)| (k.to_vec(), v.to_vec()))
                    .collect(),
                pos: usize::MAX,
            }
        }
    }

    impl UserKeyIter for VecIter {
        fn valid(&self) -> bool {
            self.pos < self.items.len()
        }
        fn key(&self) -> &[u8] {
            &self.items[self.pos].0
        }
        fn value(&self) -> &[u8] {
            &self.items[self.pos].1
        }
        fn seek_to_first(&mut self) {
            self.pos = if self.items.is_empty() { usize::MAX } else { 0 };
        }
        fn seek(&mut self, target: &[u8]) {
            self.pos = self
                .items
                .iter()
                .position(|(k, _)| k.as_slice() >= target)
                .unwrap_or(usize::MAX);
        }
        fn next(&mut self) {
            if self.pos < self.items.len() {
                self.pos += 1;
                if self.pos == self.items.len() {
                    self.pos = usize::MAX;
                }
            }
        }
    }

    #[test]
    fn empty_merge() {
        let mut m = MergingIterator::new(vec![]);
        m.seek_to_first();
        assert!(!m.valid());
    }

    #[test]
    fn single_source() {
        let src = VecIter::new(vec![(b"a", b"1"), (b"b", b"2")]);
        let mut m = MergingIterator::new(vec![Box::new(src)]);
        m.seek_to_first();
        assert!(m.valid());
        assert_eq!(m.key(), b"a");
        m.next();
        assert_eq!(m.key(), b"b");
        m.next();
        assert!(!m.valid());
    }

    #[test]
    fn two_sources_interleaved() {
        let s1 = VecIter::new(vec![(b"a", b"1"), (b"c", b"3"), (b"e", b"5")]);
        let s2 = VecIter::new(vec![(b"b", b"2"), (b"d", b"4"), (b"f", b"6")]);
        let mut m = MergingIterator::new(vec![Box::new(s1), Box::new(s2)]);
        m.seek_to_first();
        let mut got = Vec::new();
        while m.valid() {
            got.push((m.key().to_vec(), m.value().to_vec()));
            m.next();
        }
        assert_eq!(
            got,
            vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec()),
                (b"c".to_vec(), b"3".to_vec()),
                (b"d".to_vec(), b"4".to_vec()),
                (b"e".to_vec(), b"5".to_vec()),
                (b"f".to_vec(), b"6".to_vec()),
            ]
        );
    }

    #[test]
    fn first_source_wins_on_tie() {
        // Both sources have key "k"; the first one (with value "new")
        // should win and the second is silently dropped.
        let s1 = VecIter::new(vec![(b"k", b"new")]);
        let s2 = VecIter::new(vec![(b"k", b"old")]);
        let mut m = MergingIterator::new(vec![Box::new(s1), Box::new(s2)]);
        m.seek_to_first();
        assert!(m.valid());
        assert_eq!(m.key(), b"k");
        assert_eq!(m.value(), b"new");
        m.next();
        assert!(!m.valid());
    }

    #[test]
    fn tombstones_are_visible() {
        let s = VecIter::new(vec![(b"k", b"")]);
        let mut m = MergingIterator::new(vec![Box::new(s)]);
        m.seek_to_first();
        assert!(m.valid());
        assert!(m.is_tombstone());
    }

    #[test]
    fn seek_advances_all_sources() {
        let s1 = VecIter::new(vec![(b"a", b"1"), (b"c", b"3")]);
        let s2 = VecIter::new(vec![(b"b", b"2"), (b"d", b"4")]);
        let mut m = MergingIterator::new(vec![Box::new(s1), Box::new(s2)]);
        m.seek(b"c");
        assert!(m.valid());
        assert_eq!(m.key(), b"c");
        m.next();
        assert_eq!(m.key(), b"d");
    }
}
