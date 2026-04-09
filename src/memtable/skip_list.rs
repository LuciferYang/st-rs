//! Port of `memtable/inlineskiplist.h` — **single-threaded** variant.
//!
//! Upstream `InlineSkipList` is a lock-free concurrent skiplist with
//! atomic pointers and careful memory ordering. Writing a correct
//! concurrent lock-free skiplist in **safe** Rust is effectively
//! impossible without unsafe aliasing primitives — so this Layer 3a
//! port is **single-threaded only**. It gives you the same O(log n)
//! seek/insert/iterate shape and the same comparator contract, but
//! `&mut self` is required for insertion.
//!
//! A Layer 4 follow-up can add a `ConcurrentSkipList` inside its own
//! `unsafe` module once the engine's concurrent memtable path
//! actually needs it.
//!
//! # Implementation
//!
//! Nodes live in a `Vec<Node>`, linked by `Option<usize>` indices.
//! This is the same "generational index" pattern used for the LRU
//! cache in Layer 2, and for the same reason: it lets us build a
//! classic multi-level pointer data structure without `Rc<RefCell>`
//! or raw pointers.
//!
//! The head is always at index `0` and is a sentinel with
//! [`MAX_HEIGHT`] next-pointers, all initially `None`. `current_height`
//! tracks the maximum level currently in use so that searches never
//! bother with higher-but-empty levels.
//!
//! # Ordering
//!
//! The skiplist is generic over a comparator supplied at
//! construction. The memtable passes an
//! [`crate::db::dbformat::InternalKeyComparator`] here, so the
//! skiplist's sorted order *is* the internal-key sorted order.
//!
//! # Removal
//!
//! Memtables are immutable after they become full — they are either
//! flushed or dropped wholesale. The skiplist therefore exposes no
//! `remove` method; if you need it, drop the list and rebuild.

use crate::ext::comparator::Comparator;
use crate::util::random::Random;
use std::cmp::Ordering;
use std::sync::Arc;

/// Maximum skiplist level. 12 supports ~`BRANCHING_FACTOR^12 =
/// 16_777_216` entries with near-optimal search cost. Matches
/// upstream `kMaxHeight`.
pub const MAX_HEIGHT: usize = 12;

/// Branching factor for random level selection. A new entry is
/// promoted one level with probability `1 / BRANCHING_FACTOR`.
/// Matches upstream `kBranching`.
pub const BRANCHING_FACTOR: u32 = 4;

type NodeIndex = usize;

/// One skiplist node: an owned key-value pair plus a per-level next
/// pointer array. The length of `next` is the node's *height*.
struct Node {
    key: Vec<u8>,
    value: Vec<u8>,
    /// `next[i]` is the successor at level `i`. `None` means "end of
    /// list at this level."
    next: Vec<Option<NodeIndex>>,
}

/// Safe single-threaded skiplist ordered by a comparator trait object.
pub struct SkipList {
    /// All nodes. Index `0` is the head sentinel with `MAX_HEIGHT`
    /// next pointers. Subsequent indices are the real nodes, in
    /// insertion order.
    nodes: Vec<Node>,
    /// Highest level that is actually in use. Walks start here.
    current_height: usize,
    /// PRNG used to pick node heights. Deterministic across runs
    /// (seeded from a fixed constant) so that tests are reproducible.
    rand: Random,
    /// Comparator over the stored keys.
    comparator: Arc<dyn Comparator>,
    /// Number of stored keys. We track it separately because `nodes`
    /// also includes the head sentinel.
    num_entries: usize,
    /// Running total of bytes stored in keys + values + per-node
    /// fixed overhead. Used for `approximate_memory_usage`.
    memory_usage: usize,
}

impl SkipList {
    /// Index of the head sentinel. Always `0`.
    const HEAD: NodeIndex = 0;

    /// Create an empty skiplist ordered by `comparator`.
    pub fn new(comparator: Arc<dyn Comparator>) -> Self {
        let head = Node {
            key: Vec::new(),
            value: Vec::new(),
            next: vec![None; MAX_HEIGHT],
        };
        Self {
            nodes: vec![head],
            current_height: 1,
            rand: Random::new(0xdead_beef),
            comparator,
            num_entries: 0,
            memory_usage: 0,
        }
    }

    /// Number of stored entries (excludes the head sentinel).
    pub fn len(&self) -> usize {
        self.num_entries
    }

    /// Whether the skiplist is empty.
    pub fn is_empty(&self) -> bool {
        self.num_entries == 0
    }

    /// Approximate memory usage. Counts key bytes + value bytes +
    /// a fixed per-node overhead for the next-pointer vector.
    pub fn approximate_memory_usage(&self) -> usize {
        self.memory_usage
    }

    /// Insert a (key, value) pair. If an equal key already exists,
    /// the new entry is inserted *before* it (not replacing it) —
    /// this matches upstream's "allow duplicates" semantics, which
    /// the memtable relies on for MVCC.
    pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) {
        // Compute predecessors at every level so we can splice the
        // new node in. We can't use `find_greater_or_equal` because
        // it returns only the successor; we need the full prev array.
        let mut prev = [Self::HEAD; MAX_HEIGHT];
        self.compute_predecessors(&key, &mut prev);

        // Pick a random height and grow current_height if needed.
        let height = self.random_height();
        if height > self.current_height {
            // New levels have the head as their sole predecessor.
            for pred in prev.iter_mut().take(height).skip(self.current_height) {
                *pred = Self::HEAD;
            }
            self.current_height = height;
        }

        // Create the new node, pointing at the predecessors' successors.
        let mut new_next = Vec::with_capacity(height);
        for (level, &prev_idx) in prev.iter().enumerate().take(height) {
            new_next.push(self.nodes[prev_idx].next[level]);
        }
        // Record sizes before moving the buffers into the node.
        let added_bytes = key.len() + value.len() + std::mem::size_of::<Node>() + height * 8;
        let new_idx = self.nodes.len();
        self.nodes.push(Node {
            key,
            value,
            next: new_next,
        });

        // Splice the new node in at every level up to `height`.
        for (level, &prev_idx) in prev.iter().enumerate().take(height) {
            self.nodes[prev_idx].next[level] = Some(new_idx);
        }

        self.num_entries += 1;
        self.memory_usage += added_bytes;
    }

    /// Pick a random height in `[1, MAX_HEIGHT]`. Each successive
    /// level is half as likely (with `BRANCHING_FACTOR=4`).
    fn random_height(&mut self) -> usize {
        let mut height = 1usize;
        while height < MAX_HEIGHT && self.rand.one_in(BRANCHING_FACTOR) {
            height += 1;
        }
        height
    }

    /// Write the per-level predecessor of `target` into `prev`. At
    /// level `i`, `prev[i]` is the index of the node whose next-at-`i`
    /// is either `None` or points at a node whose key is `>= target`.
    fn compute_predecessors(&self, target: &[u8], prev: &mut [NodeIndex; MAX_HEIGHT]) {
        let mut current = Self::HEAD;
        let mut level = self.current_height - 1;
        loop {
            match self.nodes[current].next[level] {
                Some(next_idx)
                    if self
                        .comparator
                        .cmp(&self.nodes[next_idx].key, target)
                        == Ordering::Less =>
                {
                    current = next_idx;
                }
                _ => {
                    prev[level] = current;
                    if level == 0 {
                        return;
                    }
                    level -= 1;
                }
            }
        }
    }

    /// Find the first node with key `>= target`. Returns `None` if
    /// every key is strictly less.
    fn find_greater_or_equal(&self, target: &[u8]) -> Option<NodeIndex> {
        let mut current = Self::HEAD;
        let mut level = self.current_height - 1;
        loop {
            match self.nodes[current].next[level] {
                Some(next_idx)
                    if self
                        .comparator
                        .cmp(&self.nodes[next_idx].key, target)
                        == Ordering::Less =>
                {
                    current = next_idx;
                }
                other => {
                    if level == 0 {
                        return other;
                    }
                    level -= 1;
                }
            }
        }
    }

    /// Find the last node with key `<= target`. Returns `None` if
    /// the skiplist is empty or every key is strictly greater.
    fn find_less_than(&self, target: &[u8]) -> Option<NodeIndex> {
        let mut current = Self::HEAD;
        let mut level = self.current_height - 1;
        loop {
            while let Some(next_idx) = self.nodes[current].next[level] {
                if self.comparator.cmp(&self.nodes[next_idx].key, target) == Ordering::Less {
                    current = next_idx;
                } else {
                    break;
                }
            }
            if level == 0 {
                return if current == Self::HEAD {
                    None
                } else {
                    Some(current)
                };
            }
            level -= 1;
        }
    }

    /// Find the node with the largest key.
    fn find_last(&self) -> Option<NodeIndex> {
        let mut current = Self::HEAD;
        let mut level = self.current_height - 1;
        loop {
            while let Some(next_idx) = self.nodes[current].next[level] {
                current = next_idx;
            }
            if level == 0 {
                return if current == Self::HEAD {
                    None
                } else {
                    Some(current)
                };
            }
            level -= 1;
        }
    }

    /// Returns `true` if some stored key compares equal to `target`.
    pub fn contains(&self, target: &[u8]) -> bool {
        match self.find_greater_or_equal(target) {
            Some(idx) => self.comparator.cmp(&self.nodes[idx].key, target) == Ordering::Equal,
            None => false,
        }
    }

    /// Create a new iterator. The iterator borrows the skiplist
    /// immutably and remains valid until the borrow ends.
    pub fn iter(&self) -> SkipListIter<'_> {
        SkipListIter {
            list: self,
            current: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Iterator
// ---------------------------------------------------------------------------

/// Forward + backward iterator over a [`SkipList`].
pub struct SkipListIter<'a> {
    list: &'a SkipList,
    current: Option<NodeIndex>,
}

impl<'a> SkipListIter<'a> {
    /// Is the iterator positioned on a real node?
    pub fn valid(&self) -> bool {
        self.current.is_some()
    }

    /// Current key. Requires [`Self::valid`].
    pub fn key(&self) -> &[u8] {
        &self.list.nodes[self.current.expect("key() on invalid iter")].key
    }

    /// Current value. Requires [`Self::valid`].
    pub fn value(&self) -> &[u8] {
        &self.list.nodes[self.current.expect("value() on invalid iter")].value
    }

    /// Position at the first (smallest) key.
    pub fn seek_to_first(&mut self) {
        self.current = self.list.nodes[SkipList::HEAD].next[0];
    }

    /// Position at the last (largest) key.
    pub fn seek_to_last(&mut self) {
        self.current = self.list.find_last();
    }

    /// Position at the first key `>= target`.
    pub fn seek(&mut self, target: &[u8]) {
        self.current = self.list.find_greater_or_equal(target);
    }

    /// Position at the last key `<= target`.
    pub fn seek_for_prev(&mut self, target: &[u8]) {
        // First, find the node at-or-after target.
        let at_or_after = self.list.find_greater_or_equal(target);
        match at_or_after {
            Some(idx)
                if self.list.comparator.cmp(&self.list.nodes[idx].key, target)
                    == Ordering::Equal =>
            {
                // Exact match — stay on it.
                self.current = Some(idx);
            }
            _ => {
                // Strictly less — walk back by one.
                self.current = self.list.find_less_than(target);
            }
        }
    }

    /// Advance to the next key.
    pub fn next(&mut self) {
        if let Some(idx) = self.current {
            self.current = self.list.nodes[idx].next[0];
        }
    }

    /// Retreat to the previous key. O(log n).
    pub fn prev(&mut self) {
        if let Some(idx) = self.current {
            let current_key = self.list.nodes[idx].key.clone();
            self.current = self.list.find_less_than(&current_key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ext::comparator::BytewiseComparator;

    fn list() -> SkipList {
        SkipList::new(Arc::new(BytewiseComparator))
    }

    #[test]
    fn empty_list() {
        let l = list();
        assert!(l.is_empty());
        assert_eq!(l.len(), 0);
        assert!(!l.contains(b"anything"));
        let mut it = l.iter();
        it.seek_to_first();
        assert!(!it.valid());
    }

    #[test]
    fn single_insert_contains() {
        let mut l = list();
        l.insert(b"hello".to_vec(), b"world".to_vec());
        assert!(l.contains(b"hello"));
        assert!(!l.contains(b"missing"));
        assert_eq!(l.len(), 1);
    }

    #[test]
    fn sorted_iteration() {
        let mut l = list();
        for &k in &[b"b", b"a", b"d", b"c"] {
            l.insert(k.to_vec(), b"v".to_vec());
        }
        let mut it = l.iter();
        it.seek_to_first();
        let mut keys = Vec::new();
        while it.valid() {
            keys.push(it.key().to_vec());
            it.next();
        }
        let expected: Vec<Vec<u8>> = [b"a", b"b", b"c", b"d"]
            .iter()
            .map(|k| k.to_vec())
            .collect();
        assert_eq!(keys, expected);
    }

    #[test]
    fn seek_positions_correctly() {
        let mut l = list();
        for k in ["a", "c", "e", "g"] {
            l.insert(k.as_bytes().to_vec(), b"v".to_vec());
        }
        let mut it = l.iter();
        it.seek(b"c");
        assert_eq!(it.key(), b"c");
        it.seek(b"d");
        assert_eq!(it.key(), b"e");
        it.seek(b"z");
        assert!(!it.valid());
    }

    #[test]
    fn seek_to_last() {
        let mut l = list();
        for k in ["a", "b", "c"] {
            l.insert(k.as_bytes().to_vec(), b"v".to_vec());
        }
        let mut it = l.iter();
        it.seek_to_last();
        assert_eq!(it.key(), b"c");
        it.next();
        assert!(!it.valid());
    }

    #[test]
    fn backward_iteration() {
        let mut l = list();
        for k in ["a", "b", "c", "d"] {
            l.insert(k.as_bytes().to_vec(), b"v".to_vec());
        }
        let mut it = l.iter();
        it.seek_to_last();
        let mut keys = Vec::new();
        while it.valid() {
            keys.push(it.key().to_vec());
            it.prev();
        }
        let expected: Vec<Vec<u8>> = [b"d", b"c", b"b", b"a"]
            .iter()
            .map(|k| k.to_vec())
            .collect();
        assert_eq!(keys, expected);
    }

    #[test]
    fn seek_for_prev() {
        let mut l = list();
        for k in ["a", "c", "e"] {
            l.insert(k.as_bytes().to_vec(), b"v".to_vec());
        }
        let mut it = l.iter();
        it.seek_for_prev(b"d");
        assert_eq!(it.key(), b"c");
        it.seek_for_prev(b"c");
        assert_eq!(it.key(), b"c"); // exact match stays put
        it.seek_for_prev(b"a");
        assert_eq!(it.key(), b"a");
        it.seek_for_prev(b" "); // before everything
        assert!(!it.valid());
    }

    #[test]
    fn large_insertion_stays_sorted() {
        let mut l = list();
        // Insert 1000 keys in reverse order.
        for i in (0..1000u32).rev() {
            let k = format!("key{i:04}");
            l.insert(k.into_bytes(), b"v".to_vec());
        }
        assert_eq!(l.len(), 1000);

        // Iterate forwards and verify order.
        let mut it = l.iter();
        it.seek_to_first();
        let mut i = 0u32;
        while it.valid() {
            let expected = format!("key{i:04}");
            assert_eq!(it.key(), expected.as_bytes());
            i += 1;
            it.next();
        }
        assert_eq!(i, 1000);
    }

    #[test]
    fn memory_usage_grows_with_inserts() {
        let mut l = list();
        assert_eq!(l.approximate_memory_usage(), 0);
        l.insert(b"k".to_vec(), b"v".to_vec());
        assert!(l.approximate_memory_usage() > 0);
        let usage1 = l.approximate_memory_usage();
        l.insert(b"k2".to_vec(), b"v2".to_vec());
        assert!(l.approximate_memory_usage() > usage1);
    }
}
