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

//! Port of `util/heap.h`.
//!
//! Generic binary min-heap parameterised over a comparator
//! supplied at construction. Used by k-way merging iterators
//! and any other "find the smallest pending element" code path.
//!
//! # Why a custom heap instead of `std::collections::BinaryHeap`?
//!
//! `std::collections::BinaryHeap<T>` requires `T: Ord`, which
//! means the comparator must be encoded in the type. The engine
//! needs to merge sources by an `Arc<dyn Comparator>` chosen at
//! runtime — that's the whole point of the [`crate::ext::comparator::Comparator`]
//! trait. Wrapping items in a newtype that captures the comparator
//! at construction works but is awkward; this small generic heap
//! is cleaner.
//!
//! # Algorithm
//!
//! Standard array-backed binary heap with sift-up on `push` and
//! sift-down on `pop`. O(log n) per operation, no allocations
//! after the first growth.
//!
//! # Min-heap vs max-heap
//!
//! This is a **min-heap**: `peek` returns the smallest element
//! according to the comparator. The merging iterator wants
//! "smallest current key wins", which is the natural fit. Wrap
//! the comparator in a `|a, b| inner.cmp(b, a)` closure to get
//! a max-heap if you need one.

use std::cmp::Ordering;

/// Type alias for the heap's comparator function.
type CmpFn<T> = Box<dyn Fn(&T, &T) -> Ordering + Send + Sync>;

/// Generic binary min-heap.
pub struct BinaryHeap<T> {
    data: Vec<T>,
    cmp: CmpFn<T>,
}

impl<T> BinaryHeap<T> {
    /// Create an empty heap with the given comparator.
    pub fn new<F>(cmp: F) -> Self
    where
        F: Fn(&T, &T) -> Ordering + Send + Sync + 'static,
    {
        Self {
            data: Vec::new(),
            cmp: Box::new(cmp),
        }
    }

    /// Create an empty heap with pre-allocated capacity.
    pub fn with_capacity<F>(cap: usize, cmp: F) -> Self
    where
        F: Fn(&T, &T) -> Ordering + Send + Sync + 'static,
    {
        Self {
            data: Vec::with_capacity(cap),
            cmp: Box::new(cmp),
        }
    }

    /// Number of elements currently in the heap.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Whether the heap is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Borrow the smallest element without removing it.
    pub fn peek(&self) -> Option<&T> {
        self.data.first()
    }

    /// Insert an element. O(log n).
    pub fn push(&mut self, item: T) {
        self.data.push(item);
        self.sift_up(self.data.len() - 1);
    }

    /// Remove and return the smallest element. O(log n).
    pub fn pop(&mut self) -> Option<T> {
        let last = self.data.pop()?;
        if self.data.is_empty() {
            return Some(last);
        }
        // Swap the last element to the root, return the old root.
        let root = std::mem::replace(&mut self.data[0], last);
        self.sift_down(0);
        Some(root)
    }

    /// Replace the root in O(log n). Equivalent to `pop`+`push`
    /// but only sifts once.
    pub fn replace_root(&mut self, item: T) -> Option<T> {
        if self.data.is_empty() {
            self.data.push(item);
            return None;
        }
        let old = std::mem::replace(&mut self.data[0], item);
        self.sift_down(0);
        Some(old)
    }

    /// Discard every element. O(1) (does not free capacity).
    pub fn clear(&mut self) {
        self.data.clear();
    }

    /// Sift the element at `idx` upward until heap property holds.
    fn sift_up(&mut self, mut idx: usize) {
        while idx > 0 {
            let parent = (idx - 1) / 2;
            if (self.cmp)(&self.data[idx], &self.data[parent]) == Ordering::Less {
                self.data.swap(idx, parent);
                idx = parent;
            } else {
                break;
            }
        }
    }

    /// Sift the element at `idx` downward until heap property holds.
    fn sift_down(&mut self, mut idx: usize) {
        let len = self.data.len();
        loop {
            let left = 2 * idx + 1;
            let right = 2 * idx + 2;
            let mut smallest = idx;
            if left < len && (self.cmp)(&self.data[left], &self.data[smallest]) == Ordering::Less {
                smallest = left;
            }
            if right < len && (self.cmp)(&self.data[right], &self.data[smallest]) == Ordering::Less {
                smallest = right;
            }
            if smallest == idx {
                break;
            }
            self.data.swap(idx, smallest);
            idx = smallest;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn min_u32_heap() -> BinaryHeap<u32> {
        BinaryHeap::new(|a: &u32, b: &u32| a.cmp(b))
    }

    #[test]
    fn empty_heap() {
        let h = min_u32_heap();
        assert!(h.is_empty());
        assert_eq!(h.len(), 0);
        assert!(h.peek().is_none());
    }

    #[test]
    fn push_pop_yields_sorted_order() {
        let mut h = min_u32_heap();
        let input = [5u32, 3, 8, 1, 9, 2, 7, 4, 6];
        for &v in &input {
            h.push(v);
        }
        let mut out = Vec::new();
        while let Some(v) = h.pop() {
            out.push(v);
        }
        assert_eq!(out, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    #[test]
    fn peek_returns_smallest() {
        let mut h = min_u32_heap();
        h.push(5);
        assert_eq!(h.peek(), Some(&5));
        h.push(2);
        assert_eq!(h.peek(), Some(&2));
        h.push(7);
        assert_eq!(h.peek(), Some(&2));
    }

    #[test]
    fn replace_root_keeps_heap_invariant() {
        let mut h = min_u32_heap();
        for v in [1, 3, 5, 7, 9] {
            h.push(v);
        }
        // Replace the root (1) with 6 — should bubble down to its
        // correct position and the new root should be 3.
        let old = h.replace_root(6);
        assert_eq!(old, Some(1));
        assert_eq!(h.peek(), Some(&3));
        let mut out = Vec::new();
        while let Some(v) = h.pop() {
            out.push(v);
        }
        assert_eq!(out, vec![3, 5, 6, 7, 9]);
    }

    #[test]
    fn replace_root_on_empty_acts_as_push() {
        let mut h = min_u32_heap();
        let old = h.replace_root(42);
        assert_eq!(old, None);
        assert_eq!(h.peek(), Some(&42));
    }

    #[test]
    fn clear_resets_heap() {
        let mut h = min_u32_heap();
        h.push(1);
        h.push(2);
        h.push(3);
        h.clear();
        assert!(h.is_empty());
    }

    #[test]
    fn custom_comparator_max_heap() {
        // Reverse comparator → max-heap.
        let mut h: BinaryHeap<u32> = BinaryHeap::new(|a: &u32, b: &u32| b.cmp(a));
        for &v in &[3u32, 1, 4, 1, 5, 9, 2, 6, 5] {
            h.push(v);
        }
        // Now pop should yield in descending order.
        let mut out = Vec::new();
        while let Some(v) = h.pop() {
            out.push(v);
        }
        assert_eq!(out, vec![9, 6, 5, 5, 4, 3, 2, 1, 1]);
    }

    #[test]
    fn comparator_over_complex_type() {
        // Min-heap of (priority, value) pairs ordered by priority.
        type Item = (u32, &'static str);
        let mut h: BinaryHeap<Item> = BinaryHeap::new(|a: &Item, b: &Item| a.0.cmp(&b.0));
        h.push((5, "five"));
        h.push((1, "one"));
        h.push((3, "three"));
        assert_eq!(h.pop(), Some((1, "one")));
        assert_eq!(h.pop(), Some((3, "three")));
        assert_eq!(h.pop(), Some((5, "five")));
        assert_eq!(h.pop(), None);
    }
}
