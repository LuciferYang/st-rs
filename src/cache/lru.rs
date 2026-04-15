//! Port of `cache/lru_cache.{h,cc}` — single-shard LRU cache.
//!
//! Implements the Layer 0 [`Cache`](crate::ext::cache::Cache) trait
//! using a classic HashMap + doubly-linked-list structure. The cache
//! tracks each entry by key, orders entries by last access, and
//! evicts from the tail whenever `insert` pushes total usage above
//! the configured capacity.
//!
//! # Design
//!
//! Upstream `LRUCache` is sharded (default 64 shards) with each shard
//! being a `LRUCacheShard` protected by its own mutex. This port is
//! **single-shard** — there's one big mutex over the HashMap and the
//! linked list. Contention is acceptable at Layer 2 because:
//!
//! - The engine layer doesn't exist yet, so we can't benchmark.
//! - Sharding just wraps N of these; the shape will be identical.
//! - A safer, linear implementation is easier to audit at this layer.
//!
//! A Layer 2b follow-up will wrap this in a `ShardedLruCache` with
//! `FNV`-based shard selection.
//!
//! # Linked list implementation
//!
//! Rust doesn't let you write a classic doubly-linked list in safe
//! code — you'd need `Rc<RefCell<Node>>` with cycles, or unsafe
//! pointers. Both are clumsy. Instead we use the **generational-index**
//! pattern: nodes live in a `Vec<Option<Node>>` and are linked by
//! `usize` indices. This is how `slotmap`, `generational-arena`, and
//! several production Rust crates solve the same problem.
//!
//! - `nodes[i]` is `Some(Node)` if slot `i` is live, `None` if free.
//! - Free slots are tracked in `free_list` for O(1) reuse.
//! - `head_` and `tail_` point to the MRU / LRU ends.
//! - Lookup goes through a `HashMap<Vec<u8>, NodeIndex>`.

use crate::core::status::{Result, Status};
use crate::ext::cache::{Cache, CacheHandle, CachePriority};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

type NodeIndex = usize;

/// One entry in the LRU.
#[derive(Debug)]
struct Node {
    key: Vec<u8>,
    value: Arc<Vec<u8>>,
    charge: usize,
    /// Retained for a future priority-aware eviction path. The simple
    /// Layer 2 LRU evicts strictly by recency.
    #[allow(dead_code)]
    priority: CachePriority,
    /// Link to the slot closer to MRU. `None` for the head.
    prev: Option<NodeIndex>,
    /// Link to the slot closer to LRU. `None` for the tail.
    next: Option<NodeIndex>,
}

/// The guarded inner state.
struct Inner {
    nodes: Vec<Option<Node>>,
    free_list: Vec<NodeIndex>,
    lookup: HashMap<Vec<u8>, NodeIndex>,
    head: Option<NodeIndex>, // most-recently-used
    tail: Option<NodeIndex>, // least-recently-used
    usage: usize,
    capacity: usize,
}

impl Inner {
    fn new(capacity: usize) -> Self {
        Self {
            nodes: Vec::new(),
            free_list: Vec::new(),
            lookup: HashMap::new(),
            head: None,
            tail: None,
            usage: 0,
            capacity,
        }
    }

    fn allocate_slot(&mut self, node: Node) -> NodeIndex {
        if let Some(idx) = self.free_list.pop() {
            self.nodes[idx] = Some(node);
            idx
        } else {
            let idx = self.nodes.len();
            self.nodes.push(Some(node));
            idx
        }
    }

    fn free_slot(&mut self, idx: NodeIndex) -> Option<Node> {
        let taken = self.nodes[idx].take();
        if taken.is_some() {
            self.free_list.push(idx);
        }
        taken
    }

    /// Detach node `idx` from the linked list. Does NOT free the
    /// slot — the caller decides whether to re-link or discard.
    fn detach(&mut self, idx: NodeIndex) {
        let prev;
        let next;
        {
            let node = self.nodes[idx]
                .as_ref()
                .expect("detach on freed slot");
            prev = node.prev;
            next = node.next;
        }
        if let Some(p) = prev {
            self.nodes[p].as_mut().unwrap().next = next;
        } else {
            // `idx` was head.
            self.head = next;
        }
        if let Some(n) = next {
            self.nodes[n].as_mut().unwrap().prev = prev;
        } else {
            // `idx` was tail.
            self.tail = prev;
        }
        let node = self.nodes[idx].as_mut().unwrap();
        node.prev = None;
        node.next = None;
    }

    /// Insert `idx` at the head of the LRU list.
    fn push_front(&mut self, idx: NodeIndex) {
        let old_head = self.head;
        {
            let node = self.nodes[idx].as_mut().unwrap();
            node.prev = None;
            node.next = old_head;
        }
        if let Some(h) = old_head {
            self.nodes[h].as_mut().unwrap().prev = Some(idx);
        } else {
            // List was empty → tail is now `idx` too.
            self.tail = Some(idx);
        }
        self.head = Some(idx);
    }

    /// Move an already-present node to the head.
    fn bump_to_front(&mut self, idx: NodeIndex) {
        if self.head == Some(idx) {
            return;
        }
        self.detach(idx);
        self.push_front(idx);
    }

    /// Evict from the tail until `usage <= capacity`. Does not evict
    /// if the cache is empty or already within budget.
    fn evict_to_fit(&mut self) {
        while self.usage > self.capacity {
            let Some(tail_idx) = self.tail else {
                return;
            };
            self.detach(tail_idx);
            let node = self
                .free_slot(tail_idx)
                .expect("tail slot should be live");
            self.usage = self.usage.saturating_sub(node.charge);
            self.lookup.remove(&node.key);
        }
    }
}

/// A single-shard LRU [`Cache`].
pub struct LruCache {
    inner: Mutex<Inner>,
    name: &'static str,
}

impl LruCache {
    /// Create a new LRU cache with the given capacity in bytes.
    pub fn new(capacity: usize) -> Self {
        Self::with_name(capacity, "LruCache")
    }

    /// Create with a custom `name` for diagnostics.
    pub fn with_name(capacity: usize, name: &'static str) -> Self {
        Self {
            inner: Mutex::new(Inner::new(capacity)),
            name,
        }
    }

    /// Configured capacity in bytes.
    pub fn capacity(&self) -> usize {
        self.inner.lock().unwrap().capacity
    }

    /// Current usage in bytes (sum of charges of all cached entries).
    pub fn usage(&self) -> usize {
        self.inner.lock().unwrap().usage
    }
}

/// Handle returned to users from [`Cache::insert`] / [`Cache::lookup`].
///
/// Internally just holds an `Arc` to the value bytes and the charge.
/// Dropping the handle releases the caller's reference but does NOT
/// directly trigger eviction; the cache evicts when usage exceeds
/// capacity on a future insert.
pub struct LruHandle {
    value: Arc<Vec<u8>>,
    charge: usize,
}

impl CacheHandle for LruHandle {
    fn value(&self) -> &[u8] {
        self.value.as_slice()
    }
    fn charge(&self) -> usize {
        self.charge
    }
}

impl Cache for LruCache {
    fn name(&self) -> &'static str {
        self.name
    }

    fn insert(
        &self,
        key: &[u8],
        value: Vec<u8>,
        charge: usize,
        priority: CachePriority,
    ) -> Result<Arc<dyn CacheHandle>> {
        if charge > {
            let inner = self.inner.lock().expect("LruCache poisoned");
            inner.capacity
        } {
            // Entry alone exceeds capacity — upstream rejects in
            // strict mode and stores anyway in loose mode. We reject
            // with `InvalidArgument` to make the misuse obvious.
            return Err(Status::invalid_argument(format!(
                "cache entry charge {} exceeds capacity",
                charge
            )));
        }

        let mut inner = self.inner.lock().expect("LruCache poisoned");

        // Replace existing entry, if any.
        if let Some(&idx) = inner.lookup.get(key) {
            inner.detach(idx);
            let old = inner.free_slot(idx).unwrap();
            inner.usage = inner.usage.saturating_sub(old.charge);
            inner.lookup.remove(&old.key);
        }

        let value_arc = Arc::new(value);
        let node = Node {
            key: key.to_vec(),
            value: Arc::clone(&value_arc),
            charge,
            priority,
            prev: None,
            next: None,
        };
        let idx = inner.allocate_slot(node);
        inner.push_front(idx);
        inner.lookup.insert(key.to_vec(), idx);
        inner.usage += charge;
        inner.evict_to_fit();

        Ok(Arc::new(LruHandle {
            value: value_arc,
            charge,
        }))
    }

    fn lookup(&self, key: &[u8]) -> Option<Arc<dyn CacheHandle>> {
        let mut inner = self.inner.lock().expect("LruCache poisoned");
        let idx = *inner.lookup.get(key)?;
        // Move to front.
        inner.bump_to_front(idx);
        let node = inner.nodes[idx].as_ref().unwrap();
        Some(Arc::new(LruHandle {
            value: Arc::clone(&node.value),
            charge: node.charge,
        }))
    }

    fn erase(&self, key: &[u8]) {
        let mut inner = self.inner.lock().expect("LruCache poisoned");
        if let Some(&idx) = inner.lookup.get(key) {
            inner.detach(idx);
            if let Some(node) = inner.free_slot(idx) {
                inner.usage = inner.usage.saturating_sub(node.charge);
                inner.lookup.remove(&node.key);
            }
        }
    }

    fn get_usage(&self) -> usize {
        self.inner.lock().expect("LruCache poisoned").usage
    }

    fn get_capacity(&self) -> usize {
        self.inner.lock().expect("LruCache poisoned").capacity
    }

    fn set_capacity(&self, capacity: usize) {
        let mut inner = self.inner.lock().expect("LruCache poisoned");
        inner.capacity = capacity;
        inner.evict_to_fit();
    }

    fn erase_unreferenced(&self) {
        // In this single-shard port we don't track per-entry reference
        // counts (the returned `Arc<CacheHandle>` is independent of
        // the cache's own Arc). `erase_unreferenced` is therefore a
        // no-op; real RocksDB behaviour is approximated by the normal
        // eviction path.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_lookup_round_trip() {
        let cache = LruCache::new(1024);
        let h = cache
            .insert(b"k1", vec![1, 2, 3], 3, CachePriority::default())
            .unwrap();
        assert_eq!(h.value(), &[1, 2, 3]);
        assert_eq!(cache.get_usage(), 3);

        let h2 = cache.lookup(b"k1").unwrap();
        assert_eq!(h2.value(), &[1, 2, 3]);
    }

    #[test]
    fn lookup_missing_returns_none() {
        let cache = LruCache::new(1024);
        assert!(cache.lookup(b"nope").is_none());
    }

    #[test]
    fn capacity_eviction_drops_oldest() {
        let cache = LruCache::new(10);
        cache
            .insert(b"a", vec![0; 4], 4, CachePriority::default())
            .unwrap();
        cache
            .insert(b"b", vec![0; 4], 4, CachePriority::default())
            .unwrap();
        cache
            .insert(b"c", vec![0; 4], 4, CachePriority::default())
            .unwrap();
        // Inserted 12 bytes into a 10-byte cache → oldest (`a`) evicted.
        assert_eq!(cache.get_usage(), 8);
        assert!(cache.lookup(b"a").is_none());
        assert!(cache.lookup(b"b").is_some());
        assert!(cache.lookup(b"c").is_some());
    }

    #[test]
    fn lookup_bumps_to_front() {
        let cache = LruCache::new(10);
        cache
            .insert(b"a", vec![0; 4], 4, CachePriority::default())
            .unwrap();
        cache
            .insert(b"b", vec![0; 4], 4, CachePriority::default())
            .unwrap();
        // Touching `a` should make it the MRU entry.
        let _ = cache.lookup(b"a");
        // Now insert `c` — should evict `b` (LRU), not `a`.
        cache
            .insert(b"c", vec![0; 4], 4, CachePriority::default())
            .unwrap();
        assert!(cache.lookup(b"a").is_some());
        assert!(cache.lookup(b"b").is_none());
        assert!(cache.lookup(b"c").is_some());
    }

    #[test]
    fn erase_removes_entry() {
        let cache = LruCache::new(1024);
        cache
            .insert(b"k", vec![1, 2], 2, CachePriority::default())
            .unwrap();
        assert_eq!(cache.get_usage(), 2);
        cache.erase(b"k");
        assert_eq!(cache.get_usage(), 0);
        assert!(cache.lookup(b"k").is_none());
    }

    #[test]
    fn insert_replaces_existing_entry() {
        let cache = LruCache::new(1024);
        cache
            .insert(b"k", vec![1], 1, CachePriority::default())
            .unwrap();
        cache
            .insert(b"k", vec![2, 3], 2, CachePriority::default())
            .unwrap();
        assert_eq!(cache.get_usage(), 2);
        assert_eq!(cache.lookup(b"k").unwrap().value(), &[2, 3]);
    }

    #[test]
    fn set_capacity_triggers_eviction() {
        let cache = LruCache::new(100);
        for i in 0..10 {
            cache
                .insert(&[i], vec![0; 10], 10, CachePriority::default())
                .unwrap();
        }
        assert_eq!(cache.get_usage(), 100);
        cache.set_capacity(40);
        assert!(cache.get_usage() <= 40);
    }

    #[test]
    fn handle_outlives_eviction() {
        let cache = LruCache::new(4);
        let h = cache
            .insert(b"k", vec![9, 9, 9, 9], 4, CachePriority::default())
            .unwrap();
        // Evict by adding a new entry of full size.
        cache
            .insert(b"k2", vec![1, 2, 3, 4], 4, CachePriority::default())
            .unwrap();
        assert!(cache.lookup(b"k").is_none());
        // Our handle still works because the Arc keeps the value alive.
        assert_eq!(h.value(), &[9, 9, 9, 9]);
    }

    #[test]
    fn entry_larger_than_capacity_is_rejected() {
        let cache = LruCache::new(10);
        // `unwrap_err` requires `Debug` on the Ok type (which is
        // `Arc<dyn CacheHandle>` — not Debug), so match instead.
        match cache.insert(b"k", vec![0; 100], 100, CachePriority::default()) {
            Ok(_) => panic!("expected error"),
            Err(e) => assert!(e.is_invalid_argument()),
        }
    }
}
