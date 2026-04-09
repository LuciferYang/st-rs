//! Port of `include/rocksdb/cache.h`.
//!
//! A [`Cache`] is a bounded in-memory key → value mapping with a
//! replacement policy (LRU, CLOCK, HyperClock, …). The LSM engine uses it
//! primarily for SST block caching: the key is an internal block identifier,
//! the value is the uncompressed block contents.
//!
//! Layer 0 only defines the trait. Real implementations (`LruCache`,
//! `ClockCache`, `HyperClockCache`) belong in Layer 2 under a `cache` crate.

use crate::core::status::Result;
use std::sync::Arc;

/// Opaque type used by [`Cache`] to identify cached entries. Upstream uses
/// `void*` + manual `Release`; in Rust we use a refcounted handle whose
/// `Drop` implementation releases the entry.
pub trait CacheHandle: Send + Sync {
    /// Access the cached value bytes without copying.
    fn value(&self) -> &[u8];

    /// The charge (cost) this entry contributes to the cache capacity.
    fn charge(&self) -> usize;
}

/// Eviction priority for cache entries. High-priority entries are evicted
/// last. Matches upstream `Cache::Priority`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum CachePriority {
    /// Filter and index blocks — should stay resident longer.
    High,
    /// Regular data blocks.
    #[default]
    Low,
    /// Bottommost-level data blocks — evict aggressively.
    Bottom,
}

/// A bounded in-memory cache. Implementations must be thread-safe.
pub trait Cache: Send + Sync {
    /// A stable name for this cache implementation.
    fn name(&self) -> &'static str;

    /// Insert `value` under `key`, charging `charge` bytes against the
    /// capacity. Returns a handle whose lifetime controls when the entry
    /// becomes eligible for eviction.
    fn insert(
        &self,
        key: &[u8],
        value: Vec<u8>,
        charge: usize,
        priority: CachePriority,
    ) -> Result<Arc<dyn CacheHandle>>;

    /// Look up an entry. Returns `None` on miss.
    fn lookup(&self, key: &[u8]) -> Option<Arc<dyn CacheHandle>>;

    /// Remove an entry from the cache. Already-held handles remain valid
    /// until released.
    fn erase(&self, key: &[u8]);

    /// Total charge (bytes) currently held by the cache.
    fn get_usage(&self) -> usize;

    /// Configured capacity in bytes.
    fn get_capacity(&self) -> usize;

    /// Dynamically resize the cache.
    fn set_capacity(&self, capacity: usize);

    /// Forcibly evict every unreferenced entry. Mostly useful for tests.
    fn erase_unreferenced(&self);
}
