//! Port of `cache/` from upstream.
//!
//! Concrete implementations of the Layer 0 [`Cache`](crate::ext::cache::Cache)
//! trait. Layer 2 ships a single-shard LRU cache; sharded and
//! HyperClock variants are deferred to Layer 2b.

pub mod lru;
