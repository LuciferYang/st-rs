//! Port of `memtable/` from upstream.
//!
//! In-memory sorted key-value stores used to buffer writes before they
//! are flushed into an L0 SST. Upstream ships several backends
//! (`SkipListRep`, `HashLinkList`, `HashSkipList`, `VectorRep`);
//! Layer 3a ports only the default: a safe, **single-threaded**
//! skiplist. A Layer 4 follow-up can add a concurrent lock-free
//! variant once the engine actually needs one.
//!
//! The two key types:
//! - [`memtable_rep::MemTableRep`] — the abstract trait a
//!   memtable-like store must implement.
//! - [`skip_list::SkipList`] — a concrete safe skiplist with its own
//!   `MemTableRep` implementation.

pub mod memtable_rep;
pub mod skip_list;
