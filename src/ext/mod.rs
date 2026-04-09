//! User extension points.
//!
//! These are the traits the user plugs concrete implementations into to
//! customise the engine: the key ordering, block cache, SST factory,
//! filter policy, merge operator, and compaction filter. Every engine
//! layer above this one parameterises over these traits.

pub mod cache;
pub mod compaction_filter;
pub mod comparator;
pub mod filter_policy;
pub mod merge_operator;
pub mod table;
