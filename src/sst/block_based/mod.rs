//! Port of `table/block_based/` from upstream.
//!
//! Layer 3a ships only the data-block primitives:
//! - [`block_builder::BlockBuilder`] — accumulates key-value records
//!   with prefix compression and restart markers, then emits the
//!   encoded block.
//! - [`block::Block`] + [`block::BlockIter`] — parses an encoded
//!   block and provides seek/next/prev iteration.
//!
//! Layer 3b will add `BlockBasedTableBuilder` and
//! `BlockBasedTableReader`, which stitch data blocks into a full SST
//! together with an index block, metaindex, filter, and footer.

pub mod block;
pub mod block_builder;
