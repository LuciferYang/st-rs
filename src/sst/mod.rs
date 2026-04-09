//! Port of `table/` from upstream — the concrete on-disk SST format.
//!
//! Name rationale: this module is called `sst` (not `table`) because
//! [`crate::ext::table`] already exists as the [`crate::ext::table::TableFactory`]
//! extension-point trait. Having both `ext::table` (the trait) and
//! `sst::block_based` (the implementation) keeps the two unambiguous.
//!
//! # Layer 3a contents
//!
//! - [`format`] — `BlockHandle`, `Footer`, the magic number, and
//!   encoding helpers for the bottom of an SST file.
//! - [`block_based::block`] — a single data block reader plus
//!   `BlockIter` for iterating its key-value records.
//! - [`block_based::block_builder`] — a `BlockBuilder` that
//!   accumulates key-value records with prefix compression and
//!   restart points, then emits the encoded block bytes.
//!
//! Not in 3a (deferred to 3b):
//!
//! - `BlockBasedTableBuilder` / `BlockBasedTableReader` — the code
//!   that stitches data blocks + an index block + a filter block +
//!   a footer into a complete SST file.
//! - Multi-level `IndexBuilder`.
//! - Bloom / ribbon `FilterBlockBuilder`.

pub mod block_based;
pub mod format;
