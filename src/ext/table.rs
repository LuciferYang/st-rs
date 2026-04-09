//! Port of `include/rocksdb/table.h`.
//!
//! [`TableFactory`] is the extension point for plugging alternative SST
//! formats into the engine. Upstream has several: `BlockBasedTableFactory`
//! (the default), `PlainTableFactory`, `CuckooTableFactory`. Layer 0
//! defines only the abstract factory; concrete formats live in Layer 3.

/// Factory for the on-disk table (SST) format used by a column family.
///
/// Implementations must be thread-safe since the same factory is shared
/// across all compaction threads of a CF.
pub trait TableFactory: Send + Sync {
    /// Stable name (for OPTIONS-file serialisation and sanity checks).
    /// Matches upstream `TableFactory::Name()`.
    fn name(&self) -> &'static str;

    // The builder and reader methods (`NewTableBuilder`, `NewTableReader`)
    // depend on types — BlockBasedTableOptions, TableBuilder trait,
    // TableReader trait — that belong in Layer 3 alongside the concrete
    // block_based_table implementation. They are intentionally deferred.
}

/// Format version of the block-based SST table. Matches upstream
/// `BlockBasedTableOptions::format_version`. Exposed here because several
/// public callers (compatibility checks, OPTIONS-file parsing) need it
/// before Layer 3 is available.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[allow(missing_docs)]
pub enum TableFormatVersion {
    V0 = 0,
    V1 = 1,
    V2 = 2,
    V3 = 3,
    V4 = 4,
    #[default]
    V5 = 5,
    V6 = 6,
}

/// High-level hint about how a table will be accessed. Mirrors
/// `BlockBasedTableOptions::IndexType` from upstream, trimmed to variants
/// that are observable at Layer 0.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[allow(missing_docs)]
pub enum IndexType {
    #[default]
    BinarySearch,
    HashSearch,
    TwoLevelIndexSearch,
    BinarySearchWithFirstKey,
}
