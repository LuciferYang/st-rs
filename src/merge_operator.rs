//! Port of `include/rocksdb/merge_operator.h`.
//!
//! A [`MergeOperator`] turns a sequence of merge operands and an optional
//! existing value into a single resulting value. Used to implement
//! counters, sets, and any other "read-modify-write" pattern that would
//! otherwise require a full Get / Put cycle under a lock.
//!
//! Layer 0 exposes only the *full-merge* variant. Upstream also has
//! "associative" and "partial-merge" flavors; those are convenience
//! optimisations that don't belong at the trait-definition layer.

use crate::status::Result;

/// User-defined combiner over a sequence of operands.
///
/// Implementations must be deterministic and thread-safe.
pub trait MergeOperator: Send + Sync {
    /// Stable name. Must be consistent across reopens — the engine
    /// compares this against the name stored in the OPTIONS file.
    fn name(&self) -> &'static str;

    /// Full merge: given the current value (if any) and an ordered list
    /// of operands oldest-first, produce the new value.
    ///
    /// Returning `Err` aborts the merge and surfaces as a
    /// `Status::Corruption` to the caller — the engine does not retry.
    fn full_merge(
        &self,
        key: &[u8],
        existing_value: Option<&[u8]>,
        operand_list: &[&[u8]],
    ) -> Result<Vec<u8>>;

    /// Partial merge of two adjacent operands during compaction. Returning
    /// `Ok(None)` means "cannot partial-merge these two", which is always
    /// a valid answer. Default: returns `Ok(None)`.
    fn partial_merge(
        &self,
        _key: &[u8],
        _left: &[u8],
        _right: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }

    /// If `true`, repeated identical operands can be de-duplicated by the
    /// engine during compaction. Default: `false`.
    fn allow_single_operand(&self) -> bool {
        false
    }
}
