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

use crate::core::status::Result;

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

/// Built-in merge operator that concatenates operands with a
/// delimiter byte. This is the Rust equivalent of upstream's
/// `StringAppendOperator` / `"stringappendtest"`.
///
/// Flink uses this for `ListState`: each `ListState.add(v)` calls
/// `db.merge(key, serialize(v))`, and the operator concatenates
/// all serialized values with a delimiter.
pub struct StringAppendOperator {
    /// Delimiter byte inserted between operands. Flink uses `,`
    /// (0x2C) by default, but this is configurable.
    delimiter: u8,
}

impl StringAppendOperator {
    /// The canonical name used by Flink / upstream to identify this
    /// operator. Must be passed to
    /// `ColumnFamilyOptions::merge_operator_name`.
    pub const NAME: &'static str = "stringappendtest";

    /// Create a new operator with the given delimiter byte.
    pub fn new(delimiter: u8) -> Self {
        Self { delimiter }
    }
}

impl Default for StringAppendOperator {
    /// Default delimiter is `,` (0x2C).
    fn default() -> Self {
        Self { delimiter: b',' }
    }
}

impl MergeOperator for StringAppendOperator {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn full_merge(
        &self,
        _key: &[u8],
        existing_value: Option<&[u8]>,
        operand_list: &[&[u8]],
    ) -> Result<Vec<u8>> {
        // Estimate total size for a single allocation.
        let delim_count = if existing_value.is_some() {
            operand_list.len()
        } else {
            operand_list.len().saturating_sub(1)
        };
        let total_len: usize = existing_value.map_or(0, |v| v.len())
            + operand_list.iter().map(|o| o.len()).sum::<usize>()
            + delim_count;
        let mut result = Vec::with_capacity(total_len);

        if let Some(existing) = existing_value {
            result.extend_from_slice(existing);
        }
        for operand in operand_list {
            if !result.is_empty() {
                result.push(self.delimiter);
            }
            result.extend_from_slice(operand);
        }
        Ok(result)
    }

    fn partial_merge(
        &self,
        _key: &[u8],
        left: &[u8],
        right: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        let mut result = Vec::with_capacity(left.len() + 1 + right.len());
        result.extend_from_slice(left);
        result.push(self.delimiter);
        result.extend_from_slice(right);
        Ok(Some(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_merge_with_base() {
        let op = StringAppendOperator::default();
        let result = op
            .full_merge(b"k", Some(b"a"), &[b"b", b"c"])
            .unwrap();
        assert_eq!(result, b"a,b,c");
    }

    #[test]
    fn full_merge_without_base() {
        let op = StringAppendOperator::default();
        let result = op.full_merge(b"k", None, &[b"x", b"y"]).unwrap();
        assert_eq!(result, b"x,y");
    }

    #[test]
    fn full_merge_single_operand_with_base() {
        let op = StringAppendOperator::default();
        let result = op.full_merge(b"k", Some(b"base"), &[b"op"]).unwrap();
        assert_eq!(result, b"base,op");
    }

    #[test]
    fn full_merge_single_operand_no_base() {
        let op = StringAppendOperator::default();
        let result = op.full_merge(b"k", None, &[b"only"]).unwrap();
        assert_eq!(result, b"only");
    }

    #[test]
    fn full_merge_empty_operands_with_base() {
        let op = StringAppendOperator::default();
        let result = op.full_merge(b"k", Some(b"base"), &[]).unwrap();
        assert_eq!(result, b"base");
    }

    #[test]
    fn full_merge_custom_delimiter() {
        let op = StringAppendOperator::new(b'|');
        let result = op
            .full_merge(b"k", Some(b"a"), &[b"b", b"c"])
            .unwrap();
        assert_eq!(result, b"a|b|c");
    }

    #[test]
    fn partial_merge_concatenates() {
        let op = StringAppendOperator::default();
        let result = op.partial_merge(b"k", b"left", b"right").unwrap();
        assert_eq!(result, Some(b"left,right".to_vec()));
    }

    #[test]
    fn name_matches_upstream() {
        let op = StringAppendOperator::default();
        assert_eq!(op.name(), "stringappendtest");
    }
}
