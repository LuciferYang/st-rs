//! Port of `include/rocksdb/filter_policy.h`.
//!
//! A [`FilterPolicy`] builds and queries probabilistic filters (typically
//! Bloom or Ribbon) stored inside SST files. The engine consults these
//! filters on every point lookup: a "definitely not present" answer lets
//! the engine skip loading and searching the SST's data blocks.

/// A builder that accumulates keys during SST construction and then emits
/// a serialised filter. Matches upstream `FilterBitsBuilder`.
pub trait FilterBitsBuilder: Send {
    /// Add a key that will be a member of the filter.
    fn add_key(&mut self, key: &[u8]);

    /// Number of keys added so far.
    fn num_added(&self) -> usize;

    /// Finalise the filter and return its serialised bytes.
    fn finish(&mut self) -> Vec<u8>;

    /// Approximate size (in bytes) the filter will have after `finish`,
    /// assuming no more keys are added.
    fn approximate_num_entries(&self) -> usize;
}

/// A reader that queries the serialised filter produced by
/// [`FilterBitsBuilder::finish`]. Matches upstream `FilterBitsReader`.
pub trait FilterBitsReader: Send + Sync {
    /// Returns `true` if `key` *may* be present, `false` if it is
    /// definitely absent. The false-positive rate depends on the policy's
    /// tuning.
    fn may_match(&self, key: &[u8]) -> bool;

    /// Batch version of [`Self::may_match`]. Default implementation loops.
    fn may_match_multi(&self, keys: &[&[u8]]) -> Vec<bool> {
        keys.iter().map(|k| self.may_match(k)).collect()
    }
}

/// The policy that decides *which* filter implementation is used and with
/// what tuning. Matches upstream `FilterPolicy`.
pub trait FilterPolicy: Send + Sync {
    /// Stable name — must match on a round-trip through the OPTIONS file.
    fn name(&self) -> &'static str;

    /// Create a new builder. The caller fills it with keys, then calls
    /// `finish` to emit the serialised filter.
    fn new_builder(&self) -> Box<dyn FilterBitsBuilder>;

    /// Open a serialised filter for querying.
    fn new_reader(&self, contents: &[u8]) -> Box<dyn FilterBitsReader>;
}
