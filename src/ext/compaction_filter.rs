// Copyright 2025 The st-rs Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Port of `include/rocksdb/compaction_filter.h`.
//!
//! A [`CompactionFilter`] is called during compaction for each key in the
//! input. It can drop keys, rewrite their values, or leave them unchanged.
//! Used to implement TTL, garbage collection, and custom tombstoning —
//! which is exactly how Flink state backends use ForSt: the
//! `FlinkCompactionFilter` walks state and drops entries that have expired
//! under the state TTL policy.

use crate::core::types::EntryType;

/// Decision returned from [`CompactionFilter::filter`].
#[derive(Debug)]
pub enum CompactionDecision {
    /// Keep the entry as-is.
    Keep,
    /// Drop the entry entirely.
    Remove,
    /// Keep the entry but rewrite its value.
    ChangeValue(Vec<u8>),
    /// Remove every key in `[current, until)`. Used by range-delete-like
    /// filters to drop large regions in a single decision.
    RemoveAndSkipUntil(Vec<u8>),
}

/// A user-defined filter applied during compaction. Mirrors upstream
/// `CompactionFilter`.
///
/// Must be thread-safe: one filter instance is shared across all
/// concurrent sub-compactions of a single compaction job.
pub trait CompactionFilter: Send + Sync {
    /// Stable name stored in the OPTIONS file.
    fn name(&self) -> &'static str;

    /// Invoked for each input entry. `level` is the output level of the
    /// compaction currently running, 0 for flushes.
    fn filter(
        &self,
        level: u32,
        key: &[u8],
        entry_type: EntryType,
        existing_value: &[u8],
    ) -> CompactionDecision;

    /// If `true`, the engine may call [`Self::filter`] on blob-indirect
    /// values. Default: `false` — safer, but pays the cost of materialising
    /// the blob value.
    fn filter_blob_by_key(&self) -> bool {
        false
    }

    /// If `true`, entries with a `Merge` type will also be passed to
    /// [`Self::filter`]. Default: `false`.
    fn allow_merge(&self) -> bool {
        false
    }
}

/// Factory for [`CompactionFilter`] instances. The engine creates a fresh
/// filter for each compaction job, because compaction filters are often
/// stateful (e.g. the Flink TTL filter carries per-job wall-clock anchors).
pub trait CompactionFilterFactory: Send + Sync {
    /// Stable name.
    fn name(&self) -> &'static str;

    /// Create a filter for a specific compaction job.
    ///
    /// `is_full_compaction`: `true` if this compaction will include every
    /// SST in the input levels (and therefore every live key in the CF).
    /// `is_manual_compaction`: `true` if the user triggered this via
    /// `Db::compact_range`.
    fn create_compaction_filter(
        &self,
        is_full_compaction: bool,
        is_manual_compaction: bool,
    ) -> Box<dyn CompactionFilter>;
}
