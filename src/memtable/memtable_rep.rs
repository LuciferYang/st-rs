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

//! Port of `memtable/memtablerep.h` (the abstract base class).
//!
//! Upstream `MemTableRep` is an interface with `Insert`, `Contains`,
//! `Get`, and `NewIterator` methods, backed by one of several
//! data-structure implementations. The DB layer's [`MemTable`] wraps
//! a `MemTableRep` instance plus an arena for byte storage.
//!
//! This Rust port keeps the same abstract shape. Implementations must
//! be `Send` so the memtable can hand them off to a flush thread;
//! they need *not* be `Sync` because each memtable is owned by a
//! single writer at a time (the write thread).

use crate::core::status::Result;

/// Abstract in-memory sorted store. Keys are opaque length-prefixed
/// byte strings encoded by the memtable's `KeyEncoder`
/// (`varint(internal_key_size) || internal_key || varint(value_size)
/// || value`). The implementation stores them as a single opaque key
/// and only compares using the comparator supplied at construction.
pub trait MemTableRep: Send {
    /// Insert an entry. The buffer holds the encoded key+value; the
    /// comparator sees the prefix as the "key" portion.
    fn insert(&mut self, entry: Vec<u8>) -> Result<()>;

    /// Returns `true` if any entry has the given lookup-prefix. The
    /// `lookup_key` is what a caller would pass to a point-read — the
    /// implementation compares it against the stored entries using
    /// the comparator.
    fn contains(&self, lookup_key: &[u8]) -> bool;

    /// Approximate memory usage in bytes — used by the write buffer
    /// manager to decide when to flush. Includes both the stored
    /// entries and the overhead of the data structure.
    fn approximate_memory_usage(&self) -> usize;

    /// Number of entries stored. Matches upstream `MemTableRep::NumEntries()`.
    fn num_entries(&self) -> usize;

    /// Open a new forward/backward iterator. The caller is responsible
    /// for re-seeking after any mutation of the underlying store.
    fn new_iterator(&self) -> Box<dyn MemTableRepIterator<'_> + '_>;
}

/// Iterator over a [`MemTableRep`]. Mirrors upstream
/// `MemTableRep::Iterator`. Not tied to the Layer 0 `DbIterator`
/// trait because the return type here is "the encoded entry buffer,"
/// not a `(key, value)` pair — the caller is expected to parse the
/// entry into `(internal_key, value)` using `util::coding`.
pub trait MemTableRepIterator<'a> {
    /// Is the iterator currently positioned on an entry?
    fn valid(&self) -> bool;

    /// The encoded entry at the current position. Requires [`Self::valid`].
    fn entry(&self) -> &[u8];

    /// Position at the first entry.
    fn seek_to_first(&mut self);

    /// Position at the last entry.
    fn seek_to_last(&mut self);

    /// Position at the first entry whose key is `>= target`.
    fn seek(&mut self, target: &[u8]);

    /// Advance to the next entry.
    fn next(&mut self);

    /// Retreat to the previous entry.
    fn prev(&mut self);
}
