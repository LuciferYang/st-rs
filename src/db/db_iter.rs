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

//! Port of `db/db_iter.{h,cc}` simplified for Layer 4b.
//!
//! Two things live here:
//!
//! 1. **Adapter types** ([`MemtableUserKeyIter`] and
//!    [`SstUserKeyIter`]) that present the engine's heterogeneous
//!    sources (memtable internal-key skiplist, SST user-key block
//!    iterator) through the uniform
//!    [`crate::db::merging_iterator::UserKeyIter`] interface.
//!    These are used by **compaction** to k-way-merge SSTs.
//!
//! 2. The user-facing [`DbIterator`] returned by
//!    [`crate::db::db_impl::DbImpl::iter`]. At Layer 4b this is
//!    **eager**: it materialises every live (non-tombstone) entry
//!    in the engine into a sorted owned `Vec` at construction
//!    time, then walks the vec on each `next` call.
//!
//! # Why eager?
//!
//! A streaming `DbIterator` would need to hold a `MergingIterator`
//! whose sources include `SstIter`s that borrow from
//! `BlockBasedTableReader` instances owned by the engine state.
//! That's a textbook self-referential lifetime problem in Rust:
//! the iterator can't simultaneously own the table-reader Arcs
//! and borrow from them. Workarounds (`ouroboros`, `Pin` + raw
//! pointers, full unsafe) are out of scope at this layer.
//!
//! Eager materialisation is O(N) memory and time per `iter()`
//! call but makes the iterator a plain `Vec` walk with no
//! lifetimes — which keeps Layer 4b safe and reviewable. A
//! Layer 4c follow-up can introduce a streaming variant behind
//! `unsafe` (matching upstream).

use crate::core::status::Result;
use crate::core::types::{SequenceNumber, ValueType};
use crate::db::dbformat::ParsedInternalKey;
use crate::db::memtable::MemTable;
use crate::db::merging_iterator::{MergingIterator, UserKeyIter};
use crate::memtable::skip_list::SkipListIter;
use crate::sst::block_based::sst_iterator::SstIter;
use crate::sst::block_based::table_reader::BlockBasedTableReader;
use std::collections::BTreeMap;
use std::sync::Arc;

/// "Read at the latest possible sequence" — used by default when
/// no explicit snapshot is passed. The 56-bit sequence field has
/// plenty of headroom.
pub const READ_AT_LATEST: SequenceNumber = u64::MAX >> 8;

// ---------------------------------------------------------------------------
// MemtableUserKeyIter — adapts the memtable's internal-key iterator
// ---------------------------------------------------------------------------

/// Wraps a [`SkipListIter`] over the memtable so it presents a
/// **user-key**-shaped view to the merging iterator.
///
/// The memtable stores entries as `(internal_key, value)` where
/// `internal_key = user_key || trailer(seq, type)`. The
/// `InternalKeyComparator` orders these by user-key ascending and
/// then sequence descending, so the *first* entry for any given
/// user key is always the newest version.
///
/// This wrapper presents only the newest version per user key.
/// Tombstones (Deletion / SingleDeletion) are presented with an
/// empty value, matching the engine convention.
pub struct MemtableUserKeyIter<'a> {
    inner: SkipListIter<'a>,
    cur_key: Vec<u8>,
    cur_value: Vec<u8>,
    valid: bool,
    /// Read-at sequence — only entries with `seq <= read_seq` are
    /// visible. Defaults to [`READ_AT_LATEST`] when the caller
    /// didn't pick a specific snapshot.
    read_seq: SequenceNumber,
}

impl<'a> MemtableUserKeyIter<'a> {
    /// Build a new wrapper that reads at the latest sequence.
    /// Caller must call `seek_to_first` or `seek` before reading.
    pub fn new(memtable: &'a MemTable) -> Self {
        Self::new_at(memtable, READ_AT_LATEST)
    }

    /// Build a wrapper that reads at a specific snapshot sequence.
    /// Entries with `seq > read_seq` are invisible — the iterator
    /// skips past them when positioning.
    pub fn new_at(memtable: &'a MemTable, read_seq: SequenceNumber) -> Self {
        Self {
            inner: memtable.iter(),
            cur_key: Vec::new(),
            cur_value: Vec::new(),
            valid: false,
            read_seq,
        }
    }

    /// Walk the inner iterator forward until we find an entry
    /// whose sequence is `<= read_seq` (visible at our snapshot).
    /// Thanks to internal-key ordering (newest-first per user
    /// key), the first visible entry per user key is the newest
    /// version at or below `read_seq` — exactly what a snapshot
    /// read wants.
    ///
    /// If an entire user key's versions are all above `read_seq`,
    /// this function's loop naturally advances to the next user
    /// key without surfacing anything for the invisible one.
    fn read_current(&mut self) {
        while self.inner.valid() {
            let parsed = match ParsedInternalKey::parse(self.inner.key()) {
                Ok(p) => p,
                Err(_) => {
                    self.valid = false;
                    return;
                }
            };
            if parsed.sequence > self.read_seq {
                // Invisible — skip and keep scanning.
                self.inner.next();
                continue;
            }
            // First visible version for this user key. Surface it.
            self.cur_key.clear();
            self.cur_key.extend_from_slice(parsed.user_key);
            self.cur_value.clear();
            match parsed.value_type {
                ValueType::Value | ValueType::Merge => {
                    self.cur_value.extend_from_slice(self.inner.value());
                }
                _ => {
                    // Deletion / SingleDeletion / RangeDeletion /
                    // blob / wide-column → empty value (tombstone).
                }
            }
            self.valid = true;
            return;
        }
        self.valid = false;
        self.cur_key.clear();
        self.cur_value.clear();
    }

    /// Advance past every remaining entry that shares the current
    /// user key — they're all older versions of a key we've
    /// already surfaced.
    fn skip_duplicates(&mut self) {
        let cur_key = self.cur_key.clone();
        loop {
            self.inner.next();
            if !self.inner.valid() {
                break;
            }
            let parsed = match ParsedInternalKey::parse(self.inner.key()) {
                Ok(p) => p,
                Err(_) => break,
            };
            if parsed.user_key != cur_key.as_slice() {
                break;
            }
        }
    }
}

impl<'a> UserKeyIter for MemtableUserKeyIter<'a> {
    fn valid(&self) -> bool {
        self.valid
    }
    fn key(&self) -> &[u8] {
        &self.cur_key
    }
    fn value(&self) -> &[u8] {
        &self.cur_value
    }
    fn seek_to_first(&mut self) {
        self.inner.seek_to_first();
        self.read_current();
    }
    fn seek(&mut self, target: &[u8]) {
        // Seek to `read_seq` — entries at higher seqs (not visible
        // to this snapshot) sort *earlier* in the internal-key
        // order, so seeking at `read_seq` lands us on or just
        // before the first visible version at this user key.
        // `read_current` then skips forward past any invisible
        // versions.
        let lookup = crate::db::dbformat::LookupKey::new(target, self.read_seq);
        self.inner.seek(lookup.internal_key());
        self.read_current();
    }
    fn next(&mut self) {
        if !self.valid {
            return;
        }
        self.skip_duplicates();
        self.read_current();
    }
}

// ---------------------------------------------------------------------------
// SstUserKeyIter — adapter from SstIter to UserKeyIter
// ---------------------------------------------------------------------------

/// Wraps an [`SstIter`] so the merging iterator can consume it.
///
/// # Layer 4d: internal-key SSTs
///
/// Starting at Layer 4d, SSTs store **internal keys** in
/// `(user_key asc, seq desc, type desc)` order. This wrapper
/// parses each entry's internal key, decodes the user key + value
/// type, and presents only the **newest** version per user key —
/// mirroring what `MemtableUserKeyIter` does for the memtable.
/// Tombstones (Deletion / SingleDeletion) are surfaced with an
/// empty value, matching the engine convention.
pub struct SstUserKeyIter<'a> {
    inner: SstIter<'a>,
    /// Decoded current user key (parsed from the SST's internal key).
    cur_key: Vec<u8>,
    /// Decoded current value (empty for tombstones).
    cur_value: Vec<u8>,
    /// True iff `cur_key`/`cur_value` are populated.
    valid: bool,
    /// Snapshot sequence — see [`MemtableUserKeyIter::new_at`].
    read_seq: SequenceNumber,
}

impl<'a> SstUserKeyIter<'a> {
    /// Wrap an existing SST iterator at the latest sequence.
    pub fn new(inner: SstIter<'a>) -> Self {
        Self::new_at(inner, READ_AT_LATEST)
    }

    /// Wrap an SST iterator that reads at a specific snapshot
    /// sequence. Entries with `seq > read_seq` are skipped.
    pub fn new_at(inner: SstIter<'a>, read_seq: SequenceNumber) -> Self {
        Self {
            inner,
            cur_key: Vec::new(),
            cur_value: Vec::new(),
            valid: false,
            read_seq,
        }
    }

    /// Skip invisible entries (seq > read_seq) and surface the
    /// first visible one. Identical in shape to
    /// [`MemtableUserKeyIter::read_current`].
    fn read_current(&mut self) {
        while self.inner.valid() {
            let parsed = match ParsedInternalKey::parse(self.inner.key()) {
                Ok(p) => p,
                Err(_) => {
                    self.valid = false;
                    return;
                }
            };
            if parsed.sequence > self.read_seq {
                self.inner.next();
                continue;
            }
            self.cur_key.clear();
            self.cur_key.extend_from_slice(parsed.user_key);
            self.cur_value.clear();
            match parsed.value_type {
                ValueType::Value | ValueType::Merge => {
                    self.cur_value.extend_from_slice(self.inner.value());
                }
                _ => {
                    // Deletion / SingleDeletion / RangeDeletion / blob /
                    // wide-column → empty value (tombstone).
                }
            }
            self.valid = true;
            return;
        }
        self.valid = false;
        self.cur_key.clear();
        self.cur_value.clear();
    }

    /// Skip past every additional entry in the SST that shares
    /// the current user key. The first one we landed on (newest,
    /// thanks to internal-key ordering) is what we surface; the
    /// rest are older versions that must be hidden from the
    /// merging iterator.
    fn skip_duplicates(&mut self) {
        let cur_key = self.cur_key.clone();
        loop {
            self.inner.next();
            if !self.inner.valid() {
                break;
            }
            let parsed = match ParsedInternalKey::parse(self.inner.key()) {
                Ok(p) => p,
                Err(_) => break,
            };
            if parsed.user_key != cur_key.as_slice() {
                break;
            }
        }
    }
}

impl<'a> UserKeyIter for SstUserKeyIter<'a> {
    fn valid(&self) -> bool {
        self.valid
    }
    fn key(&self) -> &[u8] {
        &self.cur_key
    }
    fn value(&self) -> &[u8] {
        &self.cur_value
    }
    fn seek_to_first(&mut self) {
        self.inner.seek_to_first();
        self.read_current();
    }
    fn seek(&mut self, target: &[u8]) {
        // Seek at our snapshot sequence.
        let lookup = crate::db::dbformat::LookupKey::new(target, self.read_seq);
        self.inner.seek(lookup.internal_key());
        self.read_current();
    }
    fn next(&mut self) {
        if !self.valid {
            return;
        }
        self.skip_duplicates();
        self.read_current();
    }
    fn status(&self) -> Result<()> {
        self.inner.status()
    }
}

// ---------------------------------------------------------------------------
// DbIterator — eager, vec-backed
// ---------------------------------------------------------------------------

/// User-facing forward + backward iterator over the engine.
/// Tombstones are skipped at materialisation time; the user only
/// sees live `(key, value)` pairs in sorted order.
///
/// Returned by [`crate::db::db_impl::DbImpl::iter`]. Owns its own
/// snapshot of the engine state, so it survives concurrent writes
/// to the engine without interference.
///
/// # Two construction modes
///
/// - **Eager** ([`Self::from_snapshot`] etc.): merges every source
///   into a single `Vec` at construction time. O(N) memory; safe for
///   small / test workloads where N is bounded.
/// - **Streaming** ([`Self::from_arcs`]): owns an `Arc` to each
///   source and refills a bounded chunk on demand inside `next()` /
///   `seek()`. O(chunk_size) steady-state memory. The path Flink
///   needs for `MapState` / `ListState` over multi-GB data.
///
/// Forward iteration stays chunked end-to-end. The first backward
/// op (`prev` / `seek_to_last`) triggers a one-shot full materialise
/// fallback — backward over multi-GB state isn't a Flink workload
/// today and the simple fallback keeps the streaming code small.
pub struct DbIterator {
    /// Current chunk of (key, value) — tombstones already filtered.
    /// In eager mode this is the full materialised set.
    items: Vec<(Vec<u8>, Vec<u8>)>,
    /// Position in `items`. `None` means "invalid". In streaming mode
    /// `Some(items.len())` triggers a refill on the next forward step.
    pos: Option<usize>,
    /// Streaming-mode state. `None` for eager iterators (legacy
    /// constructors) — those keep `items` populated end-to-end.
    streaming: Option<StreamingState>,
}

/// Snapshot-owned sources + chunk cursor. Lives inside a `DbIterator`
/// when constructed via [`DbIterator::from_arcs`].
struct StreamingState {
    sources: IteratorSources,
    /// Snapshot sequence — entries with seq > read_seq are invisible.
    read_seq: SequenceNumber,
    /// Maximum live entries per chunk. Bounds steady-state memory.
    chunk_size: usize,
    /// Refill cursor — what `refill()` should do next time it runs.
    cursor: RefillCursor,
    /// Lazy fallback for backward iteration. When the user calls
    /// `prev` or `seek_to_last`, we materialise the entire snapshot
    /// here and switch into eager backward mode.
    backward_full: Option<Vec<(Vec<u8>, Vec<u8>)>>,
}

/// Where the next `refill()` should start.
enum RefillCursor {
    /// Refill from the beginning of every source. Either initial state
    /// or after `seek_to_first`.
    Start,
    /// Refill from `target` inclusive — set by `seek(target)`.
    SeekFrom(Vec<u8>),
    /// Refill from the key strictly greater than this — set after a
    /// chunk is consumed, so the next chunk continues forward.
    After(Vec<u8>),
    /// All sources exhausted — no more refills.
    Done,
}

/// Snapshot of every source the streaming iterator may need to read.
struct IteratorSources {
    /// Active memtable, eagerly cloned into a sorted user-key view at
    /// iterator-construction time. Bounded by `write_buffer_size`
    /// (default 64 MiB) so this is safe to materialise in full.
    /// Each entry is `(user_key, value, is_tombstone)`.
    memtable_view: Vec<(Vec<u8>, Vec<u8>, bool)>,
    /// Same shape for the immutable memtable (a flush-in-progress one),
    /// if present.
    immutable_view: Vec<(Vec<u8>, Vec<u8>, bool)>,
    /// SST readers in priority order (newest-first per L0, then L1).
    /// Each refill creates a fresh `SstUserKeyIter` per reader and
    /// drops it before returning, so the readers themselves stay
    /// borrow-free between calls.
    ssts: Vec<Arc<BlockBasedTableReader>>,
}

impl DbIterator {
    /// Create an empty iterator. Used when the engine has no
    /// data; the test path uses [`Self::from_snapshot`].
    pub fn empty() -> Self {
        Self {
            items: Vec::new(),
            pos: None,
            streaming: None,
        }
    }

    /// Build a `DbIterator` by eagerly merging the memtable + a
    /// snapshot of SST readers. The merge proceeds in
    /// "newest-first" priority order: the memtable wins ties
    /// against any SST, and earlier SSTs in the list win against
    /// later ones (call sites should pass SSTs newest-first).
    /// Tombstones — entries with an empty value — are filtered
    /// out before the iterator is returned.
    pub fn from_snapshot(
        memtable: &MemTable,
        ssts: &[Arc<BlockBasedTableReader>],
    ) -> Result<Self> {
        Self::from_snapshot_with_immutable(memtable, None, ssts)
    }

    /// Same as [`Self::from_snapshot`] but also includes an
    /// **immutable memtable** (a memtable that has been frozen
    /// by a flush job but not yet turned into an SST). Reads
    /// at the latest sequence (`READ_AT_LATEST`).
    ///
    /// Priority order (first wins):
    ///
    /// 1. Active memtable (newest)
    /// 2. Immutable memtable (in-progress flush)
    /// 3. SSTs in the order passed (newest-first by convention)
    pub fn from_snapshot_with_immutable(
        memtable: &MemTable,
        immutable: Option<&MemTable>,
        ssts: &[Arc<BlockBasedTableReader>],
    ) -> Result<Self> {
        Self::from_snapshot_with_immutable_at(memtable, immutable, ssts, READ_AT_LATEST)
    }

    /// Same as [`Self::from_snapshot_with_immutable`] but reads
    /// at a specific snapshot sequence. Entries with
    /// `seq > read_seq` are filtered out by the adapters, so the
    /// resulting iterator only sees user keys that existed at
    /// the time the snapshot was taken.
    pub fn from_snapshot_with_immutable_at(
        memtable: &MemTable,
        immutable: Option<&MemTable>,
        ssts: &[Arc<BlockBasedTableReader>],
        read_seq: SequenceNumber,
    ) -> Result<Self> {
        // BTreeMap collapses duplicates by user key. We insert
        // the active memtable first; later sources only insert
        // if the key isn't already present, so the first source
        // wins on ties.
        let mut map: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

        // 1. Active memtable.
        let mut mt = MemtableUserKeyIter::new_at(memtable, read_seq);
        mt.seek_to_first();
        while mt.valid() {
            let key = mt.key().to_vec();
            if mt.is_tombstone() {
                map.insert(key, Vec::new()); // sentinel
            } else {
                map.insert(key, mt.value().to_vec());
            }
            mt.next();
        }

        // 2. Immutable memtable (only inserts not already present).
        if let Some(imm) = immutable {
            let mut it = MemtableUserKeyIter::new_at(imm, read_seq);
            it.seek_to_first();
            while it.valid() {
                let key = it.key().to_vec();
                map.entry(key).or_insert_with(|| {
                    if it.is_tombstone() {
                        Vec::new()
                    } else {
                        it.value().to_vec()
                    }
                });
                it.next();
            }
        }

        // 3. SSTs in newest-first order. We go through
        //    `SstUserKeyIter::new_at` so internal-key SSTs are
        //    filtered by the snapshot sequence, then parsed and
        //    deduplicated per user key.
        for table in ssts {
            let mut it = SstUserKeyIter::new_at(table.iter(), read_seq);
            it.seek_to_first();
            while it.valid() {
                let key = it.key().to_vec();
                map.entry(key).or_insert_with(|| {
                    if it.is_tombstone() {
                        Vec::new()
                    } else {
                        it.value().to_vec()
                    }
                });
                it.next();
            }
            it.status()?;
        }

        // 4. Filter tombstones (empty values).
        let items: Vec<(Vec<u8>, Vec<u8>)> = map
            .into_iter()
            .filter(|(_, v)| !v.is_empty())
            .collect();

        Ok(Self {
            items,
            pos: None,
            streaming: None,
        })
    }

    /// Default chunk size for [`Self::from_arcs`] — enough to hide
    /// per-refill overhead while staying small enough that a single
    /// chunk fits comfortably in L2 cache.
    pub const DEFAULT_CHUNK_SIZE: usize = 1024;

    /// Build a streaming `DbIterator` that owns `Arc`s to its sources
    /// and refills a bounded chunk on demand. Use this on multi-GB
    /// state — the eager constructors above materialise everything
    /// into a single `Vec` and OOM at scale.
    ///
    /// Memtables are still snapshot-cloned at construction (bounded by
    /// `write_buffer_size`); only the SST traversal is incremental.
    pub fn from_arcs(
        memtable: &MemTable,
        immutable: Option<&MemTable>,
        ssts: Vec<Arc<BlockBasedTableReader>>,
        read_seq: SequenceNumber,
        chunk_size: usize,
    ) -> Self {
        let memtable_view = collect_memtable_view(memtable, read_seq);
        let immutable_view = immutable
            .map(|m| collect_memtable_view(m, read_seq))
            .unwrap_or_default();
        let sources = IteratorSources {
            memtable_view,
            immutable_view,
            ssts,
        };
        Self {
            items: Vec::new(),
            pos: None,
            streaming: Some(StreamingState {
                sources,
                read_seq,
                chunk_size,
                cursor: RefillCursor::Start,
                backward_full: None,
            }),
        }
    }

    /// Number of live entries materialised.
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Whether the materialised snapshot is empty.
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Is the iterator positioned on a live entry?
    pub fn valid(&self) -> bool {
        self.pos.is_some()
    }

    /// Current key. Requires [`Self::valid`].
    pub fn key(&self) -> &[u8] {
        let i = self.pos.expect("DbIterator::key on invalid iter");
        &self.items[i].0
    }

    /// Current value. Requires [`Self::valid`].
    pub fn value(&self) -> &[u8] {
        let i = self.pos.expect("DbIterator::value on invalid iter");
        &self.items[i].1
    }

    /// Position at the first live key in the snapshot.
    pub fn seek_to_first(&mut self) {
        if self.streaming.is_some() {
            self.streaming.as_mut().unwrap().cursor = RefillCursor::Start;
            self.refill();
            return;
        }
        self.pos = if self.items.is_empty() { None } else { Some(0) };
    }

    /// Position at the last live key in the snapshot. In streaming
    /// mode this triggers a one-shot full materialisation; subsequent
    /// `prev` walks the backward buffer.
    pub fn seek_to_last(&mut self) {
        if self.streaming.is_some() {
            self.materialise_backward();
            // Reuse `items` for the backward pass.
            self.pos = if self.items.is_empty() {
                None
            } else {
                Some(self.items.len() - 1)
            };
            return;
        }
        self.pos = if self.items.is_empty() {
            None
        } else {
            Some(self.items.len() - 1)
        };
    }

    /// Position at the first key `>= target`.
    pub fn seek(&mut self, target: &[u8]) {
        if self.streaming.is_some() {
            self.streaming.as_mut().unwrap().cursor =
                RefillCursor::SeekFrom(target.to_vec());
            self.refill();
            return;
        }
        let idx = self
            .items
            .partition_point(|(k, _)| k.as_slice() < target);
        self.pos = if idx >= self.items.len() {
            None
        } else {
            Some(idx)
        };
    }

    /// Position at the last key `<= target`.
    pub fn seek_for_prev(&mut self, target: &[u8]) {
        if self.streaming.is_some() {
            self.materialise_backward();
            // Fall through to the eager binary-search path below.
        }
        let idx = self
            .items
            .partition_point(|(k, _)| k.as_slice() <= target);
        self.pos = if idx == 0 { None } else { Some(idx - 1) };
    }

    /// Advance to the next live key. In streaming mode this refills
    /// the chunk transparently when the current one runs out.
    pub fn next(&mut self) {
        if let Some(i) = self.pos {
            let next = i + 1;
            if next < self.items.len() {
                self.pos = Some(next);
                return;
            }
            // End of current chunk. In streaming mode, try to refill.
            if self.streaming.is_some() {
                let last_key = self.items[i].0.clone();
                let s = self.streaming.as_mut().unwrap();
                // Avoid overwriting an already-Done cursor.
                if !matches!(s.cursor, RefillCursor::Done) {
                    s.cursor = RefillCursor::After(last_key);
                }
                self.refill();
                return;
            }
            self.pos = None;
        }
    }

    /// Step back to the previous live key. Triggers full materialise
    /// the first time it's called in streaming mode.
    pub fn prev(&mut self) {
        if self.streaming.is_some() && self.streaming.as_ref().unwrap()
                .backward_full.is_none() {
            // Capture current key (if any) so we can re-position after
            // switching to the backward buffer.
            let cur_key = self.pos.map(|i| self.items[i].0.clone());
            self.materialise_backward();
            // Re-position at `cur_key` (or invalid if we were past end).
            self.pos = match cur_key {
                Some(k) => self
                    .items
                    .iter()
                    .position(|(ik, _)| ik == &k),
                None => None,
            };
        }
        if let Some(i) = self.pos {
            self.pos = if i == 0 { None } else { Some(i - 1) };
        }
    }

    // --------- Streaming-mode internals ---------

    /// Drain a fresh chunk from the underlying sources into `items`.
    /// Resets `pos` to point at the first entry of the new chunk
    /// (or to `None` if empty).
    fn refill(&mut self) {
        let s = match self.streaming.as_mut() {
            Some(s) => s,
            None => return,
        };
        if matches!(s.cursor, RefillCursor::Done) {
            self.items.clear();
            self.pos = None;
            return;
        }

        // Decide our seek target for this refill.
        let (start_key, exclusive): (Option<&[u8]>, bool) = match &s.cursor {
            RefillCursor::Start => (None, false),
            RefillCursor::SeekFrom(k) => (Some(k.as_slice()), false),
            RefillCursor::After(k) => (Some(k.as_slice()), true),
            RefillCursor::Done => unreachable!(),
        };

        let chunk = build_chunk(
            &s.sources,
            s.read_seq,
            start_key,
            exclusive,
            s.chunk_size,
        );

        // Update the cursor: if we filled less than chunk_size live
        // entries AND nothing was skipped past, sources are exhausted.
        // Otherwise advance after the last key we emitted.
        match chunk.last_key.clone() {
            Some(last) if chunk.live.len() == s.chunk_size => {
                s.cursor = RefillCursor::After(last);
            }
            Some(_) => {
                // We emitted fewer than chunk_size — sources exhausted.
                s.cursor = RefillCursor::Done;
            }
            None => {
                s.cursor = RefillCursor::Done;
            }
        }

        self.items = chunk.live;
        self.pos = if self.items.is_empty() {
            None
        } else {
            Some(0)
        };
    }

    /// Materialise the entire snapshot into `items` for backward
    /// iteration. Idempotent — no-op once we've already done it.
    fn materialise_backward(&mut self) {
        let s = match self.streaming.as_ref() {
            Some(s) => s,
            None => return,
        };
        if s.backward_full.is_some() {
            // Already materialised — switch `items` to the cached buf.
            self.items = self.streaming.as_ref().unwrap()
                .backward_full.as_ref().unwrap().clone();
            return;
        }

        // Drain everything in one big chunk.
        let chunk = build_chunk(&s.sources, s.read_seq, None, false, usize::MAX);
        let s = self.streaming.as_mut().unwrap();
        s.backward_full = Some(chunk.live.clone());
        s.cursor = RefillCursor::Done;
        self.items = chunk.live;
    }
}

// ---------------------------------------------------------------------------
// Streaming-mode helpers
// ---------------------------------------------------------------------------

/// One refill's worth of live (non-tombstone) entries plus the last
/// user key the merge actually visited (which may be a tombstone, used
/// to advance the cursor for the next refill).
struct ChunkOutput {
    /// Live entries in user-key order, ready to walk.
    live: Vec<(Vec<u8>, Vec<u8>)>,
    /// Last user key consumed by this refill (live or tombstone).
    /// `None` means "merge produced nothing" — no more data.
    last_key: Option<Vec<u8>>,
}

/// Snapshot the memtable's contents into a sorted, deduplicated user-key
/// view. Each output row is `(user_key, value, is_tombstone)`. The
/// memtable-iter already surfaces only the newest visible version per
/// user key for `read_seq`, so the output is a clean snapshot view.
fn collect_memtable_view(
    memtable: &MemTable,
    read_seq: SequenceNumber,
) -> Vec<(Vec<u8>, Vec<u8>, bool)> {
    let mut out: Vec<(Vec<u8>, Vec<u8>, bool)> = Vec::new();
    let mut it = MemtableUserKeyIter::new_at(memtable, read_seq);
    it.seek_to_first();
    while it.valid() {
        let key = it.key().to_vec();
        if it.is_tombstone() {
            out.push((key, Vec::new(), true));
        } else {
            out.push((key, it.value().to_vec(), false));
        }
        it.next();
    }
    out
}

/// Merge memtable view + immutable view + SST iterators starting at
/// `start_key` (or beginning if `None`). Honours newest-source-wins:
/// memtable > immutable > SSTs in the order passed.
///
/// `chunk_size` caps the number of **live** entries returned. We may
/// process more sources than that number (tombstones consume budget on
/// the source side but not on the live counter).
fn build_chunk(
    sources: &IteratorSources,
    read_seq: SequenceNumber,
    start_key: Option<&[u8]>,
    exclusive: bool,
    chunk_size: usize,
) -> ChunkOutput {
    use std::collections::BTreeMap;

    // Collect into a BTreeMap so we can deduplicate by user key with
    // first-source-wins semantics. Only enumerate as many keys as we
    // need to fill the chunk.
    let mut map: BTreeMap<Vec<u8>, (Vec<u8>, bool)> = BTreeMap::new();
    let mut last_key: Option<Vec<u8>> = None;

    // Walk both memtable views in priority order. Inlined twice to
    // sidestep the borrow checker (a closure would capture `map`
    // mutably and conflict with the post-call live-count check).
    for view in [&sources.memtable_view, &sources.immutable_view] {
        if live_count(&map) >= chunk_size {
            break;
        }
        let start_idx = match start_key {
            None => 0,
            Some(target) => {
                let mut i = view.partition_point(|(k, _, _)| k.as_slice() < target);
                if exclusive && i < view.len() && view[i].0.as_slice() == target {
                    i += 1;
                }
                i
            }
        };
        for (k, v, is_tomb) in &view[start_idx..] {
            map.entry(k.clone())
                .or_insert_with(|| (v.clone(), *is_tomb));
            if last_key.as_deref().is_none_or(|lk| lk < k.as_slice()) {
                last_key = Some(k.clone());
            }
            if live_count(&map) >= chunk_size {
                break;
            }
        }
    }

    // SSTs in priority order. Each gets a fresh iterator scoped to
    // this call — no borrow leaks back to the iterator struct.
    for table in &sources.ssts {
        if live_count(&map) >= chunk_size {
            break;
        }
        let mut it = SstUserKeyIter::new_at(table.iter(), read_seq);
        match start_key {
            None => it.seek_to_first(),
            Some(target) => {
                it.seek(target);
                if exclusive && it.valid() && it.key() == target {
                    it.next();
                }
            }
        }
        while it.valid() {
            let k = it.key().to_vec();
            let is_tomb = it.is_tombstone();
            map.entry(k.clone())
                .or_insert_with(|| {
                    if is_tomb {
                        (Vec::new(), true)
                    } else {
                        (it.value().to_vec(), false)
                    }
                });
            if last_key.as_deref().is_none_or(|lk| lk < k.as_slice()) {
                last_key = Some(k);
            }
            if map.values().filter(|(_, t)| !t).count() >= chunk_size {
                break;
            }
            it.next();
        }
    }

    let live: Vec<(Vec<u8>, Vec<u8>)> = map
        .into_iter()
        .filter(|(_, (_, is_tomb))| !is_tomb)
        .map(|(k, (v, _))| (k, v))
        .collect();
    ChunkOutput { live, last_key }
}

#[inline]
fn live_count(map: &std::collections::BTreeMap<Vec<u8>, (Vec<u8>, bool)>) -> usize {
    map.values().filter(|(_, t)| !t).count()
}

/// Helper: build a merging iterator over a memtable + a snapshot
/// of live SSTs. Used internally by compaction. The public read
/// path goes through [`DbIterator::from_snapshot`] which has no
/// lifetime constraints.
pub fn build_merging_iter<'a>(
    memtable: &'a MemTable,
    ssts: &'a [Arc<BlockBasedTableReader>],
) -> MergingIterator<'a> {
    let mut sources: Vec<Box<dyn UserKeyIter + 'a>> = Vec::with_capacity(ssts.len() + 1);
    sources.push(Box::new(MemtableUserKeyIter::new(memtable)));
    for table in ssts {
        sources.push(Box::new(SstUserKeyIter::new(table.iter())));
    }
    MergingIterator::new(sources)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ext::comparator::BytewiseComparator;

    fn empty_ssts() -> Vec<Arc<BlockBasedTableReader>> {
        Vec::new()
    }

    #[test]
    fn empty_iterator() {
        let mt = MemTable::new(Arc::new(BytewiseComparator));
        let it = DbIterator::from_snapshot(&mt, &empty_ssts()).unwrap();
        assert!(it.is_empty());
        assert!(!it.valid());
    }

    #[test]
    fn iterator_walks_memtable_entries() {
        let mut mt = MemTable::new(Arc::new(BytewiseComparator));
        mt.add(1, ValueType::Value, b"a", b"1").unwrap();
        mt.add(2, ValueType::Value, b"b", b"2").unwrap();
        mt.add(3, ValueType::Value, b"c", b"3").unwrap();
        let mut it = DbIterator::from_snapshot(&mt, &empty_ssts()).unwrap();
        it.seek_to_first();
        let mut got: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        while it.valid() {
            got.push((it.key().to_vec(), it.value().to_vec()));
            it.next();
        }
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].0, b"a");
        assert_eq!(got[2].0, b"c");
    }

    #[test]
    fn iterator_skips_tombstones() {
        let mut mt = MemTable::new(Arc::new(BytewiseComparator));
        mt.add(1, ValueType::Value, b"a", b"1").unwrap();
        mt.add(2, ValueType::Deletion, b"a", b"").unwrap();
        mt.add(3, ValueType::Value, b"b", b"2").unwrap();
        let mut it = DbIterator::from_snapshot(&mt, &empty_ssts()).unwrap();
        it.seek_to_first();
        // `a` was deleted; we should only see `b`.
        assert!(it.valid());
        assert_eq!(it.key(), b"b");
        it.next();
        assert!(!it.valid());
    }

    #[test]
    fn newer_memtable_versions_shadow_older() {
        let mut mt = MemTable::new(Arc::new(BytewiseComparator));
        mt.add(1, ValueType::Value, b"k", b"old").unwrap();
        mt.add(2, ValueType::Value, b"k", b"new").unwrap();
        let mut it = DbIterator::from_snapshot(&mt, &empty_ssts()).unwrap();
        it.seek_to_first();
        assert!(it.valid());
        assert_eq!(it.value(), b"new");
        it.next();
        assert!(!it.valid());
    }

    #[test]
    fn seek_lands_at_or_after_target() {
        let mut mt = MemTable::new(Arc::new(BytewiseComparator));
        for &k in &[b"a", b"c", b"e", b"g"] {
            mt.add(1, ValueType::Value, k, b"v").unwrap();
        }
        let mut it = DbIterator::from_snapshot(&mt, &empty_ssts()).unwrap();
        it.seek(b"c");
        assert_eq!(it.key(), b"c");
        it.seek(b"d");
        assert_eq!(it.key(), b"e");
        it.seek(b"z");
        assert!(!it.valid());
    }

    #[test]
    fn seek_for_prev_works() {
        let mut mt = MemTable::new(Arc::new(BytewiseComparator));
        for &k in &[b"a", b"c", b"e"] {
            mt.add(1, ValueType::Value, k, b"v").unwrap();
        }
        let mut it = DbIterator::from_snapshot(&mt, &empty_ssts()).unwrap();
        it.seek_for_prev(b"d");
        assert_eq!(it.key(), b"c");
        it.seek_for_prev(b"a");
        assert_eq!(it.key(), b"a");
        it.seek_for_prev(b" ");
        assert!(!it.valid());
    }

    #[test]
    fn backward_iteration() {
        let mut mt = MemTable::new(Arc::new(BytewiseComparator));
        for (i, k) in [b"a" as &[u8], b"b", b"c"].iter().enumerate() {
            mt.add(i as u64 + 1, ValueType::Value, k, b"v").unwrap();
        }
        let mut it = DbIterator::from_snapshot(&mt, &empty_ssts()).unwrap();
        it.seek_to_last();
        assert_eq!(it.key(), b"c");
        it.prev();
        assert_eq!(it.key(), b"b");
        it.prev();
        assert_eq!(it.key(), b"a");
        it.prev();
        assert!(!it.valid());
    }

    #[test]
    fn db_iter_seek_to_last_returns_last_user_key() {
        let mut mt = MemTable::new(Arc::new(BytewiseComparator));
        mt.add(1, ValueType::Value, b"alpha", b"v1").unwrap();
        mt.add(2, ValueType::Value, b"beta", b"v2").unwrap();
        mt.add(3, ValueType::Value, b"delta", b"v3").unwrap();
        mt.add(4, ValueType::Value, b"gamma", b"v4").unwrap();
        let mut it = DbIterator::from_snapshot(&mt, &empty_ssts()).unwrap();
        it.seek_to_last();
        assert!(it.valid());
        assert_eq!(it.key(), b"gamma"); // lexicographically last
        assert_eq!(it.value(), b"v4");
    }

    #[test]
    fn db_iter_prev_walks_backward_through_all() {
        let mut mt = MemTable::new(Arc::new(BytewiseComparator));
        for (i, k) in ["a", "b", "c", "d", "e"].iter().enumerate() {
            mt.add(i as u64 + 1, ValueType::Value, k.as_bytes(), b"v")
                .unwrap();
        }
        let mut it = DbIterator::from_snapshot(&mt, &empty_ssts()).unwrap();
        it.seek_to_last();
        let mut backward_keys = Vec::new();
        while it.valid() {
            backward_keys.push(it.key().to_vec());
            it.prev();
        }
        assert_eq!(
            backward_keys,
            vec![
                b"e".to_vec(),
                b"d".to_vec(),
                b"c".to_vec(),
                b"b".to_vec(),
                b"a".to_vec(),
            ]
        );
    }

    #[test]
    fn db_iter_seek_for_prev_between_entries() {
        let mut mt = MemTable::new(Arc::new(BytewiseComparator));
        mt.add(1, ValueType::Value, b"aa", b"1").unwrap();
        mt.add(2, ValueType::Value, b"cc", b"3").unwrap();
        mt.add(3, ValueType::Value, b"ee", b"5").unwrap();
        mt.add(4, ValueType::Value, b"gg", b"7").unwrap();
        let mut it = DbIterator::from_snapshot(&mt, &empty_ssts()).unwrap();
        // seek_for_prev("dd") should land on "cc" (last key <= "dd")
        it.seek_for_prev(b"dd");
        assert!(it.valid());
        assert_eq!(it.key(), b"cc");
        assert_eq!(it.value(), b"3");
        // seek_for_prev at exact key should land on that key
        it.seek_for_prev(b"ee");
        assert!(it.valid());
        assert_eq!(it.key(), b"ee");
        assert_eq!(it.value(), b"5");
        // seek_for_prev before all keys -> invalid
        it.seek_for_prev(b"a");
        assert!(!it.valid());
    }

    #[test]
    fn db_iter_seek_to_last_on_empty() {
        let mt = MemTable::new(Arc::new(BytewiseComparator));
        let mut it = DbIterator::from_snapshot(&mt, &empty_ssts()).unwrap();
        it.seek_to_last();
        assert!(!it.valid());
    }

    #[test]
    fn db_iter_prev_at_first_invalidates() {
        let mut mt = MemTable::new(Arc::new(BytewiseComparator));
        mt.add(1, ValueType::Value, b"only", b"one").unwrap();
        let mut it = DbIterator::from_snapshot(&mt, &empty_ssts()).unwrap();
        it.seek_to_first();
        assert!(it.valid());
        assert_eq!(it.key(), b"only");
        it.prev();
        assert!(!it.valid());
    }

    // ---- Streaming-mode tests ----

    #[test]
    fn streaming_iter_walks_full_set_with_chunked_refills() {
        let mut mt = MemTable::new(Arc::new(BytewiseComparator));
        // 100 keys, chunk_size=8 forces ~13 refills.
        for i in 0..100 {
            let k = format!("k{i:04}");
            mt.add(i + 1, ValueType::Value, k.as_bytes(), k.as_bytes())
                .unwrap();
        }
        let mut it = DbIterator::from_arcs(&mt, None, Vec::new(),
                READ_AT_LATEST, 8);
        it.seek_to_first();
        let mut keys = Vec::new();
        while it.valid() {
            keys.push(String::from_utf8(it.key().to_vec()).unwrap());
            it.next();
        }
        assert_eq!(keys.len(), 100);
        assert_eq!(keys[0], "k0000");
        assert_eq!(keys[99], "k0099");
    }

    #[test]
    fn streaming_iter_skips_tombstones_across_chunks() {
        let mut mt = MemTable::new(Arc::new(BytewiseComparator));
        for i in 0..20 {
            let k = format!("k{i:04}");
            // Even keys: live; odd keys: tombstone.
            let vt = if i % 2 == 0 {
                ValueType::Value
            } else {
                ValueType::Deletion
            };
            mt.add(i + 1, vt, k.as_bytes(), b"v").unwrap();
        }
        let mut it = DbIterator::from_arcs(&mt, None, Vec::new(),
                READ_AT_LATEST, 4);
        it.seek_to_first();
        let mut keys = Vec::new();
        while it.valid() {
            keys.push(String::from_utf8(it.key().to_vec()).unwrap());
            it.next();
        }
        // 10 even keys: k0000, k0002, ... k0018
        assert_eq!(keys.len(), 10);
        assert_eq!(keys.first().map(String::as_str), Some("k0000"));
        assert_eq!(keys.last().map(String::as_str), Some("k0018"));
    }

    #[test]
    fn streaming_iter_seek_skips_to_target() {
        let mut mt = MemTable::new(Arc::new(BytewiseComparator));
        for i in 0..30 {
            let k = format!("k{i:04}");
            mt.add(i + 1, ValueType::Value, k.as_bytes(), b"v").unwrap();
        }
        let mut it = DbIterator::from_arcs(&mt, None, Vec::new(),
                READ_AT_LATEST, 5);
        it.seek(b"k0010");
        let mut keys = Vec::new();
        while it.valid() {
            keys.push(String::from_utf8(it.key().to_vec()).unwrap());
            it.next();
        }
        // Should yield k0010..k0029 (20 entries).
        assert_eq!(keys.len(), 20);
        assert_eq!(keys[0], "k0010");
        assert_eq!(keys[19], "k0029");
    }

    #[test]
    fn streaming_iter_backward_falls_back_to_full_materialise() {
        let mut mt = MemTable::new(Arc::new(BytewiseComparator));
        for i in 0..10 {
            let k = format!("k{i:04}");
            mt.add(i + 1, ValueType::Value, k.as_bytes(), b"v").unwrap();
        }
        let mut it = DbIterator::from_arcs(&mt, None, Vec::new(),
                READ_AT_LATEST, 3);
        it.seek_to_last();
        let mut keys = Vec::new();
        while it.valid() {
            keys.push(String::from_utf8(it.key().to_vec()).unwrap());
            it.prev();
        }
        // Walked backward through all 10 keys.
        assert_eq!(keys.len(), 10);
        assert_eq!(keys.first().map(String::as_str), Some("k0009"));
        assert_eq!(keys.last().map(String::as_str), Some("k0000"));
    }
}
