//! Port of `db/memtable.{h,cc}`.
//!
//! The `MemTable` class is the in-memory write buffer that every
//! `Put` / `Delete` / `Merge` lands in first. It sits between the
//! WriteBatch replayer and the flush job:
//!
//! ```text
//!   Put → WriteBatch → WriteThread → MemTable::add
//!                                      │
//!                                      ▼
//!                               (immutable once full)
//!                                      │
//!                                      ▼
//!                                 FlushJob → L0 SST
//! ```
//!
//! # What this port includes
//!
//! - `add(seq, type, key, value)` — insert an entry at a given
//!   sequence number.
//! - `get(lookup_key)` — point-lookup returning `(value_type, value)`
//!   or `None` on miss.
//! - `iter()` — sorted internal-key iterator.
//! - Size accounting (`approximate_memory_usage`, `num_entries`).
//!
//! # What this port skips (Layer 4)
//!
//! - Merge operand assembly (requires a `MergeOperator` to combine
//!   multiple merge records; the shape exists, the wiring doesn't).
//! - Range deletion aggregator (a separate secondary structure).
//! - Concurrent memtable insert — this port is single-threaded.
//! - Prefix bloom filter acceleration.
//! - Atomic flush coordination across column families.
//!
//! # Encoding
//!
//! Upstream encodes each memtable entry as:
//!
//! ```text
//!   varint(internal_key_len) || internal_key || varint(value_len) || value
//! ```
//!
//! and stores the whole thing as a single opaque "key" inside the
//! skiplist, with the first prefix (the length-prefixed internal
//! key) serving as the comparable prefix. This port takes a simpler
//! route: it stores the internal key and value as **separate** fields
//! in the skiplist, because the skiplist is in-memory only — there's
//! no wire-format compatibility concern. The Layer 4 memtable can
//! swap in the packed encoding if it turns out to matter for speed.

use crate::core::status::Result;
use crate::core::types::{SequenceNumber, ValueType};
use crate::db::dbformat::{InternalKey, InternalKeyComparator, LookupKey, ParsedInternalKey};
use crate::ext::comparator::Comparator;
use crate::memtable::skip_list::SkipList;
use std::sync::Arc;

/// In-memory write buffer.
pub struct MemTable {
    /// Skiplist ordered by the internal-key comparator so that a
    /// single seek to `(user_key, seq, value_type_for_seek)` returns
    /// the newest matching entry.
    list: SkipList,
    /// The comparator used to build `list`. Kept so the memtable can
    /// expose it to the engine layer without re-constructing.
    comparator: Arc<InternalKeyComparator>,
    /// Highest sequence number ever `add`ed. Exposed to the engine
    /// for WAL consistency checks.
    highest_sequence: SequenceNumber,
}

/// Result of a successful memtable point lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemTableGetResult {
    /// Latest entry for this user key is a live value.
    Found(Vec<u8>),
    /// Latest entry is a tombstone — the key is explicitly deleted.
    Deleted,
    /// No entry for this user key at or below the lookup sequence.
    NotFound,
}

impl MemTable {
    /// Create an empty memtable with the given user comparator
    /// (typically `BytewiseComparator`).
    pub fn new(user_comparator: Arc<dyn Comparator>) -> Self {
        let internal = Arc::new(InternalKeyComparator::new(user_comparator));
        let list = SkipList::new(internal.clone() as Arc<dyn Comparator>);
        Self {
            list,
            comparator: internal,
            highest_sequence: 0,
        }
    }

    /// Access the internal-key comparator. Used by the engine to
    /// create iterators that share the memtable's ordering.
    pub fn comparator(&self) -> &Arc<InternalKeyComparator> {
        &self.comparator
    }

    /// Number of entries (across all sequence numbers and types).
    pub fn num_entries(&self) -> usize {
        self.list.len()
    }

    /// Approximate byte usage. The engine consults this against
    /// `write_buffer_size` to decide when to switch memtables.
    pub fn approximate_memory_usage(&self) -> usize {
        self.list.approximate_memory_usage()
    }

    /// Highest sequence number ever added to this memtable.
    pub fn highest_sequence(&self) -> SequenceNumber {
        self.highest_sequence
    }

    /// Insert an entry. `seq` must be strictly greater than every
    /// previously-added entry in this memtable if the caller wants
    /// MVCC ordering to hold, but this is not enforced here — the
    /// write thread in the engine guarantees it.
    pub fn add(
        &mut self,
        seq: SequenceNumber,
        value_type: ValueType,
        user_key: &[u8],
        value: &[u8],
    ) -> Result<()> {
        let key = InternalKey::new(user_key, seq, value_type).into_bytes();
        self.list.insert(key, value.to_vec());
        if seq > self.highest_sequence {
            self.highest_sequence = seq;
        }
        Ok(())
    }

    /// Point lookup. Returns the newest entry for the lookup key's
    /// user key at a sequence `<=` the lookup sequence. If the newest
    /// such entry is a `Deletion` or `SingleDeletion`, returns
    /// [`MemTableGetResult::Deleted`]. If there is no matching entry
    /// at all, returns [`MemTableGetResult::NotFound`].
    ///
    /// Merge operand assembly is **not** implemented in this Layer 3a
    /// port — a `Merge` entry is returned as-if it were a `Value`,
    /// which is technically wrong but has no observable effect until
    /// the engine's merge operator is wired up.
    pub fn get(&self, lookup: &LookupKey) -> MemTableGetResult {
        let mut it = self.list.iter();
        it.seek(lookup.internal_key());
        if !it.valid() {
            return MemTableGetResult::NotFound;
        }

        // The seek landed on the first internal key `>= lookup_key`.
        // Because the internal comparator orders by (user_key asc,
        // seq desc, type desc), the first result — if it matches the
        // user key — is the newest version ≤ the lookup sequence.
        let parsed = match ParsedInternalKey::parse(it.key()) {
            Ok(p) => p,
            Err(_) => return MemTableGetResult::NotFound,
        };

        // Check that the user key actually matches. If not, the
        // lookup key's user key has no entry.
        let user_cmp = self.comparator.user_comparator();
        if user_cmp.cmp(parsed.user_key, lookup.user_key()) != std::cmp::Ordering::Equal {
            return MemTableGetResult::NotFound;
        }

        match parsed.value_type {
            ValueType::Value | ValueType::Merge => {
                // Simplification: treat Merge as Value for now.
                MemTableGetResult::Found(it.value().to_vec())
            }
            ValueType::Deletion | ValueType::SingleDeletion => MemTableGetResult::Deleted,
            // Range deletions are handled by a separate aggregator
            // in upstream; ignore here.
            ValueType::RangeDeletion => MemTableGetResult::NotFound,
            // Blob indices and wide-column entries need Layer 4+
            // support; treat as not-found for now.
            ValueType::BlobIndex | ValueType::WideColumnEntity => MemTableGetResult::NotFound,
        }
    }

    /// Iterate over every entry in sorted internal-key order. The
    /// iterator yields raw internal-key bytes, which callers can
    /// parse via [`ParsedInternalKey::parse`].
    pub fn iter(&self) -> crate::memtable::skip_list::SkipListIter<'_> {
        self.list.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ext::comparator::BytewiseComparator;

    fn memtable() -> MemTable {
        MemTable::new(Arc::new(BytewiseComparator))
    }

    #[test]
    fn insert_and_get_single_entry() {
        let mut mt = memtable();
        mt.add(1, ValueType::Value, b"hello", b"world").unwrap();
        assert_eq!(mt.num_entries(), 1);
        assert_eq!(mt.highest_sequence(), 1);

        let lk = LookupKey::new(b"hello", 10);
        assert_eq!(mt.get(&lk), MemTableGetResult::Found(b"world".to_vec()));
    }

    #[test]
    fn newer_sequence_shadows_older() {
        let mut mt = memtable();
        mt.add(1, ValueType::Value, b"k", b"v1").unwrap();
        mt.add(2, ValueType::Value, b"k", b"v2").unwrap();
        mt.add(3, ValueType::Value, b"k", b"v3").unwrap();

        let lk = LookupKey::new(b"k", 10);
        assert_eq!(mt.get(&lk), MemTableGetResult::Found(b"v3".to_vec()));
    }

    #[test]
    fn lookup_at_old_sequence_sees_old_value() {
        let mut mt = memtable();
        mt.add(1, ValueType::Value, b"k", b"v1").unwrap();
        mt.add(2, ValueType::Value, b"k", b"v2").unwrap();
        mt.add(3, ValueType::Value, b"k", b"v3").unwrap();

        // At sequence 2, we should see v2, not v3.
        let lk = LookupKey::new(b"k", 2);
        assert_eq!(mt.get(&lk), MemTableGetResult::Found(b"v2".to_vec()));
    }

    #[test]
    fn deletion_tombstone() {
        let mut mt = memtable();
        mt.add(1, ValueType::Value, b"k", b"v").unwrap();
        mt.add(2, ValueType::Deletion, b"k", b"").unwrap();

        let lk = LookupKey::new(b"k", 10);
        assert_eq!(mt.get(&lk), MemTableGetResult::Deleted);
    }

    #[test]
    fn lookup_missing_key() {
        let mut mt = memtable();
        mt.add(1, ValueType::Value, b"exists", b"v").unwrap();
        let lk = LookupKey::new(b"missing", 10);
        assert_eq!(mt.get(&lk), MemTableGetResult::NotFound);
    }

    #[test]
    fn iter_yields_internal_key_order() {
        let mut mt = memtable();
        mt.add(1, ValueType::Value, b"a", b"1").unwrap();
        mt.add(2, ValueType::Value, b"b", b"2").unwrap();
        mt.add(3, ValueType::Value, b"a", b"1b").unwrap();

        let mut keys = Vec::new();
        let mut it = mt.iter();
        it.seek_to_first();
        while it.valid() {
            let p = ParsedInternalKey::parse(it.key()).unwrap();
            keys.push((p.user_key.to_vec(), p.sequence));
            it.next();
        }
        // Expected order: a@3 (newer first), a@1, b@2
        assert_eq!(
            keys,
            vec![
                (b"a".to_vec(), 3),
                (b"a".to_vec(), 1),
                (b"b".to_vec(), 2),
            ]
        );
    }

    #[test]
    fn highest_sequence_tracks_max() {
        let mut mt = memtable();
        mt.add(5, ValueType::Value, b"k", b"v").unwrap();
        mt.add(2, ValueType::Value, b"k", b"v").unwrap();
        mt.add(10, ValueType::Value, b"k", b"v").unwrap();
        assert_eq!(mt.highest_sequence(), 10);
    }
}
