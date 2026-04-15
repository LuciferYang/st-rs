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

//! Port of `db/dbformat.{h,cc}`.
//!
//! The types defined here are shared between the memtable, the SST
//! format, the WAL replayer, and the DB read path. Every layer above
//! Layer 1 needs to understand the **internal key format**: the
//! packing of `(user_key, sequence, value_type)` into a single
//! comparable byte string.
//!
//! # Internal key layout
//!
//! ```text
//!   +-----------+----------------------+
//!   |  user_key | ~((seq<<8)|type) BE  |
//!   +-----------+----------------------+
//!                    8 bytes
//! ```
//!
//! The trailing 8 bytes are the bitwise **complement** of
//! `(seq << 8) | type`, written in **big-endian** order. This is a
//! deliberate divergence from upstream RocksDB's LE-packed trailer,
//! chosen so that **bytewise lexicographic order** of the encoded
//! internal key matches the desired MVCC order `(user_key asc,
//! sequence desc, type desc)`.
//!
//! ## Why the encoding matters
//!
//! The ordering trick lets every data-block-based container in the
//! engine — `memtable::SkipList`, `sst::block_based::BlockBuilder`,
//! `sst::block_based::Block`, and `sst::block_based::sst_iterator`
//! — sort internal keys correctly **without being aware of the
//! internal-key structure**. They just sort bytewise. That means:
//!
//! - The skiplist in the memtable uses a bytewise comparator and
//!   gets MVCC order for free.
//! - The SST data block builder and reader (which use lexicographic
//!   order for prefix compression and seek) work unchanged when
//!   fed internal keys.
//! - `BlockBasedTableReader::iter().seek(lookup_key)` lands on the
//!   correct MVCC entry with no custom comparator plumbing.
//!
//! The tradeoff: **non-bytewise user comparators are not supported**
//! at this layer. Layer 4d's `InternalKeyComparator` documents this
//! assumption and delegates to bytewise comparison. A follow-up
//! layer can add a comparator-parameterised block format if needed.
//!
//! Sequence numbers are still 56 bits (`0..2^56`). Total internal
//! key length is still always `user_key.len() + 8`.
//!
//! ## Why BE-inverted?
//!
//! - **Big-endian**: so that the most-significant byte comes first
//!   in the trailer, making numeric comparison match lexicographic
//!   comparison of the bytes.
//! - **Inverted**: so that a *larger* sequence (numerically) becomes
//!   a *smaller* byte string, which is what we want for "newer
//!   first" ordering.
//!
//! Put together: larger seq → smaller inverted value → smaller BE
//! bytes → sorts earlier. Newer-first order for MVCC reads "just
//! works" with any bytewise container.

use crate::core::status::{Result, Status};
use crate::core::types::{SequenceNumber, ValueType};
use crate::ext::comparator::Comparator;
use std::cmp::Ordering;
use std::sync::Arc;

/// Maximum representable sequence number. Only the low 56 bits of a
/// `u64` are used; the top byte is the value-type field.
pub const MAX_SEQUENCE_NUMBER: SequenceNumber = (1u64 << 56) - 1;

/// Value-type used when constructing a [`LookupKey`] for a point
/// lookup. Picked as the *largest* value-type byte so that, at a tied
/// sequence number, the lookup key sorts *before* any real internal
/// key and the first match is always a genuine newer-or-equal entry.
/// Matches upstream `kValueTypeForSeek`.
pub const VALUE_TYPE_FOR_SEEK: u8 = value_type_byte(ValueType::WideColumnEntity);

/// Convert a [`ValueType`] enum variant to its one-byte on-disk tag.
///
/// The numeric values must match upstream's `kTypeValue`, `kTypeDeletion`,
/// etc. They are also fixed on-disk — changing them breaks every
/// pre-existing SST and WAL.
pub const fn value_type_byte(t: ValueType) -> u8 {
    match t {
        ValueType::Deletion => 0x0,
        ValueType::Value => 0x1,
        ValueType::Merge => 0x2,
        ValueType::SingleDeletion => 0x7,
        ValueType::RangeDeletion => 0xF,
        ValueType::BlobIndex => 0x11,
        ValueType::WideColumnEntity => 0x17,
    }
}

/// Inverse of [`value_type_byte`]. Returns `None` for unrecognised
/// tags, which the caller should surface as `Status::Corruption`.
pub const fn byte_to_value_type(b: u8) -> Option<ValueType> {
    match b {
        0x0 => Some(ValueType::Deletion),
        0x1 => Some(ValueType::Value),
        0x2 => Some(ValueType::Merge),
        0x7 => Some(ValueType::SingleDeletion),
        0xF => Some(ValueType::RangeDeletion),
        0x11 => Some(ValueType::BlobIndex),
        0x17 => Some(ValueType::WideColumnEntity),
        _ => None,
    }
}

/// Pack a sequence number and value type into the 8-byte trailer of
/// an internal key. Matches upstream `PackSequenceAndType`.
#[inline]
pub fn pack_seq_and_type(seq: SequenceNumber, t: ValueType) -> u64 {
    debug_assert!(seq <= MAX_SEQUENCE_NUMBER);
    (seq << 8) | (value_type_byte(t) as u64)
}

/// Inverse of [`pack_seq_and_type`]. Returns `None` for an unknown
/// value-type byte.
#[inline]
pub fn unpack_seq_and_type(packed: u64) -> Option<(SequenceNumber, ValueType)> {
    let seq = packed >> 8;
    let t = byte_to_value_type((packed & 0xff) as u8)?;
    Some((seq, t))
}

// ---------------------------------------------------------------------------
// ParsedInternalKey — borrowed view
// ---------------------------------------------------------------------------

/// Borrowed view over an encoded internal key. Zero-copy; the user
/// key slice points into the caller's buffer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedInternalKey<'a> {
    /// The user-supplied key bytes.
    pub user_key: &'a [u8],
    /// Sequence number (56 bits).
    pub sequence: SequenceNumber,
    /// Value type tag.
    pub value_type: ValueType,
}

impl<'a> ParsedInternalKey<'a> {
    /// Decode `internal_key` into its three logical components.
    /// Returns `Status::Corruption` if the buffer is shorter than 8
    /// bytes or carries an unknown value-type byte.
    pub fn parse(internal_key: &'a [u8]) -> Result<Self> {
        if internal_key.len() < 8 {
            return Err(Status::corruption(format!(
                "internal key too short: {} bytes",
                internal_key.len()
            )));
        }
        let split = internal_key.len() - 8;
        let user_key = &internal_key[..split];
        let trailer_bytes: [u8; 8] = internal_key[split..]
            .try_into()
            .expect("slice of length 8");
        // BE-inverted: read as big-endian and then invert to
        // recover the original packed value.
        let packed = !u64::from_be_bytes(trailer_bytes);
        let (sequence, value_type) = unpack_seq_and_type(packed).ok_or_else(|| {
            Status::corruption(format!("unknown value type byte {:#x}", packed & 0xff))
        })?;
        Ok(Self {
            user_key,
            sequence,
            value_type,
        })
    }
}

// ---------------------------------------------------------------------------
// InternalKey — owned
// ---------------------------------------------------------------------------

/// Owned, encoded internal key. Used when a component needs to store
/// a key independently of its source buffer.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct InternalKey {
    bytes: Vec<u8>,
}

impl InternalKey {
    /// Construct from the three logical components. Trailer is
    /// encoded as `~((seq<<8) | type)` in big-endian byte order —
    /// see the module-level doc for why.
    pub fn new(user_key: &[u8], seq: SequenceNumber, value_type: ValueType) -> Self {
        let packed = pack_seq_and_type(seq, value_type);
        let mut bytes = Vec::with_capacity(user_key.len() + 8);
        bytes.extend_from_slice(user_key);
        bytes.extend_from_slice(&(!packed).to_be_bytes());
        Self { bytes }
    }

    /// The encoded internal key. This is what gets stored in a
    /// memtable / SST and compared by [`InternalKeyComparator`].
    pub fn encode(&self) -> &[u8] {
        &self.bytes
    }

    /// Consume `self` and return the underlying buffer.
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    /// Borrowed user key portion.
    pub fn user_key(&self) -> &[u8] {
        &self.bytes[..self.bytes.len() - 8]
    }

    /// Sequence number. Never fails on a well-formed `InternalKey`
    /// constructed through [`Self::new`] — only a manually-corrupted
    /// buffer could trip this.
    pub fn sequence(&self) -> SequenceNumber {
        self.packed() >> 8
    }

    /// Value type. Panics on a manually-corrupted buffer with an
    /// unknown type byte; use [`ParsedInternalKey::parse`] instead if
    /// you need to handle corruption gracefully.
    pub fn value_type(&self) -> ValueType {
        byte_to_value_type((self.packed() & 0xff) as u8).expect("unknown value-type byte")
    }

    fn packed(&self) -> u64 {
        let trailer: [u8; 8] = self.bytes[self.bytes.len() - 8..]
            .try_into()
            .expect("InternalKey is at least 8 bytes");
        // BE-inverted: read BE and flip to recover original packed.
        !u64::from_be_bytes(trailer)
    }
}

// ---------------------------------------------------------------------------
// LookupKey — what you pass to MemTable::Get
// ---------------------------------------------------------------------------

/// The wire format a caller hands to the memtable for a point lookup.
///
/// Upstream encodes this as `varint(internal_key_size) || user_key ||
/// pack(seq, kValueTypeForSeek)`. The varint prefix lets the memtable
/// skiplist compare length-prefixed internal keys without a second
/// length field. This port follows the same layout.
#[derive(Debug, Clone)]
pub struct LookupKey {
    /// Full encoded buffer: varint-length-prefixed internal key.
    buf: Vec<u8>,
    /// Byte offset where the internal key starts (i.e. just after
    /// the varint length prefix).
    internal_key_offset: usize,
}

impl LookupKey {
    /// Construct a lookup key for `user_key` at sequence `seq`. The
    /// value-type is fixed to [`VALUE_TYPE_FOR_SEEK`] so the resulting
    /// internal key sorts *immediately before* any real entry at the
    /// same sequence.
    pub fn new(user_key: &[u8], seq: SequenceNumber) -> Self {
        // Internal key size is user_key.len() + 8.
        let internal_key_size = user_key.len() + 8;
        // Varint encoding of internal_key_size needs at most 5 bytes.
        let mut buf = Vec::with_capacity(5 + internal_key_size);
        crate::util::coding::put_varint32(&mut buf, internal_key_size as u32);
        let internal_key_offset = buf.len();
        buf.extend_from_slice(user_key);
        // BE-inverted trailer: same encoding as InternalKey.
        let packed = (seq << 8) | VALUE_TYPE_FOR_SEEK as u64;
        buf.extend_from_slice(&(!packed).to_be_bytes());
        Self {
            buf,
            internal_key_offset,
        }
    }

    /// The length-prefixed encoded form, matching upstream's
    /// `LookupKey::memtable_key()`. This is what the memtable hash
    /// compares against.
    pub fn memtable_key(&self) -> &[u8] {
        &self.buf
    }

    /// The internal key only (no length prefix), matching upstream's
    /// `LookupKey::internal_key()`.
    pub fn internal_key(&self) -> &[u8] {
        &self.buf[self.internal_key_offset..]
    }

    /// Just the user key portion.
    pub fn user_key(&self) -> &[u8] {
        &self.internal_key()[..self.internal_key().len() - 8]
    }
}

// ---------------------------------------------------------------------------
// InternalKeyComparator
// ---------------------------------------------------------------------------

/// Comparator that orders internal keys per the contract in the
/// module-level doc: user key ascending, then sequence descending,
/// then value-type descending.
pub struct InternalKeyComparator {
    user_comparator: Arc<dyn Comparator>,
}

impl InternalKeyComparator {
    /// Wrap a user-supplied comparator.
    pub fn new(user_comparator: Arc<dyn Comparator>) -> Self {
        Self { user_comparator }
    }

    /// Access the inner user comparator.
    pub fn user_comparator(&self) -> &Arc<dyn Comparator> {
        &self.user_comparator
    }
}

impl std::fmt::Debug for InternalKeyComparator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "InternalKeyComparator({})",
            self.user_comparator.name()
        )
    }
}

impl Comparator for InternalKeyComparator {
    fn name(&self) -> &'static str {
        "rocksdb.InternalKeyComparator"
    }

    fn cmp(&self, a: &[u8], b: &[u8]) -> Ordering {
        // With the BE-inverted trailer encoding, bytewise lex order
        // already matches (user_key asc, sequence desc, type desc).
        // The `user_comparator` field is retained for API stability
        // but must be `BytewiseComparator` at this layer — non-
        // bytewise user comparators would break the ordering
        // invariant and are not supported until a future layer
        // threads an explicit comparator through the block format.
        debug_assert!(a.len() >= 8);
        debug_assert!(b.len() >= 8);
        a.cmp(b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ext::comparator::BytewiseComparator;

    fn user_cmp() -> Arc<dyn Comparator> {
        Arc::new(BytewiseComparator)
    }

    #[test]
    fn pack_unpack_round_trip() {
        let packed = pack_seq_and_type(42, ValueType::Value);
        let (seq, t) = unpack_seq_and_type(packed).unwrap();
        assert_eq!(seq, 42);
        assert_eq!(t, ValueType::Value);
    }

    #[test]
    fn max_sequence_round_trips() {
        let packed = pack_seq_and_type(MAX_SEQUENCE_NUMBER, ValueType::Deletion);
        let (seq, t) = unpack_seq_and_type(packed).unwrap();
        assert_eq!(seq, MAX_SEQUENCE_NUMBER);
        assert_eq!(t, ValueType::Deletion);
    }

    #[test]
    fn internal_key_round_trip() {
        let key = InternalKey::new(b"mykey", 100, ValueType::Value);
        assert_eq!(key.user_key(), b"mykey");
        assert_eq!(key.sequence(), 100);
        assert_eq!(key.value_type(), ValueType::Value);
        // Encoded form: "mykey" + 8 byte trailer = 13 bytes.
        assert_eq!(key.encode().len(), 13);
    }

    #[test]
    fn parsed_internal_key_matches() {
        let key = InternalKey::new(b"xx", 7, ValueType::Merge);
        let parsed = ParsedInternalKey::parse(key.encode()).unwrap();
        assert_eq!(parsed.user_key, b"xx");
        assert_eq!(parsed.sequence, 7);
        assert_eq!(parsed.value_type, ValueType::Merge);
    }

    #[test]
    fn parse_rejects_short_input() {
        assert!(ParsedInternalKey::parse(b"abc").unwrap_err().is_corruption());
    }

    #[test]
    fn parse_rejects_unknown_type() {
        // user_key "hi" + trailer with type 0x42 (unused).
        let mut buf = vec![b'h', b'i'];
        buf.extend_from_slice(&((1u64 << 8) | 0x42).to_le_bytes());
        assert!(ParsedInternalKey::parse(&buf).unwrap_err().is_corruption());
    }

    #[test]
    fn internal_comparator_orders_user_keys_asc() {
        let c = InternalKeyComparator::new(user_cmp());
        let a = InternalKey::new(b"aa", 1, ValueType::Value);
        let b = InternalKey::new(b"bb", 1, ValueType::Value);
        assert_eq!(c.cmp(a.encode(), b.encode()), Ordering::Less);
    }

    #[test]
    fn internal_comparator_orders_sequence_desc_on_tie() {
        let c = InternalKeyComparator::new(user_cmp());
        // Same user key, different sequences: newer should sort first.
        let newer = InternalKey::new(b"k", 100, ValueType::Value);
        let older = InternalKey::new(b"k", 50, ValueType::Value);
        assert_eq!(c.cmp(newer.encode(), older.encode()), Ordering::Less);
        assert_eq!(c.cmp(older.encode(), newer.encode()), Ordering::Greater);
    }

    #[test]
    fn lookup_key_encoding() {
        let lk = LookupKey::new(b"hello", 42);
        // user_key is 5 bytes + trailer 8 bytes = 13 byte internal key.
        // Varint-encoded length: 13 fits in 1 byte.
        assert_eq!(lk.memtable_key().len(), 1 + 13);
        assert_eq!(lk.internal_key().len(), 13);
        assert_eq!(lk.user_key(), b"hello");
    }

    #[test]
    fn lookup_key_sorts_before_equal_sequence() {
        // A lookup key at sequence S should sort >= any real entry
        // at the same user key with sequence <= S. Verify by direct
        // comparison through InternalKeyComparator.
        let c = InternalKeyComparator::new(user_cmp());
        let lk = LookupKey::new(b"k", 100);
        let real = InternalKey::new(b"k", 100, ValueType::Value);
        // Lookup key uses VALUE_TYPE_FOR_SEEK = 0x17 (largest), which
        // packs into a larger trailer, which sorts *before* a real
        // entry at the same sequence (descending on trailer).
        assert_eq!(
            c.cmp(lk.internal_key(), real.encode()),
            Ordering::Less,
            "lookup key should sort before real entry at same seq"
        );
    }
}
