//! Port of `include/rocksdb/types.h`.
//!
//! Primitive aliases and enums used across the public API. This module is
//! leaf-level — it depends on nothing else in the crate.

/// Numeric identifier of a column family, unique within a single DB instance.
///
/// Mirrors `rocksdb::ColumnFamilyId` (`uint32_t`).
pub type ColumnFamilyId = u32;

/// Monotonic sequence number assigned to each write. Mirrors
/// `rocksdb::SequenceNumber` (`uint64_t`).
pub type SequenceNumber = u64;

/// The smallest sequence number that may be assigned to an uncommitted
/// write. Sequence 0 is reserved as "always committed" (mirrors
/// `kMinUnCommittedSeq`).
pub const MIN_UNCOMMITTED_SEQ: SequenceNumber = 1;

/// Why an SST file was created. Mirrors `TableFileCreationReason`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TableFileCreationReason {
    /// A memtable was flushed into this SST.
    Flush,
    /// A compaction produced this SST.
    Compaction,
    /// The SST was rebuilt during crash recovery.
    Recovery,
    /// Any other reason (e.g. manual ingest, test utilities).
    Misc,
}

/// Why a blob file was created. Mirrors `BlobFileCreationReason`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BlobFileCreationReason {
    /// The blob file was produced by a memtable flush.
    Flush,
    /// The blob file was produced by a compaction.
    Compaction,
    /// The blob file was rebuilt during crash recovery.
    Recovery,
}

/// The kind of file RocksDB stores inside a DB directory. Mirrors `FileType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[allow(missing_docs)]
pub enum FileType {
    WalFile,
    DbLockFile,
    TableFile,
    DescriptorFile,
    CurrentFile,
    TempFile,
    /// Either the current or an old info log.
    InfoLogFile,
    MetaDatabase,
    IdentityFile,
    OptionsFile,
    BlobFile,
}

/// User-visible representation of an internal entry type. Mirrors
/// `enum EntryType`. The ordering of variants must not change — it is
/// observable to compaction filters and merge operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[allow(missing_docs)]
pub enum EntryType {
    Put,
    Delete,
    SingleDelete,
    Merge,
    RangeDeletion,
    BlobIndex,
    DeleteWithTimestamp,
    WideColumnEntity,
    Other,
}

/// The low-level value tag encoded in an internal key. Used by the memtable
/// and SST format. Roughly maps to upstream `kTypeValue`, `kTypeDeletion`,
/// etc. Only the common variants are included at Layer 0; the engine layer
/// may extend with private tags.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[allow(missing_docs)]
#[repr(u8)]
pub enum ValueType {
    Deletion = 0x0,
    Value = 0x1,
    Merge = 0x2,
    SingleDeletion = 0x7,
    RangeDeletion = 0xF,
    BlobIndex = 0x11,
    WideColumnEntity = 0x17,
}

/// Cause of a write stall — the reason a `Put`/`Write` is being slowed or
/// stopped. Mirrors upstream `WriteStallCause` (non-exhaustive: we only carry
/// the publicly observable variants).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[allow(missing_docs)]
pub enum WriteStallCause {
    MemtableLimit,
    L0FileCountLimit,
    PendingCompactionBytes,
    WriteBufferManagerLimit,
    None,
}

/// Current write-stall state for a column family or the whole DB.
/// Mirrors `WriteStallCondition`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum WriteStallCondition {
    /// Writes are being artificially slowed.
    Delayed,
    /// Writes are completely halted until conditions improve.
    Stopped,
    /// Normal — no throttling in effect.
    #[default]
    Normal,
}

/// Temperature hint for files — used by tiered storage to place "hot" vs
/// "cold" data on different media. Mirrors `Temperature`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[allow(missing_docs)]
pub enum Temperature {
    #[default]
    Unknown,
    Hot,
    Warm,
    Cold,
}

/// Compression algorithm. Mirrors `CompressionType`. Only the algorithms that
/// may appear in the on-disk format are listed; Layer 0 does not choose an
/// implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[allow(missing_docs)]
pub enum CompressionType {
    #[default]
    None = 0x0,
    Snappy = 0x1,
    Zlib = 0x2,
    Bzip2 = 0x3,
    Lz4 = 0x4,
    Lz4hc = 0x5,
    Xpress = 0x6,
    Zstd = 0x7,
}

/// Checksum algorithm used for block / file integrity. Mirrors `ChecksumType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[allow(missing_docs)]
pub enum ChecksumType {
    NoChecksum = 0x0,
    Crc32c = 0x1,
    XxHash = 0x2,
    #[default]
    XxHash64 = 0x3,
    Xxh3 = 0x4,
}
