//! Port of `include/rocksdb/options.h`.
//!
//! Upstream splits options into five structs:
//! - `DBOptions`      — properties set per-DB (paths, thread pools, WAL, …)
//! - `ColumnFamilyOptions` — properties set per-CF (comparator, compression, memtable, …)
//! - `Options`        — the convenience superset: `DBOptions + ColumnFamilyOptions`
//! - `ReadOptions`    — per-read knobs (snapshot, fill_cache, …)
//! - `WriteOptions`   — per-write knobs (disable_wal, sync, …)
//! - `FlushOptions`   — per-flush knobs
//! - `CompactionOptions` — per-manual-compaction knobs
//!
//! This module faithfully reproduces those shapes. The field set is the
//! **publicly observable** subset; engine-internal knobs (e.g. the hundred+
//! "allow_concurrent_memtable_write" style fields in upstream's
//! `advanced_options.h`) are deliberately omitted at Layer 0 and will live
//! in an `advanced_options` module alongside the engine.
//!
//! All structs derive [`Clone`] and provide a `Default` impl whose values
//! mirror upstream's `OldDefaults()` — safe, conservative starting points.

use crate::core::types::{ChecksumType, CompressionType};

// ============================================================================
// DBOptions
// ============================================================================

/// Options that apply to the DB as a whole (not per-column-family).
///
/// Mirrors upstream `DBOptions`. Instances can be constructed from defaults
/// and then mutated before passing to `Db::open`.
#[derive(Debug, Clone)]
pub struct DbOptions {
    /// If `true`, the DB will be created if it doesn't exist. Upstream
    /// default: `false`.
    pub create_if_missing: bool,

    /// If `true`, an error is raised if the DB already exists.
    pub error_if_exists: bool,

    /// If `true`, missing column families are created on open. Upstream
    /// default: `false`.
    pub create_missing_column_families: bool,

    /// If `true`, paranoid checks are performed on open; more robust but
    /// slower. Upstream default: `true`.
    pub paranoid_checks: bool,

    /// Number of open files the engine may keep in its cache. `-1` means
    /// "unlimited". Upstream default: `-1`.
    pub max_open_files: i32,

    /// Total amount of WAL bytes across all column families. `0` means
    /// "use the default of 4 * max_write_buffer_number * write_buffer_size".
    pub max_total_wal_size: u64,

    /// Maximum number of concurrent background compaction jobs.
    pub max_background_compactions: i32,

    /// Maximum number of concurrent background flush jobs.
    pub max_background_flushes: i32,

    /// Maximum WAL file size before a new one is rotated in. `0` means
    /// "use the default of write_buffer_size * max_write_buffer_number".
    pub max_log_file_size: u64,

    /// Time-to-live for log files, in seconds. `0` means "never delete".
    pub log_file_time_to_roll: u64,

    /// Maximum number of info-log files to keep.
    pub keep_log_file_num: usize,

    /// How often to flush the info log (nanoseconds).
    pub info_log_level: InfoLogLevel,

    /// If set, the DB uses this directory for WAL. Empty means "same as
    /// the DB data directory".
    pub wal_dir: String,

    /// Whether `fsync` is called on directory changes. Required for
    /// durability on some filesystems.
    pub use_fsync: bool,

    /// Bytes to write before syncing in `WritableFile::Flush`.
    pub bytes_per_sync: u64,

    /// Bytes to write before syncing the WAL.
    pub wal_bytes_per_sync: u64,

    /// If `true`, unreleased snapshots will not block compaction. Upstream
    /// default: `false`.
    pub allow_mmap_reads: bool,

    /// If `true`, use mmap for writes. Dangerous unless you know your
    /// filesystem's semantics. Upstream default: `false`.
    pub allow_mmap_writes: bool,

    /// If `true`, direct I/O is used for reads.
    pub use_direct_reads: bool,

    /// If `true`, direct I/O is used for flushes and compactions.
    pub use_direct_io_for_flush_and_compaction: bool,

    /// Maximum number of write buffer manager memory, in bytes.
    pub db_write_buffer_size: usize,

    /// Comparator name to sanity-check at open time. Empty means "any".
    pub expected_comparator_name: String,
}

impl Default for DbOptions {
    fn default() -> Self {
        Self {
            create_if_missing: false,
            error_if_exists: false,
            create_missing_column_families: false,
            paranoid_checks: true,
            max_open_files: -1,
            max_total_wal_size: 0,
            max_background_compactions: 1,
            max_background_flushes: 1,
            max_log_file_size: 0,
            log_file_time_to_roll: 0,
            keep_log_file_num: 1000,
            info_log_level: InfoLogLevel::Info,
            wal_dir: String::new(),
            use_fsync: false,
            bytes_per_sync: 0,
            wal_bytes_per_sync: 0,
            allow_mmap_reads: false,
            allow_mmap_writes: false,
            use_direct_reads: false,
            use_direct_io_for_flush_and_compaction: false,
            db_write_buffer_size: 0,
            expected_comparator_name: String::new(),
        }
    }
}

/// Minimum level that will actually be written to the info log.
/// Matches upstream `InfoLogLevel`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[allow(missing_docs)]
pub enum InfoLogLevel {
    Debug,
    #[default]
    Info,
    Warn,
    Error,
    Fatal,
    Header,
}

// ============================================================================
// ColumnFamilyOptions
// ============================================================================

/// Options that apply to a single column family. Mirrors
/// `ColumnFamilyOptions`. These can be changed per-column-family at open
/// time and partially at runtime via `Db::set_options`.
#[derive(Debug, Clone)]
pub struct ColumnFamilyOptions {
    /// Comparator name. Must match the comparator used to create this CF
    /// or opening will fail.
    pub comparator_name: String,

    /// Name of a merge operator. Empty means "no merge operator".
    pub merge_operator_name: String,

    /// Name of a compaction filter. Empty means "no compaction filter".
    pub compaction_filter_name: String,

    /// Size of each memtable before it becomes immutable. Default 64 MiB.
    pub write_buffer_size: usize,

    /// Maximum number of memtables (both mutable and immutable) the CF
    /// may hold before writes stall. Default: 2.
    pub max_write_buffer_number: i32,

    /// Maximum number of memtables that may be merged on flush. Default: 1.
    pub min_write_buffer_number_to_merge: i32,

    /// Compression algorithm for data blocks in SSTs below the bottommost
    /// level. Default: `CompressionType::Snappy`.
    pub compression: CompressionType,

    /// Compression for the bottommost level. `None` means "same as
    /// `compression`".
    pub bottommost_compression: Option<CompressionType>,

    /// Maximum L0 files before flushes are slowed down. Default: 20.
    pub level0_slowdown_writes_trigger: i32,

    /// Maximum L0 files before writes are halted. Default: 36.
    pub level0_stop_writes_trigger: i32,

    /// Maximum L0 files before a compaction is triggered. Default: 4.
    pub level0_file_num_compaction_trigger: i32,

    /// Target size of files at the "base" level (L1). Default: 64 MiB.
    pub target_file_size_base: u64,

    /// Multiplier for target file size at each subsequent level.
    pub target_file_size_multiplier: i32,

    /// Total bytes at L1. Default: 256 MiB.
    pub max_bytes_for_level_base: u64,

    /// Multiplier for per-level size.
    pub max_bytes_for_level_multiplier: f64,

    /// Number of levels in the LSM tree. Default: 7.
    pub num_levels: i32,

    /// Disable automatic compactions. Useful for bulk loading.
    pub disable_auto_compactions: bool,
}

impl Default for ColumnFamilyOptions {
    fn default() -> Self {
        Self {
            comparator_name: "leveldb.BytewiseComparator".to_string(),
            merge_operator_name: String::new(),
            compaction_filter_name: String::new(),
            write_buffer_size: 64 * 1024 * 1024,
            max_write_buffer_number: 2,
            min_write_buffer_number_to_merge: 1,
            compression: CompressionType::Snappy,
            bottommost_compression: None,
            level0_slowdown_writes_trigger: 20,
            level0_stop_writes_trigger: 36,
            level0_file_num_compaction_trigger: 4,
            target_file_size_base: 64 * 1024 * 1024,
            target_file_size_multiplier: 1,
            max_bytes_for_level_base: 256 * 1024 * 1024,
            max_bytes_for_level_multiplier: 10.0,
            num_levels: 7,
            disable_auto_compactions: false,
        }
    }
}

// ============================================================================
// Options (the union)
// ============================================================================

/// Convenience superset of [`DbOptions`] and [`ColumnFamilyOptions`], used
/// by `Db::open` for the single-column-family case.
#[derive(Debug, Clone, Default)]
pub struct Options {
    /// The DB-wide options.
    pub db: DbOptions,
    /// The column-family options to apply to the default CF.
    pub cf: ColumnFamilyOptions,
}

impl Options {
    /// Convenience: set both `create_if_missing` and
    /// `create_missing_column_families`. Matches upstream
    /// `Options::PrepareForBulkLoad` in spirit.
    #[must_use]
    pub fn create_if_missing(mut self, yes: bool) -> Self {
        self.db.create_if_missing = yes;
        self.db.create_missing_column_families = yes;
        self
    }
}

// ============================================================================
// Per-call options
// ============================================================================

/// Per-read options. Mirrors `ReadOptions`.
#[derive(Debug, Clone, Default)]
pub struct ReadOptions {
    /// Read from the point in time pinned by this snapshot. Stored as a
    /// sequence number; `None` means "current".
    pub snapshot: Option<u64>,

    /// If `false`, block reads will not populate the block cache. Default: `true`.
    pub fill_cache: bool,

    /// Verify checksums on every block read. Default: `true`.
    pub verify_checksums: bool,

    /// Do not read past this upper bound during iteration. Half-open.
    pub iterate_upper_bound: Option<Vec<u8>>,

    /// Do not read before this lower bound during iteration.
    pub iterate_lower_bound: Option<Vec<u8>>,

    /// If `true`, the iterator's returned keys stay pinned for the
    /// lifetime of the iterator (never invalidated by `next`/`prev`).
    pub pin_data: bool,

    /// If `true`, return immediately with `Incomplete` rather than blocking
    /// on I/O.
    pub read_tier_memtable_only: bool,
}

impl ReadOptions {
    /// Construct [`ReadOptions`] with upstream defaults: `fill_cache = true`,
    /// `verify_checksums = true`, no snapshot, no bounds.
    pub fn new() -> Self {
        Self {
            snapshot: None,
            fill_cache: true,
            verify_checksums: true,
            iterate_upper_bound: None,
            iterate_lower_bound: None,
            pin_data: false,
            read_tier_memtable_only: false,
        }
    }
}

/// Per-write options. Mirrors `WriteOptions`.
#[derive(Debug, Clone, Default)]
pub struct WriteOptions {
    /// `fsync` the WAL before returning. Durable but slow.
    pub sync: bool,
    /// Skip the WAL entirely. Very fast but data is lost on crash.
    pub disable_wal: bool,
    /// If `true`, the write may fail with `Incomplete` rather than
    /// blocking under write-stall conditions.
    pub no_slowdown: bool,
    /// If `true`, the write is treated as a low-priority compaction write.
    pub low_pri: bool,
}

/// Per-flush options. Mirrors `FlushOptions`.
#[derive(Debug, Clone)]
pub struct FlushOptions {
    /// Block until the flush completes.
    pub wait: bool,
    /// Allow write stalls during the flush.
    pub allow_write_stall: bool,
}

impl Default for FlushOptions {
    fn default() -> Self {
        Self {
            wait: true,
            allow_write_stall: false,
        }
    }
}

/// Per-compaction options. Mirrors `CompactionOptions`.
#[derive(Debug, Clone, Default)]
pub struct CompactionOptions {
    /// Compression to use for output SSTs. `None` means "inherit from CF".
    pub compression: Option<CompressionType>,
    /// Output file size. `0` means "inherit from CF".
    pub output_file_size_limit: u64,
    /// Maximum number of threads used for sub-compactions. `0` means 1.
    pub max_subcompactions: u32,
}

/// Per-file I/O options. Mirrors the intersection of `EnvOptions` and
/// `FileOptions` in upstream — anything the FileSystem layer may legitimately
/// care about at Layer 0.
#[derive(Debug, Clone, Default)]
pub struct FileOptions {
    /// Use mmap for reads when possible.
    pub use_mmap_reads: bool,
    /// Use mmap for writes when possible.
    pub use_mmap_writes: bool,
    /// Use direct I/O for reads.
    pub use_direct_reads: bool,
    /// Use direct I/O for writes.
    pub use_direct_writes: bool,
    /// Bytes to write before syncing.
    pub bytes_per_sync: u64,
    /// Fall back to `fsync` after `fdatasync` if the filesystem requires it.
    pub strict_bytes_per_sync: bool,
    /// Checksum type to apply for handoff verification.
    pub handoff_checksum_type: ChecksumType,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_options_are_conservative() {
        let o = Options::default();
        assert!(!o.db.create_if_missing);
        assert!(o.db.paranoid_checks);
        assert_eq!(o.cf.num_levels, 7);
        assert_eq!(o.cf.write_buffer_size, 64 * 1024 * 1024);
    }

    #[test]
    fn create_if_missing_builder() {
        let o = Options::default().create_if_missing(true);
        assert!(o.db.create_if_missing);
        assert!(o.db.create_missing_column_families);
    }

    #[test]
    fn read_options_defaults() {
        let r = ReadOptions::new();
        assert!(r.fill_cache);
        assert!(r.verify_checksums);
        assert!(r.snapshot.is_none());
    }
}
