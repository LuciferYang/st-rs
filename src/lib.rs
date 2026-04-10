//! # st-rs — Rust port of ForSt
//!
//! `st-rs` ports the [ForSt](https://github.com/ververica/ForSt) project —
//! a RocksDB fork for Apache Flink state backends — from C++ to idiomatic
//! Rust. The port is organised by the same layering used in
//! `FORST-READING-ORDER.md`:
//!
//! | Layer | Purpose                          | Modules in this crate                               |
//! |------:|----------------------------------|-----------------------------------------------------|
//! | 0     | Public API + platform shim       | [`core`], [`env`], [`api`], [`ext`], [`port`]       |
//! | 1     | Foundation utilities             | [`util`], [`memory`], [`logging`], [`monitoring`]   |
//! | 2     | I/O, buffered files, block cache | [`file`], [`cache`], `env::posix`, `env::thread_pool` |
//! | 3a    | InternalKey, memtable, SST format primitives | [`db`], [`memtable`], [`sst`]                       |
//! | 3b    | SST builder/reader, bloom filter, CRC32C | `sst::block_based::table_*`, `util::crc32c`     |
//! | 4a    | Minimum-viable engine: WAL + DbImpl | `db::log_*`, `db::db_impl`                          |
//! | 4b    | SST iterator, merging iter, DbIterator, compaction | `sst::block_based::sst_iterator`, `db::merging_iterator`, `db::db_iter`, `db::compaction` |
//! | 4c    | Background flush, immutable memtable, atomic CURRENT, util::heap | `db::db_impl` (refactored), [`util::heap`]                |
//! | 4d    | Internal-key SSTs (BE-inverted trailer) enabling MVCC in the SST format | `db::dbformat`, `db::db_impl` flush/get, `db::compaction` |
//! | 4e    | Explicit snapshots + snapshot-aware reads + retention in compaction | `db::db_impl` (`snapshot`, `get_at`, `iter_at`), `db::compaction` |
//! | 4f    | Background compaction via a second thread-pool worker | `db::db_impl` (`schedule_compaction`, `wait_for_pending_work`) |
//! | 4g    | Multi-level layout (L0 overlapping, L1 non-overlapping) | `db::db_impl` (l0/l1 fields, binary-search read, overlap picker) |
//! | 5     | Optional features (checkpoint)   | [`utilities::checkpoint`]                            |
//! | 6     | Tools & stress                   | *deferred*                                          |
//! | 7     | ForSt-specific Flink bridge      | *deferred*                                          |
//!
//! ## Module organisation
//!
//! The crate groups files by topic, not by the upstream file name:
//!
//! | Module          | Corresponds to upstream                                                       |
//! |-----------------|--------------------------------------------------------------------------------|
//! | [`core`]        | `status.h`, `slice.h`, `types.h`                                               |
//! | [`env`]         | `env.h`, `file_system.h`                                                       |
//! | [`api`]         | `db.h`, `iterator.h`, `write_batch.h`, `options.h`, `snapshot.h`               |
//! | [`ext`]         | `comparator.h`, `cache.h`, `table.h`, `filter_policy.h`, `merge_operator.h`, `compaction_filter.h` |
//! | [`port`]        | `port/port.h`                                                                  |
//! | [`util`]        | `util/coding.*`, `util/hash.*`, `util/random.*`, `util/string_util.*`          |
//! | [`memory`]      | `memory/arena.*`, `memory/allocator.h`                                         |
//! | [`logging`]     | `logging/logging.h`, `logging/auto_roll_logger.h`                              |
//! | [`monitoring`]  | `monitoring/statistics_impl.*`, `monitoring/histogram.h`, `monitoring/perf_context_imp.h` |
//!
//! Everything that is commonly used is re-exported from the crate root so
//! users can write `use st_rs::{Status, Slice, Db, WriteBatch, ...}` without
//! knowing the internal module layout.

#![warn(missing_docs)]
#![warn(rust_2018_idioms)]
#![deny(unsafe_code)]

pub mod api;
pub mod cache;
pub mod core;
pub mod db;
pub mod env;
pub mod ext;
pub mod file;
pub mod logging;
pub mod memory;
pub mod memtable;
pub mod monitoring;
pub mod port;
pub mod sst;
pub mod util;
pub mod utilities;

// ---------- Layer 0 re-exports ----------

pub use crate::api::db::{ColumnFamilyDescriptor, ColumnFamilyHandle, Db, DbOpener, OpenCfResult};
// `DbIterator` (the Layer 0 abstract iterator trait) is reachable
// via `st_rs::api::iterator::DbIterator`. The crate root re-exports
// the **concrete** Layer 4b iterator struct under the same name —
// see further down. Most users never need the trait directly.
pub use crate::api::iterator::{EmptyIterator, ErrorIterator};
pub use crate::api::options::{
    ColumnFamilyOptions, CompactionOptions, DbOptions, FileOptions, FlushOptions, InfoLogLevel,
    Options, ReadOptions, WriteOptions,
};
pub use crate::api::snapshot::{SimpleSnapshot, Snapshot};
pub use crate::api::write_batch::{Record, WriteBatch, WriteBatchHandler};
pub use crate::core::slice::{Slice, SliceExt};
pub use crate::core::status::{Code, IoStatus, Result, Severity, Status, SubCode};
pub use crate::core::types::{
    ChecksumType, ColumnFamilyId, CompressionType, EntryType, FileType, SequenceNumber,
    Temperature, ValueType, WriteStallCause, WriteStallCondition,
};
pub use crate::env::env_trait::{
    Clock, CompositeEnv, Env, Priority, ScheduleHandle, SystemClockImpl, ThreadPool,
};
pub use crate::env::file_system::{
    AccessPattern, FileAttributes, FileLock, FileSystem, FsDirectory, FsRandomAccessFile,
    FsSequentialFile, FsWritableFile, IoOptions, IoPriority, IoType,
};
pub use crate::ext::cache::{Cache, CacheHandle, CachePriority};
pub use crate::ext::compaction_filter::{
    CompactionDecision, CompactionFilter, CompactionFilterFactory,
};
pub use crate::ext::comparator::{BytewiseComparator, Comparator, ReverseBytewiseComparator};
pub use crate::ext::filter_policy::{FilterBitsBuilder, FilterBitsReader, FilterPolicy};
pub use crate::ext::merge_operator::MergeOperator;
pub use crate::ext::table::{IndexType, TableFactory, TableFormatVersion};

// ---------- Layer 1 re-exports ----------

pub use crate::logging::logger::{ConsoleLogger, Logger, LogLevel};
pub use crate::memory::allocator::Allocator;
pub use crate::memory::arena::Arena;
pub use crate::monitoring::histogram::{Histogram, HistogramSnapshot};
pub use crate::monitoring::perf_context::PerfContext;
pub use crate::monitoring::statistics::{Statistics, StatisticsImpl, Ticker};

// ---------- Layer 2 re-exports ----------

pub use crate::cache::lru::{LruCache, LruHandle};
pub use crate::env::thread_pool::StdThreadPool;
#[cfg(unix)]
pub use crate::env::posix::{
    PosixDirectory, PosixFileLock, PosixFileSystem, PosixRandomAccessFile, PosixSequentialFile,
    PosixWritableFile,
};
pub use crate::file::file_util::{copy_file, delete_if_exists, sync_directory};
pub use crate::file::filename::{
    make_blob_file_name, make_current_file_name, make_descriptor_file_name,
    make_identity_file_name, make_info_log_file_name, make_lock_file_name,
    make_old_info_log_file_name, make_options_file_name, make_table_file_name,
    make_temp_file_name, make_wal_file_name, parse_file_name,
};
pub use crate::file::random_access_file_reader::RandomAccessFileReader;
pub use crate::file::sequence_file_reader::SequentialFileReader;
pub use crate::file::writable_file_writer::WritableFileWriter;

// ---------- Layer 3a re-exports ----------

pub use crate::db::dbformat::{
    byte_to_value_type, pack_seq_and_type, unpack_seq_and_type, value_type_byte, InternalKey,
    InternalKeyComparator, LookupKey, ParsedInternalKey, MAX_SEQUENCE_NUMBER, VALUE_TYPE_FOR_SEEK,
};
pub use crate::db::memtable::{MemTable, MemTableGetResult};
pub use crate::memtable::memtable_rep::{MemTableRep, MemTableRepIterator};
pub use crate::memtable::skip_list::{SkipList, SkipListIter, BRANCHING_FACTOR, MAX_HEIGHT};
pub use crate::sst::block_based::block::{Block, BlockIter};
pub use crate::sst::block_based::block_builder::{BlockBuilder, DEFAULT_BLOCK_RESTART_INTERVAL};
pub use crate::sst::format::{
    put_block_trailer, verify_block_trailer, BlockHandle, Footer,
    BLOCK_BASED_TABLE_MAGIC_NUMBER, BLOCK_TRAILER_SIZE, DEFAULT_FORMAT_VERSION,
    MAX_BLOCK_HANDLE_ENCODED_LENGTH,
};

// ---------- Layer 3b re-exports ----------

pub use crate::sst::block_based::filter_block::{
    BloomFilterBuilder, BloomFilterPolicy, BloomFilterReader, DEFAULT_BITS_PER_KEY,
    FILTER_METAINDEX_KEY,
};
pub use crate::sst::block_based::table_builder::{
    BlockBasedTableBuilder, BlockBasedTableOptions,
};
pub use crate::sst::block_based::table_reader::BlockBasedTableReader;

// ---------- Layer 4a re-exports ----------

pub use crate::db::db_impl::DbImpl;
pub use crate::db::log_format::{RecordType, BLOCK_SIZE as WAL_BLOCK_SIZE, HEADER_SIZE as WAL_HEADER_SIZE};
pub use crate::db::log_reader::LogReader;
pub use crate::db::log_writer::LogWriter;

// ---------- Layer 4b re-exports ----------

pub use crate::db::compaction::{pick_compaction, pick_compaction_batch, CompactionJob};
pub use crate::db::db_iter::{DbIterator, MemtableUserKeyIter, SstUserKeyIter};
pub use crate::db::merging_iterator::{MergingIterator, UserKeyIter};
pub use crate::sst::block_based::sst_iterator::SstIter;

// ---------- Layer 4c re-exports ----------

pub use crate::util::heap::BinaryHeap;

// ---------- Layer 4e re-exports ----------

pub use crate::db::db_impl::DbSnapshot;
pub use crate::db::db_iter::READ_AT_LATEST;

// ---------- Layer 5 re-exports ----------

pub use crate::utilities::checkpoint::create_checkpoint;
