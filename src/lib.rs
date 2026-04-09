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
//! | 2     | I/O, buffered files, block cache | *deferred*                                          |
//! | 3     | Memtable & SST format            | *deferred*                                          |
//! | 4     | LSM engine                       | *deferred*                                          |
//! | 5     | Optional features                | *deferred*                                          |
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
pub mod core;
pub mod env;
pub mod ext;
pub mod logging;
pub mod memory;
pub mod monitoring;
pub mod port;
pub mod util;

// ---------- Layer 0 re-exports ----------

pub use crate::api::db::{ColumnFamilyDescriptor, ColumnFamilyHandle, Db, DbOpener, OpenCfResult};
pub use crate::api::iterator::{DbIterator, EmptyIterator, ErrorIterator};
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
