//! # st-rs — ForSt Layer 0
//!
//! Rust port of ForSt's public API surface (`include/rocksdb/*.h`) and platform
//! shim (`port/`). This crate contains **trait and type definitions only** —
//! no engine, memtable, SST, or I/O implementation.
//!
//! The module layout mirrors the upstream header layout:
//!
//! | Upstream header                  | This crate            |
//! |----------------------------------|-----------------------|
//! | `include/rocksdb/status.h`       | [`status`]            |
//! | `include/rocksdb/slice.h`        | [`slice`]             |
//! | `include/rocksdb/types.h`        | [`types`]             |
//! | `include/rocksdb/comparator.h`   | [`comparator`]        |
//! | `include/rocksdb/env.h`          | [`env`]               |
//! | `include/rocksdb/file_system.h`  | [`file_system`]       |
//! | `include/rocksdb/iterator.h`     | [`iterator`]          |
//! | `include/rocksdb/snapshot.h`     | [`snapshot`]          |
//! | `include/rocksdb/write_batch.h`  | [`write_batch`]       |
//! | `include/rocksdb/options.h`      | [`options`]           |
//! | `include/rocksdb/db.h`           | [`db`]                |
//! | `include/rocksdb/cache.h`        | [`cache`]             |
//! | `include/rocksdb/table.h`        | [`table`]             |
//! | `include/rocksdb/filter_policy.h`| [`filter_policy`]     |
//! | `include/rocksdb/merge_operator.h`| [`merge_operator`]   |
//! | `include/rocksdb/compaction_filter.h`| [`compaction_filter`]|
//! | `port/port.h`                    | [`port`]              |
//!
//! See `README.md` and `FORST-READING-ORDER.md` (in the ForSt repo) for the
//! full reading plan.

#![warn(missing_docs)]
#![warn(rust_2018_idioms)]
#![forbid(unsafe_code)]

pub mod cache;
pub mod compaction_filter;
pub mod comparator;
pub mod db;
pub mod env;
pub mod file_system;
pub mod filter_policy;
pub mod iterator;
pub mod merge_operator;
pub mod options;
pub mod port;
pub mod slice;
pub mod snapshot;
pub mod status;
pub mod table;
pub mod types;
pub mod write_batch;

// -------- Top-level re-exports of the most common types --------
//
// These mirror the "everyone includes these" headers in upstream RocksDB:
// Status, Slice, SequenceNumber, Options, DB, WriteBatch, Iterator.

pub use crate::comparator::{BytewiseComparator, Comparator, ReverseBytewiseComparator};
pub use crate::db::Db;
pub use crate::iterator::DbIterator;
pub use crate::options::{
    ColumnFamilyOptions, CompactionOptions, DbOptions, FlushOptions, Options, ReadOptions,
    WriteOptions,
};
pub use crate::slice::{Slice, SliceExt};
pub use crate::snapshot::Snapshot;
pub use crate::status::{Code, IoStatus, Result, Severity, Status, SubCode};
pub use crate::types::{ColumnFamilyId, EntryType, FileType, SequenceNumber, ValueType};
pub use crate::write_batch::{WriteBatch, WriteBatchHandler};
