//! Port of the foundational `db/` code from upstream — specifically the
//! files Layer 3 depends on before any of the LSM engine itself can be
//! built: `db/dbformat.{h,cc}` and `db/memtable.{h,cc}`.
//!
//! The rest of `db/` (write path, version set, compaction, iterators,
//! `db_impl.cc`) is Layer 4 and is intentionally absent here. We only
//! pull the types that `memtable/` and `sst/` cannot compile without.

pub mod compaction;
pub mod db_impl;
pub mod db_iter;
pub mod dbformat;
pub mod ingest_external_file;
pub mod log_format;
pub mod log_reader;
pub mod log_writer;
pub mod memtable;
pub mod merging_iterator;
pub mod version_edit;
pub mod version_set;
