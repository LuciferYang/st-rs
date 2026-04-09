//! Port of the foundational `db/` code from upstream — specifically the
//! files Layer 3 depends on before any of the LSM engine itself can be
//! built: `db/dbformat.{h,cc}` and `db/memtable.{h,cc}`.
//!
//! The rest of `db/` (write path, version set, compaction, iterators,
//! `db_impl.cc`) is Layer 4 and is intentionally absent here. We only
//! pull the types that `memtable/` and `sst/` cannot compile without.

pub mod dbformat;
pub mod memtable;
