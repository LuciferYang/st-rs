//! User-facing database API.
//!
//! Everything a user of the engine interacts with directly: the `Db` trait,
//! iterators, write batches, options structs, and snapshots. These roughly
//! correspond to the set of upstream headers a new user is told to include
//! first.

pub mod db;
pub mod iterator;
pub mod options;
pub mod snapshot;
pub mod write_batch;
