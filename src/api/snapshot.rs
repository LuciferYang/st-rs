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

//! Port of `include/rocksdb/snapshot.h`.
//!
//! A [`Snapshot`] is a handle to a point-in-time view of a DB, defined by
//! the sequence number that was current at the moment [`crate::db::Db::snapshot`]
//! was called. Reads taken with `ReadOptions::snapshot` set to this value see
//! exactly the data that existed at that sequence number — the engine retains
//! any older versions of keys until the snapshot is released.
//!
//! Upstream uses an abstract `Snapshot` class with a single virtual method
//! `GetSequenceNumber()`. We follow the same minimal shape, plus a simple
//! concrete implementation for tests.

use crate::core::types::SequenceNumber;

/// A point-in-time view of a DB. Borrowing a `&dyn Snapshot` from the DB
/// prevents the sequence number it pins from being garbage-collected.
///
/// Implementations must be thread-safe.
pub trait Snapshot: Send + Sync {
    /// The sequence number pinned by this snapshot.
    fn sequence_number(&self) -> SequenceNumber;

    /// The wall-clock time at which this snapshot was taken, in nanoseconds
    /// since the UNIX epoch, if known. Mirrors upstream `GetUnixTime()`;
    /// default returns 0 meaning "unknown".
    fn unix_time_nanos(&self) -> u64 {
        0
    }

    /// The timestamp at which this snapshot was taken in the engine's
    /// timestamp domain, if timestamps are in use. Returns `None` otherwise.
    fn timestamp(&self) -> Option<&[u8]> {
        None
    }
}

/// A trivial `Snapshot` implementation that just wraps a sequence number.
/// Intended for unit tests and for simple engine implementations that do
/// not support wall-clock snapshot metadata.
#[derive(Debug, Clone, Copy)]
pub struct SimpleSnapshot {
    seq: SequenceNumber,
}

impl SimpleSnapshot {
    /// Create a new snapshot pinning the given sequence number.
    pub const fn new(seq: SequenceNumber) -> Self {
        Self { seq }
    }
}

impl Snapshot for SimpleSnapshot {
    fn sequence_number(&self) -> SequenceNumber {
        self.seq
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_snapshot_exposes_sequence() {
        let s = SimpleSnapshot::new(42);
        let r: &dyn Snapshot = &s;
        assert_eq!(r.sequence_number(), 42);
        assert_eq!(r.unix_time_nanos(), 0);
        assert!(r.timestamp().is_none());
    }
}
