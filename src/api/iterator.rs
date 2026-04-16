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

//! Port of `include/rocksdb/iterator.h`.
//!
//! The RocksDB iterator is **seekable** and **bidirectional**, which does
//! not fit `core::iter::Iterator`. To avoid the naming collision and the
//! semantic confusion that would follow from `impl Iterator for DbIterator`,
//! this trait is named [`DbIterator`].
//!
//! Invariants:
//! - An iterator is either *positioned* on a `(key, value)` pair, or *invalid*.
//! - [`DbIterator::valid`] is the single source of truth.
//! - [`DbIterator::key`] and [`DbIterator::value`] may only be called when
//!   [`DbIterator::valid`] returns `true`. Implementations may panic otherwise.
//! - [`DbIterator::status`] returns the first error encountered; the iterator
//!   becomes permanently invalid once any error has been observed.
//!
//! The lifetime on [`DbIterator::key`] and [`DbIterator::value`] ties the
//! borrow to `&self`, matching the upstream rule that "the returned slice is
//! valid until the next mutation of the iterator."

use crate::core::status::{Result, Status};

/// Seekable bidirectional cursor over a sorted (key, value) source.
///
/// Implementations must be `Send` so the engine can pass them across
/// compaction/flush thread boundaries. They need not be `Sync` — the
/// upstream contract is "not safe for concurrent use from multiple threads."
pub trait DbIterator: Send {
    /// Returns `true` iff the iterator is positioned on a valid entry.
    /// Always returns `false` if [`Self::status`] is not OK.
    fn valid(&self) -> bool;

    /// Position at the first key in the source. After this call,
    /// [`Self::valid`] is true iff the source is non-empty.
    fn seek_to_first(&mut self);

    /// Position at the last key in the source.
    fn seek_to_last(&mut self);

    /// Position at the first key `>= target`. `target` does not include any
    /// timestamp suffix.
    fn seek(&mut self, target: &[u8]);

    /// Position at the last key `<= target`.
    fn seek_for_prev(&mut self, target: &[u8]);

    /// Advance to the next entry. Requires [`Self::valid`] to be true.
    fn next(&mut self);

    /// Retreat to the previous entry. Requires [`Self::valid`] to be true.
    fn prev(&mut self);

    /// Current key. Valid only while the iterator is positioned; the slice
    /// becomes invalid on the next `seek`/`next`/`prev` call.
    fn key(&self) -> &[u8];

    /// Current value.
    fn value(&self) -> &[u8];

    /// Returns the first error observed, or `Ok(())` if none. The iterator
    /// becomes permanently invalid once an error has been observed.
    fn status(&self) -> Result<()>;

    /// Refresh the iterator to see the latest DB state. The iterator
    /// becomes invalid after this call — callers must re-seek. Default:
    /// returns `NotSupported`, matching upstream.
    fn refresh(&mut self) -> Result<()> {
        Err(Status::not_supported("Refresh() is not supported"))
    }
}

/// Trivial iterator that yields nothing. Matches upstream `NewEmptyIterator()`.
#[derive(Debug, Default)]
pub struct EmptyIterator;

impl DbIterator for EmptyIterator {
    fn valid(&self) -> bool {
        false
    }
    fn seek_to_first(&mut self) {}
    fn seek_to_last(&mut self) {}
    fn seek(&mut self, _target: &[u8]) {}
    fn seek_for_prev(&mut self, _target: &[u8]) {}
    fn next(&mut self) {
        panic!("next() called on invalid iterator")
    }
    fn prev(&mut self) {
        panic!("prev() called on invalid iterator")
    }
    fn key(&self) -> &[u8] {
        panic!("key() called on invalid iterator")
    }
    fn value(&self) -> &[u8] {
        panic!("value() called on invalid iterator")
    }
    fn status(&self) -> Result<()> {
        Ok(())
    }
}

/// Iterator that starts in a permanently errored state. Matches upstream
/// `NewErrorIterator(status)`.
#[derive(Debug)]
pub struct ErrorIterator {
    status: Status,
}

impl ErrorIterator {
    /// Create a new error iterator carrying the given status.
    pub fn new(status: Status) -> Self {
        Self { status }
    }
}

impl DbIterator for ErrorIterator {
    fn valid(&self) -> bool {
        false
    }
    fn seek_to_first(&mut self) {}
    fn seek_to_last(&mut self) {}
    fn seek(&mut self, _target: &[u8]) {}
    fn seek_for_prev(&mut self, _target: &[u8]) {}
    fn next(&mut self) {
        panic!("next() called on error iterator")
    }
    fn prev(&mut self) {
        panic!("prev() called on error iterator")
    }
    fn key(&self) -> &[u8] {
        panic!("key() called on error iterator")
    }
    fn value(&self) -> &[u8] {
        panic!("value() called on error iterator")
    }
    fn status(&self) -> Result<()> {
        Err(self.status.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_iterator_never_valid() {
        let mut it = EmptyIterator;
        assert!(!it.valid());
        it.seek_to_first();
        assert!(!it.valid());
        it.seek(b"x");
        assert!(!it.valid());
        assert!(it.status().is_ok());
    }

    #[test]
    fn error_iterator_reports_status() {
        let it = ErrorIterator::new(Status::corruption("bad block"));
        assert!(!it.valid());
        assert!(it.status().unwrap_err().is_corruption());
    }

    // ---- EmptyIterator extended coverage ----

    #[test]
    fn empty_iterator_seek_to_last_stays_invalid() {
        let mut it = EmptyIterator;
        it.seek_to_last();
        assert!(!it.valid());
    }

    #[test]
    fn empty_iterator_seek_for_prev_stays_invalid() {
        let mut it = EmptyIterator;
        it.seek_for_prev(b"target");
        assert!(!it.valid());
    }

    #[test]
    fn empty_iterator_status_is_ok() {
        let it = EmptyIterator;
        assert!(it.status().is_ok());
    }

    #[test]
    fn empty_iterator_refresh_not_supported() {
        let mut it = EmptyIterator;
        let result = it.refresh();
        assert!(result.is_err());
        assert!(result.unwrap_err().is_not_supported());
    }

    #[test]
    #[should_panic(expected = "next() called on invalid iterator")]
    fn empty_iterator_next_panics() {
        let mut it = EmptyIterator;
        it.next();
    }

    #[test]
    #[should_panic(expected = "prev() called on invalid iterator")]
    fn empty_iterator_prev_panics() {
        let mut it = EmptyIterator;
        it.prev();
    }

    #[test]
    #[should_panic(expected = "key() called on invalid iterator")]
    fn empty_iterator_key_panics() {
        let it = EmptyIterator;
        let _ = it.key();
    }

    #[test]
    #[should_panic(expected = "value() called on invalid iterator")]
    fn empty_iterator_value_panics() {
        let it = EmptyIterator;
        let _ = it.value();
    }

    // ---- ErrorIterator extended coverage ----

    #[test]
    fn error_iterator_seek_to_first_stays_invalid() {
        let mut it = ErrorIterator::new(Status::io_error("disk fail"));
        it.seek_to_first();
        assert!(!it.valid());
    }

    #[test]
    fn error_iterator_seek_to_last_stays_invalid() {
        let mut it = ErrorIterator::new(Status::io_error("disk fail"));
        it.seek_to_last();
        assert!(!it.valid());
    }

    #[test]
    fn error_iterator_seek_stays_invalid() {
        let mut it = ErrorIterator::new(Status::io_error("disk fail"));
        it.seek(b"any");
        assert!(!it.valid());
    }

    #[test]
    fn error_iterator_seek_for_prev_stays_invalid() {
        let mut it = ErrorIterator::new(Status::io_error("disk fail"));
        it.seek_for_prev(b"any");
        assert!(!it.valid());
    }

    #[test]
    fn error_iterator_status_preserves_error_details() {
        let it = ErrorIterator::new(Status::not_found("key missing"));
        let err = it.status().unwrap_err();
        assert!(err.is_not_found());
        assert_eq!(err.message.as_deref(), Some("key missing"));
    }

    #[test]
    fn error_iterator_refresh_not_supported() {
        let mut it = ErrorIterator::new(Status::corruption("bad"));
        let result = it.refresh();
        assert!(result.is_err());
        assert!(result.unwrap_err().is_not_supported());
    }

    #[test]
    #[should_panic(expected = "next() called on error iterator")]
    fn error_iterator_next_panics() {
        let mut it = ErrorIterator::new(Status::corruption("bad"));
        it.next();
    }

    #[test]
    #[should_panic(expected = "prev() called on error iterator")]
    fn error_iterator_prev_panics() {
        let mut it = ErrorIterator::new(Status::corruption("bad"));
        it.prev();
    }

    #[test]
    #[should_panic(expected = "key() called on error iterator")]
    fn error_iterator_key_panics() {
        let it = ErrorIterator::new(Status::corruption("bad"));
        let _ = it.key();
    }

    #[test]
    #[should_panic(expected = "value() called on error iterator")]
    fn error_iterator_value_panics() {
        let it = ErrorIterator::new(Status::corruption("bad"));
        let _ = it.value();
    }

    // ---- Send trait verification ----

    #[test]
    fn empty_iterator_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<EmptyIterator>();
    }

    #[test]
    fn error_iterator_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<ErrorIterator>();
    }
}
