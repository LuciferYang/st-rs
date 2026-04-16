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

//! Port of `include/rocksdb/status.h`.
//!
//! `Status` is ForSt's universal error type, carrying a code, a sub-code, a
//! severity, a message, and a few flags (retryable, data-loss). Unlike
//! `std::io::Error`, it is cheap to copy when there is no message attached
//! (the common success path), and its code/sub-code pair encodes a richer
//! taxonomy than `ErrorKind`.
//!
//! In the Rust port, [`Status`] is used as both a standalone value and as the
//! error variant of [`Result`]. `Status::ok()` is represented by any
//! `Status` with `code == Code::Ok`, and the idiomatic way to propagate
//! failures is to return `Result<T>` and use `?`.

use core::fmt;

/// Short-hand `Result` where the error type is [`Status`].
pub type Result<T> = core::result::Result<T, Status>;

/// Type alias for `Status` used specifically in the I/O layer.
///
/// Upstream RocksDB uses a separate `IOStatus` subclass for stricter type
/// safety on the file-system boundary. Rust has no inheritance, so a newtype
/// would be pure boilerplate at this layer — we use an alias instead, and
/// follow the convention of returning `IoStatus` from [`crate::file_system`]
/// methods purely as documentation.
pub type IoStatus = Status;

/// The primary error category.
///
/// Mirrors `enum Status::Code` in `include/rocksdb/status.h`. New variants
/// may only be appended at the end, to preserve the numeric values for
/// on-disk compatibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Code {
    /// The operation succeeded.
    Ok = 0,
    /// A lookup found no matching key.
    NotFound = 1,
    /// On-disk data was detected as corrupt (bad checksum, bad format).
    Corruption = 2,
    /// The requested operation is not implemented by this build.
    NotSupported = 3,
    /// An argument failed validation.
    InvalidArgument = 4,
    /// An underlying filesystem operation failed.
    IoError = 5,
    /// A merge operation is still in progress.
    MergeInProgress = 6,
    /// A partial result was returned; see the sub-code for why.
    Incomplete = 7,
    /// The DB is shutting down.
    ShutdownInProgress = 8,
    /// An operation exceeded its timeout budget.
    TimedOut = 9,
    /// The operation was aborted by a higher-level policy (e.g. memory limit).
    Aborted = 10,
    /// A contended resource could not be acquired right now.
    Busy = 11,
    /// A TTL-scoped entry has expired.
    Expired = 12,
    /// The operation should be retried.
    TryAgain = 13,
    /// The requested compaction is too large to schedule.
    CompactionTooLarge = 14,
    /// The target column family has been dropped.
    ColumnFamilyDropped = 15,
}

/// Refinement of [`Code`] for cases where a single code would be ambiguous.
///
/// Mirrors `enum Status::SubCode`. Only a subset of `(Code, SubCode)` pairs
/// is meaningful; check the `Status::is_*` helpers before reading the
/// sub-code directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
#[allow(missing_docs)]
pub enum SubCode {
    None = 0,
    MutexTimeout = 1,
    LockTimeout = 2,
    LockLimit = 3,
    NoSpace = 4,
    Deadlock = 5,
    StaleFile = 6,
    MemoryLimit = 7,
    SpaceLimit = 8,
    PathNotFound = 9,
    MergeOperandsInsufficientCapacity = 10,
    ManualCompactionPaused = 11,
    Overwritten = 12,
    TxnNotPrepared = 13,
    IoFenced = 14,
    MergeOperatorFailed = 15,
    MergeOperandThresholdExceeded = 16,
}

/// How severe the error is, for crash / degraded-mode decision making.
///
/// Mirrors `enum Status::Severity`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(u8)]
pub enum Severity {
    /// Success carries this severity.
    #[default]
    NoError = 0,
    /// Recoverable failure; the DB can continue but some operations degraded.
    SoftError = 1,
    /// Hard failure; writes should stop until the user resolves it.
    HardError = 2,
    /// Fatal: the DB instance should be closed.
    FatalError = 3,
    /// Unrecoverable: on-disk data is lost.
    UnrecoverableError = 4,
}

/// The result of a ForSt operation.
///
/// An OK status (the default) is a zero-message value. Non-OK statuses carry
/// an optional descriptive message, a sub-code, a severity, and two flags:
/// `retryable` (the caller may re-attempt the operation) and `data_loss`
/// (the error implies permanent data loss).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Status {
    /// Primary error category.
    pub code: Code,
    /// Refinement of [`Self::code`].
    pub subcode: SubCode,
    /// How severe the error is.
    pub severity: Severity,
    /// Whether re-attempting may succeed.
    pub retryable: bool,
    /// Whether the error implies permanent data loss.
    pub data_loss: bool,
    /// Scope tag used by upstream to distinguish error sources.
    pub scope: u8,
    /// Optional human-readable message. `None` for all OK statuses.
    pub message: Option<String>,
}

impl Default for Status {
    fn default() -> Self {
        Status::ok()
    }
}

impl Status {
    /// Returns a success status.
    #[inline]
    pub const fn ok() -> Self {
        Self {
            code: Code::Ok,
            subcode: SubCode::None,
            severity: Severity::NoError,
            retryable: false,
            data_loss: false,
            scope: 0,
            message: None,
        }
    }

    /// Returns `true` iff `self.code == Code::Ok`.
    #[inline]
    pub const fn is_ok(&self) -> bool {
        matches!(self.code, Code::Ok)
    }

    /// Convenience constructor for a failure with a given code and message.
    pub fn new(code: Code, msg: impl Into<String>) -> Self {
        Self {
            code,
            subcode: SubCode::None,
            severity: Severity::NoError,
            retryable: false,
            data_loss: false,
            scope: 0,
            message: Some(msg.into()),
        }
    }

    /// Convenience constructor for a failure with a given code and sub-code.
    pub const fn with_subcode(code: Code, subcode: SubCode) -> Self {
        Self {
            code,
            subcode,
            severity: Severity::NoError,
            retryable: false,
            data_loss: false,
            scope: 0,
            message: None,
        }
    }

    // -- Factories for each code (mirroring `Status::NotFound`, `::Corruption`, …) --

    /// `Code::NotFound` with an optional message.
    pub fn not_found(msg: impl Into<String>) -> Self {
        Self::new(Code::NotFound, msg)
    }
    /// `Code::Corruption` with a message.
    pub fn corruption(msg: impl Into<String>) -> Self {
        Self::new(Code::Corruption, msg)
    }
    /// `Code::NotSupported` with a message.
    pub fn not_supported(msg: impl Into<String>) -> Self {
        Self::new(Code::NotSupported, msg)
    }
    /// `Code::InvalidArgument` with a message.
    pub fn invalid_argument(msg: impl Into<String>) -> Self {
        Self::new(Code::InvalidArgument, msg)
    }
    /// `Code::IoError` with a message.
    pub fn io_error(msg: impl Into<String>) -> Self {
        Self::new(Code::IoError, msg)
    }
    /// `Code::MergeInProgress` with a message.
    pub fn merge_in_progress(msg: impl Into<String>) -> Self {
        Self::new(Code::MergeInProgress, msg)
    }
    /// `Code::Incomplete` with a message.
    pub fn incomplete(msg: impl Into<String>) -> Self {
        Self::new(Code::Incomplete, msg)
    }
    /// `Code::ShutdownInProgress` with a message.
    pub fn shutdown_in_progress(msg: impl Into<String>) -> Self {
        Self::new(Code::ShutdownInProgress, msg)
    }
    /// `Code::TimedOut` with a message.
    pub fn timed_out(msg: impl Into<String>) -> Self {
        Self::new(Code::TimedOut, msg)
    }
    /// `Code::Aborted` with a message.
    pub fn aborted(msg: impl Into<String>) -> Self {
        Self::new(Code::Aborted, msg)
    }
    /// `Code::Busy` with a message.
    pub fn busy(msg: impl Into<String>) -> Self {
        Self::new(Code::Busy, msg)
    }
    /// `Code::Expired` with a message.
    pub fn expired(msg: impl Into<String>) -> Self {
        Self::new(Code::Expired, msg)
    }
    /// `Code::TryAgain` with a message.
    pub fn try_again(msg: impl Into<String>) -> Self {
        Self::new(Code::TryAgain, msg)
    }
    /// `Code::CompactionTooLarge` with a message.
    pub fn compaction_too_large(msg: impl Into<String>) -> Self {
        Self::new(Code::CompactionTooLarge, msg)
    }
    /// `Code::ColumnFamilyDropped` with a message.
    pub fn column_family_dropped(msg: impl Into<String>) -> Self {
        Self::new(Code::ColumnFamilyDropped, msg)
    }

    /// `IOError` with `SubCode::NoSpace`.
    pub const fn no_space() -> Self {
        Self::with_subcode(Code::IoError, SubCode::NoSpace)
    }
    /// `Aborted` with `SubCode::MemoryLimit`.
    pub const fn memory_limit() -> Self {
        Self::with_subcode(Code::Aborted, SubCode::MemoryLimit)
    }
    /// `IOError` with `SubCode::SpaceLimit`.
    pub const fn space_limit() -> Self {
        Self::with_subcode(Code::IoError, SubCode::SpaceLimit)
    }
    /// `IOError` with `SubCode::PathNotFound`.
    pub const fn path_not_found() -> Self {
        Self::with_subcode(Code::IoError, SubCode::PathNotFound)
    }

    // -- Predicates that match upstream `Status::Is*` helpers --

    /// Matches `Status::IsNotFound()`.
    pub const fn is_not_found(&self) -> bool {
        matches!(self.code, Code::NotFound)
    }
    /// Matches `Status::IsCorruption()`.
    pub const fn is_corruption(&self) -> bool {
        matches!(self.code, Code::Corruption)
    }
    /// Matches `Status::IsNotSupported()`.
    pub const fn is_not_supported(&self) -> bool {
        matches!(self.code, Code::NotSupported)
    }
    /// Matches `Status::IsInvalidArgument()`.
    pub const fn is_invalid_argument(&self) -> bool {
        matches!(self.code, Code::InvalidArgument)
    }
    /// Matches `Status::IsIOError()`.
    pub const fn is_io_error(&self) -> bool {
        matches!(self.code, Code::IoError)
    }
    /// Matches `Status::IsBusy()`.
    pub const fn is_busy(&self) -> bool {
        matches!(self.code, Code::Busy)
    }
    /// Matches `Status::IsTryAgain()`.
    pub const fn is_try_again(&self) -> bool {
        matches!(self.code, Code::TryAgain)
    }
    /// Matches `Status::IsTimedOut()`.
    pub const fn is_timed_out(&self) -> bool {
        matches!(self.code, Code::TimedOut)
    }
    /// Matches `Status::IsAborted()`.
    pub const fn is_aborted(&self) -> bool {
        matches!(self.code, Code::Aborted)
    }
    /// Matches `Status::IsIncomplete()`.
    pub const fn is_incomplete(&self) -> bool {
        matches!(self.code, Code::Incomplete)
    }
    /// Matches `Status::IsShutdownInProgress()`.
    pub const fn is_shutdown_in_progress(&self) -> bool {
        matches!(self.code, Code::ShutdownInProgress)
    }
    /// Matches `Status::IsNoSpace()` — `IOError` with sub-code `NoSpace`.
    pub const fn is_no_space(&self) -> bool {
        matches!(self.code, Code::IoError) && matches!(self.subcode, SubCode::NoSpace)
    }
    /// Matches `Status::IsMemoryLimit()` — `Aborted` with sub-code `MemoryLimit`.
    pub const fn is_memory_limit(&self) -> bool {
        matches!(self.code, Code::Aborted) && matches!(self.subcode, SubCode::MemoryLimit)
    }
    /// Matches `Status::IsPathNotFound()`.
    pub const fn is_path_not_found(&self) -> bool {
        matches!(self.code, Code::IoError | Code::NotFound)
            && matches!(self.subcode, SubCode::PathNotFound)
    }
    /// Matches `Status::IsDeadlock()`.
    pub const fn is_deadlock(&self) -> bool {
        matches!(self.code, Code::Busy) && matches!(self.subcode, SubCode::Deadlock)
    }
    /// Matches `Status::IsColumnFamilyDropped()`.
    pub const fn is_column_family_dropped(&self) -> bool {
        matches!(self.code, Code::ColumnFamilyDropped)
    }

    /// Sets the severity. Builder-style.
    #[must_use]
    pub fn with_severity(mut self, sev: Severity) -> Self {
        self.severity = sev;
        self
    }

    /// Marks the status as retryable. Builder-style.
    #[must_use]
    pub const fn with_retryable(mut self, retryable: bool) -> Self {
        self.retryable = retryable;
        self
    }

    /// Marks the status as implying permanent data loss. Builder-style.
    #[must_use]
    pub const fn with_data_loss(mut self, data_loss: bool) -> Self {
        self.data_loss = data_loss;
        self
    }

    /// Keep `self` if it's already non-OK; otherwise replace with `other`.
    ///
    /// Mirrors upstream `Status::UpdateIfOk`. Useful for gathering the
    /// first error from a sequence of fallible steps.
    pub fn update_if_ok(&mut self, other: Status) {
        if self.is_ok() {
            *self = other;
        }
    }
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_ok() {
            return f.write_str("OK");
        }
        let code = match self.code {
            Code::Ok => "OK",
            Code::NotFound => "NotFound",
            Code::Corruption => "Corruption",
            Code::NotSupported => "NotSupported",
            Code::InvalidArgument => "InvalidArgument",
            Code::IoError => "IOError",
            Code::MergeInProgress => "MergeInProgress",
            Code::Incomplete => "Incomplete",
            Code::ShutdownInProgress => "ShutdownInProgress",
            Code::TimedOut => "TimedOut",
            Code::Aborted => "Aborted",
            Code::Busy => "Busy",
            Code::Expired => "Expired",
            Code::TryAgain => "TryAgain",
            Code::CompactionTooLarge => "CompactionTooLarge",
            Code::ColumnFamilyDropped => "ColumnFamilyDropped",
        };
        match &self.message {
            Some(msg) => write!(f, "{code}: {msg}"),
            None => f.write_str(code),
        }
    }
}

impl std::error::Error for Status {}

impl From<std::io::Error> for Status {
    fn from(err: std::io::Error) -> Self {
        use std::io::ErrorKind;
        let subcode = match err.kind() {
            ErrorKind::NotFound => SubCode::PathNotFound,
            ErrorKind::TimedOut => SubCode::LockTimeout,
            _ => SubCode::None,
        };
        let mut status = Status::new(Code::IoError, err.to_string());
        status.subcode = subcode;
        status
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ok_is_ok() {
        let s = Status::ok();
        assert!(s.is_ok());
        assert!(!s.is_not_found());
        assert_eq!(s.to_string(), "OK");
    }

    #[test]
    fn not_found_predicate() {
        let s = Status::not_found("missing");
        assert!(s.is_not_found());
        assert!(!s.is_ok());
        assert_eq!(s.to_string(), "NotFound: missing");
    }

    #[test]
    fn path_not_found_derived_from_subcode() {
        let s = Status::path_not_found();
        assert!(s.is_io_error());
        assert!(s.is_path_not_found());
    }

    #[test]
    fn update_if_ok_preserves_first_error() {
        let mut s = Status::ok();
        s.update_if_ok(Status::corruption("bad"));
        assert!(s.is_corruption());
        s.update_if_ok(Status::io_error("ignored"));
        assert!(s.is_corruption(), "first error should be preserved");
    }

    #[test]
    fn result_integration() {
        fn do_thing(flag: bool) -> Result<u32> {
            if flag {
                Ok(42)
            } else {
                Err(Status::invalid_argument("nope"))
            }
        }
        assert_eq!(do_thing(true).unwrap(), 42);
        let err = do_thing(false).unwrap_err();
        assert!(err.is_invalid_argument());
    }

    #[test]
    fn from_io_error() {
        let io = std::io::Error::new(std::io::ErrorKind::NotFound, "x");
        let s: Status = io.into();
        assert!(s.is_io_error());
        assert!(matches!(s.subcode, SubCode::PathNotFound));
    }

    // ---- Constructor coverage ----

    #[test]
    fn corruption_constructor() {
        let s = Status::corruption("bad checksum");
        assert!(s.is_corruption());
        assert!(!s.is_ok());
        assert_eq!(s.code, Code::Corruption);
        assert_eq!(s.message.as_deref(), Some("bad checksum"));
    }

    #[test]
    fn not_supported_constructor() {
        let s = Status::not_supported("no bloom filter");
        assert!(s.is_not_supported());
        assert_eq!(s.code, Code::NotSupported);
        assert_eq!(s.message.as_deref(), Some("no bloom filter"));
    }

    #[test]
    fn invalid_argument_constructor() {
        let s = Status::invalid_argument("bad path");
        assert!(s.is_invalid_argument());
        assert_eq!(s.code, Code::InvalidArgument);
    }

    #[test]
    fn io_error_constructor() {
        let s = Status::io_error("disk full");
        assert!(s.is_io_error());
        assert_eq!(s.code, Code::IoError);
        assert_eq!(s.message.as_deref(), Some("disk full"));
    }

    // ---- Predicate coverage ----

    #[test]
    fn busy_predicate() {
        let s = Status::busy("resource contention");
        assert!(s.is_busy());
        assert!(!s.is_ok());
    }

    #[test]
    fn try_again_predicate() {
        let s = Status::try_again("retry later");
        assert!(s.is_try_again());
    }

    #[test]
    fn timed_out_predicate() {
        let s = Status::timed_out("deadline exceeded");
        assert!(s.is_timed_out());
    }

    #[test]
    fn aborted_predicate() {
        let s = Status::aborted("cancelled by policy");
        assert!(s.is_aborted());
    }

    #[test]
    fn incomplete_predicate() {
        let s = Status::incomplete("partial result");
        assert!(s.is_incomplete());
    }

    #[test]
    fn shutdown_in_progress_predicate() {
        let s = Status::shutdown_in_progress("closing");
        assert!(s.is_shutdown_in_progress());
    }

    #[test]
    fn column_family_dropped_predicate() {
        let s = Status::column_family_dropped("cf gone");
        assert!(s.is_column_family_dropped());
    }

    // ---- Compound subcodes ----

    #[test]
    fn no_space_constructor_and_predicate() {
        let s = Status::no_space();
        assert!(s.is_io_error());
        assert!(s.is_no_space());
        assert_eq!(s.subcode, SubCode::NoSpace);
    }

    #[test]
    fn memory_limit_constructor_and_predicate() {
        let s = Status::memory_limit();
        assert!(s.is_aborted());
        assert!(s.is_memory_limit());
        assert_eq!(s.subcode, SubCode::MemoryLimit);
    }

    #[test]
    fn space_limit_constructor() {
        let s = Status::space_limit();
        assert!(s.is_io_error());
        assert_eq!(s.subcode, SubCode::SpaceLimit);
    }

    #[test]
    fn deadlock_predicate() {
        let s = Status::with_subcode(Code::Busy, SubCode::Deadlock);
        assert!(s.is_busy());
        assert!(s.is_deadlock());
    }

    // ---- Display / Debug formatting ----

    #[test]
    fn display_ok() {
        let s = Status::ok();
        assert_eq!(format!("{s}"), "OK");
    }

    #[test]
    fn display_corruption_with_message() {
        let s = Status::corruption("bad block");
        assert_eq!(format!("{s}"), "Corruption: bad block");
    }

    #[test]
    fn display_io_error_without_message() {
        let s = Status::no_space();
        // No message attached, just the code name.
        assert_eq!(format!("{s}"), "IOError");
    }

    #[test]
    fn display_all_code_names() {
        // Verify each code renders as expected in Display.
        let cases = [
            (Code::NotFound, "NotFound"),
            (Code::NotSupported, "NotSupported"),
            (Code::InvalidArgument, "InvalidArgument"),
            (Code::IoError, "IOError"),
            (Code::MergeInProgress, "MergeInProgress"),
            (Code::Incomplete, "Incomplete"),
            (Code::ShutdownInProgress, "ShutdownInProgress"),
            (Code::TimedOut, "TimedOut"),
            (Code::Aborted, "Aborted"),
            (Code::Busy, "Busy"),
            (Code::Expired, "Expired"),
            (Code::TryAgain, "TryAgain"),
            (Code::CompactionTooLarge, "CompactionTooLarge"),
            (Code::ColumnFamilyDropped, "ColumnFamilyDropped"),
        ];
        for (code, expected) in cases {
            let s = Status::with_subcode(code, SubCode::None);
            assert_eq!(format!("{s}"), expected);
        }
    }

    #[test]
    fn debug_format_includes_fields() {
        let s = Status::corruption("test");
        let dbg = format!("{s:?}");
        assert!(dbg.contains("Corruption"), "Debug should contain code: {dbg}");
        assert!(dbg.contains("test"), "Debug should contain message: {dbg}");
    }

    // ---- Default trait ----

    #[test]
    fn default_is_ok() {
        let s: Status = Default::default();
        assert!(s.is_ok());
        assert_eq!(s.code, Code::Ok);
        assert_eq!(s.subcode, SubCode::None);
        assert_eq!(s.severity, Severity::NoError);
        assert!(!s.retryable);
        assert!(!s.data_loss);
    }

    // ---- Builder methods ----

    #[test]
    fn with_severity_builder() {
        let s = Status::io_error("fail").with_severity(Severity::FatalError);
        assert!(s.is_io_error());
        assert_eq!(s.severity, Severity::FatalError);
    }

    #[test]
    fn with_retryable_builder() {
        let s = Status::io_error("transient").with_retryable(true);
        assert!(s.retryable);
    }

    #[test]
    fn with_data_loss_builder() {
        let s = Status::corruption("permanent").with_data_loss(true);
        assert!(s.data_loss);
    }

    // ---- From<io::Error> additional coverage ----

    #[test]
    fn from_io_error_timed_out() {
        let io = std::io::Error::new(std::io::ErrorKind::TimedOut, "lock wait");
        let s: Status = io.into();
        assert!(s.is_io_error());
        assert_eq!(s.subcode, SubCode::LockTimeout);
    }

    #[test]
    fn from_io_error_other_kind() {
        let io = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied");
        let s: Status = io.into();
        assert!(s.is_io_error());
        assert_eq!(s.subcode, SubCode::None);
        assert!(s.message.as_deref().unwrap().contains("denied"));
    }

    // ---- std::error::Error trait ----

    #[test]
    fn status_implements_error_trait() {
        let s = Status::io_error("some error");
        let e: &dyn std::error::Error = &s;
        assert!(e.to_string().contains("IOError"));
    }

    // ---- Remaining factories ----

    #[test]
    fn merge_in_progress_constructor() {
        let s = Status::merge_in_progress("merging");
        assert_eq!(s.code, Code::MergeInProgress);
    }

    #[test]
    fn expired_constructor() {
        let s = Status::expired("ttl exceeded");
        assert_eq!(s.code, Code::Expired);
    }

    #[test]
    fn compaction_too_large_constructor() {
        let s = Status::compaction_too_large("too big");
        assert_eq!(s.code, Code::CompactionTooLarge);
    }

    // ---- with_subcode constructor ----

    #[test]
    fn with_subcode_constructor() {
        let s = Status::with_subcode(Code::IoError, SubCode::IoFenced);
        assert!(s.is_io_error());
        assert_eq!(s.subcode, SubCode::IoFenced);
        assert!(s.message.is_none());
    }

    // ---- new constructor ----

    #[test]
    fn new_constructor_sets_all_defaults() {
        let s = Status::new(Code::Busy, "locked");
        assert_eq!(s.code, Code::Busy);
        assert_eq!(s.subcode, SubCode::None);
        assert_eq!(s.severity, Severity::NoError);
        assert!(!s.retryable);
        assert!(!s.data_loss);
        assert_eq!(s.scope, 0);
        assert_eq!(s.message.as_deref(), Some("locked"));
    }
}
