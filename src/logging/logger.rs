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

//! Port of `logging/logging.h` — the internal logging sink.
//!
//! A [`Logger`] is any `Send + Sync` sink that accepts pre-formatted
//! log records at one of five severity levels. The engine will reach for
//! this trait behind macros; at Layer 1 we don't port the macros because
//! they are easily replaced with `format!` + method calls, and keeping
//! the surface small makes the trait easier to evolve.
//!
//! Two concrete loggers are provided:
//! - [`NoopLogger`] — discards every record. Useful as a default.
//! - [`ConsoleLogger`] — prints to stderr. Useful in tests and early
//!   engine development.

use std::sync::Mutex;

/// Severity level of a log record. Mirrors `InfoLogLevel` from upstream
/// — the values match the numeric order so `level >= WARN` is a
/// meaningful filter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
#[repr(u8)]
pub enum LogLevel {
    /// Verbose diagnostic output; off in release builds by default.
    Debug = 0,
    /// Routine progress messages.
    #[default]
    Info = 1,
    /// Recoverable anomalies.
    Warn = 2,
    /// Errors that required a fallback but did not crash the engine.
    Error = 3,
    /// Fatal conditions; the engine is about to shut down.
    Fatal = 4,
    /// Header records emitted once at startup (DB options, build info).
    /// Always shown regardless of level filter.
    Header = 5,
}

impl LogLevel {
    /// Human-readable short name used in the default record format.
    pub fn as_str(self) -> &'static str {
        match self {
            LogLevel::Debug => "DEBUG",
            LogLevel::Info => "INFO",
            LogLevel::Warn => "WARN",
            LogLevel::Error => "ERROR",
            LogLevel::Fatal => "FATAL",
            LogLevel::Header => "HEADER",
        }
    }
}

/// Trait implemented by any log sink. Must be thread-safe because the
/// engine writes log records from flush/compaction background threads.
pub trait Logger: Send + Sync {
    /// Emit a record at `level` with the given message. Implementations
    /// should apply their own level filter and return without work when
    /// the record is below the filter threshold.
    fn log(&self, level: LogLevel, message: &str);

    /// Current minimum level. Records below this level should be dropped.
    fn level(&self) -> LogLevel {
        LogLevel::Info
    }

    /// Flush any buffered output. Default: no-op.
    fn flush(&self) {}

    // -- Convenience shortcuts (not overridable) --

    /// Log at `Info` level.
    fn info(&self, message: &str) {
        self.log(LogLevel::Info, message);
    }
    /// Log at `Warn` level.
    fn warn(&self, message: &str) {
        self.log(LogLevel::Warn, message);
    }
    /// Log at `Error` level.
    fn error(&self, message: &str) {
        self.log(LogLevel::Error, message);
    }
}

/// A logger that discards every record. The default when no sink is
/// configured.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopLogger;

impl Logger for NoopLogger {
    fn log(&self, _level: LogLevel, _message: &str) {}
}

/// A logger that writes to stderr with a `LEVEL  message` format.
/// Thread-safe via an internal mutex around the writes.
pub struct ConsoleLogger {
    level: LogLevel,
    /// Lock around the actual stderr write so records from concurrent
    /// threads don't interleave.
    lock: Mutex<()>,
}

impl ConsoleLogger {
    /// Create a new console logger that accepts records at `level` or
    /// higher.
    pub fn new(level: LogLevel) -> Self {
        Self {
            level,
            lock: Mutex::new(()),
        }
    }
}

impl Default for ConsoleLogger {
    fn default() -> Self {
        Self::new(LogLevel::Info)
    }
}

impl Logger for ConsoleLogger {
    fn log(&self, level: LogLevel, message: &str) {
        if level < self.level && level != LogLevel::Header {
            return;
        }
        let _guard = self.lock.lock().expect("ConsoleLogger mutex poisoned");
        eprintln!("{:<6} {}", level.as_str(), message);
    }

    fn level(&self) -> LogLevel {
        self.level
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// An in-memory logger used to assert on emitted records.
    #[derive(Default)]
    struct CaptureLogger {
        records: Mutex<Vec<(LogLevel, String)>>,
        level: LogLevel,
    }
    impl Logger for CaptureLogger {
        fn log(&self, level: LogLevel, message: &str) {
            if level < self.level && level != LogLevel::Header {
                return;
            }
            self.records.lock().unwrap().push((level, message.to_string()));
        }
        fn level(&self) -> LogLevel {
            self.level
        }
    }

    #[test]
    fn level_ordering() {
        assert!(LogLevel::Debug < LogLevel::Info);
        assert!(LogLevel::Info < LogLevel::Warn);
        assert!(LogLevel::Warn < LogLevel::Error);
        assert!(LogLevel::Error < LogLevel::Fatal);
    }

    #[test]
    fn noop_logger_does_nothing() {
        let l = NoopLogger;
        l.info("hello");
        l.error("world");
        // No assertions — just confirming no panic and compilation.
    }

    #[test]
    fn capture_logger_filters_below_level() {
        let cap = Arc::new(CaptureLogger {
            level: LogLevel::Warn,
            ..Default::default()
        });
        cap.info("should be dropped");
        cap.warn("kept");
        cap.error("also kept");
        let records = cap.records.lock().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].0, LogLevel::Warn);
        assert_eq!(records[1].0, LogLevel::Error);
    }

    #[test]
    fn header_records_bypass_level_filter() {
        let cap = Arc::new(CaptureLogger {
            level: LogLevel::Fatal,
            ..Default::default()
        });
        cap.log(LogLevel::Header, "RocksDB v8.10.0");
        assert_eq!(cap.records.lock().unwrap().len(), 1);
    }

    #[test]
    fn console_logger_does_not_panic() {
        let logger = ConsoleLogger::new(LogLevel::Debug);
        logger.log(LogLevel::Debug, "debug message");
        logger.log(LogLevel::Info, "info message");
        logger.log(LogLevel::Warn, "warn message");
        logger.log(LogLevel::Error, "error message");
        logger.log(LogLevel::Fatal, "fatal message");
        logger.log(LogLevel::Header, "header message");
        // Convenience methods.
        logger.info("info shortcut");
        logger.warn("warn shortcut");
        logger.error("error shortcut");
        logger.flush();
    }

    #[test]
    fn console_logger_filters_below_level() {
        // Create a console logger at Warn level. It writes to stderr
        // so we can't capture output easily, but we can verify it
        // doesn't panic and that the level() accessor works.
        let logger = ConsoleLogger::new(LogLevel::Warn);
        assert_eq!(logger.level(), LogLevel::Warn);
        // These should be filtered (below Warn).
        logger.log(LogLevel::Debug, "should be filtered");
        logger.log(LogLevel::Info, "should be filtered");
        // These should pass.
        logger.log(LogLevel::Warn, "should pass");
        logger.log(LogLevel::Error, "should pass");
    }

    #[test]
    fn console_logger_default_level_is_info() {
        let logger = ConsoleLogger::default();
        assert_eq!(logger.level(), LogLevel::Info);
    }

    #[test]
    fn noop_logger_convenience_methods() {
        let l = NoopLogger;
        l.log(LogLevel::Debug, "d");
        l.log(LogLevel::Info, "i");
        l.log(LogLevel::Warn, "w");
        l.log(LogLevel::Error, "e");
        l.log(LogLevel::Fatal, "f");
        l.log(LogLevel::Header, "h");
        l.info("info");
        l.warn("warn");
        l.error("error");
        l.flush();
        // NoopLogger level defaults to Info.
        assert_eq!(l.level(), LogLevel::Info);
    }

    #[test]
    fn log_level_full_ordering() {
        assert!(LogLevel::Debug < LogLevel::Info);
        assert!(LogLevel::Info < LogLevel::Warn);
        assert!(LogLevel::Warn < LogLevel::Error);
        assert!(LogLevel::Error < LogLevel::Fatal);
        assert!(LogLevel::Fatal < LogLevel::Header);
        // Equality.
        assert_eq!(LogLevel::Info, LogLevel::Info);
    }

    #[test]
    fn log_level_as_str() {
        assert_eq!(LogLevel::Debug.as_str(), "DEBUG");
        assert_eq!(LogLevel::Info.as_str(), "INFO");
        assert_eq!(LogLevel::Warn.as_str(), "WARN");
        assert_eq!(LogLevel::Error.as_str(), "ERROR");
        assert_eq!(LogLevel::Fatal.as_str(), "FATAL");
        assert_eq!(LogLevel::Header.as_str(), "HEADER");
    }

    #[test]
    fn log_level_default_is_info() {
        let level: LogLevel = Default::default();
        assert_eq!(level, LogLevel::Info);
    }
}
