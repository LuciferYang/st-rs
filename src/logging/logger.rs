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
}
