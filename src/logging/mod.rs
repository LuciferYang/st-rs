//! Port of `logging/` from upstream.
//!
//! Internal logging used by the engine for info, warning, and error
//! messages. Not to be confused with the write-ahead log (WAL), which
//! lives in the `db/` layer.
//!
//! Upstream ships `logging/logging.h` (the `ROCKS_LOG_*` macros),
//! `auto_roll_logger.h` (file rotation), and `event_logger.h` (structured
//! JSON). Layer 1 ports only the minimal `Logger` trait plus a
//! `ConsoleLogger` implementation; rotation and structured events belong
//! in a higher layer once there's a real logging sink.

pub mod logger;
