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

//! Port of `include/rocksdb/env.h`.
//!
//! In upstream RocksDB, `Env` is the legacy abstraction that bundles a
//! filesystem, a thread pool, a clock, and a logger into one virtual class.
//! The newer [`FileSystem`](crate::env::file_system::FileSystem) trait
//! carves off the filesystem part; `Env` keeps the rest.
//!
//! ForSt's `FlinkEnv` (the Java side) constructs a `FlinkFileSystem` on the
//! C++ side, wraps it into an `Env` via `NewCompositeEnv(flink_fs)`, and
//! the user installs that `Env` on `DBOptions::env`. In this Rust port we
//! mirror the split: [`Env`] is a thin composition of a [`FileSystem`],
//! a [`Clock`], and a background thread pool.

use crate::core::status::Result;
use crate::env::file_system::FileSystem;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Per-job priority for background work. Mirrors upstream `Env::Priority`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Priority {
    /// Low priority: background compaction and other best-effort work.
    #[default]
    Low,
    /// High priority: flushes.
    High,
    /// User-initiated foreground work that should not be queued behind
    /// compaction.
    User,
    /// Reserved for the bottom-most compaction thread pool.
    Bottom,
}

/// Handle returned when scheduling a function on the background thread pool.
/// Mirrors upstream's `ThreadPool::Id`.
pub type ScheduleHandle = u64;

/// Source of wall-clock and monotonic time. Mirrors `SystemClock`.
///
/// Separating the clock from the environment lets tests inject a frozen
/// clock without mocking the entire `Env` — which is how upstream's
/// `mock_time_env.h` is structured.
pub trait Clock: Send + Sync {
    /// Monotonically non-decreasing microseconds. Used for deadlines.
    fn now_micros(&self) -> u64;

    /// Nanoseconds since the UNIX epoch. May move backwards on clock
    /// adjustment. Used for timestamps in log files and metadata.
    fn now_nanos(&self) -> u64;

    /// Sleep the current thread for at least `dur`.
    fn sleep_for(&self, dur: Duration);
}

/// Default [`Clock`] implementation backed by `std::time`.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClockImpl;

impl Clock for SystemClockImpl {
    fn now_micros(&self) -> u64 {
        // Monotonic clock via `Instant` is preferred, but we can only
        // convert to an absolute value with a reference point. We fall
        // back to the wall clock here; real engine usage should prefer
        // `std::time::Instant` for deadlines.
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(d) => d.as_micros() as u64,
            Err(_) => 0,
        }
    }

    fn now_nanos(&self) -> u64 {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(d) => d.as_nanos() as u64,
            Err(_) => 0,
        }
    }

    fn sleep_for(&self, dur: Duration) {
        std::thread::sleep(dur);
    }
}

/// A background task scheduler. Mirrors the thread-pool subset of upstream
/// `Env`. Engine implementations will typically wrap `rayon` or a custom
/// work-stealing pool behind this trait.
pub trait ThreadPool: Send + Sync {
    /// Enqueue a task at the given priority. Returns a handle that can be
    /// used with [`Self::cancel`].
    fn schedule(
        &self,
        task: Box<dyn FnOnce() + Send + 'static>,
        priority: Priority,
    ) -> ScheduleHandle;

    /// Cancel a previously scheduled task. Returns `true` if the task was
    /// cancelled before it started; `false` if it was already running or
    /// had completed.
    fn cancel(&self, handle: ScheduleHandle) -> bool;

    /// Resize the pool at the given priority. Returns the previous size.
    fn set_background_threads(&self, num: usize, priority: Priority) -> usize;

    /// Current number of threads at the given priority.
    fn get_background_threads(&self, priority: Priority) -> usize;

    /// Block until all scheduled tasks complete.
    fn wait_for_jobs_and_join_all(&self);
}

/// A bundled environment: filesystem, clock, and thread pool. Mirrors
/// upstream `Env` after the `FileSystem` split.
///
/// Implementations compose the three pieces. The typical construction for
/// ForSt is:
///
/// ```rust,no_run
/// use st_rs::env::env_trait::{CompositeEnv, SystemClockImpl};
/// use st_rs::env::thread_pool::StdThreadPool;
/// use st_rs::env::posix::PosixFileSystem;
/// use std::sync::Arc;
///
/// let fs = Arc::new(PosixFileSystem::new());
/// let env = CompositeEnv::new(
///     fs as Arc<dyn st_rs::FileSystem>,
///     Arc::new(SystemClockImpl),
///     Arc::new(StdThreadPool::new(1, 0, 0, 0)),
/// );
/// ```
pub trait Env: Send + Sync {
    /// The underlying filesystem.
    fn file_system(&self) -> &Arc<dyn FileSystem>;

    /// The clock in use.
    fn clock(&self) -> &Arc<dyn Clock>;

    /// The background thread pool.
    fn thread_pool(&self) -> &Arc<dyn ThreadPool>;

    /// Get the hostname of this machine. Used by upstream to tag info logs.
    fn host_name(&self) -> Result<String> {
        Ok(String::from("localhost"))
    }

    /// Get the current thread's OS-level identifier, as a 64-bit integer.
    fn get_thread_id(&self) -> u64 {
        // std::thread::current().id() returns a ThreadId, which is opaque.
        // Layer 0 doesn't need the real value — engines that care can
        // override this with platform-specific code (gettid / GetCurrentThreadId).
        0
    }
}

/// A trivial [`Env`] implementation that just holds the three sub-traits by
/// `Arc`. Useful as a building block for higher layers — the pattern is the
/// same as upstream `NewCompositeEnv(filesystem)`.
pub struct CompositeEnv {
    fs: Arc<dyn FileSystem>,
    clock: Arc<dyn Clock>,
    pool: Arc<dyn ThreadPool>,
}

impl CompositeEnv {
    /// Bundle an `Env` from the three sub-components.
    pub fn new(
        fs: Arc<dyn FileSystem>,
        clock: Arc<dyn Clock>,
        pool: Arc<dyn ThreadPool>,
    ) -> Self {
        Self { fs, clock, pool }
    }
}

impl Env for CompositeEnv {
    fn file_system(&self) -> &Arc<dyn FileSystem> {
        &self.fs
    }
    fn clock(&self) -> &Arc<dyn Clock> {
        &self.clock
    }
    fn thread_pool(&self) -> &Arc<dyn ThreadPool> {
        &self.pool
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_clock_returns_nonzero_time() {
        let c = SystemClockImpl;
        assert!(c.now_nanos() > 0);
        assert!(c.now_micros() > 0);
    }
}
