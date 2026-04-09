//! Port of `monitoring/perf_context_imp.h`.
//!
//! A [`PerfContext`] is a **per-thread** struct of fine-grained counters
//! that the engine updates on hot paths (bytes read, cache lookups,
//! filter matches, …). Users enable it at a given level and then read
//! the counters after a call to understand where time went.
//!
//! Rust translation: upstream uses a thread-local `PerfContext` global
//! plus `PERF_TIMER_GUARD` macros. We keep the same model — a
//! `thread_local!` cell of `PerfContext` accessed via [`with`] and
//! [`mut_with`]. This avoids the ergonomic baggage of plumbing the
//! context through every call in a trait signature.

use std::cell::RefCell;

/// Verbosity level for perf-context tracking. Mirrors upstream
/// `PerfLevel`. Higher levels enable more expensive instrumentation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(u8)]
pub enum PerfLevel {
    /// No instrumentation — every counter read returns zero.
    Disable = 0,
    /// Enable counters but not timers.
    EnableCount = 1,
    /// Enable counters and time measurements outside the mutex.
    #[default]
    EnableTimeExceptForMutex = 2,
    /// Enable counters and all time measurements including mutex wait.
    EnableTime = 3,
}

/// Per-thread performance counters. All fields are public so engine code
/// can update them directly (matching upstream's C++ style — no getter
/// boilerplate).
#[derive(Debug, Clone, Default)]
#[allow(missing_docs)]
pub struct PerfContext {
    // -- Core read-path counters --
    pub user_key_comparison_count: u64,
    pub block_cache_hit_count: u64,
    pub block_cache_miss_count: u64,
    pub block_read_count: u64,
    pub block_read_byte: u64,
    pub block_read_time: u64,
    pub block_checksum_time: u64,
    pub block_decompress_time: u64,

    // -- Memtable --
    pub get_from_memtable_count: u64,
    pub get_from_memtable_time: u64,

    // -- Get / MultiGet --
    pub get_cpu_nanos: u64,
    pub get_snapshot_time: u64,
    pub get_post_process_time: u64,
    pub get_from_output_files_time: u64,

    // -- Writes --
    pub write_wal_time: u64,
    pub write_memtable_time: u64,
    pub write_pre_and_post_process_time: u64,
    pub write_thread_wait_nanos: u64,

    // -- Seek / iteration --
    pub seek_on_memtable_time: u64,
    pub seek_on_memtable_count: u64,
    pub seek_internal_seek_time: u64,
    pub find_next_user_entry_time: u64,

    // -- Bloom filter --
    pub bloom_memtable_hit_count: u64,
    pub bloom_memtable_miss_count: u64,
    pub bloom_sst_hit_count: u64,
    pub bloom_sst_miss_count: u64,
}

impl PerfContext {
    /// Zero every counter.
    pub fn reset(&mut self) {
        *self = PerfContext::default();
    }
}

thread_local! {
    static PERF_CTX: RefCell<PerfContext> = RefCell::new(PerfContext::default());
    static PERF_LEVEL: RefCell<PerfLevel> = const { RefCell::new(PerfLevel::Disable) };
}

/// Set the current thread's perf level.
pub fn set_perf_level(level: PerfLevel) {
    PERF_LEVEL.with(|cell| *cell.borrow_mut() = level);
}

/// Get the current thread's perf level.
pub fn get_perf_level() -> PerfLevel {
    PERF_LEVEL.with(|cell| *cell.borrow())
}

/// Read from the current thread's perf context.
pub fn with<R>(f: impl FnOnce(&PerfContext) -> R) -> R {
    PERF_CTX.with(|cell| f(&cell.borrow()))
}

/// Mutate the current thread's perf context.
pub fn mut_with<R>(f: impl FnOnce(&mut PerfContext) -> R) -> R {
    PERF_CTX.with(|cell| f(&mut cell.borrow_mut()))
}

/// Convenience: zero the current thread's perf context.
pub fn reset() {
    mut_with(|pc| pc.reset());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_zero() {
        let pc = PerfContext::default();
        assert_eq!(pc.block_cache_hit_count, 0);
        assert_eq!(pc.get_cpu_nanos, 0);
    }

    #[test]
    fn thread_local_roundtrip() {
        reset();
        mut_with(|pc| {
            pc.block_cache_hit_count = 3;
            pc.block_cache_miss_count = 1;
        });
        with(|pc| {
            assert_eq!(pc.block_cache_hit_count, 3);
            assert_eq!(pc.block_cache_miss_count, 1);
        });
        reset();
        with(|pc| {
            assert_eq!(pc.block_cache_hit_count, 0);
        });
    }

    #[test]
    fn perf_level_roundtrip() {
        set_perf_level(PerfLevel::EnableTime);
        assert_eq!(get_perf_level(), PerfLevel::EnableTime);
        set_perf_level(PerfLevel::Disable);
        assert_eq!(get_perf_level(), PerfLevel::Disable);
    }
}
