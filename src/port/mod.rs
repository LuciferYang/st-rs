//! Port of `port/port.h` — platform abstraction constants.
//!
//! In upstream RocksDB, the `port/` directory provides mutexes, condvars,
//! atomics, thread helpers, byte-order helpers, and prefetch intrinsics
//! behind a single `port::` namespace. In Rust, the standard library already
//! covers all of those: `std::sync::{Mutex, RwLock, Condvar}`,
//! `std::sync::atomic::*`, `std::thread`, integer `to_le_bytes`/`from_le_bytes`.
//!
//! So the Rust "port" is essentially empty — its only job is to centralise
//! the few constants (page size, cache-line size, endian assumption) so that
//! higher layers don't re-derive them.

/// The CPU cache line size in bytes. Matches `CACHE_LINE_SIZE` from
/// upstream `port/port_posix.h`. Most x86-64 and aarch64 CPUs use 64 bytes.
pub const CACHE_LINE_SIZE: usize = 64;

/// Default memory page size in bytes. Matches `kDefaultPageSize` from
/// upstream. Real page size should be queried at runtime on systems that
/// support larger pages; this is the conservative default.
pub const DEFAULT_PAGE_SIZE: usize = 4096;

/// `true` if the current target is little-endian. RocksDB assumes
/// little-endian throughout the SST encoding, matching upstream's
/// `PLATFORM_IS_LITTLE_ENDIAN` macro.
pub const IS_LITTLE_ENDIAN: bool = cfg!(target_endian = "little");

/// Maximum file path length the engine is willing to handle.
/// Upstream uses `PATH_MAX` from `<limits.h>`; we pick a conservative
/// cross-platform constant here. Higher layers that need the real OS limit
/// should query it via `libc` or `std::os`.
pub const MAX_PATH_LENGTH: usize = 4096;

// Compile-time sanity check on the constants.
const _: () = {
    assert!(CACHE_LINE_SIZE >= 32 && CACHE_LINE_SIZE <= 256);
    assert!(DEFAULT_PAGE_SIZE.is_power_of_two());
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn little_endian_matches_target() {
        assert_eq!(IS_LITTLE_ENDIAN, u16::from_ne_bytes([1, 0]) == 1);
    }
}
