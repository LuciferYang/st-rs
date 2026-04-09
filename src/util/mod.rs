//! Port of `util/` from upstream.
//!
//! Foundation utilities used everywhere in the engine: integer encoding,
//! hashing, pseudo-random number generation, and small string helpers.
//! These modules are deliberately leaf-level — they depend on nothing
//! except [`crate::core`] and [`crate::port`].
//!
//! Not ported (handled by `std` or a later layer):
//! - `util/mutexlock.h` — use `std::sync::MutexGuard` from `.lock().unwrap()`.
//! - `util/thread_local.h` — use `std::thread_local!`.
//! - `util/autovector.h` — use `Vec`, or add the `smallvec` crate in a higher layer.
//! - `util/aligned_buffer.h` — belongs with direct-I/O code in Layer 2.
//! - `util/crc32c.h` — a follow-up module; `crc32fast` crate can replace it.

pub mod coding;
pub mod crc32c;
pub mod hash;
pub mod heap;
pub mod random;
pub mod string_util;
