//! Port of `memory/` from upstream.
//!
//! Allocator abstractions. At Layer 1 we only define the contract and
//! ship a simple bump allocator ([`arena::Arena`]); the concurrent and
//! jemalloc-backed variants that upstream ships live in Layer 2.

pub mod allocator;
pub mod arena;
