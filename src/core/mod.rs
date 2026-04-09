//! Universal primitives used everywhere else in the crate.
//!
//! This module groups the three types that every other file references:
//! [`status::Status`], [`slice::Slice`], and the primitive aliases in
//! [`types`]. Keeping them together in `core::` makes it obvious that they
//! are foundational and have no internal dependencies (not even on `port`).

pub mod slice;
pub mod status;
pub mod types;
