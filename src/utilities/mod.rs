//! Port of `utilities/` from upstream.
//!
//! Optional features layered on top of the engine. Each submodule
//! is self-contained — it depends on the core engine API but not
//! on other utilities.
//!
//! Layer 5 ships:
//! - [`checkpoint`] — create a consistent hard-linked copy of a
//!   live DB.

pub mod checkpoint;
