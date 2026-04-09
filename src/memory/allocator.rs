//! Port of `memory/allocator.h`.
//!
//! Trait that abstracts over the engine's several allocators: the bump
//! [`crate::memory::arena::Arena`] used by the memtable, the
//! `ConcurrentArena` used by shared memtables, and jemalloc-backed
//! allocators for the block cache. Layer 1 only ships the trait plus the
//! simple `Arena`.

/// An allocator that hands out bump-style slices from an internal pool.
///
/// `allocate` takes `&mut self` (rather than `&self` with interior
/// mutability) because the returned `&mut [u8]` must be unique for the
/// duration of its use. The memtable invariant that "only one thread
/// holds a borrow at a time" matches this naturally.
pub trait Allocator {
    /// Allocate `size` bytes. Panics on allocation failure — mirrors
    /// upstream which aborts on OOM.
    fn allocate(&mut self, size: usize) -> &mut [u8];

    /// Allocate `size` bytes aligned to `align`. Must satisfy
    /// `align.is_power_of_two()`.
    fn allocate_aligned(&mut self, size: usize, align: usize) -> &mut [u8];

    /// Total number of bytes this allocator has handed out. Used by the
    /// memtable to decide when to flush.
    fn bytes_allocated(&self) -> usize;

    /// Approximate memory usage including internal fragmentation. For a
    /// bump allocator this is `bytes_allocated` rounded up to the nearest
    /// block boundary.
    fn memory_allocated(&self) -> usize;
}
