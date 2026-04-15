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
