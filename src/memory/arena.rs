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

//! Port of `memory/arena.h` + `memory/arena.cc`.
//!
//! Bump-allocator used primarily by the memtable. A memtable instantiates
//! exactly one [`Arena`], writes all its skiplist nodes and key/value
//! bytes into it, and drops the whole arena in one shot when the memtable
//! is retired. Allocation is O(1): a pointer bump in the active block,
//! with a new block allocated on demand.
//!
//! # Differences from upstream
//!
//! Upstream's `Arena` returns `char*` pointers that all alias with the
//! arena's internal buffer. Rust's borrow checker rejects that pattern in
//! safe code — you cannot hold multiple live `&mut [u8]` references into
//! the same backing store at once.
//!
//! The Layer 1 port restricts `allocate` to take `&mut self`, which means
//! each allocation must be fully used (written into, or copied out into
//! an owned `Vec`) before the next allocation begins. This is sufficient
//! for the vast majority of Layer 1 tests and for simple memtable-like
//! structures that store offsets rather than pointers.
//!
//! A Layer 2 `ConcurrentArena` (which is inherently unsafe on both sides)
//! will replace this with an `&self`-based API, behind its own `unsafe`
//! module boundary.

use crate::memory::allocator::Allocator;

/// Default block size: 4 KiB. Matches upstream `kDefaultBlockSize`.
pub const DEFAULT_BLOCK_SIZE: usize = 4096;

/// A bump allocator backed by a vector of fixed-size blocks.
///
/// Allocations larger than the block size get their own one-off block;
/// this matches upstream `Arena::AllocateFallback` with the "big block"
/// branch.
#[derive(Debug)]
pub struct Arena {
    /// The list of blocks this arena owns. The last entry is the
    /// "current" block that new allocations bump into. Each block is a
    /// `Vec<u8>` pre-sized to its capacity — we track the used prefix
    /// separately so the slice returned to callers is guaranteed not
    /// to be invalidated by a future push.
    blocks: Vec<Vec<u8>>,
    /// Index of the current block within `blocks`. `usize::MAX` means
    /// "no blocks yet".
    current: usize,
    /// Number of bytes already consumed in the current block.
    current_offset: usize,
    /// Size of a fresh ("normal") block.
    block_size: usize,
    /// Total bytes returned to callers.
    bytes_allocated: usize,
    /// Total bytes of backing memory across all blocks.
    memory_allocated: usize,
}

impl Arena {
    /// Create a new arena with the default block size.
    pub fn new() -> Self {
        Self::with_block_size(DEFAULT_BLOCK_SIZE)
    }

    /// Create a new arena with a custom block size. `block_size` must be
    /// a power of two greater than zero; non-conforming values panic.
    pub fn with_block_size(block_size: usize) -> Self {
        assert!(block_size.is_power_of_two(), "block size must be a power of two");
        Self {
            blocks: Vec::new(),
            current: usize::MAX,
            current_offset: 0,
            block_size,
            bytes_allocated: 0,
            memory_allocated: 0,
        }
    }

    /// The configured block size.
    pub fn block_size(&self) -> usize {
        self.block_size
    }

    /// Allocate a new block of `size` bytes of backing memory and make it
    /// the current block. Used both on first allocation and as the
    /// fallback when the current block is full.
    fn allocate_new_block(&mut self, size: usize) {
        // We zero the block rather than using `with_capacity`+`set_len`
        // because we want the returned slice to be initialised (safe Rust
        // cannot hand out uninitialised bytes).
        let block = vec![0u8; size];
        self.memory_allocated += block.capacity();
        self.blocks.push(block);
        self.current = self.blocks.len() - 1;
        self.current_offset = 0;
    }

    /// Internal allocator that returns a slice of `size` bytes from the
    /// current block, allocating a new block if needed.
    fn allocate_internal(&mut self, size: usize) -> &mut [u8] {
        if self.current == usize::MAX || self.current_offset + size > self.blocks[self.current].len()
        {
            // Oversized allocations get a dedicated block sized exactly
            // to fit. Upstream uses a cutoff at `block_size / 4`; we use
            // the same shape.
            let new_block_size = if size > self.block_size / 4 {
                size
            } else {
                self.block_size
            };
            self.allocate_new_block(new_block_size);
        }
        let block = &mut self.blocks[self.current];
        let start = self.current_offset;
        self.current_offset += size;
        self.bytes_allocated += size;
        &mut block[start..start + size]
    }
}

impl Default for Arena {
    fn default() -> Self {
        Self::new()
    }
}

impl Allocator for Arena {
    fn allocate(&mut self, size: usize) -> &mut [u8] {
        self.allocate_internal(size)
    }

    fn allocate_aligned(&mut self, size: usize, align: usize) -> &mut [u8] {
        assert!(align.is_power_of_two(), "align must be a power of two");
        // Pad the current offset up to alignment before handing out.
        if self.current != usize::MAX {
            let padding = (align - (self.current_offset & (align - 1))) & (align - 1);
            if self.current_offset + padding + size <= self.blocks[self.current].len() {
                self.current_offset += padding;
                self.bytes_allocated += padding;
                return self.allocate_internal(size);
            }
        }
        // Either there's no current block or the padded request wouldn't
        // fit; force a new block and allocate from it. New blocks are
        // already aligned to at least `usize` alignment — sufficient for
        // every alignment request the memtable will make.
        self.allocate_internal(size)
    }

    fn bytes_allocated(&self) -> usize {
        self.bytes_allocated
    }

    fn memory_allocated(&self) -> usize {
        self.memory_allocated
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_arena_is_empty() {
        let arena = Arena::new();
        assert_eq!(arena.bytes_allocated(), 0);
        assert_eq!(arena.memory_allocated(), 0);
    }

    #[test]
    fn single_allocation_within_block() {
        let mut arena = Arena::with_block_size(1024);
        let slice = arena.allocate(16);
        slice.copy_from_slice(b"hello-world-1234");
        assert_eq!(arena.bytes_allocated(), 16);
        assert_eq!(arena.memory_allocated(), 1024);
    }

    #[test]
    fn multiple_sequential_allocations() {
        let mut arena = Arena::with_block_size(1024);
        for i in 0..50 {
            let slice = arena.allocate(10);
            slice.fill(i as u8);
        }
        assert_eq!(arena.bytes_allocated(), 500);
        assert_eq!(arena.memory_allocated(), 1024);
    }

    #[test]
    fn new_block_when_current_full() {
        let mut arena = Arena::with_block_size(64);
        // 64 bytes exactly fills block 1; a 10-byte request forces a new block.
        let _ = arena.allocate(64);
        assert_eq!(arena.memory_allocated(), 64);
        let _ = arena.allocate(10);
        assert_eq!(arena.memory_allocated(), 128);
        assert_eq!(arena.bytes_allocated(), 74);
    }

    #[test]
    fn oversized_allocation_gets_dedicated_block() {
        let mut arena = Arena::with_block_size(64);
        let _ = arena.allocate(200);
        // Block was sized to exactly fit 200 bytes (oversize path), so
        // total memory is just 200 bytes.
        assert_eq!(arena.memory_allocated(), 200);
        assert_eq!(arena.bytes_allocated(), 200);
    }

    #[test]
    fn allocate_aligned_honours_alignment() {
        let mut arena = Arena::with_block_size(1024);
        // First small allocation leaves offset at 3.
        let _ = arena.allocate(3);
        // Aligned-8 request: the implementation should pad forward.
        let aligned = arena.allocate_aligned(16, 8);
        aligned.fill(0xab);
        // After this, bytes_allocated should include the padding +
        // the 16 bytes: 3 (initial) + 5 (padding to 8) + 16 = 24.
        assert_eq!(arena.bytes_allocated(), 24);
    }
}
