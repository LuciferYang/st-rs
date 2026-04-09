# Layer 3a Rust Port — Implementation Record

Layer 3 is the densest layer in ForSt — it ports the upstream memtable and the full block-based SST format. To keep the commit reviewable, I've split it:

- **Layer 3a (this commit)** — `InternalKey`, `LookupKey`, `InternalKeyComparator`, a safe single-threaded `SkipList`, the `MemTable` class, SST format primitives (`BlockHandle`, `Footer`), and the data-block format (reader + builder).
- **Layer 3b (follow-up)** — `BlockBasedTableBuilder` / `BlockBasedTableReader`, multi-level `IndexBuilder`, bloom / ribbon `FilterBlockBuilder`, plus wiring through `TableFactory`. These are the "assembler" pieces that stitch 3a's atomic parts into complete SST files.

Companion docs:
- `LAYER0-PORT.md`, `LAYER1-PORT.md`, `LAYER2-PORT.md` — earlier layers
- `ForSt/FORST-READING-ORDER.md` — the layered reading plan

## What landed

**11 new source files, 2,427 LOC**, still zero external dependencies, still `#![deny(unsafe_code)]`.

```
src/
├── db/                          ← NEW: foundational types from upstream db/
│   ├── mod.rs
│   ├── dbformat.rs              ← InternalKey, ParsedInternalKey, LookupKey,
│   │                              InternalKeyComparator, pack/unpack helpers
│   └── memtable.rs              ← MemTable class (add, get, iter)
├── memtable/                    ← NEW
│   ├── mod.rs
│   ├── memtable_rep.rs          ← MemTableRep trait + Iterator trait
│   └── skip_list.rs             ← safe single-threaded skiplist
└── sst/                         ← NEW top-level SST subsystem
    ├── mod.rs
    ├── format.rs                ← BlockHandle, Footer, magic number,
    │                              block trailer helper
    └── block_based/
        ├── mod.rs
        ├── block.rs             ← Block reader + BlockIter
        └── block_builder.rs     ← BlockBuilder (prefix-compressed records
                                    with restart points)
```

## Module summaries

### `db/dbformat.rs` — the internal-key format

This is the foundation every higher layer depends on. Defines:

| Item | Purpose |
|---|---|
| `pack_seq_and_type(seq, type)` / `unpack_seq_and_type(packed)` | 56-bit sequence + 8-bit type → 64-bit trailer, and back. Matches upstream wire format exactly — `value_type_byte(ValueType::Value) == 0x1`, etc. |
| `InternalKey` | Owned `Vec<u8>` holding `user_key || trailer(LE)`. Provides `encode()`, `user_key()`, `sequence()`, `value_type()`. |
| `ParsedInternalKey<'a>` | Zero-copy borrowed view. Returns `Status::Corruption` on short input or unknown value-type byte. |
| `LookupKey` | What you pass to `MemTable::get`. Encodes `varint(internal_key_size) || user_key || trailer(seq, VALUE_TYPE_FOR_SEEK)` — matching upstream's memtable key format. Exposes `memtable_key()`, `internal_key()`, `user_key()`. |
| `InternalKeyComparator` | Wraps a user comparator. Orders by **(user_key asc, seq desc, type desc)** — the combined ordering that makes "get latest version ≤ sequence S" a single seek. Implements the Layer 0 `Comparator` trait so it plugs directly into the skiplist. |

**Key invariant**: `VALUE_TYPE_FOR_SEEK = 0x17` is the largest value-type byte, so a lookup key packs into a larger trailer than any real entry at the same sequence — and with the descending-trailer ordering, it sorts *immediately before* a real entry. The first seek result at or after the lookup key is therefore the newest matching entry.

### `memtable/skip_list.rs` — safe single-threaded skiplist

Upstream's `InlineSkipList` is lock-free with atomic pointers and careful memory ordering. Writing that correctly in **safe** Rust is effectively impossible — so Layer 3a ports a single-threaded variant instead. Same O(log n) shape, same comparator contract, different concurrency story.

- **Data structure**: `Vec<Node>` with `Option<usize>` indices for next-pointers. Same generational-index pattern as the Layer 2 LRU cache — avoids `Rc<RefCell>` and unsafe.
- **Heights**: random, sampled from Layer 1's `Random` (Park-Miller), branching factor 4, max height 12. Deterministic across runs (seeded from a constant) so tests are reproducible.
- **No removal**: memtables are immutable after they're flushed, so the skiplist has no delete path.
- **Iterator**: forward + backward + seek. `prev()` is O(log n) via `find_less_than`, matching upstream's approach of walking back from the head rather than carrying back-pointers.
- **MVCC-friendly**: the skiplist allows duplicate keys (compared by the user comparator). When the InternalKeyComparator is plugged in, duplicates only appear when user-supplied keys would collide with a different `(seq, type)` — which is fine, and in fact what the memtable relies on.

### `memtable/memtable_rep.rs` — abstract `MemTableRep` trait

Matches upstream's `memtablerep.h`. Defines `MemTableRep` + `MemTableRepIterator` so the engine can swap in alternative backends (`HashLinkList`, `VectorRep`, …) without changing the `MemTable` class. Layer 3a only implements this trait via the skiplist; alternative reps are a follow-up.

### `db/memtable.rs` — the `MemTable` class

Ties everything together. A `MemTable` holds a `SkipList` ordered by an `InternalKeyComparator`, plus accounting for `highest_sequence` and `num_entries`.

```rust
impl MemTable {
    pub fn add(&mut self, seq, value_type, user_key, value) -> Result<()>;
    pub fn get(&self, lookup: &LookupKey) -> MemTableGetResult;
    pub fn iter(&self) -> SkipListIter;
    pub fn num_entries(&self) -> usize;
    pub fn approximate_memory_usage(&self) -> usize;
}

enum MemTableGetResult {
    Found(Vec<u8>),    // latest entry is a live value
    Deleted,           // latest entry is a tombstone
    NotFound,          // no matching entry at all
}
```

**Simplifications from upstream:**
- Merge operand assembly is skipped — `Merge` entries are treated as `Value` for now. The shape exists; the operator wiring comes in Layer 4 alongside the MergeOperator trait callsite.
- Range deletion handling is skipped — those live in a separate aggregator in upstream.
- Blob indices and wide-column entries return `NotFound` (they need Layer 4+ support).
- Entries are stored as `(internal_key_bytes, value_bytes)` in separate skiplist fields rather than the packed `varint(len) || internal_key || varint(len) || value` format upstream uses. This is a memtable-internal detail with no wire-format implications — the engine can swap in the packed form later if it turns out to matter for speed.

### `sst/format.rs` — SST bottom-of-file primitives

| Type | Purpose |
|---|---|
| `BlockHandle` | `(offset, size)` — varint64-pair encoded. Has a `NULL` sentinel (both zero) for optional handles (e.g. "no filter block"). |
| `Footer` | Fixed 48-byte footer: metaindex handle + index handle + 8-byte magic number. Handles are zero-padded to `MAX_BLOCK_HANDLE_ENCODED_LENGTH` so the magic always lands at the same offset. |
| `BLOCK_BASED_TABLE_MAGIC_NUMBER` | `0x88e2_41b7_85f4_cff7` — matches upstream. |
| `BLOCK_TRAILER_SIZE = 5` | 1-byte compression type + 4-byte CRC32C. |
| `put_block_trailer` | Writes the 5-byte trailer. Uses a **placeholder** hash (Layer 1's FNV-1a) as the checksum, **not** CRC32C. Good enough for intra-crate round-trips; Layer 3b replaces with a real CRC32C once `util::hash` gains one. |

The `Footer::decode_from` method accepts any buffer ≥ 48 bytes and only looks at the last 48 — matching upstream's "read the tail of the SST file" open pattern.

### `sst/block_based/block_builder.rs` — data block writer

Accumulates key-value records with prefix compression, anchored at restart points every `DEFAULT_BLOCK_RESTART_INTERVAL = 16` records.

Each record is encoded as:
```
shared_len:varint32  non_shared_len:varint32  value_len:varint32  key_delta:bytes  value:bytes
```

At a restart point, `shared_len == 0` and the full key is written. Restart offsets are appended to the end of the buffer as little-endian `u32`s, followed by a final `u32` `num_restarts`.

```rust
let mut b = BlockBuilder::with_restart_interval(16);
b.add(b"user_1001_attr_a", b"v1");
b.add(b"user_1001_attr_b", b"v2");
let bytes = b.finish();  // complete data block
```

**Contract**: keys must be added in strictly increasing order. Debug builds assert this; release builds silently produce garbage if violated (matches upstream).

### `sst/block_based/block.rs` — data block reader

Parses encoded blocks and provides forward + backward iteration with seek. Implementation:

- **Forward iteration**: walks records sequentially, rebuilding each key by keeping `shared` bytes of the previous key and appending the `non_shared` delta.
- **`seek(target)`**: binary search over the restart array — each restart point's key has no prefix compression, so we can compare it directly against `target`. Once we've narrowed to one restart interval, we linear-scan forward.
- **`prev()`**: walks back to the restart at/before the current position, then linear-scans forward until the next record would land on the current one. O(block_restart_interval) rather than O(1), matching upstream.
- **Corruption handling**: lazy — invalid varints or truncated records trip `Status::Corruption` on the iterator step that encounters them, leaving the iterator in an invalid state with a non-OK status.

The iterator owns a `key_buf: Vec<u8>` that gets rebuilt on each step — the returned `&[u8]` from `key()` borrows from this buffer, so it's invalidated on the next step (matching upstream's contract and Layer 0's `DbIterator` rules).

## Verification

| Check | Command | Result |
|---|---|---|
| Clean build | `cargo build` | ✅ 0 warnings, 0 errors |
| Clippy (all targets, `-D warnings`) | `cargo clippy --all-targets -- -D warnings` | ✅ clean |
| Unit tests | `cargo test` | ✅ **169 passed**, 2 ignored doc-tests |
| No unsafe | `#![deny(unsafe_code)]` crate-wide | ✅ enforced |
| Every public item documented | `#![warn(missing_docs)]` | ✅ enforced |
| Zero external dependencies | empty `[dependencies]` | ✅ still std-only |

### Test count by layer

| Layer | Tests |
|---|--:|
| Layer 0 | 28 |
| Layer 1 | 52 |
| Layer 2 | 42 |
| **Layer 3a (new)** | **47** |
| **Total** | **169** |

### Layer 3a test breakdown

| Module | Tests | What they verify |
|---|--:|---|
| `db::dbformat` | 9 | `pack_seq_and_type` round-trip; `MAX_SEQUENCE_NUMBER` round-trip; `InternalKey` round-trip; `ParsedInternalKey` round-trip; parse rejects short input; parse rejects unknown type; internal comparator orders user-keys asc; internal comparator orders sequence desc on tie; `LookupKey` encoding; lookup-key sorts before equal-sequence entry |
| `memtable::skip_list` | 9 | Empty list; single-insert-contains; sorted iteration; `seek` positions correctly; `seek_to_last`; backward iteration; `seek_for_prev`; 1000-element stress test; memory usage grows |
| `db::memtable` | 6 | Insert + get single entry; newer sequence shadows older; lookup at old sequence sees old value; deletion tombstone; lookup missing key; iter yields internal-key order; `highest_sequence` tracks max |
| `sst::format` | 6 | `BlockHandle` round-trip; `BlockHandle::NULL` round-trip; `Footer` round-trip; `Footer` decodes from tail of larger buffer; `Footer` rejects short input; `Footer` rejects bad magic; block trailer is 5 bytes |
| `sst::block_based::block_builder` | 5 | Single record; restart-interval boundary; `reset` allows reuse; `add` after `finish` panics; `current_size_estimate` grows |
| `sst::block_based::block` | 8 | Single record round-trip; multi-record forward iteration; prefix compression round-trips; `seek` hits exact match; `seek` rounds up; `seek` past end is invalid; `seek_to_last`; backward iteration; 100-record large block with multiple restarts |

## Known simplifications

1. **`SkipList` is single-threaded.** Upstream's `InlineSkipList` is lock-free concurrent; writing that correctly in safe Rust is effectively impossible. Layer 4 will add a `ConcurrentSkipList` inside an `unsafe` module when the engine actually needs concurrent memtable writes.
2. **No merge operand assembly** in the memtable. `Merge` entries are treated as `Value`. The shape is right; the operator wiring comes in Layer 4.
3. **No range-deletion handling** in the memtable. Upstream handles range deletes in a separate aggregator.
4. **Block trailer checksum is FNV-1a, not CRC32C.** `util::hash::hash` is wire-incompatible with upstream; Layer 3b will swap in a real CRC32C.
5. **Footer has no "extended" variant.** Upstream's newer format adds a version byte and a checksum-type byte; Layer 3a uses the legacy 48-byte layout. Layer 3b will add the extended form.
6. **No compression support on blocks.** `BlockBuilder` emits raw bytes; the compression-type byte in the trailer is always `0` (no compression). Compression is a Layer 4 follow-up.
7. **No range tombstone aggregator, no table properties collector, no user-defined timestamp support.** These are all present in upstream but belong in higher layers.

## Change log

- **v0.0.4** — Layer 3a landed. 11 new source files across `db/`, `memtable/`, `sst/`. 169 passing tests (28 Layer 0 + 52 Layer 1 + 42 Layer 2 + 47 Layer 3a), clippy-clean, still zero external dependencies.
