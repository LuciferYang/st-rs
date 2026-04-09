# Layer 3b Rust Port — Implementation Record

Layer 3b finishes the SST format port. Where Layer 3a delivered the atomic
pieces (`InternalKey`, skiplist memtable, `BlockHandle`, `Footer`,
`Block`, `BlockBuilder`), Layer 3b adds the **assembler** that stitches
them into complete SST files plus a real CRC32C and a working bloom filter.

After this commit, `st-rs` can:

1. Build a complete block-based SST end-to-end through
   `BlockBasedTableBuilder` (data blocks + filter + metaindex + index + footer).
2. Open the file via `BlockBasedTableReader` and serve point lookups
   with the filter → index → data block path.
3. Verify every block's CRC32C against the wire-exact upstream
   masked-CRC format.

This is the final piece before the engine layer (Layer 4) can compose
everything into a real write path.

Companion docs:
- `LAYER0-PORT.md` … `LAYER3A-PORT.md` — earlier layers
- `ForSt/FORST-READING-ORDER.md` — the layered reading plan

## What landed

**4 new source files, 1,322 LOC**, plus a small update to `sst/format.rs`
to swap the FNV-1a checksum placeholder for real CRC32C. Still zero
external dependencies, still `#![deny(unsafe_code)]`.

```
src/
├── util/
│   └── crc32c.rs                 ← NEW  table-based CRC-32C + mask/unmask
└── sst/
    ├── format.rs                 ← UPDATED  uses crc32c::mask, ships
    │                                        verify_block_trailer + tests
    └── block_based/
        ├── filter_block.rs       ← NEW  BloomFilterPolicy +
        │                                BloomFilterBuilder + Reader
        ├── table_builder.rs      ← NEW  BlockBasedTableBuilder
        └── table_reader.rs       ← NEW  BlockBasedTableReader
```

## Module summaries

### `util/crc32c.rs` — wire-exact CRC-32C

Table-based Sarwate algorithm: 1 KiB precomputed table, one XOR + table
lookup per byte. Portable, ~30 lines of actual code, produces output
identical to upstream RocksDB and to Python's `crc32c.crc32c`.

| Function | Purpose |
|---|---|
| `crc32c(data)` | One-shot CRC-32C of `data`. |
| `crc32c_extend(init, data)` | Streaming form — used by the block trailer to chain `crc32c(block_bytes) → crc32c_extend(_, &[compression_byte])`. |
| `mask(crc)` | Apply leveldb/RocksDB's "masked CRC" transform: `rotate_right(15) + 0xa282ead8`. Always call before writing the CRC into a block trailer. |
| `unmask(masked)` | Inverse of `mask`. |

**Test vectors** verified against the canonical CRC-32C reference:

| Input | Expected CRC32C |
|---|---|
| `""` | `0x00000000` |
| `"a"` | `0xc1d04330` |
| `"123456789"` | `0xe3069283` (the standard check value) |
| `"hello world"` | `0xc99465aa` |

Plus tests for `mask`/`unmask` round-trip and streaming-vs-one-shot
equivalence.

### `sst/format.rs` — checksum upgrade

The Layer 3a `block_checksum_placeholder` (FNV-1a) is gone. Replaced with:

```rust
fn compute_block_checksum(block_bytes: &[u8], compression_byte: u8) -> u32 {
    let crc = crc32c(block_bytes);
    let crc = crc32c_extend(crc, &[compression_byte]);
    mask(crc)
}
```

This matches upstream's `ComputeBuiltinChecksum` for `kCRC32c` exactly:
the CRC covers the block bytes plus the trailing compression-type byte,
and the result goes through the mask before storage. A block written
by `st-rs` is now byte-equal to a block written by upstream RocksDB
for the same input (modulo the per-record hash function used by the
filter, which is still FNV — see "Known simplifications" below).

A new `verify_block_trailer(block_bytes, trailer)` helper is exported
for the table reader's read path. Returns `Status::Corruption` on a
mismatch, the decoded compression byte on success.

### `sst/block_based/filter_block.rs` — bloom filter

Implements the Layer 0 [`FilterPolicy`](crate::ext::filter_policy::FilterPolicy)
trait via three public types:

| Type | Implements | Purpose |
|---|---|---|
| `BloomFilterPolicy` | `FilterPolicy` | Factory: produces builders/readers, exposes `num_probes()`. Tunable via `bits_per_key` (default 10). |
| `BloomFilterBuilder` | `FilterBitsBuilder` | Accumulates keys during SST construction; `finish()` emits the encoded filter bytes. |
| `BloomFilterReader` | `FilterBitsReader` | Parses an encoded filter and answers `may_match` queries. |

**Encoding**: monolithic full-filter form. The bit array is sized at
`num_keys * bits_per_key` (clamped to ≥64 bits, rounded up to a whole
byte). The number of hash probes `k` is stored in the trailing byte
so readers can recover it from the buffer alone.

**Hash strategy**: double hashing — compute one base hash via
`util::hash::hash`, derive the per-probe stride via a 15-bit rotate,
then walk `k` probes by repeated addition. Same trick upstream's
`FullFilterBitsBuilder` uses.

**FPR check**: a stress test inserts 1000 keys, queries 10000 random
non-members, and asserts the false-positive rate is below 5% (it
typically lands around 0.8% with the default tuning, matching
expectations for `bits_per_key = 10`).

### `sst/block_based/table_builder.rs` — `BlockBasedTableBuilder`

Single-pass SST writer. Configuration via a small struct:

```rust
pub struct BlockBasedTableOptions {
    pub block_size: usize,                       // default 4 KiB
    pub block_restart_interval: usize,           // default 16
    pub bloom_filter_bits_per_key: Option<u32>,  // default Some(10)
}
```

**Lifecycle:**

```rust
let writer = WritableFileWriter::new(fs.new_writable_file(&path, &opts)?);
let mut tb = BlockBasedTableBuilder::new(writer, BlockBasedTableOptions::default());
tb.add(b"k1", b"v1")?;
tb.add(b"k2", b"v2")?;
tb.finish()?;  // takes self by value
```

`finish` consumes `self`, so misuse-after-finish is a compile error.
The internal `add()` method does its own check via a `finished` flag
for the rare case where state is mutated through other code paths
(only the test module exercises this).

**File layout** (matches upstream's legacy block-based format):

```text
data block 0      [trailer]
data block 1      [trailer]
...
filter block      [trailer]   ← if bloom_filter_bits_per_key is Some
metaindex block   [trailer]
index block       [trailer]
footer (48 bytes)
```

**Index strategy**: single-level. The builder maintains a separate
`BlockBuilder` for the index with `restart_interval=1` (so every index
entry is its own restart — easier to binary-search). Each finished
data block emits one index entry with the *previous* `last_key` as
the separator, deferred until the next `add()` call so the builder
sees the boundary.

**Filter wiring**: the builder owns a `Box<dyn FilterBitsBuilder>`
created via `BloomFilterPolicy::new_builder`. Every key is fed into
the filter as it arrives. At finish, the filter is materialised and
written as a single block; its handle goes into the metaindex under
`FILTER_METAINDEX_KEY = "filter.st_rs.BloomFilter"`.

### `sst/block_based/table_reader.rs` — `BlockBasedTableReader`

Read path. Opens an SST through a Layer 2 `RandomAccessFileReader`,
parses the footer, loads the index and (optional) filter into memory,
and serves point lookups.

**Open path:**

1. Read the last 48 bytes → decode `Footer`.
2. Read the index block via `read_block` (which verifies the trailer
   CRC) → wrap in a `Block`.
3. Read the metaindex, search for `FILTER_METAINDEX_KEY`, and if
   present, read the filter block and wrap in a `BloomFilterReader`.

**Get path:**

```rust
pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
    if let Some(filter) = &self.filter {
        if !filter.may_match(key) { return Ok(None); }
    }
    let mut idx_iter = self.index_block.iter();
    idx_iter.seek(key);
    if !idx_iter.valid() { return Ok(None); }
    let (handle, _) = BlockHandle::decode_from(idx_iter.value())?;
    let block = Block::new(read_block(&self.file, handle)?)?;
    let mut it = block.iter();
    it.seek(key);
    if it.valid() && it.key() == key {
        Ok(Some(it.value().to_vec()))
    } else {
        Ok(None)
    }
}
```

`read_block` is a private helper that reads `block.size + 5` bytes
(payload + trailer), verifies the trailer's CRC, and returns the
payload only.

**Out of scope (Layer 4 / 4b):**
- **No block cache.** Every `get` reads the data block fresh from
  the file. Layer 4 will plug in `crate::cache::lru::LruCache` —
  the trait shape is already there.
- **No iterator API.** The reader exposes only point lookup; a full
  forward/backward iterator over the SST will land alongside the
  block-cached read path.
- **No two-level partitioned index.** Single-level only.
- **No prefix bloom.** Full bloom only.

## Verification

| Check | Command | Result |
|---|---|---|
| Clean build | `cargo build` | ✅ 0 warnings, 0 errors |
| Clippy (all targets, `-D warnings`) | `cargo clippy --all-targets -- -D warnings` | ✅ clean |
| Unit tests | `cargo test` | ✅ **200 passed**, 3 ignored doc-tests |
| No unsafe | `#![deny(unsafe_code)]` crate-wide | ✅ enforced |
| Every public item documented | `#![warn(missing_docs)]` | ✅ enforced |
| Zero external dependencies | empty `[dependencies]` | ✅ still std-only |

### Test count by layer

| Layer | Tests |
|---|--:|
| Layer 0 | 28 |
| Layer 1 | 52 (now includes 8 CRC32C tests) |
| Layer 2 | 42 |
| Layer 3a | 47 |
| **Layer 3b (new)** | **31** |
| **Total** | **200** |

### Layer 3b test breakdown

| Module | Tests | What they verify |
|---|--:|---|
| `util::crc32c` | 8 | Empty input; single byte; canonical "123456789" check value; "hello world"; extend equivalence; extend on empty; mask/unmask round trip; mask is non-identity; large buffer matches piecewise |
| `sst::format` (additions) | 4 | Trailer round-trip; trailer detects CRC corruption; trailer detects body tamper; rejects wrong length |
| `sst::block_based::filter_block` | 6 | No false negatives for inserted keys; FPR is reasonable on random keys; degenerate empty reader matches everything; builder reuse after finish; `k` derivation matches formula; name is stable |
| `sst::block_based::table_builder` | 6 | Finishes with valid footer; empty table still emits footer; many records produce multiple data blocks; `num_entries` tracks `add` calls; `add` after `finish` panics; in-memory writer satisfies the trait |
| `sst::block_based::table_reader` | 7 | Single-record round-trip; 1000-record round-trip with multi-block SST; filter doesn't reject true positives; no filter when disabled; open rejects too-small file; corruption in data block surfaces on get |

The 1000-record round-trip is the headline test: build a 1000-entry
SST with `block_size=256` (forcing many data blocks), open it,
spot-check 7 hits and 2 misses. End-to-end exercise of every Layer
3b component.

## Known simplifications

1. **No block cache in the reader.** Every `get` reads the data
   block from the file. Layer 4 will plug in the existing
   `LruCache` from Layer 2.
2. **No SST iterator** — only point lookup. A full forward/backward
   iterator across an SST is needed for the engine read path and
   will arrive together with the block cache.
3. **No two-level partitioned index.** Single-level only. Upstream
   uses a two-level index for SSTs above ~1 GB to keep the resident
   index size bounded.
4. **No compression on data blocks.** Every block is written with
   compression byte 0 (`kNoCompression`). Adding Snappy/Zstd is
   mechanical once those crates are added; the trailer machinery
   already handles a non-zero compression byte.
5. **No prefix bloom filter.** Only full bloom. Prefix-extractor
   plumbing is in `ColumnFamilyOptions` already; the filter side
   needs a separate prefix-aware builder.
6. **Bloom filter hash is FNV-1a, not upstream's `Hash`.** Same
   limitation as Layer 1's `util::hash`. Within `st-rs`, builder
   and reader share the same hash so intra-crate round trips work.
   When `util::hash` is made wire-compatible with upstream, no
   changes to `filter_block.rs` are required.
7. **CRC32C is table-based**, not the SSE 4.2 intrinsic upstream
   uses on x86. ~10× slower but portable and bit-equal. A `cfg`
   fast path can drop in later.
8. **No range deletion block.** Upstream emits a separate
   `range_del` block for tombstones spanning multiple keys; that
   belongs in Layer 4 alongside the range-delete aggregator.
9. **No table properties block.** Per-SST stats (smallest/largest
   key, num entries, num data blocks, …) are tracked in
   `BlockBasedTableBuilder` internally but not yet emitted as a
   block. Layer 4 will add it once the engine starts caring.

## Change log

- **v0.0.5** — Layer 3b landed. 4 new source files (1322 LOC) plus
  the `sst/format.rs` checksum upgrade. 200 passing tests, clippy
  clean, still zero dependencies. The crate can now build complete
  SSTs end-to-end and read them back through a CRC-verified path.
