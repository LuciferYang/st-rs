# Layer 4b Rust Port — Implementation Record

Layer 4b makes the engine *real*. Layer 4a was the proof-of-concept
("the WAL works, basic put/get works"); Layer 4b adds the pieces a
production user actually needs:

- A **forward iterator over a complete SST** (`SstIter`) — required
  by every read path that wants more than point lookup.
- A **k-way merging iterator** (`MergingIterator`) — used by both
  the compaction job and (indirectly) the user-facing iterator.
- A **user-facing `DbIterator`** with seek / next / prev /
  seek_to_first / seek_to_last / seek_for_prev. Materialised eagerly
  under the engine lock so the iterator outlives concurrent writes.
- **Compaction** (`CompactionJob`) that merges multiple SSTs into
  one, deduping by user key (newer wins) and dropping tombstones.
- **Automatic compaction** triggered inside `DbImpl::flush` once the
  live SST count crosses a threshold.

After this commit, a user can:

```rust
let db = DbImpl::open(&opts, path)?;
for i in 0..10_000 {
    db.put(format!("key{i:05}").as_bytes(), b"value")?;
    if i % 1000 == 999 { db.flush()?; }   // forces SST creation
}
// Range scan via iterator:
let mut iter = db.iter()?;
iter.seek(b"key01000");
while iter.valid() && iter.key() < b"key02000".as_slice() {
    process(iter.key(), iter.value());
    iter.next();
}
```

and the engine will automatically compact accumulated SSTs in the
background of `flush()` calls so that read amplification stays
bounded.

Companion docs:
- `LAYER0-PORT.md` … `LAYER4A-PORT.md` — earlier layers
- `ForSt/FORST-READING-ORDER.md` — the layered reading plan

## What landed

**4 new source files, 1,598 LOC**, plus updates to `db_impl.rs`,
`db/mod.rs`, `sst/block_based/{mod,table_reader}.rs`, and `lib.rs`.
Still safe Rust, still zero external dependencies.

```
src/
├── sst/block_based/
│   ├── sst_iterator.rs              ← NEW  Two-level iterator
│   │                                       (index → data block)
│   └── table_reader.rs              ← UPDATE  iter() + pub(crate)
│                                              accessors
└── db/
    ├── merging_iterator.rs          ← NEW  k-way merge over
    │                                       UserKeyIter sources
    ├── db_iter.rs                   ← NEW  MemtableUserKeyIter,
    │                                       SstUserKeyIter, eager
    │                                       DbIterator
    ├── compaction.rs                ← NEW  pick_compaction +
    │                                       CompactionJob
    └── db_impl.rs                   ← UPDATE  iter() method,
                                              automatic compaction
                                              after every flush
```

## Module summaries

### `sst/block_based/sst_iterator.rs` — `SstIter`

A two-level iterator that walks the index block to discover data
block handles, fetches each data block via the table reader's
`pub(crate)` `read_block_bytes` accessor, and parses records inline.

**Why inline parsing instead of reusing `BlockIter`?** A `BlockIter`
borrows from a `Block`. If the SstIter held both an owned `Block`
*and* a `BlockIter` that borrowed from it, that's a textbook
self-referential lifetime problem in safe Rust. The fix is to store
just the block bytes + a cursor + a key buffer, and parse records
manually. About 50 lines of duplicated logic, no `unsafe`, no
external crates.

The iterator supports:
- `seek_to_first` + `next` for full forward scans
- `seek(target)` — uses the index block's binary search to find
  the right data block, then linearly scans inside it

Backward iteration is deferred to Layer 4c (compaction and the
read path don't need it).

### `sst/block_based/table_reader.rs` — `iter()` + accessors

Three small additions to `BlockBasedTableReader`:

```rust
impl BlockBasedTableReader {
    pub fn iter(&self) -> SstIter<'_> { ... }
    pub(crate) fn index_block(&self) -> &Block { ... }
    pub(crate) fn read_block_bytes(&self, handle: BlockHandle) -> Result<Vec<u8>> { ... }
}
```

The `pub(crate)` accessors are for `SstIter` only — the public API
is still just `get` + `iter`.

### `db/merging_iterator.rs` — `UserKeyIter` + `MergingIterator`

The `UserKeyIter` trait is the contract every source must implement:

```rust
pub trait UserKeyIter {
    fn valid(&self) -> bool;
    fn key(&self) -> &[u8];   // user key
    fn value(&self) -> &[u8]; // empty = tombstone
    fn is_tombstone(&self) -> bool { self.value().is_empty() }
    fn seek_to_first(&mut self);
    fn seek(&mut self, target: &[u8]);
    fn next(&mut self);
    fn status(&self) -> Result<()> { Ok(()) }
}
```

`MergingIterator` k-way-merges a `Vec<Box<dyn UserKeyIter>>`. The
algorithm is a deliberately naive O(k) linear scan to find the
smallest current key on each `next` — fine for `k <= 32` and avoids
the complexity of a custom heap-with-comparator. A future Layer 4c
can swap in `util::heap` for hot paths.

**Tie-breaking**: when two sources hold the same user key, the
**first source** in the input vector wins. Other matching sources
are advanced past the duplicate. Callers express priority by
ordering: pass the memtable first, then SSTs newest-first.

### `db/db_iter.rs` — adapters + the user-facing iterator

Three things:

**`MemtableUserKeyIter`** wraps the memtable's internal-key
`SkipListIter` and presents a user-key view. The internal key
ordering already places the newest version first per user key, so
this wrapper just decodes the current entry and skips duplicates
on `next()`. Tombstones (Deletion / SingleDeletion) are exposed as
empty values.

**`SstUserKeyIter`** is a thin passthrough over `SstIter`. SSTs
already store user keys, so it's mostly a delegate that reports
empty values as tombstones.

**`DbIterator`** is the user-facing iterator returned by
`DbImpl::iter()`. **It is eager** — at construction time it
materialises every live (non-tombstone) entry into a sorted
`Vec<(Vec<u8>, Vec<u8>)>`, then walks the vec on each step. This
is O(N) memory and time per `iter()` call, but it sidesteps the
self-referential lifetime trap a streaming `MergingIterator`-backed
iterator would hit (the iterator would need to own the table
readers AND borrow from them). Layer 4c can introduce a streaming
variant behind `unsafe`.

The iterator supports the full set: `seek_to_first` /
`seek_to_last` / `seek` / `seek_for_prev` / `next` / `prev`. The
backward operations are O(1) thanks to the materialised vec.

### `db/compaction.rs` — `CompactionJob`

```rust
pub fn pick_compaction(num_ssts: usize, trigger: usize) -> Option<Vec<usize>>;

pub struct CompactionJob<'a> { ... }
impl<'a> CompactionJob<'a> {
    pub fn new(fs, inputs, output_path, table_options) -> Self;
    pub fn run(self) -> Result<usize>;  // returns # of records written
}
```

The picker policy is dead simple: if the number of SSTs is at least
the trigger, return *all* of them as candidates for compaction. A
real engine would compact only the oldest few and respect overlap
ranges, but a single all-encompassing compaction is correct (just
slower on large databases).

The job itself opens an `SstIter` per input, wraps each in an
`SstUserKeyIter`, builds a `MergingIterator` (newest-first
priority), opens a fresh `BlockBasedTableBuilder` for the output,
and streams the merged stream through it. **Tombstones are dropped
outright** at this layer because there's no level below — there's
nothing older to shadow.

Three integration tests cover:
- Two disjoint SSTs merged into one with all six records
- Two overlapping SSTs where the newer's value wins
- Tombstones in the newer SST cause the matching keys to disappear

### `db/db_impl.rs` — updates

Two new things:

**`iter()`**:
```rust
pub fn iter(&self) -> Result<DbIterator> {
    let state = self.state.lock().unwrap();
    let ssts: Vec<Arc<BlockBasedTableReader>> =
        state.ssts.iter().map(|e| Arc::clone(&e.reader)).collect();
    let it = DbIterator::from_snapshot(&state.memtable, &ssts)?;
    drop(state);
    Ok(it)
}
```

The lock is held only long enough to materialise the snapshot;
once `from_snapshot` returns, the iterator is fully self-contained
and concurrent writes don't affect it.

**Automatic compaction** at the end of `flush()`:

```rust
const COMPACTION_TRIGGER: usize = 4;

// ... after writing the new SST and updating CURRENT ...
let to_compact = pick_compaction(state.ssts.len(), Self::COMPACTION_TRIGGER);
let plan = to_compact.map(|indices| {
    let inputs = indices.iter().map(|&i| Arc::clone(&state.ssts[i].reader)).collect();
    let input_numbers = indices.iter().map(|&i| state.ssts[i].number).collect();
    let out_number = state.next_file_number;
    state.next_file_number += 1;
    (out_number, inputs, input_numbers)
});
drop(state);
if let Some((out_num, inputs, nums)) = plan {
    self.run_compaction(out_num, inputs, nums)?;
}
```

`run_compaction` runs the merge **without** holding the engine
lock, then re-acquires the lock just to swap inputs for output in
`state.ssts` and to delete the input files. This means concurrent
`get` / `iter` / `put` calls don't block on compaction I/O.

## Layer 0 trait collision

The Layer 0 abstract iterator is named `DbIterator` (renamed from
upstream's `Iterator` to avoid colliding with `core::iter::Iterator`).
The Layer 4b concrete struct is *also* `DbIterator` because that's
the natural name for "the thing `db.iter()` returns".

Resolution: at the crate root, the **concrete struct** is exposed
as `st_rs::DbIterator` because it's what users actually call. The
Layer 0 trait is reachable via the longer path
`st_rs::api::iterator::DbIterator`. A documentation comment in
`lib.rs` explains the choice.

Future Layer 4c will probably implement the trait *for* the
concrete struct, at which point both will coexist under the same
name without confusion.

## Verification

| Check | Command | Result |
|---|---|---|
| Clean build | `cargo build` | ✅ 0 warnings, 0 errors |
| Clippy (all targets, `-D warnings`) | `cargo clippy --all-targets -- -D warnings` | ✅ clean |
| Unit tests | `cargo test` | ✅ **241 passed**, 3 ignored doc-tests |
| No unsafe | `#![deny(unsafe_code)]` crate-wide | ✅ enforced |
| Every public item documented | `#![warn(missing_docs)]` | ✅ enforced |
| Zero external dependencies | empty `[dependencies]` | ✅ still std-only |

### Test count by layer

| Layer | Tests |
|---|--:|
| Layer 0 | 28 |
| Layer 1 | 52 |
| Layer 2 | 42 |
| Layer 3a | 47 |
| Layer 3b | 31 |
| Layer 4a | 14 |
| **Layer 4b (new)** | **27** |
| **Total** | **241** |

### Layer 4b test breakdown

| Module | Tests | What they verify |
|---|--:|---|
| `sst::block_based::sst_iterator` | 4 | Empty iter; single-block forward scan; multi-block forward scan (200 records); seek lands at-or-after target |
| `db::merging_iterator` | 6 | Empty merge; single source; two interleaved sources; first-source-wins on tie; tombstones visible; `seek` advances all sources |
| `db::db_iter` | 7 | Empty iterator; walks memtable entries; skips tombstones; newer versions shadow older; `seek` lands at-or-after; `seek_for_prev` works; backward iteration |
| `db::compaction` | 5 | `pick_compaction` returns None below trigger; returns all at trigger; merges two disjoint SSTs; dedups newer wins; drops tombstones |
| `db::db_impl` | 5 (new) | `iter` walks memtable only; `iter` merges memtable + SST; `iter` skips deleted keys; auto compaction collapses SSTs; iter after compaction returns correct data |

### Headline test

**`auto_compaction_collapses_ssts`** in `db_impl::tests`: issue 5
flushes (50 puts total). The 4th flush hits the compaction
trigger, and the test verifies that:

1. After all flushes, every key is still readable
2. The SST count has shrunk below the number of flushes (compaction
   merged some away)

End-to-end exercise of the new pipeline:
`Put → WAL → memtable → flush → SST → flush → SST → ... → trigger
→ pick_compaction → CompactionJob::new → SstIter × N →
SstUserKeyIter × N → MergingIterator → BlockBasedTableBuilder →
new SST on disk → swap state.ssts → delete inputs`.

## Known simplifications (deferred to Layer 4c)

1. **`DbIterator` is eager.** Materialises every live entry into a
   `Vec` at construction. O(N) memory; fine for thousands of keys
   but not millions. Layer 4c streaming variant requires `unsafe`.
2. **No background compaction thread** — `flush()` runs the
   compaction synchronously after writing the new SST. Wall-clock
   latency of `put()` is unaffected most of the time, but the call
   that triggers compaction sees the cost. Layer 4c will spawn
   compaction on the `StdThreadPool`.
3. **No multi-level layout (L0/L1/L2…).** Every SST is "the same
   level"; compaction merges all of them at once. A real engine
   maintains levels with size-based triggers and overlap
   constraints. Mostly a tuning concern at this scale.
4. **No snapshot retention.** Without snapshots, every compaction
   is bottommost — tombstones can be dropped outright. When Layer
   4c adds explicit snapshots, compaction will need to track the
   oldest live snapshot and retain tombstones above it.
5. **`prev` on `SstIter` not implemented.** The block-based format
   does support backward iteration (Layer 3a's `BlockIter` has
   `prev()`); reusing it from `SstIter` requires either rebuilding
   the iterator across blocks (slow) or implementing a separate
   "current data block iter" state. Forward-only is enough for
   compaction and for the eager `DbIterator`.
6. **`MergingIterator` is naive O(k) per `next`.** With `k <= 32`
   this is fine. For larger k a `util::heap`-based version will
   land in Layer 4c.
7. **Compaction picks "all SSTs" unconditionally** when the trigger
   fires. A real picker would respect overlap ranges and pick the
   minimum set whose merge would reduce the level count.
8. **No real `Db` trait impl yet.** `DbImpl` is still a concrete
   struct with the engine API. Layer 4c will implement the Layer 0
   `Db` trait on top of it once column families and snapshots
   land.

## Change log

- **v0.0.7** — Layer 4b landed. 4 new source files (1598 LOC),
  241 passing tests, clippy clean, still zero dependencies. The
  engine now exposes a working forward + backward iterator and
  automatically compacts accumulated SSTs to keep read amplification
  bounded.
