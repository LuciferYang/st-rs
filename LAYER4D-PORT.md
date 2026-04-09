# Layer 4d Rust Port — Implementation Record

Layer 4d is **the invisible refactor that unlocks everything after it**.
Externally, every Layer 4c test still passes unchanged — the public API
behaves identically. Internally, the on-disk SST format now retains
**every version** of every key rather than collapsing to one entry per
user key at flush time. This is the structural change that makes
snapshots, multi-level compaction, and MVCC reads possible in the
layers that follow.

The key insight is a small encoding change: the trailing 8 bytes of an
`InternalKey` are now **big-endian and bitwise-inverted** instead of
little-endian and raw. With this encoding, **bytewise lexicographic
order matches `(user_key asc, seq desc, type desc)`** — the canonical
MVCC order — which means every byte-ordered container in the engine
(skiplist, block builder, block reader, block iterator) handles
internal keys correctly with **zero code changes** to those
containers. The only files that had to move are the three that
actually know about `(user_key, seq, type)` as a structured tuple.

Companion docs:
- `LAYER0-PORT.md` … `LAYER4C-PORT.md` — earlier layers
- `ForSt/FORST-READING-ORDER.md` — the layered reading plan

## What landed

**No new source files** this time — Layer 4d is a refactor.
Substantial updates to `db/dbformat.rs`, `db/db_impl.rs`,
`db/db_iter.rs`, and `db/compaction.rs`. Still safe Rust, still zero
external dependencies.

```
src/db/
├── dbformat.rs         ← UPDATE  BE-inverted trailer encoding
│                                  for InternalKey + LookupKey;
│                                  InternalKeyComparator simplified
│                                  to bytewise
├── db_impl.rs          ← UPDATE  flush writes internal keys
│                                  directly (no dedup); get()
│                                  seeks SSTs via LookupKey and
│                                  parses the result; open()
│                                  scans SSTs to recover
│                                  last_sequence
├── db_iter.rs          ← UPDATE  SstUserKeyIter parses internal
│                                  keys, presents a user-key view
│                                  with duplicate-skipping
└── compaction.rs       ← UPDATE  CompactionJob collects every
                                    (internal_key, value) pair
                                    from inputs, sorts bytewise,
                                    dedups by first-per-user-key,
                                    drops tombstones
```

## The encoding change

**Before** (Layer 3a … 4c):

```text
  +-----------+-----------------+
  |  user_key | (seq<<8|type)   |
  |           |    as LE u64    |
  +-----------+-----------------+
```

**After** (Layer 4d+):

```text
  +-----------+---------------------+
  |  user_key | ~(seq<<8|type)      |
  |           |       as BE u64     |
  +-----------+---------------------+
```

**Why bytewise lex order matches MVCC order:**

- **Big-endian**: the most-significant byte of the trailer comes first,
  so numeric comparison of the trailer matches lexicographic comparison
  of its bytes.
- **Inverted**: a *larger* sequence (numerically) becomes a *smaller*
  inverted value, which in BE bytes becomes lexicographically smaller,
  which sorts earlier. So larger seq = sorts earlier = newest-first,
  exactly what MVCC wants.

Walk through the three cases:

1. **Different user keys**: bytewise compare stops at the first
   differing user-key byte. Internal-key order == user-key order == user
   comparator order (assumed `BytewiseComparator`). ✓
2. **Same user key, different seq**: bytewise compare walks past the
   shared user-key bytes into the trailer. Larger seq → smaller
   inverted → smaller BE bytes → sorts earlier. ✓
3. **Same user key, same seq, different type**: the type byte is the
   low byte of the packed value. Larger type → smaller inverted → sorts
   earlier. `VALUE_TYPE_FOR_SEEK = 0x17` (the largest known type)
   therefore sorts *immediately before* any real entry at the same
   sequence, which is what a `LookupKey` needs. ✓

**Tradeoff**: only `BytewiseComparator` is supported as the user
comparator. Non-bytewise comparators (e.g. reverse, natural-number, or
user-supplied) would break the ordering invariant. A future layer can
thread an explicit comparator through `BlockBuilder` / `BlockIter` if
that becomes necessary.

## Module updates

### `db/dbformat.rs`

- `InternalKey::new`: writes `(!packed).to_be_bytes()` into the trailer
  slot instead of `packed.to_le_bytes()`.
- `InternalKey::packed`: reads `from_be_bytes` and flips.
- `ParsedInternalKey::parse`: same BE-inverted decode.
- `LookupKey::new`: same BE-inverted encode for the trailer.
- `InternalKeyComparator::cmp`: **simplified to `a.cmp(b)`**
  (bytewise). The `user_comparator` field is retained for API
  stability but the cmp implementation no longer consults it. The
  module-level doc explains why.

Every `dbformat` test still passes. The round-trip tests (`new` →
`parse` → check fields) exercise the new encoding end-to-end; the
ordering tests (`internal_comparator_orders_sequence_desc_on_tie`,
`lookup_key_sorts_before_equal_sequence`) confirm the invariants still
hold.

### `db/db_impl.rs`

**`write_immutable_to_sst`** — the flush path. Before Layer 4d:

```rust
while it.valid() {
    let parsed = ParsedInternalKey::parse(it.key())?;
    let is_dup = ... ;   // dedup by user key
    if !is_dup {
        let value = match parsed.value_type {
            ValueType::Value | ValueType::Merge => it.value(),
            _ => &[],                              // tombstone
        };
        tb.add(parsed.user_key, value)?;           // USER key
        last_user_key = Some(parsed.user_key.to_vec());
    }
    it.next();
}
```

After Layer 4d:

```rust
while it.valid() {
    tb.add(it.key(), it.value())?;                 // INTERNAL key
    it.next();
}
```

The memtable is already sorted in internal-key order, so we stream its
entries straight into the block builder. No dedup. No type-based value
rewriting. Every version survives.

**`get`** — the point-lookup path. Before:

```rust
for table in ssts {
    if let Some(v) = table.get(key)? {
        if v.is_empty() { return Ok(None); }       // tombstone marker
        return Ok(Some(v));
    }
}
```

After:

```rust
for table in &ssts {
    let mut it = table.iter();
    it.seek(lookup.internal_key());
    if !it.valid() { continue; }
    let parsed = match ParsedInternalKey::parse(it.key()) {
        Ok(p) => p,
        Err(_) => continue,
    };
    if parsed.user_key != key { continue; }
    return Ok(match parsed.value_type {
        ValueType::Value | ValueType::Merge => Some(it.value().to_vec()),
        ValueType::Deletion | ValueType::SingleDeletion => None,
        _ => None,
    });
}
```

Each SST gets a fresh iterator positioned at the lookup key. The seek
lands on the newest visible version for `key` (or beyond it). The
user-key check distinguishes "we found an older newer-user-key entry"
from "we actually have `key`".

**`open_with_fs`** — the reopen path. Added a new step before WAL
replay:

```rust
// Initialise `last_sequence` from the existing SSTs. Each SST holds
// internal keys whose trailer encodes the sequence, so we walk every
// entry and keep the maximum. Without this step, reopening a DB that
// was closed cleanly (empty WAL, all data in SSTs) would reset
// `last_sequence` to 0 and subsequent `LookupKey`s would sort *after*
// every real entry, causing every `get()` to miss.
let mut last_sequence = 0u64;
for entry in &ssts {
    let mut it = entry.reader.iter();
    it.seek_to_first();
    while it.valid() {
        let parsed = ParsedInternalKey::parse(it.key())?;
        if parsed.sequence > last_sequence {
            last_sequence = parsed.sequence;
        }
        it.next();
    }
    it.status()?;
}
```

This is the one clear cost of deferring a real manifest: on open we
have to scan every entry in every SST to recover `last_sequence`.
O(data) but only once per open, and a follow-up layer can persist
`last_sequence` in `CURRENT` (or a real `MANIFEST`) to avoid the scan.

### `db/db_iter.rs` — `SstUserKeyIter` parses internal keys

Layer 4c's `SstUserKeyIter` assumed SSTs stored user keys with an
empty-value tombstone marker — a passthrough over `SstIter`. Layer 4d
replaces it with a structurally identical clone of
`MemtableUserKeyIter`: it parses each entry's internal key, presents
the user-key view, and skips duplicates on `next` so the merging
iterator only sees the newest version per user key.

```rust
impl<'a> UserKeyIter for SstUserKeyIter<'a> {
    fn valid(&self) -> bool { self.valid }
    fn key(&self) -> &[u8] { &self.cur_key }
    fn value(&self) -> &[u8] { &self.cur_value }
    fn seek_to_first(&mut self) { self.inner.seek_to_first(); self.read_current(); }
    fn seek(&mut self, target: &[u8]) {
        let lookup = LookupKey::new(target, u64::MAX >> 8);
        self.inner.seek(lookup.internal_key());
        self.read_current();
    }
    fn next(&mut self) { self.skip_duplicates(); self.read_current(); }
}
```

`read_current` parses the current SST entry's internal key into the
`cur_key` / `cur_value` buffers, dispatching on `value_type` to
determine whether the result is a live value or a tombstone.
`skip_duplicates` walks past every subsequent entry sharing the
current `cur_key`.

### `db/compaction.rs` — internal-key aware merge

Layer 4c's `CompactionJob::run` went through a `MergingIterator` of
`SstUserKeyIter`s and wrote user keys into the output. Layer 4d
rewrites it to operate on internal keys directly:

```rust
pub fn run(self) -> Result<usize> {
    // 1. Collect every (internal_key, value) pair from every input.
    let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    for reader in &self.inputs {
        let mut it = reader.iter();
        it.seek_to_first();
        while it.valid() {
            entries.push((it.key().to_vec(), it.value().to_vec()));
            it.next();
        }
        it.status()?;
    }
    // 2. Sort bytewise — yields (user_key asc, seq desc, type desc)
    //    thanks to the BE-inverted encoding.
    entries.sort_by(|a, b| a.0.cmp(&b.0));

    // 3 + 4. Dedup by user key (first wins = newest), drop tombstones.
    let mut tb = ... ;
    let mut written = 0;
    let mut last_user_key: Option<Vec<u8>> = None;
    for (ikey, value) in &entries {
        let parsed = ParsedInternalKey::parse(ikey)?;
        if last_user_key.as_deref() == Some(parsed.user_key) {
            continue;                             // older version
        }
        last_user_key = Some(parsed.user_key.to_vec());
        match parsed.value_type {
            ValueType::Value | ValueType::Merge => {
                tb.add(ikey, value)?;             // INTERNAL key
                written += 1;
            }
            _ => {}                                // tombstone, drop
        }
    }
    tb.finish()?;
    Ok(written)
}
```

O(N) memory — every entry from every input SST is materialised. Fine
for Layer 4d; Layer 4e will replace this with a streaming k-way merge.

## Verification

| Check | Command | Result |
|---|---|---|
| Clean build | `cargo build` | ✅ 0 warnings, 0 errors |
| Clippy (all targets, `-D warnings`) | `cargo clippy --all-targets -- -D warnings` | ✅ clean |
| Unit tests | `cargo test` | ✅ **255 passed**, 3 ignored doc-tests |
| No unsafe | `#![deny(unsafe_code)]` crate-wide | ✅ enforced |
| Every public item documented | `#![warn(missing_docs)]` | ✅ enforced |
| Zero external dependencies | empty `[dependencies]` | ✅ still std-only |

### Test count by layer

| Layer | Tests |
|---|--:|
| Layer 0 | 28 |
| Layer 1 | 60 |
| Layer 2 | 42 |
| Layer 3a | 47 |
| Layer 3b | 31 |
| Layer 4a | 14 |
| Layer 4b | 27 |
| Layer 4c | 5 |
| **Layer 4d (new)** | **1** |
| **Total** | **255** |

Layer 4d only adds one new test because **its correctness guarantee is
"Layer 4a–4c tests still pass"**. The refactor is invisible from the
outside. The one new test,
`sst_retains_multiple_versions_per_user_key`, proves the invisible
change: it writes three versions of the same key to a single SST and
asserts that all three survive as separate entries inside the file.

### Headline test

`sst_retains_multiple_versions_per_user_key`:

```rust
let db_opts = DbOptions {
    create_if_missing: true,
    db_write_buffer_size: 64 * 1024,    // don't auto-flush mid-put
    ..DbOptions::default()
};
let db = DbImpl::open(&db_opts, &dir)?;
db.put(b"k", b"v1")?;
db.put(b"k", b"v2")?;
db.put(b"k", b"v3")?;
db.flush()?;
db.wait_for_pending_flush()?;

// Reading through the engine still returns the newest version...
assert_eq!(db.get(b"k")?, Some(b"v3".to_vec()));

// ...but the raw SST file retains ALL THREE versions.
let versions: Vec<_> = table.iter()
    .filter(|e| ParsedInternalKey::parse(e.key()).unwrap().user_key == b"k")
    .map(|e| (parse(e.key()).sequence, e.value().to_vec()))
    .collect();
assert_eq!(versions.len(), 3);
assert_eq!(versions[0].1, b"v3");   // newest first (internal-key order)
assert_eq!(versions[1].1, b"v2");
assert_eq!(versions[2].1, b"v1");
```

Before Layer 4d, the SST would have exactly one entry for `k` (value
`v3`). After Layer 4d, it has three. This is the structural change
that makes snapshots viable.

## Known simplifications (deferred to Layer 4e)

1. **No snapshot API.** `Db::snapshot()` and `get_at(key, snapshot)`
   aren't yet exposed. The on-disk format now supports them; Layer 4e
   will add the API + snapshot-aware compaction (preserving versions
   above the oldest live snapshot).
2. **Compaction still drops all tombstones.** Because there are no
   snapshots yet, every compaction is effectively bottommost. When
   snapshots land, compaction will need a `min_snap_seq` to decide
   which versions and tombstones can be dropped.
3. **`open_with_fs` scans every SST entry to recover `last_sequence`.**
   O(data) on reopen. A `MANIFEST` (or an extended `CURRENT`) can
   persist the value and skip the scan.
4. **Compaction's merge is O(N) memory.** Materialises every entry
   from every input. Streaming k-way merge is a Layer 4e follow-up.
5. **Non-bytewise user comparators are unsupported.** The BE-inverted
   encoding assumes `BytewiseComparator`. A layer that needs custom
   comparators will have to thread an explicit comparator through
   `BlockBuilder` / `BlockIter`.
6. **No multi-level layout** — L0/L1/L2 still collapsed into a single
   "newest-first" SST list. Layer 4e.
7. **No column families, still no full `Db` trait impl** — same as
   Layer 4c.
8. **Background compaction still runs synchronously inside the flush
   worker** — same as Layer 4c.

## Change log

- **v0.0.9** — Layer 4d landed. No new source files; substantial
  updates to `dbformat.rs`, `db_impl.rs`, `db_iter.rs`, `compaction.rs`.
  255 passing tests, clippy clean, still zero dependencies. SSTs now
  store internal keys with a BE-inverted trailer, so bytewise lex
  order matches MVCC order and every byte-ordered container in the
  engine handles internal keys transparently.
