# Layer 4e Rust Port — Implementation Record

Layer 4e exposes what Layer 4d made possible: **explicit snapshots**.
A user can now take a snapshot, keep writing (including overwrites
and deletes), and later read **through the snapshot** to see the
world as it was when the snapshot was taken — even after flush and
compaction have rewritten the underlying SSTs.

This is the payoff for Layer 4d's "invisible" format refactor. Where
Layer 4d stored every version in the SST but offered no way to read
them, Layer 4e adds the three pieces needed for snapshot-correct
MVCC:

1. **A snapshot handle** (`DbSnapshot`) that pins a sequence number
   and is tracked by the engine.
2. **Read-path methods** (`get_at`, `iter_at`) that take a snapshot
   and filter every source — memtable, immutable memtable, SSTs — by
   `seq <= snapshot.sequence_number()`.
3. **Snapshot-aware compaction** that preserves versions above the
   oldest live snapshot and retains the newest version at or below
   it per user key, so snapshot reads still find their data after
   compaction runs.

Companion docs:
- `LAYER0-PORT.md` … `LAYER4D-PORT.md` — earlier layers
- `ForSt/FORST-READING-ORDER.md` — the layered reading plan

## What landed

**No new source files** — Layer 4e is a vertical feature cutting
across four existing modules. Still safe Rust, still zero external
dependencies.

```
src/db/
├── dbformat.rs        ← (unchanged in 4e; used as the fundamental
│                         sequence + type encoding)
├── db_impl.rs         ← UPDATE: DbSnapshot type, snapshot(),
│                                get_at(), iter_at(),
│                                release_snapshot_internal,
│                                snapshot-aware get path,
│                                min_snap_seq propagation into
│                                compaction
├── db_iter.rs         ← UPDATE: MemtableUserKeyIter::new_at,
│                                SstUserKeyIter::new_at,
│                                DbIterator::from_snapshot_with_immutable_at,
│                                READ_AT_LATEST constant
└── compaction.rs      ← UPDATE: CompactionJob.min_snap_seq +
                                  with_min_snap_seq builder; the
                                  run() loop applies the retention
                                  rule described below
```

## The user-facing API

```rust
use st_rs::{DbImpl, DbOptions};

let opts = DbOptions { create_if_missing: true, ..DbOptions::default() };
let db = DbImpl::open(&opts, path)?;

db.put(b"k", b"old")?;
let snap = db.snapshot();          // Arc<DbSnapshot>
db.put(b"k", b"new")?;              // after the snapshot

// Current read sees the newest value.
assert_eq!(db.get(b"k")?, Some(b"new".to_vec()));

// Snapshot read sees the pre-snapshot value.
assert_eq!(db.get_at(b"k", &*snap)?, Some(b"old".to_vec()));

// Same for iterators.
let snap_iter = db.iter_at(&*snap)?;      // iterator at the snapshot
let live_iter = db.iter()?;                // iterator at "now"

// Release the snapshot explicitly (or drop the Arc).
drop(snap);
```

The snapshot is **refcounted**: every `Arc::clone` extends its
lifetime, and the Drop impl fires only when the last Arc is dropped.
The engine tracks live snapshots in a `BTreeMap<seq, refcount>` so
it can compute `min_snap_seq` — the oldest live snapshot — at
compaction time.

## `DbSnapshot` — the engine's snapshot type

```rust
pub struct DbSnapshot {
    seq: SequenceNumber,
    db: Weak<DbImpl>,
}

impl crate::api::snapshot::Snapshot for DbSnapshot {
    fn sequence_number(&self) -> SequenceNumber { self.seq }
}

impl Drop for DbSnapshot {
    fn drop(&mut self) {
        if let Some(db) = self.db.upgrade() {
            db.release_snapshot_internal(self.seq);
        }
        // Else: the engine has been dropped. Nothing to release.
    }
}
```

Key design points:

- **`Weak<DbImpl>` in Drop**: prevents a reference cycle. The engine
  knows about snapshots (it tracks `min_snap_seq`); snapshots know
  about the engine (via Drop). If we used `Arc<DbImpl>` in the
  snapshot, dropping the user's last `Arc<DbImpl>` wouldn't actually
  free the engine because held-forever snapshots would keep it
  alive. `Weak` breaks the cycle.
- **Implements the Layer 0 `Snapshot` trait** so callers can use it
  through `&dyn Snapshot` in `get_at` / `iter_at`.
- **Implements Debug** because `unwrap_err` on a `Result<Arc<DbSnapshot>, _>`
  would otherwise fail to compile in tests.
- **`sequence()` convenience method** alongside the trait's
  `sequence_number()` — same value, shorter name.

## Snapshot tracking in `DbState`

```rust
struct DbState {
    // ...existing fields...

    /// Live snapshot multiset: `seq → refcount`. Used to compute
    /// `min_snap_seq` for snapshot-aware compaction.
    snapshots: BTreeMap<SequenceNumber, usize>,
}

impl DbState {
    fn min_snap_seq(&self) -> SequenceNumber {
        self.snapshots
            .keys()
            .next()
            .copied()
            .unwrap_or(SequenceNumber::MAX)
    }
}
```

- **`BTreeMap` not `BTreeSet`**: multiple snapshots can pin the same
  sequence (take two snapshots in quick succession before any
  intervening writes). The `usize` value is a refcount so dropping
  one doesn't prematurely release the sequence.
- **`u64::MAX` sentinel**: when there are no live snapshots,
  `min_snap_seq()` returns `MAX`. The compaction retention rule
  naturally degrades in this case to "keep only the newest version
  per user key" (Layer 4d behaviour) because every real version has
  `seq < u64::MAX`.

## Read path: `get_at` and `iter_at`

The existing `get` is refactored to route through a shared
`get_at_seq(key, read_seq)` internal helper:

```rust
pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
    let read_seq = self.state.lock().unwrap().last_sequence;
    self.get_at_seq(key, read_seq)
}

pub fn get_at(&self, key: &[u8], snapshot: &dyn Snapshot) -> Result<Option<Vec<u8>>> {
    self.get_at_seq(key, snapshot.sequence_number())
}

fn get_at_seq(&self, key: &[u8], read_seq: SequenceNumber) -> Result<Option<Vec<u8>>> {
    let lookup = LookupKey::new(key, read_seq);  // ← uses read_seq
    // 1. memtable.get(&lookup)   — seek pins read_seq
    // 2. immutable.get(&lookup)  — same
    // 3. SSTs: iter.seek(lookup.internal_key()) + parse + check
    // ...
}
```

Because `LookupKey::new(key, read_seq)` bakes the sequence into the
lookup's trailer, every `seek` call automatically skips entries
whose sequence is above `read_seq`. The memtable's `get` uses the
same `LookupKey` so its internal seek pins the same sequence.

The BE-inverted trailer encoding from Layer 4d is what makes this
work transparently: a lookup at `(key, read_seq)` sorts
lexicographically before any real entry at `(key, seq > read_seq)`,
so `seek` lands on the first visible version.

Iterators go through `DbIterator::from_snapshot_with_immutable_at`,
which passes `read_seq` down to the `MemtableUserKeyIter::new_at`
and `SstUserKeyIter::new_at` adapters.

## Snapshot-aware adapters

Both adapters gain a `read_seq` field and a new `read_current`
loop that walks the inner iterator forward, skipping any entry with
`seq > read_seq`, until it lands on a visible entry:

```rust
fn read_current(&mut self) {
    while self.inner.valid() {
        let parsed = match ParsedInternalKey::parse(self.inner.key()) {
            Ok(p) => p,
            Err(_) => { self.valid = false; return; }
        };
        if parsed.sequence > self.read_seq {
            self.inner.next();  // invisible — skip
            continue;
        }
        // First visible version for this user key — surface it.
        self.cur_key.clear();
        self.cur_key.extend_from_slice(parsed.user_key);
        self.cur_value.clear();
        match parsed.value_type {
            ValueType::Value | ValueType::Merge => {
                self.cur_value.extend_from_slice(self.inner.value());
            }
            _ => {}
        }
        self.valid = true;
        return;
    }
    self.valid = false;
}
```

Because internal-key order is `(user_key asc, seq desc, type desc)`,
the loop walks through **newest-first** versions of the current
user key. The first one with `seq <= read_seq` is the newest visible
version — exactly what a snapshot read wants. If every version for
a given user key is above `read_seq`, the loop naturally advances
past them all to the first entry for the next user key.

`seek(target)` now uses `LookupKey::new(target, self.read_seq)`
instead of `READ_AT_LATEST`, so binary search inside the SST index
lands on or before the first visible version.

## Snapshot-aware compaction

The interesting one. `CompactionJob` gains a `min_snap_seq` field
and a builder method:

```rust
impl<'a> CompactionJob<'a> {
    #[must_use]
    pub fn with_min_snap_seq(mut self, min_snap_seq: SequenceNumber) -> Self {
        self.min_snap_seq = min_snap_seq;
        self
    }
}
```

`DbImpl::run_compaction` snapshots `state.min_snap_seq()` under the
lock and passes it to the job:

```rust
let min_snap_seq = {
    let state = self.state.lock().unwrap();
    state.min_snap_seq()
};
let job = CompactionJob::new(...)
    .with_min_snap_seq(min_snap_seq);
```

The retention rule is applied as the sorted vec is walked:

```rust
let mut last_user_key: Option<Vec<u8>> = None;
let mut emitted_below_min = false;
let drop_tombstones = self.min_snap_seq == SequenceNumber::MAX;

for (ikey, value) in &entries {
    let parsed = ParsedInternalKey::parse(ikey)?;
    let new_user_key = last_user_key.as_deref() != Some(parsed.user_key);

    let emit = if new_user_key {
        // Always emit the newest version for a new user key.
        last_user_key = Some(parsed.user_key.to_vec());
        emitted_below_min = parsed.sequence <= self.min_snap_seq;
        true
    } else if parsed.sequence > self.min_snap_seq {
        // Still above the oldest live snapshot — preserve.
        true
    } else if !emitted_below_min {
        // First version at or below min_snap_seq for this user key.
        emitted_below_min = true;
        true
    } else {
        // Older version below min_snap_seq that's already shadowed
        // for every live snapshot. Drop.
        false
    };
    // ... emit / drop, honouring tombstone rules ...
}
```

**Rule in English:**

> For each user key, walk versions newest-first. Always emit the
> newest. Then: emit every subsequent version with
> `seq > min_snap_seq` (some snapshot above might need it). Then
> emit the first version with `seq <= min_snap_seq` (the oldest
> live snapshot needs it). Drop everything older than that.

When there are no live snapshots (`min_snap_seq == MAX`), every
version's seq is below `MAX`, so the rule collapses to "emit only
the first version per user key" — exactly the Layer 4d behaviour.
No regression on the no-snapshot path.

**Tombstone handling:** when `min_snap_seq == MAX` the compaction
is guaranteed bottommost (no live snapshot can see the shadowed
pre-tombstone value) and tombstones are dropped outright. When
there are live snapshots, tombstones are kept if the retention rule
would emit them — a snapshot might need to observe the deletion
rather than the pre-deletion value.

## Verification

| Check | Command | Result |
|---|---|---|
| Clean build | `cargo build` | ✅ 0 warnings, 0 errors |
| Clippy (all targets, `-D warnings`) | `cargo clippy --all-targets -- -D warnings` | ✅ clean |
| Unit tests | `cargo test` | ✅ **261 passed**, 4 ignored doc-tests |
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
| Layer 4d | 1 |
| **Layer 4e (new)** | **6** |
| **Total** | **261** |

### Layer 4e tests

| Test | What it verifies |
|---|---|
| `snapshot_sees_pre_overwrite_value_in_memtable` | Take snapshot, overwrite in memtable, `get_at` returns old value. |
| `snapshot_sees_pre_overwrite_value_after_flush` | Same as above but after a flush — both versions end up in the SST and `get_at` still finds the pre-snapshot one. |
| `snapshot_sees_pre_delete_value` | Take snapshot, delete the key, `get_at` returns the pre-delete value while `get` returns None. |
| `snapshot_survives_compaction` | Take snapshot, force multiple flushes to trigger compaction, confirm the snapshot still sees the pre-snapshot value (snapshot-aware retention held). |
| `dropping_snapshot_releases_retention` | `live_snapshot_count` returns 0 before, 1 after `snapshot()`, 0 again after `drop`. |
| `iter_at_snapshot_excludes_newer_writes` | `iter_at(&snap)` skips post-snapshot puts entirely; overwritten keys return their pre-snapshot value. |

### Headline test

`snapshot_survives_compaction`:

```rust
db.put(b"k", b"old")?;
let snap = db.snapshot();
db.put(b"k", b"new")?;

// Force multiple flushes + a compaction.
db.flush()?;
db.wait_for_pending_flush()?;
for i in 0..4 {
    db.put(format!("filler{i}").as_bytes(), b"v")?;
    db.flush()?;
    db.wait_for_pending_flush()?;
}

// Compaction has almost certainly run (trigger = 4). The
// snapshot must still see "old" because min_snap_seq was
// passed through and compaction respected it.
assert_eq!(db.get(b"k")?, Some(b"new".to_vec()));
assert_eq!(db.get_at(b"k", &*snap)?, Some(b"old".to_vec()));
```

End-to-end exercise of the whole pipeline: snapshot registration →
sequence pinning → flush writing both versions into an SST (Layer
4d) → compaction snapshot-aware retention (Layer 4e) → get_at seek
at the pinned sequence finding the right version.

## Known simplifications (deferred to Layer 4f / 4g)

1. **Long-lived snapshots pin old versions forever.** A snapshot
   held across many compactions prevents all older versions below
   its pinned sequence from being dropped. A production engine
   watches the oldest snapshot's age and warns. We don't.
2. **No multi-snapshot optimisation.** The retention rule uses only
   `min_snap_seq`. If snapshots exist at seqs 100 and 500, we
   preserve every version down to seq 100 rather than the minimum
   set needed for both. Harmless (retains more than necessary) but
   wasteful on write-heavy workloads.
3. **No `refresh()` / `update_snap_seq()`** — upstream has API for
   rewinding a snapshot to a newer sequence. We don't.
4. **Compaction still runs synchronously inside the flush worker**
   — Layer 4c deferred this. Now is a good time to move compaction
   to its own pool slot; that's Layer 4f alongside multi-level.
5. **Iterator is still eager.** A snapshot iterator materialises
   every visible key at construction, even if the user only wants
   a short range scan. A streaming iterator is a Layer 4g goal.
6. **No multi-level layout (L0/L1/…)** — still collapsed into a
   single newest-first SST list. Layer 4f.
7. **Still no real MANIFEST** — `CURRENT` is a flat SST-number list.
   Reopening still scans every SST entry to recover `last_sequence`.
   Layer 4f.
8. **Still no column families, still no full `Db` trait impl.**

## Change log

- **v0.0.10** — Layer 4e landed. 0 new source files; substantial
  updates to `db_impl.rs`, `db_iter.rs`, `compaction.rs`. 261
  passing tests, clippy clean, still zero dependencies. Users can
  now take snapshots, read through them, and trust that compaction
  respects the retention window.
