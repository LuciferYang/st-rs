# Layer 4c Rust Port — Implementation Record

Layer 4c makes the engine usable under load. Where Layer 4b added the
read path (iterators) and the write-amplification fix (compaction),
Layer 4c addresses the **latency** side: writes that trigger a flush
no longer block on the SST write, and concurrent reads can proceed
during a flush.

The architectural change is the classic **immutable memtable** pattern:
the active memtable is moved into a separate `immutable` slot before
the SST write begins, so reads can still find pending data while a
worker is busy producing the SST file.

Companion docs:
- `LAYER0-PORT.md` … `LAYER4B-PORT.md` — earlier layers
- `ForSt/FORST-READING-ORDER.md` — the layered reading plan

## What landed

**1 new source file** (`util/heap.rs`, 255 LOC) plus substantial
refactoring of `db/db_impl.rs` (+408 net LOC), `db/db_iter.rs`
(+25 net LOC), and `db/compaction.rs` (+30 net LOC). Still safe
Rust, still zero external dependencies.

```
src/
├── util/
│   └── heap.rs                       ← NEW   generic min-heap
│                                              with custom comparator
└── db/
    ├── compaction.rs                 ← UPDATE smarter picker:
    │                                          pick the OLDEST N
    │                                          SSTs instead of all
    ├── db_iter.rs                    ← UPDATE add
    │                                          from_snapshot_with_immutable
    └── db_impl.rs                    ← UPDATE immutable memtable,
                                              background flush via
                                              StdThreadPool, atomic
                                              CURRENT writes,
                                              wait_for_pending_flush
```

## Module summaries

### `util/heap.rs` — generic binary min-heap

`std::collections::BinaryHeap<T>` requires `T: Ord`, which means
the comparator has to be encoded in the type. The engine needs to
merge sources by an `Arc<dyn Comparator>` chosen at runtime — that's
the whole point of the `Comparator` trait. So this Layer 1 follow-up
adds a small custom heap that takes the comparator as a closure at
construction:

```rust
pub struct BinaryHeap<T> {
    data: Vec<T>,
    cmp: Box<dyn Fn(&T, &T) -> Ordering + Send + Sync>,
}

impl<T> BinaryHeap<T> {
    pub fn new<F>(cmp: F) -> Self
    where F: Fn(&T, &T) -> Ordering + Send + Sync + 'static;

    pub fn push(&mut self, item: T);          // O(log n)
    pub fn pop(&mut self) -> Option<T>;       // O(log n)
    pub fn peek(&self) -> Option<&T>;         // O(1)
    pub fn replace_root(&mut self, item: T) -> Option<T>; // O(log n)
}
```

Standard array-backed sift-up / sift-down. ~120 LOC of actual code
+ tests covering empty, push/pop ordering, peek, replace_root, the
"all 0xff" cleanup case, custom max-heap via reverse comparator,
and a heap of complex tuples.

This closes one of the audit findings from Layer 4a: `util/heap.h`
was missing.

### `db/compaction.rs` — smarter picker

Layer 4b's picker said "if there are at least `trigger` SSTs,
compact **all** of them into one." This works but does too much
work per compaction — a 1000-SST DB would do a single 1000-input
compaction.

Layer 4c picks the **oldest `batch_size` SSTs**:

```rust
pub fn pick_compaction(num_ssts: usize, trigger: usize) -> Option<Vec<usize>>;
pub fn pick_compaction_batch(num_ssts: usize, trigger: usize, batch_size: usize) -> Option<Vec<usize>>;
```

The default batch is `trigger` (so a `trigger=4` engine compacts
4 SSTs at a time once 4 accumulate). The returned indices are at
the **end** of the newest-first SST list (the oldest entries),
so subsequent point lookups that hit the freshest SST stay fast —
the freshly-flushed SST is never compaction input until enough
older ones accumulate.

Two new tests:
- `pick_compaction_with_more_than_trigger_picks_oldest`
- `pick_compaction_batch_explicit_size`

### `db/db_iter.rs` — `from_snapshot_with_immutable`

The eager `DbIterator` learned how to merge an **immutable
memtable** into the snapshot, between the active memtable and
the SSTs:

```rust
pub fn from_snapshot_with_immutable(
    memtable: &MemTable,
    immutable: Option<&MemTable>,
    ssts: &[Arc<BlockBasedTableReader>],
) -> Result<Self> { ... }
```

Priority order (first wins):
1. Active memtable
2. Immutable memtable (in-progress flush)
3. SSTs in newest-first order

The old `from_snapshot` is now a thin wrapper that passes
`immutable = None`.

### `db/db_impl.rs` — the headline change

This is where the immutable memtable + background flush plumbing
lives. The shape:

#### New fields

```rust
struct DbState {
    memtable: MemTable,
    immutable: Option<Arc<MemTable>>,        // NEW
    ssts: Vec<SstEntry>,
    wal: LogWriter,
    wal_number: u64,
    immutable_wal_number: Option<u64>,       // NEW
    pending_flush_sst_number: Option<u64>,   // NEW
    next_file_number: u64,
    last_sequence: SequenceNumber,
    closing: bool,                            // NEW
}

pub struct DbImpl {
    path: PathBuf,
    fs: Arc<dyn FileSystem>,
    state: Mutex<DbState>,
    flush_done: Condvar,                     // NEW
    lock: Mutex<Option<Box<dyn FileLock>>>,
    user_comparator: Arc<dyn Comparator>,
    write_buffer_size: usize,
    thread_pool: Arc<StdThreadPool>,         // NEW
    weak_self: OnceLock<Weak<Self>>,         // NEW
}
```

The `weak_self` field uses the standard "OnceLock weak self-ref"
pattern: `open()` builds the `Arc<Self>`, immediately downgrades
it into the OnceLock, and from then on background tasks can
upgrade the weak ref to call back into the engine. If the user
drops their last `Arc<DbImpl>` while a task is mid-flight, the
upgrade fails and the task drops its work — the SST file (if any)
is left on disk and gets picked up by the next `open()` via the
directory listing.

#### New flush pipeline

The old monolithic `flush()` is split into three phases:

```rust
pub fn flush(&self) -> Result<()> {
    let setup = self.start_flush_locked()?;
    let setup = match setup { Some(s) => s, None => return Ok(()) };
    let result = self.write_immutable_to_sst(&setup);
    self.complete_flush_locked(setup, result)
}
```

**`start_flush_locked`** (under the lock):
1. Wait on `flush_done` until any in-progress flush completes
2. Reserve a SST file number
3. Move the active memtable into `state.immutable` via
   `mem::replace`, leaving a fresh empty memtable behind
4. Roll the WAL: open a new log file, close the old writer
5. Record the old WAL number so we can delete it later

**`write_immutable_to_sst`** (no lock held):
- Iterate the immutable memtable in internal-key order
- Dedup by user key (newest version wins)
- Stream into a `BlockBasedTableBuilder`
- `finish()` produces the SST file
- Re-open it through `BlockBasedTableReader` and return the reader

This is the slow part. Concurrent puts and gets proceed unblocked
because the lock is released for the duration.

**`complete_flush_locked`** (under the lock):
- Insert the new SST at the front of `state.ssts`
- Clear `state.immutable` and the pending fields
- Persist the new SST list to CURRENT via the atomic write
- Delete the obsolete WAL file
- Maybe schedule a compaction (also under the lock for the
  pick step, then released for the merge I/O)
- Notify `flush_done` so any waiters proceed
- On error: still clear `immutable` (otherwise the next
  `start_flush_locked` would deadlock waiting), preserve the
  old WAL so its records replay on the next `open()`

#### Background variant

```rust
pub fn schedule_flush(&self) -> Result<()> {
    let setup = self.start_flush_locked()?;
    let setup = match setup { Some(s) => s, None => return Ok(()) };
    let weak = self.weak_self();
    self.thread_pool.schedule(Box::new(move || {
        if let Some(db) = weak.upgrade() {
            let result = db.write_immutable_to_sst(&setup);
            let _ = db.complete_flush_locked(setup, result);
        }
    }), Priority::Low);
    Ok(())
}
```

`put()` now calls `schedule_flush` instead of `flush` when the
memtable size trigger fires. The user-visible `flush()` method
stays synchronous so tests and explicit-flush callers get
predictable semantics.

#### `wait_for_pending_flush()`

```rust
pub fn wait_for_pending_flush(&self) -> Result<()> {
    let mut state = self.state.lock().unwrap();
    while state.immutable.is_some() {
        state = self.flush_done.wait(state).unwrap();
    }
    Ok(())
}
```

Used by tests and by `close()` to drain background work before
proceeding.

#### Atomic CURRENT writes

```rust
fn write_current_atomic(&self, contents: &str) -> Result<()> {
    let current_path = make_current_file_name(&self.path);
    let tmp_path = self.path.join("CURRENT.tmp");
    // Write to tmp, fsync, then rename.
    let tmp_file = self.fs.new_writable_file(&tmp_path, &Default::default())?;
    let mut writer = WritableFileWriter::new(tmp_file);
    writer.append(contents.as_bytes(), &io)?;
    writer.flush(&io)?;
    writer.sync(&io)?;
    writer.close(&io)?;
    self.fs.rename_file(&tmp_path, &current_path)?;
    Ok(())
}
```

This replaces Layer 4b's direct write to `CURRENT`, which was
crash-unsafe (a torn write would leave `CURRENT` in a half-written
state). The tmp+rename pattern guarantees that on any crash the
filesystem either has the new `CURRENT` complete or the old one
still in place — never a torn intermediate.

#### `close()` drains the pool

```rust
pub fn close(&self) -> Result<()> {
    self.wait_for_pending_flush()?;        // 1. drain background
    self.flush()?;                          // 2. final memtable
    { /* set state.closing = true */ }
    self.thread_pool.wait_for_jobs_and_join_all();
    // 3. release the lock file
    if let Some(lock) = self.lock.lock().unwrap().take() {
        self.fs.unlock_file(lock)?;
    }
    Ok(())
}
```

`close()` is the right place to finalise — `Drop` is best-effort
and runs without the ability to surface errors.

## Verification

| Check | Command | Result |
|---|---|---|
| Clean build | `cargo build` | ✅ 0 warnings, 0 errors |
| Clippy (all targets, `-D warnings`) | `cargo clippy --all-targets -- -D warnings` | ✅ clean |
| Unit tests | `cargo test` | ✅ **254 passed**, 3 ignored doc-tests |
| No unsafe | `#![deny(unsafe_code)]` crate-wide | ✅ enforced |
| Every public item documented | `#![warn(missing_docs)]` | ✅ enforced |
| Zero external dependencies | empty `[dependencies]` | ✅ still std-only |

### Test count by layer

| Layer | Tests |
|---|--:|
| Layer 0 | 28 |
| Layer 1 | 60 (now includes 8 heap tests) |
| Layer 2 | 42 |
| Layer 3a | 47 |
| Layer 3b | 31 |
| Layer 4a | 14 |
| Layer 4b | 27 |
| **Layer 4c (new)** | **5** |
| **Total** | **254** |

### Layer 4c test breakdown

| Test | What it verifies |
|---|---|
| `util::heap` (8 tests) | Empty heap; push/pop yields sorted; peek; replace_root; replace_root on empty; clear; custom max-heap; comparator over complex type |
| `compaction::pick_compaction_with_more_than_trigger_picks_oldest` | Picker selects the tail (oldest) of the newest-first list |
| `compaction::pick_compaction_batch_explicit_size` | Custom batch size honoured |
| `db_impl::schedule_flush_runs_in_background` | `schedule_flush` returns promptly; data is visible during the background SST write; `wait_for_pending_flush` drains it |
| `db_impl::writes_during_background_flush_go_to_new_memtable` | Concurrent puts during a background flush land in the **new** active memtable; both old and new data are visible afterwards |
| `db_impl::current_file_is_atomic_after_flush` | After a flush, `CURRENT` exists and `CURRENT.tmp` does not (the rename completed cleanly) |

All of Layer 4a's and 4b's tests still pass with the new
plumbing — no behavioural regressions.

## Headline test

**`schedule_flush_runs_in_background`** is the headline:

1. Open a fresh DB
2. Put 50 keys
3. Call `schedule_flush()` — must return promptly
4. Read all 50 keys back via `get()` — they must still be visible
   (either in the immutable memtable if the worker is mid-flush,
   or in the new SST if it's done)
5. Call `wait_for_pending_flush()` to drain the background work
6. Read all 50 keys again — they must still be visible (now
   guaranteed to be in the SST)

This exercises the full new pipeline:

```
put → memtable
schedule_flush → start_flush_locked → mem::replace memtable
                                    → state.immutable = Arc<MemTable>
                                    → roll WAL
                                    → reserve SST number
                                    → release lock
                                    → thread_pool.schedule(closure)
                                  
[concurrent get] → state.lock → check active memtable (empty)
                              → check immutable memtable (HIT!)
                              → return value

[worker thread] write_immutable_to_sst → BlockBasedTableBuilder → SST file
                complete_flush_locked → state.lock
                                     → install SST in state.ssts
                                     → write_current_atomic
                                     → clear state.immutable
                                     → notify flush_done

wait_for_pending_flush → state.lock
                       → state.immutable is None now → return
```

## Known simplifications (deferred to Layer 4d)

1. **No multi-level layout (L0/L1/L2…).** Compaction merges
   sibling SSTs into one without distinguishing levels.
2. **Compaction is still synchronous** (called from inside
   `complete_flush_locked` which is called from a worker, but
   the worker waits for the compaction to finish before
   notifying `flush_done`). Layer 4d will run compaction on its
   own pool slot.
3. **Picker uses simple "oldest N"** without overlap-range
   awareness. Real engines pick the minimum input set whose
   merge would reduce the level count.
4. **No real `MANIFEST-NNNNNN` files** — `CURRENT` is still a
   single line of comma-separated SST numbers, just now written
   atomically. `VersionEdit` records will land in Layer 4d.
5. **No snapshots** — requires internal-key SSTs, deferred.
6. **Single column family** (`default` only).
7. **No full `Db` trait impl** — `DbImpl` is still a concrete
   struct.
8. **`schedule_flush` blocks on `start_flush_locked`** if the
   immutable slot is occupied. A real engine would use a small
   queue of immutable memtables. Single-slot is fine for the
   typical "trigger fires every few seconds" cadence.
9. **Background pool has 1 worker.** Layer 4d will scale this
   based on `DBOptions::max_background_jobs`.
10. **`Drop` is still best-effort** — relies on the user calling
    `close()`. If the user drops without closing, in-flight
    background tasks may try to call back into a freed engine
    via `Weak::upgrade`, fail, and silently drop their work.
    Orphaned SST files are picked up on the next `open()`.

## Change log

- **v0.0.8** — Layer 4c landed. 1 new source file (`util/heap.rs`,
  255 LOC) + substantial refactoring of `db_impl.rs` (+408 LOC),
  `db_iter.rs` (+25 LOC), `compaction.rs` (+30 LOC). 254 passing
  tests, clippy clean, still zero dependencies. The engine now
  has crash-safe atomic CURRENT writes, an immutable memtable
  for concurrent reads during flush, background flush via the
  Layer 2 `StdThreadPool` + a `Weak<Self>` callback pattern, and
  a smarter compaction picker that bounds per-compaction work.
