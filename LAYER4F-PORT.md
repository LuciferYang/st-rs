# Layer 4f Rust Port — Implementation Record

Layer 4f moves compaction off the flush worker and onto its own
background thread. Before this layer, compaction ran **inside**
`complete_flush_locked` on the flush worker — so a flush that
triggered a compaction couldn't return until the whole compaction
had finished, blocking subsequent flushes behind it. Layer 4f makes
flush and compaction run concurrently on separate pool slots.

This is a focused, tight commit: one architectural change, no new
modules, no on-disk format changes, no new public types beyond a
small extension to `DbImpl`'s API surface.

Companion docs:
- `LAYER0-PORT.md` … `LAYER4E-PORT.md` — earlier layers
- `ForSt/FORST-READING-ORDER.md` — the layered reading plan

## What landed

**No new source files.** Updates to `db/db_impl.rs` only. Still
safe Rust, still zero external dependencies.

```
src/db/
├── db_impl.rs       ← UPDATE  CompactionPlan struct; the flush
│                               worker schedules compactions on
│                               Priority::Bottom instead of
│                               running them inline;
│                               wait_for_pending_work drains both
│                               flush and compaction;
│                               pending_compaction field in DbState;
│                               picker skips while a compaction is
│                               already in flight
```

## The change in one picture

**Before (Layer 4e):**

```text
Flush worker thread (Priority::Low):
    ├── write SST
    ├── install in state.ssts
    ├── write CURRENT atomically
    ├── delete obsolete WAL
    └── pick_compaction?
            └── ► RUN COMPACTION INLINE ◄  (blocks this worker)
                    ├── collect entries
                    ├── sort
                    ├── write output SST
                    ├── install / delete inputs
                    └── return
            (next flush waits on this worker)
```

**After (Layer 4f):**

```text
Flush worker thread (Priority::Low):
    ├── write SST
    ├── install in state.ssts
    ├── write CURRENT atomically
    ├── delete obsolete WAL
    ├── pick_compaction?
    │       └── schedule_compaction(plan)  ← puts on pool & returns
    └── return  (ready for the next flush)

Compaction worker thread (Priority::Bottom):
    ├── run_compaction_task(plan)
    │       ├── collect entries from input SSTs
    │       ├── sort
    │       ├── write output SST
    │       └── install / delete inputs
    ├── clear state.pending_compaction
    └── notify flush_done condvar
```

With separate priority slots, `StdThreadPool::new(low=1, ..., bottom=1)`
gives us one dedicated worker per priority. Flush and compaction
never block each other.

## Module updates

### `DbState` — new field

```rust
struct DbState {
    // ...existing fields...

    /// File number of the compaction output currently being
    /// produced by a background worker. `None` when no
    /// compaction is in flight. Layer 4f only allows one
    /// compaction at a time — the picker skips scheduling
    /// while this is `Some`.
    pending_compaction: Option<u64>,
}
```

A single-slot marker is enough at this layer: the picker is still
simple (pick the oldest N SSTs and compact them), and queueing
multiple concurrent compactions would require a more sophisticated
scheduler than we want at Layer 4f.

### `CompactionPlan` — captures everything a worker needs

```rust
struct CompactionPlan {
    out_number: u64,
    inputs: Vec<Arc<BlockBasedTableReader>>,
    input_numbers: Vec<u64>,
    min_snap_seq: SequenceNumber,
}
```

The `min_snap_seq` is captured **at plan creation time** while we
hold the state lock. This fixes a subtle race with Layer 4e: a
snapshot taken between picker and runner would otherwise find its
view retroactively invalidated by a compaction already in flight.
We snapshot the retention boundary once, and any new snapshots
taken after plan creation are no worse off than if they'd been
taken during the compaction's write phase — they still see every
version above the picker-captured `min_snap_seq`, which is the
oldest live snapshot at plan creation, hence ≥ the oldest at *any*
moment during the compaction's life.

### `maybe_pick_compaction` — skip while in flight

```rust
fn maybe_pick_compaction(&self, state: &mut DbState) -> Option<CompactionPlan> {
    if state.pending_compaction.is_some() {
        return None;           // ← new: one at a time
    }
    let indices = pick_compaction(state.ssts.len(), Self::COMPACTION_TRIGGER)?;
    // ... build the plan, including state.min_snap_seq() ...
    Some(CompactionPlan { out_number, inputs, input_numbers, min_snap_seq })
}
```

If a compaction is running and a new flush wants to trigger
another, we skip. The next flush that observes an empty
`pending_compaction` will re-run the picker with the current SST
list and pick up any unfinished work.

### `schedule_compaction` — puts the plan on the pool

```rust
fn schedule_compaction(&self, plan: CompactionPlan) {
    {
        let mut state = self.state.lock().unwrap();
        state.pending_compaction = Some(plan.out_number);
    }
    let weak = self.weak_self();
    self.thread_pool.schedule(
        Box::new(move || {
            if let Some(db) = weak.upgrade() {
                let _ = db.run_compaction_task(plan);
            }
        }),
        Priority::Bottom,
    );
}
```

Same `Weak<DbImpl>` pattern as background flush: the closure
upgrades the Weak to call back into the engine, and if the engine
has been dropped mid-flight, the upgrade fails and the task drops
its work. Half-written output SSTs become orphans on disk and get
picked up by the next `open()` via the directory listing.

### `run_compaction_task` + `run_compaction_inner` — split

The old `run_compaction` becomes two methods:

- **`run_compaction_task(plan)`** runs on the background worker,
  calls `run_compaction_inner(plan)`, clears
  `state.pending_compaction`, and notifies `flush_done` so any
  waiter wakes up.
- **`run_compaction_inner(plan)`** contains the actual merge +
  install logic — shared by the task wrapper and (potentially)
  any future synchronous caller.

### `wait_for_pending_work` — drains both

```rust
pub fn wait_for_pending_work(&self) -> Result<()> {
    let mut state = self.state.lock().unwrap();
    while state.immutable.is_some() || state.pending_compaction.is_some() {
        state = self.flush_done.wait(state).unwrap();
    }
    Ok(())
}

// Backwards compatibility with Layer 4c:
pub fn wait_for_pending_flush(&self) -> Result<()> {
    self.wait_for_pending_work()
}
```

The condvar is reused for both flush and compaction completion.
The wait loop's double condition (`immutable.is_some() ||
pending_compaction.is_some()`) handles the spurious-wakeup case:
if a flush completes first but a compaction is still running,
we see `immutable == None && pending_compaction == Some`, so we
re-wait. When the compaction also completes, the condition
becomes false and we return.

### Thread pool construction

```rust
// Before (Layer 4c):
let thread_pool = Arc::new(StdThreadPool::new(1, 0, 0, 0));  // 1 Low

// After (Layer 4f):
let thread_pool = Arc::new(StdThreadPool::new(1, 0, 0, 1));  // 1 Low + 1 Bottom
```

Separate priority slots → separate workers → no cross-priority
blocking.

## Verification

| Check | Command | Result |
|---|---|---|
| Clean build | `cargo build` | ✅ 0 warnings, 0 errors |
| Clippy (all targets, `-D warnings`) | `cargo clippy --all-targets -- -D warnings` | ✅ clean |
| Unit tests | `cargo test` | ✅ **264 passed**, 4 ignored doc-tests |
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
| Layer 4e | 6 |
| **Layer 4f (new)** | **3** |
| **Total** | **264** |

Every Layer 4a–4e test still passes. The existing
`auto_compaction_collapses_ssts` test (from Layer 4b) now exercises
the background path transparently — it calls `db.flush()` a few
times, the flush worker schedules a compaction instead of running
it inline, and the test's follow-up reads pick up the merged data
after the background worker finishes. The test didn't need any
changes.

### Layer 4f tests

| Test | What it verifies |
|---|---|
| `compaction_runs_on_background_worker` | 4 flushes trigger a compaction; `wait_for_pending_work` drains it; `pending_compaction` is `None` after; every key still readable. |
| `wait_for_pending_work_drains_flush_and_compaction` | `schedule_flush` + `wait_for_pending_work` leaves the engine quiescent (no immutable, no pending compaction). |
| `picker_skips_while_compaction_in_flight` | Force `pending_compaction = Some(9999)`; call `maybe_pick_compaction`; assert it returns `None` even though the SST count exceeds the trigger. |

## Known simplifications (still deferred)

1. **Single-slot compaction queue.** Only one compaction can run
   at a time. If the write rate pushes SSTs to accumulate faster
   than compaction can drain, some compactions are skipped (not
   queued). Eventually the next flush trigger re-evaluates the
   picker on the current state and catches up.
2. **No multi-level layout** (L0/L1/L2…). All SSTs still live in
   a flat newest-first list. Compaction merges "the oldest N" —
   the Layer 4c picker.
3. **No overlap-range picker.** Without multi-level, there's no
   notion of "compact files that overlap this key range." Not a
   regression from earlier layers.
4. **Streaming compaction merge is still O(N) memory.** Every
   input entry is collected into a `Vec` and sorted. A streaming
   k-way merge arrives together with multi-level layout.
5. **No `MANIFEST-NNNNNN`** — `CURRENT` is still a flat file.
6. **No column families, no full `Db` trait impl.**
7. **Iterator is still eager** (Layer 4c decision).

## Change log

- **v0.0.11** — Layer 4f landed. 0 new source files; updates to
  `db_impl.rs` only. 264 passing tests, clippy clean, still zero
  dependencies. Background compaction runs on its own pool slot
  so flushes no longer block behind compaction, and
  `wait_for_pending_work` drains both kinds of background work.
