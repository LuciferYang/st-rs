# Code Review — 2026-05-11

Multi-angle review of the v0.1.0 codebase, run as 5 specialized
review agents in parallel. **All findings, no filtering. Review-only —
no patches proposed inline.** Each item lists severity, file, category,
description, suggested fix (text-only), and effort estimate.

## Scope & methodology

- **F-** Engine internals (`src/db/`, `src/sst/`, `src/memtable/`, `src/cache/`, `src/file/`, `src/env/`) — `rust-reviewer`
- **S-** Security & crash-safety (JNI boundary, file I/O, WAL, MANIFEST) — `security-reviewer`
- **J-** JNI unsafe correctness (`st-rs-jni/`) — `rust-reviewer` (focused)
- **A-** Public API + architecture (`src/api/`, `src/lib.rs`, module layering) — `architect`
- **C-** Recently-landed concurrency work (c1 RwLock, c2 group commit) — `code-reviewer`

Each agent had **unbounded findings, review-only**, no patches.

## Summary by severity

| Source | CRITICAL | HIGH | MEDIUM | LOW | Total |
|---|---:|---:|---:|---:|---:|
| F (engine) | 2 | 11 | 5 | 9 | 27 |
| S (security) | 3 | 9 | 9 | 4 | 25 |
| J (JNI unsafe) | 5 | 5 | 6 | 4 | 20 |
| A (architecture) | 1 | 15 | 36 | 48 | 100 |
| C (concurrency) | 0 | 4 | 4 | 4 | 12 |
| **Grand total** | **11** | **44** | **60** | **69** | **184** |

## Critical findings (must-fix before any user-facing release)

Eleven findings flagged CRITICAL by at least one reviewer. These cluster
into three correctness/safety themes. Several are duplicates spotted by
different reviewers — noted as cross-references.

| # | Title | Theme |
|---|---|---|
| **F-1 / S-6** | WAL omits column-family ID; non-default-CF writes lost or misrouted on crash recovery | WAL crash-safety |
| **F-2** | `VersionSet::apply_edit` ignores `column_family_id` field on replay — CF IDs drift on recovery | MANIFEST recovery |
| **S-1 / J-1** | `from_handle` returns `&'static T` — lifetime lie, enables use-after-free across JNI dispose | JNI handle lifetime |
| **S-2** | `WriteBatch` JNI ops alias `&` and `&mut` simultaneously — UB under concurrent access | JNI aliasing |
| **S-3** | `closeDatabase` + `disposeInternalNative` ordering hazard — background threads can outlive Java handle | JNI lifecycle |
| **J-2** | `drop_handle::<Arc<T>>` Arc-count vs `close()` clones — UAF window if `close()` clones the Arc | JNI handle lifetime |
| **J-3** | `WriteBatch_deleteRange` dereferences null handle without guard (other siblings guard) | JNI null deref |
| **J-4** | All 5 `DBOptions` setters dereference `handle` unconditionally without null check | JNI null deref |
| **J-5** | `deleteFilesInRanges0` declares `JByteArray` but receives `byte[][]` — JNI type-pun | JNI protocol |
| **A-2** | `Db::new_iterator` and `Db::new_iterator_cf` return `EmptyIterator` — trait callers silently see empty DB | Trait contract bug |

**Notes**:
- `F-1` and `S-6` are the same WAL-CF-ID bug seen from engine + security angles. One fix closes both.
- `S-1` and `J-1` are the same `from_handle` `&'static` lifetime lie. One fix closes both.
- `A-2` is duplicated as `F-13` from the engine angle. One fix closes both.

The CRITICAL list is largely about the **JNI boundary** (6/11) and **WAL+MANIFEST recovery** (3/11). Flink's production `disable_wal=true` configuration sidesteps the WAL findings, and the JNI handle-lifetime issues require both bad caller behavior AND specific timing — but a future C ABI consumer or any debug-mode test that enables the WAL on non-default CFs will trip them.

## Table of contents

- [F. Engine internals (27 findings)](#f-engine-internals)
- [S. Security & crash-safety (25 findings)](#s-security--crash-safety)
- [J. JNI unsafe correctness (20 findings)](#j-jni-unsafe-correctness)
- [A. Public API + architecture (100 findings)](#a-public-api--architecture)
- [C. Concurrency (c1 + c2) (12 findings)](#c-concurrency-c1--c2)

---

## F. Engine internals

Baseline: `cargo check`, `cargo clippy -- -D warnings`, and `cargo test` all clean before review.

### F-1. WAL encoding omits column-family ID — crash-recovery data loss for non-default CFs

- **Severity**: CRITICAL
- **File**: src/db/db_impl.rs:3328
- **Category**: correctness
- **Description**: `encode_batch_record` does not write the column-family ID into the WAL record; during replay every entry is replayed into the default CF. The code documents this: "Cross-CF WriteBatch entries will lose their CF affinity after a crash." Any process crash with WAL-enabled writes to non-default CFs will silently corrupt or lose data on reopen.
- **Suggested fix**: Add a varint-encoded CF ID field before the key in each WAL record entry and read it back in `replay_record_with_tombstones`, routing each entry to the correct CF.
- **Effort**: medium

### F-2. `VersionSet::apply_edit` allocates CF IDs from its own counter, ignoring the `VersionEdit`'s CF ID

- **Severity**: CRITICAL
- **File**: src/db/version_set.rs:218
- **Category**: correctness
- **Description**: When an `is_column_family_add` edit is replayed during MANIFEST recovery, `apply_edit` allocates a new CF ID from `self.next_cf_id` rather than using the `column_family_id` field present in the edit. Recovery can re-assign IDs in counter order, drifting from those in the actual file-level edits, causing subsequent file-add/file-delete edits to silently target the wrong CF state.
- **Suggested fix**: Use `edit.column_family_id` as the authoritative CF ID when applying the edit, advancing `next_cf_id` to at least `that_id + 1` to preserve monotonicity.
- **Effort**: small

### F-3. `closing` flag is set but never read — background workers cannot observe shutdown

- **Severity**: HIGH
- **File**: src/db/db_impl.rs:250, 3268
- **Category**: correctness
- **Description**: `DbState::closing` is documented as "Set when the engine is closing — background tasks check this to skip work that no longer matters." In practice `close()` sets it but no code reads it. The thread pool join is the real shutdown barrier. The flag is pure dead state.
- **Suggested fix**: Either remove the field (the pool join is the real shutdown barrier) or wire `closing` checks into `run_compaction_task` / `complete_flush_locked` so late-started background work short-circuits.
- **Effort**: small

### F-4. Filesystem I/O performed while holding `state` read lock in `get_int_property_cf`

- **Severity**: HIGH
- **File**: src/db/db_impl.rs:1064–1094
- **Category**: correctness
- **Description**: The `"total-sst-files-size"`, `"live-sst-files-size"`, and `"estimate-pending-compaction-bytes"` branches call `self.fs.get_file_size(...)` in a loop while holding the `state` read guard. On POSIX `get_file_size` calls `stat(2)` which can block for milliseconds; on JNI-backed Flink FS the latency is worse. This serialises every concurrent write behind a stat call, defeating the RwLock.
- **Suggested fix**: Snapshot the file-number list under the read lock, release the lock, then perform `get_file_size` calls outside any engine lock (mirroring `get_live_files_metadata`).
- **Effort**: small

### F-5. `Vec::retain` with linear `contains` makes L0/L1 removal O(n²) during compaction

- **Severity**: HIGH
- **File**: src/db/db_impl.rs:3175
- **Category**: perf
- **Description**: `run_compaction_inner` removes input SSTs via `retain(|e| !input_numbers.contains(&e.number))`. `input_numbers` is a `Vec<u64>`, so each `contains` is O(m). With `COMPACTION_TRIGGER = 4` this is benign, but if the trigger or batch grows it scales O(L × m).
- **Suggested fix**: Convert `input_numbers` into a `HashSet<u64>` before the two `retain` calls.
- **Effort**: trivial

### F-6. Nested `state.write()` → `state.version_set.lock()` pattern is undocumented and fragile

- **Severity**: HIGH
- **File**: src/db/db_impl.rs:1029, 1114
- **Category**: correctness
- **Description**: `get_int_property_cf` takes `state.read()` at line 1029 then calls through methods that re-enter `state.version_set.lock()` (e.g., `manifest_file_number`). Same nesting in `get_live_files` and `flush_wal`. No centralised documentation of the lock-ordering hierarchy; a future change taking version_set first then state would create a fresh inversion.
- **Suggested fix**: Add a module-level comment documenting the canonical lock order (`state.write/read()` > `state.wal.lock()` > `state.version_set.lock()`); consider extracting a `with_manifest_writer` helper.
- **Effort**: medium

### F-7. `CompactionDecision::RemoveAndSkipUntil` is silently a no-op

- **Severity**: HIGH
- **File**: src/db/compaction.rs:362
- **Category**: correctness
- **Description**: The `RemoveAndSkipUntil` variant is handled by a comment and nothing else — the entry is neither removed nor skipped. A compaction filter that returns this decision (e.g., Flink's TTL filter using range skipping) silently has no effect: the entry is kept rather than removed.
- **Suggested fix**: Implement the skip-until semantic (track `skip_key`, suppress all subsequent entries with user keys less than `skip_key`) or return `Status::not_supported` so callers discover the gap.
- **Effort**: medium

### F-8. `SkipListIter::prev` clones the current key on every backward step

- **Severity**: HIGH
- **File**: src/memtable/skip_list.rs:376
- **Category**: perf
- **Description**: `prev()` calls `self.list.nodes[idx].key.clone()` to obtain the current key, then passes it to `find_less_than`. Every backward iterator step allocates a fresh `Vec<u8>`. In a memtable with 64-byte internal keys and 100k entries, that is 6.4 MB of temporary allocations per full backward scan.
- **Suggested fix**: Restructure `find_less_than` to accept a `&[u8]` borrowed from the existing node, or store keys as `Bytes`/`Arc<[u8]>` to make the clone cheap.
- **Effort**: medium

### F-9. `get_cf_at_seq` clones the entire `range_tombstones` Vec on every read

- **Severity**: HIGH
- **File**: src/db/db_impl.rs:1431
- **Category**: perf
- **Description**: `let range_tombstones = cf.range_tombstones.clone()` under the read lock on every point lookup. With an active delete-range (e.g., Flink TTL), this clones `Vec<(Vec<u8>, Vec<u8>, u64)>` — heap allocation for every key/value pair inside every tombstone — on every get call.
- **Suggested fix**: Snapshot only the needed data (slice triples into a `SmallVec`), or check the tombstones while still under the read lock before the expensive SST I/O begins.
- **Effort**: small

### F-10. `compute_sst_range` performs a full O(n) table scan at open-time and after every flush/compaction

- **Severity**: HIGH
- **File**: src/db/db_impl.rs:208, 2779, 3203
- **Category**: perf
- **Description**: Fallback no-MANIFEST open iterates every entry in every SST to find the smallest/largest user keys. `complete_flush_locked` and `run_compaction_inner` re-scan freshly-written SSTs whose key ranges were already known from the in-memory data being flushed/compacted.
- **Suggested fix**: Track `smallest_key` and `largest_key` incrementally as entries are written into the `BlockBasedTableBuilder`. For the open-time fallback, restrict the scan to first + last entry (seek-to-first + seek-to-last).
- **Effort**: medium

### F-11. `apply_group_commit_batch` flushes non-default CFs synchronously on the leader thread

- **Severity**: HIGH
- **File**: src/db/db_impl.rs:1886
- **Category**: design
- **Description**: For non-default CFs the leader calls `flush_cf()` directly (synchronous SST write) on the write-path thread after `drop(state)` but before filling response slots. Every writer in the batch is blocked behind this synchronous I/O. Default CF uses `schedule_flush()` (background); non-default CFs do not.
- **Suggested fix**: Introduce a `schedule_flush_cf(cf_id)` that routes the non-default CF flush to the background pool. (Duplicate of C-2.)
- **Effort**: medium

### F-12. `PosixFileLock` uses `create_new(true)` — not a real advisory lock; stale locks survive crashes

- **Severity**: HIGH
- **File**: src/env/posix.rs:228, 447
- **Category**: correctness
- **Description**: LOCK file is created via `OpenOptions::create_new(true)` — an atomic create, not `flock`/`fcntl`. If the owning process crashes, the LOCK file is left on disk and all future opens fail with `AlreadyExists`. RocksDB uses `fcntl(F_SETLK)` which the kernel clears on process death. (Duplicate of S-7.)
- **Suggested fix**: Use `rustix::fs::flock` or `fs2`'s `FileExt::lock_exclusive` so the lock is released by the kernel on process death.
- **Effort**: small

### F-13. `Db::new_iterator` returns `EmptyIterator` — trait contract silently unimplemented

- **Severity**: HIGH
- **File**: src/db/db_impl.rs:3658
- **Category**: correctness
- **Description**: The trait `Db::new_iterator` and `Db::new_iterator_cf` implementations always return `EmptyIterator`. Trait-object callers (including the JNI layer if it routes through the trait) get an iterator that reports zero entries even if the database is populated. (Duplicate of A-2.)
- **Suggested fix**: Wire the trait implementations to call `DbImpl::iter()` / `DbImpl::iter_cf()` and return a `Box<dyn api::iterator::DbIterator>` adapter over the concrete `db_iter::DbIterator`.
- **Effort**: medium

### F-14. `has_manifest` detection relies on a magic constant (`next_file_number > 2`)

- **Severity**: MEDIUM
- **File**: src/db/db_impl.rs:552
- **Category**: maintainability
- **Description**: Infers MANIFEST presence by checking whether the file counter is above the initial value of 2. Fragile: any change to the initial value silently changes open-time behaviour.
- **Suggested fix**: Add `fn has_manifest(&self) -> bool` to `VersionSet` based on a dedicated `recovered_from_manifest` flag.
- **Effort**: trivial

### F-15. Nested-mutex lock-order hierarchy is repeated inline, never documented

- **Severity**: MEDIUM
- **File**: src/db/db_impl.rs:1200, 1241, 1388, 2806, 3225
- **Category**: maintainability
- **Description**: `state.version_set.lock()` is taken under `state.write()` (or `state.read()`) at 6+ call sites with no centralised documentation of the ordering, and no lint preventing reverse acquisition in future code. (Duplicate of C-8.)
- **Suggested fix**: Add a module-level doc comment defining the canonical lock order and reference it from every acquisition site.
- **Effort**: trivial

### F-16. `record_buf` allocated fresh inside the WAL-write loop in `apply_group_commit_batch`

- **Severity**: MEDIUM
- **File**: src/db/db_impl.rs:1831
- **Category**: perf
- **Description**: `let mut record_buf = Vec::new()` inside the per-submission loop allocates a new heap buffer each iteration. At high group-commit fan-out one heap allocation per submitter per batch.
- **Suggested fix**: Lift `record_buf` outside the loop and `clear()` between iterations.
- **Effort**: trivial

### F-17. `LruCache::insert` acquires the inner lock twice — TOCTOU on capacity

- **Severity**: MEDIUM
- **File**: src/cache/lru.rs:252
- **Category**: perf
- **Description**: `insert` locks once to read `inner.capacity` (guard immediately dropped), then locks again to mutate. Capacity could change via `set_capacity` between the two; the first check provides no real guarantee.
- **Suggested fix**: Hold a single guard for the entire method, checking `charge > inner.capacity` inline before mutation.
- **Effort**: trivial

### F-18. `close()` does not flush non-default CFs — silent data loss on clean shutdown

- **Severity**: MEDIUM
- **File**: src/db/db_impl.rs:3251
- **Category**: correctness
- **Description**: `close()` only flushes the default CF's active memtable. For WAL-disabled workloads (Flink's primary mode), data in non-default CF memtables is silently dropped on close.
- **Suggested fix**: Call `flush_all_cfs()` inside `close()` before the lock release.
- **Effort**: small

### F-19. L0 SST prepend via `Vec::insert(0, ...)` is O(n) on every flush

- **Severity**: MEDIUM
- **File**: src/db/db_impl.rs:2781, 2427
- **Category**: perf
- **Description**: `cf.l0.insert(0, entry)` shifts every existing element. L0 is bounded today by the compaction trigger but the pattern is O(n) by definition.
- **Suggested fix**: Use `VecDeque<SstEntry>` for L0 (push_front is O(1)), or reverse the convention to append.
- **Effort**: small

### F-20. Duplicate / wrong docstring on `DbImpl::path()`

- **Severity**: LOW
- **File**: src/db/db_impl.rs:969–976
- **Category**: maintainability
- **Description**: The method `path()` has two separate `///` doc-comment blocks; the first describes `manifest_file_number` (the next method) — stale copy-paste.
- **Suggested fix**: Remove the misplaced doc comment from `path()`.
- **Effort**: trivial

### F-21. `default_cf()` and `default_cf_mut()` panic via `.expect`

- **Severity**: LOW
- **File**: src/db/db_impl.rs:303, 308
- **Category**: idiomatic
- **Description**: Both helpers `.expect("default CF missing")` while `cf()` / `cf_mut()` return `Result`. Inconsistent error model; an invariant violation panics rather than returning a recoverable error.
- **Suggested fix**: Return `Result` from `default_cf()` / `default_cf_mut()`, consistent with `cf()` / `cf_mut()`, or add `// INVARIANT:` justification.
- **Effort**: trivial

### F-22. `LruCache` internal `unwrap()` calls are unjustified panics in library code

- **Severity**: LOW
- **File**: src/cache/lru.rs:130, 136, 141, 150, 155, 182, 270, 301
- **Category**: idiomatic
- **Description**: Multiple `.unwrap()` calls inside `Inner::detach` / `push_front` / `evict_to_fit` / `insert` / `lookup`. Valid only while structural invariants hold; any future bug turns into an unrecoverable panic.
- **Suggested fix**: Use `expect("LruCache invariant violated: ...")` with specific messages, or audit and assert the invariants explicitly.
- **Effort**: trivial

### F-23. `SkipList` uses a fixed PRNG seed for all instances

- **Severity**: LOW
- **File**: src/memtable/skip_list.rs:117
- **Category**: design
- **Description**: Every `SkipList` is seeded `Random::new(0xdead_beef)`. Across CFs and DB instances the height sequence is identical for the same insertion order — eliminating random balancing if writes hit a worst-case sequence for this seed.
- **Suggested fix**: Accept optional seed or mix in a per-instance counter (global `AtomicU64`).
- **Effort**: trivial

### F-24. `thread_pool` uses unbounded `mpsc::channel` — no back-pressure

- **Severity**: LOW
- **File**: src/env/thread_pool.rs:161
- **Category**: design
- **Description**: Each priority bucket is an unbounded `mpsc::channel`. A fast write path can enqueue tasks faster than the worker drains, growing queue + `live` set without bound.
- **Suggested fix**: Switch to `mpsc::sync_channel(n)` with a bounded depth or document the L0 stop-writes trigger as the bound.
- **Effort**: small

### F-25. `read_file_to_string` over-allocates for the CURRENT file

- **Severity**: LOW
- **File**: src/db/version_set.rs:393
- **Category**: correctness
- **Description**: Reads CURRENT with a 4096-byte buffer in a loop; CURRENT is always <64 bytes. Cosmetic, not a bug.
- **Suggested fix**: Use `Vec::with_capacity(64)` or `fs.read_to_string(path)` if available.
- **Effort**: trivial

### F-26. `db_impl.rs` is 6786 lines — far over the project's 800-line guideline

- **Severity**: LOW
- **File**: src/db/db_impl.rs (entire file)
- **Category**: maintainability
- **Description**: Single ~6.8k-line file holds WAL encoding, snapshot management, group-commit coordinator, flush pipeline, compaction pipeline, property queries, CF management, SST ingestion. Hard to navigate; slow incremental compilation; merge-conflict risk.
- **Suggested fix**: Extract subsystems: `db/write_path.rs`, `db/flush.rs`, `db/compaction_schedule.rs`, `db/snapshot.rs`, `db/properties.rs`.
- **Effort**: large

### F-27. `maybe_pick_compaction` returns an unnamed 6-tuple from a labeled block

- **Severity**: LOW
- **File**: src/db/db_impl.rs:2844
- **Category**: maintainability
- **Description**: The `'pick` labeled block breaks with `(cf_id, inputs, input_numbers, input_levels, filter_factory, merge_operator)`. Hard to read, fragile to extend.
- **Suggested fix**: Reuse the existing `CompactionPlan` struct or extract a new `PickedCompaction` struct.
- **Effort**: small

---

## S. Security & crash-safety

Files reviewed: `st-rs-jni/src/*.rs`, `src/env/*.rs`, `src/file/*.rs`, `src/db/log_*.rs`, `src/db/version_set.rs`, `src/db/db_impl.rs`.

### S-1. `from_handle` erases the pointed-to lifetime — use-after-free vector

- **Severity**: CRITICAL
- **File**: st-rs-jni/src/lib.rs:54
- **Category**: unsafe-correctness
- **Description**: `from_handle<T>` returns `Option<&'static T>`. The `'static` bound is a lie: the actual lifetime is "until Java calls `disposeInternal`". Any caller that holds the returned reference past dispose has a dangling pointer; Rust's borrow checker doesn't flag this. Present in ~35 call sites. (Duplicate of J-1.)
- **Suggested fix**: Change to `unsafe fn from_handle<T>(handle: jlong) -> Option<*const T>` and immediately clone/upgrade inside each caller. For `Arc<T>` handles, clone the `Arc` before any potential dispose call.
- **Effort**: medium

### S-2. Mutable aliasing of `WriteBatch` via raw pointer while immutable borrow co-exists

- **Severity**: CRITICAL
- **File**: st-rs-jni/src/lib.rs:664 (also 707, 744, 784, 838)
- **Category**: unsafe-correctness
- **Description**: `WriteBatch_put`/`delete`/`merge`/`deleteRange`/`clear` obtain `&mut WriteBatch` via `&mut *(handle as *mut st_rs::WriteBatch)` without checking for aliasing shared borrows. `WriteBatch_count` (line 821) obtains a `&WriteBatch` via `from_handle` simultaneously, yielding `&T` and `&mut T` to the same object — UB.
- **Suggested fix**: Wrap `WriteBatch` in a `Mutex<WriteBatch>` at the JNI boundary, or rely on documented Java-side single-threaded semantics + add null guards consistently.
- **Effort**: medium

### S-3. `closeDatabase` + `disposeInternalNative` ordering hazard

- **Severity**: CRITICAL
- **File**: st-rs-jni/src/lib.rs:117
- **Category**: unsafe-correctness
- **Description**: `closeDatabase` obtains `&Arc<DbImpl>` and calls `db.close()`. Background threads spawned by `close()` hold `Weak<DbImpl>` and may still operate on the WAL and file system after the Java handle is disposed. Refcount prevents double-free, but operations on a "closed" engine continue.
- **Suggested fix**: Either guarantee Java-side calling order (`closeDatabase` before `disposeInternalNative`) by contract, or consolidate both ops into `disposeInternalNative` via a `Drop`-implementing wrapper that calls `close()` from `drop_handle`.
- **Effort**: small

### S-4. `JniWriteStream` silently discards write errors on drop

- **Severity**: HIGH
- **File**: st-rs-jni/src/jni_fs_backend.rs:269
- **Category**: crash-safety
- **Description**: `JniWriteStream::drop` commits accumulated bytes via `let _ = self.backend.call_write_method(...)`. JNI failures (JVM detached, HDFS unreachable, OOM) are silently swallowed — WAL/MANIFEST writes lose data with no error signal.
- **Suggested fix**: Add an explicit `commit()` returning `Result`; `drop()` only logs/poisons if commit was not called.
- **Effort**: small

### S-5. `get_long_array_region` trusts Java to return an array of exactly 3 elements

- **Severity**: HIGH
- **File**: st-rs-jni/src/jni_fs_backend.rs:236
- **Category**: jni-boundary
- **Description**: `call_file_status_method` reads up to 3 longs via `get_long_array_region`. If Java returns fewer, the call succeeds with zero-padded buf — `buf[2]` is silently treated as 0. Latent correctness issue for custom Java FS implementations.
- **Suggested fix**: Verify the Java array length via `env.get_array_length(&long_array)` and error if not 3.
- **Effort**: trivial

### S-6. WAL records omit column-family ID — silent CF misrouting on crash recovery

- **Severity**: HIGH
- **File**: src/db/db_impl.rs:3328
- **Category**: crash-safety
- **Description**: WAL encoder writes no CF ID; replay inserts everything into the default CF. Multi-CF WAL-enabled writes are misrouted on recovery. Same bug as F-1; this is the security angle. Flink production uses `disable_wal=true` and avoids this, but the trap is live for any debug-mode WAL-enabled multi-CF workload.
- **Suggested fix**: Same as F-1. Encode CF ID as a varint per record entry; restore in replay.
- **Effort**: medium

### S-7. `PosixFileLock` does not use `fcntl` advisory locks — stale LOCK after crash

- **Severity**: HIGH
- **File**: src/env/posix.rs:222
- **Category**: crash-safety
- **Description**: LOCK file created with `create_new(true)`. SIGKILL/OOM-kill leaves the file on disk; next open fails permanently. Real RocksDB uses `fcntl(F_SETLK)`. (Duplicate of F-12.)
- **Suggested fix**: Use `fcntl(F_SETLK)` or `flock(LOCK_EX | LOCK_NB)` via `rustix` or `nix`.
- **Effort**: medium

### S-8. Directory `fsync` is never called after SST or WAL file creation/rename

- **Severity**: HIGH
- **File**: src/db/db_impl.rs (flush path), src/db/version_set.rs (CURRENT rename)
- **Category**: crash-safety
- **Description**: `sync_directory` helper exists but is never called. On Linux ext4/xfs with certain mounts, newly-created files and renames may not survive power loss even if file data is fsynced, because the directory entry update can be lost.
- **Suggested fix**: Call `sync_directory(&self.fs, &self.path)` after each new SST file creation and after each `rename_file` to CURRENT.
- **Effort**: small

### S-9. WAL active writer is never explicitly flushed during `DbImpl::close` with an empty memtable

- **Severity**: HIGH
- **File**: src/db/db_impl.rs:3258
- **Category**: crash-safety
- **Description**: `close()` calls `flush()` which rolls the WAL only if there's data; with an empty memtable, the WAL writer's buffered bytes are never explicitly flushed/closed. Writes that occurred after the last auto-flush with `sync=false` may be in the buffer and lost.
- **Suggested fix**: In `close()`, explicitly call `state.wal.lock().sync()` (or `flush + close`) before lock release.
- **Effort**: trivial

### S-10. `attach_current_thread` blocks JVM shutdown — non-daemon threads from native code

- **Severity**: HIGH
- **File**: st-rs-jni/src/jni_fs_backend.rs:64 (multiple call sites)
- **Category**: jni-boundary
- **Description**: `attach_current_thread` registers Rust background threads as JVM non-daemon threads, which prevent JVM exit. Repeated attach/detach per I/O op also adds JNI-internal contention. (Related to J-13.)
- **Suggested fix**: Use `attach_current_thread_as_daemon` (or `attach_current_thread_permanently` for long-lived workers) once at thread start.
- **Effort**: small

### S-11. `RwLock` poisoning will panic across JNI on contended writers

- **Severity**: HIGH
- **File**: src/db/db_impl.rs:829 (all `.read()/.write().unwrap()` sites)
- **Category**: jni-boundary
- **Description**: A writer panicking under `state.write()` poisons the lock. All subsequent `.unwrap()` calls panic — and panicking across a JNI `extern "system"` frame is UB.
- **Suggested fix**: Replace `.unwrap()` with `.unwrap_or_else(|p| p.into_inner())`, and wrap background-task closures in `std::panic::catch_unwind`.
- **Effort**: medium

### S-12. WAL payload silently truncated to 65535 bytes per block fragment (release builds)

- **Severity**: HIGH
- **File**: src/db/log_writer.rs:122
- **Category**: crash-safety
- **Description**: `emit_physical_record` only `debug_assert!` validates `payload.len() <= 0xffff`. In release, larger payloads silently cast `as u16` — wrong length written, CRC mismatch on replay or silent data truncation.
- **Suggested fix**: Replace `debug_assert!` with hard `assert!` or return `Status::io_error` on oversize.
- **Effort**: trivial

### S-13. `encode_batch_record` truncates record count silently from `usize` to `u32`

- **Severity**: HIGH
- **File**: src/db/db_impl.rs:3335
- **Category**: crash-safety
- **Description**: `batch.records().len() as u32` silently truncates if a WriteBatch has more than `u32::MAX` records. Replay reads the truncated count and stops early — undetected data loss.
- **Suggested fix**: Bounds-check before the cast, or widen the wire format to `u64`.
- **Effort**: trivial

### S-14. `JniListCompactionFilter` silently treats JNI errors as "keep" — TTL data not purged

- **Severity**: MEDIUM
- **File**: st-rs-jni/src/jni_list_compaction_filter.rs:104
- **Category**: jni-boundary
- **Description**: `next_unexpired_offset` returns `None` on any JNI error; the filter maps `None` to `Keep`, meaning JVM degradation causes silent unbounded list-state growth. Error is also unlogged.
- **Suggested fix**: Log a warning on JNI-error-induced `None`; consider surfacing a metric.
- **Effort**: trivial

### S-15. Java exception left pending after `call_method` returns Err

- **Severity**: MEDIUM
- **File**: st-rs-jni/src/jni_fs_backend.rs:72 (every `call_method` site)
- **Category**: jni-boundary
- **Description**: JNI spec requires `env.exception_clear()` after a Java exception is observed. Currently the `map_err` propagates the Rust error without clearing the pending Java exception, which can cascade into persistent JNI failures on the same thread.
- **Suggested fix**: After every `call_method` returning `Err`, call `env.exception_clear()` before propagating.
- **Effort**: small

### S-16. Non-default CFs' WAL never deleted — orphaned WAL files grow unboundedly

- **Severity**: MEDIUM
- **File**: src/db/db_impl.rs:2687, 2808
- **Category**: resource-exhaustion
- **Description**: `complete_flush_locked` only deletes the old WAL when `cf_id == DEFAULT_CF_ID`. If the default CF stays empty while non-default CFs flush, their WALs are never deleted.
- **Suggested fix**: Track minimum WAL number still needed across all CFs; delete WALs below the watermark.
- **Effort**: medium

### S-17. `list_status` returns at most one entry — MANIFEST recovery broken for Flink FS

- **Severity**: MEDIUM
- **File**: st-rs-jni/src/jni_fs_backend.rs:315
- **Category**: jni-boundary
- **Description**: Documented stub returns a single entry instead of all directory children. Recovery against a Flink-backed FS would see only one file in a multi-SST directory.
- **Suggested fix**: Implement properly by calling Java `FileSystem.listStatus` and parsing the returned `FileStatus[]`.
- **Effort**: medium

### S-18. `state.read()` held during disk I/O for SST-size property queries

- **Severity**: MEDIUM
- **File**: src/db/db_impl.rs:1064
- **Category**: concurrency-hazard
- **Description**: SST-size property handlers call `self.fs.get_file_size(...)` while holding `state.read()`. Reader-priority RwLock doesn't block reads, but blocks the write guard the group-commit leader needs. Slow FS (HDFS) latency stalls all writes during a metrics poll. (Duplicate of F-4.)
- **Suggested fix**: Snapshot file numbers under read lock, drop lock, then stat.
- **Effort**: small

### S-19. `CURRENT.tmp` is not unique per-process

- **Severity**: MEDIUM
- **File**: src/db/version_set.rs:196
- **Category**: crash-safety
- **Description**: Always-`CURRENT.tmp` temp filename for the atomic CURRENT update. Race or stale tmp can corrupt CURRENT or be silently overwritten during recovery.
- **Suggested fix**: Use `CURRENT.tmp.<pid>.<counter>` for uniqueness.
- **Effort**: trivial

### S-20. `setMaxOpenFiles` / `setDbWriteBufferSize` accept arbitrary values without bounds

- **Severity**: MEDIUM
- **File**: st-rs-jni/src/lib.rs:1007, 1029
- **Category**: resource-exhaustion
- **Description**: JNI setters pass `jlong` straight through to engine options. `i64::MAX` cast to `usize` causes unreasonable allocations / OOM.
- **Suggested fix**: Clamp/validate in each setter; bound `db_write_buffer_size` to a sane maximum.
- **Effort**: trivial

### S-21. `write_after_signal` holds `flush_coord` mutex across 50 ms wait

- **Severity**: MEDIUM
- **File**: src/db/db_impl.rs:2536
- **Category**: concurrency-hazard
- **Description**: While waiter is inside `wait_timeout(50 ms)`, `signal_flush_done` (also wants `flush_coord`) blocks for up to 50 ms. `Condvar::wait_timeout` does release the guard during sleep — but the docs comment "briefly takes the mutex" is misleading.
- **Suggested fix**: Verify `parking_lot::Condvar::wait_timeout` semantics; clarify the comment. If guard isn't released during wait, restructure.
- **Effort**: small

### S-22. Nested-lock order (`state` → `wal` → `version_set`) is undocumented

- **Severity**: MEDIUM
- **File**: src/db/db_impl.rs:240
- **Category**: concurrency-hazard
- **Description**: Both `wal` and `version_set` always acquired under `state.write()`. No code comment documents this; a future change taking them in reverse order would deadlock. (Duplicate of F-15 / C-8.)
- **Suggested fix**: Module-level comment or `// LOCK ORDER:` per acquisition site.
- **Effort**: trivial

### S-23. `ingest_external_file` does not validate source path for traversal

- **Severity**: LOW
- **File**: src/db/db_impl.rs:2362
- **Category**: unsafe-correctness
- **Description**: Caller-provided `&[&Path]` is passed directly to `std::fs::rename` / `std::fs::copy`. `../../sensitive/file.sst` would be accepted. `validate_sst` only checks format, not location.
- **Suggested fix**: Canonicalize and verify source resolves within an expected parent directory if security-sensitive.
- **Effort**: trivial

### S-24. `DbImpl::Drop` releases LOCK but does not flush WAL

- **Severity**: LOW
- **File**: src/db/db_impl.rs:3296
- **Category**: crash-safety
- **Description**: If the `Arc<DbImpl>` is dropped without `close()` (test, forgotten dispose), WAL buffered bytes are silently lost and the LOCK is cleanly removed — looks like a clean shutdown.
- **Suggested fix**: Best-effort WAL sync in `Drop` before releasing the lock.
- **Effort**: small

### S-25. `attach_current_thread` per file op generates JVM thread-local churn

- **Severity**: LOW
- **File**: st-rs-jni/src/jni_fs_backend.rs:64
- **Category**: resource-exhaustion
- **Description**: Each helper independently attaches+detaches. Compaction reads many SST blocks → repeated attach/detach per syscall → JVM-internal lock contention. (Related to S-10 / J-13.)
- **Suggested fix**: Attach once per task, cache the `AttachGuard`.
- **Effort**: medium

---

## J. JNI unsafe correctness

### J-1. `from_handle` returns `&'static T` — lifetime is a lie

- **Severity**: CRITICAL
- **File**: st-rs-jni/src/lib.rs:54
- **Category**: unsafe-correctness
- **Description**: Same as S-1. `from_handle<T>` returns `Option<&'static T>` for a pointer whose actual lifetime is bounded by the Box allocation, which ends at `disposeInternalNative`. Compiler accepts the `'static` reference and gives no protection against UAF.
- **Suggested fix**: Return `Option<*const T>` and force callers to immediately upgrade/clone. Or use a guard type with a finite lifetime.
- **Effort**: medium

### J-2. `drop_handle::<Arc<T>>` Arc-count vs `close()` Arc clones — UAF window

- **Severity**: CRITICAL
- **File**: st-rs-jni/src/lib.rs:62, 107, 141
- **Category**: unsafe-correctness
- **Description**: `Arc<DbImpl>` is boxed → heap holds `Box<Arc<DbImpl>>`. `drop_handle` drops the box (Arc count -1). If `close()` clones the Arc internally (background thread pool, etc.) and the clone outlives the dispose, the engine's invariants depend on the Arc refcount alone — no compiler protection if a `from_handle` reference is held across a dispose.
- **Suggested fix**: Assert refcount in debug; document the invariant; consolidate close + dispose via `Drop` wrapper.
- **Effort**: small

### J-3. `WriteBatch_deleteRange` dereferences null handle without guard

- **Severity**: CRITICAL
- **File**: st-rs-jni/src/lib.rs:784
- **Category**: unsafe-correctness
- **Description**: Sibling `put`/`delete`/`merge`/`clear` all guard `if handle == 0`. `deleteRange` does not — null handle = null pointer deref = UB.
- **Suggested fix**: Add the same null guard.
- **Effort**: trivial

### J-4. `DBOptions` setters all dereference handle unconditionally

- **Severity**: CRITICAL
- **File**: st-rs-jni/src/lib.rs:1001, 1013, 1024, 1035, 1046
- **Category**: unsafe-correctness
- **Description**: Five `DBOptions_set*` JNI functions cast handle directly to `&mut *` without null check. WriteOptions setters at least check first. Null handle → null deref.
- **Suggested fix**: Add `if handle == 0 { return; }` to each setter.
- **Effort**: trivial

### J-5. `deleteFilesInRanges0` parameter type-puns `JByteArray` as `byte[][]`

- **Severity**: CRITICAL
- **File**: st-rs-jni/src/lib.rs:1586
- **Category**: unsafe-correctness / jni-protocol
- **Description**: Declares `ranges: JByteArray` but the Java call passes `[[B`, then converts via `JObjectArray::from_raw`. Relies on JVM-internal array-of-arrays representation being pointer-compatible with `byte[]` — not guaranteed by the JNI spec.
- **Suggested fix**: Change the Rust parameter type to `JObjectArray`.
- **Effort**: trivial

### J-6. Missing `// SAFETY:` comments on every `unsafe` block

- **Severity**: HIGH
- **File**: st-rs-jni/src/lib.rs (pervasive), st-rs-jni/src/jni_fs_backend.rs:44, st-rs-jni/src/jni_list_compaction_filter.rs:50, 134
- **Category**: unsafe-correctness / idiomatic
- **Description**: 50+ `unsafe` blocks, only 2 have safety comments. `unsafe impl Send` / `Sync` blocks have module-level prose but no per-impl `// SAFETY:`.
- **Suggested fix**: Add `// SAFETY:` per block stating the exact invariant relied on.
- **Effort**: medium

### J-7. `WriteOptions` setters alias `&` and `&mut` on the same handle

- **Severity**: HIGH
- **File**: st-rs-jni/src/lib.rs:928, 942
- **Category**: unsafe-correctness
- **Description**: `setDisableWAL` / `setSync` first take `&'static RustWriteOptions` via `from_handle`, then cast same pointer to `&mut RustWriteOptions`. Co-existing `&` and `&mut` to same memory = UB regardless of practical single-threadedness.
- **Suggested fix**: Use a direct `if handle == 0 { return; }` guard and cast straight to `&mut *`. Skip the `from_handle` round-trip.
- **Effort**: trivial

### J-8. `panic = "abort"` is release-only — debug builds unwind across JNI

- **Severity**: HIGH
- **File**: Cargo.toml:61
- **Category**: panic-safety
- **Description**: `[profile.release]` sets `panic = "abort"`; `[profile.dev]` does not. Any panic in a JNI `extern "system"` function in a debug build unwinds across the C ABI boundary — UB.
- **Suggested fix**: Set `panic = "abort"` on `st-rs-jni`'s dev profile, or document that JNI must not be exercised under non-release builds.
- **Effort**: small

### J-9. `throw_rocks_exception` silently drops its own error; doesn't check pending exceptions

- **Severity**: HIGH
- **File**: st-rs-jni/src/lib.rs:69
- **Category**: jni-protocol
- **Description**: `let _ = env.throw_new(...)` discards result. If the exception class isn't found, no Java exception is set and the caller's branch returns leaving the JVM in an inconsistent state. Also, no `exception_check` at entry to JNI functions — earlier JNI calls may have left a pending exception.
- **Suggested fix**: Check throw_new result; check `env.exception_check()` at entry.
- **Effort**: medium

### J-10. `JniWriteStream::drop` silently swallows write errors

- **Severity**: HIGH
- **File**: st-rs-jni/src/jni_fs_backend.rs:269
- **Category**: jni-protocol / ffi-leak
- **Description**: Same as S-4. Drop calls `let _ = self.backend.call_write_method(...)`. WAL/SST data loss is silent.
- **Suggested fix**: Explicit commit method returning `Result`; drop only logs/poisons.
- **Effort**: medium

### J-11. `JniFsBackend::create` allocates a fresh `Arc<JniFsBackend>` instead of cloning Arc

- **Severity**: MEDIUM
- **File**: st-rs-jni/src/jni_fs_backend.rs:299
- **Category**: ffi-leak / performance
- **Description**: `create` does `Arc::new(JniFsBackend { jvm: Arc::clone(...), fs_ref: self.fs_ref.clone() })` instead of `Arc::clone(self)`. Wastes JNI allocation + Rust Arc allocation per call.
- **Suggested fix**: Change trait to `self: &Arc<Self>` and clone the Arc.
- **Effort**: small

### J-12. `list_status` swallows errors, returns empty Vec

- **Severity**: MEDIUM
- **File**: st-rs-jni/src/jni_fs_backend.rs:315
- **Category**: jni-protocol
- **Description**: Returns `Ok(Vec::new())` on any error. Engine can mistake "FS broken" for "directory empty" → data-loss decisions.
- **Suggested fix**: Propagate the error, or `unimplemented!()` until properly implemented.
- **Effort**: small

### J-13. `attach_current_thread` (non-daemon) on background threads

- **Severity**: MEDIUM
- **File**: st-rs-jni/src/jni_fs_backend.rs:64 (multiple), st-rs-jni/src/jni_list_compaction_filter.rs:74, 189
- **Category**: jni-protocol
- **Description**: Non-daemon attachment prevents JVM shutdown. Same as S-10.
- **Suggested fix**: Use `attach_current_thread_as_daemon`.
- **Effort**: trivial

### J-14. `(batch.len() * 2) as i32` and similar casts can silently overflow

- **Severity**: MEDIUM
- **File**: st-rs-jni/src/lib.rs:1387, 1412, 1416, 1460, 1462, 1464, 1698, 1701, 1706, 1708, 1981
- **Category**: unsafe-correctness
- **Description**: `usize → i32` or `usize → u32` casts in JNI return paths. Overflow → wrapping → negative array length passed to JVM → undefined.
- **Suggested fix**: `try_into::<i32>()` with throw-on-error, or debug assertions.
- **Effort**: small

### J-15. `from_handle::<T>` has no runtime type tag — wrong-type confusion = UB

- **Severity**: MEDIUM
- **File**: st-rs-jni/src/lib.rs:54
- **Category**: unsafe-correctness
- **Description**: Any `jlong` reinterpreted as `*const T`. If Java passes a WriteBatch handle where DbImpl is expected, immediate type-confusion UB.
- **Suggested fix**: Tag handles with a `u32` discriminant in a newtype wrapper; assert on retrieval.
- **Effort**: medium

### J-16. `getSnapshot` / `releaseSnapshot` lack use-after-release protection

- **Severity**: MEDIUM
- **File**: st-rs-jni/src/lib.rs:1138
- **Category**: unsafe-correctness
- **Description**: Same root cause as J-1 / J-15. If Java uses a released snapshot handle, dereferences freed memory.
- **Suggested fix**: Tagged handles (J-15); zero Java-side `nativeHandle_` after release.
- **Effort**: medium

### J-17. `newIteratorCf` delegates by calling another `extern "system"` fn directly

- **Severity**: LOW
- **File**: st-rs-jni/src/lib.rs:1206
- **Category**: idiomatic
- **Description**: Tightly couples by mangled JNI name; surprising `mut env` shadowing.
- **Suggested fix**: Extract a private helper.
- **Effort**: trivial

### J-18. Redundant `from_handle` round-trip in `WriteOptions` setters

- **Severity**: LOW
- **File**: st-rs-jni/src/lib.rs:928, 942
- **Category**: idiomatic
- **Description**: Same as J-7. The `from_handle` is only useful as a null check; replace with direct null guard.
- **Suggested fix**: Direct null guard, no `from_handle`.
- **Effort**: trivial

### J-19. No test coverage in the JNI crate itself

- **Severity**: LOW
- **File**: st-rs-jni/src (entire crate)
- **Category**: idiomatic
- **Description**: No `#[test]` blocks. Pure-Rust helpers (`to_handle`/`from_handle`/`drop_handle` round-trip, `nextBatchPacked0` buffer layout, null-handle guards) are testable without a JVM.
- **Suggested fix**: Add a `#[cfg(test)]` module.
- **Effort**: small

### J-20. `unsafe impl Send` / `Sync` for backends lack per-impl safety justification

- **Severity**: LOW
- **File**: st-rs-jni/src/jni_fs_backend.rs:44, st-rs-jni/src/jni_list_compaction_filter.rs:50, 134
- **Category**: unsafe-correctness / idiomatic
- **Description**: Module comment says "JavaVM and GlobalRef are Send+Sync" but `GlobalRef`'s `Sync` is debatable — safe only because JNI serializes JVM-side, not Rust-side.
- **Suggested fix**: Per-impl `// SAFETY:` comment explaining the JNI attach + lack of Rust-side shared mutable state.
- **Effort**: trivial

---

## C. Concurrency (c1 + c2)

### Canonical lock order observed across all code paths

1. `write_coord.inner` (parking_lot::Mutex) — brief, enqueue/drain
2. `state` (std::RwLock) — write or read guard
3. `flush_coord` (std::Mutex) — only inside `write_after_signal`/`signal_flush_done`, always *after* `state` is dropped
4. `state.wal` (std::Mutex) — inner, always under outer `state.write()`
5. `state.version_set` (std::Mutex) — inner, always under outer `state.write/read()`
6. `ResponseSlot.inner` (parking_lot::Mutex) — per-call, independent

### C-1. WAL partial-write on group-commit error leaves `next_seq` gap

- **Severity**: HIGH
- **File**: src/db/db_impl.rs:1833
- **Category**: invariant
- **Description**: When `wal.add_record` fails at submission `k`, all submissions in the batch are failed via `response.fill(Err(...))`. But `next_seq` advanced past submissions `0..k` that *were* durably written. Replay sees the WAL records but engine `last_sequence` after reopen is lower → replayed records considered "from the future."
- **Suggested fix**: Either advance `last_sequence` through the last successfully-written submission before failing the remainder, or treat any WAL error as fatal for the WAL file and roll a new one.
- **Effort**: medium

### C-2. Non-default CF flush is synchronous on the leader thread

- **Severity**: HIGH
- **File**: src/db/db_impl.rs:1891
- **Category**: starvation
- **Description**: Leader calls `flush_cf()` synchronously after step 2; all followers wait through the SST write. Default CF correctly uses background `schedule_flush()`. (Duplicate of F-11.)
- **Suggested fix**: Add `schedule_flush_cf` that routes to the background pool; notify followers after memtable insert, not after SST write.
- **Effort**: medium

### C-5. Panic in `apply_group_commit_batch` after `drop(state)` leaves followers waiting forever

- **Severity**: HIGH
- **File**: src/db/db_impl.rs:1879
- **Category**: panic-safety
- **Description**: If a panic occurs in steps 3–5 (after dropping `state`), no response slot is filled. `parking_lot::Mutex` doesn't poison and the condvar has no timeout → followers block forever. `panic = "abort"` saves release but dev/test builds can hang.
- **Suggested fix**: RAII guard that fills unfilled response slots with an error on drop.
- **Effort**: small

### C-10. Group-commit leader can block forever on L0 stall — no caller escape

- **Severity**: HIGH
- **File**: src/db/db_impl.rs:1801
- **Category**: starvation
- **Description**: At `level0_stop_writes_trigger`, `write_after_signal` loops indefinitely until compaction reduces L0. All followers waiting on `response.take()` have no timeout either. Stuck compaction = entire write path halts permanently.
- **Suggested fix**: Configurable `write_stall_timeout`; return `Status::Busy` if exceeded.
- **Effort**: medium

### C-3. `snapshot()` takes write guard unnecessarily, blocking all reads

- **Severity**: MEDIUM
- **File**: src/db/db_impl.rs:2171
- **Category**: starvation
- **Description**: `snapshot()` takes `state.write()` only to insert into `state.snapshots`. Pure metadata mutation, no ordering dependency with concurrent reads. Every snapshot creation briefly stalls all `get()`/`iter()`.
- **Suggested fix**: Move `snapshots` to its own `Mutex<BTreeMap<...>>` independent of the state RwLock; expose `last_sequence` as `AtomicU64`.
- **Effort**: medium

### C-4. `flush_wal` takes inner `state.wal.lock()` under a read guard — priority inversion

- **Severity**: MEDIUM
- **File**: src/db/db_impl.rs:3712
- **Category**: lock-order
- **Description**: `flush_wal` holds `state.read()` then `state.wal.lock()`. Group-commit leader holds `state.write()` then `state.wal.lock()`. Both consistent (inner after outer), but `flush_wal` reader blocks the leader on inner `wal` while leader's `state.write()` request is queued behind readers.
- **Suggested fix**: Route `flush_wal` through `write_coord`, or document the non-concurrent expectation.
- **Effort**: small

### C-7. `ingest_external_file` drops + re-acquires `state.write()` mid-file — TOCTOU + non-atomic across paths

- **Severity**: MEDIUM
- **File**: src/db/db_impl.rs:2368
- **Category**: race-condition
- **Description**: Per-path: reserve `next_file_number`, drop guard, copy/scan file, re-acquire guard, install entry. Between guard acquisitions, concurrent ops can advance counter or delete files. Across multiple `paths`, failure mid-loop leaves earlier paths committed.
- **Suggested fix**: Reserve all file numbers in a single lock acquisition at the start; document the non-atomic-across-paths semantic.
- **Effort**: small

### C-12. WAL-success + memtable-failure on partial batch leaves visible-after-recovery anomaly

- **Severity**: MEDIUM
- **File**: src/db/db_impl.rs:1827
- **Category**: invariant
- **Description**: After a WAL error at submission `k`, submissions `0..k` had `disable_wal=false` and were physically written to the WAL. All return error to caller. On crash + replay, those records *do* appear in the database despite the error return — anomaly between caller's view and recovered state. (Related to C-1.)
- **Suggested fix**: Commit `0..k` to the memtable + advance `last_sequence` before returning error only to the truly unapplied submissions.
- **Effort**: medium

### C-6. `last_sequence` read without write guard in `get()` / `multi_get()`

- **Severity**: LOW
- **File**: src/db/db_impl.rs:1907, 2008
- **Category**: invariant
- **Description**: Read under `state.read()`, dropped, then used. Correct today (read guard provides memory ordering) but fragile — a future change to `AtomicU64` or unguarded read would introduce stale-read window. No comment.
- **Suggested fix**: Inline comment noting the read-guard is load-bearing for `last_sequence` consistency.
- **Effort**: trivial

### C-8. `write_coord.inner` vs `state` ordering invariant is undocumented

- **Severity**: LOW
- **File**: src/db/db_impl.rs:1774, 2543
- **Category**: lock-order
- **Description**: `write_coord.inner` is always released before `write_after_signal` runs. Future refactor holding both = deadlock vector. Convention is undocumented.
- **Suggested fix**: `// INVARIANT:` comment at `WriteCoord` definition.
- **Effort**: trivial

### C-9. `signal_flush_done` ordering rule is correct but undocumented

- **Severity**: LOW
- **File**: src/db/db_impl.rs:2560
- **Category**: invariant
- **Description**: Must be called *after* releasing `state.write()` (otherwise waiter deadlocks on `state.write()` inside `write_after_signal`). Currently always done correctly; no comment enforcing.
- **Suggested fix**: Comment at `run_compaction_task` and `complete_flush_locked`.
- **Effort**: trivial

### C-11. `release_snapshot_internal` takes write guard — same starvation as C-3

- **Severity**: LOW
- **File**: src/db/db_impl.rs:2231
- **Category**: starvation
- **Description**: Snapshot drop blocks all concurrent reads. Workloads with many short-lived snapshots (common in iterator patterns) see hidden write-lock contention.
- **Suggested fix**: Subsumed by C-3 fix (move `snapshots` to its own mutex).
- **Effort**: (same change as C-3)

---

## A. Public API + architecture

100 findings on API surface, naming, module organization, ABI hazards.
The full content is below — large but coherent as one section.

Now I have a comprehensive view of the system. Let me compile the findings.

# Architecture Review: st-rs Public API Surface and Module Organization

### A-1. `Db` trait and `DbImpl` define two parallel, divergent APIs

- **Severity**: HIGH
- **File or area**: cross-cutting — /Users/yangjie01/SourceCode/git/st-rs/src/api/db.rs:81, /Users/yangjie01/SourceCode/git/st-rs/src/db/db_impl.rs:458
- **Category**: api-design
- **Description**: `DbImpl` exposes a much richer inherent API than the `Db` trait surface (e.g. `multi_put`, `multi_delete`, `delete_range`, `delete_range_cf`, `delete_files_in_ranges`, `iter`, `iter_cf`, `iter_at`, `prefix_scan`, `prefix_scan_cf`, `get_at`, `ingest_external_file`, `flush_all_cfs`, `schedule_flush`, `wait_for_pending_flush`, `wait_for_pending_work`, `set_merge_operator`, `set_compaction_filter_factory`, `create_column_family_with_import`, `get_int_property`, `get_property_cf`, `get_int_property_cf`, `get_live_files`, `get_live_files_metadata`, `disable_file_deletions`, `enable_file_deletions`, `snapshot_live_files`, `manifest_file_number`, `is_write_stopped`, `file_system`, `db_path`, `path`, `sequence`). Users hitting "I want a trait object" hit a degraded surface; users discovering the inherent API are locked into the concrete type. This is exactly the leaky-abstraction trap the Stage-2 C ABI will inherit and amplify.
- **Suggested fix**: Either (a) make `Db` the canonical surface and demote inherent methods on `DbImpl` to a smaller private/internal set, or (b) explicitly mark the inherent API as "advanced/engine-specific" with a doc convention, and lift the genuinely portable ones (range delete, iterator, ingest) into the trait. Decide upfront — the C ABI will lock the choice.
- **Effort**: large

### A-2. `Db::new_iterator` returns an `EmptyIterator` placeholder instead of a real iterator

- **Severity**: CRITICAL
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/db/db_impl.rs:3658-3675
- **Category**: api-design
- **Category**: api-design
- **Description**: The trait's `new_iterator` / `new_iterator_cf` implementations silently return `EmptyIterator` regardless of DB state because `db::db_iter::DbIterator` (the concrete type) does not implement `api::iterator::DbIterator` (the trait). Any caller using the trait surface gets silently wrong (empty) results. This is a correctness bug masquerading as API design.
- **Suggested fix**: Either implement the trait for the concrete iterator, return a `Box<dyn DbIterator>` adapter, or remove the iterator methods from the trait until they can be wired up correctly. Returning a silently-empty iterator is worse than `NotSupported`.
- **Effort**: medium

### A-3. Trait-level `Db::compact_range` is a silent no-op

- **Severity**: HIGH
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/db/db_impl.rs:3698-3705
- **Category**: api-design
- **Description**: The `compact_range` trait method returns `Ok(())` without doing anything ("Manual compaction not yet implemented. No-op."). Returning success for an unsupported operation violates the principle of least surprise and will hide bugs at the JNI/C-ABI boundary. Callers cannot distinguish "compacted" from "ignored".
- **Suggested fix**: Return `Status::not_supported(...)` until manual compaction is implemented, and document the gap; or remove the method from the trait until it is real.
- **Effort**: trivial

### A-4. `Db::single_delete` silently downgrades to `delete`

- **Severity**: HIGH
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/db/db_impl.rs:3593-3600
- **Category**: api-design
- **Description**: The trait `single_delete` delegates to `DbImpl::delete`. `SingleDelete` has subtly different semantics from `Delete` (it asserts the key was written exactly once and can be cheaper to compact). A silent downgrade is a correctness footgun for callers relying on the difference, especially since `WriteBatch::single_delete` does properly produce a `Record::SingleDelete`.
- **Suggested fix**: Wire `single_delete` through to the actual `ValueType::SingleDeletion` path (via a `WriteBatch::single_delete` round-trip is fine), or return `Status::not_supported`. Do not silently change semantics.
- **Effort**: small

### A-5. `Db::release_snapshot` is a no-op that contradicts its own contract

- **Severity**: MEDIUM
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/api/db.rs:178, /Users/yangjie01/SourceCode/git/st-rs/src/db/db_impl.rs:3681-3684
- **Category**: api-design
- **Description**: The `Db::snapshot()` doc says "failing to release it pins sequence numbers and prevents compaction from dropping older versions," but `release_snapshot` is documented as a no-op because release happens on `Drop`. This is internally inconsistent: dropping the `Arc<dyn Snapshot>` works only because `DbImpl` implements `Drop` on `DbSnapshot`, but a future implementor of `Db` would have no idea that the trait demands `Drop`-based lifetime semantics rather than an explicit `release_snapshot`. The trait contract is silently engine-specific.
- **Suggested fix**: Pick one model. Either (a) require all implementations to release on `Drop` and remove `release_snapshot` from the trait, or (b) require explicit release and have `DbImpl` honour it. Document the chosen model on the trait.
- **Effort**: small

### A-6. `ReadOptions::snapshot` is a raw `u64` while `Snapshot` is a trait — they share no link

- **Severity**: HIGH
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/api/options.rs:296-298, /Users/yangjie01/SourceCode/git/st-rs/src/api/snapshot.rs:33
- **Category**: api-design
- **Description**: `ReadOptions::snapshot: Option<u64>` is a free-form sequence number, but the canonical way to obtain one is `Db::snapshot() -> Arc<dyn Snapshot>` which exposes `sequence_number(&self) -> SequenceNumber`. The user has to manually pluck the seq out of the `Snapshot` and stuff it into `ReadOptions`. This bypasses the type system entirely — a stale or fabricated seq number is accepted at face value and may produce wrong reads silently. The trait link between "I have a Snapshot" and "I want to read at that Snapshot" is broken.
- **Suggested fix**: Change `ReadOptions::snapshot` to `Option<Arc<dyn Snapshot>>` (or `&'a dyn Snapshot` with a lifetime) so the type system enforces the linkage and the engine retains the refcount for as long as the read planner needs it. Upstream RocksDB uses a `const Snapshot*`.
- **Effort**: medium

### A-7. `read_tier_memtable_only` naming diverges from upstream `ReadTier` enum

- **Severity**: MEDIUM
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/api/options.rs:317-318
- **Category**: naming
- **Description**: Upstream uses an enum `ReadTier { kReadAllTier, kBlockCacheTier, kPersistedTier, kMemtableTier }`; the port collapses this to a single `bool read_tier_memtable_only`. This loses three of four variants and forecloses on `BlockCacheTier`/`PersistedTier` without a breaking change. Future C-ABI mirrors will have to either fork a new enum or invent ad-hoc bools.
- **Suggested fix**: Promote `read_tier_memtable_only` to a `ReadTier` enum mirroring upstream, even if only `All` and `MemtableOnly` are implemented today. Cheap to add now, expensive to migrate later.
- **Effort**: trivial

### A-8. `Options` only re-exposes a single CF — multi-CF `Options` shape is missing

- **Severity**: MEDIUM
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/api/options.rs:269-275
- **Category**: api-design
- **Description**: `Options { db: DbOptions, cf: ColumnFamilyOptions }` only covers the single-default-CF case. Multi-CF DBs must pass `ColumnFamilyDescriptor`s separately to `DbOpener::open_cf`. This duplicates configuration intent (DB options also live separately) and creates an irregular API: "open with one CF uses `Options`, open with many uses `DbOptions + Vec<ColumnFamilyDescriptor>`."
- **Suggested fix**: Either remove `Options` (force all callers through `DbOptions + Vec<ColumnFamilyDescriptor>` for symmetry), or extend `Options` to carry a `Vec<ColumnFamilyDescriptor>` so it can serve as a single config object for any DB shape.
- **Effort**: small

### A-9. `DbOpener` is declared but never implemented

- **Severity**: MEDIUM
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/api/db.rs:209-232
- **Category**: api-design
- **Description**: `DbOpener` is documented as "engines implement a factory function of this shape" and is re-exported at the crate root, but `DbImpl::open`/`open_with_fs` is a plain inherent `fn` and there is no `impl DbOpener for DbImpl`. The trait is unenforced — a second engine could add `Foo::open(...)` with a totally different signature and nobody would notice. For a project that wants to support multiple backends, an unused factory trait is worse than no trait.
- **Suggested fix**: Either implement `DbOpener for DbImpl` (and replace `DbImpl::open` calls with the trait method in examples) or delete `DbOpener` and document the convention in prose. Don't ship a trait that nobody implements.
- **Effort**: small

### A-10. `OpenCfResult` newtype is exported but `DbOpener::open` returns the bare `Self::Db`

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/api/db.rs:39, /Users/yangjie01/SourceCode/git/st-rs/src/api/db.rs:214
- **Category**: api-design
- **Description**: `open()` returns `Self::Db` (no CF handles), `open_cf()` returns `OpenCfResult<Self::Db>` (a tuple). The two factory methods produce different shapes for what is conceptually "an open DB plus a default CF handle". A user who opens with just `open()` has no CF handle and must call `default_column_family()` separately.
- **Suggested fix**: Make `open()` also return `(Db, default_cf_handle)` for symmetry, or always return a `(Db, Vec<Handle>)` where the vec is `[default]` in the no-CF case. Pick one.
- **Effort**: trivial

### A-11. Asymmetric `*_cf` API: some methods exist only as `_cf` variants, some only as defaults

- **Severity**: MEDIUM
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/api/db.rs:99-156
- **Category**: api-design
- **Description**: The trait has `put`/`put_cf`, `delete`/`delete_cf`, `get`/`get_cf`, `flush`/`flush_cf`, `new_iterator`/`new_iterator_cf` — but `single_delete`, `merge`, `multi_get`, `compact_range`, and `snapshot` lack `_cf` variants. Most are CF-relevant in upstream RocksDB (`SingleDelete(CF)`, `Merge(CF)`, `MultiGet(vec<CF>)`, `CompactRange(CF)`). The set of "which methods are CF-aware" is unpredictable, which makes the API hard to learn and hard to mirror in C.
- **Suggested fix**: Make CF-awareness uniform. Every mutation/read should have a `_cf` variant, and the no-CF version should be a thin wrapper that calls `_cf(default_column_family(), …)`.
- **Effort**: medium

### A-12. `multi_get` returns `Vec<Result<Option<Vec<u8>>>>` — unergonomic and inefficient

- **Severity**: MEDIUM
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/api/db.rs:151-156, /Users/yangjie01/SourceCode/git/st-rs/src/db/db_impl.rs:3644-3656
- **Category**: api-design
- **Description**: Returning per-key `Result` mixes I/O errors with per-key not-found, forcing callers to walk the vec. Upstream returns parallel arrays `(statuses, values)`, and most consumers want either "all-or-nothing" or batched-keys with shared error path. Furthermore, the implementation in the trait impl iterates per-key and calls single `get_at_seq` for each, which is the same N round-trips as a loop of `get` — no batching benefit. The function name implies batched I/O but doesn't deliver.
- **Suggested fix**: Decide whether `multi_get` is a real batched operation (in which case parallelize/batch the underlying lookups and return `Result<Vec<Option<Vec<u8>>>>`) or a convenience helper. Pick a return shape that reflects reality.
- **Effort**: medium

### A-13. `WriteOptions` is largely ignored at the trait impl

- **Severity**: MEDIUM
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/db/db_impl.rs:3557-3611
- **Category**: api-design
- **Description**: The `Db` trait impl on `DbImpl` accepts `_opts: &WriteOptions` on `put`, `put_cf`, `delete`, `delete_cf`, `single_delete`, `merge` but doesn't propagate them — it calls the inherent `DbImpl::put` (which uses `WriteOptions::default()` internally). Only `write` honours `WriteOptions` (via `write_opt`). Setting `WriteOptions::sync = true` through the trait `Db::put` silently does nothing.
- **Suggested fix**: Either propagate `WriteOptions` through every write path (`DbImpl::put_opt` exists; use it for `put` too — and add `put_cf_opt`, `delete_opt`, `delete_cf_opt`, etc.), or remove `WriteOptions` from methods that ignore it.
- **Effort**: small

### A-14. `WriteOptions::low_pri` and `no_slowdown` are accepted but unused

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/api/options.rs:339-349
- **Category**: api-design
- **Description**: `WriteOptions::no_slowdown` and `low_pri` are exposed but never read in `DbImpl::write_opt`/`apply_group_commit_batch`. Public fields that "do nothing" are an anti-pattern; a future maintainer will assume they work.
- **Suggested fix**: Either implement the behaviour, mark them `#[doc(hidden)]` until implemented, or drop them from `WriteOptions` for now.
- **Effort**: trivial

### A-15. `DbImpl::put_opt` exists but `Db::put_opt` does not

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/db/db_impl.rs:1683, /Users/yangjie01/SourceCode/git/st-rs/src/api/db.rs:100
- **Category**: api-design
- **Description**: The trait has only `put`/`put_cf` taking `WriteOptions`. The concrete type has `put`, `put_opt`. Both shapes co-exist (no-opts vs opts). Inconsistent: most users either want all-or-nothing-default. This compounds Finding A-1.
- **Suggested fix**: Either drop `put_opt` and standardize on "opts always required" (idiomatic for the trait), or add `_opt` siblings on the trait so the API is uniform between the trait and the impl.
- **Effort**: small

### A-16. `EmptyIterator::next/prev/key/value` panic — not a great trait contract

- **Severity**: MEDIUM
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/api/iterator.rs:97-111
- **Category**: api-design
- **Description**: The `DbIterator` trait documents that `next`/`prev`/`key`/`value` are valid only when `valid()` is true, and `EmptyIterator` panics rather than returning sentinel slices or `None`. A panicking trait contract is hostile to a future C ABI (FFI cannot catch panics safely without `catch_unwind`) and tempts callers to guard every call. Upstream returns an empty slice and silently ignores invalid `next`.
- **Suggested fix**: Make the contract explicit: either trait methods are total (return `Option<&[u8]>` or empty slice) or define a single "valid required" rule and document the panic explicitly. Add `#[must_use]` to `valid` to encourage proper guards. For the C ABI, plan for either a total interface or explicit catch_unwind boundaries.
- **Effort**: medium

### A-17. `DbIterator` is both a trait name and a concrete struct re-exported as the same identifier

- **Severity**: HIGH
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/lib.rs:86-88, /Users/yangjie01/SourceCode/git/st-rs/src/lib.rs:184
- **Category**: naming
- **Description**: `crate::api::iterator::DbIterator` (trait) and `crate::db::db_iter::DbIterator` (concrete struct) share the same name. The crate root re-exports the concrete one (line 184) so `use st_rs::DbIterator` resolves to the struct; users wanting the trait must remember to write `st_rs::api::iterator::DbIterator`. This is a guaranteed source of confusion, conflicts in `use` lines, and rustdoc collisions. The accompanying comment ("Most users never need the trait directly") admits the problem.
- **Suggested fix**: Rename one of them. Common conventions: trait stays `DbIterator`, the concrete becomes `OwnedDbIterator` / `MaterializedDbIterator` / `SnapshotIterator`, or the trait becomes `Cursor` / `Iter`. Pick one and apply consistently.
- **Effort**: small

### A-18. `Slice` is a type alias, but the rest of the API never uses it

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/core/slice.rs:38, /Users/yangjie01/SourceCode/git/st-rs/src/api/db.rs:100
- **Category**: api-design
- **Description**: `Slice<'a>` is exported as a type alias for `&'a [u8]`. None of the `Db` / `WriteBatch` / `DbIterator` methods take or return `Slice` — they use `&[u8]` and `Vec<u8>` directly. The type alias adds noise without value: callers must learn it exists but never need it. The companion `SliceExt` trait provides only two helpers (`difference_offset`, `to_hex_string`) of marginal utility for a public API.
- **Suggested fix**: Either commit to `Slice` throughout the public surface (so users see consistent types) or remove the alias and `SliceExt` from the public API. The current half-in/half-out state is the worst of both.
- **Effort**: small

### A-19. `Status` has all-public fields, blocking ABI evolution

- **Severity**: HIGH
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/core/status.rs:137-153
- **Category**: abi
- **Description**: `Status { pub code, pub subcode, pub severity, pub retryable, pub data_loss, pub scope, pub message }` exposes every field as `pub`. Any change to the struct shape (adding a flag, packing repr) is a breaking change. For a project that will mirror to C ABI and is the universal error type, this is one of the highest-leverage things to lock down before v1.
- **Suggested fix**: Make fields private and expose accessors (`code()`, `subcode()`, `message()`). Provide builder-style constructors. This unblocks future ABI work (packing the struct to fit in a register pair, adding error chaining, swapping to `Box<str>` for messages, etc.) without breakage.
- **Effort**: medium

### A-20. `Code` and `SubCode` are `#[repr(u8)]` with documented numeric values, but not `#[non_exhaustive]`

- **Severity**: MEDIUM
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/core/status.rs:47-110
- **Category**: abi
- **Description**: The `Code` enum is documented "new variants may only be appended at the end, to preserve the numeric values for on-disk compatibility," but it isn't `#[non_exhaustive]`, so downstream `match` arms over `Code` will silently fail to compile when a new code is added. The same applies to `SubCode`, `Severity`, `FileType`, `EntryType`, `ValueType`, `WriteStallCause`, `WriteStallCondition`, `Temperature`, `CompressionType`, `ChecksumType`, `CompactionDecision`, `CachePriority`, `IndexType`, `IoPriority`, `IoType`, `AccessPattern`, `InfoLogLevel`, `Priority`, `TableFormatVersion`, `TableFileCreationReason`, `BlobFileCreationReason`.
- **Suggested fix**: Add `#[non_exhaustive]` to all public enums that may grow. This is the standard Rust pattern for "this enum may add variants in a non-breaking release."
- **Effort**: trivial

### A-21. `IoStatus` is a type alias for `Status` — purely documentary, no enforcement

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/core/status.rs:40
- **Category**: api-design
- **Description**: `pub type IoStatus = Status` is exposed as a "documentation" tag for filesystem-layer errors. There is no compile-time distinction; any `Status` can be assigned to an `IoStatus` and vice versa. It clutters the public surface (re-exported from the crate root) while providing zero type safety.
- **Suggested fix**: Either make `IoStatus` a newtype (`struct IoStatus(Status)`) so the docs are backed by the compiler, or remove the alias and rely on convention in prose.
- **Effort**: small

### A-22. `Status::ok()` exists but `Status::default() == Status::ok()` is undocumented; OK statuses go through `Result`

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/core/status.rs:155-180
- **Category**: api-design
- **Description**: The Rust idiom is `Result<T, Status>` where the `Ok` variant carries `T`; `Status` itself should only represent failures. But `Status::ok()` exists and `is_ok()` is on the type, hinting at a C++ "Status returned by value, check `is_ok()`" pattern. This will trip up Rust-native users (who expect `Result`) and is incongruent with the rest of the crate which already returns `Result<T, Status>`. Holding both idioms wastes mindshare.
- **Suggested fix**: Pick one. If `Result<T, Status>` is canonical (and it is, per `pub type Result<T> = core::result::Result<T, Status>`), then remove `Status::ok()` and `is_ok()` from the public API.
- **Effort**: small

### A-23. `DbImpl::open` returns `Arc<Self>` while `DbOpener::open` returns `Self::Db`

- **Severity**: MEDIUM
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/db/db_impl.rs:468, /Users/yangjie01/SourceCode/git/st-rs/src/api/db.rs:214
- **Category**: api-design
- **Description**: `DbImpl::open(&opts, path) -> Result<Arc<Self>>` returns an Arc because background tasks need `Weak<Self>`. The `DbOpener` trait specifies `fn open(...) -> Result<Self::Db>`. These signatures cannot meet: the inherent factory must return an Arc, but the trait demands a bare value. This is part of why `DbOpener` has no implementations (Finding A-9).
- **Suggested fix**: Change `DbOpener::open` to return `Result<Arc<Self::Db>>`, or have the engine hold a reachable `Weak<DbImpl>` differently (e.g. `Arc::new_cyclic`). The Arc-vs-bare divergence must be resolved before multiple backends are realistic.
- **Effort**: medium

### A-24. `ColumnFamilyHandle` trait has no `Drop` semantics — drop ordering vs `close` is undefined

- **Severity**: HIGH
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/api/db.rs:44-50, /Users/yangjie01/SourceCode/git/st-rs/src/api/db.rs:201
- **Category**: api-design
- **Description**: `Db::close` says "all column family handles become invalid." But `ColumnFamilyHandle` is `Send + Sync` with no documented lifetime or drop contract. If the user holds an `Arc<dyn ColumnFamilyHandle>` after `close()` and then calls `cf.id()` (allowed by the trait), what happens? In the current impl, `ColumnFamilyHandleImpl` doesn't reference the engine, so calls are safe but stale. In a future impl that resolves through the engine, this is a use-after-free. Either way the trait contract is silent.
- **Suggested fix**: Decide: are handles refcounted to the DB (engine outlives them) or invalidated on close (calls return error)? Document on the trait and back the choice with the type system (e.g. handles carry an `Arc<Db>` or return `Result<..>` on every method).
- **Effort**: medium

### A-25. `DEFAULT_COLUMN_FAMILY_NAME` is in `api::db` but `DEFAULT_CF` is in `api::write_batch`

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/api/db.rs:74, /Users/yangjie01/SourceCode/git/st-rs/src/api/write_batch.rs:41
- **Category**: naming
- **Description**: The default-CF name string (`"default"`) is `DEFAULT_COLUMN_FAMILY_NAME`. The default-CF id (`0`) is `DEFAULT_CF`. These are two facets of the same concept, named inconsistently, located in different modules. Neither is re-exported at the crate root. `DbImpl` defines a third, `DEFAULT_CF_ID: ColumnFamilyId = 0` privately.
- **Suggested fix**: Co-locate them in `api::db` (or a new `api::column_family` module) with consistent naming: `DEFAULT_CF_NAME` and `DEFAULT_CF_ID`. Re-export both at the crate root.
- **Effort**: trivial

### A-26. `WriteBatch::iterate` mixes "ordered list of records" with "WAL serialisation" — neither commits to a format

- **Severity**: MEDIUM
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/api/write_batch.rs:43-55, /Users/yangjie01/SourceCode/git/st-rs/src/db/db_impl.rs:3340-3370
- **Category**: api-design
- **Description**: `WriteBatch` doc says "The exact wire format is NOT frozen at Layer 0" and stores `Vec<Record>` (logical). But `DbImpl` already serialises this with its own tag bytes (0x0/0x1/0x2/0x7/0xF). The wire format is in fact frozen by the WAL — any change breaks replay. The "format is open" doc is a comforting lie.
- **Suggested fix**: Either move WAL serialisation into `WriteBatch::encode()`/`decode()` and freeze the wire format with version numbers, or split `WriteBatch` (the user-facing batch) from `WalRecord` (the internal encoding). Don't claim flexibility you don't have.
- **Effort**: medium

### A-27. `WriteBatchHandler::delete_range_cf` default implementation silently swallows tombstones

- **Severity**: MEDIUM
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/api/write_batch.rs:234-256
- **Category**: api-design
- **Description**: All `WriteBatchHandler` methods default to `Ok(())` so implementors override only what they care about. But `delete_range_cf` is a tombstone-emitting operation; if a downstream handler forgets to override it, the range delete is silently dropped while the batch reports success. This is the kind of error that escapes testing.
- **Suggested fix**: Default `delete_range_cf` (and arguably `merge_cf`, `single_delete_cf`) to `Err(Status::not_supported(...))` so unhandled tombstones surface loudly. Or split the trait so the "core" methods are required.
- **Effort**: trivial

### A-28. `Snapshot::unix_time_nanos` default returns 0 (= "unknown"), but the type is `u64` with no sentinel discrimination

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/api/snapshot.rs:40-43
- **Category**: api-design
- **Description**: The doc says "default returns 0 meaning 'unknown'", but `u64` cannot distinguish "epoch" from "unknown." If a snapshot is ever truly taken at epoch (or near it), the API lies.
- **Suggested fix**: Return `Option<u64>` so "unknown" has a real type. Same fix on `Snapshot::timestamp` already does this correctly — be consistent.
- **Effort**: trivial

### A-29. `DbSnapshot` is re-exported, but it cannot be constructed by user code

- **Severity**: MEDIUM
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/lib.rs:194, /Users/yangjie01/SourceCode/git/st-rs/src/db/db_impl.rs:2987
- **Category**: api-design
- **Description**: `DbSnapshot` has private fields (`seq`, `db: Weak<DbImpl>`) and no public constructor — it can only be obtained via `DbImpl::snapshot()`. Yet it is re-exported at the crate root. The re-export is misleading: users see the name in rustdoc and try to construct one, only to discover they can't.
- **Suggested fix**: Either (a) keep `DbSnapshot` accessible only via path (`db::db_impl::DbSnapshot`) and remove the crate-root re-export, or (b) keep the re-export but mark the type `#[non_exhaustive]` so it's clear it cannot be constructed.
- **Effort**: trivial

### A-30. Crate root re-exports flatten Layer 3+/4+ engine internals into the public API

- **Severity**: HIGH
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/lib.rs:146-200
- **Category**: api-design / abi
- **Description**: The lib root re-exports `InternalKey`, `LookupKey`, `ParsedInternalKey`, `MAX_SEQUENCE_NUMBER`, `VALUE_TYPE_FOR_SEEK`, `MemTable`, `SkipList`, `Block`, `BlockBuilder`, `BlockHandle`, `Footer`, `BLOCK_TRAILER_SIZE`, `BlockBasedTableBuilder`, `BlockBasedTableReader`, `BlockCache`, `LogReader`, `LogWriter`, `MergingIterator`, `SstIter`, `BinaryHeap`, `pick_compaction`, `CompactionJob`, `READ_AT_LATEST`. These are engine internals — exposing them at the crate root makes them part of the public API, and any change will be a breaking change. The C-ABI crate planned for Stage 2 will inherit all of this.
- **Suggested fix**: Triage: keep only the user-facing types (`Db`, `WriteBatch`, `Snapshot`, options, status, slice, types, `ColumnFamilyHandle`, `Comparator`, `MergeOperator`, `CompactionFilter`, `Cache`, `FilterPolicy`, `TableFactory`, `Env`/`FileSystem`, `Logger`, `Statistics`, `FlinkStateBackend`). Everything else (`InternalKey`, `MemTable`, `Block`, `LogReader`, `SstIter`, ...) should be `pub` in its module but not re-exported at the root, or moved behind a `pub mod internals` gated module clearly marked as unstable.
- **Effort**: medium

### A-31. Layer numbering in module docs and lib.rs leaks implementation history into the public API

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/lib.rs:81-211
- **Category**: doc-coverage
- **Description**: The crate root divides re-exports into "Layer 0 / Layer 1 / Layer 2 / Layer 3a / Layer 3b / Layer 4a / Layer 4b / Layer 4c / Layer 4e / Layer 5 / Layer 7" sections. These layers are FORST-READING-ORDER artifacts and have no meaning to a user of the crate. They actively make rustdoc harder to read (since users see a sectioned list rather than a topical one).
- **Suggested fix**: Group re-exports by user-facing topic (DB operations / iteration / options / extension points / status / flink integration), not by porting layer. Keep the layer mapping in a `CONTRIBUTING.md` or `ARCHITECTURE.md`.
- **Effort**: small

### A-32. `db::version_set` and `db::version_edit` are `pub mod` but not re-exported — and they shouldn't be `pub`

- **Severity**: MEDIUM
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/db/mod.rs:33-34
- **Category**: module-layering
- **Description**: `db/mod.rs` declares `pub mod version_edit; pub mod version_set;`. These are pure engine internals (MANIFEST format, file lifecycle bookkeeping). Exposing them via `pub mod` puts them on the public API surface; any change is breaking. The doc on `db/mod.rs` even says "the rest of `db/` (write path, version set, compaction, iterators, db_impl.cc) is Layer 4 and is intentionally absent" — but then makes them all `pub`.
- **Suggested fix**: Make engine-internal modules `pub(crate)` (or `pub(super)`). Audit every `pub mod` in `src/db/`, `src/sst/`, `src/memtable/`, `src/cache/`, `src/file/`, `src/util/` for whether the user really needs that surface. Most should be crate-private.
- **Effort**: medium

### A-33. `Db` trait is open for downstream impls but has no sealed marker — anyone can implement it

- **Severity**: MEDIUM
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/api/db.rs:81
- **Category**: extensibility / abi
- **Description**: The `Db` trait is unsealed. Today only `DbImpl` implements it, but the documentation says "the trait exists so multiple backends are conceivable." Once published, the trait is part of the public contract and adding methods is a breaking change. For a project still iterating on the engine surface (manual compaction, set_options, get_options, listener registration), this is a significant constraint.
- **Suggested fix**: Decide: (a) seal `Db` (private super-trait pattern) to keep room to grow, or (b) commit to "Db is forever open for impl" and freeze the method set. The same question applies to `Comparator`, `MergeOperator`, `CompactionFilter`, `Cache`, `FilterPolicy`, `TableFactory`, `Env`, `FileSystem`, `Logger`, `Statistics`, `Snapshot`, `ColumnFamilyHandle`, `WriteBatchHandler`, `DbIterator`. The right answer is probably "extension-point traits open, engine-handle traits sealed."
- **Effort**: medium

### A-34. `FlinkStateBackend` wraps `DbImpl` directly rather than the `Db` trait

- **Severity**: MEDIUM
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/flink/state_backend.rs:42-43
- **Category**: module-layering
- **Description**: `FlinkStateBackend { db: Arc<DbImpl> }` couples the Flink integration directly to the concrete engine. If another backend ever exists, the Flink layer cannot use it without modification. This contradicts the stated goal of the `Db` trait. Worse, `FlinkStateBackend` is in `src/flink/`, not `src/utilities/`, so the Flink-specific code is upstream of the generic engine — a layering inversion.
- **Suggested fix**: Make `FlinkStateBackend` generic over `D: Db` (or take `Arc<dyn Db>`). Move it to `src/utilities/flink/` to clarify that it's a utility built on the engine, not part of the engine.
- **Effort**: medium

### A-35. `flink` module re-exports leak Flink-specific concrete types at the crate root

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/lib.rs:207-211
- **Category**: module-layering
- **Description**: `FlinkFileStatus`, `FlinkFsBackend`, `TtlCompactionFilter`, `TtlCompactionFilterFactory`, `FlinkFileSystem`, `InMemoryFsBackend`, `FlinkStateBackend` are all crate-root re-exports. A user not doing Flink integration sees all of this in their rustdoc. The `flink` namespace prefix on most names is a Hungarian-notation-ism that exists *because* they were flattened.
- **Suggested fix**: Stop re-exporting `flink::*` from the crate root. Users who want Flink integration write `use st_rs::flink::*`. The names can then lose the `Flink` prefix (e.g. `flink::FileSystem`, `flink::Backend`, `flink::StateBackend`).
- **Effort**: small

### A-36. `InMemoryFsBackend` is publicly exported as both a test mock and a production type

- **Severity**: MEDIUM
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/lib.rs:210, /Users/yangjie01/SourceCode/git/st-rs/src/flink/mod.rs:30-32
- **Category**: api-design
- **Description**: `InMemoryFsBackend` is documented as "the Rust equivalent of upstream's `java/flinktestmock/`" — i.e. a test mock. Yet it's re-exported at the crate root with no `#[cfg(test)]` or `test_util` feature gate. Users may accidentally ship a test backend in production.
- **Suggested fix**: Move test-only types behind a `test-util` feature flag (or to a separate `st-rs-testing` crate). At minimum, mark the type with `#[doc(hidden)]` and document it as for-tests-only.
- **Effort**: small

### A-37. `PosixFileSystem` is unconditionally re-exported behind `#[cfg(unix)]` — Windows is silently broken

- **Severity**: MEDIUM
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/lib.rs:130-134
- **Category**: module-layering / abi
- **Description**: `PosixFileSystem` and friends are gated on `#[cfg(unix)]`, but `DbImpl::open` uses `PosixFileSystem::new()` unconditionally. On Windows, this would either fail to compile or fail at runtime depending on how the `posix` module is gated. The Stage-2 C ABI will inherit this Unix dependency.
- **Suggested fix**: Either (a) make `PosixFileSystem` cross-platform by adding a Windows file backend, (b) provide a `DefaultFileSystem` alias that picks the right backend per OS, or (c) explicitly drop Windows support in `Cargo.toml`. Don't ship a build that silently fails outside Unix.
- **Effort**: medium

### A-38. The `core::` module duplicates `std::result::Result` and re-exports its own `Result`

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/core/status.rs:31, /Users/yangjie01/SourceCode/git/st-rs/src/lib.rs:96
- **Category**: naming
- **Description**: `pub type Result<T> = core::result::Result<T, Status>` is re-exported at the crate root as `st_rs::Result`. This shadows `std::result::Result` for any user doing `use st_rs::*` or `use st_rs::Result`. Some users will be surprised when their `Result<T, E>` no longer compiles (E is locked).
- **Suggested fix**: Either keep the alias only in the `core` module (so users explicitly opt in: `use st_rs::core::status::Result`), or rename to `StResult` to avoid shadowing.
- **Effort**: trivial

### A-39. `Slice` re-export collides conceptually with `core::Slice` and `std::slice`

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/lib.rs:95
- **Category**: naming
- **Description**: `st_rs::Slice` is a type alias for `&[u8]`. With `use st_rs::*`, a function signature `fn foo(s: Slice)` reads ambiguously. Combined with the existence of `core::slice::*` and `std::slice::*` in the prelude, this is a footgun.
- **Suggested fix**: Either drop the alias from the public API (see A-18), or rename to `Bytes` / `KeySlice` / `ByteSlice`. A 5-char name shadowing fundamental std vocabulary is risky.
- **Effort**: trivial

### A-40. `Logger`, `Statistics`, `Histogram`, `Arena`, `Allocator`, `PerfContext` are re-exported but most users never need them

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/lib.rs:118-124
- **Category**: api-design
- **Description**: Layer 1 infrastructure types are re-exported at the crate root. None of them appear in any other public API method signature (e.g. `DbOptions` does not take a `Logger`). They are "available for inspection" but nothing the user can plug in. This is dead surface that hides the genuinely useful exports.
- **Suggested fix**: Either wire these into the public API (e.g. `DbOptions { logger: Option<Arc<dyn Logger>>, statistics: Option<Arc<dyn Statistics>> }` — upstream does this), or stop re-exporting them at the root.
- **Effort**: small

### A-41. `Comparator::find_shortest_separator` mutates `start` in place — non-idiomatic Rust

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/ext/comparator.rs:55-59
- **Category**: api-design
- **Description**: `fn find_shortest_separator(&self, _start: &mut Vec<u8>, _limit: &[u8])` and `find_short_successor(&self, _key: &mut Vec<u8>)` mutate their input. Per project coding style (`coding-style.md`: "ALWAYS create new objects, NEVER mutate existing ones"), this is an explicit anti-pattern. The signature is a direct port of upstream C++ but doesn't fit Rust idioms.
- **Suggested fix**: Return `Vec<u8>` (or `Cow<'_, [u8]>` for the no-shorten case) instead of mutating in place. This also aligns with the project's stated immutability rule.
- **Effort**: small

### A-42. `TableFactory` trait has only `name()` — every concrete method is "intentionally deferred"

- **Severity**: MEDIUM
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/ext/table.rs:26-35
- **Category**: extensibility
- **Description**: `TableFactory` ships with only `name()`. The actual factory methods (`new_table_builder`, `new_table_reader`) are commented out and deferred. As an extension point this is unusable — no one can implement a custom SST format because the trait has no methods. The trait exists in name only.
- **Suggested fix**: Either complete `TableFactory` (so users can plug in `PlainTable`, `CuckooTable`, etc.) or remove it from the public API until it can be made functional. Don't ship empty extension points.
- **Effort**: medium

### A-43. `CompactionFilter` extension point is wired only by name string lookup, not by factory injection

- **Severity**: MEDIUM
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/api/options.rs:188-191, /Users/yangjie01/SourceCode/git/st-rs/src/db/db_impl.rs:190-202
- **Category**: extensibility
- **Description**: `ColumnFamilyOptions::compaction_filter_name: String` and `merge_operator_name: String` are name strings resolved by `resolve_merge_operator(name)` which hardcodes only `StringAppendOperator`. Users with custom merge operators or compaction filters cannot register them through the options struct — they must use `DbImpl::set_merge_operator` and `set_compaction_filter_factory` after open. This contradicts the documented "options-driven" model and makes it impossible to set per-CF filters on the first `open_cf` call.
- **Suggested fix**: Accept `Option<Arc<dyn MergeOperator>>` and `Option<Arc<dyn CompactionFilterFactory>>` directly on `ColumnFamilyOptions`. Keep the name string only for OPTIONS-file serialisation. This matches upstream RocksDB.
- **Effort**: medium

### A-44. `DbOptions` lacks an `Env`/`FileSystem`/`Logger`/`Statistics` slot

- **Severity**: HIGH
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/api/options.rs:46-128
- **Category**: api-design
- **Description**: Upstream `DBOptions::env`, `DBOptions::info_log`, `DBOptions::statistics`, `DBOptions::row_cache`, `DBOptions::rate_limiter` are how a user customises the engine's environment. The Rust port has none of these on `DbOptions` — instead, `DbImpl::open_with_fs` is a parallel constructor. This means a user cannot set both a custom FS and a custom statistics object through one options struct, and any future ABI mirror has to invent ad-hoc setters.
- **Suggested fix**: Add `env`, `file_system`, `logger`, `statistics`, `block_cache` (as a shareable `Arc<dyn Cache>`), and `rate_limiter` fields on `DbOptions` (all `Option<Arc<dyn ...>>`). Make `DbImpl::open` consult these. Deprecate `open_with_fs`.
- **Effort**: medium

### A-45. `block_cache_size: usize` in `DbOptions` is a primitive but the engine accepts shared `Arc<dyn Cache>`

- **Severity**: MEDIUM
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/api/options.rs:127, /Users/yangjie01/SourceCode/git/st-rs/src/db/db_impl.rs:451-452
- **Category**: api-design
- **Description**: The user can only specify a *size* for the block cache, not bring their own `Arc<dyn Cache>`. Two DBs cannot share a block cache, and a user cannot inject a custom cache implementation through the options object. The `Cache` trait is defined for nothing.
- **Suggested fix**: Replace `block_cache_size: usize` with `block_cache: Option<Arc<dyn Cache>>` (with a `block_cache_size` shortcut that constructs a default LRU). This matches upstream `BlockBasedTableOptions::block_cache`.
- **Effort**: small

### A-46. `BloomFilterPolicy::DEFAULT_BITS_PER_KEY` and `FILTER_METAINDEX_KEY` are crate-root constants

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/lib.rs:166-168
- **Category**: api-design
- **Description**: `BloomFilterBuilder`, `BloomFilterPolicy`, `BloomFilterReader`, `DEFAULT_BITS_PER_KEY`, `FILTER_METAINDEX_KEY` are all re-exported at the crate root. The reader and builder are engine internals (compaction/flush only uses them); only `BloomFilterPolicy` is a user-facing extension point. Polluting the top-level rustdoc with builder/reader implementation details makes the API harder to discover.
- **Suggested fix**: Only re-export `BloomFilterPolicy`. Move `BloomFilterBuilder`/`Reader` and the constants behind their module path.
- **Effort**: trivial

### A-47. `WAL_BLOCK_SIZE`, `WAL_HEADER_SIZE`, `RecordType`, `LogReader`, `LogWriter` are part of the public API

- **Severity**: HIGH
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/lib.rs:177-179
- **Category**: abi
- **Description**: The WAL format types are re-exported at the crate root. The WAL format is the most ABI-sensitive part of the engine — changing the block size or header layout breaks every existing DB on disk. Exposing these types as `pub` means a downstream user could write code against them and any change becomes a breaking change in both the Rust API and the wire format.
- **Suggested fix**: Make WAL types `pub(crate)`. The WAL format should be an implementation detail. Document the on-disk format in `docs/wal.md` if users need to know it.
- **Effort**: trivial

### A-48. `BlockCache` is a type alias re-exported but its definition is private to `table_reader`

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/lib.rs:172
- **Category**: api-design
- **Description**: `BlockCache` is re-exported but its definition lives in `sst::block_based::table_reader`. Users see "BlockCache" in rustdoc but tracing where it comes from is non-obvious. If `BlockCache` is `Arc<dyn Cache>`, the alias adds nothing; if it's a wrapper, it should live in `cache::` or `ext::cache::`.
- **Suggested fix**: Move `BlockCache` to `cache::` (or remove if it's just an alias). Place type aliases near their conceptual home.
- **Effort**: trivial

### A-49. `DbIterator` (concrete) is eager — materializes the entire DB into memory per `iter()` call

- **Severity**: HIGH
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/db/db_iter.rs:30-46
- **Category**: api-design / abi
- **Description**: The concrete `db_iter::DbIterator` "materialises every live (non-tombstone) entry in the engine into a sorted owned Vec at construction time." This is O(N) memory and time per `iter()` call. For Flink — which constantly scans state — this is fundamentally unworkable at non-trivial state sizes. The doc admits the workaround ("eager materialisation … keeps Layer 4b safe and reviewable") but the C ABI will lock the type's shape before a streaming variant is ready.
- **Suggested fix**: Treat the current eager iterator as private (don't re-export from crate root, or rename to `EagerDbIterator`). Plan and ship a streaming iterator before promising any iterator type as part of the C ABI surface. This is a P0 for production use.
- **Effort**: large

### A-50. `iter()` returns the concrete `DbIterator` struct, but `Db::new_iterator` returns `Box<dyn DbIterator>` (the trait)

- **Severity**: MEDIUM
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/db/db_impl.rs:2252, /Users/yangjie01/SourceCode/git/st-rs/src/api/db.rs:162
- **Category**: api-design
- **Description**: Trait `Db::new_iterator` returns `Box<dyn DbIterator>` (trait object, dynamic dispatch). Inherent `DbImpl::iter()` returns `Result<crate::db::db_iter::DbIterator>` (concrete, static dispatch). The two APIs do not produce compatible iterators (the concrete one doesn't implement the trait, see A-2). A user starting with `DbImpl::iter()` cannot later swap in `Arc<dyn Db>` without rewriting iterator-touching code.
- **Suggested fix**: Pick one return shape. Either trait object everywhere (uniform but dyn-dispatch overhead), or impl-defined associated type (`type Iter: DbIterator; fn iter(&self) -> Self::Iter;`) for static dispatch.
- **Effort**: medium

### A-51. `FlushOptions::wait`'s default is `true`, but `wait_for_pending_work` exists separately

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/api/options.rs:351-367, /Users/yangjie01/SourceCode/git/st-rs/src/db/db_impl.rs:2484
- **Category**: api-design
- **Description**: `FlushOptions::wait = true` (default) makes `flush()` synchronous, but there's also a separate `wait_for_pending_work()` and `wait_for_pending_flush()`. Three different "wait for flush" entry points exist; their relationship is undocumented (does `flush(opts{wait:false})` complete before background tasks finish? what does `schedule_flush` return?).
- **Suggested fix**: Consolidate. Either `flush(opts)` waits per `opts.wait` and `wait_for_pending_flush` is removed; or `flush()` is always async and waiting is always explicit. Document the chosen model with a state diagram.
- **Effort**: small

### A-52. `CompactionOptions` exists but `compact_range` doesn't accept it

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/api/options.rs:369-378, /Users/yangjie01/SourceCode/git/st-rs/src/api/db.rs:191
- **Category**: api-design
- **Description**: `CompactionOptions { compression, output_file_size_limit, max_subcompactions }` is defined but never consumed by any public method. `Db::compact_range(begin, end)` takes no `CompactionOptions`. The type is dead.
- **Suggested fix**: Either accept `CompactionOptions` in `compact_range` (matching upstream), or drop the struct.
- **Effort**: trivial

### A-53. `FileOptions` is in `api::options` but `IoOptions` is in `env::file_system` — overlapping responsibilities

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/api/options.rs:383-399, /Users/yangjie01/SourceCode/git/st-rs/src/env/file_system.rs:72-84
- **Category**: module-layering / naming
- **Description**: `FileOptions` (mmap, direct I/O, sync, checksum) and `IoOptions` (timeout, priority, type, force_dir_fsync) both describe per-file-operation options. The split is artificial: a future FS backend wanting to know "use mmap and time out after 5s" has to take two structs. Upstream merges these.
- **Suggested fix**: Merge `FileOptions` and `IoOptions` into one `FileIoOptions` in `env::file_system`. Move the file-creation-time options (mmap, direct I/O) into `DbOptions` where they conceptually live.
- **Effort**: medium

### A-54. `Env`, `FileSystem`, and the engine all hold their own thread pool — three pools, one program

- **Severity**: MEDIUM
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/env/env_trait.rs:143-165, /Users/yangjie01/SourceCode/git/st-rs/src/db/db_impl.rs:444-445
- **Category**: module-layering
- **Description**: `Env::thread_pool()` is the documented way to get a thread pool, but `DbImpl` constructs its own `Arc<StdThreadPool>` directly rather than going through the `Env`. The `Env` abstraction is partially bypassed. A user installing a custom `Env` thinking they're injecting a thread pool will be surprised.
- **Suggested fix**: Either (a) make `DbImpl::open` always consult `env.thread_pool()`, or (b) drop `ThreadPool` from the `Env` trait. The current half-state is worse than either alternative.
- **Effort**: small

### A-55. `Env` trait does not expose a host_name override path through `DbOptions`

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/env/env_trait.rs:143-165, /Users/yangjie01/SourceCode/git/st-rs/src/api/options.rs:46
- **Category**: api-design
- **Description**: `Env::host_name` and `Env::get_thread_id` are documented as override points. But `DbOptions` has no `env` field (see A-44), so there's no way to actually install a custom `Env` and have the engine use it. The override points are documented but unreachable.
- **Suggested fix**: Wire `DbOptions::env: Option<Arc<dyn Env>>` and have the engine consult it. Same fix as A-44.
- **Effort**: medium

### A-56. `BloomFilterBuilder` / `BloomFilterPolicy` constructor exposed publicly but is filter-block-format-coupled

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/lib.rs:165-168
- **Category**: abi
- **Description**: `BloomFilterBuilder` is exposed at the crate root, but it is tightly coupled to the on-disk filter block format. Any change to the filter block layout (e.g. moving to ribbon filters) is now a breaking change to the public API. Users should plug in alternative filters through the `FilterPolicy` trait, not by constructing builders directly.
- **Suggested fix**: Make `BloomFilterBuilder`/`BloomFilterReader` `pub(crate)`. Keep only `BloomFilterPolicy` and `FilterPolicy` in the public API.
- **Effort**: trivial

### A-57. `Snapshot` trait and `SimpleSnapshot` co-exist with `DbSnapshot` — three concepts for one thing

- **Severity**: MEDIUM
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/api/snapshot.rs:55-70, /Users/yangjie01/SourceCode/git/st-rs/src/db/db_impl.rs:2987
- **Category**: api-design
- **Description**: There are three "snapshot" types: the `Snapshot` trait (interface), `SimpleSnapshot` (test-grade impl carrying just a seq), and `DbSnapshot` (production impl with `Weak<DbImpl>` for release-on-drop). All are public; all are re-exported. Users have to know which to use. The `SimpleSnapshot::new(seq)` constructor allows a user to fabricate a snapshot with an arbitrary seq, which combined with `ReadOptions::snapshot: Option<u64>` (A-6) lets them read at a sequence they never owned.
- **Suggested fix**: Keep `Snapshot` (trait) and one production impl. Make `SimpleSnapshot` `pub(crate)` or move it behind `#[cfg(test)]`. Fix A-6 simultaneously.
- **Effort**: small

### A-58. `MAX_SEQUENCE_NUMBER`, `VALUE_TYPE_FOR_SEEK`, `READ_AT_LATEST` are three different "max sequence" constants

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/db/dbformat.rs:84, /Users/yangjie01/SourceCode/git/st-rs/src/db/db_iter.rs:62
- **Category**: naming
- **Description**: `MAX_SEQUENCE_NUMBER = (1 << 56) - 1`, `READ_AT_LATEST = u64::MAX >> 8` (also `(1 << 56) - 1`). They have identical values but different names and live in different modules. Both are re-exported at the crate root. A user looking at "read at latest" might wonder why it's not `MAX_SEQUENCE_NUMBER`.
- **Suggested fix**: Pick one. Either `READ_AT_LATEST` is an alias for `MAX_SEQUENCE_NUMBER`, or one is removed.
- **Effort**: trivial

### A-59. `validate_sst` is a free function at module scope, separate from `BlockBasedTableReader`

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/db/ingest_external_file.rs:58
- **Category**: api-design
- **Description**: `validate_sst(fs, path)` is a free function, while every other SST operation is on a builder/reader struct. It's inconsistent — a user looking for "is this a valid SST?" expects it on `BlockBasedTableReader::validate(path)`.
- **Suggested fix**: Either move `validate_sst` to `BlockBasedTableReader::validate(fs, path)` as an associated function, or commit to free-function module style for the SST API.
- **Effort**: trivial

### A-60. `create_checkpoint` is a free function instead of a method/trait

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/utilities/checkpoint.rs:75, /Users/yangjie01/SourceCode/git/st-rs/src/lib.rs:203
- **Category**: api-design
- **Description**: `create_checkpoint(&DbImpl, path)` takes a concrete `&DbImpl`. It cannot be called on `&dyn Db`. Plus it's a free function rather than a method or trait method on `Db`. This couples checkpointing to `DbImpl`, mirroring the layering issue in A-34.
- **Suggested fix**: Either add `Db::checkpoint(path)` to the trait, or take `&dyn Db` (require enough trait methods to support it). Either way, decouple from `DbImpl`.
- **Effort**: medium

### A-61. `delete_files_in_ranges` accepts `&[(&[u8], &[u8])]` — non-obvious tuple ordering

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/db/db_impl.rs:1336-1340
- **Category**: api-design
- **Description**: `ranges: &[(&[u8], &[u8])]` is a slice of (begin, end) tuples but neither parameter is named. Combined with `delete_range_cf(cf, begin, end)` which has begin/end positional, callers may swap them silently.
- **Suggested fix**: Introduce a `Range { begin: &[u8], end: &[u8] }` struct (or use the standard `std::ops::Range<&[u8]>` shape) so the ordering is type-enforced.
- **Effort**: trivial

### A-62. `DbImpl::sequence()` exposes internal sequence number — a leaky abstraction

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/db/db_impl.rs:2997
- **Category**: api-design / abi
- **Description**: `pub fn sequence(&self) -> SequenceNumber` exposes the engine's internal monotonic counter. This is observable state with no clear use case in user code (snapshots already encapsulate sequences). Exposing it as `pub` invites users to write code that depends on the exact sequence number, which the engine has no contract to preserve.
- **Suggested fix**: Make `sequence()` `pub(crate)` or hide behind a debug/diagnostic feature flag. Users who want a snapshot should use `snapshot()`.
- **Effort**: trivial

### A-63. `WriteStallCause` and `WriteStallCondition` are exported but never returned by any method

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/core/types.rs:113-134
- **Category**: api-design
- **Description**: `WriteStallCondition` and `WriteStallCause` are public enums exported at the crate root, but no `Db` method returns them. `Db::is_write_stopped` is on the inherent impl but returns a `bool`. The user has no way to observe "I am being stalled because of X."
- **Suggested fix**: Either expose stall state via `Db::get_property("write-stall")` returning a structured value, or remove these types from the public API until they're consumed.
- **Effort**: small

### A-64. `EntryType`, `ValueType`, `FileType` are three overlapping enums about "kinds of stored thing"

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/core/types.rs:60-108
- **Category**: api-design
- **Description**: `EntryType` (Put/Delete/Merge/...), `ValueType` (Deletion/Value/Merge/SingleDeletion/RangeDeletion/...), and `FileType` (WalFile/TableFile/...) overlap conceptually. `EntryType` and `ValueType` are nearly the same set of variants under different names; the user has to learn which one each API takes. `CompactionFilter::filter` takes `EntryType`; the WAL replay path uses `ValueType`; both come from the same logical operation.
- **Suggested fix**: Audit whether `EntryType` and `ValueType` can be unified. If not, document the difference clearly. At minimum, name them consistently — e.g. `UserEntryKind` vs `InternalEntryTag`.
- **Effort**: small

### A-65. `ColumnFamilyHandle::name()` returns `&str` but the lifetime is undocumented

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/api/db.rs:44-50
- **Category**: api-design / abi
- **Description**: `fn name(&self) -> &str` ties the slice to `&self`. If a user wants to keep the name after the handle is dropped, they must clone it. This is fine in Rust but awkward to mirror in C (C can't easily express "borrow tied to handle lifetime"). The doc says "Stable for the lifetime of the CF" which conflicts with `&self` (suggests "stable for the lifetime of self").
- **Suggested fix**: Either return `&'static str` (require all CF names to be 'static, which they aren't), return a cloned `String`, or change the doc to say "valid while the handle is held". For the C ABI, plan to return a callback-allocated or copied string.
- **Effort**: trivial

### A-66. `Cache::insert` returns `Arc<dyn CacheHandle>` — release-on-drop is implicit and undocumented

- **Severity**: MEDIUM
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/ext/cache.rs:53-86
- **Category**: api-design / abi
- **Description**: The `Cache` trait doc says "Upstream uses `void*` + manual `Release`; in Rust we use a refcounted handle whose `Drop` implementation releases the entry." But `Drop` is not on the trait — there's no way to enforce that an implementor of `CacheHandle` does the right thing. A buggy impl that forgets to release on drop is silently wrong. For the C ABI, this drop-on-release semantics is hostile (C must call a destructor explicitly).
- **Suggested fix**: Either add an explicit `release(&self)` method on `CacheHandle` (uglier but ABI-friendly), or document that the `Drop` requirement is part of the trait contract and add a doctest verifying it.
- **Effort**: small

### A-67. `Comparator` is required to be `Send + Sync` but `BytewiseComparator` is unit-struct `Default + Copy` — boxing is forced

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/ext/comparator.rs:36, /Users/yangjie01/SourceCode/git/st-rs/src/ext/comparator.rs:70-71
- **Category**: api-design
- **Description**: Every engine consumer takes `Arc<dyn Comparator>`. `BytewiseComparator` is zero-sized. A user passing the default comparator pays one allocation + one Arc bump for every CF. There's no static-dispatch path. For a future C-ABI mirror this is fine, but for Rust performance it's an unnecessary cost.
- **Suggested fix**: Make the engine generic over `Cmp: Comparator` in addition to (or instead of) trait-object form. For backward compat, default to `BytewiseComparator`. Or provide an enum specialization for the default comparator that avoids the dyn dispatch.
- **Effort**: medium

### A-68. `unsafe_code` is denied crate-wide — this rules out efficient zero-copy iterators for the FFI layer

- **Severity**: MEDIUM
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/lib.rs:62
- **Category**: abi
- **Description**: `#![deny(unsafe_code)]` is a strong correctness signal but it precludes the streaming iterator approach (which needs `unsafe` to escape self-referential lifetime issues, as the eager-iterator doc admits — see A-49). It also makes the future C ABI crate awkward: every `extern "C"` function is unsafe, every JNI helper is unsafe. The deny will have to be relaxed sooner rather than later.
- **Suggested fix**: Plan the unsafe boundary now. Either (a) keep `deny(unsafe_code)` on the high-level crate and put streaming iterator + FFI in a separate sub-crate where unsafe is allowed, or (b) downgrade to `warn(unsafe_code)` and audit each block. Don't paint yourself into a corner.
- **Effort**: medium

### A-69. The `PinnableSlice` upstream abstraction is documented as omitted but no equivalent is offered

- **Severity**: MEDIUM
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/core/slice.rs:26-30
- **Category**: api-design
- **Description**: The slice module doc explicitly omits `PinnableSlice`, noting "it belongs in the cache layer (Layer 2), not Layer 0." But the cache layer doesn't define an equivalent. `Db::get` returns `Vec<u8>` — every read is an allocation + memcpy. For a state backend doing tight read loops (Flink's bread and butter), this is a significant per-op overhead. The C ABI plan will inherit `Vec<u8>` shape.
- **Suggested fix**: Define and ship a `PinnedValue` type (e.g. `Arc<dyn CacheHandle>` wrapper or a borrowed handle with explicit lifetime). Add `Db::get_pinned(...) -> Result<Option<PinnedValue>>`. Match upstream's pattern.
- **Effort**: medium

### A-70. The crate's `monitoring`, `logging`, `port`, `memory` modules are public but mostly empty as user-facing API

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/lib.rs:71-78
- **Category**: module-layering
- **Description**: `pub mod monitoring`, `pub mod logging`, `pub mod port`, `pub mod memory` are declared but only `monitoring::histogram`, `monitoring::statistics`, `logging::logger`, `memory::arena`, `memory::allocator` are re-exported. A user opening rustdoc sees the module list with mostly internal types. `port` in particular has no user-facing exports.
- **Suggested fix**: Make these `pub(crate) mod` unless the user genuinely needs to override a type from them. Re-export the few user-facing types from `api::` to consolidate the public surface.
- **Effort**: small

### A-71. `parking_lot` is a hidden runtime dependency exposed indirectly through `WriteCoord`

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/db/db_impl.rs:354-380
- **Category**: abi
- **Description**: `DbImpl` uses `parking_lot::Mutex`/`Condvar` internally. This isn't part of the public API today, but if the engine is ever wrapped in a C ABI, `parking_lot`'s linkage requirements (static lib) become an integration concern. The choice should be intentional and documented in the architecture, not buried in db_impl.
- **Suggested fix**: Document the `parking_lot` dependency at the crate level and the rationale (group-commit hot path needs sub-microsecond mutex). If a feature flag for `std::sync` fallback would help downstream packagers, add one.
- **Effort**: trivial

### A-72. `DbImpl::open_with_fs` accepts custom `FileSystem` but not custom `Env` / `Logger` / `ThreadPool` / `Statistics`

- **Severity**: MEDIUM
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/db/db_impl.rs:478
- **Category**: api-design
- **Description**: `open_with_fs` is the escape hatch for custom backends, but it only takes a `FileSystem`. To inject a custom thread pool, clock, or logger, a user has no entry point. They must wait for the equivalent of `open_with_env`. Today the only configurable axis is the FS; this is asymmetric.
- **Suggested fix**: Either accept the full `Env` (with FS + clock + thread pool) or make every overridable knob a field on `DbOptions` (A-44). The current single-axis override is a half-measure.
- **Effort**: small

### A-73. `pick_compaction`, `pick_compaction_batch`, `CompactionJob` are crate-root exports

- **Severity**: HIGH
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/lib.rs:183
- **Category**: abi
- **Description**: The compaction planner is exposed at the crate root. The compaction algorithm is a deep internal that users should not depend on. Any improvement (multi-level, leveled vs universal, subcompactions, dynamic level bytes) breaks the public surface.
- **Suggested fix**: Make these `pub(crate)`. Users who want to influence compaction should do so via `ColumnFamilyOptions` (compaction style, level0_file_num_compaction_trigger, ...).
- **Effort**: trivial

### A-74. `BinaryHeap` from `util::heap` is re-exported at the crate root

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/lib.rs:190
- **Category**: api-design
- **Description**: `BinaryHeap` is a generic utility, but `std::collections::BinaryHeap` already exists. Re-exporting a custom one at the crate root with the same name creates a name collision in user code that does `use st_rs::*` after `use std::collections::*`.
- **Suggested fix**: Rename (e.g. `MergeHeap`) and/or remove from the crate root.
- **Effort**: trivial

### A-75. `FsRandomAccessFile`, `FsSequentialFile`, `FsWritableFile`, `FsDirectory` trait names use `Fs` prefix; `FileLock` does not

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/env/file_system.rs
- **Category**: naming
- **Description**: The file-system trait family uses `Fs*` prefix for files (`FsRandomAccessFile`, `FsSequentialFile`, `FsWritableFile`, `FsDirectory`) but `FileLock` and `FileAttributes` and `FileSystem` do not. The prefix rule is inconsistent.
- **Suggested fix**: Pick one. Either drop the prefix everywhere (`RandomAccessFile`, `SequentialFile`, `WritableFile`, `Directory`, `Lock`, etc. — the namespace `env::file_system::` disambiguates), or use it everywhere (`FsFileLock`, `FsFileAttributes`).
- **Effort**: small

### A-76. `LRUCache` named `LruCache` (Rust-idiomatic) while `BloomFilterPolicy` keeps acronym uppercase pattern inconsistently

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/lib.rs:128, /Users/yangjie01/SourceCode/git/st-rs/src/lib.rs:166
- **Category**: naming
- **Description**: Mix of acronym casing: `LruCache` (Rust-style) vs `BloomFilterPolicy` (full words). `IoStatus`, `IoOptions`, `IoPriority`, `IoType` use Rust-style. `CompressionType::Lz4`, `Lz4hc` mix capitalization. Generally fine but spot-check for consistency.
- **Suggested fix**: Apply rustc's standard rule: acronym >2 chars becomes mixed case (`Lru`, `Crc32c`, `XxHash`). Audit and align.
- **Effort**: trivial

### A-77. `ColumnFamilyHandleImpl` is publicly visible from `db_impl` and re-exported, exposing its `id`/`name` private fields conceptually

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/db/db_impl.rs:164, /Users/yangjie01/SourceCode/git/st-rs/src/lib.rs:176
- **Category**: api-design
- **Description**: `ColumnFamilyHandleImpl` is the concrete impl of `ColumnFamilyHandle`. It's exposed at the crate root. Users may pattern-match against the concrete type rather than coding against the trait, locking the engine into shipping this name forever.
- **Suggested fix**: Either rename to `DbColumnFamilyHandle` for consistency with `DbSnapshot`, or make `pub(crate)` and require users to operate on `Arc<dyn ColumnFamilyHandle>`.
- **Effort**: trivial

### A-78. `LogLevel` (logging) and `InfoLogLevel` (options) — two enums for the same concept

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/lib.rs:119, /Users/yangjie01/SourceCode/git/st-rs/src/api/options.rs:162-172
- **Category**: naming
- **Description**: `LogLevel` (from `logging::logger`) and `InfoLogLevel` (from `api::options`) are both about log levels. Both are re-exported. The relationship and the difference are unclear from the names.
- **Suggested fix**: Use one type. If they truly differ (`Logger` filter vs `DbOptions::info_log_level`), document why; otherwise consolidate.
- **Effort**: trivial

### A-79. Trait method calls in `Db` always take `&self`; there is no `&mut self` variant — implementers must use interior mutability

- **Severity**: MEDIUM
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/api/db.rs:81
- **Category**: api-design
- **Description**: The trait commits to `fn put(&self, ...)` — meaning every implementation must use interior mutability (RwLock, Mutex, atomics). This is right for the concurrent-engine model but rules out single-threaded engines, mock impls that just want a HashMap, or zero-cost test doubles. The decision is correct for the production case but worth being explicit.
- **Suggested fix**: Document the rationale on the trait. Consider providing a `SingleThreaded Db` companion trait if mock backends are valuable. Most likely outcome: leave as-is but document.
- **Effort**: trivial

### A-80. No `Send + Sync` bound is documented for `WriteBatch`, but `Db` implementations are required to accept it from any thread

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/api/write_batch.rs:48-55
- **Category**: api-design
- **Description**: `WriteBatch` happens to be `Send + Sync` because all its fields are, but the trait contract doesn't say so. A future change adding `Rc<...>` would silently break the `Db` trait impl pattern that submits the batch to a worker thread (e.g. group commit).
- **Suggested fix**: Add explicit `static_assertions::assert_impl_all!(WriteBatch: Send, Sync);` or document the requirement in rustdoc.
- **Effort**: trivial

### A-81. `Db::flush_wal(sync: bool)` — a bool flag where two methods would read better

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/api/db.rs:198
- **Category**: api-design
- **Description**: `flush_wal(sync: bool)` — a boolean flag. At call sites `flush_wal(true)` vs `flush_wal(false)` reads opaquely. Upstream uses `FlushWAL(true)` for the same reason but Rust idioms prefer either named methods or an enum.
- **Suggested fix**: Replace with `flush_wal()` and `sync_wal()` (or accept a `FlushWalOptions { sync: bool }`).
- **Effort**: trivial

### A-82. `DbImpl::path()`, `DbImpl::db_path()` — two methods returning the same thing

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/db/db_impl.rs:821, /Users/yangjie01/SourceCode/git/st-rs/src/db/db_impl.rs:974
- **Category**: naming
- **Description**: `pub fn db_path(&self) -> &Path` and `pub fn path(&self) -> &Path` both return the DB directory. Duplicate API.
- **Suggested fix**: Pick one. Delete the other.
- **Effort**: trivial

### A-83. `validate_sst` is re-exported via `db::ingest_external_file::validate_sst` — module name is overlong and not user-friendly

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/lib.rs:199
- **Category**: naming / module-layering
- **Description**: The "SST ingestion" section re-exports `IngestExternalFileOptions` and `validate_sst` from `db::ingest_external_file`. The module name is verbose; `db` should not be a user-facing namespace (see A-30).
- **Suggested fix**: Move `IngestExternalFileOptions` to `api::options` (with the other options) and `validate_sst` to `utilities::ingest` or a similar user-facing module.
- **Effort**: small

### A-84. `Db` trait lacks `try_clone` / no documented multi-process or read-only mode story

- **Severity**: MEDIUM
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/api/db.rs:81-202
- **Category**: api-design
- **Description**: The trait does not provide an obvious "open read-only" path. `DbOpener::open_read_only` exists but the trait `Db` has no associated method to query "am I read-only?" — so writes will fail at runtime rather than at compile time. The Stage-2 C ABI will need a clear answer.
- **Suggested fix**: Either return a different trait (e.g. `ReadOnlyDb`) from `open_read_only`, or add an `is_read_only(&self) -> bool` method and have writes return `Status::not_supported`. Right now the gap is hidden.
- **Effort**: medium

### A-85. There is no `Db::set_options` / runtime reconfiguration — but upstream allows it

- **Severity**: MEDIUM
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/api/db.rs:81-202
- **Category**: extensibility
- **Description**: Upstream RocksDB allows dynamic reconfiguration via `SetOptions`. The Rust trait has no such method. The `ColumnFamilyOptions` doc at line 180 even says "These can be changed per-column-family at open time and partially at runtime via `Db::set_options`" — but no such method exists. Documentation hallucinates an API.
- **Suggested fix**: Either implement `set_options` (and decide which options are dynamically mutable) or remove the doc claim. Don't promise APIs that don't exist.
- **Effort**: medium

### A-86. `CompactionFilter`, `MergeOperator`, `Comparator`, `FilterPolicy` all use `&'static str` for name — fine for Rust, problematic for FFI

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/ext/comparator.rs:40, /Users/yangjie01/SourceCode/git/st-rs/src/ext/merge_operator.rs:34
- **Category**: abi
- **Description**: Every extension trait returns `&'static str` for its name. For a C ABI mirror, `&'static str` is fine to expose as `*const c_char` but only because Rust string literals happen to be null-terminated in practice — they aren't guaranteed to be. The C ABI will need wrapper logic to materialise a null-terminated C string.
- **Suggested fix**: Plan a `name() -> &'static CStr` (or document that the C ABI will use a callback that materializes the name). Decide before exposing the trait through FFI.
- **Effort**: small

### A-87. `Db` trait has no observer/listener API — events (flush completed, compaction completed) are unobservable

- **Severity**: MEDIUM
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/api/db.rs:81-202
- **Category**: extensibility
- **Description**: Upstream has `EventListener` with hooks for flush/compaction begin/end, ingestion, errors. The Rust port has nothing comparable. Flink needs these for its checkpoint coordinator. The omission is a structural gap that will be painful to add later (every internal flush/compaction path will need to call out to a listener registry).
- **Suggested fix**: Define an `EventListener` trait now (even if no events fire today) and add `DbOptions::listeners: Vec<Arc<dyn EventListener>>`. Easier to thread through now than to retrofit.
- **Effort**: medium

### A-88. Re-export list duplicates "Layer 4a" `LiveFileMetaData` — should live with options or a metadata module

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/lib.rs:176, /Users/yangjie01/SourceCode/git/st-rs/src/db/db_impl.rs:313-328
- **Category**: module-layering
- **Description**: `LiveFileMetaData` is defined inside `db_impl.rs` (a 4000+ line file) but is a public type. It logically belongs with other metadata types (CF info, SST info, level stats).
- **Suggested fix**: Move `LiveFileMetaData` to `api::metadata` (a new module) along with any other "describe-the-engine" types. Keep `db_impl.rs` focused on the engine impl.
- **Effort**: trivial

### A-89. `db_impl.rs` is ~4100 lines — over 5x the project's own 800-line max guideline

- **Severity**: HIGH
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/db/db_impl.rs
- **Category**: module-layering
- **Description**: The project's coding-style rule states "200-400 lines typical, 800 max." `db_impl.rs` is ~4100 lines (per grep landmarks). It mixes engine state, write path, read path, flush, compaction, ingest, snapshot, group-commit, and the `Db` trait impl. Code review and refactoring are correspondingly harder.
- **Suggested fix**: Decompose. Suggested split: `db_impl/state.rs`, `db_impl/write_path.rs` (incl. group commit), `db_impl/read_path.rs`, `db_impl/flush.rs`, `db_impl/compaction.rs`, `db_impl/snapshot.rs`, `db_impl/open.rs`, `db_impl/close.rs`, `db_impl/trait_impl.rs`. Each well under 800 lines.
- **Effort**: large

### A-90. `Db` is exported but `DbOpener` is not implemented by `DbImpl` — there is no canonical "open" path in the public API

- **Severity**: HIGH
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/api/db.rs:209, /Users/yangjie01/SourceCode/git/st-rs/src/db/db_impl.rs:468
- **Category**: api-design
- **Description**: A user wanting "I have a `dyn Db`" has no factory. They must reach for `DbImpl::open(...)`, getting an `Arc<DbImpl>`, then coerce to `Arc<dyn Db>`. The trait surface is incomplete on the open path.
- **Suggested fix**: Add a free function `open(opts, path) -> Result<Arc<dyn Db>>` to `api::db` (and `open_cf`, `open_read_only`). Internally it constructs the default engine. Users coding against the trait have a canonical entry point.
- **Effort**: small

### A-91. Snapshot trait imports `crate::db::Db::snapshot` in its doc, but the path is wrong

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/api/snapshot.rs:18
- **Category**: doc-coverage
- **Description**: The doc comment says `[crate::db::Db::snapshot]` but `Db` is in `crate::api::db`, not `crate::db`. The intra-doc link will fail to resolve in rustdoc.
- **Suggested fix**: Fix to `[crate::api::db::Db::snapshot]`. Run `cargo doc` in CI to catch broken intra-doc links.
- **Effort**: trivial

### A-92. The crate's `api` module is fine-grained (db / iterator / options / snapshot / write_batch) but the docs say "user-facing" — missing a high-level overview

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/api/mod.rs
- **Category**: doc-coverage
- **Description**: `api::mod` doc is two sentences. There's no end-to-end example showing how the user-facing types work together (open DB → create CF → put/get → snapshot → iterate). For new users, the API surface is opaque.
- **Suggested fix**: Add a substantial doc to `api::mod.rs` with a worked end-to-end example. Same for the crate root `lib.rs` — current doc is mostly a layer-by-layer table.
- **Effort**: small

### A-93. `DbImpl::set_merge_operator` and `set_compaction_filter_factory` mutate engine state through `&self` — but are not on the `Db` trait

- **Severity**: MEDIUM
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/db/db_impl.rs:1132, /Users/yangjie01/SourceCode/git/st-rs/src/db/db_impl.rs:1266
- **Category**: api-design
- **Description**: Two engine setters live on the inherent impl only. They are the only way to wire a custom merge operator (the name-based path in options resolves only `StringAppendOperator`). So in practice the user must use the concrete `DbImpl`, not `dyn Db`. This is another instance of the dual-API problem (A-1) but specifically blocking extension-point usage.
- **Suggested fix**: Either lift these onto the `Db` trait, or (preferred) make `ColumnFamilyOptions` accept `Arc<dyn MergeOperator>` / `Arc<dyn CompactionFilterFactory>` directly (A-43) so the setters become unnecessary.
- **Effort**: small

### A-94. No public versioning policy — `block_cache_size`, WAL block size, and SST magic number can all be silently changed

- **Severity**: HIGH
- **File or area**: cross-cutting — /Users/yangjie01/SourceCode/git/st-rs/src/sst/format.rs:71, /Users/yangjie01/SourceCode/git/st-rs/src/db/log_format.rs
- **Category**: abi
- **Description**: The crate is at 0.1.x and breaking changes are "acceptable." But the on-disk format (`BLOCK_BASED_TABLE_MAGIC_NUMBER`, `BLOCK_TRAILER_SIZE`, `WAL_BLOCK_SIZE`) is also re-exported as `pub` — meaning consumers may write code depending on these values. There's no `--feature on-disk-stable-vN` gating. A future SemVer 1.0 cannot meaningfully promise on-disk stability while these magic numbers are part of the public Rust API.
- **Suggested fix**: Split on-disk versioning from Rust API versioning. Make on-disk constants `pub(crate)` and expose them only through a `FormatVersion` enum with explicit upgrade/downgrade semantics. Document the on-disk stability promise (or non-promise) in `README.md`.
- **Effort**: small

### A-95. `Db` trait does not expose `wait_for_pending_work` / `flush_all_cfs` — concurrency primitives are escape-hatch only

- **Severity**: MEDIUM
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/db/db_impl.rs:2484, /Users/yangjie01/SourceCode/git/st-rs/src/db/db_impl.rs:1651
- **Category**: api-design
- **Description**: Background-work synchronisation primitives (`wait_for_pending_work`, `wait_for_pending_flush`, `flush_all_cfs`, `schedule_flush`, `schedule_compaction`) are inherent-impl only. Any future test framework or alternative backend will need them and discover they're not portable.
- **Suggested fix**: Decide which background-work primitives are part of the contract (likely `wait_for_pending_work` and `flush_all_cfs`) and add them to the trait. Hide the rest as `pub(crate)`.
- **Effort**: small

### A-96. There is no observable test for "trait surface == inherent surface" — the divergence in Findings A-1, A-2, A-3, A-4, A-13 was discoverable only by reading the code

- **Severity**: MEDIUM
- **File or area**: cross-cutting
- **Category**: api-design
- **Description**: The dual-API is not enforced by any test that verifies "every method on `Db` does what its `DbImpl` inherent counterpart does, with the options honored." Bugs like A-2 (iterator returns empty) and A-4 (single_delete becomes delete) slipped through because nothing exercises the trait through `&dyn Db`.
- **Suggested fix**: Add a test suite that opens a `DbImpl`, casts to `Arc<dyn Db>`, and runs the same operations through both surfaces — asserting bytes-equal results. This will surface every silent divergence at CI time.
- **Effort**: medium

### A-97. `Status` derives `Clone` and contains `Option<String>` — clones allocate

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/core/status.rs:137-153
- **Category**: api-design / abi
- **Description**: Every error path that propagates `Status` clones the message string. Upstream uses a `(char* state, size_t)` packed pointer that lives on the heap until released. For high-error-rate paths (NotFound during point lookups, e.g.) the allocation cost is non-trivial. The Rust port's choice of `Option<String>` makes this worse, not better.
- **Suggested fix**: Use `Option<Box<str>>` (saves the `String` capacity slot) or `Option<Arc<str>>` (zero-cost clone). The latter matches upstream's intent.
- **Effort**: trivial

### A-98. No documentation of "what the C ABI will look like" — design choices today lock in tomorrow's FFI shape

- **Severity**: MEDIUM
- **File or area**: cross-cutting
- **Category**: doc-coverage
- **Description**: The Stage-2 C ABI is mentioned in the task prompt but there's no `STAGE2-CAPI.md` or equivalent in the repo. Many of the findings above (panicking iterators, `Drop`-based snapshot release, `&'static str` names, `&[u8]` slice lifetimes) become much higher-priority if the C ABI is imminent. The team should be making API decisions with the FFI shape in mind.
- **Suggested fix**: Write a brief design doc for the C ABI: which types cross the boundary, which traits are exposed as opaque pointers, how lifetimes are handled (callback-allocated buffers? user-provided pinning?), how errors flow (status struct, error code only?). Use it to drive the prioritisation of the findings above.
- **Effort**: medium

### A-99. The `Send + Sync` story is documented inconsistently across traits

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/api/db.rs:81, /Users/yangjie01/SourceCode/git/st-rs/src/api/iterator.rs:41
- **Category**: doc-coverage
- **Description**: `Db: Send + Sync`. `Snapshot: Send + Sync`. `ColumnFamilyHandle: Send + Sync`. `Comparator: Send + Sync`. `MergeOperator: Send + Sync`. `Cache: Send + Sync`. `FilterPolicy: Send + Sync`. But `DbIterator: Send` (only Send, not Sync — documented as "the upstream contract is 'not safe for concurrent use from multiple threads.'"). `WriteBatch` and `WriteBatchHandler` have no explicit bounds. The rationale is correct but the rule isn't uniform and isn't documented in one place.
- **Suggested fix**: Add a section to `api::mod` documenting the threading model: which trait objects can be shared, which can only be moved, which are single-threaded. Audit and align all extension traits.
- **Effort**: small

### A-100. The `utilities` module has only `checkpoint` — a natural home for Stage-2 user features but nearly empty

- **Severity**: LOW
- **File or area**: /Users/yangjie01/SourceCode/git/st-rs/src/utilities/mod.rs
- **Category**: module-layering
- **Description**: `utilities::checkpoint` is the only submodule. Upstream's `utilities/` has dozens (transactions, TTL, backup engine, write-batch-with-index, ...). This isn't a bug today, but as Stage 2 adds features, the question of "is this user-facing utility or engine internal?" needs to be settled. Right now `flink::state_backend` (a user-facing utility) lives in `flink/`, not `utilities/`.
- **Suggested fix**: Document the rule for what goes in `utilities/` vs `flink/` vs `api/` vs `ext/`. Move `FlinkStateBackend` to `utilities/flink/` (A-34) once the rule is set.
- **Effort**: small

---

Relevant files for this review:

- /Users/yangjie01/SourceCode/git/st-rs/src/lib.rs
- /Users/yangjie01/SourceCode/git/st-rs/src/api/mod.rs
- /Users/yangjie01/SourceCode/git/st-rs/src/api/db.rs
- /Users/yangjie01/SourceCode/git/st-rs/src/api/options.rs
- /Users/yangjie01/SourceCode/git/st-rs/src/api/iterator.rs
- /Users/yangjie01/SourceCode/git/st-rs/src/api/write_batch.rs
- /Users/yangjie01/SourceCode/git/st-rs/src/api/snapshot.rs
- /Users/yangjie01/SourceCode/git/st-rs/src/core/status.rs
- /Users/yangjie01/SourceCode/git/st-rs/src/core/slice.rs
- /Users/yangjie01/SourceCode/git/st-rs/src/core/types.rs
- /Users/yangjie01/SourceCode/git/st-rs/src/ext/comparator.rs
- /Users/yangjie01/SourceCode/git/st-rs/src/ext/cache.rs
- /Users/yangjie01/SourceCode/git/st-rs/src/ext/merge_operator.rs
- /Users/yangjie01/SourceCode/git/st-rs/src/ext/table.rs
- /Users/yangjie01/SourceCode/git/st-rs/src/env/env_trait.rs
- /Users/yangjie01/SourceCode/git/st-rs/src/env/file_system.rs
- /Users/yangjie01/SourceCode/git/st-rs/src/db/db_impl.rs
- /Users/yangjie01/SourceCode/git/st-rs/src/db/db_iter.rs
- /Users/yangjie01/SourceCode/git/st-rs/src/db/mod.rs
- /Users/yangjie01/SourceCode/git/st-rs/src/db/dbformat.rs
- /Users/yangjie01/SourceCode/git/st-rs/src/db/ingest_external_file.rs
- /Users/yangjie01/SourceCode/git/st-rs/src/flink/mod.rs
- /Users/yangjie01/SourceCode/git/st-rs/src/flink/state_backend.rs
- /Users/yangjie01/SourceCode/git/st-rs/src/utilities/checkpoint.rs
