# Layer 2 Rust Port — Implementation Record

This document records the Layer 2 addition to the `st-rs` crate. Layer 2 is where abstract traits from Layer 0 get their first concrete implementations: POSIX-backed filesystem I/O, buffered file wrappers, filename conventions, a thread pool, and a single-shard LRU block cache.

Companion docs:
- `LAYER0-PORT.md` — Layer 0 (public API + platform shim)
- `LAYER1-PORT.md` — Layer 1 (foundation utilities + src/ reorg)
- `ForSt/FORST-READING-ORDER.md` — the layered reading plan

## What landed

Layer 2 adds 11 new source files across three module groups:

```
src/
├── env/
│   ├── posix.rs                        ← NEW  PosixFileSystem + per-file types (Unix-gated)
│   └── thread_pool.rs                  ← NEW  StdThreadPool with per-priority buckets
├── file/                               ← NEW  top-level subsystem
│   ├── mod.rs
│   ├── filename.rs                     ← RocksDB filename parsing / construction
│   ├── writable_file_writer.rs         ← buffered writer over FsWritableFile
│   ├── random_access_file_reader.rs    ← buffered reader over FsRandomAccessFile
│   ├── sequence_file_reader.rs         ← buffered reader over FsSequentialFile
│   └── file_util.rs                    ← copy_file, sync_directory, delete_if_exists
└── cache/                              ← NEW  top-level subsystem
    ├── mod.rs
    └── lru.rs                          ← single-shard LRU implementing the Cache trait
```

Plus updated re-exports in `lib.rs` and a new bullet in the layering table at the top.

## Module-by-module summary

### `env/posix.rs` — POSIX `FileSystem` implementation

| Type | Implements | Backed by |
|---|---|---|
| `PosixFileSystem` | `FileSystem` | `std::fs`, `std::os::unix::fs::FileExt` |
| `PosixSequentialFile` | `FsSequentialFile` | `std::fs::File` + `Read`/`Seek` |
| `PosixRandomAccessFile` | `FsRandomAccessFile` | `FileExt::read_at` (pread, `&File`, naturally `Sync`) |
| `PosixWritableFile` | `FsWritableFile` | `std::fs::File` + `Write`/`sync_data`/`sync_all` |
| `PosixDirectory` | `FsDirectory` | `std::fs::File::sync_all` on a directory fd |
| `PosixFileLock` | `FileLock` | atomic `OpenOptions::create_new(true)` + Drop cleanup |

- **Unix-gated**: the whole module is `#[cfg(unix)]` inside `env/mod.rs`. Windows support requires `std::os::windows::fs::FileExt::seek_read` and is a follow-up.
- **Concurrent reads** on `PosixRandomAccessFile` require no lock because `FileExt::read_at` takes `&File`.
- **Error mapping**: every `std::io::Error` runs through `io_status(&path, err)` which tags the `Status` with the file path and maps `ErrorKind::NotFound` to `SubCode::PathNotFound`.
- **Lock semantics**: `lock_file` is implemented with `OpenOptions::create_new(true)` — atomic across processes, the Drop impl removes the file. **Not robust against crashes**: if the owner dies without running Drop, the stale LOCK file blocks future opens. Real RocksDB uses `fcntl`; a follow-up can swap in `rustix::fs::flock`.

### `env/thread_pool.rs` — `StdThreadPool`

- Four priority buckets (`Low`, `High`, `User`, `Bottom`), each an independent `mpsc` channel + worker thread group.
- `schedule` assigns a monotonic `ScheduleHandle` id and pushes `(id, task)` into the bucket's channel.
- `cancel` is best-effort: it removes the id from a `HashSet<ScheduleHandle>` that workers consult before running. Already-started tasks always finish.
- Panics inside tasks are caught via `std::panic::catch_unwind(AssertUnwindSafe(task))` so one bad task doesn't kill the worker.
- `set_background_threads` grows by spawning new workers, shrinks lazily by sending poison pills.
- `wait_for_jobs_and_join_all` is terminal — the pool cannot be used after calling it.

### `file/filename.rs` — RocksDB filename conventions

Pure functions, no I/O. Covers the full set from upstream's `filename.cc`:

| File kind | `make_*` function | Pattern |
|---|---|---|
| WAL | `make_wal_file_name` | `{dir}/{number:06}.log` |
| SST | `make_table_file_name` | `{dir}/{number:06}.sst` |
| Blob | `make_blob_file_name` | `{dir}/{number:06}.blob` |
| Manifest | `make_descriptor_file_name` | `{dir}/MANIFEST-{number:06}` |
| Options | `make_options_file_name` | `{dir}/OPTIONS-{number:06}` |
| Current | `make_current_file_name` | `{dir}/CURRENT` |
| Lock | `make_lock_file_name` | `{dir}/LOCK` |
| Identity | `make_identity_file_name` | `{dir}/IDENTITY` |
| Info log | `make_info_log_file_name` | `{dir}/LOG` |
| Rolled info log | `make_old_info_log_file_name` | `{dir}/LOG.old.{ts_micros}` |
| Temp | `make_temp_file_name` | `{dir}/{number:06}.dbtmp` |

`parse_file_name` reverses the process, returning `(FileType, u64)` for any recognised name and an `InvalidArgument` status otherwise. Round-trip tested.

### `file/writable_file_writer.rs` — `WritableFileWriter`

A buffered wrapper over `Box<dyn FsWritableFile>`. Coalesces small `append` calls into a single underlying write and exposes `flush`/`sync`/`fsync`/`close`. Oversized writes (larger than the buffer) bypass the buffer and go straight to the file.

**Scope trimmed from upstream:**
- No rate-limiter hook. Upstream calls into `RateLimiter` before every flush.
- No checksum hook. Upstream optionally emits a `DataVerificationInfo` with a per-block CRC32C. Deferred until `util::hash` is wire-compatible.
- No direct I/O / `O_DIRECT` support. Requires aligned buffers the upstream tracks via `AlignedBuffer`; deferred.

### `file/random_access_file_reader.rs` — `RandomAccessFileReader`

Buffered reader over `Box<dyn FsRandomAccessFile>`. Maintains a single readahead window (default 256 KiB) guarded by a `Mutex`. Reads that land inside the window are served from memory; misses refill the window centred on the request offset.

**Simplified vs upstream:**
- Upstream has a separate `FilePrefetchBuffer` with exponentially-growing windows and async prefetch. The Layer 2 port is just a single fixed-size last-window buffer.
- No checksum verification (same reason as the writer).
- Requests larger than the readahead window bypass the buffer and go straight to the file.

### `file/sequence_file_reader.rs` — `SequentialFileReader`

Buffered wrapper over `Box<dyn FsSequentialFile>`. Used by WAL replay and MANIFEST scanning. Internal 32 KiB buffer that refills on demand; `skip` drains the buffer first then delegates to the underlying file.

### `file/file_util.rs` — helpers

Three top-level helpers layered on `FileSystem`:

- `copy_file(fs, src, dst)` — reads `src` through a `SequentialFileReader`, writes `dst` through a `WritableFileWriter`, 64 KiB buffer.
- `sync_directory(fs, dir)` — opens the directory and `fsync`s it (required on some filesystems for durability of just-renamed files).
- `delete_if_exists(fs, path)` — swallows `NotFound`, propagates everything else.

Tests here exercise the full `PosixFileSystem` path end-to-end under `#[cfg(unix)]`.

### `cache/lru.rs` — `LruCache`

Single-shard LRU implementing the Layer 0 `Cache` trait. Uses the **generational-index linked list** pattern — nodes live in `Vec<Option<Node>>`, linked by `Option<usize>` indices, which lets us write a classic doubly-linked list without `Rc<RefCell>` or unsafe.

**Features:**
- `insert` replaces existing entries with the same key.
- Entries larger than capacity are rejected with `InvalidArgument`.
- `lookup` moves the entry to the MRU position.
- `erase` removes a specific key.
- `set_capacity` shrinks or grows the cache; shrinking triggers immediate eviction.
- **Handle outlives eviction**: `Arc<Vec<u8>>` backs each entry, so a user holding an `Arc<dyn CacheHandle>` from an earlier lookup continues to see valid bytes after the entry is evicted from the cache.

**Out of scope (deferred to Layer 2b):**
- Sharded cache wrapper. Production RocksDB uses 64 shards with FNV-based key distribution — shape is identical, we'll add a `ShardedCache<C>` that wraps N `LruCache` instances.
- HyperClockCache. Complex; LRU proves the trait.
- Secondary cache (compressed L2). Deferred.
- Per-entry reference counting for eviction. The simple LRU just evicts strictly by recency; `erase_unreferenced` is a no-op.

## Verification

| Check | Command | Result |
|---|---|---|
| Clean build | `cargo build` | ✅ 0 warnings, 0 errors |
| Clippy (all targets, `-D warnings`) | `cargo clippy --all-targets -- -D warnings` | ✅ clean |
| Unit tests | `cargo test` | ✅ **122 passed**, 1 ignored doc-test |
| No unsafe | `#![deny(unsafe_code)]` crate-wide | ✅ enforced |
| Every public item documented | `#![warn(missing_docs)]` | ✅ enforced |
| Zero external dependencies | empty `[dependencies]` | ✅ still std-only |

### Test count by layer

| Layer | Tests |
|---|--:|
| Layer 0 (unchanged) | 28 |
| Layer 1 (unchanged) | 52 |
| **Layer 2 (new)** | **42** |
| **Total** | **122** |

### Layer 2 test breakdown

| Module | Tests | What they verify |
|---|--:|---|
| `env::posix` | 7 | Write/read round-trip; sequential read + EOF; `get_children`; rename + delete; lock-file exclusivity; idempotent `create_dir_if_missing`; absolute path resolution |
| `env::thread_pool` | 5 | Tasks run on workers; cancel before start; priorities are independent queues; worker count adjusts; panicking task doesn't kill worker |
| `file::filename` | 7 | Zero-padding; `make_*` produce expected paths; parse numbered files; parse bare files; parse prefix files; reject junk; round-trip |
| `file::writable_file_writer` | 4 | Coalescing of small writes; oversized write bypasses buffer; `file_size` tracks logical writes; close flushes buffer |
| `file::random_access_file_reader` | 5 | Whole-file read; sequential reads return correct bytes across window refills; read past EOF returns 0; straddling EOF returns partial; oversized request bypasses window |
| `file::sequence_file_reader` | 3 | Whole-file read; multi-chunk read spans refills; `skip` drains buffered bytes then delegates |
| `file::file_util` | 2 | `copy_file` round-trip via `PosixFileSystem`; `delete_if_exists` is idempotent |
| `cache::lru` | 9 | Insert+lookup; missing lookup; eviction drops oldest; lookup bumps to front; erase; replace; `set_capacity` triggers eviction; handle outlives eviction; oversized insert rejected |

## Known simplifications

These are intentional deviations from upstream that a reader should not try to "fix":

1. **`PosixFileSystem` is Unix-only.** Windows support is a separate follow-up using `std::os::windows::fs::FileExt::seek_read`.
2. **`PosixFileLock` is a file-based advisory lock**, not `fcntl`/`flock`. Stale lock files persist across crashes until manually deleted. A Layer 2b follow-up can swap in `rustix::fs::flock`.
3. **No rate limiter / checksum / direct I/O hooks** in `WritableFileWriter` or `RandomAccessFileReader`. The trait shapes will accommodate them when their dependencies land.
4. **`StdThreadPool` is simple** — one mpsc channel per bucket, no work-stealing. Good enough for correctness tests; real production would want `rayon` or a `crossbeam::deque`-based implementation.
5. **`StdThreadPool::cancel` is best-effort.** Tasks already running cannot be cancelled (matches upstream).
6. **`LruCache` is single-shard.** Contention will hurt under concurrent load but correctness is the Layer 2 goal, not throughput.
7. **`LruCache::erase_unreferenced` is a no-op.** The simple LRU doesn't track per-entry reference counts from returned handles; eviction happens strictly on the recency list.
8. **`file/random_access_file_reader` uses a single-window readahead buffer**, not upstream's exponentially-growing `FilePrefetchBuffer`.

## Change log

- **v0.0.3** — Layer 2 landed. 11 new source files across `env/`, `file/`, `cache/`. 122 passing tests (28 Layer 0 + 52 Layer 1 + 42 Layer 2), clippy-clean, still zero external dependencies.
