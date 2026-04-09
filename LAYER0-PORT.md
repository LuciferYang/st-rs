# Layer 0 Rust Port — Implementation Record

This document records the initial implementation of the `st-rs` crate, which ports **Layer 0** (public API surface + platform shim) of the [ForSt](https://github.com/ververica/ForSt) project from C++ to Rust.

See the companion documents in the ForSt repo for context:
- `FORST-READING-GUIDE.md` — high-level project summary.
- `FORST-READING-ORDER.md` — the layered reading plan that defines what "Layer 0" means.

## Scope

**In scope** — every header file that a user of the ForSt C++ API is supposed to include, plus the tiny set of platform constants from `port/port.h`:

| Upstream file (`include/rocksdb/…`)    | Rust module               |
|----------------------------------------|---------------------------|
| `status.h`                             | `st_rs::status`           |
| `slice.h`                              | `st_rs::slice`            |
| `types.h`                              | `st_rs::types`            |
| `comparator.h`                         | `st_rs::comparator`       |
| `env.h`                                | `st_rs::env`              |
| `file_system.h`                        | `st_rs::file_system`      |
| `iterator.h`                           | `st_rs::iterator`         |
| `snapshot.h`                           | `st_rs::snapshot`         |
| `write_batch.h`                        | `st_rs::write_batch`      |
| `options.h`                            | `st_rs::options`          |
| `db.h`                                 | `st_rs::db`               |
| `cache.h`                              | `st_rs::cache`            |
| `table.h`                              | `st_rs::table`            |
| `filter_policy.h`                      | `st_rs::filter_policy`    |
| `merge_operator.h`                     | `st_rs::merge_operator`   |
| `compaction_filter.h`                  | `st_rs::compaction_filter`|
| `port/port.h`                          | `st_rs::port`             |

**Out of scope** — deliberately deferred to later layers so this crate stays a stable Layer 0 foundation:

- `PinnableSlice` (belongs with the block cache in Layer 2)
- `ImmutableDBOptions` / `MutableDBOptions` split (engine-internal, Layer 4)
- `AdvancedColumnFamilyOptions` (~100 engine knobs in `advanced_options.h`)
- `TableBuilder` / `TableReader` / `BlockBasedTableOptions` (Layer 3)
- `Env::NewLogger` / `Logger` trait (defer to logger crate)
- Transactions, BackupEngine, Checkpoint, BlobDB, TTL, WriteBatchWithIndex (Layer 5)
- Tools, stress, benchmarks (Layer 6)
- JNI bridge (Layer 7)

## File inventory

17 source files, 2,990 lines including doc comments and tests, 0 external dependencies.

```
st-rs/
├── Cargo.toml                 # pkg = "st-rs", crate = st_rs, edition 2021, rust 1.75
├── README.md                  # public-facing summary
├── LAYER0-PORT.md             # this file
└── src/
    ├── lib.rs                 # module declarations + top-level re-exports
    ├── status.rs              # Status, Code, SubCode, Severity, Result, IoStatus
    ├── slice.rs               # Slice = &[u8] alias + SliceExt trait
    ├── types.rs               # SequenceNumber, ColumnFamilyId, ValueType,
    │                          # EntryType, FileType, CompressionType,
    │                          # ChecksumType, Temperature, WriteStall*
    ├── comparator.rs          # Comparator trait, BytewiseComparator,
    │                          # ReverseBytewiseComparator
    ├── iterator.rs            # DbIterator trait, EmptyIterator, ErrorIterator
    ├── snapshot.rs            # Snapshot trait, SimpleSnapshot
    ├── write_batch.rs         # WriteBatch, Record, WriteBatchHandler
    ├── options.rs             # DbOptions, ColumnFamilyOptions, Options,
    │                          # ReadOptions, WriteOptions, FlushOptions,
    │                          # CompactionOptions, FileOptions, InfoLogLevel
    ├── env.rs                 # Env trait, Clock, ThreadPool, CompositeEnv,
    │                          # SystemClockImpl, Priority
    ├── file_system.rs         # FileSystem, FsSequentialFile,
    │                          # FsRandomAccessFile, FsWritableFile,
    │                          # FsDirectory, FileLock, IoOptions,
    │                          # IoType, AccessPattern, FileAttributes
    ├── db.rs                  # Db trait, ColumnFamilyHandle,
    │                          # ColumnFamilyDescriptor, DbOpener,
    │                          # OpenCfResult
    ├── cache.rs               # Cache trait, CacheHandle, CachePriority
    ├── table.rs               # TableFactory, TableFormatVersion, IndexType
    ├── filter_policy.rs       # FilterPolicy, FilterBitsBuilder, FilterBitsReader
    ├── merge_operator.rs      # MergeOperator trait
    ├── compaction_filter.rs   # CompactionFilter, CompactionDecision,
    │                          # CompactionFilterFactory
    └── port.rs                # CACHE_LINE_SIZE, DEFAULT_PAGE_SIZE,
                               # IS_LITTLE_ENDIAN, MAX_PATH_LENGTH
```

## Design translations from C++ → Rust

These were the recurring decisions. They apply across the whole crate and should be preserved in follow-up layers.

### 1. `Slice` is an alias, not a newtype

Upstream `rocksdb::Slice` is a `(const char*, size_t)` pair with methods like `compare`, `starts_with`, `ends_with`, `difference_offset`. Rust's `&[u8]` already is a `(ptr, len)` view with zero-cost `cmp`, `starts_with`, `ends_with`, `eq`. A newtype would add nothing and force every caller into conversions.

**Decision:** `pub type Slice<'a> = &'a [u8];` and a `SliceExt` trait on `[u8]` for the two upstream helpers that are *not* in `core`: `difference_offset` and `to_hex_string`.

### 2. `Status` is a value, `Result<T>` is the idiomatic return

Upstream `Status` is both a success and a failure carrier — `Status::OK()` is a "success" value. In Rust the idiomatic split is `Result<T, E>`, so the crate uses:

- `Status` struct — keeps the C++ field layout (`code`, `subcode`, `severity`, `message`, `retryable`, `data_loss`, `scope`) for direct parity with upstream.
- `pub type Result<T> = core::result::Result<T, Status>` — the idiomatic return type used by every fallible function.
- `pub type IoStatus = Status` — a doc-only alias used in `file_system.rs` to flag "this error came from the filesystem layer." Upstream uses a subclass for the same purpose; in Rust a newtype would be pure boilerplate at this layer.
- `impl std::error::Error for Status` and `impl From<std::io::Error> for Status` — so `?` works against both `Status` and standard I/O errors.

### 3. Abstract classes → `Send + Sync` traits

Every upstream pure-virtual class becomes a Rust trait with `Send + Sync` bounds, matching the "safe for concurrent access from multiple threads" guarantee in the upstream headers. This applies to: `Comparator`, `Snapshot`, `FileSystem`, `FsRandomAccessFile`, `Cache`, `CacheHandle`, `TableFactory`, `FilterPolicy`, `FilterBitsReader`, `MergeOperator`, `CompactionFilter`, `CompactionFilterFactory`, `Clock`, `ThreadPool`, `Env`, `Db`, `ColumnFamilyHandle`, `FileLock`.

Per-file sequential / writable file traits (`FsSequentialFile`, `FsWritableFile`, `FsDirectory`) are **only `Send`** — matching the upstream "not safe for concurrent use" rule. `FsRandomAccessFile` is the exception: `Send + Sync`, because SST block reads run from multiple threads concurrently.

### 4. `unique_ptr<T>` → `Box<dyn T>`, `shared_ptr<T>` → `Arc<dyn T>`

Method returns that hand out ownership of a polymorphic object use `Box<dyn Trait>`. Fields / factories that need shared ownership use `Arc<dyn Trait>`. Raw pointer out-parameters from upstream become function return values:

```cpp
// upstream
virtual IOStatus NewWritableFile(const std::string& fname,
                                 const FileOptions& options,
                                 std::unique_ptr<FSWritableFile>* result,
                                 IODebugContext* dbg) = 0;
```

```rust
// port
fn new_writable_file(
    &self,
    path: &Path,
    opts: &FileOptions,
) -> Result<Box<dyn FsWritableFile>>;
```

### 5. `Iterator` becomes `DbIterator`

Rust's `core::iter::Iterator` is a pull-based, forward-only, `next() -> Option<Item>` trait. RocksDB's iterator is seekable, bidirectional, and has distinct `valid` / `key` / `value` / `status` observers. The two contracts cannot be unified without losing expressiveness, so the port renames the RocksDB trait to `DbIterator` to avoid both the naming collision and the semantic trap of `impl Iterator for DbIterator`.

### 6. `WriteBatch::Handler` → `WriteBatchHandler` trait with default methods

Upstream's `Handler` is a class with pure-virtual methods that implementors override selectively. Rust uses a trait with `Ok(())`-returning default methods so implementors only need to override the record kinds they care about:

```rust
pub trait WriteBatchHandler {
    fn put_cf(&mut self, cf: ColumnFamilyId, key: &[u8], value: &[u8]) -> Result<()> { Ok(()) }
    fn delete_cf(&mut self, cf: ColumnFamilyId, key: &[u8]) -> Result<()> { Ok(()) }
    // ... single_delete_cf, delete_range_cf, merge_cf all default to Ok(())
}
```

`WriteBatch::iterate` short-circuits on the first error, matching upstream.

### 7. `port/` is almost empty

Upstream `port/port.h` wraps `Mutex`, `CondVar`, atomics, byte-order helpers, prefetch intrinsics, and thread IDs behind a `port::` namespace so the rest of RocksDB can pretend the underlying OS is uniform. Rust's `std` already provides all of those directly and portably, so `port.rs` contains only the four constants that genuinely vary by target (`CACHE_LINE_SIZE`, `DEFAULT_PAGE_SIZE`, `IS_LITTLE_ENDIAN`, `MAX_PATH_LENGTH`) plus a `const _: ()` compile-time sanity check.

### 8. `Env` is a composition, not a god class

Upstream `Env` bundles a filesystem, a clock, a thread pool, and a logger behind a single class with ~60 virtual methods. Modern upstream has already started splitting this — `FileSystem` was carved out in 2019. The Rust port finishes the split:

```rust
pub trait Env: Send + Sync {
    fn file_system(&self) -> &Arc<dyn FileSystem>;
    fn clock(&self) -> &Arc<dyn Clock>;
    fn thread_pool(&self) -> &Arc<dyn ThreadPool>;
    // … plus a few tiny helpers (host_name, get_thread_id)
}
```

And ships a `CompositeEnv` that just holds the three sub-traits by `Arc`. This is exactly the construction ForSt's `NewFlinkEnv` performs on the C++ side — the Rust port makes it the **only** way to build an `Env`.

### 9. Enum variants match upstream numeric values where possible

`Code`, `SubCode`, `Severity`, `ValueType`, `CompressionType`, `ChecksumType`, and the various `FileType` / `EntryType` enums use `#[repr(u8)]` (or the appropriate width) with explicit numeric values matching upstream. This is necessary for on-disk format compatibility once higher layers are ported: if `ValueType::Value = 0x1` in upstream, it must be `0x1` here too.

## Verification

Every check below was run against the final committed state.

| Check | Command | Result |
|---|---|---|
| Clean build | `cargo build` | ✅ 0 warnings, 0 errors |
| Clippy (all targets, `-D warnings`) | `cargo clippy --all-targets -- -D warnings` | ✅ clean |
| Unit tests | `cargo test` | ✅ **28 passed**, 1 ignored doc-test |
| No unsafe | `#![forbid(unsafe_code)]` in `lib.rs` | ✅ enforced at compile time |
| Every public item documented | `#![warn(missing_docs)]` in `lib.rs` | ✅ enforced |
| No external dependencies | `Cargo.toml` has empty `[dependencies]` | ✅ std-only |

### Test coverage by module

| Module | Tests | What they verify |
|---|--:|---|
| `status` | 6 | `ok()` construction, predicate helpers, `path_not_found` sub-code derivation, `update_if_ok` first-error-wins, `Result<T>` integration, `From<io::Error>` |
| `slice` | 5 | `difference_offset` for identical / first-byte-diff / prefix relationships, `to_hex_string` output, type alias usability |
| `comparator` | 6 | `BytewiseComparator` name + lexicographic order, `ReverseBytewiseComparator` swap, `find_shortest_separator`, `find_short_successor` for regular & all-`0xff` keys |
| `iterator` | 2 | `EmptyIterator` never reports valid, `ErrorIterator` reports its status |
| `snapshot` | 1 | `SimpleSnapshot` exposes sequence / unix time defaults |
| `write_batch` | 3 | Put / delete mutation round-trip, `WriteBatchHandler` replay counts, short-circuit on first error |
| `options` | 3 | Default values match upstream `OldDefaults` spirit, `create_if_missing` builder, `ReadOptions::new()` defaults |
| `env` | 1 | `SystemClockImpl` returns non-zero time |
| `port` | 1 | Runtime endian matches `target_endian` (the other check is compile-time via `const _`) |

## Known simplifications (intentional)

These are differences from upstream that are *not* bugs — they exist because the upstream complexity is either unnecessary in Rust or premature for Layer 0.

1. **`Status` does not enforce `ROCKSDB_ASSERT_STATUS_CHECKED`.** Upstream has a debug-only runtime check that aborts the process if a `Status` is dropped without being inspected. Rust's `#[must_use]` attribute on `Result` gives us the same at compile time for free. `Status` itself is marked `#[derive(Debug, Clone, PartialEq, Eq)]` and is fine to drop.
2. **No `Cleanable` mixin.** Upstream uses a `Cleanable` base class to attach destructors to `Slice` / `Iterator` for the block-cache release pattern. Rust's `Drop` handles this natively; the Layer 2 cache crate will use an `Arc<CacheHandle>` instead.
3. **No `CompareInterface` separate from `Comparator`.** Upstream splits these because some internal data structures use a different comparator than the user's `Comparator`. In Rust we'll just define a second trait if and when an engine layer needs it.
4. **No `Env::IOActivity` / priority API on `IoOptions`.** Upstream's priority system has grown over time and is partly deprecated; the port takes only `IoPriority::{Low, High}` from the current recommendation.
5. **`WriteBatch` is not serialised.** Upstream serialises to a binary wire format up front so it can go directly to the WAL. The port stores a `Vec<Record>` instead. The wire format is an engine concern and will be defined in the memtable / WAL crate — Layer 0 exposes only the logical API (`put`, `delete`, `iterate`).
6. **`DbOpener` trait, not a free function.** Upstream exposes `DB::Open(options, path, &dbptr)` as a free function; the Rust port has a `DbOpener` trait that engine crates implement. This lets multiple engines (production, test stub, disk-less in-memory) coexist without name collisions.

## Follow-up layers

The layering below mirrors `FORST-READING-ORDER.md`:

| Crate | Depends on | Role | Upstream analog |
|---|---|---|---|
| `st-engine` | `st-rs` | Memtable, WAL, `VersionSet`, `DbImpl` | `db/` |
| `st-table` | `st-rs` | Block-based SST builder/reader | `table/block_based/` |
| `st-env-posix` | `st-rs` | POSIX `FileSystem` implementation | `env/io_posix.cc`, `env/fs_posix.cc` |
| `st-env-flink` | `st-rs` + `jni` | JNI bridge forwarding to `org.apache.flink.core.fs.FileSystem` | `env/flink/`, `java/forstjni/env_flink.cc` |
| `st-cache` | `st-rs` | LRU + HyperClock block cache | `cache/` |
| `st-utilities` | `st-engine` | Checkpoint, backup, transactions, TTL | `utilities/` |
| `st-tools` | all of the above | `ldb`, `sst_dump`, `db_bench` | `tools/` |

Each crate can be built independently against the stable Layer 0 trait surface defined here. Any follow-up changes to `st-rs` must preserve the trait method signatures, because every higher crate will depend on them.

## Change log

- **v0.0.1 (initial)** — first commit of Layer 0. 17 source files, 2,990 LOC, 28 passing tests, clippy-clean, no unsafe, no dependencies.
