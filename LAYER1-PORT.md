# Layer 1 Rust Port — Implementation Record

This document records the addition of **Layer 1** to the `st-rs` crate, plus the reorganisation of Layer 0 into topic-based subdirectories that happened alongside.

Companion docs:
- `LAYER0-PORT.md` — the Layer 0 port record.
- `ForSt/FORST-READING-ORDER.md` — the layered reading plan.

## Context

Two things happened in this change:

1. **Reorganised Layer 0** into topic-based subdirectories (`core/`, `env/`, `api/`, `ext/`, `port/`) so the crate has room to grow as later layers add more modules. Previously all 17 Layer 0 files sat flat under `src/`.
2. **Added Layer 1** — the foundation-utility modules (`util/`, `memory/`, `logging/`, `monitoring/`).

Both changes are committed separately so git history is clean:
- `refactor: reorganise src/ into topic-based subdirectories`
- `feat: implement Layer 1 (util, memory, logging, monitoring)`

## New crate layout

```
st-rs/
├── Cargo.toml
├── README.md
├── LAYER0-PORT.md
├── LAYER1-PORT.md              ← this file
└── src/
    ├── lib.rs                  ← top-level re-exports
    │
    ├── core/                   ← Layer 0: universal primitives
    │   ├── mod.rs
    │   ├── status.rs           (Status, Code, SubCode, Severity, Result, IoStatus)
    │   ├── slice.rs            (Slice, SliceExt)
    │   └── types.rs            (SequenceNumber, ColumnFamilyId, ValueType, …)
    │
    ├── env/                    ← Layer 0: environment abstraction
    │   ├── mod.rs              (re-exports everything)
    │   ├── env_trait.rs        (Env, Clock, ThreadPool, CompositeEnv, SystemClockImpl)
    │   └── file_system.rs      (FileSystem, FsSequentialFile, FsRandomAccessFile,
    │                            FsWritableFile, FsDirectory, IoOptions, IoType, …)
    │
    ├── api/                    ← Layer 0: user-facing DB API
    │   ├── mod.rs
    │   ├── db.rs
    │   ├── iterator.rs
    │   ├── options.rs
    │   ├── snapshot.rs
    │   └── write_batch.rs
    │
    ├── ext/                    ← Layer 0: extension-point traits
    │   ├── mod.rs
    │   ├── comparator.rs
    │   ├── cache.rs
    │   ├── table.rs
    │   ├── filter_policy.rs
    │   ├── merge_operator.rs
    │   └── compaction_filter.rs
    │
    ├── port/                   ← Layer 0: platform constants
    │   └── mod.rs
    │
    ├── util/                   ← Layer 1: leaf utilities
    │   ├── mod.rs
    │   ├── coding.rs           (varint, fixed32/64, length-prefixed slice)
    │   ├── hash.rs             (hash, hash64, lower/upper 32 of 64)
    │   ├── random.rs           (Random = Park–Miller LCG, Random64 = xorshift*)
    │   └── string_util.rs      (starts_with_ci, parse_u64, parse_bool, split)
    │
    ├── memory/                 ← Layer 1: allocators
    │   ├── mod.rs
    │   ├── allocator.rs        (Allocator trait)
    │   └── arena.rs            (Arena: bump allocator)
    │
    ├── logging/                ← Layer 1: internal logging
    │   ├── mod.rs
    │   └── logger.rs           (Logger trait, LogLevel, NoopLogger, ConsoleLogger)
    │
    └── monitoring/             ← Layer 1: observability
        ├── mod.rs
        ├── statistics.rs       (Statistics trait, StatisticsImpl, Ticker, HistogramType)
        ├── histogram.rs        (Histogram, HistogramSnapshot — bucketed, atomic)
        └── perf_context.rs     (PerfContext, thread-local with/mut_with, PerfLevel)
```

Top-level re-exports in `lib.rs` keep the flat import path working: `use st_rs::{Status, Slice, Db, WriteBatch, Arena, Logger, Histogram, …}` all resolve without the user needing to know which subdirectory a type lives in.

## Layer 0 reorganisation notes

### Why topic-based and not layer-based

I considered organising by layer (`src/layer0/`, `src/layer1/`, …) and rejected it. Layers are a *reading* concept — they describe the order in which a new contributor should absorb the codebase. They are not the right organising principle for a crate, because:

- Future layers will split across multiple topic groups (e.g. Layer 2 adds code to `cache/`, `env/posix/`, and `file/`), so a single `src/layer2/` directory would contain unrelated things.
- `lib.rs` re-exports make the physical location nearly irrelevant to users — they always write `use st_rs::Status`, never `use st_rs::layer0::Status`.
- Mature Rust crates (tokio, sled, zed-ropey, datafusion) organise by topic, not by phase of development.

The chosen five Layer 0 groups each have a clear charter:
- **`core`** — primitives every other module needs (Status, Slice, types). Zero internal dependencies.
- **`env`** — storage abstraction (Env, FileSystem). One-way dependency on `core`.
- **`api`** — user-facing ops (Db, Iterator, WriteBatch, Options, Snapshot). Depends on `core` and `ext`.
- **`ext`** — user extension points (Comparator, Cache, TableFactory, FilterPolicy, MergeOperator, CompactionFilter). Depends on `core`.
- **`port`** — platform constants. Zero internal dependencies.

### Import-path migration

Within-crate imports changed from `crate::status::…` to `crate::core::status::…` and similar. `lib.rs` still re-exports the same names at the crate root, so user code importing `st_rs::Status` is unaffected.

### File renames used `git mv`

Every move went through `git mv`, so `git log --follow` still works on the moved files. The reorganisation is visible as renames in `git show`, not as add+delete pairs.

## Layer 1 content

Layer 1 is the foundation-utility layer — modules that the LSM engine imports everywhere and that have no interesting dependencies of their own. This port includes four modules:

### `util/` — leaf helpers

| File | Key exports | Notes |
|---|---|---|
| `coding.rs` | `put_fixed32/64`, `decode_fixed32/64`, `put_varint32/64`, `get_varint32/64`, `put_length_prefixed_slice`, `get_length_prefixed_slice`, `varint_length`, `MAX_VARINT32_LENGTH`, `MAX_VARINT64_LENGTH` | **Wire-compatible** LEB128 varints and little-endian fixed integers. This is on-disk format territory; mismatches with upstream would corrupt MANIFEST/SST files at Layer 3+. |
| `hash.rs` | `hash`, `hash64`, `lower32_of_64`, `upper32_of_64` | FNV-1a 32-bit and xxHash-style 64-bit mixers. **Not** bit-compatible with upstream `util::Hash`; Layer 2 will replace this with an upstream-exact port so Bloom filters can read back what they wrote. Documented in the module header. |
| `random.rs` | `Random` (Park–Miller LCG), `Random64` (xorshift\*), `uniform`, `one_in`, `skewed` | Methods renamed `next_u32` / `next_u64` to match the `rand_core::RngCore` convention and avoid colliding with `core::iter::Iterator::next`. |
| `string_util.rs` | `starts_with_ci`, `parse_u64`, `parse_bool`, `split` | Tiny — most of `str`'s built-in methods replace upstream's hand-rolled helpers. |

**Deliberately not ported:**
- `util/mutexlock.h` — `std::sync::MutexGuard` from `.lock().unwrap()` is the Rust equivalent.
- `util/thread_local.h` — `std::thread_local!` macro replaces it directly.
- `util/autovector.h` — use `Vec` for now; the `smallvec` crate can drop in later as an opt-in dependency.
- `util/aligned_buffer.h` — belongs with direct-I/O code in Layer 2.
- `util/crc32c.h` — a follow-up; the `crc32fast` crate covers the use case if/when we need it.

### `memory/` — allocators

| File | Key exports | Notes |
|---|---|---|
| `allocator.rs` | `Allocator` trait (`allocate`, `allocate_aligned`, `bytes_allocated`, `memory_allocated`) | Defines the contract used by the memtable. |
| `arena.rs` | `Arena`, `DEFAULT_BLOCK_SIZE` | Bump allocator. Takes `&mut self` for each allocation, matching the single-threaded memtable invariant. A Layer 2 `ConcurrentArena` with `&self`-style access will live behind an `unsafe` module boundary. |

The Arena port explicitly restricts `allocate` to `&mut self` — a departure from upstream, explained in detail in the module-level doc comment. This keeps Layer 1 `#[deny(unsafe_code)]`-clean and is sufficient for the vast majority of unit testing. A proper "multiple live references from the same arena" implementation requires `UnsafeCell` and is deferred to Layer 2, when the concurrent memtable actually needs it.

### `logging/` — internal logging

| File | Key exports | Notes |
|---|---|---|
| `logger.rs` | `Logger` trait, `LogLevel`, `NoopLogger`, `ConsoleLogger` | `LogLevel` is `#[repr(u8)]` with explicit numeric ordering so `level >= WARN`-style filters work. `Header`-level records bypass the filter, matching upstream. Convenience methods (`info`, `warn`, `error`) are trait-default (non-overridable) shortcuts around `log`. |

The `ROCKS_LOG_INFO!`/`ROCKS_LOG_WARN!` macros from upstream are not ported — Rust's `format!` at the callsite is a cleaner replacement.

### `monitoring/` — observability

| File | Key exports | Notes |
|---|---|---|
| `statistics.rs` | `Statistics` trait, `StatisticsImpl`, `Ticker` enum, `HistogramType` enum | `StatisticsImpl` uses `RwLock<HashMap<K, AtomicU64>>` for tickers — the atomic inside means the hot path runs under a shared lock with no contention. First observation of a new ticker takes the exclusive lock. |
| `histogram.rs` | `Histogram`, `HistogramSnapshot`, `NUM_BUCKETS` | Thread-safe atomic histogram with 64 power-of-two buckets covering the full `u64` range. Snapshot computes `p50`, `p95`, `p99`, `p99.9`, `mean`, `sum`, `count`, `min`, `max`. |
| `perf_context.rs` | `PerfContext` struct, `PerfLevel`, `with`, `mut_with`, `reset`, `set_perf_level`, `get_perf_level` | Per-thread counters via `thread_local!`. Fields are public so engine code can update them directly (matching upstream's bare-struct style). |

## Verification

| Check | Command | Result |
|---|---|---|
| Clean build | `cargo build` | ✅ 0 warnings, 0 errors |
| Clippy (all targets, `-D warnings`) | `cargo clippy --all-targets -- -D warnings` | ✅ clean |
| Unit tests | `cargo test` | ✅ **80 passed**, 1 ignored doc-test |
| No unsafe | `#![deny(unsafe_code)]` at crate root | ✅ enforced |
| Every public item documented | `#![warn(missing_docs)]` | ✅ enforced |
| Zero external dependencies | empty `[dependencies]` | ✅ still std-only |

### Test count by layer

| Layer | Module | Tests |
|---|---|--:|
| 0 | `core::status` | 6 |
| 0 | `core::slice` | 5 |
| 0 | `ext::comparator` | 6 |
| 0 | `api::iterator` | 2 |
| 0 | `api::snapshot` | 1 |
| 0 | `api::write_batch` | 3 |
| 0 | `api::options` | 3 |
| 0 | `env::env_trait` | 1 |
| 0 | `port` | 1 |
| **Layer 0 subtotal** | | **28** |
| 1 | `util::coding` | 10 |
| 1 | `util::hash` | 6 |
| 1 | `util::random` | 7 |
| 1 | `util::string_util` | 4 |
| 1 | `memory::arena` | 6 |
| 1 | `logging::logger` | 4 |
| 1 | `monitoring::histogram` | 6 |
| 1 | `monitoring::statistics` | 4 |
| 1 | `monitoring::perf_context` | 3 |
| 1 | *misc* | 2 |
| **Layer 1 subtotal** | | **52** |
| **Total** | | **80** |

## Known simplifications

1. **`util::hash` is not bit-compatible with upstream.** Documented in the module header. Filter blocks built with this hash will be unreadable by upstream RocksDB (and vice versa). Layer 2 will replace this with an upstream-faithful `Hash` port before any on-disk filter block lands.
2. **`memory::arena` allocates under `&mut self` only.** Can't hold two live slices into the same arena at once. Sufficient for Layer 1 tests and for simple memtable-adjacent code that stores offsets rather than references. Layer 2 will add a `ConcurrentArena` that lifts this restriction, inside an `unsafe` module.
3. **`monitoring::histogram` uses power-of-two buckets**, not upstream's hand-tuned bucket table. The `HistogramSnapshot` shape matches upstream (`count`, `sum`, `min`, `max`, `mean`, `p50`, `p95`, `p99`, `p99.9`), but percentile boundaries are coarser. Good enough for observability; replace if we need finer granularity.
4. **`monitoring::statistics::Ticker` and `HistogramType` enums carry ~25 variants each**, not upstream's 200+. The enums are designed to be extended — engine layers that need additional counters can add variants without changing the trait API.
5. **No `PerfContext` timer macro.** Upstream has `PERF_TIMER_GUARD` which scopes an RAII object that writes to a counter. In Rust, a simple `let _guard = PerfTimer::new(&mut pc.field);` + `Drop` impl would do the same, but it's not needed for Layer 1.

## Change log

- **v0.0.2** — reorganised Layer 0 into `core/`, `env/`, `api/`, `ext/`, `port/` subdirectories; added Layer 1 under `util/`, `memory/`, `logging/`, `monitoring/`. 80 passing tests (28 Layer 0 + 52 Layer 1), clippy-clean, still zero external dependencies.
