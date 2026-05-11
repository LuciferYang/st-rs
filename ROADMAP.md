# Roadmap

Tracks unstarted milestones and major work items deferred from active
sessions. For perf-side follow-ups specifically (c3 concurrent skip-list,
c4 `Arc<DbState>` swap, c5 per-CF concurrency) see the gaps section of
[BENCHMARKS.md](BENCHMARKS.md). For shipped work see [CHANGELOG.md](CHANGELOG.md).

## Stage 2 — C ABI crate

A new `st-rs-c` crate that exposes the engine to non-JVM consumers
(Velox / Gluten C++, Python via cffi, Go via cgo) through an
`extern "C"` interface + a generated C header. Discussed at the end
of the 0.1.0 perf wave; **design is unstarted** beyond rough scope.

### Open design questions

Before any code lands, a design doc (`docs/C-ABI-DESIGN.md`) needs to
pin down:

1. **API surface**: which engine operations to expose. Probably
   `open` / `close` / `put` / `get` / `delete` / batch / iterator /
   CF management / snapshots / options / properties — but the exact
   list should be derived from reading Velox/Gluten's existing
   RocksDB C-API binding to see what the *target consumer* actually
   calls. Without that exercise we'd ship something that needs to be
   rewritten when the first real consumer arrives.
2. **Error reporting**: out-param `char**` (RocksDB convention,
   caller frees via engine-provided `free`) vs status-code enum +
   thread-local "last error" string vs both. Need one consistent
   pattern.
3. **Memory ownership for return values**: engine-allocated bytes +
   `forst_free(buf)` vs caller-allocated buffers + length query
   first. Affects every `get`-shaped API.
4. **Binary keys/values**: length-prefixed `char* + size_t` pairs
   throughout (RocksDB convention). Null-terminated strings are not
   an option for binary data.
5. **Header generation**: `cbindgen` (annotations on the Rust side)
   vs hand-written header. cbindgen is the standard but locks the
   `extern "C"` types into specific shapes.
6. **ABI stability**: do we promise per-minor compatibility? Version
   symbols as `forst_v1_*`?
7. **Iterator batch return**: ship a packed-bytes variant
   (analogous to the JNI's `RocksIterator.nextBatchPacked`) in v1?
   Velox almost certainly wants this to amortize FFI cost.
8. **Relation to the existing JNI**: re-implement the JNI on top of
   the C ABI (RocksDB's model, cleaner long-term) or keep them
   parallel (faster to ship, more code to maintain)?

### Suggested first step

Read the Velox + Gluten RocksDB binding code, extract the exact list
of C-API functions they call, then draft `docs/C-ABI-DESIGN.md`
covering the questions above plus the function list. **Code follows
from the design doc, not before**.

### Why deferred

The Velox/Gluten consumer doesn't exist yet for this project — we'd
be designing in a vacuum. Better to wait until there's a concrete
consumer with a concrete subset of operations they need, then design
the ABI around their actual call sites.

## Perf follow-ups

Tracked in [BENCHMARKS.md](BENCHMARKS.md) under "What we don't have":

- **c3** — concurrent / lock-free skip-list memtable
- **c4** — lock-free read snapshot via `Arc<DbState>` swap
- **c5** — per-CF concurrency (multi-CF concurrent writers)

Each is deferred until there's a measured workload that exercises it.
