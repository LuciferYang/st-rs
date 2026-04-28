# Benchmarks

st-rs ships two complementary benchmark surfaces. This doc tracks what
exists, how to run it, and the gaps that still need closing.

## What we have

### 1. Criterion microbenchmarks — `benches/engine.rs`

Statistical microbenchmarks for the engine hot paths. Drives a
`DbImpl` directly (no JNI), uses temp dirs that are cleaned up
between invocations.

```bash
cargo bench --bench engine                       # full run
cargo bench --bench engine -- get_random         # filter by group
cargo bench --bench engine -- --quick            # fast smoke check
```

Groups:

| Group | Workload | Why it matters |
|---|---|---|
| `put_sequential/put_one` | one `db.put()` per iteration | baseline write latency including WAL fsync |
| `put_batch/{8,64,512}` | one `WriteBatch::write()` per iteration | shows WAL amortization with batch size |
| `get_random/hit` | random in-range point lookup on a populated DB | warm read path: bloom hit + block cache + memtable miss → SST decode |
| `get_random/miss` | random out-of-range lookup | bloom-filter fast path; should be ~10–100× the hit rate |
| `scan_forward/full_db` | `seek_to_first` + walk every key | k-way merging-iterator throughput |
| `next_chunk/{64,256,1024}` | M5b vectorized chunked iterator | the path Flink's `nextBatch` takes; parameterized by chunk size |

CI runs `cargo bench --no-run` so the benches stay buildable, but we
do **not** gate merges on bench numbers (no regression baseline yet —
see gaps below).

### 2. Standalone perf harness — `examples/db_bench.rs`

Quick port of upstream RocksDB's `db_bench_tool.cc`. Single-shot
fillseq / readrandom / scanforward numbers on a 100k-entry DB.
Useful for eyeballing absolute throughput; not statistical.

```bash
cargo run --release --example db_bench
```

## What we don't have (gaps)

These are tracked here so future contributors can pick them up. Each
gap lists *why* it would be valuable and *what* would close it.

### a. JNI-overhead bench

**Gap:** All current benches drive `DbImpl` from Rust directly, so we
have no measurement of the per-call cost of crossing the JVM ↔ Rust
boundary (jbyte_array marshalling, GlobalRef churn, exception checks).

**Why it matters:** Flink workloads call into `org.forstdb.RocksDB`
millions of times per second. JNI overhead dominates short ops (a
1µs Rust `get` can become a 5µs Java `get` purely from boundary cost).

**To close:** add a JMH harness under `java/src/jmh/java/...` (or use
a simple JUnit benchmark with `System.nanoTime`) that times
`db.put` / `db.get` / `iterator.nextBatch` Java-side, then compare
against the Rust criterion numbers for the same workload.

### b. Compaction-throughput bench

**Gap:** No bench measures bytes/sec rewritten during background
compaction, the ratio of read vs write amplification, or the wall-clock
cost of a manual full compaction on a populated DB.

**Why it matters:** compaction tuning is a major source of
production incidents in any LSM. Without a regression baseline a
slowdown in the merging iterator, block builder, or filter-block
write path can land silently.

**To close:** new `benches/compaction.rs` that pre-populates N levels
of L0 SSTs (via `flush_all_cfs` in a loop), then times
`pick_compaction_batch + CompactionJob::run`. Report MB/s.

### c. Comparison vs upstream RocksDB / ForSt

**Gap:** Numbers are absolute. We can say "st-rs does X ops/s" but
not "st-rs does X% of ForSt's ops/s on the same workload".

**Why it matters:** the project's value proposition is "drop-in
replacement for ForSt". A 3× regression vs ForSt on a hot path is a
release blocker even if the absolute number looks fine.

**To close:** wire `rocksdbjni` as an alternate JAR in the JNI
overhead bench (b), run both against the same Java workload, and
report a ratio. Or, more ambitious: a Rust harness that drives both
engines via their FFI surfaces.

### d. Vectorized-read comparison (scalar vs `next_chunk`)

**Gap:** We have `scan_forward/full_db` (scalar `next()`) and
`next_chunk/{64,256,1024}` (M5b vectorized) as separate groups, but no
direct same-DB comparison group that would make the speedup obvious.

**Why it matters:** the M5b work was motivated by the hypothesis that
batched reads amortize per-call overhead. The current numbers don't
clearly demonstrate that — adding a paired comparison group would.

**To close:** add a `read_modes` group that runs both styles back-to-back
on the same DB and reports the ratio.

### e. CI regression gate

**Gap:** CI compiles benches but doesn't run them or compare against
a baseline. A 5× write-path regression would land green.

**Why it matters:** without a perf gate, optimizations decay silently
between releases.

**To close:** options in increasing order of fidelity —
1. Run `cargo bench` on every push, compare against the previous run's
   stored Criterion estimates (criterion writes JSON to `target/criterion/`).
2. Use `bencher.dev` or `codspeed.io` (free for OSS) — both auto-detect
   regressions and post PR comments.
3. Self-hosted: dedicate a known-spec runner so absolute numbers stay
   comparable across runs.

GHA's shared runners are too noisy for raw absolute numbers (load
averages vary), so option 2 is the most realistic short-term path.

### f. Concurrency / multi-writer bench

**Gap:** All benches are single-threaded. We don't measure throughput
under N concurrent writers, contention on the memtable's skip-list, or
the WAL write batch group commit (which is the upstream RocksDB hot
path).

**Why it matters:** Flink keyed-state writes from multiple operator
threads are the realistic workload. Single-thread numbers can mask
serious lock contention.

**To close:** new `benches/concurrent.rs` that spins up N threads
each performing `put` and reports aggregate throughput at N ∈ {1, 2,
4, 8}.

### g. Stable bench environment / variance budget

**Gap:** No documented procedure for running benches in a stable
environment (CPU governor, hyperthreading, background load), so
local numbers are not directly comparable across machines or runs.

**Why it matters:** bench output without a variance budget is
anecdotal. Even Criterion's own confidence intervals get washed out
by environmental noise.

**To close:** add a one-page "How to run benches reproducibly"
section here covering at minimum: `cset shield` / `taskset` pinning,
`performance` CPU governor, disabling turbo, and Apple Silicon's
`pmset noidle`.

## Convention

When adding a new bench group, also add it to the table above and
note its rationale. When closing a gap above, move that section into
"What we have" rather than deleting it.
