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

### 2. JMH JNI-overhead benchmark — `java/src/test/java/org/forstdb/jmh/`

Measures the same hot paths from Java so the difference between Rust
direct numbers (criterion) and Java-via-JNI numbers reveals per-call
boundary overhead (byte-array marshalling, GlobalRef churn, exception
checks).

```bash
./bench.sh                              # full suite, default tuning
./bench.sh get_hit                      # filter by regex
./bench.sh -wi 5 -i 10 -f 2 get_hit     # custom warmup / iters / forks
```

The script builds the release JNI cdylib, compiles the bench classes
under the `bench` Maven profile, and launches a fresh JVM with the
right `java.library.path` and test classpath so JMH's forks find both.

Groups mirror the criterion harness: `put_one`, `put_batch/{8,64,512}`,
`get_hit`, `get_miss`, `scan_forward`, `next_batch/{64,256,1024}`.

**Sample comparison (Apple Silicon, single-threaded, warm DB,
`-wi 1 -i 2 -f 1` — illustrative, not a publishable baseline):**

| Workload | Rust (criterion) | Java+JNI (JMH) | Delta |
|---|---:|---:|---:|
| `get_hit` (random in-range point lookup) | ~18.5 µs/op | ~23.0 µs/op | +4.5 µs (~24%) per call |

That ~4.5 µs overhead is the per-call cost of `byte[]` allocation +
JNI marshalling on the get path. It's the headline number this bench
exists to track — a 10× regression here would cost Flink workloads
real money.

### 3. Upstream-RocksDB comparison — `RocksdbBenchmark.java`

Same workloads as `JniOverheadBenchmark` but driving
`org.rocksdb.RocksDB` (rocksdbjni 9.8.4). Lives next to the JNI
bench so JMH reports both classes side-by-side, e.g.
`JniOverheadBenchmark.get_hit` vs `RocksdbBenchmark.get_hit`. Run
exactly the same way:

```bash
./bench.sh                              # both backends
./bench.sh 'get_hit$'                   # filter to one workload, both backends
```

Notes on fairness: identical key/value sizes, write-buffer cap, PRNG
access pattern. Bloom filters and Snappy compression are left at each
engine's defaults — we deliberately do **not** disable them on either
side, because "what does the engine do out of the box" is the answer
this bench is supposed to give.

**Sample numbers (Apple Silicon, single-threaded, `-wi 1 -i 2 -f 1`
— small N, illustrative only):**

| Workload | st-rs JNI | RocksDB JNI | Ratio |
|---|---:|---:|---:|
| `get_hit` | ~37 µs/op | ~4.4 µs/op | st-rs ~8.4× slower |
| `get_miss` (bloom path) | ~3.4 µs/op | ~9.9 µs/op | **st-rs ~3× faster** |
| `put_one` | ~4500 µs/op | ~7.4 µs/op | st-rs ~600× slower |

These are useful precisely because they're surprising. The `get_hit`
gap likely traces to block-cache hit rate or block decode hot path;
the `get_miss` win comes from a faster (or simpler) bloom check; the
`put_one` blow-up almost certainly means we're fsync-ing on every put
where RocksDB defaults to async WAL. Each row above is a follow-up
ticket waiting to be filed — see `gap a` below for the next bench
that would isolate the compaction side.

### 4. Standalone perf harness — `examples/db_bench.rs`

Quick port of upstream RocksDB's `db_bench_tool.cc`. Single-shot
fillseq / readrandom / scanforward numbers on a 100k-entry DB.
Useful for eyeballing absolute throughput; not statistical.

```bash
cargo run --release --example db_bench
```

## What we don't have (gaps)

These are tracked here so future contributors can pick them up. Each
gap lists *why* it would be valuable and *what* would close it.

### a. Compaction-throughput bench

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

### b. Vectorized-read comparison (scalar vs `next_chunk`)

**Gap:** We have `scan_forward/full_db` (scalar `next()`) and
`next_chunk/{64,256,1024}` (M5b vectorized) as separate groups, but no
direct same-DB comparison group that would make the speedup obvious.

**Why it matters:** the M5b work was motivated by the hypothesis that
batched reads amortize per-call overhead. The current numbers don't
clearly demonstrate that — adding a paired comparison group would.

**To close:** add a `read_modes` group that runs both styles back-to-back
on the same DB and reports the ratio.

### c. CI regression gate

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

### d. Concurrency / multi-writer bench

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

### e. Stable bench environment / variance budget

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
