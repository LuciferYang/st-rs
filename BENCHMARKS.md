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
| `put_sequential/put_one` | one `db.put()` per iteration | baseline write latency (buffered WAL append; no per-write fsync, matching RocksDB defaults) |
| `put_batch/{8,64,512}` | one `WriteBatch::write()` per iteration | shows WAL amortization with batch size |
| `get_random/hit` | random in-range point lookup on a populated DB | warm read path: bloom hit + block cache + memtable miss → SST decode |
| `get_random/miss` | random out-of-range lookup | bloom-filter fast path; should be ~10–100× the hit rate |
| `scan_forward/full_db` | `seek_to_first` + walk every key with `next()`, just count | k-way merging-iterator throughput; bench loop never reads key/value, so it isolates iterator bookkeeping cost |
| `next_chunk/{64,256,1024}` | M5b vectorized chunked iterator, materializes pairs | the path Flink's `nextBatch` takes; parameterized by chunk size |
| `read_modes/{scalar_collect,chunked_collect_1024}` | scalar `next()` + `to_vec()` vs `next_chunk(1024)` on the same DB, both producing `Vec<(Vec<u8>, Vec<u8>)>` | apples-to-apples: shows the per-call-overhead amortization the chunked path was designed for |

CI runs `cargo bench --no-run` so the benches stay buildable, but we
do **not** gate merges on bench numbers (no regression baseline yet —
see gaps below).

**Recorded result — `read_modes` (Apple Silicon, 100 samples, 50k entries):**

| Mode | Time | Per-elem | Throughput |
|---|---:|---:|---:|
| `scalar_collect` (`next()` + `to_vec()`) | 7.13 ms | 143 ns | 7.0 Melem/s |
| `chunked_collect_1024` (`next_chunk(1024)`) | 5.45 ms | 109 ns | 9.2 Melem/s |

The chunked path is **~24% faster** than scalar when both physically
materialize the `(key, value)` pairs — the JNI/Velox consumer's
workload. This is the win the M5b vectorization was designed for; the
older `scan_forward` baseline hid it because the scalar bench loop
never reads key/value at all (it just counts).

### 2. Concurrency throughput benches — `benches/concurrent.rs`

Throughput (ops/sec) at N concurrent threads. Complements the
single-thread microbenches in `engine.rs` by surfacing lock
contention on the global state mutex and memtable/skip-list
serialization that single-thread numbers can't show.

```bash
cargo bench --bench concurrent                       # full run
cargo bench --bench concurrent -- concurrent_put     # filter by group
```

Groups:

| Group | Workload | Why it matters |
|---|---|---|
| `concurrent_put/{1,2,4,8}` | N threads each writing on disjoint key ranges | exposes write-path lock contention; every concurrent putter contends on `state.lock()` for the WAL append + memtable insert |
| `concurrent_get/{1,2,4,8}` | N threads each doing random point lookups on a populated DB | reads release the lock fast (capture handles + SSTs, then unlock); should scale better than puts |

**Recorded result (Apple Silicon, 20 samples, 10k ops/thread):**

| Threads | `concurrent_put` agg | put scaling | `concurrent_get` agg | get scaling |
|---:|---:|---:|---:|---:|
| 1 | 1.17 Mops/s | 1.0× | 0.82 Mops/s | 1.0× |
| 2 | 0.75 Mops/s | 0.64× | 1.19 Mops/s | 1.45× |
| 4 | 0.55 Mops/s | 0.47× | 0.84 Mops/s | 1.02× |
| 8 | 0.47 Mops/s | 0.40× | 1.12 Mops/s | 1.36× |

Concurrent **puts get _worse_** with thread count: the single
`Mutex<DbState>` held across the whole write path causes every
extra thread to spend more wall time waiting on the lock + paying
cache-line bouncing and scheduler overhead than it does doing
useful work. **Gets** scale weakly (~1.45× peak at 2T then
plateau) — most of the read path runs after `state.lock()` is
released, but the lock is still acquired briefly per call to
capture memtable/SST handles, capping read parallelism.

These are real engine bottlenecks the bench was designed to
surface. Concrete follow-ups:

- **Sharded locking** (one mutex per CF, or RwLock around state)
  would unblock `get` scaling and reduce write contention on
  multi-CF workloads.
- **Group commit** for the WAL append path (RocksDB's classic
  approach: a leader thread serializes a window's writes, follower
  threads queue and wait) would let aggregate write throughput
  exceed single-thread.
- **Lock-free read snapshotting** via `Arc<DbState>` swapping
  would eliminate the read-side lock entirely.

None of these are necessary for current Flink workloads (single
operator thread per state backend instance), but they are the
obvious next architectural moves for higher concurrency.

### 3. JMH JNI-overhead benchmark — `java/src/test/java/org/forstdb/jmh/`

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
`get_hit`, `get_miss`, `scan_forward`, `next_batch/{64,256,1024}`,
`next_batch_packed/{64,256,1024}`.

**Sample comparison (Apple Silicon, single-threaded, warm DB,
`-wi 1 -i 2 -f 1` — illustrative, not a publishable baseline):**

| Workload | Rust (criterion) | Java+JNI (JMH) | Delta |
|---|---:|---:|---:|
| `get_hit` (random in-range point lookup) | ~18.5 µs/op | ~23.0 µs/op | +4.5 µs (~24%) per call |

That ~4.5 µs overhead is the per-call cost of `byte[]` allocation +
JNI marshalling on the get path. It's the headline number this bench
exists to track — a 10× regression here would cost Flink workloads
real money.

**Recorded result — packed vs unpacked vectorized read (50k entries,
`-wi 1 -i 2 -f 1`):**

| chunk | `next_batch` (`byte[][]`) | `next_batch_packed` (`byte[]`) | Speedup |
|---|---:|---:|---:|
| 64 | 23.47 ms | 8.05 ms | 2.9× |
| 256 | 25.63 ms | 7.74 ms | 3.3× |
| 1024 | 28.35 ms | 6.93 ms | 4.1× |

The unpacked variant pays one `byte_array_from_slice` JNI crossing
per key *and* per value — for 50k entries that's 100k JVM allocations
across the JNI boundary. The packed variant builds the chunk once
in Rust with a length-prefixed layout, then crosses with a single
`byte_array_from_slice`; the Java caller decodes pairs on demand
via `ByteBuffer.wrap(packed)`. The win grows with chunk size because
fixed per-chunk costs amortize while the per-entry savings stay
constant. See `RocksIterator.nextBatchPacked(int)`.

### 4. Upstream-RocksDB comparison — `RocksdbBenchmark.java`

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

**Measured (Apple Silicon, single-threaded, `-wi 1 -i 2 -f 1` — small
N, illustrative; not a publishable baseline):**

| Workload | st-rs JNI (before) | st-rs JNI (after) | RocksDB JNI (after) | Ratio (st-rs ÷ RocksDB) |
|---|---:|---:|---:|---:|
| `get_hit` | ~37 µs/op | **1.58 µs/op** | 1.70 µs/op | 0.93× — basically tied |
| `get_miss` | ~3.4 µs/op | **0.81 µs/op** | 3.00 µs/op | 0.27× — st-rs ~3.7× faster |
| `put_one` | ~4500 µs/op | **2.10 µs/op** | 4.55 µs/op | 0.46× — st-rs ~2.2× faster |

Two defaults landed to close the gap:

1. **`WriteOptions.sync` is now honored end-to-end.** Previously
   `DbImpl::write_opt` called `state.wal.sync()` unconditionally on
   every batch — even though the public `WriteOptions::default()`
   has `sync=false`, matching RocksDB. The trait-level `fn write`
   ignored its `opts` argument entirely. Both are fixed: the WAL is
   fsync'd only when `WriteOptions.sync == true`. Recent writes are
   still durable across process crashes (kernel buffer cache); only
   kernel or power loss can drop them. Java callers can opt back into
   per-write fsync via `new WriteOptions().setSync(true)`.

2. **`DbOptions::default().block_cache_size` is now 32 MiB**, matching
   upstream RocksDB's default `BlockBasedTableOptions`. The shared-LRU
   plumbing through `BlockBasedTableReader::open_with_cache` was
   already in place; the default was 0 (no cache), so every `get`
   re-decoded the data block from disk.

The Rust-side `cargo bench --bench engine` numbers reflect the same
two fixes (no JNI overhead in this path): `put_one` dropped from
~4500 µs to **~850 ns**, `get_hit` from ~18.5 µs to **~890 ns**.

### 5. Compaction-throughput bench — `benches/compaction.rs`

Builds N synthetic L0 SST files in a temp dir, then times
`CompactionJob::run()` merging them into a single L1 SST. Reports
MB/s on the input bytes — the metric a regression in the merging
iterator, block builder, or filter-block writer would surface.

```bash
cargo bench --bench compaction
cargo bench --bench compaction -- disjoint     # filter
```

Configurations cover the read-vs-write-amplification axes:

| Config | Inputs × entries | Overlap | What it stresses |
|---|---:|---:|---|
| `disjoint_4x25k` | 4 × 25k | 0% | merge + write path with no dedup; every entry survives |
| `overlap_50_4x25k` | 4 × 25k | 50% | typical L0 → L1 with moderate duplication |
| `fullover_4x25k` | 4 × 25k | 100% | dedup-heavy; 75% of input entries are stale and dropped |

**Recorded result (Apple Silicon, 10 samples):**

| Config | Time | Throughput |
|---|---:|---:|
| `disjoint_4x25k` | 74.7 ms | 148 MiB/s |
| `overlap_50_4x25k` | 54.5 ms | 203 MiB/s |
| `fullover_4x25k` | 42.3 ms | 262 MiB/s |

The MB/s number rises with overlap because more entries get dropped
during dedup, so the output SST writes less. A meaningful regression
would show up as a uniform drop across all three configs (slower
merging iterator) or a drop only on `fullover` (slower dedup).

### 6. Standalone perf harness — `examples/db_bench.rs`

Quick port of upstream RocksDB's `db_bench_tool.cc`. Single-shot
fillseq / readrandom / scanforward numbers on a 100k-entry DB.
Useful for eyeballing absolute throughput; not statistical.

```bash
cargo run --release --example db_bench
```

## How to run benches reproducibly

Bench numbers are only meaningful relative to a stable environment.
The defaults criterion gives you on a noisy laptop are fine for
spotting 2× regressions but useless for tracking 5–10% changes.
This section captures the minimum hygiene to get repeatable numbers
on the machines we actually use.

**General (any OS):**

- **Quiesce the box.** Close browsers, IDEs, Slack, anything that
  kicks off background work. Disable Spotlight/index-syncing.
  Bench *just* benches.
- **Run on AC power.** Battery-saving / dynamic frequency scaling
  destroys repeatability.
- **Always run the comparison side back-to-back.** Don't compare a
  number from yesterday's machine state with today's — re-run both
  baselines together and look at the *delta*, not absolute MB/s.
- **Beware thermal throttling.** Long bench runs (>5 min) on a
  thermally constrained machine (laptop, Mac mini, anything fanless)
  will drift downward as the chip heats up. If you see a bench
  monotonically slowing across criterion samples, that's the cause.
- **Use criterion's confidence intervals.** A change inside the
  reported interval is noise; criterion already prints the p-value
  for `change` rows.

**Linux (best-case stable runner):**

```bash
# 1. Set the CPU governor to "performance" so the cores don't downclock.
echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor

# 2. Disable turbo so per-core frequency is fixed (predictable, slower).
echo 1 | sudo tee /sys/devices/system/cpu/intel_pstate/no_turbo   # Intel
# AMD: cpupower frequency-set -g performance

# 3. Pin the bench process to specific cores. taskset works for any
#    binary; cset shield can isolate cores from the kernel scheduler:
sudo cset shield --cpu 2,3 --kthread on
sudo cset shield --exec -- cargo bench --bench engine

# 4. Optionally disable hyperthreading siblings of pinned cores so the
#    benched code has the L1/L2/scheduler to itself.
echo 0 | sudo tee /sys/devices/system/cpu/cpu3/online
```

**Apple Silicon (the most common dev box here):**

```bash
# 1. Keep the system from sleeping or throttling for power reasons
#    while a bench is running.
caffeinate -dimsu cargo bench --bench engine

# 2. Higher-fidelity: also prevent the OS from idling cores.
sudo pmset noidle &
NOIDLE=$!
cargo bench --bench engine
kill $NOIDLE
```

Apple Silicon has performance + efficiency cores; the macOS scheduler
will sometimes hand criterion samples to E-cores and skew the
distribution. There's no public API to pin to P-cores, but running
on AC + caffeinate gets close enough for our purposes.

**Variance budget:** treat any per-run change inside ±10% as noise
on a laptop, ±5% on a quiesced desktop, ±2% on a `cset`-shielded
Linux runner. CI runs (GHA shared runners) routinely swing ±20–30%
on iteration-throughput benches because the underlying VM is
shared — see "CI regression gate" below for how we work around it.

## CI regression gate

The criterion benches are wired to [CodSpeed](https://codspeed.io)
via the `codspeed-criterion-compat` shim and the
`.github/workflows/codspeed.yml` workflow. CodSpeed instruments at
the function-call level (using `valgrind`-style counting), which
sidesteps GHA's wall-time noise problem: the same instrumented
counts come out regardless of which physical machine GitHub
allocates. That's why we don't try to gate on raw `cargo bench`
output.

**Behavior:**

- On every push to `main`: re-runs the suite to refresh the baseline.
- On every PR: runs the suite, posts a comment with per-bench
  deltas vs the base branch, fails if a regression exceeds the
  configured threshold.

**One-time setup** (required to enable the workflow):

1. Install the [CodSpeed GitHub App](https://github.com/apps/codspeed)
   on this repo.
2. Generate a project token at <https://codspeed.io/settings>, then
   add it as a repo secret named `CODSPEED_TOKEN`.
3. Set the repo variable `CODSPEED_ENABLED=true` to turn the
   workflow on. (Off by default so the check doesn't show as
   failing while setup is pending.)

**Local fallback:** the `codspeed-criterion-compat` crate is a
drop-in for `criterion`; when the CodSpeed env vars aren't set
(local `cargo bench`, the regular `cargo bench --no-run` smoke
job in `ci.yml`) it falls through to plain criterion. So the gate
is a strict superset — adding it doesn't change developer
ergonomics.

**Why CodSpeed and not bencher.dev / self-hosted runners:** CodSpeed
is the only free-for-OSS option that gives us call-instruction
counts rather than wall time, which is what makes a perf gate
viable on GHA's shared runners. A self-hosted runner with `cset`
shielding would also work but adds infra burden we don't currently
have a maintainer for.

## What we don't have (gaps)

These are tracked here so future contributors can pick them up. Each
gap lists *why* it would be valuable and *what* would close it.

(All four originally listed gaps — a, b, c, d, e — are now closed;
this section is preserved for the next round of follow-ups.)

## Convention

When adding a new bench group, also add it to the table above and
note its rationale. When closing a gap above, move that section into
"What we have" rather than deleting it.
