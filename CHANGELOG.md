# Changelog

## 0.1.0 — 2026-05-11

First minor release after the M7 (Flink integration milestones)
shipped at 0.0.1. The arc since then is a wave of perf wins driven
by a new bench harness, a CI regression gate, and the closure of
the concurrency cliff that single-thread microbenchmarks couldn't see.

### Performance

- **WAL fsync now opt-in** — `WriteOptions.sync` is honored end-to-end
  instead of being silently ignored. Default `sync=false` matches
  upstream RocksDB. Java callers can opt into per-write fsync via
  `setSync(true)`. Closes a ~600× regression on `put_one` that came
  from unconditional `fdatasync` in the engine's write path.
- **Block cache default 32 MiB** — previously 0 (no cache), so every
  read re-decoded the data block from disk. Matches RocksDB's default
  `BlockBasedTableOptions`. Closes a ~8× gap on `get_hit`.
- **`next_chunk` no longer clones each pair** — uses `Vec::drain` to
  move ownership directly into the output. The chunked iterator was
  paradoxically slower than scalar `next()` because it allocated
  twice per element. Now ~17% faster than scalar at chunk=1024.
- **`nextBatchPacked` JNI iterator** — returns one packed `byte[]`
  instead of `2N` separately-allocated `byte[]`s. Trades 100k JVM
  allocations (for a 50k-entry chunk) for one bulk copy, decoded
  on demand via `ByteBuffer.wrap(packed)`. **3.7–8.9× faster than
  `nextBatch`** at chunk≥64 (was 2.9–4.1× at the original commit;
  the gap widened post-c1 because the per-pair JNI overhead picks
  up the new RwLock per-call cost while the packed path pays it
  once per chunk).
- **`put_opt` single-key fast path** — bypassed `WriteBatch` for the
  default-CF JNI put path, dropped 854 ns → 585 ns on `put_one`
  (criterion). Later re-routed through the c2 group-commit
  coordinator alongside `write_opt` to avoid starving queued writers.

### Concurrency cliff (c1 + c2)

- **c1: `Mutex<DbState>` → `RwLock<DbState>`** — readers no longer
  serialize on the engine lock. `concurrent_get/4` jumped from
  0.84 to **2.65 Mops/s (3.15×)**. Writers retain exclusive access.
  Required wrapping `state.wal: LogWriter` and `state.version_set:
  VersionSet` in inner `Mutex` because `Box<dyn FsWritableFile>` is
  `Send` but not `Sync`; the inner mutex is uncontended on the hot
  path because the outer write guard already serializes writers.
- **c2: leader-follower group commit on the write path** — the
  first writer to arrive becomes the leader, takes the engine
  write lock once, and applies every queued submission's WAL
  record + memtable insert in one batch (with at most one
  `fdatasync` if any submission requested it). Closes the
  `concurrent_put_sync/8` cliff: aggregate throughput went from
  **249 → 650 ops/s (2.61×)** and now scales positively with
  thread count instead of regressing.

  Trade-off: 5–15% regression on `concurrent_put` with `sync=false`
  (no fsync to amortize, per-submission overhead isn't recovered).
  We accept this because Flink uses `disable_wal=true` in production
  (no WAL writes at all), so neither the regression nor the win
  applies to the actual deployed workload — but `sync=true` is the
  canonical durable-write pattern and the architectural fix
  matters there.

  Uses `parking_lot::Mutex + Condvar` for the response slots
  (~10× faster park/unpark than std on macOS); `std::sync::RwLock`
  is kept for `DbState` itself because parking_lot's RwLock
  regressed `get_random/hit` ~3× (parking_lot favors writer
  fairness, std favors uncontended reads — picked per use-site
  rather than globally).

### Bench harness

- **`benches/engine.rs`** — criterion microbenches covering
  `put_one`, `put_batch/{8,64,512}`, `get_random/{hit,miss}`,
  `scan_forward`, `next_chunk/{64,256,1024}`, and a `read_modes`
  group with apples-to-apples scalar vs chunked comparison.
- **`benches/concurrent.rs`** — `concurrent_put/{1,2,4,8}`,
  `concurrent_put_sync/{1,2,4,8}`, `concurrent_get/{1,2,4,8}`. The
  sync variant is what made c2's value visible — without it, group
  commit was indistinguishable from regression.
- **`benches/compaction.rs`** — synthetic 4×L0 compaction with
  variable key overlap (0% / 50% / 100%). Records 148/203/262 MiB/s
  on Apple Silicon.
- **JMH JNI bench** — Java-side counterparts via
  `JniOverheadBenchmark` + side-by-side comparison vs upstream
  `rocksdbjni 9.8.4` in `RocksdbBenchmark`. Result: st-rs is faster
  than RocksDB on every measured workload (`get_hit` ~tied,
  `get_miss` 3.7× faster, `put_one` 2.2× faster).

### CI

- **CodSpeed regression gate** — `.github/workflows/codspeed.yml`
  runs the criterion suite under valgrind-style instrumentation,
  posts per-bench deltas on every PR. Sidesteps the GHA shared-runner
  noise floor (±20–30%) that makes raw `cargo bench` numbers
  useless for regression detection. Opt-in via the `CODSPEED_ENABLED`
  repo variable so the workflow is a clean skip until the maintainer
  installs the GitHub App.

### Stability

- **Flink IT savepoint-restore is now deterministic** —
  `ForStBackendCheckpointRestoreIT.valueStateSurvivesSavepointRestore`
  was failing ~1-in-10 on CI because `triggerSavepoint` raced the
  data generator (barrier reached the counter operator before any
  record for some key arrived → null restore in phase 2). Switched
  to `stopWithSavepoint`, which drains in-flight records before
  snapshot. 15/15 passes locally, no more red-check noise.

### Docs

- **BENCHMARKS.md** — full bench harness write-up, recorded numbers
  for every group, side-by-side vs RocksDB, "How to run benches
  reproducibly" with per-OS guidance + variance budget,
  documentation of the std + parking_lot mixed sync primitive
  choice (with the lesson: don't replace one with the other
  globally without benching both), and the c3/c4 follow-up plan.

### API additions

- `WriteOptions.setSync(boolean)` (Java) — toggle WAL fsync per write.
- `RocksIterator.nextBatchPacked(int) → byte[]` (Java) — packed
  vectorized read.
- `DbImpl::put_opt(&[u8], &[u8], &WriteOptions)` (Rust) — single-key
  insert with WriteOptions.

### Reverts during the arc (kept for history)

- `parking_lot::RwLock<DbState>` was tried as a drop-in replacement
  for `std::sync::RwLock<DbState>`. Reverted because it regressed
  `get_random/hit` ~3× — parking_lot's writer-fairness bias costs
  reader-acquisition overhead, while our workload's read path is
  uncontended. See BENCHMARKS.md "Why the engine uses std + parking_lot
  mixed" for the full lesson.

## 0.0.1 — 2026-04-29

Initial release: M1–M7 milestones (Flink ForSt drop-in compatibility).
See `FLINK-INTEGRATION-PLAN.md` for the M1–M7 milestone breakdown.
