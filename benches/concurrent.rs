// Copyright 2025 The st-rs Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Multi-threaded throughput benches.
//!
//! Run with: `cargo bench --bench concurrent`
//! Filter:   `cargo bench --bench concurrent -- concurrent_put`
//!
//! These are *throughput* benches (ops/sec) at N concurrent threads,
//! complementing the single-thread microbenches in `engine.rs`.
//! They surface lock contention on the global state mutex and
//! memtable/skip-list serialization that single-thread numbers hide.
//!
//! Engine semantics today: `DbImpl::state` is a single `Mutex` held
//! across the entire write path (WAL append + memtable insert +
//! flush trigger check) and briefly on the read path (capture
//! memtable handles + SST list, then released for the actual SST
//! reads). The expected scaling profile is therefore:
//!
//! - `concurrent_put`: roughly flat — writes serialize on the lock.
//! - `concurrent_get`: scales sub-linearly — most read work happens
//!   outside the lock once handles are captured.
//!
//! A regression that introduces a long-held lock on the read path
//! would show up here as `concurrent_get` flattening.

use codspeed_criterion_compat::{
    criterion_group, criterion_main, BenchmarkId, Criterion, Throughput,
};
use st_rs::{DbImpl, DbOptions, WriteOptions};
use std::hint::black_box;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const KEY_SIZE: usize = 16;
const VALUE_SIZE: usize = 100;
/// Per-thread work per criterion sample. Big enough that thread
/// spawn / join cost is amortized to <5% of measured time, small
/// enough that a full bench run finishes in a few minutes.
const OPS_PER_THREAD: u64 = 10_000;
/// Per-thread work for the sync-WAL variant. Each fsync is millisecond-
/// scale, so 1k ops × 1 ms = 1 s per sample without batching — already
/// at the upper bound of what we want a single bench sample to take.
const OPS_PER_THREAD_SYNC: u64 = 1_000;
/// Pre-populated DB size for the concurrent_get bench.
const POPULATED_ENTRIES: u64 = 50_000;

fn fresh_db_dir(tag: &str) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir()
        .join(format!("st-rs-concurrent-{tag}-{}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn opts() -> DbOptions {
    DbOptions {
        create_if_missing: true,
        // Larger write buffer than engine.rs: with N threads pumping
        // writes concurrently the memtable fills faster and we want
        // the steady-state path to dominate, not flushes.
        db_write_buffer_size: 64 * 1024 * 1024,
        ..DbOptions::default()
    }
}

fn key_of(i: u64) -> Vec<u8> {
    format!("{i:0>width$}", width = KEY_SIZE).into_bytes()
}

fn value() -> Vec<u8> {
    vec![0x42u8; VALUE_SIZE]
}

/// Measure aggregate wall time for `n_threads` threads each performing
/// `ops_per_thread` puts on disjoint key ranges. Returns total elapsed
/// time across the slowest thread (criterion's iter_custom contract).
fn run_concurrent_put(db: &Arc<DbImpl>, n_threads: usize, ops_per_thread: u64) -> Duration {
    let start = Instant::now();
    let mut handles = Vec::with_capacity(n_threads);
    for tid in 0..n_threads {
        let db = Arc::clone(db);
        let v = value();
        // Disjoint per-thread key range so threads don't collide on
        // the same memtable bucket. Spaced so the writeCursor for
        // thread `tid` over multiple iterations never overflows into
        // thread `tid+1`'s range during a sample window.
        let base = (tid as u64) * 1_000_000_000;
        handles.push(std::thread::spawn(move || {
            for i in 0..ops_per_thread {
                db.put(&key_of(base + i), &v).expect("concurrent put");
            }
        }));
    }
    for h in handles {
        h.join().expect("thread panic");
    }
    start.elapsed()
}

/// Like [`run_concurrent_put`] but every put requests `sync=true`, so
/// each one (or each batch, with c2 group commit) issues a real
/// `fdatasync` on the WAL. This is the workload group commit was
/// designed for: amortizing the millisecond-scale fsync cost across
/// concurrent writers.
fn run_concurrent_put_sync(db: &Arc<DbImpl>, n_threads: usize, ops_per_thread: u64) -> Duration {
    let start = Instant::now();
    let mut handles = Vec::with_capacity(n_threads);
    for tid in 0..n_threads {
        let db = Arc::clone(db);
        let v = value();
        let base = (tid as u64) * 1_000_000_000;
        let wo = WriteOptions {
            sync: true,
            ..WriteOptions::default()
        };
        handles.push(std::thread::spawn(move || {
            for i in 0..ops_per_thread {
                db.put_opt(&key_of(base + i), &v, &wo).expect("sync put");
            }
        }));
    }
    for h in handles {
        h.join().expect("thread panic");
    }
    start.elapsed()
}

/// Same, but each thread does point lookups on a populated DB.
fn run_concurrent_get(db: &Arc<DbImpl>, n_threads: usize, ops_per_thread: u64) -> Duration {
    let start = Instant::now();
    let mut handles = Vec::with_capacity(n_threads);
    for tid in 0..n_threads {
        let db = Arc::clone(db);
        handles.push(std::thread::spawn(move || {
            // Each thread uses a different xorshift seed so probe
            // sequences don't perfectly overlap.
            let mut state: u64 = 0x9E3779B97F4A7C15u64.wrapping_add(tid as u64 * 0xDEADBEEF);
            for _ in 0..ops_per_thread {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                let i = state % POPULATED_ENTRIES;
                let v = db.get(&key_of(i)).expect("concurrent get");
                black_box(v);
            }
        }));
    }
    for h in handles {
        h.join().expect("thread panic");
    }
    start.elapsed()
}

fn bench_concurrent_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_put");
    for &threads in &[1usize, 2, 4, 8] {
        // Throughput is total ops across all threads per iteration.
        group.throughput(Throughput::Elements(threads as u64 * OPS_PER_THREAD));
        group.bench_with_input(
            BenchmarkId::from_parameter(threads),
            &threads,
            |b, &n_threads| {
                // Open a fresh DB once per bench function, reuse across
                // criterion samples to avoid open-cost dominating.
                let dir = fresh_db_dir(&format!("put-{n_threads}"));
                let db = DbImpl::open(&opts(), &dir).expect("open");
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        total += run_concurrent_put(&db, n_threads, OPS_PER_THREAD);
                    }
                    total
                });
                let _ = db.close();
                let _ = std::fs::remove_dir_all(&dir);
            },
        );
    }
    group.finish();
}

fn bench_concurrent_get(c: &mut Criterion) {
    // Pre-populate once, reuse for every (threads, sample) pair.
    let dir = fresh_db_dir("get-shared");
    let db = DbImpl::open(&opts(), &dir).expect("open");
    let v = value();
    for i in 0..POPULATED_ENTRIES {
        db.put(&key_of(i), &v).expect("populate");
    }
    db.flush().expect("flush");
    db.wait_for_pending_work().expect("wait");

    let mut group = c.benchmark_group("concurrent_get");
    for &threads in &[1usize, 2, 4, 8] {
        group.throughput(Throughput::Elements(threads as u64 * OPS_PER_THREAD));
        group.bench_with_input(
            BenchmarkId::from_parameter(threads),
            &threads,
            |b, &n_threads| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        total += run_concurrent_get(&db, n_threads, OPS_PER_THREAD);
                    }
                    total
                });
            },
        );
    }
    group.finish();

    let _ = db.close();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Concurrent puts with `sync=true` — every put waits on a real
/// `fdatasync`. This is the workload group commit was designed for:
/// each fsync is millisecond-scale, so batching N concurrent writers
/// into one fsync should scale aggregate throughput close to linear.
fn bench_concurrent_put_sync(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_put_sync");
    // Sample size kept low because each fsync is ms-scale and the
    // bench wall time would otherwise blow past criterion's defaults.
    group.sample_size(10);
    for &threads in &[1usize, 2, 4, 8] {
        group.throughput(Throughput::Elements(threads as u64 * OPS_PER_THREAD_SYNC));
        group.bench_with_input(
            BenchmarkId::from_parameter(threads),
            &threads,
            |b, &n_threads| {
                let dir = fresh_db_dir(&format!("put-sync-{n_threads}"));
                let db = DbImpl::open(&opts(), &dir).expect("open");
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        total += run_concurrent_put_sync(&db, n_threads, OPS_PER_THREAD_SYNC);
                    }
                    total
                });
                let _ = db.close();
                let _ = std::fs::remove_dir_all(&dir);
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_concurrent_put,
    bench_concurrent_put_sync,
    bench_concurrent_get,
);
criterion_main!(benches);
