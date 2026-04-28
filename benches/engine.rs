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

//! Microbenchmarks for the hot LSM paths.
//!
//! Run with: `cargo bench --bench engine`
//! Filter:   `cargo bench --bench engine -- get_random`
//!
//! These are *engine-side* benches (no JNI overhead). The numbers are
//! intended as a regression baseline for the read/write hot paths that
//! Flink workloads exercise: small keyed-state writes, point lookups,
//! forward scans, and the M5b vectorized `next_chunk` path.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use st_rs::{DbImpl, DbOptions, WriteBatch};
use std::hint::black_box;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Cap on entries used by all benches. Keeps wall-clock bench time
/// bounded so `cargo bench` finishes in seconds, not minutes.
const N_ENTRIES: u64 = 50_000;
const KEY_SIZE: usize = 16;
const VALUE_SIZE: usize = 100;

/// Each call returns a fresh, non-colliding directory under
/// `temp_dir/st-rs-bench-<n>` so concurrent bench invocations don't
/// race on the same DB.
fn fresh_db_dir() -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir()
        .join(format!("st-rs-bench-{}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn opts() -> DbOptions {
    DbOptions {
        create_if_missing: true,
        // 4 MiB write buffer keeps L0 file count modest during a bench.
        db_write_buffer_size: 4 * 1024 * 1024,
        ..DbOptions::default()
    }
}

fn key_of(i: u64) -> Vec<u8> {
    format!("{i:0>width$}", width = KEY_SIZE).into_bytes()
}

fn value() -> Vec<u8> {
    vec![0x42u8; VALUE_SIZE]
}

/// Open a DB pre-populated with `N_ENTRIES` so read benches don't pay
/// fill cost. Returns the open handle and its on-disk dir (caller
/// drops the dir when done).
fn open_populated() -> (Arc<DbImpl>, PathBuf) {
    let dir = fresh_db_dir();
    let db = DbImpl::open(&opts(), &dir).expect("open");
    let v = value();
    for i in 0..N_ENTRIES {
        db.put(&key_of(i), &v).expect("put");
    }
    db.flush().expect("flush");
    db.wait_for_pending_work().expect("wait");
    (db, dir)
}

fn bench_put_sequential(c: &mut Criterion) {
    let mut group = c.benchmark_group("put_sequential");
    group.throughput(Throughput::Elements(1));
    group.bench_function("put_one", |b| {
        let dir = fresh_db_dir();
        let db = DbImpl::open(&opts(), &dir).expect("open");
        let v = value();
        let mut i: u64 = 0;
        b.iter(|| {
            db.put(&key_of(i), &v).expect("put");
            i = i.wrapping_add(1);
        });
        let _ = db.close();
        let _ = std::fs::remove_dir_all(&dir);
    });
    group.finish();
}

fn bench_put_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("put_batch");
    for &batch_size in &[8usize, 64, 512] {
        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &batch_size,
            |b, &n| {
                let dir = fresh_db_dir();
                let db = DbImpl::open(&opts(), &dir).expect("open");
                let v = value();
                let mut base: u64 = 0;
                b.iter(|| {
                    let mut wb = WriteBatch::default();
                    for k in 0..n {
                        wb.put(key_of(base + k as u64), v.clone());
                    }
                    db.write(&wb).expect("write");
                    base = base.wrapping_add(n as u64);
                });
                let _ = db.close();
                let _ = std::fs::remove_dir_all(&dir);
            },
        );
    }
    group.finish();
}

fn bench_get_random(c: &mut Criterion) {
    let (db, dir) = open_populated();
    let mut group = c.benchmark_group("get_random");
    group.throughput(Throughput::Elements(1));
    group.bench_function("hit", |b| {
        // Cycle pseudo-random keys via xorshift; criterion drives the
        // outer loop, we just need a non-sequential probe order.
        let mut state: u64 = 0x9E3779B97F4A7C15;
        b.iter(|| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let i = state % N_ENTRIES;
            let v = db.get(&key_of(i)).expect("get");
            black_box(v);
        });
    });
    group.bench_function("miss", |b| {
        let mut state: u64 = 0xDEADBEEFCAFEBABE;
        b.iter(|| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            // Keys outside [0, N_ENTRIES) — guaranteed bloom miss.
            let i = N_ENTRIES + (state % N_ENTRIES);
            let v = db.get(&key_of(i)).expect("get");
            black_box(v);
        });
    });
    group.finish();
    let _ = db.close();
    let _ = std::fs::remove_dir_all(&dir);
}

fn bench_scan_forward(c: &mut Criterion) {
    let (db, dir) = open_populated();
    let mut group = c.benchmark_group("scan_forward");
    group.throughput(Throughput::Elements(N_ENTRIES));
    group.bench_function("full_db", |b| {
        b.iter(|| {
            let mut it = db.iter().expect("iter");
            it.seek_to_first();
            let mut n: u64 = 0;
            while it.valid() {
                n += 1;
                it.next();
            }
            black_box(n);
        });
    });
    group.finish();
    let _ = db.close();
    let _ = std::fs::remove_dir_all(&dir);
}

fn bench_next_chunk(c: &mut Criterion) {
    let (db, dir) = open_populated();
    let mut group = c.benchmark_group("next_chunk");
    for &chunk in &[64usize, 256, 1024] {
        group.throughput(Throughput::Elements(N_ENTRIES));
        group.bench_with_input(
            BenchmarkId::from_parameter(chunk),
            &chunk,
            |b, &max| {
                b.iter(|| {
                    let mut it = db.iter().expect("iter");
                    it.seek_to_first();
                    let mut total = 0usize;
                    loop {
                        let chunk = it.next_chunk(max);
                        if chunk.is_empty() {
                            break;
                        }
                        total += chunk.len();
                    }
                    black_box(total);
                });
            },
        );
    }
    group.finish();
    let _ = db.close();
    let _ = std::fs::remove_dir_all(&dir);
}

criterion_group!(
    benches,
    bench_put_sequential,
    bench_put_batch,
    bench_get_random,
    bench_scan_forward,
    bench_next_chunk,
);
criterion_main!(benches);
