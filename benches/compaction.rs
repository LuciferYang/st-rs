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

//! Compaction-throughput bench.
//!
//! Run with: `cargo bench --bench compaction`
//! Filter:   `cargo bench --bench compaction -- merge_4_l0`
//!
//! Builds N synthetic L0 SST files in a temp dir, then times
//! `CompactionJob::run()` merging them into a single L1 SST.
//! Reports MB/s on the input bytes — the metric a regression in
//! the merging iterator, block builder, or filter-block writer
//! would surface.
//!
//! Workloads are parameterised by:
//!
//! - `num_inputs`: how many L0 SSTs to merge (read amplification proxy).
//! - `entries_per_input`: how dense each input is.
//! - `key_overlap`: fraction of keys shared across inputs (drives the
//!   dedup workload — at 0% every entry survives, at 100% only the
//!   newest version of each key survives).

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use st_rs::{
    BlockBasedTableBuilder, BlockBasedTableOptions, BlockBasedTableReader, CompactionJob,
    FileSystem, InternalKey, PosixFileSystem, RandomAccessFileReader, ValueType,
    WritableFileWriter,
};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const KEY_SIZE: usize = 16;
const VALUE_SIZE: usize = 100;

fn fresh_dir() -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir()
        .join(format!("st-rs-compaction-{}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("mkdir");
    dir
}

fn key_of(i: u64) -> Vec<u8> {
    format!("{i:0>width$}", width = KEY_SIZE).into_bytes()
}

fn value() -> Vec<u8> {
    vec![0x42u8; VALUE_SIZE]
}

/// Build a single SST containing internal-key Put entries for the
/// supplied user keys. Each input's key range is offset by `(input_idx
/// * stride)` so the inputs share `overlap_pct%` of their keys.
fn build_sst(
    fs: &dyn FileSystem,
    path: &std::path::Path,
    input_idx: u64,
    entries: u64,
    overlap_pct: u32,
    seq_base: u64,
) -> Arc<BlockBasedTableReader> {
    // overlap=100 → every input has the same key range [0, entries).
    // overlap=0   → input i has [i*entries, (i+1)*entries) (disjoint).
    // In between: keys 0..entries * (overlap/100) are shared, the rest
    // are per-input. Approximated as: stride = entries * (1 - overlap/100).
    let stride = (entries as f64 * (1.0 - (overlap_pct as f64 / 100.0))) as u64;
    let start = input_idx * stride;

    let writable = fs.new_writable_file(path, &Default::default()).expect("create");
    let mut tb = BlockBasedTableBuilder::new(
        WritableFileWriter::new(writable),
        BlockBasedTableOptions::default(),
    );

    let mut records: Vec<(Vec<u8>, Vec<u8>)> = (0..entries)
        .map(|i| {
            let ukey = key_of(start + i);
            let seq = seq_base + i;
            let ikey = InternalKey::new(&ukey, seq, ValueType::Value).into_bytes();
            (ikey, value())
        })
        .collect();
    // Block-based table requires entries in internal-key sorted order.
    records.sort_by(|a, b| a.0.cmp(&b.0));
    for (ikey, val) in &records {
        tb.add(ikey, val).expect("add");
    }
    tb.finish().expect("finish");

    let raw = fs.new_random_access_file(path, &Default::default()).expect("open");
    let reader = RandomAccessFileReader::new(raw, path.display().to_string()).expect("reader");
    Arc::new(BlockBasedTableReader::open(Arc::new(reader)).expect("table"))
}

/// Set up a fresh dir with `num_inputs` SSTs, return the readers and
/// the (sum of) input file sizes for throughput accounting.
fn prepare_inputs(
    fs: &dyn FileSystem,
    dir: &std::path::Path,
    num_inputs: u64,
    entries_per_input: u64,
    overlap_pct: u32,
) -> (Vec<Arc<BlockBasedTableReader>>, u64) {
    let mut readers = Vec::with_capacity(num_inputs as usize);
    let mut total_bytes: u64 = 0;
    for i in 0..num_inputs {
        let path = dir.join(format!("input-{i:03}.sst"));
        // Newer inputs get higher base sequences so dedup picks them.
        let seq_base = (num_inputs - i) * 1_000_000;
        let reader = build_sst(fs, &path, i, entries_per_input, overlap_pct, seq_base);
        total_bytes += fs.get_file_size(&path).unwrap_or(0);
        readers.push(reader);
    }
    (readers, total_bytes)
}

fn bench_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("compaction_merge");
    let fs = PosixFileSystem::new();

    // Three configurations covering the interesting axes:
    //   (num_inputs, entries_per_input, overlap_pct, label)
    let configs: &[(u64, u64, u32, &str)] = &[
        // Disjoint key ranges: no dedup, every input entry survives.
        // Stresses the merging iterator + block writer end-to-end.
        (4, 25_000, 0, "disjoint_4x25k"),
        // 50% overlap: realistic L0 → L1 compaction with a moderate
        // duplication factor. Half the entries are dropped during merge.
        (4, 25_000, 50, "overlap_50_4x25k"),
        // Heavy overlap: 4 inputs all hold the same key range, so
        // 75% of input entries are stale and dropped. Stresses dedup.
        (4, 25_000, 100, "fullover_4x25k"),
    ];

    for &(num_inputs, entries_per_input, overlap_pct, label) in configs {
        // Set up once per config — input SSTs are immutable, so we
        // can reuse them across all criterion samples.
        let dir = fresh_dir();
        let (inputs, total_input_bytes) =
            prepare_inputs(&fs, &dir, num_inputs, entries_per_input, overlap_pct);

        // Throughput is measured against input bytes — the standard
        // way to talk about compaction throughput in LSM literature.
        group.throughput(Throughput::Bytes(total_input_bytes));
        group.bench_with_input(BenchmarkId::from_parameter(label), &inputs, |b, inputs| {
            b.iter(|| {
                // Each iteration writes to a different output path so
                // we don't pay the cost of removing/replacing the file
                // (and so `criterion`'s sampling sees uncached writes).
                static OUT_COUNTER: AtomicU64 = AtomicU64::new(0);
                let out_n = OUT_COUNTER.fetch_add(1, Ordering::Relaxed);
                let out_path = dir.join(format!("out-{out_n:06}.sst"));
                let job = CompactionJob::new(
                    &fs,
                    inputs.clone(),
                    &out_path,
                    BlockBasedTableOptions::default(),
                );
                let _ = job.run().expect("compaction");
            });
        });

        // Best-effort cleanup; criterion may have buffered output files
        // that the next iter would reuse, so wipe the whole dir.
        let _ = std::fs::remove_dir_all(&dir);
    }
    group.finish();
}

criterion_group!(benches, bench_merge);
criterion_main!(benches);
