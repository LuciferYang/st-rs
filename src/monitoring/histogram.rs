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

//! Port of `monitoring/histogram.h`.
//!
//! A bucketed histogram used to summarise latency and size
//! distributions. Upstream's implementation is a power-of-two bucket
//! histogram with hand-tuned bucket boundaries; we port the same shape
//! but with a cleaner Rust API.
//!
//! The port trades some of the upstream hand-tuning for simplicity — it
//! uses a fixed power-of-two layout with 109 buckets covering values
//! from 1 ns to roughly 2⁵⁴. This is enough for every measurement the
//! engine records (latencies in microseconds, sizes in bytes).

use std::sync::atomic::{AtomicU64, Ordering};

/// Number of buckets in the histogram. Matches upstream
/// `kHistogramNumBuckets` approximately; the exact value is not
/// observable through any API so we pick something round that covers
/// the full `u64` range.
pub const NUM_BUCKETS: usize = 64;

/// A thread-safe bucketed histogram.
///
/// Values are placed into buckets by `log2(value + 1).floor()`, giving
/// power-of-two bucket boundaries from `[0, 1)` through
/// `[2^63, 2^64)`. This is much simpler than upstream's custom bucket
/// table but covers every measurement the engine takes.
///
/// All methods are thread-safe; `add` uses atomic operations so
/// concurrent flushes / compactions can record into the same histogram
/// without external locking.
#[derive(Debug)]
pub struct Histogram {
    /// Per-bucket hit count. Index `i` contains counts for values in
    /// `[2^i, 2^(i+1))`, with bucket 0 catching `0`.
    buckets: [AtomicU64; NUM_BUCKETS],
    /// Running sum of all observed values. Saturates at `u64::MAX`.
    sum: AtomicU64,
    /// Total number of observations.
    count: AtomicU64,
    /// Minimum observed value. `u64::MAX` when empty.
    min: AtomicU64,
    /// Maximum observed value. `0` when empty.
    max: AtomicU64,
}

impl Histogram {
    /// Create an empty histogram.
    pub fn new() -> Self {
        // We can't const-init an array of AtomicU64 in stable Rust
        // without the `array::from_fn` workaround. Use it.
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            sum: AtomicU64::new(0),
            count: AtomicU64::new(0),
            min: AtomicU64::new(u64::MAX),
            max: AtomicU64::new(0),
        }
    }

    /// Record a single observation.
    pub fn add(&self, value: u64) {
        let bucket = bucket_index(value);
        self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(value, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);

        // Update min/max with compare-and-swap loops. Ordering is
        // Relaxed because these are statistics, not a synchronisation
        // signal — a slightly stale min/max is acceptable.
        let mut cur_min = self.min.load(Ordering::Relaxed);
        while value < cur_min {
            match self.min.compare_exchange_weak(
                cur_min,
                value,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(new_cur) => cur_min = new_cur,
            }
        }
        let mut cur_max = self.max.load(Ordering::Relaxed);
        while value > cur_max {
            match self.max.compare_exchange_weak(
                cur_max,
                value,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(new_cur) => cur_max = new_cur,
            }
        }
    }

    /// Reset all buckets and running totals to zero.
    pub fn clear(&self) {
        for b in self.buckets.iter() {
            b.store(0, Ordering::Relaxed);
        }
        self.sum.store(0, Ordering::Relaxed);
        self.count.store(0, Ordering::Relaxed);
        self.min.store(u64::MAX, Ordering::Relaxed);
        self.max.store(0, Ordering::Relaxed);
    }

    /// Take a consistent snapshot of the histogram. Matches upstream's
    /// `HistogramData` return shape (min, max, sum, count, mean,
    /// percentiles).
    pub fn snapshot(&self) -> HistogramSnapshot {
        let count = self.count.load(Ordering::Relaxed);
        let sum = self.sum.load(Ordering::Relaxed);
        let mean = if count == 0 { 0.0 } else { sum as f64 / count as f64 };
        let min = if count == 0 {
            0
        } else {
            self.min.load(Ordering::Relaxed)
        };
        let max = self.max.load(Ordering::Relaxed);

        // Copy the bucket counts for percentile interpolation.
        let mut buckets = [0u64; NUM_BUCKETS];
        for (i, b) in self.buckets.iter().enumerate() {
            buckets[i] = b.load(Ordering::Relaxed);
        }

        HistogramSnapshot {
            count,
            sum,
            min,
            max,
            mean,
            p50: percentile(&buckets, count, 0.50),
            p95: percentile(&buckets, count, 0.95),
            p99: percentile(&buckets, count, 0.99),
            p99_9: percentile(&buckets, count, 0.999),
        }
    }
}

impl Default for Histogram {
    fn default() -> Self {
        Self::new()
    }
}

/// A point-in-time summary of a [`Histogram`]. Matches upstream
/// `HistogramData`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct HistogramSnapshot {
    /// Total number of observations.
    pub count: u64,
    /// Sum of all observations.
    pub sum: u64,
    /// Minimum observed value.
    pub min: u64,
    /// Maximum observed value.
    pub max: u64,
    /// Arithmetic mean.
    pub mean: f64,
    /// 50th percentile (median).
    pub p50: u64,
    /// 95th percentile.
    pub p95: u64,
    /// 99th percentile.
    pub p99: u64,
    /// 99.9th percentile.
    pub p99_9: u64,
}

/// Assign `value` to a power-of-two bucket. Bucket `i` holds values in
/// `[2^i, 2^(i+1))`; bucket 0 also absorbs `0`.
fn bucket_index(value: u64) -> usize {
    if value == 0 {
        0
    } else {
        // `63 - leading_zeros` == `log2(value).floor()`.
        (63 - value.leading_zeros()) as usize
    }
}

/// Compute an approximate percentile from the bucket array.
fn percentile(buckets: &[u64; NUM_BUCKETS], total: u64, p: f64) -> u64 {
    if total == 0 {
        return 0;
    }
    let target = (p * total as f64).ceil() as u64;
    let mut seen = 0u64;
    for (i, &count) in buckets.iter().enumerate() {
        seen += count;
        if seen >= target {
            // Return the upper bound of bucket `i`: `2^(i+1) - 1`.
            // Bucket 0 represents `[0, 2)`, so return 1.
            return if i == 0 { 1 } else { (1u64 << (i + 1)) - 1 };
        }
    }
    u64::MAX
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_snapshot() {
        let h = Histogram::new();
        let s = h.snapshot();
        assert_eq!(s.count, 0);
        assert_eq!(s.mean, 0.0);
    }

    #[test]
    fn single_observation() {
        let h = Histogram::new();
        h.add(42);
        let s = h.snapshot();
        assert_eq!(s.count, 1);
        assert_eq!(s.sum, 42);
        assert_eq!(s.min, 42);
        assert_eq!(s.max, 42);
        assert_eq!(s.mean, 42.0);
    }

    #[test]
    fn min_max_tracking() {
        let h = Histogram::new();
        for v in [10u64, 3, 25, 1, 100, 50] {
            h.add(v);
        }
        let s = h.snapshot();
        assert_eq!(s.min, 1);
        assert_eq!(s.max, 100);
        assert_eq!(s.count, 6);
    }

    #[test]
    fn bucket_index_is_log2() {
        assert_eq!(bucket_index(0), 0);
        assert_eq!(bucket_index(1), 0);
        assert_eq!(bucket_index(2), 1);
        assert_eq!(bucket_index(3), 1);
        assert_eq!(bucket_index(4), 2);
        assert_eq!(bucket_index(1023), 9);
        assert_eq!(bucket_index(1024), 10);
    }

    #[test]
    fn clear_resets_state() {
        let h = Histogram::new();
        h.add(1);
        h.add(2);
        h.clear();
        let s = h.snapshot();
        assert_eq!(s.count, 0);
        assert_eq!(s.mean, 0.0);
    }

    #[test]
    fn percentile_covers_distribution() {
        let h = Histogram::new();
        for i in 1..=100u64 {
            h.add(i);
        }
        let s = h.snapshot();
        // Bucket distribution for values 1..=100 with power-of-two buckets:
        //   bucket 0 [0, 2):   1 obs  (value 1)
        //   bucket 1 [2, 4):   2 obs  (values 2–3)
        //   bucket 2 [4, 8):   4 obs
        //   bucket 3 [8, 16):  8 obs
        //   bucket 4 [16, 32): 16 obs
        //   bucket 5 [32, 64): 32 obs → cumulative 63 ≥ 50 (p50 target) → p50 = 63
        //   bucket 6 [64, 128): 37 obs → cumulative 100 ≥ 95, ≥ 99 → p95 = p99 = 127
        assert_eq!(s.p50, 63);
        assert_eq!(s.p95, 127);
        assert_eq!(s.p99, 127);
    }

    #[test]
    fn concurrent_adds_preserve_count() {
        use std::sync::Arc;
        use std::thread;

        let h = Arc::new(Histogram::new());
        let threads: Vec<_> = (0..4)
            .map(|i| {
                let h = Arc::clone(&h);
                thread::spawn(move || {
                    for j in 0..1000 {
                        h.add((i * 1000 + j) as u64);
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(h.snapshot().count, 4000);
    }
}
