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

//! Port of `monitoring/statistics_impl.h` + `include/rocksdb/statistics.h`.
//!
//! `Statistics` is the global observability object a user installs on
//! their `DBOptions`. It holds a map of named counters ("tickers") and
//! a map of named latency distributions (histograms). Every `RecordTick`
//! and `MeasureTime` callsite inside the engine writes to this object.

use crate::monitoring::histogram::{Histogram, HistogramSnapshot};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

/// A named counter. Upstream defines this as an enum in
/// `include/rocksdb/statistics.h` with ~200 variants; the Rust port
/// starts with a small set of the most frequently touched ones, leaving
/// room for engine layers to extend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[allow(missing_docs)]
pub enum Ticker {
    // -- Cache --
    BlockCacheHit,
    BlockCacheMiss,
    BlockCacheAdd,
    BlockCacheBytesRead,
    BlockCacheBytesWrite,

    // -- Memtable --
    MemtableHit,
    MemtableMiss,

    // -- Bloom / filter --
    BloomFilterUseful,
    BloomFilterFullPositive,
    BloomFilterFullTrueNegative,

    // -- Compaction / flush --
    CompactReadBytes,
    CompactWriteBytes,
    FlushWriteBytes,

    // -- Writes --
    BytesWritten,
    KeysWritten,
    NumberKeysUpdated,

    // -- Reads --
    BytesRead,
    KeysRead,
    NumberKeysRead,

    // -- Iterators --
    NumberDbSeek,
    NumberDbNext,
    NumberDbPrev,
}

/// A named histogram. Smaller counterpart to `Ticker` — only a few are
/// exposed at Layer 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[allow(missing_docs)]
pub enum HistogramType {
    DbGet,
    DbWrite,
    DbSeek,
    CompactionTime,
    FlushTime,
    BloomFilterProbeNs,
}

/// The user-facing statistics trait.
pub trait Statistics: Send + Sync {
    /// Increment a counter by `delta`.
    fn record_tick(&self, ticker: Ticker, delta: u64);

    /// Read a counter's current value.
    fn get_ticker_count(&self, ticker: Ticker) -> u64;

    /// Record a single observation into a histogram.
    fn record_histogram(&self, histogram: HistogramType, value: u64);

    /// Snapshot a histogram.
    fn histogram_snapshot(&self, histogram: HistogramType) -> HistogramSnapshot;

    /// Reset every ticker and histogram to zero.
    fn reset(&self);

    /// Return a human-readable dump of all tickers and histograms.
    /// Matches upstream `Statistics::ToString`.
    fn to_string(&self) -> String;
}

/// Default concrete [`Statistics`] implementation backed by atomics and
/// a `RwLock`-guarded `HashMap`.
///
/// Tickers are stored in an `RwLock<HashMap<Ticker, AtomicU64>>` — the
/// atomic inside means read/increment happens under a shared lock, so
/// the common path is contention-free. Inserts (first observation) are
/// rare and take an exclusive lock.
pub struct StatisticsImpl {
    tickers: RwLock<HashMap<Ticker, AtomicU64>>,
    histograms: RwLock<HashMap<HistogramType, Histogram>>,
}

impl StatisticsImpl {
    /// Create an empty statistics object.
    pub fn new() -> Self {
        Self {
            tickers: RwLock::new(HashMap::new()),
            histograms: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for StatisticsImpl {
    fn default() -> Self {
        Self::new()
    }
}

impl Statistics for StatisticsImpl {
    fn record_tick(&self, ticker: Ticker, delta: u64) {
        // Fast path: shared lock + atomic fetch_add.
        if let Some(counter) = self.tickers.read().unwrap().get(&ticker) {
            counter.fetch_add(delta, Ordering::Relaxed);
            return;
        }
        // Slow path: create the counter under an exclusive lock.
        let mut writer = self.tickers.write().unwrap();
        writer
            .entry(ticker)
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(delta, Ordering::Relaxed);
    }

    fn get_ticker_count(&self, ticker: Ticker) -> u64 {
        self.tickers
            .read()
            .unwrap()
            .get(&ticker)
            .map(|c| c.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    fn record_histogram(&self, histogram: HistogramType, value: u64) {
        // Same fast/slow path pattern as record_tick.
        if let Some(h) = self.histograms.read().unwrap().get(&histogram) {
            h.add(value);
            return;
        }
        let mut writer = self.histograms.write().unwrap();
        writer.entry(histogram).or_default().add(value);
    }

    fn histogram_snapshot(&self, histogram: HistogramType) -> HistogramSnapshot {
        self.histograms
            .read()
            .unwrap()
            .get(&histogram)
            .map(|h| h.snapshot())
            .unwrap_or_default()
    }

    fn reset(&self) {
        for (_, counter) in self.tickers.read().unwrap().iter() {
            counter.store(0, Ordering::Relaxed);
        }
        for (_, h) in self.histograms.read().unwrap().iter() {
            h.clear();
        }
    }

    fn to_string(&self) -> String {
        use std::fmt::Write as _;
        let mut out = String::new();
        writeln!(&mut out, "-- Tickers --").unwrap();
        let tickers = self.tickers.read().unwrap();
        let mut ticker_entries: Vec<_> =
            tickers.iter().map(|(k, v)| (*k, v.load(Ordering::Relaxed))).collect();
        ticker_entries.sort_by_key(|(k, _)| format!("{k:?}"));
        for (k, v) in ticker_entries {
            writeln!(&mut out, "  {k:?} = {v}").unwrap();
        }
        writeln!(&mut out, "-- Histograms --").unwrap();
        let histograms = self.histograms.read().unwrap();
        let mut histogram_entries: Vec<_> =
            histograms.iter().map(|(k, v)| (*k, v.snapshot())).collect();
        histogram_entries.sort_by_key(|(k, _)| format!("{k:?}"));
        for (k, s) in histogram_entries {
            writeln!(
                &mut out,
                "  {k:?} count={} mean={:.2} p50={} p99={}",
                s.count, s.mean, s.p50, s.p99
            )
            .unwrap();
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ticker_round_trip() {
        let s = StatisticsImpl::new();
        s.record_tick(Ticker::BlockCacheHit, 3);
        s.record_tick(Ticker::BlockCacheHit, 4);
        s.record_tick(Ticker::BlockCacheMiss, 1);
        assert_eq!(s.get_ticker_count(Ticker::BlockCacheHit), 7);
        assert_eq!(s.get_ticker_count(Ticker::BlockCacheMiss), 1);
        assert_eq!(s.get_ticker_count(Ticker::BloomFilterUseful), 0);
    }

    #[test]
    fn histogram_record_and_snapshot() {
        let s = StatisticsImpl::new();
        for v in [10u64, 20, 30, 40] {
            s.record_histogram(HistogramType::DbGet, v);
        }
        let snap = s.histogram_snapshot(HistogramType::DbGet);
        assert_eq!(snap.count, 4);
        assert_eq!(snap.sum, 100);
    }

    #[test]
    fn reset_zeros_all_counters() {
        let s = StatisticsImpl::new();
        s.record_tick(Ticker::KeysWritten, 42);
        s.record_histogram(HistogramType::DbWrite, 99);
        s.reset();
        assert_eq!(s.get_ticker_count(Ticker::KeysWritten), 0);
        assert_eq!(s.histogram_snapshot(HistogramType::DbWrite).count, 0);
    }

    #[test]
    fn to_string_contains_records() {
        let s = StatisticsImpl::new();
        s.record_tick(Ticker::BytesWritten, 123);
        s.record_histogram(HistogramType::DbGet, 5);
        let dump = <StatisticsImpl as Statistics>::to_string(&s);
        assert!(dump.contains("BytesWritten"));
        assert!(dump.contains("123"));
        assert!(dump.contains("DbGet"));
    }
}
