//! Flink-style TTL compaction filter.
//!
//! Flink embeds a timestamp in each state value and uses a compaction
//! filter to drop entries whose timestamp is older than the configured
//! TTL. This module provides a generic TTL filter that works with any
//! timestamp-extraction function.

use crate::core::types::EntryType;
use crate::ext::compaction_filter::{CompactionDecision, CompactionFilter, CompactionFilterFactory};

/// A compaction filter that drops entries whose embedded timestamp is
/// older than `now - ttl_millis`. The timestamp is extracted from the
/// value bytes by a user-supplied function.
///
/// This mirrors Flink's `FlinkCompactionFilter` which checks state
/// TTL during compaction.
/// Type alias for the timestamp extractor function.
pub type TimestampExtractor = dyn Fn(&[u8]) -> Option<u64> + Send + Sync;

/// A compaction filter that drops entries whose embedded timestamp is
/// older than `now - ttl_millis`. The timestamp is extracted from the
/// value bytes by a user-supplied function.
///
/// This mirrors Flink's `FlinkCompactionFilter` which checks state
/// TTL during compaction.
pub struct TtlCompactionFilter {
    /// Current wall-clock time in milliseconds since UNIX epoch.
    now_millis: u64,
    /// Time-to-live in milliseconds. Entries older than `now - ttl`
    /// are dropped.
    ttl_millis: u64,
    /// Extracts the timestamp (in millis since epoch) from a value.
    /// Returns `None` if the value has no timestamp (entry is kept).
    extract_timestamp: Box<TimestampExtractor>,
}

impl TtlCompactionFilter {
    /// Create a new TTL filter.
    pub fn new(
        now_millis: u64,
        ttl_millis: u64,
        extract_timestamp: Box<TimestampExtractor>,
    ) -> Self {
        Self {
            now_millis,
            ttl_millis,
            extract_timestamp,
        }
    }
}

impl CompactionFilter for TtlCompactionFilter {
    fn name(&self) -> &'static str {
        "FlinkTtlCompactionFilter"
    }

    fn filter(
        &self,
        _level: u32,
        _key: &[u8],
        _entry_type: EntryType,
        existing_value: &[u8],
    ) -> CompactionDecision {
        if let Some(ts) = (self.extract_timestamp)(existing_value) {
            if self.now_millis.saturating_sub(ts) > self.ttl_millis {
                return CompactionDecision::Remove;
            }
        }
        CompactionDecision::Keep
    }
}

/// Factory that creates [`TtlCompactionFilter`] instances for each
/// compaction job, anchored at the current wall-clock time.
pub struct TtlCompactionFilterFactory {
    /// TTL in milliseconds.
    ttl_millis: u64,
    /// Timestamp extractor (cloned for each filter instance).
    extract_timestamp: std::sync::Arc<TimestampExtractor>,
    /// Clock function returning current time in millis since epoch.
    clock: Box<dyn Fn() -> u64 + Send + Sync>,
}

impl TtlCompactionFilterFactory {
    /// Create a new factory.
    ///
    /// - `ttl_millis`: entries older than this are dropped.
    /// - `extract_timestamp`: extracts timestamp from value bytes.
    /// - `clock`: returns current time in millis (e.g.,
    ///   `SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64`).
    pub fn new(
        ttl_millis: u64,
        extract_timestamp: std::sync::Arc<TimestampExtractor>,
        clock: Box<dyn Fn() -> u64 + Send + Sync>,
    ) -> Self {
        Self {
            ttl_millis,
            extract_timestamp,
            clock,
        }
    }
}

impl CompactionFilterFactory for TtlCompactionFilterFactory {
    fn name(&self) -> &'static str {
        "FlinkTtlCompactionFilterFactory"
    }

    fn create_compaction_filter(
        &self,
        _is_full_compaction: bool,
        _is_manual_compaction: bool,
    ) -> Box<dyn CompactionFilter> {
        let now = (self.clock)();
        let extractor = std::sync::Arc::clone(&self.extract_timestamp);
        Box::new(TtlCompactionFilter::new(
            now,
            self.ttl_millis,
            Box::new(move |v| extractor(v)),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn extract_u64_timestamp(value: &[u8]) -> Option<u64> {
        if value.len() < 8 {
            return None;
        }
        Some(u64::from_le_bytes(value[..8].try_into().unwrap()))
    }

    #[test]
    fn ttl_filter_keeps_fresh_entries() {
        let filter = TtlCompactionFilter::new(
            1000, // now
            500,  // ttl
            Box::new(extract_u64_timestamp),
        );
        // Timestamp 800 → age = 200ms < 500ms TTL → keep.
        let value = 800u64.to_le_bytes();
        assert!(matches!(
            filter.filter(0, b"k", EntryType::Put, &value),
            CompactionDecision::Keep
        ));
    }

    #[test]
    fn ttl_filter_removes_expired_entries() {
        let filter = TtlCompactionFilter::new(
            1000, // now
            500,  // ttl
            Box::new(extract_u64_timestamp),
        );
        // Timestamp 100 → age = 900ms > 500ms TTL → remove.
        let value = 100u64.to_le_bytes();
        assert!(matches!(
            filter.filter(0, b"k", EntryType::Put, &value),
            CompactionDecision::Remove
        ));
    }

    #[test]
    fn ttl_filter_keeps_entries_without_timestamp() {
        let filter = TtlCompactionFilter::new(
            1000,
            500,
            Box::new(extract_u64_timestamp),
        );
        // Value too short for timestamp → keep.
        assert!(matches!(
            filter.filter(0, b"k", EntryType::Put, b"short"),
            CompactionDecision::Keep
        ));
    }

    #[test]
    fn factory_creates_filter_with_current_time() {
        let factory = TtlCompactionFilterFactory::new(
            500,
            std::sync::Arc::new(extract_u64_timestamp),
            Box::new(|| 1000), // fixed clock
        );
        let filter = factory.create_compaction_filter(true, false);
        // Fresh entry → keep.
        let fresh = 800u64.to_le_bytes();
        assert!(matches!(
            filter.filter(0, b"k", EntryType::Put, &fresh),
            CompactionDecision::Keep
        ));
        // Expired entry → remove.
        let old = 100u64.to_le_bytes();
        assert!(matches!(
            filter.filter(0, b"k", EntryType::Put, &old),
            CompactionDecision::Remove
        ));
    }
}
