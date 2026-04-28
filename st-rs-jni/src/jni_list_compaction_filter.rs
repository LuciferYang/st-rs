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

//! Reverse-JNI compaction filter for Flink list state with element-level
//! TTL.
//!
//! Flink stores variable-length list serializations whose element offsets
//! are only computable by Flink's serializer. The Java side exposes a
//! `ListElementFilter` callback returning the offset of the first
//! unexpired element. We wrap that callback as a Rust [`CompactionFilter`]
//! so the engine can prune list values during compaction.
//!
//! Mirror of the [`crate::jni_fs_backend`] reverse-JNI pattern: hold
//! `Arc<JavaVM>` + `GlobalRef` and attach the calling thread on each
//! invocation.

use jni::objects::{GlobalRef, JValue};
use jni::JavaVM;
use st_rs::{CompactionDecision, CompactionFilter, CompactionFilterFactory, EntryType};
use std::sync::Arc;

/// CompactionFilter that delegates per-value pruning to a Java
/// `ListElementFilter` over JNI.
pub struct JniListCompactionFilter {
    /// Time-to-live in milliseconds (passed through to Java).
    ttl_millis: u64,
    /// Wall-clock anchor for this compaction job, in millis since epoch
    /// (passed through to Java).
    now_millis: u64,
    /// JVM handle for thread attachment.
    jvm: Arc<JavaVM>,
    /// Global reference to a Java object implementing
    /// `org.forstdb.FlinkCompactionFilter$ListElementFilter`.
    filter_ref: GlobalRef,
}

// Safety: GlobalRef and JavaVM are Send+Sync per the jni crate; all JNI
// access goes through attach_current_thread.
unsafe impl Send for JniListCompactionFilter {}
unsafe impl Sync for JniListCompactionFilter {}

impl JniListCompactionFilter {
    /// Create a new list compaction filter wrapping a Java
    /// `ListElementFilter` instance.
    pub fn new(
        ttl_millis: u64,
        now_millis: u64,
        jvm: Arc<JavaVM>,
        filter_ref: GlobalRef,
    ) -> Self {
        Self {
            ttl_millis,
            now_millis,
            jvm,
            filter_ref,
        }
    }

    /// Call `int nextUnexpiredOffset(byte[] list, long ttl, long now)`
    /// on the Java filter. Returns `None` if the JNI call fails (caller
    /// should treat as keep).
    fn next_unexpired_offset(&self, value: &[u8]) -> Option<i32> {
        let mut env = self.jvm.attach_current_thread().ok()?;
        let bytes = env.byte_array_from_slice(value).ok()?;
        let result = env
            .call_method(
                &self.filter_ref,
                "nextUnexpiredOffset",
                "([BJJ)I",
                &[
                    JValue::Object(&jni::objects::JObject::from(bytes)),
                    JValue::Long(self.ttl_millis as i64),
                    JValue::Long(self.now_millis as i64),
                ],
            )
            .ok()?;
        result.i().ok()
    }
}

impl CompactionFilter for JniListCompactionFilter {
    fn name(&self) -> &'static str {
        "FlinkListCompactionFilter"
    }

    fn filter(
        &self,
        _level: u32,
        _key: &[u8],
        _entry_type: EntryType,
        existing_value: &[u8],
    ) -> CompactionDecision {
        let offset = match self.next_unexpired_offset(existing_value) {
            Some(o) => o,
            None => return CompactionDecision::Keep,
        };
        if offset <= 0 {
            // No expired elements at the front — keep the value as-is.
            CompactionDecision::Keep
        } else if (offset as usize) >= existing_value.len() {
            // All elements expired — drop the entry.
            CompactionDecision::Remove
        } else {
            // Truncate expired prefix.
            CompactionDecision::ChangeValue(existing_value[offset as usize..].to_vec())
        }
    }
}

/// Factory wrapping a Java `ListElementFilterFactory`. Each compaction
/// job calls into Java to get a fresh `ListElementFilter`, anchors the
/// current wall-clock time, and produces a [`JniListCompactionFilter`].
pub struct JniListCompactionFilterFactory {
    ttl_millis: u64,
    jvm: Arc<JavaVM>,
    /// Global reference to the Java
    /// `org.forstdb.FlinkCompactionFilter$ListElementFilterFactory`.
    factory_ref: GlobalRef,
    /// Clock injected for testing; default uses `SystemTime::now()`.
    clock: Box<dyn Fn() -> u64 + Send + Sync>,
}

unsafe impl Send for JniListCompactionFilterFactory {}
unsafe impl Sync for JniListCompactionFilterFactory {}

impl JniListCompactionFilterFactory {
    /// Build a new factory.
    pub fn new(
        ttl_millis: u64,
        jvm: Arc<JavaVM>,
        factory_ref: GlobalRef,
        clock: Box<dyn Fn() -> u64 + Send + Sync>,
    ) -> Self {
        Self {
            ttl_millis,
            jvm,
            factory_ref,
            clock,
        }
    }
}

impl CompactionFilterFactory for JniListCompactionFilterFactory {
    fn name(&self) -> &'static str {
        "FlinkListCompactionFilterFactory"
    }

    fn create_compaction_filter(
        &self,
        _is_full_compaction: bool,
        _is_manual_compaction: bool,
    ) -> Box<dyn CompactionFilter> {
        let now = (self.clock)();
        let filter_ref = match make_list_element_filter(&self.jvm, &self.factory_ref) {
            Some(r) => r,
            None => {
                // If JNI is in a bad state, fall back to a no-op filter
                // that keeps every value. Compaction must not crash the
                // engine just because a filter callback failed.
                return Box::new(NoopListFilter);
            }
        };
        Box::new(JniListCompactionFilter::new(
            self.ttl_millis,
            now,
            Arc::clone(&self.jvm),
            filter_ref,
        ))
    }
}

/// Call `factory.createListElementFilter()` and wrap the returned object
/// in a global reference.
fn make_list_element_filter(
    jvm: &Arc<JavaVM>,
    factory_ref: &GlobalRef,
) -> Option<GlobalRef> {
    let mut env = jvm.attach_current_thread().ok()?;
    let result = env
        .call_method(
            factory_ref,
            "createListElementFilter",
            "()Lorg/forstdb/FlinkCompactionFilter$ListElementFilter;",
            &[],
        )
        .ok()?;
    let obj = result.l().ok()?;
    if obj.is_null() {
        return None;
    }
    env.new_global_ref(&obj).ok()
}

/// Fallback filter used when the Java factory cannot produce a callback;
/// keeps every entry to avoid losing data.
struct NoopListFilter;

impl CompactionFilter for NoopListFilter {
    fn name(&self) -> &'static str {
        "FlinkListCompactionFilter(noop)"
    }

    fn filter(
        &self,
        _level: u32,
        _key: &[u8],
        _entry_type: EntryType,
        _existing_value: &[u8],
    ) -> CompactionDecision {
        CompactionDecision::Keep
    }
}
