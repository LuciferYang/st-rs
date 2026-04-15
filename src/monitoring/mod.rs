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

//! Port of `monitoring/` from upstream.
//!
//! Counters, histograms, and per-thread performance context — the
//! observability layer the engine exposes to users and internal code.
//!
//! Three submodules:
//! - [`statistics`] — global tickers + histogram map (the user-facing
//!   `Statistics` object).
//! - [`histogram`] — the bucketed histogram type used inside
//!   `Statistics` and for standalone measurements.
//! - [`perf_context`] — per-thread fine-grained counters (cycle counts
//!   in upstream; ns-counts in the Rust port).
//!
//! Not ported: `instrumented_mutex.h` (use `std::sync::Mutex` + record
//! into `Statistics` at the callsite), `thread_status_updater.h` (used
//! primarily by the admin thread-status API, belongs in a higher layer).

pub mod histogram;
pub mod perf_context;
pub mod statistics;
