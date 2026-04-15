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

//! Port of `cache/` from upstream.
//!
//! Concrete implementations of the Layer 0 [`Cache`](crate::ext::cache::Cache)
//! trait. Layer 2 ships a single-shard LRU cache; sharded and
//! HyperClock variants are deferred to Layer 2b.

pub mod lru;
