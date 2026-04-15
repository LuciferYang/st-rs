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

//! User extension points.
//!
//! These are the traits the user plugs concrete implementations into to
//! customise the engine: the key ordering, block cache, SST factory,
//! filter policy, merge operator, and compaction filter. Every engine
//! layer above this one parameterises over these traits.

pub mod cache;
pub mod compaction_filter;
pub mod comparator;
pub mod filter_policy;
pub mod merge_operator;
pub mod table;
