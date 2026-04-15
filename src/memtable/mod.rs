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

//! Port of `memtable/` from upstream.
//!
//! In-memory sorted key-value stores used to buffer writes before they
//! are flushed into an L0 SST. Upstream ships several backends
//! (`SkipListRep`, `HashLinkList`, `HashSkipList`, `VectorRep`);
//! Layer 3a ports only the default: a safe, **single-threaded**
//! skiplist. A Layer 4 follow-up can add a concurrent lock-free
//! variant once the engine actually needs one.
//!
//! The two key types:
//! - [`memtable_rep::MemTableRep`] — the abstract trait a
//!   memtable-like store must implement.
//! - [`skip_list::SkipList`] — a concrete safe skiplist with its own
//!   `MemTableRep` implementation.

pub mod memtable_rep;
pub mod skip_list;
