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

//! Port of `table/block_based/` from upstream.
//!
//! Layer 3a ships only the data-block primitives:
//! - [`block_builder::BlockBuilder`] — accumulates key-value records
//!   with prefix compression and restart markers, then emits the
//!   encoded block.
//! - [`block::Block`] + [`block::BlockIter`] — parses an encoded
//!   block and provides seek/next/prev iteration.
//!
//! Layer 3b will add `BlockBasedTableBuilder` and
//! `BlockBasedTableReader`, which stitch data blocks into a full SST
//! together with an index block, metaindex, filter, and footer.

pub mod block;
pub mod block_builder;
pub mod filter_block;
pub mod sst_iterator;
pub mod table_builder;
pub mod table_reader;
