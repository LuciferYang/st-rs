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

//! User-facing database API.
//!
//! Everything a user of the engine interacts with directly: the `Db` trait,
//! iterators, write batches, options structs, and snapshots. These roughly
//! correspond to the set of upstream headers a new user is told to include
//! first.

pub mod db;
pub mod iterator;
pub mod options;
pub mod snapshot;
pub mod write_batch;
