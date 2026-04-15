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

//! Universal primitives used everywhere else in the crate.
//!
//! This module groups the three types that every other file references:
//! [`status::Status`], [`slice::Slice`], and the primitive aliases in
//! [`types`]. Keeping them together in `core::` makes it obvious that they
//! are foundational and have no internal dependencies (not even on `port`).

pub mod slice;
pub mod status;
pub mod types;
