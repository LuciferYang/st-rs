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

//! Port of `logging/` from upstream.
//!
//! Internal logging used by the engine for info, warning, and error
//! messages. Not to be confused with the write-ahead log (WAL), which
//! lives in the `db/` layer.
//!
//! Upstream ships `logging/logging.h` (the `ROCKS_LOG_*` macros),
//! `auto_roll_logger.h` (file rotation), and `event_logger.h` (structured
//! JSON). Layer 1 ports only the minimal `Logger` trait plus a
//! `ConsoleLogger` implementation; rotation and structured events belong
//! in a higher layer once there's a real logging sink.

pub mod logger;
