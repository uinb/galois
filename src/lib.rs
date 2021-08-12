// Copyright 2021 UINB Technologies Pte. Ltd.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![feature(type_ascription)]
#![feature(map_first_last)]
#![feature(drain_filter)]
#![allow(clippy::from_over_into)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::wrong_self_convention)]
#![allow(clippy::map_entry)]

pub mod assets;
pub mod clearing;
pub mod config;
pub mod core;
pub mod db;
pub mod event;
pub mod matcher;
pub mod orderbook;
pub mod output;
pub mod sequence;
pub mod server;
pub mod smt;
pub mod snapshot;
