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

mod assets;
mod clearing;
mod config;
mod core;
mod db;
mod event;
mod matcher;
mod orderbook;
mod output;
mod sequence;
mod server;
mod snapshot;

use lazy_static;
use std::sync::{atomic, mpsc, Arc};

fn main() {
    lazy_static::initialize(&config::C);
    lazy_static::initialize(&config::ENABLE_START_FROM_GENESIS);
    let (id, coredump) = snapshot::load().unwrap();
    let (output_tx, output_rx) = mpsc::channel();
    let (event_tx, event_rx) = mpsc::channel();
    let ready = Arc::new(atomic::AtomicBool::new(false));
    output::init(output_tx.clone(), output_rx);
    event::init(event_rx, output_tx, coredump);
    sequence::init(event_tx.clone(), id, ready.clone());
    server::init(event_tx, ready);
}
