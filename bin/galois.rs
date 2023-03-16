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

use clap::Parser;
use galois::{config, executor, fusotao, output, sequence, server, snapshot};
use std::sync::{atomic, mpsc, Arc};

fn start() {
    let (id, coredump) = snapshot::load().unwrap();
    let (output_tx, output_rx) = mpsc::channel();
    let (event_tx, event_rx) = mpsc::channel();
    let (proof_tx, proof_rx) = mpsc::channel();
    output::init(output_rx);
    fusotao::init(proof_rx);
    executor::init(event_rx, output_tx, proof_tx, coredump);
    let ready = Arc::new(atomic::AtomicBool::new(false));
    sequence::init(event_tx.clone(), id, ready.clone());
    server::init(event_tx, ready);
}

fn main() {
    lazy_static::initialize(&config::C);
    let opts = config::GaloisCli::parse();
    match opts.sub {
        Some(config::SubCmd::EncryptConfig) => config::print_config(config::C.clone()).unwrap(),
        None => start(),
    }
}
