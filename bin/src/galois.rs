// Copyright 2021-2023 UINB Technologies Pte. Ltd.

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
use galois_engine::core::Data;
use galois_engine::{
    config, executor, fusotao, output, sequence, server, shared::Shared, snapshot,
};
use std::sync::{atomic, mpsc, Arc};

fn start() {
    let (id, coredump) = snapshot::load().unwrap();
    print_symbols(&coredump);
    let (output_tx, output_rx) = mpsc::channel();
    let (event_tx, event_rx) = mpsc::channel();
    let (proof_tx, proof_rx) = mpsc::channel();
    let (msg_tx, msg_rx) = mpsc::channel();
    output::init(output_rx);
    let fuso = fusotao::init(proof_rx);
    let shared = Shared::new(fuso.state, config::C.fusotao.get_x25519_prikey().unwrap());
    executor::init(event_rx, output_tx, proof_tx, msg_tx, coredump);
    let ready = Arc::new(atomic::AtomicBool::new(false));
    sequence::init(event_tx.clone(), id, ready.clone());
    server::init(event_tx, msg_rx, shared, ready);
}

fn print_symbols(data: &Data) {
    for k in &data.orderbooks {
        log::info!(
            "base:{}, quote:{}, base_scale:{},quote_scale: {}, minbase:{}, minquote: {}",
            k.0 .0,
            k.0 .1,
            k.1.base_scale,
            k.1.quote_scale,
            k.1.min_amount,
            k.1.min_vol
        );
    }
}

fn main() {
    let opts = config::GaloisCli::parse();
    match opts.sub {
        Some(config::SubCmd::EncryptConfig) => config::print_config(&opts.file).unwrap(),
        None => {
            lazy_static::initialize(&config::C);
            start();
        }
    }
}
