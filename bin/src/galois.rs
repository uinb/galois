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
use engine::*;
use std::sync::mpsc::channel;

/// Overview:
///
///           sidecar   chain <-+
///             |         |      \
///             |         |       \
///             v         v        \
///   +-----> server   scanner      +
///   |          \       /          |
///   |\          \     /           |
///   | \          \   /            |
///   |  +-<-    sequencer          |
///   +              |              |
///   |\             |              |
///   | \            v              |
///   |  +-<-    executor           |
///   +            /   \            +
///    \          /     \          /
///     \        /       \        /
///      +-<- output   prover ->-+
///
fn start() {
    let (id, coredump) = snapshot::load().unwrap();
    let (output_tx, output_rx) = channel();
    let (event_tx, event_rx) = channel();
    let (proof_tx, proof_rx) = channel();
    let (input_tx, input_rx) = channel();
    let (reply_tx, reply_rx) = channel();
    let prover = fusotao::init(proof_rx);
    let shared = shared::Shared::new(prover.state, config::C.fusotao.get_x25519_prikey());
    output::init(output_rx);
    executor::init(event_rx, output_tx, proof_tx, reply_tx.clone(), coredump);
    sequencer::init(input_rx, event_tx, reply_tx, id);
    server::init(msg_rx, input_tx, shared);
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
