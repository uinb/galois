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

fn print_banner() {
    println!(
        r#"
        **       **
   *******     ******     **               **    ******
  ***               **    **     *****     **   **    *
 **              *****    **   ***   ***       **
 **            *******    **   **     **   **   **
 **    *****  **    **    **   *       *   **     *****
  **     ***  **    **    **   **     **   **         **
   *********   **  ****   **    *******    **    **   **
      *    *    ****  *   **      ***      **     ****"#
    );
}

/// Overview:
///
///           sidecar   chain <-+
///             ^         |      \
///             |         |       \
///             v         v        \
///   +---->  server   scanner      +
///   |          \       /          |
///   |\          \     /           |
///   | \          \   /            |
///   |  +--     sequencer          |
///   +              |              |
///   |\             |              |
///   | \            v              |
///   |  +--      executor          |
///   |              |              |
///   |              |              |
///   |              v              |
///   +           storage           |
///    \           /   \            +
///     \         /     \          /
///      \       /       \        /
///       +-- replyer committer -+
///
fn start() {
    let (id, coredump) = snapshot::load().unwrap();
    let (connector, state) = fusotao::sync().unwrap();
    let shared = Shared::new(state.clone(), C.fusotao.get_x25519());
    let (output_tx, output_rx) = std::sync::mpsc::channel();
    let (event_tx, event_rx) = std::sync::mpsc::channel();
    let (input_tx, input_rx) = std::sync::mpsc::channel();
    let (reply_tx, reply_rx) = std::sync::mpsc::channel();
    output::init(output_rx);
    committer::init(connector.clone(), state.clone());
    executor::init(event_rx, output_tx, reply_tx.clone(), coredump);
    sequencer::init(input_rx, event_tx, reply_tx, id);
    scanner::init(input_tx.clone(), connector, state);
    server::init(reply_rx, input_tx, shared);
}

fn main() {
    env_logger::init();
    let opts = config::GaloisCli::parse();
    match opts.sub {
        Some(config::SubCmd::EncryptConfig) => config::print_config(&opts.file).unwrap(),
        None => {
            print_banner();
            lazy_static::initialize(&C);
            if C.dry_run.is_some() {
                log::info!("running in dry-run mode");
            }
            start();
        }
    }
}
