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

use galois::{config, event, output, sequence, server, snapshot};
use std::sync::{atomic, mpsc, Arc};

fn main() {
    print_banner();
    lazy_static::initialize(&config::C);
    let (id, coredump) = snapshot::load().unwrap();
    let (output_tx, output_rx) = mpsc::channel();
    let (event_tx, event_rx) = mpsc::channel();
    output::init(output_tx.clone(), output_rx);
    let handler_ready = Arc::new(atomic::AtomicBool::new(false));
    event::init(event_rx, output_tx, coredump, handler_ready.clone());
    while !handler_ready.load(atomic::Ordering::Relaxed) {
        std::thread::sleep(std::time::Duration::from_millis(500));
    }
    let source_ready = Arc::new(atomic::AtomicBool::new(false));
    sequence::init(event_tx.clone(), id, source_ready.clone());
    server::init(event_tx, source_ready);
}

fn print_banner() {
    const BANNER: &str = r#"
                 **       **
   *******     ******     **               **
  ***               **    **     *****     **    ******
 **              *****    **   ***   ***        **    *
 **            *******    **   **     **   **   **
 **    *****  **    **    **   *       *   **    **
  **     ***  **    **    **   **     **   **     ****
   *********   **  ****   **    *******    **        **
      *    *    ****  *   **      ***      **    ** ***
                                                  ****
"#;
    println!("{}", BANNER);
}
