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

use crate::{config::C, input::*, output::*};
use rust_decimal::{prelude::Zero, Decimal};
use std::sync::mpsc::{Receiver, Sender};

type MarketChannel = Receiver<Vec<Output>>;
type ResponseChannel = Sender<(u64, Message)>;

pub fn init(rx: MarketChannel, tx: ResponseChannel) {
    std::thread::spawn(move || -> anyhow::Result<()> {
        loop {
            let crs = rx.recv()?;
            if C.dry_run.is_none() {}
        }
    });
    log::info!("market initialized");
}
