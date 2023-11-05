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

use crate::fusotao::ToBlockChainNumeric;
use crate::{cmd::*, config::C, core::*, db::DB, input::*, orderbook::AskOrBid};
use anyhow::{anyhow, ensure};
use mysql::{prelude::*, *};
use rocksdb::{Direction, IteratorMode, Options, DB};
use serde::{Deserialize, Serialize};
use std::{
    convert::{TryFrom, TryInto},
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::Sender,
        Arc,
    },
    time::{Duration, SystemTime},
};

pub fn init(
    rx: Receiver<Input>,
    to_executor: Sender<Event>,
    to_server: Sender<(u64, Message)>,
    init_at: u64,
) {
    let current_id = ensure_fully_loaded(init_at, tx.clone());
    std::thread::spawn(move || -> anyhow::Result<()> {
        let mut current_id = current_id;
        loop {
            let mut input = rx.recv()?;
            let (session, req_id) = (input.session, input.req_id);
            input.sequence = current_id;
            if let Ok(event) = input.try_into() {
                current_id += 1;
                // TODO save to rocksdb
                to_executor.send(event)?;
            } else {
                to_server.send((session, Message::new(req_id, v)))?;
            }
        }
    });
}

fn ensure_fully_loaded(init_at: u64, tx: Sender<Event>) -> u64 {
    // TODO load all events from rocksdb starting from init_at then return the expected sequence id
    init_at
}

#[cfg(test)]
mod test {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    pub fn test_serialize() {
        assert_eq!("{}", serde_json::to_string(&Accounts::new()).unwrap());
        let mut account = Account::default();
        account.insert(
            100,
            crate::assets::Balance {
                available: Amount::new(200, 1),
                frozen: Amount::new(0, 0),
            },
        );
        assert_eq!(
            r#"{"100":{"available":"20.0","frozen":"0"}}"#,
            serde_json::to_string(&account).unwrap()
        );
        assert_eq!(
            r#"{"available":"0","frozen":"0"}"#,
            serde_json::to_string(&crate::assets::Balance {
                available: Amount::new(0, 0),
                frozen: Amount::new(0, 0),
            })
            .unwrap()
        );

        let mut data = Data::new();
        let orderbook = crate::orderbook::OrderBook::new(
            8,
            8,
            dec!(0.001),
            dec!(0.001),
            dec!(0.001),
            dec!(0.001),
            1,
            dec!(0.1),
            dec!(0.1),
            true,
            true,
        );
        data.orderbooks.insert((0, 1), orderbook);
    }

    #[test]
    pub fn test_deserialize_cmd() {
        let transfer_in = r#"{"currency":100, "amount":"100.0", "user_id":"5Ccr8Qcp6NBMCvdUHSoqDaQMJHnA5PAC879NbWkzaiUwBdMm", "cmd":11, "block_number":1000, "extrinsic_hash":""}"#;
        let e = serde_json::from_str::<Command>(transfer_in).unwrap();
        let s: anyhow::Result<Event> = Sequence {
            id: 1,
            cmd: e,
            status: 0,
            timestamp: 0,
        }
        .try_into();
        assert!(s.is_ok());
        let bid_limit = r#"{"quote":100, "base":101, "cmd":1, "price":"10.0", "amount":"0.5", "order_id":1, "user_id":"5Ccr8Qcp6NBMCvdUHSoqDaQMJHnA5PAC879NbWkzaiUwBdMm","nonce":1,"signature":""}"#;
        let e = serde_json::from_str::<Command>(bid_limit).unwrap();
        let s: anyhow::Result<Event> = Sequence {
            id: 2,
            cmd: e,
            status: 0,
            timestamp: 0,
        }
        .try_into();
        assert!(s.is_ok());
    }
}
