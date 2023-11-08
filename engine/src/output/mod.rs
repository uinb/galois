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

use crate::{
    core::*,
    matcher::*,
    orderbook::{AskOrBid, Depth},
};
use std::{
    collections::HashMap,
    convert::Into,
    sync::mpsc::{Receiver, RecvTimeoutError},
    time::Duration,
};

#[derive(Debug, Clone)]
pub struct Output {
    pub event_id: u64,
    pub order_id: u64,
    pub user_id: UserId,
    pub symbol: Symbol,
    pub state: State,
    pub role: Role,
    pub ask_or_bid: AskOrBid,
    pub price: Price,
    pub quote_charge: Amount,
    pub quote_delta: Amount,
    pub quote_available: Amount,
    pub quote_frozen: Amount,
    pub base_charge: Amount,
    pub base_delta: Amount,
    pub base_available: Amount,
    pub base_frozen: Amount,
    pub timestamp: u64,
}

// pub fn write_depth(depth: Vec<Depth>) {
//     if crate::config::C.dry_run.is_some() {
//         return;
//     }
//     let redis = REDIS.get_connection();
//     match redis {
//         Ok(mut conn) => {
//             depth.iter().for_each(|d| {
//                 let r: redis::RedisResult<()> = conn.set(
//                     format!("V2_DEPTH_L{}_{}_{}", d.depth, d.symbol.0, d.symbol.1),
//                     serde_json::to_string(d).unwrap(),
//                 );
//                 if r.is_err() {
//                     log::error!("{:?}", r);
//                 }
//             });
//         }
//         Err(_) => {
//             log::error!("connect redis failed");
//         }
//     }
// }

pub fn init(rx: Receiver<Vec<Output>>) {
    let mut buf = HashMap::<Symbol, (u64, Vec<Output>)>::new();
    std::thread::spawn(move || loop {
        let cr = match rx.recv_timeout(Duration::from_millis(50)) {
            Ok(p) => p,
            Err(RecvTimeoutError::Timeout) => vec![],
            Err(RecvTimeoutError::Disconnected) => {
                log::error!("Output persistence thread interrupted!");
                break;
            }
        };
        // if crate::config::C.dry_run.is_none() {
        //     if cr.is_empty() {
        //         flush_all(&mut buf);
        //     } else {
        //         write(cr, &mut buf);
        //     }
        // }
    });
    log::info!("dumper initialized");
}
