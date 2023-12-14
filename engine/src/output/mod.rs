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

use crate::core::*;
use crate::executor::orderbook::Level;
use rust_decimal::{prelude::Zero, Decimal};
use serde::{Deserialize, Serialize};

pub mod market;

#[derive(Debug, Clone)]
pub struct Output {
    pub event_id: u64,
    pub order_id: u64,
    pub user_id: UserId,
    pub symbol: Symbol,
    pub state: OrderState,
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

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct Depth {
    pub asks: Vec<Level>,
    pub bids: Vec<Level>,
    pub symbol: Symbol,
}

impl From<(Symbol, &OrderBook)> for Depth {
    fn from(orderbook: (Symbol, &OrderBook)) -> Self {
        let mut asks = Vec::<Level>::new();
        let mut bids = Vec::<Level>::new();
        let mut ask_total = Decimal::zero();
        for (_, ask) in orderbook.1.asks.iter() {
            let level = ask.merge(orderbook.1.base_scale, orderbook.1.quote_scale, ask_total);
            ask_total = level.2;
            asks.push(level);
        }
        let mut bid_total = Decimal::zero();
        for (_, bid) in orderbook.1.bids.iter().rev() {
            let level = bid.merge(orderbook.1.base_scale, orderbook.1.quote_scale, bid_total);
            bid_total = level.2;
            bids.push(level);
        }
        Depth {
            asks,
            bids,
            symbol: orderbook.0,
        }
    }
}
