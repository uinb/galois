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

use crate::core::Symbol;
use linked_hash_map::LinkedHashMap;
use rust_decimal::{prelude::Zero, Decimal};
use serde::{Deserialize, Serialize};
use std::collections::{
    btree_map::OccupiedEntry,
    {BTreeMap, HashMap},
};

const DEFAULT_PAGE_SIZE: usize = 256;

#[derive(Debug, Eq, PartialEq, Clone, Copy, Deserialize, Serialize)]
pub enum AskOrBid {
    Ask,
    Bid,
}

#[derive(Deserialize, Serialize, Debug, Eq, PartialEq, Clone)]
pub struct Order {
    pub id: u64,
    pub user: u64,
    pub price: Decimal,
    pub unfilled: Decimal,
}

impl Order {
    pub const fn new(id: u64, user: u64, price: Decimal, unfilled: Decimal) -> Self {
        Self {
            id,
            user,
            price,
            unfilled,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
pub struct OrderPage {
    pub orders: LinkedHashMap<u64, Order>,
    pub amount: Decimal,
    pub price: Decimal,
}

pub type Level = (Decimal, Decimal, Decimal);

impl OrderPage {
    pub fn with_init_order(order: Order) -> Self {
        let amount = order.unfilled;
        let price = order.price;
        let mut orders = LinkedHashMap::<u64, Order>::new();
        orders.insert(order.id, order);
        Self {
            orders,
            amount,
            price,
        }
    }

    pub fn as_level(&self, base_scale: u32, quote_scale: u32, total: Decimal) -> Level {
        let mut amount = self.amount;
        let mut price = self.price;
        amount.rescale(base_scale);
        price.rescale(quote_scale);
        (price, amount, total + amount)
    }

    pub fn is_empty(&self) -> bool {
        self.orders.is_empty()
    }

    pub fn remove(&mut self, order_id: u64) -> Option<Order> {
        self.orders.remove(&order_id).map(|x| {
            self.amount -= x.unfilled;
            x
        })
    }

    pub fn get(&self, order_id: u64) -> Option<&Order> {
        self.orders.get(&order_id)
    }
}

pub type Tape = BTreeMap<Decimal, OrderPage>;

pub type Index = HashMap<u64, Decimal>;

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct OrderBook {
    pub asks: Tape,
    pub bids: Tape,
    pub indices: Index,
    pub base_scale: u32,
    pub quote_scale: u32,
    pub taker_fee: Decimal,
    pub maker_fee: Decimal,
    pub min_amount: Decimal,
    pub min_vol: Decimal,
    pub enable_market_order: bool,
    pub open: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct Depth {
    pub asks: Vec<Level>,
    pub bids: Vec<Level>,
    pub depth: usize,
    pub symbol: Symbol,
}

impl OrderBook {
    pub fn new(
        base_scale: u32,
        quote_scale: u32,
        taker_fee: Decimal,
        maker_fee: Decimal,
        min_amount: Decimal,
        min_vol: Decimal,
        enable_market_order: bool,
    ) -> Self {
        Self {
            asks: Tape::new(),
            bids: Tape::new(),
            indices: Index::with_capacity(DEFAULT_PAGE_SIZE),
            base_scale,
            quote_scale,
            taker_fee,
            maker_fee,
            min_amount,
            min_vol,
            enable_market_order,
            open: true,
        }
    }

    pub fn as_depth(&self, level: usize, symbol: Symbol) -> Depth {
        let mut asks = Vec::<Level>::new();
        let mut bids = Vec::<Level>::new();
        let mut ask_total = Decimal::zero();
        for (_, ask) in self.asks.iter().take(level) {
            let level = ask.as_level(self.base_scale, self.quote_scale, ask_total);
            ask_total = level.2;
            asks.push(level);
        }
        let mut bid_total = Decimal::zero();
        for (_, bid) in self.bids.iter().rev().take(level) {
            let level = bid.as_level(self.base_scale, self.quote_scale, bid_total);
            bid_total = level.2;
            bids.push(level);
        }
        Depth {
            asks,
            bids,
            depth: level,
            symbol,
        }
    }

    pub fn insert(&mut self, order: Order, ask_or_bid: AskOrBid) {
        match ask_or_bid {
            AskOrBid::Ask => Self::_insert(&mut self.asks, &mut self.indices, order),
            AskOrBid::Bid => Self::_insert(&mut self.bids, &mut self.indices, order),
        }
    }

    fn _insert(tape: &mut Tape, index: &mut Index, order: Order) {
        index.insert(order.id, order.price);
        tape.entry(order.price)
            .and_modify(|page| {
                page.amount += order.unfilled;
                page.orders.insert(order.id, order.clone());
            })
            .or_insert_with(|| OrderPage::with_init_order(order));
    }

    pub fn remove_from_tape(tape: &mut Tape, order_id: u64, price: Decimal) -> Option<Order> {
        let page = tape.get_mut(&price)?;
        let removed = page.remove(order_id);
        if page.is_empty() {
            tape.remove(&price);
        }
        removed
    }

    pub fn get_best_match(
        &mut self,
        ask_or_bid: AskOrBid,
    ) -> Option<OccupiedEntry<Decimal, OrderPage>> {
        match ask_or_bid {
            AskOrBid::Bid => self.asks.first_entry(),
            AskOrBid::Ask => self.bids.last_entry(),
        }
    }

    pub fn get_best_ask(&self) -> Option<Decimal> {
        self.asks.first_key_value().map(|(price, _)| *price)
    }

    pub fn get_best_bid(&self) -> Option<Decimal> {
        self.bids.last_key_value().map(|(price, _)| *price)
    }

    pub fn find_order(&self, order_id: u64) -> Option<&Order> {
        let price = self.indices.get(&order_id)?;
        let best_ask = self.get_best_ask()?;
        if *price >= best_ask {
            self.asks.get(price).and_then(|page| page.get(order_id))
        } else {
            let best_bid = self.get_best_bid()?;
            if *price <= best_bid {
                self.bids.get(price).and_then(|page| page.get(order_id))
            } else {
                None
            }
        }
    }
}

#[test]
pub fn test_scale() {
    let mut price = Decimal::new(126, 2);
    price.rescale(4);
    assert_eq!("1.2600", price.to_string());
    let mut amount = Decimal::new(1, 4);
    amount.rescale(2);
    assert_eq!("0.00", amount.to_string());
}
