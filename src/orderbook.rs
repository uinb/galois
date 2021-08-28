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

use crate::core::{Amount, Fee, Price, Symbol, UserId};
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

impl Into<u32> for AskOrBid {
    fn into(self) -> u32 {
        match self {
            AskOrBid::Ask => 0,
            AskOrBid::Bid => 1,
        }
    }
}

impl std::convert::TryFrom<u32> for AskOrBid {
    type Error = anyhow::Error;

    fn try_from(x: u32) -> anyhow::Result<Self> {
        match x {
            0 => Ok(AskOrBid::Ask),
            1 => Ok(AskOrBid::Bid),
            _ => Err(anyhow::anyhow!("")),
        }
    }
}

impl Into<u8> for AskOrBid {
    fn into(self) -> u8 {
        match self {
            AskOrBid::Ask => 0,
            AskOrBid::Bid => 1,
        }
    }
}

impl std::ops::Not for AskOrBid {
    type Output = Self;

    fn not(self) -> Self::Output {
        match self {
            Self::Bid => Self::Ask,
            Self::Ask => Self::Bid,
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Eq, PartialEq, Clone)]
pub struct Order {
    pub id: u64,
    pub user: UserId,
    pub price: Price,
    pub unfilled: Amount,
}

impl Order {
    pub const fn new(id: u64, user: UserId, price: Price, unfilled: Amount) -> Self {
        Self {
            id,
            user,
            price,
            unfilled,
        }
    }

    pub fn fill(&mut self, delta: Amount) {
        self.unfilled -= delta;
    }

    pub fn is_filled(&self) -> bool {
        self.unfilled == Amount::ZERO
    }
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
pub struct OrderPage {
    pub orders: LinkedHashMap<u64, Order>,
    pub amount: Amount,
    pub price: Price,
}

pub type Level = (Price, Amount, Amount);

impl OrderPage {
    fn with_init_order(order: Order) -> Self {
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

    pub fn as_level(&self, base_scale: u32, quote_scale: u32, total: Amount) -> Level {
        let mut amount = self.amount;
        let mut price = self.price;
        amount.rescale(base_scale);
        price.rescale(quote_scale);
        (price, amount, total + amount)
    }

    pub fn is_empty(&self) -> bool {
        self.orders.is_empty()
    }

    pub fn decr_size(&mut self, amount: &Amount) {
        self.amount -= amount;
    }

    fn remove(&mut self, order_id: u64) -> Option<Order> {
        self.orders.remove(&order_id).map(|x| {
            self.amount -= x.unfilled;
            x
        })
    }

    fn get(&self, order_id: u64) -> Option<&Order> {
        self.orders.get(&order_id)
    }
}

pub type Tape = BTreeMap<Price, OrderPage>;

pub type Index = HashMap<u64, Price>;

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Default)]
pub struct OrderBook {
    pub asks: Tape,
    pub bids: Tape,
    pub ask_size: Amount,
    pub bid_size: Amount,
    pub indices: Index,
    pub base_scale: u32,
    pub quote_scale: u32,
    pub taker_fee: Fee,
    pub maker_fee: Fee,
    pub min_amount: Amount,
    pub min_vol: Amount,
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
        taker_fee: Fee,
        maker_fee: Fee,
        min_amount: Amount,
        min_vol: Amount,
        enable_market_order: bool,
        open: bool,
    ) -> Self {
        Self {
            asks: Tape::new(),
            bids: Tape::new(),
            ask_size: Amount::ZERO,
            bid_size: Amount::ZERO,
            indices: Index::with_capacity(DEFAULT_PAGE_SIZE),
            base_scale,
            quote_scale,
            taker_fee,
            maker_fee,
            min_amount,
            min_vol,
            enable_market_order,
            open: open,
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
            AskOrBid::Ask => {
                self.ask_size += order.unfilled;
                Self::insert_into(&mut self.asks, &mut self.indices, order)
            }
            AskOrBid::Bid => {
                self.bid_size += order.unfilled;
                Self::insert_into(&mut self.bids, &mut self.indices, order)
            }
        }
    }

    fn insert_into(tape: &mut Tape, index: &mut Index, order: Order) {
        index.insert(order.id, order.price);
        tape.entry(order.price)
            .and_modify(|page| {
                page.amount += order.unfilled;
                page.orders.insert(order.id, order.clone());
            })
            .or_insert_with(|| OrderPage::with_init_order(order));
    }

    // FIXME
    pub fn decr_size_on(&mut self, ask_or_bid: AskOrBid, amount: &Amount) {
        match ask_or_bid {
            AskOrBid::Ask => self.ask_size -= amount,
            AskOrBid::Bid => self.bid_size -= amount,
        }
    }

    pub fn remove(&mut self, order_id: u64) -> Option<(Order, AskOrBid)> {
        let price = self.indices.remove(&order_id)?;
        match (self.get_best_ask(), self.get_best_bid()) {
            (Some(best_ask), Some(_)) => {
                if price >= best_ask {
                    Self::remove_from(&mut self.asks, order_id, &price).map(|o| (o, AskOrBid::Ask))
                } else {
                    Self::remove_from(&mut self.bids, order_id, &price).map(|o| (o, AskOrBid::Bid))
                }
            }
            (None, Some(_)) => {
                Self::remove_from(&mut self.bids, order_id, &price).map(|o| (o, AskOrBid::Bid))
            }
            (Some(_), None) => {
                Self::remove_from(&mut self.asks, order_id, &price).map(|o| (o, AskOrBid::Ask))
            }
            _ => None,
        }
    }

    fn remove_from(tape: &mut Tape, order_id: u64, price: &Price) -> Option<Order> {
        let page = tape.get_mut(price)?;
        let removed = page.remove(order_id);
        if page.is_empty() {
            tape.remove(&price);
        }
        removed
    }

    pub fn get_best_if_match(
        &mut self,
        ask_or_bid: AskOrBid,
        taker_price: &Price,
    ) -> Option<OccupiedEntry<Price, OrderPage>> {
        match ask_or_bid {
            AskOrBid::Bid => self.asks.first_entry().filter(|v| taker_price >= v.key()),
            AskOrBid::Ask => self.bids.last_entry().filter(|v| taker_price <= v.key()),
        }
    }

    pub fn get_best_ask(&self) -> Option<Price> {
        self.asks.first_key_value().map(|(price, _)| *price)
    }

    pub fn get_best_bid(&self) -> Option<Price> {
        self.bids.last_key_value().map(|(price, _)| *price)
    }

    pub fn find_order(&self, order_id: u64) -> Option<&Order> {
        let price = self.indices.get(&order_id)?;
        match (self.get_best_ask(), self.get_best_bid()) {
            (Some(best_ask), Some(_)) => {
                if *price >= best_ask {
                    self.asks.get(price).and_then(|page| page.get(order_id))
                } else {
                    self.bids.get(price).and_then(|page| page.get(order_id))
                }
            }
            (None, Some(_)) => self.bids.get(price).and_then(|page| page.get(order_id)),
            (Some(_), None) => self.asks.get(price).and_then(|page| page.get(order_id)),
            _ => None,
        }
    }

    pub fn should_accept(&self, price: Price, amount: Amount) -> bool {
        return self.open
            && amount >= self.min_amount
            && price.scale() <= self.quote_scale
            && amount.scale() <= self.base_scale;
    }
}

#[test]
pub fn test_scale() {
    use rust_decimal_macros::dec;

    let mut price = dec!(1.26);
    price.rescale(4);
    assert_eq!("1.2600", price.to_string());
    let mut amount = dec!(0.0001);
    amount.rescale(2);
    assert_eq!("0.00", amount.to_string());
}

#[test]
pub fn test_orderbook() {
    use rust_decimal_macros::dec;
    let base_scale = 5;
    let quote_scale = 1;
    let taker_fee = dec!(0.001);
    let maker_fee = dec!(0.001);
    let min_amount = dec!(1);
    let min_vol = dec!(1);
    let mut book = OrderBook::new(
        base_scale,
        quote_scale,
        taker_fee,
        maker_fee,
        min_amount,
        min_vol,
        true,
        true,
    );
    book.insert(
        Order::new(1, UserId::zero(), dec!(100), dec!(1)),
        AskOrBid::Bid,
    );
    assert!(book.indices.contains_key(&1));
    assert_eq!(book.bid_size, dec!(1));
    assert_eq!(book.ask_size, dec!(0));
    assert!(!book.bids.is_empty());
    assert!(book.asks.is_empty());
    assert_eq!(book.get_best_bid().unwrap(), dec!(100));
    assert!(book.get_best_ask().is_none());
    assert!(book.find_order(1).is_some());
    book.insert(
        Order::new(2, UserId::zero(), dec!(105), dec!(1)),
        AskOrBid::Ask,
    );
    assert!(book.indices.contains_key(&2));
    assert_eq!(book.ask_size, dec!(1));
    assert!(book.find_order(2).is_some());
    assert!(!book.asks.is_empty());
    assert_eq!(book.get_best_ask().unwrap(), dec!(105));
}
