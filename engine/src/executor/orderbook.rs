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

use crate::core::{Amount, Fee, OrderId, Price, UserId};
use linked_hash_map::LinkedHashMap;
use rust_decimal::prelude::Zero;
use serde::{Deserialize, Serialize};
use std::collections::{btree_map::OccupiedEntry, BTreeMap, HashMap};

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
            crate::cmd::ASK_LIMIT => Ok(AskOrBid::Ask),
            crate::cmd::BID_LIMIT => Ok(AskOrBid::Bid),
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
    pub id: OrderId,
    pub user: UserId,
    pub price: Price,
    pub unfilled: Amount,
}

impl Order {
    pub const fn new(id: OrderId, user: UserId, price: Price, unfilled: Amount) -> Self {
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
    pub orders: LinkedHashMap<OrderId, Order>,
    pub amount: Amount,
    pub price: Price,
}

pub type Level = (Price, Amount, Amount);

impl OrderPage {
    fn with_init_order(order: Order) -> Self {
        let amount = order.unfilled;
        let price = order.price;
        let mut orders = LinkedHashMap::<OrderId, Order>::new();
        orders.insert(order.id, order);
        Self {
            orders,
            amount,
            price,
        }
    }

    pub fn merge(&self, base_scale: u32, quote_scale: u32, total: Amount) -> Level {
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

    fn remove(&mut self, order_id: OrderId) -> Option<Order> {
        self.orders.remove(&order_id).map(|x| {
            self.amount -= x.unfilled;
            x
        })
    }

    fn get(&self, order_id: OrderId) -> Option<&Order> {
        self.orders.get(&order_id)
    }
}

pub type Tape = BTreeMap<Price, OrderPage>;

pub type Index = HashMap<OrderId, Price>;

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Default)]
pub struct OrderBook {
    pub asks: Tape,
    pub bids: Tape,
    pub indices: Index,
    pub base_scale: u32,
    pub quote_scale: u32,
    pub taker_fee: Fee,
    pub maker_fee: Fee,
    pub base_taker_fee: Fee,
    pub base_maker_fee: Fee,
    pub fee_times: u32,
    pub min_amount: Amount,
    pub min_vol: Amount,
    pub enable_market_order: bool,
    pub open: bool,
    pub max_id: OrderId,
}

impl OrderBook {
    pub fn new(
        base_scale: u32,
        quote_scale: u32,
        taker_fee: Fee,
        maker_fee: Fee,
        base_taker_fee: Fee,
        base_maker_fee: Fee,
        fee_times: u32,
        min_amount: Amount,
        min_vol: Amount,
        enable_market_order: bool,
        open: bool,
    ) -> Self {
        Self {
            asks: Tape::new(),
            bids: Tape::new(),
            indices: Index::with_capacity(DEFAULT_PAGE_SIZE),
            base_scale,
            quote_scale,
            taker_fee,
            maker_fee,
            base_taker_fee,
            base_maker_fee,
            fee_times,
            min_amount,
            min_vol,
            enable_market_order,
            open,
            // since we always use incr then fetch, so the first order is 1
            max_id: 0,
        }
    }

    pub fn incr_then_fetch_order_id(&mut self) -> OrderId {
        self.max_id += 1;
        self.max_id
    }

    pub fn size(&self) -> (Amount, Amount) {
        (
            self.asks.values().fold(Amount::zero(), |x, a| x + a.amount),
            self.bids.values().fold(Amount::zero(), |x, b| x + b.amount),
        )
    }

    pub fn insert(&mut self, order: Order, ask_or_bid: AskOrBid) {
        match ask_or_bid {
            AskOrBid::Ask => Self::insert_into(&mut self.asks, &mut self.indices, order),
            AskOrBid::Bid => Self::insert_into(&mut self.bids, &mut self.indices, order),
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

    pub fn remove(&mut self, order_id: OrderId) -> Option<(Order, AskOrBid)> {
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

    fn remove_from(tape: &mut Tape, order_id: OrderId, price: &Price) -> Option<Order> {
        let page = tape.get_mut(price)?;
        let removed = page.remove(order_id);
        if page.is_empty() {
            tape.remove(&price);
        }
        removed
    }

    fn get_size_from(tape: &Tape, price: &Price) -> Option<Amount> {
        tape.get(price).map(|page| page.amount)
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

    pub fn get_page_size(&self, price: &Price) -> Option<Amount> {
        match (self.get_best_ask(), self.get_best_bid()) {
            (Some(best_ask), Some(_)) => {
                if price >= &best_ask {
                    Self::get_size_from(&self.asks, price)
                } else {
                    Self::get_size_from(&self.bids, price)
                }
            }
            (None, Some(_)) => Self::get_size_from(&self.bids, price),
            (Some(_), None) => Self::get_size_from(&self.asks, price),
            _ => None,
        }
    }

    pub fn get_size_of_best(&self) -> (Option<(Price, Amount)>, Option<(Price, Amount)>) {
        (
            self.asks
                .first_key_value()
                .map(|(price, v)| (*price, v.amount)),
            self.bids
                .last_key_value()
                .map(|(price, v)| (*price, v.amount)),
        )
    }

    pub fn find_order(&self, order_id: OrderId) -> Option<&Order> {
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
        self.open
            && amount >= self.min_amount
            && price.scale() <= self.quote_scale
            && amount.scale() <= self.base_scale
    }
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
        taker_fee,
        maker_fee,
        1,
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
    assert_eq!(book.size(), (dec!(0), dec!(1)));
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
    assert_eq!(book.size().0, dec!(1));
    assert!(book.find_order(2).is_some());
    assert!(!book.asks.is_empty());
    assert_eq!(book.get_best_ask().unwrap(), dec!(105));
}
