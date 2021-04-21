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


use crate::orderbook::{AskOrBid, Order, OrderBook, OrderPage};
use rust_decimal::{prelude::Zero, Decimal};

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum State {
    Submitted,
    Canceled,
    Filled,
    PartialFilled,
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum Role {
    Taker,
    Maker,
}

pub trait IntoCode {

    fn into_code(self) -> u32;
}

impl IntoCode for Role {
    fn into_code(self) -> u32 {
        match self {
            Role::Maker => 0,
            Role::Taker => 1,
        }
    }
}

impl IntoCode for AskOrBid {
    fn into_code(self) -> u32 {
        match self {
            AskOrBid::Ask => 0,
            AskOrBid::Bid => 1,
        }
    }
}

impl IntoCode for State {
    fn into_code(self) -> u32 {
        match self {
            State::Submitted => 0,
            State::Canceled => 1,
            State::Filled => 2,
            State::PartialFilled => 3,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Taker {
    pub user_id: u64,
    pub order_id: u64,
    pub price: Decimal,
    pub unfilled: Decimal,
    pub ask_or_bid: AskOrBid,
    pub state: State,
}

impl Taker {
    pub fn taker_filled(user_id: u64, order_id: u64, price: Decimal, ask_or_bid: AskOrBid) -> Self {
        Self {
            user_id: user_id,
            order_id: order_id,
            price: price,
            unfilled: Decimal::zero(),
            ask_or_bid: ask_or_bid,
            state: State::Filled,
        }
    }

    pub fn taker_placed(
        user_id: u64,
        order_id: u64,
        price: Decimal,
        unfilled: Decimal,
        ask_or_bid: AskOrBid,
    ) -> Self {
        Self {
            user_id: user_id,
            order_id: order_id,
            price: price,
            unfilled: unfilled,
            ask_or_bid: ask_or_bid,
            state: State::PartialFilled,
        }
    }

    pub fn cancel(
        user_id: u64,
        order_id: u64,
        price: Decimal,
        unfilled: Decimal,
        ask_or_bid: AskOrBid,
    ) -> Self {
        Self {
            user_id: user_id,
            order_id: order_id,
            price: price,
            unfilled: unfilled,
            ask_or_bid: ask_or_bid,
            state: State::Canceled,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Maker {
    pub user_id: u64,
    pub order_id: u64,
    pub price: Decimal,
    pub filled: Decimal,
    pub state: State,
}

impl Maker {
    pub fn maker_filled(user_id: u64, order_id: u64, price: Decimal, filled: Decimal) -> Self {
        Self {
            user_id: user_id,
            order_id: order_id,
            price: price,
            filled: filled,
            state: State::Filled,
        }
    }

    pub fn maker_so_far(user_id: u64, order_id: u64, price: Decimal, filled: Decimal) -> Self {
        Self {
            user_id: user_id,
            order_id: order_id,
            price: price,
            filled: filled,
            state: State::PartialFilled,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Match {
    pub maker: Vec<Maker>,
    pub taker: Taker,
}

// fn ask_market(book: &mut OrderBook, order_id: &str, vol: Decimal) -> Vec<Match> {
//     vec![]
// }

// fn bid_market(book: &mut OrderBook, order_id: &str, vol: Decimal) -> Vec<Match> {
//     vec![]
// }

pub fn execute_limit(
    book: &mut OrderBook,
    user_id: u64,
    order_id: u64,
    price: Decimal,
    amount: Decimal,
    ask_or_bid: AskOrBid,
) -> Option<Match> {
    let mut makers = Vec::<Maker>::new();
    let mut unfilled = amount;
    loop {
        if unfilled == Decimal::zero() {
            return match !makers.is_empty() {
                true => Some(Match {
                    maker: makers,
                    taker: Taker::taker_filled(user_id, order_id, price, ask_or_bid),
                }),
                false => None,
            };
        }
        let best = book.get_best_match(&ask_or_bid);
        if best.is_none() {
            let order = Order::new(order_id, user_id, price, unfilled);
            book.insert(order, &ask_or_bid);
            return match !makers.is_empty() {
                true => Some(Match {
                    maker: makers,
                    taker: Taker::taker_placed(user_id, order_id, price, unfilled, ask_or_bid),
                }),
                false => None,
            };
        }
        let mut best = best.unwrap();
        if !can_trade(*best.key(), price, &ask_or_bid) {
            let order = Order::new(order_id, user_id, price, unfilled);
            book.insert(order, &ask_or_bid);
            return match !makers.is_empty() {
                true => Some(Match {
                    maker: makers,
                    taker: Taker::taker_placed(user_id, order_id, price, unfilled, ask_or_bid),
                }),
                false => None,
            };
        }
        let page = best.get_mut();
        let (remain, mut v) = take(page, unfilled);
        if page.is_empty() {
            best.remove();
        }
        v.iter().filter(|m| m.state == State::Filled).for_each(|m| {
            book.indices.remove(&m.order_id);
        });
        unfilled = remain;
        makers.append(&mut v);
    }
}

fn take(page: &mut OrderPage, taker: Decimal) -> (Decimal, Vec<Maker>) {
    let mut taker = taker;
    let mut matches = Vec::<Maker>::new();
    while taker != Decimal::zero() && !page.is_empty() {
        let mut oldest = page.orders.entries().next().unwrap();
        if taker >= oldest.get().unfilled {
            let maker = oldest.get();
            matches.push(Maker::maker_filled(
                maker.user,
                maker.id,
                maker.price,
                maker.unfilled,
            ));
            taker -= maker.unfilled;
            page.amount -= maker.unfilled;
            oldest.remove();
            continue;
        }
        let maker = oldest.get_mut();
        matches.push(Maker::maker_so_far(
            maker.user,
            maker.id,
            maker.price,
            taker,
        ));
        maker.unfilled -= taker;
        page.amount -= taker;
        taker = Decimal::zero();
    }
    (taker, matches)
}

fn can_trade(best_price: Decimal, taker_price: Decimal, ask_or_bid: &AskOrBid) -> bool {
    match ask_or_bid {
        &AskOrBid::Ask => best_price >= taker_price,
        &AskOrBid::Bid => best_price <= taker_price,
    }
}

pub fn cancel(book: &mut OrderBook, order_id: u64) -> Option<Match> {
    let price = book.indices.remove(&order_id);
    match price {
        None => None,
        Some(price) => {
            let best_ask = book.get_best_ask();
            let best_bid = book.get_best_bid();
            if best_ask.is_none() && best_bid.is_none() {
                return None;
            }
            let (from, removed) = if best_ask.is_some() && price >= best_ask.unwrap() {
                (
                    AskOrBid::Ask,
                    OrderBook::remove_from_tape(&mut book.asks, order_id, price),
                )
            } else {
                (
                    AskOrBid::Bid,
                    OrderBook::remove_from_tape(&mut book.bids, order_id, price),
                )
            };
            match removed {
                Some(order) => Some(Match {
                    maker: vec![],
                    taker: Taker::cancel(order.user, order_id, order.price, order.unfilled, from),
                }),
                None => None,
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{matcher::*, orderbook::*};
    use std::str::FromStr;

    #[test]
    pub fn test_trade() {
        let base_scale = 5;
        let quote_scale = 1;
        let taker_fee = Decimal::from_str("0.001").unwrap();
        let maker_fee = Decimal::from_str("0.001").unwrap();
        let min_amount = Decimal::from_str("1").unwrap();
        let min_vol = Decimal::from_str("1").unwrap();
        let mut book = OrderBook::new(
            base_scale,
            quote_scale,
            taker_fee,
            maker_fee,
            min_amount,
            min_vol,
            true,
        );

        let price = Decimal::from_str("0.1").unwrap();
        let amount = Decimal::from_str("100").unwrap();
        let mr = execute_limit(&mut book, 1, 1001, price, amount, AskOrBid::Bid);
        assert_eq!(true, mr.is_none());
        assert_eq!(
            Decimal::from_str("0.1").unwrap(),
            *book.get_best_match(&AskOrBid::Ask).unwrap().key()
        );
        assert_eq!(
            Decimal::from_str("100").unwrap(),
            book.get_best_match(&AskOrBid::Ask).unwrap().get().amount
        );
        assert_eq!(true, book.indices.contains_key(&1001));

        let price = Decimal::from_str("0.1").unwrap();
        let amount = Decimal::from_str("1000").unwrap();
        let mr = execute_limit(&mut book, 1, 1002, price, amount, AskOrBid::Bid);
        assert_eq!(true, mr.is_none());
        assert_eq!(
            Decimal::from_str("0.1").unwrap(),
            *book.get_best_match(&AskOrBid::Ask).unwrap().key()
        );
        assert_eq!(
            Decimal::from_str("1100").unwrap(),
            book.get_best_match(&AskOrBid::Ask).unwrap().get().amount
        );
        assert_eq!(true, book.indices.contains_key(&1002));

        let price = Decimal::from_str("0.08").unwrap();
        let amount = Decimal::from_str("200").unwrap();
        let mr = execute_limit(&mut book, 1, 1003, price, amount, AskOrBid::Ask).unwrap();
        assert_eq!(false, mr.maker.is_empty());
        assert_eq!(false, book.indices.contains_key(&1001));
        assert_eq!(true, book.indices.contains_key(&1002));
        assert_eq!(false, book.indices.contains_key(&1003));
        assert_eq!(
            &Maker::maker_filled(
                1,
                1001,
                Decimal::from_str("0.1").unwrap(),
                Decimal::from_str("100").unwrap()
            ),
            mr.maker.first().unwrap()
        );
        assert_eq!(
            &Maker::maker_so_far(
                1,
                1002,
                Decimal::from_str("0.1").unwrap(),
                Decimal::from_str("100").unwrap()
            ),
            mr.maker.get(1).unwrap()
        );
        assert_eq!(Taker::taker_filled(1, 1003, price, AskOrBid::Ask), mr.taker);
        assert_eq!(
            Decimal::from_str("0.1").unwrap(),
            *book.get_best_match(&AskOrBid::Ask).unwrap().key()
        );
        assert_eq!(
            Decimal::from_str("900").unwrap(),
            book.get_best_match(&AskOrBid::Ask).unwrap().get().amount
        );

        let price = Decimal::from_str("0.12").unwrap();
        let amount = Decimal::from_str("100").unwrap();
        let mr = execute_limit(&mut book, 1, 1004, price, amount, AskOrBid::Ask);
        assert_eq!(true, mr.is_none());
        assert_eq!(
            Decimal::from_str("0.12").unwrap(),
            *book.get_best_match(&AskOrBid::Bid).unwrap().key()
        );
        assert_eq!(
            Decimal::from_str("100").unwrap(),
            book.get_best_match(&AskOrBid::Bid).unwrap().get().amount
        );
        assert_eq!(true, book.indices.contains_key(&1004));

        let mr = cancel(&mut book, 1002).unwrap();
        let price = Decimal::from_str("0.1").unwrap();
        let unfilled = Decimal::from_str("900").unwrap();
        assert_eq!(
            Taker::cancel(1, 1002, price, unfilled, AskOrBid::Bid),
            mr.taker
        );
        assert_eq!(false, book.indices.contains_key(&1002));
        assert_eq!(true, book.bids.is_empty());

        let price = Decimal::from_str("0.12").unwrap();
        let unfilled = Decimal::from_str("100").unwrap();
        let mr = cancel(&mut book, 1004).unwrap();
        assert_eq!(
            Taker::cancel(1, 1004, price, unfilled, AskOrBid::Ask),
            mr.taker
        );
        assert_eq!(false, book.indices.contains_key(&1004));
        assert_eq!(true, book.asks.is_empty());

        let mr = cancel(&mut book, 1004);
        assert_eq!(true, mr.is_none());
        assert_eq!(false, book.indices.contains_key(&1004));
        assert_eq!(true, book.asks.is_empty());

        let mr = cancel(&mut book, 1004);
        assert_eq!(true, mr.is_none());
        assert_eq!(false, book.indices.contains_key(&1004));
        assert_eq!(true, book.asks.is_empty());
    }
}
