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

use crate::core::*;
use crate::orderbook::{AskOrBid, Order, OrderBook, OrderPage};

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum State {
    #[allow(dead_code)]
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

impl Into<u32> for Role {
    fn into(self) -> u32 {
        match self {
            Role::Maker => 0,
            Role::Taker => 1,
        }
    }
}

impl Into<u32> for AskOrBid {
    fn into(self) -> u32 {
        match self {
            AskOrBid::Ask => 0,
            AskOrBid::Bid => 1,
        }
    }
}

impl Into<u32> for State {
    fn into(self) -> u32 {
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
    pub user_id: UserId,
    pub order_id: u64,
    pub price: Price,
    pub unfilled: Amount,
    pub ask_or_bid: AskOrBid,
    pub state: State,
}

impl Taker {
    pub fn taker_filled(
        user_id: UserId,
        order_id: u64,
        price: Price,
        ask_or_bid: AskOrBid,
    ) -> Self {
        Self {
            user_id,
            order_id,
            price,
            unfilled: Amount::ZERO,
            ask_or_bid,
            state: State::Filled,
        }
    }

    pub const fn taker_placed(
        user_id: UserId,
        order_id: u64,
        price: Price,
        unfilled: Amount,
        ask_or_bid: AskOrBid,
    ) -> Self {
        Self {
            user_id,
            order_id,
            price,
            unfilled,
            ask_or_bid,
            state: State::PartialFilled,
        }
    }

    pub const fn cancel(
        user_id: UserId,
        order_id: u64,
        price: Price,
        unfilled: Amount,
        ask_or_bid: AskOrBid,
    ) -> Self {
        Self {
            user_id,
            order_id,
            price,
            unfilled,
            ask_or_bid,
            state: State::Canceled,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Maker {
    pub user_id: UserId,
    pub order_id: u64,
    pub price: Price,
    pub filled: Amount,
    pub state: State,
}

impl Maker {
    pub const fn maker_filled(
        user_id: UserId,
        order_id: u64,
        price: Price,
        filled: Amount,
    ) -> Self {
        Self {
            user_id,
            order_id,
            price,
            filled,
            state: State::Filled,
        }
    }

    pub const fn maker_so_far(
        user_id: UserId,
        order_id: u64,
        price: Price,
        filled: Amount,
    ) -> Self {
        Self {
            user_id,
            order_id,
            price,
            filled,
            state: State::PartialFilled,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Match {
    pub maker: Vec<Maker>,
    pub taker: Taker,
}

pub fn execute_limit(
    book: &mut OrderBook,
    user_id: UserId,
    order_id: u64,
    price: Price,
    amount: Amount,
    ask_or_bid: AskOrBid,
) -> Option<Match> {
    let mut makers = Vec::<Maker>::new();
    let mut unfilled = amount;
    loop {
        if unfilled == Amount::ZERO {
            return match !makers.is_empty() {
                true => Some(Match {
                    maker: makers,
                    taker: Taker::taker_filled(user_id, order_id, price, ask_or_bid),
                }),
                false => None,
            };
        }
        if let Some(mut best) = book.get_best_match(ask_or_bid) {
            if !can_trade(*best.key(), price, ask_or_bid) {
                let order = Order::new(order_id, user_id, price, unfilled);
                book.insert(order, ask_or_bid);
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
        } else {
            let order = Order::new(order_id, user_id, price, unfilled);
            book.insert(order, ask_or_bid);
            return match !makers.is_empty() {
                true => Some(Match {
                    maker: makers,
                    taker: Taker::taker_placed(user_id, order_id, price, unfilled, ask_or_bid),
                }),
                false => None,
            };
        }
    }
}

fn take(page: &mut OrderPage, mut taker: Amount) -> (Amount, Vec<Maker>) {
    let mut matches = Vec::<Maker>::new();
    while !taker.is_zero() && !page.is_empty() {
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
        taker = Amount::ZERO;
    }
    (taker, matches)
}

fn can_trade(best_price: Price, taker_price: Price, ask_or_bid: AskOrBid) -> bool {
    match ask_or_bid {
        AskOrBid::Ask => best_price >= taker_price,
        AskOrBid::Bid => best_price <= taker_price,
    }
}

pub fn cancel(book: &mut OrderBook, order_id: u64) -> Option<Match> {
    let price = book.indices.remove(&order_id)?;
    let (from, removed) = match (book.get_best_ask(), book.get_best_bid()) {
        (Some(best_ask), Some(_)) => {
            if price >= best_ask {
                (
                    AskOrBid::Ask,
                    OrderBook::remove_from_tape(&mut book.asks, order_id, price),
                )
            } else {
                (
                    AskOrBid::Bid,
                    OrderBook::remove_from_tape(&mut book.bids, order_id, price),
                )
            }
        }
        (None, Some(_)) => (
            AskOrBid::Bid,
            OrderBook::remove_from_tape(&mut book.bids, order_id, price),
        ),
        (Some(_), None) => (
            AskOrBid::Ask,
            OrderBook::remove_from_tape(&mut book.asks, order_id, price),
        ),
        _ => (AskOrBid::Ask, None),
    };
    removed.map(|order| Match {
        maker: vec![],
        taker: Taker::cancel(order.user, order_id, order.price, order.unfilled, from),
    })
}

#[cfg(test)]
mod test {
    use crate::{core::*, matcher::*, orderbook::*};
    use rust_decimal_macros::dec;

    #[test]
    pub fn test_trade() {
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
        );

        let price = dec!(0.1);
        let amount = dec!(100);
        let mr = execute_limit(
            &mut book,
            UserId::from_low_u64_be(1),
            1001,
            price,
            amount,
            AskOrBid::Bid,
        );
        assert_eq!(true, mr.is_none());
        assert_eq!(
            dec!(0.1),
            *book.get_best_match(AskOrBid::Ask).unwrap().key()
        );
        assert_eq!(
            dec!(100),
            book.get_best_match(AskOrBid::Ask).unwrap().get().amount
        );
        assert!(book.indices.contains_key(&1001));

        let price = dec!(0.1);
        let amount = dec!(1000);
        let mr = execute_limit(
            &mut book,
            UserId::from_low_u64_be(1),
            1002,
            price,
            amount,
            AskOrBid::Bid,
        );
        assert!(mr.is_none());
        assert_eq!(
            dec!(0.1),
            *book.get_best_match(AskOrBid::Ask).unwrap().key()
        );
        assert_eq!(
            dec!(1100),
            book.get_best_match(AskOrBid::Ask).unwrap().get().amount
        );
        assert!(book.indices.contains_key(&1002));

        let price = dec!(0.08);
        let amount = dec!(200);
        let mr = execute_limit(
            &mut book,
            UserId::from_low_u64_be(1),
            1003,
            price,
            amount,
            AskOrBid::Ask,
        )
        .unwrap();
        assert!(!mr.maker.is_empty());
        assert!(!book.indices.contains_key(&1001));
        assert!(book.indices.contains_key(&1002));
        assert!(!book.indices.contains_key(&1003));
        assert_eq!(
            &Maker::maker_filled(UserId::from_low_u64_be(1), 1001, dec!(0.1), dec!(100)),
            mr.maker.first().unwrap()
        );
        assert_eq!(
            &Maker::maker_so_far(UserId::from_low_u64_be(1), 1002, dec!(0.1), dec!(100)),
            mr.maker.get(1).unwrap()
        );
        assert_eq!(
            Taker::taker_filled(UserId::from_low_u64_be(1), 1003, price, AskOrBid::Ask),
            mr.taker
        );
        assert_eq!(
            dec!(0.1),
            *book.get_best_match(AskOrBid::Ask).unwrap().key()
        );
        assert_eq!(
            dec!(900),
            book.get_best_match(AskOrBid::Ask).unwrap().get().amount
        );

        let price = dec!(0.12);
        let amount = dec!(100);
        let mr = execute_limit(
            &mut book,
            UserId::from_low_u64_be(1),
            1004,
            price,
            amount,
            AskOrBid::Ask,
        );
        assert!(mr.is_none());
        assert_eq!(
            dec!(0.12),
            *book.get_best_match(AskOrBid::Bid).unwrap().key()
        );
        assert_eq!(
            dec!(100),
            book.get_best_match(AskOrBid::Bid).unwrap().get().amount
        );
        assert!(book.indices.contains_key(&1004));

        let mr = cancel(&mut book, 1002).unwrap();
        let price = dec!(0.1);
        let unfilled = dec!(900);
        assert_eq!(
            Taker::cancel(
                UserId::from_low_u64_be(1),
                1002,
                price,
                unfilled,
                AskOrBid::Bid
            ),
            mr.taker
        );
        assert!(!book.indices.contains_key(&1002));
        assert!(book.bids.is_empty());

        let price = dec!(0.12);
        let unfilled = dec!(100);
        let mr = cancel(&mut book, 1004).unwrap();
        assert_eq!(
            Taker::cancel(
                UserId::from_low_u64_be(1),
                1004,
                price,
                unfilled,
                AskOrBid::Ask
            ),
            mr.taker
        );
        assert!(!book.indices.contains_key(&1004));
        assert!(book.asks.is_empty());

        let mr = cancel(&mut book, 1004);
        assert!(mr.is_none());
        assert!(!book.indices.contains_key(&1004));
        assert!(book.asks.is_empty());

        let mr = cancel(&mut book, 1004);
        assert!(mr.is_none());
        assert!(!book.indices.contains_key(&1004));
        assert!(book.asks.is_empty());
    }
}
