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
    pub fn taker(order: Order, ask_or_bid: AskOrBid, state: State) -> Self {
        Self {
            user_id: order.user,
            order_id: order.id,
            price: order.price,
            unfilled: order.unfilled,
            ask_or_bid: ask_or_bid,
            state,
        }
    }

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
) -> Match {
    let mut makers = Vec::<Maker>::new();
    let mut order = Order::new(order_id, user_id, price, amount);
    loop {
        if order.is_filled() {
            return Match {
                maker: makers,
                taker: Taker::taker(order, ask_or_bid, State::Filled),
            };
        }
        if let Some(mut best) = book.get_best_if_match(ask_or_bid, &order.price) {
            let page = best.get_mut();
            let mut traded = take(page, &mut order);
            if page.is_empty() {
                best.remove();
            }
            traded.iter().for_each(|m| {
                book.decr_size_on(ask_or_bid, &m.filled);
                if m.state == State::Filled {
                    book.indices.remove(&m.order_id);
                }
            });
            makers.append(&mut traded);
        } else {
            book.insert(order.clone(), ask_or_bid);
            return Match {
                taker: match makers.is_empty() {
                    true => Taker::taker(order, ask_or_bid, State::Submitted),
                    false => Taker::taker(order, ask_or_bid, State::PartialFilled),
                },
                maker: makers,
            };
        }
    }
}

fn take(page: &mut OrderPage, taker: &mut Order) -> Vec<Maker> {
    let mut matches = Vec::<Maker>::new();
    while !taker.is_filled() && !page.is_empty() {
        let mut oldest = page.orders.entries().next().unwrap();
        let m = if taker.unfilled >= oldest.get().unfilled {
            let maker = oldest.get().clone();
            oldest.remove();
            Maker::maker_filled(maker.user, maker.id, maker.price, maker.unfilled)
        } else {
            let maker = oldest.get_mut();
            maker.fill(taker.unfilled);
            Maker::maker_so_far(maker.user, maker.id, maker.price, taker.unfilled)
        };
        taker.fill(m.filled);
        page.decr_size(&m.filled);
        matches.push(m);
    }
    matches
}

pub fn cancel(book: &mut OrderBook, order_id: u64) -> Option<Match> {
    let price = book.indices.remove(&order_id)?;
    let (from, removed) = match (book.get_best_ask(), book.get_best_bid()) {
        (Some(best_ask), Some(_)) => {
            if price >= best_ask {
                (
                    AskOrBid::Ask,
                    OrderBook::remove(book, order_id, &price, AskOrBid::Ask),
                )
            } else {
                (
                    AskOrBid::Bid,
                    OrderBook::remove(book, order_id, &price, AskOrBid::Bid),
                )
            }
        }
        (None, Some(_)) => (
            AskOrBid::Bid,
            OrderBook::remove(book, order_id, &price, AskOrBid::Bid),
        ),
        (Some(_), None) => (
            AskOrBid::Ask,
            OrderBook::remove(book, order_id, &price, AskOrBid::Ask),
        ),
        _ => (AskOrBid::Ask, None),
    };
    removed.map(|order| Match {
        maker: vec![],
        taker: Taker::taker(order, from, State::Canceled),
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
        assert_eq!(State::Submitted, mr.taker.state);
        assert!(mr.maker.is_empty());
        assert_eq!(
            dec!(0.1),
            *book
                .get_best_if_match(AskOrBid::Ask, &dec!(0.1))
                .unwrap()
                .key()
        );
        // best bid = 0.1, now ask with 0.11, no matches
        assert!(book.get_best_if_match(AskOrBid::Ask, &dec!(0.11)).is_none());
        // best bid = 0.1, now ask with 0.09, matches
        assert!(book.get_best_if_match(AskOrBid::Ask, &dec!(0.09)).is_some());
        assert_eq!(
            dec!(100),
            book.get_best_if_match(AskOrBid::Ask, &dec!(0.1))
                .unwrap()
                .get()
                .amount
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
        assert_eq!(State::Submitted, mr.taker.state);
        assert!(mr.maker.is_empty());
        assert_eq!(
            dec!(0.1),
            *book
                .get_best_if_match(AskOrBid::Ask, &dec!(0.1))
                .unwrap()
                .key()
        );
        assert_eq!(
            dec!(1100),
            book.get_best_if_match(AskOrBid::Ask, &dec!(0.1))
                .unwrap()
                .get()
                .amount
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
        );
        assert_eq!(State::Filled, mr.taker.state);
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
            *book
                .get_best_if_match(AskOrBid::Ask, &dec!(0.1))
                .unwrap()
                .key()
        );
        assert_eq!(
            dec!(900),
            book.get_best_if_match(AskOrBid::Ask, &dec!(0.1))
                .unwrap()
                .get()
                .amount
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
        assert_eq!(State::Submitted, mr.taker.state);
        assert!(book.get_best_if_match(AskOrBid::Bid, &dec!(0.11)).is_none());
        assert_eq!(
            dec!(0.12),
            *book
                .get_best_if_match(AskOrBid::Bid, &dec!(0.12))
                .unwrap()
                .key()
        );
        assert_eq!(
            dec!(100),
            book.get_best_if_match(AskOrBid::Bid, &dec!(0.12))
                .unwrap()
                .get()
                .amount
        );
        assert!(book.indices.contains_key(&1004));

        let mr = cancel(&mut book, 1002);
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
            mr.unwrap().taker
        );
        assert!(!book.indices.contains_key(&1002));
        assert!(book.bids.is_empty());

        let price = dec!(0.12);
        let unfilled = dec!(100);
        let mr = cancel(&mut book, 1004);
        assert!(mr.is_some());
        assert_eq!(
            Taker::cancel(
                UserId::from_low_u64_be(1),
                1004,
                price,
                unfilled,
                AskOrBid::Ask
            ),
            mr.unwrap().taker
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
