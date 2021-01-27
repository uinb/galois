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


use crate::{
    assets,
    core::{Accounts, Symbol, SYSTEM},
    matcher::{Match, Role},
    orderbook::AskOrBid,
    output::Output,
};
use rust_decimal::{prelude::Zero, Decimal};

pub fn clear(
    accounts: &mut Accounts,
    event_id: u64,
    symbol: &Symbol,
    taker_fee: Decimal,
    maker_fee: Decimal,
    mr: &Match,
    time: u64,
) -> Vec<Output> {
    let base = symbol.0;
    let quote = symbol.1;
    match mr.maker.is_empty() {
        // cancel
        true => match mr.taker.ask_or_bid {
            AskOrBid::Ask => {
                // revert base
                assets::unfreeze(accounts, mr.taker.user_id, base, mr.taker.unfilled);
                vec![Output {
                    event_id: event_id,
                    order_id: mr.taker.order_id,
                    user_id: mr.taker.user_id,
                    symbol: *symbol,
                    role: Role::Taker,
                    state: mr.taker.state,
                    ask_or_bid: AskOrBid::Ask,
                    price: mr.taker.price,
                    base: Decimal::zero(),
                    quote: Decimal::zero(),
                    base_fee: Decimal::zero(),
                    quote_fee: Decimal::zero(),
                    timestamp: time,
                }]
            }
            AskOrBid::Bid => {
                // revert quote
                assets::unfreeze(
                    accounts,
                    mr.taker.user_id,
                    quote,
                    mr.taker.unfilled * mr.taker.price,
                );
                vec![Output {
                    event_id: event_id,
                    order_id: mr.taker.order_id,
                    user_id: mr.taker.user_id,
                    symbol: *symbol,
                    role: Role::Taker,
                    state: mr.taker.state,
                    ask_or_bid: AskOrBid::Ask,
                    price: mr.taker.price,
                    base: Decimal::zero(),
                    quote: Decimal::zero(),
                    base_fee: Decimal::zero(),
                    quote_fee: Decimal::zero(),
                    timestamp: time,
                }]
            }
        },
        // deal
        false => {
            match mr.taker.ask_or_bid {
                AskOrBid::Ask => {
                    let mut cr = Vec::<Output>::new();
                    // maker base account available incr filled
                    // maker quote acount frozen decr filled * price
                    let mut base_sum = Decimal::zero();
                    let mut quote_sum = Decimal::zero();
                    for m in &mr.maker {
                        base_sum += m.filled;
                        let quote_decr = m.filled * m.price;
                        quote_sum += quote_decr;
                        // maker is bid, incr base available(filled), decr quote frozen(quot_decr)
                        assets::add_to_available(accounts, m.user_id, base, m.filled);
                        assets::deduct_frozen(accounts, m.user_id, quote, quote_decr);
                        // charge fee for maker
                        if maker_fee.is_sign_positive() {
                            // maker is bid, incr base, decr quote, so we charge base
                            let charge_fee = m.filled * maker_fee;
                            assets::deduct_available(accounts, m.user_id, base, charge_fee);
                            assets::add_to_available(accounts, SYSTEM, base, charge_fee);
                            cr.push(Output {
                                event_id: event_id,
                                order_id: m.order_id,
                                user_id: m.user_id,
                                symbol: *symbol,
                                role: Role::Maker,
                                state: m.state,
                                ask_or_bid: AskOrBid::Bid,
                                price: m.price,
                                base: m.filled,
                                quote: -quote_decr,
                                base_fee: -charge_fee,
                                quote_fee: Decimal::zero(),
                                timestamp: time,
                            });
                        } else {
                            // maker_fee is negative
                            // maker is bid, incr base, decr quote,
                            // we give it some quote from taker cost
                            // and we charge nothing from maker
                            assets::add_to_available(
                                accounts,
                                m.user_id,
                                quote,
                                quote_decr * maker_fee.abs(),
                            );
                            cr.push(Output {
                                event_id: event_id,
                                order_id: m.order_id,
                                user_id: m.user_id,
                                symbol: *symbol,
                                role: Role::Maker,
                                state: m.state,
                                ask_or_bid: AskOrBid::Bid,
                                price: m.price,
                                base: m.filled,
                                quote: -quote_decr,
                                base_fee: Decimal::zero(),
                                quote_fee: quote_decr * maker_fee.abs(),
                                timestamp: time,
                            });
                        }
                    }
                    // taker base account frozen decr sum(filled)
                    // taker quote account available incr sum(filled * price)
                    assets::deduct_frozen(accounts, mr.taker.user_id, base, base_sum);
                    assets::add_to_available(accounts, mr.taker.user_id, quote, quote_sum);
                    // charge fee for taker
                    let charge_fee = quote_sum * taker_fee;
                    if maker_fee.is_sign_positive() {
                        // taker is ask, incr quote, decr base, so we charge quote
                        assets::deduct_available(accounts, mr.taker.user_id, quote, charge_fee);
                        assets::add_to_available(accounts, SYSTEM, quote, charge_fee);
                    } else {
                        // maker_fee is negative
                        // taker is ask, incr quote, decr base, we give some of quote to maker
                        // and leave rest of quote to us
                        assets::deduct_available(accounts, mr.taker.user_id, quote, charge_fee);
                        assets::add_to_available(
                            accounts,
                            SYSTEM,
                            quote,
                            quote_sum * (taker_fee - maker_fee.abs()),
                        );
                    }
                    cr.push(Output {
                        event_id: event_id,
                        order_id: mr.taker.order_id,
                        user_id: mr.taker.user_id,
                        symbol: *symbol,
                        role: Role::Taker,
                        state: mr.taker.state,
                        ask_or_bid: AskOrBid::Ask,
                        price: mr.taker.price,
                        base: -base_sum,
                        quote: quote_sum,
                        base_fee: Decimal::zero(),
                        quote_fee: -charge_fee,
                        timestamp: time,
                    });
                    cr
                    // maker has the dealing right
                    // for taker ask, maker bid, ask_price <= bid_price
                    // the quote taker gained would be great or equal to (ask_price * amount)
                    // nothing need to return to taker
                }
                AskOrBid::Bid => {
                    let mut cr = Vec::<Output>::new();
                    // maker base account frozen decr filled
                    // maker quote account available incr filled * price
                    let mut base_sum = Decimal::zero();
                    let mut quote_sum = Decimal::zero();
                    let mut return_quote = Decimal::zero();
                    for m in &mr.maker {
                        base_sum += m.filled;
                        let quote_incr = m.filled * m.price;
                        quote_sum += quote_incr;
                        return_quote += m.filled * mr.taker.price - m.filled * m.price;
                        // maker is ask, incr quote available(quote_incr), decr base frozen(filled)
                        assets::deduct_frozen(accounts, m.user_id, base, m.filled);
                        assets::add_to_available(accounts, m.user_id, quote, quote_incr);
                        // charge fee for maker
                        if maker_fee.is_sign_positive() {
                            // maker is ask, incr quote, decr base, so we charge quote
                            let charge_fee = quote_incr * maker_fee;
                            assets::deduct_available(accounts, m.user_id, quote, charge_fee);
                            assets::add_to_available(accounts, SYSTEM, quote, charge_fee);
                            cr.push(Output {
                                event_id: event_id,
                                order_id: m.order_id,
                                user_id: m.user_id,
                                symbol: *symbol,
                                role: Role::Maker,
                                state: m.state,
                                ask_or_bid: AskOrBid::Ask,
                                price: m.price,
                                base: -m.filled,
                                quote: quote_incr,
                                base_fee: Decimal::zero(),
                                quote_fee: -charge_fee,
                                timestamp: time,
                            });
                        } else {
                            // maker_fee is negative
                            // maker is ask, incr quote, decr base,
                            // we give it some base from taker cost, and charge nothing from maker
                            assets::add_to_available(
                                accounts,
                                m.user_id,
                                base,
                                m.filled * maker_fee.abs(),
                            );
                            cr.push(Output {
                                event_id: event_id,
                                order_id: m.order_id,
                                user_id: m.user_id,
                                symbol: *symbol,
                                role: Role::Maker,
                                state: m.state,
                                ask_or_bid: AskOrBid::Ask,
                                price: m.price,
                                base: -m.filled,
                                quote: quote_incr,
                                base_fee: m.filled * maker_fee.abs(),
                                quote_fee: Decimal::zero(),
                                timestamp: time,
                            });
                        }
                    }
                    // taker base account available incr sum(filled)
                    // taker quote account frozen decr sum(filled * price=quote_sum)
                    assets::add_to_available(accounts, mr.taker.user_id, base, base_sum);
                    assets::deduct_frozen(accounts, mr.taker.user_id, quote, quote_sum);
                    // charge fee for taker
                    let charge_fee = base_sum * taker_fee;
                    if maker_fee.is_sign_positive() {
                        // taker is bid, incr base, decr quote, so we charge base
                        assets::deduct_available(accounts, mr.taker.user_id, base, charge_fee);
                        assets::add_to_available(accounts, SYSTEM, base, charge_fee);
                    } else {
                        // taker is bid, incr base, decr quote, we give some base to maker,
                        // and leave rest of base to us
                        assets::deduct_available(accounts, mr.taker.user_id, base, charge_fee);
                        assets::add_to_available(
                            accounts,
                            SYSTEM,
                            base,
                            base_sum * (taker_fee - maker_fee.abs()),
                        );
                    }
                    // maker has the dealing right
                    // for taker bid, maker ask, bid_price >= ask_price
                    // so we return some quote to taker as below formula:
                    //
                    // bid_price(taker) * maker_filled1 - ask_price1(maker1) * maker_filled1
                    //   +
                    //  ...
                    //   +
                    // bid_price(taker) * maker_filledn - ask_pricen(makern) * maker_filledn
                    if return_quote > Decimal::zero() {
                        assets::unfreeze(accounts, mr.taker.user_id, quote, return_quote);
                    }
                    cr.push(Output {
                        event_id: event_id,
                        order_id: mr.taker.order_id,
                        user_id: mr.taker.user_id,
                        symbol: *symbol,
                        role: Role::Taker,
                        state: mr.taker.state,
                        ask_or_bid: AskOrBid::Bid,
                        price: mr.taker.price,
                        base: base_sum,
                        quote: -quote_sum,
                        base_fee: -charge_fee,
                        quote_fee: Decimal::zero(),
                        timestamp: time,
                    });
                    cr
                }
            }
        }
    }
}

#[cfg(test)]
pub mod test {
    use crate::assets;
    use crate::core::Accounts;
    use crate::matcher::*;
    use crate::orderbook::*;
    use rust_decimal::{prelude::Zero, Decimal};
    use std::str::FromStr;

    #[test]
    pub fn test_clearing_on_bid_taker_price_gt_ask() {
        let mut accounts = Accounts::new();
        // taker: bid 1 btc price 10000
        assets::add_to_available(&mut accounts, 1, 100, Decimal::from_str("10000").unwrap());
        assets::freeze(&mut accounts, 1, 100, Decimal::from_str("10000").unwrap());
        assert_eq!(
            Decimal::zero(),
            assets::get_mut(&mut accounts, 1, 100).unwrap().available
        );
        assert_eq!(
            Decimal::from_str("10000").unwrap(),
            assets::get_mut(&mut accounts, 1, 100).unwrap().frozen
        );

        // maker: ask 1 btc price 9999
        assets::add_to_available(&mut accounts, 2, 101, Decimal::from_str("1").unwrap());
        assets::freeze(&mut accounts, 2, 101, Decimal::from_str("1").unwrap());
        assert_eq!(
            Decimal::zero(),
            assets::get_mut(&mut accounts, 2, 101).unwrap().available
        );
        assert_eq!(
            Decimal::from_str("1").unwrap(),
            assets::get_mut(&mut accounts, 2, 101).unwrap().frozen
        );

        let symbol = (101, 100);
        let mr = Match {
            maker: vec![Maker::maker_filled(
                2,
                1,
                Decimal::from_str("9999").unwrap(),
                Decimal::from_str("1").unwrap(),
            )],
            taker: Taker::taker_filled(1, 2, Decimal::from_str("10000").unwrap(), AskOrBid::Bid),
        };
        super::clear(
            &mut accounts,
            1,
            &symbol,
            Decimal::zero(),
            Decimal::zero(),
            &mr,
            0,
        );
        assert_eq!(
            Decimal::from_str("1").unwrap(),
            assets::get_mut(&mut accounts, 1, 100).unwrap().available
        );
        assert_eq!(
            Decimal::zero(),
            assets::get_mut(&mut accounts, 1, 100).unwrap().frozen
        );
        assert_eq!(
            Decimal::from_str("1").unwrap(),
            assets::get_mut(&mut accounts, 1, 101).unwrap().available
        );
        assert_eq!(
            Decimal::zero(),
            assets::get_mut(&mut accounts, 1, 101).unwrap().frozen
        );

        assert_eq!(
            Decimal::from_str("9999").unwrap(),
            assets::get_mut(&mut accounts, 2, 100).unwrap().available
        );
        assert_eq!(
            Decimal::zero(),
            assets::get_mut(&mut accounts, 2, 100).unwrap().frozen
        );
        assert_eq!(
            Decimal::zero(),
            assets::get_mut(&mut accounts, 2, 101).unwrap().frozen
        );
        assert_eq!(
            Decimal::zero(),
            assets::get_mut(&mut accounts, 2, 101).unwrap().available
        );
    }

    #[test]
    pub fn test_clearing_on_ask_taker_price_lt_bid() {
        let mut accounts = Accounts::new();
        // maker: bid 1 btc price 10000
        assets::add_to_available(&mut accounts, 1, 100, Decimal::from_str("10000").unwrap());
        assets::freeze(&mut accounts, 1, 100, Decimal::from_str("10000").unwrap());
        assert_eq!(
            Decimal::zero(),
            assets::get_mut(&mut accounts, 1, 100).unwrap().available
        );
        assert_eq!(
            Decimal::from_str("10000").unwrap(),
            assets::get_mut(&mut accounts, 1, 100).unwrap().frozen
        );

        // taker: ask 1 btc price 9999
        assets::add_to_available(&mut accounts, 2, 101, Decimal::from_str("1").unwrap());
        assets::freeze(&mut accounts, 2, 101, Decimal::from_str("1").unwrap());
        assert_eq!(
            Decimal::zero(),
            assets::get_mut(&mut accounts, 2, 101).unwrap().available
        );
        assert_eq!(
            Decimal::from_str("1").unwrap(),
            assets::get_mut(&mut accounts, 2, 101).unwrap().frozen
        );

        let symbol = (101, 100);
        let mr = Match {
            maker: vec![Maker::maker_filled(
                1,
                1,
                Decimal::from_str("10000").unwrap(),
                Decimal::from_str("1").unwrap(),
            )],
            taker: Taker::taker_filled(2, 2, Decimal::from_str("9999").unwrap(), AskOrBid::Ask),
        };
        super::clear(
            &mut accounts,
            1,
            &symbol,
            Decimal::zero(),
            Decimal::zero(),
            &mr,
            0,
        );
        assert_eq!(
            Decimal::zero(),
            assets::get_mut(&mut accounts, 1, 100).unwrap().available
        );
        assert_eq!(
            Decimal::zero(),
            assets::get_mut(&mut accounts, 1, 100).unwrap().frozen
        );
        assert_eq!(
            Decimal::from_str("1").unwrap(),
            assets::get_mut(&mut accounts, 1, 101).unwrap().available
        );
        assert_eq!(
            Decimal::zero(),
            assets::get_mut(&mut accounts, 1, 101).unwrap().frozen
        );

        assert_eq!(
            Decimal::from_str("10000").unwrap(),
            assets::get_mut(&mut accounts, 2, 100).unwrap().available
        );
        assert_eq!(
            Decimal::zero(),
            assets::get_mut(&mut accounts, 2, 100).unwrap().frozen
        );
        assert_eq!(
            Decimal::zero(),
            assets::get_mut(&mut accounts, 2, 101).unwrap().frozen
        );
        assert_eq!(
            Decimal::zero(),
            assets::get_mut(&mut accounts, 2, 101).unwrap().available
        );
    }

    #[test]
    pub fn test_clearing_on_cancel_bid() {
        let mut accounts = Accounts::new();
        // maker: bid 1 btc price 10000
        assets::add_to_available(&mut accounts, 1, 100, Decimal::from_str("10000").unwrap());
        assets::freeze(&mut accounts, 1, 100, Decimal::from_str("10000").unwrap());
        // taker: ask 0.5 btc price 9999
        assets::add_to_available(&mut accounts, 2, 101, Decimal::from_str("1").unwrap());
        assets::freeze(&mut accounts, 2, 101, Decimal::from_str("0.5").unwrap());
        let symbol = (101, 100);
        let mr = Match {
            maker: vec![Maker::maker_filled(
                1,
                1,
                Decimal::from_str("10000").unwrap(),
                Decimal::from_str("0.5").unwrap(),
            )],
            taker: Taker::taker_filled(2, 2, Decimal::from_str("9999").unwrap(), AskOrBid::Ask),
        };
        super::clear(
            &mut accounts,
            1,
            &symbol,
            Decimal::zero(),
            Decimal::zero(),
            &mr,
            0,
        );
        assert_eq!(
            Decimal::zero(),
            assets::get_mut(&mut accounts, 1, 100).unwrap().available
        );
        assert_eq!(
            Decimal::from_str("5000").unwrap(),
            assets::get_mut(&mut accounts, 1, 100).unwrap().frozen
        );
        assert_eq!(
            Decimal::from_str("0.5").unwrap(),
            assets::get_mut(&mut accounts, 1, 101).unwrap().available
        );
        assert_eq!(
            Decimal::zero(),
            assets::get_mut(&mut accounts, 1, 101).unwrap().frozen
        );

        assert_eq!(
            Decimal::from_str("5000").unwrap(),
            assets::get_mut(&mut accounts, 2, 100).unwrap().available
        );
        assert_eq!(
            Decimal::zero(),
            assets::get_mut(&mut accounts, 2, 100).unwrap().frozen
        );
        assert_eq!(
            Decimal::zero(),
            assets::get_mut(&mut accounts, 2, 101).unwrap().frozen
        );
        assert_eq!(
            Decimal::from_str("0.5").unwrap(),
            assets::get_mut(&mut accounts, 2, 101).unwrap().available
        );
        let mr = Match {
            maker: vec![],
            taker: Taker::cancel(
                1,
                3,
                Decimal::from_str("10000").unwrap(),
                Decimal::from_str("0.5").unwrap(),
                AskOrBid::Bid,
            ),
        };
        super::clear(
            &mut accounts,
            2,
            &symbol,
            Decimal::zero(),
            Decimal::zero(),
            &mr,
            0,
        );
        assert_eq!(
            Decimal::from_str("5000").unwrap(),
            assets::get_mut(&mut accounts, 1, 100).unwrap().available
        );
        assert_eq!(
            Decimal::zero(),
            assets::get_mut(&mut accounts, 1, 100).unwrap().frozen
        );
        assert_eq!(
            Decimal::zero(),
            assets::get_mut(&mut accounts, 1, 101).unwrap().frozen
        );
        assert_eq!(
            Decimal::from_str("0.5").unwrap(),
            assets::get_mut(&mut accounts, 1, 101).unwrap().available
        );
    }

    #[test]
    pub fn test_clearing_on_cancel_ask() {
        let mut accounts = Accounts::new();
        // maker: bid 1 btc price 10000
        assets::add_to_available(&mut accounts, 1, 100, Decimal::from_str("10000").unwrap());
        assets::freeze(&mut accounts, 1, 100, Decimal::from_str("10000").unwrap());
        // taker: ask 1.5 btc price 9999
        assets::add_to_available(&mut accounts, 2, 101, Decimal::from_str("2").unwrap());
        assets::freeze(&mut accounts, 2, 101, Decimal::from_str("1.5").unwrap());
        let symbol = (101, 100);
        let mr = Match {
            maker: vec![Maker::maker_filled(
                1,
                1,
                Decimal::from_str("10000").unwrap(),
                Decimal::from_str("1").unwrap(),
            )],
            taker: Taker::taker_placed(
                2,
                2,
                Decimal::from_str("9999").unwrap(),
                Decimal::from_str("0.5").unwrap(),
                AskOrBid::Ask,
            ),
        };
        super::clear(
            &mut accounts,
            2,
            &symbol,
            Decimal::zero(),
            Decimal::zero(),
            &mr,
            0,
        );
        assert_eq!(
            Decimal::zero(),
            assets::get_mut(&mut accounts, 1, 100).unwrap().available
        );
        assert_eq!(
            Decimal::zero(),
            assets::get_mut(&mut accounts, 1, 100).unwrap().frozen
        );
        assert_eq!(
            Decimal::from_str("1").unwrap(),
            assets::get_mut(&mut accounts, 1, 101).unwrap().available
        );
        assert_eq!(
            Decimal::zero(),
            assets::get_mut(&mut accounts, 1, 101).unwrap().frozen
        );

        assert_eq!(
            Decimal::from_str("10000").unwrap(),
            assets::get_mut(&mut accounts, 2, 100).unwrap().available
        );
        assert_eq!(
            Decimal::zero(),
            assets::get_mut(&mut accounts, 2, 100).unwrap().frozen
        );
        assert_eq!(
            Decimal::from_str("0.5").unwrap(),
            assets::get_mut(&mut accounts, 2, 101).unwrap().frozen
        );
        assert_eq!(
            Decimal::from_str("0.5").unwrap(),
            assets::get_mut(&mut accounts, 2, 101).unwrap().available
        );
        let mr = Match {
            maker: vec![],
            taker: Taker::taker_placed(
                2,
                3,
                Decimal::from_str("9999").unwrap(),
                Decimal::from_str("0.5").unwrap(),
                AskOrBid::Ask,
            ),
        };
        super::clear(
            &mut accounts,
            2,
            &symbol,
            Decimal::zero(),
            Decimal::zero(),
            &mr,
            0,
        );

        assert_eq!(
            Decimal::from_str("10000").unwrap(),
            assets::get_mut(&mut accounts, 2, 100).unwrap().available
        );
        assert_eq!(
            Decimal::zero(),
            assets::get_mut(&mut accounts, 2, 100).unwrap().frozen
        );
        assert_eq!(
            Decimal::zero(),
            assets::get_mut(&mut accounts, 2, 101).unwrap().frozen
        );
        assert_eq!(
            Decimal::from_str("1").unwrap(),
            assets::get_mut(&mut accounts, 2, 101).unwrap().available
        );
    }

    #[test]
    pub fn test_clearing_on_positive_fee() {
        let mut accounts = Accounts::new();
        // maker: bid 1 btc price 10000
        assets::add_to_available(&mut accounts, 1, 100, Decimal::from_str("10000").unwrap());
        assets::freeze(&mut accounts, 1, 100, Decimal::from_str("10000").unwrap());
        // taker: ask 1 btc price 9999
        assets::add_to_available(&mut accounts, 2, 101, Decimal::from_str("1").unwrap());
        assets::freeze(&mut accounts, 2, 101, Decimal::from_str("1").unwrap());
        let symbol = (101, 100);
        let mr = Match {
            maker: vec![Maker::maker_filled(
                1,
                1,
                Decimal::from_str("10000").unwrap(),
                Decimal::from_str("1").unwrap(),
            )],
            taker: Taker::taker_filled(2, 2, Decimal::from_str("9999").unwrap(), AskOrBid::Ask),
        };
        super::clear(
            &mut accounts,
            2,
            &symbol,
            Decimal::from_str("0.001").unwrap(),
            Decimal::from_str("0.001").unwrap(),
            &mr,
            0,
        );

        assert_eq!(
            Decimal::from_str("9990").unwrap(),
            assets::get_mut(&mut accounts, 2, 100).unwrap().available
        );
        assert_eq!(
            Decimal::zero(),
            assets::get_mut(&mut accounts, 2, 100).unwrap().frozen
        );
        assert_eq!(
            Decimal::zero(),
            assets::get_mut(&mut accounts, 2, 101).unwrap().available
        );
        assert_eq!(
            Decimal::zero(),
            assets::get_mut(&mut accounts, 2, 101).unwrap().frozen
        );

        assert_eq!(
            Decimal::zero(),
            assets::get_mut(&mut accounts, 1, 100).unwrap().available
        );
        assert_eq!(
            Decimal::zero(),
            assets::get_mut(&mut accounts, 1, 100).unwrap().frozen
        );
        assert_eq!(
            Decimal::from_str("0.999").unwrap(),
            assets::get_mut(&mut accounts, 1, 101).unwrap().available
        );
        assert_eq!(
            Decimal::zero(),
            assets::get_mut(&mut accounts, 1, 101).unwrap().frozen
        );
    }

    #[test]
    pub fn test_clearing_on_negative_fee() {
        let mut accounts = Accounts::new();
        // maker: bid 1 btc price 10000
        assets::add_to_available(&mut accounts, 1, 100, Decimal::from_str("10000").unwrap());
        assets::freeze(&mut accounts, 1, 100, Decimal::from_str("10000").unwrap());
        // taker: ask 1 btc price 9999
        assets::add_to_available(&mut accounts, 2, 101, Decimal::from_str("1").unwrap());
        assets::freeze(&mut accounts, 2, 101, Decimal::from_str("1").unwrap());
        let symbol = (101, 100);
        let mr = Match {
            maker: vec![Maker::maker_filled(
                1,
                1,
                Decimal::from_str("10000").unwrap(),
                Decimal::from_str("1").unwrap(),
            )],
            taker: Taker::taker_filled(2, 2, Decimal::from_str("9999").unwrap(), AskOrBid::Ask),
        };
        super::clear(
            &mut accounts,
            2,
            &symbol,
            Decimal::from_str("0.001").unwrap(),
            Decimal::from_str("-0.001").unwrap(),
            &mr,
            0,
        );

        // taker ask
        assert_eq!(
            Decimal::from_str("9990").unwrap(),
            assets::get_mut(&mut accounts, 2, 100).unwrap().available
        );
        assert_eq!(
            Decimal::zero(),
            assets::get_mut(&mut accounts, 2, 100).unwrap().frozen
        );
        assert_eq!(
            Decimal::zero(),
            assets::get_mut(&mut accounts, 2, 101).unwrap().available
        );
        assert_eq!(
            Decimal::zero(),
            assets::get_mut(&mut accounts, 2, 101).unwrap().frozen
        );

        // maker bid
        assert_eq!(
            Decimal::from_str("10").unwrap(),
            assets::get_mut(&mut accounts, 1, 100).unwrap().available
        );
        assert_eq!(
            Decimal::zero(),
            assets::get_mut(&mut accounts, 1, 100).unwrap().frozen
        );
        assert_eq!(
            Decimal::from_str("1").unwrap(),
            assets::get_mut(&mut accounts, 1, 101).unwrap().available
        );
        assert_eq!(
            Decimal::zero(),
            assets::get_mut(&mut accounts, 1, 101).unwrap().frozen
        );
    }

    #[test]
    pub fn test_on_clearing_negative_fee2() {
        let mut accounts = Accounts::new();
        // maker: bid 1 btc price 10000
        assets::add_to_available(&mut accounts, 1, 100, Decimal::from_str("10000").unwrap());
        assets::freeze(&mut accounts, 1, 100, Decimal::from_str("10000").unwrap());
        // taker: ask 1 btc price 9999
        assets::add_to_available(&mut accounts, 2, 101, Decimal::from_str("1").unwrap());
        assets::freeze(&mut accounts, 2, 101, Decimal::from_str("1").unwrap());
        let symbol = (101, 100);
        let mr = Match {
            maker: vec![Maker::maker_filled(
                1,
                1,
                Decimal::from_str("10000").unwrap(),
                Decimal::from_str("1").unwrap(),
            )],
            taker: Taker::taker_filled(2, 2, Decimal::from_str("9999").unwrap(), AskOrBid::Ask),
        };

        super::clear(
            &mut accounts,
            2,
            &symbol,
            Decimal::from_str("0.001").unwrap(),
            Decimal::from_str("-0.0005").unwrap(),
            &mr,
            0,
        );

        // taker ask
        assert_eq!(
            Decimal::from_str("9990").unwrap(),
            assets::get_mut(&mut accounts, 2, 100).unwrap().available
        );
        assert_eq!(
            Decimal::zero(),
            assets::get_mut(&mut accounts, 2, 100).unwrap().frozen
        );
        assert_eq!(
            Decimal::zero(),
            assets::get_mut(&mut accounts, 2, 101).unwrap().available
        );
        assert_eq!(
            Decimal::zero(),
            assets::get_mut(&mut accounts, 2, 101).unwrap().frozen
        );

        // maker bid
        assert_eq!(
            Decimal::from_str("5").unwrap(),
            assets::get_mut(&mut accounts, 1, 100).unwrap().available
        );
        assert_eq!(
            Decimal::zero(),
            assets::get_mut(&mut accounts, 1, 100).unwrap().frozen
        );
        assert_eq!(
            Decimal::from_str("1").unwrap(),
            assets::get_mut(&mut accounts, 1, 101).unwrap().available
        );
        assert_eq!(
            Decimal::zero(),
            assets::get_mut(&mut accounts, 1, 101).unwrap().frozen
        );

        // system account
        assert_eq!(
            Decimal::from_str("5").unwrap(),
            assets::get_mut(&mut accounts, super::SYSTEM, 100)
                .unwrap()
                .available
        );
        assert_eq!(
            Decimal::zero(),
            assets::get_mut(&mut accounts, super::SYSTEM, 100)
                .unwrap()
                .frozen
        );
    }

    #[test]
    pub fn test_output() {
        let base_precision = 6;
        let quote_precision = 2;
        let taker_fee = Decimal::from_str("0.001").unwrap();
        let maker_fee = Decimal::from_str("-0.0005").unwrap();
        let min_amount = Decimal::from_str("0.01").unwrap();
        let min_vol = Decimal::from_str("10").unwrap();
        let mut book = OrderBook::new(
            base_precision,
            quote_precision,
            taker_fee,
            maker_fee,
            min_amount,
            min_vol,
            true,
        );
        let mut accounts = Accounts::new();
        assets::add_to_available(&mut accounts, 1, 100, Decimal::from_str("10000").unwrap());
        assets::add_to_available(&mut accounts, 1, 101, Decimal::from_str("1").unwrap());

        let price = Decimal::new(13333, 0);
        let amount = Decimal::new(1, 1);
        assets::freeze(&mut accounts, 1, 101, amount);
        execute_limit(&mut book, 1, 1, price, amount, AskOrBid::Ask);

        let price = Decimal::new(13333, 0);
        let amount = Decimal::new(5, 1);
        assets::freeze(&mut accounts, 1, 100, price * amount);
        assert_eq!(
            assets::get(&accounts, 1, 100).unwrap().frozen,
            Decimal::from_str("6666.5").unwrap()
        );
        assert_eq!(
            assets::get(&accounts, 1, 100).unwrap().available,
            Decimal::from_str("3333.5").unwrap()
        );
        let mr = execute_limit(&mut book, 1, 2, price, amount, AskOrBid::Bid).unwrap();

        let symbol = (101, 100);
        let out = super::clear(&mut accounts, 2, &symbol, taker_fee, maker_fee, &mr, 0);
        assert_eq!(out[0].base, Decimal::new(-1, 1));
        assert_eq!(out[0].quote, Decimal::new(13333, 1));
        assert_eq!(out[0].base_fee, Decimal::new(5, 5));
        assert_eq!(out[0].quote_fee, Decimal::zero());

        assert_eq!(out[1].base, Decimal::new(1, 1));
        assert_eq!(out[1].quote, Decimal::new(-13333, 1));
        assert_eq!(out[1].base_fee, Decimal::new(-1, 4));
        assert_eq!(out[1].quote_fee, Decimal::zero());

        assert_eq!(
            assets::get(&accounts, 1, 100).unwrap().available,
            Decimal::from_str("4666.8").unwrap()
        );
        assert_eq!(
            assets::get(&accounts, 1, 100).unwrap().frozen,
            Decimal::from_str("5333.2").unwrap()
        );
        assert_eq!(
            assets::get(&accounts, 1, 101).unwrap().available,
            Decimal::new(99995, 5)
        );
        assert_eq!(
            assets::get(&accounts, 1, 101).unwrap().frozen,
            Decimal::zero()
        );

        assert_eq!(
            assets::get(&accounts, 0, 101).unwrap().available,
            Decimal::new(5, 5)
        );
    }

    #[test]
    pub fn test_dealing_rights() {
        let base_precision = 6;
        let quote_precision = 2;
        let taker_fee = Decimal::from_str("0.001").unwrap();
        let maker_fee = Decimal::from_str("-0.0005").unwrap();
        let min_amount = Decimal::from_str("0.01").unwrap();
        let min_vol = Decimal::from_str("10").unwrap();
        let mut book = OrderBook::new(
            base_precision,
            quote_precision,
            taker_fee,
            maker_fee,
            min_amount,
            min_vol,
            true,
        );
        let mut accounts = Accounts::new();
        assets::add_to_available(&mut accounts, 1, 101, Decimal::from_str("1").unwrap());
        assets::add_to_available(&mut accounts, 2, 100, Decimal::from_str("10000").unwrap());

        let price = Decimal::new(10000, 0);
        let amount = Decimal::new(1, 1);
        assets::freeze(&mut accounts, 1, 101, amount);
        execute_limit(&mut book, 1, 1, price, amount, AskOrBid::Ask);

        let price = Decimal::new(13333, 0);
        let amount = Decimal::new(5, 1);
        assets::freeze(&mut accounts, 2, 100, price * amount);
        let mr = execute_limit(&mut book, 2, 2, price, amount, AskOrBid::Bid).unwrap();

        let symbol = (101, 100);
        let out = super::clear(&mut accounts, 2, &symbol, taker_fee, maker_fee, &mr, 0);
        assert_eq!(out[0].base, Decimal::new(-1, 1));
        assert_eq!(out[0].quote, Decimal::new(10000, 1));
        assert_eq!(out[0].base_fee, Decimal::new(5, 5));
        assert_eq!(out[0].quote_fee, Decimal::zero());

        assert_eq!(out[1].base, Decimal::new(1, 1));
        assert_eq!(out[1].quote, Decimal::new(-10000, 1));
        assert_eq!(out[1].base_fee, Decimal::new(-1, 4));
        assert_eq!(out[1].quote_fee, Decimal::zero());

        println!("{:?}", mr);
        assert_eq!(
            assets::get(&accounts, 2, 100).unwrap().available,
            Decimal::from_str("3666.8").unwrap()
        );
        assert_eq!(
            assets::get(&accounts, 2, 100).unwrap().frozen,
            Decimal::from_str("5333.2").unwrap()
        );
        assert_eq!(
            assets::get(&accounts, 2, 101).unwrap().available,
            Decimal::new(999, 4)
        );
        assert_eq!(
            assets::get(&accounts, 2, 101).unwrap().frozen,
            Decimal::zero()
        );

        assert_eq!(
            assets::get(&accounts, 1, 100).unwrap().available,
            Decimal::new(1000, 0)
        );
        assert_eq!(
            assets::get(&accounts, 1, 100).unwrap().frozen,
            Decimal::zero()
        );
        assert_eq!(
            assets::get(&accounts, 1, 101).unwrap().available,
            Decimal::new(90005, 5)
        );
        assert_eq!(
            assets::get(&accounts, 1, 101).unwrap().frozen,
            Decimal::zero()
        );

        assert_eq!(
            assets::get(&accounts, 0, 101).unwrap().available,
            Decimal::new(5, 5)
        );
    }

    #[test]
    pub fn test_dealing_rights_on_taker_ask() {
        let base_precision = 6;
        let quote_precision = 2;
        let taker_fee = Decimal::from_str("0.001").unwrap();
        let maker_fee = Decimal::from_str("-0.0005").unwrap();
        let min_amount = Decimal::from_str("0.01").unwrap();
        let min_vol = Decimal::from_str("10").unwrap();
        let mut book = OrderBook::new(
            base_precision,
            quote_precision,
            taker_fee,
            maker_fee,
            min_amount,
            min_vol,
            true,
        );
        let mut accounts = Accounts::new();
        assets::add_to_available(&mut accounts, 1, 101, Decimal::from_str("1").unwrap());
        assets::add_to_available(&mut accounts, 2, 100, Decimal::from_str("10000").unwrap());

        let price = Decimal::new(13333, 0);
        let amount = Decimal::new(5, 1);
        assets::freeze(&mut accounts, 2, 100, price * amount);
        execute_limit(&mut book, 2, 1, price, amount, AskOrBid::Bid);

        let price = Decimal::new(10000, 0);
        let amount = Decimal::new(1, 1);
        assets::freeze(&mut accounts, 1, 101, amount);
        let mr = execute_limit(&mut book, 1, 2, price, amount, AskOrBid::Ask).unwrap();

        let symbol = (101, 100);
        let out = super::clear(&mut accounts, 2, &symbol, taker_fee, maker_fee, &mr, 0);
        // 2: maker bid
        assert_eq!(out[0].base, Decimal::new(1, 1));
        assert_eq!(out[0].quote, Decimal::new(-13333, 1));
        assert_eq!(out[0].base_fee, Decimal::zero());
        assert_eq!(out[0].quote_fee, Decimal::new(66665, 5));
        assert_eq!(
            assets::get(&accounts, 2, 100).unwrap().available,
            Decimal::from_str("3334.16665").unwrap()
        );
        assert_eq!(
            assets::get(&accounts, 2, 100).unwrap().frozen,
            Decimal::from_str("5333.2").unwrap()
        );
        assert_eq!(
            assets::get(&accounts, 2, 101).unwrap().available,
            Decimal::new(1, 1)
        );
        assert_eq!(
            assets::get(&accounts, 2, 101).unwrap().frozen,
            Decimal::zero()
        );
        // 1: taker ask
        assert_eq!(out[1].base, Decimal::new(-1, 1));
        assert_eq!(out[1].quote, Decimal::new(13333, 1));
        assert_eq!(out[1].base_fee, Decimal::zero());
        assert_eq!(out[1].quote_fee, Decimal::new(-13333, 4));
        assert_eq!(
            assets::get(&accounts, 1, 100).unwrap().available,
            Decimal::from_str("1331.9667").unwrap()
        );
        assert_eq!(
            assets::get(&accounts, 1, 100).unwrap().frozen,
            Decimal::zero()
        );
        assert_eq!(
            assets::get(&accounts, 1, 101).unwrap().available,
            Decimal::new(9, 1)
        );
        assert_eq!(
            assets::get(&accounts, 1, 101).unwrap().frozen,
            Decimal::zero()
        );

        assert_eq!(
            assets::get(&accounts, 0, 100).unwrap().available,
            Decimal::from_str("0.66665").unwrap()
        );
    }
}
