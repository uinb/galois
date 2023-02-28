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

use rust_decimal::{prelude::Zero, Decimal};

use crate::{
    assets,
    core::*,
    matcher::{Match, Role, State},
    orderbook::AskOrBid,
    output::Output,
};

pub fn clear(
    accounts: &mut Accounts,
    event_id: u64,
    symbol: &Symbol,
    taker_fee: Fee,
    maker_fee: Fee,
    mr: &Match,
    time: u64,
) -> Vec<Output> {
    let base = symbol.0;
    let quote = symbol.1;
    match mr.taker.state {
        State::Placed => {
            let base_account = assets::get_balance_to_owned(accounts, &mr.taker.user_id, base);
            let quote_account = assets::get_balance_to_owned(accounts, &mr.taker.user_id, quote);
            vec![Output {
                event_id,
                order_id: mr.taker.order_id,
                user_id: mr.taker.user_id,
                symbol: *symbol,
                role: Role::Taker,
                state: mr.taker.state,
                ask_or_bid: AskOrBid::Ask,
                price: mr.taker.price,
                base_delta: Amount::zero(),
                quote_delta: Amount::zero(),
                base_charge: Amount::zero(),
                quote_charge: Amount::zero(),
                base_available: base_account.available,
                quote_available: quote_account.available,
                base_frozen: base_account.frozen,
                quote_frozen: quote_account.frozen,
                timestamp: time,
            }]
        }
        State::Canceled => {
            match mr.taker.ask_or_bid {
                AskOrBid::Ask => {
                    // revert base
                    assets::try_unfreeze(accounts, &mr.taker.user_id, base, mr.taker.unfilled)
                        .unwrap();
                    let base_account =
                        assets::get_balance_to_owned(accounts, &mr.taker.user_id, base);
                    let quote_account =
                        assets::get_balance_to_owned(accounts, &mr.taker.user_id, quote);
                    vec![Output {
                        event_id,
                        order_id: mr.taker.order_id,
                        user_id: mr.taker.user_id,
                        symbol: *symbol,
                        role: Role::Taker,
                        state: mr.taker.state,
                        ask_or_bid: AskOrBid::Ask,
                        price: mr.taker.price,
                        base_delta: Amount::zero(),
                        quote_delta: Amount::zero(),
                        base_charge: Amount::zero(),
                        quote_charge: Amount::zero(),
                        base_available: base_account.available,
                        quote_available: quote_account.available,
                        base_frozen: base_account.frozen,
                        quote_frozen: quote_account.frozen,
                        timestamp: time,
                    }]
                }
                AskOrBid::Bid => {
                    // revert quote
                    assets::try_unfreeze(
                        accounts,
                        &mr.taker.user_id,
                        quote,
                        mr.taker.unfilled * mr.taker.price,
                    )
                    .unwrap();
                    let base_account =
                        assets::get_balance_to_owned(accounts, &mr.taker.user_id, base);
                    let quote_account =
                        assets::get_balance_to_owned(accounts, &mr.taker.user_id, quote);
                    vec![Output {
                        event_id,
                        order_id: mr.taker.order_id,
                        user_id: mr.taker.user_id,
                        symbol: *symbol,
                        role: Role::Taker,
                        state: mr.taker.state,
                        ask_or_bid: AskOrBid::Ask,
                        price: mr.taker.price,
                        base_delta: Amount::zero(),
                        quote_delta: Amount::zero(),
                        base_charge: Amount::zero(),
                        quote_charge: Amount::zero(),
                        base_available: base_account.available,
                        quote_available: quote_account.available,
                        base_frozen: base_account.frozen,
                        quote_frozen: quote_account.frozen,
                        timestamp: time,
                    }]
                }
            }
        }
        // Filled, PartiallyFilled, ConditionallyCanceled
        _ => {
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
                        assets::add_to_available(accounts, &m.user_id, base, m.filled).unwrap();
                        let quote_account =
                            assets::deduct_frozen(accounts, &m.user_id, quote, quote_decr).unwrap();
                        // charge fee for maker
                        // maker is bid, incr base, decr quote, so we charge base
                        let charge_fee = m.filled * maker_fee;
                        let base_account =
                            assets::deduct_available(accounts, &m.user_id, base, charge_fee)
                                .unwrap();
                        assets::add_to_available(accounts, &SYSTEM, base, charge_fee).unwrap();
                        cr.push(Output {
                            event_id,
                            order_id: m.order_id,
                            user_id: m.user_id,
                            symbol: *symbol,
                            role: Role::Maker,
                            state: m.state,
                            ask_or_bid: AskOrBid::Bid,
                            price: m.price,
                            base_delta: m.filled,
                            quote_delta: -quote_decr,
                            base_charge: -charge_fee,
                            quote_charge: Decimal::zero(),
                            base_available: base_account.available,
                            quote_available: quote_account.available,
                            base_frozen: base_account.frozen,
                            quote_frozen: quote_account.frozen,
                            timestamp: time,
                        });
                    }
                    // taker base account frozen decr sum(filled)
                    // taker quote account available incr sum(filled * price)
                    if mr.taker.state == State::ConditionallyCanceled {
                        assets::try_unfreeze(accounts, &mr.taker.user_id, base, mr.taker.unfilled)
                            .unwrap();
                    }
                    let base_account =
                        assets::deduct_frozen(accounts, &mr.taker.user_id, base, base_sum).unwrap();
                    assets::add_to_available(accounts, &mr.taker.user_id, quote, quote_sum)
                        .unwrap();
                    // charge fee for taker
                    let charge_fee = quote_sum * taker_fee;
                    // taker is ask, incr quote, decr base, so we charge quote
                    let quote_account =
                        assets::deduct_available(accounts, &mr.taker.user_id, quote, charge_fee)
                            .unwrap();
                    assets::add_to_available(accounts, &SYSTEM, quote, charge_fee).unwrap();
                    cr.push(Output {
                        event_id,
                        order_id: mr.taker.order_id,
                        user_id: mr.taker.user_id,
                        symbol: *symbol,
                        role: Role::Taker,
                        state: mr.taker.state,
                        ask_or_bid: AskOrBid::Ask,
                        price: mr.taker.price,
                        base_delta: -base_sum,
                        quote_delta: quote_sum,
                        base_charge: Decimal::zero(),
                        quote_charge: -charge_fee,
                        base_available: base_account.available,
                        quote_available: quote_account.available,
                        base_frozen: base_account.frozen,
                        quote_frozen: quote_account.frozen,
                        timestamp: time,
                    });
                    cr
                    // makers deal
                    // for taker ask, maker bid, ask_price <= bid_price
                    // the quote taker gained would be great or equal than (ask_price * amount)
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
                        let base_account =
                            assets::deduct_frozen(accounts, &m.user_id, base, m.filled).unwrap();
                        assets::add_to_available(accounts, &m.user_id, quote, quote_incr).unwrap();
                        // charge fee for maker
                        // maker is ask, incr quote, decr base, so we charge quote
                        let charge_fee = quote_incr * maker_fee;
                        let quote_account =
                            assets::deduct_available(accounts, &m.user_id, quote, charge_fee)
                                .unwrap();
                        assets::add_to_available(accounts, &SYSTEM, quote, charge_fee).unwrap();
                        cr.push(Output {
                            event_id,
                            order_id: m.order_id,
                            user_id: m.user_id,
                            symbol: *symbol,
                            role: Role::Maker,
                            state: m.state,
                            ask_or_bid: AskOrBid::Ask,
                            price: m.price,
                            base_delta: -m.filled,
                            quote_delta: quote_incr,
                            base_charge: Decimal::zero(),
                            quote_charge: -charge_fee,
                            base_available: base_account.available,
                            quote_available: quote_account.available,
                            base_frozen: base_account.frozen,
                            quote_frozen: quote_account.frozen,
                            timestamp: time,
                        });
                    }
                    // taker base account available incr sum(filled)
                    // taker quote account frozen decr sum(filled * price=quote_sum)
                    assets::add_to_available(accounts, &mr.taker.user_id, base, base_sum).unwrap();
                    assets::deduct_frozen(accounts, &mr.taker.user_id, quote, quote_sum).unwrap();
                    // charge fee for taker
                    let charge_fee = base_sum * taker_fee;
                    // taker is bid, incr base, decr quote, so we charge base
                    assets::deduct_available(accounts, &mr.taker.user_id, base, charge_fee)
                        .unwrap();
                    assets::add_to_available(accounts, &SYSTEM, base, charge_fee).unwrap();
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
                        assets::try_unfreeze(accounts, &mr.taker.user_id, quote, return_quote)
                            .unwrap();
                    }
                    if mr.taker.state == State::ConditionallyCanceled {
                        assets::try_unfreeze(
                            accounts,
                            &mr.taker.user_id,
                            quote,
                            mr.taker.unfilled * mr.taker.price,
                        )
                        .unwrap();
                    }
                    let base_account =
                        assets::get_balance_to_owned(accounts, &mr.taker.user_id, base);
                    let quote_account =
                        assets::get_balance_to_owned(accounts, &mr.taker.user_id, quote);
                    cr.push(Output {
                        event_id,
                        order_id: mr.taker.order_id,
                        user_id: mr.taker.user_id,
                        symbol: *symbol,
                        role: Role::Taker,
                        state: mr.taker.state,
                        ask_or_bid: AskOrBid::Bid,
                        price: mr.taker.price,
                        base_delta: base_sum,
                        quote_delta: -quote_sum,
                        base_charge: -charge_fee,
                        quote_charge: Decimal::zero(),
                        base_available: base_account.available,
                        quote_available: quote_account.available,
                        base_frozen: base_account.frozen,
                        quote_frozen: quote_account.frozen,
                        timestamp: time,
                    });
                    cr
                }
            }
        }
    }
}

#[allow(unused_must_use)]
#[cfg(test)]
pub mod test {
    use rust_decimal::{prelude::Zero, Decimal};
    use rust_decimal_macros::dec;

    use crate::{assets, core::*, matcher::*, orderbook::*};

    // impl UserId {
    //     // adapt to legacy code
    //     pub fn from_low_u64_be(x: u64) -> Self {
    //         let mut s = [0u8; 32];
    //         s[24..].copy_from_slice(&x.to_be_bytes());
    //         Self::new(s)
    //     }
    // }

    #[test]
    pub fn test_clearing_on_bid_taker_price_gt_ask() {
        let mut accounts = Accounts::new();
        // taker: bid 1 btc price 10000
        assets::add_to_available(&mut accounts, &UserId::from_low_u64_be(1), 100, dec!(10000));
        assets::try_freeze(&mut accounts, &UserId::from_low_u64_be(1), 100, dec!(10000)).unwrap();
        let b1_100 = assets::get_balance_to_owned(&accounts, &UserId::from_low_u64_be(1), 100);
        assert_eq!(dec!(0), b1_100.available);
        assert_eq!(dec!(10000), b1_100.frozen);

        // maker: ask 1 btc price 9999
        assets::add_to_available(&mut accounts, &UserId::from_low_u64_be(2), 101, dec!(1));
        assets::try_freeze(&mut accounts, &UserId::from_low_u64_be(2), 101, dec!(1)).unwrap();
        let b2_101 = assets::get_balance_to_owned(&accounts, &UserId::from_low_u64_be(2), 101);
        assert_eq!(dec!(0), b2_101.available);
        assert_eq!(dec!(1), b2_101.frozen);

        let symbol = (101, 100);
        let mr = Match {
            maker: vec![Maker::maker_filled(
                UserId::from_low_u64_be(2),
                1,
                dec!(9999),
                dec!(1),
            )],
            taker: Taker::taker_filled(UserId::from_low_u64_be(1), 2, dec!(10000), AskOrBid::Bid),
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
        let b1_100 = assets::get_balance_to_owned(&accounts, &UserId::from_low_u64_be(1), 100);
        assert_eq!(dec!(1), b1_100.available,);
        assert_eq!(dec!(0), b1_100.frozen);
        let b1_101 = assets::get_balance_to_owned(&mut accounts, &UserId::from_low_u64_be(1), 101);
        assert_eq!(dec!(1), b1_101.available);
        assert_eq!(dec!(0), b1_101.frozen);
        let b2_100 = assets::get_balance_to_owned(&accounts, &UserId::from_low_u64_be(2), 100);
        assert_eq!(dec!(9999), b2_100.available);
        assert_eq!(dec!(0), b2_100.frozen);
        let b2_101 = assets::get_balance_to_owned(&accounts, &UserId::from_low_u64_be(2), 101);
        assert_eq!(Decimal::zero(), b2_101.frozen);
        assert_eq!(Decimal::zero(), b2_101.available);
    }

    #[test]
    pub fn test_clearing_on_ask_taker_price_lt_bid() {
        let mut accounts = Accounts::new();
        // maker: bid 1 btc price 10000
        assets::add_to_available(&mut accounts, &UserId::from_low_u64_be(1), 100, dec!(10000));
        assets::try_freeze(&mut accounts, &UserId::from_low_u64_be(1), 100, dec!(10000)).unwrap();
        let b1_100 = assets::get_balance_to_owned(&accounts, &UserId::from_low_u64_be(1), 100);
        assert_eq!(Decimal::zero(), b1_100.available);
        assert_eq!(dec!(10000), b1_100.frozen);

        // taker: ask 1 btc price 9999
        assets::add_to_available(&mut accounts, &UserId::from_low_u64_be(2), 101, dec!(1));
        assets::try_freeze(&mut accounts, &UserId::from_low_u64_be(2), 101, dec!(1)).unwrap();
        let b2_101 = assets::get_balance_to_owned(&accounts, &UserId::from_low_u64_be(2), 101);
        assert_eq!(Decimal::zero(), b2_101.available);
        assert_eq!(dec!(1), b2_101.frozen);

        let symbol = (101, 100);
        let mr = Match {
            maker: vec![Maker::maker_filled(
                UserId::from_low_u64_be(1),
                1,
                dec!(10000),
                dec!(1),
            )],
            taker: Taker::taker_filled(UserId::from_low_u64_be(2), 2, dec!(9999), AskOrBid::Ask),
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
        let b1_100 = assets::get_balance_to_owned(&accounts, &UserId::from_low_u64_be(1), 100);
        assert_eq!(Decimal::zero(), b1_100.available);
        assert_eq!(Decimal::zero(), b1_100.frozen);
        let b1_101 = assets::get_balance_to_owned(&accounts, &UserId::from_low_u64_be(1), 101);
        assert_eq!(dec!(1), b1_101.available);
        assert_eq!(Decimal::zero(), b1_101.frozen);

        let b2_100 = assets::get_balance_to_owned(&accounts, &UserId::from_low_u64_be(2), 100);
        assert_eq!(dec!(10000), b2_100.available);
        assert_eq!(Decimal::zero(), b2_100.frozen);
        let b2_101 = assets::get_balance_to_owned(&accounts, &UserId::from_low_u64_be(2), 101);
        assert_eq!(Decimal::zero(), b2_101.frozen);
        assert_eq!(Decimal::zero(), b2_101.available);
    }

    #[test]
    pub fn test_clearing_on_cancel_bid() {
        let mut accounts = Accounts::new();
        // maker: bid 1 btc price 10000
        assets::add_to_available(&mut accounts, &UserId::from_low_u64_be(1), 100, dec!(10000));
        assets::try_freeze(&mut accounts, &UserId::from_low_u64_be(1), 100, dec!(10000)).unwrap();
        // taker: ask 0.5 btc price 9999
        assets::add_to_available(&mut accounts, &UserId::from_low_u64_be(2), 101, dec!(1));
        assets::try_freeze(&mut accounts, &UserId::from_low_u64_be(2), 101, dec!(0.5)).unwrap();
        let symbol = (101, 100);
        let mr = Match {
            maker: vec![Maker::maker_filled(
                UserId::from_low_u64_be(1),
                1,
                dec!(10000),
                dec!(0.5),
            )],
            taker: Taker::taker_filled(UserId::from_low_u64_be(2), 2, dec!(9999), AskOrBid::Ask),
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
        let b1_100 = assets::get_balance_to_owned(&accounts, &UserId::from_low_u64_be(1), 100);
        assert_eq!(Decimal::zero(), b1_100.available);
        assert_eq!(dec!(5000), b1_100.frozen);
        let b1_101 = assets::get_balance_to_owned(&accounts, &UserId::from_low_u64_be(1), 101);
        assert_eq!(dec!(0.5), b1_101.available);
        assert_eq!(Decimal::zero(), b1_101.frozen);

        let b2_100 = assets::get_balance_to_owned(&accounts, &UserId::from_low_u64_be(2), 100);
        assert_eq!(dec!(5000), b2_100.available);
        assert_eq!(Decimal::zero(), b2_100.frozen);
        let b2_101 = assets::get_balance_to_owned(&accounts, &UserId::from_low_u64_be(2), 101);
        assert_eq!(Decimal::zero(), b2_101.frozen);
        assert_eq!(dec!(0.5), b2_101.available);
        let mr = Match {
            maker: vec![],
            taker: Taker::cancel(
                UserId::from_low_u64_be(1),
                3,
                dec!(10000),
                dec!(0.5),
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
        let b1_100 = assets::get_balance_to_owned(&accounts, &UserId::from_low_u64_be(1), 100);
        assert_eq!(dec!(5000), b1_100.available);
        assert_eq!(Decimal::zero(), b1_100.frozen);
        let b1_101 = assets::get_balance_to_owned(&accounts, &UserId::from_low_u64_be(1), 101);
        assert_eq!(Decimal::zero(), b1_101.frozen);
        assert_eq!(dec!(0.5), b1_101.available);
    }

    #[test]
    pub fn test_clearing_on_cancel_ask() {
        let mut accounts = Accounts::new();
        // maker: bid 1 btc price 10000
        assets::add_to_available(&mut accounts, &UserId::from_low_u64_be(1), 100, dec!(10000));
        assets::try_freeze(&mut accounts, &UserId::from_low_u64_be(1), 100, dec!(10000)).unwrap();
        // taker: ask 1.5 btc price 9999
        assets::add_to_available(&mut accounts, &UserId::from_low_u64_be(2), 101, dec!(2));
        assets::try_freeze(&mut accounts, &UserId::from_low_u64_be(2), 101, dec!(1.5)).unwrap();
        let symbol = (101, 100);
        let mr = Match {
            maker: vec![Maker::maker_filled(
                UserId::from_low_u64_be(1),
                1,
                dec!(10000),
                dec!(1),
            )],
            taker: Taker::taker_placed(
                UserId::from_low_u64_be(2),
                2,
                dec!(9999),
                dec!(0.5),
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
        let b1_100 = assets::get_balance_to_owned(&accounts, &UserId::from_low_u64_be(1), 100);
        assert_eq!(Decimal::zero(), b1_100.available);
        assert_eq!(Decimal::zero(), b1_100.frozen);
        let b1_101 = assets::get_balance_to_owned(&accounts, &UserId::from_low_u64_be(1), 101);
        assert_eq!(dec!(1), b1_101.available);
        assert_eq!(Decimal::zero(), b1_101.frozen);

        let b2_100 = assets::get_balance_to_owned(&accounts, &UserId::from_low_u64_be(2), 100);
        assert_eq!(dec!(10000), b2_100.available);
        assert_eq!(Decimal::zero(), b2_100.frozen);
        let b2_101 = assets::get_balance_to_owned(&accounts, &UserId::from_low_u64_be(2), 101);
        assert_eq!(dec!(0.5), b2_101.frozen);
        assert_eq!(dec!(0.5), b2_101.available);
        let mr = Match {
            maker: vec![],
            // TODO tag here
            // taker: Taker::taker_placed(
            taker: Taker::cancel(
                UserId::from_low_u64_be(2),
                3,
                dec!(9999),
                dec!(0.5),
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

        let b2_100 = assets::get_balance_to_owned(&accounts, &UserId::from_low_u64_be(2), 100);
        assert_eq!(dec!(10000), b2_100.available);
        assert_eq!(Decimal::zero(), b2_100.frozen);
        let b2_101 = assets::get_balance_to_owned(&accounts, &UserId::from_low_u64_be(2), 101);
        assert_eq!(Decimal::zero(), b2_101.frozen);
        assert_eq!(dec!(1), b2_101.available);
    }

    #[test]
    pub fn test_clearing_on_positive_fee() {
        let mut accounts = Accounts::new();
        // maker: bid 1 btc price 10000
        assets::add_to_available(&mut accounts, &UserId::from_low_u64_be(1), 100, dec!(10000));
        assets::try_freeze(&mut accounts, &UserId::from_low_u64_be(1), 100, dec!(10000)).unwrap();
        // taker: ask 1 btc price 9999
        assets::add_to_available(&mut accounts, &UserId::from_low_u64_be(2), 101, dec!(1));
        assets::try_freeze(&mut accounts, &UserId::from_low_u64_be(2), 101, dec!(1)).unwrap();
        let symbol = (101, 100);
        let mr = Match {
            maker: vec![Maker::maker_filled(
                UserId::from_low_u64_be(1),
                1,
                dec!(10000),
                dec!(1),
            )],
            taker: Taker::taker_filled(UserId::from_low_u64_be(2), 2, dec!(9999), AskOrBid::Ask),
        };
        super::clear(&mut accounts, 2, &symbol, dec!(0.001), dec!(0.001), &mr, 0);

        let b2_100 = assets::get_balance_to_owned(&accounts, &UserId::from_low_u64_be(2), 100);
        assert_eq!(dec!(9990), b2_100.available);
        assert_eq!(Decimal::zero(), b2_100.frozen);
        let b2_101 = assets::get_balance_to_owned(&accounts, &UserId::from_low_u64_be(2), 101);
        assert_eq!(Decimal::zero(), b2_101.available);
        assert_eq!(Decimal::zero(), b2_101.frozen);

        let b1_100 = assets::get_balance_to_owned(&accounts, &UserId::from_low_u64_be(1), 100);
        assert_eq!(Decimal::zero(), b1_100.available);
        assert_eq!(Decimal::zero(), b1_100.frozen);
        let b1_101 = assets::get_balance_to_owned(&accounts, &UserId::from_low_u64_be(1), 101);
        assert_eq!(dec!(0.999), b1_101.available);
        assert_eq!(Decimal::zero(), b1_101.frozen);
    }

    #[test]
    pub fn test_self_trade() {
        let base_scale = 6;
        let quote_scale = 2;
        let taker_fee = dec!(0.001);
        let maker_fee = Decimal::zero();
        let min_amount = dec!(0.01);
        let min_vol = dec!(10);
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
        let mut accounts = Accounts::new();
        assets::add_to_available(&mut accounts, &UserId::from_low_u64_be(1), 100, dec!(10000));
        assets::add_to_available(&mut accounts, &UserId::from_low_u64_be(1), 101, dec!(1));

        let price = dec!(13333);
        let amount = dec!(0.1);
        assets::try_freeze(&mut accounts, &UserId::from_low_u64_be(1), 101, amount).unwrap();
        execute_limit(
            &mut book,
            UserId::from_low_u64_be(1),
            1,
            price,
            amount,
            AskOrBid::Ask,
        );

        let price = dec!(13333);
        let amount = dec!(0.5);
        assets::try_freeze(
            &mut accounts,
            &UserId::from_low_u64_be(1),
            100,
            price * amount,
        )
        .unwrap();
        let b1_100 = assets::get_balance_to_owned(&accounts, &UserId::from_low_u64_be(1), 100);
        assert_eq!(b1_100.frozen, dec!(6666.5));
        assert_eq!(b1_100.available, dec!(3333.5));
        let mr = execute_limit(
            &mut book,
            UserId::from_low_u64_be(1),
            2,
            price,
            amount,
            AskOrBid::Bid,
        );

        let symbol = (101, 100);
        let out = super::clear(&mut accounts, 2, &symbol, taker_fee, maker_fee, &mr, 0);
        assert_eq!(out[0].base_delta, Decimal::zero());
        assert_eq!(out[0].quote_delta, Decimal::zero());
        assert_eq!(out[0].base_charge, Decimal::zero());
        assert_eq!(out[0].quote_charge, Decimal::zero());

        let b1_100 = assets::get_balance_to_owned(&accounts, &UserId::from_low_u64_be(1), 100);
        assert_eq!(b1_100.available, dec!(10000));
        assert_eq!(b1_100.frozen, Decimal::zero());
        let b1_101 = assets::get_balance_to_owned(&accounts, &UserId::from_low_u64_be(1), 101);
        assert_eq!(b1_101.available, dec!(0.9));
        assert_eq!(b1_101.frozen, dec!(0.1));

        assert_eq!(
            assets::get_balance_to_owned(&accounts, &SYSTEM, 101).available,
            Decimal::zero(),
        );
    }

    #[test]
    pub fn test_dealing_rights() {
        let base_scale = 6;
        let quote_scale = 2;
        let taker_fee = dec!(0.001);
        let maker_fee = dec!(0.001);
        let min_amount = dec!(0.01);
        let min_vol = dec!(10);
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
        let mut accounts = Accounts::new();
        assets::add_to_available(&mut accounts, &UserId::from_low_u64_be(1), 101, dec!(1));
        assets::add_to_available(&mut accounts, &UserId::from_low_u64_be(2), 100, dec!(10000));

        let price = dec!(10000);
        let amount = dec!(0.1);
        assets::try_freeze(&mut accounts, &UserId::from_low_u64_be(1), 101, amount).unwrap();
        execute_limit(
            &mut book,
            UserId::from_low_u64_be(1),
            1,
            price,
            amount,
            AskOrBid::Ask,
        );

        let price = dec!(13333);
        let amount = dec!(0.5);
        assets::try_freeze(
            &mut accounts,
            &UserId::from_low_u64_be(2),
            100,
            price * amount,
        )
        .unwrap();
        let mr = execute_limit(
            &mut book,
            UserId::from_low_u64_be(2),
            2,
            price,
            amount,
            AskOrBid::Bid,
        );

        let symbol = (101, 100);
        let out = super::clear(&mut accounts, 2, &symbol, taker_fee, maker_fee, &mr, 0);
        assert_eq!(out[0].base_delta, dec!(-0.1));
        assert_eq!(out[0].quote_delta, dec!(1000));
        assert_eq!(out[0].base_charge, Decimal::zero());
        assert_eq!(out[0].quote_charge, dec!(-1));

        assert_eq!(out[1].base_delta, dec!(0.1));
        assert_eq!(out[1].quote_delta, dec!(-1000));
        assert_eq!(out[1].base_charge, dec!(-0.0001));
        assert_eq!(out[1].quote_charge, Decimal::zero());

        let b2_100 = assets::get_balance_to_owned(&accounts, &UserId::from_low_u64_be(2), 100);
        assert_eq!(b2_100.available, dec!(3666.8));
        assert_eq!(b2_100.frozen, dec!(5333.2));
        let b2_101 = assets::get_balance_to_owned(&accounts, &UserId::from_low_u64_be(2), 101);
        assert_eq!(b2_101.available, dec!(0.0999));
        assert_eq!(b2_101.frozen, Decimal::zero());
        let b1_100 = assets::get_balance_to_owned(&accounts, &UserId::from_low_u64_be(1), 100);
        assert_eq!(b1_100.available, dec!(999));
        assert_eq!(b1_100.frozen, Decimal::zero());
        let b1_101 = assets::get_balance_to_owned(&accounts, &UserId::from_low_u64_be(1), 101);
        assert_eq!(b1_101.available, dec!(0.9));
        assert_eq!(b1_101.frozen, Decimal::zero());
        let system_101 = assets::get_balance_to_owned(&accounts, &SYSTEM, 101).available;
        assert_eq!(system_101, dec!(0.0001));
        let system_100 = assets::get_balance_to_owned(&accounts, &SYSTEM, 100).available;
        assert_eq!(system_100, dec!(1));
    }

    #[test]
    pub fn test_dealing_rights_on_taker_ask() {
        let base_scale = 6;
        let quote_scale = 2;
        let taker_fee = dec!(0.001);
        let maker_fee = dec!(0.001);
        let min_amount = dec!(0.01);
        let min_vol = dec!(10);
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
        let mut accounts = Accounts::new();
        assets::add_to_available(&mut accounts, &UserId::from_low_u64_be(1), 101, dec!(1));
        assets::add_to_available(&mut accounts, &UserId::from_low_u64_be(2), 100, dec!(10000));

        let price = dec!(13333);
        let amount = dec!(0.5);
        assets::try_freeze(
            &mut accounts,
            &UserId::from_low_u64_be(2),
            100,
            price * amount,
        )
        .unwrap();
        execute_limit(
            &mut book,
            UserId::from_low_u64_be(2),
            1,
            price,
            amount,
            AskOrBid::Bid,
        );

        let price = dec!(10000);
        let amount = dec!(0.1);
        assets::try_freeze(&mut accounts, &UserId::from_low_u64_be(1), 101, amount).unwrap();
        let mr = execute_limit(
            &mut book,
            UserId::from_low_u64_be(1),
            2,
            price,
            amount,
            AskOrBid::Ask,
        );

        let symbol = (101, 100);
        let out = super::clear(&mut accounts, 2, &symbol, taker_fee, maker_fee, &mr, 0);
        // 2: maker bid
        assert_eq!(out[0].base_delta, dec!(0.1));
        assert_eq!(out[0].quote_delta, dec!(-1333.3));
        assert_eq!(out[0].base_charge, dec!(-0.0001));
        assert_eq!(out[0].quote_charge, Decimal::zero());
        let b2_100 = assets::get_balance_to_owned(&accounts, &UserId::from_low_u64_be(2), 100);
        assert_eq!(b2_100.available, dec!(3333.5));
        assert_eq!(b2_100.frozen, dec!(5333.2));
        let b2_101 = assets::get_balance_to_owned(&accounts, &UserId::from_low_u64_be(2), 101);
        assert_eq!(b2_101.available, dec!(0.0999));
        assert_eq!(b2_101.frozen, Decimal::zero());
        // 1: taker ask
        assert_eq!(out[1].base_delta, dec!(-0.1));
        assert_eq!(out[1].quote_delta, dec!(1333.3));
        assert_eq!(out[1].base_charge, Decimal::zero());
        assert_eq!(out[1].quote_charge, dec!(-1.3333));
        let b1_100 = assets::get_balance_to_owned(&accounts, &UserId::from_low_u64_be(1), 100);
        assert_eq!(b1_100.available, dec!(1331.9667));
        assert_eq!(b1_100.frozen, Decimal::zero());
        let b1_101 = assets::get_balance_to_owned(&accounts, &UserId::from_low_u64_be(1), 101);
        assert_eq!(b1_101.available, dec!(0.9));
        assert_eq!(b1_101.frozen, Decimal::zero());
        let system_100 = assets::get_balance_to_owned(&accounts, &SYSTEM, 100).available;
        let system_101 = assets::get_balance_to_owned(&accounts, &SYSTEM, 101).available;
        assert_eq!(system_100, dec!(1.3333));
        assert_eq!(system_101, dec!(0.0001));
    }
}
