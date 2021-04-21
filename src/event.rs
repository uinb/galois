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
    assets, clearing, core::*, matcher::*, orderbook::*, output, sequence, server, snapshot,
};
use rust_decimal::{prelude::Zero, Decimal};
use serde::{Deserialize, Serialize};
use serde_json;
use std::{
    sync::mpsc::{Receiver, Sender},
    thread,
};

#[derive(Debug, Eq, PartialEq, Clone, Deserialize, Serialize)]
pub enum Event {
    Limit(
        EventId,
        Symbol,
        UserId,
        OrderId,
        Price,
        Amount,
        AskOrBid,
        Timestamp,
    ),
    Market(EventId, Symbol, UserId, OrderId, Vol, AskOrBid, Timestamp),
    Cancel(EventId, Symbol, UserId, OrderId, Timestamp),
    CancelAll(EventId, Symbol, Timestamp),
    Open(EventId, Symbol, Timestamp),
    Close(EventId, Symbol, Timestamp),
    OpenAll(EventId, Timestamp),
    CloseAll(EventId, Timestamp),
    TransferOut(EventId, UserId, Currency, Amount, Timestamp),
    TransferIn(EventId, UserId, Currency, Amount, Timestamp),
    NewSymbol(
        EventId,
        Symbol,
        Scale,
        Scale,
        Fee, // Taker
        Fee, // Maker
        Amount,
        Vol,
        bool,
        Timestamp,
    ),
    UpdateSymbol(
        EventId,
        Symbol,
        Scale,
        Scale,
        Fee, // Taker
        Fee, // Maker
        Amount,
        Vol,
        bool,
        Timestamp,
    ),
    // special: `EventId` means dump at `EventId`
    Dump(EventId, Timestamp),
}

#[derive(Debug, Eq, PartialEq, Clone, Deserialize, Serialize)]
pub enum Inspection {
    ConfirmAll(u64, u64),
    UpdateDepth,
    QueryOrder(Symbol, OrderId, u64, u64),
    QueryBalance(UserId, Currency, u64, u64),
    QueryAccounts(UserId, u64, u64),
}

/// 0. symbol exists
/// 1. check symbol open
/// 2. check amount >= symbol_min_amount
/// 3. check scale
/// 4. check account
fn handle_limit(
    event_id: u64,
    data: &mut Data,
    symbol: &Symbol,
    price: Decimal,
    amount: Decimal,
    user: u64,
    order: u64,
    ask_or_bid: AskOrBid,
    time: u64,
    sender: &Sender<Vec<output::Output>>,
) -> bool {
    let orderbook = data.orderbooks.get_mut(symbol);
    if orderbook.is_none() {
        return false;
    }
    let orderbook = orderbook.unwrap();
    if !orderbook.open {
        return false;
    }
    if amount < orderbook.min_amount {
        return false;
    }
    if price.scale() > orderbook.quote_scale {
        return false;
    }
    if amount.scale() > orderbook.base_scale {
        return false;
    }
    let account = data.accounts.get_mut(&user);
    if account.is_none() {
        return false;
    }
    match ask_or_bid {
        AskOrBid::Bid => {
            // check quote account
            let account = account.unwrap().get_mut(&symbol.1);
            if account.is_none() {
                return false;
            }
            let account = account.unwrap();
            let vol = price * amount;
            if account.available < vol {
                return false;
            }
            account.available -= vol;
            account.frozen += vol;
        }
        AskOrBid::Ask => {
            // check base account
            let account = account.unwrap().get_mut(&symbol.0);
            if account.is_none() {
                return false;
            }
            let account = account.unwrap();
            if account.available < amount {
                return false;
            }
            account.available -= amount;
            account.frozen += amount;
        }
    }
    let mr = execute_limit(orderbook, user, order, price, amount, ask_or_bid);
    if mr.is_some() {
        let cr = clearing::clear(
            &mut data.accounts,
            event_id,
            symbol,
            orderbook.taker_fee,
            orderbook.maker_fee,
            &mr.unwrap(),
            time,
        );
        // FIXME
        sender.send(cr).unwrap();
    }
    true
}

pub fn init(recv: Receiver<sequence::Fusion>, sender: Sender<Vec<output::Output>>, data: Data) {
    let mut data = data;
    thread::spawn(move || loop {
        let fusion = recv.recv().unwrap();
        match fusion {
            // come from request or inner counter
            sequence::Fusion::R(watch) => {
                if !watch.cmd.validate() {
                    log::info!("illegal request {:?}", watch);
                    server::publish(server::Message::with_payload(
                        watch.session,
                        watch.req_id,
                        vec![],
                    ));
                    continue;
                }
                let inspection = watch.to_inspection();
                do_inspect(&inspection, &data);
            }
            sequence::Fusion::W(seq) => {
                if !seq.cmd.validate() {
                    log::info!("illegal sequence {:?}", seq);
                    sequence::update_sequence_status(seq.id, sequence::ERROR);
                    continue;
                }
                let event = seq.to_event();
                let (_, ok) = handle_event(&event, &mut data, &sender);
                if !ok {
                    log::info!("execute sequence {:?} failed", seq);
                    sequence::update_sequence_status(seq.id, sequence::ERROR);
                }
            }
        }
    });
}

fn handle_event(
    event: &Event,
    data: &mut Data,
    sender: &Sender<Vec<output::Output>>,
) -> (u64, bool) {
    match event {
        &Event::Limit(id, symbol, user, order, price, amount, ask_or_bid, time) => {
            let ok = handle_limit(
                id, data, &symbol, price, amount, user, order, ask_or_bid, time, &sender,
            );
            (id, ok)
        }
        &Event::Market(id, symbol, user, order, vol, ask_or_bid, time) => {
            // 0. symbol exsits
            // 1. check symbol open
            // 2. check symbol permit market order
            // 3. check account
            // 4. check vol >= min_vol
            // let symbol = s.symbol().unwrap();
            // let orderbook = data.orderbooks.get_mut(&symbol).unwrap();
            // let mr = execute(orderbook, event.clone());
            // clear(&mut data.accounts, mr);
            (id, false)
        }
        &Event::Cancel(id, symbol, user, order, time) => {
            // 0. symbol exsits
            // 1. check order's owner
            match data.orderbooks.get_mut(&symbol) {
                Some(orderbook) => {
                    let mr = cancel(orderbook, order);
                    if mr.is_some() {
                        let cr = clearing::clear(
                            &mut data.accounts,
                            id,
                            &symbol,
                            Decimal::zero(),
                            Decimal::zero(),
                            mr.as_ref().unwrap(),
                            time,
                        );
                        // FIXME
                        sender.send(cr).unwrap();
                        (id, true)
                    } else {
                        (id, true)
                    }
                }
                None => (id, true),
            }
        }
        &Event::CancelAll(id, symbol, time) => {
            let mr = match data.orderbooks.get_mut(&symbol) {
                Some(orderbook) => {
                    let ids = orderbook.indices.keys().map(|k| *k).collect::<Vec<_>>();
                    ids.into_iter()
                        .map(|id| cancel(orderbook, id))
                        .collect::<Vec<_>>()
                }
                None => vec![],
            };
            mr.iter()
                .filter(|r| r.is_some())
                .map(|r| r.as_ref().unwrap())
                .for_each(|r| {
                    let cr = clearing::clear(
                        &mut data.accounts,
                        id,
                        &symbol,
                        Decimal::zero(),
                        Decimal::zero(),
                        &r,
                        time,
                    );
                    // FIXME
                    sender.send(cr).unwrap();
                });
            (id, true)
        }
        &Event::Open(id, symbol, _) => {
            let orderbook = data.orderbooks.get_mut(&symbol);
            match orderbook {
                None => (id, false),
                Some(orderbook) => {
                    orderbook.open = true;
                    (id, true)
                }
            }
        }
        &Event::Close(id, symbol, _) => {
            let orderbook = data.orderbooks.get_mut(&symbol);
            match orderbook {
                None => (id, false),
                Some(orderbook) => {
                    orderbook.open = false;
                    (id, true)
                }
            }
        }
        &Event::OpenAll(id, _) => {
            data.orderbooks.iter_mut().for_each(|(_, v)| v.open = true);
            (id, true)
        }
        &Event::CloseAll(id, _) => {
            data.orderbooks.iter_mut().for_each(|(_, v)| v.open = false);
            (id, true)
        }
        &Event::TransferOut(id, user, currency, amount, _) => {
            let ok = assets::deduct_available(&mut data.accounts, user, currency, amount);
            (id, ok)
        }
        &Event::TransferIn(id, user, currency, amount, _) => {
            let ok = assets::add_to_available(&mut data.accounts, user, currency, amount);
            (id, ok)
        }
        &Event::NewSymbol(
            id,
            symbol,
            base_scale,
            quote_scale,
            taker_fee,
            maker_fee,
            min_amount,
            min_vol,
            enable_market_order,
            _,
        ) => {
            if !data.orderbooks.contains_key(&symbol) {
                let orderbook = OrderBook::new(
                    base_scale,
                    quote_scale,
                    taker_fee,
                    maker_fee,
                    min_amount,
                    min_vol,
                    enable_market_order,
                );
                data.orderbooks.insert(symbol, orderbook);
                (id, true)
            } else {
                (id, false)
            }
        }
        &Event::UpdateSymbol(
            id,
            symbol,
            base_scale,
            quote_scale,
            taker_fee,
            maker_fee,
            min_amount,
            min_vol,
            enable_market_order,
            _,
        ) => match data.orderbooks.get_mut(&symbol) {
            Some(orderbook) => {
                orderbook.base_scale = base_scale;
                orderbook.quote_scale = quote_scale;
                orderbook.taker_fee = taker_fee;
                orderbook.maker_fee = maker_fee;
                orderbook.min_amount = min_amount;
                orderbook.min_vol = min_vol;
                orderbook.enable_market_order = enable_market_order;
                (id, true)
            }
            None => (id, false),
        },
        &Event::Dump(id, time) => {
            snapshot::dump(id, time, &data);
            // tricky way, return u64::MAX to update nothing
            (u64::MAX, true)
        }
    }
}

fn do_inspect(inspection: &Inspection, data: &Data) {
    match inspection {
        &Inspection::QueryOrder(symbol, order_id, session, req_id) => {
            match data.orderbooks.get(&symbol) {
                Some(orderbook) => {
                    let v = match orderbook.find_order(order_id) {
                        Some(order) => serde_json::to_vec(order).unwrap_or(vec![]),
                        None => vec![],
                    };
                    server::publish(server::Message::with_payload(session, req_id, v));
                }
                None => {
                    server::publish(server::Message::with_payload(session, req_id, vec![]));
                }
            }
        }
        &Inspection::QueryBalance(user_id, currency, session, req_id) => {
            let v = match assets::get(&data.accounts, user_id, currency) {
                None => serde_json::to_vec(&assets::Account {
                    available: Decimal::new(0, 0),
                    frozen: Decimal::new(0, 0),
                })
                .unwrap(),
                Some(a) => serde_json::to_vec(a).unwrap(),
            };
            server::publish(server::Message::with_payload(session, req_id, v));
        }
        &Inspection::QueryAccounts(user_id, session, req_id) => {
            let v = match data.accounts.get(&user_id) {
                None => serde_json::to_vec(&Accounts::new()).unwrap(),
                Some(all) => serde_json::to_vec(all).unwrap(),
            };
            server::publish(server::Message::with_payload(session, req_id, v));
        }
        &Inspection::UpdateDepth => {
            let writing = data
                .orderbooks
                .iter()
                .map(|(k, v)| v.as_depth(32, *k))
                .collect::<Vec<_>>();
            output::write_depth(writing);
        }
        &Inspection::ConfirmAll(from, exclude) => sequence::confirm(from, exclude),
    }
}

#[test]
pub fn test() {
    assert_eq!("{}", serde_json::to_string(&Accounts::new()).unwrap());
    let mut account = std::collections::HashMap::<u32, assets::Account>::new();
    account.insert(
        100,
        assets::Account {
            available: Decimal::new(200, 1),
            frozen: Decimal::new(0, 0),
        },
    );
    assert_eq!(
        r#"{"100":{"available":"20.0","frozen":"0"}}"#,
        serde_json::to_string(&account).unwrap()
    );
    assert_eq!(
        r#"{"available":"0","frozen":"0"}"#,
        serde_json::to_string(&assets::Account {
            available: Decimal::new(0, 0),
            frozen: Decimal::new(0, 0),
        })
        .unwrap()
    );
    assert_eq!(true, Decimal::zero().is_sign_positive());
    assert_eq!(false, Decimal::zero().is_sign_negative());
}
