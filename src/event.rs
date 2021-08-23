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

use crate::{assets, clearing, core::*, matcher, orderbook::*, output, sequence, server, snapshot};
use cfg_if::cfg_if;
use rust_decimal::{prelude::Zero, Decimal};
use serde::{Deserialize, Serialize};
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

impl Event {
    pub fn is_trading_cmd(&self) -> bool {
        matches!(self, Event::Limit(_, _, _, _, _, _, _, _))
            || matches!(self, Event::Market(_, _, _, _, _, _, _))
            || matches!(self, Event::Cancel(_, _, _, _, _))
    }

    pub fn is_assets_cmd(&self) -> bool {
        matches!(self, Event::TransferIn(_, _, _, _, _))
            || matches!(self, Event::TransferOut(_, _, _, _, _))
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Deserialize, Serialize)]
pub enum Inspection {
    ConfirmAll(u64, u64),
    UpdateDepth,
    QueryOrder(Symbol, OrderId, u64, u64),
    QueryBalance(UserId, Currency, u64, u64),
    QueryAccounts(UserId, u64, u64),
}

impl Default for Inspection {
    fn default() -> Self {
        Self::UpdateDepth
    }
}

#[derive(Debug)]
pub enum EventsError {
    Interrupted,
    EventRejected(u64),
}

type EventExecutionResult = Result<Vec<output::Output>, EventsError>;

pub fn init(recv: Receiver<sequence::Fusion>, sender: Sender<Vec<output::Output>>, mut data: Data) {
    thread::spawn(move || -> EventExecutionResult {
        cfg_if! {
            if #[cfg(feature = "prover")] {
                use crate::prover::Prover;
                let prover = Prover::init().map_err(|_| EventsError::Interrupted)?;
            }
        }
        loop {
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
                    let inspection = watch.to_inspection().ok_or(EventsError::Interrupted)?;
                    do_inspect(inspection, &data)?;
                }
                sequence::Fusion::W(seq) => {
                    if !seq.cmd.validate() {
                        log::info!("illegal sequence {:?}", seq);
                        sequence::update_sequence_status(seq.id, sequence::ERROR)
                            .map_err(|_| EventsError::Interrupted)?;
                        continue;
                    }
                    let event = seq.to_event().ok_or(EventsError::Interrupted)?;
                    let result = handle_event(event.clone(), &mut data);
                    match result {
                        Err(EventsError::EventRejected(id)) => {
                            log::info!("execute sequence {:?} failed", seq);
                            sequence::update_sequence_status(id, sequence::ERROR)
                                .map_err(|_| EventsError::Interrupted)?;
                        }
                        Err(EventsError::Interrupted) => {
                            panic!("sequence thread panic");
                        }
                        Ok(out) => {
                            cfg_if! {
                                if #[cfg(feature = "prover")] {
                                    if event.is_trading_cmd() {
                                        prover.prove_trading_cmd(&mut data, &out);
                                    } else if event.is_assets_cmd() {
                                    }
                                }
                            }
                            sender.send(out).map_err(|_| EventsError::Interrupted)?;
                        }
                    }
                }
            }
        }
    });
}

fn handle_event(event: Event, data: &mut Data) -> EventExecutionResult {
    match event {
        Event::Limit(id, symbol, user, order, price, amount, ask_or_bid, time) => {
            let orderbook = data
                .orderbooks
                .get_mut(&symbol)
                .ok_or(EventsError::EventRejected(id))?;
            if !orderbook.should_accept(price, amount) {
                return Err(EventsError::EventRejected(id));
            }
            let (currency, val) = assets::freeze_if(&symbol, ask_or_bid, price, amount);
            assets::try_freeze(&mut data.accounts, user, currency, val)
                .map_err(|_| EventsError::EventRejected(id))?;
            let mr = matcher::execute_limit(orderbook, user, order, price, amount, ask_or_bid);
            Ok(clearing::clear(
                &mut data.accounts,
                id,
                &symbol,
                orderbook.taker_fee,
                orderbook.maker_fee,
                &mr,
                time,
            ))
        }
        Event::Market(id, _, _, _, _, _, _) => Err(EventsError::EventRejected(id)),
        Event::Cancel(id, symbol, user, order_id, time) => {
            // 0. symbol exsits
            // 1. check order's owner
            let orderbook = data
                .orderbooks
                .get_mut(&symbol)
                .ok_or(EventsError::EventRejected(id))?;
            let order = orderbook
                .find_order(order_id)
                .ok_or(EventsError::EventRejected(id))?;
            if order.user != user {
                return Err(EventsError::EventRejected(id));
            }
            let mr = matcher::cancel(orderbook, order_id).ok_or(EventsError::EventRejected(id))?;
            Ok(clearing::clear(
                &mut data.accounts,
                id,
                &symbol,
                Decimal::zero(),
                Decimal::zero(),
                &mr,
                time,
            ))
        }
        Event::CancelAll(id, symbol, time) => {
            let orderbook = data
                .orderbooks
                .get_mut(&symbol)
                .ok_or(EventsError::EventRejected(id))?;
            let ids = orderbook.indices.keys().copied().collect::<Vec<_>>();
            let mrs = ids
                .into_iter()
                .map(|id| matcher::cancel(orderbook, id))
                .filter(|mr| mr.is_some())
                .collect::<Vec<_>>();
            Ok(mrs
                .iter()
                .map(|mr| {
                    clearing::clear(
                        &mut data.accounts,
                        id,
                        &symbol,
                        Decimal::zero(),
                        Decimal::zero(),
                        mr.as_ref().unwrap(),
                        time,
                    )
                })
                .flatten()
                .collect::<Vec<_>>())
        }
        Event::Open(id, symbol, _) => {
            let orderbook = data
                .orderbooks
                .get_mut(&symbol)
                .ok_or(EventsError::EventRejected(id))?;
            orderbook.open = true;
            Ok(vec![])
        }
        Event::Close(id, symbol, _) => {
            let orderbook = data
                .orderbooks
                .get_mut(&symbol)
                .ok_or(EventsError::EventRejected(id))?;
            orderbook.open = false;
            Ok(vec![])
        }
        Event::OpenAll(_, _) => {
            data.orderbooks.iter_mut().for_each(|(_, v)| v.open = true);
            Ok(vec![])
        }
        Event::CloseAll(_, _) => {
            data.orderbooks.iter_mut().for_each(|(_, v)| v.open = false);
            Ok(vec![])
        }
        Event::TransferOut(id, user, currency, amount, _) => {
            let ok = assets::deduct_available(&mut data.accounts, user, currency, amount);
            if ok {
                // TODO
                Ok(vec![])
            } else {
                Err(EventsError::EventRejected(id))
            }
        }
        Event::TransferIn(_, user, currency, amount, _) => {
            assets::add_to_available(&mut data.accounts, user, currency, amount);
            // TODO
            Ok(vec![])
        }
        Event::NewSymbol(
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
                Ok(vec![])
            } else {
                Err(EventsError::EventRejected(id))
            }
        }
        Event::UpdateSymbol(
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
                Ok(vec![])
            }
            None => Err(EventsError::EventRejected(id)),
        },
        Event::Dump(id, time) => {
            snapshot::dump(id, time, data);
            Ok(vec![])
        }
    }
}

fn do_inspect(inspection: Inspection, data: &Data) -> EventExecutionResult {
    match inspection {
        Inspection::QueryOrder(symbol, order_id, session, req_id) => {
            let v = match data.orderbooks.get(&symbol) {
                Some(orderbook) => orderbook.find_order(order_id).map_or(vec![], |order| {
                    serde_json::to_vec(order).unwrap_or_default()
                }),
                None => vec![],
            };
            server::publish(server::Message::with_payload(session, req_id, v));
        }
        Inspection::QueryBalance(user_id, currency, session, req_id) => {
            let a = assets::get_to_owned(&data.accounts, &user_id, currency);
            let v = serde_json::to_vec(&a).unwrap_or_default();
            server::publish(server::Message::with_payload(session, req_id, v));
        }
        Inspection::QueryAccounts(user_id, session, req_id) => {
            let a = assets::get_all_to_owned(&data.accounts, &user_id);
            let v = serde_json::to_vec(&a).unwrap_or_default();
            server::publish(server::Message::with_payload(session, req_id, v));
        }
        Inspection::UpdateDepth => {
            let writing = data
                .orderbooks
                .iter()
                .map(|(k, v)| v.as_depth(32, *k))
                .collect::<Vec<_>>();
            output::write_depth(writing);
        }
        Inspection::ConfirmAll(from, exclude) => {
            sequence::confirm(from, exclude).map_err(|_| EventsError::Interrupted)?;
        }
    }
    Ok(vec![])
}

#[test]
pub fn test_serialize() {
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
