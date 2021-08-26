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
use std::convert::TryInto;
use std::sync::mpsc::{Receiver, Sender};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum Event {
    Limit(EventId, LimitCmd, Timestamp),
    Cancel(EventId, CancelCmd, Timestamp),
    TransferOut(EventId, AssetsCmd, Timestamp),
    TransferIn(EventId, AssetsCmd, Timestamp),
    UpdateSymbol(EventId, SymbolCmd, Timestamp),
    #[cfg(not(feature = "fusotao"))]
    CancelAll(EventId, Symbol, Timestamp),
    // special: `EventId` means dump at `EventId`
    Dump(EventId, Timestamp),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LimitCmd {
    pub symbol: Symbol,
    pub user_id: UserId,
    pub order_id: OrderId,
    pub price: Price,
    pub amount: Amount,
    pub ask_or_bid: AskOrBid,
    #[cfg(feature = "fusotao")]
    pub nonce: u32,
    #[cfg(feature = "fusotao")]
    pub signature: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CancelCmd {
    pub symbol: Symbol,
    pub user_id: UserId,
    pub order_id: OrderId,
    #[cfg(feature = "fusotao")]
    pub nonce: u32,
    #[cfg(feature = "fusotao")]
    pub signature: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AssetsCmd {
    pub user_id: UserId,
    pub currency: Currency,
    pub amount: Amount,
    #[cfg(feature = "fusotao")]
    pub nonce_or_block_number: u32,
    #[cfg(feature = "fusotao")]
    pub signature_or_hash: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SymbolCmd {
    pub symbol: Symbol,
    pub open: bool,
    pub base_scale: Scale,
    pub quote_scale: Scale,
    pub taker_fee: Fee,
    pub maker_fee: Fee,
    pub min_amount: Amount,
    pub min_vol: Vol,
    pub enable_market_order: bool,
}

impl Event {
    pub fn is_trading_cmd(&self) -> bool {
        matches!(self, Event::Limit(_, _, _)) || matches!(self, Event::Cancel(_, _, _))
    }

    pub fn is_assets_cmd(&self) -> bool {
        matches!(self, Event::TransferIn(_, _, _)) || matches!(self, Event::TransferOut(_, _, _))
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

type EventExecutionResult = Result<(), EventsError>;
type OutputChannel = Sender<Vec<output::Output>>;
type DriverChannel = Receiver<sequence::Fusion>;

pub fn init(recv: DriverChannel, sender: OutputChannel, mut data: Data) {
    std::thread::spawn(move || -> EventExecutionResult {
        cfg_if! {
            if #[cfg(feature = "fusotao")] {
                use crate::fusotao;
                let (tx, rx) = std::sync::mpsc::channel();
                fusotao::init(rx).map_err(|_| EventsError::Interrupted)?;
                let prover = fusotao::Prover::new(tx).map_err(|_| EventsError::Interrupted)?;
            }
        }
        loop {
            let fusion = recv.recv().unwrap();
            match fusion {
                sequence::Fusion::R(watch) => {
                    let (s, r) = (watch.session, watch.req_id);
                    if let Ok(inspection) = watch.try_into() {
                        do_inspect(inspection, &data)?;
                    } else {
                        server::publish(server::Message::with_payload(s, r, vec![]));
                    }
                }
                sequence::Fusion::W(seq) => {
                    let id = seq.id;
                    if let Ok(event) = seq.try_into() {
                        cfg_if! {
                            if #[cfg(feature = "fusotao")] {
                                let result = handle_event(event, &mut data, &sender, &prover);
                            } else {
                                let result = handle_event(event, &mut data, &sender);
                            }
                        }
                        match result {
                            Err(EventsError::EventRejected(id)) => {
                                sequence::update_sequence_status(id, sequence::ERROR)
                                    .map_err(|_| EventsError::Interrupted)?;
                            }
                            Err(EventsError::Interrupted) => {
                                panic!("sequence thread panic");
                            }
                            Ok(()) => {}
                        }
                    } else {
                        sequence::update_sequence_status(id, sequence::ERROR)
                            .map_err(|_| EventsError::Interrupted)?;
                    }
                }
            }
        }
    });
}

fn handle_event(
    event: Event,
    data: &mut Data,
    sender: &OutputChannel,
    #[cfg(feature = "fusotao")] prover: &crate::fusotao::Prover,
) -> EventExecutionResult {
    match event {
        Event::Limit(id, cmd, time) => {
            let orderbook = data
                .orderbooks
                .get_mut(&cmd.symbol)
                .ok_or(EventsError::EventRejected(id))?;
            if !orderbook.should_accept(cmd.price, cmd.amount) {
                return Err(EventsError::EventRejected(id));
            }
            let (currency, val) =
                assets::freeze_if(&cmd.symbol, cmd.ask_or_bid, cmd.price, cmd.amount);
            assets::try_freeze(&mut data.accounts, cmd.user_id, currency, val)
                .map_err(|_| EventsError::EventRejected(id))?;
            let mr = matcher::execute_limit(
                orderbook,
                cmd.user_id,
                cmd.order_id,
                cmd.price,
                cmd.amount,
                cmd.ask_or_bid,
            );
            let out = clearing::clear(
                &mut data.accounts,
                id,
                &cmd.symbol,
                orderbook.taker_fee,
                orderbook.maker_fee,
                &mr,
                time,
            );
            cfg_if! {
                if #[cfg(feature = "fusotao")] {
                    prover.prove_trading_cmd(data, &out, cmd.nonce, vec![], "".to_string())
                          .map_err(|_| EventsError::Interrupted)?;
                }
            }
            sender.send(out).map_err(|_| EventsError::Interrupted)?;
            Ok(())
        }
        Event::Cancel(id, cmd, time) => {
            // 0. symbol exsits
            // 1. check order's owner
            let orderbook = data
                .orderbooks
                .get_mut(&cmd.symbol)
                .ok_or(EventsError::EventRejected(id))?;
            let order = orderbook
                .find_order(cmd.order_id)
                .ok_or(EventsError::EventRejected(id))?;
            if order.user != cmd.user_id {
                return Err(EventsError::EventRejected(id));
            }
            let mr =
                matcher::cancel(orderbook, cmd.order_id).ok_or(EventsError::EventRejected(id))?;
            let out = clearing::clear(
                &mut data.accounts,
                id,
                &cmd.symbol,
                Decimal::zero(),
                Decimal::zero(),
                &mr,
                time,
            );
            cfg_if! {
                if #[cfg(feature = "fusotao")] {
                    prover.prove_trading_cmd(data, &out, cmd.nonce, vec![], "".to_string())
                          .map_err(|_| EventsError::Interrupted)?;
                }
            }
            sender.send(out).map_err(|_| EventsError::Interrupted)?;
            Ok(())
        }
        #[cfg(not(feature = "fusotao"))]
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
            let out = mrs
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
                .collect::<Vec<_>>();
            sender.send(out).map_err(|_| EventsError::Interrupted)?;
            Ok(())
        }
        Event::TransferOut(id, cmd, _) => {
            assets::deduct_available(&mut data.accounts, cmd.user_id, cmd.currency, cmd.amount)
                .map_err(|_| EventsError::EventRejected(id))?;
            cfg_if! {
                if #[cfg(feature = "fusotao")] {
                }
            }
            Ok(())
        }
        Event::TransferIn(_, cmd, _) => {
            assets::add_to_available(&mut data.accounts, cmd.user_id, cmd.currency, cmd.amount);
            cfg_if! {
                if #[cfg(feature = "fusotao")] {
                }
            }
            Ok(())
        }
        Event::UpdateSymbol(_, cmd, _) => {
            if !data.orderbooks.contains_key(&cmd.symbol) {
                let orderbook = OrderBook::new(
                    cmd.base_scale,
                    cmd.quote_scale,
                    cmd.taker_fee,
                    cmd.maker_fee,
                    cmd.min_amount,
                    cmd.min_vol,
                    cmd.enable_market_order,
                    cmd.open,
                );
                data.orderbooks.insert(cmd.symbol, orderbook);
            } else {
                let orderbook = data.orderbooks.get_mut(&cmd.symbol).unwrap();
                orderbook.base_scale = cmd.base_scale;
                orderbook.quote_scale = cmd.quote_scale;
                orderbook.taker_fee = cmd.taker_fee;
                orderbook.maker_fee = cmd.maker_fee;
                orderbook.min_amount = cmd.min_amount;
                orderbook.min_vol = cmd.min_vol;
                orderbook.enable_market_order = cmd.enable_market_order;
                orderbook.open = cmd.open;
            }
            Ok(())
        }
        Event::Dump(id, time) => {
            snapshot::dump(id, time, data);
            Ok(())
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
    Ok(())
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
