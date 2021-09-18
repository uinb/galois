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
use anyhow::anyhow;
use cfg_if::cfg_if;
use serde::{Deserialize, Serialize};
use std::convert::TryInto;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc::{Receiver, Sender},
    Arc,
};
use thiserror::Error;

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
    pub signature: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CancelCmd {
    pub symbol: Symbol,
    pub user_id: UserId,
    pub order_id: OrderId,
    #[cfg(feature = "fusotao")]
    pub nonce: u32,
    #[cfg(feature = "fusotao")]
    pub signature: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum InOrOut {
    In,
    Out,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AssetsCmd {
    pub user_id: UserId,
    pub in_or_out: InOrOut,
    pub currency: Currency,
    pub amount: Amount,
    #[cfg(feature = "fusotao")]
    pub nonce_or_block_number: u32,
    #[cfg(feature = "fusotao")]
    pub signature_or_hash: Vec<u8>,
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

#[derive(Debug, Error)]
pub enum EventsError {
    #[error("Events execution thread interrupted")]
    Interrupted,
    #[error("Error occurs in sequence {0}: {1}")]
    EventRejected(u64, anyhow::Error),
}

type EventExecutionResult = Result<(), EventsError>;
type OutputChannel = Sender<Vec<output::Output>>;
type DriverChannel = Receiver<sequence::Fusion>;

pub fn init(recv: DriverChannel, sender: OutputChannel, mut data: Data, ready: Arc<AtomicBool>) {
    std::thread::spawn(move || {
        cfg_if! {
            if #[cfg(feature = "fusotao")] {
                use crate::fusotao;
                let (tx, rx) = std::sync::mpsc::channel();
                fusotao::init(rx).unwrap();
                let prover = fusotao::Prover::new(tx);
            }
        }
        ready.store(true, Ordering::Relaxed);
        log::info!("event handler initialized");
        loop {
            let fusion = recv.recv().unwrap();
            match fusion {
                sequence::Fusion::R(watch) => {
                    let (s, r) = (watch.session, watch.req_id);
                    if let Ok(inspection) = watch.try_into() {
                        do_inspect(inspection, &data).unwrap();
                    } else {
                        server::publish(server::Message::with_payload(s, r, vec![]));
                    }
                }
                sequence::Fusion::W(seq) => {
                    let id = seq.id;
                    match seq.try_into() {
                        Ok(event) => {
                            cfg_if! {
                                if #[cfg(feature = "fusotao")] {
                                    let result = handle_event(event, &mut data, &sender, &prover);
                                } else {
                                    let result = handle_event(event, &mut data, &sender);
                                }
                            }
                            match result {
                                Err(EventsError::EventRejected(id, msg)) => {
                                    log::info!("Error occur in sequence {}: {:?}", id, msg);
                                    sequence::update_sequence_status(id, sequence::ERROR).unwrap();
                                }
                                Err(EventsError::Interrupted) => {
                                    panic!("sequence thread panic");
                                }
                                Ok(()) => {}
                            }
                        }
                        Err(e) => {
                            log::info!("Error occur in sequence {}: {:?}", id, e);
                            sequence::update_sequence_status(id, sequence::ERROR).unwrap();
                        }
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
                .filter(|b| b.should_accept(cmd.price, cmd.amount))
                .filter(|b| b.find_order(cmd.order_id).is_none())
                .ok_or(EventsError::EventRejected(
                    id,
                    anyhow!("order can't be accepted"),
                ))?;
            cfg_if! {
                if #[cfg(feature = "fusotao")] {
                    let size = (orderbook.ask_size, orderbook.bid_size);
                    let taker_base_before = assets::get_balance_to_owned(&data.accounts, &cmd.user_id, cmd.symbol.0);
                    let taker_quote_before = assets::get_balance_to_owned(&data.accounts, &cmd.user_id, cmd.symbol.1);
                }
            }
            let (c, val) = assets::freeze_if(&cmd.symbol, cmd.ask_or_bid, cmd.price, cmd.amount);
            assets::try_freeze(&mut data.accounts, &cmd.user_id, c, val)
                .map_err(|e| EventsError::EventRejected(id, e))?;
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
                    prover.prove_trade_cmd(
                        data,
                        cmd.nonce,
                        cmd.signature.clone(),
                        cmd.into(),
                        size.0,
                        size.1,
                        &taker_base_before,
                        &taker_quote_before,
                        &out,
                    );
                }
            }
            sender.send(out).map_err(|_| EventsError::Interrupted)?;
            Ok(())
        }
        Event::Cancel(id, cmd, time) => {
            // 0. symbol exsits
            // 1. check order's owner
            let orderbook =
                data.orderbooks
                    .get_mut(&cmd.symbol)
                    .ok_or(EventsError::EventRejected(
                        id,
                        anyhow!("orderbook not exists"),
                    ))?;
            orderbook
                .find_order(cmd.order_id)
                .filter(|o| o.user == cmd.user_id)
                .ok_or(EventsError::EventRejected(id, anyhow!("order not exists")))?;
            cfg_if! {
                if #[cfg(feature = "fusotao")] {
                    let size = (orderbook.ask_size, orderbook.bid_size);
                    let taker_base_before = assets::get_balance_to_owned(&data.accounts, &cmd.user_id, cmd.symbol.0);
                    let taker_quote_before = assets::get_balance_to_owned(&data.accounts, &cmd.user_id, cmd.symbol.1);
                }
            }
            let mr = matcher::cancel(orderbook, cmd.order_id)
                .ok_or(EventsError::EventRejected(id, anyhow!("")))?;
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
                    prover.prove_trade_cmd(
                        data,
                        cmd.nonce,
                        cmd.signature.clone(),
                        cmd.into(),
                        size.0,
                        size.1,
                        &taker_base_before,
                        &taker_quote_before,
                        &out,
                    );
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
                .ok_or(EventsError::EventRejected(
                    id,
                    anyhow!("orderbook not exists"),
                ))?;
            let ids = orderbook.indices.keys().copied().collect::<Vec<_>>();
            let matches = ids
                .into_iter()
                .filter_map(|id| matcher::cancel(orderbook, id))
                .collect::<Vec<_>>();
            let (taker_fee, maker_fee) = (orderbook.taker_fee, orderbook.maker_fee);
            matches.iter().for_each(|mr| {
                let out = clearing::clear(
                    &mut data.accounts,
                    id,
                    &symbol,
                    taker_fee,
                    maker_fee,
                    mr,
                    time,
                );
                sender.send(out).unwrap();
            });
            Ok(())
        }
        Event::TransferOut(id, cmd, _) => {
            cfg_if! {
                if #[cfg(feature = "fusotao")] {
                    let before = assets::get_balance_to_owned(&data.accounts, &cmd.user_id, cmd.currency);
                }
            }
            let after = assets::deduct_available(
                &mut data.accounts,
                &cmd.user_id,
                cmd.currency,
                cmd.amount,
            )
            .map_err(|e| EventsError::EventRejected(id, e))?;
            cfg_if! {
                if #[cfg(feature = "fusotao")] {
                    prover.prove_assets_cmd(&mut data.merkle_tree, id, cmd, &before, &after);
                }
            }
            Ok(())
        }
        Event::TransferIn(id, cmd, _) => {
            cfg_if! {
                if #[cfg(feature = "fusotao")] {
                    let before = assets::get_balance_to_owned(&data.accounts, &cmd.user_id, cmd.currency);
                }
            }
            let after = assets::add_to_available(
                &mut data.accounts,
                &cmd.user_id,
                cmd.currency,
                cmd.amount,
            )
            .map_err(|e| EventsError::EventRejected(id, e))?;
            cfg_if! {
                if #[cfg(feature = "fusotao")] {
                    prover.prove_assets_cmd(&mut data.merkle_tree, id, cmd, &before, &after);
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
            let a = assets::get_balance_to_owned(&data.accounts, &user_id, currency);
            let v = serde_json::to_vec(&a).unwrap_or_default();
            server::publish(server::Message::with_payload(session, req_id, v));
        }
        Inspection::QueryAccounts(user_id, session, req_id) => {
            let a = assets::get_account_to_owned(&data.accounts, &user_id);
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
    let mut account = Account::default();
    account.insert(
        100,
        assets::Balance {
            available: Amount::new(200, 1),
            frozen: Amount::new(0, 0),
        },
    );
    assert_eq!(
        r#"{"100":{"available":"20.0","frozen":"0"}}"#,
        serde_json::to_string(&account).unwrap()
    );
    assert_eq!(
        r#"{"available":"0","frozen":"0"}"#,
        serde_json::to_string(&assets::Balance {
            available: Amount::new(0, 0),
            frozen: Amount::new(0, 0),
        })
        .unwrap()
    );
}
