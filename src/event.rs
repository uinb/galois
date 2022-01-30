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

use std::{
    collections::HashMap,
    convert::TryInto,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{Receiver, Sender},
        Arc,
    },
};

use anyhow::anyhow;
use cfg_if::cfg_if;
use rust_decimal::{prelude::*, Decimal};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    assets, clearing,
    config::C,
    core::*,
    matcher,
    orderbook::*,
    output, sequence,
    sequence::{Command, UPDATE_SYMBOL},
    server, snapshot,
};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum Event {
    Limit(EventId, LimitCmd, Timestamp),
    Cancel(EventId, CancelCmd, Timestamp),
    TransferOut(EventId, AssetsCmd, Timestamp),
    TransferIn(EventId, AssetsCmd, Timestamp),
    UpdateSymbol(EventId, SymbolCmd, Timestamp),
    #[cfg(not(feature = "fusotao"))]
    CancelAll(EventId, Symbol, Timestamp),
}

impl Event {
    pub fn is_trading_cmd(&self) -> bool {
        matches!(self, Event::Limit(_, _, _)) || matches!(self, Event::Cancel(_, _, _))
    }

    pub fn is_assets_cmd(&self) -> bool {
        matches!(self, Event::TransferIn(_, _, _)) || matches!(self, Event::TransferOut(_, _, _))
    }

    pub fn get_id(&self) -> u64 {
        match self {
            Event::Limit(id, _, _) => *id,
            Event::Cancel(id, _, _) => *id,
            Event::TransferOut(id, _, _) => *id,
            Event::TransferIn(id, _, _) => *id,
            Event::UpdateSymbol(id, _, _) => *id,
            #[cfg(not(feature = "fusotao"))]
            Event::CancelAll(id, _, _) => *id,
        }
    }
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

impl std::convert::TryFrom<u32> for InOrOut {
    type Error = anyhow::Error;

    fn try_from(x: u32) -> anyhow::Result<Self> {
        match x {
            crate::sequence::TRANSFER_IN => Ok(InOrOut::In),
            crate::sequence::TRANSFER_OUT => Ok(InOrOut::Out),
            _ => Err(anyhow::anyhow!("")),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AssetsCmd {
    pub user_id: UserId,
    pub in_or_out: InOrOut,
    pub currency: Currency,
    pub amount: Amount,
    #[cfg(feature = "fusotao")]
    pub block_number: u32,
    #[cfg(feature = "fusotao")]
    pub extrinsic_hash: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SymbolCmd {
    pub symbol: Symbol,
    pub open: bool,
    pub base_scale: Scale,
    pub quote_scale: Scale,
    pub taker_fee: Fee,
    pub maker_fee: Fee,
    pub base_maker_fee: Fee,
    pub base_taker_fee: Fee,
    pub fee_times: u32,
    pub min_amount: Amount,
    pub min_vol: Vol,
    pub enable_market_order: bool,
}

#[derive(Debug, Eq, PartialEq, Clone, Deserialize, Serialize, Copy)]
pub enum Inspection {
    ConfirmAll(u64, u64),
    UpdateDepth,
    QueryOrder(Symbol, OrderId, u64, u64),
    QueryBalance(UserId, Currency, u64, u64),
    QueryAccounts(UserId, u64, u64),
    #[cfg(feature = "fusotao")]
    QueryProvingPerfIndex(u64, u64),
    QueryExchangeFee(Symbol, u64, u64),
    // special: `EventId` means dump at `EventId`
    Dump(EventId, Timestamp),
    #[cfg(feature = "fusotao")]
    ProvingPerfIndexCheck(EventId),
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
                let proved_event_id = Arc::new(AtomicU64::new(0));
                let prover = fusotao::Prover::new(tx, proved_event_id.clone());
                fusotao::init(rx, proved_event_id).unwrap();
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
                        cfg_if! {
                            if #[cfg(feature = "fusotao")] {
                                do_inspect(inspection, &data, &prover).unwrap();
                            }else {
                                do_inspect(inspection, &data).unwrap();
                            }
                        }
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
    data.current_event_id = event.get_id();
    match event {
        Event::Limit(id, cmd, time) => {
            let orderbook = data
                .orderbooks
                .get_mut(&cmd.symbol)
                .filter(|b| b.should_accept(cmd.price, cmd.amount, id))
                .filter(|b| b.find_order(cmd.order_id).is_none())
                .ok_or(EventsError::EventRejected(
                    id,
                    anyhow!("order can't be accepted"),
                ))?;
            cfg_if! {
                if #[cfg(feature = "fusotao")] {
                    log::info!("predicate root={:02x?} before applying {}", data.merkle_tree.root(), id);
                    let (ask_size, bid_size) = orderbook.size();
                    let (best_ask_before, best_bid_before) = orderbook.get_size_of_best();
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
                    let (maker_fee, taker_fee) = (orderbook.maker_fee, orderbook.taker_fee);
                    prover.prove_trade_cmd(
                        data,
                        cmd.nonce,
                        cmd.signature.clone(),
                        (cmd, maker_fee, taker_fee).into(),
                        ask_size,
                        bid_size,
                        best_ask_before.unwrap_or((Decimal::zero(), Decimal::zero())),
                        best_bid_before.unwrap_or((Decimal::zero(), Decimal::zero())),
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
                    log::info!("predicate root={:02x?} before applying {}", data.merkle_tree.root(), id);
                    let size = orderbook.size();
                    let (best_ask_before, best_bid_before) = orderbook.get_size_of_best();
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
                        best_ask_before.unwrap_or((Decimal::zero(), Decimal::zero())),
                        best_bid_before.unwrap_or((Decimal::zero(), Decimal::zero())),
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
                    log::info!("predicate root={:02x?} before applying {}", data.merkle_tree.root(), id);
                    let before = assets::get_balance_to_owned(&data.accounts, &cmd.user_id, cmd.currency);
                    match assets::deduct_available(
                        &mut data.accounts,
                        &cmd.user_id,
                        cmd.currency,
                        cmd.amount,
                    ) {
                        Ok(after) => {
                            prover.prove_assets_cmd(&mut data.merkle_tree, id, cmd, &before, &after);
                            Ok(())
                        }
                        Err(e) => {
                            prover.prove_cmd_rejected(&mut data.merkle_tree, id, cmd, &before);
                            Err(EventsError::EventRejected(id, e))
                        }
                    }
                } else {
                    assets::deduct_available(
                        &mut data.accounts,
                        &cmd.user_id,
                        cmd.currency,
                        cmd.amount,
                    ).map_err(|e|EventsError::EventRejected(id, e))?;
                    Ok(())
                }
            }
        }
        Event::TransferIn(id, cmd, _) => {
            cfg_if! {
                if #[cfg(feature = "fusotao")] {
                    log::info!("predicate root={:02x?} before applying {}", data.merkle_tree.root(), id);
                    let before = assets::get_balance_to_owned(&data.accounts, &cmd.user_id, cmd.currency);
                    let after = assets::add_to_available(
                        &mut data.accounts,
                        &cmd.user_id,
                        cmd.currency,
                        cmd.amount,
                    ).map_err(|e| EventsError::EventRejected(id, e))?;
                    prover.prove_assets_cmd(&mut data.merkle_tree, id, cmd, &before, &after);
                    Ok(())
                } else {
                    assets::add_to_available(
                        &mut data.accounts,
                        &cmd.user_id,
                        cmd.currency,
                        cmd.amount,
                    ).map_err(|e| EventsError::EventRejected(id, e))?;
                    Ok(())
                }
            }
        }
        Event::UpdateSymbol(_, cmd, _) => {
            if !data.orderbooks.contains_key(&cmd.symbol) {
                let orderbook = OrderBook::new(
                    cmd.base_scale,
                    cmd.quote_scale,
                    cmd.taker_fee,
                    cmd.maker_fee,
                    cmd.base_taker_fee,
                    cmd.base_maker_fee,
                    cmd.fee_times,
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
                orderbook.base_maker_fee = cmd.base_maker_fee;
                orderbook.base_taker_fee = cmd.base_taker_fee;
                orderbook.fee_times = cmd.fee_times;
                orderbook.min_amount = cmd.min_amount;
                orderbook.min_vol = cmd.min_vol;
                orderbook.enable_market_order = cmd.enable_market_order;
                orderbook.open = cmd.open;
            }
            Ok(())
        }
    }
}

fn gen_adjust_fee_cmds(delta: u64, fee_adjust_threshold: u64, data: &Data) -> Vec<Command> {
    let mut times: u32 = (delta / fee_adjust_threshold) as u32;
    times = if times > 0 { times } else { 1 };
    data.orderbooks
        .iter()
        .filter(|(_, v)| times != v.fee_times)
        .map(|(k, v)| {
            let mut cmd = Command::default();
            cmd.cmd = UPDATE_SYMBOL;
            cmd.base = Some(k.0);
            cmd.quote = Some(k.1);
            cmd.open = Some(v.open);
            cmd.enable_market_order = Some(v.enable_market_order);
            cmd.fee_times = Some(times);
            cmd.base_taker_fee = Some(v.base_taker_fee);
            cmd.base_maker_fee = Some(v.base_maker_fee);
            let fee_rate_limit = Decimal::from_str("0.02").unwrap();
            let maker_fee = v.base_maker_fee * Decimal::from(times);
            let maker_fee = if maker_fee > fee_rate_limit {
                fee_rate_limit
            } else {
                maker_fee
            };
            let taker_fee = v.base_taker_fee * Decimal::from(times);
            let taker_fee = if taker_fee > fee_rate_limit {
                fee_rate_limit
            } else {
                taker_fee
            };
            cmd.maker_fee = Some(maker_fee);
            cmd.taker_fee = Some(taker_fee);
            cmd.min_amount = Some(v.min_amount);
            cmd.min_vol = Some(v.min_vol);
            cmd.quote_scale = Some(v.quote_scale);
            cmd.base_scale = Some(v.base_scale);
            cmd
        })
        .collect::<Vec<_>>()
}

#[cfg(feature = "fusotao")]
fn update_exchange_fee(delta: u64, data: &Data) {
    let cmds = gen_adjust_fee_cmds(
        delta,
        C.fusotao.as_ref().unwrap().fee_adjust_threshold,
        data,
    );
    if !cmds.is_empty() {
        let _ = sequence::insert_sequences(&cmds);
    }
}

fn do_inspect(
    inspection: Inspection,
    data: &Data,
    #[cfg(feature = "fusotao")] prover: &crate::fusotao::Prover,
) -> EventExecutionResult {
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
        #[cfg(feature = "fusotao")]
        Inspection::QueryProvingPerfIndex(session, req_id) => {
            let current_proved_event = prover.proved_event_id.load(Ordering::Relaxed);
            let mut v: HashMap<String, u64> = HashMap::new();

            let ppi = if data.current_event_id > current_proved_event {
                data.current_event_id - current_proved_event
            } else {
                current_proved_event - data.current_event_id
            };
            v.insert(String::from("proving_perf_index"), ppi);
            let v = serde_json::to_vec(&v).unwrap_or_default();
            server::publish(server::Message::with_payload(session, req_id, v));
        }
        Inspection::QueryExchangeFee(symbol, session, req_id) => {
            let mut v: HashMap<String, Fee> = HashMap::new();
            let orderbook = data.orderbooks.get(&symbol);
            match orderbook {
                Some(book) => {
                    v.insert(String::from("maker_fee"), book.maker_fee);
                    v.insert(String::from("taker_fee"), book.taker_fee);
                }
                _ => {
                    v.insert(String::from("maker_fee"), Decimal::new(0, 0));
                    v.insert(String::from("taker_fee"), Decimal::new(0, 0));
                }
            }
            let v = serde_json::to_vec(&v).unwrap_or_default();
            server::publish(server::Message::with_payload(session, req_id, v));
        }
        Inspection::Dump(id, time) => {
            snapshot::dump(id, time, data);
        }
        #[cfg(feature = "fusotao")]
        Inspection::ProvingPerfIndexCheck(id) => {
            let current_proved_event = prover.proved_event_id.load(Ordering::Relaxed);
            if current_proved_event != 0 {
                let delta = if id > current_proved_event {
                    id - current_proved_event
                } else {
                    current_proved_event - id
                };
                update_exchange_fee(delta, data);
            }
        }
    }
    Ok(())
}

#[test]
pub fn test_serialize() {
    use rust_decimal_macros::dec;

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

    let mut data = Data::new();
    let orderbook = OrderBook::new(
        8,
        8,
        dec!(0.001),
        dec!(0.001),
        dec!(0.001),
        dec!(0.001),
        1,
        dec!(0.1),
        dec!(0.1),
        true,
        true,
    );
    data.orderbooks.insert((0, 1), orderbook);
    let cmd = gen_adjust_fee_cmds(5000, 1000, &data);
    assert_eq!(1, cmd.len());
    assert_eq!(5, cmd[0].fee_times.unwrap());
    assert_eq!(dec!(0.005), cmd[0].maker_fee.unwrap());
    assert_eq!(dec!(0.005), cmd[0].taker_fee.unwrap());
    assert_eq!(dec!(0.001), cmd[0].base_maker_fee.unwrap());
    assert_eq!(dec!(0.001), cmd[0].base_taker_fee.unwrap());
}
