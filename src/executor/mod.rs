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

pub mod assets;
pub mod clearing;
pub mod matcher;
pub mod orderbook;

use crate::{
    core::*,
    event::*,
    input::Input,
    orderbook::*,
    output::{self, Output},
    sequence, server, snapshot,
};
use anyhow::anyhow;
#[cfg(feature = "fusotao")]
use rust_decimal::prelude::*;
use rust_decimal::Decimal;
use std::{
    collections::HashMap,
    convert::TryInto,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{Receiver, Sender},
        Arc,
    },
};

type EventExecutionResult = Result<(), EventsError>;
type OutputChannel = Sender<Vec<Output>>;
type DriverChannel = Receiver<Input>;

pub fn init(recv: DriverChannel, sender: OutputChannel, mut data: Data, ready: Arc<AtomicBool>) {
    std::thread::spawn(move || {
        cfg_if::cfg_if! {
            if #[cfg(feature = "fusotao")] {
                use crate::fusotao;
                let (tx, rx) = std::sync::mpsc::channel();
                let proved_event_id = fusotao::init(rx).unwrap();
                let prover = fusotao::Prover::new(tx, proved_event_id);
            }
        }
        ready.store(true, Ordering::Relaxed);
        log::info!("event handler initialized");
        loop {
            let fusion = recv.recv().unwrap();
            match fusion {
                Input::NonModifier(whistle) => {
                    let (s, r) = (whistle.session, whistle.req_id);
                    if let Ok(inspection) = whistle.try_into() {
                        cfg_if::cfg_if! {
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
                Input::Modifier(seq) => {
                    let id = seq.id;
                    match seq.try_into() {
                        Ok(event) => {
                            cfg_if::cfg_if! {
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
                .filter(|b| b.should_accept(cmd.price, cmd.amount, cmd.order_id))
                .filter(|b| b.find_order(cmd.order_id).is_none())
                .ok_or(EventsError::EventRejected(
                    id,
                    anyhow!("order can't be accepted"),
                ))?;
            cfg_if::cfg_if! {
                if #[cfg(feature = "fusotao")] {
                    log::info!("predicate root=0x{} before applying {}", hex::encode(data.merkle_tree.root()), id);
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
            cfg_if::cfg_if! {
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
                        &mr,
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
            cfg_if::cfg_if! {
                if #[cfg(feature = "fusotao")] {
                    log::debug!("predicate root=0x{} before applying {}", hex::encode(data.merkle_tree.root()), id);
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
            cfg_if::cfg_if! {
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
                        &mr,
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
            cfg_if::cfg_if! {
                if #[cfg(feature = "fusotao")] {
                    log::info!("predicate root=0x{} before applying {}", hex::encode(data.merkle_tree.root()), id);
                    let before = assets::get_balance_to_owned(&data.accounts, &cmd.user_id, cmd.currency);
                    if data.tvl < cmd.amount {
                        prover.prove_cmd_rejected(&mut data.merkle_tree, id, cmd, &before);
                        log::error!("TVL less than transfer_out amount, event={}", id);
                        return Err(EventsError::EventRejected(id, anyhow::anyhow!("LessThanTVL")));
                    }
                    match assets::deduct_available(
                        &mut data.accounts,
                        &cmd.user_id,
                        cmd.currency,
                        cmd.amount,
                    ) {
                        Ok(after) => {
                            data.tvl -= cmd.amount;
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
            cfg_if::cfg_if! {
                if #[cfg(feature = "fusotao")] {
                    if data.tvl + cmd.amount >= crate::core::max_number() {
                        let before = assets::get_balance_to_owned(&data.accounts, &cmd.user_id, cmd.currency);
                        prover.prove_rejecting_no_reason(&mut data.merkle_tree, id, cmd, &before);
                        log::error!("TVL out of limit, event={}", id);
                        return Err(EventsError::EventRejected(id, anyhow::anyhow!("TVLOutOfLimit")));
                    }
                    log::info!("predicate root=0x{} before applying {}", hex::encode(data.merkle_tree.root()), id);
                    let before = assets::get_balance_to_owned(&data.accounts, &cmd.user_id, cmd.currency);
                    let after = assets::add_to_available(
                        &mut data.accounts,
                        &cmd.user_id,
                        cmd.currency,
                        cmd.amount,
                    ).map_err(|e| EventsError::EventRejected(id, e))?;
                    data.tvl = data.tvl + cmd.amount;
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
        #[cfg(feature = "fusotao")]
        Inspection::QueryScanHeight(session, req_id) => {
            let scaned_height = scaned_height();
            let chain_height = chain_height();
            let mut v: HashMap<String, u32> = HashMap::new();
            v.insert(String::from("scaned_height"), scaned_height);
            v.insert(String::from("chain_height"), chain_height);
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
        Inspection::ProvingPerfIndexCheck(_id) => {}
    }
    Ok(())
}

#[cfg(feature = "fusotao")]
fn scaned_height() -> u32 {
    let mut scaned_height = 0u32;
    let path: std::path::PathBuf = [&crate::config::C.sequence.coredump_dir, "fusotao.blk"]
        .iter()
        .collect();
    let finalized_file = std::fs::OpenOptions::new()
        .read(true)
        .write(false)
        .create(false)
        .open(&path);
    if let Ok(f) = finalized_file {
        let blk = unsafe { memmap::Mmap::map(&f) };
        scaned_height = match blk {
            Ok(b) => u32::from_le_bytes(b.as_ref().try_into().unwrap_or_default()),
            Err(_) => 0u32,
        };
    }
    scaned_height
}

#[cfg(feature = "fusotao")]
fn chain_height() -> u32 {
    use crate::fusotao::{FusoApi, FusoBlock};
    std::panic::catch_unwind(|| {
        let client =
            sub_api::rpc::WsRpcClient::new(&crate::config::C.fusotao.as_ref().unwrap().node_url);
        let api = FusoApi::new(client)
            .map_err(|e| {
                log::error!("{:?}", e);
                anyhow!("Fusotao node not available or runtime metadata check failed")
            })
            .unwrap();
        let r: Option<FusoBlock> = api.get_block(None).unwrap_or(None);
        r.map_or(0u32, |b| b.header.number)
    })
    .unwrap_or(0u32)
}