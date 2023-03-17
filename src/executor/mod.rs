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
    fusotao::{Proof, Prover},
    input::{Event, EventsError, Input, Inspection},
    orderbook::*,
    output::{self, Output},
    sequence,
    server::Message,
    snapshot,
};
use anyhow::anyhow;
use rust_decimal::{prelude::*, Decimal};
use std::{
    collections::HashMap,
    convert::TryInto,
    sync::mpsc::{Receiver, Sender},
};

type EventExecutionResult = Result<(), EventsError>;
type OutputChannel = Sender<Vec<Output>>;
type DriverChannel = Receiver<Input>;
type ProofChannel = Sender<Proof>;
type BackToServer = Sender<Message>;

pub fn init(
    recv: DriverChannel,
    sender: OutputChannel,
    proofs: ProofChannel,
    messages: BackToServer,
    mut data: Data,
) {
    std::thread::spawn(move || {
        let prover = Prover::new(proofs);
        let mut ephemeral = Ephemeral::new();
        log::info!("executor initialized");
        loop {
            let fusion = recv.recv().unwrap();
            match fusion {
                Input::NonModifier(whistle) => {
                    let (s, r) = (whistle.session, whistle.req_id);
                    if let Ok(inspection) = whistle.try_into() {
                        do_inspect(inspection, &data, &messages).unwrap();
                    } else {
                        let _ = messages.send(Message::with_payload(s, r, vec![]));
                    }
                }
                Input::Modifier(seq) => {
                    let id = seq.id;
                    match seq.try_into() {
                        Ok(event) => {
                            let r = do_event(event, &mut data, &mut ephemeral, &sender, &prover);
                            match r {
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

fn do_event(
    event: Event,
    data: &mut Data,
    ephemeral: &mut Ephemeral,
    sender: &OutputChannel,
    prover: &Prover,
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
            log::debug!(
                "predicate root=0x{} before applying {}",
                hex::encode(data.merkle_tree.root()),
                id
            );
            let (ask_size, bid_size) = orderbook.size();
            let (best_ask_before, best_bid_before) = orderbook.get_size_of_best();
            let taker_base_before =
                assets::get_balance_to_owned(&data.accounts, &cmd.user_id, cmd.symbol.0);
            let taker_quote_before =
                assets::get_balance_to_owned(&data.accounts, &cmd.user_id, cmd.symbol.1);
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
            log::debug!(
                "predicate root=0x{} before applying {}",
                hex::encode(data.merkle_tree.root()),
                id
            );
            let size = orderbook.size();
            let (best_ask_before, best_bid_before) = orderbook.get_size_of_best();
            let taker_base_before =
                assets::get_balance_to_owned(&data.accounts, &cmd.user_id, cmd.symbol.0);
            let taker_quote_before =
                assets::get_balance_to_owned(&data.accounts, &cmd.user_id, cmd.symbol.1);

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
            sender.send(out).map_err(|_| EventsError::Interrupted)?;
            Ok(())
        }
        Event::CancelAll(..) => {
            // let orderbook = data
            //     .orderbooks
            //     .get_mut(&symbol)
            //     .ok_or(EventsError::EventRejected(
            //         id,
            //         anyhow!("orderbook not exists"),
            //     ))?;
            // let ids = orderbook.indices.keys().copied().collect::<Vec<_>>();
            // let matches = ids
            //     .into_iter()
            //     .filter_map(|id| matcher::cancel(orderbook, id))
            //     .collect::<Vec<_>>();
            // let (taker_fee, maker_fee) = (orderbook.taker_fee, orderbook.maker_fee);
            // matches.iter().for_each(|mr| {
            //     let out = clearing::clear(
            //         &mut data.accounts,
            //         id,
            //         &symbol,
            //         taker_fee,
            //         maker_fee,
            //         mr,
            //         time,
            //     );
            //     sender.send(out).unwrap();
            // });
            Ok(())
        }
        Event::TransferOut(id, cmd, _) => {
            if !ephemeral.save_receipt((cmd.block_number, cmd.user_id)) {
                return Err(EventsError::EventRejected(
                    id,
                    anyhow!("Duplicated transfer_out extrinsic"),
                ));
            }
            log::debug!(
                "predicate root=0x{} before applying {}",
                hex::encode(data.merkle_tree.root()),
                id
            );
            let before = assets::get_balance_to_owned(&data.accounts, &cmd.user_id, cmd.currency);
            if data.tvl < cmd.amount {
                prover.prove_cmd_rejected(&mut data.merkle_tree, id, cmd, &before);
                log::error!("TVL less than transfer_out amount, event={}", id);
                return Err(EventsError::EventRejected(id, anyhow!("LessThanTVL")));
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
        }
        Event::TransferIn(id, cmd, _) => {
            if !ephemeral.save_receipt((cmd.block_number, cmd.user_id)) {
                return Err(EventsError::EventRejected(
                    id,
                    anyhow!("Duplicated transfer_in extrinsic"),
                ));
            }
            if data.tvl + cmd.amount >= crate::core::max_number() {
                let before =
                    assets::get_balance_to_owned(&data.accounts, &cmd.user_id, cmd.currency);
                prover.prove_rejecting_no_reason(&mut data.merkle_tree, id, cmd, &before);
                log::error!("TVL out of limit, event={}", id);
                return Err(EventsError::EventRejected(id, anyhow!("TVLOutOfLimit")));
            }
            log::debug!(
                "predicate root=0x{} before applying {}",
                hex::encode(data.merkle_tree.root()),
                id
            );
            let before = assets::get_balance_to_owned(&data.accounts, &cmd.user_id, cmd.currency);
            let after = assets::add_to_available(
                &mut data.accounts,
                &cmd.user_id,
                cmd.currency,
                cmd.amount,
            )
            .map_err(|e| EventsError::EventRejected(id, e))?;
            data.tvl = data.tvl + cmd.amount;
            prover.prove_assets_cmd(&mut data.merkle_tree, id, cmd, &before, &after);
            Ok(())
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
    messages: &BackToServer,
) -> EventExecutionResult {
    match inspection {
        Inspection::QueryOrder(symbol, order_id, session, req_id) => {
            let v = match data.orderbooks.get(&symbol) {
                Some(orderbook) => orderbook.find_order(order_id).map_or(vec![], |order| {
                    serde_json::to_vec(order).unwrap_or_default()
                }),
                None => vec![],
            };
            let _ = messages.send(Message::with_payload(session, req_id, v));
        }
        Inspection::QueryBalance(user_id, currency, session, req_id) => {
            let a = assets::get_balance_to_owned(&data.accounts, &user_id, currency);
            let v = serde_json::to_vec(&a).unwrap_or_default();
            let _ = messages.send(Message::with_payload(session, req_id, v));
        }
        Inspection::QueryAccounts(user_id, session, req_id) => {
            let a = assets::get_account_to_owned(&data.accounts, &user_id);
            let v = serde_json::to_vec(&a).unwrap_or_default();
            let _ = messages.send(Message::with_payload(session, req_id, v));
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
            let _ = messages.send(Message::with_payload(session, req_id, v));
        }
        Inspection::Dump(id, time) => {
            snapshot::dump(id, time, data);
        }
    }
    Ok(())
}
