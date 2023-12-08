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
pub mod orders;

use crate::{
    core::*,
    input::{self, Event, Message},
    orderbook::*,
    output::Output,
    prover, snapshot,
};
use anyhow::anyhow;
use rust_decimal::{prelude::*, Decimal};
use serde_json::{json, to_vec};
use std::{
    collections::HashMap,
    sync::mpsc::{Receiver, Sender},
};
use thiserror::Error;

type DriverChannel = Receiver<Event>;
type MarketChannel = Sender<Vec<Output>>;
type ResponseChannel = Sender<(u64, Message)>;

#[derive(Debug, Error)]
pub enum EventsError {
    #[error("event {0} interrupted")]
    Interrupted(u64),
    /// event id, session id, request id, error
    #[error("event {0} executed failed, {3}")]
    EventRejected(u64, u64, u64, anyhow::Error),
    /// event id, error
    #[error("event {0} executed failed, {1}")]
    EventIgnored(u64, anyhow::Error),
}

pub type ExecutionResult = Result<(), EventsError>;

pub fn init(recv: DriverChannel, market: MarketChannel, response: ResponseChannel, mut data: Data) {
    std::thread::spawn(move || -> anyhow::Result<()> {
        let mut ephemeral = Ephemeral::new();
        log::info!("executor initialized");
        loop {
            let event = recv.recv()?;
            match do_execute(event, &mut data, &mut ephemeral, &market, &response) {
                Ok(_) => {}
                Err(EventsError::EventRejected(id, session, req_id, e)) => {
                    log::debug!("event {} rejected: {}", id, e);
                    let msg = json!({"error": e.to_string()});
                    let v = to_vec(&msg).unwrap_or_default();
                    let _ = response.send((session, Message::new_req(req_id, v)));
                }
                Err(EventsError::EventIgnored(id, e)) => {
                    log::info!("event {} ignored: {}", id, e);
                }
                Err(EventsError::Interrupted(id)) => {
                    log::info!("executor thread interrupted at {}", id);
                    break;
                }
            }
        }
        Err(anyhow!("executor thread exited"))
    });
}

fn do_execute(
    event: Event,
    data: &mut Data,
    ephemeral: &mut Ephemeral,
    market: &MarketChannel,
    response: &ResponseChannel,
) -> ExecutionResult {
    match event {
        Event::Limit(id, cmd, time, session, req_id) => {
            data.current_event_id = id;
            let orderbook = data
                .orderbooks
                .get_mut(&cmd.symbol)
                .filter(|b| b.should_accept(cmd.price, cmd.amount))
                .ok_or(EventsError::EventRejected(
                    id,
                    session,
                    req_id,
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
                .map_err(|e| EventsError::EventRejected(id, session, req_id, e))?;
            let mr = matcher::execute_limit(
                orderbook,
                cmd.user_id,
                cmd.price,
                cmd.amount,
                cmd.ask_or_bid,
            );
            data.orders.insert(PendingOrder {
                order_id: mr.taker.order_id,
                user_id: cmd.user_id,
                symbol: cmd.symbol,
                direction: mr.taker.ask_or_bid.into(),
                create_timestamp: time,
                amount: cmd.amount,
                price: cmd.price,
                status: OrderState::Placed.into(),
                matched_quote_amount: Decimal::zero(),
                matched_base_amount: Decimal::zero(),
                base_fee: Decimal::zero(),
                quote_fee: Decimal::zero(),
            });
            // compatiable with old version since we don't use mysql auto increment id anymore
            if session != 0 {
                response
                    .send((
                        session,
                        Message::new_req(
                            req_id,
                            to_vec(&json!({
                                "id": mr.taker.order_id
                            }))
                            .expect("qed;"),
                        ),
                    ))
                    .map_err(|_| EventsError::Interrupted(id))?;
            }
            let out = clearing::clear(
                &mut data.accounts,
                id,
                &cmd.symbol,
                orderbook.taker_fee,
                orderbook.maker_fee,
                &mr,
                time,
            );
            for cr in out.iter() {
                let o = data.orders.merge(&cr);
                if session != 0 {
                    // broadcast to all sessions
                    response
                        .send((
                            0,
                            Message::new_broadcast(
                                input::ORDER_MATCHED,
                                to_vec(&o).unwrap_or_default(),
                            ),
                        ))
                        .map_err(|_| EventsError::Interrupted(id))?;
                }
            }
            let (maker_fee, taker_fee) = (orderbook.maker_fee, orderbook.taker_fee);
            let proof = prover::prove_trade_cmd(
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
            prover::save_proof(proof)
                .inspect_err(|e| log::error!("{}", e))
                .map_err(|_| EventsError::Interrupted(id))?;
            market.send(out).map_err(|_| EventsError::Interrupted(id))?;
            Ok(())
        }
        Event::Cancel(id, cmd, time, session, req_id) => {
            data.current_event_id = id;
            // 0. symbol exsits
            // 1. check order's owner
            let orderbook =
                data.orderbooks
                    .get_mut(&cmd.symbol)
                    .ok_or(EventsError::EventRejected(
                        id,
                        session,
                        req_id,
                        anyhow!("orderbook not found"),
                    ))?;
            orderbook
                .find_order(cmd.order_id)
                .filter(|o| o.user == cmd.user_id)
                .ok_or(EventsError::EventRejected(
                    id,
                    session,
                    req_id,
                    anyhow!("order doesn't exist"),
                ))?;
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
            let mr = matcher::cancel(orderbook, cmd.order_id).ok_or(EventsError::EventRejected(
                id,
                session,
                req_id,
                anyhow!("order doesn't exist"),
            ))?;
            if session != 0 {
                response
                    .send((
                        session,
                        Message::new_req(
                            req_id,
                            to_vec(&json!({
                                "id": cmd.order_id
                            }))
                            .expect("qed;"),
                        ),
                    ))
                    .map_err(|_| EventsError::Interrupted(id))?;
            }
            let out = clearing::clear(
                &mut data.accounts,
                id,
                &cmd.symbol,
                orderbook.taker_fee,
                orderbook.maker_fee,
                &mr,
                time,
            );
            for cr in out.iter() {
                data.orders.merge(&cr);
            }
            let proof = prover::prove_trade_cmd(
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
            prover::save_proof(proof)
                .inspect_err(|e| log::error!("{}", e))
                .map_err(|_| EventsError::Interrupted(id))?;
            market.send(out).map_err(|_| EventsError::Interrupted(id))?;
            Ok(())
        }
        Event::TransferOut(id, cmd) => {
            data.current_event_id = id;
            if !ephemeral.save_receipt((cmd.block_number, cmd.user_id)) {
                return Err(EventsError::EventIgnored(
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
                let proof = prover::prove_cmd_rejected(&mut data.merkle_tree, id, cmd, &before);
                log::error!("TVL less than transfer_out amount, event={}", id);
                prover::save_proof(proof)
                    .inspect_err(|e| log::error!("{}", e))
                    .map_err(|_| EventsError::Interrupted(id))?;
                return Err(EventsError::EventIgnored(id, anyhow!("TVL not enough")));
            }
            match assets::deduct_available(
                &mut data.accounts,
                &cmd.user_id,
                cmd.currency,
                cmd.amount,
            ) {
                Ok(after) => {
                    data.tvl -= cmd.amount;
                    let proof =
                        prover::prove_assets_cmd(&mut data.merkle_tree, id, cmd, &before, &after);
                    prover::save_proof(proof)
                        .inspect_err(|e| log::error!("{}", e))
                        .map_err(|_| EventsError::Interrupted(id))?;
                    Ok(())
                }
                Err(e) => {
                    let proof = prover::prove_cmd_rejected(&mut data.merkle_tree, id, cmd, &before);
                    prover::save_proof(proof)
                        .inspect_err(|e| log::error!("{}", e))
                        .map_err(|_| EventsError::Interrupted(id))?;
                    Err(EventsError::EventIgnored(id, e))
                }
            }
        }
        Event::TransferIn(id, cmd) => {
            data.current_event_id = id;
            if !ephemeral.save_receipt((cmd.block_number, cmd.user_id)) {
                return Err(EventsError::EventIgnored(
                    id,
                    anyhow!("Duplicated transfer_in extrinsic"),
                ));
            }
            if data.tvl + cmd.amount >= crate::core::max_number() {
                let before =
                    assets::get_balance_to_owned(&data.accounts, &cmd.user_id, cmd.currency);
                let proof =
                    prover::prove_rejecting_no_reason(&mut data.merkle_tree, id, cmd, &before);
                prover::save_proof(proof)
                    .inspect_err(|e| log::error!("{}", e))
                    .map_err(|_| EventsError::Interrupted(id))?;
                log::error!("TVL out of limit, event={}", id);
                return Err(EventsError::EventIgnored(id, anyhow!("TVL out of limit")));
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
            .map_err(|e| EventsError::EventIgnored(id, e))?;
            data.tvl = data.tvl + cmd.amount;
            let proof = prover::prove_assets_cmd(&mut data.merkle_tree, id, cmd, &before, &after);
            prover::save_proof(proof)
                .inspect_err(|e| log::error!("{}", e))
                .map_err(|_| EventsError::Interrupted(id))?;
            Ok(())
        }
        Event::UpdateSymbol(id, cmd) => {
            data.current_event_id = id;
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
        Event::QueryOrder(symbol, order_id, session, req_id) => {
            let v = match data.orderbooks.get(&symbol) {
                Some(orderbook) => orderbook
                    .find_order(order_id)
                    .map_or(vec![], |order| to_vec(order).unwrap_or_default()),
                None => vec![],
            };
            let _ = response.send((session, Message::new_req(req_id, v)));
            Ok(())
        }
        Event::QueryUserOrders(symbol, user_id, session, req_id) => {
            let o = data.orders.list(user_id, symbol);
            let v = to_vec(&o).unwrap_or_default();
            let _ = response.send((session, Message::new_req(req_id, v)));
            Ok(())
        }
        Event::QueryBalance(user_id, currency, session, req_id) => {
            let a = assets::get_balance_to_owned(&data.accounts, &user_id, currency);
            let v = to_vec(&a).unwrap_or_default();
            let _ = response.send((session, Message::new_req(req_id, v)));
            Ok(())
        }
        Event::QueryAccounts(user_id, session, req_id) => {
            let a = assets::get_account_to_owned(&data.accounts, &user_id);
            let v = to_vec(&a).unwrap_or_default();
            let _ = response.send((session, Message::new_req(req_id, v)));
            Ok(())
        }
        Event::QueryExchangeFee(symbol, session, req_id) => {
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
            let v = to_vec(&v).unwrap_or_default();
            let _ = response.send((session, Message::new_req(req_id, v)));
            Ok(())
        }
        Event::Dump(id) => {
            snapshot::dump(id, data);
            Ok(())
        }
    }
}
