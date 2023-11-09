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

use crate::{config::C, input::*, output::*};
use rust_decimal::{prelude::Zero, Decimal};
use std::sync::mpsc::{Receiver, Sender};

type MarketChannel = Receiver<(bool, Vec<Output>)>;
type ResponseChannel = Sender<(u64, Message)>;

pub fn init(rx: MarketChannel, tx: ResponseChannel, orders: PendingOrders) {
    let mut orders = orders;
    std::thread::spawn(move || -> anyhow::Result<()> {
        loop {
            let (should_publish, crs) = rx.recv()?;
            if C.dry_run.is_none() {
                let mut batch = WriteBatchWithTransaction::<false>::default();
                for cr in crs {
                    match cr.state {
                        State::Placed => {
                            let order = PendingOrder {
                                order_id: cr.order_id,
                                symbol: cr.symbol,
                                direction: cr.ask_or_bid.into(),
                                create_timestamp: std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap()
                                    .as_secs(),
                                // FIXME
                                amount: Decimal::zero(),
                                price: cr.price,
                                status: cr.state.into(),
                                matched_quote_amount: Decimal::zero(),
                                matched_base_amount: Decimal::zero(),
                                base_fee: Decimal::zero(),
                                quote_fee: Decimal::zero(),
                            };
                            orders
                                .entry((cr.user_id, cr.symbol))
                                .or_insert(Default::default())
                                .insert(cr.order_id, order.clone());
                            batch.put(
                                super::id_to_key(&cr.user_id, &cr.symbol),
                                super::order_to_value(&order)?,
                            );
                        }
                        State::Canceled => {
                            // FIXME
                            orders
                                .entry((cr.user_id, cr.symbol))
                                .or_insert(Default::default())
                                .remove(&cr.order_id);
                            batch.delete(super::id_to_key(&cr.user_id, &cr.symbol));
                        }
                        State::Filled => {
                            orders
                                .entry((cr.user_id, cr.symbol))
                                .or_insert(Default::default())
                                .remove(&cr.order_id);
                            batch.delete(super::id_to_key(&cr.user_id, &cr.symbol));
                        }
                        State::PartiallyFilled => {
                            // FIXME
                            orders
                                .entry((cr.user_id, cr.symbol))
                                .or_insert(Default::default())
                                .entry(cr.order_id)
                                .and_modify(|o| o.reduce(&cr));
                            let order = orders
                                .get(&(cr.user_id, cr.symbol))
                                .unwrap()
                                .get(&cr.order_id)
                                .unwrap()
                                .clone();
                            batch.put(
                                super::id_to_key(&cr.user_id, &cr.symbol),
                                super::order_to_value(&order)?,
                            );
                        }
                        State::ConditionallyCanceled => {
                            // FIXME
                            orders
                                .entry((cr.user_id, cr.symbol))
                                .or_insert(Default::default())
                                .remove(&cr.order_id);
                            batch.delete(super::id_to_key(&cr.user_id, &cr.symbol));
                        }
                    }
                }
                OUTPUT_STORE.write(batch)?;
                if should_publish {
                    // broadcast makers only
                }
            }
        }
    });
    log::info!("market initialized");
}
