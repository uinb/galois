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

use crate::{core::*, matcher::*, orderbook::*, output::*};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PendingOrder {
    order_id: u64,
    symbol: Symbol,
    direction: u8,
    create_timestamp: u64,
    amount: Decimal,
    price: Decimal,
    status: u8,
    matched_quote_amount: Decimal,
    matched_base_amount: Decimal,
    base_fee: Decimal,
    quote_fee: Decimal,
}

impl PendingOrder {
    pub fn reduce(&mut self, cr: &Output) {
        self.matched_base_amount += cr.base_delta.abs();
        self.matched_quote_amount += cr.quote_delta.abs();
        self.status = cr.state.into();
        self.base_fee += cr.base_charge;
        self.quote_fee += cr.quote_charge;
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct UserOrders {
    pub orders: HashMap<(UserId, Symbol), HashMap<OrderId, PendingOrder>>,
}

impl UserOrders {
    pub fn new() -> Self {
        Self {
            orders: HashMap::new(),
        }
    }

    pub fn list(&self, user_id: UserId, symbol: Symbol) -> Vec<PendingOrder> {
        self.orders
            .get(&(user_id, symbol))
            .map(|orders| orders.values().cloned().collect())
            .unwrap_or_default()
    }

    pub fn insert(&mut self, order: PendingOrder) {
        self.orders
            .entry((order.user_id, order.symbol))
            .or_insert_with(HashMap::new)
            .insert(order.order_id, order);
    }

    pub fn remove(&mut self, user_id: UserId, symbol: Symbol, order_id: OrderId) {
        self.orders
            .get_mut(&(user_id, symbol))
            .map(|orders| orders.remove(&order_id));
        if self
            .orders
            .get(&(user_id, symbol))
            .map(|m| m.is_empty())
            .unwrap_or(false)
        {
            self.orders.remove(&(user_id, symbol));
        }
    }

    pub fn merge(&mut self, cr: &Output) {
        match cr.state {
            State::Placed => {}
            State::Canceled => {
                self.remove(cr.user_id, cr.symbol, cr.order_id);
            }
            State::Filled => {
                self.remove(cr.user_id, cr.symbol, cr.order_id);
            }
            State::PartiallyFilled => {
                self.orders
                    .entry((cr.user_id, cr.symbol))
                    .or_insert(Default::default())
                    .entry(cr.order_id)
                    .and_modify(|o| o.reduce(&cr));
            }
            State::ConditionallyCanceled => {
                self.remove(cr.user_id, cr.symbol, cr.order_id);
            }
        }
    }
}
