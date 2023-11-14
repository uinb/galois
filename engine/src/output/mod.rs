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

use crate::{core::*, matcher::*, orderbook::*};
use rocksdb::{Direction, IteratorMode, WriteBatchWithTransaction};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

pub mod market;

#[derive(Debug, Clone)]
pub struct Output {
    pub event_id: u64,
    pub order_id: u64,
    pub user_id: UserId,
    pub symbol: Symbol,
    pub state: State,
    pub role: Role,
    pub ask_or_bid: AskOrBid,
    pub price: Price,
    pub quote_charge: Amount,
    pub quote_delta: Amount,
    pub quote_available: Amount,
    pub quote_frozen: Amount,
    pub base_charge: Amount,
    pub base_delta: Amount,
    pub base_available: Amount,
    pub base_frozen: Amount,
    pub timestamp: u64,
}

// 24 + 32 + 4 + 4 => prefix + user_id + symbol
// pub(crate) fn id_to_key(user_id: &UserId, symbol: &Symbol) -> [u8; 64] {
//     let mut key = [0xaf; 64];
//     key[24..].copy_from_slice(&user_id.0[..]);
//     key[56..].copy_from_slice(&symbol.0.to_be_bytes());
//     key[60..].copy_from_slice(&symbol.1.to_be_bytes());
//     key
// }

// pub(crate) fn lower_key() -> [u8; 64] {
//     let mut key = [0xaf; 64];
//     key[24..].copy_from_slice(&[0x00; 40]);
//     key
// }

// pub(crate) fn key_to_id(key: &[u8]) -> (UserId, Symbol) {
//     let mut user_id = [0u8; 32];
//     let mut base = [0u8; 4];
//     let mut quote = [0u8; 4];
//     user_id.copy_from_slice(&key[24..56]);
//     base.copy_from_slice(&key[56..60]);
//     quote.copy_from_slice(&key[60..]);
//     (
//         B256::new(user_id),
//         (u32::from_be_bytes(base), u32::from_be_bytes(quote)),
//     )
// }

// pub(crate) fn value_to_order(value: &[u8]) -> anyhow::Result<PendingOrder> {
//     let order = bincode::deserialize(value)?;
//     Ok(order)
// }

// pub(crate) fn order_to_value(order: &PendingOrder) -> anyhow::Result<Vec<u8>> {
//     let b = bincode::serialize(order)?;
//     Ok(b)
// }
