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

use crate::core::*;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

pub mod event;
pub mod sequence;
pub mod server;
pub mod whistle;

pub use event::*;
pub use sequence::*;
pub use whistle::*;

/// Input
///     sequence = command + database_header -> event
///     whistle = command + network_header -> inspection
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub enum Input {
    Modifier(Sequence),
    NonModifier(Whistle),
}

unsafe impl Send for Input {}

pub mod cmd {
    pub const ASK_LIMIT: u32 = 0;
    pub const BID_LIMIT: u32 = 1;
    pub const CANCEL: u32 = 4;
    pub const CANCEL_ALL: u32 = 5;
    pub const TRANSFER_OUT: u32 = 10;
    pub const TRANSFER_IN: u32 = 11;
    pub const UPDATE_SYMBOL: u32 = 13;
    pub const QUERY_ORDER: u32 = 14;
    pub const QUERY_BALANCE: u32 = 15;
    pub const QUERY_ACCOUNTS: u32 = 16;
    pub const DUMP: u32 = 17;
    pub const UPDATE_DEPTH: u32 = 18;
    pub const CONFIRM_ALL: u32 = 19;
    pub const PROVING_PERF_INDEX_CHECK: u32 = 20;
    pub const QUERY_EXCHANGE_FEE: u32 = 21;
    pub const QUERY_PROVING_PERF_INDEX: u32 = 22;
    pub const QUERY_SCAN_HEIGHT: u32 = 23;
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Default)]
pub struct Command {
    pub cmd: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quote: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub currency: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vol: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub amount: Option<Amount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<Price>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nonce: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extrinsic_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_number: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_scale: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quote_scale: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub taker_fee: Option<Fee>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub maker_fee: Option<Fee>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_taker_fee: Option<Fee>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_maker_fee: Option<Fee>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fee_times: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_amount: Option<Amount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_vol: Option<Amount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub open: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enable_market_order: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exclude: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<u64>,
}

unsafe impl Send for Command {}

use crate::cmd::*;

impl Command {
    pub fn symbol(&self) -> Option<Symbol> {
        Some((self.base?, self.quote?))
    }

    #[must_use]
    pub const fn is_read(&self) -> bool {
        matches!(
            self.cmd,
            QUERY_ACCOUNTS
                | QUERY_BALANCE
                | QUERY_ORDER
                | DUMP
                | QUERY_EXCHANGE_FEE
                | QUERY_PROVING_PERF_INDEX
                | PROVING_PERF_INDEX_CHECK
                | QUERY_SCAN_HEIGHT
        )
    }
}
