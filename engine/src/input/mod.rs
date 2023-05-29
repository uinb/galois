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

pub mod sequence;
pub mod server;
pub mod whistle;

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
    // from db to core
    pub const ASK_LIMIT: u32 = 0;
    pub const BID_LIMIT: u32 = 1;
    pub const CANCEL: u32 = 4;
    pub const CANCEL_ALL: u32 = 5;
    pub const TRANSFER_OUT: u32 = 10;
    pub const TRANSFER_IN: u32 = 11;
    pub const UPDATE_SYMBOL: u32 = 13;

    // from tcp to core
    pub const QUERY_ORDER: u32 = 14;
    pub const QUERY_BALANCE: u32 = 15;
    pub const QUERY_ACCOUNTS: u32 = 16;
    pub const QUERY_EXCHANGE_FEE: u32 = 21;

    // from timer to core
    pub const DUMP: u32 = 17;
    pub const UPDATE_DEPTH: u32 = 18;
    pub const CONFIRM_ALL: u32 = 19;

    // from tcp to shared
    pub const QUERY_PROVING_PERF_INDEX: u32 = 22; /* DEPRECATED  */
    pub const QUERY_SCAN_HEIGHT: u32 = 23; /* DEPRECATED */
    pub const QUERY_OPEN_MARKETS: u32 = 24;
    pub const GET_X25519_KEY: u32 = 25;
    pub const GET_NONCE_FOR_BROKER: u32 = 26;
    pub const QUERY_FUSOTAO_PROGRESS: u32 = 27;
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broker: Option<String>,
}

unsafe impl Send for Command {}

use crate::cmd::*;

impl Command {
    pub fn symbol(&self) -> Option<Symbol> {
        Some((self.base?, self.quote?))
    }

    #[must_use]
    pub const fn is_querying_core_data(&self) -> bool {
        matches!(
            self.cmd,
            QUERY_ACCOUNTS | QUERY_BALANCE | QUERY_ORDER | QUERY_EXCHANGE_FEE
        )
    }

    #[must_use]
    pub const fn is_querying_share_data(&self) -> bool {
        matches!(
            self.cmd,
            GET_NONCE_FOR_BROKER
                | GET_X25519_KEY
                | QUERY_OPEN_MARKETS
                | QUERY_FUSOTAO_PROGRESS
                | QUERY_PROVING_PERF_INDEX
                | QUERY_SCAN_HEIGHT
        )
    }

    #[must_use]
    pub const fn is_internally_generated(&self) -> bool {
        matches!(self.cmd, UPDATE_DEPTH | CONFIRM_ALL | DUMP)
    }
}

#[derive(Debug)]
pub struct Message {
    pub req_id: u64,
    pub payload: Vec<u8>,
}

const _MAGIC_N_MASK: u64 = 0x0316_0000_0000_0000;
const _PAYLOAD_MASK: u64 = 0x0000_ffff_0000_0000;
const _CHK_SUM_MASK: u64 = 0x0000_0000_ffff_0000;
const _ERR_RSP_MASK: u64 = 0x0000_0000_0000_0001;
const _NXT_FRM_MASK: u64 = 0x0000_0000_0000_0002;
pub const MAX_FRAME_SIZE: usize = 64 * 1024;
/// header = 0x0316<2bytes payload len><2bytes cheskcum><2bytes flag>

impl Message {
    pub fn new(req_id: u64, payload: Vec<u8>) -> Self {
        Self { req_id, payload }
    }

    pub fn encode(self) -> Vec<u8> {
        let frame_count = self.payload.len() / MAX_FRAME_SIZE + 1;
        let mut payload_len = self.payload.len();
        let mut all = Vec::<u8>::with_capacity(payload_len + 16 * frame_count);
        for i in 0..frame_count - 1 {
            let mut header = _MAGIC_N_MASK;
            header |= (MAX_FRAME_SIZE as u64) << 32;
            header |= 1;
            payload_len -= MAX_FRAME_SIZE;
            all.extend_from_slice(&header.to_be_bytes());
            all.extend_from_slice(&self.req_id.to_be_bytes());
            all.extend_from_slice(&self.payload[i * MAX_FRAME_SIZE..(i + 1) * MAX_FRAME_SIZE]);
        }
        let mut header = _MAGIC_N_MASK;
        header |= (payload_len as u64) << 32;
        all.extend_from_slice(&header.to_be_bytes());
        all.extend_from_slice(&self.req_id.to_be_bytes());
        all.extend_from_slice(&self.payload[(frame_count - 1) * MAX_FRAME_SIZE..]);
        all
    }

    pub const fn check_magic(header: u64) -> bool {
        (header & _MAGIC_N_MASK) == _MAGIC_N_MASK
    }

    pub const fn get_len(header: u64) -> usize {
        ((header & _PAYLOAD_MASK) >> 32) as usize
    }

    #[allow(dead_code)]
    pub const fn get_checksum(header: u64) -> u16 {
        ((header & _CHK_SUM_MASK) >> 16) as u16
    }

    pub const fn has_next_frame(header: u64) -> bool {
        (header & _NXT_FRM_MASK) == _NXT_FRM_MASK
    }
}
