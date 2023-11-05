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

pub mod sequencer;
pub mod server;

pub use sequencer::*;

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct Input {
    pub session: u64,
    pub req_id: u64,
    pub sequence: u64,
    pub cmd: Command,
}

impl TryInto<Event> for Input {
    type Error = anyhow::Error;

    fn try_into(self) -> anyhow::Result<Event> {
        match self.cmd.cmd {
            ASK_LIMIT | BID_LIMIT => {
                let amount = self.cmd.amount.ok_or(anyhow!(""))?;
                let price = self.cmd.price.ok_or(anyhow!(""))?;
                ensure!(
                    price.is_sign_positive() && price.scale() <= 7,
                    "invalid price numeric"
                );
                ensure!(
                    amount.is_sign_positive() && amount.scale() <= 7,
                    "invalid amount numeric"
                );
                let vol = amount.checked_mul(price).ok_or(anyhow!(""))?;
                ensure!(vol.validate(), "overflow");
                let cmd = LimitCmd {
                    symbol: self.cmd.symbol().ok_or(anyhow!(""))?,
                    user_id: UserId::from_str(self.cmd.user_id.as_ref().ok_or(anyhow!(""))?)?,
                    order_id: self.cmd.order_id.ok_or(anyhow!(""))?,
                    price,
                    amount,
                    ask_or_bid: AskOrBid::try_from(self.cmd.cmd)?,
                    nonce: self.cmd.nonce.ok_or(anyhow!(""))?,
                    signature: hex::decode(self.cmd.signature.ok_or(anyhow!(""))?)?,
                    broker: self
                        .cmd
                        .broker
                        .map(|b| UserId::from_str(b.as_ref()))
                        .transpose()?,
                };
                Ok(Event::Limit(
                    self.sequence,
                    cmd,
                    self.timestamp,
                    self.session,
                    self.req_id,
                ))
            }
            CANCEL => Ok(Event::Cancel(
                self.sequence,
                CancelCmd {
                    symbol: self.cmd.symbol().ok_or(anyhow!(""))?,
                    user_id: UserId::from_str(self.cmd.user_id.as_ref().ok_or(anyhow!(""))?)?,
                    order_id: self.cmd.order_id.ok_or(anyhow!(""))?,
                    nonce: self.cmd.nonce.ok_or(anyhow!(""))?,
                    signature: hex::decode(self.cmd.signature.ok_or(anyhow!(""))?)?,
                },
                self.timestamp,
                self.session,
                self.req_id,
            )),
            TRANSFER_OUT => Ok(Event::TransferOut(
                self.sequence,
                AssetsCmd {
                    user_id: UserId::from_str(self.cmd.user_id.as_ref().ok_or(anyhow!(""))?)?,
                    in_or_out: InOrOut::Out,
                    currency: self.cmd.currency.ok_or(anyhow!(""))?,
                    amount: self
                        .cmd
                        .amount
                        .filter(|a| a.is_sign_positive())
                        .ok_or(anyhow!(""))?,
                    block_number: self.cmd.block_number.ok_or(anyhow!(""))?,
                    extrinsic_hash: hex::decode(self.cmd.extrinsic_hash.ok_or(anyhow!(""))?)?,
                },
                self.timestamp,
            )),
            TRANSFER_IN => Ok(Event::TransferIn(
                self.sequence,
                AssetsCmd {
                    user_id: UserId::from_str(self.cmd.user_id.as_ref().ok_or(anyhow!(""))?)?,
                    in_or_out: InOrOut::In,
                    currency: self.cmd.currency.ok_or(anyhow!(""))?,
                    amount: self
                        .cmd
                        .amount
                        .filter(|a| a.is_sign_positive())
                        .ok_or(anyhow!(""))?,
                    block_number: self.cmd.block_number.ok_or(anyhow!(""))?,
                    extrinsic_hash: hex::decode(self.cmd.extrinsic_hash.ok_or(anyhow!(""))?)?,
                },
                self.timestamp,
            )),
            UPDATE_SYMBOL => Ok(Event::UpdateSymbol(
                self.sequence,
                SymbolCmd {
                    symbol: self.cmd.symbol().ok_or(anyhow!(""))?,
                    open: self.cmd.open.ok_or(anyhow!(""))?,
                    base_scale: self.cmd.base_scale.filter(|b| *b <= 7).ok_or(anyhow!(""))?,
                    quote_scale: self
                        .cmd
                        .quote_scale
                        .filter(|q| *q <= 7)
                        .ok_or(anyhow!(""))?,
                    taker_fee: self
                        .cmd
                        .taker_fee
                        .filter(|f| f.is_sign_positive())
                        .ok_or(anyhow!(""))?,
                    maker_fee: self
                        .cmd
                        .maker_fee
                        .filter(|f| f.is_sign_positive())
                        .ok_or(anyhow!(""))?,
                    base_maker_fee: self
                        .cmd
                        .base_maker_fee
                        .filter(|f| f.is_sign_positive())
                        .or(self.cmd.maker_fee)
                        .filter(|f| f.is_sign_positive())
                        .ok_or(anyhow!(""))?,
                    base_taker_fee: self
                        .cmd
                        .base_taker_fee
                        .filter(|f| f.is_sign_positive())
                        .or(self.cmd.taker_fee)
                        .filter(|f| f.is_sign_positive())
                        .ok_or(anyhow!(""))?,
                    fee_times: self.cmd.fee_times.unwrap_or(1),
                    min_amount: self
                        .cmd
                        .min_amount
                        .filter(|f| f.is_sign_positive())
                        .ok_or(anyhow!(""))?,
                    min_vol: self
                        .cmd
                        .min_vol
                        .filter(|f| f.is_sign_positive())
                        .ok_or(anyhow!(""))?,
                    enable_market_order: self.cmd.enable_market_order.ok_or(anyhow!(""))?,
                },
                self.timestamp,
                self.session,
                self.req_id,
            )),
            QUERY_ORDER => Ok(Event::QueryOrder(
                self.cmd.symbol().ok_or(anyhow!(""))?,
                self.cmd.order_id.ok_or(anyhow!(""))?,
                self.session,
                self.req_id,
            )),
            QUERY_BALANCE => Ok(Event::QueryBalance(
                UserId::from_str(self.cmd.user_id.as_ref().ok_or(anyhow!(""))?)?,
                self.cmd.currency.ok_or(anyhow!(""))?,
                self.session,
                self.req_id,
            )),
            QUERY_ACCOUNTS => Ok(Event::QueryAccounts(
                UserId::from_str(self.cmd.user_id.as_ref().ok_or(anyhow!(""))?)?,
                self.session,
                self.req_id,
            )),
            UPDATE_DEPTH => Ok(Event::UpdateDepth),
            CONFIRM_ALL => Ok(Event::ConfirmAll(
                self.cmd.from.ok_or(anyhow!(""))?,
                self.cmd.exclude.ok_or(anyhow!(""))?,
            )),
            QUERY_EXCHANGE_FEE => Ok(Event::QueryExchangeFee(
                self.cmd.symbol().ok_or(anyhow!(""))?,
                self.session,
                self.req_id,
            )),
            DUMP => Ok(Event::Dump(
                self.cmd.event_id.ok_or(anyhow!(""))?,
                self.cmd.timestamp.ok_or(anyhow!(""))?,
            )),
            _ => Err(anyhow!("Unsupported Command")),
        }
    }
}

unsafe impl Send for Input {}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum Event {
    // write
    Limit(EventId, LimitCmd, Timestamp, u64, u64),
    Cancel(EventId, CancelCmd, Timestamp, u64, u64),
    TransferOut(EventId, AssetsCmd, Timestamp),
    TransferIn(EventId, AssetsCmd, Timestamp),
    UpdateSymbol(EventId, SymbolCmd, Timestamp),
    // read
    QueryOrder(Symbol, OrderId, u64, u64),
    QueryBalance(UserId, Currency, u64, u64),
    QueryAccounts(UserId, u64, u64),
    QueryExchangeFee(Symbol, u64, u64),
    // special: `EventId` means dump at `EventId`
    Dump(EventId, Timestamp),
}

impl Event {}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LimitCmd {
    pub symbol: Symbol,
    pub user_id: UserId,
    // TODO this field is deprecated
    pub order_id: OrderId,
    pub price: Price,
    pub amount: Amount,
    pub ask_or_bid: AskOrBid,
    pub nonce: u32,
    pub signature: Vec<u8>,
    pub broker: Option<UserId>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CancelCmd {
    pub symbol: Symbol,
    pub user_id: UserId,
    pub order_id: OrderId,
    pub nonce: u32,
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
            crate::cmd::TRANSFER_IN => Ok(InOrOut::In),
            crate::cmd::TRANSFER_OUT => Ok(InOrOut::Out),
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
    pub block_number: u32,
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

pub mod cmd {
    pub const ASK_LIMIT: u32 = 0;
    pub const BID_LIMIT: u32 = 1;
    pub const CANCEL: u32 = 4;
    #[must_not_use]
    pub const CANCEL_ALL: u32 = 5; /* DEPRECATED */
    pub const TRANSFER_OUT: u32 = 10;
    pub const TRANSFER_IN: u32 = 11;
    pub const UPDATE_SYMBOL: u32 = 13;

    pub const QUERY_ORDER: u32 = 14;
    pub const QUERY_BALANCE: u32 = 15;
    pub const QUERY_ACCOUNTS: u32 = 16;
    pub const QUERY_EXCHANGE_FEE: u32 = 21;

    pub const DUMP: u32 = 17;
    #[must_not_use]
    pub const UPDATE_DEPTH: u32 = 18; /* DEPRECATED */
    #[must_not_use]
    pub const CONFIRM_ALL: u32 = 19; /* DEPRECATED */

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

/// header = 0x0316<2bytes payload len><2bytes cheskcum><2bytes flag>
pub const MAX_FRAME_SIZE: usize = 64 * 1024;

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
