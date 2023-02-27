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

use crate::{core::*, orderbook::*};
use serde::{Deserialize, Serialize};
use thiserror::Error;

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
    #[cfg(feature = "fusotao")]
    QueryScanHeight(u64, u64),
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

#[test]
pub fn test_serialize() {
    use rust_decimal_macros::dec;

    assert_eq!("{}", serde_json::to_string(&Accounts::new()).unwrap());
    let mut account = Account::default();
    account.insert(
        100,
        crate::assets::Balance {
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
        serde_json::to_string(&crate::assets::Balance {
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
}
