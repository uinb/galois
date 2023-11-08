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

use crate::{config::C, core::*, input::*};
use connector::FusoConnector;
use dashmap::DashMap;
use parity_scale_codec::{Compact, Decode, Encode, WrapperTypeDecode, WrapperTypeEncode};
use rust_decimal::{prelude::*, Decimal};
use serde::{Deserialize, Serialize};
use smt::{blake2b::Blake2bHasher, default_store::DefaultStore, SparseMerkleTree, H256};
use std::{
    convert::TryInto,
    sync::{
        atomic::{AtomicU32, AtomicU64, Ordering},
        Arc,
    },
};

pub mod committer;
pub mod connector;
pub mod prover;
pub mod scanner;

pub type BlockNumber = u32;
pub type GlobalStates = SparseMerkleTree<Blake2bHasher, H256, DefaultStore<H256>>;
pub type Sr25519Key = sp_core::sr25519::Pair;
pub type FusoAccountId = <Sr25519Key as sp_core::Pair>::Public;
pub type FusoAddress = sp_runtime::MultiAddress<FusoAccountId, ()>;
pub type FusoHash = sp_runtime::traits::BlakeTwo256;
pub type FusoHeader = sp_runtime::generic::Header<BlockNumber, FusoHash>;
pub type FusoExtrinsic = sp_runtime::OpaqueExtrinsic;
pub type FusoBlock = sp_runtime::generic::Block<FusoHeader, FusoExtrinsic>;
pub type FusoApi = sub_api::Api<Sr25519Key, sub_api::rpc::WsRpcClient>;

const ONE_ONCHAIN: u128 = 1_000_000_000_000_000_000;
const MILL: u32 = 1_000_000;
const QUINTILL: u64 = 1_000_000_000_000_000_000;
const MAX_EXTRINSIC_SIZE: usize = 3 * 1024 * 1024;

/// AccountId of chain = MultiAddress<sp_runtime::AccountId32, ()>::Id = GenericAddress::Id
/// 1. from_ss58check() or from_ss58check_with_version()
/// 2. new or from public
pub fn sync() -> anyhow::Result<(FusoConnector, Arc<FusoState>)> {
    let connector = FusoConnector::new()?;
    let progress = connector.sync_progress()?;
    let state = FusoState::default();
    state.proved_event_id.store(progress, Ordering::Relaxed);
    log::info!("proving progress synchronized");
    Ok((connector, Arc::new(state)))
}

/// tracking essential onchain states
#[derive(Clone, Debug, Default)]
pub struct FusoState {
    pub chain_height: Arc<AtomicU32>,
    pub proved_event_id: Arc<AtomicU64>,
    pub scanning_progress: Arc<AtomicU32>,
    pub symbols: DashMap<Symbol, OnchainSymbol>,
    pub currencies: DashMap<Currency, OnchainToken>,
    pub brokers: DashMap<UserId, u32>,
}

impl FusoState {
    pub fn get_proving_progress(&self) -> u64 {
        self.proved_event_id.load(Ordering::Relaxed)
    }

    pub fn get_scanning_progress(&self) -> u32 {
        self.scanning_progress.load(Ordering::Relaxed)
    }

    pub fn get_chain_height(&self) -> u32 {
        self.chain_height.load(Ordering::Relaxed)
    }
}

#[derive(Clone, Debug)]
pub struct RawParameter(pub Vec<u8>);

impl Encode for RawParameter {
    fn encode(&self) -> Vec<u8> {
        self.0.to_owned()
    }
}

#[derive(Clone, Encode, Decode, Eq, PartialEq, Debug)]
pub enum Receipt {
    Authorize(u32, u128, u32),
    Revoke(u32, u128, u32),
    // RevokeWithCallback(u32, u128, u32, Callback),
}

#[derive(Clone, Encode, Decode, Eq, PartialEq, Debug, Serialize)]
pub enum MarketStatus {
    Registered,
    Open,
    Closed,
}

#[derive(Clone, Decode, Encode, Debug, Serialize)]
pub struct OnchainSymbol {
    pub min_base: u128,
    pub base_scale: u8,
    pub quote_scale: u8,
    pub status: MarketStatus,
    pub trading_rewards: bool,
    pub liquidity_rewards: bool,
    pub unavailable_after: Option<BlockNumber>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OffchainSymbol {
    pub symbol: Symbol,
    pub min_base: Decimal,
    pub base_scale: u8,
    pub quote_scale: u8,
}

impl From<(Symbol, OnchainSymbol)> for OffchainSymbol {
    fn from((symbol, data): (Symbol, OnchainSymbol)) -> Self {
        Self {
            symbol,
            min_base: to_decimal_represent(data.min_base).expect("far away from overflow;qed"),
            base_scale: data.base_scale,
            quote_scale: data.quote_scale,
        }
    }
}

#[derive(Encode, Decode, Clone, PartialEq, Eq, Debug)]
pub enum OnchainToken {
    // symbol, contract_address, total, stable, decimals
    NEP141(Vec<u8>, Vec<u8>, u128, bool, u8),
    ERC20(Vec<u8>, Vec<u8>, u128, bool, u8),
    BEP20(Vec<u8>, Vec<u8>, u128, bool, u8),
    FND10(Vec<u8>, u128),
    POLYGON(Vec<u8>, Vec<u8>, u128, bool, u8),
}

#[derive(Clone, Decode, Debug, Default)]
pub struct Dominator {
    pub name: Vec<u8>,
    pub staked: u128,
    pub merkle_root: [u8; 32],
    pub start_from: u32,
    pub sequence: (u64, u32),
    pub status: u8,
}

#[derive(Clone, Debug, Serialize, Deserialize, Encode)]
pub struct MerkleLeaf {
    pub key: Vec<u8>,
    pub old_v: [u8; 32],
    pub new_v: [u8; 32],
}

#[derive(Debug, Clone, Encode)]
pub struct Proof {
    pub event_id: u64,
    pub user_id: UserId,
    pub cmd: FusoCommand,
    pub leaves: Vec<MerkleLeaf>,
    pub maker_page_delta: u8,
    pub maker_account_delta: u8,
    pub merkle_proof: Vec<u8>,
    pub root: [u8; 32],
}

#[derive(Encode, Decode, Clone, Debug)]
pub struct TokenHostedEvent {
    fund_owner: FusoAccountId,
    dominator: FusoAccountId,
    token_id: u32,
    amount: u128,
}

#[derive(Encode, Decode, Clone, Debug)]
pub struct TokenRevokedEvent {
    fund_owner: FusoAccountId,
    dominator: FusoAccountId,
    token_id: u32,
    amount: u128,
}

#[derive(Encode, Decode, Clone, Debug)]
pub struct TokenIssuedEvent {
    token_id: u32,
    symbol_name: Vec<u8>,
}

#[derive(Encode, Decode, Clone, Debug)]
pub struct BrokerRegisteredEvent {
    // decode into UserId
    broker_account: UserId,
    beneficiary_account: FusoAccountId,
}

#[derive(Encode, Decode, Clone, Debug)]
pub struct MarketOpenedEvent {
    dominator: FusoAccountId,
    base: u32,
    quote: u32,
    base_scale: u8,
    quote_scale: u8,
    min_base: u128,
}

#[derive(Encode, Decode, Clone, Debug)]
pub struct MarketClosedEvent {
    dominator: FusoAccountId,
    base: u32,
    quote: u32,
}

impl WrapperTypeEncode for UserId {}

impl WrapperTypeDecode for UserId {
    type Wrapped = [u8; 32];
}

#[derive(Clone, Encode, Decode, Eq, PartialEq, Debug)]
pub enum FusoCommand {
    AskLimit {
        price: Compact<u128>,
        amount: Compact<u128>,
        maker_fee: Compact<u32>,
        taker_fee: Compact<u32>,
        base: Compact<u32>,
        quote: Compact<u32>,
        broker: Option<FusoAccountId>,
    },
    BidLimit {
        price: Compact<u128>,
        amount: Compact<u128>,
        maker_fee: Compact<u32>,
        taker_fee: Compact<u32>,
        base: Compact<u32>,
        quote: Compact<u32>,
        broker: Option<FusoAccountId>,
    },
    Cancel {
        base: Compact<u32>,
        quote: Compact<u32>,
    },
    TransferOut {
        currency: Compact<u32>,
        amount: Compact<u128>,
    },
    TransferIn {
        currency: Compact<u32>,
        amount: Compact<u128>,
    },
    RejectTransferOut {
        currency: Compact<u32>,
        amount: Compact<u128>,
    },
    RejectTransferIn,
}

impl Into<FusoCommand> for (LimitCmd, Fee, Fee) {
    fn into(self) -> FusoCommand {
        match self.0.ask_or_bid {
            AskOrBid::Ask => FusoCommand::AskLimit {
                price: self.0.price.to_amount().into(),
                amount: self.0.amount.to_amount().into(),
                maker_fee: self.1.to_fee().into(),
                taker_fee: self.2.to_fee().into(),
                base: self.0.symbol.0.into(),
                quote: self.0.symbol.1.into(),
                broker: self.0.broker.map(|x| FusoAccountId::from_raw(x.0)),
            },
            AskOrBid::Bid => FusoCommand::BidLimit {
                price: self.0.price.to_amount().into(),
                amount: self.0.amount.to_amount().into(),
                maker_fee: self.1.to_fee().into(),
                taker_fee: self.2.to_fee().into(),
                base: self.0.symbol.0.into(),
                quote: self.0.symbol.1.into(),
                broker: self.0.broker.map(|x| FusoAccountId::from_raw(x.0)),
            },
        }
    }
}

impl Into<FusoCommand> for CancelCmd {
    fn into(self) -> FusoCommand {
        FusoCommand::Cancel {
            base: self.symbol.0.into(),
            quote: self.symbol.1.into(),
        }
    }
}

impl Into<FusoCommand> for (AssetsCmd, bool) {
    fn into(self) -> FusoCommand {
        match (self.0.in_or_out, self.1) {
            (InOrOut::In, true) => FusoCommand::TransferIn {
                currency: self.0.currency.into(),
                amount: self.0.amount.to_amount().into(),
            },
            (InOrOut::In, false) => FusoCommand::RejectTransferIn,
            (InOrOut::Out, true) => FusoCommand::TransferOut {
                currency: self.0.currency.into(),
                amount: self.0.amount.to_amount().into(),
            },
            (InOrOut::Out, false) => FusoCommand::RejectTransferOut {
                currency: self.0.currency.into(),
                amount: self.0.amount.to_amount().into(),
            },
        }
    }
}

fn d6() -> Amount {
    MILL.into()
}

fn d18() -> Amount {
    QUINTILL.into()
}

pub trait ToBlockChainNumeric {
    fn to_fee(self) -> u32;

    fn to_amount(self) -> u128;

    fn validate(self) -> bool;
}

impl ToBlockChainNumeric for Decimal {
    fn to_fee(self) -> u32 {
        (self * d6()).to_u32().unwrap()
    }

    fn to_amount(self) -> u128 {
        let n = self.trunc().to_u128().unwrap();
        let f = (self.fract() * d18()).to_u128().unwrap();
        n * ONE_ONCHAIN + f
    }

    fn validate(mut self) -> bool {
        self.rescale(18);
        self.scale() == 18
    }
}

pub fn to_decimal_represent(v: u128) -> Option<Decimal> {
    let n = v / ONE_ONCHAIN;
    let f = v % ONE_ONCHAIN;
    let n: Amount = n.try_into().ok()?;
    let mut f: Amount = f.try_into().ok()?;
    f.set_scale(18).ok()?;
    let r = n + f;
    if r.validate() {
        Some(r)
    } else {
        None
    }
}

fn u128le_to_h256(a0: u128, a1: u128) -> [u8; 32] {
    let mut v: [u8; 32] = Default::default();
    v[..16].copy_from_slice(&a0.to_le_bytes());
    v[16..].copy_from_slice(&a1.to_le_bytes());
    v
}

#[cfg(test)]
pub mod test {

    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    pub fn test_numeric() {
        assert!(Decimal::MAX.to_u128().is_some());
        let max = super::to_decimal_represent(u64::MAX.into());
        assert!(max.is_some());
        let v = dec!(340282366920938463463);
        assert_eq!(v.to_amount(), 340282366920938463463000000000000000000);
    }
}
