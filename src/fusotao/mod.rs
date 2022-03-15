// Copyright 2021 UINB Technologies Pte. Ltd.

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

use parity_scale_codec::{Compact, Decode, Encode, WrapperTypeEncode};
use rust_decimal::{prelude::*, Decimal};
use serde::{Deserialize, Serialize};
use smt::{blake2b::Blake2bHasher, default_store::DefaultStore, SparseMerkleTree, H256};
use std::{
    convert::TryInto,
    sync::{
        atomic::AtomicU64,
        mpsc::{Receiver, RecvTimeoutError},
        Arc,
    },
};

pub use prover::Prover;

use crate::{config::C, core::*, event::*};

mod connector;
mod persistence;
mod prover;

pub type GlobalStates = SparseMerkleTree<Blake2bHasher, H256, DefaultStore<H256>>;
pub type Sr25519Key = sp_core::sr25519::Pair;
pub type FusoAccountId = <Sr25519Key as sp_core::Pair>::Public;
pub type FusoAddress = sp_runtime::MultiAddress<FusoAccountId, ()>;
pub type FusoHash = sp_runtime::traits::BlakeTwo256;
pub type BlockNumber = u32;
pub type FusoHeader = sp_runtime::generic::Header<BlockNumber, FusoHash>;
pub type FusoExtrinsic = sp_runtime::OpaqueExtrinsic;
pub type FusoBlock = sp_runtime::generic::Block<FusoHeader, FusoExtrinsic>;
pub type FusoApi = sub_api::Api<Sr25519Key, sub_api::rpc::WsRpcClient>;

const ONE_ONCHAIN: u128 = 1_000_000_000_000_000_000;
const MILL: u32 = 1_000_000;
const BILL: u32 = 1_000_000_000;
const QUINTILL: u64 = 1_000_000_000_000_000_000;
#[allow(dead_code)]
const MAX_EXTRINSIC_BYTES: usize = 1_000_000;
#[allow(dead_code)]
const MAX_EXTRINSIC_WEIGHT: u128 = 1_000_000_000_000_000_000;

#[derive(Clone, Debug)]
pub struct RawParameter(pub Vec<u8>);

impl Encode for RawParameter {
    fn encode(&self) -> Vec<u8> {
        self.0.to_owned()
    }
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
pub struct DominatorClaimedEvent {
    dominator: FusoAccountId,
    pledge: u128,
}

#[derive(Encode, Decode, Clone, Debug)]
pub struct CoinHostedEvent {
    fund_owner: FusoAccountId,
    dominator: FusoAccountId,
    amount: u128,
}

#[derive(Encode, Decode, Clone, Debug)]
pub struct CoinRevokedEvent {
    fund_owner: FusoAccountId,
    dominator: FusoAccountId,
    amount: u128,
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

impl WrapperTypeEncode for UserId {}

/// AccountId of chain = MultiAddress<sp_runtime::AccountId32, ()>::Id = GenericAddress::Id
/// 1. from_ss58check() or from_ss58check_with_version()
/// 2. new or from public
pub fn init(rx: Receiver<Proof>) -> anyhow::Result<Arc<AtomicU64>> {
    persistence::init(rx);
    let connector = connector::FusoConnector::new()?;
    let proved = connector.sync_proving_progress()?;
    connector.start_submitting()?;
    connector.start_scanning()?;
    log::info!("fusotao prover initialized");
    Ok(proved)
}

#[derive(Clone, Encode, Decode, Eq, PartialEq, Debug)]
pub enum FusoCommand {
    // price, amounnt, maker_fee, taker_fee, base, quote
    AskLimit(
        Compact<u128>,
        Compact<u128>,
        Compact<u32>,
        Compact<u32>,
        Compact<u32>,
        Compact<u32>,
    ),
    BidLimit(
        Compact<u128>,
        Compact<u128>,
        Compact<u32>,
        Compact<u32>,
        Compact<u32>,
        Compact<u32>,
    ),
    Cancel(Compact<u32>, Compact<u32>),
    TransferOut(Compact<u32>, Compact<u128>),
    TransferIn(Compact<u32>, Compact<u128>),
}

impl Into<FusoCommand> for (LimitCmd, Fee, Fee) {
    fn into(self) -> FusoCommand {
        match self.0.ask_or_bid {
            AskOrBid::Ask => FusoCommand::AskLimit(
                self.0.price.to_amount().into(),
                self.0.amount.to_amount().into(),
                self.1.to_fee().into(),
                self.2.to_fee().into(),
                self.0.symbol.0.into(),
                self.0.symbol.1.into(),
            ),
            AskOrBid::Bid => FusoCommand::BidLimit(
                self.0.price.to_amount().into(),
                self.0.amount.to_amount().into(),
                self.1.to_fee().into(),
                self.2.to_fee().into(),
                self.0.symbol.0.into(),
                self.0.symbol.1.into(),
            ),
        }
    }
}

impl Into<FusoCommand> for CancelCmd {
    fn into(self) -> FusoCommand {
        FusoCommand::Cancel(self.symbol.0.into(), self.symbol.1.into())
    }
}

impl Into<FusoCommand> for AssetsCmd {
    fn into(self) -> FusoCommand {
        match self.in_or_out {
            InOrOut::In => {
                FusoCommand::TransferIn(self.currency.into(), self.amount.to_amount().into())
            }
            InOrOut::Out => {
                FusoCommand::TransferOut(self.currency.into(), self.amount.to_amount().into())
            }
        }
    }
}

fn d6() -> Amount {
    MILL.into()
}

#[allow(dead_code)]
fn d9() -> Amount {
    BILL.into()
}

fn d18() -> Amount {
    QUINTILL.into()
}

pub trait ToBlockChainNumeric {
    fn to_fee(self) -> u32;

    fn to_price(self) -> (u64, u64);

    fn to_amount(self) -> u128;
}

impl ToBlockChainNumeric for Decimal {
    fn to_fee(self) -> u32 {
        (self * d6()).to_u32().unwrap()
    }

    fn to_price(self) -> (u64, u64) {
        let f = self.fract() * d18();
        (self.trunc().to_u64().unwrap(), f.to_u64().unwrap())
    }

    fn to_amount(self) -> u128 {
        let n = self.trunc().to_u128().unwrap();
        let f = self.fract() * d18();
        n * ONE_ONCHAIN + f.to_u128().unwrap()
    }
}

// FIXME
pub fn to_decimal_represent(v: u128) -> Decimal {
    let n = v / ONE_ONCHAIN;
    let f = v % ONE_ONCHAIN;
    let n: Amount = n.try_into().unwrap();
    let mut f: Amount = f.try_into().unwrap();
    f.set_scale(18).unwrap();
    n + f
}

fn u128le_to_h256(a0: u128, a1: u128) -> [u8; 32] {
    let mut v: [u8; 32] = Default::default();
    v[..16].copy_from_slice(&a0.to_le_bytes());
    v[16..].copy_from_slice(&a1.to_le_bytes());
    v
}

#[cfg(test)]
pub mod test {
    use rust_decimal_macros::dec;

    use super::*;

    #[test]
    pub fn test_numeric() {
        assert!(Decimal::MAX.to_u128().is_some());
        // assert!(Decimal::MAX > dec!(340282366920938463463.374607431768211455));
        // let p = dec!(38463463.374607431768211455);
        // let (n, f) = p.to_price();
        // assert_eq!(n, 38463463);
        // assert_eq!(f, 374607431768211455);
    }
}
