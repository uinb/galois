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

mod prover;

pub use prover::Prover;

use crate::{config::C, core::*, event::*};
use anyhow::anyhow;
use rust_decimal::{prelude::*, Decimal};
use serde::{Deserialize, Serialize};
use smt::{default_store::DefaultStore, sha256::Sha256Hasher, SparseMerkleTree, H256};
use sp_core::{
    crypto::{Pair, Ss58Codec},
    sr25519::Pair as Sr25519,
};
use std::sync::mpsc::Receiver;
use sub_api::{compose_extrinsic, rpc::WsRpcClient, Api, UncheckedExtrinsicV4, XtStatus};

pub type GlobalStates = SparseMerkleTree<Sha256Hasher, H256, DefaultStore<H256>>;

const ONE_ONCHAIN: u128 = 1_000_000_000_000_000_000;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MerkleLeaf {
    pub key: H256,
    pub old_v: H256,
    pub new_v: H256,
}

#[derive(Debug, Clone)]
pub struct Proof {
    pub event_id: u64,
    pub user_id: UserId,
    pub nonce: u32,
    pub signature: Vec<u8>,
    pub cmd: Vec<u8>,
    pub leaves: Vec<MerkleLeaf>,
    pub proof_of_exists: Vec<u8>,
    pub proof_of_cmd: Vec<u8>,
    pub root: H256,
}

// TODO get last seq id after launched
/// AccountId of chain = MultiAddress<sp_runtime::AccountId32, ()>::Id = GenericAddress::Id
/// 1. from_ss58check() or from_ss58check_with_version()
/// 2. new or from public
pub fn init(rx: Receiver<Proof>) -> anyhow::Result<()> {
    let signer = Sr25519::from_string(
        &C.fusotao
            .as_ref()
            .ok_or(anyhow!("Invalid fusotao config"))?
            .key_seed,
        None,
    )
    .map_err(|_| anyhow!("Invalid fusotao config"))?;
    let client = WsRpcClient::new(
        &C.fusotao
            .as_ref()
            .ok_or(anyhow!("Invalid fusotao config"))?
            .node_url,
    );
    let api = Api::new(client)
        .map(|api| api.set_signer(signer))
        .map_err(|_| anyhow!("Fusotao node not available"))?;
    std::thread::spawn(move || {
        // let mut buf = vec![];
        let to = sp_runtime::AccountId32::from_ss58check(
            "5CJzBh1SeBJ5qKzEpz1yzk8dF45erM5VWzwz4Ef2Zs1y2nKQ",
        )
        .unwrap();
        loop {
            let _proof = rx.recv().unwrap();
            //
            let xt: UncheckedExtrinsicV4<_> = compose_extrinsic!(
                api.clone(),
                "Balances",
                "transfer",
                GenericAddress::Id(to.clone()),
                Compact(42000000000000000000_u128)
            );
            api.send_extrinsic(xt.hex_encode(), XtStatus::InBlock);
        }
    });
    Ok(())
}

impl Into<Vec<u8>> for LimitCmd {
    fn into(self) -> Vec<u8> {
        let mut v = vec![];
        v.extend_from_slice(&self.symbol.0.to_be_bytes());
        v.extend_from_slice(&self.symbol.1.to_be_bytes());
        v.extend_from_slice(self.user_id.as_ref());
        v.extend_from_slice(&self.order_id.to_be_bytes());
        v.extend_from_slice(&to_merkle_represent(self.price).to_be_bytes());
        v.extend_from_slice(&to_merkle_represent(self.amount).to_be_bytes());
        v.push(self.ask_or_bid.into());
        v
    }
}

impl Into<Vec<u8>> for CancelCmd {
    fn into(self) -> Vec<u8> {
        let mut v = vec![];
        v.extend_from_slice(&self.symbol.0.to_be_bytes());
        v.extend_from_slice(&self.symbol.1.to_be_bytes());
        v.extend_from_slice(self.user_id.as_ref());
        v.extend_from_slice(&self.order_id.to_be_bytes());
        v
    }
}

impl Into<Vec<u8>> for AssetsCmd {
    fn into(self) -> Vec<u8> {
        let mut v = vec![];
        v.extend_from_slice(&self.currency.to_be_bytes());
        v.extend_from_slice(self.user_id.as_ref());
        v.extend_from_slice(&to_merkle_represent(self.amount).to_be_bytes());
        v
    }
}

fn d18() -> Amount {
    ONE_ONCHAIN.into()
}

fn to_merkle_represent(v: Decimal) -> u128 {
    let mut fraction = v.fract();
    fraction.set_scale(18).unwrap();
    (fraction * d18()).to_u128().unwrap() + (v.floor().to_u128().unwrap() * ONE_ONCHAIN)
}

fn u128be_to_h256(a0: u128, a1: u128) -> H256 {
    let mut v: [u8; 32] = Default::default();
    v[..16].copy_from_slice(&a0.to_be_bytes());
    v[16..].copy_from_slice(&a1.to_be_bytes());
    H256::from(v)
}
