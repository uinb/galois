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
use memmap::MmapMut;
use parity_scale_codec::{Encode, WrapperTypeEncode};
use rust_decimal::{prelude::*, Decimal};
use serde::{Deserialize, Serialize};
use smt::{default_store::DefaultStore, sha256::Sha256Hasher, SparseMerkleTree, H256};
use sp_core::{
    crypto::{Pair, Ss58Codec},
    sr25519::Pair as Sr25519,
};
use std::{convert::TryInto, fs::OpenOptions, path::PathBuf, sync::mpsc::Receiver};
use sub_api::{compose_extrinsic, rpc::WsRpcClient, Api, UncheckedExtrinsicV4, XtStatus};

pub type GlobalStates = SparseMerkleTree<Sha256Hasher, H256, DefaultStore<H256>>;

const ONE_ONCHAIN: u128 = 1_000_000_000_000_000_000;

#[derive(Clone, Debug, Serialize, Deserialize, Encode)]
pub struct MerkleLeaf {
    pub key: [u8; 32],
    pub old_v: [u8; 32],
    pub new_v: [u8; 32],
}

#[derive(Debug, Clone, Encode)]
pub struct Proof {
    pub event_id: u64,
    pub user_id: UserId,
    pub nonce: u32,
    pub signature: Vec<u8>,
    pub cmd: Vec<u8>,
    pub leaves: Vec<MerkleLeaf>,
    pub proof_of_exists: Vec<u8>,
    pub proof_of_cmd: Vec<u8>,
    pub root: [u8; 32],
}

impl WrapperTypeEncode for UserId {}

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
    let path: PathBuf = [&C.sequence.coredump_dir, "onchain"].iter().collect();
    let finalized_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)?;
    finalized_file.set_len(8)?;
    let mut mmap = unsafe { MmapMut::map_mut(&finalized_file)? };
    let mut cur = u64::from_be_bytes(mmap.as_ref().try_into()?);
    std::thread::spawn(move || loop {
        let proof = rx.recv().unwrap();
        if cur >= proof.event_id {
            continue;
        }
        cur = proof.event_id;
        let xt: UncheckedExtrinsicV4<_> =
            compose_extrinsic!(api.clone(), "Receipts", "verify", proof);
        // FIXME handle network error?
        api.send_extrinsic(xt.hex_encode(), XtStatus::InBlock)
            .unwrap();
        // FIXME only update when finalized
        mmap.copy_from_slice(&cur.to_be_bytes()[..]);
    });
    // TODO read last finalized block
    std::thread::spawn(move || {});
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

fn u128be_to_h256(a0: u128, a1: u128) -> [u8; 32] {
    let mut v: [u8; 32] = Default::default();
    v[..16].copy_from_slice(&a0.to_be_bytes());
    v[16..].copy_from_slice(&a1.to_be_bytes());
    v
}
