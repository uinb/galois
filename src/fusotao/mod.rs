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

use crate::{config::C, core::*};
use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use smt::{default_store::DefaultStore, sha256::Sha256Hasher, SparseMerkleTree, H256};
use sp_core::{
    crypto::{Pair, Ss58Codec},
    sr25519::Pair as Sr25519,
};
use std::sync::mpsc::Receiver;
use sub_api::{compose_extrinsic, rpc::WsRpcClient, Api, UncheckedExtrinsicV4, XtStatus};

// pub type MerkleIdentity = H256;
pub type GlobalStates = SparseMerkleTree<Sha256Hasher, H256, DefaultStore<H256>>;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MerkleLeaf {
    pub key: H256,
    pub old_v: H256,
    pub new_v: H256,
}

// impl MerkleLeaf {
//     pub fn as_bytes(&self) -> Vec<u8> {
//         let mut v = vec![];
//         v.extend_from_slice(self.old_v.as_slice());
//         v.extend_from_slice(self.new_v.as_slice());
//         v
//     }
// }

#[derive(Debug, Clone)]
pub struct Proof {
    pub event_id: u64,
    pub user_id: UserId,
    pub nonce: u32,
    pub signature: Vec<u8>,
    pub cmd: Vec<u8>,
    pub keys: Vec<H256>,
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
