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

use crate::{assets, config::C, core::*, output::Output};
use anyhow::anyhow;
use rust_decimal::{prelude::*, Decimal};
use sha2::{Digest, Sha256};
use sp_core::Pair;
use std::{sync::mpsc, thread};
use substrate_api_client::{
    compose_extrinsic, rpc::WsRpcClient, Api, UncheckedExtrinsicV4, XtStatus,
};

pub const ONE_ONCHAIN: u128 = 1_000_000_000_000_000_000;
pub const SCALE_ONCHAIN: u32 = 18;
const ACCOUNT_KEY: u8 = 0x00;
const ORDERBOOK_KEY: u8 = 0x01;

#[derive(Debug, Clone)]
pub struct Proof {
    pub event_id: u64,
    // TODO signature
    // pub symbol: Symbol,
    // pub cmd: u8,
    // pub nonce: u32,
    pub encoded_updates: Vec<MerkleLeaf>,
    pub proofs: Vec<u8>,
}

// TODO
#[derive(Debug, Clone)]
pub struct Signature {
    pub user_id: UserId,
    pub nonce: u32,
    pub cmd: u8,
    pub sig: Bits256,
    pub params: Vec<u8>,
}

pub struct Prover(mpsc::Sender<Proof>);

impl Prover {
    pub fn init() -> anyhow::Result<Self> {
        let signer = sp_core::sr25519::Pair::from_string(
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
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || loop {
            let _proofs = rx.recv().unwrap();
            // TODO Receipts#verify
            let xt: UncheckedExtrinsicV4<_> =
                compose_extrinsic!(api.clone(), "Balances", "transfer", Compact(42_u128));
            // TODO retry
            api.send_extrinsic(xt.hex_encode(), XtStatus::InBlock);
        });
        Ok(Self(tx))
    }

    pub fn prove_trading_cmd(&self, data: &mut Data, outputs: &[Output]) -> anyhow::Result<()> {
        let mut updates = vec![];
        let symbol = outputs
            .last()
            .ok_or(anyhow!("won't happen"))?
            .symbol
            .clone();
        let event_id = outputs.last().ok_or(anyhow!("won't happen"))?.event_id;
        let orderbook = data
            .orderbooks
            .get(&symbol)
            .ok_or(anyhow!("won't happen"))?;
        let (ask, bid) = (
            to_merkle_represent(orderbook.ask_size).unwrap(),
            to_merkle_represent(orderbook.bid_size).unwrap(),
        );
        updates.push(new_orderbook_merkle_leaf(symbol, ask, bid));
        outputs
            .iter()
            .flat_map(|ref r| {
                let (ba, bf) = (
                    to_merkle_represent(r.base_available).unwrap(),
                    to_merkle_represent(r.base_frozen).unwrap(),
                );
                let leaf0 = new_account_merkle_leaf(&r.user_id, symbol.0, ba, bf);
                let (qa, qf) = (
                    to_merkle_represent(r.quote_available).unwrap(),
                    to_merkle_represent(r.quote_frozen).unwrap(),
                );
                let leaf1 = new_account_merkle_leaf(&r.user_id, symbol.1, qa, qf);
                vec![leaf0, leaf1].into_iter()
            })
            .for_each(|n| updates.push(n));
        let proof = Proof {
            event_id,
            encoded_updates: updates.clone(),
            proofs: gen_proofs(&mut data.merkle_tree, updates),
        };
        self.0
            .send(proof)
            .map_err(|_| anyhow::anyhow!("memory channel broken on prover"))
    }

    pub fn prove_assets_cmd(
        &self,
        data: &mut Data,
        event_id: u64,
        user_id: &UserId,
        currency: Currency,
    ) -> anyhow::Result<()> {
        let balance = assets::get_to_owned(&data.accounts, user_id, currency);
        let (available, frozen) = (
            to_merkle_represent(balance.available).unwrap(),
            to_merkle_represent(balance.frozen).unwrap(),
        );
        let leaf = new_account_merkle_leaf(user_id, currency, available, frozen);
        let proof = Proof {
            event_id,
            encoded_updates: vec![leaf],
            proofs: gen_proofs(&mut data.merkle_tree, vec![leaf]),
        };
        self.0
            .send(proof)
            .map_err(|_| anyhow::anyhow!("memory channel broken on prover"))
    }
}

pub fn d18() -> Amount {
    ONE_ONCHAIN.into()
}

pub fn to_merkle_represent(v: Decimal) -> Option<u128> {
    Some((v.fract() * d18()).to_u128()? + (v.floor().to_u128()? * ONE_ONCHAIN))
}

fn new_account_merkle_leaf(
    user_id: &UserId,
    currency: Currency,
    avaiable: u128,
    frozen: u128,
) -> MerkleLeaf {
    let mut hasher = Sha256::new();
    let mut value: [u8; 32] = Default::default();
    value.copy_from_slice(&[&avaiable.to_be_bytes()[..], &frozen.to_be_bytes()[..]].concat());
    hasher.update(&[ACCOUNT_KEY][..]);
    hasher.update(user_id.as_bytes());
    hasher.update(&currency.to_be_bytes()[..]);
    (hasher.finalize().into(), MerkleIdentity::from(value))
}

fn new_orderbook_merkle_leaf(symbol: Symbol, ask_size: u128, bid_size: u128) -> MerkleLeaf {
    let mut hasher = Sha256::new();
    let mut value: [u8; 32] = Default::default();
    value.copy_from_slice(&[&ask_size.to_be_bytes()[..], &bid_size.to_be_bytes()[..]].concat());
    // FIXME shall we use C-repr feature to serialize `Symbol` directly?
    let mut symbol_bits: [u8; 8] = Default::default();
    symbol_bits
        .copy_from_slice(&[&symbol.0.to_be_bytes()[..], &symbol.1.to_be_bytes()[..]].concat());
    hasher.update(&[ORDERBOOK_KEY][..]);
    hasher.update(&symbol_bits[..]);
    (hasher.finalize().into(), MerkleIdentity::from(value))
}

// FIXME unwrap
fn gen_proofs(merkle_tree: &mut GlobalStates, leaves: Vec<MerkleLeaf>) -> Vec<u8> {
    leaves.iter().for_each(|(k, v)| {
        merkle_tree.update(*k, *v).unwrap();
    });
    let proof = merkle_tree
        .merkle_proof(leaves.iter().map(|(k, _)| *k).collect::<Vec<_>>())
        .unwrap();
    proof.compile(leaves).unwrap().into()
}
