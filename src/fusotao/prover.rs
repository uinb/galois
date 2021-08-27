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

use super::*;
use crate::{assets::Balance, core::*, event::*, output::Output};
use anyhow::anyhow;
use rust_decimal::{prelude::*, Decimal};
use sha2::{Digest, Sha256};
use std::sync::mpsc::Sender;

const ONE_ONCHAIN: u128 = 1_000_000_000_000_000_000;
const ACCOUNT_KEY: u8 = 0x00;
const ORDERBOOK_KEY: u8 = 0x01;

pub struct Prover(Sender<Proof>);

impl Prover {
    pub fn new(tx: Sender<Proof>) -> anyhow::Result<Self> {
        Ok(Self(tx))
    }

    pub fn prove_trade_cmd(
        &self,
        data: &mut Data,
        nonce: u32,
        signature: Vec<u8>,
        encoded_cmd: Vec<u8>,
        ask_size_before: Amount,
        bid_size_before: Amount,
        taker_base_before: &Balance,
        taker_quote_before: &Balance,
        outputs: &[Output],
    ) {
        let mut leaves = vec![];
        let mut keys = vec![];
        let symbol = outputs.last().unwrap().symbol.clone();
        let event_id = outputs.last().unwrap().event_id;
        let user_id = outputs.last().unwrap().user_id;
        let orderbook = data.orderbooks.get(&symbol).unwrap();
        let (old_ask_size, old_bid_size) = (
            to_merkle_represent(ask_size_before).unwrap(),
            to_merkle_represent(bid_size_before).unwrap(),
        );
        let (new_ask_size, new_bid_size) = (
            to_merkle_represent(orderbook.ask_size).unwrap(),
            to_merkle_represent(orderbook.bid_size).unwrap(),
        );
        let (k, l) = new_orderbook_merkle_leaf(
            symbol,
            old_ask_size,
            old_bid_size,
            new_ask_size,
            new_bid_size,
        );
        keys.push(k);
        leaves.push(l);
        if outputs.len() > 1 {
            for output in outputs.iter() {
                // for every output, x + delta - charge = y
                // let (old_ba, old_bf) = (
                //     // TODO
                //     to_merkle_represent(r.base_available - r.base_delta + r.base_charge).unwrap(),
                //     to_merkle_represent(r.base_frozen).unwrap(),
                // );
                // let (new_ba, new_bf) = (
                //     to_merkle_represent(r.base_available).unwrap(),
                //     to_merkle_represent(r.base_frozen).unwrap(),
                // );
                // let (k, leaf0) = new_account_merkle_leaf(&r.user_id, symbol.0, ba, bf);
                // let (qa, qf) = (
                //     to_merkle_represent(r.quote_available).unwrap(),
                //     to_merkle_represent(r.quote_frozen).unwrap(),
                // );
                // let (_, leaf1) = new_account_merkle_leaf(&r.user_id, symbol.1, qa, qf);
            }
        }
        // outputs
        //     .iter()
        //     .flat_map(|ref r| {
        //         let (ba, bf) = (
        //             to_merkle_represent(r.base_available).unwrap(),
        //             to_merkle_represent(r.base_frozen).unwrap(),
        //         );
        //         let leaf0 = new_account_merkle_leaf(&r.user_id, symbol.0, ba, bf);
        //         let (qa, qf) = (
        //             to_merkle_represent(r.quote_available).unwrap(),
        //             to_merkle_represent(r.quote_frozen).unwrap(),
        //         );
        //         let leaf1 = new_account_merkle_leaf(&r.user_id, symbol.1, qa, qf);
        //         vec![leaf0, leaf1].into_iter()
        //     })
        //     .for_each(|n| updates.push(n));
        // let proof = Proof {
        //     event_id,
        //     user_id,
        //     nonce,
        //     signature,
        //     cmd,
        //     leaves: updates.clone(),
        //     proofs: gen_proofs(&mut data.merkle_tree, updates),
        // };
        // self.0.send(proof).unwrap();
    }
    // pub struct Proof {
    //     pub event_id: u64,
    //     pub user_id: UserId,
    //     pub nonce: u32,
    //     pub signature: [u8; 64],
    //     pub cmd: Vec<u8>,
    //     pub keys: Vec<H256>,
    //     pub leaves: Vec<MerkleLeaf>,
    //     pub proof_of_exists: Vec<u8>,
    //     pub proof_of_cmd: Vec<u8>,
    //     pub root: H256,
    // }
    pub fn prove_assets_cmd(
        &self,
        data: &mut Data,
        event_id: u64,
        cmd: &AssetsCmd,
        account_before: &Balance,
        account_after: &Balance,
    ) {
        // let balance = assets::get_to_owned(&data.accounts, user_id, currency);
        // let (available, frozen) = (
        //     to_merkle_represent(balance.available).unwrap(),
        //     to_merkle_represent(balance.frozen).unwrap(),
        // );
        // let leaf = new_account_merkle_leaf(user_id, currency, available, frozen);
        // let proof = Proof {
        //     event_id,
        //     // TODO
        //     user_id: UserId::zero(),
        //     nonce: 0,
        //     signature: vec![0; 32],
        //     cmd: vec![],
        //     leaves: vec![leaf.clone()],
        //     proofs: gen_proofs(&mut data.merkle_tree, vec![leaf]),
        // };
        // self.0.send(proof).unwrap();
    }
}

fn d18() -> Amount {
    ONE_ONCHAIN.into()
}

fn to_merkle_represent(v: Decimal) -> Option<u128> {
    Some((v.fract() * d18()).to_u128()? + (v.floor().to_u128()? * ONE_ONCHAIN))
}

fn beu128_to_h256(a0: u128, a1: u128) -> H256 {
    let mut v: [u8; 32] = Default::default();
    v[..16].copy_from_slice(&a0.to_be_bytes());
    v[16..].copy_from_slice(&a1.to_be_bytes());
    H256::from(v)
}

fn new_account_merkle_leaf(
    user_id: &UserId,
    currency: Currency,
    old_available: u128,
    old_frozen: u128,
    new_available: u128,
    new_frozen: u128,
) -> (H256, MerkleLeaf) {
    let mut hasher = Sha256::new();
    hasher.update(&[ACCOUNT_KEY][..]);
    hasher.update(<B256 as AsRef<[u8]>>::as_ref(user_id));
    hasher.update(&currency.to_be_bytes()[..]);
    (
        hasher.finalize().into(),
        MerkleLeaf {
            old_v: beu128_to_h256(old_available, old_frozen),
            new_v: beu128_to_h256(new_available, new_frozen),
        },
    )
}

fn new_orderbook_merkle_leaf(
    symbol: Symbol,
    old_ask_size: u128,
    old_bid_size: u128,
    new_ask_size: u128,
    new_bid_size: u128,
) -> (H256, MerkleLeaf) {
    let mut hasher = Sha256::new();
    let mut symbol_bits: [u8; 8] = Default::default();
    symbol_bits[..4].copy_from_slice(&symbol.0.to_be_bytes()[..]);
    symbol_bits[4..].copy_from_slice(&symbol.1.to_be_bytes()[..]);
    hasher.update(&[ORDERBOOK_KEY][..]);
    hasher.update(&symbol_bits[..]);
    (
        hasher.finalize().into(),
        MerkleLeaf {
            old_v: beu128_to_h256(old_ask_size, old_bid_size),
            new_v: beu128_to_h256(new_ask_size, new_bid_size),
        },
    )
}

// FIXME unwrap
fn gen_proofs(merkle_tree: &mut GlobalStates, leaves: Vec<MerkleLeaf>) -> Vec<u8> {
    vec![]
    // leaves.iter().for_each(|leaf| {
    //     merkle_tree.update(leaf.key, leaf.value).unwrap();
    // });
    // let proof = merkle_tree
    //     .merkle_proof(leaves.iter().map(|leaf| leaf.key).collect::<Vec<_>>())
    //     .unwrap();
    // proof
    //     .compile(
    //         leaves
    //             .into_iter()
    //             .map(|leaf| (leaf.key, leaf.value))
    //             .collect::<Vec<_>>(),
    //     )
    //     .unwrap()
    //     .into()
}
