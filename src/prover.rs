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

use crate::{
    core::*,
    orderbook::{AskOrBid, OrderBook},
    output::Output,
};
use rust_decimal::{prelude::*, Decimal};
use sha2::{Digest, Sha256};
use std::convert::Into;

pub const ONE_ONCHAIN: u128 = 1_000_000_000_000_000_000;
pub const SCALE_ONCHAIN: u32 = 18;
const ACCOUNT_KEY: u8 = 0x00;
const ORDERBOOK_KEY: u8 = 0x01;

#[derive(Debug, Clone)]
pub struct Proof {
    pub event_id: u64,
    pub symbol: Symbol,
    pub cmd: u8,
    pub nonce: u32,
    pub encoded_updates: Vec<MerkleLeaf>,
    pub proofs: Vec<u8>,
}

pub struct Prover;

pub fn d18() -> Amount {
    ONE_ONCHAIN.into()
}

pub fn to_merkle_represent(v: Decimal) -> Option<u128> {
    Some((v.fract() * d18()).to_u128()? + (v.floor().to_u128()? * ONE_ONCHAIN))
}

fn new_account_merkle_leaf(
    user_id: UserId,
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

pub fn prove_limit_cmd(
    event_id: u64,
    nonce: u32,
    symbol: Symbol,
    ask_or_bid: AskOrBid,
    merkle_tree: &mut GlobalStates,
    orderbook: &OrderBook,
    outputs: &[Output],
) {
    let mut updates = vec![];
    let (ask, bid) = (
        to_merkle_represent(orderbook.ask_size).unwrap(),
        to_merkle_represent(orderbook.bid_size).unwrap(),
    );
    updates.push(new_orderbook_merkle_leaf(symbol, ask, bid));
    outputs
        .iter()
        .flat_map(|r| {
            let (ba, bf) = (
                to_merkle_represent(r.base_available).unwrap(),
                to_merkle_represent(r.base_frozen).unwrap(),
            );
            let leaf0 = new_account_merkle_leaf(r.user_id, symbol.0, ba, bf);
            let (qa, qf) = (
                to_merkle_represent(r.quote_available).unwrap(),
                to_merkle_represent(r.quote_frozen).unwrap(),
            );
            let leaf1 = new_account_merkle_leaf(r.user_id, symbol.1, qa, qf);
            vec![leaf0, leaf1].into_iter()
        })
        .for_each(|n| updates.push(n));
    let proof = Proof {
        event_id,
        symbol,
        cmd: ask_or_bid.into(),
        nonce: nonce,
        encoded_updates: updates.clone(),
        proofs: gen_proofs(merkle_tree, updates),
    };
}
