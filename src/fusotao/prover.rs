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
use crate::{assets::Balance, core::*, event::*, matcher::*, orderbook::AskOrBid, output::Output};
use sha2::{Digest, Sha256};
use std::sync::mpsc::Sender;

const ACCOUNT_KEY: u8 = 0x00;
const ORDERBOOK_KEY: u8 = 0x01;

pub struct Prover(Sender<Proof>);

impl Prover {
    pub fn new(tx: Sender<Proof>) -> Self {
        Self(tx)
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
        let taker = outputs.last().unwrap();
        let symbol = taker.symbol.clone();
        let event_id = taker.event_id;
        let user_id = taker.user_id;
        let orderbook = data.orderbooks.get(&symbol).unwrap();
        let (old_ask_size, old_bid_size, new_ask_size, new_bid_size) = (
            to_merkle_represent(ask_size_before),
            to_merkle_represent(bid_size_before),
            to_merkle_represent(orderbook.ask_size),
            to_merkle_represent(orderbook.bid_size),
        );
        leaves.push(new_orderbook_merkle_leaf(
            symbol,
            old_ask_size,
            old_bid_size,
            new_ask_size,
            new_bid_size,
        ));
        outputs
            .iter()
            .take_while(|o| o.role == Role::Maker)
            .for_each(|ref r| {
                let (ba, bf, qa, qf) = match r.ask_or_bid {
                    // -base_frozen, +quote_available
                    // base_frozen0 + r.base_delta = base_frozen
                    // quote_available0 + r.quote_delta + r.quote_charge = quote_available
                    AskOrBid::Ask => (
                        r.base_available,
                        r.base_frozen - r.base_delta,
                        r.quote_available - r.quote_delta - r.quote_charge,
                        r.quote_frozen,
                    ),
                    // +base_available, -quote_frozen
                    // quote_frozen0 + r.quote_delta = quote_frozen
                    // base_available0 + r.base_delta + r.base_charge = base_available
                    AskOrBid::Bid => (
                        r.base_available - r.base_charge - r.base_delta,
                        r.base_frozen,
                        r.quote_available,
                        r.quote_frozen - r.quote_delta,
                    ),
                };
                let (new_ba, new_bf, old_ba, old_bf) = (
                    to_merkle_represent(r.base_available),
                    to_merkle_represent(r.base_frozen),
                    to_merkle_represent(ba),
                    to_merkle_represent(bf),
                );
                leaves.push(new_account_merkle_leaf(
                    &r.user_id, symbol.0, old_ba, old_bf, new_ba, new_bf,
                ));
                let (new_qa, new_qf, old_qa, old_qf) = (
                    to_merkle_represent(r.quote_available),
                    to_merkle_represent(r.quote_frozen),
                    to_merkle_represent(qa),
                    to_merkle_represent(qf),
                );
                leaves.push(new_account_merkle_leaf(
                    &r.user_id, symbol.1, old_qa, old_qf, new_qa, new_qf,
                ));
            });
        let (new_taker_ba, new_taker_bf, old_taker_ba, old_taker_bf) = (
            to_merkle_represent(taker.base_available),
            to_merkle_represent(taker.base_frozen),
            to_merkle_represent(taker_base_before.available),
            to_merkle_represent(taker_base_before.frozen),
        );
        leaves.push(new_account_merkle_leaf(
            &user_id,
            symbol.0,
            old_taker_ba,
            old_taker_bf,
            new_taker_ba,
            new_taker_bf,
        ));
        let (new_taker_qa, new_taker_qf, old_taker_qa, old_taker_qf) = (
            to_merkle_represent(taker.quote_available),
            to_merkle_represent(taker.quote_frozen),
            to_merkle_represent(taker_quote_before.available),
            to_merkle_represent(taker_quote_before.frozen),
        );
        leaves.push(new_account_merkle_leaf(
            &user_id,
            symbol.1,
            old_taker_qa,
            old_taker_qf,
            new_taker_qa,
            new_taker_qf,
        ));
        let (pr0, pr1) = gen_proofs(&mut data.merkle_tree, &leaves);
        self.0
            .send(Proof {
                event_id: event_id,
                user_id: user_id,
                nonce: nonce,
                signature: signature,
                cmd: encoded_cmd,
                leaves: leaves,
                proof_of_exists: pr0,
                proof_of_cmd: pr1,
                // TODO redundant clone because &H256 doesn't implement Into<[u8; 32]>
                root: data.merkle_tree.root().clone().into(),
            })
            .unwrap();
    }

    pub fn prove_assets_cmd(
        &self,
        merkle_tree: &mut GlobalStates,
        event_id: u64,
        cmd: AssetsCmd,
        account_before: &Balance,
        account_after: &Balance,
    ) {
        let (new_available, new_frozen, old_available, old_frozen) = (
            to_merkle_represent(account_after.available),
            to_merkle_represent(account_after.frozen),
            to_merkle_represent(account_before.available),
            to_merkle_represent(account_before.frozen),
        );
        let leaves = vec![new_account_merkle_leaf(
            &cmd.user_id,
            cmd.currency,
            old_available,
            old_frozen,
            new_available,
            new_frozen,
        )];
        let (pr0, pr1) = gen_proofs(merkle_tree, &leaves);
        self.0
            .send(Proof {
                event_id: event_id,
                user_id: cmd.user_id,
                nonce: cmd.nonce_or_block_number,
                signature: cmd.signature_or_hash.clone(),
                cmd: cmd.into(),
                leaves: leaves,
                proof_of_exists: pr0,
                proof_of_cmd: pr1,
                root: merkle_tree.root().clone().into(),
            })
            .unwrap();
    }
}

fn gen_proofs(merkle_tree: &mut GlobalStates, leaves: &Vec<MerkleLeaf>) -> (Vec<u8>, Vec<u8>) {
    let keys = leaves
        .iter()
        .map(|leaf| leaf.key.into())
        .collect::<Vec<_>>();
    let poe = merkle_tree.merkle_proof(keys.clone()).unwrap();
    let pr0 = poe
        .compile(
            leaves
                .iter()
                .map(|leaf| (leaf.key.into(), leaf.old_v.into()))
                .collect::<Vec<_>>(),
        )
        .unwrap();
    leaves.iter().for_each(|leaf| {
        merkle_tree
            .update(leaf.key.into(), leaf.new_v.into())
            .unwrap();
    });
    let poc = merkle_tree.merkle_proof(keys.clone()).unwrap();
    let pr1 = poc
        .compile(
            leaves
                .iter()
                .map(|leaf| (leaf.key.into(), leaf.new_v.into()))
                .collect::<Vec<_>>(),
        )
        .unwrap();
    (pr0.into(), pr1.into())
}

fn new_account_merkle_leaf(
    user_id: &UserId,
    currency: Currency,
    old_available: u128,
    old_frozen: u128,
    new_available: u128,
    new_frozen: u128,
) -> MerkleLeaf {
    let mut hasher = Sha256::new();
    hasher.update(&[ACCOUNT_KEY][..]);
    hasher.update(<B256 as AsRef<[u8]>>::as_ref(user_id));
    hasher.update(&currency.encode()[..]);
    MerkleLeaf {
        key: hasher.finalize().into(),
        old_v: u128be_to_h256(old_available, old_frozen),
        new_v: u128be_to_h256(new_available, new_frozen),
    }
}

fn new_orderbook_merkle_leaf(
    symbol: Symbol,
    old_ask_size: u128,
    old_bid_size: u128,
    new_ask_size: u128,
    new_bid_size: u128,
) -> MerkleLeaf {
    let mut hasher = Sha256::new();
    let mut symbol_bits = vec![0u8; 8];
    symbol_bits[..4].copy_from_slice(&symbol.0.encode()[..]);
    symbol_bits[4..].copy_from_slice(&symbol.1.encode()[..]);
    hasher.update(&[ORDERBOOK_KEY][..]);
    hasher.update(&symbol_bits[..]);
    MerkleLeaf {
        key: hasher.finalize().into(),
        old_v: u128be_to_h256(old_ask_size, old_bid_size),
        new_v: u128be_to_h256(new_ask_size, new_bid_size),
    }
}
