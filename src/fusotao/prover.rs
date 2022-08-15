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
use crate::{assets::Balance, matcher::*, orderbook::AskOrBid, output::Output};
use blake2::{Blake2b, Digest};
use generic_array::typenum::U32;
use std::{collections::HashMap, sync::mpsc::Sender};

pub type BlakeTwo256 = Blake2b<U32>;

const ACCOUNT_KEY: u8 = 0x00;
const ORDERBOOK_KEY: u8 = 0x01;
const BESTPRICE_KEY: u8 = 0x02;
const ORDERPAGE_KEY: u8 = 0x03;

pub struct Prover {
    pub sender: Sender<Proof>,
    pub proved_event_id: Arc<AtomicU64>,
}

impl Prover {
    pub fn new(tx: Sender<Proof>, proved_event_id: Arc<AtomicU64>) -> Self {
        Self {
            sender: tx,
            proved_event_id,
        }
    }

    pub fn prove_trade_cmd(
        &self,
        data: &mut Data,
        nonce: u32,
        signature: Vec<u8>,
        encoded_cmd: FusoCommand,
        ask_size_before: Amount,
        bid_size_before: Amount,
        best_ask_before: (Price, Amount),
        best_bid_before: (Price, Amount),
        taker_base_before: &Balance,
        taker_quote_before: &Balance,
        outputs: &[Output],
        matches: &Match,
    ) {
        let mut leaves = vec![];
        let taker = outputs.last().unwrap();
        let symbol = taker.symbol.clone();
        let event_id = taker.event_id;
        let user_id = taker.user_id;
        let orderbook = data.orderbooks.get(&symbol).unwrap();
        let size = orderbook.size();
        log::debug!(
            "generating merkle leaf of {:?}: orderbook = ({:?}, {:?}) -> ({:?}, {:?})",
            taker.event_id,
            ask_size_before,
            bid_size_before,
            size.0,
            size.1,
        );
        let (old_ask_size, old_bid_size, new_ask_size, new_bid_size) = (
            ask_size_before.to_amount(),
            bid_size_before.to_amount(),
            size.0.to_amount(),
            size.1.to_amount(),
        );
        leaves.push(new_orderbook_merkle_leaf(
            symbol,
            old_ask_size,
            old_bid_size,
            new_ask_size,
            new_bid_size,
        ));
        let mut maker_accounts = HashMap::<UserId, Output>::new();
        outputs
            .iter()
            .take_while(|o| o.role == Role::Maker)
            .for_each(|r| {
                maker_accounts
                    .entry(r.user_id.clone())
                    .and_modify(|out| {
                        out.quote_charge += r.quote_charge;
                        out.quote_delta += r.quote_delta;
                        out.quote_available = r.quote_available;
                        out.quote_frozen = r.quote_frozen;
                        out.base_charge += r.base_charge;
                        out.base_delta += r.base_delta;
                        out.base_available = r.base_available;
                        out.base_frozen = r.base_frozen;
                    })
                    .or_insert_with(|| r.clone());
            });
        maker_accounts.values().for_each(|r| {
            log::debug!("{:?}", r);
            let (ba, bf, qa, qf) = match r.ask_or_bid {
                // -base_frozen, +quote_available
                // base_frozen0 + r.base_delta = base_frozen
                // qa - q0 + abs(r.quote_charge) = abs(quote_delta)
                AskOrBid::Ask => (
                    r.base_available,
                    r.base_frozen + r.base_delta.abs(),
                    r.quote_available + r.quote_charge.abs() - r.quote_delta.abs(),
                    r.quote_frozen,
                ),
                // +base_available, -quote_frozen
                // quote_frozen0 + r.quote_delta = quote_frozen
                // ba0 - ba + abs(r.base_charge) = abs(base_delta)
                AskOrBid::Bid => (
                    r.base_available + r.base_charge.abs() - r.base_delta.abs(),
                    r.base_frozen,
                    r.quote_available,
                    r.quote_frozen + r.quote_delta.abs(),
                ),
            };
            let (new_ba, new_bf, old_ba, old_bf) = (
                r.base_available.to_amount(),
                r.base_frozen.to_amount(),
                ba.to_amount(),
                bf.to_amount(),
            );
            leaves.push(new_account_merkle_leaf(
                &r.user_id, symbol.0, old_ba, old_bf, new_ba, new_bf,
            ));
            let (new_qa, new_qf, old_qa, old_qf) = (
                r.quote_available.to_amount(),
                r.quote_frozen.to_amount(),
                qa.to_amount(),
                qf.to_amount(),
            );
            leaves.push(new_account_merkle_leaf(
                &r.user_id, symbol.1, old_qa, old_qf, new_qa, new_qf,
            ));
        });
        let (new_taker_ba, new_taker_bf, old_taker_ba, old_taker_bf) = (
            taker.base_available.to_amount(),
            taker.base_frozen.to_amount(),
            taker_base_before.available.to_amount(),
            taker_base_before.frozen.to_amount(),
        );
        log::debug!(
            "generating merkle leaf of {:?}: taker base = [{:?}({:?}), {:?}({:?})] -> [{:?}({:?}), {:?}({:?})]",
            taker.event_id,
            old_taker_ba,
            taker_base_before.available,
            old_taker_bf,
            taker_base_before.frozen,
            new_taker_ba,
            taker.base_available,
            new_taker_bf,
            taker.base_frozen,
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
            taker.quote_available.to_amount(),
            taker.quote_frozen.to_amount(),
            taker_quote_before.available.to_amount(),
            taker_quote_before.frozen.to_amount(),
        );
        log::debug!(
            "generating merkle leaf of {:?}: taker quote = [{:?}({:?}), {:?}({:?})] -> [{:?}({:?}), {:?}({:?})]",
            taker.event_id,
            old_taker_qa,
            taker_quote_before.available,
            old_taker_qf,
            taker_quote_before.frozen,
            new_taker_qa,
            taker.quote_available,
            new_taker_qf,
            taker.quote_frozen,
        );
        leaves.push(new_account_merkle_leaf(
            &user_id,
            symbol.1,
            old_taker_qa,
            old_taker_qf,
            new_taker_qa,
            new_taker_qf,
        ));
        let (best_ask, best_bid) = orderbook.get_size_of_best();
        leaves.push(new_bestprice_merkle_leaf(
            symbol,
            best_ask_before.0.to_amount(),
            best_bid_before.0.to_amount(),
            best_ask.map(|a| a.0).unwrap_or(Amount::zero()).to_amount(),
            best_bid.map(|b| b.0).unwrap_or(Amount::zero()).to_amount(),
        ));
        let mut pages = matches
            .page_delta
            .iter()
            .map(|(k, v)| {
                new_orderpage_merkle_leaf(symbol, k.to_amount(), v.0.to_amount(), v.1.to_amount())
            })
            .collect::<Vec<_>>();
        if taker.ask_or_bid == AskOrBid::Ask && !pages.is_empty() {
            pages.reverse();
        }
        leaves.append(&mut pages);
        let merkle_proof = gen_proofs(&mut data.merkle_tree, &leaves);
        self.sender
            .send(Proof {
                event_id,
                user_id,
                cmd: encoded_cmd,
                leaves,
                maker_page_delta: matches.page_delta.len() as u8,
                maker_account_delta: maker_accounts.len() as u8 * 2,
                merkle_proof,
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
            account_after.available.to_amount(),
            account_after.frozen.to_amount(),
            account_before.available.to_amount(),
            account_before.frozen.to_amount(),
        );
        let leaves = vec![new_account_merkle_leaf(
            &cmd.user_id,
            cmd.currency,
            old_available,
            old_frozen,
            new_available,
            new_frozen,
        )];
        let merkle_proof = gen_proofs(merkle_tree, &leaves);
        self.sender
            .send(Proof {
                event_id,
                user_id: cmd.user_id,
                cmd: (cmd, true).into(),
                leaves,
                maker_page_delta: 0,
                maker_account_delta: 0,
                merkle_proof,
                root: merkle_tree.root().clone().into(),
            })
            .unwrap();
    }

    pub fn prove_cmd_rejected(
        &self,
        merkle_tree: &mut GlobalStates,
        event_id: u64,
        cmd: AssetsCmd,
        account_before: &Balance,
    ) {
        let (old_available, old_frozen) = (
            account_before.available.to_amount(),
            account_before.frozen.to_amount(),
        );
        let leaves = vec![new_account_merkle_leaf(
            &cmd.user_id,
            cmd.currency,
            old_available,
            old_frozen,
            old_available,
            old_frozen,
        )];
        let merkle_proof = gen_proofs(merkle_tree, &leaves);
        self.sender
            .send(Proof {
                event_id,
                user_id: cmd.user_id,
                cmd: (cmd, false).into(),
                leaves,
                maker_page_delta: 0,
                maker_account_delta: 0,
                merkle_proof,
                root: merkle_tree.root().clone().into(),
            })
            .unwrap();
    }

    pub fn prove_rejecting_no_reason(
        &self,
        merkle_tree: &mut GlobalStates,
        event_id: u64,
        cmd: AssetsCmd,
        account_before: &Balance,
    ) {
        let (old_available, old_frozen) = (
            account_before.available.to_amount(),
            account_before.frozen.to_amount(),
        );
        let leaves = vec![new_account_merkle_leaf(
            &cmd.user_id,
            cmd.currency,
            old_available,
            old_frozen,
            old_available,
            old_frozen,
        )];
        // TODO
        let merkle_proof = gen_proofs(merkle_tree, &leaves);
        self.sender
            .send(Proof {
                event_id,
                user_id: cmd.user_id,
                cmd: (cmd, false).into(),
                leaves: vec![],
                maker_page_delta: 0,
                maker_account_delta: 0,
                merkle_proof: vec![],
                root: merkle_tree.root().clone().into(),
            })
            .unwrap();
    }
}

fn gen_proofs(merkle_tree: &mut GlobalStates, leaves: &Vec<MerkleLeaf>) -> Vec<u8> {
    let keys = leaves
        .iter()
        .map(|leaf| BlakeTwo256::digest(&leaf.key).into())
        .collect::<Vec<_>>();
    // TODO merge origin to support update all
    leaves.iter().for_each(|leaf| {
        merkle_tree
            .update(BlakeTwo256::digest(&leaf.key).into(), leaf.new_v.into())
            .unwrap();
    });
    merkle_tree
        .merkle_proof(keys.clone())
        .expect("generate merkle proof failed")
        .compile(keys)
        .expect("compile merkle proof failed")
        .into()
}

fn new_account_merkle_leaf(
    user_id: &UserId,
    currency: Currency,
    old_available: u128,
    old_frozen: u128,
    new_available: u128,
    new_frozen: u128,
) -> MerkleLeaf {
    let mut key = vec![ACCOUNT_KEY; 37];
    key[1..33].copy_from_slice(<B256 as AsRef<[u8]>>::as_ref(user_id));
    key[33..].copy_from_slice(&currency.to_le_bytes()[..]);
    MerkleLeaf {
        key,
        old_v: u128le_to_h256(old_available, old_frozen),
        new_v: u128le_to_h256(new_available, new_frozen),
    }
}

fn new_orderbook_merkle_leaf(
    symbol: Symbol,
    old_ask_size: u128,
    old_bid_size: u128,
    new_ask_size: u128,
    new_bid_size: u128,
) -> MerkleLeaf {
    let mut key = vec![ORDERBOOK_KEY; 9];
    key[1..5].copy_from_slice(&symbol.0.to_le_bytes()[..]);
    key[5..].copy_from_slice(&symbol.1.to_le_bytes()[..]);
    MerkleLeaf {
        key,
        old_v: u128le_to_h256(old_ask_size, old_bid_size),
        new_v: u128le_to_h256(new_ask_size, new_bid_size),
    }
}

fn new_bestprice_merkle_leaf(
    symbol: Symbol,
    old_best_ask: u128,
    old_best_bid: u128,
    new_best_ask: u128,
    new_best_bid: u128,
) -> MerkleLeaf {
    let mut key = vec![BESTPRICE_KEY; 9];
    key[1..5].copy_from_slice(&symbol.0.to_le_bytes()[..]);
    key[5..].copy_from_slice(&symbol.1.to_le_bytes()[..]);
    MerkleLeaf {
        key,
        old_v: u128le_to_h256(old_best_ask, old_best_bid),
        new_v: u128le_to_h256(new_best_ask, new_best_bid),
    }
}

fn new_orderpage_merkle_leaf(
    symbol: Symbol,
    price: u128,
    old_size: u128,
    new_size: u128,
) -> MerkleLeaf {
    let mut key = vec![ORDERPAGE_KEY; 25];
    key[1..5].copy_from_slice(&symbol.0.to_le_bytes()[..]);
    key[5..9].copy_from_slice(&symbol.1.to_le_bytes()[..]);
    key[9..].copy_from_slice(&price.to_le_bytes()[..]);
    MerkleLeaf {
        key,
        old_v: u128le_to_h256(0, old_size),
        new_v: u128le_to_h256(0, new_size),
    }
}

#[cfg(test)]
mod test {
    use std::sync::{atomic::AtomicU64, Arc};

    use super::BlakeTwo256;
    use blake2::Digest;
    use rust_decimal_macros::dec;
    use smt::{blake2b::Blake2bHasher, CompiledMerkleProof, H256};

    impl UserId {
        // adapt to legacy code
        pub fn from_low_u64_be(x: u64) -> Self {
            let mut s = [0u8; 32];
            s[24..].copy_from_slice(&x.to_be_bytes());
            Self::new(s)
        }
    }

    impl MerkleLeaf {
        const ACCOUNT_KEY: u8 = 0x00;
        const BESTPRICE_KEY: u8 = 0x02;
        const ORDERBOOK_KEY: u8 = 0x01;
        const ORDERPAGE_KEY: u8 = 0x03;

        fn try_get_account(&self) -> anyhow::Result<(u32, [u8; 32])> {
            if self.key.len() != 37 {
                return Err(anyhow::anyhow!(""));
            }
            match self.key[0] {
                Self::ACCOUNT_KEY => Ok((
                    u32::from_le_bytes(self.key[33..].try_into().map_err(|_| anyhow::anyhow!(""))?),
                    <[u8; 32]>::decode(&mut &self.key[1..33]).map_err(|_| anyhow::anyhow!(""))?,
                )),
                _ => Err(anyhow::anyhow!("")),
            }
        }

        fn try_get_symbol(&self) -> anyhow::Result<(u32, u32)> {
            if self.key.len() != 9 {
                return Err(anyhow::anyhow!(""));
            }
            match self.key[0] {
                Self::ORDERBOOK_KEY | Self::BESTPRICE_KEY => Ok((
                    u32::from_le_bytes(self.key[1..5].try_into().map_err(|_| anyhow::anyhow!(""))?),
                    u32::from_le_bytes(self.key[5..].try_into().map_err(|_| anyhow::anyhow!(""))?),
                )),
                _ => Err(anyhow::anyhow!("")),
            }
        }

        fn try_get_orderpage(&self) -> anyhow::Result<(u32, u32, u128)> {
            if self.key.len() != 25 {
                return Err(anyhow::anyhow!(""));
            }
            match self.key[0] {
                Self::ORDERPAGE_KEY => Ok((
                    u32::from_le_bytes(self.key[1..5].try_into().map_err(|_| anyhow::anyhow!(""))?),
                    u32::from_le_bytes(self.key[5..9].try_into().map_err(|_| anyhow::anyhow!(""))?),
                    u128::from_le_bytes(self.key[9..].try_into().map_err(|_| anyhow::anyhow!(""))?),
                )),
                _ => Err(anyhow::anyhow!("")),
            }
        }

        fn split_value(v: &[u8; 32]) -> ([u8; 16], [u8; 16]) {
            (v[..16].try_into().unwrap(), v[16..].try_into().unwrap())
        }

        fn split_old_to_u128(&self) -> (u128, u128) {
            let (l, r) = Self::split_value(&self.old_v);
            (u128::from_le_bytes(l), u128::from_le_bytes(r))
        }

        fn split_old_to_sum(&self) -> u128 {
            let (l, r) = self.split_old_to_u128();
            l + r
        }

        fn split_new_to_u128(&self) -> (u128, u128) {
            let (l, r) = Self::split_value(&self.new_v);
            (u128::from_le_bytes(l), u128::from_le_bytes(r))
        }

        fn split_new_to_sum(&self) -> u128 {
            let (l, r) = self.split_new_to_u128();
            l + r
        }
    }

    use crate::{assets, clearing, core::*, fusotao::*, matcher, orderbook::*};

    fn split_h256(v: &[u8; 32]) -> ([u8; 16], [u8; 16]) {
        (v[..16].try_into().unwrap(), v[16..].try_into().unwrap())
    }

    fn split_h256_u128(v: &[u8; 32]) -> (u128, u128) {
        let (l, r) = split_h256(v);
        (u128::from_le_bytes(l), u128::from_le_bytes(r))
    }

    fn split_h256_u128_sum(v: &[u8; 32]) -> u128 {
        let (l, r) = split_h256_u128(v);
        l + r
    }

    fn construct_pair() -> OrderBook {
        let base_scale = 5;
        let quote_scale = 1;
        let taker_fee = dec!(0.001);
        let maker_fee = dec!(0.001);
        let min_amount = dec!(1);
        let min_vol = dec!(1);
        OrderBook::new(
            base_scale,
            quote_scale,
            taker_fee,
            maker_fee,
            taker_fee,
            maker_fee,
            1,
            min_amount,
            min_vol,
            true,
            true,
        )
    }

    #[test]
    pub fn test_transfer_in() {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mut merkle_tree = GlobalStates::default();
            let pp = Prover::new(tx, Arc::new(AtomicU64::new(0)));
            let mut all = Accounts::new();
            let cmd0 = AssetsCmd {
                user_id: UserId::from_low_u64_be(1),
                in_or_out: InOrOut::In,
                currency: 1,
                amount: dec!(1.11111),
                block_number: 1,
                extrinsic_hash: vec![0],
            };
            let after =
                assets::add_to_available(&mut all, &cmd0.user_id, cmd0.currency, cmd0.amount)
                    .unwrap();
            let cmd1 = cmd0.clone();
            pp.prove_assets_cmd(
                &mut merkle_tree,
                1,
                cmd0,
                &assets::Balance::default(),
                &after,
            );
            let transfer_again =
                assets::add_to_available(&mut all, &cmd1.user_id, cmd1.currency, cmd1.amount)
                    .unwrap();
            pp.prove_assets_cmd(&mut merkle_tree, 1, cmd1, &after, &transfer_again);
        });
        let proof = rx.recv().unwrap();
        let mp = CompiledMerkleProof(proof.merkle_proof.clone());
        let old = proof
            .leaves
            .iter()
            .map(|v| (BlakeTwo256::digest(&v.key).into(), v.old_v.into()))
            .collect::<Vec<_>>();
        let r = mp.verify::<Blake2bHasher>(&H256::default(), old).unwrap();
        assert!(r);
        let new = proof
            .leaves
            .iter()
            .map(|v| (BlakeTwo256::digest(&v.key).into(), v.new_v.into()))
            .collect::<Vec<_>>();
        let r = mp.verify::<Blake2bHasher>(&proof.root.into(), new).unwrap();
        assert!(r);
        assert_eq!(
            split_h256_u128(&proof.leaves[0].new_v),
            (1111110000000000000, 0)
        );
        assert_eq!(split_h256_u128(&proof.leaves[0].old_v), (0, 0));
        let new_root = proof.root.clone();
        let proof = rx.recv().unwrap();
        let mp = CompiledMerkleProof(proof.merkle_proof.clone());
        let old = proof
            .leaves
            .iter()
            .map(|v| (BlakeTwo256::digest(&v.key).into(), v.old_v.into()))
            .collect::<Vec<_>>();
        let r = mp.verify::<Blake2bHasher>(&new_root.into(), old).unwrap();
        assert!(r);
        let new = proof
            .leaves
            .iter()
            .map(|v| (BlakeTwo256::digest(&v.key).into(), v.new_v.into()))
            .collect::<Vec<_>>();
        let r = mp.verify::<Blake2bHasher>(&proof.root.into(), new).unwrap();
        assert!(r);
        assert_eq!(
            split_h256_u128(&proof.leaves[0].new_v),
            (2222220000000000000, 0)
        );
        assert_eq!(
            split_h256_u128(&proof.leaves[0].old_v),
            (1111110000000000000, 0)
        );
    }

    #[test]
    pub fn test_trade() {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let merkle_tree = GlobalStates::default();
            let all = Accounts::new();
            let orderbook = construct_pair();
            let mut orderbooks = std::collections::HashMap::new();
            let (mf, tf) = (orderbook.maker_fee, orderbook.taker_fee);
            orderbooks.insert((1, 0), orderbook);
            let mut data = Data {
                orderbooks,
                accounts: all,
                merkle_tree,
                current_event_id: 0,
                tvl: Amount::zero(),
            };
            let pp = Prover::new(tx, Arc::new(AtomicU64::new(0)));
            let cmd0 = AssetsCmd {
                user_id: UserId::from_low_u64_be(1),
                in_or_out: InOrOut::In,
                currency: 1,
                amount: dec!(1.11111),
                block_number: 1,
                extrinsic_hash: vec![0],
            };
            let after = assets::add_to_available(
                &mut data.accounts,
                &cmd0.user_id,
                cmd0.currency,
                cmd0.amount,
            )
            .unwrap();
            pp.prove_assets_cmd(
                &mut data.merkle_tree,
                1,
                cmd0,
                &assets::Balance::default(),
                &after,
            );
            let cmd1 = AssetsCmd {
                user_id: UserId::from_low_u64_be(2),
                in_or_out: InOrOut::In,
                currency: 0,
                amount: dec!(99.99),
                block_number: 1,
                extrinsic_hash: vec![0],
            };
            let transfer_again = assets::add_to_available(
                &mut data.accounts,
                &cmd1.user_id,
                cmd1.currency,
                cmd1.amount,
            )
            .unwrap();
            pp.prove_assets_cmd(&mut data.merkle_tree, 1, cmd1, &after, &transfer_again);

            let size = data.orderbooks.get(&(1, 0)).unwrap().size();
            let cmd2 = LimitCmd {
                symbol: (1, 0),
                user_id: UserId::from_low_u64_be(1),
                order_id: 1,
                price: dec!(100),
                amount: dec!(0.11),
                ask_or_bid: AskOrBid::Ask,
                nonce: 1,
                signature: vec![0],
            };
            let (best_ask_before, best_bid_before) =
                data.orderbooks.get(&(1, 0)).unwrap().get_size_of_best();
            let taker_base_before =
                assets::get_balance_to_owned(&data.accounts, &cmd2.user_id, cmd2.symbol.0);
            let taker_quote_before =
                assets::get_balance_to_owned(&data.accounts, &cmd2.user_id, cmd2.symbol.1);
            let (c, val) =
                assets::freeze_if(&cmd2.symbol, cmd2.ask_or_bid, cmd2.price, cmd2.amount);
            assets::try_freeze(&mut data.accounts, &cmd2.user_id, c, val).unwrap();
            let mr = matcher::execute_limit(
                data.orderbooks.get_mut(&(1, 0)).unwrap(),
                cmd2.user_id,
                cmd2.order_id,
                cmd2.price,
                cmd2.amount,
                cmd2.ask_or_bid,
            );
            let cr = clearing::clear(&mut data.accounts, 3, &(1, 0), tf, mf, &mr, 0);
            pp.prove_trade_cmd(
                &mut data,
                cmd2.nonce,
                cmd2.signature.clone(),
                (cmd2, mf, tf).into(),
                size.0,
                size.1,
                best_ask_before.unwrap_or((Decimal::zero(), Decimal::zero())),
                best_bid_before.unwrap_or((Decimal::zero(), Decimal::zero())),
                &taker_base_before,
                &taker_quote_before,
                &cr,
                &mr,
            );

            let size = data.orderbooks.get(&(1, 0)).unwrap().size();
            let cmd2 = LimitCmd {
                symbol: (1, 0),
                user_id: UserId::from_low_u64_be(2),
                order_id: 3,
                price: dec!(90),
                amount: dec!(0.01),
                ask_or_bid: AskOrBid::Bid,
                nonce: 1,
                signature: vec![0],
            };
            let (best_ask_before, best_bid_before) =
                data.orderbooks.get(&(1, 0)).unwrap().get_size_of_best();
            let taker_base_before =
                assets::get_balance_to_owned(&data.accounts, &cmd2.user_id, cmd2.symbol.0);
            let taker_quote_before =
                assets::get_balance_to_owned(&data.accounts, &cmd2.user_id, cmd2.symbol.1);
            let (c, val) =
                assets::freeze_if(&cmd2.symbol, cmd2.ask_or_bid, cmd2.price, cmd2.amount);
            assets::try_freeze(&mut data.accounts, &cmd2.user_id, c, val).unwrap();
            let mr = matcher::execute_limit(
                data.orderbooks.get_mut(&(1, 0)).unwrap(),
                cmd2.user_id,
                cmd2.order_id,
                cmd2.price,
                cmd2.amount,
                cmd2.ask_or_bid,
            );
            let cr = clearing::clear(&mut data.accounts, 5, &(1, 0), tf, mf, &mr, 0);
            pp.prove_trade_cmd(
                &mut data,
                cmd2.nonce,
                cmd2.signature.clone(),
                (cmd2, mf, tf).into(),
                size.0,
                size.1,
                best_ask_before.unwrap_or((Decimal::zero(), Decimal::zero())),
                best_bid_before.unwrap_or((Decimal::zero(), Decimal::zero())),
                &taker_base_before,
                &taker_quote_before,
                &cr,
                &mr,
            );

            let size = data.orderbooks.get(&(1, 0)).unwrap().size();
            let cmd2 = LimitCmd {
                symbol: (1, 0),
                user_id: UserId::from_low_u64_be(1),
                order_id: 4,
                price: dec!(100),
                amount: dec!(0.11),
                ask_or_bid: AskOrBid::Ask,
                nonce: 1,
                signature: vec![0],
            };
            let (best_ask_before, best_bid_before) =
                data.orderbooks.get(&(1, 0)).unwrap().get_size_of_best();
            let taker_base_before =
                assets::get_balance_to_owned(&data.accounts, &cmd2.user_id, cmd2.symbol.0);
            let taker_quote_before =
                assets::get_balance_to_owned(&data.accounts, &cmd2.user_id, cmd2.symbol.1);
            let (c, val) =
                assets::freeze_if(&cmd2.symbol, cmd2.ask_or_bid, cmd2.price, cmd2.amount);
            assets::try_freeze(&mut data.accounts, &cmd2.user_id, c, val).unwrap();
            let mr = matcher::execute_limit(
                data.orderbooks.get_mut(&(1, 0)).unwrap(),
                cmd2.user_id,
                cmd2.order_id,
                cmd2.price,
                cmd2.amount,
                cmd2.ask_or_bid,
            );
            let cr = clearing::clear(&mut data.accounts, 6, &(1, 0), tf, mf, &mr, 0);
            pp.prove_trade_cmd(
                &mut data,
                cmd2.nonce,
                cmd2.signature.clone(),
                (cmd2, mf, tf).into(),
                size.0,
                size.1,
                best_ask_before.unwrap_or((Decimal::zero(), Decimal::zero())),
                best_bid_before.unwrap_or((Decimal::zero(), Decimal::zero())),
                &taker_base_before,
                &taker_quote_before,
                &cr,
                &mr,
            );

            let size = data.orderbooks.get(&(1, 0)).unwrap().size();
            let cmd2 = LimitCmd {
                symbol: (1, 0),
                user_id: UserId::from_low_u64_be(2),
                order_id: 5,
                price: dec!(110),
                amount: dec!(0.5),
                ask_or_bid: AskOrBid::Bid,
                nonce: 1,
                signature: vec![0],
            };
            let (best_ask_before, best_bid_before) =
                data.orderbooks.get(&(1, 0)).unwrap().get_size_of_best();
            let taker_base_before =
                assets::get_balance_to_owned(&data.accounts, &cmd2.user_id, cmd2.symbol.0);
            let taker_quote_before =
                assets::get_balance_to_owned(&data.accounts, &cmd2.user_id, cmd2.symbol.1);
            let (c, val) =
                assets::freeze_if(&cmd2.symbol, cmd2.ask_or_bid, cmd2.price, cmd2.amount);
            assets::try_freeze(&mut data.accounts, &cmd2.user_id, c, val).unwrap();
            let mr = matcher::execute_limit(
                data.orderbooks.get_mut(&(1, 0)).unwrap(),
                cmd2.user_id,
                cmd2.order_id,
                cmd2.price,
                cmd2.amount,
                cmd2.ask_or_bid,
            );
            let cr = clearing::clear(&mut data.accounts, 7, &(1, 0), tf, mf, &mr, 0);
            pp.prove_trade_cmd(
                &mut data,
                cmd2.nonce,
                cmd2.signature.clone(),
                (cmd2, mf, tf).into(),
                size.0,
                size.1,
                best_ask_before.unwrap_or((Decimal::zero(), Decimal::zero())),
                best_bid_before.unwrap_or((Decimal::zero(), Decimal::zero())),
                &taker_base_before,
                &taker_quote_before,
                &cr,
                &mr,
            );
            println!("{:?}", cr.last().unwrap());

            let size = data.orderbooks.get(&(1, 0)).unwrap().size();
            let cmd2 = LimitCmd {
                symbol: (1, 0),
                user_id: UserId::from_low_u64_be(1),
                order_id: 6,
                price: dec!(88),
                amount: dec!(0.3),
                ask_or_bid: AskOrBid::Ask,
                nonce: 1,
                signature: vec![0],
            };
            let (best_ask_before, best_bid_before) =
                data.orderbooks.get(&(1, 0)).unwrap().get_size_of_best();
            let taker_base_before =
                assets::get_balance_to_owned(&data.accounts, &cmd2.user_id, cmd2.symbol.0);
            let taker_quote_before =
                assets::get_balance_to_owned(&data.accounts, &cmd2.user_id, cmd2.symbol.1);
            let (c, val) =
                assets::freeze_if(&cmd2.symbol, cmd2.ask_or_bid, cmd2.price, cmd2.amount);
            assets::try_freeze(&mut data.accounts, &cmd2.user_id, c, val).unwrap();
            let mr = matcher::execute_limit(
                data.orderbooks.get_mut(&(1, 0)).unwrap(),
                cmd2.user_id,
                cmd2.order_id,
                cmd2.price,
                cmd2.amount,
                cmd2.ask_or_bid,
            );
            let cr = clearing::clear(&mut data.accounts, 8, &(1, 0), tf, mf, &mr, 0);
            pp.prove_trade_cmd(
                &mut data,
                cmd2.nonce,
                cmd2.signature.clone(),
                (cmd2, mf, tf).into(),
                size.0,
                size.1,
                best_ask_before.unwrap_or((Decimal::zero(), Decimal::zero())),
                best_bid_before.unwrap_or((Decimal::zero(), Decimal::zero())),
                &taker_base_before,
                &taker_quote_before,
                &cr,
                &mr,
            );
        });
        // ignore transfer in
        rx.recv().unwrap();
        rx.recv().unwrap();
        // ask 0.11, 100
        {
            let proof = rx.recv().unwrap();
            assert_eq!(proof.leaves.len(), 5);
            // ask,bid
            assert_eq!(split_h256_u128(&proof.leaves[0].old_v), (0, 0));
            assert_eq!(
                split_h256_u128(&proof.leaves[0].new_v),
                (110000000000000000, 0)
            );
            // base
            assert_eq!(
                split_h256_u128(&proof.leaves[1].old_v),
                (1111110000000000000, 0)
            );
            assert_eq!(
                split_h256_u128(&proof.leaves[1].new_v),
                (1001110000000000000, 110000000000000000)
            );
            // quote
            assert_eq!(split_h256_u128(&proof.leaves[2].old_v), (0, 0));
            assert_eq!(split_h256_u128(&proof.leaves[2].new_v), (0, 0));
            // best price a,b
            assert_eq!(split_h256_u128(&proof.leaves[3].old_v), (0, 0));
            assert_eq!(
                split_h256_u128(&proof.leaves[3].new_v),
                (100_000000000000000000, 0)
            );
            // orderpage at 100 = 0.11
            assert_eq!(split_h256_u128_sum(&proof.leaves[4].old_v), 0);
            assert_eq!(
                split_h256_u128_sum(&proof.leaves[4].new_v),
                110000000000000000
            );
        }
        // bid 0.01, 90
        {
            let proof = rx.recv().unwrap();
            // ask,bid
            assert_eq!(
                split_h256_u128(&proof.leaves[0].old_v),
                (110000000000000000, 0)
            );
            assert_eq!(
                split_h256_u128(&proof.leaves[0].new_v),
                (110000000000000000, 10000000000000000)
            );
            // base
            assert_eq!(split_h256_u128(&proof.leaves[1].old_v), (0, 0));
            assert_eq!(split_h256_u128(&proof.leaves[1].new_v), (0, 0));
            // quote
            assert_eq!(
                split_h256_u128(&proof.leaves[2].old_v),
                (99990000000000000000, 0)
            );
            assert_eq!(
                split_h256_u128(&proof.leaves[2].new_v),
                (99090000000000000000, 900000000000000000)
            );
        }
        // ask 0.11, 100
        {
            let proof = rx.recv().unwrap();
            // ask,bid
            assert_eq!(
                split_h256_u128(&proof.leaves[0].old_v),
                (110000000000000000, 10000000000000000)
            );
            assert_eq!(
                split_h256_u128(&proof.leaves[0].new_v),
                (220000000000000000, 10000000000000000)
            );
            // base
            assert_eq!(
                split_h256_u128(&proof.leaves[1].old_v),
                (1001110000000000000, 110000000000000000)
            );
            assert_eq!(
                split_h256_u128(&proof.leaves[1].new_v),
                (891110000000000000, 220000000000000000)
            );
            // quote
            assert_eq!(split_h256_u128(&proof.leaves[2].old_v), (0, 0));
            assert_eq!(split_h256_u128(&proof.leaves[2].new_v), (0, 0));
        }
        // bid 0.5, 110
        {
            let proof = rx.recv().unwrap();
            // ask,bid
            assert_eq!(
                split_h256_u128(&proof.leaves[0].old_v),
                (220000000000000000, 10000000000000000)
            );
            assert_eq!(
                split_h256_u128(&proof.leaves[0].new_v),
                (0, 290000000000000000)
            );
            // maker - base
            assert_eq!(
                split_h256_u128(&proof.leaves[1].old_v),
                (891110000000000000, 220000000000000000)
            );
            assert_eq!(
                split_h256_u128(&proof.leaves[1].new_v),
                (891110000000000000, 0)
            );
            // maker - quote
            assert_eq!(split_h256_u128(&proof.leaves[2].old_v), (0, 0));
            assert_eq!(
                split_h256_u128(&proof.leaves[2].new_v),
                (21978000000000000000, 0)
            );
            // taker - base
            assert_eq!(split_h256_u128(&proof.leaves[3].old_v), (0, 0));
            assert_eq!(
                split_h256_u128(&proof.leaves[3].new_v),
                (219780000000000000, 0)
            );
            // taker - quote
            assert_eq!(
                split_h256_u128(&proof.leaves[4].old_v),
                (99090000000000000000, 900000000000000000)
            );
            let (na, nf) = split_h256_u128(&proof.leaves[4].new_v);
            assert_eq!(na + nf + 22000000000000000000, 99990000000000000000);
        }
        // ask 0.3, 88
        {
            let proof = rx.recv().unwrap();
            // ask,bid
            assert_eq!(
                split_h256_u128(&proof.leaves[0].old_v),
                (0, 290000000000000000)
            );
            assert_eq!(
                split_h256_u128(&proof.leaves[0].new_v),
                (10000000000000000, 0)
            );
            // maker - base
            let mb0 = split_h256_u128_sum(&proof.leaves[1].old_v);
            let mb1 = split_h256_u128_sum(&proof.leaves[1].new_v);
            // maker - quote
            let mq0 = split_h256_u128_sum(&proof.leaves[2].old_v);
            let mq1 = split_h256_u128_sum(&proof.leaves[2].new_v);
            // // maker - base
            // let mb10 = split_h256_u128_sum(&proof.leaves[3].old_v);
            // let mb11 = split_h256_u128_sum(&proof.leaves[3].new_v);
            // // maker - quote
            // let mq10 = split_h256_u128_sum(&proof.leaves[4].old_v);
            // let mq11 = split_h256_u128_sum(&proof.leaves[4].new_v);

            let incr_base = mb1 - mb0;
            let decr_quote = mq0 - mq1;
            // taker - base
            let tb0 = split_h256_u128_sum(&proof.leaves[3].old_v);
            let tb1 = split_h256_u128_sum(&proof.leaves[3].new_v);
            assert_eq!(incr_base, (tb0 - tb1) / 1000 * 999);
            // taker - quote
            let tq0 = split_h256_u128_sum(&proof.leaves[4].old_v);
            let tq1 = split_h256_u128_sum(&proof.leaves[4].new_v);
            assert_eq!(decr_quote / 1000 * 999, tq1 - tq0);
        }
    }

    #[test]
    pub fn test_price() {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mut merkle_tree = GlobalStates::default();
            let pp = Prover::new(tx, Arc::new(AtomicU64::new(0)));
            let mut all = Accounts::new();
            let orderbook = construct_pair();
            let cmd0 = AssetsCmd {
                user_id: UserId::from_low_u64_be(1),
                in_or_out: InOrOut::In,
                currency: 0,
                amount: dec!(100),
                block_number: 1,
                extrinsic_hash: vec![0],
            };
            let after =
                assets::add_to_available(&mut all, &cmd0.user_id, cmd0.currency, cmd0.amount)
                    .unwrap();
            pp.prove_assets_cmd(
                &mut merkle_tree,
                1,
                cmd0,
                &assets::Balance::default(),
                &after,
            );
            let cmd1 = AssetsCmd {
                user_id: UserId::from_low_u64_be(2),
                in_or_out: InOrOut::In,
                currency: 1,
                amount: dec!(1000),
                block_number: 1,
                extrinsic_hash: vec![0],
            };
            let transfer_again =
                assets::add_to_available(&mut all, &cmd1.user_id, cmd1.currency, cmd1.amount)
                    .unwrap();
            pp.prove_assets_cmd(&mut merkle_tree, 1, cmd1, &after, &transfer_again);

            let mut orderbooks = std::collections::HashMap::new();
            let (mf, tf) = (orderbook.maker_fee, orderbook.taker_fee);
            orderbooks.insert((0, 1), orderbook);
            let mut data = Data {
                orderbooks,
                accounts: all,
                merkle_tree,
                current_event_id: 0,
                tvl: Amount::zero(),
            };

            // alice ask p=10, a=0.5
            {
                let size = data.orderbooks.get(&(0, 1)).unwrap().size();
                let cmd2 = LimitCmd {
                    symbol: (0, 1),
                    user_id: UserId::from_low_u64_be(1),
                    order_id: 1,
                    price: dec!(10),
                    amount: dec!(0.5),
                    ask_or_bid: AskOrBid::Ask,
                    nonce: 1,
                    signature: vec![0],
                };
                let (best_ask_before, best_bid_before) =
                    data.orderbooks.get(&(0, 1)).unwrap().get_size_of_best();
                let taker_base_before =
                    assets::get_balance_to_owned(&data.accounts, &cmd2.user_id, cmd2.symbol.0);
                let taker_quote_before =
                    assets::get_balance_to_owned(&data.accounts, &cmd2.user_id, cmd2.symbol.1);
                let (c, val) =
                    assets::freeze_if(&cmd2.symbol, cmd2.ask_or_bid, cmd2.price, cmd2.amount);
                assets::try_freeze(&mut data.accounts, &cmd2.user_id, c, val).unwrap();
                let mr = matcher::execute_limit(
                    data.orderbooks.get_mut(&(0, 1)).unwrap(),
                    cmd2.user_id,
                    cmd2.order_id,
                    cmd2.price,
                    cmd2.amount,
                    cmd2.ask_or_bid,
                );
                let cr = clearing::clear(&mut data.accounts, 3, &(0, 1), tf, mf, &mr, 0);
                pp.prove_trade_cmd(
                    &mut data,
                    cmd2.nonce,
                    cmd2.signature.clone(),
                    (cmd2, mf, tf).into(),
                    size.0,
                    size.1,
                    best_ask_before.unwrap_or((Decimal::zero(), Decimal::zero())),
                    best_bid_before.unwrap_or((Decimal::zero(), Decimal::zero())),
                    &taker_base_before,
                    &taker_quote_before,
                    &cr,
                    &mr,
                );
            }
            // alice ask p=10, a=0.6
            {
                let size = data.orderbooks.get(&(0, 1)).unwrap().size();
                let cmd2 = LimitCmd {
                    symbol: (0, 1),
                    user_id: UserId::from_low_u64_be(1),
                    order_id: 2,
                    price: dec!(10),
                    amount: dec!(0.6),
                    ask_or_bid: AskOrBid::Ask,
                    nonce: 1,
                    signature: vec![0],
                };
                let (best_ask_before, best_bid_before) =
                    data.orderbooks.get(&(0, 1)).unwrap().get_size_of_best();
                let taker_base_before =
                    assets::get_balance_to_owned(&data.accounts, &cmd2.user_id, cmd2.symbol.0);
                let taker_quote_before =
                    assets::get_balance_to_owned(&data.accounts, &cmd2.user_id, cmd2.symbol.1);
                let (c, val) =
                    assets::freeze_if(&cmd2.symbol, cmd2.ask_or_bid, cmd2.price, cmd2.amount);
                assets::try_freeze(&mut data.accounts, &cmd2.user_id, c, val).unwrap();
                let mr = matcher::execute_limit(
                    data.orderbooks.get_mut(&(0, 1)).unwrap(),
                    cmd2.user_id,
                    cmd2.order_id,
                    cmd2.price,
                    cmd2.amount,
                    cmd2.ask_or_bid,
                );
                let cr = clearing::clear(&mut data.accounts, 4, &(0, 1), tf, mf, &mr, 0);
                pp.prove_trade_cmd(
                    &mut data,
                    cmd2.nonce,
                    cmd2.signature.clone(),
                    (cmd2, mf, tf).into(),
                    size.0,
                    size.1,
                    best_ask_before.unwrap_or((Decimal::zero(), Decimal::zero())),
                    best_bid_before.unwrap_or((Decimal::zero(), Decimal::zero())),
                    &taker_base_before,
                    &taker_quote_before,
                    &cr,
                    &mr,
                );
            }
            // alice ask p=10, a=0.6
            {
                let size = data.orderbooks.get(&(0, 1)).unwrap().size();
                let cmd2 = LimitCmd {
                    symbol: (0, 1),
                    user_id: UserId::from_low_u64_be(1),
                    order_id: 3,
                    price: dec!(9.9),
                    amount: dec!(0.1),
                    ask_or_bid: AskOrBid::Ask,
                    nonce: 1,
                    signature: vec![0],
                };
                let (best_ask_before, best_bid_before) =
                    data.orderbooks.get(&(0, 1)).unwrap().get_size_of_best();
                let taker_base_before =
                    assets::get_balance_to_owned(&data.accounts, &cmd2.user_id, cmd2.symbol.0);
                let taker_quote_before =
                    assets::get_balance_to_owned(&data.accounts, &cmd2.user_id, cmd2.symbol.1);
                let (c, val) =
                    assets::freeze_if(&cmd2.symbol, cmd2.ask_or_bid, cmd2.price, cmd2.amount);
                assets::try_freeze(&mut data.accounts, &cmd2.user_id, c, val).unwrap();
                let mr = matcher::execute_limit(
                    data.orderbooks.get_mut(&(0, 1)).unwrap(),
                    cmd2.user_id,
                    cmd2.order_id,
                    cmd2.price,
                    cmd2.amount,
                    cmd2.ask_or_bid,
                );
                let cr = clearing::clear(&mut data.accounts, 5, &(0, 1), tf, mf, &mr, 0);
                pp.prove_trade_cmd(
                    &mut data,
                    cmd2.nonce,
                    cmd2.signature.clone(),
                    (cmd2, mf, tf).into(),
                    size.0,
                    size.1,
                    best_ask_before.unwrap_or((Decimal::zero(), Decimal::zero())),
                    best_bid_before.unwrap_or((Decimal::zero(), Decimal::zero())),
                    &taker_base_before,
                    &taker_quote_before,
                    &cr,
                    &mr,
                );
            }
            // bob p=9.9, a=0.5
            {
                let size = data.orderbooks.get(&(0, 1)).unwrap().size();
                let cmd2 = LimitCmd {
                    symbol: (0, 1),
                    user_id: UserId::from_low_u64_be(2),
                    order_id: 4,
                    price: dec!(9.9),
                    amount: dec!(0.5),
                    ask_or_bid: AskOrBid::Bid,
                    nonce: 1,
                    signature: vec![0],
                };
                let (best_ask_before, best_bid_before) =
                    data.orderbooks.get(&(0, 1)).unwrap().get_size_of_best();
                let taker_base_before =
                    assets::get_balance_to_owned(&data.accounts, &cmd2.user_id, cmd2.symbol.0);
                let taker_quote_before =
                    assets::get_balance_to_owned(&data.accounts, &cmd2.user_id, cmd2.symbol.1);
                let (c, val) =
                    assets::freeze_if(&cmd2.symbol, cmd2.ask_or_bid, cmd2.price, cmd2.amount);
                assets::try_freeze(&mut data.accounts, &cmd2.user_id, c, val).unwrap();
                let mr = matcher::execute_limit(
                    data.orderbooks.get_mut(&(0, 1)).unwrap(),
                    cmd2.user_id,
                    cmd2.order_id,
                    cmd2.price,
                    cmd2.amount,
                    cmd2.ask_or_bid,
                );
                let cr = clearing::clear(&mut data.accounts, 6, &(0, 1), tf, mf, &mr, 0);
                pp.prove_trade_cmd(
                    &mut data,
                    cmd2.nonce,
                    cmd2.signature.clone(),
                    (cmd2, mf, tf).into(),
                    size.0,
                    size.1,
                    best_ask_before.unwrap_or((Decimal::zero(), Decimal::zero())),
                    best_bid_before.unwrap_or((Decimal::zero(), Decimal::zero())),
                    &taker_base_before,
                    &taker_quote_before,
                    &cr,
                    &mr,
                );
            }
        });
        // ignore transfer in
        rx.recv().unwrap();
        rx.recv().unwrap();
        /*
         * ignore ask
         * 1. p=10, a=0.5
         * 2. p=10, a=0.6
         * 3. p=9.9, a=0.1
         */
        rx.recv().unwrap();
        rx.recv().unwrap();
        rx.recv().unwrap();
        // bid p=9.9, a=0.5
        {
            let proof = rx.recv().unwrap();
            assert_eq!(proof.leaves.len(), 7);
            // best price a,b
            assert_eq!(
                split_h256_u128(&proof.leaves[5].old_v),
                (dec!(9.9).to_amount(), 0)
            );
            assert_eq!(
                split_h256_u128(&proof.leaves[5].new_v),
                (dec!(10).to_amount(), dec!(9.9).to_amount())
            );
            assert_eq!(
                &proof.leaves[5].old_v,
                &hex::decode("00005e2c8cdd6389000000000000000000000000000000000000000000000000")
                    .unwrap()[..]
            );
            assert_eq!(
                &proof.leaves[5].new_v,
                &hex::decode("0000e8890423c78a000000000000000000005e2c8cdd63890000000000000000")
                    .unwrap()[..]
            );
            // p=9.9
            assert_eq!(
                split_h256_u128_sum(&proof.leaves[6].old_v),
                dec!(0.1).to_amount()
            );
            assert_eq!(
                split_h256_u128_sum(&proof.leaves[6].new_v),
                dec!(0.4).to_amount()
            );

            let maker_accounts = 2u8;
            let pages = 1u8;
            let (base, quote) = (0, 1);
            let leaves_count = (4u8 + maker_accounts + pages) as usize;
            assert!(proof.leaves.len() == leaves_count);
            assert!(maker_accounts % 2 == 0);
            let price = dec!(9.9).to_amount();
            let amount = dec!(0.5).to_amount();
            let base_charged = dec!(0.0001).to_amount();

            let (ask0, bid0) = proof.leaves[0].split_old_to_u128();
            let (ask1, bid1) = proof.leaves[0].split_new_to_u128();
            let ask_delta = ask0 - ask1;
            let bid_delta = bid1 - bid0;
            let taker_base = &proof.leaves[maker_accounts as usize + 1];
            let (tba0, tbf0) = taker_base.split_old_to_u128();
            let (tba1, tbf1) = taker_base.split_new_to_u128();
            let tb_delta = (tba1 + tbf1) - (tba0 + tbf0);

            let best_price = &proof.leaves[maker_accounts as usize + 3];
            let (b, q) = best_price.try_get_symbol().unwrap();
            assert!(b == base && q == quote);
            let (best_ask0, best_bid0) = best_price.split_old_to_u128();
            let (best_ask1, best_bid1) = best_price.split_new_to_u128();

            if ask_delta != 0 {
                // trading happened
                assert!(pages > 0 && price >= best_ask0,);
                // best_ask0 <= page0 < page1 < .. < pagen <= best_ask1
                let mut pre_best = best_ask0;
                let mut taken_asks = 0u128;
                for i in 0..pages as usize - 1 {
                    let page = &proof.leaves[maker_accounts as usize + 4 + i];
                    let (b, q, p) = page.try_get_orderpage().unwrap();
                    assert!(b == base && q == quote,);
                    assert!(pre_best <= p,);
                    pre_best = p;
                    assert!(page.split_new_to_sum() == 0);
                    taken_asks += page.split_old_to_sum();
                }
                if bid_delta != 0 {
                    // partial_filled
                    let taker_price_page = proof.leaves.last().unwrap();
                    let (b, q, p) = taker_price_page.try_get_orderpage().unwrap();
                    assert!(b == base && q == quote && p == price,);
                    assert!(best_bid1 == price,);
                    let prv_is_maker = taker_price_page.split_old_to_sum();
                    let now_is_taker = taker_price_page.split_new_to_sum();
                    assert!(taken_asks + prv_is_maker + now_is_taker == amount,);
                } else {
                    // filled or conditional_canceled
                    let vanity_maker = proof.leaves.last().unwrap();
                    let (b, q, p) = vanity_maker.try_get_orderpage().unwrap();
                    assert!(b == base && q == quote && p == price,);
                    assert!(best_bid1 == best_bid0,);
                    let prv_is_maker = vanity_maker.split_old_to_sum();
                    let now_is_maker = vanity_maker.split_new_to_sum();
                    assert!(tb_delta + base_charged == taken_asks + prv_is_maker - now_is_maker,);
                }
            } else {
                // no trading
                assert!(best_ask1 == best_ask0,);
                if bid_delta != 0 {
                    // placed
                    let taker_price_page = proof.leaves.last().unwrap();
                    let (b, q, p) = taker_price_page.try_get_orderpage().unwrap();
                    assert!(b == base && q == quote && p == price,);
                    let prv_is_maker = taker_price_page.split_old_to_sum();
                    let now_is_maker = taker_price_page.split_new_to_sum();
                    assert!(amount == now_is_maker - prv_is_maker,);
                }
            }
        }
    }

    #[test]
    pub fn test_canceling() {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mut merkle_tree = GlobalStates::default();
            let pp = Prover::new(tx, Arc::new(AtomicU64::new(0)));
            let mut all = Accounts::new();
            let orderbook = construct_pair();
            let cmd0 = AssetsCmd {
                user_id: UserId::from_low_u64_be(1),
                in_or_out: InOrOut::In,
                currency: 0,
                amount: dec!(100),
                block_number: 1,
                extrinsic_hash: vec![0],
            };
            let after =
                assets::add_to_available(&mut all, &cmd0.user_id, cmd0.currency, cmd0.amount)
                    .unwrap();
            pp.prove_assets_cmd(
                &mut merkle_tree,
                1,
                cmd0,
                &assets::Balance::default(),
                &after,
            );
            let cmd1 = AssetsCmd {
                user_id: UserId::from_low_u64_be(2),
                in_or_out: InOrOut::In,
                currency: 1,
                amount: dec!(1000),
                block_number: 1,
                extrinsic_hash: vec![0],
            };
            let transfer_again =
                assets::add_to_available(&mut all, &cmd1.user_id, cmd1.currency, cmd1.amount)
                    .unwrap();
            pp.prove_assets_cmd(&mut merkle_tree, 1, cmd1, &after, &transfer_again);

            let mut orderbooks = std::collections::HashMap::new();
            let (mf, tf) = (orderbook.maker_fee, orderbook.taker_fee);
            orderbooks.insert((0, 1), orderbook);
            let mut data = Data {
                orderbooks,
                accounts: all,
                merkle_tree,
                current_event_id: 0,
                tvl: Amount::zero(),
            };

            // alice ask p=10, a=1.1
            {
                let size = data.orderbooks.get(&(0, 1)).unwrap().size();
                let cmd2 = LimitCmd {
                    symbol: (0, 1),
                    user_id: UserId::from_low_u64_be(1),
                    order_id: 1,
                    price: dec!(10),
                    amount: dec!(1.1),
                    ask_or_bid: AskOrBid::Ask,
                    nonce: 1,
                    signature: vec![0],
                };
                let (best_ask_before, best_bid_before) =
                    data.orderbooks.get(&(0, 1)).unwrap().get_size_of_best();
                let taker_base_before =
                    assets::get_balance_to_owned(&data.accounts, &cmd2.user_id, cmd2.symbol.0);
                let taker_quote_before =
                    assets::get_balance_to_owned(&data.accounts, &cmd2.user_id, cmd2.symbol.1);
                let (c, val) =
                    assets::freeze_if(&cmd2.symbol, cmd2.ask_or_bid, cmd2.price, cmd2.amount);
                assets::try_freeze(&mut data.accounts, &cmd2.user_id, c, val).unwrap();
                let mr = matcher::execute_limit(
                    data.orderbooks.get_mut(&(0, 1)).unwrap(),
                    cmd2.user_id,
                    cmd2.order_id,
                    cmd2.price,
                    cmd2.amount,
                    cmd2.ask_or_bid,
                );
                let cr = clearing::clear(&mut data.accounts, 3, &(0, 1), tf, mf, &mr, 0);
                pp.prove_trade_cmd(
                    &mut data,
                    cmd2.nonce,
                    cmd2.signature.clone(),
                    (cmd2, mf, tf).into(),
                    size.0,
                    size.1,
                    best_ask_before.unwrap_or((Decimal::zero(), Decimal::zero())),
                    best_bid_before.unwrap_or((Decimal::zero(), Decimal::zero())),
                    &taker_base_before,
                    &taker_quote_before,
                    &cr,
                    &mr,
                );
            }
            // alice ask p=5, a=7.6
            {
                let size = data.orderbooks.get(&(0, 1)).unwrap().size();
                let cmd2 = LimitCmd {
                    symbol: (0, 1),
                    user_id: UserId::from_low_u64_be(1),
                    order_id: 2,
                    price: dec!(5),
                    amount: dec!(7.6),
                    ask_or_bid: AskOrBid::Ask,
                    nonce: 1,
                    signature: vec![0],
                };
                let (best_ask_before, best_bid_before) =
                    data.orderbooks.get(&(0, 1)).unwrap().get_size_of_best();
                let taker_base_before =
                    assets::get_balance_to_owned(&data.accounts, &cmd2.user_id, cmd2.symbol.0);
                let taker_quote_before =
                    assets::get_balance_to_owned(&data.accounts, &cmd2.user_id, cmd2.symbol.1);
                let (c, val) =
                    assets::freeze_if(&cmd2.symbol, cmd2.ask_or_bid, cmd2.price, cmd2.amount);
                assets::try_freeze(&mut data.accounts, &cmd2.user_id, c, val).unwrap();
                let mr = matcher::execute_limit(
                    data.orderbooks.get_mut(&(0, 1)).unwrap(),
                    cmd2.user_id,
                    cmd2.order_id,
                    cmd2.price,
                    cmd2.amount,
                    cmd2.ask_or_bid,
                );
                let cr = clearing::clear(&mut data.accounts, 4, &(0, 1), tf, mf, &mr, 0);
                pp.prove_trade_cmd(
                    &mut data,
                    cmd2.nonce,
                    cmd2.signature.clone(),
                    (cmd2, mf, tf).into(),
                    size.0,
                    size.1,
                    best_ask_before.unwrap_or((Decimal::zero(), Decimal::zero())),
                    best_bid_before.unwrap_or((Decimal::zero(), Decimal::zero())),
                    &taker_base_before,
                    &taker_quote_before,
                    &cr,
                    &mr,
                );
            }
            // alice cancel 2
            {
                let size = data.orderbooks.get(&(0, 1)).unwrap().size();
                let cmd2 = CancelCmd {
                    symbol: (0, 1),
                    user_id: UserId::from_low_u64_be(1),
                    order_id: 2,
                    nonce: 1,
                    signature: vec![0],
                };
                let (best_ask_before, best_bid_before) =
                    data.orderbooks.get(&(0, 1)).unwrap().get_size_of_best();
                let taker_base_before =
                    assets::get_balance_to_owned(&data.accounts, &cmd2.user_id, cmd2.symbol.0);
                let taker_quote_before =
                    assets::get_balance_to_owned(&data.accounts, &cmd2.user_id, cmd2.symbol.1);
                let mr = matcher::cancel(data.orderbooks.get_mut(&(0, 1)).unwrap(), cmd2.order_id)
                    .unwrap();
                let cr = clearing::clear(&mut data.accounts, 5, &(0, 1), tf, mf, &mr, 0);
                pp.prove_trade_cmd(
                    &mut data,
                    cmd2.nonce,
                    cmd2.signature.clone(),
                    cmd2.into(),
                    size.0,
                    size.1,
                    best_ask_before.unwrap_or((Decimal::zero(), Decimal::zero())),
                    best_bid_before.unwrap_or((Decimal::zero(), Decimal::zero())),
                    &taker_base_before,
                    &taker_quote_before,
                    &cr,
                    &mr,
                );
            }
        });
        // ignore transfer in
        rx.recv().unwrap();
        rx.recv().unwrap();
        /*
         * ignore ask
         * 1. p=10, a=1.1
         * 2. p=5, a=7.6
         */
        rx.recv().unwrap();
        rx.recv().unwrap();
        // cancel p=5, a=7.6
        {
            let proof = rx.recv().unwrap();
            let (base, quote) = (0, 1);
            let leaves = proof.leaves;
            let account = UserId::from_low_u64_be(1);
            assert!(leaves.len() == 5,);
            let (b, q) = leaves[0].try_get_symbol().unwrap();
            assert!(b == base && q == quote,);
            let (ask0, bid0) = leaves[0].split_old_to_u128();
            let (ask1, bid1) = leaves[0].split_new_to_u128();
            let ask_delta = ask0 - ask1;
            let bid_delta = bid0 - bid1;
            assert!(ask_delta + bid_delta != 0,);
            assert!(ask_delta & bid_delta == 0,);

            let (b, id) = leaves[1].try_get_account().unwrap();
            assert!(b == base,);
            assert!(<crate::core::B256 as AsRef<[u8; 32]>>::as_ref(&account) == &id);
            let (ba0, bf0) = leaves[1].split_old_to_u128();
            let (ba1, bf1) = leaves[1].split_new_to_u128();
            assert!(ba0 + bf0 == ba1 + bf1,);

            let (q, id) = leaves[2].try_get_account().unwrap();
            assert!(q == quote,);
            assert!(<crate::core::B256 as AsRef<[u8; 32]>>::as_ref(&account) == &id);
            let (qa0, qf0) = leaves[2].split_old_to_u128();
            let (qa1, qf1) = leaves[2].split_new_to_u128();
            assert!(qa0 + qf0 == qa1 + qf1,);

            let (best_ask0, best_bid0) = leaves[3].split_old_to_u128();
            let (b, q, cancel_at) = leaves[4].try_get_orderpage().unwrap();
            assert!(b == base && q == quote && (cancel_at >= best_ask0 || cancel_at <= best_bid0),);
            let before_cancel = leaves[4].split_old_to_sum();
            let after_cancel = leaves[4].split_new_to_sum();
            if cancel_at >= best_ask0 {
                assert!(ask_delta == before_cancel - after_cancel,);
            } else {
                assert!(bid_delta == before_cancel - after_cancel,);
            }
        }
    }

    fn cv<T, const N: usize>(v: Vec<T>) -> [T; N] {
        v.try_into().unwrap_or_else(|v: Vec<T>| {
            panic!("Expected a Vec of length {} but it was {}", N, v.len())
        })
    }

    #[test]
    pub fn test_take_best_only() {
        let ml = MerkleLeaf {
            key: hex::decode("0300000000010000000000e8890423c78a0000000000000000").unwrap(),
            old_v: cv(hex::decode(
                "00000000000000000000000000000000000042f02abf8d0a5000000000000000",
            )
            .unwrap()),
            new_v: cv(hex::decode(
                "000000000000000000000000000000000000d2b40fede2c94c00000000000000",
            )
            .unwrap()),
        };
        let (b, q, p) = ml.try_get_orderpage().unwrap();
        assert_eq!(
            super::to_decimal_represent(ml.split_old_to_sum()).unwrap(),
            dec!(1476.5)
        );
        assert_eq!(
            super::to_decimal_represent(ml.split_new_to_sum()).unwrap(),
            dec!(1416.5)
        );
    }

    #[test]
    pub fn test_take_whole_page_then_quit() {
        let taker_base = MerkleLeaf {
            key: hex::decode(
                "00a0c2d5a09b2924eb27168d3a7f98779d65c73b7fd1f77c1cb7e21ed138461e3900000000",
            )
            .unwrap(),
            old_v: cv(hex::decode(
                "0038dec81916000000000000000000000000c7e0541072860400000000000000",
            )
            .unwrap()),
            new_v: cv(hex::decode(
                "00603c426bc1985a01000000000000000000c7e0541072860400000000000000",
            )
            .unwrap()),
        };
        let maker_base = MerkleLeaf {
            key: hex::decode(
                "0016f6d1868f4ab0e070c4d7938c1bd552425804c6784a1ace659e162d44bdc56900000000",
            )
            .unwrap(),
            old_v: cv(hex::decode(
                "0000000000000000000000000000000000c04948987cf15a0100000000000000",
            )
            .unwrap()),
            new_v: cv(hex::decode(
                "0000000000000000000000000000000000000000000000000000000000000000",
            )
            .unwrap()),
        };
        let page = MerkleLeaf {
            key: hex::decode("0300000000010000000000470ea1b0f8000000000000000000").unwrap(),
            old_v: cv(hex::decode(
                "0000000000000000000000000000000000c04948987cf15a0100000000000000",
            )
            .unwrap()),
            new_v: cv(hex::decode(
                "0000000000000000000000000000000000000000000000000000000000000000",
            )
            .unwrap()),
        };
        let best_price = MerkleLeaf {
            key: hex::decode("020000000001000000").unwrap(),
            old_v: cv(hex::decode(
                "0000470ea1b0f800000000000000000000009108c73695000000000000000000",
            )
            .unwrap()),
            new_v: cv(hex::decode(
                "008027461a740a01000000000000000000009108c73695000000000000000000",
            )
            .unwrap()),
        };
        let taker_fee = 1000;
        let mb_delta = maker_base.split_old_to_sum() - maker_base.split_new_to_sum();
        let base_charged = mb_delta / taker_fee;
        let tb_delta = taker_base.split_new_to_sum() - taker_base.split_old_to_sum();
        let prv_is_maker = page.split_old_to_sum();
        let now_is_maker = page.split_new_to_sum();
        assert!(tb_delta + base_charged == prv_is_maker - now_is_maker);
    }

    impl MerkleLeaf {
        fn from_hex(key: &str, old_v: &str, new_v: &str) -> Self {
            Self {
                key: hex::decode(key).unwrap(),
                old_v: cv(hex::decode(old_v).unwrap()),
                new_v: cv(hex::decode(new_v).unwrap()),
            }
        }
    }

    #[test]
    pub fn merkle_verify_should_work() {
        let leaves = vec![
            MerkleLeaf::from_hex(
                "010000000001000000",
                "0080efab051761049c02000000000000008060b77606ab2f5b0f000000000000",
                "00c0a5636d9a6fa99a02000000000000008060b77606ab2f5b0f000000000000",
            ),
            MerkleLeaf::from_hex(
                "0016f6d1868f4ab0e070c4d7938c1bd552425804c6784a1ace659e162d44bdc56900000000",
                "0000000000000000000000000000000000c04948987cf15a0100000000000000",
                "0000000000000000000000000000000000000000000000000000000000000000",
            ),
            MerkleLeaf::from_hex(
                "0016f6d1868f4ab0e070c4d7938c1bd552425804c6784a1ace659e162d44bdc56901000000",
                "0060cdab71d92e1600000000000000000040beba547f712e0000000000000000",
                "0016194132db712e00000000000000000040beba547f712e0000000000000000",
            ),
            MerkleLeaf::from_hex(
                "00a0c2d5a09b2924eb27168d3a7f98779d65c73b7fd1f77c1cb7e21ed138461e3900000000",
                "0038dec81916000000000000000000000000c7e0541072860400000000000000",
                "00603c426bc1985a01000000000000000000c7e0541072860400000000000000",
            ),
            MerkleLeaf::from_hex(
                "00a0c2d5a09b2924eb27168d3a7f98779d65c73b7fd1f77c1cb7e21ed138461e3901000000",
                "001787d6fc288b43000000000000000000000000000000000000000000000000",
                "00871b42a0ef412b000000000000000000000000000000000000000000000000",
            ),
            MerkleLeaf::from_hex(
                "020000000001000000",
                "0000470ea1b0f800000000000000000000009108c73695000000000000000000",
                "008027461a740a01000000000000000000009108c73695000000000000000000",
            ),
            MerkleLeaf::from_hex(
                "0300000000010000000000470ea1b0f8000000000000000000",
                "0000000000000000000000000000000000c04948987cf15a0100000000000000",
                "0000000000000000000000000000000000000000000000000000000000000000",
            ),
        ];
        let mp = CompiledMerkleProof(
            hex::decode(
                "4c4fef51efba86b6937a1469334e21ac832fab5e731d9e744f3ceb4d60c7fa011ea0e1eca89470b5f9c6b96db6385c8c37237b3a58dea77a87e1f6a67820c5d401ae6d00004f0451f4ae186d9a05c17ee5f724f30e3ae98fd9ea5c2445157977e51cf35b3df2341cf11d9558312b537fe0d2c94d0f46c9e575cd4dc1ab54b952ee560bc21d542508005061a14d393a58f8749891558a4a604abe58ac66803ca0b84b5fcf43b64932cca84f015013d9b73a5ddf0de0a65186e37d66e48e8591416ee11cd6e87270eae36e4f09c950aebc2675cda42bfe3077a7dc5ca4749c7ee996add7c9ffda91980837e562119550b463c168546ae68ffd001fbc93866fe746213a1222d5ac026617e711a7c247e7507ca0d7fdb729a5706aec0f4db86cfe4ca1acc022992ca388daa2bed0c69ec12a50e528a4b614cfba56d4d890249eed3eee0666e581a50cb6415beebc0d9cf4c82850e68fecb62daa397d9f1d6027b60301761fa3a45e7ae9eea1b47ef6af432dbbef50e89d7ae815374608074f46c77254b3dd0d6e5ced33dbe1720b268094ac179bb64c4ff451f436e5ee891b625f2923db8e297a7b0aa8e3587eaea65a04623173720f200c6a744d34087485fc096e25ee0dcfbaa42ff08483d28d1f889112ab7f0eab4b6f08004f0151f6dbaa04a44bb2bfdffccc8144a5797cdeea67660ec89be12ef35e2d0bf19a792b528bcd89340247b549cd3b4c02e2b652d63b3963c472eb48b53a1ce77e8c200051027ac4835a2f9cd13d2f3f9ebc5f85ba876e1c269cfd20ab75f0082cb75f11ed3a000000000000000000000000000000000000000000000000000000000000000050380d38de546fe8d8902358b9ed8183c5d9c2c8d8c31ba15568d4d8cc459bec5050fe98468806407b2fb6f1d007a5476ff96dcda33a4a7d0bd6afc7d3bcd6f2d7c650cc82539a9fb57ca560cb288b9024d183ad5adfeb2a3964c3e0e1ccd4a5635154507b9369b0459ddfacfe18b3507a1bd084f9165c3e81da47b38578b1131a2c5b1750549f493e2860dd60b22b95ea7c4778f882de548745b849a3c3befc985084bb324c4ff451f4212c9bd349391a0144eee1c8ea2ee9489932fd1c8b4b8184482e3c42d19d0e9451d050fe13eb6899e2800a8578419c52598a650305453f37429de9af2b4d00004f01503676428417132817201e732f1a96cd590512cb64fb04428266116e020ce4638f50e844cdfe7b66a06d59e5bcaf1dea26441092b767f0c027e91eef290977f723a1510195e00a12ec35ad37d6e724390ab0853982f04bacbf725346ae78765f80315c60000000000000000000000000000000000000000000000000000000000000000050799b1503e67eabb7def82e990783d1fd4635cf53350be185e6b3ec4e2b6e7fcc5015eaeca48ef3a7075c9c2345a3b012bf55efb70596def74c29db1843292bf596508e581b55c6ccf9ad632039b7394a9f87a010265754d38e9865293ae81b0aeae350437b09251327a46fec149055e793bbcd7a93730b891313fd700391cc2c6fb5d548484c4ff351f30dd148eaaedd2c5ae2ff81a8cdd71e6a17b30eb1d8dfabf3e939b5281b64d1d87c0ecc99a8947ce99bbbd17d85b70c4b99893491ed295a7bddc97292ed02070050a46c50963b2b53d9f15882e003000a3e815464f374e40685d023a63db4b6fdd04f0151f638e5b86696292a464a86a82d34ff7ca3a65f7e1d440eb95bbb01918c8f55791a16415e5822820e865f64f3065b3d8bb2fd509538bdf6b8076c3547abd7fa04005102f4c4f742b88807edec314d74574fcb134a025b6b2e9bb34add6eaf7f6f938b610000000000000000000000000000000000000000000000000000000000004000501670d273c906783c61e2b11e2c5a16468610cc2675ca1251f466d923439f37d850a9b540b9c386d72203534b7af0571ddca4d8f5714631c084d1eb559f0692394e50b071450fa4e3e04aa8c889c0a6d4d4e634645a7587407919a5f24f014b29c20c50b389181fc05b95eb39e4a5fb15f870173206b90d9d0cedb8969dd46eefc3c6b74c4ff6506dbee8fcf763c716e486e35f82d1947fd58f7a51076f5468d16deaf8a6026811504b21a7e46057d0aa6a9bd9f92dfdafa2e746a13a92a1315d45a478998254a94c5026edaf50a4527a3456fc9c198ceae6aee9d6bcfc91be5e66a012ca74179c782c5005ff597fb9a556280d039290741cc425d190d8893fb6cae4d57c456b1c46e7f0507059de345e9653be65bfe63e6774aca5c7c0fa03d149d13ce0dfd37c8acfb70c508c4a6b5395cfea9e7e80f472fbbada60f745d9996e809150c6a4317a37b588f34850a801d8e4b6ab0462fc5c49e5af39dfe8acaffa9661bbd923bbc387fdfd382c414c4ff251f2e5f155995a0d9153d51e2c425d53f1bc67ff1ca54867122cae9d8bcb2c979b78655ad3e45567377d5f4a4bd99478e3849b1c90b9dfe3191c050b5951945503004f0251f56eba5e1afba6dea072bf4530ae47f2ab0c587e3f4edc1e26908ed54a32573b278d9edb4d5733815d91603d062dbbcddcf0d91d1dc09216fcd318397ef214130050bbad095a5ac0e0301deb94a1673e4225c44e0ef777223fd21b95585baf6312165020ab7590665078b7a000bebd89441b61fddd115bf63fd3de97254ffc5127b03050b419a8bdcc7fd2a49c19d9f849b48399a4fe66c4db119b9893973f8978c304555039d1e341b36ce61c724ea49941b5a3fbef1b185c46a5d19f5b8f4708557b72aa50c9a2c2c674ce529fbc691cf52b34afe2ac849c11403935fdb615878f5383c0af500b6bbc065a5f422ba83addaa471225600ce8b9e58f735295c20ec86cfa35b3c94c4ff750cfe2eb886dcc8c7cd7bcf4e810983e6658d69bcdec78310106b329d8b60663ba502be8d680b416ad5aa8a7f0c8c59f3a9adfdffc4e19f4e71374724b54ee06b7cd50fd2ea74fa7c2268987ca87286d2d8e4161afc1f9e55564ebf94e13b0f2764b6050ab116995d5d658127f4ba1762c0d3380f1ac1c47bb59e13ff9a27d114fc0f5955098def021cdb40c2ea4f0375b8b5af0ee6573bdb73fb58adde0728aa25f416d8e485072c5ed23861e76d08b1c48600bb5c9df88801d9e69189c9898efd08511280f7c4848",
            )
            .unwrap(),
        );
        let (old, new): (Vec<_>, Vec<_>) = leaves
            .iter()
            .map(|v| {
                let key = BlakeTwo256::digest(&v.key).into();
                ((key, v.old_v.into()), (key, v.new_v.into()))
            })
            .unzip();
        let known_root: [u8; 32] = cv(hex::decode(
            "328305a5f766959084694f439196b41d3731e4532ef9ad5fb5327868af154f4e",
        )
        .unwrap());
        let r = mp
            .verify::<smt::blake2b::Blake2bHasher>(&known_root.into(), old)
            .unwrap();
        assert!(r);
        let new_root: [u8; 32] = cv(hex::decode(
            "f545a8eabcd8fe7ee07c7c3100b896d40b83f07dc66198a3280354d48f602bdb",
        )
        .unwrap());
        let r = mp
            .verify::<smt::blake2b::Blake2bHasher>(&new_root.into(), new)
            .unwrap();
        assert!(r);
    }

    #[test]
    pub fn test_conditionally_canceled_with_single_maker() {
        let leaves = vec![
            MerkleLeaf::from_hex(
                "010000000001000000",
                "00c034bd2ec51119aa0200000000000000007b6bb331c0fc880c000000000000",
                "00809fa295efd31ea00200000000000000007b6bb331c0fc880c000000000000",
            ),
            MerkleLeaf::from_hex(
                "00c2b3f03b624590e3faa9e510b9e02828e1323282e9ce2a332d4ffae3541ae33000000000",
                "006011483cbb04bb02000000000000000000383c5699c74c0a00000000000000",
                "006011483cbb04bb02000000000000000000b08a3feae3960700000000000000",
            ),
            MerkleLeaf::from_hex(
                "00c2b3f03b624590e3faa9e510b9e02828e1323282e9ce2a332d4ffae3541ae33001000000",
                "009c1a984b470000000000000000000000000000000000000000000000000000",
                "009ccba7893e2f53000000000000000000000000000000000000000000000000",
            ),
            MerkleLeaf::from_hex(
                "00920ad03fcf8cc96d6c2a559998a976201ac2d5f166eeb491e943ffab20e7f65c00000000",
                "0000000000000000000000000000000000c0e59075e89e2f0600000000000000",
                "0000000000000000000000000000000000000000000000000000000000000000",
            ),
            MerkleLeaf::from_hex(
                "00920ad03fcf8cc96d6c2a559998a976201ac2d5f166eeb491e943ffab20e7f65c01000000",
                "004875f4569d4135010000000000000000000000000000000000000000000000",
                "0000265e74cb19f3010000000000000000000000000000000000000000000000",
            ),
            MerkleLeaf::from_hex(
                "004a33a682a770ce905478806f7f16bdfd232a723d30f36b2240274d493dc8870400000000",
                "0080a02ff09586bd0300000000000000000040763a6b0bde0000000000000000",
                "0080a02ff09586bd030000000000000000809b4302a54c7a0000000000000000",
            ),
            MerkleLeaf::from_hex(
                "004a33a682a770ce905478806f7f16bdfd232a723d30f36b2240274d493dc8870401000000",
                "00c83dc5bd572c0c00000000000000000000187abaa654030000000000000000",
                "00586a519473211800000000000000000000187abaa654030000000000000000",
            ),
            MerkleLeaf::from_hex(
                "0026e6ae092d50d58b5705ebd721fda9dcfb7c8d6670c3c65b4dde62f23889843b00000000",
                "00000000000000000000000000000000000088b116afe3b50200000000000000",
                "000000000000000000000000000000000000a027128c1c2b0200000000000000",
            ),
            MerkleLeaf::from_hex(
                "0026e6ae092d50d58b5705ebd721fda9dcfb7c8d6670c3c65b4dde62f23889843b01000000",
                "004041df2370052e00000000000000000000b2d3595bf0060000000000000000",
                "0040fe7b636ea83e00000000000000000000b2d3595bf0060000000000000000",
            ),
            MerkleLeaf::from_hex(
                "003480a04c1318d0e6c0637f7e25a520f3c89aa5cbe4744898a9aa0836cde2f45900000000",
                "0008f8ce9049255e00000000000000000080aa8677c8d0880000000000000000",
                "0008f8ce9049255e000000000000000000800f6ba7739b620000000000000000",
            ),
            MerkleLeaf::from_hex(
                "003480a04c1318d0e6c0637f7e25a520f3c89aa5cbe4744898a9aa0836cde2f45901000000",
                "805ad44c45320915000000000000000000eca75ac794a8340000000000000000",
                "80ba572749c89d19000000000000000000eca75ac794a8340000000000000000",
            ),
            MerkleLeaf::from_hex(
                "00927df3f7ca107244f47017b13e26bfeff6877b83797479b1ab38b7a509e1f14000000000",
                "00a879222f050000000000000000000000000000000000000000000000000000",
                "0020893202f8aff7090000000000000000000000000000000000000000000000",
            ),
            MerkleLeaf::from_hex(
                "00927df3f7ca107244f47017b13e26bfeff6877b83797479b1ab38b7a509e1f14001000000",
                "806423e6e35e8232010000000000000000000000000000000000000000000000",
                "80a46df70e120000000000000000000000000000000000000000000000000000",
            ),
            MerkleLeaf::from_hex(
                "020000000001000000",
                "00000c3d5d53aa01000000000000000000400dedbf8592010000000000000000",
                "00000c3d5d53aa01000000000000000000400dedbf8592010000000000000000",
            ),
            MerkleLeaf::from_hex(
                "03000000000100000000000c3d5d53aa010000000000000000",
                "0000000000000000000000000000000000c03c1919e9262f0a00000000000000",
                "000000000000000000000000000000000080a7fe7f13e9340000000000000000",
            ),
        ];
        let base = 0u32;
        let quote = 1u32;
        let price = 120000000000000000u128;
        let amount = 184052500000000000000u128;
        let pages = 1;
        let maker_accounts = 10;
        let leaves_count = (4u8 + maker_accounts + pages) as usize;
        assert!(leaves.len() == leaves_count,);
        assert!(maker_accounts % 2 == 0,);
        let (ask0, bid0) = leaves[0].split_old_to_u128();
        let (ask1, bid1) = leaves[0].split_new_to_u128();
        let ask_delta = ask0 - ask1;
        let bid_delta = bid1 - bid0;

        let taker_base = &leaves[maker_accounts as usize + 1];
        let (tba0, tbf0) = taker_base.split_old_to_u128();
        let (tba1, tbf1) = taker_base.split_new_to_u128();
        let tb_delta = (tba1 + tbf1) - (tba0 + tbf0);
        let (bk, taker_b_id) = taker_base.try_get_account().unwrap();
        let taker_quote = &leaves[maker_accounts as usize + 2];
        let (tqa0, tqf0) = taker_quote.split_old_to_u128();
        let (tqa1, tqf1) = taker_quote.split_new_to_u128();
        let (qk, taker_q_id) = taker_quote.try_get_account().unwrap();
        let tq_delta = (tqa0 + tqf0) - (tqa1 + tqf1);
        assert!(bk == base && qk == quote,);
        assert!(taker_b_id == taker_q_id,);
        let mut mb_delta = 0u128;
        let mut mq_delta = 0u128;
        for i in 0..maker_accounts as usize / 2 {
            // base first
            let maker_base = &leaves[i * 2 + 1];
            let (bk, maker_b_id) = maker_base.try_get_account().unwrap();
            let mb0 = maker_base.split_old_to_sum();
            let mb1 = maker_base.split_new_to_sum();
            let base_decr = mb0 - mb1;
            mb_delta += base_decr;
            // then quote
            let maker_quote = &leaves[i * 2 + 2];
            let (qk, maker_q_id) = maker_quote.try_get_account().unwrap();
            assert!(quote == qk && base == bk,);
            assert!(maker_b_id == maker_q_id,);
            let mq0 = maker_quote.split_old_to_sum();
            let mq1 = maker_quote.split_new_to_sum();
            let quote_incr = mq1 - mq0;
            mq_delta += quote_incr;
        }
        let quote_charged = tq_delta / 1000;
        assert!(mq_delta + quote_charged == tq_delta,);
        let base_charged = mb_delta / 1000;
        assert!(tb_delta + base_charged == mb_delta,);
        assert!(ask_delta == mb_delta,);
        if bid_delta != 0 {
            assert!(bid_delta == amount - mb_delta,);
        }
        let best_price = &leaves[maker_accounts as usize + 3];
        let (b, q) = best_price.try_get_symbol().unwrap();
        assert!(b == base && q == quote,);
        let (best_ask0, best_bid0) = best_price.split_old_to_u128();
        let (best_ask1, best_bid1) = best_price.split_new_to_u128();

        if ask_delta != 0 {
            // trading happened
            assert!(pages > 0 && price >= best_ask0,);
            // best_ask0 <= page0 < page1 < .. < pagen <= best_ask1
            let mut pre_best = best_ask0;
            let mut taken_asks = 0u128;
            for i in 0..pages as usize - 1 {
                let page = &leaves[maker_accounts as usize + 4 + i];
                let (b, q, p) = page.try_get_orderpage().unwrap();
                assert!(b == base && q == quote,);
                assert!(pre_best <= p,);
                pre_best = p;
                assert!(page.split_new_to_sum() == 0,);
                taken_asks += page.split_old_to_sum();
            }
            if bid_delta != 0 {
                // partial_filled
                let taker_price_page = leaves.last().unwrap();
                let (b, q, p) = taker_price_page.try_get_orderpage().unwrap();
                assert!(b == base && q == quote && p == price,);
                assert!(best_bid1 == price,);
                let prv_is_maker = taker_price_page.split_old_to_sum();
                let now_is_taker = taker_price_page.split_new_to_sum();
                assert!(taken_asks + prv_is_maker + now_is_taker == amount,);
            } else {
                // filled or conditional_canceled
                let vanity_maker = leaves.last().unwrap();
                let (b, q, p) = vanity_maker.try_get_orderpage().unwrap();
                assert!(b == base && q == quote,);
                assert!(best_bid1 == best_bid0,);
                let prv_is_maker = vanity_maker.split_old_to_sum();
                let now_is_maker = vanity_maker.split_new_to_sum();
                assert!(tb_delta + base_charged == taken_asks + prv_is_maker - now_is_maker,);
            }
        } else {
            // no trading
            assert!(best_ask1 == best_ask0,);
            if bid_delta != 0 {
                // placed
                let taker_price_page = leaves.last().unwrap();
                let (b, q, p) = taker_price_page.try_get_orderpage().unwrap();
                assert!(b == base && q == quote && p == price,);
                let prv_is_maker = taker_price_page.split_old_to_sum();
                let now_is_maker = taker_price_page.split_new_to_sum();
                assert!(amount == now_is_maker - prv_is_maker,);
            }
        }
    }

    #[test]
    pub fn cancel_should_work() {
        let leaves = vec![
            MerkleLeaf::from_hex(
                "010000000001000000",
                "00000000000000000000000000000000000064a7b3b6e00d0000000000000000",
                "000000000000000000000000000000000000b2d3595bf0060000000000000000",
            ),
            MerkleLeaf::from_hex(
                "00f6938f092096ceddf7f5c15c94e9a50f375ba3b4b5653e50c00d1256bd7b052800000000",
                "0000fa86365c73563c0400000000000000000000000000000000000000000000",
                "0000fa86365c73563c0400000000000000000000000000000000000000000000",
            ),
            MerkleLeaf::from_hex(
                "00f6938f092096ceddf7f5c15c94e9a50f375ba3b4b5653e50c00d1256bd7b052801000000",
                "0000f716504203391c0200000000000000005e2c8cdd63890000000000000000",
                "0000262d1631b57d1c0200000000000000002f16c6eeb1440000000000000000",
            ),
            MerkleLeaf::from_hex(
                "020000000001000000",
                "0000000000000000000000000000000000005e2c8cdd63890000000000000000",
                "0000000000000000000000000000000000005e2c8cdd63890000000000000000",
            ),
            MerkleLeaf::from_hex(
                "03000000000100000000005e2c8cdd63890000000000000000",
                "00000000000000000000000000000000000064a7b3b6e00d0000000000000000",
                "000000000000000000000000000000000000b2d3595bf0060000000000000000",
            ),
        ];
        let base = 0u32;
        let quote = 1u32;
        assert!(leaves.len() == 5,);
        let (b, q) = leaves[0].try_get_symbol().unwrap();
        assert!(b == base && q == quote,);
        let (ask0, bid0) = leaves[0].split_old_to_u128();
        let (ask1, bid1) = leaves[0].split_new_to_u128();
        let ask_delta = ask0 - ask1;
        let bid_delta = bid0 - bid1;
        assert!(ask_delta + bid_delta != 0,);
        assert!(ask_delta & bid_delta == 0,);

        let (b, id) = leaves[1].try_get_account().unwrap();
        assert!(b == base,);
        let (ba0, bf0) = leaves[1].split_old_to_u128();
        let (ba1, bf1) = leaves[1].split_new_to_u128();
        assert!(ba0 + bf0 == ba1 + bf1,);

        let (q, id) = leaves[2].try_get_account().unwrap();
        assert!(q == quote,);
        let (qa0, qf0) = leaves[2].split_old_to_u128();
        let (qa1, qf1) = leaves[2].split_new_to_u128();
        assert!(qa0 + qf0 == qa1 + qf1,);

        let (best_ask0, best_bid0) = leaves[3].split_old_to_u128();
        let (b, q, cancel_at) = leaves[4].try_get_orderpage().unwrap();
        assert!(b == base && q == quote && (cancel_at >= best_ask0 || cancel_at <= best_bid0),);
        let before_cancel = leaves[4].split_old_to_sum();
        let after_cancel = leaves[4].split_new_to_sum();
        if cancel_at >= best_ask0 && best_ask0 != 0 {
            assert!(ask_delta == before_cancel - after_cancel,);
        } else {
            assert!(bid_delta == before_cancel - after_cancel,);
        }
    }
}
