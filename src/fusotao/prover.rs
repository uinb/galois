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
                cmd: cmd.into(),
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
        let old_root = merkle_tree.root().clone();
        let merkle_proof = gen_proofs(merkle_tree, &leaves);
        if &old_root != merkle_tree.root() {
            self.sender
                .send(Proof {
                    event_id,
                    user_id: cmd.user_id,
                    cmd: cmd.into(),
                    leaves,
                    maker_page_delta: 0,
                    maker_account_delta: 0,
                    merkle_proof,
                    root: merkle_tree.root().clone().into(),
                })
                .unwrap();
        }
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
            let mut merkle_tree = GlobalStates::default();
            let pp = Prover::new(tx, Arc::new(AtomicU64::new(0)));
            let mut all = Accounts::new();
            let orderbook = construct_pair();
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
                currency: 0,
                amount: dec!(99.99),
                block_number: 1,
                extrinsic_hash: vec![0],
            };
            let transfer_again =
                assets::add_to_available(&mut all, &cmd1.user_id, cmd1.currency, cmd1.amount)
                    .unwrap();
            pp.prove_assets_cmd(&mut merkle_tree, 1, cmd1, &after, &transfer_again);

            let mut orderbooks = std::collections::HashMap::new();
            let (mf, tf) = (orderbook.maker_fee, orderbook.taker_fee);
            orderbooks.insert((1, 0), orderbook);
            let mut data = Data {
                orderbooks,
                accounts: all,
                merkle_tree,
                current_event_id: 0,
            };

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
}
