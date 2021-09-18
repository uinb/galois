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

use crate::{config::C, core::*, event::*, sequence};
use anyhow::anyhow;
use fixed::{types::extra::U64, FixedU128};
use memmap::MmapMut;
use parity_scale_codec::{Compact, Decode, Encode, WrapperTypeEncode};
use rust_decimal::{prelude::*, Decimal};
use serde::{Deserialize, Serialize};
use smt::{default_store::DefaultStore, sha256::Sha256Hasher, SparseMerkleTree, H256};
use sp_core::sr25519::{Pair as Sr25519, Public};
use sp_runtime::{
    generic::{Block, Header},
    traits::BlakeTwo256,
    MultiAddress, OpaqueExtrinsic,
};
use std::{
    convert::{TryFrom, TryInto},
    fs::OpenOptions,
    path::PathBuf,
    sync::mpsc::Receiver,
    time::Duration,
};
use sub_api::{
    rpc::{
        ws_client::{EventsDecoder, RuntimeEvent},
        WsRpcClient,
    },
    Api, FromHexString, Hash, SignedBlock, UncheckedExtrinsicV4, XtStatus,
};

pub type GlobalStates = SparseMerkleTree<Sha256Hasher, H256, DefaultStore<H256>>;
pub type FusoAccountId = Public;
pub type FusoAddress = MultiAddress<FusoAccountId, ()>;
pub type FusoHeader = Header<u32, BlakeTwo256>;
pub type FusoBlock = Block<FusoHeader, OpaqueExtrinsic>;

const ONE_ONCHAIN: u128 = 1_000_000_000_000_000_000;

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
    pub nonce: u32,
    pub signature: Vec<u8>,
    pub cmd: FusoCommand,
    pub leaves: Vec<MerkleLeaf>,
    pub proof_of_exists: Vec<u8>,
    pub proof_of_cmd: Vec<u8>,
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
pub struct TokenHostedEvent {
    fund_owner: FusoAccountId,
    dominator: FusoAccountId,
    token_id: u32,
    amount: u128,
}

impl WrapperTypeEncode for UserId {}

/// AccountId of chain = MultiAddress<sp_runtime::AccountId32, ()>::Id = GenericAddress::Id
/// 1. from_ss58check() or from_ss58check_with_version()
/// 2. new or from public
pub fn init(rx: Receiver<Proof>) -> anyhow::Result<()> {
    use sp_core::Pair;
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
    let dominator = signer.public();
    let api = Api::new(client)
        .map(|api| api.set_signer(signer))
        .map_err(|_| anyhow!("Fusotao node not available"))?;
    let path: PathBuf = [&C.sequence.coredump_dir, "fusotao.seq"].iter().collect();
    let finalized_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)?;
    finalized_file.set_len(8)?;
    let mut seq = unsafe { MmapMut::map_mut(&finalized_file)? };
    let mut cur = u64::from_le_bytes(seq.as_ref().try_into()?);
    log::info!("initiate proving at sequence {:?}", cur);
    let wapi = api.clone();
    std::thread::spawn(move || loop {
        let proof = rx.recv().unwrap();
        if cur >= proof.event_id {
            continue;
        }
        cur = proof.event_id;
        log::debug!("proofs of sequence {:?}: {:?}", cur, proof);
        let xt: UncheckedExtrinsicV4<_> =
            sub_api::compose_extrinsic!(wapi.clone(), "Receipts", "verify", proof);
        // FIXME handle network error?
        wapi.send_extrinsic(xt.hex_encode(), XtStatus::InBlock)
            .unwrap();
        // FIXME only update when finalized
        seq.copy_from_slice(&cur.to_le_bytes()[..]);
        log::info!("proofs of sequence {:?} has been uploaded", cur);
    });
    let path: PathBuf = [&C.sequence.coredump_dir, "fusotao.blk"].iter().collect();
    let finalized_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)?;
    finalized_file.set_len(32)?;
    let mut blk = unsafe { MmapMut::map_mut(&finalized_file)? };
    if blk.iter().fold(0u8, |x, a| x | a) == 0 {
        let from = Hash::from_hex(C.fusotao.as_ref().unwrap().claim_block.clone()).unwrap();
        blk.copy_from_slice(&from[..]);
        blk.flush().unwrap();
    }
    let decoder = EventsDecoder::try_from(api.metadata.clone()).unwrap();
    std::thread::spawn(move || loop {
        let current = Hash::from_slice(blk.as_ref());
        log::debug!("latest synchronized block: {:?}", current);
        if let Ok((cmds, hash)) = sync_finalized_blocks(current, &api, &dominator, &decoder) {
            log::debug!("prepare handle events {:?} before block {:?}", cmds, hash);
            match sequence::insert_sequences(&cmds) {
                Ok(()) => {
                    blk[..].copy_from_slice(&hash[..]);
                    // FIXME commit manually after memmap flush ok
                    blk.flush().unwrap();
                    if !cmds.is_empty() {
                        log::info!("all events before finalized block {:?} handled", hash);
                    }
                }
                Err(_) => log::warn!(
                    "write sequences from fusotao failed({:?} ~ {:?}), retry",
                    current,
                    hash
                ),
            }
        }
        std::thread::sleep(Duration::from_millis(3000));
    });
    log::info!("fusotao prover initialized");
    Ok(())
}

fn sync_finalized_blocks(
    current: Hash,
    api: &Api<Sr25519, WsRpcClient>,
    signer: &Public,
    decoder: &EventsDecoder,
) -> anyhow::Result<(Vec<sequence::Command>, Hash)> {
    let mut finalized = api.get_finalized_head()?.unwrap();
    let recent = finalized;
    let mut cmds = vec![];
    while finalized != current {
        let latest: SignedBlock<FusoBlock> =
            api.get_signed_block(Some(finalized)).unwrap().unwrap();
        let e = api
            .get_opaque_storage_by_key_hash(
                sub_api::utils::storage_key("System", "Events"),
                Some(finalized),
            )?
            .unwrap();
        let raw_events = decoder.decode_events(&mut e.as_slice()).map_err(|e| {
            log::error!("{:?}", e);
            anyhow::anyhow!("decode events error")
        })?;
        for (_, event) in raw_events.into_iter() {
            match event {
                RuntimeEvent::Raw(raw) if raw.module == "Receipts" => match raw.variant.as_ref() {
                    "CoinHosted" => {
                        let decoded = CoinHostedEvent::decode(&mut &raw.data[..]).unwrap();
                        if &decoded.dominator == signer {
                            let mut cmd = sequence::Command::default();
                            cmd.cmd = sequence::TRANSFER_IN;
                            cmd.currency = Some(0);
                            cmd.amount = Some(to_decimal_represent(decoded.amount));
                            cmd.user_id = Some(format!("{}", decoded.fund_owner));
                            cmd.nonce = Some(0);
                            cmd.signature = Some(hex::encode(finalized));
                            cmds.push(cmd);
                        }
                    }
                    "TokenHosted" => {
                        let decoded = TokenHostedEvent::decode(&mut &raw.data[..]).unwrap();
                        if &decoded.dominator == signer {
                            let mut cmd = sequence::Command::default();
                            cmd.cmd = sequence::TRANSFER_IN;
                            cmd.currency = Some(decoded.token_id);
                            cmd.amount = Some(to_decimal_represent(decoded.amount));
                            cmd.user_id = Some(format!("{}", decoded.fund_owner));
                            cmd.nonce = Some(0);
                            cmd.signature = Some(hex::encode(finalized));
                            cmds.push(cmd);
                        }
                    }
                    _ => {}
                },
                _ => {}
            }
        }
        finalized = latest.block.header.parent_hash;
    }
    cmds.reverse();
    Ok((cmds, recent))
}

type CU128 = Compact<u128>;
type CU32 = Compact<u32>;

#[derive(Clone, Encode, Decode, Eq, PartialEq, Debug)]
pub enum FusoCommand {
    // price, amount, maker_fee, taker_fee, base, quote
    AskLimit(CU128, CU128, CU128, CU128, CU32, CU32),
    BidLimit(CU128, CU128, CU128, CU128, CU32, CU32),
    Cancel(CU32, CU32),
    TransferOut(CU32, CU128),
    TransferIn(CU32, CU128),
}

impl Into<FusoCommand> for (LimitCmd, Fee, Fee) {
    fn into(self) -> FusoCommand {
        match self.0.ask_or_bid {
            AskOrBid::Ask => FusoCommand::AskLimit(
                to_merkle_represent(self.0.price).into(),
                to_merkle_represent(self.0.amount).into(),
                to_merkle_represent(self.1).into(),
                to_merkle_represent(self.2).into(),
                self.0.symbol.0.into(),
                self.0.symbol.1.into(),
            ),
            AskOrBid::Bid => FusoCommand::BidLimit(
                to_merkle_represent(self.0.price).into(),
                to_merkle_represent(self.0.amount).into(),
                to_merkle_represent(self.1).into(),
                to_merkle_represent(self.2).into(),
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
            InOrOut::In => FusoCommand::TransferIn(
                self.currency.into(),
                to_merkle_represent(self.amount).into(),
            ),
            InOrOut::Out => FusoCommand::TransferOut(
                self.currency.into(),
                to_merkle_represent(self.amount).into(),
            ),
        }
    }
}

fn d18() -> Amount {
    ONE_ONCHAIN.into()
}

// FIXME
fn to_decimal_represent(v: u128) -> Decimal {
    if v.trailing_zeros() >= 18 {
        Decimal::new((v / ONE_ONCHAIN).try_into().unwrap(), 0)
    } else {
        let d: Amount = v.try_into().unwrap();
        d / d18()
    }
}

// FIXME using FIXED
fn to_merkle_represent(v: Decimal) -> u128 {
    println!("{:?}", v.trunc());
    let r = v.trunc().to_u128().unwrap();
    let mut f = v.fract() * d18();
    f.rescale(18);
    r * ONE_ONCHAIN + f.to_u128().unwrap()
}

fn u128le_to_h256(a0: u128, a1: u128) -> [u8; 32] {
    let mut v: [u8; 32] = Default::default();
    v[..16].copy_from_slice(&a0.to_le_bytes());
    v[16..].copy_from_slice(&a1.to_le_bytes());
    v
}

#[cfg(test)]
pub mod test {
    use super::*;

    #[test]
    pub fn test_decimal() {
        let v = Decimal::new(10000, 2);
        assert_eq!(to_merkle_represent(v), ONE_ONCHAIN * 100);
        let v = Decimal::new(995, 1);
        assert_eq!(to_merkle_represent(v), ONE_ONCHAIN * 995 / 10);
        assert!(Decimal::MAX.to_u128().is_some());
        assert!(Decimal::new(1, 10).to_u128().is_some());
    }
}
