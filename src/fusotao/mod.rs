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
use memmap::MmapMut;
use parity_scale_codec::{Decode, Encode, WrapperTypeEncode};
use rust_decimal::{prelude::*, Decimal};
use serde::{Deserialize, Serialize};
use smt::{default_store::DefaultStore, sha256::Sha256Hasher, SparseMerkleTree, H256};
use sp_core::{
    crypto::{Pair, Ss58Codec},
    sr25519::{Pair as Sr25519, Public},
};
use sp_runtime::{
    generic::{Block, CheckedExtrinsic, Header, UncheckedExtrinsic},
    traits::BlakeTwo256,
    MultiAddress, OpaqueExtrinsic,
};
use std::{
    convert::{TryFrom, TryInto},
    fs::OpenOptions,
    path::PathBuf,
    sync::mpsc::Receiver,
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
// pub type FusoCheckedExtrinsic = CheckedExtrinsic<FusoAccountId, Call, SignedExtra>;
// pub type FusoUncheckedExtrinsic = UncheckedExtrinsic<FusoAddress, Call, Signature, SignedExtra>;
pub type FusoHeader = Header<u32, BlakeTwo256>;
pub type FusoBlock = Block<FusoHeader, OpaqueExtrinsic>;

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
    let mut cur = u64::from_be_bytes(seq.as_ref().try_into()?);
    let wapi = api.clone();
    std::thread::spawn(move || loop {
        let proof = rx.recv().unwrap();
        if cur >= proof.event_id {
            continue;
        }
        cur = proof.event_id;
        // let xt: UncheckedExtrinsicV4<_> =
        //     compose_extrinsic!(wapi.clone(), "Receipts", "verify", proof);
        // // FIXME handle network error?
        // wapi.send_extrinsic(xt.hex_encode(), XtStatus::InBlock)
        //     .unwrap();
        // FIXME only update when finalized
        seq.copy_from_slice(&cur.to_be_bytes()[..]);
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
        if let Ok((cmds, hash)) = sync_finalized_blocks(current, &api, &dominator, &decoder) {
            match sequence::insert_sequences(cmds) {
                Ok(()) => {
                    blk.copy_from_slice(&hash[..]);
                    // FIXME commit manually after memmap flush ok
                    blk.flush().unwrap();
                }
                Err(_) => {
                    log::warn!("write sequences from fusotao failed, retry");
                }
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(3000));
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
        let raw_events = decoder
            .decode_events(&mut e.as_slice())
            .map_err(|_| anyhow::anyhow!("decode events error"))?;
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

// FIXME
fn to_decimal_represent(v: u128) -> Decimal {
    if v.trailing_zeros() >= 18 {
        Decimal::new((v / ONE_ONCHAIN).try_into().unwrap(), 0)
    } else {
        let d: Amount = v.try_into().unwrap();
        d / d18()
    }
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
