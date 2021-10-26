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
use async_std::task::block_on;
use futures::future::try_join_all;
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
    Api, SignedBlock, UncheckedExtrinsicV4, XtStatus,
};

pub type GlobalStates = SparseMerkleTree<Sha256Hasher, H256, DefaultStore<H256>>;
pub type FusoAccountId = Public;
pub type FusoAddress = MultiAddress<FusoAccountId, ()>;
pub type FusoHeader = Header<u32, BlakeTwo256>;
pub type FusoBlock = Block<FusoHeader, OpaqueExtrinsic>;
pub type FusoApi = Api<Sr25519, WsRpcClient>;

const ONE_ONCHAIN: u128 = 1_000_000_000_000_000_000;
const MILL: u32 = 1_000_000;
const BILL: u32 = 1_000_000_000;
const QUINTILL: u64 = 1_000_000_000_000_000_000;

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
pub struct CoinRevokedEvent {
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

#[derive(Encode, Decode, Clone, Debug)]
pub struct TokenRevokedEvent {
    fund_owner: FusoAccountId,
    dominator: FusoAccountId,
    token_id: u32,
    amount: u128,
}

impl WrapperTypeEncode for UserId {}

fn new_api(signer: Sr25519) -> anyhow::Result<FusoApi> {
    let client = WsRpcClient::new(
        &C.fusotao
            .as_ref()
            .ok_or(anyhow!("Invalid fusotao config"))?
            .node_url,
    );
    Api::new(client)
        .map(|api| api.set_signer(signer))
        .map_err(|e| {
            log::error!("{:?}", e);
            anyhow!("Fusotao node not available or runtime metadata check failed")
        })
}

fn start_submitting(api: FusoApi, rx: Receiver<Proof>) -> anyhow::Result<()> {
    use sp_core::Pair;
    let path: PathBuf = [&C.sequence.coredump_dir, "fusotao.seq"].iter().collect();
    // TODO `-g`
    let finalized_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)?;
    finalized_file.set_len(8)?;
    let mut seq = unsafe { MmapMut::map_mut(&finalized_file)? };
    let mut cur = u64::from_le_bytes(seq.as_ref().try_into()?);
    if cur == 0 && !C.sequence.enable_from_genesis {
        panic!("couldn't load seq memmap file, add `-g` to force start from genesis");
    }
    log::info!("initiate proving at sequence {:?}", cur);
    std::thread::spawn(move || loop {
        let proof = rx.recv().unwrap();
        if cur >= proof.event_id {
            continue;
        }
        cur = proof.event_id;
        log::debug!("proofs of sequence {:?}: {:?}", cur, proof);
        let xt: UncheckedExtrinsicV4<_> =
            sub_api::compose_extrinsic!(api.clone(), "Receipts", "verify", proof);
        match api.send_extrinsic(xt.hex_encode(), XtStatus::InBlock) {
            Ok(_) => {
                // TODO scan block and revert status once chain fork
                seq.copy_from_slice(&cur.to_le_bytes()[..]);
                seq.flush().unwrap();
                log::info!("proofs of sequence {:?} has been submitted", cur);
            }
            Err(e) => log::error!("submitting proofs failed, {:?}", e),
        }
    });
    Ok(())
}

fn start_listening(api: FusoApi, who: Public) -> anyhow::Result<()> {
    let path: PathBuf = [&C.sequence.coredump_dir, "fusotao.blk"].iter().collect();
    let finalized_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)?;
    finalized_file.set_len(4)?;
    let mut blk = unsafe { MmapMut::map_mut(&finalized_file)? };
    let mut cur = u32::from_le_bytes(blk.as_ref().try_into()?);
    if cur == 0 {
        if !C.sequence.enable_from_genesis {
            panic!("couldn't load blk memmap file, add `-g` to force start from genesis");
        } else {
            let at = C.fusotao.as_ref().unwrap().claim_block;
            blk.copy_from_slice(&at.to_le_bytes()[..]);
            blk.flush().unwrap();
        }
    }
    log::info!("start synchronizing events from block {}", cur);
    let decoder = EventsDecoder::try_from(api.metadata.clone()).unwrap();
    std::thread::spawn(move || loop {
        match sync_finalized_blocks(cur, 10, &api, &who, &decoder) {
            Ok((cmds, sync, last)) => {
                if !cmds.is_empty() {
                    log::info!(
                        "prepare handle {} events before block {:?}",
                        cmds.len(),
                        sync
                    );
                }
                match sequence::insert_sequences(&cmds) {
                    Ok(()) => {
                        blk[..].copy_from_slice(&sync.to_le_bytes()[..]);
                        // FIXME commit manually after memmap flush ok
                        blk.flush().unwrap();
                        log::info!("all events before before block {} synchronized", sync);
                    }
                    Err(_) => log::warn!("save sequences from block {} failed", cur),
                }
                cur = sync;
                if cur >= last {
                    std::thread::sleep(Duration::from_millis(7000));
                }
            }
            Err(e) => log::error!("sync blocks failed, {:?}", e),
        }
    });
    Ok(())
}

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
    let api = new_api(signer.clone())?;
    start_submitting(api.clone(), rx)?;
    start_listening(api, signer.public())?;
    log::info!("fusotao prover initialized");
    Ok(())
}

async fn resolve_block(
    api: &FusoApi,
    at: u32,
    signer: &Public,
    decoder: &EventsDecoder,
) -> anyhow::Result<Vec<sequence::Command>> {
    let hash = api
        .get_block_hash(at)?
        .ok_or(anyhow!("block {} is not born", at))?;
    let e = api.get_opaque_storage_by_key_hash(
        sub_api::utils::storage_key("System", "Events"),
        Some(hash),
    )?;
    if e.is_none() {
        log::warn!("no events in block {}", at);
        return Ok(vec![]);
    }
    let e = e.unwrap();
    let raw_events = decoder.decode_events(&mut e.as_slice()).map_err(|e| {
        log::error!("{:?}", e);
        anyhow::anyhow!("decode events error")
    })?;
    let mut cmds = vec![];
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
                        cmd.block_number = Some(at);
                        cmd.extrinsic_hash = Some(hex::encode(hash));
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
                        cmd.block_number = Some(at);
                        cmd.extrinsic_hash = Some(hex::encode(hash));
                        cmds.push(cmd);
                    }
                }
                "CoinRevoked" => {
                    let decoded = CoinRevokedEvent::decode(&mut &raw.data[..]).unwrap();
                    if &decoded.dominator == signer {
                        let mut cmd = sequence::Command::default();
                        cmd.cmd = sequence::TRANSFER_OUT;
                        cmd.currency = Some(0);
                        cmd.amount = Some(to_decimal_represent(decoded.amount));
                        cmd.user_id = Some(format!("{}", decoded.fund_owner));
                        cmd.block_number = Some(at);
                        cmd.extrinsic_hash = Some(hex::encode(hash));
                        cmds.push(cmd);
                    }
                }
                "TokenRevoked" => {
                    let decoded = TokenRevokedEvent::decode(&mut &raw.data[..]).unwrap();
                    if &decoded.dominator == signer {
                        let mut cmd = sequence::Command::default();
                        cmd.cmd = sequence::TRANSFER_OUT;
                        cmd.currency = Some(decoded.token_id);
                        cmd.amount = Some(to_decimal_represent(decoded.amount));
                        cmd.user_id = Some(format!("{}", decoded.fund_owner));
                        cmd.block_number = Some(at);
                        cmd.extrinsic_hash = Some(hex::encode(hash));
                        cmds.push(cmd);
                    }
                }
                _ => {}
            },
            _ => {}
        }
    }
    Ok(cmds)
}

fn sync_finalized_blocks(
    from_block_included: u32,
    limit: u32,
    api: &FusoApi,
    signer: &Public,
    decoder: &EventsDecoder,
) -> anyhow::Result<(Vec<sequence::Command>, u32, u32)> {
    let finalized_block_hash = api
        .get_finalized_head()?
        .ok_or(anyhow!("finalized heads couldn't be found"))?;
    let finalized_block: SignedBlock<FusoBlock> =
        api.get_signed_block(Some(finalized_block_hash))?.unwrap();
    let finalized_block_number = finalized_block.block.header.number;
    log::info!(
        "current block: {}, finalized block: {}, {:?}",
        from_block_included,
        finalized_block_number,
        finalized_block_hash,
    );
    let mut f = vec![];
    let mut i = 0;
    while from_block_included + i <= finalized_block_number && i < limit {
        f.push(resolve_block(api, from_block_included + i, signer, decoder));
        i += 1;
    }
    let r = block_on(try_join_all(f))?.into_iter().flatten().collect();
    Ok((r, from_block_included + i, finalized_block_number))
}

#[derive(Clone, Encode, Decode, Eq, PartialEq, Debug)]
pub enum FusoCommand {
    // price, amounnt, maker_fee, taker_fee, base, quote
    AskLimit(
        (Compact<u64>, Compact<u64>),
        Compact<u128>,
        Compact<u32>,
        Compact<u32>,
        Compact<u32>,
        Compact<u32>,
    ),
    BidLimit(
        (Compact<u64>, Compact<u64>),
        Compact<u128>,
        Compact<u32>,
        Compact<u32>,
        Compact<u32>,
        Compact<u32>,
    ),
    Cancel(Compact<u32>, Compact<u32>),
    TransferOut(Compact<u32>, Compact<u128>),
    TransferIn(Compact<u32>, Compact<u128>),
}

impl Into<FusoCommand> for (LimitCmd, Fee, Fee) {
    fn into(self) -> FusoCommand {
        let (n, f) = self.0.price.to_price();
        match self.0.ask_or_bid {
            AskOrBid::Ask => FusoCommand::AskLimit(
                (n.into(), f.into()),
                self.0.amount.to_amount().into(),
                self.1.to_fee().into(),
                self.2.to_fee().into(),
                self.0.symbol.0.into(),
                self.0.symbol.1.into(),
            ),
            AskOrBid::Bid => FusoCommand::BidLimit(
                (n.into(), f.into()),
                self.0.amount.to_amount().into(),
                self.1.to_fee().into(),
                self.2.to_fee().into(),
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
            InOrOut::In => {
                FusoCommand::TransferIn(self.currency.into(), self.amount.to_amount().into())
            }
            InOrOut::Out => {
                FusoCommand::TransferOut(self.currency.into(), self.amount.to_amount().into())
            }
        }
    }
}

fn d6() -> Amount {
    MILL.into()
}

#[allow(dead_code)]
fn d9() -> Amount {
    BILL.into()
}

fn d18() -> Amount {
    QUINTILL.into()
}

pub trait ToBlockChainNumeric {
    fn to_fee(self) -> u32;

    fn to_price(self) -> (u64, u64);

    fn to_amount(self) -> u128;
}

impl ToBlockChainNumeric for Decimal {
    fn to_fee(self) -> u32 {
        (self * d6()).to_u32().unwrap()
    }

    fn to_price(self) -> (u64, u64) {
        let f = self.fract() * d18();
        (self.trunc().to_u64().unwrap(), f.to_u64().unwrap())
    }

    fn to_amount(self) -> u128 {
        let n = self.trunc().to_u128().unwrap();
        let f = self.fract() * d18();
        n * ONE_ONCHAIN + f.to_u128().unwrap()
    }
}

// FIXME
fn to_decimal_represent(v: u128) -> Decimal {
    let n = v / ONE_ONCHAIN;
    let f = v % ONE_ONCHAIN;
    let n: Amount = n.try_into().unwrap();
    let mut f: Amount = f.try_into().unwrap();
    f.set_scale(18).unwrap();
    n + f
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
    use rust_decimal_macros::dec;

    #[test]
    pub fn test_numeric() {
        assert!(Decimal::MAX.to_u128().is_some());
        assert!(Decimal::MAX > dec!(340282366920938463463.374607431768211455));
        let p = dec!(38463463.374607431768211455);
        let (n, f) = p.to_price();
        assert_eq!(n, 38463463);
        assert_eq!(f, 374607431768211455);
    }
}
