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

use crate::{config::C, core::*, event::*, fusotao::*, sequence};
use anyhow::anyhow;
use async_std::task::block_on;
use futures::future::try_join_all;
use memmap::MmapMut;
use parity_scale_codec::{Compact, Decode, Encode, WrapperTypeEncode};
use rocksdb::{DBWithThreadMode, MultiThreaded, Options as RocksOptions};
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
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc::{Receiver, RecvTimeoutError},
        Arc,
    },
    time::Duration,
};
use sub_api::{
    rpc::{
        ws_client::{EventsDecoder, RuntimeEvent},
        WsRpcClient,
    },
    Api, SignedBlock, UncheckedExtrinsicV4, XtStatus,
};

pub type Rocks = DBWithThreadMode<MultiThreaded>;

pub struct FusoConnector {
    api: FusoApi,
    signer: Sr25519,
    rocks: Arc<Rocks>,
    proved_event_id: Arc<AtomicU64>,
}

impl FusoConnector {
    pub fn new(rocks: Arc<Rocks>, proved_event_id: Arc<AtomicU64>) -> anyhow::Result<Self> {
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
        let api = Api::new(client)
            .map(|api| api.set_signer(signer))
            .map_err(|e| {
                log::error!("{:?}", e);
                anyhow!("Fusotao node not available or runtime metadata check failed")
            })?;
        Ok(Self {
            api: api,
            signer: signer,
            rocks: rocks,
            proved_event_id: proved_event_id,
        })
    }

    fn start_submitting(&self) -> anyhow::Result<()> {
        let path: PathBuf = [&C.sequence.coredump_dir, "fusotao.seq"].iter().collect();
        let finalized_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;
        finalized_file.set_len(8)?;
        let mut seq_mmap = unsafe { MmapMut::map_mut(&finalized_file)? };
        let mut max_proof_id = u64::from_le_bytes(seq_mmap[..].try_into()?);
        let mut buf_len = 0usize;
        let mut batch = vec![];
        if max_proof_id == 0 && !C.sequence.enable_from_genesis {
            panic!(
                "couldn't load seq memmap file, set `enable_from_genesis` to force start from genesis"
            );
        }
        log::info!("initiate proving from sequence {:?}", max_proof_id);
        std::thread::spawn(move || loop {
            let proof = match rx.recv_timeout(Duration::from_millis(60_000)) {
                Ok(p) => Some(p),
                Err(RecvTimeoutError::Timeout) => None,
                Err(RecvTimeoutError::Disconnected) => break,
            };
            if proof.is_none() && !batch.is_empty() {
                max_proof_id = submit_batch(api.clone(), &mut batch).unwrap();
                (&mut seq_mmap[..]).copy_from_slice(&max_proof_id.to_le_bytes()[..]);
                seq_mmap.flush().unwrap();
                proved_event_id.store(max_proof_id, Ordering::Relaxed);
                buf_len = 0;
                continue;
            }
            let proof = proof.unwrap();
            if max_proof_id >= proof.event_id {
                continue;
            }
            log::debug!("proof of sequence => {:?}", proof);
            let size = proof.size_hint();
            if size + buf_len > MAX_EXTRINSIC_BYTES {
                if batch.is_empty() {
                    // FIXME the proof is too long to put into a single block, e.g. too many makers
                    // this a known bug, simply deleting the command would work. we'll add a constraint to fix it in the future
                    panic!("proof size too big, delete the command and reboot");
                }
                max_proof_id = submit_batch(api.clone(), &mut batch).unwrap();
                (&mut seq_mmap[..]).copy_from_slice(&max_proof_id.to_le_bytes()[..]);
                seq_mmap.flush().unwrap();
                proved_event_id.store(max_proof_id, Ordering::Relaxed);
                buf_len = 0;
            }
            batch.push(proof);
            buf_len += size;
        });
        Ok(())
    }

    fn submit_batch(api: FusoApi, batch: &mut Vec<Proof>) -> anyhow::Result<u64> {
        use sp_core::Pair;
        if batch.is_empty() {
            return Err(anyhow::anyhow!("Empty proofs"));
        }
        let last_submitted_id = batch.last().unwrap().event_id;
        for i in 0..=3 {
            let xt: UncheckedExtrinsicV4<_> =
                sub_api::compose_extrinsic!(api, "Receipts", "verify", batch.clone());
            match api.send_extrinsic(xt.hex_encode(), XtStatus::InBlock) {
                Ok(_) => {
                    // FIXME scan block and revert status once chain fork
                    log::info!(
                        "proof of sequence until {:?} have been submitted",
                        last_submitted_id
                    );
                    batch.clear();
                    return Ok(last_submitted_id);
                }
                Err(e) => {
                    log::error!(
                        "submit proof failed, remain {:?} times retrying, error => {:?}",
                        3 - i,
                        e
                    );
                    std::thread::sleep(Duration::from_millis(i * 15000));
                }
            }
        }
        return Err(anyhow::anyhow!(
            "fail to submit proofs after 3 times retrying, {}",
            last_submitted_id
        ));
    }

    fn start_scanning(&self) -> anyhow::Result<()> {
        let mut from_block_number = u32::from_le_bytes(
            // TODO don't use ascii keys
            self.rocks
                .get_pinned(b"block_number")?
                .unwrap_or(vec![0; 4]),
        );
        if from_block_number == 0 {
            if !C.sequence.enable_from_genesis {
                panic!("couldn't load block number from rocksdb storage, add `-g` to force start from genesis");
            } else {
                let at = C.fusotao.as_ref().unwrap().claim_block;
                self.rocks.put(b"block_number", at.to_le_bytes())?;
                from_block_number = at;
            }
        }
        let decoder = EventsDecoder::try_from(api.metadata.clone())?;
        log::info!("start synchronizing from block {}", from_block_number);
        // TODO
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
                            log::info!("all events until block {} synchronized", sync);
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

    async fn resolve_block(
        api: &FusoApi,
        signer: &Public,
        at: u32,
        decoder: &EventsDecoder,
    ) -> anyhow::Result<Vec<sequence::Command>> {
        let hash = 
            .api
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
        api: &FusoApi,
        from_block_included: u32,
        limit: u32,
        decoder: &EventsDecoder,
    ) -> anyhow::Result<(Vec<sequence::Command>, u32, u32)> {
        let finalized_block_hash = api
            .get_finalized_head()?
            .ok_or(anyhow!("finalized heads couldn't be found"))?;
        let finalized_block: SignedBlock<FusoBlock> = 
            .api
            .get_signed_block(Some(finalized_block_hash))?
            .ok_or(anyhow!(
                "signed block {} couldn't be found",
                finalized_block_hash
            ))?;
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
            f.push(Self::resolve_block(api, from_block_included + i, signer, decoder));
            i += 1;
        }
        let r = block_on(try_join_all(f))?.into_iter().flatten().collect();
        Ok((r, from_block_included + i, finalized_block_number))
    }
}
