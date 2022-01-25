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

use crate::{config::C, fusotao::*, sequence};
use anyhow::anyhow;
use memmap::MmapMut;
use parity_scale_codec::Decode;
use sp_core::Pair;
use std::{
    convert::TryInto,
    fs::OpenOptions,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use sub_api::events::{EventsDecoder, Raw};

pub struct FusoConnector {
    api: FusoApi,
    signer: Sr25519Key,
    proved_event_id: Arc<AtomicU64>,
}

impl FusoConnector {
    pub fn new(proved_event_id: Arc<AtomicU64>) -> anyhow::Result<Self> {
        let signer = Sr25519Key::from_string(
            &C.fusotao
                .as_ref()
                .ok_or(anyhow!("Invalid fusotao config"))?
                .key_seed,
            None,
        )
        .map_err(|_| anyhow!("Invalid fusotao config"))?;
        let client = sub_api::rpc::WsRpcClient::new(
            &C.fusotao
                .as_ref()
                .ok_or(anyhow!("Invalid fusotao config"))?
                .node_url,
        );
        let api = FusoApi::new(client)
            .map(|api| api.set_signer(signer.clone()))
            .map_err(|e| {
                log::error!("{:?}", e);
                anyhow!("Fusotao node not available or runtime metadata check failed")
            })?;
        // TODO
        Ok(Self {
            api: api,
            signer: signer,
            proved_event_id: proved_event_id,
        })
    }

    // TODO
    fn sync_proving_progress(&self, finalized: bool) -> anyhow::Result<()> {
        Ok(())
    }

    pub fn start_submitting(&self) -> anyhow::Result<()> {
        let api = self.api.clone();
        let proved_event_id = self.proved_event_id.clone();
        std::thread::spawn(move || loop {
            let proofs = persistence::fetch_raw_after(proved_event_id.load(Ordering::Relaxed));
            if proofs.is_empty() {
                std::thread::sleep(Duration::from_millis(1000));
                continue;
            }
            let in_block = proofs.last().unwrap().0;
            let proofs = proofs.into_iter().map(|p| p.1).collect::<Vec<_>>();
            match Self::submit_batch(&api, proofs) {
                Ok(()) => proved_event_id.store(in_block, Ordering::Relaxed),
                Err(_) => {}
            }
        });
        Ok(())
    }

    fn submit_batch(api: &FusoApi, batch: Vec<RawParameter>) -> anyhow::Result<()> {
        let xt: sub_api::UncheckedExtrinsicV4<_> =
            sub_api::compose_extrinsic!(api, "Receipts", "verify", batch);
        // TODO
        api.send_extrinsic(xt.hex_encode(), sub_api::XtStatus::InBlock)
            .map_err(|e| anyhow::anyhow!("submit proofs failed, {:?}", e))
            .map(|_| ())
    }

    pub fn start_scanning(&self) -> anyhow::Result<()> {
        let api = self.api.clone();
        let who = self.signer.public().clone();
        let path: PathBuf = [&C.sequence.coredump_dir, "fusotao.blk"].iter().collect();
        let finalized_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;
        finalized_file.set_len(4)?;
        let mut blk = unsafe { MmapMut::map_mut(&finalized_file)? };
        let mut from_block_number = u32::from_le_bytes(blk.as_ref().try_into()?);
        if from_block_number == 0 {
            if !C.sequence.enable_from_genesis {
                panic!(
                    "couldn't load block number from mmap, add `-g` to force start from genesis"
                );
            } else {
                let at = C.fusotao.as_ref().unwrap().claim_block;
                from_block_number = at;
            }
        }
        let decoder = EventsDecoder::new(api.metadata.clone());
        log::info!("start synchronizing from block {}", from_block_number);
        std::thread::spawn(move || loop {
            match Self::sync_blocks_or_wait(from_block_number, &api, &who, &decoder) {
                Ok((cmds, sync)) => {
                    if !cmds.is_empty() {
                        log::info!(
                            "prepare handle {} events before block {:?}",
                            cmds.len(),
                            sync
                        );
                        match sequence::insert_sequences(&cmds) {
                            Ok(()) => {
                                from_block_number = sync;
                                blk[..].copy_from_slice(&sync.to_le_bytes()[..]);
                                // FIXME commit manually after memmap flush ok
                                blk.flush().unwrap();
                                log::info!("all events before block {} synchronized", sync);
                            }
                            Err(_) => {
                                log::warn!("save sequences from block {} failed", from_block_number)
                            }
                        }
                    } else {
                        from_block_number = sync;
                        blk[..].copy_from_slice(&sync.to_le_bytes()[..]);
                        blk.flush().unwrap();
                        log::info!("no interested events found before block {:?}", sync);
                    }
                }
                Err(e) => log::error!("sync blocks failed, {:?}", e),
            }
        });
        Ok(())
    }

    async fn resolve_block(
        api: &FusoApi,
        signer: &FusoAccountId,
        at: Option<u32>,
        decoder: &EventsDecoder,
    ) -> anyhow::Result<Vec<sequence::Command>> {
        let hash = api.get_block_hash(at)?;
        let slices: Option<Vec<u8>> = api.get_storage_value("System", "Events", hash)?;
        let events = decoder
            .decode_events(&mut slices.unwrap_or(vec![]).as_slice())
            .unwrap_or(vec![]);
        let mut cmds = vec![];
        for (_, event) in events.into_iter() {
            match event {
                Raw::Event(raw) if raw.pallet == "Receipts" => match raw.variant.as_ref() {
                    "CoinHosted" => {
                        let decoded = CoinHostedEvent::decode(&mut &raw.data[..]).unwrap();
                        if &decoded.dominator == signer {
                            let mut cmd = sequence::Command::default();
                            cmd.cmd = sequence::TRANSFER_IN;
                            cmd.currency = Some(0);
                            cmd.amount = Some(to_decimal_represent(decoded.amount));
                            cmd.user_id = Some(format!("{}", decoded.fund_owner));
                            cmd.block_number = at.or(Some(0));
                            cmd.extrinsic_hash = None;
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
                            cmd.block_number = at.or(Some(0));
                            cmd.extrinsic_hash = None;
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
                            cmd.block_number = at.or(Some(0));
                            cmd.extrinsic_hash = None;
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
                            cmd.block_number = at.or(Some(0));
                            cmd.extrinsic_hash = None;
                            cmds.push(cmd);
                        }
                    }
                    _ => {}
                },
                Raw::Event(event) => log::debug!("other event: {:?}", event),
                Raw::Error(error) => log::debug!("runtime error: {:?}", error),
            }
        }
        Ok(cmds)
    }

    fn sync_blocks_or_wait(
        from_block_included: u32,
        api: &FusoApi,
        who: &FusoAccountId,
        decoder: &EventsDecoder,
    ) -> anyhow::Result<(Vec<sequence::Command>, u32)> {
        let finalized_block_hash = api
            .get_finalized_head()?
            .ok_or(anyhow!("finalized heads couldn't be found"))?;
        let finalized_block: sub_api::SignedBlock<FusoBlock> = api
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
        if from_block_included > finalized_block_number {
            std::thread::sleep(Duration::from_millis(7000));
            return Ok((vec![], from_block_included));
        }
        let mut f = vec![];
        let mut i = 0;
        while from_block_included + i <= finalized_block_number {
            f.push(Self::resolve_block(
                api,
                who,
                Some(from_block_included + i),
                decoder,
            ));
            i += 1;
            if f.len() == 10 {
                break;
            }
        }
        let r = async_std::task::block_on(futures::future::try_join_all(f))?
            .into_iter()
            .flatten()
            .collect();
        Ok((r, from_block_included + i))
    }
}
