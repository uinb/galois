// Copyright 2021-2023 UINB Technologies Pte. Ltd.

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

use crate::{config::C, fusotao::*, input::Command, sequence};
use anyhow::anyhow;
use chrono::Local;
use memmap::MmapMut;
use node_api::events::{EventsDecoder, Raw};
use parity_scale_codec::Decode;
use sp_core::{sr25519::Public, Pair};
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

pub struct FusoConnector {
    pub api: FusoApi,
    pub signer: Sr25519Key,
    pub proved_event_id: Arc<AtomicU64>,
}

#[derive(Clone, Decode, Debug, Default)]
pub struct Dominator {
    pub name: Vec<u8>,
    pub staked: u128,
    pub merkle_root: [u8; 32],
    pub start_from: u32,
    pub sequence: (u64, u32),
    pub status: u8,
}

impl FusoConnector {
    pub fn new() -> anyhow::Result<Self> {
        let signer = Sr25519Key::from_string(&C.fusotao.key_seed, None)
            .map_err(|_| anyhow!("Invalid fusotao config"))?;
        let client = sub_api::rpc::WsRpcClient::new(&C.fusotao.node_url);
        let api = FusoApi::new(client)
            .map(|api| api.set_signer(signer.clone()))
            .map_err(|e| {
                log::error!("{:?}", e);
                anyhow!("Fusotao node not available or runtime metadata check failed")
            })?;
        Ok(Self {
            api,
            signer,
            proved_event_id: Arc::new(AtomicU64::new(0)),
        })
    }

    pub fn start_submitting(&self) -> anyhow::Result<()> {
        let api = self.api.clone();
        let proved_event_id = self.proved_event_id.clone();
        let who = self.signer.public();
        let mut last_proved_check_time = Local::now().timestamp();
        std::thread::spawn(move || loop {
            let start_from = proved_event_id.load(Ordering::Relaxed);
            let mut new_max_submitted = std::panic::catch_unwind(|| -> u64 {
                let (end_to, truncated) = Self::fetch_proofs(start_from);
                log::info!("found proofs to submit {}-{}", start_from, end_to);
                let submit_result = Self::submit_batch(&api, truncated);
                Self::handle_submit_result(submit_result, (start_from, end_to))
            })
            .unwrap_or(start_from);
            if start_from == new_max_submitted {
                std::thread::sleep(Duration::from_millis(1000));
                continue;
            }
            let now = Local::now().timestamp();
            if now - last_proved_check_time > 60 {
                new_max_submitted = std::panic::catch_unwind(|| -> u64 {
                    Self::sync_proving_progress(&who, &api).unwrap_or(new_max_submitted)
                })
                .unwrap_or(new_max_submitted);
                last_proved_check_time = now;
            }
            proved_event_id.store(new_max_submitted, Ordering::Relaxed);
        });
        Ok(())
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
                let at = C.fusotao.claim_block;
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
                        log::debug!("no interested events found before block {:?}", sync);
                    }
                }
                Err(e) => log::error!("sync blocks failed, {:?}", e),
            }
        });
        Ok(())
    }

    pub fn sync_proving_progress(who: &Public, api: &FusoApi) -> anyhow::Result<u64> {
        log::info!(
            "start to synchronize proving progress, time is {} now",
            Local::now().timestamp_millis()
        );
        let key = api
            .metadata
            .storage_map_key::<FusoAccountId>("Verifier", "Dominators", *who)
            .map_err(|_| anyhow!(""))?;
        let payload = api
            .get_opaque_storage_by_key_hash(key, None)
            .map_err(|_| anyhow!(""))?
            .ok_or(anyhow!(""))?;
        let result = Dominator::decode(&mut payload.as_slice())?;
        log::info!(
            "synchronizing proving progress: {}, time is {} now",
            result.sequence.0,
            Local::now().timestamp_millis()
        );
        Ok(result.sequence.0)
    }

    fn handle_submit_result(result: anyhow::Result<()>, (start_from, end_to): (u64, u64)) -> u64 {
        match result {
            Ok(()) => {
                log::info!("rotate proved event to {}", end_to);
                end_to
            }
            Err(e) => {
                log::error!("error occur while submitting proofs, {:?}", e);
                start_from
            }
        }
    }

    async fn resolve_block(
        api: &FusoApi,
        signer: &FusoAccountId,
        at: Option<u32>,
        decoder: &EventsDecoder,
    ) -> anyhow::Result<Vec<Command>> {
        use hex::ToHex;
        let hash = api.get_block_hash(at).map_err(|_| anyhow!(""))?;
        let key = api
            .metadata
            .storage_value_key("System", "Events")
            .map_err(|_| anyhow!(""))?;
        let payload = api.get_opaque_storage_by_key_hash(key, hash)?;
        let events = decoder
            .decode_events(&mut payload.unwrap_or(vec![]).as_slice())
            .unwrap_or(vec![]);
        let mut cmds = vec![];
        for (_, event) in events.into_iter() {
            match event {
                Raw::Event(raw) if raw.pallet == "Verifier" => match raw.variant.as_ref() {
                    "TokenHosted" => {
                        let decoded = TokenHostedEvent::decode(&mut &raw.data[..]).unwrap();
                        if &decoded.dominator == signer {
                            let mut cmd = Command::default();
                            cmd.cmd = crate::cmd::TRANSFER_IN;
                            cmd.currency = Some(decoded.token_id);
                            cmd.amount = to_decimal_represent(decoded.amount);
                            cmd.user_id = Some(format!("{}", decoded.fund_owner));
                            cmd.block_number = at.or(Some(0));
                            cmd.extrinsic_hash =
                                hash.map(|h| h.encode_hex()).or(Some("".to_string()));
                            cmds.push(cmd);
                        }
                    }
                    "TokenRevoked" => {
                        let decoded = TokenRevokedEvent::decode(&mut &raw.data[..]).unwrap();
                        if &decoded.dominator == signer {
                            let mut cmd = Command::default();
                            cmd.cmd = crate::cmd::TRANSFER_OUT;
                            cmd.currency = Some(decoded.token_id);
                            cmd.amount = to_decimal_represent(decoded.amount);
                            cmd.user_id = Some(format!("{}", decoded.fund_owner));
                            cmd.block_number = at.or(Some(0));
                            cmd.extrinsic_hash =
                                hash.map(|h| h.encode_hex()).or(Some("".to_string()));
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

    fn fetch_proofs(start_from: u64) -> (u64, Vec<RawParameter>) {
        let proofs = persistence::fetch_raw_after(start_from);
        let mut total_size = 0usize;
        let mut last_submit = start_from;
        let mut truncated = vec![];
        for (event_id, proof) in proofs.into_iter() {
            if total_size + proof.0.len() >= super::MAX_EXTRINSIC_SIZE {
                break;
            }
            total_size += proof.0.len();
            last_submit = event_id;
            truncated.push(proof);
        }
        (last_submit, truncated)
    }

    fn sync_blocks_or_wait(
        from_block_included: u32,
        api: &FusoApi,
        who: &FusoAccountId,
        decoder: &EventsDecoder,
    ) -> anyhow::Result<(Vec<Command>, u32)> {
        let finalized_block_hash = api
            .get_finalized_head()?
            .ok_or(anyhow!("finalized headers couldn't be found"))?;
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

    fn compress_proofs(raws: Vec<RawParameter>) -> Vec<u8> {
        let r = raws.encode();
        let uncompress_size = r.len();
        let compressed_proofs = lz4_flex::compress_prepend_size(r.as_ref());
        let compressed_size = compressed_proofs.len();
        log::info!(
            "proof compress: uncompress size = {}, compressed size = {}",
            uncompress_size,
            compressed_size
        );
        compressed_proofs
    }

    fn submit_batch(api: &FusoApi, batch: Vec<RawParameter>) -> anyhow::Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        log::info!(
            "===> starting to submit_proofs at {}",
            Local::now().timestamp_millis()
        );
        let hash = if C.fusotao.compress_proofs {
            let xt: sub_api::UncheckedExtrinsicV4<_, _> = sub_api::compose_extrinsic!(
                api,
                "Verifier",
                "verify_compress",
                Self::compress_proofs(batch)
            );
            api.send_extrinsic(xt.hex_encode(), sub_api::XtStatus::InBlock)
                .map_err(|e| anyhow::anyhow!("submitting proofs failed, {:?}", e))?
        } else {
            let xt: sub_api::UncheckedExtrinsicV4<_, _> =
                sub_api::compose_extrinsic!(api, "Verifier", "verify", batch);
            api.send_extrinsic(xt.hex_encode(), sub_api::XtStatus::InBlock)
                .map_err(|e| anyhow::anyhow!("submitting proofs failed, {:?}", e))?
        };
        log::info!(
            "<=== ending submit_proofs at {}",
            Local::now().timestamp_millis()
        );
        if hash.is_none() {
            Err(anyhow::anyhow!("extrinsic executed failed"))
        } else {
            log::info!(
                "[+] submitting proofs ok, extrinsic hash: {:?}",
                hex::encode(hash.unwrap())
            );
            Ok(())
        }
    }
}
