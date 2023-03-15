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

use super::*;
use crate::{config::C, input::Command, sequence};
use anyhow::anyhow;
use chrono::Local;
use node_api::decoder::{Raw, RuntimeDecoder, StorageHasher};
use parity_scale_codec::{Decode, Error as CodecError};
use sp_core::{sr25519::Public, Pair};
use std::{sync::atomic::Ordering, time::Duration};
use sub_api::Hash;

pub struct FusoConnector {
    pub api: FusoApi,
    pub signer: Sr25519Key,
    pub state: FusoState,
}

impl FusoConnector {
    pub fn new() -> anyhow::Result<Self> {
        let signer = Sr25519Key::from_string(&C.fusotao.key_seed, None)
            .map_err(|e| anyhow!("Invalid fusotao config: {:?}", e))?;
        let client = sub_api::rpc::WsRpcClient::new(&C.fusotao.node_url);
        let api = FusoApi::new(client)
            .map(|api| api.set_signer(signer.clone()))
            .inspect_err(|e| log::error!("{:?}", e))
            .map_err(|_| anyhow!("Fusotao node not available or metadata check failed."))?;
        let (block_number, hash) = Self::get_finalized_block(&api)?;
        let state = Self::fully_sync_chain(&signer.public(), &api, hash, block_number)?;
        Ok(Self { api, signer, state })
    }

    pub fn start_submitting(&self) -> anyhow::Result<()> {
        let api = self.api.clone();
        let proved_event_id = self.state.proved_event_id.clone();
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
        let scanning_block = self.state.scanning_progress.clone();
        log::info!(
            "start synchronizing from block {}",
            self.state.scanning_progress.load(Ordering::Relaxed)
        );
        std::thread::spawn(move || loop {
            let decoder = RuntimeDecoder::new(api.metadata.clone());
            let at = scanning_block.load(Ordering::Relaxed);
            match Self::sync_blocks_or_wait(at, &api, &who, &decoder) {
                Ok((cmds, sync)) => {
                    if !cmds.is_empty() {
                        log::info!("prepare handle {} events at block {:?}", cmds.len(), at);
                        match sequence::insert_sequences(&cmds) {
                            Ok(()) => {
                                scanning_block.fetch_add(1, Ordering::Relaxed);
                                log::info!("all events before block {} synchronized", at);
                            }
                            Err(e) => {
                                log::error!("save sequences at block {} failed, {:?}", at, e);
                            }
                        }
                    } else {
                        scanning_block.fetch_add(1, Ordering::Relaxed);
                        log::debug!("no interested events found before block {:?}", sync);
                    }
                }
                Err(e) => log::error!("sync block {} failed, {:?}", at, e),
            }
        });
        Ok(())
    }

    fn fully_sync_chain(
        who: &Public,
        api: &FusoApi,
        hash: Hash,
        block_number: u32,
    ) -> anyhow::Result<FusoState> {
        let state = FusoState::new();
        let decoder = RuntimeDecoder::new(api.metadata.clone());
        // proving progress, map AccountId -> Dominator
        let key = api
            .metadata
            .storage_map_key::<FusoAccountId>("Verifier", "Dominators", *who)
            .map_err(|e| anyhow!("Read storage failed: {:?}", e))?;
        let payload = api
            .get_opaque_storage_by_key_hash(key, Some(hash))
            .map_err(|e| anyhow!("Read storage failed: {:?}", e))?
            .ok_or(anyhow!(""))?;
        let result = Dominator::decode(&mut payload.as_slice())?;
        state
            .proved_event_id
            .store(result.sequence.0, Ordering::Relaxed);

        // market list, double map AccountId, Symbol -> Market
        let key = api
            .metadata
            .storage_double_map_partial_key::<FusoAccountId>("Market", "Markets", who)
            .map_err(|e| anyhow!("Read storage failed: {:?}", e))?;
        let payload = api
            .get_opaque_storage_pairs_by_key_hash(key, Some(hash))
            .map_err(|e| anyhow!("Read storage failed: {:?}", e))?
            .ok_or(anyhow!(""))?;
        for (k, v) in payload.into_iter() {
            let symbol =
                RuntimeDecoder::extract_double_map_identifier::<(u32, u32), FusoAccountId>(
                    StorageHasher::Blake2_128Concat,
                    StorageHasher::Blake2_128Concat,
                    who,
                    &mut k.as_slice(),
                )
                .map_err(|e| anyhow!("Decode storage key failed: {:?}", e))?;
            let market = OnchainSymbol::decode(&mut v.as_slice())
                .map_err(|e| anyhow!("Decode market failed: {:?}", e))?;
            state.symbols.insert(symbol, market);
        }
        // token list, map TokenId -> Token
        let key = api
            .metadata
            .storage_map_key_prefix("Token", "Tokens")
            .map_err(|e| anyhow!("Read storage failed: {:?}", e))?;
        let payload = api
            .get_opaque_storage_pairs_by_key_hash(key, Some(hash))
            .map_err(|e| anyhow!("Read storage failed: {:?}", e))?
            .ok_or(anyhow!(""))?;
        for (k, v) in payload.into_iter() {
            let token_id: u32 = RuntimeDecoder::extract_map_identifier(
                StorageHasher::Twox64Concat,
                &mut k.as_slice(),
            )
            .map_err(|e| anyhow!("Decode storage key failed: {:?}", e))?;
            let token = OnchainToken::decode(&mut v.as_slice())
                .map_err(|e| anyhow!("Decode token failed: {:?}", e))?;
            state.currencies.insert(token_id, token);
        }
        // broker list, map AccountId -> Broker
        let key = api
            .metadata
            .storage_map_key_prefix("Market", "Brokers")
            .map_err(|e| anyhow!("Read storage failed: {:?}", e))?;
        let payload = api
            .get_keys(key, Some(hash))
            .map_err(|e| anyhow!("Read storage failed: {:?}", e))?
            .ok_or(anyhow!(""))?;
        for k in payload.into_iter() {
            let broker: FusoAccountId = RuntimeDecoder::extract_map_identifier(
                StorageHasher::Blake2_128Concat,
                &mut k.as_slice(),
            )
            .map_err(|e| anyhow!("Decode storage key failed: {:?}", e))?;
            state.brokers.insert(broker, rand::random());
        }
        // pending receipts, double map AccountId, AccountId -> Receipt
        let key = api
            .metadata
            .storage_double_map_partial_key::<FusoAccountId>("Verifier", "Receipts", who)
            .map_err(|e| anyhow!("Read storage failed: {:?}", e))?;
        let payload = api
            .get_opaque_storage_pairs_by_key_hash(key, Some(hash))
            .map_err(|e| anyhow!("Read storage failed: {:?}", e))?
            .ok_or(anyhow!(""))?;
        for (k, v) in payload.into_iter() {
            let user =
                RuntimeDecoder::extract_double_map_identifier::<FusoAccountId, FusoAccountId>(
                    StorageHasher::Blake2_128Concat,
                    StorageHasher::Blake2_128Concat,
                    who,
                    &mut k.as_slice(),
                )
                .map_err(|e| anyhow!("Decode storage key failed: {:?}", e))?;
            let unexecuted = decoder
                .decode_raw_enum(
                    &mut v.as_slice(),
                    move |i, stream| -> Result<Command, CodecError> {
                        let mut cmd = Command::default();
                        cmd.cmd = crate::cmd::TRANSFER_IN;
                        cmd.currency = u32::decode(stream).ok();
                        cmd.amount = to_decimal_represent(u128::decode(stream)?);
                        cmd.user_id = Some(format!("{}", user));
                        cmd.block_number = u32::decode(stream).ok();
                        // TODO not a good idea to read the hash if the node isn't a full node
                        cmd.extrinsic_hash = Some(Default::default());
                        match i {
                            0 => {
                                cmd.cmd = crate::cmd::TRANSFER_IN;
                                Ok(cmd)
                            }
                            1 | 2 => {
                                cmd.cmd = crate::cmd::TRANSFER_OUT;
                                Ok(cmd)
                            }
                            _ => Err("Invalid enum variant".into()),
                        }
                    },
                )
                .map_err(|_| anyhow!("couldn't decode onchain Receipt"))?;
            println!("{:?}", unexecuted);
            // TODO insert sequence
        }
        state
            .scanning_progress
            .store(block_number, Ordering::Relaxed);
        println!("{:?}", state);
        // Ok(state)
        Err(anyhow!(""))
    }

    pub fn sync_proving_progress(who: &Public, api: &FusoApi) -> anyhow::Result<u64> {
        log::info!(
            "start to synchronize proving progress, time is {} now",
            Local::now().timestamp_millis()
        );
        let key = api
            .metadata
            .storage_map_key::<FusoAccountId>("Verifier", "Dominators", *who)
            .map_err(|e| anyhow!("Read storage failed: {:?}", e))?;
        let payload = api
            .get_opaque_storage_by_key_hash(key, None)
            .map_err(|e| anyhow!("Read storage failed: {:?}", e))?
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
        decoder: &RuntimeDecoder,
    ) -> anyhow::Result<Vec<Command>> {
        use hex::ToHex;
        let hash = api
            .get_block_hash(at)
            .map_err(|e| anyhow!("Read storage failed: {:?}", e))?;
        let key = api
            .metadata
            .storage_value_key("System", "Events")
            .map_err(|e| anyhow!("Read storage failed: {:?}", e))?;
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

    fn get_finalized_block(api: &FusoApi) -> anyhow::Result<(u32, Hash)> {
        let hash = api
            .get_finalized_head()?
            .ok_or(anyhow!("Finalized headers couldn't be found"))?;
        let block_number = api
            .get_signed_block(Some(hash))?
            .ok_or(anyhow!("signed block {} couldn't be found", hash))
            .map(|b: sub_api::SignedBlock<FusoBlock>| b.block.header.number)?;
        Ok((block_number, hash))
    }

    fn sync_blocks_or_wait(
        from_block_included: u32,
        api: &FusoApi,
        who: &FusoAccountId,
        decoder: &RuntimeDecoder,
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
            let xt: sub_api::UncheckedExtrinsicV4<_> = sub_api::compose_extrinsic!(
                api,
                "Verifier",
                "verify_compress",
                Self::compress_proofs(batch)
            );
            api.send_extrinsic(xt.hex_encode(), sub_api::XtStatus::InBlock)
                .map_err(|e| anyhow!("submitting proofs failed, {:?}", e))?
        } else {
            let xt: sub_api::UncheckedExtrinsicV4<_> =
                sub_api::compose_extrinsic!(api, "Verifier", "verify", batch);
            api.send_extrinsic(xt.hex_encode(), sub_api::XtStatus::InBlock)
                .map_err(|e| anyhow!("submitting proofs failed, {:?}", e))?
        };
        log::info!(
            "<=== ending submit_proofs at {}",
            Local::now().timestamp_millis()
        );
        if hash.is_none() {
            Err(anyhow!("extrinsic executed failed"))
        } else {
            log::info!(
                "[+] submitting proofs ok, extrinsic hash: {:?}",
                hex::encode(hash.unwrap())
            );
            Ok(())
        }
    }
}
