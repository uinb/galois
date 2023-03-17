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
use sub_api::{rpc::WsRpcClient, Hash};

pub struct FusoConnector {
    pub api: FusoApi,
    pub signer: Sr25519Key,
    pub state: Arc<FusoState>,
}

impl FusoConnector {
    pub fn new(dry_run: bool) -> anyhow::Result<Self> {
        let signer = Sr25519Key::from_string(&C.fusotao.key_seed, None)
            .map_err(|e| anyhow!("invalid fusotao config: {:?}", e))?;
        let client = WsRpcClient::new(&C.fusotao.node_url);
        let api = FusoApi::new(client)
            .map(|api| api.set_signer(signer.clone()))
            .inspect_err(|e| log::error!("{:?}", e))
            .map_err(|_| anyhow!("fusotao node not available or metadata check failed."))?;
        let state = if dry_run {
            Arc::new(Default::default())
        } else {
            let (block_number, hash) = Self::get_finalized_block(&api)?;
            let state = Arc::new(Self::fully_sync_chain(
                &signer.public(),
                &api,
                hash,
                block_number,
            )?);
            Self::start_submitting(api.clone(), state.proved_event_id.clone());
            Self::start_scanning(api.clone(), signer.public().clone(), state.clone());
            state
        };
        Ok(Self { api, signer, state })
    }

    fn start_submitting(api: FusoApi, proving_progress: Arc<AtomicU64>) {
        let api = api.clone();
        log::info!(
            "submitting proofs from {}",
            proving_progress.load(Ordering::Relaxed)
        );
        std::thread::spawn(move || loop {
            let start_from = proving_progress.load(Ordering::Relaxed);
            let new_max_submitted = std::panic::catch_unwind(|| -> u64 {
                let (end_to, truncated) = Self::fetch_proofs(start_from);
                if start_from == end_to {
                    return end_to;
                }
                log::info!("[+] unsubmitted proofs [{}:{}] found", start_from, end_to);
                let submit_result = Self::submit_batch(&api, truncated);
                Self::handle_submit_result(submit_result, (start_from, end_to))
            })
            .unwrap_or(start_from);
            if start_from == new_max_submitted {
                std::thread::sleep(Duration::from_millis(1000));
                continue;
            }
            proving_progress.store(new_max_submitted, Ordering::Relaxed);
        });
    }

    fn start_scanning(api: FusoApi, signer: Public, fuso_state: Arc<FusoState>) {
        let decoder = RuntimeDecoder::new(api.metadata.clone());
        log::info!(
            "scanning blocks from {}",
            fuso_state.scanning_progress.load(Ordering::Relaxed)
        );
        std::thread::spawn(move || loop {
            let at = fuso_state.scanning_progress.load(Ordering::Relaxed);
            log::info!("[*] handle block {}", at);
            match Self::handle_block(&api, &signer, at, &decoder, &fuso_state) {
                Ok(()) => {
                    fuso_state.scanning_progress.fetch_add(1, Ordering::Relaxed);
                    log::info!("[*] handle block {} done", at);
                }
                Err(e) => log::error!("[*] {:?}", e),
            }
            std::thread::sleep(Duration::from_millis(4000));
        });
    }

    fn fully_sync_chain(
        who: &Public,
        api: &FusoApi,
        hash: Hash,
        util: u32,
    ) -> anyhow::Result<FusoState> {
        let state = FusoState::default();
        let decoder = RuntimeDecoder::new(api.metadata.clone());

        // proving progress, map AccountId -> Dominator
        let key = api
            .metadata
            .storage_map_key::<FusoAccountId>("Verifier", "Dominators", *who)?;
        let payload = api
            .get_opaque_storage_by_key_hash(key, Some(hash))?
            .ok_or(anyhow!(""))?;
        let result = Dominator::decode(&mut payload.as_slice())?;
        state
            .proved_event_id
            .store(result.sequence.0, Ordering::Relaxed);

        // market list, double map AccountId, Symbol -> Market
        let key = api
            .metadata
            .storage_double_map_partial_key::<FusoAccountId>("Market", "Markets", who)?;
        let payload = api
            .get_opaque_storage_pairs_by_key_hash(key, Some(hash))?
            .ok_or(anyhow!(""))?;
        for (k, v) in payload.into_iter() {
            let symbol = RuntimeDecoder::extract_double_map_identifier::<(u32, u32), FusoAccountId>(
                StorageHasher::Blake2_128Concat,
                StorageHasher::Blake2_128Concat,
                who,
                &mut k.as_slice(),
            )?;
            let market = OnchainSymbol::decode(&mut v.as_slice())?;
            state.symbols.insert(symbol, market);
        }

        // token list, map TokenId -> Token
        let key = api.metadata.storage_map_key_prefix("Token", "Tokens")?;
        let payload = api
            .get_opaque_storage_pairs_by_key_hash(key, Some(hash))?
            .ok_or(anyhow!(""))?;
        for (k, v) in payload.into_iter() {
            let token_id: u32 = RuntimeDecoder::extract_map_identifier(
                StorageHasher::Twox64Concat,
                &mut k.as_slice(),
            )?;
            let token = OnchainToken::decode(&mut v.as_slice())?;
            state.currencies.insert(token_id, token);
        }

        // broker list, map AccountId -> Broker
        let key = api.metadata.storage_map_key_prefix("Market", "Brokers")?;
        let payload = api.get_keys(key, Some(hash))?.ok_or(anyhow!(""))?;
        for k in payload.into_iter() {
            let broker: FusoAccountId = RuntimeDecoder::extract_map_identifier(
                StorageHasher::Blake2_128Concat,
                &mut k.as_slice(),
            )?;
            state.brokers.insert(broker, rand::random());
        }

        // pending receipts, double map AccountId, AccountId -> Receipt
        let key = api
            .metadata
            .storage_double_map_partial_key::<FusoAccountId>("Verifier", "Receipts", who)?;
        let payload = api
            .get_opaque_storage_pairs_by_key_hash(key, Some(hash))?
            .ok_or(anyhow!(""))?;
        let mut commands = vec![];
        for (k, v) in payload.into_iter() {
            let user = RuntimeDecoder::extract_double_map_identifier::<FusoAccountId, FusoAccountId>(
                StorageHasher::Blake2_128Concat,
                StorageHasher::Blake2_128Concat,
                who,
                &mut k.as_slice(),
            )?;
            let unexecuted = decoder.decode_raw_enum(
                &mut v.as_slice(),
                move |i, stream| -> Result<Command, CodecError> {
                    let mut cmd = Command::default();
                    cmd.currency = Some(u32::decode(stream)?);
                    cmd.amount = to_decimal_represent(u128::decode(stream)?);
                    cmd.user_id = Some(format!("{}", user));
                    cmd.block_number = Some(u32::decode(stream)?);
                    // FIXME not a good idea to read the hash if the node isn't a full node
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
                        _ => {
                            Err("invalid enum variant of Receipt, check the fusotao version".into())
                        }
                    }
                },
            )?;
            commands.push(unexecuted);
        }
        println!("{:?}", commands);
        sequence::insert_sequences(&commands)?;
        state.scanning_progress.store(util + 1, Ordering::Relaxed);
        Ok(state)
    }

    fn handle_submit_result(result: anyhow::Result<()>, (start_from, end_to): (u64, u64)) -> u64 {
        match result {
            Ok(()) => {
                log::info!("[+] rotating proved event to {}", end_to);
                end_to
            }
            Err(e) => {
                log::error!("[-] error occured while submitting proofs, {:?}", e);
                start_from
            }
        }
    }

    fn handle_block(
        api: &FusoApi,
        signer: &FusoAccountId,
        at: u32,
        decoder: &RuntimeDecoder,
        state: &Arc<FusoState>,
    ) -> anyhow::Result<()> {
        use hex::ToHex;
        let hash = api
            .get_block_hash(Some(at))?
            .ok_or(anyhow!("block {} not ready", at))?;
        let key = api
            .metadata
            .storage_value_key("System", "Events")
            .map_err(|e| anyhow!("Read storage failed: {:?}", e))?;
        let payload = api.get_opaque_storage_by_key_hash(key, Some(hash))?;
        let events = decoder
            .decode_events(&mut payload.unwrap_or(vec![]).as_slice())
            .unwrap_or(vec![]);
        for (_, event) in events.into_iter() {
            if let Raw::Event(raw) = event {
                match (raw.pallet.as_ref(), raw.variant.as_ref()) {
                    ("Verifier", "TokenHosted") => {
                        let decoded = TokenHostedEvent::decode(&mut &raw.data[..])?;
                        if &decoded.dominator == signer {
                            let mut cmd = Command::default();
                            cmd.cmd = crate::cmd::TRANSFER_IN;
                            cmd.currency = Some(decoded.token_id);
                            cmd.amount = to_decimal_represent(decoded.amount);
                            cmd.user_id = Some(format!("{}", decoded.fund_owner));
                            cmd.block_number = Some(at);
                            cmd.extrinsic_hash = Some(hash.encode_hex());
                            sequence::insert_sequences(&mut vec![cmd])?;
                        }
                    }
                    ("Verifier", "TokenRevoked") => {
                        let decoded = TokenHostedEvent::decode(&mut &raw.data[..])?;
                        if &decoded.dominator == signer {
                            let mut cmd = Command::default();
                            cmd.cmd = crate::cmd::TRANSFER_OUT;
                            cmd.currency = Some(decoded.token_id);
                            cmd.amount = to_decimal_represent(decoded.amount);
                            cmd.user_id = Some(format!("{}", decoded.fund_owner));
                            cmd.block_number = Some(at);
                            cmd.extrinsic_hash = Some(hash.encode_hex());
                            sequence::insert_sequences(&mut vec![cmd])?;
                        }
                    }
                    ("Token", "TokenIssued") => {
                        let decoded = TokenIssuedEvent::decode(&mut &raw.data[..])?;
                        let key = api
                            .metadata
                            .storage_map_key::<u32>("Token", "Tokens", decoded.token_id)
                            .map_err(|e| anyhow!("Read storage failed: {:?}", e))?;
                        let payload = api
                            .get_opaque_storage_by_key_hash(key, Some(hash))?
                            .ok_or(anyhow::anyhow!(""))?;
                        let token = OnchainToken::decode(&mut payload.as_slice())?;
                        state.currencies.insert(decoded.token_id, token);
                    }
                    ("Market", "BrokerRegistered") => {
                        let decoded = BrokerRegisteredEvent::decode(&mut &raw.data[..])?;
                        state.brokers.insert(decoded.broker_account, rand::random());
                    }
                    ("Market", "MarketOpened") => {
                        let decoded = MarketOpenedEvent::decode(&mut &raw.data[..])?;
                        if &decoded.dominator == signer {
                            state.symbols.insert(
                                (decoded.base, decoded.quote),
                                OnchainSymbol {
                                    min_base: decoded.min_base,
                                    base_scale: decoded.base_scale,
                                    quote_scale: decoded.quote_scale,
                                    status: MarketStatus::Open,
                                    trading_rewards: true,
                                    liquidity_rewards: true,
                                    unavailable_after: None,
                                },
                            );
                            // TODO impl Into<Command> for SymbolCmd
                            let mut cmd = Command::default();
                            let milli = Decimal::from_str("0.001").unwrap();
                            cmd.cmd = crate::cmd::UPDATE_SYMBOL;
                            cmd.base = Some(decoded.base);
                            cmd.quote = Some(decoded.quote);
                            cmd.open = Some(true);
                            cmd.base_scale = Some(decoded.base_scale.into());
                            cmd.quote_scale = Some(decoded.quote_scale.into());
                            cmd.taker_fee = Some(milli);
                            cmd.maker_fee = Some(milli);
                            cmd.min_amount = to_decimal_represent(decoded.min_base);
                            // DEPRECATED
                            cmd.base_maker_fee = Some(milli);
                            cmd.base_taker_fee = Some(milli);
                            cmd.fee_times = Some(1);
                            cmd.min_vol = Some(Decimal::from_str("10").unwrap());
                            cmd.enable_market_order = Some(false);
                            sequence::insert_sequences(&mut vec![cmd])?;
                        }
                    }
                    ("Market", "MarketClosed") => {
                        let decoded = MarketClosedEvent::decode(&mut &raw.data[..])?;
                        if &decoded.dominator == signer {
                            let market = state.symbols.remove(&(decoded.base, decoded.quote));
                            let mut cmd = Command::default();
                            let milli = Decimal::from_str("0.001").unwrap();
                            cmd.cmd = crate::cmd::UPDATE_SYMBOL;
                            cmd.base = Some(decoded.base);
                            cmd.quote = Some(decoded.quote);
                            cmd.open = Some(false);
                            cmd.taker_fee = Some(milli);
                            cmd.maker_fee = Some(milli);
                            let (base_scale, quote_scale, min_amount) = market
                                .map(|(_, m)| (m.base_scale, m.quote_scale, m.min_base))
                                .ok_or(anyhow!(""))?;
                            cmd.base_scale = Some(base_scale.into());
                            cmd.quote_scale = Some(quote_scale.into());
                            cmd.min_amount = to_decimal_represent(min_amount);
                            // DEPRECATED
                            cmd.base_maker_fee = Some(milli);
                            cmd.base_taker_fee = Some(milli);
                            cmd.fee_times = Some(1);
                            cmd.min_vol = Some(Decimal::from_str("10").unwrap());
                            cmd.enable_market_order = Some(false);
                            sequence::insert_sequences(&mut vec![cmd])?;
                        }
                    }
                    _ => {}
                }
            }
        }
        Ok(())
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
            .ok_or(anyhow!("finalized headers cant be found"))?;
        let block_number = api
            .get_signed_block(Some(hash))?
            .ok_or(anyhow!("signed block {} can't be found", hash))
            .map(|b: sub_api::SignedBlock<FusoBlock>| b.block.header.number)?;
        Ok((block_number, hash))
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
            "[+] starting to submit_proofs at {}",
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
                .map_err(|e| anyhow!("[-] submitting proofs failed, {:?}", e))?
        } else {
            let xt: sub_api::UncheckedExtrinsicV4<_> =
                sub_api::compose_extrinsic!(api, "Verifier", "verify", batch);
            api.send_extrinsic(xt.hex_encode(), sub_api::XtStatus::InBlock)
                .map_err(|e| anyhow!("[-] submitting proofs failed, {:?}", e))?
        };
        log::info!(
            "[+] ending submit_proofs at {}",
            Local::now().timestamp_millis()
        );
        if hash.is_none() {
            Err(anyhow!(
                "[-] verify extrinsic executed failed, no extrinsic returns"
            ))
        } else {
            log::info!(
                "[+] submitting proofs ok, extrinsic hash: {:?}",
                hex::encode(hash.unwrap())
            );
            Ok(())
        }
    }
}
