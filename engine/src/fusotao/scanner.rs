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
use crate::{config::C, input::Command};
use anyhow::anyhow;
use chrono::Local;
use node_api::decoder::{Raw, RuntimeDecoder, StorageHasher};
use parity_scale_codec::{Decode, Error as CodecError};
use sp_core::{sr25519::Public, Pair};
use std::{sync::atomic::Ordering, sync::mpsc::Sender, thread, time::Duration};
use sub_api::{rpc::WsRpcClient, Hash};

pub fn init(tx: Sender<Input>, connector: FusoConnector, state: Arc<FusoState>) {
    let decoder = RuntimeDecoder::new(connector.api.metadata.clone());
    // TODO fully sync
    thread::spawn(move || loop {
        let at = state.scanning_progress.load(Ordering::Relaxed);
        if let Ok((finalized, _)) = connector.get_finalized_block() {
            log::info!("block {} finalized, ours {}", finalized, at);
            state.chain_height.store(finalized, Ordering::Relaxed);
            if finalized >= at {
                match handle_finalized_block(&connector, at, &decoder, &state, &tx) {
                    Ok(()) => {
                        state.scanning_progress.fetch_add(1, Ordering::Relaxed);
                        log::info!("block {} finalized", at);
                    }
                    Err(e) => log::error!("{:?}", e),
                }
            } else {
                thread::sleep(Duration::from_millis(6000));
            }
        } else {
            log::error!("scanning connection temporarily lost, retrying...");
            thread::sleep(Duration::from_millis(1000));
        }
    });
}

fn handle_finalized_block(
    connector: &FusoConnector,
    at: u32,
    decoder: &RuntimeDecoder,
    state: &Arc<FusoState>,
    to_seq: &Sender<Input>,
) -> anyhow::Result<()> {
    use hex::ToHex;
    let hash = connector
        .api
        .get_block_hash(Some(at))?
        .ok_or(anyhow!("block {} not ready", at))?;
    let key = connector
        .api
        .metadata
        .storage_value_key("System", "Events")
        .map_err(|e| anyhow!("Read storage failed: {:?}", e))?;
    let payload = connector
        .api
        .get_opaque_storage_by_key_hash(key, Some(hash))?;
    let events = decoder
        .decode_events(&mut payload.unwrap_or(vec![]).as_slice())
        .unwrap_or(vec![]);
    for (_, event) in events.into_iter() {
        if let Raw::Event(raw) = event {
            match (raw.pallet.as_ref(), raw.variant.as_ref()) {
                ("Verifier", "TokenHosted") => {
                    let decoded = TokenHostedEvent::decode(&mut &raw.data[..])?;
                    if decoded.dominator == connector.get_pubkey() {
                        let mut cmd = Command::default();
                        cmd.cmd = crate::cmd::TRANSFER_IN;
                        cmd.currency = Some(decoded.token_id);
                        cmd.amount = to_decimal_represent(decoded.amount);
                        cmd.user_id = Some(format!("{}", decoded.fund_owner));
                        cmd.block_number = Some(at);
                        cmd.extrinsic_hash = Some(hash.encode_hex());
                        let _ = to_seq.send(Input::new(cmd));
                    }
                }
                ("Verifier", "TokenRevoked") => {
                    let decoded = TokenHostedEvent::decode(&mut &raw.data[..])?;
                    if decoded.dominator == connector.get_pubkey() {
                        let mut cmd = Command::default();
                        cmd.cmd = crate::cmd::TRANSFER_OUT;
                        cmd.currency = Some(decoded.token_id);
                        cmd.amount = to_decimal_represent(decoded.amount);
                        cmd.user_id = Some(format!("{}", decoded.fund_owner));
                        cmd.block_number = Some(at);
                        cmd.extrinsic_hash = Some(hash.encode_hex());
                        let _ = to_seq.send(Input::new(cmd));
                    }
                }
                ("Token", "TokenIssued") => {
                    let decoded = TokenIssuedEvent::decode(&mut &raw.data[..])?;
                    let key = connector
                        .api
                        .metadata
                        .storage_map_key::<u32>("Token", "Tokens", decoded.token_id)
                        .map_err(|e| anyhow!("Read storage failed: {:?}", e))?;
                    let payload = connector
                        .api
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
                    if decoded.dominator == connector.get_pubkey() {
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
                        // useless
                        cmd.fee_times = Some(1);
                        // useless
                        cmd.min_vol = Some(Decimal::from_str("10").unwrap());
                        cmd.enable_market_order = Some(false);
                        let _ = to_seq.send(Input::new(cmd));
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
                    }
                }
                ("Market", "MarketClosed") => {
                    let decoded = MarketClosedEvent::decode(&mut &raw.data[..])?;
                    if decoded.dominator == connector.get_pubkey() {
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
                        // useless
                        cmd.min_vol = Some(Decimal::from_str("10").unwrap());
                        cmd.enable_market_order = Some(false);
                        let _ = to_seq.send(Input::new(cmd));
                    }
                }
                _ => {}
            }
        }
    }
    Ok(())
}
