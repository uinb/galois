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
use crate::config::C;
use anyhow::anyhow;
use node_api::decoder::{RuntimeDecoder, StorageHasher};
use parity_scale_codec::{Decode, Error as CodecError};
use sp_core::{sr25519::Public, Pair};
use std::sync::atomic::Ordering;
use sub_api::{rpc::WsRpcClient, Hash};

#[derive(Clone)]
pub struct FusoConnector {
    pub api: FusoApi,
    pub signer: Sr25519Key,
}

impl FusoConnector {
    pub fn new() -> anyhow::Result<Self> {
        let signer = Sr25519Key::from_string(&C.fusotao.key_seed, None)
            .map_err(|e| anyhow!("invalid fusotao config: {:?}", e))?;
        let client = WsRpcClient::new(&C.fusotao.node_url);
        let api = FusoApi::new(client)
            .map(|api| api.set_signer(signer.clone()))
            .inspect_err(|e| log::error!("{:?}", e))
            .map_err(|_| anyhow!("fusotao node not available or metadata check failed."))?;
        Ok(Self { api, signer })
    }

    pub fn get_pubkey(&self) -> Public {
        self.signer.public().clone()
    }

    pub fn sync_progress(&self) -> anyhow::Result<u64> {
        let (_, hash) = self.get_finalized_block()?;
        let key = self.api.metadata.storage_map_key::<FusoAccountId>(
            "Verifier",
            "Dominators",
            self.get_pubkey(),
        )?;
        let payload = self
            .api
            .get_opaque_storage_by_key_hash(key, Some(hash))?
            .ok_or(anyhow!("{} isn't the prover", self.get_pubkey()))?;
        let result = Dominator::decode(&mut payload.as_slice())?;
        Ok(result.sequence.0)
    }

    pub fn fully_sync_chain(&self, state: Arc<FusoState>) -> anyhow::Result<Vec<Command>> {
        let (block, hash) = self.get_finalized_block()?;
        let decoder = RuntimeDecoder::new(self.api.metadata.clone());

        // market list, double map AccountId, Symbol -> Market
        let key = self
            .api
            .metadata
            .storage_double_map_partial_key::<FusoAccountId>(
                "Market",
                "Markets",
                &self.get_pubkey(),
            )?;
        let payload = self
            .api
            .get_opaque_storage_pairs_by_key_hash(key, Some(hash))?
            .ok_or(anyhow!(""))?;
        for (k, v) in payload.into_iter() {
            let symbol = RuntimeDecoder::extract_double_map_identifier::<(u32, u32), FusoAccountId>(
                StorageHasher::Blake2_128Concat,
                StorageHasher::Blake2_128Concat,
                &self.get_pubkey(),
                &mut k.as_slice(),
            )?;
            let market = OnchainSymbol::decode(&mut v.as_slice())?;
            state.symbols.insert(symbol, market);
        }

        // token list, map TokenId -> Token
        let key = self
            .api
            .metadata
            .storage_map_key_prefix("Token", "Tokens")?;
        let payload = self
            .api
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
        let key = self
            .api
            .metadata
            .storage_map_key_prefix("Market", "Brokers")?;
        let payload = self.api.get_keys(key, Some(hash))?.ok_or(anyhow!(""))?;
        for k in payload.into_iter() {
            let broker: FusoAccountId = RuntimeDecoder::extract_map_identifier(
                StorageHasher::Blake2_128Concat,
                &mut k.as_slice(),
            )?;
            state.brokers.insert(broker.0.into(), rand::random());
        }

        // pending receipts, double map AccountId, AccountId -> Receipt
        let key = self
            .api
            .metadata
            .storage_double_map_partial_key::<FusoAccountId>(
                "Verifier",
                "Receipts",
                &self.get_pubkey(),
            )?;
        let payload = self
            .api
            .get_opaque_storage_pairs_by_key_hash(key, Some(hash))?
            .ok_or(anyhow!(""))?;
        let mut commands = vec![];
        for (k, v) in payload.into_iter() {
            let user = RuntimeDecoder::extract_double_map_identifier::<FusoAccountId, FusoAccountId>(
                StorageHasher::Blake2_128Concat,
                StorageHasher::Blake2_128Concat,
                &self.get_pubkey(),
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
        if !commands.is_empty() {
            log::info!("pending receipts detected: {:?}", commands);
        }
        state.scanning_progress.store(block + 1, Ordering::Relaxed);
        state.chain_height.store(block, Ordering::Relaxed);
        Ok(commands)
    }

    pub fn get_finalized_block(&self) -> anyhow::Result<(u32, Hash)> {
        let hash = self
            .api
            .get_finalized_head()?
            .ok_or(anyhow!("finalized headers cant be found"))?;
        let block_number = self
            .api
            .get_signed_block(Some(hash))?
            .ok_or(anyhow!("signed block {} can't be found", hash))
            .map(|b: sub_api::SignedBlock<FusoBlock>| b.block.header.number)?;
        Ok((block_number, hash))
    }
}
