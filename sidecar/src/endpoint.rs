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

use crate::{
    context::{Context, Session},
    db,
};
use galois_engine::core::*;
use jsonrpsee::RpcModule;
use parity_scale_codec::{Decode, Encode};
use rand::Rng;
use serde::{Deserialize, Serialize};
use sp_core::crypto::Ss58Codec;

pub fn export_rpc(context: Context) -> RpcModule<Context> {
    let mut module = RpcModule::new(context);
    module
        .register_async_method("query_pending_orders", |p, ctx| async move {
            let (symbol, user_id, signature, nonce) =
                p.parse::<(String, String, String, String)>()?;
            let user_id = crate::try_into_ss58(user_id)?;
            let symbol = crate::hexstr_to_vec(&symbol)?;
            let signature = crate::hexstr_to_vec(&signature)?;
            let nonce = crate::hexstr_to_vec(&nonce)?;
            ctx.verify_trading_signature(&symbol, &user_id, &signature, &nonce)
                .await
                .map_err(handle_error)?;
            let symbol = Symbol::decode(&mut symbol.as_slice())
                .map_err(|_| anyhow::anyhow!("invalid symbol"))?;
            db::query_pending_orders(&ctx.db, symbol, &user_id)
                .await
                .map(|r| {
                    r.into_iter()
                        .map(|o| crate::to_hexstr(o))
                        .collect::<Vec<_>>()
                })
                .map_err(handle_error)
        })
        .unwrap();
    module
        .register_async_method("query_account", |p, ctx| async move {
            let (user_id, signature, nonce) = p.parse::<(String, String, String)>()?;
            let user_id = crate::try_into_ss58(user_id)?;
            let signature = crate::hexstr_to_vec(&signature)?;
            let nonce = crate::hexstr_to_vec(&nonce)?;
            ctx.verify_trading_signature(&[], &user_id, &signature, &nonce)
                .await
                .map_err(handle_error)?;
            ctx.backend
                .get_account(&user_id)
                .await
                .map(|r| {
                    r.into_iter()
                        .map(|(k, v)| crate::to_hexstr((k, v)))
                        .collect::<Vec<_>>()
                })
                .map_err(handle_error)
        })
        .unwrap();
    module
        .register_async_method("trade", |p, ctx| async move {
            let (user_id, cmd, signature, nonce, relayer) =
                p.parse::<(String, String, String, String, String)>()?;
            let user_id = crate::try_into_ss58(user_id)?;
            let signature = crate::hexstr_to_vec(&signature)?;
            let nonce = crate::hexstr_to_vec(&nonce)?;
            let hex = crate::hexstr_to_vec(&cmd)?;
            let cmd = TradingCommand::decode(&mut hex.clone().as_slice())
                .map_err(|_| anyhow::anyhow!("Invalid command"))?;
            ctx.verify_trading_signature(&hex, &user_id, &signature, &nonce)
                .await
                .map_err(handle_error)?;
            ctx.validate_cmd(&user_id, &cmd)
                .await
                .map_err(handle_error)?;
            ctx.backend
                .submit_trading_command(user_id, cmd, relayer)
                .await
                .map(|id| crate::to_hexstr(id))
                .map_err(handle_error)
        })
        .unwrap();
    module
        .register_async_method("register_trading_key", |p, ctx| async move {
            let (user_id, user_x25519_pub, sig) = p.parse::<(String, String, String)>()?;
            log::debug!(
                "user = {}, x25519 = {}, sign = {} ",
                &user_id,
                &user_x25519_pub,
                &sig
            );
            let user_id = crate::try_into_account(user_id)?;
            let user_x25519_pub_vec = crate::hexstr_to_vec(&user_x25519_pub)?;
            let raw_sig = crate::hexstr_to_vec(&sig)?;
            if raw_sig.len() == 64 {
                let message = format!("<Bytes>{}</Bytes>", user_x25519_pub);
                crate::verify_sr25519(raw_sig, message.into_bytes().as_ref(), &user_id)
                    .map_err(handle_error)?;
            } else {
                crate::verify_ecdsa(
                    raw_sig,
                    &hex::encode(&user_x25519_pub_vec),
                    &user_id.to_ss58check(),
                )
                .map_err(handle_error)?;
            }
            let user_x25519_pub: [u8; 32] = user_x25519_pub_vec
                .try_into()
                .map_err(|_| anyhow::anyhow!("Invalid public key"))?;
            let user_x25519_pub = x25519_dalek::PublicKey::from(user_x25519_pub);
            let key = ctx.x25519.diffie_hellman(&user_x25519_pub).to_bytes();
            let key = format!("0x{}", hex::encode(&key));
            db::save_trading_key(&ctx.db, &user_id.to_ss58check(), &key).await?;
            let init_nonce = rand::thread_rng().gen_range(1..10000);
            ctx.session_nonce
                .insert(user_id.to_ss58check(), Session::new(init_nonce));
            Ok(crate::to_hexstr(init_nonce + 1))
        })
        .unwrap();
    module
        .register_async_method("register_trading_key_for_subaccount", |p, ctx| async move {
            let (user_id, bot_id, token, bot_x25519_pub, sig) =
                p.parse::<(String, String, u32, String, String)>()?;
            log::debug!(
                "user = {}, bot = {}, x25519 = {}, sign = {} ",
                &user_id,
                &bot_id,
                &bot_x25519_pub,
                &sig
            );
            let user_id = crate::try_into_account(user_id)?;
            let bot_id = crate::try_into_account(bot_id)?;
            let sub_id = crate::derive_sub_account(&user_id, &bot_id, token);
            let bot_x25519_pub_vec = crate::hexstr_to_vec(&bot_x25519_pub)?;
            let raw_sig = crate::hexstr_to_vec(&sig)?;
            let message = format!("<Bytes>{}</Bytes>", bot_x25519_pub);
            // the bot account must be sr25519
            crate::verify_sr25519(raw_sig, message.into_bytes().as_ref(), &bot_id)
                .map_err(handle_error)?;
            let bot_x25519_pub: [u8; 32] = bot_x25519_pub_vec
                .try_into()
                .map_err(|_| anyhow::anyhow!("Invalid public key"))?;
            let bot_x25519_pub = x25519_dalek::PublicKey::from(bot_x25519_pub);
            let key = ctx.x25519.diffie_hellman(&bot_x25519_pub).to_bytes();
            let key = format!("0x{}", hex::encode(&key));
            db::save_trading_key(&ctx.db, &sub_id.to_ss58check(), &key).await?;
            let init_nonce = rand::thread_rng().gen_range(1..10000);
            ctx.session_nonce
                .insert(sub_id.to_ss58check(), Session::new(init_nonce));
            Ok(crate::to_hexstr(init_nonce + 1))
        })
        .unwrap();
    module
        .register_async_method("get_nonce", |p, ctx| async move {
            let user_id = p.parse::<(String,)>()?;
            let user_id = crate::try_into_ss58(user_id.0)?;
            ctx.get_user_nonce(&user_id)
                .await
                .map(|n| crate::to_hexstr(n))
                .map_err(handle_error)
        })
        .unwrap();
    module
        .register_async_method("append_user", |p, ctx| async move {
            let (user_id, signature, nonce, relayer) =
                p.parse::<(String, String, String, String)>()?;
            let user_id = crate::try_into_ss58(user_id)?;
            let signature = crate::hexstr_to_vec(&signature)?;
            let nonce = crate::hexstr_to_vec(&nonce)?;
            ctx.verify_trading_signature(&[], &user_id, &signature, &nonce)
                .await
                .map_err(handle_error)?;
            let tx = ctx
                .subscribers
                .get(&format!("broker:{}", relayer))
                .map(|b| b.value().clone())
                .ok_or_else(|| anyhow::anyhow!("Broker not initialized."))?;
            ctx.subscribers.insert(user_id, tx);
            Ok(())
        })
        .unwrap();
    module
        .register_subscription("sub_trading", "", "unsub_trading", |p, mut sink, ctx| {
            let (broker,) = p.parse::<(String,)>()?;
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            sink.accept()?;
            tokio::spawn(async move {
                loop {
                    if let Some((user_id, order)) = rx.recv().await {
                        let v = serde_json::json!({
                            "user_id": user_id,
                            "order": crate::to_hexstr(order),
                        });
                        match sink.send(&v) {
                            Ok(true) => {}
                            Ok(false) => break,
                            Err(e) => log::error!("Unable to serialize msg, {:?}", e),
                        }
                    } else {
                        break;
                    }
                }
            });
            ctx.subscribers.insert(format!("broker:{}", broker), tx);
            Ok(())
        })
        .unwrap();
    module
}

#[derive(Eq, PartialEq, Clone, Encode, Decode, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TradingCommand {
    Ask {
        base: u32,
        quote: u32,
        amount: String,
        price: String,
    },
    Bid {
        base: u32,
        quote: u32,
        amount: String,
        price: String,
    },
    Cancel {
        base: u32,
        quote: u32,
        order_id: u64,
    },
}

impl TradingCommand {
    pub fn get_direction_if_trade(&self) -> Option<u8> {
        match self {
            TradingCommand::Ask { .. } => Some(AskOrBid::Ask.into()),
            TradingCommand::Bid { .. } => Some(AskOrBid::Bid.into()),
            _ => None,
        }
    }
}

fn handle_error(e: anyhow::Error) -> jsonrpsee::core::Error {
    let error = e.downcast::<jsonrpsee::core::Error>();
    match error {
        Ok(e) => e,
        Err(e) => e.into(),
    }
}
