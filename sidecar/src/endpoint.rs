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
    db::{self, Order},
};
use galois_engine::core::*;
use jsonrpsee::RpcModule;
use parity_scale_codec::{Decode, Encode};
use rand::Rng;
use serde::{Deserialize, Serialize};

pub fn export_rpc(context: Context) -> RpcModule<Context> {
    let mut module = RpcModule::new(context);
    module
        .register_async_method("query_pending_orders", |p, ctx| async move {
            let (symbol, user_id, signature, nonce) =
                p.parse::<(String, String, String, String)>()?;
            let symbol = crate::hexstr_to_vec(&symbol)?;
            let signature = crate::hexstr_to_vec(&signature)?;
            let nonce = crate::hexstr_to_vec(&nonce)?;
            ctx.verify_trading_signature(&symbol, &user_id, &signature, &nonce)
                .await?;
            let symbol = Symbol::decode(&mut symbol.as_slice())
                .map_err(|_| anyhow::anyhow!("invalid symbol"))?;
            db::query_pending_orders(&ctx.db, symbol, &user_id)
                .await
                .map_err(|e| e.into())
        })
        .unwrap();
    module
        .register_async_method("query_account", |p, ctx| async move {
            let (user_id, signature, nonce) = p.parse::<(String, String, String)>()?;
            let signature = crate::hexstr_to_vec(&signature)?;
            let nonce = crate::hexstr_to_vec(&nonce)?;
            ctx.verify_trading_signature(&[], &user_id, &signature, &nonce)
                .await?;
            ctx.backend
                .get_account(&user_id)
                .await
                .map_err(|e| e.into())
        })
        .unwrap();
    module
        .register_async_method("trade", |p, ctx| async move {
            let (cmd, signature, nonce, relayer) = p.parse::<(String, String, String, String)>()?;
            let signature = crate::hexstr_to_vec(&signature)?;
            let nonce = crate::hexstr_to_vec(&nonce)?;
            let hex = crate::hexstr_to_vec(&cmd)?;
            let cmd = TradingCommand::decode(&mut hex.clone().as_slice())
                .map_err(|_| anyhow::anyhow!("Invalid command"))?;
            let account = cmd.get_from_owned();
            ctx.verify_trading_signature(&hex, &account, &signature, &nonce)
                .await?;
            ctx.validate_cmd(&cmd).await?;
            db::save_trading_command(&ctx.db, cmd, &relayer)
                .await
                .map_err(|e| e.into())
        })
        .unwrap();
    module
        .register_async_method("register_trading_key", |p, ctx| async move {
            let (user_id, user_x25519_pub, sr25519_sig) = p.parse::<(String, String, String)>()?;
            let user_x25519_pub = crate::hexstr_to_vec(&user_x25519_pub)?;
            let raw_sig = crate::hexstr_to_vec(&sr25519_sig)?;
            crate::verify_sr25519(raw_sig, &user_x25519_pub, &user_id)?;
            let user_x25519_pub: [u8; 32] = user_x25519_pub
                .try_into()
                .map_err(|_| anyhow::anyhow!("Invalid public key"))?;
            let user_x25519_pub = x25519_dalek::PublicKey::from(user_x25519_pub);
            let key = ctx.x25519.diffie_hellman(&user_x25519_pub).to_bytes();
            let key = format!("0x{}", hex::encode(&key));
            db::save_trading_key(&ctx.db, &user_id, &key).await?;
            let init_nonce = rand::thread_rng().gen_range(1..10000);
            ctx.session_nonce.insert(user_id, Session::new(init_nonce));
            Ok(init_nonce + 1)
        })
        .unwrap();
    module
        .register_async_method("get_nonce", |p, ctx| async move {
            let user_id = p.parse::<String>()?;
            ctx.get_user_nonce(&user_id).await.map_err(|e| e.into())
        })
        .unwrap();
    module
        .register_subscription("sub_trading", "", "unsub_trading", |p, mut sink, ctx| {
            let (user_id, signature, nonce) = p.parse::<(String, String, String)>()?;
            let signature = crate::hexstr_to_vec(&signature)?;
            let nonce = crate::hexstr_to_vec(&nonce)?;
            futures::executor::block_on(async {
                ctx.verify_trading_signature(&[], &user_id, &signature, &nonce)
                    .await
            })?;
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            tokio::spawn(async move {
                loop {
                    if let Some(msg) = rx.recv().await {
                        let v = hex::encode(&Order::encode(&msg));
                        match sink.send(&v) {
                            Ok(true) => {}
                            Ok(false) => break,
                            Err(e) => {
                                log::error!("Unable to serialize the msg into jsonrpc, {:?}", e)
                            }
                        }
                    } else {
                        break;
                    }
                }
            });
            ctx.subscribers.insert(user_id, tx);
            Ok(())
        })
        .unwrap();
    module
}

#[derive(Eq, PartialEq, Clone, Encode, Decode, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TradingCommand {
    Ask {
        account_id: String,
        base: u32,
        quote: u32,
        amount: String,
        price: String,
    },
    Bid {
        account_id: String,
        base: u32,
        quote: u32,
        amount: String,
        price: String,
    },
    Cancel {
        account_id: String,
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

    pub fn get_from_owned(&self) -> String {
        match self {
            TradingCommand::Ask { account_id, .. } => account_id.clone(),
            TradingCommand::Bid { account_id, .. } => account_id.clone(),
            TradingCommand::Cancel { account_id, .. } => account_id.clone(),
        }
    }
}
