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

use crate::{context::Context, db};
use galois_engine::core::*;
use jsonrpsee::RpcModule;
use parity_scale_codec::{Decode, Encode};
use serde::{Deserialize, Serialize};

pub fn export_rpc(context: Context) -> RpcModule<Context> {
    let mut module = RpcModule::new(context);
    module
        .register_async_method("query_pending_orders", |p, ctx| async move {
            let (symbol, user_id, signature, nonce) =
                p.parse::<(String, String, String, String)>()?;
            // TODO
            // let (key, nonce_on_server) = ctx.get_trading_key(&user_id).await?;
            let symbol = crate::hexstr_to_vec(&symbol)?;
            let signature = crate::hexstr_to_vec(&signature)?;
            let nonce = crate::hexstr_to_vec(&nonce)?;
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
            // TODO
            // let (key, nonce_on_server) = ctx.get_trading_key(&user_id).await?;
            let signature = crate::hexstr_to_vec(&signature)?;
            let nonce = crate::hexstr_to_vec(&nonce)?;
            ctx.backend
                .get_account(&user_id)
                .await
                .map_err(|e| e.into())
        })
        .unwrap();
    module
        .register_async_method("trade", |p, ctx| async move {
            let (cmd, user_id, signature, nonce, relayer) =
                p.parse::<(String, String, String, String, String)>()?;
            // TODO
            // let (key, nonce_on_server) = ctx.get_trading_key(&user_id).await?;
            let signature = crate::hexstr_to_vec(&signature)?;
            let nonce = crate::hexstr_to_vec(&nonce)?;
            let h = crate::hexstr_to_vec(&cmd)?;
            let cmd = TradingCommand::decode(&mut h.as_slice())
                .map_err(|_| anyhow::anyhow!("Invalid command"))?;
            ctx.validate_cmd(&cmd).await?;
            db::save_trading_command(&ctx.db, cmd, &relayer)
                .await
                .map_err(|e| e.into())
        })
        .unwrap();
    module
        .register_async_method("save_trading_key", |p, ctx| async move {
            let (user_id, encrypted_key, sr25519_sig) = p.parse::<(String, String, String)>()?;
            Ok(())
        })
        .unwrap();
    module
        .register_subscription("sub_trading", "", "unsub_trading", |p, mut sink, ctx| {
            let (user_id, signature, nonce) = p.parse::<(String, String, String)>()?;
            let signature = crate::hexstr_to_vec(&signature)?;
            let nonce = crate::hexstr_to_vec(&nonce)?;
            tokio::spawn(async move {});
            Ok(())
        })
        .unwrap();
    module
        .register_subscription("sub_balance", "", "unsub_balance", |p, mut sink, ctx| {
            let (user_id, signature, nonce) = p.parse::<(String, String, String)>()?;
            let signature = crate::hexstr_to_vec(&signature)?;
            let nonce = crate::hexstr_to_vec(&nonce)?;
            tokio::spawn(async move {});
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
}
