// Copyright 2023 UINB Technologies Pte. Ltd.

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

use crate::context::Context;
use jsonrpsee::core::Error;
use jsonrpsee::RpcModule;
use parity_scale_codec::Compact;
use parity_scale_codec::{Decode, Encode};
use serde::{Deserialize, Serialize};
use sp_core::crypto::AccountId32;
use sp_core::H256;

// pub type Signature = H256;
// pub type AccountId = AccountId32;

// const VERIFY_FAILED: &str = "signature check error";

pub fn export_rpc(context: Context) -> RpcModule<Context> {
    let mut module = RpcModule::new(context);
    module
        .register_async_method("queryPendingOrders", |p, ctx| async move {
            let (symbol, user_id, signature, nonce) =
                p.parse::<(String, String, String, String)>()?;
            let (key, nonce_on_server) = ctx.get_trading_key(&user_id).await?;
            let (verified, update) = crate::verify_trading_sig_and_update_nonce(
                &symbol,
                &key,
                &nonce,
                &signature,
                nonce_on_server,
            );
            // TODO
            Ok(())
        })
        .unwrap();
    module
        .register_async_method("queryAccounts", |p, ctx| async move {
            let (user_id, signature, nonce) = p.parse::<(String, String, String)>()?;
            let (key, nonce_on_server) = ctx.get_trading_key(&user_id).await?;
            let (verified, update) = crate::verify_trading_sig_and_update_nonce(
                &"".to_string(),
                &key,
                &nonce,
                &signature,
                nonce_on_server,
            );
            // TODO
            // let r = ctx
            //     .backend
            //     .get_account(user_id)
            //     .await
            //     .ok_or(Error::Custom("Reading accounts failed.".to_string()))?;
            Ok(())
        })
        .unwrap();
    module
        .register_async_method("trade", |p, ctx| async move {
            let (cmd, user_id, signature, nonce) = p.parse::<(String, String, String, String)>()?;
            let (key, nonce_on_server) = ctx.get_trading_key(&user_id).await?;
            let (verified, update) = crate::verify_trading_sig_and_update_nonce(
                &cmd,
                &key,
                &nonce,
                &signature,
                nonce_on_server,
            );
            // if !verified {
            //     return Err(Error::Custom("Invalid signature".to_string()));
            // }
            let mut h = hex::decode(cmd.trim_start_matches("0x"))
                .map_err(|_| Error::Custom("Invalid hex format".to_string()))?;
            let cmd = TradingCommand::decode(&mut h.as_slice())
                .map_err(|_| Error::Custom("SCALE decoding error".to_string()))?;

            Ok(())
        })
        .unwrap();
    module
        .register_async_method("saveTradingKey", |p, ctx| async move {
            let (user_id, encrypted_key, sr25519_sig) = p.parse::<(String, String, String)>()?;
            Ok(())
        })
        .unwrap();
    module
        .register_subscription("sub_trading", "", "unsub_trading", |p, mut sink, ctx| {
            let (user_id, signature, nonce) = p.parse::<(String, String, u32)>()?;
            tokio::spawn(async move {});
            Ok(())
        })
        .unwrap();
    module
        .register_subscription("sub_balance", "", "unsub_balance", |p, mut sink, ctx| {
            let (user_id, currency, signature, nonce) = p.parse::<(String, u32, String, u32)>()?;
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
        order_id: String,
        account_id: String,
    },
}
