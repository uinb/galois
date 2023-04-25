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

use crate::endpoint::TradingCommand;
use galois_engine::{cmd::CANCEL, core::*, input::Command};
use parity_scale_codec::Encode;
use rust_decimal::{prelude::ToPrimitive, Decimal};
use serde::{Deserialize, Serialize};
use sqlx::types::chrono::{DateTime, Local, NaiveDateTime};
use sqlx::{MySql, Pool, Row};
use std::str::FromStr;

#[derive(Clone, Debug, Eq, PartialEq, sqlx::FromRow)]
pub struct DbOrder {
    pub f_id: u64,
    pub f_version: u64,
    pub f_user_id: String,
    pub f_amount: Decimal,
    pub f_price: Decimal,
    pub f_order_type: u16,
    pub f_timestamp: NaiveDateTime,
    pub f_status: u16,
    pub f_base_fee: Decimal,
    pub f_quote_fee: Decimal,
    pub f_last_cr: u64,
    pub f_matched_quote_amount: Decimal,
    pub f_matched_base_amount: Decimal,
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq, sqlx::FromRow)]
pub struct TradingKey {
    pub f_user_id: String,
    pub f_trading_key: String,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ClearingResult {
    pub f_id: u64,
    pub f_event_id: u64,
    pub f_order_id: u64,
    pub f_user_id: String,
    pub f_status: u16,
    pub f_role: u16,
    pub f_ask_or_bid: u16,
    pub f_price: Decimal,
    pub f_quote_delta: Decimal,
    pub f_base_delta: Decimal,
    pub f_quote_charge: Decimal,
    pub f_base_charge: Decimal,
    // FIXME it is hard to fix - -
    pub f_timestamp: DateTime<Local>,
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Encode)]
pub struct Order {
    order_id: u64,
    symbol: Symbol,
    direction: u8,
    create_timestamp: u64,
    amount: String,
    price: String,
    status: u16,
    matched_quote_amount: String,
    matched_base_amount: String,
    base_fee: String,
    quote_fee: String,
}

impl From<(Symbol, DbOrder)> for Order {
    fn from((symbol, order): (Symbol, DbOrder)) -> Self {
        Self {
            order_id: order.f_id,
            symbol,
            direction: order.f_order_type.try_into().expect("only 0 and 1;qed"),
            create_timestamp: order.f_timestamp.timestamp().to_u64().unwrap(),
            amount: order.f_amount.to_string(),
            price: order.f_price.to_string(),
            status: order.f_status,
            matched_quote_amount: order.f_matched_quote_amount.to_string(),
            matched_base_amount: order.f_matched_base_amount.to_string(),
            base_fee: order.f_base_fee.to_string(),
            quote_fee: order.f_quote_fee.to_string(),
        }
    }
}

pub async fn query_trading_key(pool: &Pool<MySql>, user_id: &String) -> anyhow::Result<String> {
    let r =
        sqlx::query_as::<_, TradingKey>("select * from t_trading_key where f_user_id=? limit 1")
            .bind(user_id)
            .fetch_one(pool)
            .await?;
    Ok(r.f_trading_key)
}

pub async fn save_trading_key(
    pool: &Pool<MySql>,
    user_id: &String,
    key: &String,
) -> anyhow::Result<()> {
    sqlx::query("replace into t_trading_key(f_user_id,f_trading_key) values(?,?)")
        .bind(user_id)
        .bind(key)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn query_pending_orders(
    pool: &Pool<MySql>,
    symbol: Symbol,
    user_id: &String,
) -> anyhow::Result<Vec<Order>> {
    // TODO limit max orders from a single address
    let sql = format!(
        "select * from t_order_{}_{} where f_user_id=? and f_status in (0,3) limit 1000",
        symbol.0, symbol.1
    );
    let r = sqlx::query_as::<_, DbOrder>(&sql)
        .bind(user_id)
        .fetch_all(pool)
        .await?;
    Ok(r.into_iter()
        .map(|o| (symbol.clone(), o).into())
        .collect::<Vec<_>>())
}

// TODO save the relayer
pub async fn save_trading_command(
    pool: &Pool<MySql>,
    user_id: impl ToString,
    cmd: TradingCommand,
    _relayer: &String,
) -> anyhow::Result<u64> {
    // TODO
    let fix_cmd_signature = "169d796416023558ef5c2580ef38c1c4f43f3c06f76ceab2412e6fc5d486a36eb0a9cb808dd4eb72f6264b4113c1a722479be205edc84d6ac5403d33d09b0087";
    let fix_cmd_nonce = 40020u32;
    let direction = cmd.get_direction_if_trade();
    match cmd {
        TradingCommand::Cancel {
            base,
            quote,
            order_id,
        } => {
            let mut cancel = Command::default();
            cancel.order_id = Some(order_id);
            cancel.base = Some(base);
            cancel.cmd = CANCEL;
            cancel.quote = Some(quote);
            cancel.user_id = Some(user_id.to_string());
            cancel.signature = Some(fix_cmd_signature.to_string());
            cancel.nonce = Some(fix_cmd_nonce);
            sqlx::query("insert into t_sequence(f_cmd) values(?)")
                .bind(serde_json::to_string(&cancel).expect("jsonser;qed"))
                .execute(pool)
                .await?;
            Ok(order_id)
        }
        TradingCommand::Ask {
            base,
            quote,
            amount,
            price,
        }
        | TradingCommand::Bid {
            base,
            quote,
            amount,
            price,
        } => {
            let mut tx = pool.begin().await?;
            let sql = format!("insert into t_order_{}_{}(f_user_id,f_amount,f_price,f_order_type) values(?,?,?,?)",
                base, quote
            );
            sqlx::query(sql.as_str())
                .bind(user_id.to_string())
                .bind(amount.clone())
                .bind(price.clone())
                .bind(direction)
                .execute(&mut tx)
                .await?;
            let result = sqlx::query("select LAST_INSERT_ID() as id")
                .fetch_one(&mut tx)
                .await?;
            let id: u64 = result.get("id");
            let mut place = Command::default();
            place.cmd = direction.expect("ask_or_bid;qed").into();
            place.order_id = Some(id);
            place.base = Some(base);
            place.quote = Some(quote);
            place.signature = Some(fix_cmd_signature.to_string());
            place.user_id = Some(user_id.to_string());
            place.price = Decimal::from_str(&price).ok();
            place.amount = Decimal::from_str(&amount).ok();
            place.nonce = Some(fix_cmd_nonce);

            sqlx::query("insert into t_sequence(f_cmd) values(?)")
                .bind(serde_json::to_string(&place).expect("jsonser;qed"))
                .execute(&mut tx)
                .await?;
            tx.commit().await?;
            Ok(id)
        }
    }
}
