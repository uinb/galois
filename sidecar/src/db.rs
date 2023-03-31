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
use galois_engine::{core::*, input::Command};
use parity_scale_codec::Encode;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::{MySql, Pool, Row};
use std::str::FromStr;

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq, sqlx::FromRow)]
pub struct DbOrder {
    pub f_id: u64,
    pub f_version: u64,
    pub f_user_id: String,
    pub f_amount: String,
    pub f_price: String,
    pub f_order_type: u16,
    pub f_timestamp: u64,
    pub f_status: u8,
    pub f_base_fee: String,
    pub f_quote_fee: String,
    pub f_last_cr: u64,
    pub f_matched_quote_amount: String,
    pub f_matched_base_amount: String,
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq, sqlx::FromRow)]
pub struct TradingKey {
    pub f_user_id: String,
    pub f_trading_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct ClearingResult {
    pub f_id: u64,
    pub f_event_id: u64,
    pub f_order_id: u64,
    pub f_user_id: String,
    pub f_status: u8,
    pub f_role: u8,
    pub f_ask_or_bid: u8,
    pub f_price: String,
    pub f_quote_delta: String,
    pub f_base_delta: String,
    pub f_quote_charge: String,
    pub f_base_charge: String,
    pub f_timestamp: u64,
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Encode)]
pub struct Order {
    order_id: u64,
    symbol: Symbol,
    direction: u8,
    create_timestamp: u64,
    amount: String,
    price: String,
    status: u8,
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
            create_timestamp: order.f_timestamp,
            amount: order.f_amount,
            price: order.f_price,
            status: order.f_status,
            matched_quote_amount: order.f_matched_quote_amount,
            matched_base_amount: order.f_matched_base_amount,
            base_fee: order.f_base_fee,
            quote_fee: order.f_quote_fee,
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
    cmd: TradingCommand,
    _relayer: &String,
) -> anyhow::Result<u64> {
    let direction = cmd.get_direction_if_trade();
    match cmd {
        TradingCommand::Cancel {
            account_id,
            base,
            quote,
            order_id,
        } => {
            let mut cancel = Command::default();
            cancel.order_id = Some(order_id);
            cancel.base = Some(base);
            cancel.quote = Some(quote);
            cancel.user_id = Some(account_id);
            sqlx::query("insert into t_sequence(f_cmd) values(?)")
                .bind(serde_json::to_string(&cancel).expect("jsonser;qed"))
                .execute(pool)
                .await?;
            Ok(order_id)
        }
        TradingCommand::Ask {
            account_id,
            base,
            quote,
            amount,
            price,
        }
        | TradingCommand::Bid {
            account_id,
            base,
            quote,
            amount,
            price,
        } => {
            let mut tx = pool.begin().await?;
            let result = sqlx::query(
                "insert into t_order(f_user_id,f_amount,f_price,f_order_type) values(?,?,?,?)",
            )
            .bind(account_id.clone())
            .bind(amount.clone())
            .bind(price.clone())
            .bind(direction)
            .fetch_one(&mut tx)
            .await?;
            let id: u64 = result.get("f_id");
            let mut place = Command::default();
            place.cmd = direction.expect("ask_or_bid;qed").into();
            place.order_id = Some(id);
            place.base = Some(base);
            place.quote = Some(quote);
            place.user_id = Some(account_id);
            place.price = Decimal::from_str(&price).ok();
            place.amount = Decimal::from_str(&amount).ok();
            sqlx::query("insert into t_sequence(f_cmd) values(?)")
                .bind(serde_json::to_string(&place).expect("jsonser;qed"))
                .execute(&mut tx)
                .await?;
            tx.commit().await?;
            Ok(id)
        }
    }
}
