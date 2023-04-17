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

use crate::db::*;
use dashmap::DashMap;
use galois_engine::core::Symbol;
use rust_decimal::Decimal;
use sqlx::{MySql, Pool};
use std::str::FromStr;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::mpsc::UnboundedSender;

/// let's kill this stupid code asap
pub async fn update_order_task(
    subscribers: Arc<DashMap<String, UnboundedSender<Order>>>,
    pool: Pool<MySql>,
    symbol: Symbol,
    symbol_closed: Arc<AtomicBool>,
) {
    let max_cr_sql = format!(
        "select max(f_id) from t_clearing_result_{}_{}",
        symbol.0, symbol.1
    );
    let mut recent_cr: u64 = sqlx::query_scalar(&max_cr_sql)
        .fetch_one(&pool)
        .await
        .expect("init sql failed");
    let fetch_sql = format!(
        "select * from t_clearing_result_{}_{} where f_event_id > ? limit 1000",
        symbol.0, symbol.1
    );
    let select_sql = format!(
        "select * from t_order_{}_{} where f_id = ?",
        symbol.0, symbol.1
    );
    let update_sql = format!(
        "update t_order_{}_{} set f_version=f_version+1,
f_status=?,
f_base_fee=?,
f_quote_fee=?,
f_last_cr=?,
f_matched_base_amount=?
f_matched_quote_amount=?,
where f_id=? and f_version=?",
        symbol.0, symbol.1
    );
    loop {
        if symbol_closed.load(Ordering::Relaxed) {
            break;
        }
        let v = sqlx::query_as::<_, ClearingResult>(&fetch_sql)
            .bind(recent_cr)
            .fetch_all(&pool)
            .await;
        match v {
            Ok(v) => {
                if v.is_empty() {
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                } else {
                    for clear in v {
                        let order = match sqlx::query_as::<_, DbOrder>(&select_sql)
                            .bind(clear.f_order_id)
                            .fetch_one(&pool)
                            .await
                        {
                            Ok(order) => order,
                            Err(_) => break,
                        };
                        let base_fee = order.f_base_fee + clear.f_base_charge.abs();
                        let quote_fee = order.f_quote_fee + clear.f_quote_charge.abs();
                        let matched_base = order.f_matched_base_amount + clear.f_base_delta.abs();
                        let matched_quote =
                            order.f_matched_quote_amount + clear.f_quote_delta.abs();
                        match sqlx::query(&update_sql)
                            .bind(clear.f_status)
                            .bind(base_fee.to_string())
                            .bind(quote_fee.to_string())
                            .bind(clear.f_id)
                            .bind(matched_base.to_string())
                            .bind(matched_quote.to_string())
                            .bind(order.f_id)
                            .bind(order.f_version)
                            .execute(&pool)
                            .await
                        {
                            Ok(_) => {
                                recent_cr = clear.f_id;
                                let user_id = order.f_user_id.clone();
                                let r = if let Some(u) = subscribers.get(&user_id) {
                                    u.value().send((symbol, order).into())
                                } else {
                                    Ok(())
                                };
                                match r {
                                    Err(_) => {
                                        subscribers.remove(&user_id);
                                    }
                                    Ok(_) => {}
                                }
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
            Err(e) => log::error!("fetch clearing results failed, {:?}", e),
        }
    }
}
