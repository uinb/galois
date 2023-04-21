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
use sqlx::{MySql, Pool};
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
        "select CAST(COALESCE(max(f_last_cr),0) as UNSIGNED) from t_order_{}_{}",
        symbol.0, symbol.1
    );
    let mut recent_cr: u64 = sqlx::query_scalar(&max_cr_sql)
        .fetch_one(&pool)
        .await
        .expect("init sql failed");
    let fetch_sql = format!(
        "select * from t_clearing_result_{}_{} where f_id > ? limit 1000",
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
f_matched_base_amount=?,
f_matched_quote_amount=?
where f_id=? and f_version=? and f_last_cr<?",
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
                    log::debug!("found clearing_result:{}", v.len());
                    for clear in v {
                        let mut order = match sqlx::query_as::<_, DbOrder>(&select_sql)
                            .bind(clear.f_order_id)
                            .fetch_one(&pool)
                            .await
                        {
                            Ok(order) => order,
                            Err(_) => break,
                        };
                        order.f_base_fee = order.f_base_fee + clear.f_base_charge.abs();
                        order.f_quote_fee = order.f_quote_fee + clear.f_quote_charge.abs();
                        order.f_matched_base_amount =
                            order.f_matched_base_amount + clear.f_base_delta.abs();
                        order.f_matched_quote_amount =
                            order.f_matched_quote_amount + clear.f_quote_delta.abs();
                        order.f_status = clear.f_status;
                        order.f_last_cr = clear.f_id;
                        match sqlx::query(&update_sql)
                            .bind(order.f_status)
                            .bind(order.f_base_fee)
                            .bind(order.f_quote_fee)
                            .bind(order.f_last_cr)
                            .bind(order.f_matched_base_amount)
                            .bind(order.f_matched_quote_amount)
                            .bind(order.f_id)
                            .bind(order.f_version)
                            .bind(order.f_last_cr)
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
                                    Err(e) => {
                                        log::debug!("send order to channel error: {}", e);
                                        subscribers.remove(&user_id);
                                    }
                                    Ok(_) => {}
                                }
                            }
                            Err(e) => {
                                log::debug!("update order error:{:?}", e);
                                break;
                            }
                        }
                    }
                }
            }
            Err(e) => log::error!("fetch clearing results failed, {:?}", e),
        }
    }
}
