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

use dashmap::DashMap;
use galois_engine::core::Symbol;
use jsonrpsee::server::SubscriptionSink;
use sqlx::{MySql, Pool};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

/// we should change this stupid way
pub async fn update_order_task(
    subscribers: Arc<DashMap<String, SubscriptionSink>>,
    pool: Pool<MySql>,
    symbol: Symbol,
    symbol_closed: Arc<AtomicBool>,
) {
    let mut recent_cr = 0u64;
    let fetch_sql = format!(
        "select * from t_clearing_result_{}_{} where f_event_id > ? limit 1000",
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
                    // TODO save and push
                }
            }
            Err(e) => log::error!("fetch clearing results failed, {:?}", e),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, sqlx::FromRow)]
pub struct ClearingResult {
    pub f_event_id: u64,
}
