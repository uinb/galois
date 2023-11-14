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

use serde::{Deserialize, Serialize};
use sqlx::{MySql, Pool};

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq, sqlx::FromRow)]
pub struct TradingKey {
    pub f_user_id: String,
    pub f_trading_key: String,
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
