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

pub fn migrate(c: crate::config::MigrateCmd) {
    cfg_if::cfg_if! {
        if #[cfg(feature = "v1-to-v2")] {
            use tokio::runtime::Runtime;
            let rt = Runtime::new().unwrap();
            rt.spawn_blocking(|| {
                v1_to_v2::migrate(c);
            });
        } else {
            println!("{:?}", c);
            panic!("The binary doesn't contain the feature, please re-compile with feature `v1-to-v2` to enable");
        }
    }
}

#[cfg(feature = "v1-to-v2")]
mod v1_to_v2 {
    use crate::{config::*, core, input::Command};
    use rust_decimal::Decimal;
    use sqlx::mysql::MySqlConnectOptions;
    use sqlx::{MySql, Pool, Row};
    use std::str::FromStr;

    pub fn migrate(c: MigrateCmd) {
        lazy_static::initialize(&C);
        let input_file = c.input_path;
        let output_file = c.output_path;
        let ignore_sequences = c.core_only;
        let f = std::fs::File::open(input_file).unwrap();
        let data = core::v1::DataV1::from_raw(f).unwrap();
        let option: MySqlConnectOptions = C.mysql.url.parse().unwrap();
        let pool: Pool<MySql> =
            futures::executor::block_on(async move { Pool::connect_with(option).await.unwrap() });
        let mut pendings = vec![];
        let pool = std::sync::Arc::new(pool);
        for (symbol, orderbook) in data.orderbooks.iter() {
            let sql = format!(
                "select * from t_order_{}_{} where f_status in (0, 3) and f_id <= {}",
                symbol.0, symbol.1, orderbook.max_id,
            );
            let s = *symbol;
            let p = pool.clone();
            let r = futures::executor::block_on(async move {
                sqlx::query(&sql)
                    .map(|row: sqlx::mysql::MySqlRow| -> core::PendingOrder {
                        core::PendingOrder {
                            order_id: row.get("f_id"),
                            user_id: core::UserId::from_str(row.get("f_user_id")).unwrap(),
                            symbol: s,
                            direction: row.get("f_order_type"),
                            create_timestamp: row.get("f_timestamp"),
                            amount: Decimal::from_str(row.get("f_amount")).unwrap(),
                            price: Decimal::from_str(row.get("f_price")).unwrap(),
                            status: row.get("f_status"),
                            matched_quote_amount: Decimal::from_str(
                                row.get("f_matched_quote_amount"),
                            )
                            .unwrap(),
                            matched_base_amount: Decimal::from_str(
                                row.get("f_matched_base_amount"),
                            )
                            .unwrap(),
                            base_fee: Decimal::from_str(row.get("f_base_fee")).unwrap(),
                            quote_fee: Decimal::from_str(row.get("f_quote_fee")).unwrap(),
                        }
                    })
                    .fetch_all(p.as_ref())
                    .await
                    .unwrap()
            });
            pendings.extend(r);
        }
        let data: core::Data = (data, pendings).into();
        let event_id = data.current_event_id;
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(output_file)
            .unwrap();
        data.into_raw(file).unwrap();
        if !ignore_sequences {
            let r = futures::executor::block_on(async move {
                let sql = format!(
                    "select f_id,f_cmd,f_status,f_time from t_sequence where f_event_id > {}",
                    event_id
                );
                sqlx::query(&sql)
                    .map(|row: sqlx::mysql::MySqlRow| -> (u64, Command, u8) {
                        let mut cmd: Command = serde_json::from_str(row.get("f_cmd")).unwrap();
                        cmd.timestamp = Some(row.get("f_time"));
                        (row.get("f_id"), cmd, row.get("f_status"))
                    })
                    .fetch_all(pool.as_ref())
                    .await
                    .unwrap()
            });
            for cmd in r {
                if cmd.2 != 2 {
                    crate::sequencer::save(cmd.0, serde_json::to_vec(&cmd.1).unwrap()).unwrap();
                }
            }
        }
    }
}
