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

use crate::core::Symbol;
use crate::db::DB;
use mysql::prelude::*;

pub fn create_mysql_table(symbol: Symbol) -> anyhow::Result<()> {
    let sql_cr = format!(
        "create table if not exists t_clearing_result_{}_{} like t_clearing_result",
        symbol.0, symbol.1
    );
    let sql_order = format!(
        "create table if not exists t_order_{}_{} like t_order",
        symbol.0, symbol.1
    );
    let sql_stick = format!(
        "create table if not exists t_stick_{}_{} like t_stick",
        symbol.0, symbol.1
    );
    let mut conn = DB
        .get_conn()
        .map_err(|_| anyhow::anyhow!("mysql not available"))?;
    conn.query_drop(sql_cr)?;
    conn.query_drop(sql_order)?;
    conn.query_drop(sql_stick)?;
    Ok(())
}
