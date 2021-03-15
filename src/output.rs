// Copyright 2021 UINB Technologies Pte. Ltd.

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


use mysql::{prelude::*, *};
use redis;
use redis::Commands;
use rust_decimal::Decimal;
use serde_json;
use std::collections::HashMap;
use std::sync::mpsc::{Receiver, Sender};
use std::thread;
use std::convert::Into;
use std::time::Duration;

use crate::{core::*, db::DB, db::REDIS, matcher::*, orderbook::AskOrBid, orderbook::Depth};

#[derive(Debug)]
pub struct Output {
    pub event_id: u64,
    pub order_id: u64,
    pub user_id: u64,
    pub symbol: Symbol,
    pub state: State,
    pub role: Role,
    pub ask_or_bid: AskOrBid,
    pub price: Decimal,
    pub quote: Decimal,
    pub base: Decimal,
    pub quote_fee: Decimal,
    pub base_fee: Decimal,
    pub timestamp: u64,
}

pub fn write_depth(depth: Vec<Depth>) {
    let redis = REDIS.get_connection();
    match redis {
        Ok(mut conn) => {
            depth.iter().for_each(|d| {
                let _: redis::RedisResult<()> = conn.set(
                    format!("V2_DEPTH_L{}_{}_{}", d.depth, d.symbol.0, d.symbol.1),
                    serde_json::to_string(d).unwrap(),
                );
            });
        }
        Err(_) => {
            log::error!("connect redis failed");
        }
    }
}

pub fn init(sender: Sender<Vec<Output>>, recv: Receiver<Vec<Output>>) {
    let mut buf = HashMap::<Symbol, (u64, Vec<Output>)>::new();
    thread::spawn(move || loop {
        let cr = recv.recv().unwrap();
        if cr.is_empty() {
            flush_all(&mut buf);
        } else {
            write(cr, &mut buf);
        }
    });
    thread::spawn(move || loop {
        thread::sleep(Duration::from_millis(1000));
        sender.send(vec![]).unwrap();
    });
}

fn get_max_record(symbol: &Symbol) -> u64 {
    let sql = format!(
        "SELECT coalesce(MAX(f_event_id), 0) from t_clearing_result_{}_{}",
        symbol.0, symbol.1
    );
    let conn = DB.get_conn();
    if conn.is_err() {
        log::error!("Error: acquire mysql connection failed, {:?}", conn);
        return 0;
    }
    let mut conn = conn.unwrap();
    let id = conn.query_first(sql).unwrap();
    id.or(Some(0)).unwrap()
}

fn flush(symbol: &Symbol, pending: &mut Vec<Output>) {
    let sql = format!(
        "INSERT IGNORE INTO t_clearing_result_{}_{}(f_event_id,f_order_id,f_user_id,f_status,f_role,f_ask_or_bid,f_price,f_quote,f_base,f_quote_fee,f_base_fee,f_timestamp) VALUES (:event_id,:order_id,:user_id,:state,:role,:ask_or_bid,:price,:quote,:base,:quote_fee,:base_fee,FROM_UNIXTIME(:timestamp))",
        symbol.0, symbol.1
    );
    let conn = DB.get_conn();
    if conn.is_err() {
        log::error!("Error: acquire mysql connection failed, {:?}", conn);
        return;
    }
    let mut conn = conn.unwrap();
    let r = conn.exec_batch(
        sql,
        pending.iter().map(|p| {
            params! {
                "event_id" => p.event_id,
                "order_id" => p.order_id,
                "user_id" => p.user_id,
                "state" => p.state.into(): u32,
                "role" => p.role.into(): u32,
                "ask_or_bid" => p.ask_or_bid.into(): u32,
                "price" => p.price,
                "quote" => p.quote,
                "base" => p.base,
                "quote_fee" => p.quote_fee,
                "base_fee" => p.base_fee,
                "timestamp" => p.timestamp,
            }
        }),
    );
    match r {
        Ok(_) => pending.clear(),
        Err(err) => {
            log::error!("Error: writing clearing result to mysql failed, {:?}", err);
        }
    }
}

fn flush_all(buf: &mut HashMap<Symbol, (u64, Vec<Output>)>) {
    for (symbol, pending) in buf.iter_mut() {
        flush(symbol, &mut pending.1);
    }
}

fn write(mut cr: Vec<Output>, buf: &mut HashMap<Symbol, (u64, Vec<Output>)>) {
    let symbol = cr.first().unwrap().symbol;
    let pending = buf.get_mut(&symbol);
    if pending.is_none() {
        buf.insert(symbol, (get_max_record(&symbol), cr));
        return;
    }
    let pending = pending.unwrap();
    let prepare_write_event_id = cr.last().unwrap().event_id;
    if prepare_write_event_id <= pending.0 {
        return;
    }
    pending.0 = prepare_write_event_id;
    pending.1.append(&mut cr);
    if pending.1.len() >= 100 {
        flush(&symbol, &mut pending.1);
    }
}
