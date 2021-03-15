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


use std::{
    sync::Arc,
    sync::atomic::{AtomicBool, Ordering},
    sync::mpsc::Sender,
    thread,
    time::{Duration, SystemTime},
};

use log;
use mysql::{*, prelude::*};
use rust_decimal::{Decimal, prelude::Zero};
use serde::{Deserialize, Serialize};
use serde_json;

use crate::{config::C, core::*, db::DB, event::*, orderbook::AskOrBid};

pub const ASK_LIMIT: u32 = 0;
pub const BID_LIMIT: u32 = 1;
pub const ASK_MARKET: u32 = 2;
pub const BID_MARKET: u32 = 3;
pub const CANCEL: u32 = 4;
pub const CANCEL_ALL: u32 = 5;
pub const OPEN: u32 = 6;
pub const CLOSE: u32 = 7;
pub const OPEN_ALL: u32 = 8;
pub const CLOSE_ALL: u32 = 9;
pub const TRANSFER_OUT: u32 = 10;
pub const TRANSFER_IN: u32 = 11;
pub const NEW_SYMBOL: u32 = 12;
pub const UPDATE_SYMBOL: u32 = 13;
pub const QUERY_ORDER: u32 = 14;
pub const QUERY_BALANCE: u32 = 15;
pub const QUERY_ACCOUNTS: u32 = 16;
pub const DUMP: u32 = 17;
pub const UPDATE_DEPTH: u32 = 18;
pub const CONFIRM_ALL: u32 = 19;

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct Sequence {
    pub id: u64,
    pub cmd: Command,
    pub status: u32,
    pub timestamp: u64,
}

impl Sequence {
    pub fn rejected(&self) -> bool {
        self.status == ERROR
    }

    pub fn to_event(&self) -> Event {
        match self.cmd.cmd {
            ASK_LIMIT => Event::Limit(
                self.id,
                self.cmd.symbol().unwrap(),
                self.cmd.user_id.unwrap(),
                self.cmd.order_id.unwrap(),
                self.cmd.price.unwrap(),
                self.cmd.amount.unwrap(),
                AskOrBid::Ask,
                self.timestamp,
            ),
            BID_LIMIT => Event::Limit(
                self.id,
                self.cmd.symbol().unwrap(),
                self.cmd.user_id.unwrap(),
                self.cmd.order_id.unwrap(),
                self.cmd.price.unwrap(),
                self.cmd.amount.unwrap(),
                AskOrBid::Bid,
                self.timestamp,
            ),
            CANCEL => Event::Cancel(
                self.id,
                self.cmd.symbol().unwrap(),
                self.cmd.user_id.unwrap(),
                self.cmd.order_id.unwrap(),
                self.timestamp,
            ),
            CANCEL_ALL => Event::CancelAll(self.id, self.cmd.symbol().unwrap(), self.timestamp),
            OPEN => Event::Open(self.id, self.cmd.symbol().unwrap(), self.timestamp),
            CLOSE => Event::Close(self.id, self.cmd.symbol().unwrap(), self.timestamp),
            OPEN_ALL => Event::OpenAll(self.id, self.timestamp),
            CLOSE_ALL => Event::CloseAll(self.id, self.timestamp),
            TRANSFER_OUT => Event::TransferOut(
                self.id,
                self.cmd.user_id.unwrap(),
                self.cmd.currency.unwrap(),
                self.cmd.amount.unwrap(),
                self.timestamp,
            ),
            TRANSFER_IN => Event::TransferIn(
                self.id,
                self.cmd.user_id.unwrap(),
                self.cmd.currency.unwrap(),
                self.cmd.amount.unwrap(),
                self.timestamp,
            ),
            NEW_SYMBOL => Event::NewSymbol(
                self.id,
                self.cmd.symbol().unwrap(),
                self.cmd.base_precision.unwrap(),
                self.cmd.quote_precision.unwrap(),
                self.cmd.taker_fee.unwrap(),
                self.cmd.maker_fee.unwrap(),
                self.cmd.min_amount.unwrap(),
                self.cmd.min_vol.unwrap(),
                self.cmd.enable_market_order.unwrap(),
                self.timestamp,
            ),
            UPDATE_SYMBOL => Event::UpdateSymbol(
                self.id,
                self.cmd.symbol().unwrap(),
                self.cmd.base_precision.unwrap(),
                self.cmd.quote_precision.unwrap(),
                self.cmd.taker_fee.unwrap(),
                self.cmd.maker_fee.unwrap(),
                self.cmd.min_amount.unwrap(),
                self.cmd.min_vol.unwrap(),
                self.cmd.enable_market_order.unwrap(),
                self.timestamp,
            ),
            DUMP => Event::Dump(self.id, self.timestamp),
            _ => unreachable!(),
        }
    }

    pub fn new_dump_sequence(at: u64, timestamp: u64) -> Self {
        Self {
            id: at,
            cmd: Command {
                cmd: DUMP,
                order_id: None,
                user_id: None,
                base: None,
                quote: None,
                currency: None,
                vol: None,
                amount: None,
                price: None,
                base_precision: None,
                quote_precision: None,
                taker_fee: None,
                maker_fee: None,
                min_amount: None,
                min_vol: None,
                enable_market_order: None,
                from: None,
                exclude: None,
            },
            status: 0,
            timestamp: timestamp,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct Watch {
    pub session: u64,
    pub req_id: u64,
    pub cmd: Command,
}

impl Watch {
    pub fn to_inspection(&self) -> Inspection {
        match self.cmd.cmd {
            QUERY_ORDER => Inspection::QueryOrder(
                self.cmd.symbol().unwrap(),
                self.cmd.order_id.unwrap(),
                self.session,
                self.req_id,
            ),
            QUERY_BALANCE => Inspection::QueryBalance(
                self.cmd.user_id.unwrap(),
                self.cmd.currency.unwrap(),
                self.session,
                self.req_id,
            ),
            QUERY_ACCOUNTS => {
                Inspection::QueryAccounts(self.cmd.user_id.unwrap(), self.session, self.req_id)
            }
            UPDATE_DEPTH => Inspection::UpdateDepth,
            CONFIRM_ALL => {
                Inspection::ConfirmAll(self.cmd.from.unwrap(), self.cmd.exclude.unwrap())
            }
            _ => unreachable!(),
        }
    }

    pub fn new_update_depth_watch() -> Self {
        Self {
            session: 0,
            req_id: 0,
            cmd: Command {
                cmd: UPDATE_DEPTH,
                order_id: None,
                user_id: None,
                base: None,
                quote: None,
                currency: None,
                vol: None,
                amount: None,
                price: None,
                base_precision: None,
                quote_precision: None,
                taker_fee: None,
                maker_fee: None,
                min_amount: None,
                min_vol: None,
                enable_market_order: None,
                from: None,
                exclude: None,
            },
        }
    }

    pub fn new_confirm_watch(from: u64, exclude: u64) -> Self {
        Self {
            session: 0,
            req_id: 0,
            cmd: Command {
                cmd: CONFIRM_ALL,
                order_id: None,
                user_id: None,
                base: None,
                quote: None,
                currency: None,
                vol: None,
                amount: None,
                price: None,
                base_precision: None,
                quote_precision: None,
                taker_fee: None,
                maker_fee: None,
                min_amount: None,
                min_vol: None,
                enable_market_order: None,
                from: Some(from),
                exclude: Some(exclude),
            },
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct Command {
    pub cmd: u32,
    order_id: Option<u64>,
    pub(crate) user_id: Option<u64>,
    base: Option<u32>,
    quote: Option<u32>,
    pub(crate) currency: Option<u32>,
    vol: Option<Decimal>,
    pub(crate) amount: Option<Decimal>,
    price: Option<Decimal>,
    base_precision: Option<u32>,
    quote_precision: Option<u32>,
    taker_fee: Option<Decimal>,
    maker_fee: Option<Decimal>,
    min_amount: Option<Decimal>,
    min_vol: Option<Decimal>,
    enable_market_order: Option<bool>,
    from: Option<u64>,
    exclude: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub enum Fusion {
    W(Sequence),
    R(Watch),
}

unsafe impl Send for Sequence {}

unsafe impl Send for Command {}

unsafe impl Send for Fusion {}

impl Command {
    pub fn symbol(&self) -> Option<Symbol> {
        match self.base.is_some() && self.quote.is_some() {
            false => None,
            true => Some((self.base.unwrap(), self.quote.unwrap())),
        }
    }

    pub fn is_read(&self) -> bool {
        match self.cmd {
            QUERY_ACCOUNTS | QUERY_BALANCE | QUERY_ORDER => true,
            _ => false,
        }
    }

    /// ONLY CHECK DATA FORMAT!!!
    pub fn validate(&self) -> bool {
        match self.cmd {
            ASK_LIMIT | BID_LIMIT => {
                self.symbol().is_some()
                    && self.user_id.is_some()
                    && self.order_id.is_some()
                    && self.price.is_some()
                    && self.price.unwrap().is_sign_positive()
                    && self.amount.is_some()
                    && self.amount.unwrap().is_sign_positive()
            }
            CANCEL => self.symbol().is_some() && self.user_id.is_some() && self.order_id.is_some(),
            CANCEL_ALL => self.symbol().is_some(),
            OPEN | CLOSE => self.symbol().is_some(),
            OPEN_ALL | CLOSE_ALL => true,
            TRANSFER_OUT | TRANSFER_IN => {
                self.user_id.is_some() && self.currency.is_some() && self.amount.is_some()
            }
            NEW_SYMBOL | UPDATE_SYMBOL => {
                self.symbol().is_some()
                    && self.base_precision.is_some()
                    && self.quote_precision.is_some()
                    && self.taker_fee.is_some()
                    && self.maker_fee.is_some()
                    && self.min_amount.is_some()
                    && self.min_amount.unwrap().is_sign_positive()
                    && self.min_vol.is_some()
                    && self.min_vol.unwrap().is_sign_positive()
                    && self.enable_market_order.is_some()
                    // taker_fee must be positive or zero, while maker_fee can be zero or negative
                    && self.taker_fee.unwrap() >= Decimal::zero()
                    // taker_fee + maker_fee must be positive or zero
                    && self.taker_fee.unwrap() + self.maker_fee.unwrap() >= Decimal::zero()
                    // taker_fee >= maker_fee
                    && self.taker_fee.unwrap() >= self.maker_fee.unwrap().abs()
            }
            QUERY_ORDER => self.symbol().is_some() && self.order_id.is_some(),
            QUERY_BALANCE => self.currency.is_some() && self.user_id.is_some(),
            QUERY_ACCOUNTS => self.user_id.is_some(),
            DUMP | UPDATE_DEPTH | CONFIRM_ALL => true,
            _ => false,
        }
    }
}

pub fn init(sender: Sender<Fusion>, id: u64, startup: Arc<AtomicBool>) {
    let mut id = id;
    let mut counter = 0usize;
    let event_sender = sender.clone();
    thread::spawn(move || loop {
        let seq = fetch_sequence_from(id);
        match seq.is_empty() {
            true => {
                startup.store(true, Ordering::Relaxed);
                thread::sleep(Duration::from_millis(C.sequence.fetch_intervel_ms));
            }
            false => {
                let from = if id == 0 { 0 } else { id - 1 };
                for s in seq.into_iter() {
                    // found break point
                    if id != s.id {
                        log::info!("expecting {}, but {} found", id, s.id);
                        let rs = insert_nop(id);
                        match rs {
                            // it means sequence rollback, {id} is void, adjust id = id + 1
                            Some(true) => {
                                id += 1;
                            }
                            // it means sequence commit, abort current batch and retry
                            Some(false) => {}
                            // other error
                            None => {}
                        }
                        break;
                    }
                    if s.rejected() {
                        id += 1;
                        continue;
                    }
                    event_sender.send(Fusion::W(s)).unwrap();
                    counter += 1;
                    if counter >= C.sequence.checkpoint {
                        counter = 0;
                        let t = SystemTime::now()
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .unwrap()
                            .as_secs();
                        event_sender
                            .send(Fusion::W(Sequence::new_dump_sequence(id, t)))
                            .unwrap();
                    }
                    id += 1;
                }
                event_sender
                    .send(Fusion::R(Watch::new_confirm_watch(from, id)))
                    .unwrap();
            }
        }
    });
    thread::spawn(move || loop {
        thread::sleep(Duration::from_millis(500));
        let watch = Watch::new_update_depth_watch();
        sender.send(Fusion::R(watch)).unwrap();
    });
}

fn fetch_sequence_from(id: u64) -> Vec<Sequence> {
    let sql = "SELECT f_id,f_cmd,f_status,UNIX_TIMESTAMP(f_timestamp) as f_timestamp FROM t_sequence WHERE f_id>=? LIMIT ?";
    let conn = DB.get_conn();
    if conn.is_err() {
        log::error!("retrieve mysql connection failed while fetch_sequence");
        return vec![];
    }
    let mut conn = conn.unwrap();
    conn.exec_map(
        sql,
        (id, C.sequence.batch_size),
        |(f_id, f_cmd, f_status, f_timestamp): (u64, String, u32, u64)| Sequence {
            id: f_id,
            cmd: serde_json::from_str(&f_cmd)
                .unwrap_or(serde_json::from_str(r#"{"cmd":999999}"#).unwrap()),
            status: f_status,
            timestamp: f_timestamp,
        },
    )
        .unwrap_or(vec![])
}

pub fn insert_nop(id: u64) -> Option<bool> {
    let sql = "INSERT INTO t_sequence(f_id,f_cmd,f_status) VALUES(?,?,?)";
    let conn = DB.get_conn();
    if conn.is_err() {
        log::error!("retrieve mysql connection failed while insert_nop");
        return None;
    }
    let mut conn = conn.unwrap();
    match conn.exec_drop(sql, (id, r#"{"cmd":999999}"#, ERROR)) {
        Ok(()) => Some(true),
        Err(err) => {
            if let mysql::error::Error::MySqlError(e) = err {
                // FIXME better way to determine duplicated entry
                if e.code == 1062 && e.message.contains("Duplicate entry") {
                    return Some(false);
                }
            }
            None
        }
    }
}

pub fn update_sequence_status(id: u64, status: u32) {
    let sql = "UPDATE t_sequence SET f_status=? WHERE f_id=?";
    let conn = DB.get_conn();
    if conn.is_err() {
        log::error!("retrieve mysql connection failed while update_sequence_status");
        return;
    }
    let mut conn = conn.unwrap();
    let _ = conn.exec_drop(sql, (status, id));
}

pub fn confirm(from: u64, exclude: u64) {
    let sql = "UPDATE t_sequence SET f_status=? WHERE f_status=? AND f_id>=? AND f_id<?";
    let conn = DB.get_conn();
    if conn.is_err() {
        log::error!("retrieve mysql connection failed while update_sequence_status");
        return;
    }
    let mut conn = conn.unwrap();
    let _ = sql.with((ACCEPTED, PENDING, from, exclude)).run(&mut conn);
}

pub const PENDING: u32 = 0;
pub const ACCEPTED: u32 = 1;
pub const ERROR: u32 = 2;
