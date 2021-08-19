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
    str::FromStr,
    sync::atomic::{AtomicBool, Ordering},
    sync::mpsc::Sender,
    sync::Arc,
    thread,
    time::{Duration, SystemTime},
};

use mysql::{prelude::*, *};
use rust_decimal::{prelude::Zero, Decimal};
use serde::{Deserialize, Serialize};

use crate::{config::C, core::*, db::DB, event::*, orderbook::AskOrBid};

pub const ASK_LIMIT: u32 = 0;
pub const BID_LIMIT: u32 = 1;
#[allow(dead_code)]
pub const ASK_MARKET: u32 = 2;
#[allow(dead_code)]
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
    #[must_use]
    pub const fn rejected(&self) -> bool {
        self.status == ERROR
    }

    pub fn to_event(&self) -> Option<Event> {
        match self.cmd.cmd {
            ASK_LIMIT => Some(Event::Limit(
                self.id,
                self.cmd.symbol()?,
                UserId::from_str(self.cmd.user_id.as_ref()?).ok()?,
                self.cmd.order_id?,
                self.cmd.price?,
                self.cmd.amount?,
                AskOrBid::Ask,
                self.timestamp,
            )),
            BID_LIMIT => Some(Event::Limit(
                self.id,
                self.cmd.symbol()?,
                UserId::from_str(self.cmd.user_id.as_ref()?).ok()?,
                self.cmd.order_id?,
                self.cmd.price?,
                self.cmd.amount?,
                AskOrBid::Bid,
                self.timestamp,
            )),
            CANCEL => Some(Event::Cancel(
                self.id,
                self.cmd.symbol()?,
                UserId::from_str(self.cmd.user_id.as_ref()?).ok()?,
                self.cmd.order_id?,
                self.timestamp,
            )),
            CANCEL_ALL => Some(Event::CancelAll(
                self.id,
                self.cmd.symbol()?,
                self.timestamp,
            )),
            OPEN => Some(Event::Open(self.id, self.cmd.symbol()?, self.timestamp)),
            CLOSE => Some(Event::Close(self.id, self.cmd.symbol()?, self.timestamp)),
            OPEN_ALL => Some(Event::OpenAll(self.id, self.timestamp)),
            CLOSE_ALL => Some(Event::CloseAll(self.id, self.timestamp)),
            TRANSFER_OUT => Some(Event::TransferOut(
                self.id,
                UserId::from_str(self.cmd.user_id.as_ref()?).ok()?,
                self.cmd.currency?,
                self.cmd.amount?,
                self.timestamp,
            )),
            TRANSFER_IN => Some(Event::TransferIn(
                self.id,
                UserId::from_str(self.cmd.user_id.as_ref()?).ok()?,
                self.cmd.currency?,
                self.cmd.amount?,
                self.timestamp,
            )),
            NEW_SYMBOL => Some(Event::NewSymbol(
                self.id,
                self.cmd.symbol()?,
                self.cmd.base_scale?,
                self.cmd.quote_scale?,
                self.cmd.taker_fee?,
                self.cmd.maker_fee?,
                self.cmd.min_amount?,
                self.cmd.min_vol?,
                self.cmd.enable_market_order?,
                self.timestamp,
            )),
            UPDATE_SYMBOL => Some(Event::UpdateSymbol(
                self.id,
                self.cmd.symbol()?,
                self.cmd.base_scale?,
                self.cmd.quote_scale?,
                self.cmd.taker_fee?,
                self.cmd.maker_fee?,
                self.cmd.min_amount?,
                self.cmd.min_vol?,
                self.cmd.enable_market_order?,
                self.timestamp,
            )),
            DUMP => Some(Event::Dump(self.id, self.timestamp)),
            _ => unreachable!(),
        }
    }

    pub const fn new_dump_sequence(at: u64, timestamp: u64) -> Self {
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
                base_scale: None,
                quote_scale: None,
                taker_fee: None,
                maker_fee: None,
                min_amount: None,
                min_vol: None,
                enable_market_order: None,
                from: None,
                exclude: None,
            },
            status: 0,
            timestamp,
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
    pub fn to_inspection(&self) -> Option<Inspection> {
        match self.cmd.cmd {
            QUERY_ORDER => Some(Inspection::QueryOrder(
                self.cmd.symbol()?,
                self.cmd.order_id?,
                self.session,
                self.req_id,
            )),
            QUERY_BALANCE => Some(Inspection::QueryBalance(
                UserId::from_str(self.cmd.user_id.as_ref()?).ok()?,
                self.cmd.currency?,
                self.session,
                self.req_id,
            )),
            QUERY_ACCOUNTS => Some(Inspection::QueryAccounts(
                UserId::from_str(self.cmd.user_id.as_ref()?).ok()?,
                self.session,
                self.req_id,
            )),
            UPDATE_DEPTH => Some(Inspection::UpdateDepth),
            CONFIRM_ALL => Some(Inspection::ConfirmAll(self.cmd.from?, self.cmd.exclude?)),
            _ => unreachable!(),
        }
    }

    pub const fn new_update_depth_watch() -> Self {
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
                base_scale: None,
                quote_scale: None,
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

    pub const fn new_confirm_watch(from: u64, exclude: u64) -> Self {
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
                base_scale: None,
                quote_scale: None,
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
    pub(crate) user_id: Option<String>,
    base: Option<u32>,
    quote: Option<u32>,
    pub(crate) currency: Option<u32>,
    vol: Option<Decimal>,
    pub(crate) amount: Option<Amount>,
    price: Option<Price>,
    base_scale: Option<u32>,
    quote_scale: Option<u32>,
    taker_fee: Option<Fee>,
    maker_fee: Option<Fee>,
    min_amount: Option<Amount>,
    min_vol: Option<Amount>,
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
        Some((self.base?, self.quote?))
    }

    #[must_use]
    pub const fn is_read(&self) -> bool {
        matches!(self.cmd, QUERY_ACCOUNTS | QUERY_BALANCE | QUERY_ORDER)
    }

    /// ONLY CHECK DATA FORMAT!!!
    #[must_use]
    pub fn validate(&self) -> bool {
        match self.cmd {
            ASK_LIMIT | BID_LIMIT => match (
                self.user_id.as_ref(),
                self.order_id,
                self.price,
                self.amount,
            ) {
                (Some(user_id), Some(_), Some(price), Some(amount)) => {
                    price.is_sign_positive()
                        && amount.is_sign_positive()
                        && UserId::from_str(user_id).is_ok()
                        && amount < max_support_number()
                        && price < max_support_number()
                        && amount
                            .checked_mul(price)
                            .map_or(false, |r| r < max_support_number())
                }
                _ => false,
            },
            CANCEL => match (self.symbol(), self.user_id.as_ref(), self.order_id) {
                (Some(_), Some(user_id), Some(_)) => UserId::from_str(user_id).is_ok(),
                _ => false,
            },
            CANCEL_ALL => self.symbol().is_some(),
            OPEN | CLOSE => self.symbol().is_some(),
            OPEN_ALL | CLOSE_ALL => true,
            TRANSFER_OUT | TRANSFER_IN => match (self.user_id.as_ref(), self.currency, self.amount)
            {
                (Some(user_id), Some(_), Some(_)) => UserId::from_str(user_id).is_ok(),
                _ => false,
            },
            NEW_SYMBOL | UPDATE_SYMBOL => {
                match (
                    self.base_scale,
                    self.quote_scale,
                    self.taker_fee,
                    self.maker_fee,
                    self.min_amount,
                    self.min_vol,
                    self.enable_market_order,
                ) {
                    (
                        Some(_base_scale),
                        Some(_quote_scale),
                        Some(taker_fee),
                        Some(maker_fee),
                        Some(min_amount),
                        Some(min_vol),
                        Some(_enable_market_order),
                    ) => {
                        min_amount.is_sign_positive()
                            && min_vol.is_sign_positive()
                            // taker_fee must be positive or zero, while maker_fee can be zero or negative
                            && taker_fee >= Decimal::zero()
                            // taker_fee + maker_fee must be positive or zero
                            && taker_fee + maker_fee >= Decimal::zero()
                            // taker_fee >= maker_fee
                            && taker_fee >= maker_fee
                    }
                    _ => false,
                }
            }
            QUERY_ORDER => self.symbol().is_some() && self.order_id.is_some(),
            QUERY_BALANCE => match (self.currency, self.user_id.as_ref()) {
                (Some(_), Some(user_id)) => UserId::from_str(user_id).is_ok(),
                _ => false,
            },
            QUERY_ACCOUNTS => match self.user_id.as_ref() {
                Some(user_id) => UserId::from_str(user_id).is_ok(),
                _ => false,
            },
            DUMP | UPDATE_DEPTH | CONFIRM_ALL => true,
            _ => false,
        }
    }
}

pub fn init(sender: Sender<Fusion>, id: u64, startup: Arc<AtomicBool>) {
    let mut id = id;
    let mut counter = 0_usize;
    let event_sender = sender.clone();
    thread::spawn(move || loop {
        let seq = fetch_sequence_from(id);
        if seq.is_empty() {
            startup.store(true, Ordering::Relaxed);
            thread::sleep(Duration::from_millis(C.sequence.fetch_intervel_ms));
        } else {
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
                .unwrap_or_else(|_| serde_json::from_str(r#"{"cmd":999999}"#).unwrap()),
            status: f_status,
            timestamp: f_timestamp,
        },
    )
    .unwrap_or_default()
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

#[test]
pub fn test_deserialize_cmd() {
    let transfer_in = r#"{"currency":100, "amount":"100.0", "user_id":"0000000000000000000000000000000000000000000000000000000000000001", "cmd":11}"#;
    let e = serde_json::from_str::<Command>(transfer_in).unwrap();
    assert!(e.validate());
    let bid_limit = r#"{"quote":100, "base":101, "cmd":1, "price":"10.0", "amount":"0.5", "order_id":1, "user_id":"0000000000000000000000000000000000000000000000000000000000000001"}"#;
    let e = serde_json::from_str::<Command>(bid_limit).unwrap();
    assert!(e.validate());
}
