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

use crate::{config::C, core::*, db::DB, event::*, orderbook::AskOrBid};
use anyhow::{anyhow, ensure};
use mysql::{prelude::*, *};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::{
    convert::{TryFrom, TryInto},
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::Sender,
        Arc,
    },
    time::{Duration, SystemTime},
};

pub const ASK_LIMIT: u32 = 0;
pub const BID_LIMIT: u32 = 1;
pub const CANCEL: u32 = 4;
pub const CANCEL_ALL: u32 = 5;
pub const TRANSFER_OUT: u32 = 10;
pub const TRANSFER_IN: u32 = 11;
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

impl TryInto<Event> for Sequence {
    type Error = anyhow::Error;

    fn try_into(self) -> anyhow::Result<Event> {
        match self.cmd.cmd {
            ASK_LIMIT | BID_LIMIT => {
                let amount = self.cmd.amount.ok_or(anyhow!(""))?;
                let price = self.cmd.price.ok_or(anyhow!(""))?;
                ensure!(price.is_sign_positive() && price < max_number(), "");
                ensure!(amount.is_sign_positive() && amount < max_number(), "");
                let vol = amount.checked_mul(price).ok_or(anyhow!(""))?;
                ensure!(vol < max_number(), "");
                let cmd = LimitCmd {
                    symbol: self.cmd.symbol().ok_or(anyhow!(""))?,
                    user_id: UserId::from_str(self.cmd.user_id.as_ref().ok_or(anyhow!(""))?)?,
                    order_id: self.cmd.order_id.ok_or(anyhow!(""))?,
                    price: price,
                    amount: amount,
                    ask_or_bid: AskOrBid::try_from(self.cmd.cmd)?,
                    #[cfg(feature = "fusotao")]
                    nonce: self.cmd.nonce.ok_or(anyhow!(""))?,
                    #[cfg(feature = "fusotao")]
                    signature: hex::decode(self.cmd.signature.ok_or(anyhow!(""))?)?,
                };
                Ok(Event::Limit(self.id, cmd, self.timestamp))
            }
            CANCEL => Ok(Event::Cancel(
                self.id,
                CancelCmd {
                    symbol: self.cmd.symbol().ok_or(anyhow!(""))?,
                    user_id: UserId::from_str(self.cmd.user_id.as_ref().ok_or(anyhow!(""))?)?,
                    order_id: self.cmd.order_id.ok_or(anyhow!(""))?,
                    #[cfg(feature = "fusotao")]
                    nonce: self.cmd.nonce.ok_or(anyhow!(""))?,
                    #[cfg(feature = "fusotao")]
                    signature: hex::decode(self.cmd.signature.ok_or(anyhow!(""))?)?,
                },
                self.timestamp,
            )),
            TRANSFER_OUT => Ok(Event::TransferOut(
                self.id,
                AssetsCmd {
                    user_id: UserId::from_str(self.cmd.user_id.as_ref().ok_or(anyhow!(""))?)?,
                    currency: self.cmd.currency.ok_or(anyhow!(""))?,
                    amount: self
                        .cmd
                        .amount
                        .filter(|a| a.is_sign_positive())
                        .ok_or(anyhow!(""))?,
                    #[cfg(feature = "fusotao")]
                    nonce_or_block_number: self.cmd.nonce.ok_or(anyhow!(""))?,
                    #[cfg(feature = "fusotao")]
                    signature_or_hash: hex::decode(self.cmd.signature.ok_or(anyhow!(""))?)?,
                },
                self.timestamp,
            )),
            TRANSFER_IN => Ok(Event::TransferIn(
                self.id,
                AssetsCmd {
                    user_id: UserId::from_str(self.cmd.user_id.as_ref().ok_or(anyhow!(""))?)?,
                    currency: self.cmd.currency.ok_or(anyhow!(""))?,
                    amount: self
                        .cmd
                        .amount
                        .filter(|a| a.is_sign_positive())
                        .ok_or(anyhow!(""))?,
                    #[cfg(feature = "fusotao")]
                    nonce_or_block_number: self.cmd.nonce.ok_or(anyhow!(""))?,
                    #[cfg(feature = "fusotao")]
                    signature_or_hash: hex::decode(self.cmd.signature.ok_or(anyhow!(""))?)?,
                },
                self.timestamp,
            )),
            UPDATE_SYMBOL => Ok(Event::UpdateSymbol(
                self.id,
                SymbolCmd {
                    symbol: self.cmd.symbol().ok_or(anyhow!(""))?,
                    open: self.cmd.open.ok_or(anyhow!(""))?,
                    base_scale: self.cmd.base_scale.filter(|b| *b < 18).ok_or(anyhow!(""))?,
                    quote_scale: self
                        .cmd
                        .quote_scale
                        .filter(|q| *q < 18)
                        .ok_or(anyhow!(""))?,
                    taker_fee: self
                        .cmd
                        .maker_fee
                        .filter(|f| f.is_sign_positive())
                        .ok_or(anyhow!(""))?,
                    maker_fee: self
                        .cmd
                        .maker_fee
                        .filter(|f| f.is_sign_positive())
                        .ok_or(anyhow!(""))?,
                    min_amount: self
                        .cmd
                        .min_amount
                        .filter(|f| f.is_sign_positive())
                        .ok_or(anyhow!(""))?,
                    min_vol: self
                        .cmd
                        .min_vol
                        .filter(|f| f.is_sign_positive())
                        .ok_or(anyhow!(""))?,
                    enable_market_order: self.cmd.enable_market_order.ok_or(anyhow!(""))?,
                },
                self.timestamp,
            )),
            #[cfg(not(feature = "fusotao"))]
            CANCEL_ALL => Ok(Event::CancelAll(
                self.id,
                self.cmd.symbol().ok_or(anyhow!(""))?,
                self.timestamp,
            )),
            DUMP => Ok(Event::Dump(self.id, self.timestamp)),
            _ => Err(anyhow!("Unsupported Command")),
        }
    }
}

impl Sequence {
    #[must_use]
    pub const fn rejected(&self) -> bool {
        self.status == ERROR
    }

    pub fn new_dump_sequence(at: u64, timestamp: u64) -> Self {
        let mut cmd = Command::default();
        cmd.cmd = DUMP;
        Self {
            id: at,
            cmd: cmd,
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

impl TryInto<Inspection> for Watch {
    type Error = anyhow::Error;

    fn try_into(self) -> anyhow::Result<Inspection> {
        match self.cmd.cmd {
            QUERY_ORDER => Ok(Inspection::QueryOrder(
                self.cmd.symbol().ok_or(anyhow!(""))?,
                self.cmd.order_id.ok_or(anyhow!(""))?,
                self.session,
                self.req_id,
            )),
            QUERY_BALANCE => Ok(Inspection::QueryBalance(
                UserId::from_str(self.cmd.user_id.as_ref().ok_or(anyhow!(""))?)?,
                self.cmd.currency.ok_or(anyhow!(""))?,
                self.session,
                self.req_id,
            )),
            QUERY_ACCOUNTS => Ok(Inspection::QueryAccounts(
                UserId::from_str(self.cmd.user_id.as_ref().ok_or(anyhow!(""))?)?,
                self.session,
                self.req_id,
            )),
            UPDATE_DEPTH => Ok(Inspection::UpdateDepth),
            CONFIRM_ALL => Ok(Inspection::ConfirmAll(
                self.cmd.from.ok_or(anyhow!(""))?,
                self.cmd.exclude.ok_or(anyhow!(""))?,
            )),
            _ => Err(anyhow!("Invalid Inspection")),
        }
    }
}

impl Watch {
    pub fn new_update_depth_watch() -> Self {
        let mut cmd = Command::default();
        cmd.cmd = UPDATE_DEPTH;
        Self {
            session: 0,
            req_id: 0,
            cmd: cmd,
        }
    }

    pub fn new_confirm_watch(from: u64, exclude: u64) -> Self {
        let mut cmd = Command::default();
        cmd.cmd = CONFIRM_ALL;
        cmd.from.replace(from);
        cmd.exclude.replace(exclude);
        Self {
            session: 0,
            req_id: 0,
            cmd: cmd,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Default)]
pub struct Command {
    pub cmd: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quote: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub currency: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vol: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub amount: Option<Amount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<Price>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nonce: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_scale: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quote_scale: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub taker_fee: Option<Fee>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub maker_fee: Option<Fee>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_amount: Option<Amount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_vol: Option<Amount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub open: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enable_market_order: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exclude: Option<u64>,
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
}

pub fn init(sender: Sender<Fusion>, id: u64, startup: Arc<AtomicBool>) {
    let mut id = id;
    let mut counter = 0_usize;
    let event_sender = sender.clone();
    log::info!("sequencer initialized");
    std::thread::spawn(move || loop {
        let seq = fetch_sequence_from(id);
        if seq.is_empty() {
            startup.store(true, Ordering::Relaxed);
            std::thread::sleep(Duration::from_millis(C.sequence.fetch_intervel_ms));
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
    std::thread::spawn(move || loop {
        std::thread::sleep(Duration::from_millis(500));
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

pub fn update_sequence_status(id: u64, status: u32) -> anyhow::Result<()> {
    let sql = "UPDATE t_sequence SET f_status=? WHERE f_id=?";
    let mut conn = DB.get_conn()?;
    conn.exec_drop(sql, (status, id))
        .map_err(|_| anyhow!("retrieve mysql connection failed while update_sequence_status"))
}

#[cfg(feature = "fusotao")]
pub fn insert_sequences(seq: Vec<Command>) -> anyhow::Result<()> {
    if seq.is_empty() {
        return Ok(());
    }
    let sql = r#"INSERT INTO t_sequence(f_cmd) VALUES (:cmd)"#;
    let mut conn = DB.get_conn()?;
    conn.exec_batch(
        sql,
        seq.iter().map(|s| {
            params! {
                "cmd" => serde_json::to_string(s).unwrap(),
            }
        }),
    )
    .map_err(|_| anyhow!("Error: writing sequence to mysql failed, {:?}"))
}

pub fn confirm(from: u64, exclude: u64) -> anyhow::Result<()> {
    let sql = "UPDATE t_sequence SET f_status=? WHERE f_status=? AND f_id>=? AND f_id<?";
    let mut conn = DB.get_conn()?;
    conn.exec_drop(sql, (ACCEPTED, PENDING, from, exclude))
        .map_err(|_| anyhow!("retrieve mysql connection failed while confirm"))
}

pub const PENDING: u32 = 0;
pub const ACCEPTED: u32 = 1;
pub const ERROR: u32 = 2;

#[test]
#[cfg(not(feature = "fusotao"))]
pub fn test_deserialize_cmd() {
    let transfer_in = r#"{"currency":100, "amount":"100.0", "user_id":"0x0000000000000000000000000000000000000000000000000000000000000001", "cmd":11}"#;
    let e = serde_json::from_str::<Command>(transfer_in).unwrap();
    let s: anyhow::Result<Event> = Sequence {
        id: 1,
        cmd: e,
        status: 0,
        timestamp: 0,
    }
    .try_into();
    assert!(s.is_ok());
    let bid_limit = r#"{"quote":100, "base":101, "cmd":1, "price":"10.0", "amount":"0.5", "order_id":1, "user_id":"0x0000000000000000000000000000000000000000000000000000000000000001"}"#;
    let e = serde_json::from_str::<Command>(bid_limit).unwrap();
    let s: anyhow::Result<Event> = Sequence {
        id: 2,
        cmd: e,
        status: 0,
        timestamp: 0,
    }
    .try_into();
    assert!(s.is_ok());
}
