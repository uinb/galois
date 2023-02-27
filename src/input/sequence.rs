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

use crate::{cmd::*, config::C, core::*, db::DB, input::*, orderbook::AskOrBid};
use anyhow::{anyhow, ensure};
use mysql::{prelude::*, *};
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
                ensure!(
                    price.is_sign_positive() && price.scale() <= 7,
                    "invalid price numeric"
                );
                ensure!(
                    amount.is_sign_positive() && amount.scale() <= 7,
                    "invalid amount numeric"
                );
                cfg_if::cfg_if! {
                    if #[cfg(features = "fusotao")] {
                        let mut vol = amount.checked_mul(price).ok_or(anyhow!(""))?;
                        ensure!(vol.validate(), "overflow");
                    }
                }
                let cmd = LimitCmd {
                    symbol: self.cmd.symbol().ok_or(anyhow!(""))?,
                    user_id: UserId::from_str(self.cmd.user_id.as_ref().ok_or(anyhow!(""))?)?,
                    order_id: self.cmd.order_id.ok_or(anyhow!(""))?,
                    price,
                    amount,
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
                    in_or_out: InOrOut::Out,
                    currency: self.cmd.currency.ok_or(anyhow!(""))?,
                    amount: self
                        .cmd
                        .amount
                        .filter(|a| a.is_sign_positive())
                        .ok_or(anyhow!(""))?,
                    #[cfg(feature = "fusotao")]
                    block_number: self.cmd.block_number.ok_or(anyhow!(""))?,
                    #[cfg(feature = "fusotao")]
                    extrinsic_hash: hex::decode(self.cmd.extrinsic_hash.ok_or(anyhow!(""))?)?,
                },
                self.timestamp,
            )),
            TRANSFER_IN => Ok(Event::TransferIn(
                self.id,
                AssetsCmd {
                    user_id: UserId::from_str(self.cmd.user_id.as_ref().ok_or(anyhow!(""))?)?,
                    in_or_out: InOrOut::In,
                    currency: self.cmd.currency.ok_or(anyhow!(""))?,
                    amount: self
                        .cmd
                        .amount
                        .filter(|a| a.is_sign_positive())
                        .ok_or(anyhow!(""))?,
                    #[cfg(feature = "fusotao")]
                    block_number: self.cmd.block_number.ok_or(anyhow!(""))?,
                    #[cfg(feature = "fusotao")]
                    extrinsic_hash: hex::decode(self.cmd.extrinsic_hash.ok_or(anyhow!(""))?)?,
                },
                self.timestamp,
            )),
            UPDATE_SYMBOL => Ok(Event::UpdateSymbol(
                self.id,
                SymbolCmd {
                    symbol: self.cmd.symbol().ok_or(anyhow!(""))?,
                    open: self.cmd.open.ok_or(anyhow!(""))?,
                    base_scale: self.cmd.base_scale.filter(|b| *b <= 7).ok_or(anyhow!(""))?,
                    quote_scale: self
                        .cmd
                        .quote_scale
                        .filter(|q| *q <= 7)
                        .ok_or(anyhow!(""))?,
                    taker_fee: self
                        .cmd
                        .taker_fee
                        .filter(|f| f.is_sign_positive())
                        .ok_or(anyhow!(""))?,
                    maker_fee: self
                        .cmd
                        .maker_fee
                        .filter(|f| f.is_sign_positive())
                        .ok_or(anyhow!(""))?,
                    base_maker_fee: self
                        .cmd
                        .base_maker_fee
                        .filter(|f| f.is_sign_positive())
                        .or(self.cmd.maker_fee)
                        .filter(|f| f.is_sign_positive())
                        .ok_or(anyhow!(""))?,
                    base_taker_fee: self
                        .cmd
                        .base_taker_fee
                        .filter(|f| f.is_sign_positive())
                        .or(self.cmd.taker_fee)
                        .filter(|f| f.is_sign_positive())
                        .ok_or(anyhow!(""))?,
                    fee_times: self.cmd.fee_times.unwrap_or(1),
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
            _ => Err(anyhow!("Unsupported Command")),
        }
    }
}

impl Sequence {
    #[must_use]
    pub const fn rejected(&self) -> bool {
        self.status == ERROR
    }
}

unsafe impl Send for Sequence {}

pub fn init(sender: Sender<Input>, id: u64, startup: Arc<AtomicBool>) {
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
                event_sender.send(Input::Modifier(s)).unwrap();
                counter += 1;
                if counter >= C.sequence.checkpoint {
                    counter = 0;
                    let t = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                    event_sender
                        .send(Input::NonModifier(Whistle::new_dump_whistle(id, t)))
                        .unwrap();
                }
                //check system busy
                if counter != 0 && counter % 200 == 0 {
                    event_sender
                        .send(Input::NonModifier(Whistle::new_proving_perf_check_whistle(
                            id,
                        )))
                        .unwrap();
                }
                id += 1;
            }
            event_sender
                .send(Input::NonModifier(Whistle::new_confirm_whistle(from, id)))
                .unwrap();
        }
    });
    std::thread::spawn(move || loop {
        std::thread::sleep(Duration::from_millis(500));
        let whistle = Whistle::new_update_depth_whistle();
        sender.send(Input::NonModifier(whistle)).unwrap();
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
pub fn insert_sequences(seq: &Vec<Command>) -> anyhow::Result<()> {
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
    .map_err(|e| anyhow!("Error: writing sequence to mysql failed, {:?}", e))
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