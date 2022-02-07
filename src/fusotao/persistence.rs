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

use crate::{db::DB, fusotao::*};
use mysql::{prelude::*, *};
use std::{sync::mpsc::Receiver, time::Duration};

pub fn init(rx: Receiver<Proof>) {
    let mut pending = Vec::with_capacity(100);
    std::thread::spawn(move || loop {
        let proof = match rx.recv_timeout(Duration::from_millis(10_000)) {
            Ok(p) => Some(p),
            Err(RecvTimeoutError::Timeout) => None,
            Err(RecvTimeoutError::Disconnected) => {
                log::error!("Proof persistence thread interrupted!");
                break;
            }
        };
        append(proof, &mut pending);
    });
}

pub fn fetch_raw_after(event_id: u64) -> Vec<(u64, RawParameter)> {
    let sql = "SELECT f_event_id,f_proof FROM t_proof WHERE f_event_id>? LIMIT ?";
    let conn = DB.get_conn();
    if conn.is_err() {
        log::error!("retrieve mysql connection failed while fetch_proofs");
        return vec![];
    }
    let mut conn = conn.unwrap();
    conn.exec_map(
        sql,
        // TODO calculate max_weight and max_length
        (event_id, C.sequence.batch_size),
        |(f_event_id, f_proof): (u64, Vec<u8>)| (f_event_id, RawParameter(f_proof)),
    )
    .unwrap_or_default()
}

fn flush(pending: &mut Vec<Proof>) {
    let sql = r#"INSERT IGNORE INTO t_proof(f_event_id,f_proof) VALUES (:event_id,:proof)"#;
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
                "proof" => p.encode(),
            }
        }),
    );
    match r {
        Ok(_) => pending.clear(),
        Err(err) => {
            log::error!("Error: writing proofs to mysql failed, {:?}", err);
        }
    }
}

fn append(proof: Option<Proof>, pending: &mut Vec<Proof>) {
    match proof {
        None => {
            if !pending.is_empty() {
                flush(pending);
            }
        }
        Some(p) => {
            pending.push(p);
            if pending.len() >= 100 {
                flush(pending);
            }
        }
    }
}
