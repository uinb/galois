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

use crate::{config::C, input::*};
use rocksdb::{Direction, IteratorMode, WriteBatchWithTransaction};
use std::{convert::TryInto, sync::mpsc::*};

pub fn init(
    rx: Receiver<Input>,
    to_executor: Sender<Event>,
    to_server: Sender<(u64, Message)>,
    init_at: u64,
) {
    let recovery = ensure_fully_loaded(init_at, to_executor.clone()).unwrap();
    log::info!(
        "historic events {}-{} have been executed",
        init_at,
        recovery
    );
    if C.dry_run.is_some() {
        return;
    }
    std::thread::spawn(move || -> anyhow::Result<()> {
        let mut current_id = recovery;
        loop {
            let mut input = rx.recv()?;
            let (session, req_id) = (input.session, input.req_id);
            input.sequence = current_id;
            let cmd = serde_json::to_vec(&input.cmd)?;
            if let Ok(event) = input.try_into() {
                save(current_id, cmd)?;
                to_executor.send(event)?;
                if current_id % C.sequence.checkpoint == 0 {
                    to_executor.send(Event::Dump(current_id))?;
                }
                current_id += 1;
            } else {
                to_server.send((session, Message::new(req_id, vec![])))?;
            }
        }
    });
}

fn save(id: u64, cmd: Vec<u8>) -> anyhow::Result<()> {
    SEQ_STORE.put(id_to_key(id), cmd)?;
    Ok(())
}

fn ensure_fully_loaded(init_at: u64, tx: Sender<Event>) -> anyhow::Result<u64> {
    let mut current_id = init_at;
    let iter = SEQ_STORE.iterator(IteratorMode::From(&id_to_key(init_at), Direction::Forward));
    for item in iter {
        let (key, value) = item?;
        current_id = key_to_id(&key);
        let input = Input {
            session: 0,
            req_id: 0,
            sequence: current_id,
            cmd: value_to_cmd(&value)
                .map_err(|_| anyhow::anyhow!("id {} is invalid", current_id))?,
        };
        let event = input
            .try_into()
            .map_err(|_| anyhow::anyhow!("id {} is invalid", current_id))?;
        // LIMIT|CANCEL(session=0, req_id=0) represent historic events, shouldn't reply
        match C.dry_run {
            Some(n) if n >= current_id => tx.send(event)?,
            None => tx.send(event)?,
            _ => break,
        }
    }
    Ok(current_id + 1)
}

pub fn remove_before(id: u64) -> anyhow::Result<()> {
    let mut batch = WriteBatchWithTransaction::<false>::default();
    batch.delete_range(id_to_key(1), id_to_key(id));
    SEQ_STORE.write(batch)?;
    Ok(())
}

fn id_to_key(id: u64) -> [u8; 16] {
    unsafe { std::mem::transmute::<[[u8; 8]; 2], [u8; 16]>([*b"sequence", id.to_be_bytes()]) }
}

fn key_to_id(key: &[u8]) -> u64 {
    let mut id = [0u8; 8];
    id.copy_from_slice(&key[8..]);
    u64::from_be_bytes(id)
}

fn value_to_cmd(value: &[u8]) -> anyhow::Result<Command> {
    let cmd = serde_json::from_slice(value)?;
    Ok(cmd)
}

#[cfg(test)]
mod test {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    pub fn test_serialize() {
        assert_eq!("{}", serde_json::to_string(&Accounts::new()).unwrap());
        let mut account = Account::default();
        account.insert(
            100,
            crate::assets::Balance {
                available: Amount::new(200, 1),
                frozen: Amount::new(0, 0),
            },
        );
        assert_eq!(
            r#"{"100":{"available":"20.0","frozen":"0"}}"#,
            serde_json::to_string(&account).unwrap()
        );
        assert_eq!(
            r#"{"available":"0","frozen":"0"}"#,
            serde_json::to_string(&crate::assets::Balance {
                available: Amount::new(0, 0),
                frozen: Amount::new(0, 0),
            })
            .unwrap()
        );

        let mut data = Data::new();
        let orderbook = crate::orderbook::OrderBook::new(
            8,
            8,
            dec!(0.001),
            dec!(0.001),
            dec!(0.001),
            dec!(0.001),
            1,
            dec!(0.1),
            dec!(0.1),
            true,
            true,
        );
        data.orderbooks.insert((0, 1), orderbook);
    }

    #[test]
    pub fn test_deserialize_cmd() {
        let transfer_in = r#"{"currency":100, "amount":"100.0", "user_id":"5Ccr8Qcp6NBMCvdUHSoqDaQMJHnA5PAC879NbWkzaiUwBdMm", "cmd":11, "block_number":1000, "extrinsic_hash":""}"#;
        let e = serde_json::from_str::<Command>(transfer_in).unwrap();
        let s: anyhow::Result<Event> = Input {
            cmd: e,
            sequence: 0,
            session: 0,
            req_id: 0,
        }
        .try_into();
        assert!(s.is_ok());
        let bid_limit = r#"{"quote":100, "base":101, "cmd":1, "price":"10.0", "amount":"0.5", "order_id":1, "user_id":"5Ccr8Qcp6NBMCvdUHSoqDaQMJHnA5PAC879NbWkzaiUwBdMm","nonce":1,"signature":""}"#;
        let e = serde_json::from_str::<Command>(bid_limit).unwrap();
        let s: anyhow::Result<Event> = Input {
            cmd: e,
            sequence: 0,
            session: 1,
            req_id: 0,
        }
        .try_into();
        assert!(s.is_ok());
    }
}
