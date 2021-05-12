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

use crate::{
    config::C,
    sequence::{Command, Fusion, Watch},
};
use async_std::{
    net::{TcpListener, TcpStream, ToSocketAddrs},
    prelude::*,
    task,
};
use chashmap::CHashMap;
use futures::channel::mpsc;
use futures::sink::SinkExt;
use lazy_static::lazy_static;
use std::borrow::BorrowMut;
use std::net::Shutdown;
use std::str;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc::Sender,
    Arc,
};

pub const MAX_FRAME_SIZE: usize = 64 * 1024;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

lazy_static! {
    static ref CHAN: CHashMap<u64, mpsc::UnboundedSender<Message>> = CHashMap::new();
}

#[derive(Debug)]
pub struct Message {
    pub session: u64,
    pub req_id: u64,
    pub payload: Vec<u8>,
}

impl Message {
    #[must_use]
    pub fn with_payload(session: u64, req_id: u64, payload: Vec<u8>) -> Self {
        Self {
            session,
            req_id,
            payload,
        }
    }

    fn encode(self) -> Vec<u8> {
        let frame_count = self.payload.len() / MAX_FRAME_SIZE + 1;
        let mut payload_len = self.payload.len();
        let mut all = Vec::<u8>::with_capacity(payload_len + 16 * frame_count);
        for i in 0..frame_count - 1 {
            let mut header = _MAGIC_N_MASK;
            header |= (MAX_FRAME_SIZE as u64) << 32;
            header |= 1;
            payload_len -= MAX_FRAME_SIZE;
            all.extend_from_slice(&header.to_be_bytes());
            all.extend_from_slice(&self.req_id.to_be_bytes());
            all.extend_from_slice(&self.payload[i * MAX_FRAME_SIZE..(i + 1) * MAX_FRAME_SIZE]);
        }
        let mut header = _MAGIC_N_MASK;
        header |= (payload_len as u64) << 32;
        all.extend_from_slice(&header.to_be_bytes());
        all.extend_from_slice(&self.req_id.to_be_bytes());
        all.extend_from_slice(&self.payload[(frame_count - 1) * MAX_FRAME_SIZE..]);
        all
    }
}

/// header = 0x0316<2bytes payload len><2bytes cheskcum><2bytes flag>

const _MAGIC_N_MASK: u64 = 0x0316_0000_0000_0000;
const _PAYLOAD_MASK: u64 = 0x0000_ffff_0000_0000;
const _CHK_SUM_MASK: u64 = 0x0000_0000_ffff_0000;
const _ERR_RSP_MASK: u64 = 0x0000_0000_0000_0001;
const _NXT_FRM_MASK: u64 = 0x0000_0000_0000_0002;

const fn check_magic(header: u64) -> bool {
    (header & _MAGIC_N_MASK) == _MAGIC_N_MASK
}

fn get_len(header: u64) -> usize {
    ((header & _PAYLOAD_MASK) >> 32) as usize
}

#[allow(dead_code)]
const fn get_checksum(header: u64) -> u16 {
    ((header & _CHK_SUM_MASK) >> 16) as u16
}

const fn has_next_frame(header: u64) -> bool {
    (header & _NXT_FRM_MASK) == _NXT_FRM_MASK
}

pub fn init(sender: Sender<Fusion>, ready: Arc<AtomicBool>) {
    let future = accept(&C.server.bind_addr, sender, ready);
    task::block_on(future).unwrap();
}

async fn accept(
    addr: impl ToSocketAddrs,
    cmd_acc: Sender<Fusion>,
    ready: Arc<AtomicBool>,
) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    let mut incoming = listener.incoming();
    let mut session = 0_u64;
    while let Some(stream) = incoming.next().await {
        if ready.load(Ordering::Relaxed) {
            let stream = stream?;
            register(session, stream, cmd_acc.clone());
            session += 1;
        }
    }
    Ok(())
}

fn register(session: u64, stream: TcpStream, cmd_acc: Sender<Fusion>) {
    let (tx, rx) = mpsc::unbounded();
    match stream.set_nodelay(true) {
        Ok(_) => {}
        Err(_) => return,
    }
    CHAN.insert(session, tx);
    let stream = Arc::new(stream);
    task::spawn(write_loop(rx, stream.clone()));
    task::spawn(read_loop(cmd_acc, session, stream));
}

async fn write_loop(
    recv_queue: mpsc::UnboundedReceiver<Message>,
    stream: Arc<TcpStream>,
) -> Result<()> {
    let mut recv_queue = recv_queue;
    let mut stream = &*stream;
    while let Some(output) = recv_queue.next().await {
        stream.write_all(&output.encode()).await?;
    }
    Ok(())
}

/// if r: (query order/assets) validate to sequence push to cmd_acc
async fn handle_req(upstream: &mut Sender<Fusion>, session: u64, req_id: u64, json: String) {
    let cmd: Command = match serde_json::from_str(&json) {
        Ok(cmd) => cmd,
        Err(_) => {
            send(Message::with_payload(session, req_id, vec![]))
                .await
                .unwrap();
            return;
        }
    };
    if !cmd.validate() {
        send(Message::with_payload(session, req_id, vec![]))
            .await
            .unwrap();
        return;
    }
    if !cmd.is_read() {
        return;
    }
    upstream
        .send(Fusion::R(Watch {
            session,
            req_id,
            cmd,
        }))
        .unwrap();
}

async fn read_loop(cmd_acc: Sender<Fusion>, session: u64, stream: Arc<TcpStream>) -> Result<()> {
    let mut cmd_acc = cmd_acc;
    let mut stream = &*stream;
    let mut buf = Vec::<u8>::with_capacity(4096);
    loop {
        let mut header = [0_u8; 8];
        let mut req_id = [0_u8; 8];
        if stream.read_exact(&mut header).await.is_err() {
            break;
        }
        if stream.read_exact(&mut req_id).await.is_err() {
            break;
        }
        let header = u64::from_be_bytes(header);
        if !check_magic(header) {
            break;
        }
        let req_id = u64::from_be_bytes(req_id);
        let mut tmp = vec![0_u8; get_len(header)];
        if stream.read_exact(&mut tmp).await.is_err() {
            break;
        }
        buf.extend_from_slice(&tmp[..]);
        if !has_next_frame(header) {
            let json = match str::from_utf8(&buf[..]) {
                Ok(json) => json.to_string(),
                Err(_) => {
                    send(Message::with_payload(session, req_id, vec![]))
                        .await
                        .unwrap();
                    buf.clear();
                    continue;
                }
            };
            handle_req(&mut cmd_acc, session, req_id, json).await;
            buf.clear();
        }
    }
    close(session);
    let _ = stream.shutdown(Shutdown::Both);
    Ok(())
}

fn close(session: u64) {
    CHAN.remove(&session);
}

pub fn publish(output: Message) {
    let _ = task::block_on(send(output));
}

async fn send(output: Message) -> std::result::Result<(), mpsc::SendError> {
    match CHAN.get_mut(&output.session) {
        None => Ok(()),
        Some(mut channel) => channel.borrow_mut().send(output).await,
    }
}
