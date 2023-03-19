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

use crate::{
    config::C,
    input::{Command, Input},
    shared::Shared,
    whistle::Whistle,
};
use async_std::{
    net::{TcpListener, TcpStream},
    prelude::*,
    task,
};
use dashmap::DashMap;
use futures::{
    channel::mpsc::{self, UnboundedReceiver, UnboundedSender},
    sink::SinkExt,
};
use std::{
    net::Shutdown,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{Receiver, Sender},
        Arc,
    },
};

pub const MAX_FRAME_SIZE: usize = 64 * 1024;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

type ToSession = UnboundedSender<Message>;
type FromSession = UnboundedReceiver<Message>;
type ToBackend = Sender<Input>;
type FromBackend = Receiver<Message>;

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

pub fn init(sender: ToBackend, receiver: FromBackend, shared: Shared, ready: Arc<AtomicBool>) {
    let listener = task::block_on(async { TcpListener::bind(&C.server.bind_addr).await }).unwrap();
    let sessions = Arc::new(DashMap::<u64, ToSession>::new());
    let sx = sessions.clone();
    std::thread::spawn(move || loop {
        let msg = receiver.recv().unwrap();
        if let Some(session) = sx.get_mut(&msg.session) {
            let mut s = session.clone();
            // relay the messages from backend to session, need to switch the runtime using async
            task::block_on(async move {
                let _ = s.send(msg).await;
            });
        } else {
            log::error!(
                "received reply from executor, but session {} not found",
                msg.session
            );
        }
    });
    log::info!("server initialized");
    let future = accept(listener, sender, shared, sessions, ready);
    task::block_on(future).unwrap();
}

async fn accept(
    listener: TcpListener,
    to_backend: ToBackend,
    shared: Shared,
    sessions: Arc<DashMap<u64, ToSession>>,
    ready: Arc<AtomicBool>,
) -> Result<()> {
    let mut incoming = listener.incoming();
    let mut session = 0_u64;
    while let Some(stream) = incoming.next().await {
        if ready.load(Ordering::Relaxed) {
            let stream = stream?;
            register(
                session,
                stream,
                to_backend.clone(),
                shared.clone(),
                &sessions,
            );
            session += 1;
        }
    }
    Ok(())
}

fn register(
    session: u64,
    stream: TcpStream,
    to_backend: ToBackend,
    shared: Shared,
    sessions: &Arc<DashMap<u64, ToSession>>,
) {
    match stream.set_nodelay(true) {
        Ok(_) => {}
        Err(_) => return,
    }
    let (tx, rx) = mpsc::unbounded();
    sessions.insert(session, tx);
    let stream = Arc::new(stream);
    task::spawn(write_loop(rx, stream.clone(), sessions.clone()));
    task::spawn(read_loop(
        to_backend.clone(),
        shared,
        session,
        stream,
        sessions.clone(),
    ));
}

async fn write_loop(
    mut recv: FromSession,
    stream: Arc<TcpStream>,
    sessions: Arc<DashMap<u64, ToSession>>,
) -> Result<()> {
    let mut stream = &*stream;
    while let Some(output) = recv.next().await {
        let session = output.session;
        match stream.write_all(&output.encode()).await {
            Ok(_) => {}
            Err(_) => {
                // for some reasons we didn't read errors
                sessions.remove(&session);
                break;
            }
        }
    }
    Ok(())
}

async fn read_loop(
    mut to_back: ToBackend,
    shared: Shared,
    session: u64,
    stream: Arc<TcpStream>,
    sessions: Arc<DashMap<u64, ToSession>>,
) -> Result<()> {
    let mut stream = &*stream;
    let mut buf = Vec::<u8>::with_capacity(4096);
    let mut to_session = sessions.get_mut(&session).unwrap();
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
            let json = match std::str::from_utf8(&buf[..]) {
                Ok(json) => json.to_string(),
                Err(_) => break,
            };
            if let Err(e) = handle_req(
                &mut to_back,
                &mut to_session,
                &shared,
                session,
                req_id,
                json,
            )
            .await
            {
                log::error!("{:?}, will close session {}", e, session);
                break;
            }
            buf.clear();
        }
    }
    let _ = stream.shutdown(Shutdown::Both);
    sessions.remove(&session);
    Ok(())
}

async fn handle_req(
    to_back: &mut ToBackend,
    to_session: &mut ToSession,
    shared: &Shared,
    session: u64,
    req_id: u64,
    json: String,
) -> Result<()> {
    let cmd: Command = serde_json::from_str(&json)
        .map_err(|e| anyhow::anyhow!("deser command failed, {:?}", e))?;
    if cmd.is_querying_core_data() {
        let w = Input::NonModifier(Whistle {
            session,
            req_id,
            cmd,
        });
        to_back
            .send(w)
            .map_err(|e| anyhow::anyhow!("read loop -> executor -> {:?}", e))?;
        Ok(())
    } else if cmd.is_querying_share_data() {
        let w = shared.handle_req(&cmd)?;
        to_session
            .send(Message::with_payload(session, req_id, w))
            .await
            .map_err(|e| anyhow::anyhow!("read loop -> write loop -> {:?}", e))?;
        Ok(())
    } else {
        Err(anyhow::anyhow!("unsupported command {} from sidecar", cmd.cmd).into())
    }
}
