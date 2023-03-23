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
    input::{Command, Input, Message},
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

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
type ToSession = UnboundedSender<Message>;
type FromSession = UnboundedReceiver<Message>;
type ToBackend = Sender<Input>;
type FromBackend = Receiver<(u64, Message)>;

pub fn init(sender: ToBackend, receiver: FromBackend, shared: Shared, ready: Arc<AtomicBool>) {
    let listener = task::block_on(async { TcpListener::bind(&C.server.bind_addr).await }).unwrap();
    let sessions = Arc::new(DashMap::<u64, ToSession>::new());
    let sx = sessions.clone();
    std::thread::spawn(move || {
        log::error!("session relayer interrupted, {:?}", relay(receiver, sx));
    });
    log::info!("server initialized");
    let future = accept(listener, sender, shared, sessions, ready);
    let _ = task::block_on(future);
    log::info!("bye!");
}

fn relay(receiver: FromBackend, sessions: Arc<DashMap<u64, ToSession>>) -> Result<()> {
    loop {
        let (session_id, msg) = receiver.recv()?;
        log::debug!("session relayer received msg: {:?}", msg);
        // relay the messages from backend to session, need to switch the runtime using async
        if let Some(mut session) = sessions.get_mut(&session_id) {
            let _ = task::block_on(session.send(msg));
        } else {
            log::error!(
                "received reply from executor, but session {} not found",
                session_id
            );
        }
    }
}

async fn accept(
    listener: TcpListener,
    to_backend: ToBackend,
    shared: Shared,
    sessions: Arc<DashMap<u64, ToSession>>,
    ready: Arc<AtomicBool>,
) -> Result<()> {
    let mut incoming = listener.incoming();
    let mut session_id = 0_u64;
    while let Some(stream) = incoming.next().await {
        if ready.load(Ordering::Relaxed) {
            let stream = stream?;
            register(
                session_id,
                stream,
                to_backend.clone(),
                shared.clone(),
                &sessions,
            );
            session_id += 1;
        }
    }
    Ok(())
}

fn register(
    session_id: u64,
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
    sessions.insert(session_id, tx);
    let stream = Arc::new(stream);
    task::spawn(write_loop(rx, stream.clone()));
    task::spawn(read_loop(
        to_backend.clone(),
        shared,
        session_id,
        stream,
        sessions.clone(),
    ));
}

async fn write_loop(mut recv: FromSession, stream: Arc<TcpStream>) -> Result<()> {
    let mut stream = &*stream;
    while let Some(output) = recv.next().await {
        match stream.write_all(&output.encode()).await {
            Ok(_) => log::debug!("replying to sidecar -> OK"),
            Err(e) => {
                log::debug!("replying to sidecar -> {:?}", e);
                break;
            }
        }
    }
    log::info!("bye! {:?}", stream);
    Ok(())
}

async fn read_loop(
    mut to_back: ToBackend,
    shared: Shared,
    session_id: u64,
    stream: Arc<TcpStream>,
    sessions: Arc<DashMap<u64, ToSession>>,
) -> Result<()> {
    let mut stream = &*stream;
    let mut buf = Vec::<u8>::with_capacity(4096);
    let mut to_session = sessions
        .get(&session_id)
        .ok_or(anyhow::anyhow!("session not found;qed?"))?
        .clone();
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
        if !Message::check_magic(header) {
            break;
        }
        let req_id = u64::from_be_bytes(req_id);
        let mut tmp = vec![0_u8; Message::get_len(header)];
        if stream.read_exact(&mut tmp).await.is_err() {
            break;
        }
        buf.extend_from_slice(&tmp[..]);
        if !Message::has_next_frame(header) {
            let json = match std::str::from_utf8(&buf[..]) {
                Ok(json) => json.to_string(),
                Err(_) => break,
            };
            if let Err(e) = handle_req(
                &mut to_back,
                &mut to_session,
                &shared,
                session_id,
                req_id,
                json,
            )
            .await
            {
                log::error!("{:?}, will close session {}", e, session_id);
                break;
            }
            buf.clear();
        }
    }
    let _ = stream.shutdown(Shutdown::Both);
    sessions.remove(&session_id);
    Ok(())
}

async fn handle_req(
    to_back: &mut ToBackend,
    to_session: &mut ToSession,
    shared: &Shared,
    session_id: u64,
    req_id: u64,
    json: String,
) -> Result<()> {
    let cmd: Command = serde_json::from_str(&json)
        .map_err(|e| anyhow::anyhow!("deser command failed, {:?}", e))?;
    if cmd.is_querying_core_data() {
        let w = Input::NonModifier(Whistle {
            session: session_id,
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
            .send(Message::new(req_id, w))
            .await
            .map_err(|e| anyhow::anyhow!("read loop -> write loop -> {:?}", e))?;
        Ok(())
    } else {
        Err(anyhow::anyhow!("unsupported command {} from sidecar", cmd.cmd).into())
    }
}
