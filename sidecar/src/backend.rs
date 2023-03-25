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

use dashmap::DashMap;
use galois_engine::{
    core::*,
    fusotao::OffchainSymbol,
    input::{cmd, Message},
    orderbook::Order as CoreOrder,
};
use serde_json::{json, to_vec, Value as JsonValue};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{
    tcp::{OwnedReadHalf, OwnedWriteHalf},
    TcpStream, ToSocketAddrs,
};
use tokio::sync::mpsc::{self, Receiver, Sender};
use x25519_dalek::StaticSecret;

type ToBackend = Sender<Option<Req>>;
type FromFrontend = Receiver<Option<Req>>;
type Notifier = Sender<JsonValue>;

#[derive(Clone, Debug)]
pub struct BackendConnection {
    to_backend: ToBackend,
}

#[derive(Clone, Debug)]
struct Req {
    pub payload: Vec<u8>,
    pub notifier: Notifier,
}

impl BackendConnection {
    pub fn new(addr: impl ToSocketAddrs + Send + Sync + Clone + 'static) -> Self {
        let (to_backend, from_frontend) = mpsc::channel(3000);
        Self::start_inner(to_backend.clone(), from_frontend, addr);
        Self { to_backend }
    }

    fn start_inner(
        to_back: ToBackend,
        from_front: FromFrontend,
        addr: impl ToSocketAddrs + Send + Sync + Clone + 'static,
    ) {
        tokio::spawn(async move {
            let sink = Arc::new(DashMap::<u64, Notifier>::new());
            let mut from_front = from_front;
            loop {
                if let Ok(stream) = TcpStream::connect(addr.clone()).await {
                    let (r, w) = stream.into_split();
                    let join = tokio::spawn(Self::write_loop(w, sink.clone(), from_front));
                    Self::read_loop(r, sink.clone()).await;
                    let _ = to_back.send(None).await;
                    from_front = join.await.unwrap();
                } else {
                    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
                }
            }
        });
    }

    async fn read_loop(mut stream: OwnedReadHalf, sink: Arc<DashMap<u64, Notifier>>) {
        log::debug!("starting background read loop.");
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
                log::debug!(
                    "receiving data from galois: {:?}",
                    std::str::from_utf8(&buf)
                );
                let json = match serde_json::from_slice(&buf[..]) {
                    Ok(json) => json,
                    Err(_) => break,
                };
                if let Some((_, noti)) = sink.remove(&req_id) {
                    let _ = noti.send(json).await;
                }
                buf.clear();
            }
        }
        sink.clear();
        log::debug!("read loop interrupted, will restart.");
    }

    async fn write_loop(
        mut stream: OwnedWriteHalf,
        sink: Arc<DashMap<u64, Notifier>>,
        mut from_front: FromFrontend,
    ) -> FromFrontend {
        log::debug!("starting background write loop.");
        let mut req_id = 1u64;
        while let Some(req) = from_front.recv().await {
            match req {
                Some(req) => {
                    req_id += 1;
                    let Req { payload, notifier } = req;
                    sink.insert(req_id, notifier);
                    let msg = Message::new(req_id, payload);
                    match stream.write_all(&msg.encode()).await {
                        Ok(_) => log::debug!("write to galois -> OK"),
                        Err(e) => log::debug!("write to galois -> {:?}", e),
                    }
                }
                None => break,
            }
        }
        log::debug!("write loop interrupted, will restart.");
        from_front
    }

    pub async fn request(&self, payload: Vec<u8>) -> anyhow::Result<JsonValue> {
        let (notifier, mut feedback) = mpsc::channel(1);
        self.to_backend
            .send(Some(Req { payload, notifier }))
            .await?;
        feedback
            .recv()
            .await
            .ok_or(anyhow::anyhow!("fail to read from backend"))
    }

    pub async fn get_nonce(&self, broker: &str) -> Option<u32> {
        let r = self
            .request(to_vec(&json!({ "cmd": cmd::GET_NONCE_FOR_BROKER, "user_id": broker })).ok()?)
            .await
            .inspect_err(|e| log::debug!("{:?}", e))
            .ok()?;
        r.get("nonce")?
            .as_i64()
            .map(|n| n.try_into().ok())
            .flatten()
    }

    pub async fn get_account(&self, user_id: &String) -> anyhow::Result<HashMap<u32, Balance>> {
        let r = self
            .request(
                to_vec(&json!({"cmd": cmd::QUERY_ACCOUNTS, "user_id": user_id}))
                    .expect("jsonser;qed"),
            )
            .await
            .inspect_err(|e| log::debug!("{:?}", e))
            .map_err(|_| anyhow::anyhow!("Galois not available"))?;
        serde_json::from_value::<HashMap<u32, Balance>>(r).map_err(|_| anyhow::anyhow!("galois?"))
    }

    pub async fn get_order(
        &self,
        symbol: Symbol,
        order_id: u64,
    ) -> anyhow::Result<Option<CoreOrder>> {
        let r = self
            .request(
                to_vec(&json!({
                    "cmd": cmd::QUERY_ORDER,
                    "base": symbol.0,
                    "quote": symbol.1,
                    "order_id": order_id,
                }))
                .expect("jsonser;qed"),
            )
            .await
            .inspect_err(|e| log::debug!("{:?}", e))
            .map_err(|_| anyhow::anyhow!("Galois not available"))?;
        serde_json::from_value::<Option<CoreOrder>>(r).map_err(|_| anyhow::anyhow!("galois?"))
    }

    pub async fn get_markets(&self) -> anyhow::Result<Vec<OffchainSymbol>> {
        let r = self
            .request(to_vec(&json!({ "cmd": cmd::QUERY_OPEN_MARKETS })).expect("jsonser;qed"))
            .await
            .inspect_err(|e| log::debug!("{:?}", e))
            .map_err(|_| anyhow::anyhow!("Galois not available"))?;
        serde_json::from_value::<Vec<OffchainSymbol>>(r).map_err(|_| anyhow::anyhow!("galois?"))
    }

    pub async fn get_x25519(&self) -> anyhow::Result<StaticSecret> {
        let r = self
            .request(to_vec(&json!({ "cmd": cmd::GET_X25519_KEY })).expect("jsonser;qed"))
            .await
            .inspect_err(|e| log::debug!("{:?}", e))
            .map_err(|_| anyhow::anyhow!("Galois not available"))?;
        let b = r
            .get("x25519")
            .map(|v| v.as_str())
            .flatten()
            .map(|hex| crate::hexstr_to_vec(&hex))
            .ok_or(anyhow::anyhow!("retrieving x25519 private key failed"))
            .flatten()?;
        let key: [u8; 32] = b
            .try_into()
            .map_err(|_| anyhow::anyhow!("x25519 config error"))?;
        Ok(StaticSecret::from(key))
    }
}