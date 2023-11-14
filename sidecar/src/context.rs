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

use crate::errors::CustomRpcError;
use crate::{
    backend::BackendConnection,
    config::Config,
    // TODO remove
    db,
    endpoint::{PendingOrderWrapper, TradingCommand},
    AccountId32,
    Sr25519Pair,
    Sr25519Public,
    Sr25519Signature,
};
use dashmap::DashMap;
use galois_engine::{core::*, fusotao::OffchainSymbol, orders::PendingOrder};
use hyper::{Body, Request, Response};
use parity_scale_codec::{Decode, Encode};
use rust_decimal::Decimal;
use sp_core::crypto::{Pair as Crypto, Ss58Codec};
use sqlx::mysql::MySqlConnectOptions;
use sqlx::{ConnectOptions, MySql, Pool};
use std::{
    collections::BTreeSet,
    error::Error,
    future::Future,
    pin::Pin,
    str::FromStr,
    sync::atomic::AtomicBool,
    sync::Arc,
    task::{Context as TaskCtx, Poll},
};
use tokio::sync::{
    mpsc::{self, UnboundedSender},
    Mutex,
};
use tower::{Layer, Service};
use x25519_dalek::StaticSecret;

pub struct Context {
    pub backend: BackendConnection,
    pub x25519: StaticSecret,
    pub db: Pool<MySql>,
    pub subscribers: Arc<DashMap<String, UnboundedSender<(String, PendingOrderWrapper)>>>,
    pub session_nonce: Arc<DashMap<String, Session>>,
    pub markets: Arc<DashMap<Symbol, (Arc<AtomicBool>, OffchainSymbol)>>,
}

impl Context {
    pub fn new(config: Config) -> Self {
        let (broadcast, mut dispatcher) = mpsc::unbounded_channel();
        let backend = BackendConnection::new(config.prover, broadcast);
        let conn = backend.clone();
        let x25519 = futures::executor::block_on(async move { conn.get_x25519().await }).unwrap();
        let db = futures::executor::block_on(async {
            let mut option: MySqlConnectOptions = config.db.parse()?;
            option.disable_statement_logging();
            Pool::connect_with(option).await
        })
        .unwrap();
        let subscribers = Arc::new(DashMap::<
            String,
            UnboundedSender<(String, PendingOrderWrapper)>,
        >::default());
        let conn = backend.clone();
        let markets = futures::executor::block_on(async move {
            conn.get_markets().await.map(|markets| {
                Arc::new(DashMap::from_iter(markets.into_iter().map(|m| {
                    (m.symbol.clone(), (Arc::new(AtomicBool::new(false)), m))
                })))
            })
        })
        .unwrap();
        log::debug!("Loading marketings from backend: {:?}", markets);
        let sub = subscribers.clone();
        tokio::spawn(async move {
            loop {
                let v = dispatcher.recv().await;
                if v.is_none() {
                    continue;
                }
                let v = v.unwrap();
                // TODO support multi-types broadcasting messages from engine
                if let Ok(o) = serde_json::from_value::<PendingOrder>(v) {
                    let user_id = o.user_id.to_string();
                    let r = if let Some(u) = sub.get(&user_id) {
                        u.value().send((user_id.clone(), o.into()))
                    } else {
                        Ok(())
                    };
                    match r {
                        Err(e) => {
                            log::debug!("sending order to channel error: {}", e);
                            sub.remove(&user_id);
                        }
                        Ok(_) => {}
                    }
                }
            }
        });
        Self {
            backend,
            x25519,
            db,
            session_nonce: Arc::new(DashMap::default()),
            subscribers,
            markets,
        }
    }

    pub async fn get_trading_key(&self, user_id: &String) -> anyhow::Result<Vec<u8>> {
        db::query_trading_key(&self.db, user_id)
            .await
            .map(|k| crate::hexstr_to_vec(&k))
            .flatten()
    }

    pub async fn get_user_nonce(&self, user_id: &String) -> anyhow::Result<u32> {
        let session = self
            .session_nonce
            .get(user_id)
            .ok_or_else(|| CustomRpcError::user_not_found())?;
        Ok(session.value().get_nonce().await)
    }

    pub async fn verify_trading_signature(
        &self,
        data: &[u8],
        user_id: &String,
        sig: &[u8],
        nonce: &[u8],
    ) -> anyhow::Result<()> {
        let mut decode = nonce.clone();
        let n = u32::decode(&mut decode)?;
        let key = self.get_trading_key(user_id).await?;
        // FIXME when sidecar reboot, the session_nonce will be empty
        let session = self
            .session_nonce
            .get(user_id)
            .ok_or(CustomRpcError::user_not_found())?;
        session.value().try_occupy_nonce(n).await?;
        let mut to_be_signed = vec![];
        to_be_signed.extend_from_slice(data);
        to_be_signed.extend_from_slice(key.as_slice());
        to_be_signed.extend_from_slice(nonce);
        log::debug!("sig content: {}", hex::encode(&to_be_signed));
        let hash = sp_core::blake2_256(&to_be_signed);
        log::debug!("user sign content: {}", hex::encode(&sig));
        log::debug!("server blake2 content: {}", hex::encode(&hash));
        anyhow::ensure!(hash.as_slice() == sig, CustomRpcError::invalid_signature());
        Ok(())
    }

    pub async fn validate_cmd(&self, user_id: &str, cmd: &TradingCommand) -> anyhow::Result<()> {
        match cmd {
            TradingCommand::Cancel {
                base,
                quote,
                order_id,
            } => {
                let order = self
                    .backend
                    .get_order((*base, *quote), *order_id)
                    .await?
                    .ok_or(CustomRpcError::order_not_exist())?;
                if format!("{:?}", order.user) != user_id {
                    Err(anyhow::anyhow!("invalid order id"))
                } else {
                    Ok(())
                }
            }
            TradingCommand::Ask {
                base,
                quote,
                amount,
                price,
            } => {
                let open = self
                    .markets
                    .get(&(*base, *quote))
                    .ok_or(anyhow::anyhow!("symbol not exists"))?;
                let market = open.value();
                let amount = Decimal::from_str(amount)?;
                let price = Decimal::from_str(price)?;
                anyhow::ensure!(
                    amount >= market.1.min_base
                        && price.is_sign_positive()
                        && price.scale() <= market.1.quote_scale.into()
                        && amount.is_sign_positive()
                        && amount.scale() <= market.1.base_scale.into(),
                    "invalid numeric"
                );
                let mut account = self.backend.get_account(user_id).await?;
                anyhow::ensure!(
                    account.remove(base).unwrap_or_default().available >= amount,
                    "insufficient balance"
                );
                Ok(())
            }
            TradingCommand::Bid {
                base,
                quote,
                amount,
                price,
            } => {
                let market = self
                    .markets
                    .get(&(*base, *quote))
                    .ok_or(anyhow::anyhow!("symbol not exists"))?;
                let amount = Decimal::from_str(amount)?;
                let price = Decimal::from_str(price)?;
                anyhow::ensure!(
                    amount >= market.1.min_base
                        && price.is_sign_positive()
                        && price.scale() <= market.1.quote_scale.into()
                        && amount.is_sign_positive()
                        && amount.scale() <= market.1.base_scale.into(),
                    "invalid numeric"
                );
                let mut account = self.backend.get_account(user_id).await?;
                anyhow::ensure!(
                    account.remove(quote).unwrap_or_default().available >= amount * price,
                    "insufficient balance"
                );
                Ok(())
            }
        }
    }
}

#[derive(Debug)]
pub struct BrokerSignatureVerifier<S> {
    pub backend: BackendConnection,
    pub inner: S,
}

#[derive(Debug, Clone)]
pub struct BrokerVerifyLayer {
    pub backend: BackendConnection,
}

impl BrokerVerifyLayer {
    pub fn new(backend: BackendConnection) -> Self {
        Self { backend }
    }
}

impl<S> Layer<S> for BrokerVerifyLayer {
    type Service = BrokerSignatureVerifier<S>;

    fn layer(&self, inner: S) -> Self::Service {
        BrokerSignatureVerifier {
            backend: self.backend.clone(),
            inner,
        }
    }
}

impl<S> Service<Request<Body>> for BrokerSignatureVerifier<S>
where
    S: Service<Request<Body>, Response = Response<Body>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<Box<dyn Error + Send + Sync>> + 'static + std::fmt::Debug,
{
    type Error = Box<dyn Error + Send + Sync + 'static>;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;
    type Response = S::Response;

    fn poll_ready(&mut self, cx: &mut TaskCtx<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(|e| e.into())
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let conn = self.backend.clone();
        let mut inner = self.inner.clone();
        Box::pin(async move {
            let nonce = req
                .headers()
                .get("X-Broker-Nonce")
                .map(|v| v.to_str().map_err(|_| anyhow::anyhow!("")))
                .ok_or(anyhow::anyhow!(""))
                .flatten()?;
            let signature = req
                .headers()
                .get("X-Broker-Signature")
                .ok_or(anyhow::anyhow!(""))
                .map(|v| v.to_str().map_err(|_| anyhow::anyhow!("")))
                .flatten()?;
            let ss58 = req
                .headers()
                .get("X-Broker-Account")
                .ok_or(anyhow::anyhow!(""))
                .map(|v| v.to_str().map_err(|_| anyhow::anyhow!("")))
                .flatten()?;
            let from_galois = conn.get_nonce(ss58).await.ok_or(anyhow::anyhow!(""))?;
            let nonce = nonce
                .parse::<u32>()
                .inspect_err(|e| log::debug!("{:?}", e))
                .map_err(|_| anyhow::anyhow!(""))?;
            if (from_galois as i64 - nonce as i64).abs() > 100 {
                return Err(anyhow::anyhow!("Nonce expired").into());
            }
            let sig_hex =
                hex::decode(signature.trim_start_matches("0x")).map_err(|_| anyhow::anyhow!(""))?;
            let raw_signature =
                Sr25519Signature::decode(&mut &sig_hex[..]).map_err(|_| anyhow::anyhow!(""))?;
            let public = AccountId32::from_ss58check(ss58)
                .map_err(|_| anyhow::anyhow!(""))
                .map(|a| Sr25519Public::from_raw(*a.as_ref()))?;
            let to_be_signed = nonce.encode();
            log::debug!("sr25519 pubkey: 0x{}", hex::encode(&public));
            log::debug!("to be signed: 0x{}", hex::encode(&to_be_signed));
            log::debug!("signature: 0x{}", hex::encode(&raw_signature));
            let verified = Sr25519Pair::verify(&raw_signature, to_be_signed, &public);
            log::debug!("verified: {}", verified);
            if verified {
                inner.call(req).await.map_err(|e| e.into())
            } else {
                Err(anyhow::anyhow!("Invalid signature").into())
            }
        })
    }
}

#[derive(Clone)]
pub struct Session {
    occupied_nonce: Arc<Mutex<BTreeSet<u32>>>,
}

impl Session {
    pub fn new(init_nonce: u32) -> Self {
        Self {
            occupied_nonce: Arc::new(Mutex::new(BTreeSet::from([init_nonce]))),
        }
    }

    pub async fn try_occupy_nonce(&self, nonce: u32) -> anyhow::Result<()> {
        let mut occupied_nonce = self.occupied_nonce.lock().await;
        if occupied_nonce.contains(&nonce) {
            Err(CustomRpcError::nonce_is_occupied(nonce))?
        } else if *occupied_nonce.first().expect("at least min;qed") > nonce {
            Err(CustomRpcError::nonce_is_expired(nonce))?
        } else {
            if occupied_nonce.len() > 100 {
                let evict = *occupied_nonce.first().expect("at least min;qed");
                occupied_nonce.remove(&evict);
            }
            occupied_nonce.insert(nonce);
            Ok(())
        }
    }

    pub async fn get_nonce(&self) -> u32 {
        let occupied_nonce = self.occupied_nonce.lock().await;
        *occupied_nonce.last().expect("at least max;qed") + 1
    }
}

#[test]
pub fn validate_signature_should_work() {
    let nonce = 83143.encode();
    let seed = "e5be9a5092b81bca64be81d212e7f2f9eba183bb7a90954f7b76361f6edb5c0a";
    let key: [u8; 32] = hex::decode(seed).unwrap().try_into().unwrap();
    let p = Sr25519Pair::from_seed(&key);
    let signature = p.sign(&nonce);
    println!("pubkey: 0x{}", hex::encode(&p.public()));
    println!("nonce: 0x{}", hex::encode(&nonce));
    println!("signature: 0x{}", hex::encode(&signature));
    assert!(Sr25519Pair::verify(&signature, nonce, &p.public()));
}

#[test]
pub fn validate_deser_signature_should_work() {
    let nonce = 83143.encode();
    let signature = "0x8a44a5e17f9bfa67330d9dbf28afee1e81ea86678beb240b4259cfcaa6c2753a3e2df60afd52171360372d3460b041fc3596ab41b6c7fc30142b091139ba5f89";
    let ss58 = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY";
    let public = Sr25519Public::from_raw(*AccountId32::from_ss58check(ss58).unwrap().as_ref());
    let signature = hex::decode(&signature.trim_start_matches("0x")).unwrap();
    let signature = Sr25519Signature::decode(&mut &signature[..]).unwrap();
    println!("pubkey: 0x{}", hex::encode(&public));
    println!("nonce: 0x{}", hex::encode(&nonce));
    println!(
        "signature: 0x{}",
        hex::encode::<&[u8; 64]>(signature.as_ref())
    );
    assert!(Sr25519Pair::verify(&signature, nonce, &public));
}
