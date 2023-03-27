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
    backend::BackendConnection,
    config::Config,
    db::{self, Order},
    endpoint::TradingCommand,
    legacy_clearing, AccountId, Pair, Public, Signature,
};
use dashmap::DashMap;
use galois_engine::{core::*, fusotao::OffchainSymbol};
use hyper::{Body, Request, Response};
use parity_scale_codec::{Decode, Encode};
use rust_decimal::Decimal;
use sp_core::crypto::{Pair as Crypto, Ss58Codec};
use sqlx::{MySql, Pool};
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
use tokio::sync::{mpsc::UnboundedSender, Mutex};
use tower::{Layer, Service};
use x25519_dalek::StaticSecret;

pub struct Context {
    pub backend: BackendConnection,
    pub x25519: StaticSecret,
    pub db: Pool<MySql>,
    pub subscribers: Arc<DashMap<String, UnboundedSender<Order>>>,
    pub session_nonce: Arc<DashMap<String, Session>>,
    pub markets: Arc<DashMap<Symbol, (Arc<AtomicBool>, OffchainSymbol)>>,
}

impl Context {
    pub fn new(config: Config) -> Self {
        let backend = BackendConnection::new(config.prover);
        let conn = backend.clone();
        let x25519 = futures::executor::block_on(async move { conn.get_x25519().await }).unwrap();
        let db = futures::executor::block_on(async { Pool::connect(&config.db).await }).unwrap();
        let subscribers = Arc::new(DashMap::default());
        let conn = backend.clone();
        let markets = futures::executor::block_on(async move {
            conn.get_markets().await.map(|markets| {
                Arc::new(DashMap::from_iter(markets.into_iter().map(|m| {
                    (m.symbol.clone(), (Arc::new(AtomicBool::new(false)), m))
                })))
            })
        })
        .unwrap();
        markets.iter().for_each(|e| {
            let pool = db.clone();
            let sub = subscribers.clone();
            let symbol = e.key().clone();
            let closed = e.value().0.clone();
            tokio::spawn(
                async move { legacy_clearing::update_order_task(sub, pool, symbol, closed) },
            );
        });
        let conn = backend.clone();
        let started = markets.clone();
        let pool = db.clone();
        let sub = subscribers.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(15000)).await;
                match conn.get_markets().await {
                    Ok(v) if started.len() != v.len() => {
                        for m in v.into_iter() {
                            if !started.contains_key(&m.symbol) {
                                let closed = Arc::new(AtomicBool::new(false));
                                let symbol = m.symbol.clone();
                                started.insert(symbol.clone(), (closed.clone(), m));
                                let sub = sub.clone();
                                let pool = pool.clone();
                                tokio::spawn(async move {
                                    legacy_clearing::update_order_task(sub, pool, symbol, closed)
                                });
                            }
                        }
                    }
                    Err(e) => log::error!("fetching markets failed(background task), {:?}", e),
                    _ => {}
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
            .ok_or_else(|| anyhow::anyhow!("user not found"))?;
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
        let session = self
            .session_nonce
            .get(user_id)
            .ok_or(anyhow::anyhow!("user not found"))?;
        session.value().try_occupy_nonce(n).await?;
        let key = self.get_trading_key(user_id).await?;
        let mut to_be_signed = vec![];
        to_be_signed.extend_from_slice(data);
        to_be_signed.extend_from_slice(key.as_slice());
        to_be_signed.extend_from_slice(nonce);
        let hash = sp_core::blake2_256(&to_be_signed);
        anyhow::ensure!(hash.as_slice() == sig, "invalid signature");
        Ok(())
    }

    pub async fn validate_cmd(&self, cmd: &TradingCommand) -> anyhow::Result<()> {
        match cmd {
            TradingCommand::Cancel {
                account_id,
                base,
                quote,
                order_id,
            } => {
                let order = self
                    .backend
                    .get_order((*base, *quote), *order_id)
                    .await?
                    .ok_or(anyhow::anyhow!("order not exists"))?;
                if order.user
                    != UserId::from_str(&account_id)
                        .map_err(|_| anyhow::anyhow!("invalid user id"))?
                {
                    Err(anyhow::anyhow!("invalid order id"))
                } else {
                    Ok(())
                }
            }
            TradingCommand::Ask {
                account_id,
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
                let mut account = self.backend.get_account(account_id).await?;
                anyhow::ensure!(
                    account.remove(base).unwrap_or_default().available >= amount,
                    "insufficient balance"
                );
                Ok(())
            }
            TradingCommand::Bid {
                account_id,
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
                let mut account = self.backend.get_account(account_id).await?;
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

    fn poll_ready(&mut self, _cx: &mut TaskCtx<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let conn = self.backend.clone();
        let mut inner = self.inner.clone();
        Box::pin(async move {
            let nonce = req
                .headers()
                .get("x-broker-nonce")
                .map(|v| v.to_str().map_err(|_| anyhow::anyhow!("")))
                .ok_or(anyhow::anyhow!(""))
                .flatten()?;
            let signature = req
                .headers()
                .get("x-broker-signature")
                .ok_or(anyhow::anyhow!(""))
                .map(|v| v.to_str().map_err(|_| anyhow::anyhow!("")))
                .flatten()?;
            let ss58 = req
                .headers()
                .get("x-broker-account")
                .ok_or(anyhow::anyhow!(""))
                .map(|v| v.to_str().map_err(|_| anyhow::anyhow!("")))
                .flatten()?;
            log::debug!("address: {}", ss58);
            log::debug!("nonce: {}", nonce);
            log::debug!("signature: {}", signature);
            let from_galois = conn.get_nonce(ss58).await.ok_or(anyhow::anyhow!(""))?;
            let nonce = nonce
                .parse::<u32>()
                .inspect_err(|e| log::debug!("{:?}", e))
                .map_err(|_| anyhow::anyhow!(""))?;
            if from_galois < nonce || from_galois - nonce > 100 {
                return Err(anyhow::anyhow!("Nonce expired").into());
            }
            let sig_hex =
                hex::decode(signature.trim_start_matches("0x")).map_err(|_| anyhow::anyhow!(""))?;
            let raw_signature =
                Signature::decode(&mut &sig_hex[..]).map_err(|_| anyhow::anyhow!(""))?;
            let public = AccountId::from_ss58check(ss58)
                .map_err(|_| anyhow::anyhow!(""))
                .map(|a| Public::from_raw(*a.as_ref()))?;
            let to_be_signed = nonce.encode();
            log::debug!("sr25519 pubkey: 0x{}", hex::encode(&public));
            log::debug!("to be signed: 0x{}", hex::encode(&to_be_signed));
            log::debug!("signature: 0x{}", hex::encode(&raw_signature));
            let verified = Pair::verify(&raw_signature, to_be_signed, &public);
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
            Err(anyhow::anyhow!("Nonce {} is occupied", nonce))
        } else if *occupied_nonce.first().expect("at least min;qed") > nonce {
            Err(anyhow::anyhow!("Nonce {} is expired", nonce))
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
    let p = Pair::from_seed(&key);
    let signature = p.sign(&nonce);
    println!("pubkey: 0x{}", hex::encode(&p.public()));
    println!("nonce: 0x{}", hex::encode(&nonce));
    println!("signature: 0x{}", hex::encode(&signature));
    assert!(Pair::verify(&signature, nonce, &p.public()));
}

#[test]
pub fn validate_deser_signature_should_work() {
    let nonce = 83143.encode();
    let signature = "0x8a44a5e17f9bfa67330d9dbf28afee1e81ea86678beb240b4259cfcaa6c2753a3e2df60afd52171360372d3460b041fc3596ab41b6c7fc30142b091139ba5f89";
    let ss58 = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY";
    let public = Public::from_raw(*AccountId::from_ss58check(ss58).unwrap().as_ref());
    let signature = hex::decode(&signature.trim_start_matches("0x")).unwrap();
    let signature = Signature::decode(&mut &signature[..]).unwrap();
    println!("pubkey: 0x{}", hex::encode(&public));
    println!("nonce: 0x{}", hex::encode(&nonce));
    println!(
        "signature: 0x{}",
        hex::encode::<&[u8; 64]>(signature.as_ref())
    );
    assert!(Pair::verify(&signature, nonce, &public));
}
