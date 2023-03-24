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

#![feature(result_option_inspect)]
#![feature(result_flattening)]
#![feature(option_zip)]
pub mod backend;
pub mod config;
pub mod context;
mod db;
pub mod endpoint;
mod legacy_clearing;

use parity_scale_codec::Decode;
pub use sp_core::crypto::AccountId32 as AccountId;
use sp_core::hashing::blake2_256;
pub use sp_core::sr25519::Pair;
pub use sp_core::sr25519::Public;
pub use sp_core::sr25519::Signature;

pub type HexEncoded = String;

// TODO
pub fn verify_trading_sig_and_update_nonce(
    t: impl AsRef<str>,
    trading_key: &HexEncoded,
    nonce: &HexEncoded,
    sig: &HexEncoded,
    nonce_on_server: u64,
) -> (bool, u64) {
    let mut nonce = match hex::decode(nonce.trim_start_matches("0x")) {
        Ok(v) => v,
        Err(_) => return (false, nonce_on_server),
    };
    let n = match u64::decode(&mut nonce.clone().as_slice()) {
        Ok(n) => n,
        Err(_) => return (false, nonce_on_server),
    };
    if n < nonce_on_server {
        return (false, nonce_on_server);
    }
    let key = match hex::decode(trading_key.trim_start_matches("0x")) {
        Ok(v) => v,
        Err(_) => return (false, nonce_on_server),
    };
    let payload = match hex::decode(t.as_ref().trim_start_matches("0x")) {
        Ok(v) => v,
        Err(_) => return (false, nonce_on_server),
    };
    nonce.extend_from_slice(&key);
    nonce.extend_from_slice(&payload);
    let hash = blake2_256(&nonce);
    let sig = match hex::decode(sig.trim_start_matches("0x")) {
        Ok(s) => s,
        Err(_) => return (false, nonce_on_server),
    };
    if hash == sig.as_slice() {
        (true, n)
    } else {
        (false, nonce_on_server)
    }
}

pub fn hexstr_to_vec(h: impl AsRef<str>) -> anyhow::Result<Vec<u8>> {
    hex::decode(h.as_ref().trim_start_matches("0x")).map_err(|_| anyhow::anyhow!("invalid hex str"))
}
