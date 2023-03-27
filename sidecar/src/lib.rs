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
use sp_core::crypto::Ss58Codec;
pub use sp_core::sr25519::Pair;
pub use sp_core::sr25519::Public;
pub use sp_core::sr25519::Signature;
use sp_core::Pair as PairT;

pub fn hexstr_to_vec(h: impl AsRef<str>) -> anyhow::Result<Vec<u8>> {
    hex::decode(h.as_ref().trim_start_matches("0x")).map_err(|_| anyhow::anyhow!("invalid hex str"))
}

pub fn verify_sr25519(sig: Vec<u8>, data: &[u8], ss58: impl AsRef<str>) -> anyhow::Result<()> {
    let public = AccountId::from_ss58check(ss58.as_ref())
        .map_err(|_| anyhow::anyhow!(""))
        .map(|a| Public::from_raw(*a.as_ref()))?;
    let sig = Signature::decode(&mut &sig[..]).map_err(|_| anyhow::anyhow!(""))?;
    let verified = Pair::verify(&sig, data, &public);
    if verified {
        Ok(())
    } else {
        Err(anyhow::anyhow!("Invalid signature"))
    }
}
