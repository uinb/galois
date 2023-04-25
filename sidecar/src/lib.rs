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
mod errors;

use anyhow::anyhow;
use parity_scale_codec::{Decode, Encode};
pub use sp_core::crypto::AccountId32;
pub use sp_core::ecdsa::{Pair as EcdsaPair, Public as EcdsaPublic, Signature as EcdsaSignature};
pub use sp_core::sr25519::{
    Pair as Sr25519Pair, Public as Sr25519Public, Signature as Sr25519Signature,
};
use sp_core::{crypto::Ss58Codec, Pair};
use crate::errors::CustomRpcError;

pub fn hexstr_to_vec(h: impl AsRef<str>) -> anyhow::Result<Vec<u8>> {
    hex::decode(h.as_ref().trim_start_matches("0x")).map_err(|_| anyhow::anyhow!("invalid hex str"))
}

pub fn to_hexstr<T: Encode>(t: T) -> String {
    format!("0x{}", hex::encode(t.encode()))
}

pub fn verify_sr25519(sig: Vec<u8>, data: &[u8], ss58: impl AsRef<str>) -> anyhow::Result<()> {
    let public = AccountId32::from_ss58check(ss58.as_ref())
        .map_err(|_| anyhow::anyhow!(""))
        .map(|a| Sr25519Public::from_raw(*a.as_ref()))?;
    let sig = Sr25519Signature::decode(&mut &sig[..])
        .map_err(|_| anyhow::anyhow!("Invalid signature"))?;
    let verified = Sr25519Pair::verify(&sig, data, &public);
    if verified {
        Ok(())
    } else {
        Err(anyhow!(CustomRpcError::invalid_signature()))
    }
}

#[cfg(feature = "testenv")]
const LEGACY_MAPPING_CODE: u16 = 5;
#[cfg(not(feature = "testenv"))]
const LEGACY_MAPPING_CODE: u16 = 1;

pub fn verify_ecdsa(sig: Vec<u8>, data: &str, mapping_addr: impl AsRef<str>) -> anyhow::Result<()> {
    let sig = EcdsaSignature::decode(&mut &sig[..])
        .map_err(|_| anyhow::anyhow!("Invalid ECDSA signature"))?;
    let wrapped_msg = [
        &[0x19u8][..],
        &format!("Ethereum Signed Message:\n{}{}", data.len(), data).as_bytes()[..],
    ]
    .concat();
    let digest = sp_core::hashing::keccak_256(&wrapped_msg[..]);
    let pubkey = sp_io::crypto::secp256k1_ecdsa_recover(&sig.0, &digest)
        .map_err(|_| anyhow!("Invalid ECDSA signature"))?;
    log::debug!(" metamask public === {}", hex::encode(pubkey));
    let addr = sp_io::hashing::keccak_256(pubkey.as_ref())[12..].to_vec();
    log::debug!("recovered eth address{}", hex::encode(addr.clone()));
    let addr = to_mapping_address(addr);
    if addr.to_ss58check() == mapping_addr.as_ref() {
        Ok(())
    } else {
        Err(anyhow::anyhow!(CustomRpcError::invalid_signature()))
    }
}

pub fn try_into_ss58(addr: String) -> anyhow::Result<String> {
    if addr.starts_with("0x") {
        let addr = hexstr_to_vec(&addr)?;
        match addr.len() {
            32 => {
                let addr = AccountId32::decode(&mut &addr[..])
                    .map_err(|_| anyhow::anyhow!("Invalid substrate address"))?;
                Ok(addr.to_ss58check())
            }
            20 => {
                let addr = to_mapping_address(addr);
                Ok(addr.to_ss58check())
            }
            _ => Err(anyhow::anyhow!("Invalid address")),
        }
    } else {
        Ok(addr)
    }
}

pub fn to_mapping_address(address: Vec<u8>) -> AccountId32 {
    let h = (b"-*-#fusotao#-*-", LEGACY_MAPPING_CODE, address)
        .using_encoded(sp_core::hashing::blake2_256);
    Decode::decode(&mut h.as_ref()).expect("32 bytes; qed")
}