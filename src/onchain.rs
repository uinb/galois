use crate::core::*;
use rust_decimal::{prelude::*, Decimal};
use sha2::{Digest, Sha256};

pub const ONE_ONCHAIN: u128 = 1_000_000_000_000_000_000;
pub const SCALE_ONCHAIN: u32 = 18;
pub const ACCOUNT_KEY: u8 = 0x00;

pub fn d18() -> Amount {
    ONE_ONCHAIN.into()
}

pub fn to_merkle_represent(v: Decimal) -> Option<u128> {
    Some((v.fract() * d18()).to_u128()? + (v.floor().to_u128()? * ONE_ONCHAIN))
}

pub fn new_account_merkle_leaf(
    user_id: UserId,
    currency: u32,
    avaiable: u128,
    frozen: u128,
) -> MerkleLeaf {
    let mut hasher = Sha256::new();
    let mut value: [u8; 32] = Default::default();
    value.copy_from_slice(&[&avaiable.to_be_bytes()[..], &frozen.to_be_bytes()[..]].concat());
    hasher.update(&[ACCOUNT_KEY][..]);
    hasher.update(user_id.as_bytes());
    hasher.update(&currency.to_be_bytes()[..]);
    (hasher.finalize().into(), MerkleIdentity::from(value))
}
