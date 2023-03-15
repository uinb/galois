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

use crate::core::{Currency, Symbol};
use parity_scale_codec::{Decode, Encode};

#[derive(Clone, Encode, Decode, Eq, PartialEq, Debug)]
pub enum MarketStatus {
    Registered,
    Open,
    Closed,
}

#[derive(Clone, Decode, Encode, Debug)]
pub struct OnchainSymbol {
    pub min_base: u128,
    pub base_scale: u8,
    pub quote_scale: u8,
    pub status: MarketStatus,
    pub trading_rewards: bool,
    pub liquidity_rewards: bool,
    pub unavailable_after: Option<super::BlockNumber>,
}

#[derive(Encode, Decode, Clone, PartialEq, Eq, Debug)]
pub enum OnchainToken {
    // symbol, contract_address, total, stable, decimals
    NEP141(Vec<u8>, Vec<u8>, u128, bool, u8),
    ERC20(Vec<u8>, Vec<u8>, u128, bool, u8),
    BEP20(Vec<u8>, Vec<u8>, u128, bool, u8),
    FND10(Vec<u8>, u128),
}
