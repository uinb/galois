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

pub use crate::{
    assets::Balance,
    fusotao::GlobalStates,
    matcher::{Role, State as OrderState},
    orderbook::AskOrBid,
    orderbook::OrderBook,
    sequence::InOrOut,
};
use flate2::{read::ZlibDecoder, write::ZlibEncoder, Compression};
use indexmap::IndexSet;
use rust_decimal::{prelude::Zero, Decimal};
use serde::{Deserialize, Serialize};
use sp_core::ByteArray;
use std::{
    collections::HashMap,
    fs::File,
    io::{BufReader, BufWriter},
};

pub type Base = u32;
pub type Quote = u32;
pub type Price = Decimal;
pub type Amount = Decimal;
pub type Vol = Decimal;
pub type Currency = u32;
pub type Symbol = (Base, Quote);
pub type EventId = u64;
pub type OrderId = u64;
pub type Fee = Decimal;
pub type Scale = u32;
pub type Timestamp = u64;
pub type Account = HashMap<Currency, Balance>;
pub type Accounts = HashMap<UserId, Account>;
pub type UserId = B256;

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize, Default)]
pub struct B256(pub [u8; 32]);

impl B256 {
    pub const fn zero() -> Self {
        Self([0; 32])
    }

    pub fn new(x: [u8; 32]) -> Self {
        Self(x)
    }

    pub fn from_hex_str(s: &str) -> anyhow::Result<Self> {
        let hex = s.trim_start_matches("0x");
        if hex.len() == 64 {
            let mut bytes = [0u8; 32];
            hex::decode_to_slice(hex, &mut bytes)
                .map_err(|_| anyhow::anyhow!("invalid hex string"))
                .map(|_| Self::from(bytes))
        } else {
            Err(anyhow::anyhow!("invalid hex string"))
        }
    }
}

impl std::ops::Deref for B256 {
    type Target = [u8; 32];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::str::FromStr for B256 {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        if s.starts_with("0x") {
            Self::from_hex_str(s)
        } else {
            use sp_core::crypto::Ss58Codec;
            Self::from_ss58check(s).map_err(|_| anyhow::anyhow!("invalid ss58 format"))
        }
    }
}

impl std::fmt::Debug for B256 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use sp_core::crypto::Ss58Codec;
        let s = self.to_ss58check();
        write!(f, "{}", &s)
    }
}

impl AsRef<[u8]> for B256 {
    fn as_ref(&self) -> &[u8] {
        &self.0[..]
    }
}

impl AsMut<[u8]> for B256 {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.0[..]
    }
}

impl AsRef<[u8; 32]> for B256 {
    fn as_ref(&self) -> &[u8; 32] {
        &self.0
    }
}

impl AsMut<[u8; 32]> for B256 {
    fn as_mut(&mut self) -> &mut [u8; 32] {
        &mut self.0
    }
}

impl From<[u8; 32]> for B256 {
    fn from(x: [u8; 32]) -> Self {
        Self::new(x)
    }
}

impl ByteArray for B256 {
    const LEN: usize = 32;
}

impl<'a> TryFrom<&'a [u8]> for B256 {
    type Error = ();

    fn try_from(value: &'a [u8]) -> Result<Self, Self::Error> {
        if value.len() != 32 {
            return Err(());
        }
        let mut out = [0u8; 32];
        out.copy_from_slice(&value[..]);
        return Ok(B256::new(out));
    }
}

impl sp_core::crypto::Ss58Codec for B256 {}

pub const SYSTEM: UserId = UserId::zero();

#[must_use]
pub fn max_number() -> Amount {
    u64::MAX.into()
}

// we only keep the last 1000 transfer_in/out receipts to remove duplicates
const RECEIPTS_RECORDS_CAPACITY: usize = 1000;
const MAX_OPEN_ORDERS_PER_SYMBOL: usize = 100;

#[derive(Clone, Debug)]
pub struct PendingOrder {
    order_id: u64,
    symbol: Symbol,
    direction: u8,
    create_timestamp: u64,
    amount: String,
    price: String,
    status: u8,
    matched_quote_amount: String,
    matched_base_amount: String,
    base_fee: Decimal,
    quote_fee: Decimal,
}

#[derive(Clone, Debug)]
pub struct Ephemeral {
    onchain_receipt_records: IndexSet<(u32, UserId)>,
    user_pending_orders: HashMap<(Symbol, UserId), HashMap<OrderId, PendingOrder>>,
}

impl Ephemeral {
    pub fn new() -> Self {
        Self {
            onchain_receipt_records: IndexSet::with_capacity(RECEIPTS_RECORDS_CAPACITY),
            user_pending_orders: HashMap::new(),
        }
    }

    pub fn save_receipt(&mut self, id: (u32, UserId)) -> bool {
        if self.onchain_receipt_records.len() >= RECEIPTS_RECORDS_CAPACITY {
            self.onchain_receipt_records.pop();
        }
        self.onchain_receipt_records.insert(id)
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Data {
    pub orderbooks: HashMap<Symbol, OrderBook>,
    pub accounts: Accounts,
    pub merkle_tree: GlobalStates,
    pub current_event_id: u64,
    pub tvl: Amount,
}

impl Data {
    pub fn new() -> Self {
        Self {
            orderbooks: HashMap::new(),
            accounts: HashMap::new(),
            merkle_tree: GlobalStates::default(),
            current_event_id: 0,
            tvl: Amount::zero(),
        }
    }

    pub fn from_raw(file: File) -> anyhow::Result<Self> {
        let reader = BufReader::new(file);
        let mut decompress = ZlibDecoder::new(reader);
        Ok(bincode::deserialize_from(&mut decompress)?)
    }

    pub fn into_raw(&self, file: File) -> anyhow::Result<()> {
        let writer = BufWriter::new(file);
        let mut compress = ZlibEncoder::new(writer, Compression::best());
        bincode::serialize_into(&mut compress, &self)?;
        Ok(())
    }
}

#[test]
pub fn test_dump() {
    use crate::orderbook::Order;
    use rust_decimal_macros::dec;

    let order = Order::new(0, UserId::zero(), Decimal::new(1, 0), Decimal::new(1, 0));
    let v = bincode::serialize(&order).unwrap();
    let des: Order = bincode::deserialize(&v).unwrap();
    assert_eq!(des, order);
    let orderbook = OrderBook::new(
        3,
        3,
        dec!(0.001),
        dec!(0.001),
        dec!(0.001),
        dec!(0.001),
        1,
        dec!(0.001),
        dec!(1.0),
        false,
        true,
    );
    let v = bincode::serialize(&orderbook).unwrap();
    let des: OrderBook = bincode::deserialize(&v).unwrap();
    assert_eq!(des, orderbook);
    let mut test = Data::new();
    test.orderbooks.insert(
        (101, 100),
        OrderBook::new(
            3,
            3,
            dec!(0.001),
            dec!(0.001),
            dec!(0.001),
            dec!(0.001),
            1,
            dec!(0.001),
            dec!(1.0),
            false,
            true,
        ),
    );
    let temp_dir = tempdir::TempDir::new(".").unwrap();
    let file_path = temp_dir.path().join("bin.gz");
    let temp_file = File::create(&file_path).unwrap();
    test.into_raw(temp_file).unwrap();
    let de = Data::from_raw(File::open(&file_path).unwrap()).unwrap();
    assert_eq!(test.orderbooks, de.orderbooks);
    assert_eq!(test.accounts, de.accounts);
}

#[test]
pub fn test_debug_b256_on_fusotao() {
    use std::str::FromStr;
    let u = B256::from_str("0x0ae466861e8397f1e3beadac1a49dc111beea3b62d34a6eb4b5be370f5aada30")
        .unwrap();
    assert_eq!(
        "5CJzBh1SeBJ5qKzEpz1yzk8dF45erM5VWzwz4Ef2Zs1y2nKQ",
        format!("{:?}", u)
    );
}
