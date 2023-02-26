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

use anyhow::{anyhow, ensure};
use rust_decimal::prelude::Zero;
use serde::{Deserialize, Serialize};

use crate::{core::*, orderbook::AskOrBid};

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq, Default)]
pub struct Balance {
    pub available: Amount,
    pub frozen: Amount,
}

pub fn get_account_to_owned(accounts: &Accounts, user: &UserId) -> Account {
    accounts.get(user).map_or(Account::default(), |b| b.clone())
}

pub fn get_balance_to_owned(accounts: &Accounts, user: &UserId, currency: Currency) -> Balance {
    match accounts.get(user) {
        None => Balance::default(),
        Some(account) => account
            .get(&currency)
            .map_or(Balance::default(), |a| a.clone()),
    }
}

fn init_balance(available: Amount) -> Balance {
    Balance {
        available,
        frozen: Amount::zero(),
    }
}

pub fn add_to_available(
    accounts: &mut Accounts,
    user: &UserId,
    currency: Currency,
    amount: Amount,
) -> anyhow::Result<Balance> {
    accounts
        .entry(*user)
        .and_modify(|account| {
            account
                .entry(currency)
                .and_modify(|balance| {
                    balance.available += amount;
                })
                .or_insert_with(|| init_balance(amount));
        })
        .or_insert_with(|| {
            let mut account = Account::default();
            account.insert(currency, init_balance(amount));
            account
        })
        .get(&currency)
        .map(|b| b.clone())
        .ok_or(anyhow!(""))
}

pub fn deduct_available(
    accounts: &mut Accounts,
    user: &UserId,
    currency: Currency,
    amount: Amount,
) -> anyhow::Result<Balance> {
    let account = accounts.get_mut(user).ok_or(anyhow!(""))?;
    let balance = account.get_mut(&currency).ok_or(anyhow!(""))?;
    ensure!(
        balance.available >= amount,
        "Insufficient available balance"
    );
    balance.available -= amount;
    Ok(balance.clone())
}

pub fn deduct_frozen(
    accounts: &mut Accounts,
    user: &UserId,
    currency: Currency,
    amount: Amount,
) -> anyhow::Result<Balance> {
    let account = accounts.get_mut(user).ok_or(anyhow!(""))?;
    let balance = account.get_mut(&currency).ok_or(anyhow!(""))?;
    ensure!(balance.frozen >= amount, "Insufficient frozen balance");
    balance.frozen -= amount;
    Ok(balance.clone())
}

pub fn freeze_if(
    symbol: &Symbol,
    ask_or_bid: AskOrBid,
    price: Price,
    amount: Amount,
) -> (Currency, Amount) {
    match ask_or_bid {
        AskOrBid::Ask => (symbol.0, amount),
        AskOrBid::Bid => (symbol.1, price * amount),
    }
}

pub fn try_freeze(
    accounts: &mut Accounts,
    user: &UserId,
    currency: Currency,
    amount: Amount,
) -> anyhow::Result<Balance> {
    let account = accounts.get_mut(user).ok_or(anyhow!(""))?;
    let balance = account.get_mut(&currency).ok_or(anyhow!(""))?;
    ensure!(balance.available >= amount, anyhow!("Available not enough"));
    balance.available -= amount;
    balance.frozen += amount;
    Ok(balance.clone())
}

pub fn try_unfreeze(
    accounts: &mut Accounts,
    user: &UserId,
    currency: Currency,
    amount: Amount,
) -> anyhow::Result<Balance> {
    let account = accounts.get_mut(user).ok_or(anyhow!(""))?;
    let balance = account.get_mut(&currency).ok_or(anyhow!(""))?;
    ensure!(balance.frozen >= amount, anyhow!("Frozen not enough"));
    balance.available += amount;
    balance.frozen -= amount;
    Ok(balance.clone())
}

#[cfg(test)]
#[allow(unused_must_use)]
mod test {
    use std::str::FromStr;

    use rust_decimal_macros::dec;

    use crate::core::UserId;

    use super::*;

    #[test]
    pub fn test_transfer() {
        let mut all = Accounts::new();
        add_to_available(&mut all, &UserId::zero(), 101, dec!(1.11111));
        add_to_available(&mut all, &UserId::zero(), 101, dec!(1.11111));
        add_to_available(&mut all, &UserId::zero(), 101, dec!(1.11111));
        add_to_available(&mut all, &UserId::zero(), 101, dec!(1.11111));
        add_to_available(&mut all, &UserId::zero(), 101, dec!(1.11111));
        add_to_available(&mut all, &UserId::zero(), 101, dec!(1.11111));
        add_to_available(&mut all, &UserId::zero(), 101, dec!(1.11111));
        assert_eq!(
            get_balance_to_owned(&all, &UserId::zero(), 101).available,
            dec!(7.77777)
        );
        deduct_available(&mut all, &UserId::zero(), 101, dec!(7.67777)).unwrap();
        assert_eq!(
            get_balance_to_owned(&all, &UserId::zero(), 101).available,
            dec!(0.1)
        );
        let ok = deduct_available(&mut all, &UserId::zero(), 101, dec!(1.0));
        assert!(ok.is_err());
        assert_eq!(
            get_balance_to_owned(&all, &UserId::zero(), 101).available,
            dec!(0.1)
        );
    }

    #[test]
    pub fn test_freeze() {
        let mut all = Accounts::new();
        add_to_available(&mut all, &UserId::zero(), 101, dec!(1.11111));
        let r = try_freeze(&mut all, &UserId::zero(), 101, dec!(0.00011));
        assert!(r.is_ok());
        let a = get_balance_to_owned(&all, &UserId::zero(), 101);
        assert_eq!(a.available, dec!(1.111));
        assert_eq!(a.frozen, dec!(0.00011));
    }

    fn help(all: &mut Accounts, json: &str) {
        let cmd: crate::sequence::Command = serde_json::from_str(json).unwrap();
        if cmd.cmd == crate::sequence::TRANSFER_IN {
            add_to_available(
                all,
                &UserId::from_str(&cmd.user_id.unwrap()).unwrap(),
                cmd.currency.unwrap(),
                cmd.amount.unwrap(),
            );
        } else if cmd.cmd == crate::sequence::TRANSFER_OUT {
            deduct_available(
                all,
                &UserId::from_str(&cmd.user_id.unwrap()).unwrap(),
                cmd.currency.unwrap(),
                cmd.amount.unwrap(),
            )
            .unwrap();
        }
    }

    #[test]
    pub fn test_deser_from_json() {
        let mut all = Accounts::new();
        let s = r#"{"amount":"10000","cmd":11,"currency":101,"user_id":"0x0000000000000000000000000000000000000000000000000000000000000002"}"#;
        help(&mut all, s);
        let s = r#"{"amount":"3.41","cmd":11,"currency":101,"user_id":"0x0000000000000000000000000000000000000000000000000000000000000002"}"#;
        help(&mut all, s);
        let s = r#"{"amount":"4.39","cmd":10,"currency":101,"user_id":"0x0000000000000000000000000000000000000000000000000000000000000002"}"#;
        help(&mut all, s);
        let s = r#"{"amount":"2.47","cmd":11,"currency":101,"user_id":"0x0000000000000000000000000000000000000000000000000000000000000002"}"#;
        help(&mut all, s);
        let s = r#"{"amount":"3.65","cmd":10,"currency":101,"user_id":"0x0000000000000000000000000000000000000000000000000000000000000002"}"#;
        help(&mut all, s);
        let s = r#"{"amount":"1.99","cmd":11,"currency":101,"user_id":"0x0000000000000000000000000000000000000000000000000000000000000002"}"#;
        help(&mut all, s);
        let s = r#"{"amount":"3.81","cmd":10,"currency":101,"user_id":"0x0000000000000000000000000000000000000000000000000000000000000002"}"#;
        help(&mut all, s);
        assert_eq!(
            get_balance_to_owned(
                &all,
                &UserId::from_str(
                    "0x0000000000000000000000000000000000000000000000000000000000000002"
                )
                .unwrap(),
                101,
            )
            .available,
            dec!(9996.02)
        );
    }
}
