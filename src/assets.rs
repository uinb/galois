// Copyright 2021 UINB Technologies Pte. Ltd.

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

use crate::{core::*, orderbook::AskOrBid};
use anyhow::{anyhow, ensure};
use rust_decimal::{prelude::Zero, Decimal};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq, Default)]
pub struct Account {
    pub available: Decimal,
    pub frozen: Decimal,
}

pub fn get_mut(accounts: &mut Accounts, user: UserId, currency: Currency) -> Option<&mut Account> {
    match accounts.get_mut(&user) {
        None => None,
        Some(account) => account.get_mut(&currency),
    }
}

pub fn get(accounts: &Accounts, user: UserId, currency: Currency) -> Option<&Account> {
    match accounts.get(&user) {
        None => None,
        Some(account) => account.get(&currency),
    }
}

pub fn get_all_to_owned(accounts: &Accounts, user: &UserId) -> Balances {
    accounts
        .get(user)
        .map_or(Balances::default(), |b| b.clone())
}

pub fn get_to_owned(accounts: &Accounts, user: &UserId, currency: Currency) -> Account {
    match accounts.get(user) {
        None => Account::default(),
        Some(account) => account
            .get(&currency)
            .map_or(Account::default(), |a| a.clone()),
    }
}

fn new_account() -> HashMap<Currency, Account> {
    HashMap::with_capacity(64)
}

fn init_wallet(available: Decimal) -> Account {
    Account {
        available,
        frozen: Zero::zero(),
    }
}

pub fn add_to_available(
    accounts: &mut Accounts,
    user: UserId,
    currency: Currency,
    amount: Decimal,
) -> bool {
    accounts
        .entry(user)
        .and_modify(|user_account| {
            user_account
                .entry(currency)
                .and_modify(|account| {
                    account.available += amount;
                })
                .or_insert_with(|| init_wallet(amount));
        })
        .or_insert_with(|| {
            let mut new_account = new_account();
            new_account.insert(currency, init_wallet(amount));
            new_account
        });
    true
}

pub fn deduct_available(
    accounts: &mut Accounts,
    user: UserId,
    currency: Currency,
    amount: Amount,
) -> bool {
    match get_mut(accounts, user, currency) {
        Some(account) => {
            if account.available < amount {
                false
            } else {
                account.available -= amount;
                true
            }
        }
        None => false,
    }
}

pub fn deduct_frozen(
    accounts: &mut Accounts,
    user: UserId,
    currency: Currency,
    amount: Amount,
) -> bool {
    match get_mut(accounts, user, currency) {
        Some(account) => {
            if account.frozen < amount {
                false
            } else {
                account.frozen -= amount;
                true
            }
        }
        None => false,
    }
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
    user: UserId,
    currency: Currency,
    amount: Amount,
) -> anyhow::Result<Account> {
    let account = get_mut(accounts, user, currency).ok_or(anyhow!("Account not found"))?;
    ensure!(account.available >= amount, anyhow!("Available not enough"));
    account.available -= amount;
    account.frozen += amount;
    Ok(account.clone())
}

pub fn unfreeze(accounts: &mut Accounts, user: UserId, currency: Currency, amount: Amount) -> bool {
    match get_mut(accounts, user, currency) {
        None => false,
        Some(account) => {
            if account.frozen < amount {
                false
            } else {
                account.available += amount;
                account.frozen -= amount;
                true
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::core::UserId;
    use rust_decimal_macros::dec;
    use serde_json;
    use std::str::FromStr;

    #[test]
    pub fn test_transfer() {
        let mut all = Accounts::new();
        add_to_available(&mut all, UserId::zero(), 101, dec!(1.11111));
        add_to_available(&mut all, UserId::zero(), 101, dec!(1.11111));
        add_to_available(&mut all, UserId::zero(), 101, dec!(1.11111));
        add_to_available(&mut all, UserId::zero(), 101, dec!(1.11111));
        add_to_available(&mut all, UserId::zero(), 101, dec!(1.11111));
        add_to_available(&mut all, UserId::zero(), 101, dec!(1.11111));
        add_to_available(&mut all, UserId::zero(), 101, dec!(1.11111));
        assert_eq!(
            get(&all, UserId::zero(), 101).unwrap().available,
            dec!(7.77777)
        );
        deduct_available(&mut all, UserId::zero(), 101, dec!(7.67777));
        assert_eq!(get(&all, UserId::zero(), 101).unwrap().available, dec!(0.1));
        let ok = deduct_available(&mut all, UserId::zero(), 101, dec!(1.0));
        assert!(!ok);
        assert_eq!(get(&all, UserId::zero(), 101).unwrap().available, dec!(0.1));
    }

    #[test]
    pub fn test_freeze() {
        let mut all = Accounts::new();
        add_to_available(&mut all, UserId::zero(), 101, dec!(1.11111));
        let r = try_freeze(&mut all, UserId::zero(), 101, dec!(0.00011));
        assert!(r.is_ok());
        let a = get(&all, UserId::zero(), 101);
        assert!(a.is_some());
        let a = a.unwrap();
        assert_eq!(a.available, dec!(1.111));
        assert_eq!(a.frozen, dec!(0.00011));
    }

    fn help(all: &mut Accounts, json: &str) {
        let cmd: crate::sequence::Command = serde_json::from_str(json).unwrap();
        if cmd.cmd == crate::sequence::TRANSFER_IN {
            add_to_available(
                all,
                UserId::from_str(&cmd.user_id.unwrap()).unwrap(),
                cmd.currency.unwrap(),
                cmd.amount.unwrap(),
            );
        } else if cmd.cmd == crate::sequence::TRANSFER_OUT {
            deduct_available(
                all,
                UserId::from_str(&cmd.user_id.unwrap()).unwrap(),
                cmd.currency.unwrap(),
                cmd.amount.unwrap(),
            );
        }
    }

    #[test]
    pub fn test_deser_from_json() {
        let mut all = Accounts::new();
        let s = r#"{"amount":"10000","cmd":11,"currency":101,"user_id":"0000000000000000000000000000000000000000000000000000000000000002"}"#;
        help(&mut all, s);
        let s = r#"{"amount":"3.41","cmd":11,"currency":101,"user_id":"0000000000000000000000000000000000000000000000000000000000000002"}"#;
        help(&mut all, s);
        let s = r#"{"amount":"4.39","cmd":10,"currency":101,"user_id":"0000000000000000000000000000000000000000000000000000000000000002"}"#;
        help(&mut all, s);
        let s = r#"{"amount":"2.47","cmd":11,"currency":101,"user_id":"0000000000000000000000000000000000000000000000000000000000000002"}"#;
        help(&mut all, s);
        let s = r#"{"amount":"3.65","cmd":10,"currency":101,"user_id":"0000000000000000000000000000000000000000000000000000000000000002"}"#;
        help(&mut all, s);
        let s = r#"{"amount":"1.99","cmd":11,"currency":101,"user_id":"0000000000000000000000000000000000000000000000000000000000000002"}"#;
        help(&mut all, s);
        let s = r#"{"amount":"3.81","cmd":10,"currency":101,"user_id":"0000000000000000000000000000000000000000000000000000000000000002"}"#;
        help(&mut all, s);
        assert_eq!(
            get(
                &all,
                UserId::from_str(
                    "0x0000000000000000000000000000000000000000000000000000000000000002"
                )
                .unwrap(),
                101
            )
            .unwrap()
            .available,
            dec!(9996.02)
        );
    }
}
