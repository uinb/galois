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

use crate::core::*;
use rust_decimal::{prelude::Zero, Decimal};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct Account {
    pub available: Decimal,
    pub frozen: Decimal,
}

pub fn get_mut<'a>(
    accounts: &'a mut Accounts,
    user: u64,
    currency: u32,
) -> Option<&'a mut Account> {
    match accounts.get_mut(&user) {
        None => None,
        Some(account) => account.get_mut(&currency),
    }
}

pub fn get<'a>(accounts: &'a Accounts, user: u64, currency: u32) -> Option<&'a Account> {
    match accounts.get(&user) {
        None => None,
        Some(account) => account.get(&currency),
    }
}

fn new_account() -> HashMap<Currency, Account> {
    HashMap::with_capacity(64)
}

fn init_wallet(available: Decimal) -> Account {
    Account {
        available: available,
        frozen: Zero::zero(),
    }
}

pub fn add_to_available(
    accounts: &mut Accounts,
    user: u64,
    currency: u32,
    amount: Decimal,
) -> bool {
    if !accounts.contains_key(&user) {
        let mut account = new_account();
        account.insert(currency, init_wallet(amount));
        accounts.insert(user, account);
    } else {
        let accounts = accounts.get_mut(&user).unwrap();
        if accounts.contains_key(&currency) {
            accounts.get_mut(&currency).unwrap().available += amount;
        } else {
            accounts.insert(currency, init_wallet(amount));
        }
    }
    true
}

pub fn deduct_available(
    accounts: &mut Accounts,
    user: u64,
    currency: u32,
    amount: Decimal,
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

pub fn deduct_frozen(accounts: &mut Accounts, user: u64, currency: u32, amount: Decimal) -> bool {
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

pub fn freeze(accounts: &mut Accounts, user: u64, currency: u32, amount: Decimal) -> bool {
    match get_mut(accounts, user, currency) {
        None => false,
        Some(account) => {
            if account.available < amount {
                false
            } else {
                account.available -= amount;
                account.frozen += amount;
                true
            }
        }
    }
}

pub fn unfreeze(accounts: &mut Accounts, user: u64, currency: u32, amount: Decimal) -> bool {
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
    use crate::core::*;
    use rust_decimal::{prelude::Zero, Decimal};
    use serde_json;
    use std::str::FromStr;

    #[test]
    pub fn test_transfer() {
        let mut all = Accounts::new();
        add_to_available(&mut all, 1, 101, Decimal::from_str("1.11111").unwrap());
        add_to_available(&mut all, 1, 101, Decimal::from_str("1.11111").unwrap());
        add_to_available(&mut all, 1, 101, Decimal::from_str("1.11111").unwrap());
        add_to_available(&mut all, 1, 101, Decimal::from_str("1.11111").unwrap());
        add_to_available(&mut all, 1, 101, Decimal::from_str("1.11111").unwrap());
        add_to_available(&mut all, 1, 101, Decimal::from_str("1.11111").unwrap());
        add_to_available(&mut all, 1, 101, Decimal::from_str("1.11111").unwrap());
        assert_eq!(
            get(&all, 1, 101).unwrap().available,
            Decimal::from_str("7.77777").unwrap()
        );
        deduct_available(&mut all, 1, 101, Decimal::from_str("7.67777").unwrap());
        assert_eq!(
            get(&all, 1, 101).unwrap().available,
            Decimal::from_str("0.1").unwrap()
        );
        let ok = deduct_available(&mut all, 1, 101, Decimal::from_str("1.0").unwrap());
        assert_eq!(false, ok);
        assert_eq!(
            get(&all, 1, 101).unwrap().available,
            Decimal::from_str("0.1").unwrap()
        );
    }

    fn help(all: &mut Accounts, json: &str) {
        let cmd: crate::sequence::Command = serde_json::from_str(json).unwrap();
        if cmd.cmd == crate::sequence::TRANSFER_IN {
            add_to_available(
                all,
                cmd.user_id.unwrap(),
                cmd.currency.unwrap(),
                cmd.amount.unwrap(),
            );
        } else if cmd.cmd == crate::sequence::TRANSFER_OUT {
            deduct_available(
                all,
                cmd.user_id.unwrap(),
                cmd.currency.unwrap(),
                cmd.amount.unwrap(),
            );
        }
    }

    #[test]
    pub fn test_deser_from_json() {
        let mut all = Accounts::new();
        let s = r#"{"amount":"10000","cmd":11,"currency":101,"user_id":2}"#;
        help(&mut all, s);
        let s = r#"{"amount":"3.41","cmd":11,"currency":101,"user_id":2}"#;
        help(&mut all, s);
        let s = r#"{"amount":"4.39","cmd":10,"currency":101,"user_id":2}"#;
        help(&mut all, s);
        let s = r#"{"amount":"2.47","cmd":11,"currency":101,"user_id":2}"#;
        help(&mut all, s);
        let s = r#"{"amount":"3.65","cmd":10,"currency":101,"user_id":2}"#;
        help(&mut all, s);
        let s = r#"{"amount":"1.99","cmd":11,"currency":101,"user_id":2}"#;
        help(&mut all, s);
        let s = r#"{"amount":"3.81","cmd":10,"currency":101,"user_id":2}"#;
        help(&mut all, s);
        assert_eq!(
            get(&all, 2, 101).unwrap().available,
            Decimal::from_str("9996.02").unwrap()
        );
    }
}
