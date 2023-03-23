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

use crate::{cmd::*, core::*, Command};
use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct Whistle {
    pub session: u64,
    pub req_id: u64,
    pub cmd: Command,
}

impl TryInto<Inspection> for Whistle {
    type Error = anyhow::Error;

    fn try_into(self) -> anyhow::Result<Inspection> {
        match self.cmd.cmd {
            QUERY_ORDER => Ok(Inspection::QueryOrder(
                self.cmd.symbol().ok_or(anyhow!(""))?,
                self.cmd.order_id.ok_or(anyhow!(""))?,
                self.session,
                self.req_id,
            )),
            QUERY_BALANCE => Ok(Inspection::QueryBalance(
                UserId::from_str(self.cmd.user_id.as_ref().ok_or(anyhow!(""))?)?,
                self.cmd.currency.ok_or(anyhow!(""))?,
                self.session,
                self.req_id,
            )),
            QUERY_ACCOUNTS => Ok(Inspection::QueryAccounts(
                UserId::from_str(self.cmd.user_id.as_ref().ok_or(anyhow!(""))?)?,
                self.session,
                self.req_id,
            )),
            UPDATE_DEPTH => Ok(Inspection::UpdateDepth),
            CONFIRM_ALL => Ok(Inspection::ConfirmAll(
                self.cmd.from.ok_or(anyhow!(""))?,
                self.cmd.exclude.ok_or(anyhow!(""))?,
            )),
            QUERY_EXCHANGE_FEE => Ok(Inspection::QueryExchangeFee(
                self.cmd.symbol().ok_or(anyhow!(""))?,
                self.session,
                self.req_id,
            )),
            DUMP => Ok(Inspection::Dump(
                self.cmd.event_id.ok_or(anyhow!(""))?,
                self.cmd.timestamp.ok_or(anyhow!(""))?,
            )),
            _ => Err(anyhow!("Invalid Inspection")),
        }
    }
}

impl Whistle {
    pub fn new_update_depth_whistle() -> Self {
        let mut cmd = Command::default();
        cmd.cmd = UPDATE_DEPTH;
        Self {
            session: 0,
            req_id: 0,
            cmd,
        }
    }

    pub fn new_dump_whistle(at: u64, time: u64) -> Self {
        let mut cmd = Command::default();
        cmd.cmd = DUMP;
        cmd.event_id = Some(at);
        cmd.timestamp = Some(time);
        Self {
            session: 0,
            req_id: 0,
            cmd,
        }
    }

    pub fn new_confirm_whistle(from: u64, exclude: u64) -> Self {
        let mut cmd = Command::default();
        cmd.cmd = CONFIRM_ALL;
        cmd.from.replace(from);
        cmd.exclude.replace(exclude);
        Self {
            session: 0,
            req_id: 0,
            cmd,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Deserialize, Serialize, Copy)]
pub enum Inspection {
    ConfirmAll(u64, u64),
    UpdateDepth,
    QueryOrder(Symbol, OrderId, u64, u64),
    QueryBalance(UserId, Currency, u64, u64),
    QueryAccounts(UserId, u64, u64),
    QueryExchangeFee(Symbol, u64, u64),
    // special: `EventId` means dump at `EventId`
    Dump(EventId, Timestamp),
}

impl Default for Inspection {
    fn default() -> Self {
        Self::UpdateDepth
    }
}
