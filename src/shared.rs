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

use crate::{cmd::*, core::UserId, fusotao::FusoState, Command};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct Shared {
    pub fuso_state: Arc<FusoState>,
}

unsafe impl Send for Shared {}
unsafe impl Sync for Shared {}

impl Shared {
    pub fn new(fuso_state: Arc<FusoState>) -> Self {
        Self { fuso_state }
    }

    async fn query_progress(&self) -> Vec<u8> {
        vec![]
    }

    // this is for helping to reject invalid orders
    async fn query_open_markets(&self) -> Vec<u8> {
        vec![]
    }

    async fn get_x25519_key(&self) -> Vec<u8> {
        vec![]
    }

    async fn get_and_incr_broker_nonce(&self, broker: UserId) -> Vec<u8> {
        // self.fuso_state.brokers.get(&broker);
        vec![]
    }

    pub async fn handle_req(&self, cmd: &Command) -> anyhow::Result<Vec<u8>> {
        Ok(vec![])
        // match cmd {
        //     QUERY_OPEN_MARKETS => {}
        //     // used for handshaking with enduser
        //     ACQUIRE_X25519_KEY => {}
        //     // used for handshaking with broker
        //     GET_AND_INCR_BROKER_NONCE => {}
        // }
    }
}
