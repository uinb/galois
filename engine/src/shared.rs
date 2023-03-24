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

use crate::{cmd::*, core::*, fusotao::*, Command};
use serde_json::{json, to_vec};
use std::str::FromStr;
use std::sync::Arc;

/// Serve the sidechar, for some requests needn't to be put into the executor
#[derive(Clone, Debug)]
pub struct Shared {
    pub fuso_state: Arc<FusoState>,
    pub x25519_priv: String,
}

unsafe impl Send for Shared {}
unsafe impl Sync for Shared {}

impl Shared {
    pub fn new(fuso_state: Arc<FusoState>, x25519_priv: String) -> Self {
        Self {
            fuso_state,
            x25519_priv,
        }
    }

    /// query scanning and proving progress
    fn query_progress(&self) -> Vec<u8> {
        let ans = json!({
            "proving_progress": self.fuso_state.get_proving_progress(),
            "scanning_progress": self.fuso_state.get_scanning_progress(),
            "chain_height": self.fuso_state.get_chain_height(),
        });
        to_vec(&ans).unwrap()
    }

    /// this is for helping to reject invalid orders, not for providing human-readable information
    /// NOTE: this is a heavy operation because we have to clone the map to avoid potential deadlock
    fn query_open_markets(&self) -> Vec<u8> {
        let symbols = self.fuso_state.symbols.clone();
        let open = symbols
            .iter()
            .map(|r| (r.key().clone(), r.value().clone()))
            .collect::<Vec<_>>();
        to_vec(&open).expect("jsonser;qed")
    }

    fn query_market(&self, symbol: &Symbol) -> Vec<u8> {
        to_vec::<Option<OffchainSymbol>>(
            &self
                .fuso_state
                .symbols
                .get(symbol)
                .map(|v| (symbol.clone(), v.value().clone()).into()),
        )
        .expect("jsonser;qed")
    }

    /// retrieve the x25519 private key
    fn get_x25519_key(&self) -> Vec<u8> {
        to_vec(&json!({ "x25519": self.x25519_priv })).expect("jsonser;qed")
    }

    /// get the broker nonce
    fn get_nonce_for_broker(&self, broker: &UserId) -> Vec<u8> {
        let p = if self.fuso_state.brokers.contains_key(broker) {
            json!({"nonce": self.fuso_state.get_chain_height()})
        } else {
            json!({"nonce": -1})
        };
        to_vec(&p).expect("jsonser;qed")
    }

    pub fn handle_req(&self, cmd: &Command) -> anyhow::Result<Vec<u8>> {
        match cmd.cmd {
            QUERY_OPEN_MARKETS => Ok(self.query_open_markets()),
            GET_X25519_KEY => Ok(self.get_x25519_key()),
            QUERY_FUSOTAO_PROGRESS => Ok(self.query_progress()),
            GET_NONCE_FOR_BROKER => {
                let broker = UserId::from_str(cmd.user_id.as_ref().ok_or(anyhow::anyhow!(""))?)?;
                Ok(self.get_nonce_for_broker(&broker))
            }
            QUERY_PROVING_PERF_INDEX => {
                to_vec(&json!({"proving_perf_index": 0})).map_err(|e| e.into())
            }
            QUERY_SCAN_HEIGHT => to_vec(&json!({
                "scaned_height": self.fuso_state.get_scanning_progress(),
                "chain_height": self.fuso_state.get_chain_height(),
            }))
            .map_err(|e| e.into()),
            QUERY_MARKET => Ok(self.query_market(&cmd.symbol().ok_or(anyhow::anyhow!(""))?)),
            _ => Err(anyhow::anyhow!("")),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::str::FromStr;

    #[test]
    pub fn incr_broker_nonce_should_work() {
        let shared = Shared::new(
            Arc::new(Default::default()),
            "0xedcff0c69e4c0fa7e9a36e2e6d07f2cc355c8d25907a0ad2ab7e03b24f8e90f3".to_string(),
        );
        let broker = UserId::from_str("5DaYdJ1fXoFetSCaA44PrK6iQeTwg9AtjzLrxaQXooRrx9RK").unwrap();
        shared.fuso_state.brokers.insert(broker.clone(), 2);
        assert_eq!(
            serde_json::json!({"nonce": 2}),
            serde_json::from_slice::<serde_json::Value>(&shared.get_nonce_for_broker(&broker))
                .unwrap()
        );
        assert_eq!(3, *shared.fuso_state.brokers.get(&broker).unwrap());
        let broker = UserId::from_str("5FhfEqhp2Dt9e1FgL9EmnE6kRT6NJgSUPCTPMCCNqxrm3MQX").unwrap();
        assert_eq!(
            serde_json::json!({"nonce": -1}),
            serde_json::from_slice::<serde_json::Value>(&shared.get_nonce_for_broker(&broker))
                .unwrap()
        );
    }
}
