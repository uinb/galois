// Copyright 2023 UINB Technologies Pte. Ltd.

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

use galois_sidecar::*;

#[tokio::main]
async fn main() {
    env_logger::init();
    log::info!("Bye, {:?}", start().await);
}

async fn start() -> anyhow::Result<()> {
    let config = config::init_config_file()?;
    let bind_addr = config.bind_addr.clone();
    let context = context::Context::new(config);
    let builder = tower::ServiceBuilder::new()
        .layer(context::BrokerVerifyLayer::new(context.backend.clone()));
    let server = jsonrpsee::server::ServerBuilder::new()
        .ws_only()
        .set_middleware(builder)
        .max_connections(10000)
        .max_subscriptions_per_connection(1024)
        .build(bind_addr.parse::<std::net::SocketAddr>()?)
        .await?;
    server.start(endpoint::export_rpc(context))?.stopped().await;
    Ok(())
}
