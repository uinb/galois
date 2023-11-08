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

use crate::{config::C, fusotao::*};
use sp_core::Pair;
use std::time::Duration;

/// since we won't wait for the proofs to be `Finalized`, we must add a watchdog to revert `proved_event_id`
pub fn init(connector: FusoConnector, progress: Arc<FusoState>) {
    if C.dry_run.is_some() {
        return;
    }
    let progress = progress.proved_event_id.clone();
    loop {
        let proved_id = progress.load(Ordering::Relaxed);
        let v = prover::fetch_raw_ge(proved_id + 1);
        if v.is_empty() {
            break;
        }
        match submit(&connector, v, true) {
            Ok(n) if n > proved_id => {
                progress.store(n, Ordering::Relaxed);
            }
            Ok(n) => {
                log::error!("expecting proof {} to be verified but failed", n);
                panic!("initializing committer failed");
            }
            Err(e) => {
                log::error!("submitting proofs failed due to {}, retrying...", e);
            }
        }
    }
    let local = progress.clone();
    let conn = connector.clone();
    std::thread::spawn(move || -> anyhow::Result<()> {
        loop {
            let id = local.load(Ordering::Relaxed);
            let v = prover::fetch_raw_ge(id + 1);
            if v.is_empty() {
                std::thread::sleep(Duration::from_millis(3000));
                continue;
            }
            match submit(&conn, v, false) {
                Ok(n) => local.store(n, Ordering::Relaxed),
                Err(e) => {
                    log::error!("submitting proofs failed due to {}, retrying...", e);
                }
            }
        }
    });
    std::thread::spawn(move || -> anyhow::Result<()> {
        loop {
            std::thread::sleep(Duration::from_secs(60));
            if let Ok(remote) = connector.sync_progress() {
                let local = progress.load(Ordering::Relaxed);
                if remote < local {
                    progress.store(remote, Ordering::Relaxed);
                }
                let _ = prover::remove_before(remote);
            }
        }
    });
}

fn compress_proofs(raws: Vec<RawParameter>) -> Vec<u8> {
    let r = raws.encode();
    let origin_size = r.len();
    let compressed_proofs = lz4_flex::compress_prepend_size(r.as_ref());
    let compressed_size = compressed_proofs.len();
    log::info!(
        "compressing proofs: origin size = {}, compressed size = {}",
        origin_size,
        compressed_size
    );
    compressed_proofs
}

fn submit(
    connector: &FusoConnector,
    batch: Vec<(u64, RawParameter)>,
    finalized: bool,
) -> anyhow::Result<u64> {
    anyhow::ensure!(!batch.is_empty(), "empty batch is not allowed");
    let (id, proofs): (Vec<u64>, Vec<RawParameter>) = batch.into_iter().unzip();
    log::debug!("submitting proofs at {}", chrono::Local::now());
    let payload: sub_api::UncheckedExtrinsicV4<_> = sub_api::compose_extrinsic!(
        connector.api,
        "Verifier",
        "verify_compress_v2",
        compress_proofs(proofs)
    );
    if finalized {
        connector
            .api
            .send_extrinsic(payload.hex_encode(), sub_api::XtStatus::Finalized)?;
        connector.sync_progress()
    } else {
        connector
            .api
            .send_extrinsic(payload.hex_encode(), sub_api::XtStatus::InBlock)?;
        Ok(id.last().copied().unwrap_or_default())
    }
}
