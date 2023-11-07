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
use std::{sync::mpsc::Receiver, time::Duration};

pub fn init(rx: Receiver<Proof>, connector: FusoConnector, progress: Arc<FusoState>) {
    let mut pending: Vec<RawParameter> = Vec::with_capacity(C.fusotao.proof_batch_limit);
    std::thread::spawn(move || loop {
        let proof = match rx.recv_timeout(Duration::from_millis(10_000)) {
            Ok(p) => Some(p),
            Err(RecvTimeoutError::Timeout) => None,
            Err(RecvTimeoutError::Disconnected) => {
                log::error!("Proof committer interrupted!");
                break;
            }
        };
        // if C.dry_run.is_some() {
        //     if let Some(p) = proof {
        //         log::info!("{}(dry-run) => 0x{}", p.event_id, &hex::encode(p.root));
        //     }
        //     continue;
        // } else {
        //     append(proof, &mut pending);
        // }
    });
}

// fn start_submitting(api: FusoApi, proving_progress: Arc<AtomicU64>) {
//     let api = api.clone();
//     log::info!(
//         "submitting proofs from {}",
//         proving_progress.load(Ordering::Relaxed)
//     );
//     std::thread::spawn(move || loop {
//         let start_from = proving_progress.load(Ordering::Relaxed);
//         let new_max_submitted = std::panic::catch_unwind(|| -> u64 {
//             let (end_to, truncated) = Self::fetch_proofs(start_from);
//             if start_from == end_to {
//                 return end_to;
//             }
//             log::info!("[+] unsubmitted proofs [{}:{}] found", start_from, end_to);
//             let submit_result = Self::submit_batch(&api, truncated);
//             Self::handle_submit_result(submit_result, (start_from, end_to))
//         })
//         .unwrap_or(start_from);
//         if start_from == new_max_submitted {
//             std::thread::sleep(Duration::from_millis(1000));
//             continue;
//         }
//         proving_progress.store(new_max_submitted, Ordering::Relaxed);
//     });
// }

// fn compress_proofs(raws: Vec<RawParameter>) -> Vec<u8> {
//     let r = raws.encode();
//     let uncompress_size = r.len();
//     let compressed_proofs = lz4_flex::compress_prepend_size(r.as_ref());
//     let compressed_size = compressed_proofs.len();
//     log::info!(
//         "proof compress: uncompress size = {}, compressed size = {}",
//         uncompress_size,
//         compressed_size
//     );
//     compressed_proofs
// }

// fn submit_batch(api: &FusoApi, batch: Vec<RawParameter>) -> anyhow::Result<()> {
//     if batch.is_empty() {
//         return Ok(());
//     }
//     log::info!(
//         "[+] starting to submit_proofs at {}",
//         Local::now().timestamp_millis()
//     );
//     let hash = if C.fusotao.compress_proofs {
//         let xt: sub_api::UncheckedExtrinsicV4<_> = sub_api::compose_extrinsic!(
//             api,
//             "Verifier",
//             "verify_compress_v2",
//             Self::compress_proofs(batch)
//         );
//         api.send_extrinsic(xt.hex_encode(), sub_api::XtStatus::InBlock)
//             .map_err(|e| anyhow!("[-] submitting proofs failed, {:?}", e))?
//     } else {
//         let xt: sub_api::UncheckedExtrinsicV4<_> =
//             sub_api::compose_extrinsic!(api, "Verifier", "verify_v2", batch);
//         api.send_extrinsic(xt.hex_encode(), sub_api::XtStatus::InBlock)
//             .map_err(|e| anyhow!("[-] submitting proofs failed, {:?}", e))?
//     };
//     log::info!(
//         "[+] ending submit_proofs at {}",
//         Local::now().timestamp_millis()
//     );
//     if hash.is_none() {
//         Err(anyhow!(
//             "[-] verify extrinsic executed failed, no extrinsic returns"
//         ))
//     } else {
//         log::info!(
//             "[+] submitting proofs ok, extrinsic hash: {:?}",
//             hex::encode(hash.unwrap())
//         );
//         Ok(())
//     }
// }
