use sp_core::Pair;
use std::convert::TryFrom;
use sub_api::rpc::WsRpcClient;
use sub_api::{compose_extrinsic, Api, GenericAddress, Metadata, UncheckedExtrinsicV4, XtStatus};

pub type FusoAccountId = <sp_core::sr25519::Pair as sp_core::Pair>::Public;
pub type FusoAddress = sp_runtime::MultiAddress<FusoAccountId, ()>;

fn main() {
    //    env_logger::init();
    let client = WsRpcClient::new("ws://127.0.0.1:9944");
    let signer = sp_core::sr25519::Pair::from_string(
        "0x1670be58752b14252a6fdc18ba2e9f960813a518bfee5788344df94fb235920b",
        None,
    )
    .unwrap();
    let api = Api::<sp_core::sr25519::Pair, _>::new(client)
        .map(|api| api.set_signer(signer))
        .unwrap();
    let meta = Metadata::try_from(api.get_metadata().unwrap()).unwrap();

    meta.print_overview();
    meta.print_pallets();
    meta.print_pallets_with_calls();
    meta.print_pallets_with_events();
    meta.print_pallets_with_errors();
    meta.print_pallets_with_constants();

    // print full substrate metadata json formatted
    println!(
        "{}",
        Metadata::pretty_format(&api.get_metadata().unwrap())
            .unwrap_or_else(|| "pretty format failed".to_string())
    );

    let to = sp_core::sr25519::Pair::from_string(
        "0x8f381297652162278ceaff97865206a4f859845f4784eb97b63196b56266aeb8",
        None,
    )
    .unwrap();

    #[allow(clippy::redundant_clone)]
    let xt: UncheckedExtrinsicV4<_> = compose_extrinsic!(
        api.clone(),
        "Balances",
        "transfer",
        GenericAddress::Id(to.public().into()),
        Compact(1_000_000_000_000_000_000_u128)
    );

    println!("[+] Composed Extrinsic:\n {:?}\n", xt);

    // send and watch extrinsic until InBlock
    let tx_hash = api
        .send_extrinsic(xt.hex_encode(), XtStatus::InBlock)
        .unwrap();
    println!("[+] Transaction got included. Hash: {:?}", tx_hash);
}
