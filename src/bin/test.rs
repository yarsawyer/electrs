use std::{path::Path, str::FromStr};

use bitcoin::{consensus::Decodable, hashes::Hash, OutPoint, Txid};
use electrs::{
    config::Config,
    inscription_entries::{
        inscription::{InscriptionExtraData, PartialTxs},
        ParsedInscription,
    },
    new_index::{schema::TxRow, DBRow, DB},
    Inscription,
};
use itertools::Itertools;
use tracing::{error, warn};

fn main() -> anyhow::Result<()> {
    let config = Config::from_args();
    let db = DB::open(Path::new("db/mainnet/newindex/txstore"), &config);

    let mut txs = [
        "041e0f86f267509a1f5c532442e6ff81504868e7c17c70baf37c039d9851cfd4",
        "556afb758617a2992986848d4c474d160b55608d90dbae51791b387211c9bad7",
        "f51ea1d5027b339d74b0cc7e5769ebad7d5001b4d07a6797c7be02e8c9b1a59f",
        "228296eac5c5f7fb7aef27a04c54bb3778657fc1d9ec59c7de9c2f203b7ba7d5",
        "55216907cf3f883d11967ed109f525fbe2f99fe034fac145dfef513a66072f4c",
        "30208ce2775134041795ada1e603c830a0f8041c63c60068bfe20c0eccfad03d",
        "48006887026db8489d7203127b6bba0811f59f03d2ca16829d0e5af1f7f5765c",
        "632e631e02091fa41132b6ce3d51ed39f6683aca2a6367809c8084d0c0ba8e0f",
        "2666d8353adcaec1a095984d6e2f4f61e12a0f8d12e2e0310d67528e6fa6f3c4",
    ]
    .map(|x| Txid::from_str(x).unwrap())
    .map(|x| TxRow::key(&x.into_inner()))
    .map(|x| db.get(&x).unwrap())
    .map(|x| bitcoin::Transaction::consensus_decode(std::io::Cursor::new(&x)).unwrap())
    .to_vec();

    txs.reverse();

    let ins_db = DB::open(Path::new("db/mainnet/newindex/inscription"), &config);

    if txs
        .iter()
        .map(|x| x.txid())
        .map(|x| {
            ins_db.get(
                &PartialTxs {
                    block_height: 0,
                    last_outpoint: OutPoint { txid: x, vout: 0 },
                    txs: vec![],
                }
                .get_db_key(),
            )
        })
        .any(|x| x.is_some())
    {
        error!("SHIT");
    } else {
        warn!("NO SHIT");
    }

    // let res = Inscription::from_transactions(&txs.iter().collect_vec());

    // if let ParsedInscription::Complete(_) = res {
    //     error!("SHIIIT");
    // } else {
    //     warn!("NO SHIT")
    // }

    // let key = InscriptionExtraData::get_db_key(OutPoint {
    //     txid: Txid::from_str("2666d8353adcaec1a095984d6e2f4f61e12a0f8d12e2e0310d67528e6fa6f3c4")
    //         .unwrap(),
    //     vout: 0,
    // });

    // let shit = db.get(&key).unwrap();

    // let shit = InscriptionExtraData::from_raw(DBRow { key, value: shit }).unwrap();

    Ok(())
}
