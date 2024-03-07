use std::path::Path;

use bitcoin::Txid;
use electrs::{
    config::Config, inscription_entries::inscription::InscriptionExtraData, new_index::DB,
    util::bincode_util,
};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tracing::error;

#[derive(Serialize, Deserialize)]
struct Shit {
    txid: Txid,
    vout: u32,
    offset: u64,
}

fn main() {
    let config = Config::from_args();

    let db = DB::open(Path::new("db/mainnet/newindex/inscription"), &config);

    let shit = db
        .iter_scan(&bincode_util::serialize_big(&("C")).unwrap())
        .map(|x| InscriptionExtraData::from_raw(x).unwrap())
        .collect_vec();

    error!("{:?}", shit.len());
}
