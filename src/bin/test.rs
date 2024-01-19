use std::path::Path;
use std::sync::Arc;

use bitcoin::BlockHash;
use electrs::new_index::{InscriptionUpdater, Store};
use electrs::util::bincode_util;
use electrs::{config::Config, new_index::DB};
use itertools::Itertools;

fn main() -> anyhow::Result<()> {
    // let config = Config::from_args();

    // let inscription_db = DB::open(&Path::new("db/mainnet/newindex/inscription"), &config);
    // let temp_db = DB::open(&Path::new("db/mainnet/newindex/temp"), &config);
    // let store = Arc::new(Store::open(&config.db_path.join("newindex"), &config));

    // let last_indexed_block:BlockHash = store
    //     .inscription_db()
    //     .get(b"ot")
    //     .map(|x| bitcoin::consensus::encode::deserialize(&x).expect("invalid chain tip in `ot`")).unwrap();
    // let block_height = store
    //     .get_block_height(last_indexed_block)
    //     .unwrap_or(config.first_inscription_block);

    // let shit = store
    //             .indexed_headers
    //             .read()
    //             .iter()
    //             .skip(last_indexed_block)
    //             .map(|x| *x.hash())
    //             .collect_vec();

    // let inscription_updater = InscriptionUpdater::new(store.inscription_db(), store.txstore_db(), store.temp_db())?;

    // inscription_updater.index_transaction_inscriptions(tx, txid, block_height)

    // let key = bincode_util::serialize_big(&(b'O', "B4VgP79yjzT6JFYMCmH6XKLhAfsAPkRDgx".as_bytes()))
    //     .unwrap();

    // dbg!(&key);

    // Ok(())
    todo!()
}
