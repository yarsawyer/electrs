use bitcoin::consensus::Decodable;
use bitcoin::hashes::hex::ToHex;
use bitcoin::hashes::Hash;
use bitcoin::Txid;
use electrs::config::Config;
use electrs::new_index::DB;
use itertools::Itertools;
use std::path::Path;
use tracing::warn;

fn main() -> anyhow::Result<()> {
    let config = Config::from_args();

    let idb = DB::open(&Path::new("db/mainnet/newindex/inscription"), &config);

    for i in idb.iter_scan(b"PTTT") {
        let txid = Txid::from_slice(&i.key[4..])?;
        let txs: Vec<Txid> = i
            .value
            .chunks(32)
            .map(|x| Txid::from_slice(x))
            .try_collect()
            .unwrap();

        if txs[0].to_hex() == *"40665337473b95c88c1da40dcdf777be22fc682d59bd023a1c044acf2fbf6590" {
            warn!("genesis {} outpoint {} len {}", txs[0], txid, txs.len());
        }
    }

    Ok(())
}
