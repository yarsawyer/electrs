use bitcoin::consensus::Decodable;
use bitcoin::hashes::hex::{FromHex, ToHex};
use bitcoin::hashes::Hash;
use bitcoin::{OutPoint, TxOut, Txid};
use electrs::{config::Config, new_index::schema::TxOutRow};
use electrs::new_index::DB;
use electrs::util::bincode_util;
use itertools::Itertools;
use std::path::Path;
use tracing::{error, warn};

fn main() -> anyhow::Result<()> {
    let config = Config::from_args();

    let idb = DB::open(&Path::new("db/mainnet/newindex/inscription"), &config);
    let txstore_db = DB::open(&Path::new("db/mainnet/newindex/txstore"), &config);
    
    let asd = txstore_db.get(&TxOutRow::key(&OutPoint{
        txid: Txid::from_hex("62fd0b346ee3a0acf997b22f69e491b5fd21d7b6d685894eda73e73a6c764237").unwrap(),
        vout: 0,
    }))
    .map(|val| bitcoin::consensus::deserialize::<TxOut>(&val).expect("failed to parse TxOut"))
    .unwrap();

    dbg!(asd);
    
    // let filter = bincode_util::serialize_big(&(b'O',"B86DEwBxcLYCfuGkZfpqhVeuKgj9fZ1EU2".as_bytes())).unwrap();
    // let mut i = 0;
    // idb.iter_scan(&filter).for_each(|x| {
    //     let txid = &x.key[x.key.len() - 32..];
    //     warn!("{}", Txid::from_slice(txid).unwrap());
    //     i+=1;
    // });
    // error!("count {i:?}");

    // for i in idb.iter_scan(b"PTTT") {
    //     let txid = Txid::from_slice(&i.key[4..])?;
    //     let txs: Vec<Txid> = i
    //         .value
    //         .chunks(32)
    //         .map(|x| Txid::from_slice(x))
    //         .try_collect()
    //         .unwrap();

    //     if txs[0].to_hex() == *"40665337473b95c88c1da40dcdf777be22fc682d59bd023a1c044acf2fbf6590" {
    //         warn!("genesis {} outpoint {} len {}", txs[0], txid, txs.len());
    //     }
    // }

    Ok(())
}
