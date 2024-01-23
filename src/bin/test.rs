use bitcoin::consensus::Decodable;
use bitcoin::hashes::Hash;
use bitcoin::Txid;
use electrs::config::Config;
use electrs::new_index::DB;
use electrs::{db_key, Inscription};
use itertools::Itertools;
use std::path::Path;
use std::str::FromStr;

fn main() -> anyhow::Result<()> {
    let config = Config::from_args();

    let txdb = DB::open(&Path::new("db/mainnet/newindex/txstore"), &config);

    let txs = vec![
        "b9402dfa7b51d32e146824b08327edb6b5471dd3442e176031bd5b7b36adb62d",
        "9348f24d035ee7c06d1e7abbf2f2d76d7842b2c0633650d976a6180e8628c9be",
        "675a523dea7273e9b01729ee783f3cbcf44173d6b1f7328fbbb990ea901b385d",
        "042f4cfd25954710d3a4d96fad4ebc0d62bd4890b82a08b0dc46de89899778a2",
        "62a6071c28ef86461e90a98f2c876d22ff105bc78f655edd369b0e35d60a5a12",
        "2ac4c1419b46cde72a7b117d663ec7339f0b402419768fdbc8924865b84adc78",
        "fc6db86fdd7432a56e64c9841f96b79d447373dd61c3bb440a4c69ee0eefae43",
        "93715beab8315dda984294f17624a8b26f4648269f5a923f8ca8fb5281050504",
    ];
    let txs = txs
        .into_iter()
        .rev()
        .map(|x| Txid::from_str(x).unwrap())
        .collect_vec();
    let txs = txs
        .into_iter()
        .map(|x| {
            let v = txdb.get(&db_key!("T", &x.into_inner())).unwrap();
            bitcoin::Transaction::consensus_decode(std::io::Cursor::new(v)).unwrap()
        })
        .collect_vec();

    dbg!(Inscription::from_transactions(txs));

    Ok(())
}
