use std::path::Path;

use bitcoin::{hashes::hex::FromHex, Txid};
use electrs::util::bincode_util;
use electrs::{
    config::Config,
    new_index::{schema::OrdHistoryRow, DB},
};
use itertools::Itertools;

fn main() {
    let shit = OrdHistoryRow::new(
        "B6921GzsGznwMAKyVLVmbXrNpfqjhD5u5w".to_string(),
        22490,
        Txid::from_hex("2b14ef8ca9fadcc0988faec94e14b73971b6e28c21b585917871b2114938ae4d").unwrap(),
        100000,
    );

    let config = Config::from_args();

    let db = DB::open(&Path::new("db/mainnet/newindex/inscription"), &config);

    let key = bincode_util::serialize_big(&(b'O', "B4VgP79yjzT6JFYMCmH6XKLhAfsAPkRDgx".as_bytes()))
        .unwrap();

    dbg!(&key);

    let shit = db
        .iter_scan(&key)
        .take(50)
        .map(OrdHistoryRow::from_row)
        .collect_vec();

    dbg!(shit);
}
