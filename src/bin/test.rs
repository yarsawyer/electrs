use std::path::Path;

use electrs::util::bincode_util;
use electrs::{
    config::Config,
    new_index::{schema::OrdHistoryRow, DB},
};
use itertools::Itertools;

fn main() {
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
