use std::path::Path;

use electrs::{config::Config, new_index::DB};

fn main() -> anyhow::Result<()> {
    let config = Config::from_args();
    let db = DB::open(Path::new("db/mainnet/newindex/temp"), &config);

    Ok(())
}
