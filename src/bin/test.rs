use std::path::Path;

use electrs::{config::Config, new_index::DB};

fn main() -> anyhow::Result<()> {
    let config = Config::from_args();
    let db = DB::open(Path::new("db/mainnet/newindex/token"), &config);
    let a = "A";
    let b = "B";
    let c = "C";
    let a = db.iter_scan(format!("{a}").as_bytes()).count();
    let b = db.iter_scan(format!("{b}").as_bytes()).count();
    let c = db.iter_scan(format!("{c}").as_bytes()).count();

    println!("count: {}", a);
    println!("count: {}", b);
    println!("count: {}", c);

    Ok(())
}
