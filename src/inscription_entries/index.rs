use super::*;

use std::path::PathBuf;

#[derive(Serialize)]
pub(crate) struct Info {
    pub(crate) blocks_indexed: u64,
    pub(crate) branch_pages: usize,
    pub(crate) fragmented_bytes: usize,
    pub(crate) index_file_size: u64,
    pub(crate) index_path: PathBuf,
    pub(crate) leaf_pages: usize,
    pub(crate) metadata_bytes: usize,
    pub(crate) outputs_traversed: u64,
    pub(crate) page_size: usize,
    pub(crate) sat_ranges: u64,
    pub(crate) stored_bytes: usize,
    pub(crate) transactions: Vec<TransactionInfo>,
    pub(crate) tree_height: usize,
    pub(crate) utxos_indexed: usize,
}

#[derive(Serialize)]
pub(crate) struct TransactionInfo {
    pub(crate) starting_block_count: u64,
    pub(crate) starting_timestamp: u128,
}

macro_rules! define_prefix {
    ($name:ident, $short_name:ident) => {
        pub(crate) const $name: &str = stringify!($short_name);
    };
}

#[macro_export]
macro_rules! db_key {
    ($str:expr, $bytes:expr) => {{
        let str_as_bytes = $str.as_bytes();
        let mut result = Vec::with_capacity(str_as_bytes.len() + $bytes.len());
        result.extend_from_slice(str_as_bytes);
        result.extend_from_slice($bytes);
        result
    }};
}

define_prefix! { INSCRIPTION_ID_TO_META, IITM }
define_prefix! { PARTIAL_TXID_TO_TXIDS, PTTT }
define_prefix! { INSCRIPTION_NUMBER, IN }
define_prefix! { OUTPOINT_IS_INSCRIPTION, OI }
define_prefix! { ADDRESS_TO_ORD_STATS, ATOS }
