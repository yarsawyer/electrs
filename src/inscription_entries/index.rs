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

// Inscription DB
define_prefix! { PARTIAL_TXID_TO_TXIDS, A }
define_prefix! { INSCRIPTION_NUMBER, B }
define_prefix! { OUTPOINT_IS_INSCRIPTION, C }
define_prefix! { ADDRESS_TO_ORD_STATS, D }
define_prefix! { OWNER_LOCATION_TO_INSCRIPTION, E }
// define_prefix! { INSCRIPTION_ID_LOCATION_TO_OWNER, F }

// Token DB
define_prefix! { TOKEN_TO_DATA, A }
define_prefix! { ADDRESS_TOKEN_TO_AMOUNT, B }
define_prefix! { ADDRESS_TICK_LOCATION_TO_TRANSFER, C }

// Temp DB
define_prefix! { TEMP_TOKEN_ACTIONS, G }
