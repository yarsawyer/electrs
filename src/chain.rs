pub use bitcoin::{
    blockdata::{opcodes, script, witness::Witness},
    consensus::deserialize,
    hashes,
    util::address,
    Block, BlockHash, BlockHeader, OutPoint, Script, Transaction, TxIn, TxOut, Txid,
};

use bitcoin::blockdata::constants::genesis_block;
pub use bitcoin::network::constants::Network as BNetwork;

pub type Value = u64;

#[derive(Debug, Copy, Clone, PartialEq, Hash, Serialize, Ord, PartialOrd, Eq)]
pub enum Network {
    Bellscoin,
    Testnet,
}

impl Network {
    pub fn magic(self) -> u32 {
        BNetwork::from(self).magic()
    }

    pub fn names() -> Vec<String> {
        vec!["mainnet".to_string(), "testnet".to_string()]
    }
}

pub fn genesis_hash(network: Network) -> BlockHash {
    tidecoin_genesis_hash(network.into())
}

pub fn tidecoin_genesis_hash(network: BNetwork) -> bitcoin::BlockHash {
    lazy_static! {
        static ref TIDECOIN_GENESIS: bitcoin::BlockHash =
            genesis_block(BNetwork::Bitcoin).block_hash();
        static ref TESTNET_GENESIS: bitcoin::BlockHash =
            genesis_block(BNetwork::Testnet).block_hash();
    }
    match network {
        BNetwork::Bitcoin => *TIDECOIN_GENESIS,
        BNetwork::Testnet => *TESTNET_GENESIS,
        _ => panic!("unsupported Bells network: {:?}", network),
    }
}

impl From<&str> for Network {
    fn from(network_name: &str) -> Self {
        match network_name {
            "mainnet" => Network::Bellscoin,
            "testnet" => Network::Testnet,
            _ => panic!("unsupported Tidecoin network: {:?}", network_name),
        }
    }
}

impl From<Network> for BNetwork {
    fn from(network: Network) -> Self {
        match network {
            Network::Bellscoin => BNetwork::Bitcoin,
            Network::Testnet => BNetwork::Testnet,
        }
    }
}

impl From<BNetwork> for Network {
    fn from(network: BNetwork) -> Self {
        match network {
            BNetwork::Bitcoin => Network::Bellscoin,
            BNetwork::Testnet => Network::Testnet,
            _ => panic!("unsupported Bells network: {:?}", network),
        }
    }
}
