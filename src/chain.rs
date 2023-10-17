pub use tidecoin::{
    blockdata::{opcodes, script, witness::Witness},
    consensus::deserialize,
    hashes,
    util::address,
    Block, BlockHash, BlockHeader, OutPoint, Script, Transaction, TxIn, TxOut, Txid,
};


use tidecoin::blockdata::constants::genesis_block;
pub use tidecoin::network::constants::Network as BNetwork;

pub type Value = u64;

#[derive(Debug, Copy, Clone, PartialEq, Hash, Serialize, Ord, PartialOrd, Eq)]
pub enum Network {
    Tidecoin,
    Testnet,
}


impl Network {
    pub fn magic(self) -> u32 {
        BNetwork::from(self).magic()
    }

    pub fn names() -> Vec<String> {
        return vec![
            "mainnet".to_string(),
            "testnet".to_string(),
        ];
    }
}

pub fn genesis_hash(network: Network) -> BlockHash {
    return tidecoin_genesis_hash(network.into());

}

pub fn tidecoin_genesis_hash(network: BNetwork) -> tidecoin::BlockHash {
    lazy_static! {
        static ref TIDECOIN_GENESIS: tidecoin::BlockHash =
            genesis_block(BNetwork::Tidecoin).block_hash();
        static ref TESTNET_GENESIS: tidecoin::BlockHash =
            genesis_block(BNetwork::Testnet).block_hash();
    }
    match network {
        BNetwork::Tidecoin => *TIDECOIN_GENESIS,
        BNetwork::Testnet => *TESTNET_GENESIS,

    }
}

impl From<&str> for Network {
    fn from(network_name: &str) -> Self {
        match network_name {
            "mainnet" => Network::Tidecoin,
            "testnet" => Network::Testnet,
            _ => panic!("unsupported Tidecoin network: {:?}", network_name),
        }
    }
}

impl From<Network> for BNetwork {
    fn from(network: Network) -> Self {
        match network {
            Network::Tidecoin => BNetwork::Tidecoin,
            Network::Testnet => BNetwork::Testnet,
        }
    }
}

impl From<BNetwork> for Network {
    fn from(network: BNetwork) -> Self {
        match network {
            BNetwork::Tidecoin => Network::Tidecoin,
            BNetwork::Testnet => Network::Testnet,
        }
    }
}
