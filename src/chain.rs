#[cfg(not(feature = "liquid"))] // use regular Tidecoin data structures
pub use tidecoin::{
    blockdata::{opcodes, script, witness::Witness},
    consensus::deserialize,
    hashes,
    util::address,
    Block, BlockHash, BlockHeader, OutPoint, Script, Transaction, TxIn, TxOut, Txid,
};

#[cfg(feature = "liquid")]
pub use {
    crate::elements::asset,
    elements::{
        address, confidential, encode::deserialize, hashes, opcodes, script, Address, AssetId,
        Block, BlockHash, BlockHeader, OutPoint, Script, Transaction, TxIn, TxInWitness as Witness,
        TxOut, Txid,
    },
};

use tidecoin::blockdata::constants::genesis_block;
pub use tidecoin::network::constants::Network as BNetwork;

#[cfg(not(feature = "liquid"))]
pub type Value = u64;
#[cfg(feature = "liquid")]
pub use confidential::Value;

#[derive(Debug, Copy, Clone, PartialEq, Hash, Serialize, Ord, PartialOrd, Eq)]
pub enum Network {
    #[cfg(not(feature = "liquid"))]
    Tidecoin,
    #[cfg(not(feature = "liquid"))]
    Testnet,
    #[cfg(feature = "liquid")]
    Liquid,
    #[cfg(feature = "liquid")]
    LiquidTestnet,
    #[cfg(feature = "liquid")]
    LiquidRegtest,
}

#[cfg(feature = "liquid")]
pub const LIQUID_TESTNET_PARAMS: address::AddressParams = address::AddressParams {
    p2pkh_prefix: 36,
    p2sh_prefix: 19,
    blinded_prefix: 23,
    bech_hrp: "tex",
    blech_hrp: "tlq",
};

impl Network {
    #[cfg(not(feature = "liquid"))]
    pub fn magic(self) -> u32 {
        BNetwork::from(self).magic()
    }

    #[cfg(feature = "liquid")]
    pub fn magic(self) -> u32 {
        match self {
            Network::Liquid | Network::LiquidRegtest => 0xDAB5_BFFA,
            Network::LiquidTestnet => 0x62DD_0E41,
        }
    }


    #[cfg(feature = "liquid")]
    pub fn address_params(self) -> &'static address::AddressParams {
        // Liquid regtest uses elements's address params
        match self {
            Network::Liquid => &address::AddressParams::LIQUID,
            Network::LiquidRegtest => &address::AddressParams::ELEMENTS,
            Network::LiquidTestnet => &LIQUID_TESTNET_PARAMS,
        }
    }

    #[cfg(feature = "liquid")]
    pub fn native_asset(self) -> &'static AssetId {
        match self {
            Network::Liquid => &asset::NATIVE_ASSET_ID,
            Network::LiquidTestnet => &asset::NATIVE_ASSET_ID_TESTNET,
            Network::LiquidRegtest => &asset::NATIVE_ASSET_ID_REGTEST,
        }
    }

    #[cfg(feature = "liquid")]
    pub fn pegged_asset(self) -> Option<&'static AssetId> {
        match self {
            Network::Liquid => Some(&*asset::NATIVE_ASSET_ID),
            Network::LiquidTestnet | Network::LiquidRegtest => None,
        }
    }

    pub fn names() -> Vec<String> {
        #[cfg(not(feature = "liquid"))]
        return vec![
            "mainnet".to_string(),
            "testnet".to_string(),
        ];

        #[cfg(feature = "liquid")]
        return vec![
            "liquid".to_string(),
            "liquidtestnet".to_string(),
            "liquidregtest".to_string(),
        ];
    }
}

pub fn genesis_hash(network: Network) -> BlockHash {
    #[cfg(not(feature = "liquid"))]
    return bitcoin_genesis_hash(network.into());
    #[cfg(feature = "liquid")]
    return liquid_genesis_hash(network);
}

pub fn bitcoin_genesis_hash(network: BNetwork) -> tidecoin::BlockHash {
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

#[cfg(feature = "liquid")]
pub fn liquid_genesis_hash(network: Network) -> elements::BlockHash {
    lazy_static! {
        static ref LIQUID_GENESIS: BlockHash =
            "1466275836220db2944ca059a3a10ef6fd2ea684b0688d2c379296888a206003"
                .parse()
                .unwrap();
    }

    match network {
        Network::Liquid => *LIQUID_GENESIS,
        // The genesis block for liquid regtest chains varies based on the chain configuration.
        // This instead uses an all zeroed-out hash, which doesn't matter in practice because its
        // only used for Electrum server discovery, which isn't active on regtest.
        _ => Default::default(),
    }
}

impl From<&str> for Network {
    fn from(network_name: &str) -> Self {
        match network_name {
            #[cfg(not(feature = "liquid"))]
            "mainnet" => Network::Tidecoin,
            #[cfg(not(feature = "liquid"))]
            "testnet" => Network::Testnet,

            #[cfg(feature = "liquid")]
            "liquid" => Network::Liquid,
            #[cfg(feature = "liquid")]
            "liquidtestnet" => Network::LiquidTestnet,
            #[cfg(feature = "liquid")]
            "liquidregtest" => Network::LiquidRegtest,

            _ => panic!("unsupported Tidecoin network: {:?}", network_name),
        }
    }
}

#[cfg(not(feature = "liquid"))]
impl From<Network> for BNetwork {
    fn from(network: Network) -> Self {
        match network {
            Network::Tidecoin => BNetwork::Tidecoin,
            Network::Testnet => BNetwork::Testnet,
        }
    }
}

#[cfg(not(feature = "liquid"))]
impl From<BNetwork> for Network {
    fn from(network: BNetwork) -> Self {
        match network {
            BNetwork::Tidecoin => Network::Tidecoin,
            BNetwork::Testnet => Network::Testnet,
        }
    }
}
