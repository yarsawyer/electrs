use super::DB;
use crate::inscription_entries::index::{
    ADDRESS_TICK_LOCATION_TO_TRANSFER, ADDRESS_TOKEN_TO_AMOUNT, TOKEN_TO_DATA,
};
use crate::new_index::DBRow;
use crate::util::bincode_util;
use bitcoin::hashes::Hash;
use bitcoin::{OutPoint, Txid};
use itertools::Itertools;
use postcard;
use serde::Deserialize;
use serde_with::serde_as;
use std::collections::{HashMap, HashSet};

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[serde(tag = "op")]
#[serde(rename_all = "lowercase")]
pub enum BRC {
    Mint {
        #[serde(flatten)]
        proto: MintProto,
    },
    Deploy {
        #[serde(flatten)]
        proto: DeployProto,
    },
    Transfer {
        #[serde(flatten)]
        proto: TransferProto,
    },
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
#[serde(tag = "p")]
#[serde_as]
pub enum MintProto {
    #[serde(rename = "bel-20")]
    Bel20 {
        tick: String,
        #[serde_as(as = "DisplayFromStr")]
        amt: u64,
    },
}
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
#[serde(tag = "p")]
#[serde_as]
pub enum DeployProto {
    #[serde(rename = "bel-20")]
    Bel20 {
        tick: String,
        #[serde_as(as = "DisplayFromStr")]
        max: u64,
        #[serde_as(as = "DisplayFromStr")]
        lim: u128,
        #[serde_as(as = "DisplayFromStr")]
        #[serde(default = "DeployProto::default_dec")]
        dec: u8,
        #[serde_as(as = "DisplayFromStr")]
        #[serde(default = "DeployProto::default_supply")]
        supply: u64,
    },
}
impl DeployProto {
    const DEFAULT_DEC: u8 = 18;
    const DEFAULT_SUPPLY: u64 = 0;
    const MAX_DEC: u8 = 18;
    fn default_dec() -> u8 {
        Self::DEFAULT_DEC
    }
    fn default_supply() -> u64 {
        Self::DEFAULT_SUPPLY
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
#[serde(tag = "p")]
#[serde_as]
pub enum TransferProto {
    #[serde(rename = "bel-20")]
    Bel20 {
        tick: String,
        #[serde_as(as = "DisplayFromStr")]
        amt: u64,
    },
}

#[derive(Default)]
pub struct TokenCache {
    // All tokens. Used to check if a transfer is valid. Used like cache, loaded from db before parsing.
    pub tokens: HashMap<TokenKey, TokenValue>,

    // All token accounts. Used to check if a transfer is valid. Used like cache, loaded from db before parsing.
    pub token_accounts: HashMap<TokenAccountKey, TokenAccountValue>,

    // All token actions that not validated yet but just parsed.
    pub token_actions: Vec<(u32, usize, TokenAction)>,

    // All transfer actions. Used to check if a transfer is valid. Used like cache.
    pub all_transfers: HashMap<OutPoint, TransferProto>,

    // All transfer actions that are valid. Used to write to the db.
    pub valid_transfers: HashMap<OutPoint, (String, TransferProto)>,
}
impl TokenCache {
    pub fn try_parse(content_type: &str, content: &[u8]) -> Option<BRC> {
        let content_type = content_type.trim().replace(' ', "").to_lowercase();
        match content_type.as_str() {
            "text/plain;charset=utf-8"
            | "text/plain"
            | "application/json"
            | "application/json;charset=utf-8" => {
                let data = String::from_utf8(content.to_vec()).ok()?;
                let brc = serde_json::from_str::<BRC>(&data.to_lowercase()).ok()?;

                match &brc {
                    BRC::Mint {
                        proto: MintProto::Bel20 { tick, .. },
                    } if tick.len() == 4 => Some(brc),
                    BRC::Deploy {
                        proto: DeployProto::Bel20 { tick, dec, .. },
                    } if tick.len() == 4 && *dec <= DeployProto::MAX_DEC => Some(brc),
                    BRC::Transfer {
                        proto: TransferProto::Bel20 { tick, .. },
                    } if tick.len() == 4 => Some(brc),
                    _ => None,
                }
            }
            _ => None,
        }
    }

    pub fn parse_token_action(
        &mut self,
        content_type: &str,
        content: &[u8],
        h: u32,
        idx: usize,
        owner: String,
        genesis: OutPoint,
        location: OutPoint,
    ) {
        match Self::try_parse(content_type, content) {
            Some(BRC::Deploy { proto }) => {
                self.token_actions
                    .push((h, idx, TokenAction::Deploy { genesis, proto }));
            }
            Some(BRC::Mint { proto }) => {
                self.token_actions
                    .push((h, idx, TokenAction::Mint { owner, proto }));
            }
            Some(BRC::Transfer { proto }) => {
                self.token_actions.push((
                    h,
                    idx,
                    TokenAction::Transfer {
                        location,
                        owner,
                        proto: proto.clone(),
                    },
                ));
                self.all_transfers.insert(location, proto);
            }
            _ => {}
        }
    }

    pub fn extend(&mut self, cache: TokenCache) {
        self.token_actions.extend(cache.token_actions);
        self.all_transfers.extend(cache.all_transfers);
    }

    pub fn try_transfered(&mut self, h: u32, idx: usize, location: OutPoint, recipient: String) {
        if !self.all_transfers.contains_key(&location)
            || !self.valid_transfers.contains_key(&location)
        {
            return;
        }
        self.token_actions.push((
            h,
            idx,
            TokenAction::Transfered {
                transfer_location: location,
                recipient,
            },
        ));
    }

    pub fn load_tokens_data(&mut self, token_db: &DB) {
        let mut tickers = HashSet::new();
        let mut users = HashSet::new();

        for (_, _, action) in &self.token_actions {
            match action {
                TokenAction::Deploy {
                    proto: DeployProto::Bel20 { tick, .. },
                    ..
                } => {
                    tickers.insert(tick);
                }
                TokenAction::Mint {
                    owner,
                    proto: MintProto::Bel20 { tick, .. },
                    ..
                } => {
                    tickers.insert(tick);
                    users.insert((owner, tick));
                }
                TokenAction::Transfer {
                    owner,
                    proto: TransferProto::Bel20 { tick, .. },
                    ..
                } => {
                    tickers.insert(tick);
                    users.insert((owner, tick));
                }
                _ => {}
            }
        }

        let keys = tickers.into_iter().collect_vec();
        let tokens = token_db
            .db
            .multi_get(keys.iter().map(|x| TokenKey::db_key(x)))
            .into_iter()
            .map(|x| x.unwrap())
            .enumerate()
            .filter_map(|(i, x)| {
                x.map(|x| {
                    (
                        TokenKey {
                            tick: keys[i].clone(),
                        },
                        TokenValue::from_db_value(&x),
                    )
                })
            })
            .collect();

        let keys = users.into_iter().collect_vec();
        let token_accounts = token_db
            .db
            .multi_get(keys.iter().map(|(o, t)| TokenAccountKey::db_key(o, t)))
            .into_iter()
            .map(|x| x.unwrap())
            .enumerate()
            .filter_map(|(i, x)| {
                x.map(|x| {
                    (
                        TokenAccountKey {
                            owner: keys[i].0.clone(),
                            tick: keys[i].1.clone(),
                        },
                        TokenAccountValue::from_db_value(&x),
                    )
                })
            })
            .collect();

        self.tokens = tokens;
        self.token_accounts = token_accounts;
    }

    pub fn process_token_actions(&mut self) {
        // We should sort token actions before processing them.
        self.token_actions
            .sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

        for (_, _, action) in self.token_actions.drain(..) {
            match action {
                TokenAction::Deploy { genesis, proto } => {
                    let tick = match &proto {
                        DeployProto::Bel20 { tick, .. } => tick.clone(),
                    };
                    self.tokens
                        .entry(TokenKey { tick })
                        .or_insert(TokenValue { genesis, proto });
                }
                TokenAction::Mint {
                    owner,
                    proto: MintProto::Bel20 { tick, amt },
                } => {
                    let key = TokenKey { tick };
                    let Some(token) = self.tokens.get_mut(&key) else {
                        continue;
                    };
                    let DeployProto::Bel20 {
                        max, lim, supply, ..
                    } = &mut token.proto;

                    if *lim != amt as u128 || *supply + amt > *max {
                        continue;
                    }
                    *supply += amt;

                    self.token_accounts
                        .entry(TokenAccountKey {
                            owner,
                            tick: key.tick,
                        })
                        .or_default()
                        .amount += amt;
                }
                TokenAction::Transfer {
                    owner,
                    location,
                    proto: TransferProto::Bel20 { tick, amt },
                } => {
                    let Some(data) = self.all_transfers.remove(&location) else {
                        continue;
                    };

                    let key = TokenKey { tick };
                    if !self.tokens.contains_key(&key) {
                        continue;
                    }

                    let key = TokenAccountKey {
                        owner,
                        tick: key.tick,
                    };
                    let Some(account) = self.token_accounts.get_mut(&key) else {
                        continue;
                    };

                    if amt > account.amount {
                        continue;
                    }
                    account.amount -= amt;

                    self.valid_transfers.insert(location, (key.owner, data));
                }
                TokenAction::Transfered {
                    transfer_location,
                    recipient,
                } => {
                    let Some((_, TransferProto::Bel20 { tick, amt })) =
                        self.valid_transfers.remove(&transfer_location)
                    else {
                        continue;
                    };
                    let key = TokenKey { tick };
                    if !self.tokens.contains_key(&key) {
                        return;
                    }

                    let key = TokenAccountKey {
                        owner: recipient,
                        tick: key.tick,
                    };
                    self.token_accounts.entry(key).or_default().amount += amt;
                }
            }
        }
    }

    pub fn write_token_data(&mut self, token_db: &DB) {
        let tokens = self
            .tokens
            .drain()
            .map(|(k, v)| DBRow {
                key: k.to_db_key(),
                value: v.to_db_value(),
            })
            .collect_vec();
        warn!("tokens len {}", tokens.len());
        token_db.write(tokens, super::db::DBFlush::Disable);

        let token_accounts = self
            .token_accounts
            .drain()
            .map(|(k, v)| DBRow {
                key: k.to_db_key(),
                value: v.to_db_value(),
            })
            .collect_vec();
        warn!("token_acc len {}", token_accounts.len());
        token_db.write(token_accounts, super::db::DBFlush::Disable);
    }

    pub fn write_valid_transfers(&mut self, token_db: &DB) {
        if !self.valid_transfers.is_empty() {
            let transfers = self
                .valid_transfers
                .drain()
                .map(|(location, (owner, proto))| {
                    let TransferProto::Bel20 { tick, .. } = &proto;
                    let key = TokenTransferKey {
                        location,
                        owner,
                        tick: tick.clone(),
                    }
                    .to_db_key();
                    let value = TokenTransferValue { proto }.to_db_value();
                    DBRow { key, value }
                })
                .collect_vec();

            token_db.write(transfers, super::db::DBFlush::Disable);
        }
    }
}

#[derive(Clone)]
pub enum TokenAction {
    Deploy {
        genesis: OutPoint,
        proto: DeployProto,
    },
    Mint {
        owner: String,
        proto: MintProto,
    },
    Transfer {
        location: OutPoint,
        owner: String,
        proto: TransferProto,
    },
    Transfered {
        transfer_location: OutPoint,
        recipient: String,
    },
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct TokenKey {
    pub tick: String,
}
impl TokenKey {
    pub fn db_key(tick: &str) -> Vec<u8> {
        bincode_util::serialize_big(&(TOKEN_TO_DATA, tick)).unwrap()
    }
    pub fn to_db_key(&self) -> Vec<u8> {
        bincode_util::serialize_big(&(TOKEN_TO_DATA, &self.tick)).unwrap()
    }
}
#[derive(Serialize, Deserialize)]
pub struct TokenValue {
    pub genesis: OutPoint,
    pub proto: DeployProto,
}
impl TokenValue {
    pub fn to_db_value(&self) -> Vec<u8> {
        serde_json::to_vec(&self).unwrap()
    }
    pub fn from_db_value(data: &[u8]) -> Self {
        serde_json::from_slice(data).unwrap()
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TokenAccountKey {
    pub owner: String,
    pub tick: String,
}
impl TokenAccountKey {
    pub fn iter_key(owner: &str) -> Vec<u8> {
        bincode_util::serialize_big(&(ADDRESS_TOKEN_TO_AMOUNT, owner)).unwrap()
    }

    pub fn parse_account_key(raw: Vec<u8>) -> Self {
        type ParseType = (String, String, String);
        let (_, owner, tick) = bincode_util::deserialize_big::<ParseType>(&raw).unwrap();

        Self { owner, tick }
    }
    pub fn to_db_key(&self) -> Vec<u8> {
        bincode_util::serialize_big(&(ADDRESS_TOKEN_TO_AMOUNT, &self.owner, &self.tick)).unwrap()
    }
    pub fn db_key(owner: &str, tick: &str) -> Vec<u8> {
        bincode_util::serialize_big(&(ADDRESS_TOKEN_TO_AMOUNT, owner, tick)).unwrap()
    }
}
#[derive(Serialize, Deserialize, Default)]
pub struct TokenAccountValue {
    pub amount: u64,
}
impl TokenAccountValue {
    pub fn from_db_value(data: &[u8]) -> Self {
        postcard::from_bytes(data).unwrap()
    }
    pub fn to_db_value(&self) -> Vec<u8> {
        postcard::to_allocvec(&self).unwrap()
    }
}

#[derive(PartialEq, Eq, Hash)]
pub struct TokenTransferKey {
    pub owner: String,
    pub tick: String,
    pub location: OutPoint,
}
impl TokenTransferKey {
    pub fn iter_key(owner: &str) -> Vec<u8> {
        bincode_util::serialize_big(&(ADDRESS_TICK_LOCATION_TO_TRANSFER, owner)).unwrap()
    }

    pub fn parse_transfer_key(raw: Vec<u8>) -> Self {
        type ParseType = (String, String, String, [u8; 32], u32);
        let (_, owner, tick, txid, vout) =
            bincode_util::deserialize_big::<ParseType>(&raw).unwrap();

        Self {
            owner,
            tick,
            location: OutPoint {
                txid: Txid::from_slice(txid.as_slice()).unwrap(),
                vout,
            },
        }
    }
    pub fn to_db_key(&self) -> Vec<u8> {
        bincode_util::serialize_big(&(
            ADDRESS_TICK_LOCATION_TO_TRANSFER,
            &self.owner,
            &self.tick,
            self.location.txid.into_inner(),
            self.location.vout,
        ))
        .unwrap()
    }
    pub fn db_key(owner: &str, tick: &str, location: OutPoint) -> Vec<u8> {
        bincode_util::serialize_big(&(
            ADDRESS_TICK_LOCATION_TO_TRANSFER,
            owner,
            tick,
            location.txid.into_inner(),
            location.vout,
        ))
        .unwrap()
    }
}

#[derive(Serialize, Deserialize)]
pub struct TokenTransferValue {
    pub proto: TransferProto,
}
impl TokenTransferValue {
    pub fn to_db_value(&self) -> Vec<u8> {
        serde_json::to_vec(&self).unwrap()
    }
    pub fn from_db_value(data: &[u8]) -> Self {
        serde_json::from_slice(data).unwrap()
    }
}

#[derive(Default)]
pub struct TokensData {
    pub tokens: HashMap<TokenKey, TokenValue>,
    pub token_accounts: HashMap<TokenAccountKey, TokenAccountValue>,
}

#[derive(Serialize, Deserialize)]
pub struct TokenBalance {
    pub tick: String,
    pub balance: u64,
    pub transferable_balance: u64,
}
