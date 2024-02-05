use std::convert::TryInto;

use anyhow::Ok;
use bitcoin::{blockdata::block, hashes::Hash, OutPoint, Txid};
use itertools::Itertools;

use crate::{
    inscription_entries::index::PARTIAL_TXID_TO_TXIDS,
    media::Media,
    new_index::DBRow,
    util::{bincode_util, errors::AsAnyhow, Bytes},
};

use super::{
    index::{
        ADDRESS_TO_ORD_STATS, INSCRIPTION_NUMBER, OUTPOINT_IS_INSCRIPTION,
        OWNER_LOCATION_TO_INSCRIPTION,
    },
    InscriptionId,
};

use {
    bitcoin::{
        blockdata::{opcodes, script},
        Script, Transaction,
    },
    std::str,
};

const PROTOCOL_ID: &[u8] = b"ord";

#[derive(Debug, PartialEq, Clone)]
pub struct Inscription {
    body: Option<Vec<u8>>,
    content_type: Option<Vec<u8>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct OrdHistoryKey {
    pub code: u8,
    pub owner: String,
    pub outpoint: OutPoint,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct OrdHistoryValue {
    pub value: u64,
    pub inscription_number: u64,
    pub inscription_id: InscriptionId,
}

impl OrdHistoryValue {
    pub fn get_raw(&self) -> Vec<u8> {
        bincode_util::serialize_big(self).unwrap()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OrdHistoryRow {
    pub key: OrdHistoryKey,
    pub value: OrdHistoryValue,
}

impl OrdHistoryRow {
    pub const CODE: u8 = OWNER_LOCATION_TO_INSCRIPTION.as_bytes()[0];

    pub fn new(owner: String, outpoint: OutPoint, value: OrdHistoryValue) -> Self {
        let key = OrdHistoryKey {
            code: OrdHistoryRow::CODE,
            owner,
            outpoint,
        };
        OrdHistoryRow { key, value }
    }

    pub fn filter(address: String) -> Bytes {
        bincode_util::serialize_big(&(OrdHistoryRow::CODE, address.as_bytes())).unwrap()
    }

    pub fn prefix_end(address: String) -> Bytes {
        bincode_util::serialize_big(&(OrdHistoryRow::CODE, address.as_bytes(), std::u32::MAX))
            .unwrap()
    }

    pub fn prefix_height(address: String, height: u32) -> Bytes {
        bincode_util::serialize_big(&(OrdHistoryRow::CODE, address.as_bytes(), height)).unwrap()
    }

    pub fn get_key(&self) -> Vec<u8> {
        bincode_util::serialize_big(&self.key).unwrap()
    }

    pub fn create_db_key(address: String, outpoint: &OutPoint) -> Vec<u8> {
        let key = OrdHistoryKey {
            owner: address,
            code: OrdHistoryRow::CODE,
            outpoint: *outpoint,
        };

        bincode_util::serialize_big(&key).unwrap()
    }

    pub fn into_row(self) -> DBRow {
        DBRow {
            key: bincode_util::serialize_big(&self.key).unwrap(),
            value: self.value.get_raw(),
        }
    }

    pub fn from_row(row: DBRow) -> Self {
        let key =
            bincode_util::deserialize_big(&row.key).expect("failed to deserialize OrdHistoryKey");
        let value = bincode_util::deserialize_big(&row.value)
            .expect("failed to deserialize OrdHistoryValue");
        OrdHistoryRow { key, value }
    }

    pub fn value_from_raw(value: &Vec<u8>) -> OrdHistoryValue {
        bincode_util::deserialize_big(value).expect("Failed to deserialize OrdHistoryValue")
    }

    pub fn get_outpoint(&self) -> OutPoint {
        self.key.outpoint
    }

    pub fn get_value(&self) -> u64 {
        self.value.value
    }

    pub fn get_address(&self) -> String {
        self.key.owner.clone()
    }

    pub fn get_inscription_number(&self) -> u64 {
        self.value.inscription_number
    }

    pub fn get_temp_db_key(address: String, outpoint: &OutPoint, block_height: u32) -> Vec<u8> {
        let key = OrdHistoryKey {
            owner: address,
            code: OrdHistoryRow::CODE,
            outpoint: *outpoint,
        };

        bincode_util::serialize_big(&(key, block_height)).unwrap()
    }

    pub fn from_temp_db_row(row: DBRow) -> anyhow::Result<(Self, u32)> {
        let (key, block_height): (OrdHistoryKey, u32) =
            bincode_util::deserialize_big(&row.key).anyhow_as("Cannot deserialize key")?;

        let value: OrdHistoryValue =
            bincode_util::deserialize_big(&row.value).anyhow_as("Cannot deserialize value")?;

        Ok((OrdHistoryRow { key, value }, block_height))
    }
}

// #[derive(Debug, Deserialize, Serialize)]
// pub struct TempData {
//     pub location: OutPoint,
//     pub block_height: u32,
//     pub owner: String,
// }
// impl TempData {
//     pub fn new(block_height: u32, location: OutPoint, owner: String) -> Self {
//         Self {
//             block_height,
//             location,
//             owner,
//         }
//     }

//     pub fn from_row(value: DBRow) -> anyhow::Result<Self> {
//         let (_, location): (u8, OutPoint) =
//             bincode_util::deserialize_big(&value.key).anyhow_as("Cannot deserialize key")?;

//         let (block_height, owner): (u32, String) =
//             bincode_util::deserialize_big(&value.value).anyhow_as("Cannot deserialize value")?;

//         Ok(Self {
//             location,
//             block_height,
//             owner,
//         })
//     }

//     pub fn to_db_row(&self) -> anyhow::Result<DBRow> {
//         Ok(DBRow {
//             key: Self::get_db_key(self.location)?,
//             value: bincode_util::serialize_big(&(self.block_height, &self.owner))
//                 .anyhow_as("Cannot serialize TempData")?,
//         })
//     }

//     pub fn get_db_key(location: OutPoint) -> anyhow::Result<Vec<u8>> {
//         bincode_util::serialize_big(&(INSCRIPTION_ID_LOCATION_TO_OWNER, &location)).anyhow()
//     }

//     pub fn get_temp_db_key(block_height: u32, location: OutPoint) -> Vec<u8> {
//         bincode_util::serialize_big(&(INSCRIPTION_ID_LOCATION_TO_OWNER, block_height, location))
//             .unwrap()
//     }

//     pub fn from_temp_db(row: DBRow) -> anyhow::Result<(Self, u32)> {
//         let (_, temp_block_height, location): (u8, u32, OutPoint) =
//             bincode_util::deserialize_big(&row.key).anyhow_as("Cannot deserialize key")?;

//         let (block_height, owner): (u32, String) =
//             bincode_util::deserialize_big(&row.value).anyhow_as("Cannot deserialize value")?;

//         Ok((
//             Self {
//                 location,
//                 block_height,
//                 owner,
//             },
//             temp_block_height,
//         ))
//     }

//     pub fn to_temp_db_row(&self, temp_block_height: u32) -> anyhow::Result<DBRow> {
//         Ok(DBRow {
//             key: Self::get_temp_db_key(temp_block_height, self.location),
//             value: bincode_util::serialize_big(&(self.block_height, &self.owner))
//                 .anyhow_as("Cannot serialize TempData")?,
//         })
//     }
// }

#[derive(Deserialize, Serialize, Debug, Default)]
pub struct UserOrdStats {
    pub amount: u64,
    pub count: u64,
}

impl UserOrdStats {
    pub fn new(amount: u64, count: u64) -> Self {
        Self { amount, count }
    }

    pub fn from_raw(value: &Vec<u8>) -> anyhow::Result<Self> {
        bincode_util::deserialize_big(value).anyhow_as("Cannot deserialize UserOrdStats")
    }

    pub fn owner_from_key(key: Vec<u8>) -> anyhow::Result<String> {
        let (_, owner): ([u8; ADDRESS_TO_ORD_STATS.len()], String) =
            bincode_util::deserialize_big(&key).anyhow_as("Cannot deserialize key")?;
        Ok(owner)
    }

    pub fn to_db_row(&self, owner: &str) -> anyhow::Result<DBRow> {
        Ok(DBRow {
            key: Self::get_db_key(owner)?,
            value: bincode_util::serialize_big(self).anyhow_as("Cannot serialize UserOrdStats")?,
        })
    }

    pub fn get_db_key(owner: &str) -> anyhow::Result<Vec<u8>> {
        bincode_util::serialize_big(&(ADDRESS_TO_ORD_STATS, owner)).anyhow()
    }

    pub fn get_temp_db_key(block_height: u32, owner: &str) -> Vec<u8> {
        bincode_util::serialize_big(&(ADDRESS_TO_ORD_STATS, block_height, owner)).unwrap()
    }

    pub fn get_temp_iter_key(block_height: u32) -> Vec<u8> {
        bincode_util::serialize_big(&(ADDRESS_TO_ORD_STATS, block_height)).unwrap()
    }

    pub fn to_temp_db_row(&self, block_height: u32, owner: &str) -> anyhow::Result<DBRow> {
        Ok(DBRow {
            key: Self::get_temp_db_key(block_height, owner),
            value: bincode_util::serialize_big(self).anyhow_as("Cannot serialize UserOrdStats")?,
        })
    }

    pub fn from_temp_db(row: DBRow) -> anyhow::Result<(Self, String)> {
        let (_, _, owner): ([u8; ADDRESS_TO_ORD_STATS.len()], u32, String) =
            bincode_util::deserialize_big(&row.key).anyhow_as("Cannot deserialize key")?;

        let stats: UserOrdStats =
            bincode_util::deserialize_big(&row.value).anyhow_as("Cannot deserialize value")?;

        Ok((stats, owner))
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct InscriptionExtraDataValue {
    pub owner: String,
    pub content_type: String,
    pub genesis: OutPoint,
    pub block_height: u32,
    pub content_length: usize,
    pub number: u64,
    pub offset: u64,
}

impl InscriptionExtraDataValue {
    pub fn from_raw(value: Vec<u8>) -> anyhow::Result<Self> {
        bincode_util::deserialize_big(&value).anyhow_as("Cannot deserialize value")
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct InscriptionExtraData {
    pub location: OutPoint,
    #[serde(flatten)]
    pub value: InscriptionExtraDataValue,
}

impl InscriptionExtraData {
    const ERROR_MESSAGE: &'static str = "Cannot deserialize / serialize InscriptionExtraData";

    pub fn new(
        location: OutPoint,
        genesis: OutPoint,
        owner: String,
        block_height: u32,
        content_type: String,
        content_length: usize,
        number: u64,
        offset: u64,
    ) -> Self {
        Self {
            location,
            value: InscriptionExtraDataValue {
                owner,
                genesis,
                block_height,
                content_length,
                content_type,
                number,
                offset,
            },
        }
    }

    pub fn from_raw(value: DBRow) -> anyhow::Result<Self> {
        let txid = Txid::from_slice(
            &value.key[OUTPOINT_IS_INSCRIPTION.len()..OUTPOINT_IS_INSCRIPTION.len() + 32],
        )
        .anyhow()?;
        let vout: u32 = u32::from_be_bytes(
            value.key[OUTPOINT_IS_INSCRIPTION.len() + 32..]
                .try_into()
                .anyhow()?,
        );

        let location = OutPoint::new(txid, vout);

        let value: InscriptionExtraDataValue =
            bincode_util::deserialize_big(&value.value).anyhow_as(Self::ERROR_MESSAGE)?;

        Ok(Self { location, value })
    }

    pub fn to_db_row(&self) -> anyhow::Result<DBRow> {
        Ok(DBRow {
            key: Self::get_db_key(self.location),
            value: bincode_util::serialize_big(&self.value).anyhow_as(Self::ERROR_MESSAGE)?,
        })
    }

    pub fn get_temp_db_key(block_height: u32, location: &OutPoint) -> Vec<u8> {
        bincode_util::serialize_big(&(
            OUTPOINT_IS_INSCRIPTION,
            block_height,
            location.txid.into_inner(),
            location.vout,
        ))
        .unwrap()
    }

    pub fn get_temp_db_iter_key(block_height: u32) -> Vec<u8> {
        bincode_util::serialize_big(&(OUTPOINT_IS_INSCRIPTION, block_height)).unwrap()
    }

    pub fn from_temp_db(row: DBRow) -> anyhow::Result<(Self, u32)> {
        let (_, block_height, txid_raw, vout): (
            [u8; OUTPOINT_IS_INSCRIPTION.len()],
            u32,
            [u8; 32],
            u32,
        ) = bincode_util::deserialize_big(&row.key).anyhow_as(Self::ERROR_MESSAGE)?;

        let location = OutPoint {
            txid: Txid::from_slice(&txid_raw).anyhow_as(Self::ERROR_MESSAGE)?,
            vout,
        };

        let extra_data: InscriptionExtraDataValue =
            bincode_util::deserialize_big(&row.value).anyhow_as(Self::ERROR_MESSAGE)?;

        Ok((
            Self {
                location,
                value: extra_data,
            },
            block_height,
        ))
    }

    pub fn to_temp_db_row(&self, block_height: u32, location: &OutPoint) -> anyhow::Result<DBRow> {
        Ok(DBRow {
            key: Self::get_temp_db_key(block_height, location),
            value: bincode_util::serialize_big(&self.value).anyhow_as(Self::ERROR_MESSAGE)?,
        })
    }

    pub fn get_db_key(outpoint: OutPoint) -> Vec<u8> {
        [
            OUTPOINT_IS_INSCRIPTION.as_bytes(),
            &outpoint.txid.into_inner(),
            &outpoint.vout.to_be_bytes(),
        ]
        .concat()
    }
}

pub struct PartialTxs {
    pub txs: Vec<Txid>,
    pub last_txid: Txid,
    pub block_height: u32,
}

impl PartialTxs {
    pub fn from_db(value: DBRow) -> anyhow::Result<Self> {
        let (_, txid): (String, [u8; 32]) = bincode_util::deserialize_big(&value.key)
            .anyhow_as("Failed to decode partial txs key")?;
        let txid = Txid::from_slice(&txid)?;

        let txs: Vec<Txid> = value
            .value
            .chunks(32)
            .map(|x| Txid::from_slice(x))
            .try_collect()
            .anyhow_as("Failed to decode transactions")?;

        Ok(Self {
            txs,
            last_txid: txid,
            block_height: 0,
        })
    }

    pub fn to_db(&self) -> anyhow::Result<DBRow> {
        Ok(DBRow {
            key: self.get_db_key(),
            value: self
                .txs
                .iter()
                .map(|x| x.into_inner())
                .collect_vec()
                .concat(),
        })
    }

    pub fn get_db_key(&self) -> Vec<u8> {
        bincode_util::serialize_big(&(PARTIAL_TXID_TO_TXIDS, self.last_txid.into_inner())).unwrap()
    }

    pub fn get_temp_iter_key(block_height: u32) -> Vec<u8> {
        bincode_util::serialize_big(&(PARTIAL_TXID_TO_TXIDS, block_height)).unwrap()
    }

    pub fn get_temp_db_key(block_height: u32, txid: &Txid) -> Vec<u8> {
        bincode_util::serialize_big(&(PARTIAL_TXID_TO_TXIDS, block_height, txid.into_inner()))
            .unwrap()
    }

    pub fn to_temp_db_row(&self) -> anyhow::Result<DBRow> {
        Ok(DBRow {
            key: Self::get_temp_db_key(self.block_height, &self.last_txid),
            value: self
                .txs
                .iter()
                .map(|x| x.into_inner())
                .collect_vec()
                .concat(),
        })
    }

    pub fn from_temp_db(row: DBRow) -> anyhow::Result<Self> {
        let (_, block_height, txid): (String, u32, [u8; 32]) =
            bincode_util::deserialize_big(&row.key)
                .anyhow_as("Failed to decode partial txs key")?;

        let last_txid = Txid::from_slice(&txid).anyhow_as("Failed to decode last_txid")?;

        let txs: Vec<Txid> = row
            .value
            .chunks(32)
            .map(|x| Txid::from_slice(x))
            .try_collect()
            .anyhow_as("Failed to decode transactions")?;

        Ok(Self {
            txs,
            last_txid,
            block_height,
        })
    }
}

#[derive(Debug, PartialEq)]
pub enum ParsedInscription {
    None,
    Partial,
    Complete(Inscription),
}

impl Inscription {
    #[cfg(test)]
    pub fn new(content_type: Option<Vec<u8>>, body: Option<Vec<u8>>) -> Self {
        Self { content_type, body }
    }

    pub fn from_transactions(txs: &[&Transaction]) -> ParsedInscription {
        let mut sig_scripts = Vec::with_capacity(txs.len());
        for i in 0..txs.len() {
            if txs[i].input.is_empty() {
                return ParsedInscription::None;
            }
            sig_scripts.push(txs[i].input[0].script_sig.clone());
        }
        InscriptionParser::parse(sig_scripts)
    }

    fn append_reveal_script_to_builder(&self, mut builder: script::Builder) -> script::Builder {
        builder = builder
            .push_opcode(opcodes::OP_FALSE)
            .push_opcode(opcodes::all::OP_IF)
            .push_slice(PROTOCOL_ID);

        if let Some(content_type) = &self.content_type {
            builder = builder.push_slice(&[1]).push_slice(content_type);
        }

        if let Some(body) = &self.body {
            builder = builder.push_slice(&[]);
            for chunk in body.chunks(520) {
                builder = builder.push_slice(chunk);
            }
        }

        builder.push_opcode(opcodes::all::OP_ENDIF)
    }

    pub fn append_reveal_script(&self, builder: script::Builder) -> Script {
        self.append_reveal_script_to_builder(builder).into_script()
    }

    pub fn media(&self) -> Media {
        if self.body.is_none() {
            return Media::Unknown;
        }

        let Some(content_type) = self.content_type() else {
            return Media::Unknown;
        };

        content_type.parse().unwrap_or(Media::Unknown)
    }

    pub fn body(&self) -> Option<&[u8]> {
        Some(self.body.as_ref()?)
    }

    pub fn into_body(self) -> Option<Vec<u8>> {
        self.body
    }

    pub fn content_length(&self) -> Option<usize> {
        Some(self.body()?.len())
    }

    pub fn content_type(&self) -> Option<&str> {
        str::from_utf8(self.content_type.as_ref()?).ok()
    }

    #[cfg(test)]
    pub fn to_witness(&self) -> bitcoin::Witness {
        let builder = script::Builder::new();

        let script = self.append_reveal_script(builder);

        let mut witness = bitcoin::Witness::new();

        witness.push(script);
        witness.push([]);

        witness
    }
}

struct InscriptionParser {}

impl InscriptionParser {
    fn parse(sig_scripts: Vec<Script>) -> ParsedInscription {
        let sig_script = &sig_scripts[0];

        let mut push_datas_vec = match Self::decode_push_datas(sig_script) {
            Some(push_datas) => push_datas,
            None => return ParsedInscription::None,
        };

        let mut push_datas = push_datas_vec.as_slice();

        // read protocol

        if push_datas.len() < 3 {
            return ParsedInscription::None;
        }

        let protocol = &push_datas[0];

        if protocol != PROTOCOL_ID {
            return ParsedInscription::None;
        }

        // read npieces

        let mut npieces = match Self::push_data_to_number(&push_datas[1]) {
            Some(n) => n,
            None => return ParsedInscription::None,
        };

        if npieces == 0 {
            return ParsedInscription::None;
        }

        // read content type

        let content_type = push_datas[2].clone();

        push_datas = &push_datas[3..];

        // read body

        let mut body = vec![];

        let mut sig_scripts = sig_scripts.as_slice();

        // loop over transactions
        loop {
            // loop over chunks
            loop {
                if npieces == 0 {
                    let inscription = Inscription {
                        content_type: Some(content_type),
                        body: Some(body),
                    };

                    return ParsedInscription::Complete(inscription);
                }

                if push_datas.len() < 2 {
                    break;
                }

                let next = match Self::push_data_to_number(&push_datas[0]) {
                    Some(n) => n,
                    None => break,
                };

                if next != npieces - 1 {
                    break;
                }

                body.append(&mut push_datas[1].clone());

                push_datas = &push_datas[2..];
                npieces -= 1;
            }

            if sig_scripts.len() <= 1 {
                return ParsedInscription::Partial;
            }

            sig_scripts = &sig_scripts[1..];

            push_datas_vec = match Self::decode_push_datas(&sig_scripts[0]) {
                Some(push_datas) => push_datas,
                None => return ParsedInscription::None,
            };

            if push_datas_vec.len() < 2 {
                return ParsedInscription::None;
            }

            let next = match Self::push_data_to_number(&push_datas_vec[0]) {
                Some(n) => n,
                None => return ParsedInscription::None,
            };

            if next != npieces - 1 {
                return ParsedInscription::None;
            }

            push_datas = push_datas_vec.as_slice();
        }
    }

    fn decode_push_datas(script: &Script) -> Option<Vec<Vec<u8>>> {
        let mut bytes = script.as_bytes();
        let mut push_datas = vec![];

        while !bytes.is_empty() {
            // op_0
            if bytes[0] == 0 {
                push_datas.push(vec![]);
                bytes = &bytes[1..];
                continue;
            }

            // op_1 - op_16
            if bytes[0] >= 81 && bytes[0] <= 96 {
                push_datas.push(vec![bytes[0] - 80]);
                bytes = &bytes[1..];
                continue;
            }

            // op_push 1-75
            if bytes[0] >= 1 && bytes[0] <= 75 {
                let len = bytes[0] as usize;
                if bytes.len() < 1 + len {
                    return None;
                }
                push_datas.push(bytes[1..1 + len].to_vec());
                bytes = &bytes[1 + len..];
                continue;
            }

            // op_pushdata1
            if bytes[0] == 76 {
                if bytes.len() < 2 {
                    return None;
                }
                let len = bytes[1] as usize;
                if bytes.len() < 2 + len {
                    return None;
                }
                push_datas.push(bytes[2..2 + len].to_vec());
                bytes = &bytes[2 + len..];
                continue;
            }

            // op_pushdata2
            if bytes[0] == 77 {
                if bytes.len() < 3 {
                    return None;
                }
                let len = ((bytes[1] as usize) << 8) + ((bytes[0] as usize) << 0);
                if bytes.len() < 3 + len {
                    return None;
                }
                push_datas.push(bytes[3..3 + len].to_vec());
                bytes = &bytes[3 + len..];
                continue;
            }

            // op_pushdata4
            if bytes[0] == 78 {
                if bytes.len() < 5 {
                    return None;
                }
                let len = ((bytes[3] as usize) << 24)
                    + ((bytes[2] as usize) << 16)
                    + ((bytes[1] as usize) << 8)
                    + ((bytes[0] as usize) << 0);
                if bytes.len() < 5 + len {
                    return None;
                }
                push_datas.push(bytes[5..5 + len].to_vec());
                bytes = &bytes[5 + len..];
                continue;
            }

            return None;
        }

        Some(push_datas)
    }

    fn push_data_to_number(data: &[u8]) -> Option<u64> {
        if data.len() == 0 {
            return Some(0);
        }

        if data.len() > 8 {
            return None;
        }

        let mut n: u64 = 0;
        let mut m: u64 = 0;

        for i in 0..data.len() {
            n += (data[i] as u64) << m;
            m += 8;
        }

        return Some(n);
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LastInscriptionNumber {
    pub number: u64,
}

impl LastInscriptionNumber {
    pub fn new(number: u64) -> Self {
        Self { number }
    }

    pub fn from_db(value: DBRow) -> anyhow::Result<Self> {
        Self::from_raw(value.value)
    }

    pub fn from_raw(value: Vec<u8>) -> anyhow::Result<Self> {
        let number: u64 = bincode_util::deserialize_big(&value)
            .anyhow_as("Cannot deserialize LastInscriptionNumber")?;

        Ok(Self { number })
    }

    pub fn to_db(&self) -> anyhow::Result<DBRow> {
        Ok(DBRow {
            key: bincode_util::serialize_big(INSCRIPTION_NUMBER)
                .anyhow_as("Cannot serialize LastInscriptionNumber")?,
            value: bincode_util::serialize_big(&self.number)
                .anyhow_as("Cannot serialize LastInscriptionNumber")?,
        })
    }

    pub fn get_db_key() -> Vec<u8> {
        bincode_util::serialize_big(INSCRIPTION_NUMBER)
            .expect("Cannot serialize LastInscriptionNumber")
    }

    pub fn get_temp_db_key(block_height: u32) -> Vec<u8> {
        bincode_util::serialize_big(&(INSCRIPTION_NUMBER, block_height)).unwrap()
    }

    pub fn to_temp_db_row(&self, block_height: u32) -> anyhow::Result<DBRow> {
        Ok(DBRow {
            key: Self::get_temp_db_key(block_height),
            value: self.to_db()?.value,
        })
    }
}
