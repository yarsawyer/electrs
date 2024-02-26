use std::convert::TryInto;

use anyhow::Ok;
use bitcoin::{
    hashes::{hex::FromHex, Hash},
    BlockHash, OutPoint, Txid,
};
use itertools::Itertools;

use crate::{
    inscription_entries::index::PARTIAL_TXID_TO_TXIDS,
    media::Media,
    new_index::{DBRow, Store},
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
    pub owner: String,
    pub location: Location,
}

impl OrdHistoryKey {
    pub fn from_raw(value: Vec<u8>) -> anyhow::Result<Self> {
        let (_, owner, txid, vout, offset): (u8, String, [u8; 32], u32, u64) =
            bincode_util::deserialize_big(&value).expect("failed to deserialize OrdHistoryKey");

        Ok(Self {
            owner,
            location: Location::new(
                OutPoint {
                    txid: Txid::from_slice(&txid)
                        .anyhow_as("Cannot deserialize OrdHistoryKey Txid")?,
                    vout,
                },
                offset,
            ),
        })
    }

    pub fn to_raw(&self) -> anyhow::Result<Vec<u8>> {
        bincode_util::serialize_big(&(
            OWNER_LOCATION_TO_INSCRIPTION.as_bytes()[0],
            &self.owner,
            self.location.outpoint.txid.into_inner(),
            self.location.outpoint.vout,
            self.location.offset,
        ))
        .anyhow_as("Failed to serialize OrdHistoryKey")
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OrdHistoryValue {
    pub inscription_number: u64,
    pub inscription_id: InscriptionId,
}

impl OrdHistoryValue {
    pub fn get_raw(&self) -> Vec<u8> {
        bincode_util::serialize_big(self).unwrap()
    }

    pub fn from_raw(value: &Vec<u8>) -> anyhow::Result<Self> {
        bincode_util::deserialize_big(value).anyhow_as("Failed to deserialize OrdHistoryValue")
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OrdHistoryRow {
    pub key: OrdHistoryKey,
    pub value: OrdHistoryValue,
}

impl OrdHistoryRow {
    pub const CODE: u8 = OWNER_LOCATION_TO_INSCRIPTION.as_bytes()[0];

    pub fn new(owner: String, location: Location, value: OrdHistoryValue) -> Self {
        let key = OrdHistoryKey { owner, location };
        OrdHistoryRow { key, value }
    }

    pub fn filter(address: String) -> Bytes {
        bincode_util::serialize_big(&(OrdHistoryRow::CODE, &address)).unwrap()
    }

    pub fn prefix_end(address: String) -> Bytes {
        bincode_util::serialize_big(&(
            OrdHistoryRow::CODE,
            address.as_bytes(),
            Txid::from_hex("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
                .unwrap()
                .into_inner(),
        ))
        .unwrap()
    }

    pub fn prefix_height(address: String, height: u32) -> Bytes {
        bincode_util::serialize_big(&(OrdHistoryRow::CODE, &address, height)).unwrap()
    }

    pub fn create_db_key(address: String, location: &Location) -> Vec<u8> {
        OrdHistoryKey {
            owner: address,
            location: location.clone(),
        }
        .to_raw()
        .unwrap()
    }

    pub fn to_db_row(self) -> DBRow {
        DBRow {
            key: Self::create_db_key(self.key.owner, &self.key.location),
            value: self.value.get_raw(),
        }
    }

    pub fn to_temp_db_row(self, block_height: u32) -> DBRow {
        DBRow {
            key: Self::get_temp_db_key(self.key.owner, &self.key.location, block_height),
            value: self.value.get_raw(),
        }
    }

    pub fn from_row(row: DBRow) -> Self {
        let value = Self::value_from_raw(&row.value);

        OrdHistoryRow {
            key: OrdHistoryKey::from_raw(row.key).unwrap(),
            value,
        }
    }

    pub fn value_from_raw(value: &Vec<u8>) -> OrdHistoryValue {
        OrdHistoryValue::from_raw(value).unwrap()
    }

    pub fn get_location(&self) -> Location {
        self.key.location.clone()
    }

    pub fn get_address(&self) -> String {
        self.key.owner.clone()
    }

    pub fn get_inscription_number(&self) -> u64 {
        self.value.inscription_number
    }

    pub fn get_temp_db_key(address: String, location: &Location, block_height: u32) -> Vec<u8> {
        [
            Self::create_db_key(address, location),
            block_height.to_be_bytes().to_vec(),
        ]
        .concat()
    }

    pub fn from_temp_db_row(row: DBRow) -> anyhow::Result<(Self, u32)> {
        let history_row = Self::from_row(DBRow {
            key: row.key[..row.key.len() - 4].to_vec(),
            value: row.value,
        });

        let height = u32::from_be_bytes(row.key[row.key.len() - 4..].try_into().unwrap());

        Ok((history_row, height))
    }
}

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
        let (_, owner): (String, String) =
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
        let (_, _, owner): (String, u32, String) =
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
    pub block_height: u32,
    pub content_length: usize,
    pub value: u64,
}

impl InscriptionExtraDataValue {
    pub fn from_raw(value: Vec<u8>) -> anyhow::Result<Self> {
        bincode_util::deserialize_big(&value).anyhow_as("Cannot deserialize value")
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct InscriptionExtraData {
    pub location: Location,
    #[serde(flatten)]
    pub value: InscriptionExtraDataValue,
}

impl InscriptionExtraData {
    const ERROR_MESSAGE: &'static str = "Cannot deserialize / serialize InscriptionExtraData";

    pub fn new(
        location: Location,
        owner: String,
        block_height: u32,
        content_type: String,
        content_length: usize,
        value: u64,
    ) -> Self {
        Self {
            location,
            value: InscriptionExtraDataValue {
                owner,
                block_height,
                content_length,
                content_type,
                value,
            },
        }
    }

    pub fn get_db_key(location: Location) -> Vec<u8> {
        bincode_util::serialize_big(&(
            OUTPOINT_IS_INSCRIPTION,
            location.outpoint.txid.into_inner(),
            location.outpoint.vout,
            location.offset,
        ))
        .anyhow_as(Self::ERROR_MESSAGE)
        .unwrap()
    }

    pub fn from_raw(value: DBRow) -> anyhow::Result<Self> {
        let (_, txid, vout, offset): (String, [u8; 32], u32, u64) =
            bincode_util::deserialize_big(&value.key).anyhow_as(Self::ERROR_MESSAGE)?;

        let location = Location::new_from_txid(&txid, vout, offset);

        let value = InscriptionExtraDataValue::from_raw(value.value)?;

        Ok(Self { location, value })
    }

    pub fn to_db_row(&self) -> anyhow::Result<DBRow> {
        Ok(DBRow {
            key: Self::get_db_key(self.location.clone()),
            value: bincode_util::serialize_big(&self.value).anyhow_as(Self::ERROR_MESSAGE)?,
        })
    }

    pub fn get_temp_db_key(block_height: u32, location: &Location) -> Vec<u8> {
        [
            Self::get_temp_db_iter_key(block_height),
            location.into_bytes().unwrap(),
        ]
        .concat()
    }

    pub fn get_temp_db_iter_key(block_height: u32) -> Vec<u8> {
        bincode_util::serialize_big(&(OUTPOINT_IS_INSCRIPTION, block_height)).unwrap()
    }

    pub fn from_temp_db(row: DBRow) -> anyhow::Result<Self> {
        let (_, _, txid_raw, vout, offset): (String, u32, [u8; 32], u32, u64) =
            bincode_util::deserialize_big(&row.key).anyhow_as(Self::ERROR_MESSAGE)?;

        let location = Location::new_from_txid(&txid_raw, vout, offset);

        let extra_data = InscriptionExtraDataValue::from_raw(row.value)?;

        Ok(Self {
            location,
            value: extra_data,
        })
    }

    pub fn to_temp_db_row(&self, block_height: u32, location: &Location) -> anyhow::Result<DBRow> {
        Ok(DBRow {
            key: Self::get_temp_db_key(block_height, location),
            value: bincode_util::serialize_big(&self.value).anyhow_as(Self::ERROR_MESSAGE)?,
        })
    }

    pub fn find_by_outpoint(outpoint: &OutPoint) -> Vec<u8> {
        bincode_util::serialize_big(&(
            OUTPOINT_IS_INSCRIPTION,
            outpoint.txid.into_inner(),
            outpoint.vout,
        ))
        .unwrap()
    }
}

pub struct PartialTxs {
    pub txs: Vec<Txid>,
    pub last_outpoint: OutPoint,
    pub block_height: u32,
}

impl PartialTxs {
    pub fn from_db(value: DBRow) -> anyhow::Result<Self> {
        let (_, txid, vout): (String, [u8; 32], u32) = bincode_util::deserialize_big(&value.key)
            .anyhow_as("Failed to decode partial txs key")?;
        let txid = Txid::from_slice(&txid)?;
        let outpoint = OutPoint { txid, vout };

        let txs: Vec<Txid> = value
            .value
            .chunks(32)
            .map(|x| Txid::from_slice(x))
            .try_collect()
            .anyhow_as("Failed to decode transactions")?;

        Ok(Self {
            txs,
            last_outpoint: outpoint,
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
        bincode_util::serialize_big(&(
            PARTIAL_TXID_TO_TXIDS,
            self.last_outpoint.txid.into_inner(),
            self.last_outpoint.vout,
        ))
        .unwrap()
    }

    pub fn get_temp_iter_key(block_height: u32) -> Vec<u8> {
        bincode_util::serialize_big(&(PARTIAL_TXID_TO_TXIDS, block_height)).unwrap()
    }

    pub fn get_temp_db_key(block_height: u32, outpoint: &OutPoint) -> Vec<u8> {
        bincode_util::serialize_big(&(
            PARTIAL_TXID_TO_TXIDS,
            block_height,
            outpoint.txid.into_inner(),
            outpoint.vout,
        ))
        .unwrap()
    }

    pub fn to_temp_db_row(&self) -> anyhow::Result<DBRow> {
        Ok(DBRow {
            key: Self::get_temp_db_key(self.block_height, &self.last_outpoint),
            value: self
                .txs
                .iter()
                .map(|x| x.into_inner())
                .collect_vec()
                .concat(),
        })
    }

    pub fn from_temp_db(row: DBRow) -> anyhow::Result<Self> {
        let (_, block_height, txid, vout): (String, u32, [u8; 32], u32) =
            bincode_util::deserialize_big(&row.key)
                .anyhow_as("Failed to decode partial txs key")?;

        let last_txid = Txid::from_slice(&txid).anyhow_as("Failed to decode last_txid")?;
        let last_outpoint = OutPoint {
            txid: last_txid,
            vout,
        };

        let txs: Vec<Txid> = row
            .value
            .chunks(32)
            .map(|x| Txid::from_slice(x))
            .try_collect()
            .anyhow_as("Failed to decode transactions")?;

        Ok(Self {
            txs,
            last_outpoint,
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
            key: Self::get_db_key(),
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

    pub fn temp_iter_db_key() -> Vec<u8> {
        bincode_util::serialize_big(&(INSCRIPTION_NUMBER)).unwrap()
    }

    pub fn from_temp_db_row(row: DBRow) -> (u32, Self) {
        let (_, height) = bincode_util::deserialize_big::<(String, _)>(&row.key)
            .expect("Cannot deserialize LastInscriptionNumber");
        let number: u64 = bincode_util::deserialize_big(&row.value)
            .expect("Cannot deserialize LastInscriptionNumber");
        (height, Self { number })
    }

    pub fn to_temp_db_row(&self, block_height: u32) -> anyhow::Result<DBRow> {
        Ok(DBRow {
            key: Self::get_temp_db_key(block_height),
            value: self.to_db()?.value,
        })
    }
}

#[derive(Serialize, Deserialize)]
pub struct InscriptionContent {
    pub content_type: String,
    pub content: Vec<u8>,
    pub inscription_id: InscriptionId,
    pub number: u64,
}

pub fn update_last_block_number(
    first_inscription_block: usize,
    store: &Store,
    block_height: u32,
    is_temp: bool,
) -> anyhow::Result<()> {
    let db = match is_temp {
        true => store.temp_db(),
        false => store.inscription_db(),
    };

    let block_entry = store
        .indexed_headers
        .read()
        .header_by_height(block_height as usize)
        .anyhow_as("Header by height not found")?
        .hash()
        .clone();

    let prev_block_height = {
        if let Some(ot) = db.get(b"ot") {
            let prev_hash = BlockHash::from_slice(&ot)?;
            store
                .indexed_headers
                .read()
                .header_by_blockhash(&prev_hash)
                .map(|x| x.height())
        } else {
            Some(first_inscription_block)
        }
    };

    if let Some(prev_block_height) = prev_block_height {
        if prev_block_height < block_height as usize {
            db.put(b"ot", &block_entry.into_inner());
        }
    }

    Ok(())
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct Location {
    pub outpoint: OutPoint,
    pub offset: u64,
}

impl Location {
    pub fn new(outpoint: OutPoint, offset: u64) -> Self {
        Self { outpoint, offset }
    }

    pub fn new_from_txid(txid: &[u8; 32], vout: u32, offset: u64) -> Self {
        Self {
            outpoint: OutPoint {
                txid: Txid::from_slice(txid).unwrap(),
                vout,
            },
            offset,
        }
    }

    pub fn from_raw(value: Vec<u8>) -> anyhow::Result<Self> {
        let (txid, vout, offset): ([u8; 32], u32, u64) =
            bincode_util::deserialize_big(&value).anyhow_as("Cannot deserialize Location")?;

        Ok(Self {
            offset,
            outpoint: OutPoint {
                txid: Txid::from_slice(&txid).anyhow_as("Cannot deserialize Location Txid")?,
                vout,
            },
        })
    }

    pub fn from_hex(value: &str) -> anyhow::Result<Self> {
        let items = value.split(":").collect_vec();

        if items.len() != 3 {
            anyhow::bail!("Invalid location format");
        }

        let txid = Txid::from_hex(items[0]).anyhow_as("Invalid txid")?;
        let vout: u32 = items[1].parse().anyhow_as("Invalid vout")?;
        let offset: u64 = items[2].parse().anyhow_as("Invalid offset")?;

        Ok(Self {
            offset,
            outpoint: OutPoint { txid, vout },
        })
    }

    pub fn into_bytes(&self) -> anyhow::Result<Vec<u8>> {
        bincode_util::serialize_big(&(
            self.outpoint.txid.into_inner(),
            self.outpoint.vout,
            self.offset,
        ))
        .anyhow_as("Cannot serialize Location")
    }
}
