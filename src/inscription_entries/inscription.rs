use anyhow::Ok;
use bitcoin::{hashes::Hash, Txid};

use crate::{
    db_key,
    media::Media,
    new_index::DBRow,
    util::{bincode_util, errors::AsAnyhow, Bytes, TransactionStatus},
};

use super::{
    index::{
        ADDRESS_TO_ORD_STATS, INSCRIPTION_ID_TO_META, LAST_INSCRIPTION_NUMBER, TXID_IS_INSCRIPTION,
    },
    Entry, InscriptionId,
};

use {
    bitcoin::{
        blockdata::{opcodes, script},
        Script, Transaction,
    },
    std::str,
};

fn get_history_key(block_height: u32, owner: String, txid: Txid) -> anyhow::Result<Vec<u8>> {
    bincode_util::serialize_big(&(b'H', block_height, owner, txid))
        .anyhow_as("Failed to serialize history key")
}

const PROTOCOL_ID: &[u8] = b"ord";

#[derive(Debug, PartialEq, Clone)]
pub(crate) struct Inscription {
    body: Option<Vec<u8>>,
    content_type: Option<Vec<u8>>,
}

#[derive(Deserialize, Serialize)]
pub(crate) struct Ord {
    #[serde(flatten)]
    pub meta: InscriptionMeta,
    pub status: TransactionStatus,
    pub owner: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct OrdHistoryKey {
    pub code: u8,
    pub address: String,
    pub confirmed_height: u32,
    pub txid: Txid,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct OrdHistoryValue {
    pub value: u64,
    pub inscription_id: InscriptionId,
    pub inscription_number: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OrdHistoryRow {
    pub key: OrdHistoryKey,
    pub value: OrdHistoryValue,
}

impl OrdHistoryRow {
    const CODE: u8 = b'O';

    pub fn new(address: String, confirmed_height: u32, txid: Txid, value: OrdHistoryValue) -> Self {
        let key = OrdHistoryKey {
            code: OrdHistoryRow::CODE,
            address,
            confirmed_height,
            txid,
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

    pub fn into_row(self) -> DBRow {
        DBRow {
            key: bincode_util::serialize_big(&self.key).unwrap(),
            value: bincode_util::serialize_big(&self.value).unwrap(),
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

    pub fn get_txid(&self) -> Txid {
        self.key.txid
    }

    pub fn get_value(&self) -> u64 {
        self.value.value
    }

    pub fn get_address(&self) -> String {
        self.key.address.clone()
    }

    pub fn get_inscription_id(&self) -> InscriptionId {
        self.value.inscription_id
    }

    pub fn get_inscription_number(&self) -> u64 {
        self.value.inscription_number
    }

    pub fn to_temp_db_row(self, block_height: u32) -> anyhow::Result<DBRow> {
        Ok(DBRow {
            key: get_history_key(block_height, self.key.address.clone(), self.key.txid)?,
            value: bincode_util::serialize_big(&HistoryType::HistoryRow(self))
                .anyhow_as("Cannot serialize OrdHistoryValue")?,
        })
    }

    pub fn from_temp_db_row(value: DBRow) -> anyhow::Result<Self> {
        let value: HistoryType = bincode_util::deserialize_big(&value.value)
            .anyhow_as("Cannot deserialize OrdHistoryValue")?;

        match value {
            HistoryType::HistoryRow(value) => Ok(value),
            _ => anyhow::bail!("Cannot deserialize OrdHistoryValue"),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct InscriptionMeta {
    pub content_type: String,
    pub content_length: usize,
    pub outpoint: Txid,
    pub genesis: Txid,
    pub inscription_id: InscriptionId,
    pub number: u64,
}

impl InscriptionMeta {
    const ERROR_MESSAGE: &'static str = "Cannot deserialize / serialize InscriptionMeta";

    pub(crate) fn new(
        content_type: String,
        content_length: usize,
        outpoint: Txid,
        genesis: Txid,
        number: u64,
    ) -> Self {
        Self {
            content_type,
            content_length,
            outpoint,
            genesis,
            number,
            inscription_id: InscriptionId::from(genesis),
        }
    }

    pub(crate) fn from_raw(value: &Vec<u8>) -> anyhow::Result<Self> {
        bincode_util::deserialize_big(value).anyhow_as(Self::ERROR_MESSAGE)
    }

    pub(crate) fn to_db_row(&self) -> anyhow::Result<DBRow> {
        let inscription_id = InscriptionId {
            index: 0,
            txid: self.genesis,
        };
        Ok(DBRow {
            key: db_key!(INSCRIPTION_ID_TO_META, &inscription_id.store().anyhow()?),
            value: bincode_util::serialize_big(self).anyhow_as(Self::ERROR_MESSAGE)?,
        })
    }

    pub(crate) fn to_temp_db_row(
        self,
        block_height: u32,
        owner: String,
        txid: Txid,
    ) -> anyhow::Result<DBRow> {
        Ok(DBRow {
            key: get_history_key(block_height, owner, txid)?,
            value: bincode_util::serialize_big(&HistoryType::Meta(self))
                .anyhow_as(Self::ERROR_MESSAGE)?,
        })
    }

    pub(crate) fn from_temp_db_row(value: DBRow) -> anyhow::Result<Self> {
        let value: HistoryType =
            bincode_util::deserialize_big(&value.value).anyhow_as(Self::ERROR_MESSAGE)?;

        match value {
            HistoryType::Meta(value) => Ok(value),
            _ => anyhow::bail!(Self::ERROR_MESSAGE),
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Default)]
pub struct UserOrdStats {
    pub amount: u64,
    pub count: u64,
}

impl UserOrdStats {
    const PREFIX: u8 = b'S';

    pub fn new(amount: u64, count: u64) -> Self {
        Self { amount, count }
    }

    pub fn from_raw(value: &Vec<u8>) -> anyhow::Result<Self> {
        bincode_util::deserialize_big(value).anyhow_as("Cannot deserialize UserOrdStats")
    }

    pub fn owner_from_key(key: Vec<u8>) -> anyhow::Result<String> {
        let (_, _, owner): (u8, u32, String) =
            bincode_util::deserialize_big(&key).anyhow_as("Cannot deserialize key")?;
        Ok(owner)
    }

    pub fn to_db_row(&self, owner: &str) -> anyhow::Result<DBRow> {
        Ok(DBRow {
            key: db_key!(ADDRESS_TO_ORD_STATS, owner.as_bytes()),
            value: bincode_util::serialize_big(self).anyhow_as("Cannot serialize UserOrdStats")?,
        })
    }

    pub fn from_temp_db(value: DBRow) -> anyhow::Result<(Self, String)> {
        let (_, _, owner): (u8, u32, String) =
            bincode_util::deserialize_big(&value.key).anyhow_as("Cannot deserialize key")?;
        let (amount, count) = bincode_util::deserialize_big(&value.value)
            .anyhow_as("Cannot deserialize UserOrdStats")?;
        Ok((Self { amount, count }, owner))
    }

    pub fn to_temp_db_row(&self, height: u32, owner: &str) -> anyhow::Result<DBRow> {
        Ok(DBRow {
            key: bincode_util::serialize_big(&(Self::PREFIX, height, owner.as_bytes()))
                .anyhow_as("Cannot serialize UserOrdStats")?,
            value: bincode_util::serialize_big(&(self.amount, self.count))
                .anyhow_as("Cannot serialize UserOrdStats")?,
        })
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) enum HistoryType {
    Meta(InscriptionMeta),
    ExtraData(InscriptionExtraData),
    HistoryRow(OrdHistoryRow),
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub(crate) struct InscriptionExtraData {
    pub(crate) genesis: Txid,
    pub(crate) owner: String,
    pub(crate) block_height: u32,
}

impl InscriptionExtraData {
    const ERROR_MESSAGE: &'static str = "Cannot deserialize / serialize InscriptionExtraData";

    pub(crate) fn new(genesis: Txid, owner: String, block_height: u32) -> Self {
        Self {
            genesis,
            owner,
            block_height,
        }
    }

    pub(crate) fn from_raw(value: &Vec<u8>) -> anyhow::Result<Self> {
        bincode_util::deserialize_big(value).anyhow_as(Self::ERROR_MESSAGE)
    }

    pub(crate) fn to_db_row(&self, last_txid: &Txid) -> anyhow::Result<DBRow> {
        Ok(DBRow {
            key: db_key!(TXID_IS_INSCRIPTION, &last_txid.into_inner()),
            value: bincode_util::serialize_big(self).anyhow_as(Self::ERROR_MESSAGE)?,
        })
    }

    pub(crate) fn from_temp_db(value: DBRow) -> anyhow::Result<Self> {
        let value: HistoryType =
            bincode_util::deserialize_big(&value.value).anyhow_as(Self::ERROR_MESSAGE)?;

        match value {
            HistoryType::ExtraData(value) => Ok(value),
            _ => anyhow::bail!(Self::ERROR_MESSAGE),
        }
    }

    pub(crate) fn to_temp_db_row(&self, txid: Txid, block_height: u32) -> anyhow::Result<DBRow> {
        Ok(DBRow {
            key: get_history_key(block_height, self.owner.clone(), txid)?,
            value: bincode_util::serialize_big(&HistoryType::ExtraData(self.clone()))
                .anyhow_as(Self::ERROR_MESSAGE)?,
        })
    }
}

pub(crate) struct PartialTxs {
    pub(crate) txs: Vec<Txid>,
    pub(crate) last_txid: Txid,
    pub(crate) block_height: u32,
}

impl PartialTxs {
    const TEMP_PREFIX: u8 = b'P';
    const PREFIX: &'static [u8; 4] = b"PTTT";
    const ERROR_MESSAGE: &'static str = "Cannot deserialize / serialize PartialTxs";

    pub(crate) fn new(txs: Vec<Txid>, last_txid: Txid, block_height: u32) -> Self {
        Self {
            txs,
            last_txid,
            block_height,
        }
    }

    pub(crate) fn from_temp_db_row(row: DBRow) -> anyhow::Result<Self> {
        let (_, last_txid, block_height): (u8, Txid, u32) =
            bincode_util::deserialize_big(&row.key).anyhow_as(Self::ERROR_MESSAGE)?;
        let txs: Vec<Txid> =
            bincode_util::deserialize_big(&row.value).anyhow_as(Self::ERROR_MESSAGE)?;

        Ok(Self {
            txs,
            last_txid,
            block_height,
        })
    }

    pub(crate) fn to_temp_db_row(&self) -> anyhow::Result<DBRow> {
        Ok(DBRow {
            key: bincode_util::serialize_big(&(
                Self::TEMP_PREFIX,
                self.last_txid,
                self.block_height,
            ))
            .anyhow_as(Self::ERROR_MESSAGE)?,
            value: bincode_util::serialize_big(&self.txs).anyhow_as(Self::ERROR_MESSAGE)?,
        })
    }

    pub(crate) fn from_db(value: DBRow) -> anyhow::Result<Self> {
        let (_, txid): ([u8; 4], Txid) =
            bincode_util::deserialize_big(&value.key).anyhow_as(Self::ERROR_MESSAGE)?;

        let txs: Vec<Txid> =
            bincode_util::deserialize_big(&value.value).anyhow_as(Self::ERROR_MESSAGE)?;

        Ok(Self {
            txs,
            last_txid: txid,
            block_height: 0,
        })
    }

    pub(crate) fn to_db(&self) -> anyhow::Result<DBRow> {
        Ok(DBRow {
            key: bincode_util::serialize_big(&(Self::PREFIX, &self.last_txid))
                .anyhow_as(Self::ERROR_MESSAGE)?,
            value: bincode_util::serialize_big(&self.txs).anyhow_as(Self::ERROR_MESSAGE)?,
        })
    }

    pub(crate) fn get_db_key(&self) -> Vec<u8> {
        bincode_util::serialize_big(&(Self::PREFIX, self.last_txid)).expect(Self::ERROR_MESSAGE)
    }

    pub(crate) fn get_temp_db_key(&self, block_height: u32) -> Vec<u8> {
        bincode_util::serialize_big(&(Self::TEMP_PREFIX, self.last_txid, block_height))
            .expect(Self::ERROR_MESSAGE)
    }

    pub(crate) fn push(&mut self, txid: Txid) {
        self.txs.push(txid);
    }
}

#[derive(Debug, PartialEq)]
pub(crate) enum ParsedInscription {
    None,
    Partial,
    Complete(Inscription),
}

impl Inscription {
    #[cfg(test)]
    pub(crate) fn new(content_type: Option<Vec<u8>>, body: Option<Vec<u8>>) -> Self {
        Self { content_type, body }
    }

    pub(crate) fn from_transactions(txs: Vec<Transaction>) -> ParsedInscription {
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

    pub(crate) fn append_reveal_script(&self, builder: script::Builder) -> Script {
        self.append_reveal_script_to_builder(builder).into_script()
    }

    pub(crate) fn media(&self) -> Media {
        if self.body.is_none() {
            return Media::Unknown;
        }

        let Some(content_type) = self.content_type() else {
            return Media::Unknown;
        };

        content_type.parse().unwrap_or(Media::Unknown)
    }

    pub(crate) fn body(&self) -> Option<&[u8]> {
        Some(self.body.as_ref()?)
    }

    pub(crate) fn into_body(self) -> Option<Vec<u8>> {
        self.body
    }

    pub(crate) fn content_length(&self) -> Option<usize> {
        Some(self.body()?.len())
    }

    pub(crate) fn content_type(&self) -> Option<&str> {
        str::from_utf8(self.content_type.as_ref()?).ok()
    }

    #[cfg(test)]
    pub(crate) fn to_witness(&self) -> bitcoin::Witness {
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
pub(crate) struct LastInscriptionNumber {
    pub number: u64,
    pub height: u32,
}

impl LastInscriptionNumber {
    const PREFIX: u8 = b'L';
    pub(crate) fn new(number: u64, height: u32) -> Self {
        Self { number, height }
    }

    pub(crate) fn from_temp_db_row(row: DBRow) -> anyhow::Result<Self> {
        let (_, height): (u8, u32) = bincode_util::deserialize_big(&row.key)
            .anyhow_as("Cannot deserialize LastInscriptionNumber")?;
        let number: u64 = bincode_util::deserialize_big(&row.value)
            .anyhow_as("Cannot deserialize LastInscriptionNumber")?;

        Ok(Self { number, height })
    }

    pub(crate) fn to_temp_db_row(&self) -> anyhow::Result<DBRow> {
        Ok(DBRow {
            key: bincode_util::serialize_big(&(Self::PREFIX, self.height))
                .anyhow_as("Cannot serialize LastInscriptionNumber")?,
            value: bincode_util::serialize_big(&self.number)
                .anyhow_as("Cannot serialize LastInscriptionNumber")?,
        })
    }

    pub(crate) fn from_db(value: DBRow) -> anyhow::Result<Self> {
        let number: u64 = bincode_util::deserialize_big(&value.value)
            .anyhow_as("Cannot deserialize LastInscriptionNumber")?;

        Ok(Self { number, height: 0 })
    }

    pub(crate) fn to_db(&self) -> anyhow::Result<DBRow> {
        Ok(DBRow {
            key: bincode_util::serialize_big(LAST_INSCRIPTION_NUMBER)
                .anyhow_as("Cannot serialize LastInscriptionNumber")?,
            value: bincode_util::serialize_big(&self.number)
                .anyhow_as("Cannot serialize LastInscriptionNumber")?,
        })
    }

    pub(crate) fn get_db_key(&self) -> Vec<u8> {
        bincode_util::serialize_big(LAST_INSCRIPTION_NUMBER)
            .expect("Cannot serialize LastInscriptionNumber")
    }

    pub(crate) fn get_temp_db_key(&self) -> Vec<u8> {
        bincode_util::serialize_big(&(Self::PREFIX, self.height))
            .expect("Cannot serialize LastInscriptionNumber")
    }
}
