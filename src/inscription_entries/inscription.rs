use bitcoin::{hashes::Hash, Txid};

use crate::{
    db_key,
    media::Media,
    new_index::DBRow,
    util::{bincode_util, errors::AsAnyhow, TransactionStatus},
};

use super::{
    index::{ADDRESS_TO_ORD_STATS, INSCRIPTION_ID_TO_META, TXID_IS_INSCRIPTION},
    Entry, InscriptionId,
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

#[derive(Debug, Deserialize, Serialize)]
pub struct InscriptionMeta {
    pub content_type: String,
    pub content_length: usize,
    pub outpoint: Txid,
    pub genesis: Txid,
    pub inscription_id: InscriptionId,
    pub number: usize,
}

impl InscriptionMeta {
    pub(crate) fn new(
        content_type: String,
        content_length: usize,
        outpoint: Txid,
        genesis: Txid,
        number: usize,
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
        bincode_util::deserialize_big(value).anyhow_as("Cannot deserialize InscriptionMeta")
    }

    pub(crate) fn to_db_row(&self) -> anyhow::Result<DBRow> {
        let inscription_id = InscriptionId {
            index: 0,
            txid: self.genesis,
        };
        Ok(DBRow {
            key: db_key!(INSCRIPTION_ID_TO_META, &inscription_id.store().anyhow()?),
            value: bincode_util::serialize_big(self)
                .anyhow_as("Cannot serialize InscriptionMeta")?,
        })
    }
}

#[derive(Deserialize, Serialize, Debug, Default)]
pub(crate) struct UserOrdStats {
    pub amount: u64,
    pub count: u64,
}

impl UserOrdStats {
    pub(crate) fn new(amount: u64, count: u64) -> Self {
        Self { amount, count }
    }

    pub(crate) fn from_raw(value: &Vec<u8>) -> anyhow::Result<Self> {
        bincode_util::deserialize_big(value).anyhow_as("Cannot deserialize UserOrdStats")
    }

    pub(crate) fn to_db_row(&self, owner: &str) -> anyhow::Result<DBRow> {
        Ok(DBRow {
            key: db_key!(ADDRESS_TO_ORD_STATS, owner.as_bytes()),
            value: bincode_util::serialize_big(self).anyhow_as("Cannot serialize UserOrdStats")?,
        })
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub(crate) struct InscriptionExtraData {
    pub(crate) genesis: Txid,
    pub(crate) owner: String,
    pub(crate) block_height: u32,
}

impl InscriptionExtraData {
    pub(crate) fn new(genesis: Txid, owner: String, block_height: u32) -> Self {
        Self {
            genesis,
            owner,
            block_height,
        }
    }

    pub(crate) fn from_raw(value: &Vec<u8>) -> anyhow::Result<Self> {
        bincode_util::deserialize_big(value).anyhow_as("Cannot deserialize InscriptionExtraData")
    }

    pub(crate) fn to_db_row(&self, last_txid: &Txid) -> anyhow::Result<DBRow> {
        Ok(DBRow {
            key: db_key!(TXID_IS_INSCRIPTION, &last_txid.into_inner()),
            value: bincode_util::serialize_big(self)
                .anyhow_as("Cannot serialize InscriptionExtraData")?,
        })
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
