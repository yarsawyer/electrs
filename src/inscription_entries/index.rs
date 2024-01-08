use crate::{config::Config, new_index::DB};

use super::*;

use std::{
    collections::BTreeMap,
    convert::TryInto,
    io::Cursor,
    path::{Path, PathBuf},
};

use self::entry::{Entry, InscriptionEntry};

const SCHEMA_VERSION: u64 = 3;

pub(crate) struct InscriptionIndex {
    database: DB,
}

#[derive(Debug, PartialEq)]
pub(crate) enum List {
    Spent,
    Unspent(Vec<(u128, u128)>),
}

#[derive(Copy, Clone)]
#[repr(u64)]
pub(crate) enum Statistic {
    Schema = 0,
    Commits = 1,
    LostSats = 2,
    OutputsTraversed = 3,
    SatRanges = 4,
}

impl Statistic {
    fn key(self) -> u64 {
        self.into()
    }
}

impl From<Statistic> for u64 {
    fn from(statistic: Statistic) -> Self {
        statistic as u64
    }
}

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
    ($prefix:expr, $value:expr) => {
        format!("{}{}", $prefix, $value).as_bytes()
    };
}

define_prefix! { SAT_TO_INSCRIPTION_ID, STID }
define_prefix! { SATPOINT_TO_INSCRIPTION_ID, SPTID }
define_prefix! { INSCRIPTION_ID_TO_SATPOINT, IDTS }
define_prefix! { INSCRIPTION_ID_TO_TXIDS, IITT }
define_prefix! { INSCRIPTION_TXID_TO_TX, ITTT }
define_prefix! { INSCRIPTION_NUMBER_TO_INSCRIPTION_ID, INTI }
define_prefix! { INSCRIPTION_ID_TO_INSCRIPTION_ENTRY, IITE }
define_prefix! { PARTIAL_TXID_TO_TXIDS, PTTT }
define_prefix! { OUTPOINT_TO_VALUE, OTV }
define_prefix! { ID_TO_ENTRY, ITE }
define_prefix! { NUMBER_TO_ID, ITE }

impl InscriptionIndex {
    pub(crate) fn open(options: &Config) -> Result<Self> {
        let database = DB::open(&Path::new("inscriptions"), options);

        Ok(Self { database })
    }

    pub(crate) fn get_inscription_id_by_sat(&self, sat: Sat) -> Result<Option<InscriptionId>> {
        Ok(self
            .database
            .get(db_key!(SAT_TO_INSCRIPTION_ID, sat.n()))
            .and_then(|v| InscriptionId::from_str(&String::from_utf8(v).unwrap()).ok()))
    }

    pub(crate) fn get_inscription_id_by_inscription_number(
        &self,
        n: u64,
    ) -> Result<Option<InscriptionId>> {
        Ok(self
            .database
            .get(db_key!(INSCRIPTION_NUMBER_TO_INSCRIPTION_ID, n))
            .and_then(|v| InscriptionId::from_str(&String::from_utf8(v).unwrap()).ok()))
    }

    pub(crate) fn get_inscription_satpoint_by_id(
        &self,
        inscription_id: InscriptionId,
    ) -> Result<Option<SatPoint>> {
        Ok(self
            .database
            .get(db_key!(
                INSCRIPTION_ID_TO_SATPOINT,
                String::from_utf8(inscription_id.store().to_vec()).unwrap()
            ))
            .and_then(|v| SatPoint::from_str(&String::from_utf8(v).unwrap()).ok()))
    }

    pub(crate) fn get_inscription_by_id(
        &self,
        inscription_id: InscriptionId,
    ) -> Result<Option<Inscription>> {
        if self
            .database
            .get(db_key!(
                INSCRIPTION_ID_TO_SATPOINT,
                String::from_utf8(inscription_id.store().to_vec()).unwrap()
            ))
            .is_none()
        {
            return Ok(None);
        }

        let txids_result = self.database.get(db_key!(
            INSCRIPTION_ID_TO_TXIDS,
            String::from_utf8(inscription_id.store().to_vec()).unwrap()
        ));

        match txids_result {
            Some(txids) => {
                let mut txs = vec![];

                for i in 0..txids.len() / 32 {
                    let txid_buf = &txids[i * 32..i * 32 + 32];
                    let tx_result = self.database.get(db_key!(
                        INSCRIPTION_TXID_TO_TX,
                        String::from_utf8(txid_buf.to_vec()).unwrap()
                    ));

                    match tx_result {
                        Some(tx_result) => {
                            let mut cursor = Cursor::new(tx_result);
                            let tx = bitcoin::Transaction::consensus_decode(&mut cursor)?;
                            txs.push(tx);
                        }
                        None => return Ok(None),
                    }
                }

                let parsed_inscription = Inscription::from_transactions(txs);

                match parsed_inscription {
                    ParsedInscription::None => return Ok(None),
                    ParsedInscription::Partial => return Ok(None),
                    ParsedInscription::Complete(inscription) => Ok(Some(inscription)),
                }
            }

            None => return Ok(None),
        }
    }

    pub(crate) fn get_inscriptions_on_output(
        &self,
        outpoint: OutPoint,
    ) -> Result<Vec<InscriptionId>> {
        Ok(Self::inscriptions_on_output(&self.database, outpoint)?
            .into_iter()
            .map(|(_satpoint, inscription_id)| inscription_id)
            .collect())
    }

    pub(crate) fn get_inscriptions(
        &self,
        n: Option<usize>,
    ) -> Result<BTreeMap<SatPoint, InscriptionId>> {
        Ok(self
            .database
            .iter_scan_from(SATPOINT_TO_INSCRIPTION_ID.as_bytes(), &[0u8; 44])
            .map(|dbrow| {
                (
                    Entry::load(dbrow.key.try_into().unwrap()),
                    Entry::load(dbrow.value.try_into().unwrap()),
                )
            })
            .take(n.unwrap_or(usize::MAX))
            .collect())
    }

    pub(crate) fn get_homepage_inscriptions(&self) -> Result<Vec<InscriptionId>> {
        Ok(self
            .database
            .iter_scan_reverse(db_key!(INSCRIPTION_NUMBER_TO_INSCRIPTION_ID, ""), &[])
            .take(8)
            .map(|dbrow| Entry::load(dbrow.value.try_into().unwrap()))
            .collect())
    }

    pub(crate) fn get_feed_inscriptions(&self, n: usize) -> Result<Vec<(u64, InscriptionId)>> {
        Ok(self
            .database
            .iter_scan_reverse(db_key!(INSCRIPTION_NUMBER_TO_INSCRIPTION_ID, ""), &[])
            .take(n)
            .map(|dbrow| {
                (
                    u64::from_le_bytes(dbrow.key.try_into().unwrap()),
                    Entry::load(dbrow.value.try_into().unwrap()),
                )
            })
            .collect())
    }

    pub(crate) fn get_inscription_entry(
        &self,
        inscription_id: InscriptionId,
    ) -> Result<Option<InscriptionEntry>> {
        Ok(self
            .database
            .get(db_key!(
                INSCRIPTION_ID_TO_INSCRIPTION_ENTRY,
                String::from_utf8(inscription_id.store().to_vec()).unwrap()
            ))
            .map(|value| {
                let (part1, rest) = value.split_at(8);
                let (part2, rest) = rest.split_at(8);
                let (part3, rest) = rest.split_at(8);
                let (part4, part5) = rest.split_at(16);

                let value1 = u64::from_le_bytes(part1.try_into().expect("Incorrect length"));
                let value2 = u64::from_le_bytes(part2.try_into().expect("Incorrect length"));
                let value3 = u64::from_le_bytes(part3.try_into().expect("Incorrect length"));
                let value4 = u128::from_le_bytes(part4.try_into().expect("Incorrect length"));
                let value5 = u32::from_le_bytes(part5.try_into().expect("Incorrect length"));

                InscriptionEntry::load((value1, value2, value3, value4, value5))
            }))
    }

    pub(crate) fn inscriptions_on_output<'tx>(
        database: &'tx DB,
        outpoint: OutPoint,
    ) -> Result<impl Iterator<Item = (SatPoint, InscriptionId)> + 'tx> {
        let start = SatPoint {
            outpoint,
            offset: 0,
        }
        .store();

        Ok(database
            .iter_scan_from(db_key!(SATPOINT_TO_INSCRIPTION_ID, ""), &start)
            .map(|dbrow| {
                (
                    Entry::load(dbrow.key.try_into().unwrap()),
                    Entry::load(dbrow.value.try_into().unwrap()),
                )
            }))
    }
}
