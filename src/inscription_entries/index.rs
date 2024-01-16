use itertools::Itertools;

use crate::{config::Config, new_index::DB, util::errors::AsAnyhow};

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
    pub fn key(self) -> u64 {
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
    ($str:expr, $bytes:expr) => {{
        let str_as_bytes = $str.as_bytes();
        let mut result = Vec::with_capacity(str_as_bytes.len() + $bytes.len());
        result.extend_from_slice(str_as_bytes);
        result.extend_from_slice($bytes);
        result
    }};
}

define_prefix! { SAT_TO_INSCRIPTION_ID, STID }
define_prefix! { SAT_TO_SATPOINT, STS }
define_prefix! { SATPOINT_TO_INSCRIPTION_ID, SPTID }
define_prefix! { INSCRIPTION_ID_TO_SATPOINT, IDTS }
define_prefix! { INSCRIPTION_ID_TO_TXIDS, IITT }
define_prefix! { INSCRIPTION_ID_TO_META, IITM }
define_prefix! { INSCRIPTION_TXID_TO_TX, ITTT }
define_prefix! { INSCRIPTION_NUMBER_TO_INSCRIPTION_ID, INTI }
define_prefix! { INSCRIPTION_ID_TO_INSCRIPTION_ENTRY, IITE }
define_prefix! { PARTIAL_TXID_TO_TXIDS, PTTT }
define_prefix! { OUTPOINT_TO_VALUE, OTV }
define_prefix! { OUTPOINT_TO_SATRANGES, OTSR }
define_prefix! { STATISTIC_TO_COUNT, STC }
define_prefix! { LAST_INSCRIPTION_NUMBER, LIN }
define_prefix! { HEIGHT_TO_BLOCK_HASH, HTBH }
define_prefix! { ID_TO_ENTRY, ITE }
define_prefix! { TXID_IS_INSCRIPTION, TII }
define_prefix! { NUMBER_TO_ID, NTI }

impl InscriptionIndex {
    pub(crate) fn open(options: &Config) -> Result<Self> {
        let database = DB::open(&Path::new("inscriptions"), options);

        Ok(Self { database })
    }

    pub(crate) fn get_inscription_id_by_sat(&self, sat: Sat) -> Result<Option<InscriptionId>> {
        let Some(v) = self
            .database
            .get(&db_key!(SAT_TO_INSCRIPTION_ID, &sat.n().to_be_bytes()))
        else {
            return Ok(None);
        };
        let v = String::from_utf8(v).track_err()?;
        Ok(InscriptionId::from_str(&v).map(Some).track_err()?)
    }

    pub(crate) fn get_inscription_id_by_inscription_number(
        &self,
        n: u64,
    ) -> Result<Option<InscriptionId>> {
        let Some(v) = self.database.get(&db_key!(
            INSCRIPTION_NUMBER_TO_INSCRIPTION_ID,
            &n.to_be_bytes()
        )) else {
            return Ok(None);
        };

        let v = String::from_utf8(v).track_err()?;
        Ok(InscriptionId::from_str(&v).map(Some).track_err()?)
    }

    pub(crate) fn get_inscription_satpoint_by_id(
        &self,
        inscription_id: InscriptionId,
    ) -> Result<Option<SatPoint>> {
        let Some(v) = self.database.get(&db_key!(
            INSCRIPTION_ID_TO_SATPOINT,
            &inscription_id.store().track_err()?
        )) else {
            return Ok(None);
        };

        let v = String::from_utf8(v).track_err()?;
        Ok(SatPoint::from_str(&v).map(Some).track_err()?)
    }

    pub(crate) fn get_inscription_by_id(
        &self,
        inscription_id: InscriptionId,
    ) -> Result<Option<Inscription>> {
        if self
            .database
            .get(&db_key!(
                INSCRIPTION_ID_TO_SATPOINT,
                &inscription_id.store().track_err()?
            ))
            .is_none()
        {
            return Ok(None);
        }

        let txids_result = self.database.get(&db_key!(
            INSCRIPTION_ID_TO_TXIDS,
            &inscription_id.store().track_err()?
        ));

        match txids_result {
            Some(txids) => {
                let mut txs = vec![];

                for i in 0..txids.len() / 32 {
                    let txid_buf = &txids[i * 32..i * 32 + 32];
                    let tx_result = self
                        .database
                        .get(&db_key!(INSCRIPTION_TXID_TO_TX, txid_buf));

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
        Self::inscriptions_on_output(&self.database, outpoint)?
            .into_iter()
            .map(|x| x.map(|(_sat_point, inscription_id)| inscription_id))
            .try_collect()
            .track_err()
    }

    pub(crate) fn get_inscriptions(
        &self,
        n: Option<usize>,
    ) -> Result<BTreeMap<SatPoint, InscriptionId>> {
        self.database
            .iter_scan_from(SATPOINT_TO_INSCRIPTION_ID.as_bytes(), &[0u8; 44])
            .map(|dbrow| {
                Ok::<_, anyhow::Error>((
                    Entry::load(dbrow.key.try_into().track_err()?).track_err()?,
                    Entry::load(dbrow.value.try_into().track_err()?).track_err()?,
                ))
            })
            .take(n.unwrap_or(usize::MAX))
            .try_collect()
    }

    pub(crate) fn get_homepage_inscriptions(&self) -> Result<Vec<InscriptionId>> {
        self.database
            .iter_scan_reverse(INSCRIPTION_NUMBER_TO_INSCRIPTION_ID.as_bytes(), &[])
            .take(8)
            .map(|dbrow| Entry::load(dbrow.value.try_into().track_err()?).track_err())
            .try_collect()
    }

    pub(crate) fn get_feed_inscriptions(&self, n: usize) -> Result<Vec<(u64, InscriptionId)>> {
        self.database
            .iter_scan_reverse(INSCRIPTION_NUMBER_TO_INSCRIPTION_ID.as_bytes(), &[])
            .take(n)
            .map(|dbrow| {
                Ok::<_, anyhow::Error>((
                    u64::from_le_bytes(dbrow.key.try_into().track_err()?),
                    Entry::load(dbrow.value.try_into().track_err()?).track_err()?,
                ))
            })
            .try_collect()
            .track_err()
    }

    pub(crate) fn get_inscription_entry(
        &self,
        inscription_id: InscriptionId,
    ) -> Result<Option<InscriptionEntry>> {
        let Some(value) = self.database.get(&db_key!(
            INSCRIPTION_ID_TO_INSCRIPTION_ENTRY,
            &inscription_id.store().track_err()?
        )) else {
            return Ok(None);
        };

        let (part1, rest) = value.split_at(8);
        let (part2, rest) = rest.split_at(8);
        let (part3, rest) = rest.split_at(8);
        let (part4, part5) = rest.split_at(16);
        let value1 = u64::from_le_bytes(part1.try_into().anyhow_as("Incorrect length")?);
        let value2 = u64::from_le_bytes(part2.try_into().anyhow_as("Incorrect length")?);
        let value3 = u64::from_le_bytes(part3.try_into().anyhow_as("Incorrect length")?);
        let value4 = u128::from_le_bytes(part4.try_into().anyhow_as("Incorrect length")?);
        let value5 = u32::from_le_bytes(part5.try_into().anyhow_as("Incorrect length")?);
        InscriptionEntry::load((value1, value2, value3, value4, value5))
            .track_err()
            .map(Some)
    }

    pub(crate) fn inscriptions_on_output<'tx>(
        database: &'tx DB,
        outpoint: OutPoint,
    ) -> Result<impl Iterator<Item = Result<(SatPoint, InscriptionId)>> + 'tx> {
        let start = SatPoint {
            outpoint,
            offset: 0,
        }
        .store()
        .track_err()?;

        Ok(database
            .iter_scan_from(SATPOINT_TO_INSCRIPTION_ID.as_bytes(), &start)
            .map(|dbrow| {
                Ok((
                    Entry::load(dbrow.key.try_into().track_err()?).track_err()?,
                    Entry::load(dbrow.value.try_into().track_err()?).track_err()?,
                ))
            }))
    }
}
