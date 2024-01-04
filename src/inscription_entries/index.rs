use crate::{config::Config, new_index::DB};

use super::*;

use core::slice::SlicePattern;
use std::{
    collections::BTreeMap,
    io::Cursor,
    path::{Path, PathBuf},
};

use self::entry::{Entry, InscriptionEntry};

const SCHEMA_VERSION: u64 = 3;

pub(crate) struct Index {
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
        const $name: String = stringify!($short_name).to_owned();
    };
}

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

impl Index {
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
        Ok(
            Self::inscriptions_on_output(SHIT.open_table(SATPOINT_TO_INSCRIPTION_ID)?, outpoint)?
                .into_iter()
                .map(|(_satpoint, inscription_id)| inscription_id)
                .collect(),
        )
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
                    Entry::load(convert_vec_to_array(dbrow.key).unwrap()),
                    Entry::load(dbrow.value),
                )
            })
            .take(n.unwrap_or(usize::MAX))
            .collect())
    }

    pub(crate) fn get_homepage_inscriptions(&self) -> Result<Vec<InscriptionId>> {
        Ok(self
            .database
            .begin_read()?
            .open_table(INSCRIPTION_NUMBER_TO_INSCRIPTION_ID)?
            .iter()?
            .rev()
            .take(8)
            .map(|(_number, id)| Entry::load(*id.value()))
            .collect())
    }

    pub(crate) fn get_latest_inscriptions_with_prev_and_next(
        &self,
        n: usize,
        from: Option<u64>,
    ) -> Result<(Vec<InscriptionId>, Option<u64>, Option<u64>)> {
        let rtx = self.database.begin_read()?;

        let inscription_number_to_inscription_id =
            rtx.open_table(INSCRIPTION_NUMBER_TO_INSCRIPTION_ID)?;

        let latest = match inscription_number_to_inscription_id.iter()?.rev().next() {
            Some((number, _id)) => number.value(),
            None => return Ok(Default::default()),
        };

        let from = from.unwrap_or(latest);

        let prev = if let Some(prev) = from.checked_sub(n.try_into()?) {
            inscription_number_to_inscription_id
                .get(&prev)?
                .map(|_| prev)
        } else {
            None
        };

        let next = if from < latest {
            Some(
                from.checked_add(n.try_into()?)
                    .unwrap_or(latest)
                    .min(latest),
            )
        } else {
            None
        };

        let inscriptions = inscription_number_to_inscription_id
            .range(..=from)?
            .rev()
            .take(n)
            .map(|(_number, id)| Entry::load(*id.value()))
            .collect();

        Ok((inscriptions, prev, next))
    }

    pub(crate) fn get_feed_inscriptions(&self, n: usize) -> Result<Vec<(u64, InscriptionId)>> {
        Ok(self
            .database
            .begin_read()?
            .open_table(INSCRIPTION_NUMBER_TO_INSCRIPTION_ID)?
            .iter()?
            .rev()
            .take(n)
            .map(|(number, id)| (number.value(), Entry::load(*id.value())))
            .collect())
    }

    pub(crate) fn get_inscription_entry(
        &self,
        inscription_id: InscriptionId,
    ) -> Result<Option<InscriptionEntry>> {
        Ok(self
            .database
            .begin_read()?
            .open_table(INSCRIPTION_ID_TO_INSCRIPTION_ENTRY)?
            .get(&inscription_id.store())?
            .map(|value| InscriptionEntry::load(value.value())))
    }

    pub(crate) fn inscriptions_on_output<'tx>(
        satpoint_to_id: DB,
        outpoint: OutPoint,
    ) -> Result<impl Iterator<Item = (SatPoint, InscriptionId)> + 'tx> {
        let start = SatPoint {
            outpoint,
            offset: 0,
        }
        .store();

        let end = SatPoint {
            outpoint,
            offset: u64::MAX,
        }
        .store();

        Ok(satpoint_to_id
            .range::<&[u8; 44]>(&start..=&end)?
            .map(|(satpoint, id)| (Entry::load(*satpoint.value()), Entry::load(*id.value()))))
    }
}

fn convert_vec_to_array(slice: Vec<u8>, elements_count: usize) -> Result<[u8; 44], &'static str> {
    if slice.len() == 44 {
        let mut array = [0u8; 44];
        array.copy_from_slice(slice.as_slice());
        Ok(array)
    } else {
        Err("Slice is not exactly 44 elements long")
    }
}
