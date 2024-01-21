use std::convert::TryInto;

use crate::{
    db_key,
    inscription_entries::{
        index::{
            ADDRESS_TO_ORD_STATS, PARTIAL_TXID_TO_TXIDS, STATISTIC_TO_COUNT, TXID_IS_INSCRIPTION,
        },
        inscription::Inscription,
        inscription::{
            HistoryAction, InscriptionExtraData, InscriptionMeta, LastInscriptionNumber,
            OrdHistoryRow, OrdHistoryValue, ParsedInscription, PartialTxs, UserOrdStats,
        },
        inscription_id::InscriptionId,
    },
    util::{bincode_util, errors::AsAnyhow},
};
use anyhow::Result;
use bitcoin::{consensus::Decodable, Address};
use bitcoin::{hashes::Hash, Transaction, Txid};
use itertools::Itertools;

use super::{DBRow, DB};

pub struct InscriptionUpdater<'a> {
    inscription_db: &'a DB,
    tx_db: &'a DB,
    temp_db: &'a DB,
}

impl<'a> InscriptionUpdater<'a> {
    pub fn new(inscription_db: &'a DB, tx_db: &'a DB, temp_db: &'a DB) -> Result<Self> {
        Ok(Self {
            inscription_db,
            tx_db,
            temp_db,
        })
    }

    pub fn index_transaction_inscriptions(
        &mut self,
        tx: &Transaction,
        txid: Txid,
        block_height: u32,
    ) -> Result<u64> {
        let previous_txid = tx.input[0].previous_output.txid;
        let tx_sat = tx.output.first().anyhow()?.value;
        let prev_tx = tx.input.first().anyhow_as("No inputs :(")?.previous_output;

        if prev_tx.vout == 0 {
            if let Some(mut inscription_extra) = self
                .inscription_db
                .remove(&db_key!(TXID_IS_INSCRIPTION, &prev_tx.txid.into_inner()))
                .map(|x| InscriptionExtraData::from_raw(&x.to_vec()))
                .transpose()?
            {
                let old_owner = inscription_extra.owner;

                // Work with old user
                let (prev_history_value, prev_stats) = {
                    let old_row = OrdHistoryRow::new(
                        old_owner.clone(),
                        inscription_extra.block_height,
                        prev_tx.txid,
                        // Value hardcoded becouse its not needed
                        OrdHistoryValue {
                            value: tx_sat,
                            inscription_id: InscriptionId {
                                txid: inscription_extra.genesis,
                                index: 0,
                            },
                            inscription_number: 0,
                        },
                    );

                    let prev_history_value = self
                        .inscription_db
                        .remove(&old_row.get_key())
                        .map(|x| OrdHistoryRow::value_from_raw(&x))
                        .anyhow_as("Failed to find OrdHistoryRow")?;

                    let prev_stats = self
                        .inscription_db
                        .remove(&db_key!(ADDRESS_TO_ORD_STATS, old_owner.as_bytes()))
                        .map(|x| UserOrdStats::from_raw(&x))
                        .transpose()?
                        .map(|mut x| {
                            x.count -= 1;
                            x.amount -= tx_sat;
                            x
                        })
                        .anyhow_as("No stats :(")?;

                    (prev_history_value, prev_stats)
                };

                // Work with new user
                let (ord_history, new_stats) = {
                    let new_owner = tx
                        .output
                        .first()
                        .and_then(|x| {
                            Address::from_script(
                                &x.script_pubkey,
                                bitcoin::network::constants::Network::Bitcoin,
                            )
                        })
                        .map(|x| x.to_string())
                        .anyhow_as("No owner :(")?;

                    inscription_extra.owner = new_owner.clone();
                    inscription_extra.block_height = block_height;

                    let mut new_stats = self
                        .inscription_db
                        .remove(&db_key!(ADDRESS_TO_ORD_STATS, new_owner.as_bytes()))
                        .map(|x| UserOrdStats::from_raw(&x))
                        .transpose()?
                        .unwrap_or_default();

                    new_stats.count += 1;
                    new_stats.amount += tx_sat;

                    (
                        OrdHistoryRow::new(new_owner, block_height, txid, prev_history_value),
                        new_stats,
                    )
                };

                self.inscription_db.write(
                    vec![
                        ord_history.into_row(),
                        inscription_extra.to_db_row(&txid)?,
                        prev_stats.to_db_row(&old_owner)?,
                        new_stats.to_db_row(&inscription_extra.owner)?,
                    ],
                    super::db::DBFlush::Disable,
                );

                return Ok(0);
            };
        }

        let partial = PartialTxs {
            block_height,
            last_txid: previous_txid,
            txs: vec![],
        };

        let txs = match self.inscription_db.get(&partial.get_db_key()) {
            Some(partial_txids) => {
                let txids = PartialTxs::from_db(DBRow {
                    key: partial.get_db_key(),
                    value: partial_txids,
                })?;

                let mut txs = vec![];
                for txid in txids.txs {
                    let tx_result = self.tx_db.get(&db_key!("T", &txid.into_inner())).anyhow()?;
                    let decoded =
                        bitcoin::Transaction::consensus_decode(std::io::Cursor::new(tx_result))?;
                    txs.push(decoded);
                }
                txs.push(tx.clone());
                txs
            }
            None => {
                vec![tx.clone()]
            }
        };

        match Inscription::from_transactions(txs.clone()) {
            ParsedInscription::None => {
                // TODO idk
            }

            ParsedInscription::Partial => {
                self.inscription_db.remove(&partial.get_db_key());
                let row = PartialTxs {
                    block_height,
                    last_txid: txid,
                    txs: txs.into_iter().map(|x| x.txid()).collect_vec(),
                }
                .to_db()?;

                self.inscription_db.put(&row.key, &row.value);
            }

            ParsedInscription::Complete(_inscription) => {
                self.temp_db.remove(&partial.get_db_key());

                let og_inscription_id = InscriptionId {
                    txid: Txid::from_slice(
                        &txs.first().anyhow_as("Partial txs vec is empty")?.txid(),
                    )
                    .track_err()?,
                    // TODO find correct index instead hardcode
                    index: 0,
                };

                let genesis = txs.first().anyhow_as("BIG COCKS")?.txid();

                let owner = tx
                    .output
                    .first()
                    .and_then(|x| {
                        Address::from_script(
                            &x.script_pubkey,
                            bitcoin::network::constants::Network::Bitcoin,
                        )
                    })
                    .map(|x| x.to_string())
                    .anyhow_as("No owner :(")?;

                let mut last_inc_n = LastInscriptionNumber {
                    height: 0,
                    number: 0,
                };

                let number: u64 = self
                    .inscription_db
                    .remove(&last_inc_n.get_db_key())
                    .map(|x| {
                        if let Ok(bytes) = x.try_into() {
                            u64::from_be_bytes(bytes)
                        } else {
                            0
                        }
                    })
                    .unwrap_or(0);

                last_inc_n.number = number;

                let inscription_meta = InscriptionMeta::new(
                    _inscription.content_type().anyhow()?.to_owned(),
                    _inscription.content_length().anyhow()?,
                    txs.last().anyhow()?.txid(),
                    og_inscription_id.txid,
                    number,
                );

                let new_row = OrdHistoryRow::new(
                    owner.clone(),
                    block_height as u32,
                    txid,
                    OrdHistoryValue {
                        inscription_id: og_inscription_id,
                        inscription_number: number,
                        value: tx_sat,
                    },
                );

                last_inc_n.number += 1;

                let mut new_stats = self
                    .inscription_db
                    .remove(&db_key!(ADDRESS_TO_ORD_STATS, owner.as_bytes()))
                    .map(|x| UserOrdStats::from_raw(&x))
                    .transpose()?
                    .unwrap_or_default();

                new_stats.count += 1;
                new_stats.amount += tx_sat;

                let inscription_extra =
                    InscriptionExtraData::new(genesis, owner.clone(), block_height);

                self.inscription_db.write(
                    vec![
                        new_row.into_row(),
                        inscription_extra.to_db_row(&txid)?,
                        inscription_meta.to_db_row()?,
                        last_inc_n.to_db()?,
                        new_stats.to_db_row(&owner)?,
                    ],
                    super::db::DBFlush::Disable,
                );
            }
        }
        Ok(0)
    }

    pub fn copy_from_main_block(&self, next_block_height: u32) -> anyhow::Result<()> {
        let p_key = PARTIAL_TXID_TO_TXIDS.as_bytes();

        let mut to_write = vec![];

        for i in self.inscription_db.iter_scan(&p_key) {
            let mut x = PartialTxs::from_db(i)?;
            x.block_height = next_block_height;
            to_write.push(x.to_temp_db_row()?);
        }

        for i in self.inscription_db.iter_scan(STATISTIC_TO_COUNT.as_bytes()) {
            let x = UserOrdStats::from_raw(&i.value)?;
            let owner = UserOrdStats::owner_from_key(i.key)?;
            to_write.push(x.to_temp_db_row(next_block_height, &owner)?);
        }

        self.inscription_db
            .write(to_write, super::db::DBFlush::Disable);

        Ok(())
    }

    pub fn clean_up_temp_db(&self, block_height: u32) -> anyhow::Result<()> {
        let p_key = bincode_util::serialize_big(&(b'P', block_height))?;

        let mut to_delete = vec![];

        for i in self.temp_db.iter_scan(&p_key) {
            to_delete.push(i.key);
        }

        let stats_key = UserOrdStats {
            amount: 0,
            count: 0,
        }
        .to_temp_db_row(block_height, "")?
        .key;

        for i in self.temp_db.iter_scan(&stats_key) {
            to_delete.push(i.key);
        }

        let history_key = bincode_util::serialize_big(&(b'H', block_height))?;

        for i in self.temp_db.iter_scan(&history_key) {
            to_delete.push(i.key);
        }

        error!("Lenth of to_delete: {}", to_delete.len());

        self.temp_db.delete_batch(to_delete);

        Ok(())
    }

    pub fn copy_to_next_block(&self, current_block_height: u32) -> anyhow::Result<()> {
        let next_block_height = current_block_height + 1;
        let p_key = bincode_util::serialize_big(&(b'P', current_block_height))?;

        let mut to_write = vec![];

        for i in self.temp_db.iter_scan(&p_key) {
            let mut x = PartialTxs::from_temp_db_row(i)?;
            x.block_height = next_block_height;
            to_write.push(x.to_temp_db_row()?);
        }

        let stats_key = UserOrdStats {
            amount: 0,
            count: 0,
        }
        .to_temp_db_row(current_block_height, "")?
        .key;

        for i in self.temp_db.iter_scan(&stats_key) {
            let (x, owner) = UserOrdStats::from_temp_db(i)?;
            to_write.push(x.to_temp_db_row(next_block_height, &owner)?);
        }

        self.temp_db.write(to_write, super::db::DBFlush::Disable);

        Ok(())
    }

    pub fn index_temp_inscriptions(
        &mut self,
        tx: &Transaction,
        txid: Txid,
        block_height: u32,
    ) -> Result<u64> {
        let prev_tx = tx.input.first().anyhow_as("No inputs :(")?;
        let partial_prev_key = &PartialTxs {
            block_height,
            last_txid: prev_tx.previous_output.txid,
            txs: vec![],
        }
        .get_db_key();
        let tx_sat = tx.output.first().anyhow()?.value;
        if prev_tx.previous_output.vout == 0 {
            if let Some(mut inscription_extra) = self
                .temp_db
                .remove(&db_key!(
                    TXID_IS_INSCRIPTION,
                    &prev_tx.previous_output.txid.into_inner()
                ))
                .map(|x| InscriptionExtraData::from_raw(&x.to_vec()))
                .transpose()?
            {
                let old_owner = inscription_extra.owner;

                // Work with old user
                let (prev_history_value, prev_stats) = {
                    let old_row = OrdHistoryRow::new(
                        old_owner.clone(),
                        inscription_extra.block_height,
                        prev_tx.previous_output.txid,
                        // Value hardcoded becouse its not needed
                        OrdHistoryValue {
                            value: tx_sat,
                            inscription_id: InscriptionId {
                                txid: inscription_extra.genesis,
                                index: 0,
                            },
                            inscription_number: 0,
                        },
                    );

                    let prev_history_value = self
                        .temp_db
                        .remove(&old_row.get_key())
                        .map(|x| OrdHistoryRow::value_from_raw(&x))
                        .anyhow_as("Failed to find OrdHistoryRow")?;

                    let prev_stats = self
                        .temp_db
                        .remove(&db_key!(ADDRESS_TO_ORD_STATS, old_owner.as_bytes()))
                        .map(|x| UserOrdStats::from_raw(&x))
                        .transpose()?
                        .map(|mut x| {
                            x.count -= 1;
                            x.amount -= tx_sat;
                            x
                        })
                        .anyhow_as("No stats :(")?;

                    (prev_history_value, prev_stats)
                };

                // Work with new user
                let (ord_history, new_stats) = {
                    let new_owner = tx
                        .output
                        .first()
                        .and_then(|x| {
                            Address::from_script(
                                &x.script_pubkey,
                                bitcoin::network::constants::Network::Bitcoin,
                            )
                        })
                        .map(|x| x.to_string())
                        .anyhow_as("No owner :(")?;

                    inscription_extra.owner = new_owner.clone();
                    inscription_extra.block_height = block_height;

                    let mut new_stats = self
                        .temp_db
                        .remove(&db_key!(ADDRESS_TO_ORD_STATS, new_owner.as_bytes()))
                        .map(|x| UserOrdStats::from_raw(&x))
                        .transpose()?
                        .unwrap_or_default();

                    new_stats.count += 1;
                    new_stats.amount += tx_sat;

                    (
                        OrdHistoryRow::new(new_owner, block_height, txid, prev_history_value),
                        new_stats,
                    )
                };

                self.temp_db.write(
                    vec![
                        ord_history.into_row(),
                        inscription_extra.to_db_row(&txid)?,
                        prev_stats.to_db_row(&old_owner)?,
                        new_stats.to_db_row(&inscription_extra.owner)?,
                    ],
                    super::db::DBFlush::Disable,
                );

                return Ok(0);
            };
        }

        let txs = match self.temp_db.get(partial_prev_key) {
            Some(partial_txids_raw) => {
                let txids = PartialTxs::from_temp_db_row(DBRow {
                    key: vec![],
                    value: partial_txids_raw,
                })?;
                let mut txs = vec![];
                for txid in txids.txs {
                    let tx_result = self.tx_db.get(&db_key!("T", &txid.into_inner())).anyhow()?;
                    let decoded =
                        bitcoin::Transaction::consensus_decode(std::io::Cursor::new(tx_result))?;
                    txs.push(decoded);
                }
                txs.push(tx.clone());
                txs
            }
            None => {
                vec![tx.clone()]
            }
        };

        match Inscription::from_transactions(txs.clone()) {
            ParsedInscription::None => {
                // TODO idk
            }

            ParsedInscription::Partial => {
                self.temp_db.remove(&partial_prev_key);
                let row = PartialTxs {
                    block_height,
                    last_txid: txid,
                    txs: txs.into_iter().map(|x| x.txid()).collect_vec(),
                }
                .to_temp_db_row()?;

                self.temp_db.put(&row.key, &row.value);
            }

            ParsedInscription::Complete(_inscription) => {
                self.temp_db.remove(&partial_prev_key);
                let genesis = txs.first().anyhow_as("BIG COCKS")?.txid();

                let og_inscription_id = InscriptionId {
                    // TODO find correct index instead hardcode
                    index: 0,
                    txid: genesis,
                };

                let owner = tx
                    .output
                    .first()
                    .and_then(|x| {
                        Address::from_script(
                            &x.script_pubkey,
                            bitcoin::network::constants::Network::Bitcoin,
                        )
                    })
                    .map(|x| x.to_string())
                    .anyhow_as("No owner :(")?;

                let mut last_inc_n = LastInscriptionNumber {
                    height: block_height,
                    number: 0,
                };

                let number = self
                    .temp_db
                    .remove(&last_inc_n.get_temp_db_key())
                    .map(|x| {
                        if let Ok(bytes) = x.try_into() {
                            u64::from_be_bytes(bytes)
                        } else {
                            0
                        }
                    })
                    .unwrap_or(0);
                last_inc_n.number = number;

                let inscription_meta = InscriptionMeta::new(
                    _inscription.content_type().anyhow()?.to_owned(),
                    _inscription.content_length().anyhow()?,
                    txid,
                    og_inscription_id.txid,
                    number,
                );

                let new_row = OrdHistoryRow::new(
                    owner.clone(),
                    block_height,
                    txid,
                    OrdHistoryValue {
                        inscription_id: og_inscription_id,
                        inscription_number: number,
                        value: tx_sat,
                    },
                );

                last_inc_n.number += 1;

                let stats = UserOrdStats {
                    amount: 0,
                    count: 0,
                };
                let stats_db_raw = stats.to_temp_db_row(block_height, &owner)?;

                let (mut new_stats, _) = self
                    .temp_db
                    .remove(&stats_db_raw.key)
                    .map(|x| {
                        UserOrdStats::from_temp_db(DBRow {
                            key: stats_db_raw.key,
                            value: x,
                        })
                    })
                    .transpose()?
                    .unwrap_or_default();

                new_stats.count += 1;
                new_stats.amount += tx_sat;

                let inscription_extra =
                    InscriptionExtraData::new(genesis, owner.clone(), block_height);

                self.temp_db.write(
                    vec![
                        new_row.to_temp_db_row(block_height, HistoryAction::Put)?,
                        inscription_extra.to_temp_db_row(
                            txid.clone(),
                            block_height,
                            HistoryAction::Put,
                        )?,
                        inscription_meta.to_temp_db_row(
                            block_height,
                            owner.clone(),
                            txid,
                            HistoryAction::Put,
                        )?,
                        last_inc_n.to_temp_db_row()?,
                        new_stats.to_temp_db_row(block_height, &owner)?,
                    ],
                    super::db::DBFlush::Disable,
                );
            }
        }
        Ok(0)
    }
}
