use std::convert::TryInto;

use crate::{
    db_key,
    inscription_entries::{
        entry::Entry,
        index::{
            ADDRESS_TO_ORD_STATS, INSCRIPTION_ID_TO_TXIDS, LAST_INSCRIPTION_NUMBER,
            PARTIAL_TXID_TO_TXIDS, TXID_IS_INSCRIPTION,
        },
        inscription::Inscription,
        inscription::{InscriptionExtraData, InscriptionMeta, ParsedInscription, UserOrdStats},
        inscription_id::InscriptionId,
    },
    util::errors::AsAnyhow,
};
use anyhow::Result;
use bitcoin::{consensus::Decodable, Address};
use bitcoin::{hashes::Hash, Transaction, Txid};

use super::{
    schema::{OrdHistoryRow, OrdHistoryValue},
    DBRow, DB,
};

pub(crate) struct InscriptionUpdater<'a> {
    inscription_db: &'a DB,
    tx_db: &'a DB,
}

impl<'a> InscriptionUpdater<'a> {
    pub(crate) fn new(inscription_db: &'a DB, tx_db: &'a DB) -> Result<Self> {
        Ok(Self {
            inscription_db,
            tx_db,
        })
    }

    pub(crate) fn index_transaction_inscriptions(
        &mut self,
        tx: &Transaction,
        txid: Txid,
        block_height: u32,
    ) -> Result<u64> {
        let previous_txid = tx.input[0].previous_output.txid;
        let previous_txid_bytes: [u8; 32] = previous_txid.into_inner();
        let tx_sat = tx.output.first().anyhow()?.value;
        let mut txids_vec = vec![];
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

        let txs = match self
            .inscription_db
            .get(&db_key!(PARTIAL_TXID_TO_TXIDS, &previous_txid_bytes))
        {
            Some(partial_txids) => {
                let txids = partial_txids;
                let mut txs = vec![];
                txids_vec = txids.to_vec();
                for i in 0..txids.len() / 32 {
                    let txid = &txids[i * 32..i * 32 + 32];

                    let tx_result = self.tx_db.get(&db_key!("T", txid)).anyhow()?;
                    let tx_buf = tx_result;
                    let mut cursor = std::io::Cursor::new(tx_buf);
                    let tx = bitcoin::Transaction::consensus_decode(&mut cursor)?;

                    txs.push(tx);
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
                let mut txid_vec = txid.into_inner().to_vec();
                txids_vec.append(&mut txid_vec);

                self.inscription_db
                    .remove(&db_key!(PARTIAL_TXID_TO_TXIDS, &previous_txid_bytes));

                self.inscription_db.put(
                    &db_key!(PARTIAL_TXID_TO_TXIDS, &txid.into_inner()),
                    txids_vec.as_slice(),
                );
            }

            ParsedInscription::Complete(_inscription) => {
                self.inscription_db
                    .remove(&db_key!(PARTIAL_TXID_TO_TXIDS, &previous_txid_bytes));

                let mut txid_vec = txid.into_inner().to_vec();
                txids_vec.append(&mut txid_vec);

                let og_inscription_id = InscriptionId {
                    txid: Txid::from_slice(&txids_vec[0..32]).track_err()?,
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

                let mut number: usize = self
                    .inscription_db
                    .remove(&LAST_INSCRIPTION_NUMBER.as_bytes())
                    .map(|x| {
                        if let Ok(bytes) = x.try_into() {
                            usize::from_be_bytes(bytes)
                        } else {
                            0
                        }
                    })
                    .unwrap_or(0);

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

                number += 1;

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
                        DBRow {
                            key: db_key!(
                                INSCRIPTION_ID_TO_TXIDS,
                                &og_inscription_id.store().anyhow()?
                            ),
                            value: txids_vec,
                        },
                        DBRow {
                            key: LAST_INSCRIPTION_NUMBER.as_bytes().to_vec(),
                            value: number.to_be_bytes().to_vec(),
                        },
                        new_stats.to_db_row(&owner)?,
                    ],
                    super::db::DBFlush::Disable,
                );
            }
        }
        Ok(0)
    }

    pub(crate) fn index_temporary_inscriptions(
        &mut self,
        tx: &Transaction,
        txid: Txid,
        block_height: u32,
    ) -> Result<u64> {
        let previous_txid = tx.input[0].previous_output.txid;
        let previous_txid_bytes: [u8; 32] = previous_txid.into_inner();
        let tx_sat = tx.output.first().anyhow()?.value;
        let mut txids_vec = vec![];
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

        let txs = match self
            .inscription_db
            .get(&db_key!(PARTIAL_TXID_TO_TXIDS, &previous_txid_bytes))
        {
            Some(partial_txids) => {
                let txids = partial_txids;
                let mut txs = vec![];
                txids_vec = txids.to_vec();
                for i in 0..txids.len() / 32 {
                    let txid = &txids[i * 32..i * 32 + 32];

                    let tx_result = self.tx_db.get(&db_key!("T", txid)).anyhow()?;
                    let tx_buf = tx_result;
                    let mut cursor = std::io::Cursor::new(tx_buf);
                    let tx = bitcoin::Transaction::consensus_decode(&mut cursor)?;

                    txs.push(tx);
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
                let mut txid_vec = txid.into_inner().to_vec();
                txids_vec.append(&mut txid_vec);

                self.inscription_db
                    .remove(&db_key!(PARTIAL_TXID_TO_TXIDS, &previous_txid_bytes));

                self.inscription_db.put(
                    &db_key!(PARTIAL_TXID_TO_TXIDS, &txid.into_inner()),
                    txids_vec.as_slice(),
                );
            }

            ParsedInscription::Complete(_inscription) => {
                self.inscription_db
                    .remove(&db_key!(PARTIAL_TXID_TO_TXIDS, &previous_txid_bytes));

                let mut txid_vec = txid.into_inner().to_vec();
                txids_vec.append(&mut txid_vec);

                let og_inscription_id = InscriptionId {
                    txid: Txid::from_slice(&txids_vec[0..32]).track_err()?,
                    // TODO find correct index instead hardcode
                    index: 0,
                };

                let genesis = txs.first().anyhow_as("Genesis not founded")?.txid();

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

                let mut number: usize = self
                    .inscription_db
                    .remove(&LAST_INSCRIPTION_NUMBER.as_bytes())
                    .map(|x| {
                        if let Ok(bytes) = x.try_into() {
                            usize::from_be_bytes(bytes)
                        } else {
                            0
                        }
                    })
                    .unwrap_or(0);

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

                number += 1;

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
                        DBRow {
                            key: db_key!(
                                INSCRIPTION_ID_TO_TXIDS,
                                &og_inscription_id.store().anyhow()?
                            ),
                            value: txids_vec,
                        },
                        DBRow {
                            key: LAST_INSCRIPTION_NUMBER.as_bytes().to_vec(),
                            value: number.to_be_bytes().to_vec(),
                        },
                        new_stats.to_db_row(&owner)?,
                    ],
                    super::db::DBFlush::Disable,
                );
            }
        }
        Ok(0)
    }
}
