use std::{collections::HashMap, convert::TryInto};

use crate::{
    db_key,
    inscription_entries::{
        index::{ADDRESS_TO_ORD_STATS, PARTIAL_TXID_TO_TXIDS, TXID_IS_INSCRIPTION},
        inscription::Inscription,
        inscription::{
            InscriptionExtraData, InscriptionMeta, LastInscriptionNumber, OrdHistoryKey,
            OrdHistoryRow, OrdHistoryValue, ParsedInscription, PartialTxs, UserOrdStats,
        },
        inscription_id::InscriptionId,
    },
    util::{bincode_util, errors::AsAnyhow, full_hash, HeaderEntry, ScriptToAddr},
};
use anyhow::{Ok, Result};
use bitcoin::{consensus::Decodable, Address};
use bitcoin::{hashes::Hash, Transaction, Txid};
use itertools::Itertools;

use super::{schema::BlockRow, DBRow, DB};
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
        tx: Transaction,
        block_height: u32,
        is_temp: bool,
        mut cache: Option<&mut HashMap<Txid, Transaction>>,
        mut partials_cache: Option<&mut HashMap<Txid, Vec<Txid>>>,
    ) -> Result<u64> {
        let txid = tx.txid();
        let previous_txid = tx.input[0].previous_output.txid;
        let tx_sat = tx.output.first().anyhow()?.value;
        let prev_tx = tx.input.first().anyhow_as("No inputs :(")?.previous_output;

        let mut to_temp_write = vec![];

        if prev_tx.vout == 0 {
            if let Some(mut inscription_extra) = self
                .inscription_db
                .remove(&db_key!(TXID_IS_INSCRIPTION, &prev_tx.txid.into_inner()))
                .map(|x| InscriptionExtraData::from_raw(&x.to_vec()))
                .transpose()?
            {
                let old_owner = inscription_extra.owner.clone();

                // Work with old user
                let prev_history_value = {
                    let old_row = OrdHistoryRow::new(
                        old_owner.clone(),
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

                    if is_temp {
                        to_temp_write.push(DBRow {
                            key: old_row.get_temp_db_key(block_height),
                            value: prev_history_value.get_raw(),
                        });
                        to_temp_write
                            .push(inscription_extra.to_temp_db_row(block_height, &previous_txid)?);
                    }

                    prev_history_value
                };

                // Work with new user
                let ord_history = {
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

                    OrdHistoryRow::new(new_owner, txid, prev_history_value)
                };

                if is_temp {
                    self.temp_db
                        .write(to_temp_write, super::db::DBFlush::Disable);
                }

                self.inscription_db.write(
                    vec![ord_history.into_row(), inscription_extra.to_db_row(&txid)?],
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

        let txs = {
            let txsids = {
                if let Some(v) = partials_cache
                    .as_mut()
                    .map(|x| x.get(&previous_txid))
                    .flatten()
                {
                    v.clone()
                } else {
                    if !is_temp {
                        vec![]
                    } else {
                        match self.inscription_db.get(&partial.get_db_key()) {
                            None => vec![txid],
                            Some(partials) => {
                                PartialTxs::from_db(DBRow {
                                    key: partial.get_db_key(),
                                    value: partials,
                                })
                                .unwrap()
                                .txs
                            }
                        }
                    }
                }
            };

            let mut txs = vec![];
            for txid in txsids {
                if let Some(v) = cache.as_ref().map(|x| x.get(&txid)).flatten() {
                    txs.push(v.clone());
                } else if is_temp {
                    let tx_result = self.tx_db.get(&db_key!("T", &txid.into_inner())).anyhow()?;
                    let decoded =
                        bitcoin::Transaction::consensus_decode(std::io::Cursor::new(tx_result))?;
                    txs.push(decoded);
                }
            }
            txs.push(tx.clone());
            txs
        };

        match Inscription::from_transactions(txs.clone()) {
            ParsedInscription::None => {
                if let Some(v) = cache.as_mut() {
                    v.remove(&txid).unwrap();
                }
            }

            ParsedInscription::Partial => {
                if let Some(_) = partials_cache
                    .as_mut()
                    .map(|x| x.remove(&previous_txid))
                    .flatten()
                {
                    partials_cache
                        .unwrap()
                        .insert(txid, txs.iter().map(|x| x.txid()).collect_vec());
                } else if is_temp {
                    self.inscription_db.remove(&partial.get_db_key());

                    let row = PartialTxs {
                        block_height,
                        last_txid: txid,
                        txs: txs.into_iter().map(|x| x.txid()).collect_vec(),
                    };

                    self.inscription_db
                        .write(vec![row.to_db()?], super::db::DBFlush::Disable);

                    if is_temp {
                        self.temp_db.remove(&PartialTxs::get_temp_db_key(
                            block_height,
                            &partial.last_txid,
                        ));
                        self.temp_db
                            .write(vec![row.to_temp_db_row()?], super::db::DBFlush::Disable);
                    }
                }
            }

            ParsedInscription::Complete(_inscription) => {
                if let Some(partials_cache) = partials_cache.as_mut() {
                    partials_cache.remove(&previous_txid);
                } else if is_temp {
                    self.inscription_db.remove(&partial.get_db_key());
                }

                let og_inscription_id = InscriptionId {
                    txid: Txid::from_slice(
                        &txs.first().anyhow_as("Partial txs vec is empty")?.txid(),
                    )
                    .anyhow()?,
                    // TODO find correct index instead hardcode
                    index: 0,
                };

                let genesis = txs[0].txid();

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

                let number: u64 = self
                    .inscription_db
                    .remove(&LastInscriptionNumber::get_db_key())
                    .map(|x| u64::from_be_bytes(x.try_into().expect("Failed to convert")))
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
                    txid,
                    OrdHistoryValue {
                        inscription_id: og_inscription_id,
                        inscription_number: number,
                        value: tx_sat,
                    },
                );

                let new_inc_n = LastInscriptionNumber {
                    height: block_height,
                    number: number + 1,
                };

                let inscription_extra =
                    InscriptionExtraData::new(genesis, owner.clone(), block_height);

                self.inscription_db.write(
                    vec![
                        new_row.into_row(),
                        inscription_extra.to_db_row(&txid)?,
                        inscription_meta.to_db_row()?,
                        new_inc_n.to_db()?,
                    ],
                    super::db::DBFlush::Disable,
                );

                if is_temp {
                    self.temp_db.remove(&PartialTxs::get_temp_db_key(
                        block_height,
                        &partial.last_txid,
                    ));
                    self.temp_db.write(
                        vec![new_inc_n.to_temp_db_row()?],
                        super::db::DBFlush::Disable,
                    );
                } else {
                    for i in txs.into_iter().rev() {
                        cache.as_mut().unwrap().remove(&i.txid());
                    }
                }
            }
        }
        Ok(0)
    }

    pub fn copy_from_main_block(&self, next_block_height: u32) -> anyhow::Result<()> {
        let mut to_write = vec![];

        for i in self
            .inscription_db
            .iter_scan(PARTIAL_TXID_TO_TXIDS.as_bytes())
        {
            let mut x = PartialTxs::from_db(i)?;
            x.block_height = next_block_height;
            to_write.push(x.to_temp_db_row()?);
        }

        for i in self
            .inscription_db
            .iter_scan(ADDRESS_TO_ORD_STATS.as_bytes())
        {
            let x = UserOrdStats::from_raw(&i.value)?;
            let owner = UserOrdStats::owner_from_key(i.key)?;
            to_write.push(x.to_temp_db_row(next_block_height, &owner)?);
        }

        let mut last_number = self
            .inscription_db
            .get(&LastInscriptionNumber::get_db_key())
            .map(|x| {
                LastInscriptionNumber::from_db(DBRow {
                    key: vec![],
                    value: x,
                })
            })
            .unwrap()
            .anyhow_as("Failed to decode last inscription number")?;

        last_number.height = next_block_height;

        to_write.push(last_number.to_temp_db_row()?);

        self.temp_db.write(to_write, super::db::DBFlush::Disable);

        Ok(())
    }

    pub fn remove_blocks(&self, blocks: Vec<HeaderEntry>) -> anyhow::Result<()> {
        let mut to_restore = vec![];

        let min_height = blocks[0].height() as u32 - 1;

        let last_inscription_number_key = LastInscriptionNumber::get_temp_db_key(min_height);
        let last_number = self
            .temp_db
            .get(&last_inscription_number_key)
            .map(|x| {
                LastInscriptionNumber::from_db(DBRow {
                    key: last_inscription_number_key,
                    value: x,
                })
                .unwrap()
            })
            .unwrap();

        to_restore.push(last_number.to_db()?);

        for block in blocks.into_iter().rev() {
            let block_height = block.height() as u32;
            self.remove_temp_data_orhpan(block_height)?;

            let txids: Vec<Txid> = {
                self.tx_db
                    .get(&BlockRow::txids_key(full_hash(&block.hash()[..])))
                    .map(|val| {
                        bincode_util::deserialize_little(&val).expect("failed to parse block txids")
                    })
                    .unwrap()
            };

            for tx in txids.into_iter().rev() {
                let temp_extra_key = InscriptionExtraData::get_temp_db_key(block_height, &tx);
                let extra_key = InscriptionExtraData::get_db_key(&tx);

                let tx_result = self.tx_db.get(&db_key!("T", &tx.into_inner())).anyhow()?;
                let decoded =
                    bitcoin::Transaction::consensus_decode(std::io::Cursor::new(tx_result))?;

                let history_row = OrdHistoryKey {
                    address: decoded.output[0]
                        .script_pubkey
                        .to_address_str(crate::chain::Network::Bellscoin)
                        .expect("SHIT"),
                    code: OrdHistoryRow::CODE,
                    txid: tx,
                };

                let history_row = OrdHistoryRow {
                    key: history_row,
                    value: OrdHistoryValue {
                        inscription_id: InscriptionId { index: 0, txid: tx },
                        inscription_number: 0,
                        value: 0,
                    },
                };
                let temp_history_key = history_row.get_temp_db_key(block_height);
                let history_key = history_row.get_key();

                let meta_key = InscriptionMeta::get_db_key(tx)?;

                if let Some(v) = self.temp_db.remove(&temp_extra_key).map(|x| {
                    InscriptionExtraData::from_temp_db(DBRow {
                        key: temp_extra_key,
                        value: x,
                    })
                    .unwrap()
                }) {
                    to_restore.push(v.0.to_db_row(&v.1)?);
                } else if let Some(v) = self.temp_db.remove(&temp_history_key).map(|x| {
                    OrdHistoryRow::from_temp_db_row(DBRow {
                        key: temp_history_key,
                        value: x,
                    })
                    .unwrap()
                }) {
                    to_restore.push(v.0.into_row());
                } else if let Some(_) = self.inscription_db.remove(&meta_key).map(|x| {
                    InscriptionMeta::from_raw(&x).expect("Failed to decode InscriptionMeta")
                }) {
                } else if let Some(_) = self.inscription_db.remove(&extra_key) {
                } else if let Some(_) = self.inscription_db.remove(&history_key) {
                }
            }
        }

        self.inscription_db
            .write(to_restore, super::db::DBFlush::Disable);

        Ok(())
    }

    pub fn remove_temp_data_orhpan(&self, block_height: u32) -> anyhow::Result<()> {
        let mut to_delete = vec![];

        for i in self
            .temp_db
            .iter_scan(&PartialTxs::get_temp_iter_key(block_height))
        {
            to_delete.push(i.key);
        }

        for i in self
            .temp_db
            .iter_scan(&UserOrdStats::get_temp_iter_key(block_height))
        {
            to_delete.push(i.key);
        }

        to_delete.push(LastInscriptionNumber::get_temp_db_key(block_height));

        self.temp_db.delete_batch(to_delete);

        Ok(())
    }

    pub fn copy_to_next_block(&self, current_block_height: u32) -> anyhow::Result<()> {
        let next_block_height = current_block_height + 1;
        let mut to_write = vec![];

        for i in self
            .temp_db
            .iter_scan(&PartialTxs::get_temp_iter_key(current_block_height))
        {
            let mut x = PartialTxs::from_temp_db(i)?;
            x.block_height = next_block_height;
            to_write.push(x.to_temp_db_row()?);
        }

        for i in self
            .temp_db
            .iter_scan(&UserOrdStats::get_temp_iter_key(current_block_height))
        {
            let (x, owner) = UserOrdStats::from_temp_db(i)?;
            to_write.push(x.to_temp_db_row(next_block_height, &owner)?);
        }

        self.temp_db.write(to_write, super::db::DBFlush::Disable);

        Ok(())
    }
}
