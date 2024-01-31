use std::{collections::HashMap, convert::TryInto};

use crate::{
    inscription_entries::{
        index::PARTIAL_TXID_TO_TXIDS,
        inscription::Inscription,
        inscription::{
            InscriptionExtraData, LastInscriptionNumber, OrdHistoryRow, OrdHistoryValue,
            ParsedInscription, PartialTxs,
        },
        InscriptionId,
    },
    new_index::schema::TxOutRow,
    util::{bincode_util, errors::AsAnyhow, full_hash, HeaderEntry, ScriptToAddr},
};
use anyhow::{Ok, Result};
use bitcoin::{consensus::Decodable, hashes::Hash, Address, BlockHash, OutPoint, Script, TxOut};
use bitcoin::{Transaction, Txid};
use itertools::Itertools;
use rayon::iter::{
    IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator, ParallelIterator,
};

use super::{
    schema::{BlockRow, TxRow},
    DBRow, Store, DB,
};
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
        &self,
        tx: Transaction,
        block_height: u32,
        txos: &HashMap<OutPoint, u64>,
    ) -> Result<u64> {
        let txid = tx.txid();

        for (idx, input) in tx.input.iter().enumerate() {
            let previous_tx = input.previous_output;
            let previous_txid = previous_tx.txid;

            let prev_outpoint = OutPoint {
                txid: previous_txid,
                vout: previous_tx.vout,
            };

            if let Some(mut inscription_extra) = self
                .inscription_db
                .remove(&InscriptionExtraData::get_db_key(prev_outpoint))
                .map(|x| {
                    InscriptionExtraData::from_raw(DBRow {
                        key: InscriptionExtraData::get_db_key(prev_outpoint),
                        value: x,
                    })
                })
                .transpose()?
            {
                let old_owner = inscription_extra.value.owner.clone();
                let mut to_write = vec![];

                to_write.push(inscription_extra.to_temp_db_row(block_height, &previous_tx)?);

                let inputs_cum = InscriptionSearcher::calc_offsets(&tx, &txos);

                let Result::Ok((vout, offset)) = InscriptionSearcher::get_output_index_by_input(
                    inputs_cum
                        .get(idx)
                        .copied()
                        .map(|x| x + inscription_extra.value.offset),
                    &tx.output,
                ) else {
                    inscription_extra.value.owner = "leaked 😭".to_owned();
                    self.inscription_db.write(
                        vec![inscription_extra.to_db_row()?],
                        crate::new_index::db::DBFlush::Disable,
                    );
                    // info!(
                    //     "Leaked inscription: {:?} - offset: {}",
                    //     tx.txid(),
                    //     inscription_extra.value.offset
                    // );
                    continue;
                };

                // Work with old user
                let prev_history_value = {
                    let prev_history_value = self
                        .inscription_db
                        .remove(&OrdHistoryRow::create_db_key(
                            old_owner.clone(),
                            &prev_outpoint,
                        ))
                        .map(|x| OrdHistoryRow::value_from_raw(&x))
                        .anyhow_as("Failed to find OrdHistoryRow")?;

                    to_write.push(DBRow {
                        key: OrdHistoryRow::get_temp_db_key(
                            old_owner.clone(),
                            &prev_outpoint,
                            block_height,
                        ),
                        value: prev_history_value.get_raw(),
                    });

                    prev_history_value
                };

                let new_outpoint = OutPoint { txid, vout };

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

                    inscription_extra.value.owner = new_owner.clone();

                    OrdHistoryRow::new(new_owner, new_outpoint, prev_history_value)
                };

                inscription_extra.location = new_outpoint;
                inscription_extra.value.offset = offset;

                self.temp_db.write(to_write, super::db::DBFlush::Disable);

                self.inscription_db.write(
                    vec![ord_history.into_row(), inscription_extra.to_db_row()?],
                    super::db::DBFlush::Disable,
                );

                return Ok(0);
            };

            let partial_key = PartialTxs::get_temp_db_key(block_height, &previous_txid);

            let txs = {
                let txsids = {
                    match self.temp_db.remove(&partial_key) {
                        None => vec![txid],
                        Some(partials) => {
                            PartialTxs::from_db(DBRow {
                                key: partial_key.clone(),
                                value: partials,
                            })
                            .unwrap()
                            .txs
                        }
                    }
                };

                let key = txsids
                    .into_iter()
                    .map(|x| TxRow::key(&x.into_inner()))
                    .collect_vec();

                let mut txs = self
                    .tx_db
                    .db
                    .multi_get(key)
                    .into_iter()
                    .flatten()
                    .flatten()
                    .map(|x| {
                        bitcoin::Transaction::consensus_decode(std::io::Cursor::new(&x))
                            .expect("failed to parse Transaction")
                    })
                    .collect_vec();

                txs.push(tx.clone());
                txs
            };

            match Inscription::from_transactions(txs.iter().collect_vec().as_slice()) {
                ParsedInscription::None => {}

                ParsedInscription::Partial => {
                    let row = PartialTxs {
                        block_height,
                        last_txid: txid,
                        txs: txs.into_iter().map(|x| x.txid()).collect_vec(),
                    };

                    self.temp_db
                        .write(vec![row.to_temp_db_row()?], super::db::DBFlush::Disable);
                }

                ParsedInscription::Complete(inscription) => {
                    let og_inscription_id = InscriptionId {
                        txid: Txid::from_slice(
                            &txs.first().anyhow_as("Partial txs vec is empty")?.txid(),
                        )
                        .anyhow()?,
                        index: 0,
                    };

                    let location = OutPoint { txid, vout: 0 };

                    let genesis = OutPoint {
                        txid: og_inscription_id.txid,
                        vout: og_inscription_id.index,
                    };

                    let owner = tx
                        .output
                        .first()
                        .and_then(|x| {
                            x.script_pubkey
                                .to_address_str(crate::chain::Network::Bellscoin)
                        })
                        .anyhow_as("No owner :(")?;

                    let inscription_number: u64 = self
                        .temp_db
                        .remove(&LastInscriptionNumber::get_db_key())
                        .map(|x| u64::from_be_bytes(x.try_into().expect("Failed to convert")))
                        .unwrap_or(0);

                    let new_row = OrdHistoryRow::new(
                        owner.clone(),
                        location,
                        OrdHistoryValue {
                            inscription_id: og_inscription_id,
                            inscription_number,
                            value: tx.output[0].value,
                        },
                    );

                    let new_inc_n = LastInscriptionNumber {
                        height: block_height,
                        number: inscription_number + 1,
                    };

                    let inscription_extra = InscriptionExtraData::new(
                        location,
                        genesis,
                        owner.clone(),
                        block_height,
                        inscription.content_type().unwrap().to_string(),
                        inscription.content_length().unwrap(),
                        inscription_number,
                        0,
                    );

                    self.inscription_db.write(
                        vec![
                            new_row.into_row(),
                            inscription_extra.to_db_row()?,
                            new_inc_n.to_db()?,
                        ],
                        super::db::DBFlush::Disable,
                    );

                    self.temp_db.remove(&partial_key);
                    self.temp_db.write(
                        vec![new_inc_n.to_temp_db_row()?],
                        super::db::DBFlush::Disable,
                    );
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

        // for i in self
        //     .inscription_db
        //     .iter_scan(ADDRESS_TO_ORD_STATS.as_bytes())
        // {
        //     let x = UserOrdStats::from_raw(&i.value)?;
        //     let owner = UserOrdStats::owner_from_key(i.key)?;
        //     to_write.push(x.to_temp_db_row(next_block_height, &owner)?);
        // }

        let mut last_number = self
            .inscription_db
            .get(&LastInscriptionNumber::get_db_key())
            .map(|x| {
                LastInscriptionNumber::from_db(DBRow {
                    key: vec![],
                    value: x,
                })
            })
            .unwrap_or(Ok(LastInscriptionNumber {
                height: 0,
                number: 0,
            }))
            .anyhow_as("Failed to decode last inscription number")?;

        last_number.height = next_block_height;

        to_write.push(last_number.to_temp_db_row()?);

        self.temp_db.write(to_write, super::db::DBFlush::Disable);

        Ok(())
    }

    pub fn reorg_handler(&self, blocks: Vec<HeaderEntry>) -> anyhow::Result<()> {
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

        let blocks = blocks.into_iter().rev().map(|block| {
            let block_height = block.height() as u32;

            let keys = self
                .tx_db
                .get(&BlockRow::txids_key(full_hash(&block.hash()[..])))
                .map(|val| {
                    bincode_util::deserialize_little::<Vec<Txid>>(&val)
                        .expect("failed to parse block txids")
                })
                .unwrap()
                .into_iter()
                .map(|x| TxRow::key(&x.into_inner()));

            let txs = self
                .tx_db
                .db
                .multi_get(keys)
                .into_iter()
                .flatten()
                .flatten()
                .map(|x| {
                    bitcoin::Transaction::consensus_decode(std::io::Cursor::new(&x))
                        .expect("failed to parse Transaction")
                })
                .collect_vec();

            (block_height, txs)
        });

        for (block_height, txs) in blocks {
            self.remove_temp_data_orhpan(block_height)?;

            // Temp db flow
            {
                self.temp_db
                    .iter_scan(&InscriptionExtraData::get_temp_db_iter_key(block_height))
                    .map(|x| {
                        (
                            x.key.clone(),
                            InscriptionExtraData::from_temp_db(x).unwrap(),
                        )
                    })
                    .for_each(|(key, (extra, _))| {
                        // Extra data to restore
                        {
                            self.temp_db.db.delete(&key).unwrap();
                            to_restore.push(extra.to_db_row().unwrap());
                        }

                        // History data to restore
                        {
                            let history_key = OrdHistoryRow::get_temp_db_key(
                                extra.value.owner,
                                &extra.location,
                                block_height,
                            );
                            let (history_row, _) = self
                                .temp_db
                                .remove(&history_key)
                                .map(|x| {
                                    OrdHistoryRow::from_temp_db_row(DBRow {
                                        key: history_key,
                                        value: x,
                                    })
                                    .unwrap()
                                })
                                .unwrap();
                            to_restore.push(history_row.into_row());
                        }
                    });
            }

            for tx in txs.into_iter().rev() {
                for (idx, output) in tx.output.iter().enumerate() {
                    let outpoint = OutPoint {
                        txid: tx.txid(),
                        vout: idx as u32,
                    };

                    // Main db flow
                    {
                        let owner = output
                            .script_pubkey
                            .to_address_str(crate::chain::Network::Bellscoin)
                            .expect("Can't parse owner");

                        let extra_key = InscriptionExtraData::get_db_key(outpoint);
                        let history_key = OrdHistoryRow::create_db_key(owner, &outpoint);

                        if let Some(_) = self.inscription_db.remove(&extra_key).map(|x| {
                            InscriptionExtraData::from_raw(DBRow {
                                key: extra_key,
                                value: x,
                            })
                            .unwrap()
                        }) {
                            self.inscription_db.db.delete(&history_key).unwrap();
                        }
                    }
                }
            }
        }

        if !to_restore.is_empty() {
            self.inscription_db
                .write(to_restore, super::db::DBFlush::Disable);
        }

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

        // for i in self
        //     .temp_db
        //     .iter_scan(&UserOrdStats::get_temp_iter_key(block_height))
        // {
        //     to_delete.push(i.key);
        // }

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

        // for i in self
        //     .temp_db
        //     .iter_scan(&UserOrdStats::get_temp_iter_key(current_block_height))
        // {
        //     let (x, owner) = UserOrdStats::from_temp_db(i)?;
        //     to_write.push(x.to_temp_db_row(next_block_height, &owner)?);
        // }

        self.temp_db.write(to_write, super::db::DBFlush::Disable);

        Ok(())
    }
}

pub struct IndexHandler<'a> {
    pub store: &'a Store,
    pub cached_partial: HashMap<Txid, Vec<(u32, usize, Transaction)>>,
    pub inscription_number: u64,
}
impl<'a> IndexHandler<'a> {
    fn get_owner(tx: &Transaction) -> anyhow::Result<String> {
        return tx
            .output
            .first()
            .and_then(|x| {
                x.script_pubkey
                    .to_address_str(crate::chain::Network::Bellscoin)
            })
            .anyhow_as("No owner 😭");
    }

    pub fn try_parse_inscription(h: u32, txs: &[Transaction]) -> DigestedBlock {
        let mut partials: HashMap<Txid, Vec<(u32, usize, Transaction)>> = HashMap::new();
        let mut inscriptions = vec![];
        let mut rest = vec![];

        for (i, tx) in txs.iter().enumerate() {
            match Inscription::from_transactions(&[tx]) {
                ParsedInscription::None => {
                    if let Some(mut chain) = partials.remove(&tx.input[0].previous_output.txid) {
                        chain.push((h, i, tx.clone()));
                        match Inscription::from_transactions(
                            &chain.iter().map(|x| &x.2).collect_vec(),
                        ) {
                            ParsedInscription::None => {
                                chain.pop();
                                partials.insert(chain.last().unwrap().2.txid(), chain);
                            }
                            ParsedInscription::Partial => {
                                partials.insert(chain.last().unwrap().2.txid(), chain);
                            }
                            ParsedInscription::Complete(inscription) => {
                                let inscription_template = InscriptionTemplate {
                                    genesis: chain.first().unwrap().2.txid(),
                                    location: tx.txid(),
                                    content_type: inscription.content_type().unwrap().to_owned(),
                                    content_len: inscription.content_length().unwrap(),
                                    owner: Self::get_owner(tx).unwrap(),
                                    inscription_number: 0,
                                    height: h,
                                    value: tx.output[0].value,
                                    offset: 0,
                                };
                                inscriptions.push((i, inscription_template));
                            }
                        }
                    } else {
                        rest.push((h, i, tx.clone()));
                    }
                }
                ParsedInscription::Partial => {
                    partials.insert(tx.txid(), vec![(h, i, tx.clone())]);
                }
                ParsedInscription::Complete(inscription) => {
                    let txid = tx.txid();
                    let inscription_template = InscriptionTemplate {
                        genesis: txid,
                        location: txid,
                        content_type: inscription.content_type().unwrap().to_owned(),
                        content_len: inscription.content_length().unwrap(),
                        owner: Self::get_owner(tx).unwrap(),
                        inscription_number: 0,
                        height: h,
                        value: tx.output[0].value,
                        offset: 0,
                    };
                    inscriptions.push((i, inscription_template));
                }
            }
        }

        DigestedBlock {
            height: h,
            partial: partials,
            completed: inscriptions,
            rest,
        }
    }

    pub fn handle_blocks(
        &mut self,
        blocks: &Vec<(u32, Vec<Transaction>)>,
    ) -> Vec<InscriptionTemplate> {
        let mut data = vec![];
        blocks
            .into_par_iter()
            .map(|(h, txs)| Self::try_parse_inscription(*h, txs))
            .collect_into_vec(&mut data);
        data.sort_unstable_by_key(|x| x.height);

        let mut completed = vec![];

        for mut digested_block in data {
            self.cached_partial.extend(digested_block.partial);

            for (h, i, tx) in digested_block.rest {
                if let Some(mut chain) = self
                    .cached_partial
                    .remove(&tx.input[0].previous_output.txid)
                {
                    chain.push((h, i, tx.clone()));
                    match Inscription::from_transactions(&chain.iter().map(|x| &x.2).collect_vec())
                    {
                        ParsedInscription::None => {
                            chain.pop();
                            self.cached_partial
                                .insert(chain.last().unwrap().2.txid(), chain);
                        }
                        ParsedInscription::Partial => {
                            self.cached_partial.insert(tx.txid(), chain);
                        }
                        ParsedInscription::Complete(inscription) => {
                            let inscription_template = InscriptionTemplate {
                                genesis: chain.first().unwrap().2.txid(),
                                location: tx.txid(),
                                content_type: inscription.content_type().unwrap().to_owned(),
                                content_len: inscription.content_length().unwrap(),
                                owner: Self::get_owner(&tx).unwrap(),
                                inscription_number: 0,
                                height: h,
                                value: tx.output[0].value,
                                offset: 0,
                            };
                            digested_block.completed.push((i, inscription_template));
                        }
                    }
                }
            }

            digested_block.completed.sort_unstable_by_key(|x| x.0);
            for (_, mut inc) in digested_block.completed {
                inc.inscription_number = self.inscription_number;
                self.inscription_number += 1;
                completed.push(inc);
            }
        }

        completed
    }

    pub fn write_inscription(&self, data: Vec<InscriptionTemplate>) -> anyhow::Result<()> {
        let mut to_write = vec![];

        for inc in data {
            let genesis = OutPoint {
                txid: inc.genesis,
                vout: 0,
            };
            let location = OutPoint {
                txid: inc.location,
                vout: 0,
            };

            let new_row = OrdHistoryRow::new(
                inc.owner.clone(),
                location,
                OrdHistoryValue {
                    inscription_id: InscriptionId {
                        txid: genesis.txid,
                        index: genesis.vout,
                    },
                    inscription_number: inc.inscription_number,
                    value: inc.value,
                },
            );

            let inscription_extra = InscriptionExtraData::new(
                location,
                genesis,
                inc.owner,
                inc.height,
                inc.content_type,
                inc.content_len,
                inc.inscription_number,
                inc.offset,
            );

            to_write.push(new_row.into_row());
            to_write.push(inscription_extra.to_db_row()?);
        }

        self.store
            .inscription_db()
            .write(to_write, super::db::DBFlush::Disable);

        Ok(())
    }

    pub fn write_patrials(&mut self) -> anyhow::Result<()> {
        if !self.cached_partial.is_empty() {
            let to_write = self
                .cached_partial
                .iter()
                .map(|(last_txid, txs)| {
                    PartialTxs {
                        block_height: txs.first().unwrap().0,
                        last_txid: *last_txid,
                        txs: txs.iter().map(|x| x.2.txid()).collect_vec(),
                    }
                    .to_db()
                    .anyhow_as("Failed to serialize partials")
                })
                .try_collect()?;

            self.cached_partial.clear();
            self.store
                .inscription_db()
                .write(to_write, super::db::DBFlush::Disable);
        }

        Ok(())
    }

    pub fn load_blocks_chunks(&self, blocks: Vec<BlockHash>) -> Vec<(u32, Vec<Transaction>)> {
        let mut chunked = Vec::new();
        blocks
            .into_par_iter()
            .map(|hash| {
                let block_height = self.store.get_block_height(hash).unwrap() as u32;
                let txs = self.get_multi_txs(&hash).unwrap().collect_vec();

                (block_height, txs)
            })
            .collect_into_vec(&mut chunked);

        chunked.sort_unstable_by_key(|x| x.0);
        chunked
    }

    pub fn write_inscription_number(&self) -> anyhow::Result<()> {
        let last_number = LastInscriptionNumber {
            height: 0,
            number: self.inscription_number,
        };

        self.store
            .inscription_db()
            .write(vec![last_number.to_db()?], super::db::DBFlush::Disable);

        Ok(())
    }

    pub fn get_multi_txs(
        &self,
        hash: &BlockHash,
    ) -> anyhow::Result<impl Iterator<Item = Transaction>> {
        let txids = self
            .store
            .txstore_db()
            .get(&BlockRow::txids_key(full_hash(&hash[..])))
            .map(|val| {
                bincode_util::deserialize_little::<Vec<Txid>>(&val)
                    .expect("failed to parse block txids")
            })
            .anyhow()?
            .into_iter()
            .map(|x| TxRow::key(&x[..]));

        Ok(self
            .store
            .txstore_db()
            .db
            .multi_get(txids)
            .into_iter()
            .flatten()
            .flatten()
            .map(|x| {
                bitcoin::Transaction::consensus_decode(std::io::Cursor::new(&x))
                    .expect("failed to parse Transaction")
            }))
    }
}

pub struct MoveIndexer<'a> {
    pub inscription_db: &'a DB,
    pub txstore_db: &'a DB,
}
impl<'a> MoveIndexer<'a> {
    pub fn get_address(script: &Script) -> String {
        Address::from_script(script, bitcoin::network::constants::Network::Bitcoin)
            .unwrap()
            .to_string()
    }

    pub fn load_inscription(&self, txs: &[Transaction]) -> Vec<(OutPoint, MovedInscription)> {
        let mut outpoints = vec![];
        for tx in txs {
            outpoints.extend(
                tx.input
                    .iter()
                    .map(|x| InscriptionExtraData::get_db_key(x.previous_output)),
            );
        }

        self.inscription_db
            .db
            .multi_get(&outpoints)
            .into_iter()
            .enumerate()
            .filter_map(|(i, x)| x.unwrap().map(|x| (i, x)))
            .map(|(i, x)| {
                InscriptionExtraData::from_raw(DBRow {
                    key: outpoints[i].clone(),
                    value: x,
                })
                .unwrap()
            })
            .map(|x| {
                (
                    x.location,
                    MovedInscription {
                        data: x,
                        leaked: false,
                        new_owner: None,
                    },
                )
            })
            .collect_vec()
    }

    pub fn handle(
        &self,
        blocks: &Vec<(u32, Vec<Transaction>)>,
    ) -> HashMap<OutPoint, MovedInscription> {
        let mut temp = vec![];
        blocks
            .par_iter()
            .map(|(_, txs)| (load_txos(self.txstore_db, txs), self.load_inscription(txs)))
            .collect_into_vec(&mut temp);

        let mut txos = HashMap::new();
        let mut inscriptions: HashMap<OutPoint, MovedInscription> = HashMap::new();

        for (txouts, inc) in temp {
            txos.extend(txouts);
            inscriptions.extend(inc);
        }

        if inscriptions.is_empty() {
            return HashMap::new();
        }

        for (_, txs) in blocks {
            for tx in txs {
                // todo coinbase be backe
                if tx.is_coin_base() {
                    continue;
                }

                let found_inscriptions = tx
                    .input
                    .iter()
                    .enumerate()
                    .map(|(idx, x)| (idx, inscriptions.remove(&x.previous_output)))
                    .filter(|x| x.1.is_some())
                    .map(|x| (x.0, x.1.unwrap()))
                    .collect_vec();

                if found_inscriptions.is_empty() {
                    continue;
                }

                let inputs_cum = InscriptionSearcher::calc_offsets(&tx, &txos);

                for (idx, mut inc) in found_inscriptions {
                    let Result::Ok((vout, offset)) = InscriptionSearcher::get_output_index_by_input(
                        inputs_cum
                            .get(idx)
                            .copied()
                            .map(|x| x + inc.data.value.offset),
                        &tx.output,
                    ) else {
                        inc.leaked = true;
                        inscriptions.insert(inc.data.location, inc);
                        continue;
                    };

                    inc.data.value.offset = offset;

                    let location = OutPoint {
                        txid: tx.txid(),
                        vout,
                    };

                    inc.new_owner =
                        Some(Self::get_address(&tx.output[vout as usize].script_pubkey));

                    inscriptions.insert(location, inc);
                }
            }
        }

        inscriptions
    }

    pub fn write_moves(&self, data: HashMap<OutPoint, MovedInscription>) -> anyhow::Result<()> {
        let mut to_write = vec![];

        for (new_location, mut inc) in data {
            if inc.leaked {
                let old_location = inc.data.location;
                let old_owner = inc.data.value.owner;

                inc.data.value.owner = "leaked 😭".to_owned();

                self.inscription_db
                    .db
                    .delete(&InscriptionExtraData::get_db_key(old_location))?;
                self.inscription_db
                    .remove(&OrdHistoryRow::create_db_key(
                        old_owner.clone(),
                        &old_location,
                    ))
                    .map(|x| OrdHistoryRow::value_from_raw(&x))
                    .anyhow_as("Failed to find OrdHistoryRow")?;

                to_write.push(inc.data.to_db_row()?);
                continue;
            }
            if let Some(new_owner) = inc.new_owner {
                let old_location = inc.data.location;
                let old_owner = inc.data.value.owner;

                inc.data.value.owner = new_owner.clone();
                inc.data.location = new_location;

                // Work with old user
                let prev_history_value = {
                    self.inscription_db
                        .db
                        .delete(&InscriptionExtraData::get_db_key(old_location))?;
                    self.inscription_db
                        .remove(&OrdHistoryRow::create_db_key(
                            old_owner.clone(),
                            &old_location,
                        ))
                        .map(|x| OrdHistoryRow::value_from_raw(&x))
                        .anyhow_as("Failed to find OrdHistoryRow")?
                };

                // Work with new user
                let new_ord_history =
                    OrdHistoryRow::new(new_owner.clone(), new_location, prev_history_value);

                to_write.push(new_ord_history.into_row());
                to_write.push(inc.data.to_db_row()?);
            }
        }

        self.inscription_db
            .write(to_write, super::db::DBFlush::Disable);

        Ok(())
    }
}

pub struct DigestedBlock {
    pub height: u32,
    pub partial: HashMap<Txid, Vec<(u32, usize, Transaction)>>,
    pub completed: Vec<(usize, InscriptionTemplate)>,
    pub rest: Vec<(u32, usize, Transaction)>,
}

pub struct InscriptionTemplate {
    pub genesis: Txid,
    pub location: Txid,
    pub content_type: String,
    pub owner: String,
    pub content_len: usize,
    pub inscription_number: u64,
    pub value: u64,
    pub height: u32,
    pub offset: u64,
}

#[derive(Debug)]
pub struct MovedInscription {
    pub data: InscriptionExtraData,
    pub new_owner: Option<String>,
    pub leaked: bool,
}

struct InscriptionSearcher {}

impl InscriptionSearcher {
    fn calc_offsets(tx: &Transaction, tx_outs: &HashMap<OutPoint, u64>) -> Vec<u64> {
        let mut input_values = tx
            .input
            .iter()
            .map(|x| *tx_outs.get(&x.previous_output).unwrap())
            .collect_vec();

        let spend: u64 = input_values.iter().sum();

        let mut fee = spend - tx.output.iter().map(|x| x.value).sum::<u64>();
        while let Some(input) = input_values.pop() {
            if input > fee {
                input_values.push(input - fee);
                break;
            }
            fee -= input;
        }

        let mut inputs_offsets = input_values.iter().fold(vec![0], |mut acc, x| {
            acc.push(acc.last().unwrap() + x);
            acc
        });

        inputs_offsets.pop();

        inputs_offsets
    }

    fn get_output_index_by_input(
        offset: Option<u64>,
        tx_outs: &Vec<TxOut>,
    ) -> anyhow::Result<(u32, u64)> {
        let Some(mut offset) = offset else {
            anyhow::bail!("leaked");
        };

        for (idx, out) in tx_outs.iter().enumerate() {
            if offset < out.value {
                return Ok((idx as u32, offset));
            }
            offset -= out.value;
        }

        anyhow::bail!("leaked");
    }
}

pub fn load_txos(tx_db: &DB, txs: &[Transaction]) -> HashMap<OutPoint, u64> {
    txs.iter()
        .filter(|x| !x.is_coin_base())
        .flat_map(|tx| {
            let keys = tx.input.iter().map(|x| x.previous_output).collect_vec();

            tx_db
                .db
                .multi_get(keys.iter().map(|x| TxOutRow::key(&x)).collect_vec())
                .iter()
                .flatten()
                .flatten()
                .map(|x| {
                    bitcoin::consensus::deserialize::<TxOut>(&x).expect("failed to parse TxOut")
                })
                .zip(keys.iter())
                .map(|x| (x.1.clone(), x.0.value))
                .collect_vec()
        })
        .collect()
}

#[macro_export]
macro_rules! measure_time {
    ($n:literal: $e:expr) => {{
        let time = std::time::Instant::now();
        let a = $e;
        tracing::warn!("{}: {:.3} s", $n, time.elapsed().as_secs_f32());
        a
    }};
}
