use std::{collections::HashMap, convert::TryInto, sync::Arc};

use crate::{
    config::TOKENS_OFFSET,
    inscription_entries::{
        index::PARTIAL_TXID_TO_TXIDS,
        inscription::{
            update_last_block_number, Inscription, InscriptionContent, InscriptionExtraData,
            LastInscriptionNumber, LeakedInscriptions, Location, MovedInscription, OrdHistoryRow,
            OrdHistoryValue, ParsedInscription, PartialTxs, UserOrdStats,
        },
        InscriptionId,
    },
    new_index::schema::TxOutRow,
    util::{bincode_util, errors::AsAnyhow, full_hash, HeaderEntry, ScriptToAddr},
    HEIGHT_DELAY,
};
use anyhow::{Ok, Result};
use bitcoin::{consensus::Decodable, hashes::Hash, OutPoint, TxOut};
use bitcoin::{Transaction, Txid};
use itertools::Itertools;

use super::{
    schema::{BlockRow, TxRow},
    token::{TokenCache, TokenTempAction, TokensData},
    DBRow, Store, DB,
};
pub struct InscriptionUpdater {
    store: Arc<Store>,
}

impl InscriptionUpdater {
    pub fn new(store: Arc<Store>) -> Result<Self> {
        Ok(Self { store })
    }

    pub fn index_transaction_inscriptions(
        &self,
        tx: Transaction,
        tx_idx: usize,
        block_height: u32,
        txos: &HashMap<OutPoint, TxOut>,
        token_cache: &mut TokenCache,
        sender: Arc<crossbeam_channel::Sender<InscriptionContent>>,
        leaked_inscriptions: &mut LeakedInscriptions,
    ) -> Result<()> {
        let txid = tx.txid();

        let mut to_temp_write = vec![];
        let mut to_write = vec![];

        let mut is_inscription_found = false;

        leaked_inscriptions.add_tx_fee(&tx, txos);

        for (idx, input) in tx.input.iter().enumerate() {
            for (key, mut inscription_extra) in self
                .store
                .inscription_db()
                .iter_scan(&InscriptionExtraData::find_by_outpoint(
                    &input.previous_output,
                ))
                .map(|x| (x.key.clone(), InscriptionExtraData::from_raw(x).unwrap()))
            {
                self.store.inscription_db().remove(&key);

                let prev_location = inscription_extra.location.clone();

                let old_owner = inscription_extra.value.owner.clone();

                if let Some(mut v) = self
                    .store
                    .inscription_db()
                    .get(&UserOrdStats::get_db_key(&old_owner).unwrap())
                    .map(|x| UserOrdStats::from_raw(&x).unwrap())
                {
                    v.amount -= inscription_extra.value.value;
                    v.count -= 1;

                    to_write.push(v.to_db_row(&old_owner).unwrap());
                }

                to_temp_write.push(inscription_extra.to_temp_db_row(block_height, &prev_location)?);

                let inputs_cum = InscriptionSearcher::calc_offsets(&tx, &txos).unwrap();

                let Result::Ok((vout, offset)) = InscriptionSearcher::get_output_index_by_input(
                    inputs_cum
                        .get(idx)
                        .copied()
                        .map(|x| x + inscription_extra.location.offset),
                    &tx.output,
                ) else {
                    leaked_inscriptions.add(
                        idx,
                        &tx,
                        inscription_extra.location.offset,
                        txos,
                        inscription_extra,
                        is_inscription_found,
                    );

                    is_inscription_found = true;

                    continue;
                };

                // Work with old user
                let prev_history_value = {
                    let key = OrdHistoryRow::create_db_key(&old_owner, &prev_location);

                    let prev_history_value = self
                        .store
                        .inscription_db()
                        .remove(&key)
                        .map(|x| OrdHistoryRow::value_from_raw(&x))
                        .anyhow_as("Failed to find OrdHistoryRow")?;

                    to_temp_write.push(
                        OrdHistoryRow::new(
                            old_owner.clone(),
                            prev_location.clone(),
                            prev_history_value.clone(),
                        )
                        .to_temp_db_row(block_height),
                    );

                    prev_history_value
                };

                let new_outpoint = Location {
                    offset,
                    outpoint: OutPoint { txid, vout },
                };

                // Work with new user
                let ord_history = {
                    let new_owner = tx.output[0]
                        .script_pubkey
                        .to_address_str(crate::chain::Network::Bellscoin)
                        .anyhow_as("No owner :(")?;

                    inscription_extra.value.owner = new_owner.clone();

                    if let Some(mut v) = self
                        .store
                        .inscription_db()
                        .get(&UserOrdStats::get_db_key(&new_owner).unwrap())
                        .map(|x| UserOrdStats::from_raw(&x).unwrap())
                    {
                        v.amount += inscription_extra.value.value;
                        v.count += 1;

                        to_write.push(v.to_db_row(&old_owner).unwrap());
                    }

                    token_cache.try_transfer(
                        block_height,
                        tx_idx,
                        prev_location.outpoint,
                        new_owner.clone(),
                    );

                    OrdHistoryRow::new(new_owner, new_outpoint.clone(), prev_history_value)
                };

                inscription_extra.location = new_outpoint;

                to_write.push(ord_history.to_db_row());
                to_write.push(inscription_extra.to_db_row()?);
            }
        }

        if !to_temp_write.is_empty() || !to_write.is_empty() {
            self.store
                .temp_db()
                .write(to_temp_write, super::db::DBFlush::Disable);
            self.store
                .inscription_db()
                .write(to_write, super::db::DBFlush::Disable);

            return Ok(());
        }

        let txs = load_partials(&self.store, tx.clone(), block_height, true);

        match Inscription::from_transactions(txs.iter().collect_vec().as_slice()) {
            ParsedInscription::None => {}

            ParsedInscription::Partial => {
                let row = PartialTxs {
                    block_height,
                    last_outpoint: OutPoint { txid, vout: 0 },
                    txs: txs.into_iter().map(|x| x.txid()).collect_vec(),
                };

                self.store
                    .temp_db()
                    .write(vec![row.to_temp_db_row()?], super::db::DBFlush::Disable);
            }

            ParsedInscription::Complete(inscription) => {
                let og_inscription_id = InscriptionId {
                    txid: Txid::from_slice(&txs[0].txid()).anyhow()?,
                    index: 0,
                };

                let location = Location {
                    offset: 0,
                    outpoint: OutPoint { txid, vout: 0 },
                };

                let genesis = OutPoint {
                    txid: og_inscription_id.txid,
                    vout: og_inscription_id.index,
                };

                let owner = tx.output[0]
                    .script_pubkey
                    .to_address_str(crate::chain::Network::Bellscoin)
                    .unwrap();

                let inscription_number: u64 = self
                    .store
                    .temp_db()
                    .remove(&LastInscriptionNumber::get_temp_db_key(block_height))
                    .map(|x| u64::from_be_bytes(x.try_into().unwrap()))
                    .unwrap_or(0);

                let new_row = OrdHistoryRow::new(
                    owner.clone(),
                    location.clone(),
                    OrdHistoryValue {
                        inscription_id: og_inscription_id,
                        inscription_number,
                    },
                );

                let new_inc_n = LastInscriptionNumber::new(inscription_number + 1);

                let inscription_extra = InscriptionExtraData::new(
                    location.clone(),
                    owner.clone(),
                    block_height,
                    inscription.content_type().unwrap().to_string(),
                    inscription.content_length().unwrap(),
                    tx.output[0].value,
                );

                sender
                    .send(InscriptionContent {
                        content: base64::encode(inscription.body().unwrap()),
                        content_type: inscription.content_type().unwrap().to_string(),
                        inscription_id: og_inscription_id,
                        number: inscription_number,
                    })
                    .anyhow_as("Failed to send inscription content")?;

                let mut to_write = vec![new_row.to_db_row(), inscription_extra.to_db_row()?];

                if let Some(mut v) = self
                    .store
                    .inscription_db()
                    .get(&UserOrdStats::get_db_key(&owner).unwrap())
                    .map(|x| UserOrdStats::from_raw(&x).unwrap())
                {
                    v.amount += inscription_extra.value.value;
                    v.count += 1;

                    to_write.push(v.to_db_row(&owner).unwrap());
                }

                token_cache.parse_token_action(
                    inscription.content_type().unwrap(),
                    inscription.body().unwrap(),
                    block_height,
                    tx_idx,
                    owner,
                    genesis,
                    location.outpoint,
                    Some(self.store.temp_db()),
                );

                self.store
                    .inscription_db()
                    .write(to_write, super::db::DBFlush::Disable);

                self.store.temp_db().write(
                    vec![new_inc_n.to_temp_db_row(block_height)?],
                    super::db::DBFlush::Disable,
                );
            }
        }

        Ok(())
    }

    pub fn chain_mempool_inscriptions(txs: &Vec<Transaction>) -> Vec<Vec<Transaction>> {
        let mut chain: HashMap<Txid, Vec<Transaction>> = txs
            .into_iter()
            .map(|x| (x.txid(), vec![x.clone()]))
            .collect();

        for tx in txs {
            let prev_txid = tx.input[0].previous_output.txid;

            if !chain.contains_key(&prev_txid) {
                continue;
            }

            if let Some(v) = chain.remove(&tx.txid()) {
                chain.get_mut(&prev_txid).unwrap().extend(v);
            }
        }

        chain.into_values().collect()
    }

    pub fn check_mempool_move(
        inscription_db: &DB,
        tx: &Transaction,
        mempool_inscriptions: &mut HashMap<OutPoint, OutPoint>,
        txos: &HashMap<OutPoint, TxOut>,
    ) -> Result<()> {
        let inputs_cum = {
            let inputs = InscriptionSearcher::calc_offsets(&tx, &txos);
            if inputs.is_none() {
                return Ok(());
            }
            inputs.unwrap()
        };

        for (idx, input) in tx.input.iter().enumerate() {
            let inscriptions = {
                let key = mempool_inscriptions
                    .remove(&input.previous_output)
                    .unwrap_or(input.previous_output);

                inscription_db
                    .iter_scan(&InscriptionExtraData::find_by_outpoint(&key))
                    .map(|x| InscriptionExtraData::from_raw(x).unwrap())
            };

            for inscription_extra in inscriptions {
                if let Result::Ok((vout, _)) = InscriptionSearcher::get_output_index_by_input(
                    inputs_cum
                        .get(idx)
                        .copied()
                        .map(|x| x + inscription_extra.location.offset),
                    &tx.output,
                ) {
                    mempool_inscriptions.insert(
                        OutPoint {
                            txid: tx.txid(),
                            vout,
                        },
                        input.previous_output,
                    );
                } else {
                    continue;
                };
            }
        }
        Ok(())
    }

    pub fn copy_from_main_block(&self, block_height: u32) -> anyhow::Result<()> {
        if let Some(_) = self
            .store
            .temp_db()
            .get(&LastInscriptionNumber::get_temp_db_key(block_height))
        {
            return Ok(());
        }

        let mut to_write = vec![];

        for i in self
            .store
            .inscription_db()
            .iter_scan(&bincode_util::serialize_big(&(PARTIAL_TXID_TO_TXIDS))?)
        {
            let mut x = PartialTxs::from_db(i)?;
            x.block_height = block_height;
            to_write.push(x.to_temp_db_row()?);
        }

        let last_number = self
            .store
            .inscription_db()
            .get(&LastInscriptionNumber::get_db_key())
            .map(LastInscriptionNumber::from_raw)
            .unwrap()
            .anyhow_as("Failed to decode last inscription number")?;

        to_write.push(last_number.to_temp_db_row(block_height)?);

        self.store
            .temp_db()
            .write(to_write, super::db::DBFlush::Disable);

        Ok(())
    }

    pub fn reorg_handler(
        &self,
        blocks: Vec<HeaderEntry>,
        first_inscription_block: usize,
    ) -> anyhow::Result<()> {
        let mut to_restore = vec![];

        let min_height = blocks.iter().map(|x| x.height()).min().unwrap() as u32 - 1;

        let last_inscription_number_key = LastInscriptionNumber::get_temp_db_key(min_height);
        let last_number = self
            .store
            .temp_db()
            .get(&last_inscription_number_key)
            .map(|x| {
                LastInscriptionNumber::from_db(DBRow {
                    key: last_inscription_number_key,
                    value: x,
                })
                .unwrap()
            })
            .expect("Failed to find last inscription number");

        to_restore.push(last_number.to_db()?);

        let blocks = blocks.into_iter().rev().map(|block| {
            let block_height = block.height() as u32;

            let keys = self
                .store
                .txstore_db()
                .get(&BlockRow::txids_key(full_hash(&block.hash()[..])))
                .map(|val| {
                    bincode_util::deserialize_little::<Vec<Txid>>(&val)
                        .expect("failed to parse block txids")
                })
                .unwrap()
                .into_iter()
                .map(|x| TxRow::key(&x.into_inner()));

            let txs = self
                .store
                .txstore_db()
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
            self.remove_temp_data_orphan(block_height, first_inscription_block)?;

            // Temp db flow
            {
                self.store
                    .temp_db()
                    .iter_scan(&InscriptionExtraData::get_temp_db_iter_key(block_height))
                    .map(|x| {
                        (
                            x.key.clone(),
                            InscriptionExtraData::from_temp_db(x).unwrap(),
                        )
                    })
                    .for_each(|(key, extra)| {
                        if let Some(mut v) = self
                            .store
                            .inscription_db()
                            .get(&UserOrdStats::get_db_key(&extra.value.owner).unwrap())
                            .map(|x| UserOrdStats::from_raw(&x).unwrap())
                        {
                            v.amount += extra.value.value;
                            v.count += 1;

                            self.store.inscription_db().write(
                                vec![v.to_db_row(&extra.value.owner).unwrap()],
                                super::db::DBFlush::Disable,
                            );
                        }

                        // Extra data to restore
                        {
                            self.store.temp_db().db.delete(&key).unwrap();
                            to_restore.push(extra.to_db_row().unwrap());
                        }

                        // History data to restore
                        {
                            let history_key = OrdHistoryRow::get_temp_db_key(
                                &extra.value.owner,
                                &extra.location,
                                block_height,
                            );
                            let history_row = self.store.temp_db().remove(&history_key).map(|x| {
                                OrdHistoryRow::from_temp_db_row(DBRow {
                                    key: history_key,
                                    value: x,
                                })
                                .unwrap()
                            });
                            if let Some((history_row, _)) = history_row {
                                to_restore.push(history_row.to_db_row());
                            }
                        }
                    });
            }

            for tx in txs.into_iter().rev() {
                if tx.is_coin_base() {
                    // TODO handle coinbase
                    continue;
                }

                for (idx, output) in tx.output.iter().enumerate() {
                    let outpoint = OutPoint {
                        txid: tx.txid(),
                        vout: idx as u32,
                    };
                    let owner = output
                        .script_pubkey
                        .to_address_str(crate::chain::Network::Bellscoin)
                        .expect("Can't parse owner");

                    // Main db flow
                    {
                        let extra_key = InscriptionExtraData::find_by_outpoint(&outpoint);

                        for extra in self
                            .store
                            .inscription_db()
                            .iter_scan(&extra_key)
                            .map(|x| InscriptionExtraData::from_raw(x).unwrap())
                        {
                            if let Some(mut v) = self
                                .store
                                .inscription_db()
                                .get(&UserOrdStats::get_db_key(&extra.value.owner).unwrap())
                                .map(|x| UserOrdStats::from_raw(&x).unwrap())
                            {
                                v.amount -= extra.value.value;
                                v.count -= 1;

                                self.store.inscription_db().write(
                                    vec![v.to_db_row(&extra.value.owner).unwrap()],
                                    super::db::DBFlush::Disable,
                                );
                            }

                            self.store.inscription_db().delete_batch(vec![
                                extra.to_db_row().unwrap().key,
                                OrdHistoryRow::create_db_key(&owner, &extra.location),
                            ]);
                        }
                    }
                }
            }
        }

        if !to_restore.is_empty() {
            self.store
                .inscription_db()
                .write(to_restore, super::db::DBFlush::Disable);
        }

        Ok(())
    }

    pub fn remove_temp_data_orphan(
        &self,
        block_height: u32,
        first_inscription_block: usize,
    ) -> anyhow::Result<()> {
        let mut to_delete = vec![];

        update_last_block_number(first_inscription_block, &self.store, block_height, false)?;

        for i in self
            .store
            .temp_db()
            .iter_scan(&PartialTxs::get_temp_iter_key(block_height))
        {
            to_delete.push(i.key);
        }

        for i in self
            .store
            .temp_db()
            .iter_scan(&TokenTempAction::get_all_iter_key())
        {
            let key = i.key.clone();
            let (height, _) = TokenTempAction::from_db_row(i);
            if height < block_height + HEIGHT_DELAY - TOKENS_OFFSET {
                to_delete.push(key);
            }
        }

        to_delete.push(LastInscriptionNumber::get_temp_db_key(block_height));

        self.store.temp_db().delete_batch(to_delete);

        Ok(())
    }

    pub fn copy_to_next_block(&self, current_block_height: u32) -> anyhow::Result<()> {
        let next_block_height = current_block_height + 1;
        let mut to_write = vec![];

        for i in self
            .store
            .temp_db()
            .iter_scan(&PartialTxs::get_temp_iter_key(current_block_height))
        {
            let mut x = PartialTxs::from_temp_db(i)?;
            x.block_height = next_block_height;
            to_write.push(x.to_temp_db_row()?);
        }

        let last_number = self
            .store
            .temp_db()
            .get(&LastInscriptionNumber::get_temp_db_key(
                current_block_height,
            ))
            .map(LastInscriptionNumber::from_raw)
            .unwrap()
            .anyhow_as("Failed to decode last inscription number")?;

        to_write.push(last_number.to_temp_db_row(next_block_height)?);

        self.store
            .temp_db()
            .write(to_write, super::db::DBFlush::Disable);

        Ok(())
    }
}

#[derive(Default)]
pub struct DigestedMoves {
    pub inscriptions: HashMap<OutPoint, MovedInscription>,
    pub tokens_data: TokensData,
}
pub struct InscriptionTemplate {
    pub genesis: OutPoint,
    pub location: Location,
    pub content_type: String,
    pub owner: String,
    pub content_len: usize,
    pub inscription_number: u64,
    pub value: u64,
    pub height: u32,
    pub content: Vec<u8>,
}

pub struct InscriptionSearcher {}

impl InscriptionSearcher {
    pub fn calc_offsets(tx: &Transaction, tx_outs: &HashMap<OutPoint, TxOut>) -> Option<Vec<u64>> {
        let mut input_values = tx
            .input
            .iter()
            .map(|x| tx_outs.get(&x.previous_output).map(|x| x.value))
            .collect::<Option<Vec<u64>>>()?;

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

        Some(inputs_offsets)
    }

    pub fn get_output_index_by_input(
        offset: Option<u64>,
        tx_outs: &[TxOut],
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

pub fn load_txos(tx_db: &DB, txs: &[Transaction]) -> HashMap<OutPoint, TxOut> {
    let keys_iter = txs
        .iter()
        .filter(|x| !x.is_coin_base())
        .flat_map(|tx| tx.input.iter().map(|x| x.previous_output));
    let keys = keys_iter.clone().map(|x| TxOutRow::key(&x)).collect_vec();

    tx_db
        .db
        .multi_get(keys)
        .iter()
        .flatten()
        .flatten()
        .map(|x| bitcoin::consensus::deserialize::<TxOut>(&x).expect("failed to parse TxOut"))
        .zip(keys_iter)
        .map(|x| (x.1.clone(), x.0))
        .collect()
}

pub fn load_partials(
    store: &Store,
    tx: Transaction,
    block_height: u32,
    remove_partials: bool,
) -> Vec<Transaction> {
    let partial_key = PartialTxs::get_temp_db_key(block_height, &tx.input[0].previous_output);

    let tx_ids = {
        match remove_partials {
            true => store.temp_db().remove(&partial_key),
            false => store.temp_db().get(&partial_key),
        }
    }
    .map(|x| {
        PartialTxs::from_temp_db(DBRow {
            key: partial_key.clone(),
            value: x,
        })
        .unwrap()
        .txs
    })
    .unwrap_or(vec![]);

    let key = tx_ids
        .into_iter()
        .map(|x| TxRow::key(&x.into_inner()))
        .collect_vec();

    let mut txs = store
        .txstore_db()
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

    if remove_partials {
        txs.push(tx);
    }

    txs
}

#[macro_export]
macro_rules! measure_time {
    ($n:literal: $e:expr) => {{
        let time = std::time::Instant::now();
        let a = $e;
        if time.elapsed().as_secs_f32() > 5.0 {
            tracing::debug!("{}: {:.3} s", $n, time.elapsed().as_secs_f32());
        }
        a
    }};
}

pub fn get_owner(tx: &Transaction, idx: usize) -> Option<String> {
    tx.output[idx]
        .script_pubkey
        .to_address_str(crate::chain::Network::Bellscoin)
}
