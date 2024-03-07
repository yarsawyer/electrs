use std::collections::{BTreeMap, HashMap};

use bitcoin::{OutPoint, Transaction};
use itertools::Itertools;
use rayon::iter::{
    IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator, ParallelIterator,
};

use crate::{
    inscription_entries::inscription::{
        InscriptionExtraData, LeakedInscriptions, Location, MovedInscription, OrdHistoryRow,
        UserOrdStats,
    },
    new_index::temp_updater::{get_owner, load_txos, InscriptionSearcher},
    util::errors::AsAnyhow,
};
use std::ops::Bound::Included;

use super::{
    token::{TokenCache, TransferProto},
    Store,
};

pub struct MoveIndexer<'a> {
    pub store: &'a Store,
    pub cached_transfer: HashMap<OutPoint, (String, TransferProto)>,
}
impl<'a> MoveIndexer<'a> {
    pub fn load_inscription(&self, txs: &[Transaction]) -> Vec<(Location, MovedInscription)> {
        txs.into_par_iter()
            .flat_map_iter(|x| {
                x.input.iter().map(|x| x.previous_output).flat_map(|x| {
                    self.store
                        .inscription_db()
                        .iter_scan(&InscriptionExtraData::find_by_outpoint(&x))
                        .map(|x| InscriptionExtraData::from_raw(x).unwrap())
                        .map(|x| {
                            (
                                x.location.clone(),
                                MovedInscription {
                                    data: x,
                                    new_owner: None,
                                },
                            )
                        })
                })
            })
            .collect()
    }

    pub fn handle(
        &mut self,
        blocks: &Vec<(u32, Vec<Transaction>)>,
        token_cache: &mut TokenCache,
    ) -> HashMap<Location, MovedInscription> {
        let mut temp = vec![];

        blocks
            .par_iter()
            .map(|(_, txs)| {
                (
                    load_txos(self.store.txstore_db(), txs),
                    self.load_inscription(txs),
                )
            })
            .collect_into_vec(&mut temp);

        let mut txos = HashMap::new();
        let mut inscriptions: BTreeMap<Location, MovedInscription> = BTreeMap::new();

        for (tx_outs, inc) in temp {
            txos.extend(tx_outs.into_iter().map(|x| (x.0, x.1)));
            for (loc, inc) in inc {
                inscriptions.insert(loc, inc);
            }
        }

        if inscriptions.is_empty() {
            return HashMap::default();
        }

        for (height, txs) in blocks {
            let mut leaked_inscriptions = None;

            for tx in txs {
                if tx.is_coin_base() {
                    leaked_inscriptions = Some(LeakedInscriptions::new(tx.clone()));
                    continue;
                }

                leaked_inscriptions.as_mut().unwrap().add_tx_fee(tx, &txos);

                let found_inscriptions = tx
                    .input
                    .iter()
                    .enumerate()
                    .flat_map(|(idx, x)| {
                        let keys = inscriptions
                            .range((
                                Included(Location {
                                    outpoint: x.previous_output,
                                    offset: 0,
                                }),
                                Included(Location {
                                    outpoint: x.previous_output,
                                    offset: u64::MAX,
                                }),
                            ))
                            .map(|x| x.0)
                            .cloned()
                            .collect_vec();

                        let mut res = vec![];

                        for location in keys {
                            res.push((
                                idx,
                                location.offset,
                                inscriptions.remove(&location).unwrap(),
                            ))
                        }

                        res.into_iter()
                    })
                    .collect_vec();

                if !found_inscriptions.is_empty() {
                    let inputs_cum = InscriptionSearcher::calc_offsets(tx, &txos).unwrap();

                    let mut is_inscription_leaked = false;

                    for (idx, current_offset, mut inc) in found_inscriptions {
                        let Result::Ok((vout, offset)) =
                            InscriptionSearcher::get_output_index_by_input(
                                inputs_cum.get(idx).copied().map(|x| x + current_offset),
                                &tx.output,
                            )
                        else {
                            leaked_inscriptions.as_mut().unwrap().add(
                                idx,
                                tx,
                                current_offset,
                                &txos,
                                inc.data,
                                is_inscription_leaked,
                            );

                            is_inscription_leaked = true;
                            continue;
                        };

                        inc.data.value.value = tx.output[vout as usize].value;
                        let location = Location {
                            offset,
                            outpoint: OutPoint {
                                txid: tx.txid(),
                                vout,
                            },
                        };

                        let new_owner = get_owner(tx, vout as usize).unwrap();
                        if inc.new_owner.is_none() {
                            token_cache.try_transfer(
                                *height,
                                idx,
                                inc.data.location.outpoint,
                                new_owner.clone(),
                            );
                        }

                        inc.new_owner = Some(new_owner);
                        inscriptions.insert(location, inc);
                    }
                }
            }

            let Some(leaked_inscriptions) = leaked_inscriptions.as_mut() else {
                continue;
            };

            for (location, inc) in leaked_inscriptions.get_leaked_inscriptions() {
                token_cache.try_transfer(
                    *height,
                    0,
                    inc.data.location.outpoint,
                    inc.new_owner.clone().unwrap(),
                );

                inscriptions.insert(location, inc);
            }
        }

        inscriptions.into_iter().collect()
    }

    pub fn write_moves(&self, data: HashMap<Location, MovedInscription>) -> anyhow::Result<()> {
        let mut to_write = vec![];

        let keys = {
            let mut keys = data.values().map(|x| &x.data.value.owner).collect_vec();
            keys.extend(data.values().map(|x| &x.new_owner).flatten());

            keys.into_iter().unique().collect_vec()
        };

        let mut stats_cache: HashMap<_, _> = self
            .store
            .inscription_db()
            .db
            .multi_get(keys.iter().map(|x| UserOrdStats::get_db_key(&x).unwrap()))
            .into_iter()
            .flatten()
            .map(|x| x.map(|x| UserOrdStats::from_raw(&x).unwrap()))
            .zip(keys)
            .map(|x| {
                let owner = x.1.clone();

                x.0.map(|z| (owner, z))
            })
            .flatten()
            .collect();

        for (new_location, mut inc) in data {
            if inc.new_owner.is_none() {
                continue;
            }

            let old_location = inc.data.location.clone();
            let key = InscriptionExtraData::get_db_key(old_location.clone());

            let old_owner = inc.data.value.owner.clone();

            if let Some(v) = stats_cache.get_mut(&old_owner) {
                v.amount -= inc.data.value.value;
                v.count -= 1;
            }

            inc.data.location = new_location.clone();
            let prev_history_value = {
                self.store.inscription_db().db.delete(&key)?;
                self.store
                    .inscription_db()
                    .remove(&OrdHistoryRow::create_db_key(&old_owner, &old_location))
                    .map(|x| OrdHistoryRow::value_from_raw(&x))
                    .anyhow_as("Failed to find OrdHistoryRow")?
            };

            if let Some(new_owner) = inc.new_owner {
                if let Some(v) = stats_cache.get_mut(&new_owner) {
                    v.amount += inc.data.value.value;
                    v.count += 1;
                }

                inc.data.value.owner = new_owner.clone();

                let new_ord_history =
                    OrdHistoryRow::new(new_owner, new_location, prev_history_value);

                to_write.push(new_ord_history.to_db_row());
            }

            to_write.push(inc.data.to_db_row()?);
        }

        to_write.extend(
            stats_cache
                .into_iter()
                .map(|x| x.1.to_db_row(&x.0).unwrap()),
        );

        self.store
            .inscription_db()
            .write(to_write, super::db::DBFlush::Enable);

        Ok(())
    }
}
