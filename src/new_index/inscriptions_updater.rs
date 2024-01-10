use bitcoin::{consensus::Encodable, TxIn};
use std::{
    collections::{HashMap, VecDeque},
    convert::TryInto, sync::Arc,
};

use crate::{
    db_key,
    inscription_entries::{
        entry::{Entry, InscriptionEntry},
        height::Height,
        index::{InscriptionIndex, NUMBER_TO_ID, INSCRIPTION_ID_TO_TXIDS},
        inscription::Inscription,
        inscription::ParsedInscription,
        inscription_id::InscriptionId,
        Sat, SatPoint,
    },
    util::errors::AsAnyhow,
};
use anyhow::{anyhow, Result};
use bitcoin::consensus::Decodable;
use bitcoin::{hashes::Hash, OutPoint, Transaction, Txid};

use super::{DB, Store};

#[derive(Clone)]
pub(super) struct Flotsam {
    inscription_id: InscriptionId,
    offset: u64,
    origin: Origin,
}

#[derive(Clone)]
enum Origin {
    New(u64),
    Old(SatPoint),
}

pub(crate) struct InscriptionUpdater<'a> {
    flotsam: Vec<Flotsam>,
    height: u64,
    id_to_satpoint: &'a str,
    id_to_txids: &'a str,
    txid_to_tx: &'a str,
    partial_txid_to_txids: &'a str,
    id_to_entry: &'a str,
    lost_sats: u64,
    next_number: u64,
    number_to_id: &'a str,
    outpoint_to_value: &'a str,
    reward: u64,
    sat_to_inscription_id: &'a str,
    timestamp: u32,
    value_cache: &'a parking_lot::RwLock<HashMap<OutPoint, u64>>,
    database: &'a DB,
}

impl<'a> InscriptionUpdater<'a> {
    pub(crate) fn new(
        height: u64,
        id_to_satpoint: &'a str,
        id_to_txids: &'a str,
        txid_to_tx: &'a str,
        partial_txid_to_txids: &'a str,
        id_to_entry: &'a str,
        lost_sats: u64,
        number_to_id: &'a str,
        outpoint_to_value: &'a str,
        sat_to_inscription_id: &'a str,
        timestamp: u32,
        value_cache: &'a parking_lot::RwLock<HashMap<OutPoint, u64>>,
        database: &'a DB,
    ) -> Result<Self> {
        let next_number = database
            .iter_scan(NUMBER_TO_ID.as_bytes())
            .map(|dbrow| {
                dbrow
                    .value
                    .try_into()
                    .map(|x| u64::from_le_bytes(x) + 1)
                    .unwrap_or(0)
            })
            .next()
            .unwrap_or(0);

        Ok(Self {
            flotsam: Vec::new(),
            height,
            id_to_satpoint,
            id_to_txids,
            txid_to_tx,
            partial_txid_to_txids,
            id_to_entry,
            lost_sats,
            next_number,
            number_to_id,
            outpoint_to_value,
            reward: Height(height).subsidy(),
            sat_to_inscription_id,
            timestamp,
            value_cache,
            database,
        })
    }

    pub(crate) fn index_transaction_inscriptions(
        &mut self,
        store: Arc<Store>,
        tx: &Transaction,
        txid: Txid,
        input_sat_ranges: Option<&VecDeque<(u128, u128)>>,
    ) -> Result<(u64, bool)> {
        let mut inscriptions = Vec::new();
        let mut is_inscription = false;
        
        let mut writer = vec![];
        tx.consensus_encode(&mut writer)?;
        store.inscription_db().put(&db_key!("T", &txid.into_inner()), &writer);

        // let mut input_value = 0;
        // for tx_in in &tx.input {
        //     if tx_in.previous_output.is_null() {
        //         input_value += Height(self.height).subsidy();
        //     } else {
        //         for bibas in
        //             InscriptionIndex::inscriptions_on_output(self.database, tx_in.previous_output)
        //                 .track_err()?
        //         {
        //             let (old_satpoint, inscription_id) = bibas.track_err()?;

        //             inscriptions.push(Flotsam {
        //                 offset: input_value + old_satpoint.offset,
        //                 inscription_id,
        //                 origin: Origin::Old(old_satpoint),
        //             });
        //         }

        //         input_value +=
        //             if let Some(value) = self.value_cache.write().remove(&tx_in.previous_output) {
        //                 value
        //             } else if let Some(value) = self.database.remove(&db_key!(
        //                 self.outpoint_to_value,
        //                 &tx_in.previous_output.store().track_err()?
        //             )) {
        //                 u64::from_le_bytes(value.try_into().track_err()?)
        //             } else {
        //                 return Err(anyhow!(
        //                     "failed to get transaction for {}",
        //                     tx_in.previous_output.txid
        //                 ));
        //             }
        //     }
        // }

        if true {
            let previous_txid = tx.input[0].previous_output.txid;
            let previous_txid_bytes: [u8; 32] = previous_txid.into_inner();
            let mut txids_vec = vec![];

            let txs = match self
                .database
                .get(&db_key!(self.partial_txid_to_txids, &previous_txid_bytes))
            {
                Some(partial_txids) => {
                    let txids = partial_txids;
                    let mut txs = vec![];
                    txids_vec = txids.to_vec();
                    for i in 0..txids.len() / 32 {
                        let txid = &txids[i * 32..i * 32 + 32];
                       
                        let tx_result = store.inscription_db().get(&db_key!("T", txid)).anyhow()?;
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
                    // todo: clean up db
                }

                ParsedInscription::Partial => {
                    let mut txid_vec = txid.into_inner().to_vec();
                    txids_vec.append(&mut txid_vec);

                    self.database
                        .remove(&db_key!(self.partial_txid_to_txids, &previous_txid_bytes));

                    self.database.put(
                        &db_key!(self.partial_txid_to_txids, &txid.into_inner()),
                        txids_vec.as_slice(),
                    );
                   
                    is_inscription = true;
                }

                ParsedInscription::Complete(_inscription) => {
                    self.database
                        .remove(&db_key!(self.partial_txid_to_txids, &previous_txid_bytes));

                    let mut txid_vec = txid.into_inner().to_vec();
                    txids_vec.append(&mut txid_vec);

                    let mut inscription_id = [0_u8; 36];
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            txids_vec.as_ptr(),
                            inscription_id.as_mut_ptr(),
                            32,
                        )
                    }

                    let og_inscription_id = InscriptionId {
                        txid: Txid::from_slice(&txids_vec[0..32]).track_err()?,
                        index: 0,
                    };
                    store.inscription_db().put(&db_key!(INSCRIPTION_ID_TO_TXIDS,&og_inscription_id.store().anyhow()?), &txids_vec);
                    //store.inscription_db().put(&db_key!(INSCRIPTION_ID_TO_INSCRIPTION_ENTRY,&og_inscription_id.store().anyhow()?), _inscription.);

                    inscriptions.push(Flotsam {
                        inscription_id: og_inscription_id,
                        offset: 0,
                        origin: Origin::New(
                            0
                            //input_value - tx.output.iter().map(|txout| txout.value).sum::<u64>(),
                        ),
                    });
                    is_inscription = true;
                }
            }
        };

        // let is_coinbase = tx
        //     .input
        //     .first()
        //     .map(|tx_in| tx_in.previous_output.is_null())
        //     .unwrap_or_default();

        // if is_coinbase {
        //     inscriptions.append(&mut self.flotsam);
        // }

        //inscriptions.sort_by_key(|flotsam| flotsam.offset);
        
        let clone_govna = inscriptions.clone().into_iter().peekable();
        let mut inscriptions = inscriptions.into_iter().peekable();

        let mut output_value = 0;
        for (vout, tx_out) in tx.output.iter().enumerate() {
            output_value += tx_out.value;

            // while let Some(flotsam) = inscriptions.peek() {
            //     if flotsam.offset >= end {
            //         break;
            //     }

            //     let new_satpoint = SatPoint {
            //         outpoint: OutPoint {
            //             txid,
            //             vout: vout.try_into().track_err()?,
            //         },
            //         offset: flotsam.offset - output_value,
            //     };

            //     self.update_inscription_location(
            //         input_sat_ranges,
            //         inscriptions.next().track_err()?,
            //         new_satpoint,
            //     )?;
            // }

            // output_value = end;

            // self.value_cache.write().insert(
            //     OutPoint {
            //         vout: vout.try_into().track_err()?,
            //         txid,
            //     },
            //     tx_out.value,
            // );
        }

        if false {
            for flotsam in inscriptions {
                let new_satpoint = SatPoint {
                    outpoint: OutPoint::null(),
                    offset: self.lost_sats + flotsam.offset - output_value,
                };
                self.update_inscription_location(input_sat_ranges, flotsam, new_satpoint)?;
            }

            Ok((
                self.reward - output_value,
                is_inscription,
            ))
        } else {
            // let reward = self.reward;
            // self.flotsam.extend(inscriptions.map(|flotsam| Flotsam {
            //     offset: reward + flotsam.offset,
            //     ..flotsam
            // }));
            // self.reward += input_value - output_value;
            Ok((
                0,
                is_inscription,
            ))
        }
    }

    fn update_inscription_location(
        &mut self,
        input_sat_ranges: Option<&VecDeque<(u128, u128)>>,
        flotsam: Flotsam,
        new_satpoint: SatPoint,
    ) -> Result<()> {
        let inscription_id = flotsam.inscription_id.store().track_err()?;

        match flotsam.origin {
            Origin::Old(old_satpoint) => {
                self.database.remove(&db_key!(
                    self.sat_to_inscription_id,
                    &old_satpoint.store().track_err()?
                ));
            }
            Origin::New(fee) => {
                self.database.put(
                    &db_key!(self.number_to_id, &self.next_number.to_be_bytes()),
                    inscription_id.as_slice(),
                );

                let mut sat = None;
                if let Some(input_sat_ranges) = input_sat_ranges {
                    let mut offset = 0;
                    for (start, end) in input_sat_ranges {
                        let size = end - start;
                        if offset + size > flotsam.offset as u128 {
                            let n = start + flotsam.offset as u128 - offset;
                            self.database.put(
                                &db_key!(self.sat_to_inscription_id, &n.to_be_bytes()),
                                inscription_id.as_slice(),
                            );
                            sat = Some(Sat(n));
                            break;
                        }
                        offset += size;
                    }
                }

                self.database.put(
                    &db_key!(self.id_to_entry, &inscription_id),
                    &tuple_to_bytes(
                        &InscriptionEntry {
                            fee,
                            height: self.height,
                            number: self.next_number,
                            sat,
                            timestamp: self.timestamp,
                        }
                        .store()
                        .track_err()?,
                    ),
                );

                self.next_number += 1;
            }
        }

        let new_satpoint = new_satpoint.store().track_err()?;

        self.database.put(
            &db_key!(self.sat_to_inscription_id, &new_satpoint),
            inscription_id.as_slice(),
        );
        self.database.put(
            &db_key!(self.id_to_satpoint, &inscription_id),
            new_satpoint.as_slice(),
        );

        Ok(())
    }
}

fn tuple_to_bytes(tup: &(u64, u64, u64, u128, u32)) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&tup.0.to_le_bytes());
    bytes.extend_from_slice(&tup.1.to_le_bytes());
    bytes.extend_from_slice(&tup.2.to_le_bytes());
    bytes.extend_from_slice(&tup.3.to_le_bytes());
    bytes.extend_from_slice(&tup.4.to_le_bytes());
    bytes
}
