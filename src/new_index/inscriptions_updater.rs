use std::{
    collections::{HashMap, VecDeque},
    convert::TryInto,
};

use anyhow::{anyhow, Result};
use bitcoin::consensus::Decodable;
use bitcoin::{hashes::Hash, OutPoint, Transaction, Txid};
use tokio::sync::mpsc::Receiver;

use crate::{
    db_key,
    inscription_entries::{
        entry::{Entry, InscriptionEntry},
        height::Height,
        index::Index,
        inscription::Inscription,
        inscription::ParsedInscription,
        inscription_id::InscriptionId,
        Sat, SatPoint,
    },
};

use super::DB;

pub(super) struct Flotsam {
    inscription_id: InscriptionId,
    offset: u64,
    origin: Origin,
}

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
    value_receiver: &'a mut Receiver<u64>,
    id_to_entry: &'a str,
    lost_sats: u64,
    next_number: u64,
    number_to_id: &'a str,
    outpoint_to_value: &'a str,
    reward: u64,
    sat_to_inscription_id: &'a str,
    satpoint_to_id: &'a str,
    timestamp: u32,
    value_cache: &'a mut HashMap<OutPoint, u64>,
    database: &'a DB,
}

impl<'a> InscriptionUpdater<'a> {
    pub(crate) fn new(
        height: u64,
        id_to_satpoint: &'a str,
        id_to_txids: &'a str,
        txid_to_tx: &'a str,
        partial_txid_to_txids: &'a str,
        value_receiver: &'a mut Receiver<u64>,
        id_to_entry: &'a str,
        lost_sats: u64,
        number_to_id: &'a str,
        outpoint_to_value: &'a str,
        sat_to_inscription_id: &'a str,
        satpoint_to_id: &'a str,
        timestamp: u32,
        value_cache: &'a mut HashMap<OutPoint, u64>,
        database: &DB,
    ) -> Result<Self> {
        let next_number = database
            .iter_scan(&[])
            .map(|dbrow| u64::from_le_bytes(dbrow.value.try_into().unwrap()) + 1)
            .next()
            .unwrap_or(0);

        Ok(Self {
            flotsam: Vec::new(),
            height,
            id_to_satpoint,
            id_to_txids,
            txid_to_tx,
            partial_txid_to_txids,
            value_receiver,
            id_to_entry,
            lost_sats,
            next_number,
            number_to_id,
            outpoint_to_value,
            reward: Height(height).subsidy(),
            sat_to_inscription_id,
            satpoint_to_id,
            timestamp,
            value_cache,
            database,
        })
    }

    pub(crate) fn index_transaction_inscriptions(
        &mut self,
        tx: &Transaction,
        txid: Txid,
        input_sat_ranges: Option<&VecDeque<(u128, u128)>>,
    ) -> Result<u64> {
        let mut inscriptions = Vec::new();

        let mut input_value = 0;
        for tx_in in &tx.input {
            if tx_in.previous_output.is_null() {
                input_value += Height(self.height).subsidy();
            } else {
                for (old_satpoint, inscription_id) in
                    Index::inscriptions_on_output(&self.database, tx_in.previous_output).unwrap()
                {
                    inscriptions.push(Flotsam {
                        offset: input_value + old_satpoint.offset,
                        inscription_id,
                        origin: Origin::Old(old_satpoint),
                    });
                }

                input_value += if let Some(value) = self.value_cache.remove(&tx_in.previous_output)
                {
                    value
                } else if let Some(value) = self.database.remove(self.outpoint_to_value.as_bytes())
                {
                    u64::from_le_bytes(value.try_into().unwrap())
                } else {
                    self.value_receiver.blocking_recv().ok_or_else(|| {
                        anyhow!(
                            "failed to get transaction for {}",
                            tx_in.previous_output.txid
                        )
                    })?
                }
            }
        }

        if inscriptions.iter().all(|flotsam| flotsam.offset != 0) {
            let previous_txid = tx.input[0].previous_output.txid;
            let previous_txid_bytes: [u8; 32] = previous_txid.into_inner();
            let mut txids_vec = vec![];

            let txs = match self.database.get(db_key!(
                self.partial_txid_to_txids,
                String::from_utf8(previous_txid_bytes.to_vec()).unwrap()
            )) {
                Some(partial_txids) => {
                    let txids = partial_txids;
                    let mut txs = vec![];
                    txids_vec = txids.to_vec();
                    for i in 0..txids.len() / 32 {
                        let txid = &txids[i * 32..i * 32 + 32];
                        let tx_result = self.database.get(db_key!(
                            self.txid_to_tx,
                            String::from_utf8(txid.to_vec()).unwrap()
                        ));
                        let tx_result = tx_result.unwrap();
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

            match Inscription::from_transactions(txs) {
                ParsedInscription::None => {
                    // todo: clean up db
                }

                ParsedInscription::Partial => {
                    let mut txid_vec = txid.into_inner().to_vec();
                    txids_vec.append(&mut txid_vec);

                    self.database.remove(db_key!(
                        self.partial_txid_to_txids,
                        String::from_utf8(previous_txid_bytes.to_vec()).unwrap()
                    ));
                    self.partial_txid_to_txids
                        .remove(&previous_txid_bytes.as_slice())?;
                    self.partial_txid_to_txids
                        .insert(&txid.into_inner().as_slice(), txids_vec.as_slice())?;

                    let mut tx_buf = vec![];
                    tx.consensus_encode(&mut tx_buf)?;
                    self.txid_to_tx
                        .insert(&txid.into_inner().as_slice(), tx_buf.as_slice())?;
                }

                ParsedInscription::Complete(_inscription) => {
                    self.partial_txid_to_txids
                        .remove(&previous_txid_bytes.as_slice())?;

                    let mut tx_buf = vec![];
                    tx.consensus_encode(&mut tx_buf)?;
                    self.txid_to_tx
                        .insert(&txid.into_inner().as_slice(), tx_buf.as_slice())?;

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
                    self.id_to_txids
                        .insert(&inscription_id, txids_vec.as_slice())?;

                    let og_inscription_id = InscriptionId {
                        txid: Txid::from_slice(&txids_vec[0..32]).unwrap(),
                        index: 0,
                    };

                    inscriptions.push(Flotsam {
                        inscription_id: og_inscription_id,
                        offset: 0,
                        origin: Origin::New(
                            input_value - tx.output.iter().map(|txout| txout.value).sum::<u64>(),
                        ),
                    });
                }
            }
        };

        let is_coinbase = tx
            .input
            .first()
            .map(|tx_in| tx_in.previous_output.is_null())
            .unwrap_or_default();

        if is_coinbase {
            inscriptions.append(&mut self.flotsam);
        }

        inscriptions.sort_by_key(|flotsam| flotsam.offset);
        let mut inscriptions = inscriptions.into_iter().peekable();

        let mut output_value = 0;
        for (vout, tx_out) in tx.output.iter().enumerate() {
            let end = output_value + tx_out.value;

            while let Some(flotsam) = inscriptions.peek() {
                if flotsam.offset >= end {
                    break;
                }

                let new_satpoint = SatPoint {
                    outpoint: OutPoint {
                        txid,
                        vout: vout.try_into().unwrap(),
                    },
                    offset: flotsam.offset - output_value,
                };

                self.update_inscription_location(
                    input_sat_ranges,
                    inscriptions.next().unwrap(),
                    new_satpoint,
                )?;
            }

            output_value = end;

            self.value_cache.insert(
                OutPoint {
                    vout: vout.try_into().unwrap(),
                    txid,
                },
                tx_out.value,
            );
        }

        if is_coinbase {
            for flotsam in inscriptions {
                let new_satpoint = SatPoint {
                    outpoint: OutPoint::null(),
                    offset: self.lost_sats + flotsam.offset - output_value,
                };
                self.update_inscription_location(input_sat_ranges, flotsam, new_satpoint)?;
            }

            Ok(self.reward - output_value)
        } else {
            self.flotsam.extend(inscriptions.map(|flotsam| Flotsam {
                offset: self.reward + flotsam.offset,
                ..flotsam
            }));
            self.reward += input_value - output_value;
            Ok(0)
        }
    }

    fn update_inscription_location(
        &mut self,
        input_sat_ranges: Option<&VecDeque<(u128, u128)>>,
        flotsam: Flotsam,
        new_satpoint: SatPoint,
    ) -> Result<()> {
        let inscription_id = flotsam.inscription_id.store();

        match flotsam.origin {
            Origin::Old(old_satpoint) => {
                self.satpoint_to_id.remove(&old_satpoint.store())?;
            }
            Origin::New(fee) => {
                self.number_to_id
                    .insert(&self.next_number, &inscription_id)?;

                let mut sat = None;
                if let Some(input_sat_ranges) = input_sat_ranges {
                    let mut offset = 0;
                    for (start, end) in input_sat_ranges {
                        let size = end - start;
                        if offset + size > flotsam.offset as u128 {
                            let n = start + flotsam.offset as u128 - offset;
                            self.sat_to_inscription_id.insert(&n, &inscription_id)?;
                            sat = Some(Sat(n));
                            break;
                        }
                        offset += size;
                    }
                }

                self.id_to_entry.insert(
                    &inscription_id,
                    &InscriptionEntry {
                        fee,
                        height: self.height,
                        number: self.next_number,
                        sat,
                        timestamp: self.timestamp,
                    }
                    .store(),
                )?;

                self.next_number += 1;
            }
        }

        let new_satpoint = new_satpoint.store();

        self.satpoint_to_id.insert(&new_satpoint, &inscription_id)?;
        self.id_to_satpoint.insert(&inscription_id, &new_satpoint)?;

        Ok(())
    }
}
