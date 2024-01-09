use super::{inscriptions_updater::InscriptionUpdater, Store};
use crate::{
    db_key,
    inscription_entries::{
        entry::SatRange,
        index::{
            Statistic, ID_TO_ENTRY, INSCRIPTION_ID_TO_SATPOINT, INSCRIPTION_ID_TO_TXIDS,
            INSCRIPTION_TXID_TO_TX, NUMBER_TO_ID, OUTPOINT_TO_SATRANGES, OUTPOINT_TO_VALUE,
            PARTIAL_TXID_TO_TXIDS, SAT_TO_INSCRIPTION_ID, SAT_TO_SATPOINT, STATISTIC_TO_COUNT, HEIGHT_TO_BLOCK_HASH,
        },
        Entry, Height, OutPointValue, Sat, SatPoint,
    },
    util::errors::AsAnyhow,
};
use bitcoin::{Block, BlockHeader, OutPoint, Transaction, Txid};
use std::convert::TryFrom;
use std::convert::TryInto;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{atomic, Arc},
    thread,
    time::{Instant, SystemTime},
};
use tokio::sync::mpsc::{error::TryRecvError, Receiver, Sender};

struct BlockData {
    header: BlockHeader,
    txdata: Vec<(Transaction, Txid)>,
}

impl From<Block> for BlockData {
    fn from(block: Block) -> Self {
        BlockData {
            header: block.header,
            txdata: block
                .txdata
                .into_iter()
                .map(|transaction| {
                    let txid = transaction.txid();
                    (transaction, txid)
                })
                .collect(),
        }
    }
}

pub(crate) struct Updater {
    range_cache: HashMap<OutPointValue, Vec<u8>>,
    height: u64,
    index_sats: bool,
    sat_ranges_since_flush: u64,
    outputs_cached: u64,
    outputs_inserted_since_flush: u64,
}

impl Updater {
    fn index_block(
        &mut self,
        store: Arc<Store>,
        outpoint_sender: &mut Sender<OutPoint>,
        value_receiver: &mut Receiver<u64>,
        block: BlockData,
        value_cache: &mut HashMap<OutPoint, u64>,
    ) -> anyhow::Result<()> {
        // If value_receiver still has values something went wrong with the last block
        // Could be an assert, shouldn't recover from this and commit the last block
        let Err(TryRecvError::Empty) = value_receiver.try_recv() else {
            anyhow::bail!("Previous block did not consume all input values");
        };

        // TODO SHIT HERE
        let first_inscription_height = 22490;
        let index_inscriptions = self.height >= first_inscription_height;

        if index_inscriptions {
            // Send all missing input outpoints to be fetched right away
            let txids = block
                .txdata
                .iter()
                .map(|(_, txid)| txid)
                .collect::<HashSet<_>>();
            for (tx, _) in &block.txdata {
                for input in &tx.input {
                    let prev_output = input.previous_output;
                    // We don't need coinbase input value
                    if prev_output.is_null() {
                        continue;
                    }
                    // We don't need input values from txs earlier in the block, since they'll be added to value_cache
                    // when the tx is indexed
                    if txids.contains(&prev_output.txid) {
                        continue;
                    }
                    // We don't need input values we already have in our value_cache from earlier blocks
                    if value_cache.contains_key(&prev_output) {
                        continue;
                    }
                    // We don't need input values we already have in our outpoint_to_value table from earlier blocks that
                    // were committed to db already
                    let outpoint_to_value = store
                        .inscription_db()
                        .get(&db_key!(OUTPOINT_TO_VALUE, &prev_output.store()?));
                    if outpoint_to_value.is_some() {
                        continue;
                    }
                    // We don't know the value of this tx input. Send this outpoint to background thread to be fetched
                    outpoint_sender.blocking_send(prev_output)?;
                }
            }
        }

        let mut lost_sats = u64::from_be_bytes(
            store
                .inscription_db()
                .get(&db_key!(
                    STATISTIC_TO_COUNT,
                    &Statistic::LostSats.key().to_be_bytes()
                ))
                .unwrap_or(vec![0_u8; 8])
                .try_into()
                .anyhow()?,
        );

        let mut inscription_updater = InscriptionUpdater::new(
            22490,
            INSCRIPTION_ID_TO_SATPOINT,
            INSCRIPTION_ID_TO_TXIDS,
            INSCRIPTION_TXID_TO_TX,
            PARTIAL_TXID_TO_TXIDS,
            ID_TO_ENTRY,
            0,
            NUMBER_TO_ID,
            OUTPOINT_TO_VALUE,
            SAT_TO_INSCRIPTION_ID,
            0,
            store.outpoint_cache(),
            store.inscription_db(),
        )
        .anyhow()?;

        if self.index_sats {
            let mut coinbase_inputs = VecDeque::new();

            let h = Height(self.height);
            if h.subsidy() > 0 {
                let start = h.starting_sat();
                coinbase_inputs.push_front((start.n(), (start + h.subsidy() as u128).n()));
                self.sat_ranges_since_flush += 1;
            }

            for (tx_offset, (tx, txid)) in block.txdata.iter().enumerate().skip(1) {
                log::trace!("Indexing transaction {tx_offset}…");

                let mut input_sat_ranges = VecDeque::new();

                for input in &tx.input {
                    let key = input.previous_output.store()?;

                    let sat_ranges = match self.range_cache.remove(&key) {
                        Some(sat_ranges) => {
                            self.outputs_cached += 1;
                            sat_ranges
                        }
                        None => store
                            .inscription_db()
                            .remove(&db_key!(OUTPOINT_TO_SATRANGES, &key))
                            .anyhow_as(format!(
                                "Could not find outpoint {} in index",
                                input.previous_output
                            ))?,
                    };

                    for chunk in sat_ranges.chunks_exact(24) {
                        input_sat_ranges.push_back(SatRange::load(chunk.try_into()?)?);
                    }
                }

                self.index_transaction_sats(
                    store.clone(),
                    tx,
                    *txid,
                    &mut input_sat_ranges,
                    &mut inscription_updater,
                    index_inscriptions,
                )?;

                coinbase_inputs.extend(input_sat_ranges);
            }

            if let Some((tx, txid)) = block.txdata.get(0) {
                self.index_transaction_sats(
                    store.clone(),
                    tx,
                    *txid,
                    &mut coinbase_inputs,
                    &mut inscription_updater,
                    index_inscriptions,
                )?;
            }

            if !coinbase_inputs.is_empty() {
                let mut lost_sat_ranges = store
                    .inscription_db()
                    .remove(&db_key!(OUTPOINT_TO_SATRANGES, &OutPoint::null().store()?))
                    .unwrap_or_default();

                for (start, end) in coinbase_inputs {
                    if !Sat(start).is_common() {
                        store.inscription_db().put(
                            &db_key!(SAT_TO_SATPOINT, &start.to_be_bytes()),
                            &SatPoint {
                                outpoint: OutPoint::null(),
                                offset: lost_sats,
                            }
                            .store()?,
                        );
                    }

                    lost_sat_ranges.extend_from_slice(&(start, end).store()?);
                    lost_sats += u64::try_from(end - start).anyhow()?;
                }

                store.inscription_db().put(
                    &db_key!(OUTPOINT_TO_SATRANGES, &OutPoint::null().store()?),
                    &lost_sat_ranges,
                );
            }
        } else {
            for (tx, txid) in block.txdata.iter().skip(1).chain(block.txdata.first()) {
                lost_sats += inscription_updater.index_transaction_inscriptions(tx, *txid, None)?;
            }
        }

        store
			.inscription_db()
			.put(
				&db_key!(STATISTIC_TO_COUNT, &Statistic::LostSats.key().to_be_bytes()),
				&lost_sats.to_be_bytes(),
        	);

		store
			.inscription_db()
			.put(
				&db_key!(HEIGHT_TO_BLOCK_HASH, &self.height.to_be_bytes()),
				&block.header.block_hash().store()?
			);

        self.height += 1;

        Ok(())
    }

    fn index_transaction_sats(
        &mut self,
        store: Arc<Store>,
        tx: &Transaction,
        txid: Txid,
        input_sat_ranges: &mut VecDeque<(u128, u128)>,
        inscription_updater: &mut InscriptionUpdater,
        index_inscriptions: bool,
    ) -> anyhow::Result<()> {
        if index_inscriptions {
            inscription_updater.index_transaction_inscriptions(tx, txid, Some(input_sat_ranges))?;
        }

        for (vout, output) in tx.output.iter().enumerate() {
            let outpoint = OutPoint {
                vout: vout.try_into().anyhow()?,
                txid,
            };
            let mut sats = Vec::new();

            let mut remaining = output.value;
            while remaining > 0 {
                let range = input_sat_ranges.pop_front().ok_or_else(|| {
                    anyhow::anyhow!("insufficient inputs for transaction outputs")
                })?;

                if !Sat(range.0).is_common() {
                    store.inscription_db().put(
                        &db_key!(SAT_TO_SATPOINT, &range.0.to_be_bytes()),
                        &SatPoint {
                            outpoint,
                            offset: output.value - remaining,
                        }
                        .store()?,
                    );
                }

                let count = u64::try_from(range.1 - range.0).anyhow()?;

                let assigned = if count > remaining {
                    self.sat_ranges_since_flush += 1;
                    let middle = range.0 + remaining as u128;
                    input_sat_ranges.push_front((middle, range.1));
                    (range.0, middle)
                } else {
                    range
                };

                sats.extend_from_slice(&assigned.store()?);

                remaining -= u64::try_from(assigned.1 - assigned.0).anyhow()?;
            }

            self.range_cache.insert(outpoint.store()?, sats);
            self.outputs_inserted_since_flush += 1;
        }

        Ok(())
    }
}
