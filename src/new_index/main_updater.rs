use std::collections::HashMap;

use bitcoin::{consensus::Decodable, BlockHash, OutPoint, Transaction, Txid};
use itertools::Itertools;
use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};

use crate::{
    inscription_entries::{
        inscription::{
            InscriptionExtraData, LastInscriptionNumber, Location, OrdHistoryRow, OrdHistoryValue,
            PartialTxs, UserOrdStats,
        },
        InscriptionId, ParsedInscription,
    },
    util::{bincode_util, errors::AsAnyhow, full_hash},
    Inscription,
};

use super::{
    schema::{BlockRow, TxRow},
    temp_updater::{get_owner, InscriptionTemplate},
    token::TokenCache,
    Store,
};

pub struct IndexHandler<'a> {
    pub store: &'a Store,
    pub cached_partial: HashMap<OutPoint, Vec<(u32, usize, Transaction)>>,
    pub inscription_number: u64,
}
impl<'a> IndexHandler<'a> {
    pub fn try_parse_inscription(h: u32, txs: &[Transaction]) -> DigestedBlock {
        let mut partials: HashMap<OutPoint, Vec<(u32, usize, Transaction)>> = HashMap::new();
        let mut inscriptions = vec![];
        let mut rest = vec![];
        let mut token_cache = TokenCache::default();

        for (i, tx) in txs.into_iter().enumerate() {
            if !Self::parse_inscriptions(
                tx,
                h,
                i,
                &mut partials,
                &mut inscriptions,
                &mut token_cache,
            ) {
                rest.push((h, i, tx.clone()));
            }
        }

        DigestedBlock {
            height: h,
            partial_inscription: partials,
            completed_inscription: inscriptions,
            rest,
            token_cache,
        }
    }

    pub fn handle_blocks(
        &mut self,
        blocks: &Vec<(u32, Vec<Transaction>)>,
        token_cache: &mut TokenCache,
    ) -> Vec<InscriptionTemplate> {
        let mut data = vec![];
        blocks
            .into_par_iter()
            .map(|(h, txs)| Self::try_parse_inscription(*h, txs))
            .collect_into_vec(&mut data);
        data.sort_unstable_by_key(|x| x.height);

        let mut completed = vec![];

        for mut digested_block in data {
            self.cached_partial
                .extend(digested_block.partial_inscription);
            token_cache.extend(digested_block.token_cache);

            for (height, index, tx) in digested_block.rest {
                Self::parse_inscriptions(
                    &tx,
                    height,
                    index,
                    &mut self.cached_partial,
                    &mut digested_block.completed_inscription,
                    token_cache,
                );
            }

            digested_block
                .completed_inscription
                .sort_unstable_by_key(|x| (x.1.height, x.0));

            for (_, mut inc) in digested_block.completed_inscription {
                inc.inscription_number = self.inscription_number;

                self.inscription_number += 1;
                completed.push(inc);
            }
        }

        completed
    }

    fn parse_inscriptions(
        tx: &Transaction,
        height: u32,
        idx: usize,
        cache: &mut HashMap<OutPoint, Vec<(u32, usize, Transaction)>>,
        inscriptions: &mut Vec<(usize, InscriptionTemplate)>,
        token_cache: &mut TokenCache,
    ) -> bool {
        let mut chain = cache
            .remove(&tx.input[0].previous_output)
            .unwrap_or_default();

        chain.push((height, idx, tx.clone()));

        match Inscription::from_transactions(&chain.iter().map(|x| &x.2).collect_vec()) {
            ParsedInscription::None => false,
            ParsedInscription::Partial => {
                cache.insert(
                    OutPoint {
                        txid: tx.txid(),
                        vout: 0,
                    },
                    chain,
                );
                true
            }
            ParsedInscription::Complete(inscription) => {
                let outpoint = OutPoint {
                    txid: tx.txid(),
                    vout: 0,
                };
                let genesis = OutPoint {
                    txid: chain.first().unwrap().2.txid(),
                    vout: 0,
                };
                let content_type = inscription.content_type().unwrap().to_owned();
                let content_len = inscription.content_length().unwrap();
                let content = inscription.into_body().unwrap();
                let owner = get_owner(tx, 0).unwrap();

                token_cache.parse_token_action(
                    &content_type,
                    &content,
                    height,
                    idx,
                    owner.clone(),
                    genesis,
                    outpoint,
                    None,
                );

                let inscription_template = InscriptionTemplate {
                    genesis,
                    location: Location {
                        offset: 0,
                        outpoint,
                    },
                    content_type,
                    content_len,
                    content,
                    owner,
                    inscription_number: 0,
                    height,
                    value: tx.output[0].value,
                };
                inscriptions.push((idx, inscription_template));

                true
            }
        }
    }

    pub fn write_inscription(&self, data: &Vec<InscriptionTemplate>) -> anyhow::Result<()> {
        let mut to_write = vec![];

        let mut stats_cache: HashMap<_, _> = self
            .store
            .inscription_db()
            .db
            .multi_get(
                data.iter()
                    .map(|x| UserOrdStats::get_db_key(&x.owner).unwrap()),
            )
            .into_iter()
            .flatten()
            .map(|x| x.map(|x| UserOrdStats::from_raw(&x).unwrap()))
            .zip(data.iter())
            .map(|x| {
                let owner = x.1.owner.clone();

                x.0.map(|z| (owner, z))
            })
            .flatten()
            .collect();

        for inc in data {
            let genesis = inc.genesis;
            let location = inc.location.clone();

            let new_row = OrdHistoryRow::new(
                inc.owner.clone(),
                location.clone(),
                OrdHistoryValue {
                    inscription_id: InscriptionId {
                        txid: genesis.txid,
                        index: genesis.vout,
                    },
                    inscription_number: inc.inscription_number,
                },
            );

            let inscription_extra = InscriptionExtraData::new(
                location,
                inc.owner.clone(),
                inc.height,
                inc.content_type.clone(),
                inc.content_len,
                inc.value,
            );

            if let Some(v) = stats_cache.get_mut(&inc.owner) {
                v.amount += inc.value;
                v.count += 1;
            }

            to_write.push(new_row.to_db_row());
            to_write.push(inscription_extra.to_db_row()?);
        }

        to_write.extend(
            stats_cache
                .into_iter()
                .map(|(k, v)| v.to_db_row(&k).unwrap()),
        );

        self.store
            .inscription_db()
            .write(to_write, super::db::DBFlush::Enable);

        Ok(())
    }

    pub fn write_partials(&mut self) -> anyhow::Result<()> {
        if !self.cached_partial.is_empty() {
            let to_write = self
                .cached_partial
                .iter()
                .map(|(last_outpoint, txs)| {
                    PartialTxs {
                        block_height: txs[0].0,
                        last_outpoint: *last_outpoint,
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
        let last_number = LastInscriptionNumber::new(self.inscription_number);

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

pub struct DigestedBlock {
    pub height: u32,
    pub partial_inscription: HashMap<OutPoint, Vec<(u32, usize, Transaction)>>,
    pub completed_inscription: Vec<(usize, InscriptionTemplate)>,
    pub rest: Vec<(u32, usize, Transaction)>,
    pub token_cache: TokenCache,
}
