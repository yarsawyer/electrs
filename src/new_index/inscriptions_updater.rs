use std::{collections::HashMap, convert::TryInto, iter::FromIterator, sync::Arc, time::Instant};

use crate::{
    db_key, inscription_entries::{
        index::{ADDRESS_TO_ORD_STATS, PARTIAL_TXID_TO_TXIDS, OUTPOINT_IS_INSCRIPTION}, inscription::Inscription, inscription::{
            InscriptionExtraData, InscriptionMeta, LastInscriptionNumber, OrdHistoryKey,
            OrdHistoryRow, OrdHistoryValue, ParsedInscription, PartialTxs, UserOrdStats,
        }, inscription_id::InscriptionId, Entry
    }, new_index::schema::TxOutRow, util::{bincode_util, errors::AsAnyhow, full_hash, HeaderEntry, ScriptToAddr}
};
use anyhow::{Ok, Result};
use bitcoin::{blockdata::block, consensus::Decodable, hashes::hex::ToHex, Address, OutPoint, Script, TxOut};
use bitcoin::{hashes::Hash, Transaction, Txid};
use itertools::Itertools;
use parking_lot::RwLock;
use rayon::iter::{FromParallelIterator, IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};

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
        &self,
        tx: Transaction,
        block_height: u32,
        is_temp: bool,
        cache: Option<Arc<RwLock<HashMap<Txid, Transaction>>>>,
        partials_cache: Option<Arc<RwLock<HashMap<Txid, Vec<Txid>>>>>,
    ) -> Result<u64> {
        // let txid = tx.txid();
        // let previous_txid = tx.input[0].previous_output.txid;
        // let tx_sat = tx.output.first().anyhow()?.value;
        // let prev_tx = tx.input.first().anyhow_as("No inputs :(")?.previous_output;
        
        // let mut to_temp_write = vec![];
        
        // if prev_tx.vout == 0 {
        //     if let Some(mut inscription_extra) = self
        //     .inscription_db
        //     .remove(&db_key!(OUTPOINT_IS_INSCRIPTION, &prev_tx.txid.into_inner()))
        //     .map(|x| InscriptionExtraData::from_raw(&x.to_vec()))
        //     .transpose()?
        //     {
        //         let old_owner = inscription_extra.owner.clone();
                
        //         // Work with old user
        //         let prev_history_value = {
        //             let old_row = OrdHistoryRow::new(
        //                 old_owner.clone(),
        //                 prev_tx.txid,
        //                 // Value hardcoded becouse its not needed
        //                 OrdHistoryValue {
        //                     value: tx_sat,
        //                     inscription_id: InscriptionId {
        //                         txid: inscription_extra.genesis,
        //                         index: 0,
        //                     },
        //                     inscription_number: 0,
        //                 },
        //             );
                    
        //             let prev_history_value = self
        //             .inscription_db
        //             .remove(&old_row.get_key())
        //             .map(|x| OrdHistoryRow::value_from_raw(&x))
        //             .anyhow_as("Failed to find OrdHistoryRow")?;
                    
        //             if is_temp {
        //                 to_temp_write.push(DBRow {
        //                     key: old_row.get_temp_db_key(block_height),
        //                     value: prev_history_value.get_raw(),
        //                 });
        //                 to_temp_write
        //                 .push(inscription_extra.to_temp_db_row(block_height, &previous_txid)?);
        //             }
                    
        //             prev_history_value
        //         };
                
        //         // Work with new user
        //         let ord_history = {
        //             let new_owner = tx
        //             .output
        //             .first()
        //             .and_then(|x| {
        //                 Address::from_script(
        //                     &x.script_pubkey,
        //                     bitcoin::network::constants::Network::Bitcoin,
        //                 )
        //             })
        //             .map(|x| x.to_string())
        //             .anyhow_as("No owner :(")?;
                    
        //             inscription_extra.owner = new_owner.clone();
        //             inscription_extra.block_height = block_height;
                    
        //             OrdHistoryRow::new(new_owner, txid, prev_history_value)
        //         };
                
        //         if is_temp {
        //             self.temp_db
        //             .write(to_temp_write, super::db::DBFlush::Disable);
        //         }
                
        //         self.inscription_db.write(
        //             vec![ord_history.into_row(), inscription_extra.to_db_row(&txid)?],
        //             super::db::DBFlush::Disable,
        //         );
                
        //         return Ok(0);
        //     };
        // }
        
        // let partial = PartialTxs {
        //     block_height,
        //     last_txid: previous_txid,
        //     txs: vec![],
        // };
        
        // let txs = {
        //     let txsids = {
        //         if let Some(v) = partials_cache
        //         .as_ref()
        //         .map(|x| x.read().get(&previous_txid).map(|x| x.clone()))
        //         .flatten()
        //         {
        //             v.clone()
        //         } else {
        //             if !is_temp {
        //                 vec![]
        //             } else {
        //                 match self.inscription_db.get(&partial.get_db_key()) {
        //                     None => vec![txid],
        //                     Some(partials) => {
        //                         PartialTxs::from_db(DBRow {
        //                             key: partial.get_db_key(),
        //                             value: partials,
        //                         })
        //                         .unwrap()
        //                         .txs
        //                     }
        //                 }
        //             }
        //         }
        //     };
            
        //     let mut txs = vec![];
        //     for txid in txsids {
        //         if let Some(v) = cache
        //         .as_ref()
        //         .map(|x| x.read().get(&txid).map(|x| x.clone()))
        //         .flatten()
        //         {
        //             txs.push(v.clone());
        //         } else if is_temp {
        //             let tx_result = self.tx_db.get(&db_key!("T", &txid.into_inner())).anyhow()?;
        //             let decoded =
        //             bitcoin::Transaction::consensus_decode(std::io::Cursor::new(tx_result))?;
        //             txs.push(decoded);
        //         }
        //     }
        //     txs.push(tx.clone());
        //     txs
        // };
        
        // match Inscription::from_transactions(txs.iter().collect_vec().as_slice()) {
        //     ParsedInscription::None => {
        //         if let Some(v) = &cache {
        //             v.write().remove(&txid);
        //         }
        //     }
            
        //     ParsedInscription::Partial => {
        //         if let Some(_) = partials_cache
        //         .as_ref()
        //         .map(|x| x.write().remove(&previous_txid))
        //         .flatten()
        //         {
        //             partials_cache
        //             .unwrap()
        //             .write()
        //             .insert(txid, txs.iter().map(|x| x.txid()).collect_vec());
        //         } else if is_temp {
        //             self.inscription_db.remove(&partial.get_db_key());
                    
        //             let row = PartialTxs {
        //                 block_height,
        //                 last_txid: txid,
        //                 txs: txs.into_iter().map(|x| x.txid()).collect_vec(),
        //             };
                    
        //             self.inscription_db
        //             .write(vec![row.to_db()?], super::db::DBFlush::Disable);
                    
        //             if is_temp {
        //                 self.temp_db.remove(&PartialTxs::get_temp_db_key(
        //                     block_height,
        //                     &partial.last_txid,
        //                 ));
        //                 self.temp_db
        //                 .write(vec![row.to_temp_db_row()?], super::db::DBFlush::Disable);
        //             }
        //         }
        //     }
            
        //     ParsedInscription::Complete(_inscription) => {
        //         if let Some(partials_cache) = &partials_cache {
        //             partials_cache.write().remove(&previous_txid);
        //         } else if is_temp {
        //             self.inscription_db.remove(&partial.get_db_key());
        //         }
                
        //         let og_inscription_id = InscriptionId {
        //             txid: Txid::from_slice(
        //                 &txs.first().anyhow_as("Partial txs vec is empty")?.txid(),
        //             )
        //             .anyhow()?,
        //             // TODO find correct index instead hardcode
        //             index: 0,
        //         };
                
        //         let genesis = txs[0].txid();
                
        //         let owner = tx
        //         .output
        //         .first()
        //         .and_then(|x| {
        //             Address::from_script(
        //                 &x.script_pubkey,
        //                 bitcoin::network::constants::Network::Bitcoin,
        //             )
        //         })
        //         .map(|x| x.to_string())
        //         .anyhow_as("No owner :(")?;
                
        //         let number: u64 = self
        //         .inscription_db
        //         .remove(&LastInscriptionNumber::get_db_key())
        //         .map(|x| u64::from_be_bytes(x.try_into().expect("Failed to convert")))
        //         .unwrap_or(0);
                
        //         let inscription_meta = InscriptionMeta::new(
        //             _inscription.content_type().anyhow()?.to_owned(),
        //             _inscription.content_length().anyhow()?,
        //             txs.last().anyhow()?.txid(),
        //             og_inscription_id.txid,
        //             number,
        //         );
                
        //         let new_row = OrdHistoryRow::new(
        //             owner.clone(),
        //             txid,
        //             OrdHistoryValue {
        //                 inscription_id: og_inscription_id,
        //                 inscription_number: number,
        //                 value: tx_sat,
        //             },
        //         );
                
        //         let new_inc_n = LastInscriptionNumber {
        //             height: block_height,
        //             number: number + 1,
        //         };
                
        //         let inscription_extra =
        //         InscriptionExtraData::new(genesis, owner.clone(), block_height);
                
        //         self.inscription_db.write(
        //             vec![
        //             new_row.into_row(),
        //             inscription_extra.to_db_row(&txid)?,
        //             inscription_meta.to_db_row()?,
        //             new_inc_n.to_db()?,
        //             ],
        //             super::db::DBFlush::Disable,
        //         );
                
        //         if is_temp {
        //             self.temp_db.remove(&PartialTxs::get_temp_db_key(
        //                 block_height,
        //                 &partial.last_txid,
        //             ));
        //             self.temp_db.write(
        //                 vec![new_inc_n.to_temp_db_row()?],
        //                 super::db::DBFlush::Disable,
        //             );
        //         } else {
        //             for i in txs.into_iter().rev() {
        //                 cache.as_ref().unwrap().write().remove(&i.txid());
        //             }
        //         }
        //     }
        // }
        // Ok(0)

        todo!()
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
        
    pub fn remove_blocks(&self, blocks: Vec<HeaderEntry>) -> anyhow::Result<()> {
        // let mut to_restore = vec![];
        
        // let min_height = blocks[0].height() as u32 - 1;
        
        // let last_inscription_number_key = LastInscriptionNumber::get_temp_db_key(min_height);
        // let last_number = self
        // .temp_db
        // .get(&last_inscription_number_key)
        // .map(|x| {
        //     LastInscriptionNumber::from_db(DBRow {
        //         key: last_inscription_number_key,
        //         value: x,
        //     })
        //     .unwrap()
        // })
        // .unwrap();
        
        // to_restore.push(last_number.to_db()?);
        
        // for block in blocks.into_iter().rev() {
        //     let block_height = block.height() as u32;
        //     self.remove_temp_data_orhpan(block_height)?;
            
        //     let txids: Vec<Txid> = {
        //         self.tx_db
        //         .get(&BlockRow::txids_key(full_hash(&block.hash()[..])))
        //         .map(|val| {
        //             bincode_util::deserialize_little(&val).expect("failed to parse block txids")
        //         })
        //         .unwrap()
        //     };
            
        //     for tx in txids.into_iter().rev() {
        //         let temp_extra_key = InscriptionExtraData::get_temp_db_key(block_height, &tx);
        //         let extra_key = InscriptionExtraData::get_db_key(&tx);
                
        //         let tx_result = self.tx_db.get(&db_key!("T", &tx.into_inner())).anyhow()?;
        //         let decoded =
        //         bitcoin::Transaction::consensus_decode(std::io::Cursor::new(tx_result))?;
                
        //         let history_row = OrdHistoryKey {
        //             address: decoded.output[0]
        //             .script_pubkey
        //             .to_address_str(crate::chain::Network::Bellscoin)
        //             .expect("SHIT"),
        //             code: OrdHistoryRow::CODE,
        //             txid: tx,
        //         };
                
        //         let history_row = OrdHistoryRow {
        //             key: history_row,
        //             value: OrdHistoryValue {
        //                 inscription_id: InscriptionId { index: 0, txid: tx },
        //                 inscription_number: 0,
        //                 value: 0,
        //             },
        //         };
        //         let temp_history_key = history_row.get_temp_db_key(block_height);
        //         let history_key = history_row.get_key();
                
        //         let meta_key = InscriptionMeta::get_db_key(tx)?;
                
        //         if let Some(v) = self.temp_db.remove(&temp_extra_key).map(|x| {
        //             InscriptionExtraData::from_temp_db(DBRow {
        //                 key: temp_extra_key,
        //                 value: x,
        //             })
        //             .unwrap()
        //         }) {
        //             to_restore.push(v.0.to_db_row(&v.1)?);
        //         } else if let Some(v) = self.temp_db.remove(&temp_history_key).map(|x| {
        //             OrdHistoryRow::from_temp_db_row(DBRow {
        //                 key: temp_history_key,
        //                 value: x,
        //             })
        //             .unwrap()
        //         }) {
        //             to_restore.push(v.0.into_row());
        //         } else if let Some(_) = self.inscription_db.remove(&meta_key).map(|x| {
        //             InscriptionMeta::from_raw(&x).expect("Failed to decode InscriptionMeta")
        //         }) {
        //         } else if let Some(_) = self.inscription_db.remove(&extra_key) {
        //         } else if let Some(_) = self.inscription_db.remove(&history_key) {
        //         }
        //     }
        // }
        
        // self.inscription_db
        // .write(to_restore, super::db::DBFlush::Disable);
        
        // Ok(())
        todo!()
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
    
pub struct IndexHandler<'a> {
    pub inscription_db: &'a DB,
    pub cached_partial: HashMap<Txid, Vec<(u32, usize, Transaction)>>,
    pub inscription_number: u64,
}
impl<'a> IndexHandler<'a> {
    fn get_owner(tx: &Transaction) -> anyhow::Result<String> {
        return tx
        .output
        .first()
        .and_then(|x| {
            Address::from_script(
                &x.script_pubkey,
                bitcoin::network::constants::Network::Bitcoin,
            )
        })
        .map(|x| x.to_string())
        .anyhow_as("No owner 😭");
    }
    fn try_move_inscription(&self, tx: &Transaction, height: u32) -> anyhow::Result<bool> {
        // let prev_tx = tx.input[0].previous_output;
        // let tx_sat = tx.output[0].value;
        // let txid = tx.txid();
        
        // if prev_tx.vout != 0 {
        //     return Ok(false);
        // }
        // let Some(mut inscription_extra) = self
        //     .inscription_db
        //     .remove(&db_key!(OUTPOINT_IS_INSCRIPTION, &prev_tx.txid.into_inner()))
        //     .map(|x| InscriptionExtraData::from_raw(&x.to_vec()))
        //     .transpose()?
        // else {
        //     return Ok(false);
        // };
        
        // let old_owner = inscription_extra.owner.clone();
        
        // // Work with old user
        // let prev_history_value = {
        //     let old_row = OrdHistoryRow::new(
        //         old_owner.clone(),
        //         prev_tx.txid,
        //         // Value hardcoded becouse its not needed
        //         OrdHistoryValue {
        //             value: tx_sat,
        //             inscription_id: InscriptionId {
        //                 txid: inscription_extra.genesis,
        //                 index: 0,
        //             },
        //             inscription_number: 0,
        //         },
        //     );
            
        //     self.inscription_db
        //     .remove(&old_row.get_key())
        //     .map(|x| OrdHistoryRow::value_from_raw(&x))
        //     .anyhow_as("Failed to find OrdHistoryRow")?
        // };
        
        // // Work with new user
        // let ord_history = {
        //     let new_owner = Self::get_owner(tx)?;
        //     inscription_extra.owner = new_owner.clone();
        //     inscription_extra.block_height = height;
        //     OrdHistoryRow::new(new_owner, txid, prev_history_value)
        // };
        
        // let new_inscription_meta = self
        // .inscription_db
        // .remove(&InscriptionMeta::get_db_key(inscription_extra.genesis)?)
        // .map(|x| InscriptionMeta::from_raw(&x))
        // .anyhow()?
        // .anyhow()?;
        
        // self.inscription_db.write(
        //     vec![
        //     ord_history.into_row(),
        //     inscription_extra.to_db_row(&txid)?,
        //     new_inscription_meta.to_db_row()?,
        //     ],
        //     super::db::DBFlush::Disable,
        // );
        
        // Ok(true)
        todo!()
    }
    
    pub fn try_parse_inscription(h: u32, txs: &[Transaction]) -> DigestedBlock {
        let mut partials: HashMap<Txid, Vec<(u32, usize, Transaction)>> = HashMap::new();
        let mut inscriptions = vec![];
        let mut rest = vec![];

        for (i, tx) in txs.iter().enumerate()  {
            match Inscription::from_transactions(&[tx]) {
                ParsedInscription::None => {
                    if let Some(mut chain) = partials.remove(&tx.input[0].previous_output.txid) {
                        chain.push((h, i, tx.clone()));
                        match Inscription::from_transactions(&chain.iter().map(|x| &x.2).collect_vec()) {
                            ParsedInscription::None => { 
                                chain.pop();
                                partials.insert(chain.last().unwrap().2.txid(), chain);
                            },
                            ParsedInscription::Partial => {
                                partials.insert(chain.last().unwrap().2.txid(), chain);
                            },
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
                                };
                                inscriptions.push((i, inscription_template));
                            },
                        }
                    }
                    else {
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
    pub fn handle_blocks(&mut self, blocks: &Vec<(u32, Vec<Transaction>)>) -> Vec<InscriptionTemplate> {
        let mut data = vec![];
        blocks.into_par_iter().map(|(h,txs)|{
            Self::try_parse_inscription(*h, txs)
        })
        .collect_into_vec(&mut data);
        data.sort_unstable_by_key(|x| x.height);
        
        let mut completed = vec![];
        
        for mut digested_block in data {
            self.cached_partial.extend(digested_block.partial);
            
            for (h, i, tx) in digested_block.rest {
                if let Some(mut chain) = self.cached_partial.remove(&tx.input[0].previous_output.txid) {
                    chain.push((h, i, tx.clone()));
                    match Inscription::from_transactions(&chain.iter().map(|x| &x.2).collect_vec()) {
                        ParsedInscription::None => { 
                            chain.pop();
                            self.cached_partial.insert(chain.last().unwrap().2.txid(), chain);
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
                            };
                            digested_block.completed.push((i, inscription_template));
                        }
                        
                    }
                    
                }
                
            }
            
            digested_block.completed.sort_unstable_by_key(|x| x.0 );
            for (_, mut inc) in  digested_block.completed { 
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
            
            let inscription_meta = InscriptionMeta::new(
                inc.content_type,
                inc.content_len,
                location,
                genesis,
                inc.inscription_number,
            );
            
            let new_row = OrdHistoryRow::new(
                inc.owner.clone(),
                location,
                OrdHistoryValue {
                    inscription_id: genesis,
                    inscription_number: inc.inscription_number,
                    value: inc.value,
                },
            );
            
            let inscription_extra = InscriptionExtraData::new(location, genesis, inc.owner, inc.height);

            to_write.push(new_row.into_row());
            to_write.push(inscription_extra.to_db_row(location)?);
            to_write.push(inscription_meta.to_db_row()?);
        }
   
        self.inscription_db.write(
            to_write,
            super::db::DBFlush::Disable,
        );
        
        Ok(())
    }
}

pub struct MoveIndexer<'a> {
    pub inscription_db: &'a DB,
    pub txstore_db: &'a DB,
}
impl<'a> MoveIndexer<'a> {
    pub fn get_address(script: &Script) -> String {
        Address::from_script(
            script,
            bitcoin::network::constants::Network::Bitcoin,
        ).unwrap().to_string()
    }
    pub fn load_txos(&self, txs: &[Transaction]) -> HashMap<OutPoint,TxOut> {
        let mut data = HashMap::new();
        for tx in txs {
            if tx.is_coin_base() { continue; }
            for txin in &tx.input{
                let txout = self.txstore_db.get(&TxOutRow::key(&txin.previous_output))
                    .map(|val| bitcoin::consensus::deserialize::<TxOut>(&val).expect("failed to parse TxOut"))
                    ;
                if txout.is_none() {
                    error!("{}",tx.txid());
                }
                data.insert(txin.previous_output, txout.unwrap());
            }
        }
        
        data
    }
    pub fn load_inscription(&self, txs: &[Transaction]) -> Vec<(OutPoint,InscriptionExtraData)> {
        let mut outpoints = vec![];
        for tx in txs {
            outpoints.extend(
                tx.input.iter()
                .map(|x| InscriptionExtraData::get_db_key(x.previous_output))
            );
        }

        self.inscription_db.db.multi_get(outpoints)
            .into_iter()
            .flatten()
            .flatten()
            .map(|x| InscriptionExtraData::from_raw(&x).unwrap())
            .map(|x| (x.location,x,))
            .collect_vec()
    }
    pub fn handle(&self, blocks: &Vec<(u32, Vec<Transaction>)>) -> HashMap<OutPoint,(InscriptionExtraData, Option<String>)> {
        let mut temp = vec![];
        let time = Instant::now();
        blocks.par_iter().map(|(_, txs)|
            (self.load_txos(txs), self.load_inscription(txs).into_iter().map(|(k,v)| (k,(v,None))).collect_vec())
        ).collect_into_vec(&mut temp);
        warn!("Data loading {}s",time.elapsed().as_secs_f64());
    
        let mut txos = HashMap::new();
        let mut inscriptions: HashMap<OutPoint,(InscriptionExtraData, Option<String>)> = HashMap::new();

        for (txouts, inc) in temp {
            txos.extend(txouts);
            inscriptions.extend(inc);
        }

        if inscriptions.is_empty() { return HashMap::new() }
        
        for (_, txs) in blocks {
            for tx in txs {
                // todo coinbase be backe
                if tx.is_coin_base() { continue; }
                // let fund: u64 = tx.input.iter().map(|x| txos.get(&x.previous_output).unwrap().value).sum();
                // let spent: u64 = tx.output.iter().map(|x| x.value).sum();
                // let fee = fund - spent;
                
                let flotsam = tx.output.iter().skip(1).fold(vec![tx.output[0].value], |mut acc, x| {
                    acc.push(acc.last().unwrap() + x.value);
                    acc
                });

                let mut offset = 0u64;
                for txin in &tx.input {
                    let Some(asd) = txos.get(&txin.previous_output)
                    else{ 
                        panic!("{} - {}",tx.txid(), txin.previous_output);
                    };
                    offset += asd.value;
                    
                    if let Some((inc,mut moved)) = inscriptions.remove(&txin.previous_output) {
                        // if inc.location.txid.to_hex() == *"e0263191b32aed4b8a4bdd936beb19109485d25d73759d5b064182dfc0b7ef23" {
                        //     dbg!(&tx);
                        //     dbg!(&flotsam);
                        //     dbg!(&offset);
                        // }
                        let Some((vout,_)) = flotsam.iter().find_position(|x| **x == offset)
                        else { 
                            inscriptions.insert(inc.location, (inc,moved));
                            continue; 
                        };
                        let location = OutPoint {
                            txid: tx.txid(),
                            vout: vout as u32,
                        };
                        let Some(_) = tx.output.get(vout) else {
                            panic!("{:#?} {}", tx, offset);
                        };
                        moved = Some(Self::get_address(&tx.output[vout].script_pubkey));
                        
                        inscriptions.insert(location, (inc,moved));
                    };
                }
            }
        }

        inscriptions
    }

    pub fn write_moves(&self, data: HashMap<OutPoint,(InscriptionExtraData, Option<String>)>) -> anyhow::Result<()> {
        let mut to_write = vec![];

        for (new_location,(mut inc, new_owner)) in data {
            if let Some(new_owner) = new_owner {
                let old_location = inc.location;
                let old_owner = inc.owner;

                inc.owner = new_owner.clone();
                inc.location = new_location;

                 // Work with old user
                let prev_history_value = {
                    let old_row = OrdHistoryRow::new(
                        old_owner.clone(),
                        old_location,
                        // Value hardcoded becouse its not needed
                        OrdHistoryValue {
                            value: 0,
                            inscription_id: inc.genesis,
                            inscription_number: 0,
                        },
                    );
                    
                    self.inscription_db
                        .remove(&old_row.get_key())
                        .map(|x| OrdHistoryRow::value_from_raw(&x))
                        .anyhow_as("Failed to find OrdHistoryRow")?
                };

               // Work with new user
                let new_ord_history = OrdHistoryRow::new(new_owner.clone(), new_location, prev_history_value);
                
                let mut new_inscription_meta = self
                    .inscription_db
                    .remove(&InscriptionMeta::get_db_key(inc.genesis)?)
                    .map(|x| InscriptionMeta::from_raw(&x))
                    .anyhow()?
                    .anyhow()?;
                new_inscription_meta.location = new_location;
                
                to_write.push(new_ord_history.into_row());
                to_write.push(inc.to_db_row(inc.location)?);
                to_write.push(new_inscription_meta.to_db_row()?);
               
            }
        }
   
        self.inscription_db.write(
            to_write,
            super::db::DBFlush::Disable,
        );
        
        Ok(())
    }
}

pub struct DigestedBlock {
    pub height: u32,
    pub partial: HashMap<Txid, Vec<(u32, usize,Transaction)>>,
    pub completed: Vec<(usize,InscriptionTemplate)>,
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
}