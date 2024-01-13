use std::sync::Arc;

use crate::{
    db_key,
    inscription_entries::{
        entry::Entry,
        height::Height,
        index::{INSCRIPTION_ID_TO_META, INSCRIPTION_ID_TO_TXIDS, TXID_IS_INSCRIPTION},
        inscription::Inscription,
        inscription::{InscriptionMeta, ParsedInscription},
        inscription_id::InscriptionId,
        SatPoint,
    },
    util::errors::AsAnyhow,
};
use anyhow::Result;
use bitcoin::consensus::Decodable;
use bitcoin::{hashes::Hash, Transaction, Txid};

use super::{Store, DB};

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
    height: u64,
    partial_txid_to_txids: &'a str,
    reward: u64,
    database: &'a DB,
}

impl<'a> InscriptionUpdater<'a> {
    pub(crate) fn new(
        height: u64,
        partial_txid_to_txids: &'a str,
        database: &'a DB,
    ) -> Result<Self> {
        Ok(Self {
            height,
            partial_txid_to_txids,
            reward: Height(height).subsidy(),
            database,
        })
    }

    pub(crate) fn index_transaction_inscriptions(
        &mut self,
        store: Arc<Store>,
        tx: &Transaction,
        txid: Txid,
    ) -> Result<u64> {
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

                    let tx_result = store.txstore_db().get(&db_key!("T", txid)).anyhow()?;
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
                let prev_tx = tx
                    .input
                    .first()
                    .anyhow_as("No inputs :(")?
                    .previous_output
                    .txid;

                if let Some(shit) = store
                    .inscription_db()
                    .remove(&db_key!(TXID_IS_INSCRIPTION, &prev_tx.into_inner()))
                {
                    //info!("Ord was move from:{:?} to:{:?}", prev_tx.to_hex(), txid.to_hex() );
                    store
                        .inscription_db()
                        .put(&db_key!(TXID_IS_INSCRIPTION, &txid.into_inner()), &shit)
                };
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
                store.inscription_db().put(
                    &db_key!(
                        INSCRIPTION_ID_TO_TXIDS,
                        &og_inscription_id.store().anyhow()?
                    ),
                    &txids_vec,
                );

                store.inscription_db().put(
                    &db_key!(TXID_IS_INSCRIPTION, &txid.into_inner()),
                    &txs.first().anyhow_as("BIG COCKS")?.txid().into_inner(),
                );

                let inscription_meta = InscriptionMeta::new(
                    _inscription.content_type().anyhow()?.to_owned(),
                    _inscription.content_length().anyhow()?,
                    txs.last().anyhow()?.txid(),
                    og_inscription_id.txid,
                );

                store.inscription_db().put(
                    &db_key!(INSCRIPTION_ID_TO_META, &og_inscription_id.store().anyhow()?),
                    &inscription_meta.into_bytes()?,
                );
            }
        }
        Ok(0)
    }
}
