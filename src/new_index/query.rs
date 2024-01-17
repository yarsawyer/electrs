use rayon::prelude::*;

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::chain::{Network, OutPoint, Transaction, TxOut, Txid};
use crate::config::Config;
use crate::daemon::Daemon;
use crate::errors::*;
use crate::new_index::{ChainQuery, Mempool, ScriptStats, SpendingInput, Utxo};
use crate::util::{is_spendable, BlockId, Bytes, TransactionStatus};

use super::exchange_data::ExchangeData;
use super::schema::OrdsSearcher;

const FEE_ESTIMATES_TTL: u64 = 60; // seconds

const CONF_TARGETS: [u16; 28] = [
    1u16, 2u16, 3u16, 4u16, 5u16, 6u16, 7u16, 8u16, 9u16, 10u16, 11u16, 12u16, 13u16, 14u16, 15u16,
    16u16, 17u16, 18u16, 19u16, 20u16, 21u16, 22u16, 23u16, 24u16, 25u16, 144u16, 504u16, 1008u16,
];

pub struct Query {
    chain: Arc<ChainQuery>, // TODO: should be used as read-only
    mempool: Arc<parking_lot::RwLock<Mempool>>,
    daemon: Arc<Daemon>,
    config: Arc<Config>,
    pub exchange_data: Arc<parking_lot::Mutex<ExchangeData>>,
    cached_estimates: parking_lot::RwLock<(HashMap<u16, f64>, Option<Instant>)>,
    cached_relayfee: parking_lot::RwLock<Option<f64>>,
}

impl Query {
    pub fn new(
        chain: Arc<ChainQuery>,
        mempool: Arc<parking_lot::RwLock<Mempool>>,
        daemon: Arc<Daemon>,
        config: Arc<Config>,
        exchange_data: Arc<parking_lot::Mutex<ExchangeData>>,
    ) -> Self {
        Query {
            chain,
            mempool,
            daemon,
            config,
            cached_estimates: parking_lot::RwLock::new((HashMap::new(), None)),
            cached_relayfee: parking_lot::RwLock::new(None),
            exchange_data,
        }
    }

    pub fn chain(&self) -> &ChainQuery {
        &self.chain
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn network(&self) -> Network {
        self.config.network_type
    }

    pub fn mempool(&self) -> parking_lot::RwLockReadGuard<Mempool> {
        self.mempool.read()
    }

    pub fn broadcast_raw(&self, txhex: &str) -> Result<Txid> {
        let txid = self.daemon.broadcast_raw(txhex)?;
        // The important part is whether we succeeded in broadcasting.
        // Ignore errors in adding to the cache and show an internal warning.
        if let Err(e) = self.mempool.write().add_by_txid(&self.daemon, &txid) {
            warn!(
                "broadcast_raw of {txid} succeeded to broadcast \
                but failed to add to mempool-electrs Mempool cache: {e}"
            );
        }
        Ok(txid)
    }

    pub fn utxo(&self, scripthash: &[u8]) -> Result<Vec<Utxo>> {
        let mut utxos = self.chain.utxo(
            scripthash,
            self.config.utxos_limit,
            super::db::DBFlush::Enable,
        )?;

        let mempool = self.mempool();
        utxos.retain(|utxo| !mempool.has_spend(&OutPoint::from(utxo)));
        utxos.extend(mempool.utxo(scripthash)?);

        Ok(utxos)
    }

    pub fn history_txids(&self, scripthash: &[u8], limit: usize) -> Vec<(Txid, Option<BlockId>)> {
        let confirmed_txids = self.chain.history_txids(scripthash, limit);
        let confirmed_len = confirmed_txids.len();
        let confirmed_txids = confirmed_txids.into_iter().map(|(tx, b)| (tx, Some(b)));

        let mempool_txids = self
            .mempool()
            .history_txids(scripthash, limit - confirmed_len)
            .into_iter()
            .map(|tx| (tx, None));

        confirmed_txids.chain(mempool_txids).collect()
    }

    pub fn stats(&self, scripthash: &[u8]) -> (ScriptStats, ScriptStats) {
        (
            self.chain.stats(scripthash, super::db::DBFlush::Enable),
            self.mempool().stats(scripthash),
        )
    }

    pub fn lookup_txn(&self, txid: &Txid) -> Option<Transaction> {
        self.chain
            .lookup_txn(txid, None)
            .or_else(|| self.mempool().lookup_txn(txid))
    }
    pub fn lookup_raw_txn(&self, txid: &Txid) -> Option<Bytes> {
        self.chain
            .lookup_raw_txn(txid, None)
            .or_else(|| self.mempool().lookup_raw_txn(txid))
    }

    /// Not all OutPoints from mempool transactions are guaranteed to be included in the result
    pub fn lookup_txos(&self, outpoints: &BTreeSet<OutPoint>) -> HashMap<OutPoint, TxOut> {
        // the mempool lookup_txos() internally looks up confirmed txos as well
        self.mempool().lookup_txos(outpoints)
    }

    pub fn lookup_spend(&self, outpoint: &OutPoint) -> Option<SpendingInput> {
        self.chain
            .lookup_spend(outpoint)
            .or_else(|| self.mempool().lookup_spend(outpoint))
    }

    pub fn lookup_tx_spends(&self, tx: Transaction) -> Vec<Option<SpendingInput>> {
        let txid = tx.txid();

        tx.output
            .par_iter()
            .enumerate()
            .map(|(vout, txout)| {
                if is_spendable(txout) {
                    self.lookup_spend(&OutPoint {
                        txid,
                        vout: vout as u32,
                    })
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn get_tx_status(&self, txid: &Txid) -> TransactionStatus {
        TransactionStatus::from(self.chain.tx_confirming_block(txid))
    }

    pub fn get_mempool_tx_fee(&self, txid: &Txid) -> Option<u64> {
        self.mempool().get_tx_fee(txid)
    }

    pub fn has_unconfirmed_parents(&self, txid: &Txid) -> bool {
        self.mempool().has_unconfirmed_parents(txid)
    }

    pub fn estimate_fee(&self, conf_target: u16) -> Option<f64> {
        if let (ref cache, Some(cache_time)) = *self.cached_estimates.read() {
            if cache_time.elapsed() < Duration::from_secs(FEE_ESTIMATES_TTL) {
                return cache.get(&conf_target).copied();
            }
        }

        self.update_fee_estimates();
        self.cached_estimates.read().0.get(&conf_target).copied()
    }

    pub fn estimate_fee_map(&self) -> HashMap<u16, f64> {
        if let (ref cache, Some(cache_time)) = *self.cached_estimates.read() {
            if cache_time.elapsed() < Duration::from_secs(FEE_ESTIMATES_TTL) {
                return cache.clone();
            }
        }

        self.update_fee_estimates();
        self.cached_estimates.read().0.clone()
    }

    fn update_fee_estimates(&self) {
        match self.daemon.estimatesmartfee_batch(&CONF_TARGETS) {
            Ok(estimates) => {
                *self.cached_estimates.write() = (estimates, Some(Instant::now()));
            }
            Err(err) => {
                warn!("failed estimating feerates: {:?}", err);
            }
        }
    }

    pub fn get_relayfee(&self) -> Result<f64> {
        if let Some(cached) = *self.cached_relayfee.read() {
            return Ok(cached);
        }

        let relayfee = self.daemon.get_relayfee()?;
        self.cached_relayfee.write().replace(relayfee);
        Ok(relayfee)
    }
}
