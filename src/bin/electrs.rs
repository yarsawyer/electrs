extern crate error_chain;
// #[macro_use]
// extern crate log;

#[macro_use]
extern crate tracing;

extern crate electrs;

use bitcoin::{hashes::Hash, BlockHash};
use error_chain::ChainedError;
use std::process;
use std::sync::Arc;
use std::time::Duration;

use electrs::{
    config::{Config, TOKENS_OFFSET},
    daemon::Daemon,
    electrum::RPC as ElectrumRPC,
    errors::*,
    inscription_entries::inscription::{update_last_block_number, InscriptionContent},
    metrics::Metrics,
    new_index::{
        exchange_data::ExchangeData, precache, schema::InscriptionParseBlock, token::TokenCache,
        ChainQuery, FetchFrom, Indexer, InscriptionUpdater, Mempool, Query, Store,
    },
    rest,
    signal::Waiter,
    util::spawn_thread,
    HEIGHT_DELAY,
};

#[cfg(feature = "liquid")]
use electrs::elements::AssetRegistry;

fn fetch_from(config: &Config, store: &Store) -> FetchFrom {
    let mut jsonrpc_import = config.jsonrpc_import;
    if !jsonrpc_import {
        // switch over to jsonrpc after the initial sync is done
        jsonrpc_import = store.done_initial_sync();
    }

    if jsonrpc_import {
        // slower, uses JSONRPC (good for incremental updates)
        FetchFrom::Bitcoind
    } else {
        // faster, uses blk*.dat files (good for initial indexing)
        FetchFrom::BlkFiles
    }
}

fn run_server(config: Arc<Config>) -> Result<()> {
    let signal = Waiter::start();
    let metrics = Metrics::new(config.monitoring_addr);
    metrics.start();

    let (sender, receiver) = crossbeam_channel::unbounded::<InscriptionContent>();
    let sender = Arc::new(sender);

    spawn_thread("inscription_content_receiver", move || {
        for _ in receiver {
            // TODO
        }
    });

    let daemon = Arc::new(Daemon::new(
        config.daemon_dir.clone(),
        config.blocks_dir.clone(),
        config.daemon_rpc_addr,
        config.cookie_getter(),
        config.network_type,
        signal.clone(),
        &metrics,
    )?);
    let store = Arc::new(Store::open(&config.db_path.join("newindex"), &config));
    let mut indexer = Indexer::open(
        Arc::clone(&store),
        fetch_from(&config, &store),
        &config,
        &metrics,
    );

    let (mut tip, _, _) = indexer.update(&daemon)?;

    let chain = Arc::new(ChainQuery::new(
        Arc::clone(&store),
        Arc::clone(&daemon),
        &config,
        &metrics,
    ));

    let tip_height = store.get_block_height(tip).unwrap() as u32;

    let temp_offset = tip_height - HEIGHT_DELAY;

    let temp_ot = {
        if let Some(ot) = indexer.clear_temp(temp_offset) {
            if tip_height - ot > HEIGHT_DELAY {
                temp_offset
            } else {
                ot
            }
        } else {
            temp_offset
        }
    } + 1;

    let ot = store
        .inscription_db()
        .get(b"ot")
        .map(|x| BlockHash::from_slice(&x).unwrap())
        .map(|x| store.get_block_height(x).unwrap())
        .unwrap_or(config.first_inscription_block);

    indexer
        .index_inscription(
            InscriptionParseBlock::FromToHeight(ot as u32, temp_offset),
            sender.clone(),
        )
        .unwrap();

    update_last_block_number(config.first_inscription_block, &store, temp_offset, false)?;

    let inscription_updater = InscriptionUpdater::new(store.clone()).unwrap();

    inscription_updater.copy_from_main_block(temp_ot).unwrap();

    let mut token_cache = {
        if let Some(parsed) = store.temp_db().remove("tc".as_bytes()) {
            serde_json::from_slice(&parsed).unwrap()
        } else {
            TokenCache::default()
        }
    };

    indexer
        .index_temp(
            &inscription_updater,
            chain.clone(),
            InscriptionParseBlock::FromHeight(temp_ot, HEIGHT_DELAY),
            &mut token_cache,
            sender.clone(),
            config.first_inscription_block,
        )
        .unwrap();

    store.inscription_db().flush();

    token_cache.process_token_actions(Some(tip_height - TOKENS_OFFSET - 1));
    token_cache.write_token_data(store.token_db());
    token_cache.write_valid_transfers(store.token_db());

    let mempool = Arc::new(parking_lot::RwLock::new(Mempool::new(
        Arc::clone(&chain),
        &metrics,
        Arc::clone(&config),
    )));
    mempool.write().update(&daemon)?;

    #[cfg(feature = "liquid")]
    let asset_db = config.asset_db_path.as_ref().map(|db_dir| {
        let asset_db = Arc::new(parking_lot::RwLock::new(AssetRegistry::new(db_dir.clone())));
        AssetRegistry::spawn_sync(asset_db.clone());
        asset_db
    });

    let exchange_data = Arc::new(parking_lot::Mutex::new(ExchangeData { bells_price: None }));

    let query = Arc::new(Query::new(
        Arc::clone(&chain),
        Arc::clone(&mempool),
        Arc::clone(&daemon),
        Arc::clone(&config),
        #[cfg(feature = "liquid")]
        asset_db,
        exchange_data,
    ));

    // TODO: configuration for which servers to start
    let rest_server = rest::start(Arc::clone(&config), Arc::clone(&query));
    let electrum_server = ElectrumRPC::start(Arc::clone(&config), Arc::clone(&query), &metrics);

    if let Some(ref precache_file) = config.precache_scripts {
        let precache_scripthashes = precache::scripthashes_from_file(precache_file.to_string())
            .chain_err(|| "cannot load scripts to precache")?;
        precache::precache(
            Arc::clone(&chain),
            precache_scripthashes,
            config.precache_threads,
        );
    }

    loop {
        if let Err(err) = signal.wait(Duration::from_millis(500), true) {
            info!("stopping server: {}", err);

            electrs::util::spawn_thread("shutdown-thread-checker", || {
                let mut counter = 40;
                let interval_ms = 500;

                while counter > 0 {
                    electrs::util::with_spawned_threads(|threads| {
                        debug!("Threads during shutdown: {:?}", threads);
                    });
                    std::thread::sleep(std::time::Duration::from_millis(interval_ms));
                    counter -= 1;
                }
            });

            rest_server.stop();
            // the electrum server is stopped when dropped
            break;
        }

        // Index new blocks
        let current_tip = daemon.getbestblockhash()?;
        if current_tip != tip {
            let (indexed_tip, new_length, removed) = indexer.update(&daemon)?;

            tip = indexed_tip;
            let block = store.get_block_height(indexed_tip).unwrap() as u32;

            if !removed.is_empty() {
                let first_height = removed.first().unwrap().height() as u32;
                error!("Reorg happened, blocks length: {}", removed.len());
                inscription_updater
                    .reorg_handler(removed, config.first_inscription_block)
                    .expect("Something went wrong with removing blocks");
                token_cache.remove_token_actions(first_height);
                inscription_updater.copy_to_next_block(first_height - 1 as u32)?;
            }

            indexer
                .index_temp(
                    &inscription_updater,
                    chain.clone(),
                    InscriptionParseBlock::FromHeight(
                        block - new_length as u32 + 1,
                        new_length as u32,
                    ),
                    &mut token_cache,
                    sender.clone(),
                    config.first_inscription_block,
                )
                .unwrap();

            token_cache.process_token_actions(Some(block - TOKENS_OFFSET - 1));

            token_cache.write_token_data(store.token_db());
            token_cache.write_valid_transfers(store.token_db());
        };

        // Update mempool
        mempool.write().update(&daemon)?;

        // Update subscribed clients
        electrum_server.notify();
    }

    store
        .temp_db()
        .put("tc".as_bytes(), &serde_json::to_vec(&token_cache).unwrap());

    info!("server stopped");
    Ok(())
}

fn main() {
    let config = Arc::new(Config::from_args());
    if let Err(e) = run_server(config) {
        error!("server failed: {}", e.display_chain());
        process::exit(1);
    }
    electrs::util::with_spawned_threads(|threads| {
        debug!("Threads before closing: {:?}", threads);
    });
}
