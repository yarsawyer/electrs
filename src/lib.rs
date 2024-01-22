#![recursion_limit = "1024"]

extern crate rocksdb;

#[macro_use]
extern crate clap;
#[macro_use]
extern crate arrayref;
#[macro_use]
extern crate error_chain;
// #[macro_use]
// extern crate log;
#[macro_use]
extern crate tracing;

#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate serde_json;

#[macro_use]
extern crate lazy_static;

pub mod chain;
pub mod config;
pub mod daemon;
pub mod electrum;
pub mod errors;
mod inscription_entries;
mod media;
pub mod metrics;
pub mod new_index;
pub mod rest;
pub mod signal;
pub mod util;

pub use self::config::HEIGHT_DELAY;
