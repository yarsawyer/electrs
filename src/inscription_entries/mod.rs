use core::fmt;
use std::{fmt::{Display, Formatter}, io, str::FromStr, ops::{Add, AddAssign}, cmp};
use anyhow::{anyhow, Error, Result};
use serde::{Deserialize, Serialize, Deserializer, Serializer};
use bitcoin::{BlockHash, OutPoint, consensus::{Encodable, Decodable}, Txid};
use derive_more::Display;

pub(crate) use self::{
    deserialize_from_str::DeserializeFromStr, 
    entry::{
        Entry,
        OutPointValue,
        SatPointValue
    }, 
    inscription_id::InscriptionId, 
    sat::Sat, 
    height::Height, 
    epoch::Epoch, 
    sat_point::SatPoint, 
    inscription::{
        Inscription, 
        ParsedInscription
    }
};

pub mod sat;
pub mod height;
pub mod epoch;
pub mod inscription_id;
pub mod entry;
pub mod deserialize_from_str;
pub mod sat_point;
pub mod index;
pub mod inscription;
pub mod decimal;
pub mod rarity;
