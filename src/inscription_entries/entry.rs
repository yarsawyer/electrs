use std::convert::TryInto;

use bitcoin::hashes::Hash;

use crate::util::errors::AsAnyhow;

use super::*;

pub(crate) trait Entry: Sized {
    type Value;

    fn load(value: Self::Value) -> Result<Self>;

    fn store(self) -> Result<Self::Value>;
}

pub(super) type BlockHashValue = [u8; 32];

impl Entry for BlockHash {
    type Value = BlockHashValue;

    fn load(value: Self::Value) -> Result<Self> {
        Ok(BlockHash::from_inner(value))
    }

    fn store(self) -> Result<Self::Value> {
        Ok(self.into_inner())
    }
}

pub(crate) struct InscriptionEntry {
    pub(crate) fee: u64,
    pub(crate) height: u64,
    pub(crate) number: u64,
    pub(crate) sat: Option<Sat>,
    pub(crate) timestamp: u32,
}

pub(crate) type InscriptionEntryValue = (u64, u64, u64, u128, u32);

impl Entry for InscriptionEntry {
    type Value = InscriptionEntryValue;

    fn load((fee, height, number, sat, timestamp): InscriptionEntryValue) -> Result<Self> {
        Ok(Self {
            fee,
            height,
            number,
            sat: if sat == u128::MAX {
                None
            } else {
                Some(Sat(sat))
            },
            timestamp,
        })
    }

    fn store(self) -> Result<Self::Value> {
        Ok((
            self.fee,
            self.height,
            self.number,
            match self.sat {
                Some(sat) => sat.n(),
                None => u128::MAX,
            },
            self.timestamp,
        ))
    }
}

pub(crate) type InscriptionIdValue = [u8; 36];

impl Entry for InscriptionId {
    type Value = InscriptionIdValue;

    fn load(value: Self::Value) -> Result<Self> {
        let (txid, index) = value.split_at(32);
        Ok(Self {
            txid: Txid::from_slice(txid).track_err()?,
            index: u32::from_be_bytes(index.try_into().track_err()?),
        })
    }

    fn store(self) -> Result<Self::Value> {
        let mut value = [0; 36];
        let (txid, index) = value.split_at_mut(32);
        txid.copy_from_slice(self.txid.as_inner());
        index.copy_from_slice(&self.index.to_be_bytes());
        Ok(value)
    }
}

pub type OutPointValue = [u8; 36];

impl Entry for OutPoint {
    type Value = OutPointValue;

    fn load(value: Self::Value) -> Result<Self> {
        Decodable::consensus_decode(&mut io::Cursor::new(value)).track_err()
    }

    fn store(self) -> Result<Self::Value> {
        let mut value = [0; 36];
        self.consensus_encode(&mut value.as_mut_slice()).track_err()?;
        Ok(value)
    }
}

pub type SatPointValue = [u8; 44];

impl Entry for SatPoint {
    type Value = SatPointValue;

    fn load(value: Self::Value) -> Result<Self> {
        Decodable::consensus_decode(&mut io::Cursor::new(value)).track_err()
    }

    fn store(self) -> Result<Self::Value> {
        let mut value = [0; 44];
        self.consensus_encode(&mut value.as_mut_slice()).track_err()?;
        Ok(value)
    }
}

pub(super) type SatRange = (u128, u128);

impl Entry for SatRange {
    type Value = [u8; 24];

    fn load(
        [b0, b1, b2, b3, b4, b5, b6, b7, b8, b9, b10, b11, b12, b13, b14, b15, b16, b17, b18, b19, b20, b21, b22, b23]: Self::Value,
    ) -> Result<Self> {
        let start = u128::from_le_bytes([
            b0, b1, b2, b3, b4, b5, b6, b7, b8, b9, b10, b11, b12, b13, b14, b15,
        ]);

        let range = u64::from_le_bytes([b16, b17, b18, b19, b20, b21, b22, b23]);

        Ok((start, start + range as u128))
    }

    fn store(self) -> Result<Self::Value> {
        let start = self.0;
        let range = u64::from((self.1 - self.0) as u64);
        let start_bytes = u128::to_le_bytes(start);
        let range_bytes = u64::to_le_bytes(range);
        let mut out = [0_u8; 24];
        unsafe { std::ptr::copy_nonoverlapping(start_bytes.as_ptr(), out.as_mut_ptr(), 16) }
        unsafe { std::ptr::copy_nonoverlapping(range_bytes.as_ptr(), out.as_mut_ptr().add(16), 8) }
        Ok(out)
    }
}