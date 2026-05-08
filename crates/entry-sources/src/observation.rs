use std::time::Instant;
use smallvec::SmallVec;
use solana_sdk::{hash::Hash, pubkey::Pubkey, signature::Signature};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceKind {
    Yellowstone,
    ShredStream,
}

pub type SignatureVec = SmallVec<[Signature; 8]>;

#[derive(Debug, Clone)]
pub struct EntryObservation {
    pub source: SourceKind,
    pub observed_at: Instant,
    pub slot: u64,
    pub entry_index: u32,
    pub num_hashes: u64,
    pub entry_hash: Hash,
    pub tx_count: u32,
    pub signatures: SignatureVec,
    pub first_shred_at: Option<Instant>,
    pub leader: Option<Pubkey>,
}

impl EntryObservation {
    #[inline]
    pub fn is_tick(&self) -> bool {
        self.tx_count == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(tx_count: u32) -> EntryObservation {
        EntryObservation {
            source: SourceKind::Yellowstone,
            observed_at: Instant::now(),
            slot: 1,
            entry_index: 0,
            num_hashes: 12500,
            entry_hash: Hash::default(),
            tx_count,
            signatures: SignatureVec::new(),
            first_shred_at: None,
            leader: None,
        }
    }

    #[test]
    fn tick_when_zero_transactions() {
        assert!(fixture(0).is_tick());
    }

    #[test]
    fn not_tick_when_has_transactions() {
        assert!(!fixture(3).is_tick());
    }
}
