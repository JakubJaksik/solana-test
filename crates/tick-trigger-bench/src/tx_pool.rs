use std::sync::Arc;
use std::time::Instant;

use dashmap::DashMap;
use solana_sdk::{hash::Hash, signature::Signature};

/// Pre-signed transaction stored in the rolling pool, keyed by (slot, tick).
#[derive(Debug)]
pub struct PreSignedTx {
    pub serialized: Vec<u8>,    // pre-serialized for sender hot path
    pub signature: Signature,
    pub blockhash: Hash,
    pub built_at: Instant,
}

#[derive(Clone, Default)]
pub struct TxPool {
    inner: Arc<DashMap<(u64, u8), PreSignedTx>>,
}

impl TxPool {
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts a pre-signed tx for (slot, tick). Returns true if slot was new,
    /// false if it overwrote.
    pub fn insert(&self, slot: u64, tick: u8, tx: PreSignedTx) -> bool {
        self.inner.insert((slot, tick), tx).is_none()
    }

    /// Removes & returns the tx for (slot, tick) if present. Hot path call.
    #[inline]
    pub fn take(&self, slot: u64, tick: u8) -> Option<PreSignedTx> {
        self.inner.remove(&(slot, tick)).map(|(_, v)| v)
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Drop entries for slots strictly older than `cutoff_slot`. Returns count
    /// of evicted entries.
    pub fn prune_older_than(&self, cutoff_slot: u64) -> usize {
        let keys: Vec<(u64, u8)> = self
            .inner
            .iter()
            .filter(|kv| kv.key().0 < cutoff_slot)
            .map(|kv| *kv.key())
            .collect();
        let mut removed = 0usize;
        for k in keys {
            if self.inner.remove(&k).is_some() {
                removed += 1;
            }
        }
        removed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::hash::Hash;

    fn dummy_tx() -> PreSignedTx {
        PreSignedTx {
            serialized: vec![0u8; 200],
            signature: Signature::default(),
            blockhash: Hash::new_unique(),
            built_at: Instant::now(),
        }
    }

    #[test]
    fn insert_and_take() {
        let pool = TxPool::new();
        assert!(pool.insert(100, 5, dummy_tx()));
        assert_eq!(pool.len(), 1);
        let t = pool.take(100, 5);
        assert!(t.is_some());
        assert_eq!(pool.len(), 0);
        assert!(pool.take(100, 5).is_none());
    }

    #[test]
    fn prune_older_than_drops_old_slots() {
        let pool = TxPool::new();
        pool.insert(100, 1, dummy_tx());
        pool.insert(100, 2, dummy_tx());
        pool.insert(150, 1, dummy_tx());
        pool.insert(200, 1, dummy_tx());
        let removed = pool.prune_older_than(150);
        assert_eq!(removed, 2); // (100,1) and (100,2)
        assert_eq!(pool.len(), 2);
        assert!(pool.take(150, 1).is_some());
        assert!(pool.take(200, 1).is_some());
    }
}
