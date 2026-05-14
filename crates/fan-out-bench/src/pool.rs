//! Pre-signed tx pool keyed by (slot, tick, sender_id).

use dashmap::DashMap;
use solana_sdk::transaction::Transaction;
use std::sync::Arc;
use std::time::Instant;

#[derive(Clone)]
pub struct PreSignedTx {
    pub tx: Arc<Transaction>,
    pub message_hash: [u8; 32],
    pub prepared_at: Instant,
    pub pool_ready_at: Instant,
}

#[derive(Default)]
pub struct TxPool {
    map: DashMap<(u64, u8, u8), PreSignedTx>,
}

impl TxPool {
    pub fn new() -> Self {
        Self { map: DashMap::with_capacity(8192) }
    }

    /// Insert a pre-signed tx. Returns true if key was previously empty.
    pub fn insert(&self, slot: u64, tick: u8, sender_id: u8, tx: PreSignedTx) -> bool {
        self.map.insert((slot, tick, sender_id), tx).is_none()
    }

    /// Take a single variant for (slot, tick, sender_id), removing it.
    pub fn take(&self, slot: u64, tick: u8, sender_id: u8) -> Option<PreSignedTx> {
        self.map.remove(&(slot, tick, sender_id)).map(|(_, v)| v)
    }

    /// Take ALL variants for (slot, tick), removing them. Returns (sender_id, tx) pairs.
    pub fn take_all_for(&self, slot: u64, tick: u8) -> Vec<(u8, PreSignedTx)> {
        let keys: Vec<(u64, u8, u8)> = self.map
            .iter()
            .filter(|e| e.key().0 == slot && e.key().1 == tick)
            .map(|e| *e.key())
            .collect();
        keys.into_iter()
            .filter_map(|k| self.map.remove(&k).map(|(key, v)| (key.2, v)))
            .collect()
    }

    /// Prune entries with slot < cutoff_slot.
    pub fn prune_older_than(&self, cutoff_slot: u64) -> usize {
        let stale: Vec<_> = self.map
            .iter()
            .filter(|e| e.key().0 < cutoff_slot)
            .map(|e| *e.key())
            .collect();
        let count = stale.len();
        for k in stale {
            self.map.remove(&k);
        }
        count
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::transaction::Transaction;

    fn fake_tx() -> PreSignedTx {
        PreSignedTx {
            tx: Arc::new(Transaction::default()),
            message_hash: [0; 32],
            prepared_at: Instant::now(),
            pool_ready_at: Instant::now(),
        }
    }

    #[test]
    fn insert_and_take_single() {
        let pool = TxPool::new();
        assert!(pool.insert(100, 5, 0, fake_tx()));
        assert_eq!(pool.len(), 1);
        let taken = pool.take(100, 5, 0);
        assert!(taken.is_some());
        assert!(pool.is_empty());
    }

    #[test]
    fn insert_twice_returns_false() {
        let pool = TxPool::new();
        assert!(pool.insert(100, 5, 0, fake_tx()));
        assert!(!pool.insert(100, 5, 0, fake_tx()));
    }

    #[test]
    fn take_missing_returns_none() {
        let pool = TxPool::new();
        assert!(pool.take(100, 5, 0).is_none());
    }

    #[test]
    fn take_all_for_returns_all_sender_variants() {
        let pool = TxPool::new();
        pool.insert(100, 5, 0, fake_tx());
        pool.insert(100, 5, 1, fake_tx());
        pool.insert(100, 5, 2, fake_tx());
        pool.insert(101, 5, 0, fake_tx());
        let taken = pool.take_all_for(100, 5);
        assert_eq!(taken.len(), 3);
        let mut ids: Vec<u8> = taken.iter().map(|(id, _)| *id).collect();
        ids.sort();
        assert_eq!(ids, vec![0, 1, 2]);
        assert_eq!(pool.len(), 1);
    }

    #[test]
    fn prune_removes_old_slots() {
        let pool = TxPool::new();
        pool.insert(100, 5, 0, fake_tx());
        pool.insert(200, 5, 0, fake_tx());
        pool.insert(300, 5, 0, fake_tx());
        let pruned = pool.prune_older_than(250);
        assert_eq!(pruned, 2);
        assert_eq!(pool.len(), 1);
    }
}
