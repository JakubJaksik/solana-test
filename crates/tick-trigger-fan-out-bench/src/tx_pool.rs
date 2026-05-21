//! Pre-signed transaction pool, keyed by (slot, tick).
//!
//! Each pool entry is a `Vec<PreSignedTx>` — one tx per enabled sender. The
//! preparer SHUFFLES the vector before insert so the dispatcher's fire-time
//! send order is randomized per trigger (no sender_id has a fixed "first"
//! position across triggers). For phase 3 with 1 sender the vec has length 1.

use dashmap::DashMap;
use solana_sdk::hash::Hash;
use solana_sdk::signature::Signature;
use solana_sdk::transaction::Transaction;
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct PreSignedTx {
    pub sender_id: u8,
    pub tx: Arc<Transaction>,
    pub signature: Signature,
    pub blockhash: Hash,
    pub prepared_at: Instant,
}

pub struct TxPool {
    inner: DashMap<(u64, u8), Vec<PreSignedTx>>,
}

impl Default for TxPool {
    fn default() -> Self {
        Self::new()
    }
}

impl TxPool {
    pub fn new() -> Self {
        Self {
            inner: DashMap::with_capacity(4096),
        }
    }

    /// Atomically take and remove the full variant list for `(slot, tick)`.
    /// Order in the returned vec is whatever the preparer set (typically
    /// a per-trigger shuffle).
    pub fn take(&self, slot: u64, tick: u8) -> Option<Vec<PreSignedTx>> {
        self.inner.remove(&(slot, tick)).map(|(_, v)| v)
    }

    /// Insert the full variant list for `(slot, tick)`. Replaces any prior
    /// vec for the same key.
    pub fn insert_all(&self, slot: u64, tick: u8, txs: Vec<PreSignedTx>) {
        self.inner.insert((slot, tick), txs);
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Drop entries for slots older than `cutoff_slot`. Called periodically
    /// by the preparer to bound memory when triggers don't fire.
    pub fn evict_below(&self, cutoff_slot: u64) {
        self.inner.retain(|(s, _), _| *s >= cutoff_slot);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy(sender_id: u8) -> PreSignedTx {
        PreSignedTx {
            sender_id,
            tx: Arc::new(Transaction::default()),
            signature: Signature::default(),
            blockhash: Hash::default(),
            prepared_at: Instant::now(),
        }
    }

    #[test]
    fn take_returns_full_vec_and_removes_key() {
        let p = TxPool::new();
        p.insert_all(100, 5, vec![dummy(0), dummy(1), dummy(2)]);
        assert_eq!(p.len(), 1);
        let v = p.take(100, 5).unwrap();
        assert_eq!(v.len(), 3);
        assert!(p.is_empty());
        assert!(p.take(100, 5).is_none());
    }

    #[test]
    fn evict_drops_old_slots() {
        let p = TxPool::new();
        for s in 100..110u64 {
            p.insert_all(s, 1, vec![dummy(0)]);
        }
        p.evict_below(105);
        assert_eq!(p.len(), 5);
        assert!(p.take(104, 1).is_none());
        assert!(p.take(105, 1).is_some());
    }
}
