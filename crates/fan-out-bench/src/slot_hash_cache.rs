//! SlotHashCache — shared cache of the last seen PoH entry hash per slot.
//!
//! Populated by Observer on every merged entry. Consumed by Matcher to compute
//! the next durable-nonce blockhash locally from on-chain data (SS+YS), with
//! NO RPC poll in the hot path.
//!
//! Solana semantics: for a tx executed in slot S, `recent_blockhash` registered
//! in the bank's BlockhashQueue is the `last_entry_hash` of the most recent
//! prior slot. In practice that is `last_entry_hash(S-1)`, falling back to
//! `S-2..S-N` if S-1 was skipped.
//!
//! `AdvanceNonceAccount` then sets the nonce account state to
//! `sha256("DURABLE_NONCE" || recent_blockhash)`. We compute that locally with
//! `compute_next_durable_nonce`.
//!
//! Rolling window eviction keeps the cache bounded (~64 slots).

use parking_lot::RwLock;
use solana_sdk::hash::{hashv, Hash};
use std::collections::BTreeMap;

const DURABLE_NONCE_HASH_PREFIX: &[u8] = b"DURABLE_NONCE";

/// Compute the new durable nonce value after an `AdvanceNonceAccount` ix that
/// observed `recent_blockhash` in the bank.
pub fn compute_next_durable_nonce(recent_blockhash: Hash) -> Hash {
    hashv(&[DURABLE_NONCE_HASH_PREFIX, recent_blockhash.as_ref()])
}

pub struct SlotHashCache {
    // (slot) → (max entry_index seen, that entry's hash).
    // We track entry_index so out-of-order arrivals from SS+YS don't overwrite
    // the true last entry with an earlier one. Bug observed: SS shred recovery
    // + YS gRPC async can deliver entry[N-1] AFTER entry[N], and a naive
    // "last write wins" cache would store the wrong hash → wrong durable
    // nonce → silent rejection at AdvanceNonceAccount validation on-chain.
    inner: RwLock<BTreeMap<u64, (u32, Hash)>>,
    window: u64,
}

impl SlotHashCache {
    pub fn new(window: u64) -> Self {
        Self {
            inner: RwLock::new(BTreeMap::new()),
            window,
        }
    }

    /// Record the hash of an entry. Only commits the hash when this entry's
    /// `entry_index` is greater than any previously seen for this slot. The
    /// final stored hash is therefore the highest-index entry hash, regardless
    /// of arrival order.
    pub fn update(&self, slot: u64, entry_index: u32, entry_hash: Hash) {
        let mut guard = self.inner.write();
        match guard.entry(slot) {
            std::collections::btree_map::Entry::Vacant(e) => {
                e.insert((entry_index, entry_hash));
            }
            std::collections::btree_map::Entry::Occupied(mut e) => {
                if entry_index > e.get().0 {
                    *e.get_mut() = (entry_index, entry_hash);
                }
            }
        }
        if let Some((&max_slot, _)) = guard.iter().next_back() {
            if max_slot > self.window {
                let cutoff = max_slot - self.window;
                while let Some((&k, _)) = guard.iter().next() {
                    if k < cutoff {
                        guard.remove(&k);
                    } else {
                        break;
                    }
                }
            }
        }
    }

    pub fn get(&self, slot: u64) -> Option<Hash> {
        self.inner.read().get(&slot).map(|(_, h)| *h)
    }

    /// Returns `(max_entry_index, hash)` for `slot`. Diagnostic.
    pub fn get_with_index(&self, slot: u64) -> Option<(u32, Hash)> {
        self.inner.read().get(&slot).copied()
    }

    /// Lookup the recent_blockhash to use for a tx landed in `landed_slot`.
    /// Tries `landed_slot - 1`, then walks back up to `max_lookback` slots in
    /// case the immediate predecessor was skipped (no entries → no cache hit).
    /// Returns `(source_slot, max_entry_index, hash)`.
    pub fn lookup_recent_blockhash(
        &self,
        landed_slot: u64,
        max_lookback: u64,
    ) -> Option<(u64, u32, Hash)> {
        let guard = self.inner.read();
        for back in 1..=max_lookback {
            let s = landed_slot.checked_sub(back)?;
            if let Some((idx, h)) = guard.get(&s) {
                return Some((s, *idx, *h));
            }
        }
        None
    }

    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.read().is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_next_durable_nonce_matches_solana_formula() {
        // Mirror solana-nonce-3.0.0 src/state.rs:57:
        //   hashv(&[DURABLE_NONCE_HASH_PREFIX, blockhash.as_ref()])
        let bh = Hash::new_unique();
        let computed = compute_next_durable_nonce(bh);
        let expected = hashv(&[b"DURABLE_NONCE", bh.as_ref()]);
        assert_eq!(computed, expected);
    }

    #[test]
    fn update_and_get_roundtrip() {
        let cache = SlotHashCache::new(64);
        let h = Hash::new_unique();
        cache.update(100, 5, h);
        assert_eq!(cache.get(100), Some(h));
        assert_eq!(cache.get(99), None);
    }

    #[test]
    fn update_keeps_higher_entry_index_only() {
        let cache = SlotHashCache::new(64);
        let h_low = Hash::new_unique();
        let h_high = Hash::new_unique();
        // Out-of-order arrival: higher entry_index comes first, then lower.
        cache.update(100, 10, h_high);
        cache.update(100, 5, h_low);
        // Cache should retain the higher-index entry's hash.
        assert_eq!(cache.get(100), Some(h_high));
        assert_eq!(cache.get_with_index(100), Some((10, h_high)));
    }

    #[test]
    fn update_advances_when_new_index_is_higher() {
        let cache = SlotHashCache::new(64);
        let h1 = Hash::new_unique();
        let h2 = Hash::new_unique();
        cache.update(100, 5, h1);
        cache.update(100, 10, h2);
        assert_eq!(cache.get(100), Some(h2));
    }

    #[test]
    fn lookup_falls_back_when_predecessor_skipped() {
        let cache = SlotHashCache::new(64);
        let h = Hash::new_unique();
        cache.update(98, 63, h);
        // slot 99 skipped, no entries
        let (src, idx, found) = cache.lookup_recent_blockhash(100, 5).unwrap();
        assert_eq!(src, 98);
        assert_eq!(idx, 63);
        assert_eq!(found, h);
    }

    #[test]
    fn lookup_returns_immediate_predecessor_when_available() {
        let cache = SlotHashCache::new(64);
        let h99 = Hash::new_unique();
        let h98 = Hash::new_unique();
        cache.update(98, 63, h98);
        cache.update(99, 63, h99);
        let (src, _idx, found) = cache.lookup_recent_blockhash(100, 5).unwrap();
        assert_eq!(src, 99);
        assert_eq!(found, h99);
    }

    #[test]
    fn lookup_returns_none_when_beyond_lookback() {
        let cache = SlotHashCache::new(64);
        cache.update(90, 63, Hash::new_unique());
        assert!(cache.lookup_recent_blockhash(100, 5).is_none());
    }

    #[test]
    fn eviction_drops_old_slots() {
        let cache = SlotHashCache::new(10);
        for s in 100..120u64 {
            cache.update(s, 63, Hash::new_unique());
        }
        assert!(cache.get(100).is_none(), "slot 100 should be evicted");
        assert!(cache.get(119).is_some(), "slot 119 should still be present");
        assert!(cache.len() <= 11);
    }
}
