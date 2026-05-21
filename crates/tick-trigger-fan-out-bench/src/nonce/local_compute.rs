//! Local-compute durable nonce restoration.
//!
//! Listens on a side channel of `OrderedEvent::SlotComplete` events (cloned
//! by phase3_run from the supervisor output) and, for each newly sealed
//! slot, computes `sha256("DURABLE_NONCE" || last_entry_hash_of_slot)`.
//!
//! When we observe one of our tx's `MatchEvent` for slot S, the nonce we
//! used was advanced on-chain from `bh_used` to `sha256("DURABLE_NONCE" ||
//! last_entry_hash(S-1))`. We don't directly know which nonce_id was used
//! for which slot from this thread — that linkage lives in the recorder.
//! Instead, we maintain a small `slot → last_entry_hash` cache here and
//! the recorder calls `compute_next_nonce_for(slot)` when it sees a sibling
//! land, pushing the result to `NonceManager`.
//!
//! This module is intentionally pure data: a `BTreeMap<u64, Hash>` behind
//! a `RwLock`, with rolling eviction. The actual nonce-advance work happens
//! synchronously in the recorder's `MatchEvent` handler — no extra thread,
//! no extra channel.

use parking_lot::RwLock;
use solana_sdk::hash::{hashv, Hash};
use std::collections::BTreeMap;

const DURABLE_NONCE_HASH_PREFIX: &[u8] = b"DURABLE_NONCE";

/// Solana's durable-nonce hashing: `next_nonce = sha256("DURABLE_NONCE" || prev)`.
pub fn compute_next_durable_nonce(recent_blockhash: Hash) -> Hash {
    hashv(&[DURABLE_NONCE_HASH_PREFIX, recent_blockhash.as_ref()])
}

/// Rolling cache of slot → `last_entry_hash`. The supervisor publishes one
/// `SlotComplete` per sealed slot; we record it here and look up by slot
/// later when the recorder needs to advance a nonce.
pub struct SlotHashCache {
    inner: RwLock<BTreeMap<u64, Hash>>,
    window: u64,
}

impl SlotHashCache {
    pub fn new(window: u64) -> Self {
        Self {
            inner: RwLock::new(BTreeMap::new()),
            window,
        }
    }

    /// Record an entry's hash for `slot`. Idempotent — last-write-wins.
    /// Called on EVERY supervisor Entry event (entries arrive in PoH order
    /// thanks to the supervisor's reorder buffer, so the final write before
    /// we transition to `slot+1` is the correct last_entry_hash). Also
    /// safe to call from `SlotComplete` as a confirmation.
    pub fn record_entry(&self, slot: u64, entry_hash: Hash) {
        let mut guard = self.inner.write();
        guard.insert(slot, entry_hash);
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

    /// Back-compat alias for `record_entry`.
    pub fn record_slot_complete(&self, slot: u64, last_entry_hash: Hash) {
        self.record_entry(slot, last_entry_hash);
    }

    /// Returns the next durable-nonce value for a tx that LANDED in
    /// `landed_slot`. Walks back from `landed_slot - 1` up to `max_lookback`
    /// slots, taking the first slot with a recorded `last_entry_hash`. The
    /// rare empty case (no cached slot in range) returns `None` — caller
    /// falls back to RPC.
    pub fn next_nonce_for_landed_slot(
        &self,
        landed_slot: u64,
        max_lookback: u64,
    ) -> Option<(u64, Hash, Hash)> {
        let guard = self.inner.read();
        for back in 1..=max_lookback {
            let s = landed_slot.checked_sub(back)?;
            if let Some(h) = guard.get(&s) {
                let next = compute_next_durable_nonce(*h);
                return Some((s, *h, next));
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
    use solana_sdk::hash::Hash;

    #[test]
    fn compute_matches_durable_nonce_formula() {
        // sha256("DURABLE_NONCE" || bh) — must match the on-chain formula.
        let bh = Hash::new_unique();
        let next = compute_next_durable_nonce(bh);
        let expected = hashv(&[b"DURABLE_NONCE", bh.as_ref()]);
        assert_eq!(next, expected);
    }

    #[test]
    fn cache_returns_next_nonce_from_prev_slot() {
        let cache = SlotHashCache::new(64);
        let bh99 = Hash::new_unique();
        cache.record_slot_complete(99, bh99);
        let (src, prev, next) = cache.next_nonce_for_landed_slot(100, 5).unwrap();
        assert_eq!(src, 99);
        assert_eq!(prev, bh99);
        assert_eq!(next, compute_next_durable_nonce(bh99));
    }

    #[test]
    fn cache_falls_back_when_predecessor_missing() {
        let cache = SlotHashCache::new(64);
        let bh97 = Hash::new_unique();
        cache.record_slot_complete(97, bh97);
        // slots 98, 99 skipped — should still find 97 within max_lookback=5
        let (src, _, _) = cache.next_nonce_for_landed_slot(100, 5).unwrap();
        assert_eq!(src, 97);
    }

    #[test]
    fn cache_returns_none_when_beyond_lookback() {
        let cache = SlotHashCache::new(64);
        cache.record_slot_complete(90, Hash::new_unique());
        assert!(cache.next_nonce_for_landed_slot(100, 5).is_none());
    }

    #[test]
    fn rolling_window_evicts_old_slots() {
        let cache = SlotHashCache::new(10);
        for s in 100..120u64 {
            cache.record_slot_complete(s, Hash::new_unique());
        }
        assert!(cache.next_nonce_for_landed_slot(101, 1).is_none());
        assert!(cache.next_nonce_for_landed_slot(120, 1).is_some());
    }
}
