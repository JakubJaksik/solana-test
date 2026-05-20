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
use solana_sdk::hash::{hash as sha256, hashv, Hash};
use std::collections::BTreeMap;

const DURABLE_NONCE_HASH_PREFIX: &[u8] = b"DURABLE_NONCE";

/// Solana PoH constants (must match observer.rs / runtime).
pub const HASHES_PER_TICK: u64 = 62_500;
pub const TICKS_PER_SLOT: u8 = 64;

/// Compute the new durable nonce value after an `AdvanceNonceAccount` ix that
/// observed `recent_blockhash` in the bank.
pub fn compute_next_durable_nonce(recent_blockhash: Hash) -> Hash {
    hashv(&[DURABLE_NONCE_HASH_PREFIX, recent_blockhash.as_ref()])
}

/// Chain-hash forward by `n` iterations of `hash(prev)` — the PoH semantics for
/// a tick entry with no data entries (just empty hash iterations).
/// Used as fallback when we have an early tick of slot S-1 but missed the final
/// tick 64; we extrapolate forward by `(TICKS_PER_SLOT - last_seen_tick) * HASHES_PER_TICK`.
///
/// Assumption: between the last seen tick and tick 64, the slot contained NO
/// data entries (only empty ticks). This holds at the end of most slots on
/// mainnet but is not guaranteed. If the assumption breaks, the computed hash
/// will not match the on-chain `last_blockhash` and the resulting durable nonce
/// will reject the next tx — caller should treat as best-effort.
pub fn chain_hash_forward(start: Hash, iterations: u64) -> Hash {
    let mut h = start;
    for _ in 0..iterations {
        h = sha256(h.as_ref());
    }
    h
}

#[derive(Debug, Default, Clone, Copy)]
struct SlotInfo {
    // Highest entry_index seen for this slot, regardless of type.
    max_entry_index: u32,
    // Hash of the entry at `max_entry_index`.
    last_entry_hash: Hash,
    // Highest tick number (1..=64) seen for this slot, if any.
    last_tick_idx: Option<u8>,
    // Hash of the entry at `last_tick_idx`, if last_tick_idx is Some.
    last_tick_hash: Option<Hash>,
}

pub struct SlotHashCache {
    inner: RwLock<BTreeMap<u64, SlotInfo>>,
    window: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct LookupResult {
    pub source_slot: u64,
    /// The tick we anchored on (1..=64). If chained_hashes > 0, the returned
    /// hash is extrapolated from this tick to tick 64.
    pub anchor_tick: u8,
    /// Final hash returned. Either the on-chain tick hash (chained_hashes==0)
    /// or the chain-extrapolated tick-64 hash.
    pub hash: Hash,
    /// Number of sha256 iterations applied to anchor hash to reach `hash`.
    /// 0 = direct, ((TICKS_PER_SLOT - anchor_tick) * HASHES_PER_TICK) otherwise.
    pub chained_hashes: u64,
}

impl SlotHashCache {
    pub fn new(window: u64) -> Self {
        Self {
            inner: RwLock::new(BTreeMap::new()),
            window,
        }
    }

    /// Record an entry. `tick_idx` is `Some(N)` if this entry was the N-th tick
    /// of the slot (1..=TICKS_PER_SLOT); `None` if it was a data entry.
    /// Out-of-order arrivals are handled — we keep the highest indices seen.
    pub fn update(&self, slot: u64, entry_index: u32, entry_hash: Hash, tick_idx: Option<u8>) {
        let mut guard = self.inner.write();
        let info = guard.entry(slot).or_default();
        if entry_index > info.max_entry_index || info.max_entry_index == 0 {
            info.max_entry_index = entry_index;
            info.last_entry_hash = entry_hash;
        }
        if let Some(t) = tick_idx {
            if info.last_tick_idx.is_none_or(|cur| t > cur) {
                info.last_tick_idx = Some(t);
                info.last_tick_hash = Some(entry_hash);
            }
        }

        // Rolling-window eviction.
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

    pub fn get_last_tick(&self, slot: u64) -> Option<(u8, Hash)> {
        let guard = self.inner.read();
        let info = guard.get(&slot)?;
        Some((info.last_tick_idx?, info.last_tick_hash?))
    }

    /// Returns `(max_entry_index, last_tick_idx, last_entry_hash)`. Diagnostic.
    pub fn get_summary(&self, slot: u64) -> Option<(u32, Option<u8>, Hash)> {
        let guard = self.inner.read();
        let info = guard.get(&slot)?;
        Some((info.max_entry_index, info.last_tick_idx, info.last_entry_hash))
    }

    /// Look up the `recent_blockhash` to use for a tx landed in `landed_slot`.
    /// Walks back from `landed_slot - 1` up to `max_lookback` slots looking
    /// for the most recent slot with a tick observed.
    ///
    /// If the matched slot has tick 64 observed → returns that hash directly.
    /// Otherwise chain-hashes forward `(64 - anchor_tick) * HASHES_PER_TICK`
    /// times to extrapolate the slot's last_blockhash (Solana PoH semantics
    /// for empty ticks). This is best-effort: assumes no data entries between
    /// the anchor tick and tick 64.
    pub fn lookup_recent_blockhash(
        &self,
        landed_slot: u64,
        max_lookback: u64,
    ) -> Option<LookupResult> {
        let guard = self.inner.read();
        for back in 1..=max_lookback {
            let s = landed_slot.checked_sub(back)?;
            let Some(info) = guard.get(&s) else { continue };
            let (Some(tick_idx), Some(tick_hash)) = (info.last_tick_idx, info.last_tick_hash)
            else {
                continue;
            };
            if tick_idx >= TICKS_PER_SLOT {
                return Some(LookupResult {
                    source_slot: s,
                    anchor_tick: tick_idx,
                    hash: tick_hash,
                    chained_hashes: 0,
                });
            }
            // Missing one or more final ticks. Chain-hash forward.
            let missing_ticks = (TICKS_PER_SLOT - tick_idx) as u64;
            let chained = missing_ticks * HASHES_PER_TICK;
            let extrapolated = chain_hash_forward(tick_hash, chained);
            return Some(LookupResult {
                source_slot: s,
                anchor_tick: tick_idx,
                hash: extrapolated,
                chained_hashes: chained,
            });
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
    fn update_and_get_last_tick() {
        let cache = SlotHashCache::new(64);
        let h = Hash::new_unique();
        cache.update(100, 5, h, Some(1));
        assert_eq!(cache.get_last_tick(100), Some((1, h)));
        assert_eq!(cache.get_last_tick(99), None);
    }

    #[test]
    fn update_keeps_highest_tick_only() {
        let cache = SlotHashCache::new(64);
        let h_low = Hash::new_unique();
        let h_high = Hash::new_unique();
        // Out-of-order: higher tick first then lower tick — cache must keep higher.
        cache.update(100, 100, h_high, Some(63));
        cache.update(100, 50, h_low, Some(20));
        assert_eq!(cache.get_last_tick(100), Some((63, h_high)));
    }

    #[test]
    fn data_entries_do_not_overwrite_tick_hash() {
        let cache = SlotHashCache::new(64);
        let tick_hash = Hash::new_unique();
        let data_hash = Hash::new_unique();
        cache.update(100, 50, tick_hash, Some(20));
        cache.update(100, 51, data_hash, None);
        assert_eq!(cache.get_last_tick(100), Some((20, tick_hash)));
    }

    #[test]
    fn lookup_returns_final_tick_directly_when_complete() {
        let cache = SlotHashCache::new(64);
        let final_tick_hash = Hash::new_unique();
        cache.update(99, 200, final_tick_hash, Some(TICKS_PER_SLOT));
        let r = cache.lookup_recent_blockhash(100, 5).unwrap();
        assert_eq!(r.source_slot, 99);
        assert_eq!(r.anchor_tick, TICKS_PER_SLOT);
        assert_eq!(r.chained_hashes, 0);
        assert_eq!(r.hash, final_tick_hash);
    }

    #[test]
    fn lookup_chain_hashes_when_missing_final_ticks() {
        let cache = SlotHashCache::new(64);
        let penultimate_tick_hash = Hash::new_unique();
        cache.update(99, 200, penultimate_tick_hash, Some(TICKS_PER_SLOT - 1));
        let r = cache.lookup_recent_blockhash(100, 5).unwrap();
        assert_eq!(r.source_slot, 99);
        assert_eq!(r.anchor_tick, TICKS_PER_SLOT - 1);
        assert_eq!(r.chained_hashes, HASHES_PER_TICK);
        // Verify chain-hash is deterministic and forward-only.
        let expected = chain_hash_forward(penultimate_tick_hash, HASHES_PER_TICK);
        assert_eq!(r.hash, expected);
        assert_ne!(r.hash, penultimate_tick_hash);
    }

    #[test]
    fn lookup_chain_hashes_two_missing_ticks() {
        let cache = SlotHashCache::new(64);
        let h = Hash::new_unique();
        cache.update(99, 200, h, Some(TICKS_PER_SLOT - 2));
        let r = cache.lookup_recent_blockhash(100, 5).unwrap();
        assert_eq!(r.chained_hashes, 2 * HASHES_PER_TICK);
    }

    #[test]
    fn lookup_falls_back_when_predecessor_skipped() {
        let cache = SlotHashCache::new(64);
        let h = Hash::new_unique();
        cache.update(98, 200, h, Some(TICKS_PER_SLOT));
        let r = cache.lookup_recent_blockhash(100, 5).unwrap();
        assert_eq!(r.source_slot, 98);
        assert_eq!(r.hash, h);
    }

    #[test]
    fn lookup_returns_none_when_no_tick_anywhere() {
        let cache = SlotHashCache::new(64);
        // Insert only data entries (no tick info) for nearby slots.
        cache.update(99, 200, Hash::new_unique(), None);
        cache.update(98, 200, Hash::new_unique(), None);
        assert!(cache.lookup_recent_blockhash(100, 5).is_none());
    }

    #[test]
    fn eviction_drops_old_slots() {
        let cache = SlotHashCache::new(10);
        for s in 100..120u64 {
            cache.update(s, 200, Hash::new_unique(), Some(TICKS_PER_SLOT));
        }
        assert!(cache.get_last_tick(100).is_none(), "slot 100 should be evicted");
        assert!(cache.get_last_tick(119).is_some(), "slot 119 should still be present");
        assert!(cache.len() <= 11);
    }

    #[test]
    fn chain_hash_forward_is_deterministic() {
        let start = Hash::new_unique();
        let r1 = chain_hash_forward(start, 100);
        let r2 = chain_hash_forward(start, 100);
        assert_eq!(r1, r2);
        // Chaining 50 + 50 == chaining 100 in one go.
        let half = chain_hash_forward(start, 50);
        let combined = chain_hash_forward(half, 50);
        assert_eq!(combined, r1);
    }
}
