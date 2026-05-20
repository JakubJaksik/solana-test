//! NonceManager — durable nonce pool state machine.
//!
//! See spec §7.1 — state machine: Ready → InFlight → AwaitingUpdate → Ready,
//! with fallback to Stale on timeout. RR allocator.

use parking_lot::RwLock;
use solana_sdk::{hash::Hash, pubkey::Pubkey};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

pub type NonceId = u16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NonceState {
    Ready { blockhash: Hash },
    InFlight { blockhash_used: Hash, since: Instant },
    AwaitingUpdate { blockhash_used: Hash, since: Instant },
    Stale { blockhash_used: Hash, since: Instant },
}

impl NonceState {
    pub fn is_ready(&self) -> bool {
        matches!(self, NonceState::Ready { .. })
    }
}

pub struct NonceEntry {
    pub id: NonceId,
    pub pubkey: Pubkey,
    state: RwLock<NonceState>,
}

impl NonceEntry {
    pub fn new(id: NonceId, pubkey: Pubkey, blockhash: Hash) -> Self {
        Self {
            id,
            pubkey,
            state: RwLock::new(NonceState::Ready { blockhash }),
        }
    }

    pub fn state(&self) -> NonceState {
        *self.state.read()
    }

    pub fn set_state(&self, new_state: NonceState) {
        *self.state.write() = new_state;
    }
}

pub struct NonceManager {
    entries: Vec<Arc<NonceEntry>>,
    pubkey_index: std::collections::HashMap<Pubkey, usize>,
    rr_cursor: AtomicUsize,
}

impl NonceManager {
    pub fn new(entries: Vec<(NonceId, Pubkey, Hash)>) -> Self {
        let pubkey_index = entries
            .iter()
            .enumerate()
            .map(|(idx, (_, pk, _))| (*pk, idx))
            .collect();
        let entries = entries
            .into_iter()
            .map(|(id, pk, bh)| Arc::new(NonceEntry::new(id, pk, bh)))
            .collect();
        Self {
            entries,
            pubkey_index,
            rr_cursor: AtomicUsize::new(0),
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn take_ready(&self) -> Option<(NonceId, Pubkey, Hash)> {
        let n = self.entries.len();
        if n == 0 {
            return None;
        }
        let start = self.rr_cursor.fetch_add(1, Ordering::Relaxed) % n;
        for offset in 0..n {
            let idx = (start + offset) % n;
            let entry = &self.entries[idx];
            let mut guard = entry.state.write();
            if let NonceState::Ready { blockhash } = *guard {
                *guard = NonceState::InFlight {
                    blockhash_used: blockhash,
                    since: Instant::now(),
                };
                return Some((entry.id, entry.pubkey, blockhash));
            }
        }
        None
    }

    pub fn entries(&self) -> &[Arc<NonceEntry>] {
        &self.entries
    }

    pub fn get_by_pubkey(&self, pubkey: &Pubkey) -> Option<&Arc<NonceEntry>> {
        self.pubkey_index.get(pubkey).map(|&idx| &self.entries[idx])
    }

    pub fn get_by_id(&self, id: NonceId) -> Option<&Arc<NonceEntry>> {
        self.entries.iter().find(|e| e.id == id)
    }

    pub fn count_in_state(&self, predicate: impl Fn(&NonceState) -> bool) -> usize {
        self.entries.iter().filter(|e| predicate(&e.state())).count()
    }

    pub fn ready_count(&self) -> usize {
        self.count_in_state(|s| s.is_ready())
    }

    /// Called by Geyser/YS subscription when nonce account state changes on chain.
    /// If observed blockhash differs from cached → nonce advanced → transition to Ready.
    pub fn on_account_update(&self, pubkey: &Pubkey, new_blockhash: Hash) -> bool {
        let entry = match self.get_by_pubkey(pubkey) {
            Some(e) => e,
            None => return false,
        };
        let mut guard = entry.state.write();
        let advanced = match *guard {
            NonceState::Ready { blockhash } => blockhash != new_blockhash,
            NonceState::InFlight { blockhash_used, .. }
            | NonceState::AwaitingUpdate { blockhash_used, .. }
            | NonceState::Stale { blockhash_used, .. } => blockhash_used != new_blockhash,
        };
        if advanced {
            *guard = NonceState::Ready {
                blockhash: new_blockhash,
            };
        }
        advanced
    }

    /// Called by matcher when ANY sibling sig from a trigger is observed landed.
    /// Transitions InFlight → AwaitingUpdate.
    pub fn on_observed_landing(&self, nonce_id: NonceId) {
        let entry = match self.get_by_id(nonce_id) {
            Some(e) => e,
            None => return,
        };
        let mut guard = entry.state.write();
        if let NonceState::InFlight { blockhash_used, .. } = *guard {
            *guard = NonceState::AwaitingUpdate {
                blockhash_used,
                since: Instant::now(),
            };
        }
    }

    /// Called by matcher with the locally-computed next durable-nonce value
    /// (see `slot_hash_cache::compute_next_durable_nonce`). Transitions any
    /// non-Ready state directly to Ready { new_blockhash }, bypassing the
    /// YS/RPC observation path entirely.
    pub fn on_landing_with_blockhash(&self, nonce_id: NonceId, new_blockhash: Hash) -> bool {
        let entry = match self.get_by_id(nonce_id) {
            Some(e) => e,
            None => return false,
        };
        let mut guard = entry.state.write();
        match *guard {
            NonceState::InFlight { .. }
            | NonceState::AwaitingUpdate { .. }
            | NonceState::Stale { .. } => {
                *guard = NonceState::Ready {
                    blockhash: new_blockhash,
                };
                true
            }
            NonceState::Ready { blockhash } => {
                // Out-of-order observation: if local-compute reports a different
                // hash than what we already have Ready, accept the newer value.
                if blockhash != new_blockhash {
                    *guard = NonceState::Ready {
                        blockhash: new_blockhash,
                    };
                    true
                } else {
                    false
                }
            }
        }
    }

    /// Sweep entries past deadline. Returns list of pubkeys now in Stale state.
    pub fn tick_timeouts(
        &self,
        in_flight_deadline: std::time::Duration,
        awaiting_update_deadline: std::time::Duration,
    ) -> Vec<(NonceId, Pubkey)> {
        let now = Instant::now();
        let mut stale_now: Vec<(NonceId, Pubkey)> = Vec::new();
        for entry in &self.entries {
            let mut guard = entry.state.write();
            let became_stale = match *guard {
                NonceState::InFlight { blockhash_used, since }
                    if now.duration_since(since) >= in_flight_deadline =>
                {
                    *guard = NonceState::Stale {
                        blockhash_used,
                        since: now,
                    };
                    true
                }
                NonceState::AwaitingUpdate { blockhash_used, since }
                    if now.duration_since(since) >= awaiting_update_deadline =>
                {
                    *guard = NonceState::Stale {
                        blockhash_used,
                        since: now,
                    };
                    true
                }
                _ => false,
            };
            if became_stale {
                stale_now.push((entry.id, entry.pubkey));
            }
        }
        stale_now
    }

    /// Called by matcher when a trigger's nonce_restore_after window elapsed
    /// without any sibling sig being matched. Presumes no variant landed →
    /// AdvanceNonceAccount was NOT executed → on-chain blockhash is unchanged.
    /// Restores InFlight/AwaitingUpdate/Stale back to Ready with the SAME
    /// blockhash it was using. Returns true if state actually changed.
    ///
    /// Safety: if a tx actually lands later (delayed observation), the
    /// subsequent MatchEvent will trigger `on_landing_with_blockhash` which
    /// overwrites with the correct new blockhash. Worst case: one extra tx
    /// signs+sends with a stale blockhash and fails validation on-chain at
    /// 0 lamports cost (durable-nonce property).
    pub fn restore_unchanged(&self, nonce_id: NonceId) -> bool {
        let entry = match self.get_by_id(nonce_id) {
            Some(e) => e,
            None => return false,
        };
        let mut guard = entry.state.write();
        match *guard {
            NonceState::InFlight { blockhash_used, .. }
            | NonceState::AwaitingUpdate { blockhash_used, .. }
            | NonceState::Stale { blockhash_used, .. } => {
                *guard = NonceState::Ready {
                    blockhash: blockhash_used,
                };
                true
            }
            NonceState::Ready { .. } => false,
        }
    }

    /// Called by RPC fallback after re-fetching account state.
    pub fn on_fallback_refresh(&self, pubkey: &Pubkey, observed_blockhash: Hash) {
        let entry = match self.get_by_pubkey(pubkey) {
            Some(e) => e,
            None => return,
        };
        let mut guard = entry.state.write();
        if let NonceState::Stale { blockhash_used, .. } = *guard {
            if blockhash_used == observed_blockhash {
                *guard = NonceState::Ready {
                    blockhash: blockhash_used,
                };
            } else {
                *guard = NonceState::Ready {
                    blockhash: observed_blockhash,
                };
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_manager(n: usize) -> NonceManager {
        let entries: Vec<_> = (0..n)
            .map(|i| (i as NonceId, Pubkey::new_unique(), Hash::new_unique()))
            .collect();
        NonceManager::new(entries)
    }

    #[test]
    fn empty_manager_returns_none() {
        let manager = NonceManager::new(vec![]);
        assert!(manager.is_empty());
        assert!(manager.take_ready().is_none());
    }

    #[test]
    fn take_ready_transitions_to_in_flight() {
        let manager = make_manager(3);
        let (id, _, _) = manager.take_ready().unwrap();
        let entry = manager.get_by_id(id).unwrap();
        assert!(matches!(entry.state(), NonceState::InFlight { .. }));
    }

    #[test]
    fn take_ready_returns_none_when_all_in_flight() {
        let manager = make_manager(2);
        manager.take_ready().unwrap();
        manager.take_ready().unwrap();
        assert!(manager.take_ready().is_none());
    }

    #[test]
    fn ready_count_decreases_on_take() {
        let manager = make_manager(5);
        assert_eq!(manager.ready_count(), 5);
        manager.take_ready().unwrap();
        assert_eq!(manager.ready_count(), 4);
        manager.take_ready().unwrap();
        assert_eq!(manager.ready_count(), 3);
    }

    #[test]
    fn rr_rotation_uses_different_nonces() {
        let manager = make_manager(3);
        let mut ids = std::collections::HashSet::new();
        for _ in 0..3 {
            let (id, _, _) = manager.take_ready().unwrap();
            ids.insert(id);
        }
        assert_eq!(ids.len(), 3);
    }

    #[test]
    fn get_by_pubkey_finds_entry() {
        let pk = Pubkey::new_unique();
        let bh = Hash::new_unique();
        let manager = NonceManager::new(vec![(42, pk, bh)]);
        let entry = manager.get_by_pubkey(&pk).unwrap();
        assert_eq!(entry.id, 42);
    }

    #[test]
    fn get_by_pubkey_returns_none_for_unknown() {
        let manager = make_manager(3);
        assert!(manager.get_by_pubkey(&Pubkey::new_unique()).is_none());
    }

    use std::time::Duration;

    #[test]
    fn on_account_update_advances_to_ready() {
        let pk = Pubkey::new_unique();
        let bh1 = Hash::new_unique();
        let bh2 = Hash::new_unique();
        let manager = NonceManager::new(vec![(0, pk, bh1)]);
        manager.take_ready().unwrap();
        let advanced = manager.on_account_update(&pk, bh2);
        assert!(advanced);
        let entry = manager.get_by_id(0).unwrap();
        match entry.state() {
            NonceState::Ready { blockhash } => assert_eq!(blockhash, bh2),
            other => panic!("expected Ready, got {:?}", other),
        }
    }

    #[test]
    fn on_account_update_same_blockhash_no_advance() {
        let pk = Pubkey::new_unique();
        let bh = Hash::new_unique();
        let manager = NonceManager::new(vec![(0, pk, bh)]);
        manager.take_ready().unwrap();
        let advanced = manager.on_account_update(&pk, bh);
        assert!(!advanced);
        assert!(matches!(manager.get_by_id(0).unwrap().state(), NonceState::InFlight { .. }));
    }

    #[test]
    fn on_landing_with_blockhash_from_in_flight_to_ready() {
        let pk = Pubkey::new_unique();
        let bh1 = Hash::new_unique();
        let bh2 = Hash::new_unique();
        let manager = NonceManager::new(vec![(7, pk, bh1)]);
        manager.take_ready().unwrap();
        let changed = manager.on_landing_with_blockhash(7, bh2);
        assert!(changed);
        match manager.get_by_id(7).unwrap().state() {
            NonceState::Ready { blockhash } => assert_eq!(blockhash, bh2),
            other => panic!("expected Ready, got {:?}", other),
        }
    }

    #[test]
    fn on_landing_with_blockhash_noop_for_unknown_id() {
        let manager = make_manager(2);
        assert!(!manager.on_landing_with_blockhash(99, Hash::new_unique()));
    }

    #[test]
    fn on_landing_with_blockhash_from_stale_recovers() {
        let pk = Pubkey::new_unique();
        let bh1 = Hash::new_unique();
        let bh2 = Hash::new_unique();
        let manager = NonceManager::new(vec![(0, pk, bh1)]);
        manager.take_ready().unwrap();
        std::thread::sleep(Duration::from_millis(20));
        manager.tick_timeouts(Duration::from_millis(10), Duration::from_secs(5));
        assert!(matches!(manager.get_by_id(0).unwrap().state(), NonceState::Stale { .. }));
        assert!(manager.on_landing_with_blockhash(0, bh2));
        match manager.get_by_id(0).unwrap().state() {
            NonceState::Ready { blockhash } => assert_eq!(blockhash, bh2),
            other => panic!("expected Ready, got {:?}", other),
        }
    }

    #[test]
    fn on_observed_landing_transitions_to_awaiting_update() {
        let manager = make_manager(1);
        let (id, _, _) = manager.take_ready().unwrap();
        manager.on_observed_landing(id);
        assert!(matches!(
            manager.get_by_id(id).unwrap().state(),
            NonceState::AwaitingUpdate { .. }
        ));
    }

    #[test]
    fn tick_timeouts_moves_in_flight_to_stale() {
        let manager = make_manager(2);
        manager.take_ready().unwrap();
        manager.take_ready().unwrap();
        std::thread::sleep(Duration::from_millis(20));
        let stale = manager.tick_timeouts(Duration::from_millis(10), Duration::from_secs(5));
        assert_eq!(stale.len(), 2);
        for entry in manager.entries() {
            assert!(matches!(entry.state(), NonceState::Stale { .. }));
        }
    }

    #[test]
    fn restore_unchanged_from_in_flight() {
        let pk = Pubkey::new_unique();
        let bh = Hash::new_unique();
        let manager = NonceManager::new(vec![(0, pk, bh)]);
        manager.take_ready().unwrap();
        assert!(matches!(manager.get_by_id(0).unwrap().state(), NonceState::InFlight { .. }));
        assert!(manager.restore_unchanged(0));
        match manager.get_by_id(0).unwrap().state() {
            NonceState::Ready { blockhash } => assert_eq!(blockhash, bh, "restore preserves blockhash"),
            other => panic!("expected Ready, got {:?}", other),
        }
    }

    #[test]
    fn restore_unchanged_noop_when_already_ready() {
        let manager = make_manager(1);
        let entry = &manager.entries()[0];
        assert!(matches!(entry.state(), NonceState::Ready { .. }));
        assert!(!manager.restore_unchanged(entry.id));
    }

    #[test]
    fn on_fallback_refresh_same_blockhash_returns_to_ready_same_value() {
        let pk = Pubkey::new_unique();
        let bh = Hash::new_unique();
        let manager = NonceManager::new(vec![(0, pk, bh)]);
        manager.take_ready().unwrap();
        std::thread::sleep(Duration::from_millis(20));
        manager.tick_timeouts(Duration::from_millis(10), Duration::from_secs(5));
        manager.on_fallback_refresh(&pk, bh);
        match manager.get_by_id(0).unwrap().state() {
            NonceState::Ready { blockhash } => assert_eq!(blockhash, bh),
            other => panic!("expected Ready with same bh, got {:?}", other),
        }
    }

    #[test]
    fn on_fallback_refresh_new_blockhash_returns_to_ready_new_value() {
        let pk = Pubkey::new_unique();
        let bh1 = Hash::new_unique();
        let bh2 = Hash::new_unique();
        let manager = NonceManager::new(vec![(0, pk, bh1)]);
        manager.take_ready().unwrap();
        std::thread::sleep(Duration::from_millis(20));
        manager.tick_timeouts(Duration::from_millis(10), Duration::from_secs(5));
        manager.on_fallback_refresh(&pk, bh2);
        match manager.get_by_id(0).unwrap().state() {
            NonceState::Ready { blockhash } => assert_eq!(blockhash, bh2),
            other => panic!("expected Ready with new bh, got {:?}", other),
        }
    }
}
