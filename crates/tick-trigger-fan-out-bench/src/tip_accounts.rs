//! Per-vendor tip account lists + round-robin rotator.
//!
//! Each vendor publishes a small set of tip accounts; rotating per tx
//! balances load and avoids the "all txs hit the same account" anti-pattern
//! that vendors sometimes rate-limit on. Future phases (Jito, Nozomi,
//! bloXroute, etc.) get their own slices here.

use crate::config::SenderKind;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;

/// Helius `/fast` SWQoS tip accounts (mainnet). Published in their docs.
/// Rotating through these distributes load and avoids one account becoming
/// a bottleneck.
const HELIUS_TIP_ACCOUNTS_STR: &[&str] = &[
    "4ACfpUFoaSD9bfPdeu6DBt89gB6ENTeHBXCAi87NhDEE",
    "D2L6yPZ2FmmmTKPgzaMKdhu6EWZcTpLy1Vhx8uvAfrLT",
    "9bnz4RShgq1hAnLnZbP8kbgBg1kEmcJBYQq3gQbmnSta",
    "5VY91ws6B2hMmBFRsXkoAAdsPHBJwRfBht4DXox3xkwn",
    "2nyhqdwKcJZR2vcqCyrYsaPVdAnFoJjiksCXJ7hfEYgD",
    "2q5pghRs6arqVjRvT5gfgWfWcHWmw1ZuCzphgd5KfWGJ",
    "wyvPkWjVZz1M8fHQnMMCDTQDbkManefNNhweYk5WkcF",
    "3KCKozbAaF75qEU33jtzozcJ29yJuaLJTy2jFdzUY8bT",
    "4vieeGHPYPG2MmyPRcYjdiDmmhN3ww7hsFNap8pVN3Ey",
    "4TQLFNWK8AovT1gFvda5jfw2oJeRMKEmw7aH6MGBJ3or",
];

pub fn helius_tip_accounts() -> &'static [Pubkey] {
    static CACHED: OnceLock<Vec<Pubkey>> = OnceLock::new();
    CACHED.get_or_init(|| {
        HELIUS_TIP_ACCOUNTS_STR
            .iter()
            .map(|s| Pubkey::from_str(s).expect("hardcoded helius tip account parses"))
            .collect()
    })
}

/// Return the tip account list for a given sender kind. Empty slice means
/// no tip account (sender protocol does not use them).
pub fn tip_accounts_for(kind: SenderKind) -> &'static [Pubkey] {
    match kind {
        SenderKind::Helius => helius_tip_accounts(),
    }
}

/// Round-robin rotator over a tip account list. Single-threaded merger
/// safety + lock-free read make it cheap on the hot path (a single
/// `fetch_add` + modulo).
pub struct TipAccountRotator {
    accounts: Vec<Pubkey>,
    cursor: AtomicUsize,
}

impl TipAccountRotator {
    pub fn new(accounts: Vec<Pubkey>) -> Self {
        Self {
            accounts,
            cursor: AtomicUsize::new(0),
        }
    }

    /// Returns the next account in rotation. `None` if the list is empty.
    pub fn next(&self) -> Option<Pubkey> {
        if self.accounts.is_empty() {
            return None;
        }
        let idx = self.cursor.fetch_add(1, Ordering::Relaxed) % self.accounts.len();
        Some(self.accounts[idx])
    }

    pub fn len(&self) -> usize {
        self.accounts.len()
    }

    pub fn is_empty(&self) -> bool {
        self.accounts.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn helius_list_loads_and_parses() {
        let list = helius_tip_accounts();
        assert!(list.len() >= 10);
        // All distinct.
        let mut sorted = list.to_vec();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), list.len());
    }

    #[test]
    fn rotator_cycles_in_order() {
        let r = TipAccountRotator::new(helius_tip_accounts().to_vec());
        let a = r.next().unwrap();
        let b = r.next().unwrap();
        assert_ne!(a, b);
        // After len() calls we wrap back to start.
        for _ in 0..(r.len() - 2) {
            r.next();
        }
        let wrapped = r.next().unwrap();
        assert_eq!(wrapped, a);
    }

    #[test]
    fn empty_rotator_returns_none() {
        let r = TipAccountRotator::new(vec![]);
        assert!(r.next().is_none());
    }
}
