//! Integration test for nonce subsystem state machine.

use fan_out_bench::nonce::manager::{NonceManager, NonceState};
use solana_sdk::{hash::Hash, pubkey::Pubkey};
use std::time::Duration;

#[test]
fn full_cycle_take_observe_update_returns_ready() {
    let pk = Pubkey::new_unique();
    let bh1 = Hash::new_unique();
    let bh2 = Hash::new_unique();
    let manager = NonceManager::new(vec![(0, pk, bh1)]);

    let (id, pubkey_out, blockhash_out) = manager.take_ready().unwrap();
    assert_eq!(id, 0);
    assert_eq!(pubkey_out, pk);
    assert_eq!(blockhash_out, bh1);
    assert!(matches!(manager.get_by_id(0).unwrap().state(), NonceState::InFlight { .. }));

    manager.on_observed_landing(0);
    assert!(matches!(
        manager.get_by_id(0).unwrap().state(),
        NonceState::AwaitingUpdate { .. }
    ));

    let advanced = manager.on_account_update(&pk, bh2);
    assert!(advanced);
    assert!(matches!(
        manager.get_by_id(0).unwrap().state(),
        NonceState::Ready { .. }
    ));

    let (_id2, _pk2, bh2_out) = manager.take_ready().unwrap();
    assert_eq!(bh2_out, bh2);
}

#[test]
fn stale_recovery_via_fallback() {
    let pk = Pubkey::new_unique();
    let bh1 = Hash::new_unique();
    let bh2 = Hash::new_unique();
    let manager = NonceManager::new(vec![(0, pk, bh1)]);

    manager.take_ready().unwrap();

    std::thread::sleep(Duration::from_millis(20));
    let stale = manager.tick_timeouts(Duration::from_millis(10), Duration::from_secs(5));
    assert_eq!(stale.len(), 1);

    manager.on_fallback_refresh(&pk, bh2);
    match manager.get_by_id(0).unwrap().state() {
        NonceState::Ready { blockhash } => assert_eq!(blockhash, bh2),
        other => panic!("expected Ready, got {:?}", other),
    }
}

#[test]
fn rr_distributes_evenly_across_pool() {
    let n = 10;
    let entries: Vec<_> = (0..n)
        .map(|i| (i as u16, Pubkey::new_unique(), Hash::new_unique()))
        .collect();
    let manager = NonceManager::new(entries);

    let mut taken_ids = Vec::new();
    for _ in 0..n {
        let (id, _, _) = manager.take_ready().unwrap();
        taken_ids.push(id);
    }
    taken_ids.sort();
    let expected: Vec<u16> = (0..n as u16).collect();
    assert_eq!(taken_ids, expected);

    assert!(manager.take_ready().is_none());
}

#[test]
fn no_landing_then_fallback_with_same_blockhash_returns_to_ready_for_reuse() {
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
    let (_id, _pk, bh_out) = manager.take_ready().unwrap();
    assert_eq!(bh_out, bh);
}
