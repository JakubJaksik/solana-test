use alloy::primitives::B256;
use std::collections::HashSet;
use tx_cutoff::tracker::{InclusionStatus, Tracker};

fn h(n: u8) -> B256 {
    let mut b = [0u8; 32];
    b[31] = n;
    B256::from(b)
}

#[test]
fn tx_included_in_target_block_marked_target() {
    let mut t = Tracker::new(10);
    t.record_sent(h(1), 100, 101);
    let mut hashes = HashSet::new();
    hashes.insert(h(1));
    t.observe_block(101, &hashes);
    assert_eq!(t.status(&h(1)), Some(InclusionStatus::IncludedTarget));
}

#[test]
fn tx_included_late_marked_with_offset() {
    let mut t = Tracker::new(10);
    t.record_sent(h(2), 100, 101);
    let empty = HashSet::new();
    t.observe_block(101, &empty);
    let mut hashes = HashSet::new();
    hashes.insert(h(2));
    t.observe_block(103, &hashes); // 2 blocks late
    assert_eq!(t.status(&h(2)), Some(InclusionStatus::IncludedLate(2)));
}

#[test]
fn tx_not_seen_within_lookahead_marked_dropped() {
    let mut t = Tracker::new(3);
    t.record_sent(h(3), 100, 101);
    let empty = HashSet::new();
    t.observe_block(101, &empty);
    t.observe_block(102, &empty);
    t.observe_block(103, &empty);
    t.observe_block(104, &empty);
    t.observe_block(105, &empty); // lookahead 3 exceeded
    assert_eq!(t.status(&h(3)), Some(InclusionStatus::Dropped));
}

#[test]
fn tx_with_send_error_is_recorded() {
    let mut t = Tracker::new(10);
    t.record_send_error(h(4), "nonce_too_low".into());
    assert_eq!(
        t.status(&h(4)),
        Some(InclusionStatus::SendError("nonce_too_low".into()))
    );
}

#[test]
fn pending_count_shrinks_as_classifications_land() {
    let mut t = Tracker::new(10);
    t.record_sent(h(5), 100, 101);
    t.record_sent(h(6), 100, 101);
    assert_eq!(t.pending_count(), 2);
    let mut hashes = HashSet::new();
    hashes.insert(h(5));
    t.observe_block(101, &hashes);
    assert_eq!(t.pending_count(), 1);
}

#[test]
fn drain_resolved_returns_only_resolved_once() {
    let mut t = Tracker::new(10);
    t.record_sent(h(7), 100, 101);
    let mut hashes = HashSet::new();
    hashes.insert(h(7));
    t.observe_block(101, &hashes);
    let first = t.drain_resolved();
    assert_eq!(first.len(), 1);
    let second = t.drain_resolved();
    assert_eq!(second.len(), 0, "drain should consume");
}
