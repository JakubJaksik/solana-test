use tx_cutoff::engine::AbortTracker;

#[test]
fn abort_tracker_triggers_on_n_consecutive_all_fail_blocks() {
    let mut a = AbortTracker::new(3);
    a.record_block(0, 5);
    a.record_block(0, 5);
    a.record_block(0, 5);
    assert!(a.should_abort());
}

#[test]
fn abort_tracker_resets_on_partial_success() {
    let mut a = AbortTracker::new(3);
    a.record_block(0, 5);
    a.record_block(0, 5);
    a.record_block(1, 5);
    a.record_block(0, 5);
    assert!(!a.should_abort());
}

#[test]
fn abort_tracker_never_triggers_when_threshold_is_huge() {
    let mut a = AbortTracker::new(u64::MAX);
    for _ in 0..1000 {
        a.record_block(0, 5);
    }
    assert!(!a.should_abort());
}
