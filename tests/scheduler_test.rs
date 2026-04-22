use tx_cutoff::scheduler::{Schedule, SchedulerConfig};

fn sample_config() -> SchedulerConfig {
    SchedulerConfig {
        start_ms: 8500,
        end_ms: 11500,
        step_ms: 50,
        samples_per_wallet_per_slot: 20,
    }
}

#[test]
fn slots_count_is_computed_correctly() {
    let s = Schedule::new(sample_config()).unwrap();
    assert_eq!(s.slots_count(), 61);
}

#[test]
fn total_blocks_equals_slots_times_samples() {
    let s = Schedule::new(sample_config()).unwrap();
    assert_eq!(s.total_blocks(), 61 * 20);
}

#[test]
fn block_index_maps_to_correct_slot_ms() {
    let s = Schedule::new(sample_config()).unwrap();
    for i in 0..20 {
        assert_eq!(s.slot_ms_for(i), Some(8500), "i={}", i);
    }
    for i in 20..40 {
        assert_eq!(s.slot_ms_for(i), Some(8550), "i={}", i);
    }
    assert_eq!(s.slot_ms_for(1200), Some(11500));
    assert_eq!(s.slot_ms_for(1219), Some(11500));
    assert_eq!(s.slot_ms_for(1220), None);
    assert_eq!(s.slot_ms_for(9999), None);
}

#[test]
fn sample_index_is_modulo_samples_per_slot() {
    let s = Schedule::new(sample_config()).unwrap();
    assert_eq!(s.sample_idx_for(0), Some(0));
    assert_eq!(s.sample_idx_for(19), Some(19));
    assert_eq!(s.sample_idx_for(20), Some(0));
    assert_eq!(s.sample_idx_for(1219), Some(19));
    assert_eq!(s.sample_idx_for(1220), None);
}

#[test]
fn invalid_config_end_less_than_start_fails() {
    let c = SchedulerConfig {
        start_ms: 11500,
        end_ms: 8500,
        step_ms: 50,
        samples_per_wallet_per_slot: 20,
    };
    assert!(Schedule::new(c).is_err());
}

#[test]
fn invalid_config_zero_step_fails() {
    let c = SchedulerConfig {
        start_ms: 8500,
        end_ms: 11500,
        step_ms: 0,
        samples_per_wallet_per_slot: 20,
    };
    assert!(Schedule::new(c).is_err());
}

#[test]
fn invalid_config_zero_samples_fails() {
    let c = SchedulerConfig {
        start_ms: 8500,
        end_ms: 11500,
        step_ms: 50,
        samples_per_wallet_per_slot: 0,
    };
    assert!(Schedule::new(c).is_err());
}

#[test]
fn non_divisible_range_still_works() {
    let c = SchedulerConfig {
        start_ms: 8500,
        end_ms: 11499,
        step_ms: 50,
        samples_per_wallet_per_slot: 20,
    };
    let s = Schedule::new(c).unwrap();
    assert_eq!(s.slots_count(), 60);
}
