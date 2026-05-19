//! Integration: SS + YS entry streams → merger → observer → trigger events.

use crossbeam_channel::{bounded, unbounded};
use entry_sources::{EntryObservation, SignatureVec, SourceKind};
use fan_out_bench::counters::BenchCounters;
use fan_out_bench::merger::{spawn as spawn_merger, MergerConfig};
use fan_out_bench::observer::{spawn as spawn_observer, ObserverConfig, HASHES_PER_TICK};
use fan_out_bench::trigger::TriggerEvent;
use solana_sdk::hash::Hash;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn make_obs(slot: u64, entry_hash: Hash, num_hashes: u64, tx_count: u32, source: SourceKind) -> EntryObservation {
    EntryObservation {
        source,
        observed_at: Instant::now(),
        slot,
        entry_index: 0,
        num_hashes,
        entry_hash,
        tx_count,
        signatures: SignatureVec::new(),
        first_shred_at: None,
        leader: None,
    }
}

#[test]
fn ss_only_stream_drives_observer_to_fire_trigger() {
    let mut schedule = HashSet::new();
    schedule.insert((100, 2));

    let (ss_tx, ss_rx) = unbounded();
    let (_ys_tx, ys_rx) = unbounded::<EntryObservation>();
    let (merged_tx, merged_rx) = bounded(100);
    let (trigger_tx, trigger_rx) = bounded::<TriggerEvent>(100);
    let stop = Arc::new(AtomicBool::new(false));
    let counters = Arc::new(BenchCounters::default());
    let current_slot = Arc::new(AtomicU64::new(0));

    let merger_handle = spawn_merger(MergerConfig {
        ss_rx, ys_rx, out_tx: merged_tx,
        pinned_core: None,
        counters: counters.clone(),
        stop: stop.clone(),
    }).unwrap();

    let observer_handle = spawn_observer(ObserverConfig {
        merged_rx,
        schedule: Arc::new(schedule),
        trigger_tx,
        current_slot,
        pinned_core: None,
        counters: counters.clone(),
        stop: stop.clone(),
    }).unwrap();

    ss_tx.send(make_obs(100, Hash::new_unique(), HASHES_PER_TICK, 0, SourceKind::ShredStream)).unwrap();
    ss_tx.send(make_obs(100, Hash::new_unique(), HASHES_PER_TICK, 0, SourceKind::ShredStream)).unwrap();

    std::thread::sleep(Duration::from_millis(100));
    let event = trigger_rx.try_recv().expect("expected trigger");
    assert_eq!(event.slot, 100);
    assert_eq!(event.tick, 2);

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    drop(ss_tx);
    let _ = merger_handle.join();
    let _ = observer_handle.join();
}

#[test]
fn dedup_when_both_sources_emit_same_entry_then_only_one_trigger() {
    let mut schedule = HashSet::new();
    schedule.insert((100, 1));

    let (ss_tx, ss_rx) = unbounded();
    let (ys_tx, ys_rx) = unbounded();
    let (merged_tx, merged_rx) = bounded(100);
    let (trigger_tx, trigger_rx) = bounded::<TriggerEvent>(100);
    let stop = Arc::new(AtomicBool::new(false));
    let counters = Arc::new(BenchCounters::default());
    let current_slot = Arc::new(AtomicU64::new(0));

    let merger_handle = spawn_merger(MergerConfig {
        ss_rx, ys_rx, out_tx: merged_tx,
        pinned_core: None,
        counters: counters.clone(),
        stop: stop.clone(),
    }).unwrap();

    let observer_handle = spawn_observer(ObserverConfig {
        merged_rx,
        schedule: Arc::new(schedule),
        trigger_tx,
        current_slot,
        pinned_core: None,
        counters: counters.clone(),
        stop: stop.clone(),
    }).unwrap();

    let h = Hash::new_unique();
    ss_tx.send(make_obs(100, h, HASHES_PER_TICK, 0, SourceKind::ShredStream)).unwrap();
    ys_tx.send(make_obs(100, h, HASHES_PER_TICK, 0, SourceKind::Yellowstone)).unwrap();

    std::thread::sleep(Duration::from_millis(100));

    assert!(trigger_rx.try_recv().is_ok());
    assert!(trigger_rx.try_recv().is_err(), "should not have second trigger");

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    drop(ss_tx);
    drop(ys_tx);
    let _ = merger_handle.join();
    let _ = observer_handle.join();
}
