//! Observer — tracks PoH ticks per slot from merged entry stream and fires
//! TriggerEvent when schedule (slot, tick) matches.
//!
//! See spec §2.2 + §7.2. Reference impl pattern: crates/tick-trigger-bench/src/observer.rs

use crate::counters::BenchCounters;
use crate::match_event::MatchEvent;
use crate::merger::MergedEntry;
use crate::trigger::TriggerEvent;
use crossbeam_channel::{Receiver, Sender};
use dashmap::DashSet;
use solana_sdk::hash::Hash;
use solana_sdk::signature::Signature;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Instant;

pub const HASHES_PER_TICK: u64 = 62_500;
pub const TICKS_PER_SLOT: u8 = 64;

#[derive(Debug, Default)]
struct SlotState {
    tick_idx: u8,
    hash_count_since_last_tick: u64,
    cumulative_hashes_in_slot: u64,
    seen_entries: HashSet<Hash>,
    fired_ticks: HashSet<u8>,
}

pub struct ObserverConfig {
    pub merged_rx: Receiver<MergedEntry>,
    pub schedule: Arc<HashSet<(u64, u8)>>,
    pub trigger_tx: Sender<TriggerEvent>,
    pub match_tx: Sender<MatchEvent>,
    pub pending_sigs: Arc<DashSet<Signature>>,
    pub current_slot: Arc<AtomicU64>,
    pub pinned_core: Option<usize>,
    pub counters: Arc<BenchCounters>,
    pub stop: Arc<AtomicBool>,
}

pub fn spawn(cfg: ObserverConfig) -> std::io::Result<JoinHandle<()>> {
    std::thread::Builder::new()
        .name("observer".into())
        .spawn(move || {
            if let Some(core) = cfg.pinned_core {
                core_affinity::set_for_current(core_affinity::CoreId { id: core });
            }
            run_loop(cfg);
        })
}

fn run_loop(cfg: ObserverConfig) {
    let mut slot_states: HashMap<u64, SlotState> = HashMap::with_capacity(64);
    let mut last_eviction = Instant::now();

    loop {
        if cfg.stop.load(Ordering::Relaxed) {
            break;
        }
        let merged = match cfg.merged_rx.recv_timeout(std::time::Duration::from_millis(200)) {
            Ok(m) => m,
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                maybe_evict_old_slots(&mut slot_states, &cfg.current_slot, &mut last_eviction);
                continue;
            }
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
        };

        process_entry(&merged, &mut slot_states, &cfg);
        let current = cfg.current_slot.load(Ordering::Relaxed);
        if merged.observation.slot > current {
            cfg.current_slot
                .store(merged.observation.slot, Ordering::Relaxed);
        }
        maybe_evict_old_slots(&mut slot_states, &cfg.current_slot, &mut last_eviction);
    }
}

fn process_entry(merged: &MergedEntry, states: &mut HashMap<u64, SlotState>, cfg: &ObserverConfig) {
    let obs = &merged.observation;
    let state = states.entry(obs.slot).or_default();

    if !state.seen_entries.insert(obs.entry_hash) {
        return;
    }

    state.cumulative_hashes_in_slot = state
        .cumulative_hashes_in_slot
        .saturating_add(obs.num_hashes);
    state.hash_count_since_last_tick = state
        .hash_count_since_last_tick
        .saturating_add(obs.num_hashes);

    let is_tick = obs.tx_count == 0
        && state.hash_count_since_last_tick == HASHES_PER_TICK;

    if is_tick {
        if state.tick_idx < TICKS_PER_SLOT {
            state.tick_idx = state.tick_idx.saturating_add(1);
            let tick_now = state.tick_idx;
            state.hash_count_since_last_tick = 0;

            cfg.counters
                .schedule_contains_calls
                .fetch_add(1, Ordering::Relaxed);
            if cfg.schedule.contains(&(obs.slot, tick_now))
                && state.fired_ticks.insert(tick_now)
            {
                cfg.counters
                    .schedule_contains_true
                    .fetch_add(1, Ordering::Relaxed);
                let event = TriggerEvent {
                    slot: obs.slot,
                    tick: tick_now,
                    cumulative_hashes_in_slot: state.cumulative_hashes_in_slot,
                    observed_at: Instant::now(),
                };
                if cfg.trigger_tx.try_send(event).is_err() {
                    cfg.counters
                        .tick_event_queue_full
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        } else {
            cfg.counters
                .fork_tick_overflow
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    // Sig matching: if this entry contains any of our pending signatures, emit MatchEvent.
    for sig in obs.signatures.iter() {
        if cfg.pending_sigs.remove(sig).is_some() {
            let event = MatchEvent {
                signature: *sig,
                observed_at: Instant::now(),
                observed_slot: obs.slot,
                observed_entry_index: obs.entry_index,
                observed_tick_in_slot: if state.tick_idx > 0 { Some(state.tick_idx) } else { None },
                observed_cumulative_hashes_in_slot: Some(state.cumulative_hashes_in_slot),
                observed_source: merged.first_seen_source,
            };
            if cfg.match_tx.try_send(event).is_err() {
                cfg.counters
                    .match_queue_full
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

fn maybe_evict_old_slots(
    states: &mut HashMap<u64, SlotState>,
    current_slot: &AtomicU64,
    last_eviction: &mut Instant,
) {
    if last_eviction.elapsed() < std::time::Duration::from_millis(500) {
        return;
    }
    *last_eviction = Instant::now();
    let current = current_slot.load(Ordering::Relaxed);
    if current > 64 {
        let cutoff = current.saturating_sub(64);
        states.retain(|s, _| *s >= cutoff);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_channel::{bounded, unbounded};
    use entry_sources::{EntryObservation, SignatureVec, SourceKind};
    use solana_sdk::signature::Signature;
    use std::time::Duration;

    fn make_merged(slot: u64, entry_hash: Hash, num_hashes: u64, tx_count: u32) -> MergedEntry {
        MergedEntry {
            observation: EntryObservation {
                source: SourceKind::ShredStream,
                observed_at: Instant::now(),
                slot,
                entry_index: 0,
                num_hashes,
                entry_hash,
                tx_count,
                signatures: SignatureVec::new(),
                first_shred_at: None,
                leader: None,
            },
            first_seen_source: SourceKind::ShredStream,
        }
    }

    #[allow(clippy::type_complexity)]
    fn setup_observer(
        schedule: HashSet<(u64, u8)>,
    ) -> (
        crossbeam_channel::Sender<MergedEntry>,
        crossbeam_channel::Receiver<TriggerEvent>,
        crossbeam_channel::Receiver<MatchEvent>,
        Arc<DashSet<Signature>>,
        Arc<AtomicBool>,
        Arc<BenchCounters>,
        JoinHandle<()>,
    ) {
        let (in_tx, in_rx) = unbounded();
        let (out_tx, out_rx) = bounded(100);
        let (match_tx, match_rx) = bounded(100);
        let pending_sigs = Arc::new(DashSet::new());
        let stop = Arc::new(AtomicBool::new(false));
        let counters = Arc::new(BenchCounters::default());
        let current_slot = Arc::new(AtomicU64::new(0));
        let handle = spawn(ObserverConfig {
            merged_rx: in_rx,
            schedule: Arc::new(schedule),
            trigger_tx: out_tx,
            match_tx,
            pending_sigs: pending_sigs.clone(),
            current_slot,
            pinned_core: None,
            counters: counters.clone(),
            stop: stop.clone(),
        }).unwrap();
        (in_tx, out_rx, match_rx, pending_sigs, stop, counters, handle)
    }

    fn shutdown(in_tx: crossbeam_channel::Sender<MergedEntry>, stop: Arc<AtomicBool>, handle: JoinHandle<()>) {
        std::thread::sleep(Duration::from_millis(50));
        stop.store(true, Ordering::Relaxed);
        drop(in_tx);
        let _ = handle.join();
    }

    #[test]
    fn counts_one_tick_per_complete_hash_count() {
        let mut schedule = HashSet::new();
        schedule.insert((100, 1));
        let (in_tx, out_rx, _match_rx, _pending_sigs, stop, _counters, handle) = setup_observer(schedule);

        in_tx
            .send(make_merged(100, Hash::new_unique(), HASHES_PER_TICK, 0))
            .unwrap();

        std::thread::sleep(Duration::from_millis(30));
        let event = out_rx.try_recv().expect("expected trigger event");
        assert_eq!(event.slot, 100);
        assert_eq!(event.tick, 1);
        assert_eq!(event.cumulative_hashes_in_slot, HASHES_PER_TICK);

        shutdown(in_tx, stop, handle);
    }

    #[test]
    fn no_trigger_when_schedule_does_not_match() {
        let schedule: HashSet<(u64, u8)> = HashSet::new();
        let (in_tx, out_rx, _match_rx, _pending_sigs, stop, counters, handle) = setup_observer(schedule);

        in_tx
            .send(make_merged(100, Hash::new_unique(), HASHES_PER_TICK, 0))
            .unwrap();
        std::thread::sleep(Duration::from_millis(30));
        assert!(out_rx.try_recv().is_err());
        assert!(counters.schedule_contains_calls.load(Ordering::Relaxed) >= 1);
        assert_eq!(counters.schedule_contains_true.load(Ordering::Relaxed), 0);

        shutdown(in_tx, stop, handle);
    }

    #[test]
    fn ticks_accumulate_to_match_higher_tick_in_schedule() {
        let mut schedule = HashSet::new();
        schedule.insert((100, 3));
        let (in_tx, out_rx, _match_rx, _pending_sigs, stop, _counters, handle) = setup_observer(schedule);

        for _ in 0..3 {
            in_tx
                .send(make_merged(100, Hash::new_unique(), HASHES_PER_TICK, 0))
                .unwrap();
        }
        std::thread::sleep(Duration::from_millis(50));
        let event = out_rx.try_recv().expect("expected trigger at tick 3");
        assert_eq!(event.tick, 3);

        shutdown(in_tx, stop, handle);
    }

    #[test]
    fn duplicate_entry_hashes_ignored() {
        let mut schedule = HashSet::new();
        schedule.insert((100, 1));
        let (in_tx, out_rx, _match_rx, _pending_sigs, stop, _counters, handle) = setup_observer(schedule);

        let h = Hash::new_unique();
        in_tx.send(make_merged(100, h, HASHES_PER_TICK, 0)).unwrap();
        in_tx.send(make_merged(100, h, HASHES_PER_TICK, 0)).unwrap();
        std::thread::sleep(Duration::from_millis(30));

        let event = out_rx.try_recv().unwrap();
        assert_eq!(event.tick, 1);
        assert!(out_rx.try_recv().is_err());

        shutdown(in_tx, stop, handle);
    }

    #[test]
    fn non_tick_entry_does_not_advance_tick() {
        let mut schedule = HashSet::new();
        schedule.insert((100, 1));
        let (in_tx, out_rx, _match_rx, _pending_sigs, stop, _counters, handle) = setup_observer(schedule);

        in_tx
            .send(make_merged(100, Hash::new_unique(), HASHES_PER_TICK, 5))
            .unwrap();
        std::thread::sleep(Duration::from_millis(30));
        assert!(out_rx.try_recv().is_err(), "no trigger for tx-bearing entry");

        shutdown(in_tx, stop, handle);
    }

    #[test]
    fn trigger_not_fired_twice_for_same_tick() {
        let mut schedule = HashSet::new();
        schedule.insert((100, 1));
        let (in_tx, out_rx, _match_rx, _pending_sigs, stop, _counters, handle) = setup_observer(schedule);

        let h1 = Hash::new_unique();
        let h2 = Hash::new_unique();
        in_tx.send(make_merged(100, h1, HASHES_PER_TICK, 0)).unwrap();
        std::thread::sleep(Duration::from_millis(30));
        assert!(out_rx.try_recv().is_ok());

        in_tx.send(make_merged(100, h2, HASHES_PER_TICK, 0)).unwrap();
        std::thread::sleep(Duration::from_millis(30));
        assert!(out_rx.try_recv().is_err());

        shutdown(in_tx, stop, handle);
    }

    #[test]
    fn sig_in_entry_emits_match_event() {
        use solana_sdk::signature::Signature;
        let schedule: HashSet<(u64, u8)> = HashSet::new();
        let (in_tx, _out_rx, match_rx, pending_sigs, stop, _counters, handle) = setup_observer(schedule);

        let sig = Signature::new_unique();
        pending_sigs.insert(sig);

        let mut obs = MergedEntry {
            observation: EntryObservation {
                source: SourceKind::ShredStream,
                observed_at: Instant::now(),
                slot: 100,
                entry_index: 7,
                num_hashes: 1000,
                entry_hash: Hash::new_unique(),
                tx_count: 1,
                signatures: SignatureVec::new(),
                first_shred_at: None,
                leader: None,
            },
            first_seen_source: SourceKind::ShredStream,
        };
        obs.observation.signatures.push(sig);
        in_tx.send(obs).unwrap();

        std::thread::sleep(Duration::from_millis(30));
        let event = match_rx.try_recv().expect("expected match event");
        assert_eq!(event.signature, sig);
        assert_eq!(event.observed_slot, 100);
        assert_eq!(event.observed_entry_index, 7);

        shutdown(in_tx, stop, handle);
    }
}
