use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crossbeam_channel::{Receiver, Sender};
use dashmap::DashSet;
use entry_sources::EntryObservation;
use solana_sdk::signature::Signature;
use tracing::{info, warn};

use crate::counters::BenchCounters;
use crate::sidecar::TickEvent;
use crate::tx_pool::{PreSignedTx, TxPool};

/// Solana mainnet hashes_per_tick (10M hashes/s / 160 ticks/s). Used to
/// validate ticks per runtime's verify_tick_hash_count: a real PoH tick is
/// an empty entry where cumulative num_hashes since previous tick equals
/// this value. Empty entries that don't match are skipped.
const HASHES_PER_TICK: u64 = 62_500;

#[derive(Debug, Default)]
struct SlotState {
    /// Index of the last valid PoH tick observed in this slot (1..=64).
    tick_idx: u8,
    /// Cumulative num_hashes since the last valid tick.
    hash_count: u64,
}

#[derive(Debug)]
pub struct SendCommand {
    pub tx: PreSignedTx,
    pub schedule_slot: u64,
    pub schedule_tick: u8,
    pub trigger_observed_at: Instant,
}

#[derive(Debug)]
pub struct MatchEvent {
    pub signature: Signature,
    pub observed_at: Instant,
    pub observed_slot: u64,
    pub observed_entry_index: u32,
    pub observed_tick_in_slot: Option<u8>,
}

pub struct ObserverConfig {
    pub entry_rx: Receiver<EntryObservation>,
    pub schedule: Arc<HashSet<(u64, u8)>>,
    pub pool: TxPool,
    pub send_queue: Sender<SendCommand>,
    pub match_queue: Sender<MatchEvent>,
    pub pending_sigs: Arc<DashSet<Signature>>,
    pub current_slot: Arc<AtomicU64>,
    pub tick_event_tx: Sender<TickEvent>,
    pub pinned_core: Option<usize>,
    pub counters: Arc<BenchCounters>,
    pub stop: Arc<AtomicBool>,
}

pub fn spawn(cfg: ObserverConfig) -> std::io::Result<std::thread::JoinHandle<()>> {
    std::thread::Builder::new()
        .name("ss-observer".into())
        .spawn(move || {
            if let Some(c) = cfg.pinned_core {
                core_affinity::set_for_current(core_affinity::CoreId { id: c });
            }
            run_loop(cfg);
        })
}

fn run_loop(cfg: ObserverConfig) {
    let mut slot_state: HashMap<u64, SlotState> = HashMap::with_capacity(2048);
    // Skip the first slot we observe (we may have subscribed mid-slot;
    // its cumulative hash count is incomplete and tick numbering would be off).
    let mut first_slot_seen: Option<u64> = None;

    loop {
        if cfg.stop.load(Ordering::Relaxed) {
            break;
        }
        let obs = match cfg.entry_rx.recv() {
            Ok(o) => o,
            Err(_) => break,
        };
        let observed_at = obs.observed_at;
        let slot = obs.slot;

        if slot_state.len() > 4096 {
            slot_state.retain(|s, _| *s + 200 >= slot);
        }

        cfg.current_slot.store(slot, Ordering::Relaxed);

        // Establish first-slot warmup boundary
        if first_slot_seen.is_none() {
            first_slot_seen = Some(slot);
        }
        let in_warmup = Some(slot) == first_slot_seen;
        // Still process match path during warmup (sigs irrelevant for tick numbering)
        // but skip tick counting + schedule trigger.

        if !in_warmup {
            let state = slot_state.entry(slot).or_default();
            state.hash_count = state.hash_count.saturating_add(obs.num_hashes);

            // is_tick() per solana_entry: transactions.is_empty().
            // Per verify_tick_hash_count: a VALID tick has cumulative
            // num_hashes since previous tick == HASHES_PER_TICK. Empty
            // entries that don't match (fork dup, empty data entries
            // emitted by some leaders) are skipped.
            if obs.tx_count == 0 {
                if state.hash_count == HASHES_PER_TICK {
                    state.tick_idx = state.tick_idx.saturating_add(1);
                    state.hash_count = 0;
                    let tick_val = state.tick_idx;

                    let ev = TickEvent {
                        observed_at,
                        slot,
                        tick_idx: tick_val,
                        num_hashes: obs.num_hashes,
                    };
                    if cfg.tick_event_tx.try_send(ev).is_err() {
                        cfg.counters.inc(&cfg.counters.tick_event_queue_full);
                    }

                    if tick_val > 64 {
                        cfg.counters.inc(&cfg.counters.fork_tick_overflow);
                        continue;
                    }

                    if cfg.schedule.contains(&(slot, tick_val)) {
                        if let Some(tx) = cfg.pool.take(slot, tick_val) {
                            let cmd = SendCommand {
                                tx,
                                schedule_slot: slot,
                                schedule_tick: tick_val,
                                trigger_observed_at: observed_at,
                            };
                            if cfg.send_queue.try_send(cmd).is_err() {
                                cfg.counters.inc(&cfg.counters.send_queue_full);
                            }
                        } else {
                            cfg.counters.inc(&cfg.counters.pool_empty);
                        }
                    }
                } else {
                    // Empty entry but cumulative hash count != HASHES_PER_TICK.
                    // Either fork duplicate or leader-emitted no-op entry.
                    cfg.counters.inc(&cfg.counters.fork_tick_overflow);
                }
            }
        }

        for sig in &obs.signatures {
            if cfg.pending_sigs.contains(sig) {
                let ev = MatchEvent {
                    signature: *sig,
                    observed_at,
                    observed_slot: slot,
                    observed_entry_index: obs.entry_index,
                    observed_tick_in_slot: slot_state.get(&slot).map(|s| s.tick_idx),
                };
                if cfg.match_queue.try_send(ev).is_err() {
                    cfg.counters.inc(&cfg.counters.match_queue_full);
                }
            }
        }
    }
    info!("ss-observer thread exiting");
    warn!(slot_state_len = slot_state.len(), "observer final state");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_channel::bounded;
    use entry_sources::observation::{SignatureVec, SourceKind};
    use solana_sdk::hash::Hash;

    fn make_tick(slot: u64, num_hashes: u64, entry_index: u32) -> EntryObservation {
        EntryObservation {
            source: SourceKind::ShredStream,
            observed_at: Instant::now(),
            slot,
            entry_index,
            num_hashes,
            entry_hash: Hash::new_unique(),
            tx_count: 0,
            signatures: SignatureVec::new(),
            first_shred_at: None,
            leader: None,
        }
    }

    #[test]
    fn trigger_fires_when_schedule_matches() {
        let (entry_tx, entry_rx) = bounded(16);
        let (send_tx, send_rx) = bounded::<SendCommand>(16);
        let (match_tx, _match_rx) = bounded::<MatchEvent>(16);
        let (tick_ev_tx, _tick_ev_rx) = bounded::<TickEvent>(16);

        let mut schedule_inner: HashSet<(u64, u8)> = HashSet::new();
        schedule_inner.insert((1000, 2));
        let schedule = Arc::new(schedule_inner);

        let pool = TxPool::new();
        pool.insert(
            1000,
            2,
            crate::tx_pool::PreSignedTx {
                serialized: vec![0u8; 200],
                signature: Signature::default(),
                blockhash: Hash::default(),
                built_at: Instant::now(),
            },
        );

        let pending = Arc::new(DashSet::new());
        let current_slot = Arc::new(AtomicU64::new(0));
        let counters = Arc::new(BenchCounters::default());
        let stop = Arc::new(AtomicBool::new(false));

        let handle = spawn(ObserverConfig {
            entry_rx,
            schedule: schedule.clone(),
            pool,
            send_queue: send_tx,
            match_queue: match_tx,
            pending_sigs: pending.clone(),
            current_slot: current_slot.clone(),
            tick_event_tx: tick_ev_tx,
            pinned_core: None,
            counters: counters.clone(),
            stop: stop.clone(),
        })
        .unwrap();

        // Warmup slot first (will be skipped per first-slot policy)
        entry_tx.send(make_tick(999, 62_500, 0)).unwrap();
        // Then real test slot
        entry_tx.send(make_tick(1000, 62_500, 0)).unwrap();
        entry_tx.send(make_tick(1000, 62_500, 1)).unwrap();
        entry_tx.send(make_tick(1000, 62_500, 2)).unwrap();

        let cmd = send_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap();
        assert_eq!(cmd.schedule_slot, 1000);
        assert_eq!(cmd.schedule_tick, 2);
        stop.store(true, Ordering::Relaxed);
        drop(entry_tx);
        let _ = handle.join();
    }

    #[test]
    fn pool_empty_increments_counter() {
        let (entry_tx, entry_rx) = bounded(16);
        let (send_tx, _send_rx) = bounded::<SendCommand>(16);
        let (match_tx, _match_rx) = bounded::<MatchEvent>(16);
        let (tick_ev_tx, _tick_ev_rx) = bounded::<TickEvent>(16);

        let mut schedule_inner: HashSet<(u64, u8)> = HashSet::new();
        schedule_inner.insert((1000, 1));
        let schedule = Arc::new(schedule_inner);

        let pool = TxPool::new();
        let pending = Arc::new(DashSet::new());
        let current_slot = Arc::new(AtomicU64::new(0));
        let counters = Arc::new(BenchCounters::default());
        let stop = Arc::new(AtomicBool::new(false));

        let handle = spawn(ObserverConfig {
            entry_rx,
            schedule: schedule.clone(),
            pool,
            send_queue: send_tx,
            match_queue: match_tx,
            pending_sigs: pending.clone(),
            current_slot: current_slot.clone(),
            tick_event_tx: tick_ev_tx,
            pinned_core: None,
            counters: counters.clone(),
            stop: stop.clone(),
        })
        .unwrap();

        // Warmup slot first
        entry_tx.send(make_tick(999, 62_500, 0)).unwrap();
        entry_tx.send(make_tick(1000, 62_500, 0)).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(100));
        assert_eq!(counters.snapshot().pool_empty, 1);
        stop.store(true, Ordering::Relaxed);
        drop(entry_tx);
        let _ = handle.join();
    }
}
