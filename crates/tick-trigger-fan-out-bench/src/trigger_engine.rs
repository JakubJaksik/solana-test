//! Trigger engine — hot path consumer of the supervisor's OrderedEvent stream.
//!
//! Per `Entry` event, the engine does TWO things in a single pass:
//!
//!   1. **Schedule check** — if `(slot, tick_idx_in_slot)` matches the
//!      currently-loaded schedule (ArcSwap snapshot), emit a `TriggerEvent`.
//!      The `fired_ticks` set prevents firing twice for the same scheduled
//!      tick (tick_idx can advance through the same number multiple times
//!      under late-tick counting — see poh_supervisor).
//!
//!   2. **Signature match** — for each signature carried by the entry,
//!      remove it from `pending_sigs` if present. If hit, emit a `MatchEvent`.
//!      Per-entry sig count is small (mainnet entries usually have 0-3 sigs;
//!      tick entries have 0).
//!
//! All hot-path operations: O(1) hash lookup (ArcSwap snapshot for schedule,
//! DashSet for pending sigs). Atomic counters with `Ordering::Relaxed`.
//! Zero allocations on the fast path beyond the event channel try_send.

use crate::nonce::local_compute::SlotHashCache as NonceSlotHashCache;
use crate::poh_supervisor::OrderedEvent;
use arc_swap::ArcSwap;
use crossbeam_channel::{Receiver, Sender};
use dashmap::DashSet;
use entry_sources::SourceKind;
use solana_sdk::signature::Signature;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Instant;

/// Unique identifier for a trigger fire. Used to correlate downstream
/// SendEvent/MatchEvent records back to their originating trigger.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize)]
pub struct TriggerId(pub u64);

impl TriggerId {
    /// Encode (slot, tick) deterministically. Distinct (slot, tick) → distinct
    /// id; lets the recorder use the id as a map key.
    pub fn from_slot_tick(slot: u64, tick: u8) -> Self {
        // Pack: top 56 bits = slot (enough for >>1000 years at 2.5 slots/sec),
        // low 8 bits = tick.
        Self((slot << 8) | tick as u64)
    }
}

#[derive(Debug, Clone)]
pub struct TriggerEvent {
    pub trigger_id: TriggerId,
    pub slot: u64,
    pub tick: u8,
    pub trigger_observed_at: Instant,
    /// True if the supervisor reported tick_uncertain on this entry — i.e.
    /// at least one `Missing` was emitted earlier in this slot, so the
    /// `tick` index may be off-by-one from on-chain reality. Consumer can
    /// choose to drop the trigger or fire anyway.
    pub tick_uncertain: bool,
}

#[derive(Debug, Clone)]
pub struct MatchEvent {
    pub signature: Signature,
    pub observed_at: Instant,
    pub observed_slot: u64,
    pub observed_entry_index: u32,
    pub observed_tick: u8,
    pub observed_source: SourceKind,
}

#[derive(Debug, Default)]
pub struct TriggerEngineCounters {
    /// Number of Entry events observed.
    pub entries_seen: AtomicU64,
    /// Schedule lookups that matched. Equals total triggers fired
    /// (modulo `trigger_tx_full`).
    pub schedule_hits: AtomicU64,
    /// Triggers fired but downstream channel was full → dropped.
    pub trigger_tx_full: AtomicU64,
    /// Triggers suppressed by `fired_ticks` (already fired this tick).
    pub schedule_duplicate_suppressed: AtomicU64,
    /// Triggers fired with tick_uncertain set.
    pub triggers_with_uncertain_tick: AtomicU64,
    /// Signatures looked up in `pending_sigs` (one per signature per entry).
    pub sig_lookups: AtomicU64,
    /// Sig hits → MatchEvent emitted.
    pub sig_hits: AtomicU64,
    /// MatchEvent try_send failed (channel full).
    pub match_tx_full: AtomicU64,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct TriggerEngineCountersSnapshot {
    pub entries_seen: u64,
    pub schedule_hits: u64,
    pub trigger_tx_full: u64,
    pub schedule_duplicate_suppressed: u64,
    pub triggers_with_uncertain_tick: u64,
    pub sig_lookups: u64,
    pub sig_hits: u64,
    pub match_tx_full: u64,
}

impl TriggerEngineCounters {
    pub fn snapshot(&self) -> TriggerEngineCountersSnapshot {
        let l = |c: &AtomicU64| c.load(Ordering::Relaxed);
        TriggerEngineCountersSnapshot {
            entries_seen: l(&self.entries_seen),
            schedule_hits: l(&self.schedule_hits),
            trigger_tx_full: l(&self.trigger_tx_full),
            schedule_duplicate_suppressed: l(&self.schedule_duplicate_suppressed),
            triggers_with_uncertain_tick: l(&self.triggers_with_uncertain_tick),
            sig_lookups: l(&self.sig_lookups),
            sig_hits: l(&self.sig_hits),
            match_tx_full: l(&self.match_tx_full),
        }
    }
}

pub struct TriggerEngineConfig {
    pub ordered_rx: Receiver<OrderedEvent>,
    /// Live schedule snapshot. Updated by a separate pump thread via `store()`.
    pub schedule: Arc<ArcSwap<HashSet<(u64, u8)>>>,
    /// Sigs we're waiting to see on-chain. Senders insert before sending;
    /// engine removes on first match.
    pub pending_sigs: Arc<DashSet<Signature>>,
    pub trigger_tx: Sender<TriggerEvent>,
    pub match_tx: Sender<MatchEvent>,
    pub counters: Arc<TriggerEngineCounters>,
    pub stop: Arc<AtomicBool>,
    pub pinned_core: Option<usize>,
    /// Optional nonce slot-hash cache. When `Some`, engine pushes
    /// `SlotComplete.last_entry_hash` into this cache so the recorder can
    /// later compute the next durable nonce without an RPC round-trip.
    pub nonce_slot_hash_cache: Option<Arc<NonceSlotHashCache>>,
}

pub fn spawn(cfg: TriggerEngineConfig) -> std::io::Result<JoinHandle<()>> {
    std::thread::Builder::new()
        .name("trigger-engine".into())
        .spawn(move || {
            if let Some(core) = cfg.pinned_core {
                core_affinity::set_for_current(core_affinity::CoreId { id: core });
            }
            run_loop(cfg);
        })
}

fn run_loop(cfg: TriggerEngineConfig) {
    // Track which (slot, tick) we've already fired for in the current window
    // so we don't re-fire when tick_idx advances through the same value via
    // late-tick counting. We compact this set as slots age out — keep last
    // 256 slots only (~100s of history).
    let mut fired: HashSet<(u64, u8)> = HashSet::with_capacity(1024);
    let mut last_compact_slot: u64 = 0;
    loop {
        if cfg.stop.load(Ordering::Relaxed) {
            break;
        }
        let event = match cfg
            .ordered_rx
            .recv_timeout(std::time::Duration::from_millis(200))
        {
            Ok(e) => e,
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
        };
        // Slot lifecycle events feed the nonce slot_hash_cache (when nonce
        // mode is enabled). Other variants are no-ops for the engine.
        match &event {
            OrderedEvent::SlotComplete(c) => {
                if let Some(cache) = &cfg.nonce_slot_hash_cache {
                    cache.record_slot_complete(c.slot, c.last_entry_hash);
                }
            }
            OrderedEvent::SlotIncomplete(i) => {
                // Best-effort: still push the highest-seen hash. Recorder
                // can fall back to RPC if local-compute produces a wrong
                // nonce (tx will silently fail on chain → matcher times out).
                if let Some(cache) = &cfg.nonce_slot_hash_cache {
                    cache.record_slot_complete(i.slot, i.last_entry_hash);
                }
            }
            _ => {}
        }
        if let OrderedEvent::Entry(e) = event {
            cfg.counters.entries_seen.fetch_add(1, Ordering::Relaxed);

            // Feed the nonce slot-hash cache on EVERY entry (not just on
            // SlotComplete). Supervisor emits entries in PoH order so the
            // final write for slot S is the correct last_entry_hash by the
            // time we observe slot S+1. This eliminates the seal_lag race
            // where MatchEvent arrives ~100ms after a sig lands but
            // SlotComplete trails by ~4s (seal_lag * 400ms).
            if let Some(cache) = &cfg.nonce_slot_hash_cache {
                cache.record_entry(e.slot, e.observation.entry_hash);
            }

            // Schedule check.
            let sched = cfg.schedule.load();
            let key = (e.slot, e.tick_idx_in_slot);
            if e.tick_idx_in_slot > 0 && sched.contains(&key) {
                if fired.insert(key) {
                    cfg.counters.schedule_hits.fetch_add(1, Ordering::Relaxed);
                    if e.tick_uncertain {
                        cfg.counters
                            .triggers_with_uncertain_tick
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    let trig = TriggerEvent {
                        trigger_id: TriggerId::from_slot_tick(e.slot, e.tick_idx_in_slot),
                        slot: e.slot,
                        tick: e.tick_idx_in_slot,
                        trigger_observed_at: Instant::now(),
                        tick_uncertain: e.tick_uncertain,
                    };
                    if cfg.trigger_tx.try_send(trig).is_err() {
                        cfg.counters.trigger_tx_full.fetch_add(1, Ordering::Relaxed);
                    }
                } else {
                    cfg.counters
                        .schedule_duplicate_suppressed
                        .fetch_add(1, Ordering::Relaxed);
                }
            }

            // Sig match.
            for sig in e.observation.signatures.iter() {
                cfg.counters.sig_lookups.fetch_add(1, Ordering::Relaxed);
                if cfg.pending_sigs.remove(sig).is_some() {
                    cfg.counters.sig_hits.fetch_add(1, Ordering::Relaxed);
                    let m = MatchEvent {
                        signature: *sig,
                        observed_at: Instant::now(),
                        observed_slot: e.slot,
                        observed_entry_index: e.entry_index,
                        observed_tick: e.tick_idx_in_slot,
                        observed_source: e.observation.source,
                    };
                    if cfg.match_tx.try_send(m).is_err() {
                        cfg.counters.match_tx_full.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }

            // Periodic compaction: drop `fired` entries from slots
            // older than e.slot - 256. Cheap because we only touch
            // when slot advances meaningfully.
            if e.slot > last_compact_slot + 256 {
                last_compact_slot = e.slot;
                let cutoff = e.slot.saturating_sub(256);
                fired.retain(|(s, _)| *s >= cutoff);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::poh_supervisor::OrderedEntry;
    use crossbeam_channel::{bounded, unbounded};
    use entry_sources::{EntryObservation, SignatureVec};

    fn entry_event(slot: u64, idx: u32, tick: u8, sigs: Vec<Signature>) -> OrderedEvent {
        let mut sv = SignatureVec::new();
        for s in sigs {
            sv.push(s);
        }
        OrderedEvent::Entry(OrderedEntry {
            slot,
            entry_index: idx,
            tick_idx_in_slot: tick,
            cumulative_hashes_in_slot: 62500 * tick as u64,
            observation: EntryObservation {
                source: SourceKind::ShredStream,
                observed_at: Instant::now(),
                slot,
                entry_index: idx,
                num_hashes: 0,
                entry_hash: solana_sdk::hash::Hash::default(),
                tx_count: if sigs_empty(&sv) { 0 } else { sv.len() as u32 },
                signatures: sv,
                first_shred_at: None,
                leader: None,
            },
            was_reordered: false,
            wait_duration_us: 0,
            tick_uncertain: false,
        })
    }

    fn sigs_empty(v: &SignatureVec) -> bool {
        v.is_empty()
    }

    #[test]
    fn schedule_hit_fires_trigger_once() {
        let (in_tx, in_rx) = unbounded::<OrderedEvent>();
        let (tt, tr) = bounded::<TriggerEvent>(16);
        let (mt, _mr) = bounded::<MatchEvent>(16);
        let sched: Arc<ArcSwap<HashSet<(u64, u8)>>> =
            Arc::new(ArcSwap::from_pointee(HashSet::from([(100u64, 5u8)])));
        let pending = Arc::new(DashSet::new());
        let counters = Arc::new(TriggerEngineCounters::default());
        let stop = Arc::new(AtomicBool::new(false));
        let h = spawn(TriggerEngineConfig {
            ordered_rx: in_rx,
            schedule: sched.clone(),
            pending_sigs: pending.clone(),
            trigger_tx: tt,
            match_tx: mt,
            counters: counters.clone(),
            stop: stop.clone(),
            pinned_core: None,
            nonce_slot_hash_cache: None,
        })
        .unwrap();
        in_tx.send(entry_event(100, 5, 5, vec![])).unwrap();
        // Second event with the same (slot, tick) — should be suppressed.
        in_tx.send(entry_event(100, 6, 5, vec![])).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(40));
        stop.store(true, Ordering::Relaxed);
        drop(in_tx);
        let _ = h.join();
        let snap = counters.snapshot();
        assert_eq!(snap.schedule_hits, 1);
        assert_eq!(snap.schedule_duplicate_suppressed, 1);
        let mut got = Vec::new();
        while let Ok(t) = tr.try_recv() {
            got.push(t);
        }
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].slot, 100);
        assert_eq!(got[0].tick, 5);
    }

    #[test]
    fn sig_match_emits_match_event_and_removes_pending() {
        let (in_tx, in_rx) = unbounded::<OrderedEvent>();
        let (tt, _tr) = bounded::<TriggerEvent>(16);
        let (mt, mr) = bounded::<MatchEvent>(16);
        let sched: Arc<ArcSwap<HashSet<(u64, u8)>>> =
            Arc::new(ArcSwap::from_pointee(HashSet::new()));
        let pending = Arc::new(DashSet::new());
        let sig = Signature::new_unique();
        pending.insert(sig);
        let counters = Arc::new(TriggerEngineCounters::default());
        let stop = Arc::new(AtomicBool::new(false));
        let h = spawn(TriggerEngineConfig {
            ordered_rx: in_rx,
            schedule: sched,
            pending_sigs: pending.clone(),
            trigger_tx: tt,
            match_tx: mt,
            counters: counters.clone(),
            stop: stop.clone(),
            pinned_core: None,
            nonce_slot_hash_cache: None,
        })
        .unwrap();
        in_tx.send(entry_event(100, 0, 0, vec![sig])).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(40));
        stop.store(true, Ordering::Relaxed);
        drop(in_tx);
        let _ = h.join();
        let snap = counters.snapshot();
        assert_eq!(snap.sig_hits, 1);
        let m = mr.try_recv().unwrap();
        assert_eq!(m.signature, sig);
        assert!(!pending.contains(&sig));
    }

    #[test]
    fn missed_schedule_does_not_fire() {
        let (in_tx, in_rx) = unbounded::<OrderedEvent>();
        let (tt, tr) = bounded::<TriggerEvent>(16);
        let (mt, _mr) = bounded::<MatchEvent>(16);
        let sched: Arc<ArcSwap<HashSet<(u64, u8)>>> =
            Arc::new(ArcSwap::from_pointee(HashSet::from([(100u64, 5u8)])));
        let counters = Arc::new(TriggerEngineCounters::default());
        let stop = Arc::new(AtomicBool::new(false));
        let h = spawn(TriggerEngineConfig {
            ordered_rx: in_rx,
            schedule: sched,
            pending_sigs: Arc::new(DashSet::new()),
            trigger_tx: tt,
            match_tx: mt,
            counters: counters.clone(),
            stop: stop.clone(),
            pinned_core: None,
            nonce_slot_hash_cache: None,
        })
        .unwrap();
        // Tick 6 — not scheduled.
        in_tx.send(entry_event(100, 0, 6, vec![])).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(40));
        stop.store(true, Ordering::Relaxed);
        drop(in_tx);
        let _ = h.join();
        assert!(tr.try_recv().is_err());
        assert_eq!(counters.snapshot().schedule_hits, 0);
    }
}
