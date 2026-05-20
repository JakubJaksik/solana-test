//! PoH supervisor — reorders the merger's dedup'd stream into strict PoH
//! order, fills gaps with explicit `Missing` markers after a timeout, and
//! emits slot lifecycle events so downstream consumers always know exactly
//! what they have (entry / missing / slot end).
//!
//! Inputs:  `MergedEntry` stream from the entry merger (arrival order).
//! Outputs: `OrderedEvent` stream in strict PoH order per slot.
//!
//! Pipeline guarantees:
//! - For any slot S, events arrive in `entry_index` order: 0, 1, 2, ...
//! - A gap that doesn't fill within `entry_timeout` produces an explicit
//!   `Missing { slot, entry_index }` event; subsequent entries continue
//!   without further delay.
//! - Every observed slot eventually yields either `SlotComplete` (tick 64
//!   reached) or `SlotIncomplete` (sealed before tick 64) — never both,
//!   never neither.
//! - The `tick_uncertain` flag on `Entry` is set once any `Missing` was
//!   emitted earlier in that slot, so consumers can choose to skip
//!   tick-sensitive actions (e.g. tick-aligned trigger firing) for that
//!   slot.

use crate::merger::MergedEntry;
use crossbeam_channel::{Receiver, Sender};
use entry_sources::EntryObservation;
use serde::Serialize;
use solana_sdk::hash::Hash;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// Solana PoH constants — must mirror what observer.rs uses elsewhere.
pub const HASHES_PER_TICK: u64 = 62_500;
pub const TICKS_PER_SLOT: u8 = 64;

// `Entry` is much larger than the other variants (it carries the full
// `EntryObservation` including a `SmallVec<[Signature; 8]>`). Boxing it would
// add a heap allocation per emit on the hot path; we accept the size
// asymmetry to keep emit zero-alloc.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum OrderedEvent {
    Entry(OrderedEntry),
    Missing(OrderedMissing),
    SlotComplete(OrderedSlotComplete),
    SlotIncomplete(OrderedSlotIncomplete),
}

#[derive(Debug)]
pub struct OrderedEntry {
    pub slot: u64,
    pub entry_index: u32,
    /// 0 = no tick observed yet in slot, 1..=64 = tick number this entry is at.
    pub tick_idx_in_slot: u8,
    /// Sum of `num_hashes` for all entries observed so far in this slot. If
    /// any `Missing` was emitted earlier, this is an under-estimate.
    pub cumulative_hashes_in_slot: u64,
    pub observation: EntryObservation,
    /// True when this entry sat in the reorder buffer before being emitted.
    pub was_reordered: bool,
    /// Microseconds spent in the reorder buffer (0 if was_reordered = false).
    pub wait_duration_us: u32,
    /// True if any `Missing` was emitted earlier in this slot. PoH counters
    /// (`tick_idx_in_slot`, `cumulative_hashes_in_slot`) are best-effort once
    /// this is set.
    pub tick_uncertain: bool,
}

#[derive(Debug)]
pub struct OrderedMissing {
    pub slot: u64,
    pub entry_index: u32,
    pub waited_for_us: u32,
}

#[derive(Debug)]
pub struct OrderedSlotComplete {
    pub slot: u64,
    pub total_entries: u32,
    pub last_entry_hash: Hash,
    pub last_tick_observed: u8,
    pub missing_count: u32,
}

#[derive(Debug)]
pub struct OrderedSlotIncomplete {
    pub slot: u64,
    pub last_seen_index: u32,
    /// Best-effort: hash of the highest-index entry we did see.
    pub last_entry_hash: Hash,
    pub last_tick_observed: u8,
    pub missing_count: u32,
}

#[derive(Default, Debug)]
pub struct PohSupervisorCounters {
    /// Entry arrived at `next_expected_idx` — no waiting, no buffer.
    pub entries_emitted_immediate: AtomicU64,
    /// Entry sat in the reorder buffer before being emitted in PoH order.
    pub entries_emitted_reordered: AtomicU64,
    /// Gave up waiting after `entry_timeout` — emitted Missing marker.
    pub entries_missing_timeout: AtomicU64,
    pub slots_complete: AtomicU64,
    pub slots_incomplete: AtomicU64,
    pub slots_with_missing: AtomicU64,
    /// Sum / max of buffer wait times (us) — for histogram.
    pub reorder_wait_sum_us: AtomicU64,
    pub reorder_wait_max_us: AtomicU64,
    pub pending_peak_size: AtomicU64,
    /// Pending buffer hit `max_pending_per_slot` — entry was dropped.
    pub pending_drops_capacity: AtomicU64,
    /// `idx < next_expected_idx` — already past, dropped silently.
    pub duplicates_dropped: AtomicU64,
    /// Output channel full when emitting an event.
    pub output_full: AtomicU64,
    /// Observation for a slot that was already sealed — dropped.
    pub late_arrivals_after_seal: AtomicU64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct PohSupervisorCountersSnapshot {
    pub entries_emitted_immediate: u64,
    pub entries_emitted_reordered: u64,
    pub entries_missing_timeout: u64,
    pub slots_complete: u64,
    pub slots_incomplete: u64,
    pub slots_with_missing: u64,
    pub reorder_wait_sum_us: u64,
    pub reorder_wait_max_us: u64,
    pub pending_peak_size: u64,
    pub pending_drops_capacity: u64,
    pub duplicates_dropped: u64,
    pub output_full: u64,
    pub late_arrivals_after_seal: u64,
}

impl PohSupervisorCounters {
    pub fn snapshot(&self) -> PohSupervisorCountersSnapshot {
        let l = |c: &AtomicU64| c.load(Ordering::Relaxed);
        PohSupervisorCountersSnapshot {
            entries_emitted_immediate: l(&self.entries_emitted_immediate),
            entries_emitted_reordered: l(&self.entries_emitted_reordered),
            entries_missing_timeout: l(&self.entries_missing_timeout),
            slots_complete: l(&self.slots_complete),
            slots_incomplete: l(&self.slots_incomplete),
            slots_with_missing: l(&self.slots_with_missing),
            reorder_wait_sum_us: l(&self.reorder_wait_sum_us),
            reorder_wait_max_us: l(&self.reorder_wait_max_us),
            pending_peak_size: l(&self.pending_peak_size),
            pending_drops_capacity: l(&self.pending_drops_capacity),
            duplicates_dropped: l(&self.duplicates_dropped),
            output_full: l(&self.output_full),
            late_arrivals_after_seal: l(&self.late_arrivals_after_seal),
        }
    }
}

#[derive(Default)]
struct PohSlot {
    next_expected_idx: u32,
    pending: BTreeMap<u32, PendingEntry>,
    cumulative_hashes: u64,
    hashes_since_last_tick: u64,
    tick_idx: u8,
    last_entry_hash: Option<Hash>,
    last_entry_index: u32,
    has_missing: bool,
    missing_count: u32,
    total_emitted: u32,
    last_emit_at: Option<Instant>,
    sealed: bool,
}

struct PendingEntry {
    merged: MergedEntry,
    buffered_at: Instant,
}

pub struct PohSupervisorConfig {
    pub merged_rx: Receiver<MergedEntry>,
    pub out_tx: Sender<OrderedEvent>,
    /// How long to wait for a missing `next_expected_idx` before emitting
    /// `Missing` and skipping forward.
    pub entry_timeout: Duration,
    /// Seal a slot once we have observed an entry whose slot is this many
    /// ahead of it (e.g. 5 ⇒ slot S is sealed when slot S+5 arrives).
    pub slot_seal_lag_slots: u64,
    /// Hard cap on pending size per slot, to bound memory under pathological
    /// reordering.
    pub max_pending_per_slot: usize,
    /// Sweep interval for the timeout / seal machinery. 10 ms gives ms-level
    /// reaction to gaps without burning CPU.
    pub tick_check_interval: Duration,
    pub counters: Arc<PohSupervisorCounters>,
    pub stop: Arc<AtomicBool>,
}

pub fn spawn(cfg: PohSupervisorConfig) -> std::io::Result<JoinHandle<()>> {
    std::thread::Builder::new()
        .name("poh-supervisor".into())
        .spawn(move || run_loop(cfg))
}

fn run_loop(cfg: PohSupervisorConfig) {
    let mut slots: BTreeMap<u64, PohSlot> = BTreeMap::new();
    let mut max_slot: u64 = 0;
    let mut last_sweep = Instant::now();

    loop {
        if cfg.stop.load(Ordering::Relaxed) {
            break;
        }
        crossbeam_channel::select! {
            recv(cfg.merged_rx) -> msg => match msg {
                Ok(merged) => {
                    let s = merged.observation.slot;
                    if s > max_slot { max_slot = s; }
                    process(&mut slots, merged, &cfg);
                }
                Err(_) => break,
            },
            default(cfg.tick_check_interval) => {}
        }
        let now = Instant::now();
        if now.duration_since(last_sweep) >= cfg.tick_check_interval {
            last_sweep = now;
            sweep(&mut slots, max_slot, now, &cfg);
        }
    }

    // Final flush: sweep + seal anything left.
    let now = Instant::now();
    sweep(&mut slots, max_slot, now, &cfg);
    let remaining: Vec<u64> = slots.keys().copied().collect();
    for s in remaining {
        seal_slot(&mut slots, s, now, &cfg);
    }
}

#[inline(always)]
fn process(slots: &mut BTreeMap<u64, PohSlot>, merged: MergedEntry, cfg: &PohSupervisorConfig) {
    let slot = merged.observation.slot;
    let idx = merged.observation.entry_index;
    let now = Instant::now();

    let slot_state = slots.entry(slot).or_default();
    if slot_state.sealed {
        cfg.counters
            .late_arrivals_after_seal
            .fetch_add(1, Ordering::Relaxed);
        return;
    }
    if slot_state.last_emit_at.is_none() {
        slot_state.last_emit_at = Some(now);
    }

    if idx == slot_state.next_expected_idx {
        emit_entry(slot_state, merged, now, false, 0, cfg);
        slot_state.next_expected_idx += 1;
        drain_consecutive_pending(slot_state, now, cfg);
    } else if idx > slot_state.next_expected_idx {
        if slot_state.pending.len() >= cfg.max_pending_per_slot {
            cfg.counters
                .pending_drops_capacity
                .fetch_add(1, Ordering::Relaxed);
            return;
        }
        slot_state.pending.insert(
            idx,
            PendingEntry {
                merged,
                buffered_at: now,
            },
        );
        let size = slot_state.pending.len() as u64;
        cfg.counters
            .pending_peak_size
            .fetch_max(size, Ordering::Relaxed);
    } else {
        cfg.counters
            .duplicates_dropped
            .fetch_add(1, Ordering::Relaxed);
    }
}

fn drain_consecutive_pending(
    slot_state: &mut PohSlot,
    now: Instant,
    cfg: &PohSupervisorConfig,
) {
    while let Some((&first_idx, _)) = slot_state.pending.first_key_value() {
        if first_idx != slot_state.next_expected_idx {
            break;
        }
        let entry = slot_state.pending.pop_first().unwrap().1;
        let wait_us = now
            .saturating_duration_since(entry.buffered_at)
            .as_micros()
            .min(u32::MAX as u128) as u32;
        emit_entry(slot_state, entry.merged, now, true, wait_us, cfg);
        slot_state.next_expected_idx += 1;
    }
}

fn emit_entry(
    slot_state: &mut PohSlot,
    merged: MergedEntry,
    now: Instant,
    was_reordered: bool,
    wait_us: u32,
    cfg: &PohSupervisorConfig,
) {
    let obs = merged.observation;

    // PoH tracking: only meaningful while no Missing has occurred.
    if !slot_state.has_missing {
        slot_state.cumulative_hashes =
            slot_state.cumulative_hashes.saturating_add(obs.num_hashes);
        slot_state.hashes_since_last_tick = slot_state
            .hashes_since_last_tick
            .saturating_add(obs.num_hashes);
        let is_tick =
            obs.tx_count == 0 && slot_state.hashes_since_last_tick == HASHES_PER_TICK;
        if is_tick && slot_state.tick_idx < TICKS_PER_SLOT {
            slot_state.tick_idx = slot_state.tick_idx.saturating_add(1);
            slot_state.hashes_since_last_tick = 0;
        }
    }

    slot_state.last_entry_hash = Some(obs.entry_hash);
    slot_state.last_entry_index = obs.entry_index;
    slot_state.last_emit_at = Some(now);
    slot_state.total_emitted = slot_state.total_emitted.saturating_add(1);

    let event = OrderedEvent::Entry(OrderedEntry {
        slot: obs.slot,
        entry_index: obs.entry_index,
        tick_idx_in_slot: slot_state.tick_idx,
        cumulative_hashes_in_slot: slot_state.cumulative_hashes,
        observation: obs,
        was_reordered,
        wait_duration_us: wait_us,
        tick_uncertain: slot_state.has_missing,
    });

    if was_reordered {
        cfg.counters
            .entries_emitted_reordered
            .fetch_add(1, Ordering::Relaxed);
        cfg.counters
            .reorder_wait_sum_us
            .fetch_add(wait_us as u64, Ordering::Relaxed);
        cfg.counters
            .reorder_wait_max_us
            .fetch_max(wait_us as u64, Ordering::Relaxed);
    } else {
        cfg.counters
            .entries_emitted_immediate
            .fetch_add(1, Ordering::Relaxed);
    }

    if cfg.out_tx.try_send(event).is_err() {
        cfg.counters.output_full.fetch_add(1, Ordering::Relaxed);
    }
}

fn emit_missing(
    slot_state: &mut PohSlot,
    slot: u64,
    now: Instant,
    cfg: &PohSupervisorConfig,
) {
    let waited_for_us = slot_state
        .pending
        .first_key_value()
        .map(|(_, p)| {
            now.saturating_duration_since(p.buffered_at)
                .as_micros()
                .min(u32::MAX as u128) as u32
        })
        .unwrap_or_else(|| cfg.entry_timeout.as_micros().min(u32::MAX as u128) as u32);

    let event = OrderedEvent::Missing(OrderedMissing {
        slot,
        entry_index: slot_state.next_expected_idx,
        waited_for_us,
    });

    slot_state.missing_count = slot_state.missing_count.saturating_add(1);
    slot_state.has_missing = true;
    slot_state.next_expected_idx += 1;

    cfg.counters
        .entries_missing_timeout
        .fetch_add(1, Ordering::Relaxed);

    if cfg.out_tx.try_send(event).is_err() {
        cfg.counters.output_full.fetch_add(1, Ordering::Relaxed);
    }
}

fn sweep(
    slots: &mut BTreeMap<u64, PohSlot>,
    max_slot: u64,
    now: Instant,
    cfg: &PohSupervisorConfig,
) {
    // Phase A: per-slot, advance past stale gaps (Missing + drain consecutives).
    let slot_keys: Vec<u64> = slots.keys().copied().collect();
    for slot_key in slot_keys {
        let slot_state = match slots.get_mut(&slot_key) {
            Some(s) if !s.sealed => s,
            _ => continue,
        };

        loop {
            // Drain any pending now matching next_expected_idx (gap closed).
            if let Some((&first_idx, _)) = slot_state.pending.first_key_value() {
                if first_idx == slot_state.next_expected_idx {
                    let entry = slot_state.pending.pop_first().unwrap().1;
                    let wait_us = now
                        .saturating_duration_since(entry.buffered_at)
                        .as_micros()
                        .min(u32::MAX as u128) as u32;
                    emit_entry(slot_state, entry.merged, now, true, wait_us, cfg);
                    slot_state.next_expected_idx += 1;
                    continue;
                }
                // Otherwise — check timeout on the oldest pending entry.
                let (_, p) = slot_state.pending.first_key_value().unwrap();
                if now.saturating_duration_since(p.buffered_at) >= cfg.entry_timeout {
                    emit_missing(slot_state, slot_key, now, cfg);
                    continue;
                }
            }
            break;
        }
    }

    // Phase B: seal slots that are far enough behind.
    if max_slot >= cfg.slot_seal_lag_slots {
        let cutoff = max_slot - cfg.slot_seal_lag_slots;
        let to_seal: Vec<u64> = slots
            .iter()
            .filter(|(s, st)| **s <= cutoff && !st.sealed)
            .map(|(s, _)| *s)
            .collect();
        for s in to_seal {
            seal_slot(slots, s, now, cfg);
        }
    }

    // Phase C: drop sealed slot state once it's well behind, to bound memory.
    let drop_cutoff = max_slot.saturating_sub(cfg.slot_seal_lag_slots + 10);
    slots.retain(|s, st| *s > drop_cutoff || !st.sealed);
}

fn seal_slot(
    slots: &mut BTreeMap<u64, PohSlot>,
    slot_key: u64,
    now: Instant,
    cfg: &PohSupervisorConfig,
) {
    let slot_state = match slots.get_mut(&slot_key) {
        Some(s) if !s.sealed => s,
        _ => return,
    };

    // Flush remaining pending in strict order: emit Missing for any gap
    // strictly before each pending index, then emit the pending entry.
    let max_pending_idx = slot_state.pending.last_key_value().map(|(k, _)| *k);
    if let Some(max_idx) = max_pending_idx {
        while slot_state.next_expected_idx <= max_idx {
            if let Some((&first_idx, _)) = slot_state.pending.first_key_value() {
                if first_idx == slot_state.next_expected_idx {
                    let entry = slot_state.pending.pop_first().unwrap().1;
                    let wait_us = now
                        .saturating_duration_since(entry.buffered_at)
                        .as_micros()
                        .min(u32::MAX as u128) as u32;
                    emit_entry(slot_state, entry.merged, now, true, wait_us, cfg);
                    slot_state.next_expected_idx += 1;
                } else {
                    emit_missing(slot_state, slot_key, now, cfg);
                }
            } else {
                break;
            }
        }
    }

    slot_state.sealed = true;
    if slot_state.has_missing {
        cfg.counters
            .slots_with_missing
            .fetch_add(1, Ordering::Relaxed);
    }
    let last_hash = slot_state.last_entry_hash.unwrap_or_default();

    let event = if slot_state.tick_idx >= TICKS_PER_SLOT {
        cfg.counters.slots_complete.fetch_add(1, Ordering::Relaxed);
        OrderedEvent::SlotComplete(OrderedSlotComplete {
            slot: slot_key,
            total_entries: slot_state.total_emitted,
            last_entry_hash: last_hash,
            last_tick_observed: slot_state.tick_idx,
            missing_count: slot_state.missing_count,
        })
    } else {
        cfg.counters.slots_incomplete.fetch_add(1, Ordering::Relaxed);
        OrderedEvent::SlotIncomplete(OrderedSlotIncomplete {
            slot: slot_key,
            last_seen_index: slot_state.last_entry_index,
            last_entry_hash: last_hash,
            last_tick_observed: slot_state.tick_idx,
            missing_count: slot_state.missing_count,
        })
    };
    if cfg.out_tx.try_send(event).is_err() {
        cfg.counters.output_full.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::merger::MergedEntry;
    use crossbeam_channel::{bounded, unbounded};
    use entry_sources::{SignatureVec, SourceKind};

    fn obs(slot: u64, idx: u32, num_hashes: u64, tx_count: u32) -> EntryObservation {
        EntryObservation {
            source: SourceKind::ShredStream,
            observed_at: Instant::now(),
            slot,
            entry_index: idx,
            num_hashes,
            entry_hash: Hash::new_unique(),
            tx_count,
            signatures: SignatureVec::new(),
            first_shred_at: None,
            leader: None,
        }
    }

    fn merged(slot: u64, idx: u32, num_hashes: u64, tx_count: u32) -> MergedEntry {
        MergedEntry {
            observation: obs(slot, idx, num_hashes, tx_count),
            first_seen_source: SourceKind::ShredStream,
        }
    }

    fn cfg_with(
        merged_rx: Receiver<MergedEntry>,
        out_tx: Sender<OrderedEvent>,
        entry_timeout: Duration,
    ) -> (PohSupervisorConfig, Arc<PohSupervisorCounters>, Arc<AtomicBool>) {
        let counters = Arc::new(PohSupervisorCounters::default());
        let stop = Arc::new(AtomicBool::new(false));
        let cfg = PohSupervisorConfig {
            merged_rx,
            out_tx,
            entry_timeout,
            slot_seal_lag_slots: 3,
            max_pending_per_slot: 1024,
            tick_check_interval: Duration::from_millis(5),
            counters: counters.clone(),
            stop: stop.clone(),
        };
        (cfg, counters, stop)
    }

    fn collect_events(rx: &Receiver<OrderedEvent>, expect_at_least: usize, deadline: Duration) -> Vec<OrderedEvent> {
        let deadline = Instant::now() + deadline;
        let mut out = Vec::new();
        while out.len() < expect_at_least && Instant::now() < deadline {
            match rx.recv_timeout(Duration::from_millis(20)) {
                Ok(e) => out.push(e),
                Err(_) => std::thread::sleep(Duration::from_millis(5)),
            }
        }
        // drain anything else immediately available
        while let Ok(e) = rx.try_recv() {
            out.push(e);
        }
        out
    }

    #[test]
    fn in_order_entries_emit_immediately() {
        let (in_tx, in_rx) = unbounded();
        let (out_tx, out_rx) = bounded(64);
        let (cfg, counters, stop) = cfg_with(in_rx, out_tx, Duration::from_millis(50));
        let h = spawn(cfg).unwrap();

        for i in 0..3 {
            in_tx.send(merged(100, i, HASHES_PER_TICK, 0)).unwrap();
        }
        std::thread::sleep(Duration::from_millis(40));
        stop.store(true, Ordering::Relaxed);
        drop(in_tx);
        let _ = h.join();

        let events = collect_events(&out_rx, 3, Duration::from_millis(50));
        let entries: Vec<_> = events
            .iter()
            .filter_map(|e| match e {
                OrderedEvent::Entry(e) => Some(e),
                _ => None,
            })
            .collect();
        assert_eq!(entries.len(), 3);
        for (i, e) in entries.iter().enumerate() {
            assert_eq!(e.entry_index, i as u32);
            assert!(!e.was_reordered);
            assert_eq!(e.wait_duration_us, 0);
        }
        let snap = counters.snapshot();
        assert_eq!(snap.entries_emitted_immediate, 3);
        assert_eq!(snap.entries_emitted_reordered, 0);
    }

    #[test]
    fn out_of_order_entry_buffered_then_drained() {
        let (in_tx, in_rx) = unbounded();
        let (out_tx, out_rx) = bounded(64);
        let (cfg, counters, stop) = cfg_with(in_rx, out_tx, Duration::from_millis(500));
        let h = spawn(cfg).unwrap();

        // idx 1 arrives first → buffered. Then idx 0 closes the gap.
        in_tx.send(merged(100, 1, 1000, 1)).unwrap();
        std::thread::sleep(Duration::from_millis(10));
        in_tx.send(merged(100, 0, 1000, 1)).unwrap();
        std::thread::sleep(Duration::from_millis(40));
        stop.store(true, Ordering::Relaxed);
        drop(in_tx);
        let _ = h.join();

        let events = collect_events(&out_rx, 2, Duration::from_millis(50));
        let entries: Vec<_> = events
            .iter()
            .filter_map(|e| match e {
                OrderedEvent::Entry(e) => Some(e),
                _ => None,
            })
            .collect();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].entry_index, 0);
        assert_eq!(entries[1].entry_index, 1);
        assert!(entries[1].was_reordered);
        let snap = counters.snapshot();
        assert_eq!(snap.entries_emitted_reordered, 1);
    }

    #[test]
    fn entry_timeout_emits_missing_then_unblocks() {
        let (in_tx, in_rx) = unbounded();
        let (out_tx, out_rx) = bounded(64);
        let (cfg, counters, stop) = cfg_with(in_rx, out_tx, Duration::from_millis(20));
        let h = spawn(cfg).unwrap();

        // idx 1 sits in pending. idx 0 never comes.
        in_tx.send(merged(100, 1, 1000, 1)).unwrap();
        std::thread::sleep(Duration::from_millis(60));
        stop.store(true, Ordering::Relaxed);
        drop(in_tx);
        let _ = h.join();

        let events = collect_events(&out_rx, 2, Duration::from_millis(50));
        let kinds: Vec<&str> = events
            .iter()
            .map(|e| match e {
                OrderedEvent::Entry(_) => "entry",
                OrderedEvent::Missing(_) => "missing",
                OrderedEvent::SlotComplete(_) => "complete",
                OrderedEvent::SlotIncomplete(_) => "incomplete",
            })
            .collect();
        // Expected: Missing(0), Entry(1), SlotIncomplete(100) on shutdown flush.
        assert_eq!(kinds[0], "missing");
        assert_eq!(kinds[1], "entry");
        if let OrderedEvent::Entry(e) = &events[1] {
            assert_eq!(e.entry_index, 1);
            assert!(e.tick_uncertain, "tick_uncertain set once Missing emitted");
        }
        let snap = counters.snapshot();
        assert_eq!(snap.entries_missing_timeout, 1);
    }

    #[test]
    fn slot_complete_when_tick_64_reached() {
        let (in_tx, in_rx) = unbounded();
        let (out_tx, out_rx) = bounded(2048);
        let (cfg, counters, stop) = cfg_with(in_rx, out_tx, Duration::from_millis(500));
        let h = spawn(cfg).unwrap();

        // Emit 64 ticks: each is (tx_count=0, num_hashes=HASHES_PER_TICK).
        for i in 0..64u32 {
            in_tx
                .send(merged(100, i, HASHES_PER_TICK, 0))
                .unwrap();
        }
        // Push a later slot so slot 100 seals.
        in_tx.send(merged(105, 0, 1, 0)).unwrap();
        std::thread::sleep(Duration::from_millis(80));
        stop.store(true, Ordering::Relaxed);
        drop(in_tx);
        let _ = h.join();

        let events = collect_events(&out_rx, 65, Duration::from_millis(50));
        let complete = events.iter().find_map(|e| match e {
            OrderedEvent::SlotComplete(c) if c.slot == 100 => Some(c),
            _ => None,
        });
        assert!(complete.is_some(), "expected SlotComplete for slot 100");
        let c = complete.unwrap();
        assert_eq!(c.last_tick_observed, TICKS_PER_SLOT);
        assert_eq!(c.total_entries, 64);
        let snap = counters.snapshot();
        assert!(snap.slots_complete >= 1);
    }

    #[test]
    fn slot_incomplete_when_no_final_tick() {
        let (in_tx, in_rx) = unbounded();
        let (out_tx, out_rx) = bounded(64);
        let (cfg, counters, stop) = cfg_with(in_rx, out_tx, Duration::from_millis(500));
        let h = spawn(cfg).unwrap();

        // Send a few data entries — never reach tick 64.
        for i in 0..3u32 {
            in_tx.send(merged(100, i, 1000, 5)).unwrap();
        }
        // Bump max_slot past seal_lag.
        in_tx.send(merged(105, 0, 1, 0)).unwrap();
        std::thread::sleep(Duration::from_millis(60));
        stop.store(true, Ordering::Relaxed);
        drop(in_tx);
        let _ = h.join();

        let events = collect_events(&out_rx, 4, Duration::from_millis(50));
        let incomplete = events.iter().find_map(|e| match e {
            OrderedEvent::SlotIncomplete(c) if c.slot == 100 => Some(c),
            _ => None,
        });
        assert!(incomplete.is_some());
        let i = incomplete.unwrap();
        assert!(i.last_tick_observed < TICKS_PER_SLOT);
        assert_ne!(i.last_entry_hash, Hash::default());
        let snap = counters.snapshot();
        assert!(snap.slots_incomplete >= 1);
    }

    #[test]
    fn duplicate_below_next_expected_dropped() {
        let (in_tx, in_rx) = unbounded();
        let (out_tx, out_rx) = bounded(64);
        let (cfg, counters, stop) = cfg_with(in_rx, out_tx, Duration::from_millis(500));
        let h = spawn(cfg).unwrap();

        in_tx.send(merged(100, 0, 1000, 1)).unwrap();
        std::thread::sleep(Duration::from_millis(5));
        in_tx.send(merged(100, 0, 1000, 1)).unwrap(); // duplicate
        std::thread::sleep(Duration::from_millis(20));
        stop.store(true, Ordering::Relaxed);
        drop(in_tx);
        let _ = h.join();

        let events = collect_events(&out_rx, 1, Duration::from_millis(50));
        let entries = events
            .iter()
            .filter(|e| matches!(e, OrderedEvent::Entry(_)))
            .count();
        assert_eq!(entries, 1);
        let snap = counters.snapshot();
        assert!(snap.duplicates_dropped >= 1);
    }
}
