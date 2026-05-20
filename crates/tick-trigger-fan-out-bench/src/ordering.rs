//! Per-slot ordering tracker.
//!
//! Consumes the merger's primary emissions and answers: when slot S is sealed
//! (we've moved past it), did the entries arrive in increasing `entry_index`
//! order? If not, how out-of-order was it?
//!
//! Why this matters for downstream phases: PoH tick accounting, durable nonce
//! `last_entry_hash`, and any "did this sig appear by tick K?" question all
//! depend on ordering. Phase 1 quantifies the actual disorder rate so we can
//! pick the right downstream strategy (strict ordering vs index-aware vs
//! fallback to RPC).
//!
//! What we count, per slot:
//! - `entries_seen`: distinct primary entries observed in this slot
//! - `max_index`: highest entry_index seen
//! - `out_of_order_count`: number of entries whose index < max_index_at_arrival
//! - `max_backward_gap`: largest (max_index_at_arrival - this_index) observed
//! - `index_gaps_below_max`: indices that should have been observed but weren't
//!   by the time the slot was sealed (e.g. saw indices 0,1,3,4 → gap at 2)
//! - `is_tick_last`: whether the highest-index entry was a tick (it should be)
//! - `last_entry_tx_count`: tx_count of the highest-index entry (≥0; tick=0)
//!
//! A slot is "sealed" as soon as we observe an entry with slot > S (we assume
//! monotonic slot progression at chain-tip; reorgs are rare and not handled
//! here — they'd show up as "completed" slots that get more entries later,
//! which we just count as out_of_order against the now-frozen max_index).

use entry_sources::EntryObservation;
use parking_lot::RwLock;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct SlotOrderingReport {
    pub slot: u64,
    pub entries_seen: u32,
    pub max_index: u32,
    pub out_of_order_count: u32,
    pub max_backward_gap: u32,
    pub missing_indices: Vec<u32>,
    pub last_entry_was_tick: bool,
    pub last_entry_tx_count: u32,
}

#[derive(Default, Debug)]
pub struct OrderingCounters {
    pub slots_sealed: AtomicU64,
    pub slots_fully_ordered: AtomicU64,
    pub slots_with_disorder: AtomicU64,
    pub slots_with_gaps: AtomicU64,
    pub slots_ending_on_tick: AtomicU64,
    pub total_out_of_order: AtomicU64,
    pub total_missing_indices: AtomicU64,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct OrderingCountersSnapshot {
    pub slots_sealed: u64,
    pub slots_fully_ordered: u64,
    pub slots_with_disorder: u64,
    pub slots_with_gaps: u64,
    pub slots_ending_on_tick: u64,
    pub total_out_of_order: u64,
    pub total_missing_indices: u64,
}

impl OrderingCounters {
    pub fn snapshot(&self) -> OrderingCountersSnapshot {
        let l = |c: &AtomicU64| c.load(AtomicOrdering::Relaxed);
        OrderingCountersSnapshot {
            slots_sealed: l(&self.slots_sealed),
            slots_fully_ordered: l(&self.slots_fully_ordered),
            slots_with_disorder: l(&self.slots_with_disorder),
            slots_with_gaps: l(&self.slots_with_gaps),
            slots_ending_on_tick: l(&self.slots_ending_on_tick),
            total_out_of_order: l(&self.total_out_of_order),
            total_missing_indices: l(&self.total_missing_indices),
        }
    }
}

#[derive(Debug, Default)]
struct SlotProgress {
    seen_indices: BTreeSet<u32>,
    max_index_at_arrival: u32,
    out_of_order: u32,
    max_backward_gap: u32,
    last_entry: Option<(u32, u32)>, // (max_index_so_far, tx_count_of_that_entry)
}

pub struct OrderingTracker {
    counters: Arc<OrderingCounters>,
    in_progress: RwLock<BTreeMap<u64, SlotProgress>>,
    sealed_reports: RwLock<Vec<SlotOrderingReport>>,
    /// Maximum slot seen across all observations — used to decide when a slot
    /// is "behind" enough to seal.
    high_water: RwLock<u64>,
    /// How many slots behind `high_water` before we seal an in-progress slot.
    /// 5 means: when we see slot S+5, slot S is sealed.
    seal_lag: u64,
}

impl OrderingTracker {
    pub fn new(counters: Arc<OrderingCounters>, seal_lag: u64) -> Self {
        Self {
            counters,
            in_progress: RwLock::new(BTreeMap::new()),
            sealed_reports: RwLock::new(Vec::new()),
            high_water: RwLock::new(0),
            seal_lag,
        }
    }

    /// Observe a primary entry. Returns any newly-sealed slot reports.
    pub fn observe(&self, obs: &EntryObservation) -> Vec<SlotOrderingReport> {
        let slot = obs.slot;
        let idx = obs.entry_index;

        {
            let mut hw = self.high_water.write();
            if slot > *hw {
                *hw = slot;
            }
        }

        {
            let mut ip = self.in_progress.write();
            let prog = ip.entry(slot).or_default();
            // Detect out-of-order: this index arrived after we'd already seen a higher one.
            if idx < prog.max_index_at_arrival {
                prog.out_of_order += 1;
                let gap = prog.max_index_at_arrival - idx;
                if gap > prog.max_backward_gap {
                    prog.max_backward_gap = gap;
                }
            } else if idx > prog.max_index_at_arrival {
                prog.max_index_at_arrival = idx;
                prog.last_entry = Some((idx, obs.tx_count));
            }
            prog.seen_indices.insert(idx);
        }

        self.maybe_seal()
    }

    fn maybe_seal(&self) -> Vec<SlotOrderingReport> {
        let hw = *self.high_water.read();
        if hw <= self.seal_lag {
            return Vec::new();
        }
        let cutoff = hw - self.seal_lag;
        let mut to_seal = Vec::new();
        {
            let ip = self.in_progress.read();
            for &slot in ip.keys() {
                if slot <= cutoff {
                    to_seal.push(slot);
                }
            }
        }
        let mut reports = Vec::new();
        let mut ip = self.in_progress.write();
        for slot in to_seal {
            if let Some(prog) = ip.remove(&slot) {
                let report = build_report(slot, prog);
                self.update_counters(&report);
                reports.push(report.clone());
                self.sealed_reports.write().push(report);
            }
        }
        reports
    }

    /// Flush ALL in-progress slots as sealed (called at shutdown).
    pub fn flush_all(&self) -> Vec<SlotOrderingReport> {
        let mut reports = Vec::new();
        let mut ip = self.in_progress.write();
        let slots: Vec<u64> = ip.keys().copied().collect();
        for slot in slots {
            if let Some(prog) = ip.remove(&slot) {
                let report = build_report(slot, prog);
                self.update_counters(&report);
                reports.push(report.clone());
                self.sealed_reports.write().push(report);
            }
        }
        reports
    }

    fn update_counters(&self, report: &SlotOrderingReport) {
        self.counters
            .slots_sealed
            .fetch_add(1, AtomicOrdering::Relaxed);
        if report.out_of_order_count == 0 && report.missing_indices.is_empty() {
            self.counters
                .slots_fully_ordered
                .fetch_add(1, AtomicOrdering::Relaxed);
        }
        if report.out_of_order_count > 0 {
            self.counters
                .slots_with_disorder
                .fetch_add(1, AtomicOrdering::Relaxed);
        }
        if !report.missing_indices.is_empty() {
            self.counters
                .slots_with_gaps
                .fetch_add(1, AtomicOrdering::Relaxed);
        }
        if report.last_entry_was_tick {
            self.counters
                .slots_ending_on_tick
                .fetch_add(1, AtomicOrdering::Relaxed);
        }
        self.counters
            .total_out_of_order
            .fetch_add(report.out_of_order_count as u64, AtomicOrdering::Relaxed);
        self.counters.total_missing_indices.fetch_add(
            report.missing_indices.len() as u64,
            AtomicOrdering::Relaxed,
        );
    }

    pub fn sealed_reports(&self) -> Vec<SlotOrderingReport> {
        self.sealed_reports.read().clone()
    }
}

fn build_report(slot: u64, prog: SlotProgress) -> SlotOrderingReport {
    let (last_idx, last_tx_count) = prog.last_entry.unwrap_or((0, 0));
    // Compute missing: every integer in [0, max_index] absent from seen_indices.
    let mut missing = Vec::new();
    if !prog.seen_indices.is_empty() {
        for i in 0..=last_idx {
            if !prog.seen_indices.contains(&i) {
                missing.push(i);
            }
        }
    }
    SlotOrderingReport {
        slot,
        entries_seen: prog.seen_indices.len() as u32,
        max_index: last_idx,
        out_of_order_count: prog.out_of_order,
        max_backward_gap: prog.max_backward_gap,
        missing_indices: missing,
        last_entry_was_tick: last_tx_count == 0,
        last_entry_tx_count: last_tx_count,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use entry_sources::{SignatureVec, SourceKind};
    use solana_sdk::hash::Hash;
    use std::time::Instant;

    fn obs(slot: u64, idx: u32, tx_count: u32) -> EntryObservation {
        EntryObservation {
            source: SourceKind::ShredStream,
            observed_at: Instant::now(),
            slot,
            entry_index: idx,
            num_hashes: 62_500,
            entry_hash: Hash::new_unique(),
            tx_count,
            signatures: SignatureVec::new(),
            first_shred_at: None,
            leader: None,
        }
    }

    fn tracker() -> OrderingTracker {
        OrderingTracker::new(Arc::new(OrderingCounters::default()), 3)
    }

    #[test]
    fn in_order_slot_no_disorder() {
        let t = tracker();
        for i in 0..10 {
            t.observe(&obs(100, i, if i == 9 { 0 } else { 1 }));
        }
        // Push high water past seal_lag — slot 100 seals during these observations.
        t.observe(&obs(104, 0, 1));
        t.observe(&obs(105, 0, 1));
        let r100 = t
            .sealed_reports()
            .into_iter()
            .find(|r| r.slot == 100)
            .expect("slot 100 must have been sealed");
        assert_eq!(r100.out_of_order_count, 0);
        assert_eq!(r100.entries_seen, 10);
        assert_eq!(r100.max_index, 9);
        assert!(r100.last_entry_was_tick);
        assert!(r100.missing_indices.is_empty());
    }

    #[test]
    fn detects_out_of_order_arrival() {
        let t = tracker();
        t.observe(&obs(100, 0, 1));
        t.observe(&obs(100, 1, 1));
        t.observe(&obs(100, 5, 1)); // jump
        t.observe(&obs(100, 3, 1)); // backwards
        t.observe(&obs(100, 4, 1)); // backwards (5 already seen)
        t.observe(&obs(100, 6, 0)); // tick last
        t.observe(&obs(104, 0, 1));
        t.observe(&obs(105, 0, 1));
        let r = t
            .sealed_reports()
            .into_iter()
            .find(|r| r.slot == 100)
            .expect("slot 100 must have been sealed");
        assert_eq!(r.out_of_order_count, 2, "two entries arrived after a higher index");
        assert_eq!(r.max_backward_gap, 2); // max was 5 when we saw 3 → gap 2
        assert_eq!(r.missing_indices, vec![2]);
        // Distinct indices observed: 0, 1, 3, 4, 5, 6 = 6.
        assert_eq!(r.entries_seen, 6);
        assert_eq!(r.max_index, 6);
        assert!(r.last_entry_was_tick);
    }

    #[test]
    fn flush_all_seals_remaining() {
        let t = tracker();
        t.observe(&obs(100, 0, 1));
        t.observe(&obs(100, 1, 0));
        // Do NOT push high water — should not auto-seal.
        assert_eq!(t.counters.snapshot().slots_sealed, 0);
        let reports = t.flush_all();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].slot, 100);
        assert_eq!(t.counters.snapshot().slots_sealed, 1);
    }

    #[test]
    fn counter_aggregation_across_slots() {
        let t = tracker();
        // Slot 100: clean.
        for i in 0..3 {
            t.observe(&obs(100, i, if i == 2 { 0 } else { 1 }));
        }
        // Slot 101: out-of-order.
        t.observe(&obs(101, 0, 1));
        t.observe(&obs(101, 2, 1));
        t.observe(&obs(101, 1, 1));
        t.observe(&obs(101, 3, 0));
        // Push past seal_lag to seal both.
        t.observe(&obs(110, 0, 1));
        t.flush_all();
        let snap = t.counters.snapshot();
        assert_eq!(snap.slots_sealed, 3);
        assert_eq!(snap.slots_fully_ordered, 2); // slot 100 + slot 110
        assert_eq!(snap.slots_with_disorder, 1);
        assert!(snap.total_out_of_order >= 1);
    }
}
