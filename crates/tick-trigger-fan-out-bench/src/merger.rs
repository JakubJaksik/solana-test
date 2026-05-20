//! Two-source entry merger — single unified output stream.
//!
//! Receives `EntryObservation` from ShredStream and Yellowstone, dedups by
//! `(slot, entry_hash)`, and emits each unique entry **exactly once**, using
//! whichever source saw it first. Downstream consumers see a single, unified
//! stream — they do not know (or care) whether SS or YS won the race for any
//! particular entry. The race winner is tracked in metrics so we can answer
//! "who is faster" without polluting the data path.
//!
//! Output protocol:
//! - First time `(slot, entry_hash)` arrives: emit `MergedEntry { observation,
//!   first_seen_source }`. `observation` carries the WINNER's data.
//! - Second time (the other source confirms): silently absorbed; we record
//!   the inter-source latency delta in `confirm_latency_*` counters and bump
//!   the "confirmed by both" count.
//! - Same-source repeat or any third arrival: dropped, counted as `duplicates`.
//!
//! Eviction: dedup keys for `(slot, entry_hash)` are dropped when the slot is
//! more than `DEDUP_WINDOW_SLOTS` behind the highest slot seen.

use ahash::RandomState as AHasher;
use crossbeam_channel::{Receiver, Sender};
use entry_sources::{EntryObservation, SourceKind};
use solana_sdk::hash::Hash;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Instant;

/// Dedup HashMap with ahash instead of SipHash. 2-3× faster on our key shape,
/// and the merger runs single-threaded so we don't need DoS-resistant hashing.
type DedupMap = HashMap<(u64, Hash), DedupState, AHasher>;

/// How many slots of dedup history to retain. Mainnet ~2.5 slots/sec, so 200
/// slots covers ~80s of history — comfortably longer than any plausible
/// inter-source delay.
const DEDUP_WINDOW_SLOTS: u64 = 200;

#[derive(Debug, Clone)]
pub struct MergedEntry {
    /// First-arriving observation, carrying the winning source's data
    /// (`observation.source` and `observation.observed_at`).
    pub observation: EntryObservation,
    /// Which source observed this entry first. Equals `observation.source`;
    /// duplicated for callers that pass `observation` deeper without the
    /// source field.
    pub first_seen_source: SourceKind,
}

#[derive(Default, Debug)]
pub struct MergerCounters {
    pub ss_received: AtomicU64,
    pub ys_received: AtomicU64,
    pub ss_first: AtomicU64,
    pub ys_first: AtomicU64,
    /// Number of unique entries that BOTH sources eventually reported. The
    /// merger absorbed the second sighting silently; downstream never saw it.
    pub confirmed_by_both: AtomicU64,
    /// Sum of (second_seen_at - first_seen_at) in nanoseconds, across all
    /// `confirmed_by_both` events. Average = sum / confirmed_by_both.
    pub confirm_latency_sum_ns: AtomicU64,
    /// Minimum / maximum observed second-source latency in nanoseconds. min
    /// starts at u64::MAX (sentinel); a snapshot with `confirmed_by_both == 0`
    /// should ignore min/max.
    pub confirm_latency_min_ns: AtomicU64,
    pub confirm_latency_max_ns: AtomicU64,
    /// Same-source repeat or 3rd-arrival — silently dropped.
    pub duplicates: AtomicU64,
    /// Output channel full → MergedEntry dropped.
    pub output_full: AtomicU64,
}

#[derive(Debug, serde::Serialize, Default, Clone)]
pub struct MergerCountersSnapshot {
    pub ss_received: u64,
    pub ys_received: u64,
    pub ss_first: u64,
    pub ys_first: u64,
    pub confirmed_by_both: u64,
    pub confirm_latency_sum_ns: u64,
    pub confirm_latency_min_ns: u64,
    pub confirm_latency_max_ns: u64,
    pub duplicates: u64,
    pub output_full: u64,
}

impl MergerCountersSnapshot {
    /// Mean inter-source latency in microseconds, computed only over entries
    /// confirmed by both sources. Returns `None` when no confirmations yet.
    pub fn confirm_latency_avg_us(&self) -> Option<f64> {
        if self.confirmed_by_both == 0 {
            return None;
        }
        Some(self.confirm_latency_sum_ns as f64 / self.confirmed_by_both as f64 / 1_000.0)
    }
}

impl MergerCounters {
    pub fn new() -> Self {
        Self {
            confirm_latency_min_ns: AtomicU64::new(u64::MAX),
            ..Default::default()
        }
    }

    pub fn snapshot(&self) -> MergerCountersSnapshot {
        let l = |c: &AtomicU64| c.load(Ordering::Relaxed);
        let confirmed = l(&self.confirmed_by_both);
        MergerCountersSnapshot {
            ss_received: l(&self.ss_received),
            ys_received: l(&self.ys_received),
            ss_first: l(&self.ss_first),
            ys_first: l(&self.ys_first),
            confirmed_by_both: confirmed,
            confirm_latency_sum_ns: l(&self.confirm_latency_sum_ns),
            confirm_latency_min_ns: if confirmed == 0 { 0 } else { l(&self.confirm_latency_min_ns) },
            confirm_latency_max_ns: l(&self.confirm_latency_max_ns),
            duplicates: l(&self.duplicates),
            output_full: l(&self.output_full),
        }
    }

    fn record_confirm_latency(&self, delta_ns: u64) {
        self.confirmed_by_both.fetch_add(1, Ordering::Relaxed);
        self.confirm_latency_sum_ns
            .fetch_add(delta_ns, Ordering::Relaxed);
        // Min via fetch_min (atomic min as of Rust 1.45+).
        self.confirm_latency_min_ns
            .fetch_min(delta_ns, Ordering::Relaxed);
        self.confirm_latency_max_ns
            .fetch_max(delta_ns, Ordering::Relaxed);
    }
}

pub struct MergerConfig {
    pub ss_rx: Receiver<EntryObservation>,
    pub ys_rx: Receiver<EntryObservation>,
    pub out_tx: Sender<MergedEntry>,
    pub counters: Arc<MergerCounters>,
    pub stop: Arc<AtomicBool>,
}

pub fn spawn(cfg: MergerConfig) -> std::io::Result<JoinHandle<()>> {
    std::thread::Builder::new()
        .name("entry-merger".into())
        .spawn(move || run_loop(cfg))
}

fn run_loop(cfg: MergerConfig) {
    // Dedup state. AHash + entry API avoid double-hash; capacity sized for
    // ~200 slots * ~250 entries = 50k under sustained mainnet load.
    let mut state: DedupMap = HashMap::with_capacity_and_hasher(65_536, AHasher::default());
    // Per-slot key index: lets eviction touch only the keys belonging to
    // expired slots (O(slot_keys)) instead of scanning the whole map
    // (O(total) — observable as a ~100µs spike on each slot boundary).
    let mut slot_index: BTreeMap<u64, Vec<Hash>> = BTreeMap::new();
    let mut max_slot: u64 = 0;
    loop {
        if cfg.stop.load(Ordering::Relaxed) {
            break;
        }
        let (obs, source) = crossbeam_channel::select! {
            recv(cfg.ss_rx) -> msg => match msg {
                Ok(o) => {
                    cfg.counters.ss_received.fetch_add(1, Ordering::Relaxed);
                    (o, SourceKind::ShredStream)
                }
                Err(_) => { tracing::warn!("ss channel closed"); break; }
            },
            recv(cfg.ys_rx) -> msg => match msg {
                Ok(o) => {
                    cfg.counters.ys_received.fetch_add(1, Ordering::Relaxed);
                    (o, SourceKind::Yellowstone)
                }
                Err(_) => { tracing::warn!("ys channel closed"); break; }
            },
        };

        process(&mut state, &mut slot_index, &mut max_slot, obs, source, &cfg);
    }
}

#[derive(Clone, Copy)]
enum DedupState {
    Primary {
        first_source: SourceKind,
        first_seen_at: Instant,
    },
    Confirmed,
}

#[inline(always)]
fn process(
    state: &mut DedupMap,
    slot_index: &mut BTreeMap<u64, Vec<Hash>>,
    max_slot: &mut u64,
    obs: EntryObservation,
    source: SourceKind,
    cfg: &MergerConfig,
) {
    let slot = obs.slot;
    let hash = obs.entry_hash;
    let arrival = obs.observed_at;
    let key = (slot, hash);
    // Entry API → single hash computation instead of get+insert.
    use std::collections::hash_map::Entry;
    match state.entry(key) {
        Entry::Vacant(slot_entry) => {
            slot_entry.insert(DedupState::Primary {
                first_source: source,
                first_seen_at: arrival,
            });
            // Track key under its slot for O(slot_size) eviction later.
            slot_index.entry(slot).or_default().push(hash);
            match source {
                SourceKind::ShredStream => {
                    cfg.counters.ss_first.fetch_add(1, Ordering::Relaxed);
                }
                SourceKind::Yellowstone => {
                    cfg.counters.ys_first.fetch_add(1, Ordering::Relaxed);
                }
            }
            let merged = MergedEntry {
                observation: obs,
                first_seen_source: source,
            };
            if cfg.out_tx.try_send(merged).is_err() {
                cfg.counters.output_full.fetch_add(1, Ordering::Relaxed);
            }
        }
        Entry::Occupied(mut existing) => match *existing.get() {
            DedupState::Primary {
                first_source,
                first_seen_at,
            } if first_source != source => {
                *existing.get_mut() = DedupState::Confirmed;
                let delta_ns = arrival
                    .saturating_duration_since(first_seen_at)
                    .as_nanos()
                    .min(u64::MAX as u128) as u64;
                cfg.counters.record_confirm_latency(delta_ns);
            }
            _ => {
                cfg.counters.duplicates.fetch_add(1, Ordering::Relaxed);
            }
        },
    }

    if slot > *max_slot {
        *max_slot = slot;
        if *max_slot > DEDUP_WINDOW_SLOTS {
            let cutoff = *max_slot - DEDUP_WINDOW_SLOTS;
            // Drain all slots strictly older than cutoff. Each removed slot
            // touches only its own keys — O(slot_size), not O(total).
            // BTreeMap iterates in order, so we stop once we pass cutoff.
            let mut expired: Vec<u64> = Vec::new();
            for &s in slot_index.keys() {
                if s < cutoff {
                    expired.push(s);
                } else {
                    break;
                }
            }
            for s in expired {
                if let Some(hashes) = slot_index.remove(&s) {
                    for h in hashes {
                        state.remove(&(s, h));
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_channel::{bounded, unbounded};
    use entry_sources::SignatureVec;
    use std::time::{Duration, Instant};

    fn obs(slot: u64, idx: u32, hash: Hash, source: SourceKind, tx_count: u32) -> EntryObservation {
        EntryObservation {
            source,
            observed_at: Instant::now(),
            slot,
            entry_index: idx,
            num_hashes: 62_500,
            entry_hash: hash,
            tx_count,
            signatures: SignatureVec::new(),
            first_shred_at: None,
            leader: None,
        }
    }

    fn drain(rx: &Receiver<MergedEntry>) -> Vec<MergedEntry> {
        let mut out = Vec::new();
        while let Ok(m) = rx.try_recv() {
            out.push(m);
        }
        out
    }

    fn run_with(inputs: Vec<(EntryObservation, SourceKind)>) -> (Vec<MergedEntry>, MergerCountersSnapshot) {
        let (ss_tx, ss_rx) = unbounded();
        let (ys_tx, ys_rx) = unbounded();
        let (out_tx, out_rx) = bounded(1024);
        let counters = Arc::new(MergerCounters::new());
        let stop = Arc::new(AtomicBool::new(false));
        let handle = spawn(MergerConfig {
            ss_rx,
            ys_rx,
            out_tx,
            counters: counters.clone(),
            stop: stop.clone(),
        }).unwrap();

        for (o, src) in inputs {
            match src {
                SourceKind::ShredStream => ss_tx.send(o).unwrap(),
                SourceKind::Yellowstone => ys_tx.send(o).unwrap(),
            }
        }
        std::thread::sleep(Duration::from_millis(80));
        stop.store(true, Ordering::Relaxed);
        drop(ss_tx);
        drop(ys_tx);
        let _ = handle.join();
        (drain(&out_rx), counters.snapshot())
    }

    #[test]
    fn single_source_each_entry_emitted_once() {
        let h1 = Hash::new_unique();
        let h2 = Hash::new_unique();
        let (out, c) = run_with(vec![
            (obs(100, 0, h1, SourceKind::ShredStream, 0), SourceKind::ShredStream),
            (obs(100, 1, h2, SourceKind::ShredStream, 0), SourceKind::ShredStream),
        ]);
        assert_eq!(out.len(), 2);
        assert_eq!(c.ss_first, 2);
        assert_eq!(c.confirmed_by_both, 0);
    }

    #[test]
    fn second_source_absorbed_silently_with_latency() {
        // Both sources see the same entry — merger emits ONCE for the winner
        // and silently records the inter-source latency for the loser.
        let h = Hash::new_unique();
        let (out, c) = run_with(vec![
            (obs(100, 0, h, SourceKind::ShredStream, 0), SourceKind::ShredStream),
            (obs(100, 0, h, SourceKind::Yellowstone, 0), SourceKind::Yellowstone),
        ]);
        assert_eq!(out.len(), 1, "exactly one MergedEntry per unique (slot,hash)");
        assert_eq!(c.confirmed_by_both, 1);
        assert_eq!(c.duplicates, 0);
        assert_eq!(c.ss_first + c.ys_first, 1);
        // Latency was measured; min/max equal (single sample), avg defined.
        assert_eq!(c.confirm_latency_min_ns, c.confirm_latency_max_ns);
        assert!(c.confirm_latency_avg_us().is_some());
    }

    #[test]
    fn same_source_repeat_is_dropped_as_duplicate() {
        let h = Hash::new_unique();
        let (out, c) = run_with(vec![
            (obs(100, 0, h, SourceKind::ShredStream, 0), SourceKind::ShredStream),
            (obs(100, 0, h, SourceKind::ShredStream, 0), SourceKind::ShredStream),
        ]);
        assert_eq!(out.len(), 1);
        assert_eq!(c.duplicates, 1);
        assert_eq!(c.confirmed_by_both, 0);
    }

    #[test]
    fn third_arrival_dropped_after_both_sources_confirmed() {
        let h = Hash::new_unique();
        let (out, c) = run_with(vec![
            (obs(100, 0, h, SourceKind::ShredStream, 0), SourceKind::ShredStream),
            (obs(100, 0, h, SourceKind::Yellowstone, 0), SourceKind::Yellowstone),
            (obs(100, 0, h, SourceKind::ShredStream, 0), SourceKind::ShredStream),
        ]);
        assert_eq!(out.len(), 1, "still only one emit");
        assert_eq!(c.confirmed_by_both, 1);
        assert_eq!(c.duplicates, 1);
    }

    #[test]
    fn ys_only_emits_once_with_ys_first() {
        let h = Hash::new_unique();
        let (out, c) = run_with(vec![
            (obs(100, 0, h, SourceKind::Yellowstone, 0), SourceKind::Yellowstone),
        ]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].first_seen_source, SourceKind::Yellowstone);
        assert_eq!(c.ys_first, 1);
        assert_eq!(c.ss_first, 0);
    }

    #[test]
    fn snapshot_avg_us_none_when_no_confirmations() {
        let c = MergerCounters::new().snapshot();
        assert!(c.confirm_latency_avg_us().is_none());
    }

    #[test]
    fn different_entry_hashes_treated_independently() {
        let h1 = Hash::new_unique();
        let h2 = Hash::new_unique();
        let (out, _) = run_with(vec![
            (obs(100, 0, h1, SourceKind::ShredStream, 0), SourceKind::ShredStream),
            (obs(100, 1, h2, SourceKind::ShredStream, 5), SourceKind::ShredStream),
        ]);
        assert_eq!(out.len(), 2);
    }
}
