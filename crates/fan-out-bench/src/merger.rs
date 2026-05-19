//! EntryMerger — merges SS + YS entry streams with dedup by (slot, entry_hash).
//!
//! Emits each unique entry ONCE (first-seen). The slower source's later
//! observation is dropped at this stage; Plan 4 matcher will track per-source
//! signature timestamps separately for parquet.
//!
//! Rolling-window eviction: drop dedup keys with slot < current_slot - WINDOW.

use crate::counters::BenchCounters;
use crossbeam_channel::{Receiver, Sender};
use entry_sources::{EntryObservation, SourceKind};
use solana_sdk::hash::Hash;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;

const DEDUP_WINDOW_SLOTS: u64 = 50;

#[derive(Debug, Clone)]
pub struct MergedEntry {
    pub observation: EntryObservation,
    pub first_seen_source: SourceKind,
}

pub struct MergerConfig {
    pub ss_rx: Receiver<EntryObservation>,
    pub ys_rx: Receiver<EntryObservation>,
    pub out_tx: Sender<MergedEntry>,
    pub pinned_core: Option<usize>,
    pub counters: Arc<BenchCounters>,
    pub stop: Arc<AtomicBool>,
}

pub fn spawn(cfg: MergerConfig) -> std::io::Result<JoinHandle<()>> {
    std::thread::Builder::new()
        .name("entry-merger".into())
        .spawn(move || {
            if let Some(core) = cfg.pinned_core {
                core_affinity::set_for_current(core_affinity::CoreId { id: core });
            }
            run_loop(cfg);
        })
}

fn run_loop(cfg: MergerConfig) {
    let mut seen: HashSet<(u64, Hash)> = HashSet::with_capacity(8192);
    let mut max_slot: u64 = 0;

    loop {
        if cfg.stop.load(Ordering::Relaxed) {
            break;
        }
        let (obs, source) = crossbeam_channel::select! {
            recv(cfg.ss_rx) -> msg => match msg {
                Ok(o) => (o, SourceKind::ShredStream),
                Err(_) => {
                    tracing::warn!("ss channel disconnected");
                    break;
                }
            },
            recv(cfg.ys_rx) -> msg => match msg {
                Ok(o) => (o, SourceKind::Yellowstone),
                Err(_) => {
                    tracing::warn!("ys channel disconnected");
                    break;
                }
            },
        };

        let key = (obs.slot, obs.entry_hash);
        if !seen.insert(key) {
            continue;
        }

        if obs.slot > max_slot {
            max_slot = obs.slot;
            if max_slot > DEDUP_WINDOW_SLOTS {
                let cutoff = max_slot - DEDUP_WINDOW_SLOTS;
                seen.retain(|(s, _)| *s >= cutoff);
            }
        }

        let merged = MergedEntry {
            observation: obs,
            first_seen_source: source,
        };
        if cfg.out_tx.try_send(merged).is_err() {
            cfg.counters
                .send_event_queue_full
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_channel::{bounded, unbounded};
    use entry_sources::{EntryObservation, SignatureVec};
    use std::time::Instant;

    fn make_obs(slot: u64, entry_hash: Hash, source: SourceKind) -> EntryObservation {
        EntryObservation {
            source,
            observed_at: Instant::now(),
            slot,
            entry_index: 0,
            num_hashes: 62_500,
            entry_hash,
            tx_count: 0,
            signatures: SignatureVec::new(),
            first_shred_at: None,
            leader: None,
        }
    }

    #[test]
    fn dedups_same_slot_entry_hash() {
        let (ss_tx, ss_rx) = unbounded();
        let (ys_tx, ys_rx) = unbounded();
        let (out_tx, out_rx) = bounded(100);
        let stop = Arc::new(AtomicBool::new(false));
        let counters = Arc::new(BenchCounters::default());

        let h = Hash::new_unique();
        ss_tx.send(make_obs(100, h, SourceKind::ShredStream)).unwrap();
        ys_tx.send(make_obs(100, h, SourceKind::Yellowstone)).unwrap();

        let handle = spawn(MergerConfig {
            ss_rx, ys_rx, out_tx,
            pinned_core: None,
            counters: counters.clone(),
            stop: stop.clone(),
        }).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(50));
        stop.store(true, Ordering::Relaxed);
        drop(ss_tx);
        drop(ys_tx);
        let _ = handle.join();

        let mut merged = Vec::new();
        while let Ok(m) = out_rx.try_recv() {
            merged.push(m);
        }
        assert_eq!(merged.len(), 1, "expected exactly 1 merged entry, got {}", merged.len());
    }

    #[test]
    fn different_entry_hashes_both_emitted() {
        let (ss_tx, ss_rx) = unbounded();
        let (_ys_tx, ys_rx) = unbounded::<EntryObservation>();
        let (out_tx, out_rx) = bounded(100);
        let stop = Arc::new(AtomicBool::new(false));
        let counters = Arc::new(BenchCounters::default());

        ss_tx.send(make_obs(100, Hash::new_unique(), SourceKind::ShredStream)).unwrap();
        ss_tx.send(make_obs(100, Hash::new_unique(), SourceKind::ShredStream)).unwrap();
        ss_tx.send(make_obs(100, Hash::new_unique(), SourceKind::ShredStream)).unwrap();

        let handle = spawn(MergerConfig {
            ss_rx, ys_rx, out_tx,
            pinned_core: None,
            counters: counters.clone(),
            stop: stop.clone(),
        }).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(50));
        stop.store(true, Ordering::Relaxed);
        drop(ss_tx);
        let _ = handle.join();

        let mut merged = Vec::new();
        while let Ok(m) = out_rx.try_recv() {
            merged.push(m);
        }
        assert_eq!(merged.len(), 3);
    }

    #[test]
    fn first_seen_source_is_recorded() {
        let (ss_tx, ss_rx) = unbounded();
        let (_ys_tx, ys_rx) = unbounded::<EntryObservation>();
        let (out_tx, out_rx) = bounded(10);
        let stop = Arc::new(AtomicBool::new(false));
        let counters = Arc::new(BenchCounters::default());

        ss_tx.send(make_obs(100, Hash::new_unique(), SourceKind::ShredStream)).unwrap();
        let handle = spawn(MergerConfig {
            ss_rx, ys_rx, out_tx,
            pinned_core: None,
            counters: counters.clone(),
            stop: stop.clone(),
        }).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(30));
        stop.store(true, Ordering::Relaxed);
        drop(ss_tx);
        let _ = handle.join();

        let merged = out_rx.try_recv().unwrap();
        assert_eq!(merged.first_seen_source, SourceKind::ShredStream);
    }
}
