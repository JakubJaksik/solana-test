use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender};
use solana_entry::entry::Entry;
use solana_ledger::shred::Shredder;

use crate::counters::DropCounters;
use crate::observation::{EntryObservation, SignatureVec, SourceKind};

use super::fec_tracker::{FecSetReady, FecTracker};
use super::RawShredPacket;

pub struct DeshredWorkerConfig {
    pub raw_rx: Receiver<RawShredPacket>,
    pub obs_tx: Sender<EntryObservation>,
    pub pinned_core: Option<usize>,
    pub counters: Arc<DropCounters>,
}

pub fn spawn(cfg: DeshredWorkerConfig) -> std::io::Result<()> {
    std::thread::Builder::new()
        .name("ss-deshred".into())
        .spawn(move || {
            if let Some(core) = cfg.pinned_core {
                core_affinity::set_for_current(core_affinity::CoreId { id: core });
            }
            run_loop(cfg.raw_rx, cfg.obs_tx, cfg.counters);
        })?;
    Ok(())
}

fn run_loop(
    raw_rx: Receiver<RawShredPacket>,
    obs_tx: Sender<EntryObservation>,
    counters: Arc<DropCounters>,
) {
    let mut tracker = FecTracker::new(counters.clone());
    // (slot) → cumulative entry index emitted so far for that slot
    let mut slot_entry_offset: HashMap<u64, u32> = HashMap::with_capacity(512);
    let mut highest_slot: u64 = 0;
    let mut last_evict = Instant::now();

    while let Ok(pkt) = raw_rx.recv() {
        if let Some(ready) = tracker.ingest(pkt) {
            if ready.slot > highest_slot {
                highest_slot = ready.slot;
            }
            emit_entries(ready, &mut slot_entry_offset, &obs_tx, &counters);
        }
        let now = Instant::now();
        if now.duration_since(last_evict) > Duration::from_millis(500) {
            tracker.evict_older_than(now, Duration::from_secs(2));
            // Prune slot_entry_offset entries for slots far behind the frontier
            // to prevent unbounded growth over a long epoch run.
            if slot_entry_offset.len() > 2048 {
                let cutoff = highest_slot.saturating_sub(512);
                slot_entry_offset.retain(|s, _| *s >= cutoff);
            }
            last_evict = now;
        }
    }
}

fn emit_entries(
    ready: FecSetReady,
    slot_offset: &mut HashMap<u64, u32>,
    obs_tx: &Sender<EntryObservation>,
    counters: &DropCounters,
) {
    // Shredder::deshred takes an iterator of items implementing AsRef<[u8]>.
    // Shred::payload() returns &Payload which implements AsRef<[u8]>.
    let bytes = match Shredder::deshred(ready.data_shreds.iter().map(|s| s.payload())) {
        Ok(b) => b,
        Err(_) => {
            counters.inc(&counters.ss_deshred_error);
            return;
        }
    };

    // The on-wire format uses wincode (not plain bincode) because Entry has
    // #[wincode(with = ...)] annotations on its fields. wincode is
    // bincode-layout-compatible for simple types but diverges for the
    // transactions vec. Use wincode::deserialize as the blockstore does.
    let entries: Vec<Entry> = match wincode::deserialize(&bytes) {
        Ok(e) => e,
        Err(_) => {
            counters.inc(&counters.ss_entry_decode_error);
            return;
        }
    };

    // Cumulative entry index for this slot. FEC sets in a slot arrive in order.
    // If we missed the first FEC set of a slot we lose alignment for that slot —
    // the post-process is robust to this (such slots appear as YS_ONLY in the report).
    let base_index = *slot_offset.entry(ready.slot).or_insert(0);

    for (i, entry) in entries.iter().enumerate() {
        let entry_index = base_index + i as u32;

        // First signature of each transaction is the canonical one.
        let mut sigs = SignatureVec::with_capacity(entry.transactions.len().min(8));
        for tx in &entry.transactions {
            if let Some(sig) = tx.signatures.first() {
                sigs.push(*sig);
            }
        }

        let obs = EntryObservation {
            source: SourceKind::ShredStream,
            observed_at: ready.completed_at,
            slot: ready.slot,
            entry_index,
            num_hashes: entry.num_hashes,
            entry_hash: entry.hash,
            tx_count: entry.transactions.len() as u32,
            signatures: sigs,
            first_shred_at: Some(ready.first_shred_at),
            leader: None,
        };
        if obs_tx.try_send(obs).is_err() {
            counters.inc(&counters.ss_obs_channel_full);
        }
    }
    *slot_offset.get_mut(&ready.slot).unwrap() = base_index + entries.len() as u32;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_loop_exits_when_input_closes() {
        // Smoke: drop the sender immediately, run_loop should see disconnect and return.
        let (raw_tx, raw_rx) = crossbeam_channel::bounded::<RawShredPacket>(1);
        let (obs_tx, _obs_rx) = crossbeam_channel::bounded::<EntryObservation>(1);
        let counters = Arc::new(DropCounters::default());
        // Drop the raw_tx so recv returns Err immediately.
        drop(raw_tx);
        // Run inline (don't spawn thread) — should return promptly.
        run_loop(raw_rx, obs_tx, counters);
    }
}
