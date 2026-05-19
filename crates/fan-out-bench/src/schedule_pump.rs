//! Schedule pump — generates chunks of ScheduleEntry lazily and pushes
//! them onto schedule_tx as observer's current_slot catches up.

use crate::counters::BenchCounters;
use crate::schedule::{Schedule, ScheduleEntry};
use crossbeam_channel::Sender;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

pub struct PumpConfig {
    pub schedule: Schedule,
    pub schedule_tx: Sender<ScheduleEntry>,
    pub current_slot: Arc<AtomicU64>,
    pub lead_slots: u64,
    pub pinned_core: Option<usize>,
    pub counters: Arc<BenchCounters>,
    pub stop: Arc<AtomicBool>,
}

pub fn spawn(cfg: PumpConfig) -> std::io::Result<JoinHandle<()>> {
    std::thread::Builder::new()
        .name("schedule-pump".into())
        .spawn(move || {
            if let Some(core) = cfg.pinned_core {
                core_affinity::set_for_current(core_affinity::CoreId { id: core });
            }
            run_loop(cfg);
        })
}

fn run_loop(mut cfg: PumpConfig) {
    let mut buffered: Vec<ScheduleEntry> = Vec::new();
    loop {
        if cfg.stop.load(Ordering::Relaxed) {
            break;
        }
        let current = cfg.current_slot.load(Ordering::Relaxed);

        if buffered.is_empty() {
            buffered = cfg.schedule.generate_chunk();
            tracing::info!(
                chunk_index = cfg.schedule.current_chunk_index,
                size = buffered.len(),
                "schedule-pump generated chunk"
            );
        }

        let lead_cutoff = current + cfg.lead_slots;
        while let Some(entry) = buffered.first() {
            if entry.slot > lead_cutoff && current > 0 {
                break;
            }
            let entry = buffered.remove(0);
            if cfg.schedule_tx.send(entry).is_err() {
                tracing::warn!("schedule_tx closed, schedule-pump exiting");
                return;
            }
        }

        std::thread::sleep(Duration::from_millis(50));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_channel::bounded;

    #[test]
    fn pump_emits_entries_when_current_slot_advances() {
        let schedule = Schedule::new(Some(42), 100, 5);
        let (tx, rx) = bounded::<ScheduleEntry>(100);
        let current_slot = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));

        let handle = spawn(PumpConfig {
            schedule,
            schedule_tx: tx.clone(),
            current_slot: current_slot.clone(),
            lead_slots: 1000,
            pinned_core: None,
            counters: Arc::new(BenchCounters::default()),
            stop: stop.clone(),
        }).unwrap();

        current_slot.store(50, Ordering::Relaxed);
        std::thread::sleep(Duration::from_millis(150));

        let mut count = 0;
        while rx.try_recv().is_ok() {
            count += 1;
        }
        assert!(count >= 5, "expected at least 5 entries pumped, got {}", count);

        stop.store(true, Ordering::Relaxed);
        drop(tx);
        let _ = handle.join();
    }
}
