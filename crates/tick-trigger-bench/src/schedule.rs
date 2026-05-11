use rand::{Rng, SeedableRng};
use rand::rngs::SmallRng;
use serde::{Deserialize, Serialize};
use std::path::Path;

pub const TICKS_PER_SLOT: u8 = 64;
pub const TX_PER_SLOT: usize = 3;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ScheduleEntry {
    pub slot: u64,
    pub tick: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schedule {
    pub seed: u64,
    pub start_slot: u64,
    pub num_slots: u64,
    pub entries: Vec<ScheduleEntry>,
}

impl Schedule {
    pub fn generate(start_slot: u64, num_slots: u64, seed: u64) -> Self {
        let mut rng = SmallRng::seed_from_u64(seed);
        let mut entries = Vec::with_capacity((num_slots * TX_PER_SLOT as u64) as usize);
        for offset in 0..num_slots {
            let slot = start_slot + offset;
            let mut ticks: Vec<u8> = Vec::with_capacity(TX_PER_SLOT);
            while ticks.len() < TX_PER_SLOT {
                let t = rng.gen_range(1..=TICKS_PER_SLOT);
                if !ticks.contains(&t) {
                    ticks.push(t);
                }
            }
            ticks.sort();
            for tick in ticks {
                entries.push(ScheduleEntry { slot, tick });
            }
        }
        Self { seed, start_slot, num_slots, entries }
    }

    pub fn save(&self, path: &Path) -> anyhow::Result<()> {
        let json = serde_json::to_string(self)?;
        std::fs::write(path, json)?;
        Ok(())
    }

    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let json = std::fs::read_to_string(path)?;
        Ok(serde_json::from_str(&json)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generates_three_unique_ticks_per_slot() {
        let s = Schedule::generate(1000, 10, 42);
        assert_eq!(s.entries.len(), 30);
        for slot_offset in 0..10u64 {
            let slot = 1000 + slot_offset;
            let mut ticks_for_slot: Vec<u8> = s.entries.iter()
                .filter(|e| e.slot == slot)
                .map(|e| e.tick)
                .collect();
            assert_eq!(ticks_for_slot.len(), 3);
            ticks_for_slot.sort();
            ticks_for_slot.dedup();
            assert_eq!(ticks_for_slot.len(), 3, "duplicate ticks in slot {slot}");
            for t in ticks_for_slot {
                assert!(t >= 1 && t <= 64);
            }
        }
    }

    #[test]
    fn ticks_are_sorted_within_slot() {
        let s = Schedule::generate(1000, 5, 1);
        for window in s.entries.windows(2) {
            if window[0].slot == window[1].slot {
                assert!(window[0].tick < window[1].tick);
            }
        }
    }

    #[test]
    fn same_seed_produces_same_schedule() {
        let a = Schedule::generate(1000, 100, 12345);
        let b = Schedule::generate(1000, 100, 12345);
        assert_eq!(a.entries, b.entries);
    }

    #[test]
    fn different_seed_produces_different_schedule() {
        let a = Schedule::generate(1000, 100, 12345);
        let b = Schedule::generate(1000, 100, 54321);
        assert_ne!(a.entries, b.entries);
    }

    #[test]
    fn save_load_roundtrip() {
        let s = Schedule::generate(1000, 10, 42);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("schedule.json");
        s.save(&path).unwrap();
        let loaded = Schedule::load(&path).unwrap();
        assert_eq!(s.entries, loaded.entries);
        assert_eq!(s.seed, loaded.seed);
    }
}
