//! Deterministic schedule generator.
//!
//! Picks one `(slot, tick)` per slot to fire on. Seeded by the run config
//! so a run is reproducible. Emits in chunks ahead of the live slot so the
//! engine has the schedule loaded by the time the slot arrives.

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ScheduleEntry {
    pub slot: u64,
    pub tick: u8,
}

pub struct Schedule {
    rng: SmallRng,
    next_slot: u64,
    chunk_size: u64,
    triggers_per_slot: u32,
}

impl Schedule {
    pub fn new(seed: Option<u64>, start_slot: u64, chunk_size: u64, triggers_per_slot: u32) -> Self {
        let seed = seed.unwrap_or_else(|| {
            // Use system time for non-deterministic seed when not specified.
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0xDEADBEEF)
        });
        Self {
            rng: SmallRng::seed_from_u64(seed),
            next_slot: start_slot,
            chunk_size: chunk_size.max(1),
            triggers_per_slot: triggers_per_slot.max(1),
        }
    }

    /// Generate the next chunk of schedule entries.
    pub fn next_chunk(&mut self) -> Vec<ScheduleEntry> {
        let mut out = Vec::with_capacity((self.chunk_size * self.triggers_per_slot as u64) as usize);
        for offset in 0..self.chunk_size {
            let slot = self.next_slot + offset;
            // Pick `triggers_per_slot` distinct ticks per slot. Tick range 1..=64.
            let mut picked: Vec<u8> = Vec::with_capacity(self.triggers_per_slot as usize);
            while picked.len() < self.triggers_per_slot as usize && picked.len() < 64 {
                let tick: u8 = self.rng.random_range(1u8..=64u8);
                if !picked.contains(&tick) {
                    picked.push(tick);
                }
            }
            for tick in picked {
                out.push(ScheduleEntry { slot, tick });
            }
        }
        self.next_slot = self.next_slot.saturating_add(self.chunk_size);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_with_seed() {
        let mut a = Schedule::new(Some(42), 1000, 5, 1);
        let mut b = Schedule::new(Some(42), 1000, 5, 1);
        assert_eq!(a.next_chunk(), b.next_chunk());
    }

    #[test]
    fn different_seed_produces_different_schedule() {
        let mut a = Schedule::new(Some(1), 1000, 100, 1);
        let mut b = Schedule::new(Some(2), 1000, 100, 1);
        assert_ne!(a.next_chunk(), b.next_chunk());
    }

    #[test]
    fn chunk_size_respected() {
        let mut s = Schedule::new(Some(7), 100, 3, 1);
        let chunk = s.next_chunk();
        assert_eq!(chunk.len(), 3);
        assert_eq!(chunk[0].slot, 100);
        assert_eq!(chunk[2].slot, 102);
    }

    #[test]
    fn next_chunk_advances_slot_cursor() {
        let mut s = Schedule::new(Some(1), 100, 5, 1);
        let c1 = s.next_chunk();
        let c2 = s.next_chunk();
        assert_eq!(c1.last().unwrap().slot, 104);
        assert_eq!(c2.first().unwrap().slot, 105);
    }

    #[test]
    fn tick_in_valid_range() {
        let mut s = Schedule::new(Some(1), 0, 1000, 1);
        for entry in s.next_chunk() {
            assert!((1..=64).contains(&entry.tick));
        }
    }

    #[test]
    fn multiple_triggers_per_slot_unique() {
        let mut s = Schedule::new(Some(1), 0, 10, 3);
        let chunk = s.next_chunk();
        // Group by slot — each should have 3 distinct ticks.
        for slot in 0..10 {
            let ticks: Vec<u8> = chunk
                .iter()
                .filter(|e| e.slot == slot)
                .map(|e| e.tick)
                .collect();
            assert_eq!(ticks.len(), 3);
            let mut sorted = ticks.clone();
            sorted.sort();
            sorted.dedup();
            assert_eq!(sorted.len(), 3, "ticks must be distinct");
        }
    }
}
