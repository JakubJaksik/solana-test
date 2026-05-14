//! Deterministic chunked schedule generator.
//!
//! Per slot: 1 random tick (1..=64). Seed deterministic; chunks
//! generated lazily — supports open-ended runs without OOM.

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

pub const TICKS_PER_SLOT: u8 = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleEntry {
    pub slot: u64,
    pub tick: u8, // 1..=64
}

#[derive(Debug, Clone)]
pub struct Schedule {
    pub seed: u64,
    pub start_slot: u64,
    pub chunk_size_slots: u64,
    pub current_chunk_index: u64,
}

impl Schedule {
    pub fn new(seed: Option<u64>, start_slot: u64, chunk_size_slots: u64) -> Self {
        let seed = seed.unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0xDEAD_BEEF_DEAD_BEEFu64)
        });
        Self {
            seed,
            start_slot,
            chunk_size_slots,
            current_chunk_index: 0,
        }
    }

    pub fn generate_chunk(&mut self) -> Vec<ScheduleEntry> {
        let chunk_index = self.current_chunk_index;
        self.current_chunk_index += 1;
        self.generate_chunk_at(chunk_index)
    }

    pub fn generate_chunk_at(&self, chunk_index: u64) -> Vec<ScheduleEntry> {
        let chunk_seed = self.seed.wrapping_add(chunk_index.wrapping_mul(0x9E37_79B9_7F4A_7C15));
        let mut rng = SmallRng::seed_from_u64(chunk_seed);
        let chunk_start = self.start_slot + chunk_index * self.chunk_size_slots;
        (0..self.chunk_size_slots)
            .map(|i| ScheduleEntry {
                slot: chunk_start + i,
                tick: rng.gen_range(1..=TICKS_PER_SLOT),
            })
            .collect()
    }

    pub fn chunk_start_slot(&self, chunk_index: u64) -> u64 {
        self.start_slot + chunk_index * self.chunk_size_slots
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_same_seed_produces_same_chunks() {
        let a = Schedule::new(Some(42), 1000, 100).generate_chunk_at(0);
        let b = Schedule::new(Some(42), 1000, 100).generate_chunk_at(0);
        assert_eq!(a, b);
    }

    #[test]
    fn different_seeds_produce_different_chunks() {
        let a = Schedule::new(Some(42), 1000, 100).generate_chunk_at(0);
        let b = Schedule::new(Some(43), 1000, 100).generate_chunk_at(0);
        assert_ne!(a, b);
    }

    #[test]
    fn chunk_has_correct_size() {
        let chunk = Schedule::new(Some(42), 1000, 100).generate_chunk_at(0);
        assert_eq!(chunk.len(), 100);
    }

    #[test]
    fn chunk_slots_are_contiguous() {
        let chunk = Schedule::new(Some(42), 1000, 100).generate_chunk_at(0);
        for (i, entry) in chunk.iter().enumerate() {
            assert_eq!(entry.slot, 1000 + i as u64);
        }
    }

    #[test]
    fn ticks_within_valid_range() {
        let chunk = Schedule::new(Some(42), 1000, 1000).generate_chunk_at(0);
        for entry in &chunk {
            assert!(entry.tick >= 1 && entry.tick <= 64, "tick out of range: {}", entry.tick);
        }
    }

    #[test]
    fn sequential_chunks_have_disjoint_slot_ranges() {
        let mut sched = Schedule::new(Some(42), 1000, 100);
        let chunk0 = sched.generate_chunk();
        let chunk1 = sched.generate_chunk();
        assert_eq!(chunk0.last().unwrap().slot + 1, chunk1.first().unwrap().slot);
    }

    #[test]
    fn chunk_index_calculation() {
        let sched = Schedule::new(Some(42), 1000, 100);
        assert_eq!(sched.chunk_start_slot(0), 1000);
        assert_eq!(sched.chunk_start_slot(1), 1100);
        assert_eq!(sched.chunk_start_slot(10), 2000);
    }

    #[test]
    fn schedule_with_none_seed_uses_time_based() {
        let s = Schedule::new(None, 0, 10);
        assert_ne!(s.seed, 0);
    }
}
