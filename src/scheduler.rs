//! Deterministyczny scheduler: mapuje `block_index` → `(slot_ms, sample_idx)`.
//!
//! Plan sekwencyjny: bloki 0..samples są przypisane slot[0],
//! samples..2*samples → slot[1], itd.

use thiserror::Error;

#[derive(Debug, Clone, Copy)]
pub struct SchedulerConfig {
    pub start_ms: u64,
    pub end_ms: u64,
    pub step_ms: u64,
    pub samples_per_wallet_per_slot: u64,
}

#[derive(Debug, Error)]
pub enum SchedulerError {
    #[error("start_ms ({start}) must be <= end_ms ({end})")]
    InvalidRange { start: u64, end: u64 },
    #[error("step_ms must be > 0")]
    ZeroStep,
    #[error("samples_per_wallet_per_slot must be > 0")]
    ZeroSamples,
}

#[derive(Debug, Clone)]
pub struct Schedule {
    cfg: SchedulerConfig,
    slots_count: u64,
}

impl Schedule {
    pub fn new(cfg: SchedulerConfig) -> Result<Self, SchedulerError> {
        if cfg.start_ms > cfg.end_ms {
            return Err(SchedulerError::InvalidRange {
                start: cfg.start_ms,
                end: cfg.end_ms,
            });
        }
        if cfg.step_ms == 0 {
            return Err(SchedulerError::ZeroStep);
        }
        if cfg.samples_per_wallet_per_slot == 0 {
            return Err(SchedulerError::ZeroSamples);
        }
        let slots_count = (cfg.end_ms - cfg.start_ms) / cfg.step_ms + 1;
        Ok(Self { cfg, slots_count })
    }

    pub fn slots_count(&self) -> u64 {
        self.slots_count
    }

    pub fn total_blocks(&self) -> u64 {
        self.slots_count * self.cfg.samples_per_wallet_per_slot
    }

    pub fn slot_ms_for(&self, block_index: u64) -> Option<u64> {
        if block_index >= self.total_blocks() {
            return None;
        }
        let slot_idx = block_index / self.cfg.samples_per_wallet_per_slot;
        Some(self.cfg.start_ms + slot_idx * self.cfg.step_ms)
    }

    pub fn sample_idx_for(&self, block_index: u64) -> Option<u64> {
        if block_index >= self.total_blocks() {
            return None;
        }
        Some(block_index % self.cfg.samples_per_wallet_per_slot)
    }

    pub fn config(&self) -> &SchedulerConfig {
        &self.cfg
    }
}
