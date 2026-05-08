use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Identifier (Ed25519 public key, base58) of a Solana validator.
pub type ValidatorIdentity = String;

/// Geographic + infrastructural metadata for a single validator.
/// Sourced from validators.app.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ValidatorInfo {
    pub identity: ValidatorIdentity,
    pub name: Option<String>,
    pub vote_account: Option<String>,
    pub active_stake_lamports: u64,
    pub country_code: Option<String>,
    pub data_center_key: Option<String>,
    pub asn: Option<u64>,
    pub asn_organization: Option<String>,
    pub ip: Option<String>,
}

/// Solana epoch metadata captured at fetch time.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EpochInfo {
    pub epoch: u64,
    pub absolute_slot: u64,
    pub slot_index: u64,
    pub slots_in_epoch: u64,
}

impl EpochInfo {
    pub fn epoch_first_slot(&self) -> u64 {
        self.absolute_slot - self.slot_index
    }
    pub fn epoch_last_slot(&self) -> u64 {
        self.epoch_first_slot() + self.slots_in_epoch - 1
    }
}

/// Leader schedule for one epoch: validator identity -> slot indices (relative to epoch start).
pub type LeaderSchedule = BTreeMap<ValidatorIdentity, Vec<u64>>;

/// Combined snapshot — validators + epoch + leader schedule. Cacheable as JSON.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpochSnapshot {
    pub fetched_at: DateTime<Utc>,
    pub epoch: EpochInfo,
    pub validators: Vec<ValidatorInfo>,
    pub schedule: LeaderSchedule,
}

/// Slot -> who leads it + their geo. Output of `aggregate::build_slot_map`.
#[derive(Debug, Clone, Serialize)]
pub struct SlotEntry {
    pub absolute_slot: u64,
    pub identity: ValidatorIdentity,
    pub validator_name: Option<String>,
    pub country_code: Option<String>,
    pub data_center_key: Option<String>,
    pub stake_lamports: u64,
}

/// Aggregated counts/percentages per country for a single epoch.
#[derive(Debug, Clone, Serialize)]
pub struct CountrySummary {
    pub country_code: String,
    pub slot_count: u64,
    pub slot_percentage: f64,
    pub stake_lamports: u128,
    pub stake_percentage: f64,
    pub validator_count: u64,
}

/// Top-level epoch summary returned by `aggregate::summarize`.
#[derive(Debug, Clone, Serialize)]
pub struct EpochSummary {
    pub epoch: u64,
    pub total_slots: u64,
    pub mapped_slots: u64,
    pub unknown_slots: u64,
    pub countries: Vec<CountrySummary>,
    pub total_stake_lamports: u128,
}
