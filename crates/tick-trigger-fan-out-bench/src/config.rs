//! Phase 3+ configuration.
//!
//! Single JSON file describes everything: RPC, wallet, gRPC sources,
//! supervisor tuning, schedule generation, tx parameters, senders, run
//! parameters. Designed to grow — adding fan-out vendors, durable nonce,
//! Jito tip strategies etc. should only need new fields or new sender
//! kinds, not structural changes.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub rpc: RpcConfig,
    pub wallet: WalletConfig,
    pub sources: SourcesConfig,
    pub supervisor: SupervisorConfig,
    pub schedule: ScheduleConfig,
    pub tx: TxConfig,
    pub senders: Vec<SenderConfig>,
    pub run: RunConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RpcConfig {
    pub url: String,
    #[serde(default = "default_blockhash_refresh_secs")]
    pub blockhash_refresh_secs: u64,
}

fn default_blockhash_refresh_secs() -> u64 {
    5
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WalletConfig {
    pub keypair_path: PathBuf,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SourcesConfig {
    pub shredstream_grpc_url: String,
    pub yellowstone_grpc_url: String,
    #[serde(default)]
    pub yellowstone_auth_token: String,
    #[serde(default = "default_source_channel_capacity")]
    pub channel_capacity: usize,
}

fn default_source_channel_capacity() -> usize {
    65536
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SupervisorConfig {
    #[serde(default = "default_entry_timeout_ms")]
    pub entry_timeout_ms: u64,
    #[serde(default = "default_slot_seal_lag_slots")]
    pub slot_seal_lag_slots: u64,
}

fn default_entry_timeout_ms() -> u64 {
    150
}
fn default_slot_seal_lag_slots() -> u64 {
    10
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ScheduleConfig {
    /// `None` → seeded from system time (different every run).
    /// `Some(N)` → reproducible.
    #[serde(default)]
    pub seed: Option<u64>,
    /// How many slots per pump chunk.
    #[serde(default = "default_chunk_size_slots")]
    pub chunk_size_slots: u64,
    /// Pump emits chunks `lead_slots` ahead of `current_slot` so the
    /// engine has the schedule ready when the slot arrives.
    #[serde(default = "default_lead_slots")]
    pub lead_slots: u64,
}

fn default_chunk_size_slots() -> u64 {
    30
}
fn default_lead_slots() -> u64 {
    100
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TxConfig {
    /// Self-transfer amount (back to wallet). 1 lamport keeps it minimum
    /// while still being a real tx with system_program::transfer ix.
    #[serde(default = "default_self_transfer_lamports")]
    pub self_transfer_lamports: u64,
    /// Priority fee microlamports per CU (set via ComputeBudgetInstruction).
    #[serde(default = "default_priority_fee")]
    pub priority_fee_microlamports: u64,
    /// Compute unit limit (set via ComputeBudgetInstruction).
    #[serde(default = "default_compute_unit_limit")]
    pub compute_unit_limit: u32,
}

fn default_self_transfer_lamports() -> u64 {
    1
}
fn default_priority_fee() -> u64 {
    5000
}
fn default_compute_unit_limit() -> u32 {
    200_000
}

/// Sender kind discriminator. New protocols/vendors get a new variant.
/// HTTP-based for now; QUIC/gRPC/custom clients will follow in later phases.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SenderKind {
    Helius,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SenderConfig {
    pub id: u8,
    pub name: String,
    pub kind: SenderKind,
    pub endpoint_url: String,
    #[serde(default)]
    pub tip_lamports: u64,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

fn default_enabled() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RunConfig {
    /// Top-level dir under which each run gets its own subdirectory.
    pub output_dir: PathBuf,
    /// Stop run if wallet balance drops below this (avoids draining).
    #[serde(default = "default_min_balance_lamports")]
    pub min_balance_lamports: u64,
    /// How long a trigger waits for its tx to be observed before being
    /// emitted as UNKNOWN_PENDING. 90 s comfortably covers mainnet finality.
    #[serde(default = "default_observation_deadline_secs")]
    pub observation_deadline_secs: u64,
    /// Total run duration as a humantime string (e.g. "5m", "60s").
    /// Parse with `humantime::parse_duration(&run.duration)`.
    pub duration: String,
    /// Send rate hint per slot: 1 trigger per slot is the default.
    /// Each scheduled (slot, tick) fires exactly once.
    #[serde(default = "default_triggers_per_slot")]
    pub triggers_per_slot: u32,
}

fn default_min_balance_lamports() -> u64 {
    1_500_000
}
fn default_observation_deadline_secs() -> u64 {
    90
}
fn default_triggers_per_slot() -> u32 {
    1
}

impl Config {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)?;
        let cfg: Config = serde_json::from_str(&text)?;
        Ok(cfg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_config() {
        let json = r#"{
          "rpc": { "url": "https://api.mainnet.solana.com" },
          "wallet": { "keypair_path": "/tmp/wallet.json" },
          "sources": {
            "shredstream_grpc_url": "http://127.0.0.1:9999",
            "yellowstone_grpc_url": "https://example.com:2053",
            "yellowstone_auth_token": "tok"
          },
          "supervisor": {},
          "schedule": {},
          "tx": {},
          "senders": [
            { "id": 0, "name": "helius-dual", "kind": "helius",
              "endpoint_url": "http://x", "tip_lamports": 200000 }
          ],
          "run": {
            "output_dir": "runs",
            "duration": "60s",
            "triggers_per_slot": 1
          }
        }"#;
        let cfg: Config = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.senders.len(), 1);
        assert_eq!(cfg.supervisor.entry_timeout_ms, 150);
        assert_eq!(cfg.supervisor.slot_seal_lag_slots, 10);
        assert_eq!(cfg.tx.priority_fee_microlamports, 5000);
        assert_eq!(cfg.schedule.chunk_size_slots, 30);
        assert_eq!(cfg.run.triggers_per_slot, 1);
    }

    #[test]
    fn disabled_sender_parses() {
        let json = r#"{ "id":1, "name":"x", "kind":"helius",
          "endpoint_url":"http://x", "tip_lamports":1000, "enabled":false }"#;
        let s: SenderConfig = serde_json::from_str(json).unwrap();
        assert!(!s.enabled);
    }
}
