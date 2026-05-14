//! Configuration types for fan-out-bench.
//!
//! Loaded from JSON. See `config.example.json` for full example.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub run: RunConfig,
    pub sources: SourcesConfig,
    pub nonce: NonceConfig,
    pub senders: Vec<SenderConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunConfig {
    pub wallet_keypair_path: PathBuf,
    pub output_dir: PathBuf,
    pub schedule_seed: Option<u64>,
    pub chunk_size_slots: u64,
    pub min_balance_lamports: u64,
    pub priority_fee_microlamports: u64,
    pub compute_unit_limit: u32,
    pub observation_deadline_secs: u64,
    pub core_pinning: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourcesConfig {
    pub shredstream_grpc_url: String,
    pub yellowstone_grpc_url: String,
    pub yellowstone_auth_token: Option<String>,
    pub helius_rpc_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NonceConfig {
    pub config_path: PathBuf,
    pub keypairs_path: PathBuf,
    pub pool_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SenderConfig {
    pub id: u8,
    pub name: String,
    pub kind: SenderKind,
    pub endpoint_url: String,
    pub region: String,
    pub auth: AuthConfig,
    pub tip_lamports: u64,
    pub enabled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SenderKind {
    Mock,
    Helius,
    Jito,
    JitoBundle,
    Nozomi,
    Syncro,
    Astralane,
    Slot0,
    AllenharkQuic,
    AllenharkHttps,
    Nextblock,
    NextblockQuic,
    Bloxroute,
    BlockrazorHttp,
    BlockrazorGrpc,
    Triton,
    Harmonic,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AuthConfig {
    None,
    QueryParam { key: String, value: String },
    Header { name: String, value: String },
    Bearer { token: String },
    PathToken { token: String },
}

impl Config {
    pub fn load(path: &std::path::Path) -> anyhow::Result<Self> {
        let bytes = std::fs::read(path)?;
        let config: Self = serde_json::from_slice(&bytes)?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(self.nonce.pool_size > 0, "nonce.pool_size must be > 0");
        anyhow::ensure!(self.run.chunk_size_slots > 0, "run.chunk_size_slots must be > 0");
        anyhow::ensure!(!self.senders.is_empty(), "at least one sender required");
        let mut seen_ids = std::collections::HashSet::new();
        for sender in &self.senders {
            anyhow::ensure!(
                seen_ids.insert(sender.id),
                "duplicate sender id: {}",
                sender.id
            );
            anyhow::ensure!(sender.id <= 93, "sender.id {} > 93 (memo encoding limit)", sender.id);
        }
        Ok(())
    }

    pub fn enabled_senders(&self) -> impl Iterator<Item = &SenderConfig> {
        self.senders.iter().filter(|s| s.enabled)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_rejects_duplicate_sender_ids() {
        let config = Config {
            run: dummy_run_config(),
            sources: dummy_sources_config(),
            nonce: NonceConfig { config_path: "x".into(), keypairs_path: "y".into(), pool_size: 10 },
            senders: vec![
                SenderConfig { id: 1, name: "a".into(), kind: SenderKind::Mock, endpoint_url: "".into(), region: "fra".into(), auth: AuthConfig::None, tip_lamports: 1000, enabled: true },
                SenderConfig { id: 1, name: "b".into(), kind: SenderKind::Mock, endpoint_url: "".into(), region: "fra".into(), auth: AuthConfig::None, tip_lamports: 1000, enabled: true },
            ],
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn validate_rejects_sender_id_over_93() {
        let config = Config {
            run: dummy_run_config(),
            sources: dummy_sources_config(),
            nonce: NonceConfig { config_path: "x".into(), keypairs_path: "y".into(), pool_size: 10 },
            senders: vec![
                SenderConfig { id: 94, name: "a".into(), kind: SenderKind::Mock, endpoint_url: "".into(), region: "fra".into(), auth: AuthConfig::None, tip_lamports: 1000, enabled: true },
            ],
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn validate_rejects_empty_senders() {
        let config = Config {
            run: dummy_run_config(),
            sources: dummy_sources_config(),
            nonce: NonceConfig { config_path: "x".into(), keypairs_path: "y".into(), pool_size: 10 },
            senders: vec![],
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn enabled_senders_filter() {
        let config = Config {
            run: dummy_run_config(),
            sources: dummy_sources_config(),
            nonce: NonceConfig { config_path: "x".into(), keypairs_path: "y".into(), pool_size: 10 },
            senders: vec![
                SenderConfig { id: 1, name: "on".into(), kind: SenderKind::Mock, endpoint_url: "".into(), region: "fra".into(), auth: AuthConfig::None, tip_lamports: 1000, enabled: true },
                SenderConfig { id: 2, name: "off".into(), kind: SenderKind::Mock, endpoint_url: "".into(), region: "fra".into(), auth: AuthConfig::None, tip_lamports: 1000, enabled: false },
            ],
        };
        let names: Vec<_> = config.enabled_senders().map(|s| s.name.as_str()).collect();
        assert_eq!(names, vec!["on"]);
    }

    #[test]
    fn parse_example_config_file() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("config.example.json");
        let config = Config::load(&path).expect("example config should parse and validate");
        assert!(!config.senders.is_empty());
        assert_eq!(config.nonce.pool_size, 150);
    }

    fn dummy_run_config() -> RunConfig {
        RunConfig {
            wallet_keypair_path: "wallet.json".into(),
            output_dir: "runs".into(),
            schedule_seed: Some(42),
            chunk_size_slots: 1000,
            min_balance_lamports: 1_500_000,
            priority_fee_microlamports: 5000,
            compute_unit_limit: 200_000,
            observation_deadline_secs: 90,
            core_pinning: None,
        }
    }

    fn dummy_sources_config() -> SourcesConfig {
        SourcesConfig {
            shredstream_grpc_url: "http://127.0.0.1:9999".into(),
            yellowstone_grpc_url: "http://localhost:10000".into(),
            yellowstone_auth_token: None,
            helius_rpc_url: "https://api.mainnet.solana.com".into(),
        }
    }
}
