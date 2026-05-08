use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub validators_app: ValidatorsAppConfig,
    pub solana_rpc: SolanaRpcConfig,
    pub cache: CacheConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidatorsAppConfig {
    /// Bearer token from https://www.validators.app/users/sign_up → Settings → API Tokens.
    pub api_token: String,
    /// Base URL — usually https://www.validators.app
    #[serde(default = "default_validators_app_base_url")]
    pub base_url: String,
    /// "mainnet" | "testnet"
    #[serde(default = "default_network")]
    pub network: String,
}

fn default_validators_app_base_url() -> String {
    "https://www.validators.app".into()
}
fn default_network() -> String {
    "mainnet".into()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SolanaRpcConfig {
    /// HTTP RPC endpoint, e.g. https://api.mainnet-beta.solana.com or your own dedicated node.
    pub url: String,
    /// Optional auth header value if your RPC needs it.
    #[serde(default)]
    pub auth_header: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    /// Where to write per-epoch JSON snapshots. Defaults to `./runs`.
    #[serde(default = "default_cache_dir")]
    pub dir: PathBuf,
}
fn default_cache_dir() -> PathBuf {
    PathBuf::from("./runs")
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read config at {}", path.display()))?;
        let cfg: Config = serde_json::from_str(&raw)
            .with_context(|| format!("failed to parse config at {}", path.display()))?;
        Ok(cfg)
    }
}
