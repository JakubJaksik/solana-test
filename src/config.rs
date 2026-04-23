//! Konfiguracja runu — JSON, wallety inline, walidacja fail-fast.

use serde::{Deserialize, Serialize};
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON parse error: {0}")]
    Parse(#[from] serde_json::Error),
    #[error("validation error: {0}")]
    Validation(String),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub chain: ChainConfig,
    pub timing: TimingConfig,
    pub gas: GasConfig,
    pub tracking: TrackingConfig,
    pub swap: SwapConfig,
    pub wallets: Vec<WalletConfig>,
    pub output: OutputConfig,
    #[serde(default)]
    pub send: SendConfig,
    /// Opcjonalna cena ETH w USD do wyświetlania kosztów w pre-flight summary.
    /// Nie wpływa na logikę — tylko display.
    #[serde(default)]
    pub eth_price_usd: Option<f64>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ChainConfig {
    pub name: String,
    pub chain_id: u64,
    pub rpc_ws: String,
    pub rpc_http: String,
    /// Optional write-only endpoint (np. sequencer.base.org) używany tylko
    /// dla eth_sendRawTransaction. Jeśli pominięty, wszystko idzie do rpc_http.
    #[serde(default)]
    pub rpc_http_send: Option<String>,
    /// Optional MEV-builder bundle endpoint (np. rpc.beaverbuild.org na ETH).
    /// Gdy ustawiony, engine main loop używa `eth_sendBundle` z `blockNumber`
    /// targetującym N+1 zamiast `eth_sendRawTransaction`. Preflight
    /// (approve + calibration) nadal chodzi przez rpc_http_send/rpc_http
    /// bo bundle wymaga konkretnego target bloku.
    #[serde(default)]
    pub bundle_url: Option<String>,
}

impl ChainConfig {
    /// Zwraca URL do użycia dla eth_sendRawTransaction.
    /// Prefer rpc_http_send (dedykowany send endpoint, np. sequencer),
    /// fallback do rpc_http.
    pub fn send_url(&self) -> &str {
        self.rpc_http_send.as_deref().unwrap_or(&self.rpc_http)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TimingConfig {
    pub start_ms: u64,
    pub end_ms: u64,
    pub step_ms: u64,
    pub samples_per_wallet_per_slot: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GasConfig {
    pub max_priority_fee_gwei: f64,
    pub max_fee_multiplier: f64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TrackingConfig {
    pub inclusion_lookahead_blocks: u64,
    pub abort_on_consecutive_failed_blocks: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SwapConfig {
    pub router_address: String,
    pub pool_fee_tier: u32,
    pub token_a: String,
    pub token_b: String,
    pub amount_in_a: String,
    pub amount_in_b: String,
    pub slippage_bps: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WalletConfig {
    pub label: String,
    pub private_key: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OutputConfig {
    pub dir: String,
    pub stdout_report: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct SendConfig {
    #[serde(default)]
    pub worker_threads: Option<usize>,
    #[serde(default)]
    pub spin_window_us: Option<u64>,
}

impl SendConfig {
    pub fn resolved_worker_threads(&self, wallet_count: usize) -> usize {
        self.worker_threads.unwrap_or(wallet_count.max(1))
    }

    pub fn resolved_spin_window_us(&self) -> u64 {
        self.spin_window_us.unwrap_or(2000)
    }
}

#[derive(Debug, Serialize)]
pub struct ConfigSnapshot<'a> {
    pub chain: &'a ChainConfig,
    pub timing: &'a TimingConfig,
    pub gas: &'a GasConfig,
    pub tracking: &'a TrackingConfig,
    pub swap: &'a SwapConfig,
    pub output: &'a OutputConfig,
    pub wallets: Vec<WalletSnapshot<'a>>,
    pub send: &'a SendConfig,
}

#[derive(Debug, Serialize)]
pub struct WalletSnapshot<'a> {
    pub label: &'a str,
    pub private_key: &'static str,
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let raw = std::fs::read_to_string(path)?;
        let cfg: Config = serde_json::from_str(&raw)?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn to_snapshot(&self) -> ConfigSnapshot<'_> {
        ConfigSnapshot {
            chain: &self.chain,
            timing: &self.timing,
            gas: &self.gas,
            tracking: &self.tracking,
            swap: &self.swap,
            output: &self.output,
            send: &self.send,
            wallets: self
                .wallets
                .iter()
                .map(|w| WalletSnapshot {
                    label: &w.label,
                    private_key: "***redacted***",
                })
                .collect(),
        }
    }

    fn validate(&self) -> Result<(), ConfigError> {
        use ConfigError::Validation as V;

        if self.wallets.is_empty() {
            return Err(V("wallets must not be empty".into()));
        }

        let mut labels = std::collections::HashSet::new();
        for w in &self.wallets {
            if !labels.insert(&w.label) {
                return Err(V(format!("duplicate wallet label: {}", w.label)));
            }
            if !w.private_key.starts_with("0x") || w.private_key.len() != 66 {
                return Err(V(format!(
                    "wallet '{}' invalid private_key format",
                    w.label
                )));
            }
        }

        if self.timing.start_ms >= self.timing.end_ms {
            return Err(V(format!(
                "timing.start_ms ({}) must be < end_ms ({})",
                self.timing.start_ms, self.timing.end_ms
            )));
        }
        if self.timing.step_ms == 0 {
            return Err(V("timing.step_ms must be > 0".into()));
        }
        if self.timing.samples_per_wallet_per_slot == 0 {
            return Err(V("timing.samples_per_wallet_per_slot must be > 0".into()));
        }

        if self.tracking.inclusion_lookahead_blocks < 2 {
            return Err(V("tracking.inclusion_lookahead_blocks must be >= 2".into()));
        }

        if self.gas.max_priority_fee_gwei < 0.0 {
            return Err(V("gas.max_priority_fee_gwei must be >= 0".into()));
        }
        if self.gas.max_fee_multiplier < 1.0 {
            return Err(V("gas.max_fee_multiplier must be >= 1.0".into()));
        }

        if !is_hex_address(&self.swap.router_address)
            || !is_hex_address(&self.swap.token_a)
            || !is_hex_address(&self.swap.token_b)
        {
            return Err(V("swap addresses must be 0x-prefixed 40-hex".into()));
        }

        Ok(())
    }
}

fn is_hex_address(s: &str) -> bool {
    s.starts_with("0x") && s.len() == 42 && s[2..].chars().all(|c| c.is_ascii_hexdigit())
}
