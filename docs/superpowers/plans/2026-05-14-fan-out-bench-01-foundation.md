# fan-out-bench — Plan 1: Foundation

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Postaw scaffolding nowego crate'a `fan-out-bench` w workspace, zbuduj pełną pre-network warstwę (config, schedule, memo, tip accounts, tx_builder, pool, parquet schema) z testami i mock end-to-end flow potwierdzającym wiring. Po tym planie nie wysyłamy nic do prawdziwych senderów, ale mamy testowalne presigning + parquet writing + scheduling.

**Architecture:** Standalone crate w workspace, reuses `entry-sources` types. TDD per moduł, frequent commits. Każdy moduł testowalny w izolacji + 1 mock e2e test na końcu.

**Tech Stack:** Rust 2024, solana-sdk 3.0, dashmap, parquet/arrow, crossbeam-channel, tokio (minimalnie w P1, więcej w P4+), tracing.

**Reference spec:** `docs/superpowers/specs/2026-05-14-fan-out-bench-design.md`

---

## File structure (Plan 1 scope)

Files created in this plan:

```
crates/fan-out-bench/
├── Cargo.toml                         — workspace member declaration
├── README.md                          — short overview (will grow in later plans)
├── config.example.json                — example config with 2 mock senders
├── src/
│   ├── lib.rs                         — module declarations only
│   ├── main.rs                        — minimal CLI stub (clap; full CLI in Plan 7)
│   ├── config.rs                      — Config, SenderConfig, RunConfig structs (serde)
│   ├── schedule.rs                    — Schedule, ScheduleEntry, deterministic gen
│   ├── memo.rs                        — encode/decode sender_id ↔ memo byte
│   ├── tip_accounts.rs                — static per-sender tip account lists + RR rotator
│   ├── tx_builder.rs                  — central tx composition, hard asserts on layout
│   ├── pool.rs                        — DashMap<(slot,tick,sender_id), PreSignedTx>
│   ├── senders/
│   │   ├── mod.rs                     — TxSender trait + SenderId/SendOutcome types
│   │   └── mock.rs                    — MockSender for tests
│   ├── outcome.rs                     — TentativeOutcome / FinalStatus enums
│   ├── attempt_state.rs               — AttemptState enum for matcher
│   ├── writer/
│   │   ├── mod.rs                     — re-exports
│   │   ├── schema.rs                  — Arrow schema definition
│   │   ├── record.rs                  — FinalRecord struct, conversion to Arrow row
│   │   └── parquet_sink.rs            — background thread writer, row groups
│   └── counters.rs                    — BenchCounters atomic struct
└── tests/
    └── e2e_mock.rs                    — full pipeline test with mocks
```

NOT in this plan (deferred to later plans):
- nonce_manager, sources, observer, dispatcher, matcher, finality_tracker, real senders, setup-nonces binary, runtime wiring, budget_watcher, clock_monitor

---

## Task 1: Cargo workspace scaffolding

**Files:**
- Create: `crates/fan-out-bench/Cargo.toml`
- Create: `crates/fan-out-bench/src/lib.rs`
- Create: `crates/fan-out-bench/src/main.rs`
- Modify: `Cargo.toml` (workspace root) — add member

- [ ] **Step 1: Add crate as workspace member**

Edit root `Cargo.toml`, in `[workspace] members` array add `"crates/fan-out-bench"`:

```toml
members = [
    "crates/tx-cutoff",
    "crates/solana-leader-map",
    "crates/entry-sources",
    "crates/entry-comparator",
    "crates/tick-trigger-bench",
    "crates/fan-out-bench",
]
```

- [ ] **Step 2: Create crate Cargo.toml**

Write `crates/fan-out-bench/Cargo.toml`:

```toml
[package]
name = "fan-out-bench"
version = "0.1.0"
edition.workspace = true
license.workspace = true
rust-version.workspace = true
description = "Etap 1 — fan-out multi-sender Solana tx send benchmark with durable nonce dedup"

[dependencies]
entry-sources = { path = "../entry-sources" }
tokio.workspace = true
crossbeam-channel.workspace = true
core_affinity.workspace = true
clap = { version = "4", features = ["derive", "env"] }
serde.workspace = true
serde_json.workspace = true
anyhow.workspace = true
thiserror.workspace = true
tracing.workspace = true
tracing-subscriber.workspace = true
chrono.workspace = true
mimalloc.workspace = true
arrow.workspace = true
arrow-array.workspace = true
arrow-schema.workspace = true
parquet.workspace = true
smallvec.workspace = true
solana-sdk.workspace = true
solana-client.workspace = true
solana-compute-budget-interface = "3.0"
solana-system-interface = "3.2"
spl-memo = "6"
dashmap.workspace = true
rand.workspace = true
arc-swap.workspace = true
bs58.workspace = true
humantime.workspace = true
hostname.workspace = true
reqwest = { version = "0.12", features = ["rustls-tls"], default-features = false }
bincode.workspace = true
base64 = "0.22"
futures-util.workspace = true
sha2 = "0.10"
hex = "0.4"

[dev-dependencies]
tokio = { version = "1", features = ["full", "test-util"] }
pretty_assertions = "1"
tempfile = "3"
```

- [ ] **Step 3: Create lib.rs with module declarations**

Write `crates/fan-out-bench/src/lib.rs`:

```rust
pub mod attempt_state;
pub mod config;
pub mod counters;
pub mod memo;
pub mod outcome;
pub mod pool;
pub mod schedule;
pub mod senders;
pub mod tip_accounts;
pub mod tx_builder;
pub mod writer;
```

- [ ] **Step 4: Create minimal main.rs**

Write `crates/fan-out-bench/src/main.rs`:

```rust
fn main() {
    tracing_subscriber::fmt::init();
    println!("fan-out-bench v0.1.0 — full CLI in Plan 7");
}
```

- [ ] **Step 5: Verify build**

Run: `cargo check -p fan-out-bench`
Expected: build succeeds, may warn about empty modules (OK).

Note: at this point lib.rs references modules that don't exist yet — to avoid compile errors, create empty stub files first:

```bash
cd /home/jjaksik/Repos/my-scripts/crates/fan-out-bench/src
mkdir -p senders writer
touch attempt_state.rs config.rs counters.rs memo.rs outcome.rs pool.rs schedule.rs tip_accounts.rs tx_builder.rs
touch senders/mod.rs writer/mod.rs
```

Each stub file should have placeholder `// implementation in later task` comment to satisfy compiler. For modules with submodules (`senders`, `writer`), put `// submodules added in later tasks` in `mod.rs`.

Run again: `cargo check -p fan-out-bench` — must pass.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml crates/fan-out-bench/
git commit -m "feat(fan-out-bench): scaffold crate with workspace member + stub modules"
```

---

## Task 2: Memo encoding/decoding

**Files:**
- Create: `crates/fan-out-bench/src/memo.rs`

- [ ] **Step 1: Write failing tests**

Write `crates/fan-out-bench/src/memo.rs`:

```rust
//! Memo program payload encoding for sender attribution.
//!
//! SPL Memo program validates UTF-8. We use 1 byte from ASCII printable range
//! (0x21..0x7E, '!' to '~') to encode sender_id 0..93. Decoder: byte - b'!'.

const BASE: u8 = b'!';
const MAX_SENDER_ID: u8 = b'~' - BASE; // = 93

#[derive(Debug, thiserror::Error)]
pub enum MemoError {
    #[error("sender_id {0} exceeds max {}", MAX_SENDER_ID)]
    SenderIdTooLarge(u8),
    #[error("memo byte {0:#x} out of valid range 0x21..0x7E")]
    InvalidMemoByte(u8),
    #[error("memo data must be exactly 1 byte, got {0}")]
    WrongLength(usize),
}

pub fn encode(sender_id: u8) -> Result<[u8; 1], MemoError> {
    if sender_id > MAX_SENDER_ID {
        return Err(MemoError::SenderIdTooLarge(sender_id));
    }
    Ok([BASE + sender_id])
}

pub fn decode(memo: &[u8]) -> Result<u8, MemoError> {
    if memo.len() != 1 {
        return Err(MemoError::WrongLength(memo.len()));
    }
    let byte = memo[0];
    if byte < b'!' || byte > b'~' {
        return Err(MemoError::InvalidMemoByte(byte));
    }
    Ok(byte - BASE)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_round_trip_full_range() {
        for sender_id in 0..=MAX_SENDER_ID {
            let memo = encode(sender_id).unwrap();
            assert_eq!(memo.len(), 1);
            let decoded = decode(&memo).unwrap();
            assert_eq!(decoded, sender_id);
        }
    }

    #[test]
    fn encode_zero_is_exclamation_mark() {
        assert_eq!(encode(0).unwrap(), [b'!']);
    }

    #[test]
    fn encode_max_is_tilde() {
        assert_eq!(encode(MAX_SENDER_ID).unwrap(), [b'~']);
    }

    #[test]
    fn encode_rejects_too_large() {
        assert!(matches!(encode(94), Err(MemoError::SenderIdTooLarge(94))));
        assert!(matches!(encode(255), Err(MemoError::SenderIdTooLarge(255))));
    }

    #[test]
    fn decode_rejects_wrong_length() {
        assert!(matches!(decode(&[]), Err(MemoError::WrongLength(0))));
        assert!(matches!(decode(&[b'!', b'!']), Err(MemoError::WrongLength(2))));
    }

    #[test]
    fn decode_rejects_out_of_range() {
        assert!(matches!(decode(&[0x20]), Err(MemoError::InvalidMemoByte(0x20)))); // space
        assert!(matches!(decode(&[0x7F]), Err(MemoError::InvalidMemoByte(0x7F)))); // DEL
        assert!(matches!(decode(&[0xFF]), Err(MemoError::InvalidMemoByte(0xFF))));
    }

    #[test]
    fn all_encoded_bytes_are_valid_utf8() {
        for sender_id in 0..=MAX_SENDER_ID {
            let memo = encode(sender_id).unwrap();
            assert!(std::str::from_utf8(&memo).is_ok(), "sender_id {} produced invalid utf8", sender_id);
        }
    }
}
```

- [ ] **Step 2: Run tests, verify pass**

Run: `cargo test -p fan-out-bench --lib memo`
Expected: 6 tests pass.

- [ ] **Step 3: Commit**

```bash
git add crates/fan-out-bench/src/memo.rs
git commit -m "feat(fan-out-bench): add memo encoder/decoder with UTF-8 safe ASCII range"
```

---

## Task 3: Config types

**Files:**
- Create: `crates/fan-out-bench/src/config.rs`
- Create: `crates/fan-out-bench/config.example.json`

- [ ] **Step 1: Write config module with serde structs**

Write `crates/fan-out-bench/src/config.rs`:

```rust
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[serde(rename_all = "snake_case")]
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
```

- [ ] **Step 2: Create config.example.json**

Write `crates/fan-out-bench/config.example.json`:

```json
{
  "run": {
    "wallet_keypair_path": "~/.config/solana/dex-bench.json",
    "output_dir": "runs",
    "schedule_seed": null,
    "chunk_size_slots": 1000,
    "min_balance_lamports": 1500000,
    "priority_fee_microlamports": 5000,
    "compute_unit_limit": 200000,
    "observation_deadline_secs": 90,
    "core_pinning": "ss_grpc=2,ys_grpc=3,merger=4,observer=5,preparer=6,matcher=7,writer=8"
  },
  "sources": {
    "shredstream_grpc_url": "http://127.0.0.1:9999",
    "yellowstone_grpc_url": "http://localhost:10000",
    "yellowstone_auth_token": null,
    "helius_rpc_url": "https://api.mainnet.solana.com"
  },
  "nonce": {
    "config_path": "nonce-config.json",
    "keypairs_path": "nonce-keypairs.json",
    "pool_size": 150
  },
  "senders": [
    {
      "id": 0,
      "name": "mock-a",
      "kind": "mock",
      "endpoint_url": "mock://localhost",
      "region": "fra",
      "auth": { "type": "none" },
      "tip_lamports": 1000,
      "enabled": true
    },
    {
      "id": 1,
      "name": "mock-b",
      "kind": "mock",
      "endpoint_url": "mock://localhost",
      "region": "ams",
      "auth": { "type": "none" },
      "tip_lamports": 1000,
      "enabled": true
    }
  ]
}
```

- [ ] **Step 3: Add roundtrip test for example config**

Add this test at the end of `src/config.rs` `mod tests`:

```rust
    #[test]
    fn parse_example_config_file() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("config.example.json");
        let config = Config::load(&path).expect("example config should parse and validate");
        assert!(!config.senders.is_empty());
        assert_eq!(config.nonce.pool_size, 150);
    }
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p fan-out-bench --lib config`
Expected: 5 tests pass (including new example file parsing).

- [ ] **Step 5: Commit**

```bash
git add crates/fan-out-bench/src/config.rs crates/fan-out-bench/config.example.json
git commit -m "feat(fan-out-bench): add Config types + example JSON with validation"
```

---

## Task 4: Outcome and AttemptState enums

**Files:**
- Create: `crates/fan-out-bench/src/outcome.rs`
- Create: `crates/fan-out-bench/src/attempt_state.rs`

- [ ] **Step 1: Write outcome enums**

Write `crates/fan-out-bench/src/outcome.rs`:

```rust
//! Outcome enums for tx attempts.
//!
//! See spec §3.4 — dwustopniowa rezolucja:
//! - `tentative_outcome` emitowane real-time przez matcher
//! - `final_status` emitowane post-finality przez finality_tracker

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TentativeOutcome {
    LandedTentative,
    DedupedTentative,
    UnknownPending,
    TrulyMissing,
    SendError,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FinalStatus {
    Pending,
    Confirmed,
    ReorgedOut,
    UncertainNoStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ObservedSource {
    Ss,
    Ys,
    Both,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CommitmentAtResolution {
    Processed,
    Confirmed,
    Finalized,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RateLimitState {
    Ok,
    Throttled429,
    CircuitOpen,
    Timeout,
}

impl TentativeOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            TentativeOutcome::LandedTentative => "LANDED_TENTATIVE",
            TentativeOutcome::DedupedTentative => "DEDUPED_TENTATIVE",
            TentativeOutcome::UnknownPending => "UNKNOWN_PENDING",
            TentativeOutcome::TrulyMissing => "TRULY_MISSING",
            TentativeOutcome::SendError => "SEND_ERROR",
        }
    }
}

impl FinalStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            FinalStatus::Pending => "PENDING",
            FinalStatus::Confirmed => "CONFIRMED",
            FinalStatus::ReorgedOut => "REORGED_OUT",
            FinalStatus::UncertainNoStatus => "UNCERTAIN_NO_STATUS",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde_roundtrip_tentative_outcome() {
        let json = serde_json::to_string(&TentativeOutcome::LandedTentative).unwrap();
        assert_eq!(json, "\"LANDED_TENTATIVE\"");
        let parsed: TentativeOutcome = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, TentativeOutcome::LandedTentative);
    }

    #[test]
    fn as_str_matches_serde() {
        let json = serde_json::to_string(&TentativeOutcome::DedupedTentative).unwrap();
        // serde produces "DEDUPED_TENTATIVE", as_str returns same minus quotes
        assert_eq!(json.trim_matches('"'), TentativeOutcome::DedupedTentative.as_str());
    }
}
```

- [ ] **Step 2: Write attempt_state**

Write `crates/fan-out-bench/src/attempt_state.rs`:

```rust
//! AttemptState — single-owner state machine per (trigger_id, sender_id).
//!
//! See spec §7.2 — eliminuje race conditions w matcher.

use crate::outcome::{ObservedSource, TentativeOutcome};
use solana_sdk::signature::Signature;

#[derive(Debug, Clone)]
pub enum AttemptState {
    SentPending {
        send_at_ns: u64,
        sig: Signature,
    },
    SentAcked {
        send_at_ns: u64,
        send_ack_at_ns: u64,
        sig: Signature,
        provider_request_id: Option<String>,
    },
    SendFailed {
        send_at_ns: u64,
        send_ack_at_ns: Option<u64>,
        error: String,
        sig: Signature,
    },
    ObservedTentative {
        send_at_ns: u64,
        send_ack_at_ns: Option<u64>,
        sig: Signature,
        observed_at_ns: u64,
        observed_source: ObservedSource,
        outcome: TentativeOutcome, // LandedTentative or DedupedTentative
        provider_request_id: Option<String>,
    },
    UnknownPending {
        send_at_ns: u64,
        send_ack_at_ns: Option<u64>,
        sig: Signature,
    },
    TrulyMissing {
        send_at_ns: u64,
        send_ack_at_ns: Option<u64>,
        sig: Signature,
    },
}

impl AttemptState {
    pub fn sig(&self) -> &Signature {
        match self {
            Self::SentPending { sig, .. }
            | Self::SentAcked { sig, .. }
            | Self::SendFailed { sig, .. }
            | Self::ObservedTentative { sig, .. }
            | Self::UnknownPending { sig, .. }
            | Self::TrulyMissing { sig, .. } => sig,
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::SendFailed { .. }
                | Self::ObservedTentative { .. }
                | Self::TrulyMissing { .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::signature::Signature;

    #[test]
    fn is_terminal_correct() {
        let sig = Signature::default();
        assert!(!AttemptState::SentPending { send_at_ns: 0, sig }.is_terminal());
        assert!(!AttemptState::SentAcked { send_at_ns: 0, send_ack_at_ns: 1, sig, provider_request_id: None }.is_terminal());
        assert!(AttemptState::SendFailed { send_at_ns: 0, send_ack_at_ns: None, error: "x".into(), sig }.is_terminal());
        assert!(AttemptState::TrulyMissing { send_at_ns: 0, send_ack_at_ns: None, sig }.is_terminal());
    }
}
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p fan-out-bench --lib outcome attempt_state`
Expected: 3 tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/fan-out-bench/src/outcome.rs crates/fan-out-bench/src/attempt_state.rs
git commit -m "feat(fan-out-bench): add outcome enums + AttemptState state machine types"
```

---

## Task 5: Tip account registry

**Files:**
- Create: `crates/fan-out-bench/src/tip_accounts.rs`

- [ ] **Step 1: Write tip accounts module with all 12 sender lists**

Write `crates/fan-out-bench/src/tip_accounts.rs`. (Pełne listy z spec §5.2.):

```rust
//! Static tip account lists per sender + RR rotator.
//!
//! Source: spec §5.2 — adresy zebrane z official docs każdego sendera.

use crate::config::SenderKind;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct TipAccountRotator {
    accounts: Vec<Pubkey>,
    cursor: AtomicUsize,
}

impl TipAccountRotator {
    pub fn new(accounts: Vec<Pubkey>) -> Self {
        Self {
            accounts,
            cursor: AtomicUsize::new(0),
        }
    }

    pub fn next(&self) -> Option<Pubkey> {
        if self.accounts.is_empty() {
            return None;
        }
        let idx = self.cursor.fetch_add(1, Ordering::Relaxed) % self.accounts.len();
        Some(self.accounts[idx])
    }

    pub fn len(&self) -> usize {
        self.accounts.len()
    }

    pub fn is_empty(&self) -> bool {
        self.accounts.is_empty()
    }
}

pub fn tip_accounts_for(kind: SenderKind) -> Vec<Pubkey> {
    let strs: &[&str] = match kind {
        SenderKind::Mock => &[],
        SenderKind::Helius => HELIUS,
        SenderKind::Jito | SenderKind::JitoBundle => JITO,
        SenderKind::Nozomi => NOZOMI,
        SenderKind::Syncro => SYNCRO,
        SenderKind::Astralane => ASTRALANE,
        SenderKind::Slot0 => SLOT0,
        SenderKind::AllenharkQuic | SenderKind::AllenharkHttps => ALLENHARK,
        SenderKind::Nextblock | SenderKind::NextblockQuic => NEXTBLOCK,
        SenderKind::Bloxroute => BLOXROUTE,
        SenderKind::BlockrazorHttp | SenderKind::BlockrazorGrpc => BLOCKRAZOR,
        SenderKind::Triton | SenderKind::Harmonic => &[], // no vendor tip account
    };
    strs.iter()
        .map(|s| Pubkey::from_str(s).expect("hardcoded tip pubkey must parse"))
        .collect()
}

const HELIUS: &[&str] = &[
    "4ACfpUFoaSD9bfPdeu6DBt89gB6ENTeHBXCAi87NhDEE",
    "D2L6yPZ2FmmmTKPgzaMKdhu6EWZcTpLy1Vhx8uvZe7NZ",
    "9bnz4RShgq1hAnLnZbP8kbgBg1kEmcJBYQq3gQbmnSta",
    "5VY91ws6B2hMmBFRsXkoAAdsPHBJwRfBht4DXox3xkwn",
    "2nyhqdwKcJZR2vcqCyrYsaPVdAnFoJjiksCXJ7hfEYgD",
    "2q5pghRs6arqVjRvT5gfgWfWcHWmw1ZuCzphgd5KfWGJ",
    "wyvPkWjVZz1M8fHQnMMCDTQDbkManefNNhweYk5WkcF",
    "3KCKozbAaF75qEU33jtzozcJ29yJuaLJTy2jFdzUY8bT",
    "4vieeGHPYPG2MmyPRcYjdiDmmhN3ww7hsFNap8pVN3Ey",
    "4TQLFNWK8AovT1gFvda5jfw2oJeRMKEmw7aH6MGBJ3or",
];

const JITO: &[&str] = &[
    "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
    "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe",
    "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
    "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
    "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
    "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
    "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
    "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
];

const NOZOMI: &[&str] = &[
    "TEMPaMeCRFAS9EKF53Jd6KpHxgL47uWLcpFArU1Fanq",
    "noz3jAjPiHuBPqiSPkkugaJDkJscPuRhYnSpbi8UvC4",
    "noz3str9KXfpKknefHji8L1mPgimezaiUyCHYMDv1GE",
    "noz6uoYCDijhu1V7cutCpwxNiSovEwLdRHPwmgCGDNo",
    "noz9EPNcT7WH6Sou3sr3GGjHQYVkN3DNirpbvDkv9YJ",
    "nozc5yT15LazbLTFVZzoNZCwjh3yUtW86LoUyqsBu4L",
    "nozFrhfnNGoyqwVuwPAW4aaGqempx4PU6g6D9CJMv7Z",
    "nozievPk7HyK1Rqy1MPJwVQ7qQg2QoJGyP71oeDwbsu",
    "noznbgwYnBLDHu8wcQVCEw6kDrXkPdKkydGJGNXGvL7",
    "nozNVWs5N8mgzuD3qigrCG2UoKxZttxzZ85pvAQVrbP",
    "nozpEGbwx4BcGp6pvEdAh1JoC2CQGZdU6HbNP1v2p6P",
    "nozrhjhkCr3zXT3BiT4WCodYCUFeQvcdUkM7MqhKqge",
    "nozrwQtWhEdrA6W8dkbt9gnUaMs52PdAv5byipnadq3",
    "nozUacTVWub3cL4mJmGCYjKZTnE9RbdY5AP46iQgbPJ",
    "nozWCyTPppJjRuw2fpzDhhWbW355fzosWSzrrMYB1Qk",
    "nozWNju6dY353eMkMqURqwQEoM3SFgEKC6psLCSfUne",
    "nozxNBgWohjR75vdspfxR5H9ceC7XXH99xpxhVGt3Bb",
];

const SYNCRO: &[&str] = &[
    "BPZrtYhdoAhiHWV5EgGLoV7bZFbMamBZurGDq4DmST8v",
    "7D5pdbkV75Sr73M1YFNZwXMed6DenwkdfbJwVWrX6drQ",
    "ELpn2NryEW4B3psG36eSjF45YcGMQpGGuu9J2AgAccbV",
    "FnckAPC9PitnRpGZM2M4WLwb3w9odRLJ7EDRZDngjvd6",
    "3ZnDTgvVfwzqwWoqAUmDkgVtXvXqjmeb5t9zxD5pMbmv",
    "3SLDFcdCzMbcFNguZhzmV4zqEAUvcPoKY13akpE4Tq1p",
    "48tT6LJqrsoFrLpzZSHkjGdGTWtsJ1PvjgWZjh8qF1RK",
    "7GM9fpVMHHcrK4cgzfVdzJvjiy1bSyfwSYzhxvgbfVLg",
    "CBd8GE3ffMJKf3iCCcNNBEifMxH1WpgtTzRnXPxxbjGE",
];

const ASTRALANE: &[&str] = &[
    "astrazznxsGUhWShqgNtAdfrzP2G83DzcWVJDxwV9bF",
    "astra4uejePWneqNaJKuFFA8oonqCE1sqF6b45kDMZm",
    "astra9xWY93QyfG6yM8zwsKsRodscjQ2uU2HKNL5prk",
    "astraRVUuTHjpwEVvNBeQEgwYx9w9CFyfxjYoobCZhL",
    "astraEJ2fEj8Xmy6KLG7B3VfbKfsHXhHrNdCQx7iGJK",
    "astraubkDw81n4LuutzSQ8uzHCv4BhPVhfvTcYv8SKC",
    "astraZW5GLFefxNPAatceHhYjfA1ciq9gvfEg2S47xk",
    "astrawVNP4xDBKT7rAdxrLYiTSTdqtUr63fSMduivXK",
];

const SLOT0: &[&str] = &[
    "6fQaVhYZA4w3MBSXjJ81Vf6W1EDYeUPXpgVQ6UQyU1Av",
    "4HiwLEP2Bzqj3hM2ENxJuzhcPCdsafwiet3oGkMkuQY4",
    "7toBU3inhmrARGngC7z6SjyP85HgGMmCTEwGNRAcYnEK",
    "8mR3wB1nh4D6J9RUCugxUpc6ya8w38LPxZ3ZjcBhgzws",
    "6SiVU5WEwqfFapRuYCndomztEwDjvS5xgtEof3PLEGm9",
    "TpdxgNJBWZRL8UXF5mrEsyWxDWx9HQexA9P1eTWQ42p",
    "D8f3WkQu6dCF33cZxuAsrKHrGsqGP2yvAHf8mX6RXnwf",
    "GQPFicsy3P3NXxB5piJohoxACqTvWE9fKpLgdsMduoHE",
    "Ey2JEr8hDkgN8qKJGrLf2yFjRhW7rab99HVxwi5rcvJE",
    "4iUgjMT8q2hNZnLuhpqZ1QtiV8deFPy2ajvvjEpKKgsS",
    "3Rz8uD83QsU8wKvZbgWAPvCNDU6Fy8TSZTMcPm3RB6zt",
    "DiTmWENJsHQdawVUUKnUXkconcpW4Jv52TnMWhkncF6t",
    "HRyRhQ86t3H4aAtgvHVpUJmw64BDrb61gRiKcdKUXs5c",
    "7y4whZmw388w1ggjToDLSBLv47drw5SUXcLk6jtmwixd",
    "J9BMEWFbCBEjtQ1fG5Lo9kouX1HfrKQxeUxetwXrifBw",
    "8U1JPQh3mVQ4F5jwRdFTBzvNRQaYFQppHQYoH38DJGSQ",
    "Eb2KpSC8uMt9GmzyAEm5Eb1AAAgTjRaXWFjKyFXHZxF3",
    "FCjUJZ1qozm1e8romw216qyfQMaaWKxWsuySnumVCCNe",
    "ENxTEjSQ1YabmUpXAdCgevnHQ9MHdLv8tzFiuiYJqa13",
    "6rYLG55Q9RpsPGvqdPNJs4z5WTxJVatMB8zV3WJhs5EK",
    "Cix2bHfqPcKcM233mzxbLk14kSggUUiz2A87fJtGivXr",
];

const ALLENHARK: &[&str] = &[
    "hark1zxc5Rz3K8Kquz79WPWFEgNCFeJnsMJ16f22uNP",
    "harkm2BTWxZuszoNpZnfe84jRbQTg6KGHaQBmWzDGQQ",
    "hark4CwtTnN2y9FaxjcFBAJdJqQrpouu5pgEixfqdEz",
    "harkoJfnM6dxrJydx5eVmDVwAgwC94KbhuxF69UbXwP",
    "hark6hUDUTekc1DGxWdJcuyDZwf6pJdCxd4SXAVtta6",
    "harkoTvFpKSrEQduYrNHXCurARVT19Ud3BnFhVxabos",
    "harkEpXoJv5qVzHaN7HSuUAd6PHjyMcFMcDYBMDJCEQ",
    "harkyXDdZSoJGyCxa24t2QXx1poPyp8YfghbtpzGSzK",
    "harkR2YJ4Dpt4UDJTcBirjnSPBhNpQFcoFkNpCkVqNk",
    "harkRBygM8pHYe4K8eBjfxyEX19oJn3LepFjvNbLbyi",
    "harkYFxB6DuUFNwDLvA5CQ66KpfRvFgUoVypMagNcmd",
];

const NEXTBLOCK: &[&str] = &[
    "NextbLoCkVtMGcV47JzewQdvBpLqT9TxQFozQkN98pE",
    "NexTbLoCkWykbLuB1NkjXgFWkX9oAtcoagQegygXXA2",
    "NeXTBLoCKs9F1y5PJS9CKrFNNLU1keHW71rfh7KgA1X",
    "NexTBLockJYZ7QD7p2byrUa6df8ndV2WSd8GkbWqfbb",
    "neXtBLock1LeC67jYd1QdAa32kbVeubsfPNTJC1V5At",
    "nEXTBLockYgngeRmRrjDV31mGSekVPqZoMGhQEZtPVG",
    "NEXTbLoCkB51HpLBLojQfpyVAMorm3zzKg7w9NFdqid",
    "nextBLoCkPMgmG8ZgJtABeScP35qLa2AMCNKntAP7Xc",
];

const BLOXROUTE: &[&str] = &[
    "HWEoBxYs7ssKuudEjzjmpfJVX7Dvi7wescFsVx2L5yoY",
    "95cfoy472fcQHaw4tPGBTKpn6ZQnfEPfBgDQx6gcRmRg",
    "3UQUKjhMKaY2S6bjcQD6yHB7utcZt5bfarRCmctpRtUd",
    "FogxVNs6Mm2w9rnGL1vkARSwJxvLE8mujTv3LK8RnUhF",
];

const BLOCKRAZOR: &[&str] = &[
    "Gywj98ophM7GmkDdaWs4isqZnDdFCW7B46TXmKfvyqSm",
    "FjmZZrFvhnqqb9ThCuMVnENaM3JGVuGWNyCAxRJcFpg9",
    "6No2i3aawzHsjtThw81iq1EXPJN6rh8eSJCLaYZfKDTG",
    "A9cWowVAiHe9pJfKAj3TJiN9VpbzMUq6E4kEvf5mUT22",
    "68Pwb4jS7eZATjDfhmTXgRJjCiZmw1L7Huy4HNpnxJ3o",
    "4ABhJh5rZPjv63RBJBuyWzBK3g9gWMUQdTZP2kiW31V9",
    "B2M4NG5eyZp5SBQrSdtemzk5TqVuaWGQnowGaCBt8GyM",
    "5jA59cXMKQqZAVdtopv8q3yyw9SYfiE3vUCbt7p8MfVf",
    "5YktoWygr1Bp9wiS1xtMtUki1PeYuuzuCF98tqwYxf61",
    "295Avbam4qGShBYK7E9H5Ldew4B3WyJGmgmXfiWdeeyV",
    "EDi4rSy2LZgKJX74mbLTFk4mxoTgT6F7HxxzG2HBAFyK",
    "BnGKHAC386n4Qmv9xtpBVbRaUTKixjBe3oagkPFKtoy6",
    "Dd7K2Fp7AtoN8xCghKDRmyqr5U169t48Tw5fEd3wT9mq",
    "AP6qExwrbRgBAVaehg4b5xHENX815sMabtBzUzVB4v8S",
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_hardcoded_addresses_parse() {
        for kind in [
            SenderKind::Helius, SenderKind::Jito, SenderKind::Nozomi,
            SenderKind::Syncro, SenderKind::Astralane, SenderKind::Slot0,
            SenderKind::AllenharkQuic, SenderKind::Nextblock, SenderKind::Bloxroute,
            SenderKind::BlockrazorHttp,
        ] {
            let accounts = tip_accounts_for(kind);
            assert!(!accounts.is_empty(), "{:?} should have tip accounts", kind);
        }
    }

    #[test]
    fn triton_and_harmonic_have_no_vendor_tip() {
        assert!(tip_accounts_for(SenderKind::Triton).is_empty());
        assert!(tip_accounts_for(SenderKind::Harmonic).is_empty());
    }

    #[test]
    fn mock_has_no_tip_accounts() {
        assert!(tip_accounts_for(SenderKind::Mock).is_empty());
    }

    #[test]
    fn rotator_cycles_in_rr() {
        use solana_sdk::pubkey::Pubkey;
        let a = Pubkey::new_unique();
        let b = Pubkey::new_unique();
        let c = Pubkey::new_unique();
        let rotator = TipAccountRotator::new(vec![a, b, c]);
        assert_eq!(rotator.next(), Some(a));
        assert_eq!(rotator.next(), Some(b));
        assert_eq!(rotator.next(), Some(c));
        assert_eq!(rotator.next(), Some(a));
    }

    #[test]
    fn rotator_empty_returns_none() {
        let rotator = TipAccountRotator::new(vec![]);
        assert_eq!(rotator.next(), None);
    }

    #[test]
    fn expected_counts_match_spec() {
        // Sanity check: counts match what spec §5.2 declares
        assert_eq!(tip_accounts_for(SenderKind::Helius).len(), 10);
        assert_eq!(tip_accounts_for(SenderKind::Jito).len(), 8);
        assert_eq!(tip_accounts_for(SenderKind::Nozomi).len(), 17);
        assert_eq!(tip_accounts_for(SenderKind::Syncro).len(), 9);
        assert_eq!(tip_accounts_for(SenderKind::Astralane).len(), 8);
        assert_eq!(tip_accounts_for(SenderKind::Slot0).len(), 21);
        assert_eq!(tip_accounts_for(SenderKind::AllenharkQuic).len(), 11);
        assert_eq!(tip_accounts_for(SenderKind::Nextblock).len(), 8);
        assert_eq!(tip_accounts_for(SenderKind::Bloxroute).len(), 4);
        assert_eq!(tip_accounts_for(SenderKind::BlockrazorHttp).len(), 14);
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p fan-out-bench --lib tip_accounts`
Expected: 6 tests pass, in particular `expected_counts_match_spec` validates list completeness.

- [ ] **Step 3: Commit**

```bash
git add crates/fan-out-bench/src/tip_accounts.rs
git commit -m "feat(fan-out-bench): add per-sender tip account registry + RR rotator"
```

---

## Task 6: Schedule (deterministic, chunked)

**Files:**
- Create: `crates/fan-out-bench/src/schedule.rs`

- [ ] **Step 1: Write Schedule with chunked generation**

Write `crates/fan-out-bench/src/schedule.rs`:

```rust
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
        assert_ne!(s.seed, 0); // unlikely to be exactly 0
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p fan-out-bench --lib schedule`
Expected: 8 tests pass.

- [ ] **Step 3: Commit**

```bash
git add crates/fan-out-bench/src/schedule.rs
git commit -m "feat(fan-out-bench): add deterministic chunked schedule generator"
```

---

## Task 7: TxBuilder (with vendor tip variant)

**Files:**
- Create: `crates/fan-out-bench/src/tx_builder.rs`

- [ ] **Step 1: Write tx_builder with hard asserts**

Write `crates/fan-out-bench/src/tx_builder.rs`:

```rust
//! Central tx composition for fan-out variants.
//!
//! ENFORCEMENT (spec §4.5): AdvanceNonce must be instruction[0], memo must be
//! ASCII printable byte, recent_blockhash must be the nonce_blockhash.
//!
//! All sender impls MUST go through this module. No instruction composition
//! anywhere else in the crate.

use crate::config::SenderKind;
use crate::memo;
use solana_sdk::{
    hash::Hash,
    instruction::Instruction,
    message::Message,
    pubkey::Pubkey,
    signature::{Keypair, Signature, Signer},
    transaction::Transaction,
};
use solana_system_interface::instruction as sys_instruction;
use solana_compute_budget_interface::ComputeBudgetInstruction;

pub struct VariantParams {
    pub nonce_pubkey: Pubkey,
    pub nonce_blockhash: Hash,
    pub payer: Pubkey,           // = authority = wallet
    pub sender_id: u8,
    pub sender_kind: SenderKind,
    pub tip_account: Option<Pubkey>, // None for Triton/Harmonic
    pub tip_lamports: u64,
    pub priority_fee_microlamports: u64,
    pub compute_unit_limit: u32,
}

pub struct VariantTx {
    pub tx: Transaction,
    pub signature: Signature,
    pub message_hash: [u8; 32],
}

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("memo encoding failed: {0}")]
    Memo(#[from] memo::MemoError),
    #[error("tip required for sender kind {0:?} but tip_account is None")]
    MissingTipAccount(SenderKind),
}

pub fn build_variant(p: VariantParams, signer: &Keypair) -> Result<VariantTx, BuildError> {
    // Validate sender_kind vs tip_account combination
    let needs_tip = !matches!(p.sender_kind, SenderKind::Triton | SenderKind::Harmonic | SenderKind::Mock);
    if needs_tip && p.tip_account.is_none() {
        return Err(BuildError::MissingTipAccount(p.sender_kind));
    }

    let memo_bytes = memo::encode(p.sender_id)?;
    let mut ixs: Vec<Instruction> = Vec::with_capacity(6);

    // [0] AdvanceNonceAccount — MUST be first
    ixs.push(sys_instruction::advance_nonce_account(&p.nonce_pubkey, &p.payer));

    // [1] self-transfer with unique amount (1 + sender_id)
    ixs.push(sys_instruction::transfer(&p.payer, &p.payer, 1 + p.sender_id as u64));

    // [2] tip transfer (if vendor uses tip account)
    if let Some(tip_account) = p.tip_account {
        ixs.push(sys_instruction::transfer(&p.payer, &tip_account, p.tip_lamports));
    }

    // [3+] Compute budget
    ixs.push(ComputeBudgetInstruction::set_compute_unit_limit(p.compute_unit_limit));
    ixs.push(ComputeBudgetInstruction::set_compute_unit_price(p.priority_fee_microlamports));

    // [N] Memo program (signers list empty — no signer required)
    let memo_program_id = spl_memo::id();
    ixs.push(Instruction {
        program_id: memo_program_id,
        accounts: vec![],
        data: memo_bytes.to_vec(),
    });

    let message = Message::new_with_blockhash(&ixs, Some(&p.payer), &p.nonce_blockhash);
    let mut tx = Transaction::new_unsigned(message);
    tx.sign(&[signer], p.nonce_blockhash);

    // HARD ASSERTS (debug builds catch errors; release silently trusts builder)
    debug_assert_eq!(tx.message.recent_blockhash, p.nonce_blockhash);
    debug_assert!(is_advance_nonce_instruction(&tx.message, 0),
        "instruction[0] must be AdvanceNonceAccount, got {:?}",
        tx.message.instructions[0]);
    debug_assert_eq!(tx.signatures.len(), 1, "expected single signer");

    let signature = tx.signatures[0];

    // Compute message hash for parquet dedup verification
    use sha2::{Digest, Sha256};
    let serialized = tx.message.serialize();
    let mut hasher = Sha256::new();
    hasher.update(&serialized);
    let message_hash: [u8; 32] = hasher.finalize().into();

    Ok(VariantTx {
        tx,
        signature,
        message_hash,
    })
}

fn is_advance_nonce_instruction(msg: &Message, idx: usize) -> bool {
    use solana_sdk::system_program;
    if idx >= msg.instructions.len() {
        return false;
    }
    let ix = &msg.instructions[idx];
    let program_id = msg.account_keys[ix.program_id_index as usize];
    if program_id != system_program::id() {
        return false;
    }
    // Discriminator: SystemInstruction::AdvanceNonceAccount = variant index 4
    // Layout: first 4 bytes (u32 LE) = variant index
    ix.data.len() >= 4 && ix.data[..4] == [4, 0, 0, 0]
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::signature::Keypair;

    fn dummy_params(sender_kind: SenderKind, sender_id: u8, tip_account: Option<Pubkey>) -> (VariantParams, Keypair) {
        let signer = Keypair::new();
        let nonce_pubkey = Pubkey::new_unique();
        let nonce_blockhash = Hash::new_unique();
        let params = VariantParams {
            nonce_pubkey,
            nonce_blockhash,
            payer: signer.pubkey(),
            sender_id,
            sender_kind,
            tip_account,
            tip_lamports: 5000,
            priority_fee_microlamports: 5000,
            compute_unit_limit: 200_000,
        };
        (params, signer)
    }

    #[test]
    fn instruction_zero_is_advance_nonce() {
        let (p, signer) = dummy_params(SenderKind::Helius, 0, Some(Pubkey::new_unique()));
        let variant = build_variant(p, &signer).unwrap();
        assert!(is_advance_nonce_instruction(&variant.tx.message, 0));
    }

    #[test]
    fn recent_blockhash_is_nonce_blockhash() {
        let (p, signer) = dummy_params(SenderKind::Helius, 0, Some(Pubkey::new_unique()));
        let expected = p.nonce_blockhash;
        let variant = build_variant(p, &signer).unwrap();
        assert_eq!(variant.tx.message.recent_blockhash, expected);
    }

    #[test]
    fn memo_byte_is_ascii_safe() {
        for sender_id in 0..=93u8 {
            let (p, signer) = dummy_params(SenderKind::Helius, sender_id, Some(Pubkey::new_unique()));
            let variant = build_variant(p, &signer).unwrap();
            // Find memo instruction (last one)
            let last_ix = variant.tx.message.instructions.last().unwrap();
            assert_eq!(last_ix.data.len(), 1, "memo should be 1 byte");
            let byte = last_ix.data[0];
            assert!(byte >= b'!' && byte <= b'~', "memo byte {:#x} not ASCII printable", byte);
        }
    }

    #[test]
    fn triton_has_no_tip_instruction() {
        let (p, signer) = dummy_params(SenderKind::Triton, 5, None);
        let variant = build_variant(p, &signer).unwrap();
        // For triton: [0] advance, [1] self-tx, [2] cu_limit, [3] cu_price, [4] memo
        // No tip transfer present
        assert_eq!(variant.tx.message.instructions.len(), 5);
    }

    #[test]
    fn helius_has_tip_instruction() {
        let (p, signer) = dummy_params(SenderKind::Helius, 5, Some(Pubkey::new_unique()));
        let variant = build_variant(p, &signer).unwrap();
        // [0] advance, [1] self-tx, [2] tip, [3] cu_limit, [4] cu_price, [5] memo
        assert_eq!(variant.tx.message.instructions.len(), 6);
    }

    #[test]
    fn helius_without_tip_account_errors() {
        let (p, signer) = dummy_params(SenderKind::Helius, 5, None);
        assert!(matches!(build_variant(p, &signer), Err(BuildError::MissingTipAccount(_))));
    }

    #[test]
    fn variants_for_different_sender_ids_have_different_message_hashes() {
        let common_signer = Keypair::new();
        let nonce_pubkey = Pubkey::new_unique();
        let nonce_blockhash = Hash::new_unique();
        let tip_account = Pubkey::new_unique();
        let make = |sender_id: u8| -> [u8; 32] {
            let params = VariantParams {
                nonce_pubkey,
                nonce_blockhash,
                payer: common_signer.pubkey(),
                sender_id,
                sender_kind: SenderKind::Helius,
                tip_account: Some(tip_account),
                tip_lamports: 5000,
                priority_fee_microlamports: 5000,
                compute_unit_limit: 200_000,
            };
            build_variant(params, &common_signer).unwrap().message_hash
        };
        assert_ne!(make(0), make(1));
        assert_ne!(make(1), make(2));
    }

    #[test]
    fn variants_for_different_sender_ids_have_different_signatures() {
        let common_signer = Keypair::new();
        let nonce_pubkey = Pubkey::new_unique();
        let nonce_blockhash = Hash::new_unique();
        let tip_account = Pubkey::new_unique();
        let make = |sender_id: u8| -> Signature {
            let params = VariantParams {
                nonce_pubkey,
                nonce_blockhash,
                payer: common_signer.pubkey(),
                sender_id,
                sender_kind: SenderKind::Helius,
                tip_account: Some(tip_account),
                tip_lamports: 5000,
                priority_fee_microlamports: 5000,
                compute_unit_limit: 200_000,
            };
            build_variant(params, &common_signer).unwrap().signature
        };
        assert_ne!(make(0), make(1));
        assert_ne!(make(1), make(2));
    }

    #[test]
    fn rejects_sender_id_over_93() {
        let (mut p, signer) = dummy_params(SenderKind::Helius, 94, Some(Pubkey::new_unique()));
        p.sender_id = 94;
        assert!(matches!(build_variant(p, &signer), Err(BuildError::Memo(_))));
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p fan-out-bench --lib tx_builder`
Expected: 9 tests pass.

- [ ] **Step 3: Commit**

```bash
git add crates/fan-out-bench/src/tx_builder.rs
git commit -m "feat(fan-out-bench): add central tx_builder with hard asserts on layout"
```

---

## Task 8: Pool

**Files:**
- Create: `crates/fan-out-bench/src/pool.rs`

- [ ] **Step 1: Write pool with DashMap backing**

Write `crates/fan-out-bench/src/pool.rs`:

```rust
//! Pre-signed tx pool keyed by (slot, tick, sender_id).

use dashmap::DashMap;
use solana_sdk::transaction::Transaction;
use std::sync::Arc;
use std::time::Instant;

#[derive(Clone)]
pub struct PreSignedTx {
    pub tx: Arc<Transaction>,
    pub message_hash: [u8; 32],
    pub prepared_at: Instant,
    pub pool_ready_at: Instant,
}

#[derive(Default)]
pub struct TxPool {
    map: DashMap<(u64, u8, u8), PreSignedTx>,
}

impl TxPool {
    pub fn new() -> Self {
        Self { map: DashMap::with_capacity(8192) }
    }

    /// Insert a pre-signed tx. Returns true if key was previously empty.
    pub fn insert(&self, slot: u64, tick: u8, sender_id: u8, tx: PreSignedTx) -> bool {
        self.map.insert((slot, tick, sender_id), tx).is_none()
    }

    /// Take a single variant for (slot, tick, sender_id), removing it.
    pub fn take(&self, slot: u64, tick: u8, sender_id: u8) -> Option<PreSignedTx> {
        self.map.remove(&(slot, tick, sender_id)).map(|(_, v)| v)
    }

    /// Take ALL variants for (slot, tick), removing them. Returns (sender_id, tx) pairs.
    pub fn take_all_for(&self, slot: u64, tick: u8) -> Vec<(u8, PreSignedTx)> {
        // Two-pass: collect matching keys, then remove
        let keys: Vec<(u64, u8, u8)> = self.map
            .iter()
            .filter(|e| e.key().0 == slot && e.key().1 == tick)
            .map(|e| *e.key())
            .collect();
        keys.into_iter()
            .filter_map(|k| self.map.remove(&k).map(|(key, v)| (key.2, v)))
            .collect()
    }

    /// Prune entries with slot < cutoff_slot.
    pub fn prune_older_than(&self, cutoff_slot: u64) -> usize {
        let stale: Vec<_> = self.map
            .iter()
            .filter(|e| e.key().0 < cutoff_slot)
            .map(|e| *e.key())
            .collect();
        let count = stale.len();
        for k in stale {
            self.map.remove(&k);
        }
        count
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::transaction::Transaction;

    fn fake_tx() -> PreSignedTx {
        PreSignedTx {
            tx: Arc::new(Transaction::default()),
            message_hash: [0; 32],
            prepared_at: Instant::now(),
            pool_ready_at: Instant::now(),
        }
    }

    #[test]
    fn insert_and_take_single() {
        let pool = TxPool::new();
        assert!(pool.insert(100, 5, 0, fake_tx()));
        assert_eq!(pool.len(), 1);
        let taken = pool.take(100, 5, 0);
        assert!(taken.is_some());
        assert!(pool.is_empty());
    }

    #[test]
    fn insert_twice_returns_false() {
        let pool = TxPool::new();
        assert!(pool.insert(100, 5, 0, fake_tx()));
        assert!(!pool.insert(100, 5, 0, fake_tx())); // overwrite
    }

    #[test]
    fn take_missing_returns_none() {
        let pool = TxPool::new();
        assert!(pool.take(100, 5, 0).is_none());
    }

    #[test]
    fn take_all_for_returns_all_sender_variants() {
        let pool = TxPool::new();
        pool.insert(100, 5, 0, fake_tx());
        pool.insert(100, 5, 1, fake_tx());
        pool.insert(100, 5, 2, fake_tx());
        pool.insert(101, 5, 0, fake_tx()); // different slot, shouldn't be taken
        let taken = pool.take_all_for(100, 5);
        assert_eq!(taken.len(), 3);
        let mut ids: Vec<u8> = taken.iter().map(|(id, _)| *id).collect();
        ids.sort();
        assert_eq!(ids, vec![0, 1, 2]);
        assert_eq!(pool.len(), 1); // (101,5,0) remains
    }

    #[test]
    fn prune_removes_old_slots() {
        let pool = TxPool::new();
        pool.insert(100, 5, 0, fake_tx());
        pool.insert(200, 5, 0, fake_tx());
        pool.insert(300, 5, 0, fake_tx());
        let pruned = pool.prune_older_than(250);
        assert_eq!(pruned, 2);
        assert_eq!(pool.len(), 1);
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p fan-out-bench --lib pool`
Expected: 5 tests pass.

- [ ] **Step 3: Commit**

```bash
git add crates/fan-out-bench/src/pool.rs
git commit -m "feat(fan-out-bench): add DashMap-backed pre-signed tx pool"
```

---

## Task 9: TxSender trait + MockSender

**Files:**
- Create: `crates/fan-out-bench/src/senders/mod.rs`
- Create: `crates/fan-out-bench/src/senders/mock.rs`

- [ ] **Step 1: Define trait and types**

Write `crates/fan-out-bench/src/senders/mod.rs`:

```rust
//! TxSender trait — uniform contract for all send mechanisms.

pub mod mock;

use crate::outcome::RateLimitState;
use solana_sdk::{signature::Signature, transaction::Transaction};
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct SendOutcome {
    pub send_at: Instant,
    pub send_ack_at: Option<Instant>,
    pub signature: Signature,
    pub provider_request_id: Option<String>,
    pub http_status: Option<u16>,
    pub rpc_err_code: Option<i32>,
    pub rpc_err_message: Option<String>,
    pub rate_limit_state: RateLimitState,
    pub error: Option<String>,
}

#[async_trait::async_trait]
pub trait TxSender: Send + Sync {
    fn id(&self) -> u8;
    fn name(&self) -> &str;
    fn endpoint_url(&self) -> &str;
    fn protocol(&self) -> &'static str;
    async fn send(&self, tx: &Transaction) -> SendOutcome;
}
```

Add `async-trait = "0.1"` to `crates/fan-out-bench/Cargo.toml` `[dependencies]`.

- [ ] **Step 2: Implement MockSender**

Write `crates/fan-out-bench/src/senders/mock.rs`:

```rust
//! MockSender — for tests and end-to-end mock pipeline.
//!
//! Configurable to always ack, always error, or pseudo-random based on signature.

use super::{SendOutcome, TxSender};
use crate::outcome::RateLimitState;
use solana_sdk::{signature::Signature, transaction::Transaction};
use std::time::{Duration, Instant};

pub struct MockSender {
    id: u8,
    name: String,
    endpoint_url: String,
    pub ack_delay: Duration,
    pub mode: MockMode,
}

#[derive(Clone)]
pub enum MockMode {
    AlwaysAck,
    AlwaysError(String),
    AckHalfRandom { seed: u64 },
}

impl MockSender {
    pub fn always_ack(id: u8, name: impl Into<String>) -> Self {
        Self {
            id,
            name: name.into(),
            endpoint_url: "mock://always-ack".into(),
            ack_delay: Duration::from_millis(1),
            mode: MockMode::AlwaysAck,
        }
    }

    pub fn always_error(id: u8, name: impl Into<String>, err: impl Into<String>) -> Self {
        let err = err.into();
        Self {
            id,
            name: name.into(),
            endpoint_url: "mock://error".into(),
            ack_delay: Duration::from_millis(1),
            mode: MockMode::AlwaysError(err),
        }
    }
}

#[async_trait::async_trait]
impl TxSender for MockSender {
    fn id(&self) -> u8 { self.id }
    fn name(&self) -> &str { &self.name }
    fn endpoint_url(&self) -> &str { &self.endpoint_url }
    fn protocol(&self) -> &'static str { "MOCK" }

    async fn send(&self, tx: &Transaction) -> SendOutcome {
        let send_at = Instant::now();
        tokio::time::sleep(self.ack_delay).await;
        let send_ack_at = Some(Instant::now());
        let signature = tx.signatures.first().copied().unwrap_or_default();
        match &self.mode {
            MockMode::AlwaysAck => SendOutcome {
                send_at, send_ack_at, signature,
                provider_request_id: Some(format!("mock-{}-{}", self.name, signature)),
                http_status: Some(200),
                rpc_err_code: None,
                rpc_err_message: None,
                rate_limit_state: RateLimitState::Ok,
                error: None,
            },
            MockMode::AlwaysError(msg) => SendOutcome {
                send_at, send_ack_at: None, signature,
                provider_request_id: None,
                http_status: Some(500),
                rpc_err_code: Some(-32000),
                rpc_err_message: Some(msg.clone()),
                rate_limit_state: RateLimitState::Ok,
                error: Some(msg.clone()),
            },
            MockMode::AckHalfRandom { seed } => {
                let h = (signature.as_ref()[0] as u64) ^ seed;
                if h % 2 == 0 {
                    SendOutcome {
                        send_at, send_ack_at, signature,
                        provider_request_id: None,
                        http_status: Some(200),
                        rpc_err_code: None,
                        rpc_err_message: None,
                        rate_limit_state: RateLimitState::Ok,
                        error: None,
                    }
                } else {
                    SendOutcome {
                        send_at, send_ack_at: None, signature,
                        provider_request_id: None,
                        http_status: Some(429),
                        rpc_err_code: None,
                        rpc_err_message: Some("mock rate limited".into()),
                        rate_limit_state: RateLimitState::Throttled429,
                        error: Some("mock rate limited".into()),
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::transaction::Transaction;

    #[tokio::test]
    async fn always_ack_returns_ok() {
        let sender = MockSender::always_ack(0, "mock");
        let outcome = sender.send(&Transaction::default()).await;
        assert!(outcome.error.is_none());
        assert!(outcome.send_ack_at.is_some());
        assert_eq!(outcome.http_status, Some(200));
    }

    #[tokio::test]
    async fn always_error_returns_err() {
        let sender = MockSender::always_error(0, "mock", "boom");
        let outcome = sender.send(&Transaction::default()).await;
        assert_eq!(outcome.error.as_deref(), Some("boom"));
        assert!(outcome.send_ack_at.is_none());
    }
}
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p fan-out-bench --lib senders`
Expected: 2 tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/fan-out-bench/src/senders/ crates/fan-out-bench/Cargo.toml
git commit -m "feat(fan-out-bench): add TxSender trait + MockSender for tests"
```

---

## Task 10: BenchCounters

**Files:**
- Create: `crates/fan-out-bench/src/counters.rs`

- [ ] **Step 1: Write counters with atomic fields**

Write `crates/fan-out-bench/src/counters.rs`:

```rust
//! Atomic counters for bench telemetry.

use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
pub struct BenchCounters {
    pub pool_empty: AtomicU64,
    pub pool_overwrite: AtomicU64,
    pub send_queue_full: AtomicU64,
    pub match_queue_full: AtomicU64,
    pub finality_queue_full: AtomicU64,
    pub fallback_queue_full: AtomicU64,
    pub send_event_queue_full: AtomicU64,
    pub final_queue_full: AtomicU64,
    pub tick_event_queue_full: AtomicU64,
    pub send_http_error: AtomicU64,
    pub send_network_error: AtomicU64,
    pub send_throttled_429: AtomicU64,
    pub blockhash_expired: AtomicU64,
    pub preparer_blockhash_fail: AtomicU64,
    pub preparer_signing_fail: AtomicU64,
    pub fork_tick_overflow: AtomicU64,
    pub nonce_stalls: AtomicU64,
    pub nonce_advance_observed: AtomicU64,
    pub schedule_contains_calls: AtomicU64,
    pub schedule_contains_true: AtomicU64,
    pub rpc_fallback_error: AtomicU64,
    pub rpc_fallback_recovered_landed: AtomicU64,
    pub rpc_fallback_confirmed_missing: AtomicU64,
    pub finality_confirmed: AtomicU64,
    pub finality_reorged_out: AtomicU64,
    pub finality_uncertain: AtomicU64,
}

#[derive(Debug, Default, Serialize)]
pub struct CountersSnapshot {
    pub pool_empty: u64,
    pub pool_overwrite: u64,
    pub send_queue_full: u64,
    pub match_queue_full: u64,
    pub finality_queue_full: u64,
    pub fallback_queue_full: u64,
    pub send_event_queue_full: u64,
    pub final_queue_full: u64,
    pub tick_event_queue_full: u64,
    pub send_http_error: u64,
    pub send_network_error: u64,
    pub send_throttled_429: u64,
    pub blockhash_expired: u64,
    pub preparer_blockhash_fail: u64,
    pub preparer_signing_fail: u64,
    pub fork_tick_overflow: u64,
    pub nonce_stalls: u64,
    pub nonce_advance_observed: u64,
    pub schedule_contains_calls: u64,
    pub schedule_contains_true: u64,
    pub rpc_fallback_error: u64,
    pub rpc_fallback_recovered_landed: u64,
    pub rpc_fallback_confirmed_missing: u64,
    pub finality_confirmed: u64,
    pub finality_reorged_out: u64,
    pub finality_uncertain: u64,
}

impl BenchCounters {
    pub fn snapshot(&self) -> CountersSnapshot {
        let l = |c: &AtomicU64| c.load(Ordering::Relaxed);
        CountersSnapshot {
            pool_empty: l(&self.pool_empty),
            pool_overwrite: l(&self.pool_overwrite),
            send_queue_full: l(&self.send_queue_full),
            match_queue_full: l(&self.match_queue_full),
            finality_queue_full: l(&self.finality_queue_full),
            fallback_queue_full: l(&self.fallback_queue_full),
            send_event_queue_full: l(&self.send_event_queue_full),
            final_queue_full: l(&self.final_queue_full),
            tick_event_queue_full: l(&self.tick_event_queue_full),
            send_http_error: l(&self.send_http_error),
            send_network_error: l(&self.send_network_error),
            send_throttled_429: l(&self.send_throttled_429),
            blockhash_expired: l(&self.blockhash_expired),
            preparer_blockhash_fail: l(&self.preparer_blockhash_fail),
            preparer_signing_fail: l(&self.preparer_signing_fail),
            fork_tick_overflow: l(&self.fork_tick_overflow),
            nonce_stalls: l(&self.nonce_stalls),
            nonce_advance_observed: l(&self.nonce_advance_observed),
            schedule_contains_calls: l(&self.schedule_contains_calls),
            schedule_contains_true: l(&self.schedule_contains_true),
            rpc_fallback_error: l(&self.rpc_fallback_error),
            rpc_fallback_recovered_landed: l(&self.rpc_fallback_recovered_landed),
            rpc_fallback_confirmed_missing: l(&self.rpc_fallback_confirmed_missing),
            finality_confirmed: l(&self.finality_confirmed),
            finality_reorged_out: l(&self.finality_reorged_out),
            finality_uncertain: l(&self.finality_uncertain),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_serializes_to_json() {
        let counters = BenchCounters::default();
        counters.pool_empty.fetch_add(5, Ordering::Relaxed);
        let snap = counters.snapshot();
        let json = serde_json::to_string(&snap).unwrap();
        assert!(json.contains("\"pool_empty\":5"));
    }

    #[test]
    fn snapshot_independent_of_counter_state() {
        let counters = BenchCounters::default();
        let s1 = counters.snapshot();
        counters.pool_empty.fetch_add(10, Ordering::Relaxed);
        let s2 = counters.snapshot();
        assert_eq!(s1.pool_empty, 0);
        assert_eq!(s2.pool_empty, 10);
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p fan-out-bench --lib counters`
Expected: 2 tests pass.

- [ ] **Step 3: Commit**

```bash
git add crates/fan-out-bench/src/counters.rs
git commit -m "feat(fan-out-bench): add atomic BenchCounters with serializable snapshot"
```

---

## Task 11: Parquet schema

**Files:**
- Create: `crates/fan-out-bench/src/writer/mod.rs`
- Create: `crates/fan-out-bench/src/writer/schema.rs`

- [ ] **Step 1: Write writer/mod.rs as re-export hub**

Write `crates/fan-out-bench/src/writer/mod.rs`:

```rust
//! Parquet output sink + Arrow schema.

pub mod record;
pub mod schema;
pub mod parquet_sink;

pub use record::FinalRecord;
pub use schema::final_record_schema;
pub use parquet_sink::{ParquetWriterConfig, spawn_parquet};
```

- [ ] **Step 2: Write Arrow schema definition**

Write `crates/fan-out-bench/src/writer/schema.rs`. Schema corresponds to spec §6.1 column list:

```rust
//! Arrow schema for tx-events.parquet.
//!
//! Column list and types match spec §6.1.

use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;

pub fn final_record_schema() -> Arc<Schema> {
    let fields = vec![
        // Trigger / variant identification
        Field::new("trigger_slot", DataType::UInt64, false),
        Field::new("trigger_tick", DataType::UInt8, false),
        Field::new("trigger_id", DataType::FixedSizeBinary(16), false),
        Field::new("nonce_account_id", DataType::UInt16, false),
        Field::new("nonce_blockhash_used", DataType::FixedSizeBinary(32), false),
        Field::new("sender_id", DataType::UInt8, false),
        Field::new("sender_name", DataType::Utf8, false),
        Field::new("tx_signature", DataType::FixedSizeBinary(64), false),
        Field::new("tx_message_hash", DataType::FixedSizeBinary(32), false),

        // Sender config snapshot
        Field::new("endpoint_url", DataType::Utf8, false),
        Field::new("protocol", DataType::Utf8, false),
        Field::new("auth_tier", DataType::Utf8, true),
        Field::new("tip_account_used", DataType::FixedSizeBinary(32), true),
        Field::new("tip_lamports", DataType::UInt64, false),
        Field::new("priority_fee_microlamports", DataType::UInt64, false),
        Field::new("compute_unit_limit", DataType::UInt32, false),

        // Timestamps
        Field::new("prepared_at_ns", DataType::UInt64, false),
        Field::new("pool_ready_at_ns", DataType::UInt64, false),
        Field::new("trigger_observed_at_ns", DataType::UInt64, false),
        Field::new("send_at_ns", DataType::UInt64, false),
        Field::new("send_ack_at_ns", DataType::UInt64, true),
        Field::new("send_order_in_trigger", DataType::UInt8, false),
        Field::new("host_clock_offset_ns", DataType::Int64, true),

        // Send outcome (transport)
        Field::new("send_error", DataType::Utf8, true),
        Field::new("rpc_err_code", DataType::Int32, true),
        Field::new("rpc_err_message", DataType::Utf8, true),
        Field::new("provider_request_id", DataType::Utf8, true),
        Field::new("http_status", DataType::UInt16, true),
        Field::new("rate_limit_state", DataType::Utf8, false),

        // Observation
        Field::new("observed_slot", DataType::UInt64, true),
        Field::new("observed_entry_index", DataType::UInt32, true),
        Field::new("observed_tick_in_slot", DataType::UInt8, true),
        Field::new("observed_cumulative_hashes_in_slot", DataType::UInt64, true),
        Field::new("ss_observed_at_ns", DataType::UInt64, true),
        Field::new("ys_observed_at_ns", DataType::UInt64, true),
        Field::new("observed_at_ns", DataType::UInt64, true),
        Field::new("observed_source", DataType::Utf8, true),
        Field::new("commitment_at_resolution", DataType::Utf8, true),

        // Outcome
        Field::new("tentative_outcome", DataType::Utf8, false),
        Field::new("final_status", DataType::Utf8, false),
        Field::new("siblings_resolved_at_ns", DataType::UInt64, true),

        // Leader context
        Field::new("leader_pubkey", DataType::FixedSizeBinary(32), true),
        Field::new("leader_region_cc", DataType::Utf8, true),
        Field::new("leader_dc_label", DataType::Utf8, true),
        Field::new("leader_continent", DataType::Utf8, true),
        Field::new("leader_stake_lamports", DataType::UInt64, true),
        Field::new("validator_client", DataType::Utf8, true),

        // Deltas
        Field::new("tick_delta", DataType::Int32, true),
        Field::new("hash_delta", DataType::Int64, true),
        Field::new("slot_delta", DataType::Int32, true),
        Field::new("leader_changed", DataType::Boolean, false),
        Field::new("wall_trigger_to_send_ns", DataType::Int64, true),
        Field::new("wall_send_rtt_ns", DataType::Int64, true),
        Field::new("wall_send_to_observed_ns", DataType::Int64, true),
        Field::new("wall_send_to_ss_observed_ns", DataType::Int64, true),
        Field::new("wall_send_to_ys_observed_ns", DataType::Int64, true),

        // Nonce context
        Field::new("nonce_update_observed_at_ns", DataType::UInt64, true),
        Field::new("nonce_update_source", DataType::Utf8, true),
        Field::new("nonce_advanced_to_slot", DataType::UInt64, true),

        // Run metadata
        Field::new("run_id", DataType::Utf8, false),
        Field::new("chunk_index", DataType::UInt32, false),
    ];
    Arc::new(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_field_count_matches_spec() {
        let schema = final_record_schema();
        // Spec §6.1: 9 identification + 7 config + 7 timestamps + 6 send_outcome
        // + 9 observation + 3 outcome + 6 leader + 9 deltas + 3 nonce + 2 run = 61
        assert_eq!(schema.fields().len(), 61);
    }

    #[test]
    fn no_duplicate_field_names() {
        let schema = final_record_schema();
        let names: std::collections::HashSet<_> = schema.fields().iter().map(|f| f.name().clone()).collect();
        assert_eq!(names.len(), schema.fields().len());
    }
}
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p fan-out-bench --lib writer::schema`
Expected: 2 tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/fan-out-bench/src/writer/
git commit -m "feat(fan-out-bench): add Arrow schema definition (61 columns)"
```

---

## Task 12: FinalRecord struct

**Files:**
- Create: `crates/fan-out-bench/src/writer/record.rs`

- [ ] **Step 1: Write FinalRecord with all fields matching schema**

Write `crates/fan-out-bench/src/writer/record.rs`:

```rust
//! FinalRecord — in-memory representation of one parquet row.

use crate::outcome::{CommitmentAtResolution, FinalStatus, ObservedSource, RateLimitState, TentativeOutcome};
use solana_sdk::{hash::Hash, pubkey::Pubkey, signature::Signature};

#[derive(Debug, Clone)]
pub struct FinalRecord {
    // identification
    pub trigger_slot: u64,
    pub trigger_tick: u8,
    pub trigger_id: [u8; 16],
    pub nonce_account_id: u16,
    pub nonce_blockhash_used: Hash,
    pub sender_id: u8,
    pub sender_name: String,
    pub tx_signature: Signature,
    pub tx_message_hash: [u8; 32],

    // sender config snapshot
    pub endpoint_url: String,
    pub protocol: String,
    pub auth_tier: Option<String>,
    pub tip_account_used: Option<Pubkey>,
    pub tip_lamports: u64,
    pub priority_fee_microlamports: u64,
    pub compute_unit_limit: u32,

    // timestamps
    pub prepared_at_ns: u64,
    pub pool_ready_at_ns: u64,
    pub trigger_observed_at_ns: u64,
    pub send_at_ns: u64,
    pub send_ack_at_ns: Option<u64>,
    pub send_order_in_trigger: u8,
    pub host_clock_offset_ns: Option<i64>,

    // send outcome
    pub send_error: Option<String>,
    pub rpc_err_code: Option<i32>,
    pub rpc_err_message: Option<String>,
    pub provider_request_id: Option<String>,
    pub http_status: Option<u16>,
    pub rate_limit_state: RateLimitState,

    // observation
    pub observed_slot: Option<u64>,
    pub observed_entry_index: Option<u32>,
    pub observed_tick_in_slot: Option<u8>,
    pub observed_cumulative_hashes_in_slot: Option<u64>,
    pub ss_observed_at_ns: Option<u64>,
    pub ys_observed_at_ns: Option<u64>,
    pub observed_at_ns: Option<u64>,
    pub observed_source: Option<ObservedSource>,
    pub commitment_at_resolution: Option<CommitmentAtResolution>,

    // outcome
    pub tentative_outcome: TentativeOutcome,
    pub final_status: FinalStatus,
    pub siblings_resolved_at_ns: Option<u64>,

    // leader
    pub leader_pubkey: Option<Pubkey>,
    pub leader_region_cc: Option<String>,
    pub leader_dc_label: Option<String>,
    pub leader_continent: Option<String>,
    pub leader_stake_lamports: Option<u64>,
    pub validator_client: Option<String>,

    // deltas
    pub tick_delta: Option<i32>,
    pub hash_delta: Option<i64>,
    pub slot_delta: Option<i32>,
    pub leader_changed: bool,
    pub wall_trigger_to_send_ns: Option<i64>,
    pub wall_send_rtt_ns: Option<i64>,
    pub wall_send_to_observed_ns: Option<i64>,
    pub wall_send_to_ss_observed_ns: Option<i64>,
    pub wall_send_to_ys_observed_ns: Option<i64>,

    // nonce
    pub nonce_update_observed_at_ns: Option<u64>,
    pub nonce_update_source: Option<String>,
    pub nonce_advanced_to_slot: Option<u64>,

    // run metadata
    pub run_id: String,
    pub chunk_index: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn final_record_can_be_constructed() {
        let r = FinalRecord {
            trigger_slot: 100,
            trigger_tick: 5,
            trigger_id: [0; 16],
            nonce_account_id: 0,
            nonce_blockhash_used: Hash::default(),
            sender_id: 1,
            sender_name: "mock".into(),
            tx_signature: Signature::default(),
            tx_message_hash: [0; 32],
            endpoint_url: "mock://x".into(),
            protocol: "MOCK".into(),
            auth_tier: None,
            tip_account_used: None,
            tip_lamports: 1000,
            priority_fee_microlamports: 5000,
            compute_unit_limit: 200_000,
            prepared_at_ns: 1,
            pool_ready_at_ns: 2,
            trigger_observed_at_ns: 3,
            send_at_ns: 4,
            send_ack_at_ns: Some(5),
            send_order_in_trigger: 0,
            host_clock_offset_ns: None,
            send_error: None,
            rpc_err_code: None,
            rpc_err_message: None,
            provider_request_id: None,
            http_status: Some(200),
            rate_limit_state: RateLimitState::Ok,
            observed_slot: None,
            observed_entry_index: None,
            observed_tick_in_slot: None,
            observed_cumulative_hashes_in_slot: None,
            ss_observed_at_ns: None,
            ys_observed_at_ns: None,
            observed_at_ns: None,
            observed_source: None,
            commitment_at_resolution: None,
            tentative_outcome: TentativeOutcome::SendError,
            final_status: FinalStatus::Pending,
            siblings_resolved_at_ns: None,
            leader_pubkey: None,
            leader_region_cc: None,
            leader_dc_label: None,
            leader_continent: None,
            leader_stake_lamports: None,
            validator_client: None,
            tick_delta: None,
            hash_delta: None,
            slot_delta: None,
            leader_changed: false,
            wall_trigger_to_send_ns: None,
            wall_send_rtt_ns: None,
            wall_send_to_observed_ns: None,
            wall_send_to_ss_observed_ns: None,
            wall_send_to_ys_observed_ns: None,
            nonce_update_observed_at_ns: None,
            nonce_update_source: None,
            nonce_advanced_to_slot: None,
            run_id: "test".into(),
            chunk_index: 0,
        };
        assert_eq!(r.trigger_slot, 100);
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p fan-out-bench --lib writer::record`
Expected: 1 test passes.

- [ ] **Step 3: Commit**

```bash
git add crates/fan-out-bench/src/writer/record.rs
git commit -m "feat(fan-out-bench): add FinalRecord struct matching schema"
```

---

## Task 13: Parquet sink (background thread)

**Files:**
- Create: `crates/fan-out-bench/src/writer/parquet_sink.rs`

- [ ] **Step 1: Write parquet sink that consumes FinalRecord stream**

Write `crates/fan-out-bench/src/writer/parquet_sink.rs`:

```rust
//! Background-thread Parquet writer.
//!
//! Consumes FinalRecord from a crossbeam channel, batches into row groups,
//! flushes to disk as records accumulate.

use crate::counters::BenchCounters;
use crate::outcome::{FinalStatus, ObservedSource, RateLimitState, TentativeOutcome};
use crate::writer::record::FinalRecord;
use crate::writer::schema::final_record_schema;
use arrow_array::{
    builder::*, ArrayRef, RecordBatch,
};
use arrow_schema::Schema;
use crossbeam_channel::Receiver;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

pub struct ParquetWriterConfig {
    pub final_rx: Receiver<FinalRecord>,
    pub output_path: PathBuf,
    pub row_group_size: usize,
    pub flush_interval: Duration,
    pub pinned_core: Option<usize>,
    pub counters: Arc<BenchCounters>,
}

pub fn spawn_parquet(cfg: ParquetWriterConfig) -> std::io::Result<JoinHandle<()>> {
    std::thread::Builder::new()
        .name("parquet-writer".into())
        .spawn(move || {
            if let Some(core) = cfg.pinned_core {
                core_affinity::set_for_current(core_affinity::CoreId { id: core });
            }
            run_loop(cfg).unwrap_or_else(|e| {
                tracing::error!(error = %e, "parquet writer terminated with error");
            });
        })
}

fn run_loop(cfg: ParquetWriterConfig) -> anyhow::Result<()> {
    let schema = final_record_schema();
    let file = File::create(&cfg.output_path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

    let mut buffer: Vec<FinalRecord> = Vec::with_capacity(cfg.row_group_size);
    let mut last_flush = std::time::Instant::now();

    loop {
        match cfg.final_rx.recv_timeout(Duration::from_millis(500)) {
            Ok(rec) => {
                buffer.push(rec);
                if buffer.len() >= cfg.row_group_size {
                    flush_buffer(&mut writer, &schema, &mut buffer)?;
                    last_flush = std::time::Instant::now();
                }
            }
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                if !buffer.is_empty() && last_flush.elapsed() >= cfg.flush_interval {
                    flush_buffer(&mut writer, &schema, &mut buffer)?;
                    last_flush = std::time::Instant::now();
                }
            }
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                // drain remaining and exit
                if !buffer.is_empty() {
                    flush_buffer(&mut writer, &schema, &mut buffer)?;
                }
                break;
            }
        }
    }

    writer.close()?;
    tracing::info!(path = ?cfg.output_path, "parquet writer closed cleanly");
    Ok(())
}

fn flush_buffer(
    writer: &mut ArrowWriter<File>,
    schema: &Schema,
    buffer: &mut Vec<FinalRecord>,
) -> anyhow::Result<()> {
    if buffer.is_empty() {
        return Ok(());
    }
    let batch = records_to_batch(schema, buffer)?;
    writer.write(&batch)?;
    buffer.clear();
    Ok(())
}

fn records_to_batch(schema: &Schema, records: &[FinalRecord]) -> anyhow::Result<RecordBatch> {
    let n = records.len();
    let mut b_trigger_slot = UInt64Builder::with_capacity(n);
    let mut b_trigger_tick = UInt8Builder::with_capacity(n);
    let mut b_trigger_id = FixedSizeBinaryBuilder::with_capacity(n, 16);
    let mut b_nonce_account_id = UInt16Builder::with_capacity(n);
    let mut b_nonce_blockhash_used = FixedSizeBinaryBuilder::with_capacity(n, 32);
    let mut b_sender_id = UInt8Builder::with_capacity(n);
    let mut b_sender_name = StringBuilder::with_capacity(n, n * 8);
    let mut b_tx_signature = FixedSizeBinaryBuilder::with_capacity(n, 64);
    let mut b_tx_message_hash = FixedSizeBinaryBuilder::with_capacity(n, 32);

    let mut b_endpoint_url = StringBuilder::with_capacity(n, n * 32);
    let mut b_protocol = StringBuilder::with_capacity(n, n * 8);
    let mut b_auth_tier = StringBuilder::with_capacity(n, n * 8);
    let mut b_tip_account_used = FixedSizeBinaryBuilder::with_capacity(n, 32);
    let mut b_tip_lamports = UInt64Builder::with_capacity(n);
    let mut b_priority_fee_microlamports = UInt64Builder::with_capacity(n);
    let mut b_compute_unit_limit = UInt32Builder::with_capacity(n);

    let mut b_prepared_at_ns = UInt64Builder::with_capacity(n);
    let mut b_pool_ready_at_ns = UInt64Builder::with_capacity(n);
    let mut b_trigger_observed_at_ns = UInt64Builder::with_capacity(n);
    let mut b_send_at_ns = UInt64Builder::with_capacity(n);
    let mut b_send_ack_at_ns = UInt64Builder::with_capacity(n);
    let mut b_send_order_in_trigger = UInt8Builder::with_capacity(n);
    let mut b_host_clock_offset_ns = Int64Builder::with_capacity(n);

    let mut b_send_error = StringBuilder::with_capacity(n, n * 16);
    let mut b_rpc_err_code = Int32Builder::with_capacity(n);
    let mut b_rpc_err_message = StringBuilder::with_capacity(n, n * 16);
    let mut b_provider_request_id = StringBuilder::with_capacity(n, n * 16);
    let mut b_http_status = UInt16Builder::with_capacity(n);
    let mut b_rate_limit_state = StringBuilder::with_capacity(n, n * 8);

    let mut b_observed_slot = UInt64Builder::with_capacity(n);
    let mut b_observed_entry_index = UInt32Builder::with_capacity(n);
    let mut b_observed_tick_in_slot = UInt8Builder::with_capacity(n);
    let mut b_observed_cumulative_hashes_in_slot = UInt64Builder::with_capacity(n);
    let mut b_ss_observed_at_ns = UInt64Builder::with_capacity(n);
    let mut b_ys_observed_at_ns = UInt64Builder::with_capacity(n);
    let mut b_observed_at_ns = UInt64Builder::with_capacity(n);
    let mut b_observed_source = StringBuilder::with_capacity(n, n * 4);
    let mut b_commitment_at_resolution = StringBuilder::with_capacity(n, n * 8);

    let mut b_tentative_outcome = StringBuilder::with_capacity(n, n * 16);
    let mut b_final_status = StringBuilder::with_capacity(n, n * 8);
    let mut b_siblings_resolved_at_ns = UInt64Builder::with_capacity(n);

    let mut b_leader_pubkey = FixedSizeBinaryBuilder::with_capacity(n, 32);
    let mut b_leader_region_cc = StringBuilder::with_capacity(n, n * 2);
    let mut b_leader_dc_label = StringBuilder::with_capacity(n, n * 16);
    let mut b_leader_continent = StringBuilder::with_capacity(n, n * 8);
    let mut b_leader_stake_lamports = UInt64Builder::with_capacity(n);
    let mut b_validator_client = StringBuilder::with_capacity(n, n * 16);

    let mut b_tick_delta = Int32Builder::with_capacity(n);
    let mut b_hash_delta = Int64Builder::with_capacity(n);
    let mut b_slot_delta = Int32Builder::with_capacity(n);
    let mut b_leader_changed = BooleanBuilder::with_capacity(n);
    let mut b_wall_trigger_to_send_ns = Int64Builder::with_capacity(n);
    let mut b_wall_send_rtt_ns = Int64Builder::with_capacity(n);
    let mut b_wall_send_to_observed_ns = Int64Builder::with_capacity(n);
    let mut b_wall_send_to_ss_observed_ns = Int64Builder::with_capacity(n);
    let mut b_wall_send_to_ys_observed_ns = Int64Builder::with_capacity(n);

    let mut b_nonce_update_observed_at_ns = UInt64Builder::with_capacity(n);
    let mut b_nonce_update_source = StringBuilder::with_capacity(n, n * 4);
    let mut b_nonce_advanced_to_slot = UInt64Builder::with_capacity(n);

    let mut b_run_id = StringBuilder::with_capacity(n, n * 16);
    let mut b_chunk_index = UInt32Builder::with_capacity(n);

    for r in records {
        b_trigger_slot.append_value(r.trigger_slot);
        b_trigger_tick.append_value(r.trigger_tick);
        b_trigger_id.append_value(&r.trigger_id)?;
        b_nonce_account_id.append_value(r.nonce_account_id);
        b_nonce_blockhash_used.append_value(r.nonce_blockhash_used.as_ref())?;
        b_sender_id.append_value(r.sender_id);
        b_sender_name.append_value(&r.sender_name);
        b_tx_signature.append_value(r.tx_signature.as_ref())?;
        b_tx_message_hash.append_value(&r.tx_message_hash)?;

        b_endpoint_url.append_value(&r.endpoint_url);
        b_protocol.append_value(&r.protocol);
        match &r.auth_tier { Some(s) => b_auth_tier.append_value(s), None => b_auth_tier.append_null() }
        match &r.tip_account_used { Some(p) => b_tip_account_used.append_value(p.as_ref())?, None => b_tip_account_used.append_null() }
        b_tip_lamports.append_value(r.tip_lamports);
        b_priority_fee_microlamports.append_value(r.priority_fee_microlamports);
        b_compute_unit_limit.append_value(r.compute_unit_limit);

        b_prepared_at_ns.append_value(r.prepared_at_ns);
        b_pool_ready_at_ns.append_value(r.pool_ready_at_ns);
        b_trigger_observed_at_ns.append_value(r.trigger_observed_at_ns);
        b_send_at_ns.append_value(r.send_at_ns);
        match r.send_ack_at_ns { Some(v) => b_send_ack_at_ns.append_value(v), None => b_send_ack_at_ns.append_null() }
        b_send_order_in_trigger.append_value(r.send_order_in_trigger);
        match r.host_clock_offset_ns { Some(v) => b_host_clock_offset_ns.append_value(v), None => b_host_clock_offset_ns.append_null() }

        match &r.send_error { Some(s) => b_send_error.append_value(s), None => b_send_error.append_null() }
        match r.rpc_err_code { Some(v) => b_rpc_err_code.append_value(v), None => b_rpc_err_code.append_null() }
        match &r.rpc_err_message { Some(s) => b_rpc_err_message.append_value(s), None => b_rpc_err_message.append_null() }
        match &r.provider_request_id { Some(s) => b_provider_request_id.append_value(s), None => b_provider_request_id.append_null() }
        match r.http_status { Some(v) => b_http_status.append_value(v), None => b_http_status.append_null() }
        b_rate_limit_state.append_value(rate_limit_state_str(r.rate_limit_state));

        match r.observed_slot { Some(v) => b_observed_slot.append_value(v), None => b_observed_slot.append_null() }
        match r.observed_entry_index { Some(v) => b_observed_entry_index.append_value(v), None => b_observed_entry_index.append_null() }
        match r.observed_tick_in_slot { Some(v) => b_observed_tick_in_slot.append_value(v), None => b_observed_tick_in_slot.append_null() }
        match r.observed_cumulative_hashes_in_slot { Some(v) => b_observed_cumulative_hashes_in_slot.append_value(v), None => b_observed_cumulative_hashes_in_slot.append_null() }
        match r.ss_observed_at_ns { Some(v) => b_ss_observed_at_ns.append_value(v), None => b_ss_observed_at_ns.append_null() }
        match r.ys_observed_at_ns { Some(v) => b_ys_observed_at_ns.append_value(v), None => b_ys_observed_at_ns.append_null() }
        match r.observed_at_ns { Some(v) => b_observed_at_ns.append_value(v), None => b_observed_at_ns.append_null() }
        match r.observed_source { Some(s) => b_observed_source.append_value(observed_source_str(s)), None => b_observed_source.append_null() }
        match r.commitment_at_resolution { Some(c) => b_commitment_at_resolution.append_value(commitment_str(c)), None => b_commitment_at_resolution.append_null() }

        b_tentative_outcome.append_value(r.tentative_outcome.as_str());
        b_final_status.append_value(r.final_status.as_str());
        match r.siblings_resolved_at_ns { Some(v) => b_siblings_resolved_at_ns.append_value(v), None => b_siblings_resolved_at_ns.append_null() }

        match r.leader_pubkey { Some(p) => b_leader_pubkey.append_value(p.as_ref())?, None => b_leader_pubkey.append_null() }
        match &r.leader_region_cc { Some(s) => b_leader_region_cc.append_value(s), None => b_leader_region_cc.append_null() }
        match &r.leader_dc_label { Some(s) => b_leader_dc_label.append_value(s), None => b_leader_dc_label.append_null() }
        match &r.leader_continent { Some(s) => b_leader_continent.append_value(s), None => b_leader_continent.append_null() }
        match r.leader_stake_lamports { Some(v) => b_leader_stake_lamports.append_value(v), None => b_leader_stake_lamports.append_null() }
        match &r.validator_client { Some(s) => b_validator_client.append_value(s), None => b_validator_client.append_null() }

        match r.tick_delta { Some(v) => b_tick_delta.append_value(v), None => b_tick_delta.append_null() }
        match r.hash_delta { Some(v) => b_hash_delta.append_value(v), None => b_hash_delta.append_null() }
        match r.slot_delta { Some(v) => b_slot_delta.append_value(v), None => b_slot_delta.append_null() }
        b_leader_changed.append_value(r.leader_changed);
        match r.wall_trigger_to_send_ns { Some(v) => b_wall_trigger_to_send_ns.append_value(v), None => b_wall_trigger_to_send_ns.append_null() }
        match r.wall_send_rtt_ns { Some(v) => b_wall_send_rtt_ns.append_value(v), None => b_wall_send_rtt_ns.append_null() }
        match r.wall_send_to_observed_ns { Some(v) => b_wall_send_to_observed_ns.append_value(v), None => b_wall_send_to_observed_ns.append_null() }
        match r.wall_send_to_ss_observed_ns { Some(v) => b_wall_send_to_ss_observed_ns.append_value(v), None => b_wall_send_to_ss_observed_ns.append_null() }
        match r.wall_send_to_ys_observed_ns { Some(v) => b_wall_send_to_ys_observed_ns.append_value(v), None => b_wall_send_to_ys_observed_ns.append_null() }

        match r.nonce_update_observed_at_ns { Some(v) => b_nonce_update_observed_at_ns.append_value(v), None => b_nonce_update_observed_at_ns.append_null() }
        match &r.nonce_update_source { Some(s) => b_nonce_update_source.append_value(s), None => b_nonce_update_source.append_null() }
        match r.nonce_advanced_to_slot { Some(v) => b_nonce_advanced_to_slot.append_value(v), None => b_nonce_advanced_to_slot.append_null() }

        b_run_id.append_value(&r.run_id);
        b_chunk_index.append_value(r.chunk_index);
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(b_trigger_slot.finish()),
        Arc::new(b_trigger_tick.finish()),
        Arc::new(b_trigger_id.finish()),
        Arc::new(b_nonce_account_id.finish()),
        Arc::new(b_nonce_blockhash_used.finish()),
        Arc::new(b_sender_id.finish()),
        Arc::new(b_sender_name.finish()),
        Arc::new(b_tx_signature.finish()),
        Arc::new(b_tx_message_hash.finish()),
        Arc::new(b_endpoint_url.finish()),
        Arc::new(b_protocol.finish()),
        Arc::new(b_auth_tier.finish()),
        Arc::new(b_tip_account_used.finish()),
        Arc::new(b_tip_lamports.finish()),
        Arc::new(b_priority_fee_microlamports.finish()),
        Arc::new(b_compute_unit_limit.finish()),
        Arc::new(b_prepared_at_ns.finish()),
        Arc::new(b_pool_ready_at_ns.finish()),
        Arc::new(b_trigger_observed_at_ns.finish()),
        Arc::new(b_send_at_ns.finish()),
        Arc::new(b_send_ack_at_ns.finish()),
        Arc::new(b_send_order_in_trigger.finish()),
        Arc::new(b_host_clock_offset_ns.finish()),
        Arc::new(b_send_error.finish()),
        Arc::new(b_rpc_err_code.finish()),
        Arc::new(b_rpc_err_message.finish()),
        Arc::new(b_provider_request_id.finish()),
        Arc::new(b_http_status.finish()),
        Arc::new(b_rate_limit_state.finish()),
        Arc::new(b_observed_slot.finish()),
        Arc::new(b_observed_entry_index.finish()),
        Arc::new(b_observed_tick_in_slot.finish()),
        Arc::new(b_observed_cumulative_hashes_in_slot.finish()),
        Arc::new(b_ss_observed_at_ns.finish()),
        Arc::new(b_ys_observed_at_ns.finish()),
        Arc::new(b_observed_at_ns.finish()),
        Arc::new(b_observed_source.finish()),
        Arc::new(b_commitment_at_resolution.finish()),
        Arc::new(b_tentative_outcome.finish()),
        Arc::new(b_final_status.finish()),
        Arc::new(b_siblings_resolved_at_ns.finish()),
        Arc::new(b_leader_pubkey.finish()),
        Arc::new(b_leader_region_cc.finish()),
        Arc::new(b_leader_dc_label.finish()),
        Arc::new(b_leader_continent.finish()),
        Arc::new(b_leader_stake_lamports.finish()),
        Arc::new(b_validator_client.finish()),
        Arc::new(b_tick_delta.finish()),
        Arc::new(b_hash_delta.finish()),
        Arc::new(b_slot_delta.finish()),
        Arc::new(b_leader_changed.finish()),
        Arc::new(b_wall_trigger_to_send_ns.finish()),
        Arc::new(b_wall_send_rtt_ns.finish()),
        Arc::new(b_wall_send_to_observed_ns.finish()),
        Arc::new(b_wall_send_to_ss_observed_ns.finish()),
        Arc::new(b_wall_send_to_ys_observed_ns.finish()),
        Arc::new(b_nonce_update_observed_at_ns.finish()),
        Arc::new(b_nonce_update_source.finish()),
        Arc::new(b_nonce_advanced_to_slot.finish()),
        Arc::new(b_run_id.finish()),
        Arc::new(b_chunk_index.finish()),
    ];
    let batch = RecordBatch::try_new(Arc::new(schema.clone()), arrays)?;
    Ok(batch)
}

fn rate_limit_state_str(s: RateLimitState) -> &'static str {
    match s {
        RateLimitState::Ok => "OK",
        RateLimitState::Throttled429 => "THROTTLED_429",
        RateLimitState::CircuitOpen => "CIRCUIT_OPEN",
        RateLimitState::Timeout => "TIMEOUT",
    }
}

fn observed_source_str(s: ObservedSource) -> &'static str {
    match s {
        ObservedSource::Ss => "SS",
        ObservedSource::Ys => "YS",
        ObservedSource::Both => "BOTH",
    }
}

fn commitment_str(c: CommitmentAtResolution) -> &'static str {
    match c {
        CommitmentAtResolution::Processed => "PROCESSED",
        CommitmentAtResolution::Confirmed => "CONFIRMED",
        CommitmentAtResolution::Finalized => "FINALIZED",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::outcome::TentativeOutcome;
    use crossbeam_channel::bounded;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use solana_sdk::{hash::Hash, signature::Signature};
    use std::fs::File;
    use tempfile::TempDir;

    fn fake_record(slot: u64, sender_id: u8) -> FinalRecord {
        FinalRecord {
            trigger_slot: slot,
            trigger_tick: 5,
            trigger_id: [0; 16],
            nonce_account_id: 0,
            nonce_blockhash_used: Hash::default(),
            sender_id,
            sender_name: format!("mock-{}", sender_id),
            tx_signature: Signature::default(),
            tx_message_hash: [0; 32],
            endpoint_url: "mock://x".into(),
            protocol: "MOCK".into(),
            auth_tier: None,
            tip_account_used: None,
            tip_lamports: 1000,
            priority_fee_microlamports: 5000,
            compute_unit_limit: 200_000,
            prepared_at_ns: 1, pool_ready_at_ns: 2, trigger_observed_at_ns: 3,
            send_at_ns: 4, send_ack_at_ns: Some(5), send_order_in_trigger: 0,
            host_clock_offset_ns: None,
            send_error: None, rpc_err_code: None, rpc_err_message: None,
            provider_request_id: None, http_status: Some(200),
            rate_limit_state: RateLimitState::Ok,
            observed_slot: None, observed_entry_index: None,
            observed_tick_in_slot: None, observed_cumulative_hashes_in_slot: None,
            ss_observed_at_ns: None, ys_observed_at_ns: None,
            observed_at_ns: None, observed_source: None,
            commitment_at_resolution: None,
            tentative_outcome: TentativeOutcome::LandedTentative,
            final_status: FinalStatus::Pending,
            siblings_resolved_at_ns: None,
            leader_pubkey: None, leader_region_cc: None, leader_dc_label: None,
            leader_continent: None, leader_stake_lamports: None, validator_client: None,
            tick_delta: None, hash_delta: None, slot_delta: None,
            leader_changed: false,
            wall_trigger_to_send_ns: None, wall_send_rtt_ns: None,
            wall_send_to_observed_ns: None, wall_send_to_ss_observed_ns: None,
            wall_send_to_ys_observed_ns: None,
            nonce_update_observed_at_ns: None, nonce_update_source: None,
            nonce_advanced_to_slot: None,
            run_id: "test".into(), chunk_index: 0,
        }
    }

    #[test]
    fn writes_and_reads_back_records() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("out.parquet");
        let (tx, rx) = bounded(100);
        let handle = spawn_parquet(ParquetWriterConfig {
            final_rx: rx,
            output_path: path.clone(),
            row_group_size: 5,
            flush_interval: Duration::from_millis(100),
            pinned_core: None,
            counters: Arc::new(BenchCounters::default()),
        }).unwrap();

        for i in 0..10 {
            tx.send(fake_record(100 + i, (i % 2) as u8)).unwrap();
        }
        drop(tx);
        handle.join().unwrap();

        let file = File::open(&path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap().build().unwrap();
        let mut total_rows = 0;
        for batch in reader {
            let batch = batch.unwrap();
            total_rows += batch.num_rows();
        }
        assert_eq!(total_rows, 10);
    }
}
```

- [ ] **Step 2: Run tests (this is heavy; may need to fix compilation issues)**

Run: `cargo test -p fan-out-bench --lib writer::parquet_sink`
Expected: 1 test passes. If arrow builder API mismatches occur, consult arrow 55 docs; the pattern shown is canonical.

- [ ] **Step 3: Commit**

```bash
git add crates/fan-out-bench/src/writer/parquet_sink.rs
git commit -m "feat(fan-out-bench): add background-thread parquet sink with row groups"
```

---

## Task 14: End-to-end mock test

**Files:**
- Create: `crates/fan-out-bench/tests/e2e_mock.rs`

- [ ] **Step 1: Write integration test simulating the full pre-network flow**

Write `crates/fan-out-bench/tests/e2e_mock.rs`. This test wires up: schedule → tx_builder → pool → mock dispatcher → fake observations → parquet writer. Verifies dedup logic + record counts.

```rust
//! End-to-end mock pipeline test.
//!
//! Simulates: schedule → presign → mock dispatch → fake observation → parquet.
//! Verifies dedup logic (1 LANDED + N-1 DEDUPED per trigger).

use crossbeam_channel::bounded;
use fan_out_bench::{
    attempt_state::AttemptState,
    config::SenderKind,
    counters::BenchCounters,
    outcome::{FinalStatus, ObservedSource, RateLimitState, TentativeOutcome},
    pool::{PreSignedTx, TxPool},
    schedule::Schedule,
    senders::mock::MockSender,
    senders::TxSender,
    tx_builder::{build_variant, VariantParams},
    writer::{record::FinalRecord, ParquetWriterConfig, spawn_parquet},
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use solana_sdk::{hash::Hash, pubkey::Pubkey, signature::Keypair, signer::Signer};
use std::fs::File;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_mock_pipeline_produces_expected_outcomes() {
    let tmp = TempDir::new().unwrap();
    let parquet_path = tmp.path().join("tx-events.parquet");

    // Setup mock senders (3, all always-ack)
    let senders: Vec<Arc<dyn TxSender>> = vec![
        Arc::new(MockSender::always_ack(0, "mock-a")),
        Arc::new(MockSender::always_ack(1, "mock-b")),
        Arc::new(MockSender::always_ack(2, "mock-c")),
    ];

    // Setup parquet writer
    let (final_tx, final_rx) = bounded::<FinalRecord>(1000);
    let writer_handle = spawn_parquet(ParquetWriterConfig {
        final_rx,
        output_path: parquet_path.clone(),
        row_group_size: 16,
        flush_interval: Duration::from_millis(100),
        pinned_core: None,
        counters: Arc::new(BenchCounters::default()),
    }).unwrap();

    // Generate small schedule
    let mut schedule = Schedule::new(Some(42), 1000, 5);
    let chunk = schedule.generate_chunk();
    assert_eq!(chunk.len(), 5);

    // Authority/payer keypair
    let signer = Arc::new(Keypair::new());
    let nonce_pubkey = Pubkey::new_unique();
    let nonce_blockhash = Hash::new_unique();

    // For each scheduled entry, build N variants, simulate dispatch, simulate one wins
    let pool = TxPool::new();
    for entry in &chunk {
        for sender in &senders {
            let tip_account = Pubkey::new_unique(); // mock
            let variant = build_variant(
                VariantParams {
                    nonce_pubkey,
                    nonce_blockhash,
                    payer: signer.pubkey(),
                    sender_id: sender.id(),
                    sender_kind: SenderKind::Mock,
                    tip_account: None, // mock doesn't need
                    tip_lamports: 1000,
                    priority_fee_microlamports: 5000,
                    compute_unit_limit: 200_000,
                },
                &signer,
            ).unwrap();
            pool.insert(
                entry.slot,
                entry.tick,
                sender.id(),
                PreSignedTx {
                    tx: Arc::new(variant.tx),
                    message_hash: variant.message_hash,
                    prepared_at: Instant::now(),
                    pool_ready_at: Instant::now(),
                },
            );
        }
    }
    assert_eq!(pool.len(), 5 * 3); // 5 slots × 3 senders

    // Simulate trigger firing: per (slot,tick), take all variants, "dispatch", emit records
    let run_id = "e2e-mock".to_string();
    let anchor = Instant::now();
    for entry in &chunk {
        let variants = pool.take_all_for(entry.slot, entry.tick);
        assert_eq!(variants.len(), 3);

        // Simulate send_outcome for each, plus pick winner (deterministic: sender_id 0 wins)
        let mut records = Vec::new();
        for (order, (sender_id, presigned)) in variants.iter().enumerate() {
            let send_at_ns = anchor.elapsed().as_nanos() as u64;
            let sender = senders.iter().find(|s| s.id() == *sender_id).unwrap();
            let send_outcome = sender.send(&presigned.tx).await;
            let send_ack_at_ns = send_outcome.send_ack_at.map(|t| t.duration_since(anchor).as_nanos() as u64);

            // Winner = sender_id 0; others = DEDUPED
            let (tentative, observed_at, observed_source) = if *sender_id == 0 {
                (TentativeOutcome::LandedTentative, Some(send_at_ns + 100_000_000), Some(ObservedSource::Ss))
            } else {
                (TentativeOutcome::DedupedTentative, None, None)
            };

            let record = FinalRecord {
                trigger_slot: entry.slot,
                trigger_tick: entry.tick,
                trigger_id: [0; 16],
                nonce_account_id: 0,
                nonce_blockhash_used: nonce_blockhash,
                sender_id: *sender_id,
                sender_name: sender.name().to_string(),
                tx_signature: send_outcome.signature,
                tx_message_hash: presigned.message_hash,
                endpoint_url: sender.endpoint_url().to_string(),
                protocol: sender.protocol().to_string(),
                auth_tier: None,
                tip_account_used: None,
                tip_lamports: 1000,
                priority_fee_microlamports: 5000,
                compute_unit_limit: 200_000,
                prepared_at_ns: presigned.prepared_at.duration_since(anchor).as_nanos() as u64,
                pool_ready_at_ns: presigned.pool_ready_at.duration_since(anchor).as_nanos() as u64,
                trigger_observed_at_ns: send_at_ns,
                send_at_ns,
                send_ack_at_ns,
                send_order_in_trigger: order as u8,
                host_clock_offset_ns: None,
                send_error: send_outcome.error.clone(),
                rpc_err_code: send_outcome.rpc_err_code,
                rpc_err_message: send_outcome.rpc_err_message.clone(),
                provider_request_id: send_outcome.provider_request_id.clone(),
                http_status: send_outcome.http_status,
                rate_limit_state: send_outcome.rate_limit_state,
                observed_slot: if observed_at.is_some() { Some(entry.slot) } else { None },
                observed_entry_index: None,
                observed_tick_in_slot: None,
                observed_cumulative_hashes_in_slot: None,
                ss_observed_at_ns: observed_at,
                ys_observed_at_ns: None,
                observed_at_ns: observed_at,
                observed_source,
                commitment_at_resolution: None,
                tentative_outcome: tentative,
                final_status: FinalStatus::Pending,
                siblings_resolved_at_ns: None,
                leader_pubkey: None, leader_region_cc: None, leader_dc_label: None,
                leader_continent: None, leader_stake_lamports: None, validator_client: None,
                tick_delta: None, hash_delta: None, slot_delta: None,
                leader_changed: false,
                wall_trigger_to_send_ns: None, wall_send_rtt_ns: None,
                wall_send_to_observed_ns: None, wall_send_to_ss_observed_ns: None,
                wall_send_to_ys_observed_ns: None,
                nonce_update_observed_at_ns: None, nonce_update_source: None,
                nonce_advanced_to_slot: None,
                run_id: run_id.clone(),
                chunk_index: 0,
            };
            records.push(record);
        }
        for r in records {
            final_tx.send(r).unwrap();
        }
    }

    drop(final_tx);
    writer_handle.join().unwrap();

    // Read parquet back, verify counts
    let file = File::open(&parquet_path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap().build().unwrap();
    let mut total = 0;
    let mut landed = 0;
    let mut deduped = 0;
    for batch in reader {
        let batch = batch.unwrap();
        total += batch.num_rows();
        let outcome_col = batch.column_by_name("tentative_outcome").unwrap();
        let outcomes = outcome_col.as_any().downcast_ref::<arrow_array::StringArray>().unwrap();
        for i in 0..batch.num_rows() {
            match outcomes.value(i) {
                "LANDED_TENTATIVE" => landed += 1,
                "DEDUPED_TENTATIVE" => deduped += 1,
                other => panic!("unexpected outcome: {}", other),
            }
        }
    }
    assert_eq!(total, 5 * 3, "expected 15 records (5 triggers × 3 senders)");
    assert_eq!(landed, 5, "exactly 1 LANDED per trigger");
    assert_eq!(deduped, 10, "exactly 2 DEDUPED per trigger");
}
```

- [ ] **Step 2: Run the test**

Run: `cargo test -p fan-out-bench --test e2e_mock`
Expected: 1 test passes. May need adjustments if Arrow API differs slightly.

- [ ] **Step 3: Commit**

```bash
git add crates/fan-out-bench/tests/e2e_mock.rs
git commit -m "test(fan-out-bench): add end-to-end mock pipeline integration test"
```

---

## Task 15: README skeleton

**Files:**
- Create: `crates/fan-out-bench/README.md`

- [ ] **Step 1: Write minimal README pointing to spec**

Write `crates/fan-out-bench/README.md`:

```markdown
# fan-out-bench

Etap 1 multi-sender Solana tx send benchmark z durable nonce dedup. Wysyła pre-signed self-transfer tx równolegle przez N senderów (Helius, Jito, Nozomi, BlockRazor, AllenHark, etc.), używa durable nonce do dedup w validation phase, zapisuje per-sender outcome/latency do parquet.

## Status (Plan 1 complete)

Plan 1 — foundation:
- ✅ Crate scaffolding + workspace member
- ✅ Config types + example JSON
- ✅ Schedule (deterministic, chunked, open-ended)
- ✅ Memo encoder (ASCII printable, UTF-8 safe)
- ✅ Tip account registry (12 senderów, RR rotator)
- ✅ TxBuilder z hard asserts (AdvanceNonce ix[0], etc.)
- ✅ Pool (DashMap-backed)
- ✅ TxSender trait + MockSender
- ✅ Parquet schema (61 kolumn) + writer
- ✅ Counters
- ✅ End-to-end mock pipeline test

Not yet implemented (later plans):
- Plan 2: nonce setup/teardown binaries, NonceManager, YS subscription
- Plan 3: SS+YS entry merger, Observer with PoH tick tracking
- Plan 4: First real senders (Helius, Jito), Matcher state machine, Finality tracker, runtime wiring
- Plan 5: REST senders (Nozomi, 0slot, bloXroute, Astralane, Syncro, Triton)
- Plan 6: gRPC/QUIC senders (BlockRazor, AllenHark, NextBlock, Harmonic)
- Plan 7: Ops + polish (budget watcher, clock monitor, probe-senders, smoke harness)

## Reference

- Design spec: `../../docs/superpowers/specs/2026-05-14-fan-out-bench-design.md`
- Implementation plans: `../../docs/superpowers/plans/2026-05-14-fan-out-bench-*.md`

## Quick test

```bash
cargo test -p fan-out-bench
```

Should run ~35 unit tests + 1 integration test, all passing.
```

- [ ] **Step 2: Commit**

```bash
git add crates/fan-out-bench/README.md
git commit -m "docs(fan-out-bench): add README pointing to spec and plan progress"
```

---

## Task 16: Final verification

- [ ] **Step 1: Full test suite passes**

Run: `cargo test -p fan-out-bench`
Expected: all tests pass (~35 unit + 1 integration).

- [ ] **Step 2: Lints clean**

Run: `cargo clippy -p fan-out-bench --all-targets -- -D warnings`
Expected: no warnings. If any appear, fix them inline (likely unused imports or dead code in mod stubs from Task 1).

- [ ] **Step 3: Verify release build**

Run: `cargo build --release -p fan-out-bench`
Expected: builds successfully.

- [ ] **Step 4: Confirm git log shows expected commits**

Run: `git log --oneline -20`
Expected: ~15 commits from this plan, each with `feat(fan-out-bench):` or `test(fan-out-bench):` or `docs(fan-out-bench):` prefix.

---

## Plan 1 done

Foundation gotowa. Następne plany:
- Plan 2: Nonce infrastructure
- Plan 3: Entry sources + Observer
- Plan 4: First senders + Matcher + Runtime
- Plan 5: REST senders
- Plan 6: gRPC/QUIC senders
- Plan 7: Ops + polish
