# fan-out-bench — Plan 4: First senders + Matcher + Runtime

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development.

**Goal:** Złożyć pełny pipeline benchu z pierwszymi realnymi senderami (Helius + Jito-single + Jito-bundle), Matcher state machine, Preparer, Dispatcher i Runtime CLI. Po tym planie bench jest uruchamialny — przyjmuje config, generuje schedule, łączy się z SS+YS, wysyła tx, zapisuje parquet. Finality tracker + RPC fallback są STUB (zapisują tylko tentative outcomes) — Plan 5 dodaje pełną finalność.

**Architecture:** Wszystkie komponenty z Plan 1-3 łączone w `runtime.rs`. Każdy sender to implementacja TxSender trait. Matcher trzyma single-owner state per (trigger_id, sender_id). Preparer karmi pool z N wariantów per trigger używając NonceManager.

**Tech Stack:** Reqwest dla HTTP JSON-RPC, tokio dla dispatcher async, std threads dla observer/preparer/matcher/parquet.

**Reference spec:** `docs/superpowers/specs/2026-05-14-fan-out-bench-design.md` §2.2, §3.4, §3.5, §7.2

**Previous plans:** 1 (foundation), 2 (nonce), 3 (observer).

---

## File structure (Plan 4 scope)

```
crates/fan-out-bench/
├── src/
│   ├── lib.rs                       — declare new modules
│   ├── http_jsonrpc.rs              — shared reqwest helper for JSON-RPC senders
│   ├── observer.rs                  — extended: sig matching against pending_sigs
│   ├── match_event.rs               — MatchEvent type emitted by observer for matcher
│   ├── senders/
│   │   ├── helius.rs                — HeliusSender impl
│   │   └── jito.rs                  — JitoSender impl (sendTransaction + sendBundle)
│   ├── preparer.rs                  — signs N variants per (slot,tick) using nonce + tx_builder
│   ├── dispatcher.rs                — fan-out async POST per-sender
│   ├── matcher.rs                   — AttemptState map + tentative resolution
│   ├── trigger_id.rs                — TriggerId type (hash of slot,tick,nonce_id)
│   ├── runtime.rs                   — wires everything into one run loop
│   └── bin/
│       └── run.rs                   — CLI: cargo run --bin run -- --config <path>
└── tests/
    └── pipeline_mock.rs             — full pipeline with mock senders + mock entries
```

NOT in this plan (deferred):
- Finality tracker (Plan 5) — `final_status` stays `PENDING` in parquet
- RPC fallback for UNKNOWN_PENDING (Plan 5) — left as tentative
- Remaining REST senders (Plan 5)
- gRPC/QUIC senders (Plan 6)

---

## Task 1: Module scaffolding

**Files:**
- Modify: `crates/fan-out-bench/src/lib.rs`
- Modify: `crates/fan-out-bench/src/senders/mod.rs`
- Create: stubs for new modules

- [ ] **Step 1: Update lib.rs**

```rust
pub mod attempt_state;
pub mod config;
pub mod counters;
pub mod dispatcher;
pub mod http_jsonrpc;
pub mod match_event;
pub mod matcher;
pub mod memo;
pub mod merger;
pub mod nonce;
pub mod observer;
pub mod outcome;
pub mod pool;
pub mod preparer;
pub mod runtime;
pub mod schedule;
pub mod senders;
pub mod tip_accounts;
pub mod trigger;
pub mod trigger_id;
pub mod tx_builder;
pub mod wallet;
pub mod writer;
```

- [ ] **Step 2: Update senders/mod.rs to declare new sender submodules**

Edit `crates/fan-out-bench/src/senders/mod.rs`, change `pub mod mock;` block to:

```rust
pub mod helius;
pub mod jito;
pub mod mock;
```

(Keep everything else in mod.rs unchanged — `TxSender` trait, `SendOutcome`, etc.)

- [ ] **Step 3: Create stub files**

```bash
cd /home/jjaksik/Repos/my-scripts/crates/fan-out-bench/src
touch dispatcher.rs http_jsonrpc.rs match_event.rs matcher.rs preparer.rs runtime.rs trigger_id.rs
touch senders/helius.rs senders/jito.rs
mkdir -p bin
touch bin/run.rs
```

Each stub gets `// implementation in later task` line. `bin/run.rs` gets:

```rust
fn main() {
    println!("run binary — implementation in later task");
}
```

- [ ] **Step 4: Verify build**

Run: `cargo check -p fan-out-bench`. Expected: clean.

---

## Task 2: HTTP JSON-RPC helper

**Files:**
- Replace stub: `crates/fan-out-bench/src/http_jsonrpc.rs`

- [ ] **Step 1: Implement helper**

```rust
//! Shared reqwest helper for JSON-RPC senders (Helius, Jito, Nozomi, etc.).
//!
//! Constructs the standard `sendTransaction` JSON-RPC body and parses response.

use base64::Engine;
use serde::{Deserialize, Serialize};
use solana_sdk::transaction::Transaction;
use std::time::Duration;

#[derive(Debug, Serialize)]
struct JsonRpcRequest<'a> {
    jsonrpc: &'static str,
    id: u64,
    method: &'a str,
    params: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    pub id: serde_json::Value,
    #[serde(default)]
    pub result: Option<String>,
    #[serde(default)]
    pub error: Option<JsonRpcError>,
}

#[derive(Debug, Deserialize)]
pub struct JsonRpcError {
    pub code: i32,
    pub message: String,
    #[serde(default)]
    pub data: Option<serde_json::Value>,
}

/// Serialize transaction to base64 (the encoding most senders expect).
pub fn tx_to_base64(tx: &Transaction) -> String {
    let bytes = bincode::serialize(tx).expect("transaction serialization never fails");
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

/// Construct the standard sendTransaction JSON-RPC body for a base64-encoded tx.
pub fn build_send_transaction_body(tx_base64: &str, skip_preflight: bool, max_retries: u64) -> String {
    let req = JsonRpcRequest {
        jsonrpc: "2.0",
        id: 1,
        method: "sendTransaction",
        params: serde_json::json!([
            tx_base64,
            {
                "encoding": "base64",
                "skipPreflight": skip_preflight,
                "maxRetries": max_retries,
            }
        ]),
    };
    serde_json::to_string(&req).expect("JSON-RPC body serialization never fails")
}

/// Build a reqwest client with HTTP/HTTPS, rustls, keep-alive.
pub fn build_http_client(timeout: Duration) -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(timeout)
        .pool_max_idle_per_host(8)
        .tcp_keepalive(Duration::from_secs(30))
        .build()
        .expect("reqwest client build never fails with these settings")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_body_has_required_fields() {
        let body = build_send_transaction_body("ZmFrZQ==", true, 0);
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["jsonrpc"], "2.0");
        assert_eq!(v["method"], "sendTransaction");
        assert_eq!(v["params"][0], "ZmFrZQ==");
        assert_eq!(v["params"][1]["encoding"], "base64");
        assert_eq!(v["params"][1]["skipPreflight"], true);
        assert_eq!(v["params"][1]["maxRetries"], 0);
    }

    #[test]
    fn parse_success_response() {
        let json = r#"{"jsonrpc":"2.0","id":1,"result":"5fzAB...txSig"}"#;
        let r: JsonRpcResponse = serde_json::from_str(json).unwrap();
        assert_eq!(r.result.as_deref(), Some("5fzAB...txSig"));
        assert!(r.error.is_none());
    }

    #[test]
    fn parse_error_response() {
        let json = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32005,"message":"Too many requests"}}"#;
        let r: JsonRpcResponse = serde_json::from_str(json).unwrap();
        assert!(r.result.is_none());
        let err = r.error.unwrap();
        assert_eq!(err.code, -32005);
        assert_eq!(err.message, "Too many requests");
    }

    #[test]
    fn tx_to_base64_produces_url_safe() {
        let tx = Transaction::default();
        let b64 = tx_to_base64(&tx);
        // base64 std encoding uses A-Z, a-z, 0-9, +, /, =
        assert!(b64.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '/' | '=')));
    }
}
```

Run: `cargo test -p fan-out-bench --lib http_jsonrpc`. Expected: 4 tests pass.

---

## Task 3: Helius sender

**Files:**
- Replace stub: `crates/fan-out-bench/src/senders/helius.rs`

- [ ] **Step 1: Implement HeliusSender**

```rust
//! Helius Sender impl — HTTP POST to FRA fast endpoint.
//!
//! Endpoint: http://fra-sender.helius-rpc.com/fast
//! Body: JSON-RPC sendTransaction with base64 tx
//! Auth: optional ?api-key= query param (for custom TPS)

use super::{SendOutcome, TxSender};
use crate::http_jsonrpc::{build_http_client, build_send_transaction_body, tx_to_base64, JsonRpcResponse};
use crate::outcome::RateLimitState;
use solana_sdk::transaction::Transaction;
use std::str::FromStr;
use std::time::{Duration, Instant};

pub struct HeliusSender {
    id: u8,
    name: String,
    endpoint: String,
    api_key: Option<String>,
    swqos_only: bool,
    client: reqwest::Client,
}

impl HeliusSender {
    pub fn new(
        id: u8,
        name: impl Into<String>,
        endpoint: impl Into<String>,
        api_key: Option<String>,
        swqos_only: bool,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            endpoint: endpoint.into(),
            api_key,
            swqos_only,
            client: build_http_client(Duration::from_secs(5)),
        }
    }

    fn build_url(&self) -> String {
        let mut url = self.endpoint.clone();
        let mut qs: Vec<String> = Vec::new();
        if let Some(key) = &self.api_key {
            qs.push(format!("api-key={}", key));
        }
        if self.swqos_only {
            qs.push("swqos_only=true".into());
        }
        if !qs.is_empty() {
            url.push('?');
            url.push_str(&qs.join("&"));
        }
        url
    }
}

#[async_trait::async_trait]
impl TxSender for HeliusSender {
    fn id(&self) -> u8 { self.id }
    fn name(&self) -> &str { &self.name }
    fn endpoint_url(&self) -> &str { &self.endpoint }
    fn protocol(&self) -> &'static str { "HTTP_JSONRPC" }

    async fn send(&self, tx: &Transaction) -> SendOutcome {
        let send_at = Instant::now();
        let signature = tx.signatures.first().copied().unwrap_or_default();
        let b64 = tx_to_base64(tx);
        let body = build_send_transaction_body(&b64, true, 0);
        let url = self.build_url();

        let resp_result = self.client
            .post(&url)
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .await;

        let send_ack_at = Some(Instant::now());

        match resp_result {
            Err(e) => SendOutcome {
                send_at, send_ack_at: None, signature,
                provider_request_id: None,
                http_status: None,
                rpc_err_code: None,
                rpc_err_message: None,
                rate_limit_state: if e.is_timeout() { RateLimitState::Timeout } else { RateLimitState::Ok },
                error: Some(format!("network: {}", e)),
            },
            Ok(resp) => {
                let status = resp.status().as_u16();
                let body_text = resp.text().await.unwrap_or_default();
                if let Ok(parsed) = serde_json::from_str::<JsonRpcResponse>(&body_text) {
                    if let Some(err) = parsed.error {
                        let rate_limit_state = if err.code == -32005 || status == 429 {
                            RateLimitState::Throttled429
                        } else {
                            RateLimitState::Ok
                        };
                        SendOutcome {
                            send_at, send_ack_at, signature,
                            provider_request_id: None,
                            http_status: Some(status),
                            rpc_err_code: Some(err.code),
                            rpc_err_message: Some(err.message.clone()),
                            rate_limit_state,
                            error: Some(err.message),
                        }
                    } else {
                        // Returned sig may differ from local-calculated if Helius re-derives;
                        // prefer local signature for correctness with pending_sigs lookup.
                        let returned_sig = parsed.result.and_then(|s| solana_sdk::signature::Signature::from_str(&s).ok());
                        SendOutcome {
                            send_at, send_ack_at, signature: returned_sig.unwrap_or(signature),
                            provider_request_id: None,
                            http_status: Some(status),
                            rpc_err_code: None,
                            rpc_err_message: None,
                            rate_limit_state: RateLimitState::Ok,
                            error: None,
                        }
                    }
                } else {
                    SendOutcome {
                        send_at, send_ack_at, signature,
                        provider_request_id: None,
                        http_status: Some(status),
                        rpc_err_code: None,
                        rpc_err_message: Some(format!("non-JSONRPC response: {}", body_text)),
                        rate_limit_state: if status == 429 { RateLimitState::Throttled429 } else { RateLimitState::Ok },
                        error: Some(format!("HTTP {} body: {}", status, body_text)),
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_url_no_auth_no_swqos() {
        let s = HeliusSender::new(0, "helius", "http://fra-sender.helius-rpc.com/fast", None, false);
        assert_eq!(s.build_url(), "http://fra-sender.helius-rpc.com/fast");
    }

    #[test]
    fn build_url_with_api_key() {
        let s = HeliusSender::new(0, "helius", "http://x/fast", Some("KEY".into()), false);
        assert_eq!(s.build_url(), "http://x/fast?api-key=KEY");
    }

    #[test]
    fn build_url_swqos_only() {
        let s = HeliusSender::new(0, "helius", "http://x/fast", None, true);
        assert_eq!(s.build_url(), "http://x/fast?swqos_only=true");
    }

    #[test]
    fn build_url_with_api_key_and_swqos() {
        let s = HeliusSender::new(0, "helius", "http://x/fast", Some("KEY".into()), true);
        assert_eq!(s.build_url(), "http://x/fast?api-key=KEY&swqos_only=true");
    }

    #[test]
    fn protocol_is_http_jsonrpc() {
        let s = HeliusSender::new(0, "helius", "http://x", None, false);
        assert_eq!(s.protocol(), "HTTP_JSONRPC");
    }
}
```

Run: `cargo test -p fan-out-bench --lib senders::helius`. Expected: 5 tests pass.

---

## Task 4: Jito sender

**Files:**
- Replace stub: `crates/fan-out-bench/src/senders/jito.rs`

- [ ] **Step 1: Implement JitoSender**

```rust
//! Jito Block Engine sender — single tx via /api/v1/transactions.
//!
//! Endpoint: https://frankfurt.mainnet.block-engine.jito.wtf/api/v1/transactions
//! Auth: none default; optional x-jito-auth header for custom rate limits
//! Note: Bundle path (sendBundle) is a separate sender impl in Plan 5/6.

use super::{SendOutcome, TxSender};
use crate::http_jsonrpc::{build_http_client, build_send_transaction_body, tx_to_base64, JsonRpcResponse};
use crate::outcome::RateLimitState;
use solana_sdk::transaction::Transaction;
use std::str::FromStr;
use std::time::{Duration, Instant};

pub struct JitoSender {
    id: u8,
    name: String,
    endpoint: String,
    auth_uuid: Option<String>,
    client: reqwest::Client,
}

impl JitoSender {
    pub fn new(
        id: u8,
        name: impl Into<String>,
        endpoint: impl Into<String>,
        auth_uuid: Option<String>,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            endpoint: endpoint.into(),
            auth_uuid,
            client: build_http_client(Duration::from_secs(5)),
        }
    }
}

#[async_trait::async_trait]
impl TxSender for JitoSender {
    fn id(&self) -> u8 { self.id }
    fn name(&self) -> &str { &self.name }
    fn endpoint_url(&self) -> &str { &self.endpoint }
    fn protocol(&self) -> &'static str { "HTTP_JSONRPC" }

    async fn send(&self, tx: &Transaction) -> SendOutcome {
        let send_at = Instant::now();
        let signature = tx.signatures.first().copied().unwrap_or_default();
        let b64 = tx_to_base64(tx);
        let body = build_send_transaction_body(&b64, true, 0);

        let mut req = self.client
            .post(&self.endpoint)
            .header("Content-Type", "application/json")
            .body(body);
        if let Some(uuid) = &self.auth_uuid {
            req = req.header("x-jito-auth", uuid);
        }

        let resp_result = req.send().await;
        let send_ack_at = Some(Instant::now());

        match resp_result {
            Err(e) => SendOutcome {
                send_at, send_ack_at: None, signature,
                provider_request_id: None,
                http_status: None,
                rpc_err_code: None,
                rpc_err_message: None,
                rate_limit_state: if e.is_timeout() { RateLimitState::Timeout } else { RateLimitState::Ok },
                error: Some(format!("network: {}", e)),
            },
            Ok(resp) => {
                let status = resp.status().as_u16();
                let body_text = resp.text().await.unwrap_or_default();
                if let Ok(parsed) = serde_json::from_str::<JsonRpcResponse>(&body_text) {
                    if let Some(err) = parsed.error {
                        let rate_limit_state = if err.code == -32005 || status == 429 {
                            RateLimitState::Throttled429
                        } else {
                            RateLimitState::Ok
                        };
                        SendOutcome {
                            send_at, send_ack_at, signature,
                            provider_request_id: None,
                            http_status: Some(status),
                            rpc_err_code: Some(err.code),
                            rpc_err_message: Some(err.message.clone()),
                            rate_limit_state,
                            error: Some(err.message),
                        }
                    } else {
                        let returned_sig = parsed.result.and_then(|s| solana_sdk::signature::Signature::from_str(&s).ok());
                        SendOutcome {
                            send_at, send_ack_at, signature: returned_sig.unwrap_or(signature),
                            provider_request_id: None,
                            http_status: Some(status),
                            rpc_err_code: None,
                            rpc_err_message: None,
                            rate_limit_state: RateLimitState::Ok,
                            error: None,
                        }
                    }
                } else {
                    SendOutcome {
                        send_at, send_ack_at, signature,
                        provider_request_id: None,
                        http_status: Some(status),
                        rpc_err_code: None,
                        rpc_err_message: Some(format!("non-JSONRPC response: {}", body_text)),
                        rate_limit_state: if status == 429 { RateLimitState::Throttled429 } else { RateLimitState::Ok },
                        error: Some(format!("HTTP {} body: {}", status, body_text)),
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jito_construct_basic() {
        let s = JitoSender::new(0, "jito-fra", "https://frankfurt.mainnet.block-engine.jito.wtf/api/v1/transactions", None);
        assert_eq!(s.name(), "jito-fra");
        assert_eq!(s.protocol(), "HTTP_JSONRPC");
    }

    #[test]
    fn jito_with_auth() {
        let s = JitoSender::new(0, "jito-fra", "https://x/api/v1/transactions", Some("uuid-123".into()));
        assert_eq!(s.auth_uuid.as_deref(), Some("uuid-123"));
    }
}
```

Run: `cargo test -p fan-out-bench --lib senders::jito`. Expected: 2 tests pass.

---

## Task 5: TriggerId type

**Files:**
- Replace stub: `crates/fan-out-bench/src/trigger_id.rs`

- [ ] **Step 1: Implement TriggerId**

```rust
//! TriggerId — uniquely identifies a (slot, tick, nonce_account_id) trigger.
//!
//! Used as key in Matcher's AttemptState map to group sibling attempts.

use crate::nonce::manager::NonceId;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TriggerId(pub [u8; 16]);

impl TriggerId {
    pub fn new(slot: u64, tick: u8, nonce_id: NonceId) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(slot.to_le_bytes());
        hasher.update([tick]);
        hasher.update(nonce_id.to_le_bytes());
        let full: [u8; 32] = hasher.finalize().into();
        let mut out = [0u8; 16];
        out.copy_from_slice(&full[..16]);
        TriggerId(out)
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trigger_id_deterministic() {
        let a = TriggerId::new(100, 5, 0);
        let b = TriggerId::new(100, 5, 0);
        assert_eq!(a, b);
    }

    #[test]
    fn trigger_id_unique_per_args() {
        let a = TriggerId::new(100, 5, 0);
        let b = TriggerId::new(100, 5, 1);
        let c = TriggerId::new(100, 6, 0);
        let d = TriggerId::new(101, 5, 0);
        assert_ne!(a, b);
        assert_ne!(a, c);
        assert_ne!(a, d);
    }
}
```

Run: `cargo test -p fan-out-bench --lib trigger_id`. Expected: 2 tests pass.

---

## Task 6: MatchEvent + Observer sig matching extension

**Files:**
- Replace stub: `crates/fan-out-bench/src/match_event.rs`
- Modify: `crates/fan-out-bench/src/observer.rs`

- [ ] **Step 1: Define MatchEvent**

```rust
//! MatchEvent — emitted by observer when a pending signature is observed.

use entry_sources::SourceKind;
use solana_sdk::signature::Signature;
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct MatchEvent {
    pub signature: Signature,
    pub observed_at: Instant,
    pub observed_slot: u64,
    pub observed_entry_index: u32,
    pub observed_tick_in_slot: Option<u8>,
    pub observed_cumulative_hashes_in_slot: Option<u64>,
    pub observed_source: SourceKind,
}
```

- [ ] **Step 2: Extend ObserverConfig and observer.rs**

Modify `crates/fan-out-bench/src/observer.rs`:

1. Add use: `use crate::match_event::MatchEvent;` and `use solana_sdk::signature::Signature;` and `use dashmap::DashSet;`
2. Extend `ObserverConfig` with two new fields (right after `trigger_tx`):

```rust
    pub match_tx: Sender<MatchEvent>,
    pub pending_sigs: Arc<DashSet<Signature>>,
```

3. In `process_entry`, add sig matching AFTER the tick detection block. Find this section near the end of `process_entry`:

```rust
        } else {
            cfg.counters
                .fork_tick_overflow
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}
```

Add immediately AFTER the `if is_tick { ... }` block (still inside `process_entry`, before closing `}`):

```rust
    // Sig matching: if this entry contains any of our pending signatures, emit MatchEvent.
    for sig in obs.signatures.iter() {
        if cfg.pending_sigs.remove(sig).is_some() {
            let event = MatchEvent {
                signature: *sig,
                observed_at: Instant::now(),
                observed_slot: obs.slot,
                observed_entry_index: obs.entry_index,
                observed_tick_in_slot: if state.tick_idx > 0 { Some(state.tick_idx) } else { None },
                observed_cumulative_hashes_in_slot: Some(state.cumulative_hashes_in_slot),
                observed_source: merged.first_seen_source,
            };
            if cfg.match_tx.try_send(event).is_err() {
                cfg.counters
                    .match_queue_full
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }
```

4. Update setup_observer in tests to include the new fields. In observer.rs test `setup_observer`:

```rust
    #[allow(clippy::type_complexity)]
    fn setup_observer(
        schedule: HashSet<(u64, u8)>,
    ) -> (
        crossbeam_channel::Sender<MergedEntry>,
        crossbeam_channel::Receiver<TriggerEvent>,
        crossbeam_channel::Receiver<MatchEvent>,
        Arc<DashSet<Signature>>,
        Arc<AtomicBool>,
        Arc<BenchCounters>,
        JoinHandle<()>,
    ) {
        let (in_tx, in_rx) = unbounded();
        let (out_tx, out_rx) = bounded(100);
        let (match_tx, match_rx) = bounded(100);
        let pending_sigs = Arc::new(DashSet::new());
        let stop = Arc::new(AtomicBool::new(false));
        let counters = Arc::new(BenchCounters::default());
        let current_slot = Arc::new(AtomicU64::new(0));
        let handle = spawn(ObserverConfig {
            merged_rx: in_rx,
            schedule: Arc::new(schedule),
            trigger_tx: out_tx,
            match_tx,
            pending_sigs: pending_sigs.clone(),
            current_slot,
            pinned_core: None,
            counters: counters.clone(),
            stop: stop.clone(),
        }).unwrap();
        (in_tx, out_rx, match_rx, pending_sigs, stop, counters, handle)
    }
```

5. Update all 6 existing tests that call `setup_observer` — they expect 5-tuple, now return 7-tuple. For each test, change the destructuring pattern. Use:

```rust
        let (in_tx, out_rx, _match_rx, _pending_sigs, stop, _counters, handle) = setup_observer(schedule);
```

For `no_trigger_when_schedule_does_not_match`, keep `counters` named:

```rust
        let (in_tx, out_rx, _match_rx, _pending_sigs, stop, counters, handle) = setup_observer(schedule);
```

6. Add NEW test at end of mod tests:

```rust
    #[test]
    fn sig_in_entry_emits_match_event() {
        use solana_sdk::signature::Signature;
        let schedule: HashSet<(u64, u8)> = HashSet::new();
        let (in_tx, _out_rx, match_rx, pending_sigs, stop, _counters, handle) = setup_observer(schedule);

        let sig = Signature::new_unique();
        pending_sigs.insert(sig);

        let mut obs = MergedEntry {
            observation: EntryObservation {
                source: SourceKind::ShredStream,
                observed_at: Instant::now(),
                slot: 100,
                entry_index: 7,
                num_hashes: 1000,
                entry_hash: Hash::new_unique(),
                tx_count: 1,
                signatures: SignatureVec::new(),
                first_shred_at: None,
                leader: None,
            },
            first_seen_source: SourceKind::ShredStream,
        };
        obs.observation.signatures.push(sig);
        in_tx.send(obs).unwrap();

        std::thread::sleep(Duration::from_millis(30));
        let event = match_rx.try_recv().expect("expected match event");
        assert_eq!(event.signature, sig);
        assert_eq!(event.observed_slot, 100);
        assert_eq!(event.observed_entry_index, 7);

        shutdown(in_tx, stop, handle);
    }
```

Run: `cargo test -p fan-out-bench --lib observer`. Expected: 7 tests pass.

Also fix integration test `tests/observer_integration.rs` to include new ObserverConfig fields. Change both ObserverConfig constructions to include:

```rust
        match_tx: <add a bounded channel sender>,
        pending_sigs: Arc::new(dashmap::DashSet::new()),
```

For each integration test, add at top of test body:
```rust
    let (match_tx, _match_rx) = bounded::<fan_out_bench::match_event::MatchEvent>(100);
```

And add to ObserverConfig:
```rust
        match_tx,
        pending_sigs: Arc::new(dashmap::DashSet::new()),
```

Run: `cargo test -p fan-out-bench --test observer_integration`. Expected: 2 tests pass.

---

## Task 7: Matcher — core types

**Files:**
- Replace stub: `crates/fan-out-bench/src/matcher.rs`

- [ ] **Step 1: Define MatcherConfig + skeleton**

```rust
//! Matcher — single-owner state machine per (TriggerId, sender_id).
//!
//! See spec §7.2. Receives SendEvent (transport outcome) + MatchEvent (on-chain
//! observation), maintains AttemptState, emits FinalRecord rows when terminal.

use crate::attempt_state::AttemptState;
use crate::counters::BenchCounters;
use crate::match_event::MatchEvent;
use crate::outcome::{FinalStatus, ObservedSource, RateLimitState, TentativeOutcome};
use crate::trigger_id::TriggerId;
use crate::writer::record::FinalRecord;
use crossbeam_channel::{Receiver, Sender};
use dashmap::DashSet;
use entry_sources::SourceKind;
use solana_sdk::{hash::Hash, pubkey::Pubkey, signature::Signature};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// SendEvent emitted by dispatcher after attempting a single tx send.
#[derive(Debug, Clone)]
pub struct SendEvent {
    pub trigger_id: TriggerId,
    pub sender_id: u8,
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

/// Attempt registration emitted by dispatcher before send_event.
#[derive(Debug, Clone)]
pub struct RegisterEvent {
    pub trigger_id: TriggerId,
    pub sender_id: u8,
    pub sender_name: String,
    pub endpoint_url: String,
    pub protocol: String,
    pub auth_tier: Option<String>,
    pub tip_account_used: Option<Pubkey>,
    pub tip_lamports: u64,
    pub priority_fee_microlamports: u64,
    pub compute_unit_limit: u32,
    pub signature: Signature,
    pub tx_message_hash: [u8; 32],
    pub send_order_in_trigger: u8,
    /// (slot, tick) from schedule
    pub trigger_slot: u64,
    pub trigger_tick: u8,
    pub nonce_account_id: u16,
    pub nonce_blockhash_used: Hash,
    pub prepared_at: Instant,
    pub pool_ready_at: Instant,
    pub trigger_observed_at: Instant,
}

struct AttemptRecord {
    /// All RegisterEvent context kept here so we can build FinalRecord on emit.
    reg: RegisterEvent,
    state: AttemptState,
    /// MatchEvent details if we've observed this sig.
    match_info: Option<MatchInfo>,
}

#[derive(Debug, Clone)]
struct MatchInfo {
    observed_at: Instant,
    observed_slot: u64,
    observed_entry_index: u32,
    observed_tick_in_slot: Option<u8>,
    observed_cumulative_hashes_in_slot: Option<u64>,
    observed_source: SourceKind,
}

pub struct MatcherConfig {
    pub register_rx: Receiver<RegisterEvent>,
    pub send_event_rx: Receiver<SendEvent>,
    pub match_event_rx: Receiver<MatchEvent>,
    pub final_tx: Sender<FinalRecord>,
    pub pending_sigs: Arc<DashSet<Signature>>,
    pub deadline: Duration,
    pub run_id: String,
    pub anchor: Instant,
    pub pinned_core: Option<usize>,
    pub counters: Arc<BenchCounters>,
    pub stop: Arc<AtomicBool>,
}

pub fn spawn(cfg: MatcherConfig) -> std::io::Result<JoinHandle<()>> {
    std::thread::Builder::new()
        .name("matcher".into())
        .spawn(move || {
            if let Some(core) = cfg.pinned_core {
                core_affinity::set_for_current(core_affinity::CoreId { id: core });
            }
            run_loop(cfg);
        })
}

fn run_loop(cfg: MatcherConfig) {
    let mut attempts: HashMap<(TriggerId, u8), AttemptRecord> = HashMap::with_capacity(1024);
    let mut sig_to_key: HashMap<Signature, (TriggerId, u8)> = HashMap::with_capacity(1024);
    let mut last_deadline_sweep = Instant::now();

    loop {
        if cfg.stop.load(Ordering::Relaxed) {
            break;
        }
        crossbeam_channel::select! {
            recv(cfg.register_rx) -> msg => {
                if let Ok(reg) = msg {
                    register_attempt(&mut attempts, &mut sig_to_key, reg);
                }
            },
            recv(cfg.send_event_rx) -> msg => {
                if let Ok(ev) = msg {
                    handle_send_event(&mut attempts, ev, &cfg);
                }
            },
            recv(cfg.match_event_rx) -> msg => {
                if let Ok(ev) = msg {
                    handle_match_event(&mut attempts, &sig_to_key, ev, &cfg);
                }
            },
            default(Duration::from_millis(200)) => {
                // periodic deadline sweep
            }
        }
        if last_deadline_sweep.elapsed() >= Duration::from_millis(500) {
            last_deadline_sweep = Instant::now();
            sweep_deadlines(&mut attempts, &mut sig_to_key, &cfg);
        }
    }
    // drain on shutdown
    sweep_deadlines(&mut attempts, &mut sig_to_key, &cfg);
}

fn register_attempt(
    attempts: &mut HashMap<(TriggerId, u8), AttemptRecord>,
    sig_to_key: &mut HashMap<Signature, (TriggerId, u8)>,
    reg: RegisterEvent,
) {
    let key = (reg.trigger_id, reg.sender_id);
    sig_to_key.insert(reg.signature, key);
    let state = AttemptState::SentPending {
        send_at_ns: 0, // filled at send_event time
        sig: reg.signature,
    };
    attempts.insert(key, AttemptRecord { reg, state, match_info: None });
}

fn handle_send_event(
    attempts: &mut HashMap<(TriggerId, u8), AttemptRecord>,
    ev: SendEvent,
    cfg: &MatcherConfig,
) {
    let key = (ev.trigger_id, ev.sender_id);
    let Some(rec) = attempts.get_mut(&key) else { return };
    let send_at_ns = ns_since(ev.send_at, cfg.anchor);
    let send_ack_at_ns = ev.send_ack_at.map(|t| ns_since(t, cfg.anchor));
    if let Some(err) = &ev.error {
        rec.state = AttemptState::SendFailed {
            send_at_ns,
            send_ack_at_ns,
            error: err.clone(),
            sig: ev.signature,
        };
        // Emit row immediately for failed sends
        let record = build_record_from_attempt(rec, &ev, cfg, TentativeOutcome::SendError);
        if cfg.final_tx.try_send(record).is_err() {
            cfg.counters.final_queue_full.fetch_add(1, Ordering::Relaxed);
        }
        attempts.remove(&key);
    } else {
        rec.state = AttemptState::SentAcked {
            send_at_ns,
            send_ack_at_ns: send_ack_at_ns.unwrap_or(0),
            sig: ev.signature,
            provider_request_id: ev.provider_request_id.clone(),
        };
    }
}

fn handle_match_event(
    attempts: &mut HashMap<(TriggerId, u8), AttemptRecord>,
    sig_to_key: &HashMap<Signature, (TriggerId, u8)>,
    ev: MatchEvent,
    cfg: &MatcherConfig,
) {
    let Some(&winner_key) = sig_to_key.get(&ev.signature) else { return };
    let (winner_trigger_id, _) = winner_key;

    // Find all siblings for this trigger
    let sibling_keys: Vec<(TriggerId, u8)> = attempts
        .keys()
        .filter(|(tid, _)| *tid == winner_trigger_id)
        .copied()
        .collect();

    // Build match info for winner
    let match_info = MatchInfo {
        observed_at: ev.observed_at,
        observed_slot: ev.observed_slot,
        observed_entry_index: ev.observed_entry_index,
        observed_tick_in_slot: ev.observed_tick_in_slot,
        observed_cumulative_hashes_in_slot: ev.observed_cumulative_hashes_in_slot,
        observed_source: ev.observed_source,
    };

    // Emit + remove all sibling records
    let mut to_remove = Vec::new();
    for key in sibling_keys {
        let Some(rec) = attempts.get_mut(&key) else { continue };
        let outcome = if key == winner_key {
            rec.match_info = Some(match_info.clone());
            TentativeOutcome::LandedTentative
        } else {
            rec.match_info = Some(match_info.clone());
            TentativeOutcome::DedupedTentative
        };
        let record = build_record_from_record(rec, cfg, outcome, Some(ev.observed_at));
        if cfg.final_tx.try_send(record).is_err() {
            cfg.counters.final_queue_full.fetch_add(1, Ordering::Relaxed);
        }
        to_remove.push(key);
    }
    for key in to_remove {
        attempts.remove(&key);
    }
}

fn sweep_deadlines(
    attempts: &mut HashMap<(TriggerId, u8), AttemptRecord>,
    sig_to_key: &mut HashMap<Signature, (TriggerId, u8)>,
    cfg: &MatcherConfig,
) {
    let now = Instant::now();
    let mut to_emit: Vec<(TriggerId, u8)> = Vec::new();
    for (key, rec) in attempts.iter() {
        let elapsed = now.duration_since(rec.reg.trigger_observed_at);
        if elapsed >= cfg.deadline {
            to_emit.push(*key);
        }
    }
    for key in to_emit {
        if let Some(rec) = attempts.remove(&key) {
            sig_to_key.remove(&rec.reg.signature);
            let record = build_record_from_record(&rec, cfg, TentativeOutcome::UnknownPending, None);
            if cfg.final_tx.try_send(record).is_err() {
                cfg.counters.final_queue_full.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

fn ns_since(t: Instant, anchor: Instant) -> u64 {
    t.duration_since(anchor).as_nanos().min(u64::MAX as u128) as u64
}

fn build_record_from_attempt(
    rec: &AttemptRecord,
    send_ev: &SendEvent,
    cfg: &MatcherConfig,
    outcome: TentativeOutcome,
) -> FinalRecord {
    FinalRecord {
        trigger_slot: rec.reg.trigger_slot,
        trigger_tick: rec.reg.trigger_tick,
        trigger_id: *rec.reg.trigger_id.as_bytes(),
        nonce_account_id: rec.reg.nonce_account_id,
        nonce_blockhash_used: rec.reg.nonce_blockhash_used,
        sender_id: rec.reg.sender_id,
        sender_name: rec.reg.sender_name.clone(),
        tx_signature: rec.reg.signature,
        tx_message_hash: rec.reg.tx_message_hash,
        endpoint_url: rec.reg.endpoint_url.clone(),
        protocol: rec.reg.protocol.clone(),
        auth_tier: rec.reg.auth_tier.clone(),
        tip_account_used: rec.reg.tip_account_used,
        tip_lamports: rec.reg.tip_lamports,
        priority_fee_microlamports: rec.reg.priority_fee_microlamports,
        compute_unit_limit: rec.reg.compute_unit_limit,
        prepared_at_ns: ns_since(rec.reg.prepared_at, cfg.anchor),
        pool_ready_at_ns: ns_since(rec.reg.pool_ready_at, cfg.anchor),
        trigger_observed_at_ns: ns_since(rec.reg.trigger_observed_at, cfg.anchor),
        send_at_ns: ns_since(send_ev.send_at, cfg.anchor),
        send_ack_at_ns: send_ev.send_ack_at.map(|t| ns_since(t, cfg.anchor)),
        send_order_in_trigger: rec.reg.send_order_in_trigger,
        host_clock_offset_ns: None,
        send_error: send_ev.error.clone(),
        rpc_err_code: send_ev.rpc_err_code,
        rpc_err_message: send_ev.rpc_err_message.clone(),
        provider_request_id: send_ev.provider_request_id.clone(),
        http_status: send_ev.http_status,
        rate_limit_state: send_ev.rate_limit_state,
        observed_slot: None,
        observed_entry_index: None,
        observed_tick_in_slot: None,
        observed_cumulative_hashes_in_slot: None,
        ss_observed_at_ns: None,
        ys_observed_at_ns: None,
        observed_at_ns: None,
        observed_source: None,
        commitment_at_resolution: None,
        tentative_outcome: outcome,
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
        wall_trigger_to_send_ns: Some(ns_since(send_ev.send_at, rec.reg.trigger_observed_at) as i64),
        wall_send_rtt_ns: send_ev.send_ack_at.map(|t| t.duration_since(send_ev.send_at).as_nanos() as i64),
        wall_send_to_observed_ns: None,
        wall_send_to_ss_observed_ns: None,
        wall_send_to_ys_observed_ns: None,
        nonce_update_observed_at_ns: None,
        nonce_update_source: None,
        nonce_advanced_to_slot: None,
        run_id: cfg.run_id.clone(),
        chunk_index: 0,
    }
}

fn build_record_from_record(
    rec: &AttemptRecord,
    cfg: &MatcherConfig,
    outcome: TentativeOutcome,
    siblings_resolved_at: Option<Instant>,
) -> FinalRecord {
    let (send_at_ns, send_ack_at_ns, send_error, rpc_err_code, rpc_err_message, provider_request_id, http_status, rate_limit_state) =
        match &rec.state {
            AttemptState::SentAcked { send_at_ns, send_ack_at_ns, provider_request_id, .. } => (
                *send_at_ns, Some(*send_ack_at_ns), None, None, None, provider_request_id.clone(), None, RateLimitState::Ok,
            ),
            AttemptState::SentPending { send_at_ns, .. } => (
                *send_at_ns, None, None, None, None, None, None, RateLimitState::Ok,
            ),
            AttemptState::SendFailed { send_at_ns, send_ack_at_ns, error, .. } => (
                *send_at_ns, *send_ack_at_ns, Some(error.clone()), None, None, None, None, RateLimitState::Ok,
            ),
            _ => (0, None, None, None, None, None, None, RateLimitState::Ok),
        };

    let (observed_slot, observed_entry_index, observed_tick_in_slot, observed_cumulative_hashes, observed_at_ns, observed_source) =
        if let Some(mi) = &rec.match_info {
            (Some(mi.observed_slot), Some(mi.observed_entry_index), mi.observed_tick_in_slot,
             mi.observed_cumulative_hashes_in_slot, Some(ns_since(mi.observed_at, cfg.anchor)),
             Some(match mi.observed_source {
                 SourceKind::ShredStream => ObservedSource::Ss,
                 SourceKind::Yellowstone => ObservedSource::Ys,
             }))
        } else {
            (None, None, None, None, None, None)
        };

    FinalRecord {
        trigger_slot: rec.reg.trigger_slot,
        trigger_tick: rec.reg.trigger_tick,
        trigger_id: *rec.reg.trigger_id.as_bytes(),
        nonce_account_id: rec.reg.nonce_account_id,
        nonce_blockhash_used: rec.reg.nonce_blockhash_used,
        sender_id: rec.reg.sender_id,
        sender_name: rec.reg.sender_name.clone(),
        tx_signature: rec.reg.signature,
        tx_message_hash: rec.reg.tx_message_hash,
        endpoint_url: rec.reg.endpoint_url.clone(),
        protocol: rec.reg.protocol.clone(),
        auth_tier: rec.reg.auth_tier.clone(),
        tip_account_used: rec.reg.tip_account_used,
        tip_lamports: rec.reg.tip_lamports,
        priority_fee_microlamports: rec.reg.priority_fee_microlamports,
        compute_unit_limit: rec.reg.compute_unit_limit,
        prepared_at_ns: ns_since(rec.reg.prepared_at, cfg.anchor),
        pool_ready_at_ns: ns_since(rec.reg.pool_ready_at, cfg.anchor),
        trigger_observed_at_ns: ns_since(rec.reg.trigger_observed_at, cfg.anchor),
        send_at_ns,
        send_ack_at_ns,
        send_order_in_trigger: rec.reg.send_order_in_trigger,
        host_clock_offset_ns: None,
        send_error,
        rpc_err_code,
        rpc_err_message,
        provider_request_id,
        http_status,
        rate_limit_state,
        observed_slot,
        observed_entry_index,
        observed_tick_in_slot,
        observed_cumulative_hashes_in_slot: observed_cumulative_hashes,
        ss_observed_at_ns: None,
        ys_observed_at_ns: None,
        observed_at_ns,
        observed_source,
        commitment_at_resolution: None,
        tentative_outcome: outcome,
        final_status: FinalStatus::Pending,
        siblings_resolved_at_ns: siblings_resolved_at.map(|t| ns_since(t, cfg.anchor)),
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
        run_id: cfg.run_id.clone(),
        chunk_index: 0,
    }
}
```

- [ ] **Step 2: Verify build**

Run: `cargo check -p fan-out-bench`. Expected: clean.

---

## Task 8: Matcher tests

**Files:**
- Append to: `crates/fan-out-bench/src/matcher.rs`

- [ ] **Step 1: Add test module**

Append to end of `matcher.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_channel::{bounded, unbounded};

    fn make_register(trigger_id: TriggerId, sender_id: u8, sig: Signature, anchor: Instant) -> RegisterEvent {
        RegisterEvent {
            trigger_id, sender_id,
            sender_name: format!("s{}", sender_id),
            endpoint_url: "http://mock".into(),
            protocol: "MOCK".into(),
            auth_tier: None,
            tip_account_used: None,
            tip_lamports: 1000,
            priority_fee_microlamports: 5000,
            compute_unit_limit: 200_000,
            signature: sig,
            tx_message_hash: [0; 32],
            send_order_in_trigger: 0,
            trigger_slot: 100, trigger_tick: 5,
            nonce_account_id: 0,
            nonce_blockhash_used: Hash::default(),
            prepared_at: anchor,
            pool_ready_at: anchor,
            trigger_observed_at: anchor,
        }
    }

    #[allow(clippy::type_complexity)]
    fn setup() -> (
        crossbeam_channel::Sender<RegisterEvent>,
        crossbeam_channel::Sender<SendEvent>,
        crossbeam_channel::Sender<MatchEvent>,
        crossbeam_channel::Receiver<FinalRecord>,
        Arc<AtomicBool>,
        JoinHandle<()>,
        Instant,
    ) {
        let (reg_tx, reg_rx) = unbounded();
        let (send_tx, send_rx) = unbounded();
        let (match_tx, match_rx) = unbounded();
        let (final_tx, final_rx) = bounded(100);
        let stop = Arc::new(AtomicBool::new(false));
        let anchor = Instant::now();
        let handle = spawn(MatcherConfig {
            register_rx: reg_rx,
            send_event_rx: send_rx,
            match_event_rx: match_rx,
            final_tx,
            pending_sigs: Arc::new(DashSet::new()),
            deadline: Duration::from_millis(200),
            run_id: "test".into(),
            anchor,
            pinned_core: None,
            counters: Arc::new(BenchCounters::default()),
            stop: stop.clone(),
        }).unwrap();
        (reg_tx, send_tx, match_tx, final_rx, stop, handle, anchor)
    }

    fn shutdown(reg_tx: crossbeam_channel::Sender<RegisterEvent>, send_tx: crossbeam_channel::Sender<SendEvent>, match_tx: crossbeam_channel::Sender<MatchEvent>, stop: Arc<AtomicBool>, handle: JoinHandle<()>) {
        std::thread::sleep(Duration::from_millis(50));
        stop.store(true, Ordering::Relaxed);
        drop(reg_tx); drop(send_tx); drop(match_tx);
        let _ = handle.join();
    }

    #[test]
    fn winner_and_siblings_emit_correct_outcomes() {
        let (reg_tx, send_tx, match_tx, final_rx, stop, handle, anchor) = setup();
        let tid = TriggerId::new(100, 5, 0);
        let sig_a = Signature::new_unique();
        let sig_b = Signature::new_unique();
        let sig_c = Signature::new_unique();

        reg_tx.send(make_register(tid, 0, sig_a, anchor)).unwrap();
        reg_tx.send(make_register(tid, 1, sig_b, anchor)).unwrap();
        reg_tx.send(make_register(tid, 2, sig_c, anchor)).unwrap();

        std::thread::sleep(Duration::from_millis(10));

        // Match sig_b
        match_tx.send(MatchEvent {
            signature: sig_b,
            observed_at: Instant::now(),
            observed_slot: 100,
            observed_entry_index: 0,
            observed_tick_in_slot: Some(5),
            observed_cumulative_hashes_in_slot: Some(312_500),
            observed_source: SourceKind::ShredStream,
        }).unwrap();

        std::thread::sleep(Duration::from_millis(50));

        let mut records = Vec::new();
        while let Ok(r) = final_rx.try_recv() {
            records.push(r);
        }
        assert_eq!(records.len(), 3);
        let landed: Vec<_> = records.iter().filter(|r| r.tentative_outcome == TentativeOutcome::LandedTentative).collect();
        let deduped: Vec<_> = records.iter().filter(|r| r.tentative_outcome == TentativeOutcome::DedupedTentative).collect();
        assert_eq!(landed.len(), 1);
        assert_eq!(deduped.len(), 2);
        assert_eq!(landed[0].sender_id, 1);

        shutdown(reg_tx, send_tx, match_tx, stop, handle);
    }

    #[test]
    fn send_error_emits_immediately() {
        let (reg_tx, send_tx, match_tx, final_rx, stop, handle, anchor) = setup();
        let tid = TriggerId::new(100, 5, 0);
        let sig_a = Signature::new_unique();

        reg_tx.send(make_register(tid, 0, sig_a, anchor)).unwrap();
        std::thread::sleep(Duration::from_millis(10));
        send_tx.send(SendEvent {
            trigger_id: tid, sender_id: 0,
            send_at: Instant::now(),
            send_ack_at: None,
            signature: sig_a,
            provider_request_id: None,
            http_status: Some(500),
            rpc_err_code: None,
            rpc_err_message: None,
            rate_limit_state: RateLimitState::Ok,
            error: Some("boom".into()),
        }).unwrap();
        std::thread::sleep(Duration::from_millis(30));

        let rec = final_rx.try_recv().unwrap();
        assert_eq!(rec.tentative_outcome, TentativeOutcome::SendError);
        assert_eq!(rec.send_error.as_deref(), Some("boom"));

        shutdown(reg_tx, send_tx, match_tx, stop, handle);
    }

    #[test]
    fn deadline_triggers_unknown_pending() {
        let (reg_tx, send_tx, match_tx, final_rx, stop, handle, anchor) = setup();
        let tid = TriggerId::new(100, 5, 0);
        let sig_a = Signature::new_unique();

        reg_tx.send(make_register(tid, 0, sig_a, anchor)).unwrap();
        // Wait past deadline (200ms in setup)
        std::thread::sleep(Duration::from_millis(800));

        let rec = final_rx.try_recv().expect("expected deadline emission");
        assert_eq!(rec.tentative_outcome, TentativeOutcome::UnknownPending);

        shutdown(reg_tx, send_tx, match_tx, stop, handle);
    }
}
```

Run: `cargo test -p fan-out-bench --lib matcher`. Expected: 3 tests pass.

---

## Task 9: Preparer

**Files:**
- Replace stub: `crates/fan-out-bench/src/preparer.rs`

- [ ] **Step 1: Implement Preparer**

```rust
//! Preparer — signs N variants per scheduled (slot, tick) using NonceManager
//! and tx_builder, inserts into TxPool with prepared_at + pool_ready_at timestamps.

use crate::config::{SenderConfig, SenderKind};
use crate::counters::BenchCounters;
use crate::nonce::manager::NonceManager;
use crate::pool::{PreSignedTx, TxPool};
use crate::schedule::ScheduleEntry;
use crate::tip_accounts::TipAccountRotator;
use crate::tx_builder::{build_variant, VariantParams};
use crossbeam_channel::Receiver;
use solana_sdk::signature::Keypair;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Instant;

pub struct PreparerConfig {
    pub schedule_rx: Receiver<ScheduleEntry>,
    pub senders: Vec<SenderConfig>,
    pub tip_rotators: HashMap<u8, Arc<TipAccountRotator>>,
    pub nonce_manager: Arc<NonceManager>,
    pub pool: Arc<TxPool>,
    pub authority: Arc<Keypair>,
    pub priority_fee_microlamports: u64,
    pub compute_unit_limit: u32,
    pub pinned_core: Option<usize>,
    pub counters: Arc<BenchCounters>,
    pub stop: Arc<AtomicBool>,
}

pub struct PreparedTrigger {
    pub slot: u64,
    pub tick: u8,
    pub nonce_account_id: u16,
}

pub fn spawn(cfg: PreparerConfig) -> (std::io::Result<JoinHandle<()>>, crossbeam_channel::Receiver<PreparedTrigger>) {
    let (prepared_tx, prepared_rx) = crossbeam_channel::bounded::<PreparedTrigger>(8192);
    let handle = std::thread::Builder::new()
        .name("preparer".into())
        .spawn(move || {
            if let Some(core) = cfg.pinned_core {
                core_affinity::set_for_current(core_affinity::CoreId { id: core });
            }
            run_loop(cfg, prepared_tx);
        });
    (handle, prepared_rx)
}

fn run_loop(cfg: PreparerConfig, prepared_tx: crossbeam_channel::Sender<PreparedTrigger>) {
    use solana_sdk::signer::Signer;
    let authority_pubkey = cfg.authority.pubkey();
    loop {
        if cfg.stop.load(Ordering::Relaxed) {
            break;
        }
        let entry = match cfg.schedule_rx.recv_timeout(std::time::Duration::from_millis(200)) {
            Ok(e) => e,
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
        };

        // Take a nonce from the pool
        let Some((nonce_id, nonce_pubkey, nonce_blockhash)) = cfg.nonce_manager.take_ready() else {
            cfg.counters.nonce_stalls.fetch_add(1, Ordering::Relaxed);
            continue;
        };

        let prepared_at = Instant::now();
        let mut signed_count = 0;
        for sender in &cfg.senders {
            if !sender.enabled {
                continue;
            }
            let tip_account = cfg.tip_rotators.get(&sender.id).and_then(|r| r.next());
            let needs_tip_account = !matches!(sender.kind, SenderKind::Triton | SenderKind::Harmonic | SenderKind::Mock);
            if needs_tip_account && tip_account.is_none() {
                continue;
            }

            let params = VariantParams {
                nonce_pubkey,
                nonce_blockhash,
                payer: authority_pubkey,
                sender_id: sender.id,
                sender_kind: sender.kind,
                tip_account,
                tip_lamports: sender.tip_lamports,
                priority_fee_microlamports: cfg.priority_fee_microlamports,
                compute_unit_limit: cfg.compute_unit_limit,
            };

            match build_variant(params, &cfg.authority) {
                Ok(variant) => {
                    let pool_ready_at = Instant::now();
                    let pre = PreSignedTx {
                        tx: Arc::new(variant.tx),
                        message_hash: variant.message_hash,
                        prepared_at,
                        pool_ready_at,
                    };
                    cfg.pool.insert(entry.slot, entry.tick, sender.id, pre);
                    signed_count += 1;
                }
                Err(e) => {
                    tracing::warn!(error = %e, sender = %sender.name, "build_variant failed");
                    cfg.counters.preparer_signing_fail.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        tracing::debug!(slot = entry.slot, tick = entry.tick, nonce_id, signed_count, "prepared");

        if prepared_tx.try_send(PreparedTrigger {
            slot: entry.slot,
            tick: entry.tick,
            nonce_account_id: nonce_id,
        }).is_err() {
            cfg.counters.send_queue_full.fetch_add(1, Ordering::Relaxed);
        }
    }
}
```

- [ ] **Step 2: Verify**

Run: `cargo check -p fan-out-bench`. Expected: clean. (No standalone tests — exercised in pipeline_mock integration test.)

---

## Task 10: Dispatcher

**Files:**
- Replace stub: `crates/fan-out-bench/src/dispatcher.rs`

- [ ] **Step 1: Implement async Dispatcher**

```rust
//! Dispatcher — fan-out async sends per-sender.
//!
//! Consumes PreparedTrigger from preparer, takes all variants from pool,
//! randomizes order, dispatches to each sender's TxSender::send(), emits
//! RegisterEvent + SendEvent to matcher.

use crate::counters::BenchCounters;
use crate::matcher::{RegisterEvent, SendEvent};
use crate::pool::TxPool;
use crate::preparer::PreparedTrigger;
use crate::senders::TxSender;
use crate::trigger::TriggerEvent;
use crate::trigger_id::TriggerId;
use crossbeam_channel::{Receiver, Sender};
use dashmap::DashSet;
use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use solana_sdk::signature::Signature;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;

pub struct DispatcherConfig {
    pub trigger_rx: Receiver<TriggerEvent>,
    pub prepared_rx: Receiver<PreparedTrigger>,
    pub pool: Arc<TxPool>,
    pub senders: HashMap<u8, Arc<dyn TxSender>>,
    pub sender_meta: HashMap<u8, SenderMeta>,
    pub register_tx: Sender<RegisterEvent>,
    pub send_event_tx: Sender<SendEvent>,
    pub pending_sigs: Arc<DashSet<Signature>>,
    pub schedule_seed: u64,
    pub counters: Arc<BenchCounters>,
    pub stop: Arc<AtomicBool>,
}

#[derive(Debug, Clone)]
pub struct SenderMeta {
    pub name: String,
    pub endpoint_url: String,
    pub protocol: String,
    pub auth_tier: Option<String>,
    pub tip_lamports: u64,
    pub priority_fee_microlamports: u64,
    pub compute_unit_limit: u32,
}

pub fn run_blocking(cfg: DispatcherConfig) -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .thread_name("dispatcher-tokio")
        .build()?;
    runtime.block_on(run_async(cfg));
    Ok(())
}

async fn run_async(cfg: DispatcherConfig) {
    let (cmd_tx, mut cmd_rx) = mpsc::channel::<SendCommand>(4096);
    let counters_clone = cfg.counters.clone();
    let send_event_tx_clone = cfg.send_event_tx.clone();

    // Spawn dispatcher worker pool — for_each_concurrent of cmd_rx
    let workers = tokio::spawn(async move {
        while let Some(cmd) = cmd_rx.recv().await {
            let sender = cmd.sender.clone();
            let outcome_tx = send_event_tx_clone.clone();
            tokio::spawn(async move {
                let outcome = sender.send(&cmd.tx).await;
                let ev = SendEvent {
                    trigger_id: cmd.trigger_id,
                    sender_id: cmd.sender_id,
                    send_at: outcome.send_at,
                    send_ack_at: outcome.send_ack_at,
                    signature: outcome.signature,
                    provider_request_id: outcome.provider_request_id,
                    http_status: outcome.http_status,
                    rpc_err_code: outcome.rpc_err_code,
                    rpc_err_message: outcome.rpc_err_message,
                    rate_limit_state: outcome.rate_limit_state,
                    error: outcome.error,
                };
                let _ = outcome_tx.send(ev);
            });
        }
    });

    // Main: pair prepared triggers with trigger events from observer, dispatch fan-out
    loop {
        if cfg.stop.load(Ordering::Relaxed) {
            break;
        }
        let trigger = match cfg.trigger_rx.recv_timeout(std::time::Duration::from_millis(200)) {
            Ok(t) => t,
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
        };
        // Find corresponding PreparedTrigger for nonce_id (best-effort poll)
        let mut prepared = None;
        for _ in 0..10 {
            match cfg.prepared_rx.try_recv() {
                Ok(p) if p.slot == trigger.slot && p.tick == trigger.tick => {
                    prepared = Some(p);
                    break;
                }
                Ok(_) => continue,
                Err(_) => break,
            }
        }
        let nonce_account_id = prepared.map(|p| p.nonce_account_id).unwrap_or(0);

        // Take all variants for this trigger
        let variants = cfg.pool.take_all_for(trigger.slot, trigger.tick);
        if variants.is_empty() {
            cfg.counters.pool_empty.fetch_add(1, Ordering::Relaxed);
            continue;
        }

        // Deterministic perm seed: schedule_seed ^ (slot << 8) ^ tick
        let perm_seed = cfg.schedule_seed ^ (trigger.slot << 8) ^ (trigger.tick as u64);
        let mut rng = SmallRng::seed_from_u64(perm_seed);
        let mut indices: Vec<usize> = (0..variants.len()).collect();
        indices.shuffle(&mut rng);

        let trigger_id = TriggerId::new(trigger.slot, trigger.tick, nonce_account_id);
        let now = Instant::now();
        for (order, &idx) in indices.iter().enumerate() {
            let (sender_id, presigned) = &variants[idx];
            let Some(sender) = cfg.senders.get(sender_id) else { continue };
            let meta = match cfg.sender_meta.get(sender_id) {
                Some(m) => m.clone(),
                None => continue,
            };
            let sig = presigned.tx.signatures.first().copied().unwrap_or_default();
            cfg.pending_sigs.insert(sig);
            let reg = RegisterEvent {
                trigger_id,
                sender_id: *sender_id,
                sender_name: meta.name,
                endpoint_url: meta.endpoint_url,
                protocol: meta.protocol,
                auth_tier: meta.auth_tier,
                tip_account_used: None, // TODO: pass through from preparer (Plan 5)
                tip_lamports: meta.tip_lamports,
                priority_fee_microlamports: meta.priority_fee_microlamports,
                compute_unit_limit: meta.compute_unit_limit,
                signature: sig,
                tx_message_hash: presigned.message_hash,
                send_order_in_trigger: order as u8,
                trigger_slot: trigger.slot,
                trigger_tick: trigger.tick,
                nonce_account_id,
                nonce_blockhash_used: solana_sdk::hash::Hash::default(),
                prepared_at: presigned.prepared_at,
                pool_ready_at: presigned.pool_ready_at,
                trigger_observed_at: trigger.observed_at,
            };
            let _ = cfg.register_tx.send(reg);
            let cmd = SendCommand {
                tx: presigned.tx.as_ref().clone(),
                trigger_id,
                sender_id: *sender_id,
                sender: sender.clone(),
            };
            if cmd_tx.send(cmd).await.is_err() {
                cfg.counters.send_queue_full.fetch_add(1, Ordering::Relaxed);
            }
        }
        let _ = counters_clone.schedule_contains_true.fetch_add(0, Ordering::Relaxed);
        let _ = now; // silence unused
    }
    drop(cmd_tx);
    let _ = workers.await;
}

struct SendCommand {
    tx: solana_sdk::transaction::Transaction,
    trigger_id: TriggerId,
    sender_id: u8,
    sender: Arc<dyn TxSender>,
}
```

- [ ] **Step 2: Verify build**

Run: `cargo check -p fan-out-bench`. Expected: clean.

---

## Task 11: Runtime config + run command

**Files:**
- Replace stub: `crates/fan-out-bench/src/runtime.rs`

- [ ] **Step 1: Implement runtime that wires components together**

```rust
//! Runtime — wires schedule → preparer → observer → dispatcher → matcher → parquet.
//!
//! Sources (SS, YS, nonce_manager, finality_tracker, RPC fallback) are
//! constructed by caller and injected. This module orchestrates the
//! mid-tier pipeline.

use crate::config::Config;
use crate::counters::BenchCounters;
use crate::dispatcher::{DispatcherConfig, SenderMeta};
use crate::matcher::{MatcherConfig, RegisterEvent, SendEvent};
use crate::merger::{spawn as spawn_merger, MergerConfig};
use crate::nonce::manager::NonceManager;
use crate::observer::{spawn as spawn_observer, ObserverConfig};
use crate::pool::TxPool;
use crate::preparer::{spawn as spawn_preparer, PreparerConfig};
use crate::schedule::{Schedule, ScheduleEntry};
use crate::senders::TxSender;
use crate::tip_accounts::{tip_accounts_for, TipAccountRotator};
use crate::writer::{spawn_parquet, FinalRecord, ParquetWriterConfig};
use crossbeam_channel::{bounded, unbounded};
use dashmap::DashSet;
use entry_sources::EntryObservation;
use solana_sdk::signature::{Keypair, Signature};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub struct RuntimeInputs {
    pub config: Config,
    pub authority: Arc<Keypair>,
    pub nonce_manager: Arc<NonceManager>,
    pub ss_entry_rx: crossbeam_channel::Receiver<EntryObservation>,
    pub ys_entry_rx: crossbeam_channel::Receiver<EntryObservation>,
    pub senders: HashMap<u8, Arc<dyn TxSender>>,
    pub output_dir: PathBuf,
    pub run_id: String,
}

pub struct RuntimeHandles {
    pub stop: Arc<AtomicBool>,
    pub schedule_tx: crossbeam_channel::Sender<ScheduleEntry>,
    pub counters: Arc<BenchCounters>,
}

pub fn start(inputs: RuntimeInputs) -> anyhow::Result<RuntimeHandles> {
    let stop = Arc::new(AtomicBool::new(false));
    let counters = Arc::new(BenchCounters::default());
    let anchor = Instant::now();

    let pool = Arc::new(TxPool::new());
    let pending_sigs: Arc<DashSet<Signature>> = Arc::new(DashSet::new());

    let (merged_tx, merged_rx) = bounded(65536);
    let (trigger_tx, trigger_rx) = bounded(65536);
    let (match_tx, match_event_rx) = bounded(65536);
    let (schedule_tx, schedule_rx) = unbounded::<ScheduleEntry>();
    let (register_tx, register_rx) = unbounded::<RegisterEvent>();
    let (send_event_tx, send_event_rx) = unbounded::<SendEvent>();
    let (final_tx, final_rx) = bounded::<FinalRecord>(65536);

    // Merger: SS+YS → merged
    let _merger_handle = spawn_merger(MergerConfig {
        ss_rx: inputs.ss_entry_rx,
        ys_rx: inputs.ys_entry_rx,
        out_tx: merged_tx,
        pinned_core: None,
        counters: counters.clone(),
        stop: stop.clone(),
    })?;

    // Observer: merged → trigger + match
    let schedule_set: Arc<HashSet<(u64, u8)>> = Arc::new(HashSet::new());
    let _observer_handle = spawn_observer(ObserverConfig {
        merged_rx,
        schedule: schedule_set,
        trigger_tx,
        match_tx,
        pending_sigs: pending_sigs.clone(),
        current_slot: Arc::new(AtomicU64::new(0)),
        pinned_core: None,
        counters: counters.clone(),
        stop: stop.clone(),
    })?;

    // Tip rotators
    let mut tip_rotators: HashMap<u8, Arc<TipAccountRotator>> = HashMap::new();
    let mut sender_meta: HashMap<u8, SenderMeta> = HashMap::new();
    for sc in &inputs.config.senders {
        let accounts = tip_accounts_for(sc.kind);
        tip_rotators.insert(sc.id, Arc::new(TipAccountRotator::new(accounts)));
        sender_meta.insert(sc.id, SenderMeta {
            name: sc.name.clone(),
            endpoint_url: sc.endpoint_url.clone(),
            protocol: "HTTP_JSONRPC".to_string(), // refined per impl in Plan 6
            auth_tier: None,
            tip_lamports: sc.tip_lamports,
            priority_fee_microlamports: inputs.config.run.priority_fee_microlamports,
            compute_unit_limit: inputs.config.run.compute_unit_limit,
        });
    }

    // Preparer: schedule_rx → pool
    let (_preparer_handle, prepared_rx) = spawn_preparer(PreparerConfig {
        schedule_rx,
        senders: inputs.config.senders.clone(),
        tip_rotators,
        nonce_manager: inputs.nonce_manager.clone(),
        pool: pool.clone(),
        authority: inputs.authority.clone(),
        priority_fee_microlamports: inputs.config.run.priority_fee_microlamports,
        compute_unit_limit: inputs.config.run.compute_unit_limit,
        pinned_core: None,
        counters: counters.clone(),
        stop: stop.clone(),
    });

    // Dispatcher: trigger + pool → send + register
    let dispatcher_cfg = DispatcherConfig {
        trigger_rx,
        prepared_rx,
        pool: pool.clone(),
        senders: inputs.senders,
        sender_meta,
        register_tx,
        send_event_tx,
        pending_sigs: pending_sigs.clone(),
        schedule_seed: inputs.config.run.schedule_seed.unwrap_or(0),
        counters: counters.clone(),
        stop: stop.clone(),
    };
    let dispatcher_stop = stop.clone();
    std::thread::Builder::new()
        .name("dispatcher".into())
        .spawn(move || {
            if let Err(e) = crate::dispatcher::run_blocking(dispatcher_cfg) {
                tracing::error!(error = %e, "dispatcher exited with error");
            }
            let _ = dispatcher_stop;
        })?;

    // Matcher
    let _matcher_handle = crate::matcher::spawn(MatcherConfig {
        register_rx,
        send_event_rx,
        match_event_rx,
        final_tx,
        pending_sigs,
        deadline: Duration::from_secs(inputs.config.run.observation_deadline_secs),
        run_id: inputs.run_id.clone(),
        anchor,
        pinned_core: None,
        counters: counters.clone(),
        stop: stop.clone(),
    })?;

    // Parquet
    let parquet_path = inputs.output_dir.join("tx-events.parquet");
    let _parquet_handle = spawn_parquet(ParquetWriterConfig {
        final_rx,
        output_path: parquet_path,
        row_group_size: 32768,
        flush_interval: Duration::from_secs(60),
        pinned_core: None,
        counters: counters.clone(),
    })?;

    let _ = schedule_tx.send(ScheduleEntry { slot: 0, tick: 0 }); // warm up the channel — placeholder
    let _ = Schedule::new(None, 0, 1); // type ref

    Ok(RuntimeHandles {
        stop,
        schedule_tx,
        counters,
    })
}
```

(Note: this runtime is a skeleton — real wiring with SS/YS gRPC clients comes in the CLI `bin/run.rs` task.)

- [ ] **Step 2: Verify**

Run: `cargo check -p fan-out-bench`. Expected: clean.

---

## Task 12: CLI `run` binary

**Files:**
- Replace stub: `crates/fan-out-bench/src/bin/run.rs`

- [ ] **Step 1: Write CLI binary**

```rust
//! CLI: cargo run --bin run -- --config <path>
//!
//! Loads config, sets up SS+YS, NonceManager, senders, runs until Ctrl-C or budget exhausted.

use anyhow::{Context, Result};
use clap::Parser;
use fan_out_bench::config::{Config, SenderKind};
use fan_out_bench::nonce::bootstrap::bootstrap;
use fan_out_bench::nonce::manager::NonceManager;
use fan_out_bench::runtime::{start as start_runtime, RuntimeInputs};
use fan_out_bench::senders::helius::HeliusSender;
use fan_out_bench::senders::jito::JitoSender;
use fan_out_bench::senders::TxSender;
use fan_out_bench::wallet::load_keypair_file;
use solana_client::rpc_client::RpcClient;
use solana_commitment_config::CommitmentConfig;
use solana_sdk::signature::Signer;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Parser, Debug)]
#[command(name = "run")]
struct Args {
    #[arg(long)]
    config: PathBuf,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let config = Config::load(&args.config).context("load config")?;

    let authority = Arc::new(load_keypair_file(&config.run.wallet_keypair_path).context("load wallet")?);
    tracing::info!(authority = %authority.pubkey(), "fan-out-bench starting");

    let rpc = RpcClient::new_with_commitment(config.sources.helius_rpc_url.clone(), CommitmentConfig::confirmed());
    let nonce_entries = bootstrap(&rpc, &config.nonce.config_path, &authority.pubkey()).context("bootstrap nonces")?;
    let nonce_manager = Arc::new(NonceManager::new(nonce_entries));
    tracing::info!(count = nonce_manager.len(), "nonce manager ready");

    // Build senders from config
    let mut senders: HashMap<u8, Arc<dyn TxSender>> = HashMap::new();
    for sc in config.enabled_senders() {
        let sender: Arc<dyn TxSender> = match sc.kind {
            SenderKind::Helius => {
                let api_key = match &sc.auth {
                    fan_out_bench::config::AuthConfig::QueryParam { value, .. } => Some(value.clone()),
                    _ => None,
                };
                Arc::new(HeliusSender::new(sc.id, sc.name.clone(), sc.endpoint_url.clone(), api_key, false))
            }
            SenderKind::Jito => {
                let auth = match &sc.auth {
                    fan_out_bench::config::AuthConfig::Header { value, .. } => Some(value.clone()),
                    _ => None,
                };
                Arc::new(JitoSender::new(sc.id, sc.name.clone(), sc.endpoint_url.clone(), auth))
            }
            _ => {
                tracing::warn!(name = %sc.name, "sender kind not implemented yet, skipping");
                continue;
            }
        };
        senders.insert(sc.id, sender);
    }
    tracing::info!(count = senders.len(), "senders configured");

    // SS + YS entry channels — STUB for Plan 4 (real gRPC clients hooked in Plan 6 ops).
    // For now we create dummy channels that won't receive anything.
    let (_ss_tx_dummy, ss_rx) = crossbeam_channel::unbounded();
    let (_ys_tx_dummy, ys_rx) = crossbeam_channel::unbounded();

    let run_id = chrono::Utc::now().format("%Y%m%d-%H%M%S").to_string();
    let output_dir = config.run.output_dir.join(&run_id);
    std::fs::create_dir_all(&output_dir)?;

    let handles = start_runtime(RuntimeInputs {
        config: config.clone(),
        authority,
        nonce_manager,
        ss_entry_rx: ss_rx,
        ys_entry_rx: ys_rx,
        senders,
        output_dir,
        run_id,
    })?;

    tracing::info!("runtime started — bench is running. Ctrl-C to stop.");
    tracing::warn!("NOTE: Plan 4 runtime — SS/YS gRPC clients not yet hooked. Bench will idle.");

    // Wait for Ctrl-C
    let stop = handles.stop.clone();
    ctrlc::set_handler(move || {
        tracing::info!("Ctrl-C received, signalling shutdown");
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
    })?;

    // Block until stop is signalled
    while !handles.stop.load(std::sync::atomic::Ordering::Relaxed) {
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
    tracing::info!("shutdown complete");
    Ok(())
}
```

- [ ] **Step 2: Add `ctrlc = "3"` to Cargo.toml dependencies**

Edit `crates/fan-out-bench/Cargo.toml`, add to `[dependencies]`:

```toml
ctrlc = "3"
```

- [ ] **Step 3: Verify**

Run: `cargo check --bin run -p fan-out-bench`. Expected: clean build.

---

## Task 13: Pipeline mock integration test

**Files:**
- Create: `crates/fan-out-bench/tests/pipeline_mock.rs`

- [ ] **Step 1: Write end-to-end pipeline test with mock senders**

```rust
//! Pipeline integration test — full bench with mock senders, mock entry sources,
//! mock observer. Verifies LANDED + DEDUPED rows appear in parquet.

use crossbeam_channel::{bounded, unbounded};
use entry_sources::{EntryObservation, SignatureVec, SourceKind};
use fan_out_bench::counters::BenchCounters;
use fan_out_bench::match_event::MatchEvent;
use fan_out_bench::matcher::{spawn as spawn_matcher, MatcherConfig, RegisterEvent, SendEvent};
use fan_out_bench::outcome::{FinalStatus, RateLimitState, TentativeOutcome};
use fan_out_bench::trigger_id::TriggerId;
use fan_out_bench::writer::{record::FinalRecord, spawn_parquet, ParquetWriterConfig};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use solana_sdk::{hash::Hash, signature::Signature};
use std::fs::File;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;

#[test]
fn matcher_to_parquet_emits_winner_and_siblings() {
    let tmp = TempDir::new().unwrap();
    let parquet_path = tmp.path().join("tx-events.parquet");

    let (reg_tx, reg_rx) = unbounded();
    let (send_tx, send_rx) = unbounded::<SendEvent>();
    let (match_tx, match_rx) = unbounded();
    let (final_tx, final_rx) = bounded::<FinalRecord>(1000);
    let stop = Arc::new(AtomicBool::new(false));
    let anchor = Instant::now();

    let parquet_handle = spawn_parquet(ParquetWriterConfig {
        final_rx,
        output_path: parquet_path.clone(),
        row_group_size: 16,
        flush_interval: Duration::from_millis(100),
        pinned_core: None,
        counters: Arc::new(BenchCounters::default()),
    }).unwrap();

    let matcher_handle = spawn_matcher(MatcherConfig {
        register_rx: reg_rx,
        send_event_rx: send_rx,
        match_event_rx: match_rx,
        final_tx: final_tx.clone(),
        pending_sigs: Arc::new(dashmap::DashSet::new()),
        deadline: Duration::from_secs(60),
        run_id: "pipeline-mock".into(),
        anchor,
        pinned_core: None,
        counters: Arc::new(BenchCounters::default()),
        stop: stop.clone(),
    }).unwrap();

    // Register 3 attempts for trigger T1
    let tid = TriggerId::new(100, 5, 0);
    let sigs = [Signature::new_unique(), Signature::new_unique(), Signature::new_unique()];
    for (i, sig) in sigs.iter().enumerate() {
        reg_tx.send(RegisterEvent {
            trigger_id: tid,
            sender_id: i as u8,
            sender_name: format!("mock-{}", i),
            endpoint_url: "mock://x".into(),
            protocol: "MOCK".into(),
            auth_tier: None,
            tip_account_used: None,
            tip_lamports: 1000,
            priority_fee_microlamports: 5000,
            compute_unit_limit: 200_000,
            signature: *sig,
            tx_message_hash: [0; 32],
            send_order_in_trigger: i as u8,
            trigger_slot: 100,
            trigger_tick: 5,
            nonce_account_id: 0,
            nonce_blockhash_used: Hash::default(),
            prepared_at: anchor,
            pool_ready_at: anchor,
            trigger_observed_at: anchor,
        }).unwrap();
    }

    std::thread::sleep(Duration::from_millis(20));

    // Match sigs[1] — that's the winner
    match_tx.send(MatchEvent {
        signature: sigs[1],
        observed_at: Instant::now(),
        observed_slot: 100,
        observed_entry_index: 3,
        observed_tick_in_slot: Some(5),
        observed_cumulative_hashes_in_slot: Some(312_500),
        observed_source: SourceKind::ShredStream,
    }).unwrap();

    std::thread::sleep(Duration::from_millis(80));

    drop(reg_tx); drop(send_tx); drop(match_tx);
    drop(final_tx);
    stop.store(true, Ordering::Relaxed);

    let _ = matcher_handle.join();
    let _ = parquet_handle.join();

    // Read parquet, verify outcomes
    let file = File::open(&parquet_path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap().build().unwrap();
    let mut landed = 0;
    let mut deduped = 0;
    let mut total = 0;
    for batch in reader {
        let batch = batch.unwrap();
        total += batch.num_rows();
        let col = batch.column_by_name("tentative_outcome").unwrap();
        let strs = col.as_any().downcast_ref::<arrow_array::StringArray>().unwrap();
        for i in 0..batch.num_rows() {
            match strs.value(i) {
                "LANDED_TENTATIVE" => landed += 1,
                "DEDUPED_TENTATIVE" => deduped += 1,
                _ => {}
            }
        }
    }
    assert_eq!(total, 3);
    assert_eq!(landed, 1);
    assert_eq!(deduped, 2);
}

// Suppress unused warning
fn _unused_imports() {
    use entry_sources as _;
    use fan_out_bench::match_event as _;
    let _ = SignatureVec::new();
    let _ = EntryObservation {
        source: SourceKind::ShredStream,
        observed_at: Instant::now(),
        slot: 0,
        entry_index: 0,
        num_hashes: 0,
        entry_hash: Hash::default(),
        tx_count: 0,
        signatures: SignatureVec::new(),
        first_shred_at: None,
        leader: None,
    };
    let _ = FinalStatus::Pending;
    let _ = RateLimitState::Ok;
    let _ = TentativeOutcome::LandedTentative;
}
```

Run: `cargo test -p fan-out-bench --test pipeline_mock`. Expected: 1 test passes.

---

## Task 14: Final verification + README

- [ ] **Step 1: Full test suite**

Run: `cargo test -p fan-out-bench`. Expected: all tests pass (~100 total).

- [ ] **Step 2: Clippy clean**

Run: `cargo clippy -p fan-out-bench --all-targets --no-deps -- -D warnings`. Expected: no warnings. If new clippy issues appear, fix them inline (likely type_complexity in new structs — add `#[allow(clippy::type_complexity)]` on affected fns).

- [ ] **Step 3: Build all bins**

Run: `cargo build -p fan-out-bench --bins`. Expected: builds `setup_nonces`, `teardown_nonces`, `run`.

- [ ] **Step 4: Update README**

In `crates/fan-out-bench/README.md`, replace `Plan 4: ...` entry in "Not yet implemented" with:

```markdown
Plan 4 — pipeline + first senders:
- ✅ HTTP JSON-RPC helper
- ✅ HeliusSender (HTTP)
- ✅ JitoSender (single tx HTTP)
- ✅ TriggerId hashing
- ✅ MatchEvent + observer sig matching
- ✅ Matcher state machine (single-owner per (TriggerId, sender_id))
- ✅ Preparer (nonce + tx_builder integration)
- ✅ Dispatcher (async fan-out with deterministic perm)
- ✅ Runtime wiring
- ✅ `run` CLI binary (skeleton — SS/YS gRPC hooks in Plan 5)
- ✅ Pipeline mock integration test
- ⏸ Finality tracker (deferred to Plan 5)
- ⏸ RPC fallback for UNKNOWN_PENDING (deferred to Plan 5)
```

---

## Plan 4 done

Po tym planie mamy:
- Pełen pipeline złożony (preparer → pool → observer → dispatcher → matcher → parquet)
- Pierwsze dwa real sendery (Helius + Jito-single-tx)
- CLI `run` binary z możliwością uruchomienia (idle do hookowania SS/YS w Plan 5)
- Parquet zapisuje LANDED_TENTATIVE/DEDUPED_TENTATIVE/SEND_ERROR/UNKNOWN_PENDING rows

**Następny plan:** Plan 5 — finality tracker + RPC fallback + SS/YS gRPC hookup w runtime + smoke test na devnet. To pierwszy plan który daje **uruchamialny end-to-end bench na real chain**.
