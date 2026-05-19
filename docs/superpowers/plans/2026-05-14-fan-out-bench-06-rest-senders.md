# fan-out-bench — Plan 6: Remaining REST senders

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development.

**Goal:** Dodać pozostałe REST-based sendery (Nozomi, 0slot, bloXroute, Astralane, Syncro, Triton) + Jito sendBundle path + Helius swqos_only jako osobny sender_id. Po tym planie bench obsługuje 8-10 typów senderów × multiple regions = config może mieć 15+ sender_id enabled w jednym runie.

**Architecture:** Każdy sender to nowy moduł w `src/senders/`. Wszystkie używają shared `http_jsonrpc.rs` helper gdzie pasuje. Senderzy z custom body shape (bloXroute, Astralane plaintext, Jito bundle) mają własne body builders ale wciąż używają shared reqwest client setup.

**Tech Stack:** reqwest (już mamy), serde_json.

**Reference spec:** §5.1, §5.3

**Previous plans:** 1-5 (foundation through real-chain wiring).

---

## File structure (Plan 6 scope)

```
crates/fan-out-bench/src/senders/
├── nozomi.rs                — JSON-RPC HTTP + ?c=<key>
├── slot0.rs                 — HTTPS JSON-RPC + ?api-key=<key>
├── bloxroute.rs             — HTTP custom JSON body + Authorization header
├── astralane.rs             — HTTP plaintext base64 + ?api-key=<key>
├── syncro.rs                — HTTPS JSON-RPC + Bearer/X-Api-Key
├── triton.rs                — HTTPS JSON-RPC + path token
└── jito_bundle.rs           — HTTPS JSON-RPC sendBundle method
```

Plus updates to `bin/run.rs` (match arms for new SenderKinds) and `config.example.json` (sample multi-sender config).

---

## Task 1: Module scaffolding

**Files:**
- Modify: `crates/fan-out-bench/src/senders/mod.rs`
- Create: stubs for 7 new sender files

- [ ] **Step 1: Update senders/mod.rs**

Edit `crates/fan-out-bench/src/senders/mod.rs`, ADD module declarations between `helius` and `jito`:

```rust
pub mod astralane;
pub mod bloxroute;
pub mod helius;
pub mod jito;
pub mod jito_bundle;
pub mod mock;
pub mod nozomi;
pub mod slot0;
pub mod syncro;
pub mod triton;
```

(Keep TxSender trait + SendOutcome struct + everything else unchanged.)

- [ ] **Step 2: Create stub files**

```bash
cd /home/jjaksik/Repos/my-scripts/crates/fan-out-bench/src/senders
touch astralane.rs bloxroute.rs jito_bundle.rs nozomi.rs slot0.rs syncro.rs triton.rs
```

Each stub: `// implementation in later task`.

- [ ] **Step 3: Verify**

Run: `cargo check -p fan-out-bench`. Expected: clean.

---

## Task 2: Nozomi sender

**Files:**
- Replace stub: `crates/fan-out-bench/src/senders/nozomi.rs`

- [ ] **Step 1: Implement NozomiSender (JSON-RPC variant)**

```rust
//! Nozomi (Temporal) sender — HTTP JSON-RPC with ?c=<key> auth query param.
//!
//! FRA endpoint: http://fra2.nozomi.temporal.xyz/?c=<key>
//! Min tip: 1_000_000 lamports to one of 17 tip accounts.
//! QoS penalty: <10% landing rate in 30min → priority discount.

use super::{SendOutcome, TxSender};
use crate::http_jsonrpc::{build_http_client, build_send_transaction_body, tx_to_base64, JsonRpcResponse};
use crate::outcome::RateLimitState;
use solana_sdk::transaction::Transaction;
use std::str::FromStr;
use std::time::{Duration, Instant};

pub struct NozomiSender {
    id: u8,
    name: String,
    endpoint: String,
    api_key: String,
    client: reqwest::Client,
}

impl NozomiSender {
    pub fn new(
        id: u8,
        name: impl Into<String>,
        endpoint: impl Into<String>,
        api_key: impl Into<String>,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            endpoint: endpoint.into(),
            api_key: api_key.into(),
            client: build_http_client(Duration::from_secs(5)),
        }
    }

    fn build_url(&self) -> String {
        format!("{}?c={}", self.endpoint, self.api_key)
    }
}

#[async_trait::async_trait]
impl TxSender for NozomiSender {
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

        let resp = self.client
            .post(&url)
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .await;

        let send_ack_at = Some(Instant::now());
        super::parse_jsonrpc_or_text(resp, send_at, send_ack_at, signature).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_url_includes_api_key_query() {
        let s = NozomiSender::new(0, "nozomi-fra", "http://fra2.nozomi.temporal.xyz/", "KEY123");
        assert_eq!(s.build_url(), "http://fra2.nozomi.temporal.xyz/?c=KEY123");
    }

    #[test]
    fn protocol_correct() {
        let s = NozomiSender::new(0, "nozomi", "http://x", "k");
        assert_eq!(s.protocol(), "HTTP_JSONRPC");
    }
}
```

- [ ] **Step 2: Add shared `parse_jsonrpc_or_text` helper to senders/mod.rs**

Add to `crates/fan-out-bench/src/senders/mod.rs` (at end, after TxSender trait):

```rust
/// Parse a reqwest response as JSON-RPC; fall back to text if non-JSON.
/// Shared logic for all JSON-RPC senders.
pub(crate) async fn parse_jsonrpc_or_text(
    resp_result: Result<reqwest::Response, reqwest::Error>,
    send_at: std::time::Instant,
    send_ack_at: Option<std::time::Instant>,
    signature: solana_sdk::signature::Signature,
) -> SendOutcome {
    use crate::http_jsonrpc::JsonRpcResponse;
    use crate::outcome::RateLimitState;
    use std::str::FromStr;

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
```

Remove unused imports (`JsonRpcResponse`, `FromStr`, `RateLimitState`) from `nozomi.rs` if added by mistake.

Run: `cargo test -p fan-out-bench --lib senders::nozomi`. Expected: 2 tests pass.

---

## Task 3: 0slot sender

**Files:**
- Replace stub: `crates/fan-out-bench/src/senders/slot0.rs`

- [ ] **Step 1: Implement Slot0Sender**

```rust
//! 0slot.trade sender — HTTPS JSON-RPC with ?api-key=<key>.
//!
//! Regions: de, ams, ny, jp, la — each = separate sender_id in config.
//! Min tip: 100_000 (advanced) / 1_000_000 (trial) to 21 tip accounts.

use super::{parse_jsonrpc_or_text, SendOutcome, TxSender};
use crate::http_jsonrpc::{build_http_client, build_send_transaction_body, tx_to_base64};
use solana_sdk::transaction::Transaction;
use std::time::{Duration, Instant};

pub struct Slot0Sender {
    id: u8,
    name: String,
    endpoint: String,
    api_key: String,
    client: reqwest::Client,
}

impl Slot0Sender {
    pub fn new(
        id: u8,
        name: impl Into<String>,
        endpoint: impl Into<String>,
        api_key: impl Into<String>,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            endpoint: endpoint.into(),
            api_key: api_key.into(),
            client: build_http_client(Duration::from_secs(5)),
        }
    }

    fn build_url(&self) -> String {
        format!("{}?api-key={}", self.endpoint, self.api_key)
    }
}

#[async_trait::async_trait]
impl TxSender for Slot0Sender {
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

        let resp = self.client
            .post(&url)
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .await;

        let send_ack_at = Some(Instant::now());
        parse_jsonrpc_or_text(resp, send_at, send_ack_at, signature).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_url_with_key() {
        let s = Slot0Sender::new(0, "0slot-de", "https://de.0slot.trade", "KEY");
        assert_eq!(s.build_url(), "https://de.0slot.trade?api-key=KEY");
    }
}
```

Run: `cargo test -p fan-out-bench --lib senders::slot0`. Expected: 1 test passes.

---

## Task 4: bloXroute sender

**Files:**
- Replace stub: `crates/fan-out-bench/src/senders/bloxroute.rs`

- [ ] **Step 1: Implement BloxrouteSender (custom body shape)**

```rust
//! bloXroute Trader API sender — HTTP custom body.
//!
//! FRA: http://germany.solana.dex.blxrbdn.com/api/v2/submit
//! Auth: Authorization header
//! Body shape (NOT standard JSON-RPC):
//! { "transaction": { "content": "<BASE64>" }, "skipPreFlight": true, ... }

use super::{SendOutcome, TxSender};
use crate::http_jsonrpc::{build_http_client, tx_to_base64};
use crate::outcome::RateLimitState;
use serde::{Deserialize, Serialize};
use solana_sdk::transaction::Transaction;
use std::str::FromStr;
use std::time::{Duration, Instant};

#[derive(Serialize)]
struct SubmitBody<'a> {
    transaction: SubmitTx<'a>,
    #[serde(rename = "skipPreFlight")]
    skip_preflight: bool,
    #[serde(rename = "frontRunningProtection")]
    front_running_protection: bool,
    #[serde(rename = "submitProtection")]
    submit_protection: &'static str,
    #[serde(rename = "useStakedRPCs")]
    use_staked_rpcs: bool,
}

#[derive(Serialize)]
struct SubmitTx<'a> {
    content: &'a str,
}

#[derive(Deserialize)]
struct SubmitResponse {
    signature: Option<String>,
}

pub struct BloxrouteSender {
    id: u8,
    name: String,
    endpoint: String,
    auth_header: String,
    client: reqwest::Client,
}

impl BloxrouteSender {
    pub fn new(
        id: u8,
        name: impl Into<String>,
        endpoint: impl Into<String>,
        auth_header: impl Into<String>,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            endpoint: endpoint.into(),
            auth_header: auth_header.into(),
            client: build_http_client(Duration::from_secs(5)),
        }
    }
}

#[async_trait::async_trait]
impl TxSender for BloxrouteSender {
    fn id(&self) -> u8 { self.id }
    fn name(&self) -> &str { &self.name }
    fn endpoint_url(&self) -> &str { &self.endpoint }
    fn protocol(&self) -> &'static str { "HTTP_PLAIN" }

    async fn send(&self, tx: &Transaction) -> SendOutcome {
        let send_at = Instant::now();
        let signature = tx.signatures.first().copied().unwrap_or_default();
        let b64 = tx_to_base64(tx);
        let body = serde_json::to_string(&SubmitBody {
            transaction: SubmitTx { content: &b64 },
            skip_preflight: true,
            front_running_protection: false,
            submit_protection: "SP_LOW",
            use_staked_rpcs: true,
        }).unwrap_or_default();

        let resp = self.client
            .post(&self.endpoint)
            .header("Content-Type", "application/json")
            .header("Authorization", &self.auth_header)
            .body(body)
            .send()
            .await;

        let send_ack_at = Some(Instant::now());

        match resp {
            Err(e) => SendOutcome {
                send_at, send_ack_at: None, signature,
                provider_request_id: None,
                http_status: None,
                rpc_err_code: None,
                rpc_err_message: None,
                rate_limit_state: if e.is_timeout() { RateLimitState::Timeout } else { RateLimitState::Ok },
                error: Some(format!("network: {}", e)),
            },
            Ok(r) => {
                let status = r.status().as_u16();
                let text = r.text().await.unwrap_or_default();
                if status == 200 {
                    let returned = serde_json::from_str::<SubmitResponse>(&text)
                        .ok()
                        .and_then(|r| r.signature)
                        .and_then(|s| solana_sdk::signature::Signature::from_str(&s).ok());
                    SendOutcome {
                        send_at, send_ack_at, signature: returned.unwrap_or(signature),
                        provider_request_id: None,
                        http_status: Some(status),
                        rpc_err_code: None,
                        rpc_err_message: None,
                        rate_limit_state: RateLimitState::Ok,
                        error: None,
                    }
                } else {
                    SendOutcome {
                        send_at, send_ack_at, signature,
                        provider_request_id: None,
                        http_status: Some(status),
                        rpc_err_code: None,
                        rpc_err_message: Some(text.clone()),
                        rate_limit_state: if status == 429 { RateLimitState::Throttled429 } else { RateLimitState::Ok },
                        error: Some(format!("HTTP {}: {}", status, text)),
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
    fn body_shape_correct() {
        let body = serde_json::to_string(&SubmitBody {
            transaction: SubmitTx { content: "BASE64TX" },
            skip_preflight: true,
            front_running_protection: false,
            submit_protection: "SP_LOW",
            use_staked_rpcs: true,
        }).unwrap();
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["transaction"]["content"], "BASE64TX");
        assert_eq!(v["skipPreFlight"], true);
        assert_eq!(v["submitProtection"], "SP_LOW");
        assert_eq!(v["useStakedRPCs"], true);
    }

    #[test]
    fn protocol_is_http_plain() {
        let s = BloxrouteSender::new(0, "blox", "http://x", "auth");
        assert_eq!(s.protocol(), "HTTP_PLAIN");
    }
}
```

Run: `cargo test -p fan-out-bench --lib senders::bloxroute`. Expected: 2 tests pass.

---

## Task 5: Astralane sender

**Files:**
- Replace stub: `crates/fan-out-bench/src/senders/astralane.rs`

- [ ] **Step 1: Implement AstralaneSender (plaintext base64 body)**

```rust
//! Astralane Iris sender — HTTP plaintext base64 body via /iris2 endpoint.
//!
//! Endpoint: http://fr.gateway.astralane.io/iris2?api-key=<key>&method=sendTransaction
//! Body: raw base64 transaction (text/plain content-type)
//! Min tip: 10_000 lamports (Iris)

use super::{SendOutcome, TxSender};
use crate::http_jsonrpc::{build_http_client, tx_to_base64};
use crate::outcome::RateLimitState;
use solana_sdk::transaction::Transaction;
use std::time::{Duration, Instant};

pub struct AstralaneSender {
    id: u8,
    name: String,
    endpoint: String,
    api_key: String,
    client: reqwest::Client,
}

impl AstralaneSender {
    pub fn new(
        id: u8,
        name: impl Into<String>,
        endpoint: impl Into<String>,
        api_key: impl Into<String>,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            endpoint: endpoint.into(),
            api_key: api_key.into(),
            client: build_http_client(Duration::from_secs(5)),
        }
    }

    fn build_url(&self) -> String {
        // append &method=sendTransaction if endpoint is /iris2 path
        if self.endpoint.contains("/iris2") {
            format!("{}?api-key={}&method=sendTransaction", self.endpoint, self.api_key)
        } else {
            format!("{}?api-key={}", self.endpoint, self.api_key)
        }
    }
}

#[async_trait::async_trait]
impl TxSender for AstralaneSender {
    fn id(&self) -> u8 { self.id }
    fn name(&self) -> &str { &self.name }
    fn endpoint_url(&self) -> &str { &self.endpoint }
    fn protocol(&self) -> &'static str { "HTTP_PLAIN" }

    async fn send(&self, tx: &Transaction) -> SendOutcome {
        let send_at = Instant::now();
        let signature = tx.signatures.first().copied().unwrap_or_default();
        let b64 = tx_to_base64(tx);
        let url = self.build_url();

        let resp = self.client
            .post(&url)
            .header("Content-Type", "text/plain")
            .body(b64)
            .send()
            .await;

        let send_ack_at = Some(Instant::now());

        match resp {
            Err(e) => SendOutcome {
                send_at, send_ack_at: None, signature,
                provider_request_id: None,
                http_status: None,
                rpc_err_code: None,
                rpc_err_message: None,
                rate_limit_state: if e.is_timeout() { RateLimitState::Timeout } else { RateLimitState::Ok },
                error: Some(format!("network: {}", e)),
            },
            Ok(r) => {
                let status = r.status().as_u16();
                let text = r.text().await.unwrap_or_default();
                if status == 200 {
                    SendOutcome {
                        send_at, send_ack_at, signature,
                        provider_request_id: None,
                        http_status: Some(status),
                        rpc_err_code: None,
                        rpc_err_message: None,
                        rate_limit_state: RateLimitState::Ok,
                        error: None,
                    }
                } else {
                    SendOutcome {
                        send_at, send_ack_at, signature,
                        provider_request_id: None,
                        http_status: Some(status),
                        rpc_err_code: None,
                        rpc_err_message: Some(text.clone()),
                        rate_limit_state: if status == 429 { RateLimitState::Throttled429 } else { RateLimitState::Ok },
                        error: Some(format!("HTTP {}: {}", status, text)),
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
    fn iris2_url_includes_method_param() {
        let s = AstralaneSender::new(0, "astra", "http://fr.gateway.astralane.io/iris2", "KEY");
        assert_eq!(s.build_url(), "http://fr.gateway.astralane.io/iris2?api-key=KEY&method=sendTransaction");
    }

    #[test]
    fn non_iris2_url_no_method() {
        let s = AstralaneSender::new(0, "astra", "http://fr.gateway.astralane.io/iris", "KEY");
        assert_eq!(s.build_url(), "http://fr.gateway.astralane.io/iris?api-key=KEY");
    }
}
```

Run: `cargo test -p fan-out-bench --lib senders::astralane`. Expected: 2 tests pass.

---

## Task 6: Syncro sender

**Files:**
- Replace stub: `crates/fan-out-bench/src/senders/syncro.rs`

- [ ] **Step 1: Implement SyncroSender (JSON-RPC + Bearer)**

```rust
//! Syncro Sender (P2P.org) — JSON-RPC with Bearer auth.
//!
//! Public path: /public (no auth, 1 TPS/IP)
//! Private path: / or /rpc (Bearer or X-Api-Key auth, 50 TPS)
//! Min tip: 100_000 public / 1_000_000 private. 9 tip accounts.

use super::{parse_jsonrpc_or_text, SendOutcome, TxSender};
use crate::http_jsonrpc::{build_http_client, build_send_transaction_body, tx_to_base64};
use solana_sdk::transaction::Transaction;
use std::time::{Duration, Instant};

pub enum SyncroAuth {
    None,
    Bearer(String),
    XApiKey(String),
}

pub struct SyncroSender {
    id: u8,
    name: String,
    endpoint: String,
    auth: SyncroAuth,
    client: reqwest::Client,
}

impl SyncroSender {
    pub fn new(
        id: u8,
        name: impl Into<String>,
        endpoint: impl Into<String>,
        auth: SyncroAuth,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            endpoint: endpoint.into(),
            auth,
            client: build_http_client(Duration::from_secs(5)),
        }
    }
}

#[async_trait::async_trait]
impl TxSender for SyncroSender {
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
        match &self.auth {
            SyncroAuth::None => {}
            SyncroAuth::Bearer(t) => req = req.header("Authorization", format!("Bearer {}", t)),
            SyncroAuth::XApiKey(k) => req = req.header("X-Api-Key", k),
        }
        let resp = req.send().await;
        let send_ack_at = Some(Instant::now());
        parse_jsonrpc_or_text(resp, send_at, send_ack_at, signature).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn syncro_construct() {
        let s = SyncroSender::new(0, "syncro-priv", "https://x/rpc", SyncroAuth::Bearer("T".into()));
        assert_eq!(s.name(), "syncro-priv");
        assert_eq!(s.protocol(), "HTTP_JSONRPC");
    }
}
```

Run: `cargo test -p fan-out-bench --lib senders::syncro`. Expected: 1 test passes.

---

## Task 7: Triton sender

**Files:**
- Replace stub: `crates/fan-out-bench/src/senders/triton.rs`

- [ ] **Step 1: Implement TritonSender (path token auth)**

```rust
//! Triton One sender — HTTPS JSON-RPC with path-token auth.
//!
//! Endpoint pattern: https://<app>.mainnet.rpcpool.com/<token>
//! Auth: token embedded in URL path, no separate header
//! No vendor tip account — only priority fee. SWQoS+Jet are default.

use super::{parse_jsonrpc_or_text, SendOutcome, TxSender};
use crate::http_jsonrpc::{build_http_client, build_send_transaction_body, tx_to_base64};
use solana_sdk::transaction::Transaction;
use std::time::{Duration, Instant};

pub struct TritonSender {
    id: u8,
    name: String,
    /// Full URL including path token (e.g. https://app.mainnet.rpcpool.com/SECRET_TOKEN)
    endpoint: String,
    client: reqwest::Client,
}

impl TritonSender {
    pub fn new(id: u8, name: impl Into<String>, endpoint: impl Into<String>) -> Self {
        Self {
            id,
            name: name.into(),
            endpoint: endpoint.into(),
            client: build_http_client(Duration::from_secs(5)),
        }
    }
}

#[async_trait::async_trait]
impl TxSender for TritonSender {
    fn id(&self) -> u8 { self.id }
    fn name(&self) -> &str { &self.name }
    fn endpoint_url(&self) -> &str { &self.endpoint }
    fn protocol(&self) -> &'static str { "HTTP_JSONRPC" }

    async fn send(&self, tx: &Transaction) -> SendOutcome {
        let send_at = Instant::now();
        let signature = tx.signatures.first().copied().unwrap_or_default();
        let b64 = tx_to_base64(tx);
        let body = build_send_transaction_body(&b64, true, 0);

        let resp = self.client
            .post(&self.endpoint)
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .await;
        let send_ack_at = Some(Instant::now());
        parse_jsonrpc_or_text(resp, send_at, send_ack_at, signature).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn triton_construct() {
        let s = TritonSender::new(0, "triton", "https://x.mainnet.rpcpool.com/TOKEN");
        assert_eq!(s.endpoint_url(), "https://x.mainnet.rpcpool.com/TOKEN");
    }
}
```

Run: `cargo test -p fan-out-bench --lib senders::triton`. Expected: 1 test passes.

---

## Task 8: Jito bundle sender

**Files:**
- Replace stub: `crates/fan-out-bench/src/senders/jito_bundle.rs`

- [ ] **Step 1: Implement JitoBundleSender (sendBundle method)**

```rust
//! Jito bundle sender — POST to /api/v1/bundles with sendBundle method.
//!
//! Body: { jsonrpc, id, method: "sendBundle", params: [[<BASE64_TX>], { encoding: "base64" }] }
//! Response: { result: "<BUNDLE_UUID>" } (not a signature)
//!
//! Single-tx bundle is the use case here — fan-out bench sends 1 tx per
//! variant, but using bundle path bo it gives revert protection.

use super::{SendOutcome, TxSender};
use crate::http_jsonrpc::{build_http_client, tx_to_base64, JsonRpcResponse};
use crate::outcome::RateLimitState;
use serde::Serialize;
use solana_sdk::transaction::Transaction;
use std::time::{Duration, Instant};

#[derive(Serialize)]
struct BundleRequest<'a> {
    jsonrpc: &'static str,
    id: u64,
    method: &'static str,
    params: (Vec<&'a str>, BundleParams),
}

#[derive(Serialize)]
struct BundleParams {
    encoding: &'static str,
}

pub struct JitoBundleSender {
    id: u8,
    name: String,
    endpoint: String,
    auth_uuid: Option<String>,
    client: reqwest::Client,
}

impl JitoBundleSender {
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

    fn build_body(&self, tx_base64: &str) -> String {
        let req = BundleRequest {
            jsonrpc: "2.0",
            id: 1,
            method: "sendBundle",
            params: (vec![tx_base64], BundleParams { encoding: "base64" }),
        };
        serde_json::to_string(&req).unwrap_or_default()
    }
}

#[async_trait::async_trait]
impl TxSender for JitoBundleSender {
    fn id(&self) -> u8 { self.id }
    fn name(&self) -> &str { &self.name }
    fn endpoint_url(&self) -> &str { &self.endpoint }
    fn protocol(&self) -> &'static str { "HTTP_JSONRPC" }

    async fn send(&self, tx: &Transaction) -> SendOutcome {
        let send_at = Instant::now();
        let signature = tx.signatures.first().copied().unwrap_or_default();
        let b64 = tx_to_base64(tx);
        let body = self.build_body(&b64);

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
                        // sendBundle returns a bundle UUID, not a signature.
                        // We keep the locally-computed signature for pending_sigs.
                        SendOutcome {
                            send_at, send_ack_at, signature,
                            provider_request_id: parsed.result,
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
    fn build_body_uses_sendbundle_method() {
        let s = JitoBundleSender::new(0, "jito-bundle", "https://x", None);
        let body = s.build_body("BASE64TX");
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["method"], "sendBundle");
        assert_eq!(v["params"][0][0], "BASE64TX");
        assert_eq!(v["params"][1]["encoding"], "base64");
    }
}
```

Run: `cargo test -p fan-out-bench --lib senders::jito_bundle`. Expected: 1 test passes.

---

## Task 9: Wire new sender kinds in bin/run.rs

**Files:**
- Modify: `crates/fan-out-bench/src/bin/run.rs`

- [ ] **Step 1: Extend match block for new sender kinds**

In `bin/run.rs`, find the `match sc.kind {` block (currently has Helius + Jito + `_ => skip`). Replace the `_ => ...` with explicit handling for each new kind:

```rust
            SenderKind::Nozomi => {
                let api_key = match &sc.auth {
                    fan_out_bench::config::AuthConfig::QueryParam { value, .. } => value.clone(),
                    _ => {
                        tracing::warn!(name = %sc.name, "nozomi requires QueryParam auth, skipping");
                        continue;
                    }
                };
                Arc::new(fan_out_bench::senders::nozomi::NozomiSender::new(
                    sc.id, sc.name.clone(), sc.endpoint_url.clone(), api_key,
                ))
            }
            SenderKind::Slot0 => {
                let api_key = match &sc.auth {
                    fan_out_bench::config::AuthConfig::QueryParam { value, .. } => value.clone(),
                    _ => { tracing::warn!(name = %sc.name, "0slot requires QueryParam auth, skipping"); continue; }
                };
                Arc::new(fan_out_bench::senders::slot0::Slot0Sender::new(
                    sc.id, sc.name.clone(), sc.endpoint_url.clone(), api_key,
                ))
            }
            SenderKind::Bloxroute => {
                let auth = match &sc.auth {
                    fan_out_bench::config::AuthConfig::Header { value, .. } => value.clone(),
                    _ => { tracing::warn!(name = %sc.name, "bloxroute requires Header auth, skipping"); continue; }
                };
                Arc::new(fan_out_bench::senders::bloxroute::BloxrouteSender::new(
                    sc.id, sc.name.clone(), sc.endpoint_url.clone(), auth,
                ))
            }
            SenderKind::Astralane => {
                let api_key = match &sc.auth {
                    fan_out_bench::config::AuthConfig::QueryParam { value, .. } => value.clone(),
                    _ => { tracing::warn!(name = %sc.name, "astralane requires QueryParam auth, skipping"); continue; }
                };
                Arc::new(fan_out_bench::senders::astralane::AstralaneSender::new(
                    sc.id, sc.name.clone(), sc.endpoint_url.clone(), api_key,
                ))
            }
            SenderKind::Syncro => {
                let auth = match &sc.auth {
                    fan_out_bench::config::AuthConfig::Bearer { token } => fan_out_bench::senders::syncro::SyncroAuth::Bearer(token.clone()),
                    fan_out_bench::config::AuthConfig::Header { value, .. } => fan_out_bench::senders::syncro::SyncroAuth::XApiKey(value.clone()),
                    fan_out_bench::config::AuthConfig::None => fan_out_bench::senders::syncro::SyncroAuth::None,
                    _ => { tracing::warn!(name = %sc.name, "syncro requires Bearer/Header/None auth, skipping"); continue; }
                };
                Arc::new(fan_out_bench::senders::syncro::SyncroSender::new(
                    sc.id, sc.name.clone(), sc.endpoint_url.clone(), auth,
                ))
            }
            SenderKind::Triton => {
                Arc::new(fan_out_bench::senders::triton::TritonSender::new(
                    sc.id, sc.name.clone(), sc.endpoint_url.clone(),
                ))
            }
            SenderKind::JitoBundle => {
                let auth = match &sc.auth {
                    fan_out_bench::config::AuthConfig::Header { value, .. } => Some(value.clone()),
                    _ => None,
                };
                Arc::new(fan_out_bench::senders::jito_bundle::JitoBundleSender::new(
                    sc.id, sc.name.clone(), sc.endpoint_url.clone(), auth,
                ))
            }
            _ => {
                tracing::warn!(name = %sc.name, kind = ?sc.kind, "sender kind not implemented yet, skipping");
                continue;
            }
```

Insert these before the existing `_ => ...` arm, after the `SenderKind::Jito => { ... }` arm.

- [ ] **Step 2: Verify**

Run: `cargo check --bin run -p fan-out-bench`. Expected: clean.

---

## Task 10: Extended config example

**Files:**
- Modify: `crates/fan-out-bench/config.example.json`

- [ ] **Step 1: Add example sender configs for new kinds**

Replace `senders` array in `crates/fan-out-bench/config.example.json` with:

```json
  "senders": [
    {
      "id": 0,
      "name": "helius-dual",
      "kind": "helius",
      "endpoint_url": "http://fra-sender.helius-rpc.com/fast",
      "region": "fra",
      "auth": { "type": "none" },
      "tip_lamports": 200000,
      "enabled": true
    },
    {
      "id": 1,
      "name": "helius-swqos",
      "kind": "helius",
      "endpoint_url": "http://fra-sender.helius-rpc.com/fast?swqos_only=true",
      "region": "fra",
      "auth": { "type": "none" },
      "tip_lamports": 5000,
      "enabled": false
    },
    {
      "id": 2,
      "name": "jito-fra-tx",
      "kind": "jito",
      "endpoint_url": "https://frankfurt.mainnet.block-engine.jito.wtf/api/v1/transactions",
      "region": "fra",
      "auth": { "type": "none" },
      "tip_lamports": 1000,
      "enabled": true
    },
    {
      "id": 3,
      "name": "jito-fra-bundle",
      "kind": "jito_bundle",
      "endpoint_url": "https://frankfurt.mainnet.block-engine.jito.wtf/api/v1/bundles",
      "region": "fra",
      "auth": { "type": "none" },
      "tip_lamports": 1000,
      "enabled": false
    },
    {
      "id": 4,
      "name": "nozomi-fra",
      "kind": "nozomi",
      "endpoint_url": "http://fra2.nozomi.temporal.xyz/",
      "region": "fra",
      "auth": { "type": "query_param", "key": "c", "value": "YOUR_NOZOMI_KEY" },
      "tip_lamports": 1000000,
      "enabled": false
    },
    {
      "id": 5,
      "name": "0slot-de",
      "kind": "slot0",
      "endpoint_url": "https://de.0slot.trade",
      "region": "fra",
      "auth": { "type": "query_param", "key": "api-key", "value": "YOUR_0SLOT_KEY" },
      "tip_lamports": 100000,
      "enabled": false
    },
    {
      "id": 6,
      "name": "bloxroute-fra",
      "kind": "bloxroute",
      "endpoint_url": "http://germany.solana.dex.blxrbdn.com/api/v2/submit",
      "region": "fra",
      "auth": { "type": "header", "name": "Authorization", "value": "YOUR_BLOXROUTE_AUTH" },
      "tip_lamports": 1000000,
      "enabled": false
    },
    {
      "id": 7,
      "name": "astralane-fra",
      "kind": "astralane",
      "endpoint_url": "http://fr.gateway.astralane.io/iris2",
      "region": "fra",
      "auth": { "type": "query_param", "key": "api-key", "value": "YOUR_ASTRALANE_KEY" },
      "tip_lamports": 10000,
      "enabled": false
    },
    {
      "id": 8,
      "name": "syncro-private",
      "kind": "syncro",
      "endpoint_url": "https://YOUR_SYNCRO_HOST/rpc",
      "region": "fra",
      "auth": { "type": "bearer", "token": "YOUR_SYNCRO_TOKEN" },
      "tip_lamports": 1000000,
      "enabled": false
    },
    {
      "id": 9,
      "name": "triton-mainnet",
      "kind": "triton",
      "endpoint_url": "https://YOUR_APP.mainnet.rpcpool.com/YOUR_SECRET_TOKEN",
      "region": "fra",
      "auth": { "type": "none" },
      "tip_lamports": 0,
      "enabled": false
    }
  ]
```

Run: `cargo test -p fan-out-bench --lib config::tests::parse_example_config_file`. Expected: passes.

---

## Task 11: Final verification + README

- [ ] **Step 1: Full test suite**

Run: `cargo test -p fan-out-bench`. Expected: all tests pass (~115+ tests).

- [ ] **Step 2: Clippy**

Run: `cargo clippy -p fan-out-bench --all-targets --no-deps -- -D warnings`. Expected: clean.

- [ ] **Step 3: Build all bins**

Run: `cargo build -p fan-out-bench --bins`. Expected: 3 binaries built.

- [ ] **Step 4: README update**

In `crates/fan-out-bench/README.md`, replace `Plan 6: ...` line in "Not yet implemented" with:

```markdown
Plan 6 — REST senders complete:
- ✅ NozomiSender (JSON-RPC + ?c=<key>)
- ✅ Slot0Sender (HTTPS JSON-RPC + ?api-key=<key>, multi-region)
- ✅ BloxrouteSender (custom JSON body + Authorization header)
- ✅ AstralaneSender (HTTP plaintext /iris2)
- ✅ SyncroSender (JSON-RPC + Bearer/X-Api-Key)
- ✅ TritonSender (path token auth, no vendor tip)
- ✅ JitoBundleSender (sendBundle method, separate sender_id)
- ✅ Helius swqos_only as separate sender_id (via config)
- ✅ Extended config example with all 10 sender variants
```

---

## Plan 6 done

Po tym planie:
- 9 sender impls (Helius, Jito-tx, Jito-bundle, Nozomi, 0slot, bloXroute, Astralane, Syncro, Triton)
- Każdy sender może mieć multi-region (osobne sender_id w config)
- Bench config może mieć 15+ sender_id enabled jednocześnie

**Następny plan:** Plan 7 — gRPC/QUIC senders (BlockRazor, AllenHark, NextBlock, Harmonic) + ops/polish (probe-senders binary, clock monitor, smoke harness improvements).
