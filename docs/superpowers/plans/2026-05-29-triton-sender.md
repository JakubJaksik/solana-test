# Triton Sender Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Triton One as a third tx-send method (`SenderKind::Triton`) to `tick-trigger-fan-out-bench`, as a minimal HTTP `sendTransaction` sender (Jet/SWQoS server-side), reusing the existing shared durable-nonce / memo / preparer machinery.

**Architecture:** New `senders/triton.rs` implementing the existing `TxSender` trait — a near-clone of `HeliusSender`. Differences: no `preflightCommitment`, no tip, secret token in URL path (redacted in logs/records), `protocol() = "TRITON"`, and a fire-and-forget connection pre-warm. Wired via one new `SenderKind` variant + a factory arm in `phase3_run.rs`. No changes to preparer/tx_builder/nonce/recorder.

**Tech Stack:** Rust, `reqwest` (rustls, HTTP/1.1 keep-alive), `async-trait`, `serde`/`serde_json`, `base64`, `bincode`, `solana-sdk`, `tokio`.

**Spec:** `docs/superpowers/specs/2026-05-29-triton-sender-design.md`

**Project rule — commits:** The user commits manually. Each "Checkpoint" step is a stop-and-report point; do NOT run `git commit` yourself — report green tests and the suggested message, the user commits.

**Note on intermediate builds:** Tasks 1–4 add `senders/triton.rs` as a standalone module (referenced by `pub mod triton;`). It is not used by the factory until Task 5, so intermediate `cargo` runs may emit `dead_code`/`unused` warnings — these are warnings, not errors; tests pass. Task 5 removes them by wiring the sender in.

**Run commands from the workspace root** (`/home/jjaksik/Repos/my-scripts`).

---

## File Structure

- **Create** `crates/tick-trigger-fan-out-bench/src/senders/triton.rs` — `TritonSender` (TxSender impl) + pure helpers `redact_endpoint`, `build_body`, `parse_reply` + unit tests.
- **Modify** `crates/tick-trigger-fan-out-bench/src/senders/mod.rs` — add `pub mod triton;`.
- **Modify** `crates/tick-trigger-fan-out-bench/src/config.rs` — add `Triton` to `SenderKind` (+ test).
- **Modify** `crates/tick-trigger-fan-out-bench/src/tip_accounts.rs` — add `SenderKind::Triton => &[]` arm (+ test).
- **Modify** `crates/tick-trigger-fan-out-bench/src/bin/phase3_run.rs` — import `TritonSender` + factory match arm.

---

## Task 1: `redact_endpoint` helper + create module

**Files:**
- Create: `crates/tick-trigger-fan-out-bench/src/senders/triton.rs`
- Modify: `crates/tick-trigger-fan-out-bench/src/senders/mod.rs:7-8`

- [ ] **Step 1: Create the module with the failing test**

Create `crates/tick-trigger-fan-out-bench/src/senders/triton.rs`:

```rust
//! Triton One `sendTransaction` sender (HTTP JSON-RPC).
//!
//! Triton routes every `sendTransaction` through "Jet" with stake-weighted QoS
//! (QUIC, leader pre-connect) by default — the fast path is server-side, so the
//! client is a plain HTTP JSON-RPC sender. We POST a single pre-signed tx as
//! base64 with `skipPreflight=true` and `maxRetries=0`. No tip is required
//! (priority fee drives inclusion).
//!
//! Auth: the secret token is embedded in the endpoint URL path
//! (`https://<ep>.mainnet.rpcpool.com/<TOKEN>`). The token is kept private and
//! NEVER logged — `endpoint_url()` returns a redacted (scheme + host) form.

/// Strip the secret token (URL path) for safe logging: keep scheme + host only.
/// `https://name.mainnet.rpcpool.com/TOKEN` -> `https://name.mainnet.rpcpool.com`.
fn redact_endpoint(url: &str) -> String {
    match url.split_once("://") {
        Some((scheme, rest)) => {
            let host = rest.split('/').next().unwrap_or(rest);
            format!("{scheme}://{host}")
        }
        None => url.split('/').next().unwrap_or(url).to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redact_endpoint_strips_token_path() {
        let url = "https://my-app.mainnet.rpcpool.com/SECRET-TOKEN-123";
        let red = redact_endpoint(url);
        assert_eq!(red, "https://my-app.mainnet.rpcpool.com");
        assert!(!red.contains("SECRET-TOKEN-123"));
    }

    #[test]
    fn redact_endpoint_handles_no_path() {
        assert_eq!(
            redact_endpoint("https://my-app.mainnet.rpcpool.com"),
            "https://my-app.mainnet.rpcpool.com"
        );
    }
}
```

Add to `crates/tick-trigger-fan-out-bench/src/senders/mod.rs` (currently lines 7-8 are `pub mod helius;` / `pub mod jito;`):

```rust
pub mod helius;
pub mod jito;
pub mod triton;
```

- [ ] **Step 2: Run the test to verify it passes (helper compiles)**

Run: `cargo test -p tick-trigger-fan-out-bench redact_endpoint`
Expected: PASS — `test senders::triton::tests::redact_endpoint_strips_token_path ... ok` and `...handles_no_path ... ok`.
(If the module weren't wired into `mod.rs`, this would fail with `unresolved module` — the test passing confirms wiring.)

- [ ] **Step 3: Checkpoint — user commits**

Report green. Suggested message: `feat(triton): add senders/triton module + redact_endpoint helper`.

---

## Task 2: `build_body` — JSON-RPC request serialization

**Files:**
- Modify: `crates/tick-trigger-fan-out-bench/src/senders/triton.rs`

- [ ] **Step 1: Write the failing test**

Add these imports at the top of `triton.rs` (below the module doc comment):

```rust
use serde::Serialize;
use solana_sdk::transaction::Transaction;
```

Add to the `mod tests` block (inside it), a tx factory + the test:

```rust
    use solana_sdk::hash::Hash;
    use solana_sdk::message::Message;
    use solana_sdk::signature::{Keypair, Signer};
    use solana_system_interface::instruction as system_instruction;

    fn sample_tx() -> Transaction {
        let payer = Keypair::new();
        let ix = system_instruction::transfer(&payer.pubkey(), &payer.pubkey(), 1);
        let msg = Message::new(&[ix], Some(&payer.pubkey()));
        let mut tx = Transaction::new_unsigned(msg);
        tx.sign(&[&payer], Hash::new_unique());
        tx
    }

    #[test]
    fn build_body_matches_triton_send_transaction_shape() {
        use base64::Engine as _;
        let tx = sample_tx();
        let body = build_body(&tx);
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["jsonrpc"], "2.0");
        assert_eq!(v["method"], "sendTransaction");
        assert_eq!(v["params"][1]["encoding"], "base64");
        assert_eq!(v["params"][1]["skipPreflight"], true);
        assert_eq!(v["params"][1]["maxRetries"], 0);
        // Triton does NOT use preflightCommitment (unlike Helius).
        assert!(v["params"][1].get("preflightCommitment").is_none());
        // params[0] is base64(bincode(tx)).
        let expected_b64 = base64::engine::general_purpose::STANDARD
            .encode(bincode::serialize(&tx).unwrap());
        assert_eq!(v["params"][0], expected_b64);
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p tick-trigger-fan-out-bench build_body_matches`
Expected: FAIL — compile error `E0425: cannot find function 'build_body' in this scope`.

- [ ] **Step 3: Write minimal implementation**

Add to `triton.rs` (above the `#[cfg(test)]` block):

```rust
#[derive(Serialize)]
struct SendRequest<'a> {
    jsonrpc: &'static str,
    id: u64,
    method: &'static str,
    params: (&'a str, SendOptions),
}

#[derive(Serialize)]
struct SendOptions {
    encoding: &'static str,
    #[serde(rename = "skipPreflight")]
    skip_preflight: bool,
    #[serde(rename = "maxRetries")]
    max_retries: u32,
}

/// Build the JSON-RPC `sendTransaction` request body for a pre-signed tx:
/// `base64(bincode(tx))` + `{encoding:"base64", skipPreflight:true, maxRetries:0}`.
/// No `preflightCommitment` — Triton does not need it under `skipPreflight`.
fn build_body(tx: &Transaction) -> String {
    use base64::Engine as _;
    let serialized = bincode::serialize(tx).unwrap_or_default();
    let b64 = base64::engine::general_purpose::STANDARD.encode(&serialized);
    serde_json::to_string(&SendRequest {
        jsonrpc: "2.0",
        id: 1,
        method: "sendTransaction",
        params: (
            &b64,
            SendOptions {
                encoding: "base64",
                skip_preflight: true,
                max_retries: 0,
            },
        ),
    })
    .unwrap_or_default()
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p tick-trigger-fan-out-bench build_body_matches`
Expected: PASS — `test senders::triton::tests::build_body_matches_triton_send_transaction_shape ... ok`.

- [ ] **Step 5: Checkpoint — user commits**

Report green. Suggested message: `feat(triton): build_body — sendTransaction JSON-RPC request`.

---

## Task 3: `parse_reply` — JSON-RPC response parsing

**Files:**
- Modify: `crates/tick-trigger-fan-out-bench/src/senders/triton.rs`

- [ ] **Step 1: Write the failing tests**

Add to the `mod tests` block:

```rust
    #[test]
    fn parse_reply_ok_returns_signature() {
        let body = r#"{"jsonrpc":"2.0","result":"5SigabcDEF","id":1}"#;
        match parse_reply(body) {
            ParsedReply::Ok { signature } => assert_eq!(signature.as_deref(), Some("5SigabcDEF")),
            other => panic!("expected Ok, got {:?}", other),
        }
    }

    #[test]
    fn parse_reply_error_returns_code_and_message() {
        let body = r#"{"jsonrpc":"2.0","error":{"code":-32002,"message":"blockhash not found"},"id":1}"#;
        match parse_reply(body) {
            ParsedReply::RpcError { code, message } => {
                assert_eq!(code, -32002);
                assert_eq!(message, "blockhash not found");
            }
            other => panic!("expected RpcError, got {:?}", other),
        }
    }

    #[test]
    fn parse_reply_non_json_is_captured() {
        match parse_reply("502 Bad Gateway") {
            ParsedReply::NonJson { body } => assert_eq!(body, "502 Bad Gateway"),
            other => panic!("expected NonJson, got {:?}", other),
        }
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p tick-trigger-fan-out-bench parse_reply`
Expected: FAIL — compile error `E0433`/`E0425`: cannot find `parse_reply` / `ParsedReply`.

- [ ] **Step 3: Write minimal implementation**

Add the `Deserialize` import to the existing serde use line so it reads:

```rust
use serde::{Deserialize, Serialize};
```

Add to `triton.rs` (above the `#[cfg(test)]` block):

```rust
#[derive(Deserialize)]
struct JsonRpcResponse {
    result: Option<String>,
    error: Option<JsonRpcError>,
}

#[derive(Deserialize)]
struct JsonRpcError {
    code: i32,
    message: String,
}

/// Outcome of parsing a Triton JSON-RPC reply body. `send()` maps this to a
/// `SendOutcome` together with timing/status.
#[derive(Debug)]
enum ParsedReply {
    Ok { signature: Option<String> },
    RpcError { code: i32, message: String },
    NonJson { body: String },
}

fn parse_reply(body: &str) -> ParsedReply {
    match serde_json::from_str::<JsonRpcResponse>(body) {
        Ok(r) => match r.error {
            Some(err) => ParsedReply::RpcError { code: err.code, message: err.message },
            None => ParsedReply::Ok { signature: r.result },
        },
        Err(_) => ParsedReply::NonJson { body: body.to_string() },
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p tick-trigger-fan-out-bench parse_reply`
Expected: PASS — 3 tests ok.

- [ ] **Step 5: Checkpoint — user commits**

Report green. Suggested message: `feat(triton): parse_reply — JSON-RPC response parsing`.

---

## Task 4: `TritonSender` struct + `TxSender` impl + `send()` + warm-up

**Files:**
- Modify: `crates/tick-trigger-fan-out-bench/src/senders/triton.rs`

- [ ] **Step 1: Write the failing tests**

Add to the `mod tests` block:

```rust
    #[test]
    fn endpoint_url_redacts_token() {
        let s = TritonSender::new(
            3,
            "triton-fra",
            "https://my-app.mainnet.rpcpool.com/SECRET-TOKEN-123",
        );
        assert_eq!(s.endpoint_url(), "https://my-app.mainnet.rpcpool.com");
        assert!(!s.endpoint_url().contains("SECRET-TOKEN-123"));
    }

    #[test]
    fn protocol_label_is_triton() {
        let s = TritonSender::new(3, "triton-fra", "https://x.mainnet.rpcpool.com/t");
        assert_eq!(s.protocol(), "TRITON");
        assert_eq!(s.id(), 3);
        assert_eq!(s.name(), "triton-fra");
    }
```

Note: `protocol`, `id`, `name`, `endpoint_url` come from the `TxSender` trait — `use super::*;` already re-exports it via `super::{SendOutcome, TxSender}` (added in Step 3).

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p tick-trigger-fan-out-bench -- endpoint_url_redacts_token protocol_label_is_triton`
Expected: FAIL — compile error `E0433`: cannot find type `TritonSender`.

- [ ] **Step 3: Write the implementation**

Replace the top-of-file imports so the full set is present:

```rust
use super::{SendOutcome, TxSender};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use solana_sdk::transaction::Transaction;
use std::time::{Duration, Instant};
```

Add to `triton.rs` (above the `#[cfg(test)]` block):

```rust
pub struct TritonSender {
    id: u8,
    name: String,
    /// Full URL incl. secret token — private, used ONLY as the POST target.
    endpoint: String,
    /// Token-redacted (scheme + host). Returned by `endpoint_url()` and used in
    /// `SendOutcome.endpoint_url_used` so the token never reaches logs/records.
    endpoint_display: String,
    client: reqwest::Client,
}

impl TritonSender {
    pub fn new(id: u8, name: impl Into<String>, endpoint: impl Into<String>) -> Self {
        let endpoint = endpoint.into();
        let endpoint_display = redact_endpoint(&endpoint);
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .tcp_nodelay(true)
            .pool_max_idle_per_host(8)
            .tcp_keepalive(Duration::from_secs(30))
            .build()
            .expect("reqwest client");
        Self { id, name: name.into(), endpoint, endpoint_display, client }
    }

    /// Fire-and-forget connection pre-warm: spawns a lightweight `getHealth` so
    /// the first real send reuses a warm keep-alive connection instead of paying
    /// TCP+TLS handshake on the hot path. `reqwest::Client` clones share the
    /// connection pool, so warming a clone warms this sender's pool.
    pub fn spawn_warmup(&self, handle: &tokio::runtime::Handle) {
        let client = self.client.clone();
        let endpoint = self.endpoint.clone();
        handle.spawn(async move {
            let _ = client
                .post(&endpoint)
                .header("Content-Type", "application/json")
                .body(r#"{"jsonrpc":"2.0","id":1,"method":"getHealth"}"#)
                .send()
                .await;
        });
    }
}

#[async_trait]
impl TxSender for TritonSender {
    fn id(&self) -> u8 {
        self.id
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn endpoint_url(&self) -> &str {
        &self.endpoint_display
    }
    fn protocol(&self) -> &'static str {
        "TRITON"
    }

    async fn send(&self, tx: &Transaction) -> SendOutcome {
        let signature = tx.signatures.first().copied().unwrap_or_default();
        let body = build_body(tx);

        let send_at = Instant::now();
        let result = self
            .client
            .post(&self.endpoint)
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .await;
        let send_ack_at = Some(Instant::now());

        let redacted = self.endpoint_display.clone();
        match result {
            Err(e) => SendOutcome {
                send_at,
                send_ack_at: None,
                signature,
                http_status: None,
                rpc_err_code: None,
                rpc_err_message: None,
                provider_request_id: None,
                error: Some(format!("network: {}", e)),
                endpoint_url_used: Some(redacted),
            },
            Ok(resp) => {
                let status = resp.status().as_u16();
                let body_text = resp.text().await.unwrap_or_default();
                match parse_reply(&body_text) {
                    ParsedReply::Ok { signature: returned } => {
                        let returned_sig = returned.as_deref().and_then(|s| s.parse().ok());
                        SendOutcome {
                            send_at,
                            send_ack_at,
                            signature: returned_sig.unwrap_or(signature),
                            http_status: Some(status),
                            rpc_err_code: None,
                            rpc_err_message: None,
                            provider_request_id: None,
                            error: None,
                            endpoint_url_used: Some(redacted),
                        }
                    }
                    ParsedReply::RpcError { code, message } => SendOutcome {
                        send_at,
                        send_ack_at,
                        signature,
                        http_status: Some(status),
                        rpc_err_code: Some(code),
                        rpc_err_message: Some(message.clone()),
                        provider_request_id: None,
                        error: Some(message),
                        endpoint_url_used: Some(redacted),
                    },
                    ParsedReply::NonJson { body } => SendOutcome {
                        send_at,
                        send_ack_at,
                        signature,
                        http_status: Some(status),
                        rpc_err_code: None,
                        rpc_err_message: Some(format!("non-JSONRPC body: {}", body)),
                        provider_request_id: None,
                        error: Some(format!("HTTP {} body: {}", status, body)),
                        endpoint_url_used: Some(redacted),
                    },
                }
            }
        }
    }
}
```

- [ ] **Step 4: Run tests to verify they pass + whole module compiles**

Run: `cargo test -p tick-trigger-fan-out-bench senders::triton`
Expected: PASS — all `senders::triton::tests::*` ok (redact ×2, build_body, parse_reply ×3, endpoint_url_redacts_token, protocol_label_is_triton).

- [ ] **Step 5: Checkpoint — user commits**

Report green. Suggested message: `feat(triton): TritonSender (TxSender impl) + send + warm-up`.

---

## Task 5: Wire Triton into config, tip accounts, and the factory

This is the integration task. Adding `SenderKind::Triton` makes the exhaustive `match`es in `tip_accounts_for` and the `phase3_run` factory non-exhaustive — both must be fixed in this task for the crate to compile.

**Files:**
- Modify: `crates/tick-trigger-fan-out-bench/src/config.rs:155-158` (enum) + tests (~line 327)
- Modify: `crates/tick-trigger-fan-out-bench/src/tip_accounts.rs:66-71` + tests
- Modify: `crates/tick-trigger-fan-out-bench/src/bin/phase3_run.rs:62-66` (import) + `:311-345` (factory)

- [ ] **Step 1: Write the failing tests**

In `config.rs`, add to the `#[cfg(test)] mod tests` block (after `jito_sender_with_full_bundle_fields_parses`, ~line 327):

```rust
    #[test]
    fn triton_sender_kind_parses_with_defaults() {
        let json = r#"{
          "id": 3, "name": "triton-fra", "kind": "triton",
          "endpoint_url": "https://my-app.mainnet.rpcpool.com/TOKEN"
        }"#;
        let s: SenderConfig = serde_json::from_str(json).unwrap();
        assert_eq!(s.kind, SenderKind::Triton);
        assert_eq!(s.tip_lamports, 0); // no tip for Triton
        assert!(s.enabled);
    }
```

In `tip_accounts.rs`, add to the `#[cfg(test)] mod tests` block:

```rust
    #[test]
    fn tip_accounts_for_triton_is_empty() {
        assert!(tip_accounts_for(SenderKind::Triton).is_empty());
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p tick-trigger-fan-out-bench triton_sender_kind_parses_with_defaults`
Expected: FAIL — compile error `E0599`/`E0433`: no variant named `Triton` for `SenderKind`. (The whole crate fails to compile because `SenderKind::Triton` doesn't exist yet.)

- [ ] **Step 3: Add the enum variant**

In `crates/tick-trigger-fan-out-bench/src/config.rs:155-158`, change:

```rust
pub enum SenderKind {
    Helius,
    Jito,
}
```
to:
```rust
pub enum SenderKind {
    Helius,
    Jito,
    Triton,
}
```

- [ ] **Step 4: Fix the `tip_accounts_for` match**

In `crates/tick-trigger-fan-out-bench/src/tip_accounts.rs:66-71`, change:

```rust
pub fn tip_accounts_for(kind: SenderKind) -> &'static [Pubkey] {
    match kind {
        SenderKind::Helius => helius_tip_accounts(),
        SenderKind::Jito => jito_tip_accounts(),
    }
}
```
to:
```rust
pub fn tip_accounts_for(kind: SenderKind) -> &'static [Pubkey] {
    match kind {
        SenderKind::Helius => helius_tip_accounts(),
        SenderKind::Jito => jito_tip_accounts(),
        // Triton (Jet/SWQoS) needs no tip — inclusion is driven by priority fee.
        SenderKind::Triton => &[],
    }
}
```

- [ ] **Step 5: Add the factory arm + import in `phase3_run.rs`**

In `crates/tick-trigger-fan-out-bench/src/bin/phase3_run.rs:62-66`, change the senders import:

```rust
use tick_trigger_fan_out_bench::senders::{
    helius::HeliusSender,
    jito::{tip_updater::JitoTipUpdater, JitoBundleCounters, JitoBundleSender},
    triton::TritonSender,
    TxSender,
};
```

In the factory `match sc.kind { ... }` (currently ends at line 345, after the `SenderKind::Jito => { ... }` arm), add a third arm before the closing `}`:

```rust
            SenderKind::Triton => {
                let t = TritonSender::new(sc.id, sc.name.clone(), sc.endpoint_url.clone());
                // Pre-warm the keep-alive connection off the hot path so the
                // first send doesn't pay TCP+TLS handshake.
                t.spawn_warmup(&bg_handle);
                Arc::new(t) as Arc<dyn TxSender>
            }
```

- [ ] **Step 6: Run the targeted tests + full build + full test suite**

Run: `cargo test -p tick-trigger-fan-out-bench triton_sender_kind_parses_with_defaults tip_accounts_for_triton_is_empty`
Expected: PASS — both ok.

Run: `cargo build -p tick-trigger-fan-out-bench`
Expected: builds clean (factory arm compiles; no non-exhaustive-match errors; no `dead_code` warnings for `TritonSender` now that it's used).

Run: `cargo test -p tick-trigger-fan-out-bench`
Expected: the entire crate test suite passes (existing tests + new Triton tests).

- [ ] **Step 7: Checkpoint — user commits**

Report green (with the full `cargo test` summary). Suggested message: `feat(triton): wire SenderKind::Triton into config, tip accounts, and phase3 factory`.

---

## After implementation (manual, outside this plan)

1. Add a Triton entry to the run config and **activate the Tier 3 endpoint** on the Triton dashboard:
   ```json
   { "id": <next>, "name": "triton-fra", "kind": "triton",
     "endpoint_url": "https://<ep>.mainnet.rpcpool.com/<TOKEN>",
     "tip_lamports": 0, "enabled": true }
   ```
   Token is a secret → config only (gitignored), never code. Ensure `tx.priority_fee_microlamports > 0`.
2. Empirical smoke (per spec "Risks"): run the bench briefly and confirm in the JSONL that the Triton sender's tx **lands** and the shared nonce **advances** (i.e. Triton appears as a `Landed` winner and `endpoint_url_used` is token-free).

---

## Self-Review (filled in by plan author)

**Spec coverage:** new module (Task 1–4), `build_body`/request shape (Task 2), response/outcome mapping (Task 4 via Task 3), redaction (Task 1 + Task 4 test), `protocol="TRITON"` (Task 4), no-tip via `tip_accounts_for` empty + `tip_lamports:0` (Task 5), `SenderKind::Triton` + factory + warm-up (Task 5), config secret + priority-fee note (post-impl section). Unchanged components (preparer/tx_builder/nonce/recorder) require no tasks by design — verified by the full `cargo test` in Task 5 Step 6. Account checklist captured in the post-impl section.

**Placeholder scan:** none — every step has concrete code/commands.

**Type consistency:** `redact_endpoint`, `build_body`, `parse_reply`/`ParsedReply`, `SendRequest`/`SendOptions`, `JsonRpcResponse`/`JsonRpcError`, `TritonSender::new`, `spawn_warmup`, and the `TxSender` methods are used consistently across tasks; `SendOutcome` fields match `senders/mod.rs:14-25`; `TxSender` method set matches `senders/mod.rs:27-34`.
