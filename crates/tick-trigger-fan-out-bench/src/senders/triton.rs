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

use super::{SendOutcome, TxSender};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use solana_sdk::transaction::Transaction;
use std::time::{Duration, Instant};

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

#[cfg(test)]
mod tests {
    use super::*;
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
}
