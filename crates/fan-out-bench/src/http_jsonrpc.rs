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

pub fn tx_to_base64(tx: &Transaction) -> String {
    let bytes = bincode::serialize(tx).expect("transaction serialization never fails");
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

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
        assert!(b64.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '/' | '=')));
    }
}
