use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use serde::Deserialize;

/// Client for Helius Sender. Constructs `{endpoint}/fast?api-key=...&swqos_only=...`
/// URLs at request time. Body is JSON-RPC `sendTransaction` with the signed tx
/// encoded as base64.
pub struct HeliusSender {
    client: reqwest::Client,
    /// Base URL such as `http://fra-sender.helius-rpc.com` (no trailing slash, no `/fast`, no query).
    endpoint: String,
    /// Optional Helius API key. If `None`, query string omits `api-key=`.
    api_key: Option<String>,
    /// If true, append `swqos_only=true` to the query (no Jito tip routing).
    swqos_only: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum SendError {
    #[error("http status {0}: {1}")]
    HttpStatus(u16, String),
    #[error("rpc error: {0}")]
    RpcError(String),
    #[error("network: {0}")]
    Network(#[from] reqwest::Error),
    #[error("response parse: {0}")]
    Parse(String),
}

impl HeliusSender {
    pub fn new(
        endpoint: String,
        api_key: Option<String>,
        swqos_only: bool,
    ) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .pool_max_idle_per_host(8)
            .timeout(Duration::from_secs(5))
            .tcp_keepalive(Some(Duration::from_secs(30)))
            .build()?;
        // strip trailing slash if present
        let endpoint = endpoint.trim_end_matches('/').to_string();
        Ok(Self { client, endpoint, api_key, swqos_only })
    }

    /// Send a signed transaction. `signed_tx_bytes` is the wire-format
    /// serialized signed transaction (typically from `bincode::serialize(&tx)`).
    pub async fn send_raw(&self, signed_tx_bytes: Vec<u8>) -> Result<String, SendError> {
        let encoded = B64.encode(&signed_tx_bytes);
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendTransaction",
            "params": [
                encoded,
                {"encoding": "base64", "skipPreflight": true, "maxRetries": 0}
            ]
        });

        let mut url = format!("{}/fast", self.endpoint);
        let mut qs: Vec<(&str, &str)> = Vec::with_capacity(2);
        if let Some(k) = self.api_key.as_deref() {
            qs.push(("api-key", k));
        }
        let swqos_str: &str = if self.swqos_only { "true" } else { "false" };
        qs.push(("swqos_only", swqos_str));

        if !qs.is_empty() {
            url.push('?');
            for (i, (k, v)) in qs.iter().enumerate() {
                if i > 0 { url.push('&'); }
                // simple encode: only api-key may contain special chars but Helius keys are hex/uuid
                url.push_str(k);
                url.push('=');
                url.push_str(v);
            }
        }

        let resp = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .body(serde_json::to_vec(&body).map_err(|e| SendError::Parse(e.to_string()))?)
            .send()
            .await?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(SendError::HttpStatus(status.as_u16(), text));
        }

        #[derive(Deserialize)]
        struct RpcResp {
            result: Option<String>,
            error: Option<RpcError>,
        }
        #[derive(Deserialize)]
        struct RpcError {
            message: String,
        }

        let parsed: RpcResp = serde_json::from_str(&text)
            .map_err(|e| SendError::Parse(format!("body: {text} err: {e}")))?;
        if let Some(e) = parsed.error {
            return Err(SendError::RpcError(e.message));
        }
        parsed
            .result
            .ok_or_else(|| SendError::Parse(format!("missing result: {text}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_client_with_no_api_key() {
        let s = HeliusSender::new("http://localhost:0".into(), None, true).unwrap();
        assert_eq!(s.endpoint, "http://localhost:0");
        assert!(s.api_key.is_none());
        assert!(s.swqos_only);
    }

    #[test]
    fn endpoint_trailing_slash_stripped() {
        let s = HeliusSender::new("http://localhost:0/".into(), Some("abc".into()), false).unwrap();
        assert_eq!(s.endpoint, "http://localhost:0");
    }

    #[test]
    fn base64_encoding_roundtrip() {
        let bytes = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let encoded = B64.encode(&bytes);
        assert_eq!(encoded, "3q2+7w==");
    }
}
