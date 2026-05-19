//! AllenHark Relay sender — HTTPS REST POST.
//!
//! Endpoint: https://fra.relay.allenhark.com/v1/sendTx
//! Body: { "tx": "<BASE64>", "simulate": false }
//! Auth: x-api-key header (optional)

use super::{SendOutcome, TxSender};
use crate::http_jsonrpc::{build_http_client, tx_to_base64};
use crate::outcome::RateLimitState;
use serde::{Deserialize, Serialize};
use solana_sdk::transaction::Transaction;
use std::str::FromStr;
use std::time::{Duration, Instant};

#[derive(Serialize)]
struct AllenHarkBody<'a> {
    tx: &'a str,
    simulate: bool,
}

#[derive(Deserialize)]
#[allow(dead_code)]
struct AllenHarkResponse {
    status: Option<String>,
    request_id: Option<String>,
    signature: Option<String>,
    error: Option<String>,
}

pub struct AllenHarkSender {
    id: u8,
    name: String,
    endpoint: String,
    api_key: Option<String>,
    client: reqwest::Client,
}

impl AllenHarkSender {
    pub fn new(
        id: u8,
        name: impl Into<String>,
        endpoint: impl Into<String>,
        api_key: Option<String>,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            endpoint: endpoint.into(),
            api_key,
            client: build_http_client(Duration::from_secs(5)),
        }
    }
}

#[async_trait::async_trait]
impl TxSender for AllenHarkSender {
    fn id(&self) -> u8 { self.id }
    fn name(&self) -> &str { &self.name }
    fn endpoint_url(&self) -> &str { &self.endpoint }
    fn protocol(&self) -> &'static str { "HTTP_PLAIN" }

    async fn send(&self, tx: &Transaction) -> SendOutcome {
        let send_at = Instant::now();
        let signature = tx.signatures.first().copied().unwrap_or_default();
        let b64 = tx_to_base64(tx);
        let body = serde_json::to_string(&AllenHarkBody {
            tx: &b64,
            simulate: false,
        }).unwrap_or_default();

        let mut req = self.client
            .post(&self.endpoint)
            .header("Content-Type", "application/json")
            .body(body);
        if let Some(key) = &self.api_key {
            req = req.header("x-api-key", key);
        }

        let resp = req.send().await;
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
                let parsed: Option<AllenHarkResponse> = serde_json::from_str(&text).ok();
                let returned_sig = parsed.as_ref()
                    .and_then(|r| r.signature.as_deref())
                    .and_then(|s| solana_sdk::signature::Signature::from_str(s).ok());
                let provider_id = parsed.as_ref().and_then(|r| r.request_id.clone());
                let err_msg = parsed.as_ref().and_then(|r| r.error.clone());

                if status == 200 && err_msg.is_none() {
                    SendOutcome {
                        send_at, send_ack_at, signature: returned_sig.unwrap_or(signature),
                        provider_request_id: provider_id,
                        http_status: Some(status),
                        rpc_err_code: None,
                        rpc_err_message: None,
                        rate_limit_state: RateLimitState::Ok,
                        error: None,
                    }
                } else {
                    SendOutcome {
                        send_at, send_ack_at, signature,
                        provider_request_id: provider_id,
                        http_status: Some(status),
                        rpc_err_code: None,
                        rpc_err_message: err_msg.clone().or(Some(text.clone())),
                        rate_limit_state: if status == 429 { RateLimitState::Throttled429 } else { RateLimitState::Ok },
                        error: err_msg.or(Some(format!("HTTP {}: {}", status, text))),
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
    fn body_shape() {
        let body = serde_json::to_string(&AllenHarkBody { tx: "BASE64TX", simulate: false }).unwrap();
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["tx"], "BASE64TX");
        assert_eq!(v["simulate"], false);
    }

    #[test]
    fn allenhark_construct() {
        let s = AllenHarkSender::new(0, "ah", "https://x", Some("KEY".into()));
        assert_eq!(s.name(), "ah");
        assert_eq!(s.protocol(), "HTTP_PLAIN");
    }
}
