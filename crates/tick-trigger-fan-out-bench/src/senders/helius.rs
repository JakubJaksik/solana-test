//! Helius `/fast` JSON-RPC sender (HTTP).
//!
//! Endpoint accepts the standard `sendTransaction` JSON-RPC. We post a
//! single tx encoded as base64 with `skipPreflight=true` and
//! `preflightCommitment=processed`. Helius returns the signature on success
//! or a JSON-RPC error.

use super::{SendOutcome, TxSender};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use solana_sdk::transaction::Transaction;
use std::time::{Duration, Instant};

pub struct HeliusSender {
    id: u8,
    name: String,
    endpoint: String,
    client: reqwest::Client,
}

impl HeliusSender {
    pub fn new(id: u8, name: impl Into<String>, endpoint: impl Into<String>) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .tcp_nodelay(true)
            .pool_max_idle_per_host(8)
            .build()
            .expect("reqwest client");
        Self {
            id,
            name: name.into(),
            endpoint: endpoint.into(),
            client,
        }
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
    #[serde(rename = "preflightCommitment")]
    preflight_commitment: &'static str,
    #[serde(rename = "maxRetries")]
    max_retries: u32,
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

#[async_trait]
impl TxSender for HeliusSender {
    fn id(&self) -> u8 {
        self.id
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn endpoint_url(&self) -> &str {
        &self.endpoint
    }
    fn protocol(&self) -> &'static str {
        "HTTP_JSONRPC"
    }

    async fn send(&self, tx: &Transaction) -> SendOutcome {
        let signature = tx.signatures.first().copied().unwrap_or_default();
        let serialized = bincode::serialize(tx).unwrap_or_default();
        use base64::Engine as _;
        let b64 = base64::engine::general_purpose::STANDARD.encode(&serialized);
        let body = serde_json::to_string(&SendRequest {
            jsonrpc: "2.0",
            id: 1,
            method: "sendTransaction",
            params: (
                &b64,
                SendOptions {
                    encoding: "base64",
                    skip_preflight: true,
                    preflight_commitment: "processed",
                    max_retries: 0,
                },
            ),
        })
        .unwrap_or_default();

        let send_at = Instant::now();
        let result = self
            .client
            .post(&self.endpoint)
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .await;
        let send_ack_at = Some(Instant::now());

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
                endpoint_url_used: Some(self.endpoint.clone()),
            },
            Ok(resp) => {
                let status = resp.status().as_u16();
                let body_text = resp.text().await.unwrap_or_default();
                match serde_json::from_str::<JsonRpcResponse>(&body_text) {
                    Ok(parsed) => {
                        if let Some(err) = parsed.error {
                            SendOutcome {
                                send_at,
                                send_ack_at,
                                signature,
                                http_status: Some(status),
                                rpc_err_code: Some(err.code),
                                rpc_err_message: Some(err.message.clone()),
                                provider_request_id: None,
                                error: Some(err.message),
                                endpoint_url_used: Some(self.endpoint.clone()),
                            }
                        } else {
                            let returned_sig =
                                parsed.result.as_deref().and_then(|s| s.parse().ok());
                            SendOutcome {
                                send_at,
                                send_ack_at,
                                signature: returned_sig.unwrap_or(signature),
                                http_status: Some(status),
                                rpc_err_code: None,
                                rpc_err_message: None,
                                provider_request_id: None,
                                error: None,
                                endpoint_url_used: Some(self.endpoint.clone()),
                            }
                        }
                    }
                    Err(_) => SendOutcome {
                        send_at,
                        send_ack_at,
                        signature,
                        http_status: Some(status),
                        rpc_err_code: None,
                        rpc_err_message: Some(format!("non-JSONRPC body: {}", body_text)),
                        provider_request_id: None,
                        error: Some(format!("HTTP {} body: {}", status, body_text)),
                        endpoint_url_used: Some(self.endpoint.clone()),
                    },
                }
            }
        }
    }
}
