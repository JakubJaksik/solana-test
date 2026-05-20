//! Jito bundle sender — POST to /api/v1/bundles with sendBundle method.

use super::{back_off_skip_outcome, BackOffState, SendOutcome, TxSender};
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
    back_off: BackOffState,
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
            back_off: BackOffState::default(),
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
        let signature = tx.signatures.first().copied().unwrap_or_default();
        if let Some(remaining) = self.back_off.remaining() {
            return back_off_skip_outcome(signature, remaining.as_millis());
        }
        let send_at = Instant::now();
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
                        if rate_limit_state == RateLimitState::Throttled429 {
                            if let Some(ms) = BackOffState::parse_retry_after_ms(&err.message) {
                                self.back_off.record_retry_after(ms);
                            }
                        }
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
