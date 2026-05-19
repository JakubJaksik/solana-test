//! Helius Sender impl — HTTP POST to FRA fast endpoint.

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
