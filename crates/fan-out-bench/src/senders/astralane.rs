//! Astralane Iris sender — HTTP plaintext base64 body via /iris2 endpoint.

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
