//! BlockRazor sender — HTTP v2 plaintext base64 body.

use super::{SendOutcome, TxSender};
use crate::http_jsonrpc::{build_http_client, tx_to_base64};
use crate::outcome::RateLimitState;
use solana_sdk::transaction::Transaction;
use std::time::{Duration, Instant};

pub struct BlockRazorSender {
    id: u8,
    name: String,
    endpoint: String,
    auth_token: String,
    mode: String,
    client: reqwest::Client,
}

impl BlockRazorSender {
    pub fn new(
        id: u8,
        name: impl Into<String>,
        endpoint: impl Into<String>,
        auth_token: impl Into<String>,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            endpoint: endpoint.into(),
            auth_token: auth_token.into(),
            mode: "fast".to_string(),
            client: build_http_client(Duration::from_secs(5)),
        }
    }

    fn build_url(&self) -> String {
        format!(
            "{}?auth={}&mode={}&revertProtection=false",
            self.endpoint, self.auth_token, self.mode
        )
    }
}

#[async_trait::async_trait]
impl TxSender for BlockRazorSender {
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
                let parsed: Option<serde_json::Value> = serde_json::from_str(&text).ok();
                let err_msg = parsed.as_ref()
                    .and_then(|v| v.get("error"))
                    .and_then(|e| e.as_str())
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string());
                if status == 200 && err_msg.is_none() {
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
    fn build_url_has_all_params() {
        let s = BlockRazorSender::new(0, "br", "http://frankfurt.solana.blockrazor.xyz:443/v2/sendTransaction", "TOKEN");
        let url = s.build_url();
        assert!(url.contains("auth=TOKEN"));
        assert!(url.contains("mode=fast"));
        assert!(url.contains("revertProtection=false"));
    }
}
