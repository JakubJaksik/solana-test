//! bloXroute Trader API sender — HTTP custom body shape.

use super::{SendOutcome, TxSender};
use crate::http_jsonrpc::{build_http_client, tx_to_base64};
use crate::outcome::RateLimitState;
use serde::{Deserialize, Serialize};
use solana_sdk::transaction::Transaction;
use std::str::FromStr;
use std::time::{Duration, Instant};

#[derive(Serialize)]
struct SubmitBody<'a> {
    transaction: SubmitTx<'a>,
    #[serde(rename = "skipPreFlight")]
    skip_preflight: bool,
    #[serde(rename = "frontRunningProtection")]
    front_running_protection: bool,
    #[serde(rename = "submitProtection")]
    submit_protection: &'static str,
    #[serde(rename = "useStakedRPCs")]
    use_staked_rpcs: bool,
}

#[derive(Serialize)]
struct SubmitTx<'a> {
    content: &'a str,
}

#[derive(Deserialize)]
struct SubmitResponse {
    signature: Option<String>,
}

pub struct BloxrouteSender {
    id: u8,
    name: String,
    endpoint: String,
    auth_header: String,
    client: reqwest::Client,
}

impl BloxrouteSender {
    pub fn new(
        id: u8,
        name: impl Into<String>,
        endpoint: impl Into<String>,
        auth_header: impl Into<String>,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            endpoint: endpoint.into(),
            auth_header: auth_header.into(),
            client: build_http_client(Duration::from_secs(5)),
        }
    }
}

#[async_trait::async_trait]
impl TxSender for BloxrouteSender {
    fn id(&self) -> u8 { self.id }
    fn name(&self) -> &str { &self.name }
    fn endpoint_url(&self) -> &str { &self.endpoint }
    fn protocol(&self) -> &'static str { "HTTP_PLAIN" }

    async fn send(&self, tx: &Transaction) -> SendOutcome {
        let send_at = Instant::now();
        let signature = tx.signatures.first().copied().unwrap_or_default();
        let b64 = tx_to_base64(tx);
        let body = serde_json::to_string(&SubmitBody {
            transaction: SubmitTx { content: &b64 },
            skip_preflight: true,
            front_running_protection: false,
            submit_protection: "SP_LOW",
            use_staked_rpcs: true,
        }).unwrap_or_default();

        let resp = self.client
            .post(&self.endpoint)
            .header("Content-Type", "application/json")
            .header("Authorization", &self.auth_header)
            .body(body)
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
                    let returned = serde_json::from_str::<SubmitResponse>(&text)
                        .ok()
                        .and_then(|r| r.signature)
                        .and_then(|s| solana_sdk::signature::Signature::from_str(&s).ok());
                    SendOutcome {
                        send_at, send_ack_at, signature: returned.unwrap_or(signature),
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
    fn body_shape_correct() {
        let body = serde_json::to_string(&SubmitBody {
            transaction: SubmitTx { content: "BASE64TX" },
            skip_preflight: true,
            front_running_protection: false,
            submit_protection: "SP_LOW",
            use_staked_rpcs: true,
        }).unwrap();
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["transaction"]["content"], "BASE64TX");
        assert_eq!(v["skipPreFlight"], true);
        assert_eq!(v["submitProtection"], "SP_LOW");
        assert_eq!(v["useStakedRPCs"], true);
    }

    #[test]
    fn protocol_is_http_plain() {
        let s = BloxrouteSender::new(0, "blox", "http://x", "auth");
        assert_eq!(s.protocol(), "HTTP_PLAIN");
    }
}
