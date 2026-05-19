//! NextBlock sender — HTTPS REST POST /api/v2/submit.

use super::{SendOutcome, TxSender};
use crate::http_jsonrpc::{build_http_client, tx_to_base64};
use crate::outcome::RateLimitState;
use serde::{Deserialize, Serialize};
use solana_sdk::transaction::Transaction;
use std::str::FromStr;
use std::time::{Duration, Instant};

#[derive(Serialize)]
struct NextBlockBody<'a> {
    transaction: NextBlockTx<'a>,
    #[serde(rename = "skipPreFlight")]
    skip_preflight: bool,
    #[serde(rename = "frontRunningProtection")]
    front_running_protection: bool,
    #[serde(rename = "disableRetries")]
    disable_retries: bool,
    #[serde(rename = "revertOnFail")]
    revert_on_fail: bool,
    #[serde(rename = "snipeTransaction")]
    snipe_transaction: bool,
}

#[derive(Serialize)]
struct NextBlockTx<'a> {
    content: &'a str,
}

#[derive(Deserialize)]
#[allow(dead_code)]
struct NextBlockResponse {
    signature: Option<String>,
    uuid: Option<String>,
    message: Option<String>,
    code: Option<i32>,
}

pub struct NextBlockSender {
    id: u8,
    name: String,
    endpoint: String,
    auth_header: String,
    client: reqwest::Client,
}

impl NextBlockSender {
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
impl TxSender for NextBlockSender {
    fn id(&self) -> u8 { self.id }
    fn name(&self) -> &str { &self.name }
    fn endpoint_url(&self) -> &str { &self.endpoint }
    fn protocol(&self) -> &'static str { "HTTP_PLAIN" }

    async fn send(&self, tx: &Transaction) -> SendOutcome {
        let send_at = Instant::now();
        let signature = tx.signatures.first().copied().unwrap_or_default();
        let b64 = tx_to_base64(tx);
        let body = serde_json::to_string(&NextBlockBody {
            transaction: NextBlockTx { content: &b64 },
            skip_preflight: true,
            front_running_protection: false,
            disable_retries: false,
            revert_on_fail: false,
            snipe_transaction: false,
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
                let parsed: Option<NextBlockResponse> = serde_json::from_str(&text).ok();
                let returned_sig = parsed.as_ref()
                    .and_then(|r| r.signature.as_deref())
                    .and_then(|s| solana_sdk::signature::Signature::from_str(s).ok());
                let uuid = parsed.as_ref().and_then(|r| r.uuid.clone());
                let has_err_code = parsed.as_ref().and_then(|r| r.code).is_some();

                if status == 200 && !has_err_code {
                    SendOutcome {
                        send_at, send_ack_at, signature: returned_sig.unwrap_or(signature),
                        provider_request_id: uuid,
                        http_status: Some(status),
                        rpc_err_code: None,
                        rpc_err_message: None,
                        rate_limit_state: RateLimitState::Ok,
                        error: None,
                    }
                } else {
                    let code = parsed.as_ref().and_then(|r| r.code);
                    let msg = parsed.as_ref().and_then(|r| r.message.clone()).unwrap_or_else(|| text.clone());
                    SendOutcome {
                        send_at, send_ack_at, signature,
                        provider_request_id: uuid,
                        http_status: Some(status),
                        rpc_err_code: code,
                        rpc_err_message: Some(msg.clone()),
                        rate_limit_state: if status == 429 { RateLimitState::Throttled429 } else { RateLimitState::Ok },
                        error: Some(msg),
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
        let body = serde_json::to_string(&NextBlockBody {
            transaction: NextBlockTx { content: "BASE64TX" },
            skip_preflight: true,
            front_running_protection: false,
            disable_retries: false,
            revert_on_fail: false,
            snipe_transaction: false,
        }).unwrap();
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["transaction"]["content"], "BASE64TX");
        assert_eq!(v["skipPreFlight"], true);
    }
}
