//! TxSender trait — uniform contract for all send mechanisms.

pub mod allenhark;
pub mod astralane;
pub mod blockrazor;
pub mod bloxroute;
pub mod helius;
pub mod jito;
pub mod jito_bundle;
pub mod mock;
pub mod nextblock;
pub mod nozomi;
pub mod slot0;
pub mod syncro;
pub mod triton;

use crate::outcome::RateLimitState;
use solana_sdk::{signature::Signature, transaction::Transaction};
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct SendOutcome {
    pub send_at: Instant,
    pub send_ack_at: Option<Instant>,
    pub signature: Signature,
    pub provider_request_id: Option<String>,
    pub http_status: Option<u16>,
    pub rpc_err_code: Option<i32>,
    pub rpc_err_message: Option<String>,
    pub rate_limit_state: RateLimitState,
    pub error: Option<String>,
}

#[async_trait::async_trait]
pub trait TxSender: Send + Sync {
    fn id(&self) -> u8;
    fn name(&self) -> &str;
    fn endpoint_url(&self) -> &str;
    fn protocol(&self) -> &'static str;
    async fn send(&self, tx: &Transaction) -> SendOutcome;
}

/// Parse a reqwest response as JSON-RPC; fall back to text if non-JSON.
/// Shared logic for all JSON-RPC senders.
pub(crate) async fn parse_jsonrpc_or_text(
    resp_result: Result<reqwest::Response, reqwest::Error>,
    send_at: std::time::Instant,
    send_ack_at: Option<std::time::Instant>,
    signature: solana_sdk::signature::Signature,
) -> SendOutcome {
    use crate::http_jsonrpc::JsonRpcResponse;
    use crate::outcome::RateLimitState;
    use std::str::FromStr;

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
