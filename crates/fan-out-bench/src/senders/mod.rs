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

/// Shared back-off state for senders that respect "Retry after Nms" hints
/// (notably Jito Block Engine). When the endpoint returns 429 with a retry
/// delay, we record `now + delay` and refuse to send again until expired —
/// this avoids triggering exponential back-off cascades that lock our IP for
/// up to 2 minutes.
#[derive(Default)]
pub struct BackOffState {
    until: parking_lot::Mutex<Option<std::time::Instant>>,
}

impl BackOffState {
    pub fn remaining(&self) -> Option<std::time::Duration> {
        let guard = self.until.lock();
        let t = (*guard)?;
        t.checked_duration_since(std::time::Instant::now())
    }

    pub fn record_retry_after(&self, ms: u64) {
        let until = std::time::Instant::now() + std::time::Duration::from_millis(ms);
        let mut guard = self.until.lock();
        // Take the LATEST of current and proposed — never shorten an existing window.
        let new = match *guard {
            Some(existing) if existing > until => existing,
            _ => until,
        };
        *guard = Some(new);
    }

    /// Extract "Retry after Nms" / "Retry after N ms" from an error message,
    /// matching Jito's wording exactly. Returns None if the format differs.
    pub fn parse_retry_after_ms(message: &str) -> Option<u64> {
        let idx = message.find("Retry after ")?;
        let tail = &message[idx + "Retry after ".len()..];
        let digits: String = tail.chars().take_while(|c| c.is_ascii_digit()).collect();
        if digits.is_empty() {
            return None;
        }
        digits.parse().ok()
    }
}

/// Build a SendOutcome representing a request that was skipped client-side
/// because the sender is in a self-imposed back-off window.
pub(crate) fn back_off_skip_outcome(
    signature: solana_sdk::signature::Signature,
    remaining_ms: u128,
) -> SendOutcome {
    let now = std::time::Instant::now();
    SendOutcome {
        send_at: now,
        send_ack_at: Some(now),
        signature,
        provider_request_id: None,
        http_status: None,
        rpc_err_code: None,
        rpc_err_message: Some(format!("client back-off active for {} ms", remaining_ms)),
        rate_limit_state: RateLimitState::Throttled429,
        error: Some(format!("client back-off: skip ({} ms remaining)", remaining_ms)),
    }
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

#[cfg(test)]
mod back_off_tests {
    use super::*;

    #[test]
    fn parses_jito_retry_after_format() {
        let msg = "Rate limit exceeded. Limit: 1 per second for txn requests. Back-off triggered: Retry after 1606ms";
        assert_eq!(BackOffState::parse_retry_after_ms(msg), Some(1606));
    }

    #[test]
    fn parses_with_space_before_ms() {
        let msg = "Retry after 1000 ms";
        assert_eq!(BackOffState::parse_retry_after_ms(msg), Some(1000));
    }

    #[test]
    fn returns_none_when_format_differs() {
        assert!(BackOffState::parse_retry_after_ms("Network congested. Endpoint is globally rate limited.").is_none());
    }

    #[test]
    fn remaining_returns_none_when_no_back_off() {
        let s = BackOffState::default();
        assert!(s.remaining().is_none());
    }

    #[test]
    fn remaining_some_after_record() {
        let s = BackOffState::default();
        s.record_retry_after(500);
        assert!(s.remaining().is_some());
        assert!(s.remaining().unwrap().as_millis() <= 500);
    }

    #[test]
    fn record_never_shortens_window() {
        let s = BackOffState::default();
        s.record_retry_after(5000);
        s.record_retry_after(100);
        let remaining_ms = s.remaining().unwrap().as_millis();
        assert!(remaining_ms > 1000, "shorter retry should not override longer one (got {} ms)", remaining_ms);
    }
}
