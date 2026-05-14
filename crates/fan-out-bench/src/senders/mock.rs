//! MockSender — for tests and end-to-end mock pipeline.

use super::{SendOutcome, TxSender};
use crate::outcome::RateLimitState;
use solana_sdk::transaction::Transaction;
use std::time::{Duration, Instant};

pub struct MockSender {
    id: u8,
    name: String,
    endpoint_url: String,
    pub ack_delay: Duration,
    pub mode: MockMode,
}

#[derive(Clone)]
pub enum MockMode {
    AlwaysAck,
    AlwaysError(String),
    AckHalfRandom { seed: u64 },
}

impl MockSender {
    pub fn always_ack(id: u8, name: impl Into<String>) -> Self {
        Self {
            id,
            name: name.into(),
            endpoint_url: "mock://always-ack".into(),
            ack_delay: Duration::from_millis(1),
            mode: MockMode::AlwaysAck,
        }
    }

    pub fn always_error(id: u8, name: impl Into<String>, err: impl Into<String>) -> Self {
        let err = err.into();
        Self {
            id,
            name: name.into(),
            endpoint_url: "mock://error".into(),
            ack_delay: Duration::from_millis(1),
            mode: MockMode::AlwaysError(err),
        }
    }
}

#[async_trait::async_trait]
impl TxSender for MockSender {
    fn id(&self) -> u8 { self.id }
    fn name(&self) -> &str { &self.name }
    fn endpoint_url(&self) -> &str { &self.endpoint_url }
    fn protocol(&self) -> &'static str { "MOCK" }

    async fn send(&self, tx: &Transaction) -> SendOutcome {
        let send_at = Instant::now();
        tokio::time::sleep(self.ack_delay).await;
        let send_ack_at = Some(Instant::now());
        let signature = tx.signatures.first().copied().unwrap_or_default();
        match &self.mode {
            MockMode::AlwaysAck => SendOutcome {
                send_at, send_ack_at, signature,
                provider_request_id: Some(format!("mock-{}-{}", self.name, signature)),
                http_status: Some(200),
                rpc_err_code: None,
                rpc_err_message: None,
                rate_limit_state: RateLimitState::Ok,
                error: None,
            },
            MockMode::AlwaysError(msg) => SendOutcome {
                send_at, send_ack_at: None, signature,
                provider_request_id: None,
                http_status: Some(500),
                rpc_err_code: Some(-32000),
                rpc_err_message: Some(msg.clone()),
                rate_limit_state: RateLimitState::Ok,
                error: Some(msg.clone()),
            },
            MockMode::AckHalfRandom { seed } => {
                let h = (signature.as_ref()[0] as u64) ^ seed;
                if h % 2 == 0 {
                    SendOutcome {
                        send_at, send_ack_at, signature,
                        provider_request_id: None,
                        http_status: Some(200),
                        rpc_err_code: None,
                        rpc_err_message: None,
                        rate_limit_state: RateLimitState::Ok,
                        error: None,
                    }
                } else {
                    SendOutcome {
                        send_at, send_ack_at: None, signature,
                        provider_request_id: None,
                        http_status: Some(429),
                        rpc_err_code: None,
                        rpc_err_message: Some("mock rate limited".into()),
                        rate_limit_state: RateLimitState::Throttled429,
                        error: Some("mock rate limited".into()),
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::transaction::Transaction;

    #[tokio::test]
    async fn always_ack_returns_ok() {
        let sender = MockSender::always_ack(0, "mock");
        let outcome = sender.send(&Transaction::default()).await;
        assert!(outcome.error.is_none());
        assert!(outcome.send_ack_at.is_some());
        assert_eq!(outcome.http_status, Some(200));
    }

    #[tokio::test]
    async fn always_error_returns_err() {
        let sender = MockSender::always_error(0, "mock", "boom");
        let outcome = sender.send(&Transaction::default()).await;
        assert_eq!(outcome.error.as_deref(), Some("boom"));
        assert!(outcome.send_ack_at.is_none());
    }
}
