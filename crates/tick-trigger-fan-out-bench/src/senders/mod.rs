//! `TxSender` trait — uniform contract for all send mechanisms.
//!
//! Adding a new vendor / protocol means adding one module under `senders/`
//! and one `SenderKind` variant in `crate::config`. The hot path in
//! `trigger_engine` only sees the trait. HTTP, QUIC, gRPC, in-house clients
//! all fit the same shape.

pub mod helius;
pub mod jito;

use async_trait::async_trait;
use solana_sdk::{signature::Signature, transaction::Transaction};
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct SendOutcome {
    pub send_at: Instant,
    pub send_ack_at: Option<Instant>,
    pub signature: Signature,
    pub http_status: Option<u16>,
    pub rpc_err_code: Option<i32>,
    pub rpc_err_message: Option<String>,
    pub provider_request_id: Option<String>,
    pub error: Option<String>,
    /// Which endpoint the sender actually used (relevant later for vendors
    /// with multi-region fan-out; here it equals the static endpoint).
    pub endpoint_url_used: Option<String>,
}

#[async_trait]
pub trait TxSender: Send + Sync {
    fn id(&self) -> u8;
    fn name(&self) -> &str;
    fn endpoint_url(&self) -> &str;
    fn protocol(&self) -> &'static str;
    async fn send(&self, tx: &Transaction) -> SendOutcome;

    /// Default: returns a `SendOutcome` flagged as unsupported. Only the
    /// JitoBundleSender overrides this.
    async fn send_bundle(&self, _txs: &[Transaction]) -> SendOutcome {
        SendOutcome {
            send_at: Instant::now(),
            send_ack_at: None,
            signature: Signature::default(),
            http_status: None,
            rpc_err_code: None,
            rpc_err_message: None,
            provider_request_id: None,
            error: Some("sender does not support bundles".into()),
            endpoint_url_used: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct DummySender;

    #[async_trait]
    impl TxSender for DummySender {
        fn id(&self) -> u8 { 0 }
        fn name(&self) -> &str { "dummy" }
        fn endpoint_url(&self) -> &str { "" }
        fn protocol(&self) -> &'static str { "DUMMY" }
        async fn send(&self, _tx: &Transaction) -> SendOutcome {
            SendOutcome {
                send_at: Instant::now(),
                send_ack_at: None,
                signature: Signature::default(),
                http_status: None,
                rpc_err_code: None,
                rpc_err_message: None,
                provider_request_id: None,
                error: None,
                endpoint_url_used: None,
            }
        }
    }

    #[tokio::test]
    async fn default_send_bundle_returns_unsupported_error() {
        let sender = DummySender;
        let txs = vec![Transaction::default(), Transaction::default()];
        let outcome = sender.send_bundle(&txs).await;
        assert_eq!(outcome.error.as_deref(), Some("sender does not support bundles"));
        assert!(outcome.send_ack_at.is_none());
        assert!(outcome.provider_request_id.is_none());
    }
}
