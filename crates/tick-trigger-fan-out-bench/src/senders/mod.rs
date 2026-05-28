//! `TxSender` trait — uniform contract for all send mechanisms.
//!
//! Adding a new vendor / protocol means adding one module under `senders/`
//! and one `SenderKind` variant in `crate::config`. The hot path in
//! `trigger_engine` only sees the trait.

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
    pub endpoint_url_used: Option<String>,
}

#[async_trait]
pub trait TxSender: Send + Sync {
    fn id(&self) -> u8;
    fn name(&self) -> &str;
    fn endpoint_url(&self) -> &str;
    fn protocol(&self) -> &'static str;
    async fn send(&self, tx: &Transaction) -> SendOutcome;
}
