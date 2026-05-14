//! TxSender trait — uniform contract for all send mechanisms.

pub mod mock;

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
