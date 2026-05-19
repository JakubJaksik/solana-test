//! Triton One sender — HTTPS JSON-RPC with path-token auth.

use super::{parse_jsonrpc_or_text, SendOutcome, TxSender};
use crate::http_jsonrpc::{build_http_client, build_send_transaction_body, tx_to_base64};
use solana_sdk::transaction::Transaction;
use std::time::{Duration, Instant};

pub struct TritonSender {
    id: u8,
    name: String,
    endpoint: String,
    client: reqwest::Client,
}

impl TritonSender {
    pub fn new(id: u8, name: impl Into<String>, endpoint: impl Into<String>) -> Self {
        Self {
            id,
            name: name.into(),
            endpoint: endpoint.into(),
            client: build_http_client(Duration::from_secs(5)),
        }
    }
}

#[async_trait::async_trait]
impl TxSender for TritonSender {
    fn id(&self) -> u8 { self.id }
    fn name(&self) -> &str { &self.name }
    fn endpoint_url(&self) -> &str { &self.endpoint }
    fn protocol(&self) -> &'static str { "HTTP_JSONRPC" }

    async fn send(&self, tx: &Transaction) -> SendOutcome {
        let send_at = Instant::now();
        let signature = tx.signatures.first().copied().unwrap_or_default();
        let b64 = tx_to_base64(tx);
        let body = build_send_transaction_body(&b64, true, 0);

        let resp = self.client
            .post(&self.endpoint)
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .await;
        let send_ack_at = Some(Instant::now());
        parse_jsonrpc_or_text(resp, send_at, send_ack_at, signature).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn triton_construct() {
        let s = TritonSender::new(0, "triton", "https://x.mainnet.rpcpool.com/TOKEN");
        assert_eq!(s.endpoint_url(), "https://x.mainnet.rpcpool.com/TOKEN");
    }
}
