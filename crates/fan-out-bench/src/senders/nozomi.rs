//! Nozomi (Temporal) sender — HTTP JSON-RPC with ?c=<key> auth query param.

use super::{parse_jsonrpc_or_text, SendOutcome, TxSender};
use crate::http_jsonrpc::{build_http_client, build_send_transaction_body, tx_to_base64};
use solana_sdk::transaction::Transaction;
use std::time::{Duration, Instant};

pub struct NozomiSender {
    id: u8,
    name: String,
    endpoint: String,
    api_key: String,
    client: reqwest::Client,
}

impl NozomiSender {
    pub fn new(
        id: u8,
        name: impl Into<String>,
        endpoint: impl Into<String>,
        api_key: impl Into<String>,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            endpoint: endpoint.into(),
            api_key: api_key.into(),
            client: build_http_client(Duration::from_secs(5)),
        }
    }

    fn build_url(&self) -> String {
        format!("{}?c={}", self.endpoint, self.api_key)
    }
}

#[async_trait::async_trait]
impl TxSender for NozomiSender {
    fn id(&self) -> u8 { self.id }
    fn name(&self) -> &str { &self.name }
    fn endpoint_url(&self) -> &str { &self.endpoint }
    fn protocol(&self) -> &'static str { "HTTP_JSONRPC" }

    async fn send(&self, tx: &Transaction) -> SendOutcome {
        let send_at = Instant::now();
        let signature = tx.signatures.first().copied().unwrap_or_default();
        let b64 = tx_to_base64(tx);
        let body = build_send_transaction_body(&b64, true, 0);
        let url = self.build_url();

        let resp = self.client
            .post(&url)
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
    fn build_url_includes_api_key_query() {
        let s = NozomiSender::new(0, "nozomi-fra", "http://fra2.nozomi.temporal.xyz/", "KEY123");
        assert_eq!(s.build_url(), "http://fra2.nozomi.temporal.xyz/?c=KEY123");
    }

    #[test]
    fn protocol_correct() {
        let s = NozomiSender::new(0, "nozomi", "http://x", "k");
        assert_eq!(s.protocol(), "HTTP_JSONRPC");
    }
}
