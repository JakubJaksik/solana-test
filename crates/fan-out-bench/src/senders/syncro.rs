//! Syncro Sender (P2P.org) — JSON-RPC with Bearer/X-Api-Key auth.

use super::{parse_jsonrpc_or_text, SendOutcome, TxSender};
use crate::http_jsonrpc::{build_http_client, build_send_transaction_body, tx_to_base64};
use solana_sdk::transaction::Transaction;
use std::time::{Duration, Instant};

pub enum SyncroAuth {
    None,
    Bearer(String),
    XApiKey(String),
}

pub struct SyncroSender {
    id: u8,
    name: String,
    endpoint: String,
    auth: SyncroAuth,
    client: reqwest::Client,
}

impl SyncroSender {
    pub fn new(
        id: u8,
        name: impl Into<String>,
        endpoint: impl Into<String>,
        auth: SyncroAuth,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            endpoint: endpoint.into(),
            auth,
            client: build_http_client(Duration::from_secs(5)),
        }
    }
}

#[async_trait::async_trait]
impl TxSender for SyncroSender {
    fn id(&self) -> u8 { self.id }
    fn name(&self) -> &str { &self.name }
    fn endpoint_url(&self) -> &str { &self.endpoint }
    fn protocol(&self) -> &'static str { "HTTP_JSONRPC" }

    async fn send(&self, tx: &Transaction) -> SendOutcome {
        let send_at = Instant::now();
        let signature = tx.signatures.first().copied().unwrap_or_default();
        let b64 = tx_to_base64(tx);
        let body = build_send_transaction_body(&b64, true, 0);

        let mut req = self.client
            .post(&self.endpoint)
            .header("Content-Type", "application/json")
            .body(body);
        match &self.auth {
            SyncroAuth::None => {}
            SyncroAuth::Bearer(t) => req = req.header("Authorization", format!("Bearer {}", t)),
            SyncroAuth::XApiKey(k) => req = req.header("X-Api-Key", k),
        }
        let resp = req.send().await;
        let send_ack_at = Some(Instant::now());
        parse_jsonrpc_or_text(resp, send_at, send_ack_at, signature).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn syncro_construct() {
        let s = SyncroSender::new(0, "syncro-priv", "https://x/rpc", SyncroAuth::Bearer("T".into()));
        assert_eq!(s.name(), "syncro-priv");
        assert_eq!(s.protocol(), "HTTP_JSONRPC");
    }
}
