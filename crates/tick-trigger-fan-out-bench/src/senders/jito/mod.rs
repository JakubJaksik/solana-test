//! Jito HTTP `sendTransaction` sender.
//!
//! Fans out a single transaction over N regional hosts via JSON-RPC,
//! all bound to a single rotated source IP per send. A local throttle
//! enforces a minimum interval between sends to stay below Jito's
//! per-IP / anonymous rate limits.

pub mod json_rpc;
pub mod tip_updater;

use super::{SendOutcome, TxSender};
use async_trait::async_trait;
use base64::Engine as _;
use json_rpc::{
    JsonRpcMultiIpClient, JsonRpcResponse, SendTransactionOptions, SendTransactionRequest,
};
use solana_sdk::transaction::Transaction;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone, Default)]
pub struct JitoBundleCounters {
    pub txs_sent: Arc<AtomicU64>,
    pub first_reply_json_rpc: Arc<AtomicU64>,
    pub ip_send_count: Arc<[AtomicU64; 8]>,
}

pub struct JitoBundleSender {
    id: u8,
    name: String,
    endpoint_template: String,
    pub(crate) json_rpc: JsonRpcMultiIpClient,
    pub(crate) ip_count: usize,
    pub(crate) ip_cursor: AtomicUsize,
    pub(crate) current_tip_lamports: Arc<AtomicU64>,
    pub(crate) counters: JitoBundleCounters,
    min_send_interval: Duration,
    last_send_at: parking_lot::Mutex<Option<Instant>>,
}

impl JitoBundleSender {
    pub fn new(
        id: u8,
        name: String,
        endpoint_template: String,
        regions: Vec<String>,
        outbound_ips: Vec<String>,
        current_tip_lamports: Arc<AtomicU64>,
        min_send_interval_ms: u64,
        counters: JitoBundleCounters,
    ) -> Self {
        let json_rpc = JsonRpcMultiIpClient::new(&endpoint_template, &regions, &outbound_ips);
        let ip_count = json_rpc.ip_count();
        Self {
            id,
            name,
            endpoint_template,
            json_rpc,
            ip_count,
            ip_cursor: AtomicUsize::new(0),
            current_tip_lamports,
            counters,
            min_send_interval: Duration::from_millis(min_send_interval_ms),
            last_send_at: parking_lot::Mutex::new(None),
        }
    }

    pub fn current_tip_lamports(&self) -> u64 {
        self.current_tip_lamports.load(Ordering::Relaxed)
    }

    fn next_ip_idx(&self) -> usize {
        self.ip_cursor.fetch_add(1, Ordering::Relaxed) % self.ip_count
    }
}

enum FanoutReply {
    Success {
        host_url: String,
        send_ack_at: Instant,
        signature: String,
        http_status: Option<u16>,
    },
    Error {
        host_url: String,
        send_ack_at: Instant,
        http_status: Option<u16>,
        rpc_err_code: Option<i32>,
        rpc_err_message: Option<String>,
        error: String,
    },
}

#[async_trait]
impl TxSender for JitoBundleSender {
    fn id(&self) -> u8 { self.id }
    fn name(&self) -> &str { &self.name }
    fn endpoint_url(&self) -> &str { &self.endpoint_template }
    fn protocol(&self) -> &'static str { "JITO" }

    async fn send(&self, tx: &Transaction) -> SendOutcome {
        let send_at_init = Instant::now();
        let signature = tx.signatures.first().copied().unwrap_or_default();

        if self.min_send_interval > Duration::ZERO {
            let now = Instant::now();
            let mut last = self.last_send_at.lock();
            if let Some(prev) = *last {
                if now.duration_since(prev) < self.min_send_interval {
                    return SendOutcome {
                        send_at: now, send_ack_at: Some(now), signature,
                        http_status: None, rpc_err_code: None, rpc_err_message: None,
                        provider_request_id: None,
                        error: Some("throttled_local".into()),
                        endpoint_url_used: None,
                    };
                }
            }
            *last = Some(now);
        }

        if self.json_rpc.host_count() == 0 {
            return SendOutcome {
                send_at: send_at_init, send_ack_at: None, signature,
                http_status: None, rpc_err_code: None, rpc_err_message: None,
                provider_request_id: None,
                error: Some("no regions configured".into()),
                endpoint_url_used: None,
            };
        }

        let raw = bincode::serialize(tx).unwrap_or_default();
        let b64 = base64::engine::general_purpose::STANDARD.encode(&raw);
        let body = serde_json::to_string(&SendTransactionRequest {
            jsonrpc: "2.0",
            id: 1,
            method: "sendTransaction",
            params: (
                b64.as_str(),
                SendTransactionOptions {
                    encoding: "base64",
                    skip_preflight: true,
                    max_retries: 0,
                },
            ),
        })
        .unwrap_or_default();
        let body_arc: Arc<String> = Arc::new(body);

        let ip_idx = self.next_ip_idx();
        self.counters.txs_sent.fetch_add(1, Ordering::Relaxed);
        if ip_idx < self.counters.ip_send_count.len() {
            self.counters.ip_send_count[ip_idx].fetch_add(1, Ordering::Relaxed);
        }
        let send_at = Instant::now();
        let total_paths = self.json_rpc.host_count();
        let (tx_first, mut rx_first) = tokio::sync::mpsc::channel::<FanoutReply>(total_paths.max(1));

        for host_idx in 0..self.json_rpc.host_count() {
            let host_url = self.json_rpc.hosts[host_idx].clone();
            let body = body_arc.clone();
            let tx_first = tx_first.clone();
            let client = self.json_rpc.grid_client(host_idx, ip_idx);
            tokio::spawn(async move {
                let result = client
                    .post(&host_url)
                    .header("Content-Type", "application/json")
                    .body((*body).clone())
                    .send().await;
                let send_ack_at = Instant::now();
                let reply = match result {
                    Ok(resp) => {
                        let status = resp.status().as_u16();
                        let text = resp.text().await.unwrap_or_default();
                        parse_json_rpc_reply(host_url, status, text, send_ack_at)
                    }
                    Err(e) => FanoutReply::Error {
                        host_url, send_ack_at,
                        http_status: None, rpc_err_code: None, rpc_err_message: None,
                        error: format!("network: {}", e),
                    },
                };
                let _ = tx_first.send(reply).await;
            });
        }
        drop(tx_first);

        let Some(first) = rx_first.recv().await else {
            return SendOutcome {
                send_at, send_ack_at: None, signature,
                http_status: None, rpc_err_code: None, rpc_err_message: None,
                provider_request_id: None,
                error: Some("all fan-out tasks dropped without reply".into()),
                endpoint_url_used: None,
            };
        };

        match first {
            FanoutReply::Success { host_url, send_ack_at, signature: sig_str, http_status } => {
                self.counters.first_reply_json_rpc.fetch_add(1, Ordering::Relaxed);
                SendOutcome {
                    send_at, send_ack_at: Some(send_ack_at), signature,
                    http_status, rpc_err_code: None, rpc_err_message: None,
                    provider_request_id: Some(sig_str),
                    error: None,
                    endpoint_url_used: Some(host_url),
                }
            }
            FanoutReply::Error { host_url, send_ack_at, http_status, rpc_err_code, rpc_err_message, error } => SendOutcome {
                send_at, send_ack_at: Some(send_ack_at), signature,
                http_status, rpc_err_code, rpc_err_message,
                provider_request_id: None,
                error: Some(error),
                endpoint_url_used: Some(host_url),
            },
        }
    }
}

fn parse_json_rpc_reply(host_url: String, status: u16, body: String, send_ack_at: Instant) -> FanoutReply {
    match serde_json::from_str::<JsonRpcResponse>(&body) {
        Ok(parsed) => {
            if let Some(err) = parsed.error {
                FanoutReply::Error {
                    host_url, send_ack_at,
                    http_status: Some(status),
                    rpc_err_code: Some(err.code),
                    rpc_err_message: Some(err.message.clone()),
                    error: err.message,
                }
            } else if let Some(sig) = parsed.result {
                FanoutReply::Success {
                    host_url, send_ack_at, signature: sig, http_status: Some(status),
                }
            } else {
                FanoutReply::Error {
                    host_url, send_ack_at,
                    http_status: Some(status),
                    rpc_err_code: None,
                    rpc_err_message: Some("empty result".into()),
                    error: "empty result".into(),
                }
            }
        }
        Err(_) => FanoutReply::Error {
            host_url, send_ack_at,
            http_status: Some(status),
            rpc_err_code: None,
            rpc_err_message: Some(format!("non-JSONRPC body: {body}")),
            error: format!("HTTP {status} body: {body}"),
        },
    }
}

#[cfg(test)]
mod sender_tests {
    use super::*;
    use std::sync::atomic::Ordering;

    fn make_sender() -> JitoBundleSender {
        JitoBundleSender::new(
            7, "jito-test".into(),
            "https://{region}.x/api/v1/transactions".into(),
            vec!["frankfurt".into(), "amsterdam".into()],
            vec!["10.0.0.1".into(), "10.0.0.2".into(), "10.0.0.3".into()],
            Arc::new(AtomicU64::new(50_000)),
            0,
            JitoBundleCounters::default(),
        )
    }

    #[test]
    fn ip_cursor_rotates_round_robin() {
        let s = make_sender();
        let a = s.ip_cursor.fetch_add(1, Ordering::Relaxed) % s.ip_count;
        let b = s.ip_cursor.fetch_add(1, Ordering::Relaxed) % s.ip_count;
        let c = s.ip_cursor.fetch_add(1, Ordering::Relaxed) % s.ip_count;
        let d = s.ip_cursor.fetch_add(1, Ordering::Relaxed) % s.ip_count;
        assert_eq!((a, b, c, d), (0, 1, 2, 0));
    }

    #[test]
    fn current_tip_lamports_reads_atomic() {
        let s = make_sender();
        assert_eq!(s.current_tip_lamports(), 50_000);
        s.current_tip_lamports.store(123_456, Ordering::Relaxed);
        assert_eq!(s.current_tip_lamports(), 123_456);
    }

    #[test]
    fn protocol_label_is_jito() {
        let s = make_sender();
        assert_eq!(s.protocol(), "JITO");
    }

    #[tokio::test]
    async fn send_with_no_regions_returns_error() {
        let s = JitoBundleSender::new(
            7, "jito-test".into(),
            "https://{region}.x/api/v1/transactions".into(),
            vec![],
            vec!["10.0.0.1".into()],
            Arc::new(AtomicU64::new(50_000)),
            0,
            JitoBundleCounters::default(),
        );
        let tx = Transaction::default();
        let outcome = s.send(&tx).await;
        assert_eq!(outcome.error.as_deref(), Some("no regions configured"));
    }
}
