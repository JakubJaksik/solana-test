//! Jito Block Engine bundle sender.
//!
//! Sends a 2-tx bundle (durable-nonce Tx1 + throwaway-tipper Tx2) over
//! 8 regional hosts × {JSON-RPC, gRPC} = 16 parallel paths, all bound
//! to a single rotated source IP per send.

pub mod auth;
pub mod json_rpc;
pub mod grpc;
pub mod tip_updater;

/// Generated protobuf types.
pub mod proto {
    pub mod packet { tonic::include_proto!("packet"); }
    pub mod shared { tonic::include_proto!("shared"); }
    pub mod bundle { tonic::include_proto!("bundle"); }
    pub mod searcher { tonic::include_proto!("searcher"); }
    pub mod auth { tonic::include_proto!("auth"); }
}

use super::{SendOutcome, TxSender};
use async_trait::async_trait;
use base64::Engine as _;
use grpc::GrpcMultiIpClient;
use json_rpc::{JsonRpcMultiIpClient, JsonRpcResponse, SendBundleOptions, SendBundleRequest};
use solana_sdk::transaction::Transaction;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Per-sender metric counters. Cloned-Arc so both the sender and the
/// dispatcher's reporting code see the same atomics.
#[derive(Clone, Default)]
pub struct JitoBundleCounters {
    pub bundles_sent: Arc<AtomicU64>,
    pub first_reply_json_rpc: Arc<AtomicU64>,
    pub first_reply_grpc: Arc<AtomicU64>,
    pub ip_send_count: Arc<[AtomicU64; 8]>,
}

pub struct JitoBundleSender {
    id: u8,
    name: String,
    endpoint_template: String,
    pub(crate) json_rpc: JsonRpcMultiIpClient,
    pub(crate) grpc: Option<GrpcMultiIpClient>,
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
        use_grpc: bool,
        current_tip_lamports: Arc<AtomicU64>,
        min_send_interval_ms: u64,
        counters: JitoBundleCounters,
    ) -> Result<Self, tonic::transport::Error> {
        let json_rpc = JsonRpcMultiIpClient::new(&endpoint_template, &regions, &outbound_ips);
        let grpc = if use_grpc {
            Some(GrpcMultiIpClient::new(&endpoint_template, &regions, &outbound_ips)?)
        } else {
            None
        };
        let ip_count = json_rpc.ip_count();
        Ok(Self {
            id,
            name,
            endpoint_template,
            json_rpc,
            grpc,
            ip_count,
            ip_cursor: AtomicUsize::new(0),
            current_tip_lamports,
            counters,
            min_send_interval: Duration::from_millis(min_send_interval_ms),
            last_send_at: parking_lot::Mutex::new(None),
        })
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
        method: &'static str,
        host_url: String,
        send_ack_at: Instant,
        bundle_id: String,
        http_status: Option<u16>,
    },
    Error {
        method: &'static str,
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
    fn protocol(&self) -> &'static str { "JITO_BUNDLE" }

    async fn send(&self, tx: &Transaction) -> SendOutcome {
        // Wrap as a 1-tx bundle. This is the standard Jito path for the
        // single-tx mode (tip baked into the same tx as the workload).
        let txs = [tx.clone()];
        self.send_bundle(&txs).await
    }

    async fn send_bundle(&self, txs: &[Transaction]) -> SendOutcome {
        let send_at_init = Instant::now();
        let signature = txs.first().and_then(|t| t.signatures.first().copied()).unwrap_or_default();

        // Local throttle.
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

        // Serialize each tx once.
        let raw: Vec<Vec<u8>> = txs.iter().map(|t| bincode::serialize(t).unwrap_or_default()).collect();
        let b64_owned: Vec<String> = raw.iter().map(|b| base64::engine::general_purpose::STANDARD.encode(b)).collect();
        let b64_refs: Vec<&str> = b64_owned.iter().map(String::as_str).collect();
        let body = serde_json::to_string(&SendBundleRequest {
            jsonrpc: "2.0", id: 1, method: "sendBundle",
            params: (b64_refs, SendBundleOptions { encoding: "base64" }),
        }).unwrap_or_default();
        let body_arc: Arc<String> = Arc::new(body);

        let ip_idx = self.next_ip_idx();
        self.counters.bundles_sent.fetch_add(1, Ordering::Relaxed);
        if ip_idx < self.counters.ip_send_count.len() {
            self.counters.ip_send_count[ip_idx].fetch_add(1, Ordering::Relaxed);
        }
        let send_at = Instant::now();
        let total_paths = self.json_rpc.host_count() + self.grpc.as_ref().map(|g| g.host_count()).unwrap_or(0);
        let (tx_first, mut rx_first) = tokio::sync::mpsc::channel::<FanoutReply>(total_paths.max(1));

        // JSON-RPC fan-out.
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
                        parse_json_rpc_reply(host_url, status, text, send_ack_at, "JSON-RPC")
                    }
                    Err(e) => FanoutReply::Error {
                        method: "JSON-RPC", host_url, send_ack_at,
                        http_status: None, rpc_err_code: None, rpc_err_message: None,
                        error: format!("network: {}", e),
                    },
                };
                let _ = tx_first.send(reply).await;
            });
        }

        // gRPC fan-out (if enabled).
        if let Some(grpc) = &self.grpc {
            let packet_bytes = raw.clone();
            for host_idx in 0..grpc.host_count() {
                let host = grpc.hosts[host_idx].clone();
                let host_url = format!("https://{host}:443");
                let packets = packet_bytes.clone();
                let tx_first = tx_first.clone();
                let channel = grpc.grid_channel(host_idx, ip_idx);
                tokio::spawn(async move {
                    use proto::bundle::Bundle as PbBundle;
                    use proto::packet::Packet as PbPacket;
                    use proto::searcher::searcher_service_client::SearcherServiceClient;
                    use proto::searcher::SendBundleRequest as PbSendBundleRequest;
                    let mut client = SearcherServiceClient::new(channel);
                    let pb_packets: Vec<PbPacket> = packets.iter().map(|b| PbPacket { data: b.clone(), meta: None }).collect();
                    let req = tonic::Request::new(PbSendBundleRequest { bundle: Some(PbBundle { header: None, packets: pb_packets }) });
                    let res = client.send_bundle(req).await;
                    let send_ack_at = Instant::now();
                    let reply = match res {
                        Ok(r) => FanoutReply::Success {
                            method: "gRPC", host_url,
                            send_ack_at, bundle_id: r.into_inner().uuid,
                            http_status: None,
                        },
                        Err(status) => FanoutReply::Error {
                            method: "gRPC", host_url, send_ack_at,
                            http_status: None,
                            rpc_err_code: Some(status.code() as i32),
                            rpc_err_message: Some(status.message().to_string()),
                            error: format!("grpc: {:?}: {}", status.code(), status.message()),
                        },
                    };
                    let _ = tx_first.send(reply).await;
                });
            }
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
            FanoutReply::Success { method, host_url, send_ack_at, bundle_id, http_status } => {
                match method {
                    "JSON-RPC" => { self.counters.first_reply_json_rpc.fetch_add(1, Ordering::Relaxed); }
                    "gRPC" => { self.counters.first_reply_grpc.fetch_add(1, Ordering::Relaxed); }
                    _ => {}
                }
                SendOutcome {
                send_at, send_ack_at: Some(send_ack_at), signature,
                http_status, rpc_err_code: None, rpc_err_message: None,
                provider_request_id: Some(bundle_id),
                error: None,
                endpoint_url_used: Some(format!("{}/{}", host_url, method)),
                }
            }
            FanoutReply::Error { method, host_url, send_ack_at, http_status, rpc_err_code, rpc_err_message, error } => SendOutcome {
                send_at, send_ack_at: Some(send_ack_at), signature,
                http_status, rpc_err_code, rpc_err_message,
                provider_request_id: None,
                error: Some(error),
                endpoint_url_used: Some(format!("{}/{}", host_url, method)),
            },
        }
    }
}

fn parse_json_rpc_reply(host_url: String, status: u16, body: String, send_ack_at: Instant, method: &'static str) -> FanoutReply {
    match serde_json::from_str::<JsonRpcResponse>(&body) {
        Ok(parsed) => {
            if let Some(err) = parsed.error {
                FanoutReply::Error {
                    method, host_url, send_ack_at,
                    http_status: Some(status),
                    rpc_err_code: Some(err.code),
                    rpc_err_message: Some(err.message.clone()),
                    error: err.message,
                }
            } else if let Some(bundle_id) = parsed.result {
                FanoutReply::Success {
                    method, host_url, send_ack_at, bundle_id, http_status: Some(status),
                }
            } else {
                FanoutReply::Error {
                    method, host_url, send_ack_at,
                    http_status: Some(status),
                    rpc_err_code: None,
                    rpc_err_message: Some("empty result".into()),
                    error: "empty result".into(),
                }
            }
        }
        Err(_) => FanoutReply::Error {
            method, host_url, send_ack_at,
            http_status: Some(status),
            rpc_err_code: None,
            rpc_err_message: Some(format!("non-JSONRPC body: {body}")),
            error: format!("HTTP {status} body: {body}"),
        },
    }
}

#[cfg(test)]
mod proto_smoke {
    use super::proto::bundle::Bundle;
    use super::proto::packet::Packet;
    use super::proto::searcher::SendBundleRequest;

    #[test]
    fn generated_types_construct() {
        let pkt = Packet { data: vec![1, 2, 3], meta: None };
        let bundle = Bundle { header: None, packets: vec![pkt] };
        let req = SendBundleRequest { bundle: Some(bundle) };
        assert_eq!(req.bundle.unwrap().packets[0].data, vec![1, 2, 3]);
    }
}

#[cfg(test)]
mod sender_tests {
    use super::*;
    use std::sync::atomic::Ordering;

    fn make_sender() -> JitoBundleSender {
        JitoBundleSender::new(
            7, "jito-test".into(),
            "https://{region}.x".into(),
            vec!["frankfurt".into(), "amsterdam".into()],
            vec!["10.0.0.1".into(), "10.0.0.2".into(), "10.0.0.3".into()],
            false,
            Arc::new(AtomicU64::new(50_000)),
            0,
            JitoBundleCounters::default(),
        ).unwrap()
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
    fn protocol_label_is_jito_bundle() {
        let s = make_sender();
        assert_eq!(s.protocol(), "JITO_BUNDLE");
    }

    #[tokio::test]
    async fn send_single_tx_wraps_into_one_tx_bundle() {
        // No regions, no real network: send_bundle short-circuits with
        // "no regions configured". send() must reach send_bundle (via
        // wrapping) and surface the same error — proving it's not
        // returning the old "requires send_bundle" sentinel.
        let s = JitoBundleSender::new(
            7, "jito-test".into(),
            "https://{region}.x".into(),
            vec![], // no regions
            vec!["10.0.0.1".into()],
            false,
            Arc::new(AtomicU64::new(50_000)),
            0,
            JitoBundleCounters::default(),
        ).unwrap();
        let tx = Transaction::default();
        let outcome = s.send(&tx).await;
        assert_eq!(outcome.error.as_deref(), Some("no regions configured"));
    }
}
