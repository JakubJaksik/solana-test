//! Jito Block Engine `sendBundle` sender (HTTP JSON-RPC, multi-region).
//!
//! Wraps a pre-signed tx (with Jito tip already baked in by the preparer)
//! into a bundle-of-one and POSTs to N regional block engine endpoints in
//! parallel. Bundle is deduplicated server-side by `bundle_id`, so fan-out
//! reaches all regions but counts as a single auction submission.
//!
//! IP rotation: an optional list of source IPs is round-robined per region
//! (separate cursor per region) so the per-IP-per-region rate limit budget
//! (default 1 rps) is spread across IPs.
//!
//! Reporting: `SendOutcome` carries the FIRST response (success or error)
//! that arrives back from any region. The remaining region tasks continue
//! to completion in the background so all regions actually receive the
//! bundle, but we don't wait for them.

use super::{SendOutcome, TxSender};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use solana_sdk::transaction::Transaction;
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub struct JitoSender {
    id: u8,
    name: String,
    /// URL template containing `{region}`; preserved verbatim for
    /// `endpoint_url()` accessor. Per-region resolved URLs live in `regions`.
    endpoint_template: String,
    /// One client per outbound IP. Empty `outbound_ips` → single client with
    /// OS-chosen source IP. Cloning a `Client` is cheap (Arc internal).
    clients_by_ip: Vec<reqwest::Client>,
    regions: Vec<JitoRegion>,
}

struct JitoRegion {
    /// Region identifier (e.g. "frankfurt"). Kept for diagnostics/logging.
    #[allow(dead_code)]
    name: String,
    url: String,
    /// Round-robin cursor into `clients_by_ip`. Separate per region so each
    /// region's rate-limit budget is spread evenly across all IPs.
    ip_cursor: AtomicUsize,
}

impl JitoSender {
    pub fn new(
        id: u8,
        name: impl Into<String>,
        endpoint_template: impl Into<String>,
        regions: Vec<String>,
        outbound_ips: Vec<String>,
    ) -> Self {
        let endpoint_template = endpoint_template.into();
        let clients_by_ip = build_clients(&outbound_ips);
        let regions = regions
            .into_iter()
            .map(|r| JitoRegion {
                url: substitute_region(&endpoint_template, &r),
                name: r,
                ip_cursor: AtomicUsize::new(0),
            })
            .collect();
        Self {
            id,
            name: name.into(),
            endpoint_template,
            clients_by_ip,
            regions,
        }
    }
}

fn build_clients(outbound_ips: &[String]) -> Vec<reqwest::Client> {
    fn base() -> reqwest::ClientBuilder {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .tcp_nodelay(true)
            .pool_max_idle_per_host(8)
            .tcp_keepalive(Duration::from_secs(30))
    }
    if outbound_ips.is_empty() {
        return vec![base().build().expect("reqwest client")];
    }
    outbound_ips
        .iter()
        .map(|s| {
            let ip = IpAddr::from_str(s)
                .unwrap_or_else(|_| panic!("invalid outbound_ip {s:?}"));
            base()
                .local_address(Some(ip))
                .build()
                .expect("reqwest client with local_address")
        })
        .collect()
}

fn substitute_region(template: &str, region: &str) -> String {
    template.replace("{region}", region)
}

#[derive(Serialize)]
struct SendBundleRequest<'a> {
    jsonrpc: &'static str,
    id: u64,
    method: &'static str,
    /// Jito spec: `params` is a tuple/array `[ [tx_b64, ...], options? ]`.
    /// We send a single tx (bundle-of-one) so the inner array has length 1.
    params: ([&'a str; 1],),
}

#[derive(Deserialize)]
struct JsonRpcResponse {
    result: Option<String>,
    error: Option<JsonRpcError>,
}

#[derive(Deserialize)]
struct JsonRpcError {
    code: i32,
    message: String,
}

#[async_trait]
impl TxSender for JitoSender {
    fn id(&self) -> u8 {
        self.id
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn endpoint_url(&self) -> &str {
        &self.endpoint_template
    }
    fn protocol(&self) -> &'static str {
        "HTTP_JSONRPC_BUNDLE"
    }

    async fn send(&self, tx: &Transaction) -> SendOutcome {
        let signature = tx.signatures.first().copied().unwrap_or_default();
        let serialized = bincode::serialize(tx).unwrap_or_default();
        use base64::Engine as _;
        let b64 = base64::engine::general_purpose::STANDARD.encode(&serialized);

        let body = serde_json::to_string(&SendBundleRequest {
            jsonrpc: "2.0",
            id: 1,
            method: "sendBundle",
            params: ([b64.as_str()],),
        })
        .unwrap_or_default();
        let body: Arc<String> = Arc::new(body);

        if self.regions.is_empty() {
            return SendOutcome {
                send_at: Instant::now(),
                send_ack_at: None,
                signature,
                http_status: None,
                rpc_err_code: None,
                rpc_err_message: None,
                provider_request_id: None,
                error: Some("no regions configured".into()),
                endpoint_url_used: None,
            };
        }

        let send_at = Instant::now();
        let (tx_first, mut rx_first) =
            tokio::sync::mpsc::channel::<RegionReply>(self.regions.len());

        for region in &self.regions {
            let client = self.next_client_for(region);
            let url = region.url.clone();
            let body = body.clone();
            let tx_first = tx_first.clone();
            tokio::spawn(async move {
                let result = client
                    .post(&url)
                    .header("Content-Type", "application/json")
                    .body((*body).clone())
                    .send()
                    .await;
                let send_ack_at = Instant::now();
                let reply = match result {
                    Ok(resp) => {
                        let status = resp.status().as_u16();
                        let body_text = resp.text().await.unwrap_or_default();
                        RegionReply::Http {
                            url,
                            status,
                            body: body_text,
                            send_ack_at,
                        }
                    }
                    Err(e) => RegionReply::Network {
                        url,
                        error: e.to_string(),
                        send_ack_at,
                    },
                };
                // First sender wins via channel ordering; rest are ignored
                // but the POST already went out so the bundle reached the
                // region regardless.
                let _ = tx_first.send(reply).await;
            });
        }
        drop(tx_first);

        let Some(first) = rx_first.recv().await else {
            return SendOutcome {
                send_at,
                send_ack_at: None,
                signature,
                http_status: None,
                rpc_err_code: None,
                rpc_err_message: None,
                provider_request_id: None,
                error: Some("all region tasks dropped without reply".into()),
                endpoint_url_used: None,
            };
        };

        match first {
            RegionReply::Network {
                url,
                error,
                send_ack_at,
            } => SendOutcome {
                send_at,
                send_ack_at: Some(send_ack_at),
                signature,
                http_status: None,
                rpc_err_code: None,
                rpc_err_message: None,
                provider_request_id: None,
                error: Some(format!("network: {}", error)),
                endpoint_url_used: Some(url),
            },
            RegionReply::Http {
                url,
                status,
                body,
                send_ack_at,
            } => match serde_json::from_str::<JsonRpcResponse>(&body) {
                Ok(parsed) => {
                    if let Some(err) = parsed.error {
                        SendOutcome {
                            send_at,
                            send_ack_at: Some(send_ack_at),
                            signature,
                            http_status: Some(status),
                            rpc_err_code: Some(err.code),
                            rpc_err_message: Some(err.message.clone()),
                            provider_request_id: None,
                            error: Some(err.message),
                            endpoint_url_used: Some(url),
                        }
                    } else {
                        SendOutcome {
                            send_at,
                            send_ack_at: Some(send_ack_at),
                            signature,
                            http_status: Some(status),
                            rpc_err_code: None,
                            rpc_err_message: None,
                            provider_request_id: parsed.result,
                            error: None,
                            endpoint_url_used: Some(url),
                        }
                    }
                }
                Err(_) => SendOutcome {
                    send_at,
                    send_ack_at: Some(send_ack_at),
                    signature,
                    http_status: Some(status),
                    rpc_err_code: None,
                    rpc_err_message: Some(format!("non-JSONRPC body: {}", body)),
                    provider_request_id: None,
                    error: Some(format!("HTTP {} body: {}", status, body)),
                    endpoint_url_used: Some(url),
                },
            },
        }
    }
}

impl JitoSender {
    fn next_client_for(&self, region: &JitoRegion) -> reqwest::Client {
        let idx = region.ip_cursor.fetch_add(1, Ordering::Relaxed)
            % self.clients_by_ip.len();
        self.clients_by_ip[idx].clone()
    }
}

enum RegionReply {
    Http {
        url: String,
        status: u16,
        body: String,
        send_ack_at: Instant,
    },
    Network {
        url: String,
        error: String,
        send_ack_at: Instant,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn region_url_substitution_replaces_placeholder() {
        let url = substitute_region(
            "https://{region}.mainnet.block-engine.jito.wtf",
            "frankfurt",
        );
        assert_eq!(url, "https://frankfurt.mainnet.block-engine.jito.wtf");
    }

    #[test]
    fn region_url_substitution_handles_multiple_placeholders() {
        let url = substitute_region("{region}-a-{region}", "ams");
        assert_eq!(url, "ams-a-ams");
    }

    #[test]
    fn build_clients_empty_ips_gives_one_default_client() {
        let clients = build_clients(&[]);
        assert_eq!(clients.len(), 1);
    }

    #[test]
    fn build_clients_one_per_ip() {
        let clients = build_clients(&["127.0.0.1".into(), "127.0.0.2".into()]);
        assert_eq!(clients.len(), 2);
    }

    #[test]
    fn ip_rotation_round_robin_per_region() {
        let sender = JitoSender::new(
            7,
            "t",
            "https://{region}.x",
            vec!["a".into(), "b".into()],
            vec!["127.0.0.1".into(), "127.0.0.2".into(), "127.0.0.3".into()],
        );
        // Each region has its own cursor — exercise region 0 three times,
        // observe that cursor wraps after len=3 calls.
        let r0 = &sender.regions[0];
        let i1 = r0.ip_cursor.fetch_add(1, Ordering::Relaxed) % 3;
        let i2 = r0.ip_cursor.fetch_add(1, Ordering::Relaxed) % 3;
        let i3 = r0.ip_cursor.fetch_add(1, Ordering::Relaxed) % 3;
        let i4 = r0.ip_cursor.fetch_add(1, Ordering::Relaxed) % 3;
        assert_eq!((i1, i2, i3, i4), (0, 1, 2, 0));
        // Region 1 cursor untouched — starts at 0.
        let r1 = &sender.regions[1];
        let j1 = r1.ip_cursor.fetch_add(1, Ordering::Relaxed) % 3;
        assert_eq!(j1, 0);
    }

    #[test]
    fn bundle_request_payload_matches_jito_spec() {
        let req = SendBundleRequest {
            jsonrpc: "2.0",
            id: 1,
            method: "sendBundle",
            params: (["TXBASE64"],),
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["jsonrpc"], "2.0");
        assert_eq!(json["method"], "sendBundle");
        // params shape: [ [tx_b64] ] — tuple of single inner array
        assert_eq!(json["params"][0][0], "TXBASE64");
    }

    #[test]
    fn endpoint_url_returns_template() {
        let sender = JitoSender::new(
            0, "n", "https://{region}.x.y", vec!["r1".into()], vec![],
        );
        assert_eq!(sender.endpoint_url(), "https://{region}.x.y");
    }

    #[test]
    fn protocol_label_is_bundle() {
        let sender = JitoSender::new(0, "n", "x", vec!["r".into()], vec![]);
        assert_eq!(sender.protocol(), "HTTP_JSONRPC_BUNDLE");
    }
}
