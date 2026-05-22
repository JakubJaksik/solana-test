//! JSON-RPC multi-IP client for Jito sendBundle.
//!
//! Holds 8 hosts × N source IPs = 8N reqwest clients, each bound to a
//! specific outbound IP. `grid_client(host_idx, ip_idx)` returns a cheap
//! clone for one POST. The sender orchestrates the 8 parallel calls.

use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use std::str::FromStr;
use std::time::Duration;

/// Per-host, per-IP grid of `reqwest::Client`s.
pub struct JsonRpcMultiIpClient {
    pub hosts: Vec<String>,         // 8 fully-substituted URLs
    grid: Vec<Vec<reqwest::Client>>, // grid[host_idx][ip_idx]
    ip_count: usize,
}

impl JsonRpcMultiIpClient {
    pub fn new(endpoint_template: &str, regions: &[String], outbound_ips: &[String]) -> Self {
        let hosts: Vec<String> = regions
            .iter()
            .map(|r| endpoint_template.replace("{region}", r))
            .collect();
        let ip_count = outbound_ips.len().max(1);
        let grid: Vec<Vec<reqwest::Client>> = hosts
            .iter()
            .map(|_| build_clients_for_host(outbound_ips))
            .collect();
        Self { hosts, grid, ip_count }
    }

    pub fn host_count(&self) -> usize { self.hosts.len() }
    pub fn ip_count(&self) -> usize { self.ip_count }

    /// Cheap clone (Arc internal). Picked by the sender for one POST.
    pub fn grid_client(&self, host_idx: usize, ip_idx: usize) -> reqwest::Client {
        self.grid[host_idx][ip_idx % self.ip_count].clone()
    }
}

fn build_clients_for_host(outbound_ips: &[String]) -> Vec<reqwest::Client> {
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
            let ip = IpAddr::from_str(s).unwrap_or_else(|_| panic!("invalid outbound_ip {s:?}"));
            base()
                .local_address(Some(ip))
                .build()
                .expect("reqwest client with local_address")
        })
        .collect()
}

#[derive(Serialize)]
pub struct SendBundleRequest<'a> {
    pub jsonrpc: &'static str,
    pub id: u64,
    pub method: &'static str,
    /// Jito spec: `params` is `[[tx_b64, ...], { "encoding": "base64" }]`.
    pub params: (Vec<&'a str>, SendBundleOptions),
}

#[derive(Serialize)]
pub struct SendBundleOptions {
    pub encoding: &'static str,
}

#[derive(Deserialize)]
pub struct JsonRpcResponse {
    pub result: Option<String>,
    pub error: Option<JsonRpcError>,
}

#[derive(Deserialize)]
pub struct JsonRpcError {
    pub code: i32,
    pub message: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matrix_substitutes_regions_correctly() {
        let c = JsonRpcMultiIpClient::new(
            "https://{region}.mainnet.block-engine.jito.wtf",
            &["frankfurt".into(), "amsterdam".into(), "dublin".into(),
              "london".into(), "ny".into(), "tokyo".into(),
              "slc".into(), "singapore".into()],
            &[],
        );
        assert_eq!(c.host_count(), 8);
        assert_eq!(c.hosts[0], "https://frankfurt.mainnet.block-engine.jito.wtf");
        assert_eq!(c.hosts[7], "https://singapore.mainnet.block-engine.jito.wtf");
    }

    #[test]
    fn matrix_builds_one_client_per_ip_per_host() {
        let c = JsonRpcMultiIpClient::new(
            "https://{region}.x",
            &["r1".into(), "r2".into()],
            &["127.0.0.1".into(), "127.0.0.2".into(), "127.0.0.3".into()],
        );
        assert_eq!(c.host_count(), 2);
        assert_eq!(c.ip_count(), 3);
        assert_eq!(c.grid[0].len(), 3);
        assert_eq!(c.grid[1].len(), 3);
    }

    #[test]
    fn empty_outbound_ips_yields_ip_count_one() {
        let c = JsonRpcMultiIpClient::new("x", &["r1".into()], &[]);
        assert_eq!(c.ip_count(), 1);
        assert_eq!(c.grid[0].len(), 1);
    }

    #[test]
    fn send_bundle_request_serializes_per_jito_spec() {
        let req = SendBundleRequest {
            jsonrpc: "2.0",
            id: 1,
            method: "sendBundle",
            params: (vec!["TX1_B64", "TX2_B64"], SendBundleOptions { encoding: "base64" }),
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["method"], "sendBundle");
        assert_eq!(json["params"][0][0], "TX1_B64");
        assert_eq!(json["params"][0][1], "TX2_B64");
        assert_eq!(json["params"][1]["encoding"], "base64");
    }
}
