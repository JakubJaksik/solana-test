//! JSON-RPC multi-IP client for Jito `sendTransaction`.
//!
//! Holds N hosts × M source IPs = N×M reqwest clients, each bound to a
//! specific outbound IP. `grid_client(host_idx, ip_idx)` returns a cheap
//! clone for one POST. The sender orchestrates the parallel calls across
//! regional hosts using one rotated source IP per send.

use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use std::str::FromStr;
use std::time::Duration;

pub struct JsonRpcMultiIpClient {
    pub hosts: Vec<String>,
    grid: Vec<Vec<reqwest::Client>>,
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
pub struct SendTransactionRequest<'a> {
    pub jsonrpc: &'static str,
    pub id: u64,
    pub method: &'static str,
    /// Jito spec: `params` is `[<base64_tx>, { ...options }]`.
    pub params: (&'a str, SendTransactionOptions),
}

#[derive(Serialize)]
pub struct SendTransactionOptions {
    pub encoding: &'static str,
    #[serde(rename = "skipPreflight")]
    pub skip_preflight: bool,
    #[serde(rename = "maxRetries")]
    pub max_retries: u64,
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
            "https://{region}.mainnet.block-engine.jito.wtf/api/v1/transactions",
            &["frankfurt".into(), "amsterdam".into(), "dublin".into(),
              "london".into(), "ny".into(), "tokyo".into(),
              "slc".into(), "singapore".into()],
            &[],
        );
        assert_eq!(c.host_count(), 8);
        assert_eq!(
            c.hosts[0],
            "https://frankfurt.mainnet.block-engine.jito.wtf/api/v1/transactions"
        );
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
    fn send_transaction_request_serializes_per_jito_spec() {
        let req = SendTransactionRequest {
            jsonrpc: "2.0",
            id: 1,
            method: "sendTransaction",
            params: (
                "TX_B64",
                SendTransactionOptions {
                    encoding: "base64",
                    skip_preflight: true,
                    max_retries: 0,
                },
            ),
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["method"], "sendTransaction");
        assert_eq!(json["params"][0], "TX_B64");
        assert_eq!(json["params"][1]["encoding"], "base64");
        assert_eq!(json["params"][1]["skipPreflight"], true);
        assert_eq!(json["params"][1]["maxRetries"], 0);
    }
}
