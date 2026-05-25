//! gRPC multi-IP client for Jito sendBundle.
//!
//! Builds 8 hosts × N IPs lazy-connected channels with per-IP source binding
//! via tonic 0.13 `Endpoint::local_address`. `grid_channel(host_idx, ip_idx)`
//! returns a cheap clone for one RPC call.

use super::proto::bundle::Bundle;
use super::proto::packet::{Meta, Packet};
use super::proto::searcher::searcher_service_client::SearcherServiceClient;
use super::proto::searcher::SendBundleRequest;
use std::net::IpAddr;
use std::str::FromStr;
use std::time::Duration;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

pub struct GrpcMultiIpClient {
    pub hosts: Vec<String>,             // 8 bare hostnames (no scheme)
    grid: Vec<Vec<Channel>>,            // grid[host_idx][ip_idx]
    ip_count: usize,
}

impl GrpcMultiIpClient {
    pub fn new(
        endpoint_template: &str,
        regions: &[String],
        outbound_ips: &[String],
    ) -> Result<Self, tonic::transport::Error> {
        // The JSON-RPC template is `https://{region}.mainnet.block-engine.jito.wtf`.
        // For gRPC we extract just the host (no scheme, no path) and connect to :443.
        let hosts: Vec<String> = regions
            .iter()
            .map(|r| {
                endpoint_template
                    .replace("{region}", r)
                    .trim_start_matches("https://")
                    .trim_start_matches("http://")
                    .split('/')
                    .next()
                    .unwrap()
                    .to_string()
            })
            .collect();

        let ips: Vec<Option<IpAddr>> = if outbound_ips.is_empty() {
            vec![None]
        } else {
            outbound_ips
                .iter()
                .map(|s| Some(IpAddr::from_str(s).unwrap_or_else(|_| panic!("invalid outbound_ip {s:?}"))))
                .collect()
        };
        let ip_count = ips.len();

        let mut grid: Vec<Vec<Channel>> = Vec::with_capacity(hosts.len());
        for host in &hosts {
            let mut row = Vec::with_capacity(ip_count);
            for ip in &ips {
                let tls = ClientTlsConfig::new()
                    .domain_name(host.clone())
                    .with_native_roots();
                let mut ep = Endpoint::from_shared(format!("https://{host}:443"))?
                    .tls_config(tls)?
                    .timeout(Duration::from_secs(5))
                    .tcp_keepalive(Some(Duration::from_secs(30)))
                    .http2_keep_alive_interval(Duration::from_secs(20));
                if let Some(addr) = ip {
                    ep = ep.local_address(Some(*addr));
                }
                row.push(ep.connect_lazy());
            }
            grid.push(row);
        }

        Ok(Self { hosts, grid, ip_count })
    }

    pub fn host_count(&self) -> usize { self.hosts.len() }
    pub fn ip_count(&self) -> usize { self.ip_count }

    /// Cheap clone (Arc internal).
    pub fn grid_channel(&self, host_idx: usize, ip_idx: usize) -> Channel {
        self.grid[host_idx][ip_idx % self.ip_count].clone()
    }

    /// Invoke `SearcherService::SendBundle` on (host_idx, ip_idx).
    /// Returns the `uuid` (bundle id) string on success.
    pub async fn send_bundle(
        &self,
        host_idx: usize,
        ip_idx: usize,
        packet_bytes: &[Vec<u8>],
    ) -> Result<String, tonic::Status> {
        let channel = self.grid_channel(host_idx, ip_idx);
        let mut client = SearcherServiceClient::new(channel);
        let packets: Vec<Packet> = packet_bytes
            .iter()
            .map(|b| packet_from_bytes(b.clone()))
            .collect();
        let bundle = Bundle { header: None, packets };
        let req = tonic::Request::new(SendBundleRequest { bundle: Some(bundle) });
        let resp = client.send_bundle(req).await?;
        Ok(resp.into_inner().uuid)
    }
}

/// Build a Jito gRPC packet from serialized transaction bytes.
///
/// Jito's TS SDK fills packet metadata with the byte length; leaving `meta`
/// empty can make downstream packet conversion treat the payload as an empty
/// or malformed packet even though `data` is present.
pub fn packet_from_bytes(data: Vec<u8>) -> Packet {
    let size = data.len() as u64;
    Packet {
        data,
        meta: Some(Meta {
            size,
            addr: "0.0.0.0".to_string(),
            port: 0,
            flags: None,
            sender_stake: 0,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn extracts_host_from_https_template() {
        let c = GrpcMultiIpClient::new(
            "https://{region}.mainnet.block-engine.jito.wtf",
            &["frankfurt".into()],
            &[],
        )
        .unwrap();
        assert_eq!(c.hosts[0], "frankfurt.mainnet.block-engine.jito.wtf");
    }

    #[tokio::test]
    async fn grid_sized_hosts_times_ips() {
        let c = GrpcMultiIpClient::new(
            "https://{region}.x.y",
            &["r1".into(), "r2".into(), "r3".into()],
            &["10.0.0.1".into(), "10.0.0.2".into()],
        )
        .unwrap();
        assert_eq!(c.host_count(), 3);
        assert_eq!(c.ip_count(), 2);
    }

    #[tokio::test]
    async fn empty_ips_yields_one_default_channel_per_host() {
        let c = GrpcMultiIpClient::new("https://{region}.x", &["r1".into()], &[]).unwrap();
        assert_eq!(c.ip_count(), 1);
    }
}
