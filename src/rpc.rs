//! RPC: WS newHeads subscription + HTTP JSON-RPC sender.
//!
//! # HTTP client
//! [`HttpRpcClient`] wraps `reqwest` for fire-and-forget JSON-RPC calls. It exposes:
//! - [`HttpRpcClient::send_raw_transaction_prepared`] — returns [`SendOutcome`]
//! - Various helper calls: `eth_chain_id`, `eth_block_number`, `eth_get_balance`,
//!   `eth_get_transaction_count`, `eth_get_block_tx_hashes`, `latest_base_fee`
//!
//! # WS subscriber
//! [`WsBlockSubscriber`] connects to an Ethereum node via WebSocket and streams
//! [`alloy::rpc::types::Header`] values through an `mpsc` channel.
//!
//! Uses alloy 1.x API:
//! - `alloy::providers::{ProviderBuilder, WsConnect}`
//! - `provider.subscribe_blocks()` → stream of `alloy::rpc::types::Header`
//! - The `HeaderResponse` for `Ethereum` network is `alloy_rpc_types_eth::Header`
//!   which contains a `hash: BlockHash` and `inner: alloy_consensus::Header`
//!   (fields: `number: u64`, `base_fee_per_gas: Option<u64>`, etc.)

use alloy::{
    primitives::{B256, U256},
    providers::{Provider, ProviderBuilder, WsConnect},
    rpc::types::Header,
};
use futures_util::StreamExt;
use reqwest::Client;
use serde::Deserialize;
use std::time::Duration;
use thiserror::Error;
use tokio::{sync::mpsc, task::JoinHandle};
use tracing::{debug, error, warn};

// ── Errors ──────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum RpcError {
    #[error("transport error: {0}")]
    Transport(String),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("HTTP client: {0}")]
    Http(#[from] reqwest::Error),

    #[error("WS error: {0}")]
    Ws(String),
}

// ── HTTP send outcome ────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum SendOutcome {
    /// Transaction accepted by the node; `tx_hash` is the canonical hash.
    Accepted { tx_hash: B256 },
    /// Node returned a JSON-RPC error (e.g. nonce too low, insufficient gas).
    Rejected { code: i64, message: String },
}

// ── Internal JSON-RPC response shapes ────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct RpcError_ {
    code: i64,
    message: String,
}

#[derive(Debug, Deserialize)]
struct RpcResponse<T> {
    #[allow(dead_code)]
    jsonrpc: String,
    #[allow(dead_code)]
    id: serde_json::Value,
    #[serde(default)]
    result: Option<T>,
    #[serde(default)]
    error: Option<RpcError_>,
}

// ── Payload builder ──────────────────────────────────────────────────────────

/// Build a JSON-RPC `eth_sendRawTransaction` payload string.
///
/// `raw_hex_0x` must be a `0x`-prefixed hex-encoded signed transaction.
pub fn build_send_payload(id: u64, raw_hex_0x: &str) -> String {
    format!(
        r#"{{"jsonrpc":"2.0","id":{},"method":"eth_sendRawTransaction","params":["{}"]}}"#,
        id, raw_hex_0x
    )
}

// ── HTTP client ──────────────────────────────────────────────────────────────

/// Low-latency HTTP JSON-RPC client backed by a persistent `reqwest::Client`.
///
/// Connection pooling is configured for high-throughput send loops.
pub struct HttpRpcClient {
    url: String,
    client: Client,
}

impl HttpRpcClient {
    /// Create a new client targeting `url` (HTTP or HTTPS).
    pub fn new(url: &str) -> Result<Self, RpcError> {
        let client = Client::builder()
            .pool_max_idle_per_host(16)
            .tcp_keepalive(Some(Duration::from_secs(60)))
            .timeout(Duration::from_secs(10))
            .build()?;
        debug!(url, "HttpRpcClient created");
        Ok(Self {
            url: url.to_string(),
            client,
        })
    }

    /// Send a pre-built JSON-RPC payload and interpret the response as
    /// [`SendOutcome`].
    pub async fn send_raw_transaction_prepared(
        &self,
        payload: &str,
    ) -> Result<SendOutcome, RpcError> {
        let text = self.raw_call(payload).await?;
        let parsed: RpcResponse<B256> = serde_json::from_str(&text)?;
        if let Some(err) = parsed.error {
            debug!(code = err.code, message = %err.message, "eth_sendRawTransaction rejected");
            return Ok(SendOutcome::Rejected {
                code: err.code,
                message: err.message,
            });
        }
        if let Some(tx_hash) = parsed.result {
            debug!(?tx_hash, "eth_sendRawTransaction accepted");
            return Ok(SendOutcome::Accepted { tx_hash });
        }
        Err(RpcError::Transport(
            "missing result and error in response".into(),
        ))
    }

    /// Return the chain ID as `u64`.
    pub async fn eth_chain_id(&self) -> Result<u64, RpcError> {
        let payload = r#"{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}"#;
        let resp = self.raw_call(payload).await?;
        let parsed: RpcResponse<String> = serde_json::from_str(&resp)?;
        let hex = parsed
            .result
            .ok_or_else(|| RpcError::Transport("no result for eth_chainId".into()))?;
        u64::from_str_radix(hex.trim_start_matches("0x"), 16)
            .map_err(|e| RpcError::Transport(e.to_string()))
    }

    /// Return the latest block number.
    pub async fn eth_block_number(&self) -> Result<u64, RpcError> {
        let payload = r#"{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}"#;
        let resp = self.raw_call(payload).await?;
        let parsed: RpcResponse<String> = serde_json::from_str(&resp)?;
        let hex = parsed
            .result
            .ok_or_else(|| RpcError::Transport("no result for eth_blockNumber".into()))?;
        u64::from_str_radix(hex.trim_start_matches("0x"), 16)
            .map_err(|e| RpcError::Transport(e.to_string()))
    }

    /// Return the ETH balance of `addr` at the latest block.
    pub async fn eth_get_balance(&self, addr: &str) -> Result<U256, RpcError> {
        let payload = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["{}","latest"]}}"#,
            addr
        );
        let resp = self.raw_call(&payload).await?;
        let parsed: RpcResponse<String> = serde_json::from_str(&resp)?;
        let hex = parsed
            .result
            .ok_or_else(|| RpcError::Transport("no result for eth_getBalance".into()))?;
        U256::from_str_radix(hex.trim_start_matches("0x"), 16)
            .map_err(|e| RpcError::Transport(e.to_string()))
    }

    /// Return the pending transaction count (nonce) of `addr`.
    pub async fn eth_get_transaction_count(&self, addr: &str) -> Result<u64, RpcError> {
        let payload = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"eth_getTransactionCount","params":["{}","pending"]}}"#,
            addr
        );
        let resp = self.raw_call(&payload).await?;
        let parsed: RpcResponse<String> = serde_json::from_str(&resp)?;
        let hex = parsed
            .result
            .ok_or_else(|| RpcError::Transport("no result for eth_getTransactionCount".into()))?;
        u64::from_str_radix(hex.trim_start_matches("0x"), 16)
            .map_err(|e| RpcError::Transport(e.to_string()))
    }

    /// Return the transaction hashes included in block `block_num`.
    ///
    /// Manually parses JSON to stay independent of alloy's generic `Block` type
    /// complexity.
    pub async fn eth_get_block_tx_hashes(&self, block_num: u64) -> Result<Vec<B256>, RpcError> {
        let payload = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["0x{:x}",false]}}"#,
            block_num
        );
        let resp = self.raw_call(&payload).await?;
        let v: serde_json::Value = serde_json::from_str(&resp)?;
        let arr = v["result"]["transactions"]
            .as_array()
            .ok_or_else(|| RpcError::Transport("no transactions array in block response".into()))?;
        let mut out = Vec::with_capacity(arr.len());
        for item in arr {
            let s = item
                .as_str()
                .ok_or_else(|| RpcError::Transport("transaction entry is not a string".into()))?;
            out.push(
                s.parse::<B256>()
                    .map_err(|e| RpcError::Transport(e.to_string()))?,
            );
        }
        debug!(block_num, tx_count = out.len(), "fetched block tx hashes");
        Ok(out)
    }

    /// Return the `baseFeePerGas` field from the latest block (in wei).
    pub async fn latest_base_fee(&self) -> Result<u128, RpcError> {
        let payload =
            r#"{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["latest",false]}"#;
        let resp = self.raw_call(payload).await?;
        let v: serde_json::Value = serde_json::from_str(&resp)?;
        let base_fee_hex = v["result"]["baseFeePerGas"].as_str().ok_or_else(|| {
            RpcError::Transport("no baseFeePerGas field in latest block response".into())
        })?;
        u128::from_str_radix(base_fee_hex.trim_start_matches("0x"), 16)
            .map_err(|e| RpcError::Transport(e.to_string()))
    }

    /// Raw JSON-RPC call — returns the response body as a `String` for the
    /// caller to parse.
    pub async fn raw_call(&self, payload: &str) -> Result<String, RpcError> {
        let resp = self
            .client
            .post(&self.url)
            .header("content-type", "application/json")
            .body(payload.to_string())
            .send()
            .await?;
        Ok(resp.text().await?)
    }
}

// ── WS block subscriber ──────────────────────────────────────────────────────

/// Thin wrapper that connects to an Ethereum node via WebSocket and forwards
/// new block headers to an `mpsc` channel.
///
/// Uses alloy 1.x: `WsConnect` + `ProviderBuilder::connect_ws` +
/// `provider.subscribe_blocks()`.
///
/// # Type note
/// `subscribe_blocks()` yields `alloy::rpc::types::Header` where:
/// - `header.hash` — `BlockHash` (= `B256`)
/// - `header.inner.number` — `u64`
/// - `header.inner.base_fee_per_gas` — `Option<u64>`
///
/// Task 12 (engine) will call [`WsBlockSubscriber::connect`] then
/// [`WsBlockSubscriber::spawn_stream`].
pub struct WsBlockSubscriber {
    ws_url: String,
}

impl WsBlockSubscriber {
    /// Create a subscriber for the given WebSocket URL.
    pub fn new(ws_url: &str) -> Self {
        Self {
            ws_url: ws_url.to_string(),
        }
    }

    /// Connect to the node, subscribe to `newHeads`, and spawn a background
    /// task that forwards headers to `tx`.
    ///
    /// Returns a [`JoinHandle`] the caller can use to cancel or await the
    /// stream. The task exits when the subscription stream ends or when the
    /// `JoinHandle` is dropped (via `abort()`).
    pub async fn spawn_stream(&self, tx: mpsc::Sender<Header>) -> Result<JoinHandle<()>, RpcError> {
        let ws = WsConnect::new(self.ws_url.clone());
        let provider = ProviderBuilder::new()
            .connect_ws(ws)
            .await
            .map_err(|e| RpcError::Ws(e.to_string()))?;

        let subscription = provider
            .subscribe_blocks()
            .await
            .map_err(|e| RpcError::Ws(e.to_string()))?;

        let handle = tokio::spawn(async move {
            // Keep the provider alive for the duration of the stream.
            let _provider = provider;
            let mut stream = subscription.into_stream();
            while let Some(header) = stream.next().await {
                debug!(
                    block_number = header.inner.number,
                    hash = ?header.hash,
                    "WS newHead received"
                );
                if tx.send(header).await.is_err() {
                    warn!("WsBlockSubscriber: receiver dropped, stopping stream");
                    break;
                }
            }
            error!("WsBlockSubscriber: newHeads stream ended unexpectedly");
        });

        Ok(handle)
    }
}
