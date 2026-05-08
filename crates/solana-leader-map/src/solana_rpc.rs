use anyhow::{Context, Result, bail};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;
use tracing::info;

use crate::config::SolanaRpcConfig;
use crate::domain::{EpochInfo, LeaderSchedule};

#[derive(Debug, Serialize)]
struct RpcRequest<'a, P> {
    jsonrpc: &'a str,
    id: u64,
    method: &'a str,
    params: P,
}

#[derive(Debug, Deserialize)]
struct RpcError {
    code: i64,
    message: String,
}

#[derive(Debug, Deserialize)]
struct EpochInfoResp {
    epoch: u64,
    #[serde(rename = "absoluteSlot")]
    absolute_slot: u64,
    #[serde(rename = "slotIndex")]
    slot_index: u64,
    #[serde(rename = "slotsInEpoch")]
    slots_in_epoch: u64,
}

pub struct SolanaRpcClient {
    http: Client,
    cfg: SolanaRpcConfig,
}

impl SolanaRpcClient {
    pub fn new(cfg: SolanaRpcConfig) -> Result<Self> {
        let http = Client::builder()
            .timeout(Duration::from_secs(120))
            .user_agent("solana-leader-map/0.1")
            .build()
            .context("failed to build reqwest client")?;
        Ok(Self { http, cfg })
    }

    pub async fn get_epoch_info(&self) -> Result<EpochInfo> {
        let resp: EpochInfoResp = self.call("getEpochInfo", serde_json::json!([])).await?;
        Ok(EpochInfo {
            epoch: resp.epoch,
            absolute_slot: resp.absolute_slot,
            slot_index: resp.slot_index,
            slots_in_epoch: resp.slots_in_epoch,
        })
    }

    /// `getLeaderSchedule(null)` returns the schedule for the current epoch as a map of
    /// validator-identity → list of slot-indices relative to the start of that epoch.
    pub async fn get_leader_schedule(&self) -> Result<LeaderSchedule> {
        info!("fetching leader schedule from RPC (this can take 10-60s)");
        let raw: BTreeMap<String, Vec<u64>> =
            self.call("getLeaderSchedule", serde_json::json!([null])).await?;
        Ok(raw)
    }

    async fn call<P: Serialize, R: for<'de> Deserialize<'de>>(
        &self,
        method: &str,
        params: P,
    ) -> Result<R> {
        let req = RpcRequest {
            jsonrpc: "2.0",
            id: 1,
            method,
            params,
        };
        let mut builder = self
            .http
            .post(&self.cfg.url)
            .header("Content-Type", "application/json");
        if let Some(auth) = &self.cfg.auth_header {
            builder = builder.header("Authorization", auth);
        }
        let resp = builder
            .json(&req)
            .send()
            .await
            .with_context(|| format!("RPC {} request failed", method))?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            bail!("RPC {} returned {}: {}", method, status, body);
        }

        // Two-step: parse to Value first, then dispatch on `result` or `error`.
        // Avoids serde generic bounds that would force R: Default on every call site.
        let raw: serde_json::Value = resp
            .json()
            .await
            .with_context(|| format!("RPC {} response parse failed", method))?;

        if let Some(err_val) = raw.get("error") {
            let err: RpcError = serde_json::from_value(err_val.clone())
                .unwrap_or(RpcError { code: 0, message: err_val.to_string() });
            bail!("RPC {} error {}: {}", method, err.code, err.message);
        }
        let result_val = raw
            .get("result")
            .with_context(|| format!("RPC {} returned no result field", method))?
            .clone();
        let parsed: R = serde_json::from_value(result_val)
            .with_context(|| format!("RPC {} result decode", method))?;
        Ok(parsed)
    }
}
