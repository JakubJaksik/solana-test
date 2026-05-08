use anyhow::{Context, Result, bail};
use reqwest::Client;
use serde::Deserialize;
use std::time::Duration;
use tracing::{debug, info};

use crate::config::ValidatorsAppConfig;
use crate::domain::ValidatorInfo;

/// Raw shape returned by validators.app /api/v1/validators/{network}.json
/// Many fields are optional or vary; we only deserialize what we need.
#[derive(Debug, Deserialize)]
struct RawValidator {
    #[serde(default)]
    account: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    vote_account: Option<String>,
    #[serde(default)]
    active_stake: Option<u64>,
    #[serde(default)]
    country_code: Option<String>,
    #[serde(default)]
    data_center_key: Option<String>,
    #[serde(default)]
    autonomous_system_number: Option<u64>,
    #[serde(default)]
    asn: Option<String>,
    #[serde(default)]
    ip: Option<String>,
}

pub struct ValidatorsAppClient {
    http: Client,
    cfg: ValidatorsAppConfig,
}

impl ValidatorsAppClient {
    pub fn new(cfg: ValidatorsAppConfig) -> Result<Self> {
        let http = Client::builder()
            .timeout(Duration::from_secs(60))
            .user_agent("solana-leader-map/0.1")
            .build()
            .context("failed to build reqwest client")?;
        Ok(Self { http, cfg })
    }

    /// GET /api/v1/validators/{network}.json — all validators with geo.
    pub async fn fetch_all_validators(&self) -> Result<Vec<ValidatorInfo>> {
        let url = format!(
            "{}/api/v1/validators/{}.json?per=10000",
            self.cfg.base_url.trim_end_matches('/'),
            self.cfg.network
        );
        info!(url = %url, "fetching validators.app");

        let resp = self
            .http
            .get(&url)
            .header("Token", &self.cfg.api_token)
            .send()
            .await
            .context("validators.app request failed")?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            bail!("validators.app returned {}: {}", status, body);
        }

        let raw: Vec<RawValidator> = resp
            .json()
            .await
            .context("failed to parse validators.app JSON response")?;

        debug!(count = raw.len(), "validators.app raw response");

        let validators = raw.into_iter().filter_map(map_raw).collect::<Vec<_>>();
        info!(
            mapped = validators.len(),
            "validators.app: parsed validator records"
        );
        Ok(validators)
    }
}

fn map_raw(r: RawValidator) -> Option<ValidatorInfo> {
    let identity = r.account?;
    Some(ValidatorInfo {
        identity,
        name: r.name,
        vote_account: r.vote_account,
        active_stake_lamports: r.active_stake.unwrap_or(0),
        country_code: r.country_code,
        data_center_key: r.data_center_key,
        asn: r.autonomous_system_number,
        asn_organization: r.asn,
        ip: r.ip,
    })
}
