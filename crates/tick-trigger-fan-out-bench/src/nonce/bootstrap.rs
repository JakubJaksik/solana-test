//! Bootstrap nonce manager state from RPC at startup.
//!
//! Reads `nonce-config.json` (created by `setup-nonces`), fetches current
//! account state via `getMultipleAccounts`, validates each is Initialized
//! with correct authority, returns `Vec<(NonceId, Pubkey, Hash)>` ready for
//! `NonceManager::new()`.

use crate::nonce::manager::NonceId;
use crate::nonce::state::parse_nonce_account_data;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use solana_client::rpc_client::RpcClient;
use solana_sdk::{hash::Hash, pubkey::Pubkey};
use std::path::Path;
use std::str::FromStr;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NonceConfigFile {
    pub accounts: Vec<NonceConfigEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NonceConfigEntry {
    pub id: u16,
    pub pubkey: String,
    pub initial_blockhash: String,
}

pub fn bootstrap(
    rpc: &RpcClient,
    config_path: &Path,
    expected_authority: &Pubkey,
) -> Result<Vec<(NonceId, Pubkey, Hash)>> {
    let expanded = expand_tilde(config_path);
    let bytes = std::fs::read(&expanded)
        .with_context(|| format!("read nonce config: {}", expanded.display()))?;
    let config: NonceConfigFile =
        serde_json::from_slice(&bytes).context("parse nonce config")?;

    let pubkeys: Vec<Pubkey> = config
        .accounts
        .iter()
        .map(|e| Pubkey::from_str(&e.pubkey).context("invalid pubkey in nonce config"))
        .collect::<Result<_>>()?;

    let mut accounts = Vec::with_capacity(pubkeys.len());
    for chunk in pubkeys.chunks(100) {
        let batch = rpc
            .get_multiple_accounts(chunk)
            .context("getMultipleAccounts")?;
        accounts.extend(batch);
    }

    let mut result = Vec::with_capacity(config.accounts.len());
    for (entry, acc_opt) in config.accounts.iter().zip(accounts.iter()) {
        let acc = acc_opt.as_ref().with_context(|| {
            format!("nonce account {} does not exist on chain", entry.pubkey)
        })?;
        let state = parse_nonce_account_data(&acc.data)
            .with_context(|| format!("parse nonce {}", entry.pubkey))?;
        if state.authority != *expected_authority {
            anyhow::bail!(
                "nonce {} authority mismatch: got {}, expected {}",
                entry.pubkey,
                state.authority,
                expected_authority
            );
        }
        let pubkey = Pubkey::from_str(&entry.pubkey).unwrap();
        result.push((entry.id, pubkey, state.blockhash));
    }
    Ok(result)
}

/// Local tilde expansion duplicated here to avoid public-API churn in
/// `wallet.rs` (its `expand_tilde` is private to that module).
fn expand_tilde(path: &Path) -> std::path::PathBuf {
    let s = path.to_string_lossy();
    if let Some(stripped) = s.strip_prefix("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            let mut p = std::path::PathBuf::from(home);
            p.push(stripped);
            return p;
        }
    }
    path.to_path_buf()
}
