//! RPC fallback poller for Stale nonces.

use crate::nonce::manager::NonceManager;
use crate::nonce::state::parse_nonce_account_data;
use anyhow::Result;
use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;
use std::time::Duration;

pub struct RpcPollerConfig {
    pub rpc: Arc<RpcClient>,
    pub manager: Arc<NonceManager>,
    pub poll_interval: Duration,
    pub in_flight_deadline: Duration,
    pub awaiting_update_deadline: Duration,
    pub stop: Arc<std::sync::atomic::AtomicBool>,
}

pub fn spawn(cfg: RpcPollerConfig) -> std::io::Result<std::thread::JoinHandle<()>> {
    std::thread::Builder::new()
        .name("nonce-rpc-poller".into())
        .spawn(move || run_loop(cfg))
}

fn run_loop(cfg: RpcPollerConfig) {
    while !cfg.stop.load(std::sync::atomic::Ordering::Relaxed) {
        let new_stale = cfg.manager.tick_timeouts(cfg.in_flight_deadline, cfg.awaiting_update_deadline);
        if !new_stale.is_empty() {
            tracing::warn!(count = new_stale.len(), "nonces became stale, refreshing via RPC");
        }

        let stale_pubkeys: Vec<Pubkey> = cfg
            .manager
            .entries()
            .iter()
            .filter(|e| matches!(e.state(), crate::nonce::manager::NonceState::Stale { .. }))
            .map(|e| e.pubkey)
            .collect();

        if !stale_pubkeys.is_empty() {
            match refresh_batch(&cfg.rpc, &cfg.manager, &stale_pubkeys) {
                Ok(refreshed) => tracing::info!(refreshed, "rpc fallback refreshed stale nonces"),
                Err(e) => tracing::error!(error = %e, "rpc fallback batch failed"),
            }
        }
        std::thread::sleep(cfg.poll_interval);
    }
}

fn refresh_batch(rpc: &RpcClient, manager: &NonceManager, pubkeys: &[Pubkey]) -> Result<usize> {
    let mut refreshed = 0;
    for chunk in pubkeys.chunks(100) {
        let accounts = rpc.get_multiple_accounts(chunk)?;
        for (pk, acc_opt) in chunk.iter().zip(accounts.iter()) {
            if let Some(acc) = acc_opt {
                if let Ok(state) = parse_nonce_account_data(&acc.data) {
                    manager.on_fallback_refresh(pk, state.blockhash);
                    refreshed += 1;
                }
            }
        }
    }
    Ok(refreshed)
}
