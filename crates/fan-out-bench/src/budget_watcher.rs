//! Budget watcher — periodically polls wallet balance via RPC.
//! Signals stop when balance drops below threshold + nonce rent reserve.

use crate::counters::BenchCounters;
use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

pub struct BudgetWatcherConfig {
    pub rpc: Arc<RpcClient>,
    pub wallet_pubkey: Pubkey,
    pub min_balance_lamports: u64,
    pub nonce_rent_reserve_lamports: u64,
    pub poll_interval: Duration,
    pub pinned_core: Option<usize>,
    pub counters: Arc<BenchCounters>,
    pub stop: Arc<AtomicBool>,
}

pub fn spawn(cfg: BudgetWatcherConfig) -> std::io::Result<JoinHandle<()>> {
    std::thread::Builder::new()
        .name("budget-watcher".into())
        .spawn(move || {
            if let Some(core) = cfg.pinned_core {
                core_affinity::set_for_current(core_affinity::CoreId { id: core });
            }
            run_loop(cfg);
        })
}

fn run_loop(cfg: BudgetWatcherConfig) {
    let threshold = cfg.min_balance_lamports + cfg.nonce_rent_reserve_lamports;
    loop {
        if cfg.stop.load(Ordering::Relaxed) {
            break;
        }
        match cfg.rpc.get_balance(&cfg.wallet_pubkey) {
            Ok(balance) => {
                tracing::debug!(balance, threshold, "budget check");
                if balance < threshold {
                    tracing::warn!(
                        balance,
                        threshold,
                        "balance below threshold, signalling stop"
                    );
                    cfg.stop.store(true, Ordering::Relaxed);
                    break;
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "getBalance failed; continuing");
            }
        }
        std::thread::sleep(cfg.poll_interval);
    }
}
