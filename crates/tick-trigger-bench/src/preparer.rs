use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const BLOCKHASH_MAX_AGE: Duration = Duration::from_secs(50);

use solana_client::rpc_client::RpcClient;
use solana_sdk::signature::Keypair;
use tracing::{info, warn};

use crate::counters::BenchCounters;
use crate::schedule::ScheduleEntry;
use crate::tx_pool::{PreSignedTx, TxPool};
use crate::wallet::{build_self_transfer, primary_signature, serialize_tx};

pub struct PreparerConfig {
    pub schedule: Arc<Vec<ScheduleEntry>>,
    pub keypair: Arc<Keypair>,
    pub rpc_url: String,
    pub pool: TxPool,
    pub current_slot: Arc<AtomicU64>,    // updated by observer thread
    pub refresh_interval: Duration,       // default 30s
    pub look_ahead_slots: u64,            // default 100
    pub amount_lamports: u64,
    pub priority_fee_microlamports: u64,
    pub helius_tip_lamports: u64,
    pub pinned_core: Option<usize>,
    pub counters: Arc<BenchCounters>,
    pub stop: Arc<AtomicBool>,
}

pub fn spawn(cfg: PreparerConfig) -> std::io::Result<std::thread::JoinHandle<()>> {
    std::thread::Builder::new()
        .name("preparer".into())
        .spawn(move || {
            if let Some(c) = cfg.pinned_core {
                core_affinity::set_for_current(core_affinity::CoreId { id: c });
            }
            run_loop(cfg);
        })
}

fn run_loop(cfg: PreparerConfig) {
    let client = RpcClient::new(cfg.rpc_url.clone());
    // Maps (slot, tick) → built_at so we can detect expired blockhashes.
    let mut already_signed: HashMap<(u64, u8), Instant> = HashMap::with_capacity(64_000);

    while !cfg.stop.load(Ordering::Relaxed) {
        // 1. Fetch blockhash
        let blockhash = match client.get_latest_blockhash() {
            Ok(bh) => bh,
            Err(e) => {
                cfg.counters.inc(&cfg.counters.preparer_blockhash_fail);
                warn!(error = %e, "preparer: blockhash fetch failed");
                std::thread::sleep(Duration::from_secs(2));
                continue;
            }
        };

        let now = Instant::now();

        // 1b. Expire stale signed entries so they get re-signed with a fresh blockhash.
        let expired: Vec<(u64, u8)> = already_signed
            .iter()
            .filter(|(_, built_at)| now.duration_since(**built_at) >= BLOCKHASH_MAX_AGE)
            .map(|(k, _)| *k)
            .collect();
        let expired_count = expired.len();
        for key in expired {
            already_signed.remove(&key);
            cfg.pool.take(key.0, key.1); // evict expired tx from pool
        }
        if expired_count > 0 {
            info!(expired_count, "preparer: expired stale signed entries");
        }

        // 2. Determine window of slots to pre-sign for
        let now_slot = cfg.current_slot.load(Ordering::Relaxed);
        if now_slot == 0 {
            // observer hasn't started yet — wait
            std::thread::sleep(Duration::from_millis(200));
            continue;
        }
        let window_lo = now_slot;
        let window_hi = now_slot + cfg.look_ahead_slots;

        // 3. Pre-sign every schedule entry in window not yet signed
        let mut new_signed = 0usize;
        for entry in cfg.schedule.iter() {
            if entry.slot < window_lo || entry.slot >= window_hi {
                continue;
            }
            if already_signed.contains_key(&(entry.slot, entry.tick)) {
                continue;
            }
            let built_at = Instant::now();
            let tx = build_self_transfer(
                &cfg.keypair,
                cfg.amount_lamports,
                cfg.priority_fee_microlamports,
                cfg.helius_tip_lamports,
                blockhash,
            );
            let sig = primary_signature(&tx);
            let serialized = serialize_tx(&tx);
            let pre = PreSignedTx {
                serialized,
                signature: sig,
                blockhash,
                built_at,
            };
            cfg.pool.insert(entry.slot, entry.tick, pre);
            already_signed.insert((entry.slot, entry.tick), built_at);
            new_signed += 1;
        }

        // 4. Prune pool of slots that have passed
        let pruned = cfg.pool.prune_older_than(now_slot.saturating_sub(2));

        if new_signed > 0 || pruned > 0 {
            info!(new_signed, pruned, pool_size = cfg.pool.len(),
                  "preparer cycle complete");
        }

        // 5. Sleep until next refresh
        std::thread::sleep(cfg.refresh_interval);
    }
    info!("preparer thread exiting");
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::hash::Hash;

    #[test]
    fn build_self_transfer_smoke() {
        // Smoke test that we're calling build_self_transfer correctly with our types.
        let kp = Keypair::new();
        let bh = Hash::new_unique();
        let tx = build_self_transfer(&kp, 1, 5000, 5000, bh);
        assert!(tx.is_signed());
        let _sig = primary_signature(&tx);
    }
}
