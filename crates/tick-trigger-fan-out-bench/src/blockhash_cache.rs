//! Blockhash cache.
//!
//! A background thread refreshes the latest blockhash from RPC every
//! `refresh_secs` seconds and stores it in an `ArcSwap<Hash>`. The hot path
//! reads the current value via a single atomic load (zero allocations) —
//! see `BlockhashCache::current()`.
//!
//! Why ArcSwap not RwLock: blockhash reads happen per trigger fire, on a
//! latency-sensitive thread. ArcSwap::load is wait-free; RwLock::read may
//! park briefly under writer contention.

use arc_swap::ArcSwap;
use solana_client::rpc_client::RpcClient;
use solana_sdk::hash::Hash;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

pub struct BlockhashCache {
    /// Latest fetched blockhash; ArcSwap so the hot-path read is atomic.
    inner: Arc<ArcSwap<Hash>>,
    /// Unix nanos of last successful refresh — for staleness checks.
    last_refresh_ns: Arc<AtomicU64>,
    /// Count of failed fetch attempts (telemetry).
    refresh_errors: Arc<AtomicU64>,
}

impl BlockhashCache {
    pub fn current(&self) -> Hash {
        **self.inner.load()
    }

    pub fn last_refresh_ns(&self) -> u64 {
        self.last_refresh_ns.load(Ordering::Relaxed)
    }

    pub fn refresh_errors(&self) -> u64 {
        self.refresh_errors.load(Ordering::Relaxed)
    }

    /// True if the cache has never received a successful fetch.
    pub fn is_empty(&self) -> bool {
        self.last_refresh_ns.load(Ordering::Relaxed) == 0
    }
}

pub struct BlockhashCacheRunner {
    pub cache: Arc<BlockhashCache>,
    pub handle: JoinHandle<()>,
}

pub fn spawn(
    rpc: Arc<RpcClient>,
    refresh_interval: Duration,
    stop: Arc<AtomicBool>,
) -> BlockhashCacheRunner {
    let inner = Arc::new(ArcSwap::from_pointee(Hash::default()));
    let last_refresh_ns = Arc::new(AtomicU64::new(0));
    let refresh_errors = Arc::new(AtomicU64::new(0));
    let cache = Arc::new(BlockhashCache {
        inner: inner.clone(),
        last_refresh_ns: last_refresh_ns.clone(),
        refresh_errors: refresh_errors.clone(),
    });
    let handle = std::thread::Builder::new()
        .name("blockhash-refresh".into())
        .spawn(move || {
            // Initial fetch — keep retrying with backoff until we get one.
            let mut backoff = Duration::from_millis(200);
            loop {
                if stop.load(Ordering::Relaxed) {
                    return;
                }
                match rpc.get_latest_blockhash() {
                    Ok(h) => {
                        inner.store(Arc::new(h));
                        update_last_refresh(&last_refresh_ns);
                        tracing::info!(blockhash = %h, "blockhash cache primed");
                        break;
                    }
                    Err(e) => {
                        refresh_errors.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(error = %e, "initial blockhash fetch failed; retrying");
                        std::thread::sleep(backoff);
                        backoff = (backoff * 2).min(Duration::from_secs(5));
                    }
                }
            }
            // Steady refresh loop.
            let mut next = Instant::now() + refresh_interval;
            while !stop.load(Ordering::Relaxed) {
                let now = Instant::now();
                if now < next {
                    std::thread::sleep((next - now).min(Duration::from_millis(200)));
                    continue;
                }
                next = now + refresh_interval;
                match rpc.get_latest_blockhash() {
                    Ok(h) => {
                        inner.store(Arc::new(h));
                        update_last_refresh(&last_refresh_ns);
                    }
                    Err(e) => {
                        refresh_errors.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(error = %e, "blockhash refresh failed; keeping previous");
                    }
                }
            }
        })
        .expect("spawn blockhash-refresh");
    BlockhashCacheRunner { cache, handle }
}

fn update_last_refresh(slot: &AtomicU64) {
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    slot.store(now_ns, Ordering::Relaxed);
}
