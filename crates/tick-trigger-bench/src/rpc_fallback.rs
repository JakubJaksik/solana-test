use std::collections::VecDeque;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use solana_client::rpc_client::RpcClient;
use solana_sdk::signature::Signature;
use tracing::{info, warn};

use crate::counters::BenchCounters;

/// Signatures pending a fallback check. Producers push when finalizer emits
/// UNKNOWN_PENDING. This thread drains in batches and runs RPC queries.
#[derive(Default, Clone)]
pub struct FallbackQueue {
    inner: Arc<Mutex<VecDeque<Signature>>>,
}

impl FallbackQueue {
    pub fn push(&self, sig: Signature) {
        self.inner.lock().unwrap().push_back(sig);
    }
    fn drain_up_to(&self, n: usize) -> Vec<Signature> {
        let mut g = self.inner.lock().unwrap();
        let take = g.len().min(n);
        g.drain(..take).collect()
    }
    pub fn len(&self) -> usize { self.inner.lock().unwrap().len() }
}

pub struct RpcFallbackConfig {
    pub queue: FallbackQueue,
    pub rpc_url: String,
    pub annotations_path: PathBuf,
    pub poll_interval: Duration,
    pub batch_size: usize,
    pub pinned_core: Option<usize>,
    pub counters: Arc<BenchCounters>,
    pub stop: Arc<AtomicBool>,
}

pub fn spawn(cfg: RpcFallbackConfig) -> std::io::Result<std::thread::JoinHandle<()>> {
    std::thread::Builder::new()
        .name("rpc-fallback".into())
        .spawn(move || {
            if let Some(c) = cfg.pinned_core {
                core_affinity::set_for_current(core_affinity::CoreId { id: c });
            }
            run_loop(cfg);
        })
}

fn run_loop(cfg: RpcFallbackConfig) {
    let client = RpcClient::new(cfg.rpc_url.clone());
    let mut annotations = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&cfg.annotations_path)
        .ok();
    while !cfg.stop.load(Ordering::Relaxed) {
        std::thread::sleep(cfg.poll_interval);
        loop {
            let batch = cfg.queue.drain_up_to(cfg.batch_size);
            if batch.is_empty() { break; }
            let result = client.get_signature_statuses(&batch);
            match result {
                Ok(resp) => {
                    for (sig, status) in batch.iter().zip(resp.value.iter()) {
                        let landed = status.as_ref().map(|s| {
                            s.confirmations.is_none() || s.confirmation_status.is_some()
                        }).unwrap_or(false);
                        let line = serde_json::json!({
                            "sig": bs58::encode(sig.as_ref()).into_string(),
                            "rpc_landed": landed,
                            "status": if landed { "MISSING_FROM_STREAM" } else { "TRULY_MISSING" },
                        });
                        if let Some(f) = annotations.as_mut() {
                            let _ = writeln!(f, "{}", line);
                        }
                    }
                }
                Err(e) => {
                    cfg.counters.inc(&cfg.counters.rpc_fallback_error);
                    warn!(error = %e, "rpc fallback batch failed");
                }
            }
        }
    }
    info!("rpc-fallback exiting");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_push_drain() {
        let q = FallbackQueue::default();
        q.push(Signature::default());
        q.push(Signature::default());
        assert_eq!(q.len(), 2);
        let batch = q.drain_up_to(10);
        assert_eq!(batch.len(), 2);
        assert_eq!(q.len(), 0);
    }
}
