//! RPC fallback — for tentative UNKNOWN_PENDING records, poll
//! getSignatureStatuses to determine TRULY_MISSING vs late LANDED.

use crate::counters::BenchCounters;
use crate::finality_tracker::FinalityQueueEntry;
use crossbeam_channel::Receiver;
use serde::Serialize;
use solana_client::rpc_client::RpcClient;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

#[derive(Debug, Serialize)]
struct FallbackUpdate {
    trigger_id: String,
    sender_id: u8,
    tx_signature: String,
    final_status: &'static str,
    finalization_checked_at_ns: u64,
}

pub struct RpcFallbackConfig {
    pub fallback_rx: Receiver<FinalityQueueEntry>,
    pub rpc: Arc<RpcClient>,
    pub output_path: PathBuf,
    pub poll_interval: Duration,
    pub max_age_secs: u64,
    pub anchor: Instant,
    pub pinned_core: Option<usize>,
    pub counters: Arc<BenchCounters>,
    pub stop: Arc<AtomicBool>,
}

pub fn spawn(cfg: RpcFallbackConfig) -> std::io::Result<JoinHandle<()>> {
    std::thread::Builder::new()
        .name("rpc-fallback".into())
        .spawn(move || {
            if let Some(core) = cfg.pinned_core {
                core_affinity::set_for_current(core_affinity::CoreId { id: core });
            }
            run_loop(cfg);
        })
}

fn run_loop(cfg: RpcFallbackConfig) {
    let mut pending: Vec<FinalityQueueEntry> = Vec::new();
    let mut file = match OpenOptions::new()
        .create(true)
        .append(true)
        .open(&cfg.output_path)
    {
        Ok(f) => f,
        Err(e) => {
            tracing::error!(error = %e, path = ?cfg.output_path, "failed to open fallback log");
            return;
        }
    };

    loop {
        if cfg.stop.load(Ordering::Relaxed) {
            break;
        }

        while let Ok(entry) = cfg.fallback_rx.try_recv() {
            pending.push(entry);
        }

        if !pending.is_empty() {
            let chunk_size = pending.len().min(100);
            let sigs: Vec<_> = pending.iter().take(chunk_size).map(|e| e.signature).collect();
            match cfg.rpc.get_signature_statuses(&sigs) {
                Ok(resp) => {
                    let now_ns = Instant::now().duration_since(cfg.anchor).as_nanos() as u64;
                    let statuses = resp.value;
                    let mut still_pending = Vec::new();
                    for (entry, status_opt) in pending.drain(..chunk_size).zip(statuses) {
                        let age = entry.queued_at.elapsed().as_secs();
                        let final_status = if status_opt.is_some() {
                            Some("CONFIRMED")
                        } else if age >= cfg.max_age_secs {
                            Some("UNCERTAIN_NO_STATUS")
                        } else {
                            None
                        };

                        if let Some(fs) = final_status {
                            let update = FallbackUpdate {
                                trigger_id: hex::encode(entry.trigger_id.as_bytes()),
                                sender_id: entry.sender_id,
                                tx_signature: entry.signature.to_string(),
                                final_status: fs,
                                finalization_checked_at_ns: now_ns,
                            };
                            if let Ok(line) = serde_json::to_string(&update) {
                                let _ = writeln!(file, "{}", line);
                            }
                            match fs {
                                "CONFIRMED" => cfg.counters.rpc_fallback_recovered_landed.fetch_add(1, Ordering::Relaxed),
                                "UNCERTAIN_NO_STATUS" => cfg.counters.rpc_fallback_confirmed_missing.fetch_add(1, Ordering::Relaxed),
                                _ => 0,
                            };
                        } else {
                            still_pending.push(entry);
                        }
                    }
                    pending.extend(still_pending);
                }
                Err(e) => {
                    tracing::warn!(error = %e, "rpc fallback getSignatureStatuses failed");
                    cfg.counters.rpc_fallback_error.fetch_add(1, Ordering::Relaxed);
                }
            }
            let _ = file.flush();
        }

        std::thread::sleep(cfg.poll_interval);
    }
}
