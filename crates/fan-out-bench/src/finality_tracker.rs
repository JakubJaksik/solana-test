//! Finality tracker — polls `getSignatureStatuses(commitment=finalized)`
//! for tentative records; writes finality-updates.jsonl side file.

use crate::counters::BenchCounters;
use crate::trigger_id::TriggerId;
use crossbeam_channel::Receiver;
use serde::Serialize;
use solana_client::rpc_client::RpcClient;
use solana_sdk::signature::Signature;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct FinalityQueueEntry {
    pub trigger_id: TriggerId,
    pub sender_id: u8,
    pub signature: Signature,
    pub queued_at: Instant,
}

#[derive(Debug, Serialize)]
struct FinalityUpdate {
    trigger_id: String,
    sender_id: u8,
    tx_signature: String,
    final_status: &'static str,
    finalization_slot: Option<u64>,
    finalization_checked_at_ns: u64,
}

pub struct FinalityTrackerConfig {
    pub finality_rx: Receiver<FinalityQueueEntry>,
    pub rpc: Arc<RpcClient>,
    pub output_path: PathBuf,
    pub poll_interval: Duration,
    pub anchor: Instant,
    pub pinned_core: Option<usize>,
    pub counters: Arc<BenchCounters>,
    pub stop: Arc<AtomicBool>,
}

pub fn spawn(cfg: FinalityTrackerConfig) -> std::io::Result<JoinHandle<()>> {
    std::thread::Builder::new()
        .name("finality-tracker".into())
        .spawn(move || {
            if let Some(core) = cfg.pinned_core {
                core_affinity::set_for_current(core_affinity::CoreId { id: core });
            }
            run_loop(cfg);
        })
}

fn run_loop(cfg: FinalityTrackerConfig) {
    let mut pending: Vec<FinalityQueueEntry> = Vec::with_capacity(1024);
    let mut file = match OpenOptions::new()
        .create(true)
        .append(true)
        .open(&cfg.output_path)
    {
        Ok(f) => f,
        Err(e) => {
            tracing::error!(error = %e, path = ?cfg.output_path, "failed to open finality-updates.jsonl");
            return;
        }
    };

    loop {
        if cfg.stop.load(Ordering::Relaxed) {
            break;
        }

        while let Ok(entry) = cfg.finality_rx.try_recv() {
            pending.push(entry);
        }

        if !pending.is_empty() {
            let chunk_size = pending.len().min(100);
            let sigs: Vec<Signature> = pending.iter().take(chunk_size).map(|e| e.signature).collect();
            match cfg.rpc.get_signature_statuses(&sigs) {
                Ok(resp) => {
                    let now_ns = Instant::now().duration_since(cfg.anchor).as_nanos() as u64;
                    let statuses = resp.value;
                    let mut still_pending = Vec::new();
                    for (entry, status_opt) in pending.drain(..chunk_size).zip(statuses) {
                        let final_status = match &status_opt {
                            Some(s) if matches!(s.confirmation_status, Some(solana_transaction_status::TransactionConfirmationStatus::Finalized)) => Some("CONFIRMED"),
                            None if entry.queued_at.elapsed() > Duration::from_secs(300) => Some("UNCERTAIN_NO_STATUS"),
                            _ => None,
                        };
                        if let Some(fs) = final_status {
                            let update = FinalityUpdate {
                                trigger_id: hex::encode(entry.trigger_id.as_bytes()),
                                sender_id: entry.sender_id,
                                tx_signature: entry.signature.to_string(),
                                final_status: fs,
                                finalization_slot: status_opt.as_ref().map(|s| s.slot),
                                finalization_checked_at_ns: now_ns,
                            };
                            if let Ok(line) = serde_json::to_string(&update) {
                                let _ = writeln!(file, "{}", line);
                            }
                            match fs {
                                "CONFIRMED" => cfg.counters.finality_confirmed.fetch_add(1, Ordering::Relaxed),
                                "UNCERTAIN_NO_STATUS" => cfg.counters.finality_uncertain.fetch_add(1, Ordering::Relaxed),
                                _ => 0,
                            };
                        } else {
                            still_pending.push(entry);
                        }
                    }
                    pending.extend(still_pending);
                }
                Err(e) => {
                    tracing::warn!(error = %e, "getSignatureStatuses failed");
                }
            }
            let _ = file.flush();
        }

        std::thread::sleep(cfg.poll_interval);
    }
    let _ = file.flush();
}
