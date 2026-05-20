//! Preparer — signs N variants per scheduled (slot, tick) using NonceManager
//! and tx_builder, inserts into TxPool with prepared_at + pool_ready_at timestamps.

use crate::config::{SenderConfig, SenderKind};
use crate::counters::BenchCounters;
use crate::nonce::manager::NonceManager;
use crate::pool::{PreSignedTx, TxPool};
use crate::schedule::ScheduleEntry;
use crate::tip_accounts::TipAccountRotator;
use crate::tx_builder::{build_variant, VariantParams};
use crossbeam_channel::Receiver;
use solana_sdk::signature::Keypair;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

pub struct PreparerConfig {
    pub schedule_rx: Receiver<ScheduleEntry>,
    pub senders: Vec<SenderConfig>,
    pub tip_rotators: HashMap<u8, Arc<TipAccountRotator>>,
    pub nonce_manager: Arc<NonceManager>,
    pub pool: Arc<TxPool>,
    pub authority: Arc<Keypair>,
    pub priority_fee_microlamports: u64,
    pub compute_unit_limit: u32,
    pub pinned_core: Option<usize>,
    pub counters: Arc<BenchCounters>,
    pub stop: Arc<AtomicBool>,
}

pub struct PreparedTrigger {
    pub slot: u64,
    pub tick: u8,
    pub nonce_account_id: u16,
}

pub fn spawn(cfg: PreparerConfig) -> (std::io::Result<JoinHandle<()>>, crossbeam_channel::Receiver<PreparedTrigger>) {
    let (prepared_tx, prepared_rx) = crossbeam_channel::bounded::<PreparedTrigger>(8192);
    let handle = std::thread::Builder::new()
        .name("preparer".into())
        .spawn(move || {
            if let Some(core) = cfg.pinned_core {
                core_affinity::set_for_current(core_affinity::CoreId { id: core });
            }
            run_loop(cfg, prepared_tx);
        });
    (handle, prepared_rx)
}

fn run_loop(cfg: PreparerConfig, prepared_tx: crossbeam_channel::Sender<PreparedTrigger>) {
    use solana_sdk::signer::Signer;
    let authority_pubkey = cfg.authority.pubkey();
    let mut total_prepared: u64 = 0;
    let mut total_signed: u64 = 0;
    let mut last_status = Instant::now();
    // Local queue: when no ready nonce is available we MUST NOT drop entries —
    // schedule_pump generates each (slot, tick) deterministically and never
    // re-sends. Buffer pending entries here and wait for a nonce.
    let mut pending: VecDeque<ScheduleEntry> = VecDeque::with_capacity(1024);
    loop {
        if cfg.stop.load(Ordering::Relaxed) {
            break;
        }

        if last_status.elapsed() >= Duration::from_secs(5) {
            last_status = Instant::now();
            tracing::info!(
                total_prepared,
                total_signed,
                nonce_ready = cfg.nonce_manager.ready_count(),
                nonce_total = cfg.nonce_manager.len(),
                pending_entries = pending.len(),
                "preparer status"
            );
        }

        // Drain any newly-arrived schedule entries into the local queue.
        while let Ok(e) = cfg.schedule_rx.try_recv() {
            pending.push_back(e);
        }

        // If queue is empty, block briefly waiting for the next entry.
        if pending.is_empty() {
            match cfg.schedule_rx.recv_timeout(Duration::from_millis(200)) {
                Ok(e) => pending.push_back(e),
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
            }
        }

        // Try to claim a ready nonce. If none, keep entries in queue and back off.
        let Some((nonce_id, nonce_pubkey, nonce_blockhash)) = cfg.nonce_manager.take_ready() else {
            cfg.counters.nonce_stalls.fetch_add(1, Ordering::Relaxed);
            std::thread::sleep(Duration::from_millis(2));
            continue;
        };

        // Safe: we just verified pending is non-empty above and only this thread pops.
        let entry = pending.pop_front().expect("pending non-empty after refill");
        total_prepared += 1;

        let prepared_at = Instant::now();
        let mut signed_count = 0;
        for sender in &cfg.senders {
            if !sender.enabled {
                continue;
            }
            let tip_account = cfg.tip_rotators.get(&sender.id).and_then(|r| r.next());
            let needs_tip_account = !matches!(sender.kind, SenderKind::Triton | SenderKind::Harmonic | SenderKind::Mock);
            if needs_tip_account && tip_account.is_none() {
                continue;
            }

            let params = VariantParams {
                nonce_pubkey,
                nonce_blockhash,
                payer: authority_pubkey,
                sender_id: sender.id,
                sender_kind: sender.kind,
                tip_account,
                tip_lamports: sender.tip_lamports,
                priority_fee_microlamports: cfg.priority_fee_microlamports,
                compute_unit_limit: cfg.compute_unit_limit,
            };

            match build_variant(params, &cfg.authority) {
                Ok(variant) => {
                    let pool_ready_at = Instant::now();
                    let pre = PreSignedTx {
                        tx: Arc::new(variant.tx),
                        message_hash: variant.message_hash,
                        prepared_at,
                        pool_ready_at,
                    };
                    cfg.pool.insert(entry.slot, entry.tick, sender.id, pre);
                    signed_count += 1;
                    total_signed += 1;
                }
                Err(e) => {
                    tracing::warn!(error = %e, sender = %sender.name, "build_variant failed");
                    cfg.counters.preparer_signing_fail.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        tracing::debug!(slot = entry.slot, tick = entry.tick, nonce_id, signed_count, "prepared");

        if prepared_tx.try_send(PreparedTrigger {
            slot: entry.slot,
            tick: entry.tick,
            nonce_account_id: nonce_id,
        }).is_err() {
            cfg.counters.send_queue_full.fetch_add(1, Ordering::Relaxed);
        }
    }
}
