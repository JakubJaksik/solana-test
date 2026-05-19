//! Dispatcher — fan-out async sends per-sender.

use crate::counters::BenchCounters;
use crate::matcher::{RegisterEvent, SendEvent};
use crate::pool::TxPool;
use crate::preparer::PreparedTrigger;
use crate::senders::TxSender;
use crate::trigger::TriggerEvent;
use crate::trigger_id::TriggerId;
use crossbeam_channel::{Receiver, Sender};
use dashmap::DashSet;
use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use solana_sdk::signature::Signature;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

pub struct DispatcherConfig {
    pub trigger_rx: Receiver<TriggerEvent>,
    pub prepared_rx: Receiver<PreparedTrigger>,
    pub pool: Arc<TxPool>,
    pub senders: HashMap<u8, Arc<dyn TxSender>>,
    pub sender_meta: HashMap<u8, SenderMeta>,
    pub register_tx: Sender<RegisterEvent>,
    pub send_event_tx: Sender<SendEvent>,
    pub pending_sigs: Arc<DashSet<Signature>>,
    pub schedule_seed: u64,
    pub counters: Arc<BenchCounters>,
    pub stop: Arc<AtomicBool>,
}

#[derive(Debug, Clone)]
pub struct SenderMeta {
    pub name: String,
    pub endpoint_url: String,
    pub protocol: String,
    pub auth_tier: Option<String>,
    pub tip_lamports: u64,
    pub priority_fee_microlamports: u64,
    pub compute_unit_limit: u32,
}

pub fn run_blocking(cfg: DispatcherConfig) -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .thread_name("dispatcher-tokio")
        .build()?;
    runtime.block_on(run_async(cfg));
    Ok(())
}

async fn run_async(cfg: DispatcherConfig) {
    let (cmd_tx, mut cmd_rx) = mpsc::channel::<SendCommand>(4096);
    let send_event_tx_clone = cfg.send_event_tx.clone();

    let workers = tokio::spawn(async move {
        while let Some(cmd) = cmd_rx.recv().await {
            let sender = cmd.sender.clone();
            let outcome_tx = send_event_tx_clone.clone();
            tokio::spawn(async move {
                let outcome = sender.send(&cmd.tx).await;
                let ev = SendEvent {
                    trigger_id: cmd.trigger_id,
                    sender_id: cmd.sender_id,
                    send_at: outcome.send_at,
                    send_ack_at: outcome.send_ack_at,
                    signature: outcome.signature,
                    provider_request_id: outcome.provider_request_id,
                    http_status: outcome.http_status,
                    rpc_err_code: outcome.rpc_err_code,
                    rpc_err_message: outcome.rpc_err_message,
                    rate_limit_state: outcome.rate_limit_state,
                    error: outcome.error,
                };
                let _ = outcome_tx.send(ev);
            });
        }
    });

    loop {
        if cfg.stop.load(Ordering::Relaxed) {
            break;
        }
        let trigger = match cfg.trigger_rx.recv_timeout(std::time::Duration::from_millis(200)) {
            Ok(t) => t,
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
        };
        let mut prepared = None;
        for _ in 0..10 {
            match cfg.prepared_rx.try_recv() {
                Ok(p) if p.slot == trigger.slot && p.tick == trigger.tick => {
                    prepared = Some(p);
                    break;
                }
                Ok(_) => continue,
                Err(_) => break,
            }
        }
        let nonce_account_id = prepared.map(|p| p.nonce_account_id).unwrap_or(0);

        let variants = cfg.pool.take_all_for(trigger.slot, trigger.tick);
        if variants.is_empty() {
            cfg.counters.pool_empty.fetch_add(1, Ordering::Relaxed);
            continue;
        }

        let perm_seed = cfg.schedule_seed ^ (trigger.slot << 8) ^ (trigger.tick as u64);
        let mut rng = SmallRng::seed_from_u64(perm_seed);
        let mut indices: Vec<usize> = (0..variants.len()).collect();
        indices.shuffle(&mut rng);

        let trigger_id = TriggerId::new(trigger.slot, trigger.tick, nonce_account_id);
        for (order, &idx) in indices.iter().enumerate() {
            let (sender_id, presigned) = &variants[idx];
            let Some(sender) = cfg.senders.get(sender_id) else { continue };
            let meta = match cfg.sender_meta.get(sender_id) {
                Some(m) => m.clone(),
                None => continue,
            };
            let sig = presigned.tx.signatures.first().copied().unwrap_or_default();
            cfg.pending_sigs.insert(sig);
            let reg = RegisterEvent {
                trigger_id,
                sender_id: *sender_id,
                sender_name: meta.name,
                endpoint_url: meta.endpoint_url,
                protocol: meta.protocol,
                auth_tier: meta.auth_tier,
                tip_account_used: None,
                tip_lamports: meta.tip_lamports,
                priority_fee_microlamports: meta.priority_fee_microlamports,
                compute_unit_limit: meta.compute_unit_limit,
                signature: sig,
                tx_message_hash: presigned.message_hash,
                send_order_in_trigger: order as u8,
                trigger_slot: trigger.slot,
                trigger_tick: trigger.tick,
                nonce_account_id,
                nonce_blockhash_used: solana_sdk::hash::Hash::default(),
                prepared_at: presigned.prepared_at,
                pool_ready_at: presigned.pool_ready_at,
                trigger_observed_at: trigger.observed_at,
            };
            let _ = cfg.register_tx.send(reg);
            let cmd = SendCommand {
                tx: presigned.tx.as_ref().clone(),
                trigger_id,
                sender_id: *sender_id,
                sender: sender.clone(),
            };
            if cmd_tx.send(cmd).await.is_err() {
                cfg.counters.send_queue_full.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    drop(cmd_tx);
    let _ = workers.await;
}

struct SendCommand {
    tx: solana_sdk::transaction::Transaction,
    trigger_id: TriggerId,
    sender_id: u8,
    sender: Arc<dyn TxSender>,
}
