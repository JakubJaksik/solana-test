use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender};
use dashmap::DashSet;
use solana_sdk::signature::Signature;
use tracing::info;

use crate::counters::BenchCounters;
use crate::helius_sender::{HeliusSender, SendError};
use crate::observer::SendCommand;

#[derive(Debug)]
pub struct SendEvent {
    pub signature: Signature,
    pub schedule_slot: u64,
    pub schedule_tick: u8,
    pub trigger_observed_at: Instant,
    pub send_at: Instant,
    pub response_at: Instant,
    pub error: Option<String>,
}

pub struct SenderConfig {
    pub send_queue: Receiver<SendCommand>,
    pub send_event_tx: Sender<SendEvent>,
    pub pending_sigs: Arc<DashSet<Signature>>,
    pub helius: Arc<HeliusSender>,
    pub blockhash_max_age: Duration,
    pub pinned_core: Option<usize>,
    pub counters: Arc<BenchCounters>,
    pub stop: Arc<AtomicBool>,
}

pub fn spawn(cfg: SenderConfig) -> std::io::Result<std::thread::JoinHandle<()>> {
    std::thread::Builder::new()
        .name("sender".into())
        .spawn(move || {
            if let Some(c) = cfg.pinned_core {
                core_affinity::set_for_current(core_affinity::CoreId { id: c });
            }
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .enable_all()
                .build()
                .expect("build tokio runtime");
            rt.block_on(run_loop(cfg));
        })
}

async fn run_loop(cfg: SenderConfig) {
    loop {
        if cfg.stop.load(Ordering::Relaxed) {
            break;
        }
        let cmd = match cfg.send_queue.recv_timeout(Duration::from_millis(100)) {
            Ok(c) => c,
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return,
        };
        // Guard: skip if blockhash is too old (defense in depth vs. preparer expiry tracking).
        if cmd.tx.built_at.elapsed() > cfg.blockhash_max_age {
            cfg.counters.inc(&cfg.counters.blockhash_expired);
            continue;
        }

        let sig = cmd.tx.signature;
        // Register in pending BEFORE send so observer can match earliest.
        cfg.pending_sigs.insert(sig);

        let helius = cfg.helius.clone();
        let counters = cfg.counters.clone();
        let send_event_tx = cfg.send_event_tx.clone();
        let trigger_observed_at = cmd.trigger_observed_at;
        let schedule_slot = cmd.schedule_slot;
        let schedule_tick = cmd.schedule_tick;
        let serialized = cmd.tx.serialized;
        tokio::spawn(async move {
            let send_at = Instant::now();
            let result = helius.send_raw(serialized).await;
            let response_at = Instant::now();

            let error = match &result {
                Ok(_) => None,
                Err(SendError::HttpStatus(code, body)) => {
                    counters.inc(&counters.send_http_error);
                    Some(format!("http {}: {}", code, body))
                }
                Err(SendError::Network(e)) => {
                    counters.inc(&counters.send_network_error);
                    Some(format!("net: {}", e))
                }
                Err(SendError::RpcError(msg)) => {
                    counters.inc(&counters.send_http_error);
                    Some(format!("rpc: {}", msg))
                }
                Err(SendError::Parse(msg)) => {
                    counters.inc(&counters.send_http_error);
                    Some(format!("parse: {}", msg))
                }
            };

            let ev = SendEvent {
                signature: sig,
                schedule_slot,
                schedule_tick,
                trigger_observed_at,
                send_at,
                response_at,
                error,
            };
            if send_event_tx.try_send(ev).is_err() {
                counters.inc(&counters.send_event_queue_full);
            }
        });
    }
    info!("sender thread exiting");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn send_event_struct_constructable() {
        let ev = SendEvent {
            signature: Signature::default(),
            schedule_slot: 100,
            schedule_tick: 5,
            trigger_observed_at: Instant::now(),
            send_at: Instant::now(),
            response_at: Instant::now(),
            error: None,
        };
        assert_eq!(ev.schedule_slot, 100);
    }
}
