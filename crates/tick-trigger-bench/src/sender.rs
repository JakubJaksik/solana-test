use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam_channel::Sender;
use dashmap::DashSet;
use futures_util::StreamExt;
use solana_sdk::signature::Signature;
use tokio::sync::mpsc as tokio_mpsc;
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

pub struct DispatcherConfig {
    pub send_queue: tokio_mpsc::Receiver<SendCommand>,
    pub send_event_tx: Sender<SendEvent>,
    pub pending_sigs: Arc<DashSet<Signature>>,
    pub helius: Arc<HeliusSender>,
    pub blockhash_max_age: Duration,
    pub max_inflight: usize,
    pub counters: Arc<BenchCounters>,
}

/// Run the send dispatcher as a single tokio task on the provided runtime.
/// Consumes SendCommands from the tokio mpsc and processes them with
/// `for_each_concurrent(max_inflight)` — N HTTP requests in flight at once,
/// no per-request `tokio::spawn`. Replaces the previous dedicated sender
/// thread + spawn-per-request pattern to shave ~50-100μs from M1.
pub async fn run_dispatcher(cfg: DispatcherConfig) {
    use futures_util::stream;
    let DispatcherConfig {
        send_queue,
        send_event_tx,
        pending_sigs,
        helius,
        blockhash_max_age,
        max_inflight,
        counters,
    } = cfg;

    // Adapt the tokio Receiver into a Stream<Item = SendCommand>.
    let cmd_stream = stream::unfold(send_queue, |mut rx| async move {
        let next = rx.recv().await?;
        Some((next, rx))
    });

    cmd_stream
        .for_each_concurrent(max_inflight, move |cmd| {
            let helius = helius.clone();
            let send_event_tx = send_event_tx.clone();
            let pending_sigs = pending_sigs.clone();
            let counters = counters.clone();
            async move {
                process_one(
                    cmd,
                    &helius,
                    &send_event_tx,
                    &pending_sigs,
                    blockhash_max_age,
                    &counters,
                )
                .await;
            }
        })
        .await;

    info!("send-dispatcher exiting (channel closed)");
}

async fn process_one(
    cmd: SendCommand,
    helius: &Arc<HeliusSender>,
    send_event_tx: &Sender<SendEvent>,
    pending_sigs: &Arc<DashSet<Signature>>,
    blockhash_max_age: Duration,
    counters: &Arc<BenchCounters>,
) {
    if cmd.tx.built_at.elapsed() > blockhash_max_age {
        counters.inc(&counters.blockhash_expired);
        return;
    }

    let sig = cmd.tx.signature;
    // Register in pending BEFORE send so observer can match earliest.
    pending_sigs.insert(sig);

    let send_at = Instant::now();
    let result = helius.send_raw(cmd.tx.serialized).await;
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
        schedule_slot: cmd.schedule_slot,
        schedule_tick: cmd.schedule_tick,
        trigger_observed_at: cmd.trigger_observed_at,
        send_at,
        response_at,
        error,
    };
    if send_event_tx.try_send(ev).is_err() {
        counters.inc(&counters.send_event_queue_full);
    }
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
