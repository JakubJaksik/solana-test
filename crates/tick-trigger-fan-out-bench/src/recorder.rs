//! Recorder — async per-trigger lifecycle aggregator + JSONL writer.
//!
//! Listens on three input channels:
//!   - `register_rx`: emitted by the send dispatcher BEFORE the network call,
//!     so we have the (trigger_id, sender_id, signature) tuple known before
//!     the tx hits the wire.
//!   - `send_rx`: emitted by the send dispatcher AFTER the network call,
//!     carries SendOutcome (ack/error/etc).
//!   - `match_rx`: emitted by the trigger engine when the tx's signature
//!     is observed on chain.
//!
//! Per (trigger_id, sender_id) the recorder maintains a `TriggerAttempt`
//! state machine. On any terminal transition (match observed, send error,
//! deadline elapsed) it writes a JSONL row to disk and drops the state.
//!
//! Hot path: only counter increments and a `try_send` from the engine /
//! sender threads — the recorder runs on its own thread, off the critical
//! path. The disk write is the recorder's problem, not the engine's.

use crate::senders::SendOutcome;
use crate::trigger_engine::{MatchEvent, TriggerId};
use crossbeam_channel::Receiver;
use serde::Serialize;
use solana_sdk::signature::Signature;
use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct RegisterEvent {
    pub trigger_id: TriggerId,
    pub sender_id: u8,
    pub sender_name: String,
    pub endpoint_url: String,
    pub protocol: String,
    pub signature: Signature,
    pub slot: u64,
    pub tick: u8,
    pub trigger_observed_at: Instant,
    pub prepared_at: Instant,
    pub blockhash: solana_sdk::hash::Hash,
}

#[derive(Debug, Clone)]
pub struct SendEvent {
    pub trigger_id: TriggerId,
    pub sender_id: u8,
    pub outcome: SendOutcome,
}

#[derive(Debug, Serialize, Clone, Copy, PartialEq, Eq)]
pub enum FinalOutcome {
    Landed,
    SendError,
    UnknownPending,
}

#[derive(Serialize, Debug)]
pub struct TriggerRecord {
    pub run_id: String,
    pub trigger_id: u64,
    pub slot: u64,
    pub tick: u8,
    pub sender_id: u8,
    pub sender_name: String,
    pub endpoint_url: String,
    pub protocol: String,
    pub tx_signature: String,
    pub blockhash: String,

    // Latency points (nanoseconds since `anchor` Instant).
    pub trigger_observed_at_ns: u64,
    pub prepared_at_ns: u64,
    pub send_at_ns: u64,
    pub send_ack_at_ns: Option<u64>,
    pub observed_at_ns: Option<u64>,

    // Derived latency deltas (ns).
    pub wall_prepared_to_send_ns: Option<u64>,
    pub wall_send_rtt_ns: Option<u64>,
    pub wall_send_to_observed_ns: Option<u64>,
    pub wall_trigger_to_observed_ns: Option<u64>,

    // Observation context (None when not observed).
    pub observed_slot: Option<u64>,
    pub observed_entry_index: Option<u32>,
    pub observed_tick: Option<u8>,
    pub observed_source: Option<&'static str>,

    // Send-side error info.
    pub http_status: Option<u16>,
    pub rpc_err_code: Option<i32>,
    pub rpc_err_message: Option<String>,
    pub send_error: Option<String>,
    pub endpoint_url_used: Option<String>,

    pub final_outcome: FinalOutcome,
}

#[derive(Debug, Default)]
pub struct RecorderCounters {
    pub records_landed: AtomicU64,
    pub records_send_error: AtomicU64,
    pub records_unknown_pending: AtomicU64,
    pub register_events: AtomicU64,
    pub send_events: AtomicU64,
    pub match_events: AtomicU64,
    pub write_errors: AtomicU64,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct RecorderCountersSnapshot {
    pub records_landed: u64,
    pub records_send_error: u64,
    pub records_unknown_pending: u64,
    pub register_events: u64,
    pub send_events: u64,
    pub match_events: u64,
    pub write_errors: u64,
}

impl RecorderCounters {
    pub fn snapshot(&self) -> RecorderCountersSnapshot {
        let l = |c: &AtomicU64| c.load(Ordering::Relaxed);
        RecorderCountersSnapshot {
            records_landed: l(&self.records_landed),
            records_send_error: l(&self.records_send_error),
            records_unknown_pending: l(&self.records_unknown_pending),
            register_events: l(&self.register_events),
            send_events: l(&self.send_events),
            match_events: l(&self.match_events),
            write_errors: l(&self.write_errors),
        }
    }
}

pub struct RecorderConfig {
    pub register_rx: Receiver<RegisterEvent>,
    pub send_rx: Receiver<SendEvent>,
    pub match_rx: Receiver<MatchEvent>,
    pub output_path: PathBuf,
    pub run_id: String,
    pub anchor: Instant,
    pub deadline: Duration,
    pub counters: Arc<RecorderCounters>,
    pub stop: Arc<AtomicBool>,
}

#[derive(Debug)]
struct TriggerAttempt {
    register: Option<RegisterEvent>,
    send: Option<SendEvent>,
    matched: Option<MatchEvent>,
    deadline_at: Instant,
}

pub fn spawn(cfg: RecorderConfig) -> std::io::Result<JoinHandle<()>> {
    std::thread::Builder::new()
        .name("recorder".into())
        .spawn(move || run_loop(cfg))
}

fn run_loop(cfg: RecorderConfig) {
    let mut file = match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&cfg.output_path)
    {
        Ok(f) => f,
        Err(e) => {
            tracing::error!(error = %e, path = %cfg.output_path.display(), "recorder failed to open output");
            return;
        }
    };
    let mut attempts: HashMap<(TriggerId, u8), TriggerAttempt> = HashMap::with_capacity(256);
    let mut sig_to_key: HashMap<Signature, (TriggerId, u8)> = HashMap::with_capacity(256);
    let mut last_sweep = Instant::now();

    loop {
        if cfg.stop.load(Ordering::Relaxed) {
            break;
        }
        crossbeam_channel::select! {
            recv(cfg.register_rx) -> msg => {
                if let Ok(reg) = msg {
                    cfg.counters.register_events.fetch_add(1, Ordering::Relaxed);
                    handle_register(reg, &mut attempts, &mut sig_to_key, &cfg);
                }
            },
            recv(cfg.send_rx) -> msg => {
                if let Ok(s) = msg {
                    cfg.counters.send_events.fetch_add(1, Ordering::Relaxed);
                    handle_send(s, &mut attempts, &mut sig_to_key, &cfg, &mut file);
                }
            },
            recv(cfg.match_rx) -> msg => {
                if let Ok(m) = msg {
                    cfg.counters.match_events.fetch_add(1, Ordering::Relaxed);
                    handle_match(m, &mut attempts, &mut sig_to_key, &cfg, &mut file);
                }
            },
            default(Duration::from_millis(200)) => {},
        }
        if last_sweep.elapsed() >= Duration::from_millis(500) {
            last_sweep = Instant::now();
            sweep_deadlines(&mut attempts, &mut sig_to_key, &cfg, &mut file);
        }
    }
    // Final flush — emit anything still in flight as UNKNOWN_PENDING.
    let keys: Vec<(TriggerId, u8)> = attempts.keys().copied().collect();
    for k in keys {
        if let Some(att) = attempts.remove(&k) {
            if let Some(reg) = &att.register {
                sig_to_key.remove(&reg.signature);
            }
            emit_record(att, FinalOutcome::UnknownPending, &cfg, &mut file);
        }
    }
    let _ = file.flush();
}

fn handle_register(
    reg: RegisterEvent,
    attempts: &mut HashMap<(TriggerId, u8), TriggerAttempt>,
    sig_to_key: &mut HashMap<Signature, (TriggerId, u8)>,
    cfg: &RecorderConfig,
) {
    let key = (reg.trigger_id, reg.sender_id);
    sig_to_key.insert(reg.signature, key);
    let deadline_at = reg.trigger_observed_at + cfg.deadline;
    attempts.insert(
        key,
        TriggerAttempt {
            register: Some(reg),
            send: None,
            matched: None,
            deadline_at,
        },
    );
}

fn handle_send(
    s: SendEvent,
    attempts: &mut HashMap<(TriggerId, u8), TriggerAttempt>,
    sig_to_key: &mut HashMap<Signature, (TriggerId, u8)>,
    cfg: &RecorderConfig,
    file: &mut std::fs::File,
) {
    let key = (s.trigger_id, s.sender_id);
    let Some(att) = attempts.get_mut(&key) else {
        return;
    };
    let send_error = s.outcome.error.is_some();
    att.send = Some(s);
    if send_error {
        // Send-side error is a terminal state — no sig to observe, can emit now.
        if let Some(att) = attempts.remove(&key) {
            if let Some(reg) = &att.register {
                sig_to_key.remove(&reg.signature);
            }
            emit_record(att, FinalOutcome::SendError, cfg, file);
        }
    }
}

fn handle_match(
    m: MatchEvent,
    attempts: &mut HashMap<(TriggerId, u8), TriggerAttempt>,
    sig_to_key: &mut HashMap<Signature, (TriggerId, u8)>,
    cfg: &RecorderConfig,
    file: &mut std::fs::File,
) {
    let Some(&key) = sig_to_key.get(&m.signature) else {
        return; // sig wasn't ours (already emitted as send error, or stale)
    };
    let Some(att) = attempts.get_mut(&key) else {
        sig_to_key.remove(&m.signature);
        return;
    };
    att.matched = Some(m);
    if let Some(att) = attempts.remove(&key) {
        if let Some(reg) = &att.register {
            sig_to_key.remove(&reg.signature);
        }
        emit_record(att, FinalOutcome::Landed, cfg, file);
    }
}

fn sweep_deadlines(
    attempts: &mut HashMap<(TriggerId, u8), TriggerAttempt>,
    sig_to_key: &mut HashMap<Signature, (TriggerId, u8)>,
    cfg: &RecorderConfig,
    file: &mut std::fs::File,
) {
    let now = Instant::now();
    let expired: Vec<(TriggerId, u8)> = attempts
        .iter()
        .filter(|(_, att)| att.deadline_at <= now)
        .map(|(k, _)| *k)
        .collect();
    for key in expired {
        if let Some(att) = attempts.remove(&key) {
            if let Some(reg) = &att.register {
                sig_to_key.remove(&reg.signature);
            }
            emit_record(att, FinalOutcome::UnknownPending, cfg, file);
        }
    }
}

fn emit_record(
    att: TriggerAttempt,
    outcome: FinalOutcome,
    cfg: &RecorderConfig,
    file: &mut std::fs::File,
) {
    let Some(reg) = att.register else {
        return;
    };
    let ns_since = |t: Instant| t.saturating_duration_since(cfg.anchor).as_nanos() as u64;
    let trigger_observed_at_ns = ns_since(reg.trigger_observed_at);
    let prepared_at_ns = ns_since(reg.prepared_at);
    let (send_at_ns, send_ack_at_ns, http_status, rpc_err_code, rpc_err_message, send_error, endpoint_url_used) =
        match &att.send {
            Some(s) => (
                ns_since(s.outcome.send_at),
                s.outcome.send_ack_at.map(ns_since),
                s.outcome.http_status,
                s.outcome.rpc_err_code,
                s.outcome.rpc_err_message.clone(),
                s.outcome.error.clone(),
                s.outcome.endpoint_url_used.clone(),
            ),
            None => (0, None, None, None, None, None, None),
        };
    let observed_at_ns = att.matched.as_ref().map(|m| ns_since(m.observed_at));
    let observed_slot = att.matched.as_ref().map(|m| m.observed_slot);
    let observed_entry_index = att.matched.as_ref().map(|m| m.observed_entry_index);
    let observed_tick = att.matched.as_ref().map(|m| m.observed_tick);
    let observed_source = att.matched.as_ref().map(|m| match m.observed_source {
        entry_sources::SourceKind::ShredStream => "SS",
        entry_sources::SourceKind::Yellowstone => "YS",
    });

    let wall_prepared_to_send_ns = att.send.as_ref().map(|_| {
        send_at_ns.saturating_sub(prepared_at_ns)
    });
    let wall_send_rtt_ns = match (&att.send, send_ack_at_ns) {
        (Some(_), Some(ack_ns)) => Some(ack_ns.saturating_sub(send_at_ns)),
        _ => None,
    };
    let wall_send_to_observed_ns = match (&att.send, observed_at_ns) {
        (Some(_), Some(obs_ns)) => Some(obs_ns.saturating_sub(send_at_ns)),
        _ => None,
    };
    let wall_trigger_to_observed_ns =
        observed_at_ns.map(|obs_ns| obs_ns.saturating_sub(trigger_observed_at_ns));

    match outcome {
        FinalOutcome::Landed => {
            cfg.counters.records_landed.fetch_add(1, Ordering::Relaxed);
        }
        FinalOutcome::SendError => {
            cfg.counters
                .records_send_error
                .fetch_add(1, Ordering::Relaxed);
        }
        FinalOutcome::UnknownPending => {
            cfg.counters
                .records_unknown_pending
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    let record = TriggerRecord {
        run_id: cfg.run_id.clone(),
        trigger_id: reg.trigger_id.0,
        slot: reg.slot,
        tick: reg.tick,
        sender_id: reg.sender_id,
        sender_name: reg.sender_name,
        endpoint_url: reg.endpoint_url,
        protocol: reg.protocol,
        tx_signature: reg.signature.to_string(),
        blockhash: reg.blockhash.to_string(),
        trigger_observed_at_ns,
        prepared_at_ns,
        send_at_ns,
        send_ack_at_ns,
        observed_at_ns,
        wall_prepared_to_send_ns,
        wall_send_rtt_ns,
        wall_send_to_observed_ns,
        wall_trigger_to_observed_ns,
        observed_slot,
        observed_entry_index,
        observed_tick,
        observed_source,
        http_status,
        rpc_err_code,
        rpc_err_message,
        send_error,
        endpoint_url_used,
        final_outcome: outcome,
    };
    if let Ok(line) = serde_json::to_string(&record) {
        if writeln!(file, "{}", line).is_err() {
            cfg.counters.write_errors.fetch_add(1, Ordering::Relaxed);
        }
    }
}

