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

use crate::nonce::local_compute::SlotHashCache;
use crate::nonce::manager::{NonceId, NonceManager};
use crate::senders::SendOutcome;
use crate::trigger_engine::{MatchEvent, TriggerId};
use crossbeam_channel::Receiver;
use serde::Serialize;
use solana_sdk::signature::Signature;
use std::collections::{HashMap, HashSet};
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
    /// `Some(id)` when nonce-mode: the nonce account used. Recorder uses
    /// this to call `NonceManager::on_landing_with_blockhash` after a sibling
    /// is observed landing (computing next nonce via SlotHashCache).
    pub nonce_id: Option<NonceId>,
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
    /// Provider-side request id (Jito bundle UUID, Helius signature, etc).
    /// Useful for follow-up status queries against the provider.
    pub provider_request_id: Option<String>,

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
    /// Successful nonce advances pushed to NonceManager from `MatchEvent`.
    pub nonce_advanced_local: AtomicU64,
    /// MatchEvents where we couldn't find prev slot's last_entry_hash in
    /// the local cache (slot S-1..S-5 all missing). RPC fallback will
    /// eventually recover.
    pub nonce_local_compute_miss: AtomicU64,
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
    pub nonce_advanced_local: u64,
    pub nonce_local_compute_miss: u64,
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
            nonce_advanced_local: l(&self.nonce_advanced_local),
            nonce_local_compute_miss: l(&self.nonce_local_compute_miss),
        }
    }
}

/// Per-(sender_id) aggregate, captured at end of run from in-memory state.
#[derive(Debug, Clone, Default, Serialize)]
pub struct PerSenderStats {
    pub sender_id: u8,
    pub attempts: u64,
    pub landed: u64,
    pub send_error: u64,
    pub unknown_pending: u64,
    pub send_rtt_us_sum: u64,
    pub send_rtt_us_count: u64,
    pub send_to_observed_us_sum: u64,
    pub send_to_observed_us_count: u64,
}

impl PerSenderStats {
    pub fn send_rtt_us_avg(&self) -> Option<f64> {
        if self.send_rtt_us_count == 0 {
            None
        } else {
            Some(self.send_rtt_us_sum as f64 / self.send_rtt_us_count as f64)
        }
    }
    pub fn send_to_observed_us_avg(&self) -> Option<f64> {
        if self.send_to_observed_us_count == 0 {
            None
        } else {
            Some(self.send_to_observed_us_sum as f64 / self.send_to_observed_us_count as f64)
        }
    }
}

/// Top-level summary of "real" work — counted only between the first
/// `RegisterEvent` and the last one, excluding startup/shutdown noise.
#[derive(Debug, Clone, Default, Serialize)]
pub struct ActiveWindowSummary {
    /// First send_at across the whole run (epoch-anchored ns since `anchor`).
    pub first_send_at_ns: Option<u64>,
    /// Last send_at across the whole run.
    pub last_send_at_ns: Option<u64>,
    pub active_secs: f64,

    /// Unique trigger_ids registered within the active window.
    pub triggers_attempted: u64,
    /// Unique trigger_ids that had ≥1 sibling sig observed on chain.
    pub triggers_landed: u64,

    /// Per-trigger attempt totals (all senders combined).
    pub attempts_total: u64,
    pub attempts_landed: u64,
    pub attempts_send_error: u64,
    pub attempts_unknown_pending: u64,

    pub per_sender: Vec<PerSenderStats>,
}

impl ActiveWindowSummary {
    pub fn trigger_land_rate(&self) -> Option<f64> {
        if self.triggers_attempted == 0 {
            None
        } else {
            Some(self.triggers_landed as f64 / self.triggers_attempted as f64)
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
    /// When `Some`, recorder consults `SlotHashCache` on `MatchEvent` and
    /// pushes the next durable nonce to `NonceManager`. When `None`, no-op
    /// (fresh-blockhash mode).
    pub nonce_manager: Option<Arc<NonceManager>>,
    pub slot_hash_cache: Option<Arc<SlotHashCache>>,
    /// Per-trigger / per-sender aggregates accumulated into `summary` at
    /// shutdown. Single-thread access from the recorder loop.
    pub summary: Arc<parking_lot::Mutex<ActiveWindowSummary>>,
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

/// In-loop aggregate state that gets dumped into `cfg.summary` at shutdown.
#[derive(Default)]
struct AggregateState {
    first_send_at: Option<Instant>,
    last_send_at: Option<Instant>,
    triggers_attempted: HashSet<TriggerId>,
    triggers_landed: HashSet<TriggerId>,
    per_sender: HashMap<u8, PerSenderStats>,
    attempts_total: u64,
    attempts_landed: u64,
    attempts_send_error: u64,
    attempts_unknown_pending: u64,
}

impl AggregateState {
    fn record_register(&mut self, reg: &RegisterEvent) {
        self.triggers_attempted.insert(reg.trigger_id);
        let entry = self
            .per_sender
            .entry(reg.sender_id)
            .or_insert(PerSenderStats {
                sender_id: reg.sender_id,
                ..Default::default()
            });
        entry.attempts += 1;
        self.attempts_total += 1;
    }
    fn record_send(&mut self, ev: &SendEvent) {
        let send_at = ev.outcome.send_at;
        match self.first_send_at {
            None => self.first_send_at = Some(send_at),
            Some(t) if send_at < t => self.first_send_at = Some(send_at),
            _ => {}
        }
        match self.last_send_at {
            None => self.last_send_at = Some(send_at),
            Some(t) if send_at > t => self.last_send_at = Some(send_at),
            _ => {}
        }
        if let Some(stats) = self.per_sender.get_mut(&ev.sender_id) {
            if let (Some(ack), send) = (ev.outcome.send_ack_at, ev.outcome.send_at) {
                let rtt_us = ack.saturating_duration_since(send).as_micros() as u64;
                stats.send_rtt_us_sum += rtt_us;
                stats.send_rtt_us_count += 1;
            }
        }
    }
    fn record_outcome(
        &mut self,
        trigger_id: TriggerId,
        sender_id: u8,
        outcome: FinalOutcome,
        send_to_observed_us: Option<u64>,
    ) {
        let stats = self
            .per_sender
            .entry(sender_id)
            .or_insert(PerSenderStats {
                sender_id,
                ..Default::default()
            });
        match outcome {
            FinalOutcome::Landed => {
                stats.landed += 1;
                self.attempts_landed += 1;
                self.triggers_landed.insert(trigger_id);
                if let Some(us) = send_to_observed_us {
                    stats.send_to_observed_us_sum += us;
                    stats.send_to_observed_us_count += 1;
                }
            }
            FinalOutcome::SendError => {
                stats.send_error += 1;
                self.attempts_send_error += 1;
            }
            FinalOutcome::UnknownPending => {
                stats.unknown_pending += 1;
                self.attempts_unknown_pending += 1;
            }
        }
    }

    fn into_summary(self, anchor: Instant) -> ActiveWindowSummary {
        let ns_since = |t: Instant| t.saturating_duration_since(anchor).as_nanos() as u64;
        let first_ns = self.first_send_at.map(ns_since);
        let last_ns = self.last_send_at.map(ns_since);
        let active_secs = match (self.first_send_at, self.last_send_at) {
            (Some(a), Some(b)) => b.saturating_duration_since(a).as_secs_f64(),
            _ => 0.0,
        };
        let mut per_sender: Vec<PerSenderStats> = self.per_sender.into_values().collect();
        per_sender.sort_by_key(|s| s.sender_id);
        ActiveWindowSummary {
            first_send_at_ns: first_ns,
            last_send_at_ns: last_ns,
            active_secs,
            triggers_attempted: self.triggers_attempted.len() as u64,
            triggers_landed: self.triggers_landed.len() as u64,
            attempts_total: self.attempts_total,
            attempts_landed: self.attempts_landed,
            attempts_send_error: self.attempts_send_error,
            attempts_unknown_pending: self.attempts_unknown_pending,
            per_sender,
        }
    }
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
    let mut agg = AggregateState::default();
    let mut last_sweep = Instant::now();

    loop {
        if cfg.stop.load(Ordering::Relaxed) {
            break;
        }
        crossbeam_channel::select! {
            recv(cfg.register_rx) -> msg => {
                if let Ok(reg) = msg {
                    cfg.counters.register_events.fetch_add(1, Ordering::Relaxed);
                    agg.record_register(&reg);
                    handle_register(reg, &mut attempts, &mut sig_to_key, &cfg);
                }
            },
            recv(cfg.send_rx) -> msg => {
                if let Ok(s) = msg {
                    cfg.counters.send_events.fetch_add(1, Ordering::Relaxed);
                    agg.record_send(&s);
                    handle_send(s, &mut attempts, &mut sig_to_key, &cfg, &mut file, &mut agg);
                }
            },
            recv(cfg.match_rx) -> msg => {
                if let Ok(m) = msg {
                    cfg.counters.match_events.fetch_add(1, Ordering::Relaxed);
                    handle_match(m, &mut attempts, &mut sig_to_key, &cfg, &mut file, &mut agg);
                }
            },
            default(Duration::from_millis(200)) => {},
        }
        if last_sweep.elapsed() >= Duration::from_millis(500) {
            last_sweep = Instant::now();
            sweep_deadlines(&mut attempts, &mut sig_to_key, &cfg, &mut file, &mut agg);
        }
    }
    // Final flush — emit anything still in flight as UNKNOWN_PENDING.
    let keys: Vec<(TriggerId, u8)> = attempts.keys().copied().collect();
    for k in keys {
        if let Some(att) = attempts.remove(&k) {
            if let Some(reg) = &att.register {
                sig_to_key.remove(&reg.signature);
            }
            let trigger_id = att.register.as_ref().map(|r| r.trigger_id);
            let sender_id = att.register.as_ref().map(|r| r.sender_id);
            emit_record(att, FinalOutcome::UnknownPending, &cfg, &mut file);
            if let (Some(t), Some(s)) = (trigger_id, sender_id) {
                agg.record_outcome(t, s, FinalOutcome::UnknownPending, None);
            }
        }
    }
    let _ = file.flush();
    *cfg.summary.lock() = agg.into_summary(cfg.anchor);
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
    agg: &mut AggregateState,
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
            let tid = att.register.as_ref().map(|r| r.trigger_id);
            let sid = att.register.as_ref().map(|r| r.sender_id);
            emit_record(att, FinalOutcome::SendError, cfg, file);
            if let (Some(t), Some(s)) = (tid, sid) {
                agg.record_outcome(t, s, FinalOutcome::SendError, None);
            }
        }
    }
}

fn handle_match(
    m: MatchEvent,
    attempts: &mut HashMap<(TriggerId, u8), TriggerAttempt>,
    sig_to_key: &mut HashMap<Signature, (TriggerId, u8)>,
    cfg: &RecorderConfig,
    file: &mut std::fs::File,
    agg: &mut AggregateState,
) {
    let Some(&key) = sig_to_key.get(&m.signature) else {
        return; // sig wasn't ours (already emitted as send error, or stale)
    };
    let Some(att) = attempts.get_mut(&key) else {
        sig_to_key.remove(&m.signature);
        return;
    };
    att.matched = Some(m.clone());

    // Nonce hook: if this attempt used a durable nonce, compute the new
    // nonce value from the supervisor's slot_hash_cache and push it to the
    // manager. This is what makes durable-nonce mode self-sustaining without
    // RPC polling.
    if let (Some(mgr), Some(cache), Some(reg)) =
        (&cfg.nonce_manager, &cfg.slot_hash_cache, att.register.as_ref())
    {
        if let Some(nonce_id) = reg.nonce_id {
            match cache.next_nonce_for_landed_slot(m.observed_slot, 5) {
                Some((_src_slot, _prev_hash, next_nonce)) => {
                    mgr.on_landing_with_blockhash(nonce_id, next_nonce);
                    cfg.counters
                        .nonce_advanced_local
                        .fetch_add(1, Ordering::Relaxed);
                }
                None => {
                    // Mark observed landing so the manager state moves
                    // InFlight → AwaitingUpdate; RPC fallback will finish it.
                    mgr.on_observed_landing(nonce_id);
                    cfg.counters
                        .nonce_local_compute_miss
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    if let Some(att) = attempts.remove(&key) {
        if let Some(reg) = &att.register {
            sig_to_key.remove(&reg.signature);
        }
        let send_to_observed_us = match (&att.send, &att.matched) {
            (Some(s), Some(m)) => Some(
                m.observed_at
                    .saturating_duration_since(s.outcome.send_at)
                    .as_micros() as u64,
            ),
            _ => None,
        };
        let tid = att.register.as_ref().map(|r| r.trigger_id);
        let sid = att.register.as_ref().map(|r| r.sender_id);
        emit_record(att, FinalOutcome::Landed, cfg, file);
        if let (Some(t), Some(s)) = (tid, sid) {
            agg.record_outcome(t, s, FinalOutcome::Landed, send_to_observed_us);
        }
    }
}

fn sweep_deadlines(
    attempts: &mut HashMap<(TriggerId, u8), TriggerAttempt>,
    sig_to_key: &mut HashMap<Signature, (TriggerId, u8)>,
    cfg: &RecorderConfig,
    file: &mut std::fs::File,
    agg: &mut AggregateState,
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
            let tid = att.register.as_ref().map(|r| r.trigger_id);
            let sid = att.register.as_ref().map(|r| r.sender_id);
            emit_record(att, FinalOutcome::UnknownPending, cfg, file);
            if let (Some(t), Some(s)) = (tid, sid) {
                agg.record_outcome(t, s, FinalOutcome::UnknownPending, None);
            }
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

