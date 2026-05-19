//! Matcher — single-owner state machine per (TriggerId, sender_id).
//!
//! See spec §7.2. Receives SendEvent (transport outcome) + MatchEvent (on-chain
//! observation), maintains AttemptState, emits FinalRecord rows when terminal.

use crate::attempt_state::AttemptState;
use crate::counters::BenchCounters;
use crate::finality_tracker::FinalityQueueEntry;
use crate::match_event::MatchEvent;
use crate::outcome::{FinalStatus, ObservedSource, RateLimitState, TentativeOutcome};
use crate::trigger_id::TriggerId;
use crate::writer::record::FinalRecord;
use crossbeam_channel::{Receiver, Sender};
use dashmap::DashSet;
use entry_sources::SourceKind;
use solana_sdk::{hash::Hash, pubkey::Pubkey, signature::Signature};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct SendEvent {
    pub trigger_id: TriggerId,
    pub sender_id: u8,
    pub send_at: Instant,
    pub send_ack_at: Option<Instant>,
    pub signature: Signature,
    pub provider_request_id: Option<String>,
    pub http_status: Option<u16>,
    pub rpc_err_code: Option<i32>,
    pub rpc_err_message: Option<String>,
    pub rate_limit_state: RateLimitState,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RegisterEvent {
    pub trigger_id: TriggerId,
    pub sender_id: u8,
    pub sender_name: String,
    pub endpoint_url: String,
    pub protocol: String,
    pub auth_tier: Option<String>,
    pub tip_account_used: Option<Pubkey>,
    pub tip_lamports: u64,
    pub priority_fee_microlamports: u64,
    pub compute_unit_limit: u32,
    pub signature: Signature,
    pub tx_message_hash: [u8; 32],
    pub send_order_in_trigger: u8,
    pub trigger_slot: u64,
    pub trigger_tick: u8,
    pub nonce_account_id: u16,
    pub nonce_blockhash_used: Hash,
    pub prepared_at: Instant,
    pub pool_ready_at: Instant,
    pub trigger_observed_at: Instant,
}

struct AttemptRecord {
    reg: RegisterEvent,
    state: AttemptState,
    match_info: Option<MatchInfo>,
}

#[derive(Debug, Clone)]
struct MatchInfo {
    observed_at: Instant,
    observed_slot: u64,
    observed_entry_index: u32,
    observed_tick_in_slot: Option<u8>,
    observed_cumulative_hashes_in_slot: Option<u64>,
    observed_source: SourceKind,
}

pub struct MatcherConfig {
    pub register_rx: Receiver<RegisterEvent>,
    pub send_event_rx: Receiver<SendEvent>,
    pub match_event_rx: Receiver<MatchEvent>,
    pub final_tx: Sender<FinalRecord>,
    pub finality_tx: Option<Sender<FinalityQueueEntry>>,
    pub pending_sigs: Arc<DashSet<Signature>>,
    pub deadline: Duration,
    pub run_id: String,
    pub anchor: Instant,
    pub pinned_core: Option<usize>,
    pub counters: Arc<BenchCounters>,
    pub stop: Arc<AtomicBool>,
}

pub fn spawn(cfg: MatcherConfig) -> std::io::Result<JoinHandle<()>> {
    std::thread::Builder::new()
        .name("matcher".into())
        .spawn(move || {
            if let Some(core) = cfg.pinned_core {
                core_affinity::set_for_current(core_affinity::CoreId { id: core });
            }
            run_loop(cfg);
        })
}

fn run_loop(cfg: MatcherConfig) {
    let mut attempts: HashMap<(TriggerId, u8), AttemptRecord> = HashMap::with_capacity(1024);
    let mut sig_to_key: HashMap<Signature, (TriggerId, u8)> = HashMap::with_capacity(1024);
    let mut last_deadline_sweep = Instant::now();

    loop {
        if cfg.stop.load(Ordering::Relaxed) {
            break;
        }
        crossbeam_channel::select! {
            recv(cfg.register_rx) -> msg => {
                if let Ok(reg) = msg {
                    register_attempt(&mut attempts, &mut sig_to_key, reg);
                }
            },
            recv(cfg.send_event_rx) -> msg => {
                if let Ok(ev) = msg {
                    handle_send_event(&mut attempts, &mut sig_to_key, ev, &cfg);
                }
            },
            recv(cfg.match_event_rx) -> msg => {
                if let Ok(ev) = msg {
                    handle_match_event(&mut attempts, &sig_to_key, ev, &cfg);
                }
            },
            default(Duration::from_millis(200)) => {}
        }
        if last_deadline_sweep.elapsed() >= Duration::from_millis(500) {
            last_deadline_sweep = Instant::now();
            sweep_deadlines(&mut attempts, &mut sig_to_key, &cfg);
        }
    }
    sweep_deadlines(&mut attempts, &mut sig_to_key, &cfg);
}

fn register_attempt(
    attempts: &mut HashMap<(TriggerId, u8), AttemptRecord>,
    sig_to_key: &mut HashMap<Signature, (TriggerId, u8)>,
    reg: RegisterEvent,
) {
    let key = (reg.trigger_id, reg.sender_id);
    sig_to_key.insert(reg.signature, key);
    let state = AttemptState::SentPending {
        send_at_ns: 0,
        sig: reg.signature,
    };
    attempts.insert(key, AttemptRecord { reg, state, match_info: None });
}

fn handle_send_event(
    attempts: &mut HashMap<(TriggerId, u8), AttemptRecord>,
    sig_to_key: &mut HashMap<Signature, (TriggerId, u8)>,
    ev: SendEvent,
    cfg: &MatcherConfig,
) {
    let key = (ev.trigger_id, ev.sender_id);
    let Some(rec) = attempts.get_mut(&key) else { return };
    let send_at_ns = ns_since(ev.send_at, cfg.anchor);
    let send_ack_at_ns = ev.send_ack_at.map(|t| ns_since(t, cfg.anchor));
    if let Some(err) = &ev.error {
        rec.state = AttemptState::SendFailed {
            send_at_ns,
            send_ack_at_ns,
            error: err.clone(),
            sig: ev.signature,
        };
        let record = build_record_from_attempt(rec, &ev, cfg, TentativeOutcome::SendError);
        if cfg.final_tx.try_send(record).is_err() {
            cfg.counters.final_queue_full.fetch_add(1, Ordering::Relaxed);
        }
        sig_to_key.remove(&rec.reg.signature);
        attempts.remove(&key);
    } else {
        rec.state = AttemptState::SentAcked {
            send_at_ns,
            send_ack_at_ns: send_ack_at_ns.unwrap_or(0),
            sig: ev.signature,
            provider_request_id: ev.provider_request_id.clone(),
        };
    }
}

fn handle_match_event(
    attempts: &mut HashMap<(TriggerId, u8), AttemptRecord>,
    sig_to_key: &HashMap<Signature, (TriggerId, u8)>,
    ev: MatchEvent,
    cfg: &MatcherConfig,
) {
    let Some(&winner_key) = sig_to_key.get(&ev.signature) else { return };
    let (winner_trigger_id, _) = winner_key;

    let sibling_keys: Vec<(TriggerId, u8)> = attempts
        .keys()
        .filter(|(tid, _)| *tid == winner_trigger_id)
        .copied()
        .collect();

    let match_info = MatchInfo {
        observed_at: ev.observed_at,
        observed_slot: ev.observed_slot,
        observed_entry_index: ev.observed_entry_index,
        observed_tick_in_slot: ev.observed_tick_in_slot,
        observed_cumulative_hashes_in_slot: ev.observed_cumulative_hashes_in_slot,
        observed_source: ev.observed_source,
    };

    let mut to_remove = Vec::new();
    for key in sibling_keys {
        let Some(rec) = attempts.get_mut(&key) else { continue };
        let outcome = if key == winner_key {
            rec.match_info = Some(match_info.clone());
            TentativeOutcome::LandedTentative
        } else {
            rec.match_info = Some(match_info.clone());
            TentativeOutcome::DedupedTentative
        };
        let record = build_record_from_record(rec, cfg, outcome, Some(ev.observed_at));
        if cfg.final_tx.try_send(record).is_err() {
            cfg.counters.final_queue_full.fetch_add(1, Ordering::Relaxed);
        }
        if let Some(ftx) = &cfg.finality_tx {
            let _ = ftx.send(FinalityQueueEntry {
                trigger_id: rec.reg.trigger_id,
                sender_id: rec.reg.sender_id,
                signature: rec.reg.signature,
                queued_at: Instant::now(),
            });
        }
        to_remove.push(key);
    }
    for key in to_remove {
        attempts.remove(&key);
    }
}

fn sweep_deadlines(
    attempts: &mut HashMap<(TriggerId, u8), AttemptRecord>,
    sig_to_key: &mut HashMap<Signature, (TriggerId, u8)>,
    cfg: &MatcherConfig,
) {
    let now = Instant::now();
    let mut to_emit: Vec<(TriggerId, u8)> = Vec::new();
    for (key, rec) in attempts.iter() {
        let elapsed = now.duration_since(rec.reg.trigger_observed_at);
        if elapsed >= cfg.deadline {
            to_emit.push(*key);
        }
    }
    for key in to_emit {
        if let Some(rec) = attempts.remove(&key) {
            sig_to_key.remove(&rec.reg.signature);
            let record = build_record_from_record(&rec, cfg, TentativeOutcome::UnknownPending, None);
            if cfg.final_tx.try_send(record).is_err() {
                cfg.counters.final_queue_full.fetch_add(1, Ordering::Relaxed);
            }
            if let Some(ftx) = &cfg.finality_tx {
                let _ = ftx.send(FinalityQueueEntry {
                    trigger_id: rec.reg.trigger_id,
                    sender_id: rec.reg.sender_id,
                    signature: rec.reg.signature,
                    queued_at: Instant::now(),
                });
            }
        }
    }
}

fn ns_since(t: Instant, anchor: Instant) -> u64 {
    t.duration_since(anchor).as_nanos().min(u64::MAX as u128) as u64
}

fn build_record_from_attempt(
    rec: &AttemptRecord,
    send_ev: &SendEvent,
    cfg: &MatcherConfig,
    outcome: TentativeOutcome,
) -> FinalRecord {
    FinalRecord {
        trigger_slot: rec.reg.trigger_slot,
        trigger_tick: rec.reg.trigger_tick,
        trigger_id: *rec.reg.trigger_id.as_bytes(),
        nonce_account_id: rec.reg.nonce_account_id,
        nonce_blockhash_used: rec.reg.nonce_blockhash_used,
        sender_id: rec.reg.sender_id,
        sender_name: rec.reg.sender_name.clone(),
        tx_signature: rec.reg.signature,
        tx_message_hash: rec.reg.tx_message_hash,
        endpoint_url: rec.reg.endpoint_url.clone(),
        protocol: rec.reg.protocol.clone(),
        auth_tier: rec.reg.auth_tier.clone(),
        tip_account_used: rec.reg.tip_account_used,
        tip_lamports: rec.reg.tip_lamports,
        priority_fee_microlamports: rec.reg.priority_fee_microlamports,
        compute_unit_limit: rec.reg.compute_unit_limit,
        prepared_at_ns: ns_since(rec.reg.prepared_at, cfg.anchor),
        pool_ready_at_ns: ns_since(rec.reg.pool_ready_at, cfg.anchor),
        trigger_observed_at_ns: ns_since(rec.reg.trigger_observed_at, cfg.anchor),
        send_at_ns: ns_since(send_ev.send_at, cfg.anchor),
        send_ack_at_ns: send_ev.send_ack_at.map(|t| ns_since(t, cfg.anchor)),
        send_order_in_trigger: rec.reg.send_order_in_trigger,
        host_clock_offset_ns: None,
        send_error: send_ev.error.clone(),
        rpc_err_code: send_ev.rpc_err_code,
        rpc_err_message: send_ev.rpc_err_message.clone(),
        provider_request_id: send_ev.provider_request_id.clone(),
        http_status: send_ev.http_status,
        rate_limit_state: send_ev.rate_limit_state,
        observed_slot: None,
        observed_entry_index: None,
        observed_tick_in_slot: None,
        observed_cumulative_hashes_in_slot: None,
        ss_observed_at_ns: None,
        ys_observed_at_ns: None,
        observed_at_ns: None,
        observed_source: None,
        commitment_at_resolution: None,
        tentative_outcome: outcome,
        final_status: FinalStatus::Pending,
        siblings_resolved_at_ns: None,
        leader_pubkey: None,
        leader_region_cc: None,
        leader_dc_label: None,
        leader_continent: None,
        leader_stake_lamports: None,
        validator_client: None,
        tick_delta: None,
        hash_delta: None,
        slot_delta: None,
        leader_changed: false,
        wall_trigger_to_send_ns: Some((ns_since(send_ev.send_at, cfg.anchor) as i64) - (ns_since(rec.reg.trigger_observed_at, cfg.anchor) as i64)),
        wall_send_rtt_ns: send_ev.send_ack_at.map(|t| t.duration_since(send_ev.send_at).as_nanos() as i64),
        wall_send_to_observed_ns: None,
        wall_send_to_ss_observed_ns: None,
        wall_send_to_ys_observed_ns: None,
        nonce_update_observed_at_ns: None,
        nonce_update_source: None,
        nonce_advanced_to_slot: None,
        run_id: cfg.run_id.clone(),
        chunk_index: 0,
    }
}

fn build_record_from_record(
    rec: &AttemptRecord,
    cfg: &MatcherConfig,
    outcome: TentativeOutcome,
    siblings_resolved_at: Option<Instant>,
) -> FinalRecord {
    let (send_at_ns, send_ack_at_ns, send_error, rpc_err_code, rpc_err_message, provider_request_id, http_status, rate_limit_state) =
        match &rec.state {
            AttemptState::SentAcked { send_at_ns, send_ack_at_ns, provider_request_id, .. } => (
                *send_at_ns, Some(*send_ack_at_ns), None, None, None, provider_request_id.clone(), None, RateLimitState::Ok,
            ),
            AttemptState::SentPending { send_at_ns, .. } => (
                *send_at_ns, None, None, None, None, None, None, RateLimitState::Ok,
            ),
            AttemptState::SendFailed { send_at_ns, send_ack_at_ns, error, .. } => (
                *send_at_ns, *send_ack_at_ns, Some(error.clone()), None, None, None, None, RateLimitState::Ok,
            ),
            _ => (0, None, None, None, None, None, None, RateLimitState::Ok),
        };

    let (observed_slot, observed_entry_index, observed_tick_in_slot, observed_cumulative_hashes, observed_at_ns, observed_source) =
        if let Some(mi) = &rec.match_info {
            (Some(mi.observed_slot), Some(mi.observed_entry_index), mi.observed_tick_in_slot,
             mi.observed_cumulative_hashes_in_slot, Some(ns_since(mi.observed_at, cfg.anchor)),
             Some(match mi.observed_source {
                 SourceKind::ShredStream => ObservedSource::Ss,
                 SourceKind::Yellowstone => ObservedSource::Ys,
             }))
        } else {
            (None, None, None, None, None, None)
        };

    FinalRecord {
        trigger_slot: rec.reg.trigger_slot,
        trigger_tick: rec.reg.trigger_tick,
        trigger_id: *rec.reg.trigger_id.as_bytes(),
        nonce_account_id: rec.reg.nonce_account_id,
        nonce_blockhash_used: rec.reg.nonce_blockhash_used,
        sender_id: rec.reg.sender_id,
        sender_name: rec.reg.sender_name.clone(),
        tx_signature: rec.reg.signature,
        tx_message_hash: rec.reg.tx_message_hash,
        endpoint_url: rec.reg.endpoint_url.clone(),
        protocol: rec.reg.protocol.clone(),
        auth_tier: rec.reg.auth_tier.clone(),
        tip_account_used: rec.reg.tip_account_used,
        tip_lamports: rec.reg.tip_lamports,
        priority_fee_microlamports: rec.reg.priority_fee_microlamports,
        compute_unit_limit: rec.reg.compute_unit_limit,
        prepared_at_ns: ns_since(rec.reg.prepared_at, cfg.anchor),
        pool_ready_at_ns: ns_since(rec.reg.pool_ready_at, cfg.anchor),
        trigger_observed_at_ns: ns_since(rec.reg.trigger_observed_at, cfg.anchor),
        send_at_ns,
        send_ack_at_ns,
        send_order_in_trigger: rec.reg.send_order_in_trigger,
        host_clock_offset_ns: None,
        send_error,
        rpc_err_code,
        rpc_err_message,
        provider_request_id,
        http_status,
        rate_limit_state,
        observed_slot,
        observed_entry_index,
        observed_tick_in_slot,
        observed_cumulative_hashes_in_slot: observed_cumulative_hashes,
        ss_observed_at_ns: None,
        ys_observed_at_ns: None,
        observed_at_ns,
        observed_source,
        commitment_at_resolution: None,
        tentative_outcome: outcome,
        final_status: FinalStatus::Pending,
        siblings_resolved_at_ns: siblings_resolved_at.map(|t| ns_since(t, cfg.anchor)),
        leader_pubkey: None,
        leader_region_cc: None,
        leader_dc_label: None,
        leader_continent: None,
        leader_stake_lamports: None,
        validator_client: None,
        tick_delta: None,
        hash_delta: None,
        slot_delta: None,
        leader_changed: false,
        wall_trigger_to_send_ns: None,
        wall_send_rtt_ns: None,
        wall_send_to_observed_ns: None,
        wall_send_to_ss_observed_ns: None,
        wall_send_to_ys_observed_ns: None,
        nonce_update_observed_at_ns: None,
        nonce_update_source: None,
        nonce_advanced_to_slot: None,
        run_id: cfg.run_id.clone(),
        chunk_index: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_channel::{bounded, unbounded};

    fn make_register(trigger_id: TriggerId, sender_id: u8, sig: Signature, anchor: Instant) -> RegisterEvent {
        RegisterEvent {
            trigger_id, sender_id,
            sender_name: format!("s{}", sender_id),
            endpoint_url: "http://mock".into(),
            protocol: "MOCK".into(),
            auth_tier: None,
            tip_account_used: None,
            tip_lamports: 1000,
            priority_fee_microlamports: 5000,
            compute_unit_limit: 200_000,
            signature: sig,
            tx_message_hash: [0; 32],
            send_order_in_trigger: 0,
            trigger_slot: 100, trigger_tick: 5,
            nonce_account_id: 0,
            nonce_blockhash_used: Hash::default(),
            prepared_at: anchor,
            pool_ready_at: anchor,
            trigger_observed_at: anchor,
        }
    }

    #[allow(clippy::type_complexity)]
    fn setup() -> (
        crossbeam_channel::Sender<RegisterEvent>,
        crossbeam_channel::Sender<SendEvent>,
        crossbeam_channel::Sender<MatchEvent>,
        crossbeam_channel::Receiver<FinalRecord>,
        Arc<AtomicBool>,
        JoinHandle<()>,
        Instant,
    ) {
        let (reg_tx, reg_rx) = unbounded();
        let (send_tx, send_rx) = unbounded();
        let (match_tx, match_rx) = unbounded();
        let (final_tx, final_rx) = bounded(100);
        let stop = Arc::new(AtomicBool::new(false));
        let anchor = Instant::now();
        let handle = spawn(MatcherConfig {
            register_rx: reg_rx,
            send_event_rx: send_rx,
            match_event_rx: match_rx,
            final_tx,
            finality_tx: None,
            pending_sigs: Arc::new(DashSet::new()),
            deadline: Duration::from_millis(200),
            run_id: "test".into(),
            anchor,
            pinned_core: None,
            counters: Arc::new(BenchCounters::default()),
            stop: stop.clone(),
        }).unwrap();
        (reg_tx, send_tx, match_tx, final_rx, stop, handle, anchor)
    }

    fn shutdown(reg_tx: crossbeam_channel::Sender<RegisterEvent>, send_tx: crossbeam_channel::Sender<SendEvent>, match_tx: crossbeam_channel::Sender<MatchEvent>, stop: Arc<AtomicBool>, handle: JoinHandle<()>) {
        std::thread::sleep(Duration::from_millis(50));
        stop.store(true, Ordering::Relaxed);
        drop(reg_tx); drop(send_tx); drop(match_tx);
        let _ = handle.join();
    }

    #[test]
    fn winner_and_siblings_emit_correct_outcomes() {
        let (reg_tx, send_tx, match_tx, final_rx, stop, handle, anchor) = setup();
        let tid = TriggerId::new(100, 5, 0);
        let sig_a = Signature::new_unique();
        let sig_b = Signature::new_unique();
        let sig_c = Signature::new_unique();

        reg_tx.send(make_register(tid, 0, sig_a, anchor)).unwrap();
        reg_tx.send(make_register(tid, 1, sig_b, anchor)).unwrap();
        reg_tx.send(make_register(tid, 2, sig_c, anchor)).unwrap();

        std::thread::sleep(Duration::from_millis(10));

        match_tx.send(MatchEvent {
            signature: sig_b,
            observed_at: Instant::now(),
            observed_slot: 100,
            observed_entry_index: 0,
            observed_tick_in_slot: Some(5),
            observed_cumulative_hashes_in_slot: Some(312_500),
            observed_source: SourceKind::ShredStream,
        }).unwrap();

        std::thread::sleep(Duration::from_millis(50));

        let mut records = Vec::new();
        while let Ok(r) = final_rx.try_recv() {
            records.push(r);
        }
        assert_eq!(records.len(), 3);
        let landed: Vec<_> = records.iter().filter(|r| r.tentative_outcome == TentativeOutcome::LandedTentative).collect();
        let deduped: Vec<_> = records.iter().filter(|r| r.tentative_outcome == TentativeOutcome::DedupedTentative).collect();
        assert_eq!(landed.len(), 1);
        assert_eq!(deduped.len(), 2);
        assert_eq!(landed[0].sender_id, 1);

        shutdown(reg_tx, send_tx, match_tx, stop, handle);
    }

    #[test]
    fn send_error_emits_immediately() {
        let (reg_tx, send_tx, match_tx, final_rx, stop, handle, anchor) = setup();
        let tid = TriggerId::new(100, 5, 0);
        let sig_a = Signature::new_unique();

        reg_tx.send(make_register(tid, 0, sig_a, anchor)).unwrap();
        std::thread::sleep(Duration::from_millis(10));
        send_tx.send(SendEvent {
            trigger_id: tid, sender_id: 0,
            send_at: Instant::now(),
            send_ack_at: None,
            signature: sig_a,
            provider_request_id: None,
            http_status: Some(500),
            rpc_err_code: None,
            rpc_err_message: None,
            rate_limit_state: RateLimitState::Ok,
            error: Some("boom".into()),
        }).unwrap();
        std::thread::sleep(Duration::from_millis(30));

        let rec = final_rx.try_recv().unwrap();
        assert_eq!(rec.tentative_outcome, TentativeOutcome::SendError);
        assert_eq!(rec.send_error.as_deref(), Some("boom"));

        shutdown(reg_tx, send_tx, match_tx, stop, handle);
    }

    #[test]
    fn deadline_triggers_unknown_pending() {
        let (reg_tx, send_tx, match_tx, final_rx, stop, handle, anchor) = setup();
        let tid = TriggerId::new(100, 5, 0);
        let sig_a = Signature::new_unique();

        reg_tx.send(make_register(tid, 0, sig_a, anchor)).unwrap();
        std::thread::sleep(Duration::from_millis(800));

        let rec = final_rx.try_recv().expect("expected deadline emission");
        assert_eq!(rec.tentative_outcome, TentativeOutcome::UnknownPending);

        shutdown(reg_tx, send_tx, match_tx, stop, handle);
    }
}
