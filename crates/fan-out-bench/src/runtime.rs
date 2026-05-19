//! Runtime — wires all components for full real-chain run.

use crate::budget_watcher::{spawn as spawn_budget, BudgetWatcherConfig};
use crate::config::Config;
use crate::counters::BenchCounters;
use crate::dispatcher::{DispatcherConfig, SenderMeta};
use crate::finality_tracker::{spawn as spawn_finality, FinalityQueueEntry, FinalityTrackerConfig};
use crate::matcher::{MatcherConfig, RegisterEvent, SendEvent};
use crate::merger::{spawn as spawn_merger, MergerConfig};
use crate::nonce::manager::NonceManager;
use crate::observer::{spawn as spawn_observer, ObserverConfig};
use crate::pool::TxPool;
use crate::preparer::{spawn as spawn_preparer, PreparerConfig};
use crate::schedule::{Schedule, ScheduleEntry};
use crate::schedule_pump::{spawn as spawn_pump, PumpConfig};
use crate::senders::TxSender;
use crate::tip_accounts::{tip_accounts_for, TipAccountRotator};
use crate::writer::{spawn_parquet, FinalRecord, ParquetWriterConfig};
use arc_swap::ArcSwap;
use crossbeam_channel::{bounded, unbounded};
use dashmap::DashSet;
use entry_sources::EntryObservation;
use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signature};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub struct RuntimeInputs {
    pub config: Config,
    pub authority: Arc<Keypair>,
    pub authority_pubkey: Pubkey,
    pub nonce_manager: Arc<NonceManager>,
    pub ss_entry_rx: crossbeam_channel::Receiver<EntryObservation>,
    pub ys_entry_rx: crossbeam_channel::Receiver<EntryObservation>,
    pub senders: HashMap<u8, Arc<dyn TxSender>>,
    pub output_dir: PathBuf,
    pub run_id: String,
    pub rpc: Arc<RpcClient>,
    pub start_slot: u64,
}

pub struct RuntimeHandles {
    pub stop: Arc<AtomicBool>,
    pub counters: Arc<BenchCounters>,
}

pub fn start(inputs: RuntimeInputs) -> anyhow::Result<RuntimeHandles> {
    let stop = Arc::new(AtomicBool::new(false));
    let counters = Arc::new(BenchCounters::default());
    let anchor = Instant::now();

    let pool = Arc::new(TxPool::new());
    let pending_sigs: Arc<DashSet<Signature>> = Arc::new(DashSet::new());

    let (merged_tx, merged_rx) = bounded(65536);
    let (trigger_tx, trigger_rx) = bounded(65536);
    let (match_tx, match_event_rx) = bounded(65536);
    let (schedule_tx, schedule_rx) = unbounded::<ScheduleEntry>();
    let (register_tx, register_rx) = unbounded::<RegisterEvent>();
    let (send_event_tx, send_event_rx) = unbounded::<SendEvent>();
    let (final_tx, final_rx) = bounded::<FinalRecord>(65536);
    let (finality_tx, finality_rx) = bounded::<FinalityQueueEntry>(65536);

    let current_slot = Arc::new(AtomicU64::new(inputs.start_slot));

    // Schedule pump
    let schedule = Schedule::new(
        inputs.config.run.schedule_seed,
        inputs.start_slot,
        inputs.config.run.chunk_size_slots,
    );
    let _pump_handle = spawn_pump(PumpConfig {
        schedule,
        schedule_tx,
        current_slot: current_slot.clone(),
        lead_slots: 100,
        pinned_core: None,
        counters: counters.clone(),
        stop: stop.clone(),
    })?;

    // Merger
    let _merger_handle = spawn_merger(MergerConfig {
        ss_rx: inputs.ss_entry_rx,
        ys_rx: inputs.ys_entry_rx,
        out_tx: merged_tx,
        pinned_core: None,
        counters: counters.clone(),
        stop: stop.clone(),
    })?;

    // Observer schedule — ArcSwap so we can update it live
    let observer_schedule: Arc<ArcSwap<HashSet<(u64, u8)>>> =
        Arc::new(ArcSwap::from_pointee(HashSet::new()));

    // Schedule bridge: receives from schedule_rx, updates observer_schedule, forwards to preparer
    let (preparer_schedule_tx, preparer_schedule_rx) = unbounded::<ScheduleEntry>();
    {
        let observer_schedule = observer_schedule.clone();
        let stop_bridge = stop.clone();
        std::thread::Builder::new()
            .name("schedule-bridge".into())
            .spawn(move || {
                let mut accumulated: HashSet<(u64, u8)> = HashSet::new();
                let mut last_swap = Instant::now();
                while !stop_bridge.load(std::sync::atomic::Ordering::Relaxed) {
                    match schedule_rx.recv_timeout(Duration::from_millis(200)) {
                        Ok(entry) => {
                            accumulated.insert((entry.slot, entry.tick));
                            let _ = preparer_schedule_tx.send(entry);
                            // Swap snapshot every 500ms or every 100 entries
                            if last_swap.elapsed() >= Duration::from_millis(500) {
                                observer_schedule.store(Arc::new(accumulated.clone()));
                                last_swap = Instant::now();
                            }
                        }
                        Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                            if last_swap.elapsed() >= Duration::from_secs(2) {
                                observer_schedule.store(Arc::new(accumulated.clone()));
                                last_swap = Instant::now();
                            }
                        }
                        Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
                    }
                }
            })?;
    }

    let _observer_handle = spawn_observer(ObserverConfig {
        merged_rx,
        schedule: observer_schedule,
        trigger_tx,
        match_tx,
        pending_sigs: pending_sigs.clone(),
        current_slot: current_slot.clone(),
        pinned_core: None,
        counters: counters.clone(),
        stop: stop.clone(),
    })?;

    // Preparer
    let mut tip_rotators: HashMap<u8, Arc<TipAccountRotator>> = HashMap::new();
    let mut sender_meta: HashMap<u8, SenderMeta> = HashMap::new();
    for sc in &inputs.config.senders {
        let accounts = tip_accounts_for(sc.kind);
        tip_rotators.insert(sc.id, Arc::new(TipAccountRotator::new(accounts)));
        sender_meta.insert(sc.id, SenderMeta {
            name: sc.name.clone(),
            endpoint_url: sc.endpoint_url.clone(),
            protocol: "HTTP_JSONRPC".to_string(),
            auth_tier: None,
            tip_lamports: sc.tip_lamports,
            priority_fee_microlamports: inputs.config.run.priority_fee_microlamports,
            compute_unit_limit: inputs.config.run.compute_unit_limit,
        });
    }
    let (_preparer_handle, prepared_rx) = spawn_preparer(PreparerConfig {
        schedule_rx: preparer_schedule_rx,
        senders: inputs.config.senders.clone(),
        tip_rotators,
        nonce_manager: inputs.nonce_manager.clone(),
        pool: pool.clone(),
        authority: inputs.authority.clone(),
        priority_fee_microlamports: inputs.config.run.priority_fee_microlamports,
        compute_unit_limit: inputs.config.run.compute_unit_limit,
        pinned_core: None,
        counters: counters.clone(),
        stop: stop.clone(),
    });

    // Dispatcher
    let dispatcher_cfg = DispatcherConfig {
        trigger_rx,
        prepared_rx,
        pool: pool.clone(),
        senders: inputs.senders,
        sender_meta,
        register_tx,
        send_event_tx,
        pending_sigs: pending_sigs.clone(),
        schedule_seed: inputs.config.run.schedule_seed.unwrap_or(0),
        counters: counters.clone(),
        stop: stop.clone(),
    };
    let dispatcher_stop = stop.clone();
    std::thread::Builder::new()
        .name("dispatcher".into())
        .spawn(move || {
            if let Err(e) = crate::dispatcher::run_blocking(dispatcher_cfg) {
                tracing::error!(error = %e, "dispatcher exited with error");
            }
            let _ = dispatcher_stop;
        })?;

    // Matcher
    let _matcher_handle = crate::matcher::spawn(MatcherConfig {
        register_rx,
        send_event_rx,
        match_event_rx,
        final_tx,
        pending_sigs,
        deadline: Duration::from_secs(inputs.config.run.observation_deadline_secs),
        run_id: inputs.run_id.clone(),
        anchor,
        pinned_core: None,
        counters: counters.clone(),
        stop: stop.clone(),
        finality_tx: Some(finality_tx.clone()),
    })?;

    // Parquet
    let parquet_path = inputs.output_dir.join("tx-events.parquet");
    let _parquet_handle = spawn_parquet(ParquetWriterConfig {
        final_rx,
        output_path: parquet_path,
        row_group_size: 32768,
        flush_interval: Duration::from_secs(60),
        pinned_core: None,
        counters: counters.clone(),
    })?;

    // Finality tracker
    let finality_path = inputs.output_dir.join("finality-updates.jsonl");
    let _finality_handle = spawn_finality(FinalityTrackerConfig {
        finality_rx,
        rpc: inputs.rpc.clone(),
        output_path: finality_path,
        poll_interval: Duration::from_secs(30),
        anchor,
        pinned_core: None,
        counters: counters.clone(),
        stop: stop.clone(),
    })?;

    // Budget watcher
    let nonce_rent_reserve = (inputs.nonce_manager.len() as u64) * 1_447_680;
    let _budget_handle = spawn_budget(BudgetWatcherConfig {
        rpc: inputs.rpc.clone(),
        wallet_pubkey: inputs.authority_pubkey,
        min_balance_lamports: inputs.config.run.min_balance_lamports,
        nonce_rent_reserve_lamports: nonce_rent_reserve,
        poll_interval: Duration::from_secs(20),
        pinned_core: None,
        counters: counters.clone(),
        stop: stop.clone(),
    })?;

    Ok(RuntimeHandles {
        stop,
        counters,
    })
}
