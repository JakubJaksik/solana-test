//! Runtime — wires schedule → preparer → observer → dispatcher → matcher → parquet.

use crate::config::Config;
use crate::counters::BenchCounters;
use crate::dispatcher::{DispatcherConfig, SenderMeta};
use crate::matcher::{MatcherConfig, RegisterEvent, SendEvent};
use crate::merger::{spawn as spawn_merger, MergerConfig};
use crate::nonce::manager::NonceManager;
use crate::observer::{spawn as spawn_observer, ObserverConfig};
use crate::pool::TxPool;
use crate::preparer::{spawn as spawn_preparer, PreparerConfig};
use crate::schedule::ScheduleEntry;
use crate::senders::TxSender;
use crate::tip_accounts::{tip_accounts_for, TipAccountRotator};
use crate::writer::{spawn_parquet, FinalRecord, ParquetWriterConfig};
use crossbeam_channel::{bounded, unbounded};
use dashmap::DashSet;
use entry_sources::EntryObservation;
use solana_sdk::signature::{Keypair, Signature};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub struct RuntimeInputs {
    pub config: Config,
    pub authority: Arc<Keypair>,
    pub nonce_manager: Arc<NonceManager>,
    pub ss_entry_rx: crossbeam_channel::Receiver<EntryObservation>,
    pub ys_entry_rx: crossbeam_channel::Receiver<EntryObservation>,
    pub senders: HashMap<u8, Arc<dyn TxSender>>,
    pub output_dir: PathBuf,
    pub run_id: String,
}

pub struct RuntimeHandles {
    pub stop: Arc<AtomicBool>,
    pub schedule_tx: crossbeam_channel::Sender<ScheduleEntry>,
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

    let _merger_handle = spawn_merger(MergerConfig {
        ss_rx: inputs.ss_entry_rx,
        ys_rx: inputs.ys_entry_rx,
        out_tx: merged_tx,
        pinned_core: None,
        counters: counters.clone(),
        stop: stop.clone(),
    })?;

    let schedule_set: Arc<HashSet<(u64, u8)>> = Arc::new(HashSet::new());
    let _observer_handle = spawn_observer(ObserverConfig {
        merged_rx,
        schedule: schedule_set,
        trigger_tx,
        match_tx,
        pending_sigs: pending_sigs.clone(),
        current_slot: Arc::new(AtomicU64::new(0)),
        pinned_core: None,
        counters: counters.clone(),
        stop: stop.clone(),
    })?;

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
        schedule_rx,
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
    })?;

    let parquet_path = inputs.output_dir.join("tx-events.parquet");
    let _parquet_handle = spawn_parquet(ParquetWriterConfig {
        final_rx,
        output_path: parquet_path,
        row_group_size: 32768,
        flush_interval: Duration::from_secs(60),
        pinned_core: None,
        counters: counters.clone(),
    })?;

    Ok(RuntimeHandles {
        stop,
        schedule_tx,
        counters,
    })
}
