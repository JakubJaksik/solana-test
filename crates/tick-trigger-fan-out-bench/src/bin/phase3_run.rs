// Phase 3 binary — end-to-end trigger → 1 sender → on-chain observe.
//
// Pipeline:
//   SS gRPC ──▶ ss_warmup ──┐
//                            ├─▶ merger ──▶ supervisor ──▶ OrderedEvent
//   YS gRPC ──▶ ys_warmup ──┘                                  │
//                                                              ▼
//                            trigger_engine (schedule check + sig match)
//                                  │                  │
//                              TriggerEvent       MatchEvent
//                                  │                  │
//                                  ▼                  │
//                    dispatcher (build tx + send)     │
//                          │              │           │
//                      RegisterEvent  SendEvent       │
//                          ▼              ▼           ▼
//                            recorder (JSONL per-trigger lifecycle)
//
// Hot path: trigger_engine reads OrderedEvent, does O(1) schedule lookup
// + DashSet sig lookup, emits Trigger/Match events. Send dispatcher and
// recorder run on their own threads, off the critical latency path.

use anyhow::Context;
use arc_swap::ArcSwap;
use clap::Parser;
use crossbeam_channel::{bounded, Receiver, Sender};
use dashmap::DashSet;
use entry_sources::shredstream::grpc::ShredStreamGrpcSource;
use entry_sources::yellowstone::YellowstoneSource;
use entry_sources::{DropCounters, EntryObservation, EntrySource};
use solana_client::rpc_client::RpcClient;
use solana_commitment_config::CommitmentConfig;
use solana_sdk::signature::Signature;
use solana_sdk::signer::Signer;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tick_trigger_fan_out_bench::blockhash_cache;
use tick_trigger_fan_out_bench::config::{Config, SenderKind};
use tick_trigger_fan_out_bench::merger::{
    spawn as spawn_merger, MergedEntry, MergerConfig, MergerCounters,
};
use tick_trigger_fan_out_bench::nonce::{
    bootstrap as nonce_bootstrap,
    local_compute::SlotHashCache as NonceSlotHashCache,
    manager::NonceManager,
    rpc_fallback as nonce_rpc_fallback,
};
use tick_trigger_fan_out_bench::poh_supervisor::{
    spawn as spawn_supervisor, OrderedEvent, PohSupervisorConfig, PohSupervisorCounters,
};
use tick_trigger_fan_out_bench::preparer::{
    spawn as spawn_preparer, PreparerConfig, PreparerCounters,
};
use tick_trigger_fan_out_bench::recorder::{
    spawn as spawn_recorder, ActiveWindowSummary, RecorderConfig, RecorderCounters,
    RegisterEvent, SendEvent,
};
use tick_trigger_fan_out_bench::schedule::{Schedule, ScheduleEntry};
use tick_trigger_fan_out_bench::senders::{helius::HeliusSender, jito::JitoSender, TxSender};
use tick_trigger_fan_out_bench::trigger_engine::{
    spawn as spawn_engine, MatchEvent, TriggerEngineConfig, TriggerEngineCounters, TriggerEvent,
};
use tick_trigger_fan_out_bench::tx_pool::TxPool;
use tick_trigger_fan_out_bench::wallet;
use tokio::runtime::Builder as TokioRtBuilder;

#[derive(Parser)]
#[command(version, about = "Phase 3: e2e trigger → 1 sender → observe")]
struct Args {
    #[arg(long)]
    config: PathBuf,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cfg = Config::load(&args.config).context("load config")?;
    let duration: Duration =
        humantime::parse_duration(&cfg.run.duration).context("parse run.duration")?;
    let run_id = chrono::Utc::now().format("%Y%m%d-%H%M%S").to_string();
    let run_dir = cfg.run.output_dir.join(&run_id);
    std::fs::create_dir_all(&run_dir)?;
    let recorder_path = run_dir.join("triggers.jsonl");
    tracing::info!(?duration, run_id, path = %recorder_path.display(), "phase3_run starting");

    // Wallet + RPC + initial balance check.
    let keypair = Arc::new(wallet::load_keypair(&cfg.wallet.keypair_path)?);
    let rpc = Arc::new(RpcClient::new_with_commitment(
        cfg.rpc.url.clone(),
        CommitmentConfig::confirmed(),
    ));
    let start_balance = rpc.get_balance(&keypair.pubkey()).unwrap_or(0);
    tracing::info!(start_balance, "wallet balance at start (lamports)");
    if start_balance < cfg.run.min_balance_lamports {
        anyhow::bail!(
            "wallet balance {} below min_balance_lamports {}",
            start_balance,
            cfg.run.min_balance_lamports
        );
    }
    let start_slot = rpc.get_slot().context("rpc.get_slot")?;
    tracing::info!(start_slot, "current slot");

    // Sources.
    let stop = Arc::new(AtomicBool::new(false));
    let ss_drops = Arc::new(DropCounters::default());
    let ss_raw_rx = Box::new(ShredStreamGrpcSource {
        endpoint: cfg.sources.shredstream_grpc_url.clone(),
        channel_capacity: cfg.sources.channel_capacity,
        pinned_core: None,
        counters: ss_drops.clone(),
    })
    .start()
    .context("start shredstream source")?;
    let ys_drops = Arc::new(DropCounters::default());
    let ys_raw_rx = Box::new(YellowstoneSource {
        url: cfg.sources.yellowstone_grpc_url.clone(),
        token: if cfg.sources.yellowstone_auth_token.is_empty() {
            None
        } else {
            Some(cfg.sources.yellowstone_auth_token.clone())
        },
        channel_capacity: cfg.sources.channel_capacity,
        pinned_core: None,
        counters: ys_drops.clone(),
    })
    .start()
    .context("start yellowstone source")?;

    // Warmup gates.
    let (ss_clean_tx, ss_clean_rx) = bounded::<EntryObservation>(cfg.sources.channel_capacity);
    let ss_warmup_first = Arc::new(AtomicU64::new(0));
    let _ss_warmup = spawn_warmup("ss-warmup", ss_raw_rx, ss_clean_tx, ss_warmup_first.clone(), stop.clone());
    let (ys_clean_tx, ys_clean_rx) = bounded::<EntryObservation>(cfg.sources.channel_capacity);
    let ys_warmup_first = Arc::new(AtomicU64::new(0));
    let _ys_warmup = spawn_warmup("ys-warmup", ys_raw_rx, ys_clean_tx, ys_warmup_first.clone(), stop.clone());

    // Merger → Supervisor.
    let (merged_tx, merged_rx) = bounded::<MergedEntry>(cfg.sources.channel_capacity);
    let merger_counters = Arc::new(MergerCounters::new());
    let _merger = spawn_merger(MergerConfig {
        ss_rx: ss_clean_rx,
        ys_rx: ys_clean_rx,
        out_tx: merged_tx,
        counters: merger_counters.clone(),
        stop: stop.clone(),
    })?;
    let (ordered_tx, ordered_rx) = bounded::<OrderedEvent>(cfg.sources.channel_capacity);
    let supervisor_counters = Arc::new(PohSupervisorCounters::default());
    let _supervisor = spawn_supervisor(PohSupervisorConfig {
        merged_rx,
        out_tx: ordered_tx,
        entry_timeout: Duration::from_millis(cfg.supervisor.entry_timeout_ms),
        slot_seal_lag_slots: cfg.supervisor.slot_seal_lag_slots,
        max_pending_per_slot: 1024,
        tick_check_interval: Duration::from_millis(10),
        counters: supervisor_counters.clone(),
        stop: stop.clone(),
    })?;

    // Schedule — live ArcSwap snapshot for engine + channel feed for preparer.
    let schedule_snapshot: Arc<ArcSwap<HashSet<(u64, u8)>>> =
        Arc::new(ArcSwap::from_pointee(HashSet::new()));
    let pending_sigs: Arc<DashSet<Signature>> = Arc::new(DashSet::new());
    let (preparer_schedule_tx, preparer_schedule_rx) =
        bounded::<ScheduleEntry>(cfg.sources.channel_capacity);
    let schedule_pump_stop = stop.clone();
    let schedule_snapshot_for_pump = schedule_snapshot.clone();
    let schedule_seed = cfg.schedule.seed;
    let chunk_size = cfg.schedule.chunk_size_slots;
    let triggers_per_slot = cfg.run.triggers_per_slot;
    let lead_slots = cfg.schedule.lead_slots;
    std::thread::Builder::new()
        .name("schedule-pump".into())
        .spawn(move || {
            let mut sched = Schedule::new(schedule_seed, start_slot, chunk_size, triggers_per_slot);
            let mut accumulated: HashSet<(u64, u8)> = HashSet::new();
            // Pre-seed ~lead_slots ahead so engine + preparer have work to do.
            for _ in 0..((lead_slots / chunk_size).max(1) + 1) {
                for e in sched.next_chunk() {
                    accumulated.insert((e.slot, e.tick));
                    let _ = preparer_schedule_tx.try_send(e);
                }
            }
            schedule_snapshot_for_pump.store(Arc::new(accumulated.clone()));
            loop {
                if schedule_pump_stop.load(Ordering::Relaxed) {
                    break;
                }
                std::thread::sleep(Duration::from_secs(2));
                for e in sched.next_chunk() {
                    accumulated.insert((e.slot, e.tick));
                    let _ = preparer_schedule_tx.try_send(e);
                }
                schedule_snapshot_for_pump.store(Arc::new(accumulated.clone()));
            }
        })?;

    // Blockhash cache.
    let bh_runner = blockhash_cache::spawn(
        rpc.clone(),
        Duration::from_secs(cfg.rpc.blockhash_refresh_secs),
        stop.clone(),
    );

    // Live current_slot (used by preparer to evict pool entries for past slots).
    // The main loop polls RPC and updates this every ~5s.
    let current_slot = Arc::new(AtomicU64::new(start_slot));

    // Tx pool + Preparer (signs txs ahead of fire time so hot path has zero
    // signing cost — just a DashMap lookup).
    let tx_pool = Arc::new(TxPool::new());
    let preparer_counters = Arc::new(PreparerCounters::default());

    // Durable nonce setup (when enabled in config). MUST be initialised
    // BEFORE the trigger engine since the engine takes a clone of the
    // slot-hash cache to feed it from SlotComplete events.
    let (nonce_manager, nonce_slot_hash_cache): (
        Option<Arc<NonceManager>>,
        Option<Arc<NonceSlotHashCache>>,
    ) = if cfg.nonce.enabled {
        let nonces = nonce_bootstrap::bootstrap(
            &rpc,
            &cfg.nonce.config_path,
            &keypair.pubkey(),
        )
        .context("bootstrap nonce manager from nonce-config.json")?;
        if nonces.is_empty() {
            anyhow::bail!("nonce.enabled=true but nonce config has 0 accounts");
        }
        tracing::info!(count = nonces.len(), "durable nonce manager initialized");
        let mgr = Arc::new(NonceManager::new(nonces));
        let cache = Arc::new(NonceSlotHashCache::new(256));
        // RPC fallback poller for Stale recovery.
        let _rpc_poller = nonce_rpc_fallback::spawn(nonce_rpc_fallback::RpcPollerConfig {
            rpc: rpc.clone(),
            manager: mgr.clone(),
            poll_interval: Duration::from_secs(cfg.nonce.rpc_poll_interval_secs),
            in_flight_deadline: Duration::from_secs(cfg.nonce.in_flight_deadline_secs),
            awaiting_update_deadline: Duration::from_secs(
                cfg.nonce.awaiting_update_deadline_secs,
            ),
            stop: stop.clone(),
        })?;
        (Some(mgr), Some(cache))
    } else {
        (None, None)
    };

    // Trigger engine (now that nonce slot-hash cache is set up).
    let (trigger_tx, trigger_rx) = bounded::<TriggerEvent>(8192);
    let (match_tx, match_rx) = bounded::<MatchEvent>(8192);
    let engine_counters = Arc::new(TriggerEngineCounters::default());
    let _engine = spawn_engine(TriggerEngineConfig {
        ordered_rx,
        schedule: schedule_snapshot.clone(),
        pending_sigs: pending_sigs.clone(),
        trigger_tx,
        match_tx,
        counters: engine_counters.clone(),
        stop: stop.clone(),
        pinned_core: None,
        nonce_slot_hash_cache: nonce_slot_hash_cache.clone(),
    })?;

    // Senders: build Arc<dyn TxSender> per enabled SenderConfig.
    // For phase 3+ supports multiple — dispatcher fans out to all variants
    // produced by the preparer (one per sender) in a shuffled order.
    let enabled_senders: Vec<_> =
        cfg.senders.iter().filter(|s| s.enabled).cloned().collect();
    if enabled_senders.is_empty() {
        anyhow::bail!("no enabled senders in config");
    }
    let mut senders_by_id: std::collections::HashMap<u8, Arc<dyn TxSender>> =
        std::collections::HashMap::new();
    for sc in &enabled_senders {
        let s: Arc<dyn TxSender> = match sc.kind {
            SenderKind::Helius => Arc::new(HeliusSender::new(
                sc.id, sc.name.clone(), sc.endpoint_url.clone(),
            )),
            SenderKind::Jito => {
                if sc.regions.is_empty() {
                    anyhow::bail!(
                        "jito sender {:?} (id={}) must declare at least one region",
                        sc.name, sc.id
                    );
                }
                Arc::new(JitoSender::new(
                    sc.id,
                    sc.name.clone(),
                    sc.endpoint_url.clone(),
                    sc.regions.clone(),
                    sc.outbound_ips.clone(),
                ))
            }
        };
        tracing::info!(id = sc.id, name = %s.name(), endpoint = %s.endpoint_url(),
            tip_lamports = sc.tip_lamports,
            regions = ?sc.regions, outbound_ips = sc.outbound_ips.len(),
            "sender configured");
        senders_by_id.insert(sc.id, s);
    }

    // Spawn preparer with full sender list — it signs one variant per sender
    // per trigger, shuffled deterministically by (schedule_seed, slot, tick).
    let _preparer = spawn_preparer(PreparerConfig {
        schedule_rx: preparer_schedule_rx,
        pool: tx_pool.clone(),
        keypair: keypair.clone(),
        blockhash_cache: bh_runner.cache.clone(),
        tx_cfg: cfg.tx.clone(),
        senders: enabled_senders.clone(),
        shuffle_seed: cfg.schedule.seed.unwrap_or(0xDEADBEEF),
        current_slot: current_slot.clone(),
        nonce_manager: nonce_manager.clone(),
        counters: preparer_counters.clone(),
        stop: stop.clone(),
    })?;

    // Recorder.
    let (register_tx, register_rx) = bounded::<RegisterEvent>(8192);
    let (send_event_tx, send_event_rx) = bounded::<SendEvent>(8192);
    let recorder_counters = Arc::new(RecorderCounters::default());
    let anchor = Instant::now();
    let active_summary: Arc<parking_lot::Mutex<ActiveWindowSummary>> =
        Arc::new(parking_lot::Mutex::new(ActiveWindowSummary::default()));
    let _recorder = spawn_recorder(RecorderConfig {
        register_rx,
        send_rx: send_event_rx,
        match_rx,
        output_path: recorder_path.clone(),
        run_id: run_id.clone(),
        anchor,
        deadline: Duration::from_secs(cfg.run.observation_deadline_secs),
        counters: recorder_counters.clone(),
        stop: stop.clone(),
        nonce_manager: nonce_manager.clone(),
        slot_hash_cache: nonce_slot_hash_cache.clone(),
        summary: active_summary.clone(),
    })?;

    // Dispatcher: drains trigger_rx, takes pre-signed Vec<PreSignedTx> from
    // pool (already shuffled by preparer), fans out async send to each
    // sender in vec order. On pool miss, falls back to building inline
    // (single-variant, first enabled sender) so we never miss a fire.
    let dispatcher_stop = stop.clone();
    let dispatcher_pending = pending_sigs.clone();
    let dispatcher_senders_by_id = senders_by_id.clone();
    let dispatcher_senders_cfg = enabled_senders.clone();
    let dispatcher_pool = tx_pool.clone();
    let dispatcher_register_tx = register_tx.clone();
    let dispatcher_send_tx = send_event_tx.clone();
    let dispatcher_keypair = keypair.clone();
    let dispatcher_bh = bh_runner.cache.clone();
    let dispatcher_tx_cfg = cfg.tx.clone();
    let dispatcher_counters = Arc::new(DispatcherCounters::default());
    let dispatcher_counters_clone = dispatcher_counters.clone();
    let dispatcher_nonce_mode = cfg.nonce.enabled;
    std::thread::Builder::new()
        .name("dispatcher".into())
        .spawn(move || {
            let rt = TokioRtBuilder::new_multi_thread()
                .worker_threads(4)
                .enable_all()
                .build()
                .expect("tokio rt");
            rt.block_on(dispatcher_loop(
                trigger_rx,
                dispatcher_pending,
                dispatcher_senders_cfg,
                dispatcher_senders_by_id,
                dispatcher_pool,
                dispatcher_keypair,
                dispatcher_bh,
                dispatcher_tx_cfg,
                dispatcher_nonce_mode,
                dispatcher_register_tx,
                dispatcher_send_tx,
                dispatcher_counters_clone,
                dispatcher_stop,
            ));
        })?;

    // Ctrl-C.
    let stop_for_ctrlc = stop.clone();
    ctrlc::set_handler(move || {
        tracing::info!("Ctrl-C received");
        stop_for_ctrlc.store(true, Ordering::Relaxed);
    })
    .ok();

    // Main loop — periodic summary + duration timer + balance watchdog.
    let start = Instant::now();
    let mut next_summary = start + Duration::from_secs(5);
    let mut last_balance_check = start;
    while !stop.load(Ordering::Relaxed) {
        let now = Instant::now();
        if now >= start + duration {
            break;
        }
        if now >= next_summary {
            next_summary = now + Duration::from_secs(5);
            log_summary(
                now.duration_since(start).as_secs_f64(),
                &merger_counters,
                &supervisor_counters,
                &engine_counters,
                &recorder_counters,
            );
        }
        if now.duration_since(last_balance_check) >= Duration::from_secs(30) {
            last_balance_check = now;
            if let Ok(bal) = rpc.get_balance(&keypair.pubkey()) {
                if bal < cfg.run.min_balance_lamports {
                    tracing::warn!(
                        bal, min = cfg.run.min_balance_lamports,
                        "wallet balance below threshold — stopping run"
                    );
                    stop.store(true, Ordering::Relaxed);
                    break;
                }
            }
        }
        // Refresh current_slot from RPC every ~5s so preparer can evict
        // pool entries for slots that already passed without firing.
        if now.duration_since(last_balance_check).as_secs() % 5 == 0 {
            if let Ok(s) = rpc.get_slot() {
                current_slot.store(s, Ordering::Relaxed);
            }
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    tracing::info!("shutdown");
    stop.store(true, Ordering::Relaxed);
    // Let recorder flush; it joins on channels closing — short sleep is fine.
    std::thread::sleep(Duration::from_millis(500));
    drop(bh_runner.handle); // detach blockhash refresher

    let end_balance = rpc.get_balance(&keypair.pubkey()).unwrap_or(0);
    tracing::info!(end_balance, drained = start_balance.saturating_sub(end_balance),
        "wallet balance at end (lamports)");

    // Final report.
    let m = merger_counters.snapshot();
    let s = supervisor_counters.snapshot();
    let e = engine_counters.snapshot();
    let r = recorder_counters.snapshot();
    let pp = preparer_counters.snapshot();
    let pool_hits = dispatcher_counters.pool_hits.load(Ordering::Relaxed);
    let pool_misses_built =
        dispatcher_counters.pool_misses_fallback_built.load(Ordering::Relaxed);
    let pool_misses_skipped =
        dispatcher_counters.pool_misses_skipped_no_blockhash.load(Ordering::Relaxed);
    let active = active_summary.lock().clone();

    println!("\n=== PHASE 3 FINAL REPORT ===");
    println!("Run dir            : {}", run_dir.display());
    println!();

    println!("--- Pipeline volumes ---");
    println!("SS / YS entries     : {} / {}", m.ss_received, m.ys_received);
    println!("Supervisor          : imm={} reord={} miss={}",
        s.entries_emitted_immediate, s.entries_emitted_reordered, s.entries_missing_timeout);
    println!("Slots               : complete={} incomplete={}",
        s.slots_complete, s.slots_incomplete);
    println!("Schedule entries    : {} prepared, {} fired",
        pp.triggers_prepared, e.schedule_hits);
    println!();

    // Triggers section — unique (slot, tick) events.
    let triggers_fired = active.triggers_attempted;
    let triggers_landed = active.triggers_landed;
    let triggers_lost = triggers_fired.saturating_sub(triggers_landed);
    let land_pct = if triggers_fired > 0 {
        triggers_landed as f64 / triggers_fired as f64 * 100.0
    } else { 0.0 };
    let lost_pct = if triggers_fired > 0 {
        triggers_lost as f64 / triggers_fired as f64 * 100.0
    } else { 0.0 };
    println!("--- Triggers (unique slot+tick) ---");
    println!("Fired               : {}", triggers_fired);
    println!("Landed              : {} ({:.1}%)", triggers_landed, land_pct);
    println!("Lost (no variant)   : {} ({:.1}%)", triggers_lost, lost_pct);
    println!();

    // Sends section — one row per (trigger, sender) network attempt.
    // In nonce mode at most 1 variant per trigger can land; the sibling
    // necessarily ends up as unknown_pending. We split that out from
    // genuinely lost triggers (both variants failed to land).
    let sends_total = active.attempts_total;
    let sends_landed = active.attempts_landed;
    let sends_send_err = active.attempts_send_error;
    let sends_unknown = active.attempts_unknown_pending;
    let losing_siblings = if cfg.nonce.enabled {
        triggers_landed // 1 sibling per landed trigger, all variants except the winner
    } else {
        0
    };
    let lost_no_land = sends_unknown.saturating_sub(losing_siblings);
    println!("--- Sends (per trigger × per sender) ---");
    println!("Total sent          : {}", sends_total);
    println!("  landed            : {}", sends_landed);
    if cfg.nonce.enabled {
        println!("  losing siblings   : {}  (other variant of landed trigger; expected in nonce mode)",
            losing_siblings);
        println!("  lost (no land)    : {}  (both variants of lost triggers)", lost_no_land);
    } else {
        println!("  unknown pending   : {}", sends_unknown);
    }
    println!("  send errors       : {}", sends_send_err);
    println!();

    // Per-sender wins — in nonce mode the meaningful metric is which sender
    // got the landing when both raced. `lost_to_other` = times the other sender
    // won this trigger; their sum (per row) = total triggers landed.
    println!("--- Per-sender wins (at most 1 variant per trigger lands in nonce mode) ---");
    println!(
        "{:<3} {:<14} {:>6} {:>15} {:>12} {:>13} {:>14}",
        "id", "name", "wins", "loses_to_other", "wins_share", "rtt_avg(us)", "obs_avg(us)"
    );
    let sender_name_by_id: std::collections::HashMap<u8, String> = cfg
        .senders
        .iter()
        .map(|sc| (sc.id, sc.name.clone()))
        .collect();
    for ps in &active.per_sender {
        let lost_to_other = triggers_landed.saturating_sub(ps.landed);
        let wins_share = if triggers_landed > 0 {
            ps.landed as f64 / triggers_landed as f64 * 100.0
        } else { 0.0 };
        let name = sender_name_by_id
            .get(&ps.sender_id)
            .cloned()
            .unwrap_or_default();
        println!(
            "{:<3} {:<14} {:>6} {:>15} {:>11.1}% {:>13} {:>14}",
            ps.sender_id,
            name,
            ps.landed,
            lost_to_other,
            wins_share,
            ps.send_rtt_us_avg().map(|v| format!("{:.0}", v)).unwrap_or_else(|| "-".into()),
            ps.send_to_observed_us_avg().map(|v| format!("{:.0}", v)).unwrap_or_else(|| "-".into()),
        );
    }
    println!();

    println!("--- Active window ---");
    println!("Duration            : {:.2}s", active.active_secs);
    println!();

    println!("--- Diagnostics ---");
    println!("Preparer            : variants_signed={} signing_err={} bh_not_ready={} pool_evict={} past_slot={}",
        pp.variants_signed, pp.signing_errors, pp.blockhash_not_ready,
        pp.pool_evictions, pp.entries_past_slot);
    if cfg.nonce.enabled {
        println!("Preparer nonce      : stalls={} (retries waiting for Ready)", pp.nonce_stall);
    }
    let dispatcher_total = pool_hits + pool_misses_built + pool_misses_skipped;
    println!("Dispatcher          : hits={} ({:.1}%) miss_fallback={} miss_skipped={}",
        pool_hits,
        if dispatcher_total > 0 { pool_hits as f64 / dispatcher_total as f64 * 100.0 } else { 0.0 },
        pool_misses_built, pool_misses_skipped);
    println!("Trigger engine      : entries={} sched_hits={} sig_hits={}",
        e.entries_seen, e.schedule_hits, e.sig_hits);
    println!("Recorder            : register={} send={} match={} write_err={}",
        r.register_events, r.send_events, r.match_events, r.write_errors);
    if cfg.nonce.enabled {
        println!("Recorder nonce      : advanced_local={} local_compute_miss={}",
            r.nonce_advanced_local, r.nonce_local_compute_miss);
    }
    println!();
    println!("Records written to : {}", recorder_path.display());

    Ok(())
}

#[derive(Default)]
struct DispatcherCounters {
    pool_hits: AtomicU64,
    pool_misses_fallback_built: AtomicU64,
    pool_misses_skipped_no_blockhash: AtomicU64,
}

#[allow(clippy::too_many_arguments)]
async fn dispatcher_loop(
    trigger_rx: Receiver<TriggerEvent>,
    pending_sigs: Arc<DashSet<Signature>>,
    senders_cfg: Vec<tick_trigger_fan_out_bench::config::SenderConfig>,
    senders_by_id: std::collections::HashMap<u8, Arc<dyn TxSender>>,
    pool: Arc<TxPool>,
    keypair: Arc<solana_sdk::signature::Keypair>,
    bh_cache: Arc<tick_trigger_fan_out_bench::blockhash_cache::BlockhashCache>,
    tx_cfg: tick_trigger_fan_out_bench::config::TxConfig,
    nonce_mode_enabled: bool,
    register_tx: Sender<RegisterEvent>,
    send_event_tx: Sender<SendEvent>,
    counters: Arc<DispatcherCounters>,
    stop: Arc<AtomicBool>,
) {
    use tick_trigger_fan_out_bench::tip_accounts::{tip_accounts_for, TipAccountRotator};
    use tick_trigger_fan_out_bench::tx_builder;
    use tick_trigger_fan_out_bench::tx_pool::PreSignedTx;

    // Per-sender tip rotator for the fallback (pool-miss) path.
    let fallback_rotators: std::collections::HashMap<u8, Arc<TipAccountRotator>> = senders_cfg
        .iter()
        .map(|s| {
            (
                s.id,
                Arc::new(TipAccountRotator::new(tip_accounts_for(s.kind).to_vec())),
            )
        })
        .collect();

    loop {
        if stop.load(Ordering::Relaxed) {
            break;
        }
        let trig = match trigger_rx.recv_timeout(Duration::from_millis(200)) {
            Ok(t) => t,
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
        };

        // Take pre-signed variants (already shuffled by preparer). On miss
        // we synthesise a single-sender fallback vec so the loop logic is
        // unified.
        let variants: Vec<PreSignedTx> = match pool.take(trig.slot, trig.tick) {
            Some(v) => {
                counters.pool_hits.fetch_add(1, Ordering::Relaxed);
                v
            }
            None => {
                // In nonce mode we deliberately DON'T build a fallback — we'd
                // need to coordinate with the manager (take_ready, advance on
                // landing) which the preparer already owns. Better to skip
                // this trigger than emit a tx with a stale nonce.
                if nonce_mode_enabled {
                    counters
                        .pool_misses_skipped_no_blockhash
                        .fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        slot = trig.slot, tick = trig.tick,
                        "pool miss in nonce mode — skipping (preparer didn't supply variants in time)"
                    );
                    continue;
                }
                if bh_cache.is_empty() {
                    counters
                        .pool_misses_skipped_no_blockhash
                        .fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        slot = trig.slot, tick = trig.tick,
                        "pool miss AND blockhash not primed — skipping trigger"
                    );
                    continue;
                }
                counters
                    .pool_misses_fallback_built
                    .fetch_add(1, Ordering::Relaxed);
                let bh = bh_cache.current();
                // Build one variant per enabled sender, no shuffle (rare path).
                let mut v = Vec::with_capacity(senders_cfg.len());
                for sc in &senders_cfg {
                    let tip = if sc.tip_lamports > 0 {
                        fallback_rotators
                            .get(&sc.id)
                            .and_then(|r| r.next())
                    } else {
                        None
                    };
                    let built = tx_builder::build(tx_builder::BuildParams {
                        payer: &keypair,
                        blockhash: bh,
                        sender_id: sc.id,
                        trigger_id: trig.trigger_id.0,
                        tip_account: tip,
                        tip_lamports: sc.tip_lamports,
                        nonce: None,
                        tx_cfg: &tx_cfg,
                    });
                    v.push(PreSignedTx {
                        sender_id: sc.id,
                        tx: Arc::new(built.tx),
                        signature: built.signature,
                        blockhash: bh,
                        prepared_at: Instant::now(),
                        nonce_id: None,
                    });
                }
                v
            }
        };

        // Fan out: emit RegisterEvent + spawn async send for each variant.
        let send_order = variants.len();
        for (order_idx, presigned) in variants.into_iter().enumerate() {
            let sig = presigned.signature;
            pending_sigs.insert(sig);

            let sender_cfg = senders_cfg
                .iter()
                .find(|s| s.id == presigned.sender_id)
                .cloned();
            let Some(sender_cfg) = sender_cfg else { continue };
            let Some(sender) = senders_by_id.get(&presigned.sender_id).cloned() else {
                continue;
            };

            let reg = RegisterEvent {
                trigger_id: trig.trigger_id,
                sender_id: sender_cfg.id,
                sender_name: sender_cfg.name.clone(),
                endpoint_url: sender_cfg.endpoint_url.clone(),
                protocol: sender.protocol().to_string(),
                signature: sig,
                slot: trig.slot,
                tick: trig.tick,
                trigger_observed_at: trig.trigger_observed_at,
                prepared_at: presigned.prepared_at,
                blockhash: presigned.blockhash,
                nonce_id: presigned.nonce_id,
            };
            let _ = register_tx.try_send(reg);

            let sender_for_task = sender.clone();
            let send_tx_for_task = send_event_tx.clone();
            let tx_for_task = presigned.tx;
            let trigger_id = trig.trigger_id;
            let sender_id = sender_cfg.id;
            tokio::spawn(async move {
                let outcome = sender_for_task.send(&tx_for_task).await;
                let _ = send_tx_for_task.try_send(SendEvent {
                    trigger_id,
                    sender_id,
                    outcome,
                });
            });
            // We don't actually need order_idx outside metrics; suppress
            // unused-variable noise without losing intent.
            let _ = order_idx;
        }
        let _ = send_order;
    }
}

fn log_summary(
    elapsed_secs: f64,
    merger: &Arc<MergerCounters>,
    supervisor: &Arc<PohSupervisorCounters>,
    engine: &Arc<TriggerEngineCounters>,
    recorder: &Arc<RecorderCounters>,
) {
    let m = merger.snapshot();
    let s = supervisor.snapshot();
    let e = engine.snapshot();
    let r = recorder.snapshot();
    // In nonce mode r.records_landed ≈ unique triggers landed (at most 1 variant
    // per trigger lands), so we approximate triggers_lost as a running diff.
    // Slightly noisy near tail because lost-trigger detection lags by the
    // observation deadline, but good enough for live monitoring.
    let fired = e.schedule_hits;
    let landed = r.records_landed;
    let lost_so_far = fired.saturating_sub(landed).saturating_sub(r.records_unknown_pending / 2);
    tracing::info!(
        "t={:.1}s | src ss={} ys={} | slots cplt={} incp={} | triggers fired={} landed={} lost~{} | sends land={} err={} pend={}",
        elapsed_secs, m.ss_received, m.ys_received,
        s.slots_complete, s.slots_incomplete,
        fired, landed, lost_so_far,
        landed, r.records_send_error, r.records_unknown_pending,
    );
}

fn spawn_warmup(
    name: &'static str,
    rx: Receiver<EntryObservation>,
    tx: Sender<EntryObservation>,
    first_full_slot: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name(name.into())
        .spawn(move || {
            let mut startup_slot: Option<u64> = None;
            let mut warm = false;
            loop {
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                match rx.recv_timeout(Duration::from_millis(200)) {
                    Ok(obs) => {
                        if warm {
                            let _ = tx.try_send(obs);
                            continue;
                        }
                        match startup_slot {
                            None => startup_slot = Some(obs.slot),
                            Some(s0) if obs.slot <= s0 => {}
                            Some(_) => {
                                warm = true;
                                first_full_slot.store(obs.slot, Ordering::Relaxed);
                                let _ = tx.try_send(obs);
                            }
                        }
                    }
                    Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
                    Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
                }
            }
        })
        .expect("spawn warmup")
}
