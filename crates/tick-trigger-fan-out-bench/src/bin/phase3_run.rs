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
use tick_trigger_fan_out_bench::poh_supervisor::{
    spawn as spawn_supervisor, OrderedEvent, PohSupervisorConfig, PohSupervisorCounters,
};
use tick_trigger_fan_out_bench::recorder::{
    spawn as spawn_recorder, RecorderConfig, RecorderCounters, RegisterEvent, SendEvent,
};
use tick_trigger_fan_out_bench::schedule::Schedule;
use tick_trigger_fan_out_bench::senders::{helius::HeliusSender, TxSender};
use tick_trigger_fan_out_bench::trigger_engine::{
    spawn as spawn_engine, MatchEvent, TriggerEngineConfig, TriggerEngineCounters, TriggerEvent,
};
use tick_trigger_fan_out_bench::tx_builder;
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

    // Schedule — live ArcSwap snapshot + pump thread.
    let schedule_snapshot: Arc<ArcSwap<HashSet<(u64, u8)>>> =
        Arc::new(ArcSwap::from_pointee(HashSet::new()));
    let pending_sigs: Arc<DashSet<Signature>> = Arc::new(DashSet::new());
    let schedule_pump_stop = stop.clone();
    let schedule_snapshot_for_pump = schedule_snapshot.clone();
    let schedule_seed = cfg.schedule.seed;
    let chunk_size = cfg.schedule.chunk_size_slots;
    let triggers_per_slot = cfg.run.triggers_per_slot;
    std::thread::Builder::new()
        .name("schedule-pump".into())
        .spawn(move || {
            let mut sched = Schedule::new(schedule_seed, start_slot, chunk_size, triggers_per_slot);
            let mut accumulated: HashSet<(u64, u8)> = HashSet::new();
            // Pre-seed ~lead_slots ahead so the engine has schedule on day one.
            for _ in 0..((cfg.schedule.lead_slots / chunk_size).max(1) + 1) {
                for e in sched.next_chunk() {
                    accumulated.insert((e.slot, e.tick));
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
                }
                schedule_snapshot_for_pump.store(Arc::new(accumulated.clone()));
            }
        })?;

    // Trigger engine.
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
    })?;

    // Blockhash cache.
    let bh_runner = blockhash_cache::spawn(
        rpc.clone(),
        Duration::from_secs(cfg.rpc.blockhash_refresh_secs),
        stop.clone(),
    );

    // Senders (currently expects exactly 1 enabled; future phases will iterate).
    let enabled: Vec<_> = cfg.senders.iter().filter(|s| s.enabled).collect();
    if enabled.is_empty() {
        anyhow::bail!("no enabled senders in config");
    }
    if enabled.len() > 1 {
        tracing::warn!(
            "phase 3 currently uses only the FIRST enabled sender; multi-sender comes in phase 4. ignoring {} others",
            enabled.len() - 1
        );
    }
    let sender_cfg = enabled[0].clone();
    let sender: Arc<dyn TxSender> = match sender_cfg.kind {
        SenderKind::Helius => Arc::new(HeliusSender::new(
            sender_cfg.id,
            sender_cfg.name.clone(),
            sender_cfg.endpoint_url.clone(),
        )),
    };
    tracing::info!(name = %sender.name(), endpoint = %sender.endpoint_url(), "sender configured");

    // Recorder.
    let (register_tx, register_rx) = bounded::<RegisterEvent>(8192);
    let (send_event_tx, send_event_rx) = bounded::<SendEvent>(8192);
    let recorder_counters = Arc::new(RecorderCounters::default());
    let anchor = Instant::now();
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
    })?;

    // Dispatcher thread: own tokio runtime, drains trigger_rx and sends.
    let dispatcher_stop = stop.clone();
    let dispatcher_keypair = keypair.clone();
    let dispatcher_pending = pending_sigs.clone();
    let dispatcher_tx_cfg = cfg.tx.clone();
    let dispatcher_sender_cfg = sender_cfg.clone();
    let dispatcher_sender = sender.clone();
    let dispatcher_bh = bh_runner.cache.clone();
    let dispatcher_register_tx = register_tx.clone();
    let dispatcher_send_tx = send_event_tx.clone();
    std::thread::Builder::new()
        .name("dispatcher".into())
        .spawn(move || {
            let rt = TokioRtBuilder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .expect("tokio rt");
            rt.block_on(dispatcher_loop(
                trigger_rx,
                dispatcher_keypair,
                dispatcher_pending,
                dispatcher_tx_cfg,
                dispatcher_sender_cfg,
                dispatcher_sender,
                dispatcher_bh,
                dispatcher_register_tx,
                dispatcher_send_tx,
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
    println!("\n=== PHASE 3 FINAL REPORT ===");
    println!("Run dir            : {}", run_dir.display());
    println!();
    println!("--- Sources ---");
    println!("SS received        : {}", m.ss_received);
    println!("YS received        : {}", m.ys_received);
    println!();
    println!("--- Supervisor ---");
    println!("Entries immediate  : {}", s.entries_emitted_immediate);
    println!("Entries reordered  : {}", s.entries_emitted_reordered);
    println!("Missing markers    : {}", s.entries_missing_timeout);
    println!("Slots complete     : {}", s.slots_complete);
    println!("Slots incomplete   : {}", s.slots_incomplete);
    println!();
    println!("--- Trigger engine ---");
    println!("Entries seen       : {}", e.entries_seen);
    println!("Schedule hits      : {}", e.schedule_hits);
    println!("Triggers fired     : {}", e.schedule_hits);
    println!("Sig hits           : {}", e.sig_hits);
    println!();
    println!("--- Recorder (per-trigger outcomes) ---");
    println!("Landed             : {}", r.records_landed);
    println!("Send errors        : {}", r.records_send_error);
    println!("Unknown pending    : {}", r.records_unknown_pending);
    println!("Register events    : {}", r.register_events);
    println!("Send events        : {}", r.send_events);
    println!("Match events       : {}", r.match_events);
    println!();
    println!("Records written to : {}", recorder_path.display());

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn dispatcher_loop(
    trigger_rx: Receiver<TriggerEvent>,
    keypair: Arc<solana_sdk::signature::Keypair>,
    pending_sigs: Arc<DashSet<Signature>>,
    tx_cfg: tick_trigger_fan_out_bench::config::TxConfig,
    sender_cfg: tick_trigger_fan_out_bench::config::SenderConfig,
    sender: Arc<dyn TxSender>,
    bh_cache: Arc<tick_trigger_fan_out_bench::blockhash_cache::BlockhashCache>,
    register_tx: Sender<RegisterEvent>,
    send_event_tx: Sender<SendEvent>,
    stop: Arc<AtomicBool>,
) {
    loop {
        if stop.load(Ordering::Relaxed) {
            break;
        }
        let trig = match trigger_rx.recv_timeout(Duration::from_millis(200)) {
            Ok(t) => t,
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
        };
        if bh_cache.is_empty() {
            tracing::warn!(trigger_slot = trig.slot, tick = trig.tick, "blockhash not yet primed — skipping trigger");
            continue;
        }
        let bh = bh_cache.current();
        let prepared_at = Instant::now();
        let built = tx_builder::build(tx_builder::BuildParams {
            payer: &keypair,
            blockhash: bh,
            sender_id: sender_cfg.id,
            tip_account: None, // phase 3: no tip account — pure self-transfer
            tip_lamports: sender_cfg.tip_lamports,
            tx_cfg: &tx_cfg,
        });
        let sig = built.signature;
        pending_sigs.insert(sig);

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
            prepared_at,
            blockhash: bh,
        };
        let _ = register_tx.try_send(reg);

        let sender_for_task = sender.clone();
        let send_tx_for_task = send_event_tx.clone();
        let tx_for_task = built.tx;
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
    tracing::info!(
        "t={:.1}s | src ss={} ys={} | sup imm={} reord={} miss={} cplt={} incp={} | engine entries={} hits={} sig={} | rec land={} send_err={} unk={}",
        elapsed_secs, m.ss_received, m.ys_received,
        s.entries_emitted_immediate, s.entries_emitted_reordered, s.entries_missing_timeout,
        s.slots_complete, s.slots_incomplete,
        e.entries_seen, e.schedule_hits, e.sig_hits,
        r.records_landed, r.records_send_error, r.records_unknown_pending,
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
