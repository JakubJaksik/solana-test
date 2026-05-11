use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crossbeam_channel::bounded;
use dashmap::DashSet;
use entry_sources::shredstream::ShredStreamGrpcSource;
use entry_sources::{DropCounters, EntrySource};
use solana_client::rpc_client::RpcClient;
use solana_sdk::signature::Signer;
use tracing::{info, warn};

use crate::config::{RunArgs, ScheduleArgs};
use crate::counters::BenchCounters;
use crate::helius_sender::HeliusSender;
use crate::leader_cache::LeaderCache;
use crate::observer::ObserverConfig;
use crate::preparer::PreparerConfig;
use crate::rpc_fallback::FallbackQueue;
use crate::run_meta::write_run_meta;
use crate::schedule::Schedule;
use crate::sender::SenderConfig;
use crate::tx_pool::TxPool;
use crate::wallet::load_keypair;
use crate::writer::{spawn_finalizer, spawn_parquet, ParquetWriterConfig, WriterConfig};

pub fn generate_schedule(args: ScheduleArgs) -> anyhow::Result<()> {
    let seed = args.seed.unwrap_or_else(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0)
    });
    let sched = Schedule::generate(args.start_slot, args.num_slots, seed);
    sched.save(&args.out)?;
    info!(
        seed,
        num_slots = args.num_slots,
        start_slot = args.start_slot,
        entries = sched.entries.len(),
        output = ?args.out,
        "schedule generated"
    );
    Ok(())
}

pub fn run(args: RunArgs) -> anyhow::Result<()> {
    let run_id = chrono::Utc::now().format("%Y%m%d-%H%M%S").to_string();
    let run_dir = args.output_dir.join(&run_id);
    std::fs::create_dir_all(&run_dir)?;
    info!(?run_dir, "run directory ready");

    let cores = parse_cores(args.core_pinning.as_deref());

    let schedule = Schedule::load(&args.schedule)?;
    let keypair = Arc::new(load_keypair(&args.wallet_keypair)?);
    info!(
        pubkey = %keypair.as_ref().pubkey(),
        num_entries = schedule.entries.len(),
        "wallet + schedule loaded"
    );

    std::fs::copy(&args.schedule, run_dir.join("schedule.json"))?;

    let rpc = RpcClient::new(args.helius_rpc_url.clone());
    let current_slot = rpc.get_slot()?;
    let epoch_at_start = current_slot / 432_000;
    let leader_cache = LeaderCache::from_rpc(&args.helius_rpc_url, current_slot)?;
    leader_cache.snapshot_to_json(&run_dir.join("leader-schedule.json"))?;

    write_run_meta(
        &run_dir,
        &args,
        current_slot,
        epoch_at_start,
        schedule.seed,
        schedule.start_slot,
        schedule.num_slots,
    )?;

    let anchor = Instant::now();

    let counters = Arc::new(BenchCounters::default());
    let ss_counters = Arc::new(DropCounters::default());
    let pool = TxPool::new();
    let pending_sigs: Arc<DashSet<_>> = Arc::new(DashSet::with_capacity(8192));
    let current_slot_atom = Arc::new(AtomicU64::new(current_slot));
    let stop = Arc::new(AtomicBool::new(false));

    let mut schedule_inner: std::collections::HashSet<(u64, u8)> =
        std::collections::HashSet::with_capacity(schedule.entries.len());
    for e in &schedule.entries {
        schedule_inner.insert((e.slot, e.tick));
    }
    let schedule_set: Arc<std::collections::HashSet<(u64, u8)>> = Arc::new(schedule_inner);
    let schedule_arc = Arc::new(schedule.entries.clone());

    let helius = Arc::new(HeliusSender::new(
        args.helius_sender_url.clone(),
        args.helius_sender_api_key.clone(),
        args.helius_sender_swqos_only,
    )?);

    let ss_src = Box::new(ShredStreamGrpcSource {
        endpoint: args.shredstream_grpc_url.clone(),
        channel_capacity: args.channel_capacity,
        pinned_core: cores.get("ss_grpc").copied(),
        counters: ss_counters.clone(),
    });
    let entry_rx = ss_src.start()?;

    let (send_q_tx, send_q_rx) = bounded(args.channel_capacity);
    let (match_q_tx, match_q_rx) = bounded(args.channel_capacity);
    let (send_ev_tx, send_ev_rx) = bounded(args.channel_capacity);
    let (final_tx, final_rx) = bounded(args.channel_capacity);
    let (tick_event_tx, tick_event_rx) = crossbeam_channel::bounded(args.channel_capacity);

    let obs_handle = crate::observer::spawn(ObserverConfig {
        entry_rx,
        schedule: schedule_set,
        pool: pool.clone(),
        send_queue: send_q_tx,
        match_queue: match_q_tx,
        pending_sigs: pending_sigs.clone(),
        current_slot: current_slot_atom.clone(),
        tick_event_tx,
        pinned_core: cores.get("observer").copied(),
        counters: counters.clone(),
        stop: stop.clone(),
    })?;

    let prep_handle = crate::preparer::spawn(PreparerConfig {
        schedule: schedule_arc,
        keypair: keypair.clone(),
        rpc_url: args.helius_rpc_url.clone(),
        pool: pool.clone(),
        current_slot: current_slot_atom.clone(),
        refresh_interval: args.preparer_refresh,
        look_ahead_slots: args.look_ahead_slots,
        amount_lamports: args.tx_amount_lamports,
        priority_fee_microlamports: args.priority_fee_microlamports,
        helius_tip_lamports: args.helius_tip_lamports,
        pinned_core: cores.get("preparer").copied(),
        counters: counters.clone(),
        stop: stop.clone(),
    })?;

    let fallback_q = FallbackQueue::default();

    let send_handle = crate::sender::spawn(SenderConfig {
        send_queue: send_q_rx,
        send_event_tx: send_ev_tx,
        pending_sigs: pending_sigs.clone(),
        helius: helius.clone(),
        blockhash_max_age: Duration::from_secs(50),
        pinned_core: cores.get("sender").copied(),
        counters: counters.clone(),
        stop: stop.clone(),
    })?;

    let writer_handle = spawn_finalizer(WriterConfig {
        send_event_rx: send_ev_rx,
        match_rx: match_q_rx,
        final_tx,
        deadline: args.observation_deadline,
        pinned_core: cores.get("writer").copied(),
        pending_sigs: pending_sigs.clone(),
        counters: counters.clone(),
        fallback_queue: fallback_q.clone(),
        stop: stop.clone(),
    })?;

    let parquet_path = run_dir.join("tx-events.parquet");
    let parquet_handle = spawn_parquet(ParquetWriterConfig {
        final_rx,
        output_path: parquet_path.clone(),
        row_group_size: args.row_group_size,
        flush_interval: args.flush_interval,
        pinned_core: cores.get("parquet").copied(),
        leader_cache: leader_cache.clone(),
        anchor,
    })?;

    let sidecar_handle = crate::sidecar::spawn(crate::sidecar::SidecarConfig {
        rx: tick_event_rx,
        path: run_dir.join("tick-events.jsonl"),
        anchor,
        pinned_core: cores.get("sidecar").copied(),
        stop: stop.clone(),
    })?;

    let fallback_handle = crate::rpc_fallback::spawn(crate::rpc_fallback::RpcFallbackConfig {
        queue: fallback_q,
        rpc_url: args.helius_rpc_url.clone(),
        annotations_path: run_dir.join("rpc-fallback-annotations.jsonl"),
        poll_interval: Duration::from_secs(30),
        batch_size: 256,
        pinned_core: cores.get("rpc").copied(),
        counters: counters.clone(),
        stop: stop.clone(),
    })?;

    info!(
        ?parquet_path,
        max_duration = ?args.max_duration,
        "bench running; press Ctrl-C to stop early"
    );

    let start = Instant::now();
    while start.elapsed() < args.max_duration {
        if stop.load(Ordering::Relaxed) {
            break;
        }
        let now_slot = current_slot_atom.load(Ordering::Relaxed);
        if let Some(last) = schedule.entries.last() {
            if now_slot > last.slot + 10 {
                info!(
                    "schedule covered, waiting {}s for trailing observations",
                    args.observation_deadline.as_secs()
                );
                std::thread::sleep(args.observation_deadline);
                break;
            }
        }
        std::thread::sleep(Duration::from_secs(5));
    }

    info!("signalling shutdown");
    stop.store(true, Ordering::Relaxed);

    if let Err(e) = obs_handle.join() {
        warn!(?e, "observer panicked");
    }
    if let Err(e) = send_handle.join() {
        warn!(?e, "sender panicked");
    }
    if let Err(e) = prep_handle.join() {
        warn!(?e, "preparer panicked");
    }
    if let Err(e) = writer_handle.join() {
        warn!(?e, "writer panicked");
    }
    if let Err(e) = parquet_handle.join() {
        warn!(?e, "parquet writer panicked");
    }
    if let Err(e) = sidecar_handle.join() {
        warn!(?e, "tick sidecar panicked");
    }
    if let Err(e) = fallback_handle.join() {
        warn!(?e, "rpc fallback panicked");
    }

    info!(?parquet_path, "shutdown complete");
    Ok(())
}

fn parse_cores(spec: Option<&str>) -> HashMap<String, usize> {
    let mut map = HashMap::new();
    if let Some(s) = spec {
        for tok in s.split(',') {
            if let Some((k, v)) = tok.split_once('=') {
                if let Ok(n) = v.trim().parse::<usize>() {
                    map.insert(k.trim().to_string(), n);
                }
            }
        }
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_cores_empty() {
        let m = parse_cores(None);
        assert!(m.is_empty());
    }

    #[test]
    fn parse_cores_multi() {
        let m = parse_cores(Some("preparer=4,ss=2,sender=3"));
        assert_eq!(m.get("preparer").copied(), Some(4));
        assert_eq!(m.get("ss").copied(), Some(2));
        assert_eq!(m.get("sender").copied(), Some(3));
    }

    #[test]
    fn parse_cores_ignores_malformed() {
        let m = parse_cores(Some("preparer=4,bad,sender=notanumber"));
        assert_eq!(m.get("preparer").copied(), Some(4));
        assert!(!m.contains_key("bad"));
        assert!(!m.contains_key("sender"));
    }
}
