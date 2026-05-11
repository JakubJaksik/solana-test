use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crossbeam_channel::bounded;
use entry_sources::shredstream::udp_rx::DEFAULT_RX_BUFFER_BYTES;
use entry_sources::shredstream::{ShredStreamGrpcSource, ShredStreamSource};
use entry_sources::yellowstone::YellowstoneSource;
use entry_sources::{DropCounters, EntrySource};

use crate::config::ShredstreamMode;
use serde::Serialize;
use solana_client::rpc_client::RpcClient;
use tracing::{info, warn};

use crate::config::RunArgs;
use crate::correlator::{spawn as spawn_corr, CorrelatorConfig};
use crate::leader_schedule::LeaderCache;
use crate::writer::{spawn as spawn_writer, WriterConfig};

pub fn run(args: RunArgs) -> anyhow::Result<()> {
    // Per-run subdirectory.
    let run_id = chrono::Utc::now().format("%Y%m%d-%H%M%S").to_string();
    let run_dir = args.output_dir.join(&run_id);
    std::fs::create_dir_all(&run_dir)?;
    info!(?run_dir, "run directory ready");

    let cores = parse_cores(args.core_pinning.as_deref());
    let counters = Arc::new(DropCounters::default());
    let diff_dropped = Arc::new(AtomicU64::new(0));

    // Fetch leader schedule for current epoch.
    let rpc = RpcClient::new(args.rpc_url.clone());
    let current_slot = rpc.get_slot()?;
    let epoch_at_start = current_slot / 432_000;
    info!(current_slot, epoch_at_start, "fetched current slot");
    let leader_cache = LeaderCache::from_rpc(&args.rpc_url, current_slot)?;
    leader_cache.snapshot_to_json(&run_dir.join("leader-schedule.json"))?;

    // Anchor for ns offsets.
    let anchor = Instant::now();
    let anchor_systemtime_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);

    // Write run-meta.json.
    write_run_meta(&run_dir, &args, current_slot, epoch_at_start, anchor_systemtime_ns)?;

    // Spawn anomalies stats thread.
    let anomalies_path = run_dir.join("anomalies.jsonl");
    let stats_stop = Arc::new(AtomicBool::new(false));
    let stats_handle = spawn_stats_thread(
        counters.clone(),
        diff_dropped.clone(),
        anomalies_path,
        anchor,
        stats_stop.clone(),
    );

    // Spawn Yellowstone source.
    let ys_src = Box::new(YellowstoneSource {
        url: args.yellowstone_url.clone(),
        token: args.yellowstone_token.clone(),
        channel_capacity: args.channel_capacity,
        pinned_core: cores.get("ys").copied(),
        counters: counters.clone(),
    });
    let ys_rx = ys_src.start()?;

    // Spawn ShredStream source (grpc by default; udp legacy via flag).
    let ss_rx = match args.shredstream_mode {
        ShredstreamMode::Grpc => {
            info!(endpoint = %args.shredstream_grpc_url, "starting ShredStream gRPC source");
            let src = Box::new(ShredStreamGrpcSource {
                endpoint: args.shredstream_grpc_url.clone(),
                channel_capacity: args.channel_capacity,
                pinned_core: cores.get("ss_rx").copied(),
                counters: counters.clone(),
            });
            src.start()?
        }
        ShredstreamMode::Udp => {
            info!(bind = %args.shredstream_bind, "starting ShredStream raw-UDP source (legacy)");
            let src = Box::new(ShredStreamSource {
                bind: args.shredstream_bind,
                udp_channel_capacity: args.channel_capacity,
                obs_channel_capacity: args.channel_capacity,
                udp_pinned_core: cores.get("ss_rx").copied(),
                deshred_pinned_core: cores.get("deshred").copied(),
                rx_buffer_bytes: DEFAULT_RX_BUFFER_BYTES,
                counters: counters.clone(),
            });
            src.start()?
        }
    };

    // Shutdown signal — set true after duration; correlator picks it up and exits,
    // dropping its diff_tx → writer sees Disconnected → flushes + writes Parquet footer.
    let shutdown = Arc::new(AtomicBool::new(false));

    // Spawn correlator.
    let (diff_tx, diff_rx) = bounded(args.channel_capacity);
    let corr_handle = spawn_corr(CorrelatorConfig {
        ys_rx,
        ss_rx,
        diff_tx,
        anchor,
        deadline: Duration::from_secs(5),
        pinned_core: cores.get("corr").copied(),
        leader_lookup: leader_cache.clone(),
        diff_dropped: diff_dropped.clone(),
        shutdown: shutdown.clone(),
    })?;

    // Spawn Parquet writer.
    let dump_path = run_dir.join("diff.parquet");
    let writer_handle = spawn_writer(WriterConfig {
        diff_rx,
        output_path: dump_path.clone(),
        row_group_size: args.row_group_size,
        flush_interval: args.flush_interval,
        pinned_core: cores.get("writer").copied(),
    })?;

    info!(?dump_path, duration = ?args.duration, "comparator running");
    std::thread::sleep(args.duration);
    info!("duration elapsed; signalling shutdown");
    shutdown.store(true, Ordering::Relaxed);

    // Order matters: correlator first (drains, then drops diff_tx), then writer
    // (sees Disconnected, drains its channel, writes Parquet footer, returns).
    if let Err(e) = corr_handle.join() {
        warn!(?e, "correlator thread panicked");
    }
    if let Err(e) = writer_handle.join() {
        warn!(?e, "writer thread panicked");
    }

    stats_stop.store(true, Ordering::Relaxed);
    let _ = stats_handle.join();

    info!(?dump_path, "shutdown complete; Parquet finalized");
    Ok(())
}

#[derive(Serialize)]
struct RunMeta<'a> {
    started_at_utc: String,
    anchor_systemtime_ns: u64,
    host: String,
    yellowstone_endpoint: &'a str,
    shredstream_bind: String,
    rpc_url: &'a str,
    epoch_at_start: u64,
    current_slot_at_start: u64,
    binary_version: &'static str,
    config: RunMetaConfig,
}

#[derive(Serialize)]
struct RunMetaConfig {
    row_group_size: usize,
    channel_capacity: usize,
    flush_interval_secs: u64,
    duration_secs: u64,
    core_pinning: Option<String>,
}

fn write_run_meta(
    run_dir: &std::path::Path,
    args: &RunArgs,
    current_slot: u64,
    epoch_at_start: u64,
    anchor_systemtime_ns: u64,
) -> anyhow::Result<()> {
    let meta = RunMeta {
        started_at_utc: chrono::Utc::now().to_rfc3339(),
        anchor_systemtime_ns,
        host: hostname::get()
            .ok()
            .and_then(|s| s.into_string().ok())
            .unwrap_or_else(|| "unknown".to_string()),
        yellowstone_endpoint: &args.yellowstone_url,
        shredstream_bind: args.shredstream_bind.to_string(),
        rpc_url: &args.rpc_url,
        epoch_at_start,
        current_slot_at_start: current_slot,
        binary_version: env!("CARGO_PKG_VERSION"),
        config: RunMetaConfig {
            row_group_size: args.row_group_size,
            channel_capacity: args.channel_capacity,
            flush_interval_secs: args.flush_interval.as_secs(),
            duration_secs: args.duration.as_secs(),
            core_pinning: args.core_pinning.clone(),
        },
    };
    let path = run_dir.join("run-meta.json");
    std::fs::write(&path, serde_json::to_string_pretty(&meta)?)?;
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

fn spawn_stats_thread(
    counters: Arc<DropCounters>,
    diff_dropped: Arc<AtomicU64>,
    anomalies_path: PathBuf,
    anchor: Instant,
    stop: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name("stats".into())
        .spawn(move || {
            let mut file = match OpenOptions::new()
                .create(true)
                .append(true)
                .open(&anomalies_path)
            {
                Ok(f) => f,
                Err(e) => {
                    warn!(error = %e, ?anomalies_path, "cannot open anomalies file");
                    return;
                }
            };
            let mut prev = counters.snapshot();
            let mut prev_diff_dropped: u64 = 0;
            while !stop.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_secs(1));
                let cur = counters.snapshot();
                let now_ns = anchor.elapsed().as_nanos() as u64;
                for (name, delta) in cur.deltas_vs(&prev) {
                    let line = serde_json::json!({
                        "ts_ns": now_ns,
                        "kind": "channel_full_drop_or_error",
                        "counter": name,
                        "count_delta": delta,
                    });
                    let _ = writeln!(file, "{}", line);
                }
                let cur_dropped = diff_dropped.load(Ordering::Relaxed);
                if cur_dropped > prev_diff_dropped {
                    let line = serde_json::json!({
                        "ts_ns": now_ns,
                        "kind": "diff_channel_full_drop",
                        "count_delta": cur_dropped - prev_diff_dropped,
                    });
                    let _ = writeln!(file, "{}", line);
                    prev_diff_dropped = cur_dropped;
                }
                prev = cur;
            }
        })
        .expect("spawn stats thread")
}
