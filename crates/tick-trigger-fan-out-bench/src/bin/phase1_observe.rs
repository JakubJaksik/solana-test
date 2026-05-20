//! Phase 1 binary — verifies that SS+YS sources are reliable enough to build
//! the rest of the pipeline on top.
//!
//! Usage:
//!   phase1_observe \
//!     --ss-url http://127.0.0.1:9999 \
//!     --ys-url https://your-helius.com:2053 \
//!     --ys-token <uuid> \
//!     --duration 60s \
//!     --output runs/phase1-20260520/report.json
//!
//! The binary connects to both sources, runs the entry merger, and prints
//! periodic (every 5s) one-line summaries. On exit (duration elapsed or Ctrl-C)
//! it dumps a full JSON report with per-source counters, per-slot ordering
//! breakdown, and derived percentages.

use anyhow::Context;
use clap::Parser;
use crossbeam_channel::{bounded, Receiver};
use entry_sources::{DropCounters, EntrySource};
use entry_sources::shredstream::grpc::ShredStreamGrpcSource;
use entry_sources::yellowstone::YellowstoneSource;
use serde::Serialize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tick_trigger_fan_out_bench::merger::{spawn as spawn_merger, MergedEntry, MergerConfig, MergerCounters};
use tick_trigger_fan_out_bench::ordering::{OrderingCounters, OrderingTracker, SlotOrderingReport};
use tick_trigger_fan_out_bench::phase1_metrics::{build_report, one_line, Phase1Report};

#[derive(Parser)]
#[command(version, about = "Phase 1: verify SS+YS source reliability")]
struct Args {
    /// ShredStream gRPC URL (e.g. http://127.0.0.1:9999).
    #[arg(long, env = "SS_URL")]
    ss_url: String,
    /// Yellowstone gRPC URL (e.g. https://provider.example:2053).
    #[arg(long, env = "YS_URL")]
    ys_url: String,
    /// Yellowstone auth token / API key (UUID).
    #[arg(long, env = "YS_TOKEN", default_value = "")]
    ys_token: String,
    /// How long to observe (humantime, e.g. 60s, 5m).
    #[arg(long, default_value = "60s")]
    duration: humantime::Duration,
    /// Where to write the final JSON report. Optional; if omitted the report
    /// is still printed to stdout on shutdown.
    #[arg(long)]
    output: Option<PathBuf>,
    /// Channel capacity for the SS/YS receivers.
    #[arg(long, default_value_t = 65536)]
    source_channel_capacity: usize,
    /// How often (seconds) to print a one-line summary during the run.
    #[arg(long, default_value_t = 5)]
    summary_interval_secs: u64,
}

#[derive(Serialize)]
struct FullReport {
    summary: Phase1Report,
    per_slot: Vec<SlotOrderingReport>,
    /// String dumps of the SS/YS drop counters (their snapshot type does not
    /// derive `Serialize`, so we use Debug-format here).
    ss_drops_debug: String,
    ys_drops_debug: String,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let duration: Duration = args.duration.into();
    tracing::info!(?duration, ss_url = %args.ss_url, ys_url = %args.ys_url, "phase1_observe starting");

    // --- Sources ---
    let ss_counters = Arc::new(DropCounters::default());
    let ss_src = Box::new(ShredStreamGrpcSource {
        endpoint: args.ss_url.clone(),
        channel_capacity: args.source_channel_capacity,
        pinned_core: None,
        counters: ss_counters.clone(),
    });
    let ss_rx = ss_src.start().context("start shredstream source")?;
    tracing::info!("shredstream source started");

    let ys_counters = Arc::new(DropCounters::default());
    let ys_src = Box::new(YellowstoneSource {
        url: args.ys_url.clone(),
        token: if args.ys_token.is_empty() { None } else { Some(args.ys_token.clone()) },
        channel_capacity: args.source_channel_capacity,
        pinned_core: None,
        counters: ys_counters.clone(),
    });
    let ys_rx = ys_src.start().context("start yellowstone source")?;
    tracing::info!("yellowstone source started");

    // --- Merger ---
    let (merged_tx, merged_rx) = bounded::<MergedEntry>(args.source_channel_capacity);
    let merger_counters = Arc::new(MergerCounters::new());
    let stop = Arc::new(AtomicBool::new(false));
    let _merger_handle = spawn_merger(MergerConfig {
        ss_rx,
        ys_rx,
        out_tx: merged_tx,
        counters: merger_counters.clone(),
        stop: stop.clone(),
    })?;
    tracing::info!("entry-merger spawned");

    // --- Ordering tracker (consumes primary emissions) ---
    let ordering_counters = Arc::new(OrderingCounters::default());
    let tracker = Arc::new(OrderingTracker::new(ordering_counters.clone(), 5));
    let tracker_handle = spawn_tracker(merged_rx, tracker.clone(), stop.clone());

    // --- Ctrl-C handler ---
    let stop_for_ctrlc = stop.clone();
    ctrlc::set_handler(move || {
        tracing::info!("Ctrl-C received, signalling shutdown");
        stop_for_ctrlc.store(true, Ordering::Relaxed);
    }).ok();

    // --- Main loop: periodic one-line summaries until duration elapses or stop. ---
    let start = Instant::now();
    let mut next_summary = start + Duration::from_secs(args.summary_interval_secs);
    while !stop.load(Ordering::Relaxed) {
        let now = Instant::now();
        if now >= start + duration {
            break;
        }
        if now >= next_summary {
            next_summary = now + Duration::from_secs(args.summary_interval_secs);
            let report = build_report(
                now.duration_since(start),
                merger_counters.snapshot(),
                ordering_counters.snapshot(),
            );
            tracing::info!("{}", one_line(&report));
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    stop.store(true, Ordering::Relaxed);
    let _ = tracker_handle.join();

    // Flush any in-progress slots that didn't get a high-water seal during the run.
    let final_flush = tracker.flush_all();
    tracing::info!(flushed_slots = final_flush.len(), "flushed remaining in-progress slots");

    let total_elapsed = start.elapsed();
    let final_summary = build_report(
        total_elapsed,
        merger_counters.snapshot(),
        ordering_counters.snapshot(),
    );

    let ss_drops_snap = ss_counters.snapshot();
    let ys_drops_snap = ys_counters.snapshot();
    let full = FullReport {
        summary: final_summary.clone(),
        per_slot: tracker.sealed_reports(),
        ss_drops_debug: format!("{:?}", ss_drops_snap),
        ys_drops_debug: format!("{:?}", ys_drops_snap),
    };

    println!("\n=== PHASE 1 FINAL REPORT ===");
    println!("{}", one_line(&final_summary));
    println!();
    println!("SS receive total       : {}", final_summary.merger.ss_received);
    println!("YS receive total       : {}", final_summary.merger.ys_received);
    println!("Unique entries (primary): {}", final_summary.derived.unique_entries);
    println!(
        "  SS first              : {} ({:.1}%)",
        final_summary.merger.ss_first, final_summary.derived.ss_first_rate * 100.0
    );
    println!(
        "  YS first              : {} ({:.1}%)",
        final_summary.merger.ys_first, (1.0 - final_summary.derived.ss_first_rate) * 100.0
    );
    println!(
        "Both sources confirmed : {} ({:.1}%)",
        final_summary.derived.both_sources_confirmed,
        final_summary.derived.both_confirm_rate * 100.0
    );
    if let Some(avg_us) = final_summary.derived.confirm_latency_avg_us {
        println!(
            "Inter-source latency   : avg={:.0}us min={:.0}us max={:.0}us",
            avg_us,
            final_summary.derived.confirm_latency_min_us.unwrap_or(0.0),
            final_summary.derived.confirm_latency_max_us.unwrap_or(0.0),
        );
    } else {
        println!("Inter-source latency   : (no confirmations yet)");
    }
    println!("Duplicates dropped     : {}", final_summary.merger.duplicates);
    println!("Merger out_tx full     : {}", final_summary.merger.output_full);
    println!();
    println!("Slots sealed           : {}", final_summary.ordering.slots_sealed);
    println!(
        "  fully ordered         : {} ({:.1}%)",
        final_summary.ordering.slots_fully_ordered, final_summary.derived.fully_ordered_rate * 100.0
    );
    println!(
        "  with disorder         : {} (avg out-of-order per slot: {:.2})",
        final_summary.ordering.slots_with_disorder, final_summary.derived.avg_out_of_order_per_slot
    );
    println!(
        "  with index gaps       : {} (total missing: {})",
        final_summary.ordering.slots_with_gaps, final_summary.ordering.total_missing_indices
    );
    println!(
        "  ended on tick (64 .)  : {} ({:.1}%)",
        final_summary.ordering.slots_ending_on_tick, final_summary.derived.tick_ending_rate * 100.0
    );
    println!();
    println!("SS source drops        : {}", full.ss_drops_debug);
    println!("YS source drops        : {}", full.ys_drops_debug);

    if let Some(path) = &args.output {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        let json = serde_json::to_string_pretty(&full)?;
        std::fs::write(path, json).context("write output report")?;
        println!("\nFull report written to: {}", path.display());
    }

    Ok(())
}

fn spawn_tracker(
    rx: Receiver<MergedEntry>,
    tracker: Arc<OrderingTracker>,
    stop: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name("ordering-tracker".into())
        .spawn(move || {
            loop {
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                match rx.recv_timeout(Duration::from_millis(200)) {
                    Ok(merged) => {
                        // Merger emits exactly one MergedEntry per unique (slot, hash) —
                        // feed every emission to the ordering tracker.
                        tracker.observe(&merged.observation);
                    }
                    Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
                    Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
                }
            }
        })
        .expect("spawn ordering-tracker thread")
}
