//! Phase 1 binary — verifies SS+YS source reliability.
//!
//! Pipeline:
//!
//! ```text
//!   SS gRPC ──▶ ss_warmup ──┬─▶ merger ──▶ unified stream
//!                            └─▶ ss-ordering tracker
//!   YS gRPC ──▶ ys_warmup ──┬─▶ merger
//!                            └─▶ ys-ordering tracker
//! ```
//!
//! Each source feeds its OWN ordering tracker so we get an honest answer to:
//! "does this source deliver entries in correct PoH order?". The merger keeps
//! providing the unified downstream stream (one MergedEntry per unique
//! `(slot, entry_hash)`); it is not analyzed for ordering here because winner
//! selection would mix indices from two sources, masking per-source bugs.
//!
//! Warmup: each source drops observations until it crosses its first slot
//! boundary, so we don't include partial slots in the report.

use anyhow::Context;
use clap::Parser;
use crossbeam_channel::{bounded, Receiver, Sender};
use entry_sources::shredstream::grpc::ShredStreamGrpcSource;
use entry_sources::yellowstone::YellowstoneSource;
use entry_sources::{DropCounters, EntryObservation, EntrySource};
use serde::Serialize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tick_trigger_fan_out_bench::merger::{
    spawn as spawn_merger, MergedEntry, MergerConfig, MergerCounters, MergerCountersSnapshot,
};
use tick_trigger_fan_out_bench::ordering::{
    OrderingCounters, OrderingCountersSnapshot, OrderingTracker, SlotOrderingReport,
};

#[derive(Parser)]
#[command(version, about = "Phase 1: verify SS+YS source reliability")]
struct Args {
    #[arg(long, env = "SS_URL")]
    ss_url: String,
    #[arg(long, env = "YS_URL")]
    ys_url: String,
    #[arg(long, env = "YS_TOKEN", default_value = "")]
    ys_token: String,
    #[arg(long, default_value = "60s")]
    duration: humantime::Duration,
    #[arg(long)]
    output: Option<PathBuf>,
    #[arg(long, default_value_t = 65536)]
    source_channel_capacity: usize,
    #[arg(long, default_value_t = 5)]
    summary_interval_secs: u64,
}

#[derive(Serialize)]
struct FullReport {
    elapsed_secs: f64,
    merger: MergerCountersSnapshot,
    ss_ordering: OrderingCountersSnapshot,
    ys_ordering: OrderingCountersSnapshot,
    unified_ordering: OrderingCountersSnapshot,
    ss_per_slot: Vec<SlotOrderingReport>,
    ys_per_slot: Vec<SlotOrderingReport>,
    unified_per_slot: Vec<SlotOrderingReport>,
    ss_warmup_dropped: u64,
    ys_warmup_dropped: u64,
    ss_first_full_slot: u64,
    ys_first_full_slot: u64,
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

    // --- Raw sources ---
    let ss_counters = Arc::new(DropCounters::default());
    let ss_raw_rx = Box::new(ShredStreamGrpcSource {
        endpoint: args.ss_url.clone(),
        channel_capacity: args.source_channel_capacity,
        pinned_core: None,
        counters: ss_counters.clone(),
    })
    .start()
    .context("start shredstream source")?;

    let ys_counters = Arc::new(DropCounters::default());
    let ys_raw_rx = Box::new(YellowstoneSource {
        url: args.ys_url.clone(),
        token: if args.ys_token.is_empty() {
            None
        } else {
            Some(args.ys_token.clone())
        },
        channel_capacity: args.source_channel_capacity,
        pinned_core: None,
        counters: ys_counters.clone(),
    })
    .start()
    .context("start yellowstone source")?;

    let stop = Arc::new(AtomicBool::new(false));

    // --- Warmup gates with fanout to (merger, per-source ordering tracker) ---
    let (ss_merger_tx, ss_merger_rx) = bounded::<EntryObservation>(args.source_channel_capacity);
    let (ss_ord_tx, ss_ord_rx) = bounded::<EntryObservation>(args.source_channel_capacity);
    let ss_warmup_drops = Arc::new(AtomicU64::new(0));
    let ss_first_full_slot = Arc::new(AtomicU64::new(0));
    let _ss_warmup = spawn_warmup_fanout(
        "ss-warmup",
        ss_raw_rx,
        vec![ss_merger_tx, ss_ord_tx],
        ss_warmup_drops.clone(),
        ss_first_full_slot.clone(),
        stop.clone(),
    );

    let (ys_merger_tx, ys_merger_rx) = bounded::<EntryObservation>(args.source_channel_capacity);
    let (ys_ord_tx, ys_ord_rx) = bounded::<EntryObservation>(args.source_channel_capacity);
    let ys_warmup_drops = Arc::new(AtomicU64::new(0));
    let ys_first_full_slot = Arc::new(AtomicU64::new(0));
    let _ys_warmup = spawn_warmup_fanout(
        "ys-warmup",
        ys_raw_rx,
        vec![ys_merger_tx, ys_ord_tx],
        ys_warmup_drops.clone(),
        ys_first_full_slot.clone(),
        stop.clone(),
    );

    // --- Merger over warmed-up streams ---
    let (merged_tx, merged_rx) = bounded::<MergedEntry>(args.source_channel_capacity);
    let merger_counters = Arc::new(MergerCounters::new());
    let _merger_handle = spawn_merger(MergerConfig {
        ss_rx: ss_merger_rx,
        ys_rx: ys_merger_rx,
        out_tx: merged_tx,
        counters: merger_counters.clone(),
        stop: stop.clone(),
    })?;

    // --- Per-source ordering trackers ---
    let ss_ord_counters = Arc::new(OrderingCounters::default());
    let ss_tracker = Arc::new(OrderingTracker::new(ss_ord_counters.clone(), 5));
    let _ss_tracker_handle =
        spawn_obs_tracker("ss-ord", ss_ord_rx, ss_tracker.clone(), stop.clone());

    let ys_ord_counters = Arc::new(OrderingCounters::default());
    let ys_tracker = Arc::new(OrderingTracker::new(ys_ord_counters.clone(), 5));
    let _ys_tracker_handle =
        spawn_obs_tracker("ys-ord", ys_ord_rx, ys_tracker.clone(), stop.clone());

    // --- Unified ordering tracker on merger output.
    //     Per-source trackers prove each source is individually ordered.
    //     This one shows what downstream consumers (future phases) actually
    //     see: the merged stream may be out-of-order if SS-first arrivals
    //     skip an entry that later only YS delivers (~9ms delay).
    let unified_ord_counters = Arc::new(OrderingCounters::default());
    let unified_tracker = Arc::new(OrderingTracker::new(unified_ord_counters.clone(), 5));
    let _unified_tracker_handle =
        spawn_merger_tracker(merged_rx, unified_tracker.clone(), stop.clone());

    // --- Ctrl-C ---
    let stop_for_ctrlc = stop.clone();
    ctrlc::set_handler(move || {
        tracing::info!("Ctrl-C received, signalling shutdown");
        stop_for_ctrlc.store(true, Ordering::Relaxed);
    })
    .ok();

    // --- Main loop ---
    let start = Instant::now();
    let mut next_summary = start + Duration::from_secs(args.summary_interval_secs);
    let mut announced_warmup = false;
    while !stop.load(Ordering::Relaxed) {
        let now = Instant::now();
        if now >= start + duration {
            break;
        }
        if !announced_warmup
            && ss_first_full_slot.load(Ordering::Relaxed) > 0
            && ys_first_full_slot.load(Ordering::Relaxed) > 0
        {
            announced_warmup = true;
            tracing::info!(
                ss = ss_first_full_slot.load(Ordering::Relaxed),
                ys = ys_first_full_slot.load(Ordering::Relaxed),
                ss_dropped = ss_warmup_drops.load(Ordering::Relaxed),
                ys_dropped = ys_warmup_drops.load(Ordering::Relaxed),
                "warmup complete"
            );
        }
        if now >= next_summary {
            next_summary = now + Duration::from_secs(args.summary_interval_secs);
            let mc = merger_counters.snapshot();
            let s = ss_ord_counters.snapshot();
            let y = ys_ord_counters.snapshot();
            tracing::info!(
                "t={:.1}s | recv ss={} ys={} | SS ord={:.0}% tick={:.0}% ooo_avg={:.2} ({}/{}) | YS ord={:.0}% tick={:.0}% ooo_avg={:.2} ({}/{})",
                now.duration_since(start).as_secs_f64(),
                mc.ss_received, mc.ys_received,
                pct(s.slots_fully_ordered, s.slots_sealed),
                pct(s.slots_ending_on_tick, s.slots_sealed),
                avg(s.total_out_of_order, s.slots_sealed),
                s.slots_fully_ordered, s.slots_sealed,
                pct(y.slots_fully_ordered, y.slots_sealed),
                pct(y.slots_ending_on_tick, y.slots_sealed),
                avg(y.total_out_of_order, y.slots_sealed),
                y.slots_fully_ordered, y.slots_sealed,
            );
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    stop.store(true, Ordering::Relaxed);
    ss_tracker.flush_all();
    ys_tracker.flush_all();
    unified_tracker.flush_all();

    let total_elapsed = start.elapsed();
    let ss_drops_snap = ss_counters.snapshot();
    let ys_drops_snap = ys_counters.snapshot();
    let full = FullReport {
        elapsed_secs: total_elapsed.as_secs_f64(),
        merger: merger_counters.snapshot(),
        ss_ordering: ss_ord_counters.snapshot(),
        ys_ordering: ys_ord_counters.snapshot(),
        unified_ordering: unified_ord_counters.snapshot(),
        ss_per_slot: ss_tracker.sealed_reports(),
        ys_per_slot: ys_tracker.sealed_reports(),
        unified_per_slot: unified_tracker.sealed_reports(),
        ss_warmup_dropped: ss_warmup_drops.load(Ordering::Relaxed),
        ys_warmup_dropped: ys_warmup_drops.load(Ordering::Relaxed),
        ss_first_full_slot: ss_first_full_slot.load(Ordering::Relaxed),
        ys_first_full_slot: ys_first_full_slot.load(Ordering::Relaxed),
        ss_drops_debug: format!("{:?}", ss_drops_snap),
        ys_drops_debug: format!("{:?}", ys_drops_snap),
    };

    print_report(&full);

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

fn pct(num: u64, den: u64) -> f64 {
    if den == 0 {
        0.0
    } else {
        num as f64 / den as f64 * 100.0
    }
}
fn avg(num: u64, den: u64) -> f64 {
    if den == 0 {
        0.0
    } else {
        num as f64 / den as f64
    }
}

fn print_report(r: &FullReport) {
    println!("\n=== PHASE 1 FINAL REPORT ({:.1}s) ===", r.elapsed_secs);
    println!();
    println!("--- Warmup ---");
    println!("SS dropped : {} (started at slot {})", r.ss_warmup_dropped, r.ss_first_full_slot);
    println!("YS dropped : {} (started at slot {})", r.ys_warmup_dropped, r.ys_first_full_slot);
    println!();
    println!("--- Merger / race ---");
    println!("SS received : {}", r.merger.ss_received);
    println!("YS received : {}", r.merger.ys_received);
    println!(
        "SS won race : {} ({:.1}%)",
        r.merger.ss_first, pct(r.merger.ss_first, r.merger.ss_first + r.merger.ys_first)
    );
    println!(
        "YS won race : {} ({:.1}%)",
        r.merger.ys_first, pct(r.merger.ys_first, r.merger.ss_first + r.merger.ys_first)
    );
    println!(
        "Both saw    : {}",
        r.merger.confirmed_by_both
    );
    if r.merger.confirmed_by_both > 0 {
        println!(
            "Inter-source latency : avg={:.0}us min={:.0}us max={:.0}us",
            r.merger.confirm_latency_avg_us().unwrap_or(0.0),
            r.merger.confirm_latency_min_ns as f64 / 1000.0,
            r.merger.confirm_latency_max_ns as f64 / 1000.0,
        );
    }
    println!("Duplicates  : {}", r.merger.duplicates);
    println!();
    println!("--- SS ordering (per-source, true PoH check) ---");
    print_section(&r.ss_ordering);
    println!();
    println!("--- YS ordering (per-source, true PoH check) ---");
    print_section(&r.ys_ordering);
    println!();
    println!("--- Unified ordering (merger output, what downstream sees) ---");
    print_section(&r.unified_ordering);
    println!();
    println!("SS drops    : {}", r.ss_drops_debug);
    println!("YS drops    : {}", r.ys_drops_debug);
}

fn print_section(o: &OrderingCountersSnapshot) {
    println!("Slots sealed         : {}", o.slots_sealed);
    println!(
        "  fully ordered      : {} ({:.1}%)",
        o.slots_fully_ordered,
        pct(o.slots_fully_ordered, o.slots_sealed)
    );
    println!(
        "  with disorder      : {} (avg ooo/slot: {:.2})",
        o.slots_with_disorder,
        avg(o.total_out_of_order, o.slots_sealed)
    );
    println!(
        "  with index gaps    : {} (total missing: {})",
        o.slots_with_gaps, o.total_missing_indices
    );
    println!(
        "  ended on tick      : {} ({:.1}%)",
        o.slots_ending_on_tick,
        pct(o.slots_ending_on_tick, o.slots_sealed)
    );
}

fn spawn_warmup_fanout(
    name: &'static str,
    rx: Receiver<EntryObservation>,
    outs: Vec<Sender<EntryObservation>>,
    dropped: Arc<AtomicU64>,
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
                            for tx in outs.iter() {
                                let _ = tx.try_send(obs.clone());
                            }
                            continue;
                        }
                        match startup_slot {
                            None => {
                                startup_slot = Some(obs.slot);
                                dropped.fetch_add(1, Ordering::Relaxed);
                            }
                            Some(s0) if obs.slot <= s0 => {
                                dropped.fetch_add(1, Ordering::Relaxed);
                            }
                            Some(_) => {
                                warm = true;
                                first_full_slot.store(obs.slot, Ordering::Relaxed);
                                for tx in outs.iter() {
                                    let _ = tx.try_send(obs.clone());
                                }
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

fn spawn_obs_tracker(
    name: &'static str,
    rx: Receiver<EntryObservation>,
    tracker: Arc<OrderingTracker>,
    stop: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name(name.into())
        .spawn(move || loop {
            if stop.load(Ordering::Relaxed) {
                break;
            }
            match rx.recv_timeout(Duration::from_millis(200)) {
                Ok(obs) => {
                    let _ = tracker.observe(&obs);
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
            }
        })
        .expect("spawn obs tracker")
}

fn spawn_merger_tracker(
    rx: Receiver<MergedEntry>,
    tracker: Arc<OrderingTracker>,
    stop: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name("unified-tracker".into())
        .spawn(move || loop {
            if stop.load(Ordering::Relaxed) {
                break;
            }
            match rx.recv_timeout(Duration::from_millis(200)) {
                Ok(merged) => {
                    let _ = tracker.observe(&merged.observation);
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
            }
        })
        .expect("spawn unified-tracker")
}
