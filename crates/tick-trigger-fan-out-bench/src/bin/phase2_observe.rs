//! Phase 2 binary — end-to-end pipeline:
//!
//! ```text
//!   SS gRPC ──▶ ss_warmup ──┐
//!                            ├─▶ merger ──▶ poh_supervisor ──▶ OrderedEvent stream
//!   YS gRPC ──▶ ys_warmup ──┘                                        │
//!                                                                    ▼
//!                                                          (consumer: collects metrics
//!                                                           + per-slot reports)
//! ```
//!
//! Phase 2 verifies the supervisor: every event downstream consumers see is
//! in strict PoH order per slot, gaps appear explicitly as `Missing` events,
//! and each observed slot ends with exactly one `SlotComplete` or
//! `SlotIncomplete` event.

use anyhow::Context;
use clap::Parser;
use crossbeam_channel::{bounded, Receiver, Sender};
use entry_sources::shredstream::grpc::ShredStreamGrpcSource;
use entry_sources::yellowstone::YellowstoneSource;
use entry_sources::{DropCounters, EntryObservation, EntrySource};
use serde::Serialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tick_trigger_fan_out_bench::merger::{
    spawn as spawn_merger, MergedEntry, MergerConfig, MergerCounters, MergerCountersSnapshot,
};
use tick_trigger_fan_out_bench::poh_supervisor::{
    spawn as spawn_supervisor, OrderedEvent, PohSupervisorConfig, PohSupervisorCounters,
    PohSupervisorCountersSnapshot,
};

#[derive(Parser)]
#[command(version, about = "Phase 2: SS+YS → merger → PoH supervisor → ordered event stream")]
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
    /// How long the supervisor waits for a missing entry_index before
    /// emitting a `Missing` marker and skipping forward.
    #[arg(long, default_value = "50ms")]
    entry_timeout: humantime::Duration,
    /// How many slots behind the highest-seen slot before sealing.
    #[arg(long, default_value_t = 5)]
    slot_seal_lag_slots: u64,
}

#[derive(Default, Serialize, Clone)]
struct SlotSummary {
    slot: u64,
    is_complete: bool,
    is_incomplete: bool,
    total_entries: u32,
    missing_count: u32,
    last_tick_observed: u8,
}

#[derive(Serialize)]
struct FullReport {
    elapsed_secs: f64,
    merger: MergerCountersSnapshot,
    supervisor: PohSupervisorCountersSnapshot,
    sample_slots: Vec<SlotSummary>,
    ss_warmup_dropped: u64,
    ys_warmup_dropped: u64,
    ss_first_full_slot: u64,
    ys_first_full_slot: u64,
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
    let entry_timeout: Duration = args.entry_timeout.into();
    tracing::info!(
        ?duration,
        ?entry_timeout,
        slot_seal_lag = args.slot_seal_lag_slots,
        "phase2_observe starting"
    );

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

    // Warmup gates (drop each source's partial first slot).
    let (ss_clean_tx, ss_clean_rx) = bounded::<EntryObservation>(args.source_channel_capacity);
    let ss_warmup_drops = Arc::new(AtomicU64::new(0));
    let ss_first_full_slot = Arc::new(AtomicU64::new(0));
    let _ss_warmup = spawn_warmup(
        "ss-warmup",
        ss_raw_rx,
        ss_clean_tx,
        ss_warmup_drops.clone(),
        ss_first_full_slot.clone(),
        stop.clone(),
    );

    let (ys_clean_tx, ys_clean_rx) = bounded::<EntryObservation>(args.source_channel_capacity);
    let ys_warmup_drops = Arc::new(AtomicU64::new(0));
    let ys_first_full_slot = Arc::new(AtomicU64::new(0));
    let _ys_warmup = spawn_warmup(
        "ys-warmup",
        ys_raw_rx,
        ys_clean_tx,
        ys_warmup_drops.clone(),
        ys_first_full_slot.clone(),
        stop.clone(),
    );

    // Merger over warmed-up streams.
    let (merged_tx, merged_rx) = bounded::<MergedEntry>(args.source_channel_capacity);
    let merger_counters = Arc::new(MergerCounters::new());
    let _merger = spawn_merger(MergerConfig {
        ss_rx: ss_clean_rx,
        ys_rx: ys_clean_rx,
        out_tx: merged_tx,
        counters: merger_counters.clone(),
        stop: stop.clone(),
    })?;

    // PoH supervisor.
    let (ordered_tx, ordered_rx) = bounded::<OrderedEvent>(args.source_channel_capacity);
    let supervisor_counters = Arc::new(PohSupervisorCounters::default());
    let _supervisor = spawn_supervisor(PohSupervisorConfig {
        merged_rx,
        out_tx: ordered_tx,
        entry_timeout,
        slot_seal_lag_slots: args.slot_seal_lag_slots,
        max_pending_per_slot: 1024,
        tick_check_interval: Duration::from_millis(10),
        counters: supervisor_counters.clone(),
        stop: stop.clone(),
    })?;

    // Consumer: collects per-slot summaries + sanity-checks ordering.
    let slot_summaries: Arc<parking_lot::Mutex<HashMap<u64, SlotSummary>>> =
        Arc::new(parking_lot::Mutex::new(HashMap::new()));
    let ordering_violations = Arc::new(AtomicU64::new(0));
    let _consumer = spawn_consumer(
        ordered_rx,
        slot_summaries.clone(),
        ordering_violations.clone(),
        stop.clone(),
    );

    // Ctrl-C
    let stop_for_ctrlc = stop.clone();
    ctrlc::set_handler(move || {
        tracing::info!("Ctrl-C received");
        stop_for_ctrlc.store(true, Ordering::Relaxed);
    })
    .ok();

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
                "warmup complete"
            );
        }
        if now >= next_summary {
            next_summary = now + Duration::from_secs(args.summary_interval_secs);
            let m = merger_counters.snapshot();
            let s = supervisor_counters.snapshot();
            tracing::info!(
                "t={:.1}s | recv ss={} ys={} | sup: imm={} reord={} miss={} | slots: comp={} inc={} w_miss={} | order_viol={}",
                now.duration_since(start).as_secs_f64(),
                m.ss_received, m.ys_received,
                s.entries_emitted_immediate, s.entries_emitted_reordered, s.entries_missing_timeout,
                s.slots_complete, s.slots_incomplete, s.slots_with_missing,
                ordering_violations.load(Ordering::Relaxed),
            );
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    stop.store(true, Ordering::Relaxed);
    std::thread::sleep(Duration::from_millis(200)); // let supervisor flush

    let total_elapsed = start.elapsed();
    let summaries = slot_summaries.lock();
    let mut sample: Vec<SlotSummary> = summaries.values().cloned().collect();
    sample.sort_by_key(|s| s.slot);

    let full = FullReport {
        elapsed_secs: total_elapsed.as_secs_f64(),
        merger: merger_counters.snapshot(),
        supervisor: supervisor_counters.snapshot(),
        sample_slots: sample.clone(),
        ss_warmup_dropped: ss_warmup_drops.load(Ordering::Relaxed),
        ys_warmup_dropped: ys_warmup_drops.load(Ordering::Relaxed),
        ss_first_full_slot: ss_first_full_slot.load(Ordering::Relaxed),
        ys_first_full_slot: ys_first_full_slot.load(Ordering::Relaxed),
    };

    print_report(&full, ordering_violations.load(Ordering::Relaxed));

    if let Some(path) = &args.output {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        std::fs::write(path, serde_json::to_string_pretty(&full)?)
            .context("write output report")?;
        println!("\nFull report written to: {}", path.display());
    }

    Ok(())
}

fn print_report(r: &FullReport, ordering_violations: u64) {
    let sup = &r.supervisor;
    let total_emitted = sup.entries_emitted_immediate + sup.entries_emitted_reordered;
    let reorder_pct = if total_emitted > 0 {
        sup.entries_emitted_reordered as f64 / total_emitted as f64 * 100.0
    } else {
        0.0
    };
    let avg_wait_us = if sup.entries_emitted_reordered > 0 {
        sup.reorder_wait_sum_us as f64 / sup.entries_emitted_reordered as f64
    } else {
        0.0
    };

    println!("\n=== PHASE 2 FINAL REPORT ({:.1}s) ===", r.elapsed_secs);
    println!();
    println!("--- Pipeline integrity ---");
    println!("Ordering violations downstream : {} (must be 0)", ordering_violations);
    println!("Supervisor late-after-seal     : {}", sup.late_arrivals_after_seal);
    println!("Supervisor output full         : {}", sup.output_full);
    println!("Merger duplicates              : {}", r.merger.duplicates);
    println!();
    println!("--- Entries ---");
    println!("Total emitted     : {}", total_emitted);
    println!("  immediate       : {}", sup.entries_emitted_immediate);
    println!("  reordered       : {} ({:.2}% of emitted)", sup.entries_emitted_reordered, reorder_pct);
    println!(
        "  reorder wait    : avg={:.0}us max={}us",
        avg_wait_us, sup.reorder_wait_max_us
    );
    println!("Missing markers   : {}", sup.entries_missing_timeout);
    println!("Pending peak size : {}", sup.pending_peak_size);
    println!("Duplicates (sup)  : {}", sup.duplicates_dropped);
    println!();
    println!("--- Slots ---");
    println!("Complete   : {}", sup.slots_complete);
    println!("Incomplete : {}", sup.slots_incomplete);
    println!("With Missing : {} ({:.1}%)",
        sup.slots_with_missing,
        if sup.slots_complete + sup.slots_incomplete > 0 {
            sup.slots_with_missing as f64
                / (sup.slots_complete + sup.slots_incomplete) as f64
                * 100.0
        } else {
            0.0
        }
    );
    println!();
    println!("--- Merger race / latency ---");
    println!("SS won race : {}", r.merger.ss_first);
    println!("YS won race : {}", r.merger.ys_first);
    if r.merger.confirmed_by_both > 0 {
        println!(
            "Inter-source latency : avg={:.0}us min={:.0}us max={:.0}us",
            r.merger.confirm_latency_avg_us().unwrap_or(0.0),
            r.merger.confirm_latency_min_ns as f64 / 1000.0,
            r.merger.confirm_latency_max_ns as f64 / 1000.0,
        );
    }
}

fn spawn_warmup(
    name: &'static str,
    rx: Receiver<EntryObservation>,
    tx: Sender<EntryObservation>,
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
                            let _ = tx.try_send(obs);
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

fn spawn_consumer(
    rx: Receiver<OrderedEvent>,
    summaries: Arc<parking_lot::Mutex<HashMap<u64, SlotSummary>>>,
    ordering_violations: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name("consumer".into())
        .spawn(move || {
            // Per-slot next-expected-index check (would catch a supervisor bug
            // where entries leak out of order).
            let mut expected_next: HashMap<u64, u32> = HashMap::new();
            loop {
                if stop.load(Ordering::Relaxed) {
                    // Drain anything still queued before exiting.
                    while let Ok(ev) = rx.try_recv() {
                        process_event(ev, &mut expected_next, &summaries, &ordering_violations);
                    }
                    break;
                }
                match rx.recv_timeout(Duration::from_millis(200)) {
                    Ok(ev) => process_event(ev, &mut expected_next, &summaries, &ordering_violations),
                    Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
                    Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
                }
            }
        })
        .expect("spawn consumer")
}

fn process_event(
    ev: OrderedEvent,
    expected_next: &mut HashMap<u64, u32>,
    summaries: &Arc<parking_lot::Mutex<HashMap<u64, SlotSummary>>>,
    ordering_violations: &Arc<AtomicU64>,
) {
    match ev {
        OrderedEvent::Entry(e) => {
            let exp = expected_next.entry(e.slot).or_insert(0);
            if e.entry_index != *exp {
                ordering_violations.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    slot = e.slot,
                    expected = *exp,
                    actual = e.entry_index,
                    "ordering violation downstream"
                );
            }
            *exp = e.entry_index + 1;
        }
        OrderedEvent::Missing(m) => {
            let exp = expected_next.entry(m.slot).or_insert(0);
            if m.entry_index != *exp {
                ordering_violations.fetch_add(1, Ordering::Relaxed);
            }
            *exp = m.entry_index + 1;
        }
        OrderedEvent::SlotComplete(c) => {
            let mut s = summaries.lock();
            s.insert(
                c.slot,
                SlotSummary {
                    slot: c.slot,
                    is_complete: true,
                    is_incomplete: false,
                    total_entries: c.total_entries,
                    missing_count: c.missing_count,
                    last_tick_observed: c.last_tick_observed,
                },
            );
        }
        OrderedEvent::SlotIncomplete(i) => {
            let mut s = summaries.lock();
            s.insert(
                i.slot,
                SlotSummary {
                    slot: i.slot,
                    is_complete: false,
                    is_incomplete: true,
                    total_entries: 0,
                    missing_count: i.missing_count,
                    last_tick_observed: i.last_tick_observed,
                },
            );
        }
    }
}
