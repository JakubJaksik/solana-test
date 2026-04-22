//! Engine — main loop wiring: newHead → pre-sign → timed send → track → record.
//!
//! Orchestrates the full measurement run:
//! - subscribes to `newHeads` via [`crate::rpc::WsBlockSubscriber`]
//! - on each new block: resolves previously-sent txs via [`crate::tracker::Tracker`],
//!   then (if plan not complete) pre-signs per-wallet transactions and spawns
//!   timed send tasks targeting `block.timestamp * 1000 + slot_ms`
//! - per send result: writes a Pending / SendError record to JSONL and
//!   accumulates stats; when all results for a block have arrived, feeds
//!   `(successes, total)` into [`AbortTracker`]
//! - on tracker resolution (Target / Late / Dropped): writes a second JSONL
//!   record that overrides the Pending entry (two-stage JSONL, Fix A)
//! - on plan complete or Ctrl+C: drains in-flight, finalizes aggregator,
//!   renders stdout report, and writes CSV + Markdown.

use crate::config::Config;
use crate::report::{InclusionKind, JsonlWriter, SlotAggregator, TxRecord};
use crate::rpc::{HttpRpcClient, SendOutcome, WsBlockSubscriber, build_send_payload};
use crate::scheduler::Schedule;
use crate::swap::{PingPongState, SwapEncoder};
use crate::time::{hybrid_sleep_until_with_window, now_unix_ms, target_instant_from_unix_ms};
use crate::tracker::{InclusionStatus, Tracker};
use crate::wallet::{TxParams, Wallet};

use alloy::primitives::{Address, B256, U256};
use alloy::rpc::types::Header;
use anyhow::{Context, Result};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinSet;
use tracing::{error, info, warn};

// ── AbortTracker ────────────────────────────────────────────────────────────

/// Tracks consecutive blocks where all sends failed.
///
/// Increments `consecutive` when a block has at least one send attempt
/// and none of them were successful. Any block with a single success
/// resets the counter. [`Self::should_abort`] returns `true` once the
/// counter reaches or exceeds the configured threshold.
pub struct AbortTracker {
    threshold: u64,
    pub consecutive: u64,
}

impl AbortTracker {
    pub fn new(threshold: u64) -> Self {
        Self {
            threshold,
            consecutive: 0,
        }
    }

    /// Feed the counters for a completed block (i.e. after all of its
    /// scheduled send_results have been received).
    pub fn record_block(&mut self, successful_sends: u64, total_sends: u64) {
        if total_sends > 0 && successful_sends == 0 {
            self.consecutive += 1;
        } else {
            self.consecutive = 0;
        }
    }

    pub fn should_abort(&self) -> bool {
        self.consecutive >= self.threshold
    }
}

// ── Public engine context ───────────────────────────────────────────────────

/// Shared state passed into [`run`]. Built by the CLI after preflight.
pub struct EngineContext {
    pub cfg: Arc<Config>,
    /// HTTP client do read calls (chain_id, balance, block, receipts, etc.)
    pub http: Arc<HttpRpcClient>,
    /// HTTP client do eth_sendRawTransaction. Może być === http, albo dedykowany
    /// endpoint typu sequencer.base.org dla niższej latency send path.
    pub send_http: Arc<HttpRpcClient>,
    pub wallets: Arc<Vec<Wallet>>,
    pub encoders: Arc<Vec<(String, SwapEncoder, PingPongState)>>,
    pub schedule: Arc<Schedule>,
    pub gas_limits: Arc<Vec<(String, u64)>>,
    pub output_dir: PathBuf,
}

// ── Internal record types ───────────────────────────────────────────────────

/// Cached fields from the initial send_result, held until the tracker resolves
/// the transaction's inclusion status. Enables "two-stage" JSONL where a
/// Pending row is emitted on send and a resolution row on inclusion/drop.
#[derive(Clone)]
struct PartialRecord {
    block_idx: u64,
    block_num: u64,
    block_hash: String,
    slot_ms: u64,
    sample_idx: u64,
    wallet: String,
    nonce: u64,
    target_unix_ms: i64,
    sent_at_unix_ms: i64,
    wake_jitter_us: u64,
    rpc_rtt_us: u64,
    send_result: String,
    tx_hash: String,
}

impl PartialRecord {
    fn into_tx_record(self, inclusion: InclusionKind, included_block: Option<u64>) -> TxRecord {
        TxRecord {
            block_idx: self.block_idx,
            block_num: self.block_num,
            block_hash: self.block_hash,
            slot_ms: self.slot_ms,
            sample_idx: self.sample_idx,
            wallet: self.wallet,
            tx_hash: Some(self.tx_hash),
            nonce: self.nonce,
            target_unix_ms: self.target_unix_ms,
            sent_at_unix_ms: self.sent_at_unix_ms,
            wake_jitter_us: self.wake_jitter_us,
            rpc_rtt_us: self.rpc_rtt_us,
            send_result: self.send_result,
            inclusion,
            included_block,
        }
    }
}

/// Per-wallet bundle handed to a spawned send task.
#[derive(Clone)]
struct ScheduledSend {
    wallet_label: String,
    tx_hash: B256,
    payload: String,
    target_instant: Instant,
    target_unix_ms: i64,
    block_idx: u64,
    block_num: u64,
    block_hash: String,
    slot_ms: u64,
    sample_idx: u64,
    nonce: u64,
}

/// Message sent back from a send task to the main loop.
#[derive(Debug)]
struct SendResult {
    wallet_label: String,
    tx_hash: B256,
    block_idx: u64,
    block_num: u64,
    block_hash: String,
    slot_ms: u64,
    sample_idx: u64,
    nonce: u64,
    target_unix_ms: i64,
    sent_at_unix_ms: i64,
    wake_jitter_us: u64,
    rpc_rtt_us: u64,
    outcome: SendOutcome,
}

/// Per-block counters for wiring [`AbortTracker`] (Fix B).
#[derive(Default, Clone, Copy)]
struct BlockSendCounters {
    received: u64,
    successes: u64,
    total: u64,
}

// ── Main entry point ────────────────────────────────────────────────────────

/// Run the main engine loop until the plan completes, the abort tracker trips,
/// or the user sends Ctrl+C. On exit, finalize the aggregator and emit CSV +
/// Markdown reports alongside the JSONL log.
pub async fn run(ctx: EngineContext) -> Result<()> {
    // ── Wire channels & WS subscription ────────────────────────────────────
    let (header_tx, mut header_rx) = mpsc::channel::<Header>(32);
    let (send_result_tx, mut send_result_rx) = mpsc::channel::<SendResult>(1024);

    info!(url = %ctx.cfg.chain.rpc_ws, "connecting WS subscriber");
    let ws = WsBlockSubscriber::new(&ctx.cfg.chain.rpc_ws);
    let _ws_handle = ws
        .spawn_stream(header_tx)
        .await
        .context("WS subscription failed")?;

    // ── Shared state ──────────────────────────────────────────────────────
    let tracker = Arc::new(Mutex::new(Tracker::new(
        ctx.cfg.tracking.inclusion_lookahead_blocks,
    )));
    let agg = Arc::new(Mutex::new(SlotAggregator::new()));

    // Ensure output directory exists.
    std::fs::create_dir_all(&ctx.output_dir)
        .with_context(|| format!("create output dir {:?}", ctx.output_dir))?;

    let jsonl_path = ctx.output_dir.join("tx_log.jsonl");
    let jsonl = Arc::new(Mutex::new(
        JsonlWriter::create(&jsonl_path).context("open tx_log.jsonl")?,
    ));
    let pending_records: Arc<Mutex<HashMap<B256, PartialRecord>>> =
        Arc::new(Mutex::new(HashMap::new()));

    let mut abort_tracker = AbortTracker::new(ctx.cfg.tracking.abort_on_consecutive_failed_blocks);
    let mut block_send_totals: HashMap<u64, BlockSendCounters> = HashMap::new();

    let mut block_index: u64 = 0;
    let total_blocks = ctx.schedule.total_blocks();
    let mut send_set: JoinSet<()> = JoinSet::new();

    info!(total_blocks, "engine starting main loop");

    // ── Main select loop ──────────────────────────────────────────────────
    loop {
        tokio::select! {
            Some(header) = header_rx.recv() => {
                let block_num = header.inner.number;
                let block_hash = format!("{:?}", header.hash);
                let block_timestamp: u64 = header.inner.timestamp;

                // Resolve inclusions via tracker.
                let tx_hashes_vec = match ctx.http.eth_get_block_tx_hashes(block_num).await {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(block_num, error = %e, "failed to fetch block tx hashes");
                        Vec::new()
                    }
                };
                let tx_set: HashSet<B256> = tx_hashes_vec.into_iter().collect();
                let resolved = {
                    let mut t = tracker.lock().await;
                    t.observe_block(block_num, &tx_set);
                    t.drain_resolved()
                };

                // Emit Fix A second-stage records for newly resolved txs.
                for r in resolved {
                    let mut pm = pending_records.lock().await;
                    let partial_opt = pm.remove(&r.tx_hash);
                    drop(pm);
                    if let Some(part) = partial_opt {
                        let inclusion = match r.status {
                            InclusionStatus::IncludedTarget => InclusionKind::Target,
                            InclusionStatus::IncludedLate(off) => InclusionKind::Late(off),
                            InclusionStatus::Dropped => InclusionKind::Dropped,
                            // Pending / SendError shouldn't land in drain_resolved for
                            // records we stored as pending, but stay defensive.
                            InclusionStatus::Pending { .. } => continue,
                            InclusionStatus::SendError(_) => continue,
                        };
                        let rec = part.into_tx_record(inclusion, r.included_block);
                        if let Err(e) = jsonl.lock().await.write(&rec) {
                            warn!(error = %e, "JSONL write failed (resolved)");
                        }
                        agg.lock().await.ingest(&rec);
                    }
                }

                // Plan complete? Break after emitting resolutions for this block.
                if block_index >= total_blocks {
                    info!("plan complete — waiting for in-flight to finish");
                    break;
                }

                // Compute this block's slot_ms / sample_idx / target instant.
                let slot_ms = ctx
                    .schedule
                    .slot_ms_for(block_index)
                    .expect("block_index < total_blocks");
                let sample_idx = ctx
                    .schedule
                    .sample_idx_for(block_index)
                    .expect("block_index < total_blocks");

                let received_instant = Instant::now();
                let received_unix_ms = now_unix_ms();
                let target_unix_ms = (block_timestamp as i64) * 1000 + slot_ms as i64;
                if target_unix_ms <= received_unix_ms {
                    warn!(
                        block_num,
                        slot_ms,
                        target_unix_ms,
                        received_unix_ms,
                        "missed target (newHead late); skipping block_index"
                    );
                    block_index += 1;
                    continue;
                }
                let target_instant =
                    target_instant_from_unix_ms(received_instant, received_unix_ms, target_unix_ms);

                // Gas price calculation based on this block's base fee.
                let base_fee = header.inner.base_fee_per_gas.unwrap_or(0) as u128;
                let tip = (ctx.cfg.gas.max_priority_fee_gwei * 1e9) as u128;
                let max_fee = (base_fee as f64 * ctx.cfg.gas.max_fee_multiplier) as u128 + tip;

                let router: Address = match ctx.cfg.swap.router_address.parse() {
                    Ok(a) => a,
                    Err(e) => {
                        error!(error = %e, "bad router address in config");
                        break;
                    }
                };

                // Pre-sign per-wallet batch.
                let mut scheduled: Vec<ScheduledSend> = Vec::new();
                for (i, w) in ctx.wallets.iter().enumerate() {
                    let (_, encoder, state) = &ctx.encoders[i];
                    let data = match encoder.encode(state.current_direction(), block_index) {
                        Ok(d) => d,
                        Err(e) => {
                            error!(wallet = %w.label(), error = %e, "calldata encode failed");
                            continue;
                        }
                    };
                    state.advance();
                    let nonce = w.consume_nonce();
                    let gas_limit = ctx.gas_limits[i].1;
                    let signed = match w.sign_eip1559(TxParams {
                        chain_id: ctx.cfg.chain.chain_id,
                        nonce,
                        to: router,
                        value: U256::ZERO,
                        data,
                        gas_limit,
                        max_priority_fee_per_gas: tip,
                        max_fee_per_gas: max_fee,
                    }) {
                        Ok(s) => s,
                        Err(e) => {
                            error!(wallet = %w.label(), error = %e, "sign failed");
                            continue;
                        }
                    };
                    let raw_hex = hex_0x(&signed.raw);
                    let payload = build_send_payload(block_index, &raw_hex);
                    scheduled.push(ScheduledSend {
                        wallet_label: w.label().to_string(),
                        tx_hash: signed.tx_hash,
                        payload,
                        target_instant,
                        target_unix_ms,
                        block_idx: block_index,
                        block_num,
                        block_hash: block_hash.clone(),
                        slot_ms,
                        sample_idx,
                        nonce,
                    });
                }

                // Register pre-signed txs in tracker.
                {
                    let mut t = tracker.lock().await;
                    for s in &scheduled {
                        t.record_sent(s.tx_hash, block_num, block_num + 1);
                    }
                }

                let scheduled_count = scheduled.len() as u64;
                block_send_totals.insert(
                    block_index,
                    BlockSendCounters {
                        received: 0,
                        successes: 0,
                        total: scheduled_count,
                    },
                );

                // Spawn per-wallet timed send tasks.
                for s in scheduled {
                    let send_http = ctx.send_http.clone();
                    let tx = send_result_tx.clone();
                    let spin_window = Duration::from_micros(ctx.cfg.send.resolved_spin_window_us());
                    send_set.spawn(async move {
                        hybrid_sleep_until_with_window(s.target_instant, spin_window).await;
                        let t_pre = Instant::now();
                        let outcome_res = send_http.send_raw_transaction_prepared(&s.payload).await;
                        let t_post = Instant::now();
                        let (resolved_outcome, rtt_us) = match outcome_res {
                            Ok(o) => (o, (t_post - t_pre).as_micros() as u64),
                            Err(e) => {
                                let rtt = (t_post - t_pre).as_micros() as u64;
                                (
                                    SendOutcome::Rejected {
                                        code: -1,
                                        message: format!("transport: {}", e),
                                    },
                                    rtt,
                                )
                            }
                        };
                        let wake_jitter_us = t_pre
                            .saturating_duration_since(s.target_instant)
                            .as_micros() as u64;
                        let sent_at_unix_ms = s.target_unix_ms + (wake_jitter_us / 1000) as i64;
                        let _ = tx
                            .send(SendResult {
                                wallet_label: s.wallet_label,
                                tx_hash: s.tx_hash,
                                block_idx: s.block_idx,
                                block_num: s.block_num,
                                block_hash: s.block_hash,
                                slot_ms: s.slot_ms,
                                sample_idx: s.sample_idx,
                                nonce: s.nonce,
                                target_unix_ms: s.target_unix_ms,
                                sent_at_unix_ms,
                                wake_jitter_us,
                                rpc_rtt_us: rtt_us,
                                outcome: resolved_outcome,
                            })
                            .await;
                    });
                }

                block_index += 1;

                if block_index.is_multiple_of(50) {
                    info!(
                        progress = format!("[{}/{}] slot={}ms", block_index, total_blocks, slot_ms)
                    );
                }
            }

            Some(result) = send_result_rx.recv() => {
                let (send_result_str, was_success) = match &result.outcome {
                    SendOutcome::Accepted { .. } => ("ok".to_string(), true),
                    SendOutcome::Rejected { code, message } => {
                        (format!("error:{}", classify_error(*code, message)), false)
                    }
                };
                let tx_hash_hex = format!("{:?}", result.tx_hash);

                let partial = PartialRecord {
                    block_idx: result.block_idx,
                    block_num: result.block_num,
                    block_hash: result.block_hash.clone(),
                    slot_ms: result.slot_ms,
                    sample_idx: result.sample_idx,
                    wallet: result.wallet_label.clone(),
                    nonce: result.nonce,
                    target_unix_ms: result.target_unix_ms,
                    sent_at_unix_ms: result.sent_at_unix_ms,
                    wake_jitter_us: result.wake_jitter_us,
                    rpc_rtt_us: result.rpc_rtt_us,
                    send_result: send_result_str.clone(),
                    tx_hash: tx_hash_hex.clone(),
                };

                let (inclusion, store_pending) = if was_success {
                    (InclusionKind::Pending, true)
                } else {
                    {
                        let mut t = tracker.lock().await;
                        t.record_send_error(result.tx_hash, send_result_str.clone());
                    }
                    (InclusionKind::SendError, false)
                };

                let rec = partial.clone().into_tx_record(inclusion, None);
                if let Err(e) = jsonl.lock().await.write(&rec) {
                    warn!(error = %e, "JSONL write failed (initial)");
                }
                agg.lock().await.ingest(&rec);

                if store_pending {
                    pending_records.lock().await.insert(result.tx_hash, partial);
                }

                // Fix B: feed AbortTracker only once per block, after all
                // of its scheduled send_results have arrived.
                let block_idx = result.block_idx;
                let maybe_closed = {
                    let counters = block_send_totals.entry(block_idx).or_default();
                    counters.received += 1;
                    if was_success {
                        counters.successes += 1;
                    }
                    if counters.total > 0 && counters.received >= counters.total {
                        Some((counters.successes, counters.total))
                    } else {
                        None
                    }
                };
                if let Some((successes, total)) = maybe_closed {
                    abort_tracker.record_block(successes, total);
                    block_send_totals.remove(&block_idx);

                    if abort_tracker.should_abort() {
                        warn!(
                            consecutive = abort_tracker.consecutive,
                            "{} consecutive failed blocks — aborting",
                            abort_tracker.consecutive
                        );
                        break;
                    }
                }
            }

            _ = tokio::signal::ctrl_c() => {
                info!("Ctrl+C received — graceful shutdown");
                break;
            }
        }
    }

    // ── Drain remaining in-flight send_results (best-effort) ────────────────
    let deadline = Instant::now() + Duration::from_secs(3);
    while let Ok(Some(r)) = tokio::time::timeout_at(deadline.into(), send_result_rx.recv()).await {
        let was_success = matches!(r.outcome, SendOutcome::Accepted { .. });
        let send_result_str = match &r.outcome {
            SendOutcome::Accepted { .. } => "ok".to_string(),
            SendOutcome::Rejected { code, message } => {
                format!("error:{}", classify_error(*code, message))
            }
        };
        let rec = TxRecord {
            block_idx: r.block_idx,
            block_num: r.block_num,
            block_hash: r.block_hash,
            slot_ms: r.slot_ms,
            sample_idx: r.sample_idx,
            wallet: r.wallet_label,
            tx_hash: Some(format!("{:?}", r.tx_hash)),
            nonce: r.nonce,
            target_unix_ms: r.target_unix_ms,
            sent_at_unix_ms: r.sent_at_unix_ms,
            wake_jitter_us: r.wake_jitter_us,
            rpc_rtt_us: r.rpc_rtt_us,
            send_result: send_result_str,
            inclusion: if was_success {
                InclusionKind::Pending
            } else {
                InclusionKind::SendError
            },
            included_block: None,
        };
        if let Err(e) = jsonl.lock().await.write(&rec) {
            warn!(error = %e, "JSONL write failed (drain)");
        }
        agg.lock().await.ingest(&rec);
    }

    // Abort any still-running send tasks.
    send_set.abort_all();
    while send_set.join_next().await.is_some() {}

    // Flush JSONL.
    if let Err(e) = jsonl.lock().await.flush() {
        warn!(error = %e, "JSONL flush failed");
    }

    // ── Finalize reports ───────────────────────────────────────────────────
    let mut agg_locked = agg.lock().await;
    agg_locked.finalize();
    let report = crate::report::render_stdout_report(&agg_locked, &[50, 90, 95, 99]);
    println!("{}", report);
    crate::report::write_csv(ctx.output_dir.join("summary.csv"), &agg_locked)
        .context("write summary.csv")?;
    crate::report::write_markdown(ctx.output_dir.join("report.md"), &report)
        .context("write report.md")?;

    Ok(())
}

// ── Helpers ─────────────────────────────────────────────────────────────────

fn hex_0x(b: &[u8]) -> String {
    let mut s = String::with_capacity(2 + b.len() * 2);
    s.push_str("0x");
    for byte in b {
        s.push_str(&format!("{:02x}", byte));
    }
    s
}

fn classify_error(code: i64, msg: &str) -> String {
    let lower = msg.to_lowercase();
    if lower.contains("nonce too low") {
        return "nonce_too_low".into();
    }
    if lower.contains("underpriced") {
        return "replacement_underpriced".into();
    }
    if lower.contains("timeout") {
        return "rpc_timeout".into();
    }
    format!("other_{}", code)
}
