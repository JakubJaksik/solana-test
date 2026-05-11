use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

use crate::config::RunArgs;

#[derive(Serialize)]
pub struct RunMeta<'a> {
    pub started_at_utc: String,
    pub anchor_systemtime_ns: u64,
    pub host: String,
    pub helius_sender_url: &'a str,
    pub helius_rpc_url: &'a str,
    pub shredstream_grpc_url: &'a str,
    pub epoch_at_start: u64,
    pub current_slot_at_start: u64,
    pub schedule_seed: u64,
    pub schedule_start_slot: u64,
    pub schedule_num_slots: u64,
    pub binary_version: &'static str,
    pub config: RunMetaConfig,
}

#[derive(Serialize)]
pub struct RunMetaConfig {
    pub tx_amount_lamports: u64,
    pub priority_fee_microlamports: u64,
    pub row_group_size: usize,
    pub channel_capacity: usize,
    pub flush_interval_secs: u64,
    pub observation_deadline_secs: u64,
    pub preparer_refresh_secs: u64,
    pub look_ahead_slots: u64,
    pub core_pinning: Option<String>,
}

pub fn write_run_meta(
    run_dir: &Path,
    args: &RunArgs,
    current_slot: u64,
    epoch_at_start: u64,
    schedule_seed: u64,
    schedule_start_slot: u64,
    schedule_num_slots: u64,
) -> anyhow::Result<()> {
    let anchor_systemtime_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let host = hostname::get()
        .ok()
        .and_then(|s| s.into_string().ok())
        .unwrap_or_else(|| "unknown".to_string());
    let meta = RunMeta {
        started_at_utc: chrono::Utc::now().to_rfc3339(),
        anchor_systemtime_ns,
        host,
        helius_sender_url: &args.helius_sender_url,
        helius_rpc_url: &args.helius_rpc_url,
        shredstream_grpc_url: &args.shredstream_grpc_url,
        epoch_at_start,
        current_slot_at_start: current_slot,
        schedule_seed,
        schedule_start_slot,
        schedule_num_slots,
        binary_version: env!("CARGO_PKG_VERSION"),
        config: RunMetaConfig {
            tx_amount_lamports: args.tx_amount_lamports,
            priority_fee_microlamports: args.priority_fee_microlamports,
            row_group_size: args.row_group_size,
            channel_capacity: args.channel_capacity,
            flush_interval_secs: args.flush_interval.as_secs(),
            observation_deadline_secs: args.observation_deadline.as_secs(),
            preparer_refresh_secs: args.preparer_refresh.as_secs(),
            look_ahead_slots: args.look_ahead_slots,
            core_pinning: args.core_pinning.clone(),
        },
    };
    let path = run_dir.join("run-meta.json");
    std::fs::write(&path, serde_json::to_string_pretty(&meta)?)?;
    Ok(())
}
