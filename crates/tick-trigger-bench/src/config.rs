use std::path::PathBuf;
use std::time::Duration;

use clap::{Args, Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "tick-trigger-bench")]
#[command(about = "Etap 1 — tick-triggered self-transfer latency bench")]
pub struct Cli {
    #[command(subcommand)]
    pub cmd: Cmd,
}

#[derive(Subcommand, Debug)]
pub enum Cmd {
    /// Generate a schedule and save to JSON.
    Schedule(ScheduleArgs),
    /// Execute the bench: load schedule, send tx, observe entries, write parquet.
    Run(RunArgs),
}

#[derive(Args, Debug, Clone)]
pub struct ScheduleArgs {
    #[arg(long, default_value_t = 33334)]
    pub num_slots: u64,

    #[arg(long)]
    pub start_slot: u64,

    /// Optional seed for reproducibility. If absent, uses SystemTime nanos.
    #[arg(long)]
    pub seed: Option<u64>,

    #[arg(long)]
    pub out: PathBuf,
}

#[derive(Args, Debug, Clone)]
pub struct RunArgs {
    #[arg(long)]
    pub schedule: PathBuf,

    #[arg(long)]
    pub wallet_keypair: PathBuf,

    #[arg(long, env = "HELIUS_SENDER_URL",
          default_value = "https://fra-sender.helius-rpc.com/fast?swqos_only=true")]
    pub helius_sender_url: String,

    #[arg(long, env = "HELIUS_RPC_URL")]
    pub helius_rpc_url: String,

    #[arg(long, default_value = "http://127.0.0.1:9999")]
    pub shredstream_grpc_url: String,

    #[arg(long)]
    pub output_dir: PathBuf,

    #[arg(long, default_value = "5h", value_parser = humantime::parse_duration)]
    pub max_duration: Duration,

    /// Comma list: ss_grpc=2,observer=3,preparer=4,sender=5,writer=6,parquet=7,rpc=8
    #[arg(long)]
    pub core_pinning: Option<String>,

    #[arg(long, default_value_t = 1)]
    pub tx_amount_lamports: u64,

    #[arg(long, default_value_t = 5000)]
    pub priority_fee_microlamports: u64,

    #[arg(long, default_value_t = 32_768)]
    pub row_group_size: usize,

    #[arg(long, default_value = "60s", value_parser = humantime::parse_duration)]
    pub flush_interval: Duration,

    #[arg(long, default_value_t = 65_536)]
    pub channel_capacity: usize,

    #[arg(long, default_value = "90s", value_parser = humantime::parse_duration)]
    pub observation_deadline: Duration,

    #[arg(long, default_value = "30s", value_parser = humantime::parse_duration)]
    pub preparer_refresh: Duration,

    #[arg(long, default_value_t = 100)]
    pub look_ahead_slots: u64,
}
