use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use clap::{Args, Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "entry-comparator")]
#[command(about = "Compare Helius Yellowstone gRPC vs Jito ShredStream entry latency")]
pub struct Cli {
    #[command(subcommand)]
    pub cmd: Cmd,
}

#[derive(Subcommand, Debug)]
pub enum Cmd {
    /// Run the comparator (collects diff records to disk).
    Run(RunArgs),
    /// Generate quick-look report from a previous run.
    Report(ReportArgs),
}

#[derive(Args, Debug, Clone)]
pub struct RunArgs {
    #[arg(long, env = "HELIUS_GRPC_URL")]
    pub yellowstone_url: String,

    #[arg(long, env = "HELIUS_GRPC_TOKEN")]
    pub yellowstone_token: Option<String>,

    /// Public Solana RPC URL for getLeaderSchedule (HTTP, not gRPC).
    #[arg(long, env = "SOLANA_RPC_URL", default_value = "https://api.mainnet-beta.solana.com")]
    pub rpc_url: String,

    #[arg(long, default_value = "127.0.0.1:8001")]
    pub shredstream_bind: SocketAddr,

    #[arg(long)]
    pub output_dir: PathBuf,

    #[arg(long, default_value = "1h", value_parser = humantime::parse_duration)]
    pub duration: Duration,

    /// Comma list "ys=2,ss_rx=3,deshred=4,corr=5,writer=6"
    #[arg(long)]
    pub core_pinning: Option<String>,

    #[arg(long, default_value_t = 32_768)]
    pub row_group_size: usize,

    #[arg(long, default_value = "60s", value_parser = humantime::parse_duration)]
    pub flush_interval: Duration,

    #[arg(long, default_value_t = 65_536)]
    pub channel_capacity: usize,
}

#[derive(Args, Debug, Clone)]
pub struct ReportArgs {
    #[arg(long)]
    pub input_dir: PathBuf,

    #[arg(long)]
    pub output: PathBuf,
}
