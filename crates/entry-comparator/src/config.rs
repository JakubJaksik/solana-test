use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use clap::{Args, Parser, Subcommand, ValueEnum};

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum, Default)]
pub enum ShredstreamMode {
    /// Consume entries via gRPC from `jito-shredstream-proxy --grpc-service-port`.
    /// Proxy performs FEC reconstruction itself; lower bug surface, no per-shred timestamp.
    #[default]
    Grpc,
    /// Consume raw shred packets via UDP and reconstruct entries ourselves
    /// (legacy path; lacks FEC recovery — keep for step-1 experiments).
    Udp,
}

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

    /// Source mode for ShredStream side.
    #[arg(long, value_enum, default_value_t = ShredstreamMode::Grpc)]
    pub shredstream_mode: ShredstreamMode,

    /// gRPC endpoint of jito-shredstream-proxy when --shredstream-mode=grpc.
    /// Must match the proxy's `--grpc-service-port`.
    #[arg(long, default_value = "http://127.0.0.1:9999")]
    pub shredstream_grpc_url: String,

    /// UDP bind address when --shredstream-mode=udp (legacy raw-shred path).
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
