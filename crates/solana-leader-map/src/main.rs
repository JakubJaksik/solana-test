use anyhow::Result;
use clap::Parser;

use solana_leader_map::cli::{Cli, run};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,reqwest=warn,hyper=warn")),
        )
        .with_target(false)
        .init();

    let cli = Cli::parse();
    run(cli).await
}
