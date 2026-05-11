use clap::Parser;
use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use tick_trigger_bench::config::{Cli, Cmd};
use tick_trigger_bench::runtime;

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Schedule(args) => runtime::generate_schedule(args),
        Cmd::Run(args) => runtime::run(args),
    }
}
