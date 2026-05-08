use clap::Parser;
use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use entry_comparator::config::{Cli, Cmd};
use entry_comparator::{report, runtime};

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Run(args) => runtime::run(args),
        Cmd::Report(args) => report::generate(args),
    }
}
