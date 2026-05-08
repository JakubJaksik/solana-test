use anyhow::{Context, Result, bail};
use chrono::Utc;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing::info;

use crate::aggregate::{build_slot_map, summarize};
use crate::cache::Cache;
use crate::config::Config;
use crate::domain::EpochSnapshot;
use crate::output::{print_slot_range, print_summary};
use crate::solana_rpc::SolanaRpcClient;
use crate::validators_app::ValidatorsAppClient;

#[derive(Debug, Parser)]
#[command(
    author,
    version,
    about = "Per-epoch Solana leader-schedule × validator-geo mapper",
    long_about = "Fetch validator geographic data from validators.app and a leader schedule from a Solana RPC, cross-reference them, cache as JSON per epoch, and present aggregates."
)]
pub struct Cli {
    /// Path to config.json (see config.example.json).
    #[arg(short = 'c', long, default_value = "config.json")]
    pub config: PathBuf,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Fetch validators + leader schedule for the current epoch and cache to runs/.
    Fetch {
        /// Force refetch even if a cached snapshot for the current epoch exists.
        #[arg(long)]
        force: bool,
    },
    /// Print per-country slot/stake summary for the latest cached epoch (or specified --epoch).
    Summary {
        /// Specific epoch to summarize (defaults to latest cache).
        #[arg(long)]
        epoch: Option<u64>,
    },
    /// Print leader info for an absolute slot (uses latest cache or --epoch).
    At {
        slot: u64,
        #[arg(long)]
        epoch: Option<u64>,
    },
    /// Print leader info for a range of absolute slots: e.g. 251234500..251234600
    Slots {
        /// Range as `start..end` (inclusive end).
        range: String,
        #[arg(long)]
        epoch: Option<u64>,
    },
    /// Dump the snapshot JSON to stdout (latest cache or --epoch).
    Export {
        #[arg(long)]
        epoch: Option<u64>,
    },
}

pub async fn run(cli: Cli) -> Result<()> {
    let cfg = Config::load(&cli.config)
        .with_context(|| format!("loading {}", cli.config.display()))?;
    let cache = Cache::new(cfg.cache.dir.clone())?;

    match cli.command {
        Commands::Fetch { force } => fetch(&cfg, &cache, force).await,
        Commands::Summary { epoch } => summary_cmd(&cache, epoch),
        Commands::At { slot, epoch } => at_cmd(&cache, slot, epoch),
        Commands::Slots { range, epoch } => slots_cmd(&cache, &range, epoch),
        Commands::Export { epoch } => export_cmd(&cache, epoch),
    }
}

async fn fetch(cfg: &Config, cache: &Cache, force: bool) -> Result<()> {
    let rpc = SolanaRpcClient::new(cfg.solana_rpc.clone())?;
    let validators_client = ValidatorsAppClient::new(cfg.validators_app.clone())?;

    let epoch_info = rpc.get_epoch_info().await?;
    info!(
        epoch = epoch_info.epoch,
        first_slot = epoch_info.epoch_first_slot(),
        last_slot = epoch_info.epoch_last_slot(),
        "current epoch"
    );

    let cache_path = cache.snapshot_path(epoch_info.epoch);
    if cache_path.exists() && !force {
        info!(
            path = %cache_path.display(),
            "cached snapshot for current epoch already exists — use --force to refetch"
        );
        return Ok(());
    }

    let (validators, schedule) = tokio::try_join!(
        validators_client.fetch_all_validators(),
        rpc.get_leader_schedule()
    )?;

    let snap = EpochSnapshot {
        fetched_at: Utc::now(),
        epoch: epoch_info,
        validators,
        schedule,
    };

    let path = cache.save(&snap)?;
    println!(
        "✔ fetched epoch {}  •  {} validators  •  {} schedule entries  →  {}",
        snap.epoch.epoch,
        snap.validators.len(),
        snap.schedule.len(),
        path.display()
    );
    Ok(())
}

fn load_snapshot(cache: &Cache, epoch: Option<u64>) -> Result<EpochSnapshot> {
    match epoch {
        Some(e) => cache.load(e),
        None => cache
            .latest_snapshot()?
            .context("no cached snapshot found — run `fetch` first")
    }
}

fn summary_cmd(cache: &Cache, epoch: Option<u64>) -> Result<()> {
    let snap = load_snapshot(cache, epoch)?;
    let slot_map = build_slot_map(&snap);
    let summary = summarize(&snap, &slot_map);
    print_summary(&summary);
    Ok(())
}

fn at_cmd(cache: &Cache, slot: u64, epoch: Option<u64>) -> Result<()> {
    let snap = load_snapshot(cache, epoch)?;
    let slot_map = build_slot_map(&snap);
    match slot_map.get(&slot) {
        Some(entry) => {
            print_slot_range(std::slice::from_ref(entry));
        }
        None => {
            println!(
                "Slot {} is outside cached epoch (epoch {}: {}..={}).",
                slot,
                snap.epoch.epoch,
                snap.epoch.epoch_first_slot(),
                snap.epoch.epoch_last_slot()
            );
        }
    }
    Ok(())
}

fn slots_cmd(cache: &Cache, range: &str, epoch: Option<u64>) -> Result<()> {
    let (start, end) = parse_range(range)?;
    if end < start {
        bail!("range end must be ≥ start");
    }
    let snap = load_snapshot(cache, epoch)?;
    let slot_map = build_slot_map(&snap);
    let entries: Vec<_> = (start..=end)
        .filter_map(|s| slot_map.get(&s).cloned())
        .collect();
    if entries.is_empty() {
        println!("No slots from {}..={} found in cached epoch.", start, end);
    } else {
        print_slot_range(&entries);
    }
    Ok(())
}

fn parse_range(input: &str) -> Result<(u64, u64)> {
    let parts: Vec<&str> = input.split("..").collect();
    if parts.len() != 2 {
        bail!("invalid range, expected `start..end`, got `{}`", input);
    }
    let start: u64 = parts[0]
        .parse()
        .with_context(|| format!("parse start of range `{}`", input))?;
    let end: u64 = parts[1]
        .parse()
        .with_context(|| format!("parse end of range `{}`", input))?;
    Ok((start, end))
}

fn export_cmd(cache: &Cache, epoch: Option<u64>) -> Result<()> {
    let snap = load_snapshot(cache, epoch)?;
    let json = serde_json::to_string_pretty(&snap)?;
    println!("{}", json);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::parse_range;

    #[test]
    fn parses_simple_range() {
        assert_eq!(parse_range("100..200").unwrap(), (100, 200));
    }
    #[test]
    fn rejects_invalid_range() {
        assert!(parse_range("100").is_err());
        assert!(parse_range("a..b").is_err());
    }
}
