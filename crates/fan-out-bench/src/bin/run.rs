//! CLI: cargo run --bin run -- --config <path>

use anyhow::{Context, Result};
use clap::Parser;
use fan_out_bench::config::{Config, SenderKind};
use fan_out_bench::nonce::bootstrap::bootstrap;
use fan_out_bench::nonce::manager::NonceManager;
use fan_out_bench::runtime::{start as start_runtime, RuntimeInputs};
use fan_out_bench::senders::helius::HeliusSender;
use fan_out_bench::senders::jito::JitoSender;
use fan_out_bench::senders::TxSender;
use fan_out_bench::wallet::load_keypair_file;
use solana_client::rpc_client::RpcClient;
use solana_commitment_config::CommitmentConfig;
use solana_sdk::signature::Signer;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Parser, Debug)]
#[command(name = "run")]
struct Args {
    #[arg(long)]
    config: PathBuf,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let config = Config::load(&args.config).context("load config")?;

    let authority = Arc::new(load_keypair_file(&config.run.wallet_keypair_path).context("load wallet")?);
    tracing::info!(authority = %authority.pubkey(), "fan-out-bench starting");

    let rpc = RpcClient::new_with_commitment(config.sources.helius_rpc_url.clone(), CommitmentConfig::confirmed());
    let nonce_entries = bootstrap(&rpc, &config.nonce.config_path, &authority.pubkey()).context("bootstrap nonces")?;
    let nonce_manager = Arc::new(NonceManager::new(nonce_entries));
    tracing::info!(count = nonce_manager.len(), "nonce manager ready");

    let mut senders: HashMap<u8, Arc<dyn TxSender>> = HashMap::new();
    for sc in config.enabled_senders() {
        let sender: Arc<dyn TxSender> = match sc.kind {
            SenderKind::Helius => {
                let api_key = match &sc.auth {
                    fan_out_bench::config::AuthConfig::QueryParam { value, .. } => Some(value.clone()),
                    _ => None,
                };
                Arc::new(HeliusSender::new(sc.id, sc.name.clone(), sc.endpoint_url.clone(), api_key, false))
            }
            SenderKind::Jito => {
                let auth = match &sc.auth {
                    fan_out_bench::config::AuthConfig::Header { value, .. } => Some(value.clone()),
                    _ => None,
                };
                Arc::new(JitoSender::new(sc.id, sc.name.clone(), sc.endpoint_url.clone(), auth))
            }
            _ => {
                tracing::warn!(name = %sc.name, "sender kind not implemented yet, skipping");
                continue;
            }
        };
        senders.insert(sc.id, sender);
    }
    tracing::info!(count = senders.len(), "senders configured");

    let (_ss_tx_dummy, ss_rx) = crossbeam_channel::unbounded();
    let (_ys_tx_dummy, ys_rx) = crossbeam_channel::unbounded();

    let run_id = chrono::Utc::now().format("%Y%m%d-%H%M%S").to_string();
    let output_dir = config.run.output_dir.join(&run_id);
    std::fs::create_dir_all(&output_dir)?;

    let handles = start_runtime(RuntimeInputs {
        config: config.clone(),
        authority,
        nonce_manager,
        ss_entry_rx: ss_rx,
        ys_entry_rx: ys_rx,
        senders,
        output_dir,
        run_id,
    })?;

    tracing::info!("runtime started — bench is running. Ctrl-C to stop.");
    tracing::warn!("NOTE: Plan 4 runtime — SS/YS gRPC clients not yet hooked. Bench will idle.");

    let stop = handles.stop.clone();
    ctrlc::set_handler(move || {
        tracing::info!("Ctrl-C received, signalling shutdown");
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
    })?;

    while !handles.stop.load(std::sync::atomic::Ordering::Relaxed) {
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
    tracing::info!("shutdown complete");
    Ok(())
}
