//! CLI: cargo run --bin run -- --config <path>

use anyhow::{Context, Result};
use clap::Parser;
use entry_sources::shredstream::ShredStreamGrpcSource;
use entry_sources::yellowstone::YellowstoneSource;
use entry_sources::{DropCounters, EntrySource};
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
    let authority_pubkey = authority.pubkey();
    tracing::info!(authority = %authority_pubkey, "fan-out-bench starting");

    let rpc = Arc::new(RpcClient::new_with_commitment(
        config.sources.helius_rpc_url.clone(),
        CommitmentConfig::confirmed(),
    ));

    let start_slot = rpc.get_slot().context("get_slot")?;
    tracing::info!(start_slot, "current slot");

    let nonce_entries = bootstrap(&rpc, &config.nonce.config_path, &authority_pubkey).context("bootstrap nonces")?;
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
            SenderKind::Nozomi => {
                let api_key = match &sc.auth {
                    fan_out_bench::config::AuthConfig::QueryParam { value, .. } => value.clone(),
                    _ => {
                        tracing::warn!(name = %sc.name, "nozomi requires QueryParam auth, skipping");
                        continue;
                    }
                };
                Arc::new(fan_out_bench::senders::nozomi::NozomiSender::new(
                    sc.id, sc.name.clone(), sc.endpoint_url.clone(), api_key,
                ))
            }
            SenderKind::Slot0 => {
                let api_key = match &sc.auth {
                    fan_out_bench::config::AuthConfig::QueryParam { value, .. } => value.clone(),
                    _ => { tracing::warn!(name = %sc.name, "0slot requires QueryParam auth, skipping"); continue; }
                };
                Arc::new(fan_out_bench::senders::slot0::Slot0Sender::new(
                    sc.id, sc.name.clone(), sc.endpoint_url.clone(), api_key,
                ))
            }
            SenderKind::Bloxroute => {
                let auth = match &sc.auth {
                    fan_out_bench::config::AuthConfig::Header { value, .. } => value.clone(),
                    _ => { tracing::warn!(name = %sc.name, "bloxroute requires Header auth, skipping"); continue; }
                };
                Arc::new(fan_out_bench::senders::bloxroute::BloxrouteSender::new(
                    sc.id, sc.name.clone(), sc.endpoint_url.clone(), auth,
                ))
            }
            SenderKind::Astralane => {
                let api_key = match &sc.auth {
                    fan_out_bench::config::AuthConfig::QueryParam { value, .. } => value.clone(),
                    _ => { tracing::warn!(name = %sc.name, "astralane requires QueryParam auth, skipping"); continue; }
                };
                Arc::new(fan_out_bench::senders::astralane::AstralaneSender::new(
                    sc.id, sc.name.clone(), sc.endpoint_url.clone(), api_key,
                ))
            }
            SenderKind::Syncro => {
                let auth = match &sc.auth {
                    fan_out_bench::config::AuthConfig::Bearer { token } => fan_out_bench::senders::syncro::SyncroAuth::Bearer(token.clone()),
                    fan_out_bench::config::AuthConfig::Header { value, .. } => fan_out_bench::senders::syncro::SyncroAuth::XApiKey(value.clone()),
                    fan_out_bench::config::AuthConfig::None => fan_out_bench::senders::syncro::SyncroAuth::None,
                    _ => { tracing::warn!(name = %sc.name, "syncro requires Bearer/Header/None auth, skipping"); continue; }
                };
                Arc::new(fan_out_bench::senders::syncro::SyncroSender::new(
                    sc.id, sc.name.clone(), sc.endpoint_url.clone(), auth,
                ))
            }
            SenderKind::Triton => {
                Arc::new(fan_out_bench::senders::triton::TritonSender::new(
                    sc.id, sc.name.clone(), sc.endpoint_url.clone(),
                ))
            }
            SenderKind::JitoBundle => {
                let auth = match &sc.auth {
                    fan_out_bench::config::AuthConfig::Header { value, .. } => Some(value.clone()),
                    _ => None,
                };
                Arc::new(fan_out_bench::senders::jito_bundle::JitoBundleSender::new(
                    sc.id, sc.name.clone(), sc.endpoint_url.clone(), auth,
                ))
            }
            _ => {
                tracing::warn!(name = %sc.name, "sender kind not implemented yet, skipping");
                continue;
            }
        };
        senders.insert(sc.id, sender);
    }
    tracing::info!(count = senders.len(), "senders configured");

    let ss_counters = Arc::new(DropCounters::default());
    let ss_src = Box::new(ShredStreamGrpcSource {
        endpoint: config.sources.shredstream_grpc_url.clone(),
        channel_capacity: 65536,
        pinned_core: None,
        counters: ss_counters.clone(),
    });
    let ss_rx = ss_src.start().context("start shredstream source")?;
    tracing::info!("shredstream source started");

    let ys_counters = Arc::new(DropCounters::default());
    let ys_src = Box::new(YellowstoneSource {
        url: config.sources.yellowstone_grpc_url.clone(),
        token: config.sources.yellowstone_auth_token.clone(),
        channel_capacity: 65536,
        pinned_core: None,
        counters: ys_counters.clone(),
    });
    let ys_rx = ys_src.start().context("start yellowstone source")?;
    tracing::info!("yellowstone source started");

    let run_id = chrono::Utc::now().format("%Y%m%d-%H%M%S").to_string();
    let output_dir = config.run.output_dir.join(&run_id);
    std::fs::create_dir_all(&output_dir)?;
    tracing::info!(?output_dir, "run output directory");

    let handles = start_runtime(RuntimeInputs {
        config: config.clone(),
        authority,
        authority_pubkey,
        nonce_manager,
        ss_entry_rx: ss_rx,
        ys_entry_rx: ys_rx,
        senders,
        output_dir,
        run_id,
        rpc,
        start_slot,
    })?;

    tracing::info!("runtime started — bench is running. Ctrl-C to stop.");

    let stop = handles.stop.clone();
    ctrlc::set_handler(move || {
        tracing::info!("Ctrl-C received, signalling shutdown");
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
    })?;

    while !handles.stop.load(std::sync::atomic::Ordering::Relaxed) {
        std::thread::sleep(std::time::Duration::from_secs(5));
        let snap = handles.counters.snapshot();
        tracing::info!(
            pool_empty = snap.pool_empty,
            send_http_error = snap.send_http_error,
            send_throttled_429 = snap.send_throttled_429,
            finality_confirmed = snap.finality_confirmed,
            "counters snapshot"
        );
    }
    tracing::info!("shutdown complete");
    Ok(())
}
