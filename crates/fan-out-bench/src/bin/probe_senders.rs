//! Probe-senders — pre-flight compatibility check per spec §9.4.

use anyhow::{Context, Result};
use clap::Parser;
use fan_out_bench::config::{Config, SenderKind};
use fan_out_bench::senders::TxSender;
use fan_out_bench::tip_accounts::{tip_accounts_for, TipAccountRotator};
use fan_out_bench::wallet::load_keypair_file;
use solana_client::rpc_client::RpcClient;
use solana_commitment_config::CommitmentConfig;
use solana_compute_budget_interface::ComputeBudgetInstruction;
use solana_sdk::{
    instruction::Instruction,
    message::Message,
    signature::Signer,
    transaction::Transaction,
};
use solana_system_interface::instruction as sys_instruction;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;

#[derive(Parser, Debug)]
#[command(name = "probe-senders")]
struct Args {
    #[arg(long)]
    config: PathBuf,
    #[arg(long, default_value = "5")]
    tx_per_sender: usize,
    #[arg(long, default_value = "30")]
    wait_secs: u64,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let config = Config::load(&args.config).context("load config")?;
    let authority = Arc::new(load_keypair_file(&config.run.wallet_keypair_path).context("load wallet")?);
    let payer = authority.pubkey();
    tracing::info!(authority = %payer, "probe-senders starting");

    let rpc = Arc::new(RpcClient::new_with_commitment(
        config.sources.helius_rpc_url.clone(),
        CommitmentConfig::confirmed(),
    ));

    #[allow(clippy::type_complexity)]
    let mut senders: Vec<(Arc<dyn TxSender>, Arc<TipAccountRotator>, SenderKind, u64)> = Vec::new();
    for sc in config.enabled_senders() {
        let sender: Arc<dyn TxSender> = match sc.kind {
            SenderKind::Helius => Arc::new(fan_out_bench::senders::helius::HeliusSender::new(
                sc.id, sc.name.clone(), sc.endpoint_url.clone(),
                match &sc.auth {
                    fan_out_bench::config::AuthConfig::QueryParam { value, .. } => Some(value.clone()),
                    _ => None,
                },
                false,
            )),
            SenderKind::Jito => Arc::new(fan_out_bench::senders::jito::JitoSender::new(
                sc.id, sc.name.clone(), sc.endpoint_url.clone(),
                match &sc.auth {
                    fan_out_bench::config::AuthConfig::Header { value, .. } => Some(value.clone()),
                    _ => None,
                },
            )),
            _ => {
                tracing::warn!(name = %sc.name, kind = ?sc.kind, "probe-senders: kind not yet supported, skipping");
                continue;
            }
        };
        let rotator = Arc::new(TipAccountRotator::new(tip_accounts_for(sc.kind)));
        senders.push((sender, rotator, sc.kind, sc.tip_lamports));
    }

    if senders.is_empty() {
        anyhow::bail!("no probe-able senders enabled in config");
    }
    tracing::info!(count = senders.len(), "probing senders");

    let rt = Runtime::new()?;
    let mut results: HashMap<String, (usize, usize)> = HashMap::new();

    for (sender, rotator, kind, tip_lamports) in &senders {
        let name = sender.name().to_string();
        let mut sigs = Vec::new();
        let mut sent_ok = 0;
        for i in 0..args.tx_per_sender {
            let blockhash = match rpc.get_latest_blockhash() {
                Ok(b) => b,
                Err(e) => {
                    tracing::error!(error = %e, "get_latest_blockhash failed");
                    break;
                }
            };
            let tip_account = rotator.next();
            let mut ixs: Vec<Instruction> = Vec::with_capacity(5);
            ixs.push(sys_instruction::transfer(&payer, &payer, 1 + i as u64));
            if let Some(ta) = tip_account {
                ixs.push(sys_instruction::transfer(&payer, &ta, *tip_lamports));
            }
            ixs.push(ComputeBudgetInstruction::set_compute_unit_limit(200_000));
            ixs.push(ComputeBudgetInstruction::set_compute_unit_price(5_000));
            let msg = Message::new_with_blockhash(&ixs, Some(&payer), &blockhash);
            let mut tx = Transaction::new_unsigned(msg);
            tx.sign(&[authority.as_ref()], blockhash);
            let sig = tx.signatures[0];

            let outcome = rt.block_on(sender.send(&tx));
            if outcome.error.is_none() {
                sent_ok += 1;
                sigs.push(sig);
            } else {
                tracing::warn!(sender = %name, sig = %sig, error = ?outcome.error, "probe send failed");
            }
            std::thread::sleep(Duration::from_secs(2));
        }

        tracing::info!(sender = %name, sent_ok, "waiting for landings");
        std::thread::sleep(Duration::from_secs(args.wait_secs));

        let mut landed = 0;
        if !sigs.is_empty() {
            for chunk in sigs.chunks(100) {
                if let Ok(resp) = rpc.get_signature_statuses(chunk) {
                    landed += resp.value.iter().filter(|s| s.is_some()).count();
                }
            }
        }
        let landing_rate = if sent_ok > 0 { landed as f64 / sent_ok as f64 } else { 0.0 };
        let verdict = if landed >= 3 { "COMPATIBLE" } else { "INCOMPATIBLE" };
        tracing::info!(
            sender = %name,
            kind = ?kind,
            sent_ok,
            landed,
            landing_rate = format!("{:.0}%", landing_rate * 100.0),
            verdict,
            "probe result"
        );
        results.insert(name, (sent_ok, landed));
    }

    println!("\n=== Probe Summary ===");
    for (name, (sent_ok, landed)) in &results {
        let rate = if *sent_ok > 0 { *landed as f64 / *sent_ok as f64 * 100.0 } else { 0.0 };
        let verdict = if *landed >= 3 { "COMPATIBLE" } else { "INCOMPATIBLE" };
        println!("{:20} sent={} landed={} rate={:.0}% verdict={}", name, sent_ok, landed, rate, verdict);
    }
    Ok(())
}
