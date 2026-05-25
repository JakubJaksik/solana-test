//! Minimal Jito bundle diagnostic — strips EVERYTHING we suspect could be
//! wrong with our other paths:
//!   * no tonic / no gRPC
//!   * no auth (anonymous, 1 tps default rate)
//!   * one tx with tip inlined (no separate tip tx)
//!   * v0 VersionedTransaction, fresh blockhash, no nonce
//!   * root domain `mainnet.block-engine.jito.wtf` (Jito routes for us)
//!
//! Sends one bundle, waits 30s, queries BOTH:
//!   * Solana RPC `getSignatureStatuses` — ground truth: tx on chain or not
//!   * Jito `getInflightBundleStatuses` — Jito's own view
//!
//! If this lands we know the minimal path works; we then add complexity
//! (auth, nonce, multi-region) on top.

use anyhow::{Context, Result};
use base64::Engine as _;
use clap::Parser;
use solana_client::rpc_client::RpcClient;
use solana_commitment_config::CommitmentConfig;
use solana_compute_budget_interface::ComputeBudgetInstruction;
use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    message::{v0::Message as V0Message, VersionedMessage},
    pubkey::Pubkey,
    signer::Signer,
    transaction::VersionedTransaction,
};
use solana_system_interface::instruction as system_instruction;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;
use tick_trigger_fan_out_bench::config::{Config, SenderKind};
use tick_trigger_fan_out_bench::tip_accounts::tip_accounts_for;
use tick_trigger_fan_out_bench::wallet;

const MEMO_PROGRAM_ID: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";
const JITO_BUNDLES_URL: &str = "https://mainnet.block-engine.jito.wtf/api/v1/bundles";

#[derive(Parser, Debug)]
#[command(version, about = "Minimal Jito bundle send — no auth, no gRPC, no nonce")]
struct Args {
    #[arg(long)]
    config: PathBuf,
    /// Override tip in lamports (default 200_000).
    #[arg(long, default_value_t = 200_000_u64)]
    tip_lamports: u64,
    /// Override priority fee microlamports (default from config.tx).
    #[arg(long)]
    priority_fee: Option<u64>,
    /// How long to wait before querying status, in seconds.
    #[arg(long, default_value_t = 30)]
    wait_secs: u64,
    /// Alternative Jito bundles URL (e.g. region-specific). Defaults to
    /// `https://mainnet.block-engine.jito.wtf/api/v1/bundles`.
    #[arg(long)]
    jito_url: Option<String>,
    /// Skip the 1-lamport self-transfer ix. Diagnostic for whether Jito
    /// filters bundles whose only "work" is a self-transfer (= test/spam
    /// pattern). With this flag the tx has just [priority_fee, memo, tip].
    #[arg(long)]
    no_self_transfer: bool,
    /// Send the "work" lamport transfer to this recipient pubkey (base58)
    /// instead of payer (self). Tests whether self-transfers specifically
    /// are filtered. Ignored if --no-self-transfer is set.
    #[arg(long)]
    transfer_to: Option<String>,
    /// Amount (lamports) for the "work" transfer ix. Default 1.
    #[arg(long, default_value_t = 1_u64)]
    transfer_lamports: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let cfg = Config::load(&args.config).context("load config")?;
    let jito_url = args
        .jito_url
        .clone()
        .unwrap_or_else(|| JITO_BUNDLES_URL.to_string());

    let keypair = wallet::load_keypair(&cfg.wallet.keypair_path).context("load wallet")?;
    let payer_pk = keypair.pubkey();
    println!("payer: {}", payer_pk);

    let rpc = RpcClient::new_with_commitment(cfg.rpc.url.clone(), CommitmentConfig::confirmed());
    let balance = rpc.get_balance(&payer_pk).context("get balance")?;
    println!("balance: {} lamports", balance);

    let tip_accounts = tip_accounts_for(SenderKind::Jito);
    let tip_account = tip_accounts
        .first()
        .copied()
        .context("no jito tip accounts")?;
    println!("tip account: {}", tip_account);
    println!("tip lamports: {}", args.tip_lamports);

    let blockhash = rpc.get_latest_blockhash().context("get_latest_blockhash")?;
    println!("blockhash: {}", blockhash);

    // Build the bundle as a SINGLE v0 transaction with the tip baked in.
    // Order: priority_fee → memo → self-transfer (1 lam) → tip_transfer.
    let memo_program = Pubkey::from_str(MEMO_PROGRAM_ID).unwrap();
    let priority_fee = args
        .priority_fee
        .unwrap_or(cfg.tx.priority_fee_microlamports);
    let mut ixs: Vec<Instruction> = Vec::with_capacity(4);
    if priority_fee > 0 {
        ixs.push(ComputeBudgetInstruction::set_compute_unit_price(priority_fee));
    }
    ixs.push(Instruction {
        program_id: memo_program,
        accounts: vec![AccountMeta::new_readonly(payer_pk, true)],
        data: b"minimal_jito_test".to_vec(),
    });
    if !args.no_self_transfer {
        let recipient = if let Some(rcpt) = &args.transfer_to {
            Pubkey::from_str(rcpt).context("parse --transfer-to pubkey")?
        } else {
            payer_pk
        };
        ixs.push(system_instruction::transfer(&payer_pk, &recipient, args.transfer_lamports));
        println!(
            "work ix: transfer {} lam → {}",
            args.transfer_lamports, recipient
        );
    } else {
        println!("work ix: SKIPPED (--no-self-transfer)");
    }
    ixs.push(system_instruction::transfer(
        &payer_pk,
        &tip_account,
        args.tip_lamports,
    ));

    let v0 = V0Message::try_compile(&payer_pk, &ixs, &[], blockhash)
        .context("compile v0 message")?;
    let tx = VersionedTransaction::try_new(VersionedMessage::V0(v0), &[&keypair])
        .context("sign v0 tx")?;
    let sig = tx.signatures.first().copied().unwrap_or_default();
    let serialized = bincode::serialize(&tx).context("bincode serialize")?;
    let b64 = base64::engine::general_purpose::STANDARD.encode(&serialized);
    println!("\n=== built tx ===");
    println!("  signature: {}", sig);
    println!("  size: {} bytes", serialized.len());
    println!("  ix count: {}", tx.message.instructions().len());

    // Sanity sim via Solana RPC — proves tx is valid before we send.
    println!("\n=== Solana RPC simulateTransaction ===");
    match rpc.simulate_transaction(&tx) {
        Ok(sim) => {
            println!("  err: {:?}", sim.value.err);
            println!("  units_consumed: {:?}", sim.value.units_consumed);
            if sim.value.err.is_some() {
                println!("  !!! tx fails RPC sim; aborting");
                return Ok(());
            }
        }
        Err(e) => {
            println!("  simulate_transaction err: {} (continuing anyway)", e);
        }
    }

    // POST to Jito.
    println!("\n=== POST to Jito ===");
    println!("  url: {}", jito_url);
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "sendBundle",
        "params": [[b64], { "encoding": "base64" }],
    });
    let resp = http
        .post(&jito_url)
        .header("Content-Type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .context("POST sendBundle")?;
    let status = resp.status();
    let body_text = resp.text().await.unwrap_or_default();
    println!("  http status: {}", status);
    println!("  response: {}", body_text);

    let parsed: serde_json::Value = serde_json::from_str(&body_text).unwrap_or_default();
    let bundle_id = parsed
        .get("result")
        .and_then(|v| v.as_str())
        .map(String::from);
    let bundle_id = match bundle_id {
        Some(b) => {
            println!("  bundle_id: {}", b);
            b
        }
        None => {
            println!("  no bundle_id returned (Jito rejected at HTTP)");
            return Ok(());
        }
    };

    println!("\n=== waiting {}s for bundle to land ===", args.wait_secs);
    tokio::time::sleep(Duration::from_secs(args.wait_secs)).await;

    // Ground truth: Solana RPC getSignatureStatuses.
    println!("\n=== Solana RPC getSignatureStatuses (GROUND TRUTH) ===");
    let sig_body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSignatureStatuses",
        "params": [[sig.to_string()], {"searchTransactionHistory": true}],
    });
    let r = http
        .post(&cfg.rpc.url)
        .header("Content-Type", "application/json")
        .body(sig_body.to_string())
        .send()
        .await?;
    let v: serde_json::Value = r.json().await?;
    println!("{}", serde_json::to_string_pretty(&v)?);

    // Jito's view: getInflightBundleStatuses.
    println!("\n=== Jito getInflightBundleStatuses (Jito's view) ===");
    let jito_status_body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getInflightBundleStatuses",
        "params": [[bundle_id]],
    });
    let r = http
        .post(&jito_url)
        .header("Content-Type", "application/json")
        .body(jito_status_body.to_string())
        .send()
        .await?;
    let v: serde_json::Value = r.json().await?;
    println!("{}", serde_json::to_string_pretty(&v)?);

    println!();
    println!("=== interpretation ===");
    println!(" * getSignatureStatuses null     -> tx never landed on chain");
    println!(" * getSignatureStatuses {{slot, err:null}} -> tx LANDED (bundle worked)");
    println!(" * Jito status Landed            -> Jito processed + landed");
    println!(" * Jito status Invalid + sig null -> bundle dropped at ingest");
    println!(" * Jito status Pending + sig null -> auction lost (try higher tip)");
    Ok(())
}
