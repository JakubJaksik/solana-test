//! Diagnostic test using the OFFICIAL `jito-sdk-rust` crate.
//!
//! If our custom reqwest setup has a subtle issue (TLS, HTTP version, header
//! ordering, etc.), using the canonical SDK eliminates that variable. The tx
//! structure is identical to `minimal_jito_test.rs` — single v0 tx with
//! inline tip. If THIS still returns Invalid, the problem is on Jito's side
//! (wallet/IP/account state), not our code.

use anyhow::{Context, Result};
use base64::Engine as _;
use clap::Parser;
use jito_sdk_rust::JitoJsonRpcSDK;
use serde_json::json;
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
const JITO_BASE_URL: &str = "https://mainnet.block-engine.jito.wtf/api/v1";

#[derive(Parser, Debug)]
#[command(version, about = "Send 1 bundle via official jito-sdk-rust crate")]
struct Args {
    #[arg(long)]
    config: PathBuf,
    #[arg(long, default_value_t = 200_000_u64)]
    tip_lamports: u64,
    #[arg(long)]
    priority_fee: Option<u64>,
    #[arg(long, default_value_t = 30)]
    wait_secs: u64,
    /// Override base URL (default https://mainnet.block-engine.jito.wtf/api/v1).
    /// Note: the SDK appends /bundles internally so pass the API base.
    #[arg(long)]
    base_url: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let cfg = Config::load(&args.config).context("load config")?;
    let base_url = args
        .base_url
        .clone()
        .unwrap_or_else(|| JITO_BASE_URL.to_string());

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

    // Same structure as minimal_jito_test.rs: 4 ixs, v0, inline tip.
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
        data: b"jito_sdk_test".to_vec(),
    });
    ixs.push(system_instruction::transfer(&payer_pk, &payer_pk, 1));
    ixs.push(system_instruction::transfer(
        &payer_pk,
        &tip_account,
        args.tip_lamports,
    ));

    let v0 = V0Message::try_compile(&payer_pk, &ixs, &[], blockhash)
        .context("compile v0")?;
    let tx = VersionedTransaction::try_new(VersionedMessage::V0(v0), &[&keypair])
        .context("sign v0")?;
    let sig = tx.signatures.first().copied().unwrap_or_default();
    let serialized = bincode::serialize(&tx).context("bincode serialize")?;
    let b64 = base64::engine::general_purpose::STANDARD.encode(&serialized);
    println!("\n=== built tx ===");
    println!("  signature: {}", sig);
    println!("  size: {} bytes", serialized.len());
    println!("  ix count: {}", tx.message.instructions().len());

    // Sanity sim.
    println!("\n=== Solana RPC simulateTransaction ===");
    match rpc.simulate_transaction(&tx) {
        Ok(s) => {
            println!("  err: {:?}", s.value.err);
            println!("  units_consumed: {:?}", s.value.units_consumed);
            if s.value.err.is_some() {
                println!("  abort: tx fails RPC sim");
                return Ok(());
            }
        }
        Err(e) => println!("  sim err: {} (continuing anyway)", e),
    }

    // ----- Use OFFICIAL Jito SDK -----
    println!("\n=== sending via jito-sdk-rust ===");
    println!("  base url: {}", base_url);
    let jito_sdk = JitoJsonRpcSDK::new(&base_url, None);

    // SDK expects either:
    //   * Array of base64 tx strings (it wraps with `{encoding:"base64"}`)
    //   * Or full `[[tx_b64,...], {encoding:"base64"}]` array
    // We pass the simpler form.
    let bundle = json!([b64]);
    let send_resp = jito_sdk
        .send_bundle(Some(bundle), None)
        .await
        .context("send_bundle via jito-sdk-rust")?;
    println!("  response: {}", serde_json::to_string_pretty(&send_resp)?);

    let bundle_id = send_resp
        .get("result")
        .and_then(|v| v.as_str())
        .map(String::from);
    let bundle_id = match bundle_id {
        Some(b) => b,
        None => {
            println!("  no bundle_id in response — Jito rejected");
            return Ok(());
        }
    };
    println!("  bundle_id: {}", bundle_id);

    println!("\n=== waiting {}s ===", args.wait_secs);
    tokio::time::sleep(Duration::from_secs(args.wait_secs)).await;

    // Solana RPC ground truth.
    println!("\n=== Solana RPC getSignatureStatuses (GROUND TRUTH) ===");
    let sig_body = json!({
        "jsonrpc": "2.0", "id": 1,
        "method": "getSignatureStatuses",
        "params": [[sig.to_string()], {"searchTransactionHistory": true}],
    });
    let http = reqwest::Client::new();
    let r = http
        .post(&cfg.rpc.url)
        .header("Content-Type", "application/json")
        .body(sig_body.to_string())
        .send()
        .await?;
    let v: serde_json::Value = r.json().await?;
    println!("{}", serde_json::to_string_pretty(&v)?);

    // Jito's own bundle status.
    println!("\n=== Jito getBundleStatuses (via SDK) ===");
    let bs_resp = jito_sdk
        .get_bundle_statuses(vec![bundle_id.clone()])
        .await
        .context("get_bundle_statuses")?;
    println!("{}", serde_json::to_string_pretty(&bs_resp)?);

    // Also try the inflight variant (5-min look-back, different endpoint).
    println!("\n=== Jito getInflightBundleStatuses (raw POST) ===");
    let inflight_body = json!({
        "jsonrpc": "2.0", "id": 1,
        "method": "getInflightBundleStatuses",
        "params": [[bundle_id]],
    });
    let r = http
        .post(format!("{}/bundles", base_url))
        .header("Content-Type", "application/json")
        .body(inflight_body.to_string())
        .send()
        .await?;
    let v: serde_json::Value = r.json().await?;
    println!("{}", serde_json::to_string_pretty(&v)?);

    Ok(())
}
