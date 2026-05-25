//! Minimal Jito bundle diagnostic — strips EVERYTHING we suspect could be
//! wrong with our other paths:
//!   * no tonic / no gRPC
//!   * no auth (anonymous, 1 tps default rate)
//!   * one tx with tip inlined (no separate tip tx)
//!   * v0 VersionedTransaction, fresh blockhash, no nonce
//!   * region-specific HTTP endpoint from config (e.g. frankfurt)
//!
//! Sends one bundle, then polls BOTH:
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
use std::time::{Duration, Instant};
use tick_trigger_fan_out_bench::config::{Config, SenderKind};
use tick_trigger_fan_out_bench::tip_accounts::tip_accounts_for;
use tick_trigger_fan_out_bench::wallet;

const MEMO_PROGRAM_ID: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";
const JITO_BUNDLES_URL: &str = "https://frankfurt.mainnet.block-engine.jito.wtf/api/v1/bundles";

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
    /// How long to poll status, in seconds.
    #[arg(long, default_value_t = 30)]
    wait_secs: u64,
    /// Alternative Jito bundles URL (e.g. region-specific). Defaults to
    /// first configured Jito region, else Frankfurt.
    #[arg(long)]
    jito_url: Option<String>,
    /// Poll interval for Jito inflight status.
    #[arg(long, default_value_t = 1000)]
    poll_interval_ms: u64,
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
    /// Use Jito's `/api/v1/transactions` `sendTransaction` endpoint
    /// instead of `/api/v1/bundles` `sendBundle`. This is single-tx
    /// forwarding via Jito's relayer (no bundle auction). Different
    /// validation pipeline — useful to test whether bundle-specific
    /// filtering is causing our Invalid status.
    #[arg(long)]
    use_send_transaction: bool,
    /// Send as `x-jito-auth: <value>` HTTP header. For pubkey-whitelist
    /// rate-limit tier (e.g. 2 TPS), pass your registered Jito gRPC auth
    /// pubkey. Without it the request runs at the anonymous 1 TPS default.
    #[arg(long)]
    jito_auth: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let cfg = Config::load(&args.config).context("load config")?;
    let jito_url = args
        .jito_url
        .clone()
        .unwrap_or_else(|| default_jito_bundles_url(&cfg));

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

    // POST to Jito — choose endpoint based on --use-send-transaction flag.
    let (endpoint_url, body) = if args.use_send_transaction {
        // sendTransaction endpoint: /api/v1/transactions, single tx (not array).
        // Replace /bundles with /transactions in the URL.
        let tx_url = jito_url.replace("/bundles", "/transactions");
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendTransaction",
            "params": [b64, { "encoding": "base64", "skipPreflight": true }],
        });
        (tx_url, body)
    } else {
        // sendBundle endpoint: /api/v1/bundles, array of tx.
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendBundle",
            "params": [[b64], { "encoding": "base64" }],
        });
        (jito_url.clone(), body)
    };
    println!("\n=== POST to Jito ===");
    println!("  url: {}", endpoint_url);
    println!(
        "  method: {}",
        if args.use_send_transaction { "sendTransaction" } else { "sendBundle" }
    );
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    let mut req = http
        .post(&endpoint_url)
        .header("Content-Type", "application/json")
        .body(body.to_string());
    if let Some(auth) = &args.jito_auth {
        println!("  x-jito-auth: {} (whitelist tier)", auth);
        req = req.header("x-jito-auth", auth.as_str());
    }
    let resp = req.send().await.context("POST to Jito")?;
    let status = resp.status();
    let body_text = resp.text().await.unwrap_or_default();
    println!("  http status: {}", status);
    println!("  response: {}", body_text);

    let parsed: serde_json::Value = serde_json::from_str(&body_text).unwrap_or_default();
    let bundle_id = parsed
        .get("result")
        .and_then(|v| v.as_str())
        .map(String::from);
    // For sendTransaction, `result` is the tx signature — also useful but not
    // a bundle_id. We treat it as the ID we'll query for status.
    let bundle_id = match bundle_id {
        Some(b) => {
            println!(
                "  {}: {}",
                if args.use_send_transaction { "tx_signature returned" } else { "bundle_id" },
                b
            );
            b
        }
        None => {
            println!("  no result returned (Jito rejected at HTTP)");
            return Ok(());
        }
    };

    if !args.use_send_transaction {
        println!("\n=== polling Jito inflight status for up to {}s ===", args.wait_secs);
        let inflight_url = jito_method_url(&jito_url, "getInflightBundleStatuses");
        println!("  status url: {}", inflight_url);
        let started = Instant::now();
        let poll_interval = Duration::from_millis(args.poll_interval_ms.max(100));
        let mut last_status: Option<serde_json::Value> = None;
        loop {
            let v = post_bundle_status(&http, &inflight_url, "getInflightBundleStatuses", &bundle_id)
                .await?;
            let status = v
                .pointer("/result/value/0/status")
                .and_then(|x| x.as_str())
                .unwrap_or("unknown");
            let landed_slot = v
                .pointer("/result/value/0/landed_slot")
                .map(|x| x.to_string())
                .unwrap_or_else(|| "null".to_string());
            println!(
                "  t={:>5.1}s inflight status={} landed_slot={}",
                started.elapsed().as_secs_f64(),
                status,
                landed_slot
            );
            last_status = Some(v);
            if matches!(status, "Landed" | "Failed") {
                break;
            }
            if started.elapsed() >= Duration::from_secs(args.wait_secs) {
                break;
            }
            tokio::time::sleep(poll_interval).await;
        }

        if let Some(v) = last_status {
            println!("\n=== final Jito getInflightBundleStatuses ===");
            println!("{}", serde_json::to_string_pretty(&v)?);
        }

        println!("\n=== Jito getBundleStatuses ===");
        let bundle_status_url = jito_method_url(&jito_url, "getBundleStatuses");
        println!("  status url: {}", bundle_status_url);
        let v = post_bundle_status(&http, &bundle_status_url, "getBundleStatuses", &bundle_id)
            .await?;
        println!("{}", serde_json::to_string_pretty(&v)?);
    } else {
        println!("\n=== waiting {}s for tx to land ===", args.wait_secs);
        tokio::time::sleep(Duration::from_secs(args.wait_secs)).await;
    }

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

    if args.use_send_transaction {
        println!("\n=== skipping Jito bundle status (sendTransaction mode) ===");
        println!("  (the `result` was a tx signature, not a bundle uuid)");
    }

    println!();
    println!("=== interpretation ===");
    println!(" * getSignatureStatuses null     -> tx never landed on chain");
    println!(" * getSignatureStatuses {{slot, err:null}} -> tx LANDED (bundle worked)");
    println!(" * Jito status Landed            -> Jito processed + landed");
    println!(" * Jito status Pending           -> still in flight / auction path");
    println!(" * Jito status Failed            -> all receiving regions marked failed");
    println!(" * Jito status Invalid           -> not in inflight system now; can be transient early or expired after timeout");
    Ok(())
}

fn default_jito_bundles_url(cfg: &Config) -> String {
    cfg.senders
        .iter()
        .find(|s| s.kind == SenderKind::Jito && s.enabled && !s.regions.is_empty())
        .map(|s| s.endpoint_url.replace("{region}", &s.regions[0]))
        .unwrap_or_else(|| JITO_BUNDLES_URL.to_string())
}

fn jito_method_url(bundles_url: &str, method: &str) -> String {
    let (url, query) = bundles_url
        .split_once('?')
        .map(|(u, q)| (u, Some(q)))
        .unwrap_or((bundles_url, None));
    let method_url = url
        .strip_suffix("/bundles")
        .map(|prefix| format!("{prefix}/{method}"))
        .unwrap_or_else(|| url.to_string());
    match query {
        Some(q) => format!("{method_url}?{q}"),
        None => method_url,
    }
}

async fn post_bundle_status(
    http: &reqwest::Client,
    url: &str,
    method: &str,
    bundle_id: &str,
) -> Result<serde_json::Value> {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": [[bundle_id]],
    });
    let resp = http
        .post(url)
        .header("Content-Type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .with_context(|| format!("POST {method} to Jito"))?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    let value: serde_json::Value = serde_json::from_str(&text)
        .with_context(|| format!("parse {method} JSON response: HTTP {status} body={text}"))?;
    Ok(value)
}
