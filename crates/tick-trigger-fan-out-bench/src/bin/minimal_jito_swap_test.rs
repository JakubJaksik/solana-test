//! Minimal Jito sendBundle diagnostic with REAL work — a Jupiter swap.
//!
//! Hypothesis under test: Jito's block engine silently drops "obviously
//! synthetic" bundles (memo + 1-lam self-transfer + tip) regardless of
//! transport. Same envelope as `minimal_jito_test`, but the work payload
//! is a genuine SOL→USDC swap composed from Jupiter `/swap-instructions`.
//!
//! Flow:
//!   1. GET  Jupiter /quote
//!   2. POST Jupiter /swap-instructions (returns ix-level breakdown)
//!   3. Resolve ALT accounts from chain
//!   4. Build single v0 tx: [priority_fee] + [swap setup/core/cleanup] + [tip]
//!   5. Local sim via Solana RPC (must pass)
//!   6. POST sendBundle to Jito HTTP /api/v1/bundles
//!   7. Poll getInflightBundleStatuses + getBundleStatuses + ground-truth
//!      getSignatureStatuses

use anyhow::{anyhow, Context, Result};
use base64::Engine as _;
use clap::Parser;
use solana_client::rpc_client::RpcClient;
use solana_commitment_config::CommitmentConfig;
use solana_compute_budget_interface::ComputeBudgetInstruction;
use solana_address_lookup_table_interface::state::AddressLookupTable;
use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    message::{v0::Message as V0Message, AddressLookupTableAccount, VersionedMessage},
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

const JITO_BUNDLES_URL: &str = "https://frankfurt.mainnet.block-engine.jito.wtf/api/v1/bundles";
const JUPITER_BASE: &str = "https://lite-api.jup.ag/swap/v1";
const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";

#[derive(Parser, Debug)]
#[command(version, about = "Send a Jupiter swap + tip as a single-tx Jito bundle (sendBundle)")]
struct Args {
    #[arg(long)]
    config: PathBuf,
    #[arg(long, default_value_t = 1_000_000_u64)]
    tip_lamports: u64,
    #[arg(long)]
    priority_fee: Option<u64>,
    #[arg(long, default_value_t = 120)]
    wait_secs: u64,
    #[arg(long)]
    jito_url: Option<String>,
    #[arg(long, default_value_t = 1000)]
    poll_interval_ms: u64,
    #[arg(long, default_value = SOL_MINT)]
    input_mint: String,
    #[arg(long, default_value = USDC_MINT)]
    output_mint: String,
    /// Input amount in atomic units of input mint. Default 1_000_000 = 0.001 SOL.
    #[arg(long, default_value_t = 1_000_000_u64)]
    amount_in: u64,
    #[arg(long, default_value_t = 100_u16)]
    slippage_bps: u16,
    #[arg(long, default_value = JUPITER_BASE)]
    jupiter_url: String,
    /// x-jito-auth header. Note: HTTP path expects a UUID API key, not gRPC pubkey.
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

    let tip_account = tip_accounts_for(SenderKind::Jito)
        .first()
        .copied()
        .context("no jito tip accounts")?;
    println!("tip account: {}", tip_account);
    println!("tip lamports: {}", args.tip_lamports);
    println!(
        "swap: {} {} -> {} (slippage {} bps)",
        args.amount_in, args.input_mint, args.output_mint, args.slippage_bps
    );

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()?;

    // ---- 1. Jupiter quote ----
    println!("\n=== Jupiter /quote ===");
    let quote_url = format!(
        "{}/quote?inputMint={}&outputMint={}&amount={}&slippageBps={}&onlyDirectRoutes=true",
        args.jupiter_url.trim_end_matches('/'),
        args.input_mint,
        args.output_mint,
        args.amount_in,
        args.slippage_bps
    );
    println!("  url: {}", quote_url);
    let quote: serde_json::Value = http
        .get(&quote_url)
        .send()
        .await
        .context("jupiter quote GET")?
        .json()
        .await
        .context("jupiter quote json")?;
    let out_amount = quote.get("outAmount").and_then(|v| v.as_str()).unwrap_or("?");
    println!("  outAmount: {}", out_amount);

    // ---- 2. Jupiter /swap-instructions ----
    println!("\n=== Jupiter /swap-instructions ===");
    let swap_ix_url = format!("{}/swap-instructions", args.jupiter_url.trim_end_matches('/'));
    let swap_req = serde_json::json!({
        "userPublicKey": payer_pk.to_string(),
        "quoteResponse": quote,
        "wrapAndUnwrapSol": true,
        "dynamicComputeUnitLimit": true,
        "useSharedAccounts": true,
    });
    let swap_resp: serde_json::Value = http
        .post(&swap_ix_url)
        .header("Content-Type", "application/json")
        .body(swap_req.to_string())
        .send()
        .await
        .context("jupiter swap-instructions POST")?
        .json()
        .await
        .context("jupiter swap-instructions json")?;
    if let Some(err) = swap_resp.get("error") {
        return Err(anyhow!("jupiter swap-instructions error: {}", err));
    }

    let compute_unit_limit = swap_resp
        .get("computeUnitLimit")
        .and_then(|v| v.as_u64())
        .unwrap_or(400_000);
    println!("  computeUnitLimit: {}", compute_unit_limit);

    let mut ixs: Vec<Instruction> = Vec::new();

    // priority fee
    let priority_fee = args
        .priority_fee
        .unwrap_or(cfg.tx.priority_fee_microlamports);
    if priority_fee > 0 {
        ixs.push(ComputeBudgetInstruction::set_compute_unit_price(priority_fee));
    }
    ixs.push(ComputeBudgetInstruction::set_compute_unit_limit(
        compute_unit_limit as u32,
    ));

    // Jupiter's computeBudgetInstructions and tokenLedgerInstruction are
    // intentionally skipped — we set our own CU price+limit above.
    if let Some(setup) = swap_resp
        .get("setupInstructions")
        .and_then(|v| v.as_array())
    {
        for ix in setup {
            ixs.push(parse_jup_ix(ix).context("parse setup ix")?);
        }
    }
    let swap_ix = swap_resp
        .get("swapInstruction")
        .context("missing swapInstruction")?;
    ixs.push(parse_jup_ix(swap_ix).context("parse swap ix")?);
    if let Some(cleanup) = swap_resp.get("cleanupInstruction") {
        if !cleanup.is_null() {
            ixs.push(parse_jup_ix(cleanup).context("parse cleanup ix")?);
        }
    }

    // tip — LAST ix, system transfer to Jito tip account
    ixs.push(system_instruction::transfer(
        &payer_pk,
        &tip_account,
        args.tip_lamports,
    ));
    println!("  total ix count (with priority+limit+tip): {}", ixs.len());

    // ---- 3. Resolve ALT accounts ----
    let alt_keys: Vec<Pubkey> = swap_resp
        .get("addressLookupTableAddresses")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|s| s.as_str())
                .filter_map(|s| Pubkey::from_str(s).ok())
                .collect()
        })
        .unwrap_or_default();
    println!("  ALTs: {}", alt_keys.len());
    let alt_accounts = fetch_alts(&rpc, &alt_keys).context("fetch ALTs")?;

    // ---- 4. Compile + sign ----
    let blockhash = rpc.get_latest_blockhash().context("get_latest_blockhash")?;
    println!("blockhash: {}", blockhash);
    let v0 = V0Message::try_compile(&payer_pk, &ixs, &alt_accounts, blockhash)
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

    // ---- 5. Local sim ----
    println!("\n=== Solana RPC simulateTransaction ===");
    match rpc.simulate_transaction(&tx) {
        Ok(sim) => {
            println!("  err: {:?}", sim.value.err);
            println!("  units_consumed: {:?}", sim.value.units_consumed);
            if sim.value.err.is_some() {
                println!("  logs:");
                if let Some(logs) = sim.value.logs {
                    for l in logs.iter().take(40) {
                        println!("    {}", l);
                    }
                }
                println!("  !!! tx fails RPC sim; aborting (Jito would reject too)");
                return Ok(());
            }
        }
        Err(e) => {
            println!("  simulate_transaction err: {} (continuing anyway)", e);
        }
    }

    // ---- 6. POST sendBundle ----
    println!("\n=== POST sendBundle to Jito ===");
    println!("  url: {}", jito_url);
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "sendBundle",
        "params": [[b64], { "encoding": "base64" }],
    });
    let mut req = http
        .post(&jito_url)
        .header("Content-Type", "application/json")
        .body(body.to_string());
    if let Some(auth) = &args.jito_auth {
        println!("  x-jito-auth: {}", auth);
        req = req.header("x-jito-auth", auth.as_str());
    }
    let resp = req.send().await.context("POST sendBundle")?;
    let status = resp.status();
    let body_text = resp.text().await.unwrap_or_default();
    println!("  http status: {}", status);
    println!("  response: {}", body_text);
    let parsed: serde_json::Value = serde_json::from_str(&body_text).unwrap_or_default();
    let bundle_id = match parsed.get("result").and_then(|v| v.as_str()) {
        Some(b) => {
            println!("  bundle_id: {}", b);
            b.to_string()
        }
        None => {
            println!("  no bundle_id (Jito rejected at HTTP)");
            return Ok(());
        }
    };

    // ---- 7. Poll Jito inflight ----
    println!("\n=== polling getInflightBundleStatuses up to {}s ===", args.wait_secs);
    let inflight_url = jito_method_url(&jito_url, "getInflightBundleStatuses");
    let started = Instant::now();
    let poll_interval = Duration::from_millis(args.poll_interval_ms.max(100));
    let final_status = loop {
        let v = post_bundle_status(&http, &inflight_url, "getInflightBundleStatuses", &bundle_id)
            .await?;
        let s = v
            .pointer("/result/value/0/status")
            .and_then(|x| x.as_str())
            .unwrap_or("unknown")
            .to_string();
        let landed_slot = v
            .pointer("/result/value/0/landed_slot")
            .map(|x| x.to_string())
            .unwrap_or_else(|| "null".to_string());
        println!(
            "  t={:>5.1}s inflight status={} landed_slot={}",
            started.elapsed().as_secs_f64(),
            s,
            landed_slot
        );
        if matches!(s.as_str(), "Landed" | "Failed") {
            break v;
        }
        if started.elapsed() >= Duration::from_secs(args.wait_secs) {
            break v;
        }
        tokio::time::sleep(poll_interval).await;
    };
    println!("\n=== final inflight ===");
    println!("{}", serde_json::to_string_pretty(&final_status)?);

    println!("\n=== Jito getBundleStatuses ===");
    let bundle_status_url = jito_method_url(&jito_url, "getBundleStatuses");
    let v = post_bundle_status(&http, &bundle_status_url, "getBundleStatuses", &bundle_id).await?;
    println!("{}", serde_json::to_string_pretty(&v)?);

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

    println!();
    println!("=== interpretation ===");
    println!(" * sig on-chain   -> bundle LANDED. Hypothesis #2 (anti-spam pattern) confirmed.");
    println!(" * sig null + Invalid -> swap bundle ALSO rejected. Move to hypothesis #3:");
    println!("                         HTTP path needs Jito UUID API key (separate from gRPC pubkey).");
    Ok(())
}

fn parse_jup_ix(v: &serde_json::Value) -> Result<Instruction> {
    let program_id = v
        .get("programId")
        .and_then(|x| x.as_str())
        .context("ix.programId missing")?;
    let program_id = Pubkey::from_str(program_id).context("ix.programId parse")?;
    let accounts = v
        .get("accounts")
        .and_then(|x| x.as_array())
        .context("ix.accounts missing")?;
    let metas: Vec<AccountMeta> = accounts
        .iter()
        .map(|a| -> Result<AccountMeta> {
            let pk = a
                .get("pubkey")
                .and_then(|x| x.as_str())
                .context("account.pubkey missing")?;
            let pk = Pubkey::from_str(pk).context("account.pubkey parse")?;
            let is_signer = a.get("isSigner").and_then(|x| x.as_bool()).unwrap_or(false);
            let is_writable = a
                .get("isWritable")
                .and_then(|x| x.as_bool())
                .unwrap_or(false);
            Ok(if is_writable {
                AccountMeta::new(pk, is_signer)
            } else {
                AccountMeta::new_readonly(pk, is_signer)
            })
        })
        .collect::<Result<_>>()?;
    let data_b64 = v
        .get("data")
        .and_then(|x| x.as_str())
        .context("ix.data missing")?;
    let data = base64::engine::general_purpose::STANDARD
        .decode(data_b64)
        .context("ix.data base64 decode")?;
    Ok(Instruction {
        program_id,
        accounts: metas,
        data,
    })
}

fn fetch_alts(rpc: &RpcClient, keys: &[Pubkey]) -> Result<Vec<AddressLookupTableAccount>> {
    let mut out = Vec::with_capacity(keys.len());
    for key in keys {
        let acc = rpc.get_account(key).with_context(|| format!("get ALT {key}"))?;
        let table = AddressLookupTable::deserialize(&acc.data)
            .map_err(|e| anyhow!("deserialize ALT {key}: {e}"))?;
        out.push(AddressLookupTableAccount {
            key: *key,
            addresses: table.addresses.to_vec(),
        });
    }
    Ok(out)
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
        .with_context(|| format!("POST {method}"))?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    let value: serde_json::Value = serde_json::from_str(&text)
        .with_context(|| format!("parse {method} response: HTTP {status} body={text}"))?;
    Ok(value)
}
