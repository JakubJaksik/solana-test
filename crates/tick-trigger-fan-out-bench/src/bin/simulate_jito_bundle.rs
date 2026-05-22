//! Diagnostic tool: build one Jito bundle exactly like the preparer does and
//! submit it to `simulateBundle` to surface the per-tx error reason.
//!
//! Usage:
//!   simulate_jito_bundle --config phase3-config.json
//!   simulate_jito_bundle --config phase3-config.json --tip-lamports 100000
//!   simulate_jito_bundle --config phase3-config.json --no-nonce   (use fresh bh for Tx1)
//!
//! The bundle is NOT actually submitted to leaders — only simulated against
//! current chain state, so no fees are charged and no nonce state changes.

use anyhow::{Context, Result};
use base64::Engine as _;
use clap::Parser;
use serde_json::{json, Value};
use solana_client::rpc_client::RpcClient;
use solana_commitment_config::CommitmentConfig;
use solana_sdk::signer::Signer;
use std::path::PathBuf;
use std::sync::Arc;
use tick_trigger_fan_out_bench::config::{Config, SenderKind};
use tick_trigger_fan_out_bench::nonce::bootstrap::bootstrap as bootstrap_nonces;
use tick_trigger_fan_out_bench::tip_accounts::{tip_accounts_for, TipAccountRotator};
use tick_trigger_fan_out_bench::tx_builder::{self, BuildParams, NonceParams};
use tick_trigger_fan_out_bench::wallet;

#[derive(Parser, Debug)]
struct Args {
    /// Path to phase3 config json
    #[arg(long)]
    config: PathBuf,
    /// Override tip lamports (default: tip_floor_lamports from config)
    #[arg(long)]
    tip_lamports: Option<u64>,
    /// Use fresh blockhash for Tx1 (skip durable nonce path) — useful to
    /// isolate whether the 2-tx structure works at all
    #[arg(long, default_value_t = false)]
    no_nonce: bool,
    /// RPC URL override (must be a Jito-fork RPC that supports
    /// `simulateBundle`, e.g. dedicated Helius / Jito mainnet RPC). If not
    /// set, uses `cfg.rpc.url` from the config file.
    #[arg(long)]
    rpc_url: Option<String>,
    /// Commitment for blockhash fetch: processed / confirmed / finalized.
    /// `processed` is freshest but may not be propagated yet, causing
    /// BlockhashNotFound on other validators.
    #[arg(long, default_value = "confirmed")]
    commitment: String,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let cfg = Config::load(&args.config)?;

    // ── 1. Wallet ──
    let keypair = Arc::new(wallet::load_keypair(&cfg.wallet.keypair_path)?);
    println!("payer (main wallet): {}", keypair.pubkey());

    // ── 2. Find Jito sender config ──
    let jito = cfg
        .senders
        .iter()
        .find(|s| s.kind == SenderKind::Jito)
        .ok_or_else(|| anyhow::anyhow!("no jito sender in config"))?;
    let tip_lamports = args.tip_lamports.unwrap_or(jito.tip_floor_lamports);
    println!("tip lamports: {}", tip_lamports);

    // ── 3. RPC + blockhash ──
    // IMPORTANT: blockhash MUST come from a node whose view of chain is in
    // sync with the validators Jito routes bundles to. Public RPC
    // (api.mainnet.solana.com) lags vs Jito leaders → blockhash returns
    // BlockhashNotFound during validation → bundle Invalid.
    let bh_url = args
        .rpc_url
        .clone()
        .unwrap_or_else(|| cfg.rpc.url.clone());
    println!("blockhash source: {} (commitment={})", bh_url, args.commitment);
    let commitment = match args.commitment.as_str() {
        "processed" => CommitmentConfig::processed(),
        "confirmed" => CommitmentConfig::confirmed(),
        "finalized" => CommitmentConfig::finalized(),
        other => anyhow::bail!("invalid commitment: {}", other),
    };
    let rpc = RpcClient::new_with_commitment(bh_url, commitment);
    let fresh_bh = rpc.get_latest_blockhash().context("get_latest_blockhash")?;
    println!("fresh blockhash: {}", fresh_bh);

    // ── 4. Nonce (unless --no-nonce) ──
    let (tx1_bh, nonce_params) = if args.no_nonce {
        println!("--no-nonce mode: Tx1 uses fresh blockhash");
        (fresh_bh, None)
    } else {
        if !cfg.nonce.enabled {
            anyhow::bail!("nonce mode not enabled in config; use --no-nonce to bypass");
        }
        let nonces = bootstrap_nonces(&rpc, &cfg.nonce.config_path, &keypair.pubkey())
            .context("bootstrap nonces")?;
        let (id, nonce_pk, stored_hash) = nonces
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("no nonce accounts in nonce-config.json"))?;
        println!(
            "nonce: id={} pubkey={} stored_hash={}",
            id, nonce_pk, stored_hash
        );
        let np = NonceParams {
            nonce_pubkey: nonce_pk,
            authority: keypair.pubkey(),
        };
        (stored_hash, Some(np))
    };

    // ── 5. Tip account (random from Jito's 8 tip accounts) ──
    let rotator = TipAccountRotator::new(tip_accounts_for(SenderKind::Jito).to_vec());
    let tip_account = rotator
        .next()
        .ok_or_else(|| anyhow::anyhow!("no jito tip accounts"))?;
    println!("tip account: {}", tip_account);

    // ── 6. Build single tx (mode 1: tip from main wallet, durable nonce) ──
    let tx1 = tx_builder::build(BuildParams {
        payer: &keypair,
        blockhash: tx1_bh,
        sender_id: jito.id,
        trigger_id: 0xDEADBEEF,
        tip_account: Some(tip_account),
        tip_lamports,
        nonce: nonce_params,
        tx_cfg: &cfg.tx,
        fund_tipper: None,
    });

    println!("\nTx1 ixs count: {}", tx1.tx.message.instructions.len());
    println!("Tx1 signature: {}", tx1.signature);

    // ── 7. Serialize → base64 ──
    let tx1_b64 = base64::engine::general_purpose::STANDARD
        .encode(&bincode::serialize(&tx1.tx).unwrap());
    // Used to be a 2-tx bundle; now mode 1 = single tx. Keep variable for
    // backward compat with the per-tx fallback below (only sends Tx1).
    let _ = &fresh_bh;

    // ── 8. Submit simulateBundle to Jito-fork RPC ──
    let rpc_url = args.rpc_url.unwrap_or_else(|| cfg.rpc.url.clone());
    println!("\nSubmitting simulateBundle to: {}", rpc_url);
    println!("(must be a Jito-fork RPC — Helius dedicated, Jito-Solana RPC, etc.)");
    let payload = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "simulateBundle",
        "params": [{
            "encodedTransactions": [tx1_b64]
        }]
    });
    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .build()?;
    let resp = client
        .post(&rpc_url)
        .header("Content-Type", "application/json")
        .body(payload.to_string())
        .send()?;
    let status = resp.status();
    let body: Value = resp.json().unwrap_or_else(|_| json!({"raw":"<not json>"}));
    println!("\n=== simulateBundle response (HTTP {}) ===", status);
    println!("{}", serde_json::to_string_pretty(&body)?);

    // ── 9. Fallback: per-tx simulateTransaction on the same RPC ──
    // Useful when simulateBundle is unavailable: simulates Tx1 alone.
    // Tx2 alone WILL FAIL pre-sim (tipper has 0 balance) — that's a known
    // limitation of stand-alone tx simulation vs bundle simulation.
    println!("\n--- Fallback: simulateTransaction for Tx1 ---");
    for (name, b64) in [("Tx1", &tx1_b64)] {
        // (1) strict mode: keep blockhash, sigVerify on — same conditions as
        // leader pre-validation outside of bundle context.
        let p_strict = json!({
            "jsonrpc":"2.0","id":1,"method":"simulateTransaction",
            "params":[b64, {"encoding":"base64","sigVerify":true,"replaceRecentBlockhash":false}]
        });
        let r = client.post(&rpc_url)
            .header("Content-Type","application/json")
            .body(p_strict.to_string()).send()?;
        let s = r.status();
        let b: Value = r.json().unwrap_or_else(|_| json!({"raw":"<not json>"}));
        println!("\n[{}] simulateTransaction (strict, keep_blockhash) HTTP {}", name, s);
        println!("{}", serde_json::to_string_pretty(&b)?);

        // (2) replace blockhash + skip sigVerify — isolates structural / account /
        // balance issues from blockhash freshness.
        let p_relaxed = json!({
            "jsonrpc":"2.0","id":1,"method":"simulateTransaction",
            "params":[b64, {"encoding":"base64","sigVerify":false,"replaceRecentBlockhash":true}]
        });
        let r2 = client.post(&rpc_url)
            .header("Content-Type","application/json")
            .body(p_relaxed.to_string()).send()?;
        let s2 = r2.status();
        let b2: Value = r2.json().unwrap_or_else(|_| json!({"raw":"<not json>"}));
        println!("\n[{}] simulateTransaction (relaxed, replaceBlockhash=true) HTTP {}", name, s2);
        println!("{}", serde_json::to_string_pretty(&b2)?);
    }

    Ok(())
}
