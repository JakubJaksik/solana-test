//! Pre-flight: RPC sanity → wallet balances → approve → calibration swap
//! → cost projection → user prompt. Fail-fast na każdym kroku.

use crate::config::Config;
use crate::rpc::HttpRpcClient;
use crate::wallet::Wallet;
use alloy::primitives::{Address, U256};
use anyhow::{Context, Result, bail};
use std::io::{self, Write};
use tracing::{info, warn};

#[derive(Debug, Clone)]
pub struct WalletGas {
    pub label: String,
    pub gas_used: u64,
}

#[derive(Debug, Clone)]
pub struct CostProjection {
    pub per_wallet_realistic_wei: Vec<U256>,
    pub per_wallet_worst_wei: Vec<U256>,
    pub total_realistic_wei: U256,
    pub total_worst_wei: U256,
}

pub fn project_cost(
    wallets_gas: &[WalletGas],
    base_fee_wei: u128,
    tip_wei: u128,
    multiplier: f64,
    tx_per_wallet: u64,
) -> CostProjection {
    let effective_worst = (base_fee_wei as f64 * multiplier) as u128 + tip_wei;
    let effective_realistic = base_fee_wei + tip_wei;
    let mut per_wallet_realistic_wei = Vec::with_capacity(wallets_gas.len());
    let mut per_wallet_worst_wei = Vec::with_capacity(wallets_gas.len());
    let mut total_realistic = U256::ZERO;
    let mut total_worst = U256::ZERO;
    for w in wallets_gas {
        let pr =
            U256::from(w.gas_used) * U256::from(effective_realistic) * U256::from(tx_per_wallet);
        let pw = U256::from(w.gas_used) * U256::from(effective_worst) * U256::from(tx_per_wallet);
        total_realistic += pr;
        total_worst += pw;
        per_wallet_realistic_wei.push(pr);
        per_wallet_worst_wei.push(pw);
    }
    CostProjection {
        per_wallet_realistic_wei,
        per_wallet_worst_wei,
        total_realistic_wei: total_realistic,
        total_worst_wei: total_worst,
    }
}

pub struct PreflightOutcome {
    pub gas_limits: Vec<(String, u64)>,
    pub wallet_initial_nonces: Vec<(String, u64)>,
    pub base_fee_wei: u128,
}

pub async fn run(
    cfg: &Config,
    http: &HttpRpcClient,
    wallets: &[Wallet],
    skip_prompt: bool,
) -> Result<PreflightOutcome> {
    // 1. RPC sanity
    info!("pre-flight: step 1 — RPC sanity");
    let chain_id = http.eth_chain_id().await.context("eth_chainId failed")?;
    if chain_id != cfg.chain.chain_id {
        bail!(
            "chain mismatch: config {} vs RPC {}",
            cfg.chain.chain_id,
            chain_id
        );
    }
    let mut rtt_samples = Vec::new();
    for _ in 0..3 {
        let t = std::time::Instant::now();
        http.eth_block_number()
            .await
            .context("eth_blockNumber failed")?;
        rtt_samples.push(t.elapsed().as_millis() as u64);
    }
    rtt_samples.sort();
    let rtt_p50 = rtt_samples[1];
    if rtt_p50 > 200 {
        bail!("RPC baseline RTT p50 = {} ms — too slow (>200 ms)", rtt_p50);
    } else if rtt_p50 > 50 {
        warn!(rtt_p50, "RPC baseline RTT slow (>50 ms)");
    }

    // 2. Wallet balances + initial nonce
    info!("pre-flight: step 2 — wallet validation");
    let mut initial_nonces = Vec::new();
    for w in wallets {
        let addr_str = format!("{:?}", w.address());
        let bal = http.eth_get_balance(&addr_str).await?;
        if bal.is_zero() {
            bail!("wallet '{}' has zero ETH balance", w.label());
        }
        let nonce = http.eth_get_transaction_count(&addr_str).await?;
        w.set_nonce(nonce);
        initial_nonces.push((w.label().to_string(), nonce));
        info!(wallet = %w.label(), address = ?w.address(), balance_wei = %bal, nonce, "loaded");
    }

    // 3. Auto-approve (TODO Task 11)
    info!("pre-flight: step 3 — allowance + auto-approve");
    approve_tokens_if_needed(http, cfg, wallets)
        .await
        .context("auto-approve failed")?;

    // 4. Calibration swap per wallet (TODO Task 11)
    info!("pre-flight: step 4 — calibration swap");
    let base_fee_wei = http
        .latest_base_fee()
        .await
        .context("latest_base_fee failed")?;
    let router: Address = cfg.swap.router_address.parse()?;
    let mut gas_limits: Vec<(String, u64)> = Vec::new();
    for w in wallets {
        let gas_used = perform_calibration_swap(http, cfg, w, router, base_fee_wei)
            .await
            .with_context(|| format!("calibration swap failed for wallet {}", w.label()))?;
        let gas_limit = (gas_used as f64 * 1.2) as u64;
        gas_limits.push((w.label().to_string(), gas_limit));
        info!(wallet = %w.label(), gas_used, gas_limit, "calibration done");
    }

    // 5. Cost projection
    info!("pre-flight: step 5 — cost projection");
    let tip_wei = (cfg.gas.max_priority_fee_gwei * 1e9) as u128;
    let wallets_gas: Vec<WalletGas> = gas_limits
        .iter()
        .map(|(l, g)| WalletGas {
            label: l.clone(),
            gas_used: *g,
        })
        .collect();
    let slots_count = (cfg.timing.end_ms - cfg.timing.start_ms) / cfg.timing.step_ms + 1;
    let tx_per_wallet = slots_count * cfg.timing.samples_per_wallet_per_slot;
    let projection = project_cost(
        &wallets_gas,
        base_fee_wei,
        tip_wei,
        cfg.gas.max_fee_multiplier,
        tx_per_wallet,
    );

    // Balance check
    for (i, w) in wallets.iter().enumerate() {
        let bal = http.eth_get_balance(&format!("{:?}", w.address())).await?;
        if bal < projection.per_wallet_worst_wei[i] {
            bail!(
                "wallet '{}' balance {} < worst-case cost {}",
                w.label(),
                bal,
                projection.per_wallet_worst_wei[i]
            );
        }
    }

    // 6. User confirmation
    print_summary(
        cfg,
        &projection,
        &wallets_gas,
        &SummaryParams {
            chain_id,
            rtt_p50,
            base_fee_wei,
            tx_per_wallet,
            slots_count,
        },
    );
    if !skip_prompt {
        print!("\n Proceed? [y/N] ");
        io::stdout().flush().ok();
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let answer = input.trim().to_lowercase();
        if answer != "y" && answer != "yes" {
            bail!("user aborted pre-flight");
        }
    }

    Ok(PreflightOutcome {
        gas_limits,
        wallet_initial_nonces: initial_nonces,
        base_fee_wei,
    })
}

// --- Placeholders for Task 11 ---

async fn approve_tokens_if_needed(
    _http: &HttpRpcClient,
    _cfg: &Config,
    _wallets: &[Wallet],
) -> Result<()> {
    // TODO (Task 11): build approve calldata (`approve(router, uint256::MAX)`),
    // sign + send per wallet per token, wait for receipts.
    bail!("approve_tokens_if_needed not implemented; fill in Task 11");
}

async fn perform_calibration_swap(
    _http: &HttpRpcClient,
    _cfg: &Config,
    _wallet: &Wallet,
    _router: Address,
    _base_fee_wei: u128,
) -> Result<u64> {
    // TODO (Task 11): build swap tx, submit, poll receipt for gasUsed.
    bail!("perform_calibration_swap not implemented; fill in Task 11");
}

struct SummaryParams {
    chain_id: u64,
    rtt_p50: u64,
    base_fee_wei: u128,
    tx_per_wallet: u64,
    slots_count: u64,
}

fn print_summary(
    cfg: &Config,
    proj: &CostProjection,
    wallets_gas: &[WalletGas],
    params: &SummaryParams,
) {
    let SummaryParams {
        chain_id,
        rtt_p50,
        base_fee_wei,
        tx_per_wallet,
        slots_count,
    } = params;
    println!();
    println!("══════════════════════════════════════════════════════════");
    println!(" PRE-FLIGHT SUMMARY");
    println!("══════════════════════════════════════════════════════════");
    println!(" Chain: {} ({})", cfg.chain.name, chain_id);
    println!(" RPC RTT baseline (p50): {} ms", rtt_p50);
    println!(" Current baseFee: {} wei", base_fee_wei);
    println!();
    println!(" Plan:");
    println!(
        "   Slots:        {} ({}-{} ms, step {})",
        slots_count, cfg.timing.start_ms, cfg.timing.end_ms, cfg.timing.step_ms
    );
    println!(
        "   Samples/slot: {} ({} wallets × {})",
        cfg.wallets.len() as u64 * cfg.timing.samples_per_wallet_per_slot,
        cfg.wallets.len(),
        cfg.timing.samples_per_wallet_per_slot
    );
    println!(
        "   Total tx:     {}",
        tx_per_wallet * cfg.wallets.len() as u64
    );
    println!();
    println!(" Wallets & calibration:");
    for (i, w) in wallets_gas.iter().enumerate() {
        println!(
            "   {}  gas_used={:>7}  realistic_cost={} wei  worst_cost={} wei",
            w.label, w.gas_used, proj.per_wallet_realistic_wei[i], proj.per_wallet_worst_wei[i]
        );
    }
    println!();
    println!(
        " Total cost: realistic={} wei, worst={} wei",
        proj.total_realistic_wei, proj.total_worst_wei
    );
    println!("══════════════════════════════════════════════════════════");
}
