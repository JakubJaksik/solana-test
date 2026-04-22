//! Pre-flight: RPC sanity → wallet balances → approve → calibration swap
//! → cost projection → user prompt. Fail-fast na każdym kroku.

use crate::config::Config;
use crate::rpc::HttpRpcClient;
use crate::wallet::{TxParams, Wallet};
use alloy::primitives::{Address, U256};
use alloy::sol;
use alloy::sol_types::SolCall;
use anyhow::{Context, Result, bail};
use std::io::{self, Write};
use tracing::{info, warn};

sol! {
    function allowance(address owner, address spender) external view returns (uint256);
    function approve(address spender, uint256 amount) external returns (bool);
    function balanceOf(address account) external view returns (uint256);
}

fn hex_encode_0x(b: &[u8]) -> String {
    let mut s = String::with_capacity(2 + b.len() * 2);
    s.push_str("0x");
    for byte in b {
        s.push_str(&format!("{:02x}", byte));
    }
    s
}

/// Zwraca `gasUsed` z receipt dla danej tx hash. Poll co 500 ms, timeout 60 s.
async fn wait_for_receipt(http: &HttpRpcClient, tx_hash_hex: &str) -> Result<u64> {
    use tokio::time::{Duration, sleep, timeout};
    let fut = async {
        loop {
            let payload = format!(
                r#"{{"jsonrpc":"2.0","id":1,"method":"eth_getTransactionReceipt","params":["{}"]}}"#,
                tx_hash_hex
            );
            let resp = http.raw_call(&payload).await?;
            let parsed: serde_json::Value = serde_json::from_str(&resp)?;
            if parsed["result"].is_object() {
                let gas_used_hex = parsed["result"]["gasUsed"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("no gasUsed in receipt"))?;
                let gu = u64::from_str_radix(gas_used_hex.trim_start_matches("0x"), 16)?;
                let status_hex = parsed["result"]["status"].as_str().unwrap_or("0x0");
                if status_hex == "0x0" {
                    return Err(anyhow::anyhow!("tx reverted on-chain"));
                }
                return Ok::<u64, anyhow::Error>(gu);
            }
            sleep(Duration::from_millis(500)).await;
        }
    };
    timeout(Duration::from_secs(60), fut)
        .await
        .context("receipt wait timed out after 60s")?
}

pub async fn erc20_balance(http: &HttpRpcClient, token: Address, owner: Address) -> Result<U256> {
    let data = balanceOfCall { account: owner }.abi_encode();
    let payload = format!(
        r#"{{"jsonrpc":"2.0","id":1,"method":"eth_call","params":[{{"to":"{:?}","data":"{}"}},"latest"]}}"#,
        token,
        hex_encode_0x(&data)
    );
    let resp = http.raw_call(&payload).await?;
    let parsed: serde_json::Value = serde_json::from_str(&resp)?;
    let hex = parsed["result"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("no balanceOf result"))?;
    Ok(U256::from_str_radix(hex.trim_start_matches("0x"), 16)?)
}

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
    read_http: &HttpRpcClient,
    send_http: &HttpRpcClient,
    wallets: &[Wallet],
    skip_prompt: bool,
) -> Result<PreflightOutcome> {
    // 1. RPC sanity (reads)
    info!("pre-flight: step 1 — RPC sanity");
    let chain_id = read_http
        .eth_chain_id()
        .await
        .context("eth_chainId failed")?;
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
        read_http
            .eth_block_number()
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
        let bal = read_http.eth_get_balance(&addr_str).await?;
        if bal.is_zero() {
            bail!("wallet '{}' has zero ETH balance", w.label());
        }
        let nonce = read_http.eth_get_transaction_count(&addr_str).await?;
        w.set_nonce(nonce);
        initial_nonces.push((w.label().to_string(), nonce));
        info!(wallet = %w.label(), address = ?w.address(), balance_wei = %bal, nonce, "loaded");
    }

    // 3. Auto-approve
    info!("pre-flight: step 3 — allowance + auto-approve");
    approve_tokens_if_needed(read_http, send_http, cfg, wallets)
        .await
        .context("auto-approve failed")?;

    // 4. Calibration swap per wallet
    info!("pre-flight: step 4 — calibration swap");
    let base_fee_wei = read_http
        .latest_base_fee()
        .await
        .context("latest_base_fee failed")?;
    let router: Address = cfg.swap.router_address.parse()?;
    let mut gas_limits: Vec<(String, u64)> = Vec::new();
    for w in wallets {
        let gas_used = perform_calibration_swap(read_http, send_http, cfg, w, router, base_fee_wei)
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
        let bal = read_http
            .eth_get_balance(&format!("{:?}", w.address()))
            .await?;
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
    read_http: &HttpRpcClient,
    send_http: &HttpRpcClient,
    cfg: &Config,
    wallets: &[Wallet],
) -> Result<()> {
    let router: Address = cfg.swap.router_address.parse()?;
    let tokens = [
        (cfg.swap.token_a.parse::<Address>()?, "A"),
        (cfg.swap.token_b.parse::<Address>()?, "B"),
    ];
    let threshold = U256::MAX / U256::from(2u64);

    for w in wallets {
        for (token_addr, token_label) in tokens {
            // Read allowance (reads → read_http)
            let call_data = allowanceCall {
                owner: w.address(),
                spender: router,
            }
            .abi_encode();
            let hex_data = hex_encode_0x(&call_data);
            let call_payload = format!(
                r#"{{"jsonrpc":"2.0","id":1,"method":"eth_call","params":[{{"to":"{:?}","data":"{}"}},"latest"]}}"#,
                token_addr, hex_data
            );
            let resp = read_http.raw_call(&call_payload).await?;
            let parsed: serde_json::Value = serde_json::from_str(&resp)?;
            let result_hex = parsed["result"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("allowance call: no result"))?;
            let allowance = U256::from_str_radix(result_hex.trim_start_matches("0x"), 16)?;

            if allowance >= threshold {
                info!(wallet = %w.label(), token = token_label, "allowance sufficient");
                continue;
            }

            info!(wallet = %w.label(), token = token_label, "submitting approve");
            let approve_data = approveCall {
                spender: router,
                amount: U256::MAX,
            }
            .abi_encode();
            let base_fee = read_http.latest_base_fee().await?;
            let tip = (cfg.gas.max_priority_fee_gwei * 1e9) as u128;
            let max_fee = (base_fee as f64 * cfg.gas.max_fee_multiplier) as u128 + tip;
            let nonce = w.consume_nonce();
            let signed = w.sign_eip1559(TxParams {
                chain_id: cfg.chain.chain_id,
                nonce,
                to: token_addr,
                value: U256::ZERO,
                data: approve_data,
                gas_limit: 80_000,
                max_priority_fee_per_gas: tip,
                max_fee_per_gas: max_fee,
            })?;
            let raw_hex = hex_encode_0x(&signed.raw);
            let payload = crate::rpc::build_send_payload(1, &raw_hex);
            // Send → send_http (np. sequencer)
            let outcome = send_http.send_raw_transaction_prepared(&payload).await?;
            let tx_hash = match outcome {
                crate::rpc::SendOutcome::Accepted { tx_hash } => tx_hash,
                crate::rpc::SendOutcome::Rejected { code, message } => {
                    bail!("approve rejected: code={} msg={}", code, message);
                }
            };
            let tx_hash_hex = format!("{:?}", tx_hash);
            // Receipt poll → read_http
            wait_for_receipt(read_http, &tx_hash_hex)
                .await
                .with_context(|| {
                    format!(
                        "approve receipt poll (wallet={}, token={})",
                        w.label(),
                        token_label
                    )
                })?;
            info!(wallet = %w.label(), token = token_label, tx_hash = ?tx_hash, "approve confirmed");
        }
    }
    Ok(())
}

async fn perform_calibration_swap(
    read_http: &HttpRpcClient,
    send_http: &HttpRpcClient,
    cfg: &Config,
    wallet: &Wallet,
    router: Address,
    base_fee_wei: u128,
) -> Result<u64> {
    use crate::swap::{PingPongState, SwapEncoder};
    let encoder = SwapEncoder::new(&cfg.swap, wallet.address())?;
    let token_a: Address = cfg.swap.token_a.parse()?;
    let token_b: Address = cfg.swap.token_b.parse()?;
    let bal_a = erc20_balance(read_http, token_a, wallet.address()).await?;
    let bal_b = erc20_balance(read_http, token_b, wallet.address()).await?;
    let state = PingPongState::initialize(bal_a, bal_b);
    let dir = state.current_direction();

    let data = encoder.encode(dir, 0)?;
    let tip = (cfg.gas.max_priority_fee_gwei * 1e9) as u128;
    let max_fee = (base_fee_wei as f64 * cfg.gas.max_fee_multiplier) as u128 + tip;
    let nonce = wallet.consume_nonce();
    let signed = wallet.sign_eip1559(TxParams {
        chain_id: cfg.chain.chain_id,
        nonce,
        to: router,
        value: U256::ZERO,
        data,
        gas_limit: 250_000,
        max_priority_fee_per_gas: tip,
        max_fee_per_gas: max_fee,
    })?;
    let raw_hex = hex_encode_0x(&signed.raw);
    let payload = crate::rpc::build_send_payload(1, &raw_hex);
    // Send → send_http; receipt poll → read_http
    let outcome = send_http.send_raw_transaction_prepared(&payload).await?;
    let tx_hash = match outcome {
        crate::rpc::SendOutcome::Accepted { tx_hash } => tx_hash,
        crate::rpc::SendOutcome::Rejected { code, message } => {
            bail!("calibration swap rejected: code={} msg={}", code, message);
        }
    };
    let tx_hash_hex = format!("{:?}", tx_hash);
    wait_for_receipt(read_http, &tx_hash_hex).await
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
