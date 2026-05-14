//! CLI: create N durable nonce accounts.
//!
//! Generates N fresh keypairs, batches `create_nonce_account` instructions
//! ~10 per tx, sends via Helius RPC, verifies, and saves both keypair file
//! and config file.

use anyhow::{Context, Result};
use clap::Parser;
use fan_out_bench::wallet::load_keypair_file;
use serde::{Deserialize, Serialize};
use solana_client::rpc_client::RpcClient;
use solana_commitment_config::CommitmentConfig;
use solana_sdk::{
    signature::{Keypair, Signature, Signer},
    transaction::Transaction,
};
use solana_system_interface::instruction as sys_instruction;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "setup-nonces")]
struct Args {
    #[arg(long)]
    rpc_url: String,
    #[arg(long)]
    wallet: PathBuf,
    #[arg(long, default_value = "150")]
    count: usize,
    #[arg(long)]
    output_keypairs: PathBuf,
    #[arg(long)]
    output_config: PathBuf,
    #[arg(long, default_value = "10")]
    batch_size: usize,
}

#[derive(Serialize, Deserialize)]
struct NonceKeypairsFile {
    keypairs_base58: Vec<String>,
}

#[derive(Serialize, Deserialize)]
struct NonceConfigFile {
    accounts: Vec<NonceConfigEntry>,
}

#[derive(Serialize, Deserialize)]
struct NonceConfigEntry {
    id: u16,
    pubkey: String,
    initial_blockhash: String,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let authority = load_keypair_file(&args.wallet).context("load wallet")?;
    tracing::info!(authority = %authority.pubkey(), count = args.count, "setting up nonces");

    let client = RpcClient::new_with_commitment(args.rpc_url.clone(), CommitmentConfig::confirmed());
    let rent_lamports = client
        .get_minimum_balance_for_rent_exemption(80)
        .context("fetch rent exemption")?;
    tracing::info!(rent_lamports, "nonce rent");

    let mut nonce_kps: Vec<Keypair> = (0..args.count).map(|_| Keypair::new()).collect();

    let kp_file = NonceKeypairsFile {
        keypairs_base58: nonce_kps
            .iter()
            .map(|kp| bs58::encode(kp.to_bytes()).into_string())
            .collect(),
    };
    std::fs::write(
        &args.output_keypairs,
        serde_json::to_string_pretty(&kp_file)?,
    )?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&args.output_keypairs, std::fs::Permissions::from_mode(0o600))?;
    }
    tracing::info!(path = ?args.output_keypairs, "keypairs saved");

    let mut signatures: Vec<Signature> = Vec::new();
    for (batch_idx, chunk) in nonce_kps.chunks(args.batch_size).enumerate() {
        let mut ixs = Vec::new();
        for kp in chunk {
            ixs.extend(sys_instruction::create_nonce_account(
                &authority.pubkey(),
                &kp.pubkey(),
                &authority.pubkey(),
                rent_lamports,
            ));
        }
        let blockhash = client.get_latest_blockhash().context("fetch blockhash")?;
        let mut signers: Vec<&Keypair> = vec![&authority];
        signers.extend(chunk.iter());
        let tx = Transaction::new_signed_with_payer(
            &ixs,
            Some(&authority.pubkey()),
            &signers,
            blockhash,
        );
        let sig = client
            .send_and_confirm_transaction(&tx)
            .with_context(|| format!("batch {} create_nonce_account failed", batch_idx))?;
        tracing::info!(batch_idx, sig = %sig, count = chunk.len(), "batch confirmed");
        signatures.push(sig);
    }

    let mut entries: Vec<NonceConfigEntry> = Vec::with_capacity(args.count);
    for (idx, kp) in nonce_kps.drain(..).enumerate() {
        let account = client
            .get_account(&kp.pubkey())
            .with_context(|| format!("get_account for nonce {}", kp.pubkey()))?;
        let state = fan_out_bench::nonce::state::parse_nonce_account_data(&account.data)
            .with_context(|| format!("parse nonce {} data", kp.pubkey()))?;
        if state.authority != authority.pubkey() {
            anyhow::bail!(
                "nonce {} authority mismatch: got {}, expected {}",
                kp.pubkey(),
                state.authority,
                authority.pubkey()
            );
        }
        entries.push(NonceConfigEntry {
            id: idx as u16,
            pubkey: kp.pubkey().to_string(),
            initial_blockhash: state.blockhash.to_string(),
        });
    }

    let config_file = NonceConfigFile { accounts: entries };
    std::fs::write(
        &args.output_config,
        serde_json::to_string_pretty(&config_file)?,
    )?;
    tracing::info!(path = ?args.output_config, count = args.count, "config saved");

    Ok(())
}
