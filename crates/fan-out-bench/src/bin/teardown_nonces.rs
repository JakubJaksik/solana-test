//! CLI: withdraw rent from all nonce accounts back to authority.

use anyhow::{Context, Result};
use clap::Parser;
use fan_out_bench::wallet::load_keypair_file;
use serde::{Deserialize, Serialize};
use solana_client::rpc_client::RpcClient;
use solana_commitment_config::CommitmentConfig;
use solana_sdk::{
    signature::{Keypair, Signer},
    transaction::Transaction,
};
use solana_system_interface::instruction as sys_instruction;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "teardown-nonces")]
struct Args {
    #[arg(long)]
    rpc_url: String,
    #[arg(long)]
    wallet: PathBuf,
    #[arg(long)]
    keypairs: PathBuf,
    #[arg(long, default_value = "15")]
    batch_size: usize,
}

#[derive(Serialize, Deserialize)]
struct NonceKeypairsFile {
    keypairs_base58: Vec<String>,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let authority = load_keypair_file(&args.wallet).context("load wallet")?;
    let keypairs_path = fan_out_bench::wallet::expand_tilde(&args.keypairs);
    let kp_file_bytes = std::fs::read(&keypairs_path).context("read keypairs file")?;
    let kp_file: NonceKeypairsFile = serde_json::from_slice(&kp_file_bytes).context("parse keypairs file")?;

    let nonce_kps: Vec<Keypair> = kp_file
        .keypairs_base58
        .iter()
        .map(|s| {
            let bytes = bs58::decode(s).into_vec().context("decode keypair base58")?;
            if bytes.len() != 64 {
                anyhow::bail!("keypair bytes not 64 long, got {}", bytes.len());
            }
            let arr: [u8; 32] = bytes[..32]
                .try_into()
                .map_err(|_| anyhow::anyhow!("internal: 32-byte slice failed"))?;
            Ok(Keypair::new_from_array(arr))
        })
        .collect::<Result<Vec<_>>>()?;

    tracing::info!(count = nonce_kps.len(), "withdrawing nonces");
    let client = RpcClient::new_with_commitment(args.rpc_url.clone(), CommitmentConfig::confirmed());
    let rent_lamports = client.get_minimum_balance_for_rent_exemption(80)?;

    for (batch_idx, chunk) in nonce_kps.chunks(args.batch_size).enumerate() {
        let ixs: Vec<_> = chunk
            .iter()
            .map(|kp| {
                sys_instruction::withdraw_nonce_account(
                    &kp.pubkey(),
                    &authority.pubkey(),
                    &authority.pubkey(),
                    rent_lamports,
                )
            })
            .collect();
        let blockhash = client.get_latest_blockhash()?;
        let tx = Transaction::new_signed_with_payer(
            &ixs,
            Some(&authority.pubkey()),
            &[&authority],
            blockhash,
        );
        let sig = client
            .send_and_confirm_transaction(&tx)
            .with_context(|| format!("batch {} withdraw failed", batch_idx))?;
        tracing::info!(batch_idx, sig = %sig, count = chunk.len(), "batch confirmed");
    }
    tracing::info!("teardown complete");
    Ok(())
}
