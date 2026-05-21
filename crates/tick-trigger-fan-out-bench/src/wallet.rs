//! Wallet keypair loading. Solana CLI JSON-array format (64-byte secret key).

use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use std::path::Path;

pub fn load_keypair(path: &Path) -> anyhow::Result<Keypair> {
    let expanded = expand_tilde(path);
    let bytes_text = std::fs::read_to_string(&expanded)?;
    let bytes: Vec<u8> = serde_json::from_str(&bytes_text)?;
    if bytes.len() != 64 {
        anyhow::bail!("expected 64-byte secret key array, got {}", bytes.len());
    }
    let kp = Keypair::try_from(bytes.as_slice())
        .map_err(|e| anyhow::anyhow!("invalid keypair bytes: {}", e))?;
    tracing::info!(pubkey = %kp.pubkey(), path = %expanded.display(), "wallet loaded");
    Ok(kp)
}

fn expand_tilde(path: &Path) -> std::path::PathBuf {
    let s = path.to_string_lossy();
    if let Some(stripped) = s.strip_prefix("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            let mut p = std::path::PathBuf::from(home);
            p.push(stripped);
            return p;
        }
    }
    path.to_path_buf()
}
