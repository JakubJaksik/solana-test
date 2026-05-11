use std::path::Path;
use anyhow::Context;
use serde::Deserialize;
use solana_compute_budget_interface::ComputeBudgetInstruction;
use solana_sdk::{
    hash::Hash,
    instruction::Instruction,
    signature::{read_keypair_file, Keypair, Signature, Signer},
    transaction::Transaction,
};
use solana_system_interface::instruction as system_instruction;

/// User-friendly keypair config:
/// ```json
/// { "pubKey": "GiFrVbu...", "privKey": "5KMno..." }
/// ```
/// `privKey` is a base58-encoded 64-byte (secret+public) or 32-byte (secret only)
/// keypair — same format that Phantom/Solflare wallets export.
/// `pubKey` is optional; if present, we verify it matches the one derived from
/// `privKey` (sanity check that you didn't mix up files).
#[derive(Deserialize)]
struct KeypairConfig {
    #[serde(rename = "privKey", alias = "private_key", alias = "secret")]
    priv_key: String,
    #[serde(default, rename = "pubKey", alias = "public_key", alias = "pubkey")]
    pub_key: Option<String>,
}

/// Load a keypair from one of these formats (tried in order):
/// 1. **Friendly JSON config** `{"pubKey": "...", "privKey": "..."}` (recommended)
/// 2. **Solana CLI JSON array** `[1, 2, 3, ...]` (64 bytes secret+public)
/// 3. **Plain base58 string** — Phantom/Solflare export, 32 or 64 bytes
///
/// In all cases the public key is derived deterministically from the secret.
/// If `pubKey` is provided in the JSON config, we verify it matches.
pub fn load_keypair(path: &Path) -> anyhow::Result<Keypair> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("read keypair file {}", path.display()))?;
    let trimmed = content.trim();

    // 1. Try friendly JSON config first
    if let Ok(cfg) = serde_json::from_str::<KeypairConfig>(trimmed) {
        let kp = keypair_from_base58(&cfg.priv_key)?;
        if let Some(claimed) = cfg.pub_key.as_deref() {
            let claimed = claimed.trim();
            let derived = kp.pubkey().to_string();
            if claimed != derived {
                return Err(anyhow::anyhow!(
                    "pubKey mismatch in keypair config: claimed {claimed} but derived {derived}"
                ));
            }
        }
        return Ok(kp);
    }

    // 2. Try standard Solana CLI JSON array format
    if let Ok(kp) = read_keypair_file(path) {
        return Ok(kp);
    }

    // 3. Fall back: file contains a raw base58 string
    keypair_from_base58(trimmed)
}

fn keypair_from_base58(s: &str) -> anyhow::Result<Keypair> {
    let s = s.trim();
    let bytes = bs58::decode(s)
        .into_vec()
        .with_context(|| "decode base58 priv key")?;
    let secret: [u8; 32] = match bytes.len() {
        64 => bytes[..32]
            .try_into()
            .map_err(|_| anyhow::anyhow!("internal: 64-byte slice into [u8;32] failed"))?,
        32 => bytes
            .as_slice()
            .try_into()
            .map_err(|_| anyhow::anyhow!("internal: 32-byte slice into [u8;32] failed"))?,
        n => {
            return Err(anyhow::anyhow!(
                "base58 priv key has unexpected length: {n} bytes (expected 32 or 64)"
            ));
        }
    };
    Ok(Keypair::new_from_array(secret))
}

/// Build + sign a self-transfer of `amount_lamports` from `payer` to itself,
/// with `priority_fee_microlamports` set via ComputeBudget.
pub fn build_self_transfer(
    payer: &Keypair,
    amount_lamports: u64,
    priority_fee_microlamports: u64,
    blockhash: Hash,
) -> Transaction {
    let payer_pk = payer.pubkey();
    let mut ixs: Vec<Instruction> = Vec::with_capacity(3);
    // priority fee (compute unit price); 0 microlamports means skip the ix
    if priority_fee_microlamports > 0 {
        ixs.push(ComputeBudgetInstruction::set_compute_unit_price(priority_fee_microlamports));
    }
    // tight compute unit limit — self-transfer is well under 200 CUs
    ixs.push(ComputeBudgetInstruction::set_compute_unit_limit(450));
    // self-transfer
    ixs.push(system_instruction::transfer(&payer_pk, &payer_pk, amount_lamports));

    let mut tx = Transaction::new_with_payer(&ixs, Some(&payer_pk));
    tx.sign(&[payer], blockhash);
    tx
}

/// Serialize tx to wire format (raw bincode-encoded signed transaction).
pub fn serialize_tx(tx: &Transaction) -> Vec<u8> {
    bincode::serialize(tx).expect("transaction serialization never fails")
}

/// Extract the first signature (= the tx's identifier on chain).
pub fn primary_signature(tx: &Transaction) -> Signature {
    tx.signatures[0]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_self_transfer_signs_and_serializes() {
        let kp = Keypair::new();
        let bh = Hash::new_unique();
        let tx = build_self_transfer(&kp, 1, 5000, bh);
        assert_eq!(tx.signatures.len(), 1);
        assert!(tx.is_signed());
        let bytes = serialize_tx(&tx);
        assert!(bytes.len() > 100); // realistic tx size
        let sig = primary_signature(&tx);
        assert_ne!(sig, Signature::default());
    }

    #[test]
    fn zero_priority_fee_skips_compute_unit_price() {
        let kp = Keypair::new();
        let bh = Hash::new_unique();
        let tx = build_self_transfer(&kp, 1, 0, bh);
        // 2 instructions: compute_unit_limit + transfer
        assert_eq!(tx.message.instructions.len(), 2);
    }

    #[test]
    fn priority_fee_adds_compute_unit_price() {
        let kp = Keypair::new();
        let bh = Hash::new_unique();
        let tx = build_self_transfer(&kp, 1, 1000, bh);
        // 3 instructions: cu_price + cu_limit + transfer
        assert_eq!(tx.message.instructions.len(), 3);
    }

    #[test]
    fn load_keypair_roundtrip() {
        use solana_sdk::signature::write_keypair_file;
        let kp = Keypair::new();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kp.json");
        write_keypair_file(&kp, &path).unwrap();
        let loaded = load_keypair(&path).unwrap();
        assert_eq!(loaded.pubkey(), kp.pubkey());
    }

    #[test]
    fn load_keypair_base58_64_bytes_phantom_format() {
        // Phantom/Solflare exports the FULL 64-byte keypair (secret+public)
        // as a single base58 string.
        let kp = Keypair::new();
        let mut full = [0u8; 64];
        full[..32].copy_from_slice(kp.secret_bytes().as_slice());
        full[32..].copy_from_slice(&kp.pubkey().to_bytes());
        let b58 = bs58::encode(full).into_string();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("phantom.txt");
        std::fs::write(&path, &b58).unwrap();

        let loaded = load_keypair(&path).unwrap();
        assert_eq!(loaded.pubkey(), kp.pubkey());
    }

    #[test]
    fn load_keypair_base58_32_bytes_secret_only() {
        // Some wallets export only the 32-byte secret in base58.
        let kp = Keypair::new();
        let b58 = bs58::encode(kp.secret_bytes()).into_string();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("secret.txt");
        // Add trailing newline to test that trim() handles whitespace.
        std::fs::write(&path, format!("{b58}\n")).unwrap();

        let loaded = load_keypair(&path).unwrap();
        assert_eq!(loaded.pubkey(), kp.pubkey());
    }

    #[test]
    fn load_keypair_friendly_json_with_priv_only() {
        let kp = Keypair::new();
        let mut full = [0u8; 64];
        full[..32].copy_from_slice(kp.secret_bytes().as_slice());
        full[32..].copy_from_slice(&kp.pubkey().to_bytes());
        let priv_b58 = bs58::encode(full).into_string();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wallet.json");
        let json = format!(r#"{{"privKey":"{priv_b58}"}}"#);
        std::fs::write(&path, json).unwrap();

        let loaded = load_keypair(&path).unwrap();
        assert_eq!(loaded.pubkey(), kp.pubkey());
    }

    #[test]
    fn load_keypair_friendly_json_with_matching_pubkey() {
        let kp = Keypair::new();
        let mut full = [0u8; 64];
        full[..32].copy_from_slice(kp.secret_bytes().as_slice());
        full[32..].copy_from_slice(&kp.pubkey().to_bytes());
        let priv_b58 = bs58::encode(full).into_string();
        let pub_b58 = kp.pubkey().to_string();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wallet.json");
        let json = format!(r#"{{"pubKey":"{pub_b58}","privKey":"{priv_b58}"}}"#);
        std::fs::write(&path, json).unwrap();

        let loaded = load_keypair(&path).unwrap();
        assert_eq!(loaded.pubkey(), kp.pubkey());
    }

    #[test]
    fn load_keypair_rejects_pubkey_mismatch() {
        let kp = Keypair::new();
        let bogus_pub = Keypair::new().pubkey().to_string();  // different keypair's pubkey
        let mut full = [0u8; 64];
        full[..32].copy_from_slice(kp.secret_bytes().as_slice());
        full[32..].copy_from_slice(&kp.pubkey().to_bytes());
        let priv_b58 = bs58::encode(full).into_string();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wallet.json");
        let json = format!(r#"{{"pubKey":"{bogus_pub}","privKey":"{priv_b58}"}}"#);
        std::fs::write(&path, json).unwrap();

        let err = load_keypair(&path).unwrap_err();
        assert!(err.to_string().contains("pubKey mismatch"),
                "expected pubKey mismatch error, got: {err}");
    }

    #[test]
    fn load_keypair_rejects_garbage() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("garbage.txt");
        std::fs::write(&path, "not json, not base58 either... actually wait this IS base58")
            .unwrap();
        // Should error because resulting bytes aren't 32 or 64 long.
        let result = load_keypair(&path);
        assert!(result.is_err());
    }
}
