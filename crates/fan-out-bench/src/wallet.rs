//! Wallet keypair loader.
//!
//! Supports two formats:
//!  1. Solana CLI standard: JSON array of 64 bytes `[170, 234, ...]` (output
//!     of `solana-keygen new -o ...`).
//!  2. Phantom/wallet-export object: `{"pubKey": "...", "privKey": "..."}`
//!     where `privKey` is base58 (64 or 32 bytes) or hex.
//!     Field name aliases: `privateKey`, `secretKey`.

use anyhow::{bail, Context};
use serde::Deserialize;
use solana_sdk::signature::Keypair;
use std::path::Path;

#[derive(Deserialize)]
struct ObjectFormat {
    #[serde(alias = "privKey", alias = "privateKey", alias = "secretKey")]
    priv_key: String,
}

pub fn load_keypair_file(path: &Path) -> anyhow::Result<Keypair> {
    let bytes = std::fs::read(path)
        .with_context(|| format!("failed to read keypair file: {}", path.display()))?;
    let bytes_str = std::str::from_utf8(&bytes)
        .with_context(|| format!("keypair file is not UTF-8: {}", path.display()))?;

    // Try Solana CLI array format first
    if let Ok(arr) = serde_json::from_str::<Vec<u8>>(bytes_str) {
        return keypair_from_bytes(&arr, path);
    }

    // Try object format {privKey, pubKey} (Phantom/sollet export)
    if let Ok(obj) = serde_json::from_str::<ObjectFormat>(bytes_str) {
        // Try base58 first (standard Phantom export)
        if let Ok(decoded) = bs58::decode(obj.priv_key.trim()).into_vec() {
            if matches!(decoded.len(), 32 | 64) {
                return keypair_from_bytes(&decoded, path);
            }
        }
        // Try hex (some exports)
        let hex_str = obj.priv_key.trim().trim_start_matches("0x");
        if let Ok(decoded) = hex::decode(hex_str) {
            if matches!(decoded.len(), 32 | 64) {
                return keypair_from_bytes(&decoded, path);
            }
        }
        bail!(
            "keypair file {} has object format but `privKey` could not be decoded as base58 or hex (expected 32 or 64 bytes)",
            path.display()
        );
    }

    bail!(
        "keypair file {} format not recognized — expected JSON array of 64 bytes OR object with `privKey` field",
        path.display()
    )
}

fn keypair_from_bytes(bytes: &[u8], path: &Path) -> anyhow::Result<Keypair> {
    let secret: [u8; 32] = match bytes.len() {
        64 => bytes[..32]
            .try_into()
            .map_err(|_| anyhow::anyhow!("internal: 32-byte slice failed"))?,
        32 => bytes
            .try_into()
            .map_err(|_| anyhow::anyhow!("internal: 32-byte try_into failed"))?,
        n => bail!(
            "keypair file {} has {} bytes, expected 32 or 64",
            path.display(),
            n
        ),
    };
    Ok(Keypair::new_from_array(secret))
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::signature::Signer;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn loads_array_format_64_bytes() {
        let kp = Keypair::new();
        let bytes = kp.to_bytes();
        let json = serde_json::to_string(&bytes.to_vec()).unwrap();

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(json.as_bytes()).unwrap();

        let loaded = load_keypair_file(file.path()).unwrap();
        assert_eq!(loaded.pubkey(), kp.pubkey());
    }

    #[test]
    fn loads_array_format_32_bytes() {
        let kp = Keypair::new();
        let secret_only: Vec<u8> = kp.to_bytes()[..32].to_vec();
        let json = serde_json::to_string(&secret_only).unwrap();

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(json.as_bytes()).unwrap();

        let loaded = load_keypair_file(file.path()).unwrap();
        assert_eq!(loaded.pubkey(), kp.pubkey());
    }

    #[test]
    fn loads_object_format_base58_privkey_64_bytes() {
        let kp = Keypair::new();
        let bytes = kp.to_bytes();
        let priv_b58 = bs58::encode(bytes).into_string();
        let json = format!(r#"{{"pubKey":"x","privKey":"{}"}}"#, priv_b58);

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(json.as_bytes()).unwrap();

        let loaded = load_keypair_file(file.path()).unwrap();
        assert_eq!(loaded.pubkey(), kp.pubkey());
    }

    #[test]
    fn loads_object_format_base58_privkey_32_bytes() {
        let kp = Keypair::new();
        let secret_only: [u8; 32] = kp.to_bytes()[..32].try_into().unwrap();
        let priv_b58 = bs58::encode(secret_only).into_string();
        let json = format!(r#"{{"privKey":"{}"}}"#, priv_b58);

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(json.as_bytes()).unwrap();

        let loaded = load_keypair_file(file.path()).unwrap();
        assert_eq!(loaded.pubkey(), kp.pubkey());
    }

    #[test]
    fn loads_object_format_hex_privkey() {
        let kp = Keypair::new();
        let bytes = kp.to_bytes();
        let priv_hex = hex::encode(bytes);
        let json = format!(r#"{{"privateKey":"{}"}}"#, priv_hex);

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(json.as_bytes()).unwrap();

        let loaded = load_keypair_file(file.path()).unwrap();
        assert_eq!(loaded.pubkey(), kp.pubkey());
    }

    #[test]
    fn loads_object_format_secret_key_alias() {
        let kp = Keypair::new();
        let bytes = kp.to_bytes();
        let priv_b58 = bs58::encode(bytes).into_string();
        let json = format!(r#"{{"secretKey":"{}"}}"#, priv_b58);

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(json.as_bytes()).unwrap();

        let loaded = load_keypair_file(file.path()).unwrap();
        assert_eq!(loaded.pubkey(), kp.pubkey());
    }

    #[test]
    fn rejects_wrong_byte_length() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"[1, 2, 3]").unwrap();
        assert!(load_keypair_file(file.path()).is_err());
    }

    #[test]
    fn rejects_object_with_invalid_privkey() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(br#"{"privKey":"not_valid_base58_or_hex_!!!"}"#).unwrap();
        assert!(load_keypair_file(file.path()).is_err());
    }

    #[test]
    fn rejects_non_json() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"not json").unwrap();
        assert!(load_keypair_file(file.path()).is_err());
    }

    #[test]
    fn rejects_missing_file() {
        let path = std::path::Path::new("/nonexistent/path/keypair.json");
        assert!(load_keypair_file(path).is_err());
    }
}
