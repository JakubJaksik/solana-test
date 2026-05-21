//! Wallet keypair loading.
//!
//! Supported formats (auto-detected):
//!   1. Solana CLI / `solana-keygen new`: bare JSON array of 64 bytes, e.g.
//!      `[12, 34, ..., 200]`.
//!   2. Object form with base58 fields, e.g.
//!      `{"pubKey": "<base58 32B>", "privKey": "<base58 64B>"}`.
//!      `privKey` may be 64 bytes (full secret key + pubkey) or 32 bytes
//!      (seed only); in both cases we derive a valid `Keypair`. If `pubKey`
//!      is present and decodes to 32 bytes, we cross-check it against the
//!      derived public key and bail on mismatch.

use serde::Deserialize;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use std::path::Path;

#[derive(Deserialize)]
struct ObjectFormat {
    #[serde(rename = "privKey", alias = "private_key", alias = "secretKey")]
    priv_key: String,
    #[serde(rename = "pubKey", alias = "public_key", default)]
    pub_key: Option<String>,
}

pub fn load_keypair(path: &Path) -> anyhow::Result<Keypair> {
    let expanded = expand_tilde(path);
    let text = std::fs::read_to_string(&expanded)?;
    let kp = parse_keypair_text(&text)?;
    tracing::info!(pubkey = %kp.pubkey(), path = %expanded.display(), "wallet loaded");
    Ok(kp)
}

fn parse_keypair_text(text: &str) -> anyhow::Result<Keypair> {
    let trimmed = text.trim();
    // Format 1: bare JSON byte array (Solana CLI default).
    if let Ok(bytes) = serde_json::from_str::<Vec<u8>>(trimmed) {
        return keypair_from_bytes(&bytes, None);
    }
    // Format 2: object with privKey (and optional pubKey) — base58 strings.
    if let Ok(obj) = serde_json::from_str::<ObjectFormat>(trimmed) {
        let priv_bytes = bs58::decode(&obj.priv_key)
            .into_vec()
            .map_err(|e| anyhow::anyhow!("privKey base58 decode failed: {}", e))?;
        let expected_pubkey = match obj.pub_key {
            Some(s) if !s.is_empty() => {
                let pk_bytes = bs58::decode(&s)
                    .into_vec()
                    .map_err(|e| anyhow::anyhow!("pubKey base58 decode failed: {}", e))?;
                if pk_bytes.len() != 32 {
                    anyhow::bail!(
                        "pubKey decoded to {} bytes, expected 32",
                        pk_bytes.len()
                    );
                }
                Some(pk_bytes)
            }
            _ => None,
        };
        return keypair_from_bytes(&priv_bytes, expected_pubkey.as_deref());
    }
    anyhow::bail!(
        "wallet file is neither a JSON byte array nor an object with `privKey` — \
         expected formats: `[12, 34, ...]` (Solana CLI) or `{{\"pubKey\":\"...\",\"privKey\":\"...\"}}`"
    )
}

/// Build a `Keypair` from a secret key byte slice.
///
/// Accepts:
/// - 64 bytes: full Ed25519 keypair (seed + pubkey) — passed through directly.
/// - 32 bytes: seed only — we derive the pubkey to form the full 64-byte key.
///
/// When `expected_pub` is supplied, the derived pubkey is verified to match.
fn keypair_from_bytes(bytes: &[u8], expected_pub: Option<&[u8]>) -> anyhow::Result<Keypair> {
    let kp = match bytes.len() {
        64 => Keypair::try_from(bytes)
            .map_err(|e| anyhow::anyhow!("invalid 64-byte keypair: {}", e))?,
        32 => {
            // 32-byte seed: derive pubkey via ed25519-dalek and assemble the
            // 64-byte form solana_sdk::Keypair expects.
            let seed: [u8; 32] = bytes
                .try_into()
                .map_err(|_| anyhow::anyhow!("seed slice length mismatch"))?;
            let signing = ed25519_dalek::SigningKey::from_bytes(&seed);
            let verifying = signing.verifying_key();
            let mut full = [0u8; 64];
            full[..32].copy_from_slice(&signing.to_bytes());
            full[32..].copy_from_slice(verifying.as_bytes());
            Keypair::try_from(full.as_slice())
                .map_err(|e| anyhow::anyhow!("failed to assemble keypair from seed: {}", e))?
        }
        n => anyhow::bail!(
            "secret key has {} bytes; expected 32 (seed) or 64 (full keypair)",
            n
        ),
    };
    if let Some(expected) = expected_pub {
        let derived = kp.pubkey().to_bytes();
        if expected != derived {
            anyhow::bail!(
                "pubKey in wallet file does not match the key derived from privKey \
                 — wallet file is inconsistent"
            );
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a known 64-byte keypair, encode it both ways, and verify each
    /// loader path reconstructs the same pubkey.
    fn known_keypair() -> Keypair {
        let seed = [7u8; 32];
        let signing = ed25519_dalek::SigningKey::from_bytes(&seed);
        let verifying = signing.verifying_key();
        let mut full = [0u8; 64];
        full[..32].copy_from_slice(&signing.to_bytes());
        full[32..].copy_from_slice(verifying.as_bytes());
        Keypair::try_from(full.as_slice()).unwrap()
    }

    #[test]
    fn parses_solana_cli_array_format() {
        let kp = known_keypair();
        let bytes: Vec<u8> = kp.to_bytes().to_vec();
        let text = serde_json::to_string(&bytes).unwrap();
        let loaded = parse_keypair_text(&text).unwrap();
        assert_eq!(loaded.pubkey(), kp.pubkey());
    }

    #[test]
    fn parses_object_format_with_64byte_privkey() {
        let kp = known_keypair();
        let priv_b58 = bs58::encode(kp.to_bytes()).into_string();
        let pub_b58 = bs58::encode(kp.pubkey().to_bytes()).into_string();
        let text = format!(
            r#"{{"pubKey":"{}","privKey":"{}"}}"#,
            pub_b58, priv_b58
        );
        let loaded = parse_keypair_text(&text).unwrap();
        assert_eq!(loaded.pubkey(), kp.pubkey());
    }

    #[test]
    fn parses_object_format_with_32byte_seed() {
        let kp = known_keypair();
        let seed_b58 = bs58::encode(&kp.to_bytes()[..32]).into_string();
        // No pubKey field — loader derives.
        let text = format!(r#"{{"privKey":"{}"}}"#, seed_b58);
        let loaded = parse_keypair_text(&text).unwrap();
        assert_eq!(loaded.pubkey(), kp.pubkey());
    }

    #[test]
    fn object_format_rejects_mismatched_pubkey() {
        let kp = known_keypair();
        let priv_b58 = bs58::encode(kp.to_bytes()).into_string();
        // Wrong pubkey on purpose.
        let fake_pub = bs58::encode([1u8; 32]).into_string();
        let text = format!(
            r#"{{"pubKey":"{}","privKey":"{}"}}"#,
            fake_pub, priv_b58
        );
        assert!(parse_keypair_text(&text).is_err());
    }

    #[test]
    fn unrecognized_format_errors_clearly() {
        let text = r#""just-a-string""#;
        let err = parse_keypair_text(text).unwrap_err().to_string();
        assert!(err.contains("expected formats"));
    }

    #[test]
    fn accepts_secret_key_alias_field_name() {
        let kp = known_keypair();
        let priv_b58 = bs58::encode(kp.to_bytes()).into_string();
        let text = format!(r#"{{"secretKey":"{}"}}"#, priv_b58);
        let loaded = parse_keypair_text(&text).unwrap();
        assert_eq!(loaded.pubkey(), kp.pubkey());
    }
}
