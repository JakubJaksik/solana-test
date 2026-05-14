//! Wallet keypair loader.
//!
//! Reads JSON-encoded Solana keypair file (`[u8; 64]` byte array).
//! Compatible with `solana-keygen` output and `~/.config/solana/id.json`.

use anyhow::Context;
use solana_sdk::signature::Keypair;
use std::path::Path;

pub fn load_keypair_file(path: &Path) -> anyhow::Result<Keypair> {
    let bytes = std::fs::read(path)
        .with_context(|| format!("failed to read keypair file: {}", path.display()))?;
    let bytes_str = std::str::from_utf8(&bytes)
        .with_context(|| format!("keypair file is not UTF-8: {}", path.display()))?;
    let secret_bytes: Vec<u8> = serde_json::from_str(bytes_str)
        .with_context(|| format!("keypair file is not valid JSON byte array: {}", path.display()))?;
    if secret_bytes.len() != 64 {
        anyhow::bail!(
            "keypair file has {} bytes, expected 64: {}",
            secret_bytes.len(),
            path.display()
        );
    }
    // Solana keypair JSON contains 64 bytes: 32 secret + 32 pubkey.
    // `Keypair::new_from_array` takes the 32-byte secret only.
    let secret: [u8; 32] = secret_bytes[..32]
        .try_into()
        .map_err(|_| anyhow::anyhow!("internal: 32-byte slice failed"))?;
    Ok(Keypair::new_from_array(secret))
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::signature::Signer;
    use tempfile::NamedTempFile;
    use std::io::Write;

    #[test]
    fn loads_valid_keypair() {
        let kp = Keypair::new();
        let bytes = kp.to_bytes();
        let json = serde_json::to_string(&bytes.to_vec()).unwrap();

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(json.as_bytes()).unwrap();

        let loaded = load_keypair_file(file.path()).unwrap();
        assert_eq!(loaded.pubkey(), kp.pubkey());
    }

    #[test]
    fn rejects_wrong_length() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"[1, 2, 3]").unwrap();
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
