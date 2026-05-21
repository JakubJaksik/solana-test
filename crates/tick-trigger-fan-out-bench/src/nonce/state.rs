//! Parse Solana nonce account state.
//!
//! Nonce account data is bincode-serialized `nonce::state::Versions` (80 bytes).
//! We extract authority pubkey + current blockhash for our bench cache.

use solana_sdk::{
    hash::Hash,
    pubkey::Pubkey,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NonceAccountState {
    pub authority: Pubkey,
    pub blockhash: Hash,
}

#[derive(Debug, thiserror::Error)]
pub enum NonceParseError {
    #[error("account data too short: {0} bytes (expected 80)")]
    TooShort(usize),
    #[error("nonce state is Uninitialized")]
    Uninitialized,
    #[error("bincode deserialization failed: {0}")]
    Bincode(String),
}

pub fn parse_nonce_account_data(data: &[u8]) -> Result<NonceAccountState, NonceParseError> {
    if data.len() < 80 {
        return Err(NonceParseError::TooShort(data.len()));
    }
    let state_disc = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);
    if state_disc == 0 {
        return Err(NonceParseError::Uninitialized);
    }
    if state_disc != 1 {
        return Err(NonceParseError::Bincode(format!(
            "unexpected state discriminant: {}",
            state_disc
        )));
    }
    let authority_bytes: [u8; 32] = data[8..40].try_into().unwrap();
    let blockhash_bytes: [u8; 32] = data[40..72].try_into().unwrap();
    Ok(NonceAccountState {
        authority: Pubkey::from(authority_bytes),
        blockhash: Hash::from(blockhash_bytes),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_initialized_data(authority: [u8; 32], blockhash: [u8; 32]) -> Vec<u8> {
        let mut data = vec![0u8; 80];
        data[0..4].copy_from_slice(&0u32.to_le_bytes());
        data[4..8].copy_from_slice(&1u32.to_le_bytes());
        data[8..40].copy_from_slice(&authority);
        data[40..72].copy_from_slice(&blockhash);
        data
    }

    fn make_uninitialized_data() -> Vec<u8> {
        let mut data = vec![0u8; 80];
        data[0..4].copy_from_slice(&0u32.to_le_bytes());
        data[4..8].copy_from_slice(&0u32.to_le_bytes());
        data
    }

    #[test]
    fn parses_initialized_state() {
        let auth = [11u8; 32];
        let blockhash = [22u8; 32];
        let data = make_initialized_data(auth, blockhash);
        let state = parse_nonce_account_data(&data).unwrap();
        assert_eq!(state.authority.to_bytes(), auth);
        assert_eq!(state.blockhash.to_bytes(), blockhash);
    }

    #[test]
    fn rejects_uninitialized() {
        let data = make_uninitialized_data();
        assert!(matches!(parse_nonce_account_data(&data), Err(NonceParseError::Uninitialized)));
    }

    #[test]
    fn rejects_too_short() {
        let data = vec![0u8; 50];
        assert!(matches!(parse_nonce_account_data(&data), Err(NonceParseError::TooShort(50))));
    }

    #[test]
    fn rejects_unknown_state_discriminant() {
        let mut data = vec![0u8; 80];
        data[4..8].copy_from_slice(&99u32.to_le_bytes());
        assert!(matches!(parse_nonce_account_data(&data), Err(NonceParseError::Bincode(_))));
    }
}
