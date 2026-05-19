//! TriggerId — uniquely identifies a (slot, tick, nonce_account_id) trigger.

use crate::nonce::manager::NonceId;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TriggerId(pub [u8; 16]);

impl TriggerId {
    pub fn new(slot: u64, tick: u8, nonce_id: NonceId) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(slot.to_le_bytes());
        hasher.update([tick]);
        hasher.update(nonce_id.to_le_bytes());
        let full: [u8; 32] = hasher.finalize().into();
        let mut out = [0u8; 16];
        out.copy_from_slice(&full[..16]);
        TriggerId(out)
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trigger_id_deterministic() {
        let a = TriggerId::new(100, 5, 0);
        let b = TriggerId::new(100, 5, 0);
        assert_eq!(a, b);
    }

    #[test]
    fn trigger_id_unique_per_args() {
        let a = TriggerId::new(100, 5, 0);
        let b = TriggerId::new(100, 5, 1);
        let c = TriggerId::new(100, 6, 0);
        let d = TriggerId::new(101, 5, 0);
        assert_ne!(a, b);
        assert_ne!(a, c);
        assert_ne!(a, d);
    }
}
