//! AttemptState — single-owner state machine per (trigger_id, sender_id).
//!
//! See spec §7.2 — eliminuje race conditions w matcher.

use crate::outcome::{ObservedSource, TentativeOutcome};
use solana_sdk::signature::Signature;

#[derive(Debug, Clone)]
pub enum AttemptState {
    SentPending {
        send_at_ns: u64,
        sig: Signature,
    },
    SentAcked {
        send_at_ns: u64,
        send_ack_at_ns: u64,
        sig: Signature,
        provider_request_id: Option<String>,
    },
    SendFailed {
        send_at_ns: u64,
        send_ack_at_ns: Option<u64>,
        error: String,
        sig: Signature,
    },
    ObservedTentative {
        send_at_ns: u64,
        send_ack_at_ns: Option<u64>,
        sig: Signature,
        observed_at_ns: u64,
        observed_source: ObservedSource,
        outcome: TentativeOutcome, // LandedTentative or DedupedTentative
        provider_request_id: Option<String>,
    },
    UnknownPending {
        send_at_ns: u64,
        send_ack_at_ns: Option<u64>,
        sig: Signature,
    },
    TrulyMissing {
        send_at_ns: u64,
        send_ack_at_ns: Option<u64>,
        sig: Signature,
    },
}

impl AttemptState {
    pub fn sig(&self) -> &Signature {
        match self {
            Self::SentPending { sig, .. }
            | Self::SentAcked { sig, .. }
            | Self::SendFailed { sig, .. }
            | Self::ObservedTentative { sig, .. }
            | Self::UnknownPending { sig, .. }
            | Self::TrulyMissing { sig, .. } => sig,
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::SendFailed { .. }
                | Self::ObservedTentative { .. }
                | Self::TrulyMissing { .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::signature::Signature;

    #[test]
    fn is_terminal_correct() {
        let sig = Signature::default();
        assert!(!AttemptState::SentPending { send_at_ns: 0, sig }.is_terminal());
        assert!(!AttemptState::SentAcked { send_at_ns: 0, send_ack_at_ns: 1, sig, provider_request_id: None }.is_terminal());
        assert!(AttemptState::SendFailed { send_at_ns: 0, send_ack_at_ns: None, error: "x".into(), sig }.is_terminal());
        assert!(AttemptState::TrulyMissing { send_at_ns: 0, send_ack_at_ns: None, sig }.is_terminal());
    }
}
