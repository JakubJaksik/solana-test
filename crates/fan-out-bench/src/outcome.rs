//! Outcome enums for tx attempts.
//!
//! See spec §3.4 — dwustopniowa rezolucja:
//! - `tentative_outcome` emitowane real-time przez matcher
//! - `final_status` emitowane post-finality przez finality_tracker

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TentativeOutcome {
    LandedTentative,
    DedupedTentative,
    UnknownPending,
    TrulyMissing,
    SendError,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FinalStatus {
    Pending,
    Confirmed,
    ReorgedOut,
    UncertainNoStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ObservedSource {
    Ss,
    Ys,
    Both,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CommitmentAtResolution {
    Processed,
    Confirmed,
    Finalized,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RateLimitState {
    Ok,
    Throttled429,
    CircuitOpen,
    Timeout,
}

impl TentativeOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            TentativeOutcome::LandedTentative => "LANDED_TENTATIVE",
            TentativeOutcome::DedupedTentative => "DEDUPED_TENTATIVE",
            TentativeOutcome::UnknownPending => "UNKNOWN_PENDING",
            TentativeOutcome::TrulyMissing => "TRULY_MISSING",
            TentativeOutcome::SendError => "SEND_ERROR",
        }
    }
}

impl FinalStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            FinalStatus::Pending => "PENDING",
            FinalStatus::Confirmed => "CONFIRMED",
            FinalStatus::ReorgedOut => "REORGED_OUT",
            FinalStatus::UncertainNoStatus => "UNCERTAIN_NO_STATUS",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde_roundtrip_tentative_outcome() {
        let json = serde_json::to_string(&TentativeOutcome::LandedTentative).unwrap();
        assert_eq!(json, "\"LANDED_TENTATIVE\"");
        let parsed: TentativeOutcome = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, TentativeOutcome::LandedTentative);
    }

    #[test]
    fn as_str_matches_serde() {
        let json = serde_json::to_string(&TentativeOutcome::DedupedTentative).unwrap();
        assert_eq!(json.trim_matches('"'), TentativeOutcome::DedupedTentative.as_str());
    }
}
