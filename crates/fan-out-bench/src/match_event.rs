//! MatchEvent — emitted by observer when a pending signature is observed.

use entry_sources::SourceKind;
use solana_sdk::signature::Signature;
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct MatchEvent {
    pub signature: Signature,
    pub observed_at: Instant,
    pub observed_slot: u64,
    pub observed_entry_index: u32,
    pub observed_tick_in_slot: Option<u8>,
    pub observed_cumulative_hashes_in_slot: Option<u64>,
    pub observed_source: SourceKind,
}
