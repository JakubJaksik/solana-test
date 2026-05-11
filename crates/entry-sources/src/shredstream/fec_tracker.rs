use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use solana_ledger::shred::{Shred, ShredType};

use crate::counters::DropCounters;

use super::RawShredPacket;

/// A FEC set that has accumulated enough data shreds to be reconstructable.
#[derive(Debug)]
pub struct FecSetReady {
    pub slot: u64,
    pub fec_set_index: u32,
    /// Wall-clock instant of the *first* shred we received for this FEC set.
    pub first_shred_at: Instant,
    /// Wall-clock instant when this set became reconstructable.
    pub completed_at: Instant,
    /// Sorted by shred index, ready to feed Shredder::deshred.
    pub data_shreds: Vec<Shred>,
    pub coding_shreds: Vec<Shred>,
}

#[derive(Default)]
struct FecSetState {
    first_shred_at: Option<Instant>,
    /// keyed by shred index
    data: HashMap<u32, Shred>,
    coding: HashMap<u32, Shred>,
    /// Discovered when we see a shred with `last_in_slot` or `data_complete` flag set.
    /// expected_data_count = (that_shred.index - fec_set_index + 1)
    expected_data_count: Option<u32>,
}

pub struct FecTracker {
    sets: HashMap<(u64, u32), FecSetState>,
    counters: Arc<DropCounters>,
}

impl FecTracker {
    pub fn new(counters: Arc<DropCounters>) -> Self {
        Self {
            sets: HashMap::with_capacity(2048),
            counters,
        }
    }

    /// Ingest a raw shred packet. Returns `Some(FecSetReady)` if this packet completed a FEC set.
    pub fn ingest(&mut self, packet: RawShredPacket) -> Option<FecSetReady> {
        let received_at = packet.received_at;
        // Parse the shred. If this fails, increment counter and drop.
        let shred = match Shred::new_from_serialized_shred(packet.bytes) {
            Ok(s) => s,
            Err(_) => {
                self.counters.inc(&self.counters.ss_shred_parse_error);
                return None;
            }
        };

        let key = (shred.slot(), shred.fec_set_index());
        let state = self.sets.entry(key).or_insert_with(FecSetState::default);
        if state.first_shred_at.is_none() {
            state.first_shred_at = Some(received_at);
        }

        match shred.shred_type() {
            ShredType::Data => {
                // Check terminator flags to discover expected count for this FEC set.
                if shred.last_in_slot() || shred.data_complete() {
                    // expected count for THIS FEC set: (this_shred.index - fec_set_index + 1)
                    state.expected_data_count =
                        Some(shred.index() - shred.fec_set_index() + 1);
                }
                state.data.insert(shred.index(), shred);
            }
            ShredType::Code => {
                state.coding.insert(shred.index(), shred);
            }
        }

        let data_complete = state
            .expected_data_count
            .map(|n| state.data.len() as u32 >= n)
            .unwrap_or(false);

        if data_complete {
            let completed_at = Instant::now();   // capture FIRST, before any collect/sort work
            let st = self.sets.remove(&key).unwrap();
            let mut data_shreds: Vec<Shred> = st.data.into_values().collect();
            data_shreds.sort_by_key(|s| s.index());
            let coding_shreds: Vec<Shred> = st.coding.into_values().collect();

            return Some(FecSetReady {
                slot: key.0,
                fec_set_index: key.1,
                first_shred_at: st.first_shred_at.unwrap(),
                completed_at,
                data_shreds,
                coding_shreds,
            });
        }
        None
    }

    /// Drop FEC sets older than `max_age`. Returns count evicted (for stats).
    pub fn evict_older_than(&mut self, now: Instant, max_age: std::time::Duration) -> usize {
        let before = self.sets.len();
        let counters = self.counters.clone();
        self.sets.retain(|_, st| {
            let alive = st
                .first_shred_at
                .map(|t| now.duration_since(t) < max_age)
                .unwrap_or(true);
            if !alive {
                counters.inc(&counters.ss_fec_set_timeout);
            }
            alive
        });
        before - self.sets.len()
    }

    pub fn pending_set_count(&self) -> usize {
        self.sets.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_tracker_has_no_pending() {
        let counters = Arc::new(DropCounters::default());
        let t = FecTracker::new(counters);
        assert_eq!(t.pending_set_count(), 0);
    }

    #[test]
    fn invalid_shred_increments_counter() {
        let counters = Arc::new(DropCounters::default());
        let mut t = FecTracker::new(counters.clone());
        let bogus = RawShredPacket {
            bytes: vec![0u8; 32],
            received_at: Instant::now(),
        };
        assert!(t.ingest(bogus).is_none());
        assert_eq!(counters.snapshot().ss_shred_parse_error, 1);
    }

    #[test]
    fn evict_older_than_increments_timeout_counter() {
        let counters = Arc::new(DropCounters::default());
        let mut t = FecTracker::new(counters.clone());

        // Manually populate to test eviction path (ok inside same crate).
        let now = Instant::now();
        let stale_time = now - std::time::Duration::from_secs(10);
        let state = FecSetState {
            first_shred_at: Some(stale_time),
            ..Default::default()
        };
        t.sets.insert((100, 0), state);

        let evicted = t.evict_older_than(now, std::time::Duration::from_secs(2));
        assert_eq!(evicted, 1);
        assert_eq!(counters.snapshot().ss_fec_set_timeout, 1);
    }
}
