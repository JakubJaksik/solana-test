//! TriggerEvent — emitted by observer when schedule (slot, tick) matches.
//!
//! Consumed by dispatcher in Plan 4. For Plan 3 we just emit + count.

use std::time::Instant;

#[derive(Debug, Clone, Copy)]
pub struct TriggerEvent {
    pub slot: u64,
    pub tick: u8,
    /// Cumulative hashes from the start of the slot at trigger time
    /// (sub-tick precision for ex-post analysis).
    pub cumulative_hashes_in_slot: u64,
    /// Wall-clock instant when observer fired the trigger.
    pub observed_at: Instant,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trigger_event_constructs() {
        let t = TriggerEvent {
            slot: 100,
            tick: 5,
            cumulative_hashes_in_slot: 312_500,
            observed_at: Instant::now(),
        };
        assert_eq!(t.slot, 100);
        assert_eq!(t.tick, 5);
    }
}
