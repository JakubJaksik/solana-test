//! Atomic counters for bench telemetry.

use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
pub struct BenchCounters {
    pub pool_empty: AtomicU64,
    pub pool_overwrite: AtomicU64,
    pub send_queue_full: AtomicU64,
    pub match_queue_full: AtomicU64,
    pub finality_queue_full: AtomicU64,
    pub fallback_queue_full: AtomicU64,
    pub send_event_queue_full: AtomicU64,
    pub final_queue_full: AtomicU64,
    pub tick_event_queue_full: AtomicU64,
    pub send_http_error: AtomicU64,
    pub send_network_error: AtomicU64,
    pub send_throttled_429: AtomicU64,
    pub blockhash_expired: AtomicU64,
    pub preparer_blockhash_fail: AtomicU64,
    pub preparer_signing_fail: AtomicU64,
    pub fork_tick_overflow: AtomicU64,
    pub nonce_stalls: AtomicU64,
    pub nonce_advance_observed: AtomicU64,
    pub schedule_contains_calls: AtomicU64,
    pub schedule_contains_true: AtomicU64,
    pub rpc_fallback_error: AtomicU64,
    pub rpc_fallback_recovered_landed: AtomicU64,
    pub rpc_fallback_confirmed_missing: AtomicU64,
    pub finality_confirmed: AtomicU64,
    pub finality_reorged_out: AtomicU64,
    pub finality_uncertain: AtomicU64,
}

#[derive(Debug, Default, Serialize)]
pub struct CountersSnapshot {
    pub pool_empty: u64,
    pub pool_overwrite: u64,
    pub send_queue_full: u64,
    pub match_queue_full: u64,
    pub finality_queue_full: u64,
    pub fallback_queue_full: u64,
    pub send_event_queue_full: u64,
    pub final_queue_full: u64,
    pub tick_event_queue_full: u64,
    pub send_http_error: u64,
    pub send_network_error: u64,
    pub send_throttled_429: u64,
    pub blockhash_expired: u64,
    pub preparer_blockhash_fail: u64,
    pub preparer_signing_fail: u64,
    pub fork_tick_overflow: u64,
    pub nonce_stalls: u64,
    pub nonce_advance_observed: u64,
    pub schedule_contains_calls: u64,
    pub schedule_contains_true: u64,
    pub rpc_fallback_error: u64,
    pub rpc_fallback_recovered_landed: u64,
    pub rpc_fallback_confirmed_missing: u64,
    pub finality_confirmed: u64,
    pub finality_reorged_out: u64,
    pub finality_uncertain: u64,
}

impl BenchCounters {
    pub fn snapshot(&self) -> CountersSnapshot {
        let l = |c: &AtomicU64| c.load(Ordering::Relaxed);
        CountersSnapshot {
            pool_empty: l(&self.pool_empty),
            pool_overwrite: l(&self.pool_overwrite),
            send_queue_full: l(&self.send_queue_full),
            match_queue_full: l(&self.match_queue_full),
            finality_queue_full: l(&self.finality_queue_full),
            fallback_queue_full: l(&self.fallback_queue_full),
            send_event_queue_full: l(&self.send_event_queue_full),
            final_queue_full: l(&self.final_queue_full),
            tick_event_queue_full: l(&self.tick_event_queue_full),
            send_http_error: l(&self.send_http_error),
            send_network_error: l(&self.send_network_error),
            send_throttled_429: l(&self.send_throttled_429),
            blockhash_expired: l(&self.blockhash_expired),
            preparer_blockhash_fail: l(&self.preparer_blockhash_fail),
            preparer_signing_fail: l(&self.preparer_signing_fail),
            fork_tick_overflow: l(&self.fork_tick_overflow),
            nonce_stalls: l(&self.nonce_stalls),
            nonce_advance_observed: l(&self.nonce_advance_observed),
            schedule_contains_calls: l(&self.schedule_contains_calls),
            schedule_contains_true: l(&self.schedule_contains_true),
            rpc_fallback_error: l(&self.rpc_fallback_error),
            rpc_fallback_recovered_landed: l(&self.rpc_fallback_recovered_landed),
            rpc_fallback_confirmed_missing: l(&self.rpc_fallback_confirmed_missing),
            finality_confirmed: l(&self.finality_confirmed),
            finality_reorged_out: l(&self.finality_reorged_out),
            finality_uncertain: l(&self.finality_uncertain),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_serializes_to_json() {
        let counters = BenchCounters::default();
        counters.pool_empty.fetch_add(5, Ordering::Relaxed);
        let snap = counters.snapshot();
        let json = serde_json::to_string(&snap).unwrap();
        assert!(json.contains("\"pool_empty\":5"));
    }

    #[test]
    fn snapshot_independent_of_counter_state() {
        let counters = BenchCounters::default();
        let s1 = counters.snapshot();
        counters.pool_empty.fetch_add(10, Ordering::Relaxed);
        let s2 = counters.snapshot();
        assert_eq!(s1.pool_empty, 0);
        assert_eq!(s2.pool_empty, 10);
    }
}
