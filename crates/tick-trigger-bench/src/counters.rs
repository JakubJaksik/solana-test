use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default, Debug)]
pub struct BenchCounters {
    pub pool_empty: AtomicU64,           // trigger fired but no pre-signed tx
    pub send_queue_full: AtomicU64,      // hot path → sender channel overflow
    pub match_queue_full: AtomicU64,     // hot path → matcher channel overflow
    pub send_http_error: AtomicU64,      // Helius Sender returned non-2xx
    pub send_network_error: AtomicU64,   // connection / timeout
    pub preparer_blockhash_fail: AtomicU64,
    pub preparer_signing_fail: AtomicU64,
    pub fork_tick_overflow: AtomicU64,   // tick_idx > 64 within slot (fork artifact)
    pub rpc_fallback_error: AtomicU64,
    pub tick_event_queue_full: AtomicU64,  // tick sidecar channel overflow
    pub send_event_queue_full: AtomicU64,  // send-event channel overflow (after HTTP POST)
    pub final_queue_full: AtomicU64,       // final record channel overflow in writer
    pub blockhash_expired: AtomicU64,      // tx skipped because blockhash too old
}

impl BenchCounters {
    #[inline]
    pub fn inc(&self, c: &AtomicU64) {
        c.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> CounterSnapshot {
        CounterSnapshot {
            pool_empty: self.pool_empty.load(Ordering::Relaxed),
            send_queue_full: self.send_queue_full.load(Ordering::Relaxed),
            match_queue_full: self.match_queue_full.load(Ordering::Relaxed),
            send_http_error: self.send_http_error.load(Ordering::Relaxed),
            send_network_error: self.send_network_error.load(Ordering::Relaxed),
            preparer_blockhash_fail: self.preparer_blockhash_fail.load(Ordering::Relaxed),
            preparer_signing_fail: self.preparer_signing_fail.load(Ordering::Relaxed),
            fork_tick_overflow: self.fork_tick_overflow.load(Ordering::Relaxed),
            rpc_fallback_error: self.rpc_fallback_error.load(Ordering::Relaxed),
            tick_event_queue_full: self.tick_event_queue_full.load(Ordering::Relaxed),
            send_event_queue_full: self.send_event_queue_full.load(Ordering::Relaxed),
            final_queue_full: self.final_queue_full.load(Ordering::Relaxed),
            blockhash_expired: self.blockhash_expired.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct CounterSnapshot {
    pub pool_empty: u64,
    pub send_queue_full: u64,
    pub match_queue_full: u64,
    pub send_http_error: u64,
    pub send_network_error: u64,
    pub preparer_blockhash_fail: u64,
    pub preparer_signing_fail: u64,
    pub fork_tick_overflow: u64,
    pub rpc_fallback_error: u64,
    pub tick_event_queue_full: u64,
    pub send_event_queue_full: u64,
    pub final_queue_full: u64,
    pub blockhash_expired: u64,
}

impl CounterSnapshot {
    /// Returns list of (name, delta) for fields where current > previous.
    pub fn deltas_vs(&self, prev: &CounterSnapshot) -> Vec<(&'static str, u64)> {
        let mut out = Vec::new();
        macro_rules! d {
            ($name:literal, $field:ident) => {
                let delta = self.$field.saturating_sub(prev.$field);
                if delta > 0 { out.push(($name, delta)); }
            };
        }
        d!("pool_empty", pool_empty);
        d!("send_queue_full", send_queue_full);
        d!("match_queue_full", match_queue_full);
        d!("send_http_error", send_http_error);
        d!("send_network_error", send_network_error);
        d!("preparer_blockhash_fail", preparer_blockhash_fail);
        d!("preparer_signing_fail", preparer_signing_fail);
        d!("fork_tick_overflow", fork_tick_overflow);
        d!("rpc_fallback_error", rpc_fallback_error);
        d!("tick_event_queue_full", tick_event_queue_full);
        d!("send_event_queue_full", send_event_queue_full);
        d!("final_queue_full", final_queue_full);
        d!("blockhash_expired", blockhash_expired);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inc_and_snapshot() {
        let c = BenchCounters::default();
        c.inc(&c.pool_empty);
        c.inc(&c.pool_empty);
        c.inc(&c.send_http_error);
        let s = c.snapshot();
        assert_eq!(s.pool_empty, 2);
        assert_eq!(s.send_http_error, 1);
        assert_eq!(s.send_queue_full, 0);
    }

    #[test]
    fn deltas_only_nonzero() {
        let c = BenchCounters::default();
        let prev = c.snapshot();
        c.inc(&c.pool_empty);
        c.inc(&c.fork_tick_overflow);
        c.inc(&c.fork_tick_overflow);
        let cur = c.snapshot();
        let d: Vec<_> = cur.deltas_vs(&prev);
        assert_eq!(d.len(), 2);
        assert!(d.iter().any(|(n, v)| *n == "pool_empty" && *v == 1));
        assert!(d.iter().any(|(n, v)| *n == "fork_tick_overflow" && *v == 2));
    }
}
