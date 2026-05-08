use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default, Debug)]
pub struct DropCounters {
    pub ys_channel_full: AtomicU64,
    pub ys_decode_error: AtomicU64,
    pub ys_reconnects: AtomicU64,
    pub ss_udp_channel_full: AtomicU64,
    pub ss_obs_channel_full: AtomicU64,
    pub ss_shred_parse_error: AtomicU64,
    pub ss_deshred_error: AtomicU64,
    pub ss_entry_decode_error: AtomicU64,
    pub ss_fec_set_timeout: AtomicU64,
}

impl DropCounters {
    #[inline]
    pub fn inc(&self, counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> CounterSnapshot {
        CounterSnapshot {
            ys_channel_full: self.ys_channel_full.load(Ordering::Relaxed),
            ys_decode_error: self.ys_decode_error.load(Ordering::Relaxed),
            ys_reconnects: self.ys_reconnects.load(Ordering::Relaxed),
            ss_udp_channel_full: self.ss_udp_channel_full.load(Ordering::Relaxed),
            ss_obs_channel_full: self.ss_obs_channel_full.load(Ordering::Relaxed),
            ss_shred_parse_error: self.ss_shred_parse_error.load(Ordering::Relaxed),
            ss_deshred_error: self.ss_deshred_error.load(Ordering::Relaxed),
            ss_entry_decode_error: self.ss_entry_decode_error.load(Ordering::Relaxed),
            ss_fec_set_timeout: self.ss_fec_set_timeout.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct CounterSnapshot {
    pub ys_channel_full: u64,
    pub ys_decode_error: u64,
    pub ys_reconnects: u64,
    pub ss_udp_channel_full: u64,
    pub ss_obs_channel_full: u64,
    pub ss_shred_parse_error: u64,
    pub ss_deshred_error: u64,
    pub ss_entry_decode_error: u64,
    pub ss_fec_set_timeout: u64,
}

impl CounterSnapshot {
    /// Returns list of (name, delta) for each field where current > previous.
    pub fn deltas_vs(&self, prev: &CounterSnapshot) -> Vec<(&'static str, u64)> {
        let mut out = Vec::new();
        macro_rules! d {
            ($name:literal, $field:ident) => {
                let delta = self.$field.saturating_sub(prev.$field);
                if delta > 0 {
                    out.push(($name, delta));
                }
            };
        }
        d!("ys_channel_full", ys_channel_full);
        d!("ys_decode_error", ys_decode_error);
        d!("ys_reconnects", ys_reconnects);
        d!("ss_udp_channel_full", ss_udp_channel_full);
        d!("ss_obs_channel_full", ss_obs_channel_full);
        d!("ss_shred_parse_error", ss_shred_parse_error);
        d!("ss_deshred_error", ss_deshred_error);
        d!("ss_entry_decode_error", ss_entry_decode_error);
        d!("ss_fec_set_timeout", ss_fec_set_timeout);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inc_increments_relaxed() {
        let c = DropCounters::default();
        c.inc(&c.ys_channel_full);
        c.inc(&c.ys_channel_full);
        c.inc(&c.ss_obs_channel_full);
        let snap = c.snapshot();
        assert_eq!(snap.ys_channel_full, 2);
        assert_eq!(snap.ss_obs_channel_full, 1);
        assert_eq!(snap.ys_decode_error, 0);
    }

    #[test]
    fn deltas_returns_only_increased_fields() {
        let c = DropCounters::default();
        let prev = c.snapshot();
        c.inc(&c.ys_channel_full);
        c.inc(&c.ss_fec_set_timeout);
        c.inc(&c.ss_fec_set_timeout);
        let cur = c.snapshot();
        let deltas: Vec<_> = cur.deltas_vs(&prev);
        assert_eq!(deltas.len(), 2);
        assert!(deltas.iter().any(|(n, d)| *n == "ys_channel_full" && *d == 1));
        assert!(deltas.iter().any(|(n, d)| *n == "ss_fec_set_timeout" && *d == 2));
    }

    #[test]
    fn deltas_empty_when_unchanged() {
        let c = DropCounters::default();
        c.inc(&c.ys_channel_full);
        let snap = c.snapshot();
        assert!(snap.deltas_vs(&snap).is_empty());
    }
}
