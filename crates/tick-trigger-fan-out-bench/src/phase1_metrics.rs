//! Phase 1 metric aggregation + reporting.
//!
//! Combines snapshots from the merger and the ordering tracker into a single
//! report — both for periodic log lines during a run and for the final JSON
//! dump consumed by analysis scripts.
//!
//! The report intentionally does NOT include any send-path or nonce data;
//! those belong to later phases.

use crate::merger::MergerCountersSnapshot;
use crate::ordering::OrderingCountersSnapshot;
use serde::Serialize;
use std::time::Duration;

#[derive(Debug, Clone, Serialize)]
pub struct Phase1Report {
    pub elapsed_secs: f64,
    pub merger: MergerCountersSnapshot,
    pub ordering: OrderingCountersSnapshot,
    pub derived: Derived,
}

#[derive(Debug, Clone, Serialize)]
pub struct Derived {
    /// Unique entries observed = how many distinct (slot, entry_hash) the
    /// merger emitted. Equals `ss_first + ys_first`.
    pub unique_entries: u64,
    /// Subset of `unique_entries` also confirmed by the other source.
    pub both_sources_confirmed: u64,
    /// `both_sources_confirmed / unique_entries`. 0..=1.
    pub both_confirm_rate: f64,
    /// Fraction of unique entries first seen by SS. 0..=1.
    pub ss_first_rate: f64,
    /// Average inter-source latency in microseconds (defined only when at
    /// least one confirmation has been recorded).
    pub confirm_latency_avg_us: Option<f64>,
    /// Min/max inter-source latency in microseconds; None when no confirmations.
    pub confirm_latency_min_us: Option<f64>,
    pub confirm_latency_max_us: Option<f64>,
    /// Average entries per sealed slot.
    pub avg_entries_per_slot: f64,
    /// Fraction of sealed slots that ended on a tick. Expected ~1.0 healthy.
    pub tick_ending_rate: f64,
    /// Fraction of sealed slots that were fully ordered (no out-of-order).
    pub fully_ordered_rate: f64,
    /// Average out-of-order entries per sealed slot.
    pub avg_out_of_order_per_slot: f64,
}

pub fn build_report(
    elapsed: Duration,
    merger: MergerCountersSnapshot,
    ordering: OrderingCountersSnapshot,
) -> Phase1Report {
    let unique_entries = merger.ss_first + merger.ys_first;
    let both_confirm_rate = if unique_entries == 0 {
        0.0
    } else {
        merger.confirmed_by_both as f64 / unique_entries as f64
    };
    let ss_first_rate = if unique_entries == 0 {
        0.0
    } else {
        merger.ss_first as f64 / unique_entries as f64
    };
    let confirm_latency_avg_us = merger.confirm_latency_avg_us();
    let confirm_latency_min_us = if merger.confirmed_by_both > 0 {
        Some(merger.confirm_latency_min_ns as f64 / 1_000.0)
    } else {
        None
    };
    let confirm_latency_max_us = if merger.confirmed_by_both > 0 {
        Some(merger.confirm_latency_max_ns as f64 / 1_000.0)
    } else {
        None
    };
    let sealed = ordering.slots_sealed as f64;
    let avg_entries_per_slot = if sealed > 0.0 {
        unique_entries as f64 / sealed
    } else {
        0.0
    };
    let tick_ending_rate = if sealed > 0.0 {
        ordering.slots_ending_on_tick as f64 / sealed
    } else {
        0.0
    };
    let fully_ordered_rate = if sealed > 0.0 {
        ordering.slots_fully_ordered as f64 / sealed
    } else {
        0.0
    };
    let avg_out_of_order_per_slot = if sealed > 0.0 {
        ordering.total_out_of_order as f64 / sealed
    } else {
        0.0
    };
    Phase1Report {
        elapsed_secs: elapsed.as_secs_f64(),
        merger: merger.clone(),
        ordering: ordering.clone(),
        derived: Derived {
            unique_entries,
            both_sources_confirmed: merger.confirmed_by_both,
            both_confirm_rate,
            ss_first_rate,
            confirm_latency_avg_us,
            confirm_latency_min_us,
            confirm_latency_max_us,
            avg_entries_per_slot,
            tick_ending_rate,
            fully_ordered_rate,
            avg_out_of_order_per_slot,
        },
    }
}

/// Format a one-line summary for periodic log output.
pub fn one_line(report: &Phase1Report) -> String {
    let lat = report
        .derived
        .confirm_latency_avg_us
        .map(|us| format!("{:.0}us", us))
        .unwrap_or_else(|| "-".into());
    format!(
        "t={:.1}s | ss_recv={} ys_recv={} unique={} both={} ({:.1}%, avg_lat={}) ss_first={:.0}% | slots={} ordered={:.1}% ooo_avg={:.2} tick_end={:.1}%",
        report.elapsed_secs,
        report.merger.ss_received,
        report.merger.ys_received,
        report.derived.unique_entries,
        report.derived.both_sources_confirmed,
        report.derived.both_confirm_rate * 100.0,
        lat,
        report.derived.ss_first_rate * 100.0,
        report.ordering.slots_sealed,
        report.derived.fully_ordered_rate * 100.0,
        report.derived.avg_out_of_order_per_slot,
        report.derived.tick_ending_rate * 100.0,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_report_handles_zero_division() {
        let r = build_report(
            Duration::from_secs(0),
            MergerCountersSnapshot::default(),
            OrderingCountersSnapshot::default(),
        );
        assert_eq!(r.derived.unique_entries, 0);
        assert_eq!(r.derived.both_confirm_rate, 0.0);
        assert_eq!(r.derived.avg_entries_per_slot, 0.0);
    }

    #[test]
    fn derived_rates_computed() {
        let merger = MergerCountersSnapshot {
            ss_received: 100,
            ys_received: 100,
            ss_first: 80,
            ys_first: 20,
            confirmed_by_both: 90,
            confirm_latency_sum_ns: 90 * 5_000_000, // average 5ms
            confirm_latency_min_ns: 1_000_000,
            confirm_latency_max_ns: 20_000_000,
            duplicates: 10,
            output_full: 0,
        };
        let ordering = OrderingCountersSnapshot {
            slots_sealed: 10,
            slots_fully_ordered: 9,
            slots_with_disorder: 1,
            slots_with_gaps: 0,
            slots_ending_on_tick: 10,
            total_out_of_order: 3,
            total_missing_indices: 0,
        };
        let r = build_report(Duration::from_secs(60), merger, ordering);
        assert_eq!(r.derived.unique_entries, 100);
        assert!((r.derived.both_confirm_rate - 0.9).abs() < 1e-9);
        assert!((r.derived.ss_first_rate - 0.8).abs() < 1e-9);
        assert_eq!(r.derived.avg_entries_per_slot, 10.0);
        assert!((r.derived.fully_ordered_rate - 0.9).abs() < 1e-9);
        assert!((r.derived.avg_out_of_order_per_slot - 0.3).abs() < 1e-9);
        assert!((r.derived.confirm_latency_avg_us.unwrap() - 5_000.0).abs() < 1e-6);
        assert_eq!(r.derived.confirm_latency_min_us, Some(1_000.0));
        assert_eq!(r.derived.confirm_latency_max_us, Some(20_000.0));
    }
}
