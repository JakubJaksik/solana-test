//! Report: JSONL per-tx + CSV per-slot + markdown + stdout ASCII tables & chart.

use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxRecord {
    pub block_idx: u64,
    pub block_num: u64,
    pub block_hash: String,
    pub slot_ms: u64,
    pub sample_idx: u64,
    pub wallet: String,
    pub tx_hash: Option<String>,
    pub nonce: u64,
    pub target_unix_ms: i64,
    pub sent_at_unix_ms: i64,
    pub wake_jitter_us: u64,
    pub rpc_rtt_us: u64,
    pub send_result: String,
    pub inclusion: InclusionKind,
    pub included_block: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "offset")]
pub enum InclusionKind {
    Target,
    Late(u64),
    Dropped,
    Pending,
    SendError,
}

pub struct JsonlWriter {
    writer: BufWriter<File>,
    path: PathBuf,
}

impl JsonlWriter {
    pub fn create(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let f = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(Self {
            writer: BufWriter::new(f),
            path: path.as_ref().to_path_buf(),
        })
    }

    pub fn write(&mut self, record: &TxRecord) -> std::io::Result<()> {
        let line = serde_json::to_string(record).map_err(std::io::Error::other)?;
        writeln!(self.writer, "{}", line)?;
        Ok(())
    }

    pub fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct SlotStats {
    pub slot_ms: u64,
    pub sent: u64,
    pub included_target: u64,
    pub included_late: u64,
    pub dropped: u64,
    pub errors: u64,
    pub wake_jitter_p50_us: u64,
    pub wake_jitter_p99_us: u64,
    pub rpc_rtt_p50_us: u64,
    pub rpc_rtt_p99_us: u64,
}

pub struct SlotAggregator {
    slots: BTreeMap<u64, SlotStats>,
    wake_hists: BTreeMap<u64, Histogram<u64>>,
    rtt_hists: BTreeMap<u64, Histogram<u64>>,
}

impl SlotAggregator {
    pub fn new() -> Self {
        Self {
            slots: BTreeMap::new(),
            wake_hists: BTreeMap::new(),
            rtt_hists: BTreeMap::new(),
        }
    }

    /// Ingestuje rekord tx. Engine emituje DWA rekordy per tx:
    /// - `Pending` zaraz po udanym send (lub `SendError` jeśli RPC odrzucił)
    /// - `Target/Late/Dropped` po klasyfikacji przez tracker
    ///
    /// Żeby uniknąć podwójnego liczenia:
    /// - `sent` + timing histograms inkrementowane TYLKO na Pending/SendError
    /// - `included_target/late/dropped` inkrementowane TYLKO na rezolwowanych
    pub fn ingest(&mut self, rec: &TxRecord) {
        let s = self.slots.entry(rec.slot_ms).or_insert(SlotStats {
            slot_ms: rec.slot_ms,
            ..Default::default()
        });
        match &rec.inclusion {
            InclusionKind::Pending => {
                // Initial send attempt — policz sent + timing
                s.sent += 1;
                let wake_h = self
                    .wake_hists
                    .entry(rec.slot_ms)
                    .or_insert_with(|| Histogram::new(3).unwrap());
                let rtt_h = self
                    .rtt_hists
                    .entry(rec.slot_ms)
                    .or_insert_with(|| Histogram::new(3).unwrap());
                let _ = wake_h.record(rec.wake_jitter_us);
                let _ = rtt_h.record(rec.rpc_rtt_us);
            }
            InclusionKind::SendError => {
                // RPC odrzucił — jedyny rekord dla tej tx
                s.sent += 1;
                s.errors += 1;
                let wake_h = self
                    .wake_hists
                    .entry(rec.slot_ms)
                    .or_insert_with(|| Histogram::new(3).unwrap());
                let rtt_h = self
                    .rtt_hists
                    .entry(rec.slot_ms)
                    .or_insert_with(|| Histogram::new(3).unwrap());
                let _ = wake_h.record(rec.wake_jitter_us);
                let _ = rtt_h.record(rec.rpc_rtt_us);
            }
            InclusionKind::Target => s.included_target += 1,
            InclusionKind::Late(_) => s.included_late += 1,
            InclusionKind::Dropped => s.dropped += 1,
        }
    }

    pub fn finalize(&mut self) {
        for (slot, stats) in self.slots.iter_mut() {
            if let Some(h) = self.wake_hists.get(slot) {
                stats.wake_jitter_p50_us = h.value_at_quantile(0.5);
                stats.wake_jitter_p99_us = h.value_at_quantile(0.99);
            }
            if let Some(h) = self.rtt_hists.get(slot) {
                stats.rpc_rtt_p50_us = h.value_at_quantile(0.5);
                stats.rpc_rtt_p99_us = h.value_at_quantile(0.99);
            }
        }
    }

    pub fn slot(&self, slot_ms: u64) -> Option<&SlotStats> {
        self.slots.get(&slot_ms)
    }

    pub fn slots_ordered(&self) -> Vec<&SlotStats> {
        self.slots.values().collect()
    }

    /// Returns the highest slot_ms where `included_target / sent >= pct / 100`.
    pub fn cutoffs(&self, percentiles: &[u64]) -> BTreeMap<u64, u64> {
        let mut out = BTreeMap::new();
        for &p in percentiles {
            let threshold = p as f64 / 100.0;
            let mut best: Option<u64> = None;
            for s in self.slots.values() {
                if s.sent == 0 {
                    continue;
                }
                let rate = s.included_target as f64 / s.sent as f64;
                if rate >= threshold {
                    best = Some(s.slot_ms);
                }
            }
            if let Some(slot) = best {
                out.insert(p, slot);
            }
        }
        out
    }
}

impl Default for SlotAggregator {
    fn default() -> Self {
        Self::new()
    }
}

pub fn write_csv(path: impl AsRef<Path>, agg: &SlotAggregator) -> std::io::Result<()> {
    let mut f = BufWriter::new(File::create(path)?);
    writeln!(
        f,
        "slot_ms,sent,included_target,included_late,dropped,errors,pct_target,wake_jitter_p50_us,wake_jitter_p99_us,rpc_rtt_p50_us,rpc_rtt_p99_us"
    )?;
    for s in agg.slots_ordered() {
        let pct = if s.sent > 0 {
            (s.included_target as f64 / s.sent as f64) * 100.0
        } else {
            0.0
        };
        writeln!(
            f,
            "{},{},{},{},{},{},{:.2},{},{},{},{}",
            s.slot_ms,
            s.sent,
            s.included_target,
            s.included_late,
            s.dropped,
            s.errors,
            pct,
            s.wake_jitter_p50_us,
            s.wake_jitter_p99_us,
            s.rpc_rtt_p50_us,
            s.rpc_rtt_p99_us
        )?;
    }
    Ok(())
}

pub fn render_stdout_report(agg: &SlotAggregator, percentiles: &[u64]) -> String {
    let mut out = String::new();
    out.push_str("══════════════════════════════════════════════════════════\n");
    out.push_str(" RUN SUMMARY\n");
    out.push_str("══════════════════════════════════════════════════════════\n");

    let mut total_sent = 0u64;
    let mut total_target = 0u64;
    let mut total_late = 0u64;
    let mut total_dropped = 0u64;
    let mut total_errors = 0u64;
    for s in agg.slots_ordered() {
        total_sent += s.sent;
        total_target += s.included_target;
        total_late += s.included_late;
        total_dropped += s.dropped;
        total_errors += s.errors;
    }
    out.push_str(&format!(" Total tx sent:          {}\n", total_sent));
    if total_sent > 0 {
        let pct = |n: u64| (n as f64 / total_sent as f64) * 100.0;
        out.push_str(&format!(
            " Included (target block): {} ({:.2}%)\n",
            total_target,
            pct(total_target)
        ));
        out.push_str(&format!(
            " Included (late):         {} ({:.2}%)\n",
            total_late,
            pct(total_late)
        ));
        out.push_str(&format!(
            " Dropped:                 {} ({:.2}%)\n",
            total_dropped,
            pct(total_dropped)
        ));
        out.push_str(&format!(
            " Send errors:             {} ({:.2}%)\n",
            total_errors,
            pct(total_errors)
        ));
    }

    out.push_str("\n Inclusion cutoff curve:\n\n");
    out.push_str(" slot_ms │ sent │ incT │  %   │ chart\n");
    out.push_str(" ────────┼──────┼──────┼──────┼──────────────────────────────\n");
    for s in agg.slots_ordered() {
        let pct = if s.sent > 0 {
            (s.included_target as f64 / s.sent as f64) * 100.0
        } else {
            0.0
        };
        let bar_len = (pct / 100.0 * 30.0) as usize;
        let bar: String = std::iter::repeat_n('█', bar_len)
            .chain(std::iter::repeat_n('░', 30 - bar_len))
            .collect();
        out.push_str(&format!(
            "  {:>5} │  {:>3} │  {:>3} │ {:>4.0}% │ {}\n",
            s.slot_ms, s.sent, s.included_target, pct, bar
        ));
    }

    out.push_str("\n Estimated cutoffs:\n");
    let cutoffs = agg.cutoffs(percentiles);
    for p in percentiles {
        if let Some(slot) = cutoffs.get(p) {
            out.push_str(&format!("   {:>2}% inclusion: <= {} ms\n", p, slot));
        } else {
            out.push_str(&format!(
                "   {:>2}% inclusion: (no slot reached threshold)\n",
                p
            ));
        }
    }

    out.push_str("══════════════════════════════════════════════════════════\n");
    out
}

pub fn write_markdown(path: impl AsRef<Path>, content: &str) -> std::io::Result<()> {
    std::fs::write(path, content)
}
