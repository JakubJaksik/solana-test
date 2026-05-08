use std::fs::File;
use std::io::Write;

use arrow::array::{Array, BooleanArray, StringArray, UInt64Array};
use hdrhistogram::Histogram;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::config::ReportArgs;

#[derive(Default)]
struct SignedHist {
    earlier_count: u64,
    later_count: u64,
    equal_count: u64,
    earlier_abs: Option<Histogram<u64>>,
    later: Option<Histogram<u64>>,
}

impl SignedHist {
    fn new() -> Self {
        Self {
            earlier_abs: Histogram::<u64>::new(3).ok(),
            later: Histogram::<u64>::new(3).ok(),
            ..Default::default()
        }
    }

    fn record(&mut self, ss_ns: u64, ys_ns: u64) {
        if ss_ns < ys_ns {
            self.earlier_count += 1;
            if let Some(h) = self.earlier_abs.as_mut() {
                let _ = h.record(ys_ns - ss_ns);
            }
        } else if ss_ns > ys_ns {
            self.later_count += 1;
            if let Some(h) = self.later.as_mut() {
                let _ = h.record(ss_ns - ys_ns);
            }
        } else {
            self.equal_count += 1;
        }
    }
}

pub fn generate(args: ReportArgs) -> anyhow::Result<()> {
    let parquet_path = args.input_dir.join("diff.parquet");
    let file = File::open(&parquet_path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut total: u64 = 0;
    let mut both: u64 = 0;
    let mut ys_only: u64 = 0;
    let mut ss_only: u64 = 0;
    let mut hash_mismatches: u64 = 0;
    let mut sig_mismatches: u64 = 0;

    let mut h_first = SignedHist::new();
    let mut h_fec = SignedHist::new();
    let mut h_decode = Histogram::<u64>::new(3)?;
    let mut decode_samples: u64 = 0;

    for batch_result in reader {
        let batch = batch_result?;
        let n = batch.num_rows();
        total += n as u64;

        let source_col = batch
            .column_by_name("source")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| anyhow::anyhow!("missing source column"))?;
        let ys_observed = batch
            .column_by_name("ys_observed_ns")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| anyhow::anyhow!("missing ys_observed_ns"))?;
        let ss_first_shred = batch
            .column_by_name("ss_first_shred_ns")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| anyhow::anyhow!("missing ss_first_shred_ns"))?;
        let ss_fec_complete = batch
            .column_by_name("ss_fec_complete_ns")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| anyhow::anyhow!("missing ss_fec_complete_ns"))?;
        let hash_match = batch
            .column_by_name("hash_match")
            .and_then(|c| c.as_any().downcast_ref::<BooleanArray>())
            .ok_or_else(|| anyhow::anyhow!("missing hash_match"))?;
        let sig_set_match = batch
            .column_by_name("sig_set_match")
            .and_then(|c| c.as_any().downcast_ref::<BooleanArray>())
            .ok_or_else(|| anyhow::anyhow!("missing sig_set_match"))?;

        for i in 0..n {
            let src = source_col.value(i);
            match src {
                "BOTH" => {
                    both += 1;
                    if !hash_match.value(i) {
                        hash_mismatches += 1;
                    }
                    if !sig_set_match.is_null(i) && !sig_set_match.value(i) {
                        sig_mismatches += 1;
                    }
                    let ys_ns = if !ys_observed.is_null(i) {
                        Some(ys_observed.value(i))
                    } else {
                        None
                    };
                    let fs_ns = if !ss_first_shred.is_null(i) {
                        Some(ss_first_shred.value(i))
                    } else {
                        None
                    };
                    let fc_ns = if !ss_fec_complete.is_null(i) {
                        Some(ss_fec_complete.value(i))
                    } else {
                        None
                    };
                    if let (Some(ys), Some(fs)) = (ys_ns, fs_ns) {
                        h_first.record(fs, ys);
                    }
                    if let (Some(ys), Some(fc)) = (ys_ns, fc_ns) {
                        h_fec.record(fc, ys);
                    }
                    if let (Some(fs), Some(fc)) = (fs_ns, fc_ns) {
                        if fc >= fs {
                            let _ = h_decode.record(fc - fs);
                            decode_samples += 1;
                        }
                    }
                }
                "YS_ONLY" => ys_only += 1,
                "SS_ONLY" => ss_only += 1,
                _ => {}
            }
        }
    }

    let mut out = File::create(&args.output)?;
    writeln!(out, "# Entry comparator report")?;
    writeln!(out)?;
    writeln!(out, "Source: `{}`", parquet_path.display())?;
    writeln!(out)?;

    writeln!(out, "## Counts")?;
    writeln!(out, "- Total rows: {total}")?;
    writeln!(out, "- BOTH: {both}")?;
    writeln!(out, "- YS_ONLY: {ys_only}")?;
    writeln!(out, "- SS_ONLY: {ss_only}")?;
    writeln!(out, "- Hash mismatches (BOTH only): {hash_mismatches}")?;
    writeln!(out, "- Signature-set mismatches (BOTH only): {sig_mismatches}")?;
    writeln!(out)?;

    writeln!(out, "## Latency: SS first_shred vs YS observed")?;
    print_signed_hist(&mut out, &h_first)?;
    writeln!(out)?;

    writeln!(out, "## Latency: SS fec_complete vs YS observed")?;
    print_signed_hist(&mut out, &h_fec)?;
    writeln!(out)?;

    writeln!(out, "## Decode: SS fec_complete − SS first_shred (always ≥0, ns)")?;
    if decode_samples == 0 {
        writeln!(out, "- (no samples)")?;
    } else {
        writeln!(out, "- count: {decode_samples}")?;
        writeln!(out, "- min: {} ns", h_decode.min())?;
        writeln!(out, "- p50: {} ns", h_decode.value_at_quantile(0.50))?;
        writeln!(out, "- p95: {} ns", h_decode.value_at_quantile(0.95))?;
        writeln!(out, "- p99: {} ns", h_decode.value_at_quantile(0.99))?;
        writeln!(out, "- max: {} ns", h_decode.max())?;
    }
    writeln!(out)?;
    writeln!(out, "_For deeper analysis (per-leader, per-region, multi-modal histograms) load the Parquet directly into DuckDB or pandas._")?;

    println!("report written to {}", args.output.display());
    Ok(())
}

fn print_signed_hist(out: &mut File, h: &SignedHist) -> std::io::Result<()> {
    writeln!(out, "- SS earlier than YS: {} samples", h.earlier_count)?;
    if let Some(hi) = h.earlier_abs.as_ref() {
        if hi.len() > 0 {
            writeln!(out, "    - min: {} ns", hi.min())?;
            writeln!(out, "    - p50: {} ns", hi.value_at_quantile(0.50))?;
            writeln!(out, "    - p95: {} ns", hi.value_at_quantile(0.95))?;
            writeln!(out, "    - p99: {} ns", hi.value_at_quantile(0.99))?;
            writeln!(out, "    - max: {} ns", hi.max())?;
        }
    }
    writeln!(out, "- SS later than YS: {} samples", h.later_count)?;
    if let Some(hi) = h.later.as_ref() {
        if hi.len() > 0 {
            writeln!(out, "    - min: {} ns", hi.min())?;
            writeln!(out, "    - p50: {} ns", hi.value_at_quantile(0.50))?;
            writeln!(out, "    - p95: {} ns", hi.value_at_quantile(0.95))?;
            writeln!(out, "    - p99: {} ns", hi.value_at_quantile(0.99))?;
            writeln!(out, "    - max: {} ns", hi.max())?;
        }
    }
    writeln!(out, "- Equal timestamps: {}", h.equal_count)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use crate::diff_record::{DiffRecord, Source};
    use crate::writer::{spawn as spawn_writer, WriterConfig};
    use crossbeam_channel::bounded;
    use smallvec::smallvec;

    fn rec(slot: u64, source: Source, ys: Option<u64>, fs: Option<u64>, fc: Option<u64>) -> DiffRecord {
        DiffRecord {
            slot,
            entry_index: 0,
            num_hashes: 100,
            source,
            ys_observed_ns: ys,
            ss_first_shred_ns: fs,
            ss_fec_complete_ns: fc,
            ys_hash: Some([0; 32]),
            ss_hash: Some([0; 32]),
            ys_tx_count: Some(0),
            ss_tx_count: Some(0),
            hash_match: true,
            sig_set_match: None,
            leader_pubkey: None,
            ys_signatures: smallvec![],
            ss_signatures: smallvec![],
        }
    }

    fn write_test_parquet(path: &std::path::Path) {
        let (tx, rx) = bounded(64);
        let h = spawn_writer(WriterConfig {
            diff_rx: rx,
            output_path: path.to_path_buf(),
            row_group_size: 4,
            flush_interval: Duration::from_millis(50),
            pinned_core: None,
        })
        .unwrap();
        // 5 BOTH rows (some SS-earlier, some SS-later, some equal)
        tx.send(rec(1, Source::Both, Some(1000), Some(900), Some(1100))).unwrap();   // first_shred earlier, fec_complete later
        tx.send(rec(2, Source::Both, Some(2000), Some(1800), Some(2200))).unwrap();
        tx.send(rec(3, Source::Both, Some(3000), Some(3000), Some(3000))).unwrap();   // equal
        tx.send(rec(4, Source::Both, Some(4000), Some(4500), Some(4800))).unwrap();   // both later
        tx.send(rec(5, Source::YsOnly, Some(5000), None, None)).unwrap();             // ys only
        tx.send(rec(6, Source::SsOnly, None, Some(6000), Some(6100))).unwrap();        // ss only
        drop(tx);
        h.join().unwrap();
    }

    #[test]
    fn report_counts_and_writes_markdown() {
        let dir = tempfile::tempdir().unwrap();
        let parquet = dir.path().join("diff.parquet");
        write_test_parquet(&parquet);

        let report_out = dir.path().join("report.md");
        generate(ReportArgs {
            input_dir: dir.path().to_path_buf(),
            output: report_out.clone(),
        })
        .unwrap();

        let s = std::fs::read_to_string(&report_out).unwrap();
        assert!(s.contains("Total rows: 6"));
        assert!(s.contains("BOTH: 4"));
        assert!(s.contains("YS_ONLY: 1"));
        assert!(s.contains("SS_ONLY: 1"));
        assert!(s.contains("SS earlier than YS: 2"));
        assert!(s.contains("SS later than YS: 1"));
        assert!(s.contains("Equal timestamps: 1"));
    }
}
