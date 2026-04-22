use tempfile::TempDir;
use tx_cutoff::report::{InclusionKind, JsonlWriter, SlotAggregator, TxRecord};

fn rec(slot: u64, kind: InclusionKind) -> TxRecord {
    TxRecord {
        block_idx: 0,
        block_num: 1,
        block_hash: "0x00".into(),
        slot_ms: slot,
        sample_idx: 0,
        wallet: "w1".into(),
        tx_hash: Some("0xabc".into()),
        nonce: 0,
        target_unix_ms: 0,
        sent_at_unix_ms: 0,
        wake_jitter_us: 10,
        rpc_rtt_us: 1000,
        send_result: "ok".into(),
        inclusion: kind,
        included_block: None,
    }
}

#[test]
fn jsonl_writer_serializes_records_one_per_line() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("log.jsonl");
    let mut w = JsonlWriter::create(&path).unwrap();
    w.write(&rec(8500, InclusionKind::Target)).unwrap();
    w.write(&rec(8550, InclusionKind::Late(1))).unwrap();
    w.flush().unwrap();
    let content = std::fs::read_to_string(&path).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(lines.len(), 2);
    assert!(lines[0].contains(r#""slot_ms":8500"#));
    assert!(lines[1].contains(r#""slot_ms":8550"#));
}

#[test]
fn slot_aggregator_counts_per_slot_outcomes() {
    let mut agg = SlotAggregator::new();
    agg.ingest(&rec(8500, InclusionKind::Target));
    agg.ingest(&rec(8500, InclusionKind::Target));
    agg.ingest(&rec(8500, InclusionKind::Late(1)));
    agg.ingest(&rec(8500, InclusionKind::Dropped));
    agg.ingest(&rec(8550, InclusionKind::Target));

    let s1 = agg.slot(8500).unwrap();
    assert_eq!(s1.sent, 4);
    assert_eq!(s1.included_target, 2);
    assert_eq!(s1.included_late, 1);
    assert_eq!(s1.dropped, 1);

    let s2 = agg.slot(8550).unwrap();
    assert_eq!(s2.sent, 1);
    assert_eq!(s2.included_target, 1);
}

#[test]
fn slot_aggregator_computes_cutoff_percentiles() {
    let mut agg = SlotAggregator::new();
    for _ in 0..100 {
        agg.ingest(&rec(8500, InclusionKind::Target));
    }
    for _ in 0..50 {
        agg.ingest(&rec(8550, InclusionKind::Target));
    }
    for _ in 0..50 {
        agg.ingest(&rec(8550, InclusionKind::Dropped));
    }
    let c = agg.cutoffs(&[50, 90, 95, 99]);
    assert_eq!(c.get(&99).copied(), Some(8500));
    assert_eq!(c.get(&50).copied(), Some(8550));
}

#[test]
fn csv_writer_emits_header_and_rows() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("summary.csv");
    let mut agg = SlotAggregator::new();
    for _ in 0..10 {
        agg.ingest(&rec(8500, InclusionKind::Target));
    }
    agg.finalize();
    tx_cutoff::report::write_csv(&path, &agg).unwrap();
    let content = std::fs::read_to_string(&path).unwrap();
    assert!(content.starts_with("slot_ms,sent,included_target"));
    assert!(content.contains("8500,10,10"));
}

#[test]
fn render_stdout_report_contains_key_sections() {
    let mut agg = SlotAggregator::new();
    for _ in 0..10 {
        agg.ingest(&rec(8500, InclusionKind::Target));
    }
    agg.finalize();
    let out = tx_cutoff::report::render_stdout_report(&agg, &[50, 90, 95, 99]);
    assert!(out.contains("RUN SUMMARY"));
    assert!(out.contains("Inclusion cutoff curve"));
    assert!(out.contains("Estimated cutoffs"));
}
