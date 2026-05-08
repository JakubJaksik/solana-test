use std::fs::File;
use std::time::Duration;

use crossbeam_channel::bounded;
use entry_comparator::diff_record::{DiffRecord, Source};
use entry_comparator::writer::{spawn, WriterConfig};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use smallvec::smallvec;

fn record(slot: u64) -> DiffRecord {
    DiffRecord {
        slot,
        entry_index: 0,
        num_hashes: 100,
        source: Source::Both,
        ys_observed_ns: Some(slot * 1000),
        ss_first_shred_ns: Some(slot * 1000 + 1),
        ss_fec_complete_ns: Some(slot * 1000 + 2),
        ys_hash: Some([0; 32]),
        ss_hash: Some([0; 32]),
        ys_tx_count: Some(0),
        ss_tx_count: Some(0),
        hash_match: true,
        sig_set_match: None,
        leader_pubkey: Some([1; 32]),
        ys_signatures: smallvec![],
        ss_signatures: smallvec![],
    }
}

#[test]
fn roundtrip_parquet() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("diff.parquet");
    let (tx, rx) = bounded(1024);
    let h = spawn(WriterConfig {
        diff_rx: rx,
        output_path: path.clone(),
        row_group_size: 4,
        flush_interval: Duration::from_millis(100),
        pinned_core: None,
    })
    .unwrap();

    for i in 0..10u64 {
        tx.send(record(i)).unwrap();
    }
    drop(tx);
    h.join().unwrap();

    let file = File::open(&path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let total: usize = reader.map(|r| r.unwrap().num_rows()).sum();
    assert_eq!(total, 10);
}

#[test]
fn schema_includes_required_columns() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("diff.parquet");
    let (tx, rx) = bounded(16);
    let h = spawn(WriterConfig {
        diff_rx: rx,
        output_path: path.clone(),
        row_group_size: 8,
        flush_interval: Duration::from_millis(50),
        pinned_core: None,
    })
    .unwrap();

    tx.send(record(0)).unwrap();
    drop(tx);
    h.join().unwrap();

    let file = File::open(&path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let schema = builder.schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    for required in &[
        "slot",
        "entry_index",
        "source",
        "ys_observed_ns",
        "ss_fec_complete_ns",
        "hash_match",
        "leader_pubkey",
        "ys_signatures",
        "ss_signatures",
    ] {
        assert!(
            names.contains(required),
            "missing column {} (have: {:?})",
            required,
            names
        );
    }
}
