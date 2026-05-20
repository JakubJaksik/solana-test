//! Pipeline integration test — matcher + parquet end-to-end with mocked events.
//! Verifies LANDED + DEDUPED rows appear in parquet.

use crossbeam_channel::{bounded, unbounded};
use entry_sources::SourceKind;
use fan_out_bench::counters::BenchCounters;
use fan_out_bench::match_event::MatchEvent;
use fan_out_bench::matcher::{spawn as spawn_matcher, MatcherConfig, RegisterEvent, SendEvent};
use fan_out_bench::trigger_id::TriggerId;
use fan_out_bench::writer::{record::FinalRecord, spawn_parquet, ParquetWriterConfig};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use solana_sdk::{hash::Hash, signature::Signature};
use std::fs::File;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;

#[test]
fn matcher_to_parquet_emits_winner_and_siblings() {
    let tmp = TempDir::new().unwrap();
    let parquet_path = tmp.path().join("tx-events.parquet");

    let (reg_tx, reg_rx) = unbounded();
    let (_send_tx, send_rx) = unbounded::<SendEvent>();
    let (match_tx, match_rx) = unbounded();
    let (final_tx, final_rx) = bounded::<FinalRecord>(1000);
    let stop = Arc::new(AtomicBool::new(false));
    let anchor = Instant::now();

    let parquet_handle = spawn_parquet(ParquetWriterConfig {
        final_rx,
        output_path: parquet_path.clone(),
        row_group_size: 16,
        flush_interval: Duration::from_millis(100),
        pinned_core: None,
        counters: Arc::new(BenchCounters::default()),
    }).unwrap();

    let matcher_handle = spawn_matcher(MatcherConfig {
        register_rx: reg_rx,
        send_event_rx: send_rx,
        match_event_rx: match_rx,
        final_tx: final_tx.clone(),
        finality_tx: None,
        pending_sigs: Arc::new(dashmap::DashSet::new()),
        deadline: Duration::from_secs(60),
        run_id: "pipeline-mock".into(),
        anchor,
        pinned_core: None,
        counters: Arc::new(BenchCounters::default()),
        stop: stop.clone(),
        nonce_manager: None,
        slot_hash_cache: None,
        nonce_restore_after: Duration::from_secs(8),
    }).unwrap();

    let tid = TriggerId::new(100, 5, 0);
    let sigs = [Signature::new_unique(), Signature::new_unique(), Signature::new_unique()];
    for (i, sig) in sigs.iter().enumerate() {
        reg_tx.send(RegisterEvent {
            trigger_id: tid,
            sender_id: i as u8,
            sender_name: format!("mock-{}", i),
            endpoint_url: "mock://x".into(),
            protocol: "MOCK".into(),
            auth_tier: None,
            tip_account_used: None,
            tip_lamports: 1000,
            priority_fee_microlamports: 5000,
            compute_unit_limit: 200_000,
            signature: *sig,
            tx_message_hash: [0; 32],
            send_order_in_trigger: i as u8,
            trigger_slot: 100,
            trigger_tick: 5,
            nonce_account_id: 0,
            nonce_blockhash_used: Hash::default(),
            prepared_at: anchor,
            pool_ready_at: anchor,
            trigger_observed_at: anchor,
        }).unwrap();
    }

    std::thread::sleep(Duration::from_millis(20));

    match_tx.send(MatchEvent {
        signature: sigs[1],
        observed_at: Instant::now(),
        observed_slot: 100,
        observed_entry_index: 3,
        observed_tick_in_slot: Some(5),
        observed_cumulative_hashes_in_slot: Some(312_500),
        observed_source: SourceKind::ShredStream,
    }).unwrap();

    std::thread::sleep(Duration::from_millis(80));

    drop(reg_tx); drop(match_tx);
    drop(final_tx);
    stop.store(true, Ordering::Relaxed);

    let _ = matcher_handle.join();
    let _ = parquet_handle.join();

    let file = File::open(&parquet_path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap().build().unwrap();
    let mut landed = 0;
    let mut deduped = 0;
    let mut total = 0;
    for batch in reader {
        let batch = batch.unwrap();
        total += batch.num_rows();
        let col = batch.column_by_name("tentative_outcome").unwrap();
        let strs = col.as_any().downcast_ref::<arrow_array::StringArray>().unwrap();
        for i in 0..batch.num_rows() {
            match strs.value(i) {
                "LANDED_TENTATIVE" => landed += 1,
                "DEDUPED_TENTATIVE" => deduped += 1,
                _ => {}
            }
        }
    }
    assert_eq!(total, 3);
    assert_eq!(landed, 1);
    assert_eq!(deduped, 2);
}
