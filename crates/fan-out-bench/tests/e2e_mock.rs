//! End-to-end mock pipeline test.
//!
//! Simulates: schedule → presign → mock dispatch → fake observation → parquet.
//! Verifies dedup logic (1 LANDED + N-1 DEDUPED per trigger).

use crossbeam_channel::bounded;
use fan_out_bench::{
    config::SenderKind,
    counters::BenchCounters,
    outcome::{FinalStatus, ObservedSource, TentativeOutcome},
    pool::{PreSignedTx, TxPool},
    schedule::Schedule,
    senders::mock::MockSender,
    senders::TxSender,
    tx_builder::{build_variant, VariantParams},
    writer::{record::FinalRecord, ParquetWriterConfig, spawn_parquet},
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use solana_sdk::{hash::Hash, pubkey::Pubkey, signature::Keypair, signer::Signer};
use std::fs::File;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_mock_pipeline_produces_expected_outcomes() {
    let tmp = TempDir::new().unwrap();
    let parquet_path = tmp.path().join("tx-events.parquet");

    let senders: Vec<Arc<dyn TxSender>> = vec![
        Arc::new(MockSender::always_ack(0, "mock-a")),
        Arc::new(MockSender::always_ack(1, "mock-b")),
        Arc::new(MockSender::always_ack(2, "mock-c")),
    ];

    let (final_tx, final_rx) = bounded::<FinalRecord>(1000);
    let writer_handle = spawn_parquet(ParquetWriterConfig {
        final_rx,
        output_path: parquet_path.clone(),
        row_group_size: 16,
        flush_interval: Duration::from_millis(100),
        pinned_core: None,
        counters: Arc::new(BenchCounters::default()),
    }).unwrap();

    let mut schedule = Schedule::new(Some(42), 1000, 5);
    let chunk = schedule.generate_chunk();
    assert_eq!(chunk.len(), 5);

    let signer = Arc::new(Keypair::new());
    let nonce_pubkey = Pubkey::new_unique();
    let nonce_blockhash = Hash::new_unique();

    let pool = TxPool::new();
    for entry in &chunk {
        for sender in &senders {
            let variant = build_variant(
                VariantParams {
                    nonce_pubkey,
                    nonce_blockhash,
                    payer: signer.pubkey(),
                    sender_id: sender.id(),
                    sender_kind: SenderKind::Mock,
                    tip_account: None,
                    tip_lamports: 1000,
                    priority_fee_microlamports: 5000,
                    compute_unit_limit: 200_000,
                },
                &signer,
            ).unwrap();
            pool.insert(
                entry.slot,
                entry.tick,
                sender.id(),
                PreSignedTx {
                    tx: Arc::new(variant.tx),
                    message_hash: variant.message_hash,
                    prepared_at: Instant::now(),
                    pool_ready_at: Instant::now(),
                },
            );
        }
    }
    assert_eq!(pool.len(), 5 * 3);

    let run_id = "e2e-mock".to_string();
    let anchor = Instant::now();
    for entry in &chunk {
        let variants = pool.take_all_for(entry.slot, entry.tick);
        assert_eq!(variants.len(), 3);

        let mut records = Vec::new();
        for (order, (sender_id, presigned)) in variants.iter().enumerate() {
            let send_at_ns = anchor.elapsed().as_nanos() as u64;
            let sender = senders.iter().find(|s| s.id() == *sender_id).unwrap();
            let send_outcome = sender.send(&presigned.tx).await;
            let send_ack_at_ns = send_outcome.send_ack_at.map(|t| t.duration_since(anchor).as_nanos() as u64);

            let (tentative, observed_at, observed_source) = if *sender_id == 0 {
                (TentativeOutcome::LandedTentative, Some(send_at_ns + 100_000_000), Some(ObservedSource::Ss))
            } else {
                (TentativeOutcome::DedupedTentative, None, None)
            };

            let record = FinalRecord {
                trigger_slot: entry.slot,
                trigger_tick: entry.tick,
                trigger_id: [0; 16],
                nonce_account_id: 0,
                nonce_blockhash_used: nonce_blockhash,
                sender_id: *sender_id,
                sender_name: sender.name().to_string(),
                tx_signature: send_outcome.signature,
                tx_message_hash: presigned.message_hash,
                endpoint_url: sender.endpoint_url().to_string(),
                protocol: sender.protocol().to_string(),
                auth_tier: None,
                tip_account_used: None,
                tip_lamports: 1000,
                priority_fee_microlamports: 5000,
                compute_unit_limit: 200_000,
                prepared_at_ns: presigned.prepared_at.duration_since(anchor).as_nanos() as u64,
                pool_ready_at_ns: presigned.pool_ready_at.duration_since(anchor).as_nanos() as u64,
                trigger_observed_at_ns: send_at_ns,
                send_at_ns,
                send_ack_at_ns,
                send_order_in_trigger: order as u8,
                host_clock_offset_ns: None,
                send_error: send_outcome.error.clone(),
                rpc_err_code: send_outcome.rpc_err_code,
                rpc_err_message: send_outcome.rpc_err_message.clone(),
                provider_request_id: send_outcome.provider_request_id.clone(),
                http_status: send_outcome.http_status,
                rate_limit_state: send_outcome.rate_limit_state,
                observed_slot: if observed_at.is_some() { Some(entry.slot) } else { None },
                observed_entry_index: None,
                observed_tick_in_slot: None,
                observed_cumulative_hashes_in_slot: None,
                ss_observed_at_ns: observed_at,
                ys_observed_at_ns: None,
                observed_at_ns: observed_at,
                observed_source,
                commitment_at_resolution: None,
                tentative_outcome: tentative,
                final_status: FinalStatus::Pending,
                siblings_resolved_at_ns: None,
                leader_pubkey: None, leader_region_cc: None, leader_dc_label: None,
                leader_continent: None, leader_stake_lamports: None, validator_client: None,
                tick_delta: None, hash_delta: None, slot_delta: None,
                leader_changed: false,
                wall_trigger_to_send_ns: None, wall_send_rtt_ns: None,
                wall_send_to_observed_ns: None, wall_send_to_ss_observed_ns: None,
                wall_send_to_ys_observed_ns: None,
                nonce_update_observed_at_ns: None, nonce_update_source: None,
                nonce_advanced_to_slot: None,
                run_id: run_id.clone(),
                chunk_index: 0,
            };
            records.push(record);
        }
        for r in records {
            final_tx.send(r).unwrap();
        }
    }

    drop(final_tx);
    writer_handle.join().unwrap();

    let file = File::open(&parquet_path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap().build().unwrap();
    let mut total = 0;
    let mut landed = 0;
    let mut deduped = 0;
    for batch in reader {
        let batch = batch.unwrap();
        total += batch.num_rows();
        let outcome_col = batch.column_by_name("tentative_outcome").unwrap();
        let outcomes = outcome_col.as_any().downcast_ref::<arrow_array::StringArray>().unwrap();
        for i in 0..batch.num_rows() {
            match outcomes.value(i) {
                "LANDED_TENTATIVE" => landed += 1,
                "DEDUPED_TENTATIVE" => deduped += 1,
                other => panic!("unexpected outcome: {}", other),
            }
        }
    }
    assert_eq!(total, 5 * 3, "expected 15 records (5 triggers × 3 senders)");
    assert_eq!(landed, 5, "exactly 1 LANDED per trigger");
    assert_eq!(deduped, 10, "exactly 2 DEDUPED per trigger");
}
