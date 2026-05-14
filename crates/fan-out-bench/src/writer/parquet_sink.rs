//! Background-thread Parquet writer.
//!
//! Consumes FinalRecord from a crossbeam channel, batches into row groups,
//! flushes to disk as records accumulate.

use crate::counters::BenchCounters;
use crate::outcome::{CommitmentAtResolution, ObservedSource, RateLimitState};
use crate::writer::record::FinalRecord;
use crate::writer::schema::final_record_schema;
use arrow_array::{
    builder::*, ArrayRef, RecordBatch,
};
use arrow_schema::Schema;
use crossbeam_channel::Receiver;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

pub struct ParquetWriterConfig {
    pub final_rx: Receiver<FinalRecord>,
    pub output_path: PathBuf,
    pub row_group_size: usize,
    pub flush_interval: Duration,
    pub pinned_core: Option<usize>,
    pub counters: Arc<BenchCounters>,
}

pub fn spawn_parquet(cfg: ParquetWriterConfig) -> std::io::Result<JoinHandle<()>> {
    std::thread::Builder::new()
        .name("parquet-writer".into())
        .spawn(move || {
            if let Some(core) = cfg.pinned_core {
                core_affinity::set_for_current(core_affinity::CoreId { id: core });
            }
            run_loop(cfg).unwrap_or_else(|e| {
                tracing::error!(error = %e, "parquet writer terminated with error");
            });
        })
}

fn run_loop(cfg: ParquetWriterConfig) -> anyhow::Result<()> {
    let schema = final_record_schema();
    let file = File::create(&cfg.output_path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

    let mut buffer: Vec<FinalRecord> = Vec::with_capacity(cfg.row_group_size);
    let mut last_flush = std::time::Instant::now();

    loop {
        match cfg.final_rx.recv_timeout(Duration::from_millis(500)) {
            Ok(rec) => {
                buffer.push(rec);
                if buffer.len() >= cfg.row_group_size {
                    flush_buffer(&mut writer, &schema, &mut buffer)?;
                    last_flush = std::time::Instant::now();
                }
            }
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                if !buffer.is_empty() && last_flush.elapsed() >= cfg.flush_interval {
                    flush_buffer(&mut writer, &schema, &mut buffer)?;
                    last_flush = std::time::Instant::now();
                }
            }
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                if !buffer.is_empty() {
                    flush_buffer(&mut writer, &schema, &mut buffer)?;
                }
                break;
            }
        }
    }

    writer.close()?;
    tracing::info!(path = ?cfg.output_path, "parquet writer closed cleanly");
    Ok(())
}

fn flush_buffer(
    writer: &mut ArrowWriter<File>,
    schema: &Schema,
    buffer: &mut Vec<FinalRecord>,
) -> anyhow::Result<()> {
    if buffer.is_empty() {
        return Ok(());
    }
    let batch = records_to_batch(schema, buffer)?;
    writer.write(&batch)?;
    buffer.clear();
    Ok(())
}

fn records_to_batch(schema: &Schema, records: &[FinalRecord]) -> anyhow::Result<RecordBatch> {
    let n = records.len();
    let mut b_trigger_slot = UInt64Builder::with_capacity(n);
    let mut b_trigger_tick = UInt8Builder::with_capacity(n);
    let mut b_trigger_id = FixedSizeBinaryBuilder::with_capacity(n, 16);
    let mut b_nonce_account_id = UInt16Builder::with_capacity(n);
    let mut b_nonce_blockhash_used = FixedSizeBinaryBuilder::with_capacity(n, 32);
    let mut b_sender_id = UInt8Builder::with_capacity(n);
    let mut b_sender_name = StringBuilder::with_capacity(n, n * 8);
    let mut b_tx_signature = FixedSizeBinaryBuilder::with_capacity(n, 64);
    let mut b_tx_message_hash = FixedSizeBinaryBuilder::with_capacity(n, 32);

    let mut b_endpoint_url = StringBuilder::with_capacity(n, n * 32);
    let mut b_protocol = StringBuilder::with_capacity(n, n * 8);
    let mut b_auth_tier = StringBuilder::with_capacity(n, n * 8);
    let mut b_tip_account_used = FixedSizeBinaryBuilder::with_capacity(n, 32);
    let mut b_tip_lamports = UInt64Builder::with_capacity(n);
    let mut b_priority_fee_microlamports = UInt64Builder::with_capacity(n);
    let mut b_compute_unit_limit = UInt32Builder::with_capacity(n);

    let mut b_prepared_at_ns = UInt64Builder::with_capacity(n);
    let mut b_pool_ready_at_ns = UInt64Builder::with_capacity(n);
    let mut b_trigger_observed_at_ns = UInt64Builder::with_capacity(n);
    let mut b_send_at_ns = UInt64Builder::with_capacity(n);
    let mut b_send_ack_at_ns = UInt64Builder::with_capacity(n);
    let mut b_send_order_in_trigger = UInt8Builder::with_capacity(n);
    let mut b_host_clock_offset_ns = Int64Builder::with_capacity(n);

    let mut b_send_error = StringBuilder::with_capacity(n, n * 16);
    let mut b_rpc_err_code = Int32Builder::with_capacity(n);
    let mut b_rpc_err_message = StringBuilder::with_capacity(n, n * 16);
    let mut b_provider_request_id = StringBuilder::with_capacity(n, n * 16);
    let mut b_http_status = UInt16Builder::with_capacity(n);
    let mut b_rate_limit_state = StringBuilder::with_capacity(n, n * 8);

    let mut b_observed_slot = UInt64Builder::with_capacity(n);
    let mut b_observed_entry_index = UInt32Builder::with_capacity(n);
    let mut b_observed_tick_in_slot = UInt8Builder::with_capacity(n);
    let mut b_observed_cumulative_hashes_in_slot = UInt64Builder::with_capacity(n);
    let mut b_ss_observed_at_ns = UInt64Builder::with_capacity(n);
    let mut b_ys_observed_at_ns = UInt64Builder::with_capacity(n);
    let mut b_observed_at_ns = UInt64Builder::with_capacity(n);
    let mut b_observed_source = StringBuilder::with_capacity(n, n * 4);
    let mut b_commitment_at_resolution = StringBuilder::with_capacity(n, n * 8);

    let mut b_tentative_outcome = StringBuilder::with_capacity(n, n * 16);
    let mut b_final_status = StringBuilder::with_capacity(n, n * 8);
    let mut b_siblings_resolved_at_ns = UInt64Builder::with_capacity(n);

    let mut b_leader_pubkey = FixedSizeBinaryBuilder::with_capacity(n, 32);
    let mut b_leader_region_cc = StringBuilder::with_capacity(n, n * 2);
    let mut b_leader_dc_label = StringBuilder::with_capacity(n, n * 16);
    let mut b_leader_continent = StringBuilder::with_capacity(n, n * 8);
    let mut b_leader_stake_lamports = UInt64Builder::with_capacity(n);
    let mut b_validator_client = StringBuilder::with_capacity(n, n * 16);

    let mut b_tick_delta = Int32Builder::with_capacity(n);
    let mut b_hash_delta = Int64Builder::with_capacity(n);
    let mut b_slot_delta = Int32Builder::with_capacity(n);
    let mut b_leader_changed = BooleanBuilder::with_capacity(n);
    let mut b_wall_trigger_to_send_ns = Int64Builder::with_capacity(n);
    let mut b_wall_send_rtt_ns = Int64Builder::with_capacity(n);
    let mut b_wall_send_to_observed_ns = Int64Builder::with_capacity(n);
    let mut b_wall_send_to_ss_observed_ns = Int64Builder::with_capacity(n);
    let mut b_wall_send_to_ys_observed_ns = Int64Builder::with_capacity(n);

    let mut b_nonce_update_observed_at_ns = UInt64Builder::with_capacity(n);
    let mut b_nonce_update_source = StringBuilder::with_capacity(n, n * 4);
    let mut b_nonce_advanced_to_slot = UInt64Builder::with_capacity(n);

    let mut b_run_id = StringBuilder::with_capacity(n, n * 16);
    let mut b_chunk_index = UInt32Builder::with_capacity(n);

    for r in records {
        b_trigger_slot.append_value(r.trigger_slot);
        b_trigger_tick.append_value(r.trigger_tick);
        b_trigger_id.append_value(r.trigger_id)?;
        b_nonce_account_id.append_value(r.nonce_account_id);
        b_nonce_blockhash_used.append_value(r.nonce_blockhash_used.as_ref())?;
        b_sender_id.append_value(r.sender_id);
        b_sender_name.append_value(&r.sender_name);
        b_tx_signature.append_value(r.tx_signature.as_ref())?;
        b_tx_message_hash.append_value(r.tx_message_hash)?;

        b_endpoint_url.append_value(&r.endpoint_url);
        b_protocol.append_value(&r.protocol);
        match &r.auth_tier { Some(s) => b_auth_tier.append_value(s), None => b_auth_tier.append_null() }
        match &r.tip_account_used { Some(p) => b_tip_account_used.append_value(p.as_ref())?, None => b_tip_account_used.append_null() }
        b_tip_lamports.append_value(r.tip_lamports);
        b_priority_fee_microlamports.append_value(r.priority_fee_microlamports);
        b_compute_unit_limit.append_value(r.compute_unit_limit);

        b_prepared_at_ns.append_value(r.prepared_at_ns);
        b_pool_ready_at_ns.append_value(r.pool_ready_at_ns);
        b_trigger_observed_at_ns.append_value(r.trigger_observed_at_ns);
        b_send_at_ns.append_value(r.send_at_ns);
        match r.send_ack_at_ns { Some(v) => b_send_ack_at_ns.append_value(v), None => b_send_ack_at_ns.append_null() }
        b_send_order_in_trigger.append_value(r.send_order_in_trigger);
        match r.host_clock_offset_ns { Some(v) => b_host_clock_offset_ns.append_value(v), None => b_host_clock_offset_ns.append_null() }

        match &r.send_error { Some(s) => b_send_error.append_value(s), None => b_send_error.append_null() }
        match r.rpc_err_code { Some(v) => b_rpc_err_code.append_value(v), None => b_rpc_err_code.append_null() }
        match &r.rpc_err_message { Some(s) => b_rpc_err_message.append_value(s), None => b_rpc_err_message.append_null() }
        match &r.provider_request_id { Some(s) => b_provider_request_id.append_value(s), None => b_provider_request_id.append_null() }
        match r.http_status { Some(v) => b_http_status.append_value(v), None => b_http_status.append_null() }
        b_rate_limit_state.append_value(rate_limit_state_str(r.rate_limit_state));

        match r.observed_slot { Some(v) => b_observed_slot.append_value(v), None => b_observed_slot.append_null() }
        match r.observed_entry_index { Some(v) => b_observed_entry_index.append_value(v), None => b_observed_entry_index.append_null() }
        match r.observed_tick_in_slot { Some(v) => b_observed_tick_in_slot.append_value(v), None => b_observed_tick_in_slot.append_null() }
        match r.observed_cumulative_hashes_in_slot { Some(v) => b_observed_cumulative_hashes_in_slot.append_value(v), None => b_observed_cumulative_hashes_in_slot.append_null() }
        match r.ss_observed_at_ns { Some(v) => b_ss_observed_at_ns.append_value(v), None => b_ss_observed_at_ns.append_null() }
        match r.ys_observed_at_ns { Some(v) => b_ys_observed_at_ns.append_value(v), None => b_ys_observed_at_ns.append_null() }
        match r.observed_at_ns { Some(v) => b_observed_at_ns.append_value(v), None => b_observed_at_ns.append_null() }
        match r.observed_source { Some(s) => b_observed_source.append_value(observed_source_str(s)), None => b_observed_source.append_null() }
        match r.commitment_at_resolution { Some(c) => b_commitment_at_resolution.append_value(commitment_str(c)), None => b_commitment_at_resolution.append_null() }

        b_tentative_outcome.append_value(r.tentative_outcome.as_str());
        b_final_status.append_value(r.final_status.as_str());
        match r.siblings_resolved_at_ns { Some(v) => b_siblings_resolved_at_ns.append_value(v), None => b_siblings_resolved_at_ns.append_null() }

        match r.leader_pubkey { Some(p) => b_leader_pubkey.append_value(p.as_ref())?, None => b_leader_pubkey.append_null() }
        match &r.leader_region_cc { Some(s) => b_leader_region_cc.append_value(s), None => b_leader_region_cc.append_null() }
        match &r.leader_dc_label { Some(s) => b_leader_dc_label.append_value(s), None => b_leader_dc_label.append_null() }
        match &r.leader_continent { Some(s) => b_leader_continent.append_value(s), None => b_leader_continent.append_null() }
        match r.leader_stake_lamports { Some(v) => b_leader_stake_lamports.append_value(v), None => b_leader_stake_lamports.append_null() }
        match &r.validator_client { Some(s) => b_validator_client.append_value(s), None => b_validator_client.append_null() }

        match r.tick_delta { Some(v) => b_tick_delta.append_value(v), None => b_tick_delta.append_null() }
        match r.hash_delta { Some(v) => b_hash_delta.append_value(v), None => b_hash_delta.append_null() }
        match r.slot_delta { Some(v) => b_slot_delta.append_value(v), None => b_slot_delta.append_null() }
        b_leader_changed.append_value(r.leader_changed);
        match r.wall_trigger_to_send_ns { Some(v) => b_wall_trigger_to_send_ns.append_value(v), None => b_wall_trigger_to_send_ns.append_null() }
        match r.wall_send_rtt_ns { Some(v) => b_wall_send_rtt_ns.append_value(v), None => b_wall_send_rtt_ns.append_null() }
        match r.wall_send_to_observed_ns { Some(v) => b_wall_send_to_observed_ns.append_value(v), None => b_wall_send_to_observed_ns.append_null() }
        match r.wall_send_to_ss_observed_ns { Some(v) => b_wall_send_to_ss_observed_ns.append_value(v), None => b_wall_send_to_ss_observed_ns.append_null() }
        match r.wall_send_to_ys_observed_ns { Some(v) => b_wall_send_to_ys_observed_ns.append_value(v), None => b_wall_send_to_ys_observed_ns.append_null() }

        match r.nonce_update_observed_at_ns { Some(v) => b_nonce_update_observed_at_ns.append_value(v), None => b_nonce_update_observed_at_ns.append_null() }
        match &r.nonce_update_source { Some(s) => b_nonce_update_source.append_value(s), None => b_nonce_update_source.append_null() }
        match r.nonce_advanced_to_slot { Some(v) => b_nonce_advanced_to_slot.append_value(v), None => b_nonce_advanced_to_slot.append_null() }

        b_run_id.append_value(&r.run_id);
        b_chunk_index.append_value(r.chunk_index);
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(b_trigger_slot.finish()),
        Arc::new(b_trigger_tick.finish()),
        Arc::new(b_trigger_id.finish()),
        Arc::new(b_nonce_account_id.finish()),
        Arc::new(b_nonce_blockhash_used.finish()),
        Arc::new(b_sender_id.finish()),
        Arc::new(b_sender_name.finish()),
        Arc::new(b_tx_signature.finish()),
        Arc::new(b_tx_message_hash.finish()),
        Arc::new(b_endpoint_url.finish()),
        Arc::new(b_protocol.finish()),
        Arc::new(b_auth_tier.finish()),
        Arc::new(b_tip_account_used.finish()),
        Arc::new(b_tip_lamports.finish()),
        Arc::new(b_priority_fee_microlamports.finish()),
        Arc::new(b_compute_unit_limit.finish()),
        Arc::new(b_prepared_at_ns.finish()),
        Arc::new(b_pool_ready_at_ns.finish()),
        Arc::new(b_trigger_observed_at_ns.finish()),
        Arc::new(b_send_at_ns.finish()),
        Arc::new(b_send_ack_at_ns.finish()),
        Arc::new(b_send_order_in_trigger.finish()),
        Arc::new(b_host_clock_offset_ns.finish()),
        Arc::new(b_send_error.finish()),
        Arc::new(b_rpc_err_code.finish()),
        Arc::new(b_rpc_err_message.finish()),
        Arc::new(b_provider_request_id.finish()),
        Arc::new(b_http_status.finish()),
        Arc::new(b_rate_limit_state.finish()),
        Arc::new(b_observed_slot.finish()),
        Arc::new(b_observed_entry_index.finish()),
        Arc::new(b_observed_tick_in_slot.finish()),
        Arc::new(b_observed_cumulative_hashes_in_slot.finish()),
        Arc::new(b_ss_observed_at_ns.finish()),
        Arc::new(b_ys_observed_at_ns.finish()),
        Arc::new(b_observed_at_ns.finish()),
        Arc::new(b_observed_source.finish()),
        Arc::new(b_commitment_at_resolution.finish()),
        Arc::new(b_tentative_outcome.finish()),
        Arc::new(b_final_status.finish()),
        Arc::new(b_siblings_resolved_at_ns.finish()),
        Arc::new(b_leader_pubkey.finish()),
        Arc::new(b_leader_region_cc.finish()),
        Arc::new(b_leader_dc_label.finish()),
        Arc::new(b_leader_continent.finish()),
        Arc::new(b_leader_stake_lamports.finish()),
        Arc::new(b_validator_client.finish()),
        Arc::new(b_tick_delta.finish()),
        Arc::new(b_hash_delta.finish()),
        Arc::new(b_slot_delta.finish()),
        Arc::new(b_leader_changed.finish()),
        Arc::new(b_wall_trigger_to_send_ns.finish()),
        Arc::new(b_wall_send_rtt_ns.finish()),
        Arc::new(b_wall_send_to_observed_ns.finish()),
        Arc::new(b_wall_send_to_ss_observed_ns.finish()),
        Arc::new(b_wall_send_to_ys_observed_ns.finish()),
        Arc::new(b_nonce_update_observed_at_ns.finish()),
        Arc::new(b_nonce_update_source.finish()),
        Arc::new(b_nonce_advanced_to_slot.finish()),
        Arc::new(b_run_id.finish()),
        Arc::new(b_chunk_index.finish()),
    ];
    let batch = RecordBatch::try_new(Arc::new(schema.clone()), arrays)?;
    Ok(batch)
}

fn rate_limit_state_str(s: RateLimitState) -> &'static str {
    match s {
        RateLimitState::Ok => "OK",
        RateLimitState::Throttled429 => "THROTTLED_429",
        RateLimitState::CircuitOpen => "CIRCUIT_OPEN",
        RateLimitState::Timeout => "TIMEOUT",
    }
}

fn observed_source_str(s: ObservedSource) -> &'static str {
    match s {
        ObservedSource::Ss => "SS",
        ObservedSource::Ys => "YS",
        ObservedSource::Both => "BOTH",
    }
}

fn commitment_str(c: CommitmentAtResolution) -> &'static str {
    match c {
        CommitmentAtResolution::Processed => "PROCESSED",
        CommitmentAtResolution::Confirmed => "CONFIRMED",
        CommitmentAtResolution::Finalized => "FINALIZED",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::outcome::{FinalStatus, TentativeOutcome};
    use crossbeam_channel::bounded;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use solana_sdk::{hash::Hash, signature::Signature};
    use std::fs::File;
    use tempfile::TempDir;

    fn fake_record(slot: u64, sender_id: u8) -> FinalRecord {
        FinalRecord {
            trigger_slot: slot,
            trigger_tick: 5,
            trigger_id: [0; 16],
            nonce_account_id: 0,
            nonce_blockhash_used: Hash::default(),
            sender_id,
            sender_name: format!("mock-{}", sender_id),
            tx_signature: Signature::default(),
            tx_message_hash: [0; 32],
            endpoint_url: "mock://x".into(),
            protocol: "MOCK".into(),
            auth_tier: None,
            tip_account_used: None,
            tip_lamports: 1000,
            priority_fee_microlamports: 5000,
            compute_unit_limit: 200_000,
            prepared_at_ns: 1, pool_ready_at_ns: 2, trigger_observed_at_ns: 3,
            send_at_ns: 4, send_ack_at_ns: Some(5), send_order_in_trigger: 0,
            host_clock_offset_ns: None,
            send_error: None, rpc_err_code: None, rpc_err_message: None,
            provider_request_id: None, http_status: Some(200),
            rate_limit_state: RateLimitState::Ok,
            observed_slot: None, observed_entry_index: None,
            observed_tick_in_slot: None, observed_cumulative_hashes_in_slot: None,
            ss_observed_at_ns: None, ys_observed_at_ns: None,
            observed_at_ns: None, observed_source: None,
            commitment_at_resolution: None,
            tentative_outcome: TentativeOutcome::LandedTentative,
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
            run_id: "test".into(), chunk_index: 0,
        }
    }

    #[test]
    fn writes_and_reads_back_records() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("out.parquet");
        let (tx, rx) = bounded(100);
        let handle = spawn_parquet(ParquetWriterConfig {
            final_rx: rx,
            output_path: path.clone(),
            row_group_size: 5,
            flush_interval: Duration::from_millis(100),
            pinned_core: None,
            counters: Arc::new(BenchCounters::default()),
        }).unwrap();

        for i in 0..10 {
            tx.send(fake_record(100 + i, (i % 2) as u8)).unwrap();
        }
        drop(tx);
        handle.join().unwrap();

        let file = File::open(&path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap().build().unwrap();
        let mut total_rows = 0;
        for batch in reader {
            let batch = batch.unwrap();
            total_rows += batch.num_rows();
        }
        assert_eq!(total_rows, 10);
    }
}
