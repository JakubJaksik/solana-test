//! Arrow schema for tx-events.parquet.
//!
//! Column list and types match spec §6.1.

use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;

pub fn final_record_schema() -> Arc<Schema> {
    let fields = vec![
        // Trigger / variant identification
        Field::new("trigger_slot", DataType::UInt64, false),
        Field::new("trigger_tick", DataType::UInt8, false),
        Field::new("trigger_id", DataType::FixedSizeBinary(16), false),
        Field::new("nonce_account_id", DataType::UInt16, false),
        Field::new("nonce_blockhash_used", DataType::FixedSizeBinary(32), false),
        Field::new("sender_id", DataType::UInt8, false),
        Field::new("sender_name", DataType::Utf8, false),
        Field::new("tx_signature", DataType::FixedSizeBinary(64), false),
        Field::new("tx_message_hash", DataType::FixedSizeBinary(32), false),

        // Sender config snapshot
        Field::new("endpoint_url", DataType::Utf8, false),
        Field::new("protocol", DataType::Utf8, false),
        Field::new("auth_tier", DataType::Utf8, true),
        Field::new("tip_account_used", DataType::FixedSizeBinary(32), true),
        Field::new("tip_lamports", DataType::UInt64, false),
        Field::new("priority_fee_microlamports", DataType::UInt64, false),
        Field::new("compute_unit_limit", DataType::UInt32, false),

        // Timestamps
        Field::new("prepared_at_ns", DataType::UInt64, false),
        Field::new("pool_ready_at_ns", DataType::UInt64, false),
        Field::new("trigger_observed_at_ns", DataType::UInt64, false),
        Field::new("send_at_ns", DataType::UInt64, false),
        Field::new("send_ack_at_ns", DataType::UInt64, true),
        Field::new("send_order_in_trigger", DataType::UInt8, false),
        Field::new("host_clock_offset_ns", DataType::Int64, true),

        // Send outcome (transport)
        Field::new("send_error", DataType::Utf8, true),
        Field::new("rpc_err_code", DataType::Int32, true),
        Field::new("rpc_err_message", DataType::Utf8, true),
        Field::new("provider_request_id", DataType::Utf8, true),
        Field::new("http_status", DataType::UInt16, true),
        Field::new("rate_limit_state", DataType::Utf8, false),

        // Observation
        Field::new("observed_slot", DataType::UInt64, true),
        Field::new("observed_entry_index", DataType::UInt32, true),
        Field::new("observed_tick_in_slot", DataType::UInt8, true),
        Field::new("observed_cumulative_hashes_in_slot", DataType::UInt64, true),
        Field::new("ss_observed_at_ns", DataType::UInt64, true),
        Field::new("ys_observed_at_ns", DataType::UInt64, true),
        Field::new("observed_at_ns", DataType::UInt64, true),
        Field::new("observed_source", DataType::Utf8, true),
        Field::new("commitment_at_resolution", DataType::Utf8, true),

        // Outcome
        Field::new("tentative_outcome", DataType::Utf8, false),
        Field::new("final_status", DataType::Utf8, false),
        Field::new("siblings_resolved_at_ns", DataType::UInt64, true),

        // Leader context
        Field::new("leader_pubkey", DataType::FixedSizeBinary(32), true),
        Field::new("leader_region_cc", DataType::Utf8, true),
        Field::new("leader_dc_label", DataType::Utf8, true),
        Field::new("leader_continent", DataType::Utf8, true),
        Field::new("leader_stake_lamports", DataType::UInt64, true),
        Field::new("validator_client", DataType::Utf8, true),

        // Deltas
        Field::new("tick_delta", DataType::Int32, true),
        Field::new("hash_delta", DataType::Int64, true),
        Field::new("slot_delta", DataType::Int32, true),
        Field::new("leader_changed", DataType::Boolean, false),
        Field::new("wall_trigger_to_send_ns", DataType::Int64, true),
        Field::new("wall_send_rtt_ns", DataType::Int64, true),
        Field::new("wall_send_to_observed_ns", DataType::Int64, true),
        Field::new("wall_send_to_ss_observed_ns", DataType::Int64, true),
        Field::new("wall_send_to_ys_observed_ns", DataType::Int64, true),

        // Nonce context
        Field::new("nonce_update_observed_at_ns", DataType::UInt64, true),
        Field::new("nonce_update_source", DataType::Utf8, true),
        Field::new("nonce_advanced_to_slot", DataType::UInt64, true),

        // Run metadata
        Field::new("run_id", DataType::Utf8, false),
        Field::new("chunk_index", DataType::UInt32, false),
    ];
    Arc::new(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_field_count_matches_spec() {
        let schema = final_record_schema();
        assert_eq!(schema.fields().len(), 61);
    }

    #[test]
    fn no_duplicate_field_names() {
        let schema = final_record_schema();
        let names: std::collections::HashSet<_> = schema.fields().iter().map(|f| f.name().clone()).collect();
        assert_eq!(names.len(), schema.fields().len());
    }
}
