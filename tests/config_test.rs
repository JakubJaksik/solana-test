use std::io::Write;
use tempfile::NamedTempFile;
use tx_cutoff::config::{Config, ConfigError};

fn write_tmp(json: &str) -> NamedTempFile {
    let mut f = NamedTempFile::new().unwrap();
    f.write_all(json.as_bytes()).unwrap();
    f
}

fn valid_json() -> &'static str {
    r#"{
      "chain": {
        "name": "base",
        "chain_id": 8453,
        "rpc_ws": "wss://example/ws",
        "rpc_http": "https://example/http"
      },
      "timing": {
        "start_ms": 8500,
        "end_ms": 11500,
        "step_ms": 50,
        "samples_per_wallet_per_slot": 20
      },
      "gas": {
        "max_priority_fee_gwei": 0.01,
        "max_fee_multiplier": 3.0
      },
      "tracking": {
        "inclusion_lookahead_blocks": 10,
        "abort_on_consecutive_failed_blocks": 5
      },
      "swap": {
        "router_address": "0x2626664c2603336E57B271c5C0b26F421741e481",
        "pool_fee_tier": 500,
        "token_a": "0x4200000000000000000000000000000000000006",
        "token_b": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        "amount_in_a": "1000000000000000",
        "amount_in_b": "3000000",
        "slippage_bps": 300
      },
      "wallets": [
        {"label": "w1", "private_key": "0x0000000000000000000000000000000000000000000000000000000000000001"},
        {"label": "w2", "private_key": "0x0000000000000000000000000000000000000000000000000000000000000002"}
      ],
      "output": {
        "dir": "./runs",
        "stdout_report": true
      }
    }"#
}

#[test]
fn valid_config_parses_cleanly() {
    let f = write_tmp(valid_json());
    let c = Config::load(f.path()).unwrap();
    assert_eq!(c.chain.chain_id, 8453);
    assert_eq!(c.timing.start_ms, 8500);
    assert_eq!(c.wallets.len(), 2);
    assert_eq!(c.swap.pool_fee_tier, 500);
}

#[test]
fn send_defaults_are_applied_when_missing() {
    let f = write_tmp(valid_json());
    let c = Config::load(f.path()).unwrap();
    assert_eq!(c.send.resolved_worker_threads(c.wallets.len()), 2); // = wallets.len()
    assert_eq!(c.send.resolved_spin_window_us(), 2000);
}

#[test]
fn validation_rejects_empty_wallets() {
    let empty = valid_json().replace(
        r#""wallets": [
        {"label": "w1", "private_key": "0x0000000000000000000000000000000000000000000000000000000000000001"},
        {"label": "w2", "private_key": "0x0000000000000000000000000000000000000000000000000000000000000002"}
      ],"#,
        r#""wallets": [],"#,
    );
    let f = write_tmp(&empty);
    let err = Config::load(f.path()).unwrap_err();
    assert!(matches!(err, ConfigError::Validation(_)));
}

#[test]
fn validation_rejects_duplicate_wallet_labels() {
    let dup = valid_json().replace(r#""label": "w2""#, r#""label": "w1""#);
    let f = write_tmp(&dup);
    let err = Config::load(f.path()).unwrap_err();
    assert!(matches!(err, ConfigError::Validation(_)));
}

#[test]
fn validation_rejects_invalid_timing() {
    let bad = valid_json().replace(r#""start_ms": 8500"#, r#""start_ms": 12000"#);
    let f = write_tmp(&bad);
    let err = Config::load(f.path()).unwrap_err();
    assert!(matches!(err, ConfigError::Validation(_)));
}

#[test]
fn validation_rejects_invalid_lookahead() {
    let bad = valid_json().replace(
        r#""inclusion_lookahead_blocks": 10"#,
        r#""inclusion_lookahead_blocks": 1"#,
    );
    let f = write_tmp(&bad);
    let err = Config::load(f.path()).unwrap_err();
    assert!(matches!(err, ConfigError::Validation(_)));
}

#[test]
fn snapshot_redacts_private_keys() {
    let f = write_tmp(valid_json());
    let c = Config::load(f.path()).unwrap();
    let snap = serde_json::to_string(&c.to_snapshot()).unwrap();
    assert!(!snap.contains("0x0000000000000000000000000000000000000000000000000000000000000001"));
    assert!(snap.contains("redacted") || snap.contains("***"));
}

#[test]
fn missing_file_returns_io_error() {
    let err = Config::load("/nonexistent/path.json").unwrap_err();
    assert!(matches!(err, ConfigError::Io(_)));
}

#[test]
fn malformed_json_returns_parse_error() {
    let f = write_tmp("{ invalid");
    let err = Config::load(f.path()).unwrap_err();
    assert!(matches!(err, ConfigError::Parse(_)));
}
