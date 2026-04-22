use alloy::primitives::{U256, address};
use tx_cutoff::config::WalletConfig;
use tx_cutoff::wallet::{Wallet, WalletError};

fn sample_wallet() -> WalletConfig {
    // Deterministic test key. Address derived from priv_key=1:
    // 0x7e5f4552091a69125d5dfcb7b8c2659029395bdf
    WalletConfig {
        label: "test".into(),
        private_key: "0x0000000000000000000000000000000000000000000000000000000000000001".into(),
    }
}

#[test]
fn wallet_loads_valid_key_and_derives_address() {
    let w = Wallet::from_config(&sample_wallet()).unwrap();
    assert_eq!(
        w.address(),
        address!("7e5f4552091a69125d5dfcb7b8c2659029395bdf")
    );
    assert_eq!(w.label(), "test");
}

#[test]
fn wallet_rejects_invalid_key() {
    let bad = WalletConfig {
        label: "bad".into(),
        private_key: "0xzz".into(),
    };
    let err = Wallet::from_config(&bad).unwrap_err();
    assert!(matches!(err, WalletError::InvalidKey(_)));
}

#[test]
fn nonce_cache_starts_at_init_value_and_increments() {
    let w = Wallet::from_config(&sample_wallet()).unwrap();
    w.set_nonce(100);
    assert_eq!(w.next_nonce(), 100);
    assert_eq!(w.consume_nonce(), 100);
    assert_eq!(w.next_nonce(), 101);
    assert_eq!(w.consume_nonce(), 101);
    assert_eq!(w.next_nonce(), 102);
}

#[test]
fn sign_eip1559_returns_signed_tx() {
    let w = Wallet::from_config(&sample_wallet()).unwrap();
    w.set_nonce(5);
    let signed = w
        .sign_eip1559(tx_cutoff::wallet::TxParams {
            chain_id: 8453,
            nonce: 5,
            to: address!("2626664c2603336e57b271c5c0b26f421741e481"),
            value: U256::ZERO,
            data: vec![0x01, 0x02, 0x03],
            gas_limit: 150_000,
            max_priority_fee_per_gas: 10_000_000,
            max_fee_per_gas: 1_000_000_000,
        })
        .unwrap();
    // Hash format: 0x + 64 hex chars
    assert_eq!(signed.tx_hash.to_string().len(), 66);
    assert!(!signed.raw.is_empty());
    // EIP-1559 tx envelope starts with 0x02 byte after signing
    assert_eq!(signed.raw[0], 0x02);
}
