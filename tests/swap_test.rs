use alloy::primitives::{U256, address};
use tx_cutoff::config::SwapConfig;
use tx_cutoff::swap::{PingPongState, SwapDirection, SwapEncoder};

fn cfg() -> SwapConfig {
    SwapConfig {
        router_address: "0x2626664c2603336E57B271c5C0b26F421741e481".into(),
        pool_fee_tier: 500,
        token_a: "0x4200000000000000000000000000000000000006".into(),
        token_b: "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913".into(),
        amount_in_a: "1000000000000000".into(),
        amount_in_b: "3000000".into(),
        slippage_bps: 300,
    }
}

#[test]
fn ping_pong_starts_a_to_b_when_balance_a_nonzero() {
    let s = PingPongState::initialize(U256::from(1_000_000u64), U256::ZERO);
    assert_eq!(s.current_direction(), SwapDirection::AtoB);
}

#[test]
fn ping_pong_starts_b_to_a_when_only_b_has_balance() {
    let s = PingPongState::initialize(U256::ZERO, U256::from(1_000_000u64));
    assert_eq!(s.current_direction(), SwapDirection::BtoA);
}

#[test]
fn ping_pong_flips_direction_each_tick() {
    let s = PingPongState::initialize(U256::from(1u64), U256::ZERO);
    assert_eq!(s.current_direction(), SwapDirection::AtoB);
    s.advance();
    assert_eq!(s.current_direction(), SwapDirection::BtoA);
    s.advance();
    assert_eq!(s.current_direction(), SwapDirection::AtoB);
}

#[test]
fn encoder_produces_exact_input_single_selector() {
    let enc =
        SwapEncoder::new(&cfg(), address!("0000000000000000000000000000000000000001")).unwrap();
    let data = enc.encode(SwapDirection::AtoB, 0).unwrap();
    // exactInputSingle selector
    assert_eq!(&data[0..4], &[0x04, 0xe4, 0x5a, 0xaf]);
}

#[test]
fn encoder_produces_different_calldata_for_each_direction() {
    let enc =
        SwapEncoder::new(&cfg(), address!("0000000000000000000000000000000000000001")).unwrap();
    let a_to_b = enc.encode(SwapDirection::AtoB, 0).unwrap();
    let b_to_a = enc.encode(SwapDirection::BtoA, 0).unwrap();
    assert_ne!(a_to_b, b_to_a);
    assert_eq!(a_to_b.len(), b_to_a.len());
}
