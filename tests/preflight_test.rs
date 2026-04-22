use alloy::primitives::U256;
use tx_cutoff::preflight::{WalletGas, project_cost};

#[test]
fn cost_projection_computes_realistic_and_worst() {
    let wallets = vec![
        WalletGas {
            label: "w1".into(),
            gas_used: 140_000,
        },
        WalletGas {
            label: "w2".into(),
            gas_used: 150_000,
        },
    ];
    // baseFee = 1 gwei, tip = 0.01 gwei, multiplier = 3
    let proj = project_cost(&wallets, 1_000_000_000u128, 10_000_000u128, 3.0, 1000);
    assert!(proj.per_wallet_realistic_wei[0] > U256::ZERO);
    assert!(proj.per_wallet_worst_wei[0] > proj.per_wallet_realistic_wei[0]);
    assert!(proj.total_realistic_wei < proj.total_worst_wei);
    // Sanity: total = sum of per-wallet
    let sum_real = proj
        .per_wallet_realistic_wei
        .iter()
        .fold(U256::ZERO, |acc, v| acc + *v);
    assert_eq!(sum_real, proj.total_realistic_wei);
}
