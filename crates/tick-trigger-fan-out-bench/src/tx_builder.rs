//! Tx builder for phase 3+ benches.
//!
//! Builds a minimal self-transfer transaction with:
//!   - ComputeBudget::SetComputeUnitLimit
//!   - ComputeBudget::SetComputeUnitPrice (priority fee)
//!   - Memo program ix with a 1-byte sender_id (for landing attribution)
//!   - SystemProgram::transfer (payer → payer, `self_transfer_lamports`)
//!   - Optional tip transfer (payer → tip_account, `tip_lamports`)
//!
//! Designed to be cheap to build — the only allocation on the hot path
//! is the `Transaction` itself. No RPC calls, no I/O.
//!
//! Memo encoding: ASCII hex string `"{sender_id:02x}:{trigger_id:016x}"`
//! (19 bytes, valid UTF-8). The SPL Memo program rejects non-UTF-8 input
//! with `Invalid Instruction Data`, which fails the whole tx atomically
//! (nonce does NOT advance, tip is NOT paid, only base fee is charged).

use crate::config::TxConfig;
use solana_compute_budget_interface::ComputeBudgetInstruction;
use solana_sdk::hash::Hash;
use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::message::Message;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signature};
use solana_sdk::signer::Signer;
use solana_sdk::transaction::Transaction;
use solana_system_interface::instruction as system_instruction;

const MEMO_PROGRAM_ID_STR: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";

/// Rent-exempt minimum for a System-owned account holding 0 bytes of data.
/// Stable on mainnet as of 2026-05; if Solana changes the rent schedule
/// this must be updated. Used by the Jito bundle preparer for throwaway
/// tipper accounts.
pub const RENT_EXEMPT_MIN_LAMPORTS: u64 = 890_880;

/// Standard signature fee. One signature → one `BASE_TX_FEE_LAMPORTS`.
pub const BASE_TX_FEE_LAMPORTS: u64 = 5_000;

/// Solana SPL Memo v2 program id, parsed once.
fn memo_program_id() -> Pubkey {
    use std::sync::OnceLock;
    static ID: OnceLock<Pubkey> = OnceLock::new();
    *ID.get_or_init(|| {
        MEMO_PROGRAM_ID_STR
            .parse()
            .expect("hardcoded memo program id parses")
    })
}

/// Build the memo payload as a 19-byte ASCII hex string:
/// `"{sender_id:02x}:{trigger_id:016x}"`. Valid UTF-8, unique per
/// (sender_id, trigger_id), and human-readable on explorers.
pub fn memo_payload(sender_id: u8, trigger_id: u64) -> String {
    format!("{:02x}:{:016x}", sender_id, trigger_id)
}

pub struct BuildParams<'a> {
    pub payer: &'a Keypair,
    pub blockhash: Hash,
    pub sender_id: u8,
    /// Per-trigger unique value. Embedded into the memo so two triggers
    /// within the same blockhash window produce DIFFERENT signed txs,
    /// avoiding "already processed" silent drops on chain.
    pub trigger_id: u64,
    pub tip_account: Option<Pubkey>,
    pub tip_lamports: u64,
    /// Durable nonce mode. When `Some((nonce_pubkey, _))`, the tx is signed
    /// with `recent_blockhash = blockhash` (the nonce account's current
    /// stored hash) AND prepends an `AdvanceNonceAccount` instruction
    /// referencing `nonce_pubkey`. The nonce account is advanced on chain
    /// to `sha256("DURABLE_NONCE" || recent_blockhash)`.
    /// When `None`, the standard fresh-blockhash mode is used.
    pub nonce: Option<NonceParams>,
    pub tx_cfg: &'a TxConfig,
    /// When `Some((target, lamports))`, append an extra
    /// `system_instruction::transfer(payer → target, lamports)` to the tx.
    /// Used by the Jito bundle preparer to fund a throwaway tipper keypair.
    pub fund_tipper: Option<(Pubkey, u64)>,
}

#[derive(Clone, Copy)]
pub struct NonceParams {
    pub nonce_pubkey: Pubkey,
    pub authority: Pubkey,
}

pub struct BuiltTx {
    pub tx: Transaction,
    pub signature: Signature,
}

pub fn build(params: BuildParams<'_>) -> BuiltTx {
    let payer_pk = params.payer.pubkey();

    let mut ixs: Vec<Instruction> = Vec::with_capacity(6);
    // Durable nonce mode: `AdvanceNonceAccount` MUST be the first instruction
    // for the bank to recognise the tx as a nonced tx and use
    // `nonce_account.durable_nonce` as the validation blockhash.
    if let Some(np) = params.nonce {
        ixs.push(solana_system_interface::instruction::advance_nonce_account(
            &np.nonce_pubkey,
            &np.authority,
        ));
    }
    ixs.push(ComputeBudgetInstruction::set_compute_unit_limit(
        params.tx_cfg.compute_unit_limit,
    ));
    if params.tx_cfg.priority_fee_microlamports > 0 {
        ixs.push(ComputeBudgetInstruction::set_compute_unit_price(
            params.tx_cfg.priority_fee_microlamports,
        ));
    }
    // Memo: ASCII hex "ss:tttttttttttttttt" — UTF-8 required by SPL Memo.
    // Per-trigger uniqueness via trigger_id prevents on-chain "already
    // processed" silent drops when triggers share a blockhash window.
    let memo_data = memo_payload(params.sender_id, params.trigger_id).into_bytes();
    ixs.push(Instruction {
        program_id: memo_program_id(),
        accounts: vec![AccountMeta::new_readonly(payer_pk, true)],
        data: memo_data,
    });
    // Tip transfer if configured.
    if let Some(tip) = params.tip_account {
        if params.tip_lamports > 0 {
            ixs.push(system_instruction::transfer(
                &payer_pk,
                &tip,
                params.tip_lamports,
            ));
        }
    }
    // Self-transfer keeps the tx a "real" tx with non-trivial system_program use.
    ixs.push(system_instruction::transfer(
        &payer_pk,
        &payer_pk,
        params.tx_cfg.self_transfer_lamports,
    ));

    // Optional: fund a throwaway tipper account in the same tx (Jito bundle).
    if let Some((tipper, amount)) = params.fund_tipper {
        ixs.push(system_instruction::transfer(
            &payer_pk,
            &tipper,
            amount,
        ));
    }

    let message = Message::new(&ixs, Some(&payer_pk));
    let mut tx = Transaction::new_unsigned(message);
    tx.sign(&[params.payer], params.blockhash);
    let signature = tx.signatures[0];
    BuiltTx { tx, signature }
}

/// Build Tx2 of a Jito bundle. Signed by a throwaway `tipper` keypair:
/// 1) transfers `tip_lamports` to `tip_account`,
/// 2) transfers `rent_exempt_lamports` back to `main_wallet` (send-back).
///
/// Caller funds the tipper with at least `tip_lamports + rent_exempt + base_fee`
/// in Tx1 via `BuildParams.fund_tipper`. After Tx2 executes, the tipper holds
/// 0 lamports and gets GC'd by the Solana epoch cleanup.
pub fn build_tipper_tx(
    tipper: &Keypair,
    blockhash: Hash,
    tip_account: Pubkey,
    tip_lamports: u64,
    main_wallet: Pubkey,
    rent_exempt_lamports: u64,
) -> BuiltTx {
    let tipper_pk = tipper.pubkey();
    let ixs = vec![
        system_instruction::transfer(&tipper_pk, &tip_account, tip_lamports),
        system_instruction::transfer(&tipper_pk, &main_wallet, rent_exempt_lamports),
    ];
    let message = Message::new(&ixs, Some(&tipper_pk));
    let mut tx = Transaction::new_unsigned(message);
    tx.sign(&[tipper], blockhash);
    let signature = tx.signatures[0];
    BuiltTx { tx, signature }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> TxConfig {
        TxConfig {
            self_transfer_lamports: 1,
            priority_fee_microlamports: 5000,
            compute_unit_limit: 200_000,
        }
    }

    #[test]
    fn memo_payload_is_valid_utf8_and_19_bytes() {
        for sender in [0u8, 1, 7, 42, 255] {
            for trigger in [0u64, 1, 0xDEAD_BEEF, u64::MAX] {
                let s = memo_payload(sender, trigger);
                assert_eq!(s.len(), 19);
                assert!(s.is_ascii());
                // round-trip parse to confirm format
                let (l, r) = s.split_once(':').unwrap();
                assert_eq!(u8::from_str_radix(l, 16).unwrap(), sender);
                assert_eq!(u64::from_str_radix(r, 16).unwrap(), trigger);
            }
        }
    }

    #[test]
    fn distinct_inputs_produce_distinct_memo_payloads() {
        let mut seen = std::collections::HashSet::new();
        for sender in 0u8..16 {
            for trigger in 0u64..16 {
                assert!(seen.insert(memo_payload(sender, trigger)));
            }
        }
    }

    #[test]
    fn build_produces_signed_tx_with_expected_ix_count() {
        let payer = Keypair::new();
        let tip = Pubkey::new_unique();
        let cfg = cfg();
        let bh = Hash::new_unique();
        let built = build(BuildParams {
            payer: &payer,
            blockhash: bh,
            sender_id: 0,
            trigger_id: 12345,
            tip_account: Some(tip),
            tip_lamports: 1000,
            nonce: None,
            tx_cfg: &cfg,
            fund_tipper: None,
        });
        // 5 ixs: SetCULimit, SetCUPrice, Memo, Tip transfer, Self transfer.
        assert_eq!(built.tx.message.instructions.len(), 5);
        assert_eq!(built.tx.signatures.len(), 1);
        assert_ne!(built.signature, Signature::default());
    }

    #[test]
    fn build_skips_tip_transfer_when_none() {
        let payer = Keypair::new();
        let cfg = cfg();
        let built = build(BuildParams {
            payer: &payer,
            blockhash: Hash::new_unique(),
            sender_id: 0,
            trigger_id: 12345,
            tip_account: None,
            tip_lamports: 0,
            nonce: None,
            tx_cfg: &cfg,
            fund_tipper: None,
        });
        // 4 ixs (no tip): SetCULimit, SetCUPrice, Memo, Self transfer.
        assert_eq!(built.tx.message.instructions.len(), 4);
    }

    #[test]
    fn build_skips_priority_fee_when_zero() {
        let payer = Keypair::new();
        let mut cfg = cfg();
        cfg.priority_fee_microlamports = 0;
        let built = build(BuildParams {
            payer: &payer,
            blockhash: Hash::new_unique(),
            sender_id: 0,
            trigger_id: 12345,
            tip_account: None,
            tip_lamports: 0,
            nonce: None,
            tx_cfg: &cfg,
            fund_tipper: None,
        });
        // 3 ixs (no priority fee, no tip): SetCULimit, Memo, Self transfer.
        assert_eq!(built.tx.message.instructions.len(), 3);
    }

    #[test]
    fn distinct_trigger_ids_produce_distinct_signatures() {
        // Same blockhash + sender — different trigger_id MUST yield different
        // signed tx (this is the property that prevents on-chain "already
        // processed" silent drops when triggers share a blockhash window).
        let payer = Keypair::new();
        let cfg = cfg();
        let bh = Hash::new_unique();
        let a = build(BuildParams {
            payer: &payer, blockhash: bh, sender_id: 0, trigger_id: 1,
            tip_account: None, tip_lamports: 0, nonce: None, tx_cfg: &cfg, fund_tipper: None,
        });
        let b = build(BuildParams {
            payer: &payer, blockhash: bh, sender_id: 0, trigger_id: 2,
            tip_account: None, tip_lamports: 0, nonce: None, tx_cfg: &cfg, fund_tipper: None,
        });
        assert_ne!(a.signature, b.signature);
    }

    #[test]
    fn build_includes_fund_tipper_transfer_when_set() {
        let payer = Keypair::new();
        let tipper_pk = Pubkey::new_unique();
        let cfg = cfg();
        let built = build(BuildParams {
            payer: &payer,
            blockhash: Hash::new_unique(),
            sender_id: 0,
            trigger_id: 123,
            tip_account: None,
            tip_lamports: 0,
            nonce: None,
            tx_cfg: &cfg,
            fund_tipper: Some((tipper_pk, 1_000_000)),
        });
        // 4 base ixs (SetCULimit, SetCUPrice, Memo, Self transfer) + 1 fund_tipper.
        assert_eq!(built.tx.message.instructions.len(), 5);
        let keys: Vec<_> = built.tx.message.account_keys.iter().collect();
        assert!(
            keys.iter().any(|k| **k == tipper_pk),
            "tipper pubkey not found in account keys"
        );
    }

    #[test]
    fn build_tipper_tx_has_two_transfer_ixs_and_is_signed_by_tipper() {
        let tipper = Keypair::new();
        let main = Pubkey::new_unique();
        let tip_acc = Pubkey::new_unique();
        let bh = Hash::new_unique();
        let built = build_tipper_tx(&tipper, bh, tip_acc, 50_000, main, RENT_EXEMPT_MIN_LAMPORTS);
        assert_eq!(built.tx.message.instructions.len(), 2);
        assert_eq!(built.tx.signatures.len(), 1);
        assert_ne!(built.signature, Signature::default());
        assert_eq!(built.tx.message.account_keys[0], tipper.pubkey());
    }
}
