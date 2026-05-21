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
//! Memo encoding: a single printable ASCII byte derived from `sender_id`
//! (`b'!' + sender_id`, range 33..=126). Lets us identify which sender's
//! variant landed by reading the memo data of the on-chain entry.

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

/// Encode a sender id as a printable ASCII byte for the memo program.
/// Range: 33..=126 (94 values), so sender ids must be in 0..94.
pub fn memo_byte_for_sender(sender_id: u8) -> u8 {
    // saturating: clamp into the printable range so we never produce
    // an unprintable / control character.
    let offset = (sender_id as u32).min(93) as u8;
    b'!' + offset
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
    // Memo: 1 byte sender_id + 8 bytes trigger_id (LE u64).
    // Per-trigger uniqueness is critical when sharing a recent blockhash
    // across multiple triggers — otherwise identical instruction content
    // produces identical signed txs that on-chain dedupes as "already
    // processed", causing silent second-trigger drops.
    let mut memo_data = Vec::with_capacity(9);
    memo_data.push(memo_byte_for_sender(params.sender_id));
    memo_data.extend_from_slice(&params.trigger_id.to_le_bytes());
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

    let message = Message::new(&ixs, Some(&payer_pk));
    let mut tx = Transaction::new_unsigned(message);
    tx.sign(&[params.payer], params.blockhash);
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
    fn memo_byte_in_printable_range() {
        for id in 0u8..=255u8 {
            let b = memo_byte_for_sender(id);
            assert!(
                (33..=126).contains(&b),
                "sender_id {} memo byte {} out of printable ASCII",
                id, b
            );
        }
    }

    #[test]
    fn distinct_sender_ids_get_distinct_bytes_until_clamp() {
        // 0..=93 should map to distinct bytes (94 values).
        let mut seen = std::collections::HashSet::new();
        for id in 0u8..=93u8 {
            assert!(seen.insert(memo_byte_for_sender(id)));
        }
        // 94 and above clamp to the same final byte.
        assert_eq!(memo_byte_for_sender(94), memo_byte_for_sender(255));
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
            tip_account: None, tip_lamports: 0, nonce: None, tx_cfg: &cfg,
        });
        let b = build(BuildParams {
            payer: &payer, blockhash: bh, sender_id: 0, trigger_id: 2,
            tip_account: None, tip_lamports: 0, nonce: None, tx_cfg: &cfg,
        });
        assert_ne!(a.signature, b.signature);
    }
}
