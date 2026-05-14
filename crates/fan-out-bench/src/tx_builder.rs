//! Central tx composition for fan-out variants.
//!
//! ENFORCEMENT (spec §4.5): AdvanceNonce must be instruction[0], memo must be
//! ASCII printable byte, recent_blockhash must be the nonce_blockhash.
//!
//! All sender impls MUST go through this module. No instruction composition
//! anywhere else in the crate.

use crate::config::SenderKind;
use crate::memo;
use std::str::FromStr;
use solana_sdk::{
    hash::Hash,
    instruction::Instruction,
    message::Message,
    pubkey::Pubkey,
    signature::{Keypair, Signature},
    transaction::Transaction,
};
use solana_system_interface::instruction as sys_instruction;
use solana_compute_budget_interface::ComputeBudgetInstruction;

/// SPL Memo Program v3 ID (hardcoded to avoid spl-memo crate's incompatible Pubkey type).
const MEMO_PROGRAM_ID_STR: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";

fn memo_program_id() -> Pubkey {
    Pubkey::from_str(MEMO_PROGRAM_ID_STR).expect("MEMO_PROGRAM_ID_STR is a valid base58 pubkey")
}

/// System program ID is all-zero pubkey.
fn system_program_id() -> Pubkey {
    Pubkey::from([0u8; 32])
}

pub struct VariantParams {
    pub nonce_pubkey: Pubkey,
    pub nonce_blockhash: Hash,
    pub payer: Pubkey,
    pub sender_id: u8,
    pub sender_kind: SenderKind,
    pub tip_account: Option<Pubkey>,
    pub tip_lamports: u64,
    pub priority_fee_microlamports: u64,
    pub compute_unit_limit: u32,
}

pub struct VariantTx {
    pub tx: Transaction,
    pub signature: Signature,
    pub message_hash: [u8; 32],
}

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("memo encoding failed: {0}")]
    Memo(#[from] memo::MemoError),
    #[error("tip required for sender kind {0:?} but tip_account is None")]
    MissingTipAccount(SenderKind),
}

pub fn build_variant(p: VariantParams, signer: &Keypair) -> Result<VariantTx, BuildError> {
    let needs_tip = !matches!(p.sender_kind, SenderKind::Triton | SenderKind::Harmonic | SenderKind::Mock);
    if needs_tip && p.tip_account.is_none() {
        return Err(BuildError::MissingTipAccount(p.sender_kind));
    }

    let memo_bytes = memo::encode(p.sender_id)?;
    let mut ixs: Vec<Instruction> = Vec::with_capacity(6);

    ixs.push(sys_instruction::advance_nonce_account(&p.nonce_pubkey, &p.payer));
    ixs.push(sys_instruction::transfer(&p.payer, &p.payer, 1 + p.sender_id as u64));

    if let Some(tip_account) = p.tip_account {
        ixs.push(sys_instruction::transfer(&p.payer, &tip_account, p.tip_lamports));
    }

    ixs.push(ComputeBudgetInstruction::set_compute_unit_limit(p.compute_unit_limit));
    ixs.push(ComputeBudgetInstruction::set_compute_unit_price(p.priority_fee_microlamports));

    ixs.push(Instruction {
        program_id: memo_program_id(),
        accounts: vec![],
        data: memo_bytes.to_vec(),
    });

    let message = Message::new_with_blockhash(&ixs, Some(&p.payer), &p.nonce_blockhash);
    let mut tx = Transaction::new_unsigned(message);
    tx.sign(&[signer], p.nonce_blockhash);

    debug_assert_eq!(tx.message.recent_blockhash, p.nonce_blockhash);
    debug_assert!(is_advance_nonce_instruction(&tx.message, 0),
        "instruction[0] must be AdvanceNonceAccount, got {:?}",
        tx.message.instructions[0]);
    debug_assert_eq!(tx.signatures.len(), 1, "expected single signer");

    let signature = tx.signatures[0];

    use sha2::{Digest, Sha256};
    let serialized = tx.message.serialize();
    let mut hasher = Sha256::new();
    hasher.update(&serialized);
    let message_hash: [u8; 32] = hasher.finalize().into();

    Ok(VariantTx {
        tx,
        signature,
        message_hash,
    })
}

fn is_advance_nonce_instruction(msg: &Message, idx: usize) -> bool {
    if idx >= msg.instructions.len() {
        return false;
    }
    let ix = &msg.instructions[idx];
    let program_id = msg.account_keys[ix.program_id_index as usize];
    if program_id != system_program_id() {
        return false;
    }
    // SystemInstruction::AdvanceNonceAccount variant discriminator = 4 (LE u32)
    ix.data.len() >= 4 && ix.data[..4] == [4, 0, 0, 0]
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::signature::{Keypair, Signer};

    fn dummy_params(sender_kind: SenderKind, sender_id: u8, tip_account: Option<Pubkey>) -> (VariantParams, Keypair) {
        let signer = Keypair::new();
        let nonce_pubkey = Pubkey::new_unique();
        let nonce_blockhash = Hash::new_unique();
        let params = VariantParams {
            nonce_pubkey,
            nonce_blockhash,
            payer: signer.pubkey(),
            sender_id,
            sender_kind,
            tip_account,
            tip_lamports: 5000,
            priority_fee_microlamports: 5000,
            compute_unit_limit: 200_000,
        };
        (params, signer)
    }

    #[test]
    fn instruction_zero_is_advance_nonce() {
        let (p, signer) = dummy_params(SenderKind::Helius, 0, Some(Pubkey::new_unique()));
        let variant = build_variant(p, &signer).unwrap();
        assert!(is_advance_nonce_instruction(&variant.tx.message, 0));
    }

    #[test]
    fn recent_blockhash_is_nonce_blockhash() {
        let (p, signer) = dummy_params(SenderKind::Helius, 0, Some(Pubkey::new_unique()));
        let expected = p.nonce_blockhash;
        let variant = build_variant(p, &signer).unwrap();
        assert_eq!(variant.tx.message.recent_blockhash, expected);
    }

    #[test]
    fn memo_byte_is_ascii_safe() {
        for sender_id in 0..=93u8 {
            let (p, signer) = dummy_params(SenderKind::Helius, sender_id, Some(Pubkey::new_unique()));
            let variant = build_variant(p, &signer).unwrap();
            let last_ix = variant.tx.message.instructions.last().unwrap();
            assert_eq!(last_ix.data.len(), 1, "memo should be 1 byte");
            let byte = last_ix.data[0];
            assert!((b'!'..=b'~').contains(&byte), "memo byte {:#x} not ASCII printable", byte);
        }
    }

    #[test]
    fn triton_has_no_tip_instruction() {
        let (p, signer) = dummy_params(SenderKind::Triton, 5, None);
        let variant = build_variant(p, &signer).unwrap();
        assert_eq!(variant.tx.message.instructions.len(), 5);
    }

    #[test]
    fn helius_has_tip_instruction() {
        let (p, signer) = dummy_params(SenderKind::Helius, 5, Some(Pubkey::new_unique()));
        let variant = build_variant(p, &signer).unwrap();
        assert_eq!(variant.tx.message.instructions.len(), 6);
    }

    #[test]
    fn helius_without_tip_account_errors() {
        let (p, signer) = dummy_params(SenderKind::Helius, 5, None);
        assert!(matches!(build_variant(p, &signer), Err(BuildError::MissingTipAccount(_))));
    }

    #[test]
    fn variants_for_different_sender_ids_have_different_message_hashes() {
        let common_signer = Keypair::new();
        let nonce_pubkey = Pubkey::new_unique();
        let nonce_blockhash = Hash::new_unique();
        let tip_account = Pubkey::new_unique();
        let make = |sender_id: u8| -> [u8; 32] {
            let params = VariantParams {
                nonce_pubkey,
                nonce_blockhash,
                payer: common_signer.pubkey(),
                sender_id,
                sender_kind: SenderKind::Helius,
                tip_account: Some(tip_account),
                tip_lamports: 5000,
                priority_fee_microlamports: 5000,
                compute_unit_limit: 200_000,
            };
            build_variant(params, &common_signer).unwrap().message_hash
        };
        assert_ne!(make(0), make(1));
        assert_ne!(make(1), make(2));
    }

    #[test]
    fn variants_for_different_sender_ids_have_different_signatures() {
        let common_signer = Keypair::new();
        let nonce_pubkey = Pubkey::new_unique();
        let nonce_blockhash = Hash::new_unique();
        let tip_account = Pubkey::new_unique();
        let make = |sender_id: u8| -> Signature {
            let params = VariantParams {
                nonce_pubkey,
                nonce_blockhash,
                payer: common_signer.pubkey(),
                sender_id,
                sender_kind: SenderKind::Helius,
                tip_account: Some(tip_account),
                tip_lamports: 5000,
                priority_fee_microlamports: 5000,
                compute_unit_limit: 200_000,
            };
            build_variant(params, &common_signer).unwrap().signature
        };
        assert_ne!(make(0), make(1));
        assert_ne!(make(1), make(2));
    }

    #[test]
    fn rejects_sender_id_over_93() {
        let (mut p, signer) = dummy_params(SenderKind::Helius, 94, Some(Pubkey::new_unique()));
        p.sender_id = 94;
        assert!(matches!(build_variant(p, &signer), Err(BuildError::Memo(_))));
    }
}
