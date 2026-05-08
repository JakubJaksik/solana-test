//! Wallet: key load, nonce cache (atomic), EIP-1559 signing.

use crate::config::WalletConfig;
use alloy::consensus::{SignableTransaction, TxEip1559};
use alloy::eips::eip2718::Encodable2718;
use alloy::network::TxSignerSync;
use alloy::primitives::{Address, B256, Bytes, TxKind, U256};
use alloy::signers::local::PrivateKeySigner;
use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum WalletError {
    #[error("invalid private key: {0}")]
    InvalidKey(String),
    #[error("signing failed: {0}")]
    Sign(String),
}

#[derive(Debug)]
pub struct Wallet {
    label: String,
    signer: PrivateKeySigner,
    address: Address,
    nonce: AtomicU64,
}

#[derive(Debug, Clone)]
pub struct TxParams {
    pub chain_id: u64,
    pub nonce: u64,
    pub to: Address,
    pub value: U256,
    pub data: Vec<u8>,
    pub gas_limit: u64,
    pub max_priority_fee_per_gas: u128,
    pub max_fee_per_gas: u128,
}

#[derive(Debug, Clone)]
pub struct SignedTx {
    pub tx_hash: B256,
    pub raw: Bytes,
}

impl Wallet {
    pub fn from_config(cfg: &WalletConfig) -> Result<Self, WalletError> {
        let signer: PrivateKeySigner =
            cfg.private_key
                .parse()
                .map_err(|e: alloy::signers::local::LocalSignerError| {
                    WalletError::InvalidKey(format!("{e:?}"))
                })?;
        let address = signer.address();
        Ok(Self {
            label: cfg.label.clone(),
            signer,
            address,
            nonce: AtomicU64::new(0),
        })
    }

    pub fn label(&self) -> &str {
        &self.label
    }

    pub fn address(&self) -> Address {
        self.address
    }

    pub fn set_nonce(&self, n: u64) {
        self.nonce.store(n, Ordering::SeqCst);
    }

    pub fn next_nonce(&self) -> u64 {
        self.nonce.load(Ordering::SeqCst)
    }

    pub fn consume_nonce(&self) -> u64 {
        self.nonce.fetch_add(1, Ordering::SeqCst)
    }

    pub fn sign_eip1559(&self, params: TxParams) -> Result<SignedTx, WalletError> {
        let mut tx = TxEip1559 {
            chain_id: params.chain_id,
            nonce: params.nonce,
            gas_limit: params.gas_limit,
            max_fee_per_gas: params.max_fee_per_gas,
            max_priority_fee_per_gas: params.max_priority_fee_per_gas,
            to: TxKind::Call(params.to),
            value: params.value,
            input: Bytes::from(params.data),
            access_list: Default::default(),
        };
        let signature = self
            .signer
            .sign_transaction_sync(&mut tx)
            .map_err(|e| WalletError::Sign(e.to_string()))?;
        let signed = tx.into_signed(signature);
        let tx_hash = *signed.hash();
        let mut raw = Vec::with_capacity(200);
        signed.encode_2718(&mut raw);
        Ok(SignedTx {
            tx_hash,
            raw: Bytes::from(raw),
        })
    }
}
