//! Yellowstone gRPC subscription for nonce account updates.

use crate::nonce::manager::NonceManager;
use crate::nonce::state::parse_nonce_account_data;
use anyhow::{Context, Result};
use futures_util::StreamExt;
use std::sync::Arc;
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::geyser::{
    subscribe_update::UpdateOneof, SubscribeRequest, SubscribeRequestFilterAccounts,
};

pub struct GeyserConfig {
    pub endpoint: String,
    pub auth_token: Option<String>,
    pub manager: Arc<NonceManager>,
    pub stop: Arc<std::sync::atomic::AtomicBool>,
}

pub async fn run(cfg: GeyserConfig) -> Result<()> {
    let mut client = GeyserGrpcClient::build_from_shared(cfg.endpoint.clone())?
        .x_token(cfg.auth_token.clone())?
        .connect()
        .await
        .context("yellowstone connect")?;

    let pubkey_strs: Vec<String> = cfg
        .manager
        .entries()
        .iter()
        .map(|e| e.pubkey.to_string())
        .collect();

    let mut accounts_filter = std::collections::HashMap::new();
    accounts_filter.insert(
        "nonces".to_string(),
        SubscribeRequestFilterAccounts {
            account: pubkey_strs,
            owner: vec![],
            filters: vec![],
            nonempty_txn_signature: None,
        },
    );

    let req = SubscribeRequest {
        accounts: accounts_filter,
        slots: Default::default(),
        transactions: Default::default(),
        transactions_status: Default::default(),
        blocks: Default::default(),
        blocks_meta: Default::default(),
        entry: Default::default(),
        commitment: Some(yellowstone_grpc_proto::geyser::CommitmentLevel::Processed as i32),
        accounts_data_slice: vec![],
        ping: None,
        from_slot: None,
    };

    let (_subscribe_tx, mut stream) = client.subscribe_with_request(Some(req)).await?;
    tracing::info!(count = cfg.manager.len(), "yellowstone nonce subscription active");

    while !cfg.stop.load(std::sync::atomic::Ordering::Relaxed) {
        let msg = match stream.next().await {
            Some(Ok(m)) => m,
            Some(Err(e)) => {
                tracing::error!(error = %e, "yellowstone stream error");
                return Err(e.into());
            }
            None => {
                tracing::warn!("yellowstone stream ended");
                break;
            }
        };
        if let Some(UpdateOneof::Account(acc_upd)) = msg.update_oneof {
            if let Some(acc) = acc_upd.account {
                let pubkey_bytes: [u8; 32] = match acc.pubkey.as_slice().try_into() {
                    Ok(b) => b,
                    Err(_) => {
                        tracing::warn!("YS account update has wrong pubkey length");
                        continue;
                    }
                };
                let pubkey = solana_sdk::pubkey::Pubkey::from(pubkey_bytes);
                match parse_nonce_account_data(&acc.data) {
                    Ok(state) => {
                        cfg.manager.on_account_update(&pubkey, state.blockhash);
                    }
                    Err(e) => {
                        tracing::warn!(pubkey = %pubkey, error = %e, "failed to parse nonce account update");
                    }
                }
            }
        }
    }

    Ok(())
}
