use std::sync::Arc;
use std::time::Instant;

use crossbeam_channel::{bounded, Receiver, Sender};
use futures_util::StreamExt;
use solana_sdk::hash::Hash;
use tracing::{error, info};
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient};
use yellowstone_grpc_proto::geyser::{
    subscribe_update::UpdateOneof, SubscribeRequest, SubscribeRequestFilterEntry,
};

use crate::counters::DropCounters;
use crate::observation::{EntryObservation, SignatureVec, SourceKind};
use crate::source::EntrySource;

pub struct YellowstoneSource {
    pub url: String,
    pub token: Option<String>,
    pub channel_capacity: usize,
    pub pinned_core: Option<usize>,
    pub counters: Arc<DropCounters>,
}

impl EntrySource for YellowstoneSource {
    fn kind(&self) -> SourceKind {
        SourceKind::Yellowstone
    }

    fn start(self: Box<Self>) -> anyhow::Result<Receiver<EntryObservation>> {
        let (tx, rx) = bounded::<EntryObservation>(self.channel_capacity);
        let url = self.url.clone();
        let token = self.token.clone();
        let pinned = self.pinned_core;
        let counters = self.counters.clone();

        std::thread::Builder::new()
            .name("ys-rx".into())
            .spawn(move || {
                if let Some(core) = pinned {
                    core_affinity::set_for_current(core_affinity::CoreId { id: core });
                }
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("build current_thread runtime");
                rt.block_on(run_loop(url, token, tx, counters));
            })?;

        Ok(rx)
    }
}

async fn run_loop(
    url: String,
    token: Option<String>,
    tx: Sender<EntryObservation>,
    counters: Arc<DropCounters>,
) {
    let mut backoff_ms = 100u64;
    loop {
        match run_once(&url, token.as_deref(), &tx, &counters).await {
            Ok(()) => {
                info!("yellowstone stream closed cleanly, reconnecting");
            }
            Err(e) => {
                counters.inc(&counters.ys_reconnects);
                error!(error = %e, "yellowstone stream error, reconnecting");
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
        backoff_ms = (backoff_ms * 2).min(5_000);
    }
}

async fn run_once(
    url: &str,
    token: Option<&str>,
    tx: &Sender<EntryObservation>,
    counters: &DropCounters,
) -> anyhow::Result<()> {
    let use_tls = url.starts_with("https://");

    let mut builder = GeyserGrpcClient::build_from_shared(url.to_string())?;
    if use_tls {
        builder = builder.tls_config(ClientTlsConfig::new().with_native_roots())?;
    }
    if let Some(t) = token {
        builder = builder.x_token(Some(t.to_string()))?;
    }
    let mut client = builder.connect().await?;

    let mut request = SubscribeRequest::default();
    request
        .entry
        .insert("all".into(), SubscribeRequestFilterEntry {});

    let (_sender, mut stream) = client.subscribe_with_request(Some(request)).await?;
    info!("yellowstone entry subscription open");

    while let Some(message) = stream.next().await {
        let observed_at = Instant::now();
        let msg = message?;
        if let Some(UpdateOneof::Entry(entry)) = msg.update_oneof {
            let obs = decode_entry(entry, observed_at);
            if tx.try_send(obs).is_err() {
                counters.inc(&counters.ys_channel_full);
            }
        }
    }
    Ok(())
}

#[inline]
fn decode_entry(
    e: yellowstone_grpc_proto::geyser::SubscribeUpdateEntry,
    observed_at: Instant,
) -> EntryObservation {
    // SubscribeUpdateEntry proto v12 fields:
    //   slot, index, num_hashes, hash (bytes), executed_transaction_count, starting_transaction_index
    // No transactions array — signatures list is empty for Yellowstone source.
    let entry_hash = match <[u8; 32]>::try_from(e.hash.as_slice()) {
        Ok(arr) => Hash::new_from_array(arr),
        Err(_) => Hash::default(),
    };

    // Proto v12 SubscribeUpdateEntry does NOT expose raw transaction bytes,
    // so we cannot extract signatures at this layer.
    let signatures = SignatureVec::new();

    EntryObservation {
        source: SourceKind::Yellowstone,
        observed_at,
        slot: e.slot,
        entry_index: e.index as u32,
        num_hashes: e.num_hashes,
        entry_hash,
        tx_count: e.executed_transaction_count as u32,
        signatures,
        first_shred_at: None,
        leader: None,
    }
}
