use std::sync::{Arc, OnceLock};
use std::time::Instant;

use crossbeam_channel::{bounded, Receiver, Sender};
use futures_util::StreamExt;
use solana_entry::entry::Entry as SolanaEntry;
use tracing::{error, info};

use crate::counters::DropCounters;
use crate::observation::{EntryObservation, SignatureVec, SourceKind};
use crate::source::EntrySource;

pub mod proto {
    pub mod shared {
        tonic::include_proto!("shared");
    }
    pub mod shredstream {
        tonic::include_proto!("shredstream");
    }
}

use proto::shredstream::shredstream_proxy_client::ShredstreamProxyClient;
use proto::shredstream::SubscribeEntriesRequest;

/// Connects to a `jito-shredstream-proxy` instance running with
/// `--grpc-service-port <X>` and consumes deserialized Solana entries.
/// The proxy performs FEC reconstruction and entry deserialization itself,
/// so we skip our own deshred path entirely. `first_shred_at` is `None`
/// here — the proxy does not expose per-shred timestamps via this stream.
pub struct ShredStreamGrpcSource {
    pub endpoint: String,
    pub channel_capacity: usize,
    pub pinned_core: Option<usize>,
    pub counters: Arc<DropCounters>,
}

impl EntrySource for ShredStreamGrpcSource {
    fn kind(&self) -> SourceKind {
        SourceKind::ShredStream
    }

    fn start(self: Box<Self>) -> anyhow::Result<Receiver<EntryObservation>> {
        let (tx, rx) = bounded::<EntryObservation>(self.channel_capacity);
        let endpoint = self.endpoint.clone();
        let pinned = self.pinned_core;
        let counters = self.counters.clone();

        std::thread::Builder::new()
            .name("ss-grpc".into())
            .spawn(move || {
                if let Some(c) = pinned {
                    core_affinity::set_for_current(core_affinity::CoreId { id: c });
                }
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("build current_thread runtime");
                rt.block_on(run_loop(endpoint, tx, counters));
            })?;
        Ok(rx)
    }
}

async fn run_loop(
    endpoint: String,
    tx: Sender<EntryObservation>,
    counters: Arc<DropCounters>,
) {
    let mut backoff_ms = 100u64;
    loop {
        match run_once(&endpoint, &tx, &counters).await {
            Ok(()) => info!("shredstream grpc stream closed cleanly, reconnecting"),
            Err(e) => {
                error!(error = %e, "shredstream grpc error, reconnecting");
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
        backoff_ms = (backoff_ms * 2).min(5_000);
    }
}

async fn run_once(
    endpoint: &str,
    tx: &Sender<EntryObservation>,
    counters: &DropCounters,
) -> anyhow::Result<()> {
    let mut client = ShredstreamProxyClient::connect(endpoint.to_string()).await?;
    let stream = client.subscribe_entries(SubscribeEntriesRequest {}).await?;
    let mut messages = stream.into_inner();
    info!(endpoint, "shredstream grpc subscription open");

    // Per-slot cumulative entry offset. Each proxy message carries one batch
    // of entries (typically one deshredded FEC set). Without this counter the
    // emitted `entry_index` would reset to 0 on every message, making it
    // impossible for downstream consumers to reconstruct PoH order even when
    // the proxy emits batches in the right sequence.
    let mut slot_offset: std::collections::HashMap<u64, u32> =
        std::collections::HashMap::with_capacity(64);
    let mut highest_slot: u64 = 0;

    while let Some(msg) = messages.next().await {
        // Timestamp is the first operation after stream yields a message.
        let observed_at = Instant::now();
        let entry_msg = msg?;
        let slot = entry_msg.slot;
        let entries = match decode_entries(&entry_msg.entries) {
            Some(v) => v,
            None => {
                counters.inc(&counters.ss_entry_decode_error);
                continue;
            }
        };

        let base = *slot_offset.entry(slot).or_insert(0);
        for (i, entry) in entries.iter().enumerate() {
            emit(slot, base + i as u32, entry, observed_at, tx, counters);
        }
        *slot_offset.get_mut(&slot).unwrap() = base + entries.len() as u32;

        // Bounded-memory eviction: drop offsets for slots far behind the
        // frontier. 512 slots ~= 3.5 minutes of history on mainnet.
        if slot > highest_slot {
            highest_slot = slot;
        }
        if slot_offset.len() > 1024 {
            let cutoff = highest_slot.saturating_sub(512);
            slot_offset.retain(|s, _| *s >= cutoff);
        }
    }
    Ok(())
}

enum EntryCodec {
    Bincode,
    Wincode,
}

static CODEC: OnceLock<EntryCodec> = OnceLock::new();

#[inline]
fn decode_entries(bytes: &[u8]) -> Option<Vec<SolanaEntry>> {
    // Use cached codec if we have already discovered which one works.
    if let Some(codec) = CODEC.get() {
        return match codec {
            EntryCodec::Bincode => bincode::deserialize::<Vec<SolanaEntry>>(bytes).ok(),
            EntryCodec::Wincode => wincode::deserialize::<Vec<SolanaEntry>>(bytes).ok(),
        };
    }
    // First call: try both, remember which worked.
    if let Ok(v) = bincode::deserialize::<Vec<SolanaEntry>>(bytes) {
        let _ = CODEC.set(EntryCodec::Bincode);
        return Some(v);
    }
    if let Ok(v) = wincode::deserialize::<Vec<SolanaEntry>>(bytes) {
        let _ = CODEC.set(EntryCodec::Wincode);
        return Some(v);
    }
    None
}

#[inline]
fn emit(
    slot: u64,
    entry_index: u32,
    entry: &SolanaEntry,
    observed_at: Instant,
    tx: &Sender<EntryObservation>,
    counters: &DropCounters,
) {
    let mut sigs = SignatureVec::with_capacity(entry.transactions.len().min(8));
    for txn in &entry.transactions {
        if let Some(sig) = txn.signatures.first() {
            sigs.push(*sig);
        }
    }
    let obs = EntryObservation {
        source: SourceKind::ShredStream,
        observed_at,
        slot,
        entry_index,
        num_hashes: entry.num_hashes,
        entry_hash: entry.hash,
        tx_count: entry.transactions.len() as u32,
        signatures: sigs,
        first_shred_at: None,
        leader: None,
    };
    if tx.try_send(obs).is_err() {
        counters.inc(&counters.ss_obs_channel_full);
    }
}
