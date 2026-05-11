pub mod deshred;
pub mod fec_tracker;
pub mod grpc;
pub mod udp_rx;

pub use grpc::ShredStreamGrpcSource;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use crossbeam_channel::{bounded, Receiver};

use crate::counters::DropCounters;
use crate::observation::{EntryObservation, SourceKind};
use crate::source::EntrySource;

/// Raw shred packet captured from UDP with arrival timestamp.
/// Created at the RX→worker boundary; ownership transferred to worker pool.
#[derive(Debug)]
pub struct RawShredPacket {
    pub bytes: Vec<u8>,
    pub received_at: Instant,
}

pub struct ShredStreamSource {
    pub bind: SocketAddr,
    pub udp_channel_capacity: usize,
    pub obs_channel_capacity: usize,
    pub udp_pinned_core: Option<usize>,
    pub deshred_pinned_core: Option<usize>,
    pub rx_buffer_bytes: usize,
    pub counters: Arc<DropCounters>,
}

impl EntrySource for ShredStreamSource {
    fn kind(&self) -> SourceKind {
        SourceKind::ShredStream
    }

    fn start(self: Box<Self>) -> anyhow::Result<Receiver<EntryObservation>> {
        let raw_rx = udp_rx::spawn(udp_rx::UdpRxConfig {
            bind: self.bind,
            channel_capacity: self.udp_channel_capacity,
            pinned_core: self.udp_pinned_core,
            rx_buffer_bytes: self.rx_buffer_bytes,
            counters: self.counters.clone(),
        })?;

        let (obs_tx, obs_rx) = bounded::<EntryObservation>(self.obs_channel_capacity);
        deshred::spawn(deshred::DeshredWorkerConfig {
            raw_rx,
            obs_tx,
            pinned_core: self.deshred_pinned_core,
            counters: self.counters.clone(),
        })?;

        Ok(obs_rx)
    }
}
