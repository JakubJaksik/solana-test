use std::net::{SocketAddr, UdpSocket};
use std::sync::Arc;
use std::time::Instant;

use crossbeam_channel::{bounded, Receiver, Sender};
use socket2::{Domain, Socket, Type};

use crate::counters::DropCounters;

use super::RawShredPacket;

const MAX_SHRED_SIZE: usize = 1500;
pub const DEFAULT_RX_BUFFER_BYTES: usize = 64 * 1024 * 1024;

pub struct UdpRxConfig {
    pub bind: SocketAddr,
    pub channel_capacity: usize,
    pub pinned_core: Option<usize>,
    pub rx_buffer_bytes: usize,
    pub counters: Arc<DropCounters>,
}

pub fn spawn(cfg: UdpRxConfig) -> std::io::Result<Receiver<RawShredPacket>> {
    let (tx, rx) = bounded::<RawShredPacket>(cfg.channel_capacity);
    let socket = build_socket(&cfg)?;
    let counters = cfg.counters.clone();
    let pinned = cfg.pinned_core;

    std::thread::Builder::new()
        .name("ss-udp-rx".into())
        .spawn(move || {
            if let Some(core) = pinned {
                core_affinity::set_for_current(core_affinity::CoreId { id: core });
            }
            run_loop(socket, tx, counters);
        })?;
    Ok(rx)
}

fn build_socket(cfg: &UdpRxConfig) -> std::io::Result<UdpSocket> {
    let s = Socket::new(Domain::IPV4, Type::DGRAM, None)?;
    s.set_recv_buffer_size(cfg.rx_buffer_bytes)?;
    s.set_reuse_address(true)?;
    s.bind(&cfg.bind.into())?;
    Ok(s.into())
}

#[inline]
fn run_loop(socket: UdpSocket, tx: Sender<RawShredPacket>, counters: Arc<DropCounters>) {
    let mut buf = [0u8; MAX_SHRED_SIZE];
    loop {
        match socket.recv(&mut buf) {
            Ok(n) => {
                // Timestamp is the FIRST operation after recv returns.
                let received_at = Instant::now();
                // Single allocation at RX→worker boundary. Hot path itself uses stack buf.
                let packet = RawShredPacket {
                    bytes: buf[..n].to_vec().into_boxed_slice(),
                    received_at,
                };
                if tx.try_send(packet).is_err() {
                    counters.inc(&counters.ss_udp_channel_full);
                }
            }
            Err(_e) => {
                // Recv error: in practice rare (UDP doesn't have connection state).
                // No logging in hot path. Continue loop.
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddrV4};
    use std::time::Duration;

    #[test]
    fn rx_loop_receives_and_timestamps() {
        // Discover free port
        let probe = UdpSocket::bind(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))).unwrap();
        let port = probe.local_addr().unwrap().port();
        drop(probe);

        let counters = Arc::new(DropCounters::default());
        let cfg = UdpRxConfig {
            bind: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port)),
            channel_capacity: 1024,
            pinned_core: None,
            rx_buffer_bytes: 1 << 20,
            counters: counters.clone(),
        };
        let rx = spawn(cfg).expect("spawn");

        let sender = UdpSocket::bind(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))).unwrap();
        sender.send_to(b"hello", format!("127.0.0.1:{port}")).unwrap();

        let pkt = rx.recv_timeout(Duration::from_secs(2)).expect("packet");
        assert_eq!(&*pkt.bytes, b"hello");
        assert!(pkt.received_at.elapsed() < Duration::from_secs(1));
        // Counter should be untouched on a successful recv.
        assert_eq!(counters.snapshot().ss_udp_channel_full, 0);
    }

    #[test]
    fn rx_loop_increments_counter_when_channel_full() {
        let probe = UdpSocket::bind(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))).unwrap();
        let port = probe.local_addr().unwrap().port();
        drop(probe);

        let counters = Arc::new(DropCounters::default());
        // Capacity 1 → second packet without consumer drains will fill quickly.
        let cfg = UdpRxConfig {
            bind: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port)),
            channel_capacity: 1,
            pinned_core: None,
            rx_buffer_bytes: 1 << 20,
            counters: counters.clone(),
        };
        let _rx = spawn(cfg).expect("spawn");

        let sender = UdpSocket::bind(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))).unwrap();
        for _ in 0..100 {
            sender.send_to(b"x", format!("127.0.0.1:{port}")).unwrap();
        }

        // Give RX thread a moment to process and overflow the channel.
        std::thread::sleep(Duration::from_millis(200));
        // We don't drain `_rx`, so subsequent sends should bump the counter.
        // Note: due to OS-level UDP buffering some packets may be dropped at kernel
        // level before they reach our recv; we only assert >0 increments occurred.
        let dropped = counters.snapshot().ss_udp_channel_full;
        assert!(dropped > 0, "expected at least one channel-full drop, got {}", dropped);
    }
}
