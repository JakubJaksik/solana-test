use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crossbeam_channel::{Receiver, RecvTimeoutError};
use tracing::{info, warn};

#[derive(Debug)]
pub struct TickEvent {
    pub observed_at: Instant,
    pub slot: u64,
    pub tick_idx: u8,
    pub num_hashes: u64,
}

pub struct SidecarConfig {
    pub rx: Receiver<TickEvent>,
    pub path: PathBuf,
    pub anchor: Instant,
    pub pinned_core: Option<usize>,
    pub stop: Arc<AtomicBool>,
}

pub fn spawn(cfg: SidecarConfig) -> std::io::Result<std::thread::JoinHandle<()>> {
    std::thread::Builder::new()
        .name("tick-sidecar".into())
        .spawn(move || {
            if let Some(c) = cfg.pinned_core {
                core_affinity::set_for_current(core_affinity::CoreId { id: c });
            }
            run_loop(cfg);
        })
}

fn run_loop(cfg: SidecarConfig) {
    let mut file = match OpenOptions::new().create(true).append(true).open(&cfg.path) {
        Ok(f) => f,
        Err(e) => {
            warn!(error = %e, "tick sidecar open failed");
            return;
        }
    };
    loop {
        match cfg.rx.recv_timeout(std::time::Duration::from_millis(500)) {
            Ok(ev) => {
                let ts_ns = ev.observed_at.duration_since(cfg.anchor).as_nanos() as u64;
                let _ = writeln!(
                    file,
                    "{{\"ts_ns\":{},\"slot\":{},\"tick_idx\":{},\"num_hashes\":{}}}",
                    ts_ns, ev.slot, ev.tick_idx, ev.num_hashes
                );
            }
            Err(RecvTimeoutError::Timeout) => {
                if cfg.stop.load(Ordering::Relaxed) {
                    break;
                }
            }
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
    info!("tick-sidecar exiting");
}
