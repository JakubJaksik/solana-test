use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender};
use entry_sources::{EntryObservation, SourceKind};

use crate::diff_record::{DiffRecord, Source};

/// Lookup function for slot → leader pubkey. Returning `None` is fine (post-process
/// can backfill). Implementation provided by Task 9 (LeaderCache); for tests use a
/// closure or noop.
pub trait LeaderLookup: Send + Sync + 'static {
    fn lookup(&self, slot: u64) -> Option<[u8; 32]>;
}

/// Convenience impl for closures.
impl<F> LeaderLookup for F
where
    F: Fn(u64) -> Option<[u8; 32]> + Send + Sync + 'static,
{
    fn lookup(&self, slot: u64) -> Option<[u8; 32]> {
        (self)(slot)
    }
}

pub struct CorrelatorConfig {
    pub ys_rx: Receiver<EntryObservation>,
    pub ss_rx: Receiver<EntryObservation>,
    pub diff_tx: Sender<DiffRecord>,
    pub anchor: Instant,
    pub deadline: Duration,
    pub pinned_core: Option<usize>,
    pub leader_lookup: Arc<dyn LeaderLookup>,
    /// Counter for diff_tx try_send overflow.
    pub diff_dropped: Arc<AtomicU64>,
    /// External shutdown signal. When set true, the run loop exits, drops `diff_tx`,
    /// and the writer downstream sees Disconnected → flushes + closes Parquet footer.
    pub shutdown: Arc<AtomicBool>,
}

struct MatchState {
    yellowstone: Option<EntryObservation>,
    shredstream: Option<EntryObservation>,
    inserted_at: Instant,
}

pub fn spawn(cfg: CorrelatorConfig) -> std::io::Result<std::thread::JoinHandle<()>> {
    std::thread::Builder::new()
        .name("correlator".into())
        .spawn(move || {
            if let Some(core) = cfg.pinned_core {
                core_affinity::set_for_current(core_affinity::CoreId { id: core });
            }
            run_loop(cfg);
        })
}

fn run_loop(cfg: CorrelatorConfig) {
    let CorrelatorConfig {
        ys_rx,
        ss_rx,
        diff_tx,
        anchor,
        deadline,
        leader_lookup,
        diff_dropped,
        shutdown,
        ..
    } = cfg;
    let mut map: HashMap<(u64, [u8; 32]), MatchState> = HashMap::with_capacity(8192);
    let mut last_sweep = Instant::now();

    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }
        crossbeam_channel::select! {
            recv(ys_rx) -> msg => match msg {
                Ok(obs) => handle(&mut map, &diff_tx, anchor, obs, &*leader_lookup, &diff_dropped),
                Err(_) => break,
            },
            recv(ss_rx) -> msg => match msg {
                Ok(obs) => handle(&mut map, &diff_tx, anchor, obs, &*leader_lookup, &diff_dropped),
                Err(_) => break,
            },
            default(Duration::from_millis(100)) => {}
        }
        let now = Instant::now();
        if now.duration_since(last_sweep) > Duration::from_millis(250) {
            sweep(&mut map, &diff_tx, anchor, now, deadline, &*leader_lookup, &diff_dropped);
            last_sweep = now;
        }
    }
    // Final flush of any remaining state on shutdown / disconnect.
    let now = Instant::now();
    sweep(&mut map, &diff_tx, anchor, now, Duration::from_secs(0), &*leader_lookup, &diff_dropped);
    // diff_tx drops here (end of scope) → writer's recv sees Disconnected → flushes + close.
}

fn handle(
    map: &mut HashMap<(u64, [u8; 32]), MatchState>,
    diff_tx: &Sender<DiffRecord>,
    anchor: Instant,
    obs: EntryObservation,
    leader: &dyn LeaderLookup,
    diff_dropped: &AtomicU64,
) {
    let key = (obs.slot, obs.entry_hash.to_bytes());
    let entry = map.entry(key).or_insert_with(|| MatchState {
        yellowstone: None,
        shredstream: None,
        inserted_at: Instant::now(),
    });
    match obs.source {
        SourceKind::Yellowstone => entry.yellowstone = Some(obs),
        SourceKind::ShredStream => entry.shredstream = Some(obs),
    }
    if entry.yellowstone.is_some() && entry.shredstream.is_some() {
        let st = map.remove(&key).unwrap();
        emit(st, diff_tx, anchor, leader, diff_dropped);
    }
}

fn sweep(
    map: &mut HashMap<(u64, [u8; 32]), MatchState>,
    diff_tx: &Sender<DiffRecord>,
    anchor: Instant,
    now: Instant,
    deadline: Duration,
    leader: &dyn LeaderLookup,
    diff_dropped: &AtomicU64,
) {
    let stale: Vec<(u64, [u8; 32])> = map
        .iter()
        .filter(|(_, st)| now.duration_since(st.inserted_at) >= deadline)
        .map(|(k, _)| *k)
        .collect();
    for key in stale {
        let st = map.remove(&key).unwrap();
        emit(st, diff_tx, anchor, leader, diff_dropped);
    }
}

fn emit(
    st: MatchState,
    diff_tx: &Sender<DiffRecord>,
    anchor: Instant,
    leader: &dyn LeaderLookup,
    diff_dropped: &AtomicU64,
) {
    let ys = st.yellowstone.as_ref();
    let ss = st.shredstream.as_ref();
    let source = match (ys, ss) {
        (Some(_), Some(_)) => Source::Both,
        (Some(_), None) => Source::YsOnly,
        (None, Some(_)) => Source::SsOnly,
        (None, None) => unreachable!(),
    };
    let any = ys.or(ss).unwrap();
    let hash_match = match (ys, ss) {
        (Some(y), Some(s)) => y.entry_hash == s.entry_hash,
        _ => false,
    };
    let sig_set_match = match (ys, ss) {
        (Some(y), Some(s)) if !y.signatures.is_empty() && !s.signatures.is_empty() => {
            let mut a: Vec<&solana_sdk::signature::Signature> = y.signatures.iter().collect();
            let mut b: Vec<&solana_sdk::signature::Signature> = s.signatures.iter().collect();
            a.sort();
            b.sort();
            Some(a == b)
        }
        _ => None,
    };
    let leader_pubkey = leader.lookup(any.slot);

    let rec = DiffRecord {
        slot: any.slot,
        entry_index: any.entry_index,
        num_hashes: any.num_hashes,
        source,
        ys_observed_ns: ys.map(|y| y.observed_at.duration_since(anchor).as_nanos() as u64),
        ss_first_shred_ns: ss
            .and_then(|s| s.first_shred_at)
            .map(|t| t.duration_since(anchor).as_nanos() as u64),
        ss_fec_complete_ns: ss.map(|s| s.observed_at.duration_since(anchor).as_nanos() as u64),
        ys_hash: ys.map(|y| y.entry_hash.to_bytes()),
        ss_hash: ss.map(|s| s.entry_hash.to_bytes()),
        ys_tx_count: ys.map(|y| y.tx_count),
        ss_tx_count: ss.map(|s| s.tx_count),
        hash_match,
        sig_set_match,
        leader_pubkey,
        ys_signatures: ys.map(|y| y.signatures.iter().copied().collect()).unwrap_or_default(),
        ss_signatures: ss.map(|s| s.signatures.iter().copied().collect()).unwrap_or_default(),
    };
    if diff_tx.try_send(rec).is_err() {
        diff_dropped.fetch_add(1, Ordering::Relaxed);
    }
}
