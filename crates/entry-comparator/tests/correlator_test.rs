use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam_channel::bounded;
use entry_comparator::correlator::{spawn, CorrelatorConfig, LeaderLookup};
use entry_comparator::diff_record::Source;
use entry_sources::{EntryObservation, SignatureVec, SourceKind};
use solana_sdk::hash::Hash;

struct NoopLeader;
impl LeaderLookup for NoopLeader {
    fn lookup(&self, _slot: u64) -> Option<[u8; 32]> {
        None
    }
}

fn make(source: SourceKind, slot: u64, idx: u32, hash: Hash) -> EntryObservation {
    EntryObservation {
        source,
        observed_at: Instant::now(),
        slot,
        entry_index: idx,
        num_hashes: 100,
        entry_hash: hash,
        tx_count: 0,
        signatures: SignatureVec::new(),
        first_shred_at: None,
        leader: None,
    }
}

#[test]
fn matches_when_both_arrive() {
    let (ys_tx, ys_rx) = bounded(64);
    let (ss_tx, ss_rx) = bounded(64);
    let (diff_tx, diff_rx) = bounded(64);
    let _h = spawn(CorrelatorConfig {
        ys_rx,
        ss_rx,
        diff_tx,
        anchor: Instant::now(),
        deadline: Duration::from_secs(5),
        pinned_core: None,
        leader_lookup: Arc::new(NoopLeader),
        diff_dropped: Arc::new(AtomicU64::new(0)),
    })
    .unwrap();

    let h = Hash::default();
    ys_tx.send(make(SourceKind::Yellowstone, 1, 0, h)).unwrap();
    ss_tx.send(make(SourceKind::ShredStream, 1, 0, h)).unwrap();

    let rec = diff_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(rec.source, Source::Both);
    assert!(rec.hash_match);
    assert_eq!(rec.slot, 1);
    assert_eq!(rec.entry_index, 0);
    assert!(rec.ys_observed_ns.is_some());
    assert!(rec.ss_fec_complete_ns.is_some());
}

#[test]
fn emits_single_source_after_deadline() {
    let (ys_tx, ys_rx) = bounded(64);
    let (_ss_tx, ss_rx) = bounded::<EntryObservation>(64);
    let (diff_tx, diff_rx) = bounded(64);
    let _h = spawn(CorrelatorConfig {
        ys_rx,
        ss_rx,
        diff_tx,
        anchor: Instant::now(),
        deadline: Duration::from_millis(300),
        pinned_core: None,
        leader_lookup: Arc::new(NoopLeader),
        diff_dropped: Arc::new(AtomicU64::new(0)),
    })
    .unwrap();

    ys_tx
        .send(make(SourceKind::Yellowstone, 1, 0, Hash::default()))
        .unwrap();
    let rec = diff_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(rec.source, Source::YsOnly);
    assert!(!rec.hash_match);
    assert_eq!(rec.ss_fec_complete_ns, None);
}

#[test]
fn hash_mismatch_when_hashes_differ() {
    let (ys_tx, ys_rx) = bounded(64);
    let (ss_tx, ss_rx) = bounded(64);
    let (diff_tx, diff_rx) = bounded(64);
    let _h = spawn(CorrelatorConfig {
        ys_rx,
        ss_rx,
        diff_tx,
        anchor: Instant::now(),
        deadline: Duration::from_secs(5),
        pinned_core: None,
        leader_lookup: Arc::new(NoopLeader),
        diff_dropped: Arc::new(AtomicU64::new(0)),
    })
    .unwrap();

    ys_tx
        .send(make(SourceKind::Yellowstone, 1, 0, Hash::new_from_array([1; 32])))
        .unwrap();
    ss_tx
        .send(make(SourceKind::ShredStream, 1, 0, Hash::new_from_array([2; 32])))
        .unwrap();

    let rec = diff_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(rec.source, Source::Both);
    assert!(!rec.hash_match);
}

#[test]
fn leader_lookup_populates_pubkey() {
    let (ys_tx, ys_rx) = bounded(64);
    let (_ss_tx, ss_rx) = bounded::<EntryObservation>(64);
    let (diff_tx, diff_rx) = bounded(64);
    let leader_pk = [42u8; 32];
    let _h = spawn(CorrelatorConfig {
        ys_rx,
        ss_rx,
        diff_tx,
        anchor: Instant::now(),
        deadline: Duration::from_millis(200),
        pinned_core: None,
        leader_lookup: Arc::new(move |_slot| Some(leader_pk)),
        diff_dropped: Arc::new(AtomicU64::new(0)),
    })
    .unwrap();
    ys_tx
        .send(make(SourceKind::Yellowstone, 7, 0, Hash::default()))
        .unwrap();
    let rec = diff_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(rec.leader_pubkey, Some(leader_pk));
}
