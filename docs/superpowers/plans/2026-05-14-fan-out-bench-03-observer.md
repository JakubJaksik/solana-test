# fan-out-bench — Plan 3: Entry Sources Merger + Observer

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development.

**Goal:** Zbudować warstwę obserwacji on-chain: EntryMerger który łączy strumienie z Jito Shredstream + Helius Yellowstone z dedup po `(slot, entry_hash)`, plus Observer który śledzi PoH ticki per slot i odpala TriggerEvent gdy schedule trafia w `(slot, tick)`. Po tym planie obserwacja chain'a działa, ale sygnały trigger trafiają na "powietrze" — Plan 4 podpina dispatcher i matcher do nich.

**Architecture:** Dwa entry sources (już istnieją w `entry-sources` crate) feedują merger przez crossbeam channels. Merger dedup'uje używając `(slot, entry_hash)` jako klucza i emituje pojedynczy strumień na obserwer. Observer trzyma SlotState per slot z PoH tick counterem i strumień TriggerEvent na wyjściu.

**Tech Stack:** Rust 2024, entry-sources crate (reuse), crossbeam-channel, solana-sdk.

**Reference spec:** `docs/superpowers/specs/2026-05-14-fan-out-bench-design.md` §2.2, §3.1, §3.4

**Previous plans:** Plan 1 (foundation), Plan 2 (nonce infra).

---

## File structure (Plan 3 scope)

```
crates/fan-out-bench/
├── src/
│   ├── lib.rs                — declare merger, observer, trigger modules
│   ├── trigger.rs            — TriggerEvent struct
│   ├── merger.rs             — EntryMerger: dedup SS+YS by (slot, entry_hash)
│   └── observer.rs           — PoH tick tracker + schedule match → TriggerEvent emit
└── tests/
    └── observer_integration.rs — mock entry stream → observer → trigger count
```

NOT in this plan (deferred):
- Signature matching (waits for pending_sigs from Plan 4 dispatcher)
- Tick sidecar JSONL diagnostics (Plan 7 ops)
- Real SS/YS gRPC wiring at runtime (Plan 4 runtime)

---

## Task 1: Module scaffolding

**Files:**
- Modify: `crates/fan-out-bench/src/lib.rs`
- Create: `crates/fan-out-bench/src/{trigger,merger,observer}.rs` (stubs)

- [ ] **Step 1: Add modules to lib.rs**

Edit `crates/fan-out-bench/src/lib.rs` adding `merger`, `observer`, `trigger`:

```rust
pub mod attempt_state;
pub mod config;
pub mod counters;
pub mod memo;
pub mod merger;
pub mod nonce;
pub mod observer;
pub mod outcome;
pub mod pool;
pub mod schedule;
pub mod senders;
pub mod tip_accounts;
pub mod trigger;
pub mod tx_builder;
pub mod wallet;
pub mod writer;
```

- [ ] **Step 2: Create stub files**

```bash
cd /home/jjaksik/Repos/my-scripts/crates/fan-out-bench/src
touch trigger.rs merger.rs observer.rs
```

Each gets `// implementation in later task` line.

- [ ] **Step 3: Verify**

Run: `cargo check -p fan-out-bench`. Expected: clean build.

DO NOT run git operations.

---

## Task 2: TriggerEvent type

**Files:**
- Replace stub: `crates/fan-out-bench/src/trigger.rs`

- [ ] **Step 1: Write TriggerEvent**

```rust
//! TriggerEvent — emitted by observer when schedule (slot, tick) matches.
//!
//! Consumed by dispatcher in Plan 4. For Plan 3 we just emit + count.

use std::time::Instant;

#[derive(Debug, Clone, Copy)]
pub struct TriggerEvent {
    pub slot: u64,
    pub tick: u8,
    /// Cumulative hashes from the start of the slot at trigger time
    /// (sub-tick precision for ex-post analysis).
    pub cumulative_hashes_in_slot: u64,
    /// Wall-clock instant when observer fired the trigger.
    pub observed_at: Instant,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trigger_event_constructs() {
        let t = TriggerEvent {
            slot: 100,
            tick: 5,
            cumulative_hashes_in_slot: 312_500,
            observed_at: Instant::now(),
        };
        assert_eq!(t.slot, 100);
        assert_eq!(t.tick, 5);
    }
}
```

Run: `cargo test -p fan-out-bench --lib trigger`. Expected: 1 test passes.

---

## Task 3: EntryMerger — dedup by (slot, entry_hash)

**Files:**
- Replace stub: `crates/fan-out-bench/src/merger.rs`

- [ ] **Step 1: Write merger**

```rust
//! EntryMerger — merges SS + YS entry streams with dedup by (slot, entry_hash).
//!
//! Emits each unique entry ONCE (first-seen). The slower source's later
//! observation is dropped at this stage; Plan 4 matcher will track per-source
//! signature timestamps separately for parquet.
//!
//! Rolling-window eviction: drop dedup keys with slot < current_slot - WINDOW.

use crate::counters::BenchCounters;
use crossbeam_channel::{Receiver, Sender};
use entry_sources::{EntryObservation, SourceKind};
use solana_sdk::hash::Hash;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;

const DEDUP_WINDOW_SLOTS: u64 = 50;

#[derive(Debug, Clone)]
pub struct MergedEntry {
    pub observation: EntryObservation,
    pub first_seen_source: SourceKind,
}

pub struct MergerConfig {
    pub ss_rx: Receiver<EntryObservation>,
    pub ys_rx: Receiver<EntryObservation>,
    pub out_tx: Sender<MergedEntry>,
    pub pinned_core: Option<usize>,
    pub counters: Arc<BenchCounters>,
    pub stop: Arc<AtomicBool>,
}

pub fn spawn(cfg: MergerConfig) -> std::io::Result<JoinHandle<()>> {
    std::thread::Builder::new()
        .name("entry-merger".into())
        .spawn(move || {
            if let Some(core) = cfg.pinned_core {
                core_affinity::set_for_current(core_affinity::CoreId { id: core });
            }
            run_loop(cfg);
        })
}

fn run_loop(cfg: MergerConfig) {
    let mut seen: HashSet<(u64, Hash)> = HashSet::with_capacity(8192);
    let mut max_slot: u64 = 0;

    loop {
        if cfg.stop.load(Ordering::Relaxed) {
            break;
        }
        let (obs, source) = crossbeam_channel::select! {
            recv(cfg.ss_rx) -> msg => match msg {
                Ok(o) => (o, SourceKind::ShredStream),
                Err(_) => {
                    tracing::warn!("ss channel disconnected");
                    break;
                }
            },
            recv(cfg.ys_rx) -> msg => match msg {
                Ok(o) => (o, SourceKind::Yellowstone),
                Err(_) => {
                    tracing::warn!("ys channel disconnected");
                    break;
                }
            },
        };

        let key = (obs.slot, obs.entry_hash);
        if !seen.insert(key) {
            // Second-source duplicate — drop. (Plan 4 matcher tracks
            // per-source sig timestamps separately.)
            continue;
        }

        if obs.slot > max_slot {
            max_slot = obs.slot;
            // Evict stale keys outside rolling window
            if max_slot > DEDUP_WINDOW_SLOTS {
                let cutoff = max_slot - DEDUP_WINDOW_SLOTS;
                seen.retain(|(s, _)| *s >= cutoff);
            }
        }

        let merged = MergedEntry {
            observation: obs,
            first_seen_source: source,
        };
        if cfg.out_tx.try_send(merged).is_err() {
            // queue full or disconnected; count and continue
            cfg.counters
                .send_event_queue_full
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_channel::{bounded, unbounded};
    use entry_sources::{EntryObservation, SignatureVec};
    use std::time::Instant;

    fn make_obs(slot: u64, entry_hash: Hash, source: SourceKind) -> EntryObservation {
        EntryObservation {
            source,
            observed_at: Instant::now(),
            slot,
            entry_index: 0,
            num_hashes: 62_500,
            entry_hash,
            tx_count: 0,
            signatures: SignatureVec::new(),
            first_shred_at: None,
            leader: None,
        }
    }

    #[test]
    fn dedups_same_slot_entry_hash() {
        let (ss_tx, ss_rx) = unbounded();
        let (ys_tx, ys_rx) = unbounded();
        let (out_tx, out_rx) = bounded(100);
        let stop = Arc::new(AtomicBool::new(false));
        let counters = Arc::new(BenchCounters::default());

        let h = Hash::new_unique();
        ss_tx.send(make_obs(100, h, SourceKind::ShredStream)).unwrap();
        ys_tx.send(make_obs(100, h, SourceKind::Yellowstone)).unwrap();

        let handle = spawn(MergerConfig {
            ss_rx, ys_rx, out_tx,
            pinned_core: None,
            counters: counters.clone(),
            stop: stop.clone(),
        }).unwrap();

        // Wait a bit for processing
        std::thread::sleep(std::time::Duration::from_millis(50));
        stop.store(true, Ordering::Relaxed);
        // Drop senders so loop exits
        drop(ss_tx);
        drop(ys_tx);
        let _ = handle.join();

        let mut merged = Vec::new();
        while let Ok(m) = out_rx.try_recv() {
            merged.push(m);
        }
        assert_eq!(merged.len(), 1, "expected exactly 1 merged entry, got {}", merged.len());
    }

    #[test]
    fn different_entry_hashes_both_emitted() {
        let (ss_tx, ss_rx) = unbounded();
        let (_ys_tx, ys_rx) = unbounded::<EntryObservation>();
        let (out_tx, out_rx) = bounded(100);
        let stop = Arc::new(AtomicBool::new(false));
        let counters = Arc::new(BenchCounters::default());

        ss_tx.send(make_obs(100, Hash::new_unique(), SourceKind::ShredStream)).unwrap();
        ss_tx.send(make_obs(100, Hash::new_unique(), SourceKind::ShredStream)).unwrap();
        ss_tx.send(make_obs(100, Hash::new_unique(), SourceKind::ShredStream)).unwrap();

        let handle = spawn(MergerConfig {
            ss_rx, ys_rx, out_tx,
            pinned_core: None,
            counters: counters.clone(),
            stop: stop.clone(),
        }).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(50));
        stop.store(true, Ordering::Relaxed);
        drop(ss_tx);
        let _ = handle.join();

        let mut merged = Vec::new();
        while let Ok(m) = out_rx.try_recv() {
            merged.push(m);
        }
        assert_eq!(merged.len(), 3);
    }

    #[test]
    fn first_seen_source_is_recorded() {
        let (ss_tx, ss_rx) = unbounded();
        let (_ys_tx, ys_rx) = unbounded::<EntryObservation>();
        let (out_tx, out_rx) = bounded(10);
        let stop = Arc::new(AtomicBool::new(false));
        let counters = Arc::new(BenchCounters::default());

        ss_tx.send(make_obs(100, Hash::new_unique(), SourceKind::ShredStream)).unwrap();
        let handle = spawn(MergerConfig {
            ss_rx, ys_rx, out_tx,
            pinned_core: None,
            counters: counters.clone(),
            stop: stop.clone(),
        }).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(30));
        stop.store(true, Ordering::Relaxed);
        drop(ss_tx);
        let _ = handle.join();

        let merged = out_rx.try_recv().unwrap();
        assert_eq!(merged.first_seen_source, SourceKind::ShredStream);
    }
}
```

Run: `cargo test -p fan-out-bench --lib merger`. Expected: 3 tests pass.

---

## Task 4: Observer — SlotState + PoH tick tracker (without schedule matching)

**Files:**
- Replace stub: `crates/fan-out-bench/src/observer.rs` (partial — only SlotState part for this task)

- [ ] **Step 1: Write SlotState + tick counter logic**

```rust
//! Observer — tracks PoH ticks per slot from merged entry stream and fires
//! TriggerEvent when schedule (slot, tick) matches.
//!
//! See spec §2.2 + §7.2. Reference impl pattern: crates/tick-trigger-bench/src/observer.rs

use crate::counters::BenchCounters;
use crate::merger::MergedEntry;
use crate::trigger::TriggerEvent;
use crossbeam_channel::{Receiver, Sender};
use solana_sdk::hash::Hash;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Instant;

/// Solana mainnet PoH: 62_500 hashes per tick, 64 ticks per slot.
pub const HASHES_PER_TICK: u64 = 62_500;
pub const TICKS_PER_SLOT: u8 = 64;

#[derive(Debug, Default)]
struct SlotState {
    /// Index of the last valid PoH tick observed in this slot (1..=64).
    tick_idx: u8,
    /// Hashes accumulated since the previous tick boundary; used to detect
    /// when an empty entry crosses a tick threshold.
    hash_count_since_last_tick: u64,
    /// Cumulative hashes from the start of the slot (never reset by tick
    /// detection). Used to record sub-tick precision in TriggerEvent.
    cumulative_hashes_in_slot: u64,
    /// PoH entry hashes already processed for this slot (dedup across
    /// duplicate shred deliveries / fork artefacts).
    seen_entries: HashSet<Hash>,
    /// (tick, fired) — ticks at which we've already fired a trigger
    /// (prevents duplicate fires from the same slot).
    fired_ticks: HashSet<u8>,
}

pub struct ObserverConfig {
    pub merged_rx: Receiver<MergedEntry>,
    pub schedule: Arc<HashSet<(u64, u8)>>,
    pub trigger_tx: Sender<TriggerEvent>,
    pub current_slot: Arc<AtomicU64>,
    pub pinned_core: Option<usize>,
    pub counters: Arc<BenchCounters>,
    pub stop: Arc<AtomicBool>,
}

pub fn spawn(cfg: ObserverConfig) -> std::io::Result<JoinHandle<()>> {
    std::thread::Builder::new()
        .name("observer".into())
        .spawn(move || {
            if let Some(core) = cfg.pinned_core {
                core_affinity::set_for_current(core_affinity::CoreId { id: core });
            }
            run_loop(cfg);
        })
}

fn run_loop(cfg: ObserverConfig) {
    let mut slot_states: HashMap<u64, SlotState> = HashMap::with_capacity(64);
    let mut last_eviction = Instant::now();

    loop {
        if cfg.stop.load(Ordering::Relaxed) {
            break;
        }
        let merged = match cfg.merged_rx.recv_timeout(std::time::Duration::from_millis(200)) {
            Ok(m) => m,
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                maybe_evict_old_slots(&mut slot_states, &cfg.current_slot, &mut last_eviction);
                continue;
            }
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
        };

        process_entry(&merged, &mut slot_states, &cfg);
        let current = cfg.current_slot.load(Ordering::Relaxed);
        if merged.observation.slot > current {
            cfg.current_slot
                .store(merged.observation.slot, Ordering::Relaxed);
        }
        maybe_evict_old_slots(&mut slot_states, &cfg.current_slot, &mut last_eviction);
    }
}

fn process_entry(merged: &MergedEntry, states: &mut HashMap<u64, SlotState>, cfg: &ObserverConfig) {
    let obs = &merged.observation;
    let state = states.entry(obs.slot).or_default();

    // Dedup by entry_hash within slot
    if !state.seen_entries.insert(obs.entry_hash) {
        return;
    }

    state.cumulative_hashes_in_slot = state
        .cumulative_hashes_in_slot
        .saturating_add(obs.num_hashes);
    state.hash_count_since_last_tick = state
        .hash_count_since_last_tick
        .saturating_add(obs.num_hashes);

    // A PoH tick is an empty entry (tx_count = 0) whose hash_count_since_last_tick
    // equals HASHES_PER_TICK. We use the cumulative hash_count_since_last_tick
    // rather than just num_hashes because shreds may arrive in partial chunks.
    let is_tick = obs.tx_count == 0
        && state.hash_count_since_last_tick == HASHES_PER_TICK;

    if is_tick {
        // Advance tick counter
        if state.tick_idx < TICKS_PER_SLOT {
            state.tick_idx = state.tick_idx.saturating_add(1);
            let tick_now = state.tick_idx;
            state.hash_count_since_last_tick = 0;

            // Check schedule
            cfg.counters
                .schedule_contains_calls
                .fetch_add(1, Ordering::Relaxed);
            if cfg.schedule.contains(&(obs.slot, tick_now))
                && state.fired_ticks.insert(tick_now)
            {
                cfg.counters
                    .schedule_contains_true
                    .fetch_add(1, Ordering::Relaxed);
                let event = TriggerEvent {
                    slot: obs.slot,
                    tick: tick_now,
                    cumulative_hashes_in_slot: state.cumulative_hashes_in_slot,
                    observed_at: Instant::now(),
                };
                if cfg.trigger_tx.try_send(event).is_err() {
                    cfg.counters
                        .tick_event_queue_full
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        } else {
            // tick_idx overflow — symptom of forks / duplicate shreds
            cfg.counters
                .fork_tick_overflow
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn maybe_evict_old_slots(
    states: &mut HashMap<u64, SlotState>,
    current_slot: &AtomicU64,
    last_eviction: &mut Instant,
) {
    if last_eviction.elapsed() < std::time::Duration::from_millis(500) {
        return;
    }
    *last_eviction = Instant::now();
    let current = current_slot.load(Ordering::Relaxed);
    if current > 64 {
        let cutoff = current.saturating_sub(64);
        states.retain(|s, _| *s >= cutoff);
    }
}
```

Run: `cargo check -p fan-out-bench`. Expected: builds clean.

---

## Task 5: Observer tests (PoH tick counting + schedule matching)

**Files:**
- Modify: `crates/fan-out-bench/src/observer.rs` (add test module at end)

- [ ] **Step 1: Append tests block to observer.rs**

Append to the end of `crates/fan-out-bench/src/observer.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_channel::{bounded, unbounded};
    use entry_sources::{EntryObservation, SignatureVec, SourceKind};
    use std::time::Duration;

    fn make_merged(slot: u64, entry_hash: Hash, num_hashes: u64, tx_count: u32) -> MergedEntry {
        MergedEntry {
            observation: EntryObservation {
                source: SourceKind::ShredStream,
                observed_at: Instant::now(),
                slot,
                entry_index: 0,
                num_hashes,
                entry_hash,
                tx_count,
                signatures: SignatureVec::new(),
                first_shred_at: None,
                leader: None,
            },
            first_seen_source: SourceKind::ShredStream,
        }
    }

    fn setup_observer(
        schedule: HashSet<(u64, u8)>,
    ) -> (
        crossbeam_channel::Sender<MergedEntry>,
        crossbeam_channel::Receiver<TriggerEvent>,
        Arc<AtomicBool>,
        Arc<BenchCounters>,
        JoinHandle<()>,
    ) {
        let (in_tx, in_rx) = unbounded();
        let (out_tx, out_rx) = bounded(100);
        let stop = Arc::new(AtomicBool::new(false));
        let counters = Arc::new(BenchCounters::default());
        let current_slot = Arc::new(AtomicU64::new(0));
        let handle = spawn(ObserverConfig {
            merged_rx: in_rx,
            schedule: Arc::new(schedule),
            trigger_tx: out_tx,
            current_slot,
            pinned_core: None,
            counters: counters.clone(),
            stop: stop.clone(),
        }).unwrap();
        (in_tx, out_rx, stop, counters, handle)
    }

    fn shutdown(in_tx: crossbeam_channel::Sender<MergedEntry>, stop: Arc<AtomicBool>, handle: JoinHandle<()>) {
        std::thread::sleep(Duration::from_millis(50));
        stop.store(true, Ordering::Relaxed);
        drop(in_tx);
        let _ = handle.join();
    }

    #[test]
    fn counts_one_tick_per_complete_hash_count() {
        let mut schedule = HashSet::new();
        schedule.insert((100, 1));
        let (in_tx, out_rx, stop, _counters, handle) = setup_observer(schedule);

        // Empty entry with exactly HASHES_PER_TICK hashes → tick 1 of slot 100
        in_tx
            .send(make_merged(100, Hash::new_unique(), HASHES_PER_TICK, 0))
            .unwrap();

        std::thread::sleep(Duration::from_millis(30));
        let event = out_rx.try_recv().expect("expected trigger event");
        assert_eq!(event.slot, 100);
        assert_eq!(event.tick, 1);
        assert_eq!(event.cumulative_hashes_in_slot, HASHES_PER_TICK);

        shutdown(in_tx, stop, handle);
    }

    #[test]
    fn no_trigger_when_schedule_does_not_match() {
        let schedule: HashSet<(u64, u8)> = HashSet::new();
        let (in_tx, out_rx, stop, counters, handle) = setup_observer(schedule);

        in_tx
            .send(make_merged(100, Hash::new_unique(), HASHES_PER_TICK, 0))
            .unwrap();
        std::thread::sleep(Duration::from_millis(30));
        assert!(out_rx.try_recv().is_err());
        assert!(counters.schedule_contains_calls.load(Ordering::Relaxed) >= 1);
        assert_eq!(counters.schedule_contains_true.load(Ordering::Relaxed), 0);

        shutdown(in_tx, stop, handle);
    }

    #[test]
    fn ticks_accumulate_to_match_higher_tick_in_schedule() {
        let mut schedule = HashSet::new();
        schedule.insert((100, 3));
        let (in_tx, out_rx, stop, _counters, handle) = setup_observer(schedule);

        // Send 3 empty entries each with full hashes_per_tick — should fire on 3rd
        for _ in 0..3 {
            in_tx
                .send(make_merged(100, Hash::new_unique(), HASHES_PER_TICK, 0))
                .unwrap();
        }
        std::thread::sleep(Duration::from_millis(50));
        let event = out_rx.try_recv().expect("expected trigger at tick 3");
        assert_eq!(event.tick, 3);

        shutdown(in_tx, stop, handle);
    }

    #[test]
    fn duplicate_entry_hashes_ignored() {
        let mut schedule = HashSet::new();
        schedule.insert((100, 1));
        let (in_tx, out_rx, stop, _counters, handle) = setup_observer(schedule);

        let h = Hash::new_unique();
        // Same hash twice — second is dedup'd
        in_tx.send(make_merged(100, h, HASHES_PER_TICK, 0)).unwrap();
        in_tx.send(make_merged(100, h, HASHES_PER_TICK, 0)).unwrap();
        std::thread::sleep(Duration::from_millis(30));

        // Should fire trigger once
        let event = out_rx.try_recv().unwrap();
        assert_eq!(event.tick, 1);
        // No second event
        assert!(out_rx.try_recv().is_err());

        shutdown(in_tx, stop, handle);
    }

    #[test]
    fn non_tick_entry_does_not_advance_tick() {
        let mut schedule = HashSet::new();
        schedule.insert((100, 1));
        let (in_tx, out_rx, stop, _counters, handle) = setup_observer(schedule);

        // tx-bearing entry (tx_count=5) with HASHES_PER_TICK hashes — NOT a tick
        in_tx
            .send(make_merged(100, Hash::new_unique(), HASHES_PER_TICK, 5))
            .unwrap();
        std::thread::sleep(Duration::from_millis(30));
        assert!(out_rx.try_recv().is_err(), "no trigger for tx-bearing entry");

        // Now an empty entry that completes the tick
        // Note: hash_count_since_last_tick = 2*HASHES_PER_TICK now, won't match.
        // To properly trigger tick 1, we need exactly HASHES_PER_TICK accumulated.
        // This test just verifies tx_count!=0 doesn't trigger.

        shutdown(in_tx, stop, handle);
    }

    #[test]
    fn trigger_not_fired_twice_for_same_tick() {
        let mut schedule = HashSet::new();
        schedule.insert((100, 1));
        let (in_tx, out_rx, stop, _counters, handle) = setup_observer(schedule);

        let h1 = Hash::new_unique();
        let h2 = Hash::new_unique();
        in_tx.send(make_merged(100, h1, HASHES_PER_TICK, 0)).unwrap();
        std::thread::sleep(Duration::from_millis(30));
        // First trigger fires
        assert!(out_rx.try_recv().is_ok());

        // Send another tick-shaped entry that would hypothetically cross another threshold
        // — but tick_idx is already at 1; we're not at tick 2 yet.
        in_tx.send(make_merged(100, h2, HASHES_PER_TICK, 0)).unwrap();
        std::thread::sleep(Duration::from_millis(30));
        // No second trigger for tick 1
        // (this entry advances to tick 2, which isn't scheduled)
        assert!(out_rx.try_recv().is_err());

        shutdown(in_tx, stop, handle);
    }
}
```

Run: `cargo test -p fan-out-bench --lib observer`. Expected: 6 tests pass.

---

## Task 6: Integration test — multi-source dedup feeding observer

**Files:**
- Create: `crates/fan-out-bench/tests/observer_integration.rs`

- [ ] **Step 1: Write integration test**

```rust
//! Integration: SS + YS entry streams → merger → observer → trigger events.

use crossbeam_channel::{bounded, unbounded};
use entry_sources::{EntryObservation, SignatureVec, SourceKind};
use fan_out_bench::counters::BenchCounters;
use fan_out_bench::merger::{spawn as spawn_merger, MergerConfig};
use fan_out_bench::observer::{spawn as spawn_observer, ObserverConfig, HASHES_PER_TICK};
use fan_out_bench::trigger::TriggerEvent;
use solana_sdk::hash::Hash;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn make_obs(slot: u64, entry_hash: Hash, num_hashes: u64, tx_count: u32, source: SourceKind) -> EntryObservation {
    EntryObservation {
        source,
        observed_at: Instant::now(),
        slot,
        entry_index: 0,
        num_hashes,
        entry_hash,
        tx_count,
        signatures: SignatureVec::new(),
        first_shred_at: None,
        leader: None,
    }
}

#[test]
fn ss_only_stream_drives_observer_to_fire_trigger() {
    // Setup: schedule expects (slot=100, tick=2)
    let mut schedule = HashSet::new();
    schedule.insert((100, 2));

    let (ss_tx, ss_rx) = unbounded();
    let (_ys_tx, ys_rx) = unbounded::<EntryObservation>();
    let (merged_tx, merged_rx) = bounded(100);
    let (trigger_tx, trigger_rx) = bounded::<TriggerEvent>(100);
    let stop = Arc::new(AtomicBool::new(false));
    let counters = Arc::new(BenchCounters::default());
    let current_slot = Arc::new(AtomicU64::new(0));

    let merger_handle = spawn_merger(MergerConfig {
        ss_rx, ys_rx, out_tx: merged_tx,
        pinned_core: None,
        counters: counters.clone(),
        stop: stop.clone(),
    }).unwrap();

    let observer_handle = spawn_observer(ObserverConfig {
        merged_rx,
        schedule: Arc::new(schedule),
        trigger_tx,
        current_slot,
        pinned_core: None,
        counters: counters.clone(),
        stop: stop.clone(),
    }).unwrap();

    // 2 tick-shaped entries → tick 2 → trigger
    ss_tx.send(make_obs(100, Hash::new_unique(), HASHES_PER_TICK, 0, SourceKind::ShredStream)).unwrap();
    ss_tx.send(make_obs(100, Hash::new_unique(), HASHES_PER_TICK, 0, SourceKind::ShredStream)).unwrap();

    std::thread::sleep(Duration::from_millis(100));
    let event = trigger_rx.try_recv().expect("expected trigger");
    assert_eq!(event.slot, 100);
    assert_eq!(event.tick, 2);

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    drop(ss_tx);
    let _ = merger_handle.join();
    let _ = observer_handle.join();
}

#[test]
fn dedup_when_both_sources_emit_same_entry_then_only_one_trigger() {
    let mut schedule = HashSet::new();
    schedule.insert((100, 1));

    let (ss_tx, ss_rx) = unbounded();
    let (ys_tx, ys_rx) = unbounded();
    let (merged_tx, merged_rx) = bounded(100);
    let (trigger_tx, trigger_rx) = bounded::<TriggerEvent>(100);
    let stop = Arc::new(AtomicBool::new(false));
    let counters = Arc::new(BenchCounters::default());
    let current_slot = Arc::new(AtomicU64::new(0));

    let merger_handle = spawn_merger(MergerConfig {
        ss_rx, ys_rx, out_tx: merged_tx,
        pinned_core: None,
        counters: counters.clone(),
        stop: stop.clone(),
    }).unwrap();

    let observer_handle = spawn_observer(ObserverConfig {
        merged_rx,
        schedule: Arc::new(schedule),
        trigger_tx,
        current_slot,
        pinned_core: None,
        counters: counters.clone(),
        stop: stop.clone(),
    }).unwrap();

    // SS and YS both emit the SAME entry — merger dedups, observer sees 1 tick
    let h = Hash::new_unique();
    ss_tx.send(make_obs(100, h, HASHES_PER_TICK, 0, SourceKind::ShredStream)).unwrap();
    ys_tx.send(make_obs(100, h, HASHES_PER_TICK, 0, SourceKind::Yellowstone)).unwrap();

    std::thread::sleep(Duration::from_millis(100));

    // Exactly one trigger
    assert!(trigger_rx.try_recv().is_ok());
    assert!(trigger_rx.try_recv().is_err(), "should not have second trigger");

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    drop(ss_tx);
    drop(ys_tx);
    let _ = merger_handle.join();
    let _ = observer_handle.join();
}
```

Run: `cargo test -p fan-out-bench --test observer_integration`. Expected: 2 tests pass.

---

## Task 7: Final verification + README

- [ ] **Step 1: Run full test suite**

Run: `cargo test -p fan-out-bench`
Expected: all Plan 1 + Plan 2 + Plan 3 tests pass.

- [ ] **Step 2: Clippy clean**

Run: `cargo clippy -p fan-out-bench --all-targets --no-deps -- -D warnings`
Expected: no warnings.

- [ ] **Step 3: Update README**

Modify `crates/fan-out-bench/README.md` — change `Plan 3:` line in "Not yet implemented" list to "Plan 3 — entry observation:" with checkmarks:

```markdown
Plan 3 — entry observation:
- ✅ TriggerEvent type
- ✅ EntryMerger (SS + YS dedup by (slot, entry_hash))
- ✅ Observer (PoH tick counter + schedule match + trigger emit)
- ✅ Integration test (mock stream → merger → observer → trigger)
```

And in "Not yet implemented" leave Plan 4-7 entries.

---

## Plan 3 done

Po tym planie mamy:
- Pełną warstwę obserwacji on-chain (SS + YS merged, dedup'owane)
- PoH tick tracking per slot
- TriggerEvent emit gdy schedule trafia

**Następny plan:** Plan 4 — first senders (Helius + Jito) + Matcher state machine + RPC fallback + runtime wiring. To pierwszy plan który podpina razem cały bench i daje uruchamialny smoke run na mainnet.
