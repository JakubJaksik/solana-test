# fan-out-bench — Plan 2: Nonce Infrastructure

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Zbudować pełną infrastrukturę durable nonce dla benchu: NonceManager state machine z RR allocatorem, parsing nonce account state, YS gRPC subscription dla live updates, RPC fallback dla stale nonces, plus dwa CLI binary (`setup-nonces` + `teardown-nonces`) do tworzenia i zwijania puli kont.

**Architecture:** Pure-logic NonceManager z explicit state machine (4 stany: Ready/InFlight/AwaitingUpdate/Stale), testowalny w izolacji z mock event'ami. YS subscription i RPC client to thin adapters które tłumaczą Solana events → state machine method calls.

**Tech Stack:** Rust 2024, solana-sdk 3.0, solana-client 3.1, yellowstone-grpc-client 12.2, tokio.

**Reference spec:** `docs/superpowers/specs/2026-05-14-fan-out-bench-design.md` §3.3, §4

**Previous plan:** `docs/superpowers/plans/2026-05-14-fan-out-bench-01-foundation.md`

---

## File structure (Plan 2 scope)

```
crates/fan-out-bench/
├── src/
│   ├── lib.rs                         — declare nonce + wallet modules
│   ├── wallet.rs                      — load keypair from JSON file
│   ├── nonce/
│   │   ├── mod.rs                     — module re-exports
│   │   ├── state.rs                   — parse Account data → NonceAccountState
│   │   ├── manager.rs                 — NonceManager state machine + RR allocator
│   │   ├── geyser_sub.rs              — YS gRPC subscription wrapper
│   │   ├── bootstrap.rs               — getMultipleAccounts initial blockhash fetch
│   │   └── rpc_poll.rs                — getAccountInfo fallback for Stale state
│   └── bin/
│       ├── setup_nonces.rs            — CLI: create N nonce accounts
│       └── teardown_nonces.rs         — CLI: withdraw rent from all nonce accounts
└── tests/
    └── nonce_integration.rs           — mock YS + mock RPC integration test
```

NOT in this plan (deferred to later plans):
- Entry sources (Plan 3)
- Observer / PoH tick tracking (Plan 3)
- Senders, Dispatcher, Matcher (Plan 4+)
- Runtime wiring everything together (Plan 4)

---

## Task 1: Module scaffolding

**Files:**
- Modify: `crates/fan-out-bench/src/lib.rs`
- Create: `crates/fan-out-bench/src/wallet.rs` (stub)
- Create: `crates/fan-out-bench/src/nonce/mod.rs`
- Create: `crates/fan-out-bench/src/nonce/{state,manager,geyser_sub,bootstrap,rpc_poll}.rs` (stubs)

- [ ] **Step 1: Update `lib.rs` to declare new modules**

Edit `crates/fan-out-bench/src/lib.rs` adding `nonce` and `wallet` modules:

```rust
pub mod attempt_state;
pub mod config;
pub mod counters;
pub mod memo;
pub mod nonce;
pub mod outcome;
pub mod pool;
pub mod schedule;
pub mod senders;
pub mod tip_accounts;
pub mod tx_builder;
pub mod wallet;
pub mod writer;
```

- [ ] **Step 2: Create stub files**

```bash
cd /home/jjaksik/Repos/my-scripts/crates/fan-out-bench/src
touch wallet.rs
mkdir -p nonce
touch nonce/mod.rs nonce/state.rs nonce/manager.rs nonce/geyser_sub.rs nonce/bootstrap.rs nonce/rpc_poll.rs
```

Write `crates/fan-out-bench/src/nonce/mod.rs`:

```rust
pub mod bootstrap;
pub mod geyser_sub;
pub mod manager;
pub mod rpc_poll;
pub mod state;
```

Put `// implementation in later task` in `wallet.rs`, `nonce/state.rs`, `nonce/manager.rs`, `nonce/geyser_sub.rs`, `nonce/bootstrap.rs`, `nonce/rpc_poll.rs`.

- [ ] **Step 3: Verify build**

Run: `cargo check -p fan-out-bench`
Expected: builds clean.

- [ ] **Step 4: NO git operations** — just leave files for user to commit.

---

## Task 2: Wallet keypair loader

**Files:**
- Replace stub: `crates/fan-out-bench/src/wallet.rs`

- [ ] **Step 1: Write wallet module**

Replace `crates/fan-out-bench/src/wallet.rs`:

```rust
//! Wallet keypair loader.
//!
//! Reads JSON-encoded Solana keypair file (`[u8; 64]` byte array).
//! Compatible with `solana-keygen` output and `~/.config/solana/id.json`.

use anyhow::Context;
use solana_sdk::signature::Keypair;
use std::path::Path;

pub fn load_keypair_file(path: &Path) -> anyhow::Result<Keypair> {
    let bytes = std::fs::read(path)
        .with_context(|| format!("failed to read keypair file: {}", path.display()))?;
    let bytes_str = std::str::from_utf8(&bytes)
        .with_context(|| format!("keypair file is not UTF-8: {}", path.display()))?;
    let secret_bytes: Vec<u8> = serde_json::from_str(bytes_str)
        .with_context(|| format!("keypair file is not valid JSON byte array: {}", path.display()))?;
    if secret_bytes.len() != 64 {
        anyhow::bail!(
            "keypair file has {} bytes, expected 64: {}",
            secret_bytes.len(),
            path.display()
        );
    }
    let secret: [u8; 64] = secret_bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("conversion to [u8; 64] failed"))?;
    Ok(Keypair::new_from_array(secret))
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::signature::Signer;
    use tempfile::NamedTempFile;
    use std::io::Write;

    #[test]
    fn loads_valid_keypair() {
        let kp = Keypair::new();
        let bytes = kp.to_bytes();
        let json = serde_json::to_string(&bytes.to_vec()).unwrap();

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(json.as_bytes()).unwrap();

        let loaded = load_keypair_file(file.path()).unwrap();
        assert_eq!(loaded.pubkey(), kp.pubkey());
    }

    #[test]
    fn rejects_wrong_length() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"[1, 2, 3]").unwrap();
        assert!(load_keypair_file(file.path()).is_err());
    }

    #[test]
    fn rejects_non_json() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"not json").unwrap();
        assert!(load_keypair_file(file.path()).is_err());
    }

    #[test]
    fn rejects_missing_file() {
        let path = std::path::Path::new("/nonexistent/path/keypair.json");
        assert!(load_keypair_file(path).is_err());
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p fan-out-bench --lib wallet`
Expected: 4 tests pass.

---

## Task 3: Nonce account state parsing

**Files:**
- Replace stub: `crates/fan-out-bench/src/nonce/state.rs`

- [ ] **Step 1: Write nonce state parser**

Nonce account state on Solana is 80 bytes: serialized using bincode. We use `solana_sdk::nonce::state::Versions` or its newer split. Use whatever is available — likely a separate `solana-nonce` crate. If that fails to compile, **ASK** — don't substitute.

Replace `crates/fan-out-bench/src/nonce/state.rs`:

```rust
//! Parse Solana nonce account state.
//!
//! Nonce account data is bincode-serialized `nonce::state::Versions` (80 bytes).
//! We extract authority pubkey + current blockhash for our bench cache.

use solana_sdk::{
    hash::Hash,
    pubkey::Pubkey,
};

/// Parsed nonce account state — only the fields the bench cares about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NonceAccountState {
    pub authority: Pubkey,
    pub blockhash: Hash,
}

#[derive(Debug, thiserror::Error)]
pub enum NonceParseError {
    #[error("account data too short: {0} bytes (expected 80)")]
    TooShort(usize),
    #[error("nonce state is Uninitialized")]
    Uninitialized,
    #[error("bincode deserialization failed: {0}")]
    Bincode(String),
}

/// Parse a `solana_sdk::account::Account.data` slice as nonce state.
pub fn parse_nonce_account_data(data: &[u8]) -> Result<NonceAccountState, NonceParseError> {
    if data.len() < 80 {
        return Err(NonceParseError::TooShort(data.len()));
    }
    // Layout: 4 bytes version + 4 bytes state enum + 32 bytes authority + 32 bytes blockhash + 8 bytes fee_calc
    // State enum: Uninitialized = 0, Initialized = 1 (variant index as u32 LE)
    let state_disc = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);
    if state_disc == 0 {
        return Err(NonceParseError::Uninitialized);
    }
    if state_disc != 1 {
        return Err(NonceParseError::Bincode(format!(
            "unexpected state discriminant: {}",
            state_disc
        )));
    }
    let authority_bytes: [u8; 32] = data[8..40].try_into().unwrap();
    let blockhash_bytes: [u8; 32] = data[40..72].try_into().unwrap();
    Ok(NonceAccountState {
        authority: Pubkey::from(authority_bytes),
        blockhash: Hash::from(blockhash_bytes),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_initialized_data(authority: [u8; 32], blockhash: [u8; 32]) -> Vec<u8> {
        let mut data = vec![0u8; 80];
        // version = 0 (Current variant of Versions enum); state index = 1 (Initialized)
        data[0..4].copy_from_slice(&0u32.to_le_bytes());
        data[4..8].copy_from_slice(&1u32.to_le_bytes());
        data[8..40].copy_from_slice(&authority);
        data[40..72].copy_from_slice(&blockhash);
        // fee_calc = 0
        data
    }

    fn make_uninitialized_data() -> Vec<u8> {
        let mut data = vec![0u8; 80];
        data[0..4].copy_from_slice(&0u32.to_le_bytes());
        data[4..8].copy_from_slice(&0u32.to_le_bytes()); // Uninitialized
        data
    }

    #[test]
    fn parses_initialized_state() {
        let auth = [11u8; 32];
        let blockhash = [22u8; 32];
        let data = make_initialized_data(auth, blockhash);
        let state = parse_nonce_account_data(&data).unwrap();
        assert_eq!(state.authority.to_bytes(), auth);
        assert_eq!(state.blockhash.to_bytes(), blockhash);
    }

    #[test]
    fn rejects_uninitialized() {
        let data = make_uninitialized_data();
        assert!(matches!(parse_nonce_account_data(&data), Err(NonceParseError::Uninitialized)));
    }

    #[test]
    fn rejects_too_short() {
        let data = vec![0u8; 50];
        assert!(matches!(parse_nonce_account_data(&data), Err(NonceParseError::TooShort(50))));
    }

    #[test]
    fn rejects_unknown_state_discriminant() {
        let mut data = vec![0u8; 80];
        data[4..8].copy_from_slice(&99u32.to_le_bytes());
        assert!(matches!(parse_nonce_account_data(&data), Err(NonceParseError::Bincode(_))));
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p fan-out-bench --lib nonce::state`
Expected: 4 tests pass.

---

## Task 4: NonceManager core types + take_ready (RR allocator)

**Files:**
- Replace stub: `crates/fan-out-bench/src/nonce/manager.rs`

- [ ] **Step 1: Write NonceManager core**

Replace `crates/fan-out-bench/src/nonce/manager.rs`:

```rust
//! NonceManager — durable nonce pool state machine.
//!
//! See spec §7.1 — state machine: Ready → InFlight → AwaitingUpdate → Ready,
//! with fallback to Stale on timeout. RR allocator.

use parking_lot::RwLock;
use solana_sdk::{hash::Hash, pubkey::Pubkey};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

pub type NonceId = u16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NonceState {
    /// Cache fresh, no in-flight trigger, available to allocator.
    Ready { blockhash: Hash },
    /// Variants dispatched, awaiting either landing observation or deadline.
    InFlight { blockhash_used: Hash, since: Instant },
    /// Landing observed; waiting for YS account update with new blockhash.
    AwaitingUpdate { blockhash_used: Hash, since: Instant },
    /// Timeout without landing or update — RPC fallback should re-fetch.
    Stale { blockhash_used: Hash, since: Instant },
}

impl NonceState {
    pub fn is_ready(&self) -> bool {
        matches!(self, NonceState::Ready { .. })
    }
}

pub struct NonceEntry {
    pub id: NonceId,
    pub pubkey: Pubkey,
    state: RwLock<NonceState>,
}

impl NonceEntry {
    pub fn new(id: NonceId, pubkey: Pubkey, blockhash: Hash) -> Self {
        Self {
            id,
            pubkey,
            state: RwLock::new(NonceState::Ready { blockhash }),
        }
    }

    pub fn state(&self) -> NonceState {
        *self.state.read()
    }

    pub fn set_state(&self, new_state: NonceState) {
        *self.state.write() = new_state;
    }
}

pub struct NonceManager {
    entries: Vec<Arc<NonceEntry>>,
    /// pubkey → index lookup
    pubkey_index: std::collections::HashMap<Pubkey, usize>,
    /// RR cursor for take_ready
    rr_cursor: AtomicUsize,
}

impl NonceManager {
    pub fn new(entries: Vec<(NonceId, Pubkey, Hash)>) -> Self {
        let pubkey_index = entries
            .iter()
            .enumerate()
            .map(|(idx, (_, pk, _))| (*pk, idx))
            .collect();
        let entries = entries
            .into_iter()
            .map(|(id, pk, bh)| Arc::new(NonceEntry::new(id, pk, bh)))
            .collect();
        Self {
            entries,
            pubkey_index,
            rr_cursor: AtomicUsize::new(0),
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Atomically find a Ready nonce, transition it to InFlight, return (id, blockhash).
    /// Uses RR rotation starting from current cursor. Returns None if no Ready nonce.
    pub fn take_ready(&self) -> Option<(NonceId, Pubkey, Hash)> {
        let n = self.entries.len();
        if n == 0 {
            return None;
        }
        let start = self.rr_cursor.fetch_add(1, Ordering::Relaxed) % n;
        for offset in 0..n {
            let idx = (start + offset) % n;
            let entry = &self.entries[idx];
            let mut guard = entry.state.write();
            if let NonceState::Ready { blockhash } = *guard {
                *guard = NonceState::InFlight {
                    blockhash_used: blockhash,
                    since: Instant::now(),
                };
                return Some((entry.id, entry.pubkey, blockhash));
            }
        }
        None
    }

    pub fn entries(&self) -> &[Arc<NonceEntry>] {
        &self.entries
    }

    pub fn get_by_pubkey(&self, pubkey: &Pubkey) -> Option<&Arc<NonceEntry>> {
        self.pubkey_index.get(pubkey).map(|&idx| &self.entries[idx])
    }

    pub fn get_by_id(&self, id: NonceId) -> Option<&Arc<NonceEntry>> {
        self.entries.iter().find(|e| e.id == id)
    }

    pub fn count_in_state(&self, predicate: impl Fn(&NonceState) -> bool) -> usize {
        self.entries.iter().filter(|e| predicate(&e.state())).count()
    }

    pub fn ready_count(&self) -> usize {
        self.count_in_state(|s| s.is_ready())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_manager(n: usize) -> NonceManager {
        let entries: Vec<_> = (0..n)
            .map(|i| (i as NonceId, Pubkey::new_unique(), Hash::new_unique()))
            .collect();
        NonceManager::new(entries)
    }

    #[test]
    fn empty_manager_returns_none() {
        let manager = NonceManager::new(vec![]);
        assert!(manager.is_empty());
        assert!(manager.take_ready().is_none());
    }

    #[test]
    fn take_ready_transitions_to_in_flight() {
        let manager = make_manager(3);
        let (id, _, _) = manager.take_ready().unwrap();
        let entry = manager.get_by_id(id).unwrap();
        assert!(matches!(entry.state(), NonceState::InFlight { .. }));
    }

    #[test]
    fn take_ready_returns_none_when_all_in_flight() {
        let manager = make_manager(2);
        manager.take_ready().unwrap();
        manager.take_ready().unwrap();
        assert!(manager.take_ready().is_none());
    }

    #[test]
    fn ready_count_decreases_on_take() {
        let manager = make_manager(5);
        assert_eq!(manager.ready_count(), 5);
        manager.take_ready().unwrap();
        assert_eq!(manager.ready_count(), 4);
        manager.take_ready().unwrap();
        assert_eq!(manager.ready_count(), 3);
    }

    #[test]
    fn rr_rotation_uses_different_nonces() {
        let manager = make_manager(3);
        let mut ids = std::collections::HashSet::new();
        for _ in 0..3 {
            let (id, _, _) = manager.take_ready().unwrap();
            ids.insert(id);
        }
        // All 3 distinct nonces should have been used
        assert_eq!(ids.len(), 3);
    }

    #[test]
    fn get_by_pubkey_finds_entry() {
        let pk = Pubkey::new_unique();
        let bh = Hash::new_unique();
        let manager = NonceManager::new(vec![(42, pk, bh)]);
        let entry = manager.get_by_pubkey(&pk).unwrap();
        assert_eq!(entry.id, 42);
    }

    #[test]
    fn get_by_pubkey_returns_none_for_unknown() {
        let manager = make_manager(3);
        assert!(manager.get_by_pubkey(&Pubkey::new_unique()).is_none());
    }
}
```

- [ ] **Step 2: Add `parking_lot` dep**

Edit `crates/fan-out-bench/Cargo.toml`, add to `[dependencies]`:

```toml
parking_lot = "0.12"
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p fan-out-bench --lib nonce::manager`
Expected: 7 tests pass.

---

## Task 5: NonceManager state transitions

**Files:**
- Modify: `crates/fan-out-bench/src/nonce/manager.rs` (extend)

- [ ] **Step 1: Add transition methods + tests**

Append to `crates/fan-out-bench/src/nonce/manager.rs` (before the closing `}` of `impl NonceManager`):

```rust
    /// Called by Geyser/YS subscription when nonce account state changes on chain.
    /// If observed blockhash differs from cached → nonce advanced → transition to Ready.
    pub fn on_account_update(&self, pubkey: &Pubkey, new_blockhash: Hash) -> bool {
        let entry = match self.get_by_pubkey(pubkey) {
            Some(e) => e,
            None => return false,
        };
        let mut guard = entry.state.write();
        let advanced = match *guard {
            NonceState::Ready { blockhash } => blockhash != new_blockhash,
            NonceState::InFlight { blockhash_used, .. }
            | NonceState::AwaitingUpdate { blockhash_used, .. }
            | NonceState::Stale { blockhash_used, .. } => blockhash_used != new_blockhash,
        };
        if advanced {
            *guard = NonceState::Ready {
                blockhash: new_blockhash,
            };
        }
        advanced
    }

    /// Called by matcher when ANY sibling sig from a trigger is observed landed.
    /// Transitions InFlight → AwaitingUpdate.
    pub fn on_observed_landing(&self, nonce_id: NonceId) {
        let entry = match self.get_by_id(nonce_id) {
            Some(e) => e,
            None => return,
        };
        let mut guard = entry.state.write();
        if let NonceState::InFlight { blockhash_used, .. } = *guard {
            *guard = NonceState::AwaitingUpdate {
                blockhash_used,
                since: Instant::now(),
            };
        }
    }

    /// Sweep entries past deadline. Returns list of pubkeys now in Stale state
    /// (caller responsibility: hand to RPC fallback poller).
    pub fn tick_timeouts(
        &self,
        in_flight_deadline: std::time::Duration,
        awaiting_update_deadline: std::time::Duration,
    ) -> Vec<(NonceId, Pubkey)> {
        let now = Instant::now();
        let mut stale_now: Vec<(NonceId, Pubkey)> = Vec::new();
        for entry in &self.entries {
            let mut guard = entry.state.write();
            let became_stale = match *guard {
                NonceState::InFlight { blockhash_used, since }
                    if now.duration_since(since) >= in_flight_deadline =>
                {
                    *guard = NonceState::Stale {
                        blockhash_used,
                        since: now,
                    };
                    true
                }
                NonceState::AwaitingUpdate { blockhash_used, since }
                    if now.duration_since(since) >= awaiting_update_deadline =>
                {
                    *guard = NonceState::Stale {
                        blockhash_used,
                        since: now,
                    };
                    true
                }
                _ => false,
            };
            if became_stale {
                stale_now.push((entry.id, entry.pubkey));
            }
        }
        stale_now
    }

    /// Called by RPC fallback after re-fetching account state.
    /// If observed blockhash matches cached → nonce wasn't advanced → still safe to reuse.
    /// If differs → advance happened → transition to Ready.
    pub fn on_fallback_refresh(&self, pubkey: &Pubkey, observed_blockhash: Hash) {
        let entry = match self.get_by_pubkey(pubkey) {
            Some(e) => e,
            None => return,
        };
        let mut guard = entry.state.write();
        match *guard {
            NonceState::Stale { blockhash_used, .. } => {
                if blockhash_used == observed_blockhash {
                    // Nonce never advanced — variants all failed; safe to reuse same blockhash
                    *guard = NonceState::Ready {
                        blockhash: blockhash_used,
                    };
                } else {
                    // Nonce advanced — use new blockhash
                    *guard = NonceState::Ready {
                        blockhash: observed_blockhash,
                    };
                }
            }
            _ => {
                // Only touch Stale entries via this path
            }
        }
    }
```

Append to the `mod tests` block (before final `}`):

```rust
    use std::time::Duration;

    #[test]
    fn on_account_update_advances_to_ready() {
        let pk = Pubkey::new_unique();
        let bh1 = Hash::new_unique();
        let bh2 = Hash::new_unique();
        let manager = NonceManager::new(vec![(0, pk, bh1)]);
        // First take, marking InFlight
        manager.take_ready().unwrap();
        // Now an update arrives with new blockhash
        let advanced = manager.on_account_update(&pk, bh2);
        assert!(advanced);
        let entry = manager.get_by_id(0).unwrap();
        match entry.state() {
            NonceState::Ready { blockhash } => assert_eq!(blockhash, bh2),
            other => panic!("expected Ready, got {:?}", other),
        }
    }

    #[test]
    fn on_account_update_same_blockhash_no_advance() {
        let pk = Pubkey::new_unique();
        let bh = Hash::new_unique();
        let manager = NonceManager::new(vec![(0, pk, bh)]);
        manager.take_ready().unwrap();
        // Update with SAME blockhash (Geyser re-sent same state)
        let advanced = manager.on_account_update(&pk, bh);
        assert!(!advanced);
        // Should still be InFlight
        assert!(matches!(manager.get_by_id(0).unwrap().state(), NonceState::InFlight { .. }));
    }

    #[test]
    fn on_observed_landing_transitions_to_awaiting_update() {
        let manager = make_manager(1);
        let (id, _, _) = manager.take_ready().unwrap();
        manager.on_observed_landing(id);
        assert!(matches!(
            manager.get_by_id(id).unwrap().state(),
            NonceState::AwaitingUpdate { .. }
        ));
    }

    #[test]
    fn tick_timeouts_moves_in_flight_to_stale() {
        let manager = make_manager(2);
        manager.take_ready().unwrap();
        manager.take_ready().unwrap();
        // Sleep > deadline
        std::thread::sleep(Duration::from_millis(20));
        let stale = manager.tick_timeouts(Duration::from_millis(10), Duration::from_secs(5));
        assert_eq!(stale.len(), 2);
        for entry in manager.entries() {
            assert!(matches!(entry.state(), NonceState::Stale { .. }));
        }
    }

    #[test]
    fn on_fallback_refresh_same_blockhash_returns_to_ready_same_value() {
        let pk = Pubkey::new_unique();
        let bh = Hash::new_unique();
        let manager = NonceManager::new(vec![(0, pk, bh)]);
        manager.take_ready().unwrap();
        std::thread::sleep(Duration::from_millis(20));
        manager.tick_timeouts(Duration::from_millis(10), Duration::from_secs(5));
        manager.on_fallback_refresh(&pk, bh);
        match manager.get_by_id(0).unwrap().state() {
            NonceState::Ready { blockhash } => assert_eq!(blockhash, bh),
            other => panic!("expected Ready with same bh, got {:?}", other),
        }
    }

    #[test]
    fn on_fallback_refresh_new_blockhash_returns_to_ready_new_value() {
        let pk = Pubkey::new_unique();
        let bh1 = Hash::new_unique();
        let bh2 = Hash::new_unique();
        let manager = NonceManager::new(vec![(0, pk, bh1)]);
        manager.take_ready().unwrap();
        std::thread::sleep(Duration::from_millis(20));
        manager.tick_timeouts(Duration::from_millis(10), Duration::from_secs(5));
        manager.on_fallback_refresh(&pk, bh2);
        match manager.get_by_id(0).unwrap().state() {
            NonceState::Ready { blockhash } => assert_eq!(blockhash, bh2),
            other => panic!("expected Ready with new bh, got {:?}", other),
        }
    }
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p fan-out-bench --lib nonce::manager`
Expected: 13 tests pass (7 from Task 4 + 6 new).

---

## Task 6: setup-nonces binary

**Files:**
- Create: `crates/fan-out-bench/src/bin/setup_nonces.rs`

- [ ] **Step 1: Write binary**

Create `crates/fan-out-bench/src/bin/setup_nonces.rs`:

```rust
//! CLI: create N durable nonce accounts.
//!
//! Generates N fresh keypairs, batches `create_nonce_account` instructions
//! ~10 per tx (limited by 1232-byte tx size), sends via Helius RPC, verifies,
//! and saves both keypair file and config file.
//!
//! Usage:
//!   cargo run --bin setup-nonces -- \
//!     --rpc-url <URL> \
//!     --wallet ~/.config/solana/dex-bench.json \
//!     --count 150 \
//!     --output-keypairs nonce-keypairs.json \
//!     --output-config nonce-config.json

use anyhow::{Context, Result};
use clap::Parser;
use fan_out_bench::wallet::load_keypair_file;
use serde::{Deserialize, Serialize};
use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    signature::{Keypair, Signature, Signer},
    transaction::Transaction,
};
use solana_system_interface::instruction as sys_instruction;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "setup-nonces")]
struct Args {
    #[arg(long)]
    rpc_url: String,
    #[arg(long)]
    wallet: PathBuf,
    #[arg(long, default_value = "150")]
    count: usize,
    #[arg(long)]
    output_keypairs: PathBuf,
    #[arg(long)]
    output_config: PathBuf,
    #[arg(long, default_value = "10")]
    batch_size: usize,
}

#[derive(Serialize, Deserialize)]
struct NonceKeypairsFile {
    keypairs_base58: Vec<String>,
}

#[derive(Serialize, Deserialize)]
struct NonceConfigFile {
    accounts: Vec<NonceConfigEntry>,
}

#[derive(Serialize, Deserialize)]
struct NonceConfigEntry {
    id: u16,
    pubkey: String,
    initial_blockhash: String,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let authority = load_keypair_file(&args.wallet).context("load wallet")?;
    tracing::info!(authority = %authority.pubkey(), count = args.count, "setting up nonces");

    let client = RpcClient::new_with_commitment(args.rpc_url.clone(), CommitmentConfig::confirmed());
    let rent_lamports = client
        .get_minimum_balance_for_rent_exemption(80)
        .context("fetch rent exemption")?;
    tracing::info!(rent_lamports, "nonce rent");

    // Generate keypairs
    let mut nonce_kps: Vec<Keypair> = (0..args.count).map(|_| Keypair::new()).collect();

    // Save keypair file BEFORE sending tx (so we can teardown even if send fails partway)
    let kp_file = NonceKeypairsFile {
        keypairs_base58: nonce_kps
            .iter()
            .map(|kp| bs58::encode(kp.to_bytes()).into_string())
            .collect(),
    };
    std::fs::write(
        &args.output_keypairs,
        serde_json::to_string_pretty(&kp_file)?,
    )?;
    // chmod 600
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&args.output_keypairs, std::fs::Permissions::from_mode(0o600))?;
    }
    tracing::info!(path = ?args.output_keypairs, "keypairs saved");

    // Send creates in batches
    let mut signatures: Vec<Signature> = Vec::new();
    for (batch_idx, chunk) in nonce_kps.chunks(args.batch_size).enumerate() {
        let mut ixs = Vec::new();
        for kp in chunk {
            ixs.extend(sys_instruction::create_nonce_account(
                &authority.pubkey(),
                &kp.pubkey(),
                &authority.pubkey(),
                rent_lamports,
            ));
        }
        let blockhash = client.get_latest_blockhash().context("fetch blockhash")?;
        let mut signers: Vec<&Keypair> = vec![&authority];
        signers.extend(chunk.iter());
        let tx = Transaction::new_signed_with_payer(
            &ixs,
            Some(&authority.pubkey()),
            &signers,
            blockhash,
        );
        let sig = client
            .send_and_confirm_transaction(&tx)
            .with_context(|| format!("batch {} create_nonce_account failed", batch_idx))?;
        tracing::info!(batch_idx, sig = %sig, count = chunk.len(), "batch confirmed");
        signatures.push(sig);
    }

    // Verify each account is Initialized + cache initial blockhash
    let mut entries: Vec<NonceConfigEntry> = Vec::with_capacity(args.count);
    for (idx, kp) in nonce_kps.drain(..).enumerate() {
        let account = client
            .get_account(&kp.pubkey())
            .with_context(|| format!("get_account for nonce {}", kp.pubkey()))?;
        let state = fan_out_bench::nonce::state::parse_nonce_account_data(&account.data)
            .with_context(|| format!("parse nonce {} data", kp.pubkey()))?;
        if state.authority != authority.pubkey() {
            anyhow::bail!(
                "nonce {} authority mismatch: got {}, expected {}",
                kp.pubkey(),
                state.authority,
                authority.pubkey()
            );
        }
        entries.push(NonceConfigEntry {
            id: idx as u16,
            pubkey: kp.pubkey().to_string(),
            initial_blockhash: state.blockhash.to_string(),
        });
    }

    let config_file = NonceConfigFile { accounts: entries };
    std::fs::write(
        &args.output_config,
        serde_json::to_string_pretty(&config_file)?,
    )?;
    tracing::info!(path = ?args.output_config, count = args.count, "config saved");

    Ok(())
}
```

- [ ] **Step 2: Verify it builds**

Run: `cargo check --bin setup-nonces -p fan-out-bench`
Expected: builds clean. Note: this binary is for manual execution on devnet/mainnet, not unit-tested.

- [ ] **Step 3: Document the binary**

No manual run in this step — user runs it later when ready to set up the pool.

---

## Task 7: teardown-nonces binary

**Files:**
- Create: `crates/fan-out-bench/src/bin/teardown_nonces.rs`

- [ ] **Step 1: Write binary**

Create `crates/fan-out-bench/src/bin/teardown_nonces.rs`:

```rust
//! CLI: withdraw rent from all nonce accounts back to authority.
//!
//! Reads keypairs file from setup-nonces, sends withdraw_nonce_account
//! in batches, confirms.
//!
//! Usage:
//!   cargo run --bin teardown-nonces -- \
//!     --rpc-url <URL> \
//!     --wallet ~/.config/solana/dex-bench.json \
//!     --keypairs nonce-keypairs.json

use anyhow::{Context, Result};
use clap::Parser;
use fan_out_bench::wallet::load_keypair_file;
use serde::{Deserialize, Serialize};
use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    signature::{Keypair, Signer},
    transaction::Transaction,
};
use solana_system_interface::instruction as sys_instruction;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "teardown-nonces")]
struct Args {
    #[arg(long)]
    rpc_url: String,
    #[arg(long)]
    wallet: PathBuf,
    #[arg(long)]
    keypairs: PathBuf,
    #[arg(long, default_value = "15")]
    batch_size: usize,
}

#[derive(Serialize, Deserialize)]
struct NonceKeypairsFile {
    keypairs_base58: Vec<String>,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let authority = load_keypair_file(&args.wallet).context("load wallet")?;
    let kp_file_bytes = std::fs::read(&args.keypairs).context("read keypairs file")?;
    let kp_file: NonceKeypairsFile = serde_json::from_slice(&kp_file_bytes).context("parse keypairs file")?;

    let nonce_kps: Vec<Keypair> = kp_file
        .keypairs_base58
        .iter()
        .map(|s| {
            let bytes = bs58::decode(s)
                .into_vec()
                .context("decode keypair base58")?;
            let arr: [u8; 64] = bytes
                .try_into()
                .map_err(|_| anyhow::anyhow!("keypair bytes not 64 long"))?;
            Ok(Keypair::new_from_array(arr))
        })
        .collect::<Result<Vec<_>>>()?;

    tracing::info!(count = nonce_kps.len(), "withdrawing nonces");
    let client = RpcClient::new_with_commitment(args.rpc_url.clone(), CommitmentConfig::confirmed());
    let rent_lamports = client.get_minimum_balance_for_rent_exemption(80)?;

    for (batch_idx, chunk) in nonce_kps.chunks(args.batch_size).enumerate() {
        let ixs: Vec<_> = chunk
            .iter()
            .map(|kp| {
                sys_instruction::withdraw_nonce_account(
                    &kp.pubkey(),
                    &authority.pubkey(),
                    &authority.pubkey(),
                    rent_lamports,
                )
            })
            .collect();
        let blockhash = client.get_latest_blockhash()?;
        let tx = Transaction::new_signed_with_payer(
            &ixs,
            Some(&authority.pubkey()),
            &[&authority],
            blockhash,
        );
        let sig = client
            .send_and_confirm_transaction(&tx)
            .with_context(|| format!("batch {} withdraw failed", batch_idx))?;
        tracing::info!(batch_idx, sig = %sig, count = chunk.len(), "batch confirmed");
    }
    tracing::info!("teardown complete");
    Ok(())
}
```

- [ ] **Step 2: Verify it builds**

Run: `cargo check --bin teardown-nonces -p fan-out-bench`
Expected: builds clean.

---

## Task 8: Bootstrap helper (getMultipleAccounts)

**Files:**
- Replace stub: `crates/fan-out-bench/src/nonce/bootstrap.rs`

- [ ] **Step 1: Write bootstrap function**

Replace `crates/fan-out-bench/src/nonce/bootstrap.rs`:

```rust
//! Bootstrap nonce manager state from RPC at startup.
//!
//! Reads `nonce-config.json` (created by `setup-nonces`), fetches current
//! account state via `getMultipleAccounts`, validates each is Initialized
//! with correct authority, returns `Vec<(NonceId, Pubkey, Hash)>` ready for
//! `NonceManager::new()`.

use crate::nonce::manager::NonceId;
use crate::nonce::state::parse_nonce_account_data;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use solana_client::rpc_client::RpcClient;
use solana_sdk::{hash::Hash, pubkey::Pubkey};
use std::path::Path;
use std::str::FromStr;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NonceConfigFile {
    pub accounts: Vec<NonceConfigEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NonceConfigEntry {
    pub id: u16,
    pub pubkey: String,
    pub initial_blockhash: String,
}

/// Load `nonce-config.json` then call `getMultipleAccounts` to fetch CURRENT
/// blockhashes (config has stale ones from setup time). Returns entries ready
/// for `NonceManager::new()`.
pub fn bootstrap(
    rpc: &RpcClient,
    config_path: &Path,
    expected_authority: &Pubkey,
) -> Result<Vec<(NonceId, Pubkey, Hash)>> {
    let bytes = std::fs::read(config_path)
        .with_context(|| format!("read nonce config: {}", config_path.display()))?;
    let config: NonceConfigFile = serde_json::from_slice(&bytes).context("parse nonce config")?;

    let pubkeys: Vec<Pubkey> = config
        .accounts
        .iter()
        .map(|e| Pubkey::from_str(&e.pubkey).context("invalid pubkey in nonce config"))
        .collect::<Result<_>>()?;

    // RPC limit is 100 per call for getMultipleAccounts
    let mut accounts = Vec::with_capacity(pubkeys.len());
    for chunk in pubkeys.chunks(100) {
        let batch = rpc
            .get_multiple_accounts(chunk)
            .context("getMultipleAccounts")?;
        accounts.extend(batch);
    }

    let mut result = Vec::with_capacity(config.accounts.len());
    for (entry, acc_opt) in config.accounts.iter().zip(accounts.iter()) {
        let acc = acc_opt
            .as_ref()
            .with_context(|| format!("nonce account {} does not exist on chain", entry.pubkey))?;
        let state = parse_nonce_account_data(&acc.data)
            .with_context(|| format!("parse nonce {}", entry.pubkey))?;
        if state.authority != *expected_authority {
            anyhow::bail!(
                "nonce {} authority mismatch: got {}, expected {}",
                entry.pubkey,
                state.authority,
                expected_authority
            );
        }
        let pubkey = Pubkey::from_str(&entry.pubkey).unwrap();
        result.push((entry.id, pubkey, state.blockhash));
    }
    Ok(result)
}
```

- [ ] **Step 2: Verify it builds (no unit test — needs real RPC)**

Run: `cargo check -p fan-out-bench`
Expected: builds clean.

---

## Task 9: RPC fallback poller

**Files:**
- Replace stub: `crates/fan-out-bench/src/nonce/rpc_poll.rs`

- [ ] **Step 1: Write fallback poller**

Replace `crates/fan-out-bench/src/nonce/rpc_poll.rs`:

```rust
//! RPC fallback poller for Stale nonces.
//!
//! When NonceManager flags a nonce as Stale (no YS update after deadline),
//! we poll `getAccountInfo` to re-fetch current state and unblock the entry.

use crate::nonce::manager::NonceManager;
use crate::nonce::state::parse_nonce_account_data;
use anyhow::Result;
use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;
use std::time::Duration;

pub struct RpcPollerConfig {
    pub rpc: Arc<RpcClient>,
    pub manager: Arc<NonceManager>,
    pub poll_interval: Duration,
    pub in_flight_deadline: Duration,
    pub awaiting_update_deadline: Duration,
    pub stop: Arc<std::sync::atomic::AtomicBool>,
}

pub fn spawn(cfg: RpcPollerConfig) -> std::io::Result<std::thread::JoinHandle<()>> {
    std::thread::Builder::new()
        .name("nonce-rpc-poller".into())
        .spawn(move || run_loop(cfg))
}

fn run_loop(cfg: RpcPollerConfig) {
    while !cfg.stop.load(std::sync::atomic::Ordering::Relaxed) {
        // Mark new stale entries
        let new_stale = cfg.manager.tick_timeouts(cfg.in_flight_deadline, cfg.awaiting_update_deadline);
        if !new_stale.is_empty() {
            tracing::warn!(count = new_stale.len(), "nonces became stale, refreshing via RPC");
        }

        // Refresh stale entries
        let stale_pubkeys: Vec<Pubkey> = cfg
            .manager
            .entries()
            .iter()
            .filter(|e| matches!(e.state(), crate::nonce::manager::NonceState::Stale { .. }))
            .map(|e| e.pubkey)
            .collect();

        if !stale_pubkeys.is_empty() {
            match refresh_batch(&cfg.rpc, &cfg.manager, &stale_pubkeys) {
                Ok(refreshed) => tracing::info!(refreshed, "rpc fallback refreshed stale nonces"),
                Err(e) => tracing::error!(error = %e, "rpc fallback batch failed"),
            }
        }
        std::thread::sleep(cfg.poll_interval);
    }
}

fn refresh_batch(rpc: &RpcClient, manager: &NonceManager, pubkeys: &[Pubkey]) -> Result<usize> {
    let mut refreshed = 0;
    for chunk in pubkeys.chunks(100) {
        let accounts = rpc.get_multiple_accounts(chunk)?;
        for (pk, acc_opt) in chunk.iter().zip(accounts.iter()) {
            if let Some(acc) = acc_opt {
                if let Ok(state) = parse_nonce_account_data(&acc.data) {
                    manager.on_fallback_refresh(pk, state.blockhash);
                    refreshed += 1;
                }
            }
        }
    }
    Ok(refreshed)
}
```

- [ ] **Step 2: Verify it builds**

Run: `cargo check -p fan-out-bench`
Expected: clean.

---

## Task 10: YS gRPC subscription wrapper

**Files:**
- Replace stub: `crates/fan-out-bench/src/nonce/geyser_sub.rs`

- [ ] **Step 1: Look at how entry-sources uses yellowstone-grpc-client**

The pattern is in `crates/entry-sources/src/yellowstone.rs`. Read it to understand:
- Connection setup (`GeyserGrpcClient::build_from_shared`)
- `SubscribeRequest` construction
- Stream handling

**Note for implementer:** if APIs differ from this skeleton, **ASK** — don't guess.

- [ ] **Step 2: Write YS subscription wrapper**

Replace `crates/fan-out-bench/src/nonce/geyser_sub.rs`:

```rust
//! Yellowstone gRPC subscription for nonce account updates.
//!
//! Subscribes to a fixed list of nonce account pubkeys, parses each update
//! event as nonce state, calls `NonceManager::on_account_update` for each.

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
```

- [ ] **Step 3: Verify it builds**

Run: `cargo check -p fan-out-bench`
Expected: builds clean. If yellowstone-grpc-proto fields differ (e.g., `nonempty_txn_signature` vs different field name), **ASK** the controller before changing.

---

## Task 11: Integration test (mock event sequences)

**Files:**
- Create: `crates/fan-out-bench/tests/nonce_integration.rs`

- [ ] **Step 1: Write integration test simulating typical bench cycle**

Create `crates/fan-out-bench/tests/nonce_integration.rs`:

```rust
//! Integration test for nonce subsystem state machine.
//!
//! Simulates a typical bench cycle: take_ready → variants in-flight →
//! landing observed → YS update → back to ready. Plus stale + fallback path.

use fan_out_bench::nonce::manager::{NonceManager, NonceState};
use solana_sdk::{hash::Hash, pubkey::Pubkey};
use std::time::Duration;

#[test]
fn full_cycle_take_observe_update_returns_ready() {
    let pk = Pubkey::new_unique();
    let bh1 = Hash::new_unique();
    let bh2 = Hash::new_unique();
    let manager = NonceManager::new(vec![(0, pk, bh1)]);

    // 1. Take ready → InFlight
    let (id, pubkey_out, blockhash_out) = manager.take_ready().unwrap();
    assert_eq!(id, 0);
    assert_eq!(pubkey_out, pk);
    assert_eq!(blockhash_out, bh1);
    assert!(matches!(manager.get_by_id(0).unwrap().state(), NonceState::InFlight { .. }));

    // 2. Matcher observes landing → AwaitingUpdate
    manager.on_observed_landing(0);
    assert!(matches!(
        manager.get_by_id(0).unwrap().state(),
        NonceState::AwaitingUpdate { .. }
    ));

    // 3. YS update arrives with new blockhash → Ready
    let advanced = manager.on_account_update(&pk, bh2);
    assert!(advanced);
    assert!(matches!(
        manager.get_by_id(0).unwrap().state(),
        NonceState::Ready { .. }
    ));

    // 4. Take again
    let (_id2, _pk2, bh2_out) = manager.take_ready().unwrap();
    assert_eq!(bh2_out, bh2);
}

#[test]
fn stale_recovery_via_fallback() {
    let pk = Pubkey::new_unique();
    let bh1 = Hash::new_unique();
    let bh2 = Hash::new_unique();
    let manager = NonceManager::new(vec![(0, pk, bh1)]);

    // Take → InFlight
    manager.take_ready().unwrap();

    // No landing observed, no YS update → tick_timeouts moves to Stale
    std::thread::sleep(Duration::from_millis(20));
    let stale = manager.tick_timeouts(Duration::from_millis(10), Duration::from_secs(5));
    assert_eq!(stale.len(), 1);

    // RPC fallback discovers actual advance happened
    manager.on_fallback_refresh(&pk, bh2);
    match manager.get_by_id(0).unwrap().state() {
        NonceState::Ready { blockhash } => assert_eq!(blockhash, bh2),
        other => panic!("expected Ready, got {:?}", other),
    }
}

#[test]
fn rr_distributes_evenly_across_pool() {
    let n = 10;
    let entries: Vec<_> = (0..n)
        .map(|i| (i as u16, Pubkey::new_unique(), Hash::new_unique()))
        .collect();
    let manager = NonceManager::new(entries);

    let mut taken_ids = Vec::new();
    for _ in 0..n {
        let (id, _, _) = manager.take_ready().unwrap();
        taken_ids.push(id);
    }
    // All distinct
    taken_ids.sort();
    let expected: Vec<u16> = (0..n as u16).collect();
    assert_eq!(taken_ids, expected);

    // 11th take returns None (pool exhausted)
    assert!(manager.take_ready().is_none());
}

#[test]
fn no_landing_then_fallback_with_same_blockhash_returns_to_ready_for_reuse() {
    // Scenario: all N variants drop (none reach leader). Nonce never advanced.
    // Fallback fetch returns same blockhash. Should be reusable (free retry).
    let pk = Pubkey::new_unique();
    let bh = Hash::new_unique();
    let manager = NonceManager::new(vec![(0, pk, bh)]);

    manager.take_ready().unwrap();
    std::thread::sleep(Duration::from_millis(20));
    manager.tick_timeouts(Duration::from_millis(10), Duration::from_secs(5));
    manager.on_fallback_refresh(&pk, bh);

    match manager.get_by_id(0).unwrap().state() {
        NonceState::Ready { blockhash } => assert_eq!(blockhash, bh),
        other => panic!("expected Ready with same bh, got {:?}", other),
    }
    // Can take again with same blockhash
    let (_id, _pk, bh_out) = manager.take_ready().unwrap();
    assert_eq!(bh_out, bh);
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p fan-out-bench --test nonce_integration`
Expected: 4 tests pass.

---

## Task 12: Final verification

- [ ] **Step 1: Run full test suite**

Run: `cargo test -p fan-out-bench`
Expected: all tests from Plan 1 + Plan 2 pass (~70 tests total).

- [ ] **Step 2: Clippy clean**

Run: `cargo clippy -p fan-out-bench --all-targets --no-deps -- -D warnings`
Expected: no warnings.

- [ ] **Step 3: Build all binaries**

Run: `cargo build -p fan-out-bench --bins`
Expected: builds both `setup-nonces` and `teardown-nonces` binaries.

- [ ] **Step 4: Update README**

Modify `crates/fan-out-bench/README.md` to reflect Plan 2 completion. Add section under "Status":

```markdown
Plan 2 — nonce infrastructure:
- ✅ Wallet keypair loader
- ✅ Nonce state parsing (Account.data → NonceAccountState)
- ✅ NonceManager state machine (Ready/InFlight/AwaitingUpdate/Stale)
- ✅ RR allocator with take_ready()
- ✅ Bootstrap (getMultipleAccounts)
- ✅ YS gRPC subscription for live updates
- ✅ RPC fallback poller for Stale nonces
- ✅ setup-nonces binary (create N pool)
- ✅ teardown-nonces binary (refund rent)
```

Also add usage:

```markdown
## Setup nonce pool (one-time)

```bash
cargo run --release --bin setup-nonces -- \
  --rpc-url <HELIUS_OR_TRITON_RPC> \
  --wallet ~/.config/solana/dex-bench.json \
  --count 150 \
  --output-keypairs nonce-keypairs.json \
  --output-config nonce-config.json
```

Cost: ~0.22 SOL lockup (refundable via teardown-nonces), <0.001 SOL tx fees.

## Teardown nonce pool (when done)

```bash
cargo run --release --bin teardown-nonces -- \
  --rpc-url <RPC> \
  --wallet ~/.config/solana/dex-bench.json \
  --keypairs nonce-keypairs.json
```
```

---

## Plan 2 done

Po tym planie mamy:
- Pełny state machine + RR allocator dla durable nonce
- Live updates przez YS subscription
- Fallback poll RPC dla degradacji
- Setup/teardown CLI gotowe do uruchomienia (manual smoke test)

Następne plany:
- Plan 3: Entry sources merger + Observer (PoH tick tracking)
- Plan 4: First senders (Helius, Jito) + Matcher state machine + runtime wiring → pierwszy realny e2e run
- Plan 5: Remaining REST senders
- Plan 6: gRPC/QUIC senders
- Plan 7: Ops + polish
