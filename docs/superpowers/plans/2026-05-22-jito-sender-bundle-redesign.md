# Jito Sender Bundle Redesign — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the single-tx `JitoSender` with a `JitoBundleSender` that fans out a 2-tx bundle (durable-nonce Tx1 + throwaway-tipper Tx2) over 8 hosts × {JSON-RPC, gRPC} = 16 paths from a rotated pool of 5 source IPs, while leaving Helius and the rest of the pipeline untouched.

**Architecture:** Build on the existing preparer/dispatcher/pool. `TxSender` trait gains a `send_bundle` default. `PreSignedTx` gains `extra_txs` for Jito's Tx2. New `senders/jito/` submodule holds the per-protocol multi-IP clients, the tip floor poller, and the bundle sender. tonic upgrades workspace-wide from 0.12 → 0.13 to use native `Endpoint::local_address`.

**Tech Stack:** Rust 2024, Tokio, tonic 0.13 (gRPC), reqwest 0.12 (JSON-RPC), prost 0.13, parking_lot, dashmap, Solana SDK 3.0, async-trait.

**Spec:** `docs/superpowers/specs/2026-05-22-jito-sender-bundle-redesign.md`

---

## Pre-Task 0: Manual cleanup of temp commits (USER ACTION)

The branch `master` carries 6 temp commits (`9f039ff..02e72cf`, "temp: …") that should be removed before this work lands. **The user authorized force-push removal but will execute it manually — do not run git commands.**

The user will run, when ready:

```bash
git reset --hard 49ca45d
git push --force-with-lease origin master
```

Engineer: confirm before starting Task 1 that `git log --oneline -1` shows `49ca45d feat: add min sent interval` as `HEAD`. If it doesn't, ask the user.

---

## Task 1: Upgrade `tonic` 0.12 → 0.13 (workspace-wide)

**Files:**
- Modify: `Cargo.toml` (workspace root)
- Modify: `crates/entry-sources/Cargo.toml`
- Touch (verify still compiles): `crates/entry-sources/build.rs`, `crates/entry-sources/src/yellowstone.rs`, `crates/entry-sources/src/shredstream/*.rs`

**Context:** `tonic 0.13.0` added native `Endpoint::local_address(Some(IpAddr))` (PR #1567). We need it for per-IP gRPC binding in Task 8. The workspace currently pins `tonic = "0.12"`; `entry-sources` uses tonic via `yellowstone-grpc-client` (currently `12.2` which depends on tonic 0.12). The risk is that `yellowstone-grpc-client` doesn't yet support tonic 0.13. We test the upgrade first and, if blocked, fall back to a per-IP gRPC connector pattern that works on 0.12 (described in the contingency block at the end of this task).

- [ ] **Step 1: Update workspace `Cargo.toml`**

Edit `/home/jjaksik/Repos/my-scripts/Cargo.toml` lines 65-66:

```toml
tonic = "0.13"
prost = "0.13"
```

(prost stays at 0.13 — already correct; tonic bumps to 0.13.)

- [ ] **Step 2: Update `crates/entry-sources/Cargo.toml`**

Edit the build-dependencies line:

```toml
[build-dependencies]
tonic-build = "0.13"
```

- [ ] **Step 3: Try to build the workspace**

Run:
```bash
cd /home/jjaksik/Repos/my-scripts && cargo check --workspace --all-targets 2>&1 | tail -80
```

Three possible outcomes:

**(a) Clean build** — proceed to Step 4.

**(b) `yellowstone-grpc-client` incompatible** — error mentions `tonic` version mismatch with yellowstone. Bump `yellowstone-grpc-client` and `yellowstone-grpc-proto` in workspace `Cargo.toml` to a release that uses tonic 0.13 (check `cargo search yellowstone-grpc-client` — at the time of writing the latest is 6.x/12.x; bump until cargo accepts). If the latest yellowstone still requires tonic 0.12, skip to **(d) contingency**.

**(c) Minor API breakage in `entry-sources` code** — `tonic::Status` constructors, `Channel::balance_channel`, or similar — fix call sites locally. Common 0.12→0.13 changes:
- `tonic::transport::Endpoint::connect_with_connector_lazy` is still present.
- `tonic::Status::code(code)` API unchanged.
- If `Endpoint::tls_config(ClientTlsConfig::new())` complains about a missing arg, the new builder is `ClientTlsConfig::new().with_native_roots()` or `with_webpki_roots()` — pick the same one used elsewhere in the repo if it's already configured.

**(d) Contingency — yellowstone blocks the upgrade.** Revert workspace `Cargo.toml` to `tonic = "0.12"` and `tonic-build = "0.12"` in entry-sources. Document this in `crates/tick-trigger-fan-out-bench/src/senders/jito/grpc.rs` (Task 8) and use the `hyper-util` per-IP connector pattern there (also documented in Task 8). The rest of the plan does not change.

- [ ] **Step 4: Run entry-sources tests to confirm nothing broke**

```bash
cd /home/jjaksik/Repos/my-scripts && cargo test -p entry-sources 2>&1 | tail -30
```

Expected: tests pass, no compile errors.

- [ ] **Step 5: Pause for user to commit**

Tell the user: "Tonic upgrade complete (or contingency engaged — note which). Ready for you to commit. I'll wait."

---

## Task 2: Extend `TxSender` trait with `send_bundle` default

**Files:**
- Modify: `crates/tick-trigger-fan-out-bench/src/senders/mod.rs`
- Test: `crates/tick-trigger-fan-out-bench/src/senders/mod.rs` (existing `#[cfg(test)] mod tests` block or a new one)

- [ ] **Step 1: Write failing test for default `send_bundle` behavior**

Add at the bottom of `senders/mod.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::transaction::Transaction;

    struct DummySender;

    #[async_trait]
    impl TxSender for DummySender {
        fn id(&self) -> u8 { 0 }
        fn name(&self) -> &str { "dummy" }
        fn endpoint_url(&self) -> &str { "" }
        fn protocol(&self) -> &'static str { "DUMMY" }
        async fn send(&self, _tx: &Transaction) -> SendOutcome {
            SendOutcome {
                send_at: Instant::now(),
                send_ack_at: None,
                signature: Signature::default(),
                http_status: None,
                rpc_err_code: None,
                rpc_err_message: None,
                provider_request_id: None,
                error: None,
                endpoint_url_used: None,
            }
        }
    }

    #[tokio::test]
    async fn default_send_bundle_returns_unsupported_error() {
        let sender = DummySender;
        let txs = vec![Transaction::default(), Transaction::default()];
        let outcome = sender.send_bundle(&txs).await;
        assert_eq!(outcome.error.as_deref(), Some("sender does not support bundles"));
        assert!(outcome.send_ack_at.is_none());
        assert!(outcome.provider_request_id.is_none());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /home/jjaksik/Repos/my-scripts && cargo test -p tick-trigger-fan-out-bench senders::tests::default_send_bundle 2>&1 | tail -20
```

Expected: COMPILE ERROR — `send_bundle` not defined on `TxSender`.

- [ ] **Step 3: Add `send_bundle` default method to the trait**

In `senders/mod.rs`, replace the `pub trait TxSender` block:

```rust
#[async_trait]
pub trait TxSender: Send + Sync {
    fn id(&self) -> u8;
    fn name(&self) -> &str;
    fn endpoint_url(&self) -> &str;
    fn protocol(&self) -> &'static str;
    async fn send(&self, tx: &Transaction) -> SendOutcome;

    /// Default: returns a `SendOutcome` flagged as unsupported. Only the
    /// JitoBundleSender overrides this.
    async fn send_bundle(&self, _txs: &[Transaction]) -> SendOutcome {
        SendOutcome {
            send_at: Instant::now(),
            send_ack_at: None,
            signature: Signature::default(),
            http_status: None,
            rpc_err_code: None,
            rpc_err_message: None,
            provider_request_id: None,
            error: Some("sender does not support bundles".into()),
            endpoint_url_used: None,
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd /home/jjaksik/Repos/my-scripts && cargo test -p tick-trigger-fan-out-bench senders::tests::default_send_bundle 2>&1 | tail -10
```

Expected: PASS.

- [ ] **Step 5: Run the full sender suite to confirm no regressions**

```bash
cd /home/jjaksik/Repos/my-scripts && cargo test -p tick-trigger-fan-out-bench senders:: 2>&1 | tail -20
```

Expected: all green.

- [ ] **Step 6: Pause for user to commit**

---

## Task 3: Add `fund_tipper` to `BuildParams` and `build_tipper_tx` function

**Files:**
- Modify: `crates/tick-trigger-fan-out-bench/src/tx_builder.rs`

**Context:** Tx1 of a Jito bundle needs an extra `transfer(payer → tipper, fund_amount)` instruction so the throwaway tipper has lamports to pay the tip + return rent_exempt. Tx2 is a separate transaction signed by the throwaway tipper. We add an optional `fund_tipper` to `BuildParams` and a sibling `build_tipper_tx` for Tx2.

- [ ] **Step 1: Write failing test for `fund_tipper` adds a transfer ix to Tx1**

Add to the `#[cfg(test)] mod tests` block at the bottom of `tx_builder.rs`:

```rust
#[test]
fn build_includes_fund_tipper_transfer_when_set() {
    let payer = Keypair::new();
    let tipper_pk = Pubkey::new_unique();
    let cfg = cfg(); // existing helper
    let built = build(BuildParams {
        payer: &payer,
        blockhash: Hash::new_unique(),
        sender_id: 0,
        trigger_id: 123,
        tip_account: None,
        tip_lamports: 0,
        nonce: None,
        tx_cfg: &cfg,
        fund_tipper: Some((tipper_pk, 1_000_000)),
    });
    // 4 base ixs (SetCULimit, SetCUPrice, Memo, Self transfer) + 1 fund_tipper.
    assert_eq!(built.tx.message.instructions.len(), 5);
    // The fund-tipper transfer is the last instruction.
    let last_ix = built.tx.message.instructions.last().unwrap();
    // System program id is account #4 (after payer + tipper).
    // The simplest assertion is that the tx includes the tipper pubkey as an account key.
    let keys: Vec<_> = built.tx.message.account_keys.iter().collect();
    assert!(
        keys.iter().any(|k| **k == tipper_pk),
        "tipper pubkey {} not found in account keys: {:?}", tipper_pk, keys
    );
    let _ = last_ix;
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /home/jjaksik/Repos/my-scripts && cargo test -p tick-trigger-fan-out-bench tx_builder::tests::build_includes_fund_tipper_transfer_when_set 2>&1 | tail -15
```

Expected: COMPILE ERROR — `fund_tipper` field missing on `BuildParams`.

- [ ] **Step 3: Add `fund_tipper` to `BuildParams` and append the ix when set**

In `tx_builder.rs`, find the `pub struct BuildParams` and add the new field at the end:

```rust
pub struct BuildParams<'a> {
    pub payer: &'a Keypair,
    pub blockhash: Hash,
    pub sender_id: u8,
    pub trigger_id: u64,
    pub tip_account: Option<Pubkey>,
    pub tip_lamports: u64,
    pub nonce: Option<NonceParams>,
    pub tx_cfg: &'a TxConfig,
    /// When `Some((target, lamports))`, append an extra
    /// `system_instruction::transfer(payer → target, lamports)` to the tx.
    /// Used by the Jito bundle preparer to fund a throwaway tipper keypair.
    pub fund_tipper: Option<(Pubkey, u64)>,
}
```

Then in `pub fn build(params: BuildParams<'_>) -> BuiltTx`, after the self-transfer push:

```rust
    // Self-transfer keeps the tx a "real" tx with non-trivial system_program use.
    ixs.push(system_instruction::transfer(
        &payer_pk,
        &payer_pk,
        params.tx_cfg.self_transfer_lamports,
    ));

    // Optional: fund a throwaway tipper account in the same tx (Jito bundle).
    if let Some((tipper, amount)) = params.fund_tipper {
        ixs.push(system_instruction::transfer(
            &payer_pk,
            &tipper,
            amount,
        ));
    }
```

- [ ] **Step 4: Update all existing call sites that construct `BuildParams`**

The compiler will list them. They are:
- `crates/tick-trigger-fan-out-bench/src/preparer.rs:218` (inside the for loop)
- `crates/tick-trigger-fan-out-bench/src/bin/phase3_run.rs:683` (fallback path)
- Existing tests inside `tx_builder.rs`

Add `fund_tipper: None,` to each existing `BuildParams { ... }` literal. Do not change behavior.

- [ ] **Step 5: Add a constant and a `build_tipper_tx` function**

At the top of `tx_builder.rs` (after the `MEMO_PROGRAM_ID_STR` constant), add:

```rust
/// Rent-exempt minimum for a System-owned account holding 0 bytes of data.
/// Stable on mainnet as of 2026-05; if Solana changes the rent schedule
/// this must be updated. Used by the Jito bundle preparer for throwaway
/// tipper accounts.
pub const RENT_EXEMPT_MIN_LAMPORTS: u64 = 890_880;

/// Standard signature fee. One signature → one `BASE_TX_FEE_LAMPORTS`.
pub const BASE_TX_FEE_LAMPORTS: u64 = 5_000;
```

At the bottom of `tx_builder.rs` (before `#[cfg(test)]`), add:

```rust
/// Build Tx2 of a Jito bundle. Signed by a throwaway `tipper` keypair:
/// 1) transfers `tip_lamports` to `tip_account`,
/// 2) transfers `rent_exempt_lamports` back to `main_wallet` (send-back).
///
/// Caller funds the tipper with at least `tip_lamports + rent_exempt + base_fee`
/// in Tx1 via `BuildParams.fund_tipper`. After Tx2 executes, the tipper holds
/// 0 lamports and gets GC'd by the Solana epoch cleanup.
pub fn build_tipper_tx(
    tipper: &Keypair,
    blockhash: Hash,
    tip_account: Pubkey,
    tip_lamports: u64,
    main_wallet: Pubkey,
    rent_exempt_lamports: u64,
) -> BuiltTx {
    let tipper_pk = tipper.pubkey();
    let ixs = vec![
        system_instruction::transfer(&tipper_pk, &tip_account, tip_lamports),
        system_instruction::transfer(&tipper_pk, &main_wallet, rent_exempt_lamports),
    ];
    let message = Message::new(&ixs, Some(&tipper_pk));
    let mut tx = Transaction::new_unsigned(message);
    tx.sign(&[tipper], blockhash);
    let signature = tx.signatures[0];
    BuiltTx { tx, signature }
}
```

- [ ] **Step 6: Add test for `build_tipper_tx` shape**

Append to the `#[cfg(test)] mod tests` block:

```rust
#[test]
fn build_tipper_tx_has_two_transfer_ixs_and_is_signed_by_tipper() {
    let tipper = Keypair::new();
    let main = Pubkey::new_unique();
    let tip_acc = Pubkey::new_unique();
    let bh = Hash::new_unique();
    let built = build_tipper_tx(&tipper, bh, tip_acc, 50_000, main, RENT_EXEMPT_MIN_LAMPORTS);
    assert_eq!(built.tx.message.instructions.len(), 2);
    assert_eq!(built.tx.signatures.len(), 1);
    assert_ne!(built.signature, Signature::default());
    // The first signer (payer) must be the tipper.
    assert_eq!(built.tx.message.account_keys[0], tipper.pubkey());
}
```

- [ ] **Step 7: Run all tx_builder tests**

```bash
cd /home/jjaksik/Repos/my-scripts && cargo test -p tick-trigger-fan-out-bench tx_builder:: 2>&1 | tail -25
```

Expected: all tests pass, including the two new ones.

- [ ] **Step 8: Verify nothing downstream broke**

```bash
cd /home/jjaksik/Repos/my-scripts && cargo check -p tick-trigger-fan-out-bench --all-targets 2>&1 | tail -20
```

Expected: clean.

- [ ] **Step 9: Pause for user to commit**

---

## Task 4: Extend `PreSignedTx` with `extra_txs` and `BundleMeta`

**Files:**
- Modify: `crates/tick-trigger-fan-out-bench/src/tx_pool.rs`

- [ ] **Step 1: Write failing test for new fields**

Append to the `#[cfg(test)] mod tests` block at the bottom of `tx_pool.rs`:

```rust
#[test]
fn presigned_tx_supports_extra_txs_and_bundle_meta() {
    let tipper_pk = solana_sdk::pubkey::Pubkey::new_unique();
    let tip_acc = solana_sdk::pubkey::Pubkey::new_unique();
    let pre = PreSignedTx {
        sender_id: 7,
        tx: Arc::new(Transaction::default()),
        signature: Signature::default(),
        blockhash: Hash::default(),
        prepared_at: Instant::now(),
        nonce_id: None,
        extra_txs: vec![Arc::new(Transaction::default())],
        bundle_metadata: Some(BundleMeta {
            tipper_pubkey: tipper_pk,
            tip_account: tip_acc,
            tip_lamports: 25_000,
            tx2_blockhash: Hash::default(),
        }),
    };
    assert_eq!(pre.extra_txs.len(), 1);
    let m = pre.bundle_metadata.unwrap();
    assert_eq!(m.tip_lamports, 25_000);
    assert_eq!(m.tipper_pubkey, tipper_pk);
    assert_eq!(m.tip_account, tip_acc);
}

#[test]
fn presigned_tx_default_path_has_empty_extra_txs() {
    let pre = PreSignedTx {
        sender_id: 0,
        tx: Arc::new(Transaction::default()),
        signature: Signature::default(),
        blockhash: Hash::default(),
        prepared_at: Instant::now(),
        nonce_id: None,
        extra_txs: vec![],
        bundle_metadata: None,
    };
    assert!(pre.extra_txs.is_empty());
    assert!(pre.bundle_metadata.is_none());
}
```

Also update the existing `fn dummy(sender_id: u8) -> PreSignedTx` to set the new fields to defaults:

```rust
fn dummy(sender_id: u8) -> PreSignedTx {
    PreSignedTx {
        sender_id,
        tx: Arc::new(Transaction::default()),
        signature: Signature::default(),
        blockhash: Hash::default(),
        prepared_at: Instant::now(),
        nonce_id: None,
        extra_txs: vec![],
        bundle_metadata: None,
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /home/jjaksik/Repos/my-scripts && cargo test -p tick-trigger-fan-out-bench tx_pool::tests::presigned_tx_supports_extra_txs 2>&1 | tail -10
```

Expected: COMPILE ERROR — fields missing.

- [ ] **Step 3: Add `BundleMeta` struct and extend `PreSignedTx`**

In `tx_pool.rs`, replace the imports and `PreSignedTx` block:

```rust
use dashmap::DashMap;
use solana_sdk::hash::Hash;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_sdk::transaction::Transaction;
use std::sync::Arc;
use std::time::Instant;

/// Per-bundle diagnostic info attached to `PreSignedTx`. Populated by the
/// Jito preparer path; the recorder logs it but it does NOT affect the
/// hot-path send logic.
#[derive(Debug, Clone)]
pub struct BundleMeta {
    pub tipper_pubkey: Pubkey,
    pub tip_account: Pubkey,
    pub tip_lamports: u64,
    pub tx2_blockhash: Hash,
}

#[derive(Debug, Clone)]
pub struct PreSignedTx {
    pub sender_id: u8,
    /// Tx1 of a bundle, or the single tx for non-bundle senders. Its
    /// signature is the one tracked in `pending_sigs`.
    pub tx: Arc<Transaction>,
    pub signature: Signature,
    pub blockhash: Hash,
    pub prepared_at: Instant,
    pub nonce_id: Option<crate::nonce::manager::NonceId>,

    /// Bundle siblings. Empty `Vec` for non-bundle senders (Helius etc).
    /// For Jito, contains `[Tx2]` (the tipper transfer).
    pub extra_txs: Vec<Arc<Transaction>>,
    /// Diagnostic metadata for bundle sends. `None` for non-bundle senders.
    pub bundle_metadata: Option<BundleMeta>,
}
```

- [ ] **Step 4: Update preparer.rs call site at line ~228**

In `crates/tick-trigger-fan-out-bench/src/preparer.rs`, find the `variants.push(PreSignedTx { ... })` and add the two new fields:

```rust
variants.push(PreSignedTx {
    sender_id: slot.config.id,
    tx: Arc::new(built.tx),
    signature: built.signature,
    blockhash: bh,
    prepared_at,
    nonce_id,
    extra_txs: vec![],
    bundle_metadata: None,
});
```

- [ ] **Step 5: Update phase3_run.rs fallback path at lines ~693-700**

In the dispatcher fallback (where `pool.take` returned None and nonce_mode is disabled), find the `v.push(PreSignedTx { ... })` and append:

```rust
v.push(PreSignedTx {
    sender_id: sc.id,
    tx: Arc::new(built.tx),
    signature: built.signature,
    blockhash: bh,
    prepared_at: Instant::now(),
    nonce_id: None,
    extra_txs: vec![],
    bundle_metadata: None,
});
```

- [ ] **Step 6: Run tests**

```bash
cd /home/jjaksik/Repos/my-scripts && cargo test -p tick-trigger-fan-out-bench tx_pool:: 2>&1 | tail -15
```

Expected: all pass including the two new ones.

- [ ] **Step 7: Confirm whole crate still compiles**

```bash
cd /home/jjaksik/Repos/my-scripts && cargo check -p tick-trigger-fan-out-bench --all-targets 2>&1 | tail -10
```

Expected: clean (warnings are OK).

- [ ] **Step 8: Pause for user to commit**

---

## Task 5: Replace old `senders/jito.rs` with new `senders/jito/` module skeleton

**Files:**
- Delete: `crates/tick-trigger-fan-out-bench/src/senders/jito.rs`
- Create: `crates/tick-trigger-fan-out-bench/src/senders/jito/mod.rs` (skeleton)
- Modify: `crates/tick-trigger-fan-out-bench/src/senders/mod.rs` (re-export)
- Modify: `crates/tick-trigger-fan-out-bench/src/bin/phase3_run.rs` (temporarily comment out Jito sender wiring)

**Context:** We need to land the new module without breaking the build. We'll temporarily disable Jito in `phase3_run.rs` (returning a bail error if any config uses `kind: jito`); subsequent tasks restore it. **The current `tests` for the old jito.rs are deleted with it; equivalent coverage gets added in tasks 6-10.**

- [ ] **Step 1: Delete `senders/jito.rs`**

```bash
rm /home/jjaksik/Repos/my-scripts/crates/tick-trigger-fan-out-bench/src/senders/jito.rs
```

- [ ] **Step 2: Create `senders/jito/mod.rs` with placeholder `JitoBundleSender`**

Create file `/home/jjaksik/Repos/my-scripts/crates/tick-trigger-fan-out-bench/src/senders/jito/mod.rs`:

```rust
//! Jito Block Engine bundle sender.
//!
//! Sends a 2-tx bundle (durable-nonce Tx1 + throwaway-tipper Tx2) over
//! 8 regional hosts × {JSON-RPC, gRPC} = 16 parallel paths, all bound
//! to a single rotated source IP per send. See
//! `docs/superpowers/specs/2026-05-22-jito-sender-bundle-redesign.md`.

pub mod json_rpc;
pub mod grpc;
pub mod tip_updater;

use super::{SendOutcome, TxSender};
use async_trait::async_trait;
use solana_sdk::signature::Signature;
use solana_sdk::transaction::Transaction;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub struct JitoBundleSender {
    id: u8,
    name: String,
    endpoint_template: String,
    pub(crate) json_rpc: json_rpc::JsonRpcMultiIpClient,
    pub(crate) grpc: Option<grpc::GrpcMultiIpClient>,
    ip_count: usize,
    ip_cursor: AtomicUsize,
    current_tip_lamports: Arc<AtomicU64>,
    min_send_interval: Duration,
    last_send_at: parking_lot::Mutex<Option<Instant>>,
}

impl JitoBundleSender {
    /// Read the current tip floor value (lamports). Refreshed in the background
    /// by `JitoTipUpdater`. Preparer calls this at pre-sign time so each bundle
    /// captures a fixed tip amount.
    pub fn current_tip_lamports(&self) -> u64 {
        self.current_tip_lamports.load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[async_trait]
impl TxSender for JitoBundleSender {
    fn id(&self) -> u8 { self.id }
    fn name(&self) -> &str { &self.name }
    fn endpoint_url(&self) -> &str { &self.endpoint_template }
    fn protocol(&self) -> &'static str { "JITO_BUNDLE" }

    /// JitoBundleSender does not handle single-tx sends. Always use `send_bundle`.
    async fn send(&self, _tx: &Transaction) -> SendOutcome {
        SendOutcome {
            send_at: Instant::now(),
            send_ack_at: None,
            signature: Signature::default(),
            http_status: None,
            rpc_err_code: None,
            rpc_err_message: None,
            provider_request_id: None,
            error: Some("JitoBundleSender requires send_bundle".into()),
            endpoint_url_used: None,
        }
    }

    // send_bundle is implemented in Task 10.
}
```

- [ ] **Step 3: Update `senders/mod.rs` to re-export the new module**

In `crates/tick-trigger-fan-out-bench/src/senders/mod.rs`, replace `pub mod jito;` with:

```rust
pub mod jito;
```

(Already says `pub mod jito;` — Rust resolves to `jito/mod.rs` once the file exists.)

- [ ] **Step 4: Make `phase3_run.rs` compile by commenting out Jito wiring**

In `crates/tick-trigger-fan-out-bench/src/bin/phase3_run.rs`:

Change the import line 62:

```rust
use tick_trigger_fan_out_bench::senders::{helius::HeliusSender, TxSender};
// JitoBundleSender wired in Task 14.
```

Replace the `SenderKind::Jito => { … Arc::new(JitoSender::new(...)) }` block at lines 290-306 with:

```rust
SenderKind::Jito => {
    anyhow::bail!(
        "Jito sender is being rewired (Task 14 of jito-sender-bundle-redesign). \
        Disable Jito senders in config for now: id={}, name={}",
        sc.id, sc.name
    );
}
```

- [ ] **Step 5: Build the workspace**

```bash
cd /home/jjaksik/Repos/my-scripts && cargo check -p tick-trigger-fan-out-bench --all-targets 2>&1 | tail -25
```

Expected: BUILD FAILS — `json_rpc` and `grpc` modules don't exist yet. Create stubs in Step 6.

- [ ] **Step 6: Create stub submodule files**

Create `/home/jjaksik/Repos/my-scripts/crates/tick-trigger-fan-out-bench/src/senders/jito/json_rpc.rs`:

```rust
//! JSON-RPC multi-IP client for Jito sendBundle. Implemented in Task 6.

pub struct JsonRpcMultiIpClient;
```

Create `/home/jjaksik/Repos/my-scripts/crates/tick-trigger-fan-out-bench/src/senders/jito/grpc.rs`:

```rust
//! gRPC multi-IP client for Jito sendBundle. Implemented in Task 8.

pub struct GrpcMultiIpClient;
```

Create `/home/jjaksik/Repos/my-scripts/crates/tick-trigger-fan-out-bench/src/senders/jito/tip_updater.rs`:

```rust
//! Background poller for the Jito tip floor. Implemented in Task 9.
```

- [ ] **Step 7: Build again**

```bash
cd /home/jjaksik/Repos/my-scripts && cargo check -p tick-trigger-fan-out-bench --all-targets 2>&1 | tail -20
```

Expected: clean compile (warnings about unused fields/types are OK).

- [ ] **Step 8: Pause for user to commit**

---

## Task 6: Implement `JsonRpcMultiIpClient` (HostIpMatrix + per-host POST)

**Files:**
- Modify: `crates/tick-trigger-fan-out-bench/src/senders/jito/json_rpc.rs`

- [ ] **Step 1: Write failing test for matrix shape and substitution**

Replace `senders/jito/json_rpc.rs` content with the test skeleton:

```rust
//! JSON-RPC multi-IP client for Jito sendBundle.
//!
//! Holds 8 hosts × N source IPs = 8N reqwest clients, each bound to a
//! specific outbound IP. `send_for_host(host_idx, ip_idx, body)` fires
//! one POST. The sender orchestrates the 8 parallel calls.

use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

/// Per-host, per-IP grid of `reqwest::Client`s.
pub struct JsonRpcMultiIpClient {
    pub hosts: Vec<String>,         // 8 fully-substituted URLs
    grid: Vec<Vec<reqwest::Client>>, // grid[host_idx][ip_idx]
    ip_count: usize,
}

impl JsonRpcMultiIpClient {
    pub fn new(endpoint_template: &str, regions: &[String], outbound_ips: &[String]) -> Self {
        let hosts: Vec<String> = regions
            .iter()
            .map(|r| endpoint_template.replace("{region}", r))
            .collect();
        let ip_count = outbound_ips.len().max(1);
        let grid: Vec<Vec<reqwest::Client>> = hosts
            .iter()
            .map(|_| build_clients_for_host(outbound_ips))
            .collect();
        Self { hosts, grid, ip_count }
    }

    pub fn host_count(&self) -> usize { self.hosts.len() }
    pub fn ip_count(&self) -> usize { self.ip_count }

    /// Fire one POST and wait for the response (status + body text).
    pub async fn post(
        &self,
        host_idx: usize,
        ip_idx: usize,
        body: Arc<String>,
    ) -> Result<(u16, String), String> {
        let client = &self.grid[host_idx][ip_idx % self.ip_count];
        let url = &self.hosts[host_idx];
        let resp = client
            .post(url)
            .header("Content-Type", "application/json")
            .body((*body).clone())
            .send()
            .await
            .map_err(|e| e.to_string())?;
        let status = resp.status().as_u16();
        let text = resp.text().await.map_err(|e| e.to_string())?;
        Ok((status, text))
    }
}

fn build_clients_for_host(outbound_ips: &[String]) -> Vec<reqwest::Client> {
    fn base() -> reqwest::ClientBuilder {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .tcp_nodelay(true)
            .pool_max_idle_per_host(8)
            .tcp_keepalive(Duration::from_secs(30))
    }
    if outbound_ips.is_empty() {
        return vec![base().build().expect("reqwest client")];
    }
    outbound_ips
        .iter()
        .map(|s| {
            let ip = IpAddr::from_str(s).unwrap_or_else(|_| panic!("invalid outbound_ip {s:?}"));
            base()
                .local_address(Some(ip))
                .build()
                .expect("reqwest client with local_address")
        })
        .collect()
}

#[derive(Serialize)]
pub struct SendBundleRequest<'a> {
    pub jsonrpc: &'static str,
    pub id: u64,
    pub method: &'static str,
    /// Jito spec: `params` is `[[tx_b64, ...], { "encoding": "base64" }]`.
    pub params: (Vec<&'a str>, SendBundleOptions),
}

#[derive(Serialize)]
pub struct SendBundleOptions {
    pub encoding: &'static str,
}

#[derive(Deserialize)]
pub struct JsonRpcResponse {
    pub result: Option<String>,
    pub error: Option<JsonRpcError>,
}

#[derive(Deserialize)]
pub struct JsonRpcError {
    pub code: i32,
    pub message: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matrix_substitutes_regions_correctly() {
        let c = JsonRpcMultiIpClient::new(
            "https://{region}.mainnet.block-engine.jito.wtf",
            &["frankfurt".into(), "amsterdam".into(), "dublin".into(),
              "london".into(), "ny".into(), "tokyo".into(),
              "slc".into(), "singapore".into()],
            &[],
        );
        assert_eq!(c.host_count(), 8);
        assert_eq!(c.hosts[0], "https://frankfurt.mainnet.block-engine.jito.wtf");
        assert_eq!(c.hosts[7], "https://singapore.mainnet.block-engine.jito.wtf");
    }

    #[test]
    fn matrix_builds_one_client_per_ip_per_host() {
        let c = JsonRpcMultiIpClient::new(
            "https://{region}.x",
            &["r1".into(), "r2".into()],
            &["127.0.0.1".into(), "127.0.0.2".into(), "127.0.0.3".into()],
        );
        assert_eq!(c.host_count(), 2);
        assert_eq!(c.ip_count(), 3);
        assert_eq!(c.grid[0].len(), 3);
        assert_eq!(c.grid[1].len(), 3);
    }

    #[test]
    fn empty_outbound_ips_yields_ip_count_one() {
        let c = JsonRpcMultiIpClient::new("x", &["r1".into()], &[]);
        assert_eq!(c.ip_count(), 1);
        assert_eq!(c.grid[0].len(), 1);
    }

    #[test]
    fn send_bundle_request_serializes_per_jito_spec() {
        let req = SendBundleRequest {
            jsonrpc: "2.0",
            id: 1,
            method: "sendBundle",
            params: (vec!["TX1_B64", "TX2_B64"], SendBundleOptions { encoding: "base64" }),
        };
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["method"], "sendBundle");
        assert_eq!(json["params"][0][0], "TX1_B64");
        assert_eq!(json["params"][0][1], "TX2_B64");
        assert_eq!(json["params"][1]["encoding"], "base64");
    }
}
```

- [ ] **Step 2: Run tests**

```bash
cd /home/jjaksik/Repos/my-scripts && cargo test -p tick-trigger-fan-out-bench senders::jito::json_rpc:: 2>&1 | tail -25
```

Expected: 4 tests pass.

- [ ] **Step 3: Pause for user to commit**

---

## Task 7: Vendor `searcher.proto` and configure `tonic-build`

**Files:**
- Create: `crates/tick-trigger-fan-out-bench/src/senders/jito/proto/searcher.proto`
- Create: `crates/tick-trigger-fan-out-bench/src/senders/jito/proto/bundle.proto`
- Create: `crates/tick-trigger-fan-out-bench/src/senders/jito/proto/packet.proto`
- Create: `crates/tick-trigger-fan-out-bench/src/senders/jito/proto/shared.proto`
- Create: `crates/tick-trigger-fan-out-bench/build.rs`
- Modify: `crates/tick-trigger-fan-out-bench/Cargo.toml`

**Context:** Jito's searcher gRPC service depends on three sibling protos. We vendor the minimum needed for `SendBundle`. Sources: `https://github.com/jito-labs/mev-protos/blob/master/searcher/searcher.proto` and sibling files (bundle.proto, packet.proto, shared.proto). Hand-trimmed below to the fields we use.

- [ ] **Step 1: Create the proto files**

Create `crates/tick-trigger-fan-out-bench/src/senders/jito/proto/packet.proto`:

```proto
syntax = "proto3";

package packet;

message Packet {
    bytes data = 1;
    Meta meta = 2;
}

message Meta {
    uint64 size = 1;
    string addr = 2;
    uint32 port = 3;
    PacketFlags flags = 4;
    uint64 sender_stake = 5;
}

message PacketFlags {
    bool discard = 1;
    bool forwarded = 2;
    bool repair = 3;
    bool simple_vote_tx = 4;
    bool tracer_packet = 5;
}

message PacketBatch {
    repeated Packet packets = 1;
}
```

Create `crates/tick-trigger-fan-out-bench/src/senders/jito/proto/shared.proto`:

```proto
syntax = "proto3";

package shared;

import "google/protobuf/timestamp.proto";

message Header {
    google.protobuf.Timestamp ts = 1;
}

message Heartbeat {
    uint64 count = 1;
}
```

Create `crates/tick-trigger-fan-out-bench/src/senders/jito/proto/bundle.proto`:

```proto
syntax = "proto3";

package bundle;

import "packet.proto";
import "shared.proto";

message Bundle {
    shared.Header header = 1;
    repeated packet.Packet packets = 2;
}

message BundleUuid {
    Bundle bundle = 1;
    string uuid = 2;
}
```

Create `crates/tick-trigger-fan-out-bench/src/senders/jito/proto/searcher.proto`:

```proto
syntax = "proto3";

package searcher;

import "bundle.proto";
import "shared.proto";

service SearcherService {
    rpc SendBundle(SendBundleRequest) returns (SendBundleResponse) {}
    rpc GetTipAccounts(GetTipAccountsRequest) returns (GetTipAccountsResponse) {}
}

message SendBundleRequest {
    bundle.Bundle bundle = 1;
}

message SendBundleResponse {
    string uuid = 1;
}

message GetTipAccountsRequest {}

message GetTipAccountsResponse {
    repeated string accounts = 1;
}
```

- [ ] **Step 2: Create `build.rs`**

Create `/home/jjaksik/Repos/my-scripts/crates/tick-trigger-fan-out-bench/build.rs`:

```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = "src/senders/jito/proto";
    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .compile_protos(
            &[
                format!("{}/searcher.proto", proto_dir),
                format!("{}/bundle.proto", proto_dir),
                format!("{}/packet.proto", proto_dir),
                format!("{}/shared.proto", proto_dir),
            ],
            &[proto_dir],
        )?;
    println!("cargo:rerun-if-changed={}", proto_dir);
    Ok(())
}
```

- [ ] **Step 3: Add `tonic-build` to `Cargo.toml`**

Edit `crates/tick-trigger-fan-out-bench/Cargo.toml`. Add a `build` line under `[package]`:

```toml
[package]
name = "tick-trigger-fan-out-bench"
version = "0.1.0"
edition.workspace = true
license.workspace = true
rust-version.workspace = true
description = "Modular rebuild: tick-trigger semantics + fan-out tx sending. Built phase-by-phase, each phase metric'd, testable, and runnable in isolation."
build = "build.rs"
```

Add `[build-dependencies]` after `[dependencies]`:

```toml
[build-dependencies]
tonic-build = "0.13"
```

And add `tonic` and `prost` to `[dependencies]`:

```toml
tonic = { workspace = true, features = ["tls", "tls-native-roots"] }
prost = { workspace = true }
prost-types = "0.13"
```

(`tls-native-roots` is the standard for connecting to public TLS hosts.)

- [ ] **Step 4: Confirm tonic 0.13 feature names**

If Task 1 ended on the contingency path (tonic stayed at 0.12), the feature name is `tls-roots` on 0.12 — adjust accordingly. Verify with:

```bash
cd /home/jjaksik/Repos/my-scripts && cargo build -p tick-trigger-fan-out-bench 2>&1 | grep -E "feature|tls" | head -10
```

If unknown features appear, check `cargo tree -e features -p tick-trigger-fan-out-bench` to see what tonic exposes.

- [ ] **Step 5: Add a Rust module to surface generated types**

Append to `crates/tick-trigger-fan-out-bench/src/senders/jito/mod.rs` (at the top, after the existing pub mod statements):

```rust
/// Generated protobuf types for Jito searcher service.
pub mod proto {
    pub mod packet { tonic::include_proto!("packet"); }
    pub mod shared { tonic::include_proto!("shared"); }
    pub mod bundle { tonic::include_proto!("bundle"); }
    pub mod searcher { tonic::include_proto!("searcher"); }
}
```

- [ ] **Step 6: Build and verify the generated code compiles**

```bash
cd /home/jjaksik/Repos/my-scripts && cargo build -p tick-trigger-fan-out-bench 2>&1 | tail -30
```

Expected: clean build. tonic-build runs at compile time and emits Rust to `target/debug/build/.../out/`.

- [ ] **Step 7: Smoke test that generated types are importable**

Add to `senders/jito/mod.rs` (inside `mod tests`):

```rust
#[cfg(test)]
mod proto_smoke {
    use super::proto::searcher::SendBundleRequest;
    use super::proto::bundle::Bundle;
    use super::proto::packet::Packet;

    #[test]
    fn generated_types_construct() {
        let pkt = Packet { data: vec![1, 2, 3], meta: None };
        let bundle = Bundle { header: None, packets: vec![pkt] };
        let req = SendBundleRequest { bundle: Some(bundle) };
        assert_eq!(req.bundle.unwrap().packets[0].data, vec![1, 2, 3]);
    }
}
```

Run:
```bash
cd /home/jjaksik/Repos/my-scripts && cargo test -p tick-trigger-fan-out-bench senders::jito::proto_smoke 2>&1 | tail -10
```

Expected: PASS.

- [ ] **Step 8: Pause for user to commit**

---

## Task 8: Implement `GrpcMultiIpClient`

**Files:**
- Modify: `crates/tick-trigger-fan-out-bench/src/senders/jito/grpc.rs`

**Context:** Uses tonic 0.13's `Endpoint::local_address(Some(IpAddr))`. **If Task 1 ended on contingency (tonic 0.12 stayed)**, replace `local_address` with a custom `tower::Service` connector — code variant is given at the end of this task.

- [ ] **Step 1: Write the failing test**

Replace `senders/jito/grpc.rs` with:

```rust
//! gRPC multi-IP client for Jito sendBundle.
//!
//! Builds 8 hosts × N IPs lazy-connected channels with per-IP source binding.
//! `send_for_host(host_idx, ip_idx, packets)` invokes `SearcherService::send_bundle`.

use super::proto::bundle::Bundle;
use super::proto::packet::Packet;
use super::proto::searcher::searcher_service_client::SearcherServiceClient;
use super::proto::searcher::SendBundleRequest;
use std::net::IpAddr;
use std::str::FromStr;
use std::time::Duration;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

pub struct GrpcMultiIpClient {
    pub hosts: Vec<String>,             // 8 bare hostnames (no scheme)
    grid: Vec<Vec<Channel>>,            // grid[host_idx][ip_idx]
    ip_count: usize,
}

impl GrpcMultiIpClient {
    pub fn new(
        endpoint_template: &str,
        regions: &[String],
        outbound_ips: &[String],
    ) -> Result<Self, tonic::transport::Error> {
        // The JSON-RPC template is `https://{region}.mainnet.block-engine.jito.wtf`.
        // For gRPC we extract just the host (no scheme, no path) and connect to :443.
        let hosts: Vec<String> = regions
            .iter()
            .map(|r| {
                endpoint_template
                    .replace("{region}", r)
                    .trim_start_matches("https://")
                    .trim_start_matches("http://")
                    .split('/')
                    .next()
                    .unwrap()
                    .to_string()
            })
            .collect();

        let ips: Vec<Option<IpAddr>> = if outbound_ips.is_empty() {
            vec![None]
        } else {
            outbound_ips
                .iter()
                .map(|s| Some(IpAddr::from_str(s).unwrap_or_else(|_| panic!("invalid outbound_ip {s:?}"))))
                .collect()
        };
        let ip_count = ips.len();

        let mut grid: Vec<Vec<Channel>> = Vec::with_capacity(hosts.len());
        for host in &hosts {
            let mut row = Vec::with_capacity(ip_count);
            for ip in &ips {
                let mut ep = Endpoint::from_shared(format!("https://{host}:443"))?
                    .tls_config(ClientTlsConfig::new().domain_name(host).with_native_roots())?
                    .timeout(Duration::from_secs(5))
                    .tcp_keepalive(Some(Duration::from_secs(30)))
                    .http2_keep_alive_interval(Duration::from_secs(20));
                if let Some(addr) = ip {
                    ep = ep.local_address(Some(*addr));
                }
                row.push(ep.connect_lazy());
            }
            grid.push(row);
        }

        Ok(Self { hosts, grid, ip_count })
    }

    pub fn host_count(&self) -> usize { self.hosts.len() }
    pub fn ip_count(&self) -> usize { self.ip_count }

    /// Invoke `SearcherService::SendBundle` on (host_idx, ip_idx).
    /// Returns the `uuid` (bundle id) string on success.
    pub async fn send_bundle(
        &self,
        host_idx: usize,
        ip_idx: usize,
        packet_bytes: &[Vec<u8>],
    ) -> Result<String, tonic::Status> {
        let channel = self.grid[host_idx][ip_idx % self.ip_count].clone();
        let mut client = SearcherServiceClient::new(channel);
        let packets: Vec<Packet> = packet_bytes
            .iter()
            .map(|b| Packet { data: b.clone(), meta: None })
            .collect();
        let bundle = Bundle { header: None, packets };
        let req = tonic::Request::new(SendBundleRequest { bundle: Some(bundle) });
        let resp = client.send_bundle(req).await?;
        Ok(resp.into_inner().uuid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_host_from_https_template() {
        let c = GrpcMultiIpClient::new(
            "https://{region}.mainnet.block-engine.jito.wtf",
            &["frankfurt".into()],
            &[],
        )
        .unwrap();
        assert_eq!(c.hosts[0], "frankfurt.mainnet.block-engine.jito.wtf");
    }

    #[test]
    fn grid_sized_hosts_times_ips() {
        let c = GrpcMultiIpClient::new(
            "https://{region}.x.y",
            &["r1".into(), "r2".into(), "r3".into()],
            &["10.0.0.1".into(), "10.0.0.2".into()],
        )
        .unwrap();
        assert_eq!(c.host_count(), 3);
        assert_eq!(c.ip_count(), 2);
    }

    #[test]
    fn empty_ips_yields_one_default_channel_per_host() {
        let c = GrpcMultiIpClient::new("https://{region}.x", &["r1".into()], &[]).unwrap();
        assert_eq!(c.ip_count(), 1);
    }
}
```

**Contingency variant (if tonic stayed at 0.12 from Task 1):**
Replace the `if let Some(addr) = ip { ep = ep.local_address(...); }` block with a custom connector using `hyper-util`. Add `hyper-util = { version = "0.1", features = ["client", "tokio"] }` to `Cargo.toml`. The connector code:

```rust
use hyper_util::client::legacy::connect::HttpConnector;
// In the per-channel build:
let mut http = HttpConnector::new();
if let Some(addr) = ip { http.set_local_address(Some(*addr)); }
// Then construct the channel via Endpoint::connect_with_connector_lazy(http_with_tls).
```

This is more code; only do it if Task 1 forced contingency.

- [ ] **Step 2: Run the unit tests**

```bash
cd /home/jjaksik/Repos/my-scripts && cargo test -p tick-trigger-fan-out-bench senders::jito::grpc::tests 2>&1 | tail -20
```

Expected: 3 tests pass. (No network calls — only matrix-shape and host-parsing.)

- [ ] **Step 3: Pause for user to commit**

---

## Task 9: Implement `JitoTipUpdater` background task

**Files:**
- Modify: `crates/tick-trigger-fan-out-bench/src/senders/jito/tip_updater.rs`

- [ ] **Step 1: Write the failing test**

Replace `senders/jito/tip_updater.rs` with:

```rust
//! Background poller for Jito's tip floor API.
//!
//! Every `refresh_interval` GETs `https://bundles.jito.wtf/api/v1/bundles/tip_floor`,
//! log-interpolates the configured percentile, clamps to `[floor, ceiling]`,
//! and stores the result in an `AtomicU64` shared with the sender.
//! On network error, retains the previous value.

use serde::Deserialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

pub const JITO_TIP_AMOUNTS_URL: &str = "https://bundles.jito.wtf/api/v1/bundles/tip_floor";

#[derive(Debug, Deserialize, Clone)]
pub struct TipAmounts {
    pub landed_tips_25th_percentile: f64,
    pub landed_tips_50th_percentile: f64,
    pub landed_tips_75th_percentile: f64,
    pub landed_tips_95th_percentile: f64,
    pub landed_tips_99th_percentile: f64,
}

pub struct JitoTipUpdater {
    pub current_lamports: Arc<AtomicU64>,
    pub percentile: u32,
    pub floor_lamports: u64,
    pub ceiling_lamports: u64,
    pub refresh_interval: Duration,
}

impl JitoTipUpdater {
    pub fn new(percentile: u32, floor_lamports: u64, ceiling_lamports: u64, refresh_interval_ms: u64) -> Self {
        Self {
            current_lamports: Arc::new(AtomicU64::new(floor_lamports)),
            percentile,
            floor_lamports,
            ceiling_lamports,
            refresh_interval: Duration::from_millis(refresh_interval_ms),
        }
    }

    /// Spawn the background poller. Returns immediately. Loop exits when
    /// `stop` flips to true.
    pub fn spawn(self, stop: Arc<std::sync::atomic::AtomicBool>) -> tokio::task::JoinHandle<()> {
        let current = self.current_lamports.clone();
        let percentile = self.percentile;
        let floor = self.floor_lamports;
        let ceiling = self.ceiling_lamports;
        let interval = self.refresh_interval;
        tokio::spawn(async move {
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("reqwest client");
            loop {
                if stop.load(Ordering::Relaxed) { break; }
                match fetch_and_compute(&client, percentile, floor, ceiling).await {
                    Ok(v) => current.store(v, Ordering::Relaxed),
                    Err(e) => tracing::warn!(error = %e, "jito tip floor fetch failed; retaining previous value"),
                }
                tokio::time::sleep(interval).await;
            }
        })
    }
}

async fn fetch_and_compute(
    client: &reqwest::Client,
    percentile: u32,
    floor_lamports: u64,
    ceiling_lamports: u64,
) -> Result<u64, String> {
    let raw: Vec<TipAmounts> = client
        .get(JITO_TIP_AMOUNTS_URL)
        .send()
        .await
        .map_err(|e| e.to_string())?
        .json()
        .await
        .map_err(|e| e.to_string())?;
    let amounts = raw.into_iter().next().ok_or("empty tip floor response")?;
    let tip_sol = log_interpolate_percentile(&amounts, percentile as f64);
    let tip_lamports = (tip_sol * 1_000_000_000.0) as u64;
    Ok(tip_lamports.clamp(floor_lamports, ceiling_lamports))
}

/// Log-linear interpolation across the 5 percentile points. Mirrors the
/// `JitoTipUpdater.findTipValueForPercentile` algorithm in dex-trader.
pub fn log_interpolate_percentile(amounts: &TipAmounts, target_percentile: f64) -> f64 {
    let points = [
        (25.0_f64, amounts.landed_tips_25th_percentile.ln()),
        (50.0, amounts.landed_tips_50th_percentile.ln()),
        (75.0, amounts.landed_tips_75th_percentile.ln()),
        (95.0, amounts.landed_tips_95th_percentile.ln()),
        (99.0, amounts.landed_tips_99th_percentile.ln()),
    ];
    let (lower, upper) = surrounding_pair(&points, target_percentile);
    let log_tip = lower.1 + ((target_percentile - lower.0) * (upper.1 - lower.1)) / (upper.0 - lower.0);
    log_tip.exp()
}

fn surrounding_pair(points: &[(f64, f64)], target: f64) -> ((f64, f64), (f64, f64)) {
    for i in 0..points.len() - 1 {
        if target >= points[i].0 && target <= points[i + 1].0 {
            return (points[i], points[i + 1]);
        }
    }
    if target < points[0].0 { return (points[0], points[1]); }
    let last = points.len() - 1;
    (points[last - 1], points[last])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn amounts() -> TipAmounts {
        TipAmounts {
            landed_tips_25th_percentile: 0.000_010, // 10_000 lamports
            landed_tips_50th_percentile: 0.000_025, // 25_000
            landed_tips_75th_percentile: 0.000_060, // 60_000
            landed_tips_95th_percentile: 0.000_500, // 500_000
            landed_tips_99th_percentile: 0.002_000, // 2_000_000
        }
    }

    #[test]
    fn percentile_at_known_point_returns_known_value() {
        let v = log_interpolate_percentile(&amounts(), 50.0);
        let lamports = (v * 1_000_000_000.0) as u64;
        assert!(lamports >= 24_900 && lamports <= 25_100, "50th percentile must be ~25k, got {}", lamports);
    }

    #[test]
    fn percentile_below_25_clamps_to_lower_bracket() {
        let v = log_interpolate_percentile(&amounts(), 10.0);
        assert!(v > 0.0);
    }

    #[test]
    fn percentile_above_99_clamps_to_upper_bracket() {
        let v = log_interpolate_percentile(&amounts(), 100.0);
        assert!(v > 0.0);
    }

    #[test]
    fn updater_stores_floor_on_init() {
        let u = JitoTipUpdater::new(75, 15_000, 2_000_000, 30_000);
        assert_eq!(u.current_lamports.load(Ordering::Relaxed), 15_000);
    }

    #[test]
    fn updater_holds_correct_config_values() {
        let u = JitoTipUpdater::new(75, 15_000, 2_000_000, 30_000);
        assert_eq!(u.percentile, 75);
        assert_eq!(u.floor_lamports, 15_000);
        assert_eq!(u.ceiling_lamports, 2_000_000);
        assert_eq!(u.refresh_interval, Duration::from_millis(30_000));
    }
}
```

- [ ] **Step 2: Run tests**

```bash
cd /home/jjaksik/Repos/my-scripts && cargo test -p tick-trigger-fan-out-bench senders::jito::tip_updater:: 2>&1 | tail -20
```

Expected: 5 tests pass.

- [ ] **Step 3: Pause for user to commit**

---

## Task 10: Wire `JitoBundleSender::send_bundle` with fan-out + IP rotation

**Files:**
- Modify: `crates/tick-trigger-fan-out-bench/src/senders/jito/mod.rs`

**Context:** Combines the JSON-RPC + gRPC clients into a per-trigger fan-out. Per send: pick one IP index, serialize the bundle once, fire 8 JSON-RPC POSTs and (if gRPC enabled) 8 gRPC calls, first reply wins.

- [ ] **Step 1: Write the failing test for builder + IP cursor rotation**

Append to `senders/jito/mod.rs`:

```rust
#[cfg(test)]
mod sender_tests {
    use super::*;
    use std::sync::atomic::Ordering;

    fn make_sender() -> JitoBundleSender {
        JitoBundleSender::new(
            7, "jito-test",
            "https://{region}.x".into(),
            vec!["frankfurt".into(), "amsterdam".into()],
            vec!["10.0.0.1".into(), "10.0.0.2".into(), "10.0.0.3".into()],
            false,    // no gRPC for the sync test
            Arc::new(AtomicU64::new(50_000)),
            0,        // no throttle
        )
        .unwrap()
    }

    #[test]
    fn ip_cursor_rotates_round_robin() {
        let s = make_sender();
        // Each call to fetch_add returns the prior value.
        let a = s.ip_cursor.fetch_add(1, Ordering::Relaxed) % s.ip_count;
        let b = s.ip_cursor.fetch_add(1, Ordering::Relaxed) % s.ip_count;
        let c = s.ip_cursor.fetch_add(1, Ordering::Relaxed) % s.ip_count;
        let d = s.ip_cursor.fetch_add(1, Ordering::Relaxed) % s.ip_count;
        assert_eq!((a, b, c, d), (0, 1, 2, 0));
    }

    #[test]
    fn current_tip_lamports_reads_atomic() {
        let s = make_sender();
        assert_eq!(s.current_tip_lamports(), 50_000);
        s.current_tip_lamports.store(123_456, Ordering::Relaxed);
        assert_eq!(s.current_tip_lamports(), 123_456);
    }

    #[test]
    fn protocol_label_is_jito_bundle() {
        let s = make_sender();
        assert_eq!(s.protocol(), "JITO_BUNDLE");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /home/jjaksik/Repos/my-scripts && cargo test -p tick-trigger-fan-out-bench senders::jito::sender_tests 2>&1 | tail -10
```

Expected: COMPILE ERROR — `JitoBundleSender::new` not implemented.

- [ ] **Step 3: Implement constructor and `send_bundle`**

Replace `senders/jito/mod.rs` with the full implementation (the existing skeleton is fully replaced):

```rust
//! Jito Block Engine bundle sender.
//!
//! Sends a 2-tx bundle (durable-nonce Tx1 + throwaway-tipper Tx2) over
//! 8 regional hosts × {JSON-RPC, gRPC} = 16 parallel paths, all bound
//! to a single rotated source IP per send.

pub mod json_rpc;
pub mod grpc;
pub mod tip_updater;

/// Generated protobuf types.
pub mod proto {
    pub mod packet { tonic::include_proto!("packet"); }
    pub mod shared { tonic::include_proto!("shared"); }
    pub mod bundle { tonic::include_proto!("bundle"); }
    pub mod searcher { tonic::include_proto!("searcher"); }
}

use super::{SendOutcome, TxSender};
use async_trait::async_trait;
use base64::Engine as _;
use json_rpc::{JsonRpcMultiIpClient, JsonRpcResponse, SendBundleOptions, SendBundleRequest};
use grpc::GrpcMultiIpClient;
use solana_sdk::signature::Signature;
use solana_sdk::transaction::Transaction;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub struct JitoBundleSender {
    id: u8,
    name: String,
    endpoint_template: String,
    pub(crate) json_rpc: JsonRpcMultiIpClient,
    pub(crate) grpc: Option<GrpcMultiIpClient>,
    pub(crate) ip_count: usize,
    pub(crate) ip_cursor: AtomicUsize,
    pub(crate) current_tip_lamports: Arc<AtomicU64>,
    min_send_interval: Duration,
    last_send_at: parking_lot::Mutex<Option<Instant>>,
}

impl JitoBundleSender {
    pub fn new(
        id: u8,
        name: String,
        endpoint_template: String,
        regions: Vec<String>,
        outbound_ips: Vec<String>,
        use_grpc: bool,
        current_tip_lamports: Arc<AtomicU64>,
        min_send_interval_ms: u64,
    ) -> Result<Self, tonic::transport::Error> {
        let json_rpc = JsonRpcMultiIpClient::new(&endpoint_template, &regions, &outbound_ips);
        let grpc = if use_grpc {
            Some(GrpcMultiIpClient::new(&endpoint_template, &regions, &outbound_ips)?)
        } else {
            None
        };
        let ip_count = json_rpc.ip_count();
        Ok(Self {
            id,
            name,
            endpoint_template,
            json_rpc,
            grpc,
            ip_count,
            ip_cursor: AtomicUsize::new(0),
            current_tip_lamports,
            min_send_interval: Duration::from_millis(min_send_interval_ms),
            last_send_at: parking_lot::Mutex::new(None),
        })
    }

    pub fn current_tip_lamports(&self) -> u64 {
        self.current_tip_lamports.load(Ordering::Relaxed)
    }

    fn next_ip_idx(&self) -> usize {
        self.ip_cursor.fetch_add(1, Ordering::Relaxed) % self.ip_count
    }
}

enum FanoutReply {
    Success {
        method: &'static str,
        host_url: String,
        send_ack_at: Instant,
        bundle_id: String,
        http_status: Option<u16>,
    },
    Error {
        method: &'static str,
        host_url: String,
        send_ack_at: Instant,
        http_status: Option<u16>,
        rpc_err_code: Option<i32>,
        rpc_err_message: Option<String>,
        error: String,
    },
}

#[async_trait]
impl TxSender for JitoBundleSender {
    fn id(&self) -> u8 { self.id }
    fn name(&self) -> &str { &self.name }
    fn endpoint_url(&self) -> &str { &self.endpoint_template }
    fn protocol(&self) -> &'static str { "JITO_BUNDLE" }

    async fn send(&self, _tx: &Transaction) -> SendOutcome {
        SendOutcome {
            send_at: Instant::now(),
            send_ack_at: None,
            signature: Signature::default(),
            http_status: None,
            rpc_err_code: None,
            rpc_err_message: None,
            provider_request_id: None,
            error: Some("JitoBundleSender requires send_bundle".into()),
            endpoint_url_used: None,
        }
    }

    async fn send_bundle(&self, txs: &[Transaction]) -> SendOutcome {
        let send_at_init = Instant::now();
        let signature = txs.first().and_then(|t| t.signatures.first().copied()).unwrap_or_default();

        // Local throttle.
        if self.min_send_interval > Duration::ZERO {
            let now = Instant::now();
            let mut last = self.last_send_at.lock();
            if let Some(prev) = *last {
                if now.duration_since(prev) < self.min_send_interval {
                    return SendOutcome {
                        send_at: now, send_ack_at: Some(now), signature,
                        http_status: None, rpc_err_code: None, rpc_err_message: None,
                        provider_request_id: None,
                        error: Some("throttled_local".into()),
                        endpoint_url_used: None,
                    };
                }
            }
            *last = Some(now);
        }

        if self.json_rpc.host_count() == 0 {
            return SendOutcome {
                send_at: send_at_init, send_ack_at: None, signature,
                http_status: None, rpc_err_code: None, rpc_err_message: None,
                provider_request_id: None,
                error: Some("no regions configured".into()),
                endpoint_url_used: None,
            };
        }

        // Serialize each tx once.
        let raw: Vec<Vec<u8>> = txs.iter().map(|t| bincode::serialize(t).unwrap_or_default()).collect();
        let b64_owned: Vec<String> = raw.iter().map(|b| base64::engine::general_purpose::STANDARD.encode(b)).collect();
        let b64_refs: Vec<&str> = b64_owned.iter().map(String::as_str).collect();
        let body = serde_json::to_string(&SendBundleRequest {
            jsonrpc: "2.0", id: 1, method: "sendBundle",
            params: (b64_refs, SendBundleOptions { encoding: "base64" }),
        }).unwrap_or_default();
        let body_arc: Arc<String> = Arc::new(body);

        let ip_idx = self.next_ip_idx();
        let send_at = Instant::now();
        let total_paths = self.json_rpc.host_count() + self.grpc.as_ref().map(|g| g.host_count()).unwrap_or(0);
        let (tx_first, mut rx_first) = tokio::sync::mpsc::channel::<FanoutReply>(total_paths.max(1));

        // JSON-RPC fan-out.
        for host_idx in 0..self.json_rpc.host_count() {
            let host_url = self.json_rpc.hosts[host_idx].clone();
            let body = body_arc.clone();
            let json_rpc = &self.json_rpc;
            let tx_first = tx_first.clone();
            // SAFETY: client lives as long as &self; we shadow with an owned clone of reqwest::Client (cheap Arc).
            let client = json_rpc.grid_client(host_idx, ip_idx);
            tokio::spawn(async move {
                let result = client
                    .post(&host_url)
                    .header("Content-Type", "application/json")
                    .body((*body).clone())
                    .send().await;
                let send_ack_at = Instant::now();
                let reply = match result {
                    Ok(resp) => {
                        let status = resp.status().as_u16();
                        let text = resp.text().await.unwrap_or_default();
                        parse_json_rpc_reply(host_url, status, text, send_ack_at, "JSON-RPC")
                    }
                    Err(e) => FanoutReply::Error {
                        method: "JSON-RPC", host_url, send_ack_at,
                        http_status: None, rpc_err_code: None, rpc_err_message: None,
                        error: format!("network: {}", e),
                    },
                };
                let _ = tx_first.send(reply).await;
            });
        }

        // gRPC fan-out (if enabled).
        if let Some(grpc) = &self.grpc {
            let packet_bytes = raw.clone();
            for host_idx in 0..grpc.host_count() {
                let host = grpc.hosts[host_idx].clone();
                let host_url = format!("https://{host}:443");
                let packets = packet_bytes.clone();
                let tx_first = tx_first.clone();
                let grpc = grpc;
                // Channel cloning is cheap (Arc internal). We need to clone here
                // because the task moves it.
                let channel = grpc.grid_channel(host_idx, ip_idx);
                tokio::spawn(async move {
                    use super::proto::searcher::searcher_service_client::SearcherServiceClient;
                    use super::proto::searcher::SendBundleRequest as PbSendBundleRequest;
                    use super::proto::bundle::Bundle as PbBundle;
                    use super::proto::packet::Packet as PbPacket;
                    let mut client = SearcherServiceClient::new(channel);
                    let pb_packets: Vec<PbPacket> = packets.iter().map(|b| PbPacket { data: b.clone(), meta: None }).collect();
                    let req = tonic::Request::new(PbSendBundleRequest { bundle: Some(PbBundle { header: None, packets: pb_packets }) });
                    let res = client.send_bundle(req).await;
                    let send_ack_at = Instant::now();
                    let reply = match res {
                        Ok(r) => FanoutReply::Success {
                            method: "gRPC", host_url,
                            send_ack_at, bundle_id: r.into_inner().uuid,
                            http_status: None,
                        },
                        Err(status) => FanoutReply::Error {
                            method: "gRPC", host_url, send_ack_at,
                            http_status: None,
                            rpc_err_code: Some(status.code() as i32),
                            rpc_err_message: Some(status.message().to_string()),
                            error: format!("grpc: {}: {}", status.code(), status.message()),
                        },
                    };
                    let _ = tx_first.send(reply).await;
                });
            }
        }
        drop(tx_first);

        let Some(first) = rx_first.recv().await else {
            return SendOutcome {
                send_at, send_ack_at: None, signature,
                http_status: None, rpc_err_code: None, rpc_err_message: None,
                provider_request_id: None,
                error: Some("all fan-out tasks dropped without reply".into()),
                endpoint_url_used: None,
            };
        };

        match first {
            FanoutReply::Success { method, host_url, send_ack_at, bundle_id, http_status } => SendOutcome {
                send_at, send_ack_at: Some(send_ack_at), signature,
                http_status, rpc_err_code: None, rpc_err_message: None,
                provider_request_id: Some(bundle_id),
                error: None,
                endpoint_url_used: Some(format!("{}/{}", host_url, method)),
            },
            FanoutReply::Error { method, host_url, send_ack_at, http_status, rpc_err_code, rpc_err_message, error } => SendOutcome {
                send_at, send_ack_at: Some(send_ack_at), signature,
                http_status, rpc_err_code, rpc_err_message,
                provider_request_id: None,
                error: Some(error),
                endpoint_url_used: Some(format!("{}/{}", host_url, method)),
            },
        }
    }
}

fn parse_json_rpc_reply(host_url: String, status: u16, body: String, send_ack_at: Instant, method: &'static str) -> FanoutReply {
    match serde_json::from_str::<JsonRpcResponse>(&body) {
        Ok(parsed) => {
            if let Some(err) = parsed.error {
                FanoutReply::Error {
                    method, host_url, send_ack_at,
                    http_status: Some(status),
                    rpc_err_code: Some(err.code),
                    rpc_err_message: Some(err.message.clone()),
                    error: err.message,
                }
            } else if let Some(bundle_id) = parsed.result {
                FanoutReply::Success {
                    method, host_url, send_ack_at, bundle_id, http_status: Some(status),
                }
            } else {
                FanoutReply::Error {
                    method, host_url, send_ack_at,
                    http_status: Some(status),
                    rpc_err_code: None,
                    rpc_err_message: Some("empty result".into()),
                    error: "empty result".into(),
                }
            }
        }
        Err(_) => FanoutReply::Error {
            method, host_url, send_ack_at,
            http_status: Some(status),
            rpc_err_code: None,
            rpc_err_message: Some(format!("non-JSONRPC body: {body}")),
            error: format!("HTTP {status} body: {body}"),
        },
    }
}

#[cfg(test)]
mod sender_tests {
    use super::*;
    use std::sync::atomic::Ordering;

    fn make_sender() -> JitoBundleSender {
        JitoBundleSender::new(
            7, "jito-test".into(),
            "https://{region}.x".into(),
            vec!["frankfurt".into(), "amsterdam".into()],
            vec!["10.0.0.1".into(), "10.0.0.2".into(), "10.0.0.3".into()],
            false,
            Arc::new(AtomicU64::new(50_000)),
            0,
        ).unwrap()
    }

    #[test]
    fn ip_cursor_rotates_round_robin() {
        let s = make_sender();
        let a = s.ip_cursor.fetch_add(1, Ordering::Relaxed) % s.ip_count;
        let b = s.ip_cursor.fetch_add(1, Ordering::Relaxed) % s.ip_count;
        let c = s.ip_cursor.fetch_add(1, Ordering::Relaxed) % s.ip_count;
        let d = s.ip_cursor.fetch_add(1, Ordering::Relaxed) % s.ip_count;
        assert_eq!((a, b, c, d), (0, 1, 2, 0));
    }

    #[test]
    fn current_tip_lamports_reads_atomic() {
        let s = make_sender();
        assert_eq!(s.current_tip_lamports(), 50_000);
        s.current_tip_lamports.store(123_456, Ordering::Relaxed);
        assert_eq!(s.current_tip_lamports(), 123_456);
    }

    #[test]
    fn protocol_label_is_jito_bundle() {
        let s = make_sender();
        assert_eq!(s.protocol(), "JITO_BUNDLE");
    }

    #[tokio::test]
    async fn send_single_tx_returns_unsupported_error() {
        let s = make_sender();
        let tx = Transaction::default();
        let outcome = s.send(&tx).await;
        assert_eq!(outcome.error.as_deref(), Some("JitoBundleSender requires send_bundle"));
    }
}
```

- [ ] **Step 4: Add helper accessors to JSON-RPC and gRPC clients**

The sender uses `self.json_rpc.grid_client(host_idx, ip_idx)` and `self.grpc.as_ref().grid_channel(host_idx, ip_idx)`. Add these accessors:

In `senders/jito/json_rpc.rs`, add a method to `impl JsonRpcMultiIpClient`:

```rust
pub fn grid_client(&self, host_idx: usize, ip_idx: usize) -> reqwest::Client {
    self.grid[host_idx][ip_idx % self.ip_count].clone()
}
```

In `senders/jito/grpc.rs`, add a method to `impl GrpcMultiIpClient`:

```rust
pub fn grid_channel(&self, host_idx: usize, ip_idx: usize) -> tonic::transport::Channel {
    self.grid[host_idx][ip_idx % self.ip_count].clone()
}
```

- [ ] **Step 5: Run sender tests**

```bash
cd /home/jjaksik/Repos/my-scripts && cargo test -p tick-trigger-fan-out-bench senders::jito::sender_tests 2>&1 | tail -20
```

Expected: 4 tests pass.

- [ ] **Step 6: Run whole sender module test suite**

```bash
cd /home/jjaksik/Repos/my-scripts && cargo test -p tick-trigger-fan-out-bench senders:: 2>&1 | tail -30
```

Expected: all green.

- [ ] **Step 7: Pause for user to commit**

---

## Task 11: Extend `SenderConfig` with Jito-specific fields

**Files:**
- Modify: `crates/tick-trigger-fan-out-bench/src/config.rs`

**Context:** Add `use_grpc`, `tip_percentile`, `tip_floor_lamports`, `tip_ceiling_lamports`, `tip_refresh_interval_ms` as optional fields with sensible defaults. The existing `tip_lamports` field is kept for non-Jito senders (Helius pre-funded tip); for Jito it's ignored (the tip floor poller drives the value).

- [ ] **Step 1: Write failing test**

Append to `config.rs` `#[cfg(test)] mod tests`:

```rust
#[test]
fn jito_sender_with_full_bundle_fields_parses() {
    let json = r#"{
      "id": 2, "name": "jito", "kind": "jito",
      "endpoint_url": "https://{region}.mainnet.block-engine.jito.wtf",
      "regions": ["frankfurt", "amsterdam"],
      "outbound_ips": ["1.2.3.4", "1.2.3.5"],
      "use_grpc": true,
      "tip_percentile": 75,
      "tip_floor_lamports": 20000,
      "tip_ceiling_lamports": 1500000,
      "tip_refresh_interval_ms": 30000
    }"#;
    let s: SenderConfig = serde_json::from_str(json).unwrap();
    assert!(s.use_grpc);
    assert_eq!(s.tip_percentile, 75);
    assert_eq!(s.tip_floor_lamports, 20_000);
    assert_eq!(s.tip_ceiling_lamports, 1_500_000);
    assert_eq!(s.tip_refresh_interval_ms, 30_000);
}

#[test]
fn jito_defaults_when_fields_omitted() {
    let json = r#"{
      "id": 2, "name": "jito", "kind": "jito",
      "endpoint_url": "https://{region}.x",
      "regions": ["frankfurt"]
    }"#;
    let s: SenderConfig = serde_json::from_str(json).unwrap();
    assert!(!s.use_grpc);
    assert_eq!(s.tip_percentile, 75);
    assert_eq!(s.tip_floor_lamports, 15_000);
    assert_eq!(s.tip_ceiling_lamports, 2_000_000);
    assert_eq!(s.tip_refresh_interval_ms, 30_000);
}
```

- [ ] **Step 2: Run tests to verify failure**

```bash
cd /home/jjaksik/Repos/my-scripts && cargo test -p tick-trigger-fan-out-bench config::tests::jito_sender_with_full 2>&1 | tail -10
```

Expected: COMPILE ERROR — fields missing.

- [ ] **Step 3: Add fields with defaults**

In `config.rs`, edit `pub struct SenderConfig` — append new fields at the end:

```rust
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SenderConfig {
    // ... existing fields …
    #[serde(default)]
    pub min_send_interval_ms: u64,

    // ── Jito bundle sender fields (ignored by other kinds) ──
    #[serde(default)]
    pub use_grpc: bool,
    #[serde(default = "default_tip_percentile")]
    pub tip_percentile: u32,
    #[serde(default = "default_tip_floor_lamports")]
    pub tip_floor_lamports: u64,
    #[serde(default = "default_tip_ceiling_lamports")]
    pub tip_ceiling_lamports: u64,
    #[serde(default = "default_tip_refresh_interval_ms")]
    pub tip_refresh_interval_ms: u64,
}

fn default_tip_percentile() -> u32 { 75 }
fn default_tip_floor_lamports() -> u64 { 15_000 }
fn default_tip_ceiling_lamports() -> u64 { 2_000_000 }
fn default_tip_refresh_interval_ms() -> u64 { 30_000 }
```

- [ ] **Step 4: Run config tests**

```bash
cd /home/jjaksik/Repos/my-scripts && cargo test -p tick-trigger-fan-out-bench config:: 2>&1 | tail -20
```

Expected: all pass.

- [ ] **Step 5: Pause for user to commit**

---

## Task 12: Teach preparer to build Jito bundles

**Files:**
- Modify: `crates/tick-trigger-fan-out-bench/src/preparer.rs`

**Context:** When a `SenderConfig.kind == Jito` is encountered in the per-sender loop, the preparer builds Tx1 (with `fund_tipper`) and Tx2 (via `build_tipper_tx`), stores them as `PreSignedTx { tx: tx1, extra_txs: [tx2], bundle_metadata: Some(...) }`. To read the live tip floor value at signing time, the preparer needs a per-sender handle — we store `Option<Arc<AtomicU64>>` indexed by `sender_id` in `PreparerConfig`.

- [ ] **Step 1: Add `jito_tip_handles` to `PreparerConfig`**

In `preparer.rs`, edit `pub struct PreparerConfig`:

```rust
pub struct PreparerConfig {
    // ... existing fields …
    pub stop: Arc<AtomicBool>,
    /// Per-sender_id handle to the live tip floor value (lamports). Only
    /// Jito senders populate this map; preparer reads from it at pre-sign
    /// time so each bundle locks in a snapshot value.
    pub jito_tip_handles: std::collections::HashMap<u8, Arc<AtomicU64>>,
}
```

- [ ] **Step 2: Update the per-trigger loop to branch on Jito**

In the `for slot in &senders` loop in `run_loop`, replace the body:

```rust
for slot in &senders {
    let tip_account = if slot.config.tip_lamports > 0 || slot.config.kind == crate::config::SenderKind::Jito {
        slot.tip_rotator.next()
    } else {
        None
    };

    match slot.config.kind {
        crate::config::SenderKind::Jito => {
            let Some(tip_handle) = cfg.jito_tip_handles.get(&slot.config.id) else {
                tracing::error!(sender_id = slot.config.id, "no tip handle for Jito sender");
                cfg.counters.signing_errors.fetch_add(1, Ordering::Relaxed);
                continue;
            };
            let tip_lamports = tip_handle.load(Ordering::Relaxed);
            let Some(tip_account_pk) = tip_account else {
                tracing::error!(sender_id = slot.config.id, "no tip account for Jito sender");
                cfg.counters.signing_errors.fetch_add(1, Ordering::Relaxed);
                continue;
            };
            let tipper = solana_sdk::signature::Keypair::new();
            let rent_exempt = crate::tx_builder::RENT_EXEMPT_MIN_LAMPORTS;
            let base_fee = crate::tx_builder::BASE_TX_FEE_LAMPORTS;
            let fund_amount = tip_lamports + rent_exempt + base_fee;

            // Tx2 needs a fresh blockhash distinct from Tx1's nonce-stored hash.
            let fresh_bh_for_tx2 = cfg.blockhash_cache.current();

            let tx1 = tx_builder::build(tx_builder::BuildParams {
                payer: &cfg.keypair,
                blockhash: bh,
                sender_id: slot.config.id,
                trigger_id: trigger_id.0,
                tip_account: None,         // Tip is in Tx2 now.
                tip_lamports: 0,
                nonce: nonce_params,
                tx_cfg: &cfg.tx_cfg,
                fund_tipper: Some((tipper.pubkey(), fund_amount)),
            });
            let tx2 = tx_builder::build_tipper_tx(
                &tipper, fresh_bh_for_tx2, tip_account_pk, tip_lamports,
                cfg.keypair.pubkey(), rent_exempt,
            );

            variants.push(PreSignedTx {
                sender_id: slot.config.id,
                tx: Arc::new(tx1.tx),
                signature: tx1.signature,
                blockhash: bh,
                prepared_at,
                nonce_id,
                extra_txs: vec![Arc::new(tx2.tx)],
                bundle_metadata: Some(crate::tx_pool::BundleMeta {
                    tipper_pubkey: tipper.pubkey(),
                    tip_account: tip_account_pk,
                    tip_lamports,
                    tx2_blockhash: fresh_bh_for_tx2,
                }),
            });
            cfg.counters.variants_signed.fetch_add(1, Ordering::Relaxed);
        }
        _ => {
            // Non-Jito (Helius, future Triton): single tx, existing path.
            let built = tx_builder::build(tx_builder::BuildParams {
                payer: &cfg.keypair,
                blockhash: bh,
                sender_id: slot.config.id,
                trigger_id: trigger_id.0,
                tip_account,
                tip_lamports: slot.config.tip_lamports,
                nonce: nonce_params,
                tx_cfg: &cfg.tx_cfg,
                fund_tipper: None,
            });
            variants.push(PreSignedTx {
                sender_id: slot.config.id,
                tx: Arc::new(built.tx),
                signature: built.signature,
                blockhash: bh,
                prepared_at,
                nonce_id,
                extra_txs: vec![],
                bundle_metadata: None,
            });
            cfg.counters.variants_signed.fetch_add(1, Ordering::Relaxed);
        }
    }
}
```

- [ ] **Step 3: Update test fixtures in `preparer.rs`**

The unit tests in `preparer.rs` construct `SenderConfig` but currently set `kind: SenderKind::Helius`. No change to existing tests. Skip.

- [ ] **Step 4: Update call site in `phase3_run.rs`**

In `bin/phase3_run.rs`, find the `spawn_preparer(PreparerConfig { … })` call (around line 316) and add the new field with an empty map for now (real wiring happens in Task 14):

```rust
let _preparer = spawn_preparer(PreparerConfig {
    schedule_rx: preparer_schedule_rx,
    pool: tx_pool.clone(),
    keypair: keypair.clone(),
    blockhash_cache: bh_runner.cache.clone(),
    tx_cfg: cfg.tx.clone(),
    senders: enabled_senders.clone(),
    shuffle_seed: cfg.schedule.seed.unwrap_or(0xDEADBEEF),
    current_slot: current_slot.clone(),
    nonce_manager: nonce_manager.clone(),
    counters: preparer_counters.clone(),
    stop: stop.clone(),
    jito_tip_handles: std::collections::HashMap::new(),
})?;
```

- [ ] **Step 5: Build and run preparer tests**

```bash
cd /home/jjaksik/Repos/my-scripts && cargo test -p tick-trigger-fan-out-bench preparer:: 2>&1 | tail -20
```

Expected: existing 4 tests still pass.

- [ ] **Step 6: Confirm full crate compiles**

```bash
cd /home/jjaksik/Repos/my-scripts && cargo check -p tick-trigger-fan-out-bench --all-targets 2>&1 | tail -15
```

Expected: clean.

- [ ] **Step 7: Pause for user to commit**

---

## Task 13: Teach dispatcher to call `send_bundle` for bundle variants

**Files:**
- Modify: `crates/tick-trigger-fan-out-bench/src/bin/phase3_run.rs`

- [ ] **Step 1: Locate the dispatcher dispatch block**

Open `phase3_run.rs` and find the block around lines 706-754 (after `for (order_idx, presigned) in variants.into_iter().enumerate()`).

- [ ] **Step 2: Update the spawned task to branch on `extra_txs`**

Replace the existing `tokio::spawn(async move { … })` block with:

```rust
let sender_for_task = sender.clone();
let send_tx_for_task = send_event_tx.clone();
let tx_for_task = presigned.tx.clone();
let extra_txs_for_task = presigned.extra_txs.clone();
let trigger_id = trig.trigger_id;
let sender_id = sender_cfg.id;
tokio::spawn(async move {
    let outcome = if extra_txs_for_task.is_empty() {
        sender_for_task.send(&tx_for_task).await
    } else {
        let mut bundle: Vec<solana_sdk::transaction::Transaction> =
            Vec::with_capacity(1 + extra_txs_for_task.len());
        bundle.push((*tx_for_task).clone());
        for extra in &extra_txs_for_task {
            bundle.push((**extra).clone());
        }
        sender_for_task.send_bundle(&bundle).await
    };
    let _ = send_tx_for_task.try_send(SendEvent {
        trigger_id, sender_id, outcome,
    });
});
```

- [ ] **Step 3: Confirm compile**

```bash
cd /home/jjaksik/Repos/my-scripts && cargo check -p tick-trigger-fan-out-bench --all-targets 2>&1 | tail -10
```

Expected: clean.

- [ ] **Step 4: Pause for user to commit**

---

## Task 14: Wire `JitoBundleSender` + tip updater into `phase3_run.rs`

**Files:**
- Modify: `crates/tick-trigger-fan-out-bench/src/bin/phase3_run.rs`

- [ ] **Step 1: Update imports**

Replace line 62:

```rust
use tick_trigger_fan_out_bench::senders::{
    helius::HeliusSender,
    jito::{tip_updater::JitoTipUpdater, JitoBundleSender},
    TxSender,
};
```

- [ ] **Step 2: Construct sender + tip updater for each Jito config**

Replace the `SenderKind::Jito` arm (currently a `bail!` from Task 5) with:

```rust
SenderKind::Jito => {
    if sc.regions.is_empty() {
        anyhow::bail!(
            "jito sender {:?} (id={}) must declare at least one region",
            sc.name, sc.id
        );
    }
    let updater = JitoTipUpdater::new(
        sc.tip_percentile,
        sc.tip_floor_lamports,
        sc.tip_ceiling_lamports,
        sc.tip_refresh_interval_ms,
    );
    let tip_handle = updater.current_lamports.clone();
    let _tip_task = updater.spawn(stop.clone());
    jito_tip_handles.insert(sc.id, tip_handle.clone());
    let bundle_sender = JitoBundleSender::new(
        sc.id,
        sc.name.clone(),
        sc.endpoint_url.clone(),
        sc.regions.clone(),
        sc.outbound_ips.clone(),
        sc.use_grpc,
        tip_handle,
        sc.min_send_interval_ms,
    )?;
    Arc::new(bundle_sender) as Arc<dyn TxSender>
}
```

Above the sender-construction loop, declare the map:

```rust
let mut jito_tip_handles: std::collections::HashMap<u8, Arc<AtomicU64>> =
    std::collections::HashMap::new();
```

Add `use std::sync::atomic::AtomicU64;` to the file imports if not present.

- [ ] **Step 3: Wire `jito_tip_handles` into `spawn_preparer`**

In the `PreparerConfig { … }` literal around line 316:

```rust
let _preparer = spawn_preparer(PreparerConfig {
    // ... existing fields …
    stop: stop.clone(),
    jito_tip_handles: jito_tip_handles.clone(),
})?;
```

- [ ] **Step 4: Build and verify**

```bash
cd /home/jjaksik/Repos/my-scripts && cargo build -p tick-trigger-fan-out-bench --bin phase3_run 2>&1 | tail -20
```

Expected: clean build.

- [ ] **Step 5: Smoke-run `phase3_run --help`**

```bash
cd /home/jjaksik/Repos/my-scripts && cargo run -p tick-trigger-fan-out-bench --bin phase3_run -- --help 2>&1 | tail -20
```

Expected: usage prints, no panic.

- [ ] **Step 6: Pause for user to commit**

---

## Task 15: Add Jito-specific metrics

**Files:**
- Modify: `crates/tick-trigger-fan-out-bench/src/bin/phase3_run.rs`
- Modify: `crates/tick-trigger-fan-out-bench/src/senders/jito/mod.rs`

- [ ] **Step 1: Locate `DispatcherCounters`**

In `phase3_run.rs` search for `struct DispatcherCounters` and add fields:

```rust
#[derive(Debug, Default)]
struct DispatcherCounters {
    pool_hits: AtomicU64,
    pool_misses_fallback_built: AtomicU64,
    pool_misses_skipped_no_blockhash: AtomicU64,

    // ── Jito-specific ──
    jito_bundles_sent: AtomicU64,
    jito_first_reply_json_rpc: AtomicU64,
    jito_first_reply_grpc: AtomicU64,
    jito_ip_send_count: [AtomicU64; 8],   // up to 8 IPs; we report only those used
}
```

(8 is generous; outbound_ips ≤ 5 per spec but extra slots cost nothing.)

- [ ] **Step 2: Expose counter increments from `JitoBundleSender`**

In `senders/jito/mod.rs`, change `JitoBundleSender` to hold an optional callback for counter increments. Simpler: expose a `pub(crate)` snapshot accessor via fields the dispatcher already owns. Concretely, add to `JitoBundleSender`:

```rust
pub(crate) bundles_sent: Arc<AtomicU64>,
pub(crate) first_reply_json_rpc: Arc<AtomicU64>,
pub(crate) first_reply_grpc: Arc<AtomicU64>,
pub(crate) ip_send_count: Arc<[AtomicU64; 8]>,
```

Update `JitoBundleSender::new` to take a `JitoBundleCounters` struct and store its inner Arcs:

```rust
#[derive(Default)]
pub struct JitoBundleCounters {
    pub bundles_sent: Arc<AtomicU64>,
    pub first_reply_json_rpc: Arc<AtomicU64>,
    pub first_reply_grpc: Arc<AtomicU64>,
    pub ip_send_count: Arc<[AtomicU64; 8]>,
}

impl JitoBundleSender {
    pub fn new(
        id: u8, name: String, endpoint_template: String,
        regions: Vec<String>, outbound_ips: Vec<String>,
        use_grpc: bool,
        current_tip_lamports: Arc<AtomicU64>,
        min_send_interval_ms: u64,
        counters: JitoBundleCounters,
    ) -> Result<Self, tonic::transport::Error> {
        // ... existing body …
        Ok(Self {
            // ... existing fields …
            bundles_sent: counters.bundles_sent,
            first_reply_json_rpc: counters.first_reply_json_rpc,
            first_reply_grpc: counters.first_reply_grpc,
            ip_send_count: counters.ip_send_count,
        })
    }
}
```

In `send_bundle`, after picking `ip_idx`, increment counters:

```rust
let ip_idx = self.next_ip_idx();
self.bundles_sent.fetch_add(1, Ordering::Relaxed);
if ip_idx < self.ip_send_count.len() {
    self.ip_send_count[ip_idx].fetch_add(1, Ordering::Relaxed);
}
```

Right before returning the `SendOutcome` from `FanoutReply::Success` / `Error`, in the success arm match on `method` and increment the appropriate first-reply counter:

```rust
match first {
    FanoutReply::Success { method, .. } => {
        match method {
            "JSON-RPC" => { self.first_reply_json_rpc.fetch_add(1, Ordering::Relaxed); }
            "gRPC" => { self.first_reply_grpc.fetch_add(1, Ordering::Relaxed); }
            _ => {}
        }
        // existing outcome construction…
    }
    FanoutReply::Error { .. } => {
        // existing outcome construction (no counter change on first-error)
    }
}
```

(Pull out the method-match before constructing the `SendOutcome` so you can read `method` once.)

- [ ] **Step 3: Update `phase3_run.rs` Jito sender construction**

In the `SenderKind::Jito` arm, before `JitoBundleSender::new`:

```rust
let jito_counters = JitoBundleCounters {
    bundles_sent: Arc::new(AtomicU64::new(0)),
    first_reply_json_rpc: Arc::new(AtomicU64::new(0)),
    first_reply_grpc: Arc::new(AtomicU64::new(0)),
    ip_send_count: Arc::new(std::array::from_fn(|_| AtomicU64::new(0))),
};
// stash for the summary printer:
jito_counters_by_id.insert(sc.id, jito_counters.clone_arcs());
// then pass to sender:
let bundle_sender = JitoBundleSender::new(
    sc.id, sc.name.clone(), sc.endpoint_url.clone(),
    sc.regions.clone(), sc.outbound_ips.clone(),
    sc.use_grpc, tip_handle, sc.min_send_interval_ms,
    jito_counters,
)?;
```

Add a helper on `JitoBundleCounters`:

```rust
impl JitoBundleCounters {
    pub fn clone_arcs(&self) -> JitoBundleCounters {
        JitoBundleCounters {
            bundles_sent: self.bundles_sent.clone(),
            first_reply_json_rpc: self.first_reply_json_rpc.clone(),
            first_reply_grpc: self.first_reply_grpc.clone(),
            ip_send_count: self.ip_send_count.clone(),
        }
    }
}
```

Declare the per-sender map above the loop:

```rust
let mut jito_counters_by_id: std::collections::HashMap<u8, JitoBundleCounters> =
    std::collections::HashMap::new();
```

Import `JitoBundleCounters` at the top:

```rust
use tick_trigger_fan_out_bench::senders::jito::JitoBundleCounters;
```

- [ ] **Step 4: Print the counters in the summary**

In `log_summary` (around the end of `phase3_run.rs`), accept `jito_counters_by_id` and print per-sender:

```rust
for (id, c) in jito_counters_by_id.iter() {
    let bundles = c.bundles_sent.load(Ordering::Relaxed);
    let rjs = c.first_reply_json_rpc.load(Ordering::Relaxed);
    let rgr = c.first_reply_grpc.load(Ordering::Relaxed);
    println!("Jito[id={}]: bundles_sent={} first_reply: json_rpc={} grpc={}",
             id, bundles, rjs, rgr);
    let ips: Vec<u64> = c.ip_send_count.iter().map(|a| a.load(Ordering::Relaxed)).collect();
    println!("  per-IP send counts: {:?}", ips);
}
```

- [ ] **Step 5: Verify build**

```bash
cd /home/jjaksik/Repos/my-scripts && cargo build -p tick-trigger-fan-out-bench --bin phase3_run 2>&1 | tail -15
```

Expected: clean build.

- [ ] **Step 6: Run all tests**

```bash
cd /home/jjaksik/Repos/my-scripts && cargo test -p tick-trigger-fan-out-bench 2>&1 | tail -30
```

Expected: all green.

- [ ] **Step 7: Pause for user to commit**

---

## Task 16: End-to-end smoke validation

**Files:**
- None (validation only).

- [ ] **Step 1: Run the full test suite**

```bash
cd /home/jjaksik/Repos/my-scripts && cargo test --workspace 2>&1 | tail -40
```

Expected: every test passes.

- [ ] **Step 2: Build the release binary**

```bash
cd /home/jjaksik/Repos/my-scripts && cargo build --release -p tick-trigger-fan-out-bench --bin phase3_run 2>&1 | tail -10
```

Expected: success.

- [ ] **Step 3: Dry-run phase3 with the existing config**

Find the most recently used config under `runs/` or `config/`:

```bash
ls -lt /home/jjaksik/Repos/my-scripts/runs 2>/dev/null | head -10
```

Tell the user: "Smoke-run cargo run --release -p tick-trigger-fan-out-bench --bin phase3_run -- --config <PATH> --duration 30s and watch the log. Expected: preparer signs Jito bundles, dispatcher fans out to 16 paths, recorder produces parquet output. Tip updater logs current value within 30s. Report back."

Do not actually run a paid send-bench; let the user start it intentionally.

- [ ] **Step 4: Pause for user to confirm**

Tell the user: "Implementation complete. Please review the diff, run the smoke command above when ready, and confirm you're happy before I close out the plan."

---

## Spec Coverage Map

| Spec section                                            | Implementing task(s) |
|---------------------------------------------------------|----------------------|
| Behavior summary (2-tx bundle, fan-out 16, IP rotation) | Task 6, 8, 10        |
| Durable nonce semantics (1 nonce per send)              | Task 12 (preparer)   |
| `TxSender` trait extension                              | Task 2               |
| `tx_builder.rs` (`fund_tipper`, `build_tipper_tx`)      | Task 3               |
| `tx_pool.rs` (`extra_txs`, `BundleMeta`)                | Task 4               |
| `preparer.rs` 2-tx Jito path                            | Task 12              |
| Dispatcher branch on `extra_txs`                        | Task 13              |
| `JitoBundleSender` (json_rpc + grpc + tip updater)      | Tasks 6, 8, 9, 10    |
| Per-IP binding (JSON-RPC + gRPC)                        | Tasks 6, 8           |
| Tonic 0.13 upgrade                                      | Task 1               |
| Proto vendoring + tonic-build                           | Task 7               |
| Config schema (`use_grpc`, `tip_*`)                     | Task 11              |
| Metrics                                                 | Task 15              |
| Compatibility (Helius, future senders)                  | Tasks 2, 4, 12, 13   |
| Cleanup of old `senders/jito.rs`                        | Task 5               |
| Temp-commits force-push                                 | Pre-Task 0 (manual)  |

## Self-Review Notes

- All steps contain the actual code to write; no placeholders like "TBD" or "add error handling".
- Type and method names are consistent: `JitoBundleSender::new`, `BuildParams.fund_tipper`,
  `PreSignedTx.extra_txs`, `JitoTipUpdater::spawn`, `JitoBundleCounters.clone_arcs`.
- Each task ends with "Pause for user to commit" (user does manual git work).
- The plan handles the tonic-upgrade risk (contingency path documented inline in Tasks 1 and 8).
- No file is created without a clear single responsibility.
